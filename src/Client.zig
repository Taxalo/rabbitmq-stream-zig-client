//! A connection to a RabbitMQ node speaking the stream protocol.
//!
//! A `Client` runs a reader task that dispatches responses, deliveries,
//! publish confirmations and server-initiated commands, plus a heartbeat
//! task when heartbeats are negotiated. Both need an `Io` implementation that
//! supports `Io.concurrent`, such as `Io.Threaded`.
//!
//! All methods are safe to call from several tasks at the same time.

const std = @import("std");
const Io = std.Io;
const net = Io.net;
const Allocator = std.mem.Allocator;
const assert = std.debug.assert;

const protocol = @import("protocol.zig");
const Command = protocol.Command;
const ResponseCode = protocol.ResponseCode;
const OffsetSpec = protocol.OffsetSpec;
const Property = protocol.Property;
const Encoder = protocol.Encoder;
const Decoder = protocol.Decoder;
const Publisher = @import("Publisher.zig");
const Consumer = @import("Consumer.zig");

const log = std.log.scoped(.rabbitmq_stream);

const Client = @This();

gpa: Allocator,
io: Io,
transport: *Transport,
rpc_timeout: Io.Duration,

/// Guards the outgoing byte stream and `frame`.
write_mutex: Io.Mutex = .init,
frame: protocol.FrameBuilder,

/// Guards `pending`, `publishers`, `consumers`, `next_correlation_id`
/// and `close_reason`.
state_mutex: Io.Mutex = .init,
pending: std.AutoHashMapUnmanaged(u32, *Pending) = .empty,
next_correlation_id: u32 = 1,
publishers: [256]?*Publisher = @splat(null),
consumers: [256]?*Consumer = @splat(null),
close_reason: ?CloseReason = null,
/// Response code of a `close` sent by the broker.
server_close_code: ResponseCode = .ok,
/// Set once the connection is closed for any reason.
closed: Io.Event = .unset,
/// Set by `close`, so that the end of the socket is not reported as a
/// lost connection.
closing: std.atomic.Value(bool) = .init(false),

tune_event: Io.Event = .unset,
server_frame_max: u32 = 0,
server_heartbeat: u32 = 0,
/// Negotiated maximum frame size, 0 means no limit.
frame_max: u32 = 0,
/// Negotiated heartbeat in seconds, 0 means disabled.
heartbeat: u32 = 0,
/// Milliseconds on the `awake` clock when the last frame arrived.
last_read_ms: std.atomic.Value(i64) = .init(0),

/// Owns `server_properties`, `connection_properties` and
/// `server_command_versions`.
arena: std.heap.ArenaAllocator,
server_properties: []const Property = &.{},
connection_properties: []const Property = &.{},
server_version: std.SemanticVersion = .{ .major = 0, .minor = 0, .patch = 0 },
server_command_versions: []const protocol.VersionRange = &.{},

on_metadata_update: ?MetadataUpdateListener,
on_close: ?CloseListener,

reader_future: ?Io.Future(void) = null,
heartbeat_future: ?Io.Future(void) = null,
/// Tasks answering `consumer_update` requests.
tasks: Io.Group = .init,

pub const Error = error{
    ConnectionClosed,
    Timeout,
    OutOfMemory,
    MalformedFrame,
    Canceled,
    FrameTooLarge,
    TooManyPublishers,
    TooManyConsumers,
} || ResponseCode.Error;

pub const ConnectError = error{
    ConnectionFailed,
    ConcurrencyUnavailable,
    TlsFailure,
    UnsupportedSaslMechanism,
    InvalidHostName,
} || Error;

pub const SaslMechanism = enum {
    plain,
    /// Authentication with the TLS client certificate.
    external,

    fn name(m: SaslMechanism) []const u8 {
        return switch (m) {
            .plain => "PLAIN",
            .external => "EXTERNAL",
        };
    }
};

pub const TlsOptions = struct {
    /// Name used for SNI and certificate verification. Defaults to the host.
    server_name: ?[]const u8 = null,
    ca: Ca = .system,

    pub const Ca = union(enum) {
        /// Verify against the operating system certificate store.
        system,
        /// Verify against the PEM certificates in this absolute path.
        file: []const u8,
        /// Accept a self-signed server certificate.
        self_signed,
        /// Skip verification. Only for testing.
        no_verification,
    };
};

pub const Options = struct {
    host: []const u8 = "localhost",
    port: u16 = protocol.default_port,
    username: []const u8 = "guest",
    password: []const u8 = "guest",
    virtual_host: []const u8 = "/",
    sasl_mechanism: SaslMechanism = .plain,
    tls: ?TlsOptions = null,
    /// Requested heartbeat in seconds; the lower non-zero value of the
    /// client and server wins.
    heartbeat: u32 = protocol.default_heartbeat,
    /// Requested maximum frame size in bytes; the lower non-zero value of
    /// the client and server wins.
    max_frame_size: u32 = protocol.default_frame_max,
    /// Shown in the management UI.
    connection_name: ?[]const u8 = null,
    /// Extra client properties sent in `peer_properties`.
    client_properties: []const Property = &.{},
    connect_timeout: Io.Duration = .fromSeconds(10),
    /// How long to wait for the response to a request.
    rpc_timeout: Io.Duration = .fromSeconds(10),
    on_metadata_update: ?MetadataUpdateListener = null,
    on_close: ?CloseListener = null,
};

pub const UriError = error{ InvalidUri, UnsupportedScheme, OutOfMemory };

/// Parses `rabbitmq-stream://user:password@host:port/vhost`, or
/// `rabbitmq-stream+tls://...` to enable TLS, into options based on
/// `defaults`. Missing parts keep their default values; the virtual host
/// is the percent-decoded path without its leading slash (`/%2f` is `/`).
/// Strings point into `uri` or are allocated in `arena`.
pub fn optionsFromUri(arena: Allocator, uri: []const u8, defaults: Options) UriError!Options {
    const parsed = std.Uri.parse(uri) catch return error.InvalidUri;
    var o = defaults;
    if (std.ascii.eqlIgnoreCase(parsed.scheme, "rabbitmq-stream")) {
        o.tls = null;
        if (parsed.port == null) o.port = protocol.default_port;
    } else if (std.ascii.eqlIgnoreCase(parsed.scheme, "rabbitmq-stream+tls")) {
        if (o.tls == null) o.tls = .{};
        if (parsed.port == null) o.port = protocol.default_tls_port;
    } else return error.UnsupportedScheme;
    if (parsed.port) |port| o.port = port;
    if (parsed.user) |u| o.username = try u.toRawMaybeAlloc(arena);
    if (parsed.password) |pw| o.password = try pw.toRawMaybeAlloc(arena);
    if (parsed.host) |h| {
        const host = try h.toRawMaybeAlloc(arena);
        o.host = if (host.len > 2 and host[0] == '[' and host[host.len - 1] == ']') host[1 .. host.len - 1] else host;
    }
    const path = try parsed.path.toRawMaybeAlloc(arena);
    const vhost = if (path.len > 0 and path[0] == '/') path[1..] else path;
    if (vhost.len > 0) o.virtual_host = vhost;
    return o;
}

test optionsFromUri {
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const o = try optionsFromUri(a, "rabbitmq-stream://app:s%40cret@broker.example:5560/%2f", .{});
    try std.testing.expectEqualStrings("app", o.username);
    try std.testing.expectEqualStrings("s@cret", o.password);
    try std.testing.expectEqualStrings("broker.example", o.host);
    try std.testing.expectEqual(@as(u16, 5560), o.port);
    try std.testing.expectEqualStrings("/", o.virtual_host);
    try std.testing.expect(o.tls == null);

    const t = try optionsFromUri(a, "rabbitmq-stream+tls://[::1]/prod", .{});
    try std.testing.expectEqualStrings("::1", t.host);
    try std.testing.expectEqual(@as(u16, protocol.default_tls_port), t.port);
    try std.testing.expectEqualStrings("prod", t.virtual_host);
    try std.testing.expectEqualStrings("guest", t.username);
    try std.testing.expect(t.tls != null);

    try std.testing.expectError(error.UnsupportedScheme, optionsFromUri(a, "amqp://localhost", .{}));
}

/// Called from the reader task when the broker reports a topology change
/// for a stream, typically because it was deleted or its leader moved.
/// Publishers and consumers on the stream are closed before the call.
pub const MetadataUpdateListener = struct {
    context: ?*anyopaque = null,
    callback: *const fn (context: ?*anyopaque, client: *Client, stream: []const u8, code: ResponseCode) void,
};

/// Called once when the connection closes, from the task that noticed it.
pub const CloseListener = struct {
    context: ?*anyopaque = null,
    callback: *const fn (context: ?*anyopaque, client: *Client, reason: CloseReason) void,
};

pub const CloseReason = union(enum) {
    /// `close` was called.
    requested,
    /// The broker sent a `close` command.
    server: ResponseCode,
    /// The socket failed or reached end of stream.
    connection_lost,
    /// No frame arrived within two heartbeat intervals.
    heartbeat_timeout,
    /// The broker sent bytes that could not be parsed.
    protocol_error,
};

const Pending = struct {
    event: Io.Event = .unset,
    frame: ?[]u8 = null,
    err: ?Error = null,
};

/// The response to a request. `decoder` is positioned after the
/// correlation id. Free with `deinit`.
pub const Response = struct {
    frame: []u8,
    decoder: Decoder,

    pub fn deinit(r: *Response, gpa: Allocator) void {
        gpa.free(r.frame);
        r.* = undefined;
    }

    pub fn code(r: *Response) Decoder.Error!ResponseCode {
        return r.decoder.responseCode();
    }

    /// Reads the response code and fails unless it is `ok`.
    pub fn expectOk(r: *Response) (Decoder.Error || ResponseCode.Error)!void {
        try (try r.code()).check();
    }
};

const Transport = struct {
    stream: net.Stream,
    net_reader: net.Stream.Reader,
    net_writer: net.Stream.Writer,
    tls: ?std.crypto.tls.Client = null,
    read_buffer: []u8,
    write_buffer: []u8,
    tls_read_buffer: []u8 = &.{},
    tls_write_buffer: []u8 = &.{},

    fn reader(t: *Transport) *Io.Reader {
        return if (t.tls) |*tls| &tls.reader else &t.net_reader.interface;
    }

    fn writer(t: *Transport) *Io.Writer {
        return if (t.tls) |*tls| &tls.writer else &t.net_writer.interface;
    }

    /// Flushes the plaintext writer and, with TLS, the socket writer.
    fn flush(t: *Transport) Io.Writer.Error!void {
        if (t.tls) |*tls| {
            try tls.writer.flush();
            try t.net_writer.interface.flush();
        } else {
            try t.net_writer.interface.flush();
        }
    }

    fn deinit(t: *Transport, gpa: Allocator) void {
        gpa.free(t.read_buffer);
        gpa.free(t.write_buffer);
        gpa.free(t.tls_read_buffer);
        gpa.free(t.tls_write_buffer);
    }
};

const product_properties = [_]Property{
    .{ .key = "product", .value = "RabbitMQ Stream" },
    .{ .key = "version", .value = @import("root.zig").version },
    .{ .key = "platform", .value = "Zig " ++ @import("builtin").zig_version_string },
    .{ .key = "copyright", .value = "Copyright (c) the rabbitmq-stream-zig authors" },
    .{ .key = "information", .value = "Licensed under the MIT license" },
};

/// Opens a connection and performs the handshake: peer properties, SASL,
/// tune, open and command version exchange.
pub fn connect(gpa: Allocator, io: Io, options: Options) ConnectError!*Client {
    const transport = try openTransport(gpa, io, options);
    const c = gpa.create(Client) catch {
        closeTransport(gpa, io, transport);
        return error.OutOfMemory;
    };
    c.* = .{
        .gpa = gpa,
        .io = io,
        .transport = transport,
        .rpc_timeout = options.rpc_timeout,
        .frame = .init(gpa),
        .arena = .init(gpa),
        .on_metadata_update = options.on_metadata_update,
        .on_close = options.on_close,
    };
    c.touch();
    c.reader_future = io.concurrent(readLoop, .{c}) catch {
        closeTransport(gpa, io, transport);
        c.frame.deinit();
        c.arena.deinit();
        gpa.destroy(c);
        return error.ConcurrencyUnavailable;
    };
    c.handshake(options) catch |err| {
        c.destroy();
        return err;
    };
    if (c.heartbeat > 0) {
        c.heartbeat_future = io.concurrent(heartbeatLoop, .{c}) catch null;
    }
    return c;
}

fn openTransport(gpa: Allocator, io: Io, options: Options) ConnectError!*Transport {
    const stream = connectTcp(io, options.host, options.port, options.connect_timeout) catch |err| {
        log.debug("connection to {s}:{d} failed: {t}", .{ options.host, options.port, err });
        return switch (err) {
            error.Canceled => error.Canceled,
            error.InvalidHostName => error.InvalidHostName,
            else => error.ConnectionFailed,
        };
    };
    errdefer stream.close(io);

    const t = try gpa.create(Transport);
    errdefer gpa.destroy(t);
    const tls_min = std.crypto.tls.Client.min_buffer_len;
    const read_buffer = try gpa.alloc(u8, if (options.tls != null) tls_min else 64 * 1024);
    errdefer gpa.free(read_buffer);
    const write_buffer = try gpa.alloc(u8, if (options.tls != null) tls_min else 64 * 1024);
    errdefer gpa.free(write_buffer);
    t.* = .{
        .stream = stream,
        .net_reader = stream.reader(io, read_buffer),
        .net_writer = stream.writer(io, write_buffer),
        .read_buffer = read_buffer,
        .write_buffer = write_buffer,
    };
    if (options.tls) |tls_options| {
        errdefer {
            gpa.free(t.tls_read_buffer);
            gpa.free(t.tls_write_buffer);
        }
        t.tls_read_buffer = try gpa.alloc(u8, tls_min);
        t.tls_write_buffer = try gpa.alloc(u8, tls_min);
        try startTls(gpa, io, t, tls_options, options.host);
    }
    return t;
}

fn startTls(gpa: Allocator, io: Io, t: *Transport, options: TlsOptions, host: []const u8) ConnectError!void {
    const tls = std.crypto.tls;
    const Bundle = std.crypto.Certificate.Bundle;
    var bundle: Bundle = .empty;
    defer bundle.deinit(gpa);
    var bundle_lock: Io.RwLock = .init;
    const now = Io.Clock.real.now(io);
    const ca: @FieldType(tls.Client.Options, "ca") = switch (options.ca) {
        .no_verification => .no_verification,
        .self_signed => .self_signed,
        .system, .file => blk: {
            switch (options.ca) {
                .system => bundle.rescan(gpa, io, now) catch return error.TlsFailure,
                .file => |path| bundle.addCertsFromFilePathAbsolute(gpa, io, now, path) catch return error.TlsFailure,
                else => unreachable,
            }
            break :blk .{ .bundle = .{ .gpa = gpa, .io = io, .lock = &bundle_lock, .bundle = &bundle } };
        },
    };
    var entropy: [tls.Client.Options.entropy_len]u8 = undefined;
    io.random(&entropy);
    const server_name = options.server_name orelse host;
    t.tls = tls.Client.init(&t.net_reader.interface, &t.net_writer.interface, .{
        .host = if (options.ca == .no_verification) .no_verification else .{ .explicit = server_name },
        .ca = ca,
        .read_buffer = t.tls_read_buffer,
        .write_buffer = t.tls_write_buffer,
        .entropy = &entropy,
        .realtime_now = now,
    }) catch |err| {
        log.err("TLS handshake failed: {t}", .{err});
        return error.TlsFailure;
    };
}

fn closeTransport(gpa: Allocator, io: Io, t: *Transport) void {
    t.stream.close(io);
    t.deinit(gpa);
    gpa.destroy(t);
}

/// `Io.Threaded` does not implement connect timeouts, so the connection
/// attempt races a sleep.
fn connectTcp(io: Io, host: []const u8, port: u16, timeout: Io.Duration) anyerror!net.Stream {
    const Race = union(enum) {
        connected: anyerror!net.Stream,
        timed_out: Io.Cancelable!void,
    };
    var buffer: [2]Race = undefined;
    var select: Io.Select(Race) = .init(io, &buffer);
    select.concurrent(.connected, connectTcpBlocking, .{ io, host, port }) catch return connectTcpBlocking(io, host, port);
    select.concurrent(.timed_out, Io.sleep, .{ io, timeout, .awake }) catch {
        const r = try select.await();
        return r.connected;
    };
    const first = try select.await();
    while (select.cancel()) |other| switch (other) {
        .connected => |r| if (r) |stream| stream.close(io) else |_| {},
        .timed_out => {},
    };
    return switch (first) {
        .connected => |r| r,
        .timed_out => error.Timeout,
    };
}

fn connectTcpBlocking(io: Io, host: []const u8, port: u16) anyerror!net.Stream {
    const options: net.IpAddress.ConnectOptions = .{ .mode = .stream };
    if (net.IpAddress.parse(host, port)) |address| {
        return address.connect(io, options);
    } else |_| {}
    const host_name: net.HostName = try .init(host);
    return host_name.connect(io, port, options);
}

/// Closes the connection gracefully and frees the client.
///
/// Publishers and consumers still registered are detached and report
/// `error.ConnectionClosed`; they must still be closed by their owners.
pub fn close(c: *Client) void {
    c.closing.store(true, .monotonic);
    if (c.isOpen()) {
        c.sendClose(.ok, "") catch {};
    }
    c.destroy();
}

fn destroy(c: *Client) void {
    const io = c.io;
    c.shutdown(.requested);
    {
        c.state_mutex.lockUncancelable(io);
        defer c.state_mutex.unlock(io);
        for (&c.publishers) |*slot| if (slot.*) |p| {
            p.detachClient();
            slot.* = null;
        };
        for (&c.consumers) |*slot| if (slot.*) |con| {
            con.detachClient();
            slot.* = null;
        };
    }
    if (c.heartbeat_future) |*f| f.cancel(io);
    if (c.transport.tls) |*tls| {
        c.write_mutex.lockUncancelable(io);
        defer c.write_mutex.unlock(io);
        tls.end() catch {};
        c.transport.net_writer.interface.flush() catch {};
    }
    c.transport.stream.shutdown(io, .both) catch {};
    if (c.reader_future) |*f| f.cancel(io);
    c.tasks.cancel(io);
    closeTransport(c.gpa, io, c.transport);
    c.pending.deinit(c.gpa);
    c.frame.deinit();
    c.arena.deinit();
    c.gpa.destroy(c);
}

/// Waits until the connection is closed, by either side.
pub fn waitClosed(c: *Client) Io.Cancelable!void {
    try c.closed.wait(c.io);
}

pub fn isOpen(c: *Client) bool {
    c.state_mutex.lockUncancelable(c.io);
    defer c.state_mutex.unlock(c.io);
    return c.close_reason == null;
}

/// Returns a property sent by the broker in the `peer_properties` response.
pub fn serverProperty(c: *const Client, key: []const u8) ?[]const u8 {
    return Property.find(c.server_properties, key);
}

/// Returns a property sent by the broker in the `open` response, such as
/// `advertised_host` and `advertised_port`.
pub fn connectionProperty(c: *const Client, key: []const u8) ?[]const u8 {
    return Property.find(c.connection_properties, key);
}

/// Returns the highest version of `command` the broker supports, or
/// `null` when the broker did not report it.
pub fn serverCommandVersion(c: *const Client, command: Command) ?u16 {
    for (c.server_command_versions) |v| {
        if (v.command == command) return v.max;
    }
    return null;
}

/// Whether the broker supports filtering (RabbitMQ 3.13 and later).
pub fn supportsFiltering(c: *const Client) bool {
    return (c.serverCommandVersion(.publish) orelse 1) >= 2;
}

// Handshake

fn handshake(c: *Client, options: Options) ConnectError!void {
    try c.peerProperties(options);

    const mechanisms = try c.saslHandshake();
    var supported = false;
    for (mechanisms) |m| {
        if (std.ascii.eqlIgnoreCase(m, options.sasl_mechanism.name())) supported = true;
    }
    if (!supported) return error.UnsupportedSaslMechanism;

    var sasl_buf: [512]u8 = undefined;
    const sasl_data: ?[]const u8 = switch (options.sasl_mechanism) {
        .plain => protocol.saslPlain(&sasl_buf, options.username, options.password) catch return error.FrameTooLarge,
        .external => null,
    };
    try c.saslAuthenticate(options.sasl_mechanism.name(), sasl_data);

    c.tune_event.wait(c.io) catch return error.Canceled;
    c.frame_max = negotiate(options.max_frame_size, c.server_frame_max);
    c.heartbeat = negotiate(options.heartbeat, c.server_heartbeat);
    try c.sendTune(c.frame_max, c.heartbeat);

    try c.open(options.virtual_host);

    if (c.server_version.order(.{ .major = 3, .minor = 13, .patch = 0 }) != .lt) {
        try c.exchangeCommandVersions();
    }
}

fn negotiate(client: u32, server: u32) u32 {
    if (client == 0) return server;
    if (server == 0) return client;
    return @min(client, server);
}

fn peerProperties(c: *Client, options: Options) Error!void {
    var props: std.ArrayList(Property) = .empty;
    defer props.deinit(c.gpa);
    try props.appendSlice(c.gpa, &product_properties);
    if (options.connection_name) |name| try props.append(c.gpa, .{ .key = "connection_name", .value = name });
    try props.appendSlice(c.gpa, options.client_properties);

    var r = try c.request(.peer_properties, encodeProperties, .{props.items});
    defer r.deinit(c.gpa);
    try r.expectOk();
    c.server_properties = try c.decodeOwnedProperties(&r.decoder);
    if (c.serverProperty("version")) |v| c.server_version = protocol.parseVersion(v);
}

fn encodeProperties(e: *Encoder, props: []const Property) Encoder.Error!void {
    try e.properties(props);
}

fn decodeOwnedProperties(c: *Client, d: *Decoder) Error![]const Property {
    const arena = c.arena.allocator();
    if (d.remaining() == 0) return &.{};
    const count = try d.int(u32);
    const props = try arena.alloc(Property, count);
    for (props) |*p| {
        p.* = .{
            .key = try arena.dupe(u8, try d.stringOrEmpty()),
            .value = try arena.dupe(u8, try d.stringOrEmpty()),
        };
    }
    return props;
}

fn saslHandshake(c: *Client) Error![]const []const u8 {
    var r = try c.request(.sasl_handshake, encodeNothing, .{});
    defer r.deinit(c.gpa);
    try r.expectOk();
    const count = try r.decoder.int(u32);
    const arena = c.arena.allocator();
    const mechanisms = try arena.alloc([]const u8, count);
    for (mechanisms) |*m| m.* = try arena.dupe(u8, try r.decoder.stringOrEmpty());
    return mechanisms;
}

fn encodeNothing(e: *Encoder) Encoder.Error!void {
    _ = e;
}

fn saslAuthenticate(c: *Client, mechanism: []const u8, data: ?[]const u8) Error!void {
    var r = try c.request(.sasl_authenticate, encodeSaslAuthenticate, .{ mechanism, data });
    defer r.deinit(c.gpa);
    try r.expectOk();
}

fn encodeSaslAuthenticate(e: *Encoder, mechanism: []const u8, data: ?[]const u8) Encoder.Error!void {
    try e.string(mechanism);
    try e.bytes(data);
}

fn sendTune(c: *Client, frame_max: u32, heartbeat: u32) Error!void {
    try c.lockWrite();
    defer c.write_mutex.unlock(c.io);
    var e = c.frame.begin(Command.tune.responseKey(), 1) catch return error.OutOfMemory;
    e.int(u32, frame_max) catch return error.OutOfMemory;
    e.int(u32, heartbeat) catch return error.OutOfMemory;
    try c.writeFrameLocked(c.frame.finish());
}

fn open(c: *Client, virtual_host: []const u8) Error!void {
    var r = try c.request(.open, encodeString, .{virtual_host});
    defer r.deinit(c.gpa);
    try r.expectOk();
    c.connection_properties = try c.decodeOwnedProperties(&r.decoder);
}

fn encodeString(e: *Encoder, s: []const u8) Encoder.Error!void {
    try e.string(s);
}

fn exchangeCommandVersions(c: *Client) Error!void {
    var r = try c.request(.exchange_command_versions, encodeVersions, .{@as([]const protocol.VersionRange, &Command.client_versions)});
    defer r.deinit(c.gpa);
    try r.expectOk();
    const count = try r.decoder.int(u32);
    const versions = try c.arena.allocator().alloc(protocol.VersionRange, count);
    for (versions) |*v| {
        v.* = .{
            .command = @fromBackingInt(try r.decoder.int(u16)),
            .min = try r.decoder.int(u16),
            .max = try r.decoder.int(u16),
        };
    }
    c.server_command_versions = versions;
}

fn encodeVersions(e: *Encoder, versions: []const protocol.VersionRange) Encoder.Error!void {
    try e.int(u32, @intCast(versions.len));
    for (versions) |v| {
        try e.int(u16, v.command.key());
        try e.int(u16, v.min);
        try e.int(u16, v.max);
    }
}

fn sendClose(c: *Client, code: ResponseCode, reason: []const u8) Error!void {
    var r = try c.request(.close, encodeClose, .{ code, reason });
    defer r.deinit(c.gpa);
}

fn encodeClose(e: *Encoder, code: ResponseCode, reason: []const u8) Encoder.Error!void {
    try e.int(u16, code.toInt());
    try e.string(reason);
}

// Stream management

pub const LeaderLocator = enum {
    client_local,
    balanced,
    /// Accepted by older brokers only.
    random,
    /// Accepted by older brokers only.
    least_leaders,

    fn value(l: LeaderLocator) []const u8 {
        return switch (l) {
            .client_local => "client-local",
            .balanced => "balanced",
            .random => "random",
            .least_leaders => "least-leaders",
        };
    }
};

/// Arguments of a new stream. Retention is applied per segment, so a
/// stream keeps at least one segment that contains messages.
///
/// The stream protocol takes the argument names without the `x-` prefix
/// that AMQP queue declarations use; RabbitMQ then stores them as the `x-`
/// queue arguments. A name it doesn't know, `x-max-age` included, is
/// ignored without an error.
pub const StreamOptions = struct {
    /// `max-length-bytes`: maximum size of the stream.
    max_length_bytes: ?u64 = null,
    /// `max-age`: maximum age of the messages, rounded to seconds.
    max_age: ?Io.Duration = null,
    /// `stream-max-segment-size-bytes`: size of the segment files.
    max_segment_size_bytes: ?u64 = null,
    /// `queue-leader-locator`
    leader_locator: ?LeaderLocator = null,
    /// `initial-cluster-size`
    initial_cluster_size: ?u32 = null,
    /// `stream-filter-size-bytes`: bloom filter size, from 16 to 255.
    filter_size_bytes: ?u8 = null,
    /// Additional raw arguments, sent as they are (so without `x-`).
    arguments: []const Property = &.{},

    /// Builds the argument list into `buf`, using `scratch` for the
    /// formatted numbers.
    pub fn toArguments(o: StreamOptions, buf: *std.ArrayList(Property), gpa: Allocator, scratch: *[6][24]u8) Allocator.Error!void {
        if (o.max_length_bytes) |v| try buf.append(gpa, .{ .key = "max-length-bytes", .value = fmtInt(&scratch[0], v) });
        if (o.max_age) |v| {
            const s = std.fmt.bufPrint(&scratch[1], "{d}s", .{@max(0, v.toSeconds())}) catch unreachable;
            try buf.append(gpa, .{ .key = "max-age", .value = s });
        }
        if (o.max_segment_size_bytes) |v| try buf.append(gpa, .{ .key = "stream-max-segment-size-bytes", .value = fmtInt(&scratch[2], v) });
        if (o.leader_locator) |v| try buf.append(gpa, .{ .key = "queue-leader-locator", .value = v.value() });
        if (o.initial_cluster_size) |v| try buf.append(gpa, .{ .key = "initial-cluster-size", .value = fmtInt(&scratch[3], v) });
        if (o.filter_size_bytes) |v| try buf.append(gpa, .{ .key = "stream-filter-size-bytes", .value = fmtInt(&scratch[4], v) });
        try buf.appendSlice(gpa, o.arguments);
    }
};

fn fmtInt(buf: *[24]u8, v: anytype) []const u8 {
    return std.fmt.bufPrint(buf, "{d}", .{v}) catch unreachable;
}

test "StreamOptions uses the argument names of the stream protocol" {
    var args: std.ArrayList(Property) = .empty;
    defer args.deinit(std.testing.allocator);
    var scratch: [6][24]u8 = undefined;
    const options: StreamOptions = .{
        .max_length_bytes = 1000,
        .max_age = .fromSeconds(3600),
        .max_segment_size_bytes = 500,
        .leader_locator = .least_leaders,
        .initial_cluster_size = 1,
        .filter_size_bytes = 32,
    };
    try options.toArguments(&args, std.testing.allocator, &scratch);
    const expected = [_]Property{
        .{ .key = "max-length-bytes", .value = "1000" },
        .{ .key = "max-age", .value = "3600s" },
        .{ .key = "stream-max-segment-size-bytes", .value = "500" },
        .{ .key = "queue-leader-locator", .value = "least-leaders" },
        .{ .key = "initial-cluster-size", .value = "1" },
        .{ .key = "stream-filter-size-bytes", .value = "32" },
    };
    try std.testing.expectEqual(expected.len, args.items.len);
    for (expected, args.items) |want, got| {
        try std.testing.expectEqualStrings(want.key, got.key);
        try std.testing.expectEqualStrings(want.value, got.value);
    }
}

pub fn createStream(c: *Client, stream: []const u8, options: StreamOptions) Error!void {
    var args: std.ArrayList(Property) = .empty;
    defer args.deinit(c.gpa);
    var scratch: [6][24]u8 = undefined;
    try options.toArguments(&args, c.gpa, &scratch);
    var r = try c.request(.create_stream, encodeNameAndProperties, .{ stream, args.items });
    defer r.deinit(c.gpa);
    try r.expectOk();
}

fn encodeNameAndProperties(e: *Encoder, name: []const u8, props: []const Property) Encoder.Error!void {
    try e.string(name);
    try e.properties(props);
}

pub fn deleteStream(c: *Client, stream: []const u8) Error!void {
    var r = try c.request(.delete_stream, encodeString, .{stream});
    defer r.deinit(c.gpa);
    try r.expectOk();
}

/// Creates a super stream with the given partition streams and binding
/// keys (RabbitMQ 3.13 and later). Use `SuperStreamOptions` helpers to
/// generate the usual `<name>-<n>` partitions.
pub fn createSuperStream(
    c: *Client,
    name: []const u8,
    partition_streams: []const []const u8,
    binding_keys: []const []const u8,
    options: StreamOptions,
) Error!void {
    var args: std.ArrayList(Property) = .empty;
    defer args.deinit(c.gpa);
    var scratch: [6][24]u8 = undefined;
    try options.toArguments(&args, c.gpa, &scratch);
    var r = try c.request(.create_super_stream, encodeSuperStream, .{ name, partition_streams, binding_keys, args.items });
    defer r.deinit(c.gpa);
    try r.expectOk();
}

fn encodeSuperStream(
    e: *Encoder,
    name: []const u8,
    partition_streams: []const []const u8,
    binding_keys: []const []const u8,
    args: []const Property,
) Encoder.Error!void {
    try e.string(name);
    try e.strings(partition_streams);
    try e.strings(binding_keys);
    try e.properties(args);
}

pub fn deleteSuperStream(c: *Client, name: []const u8) Error!void {
    var r = try c.request(.delete_super_stream, encodeString, .{name});
    defer r.deinit(c.gpa);
    try r.expectOk();
}

/// Topology information returned by `metadata`. Free with `deinit`.
pub const Metadata = struct {
    arena: std.heap.ArenaAllocator,
    brokers: []Broker,
    streams: []StreamMetadata,

    pub const Broker = struct {
        reference: u16,
        host: []const u8,
        port: u32,
    };

    pub const StreamMetadata = struct {
        name: []const u8,
        code: ResponseCode,
        leader: ?Broker,
        replicas: []Broker,
    };

    pub fn deinit(m: *Metadata) void {
        m.arena.deinit();
        m.* = undefined;
    }

    pub fn stream(m: *const Metadata, name: []const u8) ?StreamMetadata {
        for (m.streams) |s| {
            if (std.mem.eql(u8, s.name, name)) return s;
        }
        return null;
    }
};

/// Returns the leader and replicas of each stream.
pub fn metadata(c: *Client, streams: []const []const u8) Error!Metadata {
    var r = try c.request(.metadata, encodeStrings, .{streams});
    defer r.deinit(c.gpa);
    var m: Metadata = .{ .arena = .init(c.gpa), .brokers = &.{}, .streams = &.{} };
    errdefer m.deinit();
    const arena = m.arena.allocator();
    const d = &r.decoder;

    const broker_count = try d.int(u32);
    m.brokers = try arena.alloc(Metadata.Broker, broker_count);
    for (m.brokers) |*b| {
        b.reference = try d.int(u16);
        b.host = try arena.dupe(u8, try d.stringOrEmpty());
        b.port = try d.int(u32);
    }
    const stream_count = try d.int(u32);
    m.streams = try arena.alloc(Metadata.StreamMetadata, stream_count);
    for (m.streams) |*s| {
        s.name = try arena.dupe(u8, try d.stringOrEmpty());
        s.code = try d.responseCode();
        const leader_ref = try d.int(u16);
        s.leader = findBroker(m.brokers, leader_ref);
        const replica_count = try d.int(u32);
        var replicas: std.ArrayList(Metadata.Broker) = .empty;
        for (0..replica_count) |_| {
            const ref = try d.int(u16);
            if (findBroker(m.brokers, ref)) |b| try replicas.append(arena, b);
        }
        s.replicas = replicas.items;
    }
    return m;
}

fn findBroker(brokers: []const Metadata.Broker, reference: u16) ?Metadata.Broker {
    for (brokers) |b| {
        if (b.reference == reference) return b;
    }
    return null;
}

fn encodeStrings(e: *Encoder, values: []const []const u8) Encoder.Error!void {
    try e.strings(values);
}

/// Whether a stream exists and is available.
pub fn streamExists(c: *Client, stream: []const u8) Error!bool {
    var m = try c.metadata(&.{stream});
    defer m.deinit();
    const s = m.stream(stream) orelse return false;
    return switch (s.code) {
        .ok => true,
        .stream_does_not_exist => false,
        else => |code| {
            try code.check();
            unreachable;
        },
    };
}

pub const StreamStats = struct {
    /// Offset of the first chunk, -1 when the stream is empty.
    first_chunk_id: i64 = -1,
    /// Offset of the last committed chunk, -1 when the stream is empty.
    committed_chunk_id: i64 = -1,
};

/// Returns statistics of a stream (RabbitMQ 3.11 and later).
pub fn streamStats(c: *Client, stream: []const u8) Error!StreamStats {
    var r = try c.request(.stream_stats, encodeString, .{stream});
    defer r.deinit(c.gpa);
    try r.expectOk();
    var stats: StreamStats = .{};
    const count = try r.decoder.int(u32);
    for (0..count) |_| {
        const key = try r.decoder.stringOrEmpty();
        const value = try r.decoder.int(i64);
        if (std.mem.eql(u8, key, "first_chunk_id")) stats.first_chunk_id = value;
        if (std.mem.eql(u8, key, "committed_chunk_id")) stats.committed_chunk_id = value;
    }
    return stats;
}

/// Returns the last publishing id stored for a named publisher, or 0.
pub fn queryPublisherSequence(c: *Client, reference: []const u8, stream: []const u8) Error!u64 {
    var r = try c.request(.query_publisher_sequence, encodeTwoStrings, .{ reference, stream });
    defer r.deinit(c.gpa);
    try r.expectOk();
    return r.decoder.int(u64);
}

fn encodeTwoStrings(e: *Encoder, a: []const u8, b: []const u8) Encoder.Error!void {
    try e.string(a);
    try e.string(b);
}

/// Returns the offset stored for a consumer reference, or `null` if none.
pub fn queryOffset(c: *Client, reference: []const u8, stream: []const u8) Error!?u64 {
    var r = try c.request(.query_offset, encodeTwoStrings, .{ reference, stream });
    defer r.deinit(c.gpa);
    const code = try r.code();
    if (code == .no_offset) return null;
    try code.check();
    return try r.decoder.int(u64);
}

/// Stores an offset for a consumer reference. The broker does not answer.
pub fn storeOffset(c: *Client, reference: []const u8, stream: []const u8, offset: u64) Error!void {
    try c.lockWrite();
    defer c.write_mutex.unlock(c.io);
    var e = c.frame.begin(Command.store_offset.key(), 1) catch return error.OutOfMemory;
    encodeStoreOffset(&e, reference, stream, offset) catch return error.OutOfMemory;
    try c.writeFrameLocked(c.frame.finish());
}

fn encodeStoreOffset(e: *Encoder, reference: []const u8, stream: []const u8, offset: u64) Encoder.Error!void {
    try e.string(reference);
    try e.string(stream);
    try e.int(u64, offset);
}

/// Returns the partition streams of a super stream, in order. The caller
/// owns the result; free it with `freeStrings`.
pub fn partitions(c: *Client, super_stream: []const u8) Error![][]u8 {
    var r = try c.request(.partitions, encodeString, .{super_stream});
    defer r.deinit(c.gpa);
    try r.expectOk();
    return c.decodeOwnedStrings(&r.decoder);
}

/// Returns the partition streams a routing key is bound to. The caller
/// owns the result; free it with `freeStrings`.
pub fn route(c: *Client, routing_key: []const u8, super_stream: []const u8) Error![][]u8 {
    var r = try c.request(.route, encodeTwoStrings, .{ routing_key, super_stream });
    defer r.deinit(c.gpa);
    try r.expectOk();
    return c.decodeOwnedStrings(&r.decoder);
}

fn decodeOwnedStrings(c: *Client, d: *Decoder) Error![][]u8 {
    const count = try d.int(u32);
    const list = try c.gpa.alloc([]u8, count);
    var n: usize = 0;
    errdefer {
        for (list[0..n]) |s| c.gpa.free(s);
        c.gpa.free(list);
    }
    while (n < count) : (n += 1) list[n] = try c.gpa.dupe(u8, try d.stringOrEmpty());
    return list;
}

pub fn freeStrings(gpa: Allocator, list: []const []u8) void {
    for (list) |s| gpa.free(s);
    gpa.free(list);
}

/// Resolves an offset specification to a concrete offset (RabbitMQ 4.3
/// and later). `properties` accepts `filter.N`, `match-unfiltered` and
/// `chunk_selector`.
pub fn resolveOffsetSpec(c: *Client, stream: []const u8, spec: OffsetSpec, properties: []const Property) Error!u64 {
    var r = try c.request(.resolve_offset_spec, encodeResolveOffsetSpec, .{ stream, spec, properties });
    defer r.deinit(c.gpa);
    try r.expectOk();
    _ = try r.decoder.int(u16);
    return r.decoder.int(u64);
}

fn encodeResolveOffsetSpec(e: *Encoder, stream: []const u8, spec: OffsetSpec, properties: []const Property) Encoder.Error!void {
    try e.string(stream);
    try spec.encode(e);
    if (properties.len > 0) try e.properties(properties);
}

// Low-level publisher and subscription commands, used by `Publisher` and
// `Consumer`.

pub fn declarePublisher(c: *Client, id: u8, reference: ?[]const u8, stream: []const u8) Error!void {
    var r = try c.request(.declare_publisher, encodeDeclarePublisher, .{ id, reference, stream });
    defer r.deinit(c.gpa);
    try r.expectOk();
}

fn encodeDeclarePublisher(e: *Encoder, id: u8, reference: ?[]const u8, stream: []const u8) Encoder.Error!void {
    try e.int(u8, id);
    try e.string(reference orelse "");
    try e.string(stream);
}

pub fn deletePublisher(c: *Client, id: u8) Error!void {
    var r = try c.request(.delete_publisher, encodeId, .{id});
    defer r.deinit(c.gpa);
    try r.expectOk();
}

fn encodeId(e: *Encoder, id: u8) Encoder.Error!void {
    try e.int(u8, id);
}

/// Sends a `publish` frame whose messages are already encoded in
/// `messages` (see `Publisher` for the layout).
pub fn sendPublish(c: *Client, publisher_id: u8, version: u16, count: u32, messages: []const u8) Error!void {
    const size = 2 + 2 + 1 + 4 + messages.len;
    if (c.frame_max != 0 and size + 4 > c.frame_max) return error.FrameTooLarge;
    try c.lockWrite();
    defer c.write_mutex.unlock(c.io);
    const w = c.transport.writer();
    const old = c.io.swapCancelProtection(.blocked);
    defer _ = c.io.swapCancelProtection(old);
    c.writePublish(w, size, publisher_id, version, count, messages) catch return c.writeFailed();
}

fn writePublish(c: *Client, w: *Io.Writer, size: usize, publisher_id: u8, version: u16, count: u32, messages: []const u8) Io.Writer.Error!void {
    try w.writeInt(u32, @intCast(size), .big);
    try w.writeInt(u16, Command.publish.key(), .big);
    try w.writeInt(u16, version, .big);
    try w.writeByte(publisher_id);
    try w.writeInt(u32, count, .big);
    try w.writeAll(messages);
    try c.transport.flush();
}

pub fn subscribe(
    c: *Client,
    id: u8,
    stream: []const u8,
    offset: OffsetSpec,
    initial_credit: u16,
    properties: []const Property,
) Error!void {
    var r = try c.request(.subscribe, encodeSubscribe, .{ id, stream, offset, initial_credit, properties });
    defer r.deinit(c.gpa);
    try r.expectOk();
}

fn encodeSubscribe(e: *Encoder, id: u8, stream: []const u8, offset: OffsetSpec, initial_credit: u16, properties: []const Property) Encoder.Error!void {
    try e.int(u8, id);
    try e.string(stream);
    try offset.encode(e);
    try e.int(u16, initial_credit);
    if (properties.len > 0) try e.properties(properties);
}

pub fn unsubscribe(c: *Client, id: u8) Error!void {
    var r = try c.request(.unsubscribe, encodeId, .{id});
    defer r.deinit(c.gpa);
    try r.expectOk();
}

/// Grants `credit` more chunks to a subscription. The broker only answers
/// on error.
pub fn credit(c: *Client, subscription_id: u8, amount: u16) Error!void {
    try c.lockWrite();
    defer c.write_mutex.unlock(c.io);
    var e = c.frame.begin(Command.credit.key(), 1) catch return error.OutOfMemory;
    e.int(u8, subscription_id) catch return error.OutOfMemory;
    e.int(u16, amount) catch return error.OutOfMemory;
    try c.writeFrameLocked(c.frame.finish());
}

// Registration of publishers and consumers

pub fn registerPublisher(c: *Client, p: *Publisher) Error!u8 {
    c.state_mutex.lockUncancelable(c.io);
    defer c.state_mutex.unlock(c.io);
    if (c.close_reason != null) return error.ConnectionClosed;
    for (&c.publishers, 0..) |*slot, i| {
        if (slot.* == null) {
            slot.* = p;
            return @intCast(i);
        }
    }
    return error.TooManyPublishers;
}

pub fn unregisterPublisher(c: *Client, id: u8) void {
    c.state_mutex.lockUncancelable(c.io);
    defer c.state_mutex.unlock(c.io);
    c.publishers[id] = null;
}

pub fn registerConsumer(c: *Client, consumer: *Consumer) Error!u8 {
    c.state_mutex.lockUncancelable(c.io);
    defer c.state_mutex.unlock(c.io);
    if (c.close_reason != null) return error.ConnectionClosed;
    for (&c.consumers, 0..) |*slot, i| {
        if (slot.* == null) {
            slot.* = consumer;
            return @intCast(i);
        }
    }
    return error.TooManyConsumers;
}

pub fn unregisterConsumer(c: *Client, id: u8) void {
    c.state_mutex.lockUncancelable(c.io);
    defer c.state_mutex.unlock(c.io);
    c.consumers[id] = null;
}

// Request/response plumbing

fn lockWrite(c: *Client) Error!void {
    c.write_mutex.lock(c.io) catch return error.Canceled;
    if (!c.isOpen()) {
        c.write_mutex.unlock(c.io);
        return error.ConnectionClosed;
    }
}

/// Writes a complete frame. The caller holds `write_mutex`.
fn writeFrameLocked(c: *Client, frame: []const u8) Error!void {
    const old = c.io.swapCancelProtection(.blocked);
    defer _ = c.io.swapCancelProtection(old);
    const w = c.transport.writer();
    w.writeAll(frame) catch return c.writeFailed();
    c.transport.flush() catch return c.writeFailed();
}

fn writeFailed(c: *Client) Error {
    // Makes the reader task notice the failure and close the client.
    c.transport.stream.shutdown(c.io, .both) catch {};
    return error.ConnectionClosed;
}

/// Sends a request and waits for its response. `encodeBody` writes the
/// body after the correlation id, called as `encodeBody(encoder, args...)`.
pub fn request(c: *Client, command: Command, comptime encodeBody: anytype, args: anytype) Error!Response {
    var pending: Pending = .{};
    const correlation_id = blk: {
        c.state_mutex.lockUncancelable(c.io);
        defer c.state_mutex.unlock(c.io);
        if (c.close_reason != null) return error.ConnectionClosed;
        const id = c.next_correlation_id;
        c.next_correlation_id +%= 1;
        if (c.next_correlation_id == 0) c.next_correlation_id = 1;
        try c.pending.put(c.gpa, id, &pending);
        break :blk id;
    };

    c.sendRequest(command, correlation_id, encodeBody, args) catch |err| {
        _ = c.removePending(correlation_id, &pending);
        return err;
    };

    const timeout: Io.Timeout = .{ .duration = .{ .raw = c.rpc_timeout, .clock = .awake } };
    pending.event.waitTimeout(c.io, timeout) catch |err| {
        if (c.removePending(correlation_id, &pending)) {
            return switch (err) {
                error.Timeout => error.Timeout,
                error.Canceled => error.Canceled,
            };
        }
        // The response arrived while timing out; use it.
    };
    if (pending.err) |err| return err;
    const frame = pending.frame.?;
    var d: Decoder = .init(frame);
    // Size prefix is not part of the frame buffer: key, version, correlation id.
    d.skip(8) catch {
        c.gpa.free(frame);
        return error.MalformedFrame;
    };
    return .{ .frame = frame, .decoder = d };
}

fn sendRequest(c: *Client, command: Command, correlation_id: u32, comptime encodeBody: anytype, args: anytype) Error!void {
    try c.lockWrite();
    defer c.write_mutex.unlock(c.io);
    var e = c.frame.begin(command.key(), 1) catch return error.OutOfMemory;
    e.int(u32, correlation_id) catch return error.OutOfMemory;
    @call(.auto, encodeBody, .{&e} ++ args) catch return error.OutOfMemory;
    const frame = c.frame.finish();
    // Until `open` succeeds the broker only accepts small frames.
    if (c.frame_max != 0 and frame.len > c.frame_max) return error.FrameTooLarge;
    try c.writeFrameLocked(frame);
}

/// Removes a pending request. Returns false if the reader task already
/// completed it, in which case its result is in `pending`.
fn removePending(c: *Client, correlation_id: u32, pending: *Pending) bool {
    c.state_mutex.lockUncancelable(c.io);
    defer c.state_mutex.unlock(c.io);
    if (c.pending.remove(correlation_id)) return true;
    if (pending.frame) |f| {
        c.gpa.free(f);
        pending.frame = null;
    }
    return false;
}

fn completePending(c: *Client, correlation_id: u32, frame: []u8) void {
    c.state_mutex.lockUncancelable(c.io);
    defer c.state_mutex.unlock(c.io);
    const entry = c.pending.fetchRemove(correlation_id) orelse {
        log.debug("response with unknown correlation id {d}", .{correlation_id});
        c.gpa.free(frame);
        return;
    };
    entry.value.frame = frame;
    entry.value.event.set(c.io);
}

// Reader task

fn touch(c: *Client) void {
    c.last_read_ms.store(Io.Clock.awake.now(c.io).toMilliseconds(), .monotonic);
}

/// Upper bound on incoming frames when no maximum frame size is negotiated.
const max_frame_without_limit = 64 * 1024 * 1024;

fn readLoop(c: *Client) void {
    const reason: CloseReason = if (c.readFrames()) |_| .connection_lost else |err| if (c.closing.load(.monotonic)) .requested else switch (err) {
        error.MalformedFrame, error.FrameTooLarge => .protocol_error,
        error.ServerClosed => .{ .server = c.server_close_code },
        else => .connection_lost,
    };
    c.shutdown(reason);
}

fn readFrames(c: *Client) !void {
    const r = c.transport.reader();
    while (true) {
        const size = try r.takeInt(u32, .big);
        if (size < 4) return error.MalformedFrame;
        const limit = if (c.frame_max == 0) max_frame_without_limit else c.frame_max;
        if (size > limit) return error.FrameTooLarge;
        const frame = try c.gpa.alloc(u8, size);
        r.readSliceAll(frame) catch |err| {
            c.gpa.free(frame);
            return err;
        };
        c.touch();
        try c.dispatch(frame);
    }
}

/// Handles one frame. Takes ownership of `frame`.
fn dispatch(c: *Client, frame: []u8) !void {
    var owned = true;
    defer if (owned) c.gpa.free(frame);

    var d: Decoder = .init(frame);
    const header: protocol.FrameHeader = try .decode(&d);

    if (header.isResponse()) {
        switch (header.command()) {
            .credit => {
                const code = try d.responseCode();
                const id = try d.int(u8);
                log.warn("credit for subscription {d} rejected: {f}", .{ id, code });
            },
            else => {
                const correlation_id = try d.int(u32);
                owned = false;
                c.completePending(correlation_id, frame);
            },
        }
        return;
    }

    switch (header.command()) {
        .deliver => {
            const id = try d.int(u8);
            const committed: ?u64 = if (header.version >= 2) try d.int(u64) else null;
            c.state_mutex.lockUncancelable(c.io);
            const consumer = c.consumers[id];
            if (consumer) |con| {
                // Ownership passes to the consumer while the state lock
                // keeps it registered.
                owned = false;
                con.handleDeliver(frame, d.pos, committed);
            }
            c.state_mutex.unlock(c.io);
            if (consumer == null) log.debug("delivery for unknown subscription {d}", .{id});
        },
        .publish_confirm => {
            const id = try d.int(u8);
            const count = try d.int(u32);
            const ids = try d.take(@as(usize, count) * 8);
            c.state_mutex.lockUncancelable(c.io);
            defer c.state_mutex.unlock(c.io);
            if (c.publishers[id]) |p| p.handleConfirm(ids);
        },
        .publish_error => {
            const id = try d.int(u8);
            const count = try d.int(u32);
            const errors = try d.take(@as(usize, count) * 10);
            c.state_mutex.lockUncancelable(c.io);
            defer c.state_mutex.unlock(c.io);
            if (c.publishers[id]) |p| p.handleError(errors);
        },
        .metadata_update => {
            const code = try d.responseCode();
            const stream = try d.stringOrEmpty();
            c.handleMetadataUpdate(stream, code);
        },
        .heartbeat => {},
        .tune => {
            c.server_frame_max = try d.int(u32);
            c.server_heartbeat = try d.int(u32);
            c.tune_event.set(c.io);
        },
        .close => {
            const correlation_id = try d.int(u32);
            const code = try d.responseCode();
            const reason = try d.stringOrEmpty();
            log.info("broker closed the connection: {f} {s}", .{ code, reason });
            c.server_close_code = code;
            c.respond(.close, correlation_id, .ok, .code_only) catch {};
            return error.ServerClosed;
        },
        .consumer_update => {
            const correlation_id = try d.int(u32);
            const id = try d.int(u8);
            const active = (try d.int(u8)) != 0;
            c.tasks.concurrent(c.io, handleConsumerUpdate, .{ c, correlation_id, id, active }) catch {
                // Without a free task, answer from the reader task.
                handleConsumerUpdate(c, correlation_id, id, active) catch {};
            };
        },
        else => log.debug("ignoring frame with key 0x{x:0>4}", .{header.key}),
    }
}

fn handleMetadataUpdate(c: *Client, stream: []const u8, code: ResponseCode) void {
    log.info("metadata update for stream '{s}': {f}", .{ stream, code });
    {
        c.state_mutex.lockUncancelable(c.io);
        defer c.state_mutex.unlock(c.io);
        for (c.publishers) |slot| {
            const p = slot orelse continue;
            if (std.mem.eql(u8, p.stream, stream)) p.handleStreamUnavailable(code);
        }
        for (c.consumers) |slot| {
            const con = slot orelse continue;
            if (std.mem.eql(u8, con.stream, stream)) con.handleStreamUnavailable(code);
        }
    }
    if (c.on_metadata_update) |l| l.callback(l.context, c, stream, code);
}

fn handleConsumerUpdate(c: *Client, correlation_id: u32, id: u8, active: bool) Io.Cancelable!void {
    const consumer = blk: {
        c.state_mutex.lockUncancelable(c.io);
        defer c.state_mutex.unlock(c.io);
        break :blk c.consumers[id];
    };
    const spec: ?OffsetSpec = if (consumer) |con| con.handleConsumerUpdate(active) else null;
    c.respond(.consumer_update, correlation_id, .ok, if (spec) |o| .{ .offset = o } else .offset_none) catch |err| {
        log.warn("failed to answer consumer update: {t}", .{err});
    };
}

/// Answers a request initiated by the broker.
const ResponseBody = union(enum) {
    code_only,
    /// `consumer_update` response without a starting point.
    offset_none,
    offset: OffsetSpec,
};

fn respond(c: *Client, command: Command, correlation_id: u32, code: ResponseCode, body: ResponseBody) Error!void {
    try c.lockWrite();
    defer c.write_mutex.unlock(c.io);
    var e = c.frame.begin(command.responseKey(), 1) catch return error.OutOfMemory;
    e.int(u32, correlation_id) catch return error.OutOfMemory;
    e.int(u16, code.toInt()) catch return error.OutOfMemory;
    switch (body) {
        .code_only => {},
        .offset_none => e.int(u16, @backingInt(protocol.OffsetType.none)) catch return error.OutOfMemory,
        .offset => |o| o.encode(&e) catch return error.OutOfMemory,
    }
    try c.writeFrameLocked(c.frame.finish());
}

/// Marks the client closed, fails pending requests and detaches publishers
/// and consumers. Idempotent.
fn shutdown(c: *Client, reason: CloseReason) void {
    {
        c.state_mutex.lockUncancelable(c.io);
        defer c.state_mutex.unlock(c.io);
        if (c.close_reason != null) return;
        c.close_reason = reason;
        var it = c.pending.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.*.err = error.ConnectionClosed;
            entry.value_ptr.*.event.set(c.io);
        }
        c.pending.clearRetainingCapacity();
        for (c.publishers) |slot| if (slot) |p| p.handleConnectionClosed();
        for (c.consumers) |slot| if (slot) |con| con.handleConnectionClosed();
    }
    // Wake a handshake waiting for `tune`.
    c.tune_event.set(c.io);
    if (reason != .requested) {
        log.info("connection closed: {t}", .{reason});
        // Unblock the reader if the heartbeat task noticed the failure.
        c.transport.stream.shutdown(c.io, .both) catch {};
    }
    if (c.on_close) |l| l.callback(l.context, c, reason);
    c.closed.set(c.io);
}

fn heartbeatLoop(c: *Client) void {
    const interval: Io.Duration = .fromSeconds(c.heartbeat);
    while (true) {
        c.io.sleep(interval, .awake) catch return;
        if (c.closed.isSet()) return;
        const now = Io.Clock.awake.now(c.io).toMilliseconds();
        const idle = now - c.last_read_ms.load(.monotonic);
        if (idle > 2 * @as(i64, c.heartbeat) * std.time.ms_per_s) {
            c.shutdown(.heartbeat_timeout);
            return;
        }
        c.sendHeartbeat() catch return;
    }
}

fn sendHeartbeat(c: *Client) Error!void {
    try c.lockWrite();
    defer c.write_mutex.unlock(c.io);
    _ = c.frame.begin(Command.heartbeat.key(), 1) catch return error.OutOfMemory;
    try c.writeFrameLocked(c.frame.finish());
}
