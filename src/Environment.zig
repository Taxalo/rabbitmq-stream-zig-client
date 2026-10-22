//! Entry point for applications: manages a locator connection for stream
//! management and creates publishers and consumers on the right nodes.
//!
//! Publishers connect to the leader of their stream, consumers to a replica
//! when one exists. Every publisher and consumer created here owns a
//! dedicated connection, closed together with it.
//!
//! When nodes advertise an address the application cannot reach (common
//! with containers and load balancers), set `address_resolver`, or
//! `load_balancer` to always connect through the configured endpoint and
//! retry until the broker reports the wanted node. Without either, the
//! environment falls back to the configured endpoint when the advertised
//! address cannot be reached.

const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;

const protocol = @import("protocol.zig");
const Property = protocol.Property;
const Client = @import("Client.zig");
const Publisher = @import("Publisher.zig");
const Consumer = @import("Consumer.zig");
const SuperStreamPublisher = @import("SuperStreamPublisher.zig");
const SuperStreamConsumer = @import("SuperStreamConsumer.zig");

const log = std.log.scoped(.rabbitmq_stream);

const Environment = @This();

gpa: Allocator,
io: Io,
/// Owns the copied option strings.
arena: std.heap.ArenaAllocator,
options: Options,
mutex: Io.Mutex = .init,
locator: ?*Client = null,
/// Locator clients replaced after their connection failed. They are
/// freed in `deinit` because other tasks may still hold them.
retired: std.ArrayList(*Client) = .empty,
rng: std.Random.DefaultPrng,

pub const Error = Client.ConnectError || error{ NoAvailableNode, InvalidUri, UnsupportedScheme };

pub const Endpoint = struct {
    host: []const u8,
    port: u16,
};

/// Maps the address a node advertises to the address to connect to.
pub const AddressResolver = struct {
    context: ?*anyopaque = null,
    callback: *const fn (context: ?*anyopaque, advertised: Endpoint) Endpoint,
};

pub const Options = struct {
    /// Settings for every connection. `host` and `port` are the first
    /// endpoint used for the locator connection.
    client: Client.Options = .{},
    /// Alternative to `client` and `endpoints`: connection URIs such as
    /// `rabbitmq-stream://user:password@host:5552/vhost`. The first URI
    /// sets the credentials, virtual host and TLS; all of them are
    /// endpoints for the locator connection.
    uris: []const []const u8 = &.{},
    /// More endpoints to try, in order, when the first one fails.
    endpoints: []const Endpoint = &.{},
    address_resolver: ?AddressResolver = null,
    /// Connect to every node through the configured endpoint.
    load_balancer: bool = false,
    /// Connection attempts when looking for a specific node through a
    /// load balancer.
    max_node_attempts: u32 = 10,
};

pub fn init(gpa: Allocator, io: Io, options: Options) Error!*Environment {
    const env = try gpa.create(Environment);
    errdefer gpa.destroy(env);
    var seed: u64 = undefined;
    io.random(std.mem.asBytes(&seed));
    env.* = .{
        .gpa = gpa,
        .io = io,
        .arena = .init(gpa),
        .options = options,
        .rng = .init(seed),
    };
    errdefer env.arena.deinit();
    try env.copyOptions();
    _ = try env.locatorClient();
    return env;
}

fn copyOptions(env: *Environment) (Allocator.Error || Client.UriError)!void {
    const a = env.arena.allocator();
    const o = &env.options;
    if (o.uris.len > 0) {
        o.client = try Client.optionsFromUri(a, o.uris[0], o.client);
        const endpoints = try a.alloc(Endpoint, o.uris.len - 1);
        for (endpoints, o.uris[1..]) |*dst, uri| {
            const parsed = try Client.optionsFromUri(a, uri, o.client);
            dst.* = .{ .host = parsed.host, .port = parsed.port };
        }
        o.endpoints = endpoints;
        o.uris = &.{};
    }
    o.client.host = try a.dupe(u8, o.client.host);
    o.client.username = try a.dupe(u8, o.client.username);
    o.client.password = try a.dupe(u8, o.client.password);
    o.client.virtual_host = try a.dupe(u8, o.client.virtual_host);
    if (o.client.connection_name) |n| o.client.connection_name = try a.dupe(u8, n);
    if (o.client.tls) |*tls| {
        if (tls.server_name) |n| tls.server_name = try a.dupe(u8, n);
        if (tls.ca == .file) tls.ca = .{ .file = try a.dupe(u8, tls.ca.file) };
    }
    const props = try a.alloc(Property, o.client.client_properties.len);
    for (props, o.client.client_properties) |*dst, src| dst.* = .{
        .key = try a.dupe(u8, src.key),
        .value = try a.dupe(u8, src.value),
    };
    o.client.client_properties = props;
    const endpoints = try a.alloc(Endpoint, o.endpoints.len);
    for (endpoints, o.endpoints) |*dst, src| dst.* = .{ .host = try a.dupe(u8, src.host), .port = src.port };
    o.endpoints = endpoints;
}

/// Closes the locator connection. Publishers and consumers created by the
/// environment must be closed first.
pub fn deinit(env: *Environment) void {
    if (env.locator) |c| c.close();
    for (env.retired.items) |c| c.close();
    env.retired.deinit(env.gpa);
    env.arena.deinit();
    env.gpa.destroy(env);
}

/// Returns the connection used for stream management, reconnecting if it
/// was closed.
pub fn locatorClient(env: *Environment) Error!*Client {
    try env.mutex.lock(env.io);
    defer env.mutex.unlock(env.io);
    if (env.locator) |c| {
        if (c.isOpen()) return c;
        try env.retired.append(env.gpa, c);
        env.locator = null;
    }
    var last_err: Error = error.NoAvailableNode;
    var options = env.options.client;
    options.connection_name = env.options.client.connection_name orelse "rabbitmq-stream-zig-locator";
    for (0..1 + env.options.endpoints.len) |i| {
        if (i > 0) {
            options.host = env.options.endpoints[i - 1].host;
            options.port = env.options.endpoints[i - 1].port;
        }
        const c = Client.connect(env.gpa, env.io, options) catch |err| {
            log.debug("locator connection to {s}:{d} failed: {t}", .{ options.host, options.port, err });
            last_err = err;
            continue;
        };
        env.locator = c;
        return c;
    }
    return last_err;
}

// Stream management, through the locator connection.

pub fn createStream(env: *Environment, stream: []const u8, options: Client.StreamOptions) Error!void {
    return (try env.locatorClient()).createStream(stream, options);
}

pub fn deleteStream(env: *Environment, stream: []const u8) Error!void {
    return (try env.locatorClient()).deleteStream(stream);
}

pub fn streamExists(env: *Environment, stream: []const u8) Error!bool {
    return (try env.locatorClient()).streamExists(stream);
}

pub fn streamStats(env: *Environment, stream: []const u8) Error!Client.StreamStats {
    return (try env.locatorClient()).streamStats(stream);
}

pub fn queryOffset(env: *Environment, reference: []const u8, stream: []const u8) Error!?u64 {
    return (try env.locatorClient()).queryOffset(reference, stream);
}

pub fn queryPublisherSequence(env: *Environment, reference: []const u8, stream: []const u8) Error!u64 {
    return (try env.locatorClient()).queryPublisherSequence(reference, stream);
}

pub fn metadata(env: *Environment, streams: []const []const u8) Error!Client.Metadata {
    return (try env.locatorClient()).metadata(streams);
}

/// Creates a super stream with `partition_count` partitions named
/// `<name>-0` ... `<name>-<n-1>`, bound with the keys `0` ... `<n-1>`.
pub fn createSuperStream(env: *Environment, name: []const u8, partition_count: u32, options: Client.StreamOptions) Error!void {
    var keys: std.ArrayList([]const u8) = .empty;
    defer {
        for (keys.items) |k| env.gpa.free(k);
        keys.deinit(env.gpa);
    }
    for (0..partition_count) |i| {
        const key = try env.gpa.print("{d}", .{i});
        keys.append(env.gpa, key) catch |err| {
            env.gpa.free(key);
            return err;
        };
    }
    return env.createSuperStreamWithKeys(name, keys.items, options);
}

/// Creates a super stream with one partition per binding key, named
/// `<name>-<key>`.
pub fn createSuperStreamWithKeys(env: *Environment, name: []const u8, binding_keys: []const []const u8, options: Client.StreamOptions) Error!void {
    var names: std.ArrayList([]const u8) = .empty;
    defer {
        for (names.items) |n| env.gpa.free(n);
        names.deinit(env.gpa);
    }
    for (binding_keys) |key| {
        const partition = try env.gpa.print("{s}-{s}", .{ name, key });
        names.append(env.gpa, partition) catch |err| {
            env.gpa.free(partition);
            return err;
        };
    }
    return (try env.locatorClient()).createSuperStream(name, names.items, binding_keys, options);
}

pub fn deleteSuperStream(env: *Environment, name: []const u8) Error!void {
    return (try env.locatorClient()).deleteSuperStream(name);
}

/// The caller owns the result; free it with `Client.freeStrings`.
pub fn partitions(env: *Environment, super_stream: []const u8) Error![][]u8 {
    return (try env.locatorClient()).partitions(super_stream);
}

/// The caller owns the result; free it with `Client.freeStrings`.
pub fn route(env: *Environment, routing_key: []const u8, super_stream: []const u8) Error![][]u8 {
    return (try env.locatorClient()).route(routing_key, super_stream);
}

// Publishers and consumers

/// Creates a publisher connected to the leader of `stream`.
pub fn createPublisher(env: *Environment, stream: []const u8, options: Publisher.Options) (Error || Publisher.Error)!*Publisher {
    var m = try env.metadata(&.{stream});
    defer m.deinit();
    const info = m.stream(stream) orelse return error.StreamDoesNotExist;
    try info.code.check();
    const leader = info.leader orelse return error.StreamNotAvailable;
    const client = try env.connectToNode(leader, "publisher");
    const p = Publisher.init(client, stream, options) catch |err| {
        client.close();
        return err;
    };
    p.owns_client = true;
    return p;
}

/// Creates a consumer connected to a replica of `stream`, or to the leader
/// when the stream has no replica.
pub fn createConsumer(env: *Environment, stream: []const u8, options: Consumer.Options) (Error || Consumer.Error)!*Consumer {
    return env.createConsumerInto(stream, options, null);
}

/// Like `createConsumer`, delivering into a shared inbox.
pub fn createConsumerInto(env: *Environment, stream: []const u8, options: Consumer.Options, inbox: ?*Consumer.Inbox) (Error || Consumer.Error)!*Consumer {
    var m = try env.metadata(&.{stream});
    defer m.deinit();
    const info = m.stream(stream) orelse return error.StreamDoesNotExist;
    try info.code.check();
    const node = if (info.replicas.len > 0)
        info.replicas[env.rng.random().uintLessThan(usize, info.replicas.len)]
    else
        info.leader orelse return error.StreamNotAvailable;
    const client = try env.connectToNode(node, "consumer");
    const c = (if (inbox) |i| Consumer.initWithInbox(client, stream, options, i) else Consumer.init(client, stream, options)) catch |err| {
        client.close();
        return err;
    };
    c.owns_client = true;
    return c;
}

pub fn createSuperStreamPublisher(env: *Environment, super_stream: []const u8, options: SuperStreamPublisher.Options) SuperStreamPublisher.Error!*SuperStreamPublisher {
    return SuperStreamPublisher.init(env, super_stream, options);
}

pub fn createSuperStreamConsumer(env: *Environment, super_stream: []const u8, options: Consumer.Options) SuperStreamConsumer.Error!*SuperStreamConsumer {
    return SuperStreamConsumer.init(env, super_stream, options);
}

/// Opens a connection to the node that advertises `broker`.
fn connectToNode(env: *Environment, broker: Client.Metadata.Broker, kind: []const u8) Error!*Client {
    var options = env.options.client;
    var name_buf: [128]u8 = undefined;
    options.connection_name = env.options.client.connection_name orelse
        (std.fmt.bufPrint(&name_buf, "rabbitmq-stream-zig-{s}", .{kind}) catch unreachable);
    const advertised: Endpoint = .{ .host = broker.host, .port = @intCast(broker.port) };

    if (!env.options.load_balancer) {
        const target = if (env.options.address_resolver) |r| r.callback(r.context, advertised) else advertised;
        options.host = target.host;
        options.port = target.port;
        if (Client.connect(env.gpa, env.io, options)) |c| {
            return c;
        } else |err| {
            if (env.options.address_resolver != null) return err;
            log.debug("cannot reach advertised node {s}:{d} ({t}); using the configured endpoint", .{ advertised.host, advertised.port, err });
        }
    }

    options.host = env.options.client.host;
    options.port = env.options.client.port;
    for (0..env.options.max_node_attempts) |_| {
        const c = try Client.connect(env.gpa, env.io, options);
        if (isNode(c, advertised)) return c;
        c.close();
    }
    return error.NoAvailableNode;
}

fn isNode(c: *Client, node: Endpoint) bool {
    const host = c.connectionProperty("advertised_host") orelse return true;
    const port_text = c.connectionProperty("advertised_port") orelse return true;
    const port = std.fmt.parseInt(u16, port_text, 10) catch return true;
    return std.mem.eql(u8, host, node.host) and port == node.port;
}
