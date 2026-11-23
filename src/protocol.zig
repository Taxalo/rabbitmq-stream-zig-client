//! Wire-level definitions and codecs for the RabbitMQ Stream protocol.
//!
//! Reference: `deps/rabbitmq_stream/docs/PROTOCOL.adoc` and
//! `deps/rabbitmq_stream_common/src/rabbit_stream_core.erl` in
//! `rabbitmq/rabbitmq-server`. All integers are big endian.

const std = @import("std");
const Io = std.Io;
const assert = std.debug.assert;

/// Bit set on the key of every response frame.
pub const response_flag: u16 = 0x8000;

/// Size of the `uint32` frame length prefix.
pub const frame_size_len = 4;

/// The broker enforces this frame size until `open` completes.
pub const initial_frame_max = 8192;

pub const default_port = 5552;
pub const default_tls_port = 5551;
pub const default_frame_max: u32 = 1024 * 1024;
pub const default_heartbeat: u32 = 60;

/// Maximum length of publisher and consumer references.
pub const max_reference_len = 256;

pub const Command = enum(u16) {
    declare_publisher = 0x0001,
    publish = 0x0002,
    publish_confirm = 0x0003,
    publish_error = 0x0004,
    query_publisher_sequence = 0x0005,
    delete_publisher = 0x0006,
    subscribe = 0x0007,
    deliver = 0x0008,
    credit = 0x0009,
    store_offset = 0x000a,
    query_offset = 0x000b,
    unsubscribe = 0x000c,
    create_stream = 0x000d,
    delete_stream = 0x000e,
    metadata = 0x000f,
    metadata_update = 0x0010,
    peer_properties = 0x0011,
    sasl_handshake = 0x0012,
    sasl_authenticate = 0x0013,
    tune = 0x0014,
    open = 0x0015,
    close = 0x0016,
    heartbeat = 0x0017,
    route = 0x0018,
    partitions = 0x0019,
    consumer_update = 0x001a,
    exchange_command_versions = 0x001b,
    stream_stats = 0x001c,
    create_super_stream = 0x001d,
    delete_super_stream = 0x001e,
    resolve_offset_spec = 0x001f,
    _,

    pub fn key(c: Command) u16 {
        return @backingInt(c);
    }

    pub fn responseKey(c: Command) u16 {
        return @backingInt(c) | response_flag;
    }

    /// Commands the client advertises in `exchange_command_versions`,
    /// with the minimum and maximum version it implements.
    pub const client_versions = [_]VersionRange{
        .{ .command = .declare_publisher, .min = 1, .max = 1 },
        .{ .command = .publish, .min = 1, .max = 2 },
        .{ .command = .query_publisher_sequence, .min = 1, .max = 1 },
        .{ .command = .delete_publisher, .min = 1, .max = 1 },
        .{ .command = .subscribe, .min = 1, .max = 1 },
        .{ .command = .deliver, .min = 1, .max = 2 },
        .{ .command = .credit, .min = 1, .max = 1 },
        .{ .command = .store_offset, .min = 1, .max = 1 },
        .{ .command = .query_offset, .min = 1, .max = 1 },
        .{ .command = .unsubscribe, .min = 1, .max = 1 },
        .{ .command = .create_stream, .min = 1, .max = 1 },
        .{ .command = .delete_stream, .min = 1, .max = 1 },
        .{ .command = .metadata, .min = 1, .max = 1 },
        .{ .command = .route, .min = 1, .max = 1 },
        .{ .command = .partitions, .min = 1, .max = 1 },
        .{ .command = .consumer_update, .min = 1, .max = 1 },
        .{ .command = .stream_stats, .min = 1, .max = 1 },
        .{ .command = .create_super_stream, .min = 1, .max = 1 },
        .{ .command = .delete_super_stream, .min = 1, .max = 1 },
        .{ .command = .resolve_offset_spec, .min = 1, .max = 1 },
    };
};

pub const VersionRange = struct {
    command: Command,
    min: u16,
    max: u16,
};

pub const ResponseCode = enum(u16) {
    ok = 0x01,
    stream_does_not_exist = 0x02,
    subscription_id_already_exists = 0x03,
    subscription_id_does_not_exist = 0x04,
    stream_already_exists = 0x05,
    stream_not_available = 0x06,
    sasl_mechanism_not_supported = 0x07,
    authentication_failure = 0x08,
    sasl_error = 0x09,
    sasl_challenge = 0x0a,
    sasl_authentication_failure_loopback = 0x0b,
    virtual_host_access_failure = 0x0c,
    unknown_frame = 0x0d,
    frame_too_large = 0x0e,
    internal_error = 0x0f,
    access_refused = 0x10,
    precondition_failed = 0x11,
    publisher_does_not_exist = 0x12,
    no_offset = 0x13,
    sasl_cannot_change_mechanism = 0x14,
    sasl_cannot_change_username = 0x15,
    _,

    pub fn fromInt(code: u16) ResponseCode {
        return @fromBackingInt(code);
    }

    pub fn toInt(code: ResponseCode) u16 {
        return @backingInt(code);
    }

    /// Converts a non-`ok` response code into the matching error.
    pub fn check(code: ResponseCode) Error!void {
        return switch (code) {
            .ok => {},
            .stream_does_not_exist => error.StreamDoesNotExist,
            .subscription_id_already_exists => error.SubscriptionIdAlreadyExists,
            .subscription_id_does_not_exist => error.SubscriptionIdDoesNotExist,
            .stream_already_exists => error.StreamAlreadyExists,
            .stream_not_available => error.StreamNotAvailable,
            .sasl_mechanism_not_supported => error.SaslMechanismNotSupported,
            .authentication_failure => error.AuthenticationFailure,
            .sasl_error => error.SaslError,
            .sasl_challenge => error.SaslChallenge,
            .sasl_authentication_failure_loopback => error.AuthenticationFailureLoopback,
            .virtual_host_access_failure => error.VirtualHostAccessFailure,
            .unknown_frame => error.UnknownFrame,
            .frame_too_large => error.FrameTooLarge,
            .internal_error => error.InternalError,
            .access_refused => error.AccessRefused,
            .precondition_failed => error.PreconditionFailed,
            .publisher_does_not_exist => error.PublisherDoesNotExist,
            .no_offset => error.NoOffset,
            .sasl_cannot_change_mechanism => error.SaslCannotChangeMechanism,
            .sasl_cannot_change_username => error.SaslCannotChangeUsername,
            _ => error.UnknownResponseCode,
        };
    }

    pub const Error = error{
        StreamDoesNotExist,
        SubscriptionIdAlreadyExists,
        SubscriptionIdDoesNotExist,
        StreamAlreadyExists,
        StreamNotAvailable,
        SaslMechanismNotSupported,
        AuthenticationFailure,
        SaslError,
        SaslChallenge,
        AuthenticationFailureLoopback,
        VirtualHostAccessFailure,
        UnknownFrame,
        FrameTooLarge,
        InternalError,
        AccessRefused,
        PreconditionFailed,
        PublisherDoesNotExist,
        NoOffset,
        SaslCannotChangeMechanism,
        SaslCannotChangeUsername,
        UnknownResponseCode,
    };

    pub fn format(code: ResponseCode, w: *Io.Writer) Io.Writer.Error!void {
        switch (code) {
            _ => try w.print("unknown(0x{x:0>4})", .{@backingInt(code)}),
            else => try w.writeAll(@tagName(code)),
        }
    }
};

pub const OffsetType = enum(u16) {
    none = 0,
    first = 1,
    last = 2,
    next = 3,
    offset = 4,
    timestamp = 5,
    _,
};

/// Where a subscription starts reading in a stream.
pub const OffsetSpec = union(enum) {
    /// The first available message in the stream.
    first,
    /// The beginning of the last chunk written to the stream.
    last,
    /// The next message written after the subscription is created.
    next,
    /// A specific offset. The broker clamps it to the stream boundaries.
    offset: u64,
    /// Erlang system time in milliseconds since the Unix epoch. Delivery
    /// starts at the first chunk written at or after this time.
    timestamp: i64,

    pub fn offsetType(spec: OffsetSpec) OffsetType {
        return switch (spec) {
            .first => .first,
            .last => .last,
            .next => .next,
            .offset => .offset,
            .timestamp => .timestamp,
        };
    }

    pub fn encode(spec: OffsetSpec, e: *Encoder) Io.Writer.Error!void {
        try e.int(u16, @backingInt(spec.offsetType()));
        switch (spec) {
            .first, .last, .next => {},
            .offset => |o| try e.int(u64, o),
            .timestamp => |t| try e.int(i64, t),
        }
    }

    pub fn decode(d: *Decoder) Decoder.Error!?OffsetSpec {
        const t: OffsetType = @fromBackingInt(try d.int(u16));
        return switch (t) {
            .none => null,
            .first => .first,
            .last => .last,
            .next => .next,
            .offset => .{ .offset = try d.int(u64) },
            .timestamp => .{ .timestamp = try d.int(i64) },
            _ => error.MalformedFrame,
        };
    }
};

/// A key/value string pair, used for peer properties, stream arguments
/// and subscription properties.
pub const Property = struct {
    key: []const u8,
    value: []const u8,

    /// Returns the value of the first property named `key`.
    pub fn find(properties: []const Property, key: []const u8) ?[]const u8 {
        for (properties) |p| {
            if (std.mem.eql(u8, p.key, key)) return p.value;
        }
        return null;
    }
};

/// Writes protocol primitives to an `Io.Writer`.
pub const Encoder = struct {
    w: *Io.Writer,

    pub const Error = Io.Writer.Error;

    pub fn int(e: *Encoder, comptime T: type, value: T) Error!void {
        try e.w.writeInt(T, value, .big);
    }

    pub fn raw(e: *Encoder, data: []const u8) Error!void {
        try e.w.writeAll(data);
    }

    /// `int16` length followed by the bytes. `null` is encoded as length -1.
    pub fn string(e: *Encoder, value: ?[]const u8) Error!void {
        const s = value orelse return e.int(i16, -1);
        assert(s.len <= std.math.maxInt(i16));
        try e.int(i16, @intCast(s.len));
        try e.w.writeAll(s);
    }

    /// `int32` length followed by the bytes. `null` is encoded as length -1.
    pub fn bytes(e: *Encoder, value: ?[]const u8) Error!void {
        const b = value orelse return e.int(i32, -1);
        assert(b.len <= std.math.maxInt(i32));
        try e.int(i32, @intCast(b.len));
        try e.w.writeAll(b);
    }

    pub fn properties(e: *Encoder, props: []const Property) Error!void {
        try e.int(u32, @intCast(props.len));
        for (props) |p| {
            try e.string(p.key);
            try e.string(p.value);
        }
    }

    pub fn strings(e: *Encoder, values: []const []const u8) Error!void {
        try e.int(u32, @intCast(values.len));
        for (values) |v| try e.string(v);
    }
};

pub fn stringSize(s: ?[]const u8) usize {
    return 2 + if (s) |v| v.len else 0;
}

pub fn bytesSize(b: ?[]const u8) usize {
    return 4 + if (b) |v| v.len else 0;
}

pub fn propertiesSize(props: []const Property) usize {
    var n: usize = 4;
    for (props) |p| n += stringSize(p.key) + stringSize(p.value);
    return n;
}

/// Reads protocol primitives from a byte slice. Returned slices point into
/// the decoded bytes.
pub const Decoder = struct {
    data: []const u8,
    pos: usize = 0,

    pub const Error = error{MalformedFrame};

    pub fn init(data: []const u8) Decoder {
        return .{ .data = data };
    }

    pub fn remaining(d: *const Decoder) usize {
        return d.data.len - d.pos;
    }

    pub fn rest(d: *Decoder) []const u8 {
        const r = d.data[d.pos..];
        d.pos = d.data.len;
        return r;
    }

    pub fn take(d: *Decoder, n: usize) Error![]const u8 {
        if (d.remaining() < n) return error.MalformedFrame;
        const s = d.data[d.pos..][0..n];
        d.pos += n;
        return s;
    }

    pub fn skip(d: *Decoder, n: usize) Error!void {
        _ = try d.take(n);
    }

    pub fn int(d: *Decoder, comptime T: type) Error!T {
        const n = @divExact(@typeInfo(T).int.bits, 8);
        const s = try d.take(n);
        return std.mem.readInt(T, s[0..n], .big);
    }

    pub fn string(d: *Decoder) Error!?[]const u8 {
        const len = try d.int(i16);
        if (len < 0) return null;
        return try d.take(@intCast(len));
    }

    /// Like `string` but treats `null` as an empty string.
    pub fn stringOrEmpty(d: *Decoder) Error![]const u8 {
        return (try d.string()) orelse "";
    }

    pub fn bytes(d: *Decoder) Error!?[]const u8 {
        const len = try d.int(i32);
        if (len < 0) return null;
        return try d.take(@intCast(len));
    }

    pub fn responseCode(d: *Decoder) Error!ResponseCode {
        return @fromBackingInt(try d.int(u16));
    }
};

/// The fixed header of every frame (after the size prefix).
pub const FrameHeader = struct {
    key: u16,
    version: u16,

    pub fn command(h: FrameHeader) Command {
        return @fromBackingInt(h.key & ~response_flag);
    }

    pub fn isResponse(h: FrameHeader) bool {
        return h.key & response_flag != 0;
    }

    pub fn decode(d: *Decoder) Decoder.Error!FrameHeader {
        return .{ .key = try d.int(u16), .version = try d.int(u16) };
    }
};

/// Builds a frame in an allocating buffer, so the size prefix can be
/// written once the body is known.
pub const FrameBuilder = struct {
    out: Io.Writer.Allocating,

    pub fn init(gpa: std.mem.Allocator) FrameBuilder {
        return .{ .out = .init(gpa) };
    }

    pub fn deinit(b: *FrameBuilder) void {
        b.out.deinit();
    }

    /// Starts a new frame, discarding any previous one. Returns an
    /// `Encoder` positioned after the key and version.
    pub fn begin(b: *FrameBuilder, key: u16, version: u16) Encoder.Error!Encoder {
        b.out.clearRetainingCapacity();
        var e: Encoder = .{ .w = &b.out.writer };
        try e.int(u32, 0);
        try e.int(u16, key);
        try e.int(u16, version);
        return e;
    }

    /// Patches the size prefix and returns the complete frame.
    pub fn finish(b: *FrameBuilder) []const u8 {
        const frame = b.out.written();
        std.mem.writeInt(u32, frame[0..4], @intCast(frame.len - frame_size_len), .big);
        return frame;
    }
};

/// SASL `PLAIN` initial response: `authzid NUL authcid NUL passwd`.
pub fn saslPlain(buf: []u8, username: []const u8, password: []const u8) error{NoSpaceLeft}![]const u8 {
    const len = 2 + username.len + password.len;
    if (buf.len < len) return error.NoSpaceLeft;
    buf[0] = 0;
    @memcpy(buf[1..][0..username.len], username);
    buf[1 + username.len] = 0;
    @memcpy(buf[2 + username.len ..][0..password.len], password);
    return buf[0..len];
}

/// Parses `major.minor.patch` from a broker `version` property such as
/// `4.3.6` or `3.13.0-rc.1`. Missing components default to 0.
pub fn parseVersion(text: []const u8) std.SemanticVersion {
    var v: std.SemanticVersion = .{ .major = 0, .minor = 0, .patch = 0 };
    var it = std.mem.tokenizeAny(u8, text, ".-+");
    inline for (.{ "major", "minor", "patch" }) |field| {
        const part = it.next() orelse return v;
        @field(v, field) = std.fmt.parseInt(usize, part, 10) catch return v;
    }
    return v;
}

const testing = std.testing;

test "encode and decode primitives" {
    var b: FrameBuilder = .init(testing.allocator);
    defer b.deinit();
    var e = try b.begin(Command.create_stream.key(), 1);
    try e.int(u32, 7);
    try e.string("my-stream");
    try e.string(null);
    try e.bytes("abc");
    try e.properties(&.{.{ .key = "max-age", .value = "1h" }});
    try OffsetSpec.encode(.{ .timestamp = -5 }, &e);
    const frame = b.finish();

    var d: Decoder = .init(frame);
    try testing.expectEqual(frame.len - 4, try d.int(u32));
    const h: FrameHeader = try .decode(&d);
    try testing.expectEqual(Command.create_stream, h.command());
    try testing.expect(!h.isResponse());
    try testing.expectEqual(@as(u32, 7), try d.int(u32));
    try testing.expectEqualStrings("my-stream", (try d.string()).?);
    try testing.expectEqual(@as(?[]const u8, null), try d.string());
    try testing.expectEqualStrings("abc", (try d.bytes()).?);
    try testing.expectEqual(@as(u32, 1), try d.int(u32));
    try testing.expectEqualStrings("max-age", (try d.string()).?);
    try testing.expectEqualStrings("1h", (try d.string()).?);
    try testing.expectEqual(OffsetSpec{ .timestamp = -5 }, (try OffsetSpec.decode(&d)).?);
    try testing.expectEqual(@as(usize, 0), d.remaining());
    try testing.expectError(error.MalformedFrame, d.int(u8));
}

test "offset spec without value" {
    var buf: [16]u8 = undefined;
    var w: Io.Writer = .fixed(&buf);
    var e: Encoder = .{ .w = &w };
    try OffsetSpec.encode(.first, &e);
    try testing.expectEqualSlices(u8, &.{ 0, 1 }, w.buffered());
}

test "response codes map to errors" {
    try ResponseCode.ok.check();
    try testing.expectError(error.StreamDoesNotExist, ResponseCode.fromInt(2).check());
    try testing.expectError(error.UnknownResponseCode, ResponseCode.fromInt(0x99).check());
    var buf: [32]u8 = undefined;
    try testing.expectEqualStrings("unknown(0x0099)", try std.fmt.bufPrint(&buf, "{f}", .{ResponseCode.fromInt(0x99)}));
    try testing.expectEqualStrings("no_offset", try std.fmt.bufPrint(&buf, "{f}", .{ResponseCode.no_offset}));
}

test "sasl plain and version parsing" {
    var buf: [64]u8 = undefined;
    try testing.expectEqualSlices(u8, "\x00guest\x00secret", try saslPlain(&buf, "guest", "secret"));
    const v = parseVersion("3.13.0-rc.1");
    try testing.expectEqual(@as(usize, 3), v.major);
    try testing.expectEqual(@as(usize, 13), v.minor);
    try testing.expect(parseVersion("4.3.6").order(.{ .major = 3, .minor = 13, .patch = 0 }) == .gt);
}
