//! A compact AMQP 1.0 message codec (OASIS AMQP 1.0, part 3.2).
//!
//! Stream clients store messages in the AMQP 1.0 format so that they can be
//! read by any protocol RabbitMQ supports. Encoding works from plain Zig
//! values; decoding returns views into the encoded bytes and never allocates.

const std = @import("std");
const Io = std.Io;
const assert = std.debug.assert;

pub const DecodeError = error{InvalidAmqpData};

/// Format codes from the AMQP 1.0 type system.
pub const Code = struct {
    pub const described: u8 = 0x00;
    pub const @"null": u8 = 0x40;
    pub const @"true": u8 = 0x41;
    pub const @"false": u8 = 0x42;
    pub const boolean: u8 = 0x56;
    pub const ubyte: u8 = 0x50;
    pub const ushort: u8 = 0x60;
    pub const uint: u8 = 0x70;
    pub const smalluint: u8 = 0x52;
    pub const uint0: u8 = 0x43;
    pub const ulong: u8 = 0x80;
    pub const smallulong: u8 = 0x53;
    pub const ulong0: u8 = 0x44;
    pub const byte: u8 = 0x51;
    pub const short: u8 = 0x61;
    pub const int: u8 = 0x71;
    pub const smallint: u8 = 0x54;
    pub const long: u8 = 0x81;
    pub const smalllong: u8 = 0x55;
    pub const float: u8 = 0x72;
    pub const double: u8 = 0x82;
    pub const decimal32: u8 = 0x74;
    pub const decimal64: u8 = 0x84;
    pub const decimal128: u8 = 0x94;
    pub const char: u8 = 0x73;
    pub const timestamp: u8 = 0x83;
    pub const uuid: u8 = 0x98;
    pub const vbin8: u8 = 0xa0;
    pub const vbin32: u8 = 0xb0;
    pub const str8: u8 = 0xa1;
    pub const str32: u8 = 0xb1;
    pub const sym8: u8 = 0xa3;
    pub const sym32: u8 = 0xb3;
    pub const list0: u8 = 0x45;
    pub const list8: u8 = 0xc0;
    pub const list32: u8 = 0xd0;
    pub const map8: u8 = 0xc1;
    pub const map32: u8 = 0xd1;
    pub const array8: u8 = 0xe0;
    pub const array32: u8 = 0xf0;
};

/// Descriptor codes of the message sections.
pub const Section = enum(u64) {
    header = 0x70,
    delivery_annotations = 0x71,
    message_annotations = 0x72,
    properties = 0x73,
    application_properties = 0x74,
    data = 0x75,
    amqp_sequence = 0x76,
    amqp_value = 0x77,
    footer = 0x78,
    _,
};

/// An AMQP 1.0 value. Composite values (`list`, `map`, `array`, `described`)
/// keep their encoded bytes and are traversed with iterators.
pub const Value = union(enum) {
    null,
    boolean: bool,
    ubyte: u8,
    ushort: u16,
    uint: u32,
    ulong: u64,
    byte: i8,
    short: i16,
    int: i32,
    long: i64,
    float: f32,
    double: f64,
    char: u21,
    /// Milliseconds since the Unix epoch.
    timestamp: i64,
    uuid: [16]u8,
    binary: []const u8,
    string: []const u8,
    symbol: []const u8,
    decimal32: [4]u8,
    decimal64: [8]u8,
    decimal128: [16]u8,
    list: List,
    map: Map,
    array: Array,
    described: Described,

    /// Returns the string or symbol contents, if the value is one.
    pub fn asString(v: Value) ?[]const u8 {
        return switch (v) {
            .string, .symbol => |s| s,
            else => null,
        };
    }

    /// Returns the value as an `i64` if it is any integer type that fits.
    pub fn asInt(v: Value) ?i64 {
        return switch (v) {
            .ubyte => |x| x,
            .ushort => |x| x,
            .uint => |x| x,
            .ulong => |x| std.math.cast(i64, x),
            .byte => |x| x,
            .short => |x| x,
            .int => |x| x,
            .long => |x| x,
            else => null,
        };
    }

    pub fn eql(a: Value, b: Value) bool {
        if (std.meta.activeTag(a) != std.meta.activeTag(b)) return false;
        return switch (a) {
            inline .binary, .string, .symbol => |s, tag| std.mem.eql(u8, s, @field(b, @tagName(tag))),
            inline .list, .map, .array, .described => |x, tag| std.mem.eql(u8, x.data, @field(b, @tagName(tag)).data),
            inline else => |x, tag| std.meta.eql(x, @field(b, @tagName(tag))),
        };
    }

    pub fn format(v: Value, w: *Io.Writer) Io.Writer.Error!void {
        switch (v) {
            .null => try w.writeAll("null"),
            .string => |s| try w.print("\"{s}\"", .{s}),
            .symbol => |s| try w.print(":{s}", .{s}),
            .binary => |b| try w.print("0x{x}", .{b}),
            .uuid => |u| try w.print("{x}", .{&u}),
            .list => |l| try w.print("list({d})", .{l.count}),
            .map => |m| try w.print("map({d})", .{m.count / 2}),
            .array => |a| try w.print("array({d})", .{a.count}),
            .described => try w.writeAll("described"),
            .decimal32, .decimal64, .decimal128 => try w.writeAll("decimal"),
            inline else => |x| try w.print("{any}", .{x}),
        }
    }

    // Encoding

    pub fn encodedLen(v: Value) usize {
        return switch (v) {
            .null => 1,
            .boolean => 1,
            .ubyte, .byte => 2,
            .ushort, .short => 3,
            .uint => |x| if (x == 0) 1 else if (x <= 0xff) 2 else 5,
            .ulong => |x| if (x == 0) 1 else if (x <= 0xff) 2 else 9,
            .int => |x| if (x >= -128 and x <= 127) 2 else 5,
            .long => |x| if (x >= -128 and x <= 127) 2 else 9,
            .float, .char, .decimal32 => 5,
            .double, .timestamp, .decimal64 => 9,
            .uuid, .decimal128 => 17,
            .binary, .string, .symbol => |s| variableLen(s.len),
            .list => |l| compoundLen(l.data.len, l.count),
            .map => |m| compoundLen(m.data.len, m.count),
            .array => |a| 1 + 8 + a.data.len,
            .described => |d| 1 + d.data.len,
        };
    }

    pub fn encode(v: Value, w: *Io.Writer) Io.Writer.Error!void {
        switch (v) {
            .null => try w.writeByte(Code.null),
            .boolean => |b| try w.writeByte(if (b) Code.true else Code.false),
            .ubyte => |x| try w.writeAll(&.{ Code.ubyte, x }),
            .byte => |x| try w.writeAll(&.{ Code.byte, @bitCast(x) }),
            .ushort => |x| {
                try w.writeByte(Code.ushort);
                try w.writeInt(u16, x, .big);
            },
            .short => |x| {
                try w.writeByte(Code.short);
                try w.writeInt(i16, x, .big);
            },
            .uint => |x| if (x == 0) {
                try w.writeByte(Code.uint0);
            } else if (x <= 0xff) {
                try w.writeAll(&.{ Code.smalluint, @intCast(x) });
            } else {
                try w.writeByte(Code.uint);
                try w.writeInt(u32, x, .big);
            },
            .ulong => |x| if (x == 0) {
                try w.writeByte(Code.ulong0);
            } else if (x <= 0xff) {
                try w.writeAll(&.{ Code.smallulong, @intCast(x) });
            } else {
                try w.writeByte(Code.ulong);
                try w.writeInt(u64, x, .big);
            },
            .int => |x| if (x >= -128 and x <= 127) {
                try w.writeAll(&.{ Code.smallint, @bitCast(@as(i8, @intCast(x))) });
            } else {
                try w.writeByte(Code.int);
                try w.writeInt(i32, x, .big);
            },
            .long => |x| if (x >= -128 and x <= 127) {
                try w.writeAll(&.{ Code.smalllong, @bitCast(@as(i8, @intCast(x))) });
            } else {
                try w.writeByte(Code.long);
                try w.writeInt(i64, x, .big);
            },
            .float => |x| {
                try w.writeByte(Code.float);
                try w.writeInt(u32, @bitCast(x), .big);
            },
            .double => |x| {
                try w.writeByte(Code.double);
                try w.writeInt(u64, @bitCast(x), .big);
            },
            .char => |c| {
                try w.writeByte(Code.char);
                try w.writeInt(u32, c, .big);
            },
            .timestamp => |t| {
                try w.writeByte(Code.timestamp);
                try w.writeInt(i64, t, .big);
            },
            .uuid => |u| {
                try w.writeByte(Code.uuid);
                try w.writeAll(&u);
            },
            .decimal32 => |d| {
                try w.writeByte(Code.decimal32);
                try w.writeAll(&d);
            },
            .decimal64 => |d| {
                try w.writeByte(Code.decimal64);
                try w.writeAll(&d);
            },
            .decimal128 => |d| {
                try w.writeByte(Code.decimal128);
                try w.writeAll(&d);
            },
            .binary => |b| try encodeVariable(w, Code.vbin8, Code.vbin32, b),
            .string => |s| try encodeVariable(w, Code.str8, Code.str32, s),
            .symbol => |s| try encodeVariable(w, Code.sym8, Code.sym32, s),
            .list => |l| try encodeCompound(w, Code.list0, Code.list8, Code.list32, l.data, l.count),
            .map => |m| try encodeCompound(w, null, Code.map8, Code.map32, m.data, m.count),
            .array => |a| {
                try w.writeByte(Code.array32);
                try w.writeInt(u32, @intCast(4 + a.data.len), .big);
                try w.writeInt(u32, a.count, .big);
                try w.writeAll(a.data);
            },
            .described => |d| {
                try w.writeByte(Code.described);
                try w.writeAll(d.data);
            },
        }
    }
};

fn variableLen(len: usize) usize {
    return if (len <= 0xff) 2 + len else 5 + len;
}

fn encodeVariable(w: *Io.Writer, code8: u8, code32: u8, s: []const u8) Io.Writer.Error!void {
    if (s.len <= 0xff) {
        try w.writeAll(&.{ code8, @intCast(s.len) });
    } else {
        try w.writeByte(code32);
        try w.writeInt(u32, @intCast(s.len), .big);
    }
    try w.writeAll(s);
}

fn compoundLen(data_len: usize, count: u32) usize {
    if (count == 0) return if (data_len == 0) 1 else 3;
    if (data_len + 1 <= 0xff and count <= 0xff) return 3 + data_len;
    return 9 + data_len;
}

fn encodeCompound(w: *Io.Writer, code0: ?u8, code8: u8, code32: u8, data: []const u8, count: u32) Io.Writer.Error!void {
    if (count == 0 and data.len == 0) {
        if (code0) |c| return w.writeByte(c);
    }
    if (data.len + 1 <= 0xff and count <= 0xff) {
        try w.writeAll(&.{ code8, @intCast(data.len + 1), @intCast(count) });
    } else {
        try w.writeByte(code32);
        try w.writeInt(u32, @intCast(data.len + 4), .big);
        try w.writeInt(u32, count, .big);
    }
    try w.writeAll(data);
}

/// An encoded list: `count` consecutive encoded values.
pub const List = struct {
    count: u32,
    data: []const u8,

    pub fn iterator(l: List) Iterator {
        return .{ .remaining = l.count, .d = .{ .data = l.data } };
    }
};

/// An encoded map: `count` is the number of keys plus values.
pub const Map = struct {
    count: u32,
    data: []const u8,

    pub const Entry = struct { key: Value, value: Value };

    pub fn len(m: Map) u32 {
        return m.count / 2;
    }

    pub fn iterator(m: Map) EntryIterator {
        return .{ .it = .{ .remaining = m.count, .d = .{ .data = m.data } } };
    }

    /// Looks up a string or symbol key.
    pub fn get(m: Map, key: []const u8) ?Value {
        var it = m.iterator();
        while (it.next() catch return null) |e| {
            const k = e.key.asString() orelse continue;
            if (std.mem.eql(u8, k, key)) return e.value;
        }
        return null;
    }

    pub const EntryIterator = struct {
        it: Iterator,

        pub fn next(i: *EntryIterator) DecodeError!?Entry {
            const key = try i.it.next() orelse return null;
            const value = try i.it.next() orelse return error.InvalidAmqpData;
            return .{ .key = key, .value = value };
        }
    };
};

/// An encoded array: one constructor shared by `count` elements.
pub const Array = struct {
    count: u32,
    /// The constructor followed by the element bodies.
    data: []const u8,

    pub fn iterator(a: Array) DecodeError!ArrayIterator {
        var d: Decoder = .{ .data = a.data };
        const ctor = try d.constructor();
        return .{ .remaining = a.count, .ctor = ctor, .d = d };
    }

    pub const ArrayIterator = struct {
        remaining: u32,
        ctor: Constructor,
        d: Decoder,

        pub fn next(i: *ArrayIterator) DecodeError!?Value {
            if (i.remaining == 0) return null;
            i.remaining -= 1;
            return try i.d.body(i.ctor);
        }
    };
};

/// A described value, encoded as descriptor followed by value.
pub const Described = struct {
    data: []const u8,

    pub fn descriptor(x: Described) DecodeError!Value {
        var d: Decoder = .{ .data = x.data };
        return d.value();
    }

    pub fn value(x: Described) DecodeError!Value {
        var d: Decoder = .{ .data = x.data };
        _ = try d.value();
        return d.value();
    }
};

pub const Iterator = struct {
    remaining: u32,
    d: Decoder,

    pub fn next(i: *Iterator) DecodeError!?Value {
        if (i.remaining == 0) return null;
        i.remaining -= 1;
        return try i.d.value();
    }
};

const Constructor = struct {
    code: u8,
    /// Encoded descriptor when the constructor is described.
    descriptor: ?[]const u8 = null,
};

/// Decodes AMQP values from a byte slice.
pub const Decoder = struct {
    data: []const u8,
    pos: usize = 0,

    fn take(d: *Decoder, n: usize) DecodeError![]const u8 {
        if (d.data.len - d.pos < n) return error.InvalidAmqpData;
        const s = d.data[d.pos..][0..n];
        d.pos += n;
        return s;
    }

    fn int(d: *Decoder, comptime T: type) DecodeError!T {
        const n = @divExact(@typeInfo(T).int.bits, 8);
        return std.mem.readInt(T, (try d.take(n))[0..n], .big);
    }

    pub fn atEnd(d: *const Decoder) bool {
        return d.pos >= d.data.len;
    }

    fn constructor(d: *Decoder) DecodeError!Constructor {
        const code = (try d.take(1))[0];
        if (code != Code.described) return .{ .code = code };
        const start = d.pos;
        _ = try d.value();
        const desc = d.data[start..d.pos];
        const inner = try d.constructor();
        if (inner.descriptor != null) return error.InvalidAmqpData;
        return .{ .code = inner.code, .descriptor = desc };
    }

    pub fn value(d: *Decoder) DecodeError!Value {
        const start = d.pos;
        const ctor = try d.constructor();
        if (ctor.descriptor != null) {
            _ = try d.body(ctor);
            return .{ .described = .{ .data = d.data[start + 1 .. d.pos] } };
        }
        return d.body(ctor);
    }

    fn body(d: *Decoder, ctor: Constructor) DecodeError!Value {
        return switch (ctor.code) {
            Code.null => .null,
            Code.true => .{ .boolean = true },
            Code.false => .{ .boolean = false },
            Code.boolean => .{ .boolean = (try d.take(1))[0] != 0 },
            Code.ubyte => .{ .ubyte = (try d.take(1))[0] },
            Code.ushort => .{ .ushort = try d.int(u16) },
            Code.uint => .{ .uint = try d.int(u32) },
            Code.smalluint => .{ .uint = (try d.take(1))[0] },
            Code.uint0 => .{ .uint = 0 },
            Code.ulong => .{ .ulong = try d.int(u64) },
            Code.smallulong => .{ .ulong = (try d.take(1))[0] },
            Code.ulong0 => .{ .ulong = 0 },
            Code.byte => .{ .byte = try d.int(i8) },
            Code.short => .{ .short = try d.int(i16) },
            Code.int => .{ .int = try d.int(i32) },
            Code.smallint => .{ .int = try d.int(i8) },
            Code.long => .{ .long = try d.int(i64) },
            Code.smalllong => .{ .long = try d.int(i8) },
            Code.float => .{ .float = @bitCast(try d.int(u32)) },
            Code.double => .{ .double = @bitCast(try d.int(u64)) },
            Code.decimal32 => .{ .decimal32 = (try d.take(4))[0..4].* },
            Code.decimal64 => .{ .decimal64 = (try d.take(8))[0..8].* },
            Code.decimal128 => .{ .decimal128 = (try d.take(16))[0..16].* },
            Code.char => .{ .char = std.math.cast(u21, try d.int(u32)) orelse return error.InvalidAmqpData },
            Code.timestamp => .{ .timestamp = try d.int(i64) },
            Code.uuid => .{ .uuid = (try d.take(16))[0..16].* },
            Code.vbin8 => .{ .binary = try d.take((try d.take(1))[0]) },
            Code.vbin32 => .{ .binary = try d.take(try d.int(u32)) },
            Code.str8 => .{ .string = try d.take((try d.take(1))[0]) },
            Code.str32 => .{ .string = try d.take(try d.int(u32)) },
            Code.sym8 => .{ .symbol = try d.take((try d.take(1))[0]) },
            Code.sym32 => .{ .symbol = try d.take(try d.int(u32)) },
            Code.list0 => .{ .list = .{ .count = 0, .data = &.{} } },
            Code.list8, Code.map8, Code.array8 => blk: {
                const size = (try d.take(1))[0];
                if (size < 1) return error.InvalidAmqpData;
                const payload = try d.take(size);
                const count: u32 = payload[0];
                break :blk compound(ctor.code, count, payload[1..]);
            },
            Code.list32, Code.map32, Code.array32 => blk: {
                const size = try d.int(u32);
                if (size < 4) return error.InvalidAmqpData;
                const payload = try d.take(size);
                const count = std.mem.readInt(u32, payload[0..4], .big);
                break :blk compound(ctor.code, count, payload[4..]);
            },
            else => error.InvalidAmqpData,
        };
    }

    fn compound(code: u8, count: u32, data: []const u8) Value {
        return switch (code) {
            Code.list8, Code.list32 => .{ .list = .{ .count = count, .data = data } },
            Code.map8, Code.map32 => .{ .map = .{ .count = count, .data = data } },
            else => .{ .array = .{ .count = count, .data = data } },
        };
    }
};

/// Writes a list or map whose elements are given as a tuple of `Value`s.
/// Used to encode the fixed-shape message sections.
fn encodeValueList(w: *Io.Writer, code_list: bool, values: []const Value) Io.Writer.Error!void {
    var data_len: usize = 0;
    for (values) |v| data_len += v.encodedLen();
    const count: u32 = @intCast(values.len);
    if (data_len + 1 <= 0xff and count <= 0xff) {
        try w.writeAll(&.{ if (code_list) Code.list8 else Code.map8, @intCast(data_len + 1), @intCast(count) });
    } else {
        try w.writeByte(if (code_list) Code.list32 else Code.map32);
        try w.writeInt(u32, @intCast(data_len + 4), .big);
        try w.writeInt(u32, count, .big);
    }
    for (values) |v| try v.encode(w);
}

fn valueListLen(values: []const Value) usize {
    var data_len: usize = 0;
    for (values) |v| data_len += v.encodedLen();
    if (data_len + 1 <= 0xff and values.len <= 0xff) return 3 + data_len;
    return 9 + data_len;
}

/// The `header` section.
pub const Header = struct {
    durable: bool = false,
    priority: u8 = 4,
    /// Time to live in milliseconds.
    ttl: ?u32 = null,
    first_acquirer: bool = false,
    delivery_count: u32 = 0,

    fn values(h: Header) [5]Value {
        return .{
            .{ .boolean = h.durable },
            .{ .ubyte = h.priority },
            if (h.ttl) |t| .{ .uint = t } else .null,
            .{ .boolean = h.first_acquirer },
            .{ .uint = h.delivery_count },
        };
    }

    fn decode(l: List) DecodeError!Header {
        var h: Header = .{};
        var it = l.iterator();
        var i: usize = 0;
        while (try it.next()) |v| : (i += 1) {
            if (v == .null) continue;
            switch (i) {
                0 => h.durable = if (v == .boolean) v.boolean else return error.InvalidAmqpData,
                1 => h.priority = if (v == .ubyte) v.ubyte else return error.InvalidAmqpData,
                2 => h.ttl = if (v == .uint) v.uint else return error.InvalidAmqpData,
                3 => h.first_acquirer = if (v == .boolean) v.boolean else return error.InvalidAmqpData,
                4 => h.delivery_count = if (v == .uint) v.uint else return error.InvalidAmqpData,
                else => {},
            }
        }
        return h;
    }
};

/// Values allowed for `message-id` and `correlation-id`.
pub const MessageId = union(enum) {
    ulong: u64,
    uuid: [16]u8,
    binary: []const u8,
    string: []const u8,

    fn toValue(id: MessageId) Value {
        return switch (id) {
            .ulong => |x| .{ .ulong = x },
            .uuid => |x| .{ .uuid = x },
            .binary => |x| .{ .binary = x },
            .string => |x| .{ .string = x },
        };
    }

    fn fromValue(v: Value) DecodeError!MessageId {
        return switch (v) {
            .ulong => |x| .{ .ulong = x },
            .uuid => |x| .{ .uuid = x },
            .binary => |x| .{ .binary = x },
            .string => |x| .{ .string = x },
            else => error.InvalidAmqpData,
        };
    }
};

/// The `properties` section.
pub const Properties = struct {
    message_id: ?MessageId = null,
    user_id: ?[]const u8 = null,
    to: ?[]const u8 = null,
    subject: ?[]const u8 = null,
    reply_to: ?[]const u8 = null,
    correlation_id: ?MessageId = null,
    content_type: ?[]const u8 = null,
    content_encoding: ?[]const u8 = null,
    /// Milliseconds since the Unix epoch.
    absolute_expiry_time: ?i64 = null,
    /// Milliseconds since the Unix epoch.
    creation_time: ?i64 = null,
    group_id: ?[]const u8 = null,
    group_sequence: ?u32 = null,
    reply_to_group_id: ?[]const u8 = null,

    const field_count = 13;

    fn values(p: Properties, out: *[field_count]Value) []const Value {
        out.* = .{
            if (p.message_id) |id| id.toValue() else .null,
            if (p.user_id) |x| .{ .binary = x } else .null,
            if (p.to) |x| .{ .string = x } else .null,
            if (p.subject) |x| .{ .string = x } else .null,
            if (p.reply_to) |x| .{ .string = x } else .null,
            if (p.correlation_id) |id| id.toValue() else .null,
            if (p.content_type) |x| .{ .symbol = x } else .null,
            if (p.content_encoding) |x| .{ .symbol = x } else .null,
            if (p.absolute_expiry_time) |x| .{ .timestamp = x } else .null,
            if (p.creation_time) |x| .{ .timestamp = x } else .null,
            if (p.group_id) |x| .{ .string = x } else .null,
            if (p.group_sequence) |x| .{ .uint = x } else .null,
            if (p.reply_to_group_id) |x| .{ .string = x } else .null,
        };
        // Trailing nulls may be omitted from a list.
        var n: usize = field_count;
        while (n > 0 and out[n - 1] == .null) n -= 1;
        return out[0..n];
    }

    fn decode(l: List) DecodeError!Properties {
        var p: Properties = .{};
        var it = l.iterator();
        var i: usize = 0;
        while (try it.next()) |v| : (i += 1) {
            if (v == .null) continue;
            switch (i) {
                0 => p.message_id = try .fromValue(v),
                1 => p.user_id = if (v == .binary) v.binary else return error.InvalidAmqpData,
                2 => p.to = v.asString() orelse return error.InvalidAmqpData,
                3 => p.subject = v.asString() orelse return error.InvalidAmqpData,
                4 => p.reply_to = v.asString() orelse return error.InvalidAmqpData,
                5 => p.correlation_id = try .fromValue(v),
                6 => p.content_type = v.asString() orelse return error.InvalidAmqpData,
                7 => p.content_encoding = v.asString() orelse return error.InvalidAmqpData,
                8 => p.absolute_expiry_time = if (v == .timestamp) v.timestamp else return error.InvalidAmqpData,
                9 => p.creation_time = if (v == .timestamp) v.timestamp else return error.InvalidAmqpData,
                10 => p.group_id = v.asString() orelse return error.InvalidAmqpData,
                11 => p.group_sequence = if (v == .uint) v.uint else return error.InvalidAmqpData,
                12 => p.reply_to_group_id = v.asString() orelse return error.InvalidAmqpData,
                else => {},
            }
        }
        return p;
    }
};

/// A key/value pair of an annotation map. Keys are encoded as symbols.
pub const Annotation = struct {
    key: []const u8,
    value: Value,
};

/// A key/value pair of the application properties. Keys are strings.
pub const ApplicationProperty = struct {
    key: []const u8,
    value: Value,
};

/// A message to encode.
pub const Message = struct {
    header: ?Header = null,
    delivery_annotations: []const Annotation = &.{},
    message_annotations: []const Annotation = &.{},
    properties: ?Properties = null,
    application_properties: []const ApplicationProperty = &.{},
    body: Body = .{ .data = "" },
    footer: []const Annotation = &.{},

    pub const Body = union(enum) {
        /// A single `data` section with opaque bytes.
        data: []const u8,
        /// An `amqp-value` section.
        value: Value,
        /// An `amqp-sequence` section.
        sequence: []const Value,
    };

    /// Encodes the message to `w`.
    pub fn encode(m: *const Message, w: *Io.Writer) Io.Writer.Error!void {
        if (m.header) |h| {
            try writeDescriptor(w, .header);
            const v = h.values();
            try encodeValueList(w, true, &v);
        }
        if (m.delivery_annotations.len > 0) {
            try writeDescriptor(w, .delivery_annotations);
            try encodePairs(w, Annotation, m.delivery_annotations, .symbol);
        }
        if (m.message_annotations.len > 0) {
            try writeDescriptor(w, .message_annotations);
            try encodePairs(w, Annotation, m.message_annotations, .symbol);
        }
        if (m.properties) |p| {
            try writeDescriptor(w, .properties);
            var buf: [Properties.field_count]Value = undefined;
            try encodeValueList(w, true, p.values(&buf));
        }
        if (m.application_properties.len > 0) {
            try writeDescriptor(w, .application_properties);
            try encodePairs(w, ApplicationProperty, m.application_properties, .string);
        }
        switch (m.body) {
            .data => |b| {
                try writeDescriptor(w, .data);
                try (Value{ .binary = b }).encode(w);
            },
            .value => |v| {
                try writeDescriptor(w, .amqp_value);
                try v.encode(w);
            },
            .sequence => |s| {
                try writeDescriptor(w, .amqp_sequence);
                try encodeValueList(w, true, s);
            },
        }
        if (m.footer.len > 0) {
            try writeDescriptor(w, .footer);
            try encodePairs(w, Annotation, m.footer, .symbol);
        }
    }

    /// Returns the exact number of bytes `encode` writes.
    pub fn encodedLen(m: *const Message) usize {
        var n: usize = 0;
        if (m.header) |h| {
            const v = h.values();
            n += 3 + valueListLen(&v);
        }
        if (m.delivery_annotations.len > 0) n += 3 + pairsLen(Annotation, m.delivery_annotations, .symbol);
        if (m.message_annotations.len > 0) n += 3 + pairsLen(Annotation, m.message_annotations, .symbol);
        if (m.properties) |p| {
            var buf: [Properties.field_count]Value = undefined;
            n += 3 + valueListLen(p.values(&buf));
        }
        if (m.application_properties.len > 0) n += 3 + pairsLen(ApplicationProperty, m.application_properties, .string);
        n += 3 + switch (m.body) {
            .data => |b| (Value{ .binary = b }).encodedLen(),
            .value => |v| v.encodedLen(),
            .sequence => |s| valueListLen(s),
        };
        if (m.footer.len > 0) n += 3 + pairsLen(Annotation, m.footer, .symbol);
        return n;
    }

    /// Encodes the message into a newly allocated slice owned by the caller.
    pub fn encodeAlloc(m: *const Message, gpa: std.mem.Allocator) error{OutOfMemory}![]u8 {
        const buf = try gpa.alloc(u8, m.encodedLen());
        errdefer gpa.free(buf);
        var w: Io.Writer = .fixed(buf);
        m.encode(&w) catch unreachable;
        assert(w.end == buf.len);
        return buf;
    }
};

const KeyKind = enum { symbol, string };

fn keyValue(kind: KeyKind, key: []const u8) Value {
    return switch (kind) {
        .symbol => .{ .symbol = key },
        .string => .{ .string = key },
    };
}

fn pairsDataLen(comptime T: type, pairs: []const T, kind: KeyKind) usize {
    var n: usize = 0;
    for (pairs) |p| n += keyValue(kind, p.key).encodedLen() + p.value.encodedLen();
    return n;
}

fn pairsLen(comptime T: type, pairs: []const T, kind: KeyKind) usize {
    return compoundLen(pairsDataLen(T, pairs, kind), @intCast(pairs.len * 2));
}

fn encodePairs(w: *Io.Writer, comptime T: type, pairs: []const T, kind: KeyKind) Io.Writer.Error!void {
    const data_len = pairsDataLen(T, pairs, kind);
    const count: u32 = @intCast(pairs.len * 2);
    if (data_len + 1 <= 0xff and count <= 0xff) {
        try w.writeAll(&.{ Code.map8, @intCast(data_len + 1), @intCast(count) });
    } else {
        try w.writeByte(Code.map32);
        try w.writeInt(u32, @intCast(data_len + 4), .big);
        try w.writeInt(u32, count, .big);
    }
    for (pairs) |p| {
        try keyValue(kind, p.key).encode(w);
        try p.value.encode(w);
    }
}

fn writeDescriptor(w: *Io.Writer, section: Section) Io.Writer.Error!void {
    try w.writeAll(&.{ Code.described, Code.smallulong, @intCast(@backingInt(section)) });
}

/// A decoded message. All slices point into the bytes passed to `decode`.
pub const DecodedMessage = struct {
    header: ?Header = null,
    delivery_annotations: ?Map = null,
    message_annotations: ?Map = null,
    properties: ?Properties = null,
    application_properties: ?Map = null,
    body: Body = .none,
    footer: ?Map = null,

    pub const Body = union(enum) {
        none,
        /// One or more `data` sections. Use `dataSections` when there may be
        /// more than one; `first` holds the first section.
        data: struct { first: []const u8, count: u32, raw: []const u8 },
        value: Value,
        /// One or more `amqp-sequence` sections; `first` is the first one.
        sequence: List,
    };

    /// Returns the bytes of the body when it is a single `data` section,
    /// or the string or binary contents of an `amqp-value` body.
    pub fn data(m: DecodedMessage) ?[]const u8 {
        return switch (m.body) {
            .data => |d| if (d.count == 1) d.first else null,
            .value => |v| switch (v) {
                .binary, .string => |s| s,
                else => null,
            },
            else => null,
        };
    }

    /// Iterates over all `data` sections of the body.
    pub fn dataSections(m: DecodedMessage) DataIterator {
        return switch (m.body) {
            .data => |d| .{ .d = .{ .data = d.raw } },
            else => .{ .d = .{ .data = &.{} } },
        };
    }

    pub const DataIterator = struct {
        d: Decoder,

        pub fn next(it: *DataIterator) DecodeError!?[]const u8 {
            if (it.d.atEnd()) return null;
            const section = try readSection(&it.d);
            if (section.code != .data or section.value != .binary) return error.InvalidAmqpData;
            return section.value.binary;
        }
    };
};

const RawSection = struct { code: Section, value: Value };

fn readSection(d: *Decoder) DecodeError!RawSection {
    const first = (try d.take(1))[0];
    if (first != Code.described) return error.InvalidAmqpData;
    const desc = try d.value();
    const code: u64 = switch (desc) {
        .ulong => |x| x,
        .symbol => |s| symbolSection(s) orelse return error.InvalidAmqpData,
        else => return error.InvalidAmqpData,
    };
    return .{ .code = @fromBackingInt(code), .value = try d.value() };
}

fn symbolSection(s: []const u8) ?u64 {
    const names = .{
        .{ "amqp:header:list", Section.header },
        .{ "amqp:delivery-annotations:map", Section.delivery_annotations },
        .{ "amqp:message-annotations:map", Section.message_annotations },
        .{ "amqp:properties:list", Section.properties },
        .{ "amqp:application-properties:map", Section.application_properties },
        .{ "amqp:data:binary", Section.data },
        .{ "amqp:amqp-sequence:list", Section.amqp_sequence },
        .{ "amqp:amqp-value:*", Section.amqp_value },
        .{ "amqp:footer:map", Section.footer },
    };
    inline for (names) |n| {
        if (std.mem.eql(u8, s, n[0])) return @backingInt(n[1]);
    }
    return null;
}

/// Decodes an AMQP 1.0 message.
pub fn decode(bytes: []const u8) DecodeError!DecodedMessage {
    var m: DecodedMessage = .{};
    var d: Decoder = .{ .data = bytes };
    while (!d.atEnd()) {
        const start = d.pos;
        const s = try readSection(&d);
        switch (s.code) {
            .header => m.header = try Header.decode(try expectList(s.value)),
            .delivery_annotations => m.delivery_annotations = try expectMap(s.value),
            .message_annotations => m.message_annotations = try expectMap(s.value),
            .properties => m.properties = try Properties.decode(try expectList(s.value)),
            .application_properties => m.application_properties = try expectMap(s.value),
            .data => {
                if (s.value != .binary) return error.InvalidAmqpData;
                switch (m.body) {
                    .none => m.body = .{ .data = .{ .first = s.value.binary, .count = 1, .raw = bytes[start..d.pos] } },
                    .data => |*b| {
                        b.count += 1;
                        b.raw = bytes[b.raw.ptr - bytes.ptr .. d.pos];
                    },
                    else => return error.InvalidAmqpData,
                }
            },
            .amqp_sequence => switch (m.body) {
                .none => m.body = .{ .sequence = try expectList(s.value) },
                .sequence => {},
                else => return error.InvalidAmqpData,
            },
            .amqp_value => {
                if (m.body != .none) return error.InvalidAmqpData;
                m.body = .{ .value = s.value };
            },
            .footer => m.footer = try expectMap(s.value),
            _ => return error.InvalidAmqpData,
        }
    }
    return m;
}

fn expectList(v: Value) DecodeError!List {
    return if (v == .list) v.list else error.InvalidAmqpData;
}

fn expectMap(v: Value) DecodeError!Map {
    return if (v == .map) v.map else error.InvalidAmqpData;
}

const testing = std.testing;

test "round trip of a full message" {
    const uuid: [16]u8 = @splat(0xab);
    const msg: Message = .{
        .header = .{ .durable = true, .priority = 9, .ttl = 1000 },
        .message_annotations = &.{
            .{ .key = "x-stream-filter-value", .value = .{ .string = "emea" } },
        },
        .properties = .{
            .message_id = .{ .ulong = 42 },
            .correlation_id = .{ .uuid = uuid },
            .content_type = "text/plain",
            .subject = "greeting",
            .creation_time = 1_700_000_000_000,
            .group_sequence = 3,
        },
        .application_properties = &.{
            .{ .key = "region", .value = .{ .string = "emea" } },
            .{ .key = "count", .value = .{ .int = -70000 } },
            .{ .key = "flag", .value = .{ .boolean = true } },
            .{ .key = "ratio", .value = .{ .double = 0.25 } },
        },
        .body = .{ .data = "hello world" },
    };
    const bytes = try msg.encodeAlloc(testing.allocator);
    defer testing.allocator.free(bytes);

    const dm = try decode(bytes);
    try testing.expectEqual(true, dm.header.?.durable);
    try testing.expectEqual(@as(u8, 9), dm.header.?.priority);
    try testing.expectEqual(@as(?u32, 1000), dm.header.?.ttl);
    const props = dm.properties.?;
    try testing.expectEqual(@as(u64, 42), props.message_id.?.ulong);
    try testing.expectEqualSlices(u8, &uuid, &props.correlation_id.?.uuid);
    try testing.expectEqualStrings("text/plain", props.content_type.?);
    try testing.expectEqualStrings("greeting", props.subject.?);
    try testing.expectEqual(@as(?i64, 1_700_000_000_000), props.creation_time);
    try testing.expectEqual(@as(?u32, 3), props.group_sequence);
    try testing.expectEqual(@as(?[]const u8, null), props.to);

    const app = dm.application_properties.?;
    try testing.expectEqual(@as(u32, 4), app.len());
    try testing.expectEqualStrings("emea", app.get("region").?.string);
    try testing.expectEqual(@as(i64, -70000), app.get("count").?.asInt().?);
    try testing.expectEqual(true, app.get("flag").?.boolean);
    try testing.expectEqual(@as(f64, 0.25), app.get("ratio").?.double);
    try testing.expectEqual(@as(?Value, null), app.get("missing"));
    try testing.expectEqualStrings("emea", dm.message_annotations.?.get("x-stream-filter-value").?.string);
    try testing.expectEqualStrings("hello world", dm.data().?);
}

test "large values use 32-bit encodings" {
    const big: [300]u8 = @splat('x');
    var props: [40]ApplicationProperty = undefined;
    for (&props, 0..) |*p, i| p.* = .{ .key = "k", .value = .{ .ulong = i * 1000 } };
    const msg: Message = .{ .application_properties = &props, .body = .{ .data = &big } };
    const bytes = try msg.encodeAlloc(testing.allocator);
    defer testing.allocator.free(bytes);
    const dm = try decode(bytes);
    try testing.expectEqual(@as(usize, 300), dm.data().?.len);
    var it = dm.application_properties.?.iterator();
    var i: u64 = 0;
    while (try it.next()) |e| : (i += 1) try testing.expectEqual(i * 1000, e.value.ulong);
    try testing.expectEqual(@as(u64, 40), i);
}

test "amqp-value and sequence bodies" {
    {
        const bytes = try (Message{ .body = .{ .value = .{ .string = "text" } } }).encodeAlloc(testing.allocator);
        defer testing.allocator.free(bytes);
        const dm = try decode(bytes);
        try testing.expectEqualStrings("text", dm.data().?);
    }
    {
        const bytes = try (Message{ .body = .{ .sequence = &.{ .{ .int = 1 }, .{ .symbol = "a" } } } }).encodeAlloc(testing.allocator);
        defer testing.allocator.free(bytes);
        const dm = try decode(bytes);
        var it = dm.body.sequence.iterator();
        try testing.expectEqual(@as(i32, 1), (try it.next()).?.int);
        try testing.expectEqualStrings("a", (try it.next()).?.symbol);
        try testing.expectEqual(@as(?Value, null), try it.next());
    }
}

test "multiple data sections" {
    var buf: [64]u8 = undefined;
    var w: Io.Writer = .fixed(&buf);
    try writeDescriptor(&w, .data);
    try (Value{ .binary = "ab" }).encode(&w);
    try writeDescriptor(&w, .data);
    try (Value{ .binary = "cd" }).encode(&w);
    const dm = try decode(w.buffered());
    try testing.expectEqual(@as(?[]const u8, null), dm.data());
    var it = dm.dataSections();
    try testing.expectEqualStrings("ab", (try it.next()).?);
    try testing.expectEqualStrings("cd", (try it.next()).?);
    try testing.expectEqual(@as(?[]const u8, null), try it.next());
}

test "decode rejects garbage" {
    try testing.expectError(error.InvalidAmqpData, decode("not amqp"));
    try testing.expectError(error.InvalidAmqpData, decode(&.{ 0x00, 0x53, 0x75, 0xa0, 0x05, 'a' }));
}

test "described and array values" {
    // An array of three ints followed by a described value.
    const bytes = [_]u8{ Code.array8, 14, 3, Code.int, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3 } ++
        [_]u8{ Code.described, Code.smallulong, 0x01, Code.str8, 1, 'x' };
    var d: Decoder = .{ .data = &bytes };
    var it = try (try d.value()).array.iterator();
    var sum: i32 = 0;
    while (try it.next()) |v| sum += v.int;
    try testing.expectEqual(@as(i32, 6), sum);
    const desc = (try d.value()).described;
    try testing.expectEqual(@as(u64, 1), (try desc.descriptor()).ulong);
    try testing.expectEqualStrings("x", (try desc.value()).string);
    try testing.expect(d.atEnd());
}
