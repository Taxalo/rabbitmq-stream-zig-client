//! Osiris chunks, the unit in which the broker stores and delivers messages.
//!
//! Format reference: the "Chunk format" comment in `src/osiris_log.erl` in
//! `rabbitmq/osiris`. The broker sends the 48-byte header followed by the
//! data entries; the bloom filter and the trailer are not sent to consumers.

const std = @import("std");
const Io = std.Io;
const flate = std.compress.flate;
const zstd = std.compress.zstd;

pub const Error = error{ MalformedChunk, UnsupportedCompression, DecompressionFailed, OutOfMemory };

pub const ChunkType = enum(u8) {
    user = 0,
    tracking_delta = 1,
    tracking_snapshot = 2,
    _,
};

/// Compression codecs of sub-entry batches.
pub const Compression = enum(u3) {
    none = 0,
    gzip = 1,
    snappy = 2,
    lz4 = 3,
    zstd = 4,
    _,
};

pub const Header = struct {
    magic_version: u8,
    chunk_type: ChunkType,
    num_entries: u16,
    num_records: u32,
    /// Milliseconds since the Unix epoch, set when the chunk was written.
    timestamp: i64,
    epoch: u64,
    /// The offset of the first record in the chunk.
    chunk_id: u64,
    crc: u32,
    data_length: u32,
    trailer_length: u32,
    bloom_size: u8,

    pub const size = 48;

    pub fn parse(bytes: []const u8) Error!Header {
        if (bytes.len < size) return error.MalformedChunk;
        const b = bytes[0..size];
        return .{
            .magic_version = b[0],
            .chunk_type = @fromBackingInt(b[1]),
            .num_entries = std.mem.readInt(u16, b[2..4], .big),
            .num_records = std.mem.readInt(u32, b[4..8], .big),
            .timestamp = std.mem.readInt(i64, b[8..16], .big),
            .epoch = std.mem.readInt(u64, b[16..24], .big),
            .chunk_id = std.mem.readInt(u64, b[24..32], .big),
            .crc = std.mem.readInt(u32, b[32..36], .big),
            .data_length = std.mem.readInt(u32, b[36..40], .big),
            .trailer_length = std.mem.readInt(u32, b[40..44], .big),
            .bloom_size = b[44],
        };
    }
};

/// A chunk as delivered to a consumer: header and data entries.
pub const Chunk = struct {
    header: Header,
    /// The data entries, `header.data_length` bytes.
    data: []const u8,

    pub fn parse(bytes: []const u8) Error!Chunk {
        const header: Header = try .parse(bytes);
        const rest = bytes[Header.size..];
        if (rest.len < header.data_length) return error.MalformedChunk;
        return .{ .header = header, .data = rest[0..header.data_length] };
    }

    /// Verifies the CRC32 of the data entries.
    pub fn verifyCrc(c: Chunk) bool {
        return std.hash.Crc32.hash(c.data) == c.header.crc;
    }

    pub fn entries(c: Chunk) EntryIterator {
        return .{ .data = c.data, .remaining = c.header.num_entries };
    }
};

pub const Entry = union(enum) {
    /// A single message.
    simple: []const u8,
    /// Several messages stored together, possibly compressed.
    sub_batch: SubBatch,
};

pub const SubBatch = struct {
    compression: Compression,
    record_count: u16,
    uncompressed_len: u32,
    /// The (possibly compressed) batch body.
    data: []const u8,

    /// Iterates over the records of an uncompressed batch body.
    pub fn records(body: []const u8, count: u16) RecordIterator {
        return .{ .data = body, .remaining = count };
    }
};

pub const EntryIterator = struct {
    data: []const u8,
    pos: usize = 0,
    remaining: u32,

    pub fn next(it: *EntryIterator) Error!?Entry {
        if (it.remaining == 0) return null;
        it.remaining -= 1;
        const d = it.data[it.pos..];
        if (d.len < 4) return error.MalformedChunk;
        if (d[0] & 0x80 == 0) {
            const len = std.mem.readInt(u32, d[0..4], .big);
            if (d.len - 4 < len) return error.MalformedChunk;
            it.pos += 4 + len;
            return .{ .simple = d[4..][0..len] };
        }
        if (d.len < 11) return error.MalformedChunk;
        const compression: Compression = @fromBackingInt(@as(u3, @truncate(d[0] >> 4)));
        const record_count = std.mem.readInt(u16, d[1..3], .big);
        const uncompressed_len = std.mem.readInt(u32, d[3..7], .big);
        const len = std.mem.readInt(u32, d[7..11], .big);
        if (d.len - 11 < len) return error.MalformedChunk;
        it.pos += 11 + len;
        return .{ .sub_batch = .{
            .compression = compression,
            .record_count = record_count,
            .uncompressed_len = uncompressed_len,
            .data = d[11..][0..len],
        } };
    }
};

/// Iterates over the size-prefixed records of an uncompressed sub-batch.
pub const RecordIterator = struct {
    data: []const u8,
    pos: usize = 0,
    remaining: u32,

    pub fn next(it: *RecordIterator) Error!?[]const u8 {
        if (it.remaining == 0) return null;
        it.remaining -= 1;
        const d = it.data[it.pos..];
        if (d.len < 4) return error.MalformedChunk;
        const len = std.mem.readInt(u32, d[0..4], .big);
        if (d.len - 4 < len) return error.MalformedChunk;
        it.pos += 4 + len;
        return d[4..][0..len];
    }
};

/// Decompresses a sub-batch body into `out`, which is cleared first.
/// Returns the uncompressed bytes.
pub fn decompress(batch: SubBatch, out: *Io.Writer.Allocating) Error![]const u8 {
    switch (batch.compression) {
        .none => return batch.data,
        .gzip => {
            out.clearRetainingCapacity();
            try out.ensureTotalCapacity(batch.uncompressed_len);
            var in: Io.Reader = .fixed(batch.data);
            var d: flate.Decompress = .init(&in, .gzip, &.{});
            _ = d.reader.streamRemaining(&out.writer) catch return error.DecompressionFailed;
        },
        .zstd => {
            out.clearRetainingCapacity();
            try out.ensureTotalCapacity(batch.uncompressed_len);
            var in: Io.Reader = .fixed(batch.data);
            var d: zstd.Decompress = .init(&in, &.{}, .{});
            _ = d.reader.streamRemaining(&out.writer) catch return error.DecompressionFailed;
        },
        else => return error.UnsupportedCompression,
    }
    const result = out.written();
    if (result.len != batch.uncompressed_len) return error.DecompressionFailed;
    return result;
}

/// Compresses `data` with `compression` into `out`, which is cleared first.
/// Only `none` and `gzip` are supported for publishing.
pub fn compress(
    gpa: std.mem.Allocator,
    compression: Compression,
    data: []const u8,
    out: *Io.Writer.Allocating,
) Error![]const u8 {
    switch (compression) {
        .none => return data,
        .gzip => {
            out.clearRetainingCapacity();
            try out.ensureTotalCapacity(64 + data.len / 2);
            const window = try gpa.alloc(u8, flate.max_window_len);
            defer gpa.free(window);
            var c = flate.Compress.init(&out.writer, window, .gzip, .default) catch return error.OutOfMemory;
            c.writer.writeAll(data) catch return error.OutOfMemory;
            c.finish() catch return error.OutOfMemory;
            return out.written();
        },
        else => return error.UnsupportedCompression,
    }
}

const testing = std.testing;

fn buildChunk(w: *Io.Writer, chunk_id: u64, entries_data: []const u8, num_entries: u16, num_records: u32) !void {
    try w.writeByte(0x51);
    try w.writeByte(0);
    try w.writeInt(u16, num_entries, .big);
    try w.writeInt(u32, num_records, .big);
    try w.writeInt(i64, 1_700_000_000_000, .big);
    try w.writeInt(u64, 1, .big);
    try w.writeInt(u64, chunk_id, .big);
    try w.writeInt(u32, std.hash.Crc32.hash(entries_data), .big);
    try w.writeInt(u32, @intCast(entries_data.len), .big);
    try w.writeInt(u32, 0, .big);
    try w.writeInt(u32, 0, .big);
    try w.writeAll(entries_data);
}

test "simple entries and an uncompressed sub-batch" {
    var data_buf: [128]u8 = undefined;
    var dw: Io.Writer = .fixed(&data_buf);
    try dw.writeInt(u32, 3, .big);
    try dw.writeAll("one");
    // Sub-batch with two records, no compression.
    try dw.writeByte(0x80);
    try dw.writeInt(u16, 2, .big);
    try dw.writeInt(u32, 4 + 3 + 4 + 5, .big);
    try dw.writeInt(u32, 4 + 3 + 4 + 5, .big);
    try dw.writeInt(u32, 3, .big);
    try dw.writeAll("two");
    try dw.writeInt(u32, 5, .big);
    try dw.writeAll("three");

    var chunk_buf: [256]u8 = undefined;
    var cw: Io.Writer = .fixed(&chunk_buf);
    try buildChunk(&cw, 100, dw.buffered(), 2, 3);

    const c: Chunk = try .parse(cw.buffered());
    try testing.expect(c.verifyCrc());
    try testing.expectEqual(@as(u64, 100), c.header.chunk_id);
    try testing.expectEqual(@as(u32, 3), c.header.num_records);
    var it = c.entries();
    try testing.expectEqualStrings("one", (try it.next()).?.simple);
    const batch = (try it.next()).?.sub_batch;
    try testing.expectEqual(Compression.none, batch.compression);
    var out: Io.Writer.Allocating = .init(testing.allocator);
    defer out.deinit();
    var records = SubBatch.records(try decompress(batch, &out), batch.record_count);
    try testing.expectEqualStrings("two", (try records.next()).?);
    try testing.expectEqualStrings("three", (try records.next()).?);
    try testing.expectEqual(@as(?[]const u8, null), try records.next());
    try testing.expectEqual(@as(?Entry, null), try it.next());
}

test "gzip round trip" {
    const gpa = testing.allocator;
    var body: Io.Writer.Allocating = .init(gpa);
    defer body.deinit();
    for (0..50) |_| {
        try body.writer.writeInt(u32, 11, .big);
        try body.writer.writeAll("hello world");
    }
    var compressed: Io.Writer.Allocating = .init(gpa);
    defer compressed.deinit();
    const packed_data = try compress(gpa, .gzip, body.written(), &compressed);
    try testing.expect(packed_data.len < body.written().len);

    var out: Io.Writer.Allocating = .init(gpa);
    defer out.deinit();
    const plain = try decompress(.{
        .compression = .gzip,
        .record_count = 50,
        .uncompressed_len = @intCast(body.written().len),
        .data = packed_data,
    }, &out);
    try testing.expectEqualSlices(u8, body.written(), plain);
}

test "truncated chunk is rejected" {
    try testing.expectError(error.MalformedChunk, Chunk.parse("short"));
}

test "zstd sub-batch" {
    // Three records ("alpha", "beta", "gamma") compressed with the
    // reference zstd implementation.
    const compressed = [_]u8{
        0x28, 0xb5, 0x2f, 0xfd, 0x20, 0x1a, 0xd1, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x05, 0x61, 0x6c, 0x70, 0x68, 0x61, 0x00, 0x00, 0x00, 0x04, 0x62, 0x65,
        0x74, 0x61, 0x00, 0x00, 0x00, 0x05, 0x67, 0x61, 0x6d, 0x6d, 0x61,
    };
    var out: Io.Writer.Allocating = .init(testing.allocator);
    defer out.deinit();
    const body = try decompress(.{ .compression = .zstd, .record_count = 3, .uncompressed_len = 26, .data = &compressed }, &out);
    var records = SubBatch.records(body, 3);
    try testing.expectEqualStrings("alpha", (try records.next()).?);
    try testing.expectEqualStrings("beta", (try records.next()).?);
    try testing.expectEqualStrings("gamma", (try records.next()).?);
    try testing.expectError(error.UnsupportedCompression, decompress(.{ .compression = .lz4, .record_count = 1, .uncompressed_len = 1, .data = "x" }, &out));
}
