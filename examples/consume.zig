//! Reads a stream from where this consumer left off, tracking its offset
//! on the broker.
//!
//!     zig build run-consume -- [rabbitmq-stream://guest:guest@localhost:5552/%2f]

const std = @import("std");
const rs = @import("rabbitmq_stream");

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;
    const args = try init.minimal.args.toSlice(init.arena.allocator());
    const uri = if (args.len > 1) args[1] else "rabbitmq-stream://guest:guest@localhost:5552/%2f";
    const stream = if (args.len > 2) args[2] else "zig-example-stream";

    const env: *rs.Environment = try .init(gpa, io, .{ .uris = &.{uri} });
    defer env.deinit();

    const reference = "zig-example-consumer";
    // Resume after the last stored offset, or start at the beginning.
    const start: rs.OffsetSpec = if (try env.queryOffset(reference, stream)) |o| .{ .offset = o + 1 } else .first;

    const consumer = try env.createConsumer(stream, .{ .offset = start, .name = reference });
    defer consumer.close();

    var count: usize = 0;
    var last: ?u64 = null;
    const idle: std.Io.Timeout = .{ .duration = .{ .raw = .fromSeconds(2), .clock = .awake } };
    while (try consumer.next(idle)) |delivery| {
        const message = delivery.message() catch {
            std.log.info("offset {d}: {d} raw bytes", .{ delivery.offset, delivery.data.len });
            continue;
        };
        if (count < 5) std.log.info("offset {d}: {s}", .{ delivery.offset, message.data() orelse "(non-binary body)" });
        count += 1;
        last = delivery.offset;
        if (count % 1000 == 0) try consumer.storeOffset(delivery.offset);
    }
    if (last) |o| try consumer.storeOffset(o);
    std.log.info("consumed {d} messages; no new message for 2 s", .{count});
}
