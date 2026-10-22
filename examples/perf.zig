//! Measures publish and consume throughput.
//!
//!     zig build -Doptimize=ReleaseFast run-perf -- [uri] [message count] [message size]

const std = @import("std");
const Io = std.Io;
const rs = @import("rabbitmq_stream");

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;
    const args = try init.minimal.args.toSlice(init.arena.allocator());
    const uri = if (args.len > 1) args[1] else "rabbitmq-stream://guest:guest@localhost:5552/%2f";
    const count = if (args.len > 2) try std.fmt.parseInt(usize, args[2], 10) else 1_000_000;
    const size = if (args.len > 3) try std.fmt.parseInt(usize, args[3], 10) else 100;

    const env: *rs.Environment = try .init(gpa, io, .{ .uris = &.{uri} });
    defer env.deinit();

    var name_buf: [32]u8 = undefined;
    var r: [4]u8 = undefined;
    io.random(&r);
    const stream = try std.fmt.bufPrint(&name_buf, "zig-perf-{x}", .{&r});
    try env.createStream(stream, .{});
    defer env.deleteStream(stream) catch {};

    const payload = try gpa.alloc(u8, size);
    defer gpa.free(payload);
    @memset(payload, 'x');
    const timeout: Io.Timeout = .{ .duration = .{ .raw = .fromSeconds(60), .clock = .awake } };

    {
        const publisher = try env.createPublisher(stream, .{ .batch_size = 500, .max_in_flight = 50_000 });
        defer publisher.close();
        const start = Io.Clock.awake.now(io);
        for (0..count) |_| _ = try publisher.sendRaw(payload, .{});
        try publisher.waitForConfirms(timeout);
        report("published and confirmed", count, start.untilNow(io, .awake));
    }
    {
        const consumer = try env.createConsumer(stream, .{ .offset = .first, .initial_credit = 20 });
        defer consumer.close();
        const start = Io.Clock.awake.now(io);
        var n: usize = 0;
        while (n < count) : (n += 1) _ = (try consumer.next(timeout)) orelse break;
        report("consumed", n, start.untilNow(io, .awake));
    }
}

fn report(what: []const u8, count: usize, elapsed: Io.Duration) void {
    const ms: f64 = @floatFromInt(@max(1, elapsed.toMilliseconds()));
    const rate = @as(f64, @floatFromInt(count)) / ms * 1000.0;
    std.log.info("{s} {d} messages in {d} ms ({d:.0} msg/s)", .{ what, count, elapsed.toMilliseconds(), rate });
}
