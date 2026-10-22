//! Creates a super stream with three partitions, publishes with hash
//! routing and consumes every partition as a single active consumer.
//! Start several instances to see the partitions split between them.
//!
//!     zig build run-super_stream -- [rabbitmq-stream://guest:guest@localhost:5552/%2f]

const std = @import("std");
const rs = @import("rabbitmq_stream");

fn onUpdate(_: ?*anyopaque, consumer: *rs.Consumer, active: bool) rs.OffsetSpec {
    std.log.info("partition {s} is now {s}", .{ consumer.stream, if (active) "active" else "inactive" });
    const stored = consumer.queryOffset() catch null;
    return if (stored) |o| .{ .offset = o + 1 } else .first;
}

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;
    const args = try init.minimal.args.toSlice(init.arena.allocator());
    const uri = if (args.len > 1) args[1] else "rabbitmq-stream://guest:guest@localhost:5552/%2f";

    const env: *rs.Environment = try .init(gpa, io, .{ .uris = &.{uri} });
    defer env.deinit();

    const name = "zig-invoices";
    env.createSuperStream(name, 3, .{}) catch |err| switch (err) {
        error.StreamAlreadyExists => {},
        else => return err,
    };

    {
        const producer = try env.createSuperStreamPublisher(name, .{ .routing = .hash });
        defer producer.close();
        for (0..90) |i| {
            var key: [32]u8 = undefined;
            const customer = try std.fmt.bufPrint(&key, "customer-{d}", .{i % 30});
            _ = try producer.send(customer, &.{ .body = .{ .data = customer } }, .{});
        }
        try producer.waitForConfirms(.{ .duration = .{ .raw = .fromSeconds(10), .clock = .awake } });
    }

    const consumer = try env.createSuperStreamConsumer(name, .{
        .name = "zig-invoice-processor",
        .single_active_consumer = true,
        .consumer_update_listener = .{ .callback = onUpdate },
    });
    defer consumer.close();

    var count: usize = 0;
    const idle: std.Io.Timeout = .{ .duration = .{ .raw = .fromSeconds(3), .clock = .awake } };
    while (try consumer.next(idle)) |d| : (count += 1) {
        try d.storeOffset();
    }
    std.log.info("processed {d} messages", .{count});
}
