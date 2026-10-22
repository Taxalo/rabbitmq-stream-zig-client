//! Stream filtering: the broker only sends chunks that may contain the
//! wanted filter values, and the consumer filters each message again.
//!
//!     zig build run-filtering -- [rabbitmq-stream://guest:guest@localhost:5552/%2f]

const std = @import("std");
const rs = @import("rabbitmq_stream");

fn isState(context: ?*anyopaque, delivery: *const rs.Delivery) bool {
    const wanted: *const []const u8 = @ptrCast(@alignCast(context.?));
    const message = delivery.message() catch return false;
    const props = message.application_properties orelse return false;
    const state = props.get("state") orelse return false;
    return std.mem.eql(u8, state.asString() orelse return false, wanted.*);
}

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;
    const args = try init.minimal.args.toSlice(init.arena.allocator());
    const uri = if (args.len > 1) args[1] else "rabbitmq-stream://guest:guest@localhost:5552/%2f";

    const env: *rs.Environment = try .init(gpa, io, .{ .uris = &.{uri} });
    defer env.deinit();

    const stream = "zig-filtering";
    env.createStream(stream, .{ .filter_size_bytes = 32 }) catch |err| switch (err) {
        error.StreamAlreadyExists => {},
        else => return err,
    };

    const states = [_][]const u8{ "california", "texas", "new-york" };
    {
        const publisher = try env.createPublisher(stream, .{});
        defer publisher.close();
        for (states) |state| {
            for (0..100) |_| {
                const message: rs.Message = .{
                    .application_properties = &.{.{ .key = "state", .value = .{ .string = state } }},
                    .body = .{ .data = state },
                };
                _ = try publisher.send(&message, .{ .filter_value = state });
            }
            try publisher.flush();
        }
        try publisher.waitForConfirms(.{ .duration = .{ .raw = .fromSeconds(10), .clock = .awake } });
    }

    var wanted: []const u8 = "texas";
    const consumer = try env.createConsumer(stream, .{
        .offset = .first,
        .filter = .{
            .values = &.{wanted},
            .post_filter = .{ .context = @ptrCast(&wanted), .callback = isState },
        },
    });
    defer consumer.close();

    var count: usize = 0;
    const idle: std.Io.Timeout = .{ .duration = .{ .raw = .fromSeconds(2), .clock = .awake } };
    while (try consumer.next(idle)) |_| count += 1;
    std.log.info("received {d} messages for {s}", .{ count, wanted });
}
