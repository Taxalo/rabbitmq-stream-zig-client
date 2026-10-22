//! Publishes messages to a stream and waits for the broker to confirm them.
//!
//!     zig build run-publish -- [rabbitmq-stream://guest:guest@localhost:5552/%2f]

const std = @import("std");
const rs = @import("rabbitmq_stream");

const ConfirmCounter = struct {
    ok: std.atomic.Value(u64) = .init(0),
    failed: std.atomic.Value(u64) = .init(0),

    fn onConfirm(context: ?*anyopaque, _: *rs.Publisher, c: rs.Publisher.Confirmation) void {
        const self: *ConfirmCounter = @ptrCast(@alignCast(context.?));
        if (c.code == .ok) {
            _ = self.ok.fetchAdd(c.message_count, .monotonic);
        } else {
            _ = self.failed.fetchAdd(c.message_count, .monotonic);
        }
    }
};

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;
    const args = try init.minimal.args.toSlice(init.arena.allocator());
    const uri = if (args.len > 1) args[1] else "rabbitmq-stream://guest:guest@localhost:5552/%2f";

    const env: *rs.Environment = try .init(gpa, io, .{ .uris = &.{uri} });
    defer env.deinit();

    const stream = "zig-example-stream";
    env.createStream(stream, .{
        .max_length_bytes = 1024 * 1024 * 1024,
        .max_segment_size_bytes = 100 * 1024 * 1024,
    }) catch |err| switch (err) {
        error.StreamAlreadyExists => {},
        else => return err,
    };

    var counter: ConfirmCounter = .{};
    const publisher = try env.createPublisher(stream, .{
        .confirm_listener = .{ .context = &counter, .callback = ConfirmCounter.onConfirm },
    });
    defer publisher.close();

    const count = 10_000;
    for (0..count) |i| {
        var body: [64]u8 = undefined;
        const message: rs.Message = .{
            .properties = .{ .message_id = .{ .ulong = i }, .content_type = "text/plain" },
            .application_properties = &.{.{ .key = "source", .value = .{ .string = "zig-example" } }},
            .body = .{ .data = try std.fmt.bufPrint(&body, "hello from Zig #{d}", .{i}) },
        };
        _ = try publisher.send(&message, .{});
    }
    try publisher.waitForConfirms(.{ .duration = .{ .raw = .fromSeconds(30), .clock = .awake } });
    std.log.info("{d} messages confirmed, {d} failed", .{ counter.ok.load(.monotonic), counter.failed.load(.monotonic) });
}
