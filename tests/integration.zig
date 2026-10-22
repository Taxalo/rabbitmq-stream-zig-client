//! Integration tests. They need a RabbitMQ node with the stream plugin,
//! by default on localhost:5552 (override with RABBITMQ_STREAM_HOST and
//! RABBITMQ_STREAM_PORT). Run with `zig build integration`.

const std = @import("std");
const Io = std.Io;
const testing = std.testing;
const rs = @import("rabbitmq_stream");
const Client = rs.Client;
const Environment = rs.Environment;
const Publisher = rs.Publisher;
const Consumer = rs.Consumer;

const timeout: Io.Timeout = .{ .duration = .{ .raw = .fromSeconds(10), .clock = .awake } };
const short_timeout: Io.Timeout = .{ .duration = .{ .raw = .fromMilliseconds(500), .clock = .awake } };

var host_buf: [256]u8 = undefined;

fn clientOptions() Client.Options {
    var o: Client.Options = .{ .connection_name = "rabbitmq-stream-zig-tests" };
    if (testing.environ.getAlloc(testing.allocator, "RABBITMQ_STREAM_HOST")) |h| {
        defer testing.allocator.free(h);
        const n = @min(h.len, host_buf.len);
        @memcpy(host_buf[0..n], h[0..n]);
        o.host = host_buf[0..n];
    } else |_| {}
    if (testing.environ.getAlloc(testing.allocator, "RABBITMQ_STREAM_PORT")) |p| {
        defer testing.allocator.free(p);
        o.port = std.fmt.parseInt(u16, p, 10) catch o.port;
    } else |_| {}
    return o;
}

fn uniqueName(buf: []u8, prefix: []const u8) []const u8 {
    var r: [4]u8 = undefined;
    testing.io.random(&r);
    return std.fmt.bufPrint(buf, "{s}-{x}", .{ prefix, &r }) catch unreachable;
}

fn newEnv() !*Environment {
    return Environment.init(testing.allocator, testing.io, .{ .client = clientOptions() });
}

test "connect, server properties and stream lifecycle" {
    const client = try Client.connect(testing.allocator, testing.io, clientOptions());
    defer client.close();

    try testing.expect(client.serverProperty("version") != null);
    try testing.expect(client.server_version.major >= 3);
    try testing.expect(client.frame_max > 0);
    try testing.expect(client.connectionProperty("advertised_host") != null);
    try testing.expect(client.supportsFiltering());

    var buf: [64]u8 = undefined;
    const stream = uniqueName(&buf, "lifecycle");
    try testing.expect(!try client.streamExists(stream));
    try client.createStream(stream, .{
        .max_length_bytes = 10 * 1024 * 1024,
        .max_age = .fromSeconds(3600),
        .max_segment_size_bytes = 1024 * 1024,
        .leader_locator = .balanced,
        .filter_size_bytes = 32,
    });
    try testing.expectError(error.StreamAlreadyExists, client.createStream(stream, .{}));
    try testing.expect(try client.streamExists(stream));

    var m = try client.metadata(&.{ stream, "does-not-exist-stream" });
    defer m.deinit();
    try testing.expectEqual(rs.ResponseCode.ok, m.stream(stream).?.code);
    try testing.expect(m.stream(stream).?.leader != null);
    try testing.expectEqual(rs.ResponseCode.stream_does_not_exist, m.stream("does-not-exist-stream").?.code);

    const stats = try client.streamStats(stream);
    try testing.expectEqual(@as(i64, -1), stats.committed_chunk_id);

    try client.deleteStream(stream);
    try testing.expectError(error.StreamDoesNotExist, client.deleteStream(stream));
    try testing.expectError(error.StreamDoesNotExist, client.streamStats(stream));
}

test "authentication failure is reported" {
    var o = clientOptions();
    o.password = "wrong-password";
    try testing.expectError(error.AuthenticationFailure, Client.connect(testing.allocator, testing.io, o));
}

test "unknown virtual host is reported" {
    var o = clientOptions();
    o.virtual_host = "no-such-vhost";
    try testing.expectError(error.VirtualHostAccessFailure, Client.connect(testing.allocator, testing.io, o));
}

const message_count = 2_000;

const ConfirmCounter = struct {
    confirmed: std.atomic.Value(u64) = .init(0),
    errored: std.atomic.Value(u64) = .init(0),

    fn callback(context: ?*anyopaque, _: *Publisher, c: Publisher.Confirmation) void {
        const self: *ConfirmCounter = @ptrCast(@alignCast(context.?));
        if (c.code == .ok) {
            _ = self.confirmed.fetchAdd(c.message_count, .monotonic);
        } else {
            _ = self.errored.fetchAdd(c.message_count, .monotonic);
        }
    }
};

test "publish, confirm and consume AMQP messages" {
    const env = try newEnv();
    defer env.deinit();
    var buf: [64]u8 = undefined;
    const stream = uniqueName(&buf, "pubsub");
    try env.createStream(stream, .{});
    defer env.deleteStream(stream) catch {};

    var counter: ConfirmCounter = .{};
    const publisher = try env.createPublisher(stream, .{
        .batch_size = 64,
        .confirm_listener = .{ .context = &counter, .callback = ConfirmCounter.callback },
    });
    for (0..message_count) |i| {
        var body_buf: [32]u8 = undefined;
        const body = try std.fmt.bufPrint(&body_buf, "message-{d}", .{i});
        const msg: rs.Message = .{
            .properties = .{ .message_id = .{ .ulong = i }, .content_type = "text/plain" },
            .application_properties = &.{.{ .key = "index", .value = .{ .ulong = i } }},
            .body = .{ .data = body },
        };
        const id = try publisher.send(&msg, .{});
        try testing.expectEqual(@as(u64, i + 1), id);
    }
    try publisher.waitForConfirms(timeout);
    try testing.expectEqual(@as(u64, message_count), counter.confirmed.load(.monotonic));
    try testing.expectEqual(@as(u64, message_count), publisher.stats().confirmed);
    try testing.expectEqual(@as(u64, 0), publisher.stats().in_flight);
    publisher.close();

    const consumer = try env.createConsumer(stream, .{ .offset = .first, .name = "pubsub-reader", .initial_credit = 2 });
    defer consumer.close();
    var i: u64 = 0;
    while (i < message_count) : (i += 1) {
        const d = (try consumer.next(timeout)) orelse return error.TestTimeout;
        try testing.expectEqual(i, d.offset);
        const msg = try d.message();
        try testing.expectEqual(i, msg.properties.?.message_id.?.ulong);
        try testing.expectEqualStrings("text/plain", msg.properties.?.content_type.?);
        try testing.expectEqual(i, msg.application_properties.?.get("index").?.ulong);
        var body_buf: [32]u8 = undefined;
        try testing.expectEqualStrings(try std.fmt.bufPrint(&body_buf, "message-{d}", .{i}), msg.data().?);
        if (i == message_count - 1) try d.storeOffset();
    }
    try testing.expectEqual(@as(?Consumer.Delivery, null), try consumer.next(short_timeout));
    try testing.expectEqual(@as(u64, message_count), consumer.messages_consumed.load(.monotonic));

    // StoreOffset is not confirmed; poll until it is visible.
    var stored: ?u64 = null;
    for (0..50) |_| {
        stored = try consumer.queryOffset();
        if (stored != null) break;
        try testing.io.sleep(.fromMilliseconds(100), .awake);
    }
    try testing.expectEqual(@as(?u64, message_count - 1), stored);
    try testing.expectEqual(@as(?u64, null), try env.queryOffset("never-stored", stream));

    const stats = try env.streamStats(stream);
    try testing.expectEqual(@as(i64, 0), stats.first_chunk_id);
    try testing.expect(stats.committed_chunk_id >= 0);
}

test "offset specifications" {
    const env = try newEnv();
    defer env.deinit();
    var buf: [64]u8 = undefined;
    const stream = uniqueName(&buf, "offsets");
    try env.createStream(stream, .{});
    defer env.deleteStream(stream) catch {};

    const publisher = try env.createPublisher(stream, .{ .batch_size = 10, .flush_interval = null });
    for (0..100) |i| {
        var b: [16]u8 = undefined;
        _ = try publisher.sendRaw(try std.fmt.bufPrint(&b, "{d}", .{i}), .{});
    }
    try publisher.waitForConfirms(timeout);
    try testing.io.sleep(.fromMilliseconds(50), .awake);
    const before_second_batch = std.Io.Clock.real.now(testing.io).toMilliseconds();
    try testing.io.sleep(.fromMilliseconds(50), .awake);
    for (100..110) |i| {
        var b: [16]u8 = undefined;
        _ = try publisher.sendRaw(try std.fmt.bufPrint(&b, "{d}", .{i}), .{});
    }
    try publisher.waitForConfirms(timeout);
    defer publisher.close();

    {
        // A chunk can start before the requested offset; the consumer skips.
        const c = try env.createConsumer(stream, .{ .offset = .{ .offset = 42 } });
        defer c.close();
        const d = (try c.next(timeout)).?;
        try testing.expectEqual(@as(u64, 42), d.offset);
        try testing.expectEqualStrings("42", d.data);
    }
    {
        const c = try env.createConsumer(stream, .{ .offset = .last });
        defer c.close();
        const d = (try c.next(timeout)).?;
        try testing.expectEqual(@as(u64, 100), d.chunk_id);
    }
    {
        const c = try env.createConsumer(stream, .{ .offset = .{ .timestamp = before_second_batch } });
        defer c.close();
        const d = (try c.next(timeout)).?;
        try testing.expect(d.offset >= 100);
        try testing.expect(d.chunk_timestamp >= before_second_batch);
    }
    {
        const c = try env.createConsumer(stream, .{ .offset = .next });
        defer c.close();
        try testing.expectEqual(@as(?Consumer.Delivery, null), try c.next(short_timeout));
        _ = try publisher.sendRaw("after", .{});
        try publisher.flush();
        const d = (try c.next(timeout)).?;
        try testing.expectEqualStrings("after", d.data);
        try testing.expectEqual(@as(u64, 110), d.offset);
    }
}

test "resolve offset specification" {
    const client = try Client.connect(testing.allocator, testing.io, clientOptions());
    defer client.close();
    if (client.serverCommandVersion(.resolve_offset_spec) == null) return error.SkipZigTest;

    var buf: [64]u8 = undefined;
    const stream = uniqueName(&buf, "resolve");
    try client.createStream(stream, .{});
    defer client.deleteStream(stream) catch {};
    const publisher = try Publisher.init(client, stream, .{ .flush_interval = null });
    defer publisher.close();
    for (0..5) |_| _ = try publisher.sendRaw("x", .{});
    try publisher.waitForConfirms(timeout);
    // Offsets resolve to the chunk a subscription would start from.
    try testing.expectEqual(@as(u64, 0), try client.resolveOffsetSpec(stream, .first, &.{}));
    try testing.expectEqual(@as(u64, 5), try client.resolveOffsetSpec(stream, .next, &.{}));
    try testing.expect(try client.resolveOffsetSpec(stream, .{ .offset = 3 }, &.{}) <= 3);
}

test "deduplication with a named publisher" {
    const env = try newEnv();
    defer env.deinit();
    var buf: [64]u8 = undefined;
    const stream = uniqueName(&buf, "dedup");
    try env.createStream(stream, .{});
    defer env.deleteStream(stream) catch {};

    {
        const p = try env.createPublisher(stream, .{ .name = "dedup-app" });
        defer p.close();
        try testing.expectEqual(@as(u64, 0), try p.lastPublishingId());
        for (1..11) |id| _ = try p.sendRaw("m", .{ .publishing_id = id });
        try p.waitForConfirms(timeout);
        try testing.expectEqual(@as(u64, 10), try p.lastPublishingId());
    }
    {
        const p = try env.createPublisher(stream, .{ .name = "dedup-app" });
        defer p.close();
        try testing.expectEqual(@as(u64, 11), p.next_publishing_id);
        // Already stored ids are dropped by the broker but still confirmed.
        for (1..11) |id| _ = try p.sendRaw("dup", .{ .publishing_id = id });
        _ = try p.sendRaw("new", .{});
        try p.waitForConfirms(timeout);
        try testing.expectEqual(@as(u64, 11), try p.lastPublishingId());
    }
    try testing.expectEqual(@as(u64, 11), try env.queryPublisherSequence("dedup-app", stream));

    const c = try env.createConsumer(stream, .{ .offset = .first });
    defer c.close();
    var n: usize = 0;
    var last: []const u8 = "";
    var last_buf: [8]u8 = undefined;
    while (try c.next(short_timeout)) |d| : (n += 1) {
        @memcpy(last_buf[0..d.data.len], d.data);
        last = last_buf[0..d.data.len];
    }
    try testing.expectEqual(@as(usize, 11), n);
    try testing.expectEqualStrings("new", last);
}

fn regionIs(context: ?*anyopaque, d: *const Consumer.Delivery) bool {
    const wanted: *const []const u8 = @ptrCast(@alignCast(context.?));
    const msg = d.message() catch return false;
    const region = (msg.application_properties orelse return false).get("region") orelse return false;
    return std.mem.eql(u8, region.asString() orelse return false, wanted.*);
}

test "stream filtering" {
    const env = try newEnv();
    defer env.deinit();
    var buf: [64]u8 = undefined;
    const stream = uniqueName(&buf, "filter");
    try env.createStream(stream, .{ .filter_size_bytes = 32 });
    defer env.deleteStream(stream) catch {};

    const p = try env.createPublisher(stream, .{ .batch_size = 50, .flush_interval = null });
    const regions = [_][]const u8{ "emea", "amer", "apac" };
    // Separate publish calls so that each region lands in its own chunks.
    for (regions) |region| {
        for (0..200) |_| {
            const msg: rs.Message = .{
                .application_properties = &.{.{ .key = "region", .value = .{ .string = region } }},
                .body = .{ .data = region },
            };
            _ = try p.send(&msg, .{ .filter_value = region });
        }
        try p.waitForConfirms(timeout);
    }
    // Messages without a filter value.
    for (0..10) |_| _ = try p.send(&.{ .body = .{ .data = "none" } }, .{});
    try p.waitForConfirms(timeout);
    p.close();

    var wanted: []const u8 = "amer";
    const c = try env.createConsumer(stream, .{
        .offset = .first,
        .filter = .{
            .values = &.{"amer"},
            .post_filter = .{ .context = @ptrCast(&wanted), .callback = regionIs },
        },
    });
    defer c.close();
    var n: usize = 0;
    while (try c.next(short_timeout)) |d| : (n += 1) {
        try testing.expectEqualStrings("amer", (try d.message()).data().?);
    }
    try testing.expectEqual(@as(usize, 200), n);

    const unfiltered = try env.createConsumer(stream, .{
        .offset = .first,
        .filter = .{ .values = &.{"apac"}, .match_unfiltered = true },
    });
    defer unfiltered.close();
    var seen_none = false;
    var seen_emea = false;
    while (try unfiltered.next(short_timeout)) |d| {
        const body = (try d.message()).data().?;
        if (std.mem.eql(u8, body, "none")) seen_none = true;
        if (std.mem.eql(u8, body, "emea")) seen_emea = true;
    }
    try testing.expect(seen_none);
    try testing.expect(!seen_emea);
}

test "sub-entry batching with and without compression" {
    const env = try newEnv();
    defer env.deinit();
    var buf: [64]u8 = undefined;
    const stream = uniqueName(&buf, "subentry");
    try env.createStream(stream, .{});
    defer env.deleteStream(stream) catch {};

    var counter: ConfirmCounter = .{};
    for ([_]rs.chunk.Compression{ .none, .gzip }) |compression| {
        const p = try env.createPublisher(stream, .{
            .sub_entry_size = 20,
            .compression = compression,
            .confirm_listener = .{ .context = &counter, .callback = ConfirmCounter.callback },
        });
        defer p.close();
        for (0..105) |i| {
            var b: [32]u8 = undefined;
            const msg: rs.Message = .{ .body = .{ .data = try std.fmt.bufPrint(&b, "{t}-{d}", .{ compression, i }) } };
            _ = try p.send(&msg, .{});
        }
        try p.waitForConfirms(timeout);
    }
    try testing.expectEqual(@as(u64, 210), counter.confirmed.load(.monotonic));

    const c = try env.createConsumer(stream, .{ .offset = .first });
    defer c.close();
    var n: u64 = 0;
    while (try c.next(short_timeout)) |d| : (n += 1) {
        try testing.expectEqual(n, d.offset);
        const msg = try d.message();
        var b: [32]u8 = undefined;
        const expected = try std.fmt.bufPrint(&b, "{s}-{d}", .{ if (n < 105) "none" else "gzip", n % 105 });
        try testing.expectEqualStrings(expected, msg.data().?);
    }
    try testing.expectEqual(@as(u64, 210), n);
}

const UpdateRecorder = struct {
    activations: std.atomic.Value(u32) = .init(0),

    fn callback(context: ?*anyopaque, consumer: *Consumer, active: bool) rs.OffsetSpec {
        const self: *UpdateRecorder = @ptrCast(@alignCast(context.?));
        if (!active) return .next;
        _ = self.activations.fetchAdd(1, .monotonic);
        // Requests are allowed here.
        if (consumer.queryOffset() catch null) |o| return .{ .offset = o + 1 };
        return .first;
    }
};

test "single active consumer" {
    const env = try newEnv();
    defer env.deinit();
    var buf: [64]u8 = undefined;
    const stream = uniqueName(&buf, "sac");
    try env.createStream(stream, .{});
    defer env.deleteStream(stream) catch {};

    const p = try env.createPublisher(stream, .{});
    defer p.close();
    for (0..10) |_| _ = try p.sendRaw("a", .{});
    try p.waitForConfirms(timeout);

    var recorder: UpdateRecorder = .{};
    const options: Consumer.Options = .{
        .name = "sac-group",
        .single_active_consumer = true,
        .consumer_update_listener = .{ .context = &recorder, .callback = UpdateRecorder.callback },
    };
    const first = try env.createConsumer(stream, options);
    const second = try env.createConsumer(stream, options);
    defer second.close();

    var n: u64 = 0;
    while (try first.next(short_timeout)) |d| : (n += 1) {
        if (n == 4) try d.storeOffset();
    }
    try testing.expectEqual(@as(u64, 10), n);
    try testing.expect(first.isActive());
    try testing.expect(!second.isActive());
    try testing.expectEqual(@as(?Consumer.Delivery, null), try second.next(short_timeout));

    // Wait until the stored offset is visible, then hand over.
    for (0..50) |_| {
        if ((try first.queryOffset()) != null) break;
        try testing.io.sleep(.fromMilliseconds(100), .awake);
    }
    first.close();

    // The second consumer resumes after the stored offset.
    const d = (try second.next(timeout)) orelse return error.TestTimeout;
    try testing.expectEqual(@as(u64, 5), d.offset);
    try testing.expect(second.isActive());
    try testing.expectEqual(@as(u32, 2), recorder.activations.load(.monotonic));
}

test "super streams" {
    const env = try newEnv();
    defer env.deinit();
    var buf: [64]u8 = undefined;
    const name = uniqueName(&buf, "invoices");
    try env.createSuperStream(name, 3, .{});
    defer env.deleteSuperStream(name) catch {};

    const parts = try env.partitions(name);
    defer Client.freeStrings(testing.allocator, parts);
    try testing.expectEqual(@as(usize, 3), parts.len);
    var expected_buf: [80]u8 = undefined;
    try testing.expectEqualStrings(try std.fmt.bufPrint(&expected_buf, "{s}-0", .{name}), parts[0]);
    const routed = try env.route("2", name);
    defer Client.freeStrings(testing.allocator, routed);
    try testing.expectEqualStrings(parts[2], routed[0]);

    const sp = try env.createSuperStreamPublisher(name, .{});
    var per_partition: [3]u32 = @splat(0);
    for (0..300) |i| {
        var key_buf: [16]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "customer-{d}", .{i});
        per_partition[rs.SuperStreamPublisher.hashPartition(key, 3)] += 1;
        _ = try sp.send(key, &.{ .body = .{ .data = key } }, .{});
    }
    try sp.waitForConfirms(timeout);
    sp.close();

    {
        const kp = try env.createSuperStreamPublisher(name, .{ .routing = .key });
        defer kp.close();
        _ = try kp.sendRaw("1", "by-key", .{});
        try kp.waitForConfirms(timeout);
        try testing.expectError(error.NoRoute, kp.sendRaw("no-such-key", "x", .{}));
    }
    per_partition[1] += 1;

    for (parts, 0..) |part, i| {
        const stats = try env.streamStats(part);
        try testing.expectEqual(@as(i64, per_partition[i]) > 0, stats.committed_chunk_id >= 0);
    }

    const sc = try env.createSuperStreamConsumer(name, .{ .offset = .first });
    defer sc.close();
    var counts: [3]u32 = @splat(0);
    var total: usize = 0;
    while (try sc.next(short_timeout)) |d| : (total += 1) {
        for (parts, 0..) |part, i| {
            if (std.mem.eql(u8, part, d.consumer.stream)) counts[i] += 1;
        }
    }
    try testing.expectEqual(@as(usize, 301), total);
    try testing.expectEqualSlices(u32, &per_partition, &counts);
}

test "consumer reports a deleted stream" {
    const env = try newEnv();
    defer env.deinit();
    var buf: [64]u8 = undefined;
    const stream = uniqueName(&buf, "deleted");
    try env.createStream(stream, .{});

    const c = try env.createConsumer(stream, .{});
    defer c.close();
    const p = try env.createPublisher(stream, .{});
    defer p.close();
    try env.deleteStream(stream);
    try testing.expectError(error.StreamUnavailable, c.next(timeout));
    // The publisher learns about it from the metadata update too.
    var failed = false;
    for (0..50) |_| {
        _ = p.sendRaw("x", .{}) catch |err| {
            try testing.expectEqual(error.StreamUnavailable, err);
            failed = true;
            break;
        };
        try testing.io.sleep(.fromMilliseconds(50), .awake);
    }
    try testing.expect(failed);
}

test "low-level client: subscribe, credit and unsubscribe" {
    const client = try Client.connect(testing.allocator, testing.io, clientOptions());
    defer client.close();
    var buf: [64]u8 = undefined;
    const stream = uniqueName(&buf, "lowlevel");
    try client.createStream(stream, .{});
    defer client.deleteStream(stream) catch {};

    const p = try Publisher.init(client, stream, .{ .batch_size = 1, .flush_interval = null });
    defer p.close();
    for (0..20) |_| _ = try p.sendRaw("chunk", .{});
    try p.waitForConfirms(timeout);

    // Two consumers on the same connection.
    const a = try Consumer.init(client, stream, .{ .offset = .first, .initial_credit = 1 });
    defer a.close();
    const b = try Consumer.init(client, stream, .{ .offset = .{ .offset = 10 }, .initial_credit = 1 });
    defer b.close();
    var n: usize = 0;
    while (try a.next(short_timeout)) |_| n += 1;
    try testing.expectEqual(@as(usize, 20), n);
    n = 0;
    while (try b.next(short_timeout)) |_| n += 1;
    try testing.expectEqual(@as(usize, 10), n);
    try testing.expectError(error.SubscriptionIdDoesNotExist, client.unsubscribe(200));
}

test "many concurrent publishers on one connection" {
    const client = try Client.connect(testing.allocator, testing.io, clientOptions());
    defer client.close();
    var buf: [64]u8 = undefined;
    const stream = uniqueName(&buf, "concurrent");
    try client.createStream(stream, .{});
    defer client.deleteStream(stream) catch {};

    const Worker = struct {
        fn run(cl: *Client, s: []const u8) Publisher.Error!void {
            const p = try Publisher.init(cl, s, .{ .max_in_flight = 100 });
            defer p.close();
            for (0..1000) |_| _ = try p.sendRaw("concurrent", .{});
            try p.waitForConfirms(timeout);
        }
    };
    var futures: [4]Io.Future(Publisher.Error!void) = undefined;
    for (&futures) |*f| f.* = try testing.io.concurrent(Worker.run, .{ client, stream });
    for (&futures) |*f| try f.await(testing.io);

    const stats = try client.streamStats(stream);
    try testing.expect(stats.committed_chunk_id >= 0);
    const c = try Consumer.init(client, stream, .{ .offset = .first });
    defer c.close();
    var n: usize = 0;
    while (try c.next(short_timeout)) |_| n += 1;
    try testing.expectEqual(@as(usize, 4000), n);
}

test "heartbeats keep an idle connection open" {
    var o = clientOptions();
    o.heartbeat = 1;
    const client = try Client.connect(testing.allocator, testing.io, o);
    defer client.close();
    try testing.expectEqual(@as(u32, 1), client.heartbeat);
    try testing.io.sleep(.fromMilliseconds(3500), .awake);
    try testing.expect(client.isOpen());
    _ = try client.streamExists("heartbeat-check");
}

fn nothing(_: *rs.protocol.Encoder) rs.protocol.Encoder.Error!void {}

const CloseRecorder = struct {
    reason: ?Client.CloseReason = null,

    fn callback(context: ?*anyopaque, _: *Client, reason: Client.CloseReason) void {
        const self: *CloseRecorder = @ptrCast(@alignCast(context.?));
        self.reason = reason;
    }
};

test "a close sent by the broker is handled" {
    var recorder: CloseRecorder = .{};
    var o = clientOptions();
    o.on_close = .{ .context = &recorder, .callback = CloseRecorder.callback };
    const client = try Client.connect(testing.allocator, testing.io, o);
    defer client.close();
    // The broker answers an unknown command by closing the connection.
    const unknown: rs.protocol.Command = @fromBackingInt(0x7f00);
    try testing.expectError(error.ConnectionClosed, client.request(unknown, nothing, .{}));
    try client.waitClosed();
    try testing.expectEqual(Client.CloseReason{ .server = .unknown_frame }, recorder.reason.?);
    try testing.expectError(error.ConnectionClosed, client.streamExists("x"));
}

fn toLoopback(_: ?*anyopaque, advertised: Environment.Endpoint) Environment.Endpoint {
    return .{ .host = "127.0.0.1", .port = advertised.port };
}

test "environment node selection modes" {
    var buf: [64]u8 = undefined;
    const stream = uniqueName(&buf, "modes");
    var uri_buf: [128]u8 = undefined;
    const base = clientOptions();
    const uri = try std.fmt.bufPrint(&uri_buf, "rabbitmq-stream://guest:guest@{s}:{d}/%2f", .{ base.host, base.port });

    const variants = [_]Environment.Options{
        .{ .client = base, .address_resolver = .{ .callback = toLoopback } },
        .{ .client = base, .load_balancer = true },
        .{ .uris = &.{ "rabbitmq-stream://guest:guest@unreachable.invalid:5552", uri } },
    };
    for (variants, 0..) |options, i| {
        const env = try Environment.init(testing.allocator, testing.io, options);
        defer env.deinit();
        if (i == 0) try env.createStream(stream, .{});
        const p = try env.createPublisher(stream, .{});
        _ = try p.sendRaw("x", .{});
        try p.waitForConfirms(timeout);
        p.close();
        const c = try env.createConsumer(stream, .{ .offset = .first });
        defer c.close();
        var n: usize = 0;
        while (try c.next(short_timeout)) |_| n += 1;
        try testing.expectEqual(i + 1, n);
        if (i == variants.len - 1) try env.deleteStream(stream);
    }
}

test "TLS connection" {
    const port_text = testing.environ.getAlloc(testing.allocator, "RABBITMQ_STREAM_TLS_PORT") catch return error.SkipZigTest;
    defer testing.allocator.free(port_text);
    var o = clientOptions();
    o.port = try std.fmt.parseInt(u16, port_text, 10);
    const ca = testing.environ.getAlloc(testing.allocator, "RABBITMQ_STREAM_TLS_CA") catch null;
    defer if (ca) |path| testing.allocator.free(path);
    o.tls = .{ .ca = if (ca) |path| .{ .file = path } else .no_verification };

    const client = try Client.connect(testing.allocator, testing.io, o);
    defer client.close();
    var buf: [64]u8 = undefined;
    const stream = uniqueName(&buf, "tls");
    try client.createStream(stream, .{});
    defer client.deleteStream(stream) catch {};

    // Enough traffic in both directions for TLS key updates.
    const payload: [4096]u8 = @splat('t');
    const p = try Publisher.init(client, stream, .{});
    defer p.close();
    for (0..1000) |_| _ = try p.sendRaw(&payload, .{});
    try p.waitForConfirms(timeout);
    const c = try Consumer.init(client, stream, .{ .offset = .first });
    defer c.close();
    var n: usize = 0;
    while (try c.next(short_timeout)) |d| : (n += 1) try testing.expectEqual(@as(usize, 4096), d.data.len);
    try testing.expectEqual(@as(usize, 1000), n);
}

test "messages larger than the frame size are rejected" {
    var o = clientOptions();
    o.max_frame_size = 4096;
    const client = try Client.connect(testing.allocator, testing.io, o);
    defer client.close();
    try testing.expectEqual(@as(u32, 4096), client.frame_max);
    var buf: [64]u8 = undefined;
    const stream = uniqueName(&buf, "framemax");
    try client.createStream(stream, .{});
    defer client.deleteStream(stream) catch {};
    const p = try Publisher.init(client, stream, .{ .flush_interval = null });
    defer p.close();
    const big: [5000]u8 = @splat(0);
    try testing.expectError(error.MessageTooLarge, p.sendRaw(&big, .{}));
    // Batches are split to fit in the frame size.
    for (0..20) |_| _ = try p.sendRaw(big[0..1000], .{});
    try p.waitForConfirms(timeout);
    try testing.expectEqual(@as(u64, 20), p.stats().confirmed);
}
