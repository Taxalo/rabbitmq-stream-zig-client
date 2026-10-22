//! Consumes messages from a stream.
//!
//! The broker delivers chunks while the subscription has credit; each chunk
//! costs one credit. The consumer queues delivered chunks and gives one
//! credit back each time the application has iterated over a chunk with
//! `next`, so a slow application slows the broker down instead of
//! accumulating memory.

const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;
const assert = std.debug.assert;

const protocol = @import("protocol.zig");
const ResponseCode = protocol.ResponseCode;
const OffsetSpec = protocol.OffsetSpec;
const Property = protocol.Property;
const amqp = @import("amqp.zig");
const chunk = @import("chunk.zig");
const Client = @import("Client.zig");

const log = std.log.scoped(.rabbitmq_stream);

const Consumer = @This();

gpa: Allocator,
io: Io,
/// `null` once the client has been destroyed.
client: ?*Client,
owns_client: bool = false,
id: u8 = 0,
stream: []const u8,
name: ?[]const u8,
/// Owns copies of the option strings.
arena: std.heap.ArenaAllocator,
options: Options,
inbox: *Inbox,
owns_inbox: bool,
/// Records below this offset are skipped; chunks may start before the
/// requested offset.
start_offset: std.atomic.Value(u64) = .init(0),
/// Whether this consumer is the active one of a single active consumer
/// group. Always true otherwise.
active: std.atomic.Value(bool) = .init(true),
messages_consumed: std.atomic.Value(u64) = .init(0),

pub const Error = Client.Error || chunk.Error || error{
    ConsumerClosed,
    StreamUnavailable,
    FilteringNotSupported,
    NoConsumerName,
};

pub const Options = struct {
    /// Where to start reading.
    offset: OffsetSpec = .next,
    /// Consumer reference, used for offset tracking and to name the single
    /// active consumer group. At most 256 bytes.
    name: ?[]const u8 = null,
    /// Number of chunks the broker may send ahead of the application.
    initial_credit: u16 = 10,
    /// Stream filtering (RabbitMQ 3.13 and later).
    filter: ?Filter = null,
    /// Only one consumer of the group named `name` receives messages.
    single_active_consumer: bool = false,
    /// Decides where an activated single active consumer starts. Without
    /// it, the consumer resumes after the offset stored for `name`, or uses
    /// `offset` when none is stored.
    consumer_update_listener: ?ConsumerUpdateListener = null,
    /// Name of the super stream this consumer reads a partition of.
    super_stream: ?[]const u8 = null,
    /// Additional subscription properties.
    properties: []const Property = &.{},
    /// Verify the CRC of every chunk.
    check_crc: bool = false,
};

pub const Filter = struct {
    /// The broker delivers chunks that may contain messages with one of
    /// these filter values.
    values: []const []const u8,
    /// Also deliver messages published without a filter value.
    match_unfiltered: bool = false,
    /// Chunks can contain messages that do not match, so the application
    /// filters each message again. Messages for which this returns false
    /// are skipped by `next`.
    post_filter: ?PostFilter = null,
};

pub const PostFilter = struct {
    context: ?*anyopaque = null,
    callback: *const fn (context: ?*anyopaque, delivery: *const Delivery) bool,
};

/// Called from a client task when the broker activates or deactivates a
/// single active consumer. The return value is the starting point after
/// activation and is ignored on deactivation. The callback may perform
/// requests such as `queryOffset`.
pub const ConsumerUpdateListener = struct {
    context: ?*anyopaque = null,
    callback: *const fn (context: ?*anyopaque, consumer: *Consumer, active: bool) OffsetSpec,
};

/// A message returned by `next`. `data` is valid until the next call to
/// `next` on the same inbox.
pub const Delivery = struct {
    consumer: *Consumer,
    offset: u64,
    chunk_id: u64,
    /// When the chunk was written, in milliseconds since the Unix epoch.
    chunk_timestamp: i64,
    /// The last committed chunk id, with deliver version 2.
    committed_chunk_id: ?u64,
    /// The message as stored, normally AMQP 1.0 encoded.
    data: []const u8,

    /// Decodes the AMQP 1.0 message. The result points into `data`.
    pub fn message(d: *const Delivery) amqp.DecodeError!amqp.DecodedMessage {
        return amqp.decode(d.data);
    }

    /// Stores this delivery's offset for the consumer's name.
    pub fn storeOffset(d: *const Delivery) Error!void {
        return d.consumer.storeOffset(d.offset);
    }
};

/// Subscribes to `stream` on `client`. Close it with `close` before
/// closing the client.
pub fn init(client: *Client, stream: []const u8, options: Options) Error!*Consumer {
    const inbox = try Inbox.create(client.gpa, client.io);
    errdefer inbox.destroy();
    const c = try initWithInbox(client, stream, options, inbox);
    c.owns_inbox = true;
    return c;
}

/// Like `init`, but delivers into a shared `Inbox`, so that one task can
/// read from several consumers. The inbox must outlive the consumer.
pub fn initWithInbox(client: *Client, stream: []const u8, options: Options, inbox: *Inbox) Error!*Consumer {
    if (options.name) |n| assert(n.len <= protocol.max_reference_len);
    if (options.single_active_consumer and options.name == null) return error.NoConsumerName;
    if (options.filter != null and !client.supportsFiltering()) return error.FilteringNotSupported;

    const gpa = client.gpa;
    const c = try gpa.create(Consumer);
    errdefer gpa.destroy(c);
    c.* = .{
        .gpa = gpa,
        .io = client.io,
        .client = client,
        .stream = "",
        .name = null,
        .arena = .init(gpa),
        .options = options,
        .inbox = inbox,
        .owns_inbox = false,
    };
    errdefer c.arena.deinit();
    try c.copyOptions(stream, options);
    if (options.offset == .offset) c.start_offset.store(options.offset.offset, .monotonic);
    if (options.single_active_consumer) c.active.store(false, .monotonic);

    var props: std.ArrayList(Property) = .empty;
    defer props.deinit(gpa);
    var filter_keys: std.ArrayList(u8) = .empty;
    defer filter_keys.deinit(gpa);
    try c.subscriptionProperties(&props, &filter_keys);

    inbox.addSource();
    errdefer inbox.removeSource();
    c.id = try client.registerConsumer(c);
    errdefer client.unregisterConsumer(c.id);
    try client.subscribe(c.id, c.stream, options.offset, @max(1, options.initial_credit), props.items);
    return c;
}

fn copyOptions(c: *Consumer, stream: []const u8, options: Options) Allocator.Error!void {
    const arena = c.arena.allocator();
    c.stream = try arena.dupe(u8, stream);
    if (options.name) |n| c.name = try arena.dupe(u8, n);
    c.options.name = c.name;
    if (options.super_stream) |s| c.options.super_stream = try arena.dupe(u8, s);
    if (options.filter) |f| {
        const values = try arena.alloc([]const u8, f.values.len);
        for (values, f.values) |*dst, src| dst.* = try arena.dupe(u8, src);
        c.options.filter.?.values = values;
    }
    const props = try arena.alloc(Property, options.properties.len);
    for (props, options.properties) |*dst, src| dst.* = .{
        .key = try arena.dupe(u8, src.key),
        .value = try arena.dupe(u8, src.value),
    };
    c.options.properties = props;
}

fn subscriptionProperties(c: *Consumer, props: *std.ArrayList(Property), keys: *std.ArrayList(u8)) Allocator.Error!void {
    const gpa = c.gpa;
    if (c.name) |n| try props.append(gpa, .{ .key = "name", .value = n });
    if (c.options.single_active_consumer) try props.append(gpa, .{ .key = "single-active-consumer", .value = "true" });
    if (c.options.super_stream) |s| try props.append(gpa, .{ .key = "super-stream", .value = s });
    if (c.options.filter) |f| {
        // Keys are formatted into one buffer first so that slices stay valid.
        for (0..f.values.len) |i| try keys.print(gpa, "filter.{d}", .{i});
        var pos: usize = 0;
        for (f.values, 0..) |v, i| {
            const len = std.fmt.count("filter.{d}", .{i});
            try props.append(gpa, .{ .key = keys.items[pos..][0..len], .value = v });
            pos += len;
        }
        if (f.match_unfiltered) try props.append(gpa, .{ .key = "match-unfiltered", .value = "true" });
    }
    try props.appendSlice(gpa, c.options.properties);
}

/// Unsubscribes and frees the consumer. Undelivered chunks are dropped.
pub fn close(c: *Consumer) void {
    if (c.client) |client| {
        if (client.isOpen()) {
            client.unsubscribe(c.id) catch |err| log.debug("unsubscribe failed: {t}", .{err});
        }
        client.unregisterConsumer(c.id);
    }
    c.inbox.dropFrom(c);
    c.inbox.removeSource();
    if (c.owns_inbox) c.inbox.destroy();
    const owned_client = if (c.owns_client) c.client else null;
    c.arena.deinit();
    c.gpa.destroy(c);
    if (owned_client) |client| client.close();
}

/// Returns the next message, waiting up to `timeout` for one. Returns
/// `null` on timeout. See `Inbox.next`.
pub fn next(c: *Consumer, timeout: Io.Timeout) Error!?Delivery {
    return c.inbox.next(timeout);
}

/// Stores `offset` for this consumer's name, so that it can resume from
/// it. The broker does not confirm the operation.
pub fn storeOffset(c: *Consumer, offset: u64) Error!void {
    const name = c.name orelse return error.NoConsumerName;
    const client = c.client orelse return error.ConsumerClosed;
    return client.storeOffset(name, c.stream, offset);
}

/// Returns the offset stored for this consumer's name, if any.
pub fn queryOffset(c: *Consumer) Error!?u64 {
    const name = c.name orelse return error.NoConsumerName;
    const client = c.client orelse return error.ConsumerClosed;
    return client.queryOffset(name, c.stream);
}

pub fn isActive(c: *const Consumer) bool {
    return c.active.load(.monotonic);
}

// Called by the client's tasks.

/// Takes ownership of `frame`. Called with the client's state lock held.
pub fn handleDeliver(c: *Consumer, frame: []u8, chunk_start: usize, committed: ?u64) void {
    c.inbox.push(.{ .consumer = c, .frame = frame, .chunk_start = chunk_start, .committed = committed });
}

pub fn handleConsumerUpdate(c: *Consumer, active: bool) ?OffsetSpec {
    c.active.store(active, .monotonic);
    log.debug("consumer {d} on '{s}' is now {s}", .{ c.id, c.stream, if (active) "active" else "inactive" });
    if (!active) {
        if (c.options.consumer_update_listener) |l| _ = l.callback(l.context, c, false);
        return null;
    }
    const spec: OffsetSpec = if (c.options.consumer_update_listener) |l|
        l.callback(l.context, c, true)
    else if (c.queryOffset() catch null) |stored|
        .{ .offset = stored + 1 }
    else
        c.options.offset;
    c.start_offset.store(if (spec == .offset) spec.offset else 0, .monotonic);
    return spec;
}

pub fn handleStreamUnavailable(c: *Consumer, code: ResponseCode) void {
    _ = code;
    c.inbox.fail(error.StreamUnavailable);
}

pub fn handleConnectionClosed(c: *Consumer) void {
    c.inbox.fail(error.ConnectionClosed);
}

/// Called when the client is destroyed while the consumer is open.
pub fn detachClient(c: *Consumer) void {
    c.inbox.fail(error.ConnectionClosed);
    c.client = null;
    c.owns_client = false;
}

/// Grants credit back once a chunk has been consumed.
fn chunkDone(c: *Consumer) void {
    const client = c.client orelse return;
    if (!client.isOpen()) return;
    client.credit(c.id, 1) catch |err| log.debug("credit failed: {t}", .{err});
}

/// A queue of delivered chunks shared by one or more consumers, and the
/// iteration state over the current chunk.
pub const Inbox = struct {
    gpa: Allocator,
    io: Io,
    mutex: Io.Mutex = .init,
    cond: Io.Condition = .init,
    queue: std.Deque(Item) = .empty,
    sources: u32 = 0,
    failure: ?Error = null,
    /// Iteration state, only touched by the task calling `next`.
    current: ?Current = null,
    decompressed: Io.Writer.Allocating,

    const Item = struct {
        consumer: *Consumer,
        frame: []u8,
        chunk_start: usize,
        committed: ?u64,
    };

    const Current = struct {
        item: Item,
        chunk: chunk.Chunk,
        entries: chunk.EntryIterator,
        records: ?chunk.RecordIterator = null,
        next_offset: u64,
    };

    pub fn create(gpa: Allocator, io: Io) Allocator.Error!*Inbox {
        const inbox = try gpa.create(Inbox);
        inbox.* = .{ .gpa = gpa, .io = io, .decompressed = .init(gpa) };
        return inbox;
    }

    /// Frees the inbox. All consumers using it must be closed.
    pub fn destroy(inbox: *Inbox) void {
        assert(inbox.sources == 0);
        if (inbox.current) |cur| inbox.gpa.free(cur.item.frame);
        while (inbox.queue.popFront()) |item| inbox.gpa.free(item.frame);
        inbox.queue.deinit(inbox.gpa);
        inbox.decompressed.deinit();
        inbox.gpa.destroy(inbox);
    }

    fn addSource(inbox: *Inbox) void {
        inbox.mutex.lockUncancelable(inbox.io);
        defer inbox.mutex.unlock(inbox.io);
        inbox.sources += 1;
    }

    fn removeSource(inbox: *Inbox) void {
        inbox.mutex.lockUncancelable(inbox.io);
        defer inbox.mutex.unlock(inbox.io);
        inbox.sources -= 1;
        inbox.cond.broadcast(inbox.io);
    }

    fn push(inbox: *Inbox, item: Item) void {
        inbox.mutex.lockUncancelable(inbox.io);
        defer inbox.mutex.unlock(inbox.io);
        inbox.queue.pushBack(inbox.gpa, item) catch {
            log.err("out of memory queueing a chunk; dropping it", .{});
            inbox.gpa.free(item.frame);
            return;
        };
        inbox.cond.signal(inbox.io);
    }

    fn fail(inbox: *Inbox, err: Error) void {
        inbox.mutex.lockUncancelable(inbox.io);
        defer inbox.mutex.unlock(inbox.io);
        if (inbox.failure == null) inbox.failure = err;
        inbox.cond.broadcast(inbox.io);
    }

    /// Drops queued chunks of a consumer that is being closed.
    fn dropFrom(inbox: *Inbox, c: *Consumer) void {
        inbox.mutex.lockUncancelable(inbox.io);
        defer inbox.mutex.unlock(inbox.io);
        if (inbox.current) |cur| {
            if (cur.item.consumer == c) {
                inbox.gpa.free(cur.item.frame);
                inbox.current = null;
            }
        }
        var kept: std.Deque(Item) = .empty;
        while (inbox.queue.popFront()) |item| {
            if (item.consumer == c) {
                inbox.gpa.free(item.frame);
            } else {
                kept.pushBack(inbox.gpa, item) catch inbox.gpa.free(item.frame);
            }
        }
        inbox.queue.deinit(inbox.gpa);
        inbox.queue = kept;
    }

    /// Returns the next message from any consumer of this inbox, waiting
    /// up to `timeout`. Returns `null` on timeout. Once the connection or
    /// stream fails, the queued messages are returned first, then the error.
    /// Only one task may call `next` at a time.
    pub fn next(inbox: *Inbox, timeout: Io.Timeout) Error!?Delivery {
        const deadline = timeout.toDeadline(inbox.io);
        while (true) {
            if (inbox.current) |*cur| {
                if (try inbox.nextRecord(cur)) |delivery| {
                    if (delivery.offset < cur.item.consumer.start_offset.load(.monotonic)) continue;
                    if (cur.item.consumer.options.filter) |f| {
                        if (f.post_filter) |pf| {
                            if (!pf.callback(pf.context, &delivery)) continue;
                        }
                    }
                    _ = cur.item.consumer.messages_consumed.fetchAdd(1, .monotonic);
                    return delivery;
                }
                const consumer = cur.item.consumer;
                inbox.gpa.free(cur.item.frame);
                inbox.current = null;
                consumer.chunkDone();
            }
            const item = (try inbox.waitItem(deadline)) orelse return null;
            inbox.current = inbox.startChunk(item) catch |err| {
                inbox.gpa.free(item.frame);
                item.consumer.chunkDone();
                return err;
            };
        }
    }

    fn waitItem(inbox: *Inbox, deadline: Io.Timeout) Error!?Item {
        try inbox.mutex.lock(inbox.io);
        defer inbox.mutex.unlock(inbox.io);
        while (true) {
            if (inbox.queue.popFront()) |item| return item;
            if (inbox.failure) |err| return err;
            if (inbox.sources == 0) return error.ConsumerClosed;
            inbox.cond.waitTimeout(inbox.io, &inbox.mutex, deadline) catch |err| switch (err) {
                error.Timeout => return null,
                error.Canceled => return error.Canceled,
            };
        }
    }

    fn startChunk(inbox: *Inbox, item: Item) Error!?Current {
        _ = inbox;
        const ch: chunk.Chunk = try .parse(item.frame[item.chunk_start..]);
        if (item.consumer.options.check_crc and !ch.verifyCrc()) return error.MalformedChunk;
        if (ch.header.chunk_type != .user) {
            return .{ .item = item, .chunk = ch, .entries = .{ .data = &.{}, .remaining = 0 }, .next_offset = ch.header.chunk_id };
        }
        return .{ .item = item, .chunk = ch, .entries = ch.entries(), .next_offset = ch.header.chunk_id };
    }

    fn nextRecord(inbox: *Inbox, cur: *Current) Error!?Delivery {
        while (true) {
            if (cur.records) |*records| {
                if (try records.next()) |data| return deliveryOf(cur, data);
                cur.records = null;
            }
            const entry = try cur.entries.next() orelse return null;
            switch (entry) {
                .simple => |data| return deliveryOf(cur, data),
                .sub_batch => |batch| {
                    const body = try chunk.decompress(batch, &inbox.decompressed);
                    cur.records = chunk.SubBatch.records(body, batch.record_count);
                },
            }
        }
    }

    fn deliveryOf(cur: *Current, data: []const u8) Delivery {
        const offset = cur.next_offset;
        cur.next_offset += 1;
        return .{
            .consumer = cur.item.consumer,
            .offset = offset,
            .chunk_id = cur.chunk.header.chunk_id,
            .chunk_timestamp = cur.chunk.header.timestamp,
            .committed_chunk_id = cur.item.committed,
            .data = data,
        };
    }
};
