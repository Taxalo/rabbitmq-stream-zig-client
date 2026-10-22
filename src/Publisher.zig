//! Publishes messages to a stream.
//!
//! Messages are accumulated in batches and sent in a single `publish` frame
//! when the batch is full, on `flush`, or periodically when `flush_interval`
//! is set. The broker confirms each publishing id asynchronously; at most
//! `max_in_flight` messages may be unconfirmed at once.
//!
//! Giving the publisher a `name` enables deduplication: the broker drops
//! messages whose publishing id is not greater than the last one it stored
//! for that name, and the publisher resumes from the stored id.

const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;
const assert = std.debug.assert;

const protocol = @import("protocol.zig");
const ResponseCode = protocol.ResponseCode;
const amqp = @import("amqp.zig");
const chunk = @import("chunk.zig");
const Client = @import("Client.zig");

const log = std.log.scoped(.rabbitmq_stream);

const Publisher = @This();

gpa: Allocator,
io: Io,
/// `null` once the client has been destroyed.
client: ?*Client,
owns_client: bool = false,
id: u8 = 0,
stream: []const u8,
name: ?[]const u8,
options: Options,

/// Guards the batch and the publishing id sequence.
mutex: Io.Mutex = .init,
batch: std.ArrayList(u8) = .empty,
batch_count: u32 = 0,
batch_version: u16 = 1,
sub_batch: std.ArrayList(u8) = .empty,
sub_batch_count: u16 = 0,
sub_batch_last_id: u64 = 0,
compressed: Io.Writer.Allocating,
scratch: std.ArrayList(u8) = .empty,
next_publishing_id: u64 = 1,

/// Guards the confirmation state below.
confirm_mutex: Io.Mutex = .init,
confirm_cond: Io.Condition = .init,
in_flight: u64 = 0,
/// Number of messages behind the publishing id of each sub-entry batch.
sub_batch_sizes: std.AutoHashMapUnmanaged(u64, u32) = .empty,
confirmed_count: u64 = 0,
errored_count: u64 = 0,
close_error: ?Error = null,

flusher: ?Io.Future(void) = null,

pub const Error = Client.Error || error{
    PublisherClosed,
    StreamUnavailable,
    MessageTooLarge,
    FilteringNotSupported,
    FilteringWithSubEntryBatching,
    UnsupportedCompression,
    NoPublisherName,
};

pub const Options = struct {
    /// Publisher reference; enables deduplication. At most 256 bytes.
    name: ?[]const u8 = null,
    /// Maximum number of entries per `publish` frame.
    batch_size: u32 = 100,
    /// Maximum number of unconfirmed messages; `send` blocks above it.
    max_in_flight: u32 = 10_000,
    /// When greater than 1, groups this many messages into one sub-entry,
    /// optionally compressed. Not compatible with filter values.
    sub_entry_size: u16 = 1,
    /// Compression of sub-entries. `gzip` or `none`.
    compression: chunk.Compression = .none,
    /// Flushes pending messages periodically from a background task.
    /// With `null`, messages are sent when a batch is full or on `flush`.
    flush_interval: ?Io.Duration = .fromMilliseconds(100),
    /// Receives the outcome of every publishing id.
    confirm_listener: ?ConfirmListener = null,
};

pub const Confirmation = struct {
    publishing_id: u64,
    /// `ok` when the message was stored, the error code otherwise.
    code: ResponseCode,
    /// Number of messages covered: more than 1 for sub-entry batches.
    message_count: u32,
};

/// Called from the client's reader task for each confirmed or rejected
/// publishing id. The callback must not block and must not call `send`,
/// `flush` or other methods that wait on the connection.
pub const ConfirmListener = struct {
    context: ?*anyopaque = null,
    callback: *const fn (context: ?*anyopaque, publisher: *Publisher, confirmation: Confirmation) void,
};

pub const SendOptions = struct {
    /// Explicit publishing id; must be greater than the previous one.
    /// Defaults to the next id in sequence.
    publishing_id: ?u64 = null,
    /// Filter value for stream filtering (RabbitMQ 3.13 and later).
    filter_value: ?[]const u8 = null,
};

/// Declares a publisher on `client`. Close it with `close` before closing
/// the client.
pub fn init(client: *Client, stream: []const u8, options: Options) Error!*Publisher {
    if (options.compression != .none and options.compression != .gzip) return error.UnsupportedCompression;
    if (options.name) |n| assert(n.len <= protocol.max_reference_len);
    const gpa = client.gpa;
    const p = try gpa.create(Publisher);
    errdefer gpa.destroy(p);
    const stream_copy = try gpa.dupe(u8, stream);
    errdefer gpa.free(stream_copy);
    const name_copy = if (options.name) |n| try gpa.dupe(u8, n) else null;
    errdefer if (name_copy) |n| gpa.free(n);

    p.* = .{
        .gpa = gpa,
        .io = client.io,
        .client = client,
        .stream = stream_copy,
        .name = name_copy,
        .options = options,
        .compressed = .init(gpa),
    };
    p.options.name = name_copy;
    p.options.batch_size = @max(1, options.batch_size);
    p.options.max_in_flight = @max(1, options.max_in_flight);
    errdefer p.compressed.deinit();

    p.id = try client.registerPublisher(p);
    errdefer client.unregisterPublisher(p.id);
    try client.declarePublisher(p.id, p.name, p.stream);
    if (p.name) |name| {
        p.next_publishing_id = (try client.queryPublisherSequence(name, p.stream)) + 1;
    }
    if (options.flush_interval) |interval| {
        p.flusher = p.io.concurrent(flushLoop, .{ p, interval }) catch null;
    }
    return p;
}

/// Flushes pending messages, deletes the publisher on the broker and frees
/// it. Unconfirmed messages are not waited for; call `waitForConfirms`
/// first if needed.
pub fn close(p: *Publisher) void {
    if (p.flusher) |*f| f.cancel(p.io);
    if (p.client) |c| {
        if (c.isOpen()) {
            p.flush() catch {};
            c.deletePublisher(p.id) catch |err| log.debug("delete publisher failed: {t}", .{err});
        }
        c.unregisterPublisher(p.id);
    }
    const owned_client = if (p.owns_client) p.client else null;
    p.batch.deinit(p.gpa);
    p.sub_batch.deinit(p.gpa);
    p.scratch.deinit(p.gpa);
    p.compressed.deinit();
    p.sub_batch_sizes.deinit(p.gpa);
    p.gpa.free(p.stream);
    if (p.name) |n| p.gpa.free(n);
    p.gpa.destroy(p);
    if (owned_client) |c| c.close();
}

/// Encodes `message` as AMQP 1.0 and publishes it. Returns its publishing id.
pub fn send(p: *Publisher, message: *const amqp.Message, options: SendOptions) Error!u64 {
    try p.mutex.lock(p.io);
    defer p.mutex.unlock(p.io);
    p.scratch.clearRetainingCapacity();
    const len = message.encodedLen();
    try p.scratch.ensureTotalCapacity(p.gpa, len);
    var w: Io.Writer = .fixed(p.scratch.unusedCapacitySlice()[0..len]);
    message.encode(&w) catch unreachable;
    p.scratch.items.len = len;
    return p.sendLocked(p.scratch.items, options);
}

/// Publishes already encoded bytes. Other clients expect AMQP 1.0 data.
pub fn sendRaw(p: *Publisher, data: []const u8, options: SendOptions) Error!u64 {
    try p.mutex.lock(p.io);
    defer p.mutex.unlock(p.io);
    return p.sendLocked(data, options);
}

fn sendLocked(p: *Publisher, data: []const u8, options: SendOptions) Error!u64 {
    try p.checkOpen();
    const client = p.client.?;
    if (options.filter_value != null) {
        if (!client.supportsFiltering()) return error.FilteringNotSupported;
        if (p.options.sub_entry_size > 1) return error.FilteringWithSubEntryBatching;
    }
    const entry_len = if (p.options.sub_entry_size > 1)
        8 + 11 + 4 + data.len
    else
        8 + 4 + data.len + filterLen(options.filter_value, 2);
    if (entry_len > p.maxBatchBytes()) return error.MessageTooLarge;

    try p.acquireInFlight(1);
    errdefer p.releaseInFlight(1);

    const publishing_id = options.publishing_id orelse p.next_publishing_id;
    p.next_publishing_id = @max(p.next_publishing_id, publishing_id + 1);

    if (p.options.sub_entry_size > 1) {
        try p.appendToSubBatch(publishing_id, data);
    } else {
        try p.appendEntry(publishing_id, options.filter_value, data);
    }
    return publishing_id;
}

fn filterLen(filter_value: ?[]const u8, version: u16) usize {
    if (filter_value) |f| return protocol.stringSize(f);
    return if (version >= 2) 2 else 0;
}

/// Bytes available for entries in one `publish` frame.
fn maxBatchBytes(p: *const Publisher) usize {
    const frame_max = p.client.?.frame_max;
    if (frame_max == 0) return std.math.maxInt(u32);
    return frame_max - (4 + 2 + 2 + 1 + 4);
}

fn appendEntry(p: *Publisher, publishing_id: u64, filter_value: ?[]const u8, data: []const u8) Error!void {
    if (filter_value != null and p.batch_version == 1) {
        try p.sendBatchLocked();
        p.batch_version = 2;
    }
    const len = 8 + filterLen(filter_value, p.batch_version) + 4 + data.len;
    if (p.batch.items.len + len > p.maxBatchBytes()) try p.sendBatchLocked();
    if (filter_value != null) p.batch_version = 2;

    try p.batch.ensureUnusedCapacity(p.gpa, len);
    var w: Io.Writer = .fixed(p.batch.unusedCapacitySlice());
    var e: protocol.Encoder = .{ .w = &w };
    e.int(u64, publishing_id) catch unreachable;
    if (p.batch_version >= 2) e.string(filter_value) catch unreachable;
    e.int(u32, @intCast(data.len)) catch unreachable;
    e.raw(data) catch unreachable;
    p.batch.items.len += len;
    p.batch_count += 1;
    if (p.batch_count >= p.options.batch_size) try p.sendBatchLocked();
}

fn appendToSubBatch(p: *Publisher, publishing_id: u64, data: []const u8) Error!void {
    // Header of the sub-entry plus the records accumulated so far.
    const header_len = 8 + 11;
    if (header_len + p.sub_batch.items.len + 4 + data.len > p.maxBatchBytes()) try p.finishSubBatchLocked();
    try p.sub_batch.ensureUnusedCapacity(p.gpa, 4 + data.len);
    var w: Io.Writer = .fixed(p.sub_batch.unusedCapacitySlice());
    w.writeInt(u32, @intCast(data.len), .big) catch unreachable;
    w.writeAll(data) catch unreachable;
    p.sub_batch.items.len += 4 + data.len;
    p.sub_batch_count += 1;
    p.sub_batch_last_id = publishing_id;
    if (p.sub_batch_count >= p.options.sub_entry_size) try p.finishSubBatchLocked();
}

/// Moves the accumulated sub-entry records into the batch as one entry.
fn finishSubBatchLocked(p: *Publisher) Error!void {
    if (p.sub_batch_count == 0) return;
    const body = chunk.compress(p.gpa, p.options.compression, p.sub_batch.items, &p.compressed) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return error.UnsupportedCompression,
    };
    const len = 8 + 11 + body.len;
    if (p.batch.items.len + len > p.maxBatchBytes()) try p.sendBatchLocked();
    if (len > p.maxBatchBytes()) return error.MessageTooLarge;

    {
        p.confirm_mutex.lockUncancelable(p.io);
        defer p.confirm_mutex.unlock(p.io);
        try p.sub_batch_sizes.put(p.gpa, p.sub_batch_last_id, p.sub_batch_count);
    }

    try p.batch.ensureUnusedCapacity(p.gpa, len);
    var w: Io.Writer = .fixed(p.batch.unusedCapacitySlice());
    w.writeInt(u64, p.sub_batch_last_id, .big) catch unreachable;
    w.writeByte(0x80 | (@as(u8, @backingInt(p.options.compression)) << 4)) catch unreachable;
    w.writeInt(u16, p.sub_batch_count, .big) catch unreachable;
    w.writeInt(u32, @intCast(p.sub_batch.items.len), .big) catch unreachable;
    w.writeInt(u32, @intCast(body.len), .big) catch unreachable;
    w.writeAll(body) catch unreachable;
    p.batch.items.len += len;
    p.batch_count += 1;
    p.sub_batch.clearRetainingCapacity();
    p.sub_batch_count = 0;
    if (p.batch_count >= p.options.batch_size) try p.sendBatchLocked();
}

fn sendBatchLocked(p: *Publisher) Error!void {
    if (p.batch_count == 0) return;
    defer {
        p.batch.clearRetainingCapacity();
        p.batch_count = 0;
        p.batch_version = 1;
    }
    try p.checkOpen();
    try p.client.?.sendPublish(p.id, p.batch_version, p.batch_count, p.batch.items);
}

/// Sends all pending messages, including an incomplete sub-entry batch.
pub fn flush(p: *Publisher) Error!void {
    try p.mutex.lock(p.io);
    defer p.mutex.unlock(p.io);
    try p.finishSubBatchLocked();
    try p.sendBatchLocked();
}

fn flushLoop(p: *Publisher, interval: Io.Duration) void {
    while (true) {
        p.io.sleep(interval, .awake) catch return;
        p.flush() catch |err| switch (err) {
            error.Canceled => return,
            else => {},
        };
    }
}

/// Flushes, then waits until every message sent so far is confirmed or
/// rejected.
pub fn waitForConfirms(p: *Publisher, timeout: Io.Timeout) Error!void {
    try p.flush();
    const deadline = timeout.toDeadline(p.io);
    try p.confirm_mutex.lock(p.io);
    defer p.confirm_mutex.unlock(p.io);
    while (p.in_flight > 0) {
        if (p.close_error) |err| return err;
        p.confirm_cond.waitTimeout(p.io, &p.confirm_mutex, deadline) catch |err| return switch (err) {
            error.Timeout => error.Timeout,
            error.Canceled => error.Canceled,
        };
    }
}

pub const Stats = struct {
    confirmed: u64,
    errored: u64,
    in_flight: u64,
};

pub fn stats(p: *Publisher) Stats {
    p.confirm_mutex.lockUncancelable(p.io);
    defer p.confirm_mutex.unlock(p.io);
    return .{ .confirmed = p.confirmed_count, .errored = p.errored_count, .in_flight = p.in_flight };
}

/// Returns the last publishing id the broker stored for this publisher's
/// name. Requires a named publisher.
pub fn lastPublishingId(p: *Publisher) Error!u64 {
    const name = p.name orelse return error.NoPublisherName;
    try p.checkOpen();
    return p.client.?.queryPublisherSequence(name, p.stream);
}

fn checkOpen(p: *Publisher) Error!void {
    p.confirm_mutex.lockUncancelable(p.io);
    defer p.confirm_mutex.unlock(p.io);
    if (p.close_error) |err| return err;
    if (p.client == null) return error.PublisherClosed;
}

fn acquireInFlight(p: *Publisher, n: u32) Error!void {
    try p.confirm_mutex.lock(p.io);
    defer p.confirm_mutex.unlock(p.io);
    while (p.in_flight + n > p.options.max_in_flight and p.in_flight > 0) {
        if (p.close_error) |err| return err;
        // Pending messages must reach the broker to be confirmed.
        if (p.batch_count > 0 or p.sub_batch_count > 0) {
            p.confirm_mutex.unlock(p.io);
            defer p.confirm_mutex.lockUncancelable(p.io);
            p.finishSubBatchLocked() catch {};
            p.sendBatchLocked() catch {};
            continue;
        }
        try p.confirm_cond.wait(p.io, &p.confirm_mutex);
    }
    if (p.close_error) |err| return err;
    p.in_flight += n;
}

fn releaseInFlight(p: *Publisher, n: u64) void {
    p.confirm_mutex.lockUncancelable(p.io);
    defer p.confirm_mutex.unlock(p.io);
    p.in_flight -|= n;
    p.confirm_cond.broadcast(p.io);
}

// Called by the client's reader task.

pub fn handleConfirm(p: *Publisher, ids: []const u8) void {
    var i: usize = 0;
    while (i + 8 <= ids.len) : (i += 8) {
        const publishing_id = std.mem.readInt(u64, ids[i..][0..8], .big);
        p.complete(publishing_id, .ok);
    }
}

pub fn handleError(p: *Publisher, errors: []const u8) void {
    var i: usize = 0;
    while (i + 10 <= errors.len) : (i += 10) {
        const publishing_id = std.mem.readInt(u64, errors[i..][0..8], .big);
        const code: ResponseCode = .fromInt(std.mem.readInt(u16, errors[i + 8 ..][0..2], .big));
        log.debug("message {d} rejected: {f}", .{ publishing_id, code });
        p.complete(publishing_id, code);
    }
}

fn complete(p: *Publisher, publishing_id: u64, code: ResponseCode) void {
    const count = blk: {
        p.confirm_mutex.lockUncancelable(p.io);
        defer p.confirm_mutex.unlock(p.io);
        const n: u32 = if (p.sub_batch_sizes.fetchRemove(publishing_id)) |kv| kv.value else 1;
        p.in_flight -|= n;
        if (code == .ok) p.confirmed_count += n else p.errored_count += n;
        p.confirm_cond.broadcast(p.io);
        break :blk n;
    };
    if (p.options.confirm_listener) |l| l.callback(l.context, p, .{
        .publishing_id = publishing_id,
        .code = code,
        .message_count = count,
    });
}

pub fn handleStreamUnavailable(p: *Publisher, code: ResponseCode) void {
    _ = code;
    p.setCloseError(error.StreamUnavailable);
}

pub fn handleConnectionClosed(p: *Publisher) void {
    p.setCloseError(error.ConnectionClosed);
}

/// Called when the client is destroyed while the publisher is open.
pub fn detachClient(p: *Publisher) void {
    p.setCloseError(error.ConnectionClosed);
    p.client = null;
    p.owns_client = false;
}

fn setCloseError(p: *Publisher, err: Error) void {
    p.confirm_mutex.lockUncancelable(p.io);
    defer p.confirm_mutex.unlock(p.io);
    if (p.close_error == null) p.close_error = err;
    p.confirm_cond.broadcast(p.io);
}
