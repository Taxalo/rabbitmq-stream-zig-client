//! Publishes to a super stream: a logical stream made of partition
//! streams. Each message is routed to a partition by its routing key, and
//! each partition gets its own `Publisher`, created on first use.

const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;

const amqp = @import("amqp.zig");
const Client = @import("Client.zig");
const Publisher = @import("Publisher.zig");
const Environment = @import("Environment.zig");

const SuperStreamPublisher = @This();

env: *Environment,
gpa: Allocator,
name: []const u8,
/// Partition streams, in the order returned by the broker.
partitions: [][]u8,
publishers: []?*Publisher,
options: Options,
mutex: Io.Mutex = .init,
/// Routing key to partition indexes, for `Routing.key`.
routes: std.StringHashMapUnmanaged([]u32) = .empty,

pub const Error = Environment.Error || Publisher.Error || error{NoRoute};

/// The seed of the MurmurHash3 hash used by the RabbitMQ stream clients,
/// so that all clients route a key to the same partition.
pub const hash_seed: u32 = 104729;

pub const Routing = enum {
    /// MurmurHash3 (32 bits) of the routing key modulo the number of
    /// partitions. The partitions must be bound in order.
    hash,
    /// Asks the broker which partitions the routing key is bound to.
    key,
};

pub const Options = struct {
    routing: Routing = .hash,
    /// Options for each partition publisher. A `name` is shared by all
    /// partitions; deduplication works per partition.
    publisher: Publisher.Options = .{},
};

pub fn init(env: *Environment, name: []const u8, options: Options) Error!*SuperStreamPublisher {
    const gpa = env.gpa;
    const parts = try env.partitions(name);
    errdefer Client.freeStrings(gpa, parts);
    if (parts.len == 0) return error.StreamDoesNotExist;
    const publishers = try gpa.alloc(?*Publisher, parts.len);
    errdefer gpa.free(publishers);
    @memset(publishers, null);
    const sp = try gpa.create(SuperStreamPublisher);
    errdefer gpa.destroy(sp);
    sp.* = .{
        .env = env,
        .gpa = gpa,
        .name = try gpa.dupe(u8, name),
        .partitions = parts,
        .publishers = publishers,
        .options = options,
    };
    return sp;
}

pub fn close(sp: *SuperStreamPublisher) void {
    for (sp.publishers) |p| if (p) |pub_| pub_.close();
    var it = sp.routes.iterator();
    while (it.next()) |e| {
        sp.gpa.free(e.key_ptr.*);
        sp.gpa.free(e.value_ptr.*);
    }
    sp.routes.deinit(sp.gpa);
    sp.gpa.free(sp.publishers);
    Client.freeStrings(sp.gpa, sp.partitions);
    sp.gpa.free(sp.name);
    sp.gpa.destroy(sp);
}

/// Returns the partition index for a routing key with `Routing.hash`.
pub fn hashPartition(routing_key: []const u8, partition_count: usize) usize {
    const h = std.hash.Murmur3_32.hashWithSeed(routing_key, hash_seed);
    return h % partition_count;
}

/// Publishes `message` to the partitions `routing_key` routes to. Returns
/// the publishing id in the first partition.
pub fn send(sp: *SuperStreamPublisher, routing_key: []const u8, message: *const amqp.Message, options: Publisher.SendOptions) Error!u64 {
    var buf: [1]u32 = undefined;
    const indexes = try sp.resolve(routing_key, &buf);
    var first: ?u64 = null;
    for (indexes) |i| {
        const id = try (try sp.partitionPublisher(i)).send(message, options);
        if (first == null) first = id;
    }
    return first.?;
}

/// Like `send` with already encoded bytes.
pub fn sendRaw(sp: *SuperStreamPublisher, routing_key: []const u8, data: []const u8, options: Publisher.SendOptions) Error!u64 {
    var buf: [1]u32 = undefined;
    const indexes = try sp.resolve(routing_key, &buf);
    var first: ?u64 = null;
    for (indexes) |i| {
        const id = try (try sp.partitionPublisher(i)).sendRaw(data, options);
        if (first == null) first = id;
    }
    return first.?;
}

fn resolve(sp: *SuperStreamPublisher, routing_key: []const u8, buf: *[1]u32) Error![]const u32 {
    switch (sp.options.routing) {
        .hash => {
            buf[0] = @intCast(hashPartition(routing_key, sp.partitions.len));
            return buf;
        },
        .key => {
            try sp.mutex.lock(sp.env.io);
            defer sp.mutex.unlock(sp.env.io);
            if (sp.routes.get(routing_key)) |cached| return cached;
            const streams = try sp.env.route(routing_key, sp.name);
            defer Client.freeStrings(sp.gpa, streams);
            if (streams.len == 0) return error.NoRoute;
            const indexes = try sp.gpa.alloc(u32, streams.len);
            errdefer sp.gpa.free(indexes);
            for (streams, indexes) |s, *index| {
                index.* = for (sp.partitions, 0..) |p, i| {
                    if (std.mem.eql(u8, p, s)) break @intCast(i);
                } else return error.NoRoute;
            }
            const key = try sp.gpa.dupe(u8, routing_key);
            errdefer sp.gpa.free(key);
            try sp.routes.put(sp.gpa, key, indexes);
            return indexes;
        },
    }
}

fn partitionPublisher(sp: *SuperStreamPublisher, index: usize) Error!*Publisher {
    try sp.mutex.lock(sp.env.io);
    defer sp.mutex.unlock(sp.env.io);
    if (sp.publishers[index]) |p| return p;
    const p = try sp.env.createPublisher(sp.partitions[index], sp.options.publisher);
    sp.publishers[index] = p;
    return p;
}

pub fn flush(sp: *SuperStreamPublisher) Error!void {
    for (sp.publishers) |p| if (p) |pub_| try pub_.flush();
}

pub fn waitForConfirms(sp: *SuperStreamPublisher, timeout: Io.Timeout) Error!void {
    const deadline = timeout.toDeadline(sp.env.io);
    for (sp.publishers) |p| if (p) |pub_| try pub_.waitForConfirms(deadline);
}

test hashPartition {
    // `std.hash.Murmur3_32` is MurmurHash3 x86 32-bit, as used by the
    // Java, .NET and Go stream clients.
    try std.testing.expectEqual(@as(u32, 0x248bfa47), std.hash.Murmur3_32.hashWithSeed("hello", 0));
    try std.testing.expect(hashPartition("anything", 1) == 0);
    try std.testing.expect(hashPartition("hello", 3) < 3);
}
