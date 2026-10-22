//! Consumes all partitions of a super stream through one inbox.
//!
//! With `single_active_consumer` and a `name`, instances of the application
//! sharing the name split the partitions between them: for each partition
//! one instance is active at a time.

const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;

const Client = @import("Client.zig");
const Consumer = @import("Consumer.zig");
const Environment = @import("Environment.zig");

const SuperStreamConsumer = @This();

gpa: Allocator,
name: []const u8,
partitions: [][]u8,
consumers: []*Consumer,
inbox: *Consumer.Inbox,

pub const Error = Environment.Error || Consumer.Error;

/// `options.super_stream` is set to `super_stream` for every partition.
pub fn init(env: *Environment, super_stream: []const u8, options: Consumer.Options) Error!*SuperStreamConsumer {
    const gpa = env.gpa;
    const parts = try env.partitions(super_stream);
    errdefer Client.freeStrings(gpa, parts);
    if (parts.len == 0) return error.StreamDoesNotExist;

    const inbox = try Consumer.Inbox.create(gpa, env.io);
    errdefer inbox.destroy();
    const consumers = try gpa.alloc(*Consumer, parts.len);
    errdefer gpa.free(consumers);

    var opts = options;
    opts.super_stream = super_stream;
    var n: usize = 0;
    errdefer for (consumers[0..n]) |c| c.close();
    while (n < parts.len) : (n += 1) {
        consumers[n] = try env.createConsumerInto(parts[n], opts, inbox);
    }

    const sc = try gpa.create(SuperStreamConsumer);
    errdefer gpa.destroy(sc);
    sc.* = .{
        .gpa = gpa,
        .name = try gpa.dupe(u8, super_stream),
        .partitions = parts,
        .consumers = consumers,
        .inbox = inbox,
    };
    return sc;
}

pub fn close(sc: *SuperStreamConsumer) void {
    for (sc.consumers) |c| c.close();
    sc.inbox.destroy();
    sc.gpa.free(sc.consumers);
    Client.freeStrings(sc.gpa, sc.partitions);
    sc.gpa.free(sc.name);
    sc.gpa.destroy(sc);
}

/// Returns the next message from any partition. `Delivery.consumer` tells
/// which partition it came from, for offset tracking.
pub fn next(sc: *SuperStreamConsumer, timeout: Io.Timeout) Error!?Consumer.Delivery {
    return sc.inbox.next(timeout);
}
