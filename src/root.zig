//! A RabbitMQ Stream protocol client.
//!
//! - `Environment` is the usual entry point: it manages streams and creates
//!   publishers and consumers connected to the right nodes.
//! - `Client` is a single connection exposing every protocol command.
//! - `Publisher` and `Consumer` work on a `Client`.
//! - `amqp` encodes and decodes AMQP 1.0 messages.

pub const version = "0.1.0";

pub const protocol = @import("protocol.zig");
pub const amqp = @import("amqp.zig");
pub const chunk = @import("chunk.zig");

pub const Client = @import("Client.zig");
pub const Publisher = @import("Publisher.zig");
pub const Consumer = @import("Consumer.zig");
pub const Environment = @import("Environment.zig");
pub const SuperStreamPublisher = @import("SuperStreamPublisher.zig");
pub const SuperStreamConsumer = @import("SuperStreamConsumer.zig");

pub const OffsetSpec = protocol.OffsetSpec;
pub const ResponseCode = protocol.ResponseCode;
pub const Property = protocol.Property;
pub const Message = amqp.Message;
pub const Delivery = Consumer.Delivery;
pub const StreamOptions = Client.StreamOptions;

test {
    @import("std").testing.refAllDecls(@This());
}
