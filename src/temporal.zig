//! Temporal SDK for Zig, built on the Temporal Rust core (sdk-core).

pub const client = @import("client.zig");
pub const worker = @import("worker.zig");
pub const workflow = @import("workflow.zig");
pub const activity = @import("activity.zig");
pub const nexus = @import("nexus.zig");
pub const converter = @import("converter.zig");
pub const failure = @import("failure.zig");
pub const log = @import("log.zig");
pub const history = @import("history.zig");
pub const Failure = failure.Failure;
pub const testing = @import("testing.zig");

/// Generated protobuf types for the Temporal API and the sdk-core interface.
pub const proto = @import("proto.zig");

const bridge = @import("bridge.zig");

/// SDK version reported to the server. Keep in sync with build.zig.zon.
pub const version = "0.0.0";

test {
    @import("std").testing.refAllDecls(@This());
    _ = bridge;
    _ = @import("proto_test.zig");
}
