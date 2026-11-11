//! Temporal SDK for Zig, built on the Temporal Rust core (sdk-core).

pub const client = @import("client.zig");
pub const worker = @import("worker.zig");
pub const workflow = @import("workflow.zig");
pub const activity = @import("activity.zig");
pub const nexus = @import("nexus.zig");
pub const converter = @import("converter.zig");
pub const testing = @import("testing.zig");

const bridge = @import("bridge.zig");

/// SDK version reported to the server. Keep in sync with build.zig.zon.
pub const version = "0.0.0";

test {
    @import("std").testing.refAllDecls(@This());
    _ = bridge;
}
