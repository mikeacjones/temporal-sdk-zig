//! Temporal SDK for Zig, built on the Temporal Rust core (sdk-core).

pub const client = @import("client.zig");
pub const worker = @import("worker.zig");
pub const workflow = @import("workflow.zig");
pub const activity = @import("activity.zig");
pub const nexus = @import("nexus.zig");
pub const converter = @import("converter.zig");
pub const propagation = @import("propagation.zig");
pub const interceptor = @import("interceptor.zig");
pub const search_attributes = @import("search_attributes.zig");
pub const failure = @import("failure.zig");
pub const log = @import("log.zig");
pub const history = @import("history.zig");
/// ProtoJSON of generated protobuf messages, as Temporal's CLI, Web UI and codec servers
/// write and read it.
pub const protojson = @import("protojson.zig");
pub const metrics = @import("metrics.zig");
pub const schedule = @import("schedule.zig");
pub const deployment = @import("deployment.zig");
pub const Failure = failure.Failure;
pub const Header = converter.Header;
pub const testing = @import("testing.zig");

/// Generated protobuf types for the Temporal API and the sdk-core interface.
pub const proto = @import("proto.zig");

const bridge = @import("bridge.zig");

/// SDK version reported to the server and recorded as `sdkVersion` in workflow histories:
/// build.zig.zon's `.version`, which build.zig passes in as a build option.
pub const version: []const u8 = @import("build_options").version;

test "version is build.zig.zon's .version" {
    const std = @import("std");
    try std.testing.expectEqualStrings(@import("build_options").version, version);
    _ = try std.SemanticVersion.parse(version);
    try std.testing.expect(!std.mem.eql(u8, "0.0.0", version));
}

test {
    @import("std").testing.refAllDecls(@This());
    _ = bridge;
    _ = @import("proto_test.zig");
    _ = @import("propagation_test.zig");
    _ = @import("interceptor_test.zig");
    _ = @import("metrics_test.zig");
    _ = @import("client_tls_test.zig");
    _ = @import("client_namespace_test.zig");
    _ = @import("client_rpc_options_test.zig");
    _ = @import("client_service_test.zig");
    _ = @import("client_test.zig");
    _ = @import("nexus_test.zig");
    _ = @import("failure_test.zig");
    _ = @import("converter_context_test.zig");
    _ = @import("failure_converter_test.zig");
    _ = @import("workflow_api_test.zig");
    _ = @import("payload_limits_test.zig");
    _ = @import("workflow/stack_trace_test.zig");
    _ = @import("workflow/call_sites.zig");
}
