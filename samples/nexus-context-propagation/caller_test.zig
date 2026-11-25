//! Tests of the caller workflow against the service's handler, served by the test
//! environment's worker through an endpoint that routes to it: a value propagated to the
//! caller workflow reaches the workflow the say-hello operation starts.

const std = @import("std");
const temporal = @import("temporal");
const context = @import("context.zig");
const service = @import("service.zig");
const handler = @import("handler.zig");
const caller = @import("caller.zig");

const testing = std.testing;
const WorkflowEnvironment = temporal.testing.WorkflowEnvironment;

/// An environment whose client and worker use the sample's propagators, with the caller and
/// the handler registered and `caller.endpoint` routed to its own namespace and task queue.
fn startEnvironment() !*WorkflowEnvironment {
    const env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{ .propagators = &context.propagators });
    errdefer env.deinit();
    try caller.register(&env.worker);
    try handler.register(&env.worker);
    try env.createNexusEndpoint(caller.endpoint, .{});
    return env;
}

test "a value propagated to the caller workflow reaches the workflow the say-hello operation started" {
    const env = try startEnvironment();
    defer env.deinit();

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var values: temporal.propagation.Values = .init(a);
    try values.put(context.propagate_key, .{ .key = "caller-id", .value = "samples-zig" });

    const result = try env.executeWorkflowWithOptions(caller.HelloCallerWorkflow, .{ .task_queue = "", .context = &values }, .{ "Nexus", .es }, a, null);
    try testing.expectEqualStrings("¡Hola! Nexus, caller-id: samples-zig 👋", result);

    // The handler workflow, under its business ID, produced the greeting itself.
    var greeted = try env.client.getWorkflow(service.HelloOutput, "hello-es-nexus", "");
    defer greeted.deinit();
    try testing.expectEqualStrings("¡Hola! Nexus, caller-id: samples-zig 👋", (try greeted.get(a)).message);
}

test "without a propagated value the greeting has no context" {
    const env = try startEnvironment();
    defer env.deinit();

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const result = try env.executeWorkflow(caller.HelloCallerWorkflow, .{ "Nexus", .es }, arena.allocator(), null);
    try testing.expectEqualStrings("¡Hola! Nexus 👋", result);
}
