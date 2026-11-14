//! Tests of the caller workflows against the service's handlers, served by the test
//! environment's worker through an endpoint that routes to it.

const std = @import("std");
const temporal = @import("temporal");
const service = @import("service.zig");
const handler = @import("handler.zig");
const caller = @import("caller.zig");

const testing = std.testing;
const WorkflowEnvironment = temporal.testing.WorkflowEnvironment;

/// An environment whose worker runs the caller workflows, with `caller.endpoint` routed to
/// its own namespace and task queue.
fn startEnvironment() !*WorkflowEnvironment {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    errdefer env.deinit();
    try caller.register(&env.worker);
    try env.createNexusEndpoint(caller.endpoint, .{});
    return env;
}

test "the echo caller workflow returns what the echo operation returned" {
    const env = try startEnvironment();
    defer env.deinit();
    try env.registerNexusService(handler.HelloServiceHandler);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const result = try env.executeWorkflow(caller.EchoCallerWorkflow, .{"hey"}, arena.allocator(), null);
    try testing.expectEqualStrings("hey", result);
}

test "the hello caller workflow returns the greeting of the workflow the say-hello operation started" {
    const env = try startEnvironment();
    defer env.deinit();
    try handler.register(&env.worker);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const result = try env.executeWorkflow(caller.HelloCallerWorkflow, .{ "Nexus", .de }, a, null);
    try testing.expectEqualStrings("Hallo Nexus 👋", result);

    // The operation ran the handler workflow under its business ID.
    var greeted = try env.client.getWorkflow(service.HelloOutput, "hello-de-nexus", "");
    defer greeted.deinit();
    try testing.expectEqualStrings("Hallo Nexus 👋", (try greeted.get(a)).message);
}

fn fakeHelloHandlerWorkflow(ctx: *temporal.workflow.Context, input: service.HelloInput) !service.HelloOutput {
    return .{ .message = try std.fmt.allocPrint(ctx.allocator(), "fake:{t}:{s}", .{ input.language, input.name }) };
}

test "the hello caller workflow with a test double for the handler workflow" {
    const env = try startEnvironment();
    defer env.deinit();
    try env.registerNexusService(handler.HelloServiceHandler);
    try env.registerWorkflow(temporal.workflow.Def(handler.HelloHandlerWorkflow.workflow_name, fakeHelloHandlerWorkflow));

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const result = try env.executeWorkflow(caller.HelloCallerWorkflow, .{ "test", .de }, arena.allocator(), null);
    try testing.expectEqualStrings("fake:de:test", result);
}
