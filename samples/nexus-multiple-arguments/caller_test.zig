//! Tests of the caller workflow against the service's handler, served by the test
//! environment's worker through an endpoint that routes to it.

const std = @import("std");
const temporal = @import("temporal");
const service = @import("service.zig");
const handler = @import("handler.zig");
const caller = @import("caller.zig");

const testing = std.testing;
const WorkflowEnvironment = temporal.testing.WorkflowEnvironment;

/// An environment whose worker runs the caller workflow, with `caller.endpoint` routed to its
/// own namespace and task queue.
fn startEnvironment() !*WorkflowEnvironment {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    errdefer env.deinit();
    try caller.register(&env.worker);
    try env.createNexusEndpoint(caller.endpoint, .{});
    return env;
}

test "the hello caller workflow returns the greeting of the two-argument workflow the say-hello operation started" {
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

fn fakeHelloHandlerWorkflow(ctx: *temporal.workflow.Context, name: []const u8, language: service.Language) !service.HelloOutput {
    return .{ .message = try std.fmt.allocPrint(ctx.allocator(), "fake:{t}:{s}", .{ language, name }) };
}

test "the say-hello operation passes the input's name and language as the workflow's two arguments" {
    const env = try startEnvironment();
    defer env.deinit();
    try env.registerNexusService(handler.HelloServiceHandler);
    try env.registerWorkflow(temporal.workflow.Def(handler.HelloHandlerWorkflow.workflow_name, fakeHelloHandlerWorkflow));

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const result = try env.executeWorkflow(caller.HelloCallerWorkflow, .{ "test", .tr }, arena.allocator(), null);
    try testing.expectEqualStrings("fake:tr:test", result);
}
