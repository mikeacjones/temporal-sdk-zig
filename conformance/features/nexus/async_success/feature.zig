//! A workflow calls an asynchronous Nexus operation backed by a workflow run, sees its
//! operation token once it has started, and then receives the backing workflow's result.
const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");

const nexus = temporal.nexus;
const workflow = temporal.workflow;
const SayHelloAsync = nexus.OperationDef("say-hello-async", []const u8, []const u8);
const Service = nexus.ServiceDef("test-service", .{SayHelloAsync});
const Handler = nexus.ServiceHandler(Service, .{nexus.OperationHandler(SayHelloAsync, sayHelloAsync)});
const Workflow = workflow.Def("Workflow", runWorkflow);
const HandlerWorkflow = workflow.Def("HandlerWorkflow", runHandlerWorkflow);

pub const feature = harness.define(.{
    .workflows = .{ Workflow, HandlerWorkflow },
    .nexus_services = .{Handler},
    .execute = execute,
});

fn runHandlerWorkflow(ctx: *workflow.Context, name: []const u8) ![]const u8 {
    return std.fmt.allocPrint(ctx.allocator(), "Hello, {s}!", .{name});
}

/// Starts `HandlerWorkflow` as "async-success-<name>" on the worker's task queue.
fn sayHelloAsync(ctx: *nexus.Context, name: []const u8) !nexus.WorkflowStart(HandlerWorkflow) {
    const id = try std.fmt.allocPrint(ctx.allocator(), "async-success-{s}", .{name});
    return .{ .options = .{ .id = id }, .args = .{name} };
}

fn runWorkflow(ctx: *workflow.Context, endpoint: []const u8) ![]const u8 {
    const operation = ctx.executeNexusOperation(Service, SayHelloAsync, .{
        .endpoint = endpoint,
        .schedule_to_close_timeout = .fromSeconds(60),
    }, "world");
    const token = try operation.started();
    if (token == null or token.?.len == 0)
        return ctx.fail(.{ .message = "expected a non-empty operation token", .non_retryable = true });
    return operation.get();
}

fn execute(ctx: *harness.Context) !void {
    const endpoint = ctx.nexus_endpoint orelse return ctx.fail("missing Nexus endpoint for {s}", .{ctx.task_queue});
    var run = try ctx.client.executeWorkflow(Workflow, ctx.startOptions(), .{endpoint});
    defer run.deinit();
    const result = run.get(ctx.arena) catch |err| {
        if (run.failure()) |failure| return ctx.fail("workflow failed: {s}", .{failure.message});
        return err;
    };
    try ctx.expectEqualStrings("Hello, world!", result);
    const recorded = try ctx.fetchHistory(run.id, run.run_id);
    try ctx.expectEventsInOrder(recorded, &.{
        .EVENT_TYPE_NEXUS_OPERATION_SCHEDULED,
        .EVENT_TYPE_NEXUS_OPERATION_STARTED,
        .EVENT_TYPE_NEXUS_OPERATION_COMPLETED,
    });
}
