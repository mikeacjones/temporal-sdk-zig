//! Tests of the caller workflow against the service's handler, served by the test
//! environment's worker through an endpoint that routes to it.

const std = @import("std");
const temporal = @import("temporal");
const service = @import("service.zig");
const handler = @import("handler.zig");
const caller = @import("caller.zig");
const history = @import("history.zig");

const testing = std.testing;
const workflow = temporal.workflow;
const WorkflowEnvironment = temporal.testing.WorkflowEnvironment;

/// An environment whose worker runs the caller workflow and serves the service, with
/// `caller.endpoint` routed to its own namespace and task queue.
fn startEnvironment() !*WorkflowEnvironment {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    errdefer env.deinit();
    try caller.register(&env.worker);
    try env.registerNexusService(handler.HelloServiceHandler);
    try env.createNexusEndpoint(caller.endpoint, .{});
    return env;
}

/// Bounds a caller that fails to cancel the operations, which would otherwise wait out the
/// handler workflows' ten minutes of work.
const bounded: temporal.client.StartWorkflowOptions = .{ .execution_timeout = .fromSeconds(30) };

/// tr finishes at once; the others would work for ten minutes.
const tr_first: caller.Timing = .{
    .work_ms = .{ .en = 10 * std.time.ms_per_min, .fr = 10 * std.time.ms_per_min, .de = 10 * std.time.ms_per_min, .es = 10 * std.time.ms_per_min, .tr = 0 },
    .cleanup_ms = 100,
};

test "the caller workflow returns the greeting of the first operation to finish and cancels the others, whose workflows clean up and end canceled" {
    const env = try startEnvironment();
    defer env.deinit();
    try env.registerWorkflow(handler.HelloHandlerWorkflow);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var run = try env.startWorkflow(caller.HelloCallerWorkflow, bounded, .{ "Nexus", tr_first });
    defer run.deinit();
    // `getResult` replays the caller's history too.
    try testing.expectEqualStrings("Merhaba Nexus 👋", try env.getResult(caller.HelloCallerWorkflow, &run, a, null));

    const operations = try history.operations(a, try env.client.fetchHistory(a, run.id, run.run_id));
    try testing.expectEqual(5, operations.len);
    for (operations) |op| {
        const handler_workflow = op.handler orelse return error.TestOperationNotStarted;
        if (op.language == .tr) {
            try testing.expectEqualStrings("Merhaba Nexus 👋", op.outcome.completed);
            continue;
        }
        try testing.expectEqual(.canceled, std.meta.activeTag(op.outcome));
        // The cancellation reached the handler workflow, which cleaned up and ended canceled.
        var handler_run = try env.client.getWorkflow(service.HelloOutput, handler_workflow.workflow_id, handler_workflow.run_id);
        defer handler_run.deinit();
        try testing.expectError(error.WorkflowCanceled, handler_run.get(a));
        const cleanup = try history.cleanupAfterCancel(a, try env.client.fetchHistory(a, handler_workflow.workflow_id, handler_workflow.run_id));
        try testing.expectEqual(100, cleanup.toMilliseconds());
    }
}

test "the caller workflow returns once the handlers have received the cancel requests, while their workflows still clean up" {
    const env = try startEnvironment();
    defer env.deinit();
    try env.registerWorkflow(handler.HelloHandlerWorkflow);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    // A cleanup of ten minutes: a caller that waited for the canceled operations to end,
    // rather than for their cancel requests to arrive, would run into `bounded`.
    var timing = tr_first;
    timing.cleanup_ms = 10 * std.time.ms_per_min;
    var run = try env.startWorkflow(caller.HelloCallerWorkflow, bounded, .{ "Nexus", timing });
    defer run.deinit();
    try testing.expectEqualStrings("Merhaba Nexus 👋", try env.getResult(caller.HelloCallerWorkflow, &run, a, null));

    const operations = try history.operations(a, try env.client.fetchHistory(a, run.id, run.run_id));
    try testing.expectEqual(5, operations.len);
    for (operations) |op| {
        if (op.language == .tr) continue;
        try testing.expectEqual(.canceled, std.meta.activeTag(op.outcome));
        const handler_workflow = op.handler orelse return error.TestOperationNotStarted;
        // The handler workflow has its cancel request and hasn't ended: it is cleaning up.
        const handler_history = try env.client.fetchHistory(a, handler_workflow.workflow_id, handler_workflow.run_id);
        try testing.expectError(error.NotEndedCanceled, history.cleanupAfterCancel(a, handler_history));
        const described = try env.client.describeWorkflow(a, handler_workflow.workflow_id, handler_workflow.run_id);
        try testing.expectEqual(temporal.client.WorkflowExecutionStatus.running, described.status);
    }
}

/// A test double for the handler workflow: en fails at once, the others wait until canceled.
fn failingHelloHandlerWorkflow(ctx: *workflow.Context, input: service.HelloInput) !service.HelloOutput {
    if (input.language == .en) return error.OutOfGreetings;
    try ctx.io().sleep(.fromSeconds(10 * std.time.s_per_min), .awake);
    return .{ .message = "too late" };
}

test "an operation that fails rather than being canceled fails the caller workflow, after the others are canceled" {
    const env = try startEnvironment();
    defer env.deinit();
    try env.registerWorkflow(workflow.Def(handler.HelloHandlerWorkflow.workflow_name, failingHelloHandlerWorkflow));

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var run = try env.startWorkflow(caller.HelloCallerWorkflow, bounded, .{ "Nexus", .{} });
    defer run.deinit();
    try testing.expectError(error.WorkflowFailed, env.getResult(caller.HelloCallerWorkflow, &run, a, null));

    const operations = try history.operations(a, try env.client.fetchHistory(a, run.id, run.run_id));
    try testing.expectEqual(5, operations.len);
    for (operations) |op| {
        const expected: std.meta.Tag(history.Operation.Outcome) = if (op.language == .en) .failed else .canceled;
        try testing.expectEqual(expected, std.meta.activeTag(op.outcome));
    }
}
