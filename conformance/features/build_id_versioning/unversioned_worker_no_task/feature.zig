//! An unversioned worker polling a task queue that has versions gets none of its tasks. The
//! queue gets {1.0} and {2.0, 2.1}; a workflow starts while only the feature's unversioned
//! worker polls, and a query with a 3 s deadline must not be answered; a 2.1 worker then
//! completes the workflow.
const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");
const workflow = temporal.workflow;

const Workflow = workflow.Def("Workflow", runWorkflow);

pub const feature = harness.define(.{
    .workflows = .{Workflow},
    .dev_server_args = &harness.build_ids.dev_server_args,
    .execute = execute,
});

fn runWorkflow(_: *workflow.Context) !void {}

fn execute(ctx: *harness.Context) !void {
    try harness.build_ids.addSomeVersions(ctx);
    var run = try ctx.client.executeWorkflow(Workflow, ctx.startOptions(), .{});
    defer run.deinit();
    // Ends the run on a failed check, when no 2.1 worker may be left to complete it.
    defer ctx.client.terminateWorkflow(run.id, run.run_id, "feature cleanup") catch {};

    // The suite's check: the built-in stack trace query must not be answered, as the
    // unversioned worker must not get the run's first task. Any error is the expected outcome.
    var short = try ctx.client.withRpcOptions(.{ .timeout = .fromSeconds(3), .retry = false });
    if (short.queryWorkflowByName("__stack_trace", ctx.arena, run.id, run.run_id, .{})) |_| {
        return ctx.fail("the unversioned worker should not have gotten a task, but answered query \"__stack_trace\"", .{});
    } else |err| switch (err) {
        error.Canceled => return error.Canceled,
        else => {},
    }
    const recorded = try ctx.fetchHistory(run.id, run.run_id);
    try ctx.expectNoEvent(recorded, .EVENT_TYPE_WORKFLOW_TASK_STARTED);

    try ctx.stopWorker();
    try ctx.startWorkerWith(.{ .build_id = "2.1", .use_build_id_versioning = true });
    run.get(ctx.arena) catch |err| {
        const message = if (run.failure()) |failure| failure.message else "no failure details";
        return ctx.fail("workflow returned error.{t}: {s}", .{ err, message });
    };
    try expectAllCompletedBy(ctx, run.id, run.run_id, "2.1");
}

/// Checks that every workflow task of the run was completed by build `expected` (empty for
/// an unversioned worker), as the `worker_version` of each WorkflowTaskCompleted event says:
/// a query that goes unanswered does not show which worker ran a task.
fn expectAllCompletedBy(ctx: *harness.Context, workflow_id: []const u8, run_id: []const u8, comptime expected: []const u8) !void {
    const recorded = try ctx.fetchHistory(workflow_id, run_id);
    var completed: usize = 0;
    for (recorded.events.items) |event| {
        if (event.event_type != .EVENT_TYPE_WORKFLOW_TASK_COMPLETED) continue;
        completed += 1;
        const attributes = switch (event.attributes orelse return ctx.fail("event {d}, a workflow task completion, has no attributes", .{event.event_id})) {
            .workflow_task_completed_event_attributes => |a| a,
            else => return ctx.fail("event {d} is a workflow task completion without its attributes", .{event.event_id}),
        };
        const actual = if (attributes.worker_version) |v| v.build_id else "";
        if (!std.mem.eql(u8, actual, expected))
            return ctx.fail("workflow task completed at event {d} by build \"{s}\"; expected build \"" ++ expected ++ "\"", .{ event.event_id, actual });
    }
    if (completed == 0) return ctx.fail("history has no completed workflow task", .{});
}
