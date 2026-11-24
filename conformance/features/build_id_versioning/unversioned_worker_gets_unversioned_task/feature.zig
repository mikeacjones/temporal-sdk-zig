//! An unversioned worker on a task queue that has since got versions still gets the tasks
//! of a workflow that started unversioned. The feature's unversioned worker runs the
//! workflow's first task, and the workflow waits for a signal; the queue then gets {1.0} and
//! {2.0, 2.1}; the signal's task must still reach the unversioned worker, which completes
//! the workflow.
const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");
const workflow = temporal.workflow;

const Workflow = workflow.Def("Workflow", runWorkflow);
const Unblocker = workflow.SignalDef("unblocker", fn () void);

pub const feature = harness.define(.{
    .workflows = .{Workflow},
    .dev_server_args = &harness.build_ids.dev_server_args,
    .execute = execute,
});

const State = struct {
    unblocked: bool = false,

    fn unblock(state: *State) void {
        state.unblocked = true;
    }

    fn isUnblocked(state: *State) bool {
        return state.unblocked;
    }
};

fn runWorkflow(ctx: *workflow.Context) !void {
    const state = try ctx.allocator().create(State);
    state.* = .{};
    try ctx.setSignalHandler(Unblocker, state, State.unblock);
    try ctx.await(state, State.isUnblocked);
}

fn execute(ctx: *harness.Context) !void {
    var run = try ctx.client.executeWorkflow(Workflow, ctx.startOptions(), .{});
    defer run.deinit();
    // Ends the run on a failed check before the workflow does; harmless after it completed.
    defer ctx.client.terminateWorkflow(run.id, run.run_id, "feature cleanup") catch {};
    // The run is unversioned only once an unversioned worker has completed its first task:
    // a first task still queued when the versions arrive goes to the default set, 2.1.
    try ctx.waitForEvent(run.id, run.run_id, .EVENT_TYPE_WORKFLOW_TASK_COMPLETED, .fromSeconds(10));
    try harness.build_ids.addSomeVersions(ctx);
    // The signal's task must reach the unversioned worker through the queue's routing, not
    // through the sticky queue of the worker that ran the first task.
    try harness.build_ids.resetStickyQueue(ctx, run.id, run.run_id);
    try ctx.client.signalWorkflow(Unblocker, run.id, run.run_id, .{});
    run.get(ctx.arena) catch |err| {
        const message = if (run.failure()) |failure| failure.message else "no failure details";
        return ctx.fail("workflow returned error.{t}: {s}", .{ err, message });
    };
    try expectAllCompletedBy(ctx, run.id, run.run_id, "");
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
