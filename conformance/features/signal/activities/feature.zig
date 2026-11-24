//! A signal handler runs five activities side by side and waits for all of them; the
//! workflow returns the sum of their results once the handler has stored it.
const temporal = @import("temporal");
const harness = @import("harness");
const activity = temporal.activity;
const workflow = temporal.workflow;

const Workflow = workflow.Def("Workflow", runWorkflow);
const MyActivity = activity.Def("myActivity", myActivity);
const MySignal = workflow.SignalDef("mySignal", fn () void);
const activity_count = 5;
const activity_result = 6;

pub const feature = harness.define(.{
    .workflows = .{Workflow},
    .activities = .{MyActivity},
    .execute = execute,
});

fn myActivity(_: *activity.Context) !i64 {
    return activity_result;
}

const State = struct {
    ctx: *workflow.Context,
    total: i64 = 0,

    /// Starts every activity before waiting for any, so they run side by side.
    fn receive(state: *State) !void {
        var pending: [activity_count]workflow.ActivityFuture(i64) = undefined;
        for (&pending) |*future| {
            future.* = state.ctx.executeActivity(MyActivity, .{ .start_to_close_timeout = .fromSeconds(5) }, .{});
        }
        var total: i64 = 0;
        for (&pending) |*future| total += try future.get();
        state.total = total;
    }

    fn done(state: *State) bool {
        return state.total > 0;
    }
};

fn runWorkflow(ctx: *workflow.Context) !i64 {
    const state = try ctx.allocator().create(State);
    state.* = .{ .ctx = ctx };
    try ctx.setSignalHandler(MySignal, state, State.receive);
    try ctx.await(state, State.done);
    return state.total;
}

fn execute(ctx: *harness.Context) !void {
    var run = try ctx.client.executeWorkflow(Workflow, ctx.startOptions(), .{});
    defer run.deinit();
    try ctx.client.signalWorkflow(MySignal, run.id, run.run_id, .{});
    const result = try run.get(ctx.arena);
    const expected = activity_result * activity_count;
    try ctx.expect(result == expected, "expected workflow result {d}, got {d}", .{ expected, result });

    const recorded = try ctx.fetchHistory(run.id, run.run_id);
    var completed: usize = 0;
    for (recorded.events.items) |event| {
        if (event.event_type == .EVENT_TYPE_ACTIVITY_TASK_COMPLETED) completed += 1;
    }
    try ctx.expect(completed == activity_count, "expected {d} completed activities in the history, found {d}", .{ activity_count, completed });
    try ctx.expectEventsInOrder(recorded, &.{
        .EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
        .EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
        .EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
    });
}
