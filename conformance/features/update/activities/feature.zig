const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");
const activity = temporal.activity;
const workflow = temporal.workflow;

const Workflow = workflow.Def("Workflow", runWorkflow);
const SomeActivity = activity.Def("SomeActivity", someActivity);
const UpdateActivity = workflow.UpdateDef("updateActivity", fn () i64);
const Shutdown = workflow.SignalDef("shutdown_signal", fn () void);
const activity_result = 6;
const activity_count = 5;

pub const feature = harness.define(.{
    .workflows = .{Workflow},
    .activities = .{SomeActivity},
    .execute = execute,
});

fn someActivity(_: *activity.Context) !i64 {
    return activity_result;
}

const State = struct {
    ctx: *workflow.Context,
    shutdown: bool = false,

    /// Starts every activity before waiting for any, so they run side by side.
    fn update(state: *State) !i64 {
        var pending: [activity_count]workflow.ActivityFuture(i64) = undefined;
        for (&pending) |*future| {
            future.* = state.ctx.executeActivity(SomeActivity, .{ .start_to_close_timeout = .fromSeconds(5) }, .{});
        }
        var total: i64 = 0;
        for (&pending) |*future| total += try future.get();
        return total;
    }

    fn stop(state: *State) void {
        state.shutdown = true;
    }

    fn stopped(state: *State) bool {
        return state.shutdown;
    }
};

fn runWorkflow(ctx: *workflow.Context) !void {
    const state = try ctx.allocator().create(State);
    state.* = .{ .ctx = ctx };
    try ctx.setUpdateHandler(UpdateActivity, state, State.update);
    try ctx.setSignalHandler(Shutdown, state, State.stop);
    try ctx.await(state, State.stopped);
}

fn execute(ctx: *harness.Context) !void {
    var run = try ctx.client.executeWorkflow(Workflow, ctx.startOptions(), .{});
    defer run.deinit();

    var update = try ctx.client.updateWorkflow(UpdateActivity, run.id, run.run_id, .{}, .{});
    defer update.deinit();
    const result = update.get(ctx.arena) catch |err| switch (err) {
        error.UpdateFailed => return ctx.fail("update failed: {s}", .{if (update.failure()) |f| f.message else "no failure details"}),
        else => return err,
    };
    try ctx.expect(result == activity_result * activity_count, "expected update result {d}, got {d}", .{ activity_result * activity_count, result });

    try ctx.client.signalWorkflow(Shutdown, run.id, run.run_id, .{});
    try run.get(ctx.arena);

    const recorded = try ctx.fetchHistory(run.id, run.run_id);
    try ctx.expectNoEvent(recorded, .EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_REJECTED);
    try ctx.expectEventsInOrder(recorded, &.{
        .EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED,
        .EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
        .EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_COMPLETED,
        .EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
    });
}
