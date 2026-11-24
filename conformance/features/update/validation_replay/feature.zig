//! The validator accepts only as many updates as `execute` allows, so an SDK that ran it
//! again on replay would reject the accepted update there and diverge from history. The
//! worker caches no workflows, so the live run replays the accepted update on each later
//! workflow task, and `zig build test` replays the stored history with no validation
//! allowed at all.
const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");
const activity = temporal.activity;
const workflow = temporal.workflow;

const Workflow = workflow.Def("Workflow", runWorkflow);
const TheActivity = activity.Def("TheActivity", theActivity);
const DoSomeStuff = workflow.UpdateDef("doSomeStuff", fn () void);
const Shutdown = workflow.SignalDef("shutdown_signal", fn () void);

pub const feature = harness.define(.{
    .workflows = .{Workflow},
    .activities = .{TheActivity},
    .worker_options = temporal.worker.Options{ .max_cached_workflows = 0 },
    .execute = execute,
});

/// How many more validations may pass. Not part of the workflow's state, which a real
/// validator must never read: `execute` allows exactly one.
var validations_allowed: std.atomic.Value(u32) = .init(0);

fn theActivity(_: *activity.Context) !i64 {
    return 1;
}

const State = struct {
    ctx: *workflow.Context,
    shutdown: bool = false,

    fn validate(_: *State) error{FailingValidation}!void {
        var allowed = validations_allowed.load(.acquire);
        while (allowed > 0) {
            allowed = validations_allowed.cmpxchgWeak(allowed, allowed - 1, .acq_rel, .acquire) orelse return;
        }
        return error.FailingValidation;
    }

    fn update(state: *State) !void {
        _ = try state.ctx.executeActivity(TheActivity, .{ .start_to_close_timeout = .fromSeconds(5) }, .{}).get();
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
    try ctx.setUpdateHandlerWithValidator(DoSomeStuff, state, State.update, State.validate);
    try ctx.setSignalHandler(Shutdown, state, State.stop);
    try ctx.await(state, State.stopped);
}

fn execute(ctx: *harness.Context) !void {
    validations_allowed.store(1, .release);
    var run = try ctx.client.executeWorkflow(Workflow, ctx.startOptions(), .{});
    defer run.deinit();

    var update = try ctx.client.updateWorkflow(DoSomeStuff, run.id, run.run_id, .{}, .{});
    defer update.deinit();
    update.get(ctx.arena) catch |err| switch (err) {
        error.UpdateFailed => return ctx.fail("update failed: {s}", .{if (update.failure()) |f| f.message else "no failure details"}),
        else => return err,
    };

    try ctx.client.signalWorkflow(Shutdown, run.id, run.run_id, .{});
    run.get(ctx.arena) catch |err| {
        if (run.failure()) |failure| return ctx.fail("workflow failed: {s}", .{failure.message});
        return err;
    };

    const recorded = try ctx.fetchHistory(run.id, run.run_id);
    try ctx.expectNoEvent(recorded, .EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_REJECTED);
    try ctx.expectNoEvent(recorded, .EVENT_TYPE_WORKFLOW_TASK_FAILED);
    try ctx.expectEventsInOrder(recorded, &.{
        .EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED,
        .EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
        .EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_COMPLETED,
        .EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
    });
}
