//! A worker that registers an activity but does not poll for activity tasks runs a workflow
//! that schedules that activity. The worker must not ask the server to start it eagerly,
//! so nothing runs it: the activity fails with a schedule-to-start timeout, which the
//! workflow catches, and the worker keeps running to complete the workflow.
const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");

const activity = temporal.activity;
const failure = temporal.failure;
const workflow = temporal.workflow;

const Workflow = workflow.Def("Workflow", runWorkflow);
const Dummy = activity.Def("Dummy", dummy);

pub const feature = harness.define(.{
    .workflows = .{Workflow},
    .activities = .{Dummy},
    // Go's LocalActivityWorkerOnly, Python's no_remote_activities.
    .worker_options = temporal.worker.Options{ .poll_activity_tasks = false },
    .execute = execute,
});

/// How many times `Dummy` ran; read by `execute`, which expects none.
var dummy_runs: std.atomic.Value(u32) = .init(0);

fn dummy(_: *activity.Context) !void {
    _ = dummy_runs.fetchAdd(1, .acq_rel);
}

fn runWorkflow(ctx: *workflow.Context) !void {
    // Long enough for a busy machine, short enough for quick feedback (the suite's value).
    const handle = ctx.executeActivity(Dummy, .{ .schedule_to_close_timeout = .fromSeconds(3) }, .{});
    if (handle.get()) |_| {
        return fail(ctx, "expected activity to time out, it completed", .{});
    } else |err| if (err != error.ActivityFailed) return err;

    const top = handle.failure() orelse return fail(ctx, "expected activity to time out, it failed without a failure", .{});
    if (failure.kind(top) != .activity)
        return fail(ctx, "expected an activity failure, got {t} \"{s}\"", .{ failure.kind(top), top.message });
    const cause = top.cause orelse return fail(ctx, "expected the activity failure to have a timeout cause, it has none", .{});
    const timeout_type = switch (failure.kind(cause)) {
        .timeout => |t| t.timeout_type,
        else => |k| return fail(ctx, "expected the activity failure's cause to be a timeout, got {t} \"{s}\"", .{ k, cause.message }),
    };
    if (timeout_type != .schedule_to_start)
        return fail(ctx, "expected a schedule_to_start timeout, got {t}", .{timeout_type});
}

fn fail(ctx: *workflow.Context, comptime fmt: []const u8, args: anytype) error{ ApplicationFailure, OutOfMemory } {
    return ctx.fail(.{ .message = try std.fmt.allocPrint(ctx.allocator(), fmt, args) });
}

fn execute(ctx: *harness.Context) !void {
    var run = try ctx.client.executeWorkflow(Workflow, ctx.startOptions(), .{});
    defer run.deinit();
    run.get(ctx.arena) catch |err| {
        if (run.failure()) |f| return ctx.fail("workflow failed: {s}", .{f.message});
        return err;
    };
    try ctx.expect(dummy_runs.load(.acquire) == 0, "expected Dummy not to run on a worker that does not poll activity tasks, it ran {d} times", .{dummy_runs.load(.acquire)});

    const recorded = try ctx.fetchHistory(run.id, run.run_id);
    try ctx.expectNoEvent(recorded, .EVENT_TYPE_ACTIVITY_TASK_STARTED);
    try ctx.expectEventsInOrder(recorded, &.{
        .EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
        .EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT,
        .EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
    });
}
