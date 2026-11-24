//! Activities still running when their worker begins shutting down learn of it before the
//! graceful shutdown period ends: one then completes, one fails, and one ignores it and
//! times out. The driver stops the worker once they run and starts a new one, which runs
//! the workflow to its end.
const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");

const Io = std.Io;
const activity = temporal.activity;
const workflow = temporal.workflow;

const Workflow = workflow.Def("Workflow", runWorkflow);
const CancelSuccess = activity.Def("CancelSuccess", cancelSuccess);
const CancelFailure = activity.Def("CancelFailure", cancelFailure);
const CancelIgnore = activity.Def("CancelIgnore", cancelIgnore);
const shutdown_message = "worker is shutting down";

pub const feature = harness.define(.{
    .workflows = .{Workflow},
    .activities = .{ CancelSuccess, CancelFailure, CancelIgnore },
    .worker_options = temporal.worker.Options{ .graceful_shutdown_period = .fromSeconds(1) },
    .execute = execute,
});

/// How many of the three activities have started on the first worker; read by `execute`.
var activities_started: std.atomic.Value(u32) = .init(0);

fn cancelSuccess(ctx: *activity.Context) !void {
    _ = activities_started.fetchAdd(1, .acq_rel);
    try ctx.waitForWorkerStop();
}

fn cancelFailure(ctx: *activity.Context) !void {
    _ = activities_started.fetchAdd(1, .acq_rel);
    try ctx.waitForWorkerStop();
    return ctx.fail(.{ .message = shutdown_message });
}

fn cancelIgnore(ctx: *activity.Context) !void {
    _ = activities_started.fetchAdd(1, .acq_rel);
    // Ignores the worker's cancel, as Go's time.Sleep does; its 300 ms schedule-to-close
    // timeout ends it on the server long before this returns.
    const io = ctx.io();
    const protection = io.swapCancelProtection(.blocked);
    defer _ = io.swapCancelProtection(protection);
    io.sleep(.fromSeconds(15), .awake) catch {};
}

fn runWorkflow(ctx: *workflow.Context) ![]const u8 {
    const graceful: workflow.ActivityOptions = .{
        .schedule_to_close_timeout = .fromSeconds(30),
        .retry_policy = .{ .maximum_attempts = 1 },
    };
    const ignore: workflow.ActivityOptions = .{
        .schedule_to_close_timeout = .fromMilliseconds(300),
        .retry_policy = .{ .maximum_attempts = 1 },
    };
    const success = ctx.executeActivity(CancelSuccess, graceful, .{});
    const failure = ctx.executeActivity(CancelFailure, graceful, .{});
    const ignored = ctx.executeActivity(CancelIgnore, ignore, .{});

    success.get() catch |err| {
        const message = if (success.failure()) |f| f.message else @errorName(err);
        return fail(ctx, "expected activity to succeed, got {s}", .{message});
    };

    if (failure.get()) |_| {
        return fail(ctx, "expected activity to fail with '" ++ shutdown_message ++ "', got success", .{});
    } else |err| if (err != error.ActivityFailed) return err;
    const failed = temporal.failure.rootCause(failure.failure() orelse
        return fail(ctx, "expected activity to fail with '" ++ shutdown_message ++ "', got no failure", .{}));
    if (temporal.failure.kind(failed) != .application or !std.mem.eql(u8, failed.message, shutdown_message))
        return fail(ctx, "expected activity to fail with '" ++ shutdown_message ++ "', got {t} '{s}'", .{ temporal.failure.kind(failed), failed.message });

    if (ignored.get()) |_| {
        return fail(ctx, "expected activity to fail with ScheduleToClose timeout, got success", .{});
    } else |err| if (err != error.ActivityFailed) return err;
    const timed_out = temporal.failure.rootCause(ignored.failure() orelse
        return fail(ctx, "expected activity to fail with ScheduleToClose timeout, got no failure", .{}));
    const is_schedule_to_close = switch (temporal.failure.kind(timed_out)) {
        .timeout => |t| t.timeout_type == .schedule_to_close,
        else => false,
    };
    if (!is_schedule_to_close)
        return fail(ctx, "expected activity to fail with ScheduleToClose timeout, got {t} '{s}'", .{ temporal.failure.kind(timed_out), timed_out.message });

    return "done";
}

fn fail(ctx: *workflow.Context, comptime fmt: []const u8, args: anytype) error{ ApplicationFailure, OutOfMemory } {
    return ctx.fail(.{ .message = try std.fmt.allocPrint(ctx.allocator(), fmt, args) });
}

fn execute(ctx: *harness.Context) !void {
    var run = try ctx.client.executeWorkflow(Workflow, ctx.startOptions(), .{});
    defer run.deinit();
    try ctx.waitForEvent(run.id, run.run_id, .EVENT_TYPE_ACTIVITY_TASK_SCHEDULED, .fromSeconds(5));

    // The activities must be running on this worker when it stops, or they would wait for
    // the next worker's shutdown instead.
    const deadline = Io.Timestamp.now(ctx.io, .boot).addDuration(.fromSeconds(5));
    while (activities_started.load(.acquire) < 3) {
        const remaining = Io.Timestamp.now(ctx.io, .boot).durationTo(deadline).nanoseconds;
        if (remaining <= 0) return ctx.fail("expected 3 activities to start within 5s, {d} did", .{activities_started.load(.acquire)});
        try ctx.io.sleep(.fromNanoseconds(@min(20 * std.time.ns_per_ms, remaining)), .boot);
    }

    try ctx.stopWorker();
    try ctx.startWorker();

    const result = run.get(ctx.arena) catch |err| {
        if (run.failure()) |failure| return ctx.fail("workflow failed: {s}", .{failure.message});
        return err;
    };
    try ctx.expectEqualStrings("done", result);
}
