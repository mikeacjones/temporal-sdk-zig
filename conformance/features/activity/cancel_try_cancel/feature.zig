const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");

const activity = temporal.activity;
const workflow = temporal.workflow;

const Workflow = workflow.Def("Workflow", runWorkflow);
const CancellableActivity = activity.Def("CancellableActivity", cancellableActivity);
const ActivityStarted = workflow.SignalDef("activity-started", fn () void);
const ActivityResult = workflow.SignalDef("activity-result", fn ([]const u8) void);

pub const feature = harness.define(.{
    .workflows = .{Workflow},
    .activities = .{CancellableActivity},
    .execute = execute,
});

const Signals = struct {
    started: bool = false,
    result: ?[]const u8 = null,

    fn setStarted(state: *Signals) void {
        state.started = true;
    }

    fn setResult(state: *Signals, result: []const u8) void {
        state.result = result;
    }

    fn hasStarted(state: *Signals) bool {
        return state.started;
    }

    fn hasResult(state: *Signals) bool {
        return state.result != null;
    }
};

fn runWorkflow(ctx: *workflow.Context) !void {
    const signals = try ctx.allocator().create(Signals);
    signals.* = .{};
    try ctx.setSignalHandler(ActivityStarted, signals, Signals.setStarted);
    try ctx.setSignalHandler(ActivityResult, signals, Signals.setResult);

    const pending = ctx.executeActivity(CancellableActivity, .{
        .start_to_close_timeout = .fromSeconds(90),
        .heartbeat_timeout = .fromSeconds(5),
        .retry_policy = .{ .maximum_attempts = 1 },
        .cancellation_type = .try_cancel,
    }, .{});
    // A cancel before the activity starts can resolve the future without reaching its code.
    try ctx.await(signals, Signals.hasStarted);
    pending.cancel();
    if (pending.get()) |_| {
        return ctx.fail(.{ .message = "expected activity cancellation, got successful completion" });
    } else |err| if (err != error.ActivityFailed) return err;
    const failure = pending.failure() orelse
        return ctx.fail(.{ .message = "expected cancellation details for the activity" });
    const cause = temporal.failure.rootCause(failure);
    if (temporal.failure.kind(cause) != .canceled) {
        const message = try std.fmt.allocPrint(ctx.allocator(), "expected activity cancellation, got {s}", .{cause.message});
        return ctx.fail(.{ .message = message });
    }

    try ctx.await(signals, Signals.hasResult);
    const result = signals.result.?;
    if (!std.mem.eql(u8, result, "cancelled")) {
        const message = try std.fmt.allocPrint(ctx.allocator(), "expected activity-result \"cancelled\", got \"{s}\"", .{result});
        return ctx.fail(.{ .message = message });
    }
}

fn cancellableActivity(ctx: *activity.Context) !void {
    const client = try ctx.client();
    const info = ctx.info();
    const io = ctx.io();
    const deadline = std.Io.Timestamp.now(io, .awake).addDuration(.fromSeconds(60));
    try client.signalWorkflow(ActivityStarted, info.workflow_id, info.workflow_run_id, .{});
    ctx.heartbeat({});

    while (true) {
        const remaining = std.Io.Timestamp.now(io, .awake).durationTo(deadline).nanoseconds;
        if (remaining <= 0) break;
        io.sleep(.{ .nanoseconds = @min(remaining, std.time.ns_per_s) }, .awake) catch |err| {
            if (ctx.cancelReason() != .canceled) return err;
            break;
        };
        ctx.heartbeat({});
    }
    const result = if (ctx.cancelReason() == .canceled) "cancelled" else "timeout";

    // Report what the activity observed even when its task has been canceled.
    const protection = io.swapCancelProtection(.blocked);
    defer _ = io.swapCancelProtection(protection);
    // Complete successfully to distinguish try-cancel from waiting for the activity's result.
    try client.signalWorkflow(ActivityResult, info.workflow_id, info.workflow_run_id, .{result});
}

fn execute(ctx: *harness.Context) !void {
    var options = ctx.startOptions();
    // Leave time for the activity to report its own one-minute timeout.
    options.execution_timeout = .fromSeconds(90);
    var run = try ctx.client.executeWorkflow(Workflow, options, .{});
    defer run.deinit();
    run.get(ctx.arena) catch |err| {
        if (run.failure()) |failure| return ctx.fail("workflow failed: {s}", .{failure.message});
        return err;
    };
}
