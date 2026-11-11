//! Tests for the cancellation sample.

const std = @import("std");
const temporal = @import("temporal");
const cancellation = @import("workflow.zig");

const testing = std.testing;
const activity = temporal.activity;

/// Wraps the sample's activities to record what happened, in order.
const Recorder = struct {
    var started: std.Io.Event = .unset;
    /// Set once `ActivityToBeCanceled` has returned; `seen_reason` is written before.
    var stopped: std.Io.Event = .unset;
    var seen_reason: ?activity.CancelReason = null;
    var cleanup_ran: bool = false;
    var cleanup_after_stop: bool = false;

    fn toBeCanceled(ctx: *activity.Context) !void {
        started.set(ctx.io());
        defer stopped.set(ctx.io());
        defer seen_reason = ctx.cancelReason();
        return cancellation.ActivityToBeCanceled.call(null, ctx, .{});
    }

    fn cleanup(ctx: *activity.Context) !void {
        cleanup_after_stop = stopped.isSet();
        cleanup_ran = true;
        return cancellation.CleanupActivity.call(null, ctx, .{});
    }
};

test "canceling the workflow cancels its activity, runs the cleanup, and ends canceled" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(cancellation.YourWorkflow);
    try env.registerActivityAs(cancellation.ActivityToBeCanceled, Recorder.toBeCanceled);
    try env.registerActivityAs(cancellation.CleanupActivity, Recorder.cleanup);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var run = try env.startWorkflow(cancellation.YourWorkflow, .{ .task_queue = "" }, .{});
    defer run.deinit();

    try Recorder.started.wait(testing.io);
    try env.client.cancelWorkflow(run.id, run.run_id, "test");
    try testing.expectError(error.WorkflowCanceled, env.getResult(cancellation.YourWorkflow, &run, arena.allocator(), null));

    try testing.expect(Recorder.stopped.isSet());
    try testing.expectEqual(activity.CancelReason.canceled, Recorder.seen_reason.?);
    try testing.expect(Recorder.cleanup_ran);
    try testing.expect(Recorder.cleanup_after_stop);
}

const Heartbeats = struct {
    io: std.Io,
    seen: std.Io.Event = .unset,

    fn record(context: *anyopaque, details: []const temporal.converter.Payload) void {
        _ = details;
        const h: *Heartbeats = @ptrCast(@alignCast(context));
        h.seen.set(h.io);
    }
};

fn runToBeCanceled(env: *const temporal.testing.ActivityEnvironment, arena: std.mem.Allocator) !void {
    return env.executeActivity(cancellation.ActivityToBeCanceled, .{}, arena);
}

test "the activity heartbeats until it is canceled" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var heartbeats: Heartbeats = .{ .io = testing.io };
    var env: temporal.testing.ActivityEnvironment = .init(testing.io);
    env.on_heartbeat = .{ .gpa = testing.allocator, .context = &heartbeats, .record = Heartbeats.record };

    var running = try testing.io.concurrent(runToBeCanceled, .{ &env, arena.allocator() });
    try heartbeats.seen.wait(testing.io);
    try testing.expectError(error.Canceled, running.cancel(testing.io));
}
