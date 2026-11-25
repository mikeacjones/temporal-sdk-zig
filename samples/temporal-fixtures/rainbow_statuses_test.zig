//! Tests for the rainbow statuses fixture.

const std = @import("std");
const temporal = @import("temporal");
const rainbow_statuses = @import("rainbow_statuses.zig");

const testing = std.testing;

test "the completed status completes" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(rainbow_statuses.RainbowStatusesWorkflow);
    try env.registerActivity(rainbow_statuses.CompletedActivity);
    try env.registerActivity(rainbow_statuses.FailedActivity);
    try env.registerActivity(rainbow_statuses.LongActivity);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    try env.executeWorkflow(rainbow_statuses.RainbowStatusesWorkflow, .{.completed}, arena.allocator(), null);
}

/// Waits until the run's one activity has been picked up by a worker.
fn waitForStartedActivity(env: *temporal.testing.WorkflowEnvironment, arena: std.mem.Allocator, id: []const u8, run_id: []const u8) !void {
    for (0..600) |_| {
        const d = try env.client.describeWorkflow(arena, id, run_id);
        const pending = d.raw.pending_activities.items;
        if (pending.len == 1 and pending[0].state == .PENDING_ACTIVITY_STATE_STARTED) return;
        try testing.io.sleep(.fromMilliseconds(50), .awake);
    }
    return error.ActivityNeverStarted;
}

test "a running run stays open when its worker stops during LongActivity" {
    // The fixture is for a run the UI shows as running. Stopping the worker (SIGINT or
    // SIGTERM to worker.zig) must not fail LongActivity: with one attempt allowed, a failed
    // attempt fails the run as soon as a worker polls the task queue again.
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const task_queue = "rainbow-statuses-worker-stop";
    var w = try temporal.worker.Worker.init(testing.allocator, testing.io, &env.client, task_queue, .{});
    defer w.deinit();
    try w.registerWorkflow(rainbow_statuses.RainbowStatusesWorkflow);
    try w.registerActivity(rainbow_statuses.CompletedActivity);
    try w.registerActivity(rainbow_statuses.FailedActivity);
    try w.registerActivity(rainbow_statuses.LongActivity);
    var running = try testing.io.concurrent(temporal.worker.Worker.run, .{&w});
    var stopped = false;
    defer if (!stopped) {
        w.stop();
        running.await(testing.io) catch {};
    };
    try w.waitUntilRunning();

    var run = try env.client.executeWorkflow(rainbow_statuses.RainbowStatusesWorkflow, .{ .id = "worker-stop_Running", .task_queue = task_queue }, .{.running});
    defer run.deinit();
    try waitForStartedActivity(env, a, run.id, run.run_id);

    w.stop();
    stopped = true;
    try running.await(testing.io);

    const history = try env.client.fetchHistory(a, run.id, run.run_id);
    for (history.events.items) |event| switch (event.attributes orelse continue) {
        .activity_task_failed_event_attributes => |f| {
            std.debug.print("LongActivity failed when the worker stopped: {s}\n", .{if (f.failure) |x| x.message else ""});
            return error.TestUnexpectedResult;
        },
        else => {},
    };
    try testing.expectEqual(.running, (try env.client.describeWorkflow(a, run.id, run.run_id)).status);
    try env.client.terminateWorkflow(run.id, run.run_id, "test cleanup");
}
