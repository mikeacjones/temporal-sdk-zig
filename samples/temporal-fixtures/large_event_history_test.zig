//! Tests for the large event history fixture.

const std = @import("std");
const temporal = @import("temporal");
const large_event_history = @import("large_event_history.zig");

const testing = std.testing;

test "a history of 1024 events that is not to fail completes" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(large_event_history.LargeEventHistoryWorkflow);
    try env.registerActivity(large_event_history.Activity);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    try env.executeWorkflow(large_event_history.LargeEventHistoryWorkflow, .{ 1024, false }, arena.allocator(), null);
}

/// Blocks the first activity until the worker stops, so the workflow's cancel arrives while
/// that activity is pending; every later activity returns at once.
const FirstActivityBlocks = struct {
    var calls: std.atomic.Value(u32) = .init(0);
    var started: std.Io.Event = .unset;

    fn run(ctx: *temporal.activity.Context) !void {
        if (calls.fetchAdd(1, .monotonic) != 0) return;
        started.set(ctx.io());
        try ctx.io().sleep(.fromSeconds(24 * std.time.s_per_hour), .awake);
    }
};

test "a canceled large event history workflow ends canceled" {
    // In the Go fixture a canceled workflow context makes every later activity fail with a
    // canceled error at once, and the last activity's error is what the workflow returns.
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(large_event_history.LargeEventHistoryWorkflow);
    try env.registerActivityAs(large_event_history.Activity, FirstActivityBlocks.run);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var run = try env.startWorkflow(large_event_history.LargeEventHistoryWorkflow, .{ .task_queue = "" }, .{ 60, false });
    defer run.deinit();

    try FirstActivityBlocks.started.wait(testing.io);
    try env.client.cancelWorkflow(run.id, run.run_id, "test");
    try testing.expectError(error.WorkflowCanceled, env.getResult(large_event_history.LargeEventHistoryWorkflow, &run, arena.allocator(), null));
}
