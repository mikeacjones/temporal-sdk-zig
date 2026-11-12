//! Tests for the update sample, on the time-skipping server. Every update and the "done"
//! signal are sent before waiting for the result, so time never skips while the counter waits
//! for them.

const std = @import("std");
const temporal = @import("temporal");
const update = @import("update.zig");

const testing = std.testing;

test "updates add to the counter, a negative addend is rejected, and done returns the count" {
    const env = try temporal.testing.WorkflowEnvironment.startTimeSkipping(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(update.CounterWorkflow);

    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var run = try env.startWorkflow(update.CounterWorkflow, .{ .task_queue = "" }, .{});
    defer run.deinit();

    try testing.expectEqual(0, try env.updateWorkflow(update.FetchAndAdd, arena, run.id, .{3}, null));
    try testing.expectEqual(3, try env.updateWorkflow(update.FetchAndAdd, arena, run.id, .{4}, null));

    var failure: ?temporal.Failure = null;
    try testing.expectError(error.UpdateFailed, env.updateWorkflow(update.FetchAndAdd, arena, run.id, .{-5}, &failure));
    try testing.expectEqualStrings("NegativeAddend", failure.?.message);

    // The rejected update left the counter at 7.
    try testing.expectEqual(7, try env.updateWorkflow(update.FetchAndAdd, arena, run.id, .{0}, null));
    try testing.expectEqual(7, try env.updateWorkflow(update.FetchAndAdd, arena, run.id, .{1}, null));

    try env.signalWorkflow(update.Done, run.id, .{});
    try testing.expectEqual(8, try env.getResult(update.CounterWorkflow, &run, arena, null));
}

test "done with no updates returns zero" {
    const env = try temporal.testing.WorkflowEnvironment.startTimeSkipping(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(update.CounterWorkflow);

    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var run = try env.startWorkflow(update.CounterWorkflow, .{ .task_queue = "" }, .{});
    defer run.deinit();
    try env.signalWorkflow(update.Done, run.id, .{});
    try testing.expectEqual(0, try env.getResult(update.CounterWorkflow, &run, arena, null));
}
