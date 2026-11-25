//! Tests for the stuck workflows fixture.

const std = @import("std");
const temporal = @import("temporal");
const stuck_workflows = @import("stuck_workflows.zig");

const testing = std.testing;

fn activityDouble(ctx: *temporal.activity.Context) !void {
    _ = ctx;
}

test "the workflow completes once its activity succeeds" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(stuck_workflows.StuckWorkflow);
    // Registered as "StuckWorkflowActivity", in place of the activity that always fails.
    try env.registerActivityAs(stuck_workflows.StuckWorkflowActivity, activityDouble);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    try env.executeWorkflow(stuck_workflows.StuckWorkflow, .{}, arena.allocator(), null);
}

test "the activity fails with a retryable application failure" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var env: temporal.testing.ActivityEnvironment = .init(testing.io);
    try testing.expectError(error.ApplicationFailure, env.executeActivity(stuck_workflows.StuckWorkflowActivity, .{}, arena.allocator()));
}
