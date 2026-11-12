//! Tests for the child-workflow-continue-as-new sample.

const std = @import("std");
const temporal = @import("temporal");
const sample = @import("child_workflow_continue_as_new.zig");

const testing = std.testing;

test "parent workflow sees only the result of the child's last run" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(sample.SampleParentWorkflow);
    try env.registerWorkflow(sample.SampleChildWorkflow);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var run = try env.startWorkflow(sample.SampleParentWorkflow, .{ .task_queue = "" }, .{});
    defer run.deinit();
    const result = try env.getResult(sample.SampleParentWorkflow, &run, a, null);
    try testing.expectEqualStrings("Child workflow execution completed after 5 runs", result);

    // Walk the child's runs from the last back to the first: each run names the run it
    // continued from, and each earlier run ended by continuing as new.
    const child_id = try std.fmt.allocPrint(a, "child_workflow:{s}", .{run.run_id});
    var run_id: []const u8 = "";
    var runs: usize = 0;
    while (true) : (runs += 1) {
        // An empty run ID reads the latest run.
        const events = (try env.client.fetchHistory(a, child_id, run_id)).events.items;
        const last = events[events.len - 1].attributes.?;
        if (runs == 0) {
            try testing.expect(last == .workflow_execution_completed_event_attributes);
        } else {
            try testing.expect(last == .workflow_execution_continued_as_new_event_attributes);
        }
        const started = events[0].attributes.?.workflow_execution_started_event_attributes;
        try env.replayRun(child_id, run_id);
        if (started.continued_execution_run_id.len == 0) break;
        run_id = started.continued_execution_run_id;
    }
    try testing.expectEqual(sample.child_runs, runs + 1);
}

test "child workflow fails on a run count below one" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(sample.SampleChildWorkflow);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var failure: ?temporal.failure.Failure = null;
    try testing.expectError(error.WorkflowFailed, env.executeWorkflow(sample.SampleChildWorkflow, .{ 0, 0 }, arena.allocator(), &failure));
    try testing.expectEqualStrings("invalid run count 0: it must be at least 1", failure.?.message);
}
