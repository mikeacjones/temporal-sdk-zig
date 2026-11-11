//! Tests for the query sample, on the time-skipping server so the two-minute timer takes no
//! real time.

const std = @import("std");
const temporal = @import("temporal");
const query = @import("query_workflow.zig");

const testing = std.testing;

test "the state query reports the timer wait, then done" {
    const env = try temporal.testing.WorkflowEnvironment.startTimeSkipping(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(query.QueryWorkflow);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // Time stays locked until getResult, so the timer can't fire before these queries.
    var run = try env.startWorkflow(query.QueryWorkflow, .{ .task_queue = "" }, .{});
    defer run.deinit();
    try testing.expectEqualStrings("waiting on timer", try env.client.queryWorkflow(query.State, a, run.id, run.run_id, .{}));

    // A minute in, the workflow is still waiting on its timer.
    try env.sleep(.fromSeconds(std.time.s_per_min));
    try testing.expectEqualStrings("waiting on timer", try env.client.queryWorkflow(query.State, a, run.id, run.run_id, .{}));

    try env.getResult(query.QueryWorkflow, &run, a, null);
    try testing.expectEqualStrings("done", try env.client.queryWorkflow(query.State, a, run.id, run.run_id, .{}));
}
