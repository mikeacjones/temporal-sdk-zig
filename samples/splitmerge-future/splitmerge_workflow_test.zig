//! Port of samples-go/splitmerge-future/splitmerge_workflow_test.go.

const std = @import("std");
const temporal = @import("temporal");
const splitmerge = @import("splitmerge_workflow.zig");

const testing = std.testing;

test "Workflow" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(splitmerge.SampleSplitMergeFutureWorkflow);
    try env.registerActivity(splitmerge.ChunkProcessingActivity);

    const worker_count = 5;
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const result = try env.executeWorkflow(splitmerge.SampleSplitMergeFutureWorkflow, .{worker_count}, arena.allocator(), null);

    var total_item: i64 = 0;
    var total_sum: i64 = 0;
    for (1..worker_count + 1) |i| {
        total_item += @intCast(i);
        total_sum += @intCast(i * i);
    }
    try testing.expectEqual(total_item, result.NumberOfItemsInChunk);
    try testing.expectEqual(total_sum, result.SumInChunk);
}
