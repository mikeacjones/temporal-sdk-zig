//! Tests for the splitmerge-select sample.

const std = @import("std");
const temporal = @import("temporal");
const splitmerge = @import("splitmerge_workflow.zig");

const testing = std.testing;

test "merges every chunk" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(splitmerge.SplitMergeSelect);
    try env.registerActivity(splitmerge.ProcessChunk);

    const activity_count = 5;
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const result = try env.executeWorkflow(splitmerge.SplitMergeSelect, .{activity_count}, arena.allocator(), null);

    var total_item: i64 = 0;
    var total_sum: i64 = 0;
    for (1..activity_count + 1) |i| {
        total_item += @intCast(i);
        total_sum += @intCast(i * i);
    }
    try testing.expectEqual(total_item, result.items);
    try testing.expectEqual(total_sum, result.sum);
}
