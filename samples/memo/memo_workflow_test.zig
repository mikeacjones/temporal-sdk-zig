//! Tests for the memo sample.

const std = @import("std");
const temporal = @import("temporal");
const memo = @import("memo.zig");

const testing = std.testing;

const double_workflow_id = "memo-with-test-double";

const Doubles = struct {
    fn describeWorkflow(ctx: *temporal.activity.Context, workflow_id: []const u8) ![]const memo.MemoEntry {
        _ = ctx;
        try testing.expectEqualStrings(double_workflow_id, workflow_id);
        return &.{.{ .name = "description", .value = .{ .string = "described by the test double" } }};
    }
};

test "workflow reads its start memo and upserts it, with a test double for describe" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(memo.MemoWorkflow);
    try env.registerActivityAs(memo.DescribeWorkflow, Doubles.describeWorkflow);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const result = try env.executeWorkflowWithOptions(memo.MemoWorkflow, .{
        .id = double_workflow_id,
        .task_queue = "",
        .memo = try temporal.converter.toFields(a, .{ .description = "Test memo workflow" }),
    }, .{}, a, null);
    try testing.expectEqual(1, result.len);
    try testing.expectEqualStrings("description", result[0].name);
    try testing.expectEqualStrings("described by the test double", result[0].value.string);

    // The upsert replaced the start memo on the server.
    const description = try env.client.describeWorkflow(a, double_workflow_id, "");
    try testing.expectEqual(1, description.memo.len);
    try testing.expectEqualStrings("Test upsert memo workflow", (try description.memoValue([]const u8, a, "description")).?);
}

test "describe activity reads the upserted memo from the server" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(memo.MemoWorkflow);
    var activities: memo.Activities = .{ .client = &env.client };
    try env.registerActivityWith(memo.DescribeWorkflow, &activities);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const result = try env.executeWorkflowWithOptions(memo.MemoWorkflow, .{
        .task_queue = "",
        .memo = try temporal.converter.toFields(a, .{ .description = "Test memo workflow" }),
    }, .{}, a, null);
    try testing.expectEqual(1, result.len);
    try testing.expectEqualStrings("description", result[0].name);
    try testing.expectEqualStrings("Test upsert memo workflow", result[0].value.string);
}
