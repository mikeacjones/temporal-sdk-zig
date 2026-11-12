//! Tests for the dynamic-workflows sample.

const std = @import("std");
const temporal = @import("temporal");
const dynamic_workflows = @import("dynamic_workflows.zig");

const testing = std.testing;

test "dynamic workflow calls the dynamic activity for a dynamic-activity type" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerDynamicWorkflow(dynamic_workflows.DynamicWorkflow);
    try env.registerDynamicActivity(dynamic_workflows.DynamicActivity);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const result = try env.executeWorkflowByName([]const u8, "dynamic-activity", .{ .task_queue = "" }, .{ "Hello", "World" }, arena.allocator(), null);
    try testing.expectEqualStrings("dynamic-activity - Hello - World", result);
}

test "dynamic workflow answers other types itself" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerDynamicWorkflow(dynamic_workflows.DynamicWorkflow);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const result = try env.executeWorkflowByName([]const u8, "DurableExecution", .{ .task_queue = "" }, .{ "Hello", "Temporal" }, arena.allocator(), null);
    try testing.expectEqualStrings("DurableExecution - Hello - Temporal", result);
}
