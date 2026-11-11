//! Tests for the child-workflow sample.

const std = @import("std");
const temporal = @import("temporal");
const child_workflow = @import("child_workflow.zig");

const testing = std.testing;

test "parent workflow runs the real child workflow" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(child_workflow.SampleParentWorkflow);
    try env.registerWorkflow(child_workflow.SampleChildWorkflow);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const result = try env.executeWorkflow(child_workflow.SampleParentWorkflow, .{}, arena.allocator(), null);
    try testing.expectEqualStrings("Hello World!", result);
}

fn childDouble(ctx: *temporal.workflow.Context, name: []const u8) ![]const u8 {
    return std.fmt.allocPrint(ctx.allocator(), "Hi {s}!", .{name});
}

test "parent workflow with a test double for its child workflow" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(child_workflow.SampleParentWorkflow);
    try env.registerWorkflow(temporal.workflow.Def(child_workflow.SampleChildWorkflow.workflow_name, childDouble));

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const result = try env.executeWorkflow(child_workflow.SampleParentWorkflow, .{}, arena.allocator(), null);
    try testing.expectEqualStrings("Hi World!", result);
}
