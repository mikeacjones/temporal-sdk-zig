//! Port of samples-go/goroutine/goroutine_workflow_test.go.

const std = @import("std");
const temporal = @import("temporal");
const goroutine = @import("goroutine_workflow.zig");

const testing = std.testing;

// Go registers Step1Test and Step2Test (no sleeping) under the names Step1 and Step2.
fn step1Test(ctx: *temporal.activity.Context, input: []const u8) ![]const u8 {
    return std.fmt.allocPrint(ctx.allocator(), "{s}, Step1", .{input});
}

fn step2Test(ctx: *temporal.activity.Context, input: []const u8) ![]const u8 {
    return std.fmt.allocPrint(ctx.allocator(), "{s}, Step2", .{input});
}

fn lessThan(_: void, x: []const u8, y: []const u8) bool {
    return std.mem.lessThan(u8, x, y);
}

test "GoroutineWorkflow" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(goroutine.SampleGoroutineWorkflow);
    try env.registerActivityAs(goroutine.Step1, step1Test);
    try env.registerActivityAs(goroutine.Step2, step2Test);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const result = try arena.allocator().dupe([]const u8, try env.executeWorkflow(goroutine.SampleGoroutineWorkflow, .{2}, arena.allocator(), null));
    // The order the coroutines finish in is not defined.
    std.mem.sort([]const u8, result, {}, lessThan);
    try testing.expectEqual(@as(usize, 2), result.len);
    try testing.expectEqualStrings("0, Step1, Step2", result[0]);
    try testing.expectEqualStrings("1, Step1, Step2", result[1]);
}
