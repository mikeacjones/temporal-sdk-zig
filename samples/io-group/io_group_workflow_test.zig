//! Tests for the io-group sample.

const std = @import("std");
const temporal = @import("temporal");
const io_group = @import("io_group_workflow.zig");

const testing = std.testing;

// Test doubles for Step1 and Step2 that don't sleep.
fn step1Test(ctx: *temporal.activity.Context, input: []const u8) ![]const u8 {
    return std.fmt.allocPrint(ctx.allocator(), "{s}, Step1", .{input});
}

fn step2Test(ctx: *temporal.activity.Context, input: []const u8) ![]const u8 {
    return std.fmt.allocPrint(ctx.allocator(), "{s}, Step2", .{input});
}

fn lessThan(_: void, x: []const u8, y: []const u8) bool {
    return std.mem.lessThan(u8, x, y);
}

test "every sequence runs both steps" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(io_group.IoGroup);
    try env.registerActivityAs(io_group.Step1, step1Test);
    try env.registerActivityAs(io_group.Step2, step2Test);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const result = try arena.allocator().dupe([]const u8, try env.executeWorkflow(io_group.IoGroup, .{2}, arena.allocator(), null));
    // The order the coroutines finish in is not defined.
    std.mem.sort([]const u8, result, {}, lessThan);
    try testing.expectEqual(@as(usize, 2), result.len);
    try testing.expectEqualStrings("0, Step1, Step2", result[0]);
    try testing.expectEqualStrings("1, Step1, Step2", result[1]);
}
