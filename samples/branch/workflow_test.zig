//! Tests for the branch sample.

const std = @import("std");
const temporal = @import("temporal");
const branch = @import("branch.zig");

const testing = std.testing;

// A test double that gives a different answer to each call.
const Double = struct {
    var calls: std.atomic.Value(u32) = .init(0);
    fn processBranch(ctx: *temporal.activity.Context, input: []const u8) ![]const u8 {
        _ = ctx;
        _ = input;
        const answers = [_][]const u8{ "one", "two", "three" };
        const n = calls.fetchAdd(1, .monotonic);
        if (n >= answers.len) return error.UnexpectedCall;
        return answers[n];
    }
};

fn lessThan(_: void, x: []const u8, y: []const u8) bool {
    return std.mem.lessThan(u8, x, y);
}

test "results from every branch" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(branch.Branch);
    try env.registerActivityAs(branch.ProcessBranch, Double.processBranch);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const result = try arena.allocator().dupe([]const u8, try env.executeWorkflow(branch.Branch, .{3}, arena.allocator(), null));
    std.mem.sort([]const u8, result, {}, lessThan);
    var expected = [_][]const u8{ "one", "two", "three" };
    std.mem.sort([]const u8, &expected, {}, lessThan);
    try testing.expectEqual(expected.len, result.len);
    for (expected, result) |e, r| try testing.expectEqualStrings(e, r);
}
