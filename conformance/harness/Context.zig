//! Borrowed for the task executing one feature. Activities must use their own context.
const Context = @This();

const std = @import("std");
const temporal = @import("temporal");

/// Thread-safe allocator for values that outlive a call.
gpa: std.mem.Allocator,
io: std.Io,
/// Task-local allocator whose values live through teardown.
arena: std.mem.Allocator,
/// Shared with the worker and closed by the harness.
client: *temporal.client.Client,
feature: []const u8,
namespace: []const u8,
task_queue: []const u8,
failure_message_: ?[]const u8 = null,

pub fn startOptions(ctx: *const Context) temporal.client.StartWorkflowOptions {
    return .{ .id = ctx.task_queue, .task_queue = ctx.task_queue, .execution_timeout = .fromSeconds(60) };
}

pub fn fail(ctx: *Context, comptime fmt: []const u8, args: anytype) error{FeatureFailed} {
    const message = std.fmt.allocPrint(ctx.arena, fmt, args) catch "out of memory formatting feature failure";
    ctx.failure_message_ = if (message.len == 0) "feature failed without a message" else message;
    return error.FeatureFailed;
}

pub fn expect(ctx: *Context, ok: bool, comptime fmt: []const u8, args: anytype) error{FeatureFailed}!void {
    if (!ok) return ctx.fail(fmt, args);
}

pub fn expectEqualStrings(ctx: *Context, expected: []const u8, actual: []const u8) error{FeatureFailed}!void {
    if (!std.mem.eql(u8, expected, actual))
        return ctx.fail("expected \"{f}\", got \"{f}\"", .{ std.zig.fmtString(expected), std.zig.fmtString(actual) });
}

test "conformance context defaults and assertions" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var ctx: Context = .{
        .gpa = std.testing.allocator,
        .io = std.testing.io,
        .arena = arena.allocator(),
        .client = undefined,
        .feature = "signal/basic",
        .namespace = "default",
        .task_queue = "conformance-signal/basic",
    };
    const options = ctx.startOptions();
    try std.testing.expectEqualStrings(ctx.task_queue, options.id);
    try std.testing.expectEqualStrings(ctx.task_queue, options.task_queue);
    try std.testing.expectEqual(@as(?std.Io.Duration, .fromSeconds(60)), options.execution_timeout);
    try ctx.expectEqualStrings("same", "same");
    try std.testing.expectEqual(null, ctx.failure_message_);
    try std.testing.expectError(error.FeatureFailed, ctx.expectEqualStrings("one", "two"));
    try std.testing.expectEqualStrings("expected \"one\", got \"two\"", ctx.failure_message_.?);
    try std.testing.expectError(error.FeatureFailed, ctx.expect(false, "expected {d}, got {d}", .{ 1, 2 }));
    try std.testing.expectEqualStrings("expected 1, got 2", ctx.failure_message_.?);
}
