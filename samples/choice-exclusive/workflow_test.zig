//! Tests for the choice-exclusive sample.
//!
//! The tests run on a dev server, which retries failing activities with the default retry
//! policy (forever), so the failing test doubles return non-retryable failures.

const std = @import("std");
const temporal = @import("temporal");
const choice = @import("workflow.zig");
const orders = @import("order_activities.zig");

const testing = std.testing;

test "succeeds" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(choice.ExclusiveChoice);
    var activities: orders.OrderActivities = .{ .order_choices = &.{ choice.OrderChoiceApple, choice.OrderChoiceOrange } };
    try env.registerActivitiesWith(&activities, orders.all);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    try env.executeWorkflow(choice.ExclusiveChoice, .{}, arena.allocator(), null);
}

fn failingGetOrder(ctx: *temporal.activity.Context) ![]const u8 {
    return ctx.fail(.{ .message = "Get Order Error", .non_retryable = true });
}

test "fails when getting the order fails" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(choice.ExclusiveChoice);
    try env.registerActivityAs(orders.GetOrder, failingGetOrder);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var failure: ?temporal.Failure = null;
    try testing.expectError(error.WorkflowFailed, env.executeWorkflow(choice.ExclusiveChoice, .{}, arena.allocator(), &failure));
    try testing.expectEqualStrings("Get Order Error", temporal.failure.rootCause(&failure.?).message);
}

fn failingOrder(ctx: *temporal.activity.Context, choice_name: []const u8) !void {
    _ = choice_name;
    return ctx.fail(.{ .message = "Get Order Error", .non_retryable = true });
}

// The only choice is "apple", and ordering apples fails.
test "fails when placing the order fails" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(choice.ExclusiveChoice);
    var activities: orders.OrderActivities = .{ .order_choices = &.{choice.OrderChoiceApple} };
    try env.registerActivityWith(orders.GetOrder, &activities);
    try env.registerActivityAs(orders.OrderApple, failingOrder);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    try testing.expectError(error.WorkflowFailed, env.executeWorkflow(choice.ExclusiveChoice, .{}, arena.allocator(), null));
}
