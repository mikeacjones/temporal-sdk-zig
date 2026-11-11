//! Port of samples-go/choice-exclusive/workflow_test.go.
//!
//! Go's mocked test environment doesn't retry a failing activity unless the activity options
//! set a retry policy. These tests run on a real dev server, which applies the default retry
//! policy (retry forever), so the failing test doubles return non-retryable failures.

const std = @import("std");
const temporal = @import("temporal");
const choice = @import("workflow.zig");
const orders = @import("order_activities.zig");

const testing = std.testing;

test "ExclusiveChoiceWorkflowSucceeds" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(choice.ExclusiveChoiceWorkflow);
    var activities: orders.OrderActivities = .{ .order_choices = &.{ choice.OrderChoiceApple, choice.OrderChoiceOrange } };
    try env.registerActivitiesWith(&activities, orders.all);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    try env.executeWorkflow(choice.ExclusiveChoiceWorkflow, .{}, arena.allocator(), null);
}

fn failingGetOrder(ctx: *temporal.activity.Context) ![]const u8 {
    return ctx.fail(.{ .message = "Get Order Error", .non_retryable = true });
}

test "ExclusiveChoiceWorkflowFailOnGetOrderFailure" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(choice.ExclusiveChoiceWorkflow);
    try env.registerActivityAs(orders.GetOrder, failingGetOrder);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var failure: ?temporal.Failure = null;
    try testing.expectError(error.WorkflowFailed, env.executeWorkflow(choice.ExclusiveChoiceWorkflow, .{}, arena.allocator(), &failure));
    try testing.expectEqualStrings("Get Order Error", temporal.failure.rootCause(&failure.?).message);
}

fn failingOrder(ctx: *temporal.activity.Context, choice_name: []const u8) !void {
    _ = choice_name;
    return ctx.fail(.{ .message = "Get Order Error", .non_retryable = true });
}

// Go registers only GetOrder (always "apple") and lets the unregistered OrderApple fail. A
// real worker retries unregistered activities, so here OrderApple is a failing double.
test "ExclusiveChoiceWorkflowFailOnOrdering" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(choice.ExclusiveChoiceWorkflow);
    var activities: orders.OrderActivities = .{ .order_choices = &.{choice.OrderChoiceApple} };
    try env.registerActivityWith(orders.GetOrder, &activities);
    try env.registerActivityAs(orders.OrderApple, failingOrder);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    try testing.expectError(error.WorkflowFailed, env.executeWorkflow(choice.ExclusiveChoiceWorkflow, .{}, arena.allocator(), null));
}
