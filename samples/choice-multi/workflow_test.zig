//! Tests for the choice-multi sample.

const std = @import("std");
const temporal = @import("temporal");
const choice = @import("workflow.zig");
const orders = @import("order_activities.zig");

const testing = std.testing;

test "orders every chosen item" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(choice.MultiChoice);
    var activities: orders.OrderActivities = .{ .order_choices = &.{ choice.OrderChoiceApple, choice.OrderChoiceBanana, choice.OrderChoiceOrange } };
    try env.registerActivitiesWith(&activities, orders.all);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    try env.executeWorkflow(choice.MultiChoice, .{}, arena.allocator(), null);
}
