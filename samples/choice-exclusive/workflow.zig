//! Exclusive choice: run one of several activities, chosen by the result of a previous
//! activity.

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;
const orders = @import("order_activities.zig");

pub const OrderChoiceApple = "apple";
pub const OrderChoiceBanana = "banana";
pub const OrderChoiceCherry = "cherry";
pub const OrderChoiceOrange = "orange";

fn exclusiveChoice(ctx: *workflow.Context) !void {
    const ao: workflow.ActivityOptions = .{ .start_to_close_timeout = .fromSeconds(10) };

    // Get order.
    const order_choice = try ctx.executeActivity(orders.GetOrder, ao, .{}).get();

    const logger = ctx.logger();

    // Choose the next activity based on the order result.
    const eql = std.mem.eql;
    if (eql(u8, order_choice, OrderChoiceApple)) {
        try ctx.executeActivity(orders.OrderApple, ao, .{order_choice}).get();
    } else if (eql(u8, order_choice, OrderChoiceBanana)) {
        try ctx.executeActivity(orders.OrderBanana, ao, .{order_choice}).get();
    } else if (eql(u8, order_choice, OrderChoiceCherry)) {
        try ctx.executeActivity(orders.OrderCherry, ao, .{order_choice}).get();
    } else if (eql(u8, order_choice, OrderChoiceOrange)) {
        // An activity can also be called by its name.
        try ctx.executeActivityByName(void, "OrderOrange", ao, .{order_choice}).get();
    } else {
        return ctx.fail(.{ .message = try std.fmt.allocPrint(ctx.allocator(), "unknown order choice: {s}", .{order_choice}) });
    }

    logger.info("Workflow completed.", .{});
}

pub const ExclusiveChoice = workflow.Def("ExclusiveChoice", exclusiveChoice);
