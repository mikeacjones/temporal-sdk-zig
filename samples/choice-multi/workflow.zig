//! Multi-choice: run several activities in parallel, chosen by the result of a previous
//! activity.

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;
const orders = @import("order_activities.zig");

pub const OrderChoiceApple = "apple";
pub const OrderChoiceBanana = "banana";
pub const OrderChoiceCherry = "cherry";
pub const OrderChoiceOrange = "orange";

fn multiChoice(ctx: *workflow.Context) !void {
    const ao: workflow.ActivityOptions = .{ .start_to_close_timeout = .fromSeconds(10) };

    const choices = try ctx.executeActivity(orders.GetBasketOrder, ao, .{}).get();
    const logger = ctx.logger();

    const eql = std.mem.eql;
    var futures: std.ArrayList(workflow.ActivityFuture(void)) = .empty;
    for (choices) |item| {
        const f = if (eql(u8, item, OrderChoiceApple))
            ctx.executeActivity(orders.OrderApple, ao, .{item})
        else if (eql(u8, item, OrderChoiceBanana))
            ctx.executeActivity(orders.OrderBanana, ao, .{item})
        else if (eql(u8, item, OrderChoiceCherry))
            ctx.executeActivity(orders.OrderCherry, ao, .{item})
        else if (eql(u8, item, OrderChoiceOrange))
            ctx.executeActivity(orders.OrderOrange, ao, .{item})
        else {
            logger.err("Unexpected order.", .{ .order = item });
            return ctx.fail(.{ .message = "invalid order choice" });
        };
        try futures.append(ctx.allocator(), f);
    }

    // Wait for every order; a failed order doesn't fail the workflow.
    for (futures.items) |future| future.get() catch {};

    logger.info("Workflow completed.", .{});
}

pub const MultiChoice = workflow.Def("MultiChoice", multiChoice);
