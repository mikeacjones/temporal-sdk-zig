//! The order activities of the choice-exclusive sample.

const std = @import("std");
const temporal = @import("temporal");
const activity = temporal.activity;

pub const OrderActivities = struct {
    order_choices: []const []const u8,

    pub fn getOrder(a: *OrderActivities, ctx: *activity.Context) ![]const u8 {
        if (a.order_choices.len == 0) return error.NoOrderChoices;
        const source: std.Random.IoSource = .{ .io = ctx.io() };
        const order = a.order_choices[source.interface().uintLessThan(usize, a.order_choices.len)];
        ctx.logger().info("Order is for", .{ .order = order });
        return order;
    }

    pub fn orderApple(_: *OrderActivities, ctx: *activity.Context, choice: []const u8) !void {
        ctx.logger().info("Order choice", .{ .choice = choice });
    }

    pub fn orderBanana(_: *OrderActivities, ctx: *activity.Context, choice: []const u8) !void {
        ctx.logger().info("Order choice", .{ .choice = choice });
    }

    pub fn orderCherry(_: *OrderActivities, ctx: *activity.Context, choice: []const u8) !void {
        ctx.logger().info("Order choice", .{ .choice = choice });
    }

    pub fn orderOrange(_: *OrderActivities, ctx: *activity.Context, choice: []const u8) !void {
        ctx.logger().info("Order choice", .{ .choice = choice });
    }
};

pub const GetOrder = activity.Def("GetOrder", OrderActivities.getOrder);
pub const OrderApple = activity.Def("OrderApple", OrderActivities.orderApple);
pub const OrderBanana = activity.Def("OrderBanana", OrderActivities.orderBanana);
pub const OrderCherry = activity.Def("OrderCherry", OrderActivities.orderCherry);
pub const OrderOrange = activity.Def("OrderOrange", OrderActivities.orderOrange);

/// Every order activity, for registering them together.
pub const all = .{ GetOrder, OrderApple, OrderBanana, OrderCherry, OrderOrange };
