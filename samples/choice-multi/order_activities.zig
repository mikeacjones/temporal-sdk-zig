//! The order activities of the choice-multi sample.

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

    /// A random selection of the order choices (at least one).
    pub fn getBasketOrder(a: *OrderActivities, ctx: *activity.Context) ![]const []const u8 {
        if (a.order_choices.len == 0) return error.NoOrderChoices;
        const source: std.Random.IoSource = .{ .io = ctx.io() };
        const rand = source.interface();
        var basket: std.ArrayList([]const u8) = .empty;
        for (a.order_choices) |item| {
            // Some random decision.
            if (rand.float(f32) <= 0.65) try basket.append(ctx.allocator(), item);
        }
        if (basket.items.len == 0) {
            try basket.append(ctx.allocator(), a.order_choices[rand.uintLessThan(usize, a.order_choices.len)]);
        }
        ctx.logger().info("Get basket order.", .{ .orders = basket.items });
        return basket.items;
    }

    pub fn orderApple(_: *OrderActivities, ctx: *activity.Context, choice: []const u8) !void {
        ctx.logger().info("Order choice-multi", .{ .choice = choice });
    }

    pub fn orderBanana(_: *OrderActivities, ctx: *activity.Context, choice: []const u8) !void {
        ctx.logger().info("Order choice-multi", .{ .choice = choice });
    }

    pub fn orderCherry(_: *OrderActivities, ctx: *activity.Context, choice: []const u8) !void {
        ctx.logger().info("Order choice-multi", .{ .choice = choice });
    }

    pub fn orderOrange(_: *OrderActivities, ctx: *activity.Context, choice: []const u8) !void {
        ctx.logger().info("Order choice-multi", .{ .choice = choice });
    }
};

pub const GetOrder = activity.Def("GetOrder", OrderActivities.getOrder);
pub const GetBasketOrder = activity.Def("GetBasketOrder", OrderActivities.getBasketOrder);
pub const OrderApple = activity.Def("OrderApple", OrderActivities.orderApple);
pub const OrderBanana = activity.Def("OrderBanana", OrderActivities.orderBanana);
pub const OrderCherry = activity.Def("OrderCherry", OrderActivities.orderCherry);
pub const OrderOrange = activity.Def("OrderOrange", OrderActivities.orderOrange);

/// Every order activity, for registering them together.
pub const all = .{ GetOrder, GetBasketOrder, OrderApple, OrderBanana, OrderCherry, OrderOrange };
