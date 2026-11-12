//! Synchronous proxy: a T-shirt order taken step by step from a UI. The order workflow runs for
//! the whole order and asks for an email address, a size and a color in turn. For each answer
//! the UI starts a short UpdateOrderWorkflow, which proxies the answer to the order workflow
//! by signal and blocks until the order workflow signals back the next stage, or why the answer
//! was rejected (see proxy.zig). So the UI gets a synchronous answer for each step, while the
//! order's state lives in one workflow. Once the order is complete, a ShippingWorkflow child
//! schedules the delivery and emails its details.

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;
const activity = temporal.activity;
const Time = temporal.converter.Time;
pub const proxy = @import("proxy.zig");

pub const task_queue = "ui-driven";

pub const register_stage = "register";
pub const size_stage = "size";
pub const color_stage = "color";
pub const shipping_stage = "shipping";

pub const tshirt_sizes = [_][]const u8{ "small", "medium", "large" };
pub const tshirt_colors = [_][]const u8{ "red", "blue", "black" };

pub const TShirtOrder = struct {
    email: []const u8 = "",
    size: []const u8 = "",
    color: []const u8 = "",
};

/// The result of an UpdateOrderWorkflow: the order and the stage it has reached.
pub const OrderStatus = struct {
    order_id: []const u8,
    stage: []const u8,
};

const activity_options: workflow.ActivityOptions = .{ .start_to_close_timeout = .fromSeconds(5) };

/// Waits for requests until `Check` accepts one, answering each: a rejected request gets the
/// check's failure message, the accepted one gets `next_stage`. Returns the accepted value.
fn receiveValid(ctx: *workflow.Context, requests: *proxy.RequestChannel, comptime Check: type, next_stage: []const u8) ![]const u8 {
    while (true) {
        const request = try proxy.receiveRequest(ctx, requests);
        const check = ctx.executeActivity(Check, activity_options, .{request.value});
        check.get() catch |err| switch (err) {
            error.ActivityFailed => {
                const message = if (check.failure()) |f| temporal.failure.rootCause(f).message else @errorName(err);
                try proxy.sendErrorResponse(ctx, request.calling_workflow_id, message);
                continue;
            },
            else => return err,
        };
        try proxy.sendResponse(ctx, request.calling_workflow_id, next_stage, "");
        return request.value;
    }
}

/// Takes an order driven by requests from a UI, then ships it.
fn orderWorkflow(ctx: *workflow.Context) !void {
    const requests = try proxy.RequestChannel.init(ctx);
    var order: TShirtOrder = .{};
    order.email = try receiveValid(ctx, requests, RegisterEmail, size_stage);
    order.size = try receiveValid(ctx, requests, ValidateSize, color_stage);
    // Tells the UI the order is pending shipping.
    order.color = try receiveValid(ctx, requests, ValidateColor, shipping_stage);

    try ctx.executeChildWorkflow(ShippingWorkflow, .{}, .{order}).get();
}

pub const OrderWorkflow = workflow.Def("OrderWorkflow", orderWorkflow);

fn shippingWorkflow(ctx: *workflow.Context, order: TShirtOrder) !void {
    const delivery_date = try ctx.executeActivity(ScheduleDelivery, activity_options, .{order}).get();
    try ctx.executeActivity(SendDeliveryEmail, activity_options, .{ order, delivery_date }).get();
}

pub const ShippingWorkflow = workflow.Def("ShippingWorkflow", shippingWorkflow);

/// Sends `value` for `stage` to the order workflow and returns the stage the order moved on to.
/// Fails with the order workflow's message if it rejected the value.
fn updateOrderWorkflow(ctx: *workflow.Context, order_workflow_id: []const u8, stage: []const u8, value: []const u8) !OrderStatus {
    // Listening before the request goes out, so the response can't arrive unhandled.
    const responses = try proxy.ResponseChannel.init(ctx);
    try proxy.sendRequest(ctx, order_workflow_id, stage, value);
    const response = try proxy.receiveResponse(ctx, responses);
    return .{ .order_id = order_workflow_id, .stage = response.key };
}

pub const UpdateOrderWorkflow = workflow.Def("UpdateOrderWorkflow", updateOrderWorkflow);

fn registerEmail(ctx: *activity.Context, email: []const u8) !void {
    ctx.logger().info("activity: registered email", .{ .email = email });
}

/// Fails, without retries, unless `value` is one of `allowed`.
fn validate(ctx: *activity.Context, comptime what: []const u8, comptime failure_type: []const u8, allowed: []const []const u8, value: []const u8) !void {
    for (allowed) |a| if (std.mem.eql(u8, a, value)) return;
    const message = try std.fmt.allocPrint(ctx.allocator(), what ++ ": {s} is not valid ({s})", .{
        value,
        try std.mem.join(ctx.allocator(), ", ", allowed),
    });
    return ctx.fail(.{ .message = message, .type = failure_type, .non_retryable = true });
}

fn validateSize(ctx: *activity.Context, size: []const u8) !void {
    return validate(ctx, "size", "InvalidSize", &tshirt_sizes, size);
}

fn validateColor(ctx: *activity.Context, color: []const u8) !void {
    return validate(ctx, "color", "InvalidColor", &tshirt_colors, color);
}

/// Delivers in two days.
fn scheduleDelivery(ctx: *activity.Context, order: TShirtOrder) !Time {
    const delivery_date: Time = .fromTimestamp(std.Io.Timestamp.now(ctx.io(), .real).addDuration(.fromSeconds(48 * std.time.s_per_hour)));
    var buf: [Time.max_len]u8 = undefined;
    ctx.logger().info("activity: scheduled delivery", .{
        .email = order.email,
        .size = order.size,
        .color = order.color,
        .delivery_date = delivery_date.format(&buf),
    });
    return delivery_date;
}

fn sendDeliveryEmail(ctx: *activity.Context, order: TShirtOrder, delivery_date: Time) !void {
    var buf: [Time.max_len]u8 = undefined;
    ctx.logger().info("email sent", .{
        .to = order.email,
        .size = order.size,
        .color = order.color,
        .scheduled_delivery = delivery_date.format(&buf),
    });
}

pub const RegisterEmail = activity.Def("RegisterEmail", registerEmail);
pub const ValidateSize = activity.Def("ValidateSize", validateSize);
pub const ValidateColor = activity.Def("ValidateColor", validateColor);
pub const ScheduleDelivery = activity.Def("ScheduleDelivery", scheduleDelivery);
pub const SendDeliveryEmail = activity.Def("SendDeliveryEmail", sendDeliveryEmail);
