//! Tests for the synchronous proxy sample, on the dev server: the order workflow waits for
//! requests in real time.

const std = @import("std");
const temporal = @import("temporal");
const sp = @import("synchronous_proxy.zig");

const testing = std.testing;
const Time = temporal.converter.Time;
const WorkflowEnvironment = temporal.testing.WorkflowEnvironment;

/// Runs an UpdateOrderWorkflow and returns its result, or the message it failed with.
fn update(env: *WorkflowEnvironment, arena: std.mem.Allocator, order_id: []const u8, stage: []const u8, value: []const u8) !union(enum) { status: sp.OrderStatus, rejected: []const u8 } {
    var failure: ?temporal.failure.Failure = null;
    if (env.executeWorkflow(sp.UpdateOrderWorkflow, .{ order_id, stage, value }, arena, &failure)) |status| {
        return .{ .status = status };
    } else |err| switch (err) {
        error.WorkflowFailed => return .{ .rejected = temporal.failure.rootCause(&failure.?).message },
        else => return err,
    }
}

test "an order taken one step at a time, asking again after a rejected answer" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(sp.OrderWorkflow);
    try env.registerWorkflow(sp.UpdateOrderWorkflow);
    try env.registerWorkflow(sp.ShippingWorkflow);
    try env.registerActivity(sp.RegisterEmail);
    try env.registerActivity(sp.ValidateSize);
    try env.registerActivity(sp.ValidateColor);
    try env.registerActivity(sp.ScheduleDelivery);
    try env.registerActivity(sp.SendDeliveryEmail);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var order = try env.startWorkflow(sp.OrderWorkflow, .{ .id = "order", .task_queue = "" }, .{});
    defer order.deinit();

    var result = try update(env, a, "order", sp.register_stage, "user@example.com");
    try testing.expectEqualStrings("order", result.status.order_id);
    try testing.expectEqualStrings(sp.size_stage, result.status.stage);

    result = try update(env, a, "order", sp.size_stage, "extra-large");
    try testing.expectEqualStrings("size: extra-large is not valid (small, medium, large)", result.rejected);
    result = try update(env, a, "order", sp.size_stage, "medium");
    try testing.expectEqualStrings(sp.color_stage, result.status.stage);

    result = try update(env, a, "order", sp.color_stage, "green");
    try testing.expectEqualStrings("color: green is not valid (red, blue, black)", result.rejected);
    result = try update(env, a, "order", sp.color_stage, "blue");
    try testing.expectEqualStrings(sp.shipping_stage, result.status.stage);

    try env.getResult(sp.OrderWorkflow, &order, a, null);
}

test "an update to an order that doesn't exist fails" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(sp.UpdateOrderWorkflow);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const result = try update(env, arena.allocator(), "no-such-order", sp.register_stage, "user@example.com");
    try testing.expect(std.mem.indexOf(u8, result.rejected, "not found (workflow ID \"no-such-order\")") != null);
}

const delivery_date: Time = .{ .unix_ns = 1_700_000_000 * std.time.ns_per_s };

const ShippingDoubles = struct {
    fn scheduleDelivery(ctx: *temporal.activity.Context, order: sp.TShirtOrder) !Time {
        _ = ctx;
        _ = order;
        return delivery_date;
    }

    /// Fails without retries on unexpected arguments, so a mismatch fails the workflow.
    fn sendDeliveryEmail(ctx: *temporal.activity.Context, order: sp.TShirtOrder, date: Time) !void {
        const expected = std.mem.eql(u8, order.email, "user@example.com") and
            std.mem.eql(u8, order.size, "medium") and
            std.mem.eql(u8, order.color, "blue") and
            date.unix_ns == delivery_date.unix_ns;
        if (!expected) return ctx.fail(.{ .message = "unexpected delivery email", .non_retryable = true });
    }
};

test "shipping emails the delivery date it scheduled for the order" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(sp.ShippingWorkflow);
    try env.registerActivityAs(sp.ScheduleDelivery, ShippingDoubles.scheduleDelivery);
    try env.registerActivityAs(sp.SendDeliveryEmail, ShippingDoubles.sendDeliveryEmail);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    try env.executeWorkflow(sp.ShippingWorkflow, .{.{ .email = "user@example.com", .size = "medium", .color = "blue" }}, arena.allocator(), null);
}

test "sizes and colors are validated against the catalog" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const env: temporal.testing.ActivityEnvironment = .init(testing.io);
    try env.executeActivity(sp.ValidateSize, .{"large"}, arena.allocator());
    try testing.expectError(error.ApplicationFailure, env.executeActivity(sp.ValidateSize, .{"huge"}, arena.allocator()));
    try env.executeActivity(sp.ValidateColor, .{"black"}, arena.allocator());
    try testing.expectError(error.ApplicationFailure, env.executeActivity(sp.ValidateColor, .{"purple"}, arena.allocator()));
}
