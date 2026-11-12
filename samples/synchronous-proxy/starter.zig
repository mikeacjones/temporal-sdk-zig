//! Plays the order UI: starts an order, then answers its prompts one step at a time, each
//! through an UpdateOrderWorkflow that returns the order's next stage or why the answer was
//! rejected. The answers are fixed so the program runs unattended, and the first size and
//! color given are invalid, to show a rejected answer being asked for again. At the end it
//! waits for the order workflow to ship the order.

const std = @import("std");
const temporal = @import("temporal");
const sp = @import("synchronous_proxy.zig");

const Client = temporal.client.Client;
const OrderStatus = sp.OrderStatus;

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;
    const arena = init.arena.allocator();

    const options = try temporal.client.loadDefaultOptions(arena);
    var client = Client.connect(gpa, io, options) catch |err| {
        std.log.err("Unable to create client: {t}", .{err});
        std.process.exit(1);
    };
    defer client.close();

    std.log.info("T-Shirt Order", .{});

    var order = client.executeWorkflow(sp.OrderWorkflow, .{
        .id = try std.fmt.allocPrint(arena, "order_{s}", .{try temporal.client.uuid(arena, io)}),
        .task_queue = sp.task_queue,
    }, .{}) catch |err| {
        std.log.err("Unable to create order: unable to execute order workflow: {t}", .{err});
        std.process.exit(1);
    };
    defer order.deinit();
    var status: OrderStatus = .{ .order_id = order.id, .stage = sp.register_stage };

    status = try askUntilValid(&client, arena, io, status.order_id, sp.register_stage, "Please enter you email address:", "email", &.{"user@example.com"});
    status = try askUntilValid(&client, arena, io, status.order_id, sp.size_stage, "Please enter your requested size:", "size", &.{ "extra-large", "medium" });
    status = try askUntilValid(&client, arena, io, status.order_id, sp.color_stage, "Please enter your required tshirt color:", "color", &.{ "green", "blue" });

    std.log.info("Thanks for your order!", .{});
    std.log.info("You will receive an email with shipping details shortly", .{});

    order.get(arena) catch |err| {
        const message = if (order.failure()) |f| temporal.failure.rootCause(f).message else "";
        std.log.err("Order workflow failed: {t} {s}", .{ err, message });
        std.process.exit(1);
    };
    std.log.info("Order shipped OrderID={s}", .{status.order_id});
}

/// Gives `answers` in turn for `stage` until the order accepts one, and returns the order's
/// new status. Exits if the order rejects them all.
fn askUntilValid(
    client: *Client,
    arena: std.mem.Allocator,
    io: std.Io,
    order_id: []const u8,
    stage: []const u8,
    prompt: []const u8,
    what: []const u8,
    answers: []const []const u8,
) !OrderStatus {
    for (answers) |answer| {
        std.log.info("{s}", .{prompt});
        std.log.info("> {s}", .{answer});
        var run = client.executeWorkflow(sp.UpdateOrderWorkflow, .{
            .id = try std.fmt.allocPrint(arena, "update-order_{s}", .{try temporal.client.uuid(arena, io)}),
            .task_queue = sp.task_queue,
        }, .{ order_id, stage, answer }) catch |err| {
            std.log.err("Unable to execute workflow: {t}", .{err});
            std.process.exit(1);
        };
        defer run.deinit();
        return run.get(arena) catch |err| switch (err) {
            error.WorkflowFailed => {
                const message = if (run.failure()) |f| temporal.failure.rootCause(f).message else "";
                std.log.info("invalid {s}: {s}", .{ what, message });
                continue;
            },
            else => {
                std.log.err("Unable to update order: {t}", .{err});
                std.process.exit(1);
            },
        };
    }
    std.log.err("Order rejected every {s} given", .{what});
    std.process.exit(1);
}
