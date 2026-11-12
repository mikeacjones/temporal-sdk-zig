//! Plays a shopper in one session: a fixed script of cart actions, each sent with
//! update-with-start so the first one creates the session's cart workflow and the rest reuse
//! it. It prints the cart after each action, shows an unknown action being rejected, then
//! checks out and waits for the cart workflow to complete.

const std = @import("std");
const temporal = @import("temporal");
const shoppingcart = @import("workflow.zig");

const Action = struct { action_type: []const u8, item_id: []const u8 = "", rejected: bool = false };

const script = [_]Action{
    .{ .action_type = "add", .item_id = "apple" },
    .{ .action_type = "add", .item_id = "banana" },
    .{ .action_type = "add", .item_id = "apple" },
    .{ .action_type = "remove", .item_id = "banana" },
    .{ .action_type = "list" },
    .{ .action_type = "invalid", .item_id = "apple", .rejected = true },
};

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;
    const arena = init.arena.allocator();

    // The client is a heavyweight object that should be created once per process.
    const options = try temporal.client.loadDefaultOptions(arena);
    var client = temporal.client.Client.connect(gpa, io, options) catch |err| {
        std.log.err("Unable to create client: {t}", .{err});
        std.process.exit(1);
    };
    defer client.close();

    // The session ID is the cart's workflow ID: one cart workflow per session.
    const session_id = try std.fmt.allocPrint(arena, "session-{s}", .{try temporal.client.uuid(arena, io)});

    for (script) |action| {
        var handle = client.updateWithStartWorkflow(shoppingcart.CartWorkflow, .{
            .id = session_id,
            .task_queue = shoppingcart.task_queue,
            // Update-with-start requires a conflict policy. Reusing the running workflow keeps
            // one cart for the whole session.
            .id_conflict_policy = .use_existing,
        }, .{null}, shoppingcart.UpdateCart, .{ action.action_type, action.item_id }, .{}) catch |err| {
            std.log.err("Unable to update-with-start cart {s} with action {s}: {t}", .{ session_id, action.action_type, err });
            std.process.exit(1);
        };
        defer handle.deinit();
        std.log.info("Updated workflow WorkflowID={s} RunID={s}", .{ handle.workflow_id, handle.run_id });

        const cart = handle.get(arena) catch |err| {
            const failure = handle.failure();
            const message = if (failure) |f| f.message else "";
            if (err == error.UpdateFailed and action.rejected) {
                std.log.info("Action {s} rejected: {s}", .{ action.action_type, message });
                continue;
            }
            std.log.err("Action {s} {s} failed: {t} {s}", .{ action.action_type, action.item_id, err, message });
            std.process.exit(1);
        };
        if (action.rejected) {
            std.log.err("Action {s} was accepted, want it rejected by the validator", .{action.action_type});
            std.process.exit(1);
        }
        std.log.info("Cart after {s}{s}{s}: {f}", .{ action.action_type, if (action.item_id.len > 0) " " else "", action.item_id, std.json.fmt(cart.items, .{}) });
    }

    client.signalWorkflow(shoppingcart.Checkout, session_id, "", .{}) catch |err| {
        std.log.err("Unable to signal checkout to cart {s}: {t}", .{ session_id, err });
        std.process.exit(1);
    };
    var run = try client.getWorkflow(void, session_id, "");
    defer run.deinit();
    run.get(arena) catch |err| {
        std.log.err("Unable to get the result of cart {s}: {t}", .{ session_id, err });
        std.process.exit(1);
    };
    std.log.info("Checked out: cart workflow completed", .{});
}
