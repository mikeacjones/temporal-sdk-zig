//! Shopping cart: one long-running workflow per shopping session holds the cart. Clients change
//! it with the "shopping-cart" update, sent with update-with-start so that the first action
//! creates the cart and later ones reuse it; the update's validator rejects unknown actions
//! before they reach history. The "checkout" signal ends the workflow. When the server suggests
//! continuing as new, the workflow lets running handlers finish and continues as new with the
//! cart.

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;

pub const task_queue = "shopping-cart-tq";

pub const CartState = struct {
    /// Item ID to quantity, in the order items were first added.
    items: std.json.ArrayHashMap(i64) = .{},
};

/// Changes the cart and returns it: `action_type` is "add" or "remove" (with an `item_id`), or
/// "list" (without one).
pub const UpdateCart = workflow.UpdateDef("shopping-cart", fn (action_type: []const u8, item_id: []const u8) CartState);

pub const Checkout = workflow.SignalDef("checkout", fn () void);

/// The handlers' state, in `ctx.allocator()` memory so it outlives the coroutines that use it.
const Cart = struct {
    ctx: *workflow.Context,
    state: CartState,
    checked_out: bool = false,

    fn update(c: *Cart, action_type: []const u8, item_id: []const u8) !CartState {
        const log = c.ctx.logger();
        log.info("Received update", .{ .action_type = action_type, .item_id = item_id });
        const items = &c.state.items.map;
        if (std.mem.eql(u8, action_type, "add")) {
            const entry = try items.getOrPut(c.ctx.allocator(), item_id);
            if (!entry.found_existing) entry.value_ptr.* = 0;
            entry.value_ptr.* += 1;
        } else if (std.mem.eql(u8, action_type, "remove")) {
            if (items.getPtr(item_id)) |quantity| {
                quantity.* -= 1;
                // Ordered, so the cart lists items the same way on every run and replay.
                if (quantity.* <= 0) _ = items.orderedRemove(item_id);
            }
        } else if (!std.mem.eql(u8, action_type, "list")) {
            // The validator rejects these, and it doesn't run on replay, where every update in
            // history was accepted.
            log.err("Unsupported action type", .{ .action_type = action_type });
        }
        return c.state;
    }

    fn validate(c: *Cart, action_type: []const u8, item_id: []const u8) error{ ItemIdRequired, UnsupportedActionType }!void {
        if (std.mem.eql(u8, action_type, "add") or std.mem.eql(u8, action_type, "remove")) {
            if (item_id.len == 0) return error.ItemIdRequired;
        } else if (std.mem.eql(u8, action_type, "list")) {
            if (item_id.len > 0) c.ctx.logger().warn("item_id is not needed for the \"list\" action", .{ .item_id = item_id });
        } else {
            return error.UnsupportedActionType;
        }
    }

    fn checkout(c: *Cart) void {
        c.checked_out = true;
    }

    fn checkedOutOrContinueAsNewSuggested(c: *Cart) bool {
        return c.checked_out or c.ctx.continueAsNewSuggested();
    }
};

/// `cart` is null for a new session, and the cart so far for a run that continued as new.
fn cartWorkflow(ctx: *workflow.Context, cart: ?CartState) !void {
    const log = ctx.logger();
    const c = try ctx.allocator().create(Cart);
    c.* = .{ .ctx = ctx, .state = cart orelse .{} };
    try ctx.setUpdateHandlerWithValidator(UpdateCart, c, Cart.update, Cart.validate);
    try ctx.setSignalHandler(Checkout, c, Cart.checkout);

    try ctx.await(c, Cart.checkedOutOrContinueAsNewSuggested);
    if (!c.checked_out) {
        // Updates accepted in this run must finish before it ends, so their callers get a
        // result and their changes carry over.
        try ctx.await(ctx, workflow.Context.allHandlersFinished);
        log.info("Continuing as new", .{});
        return ctx.continueAsNew(CartWorkflow, .{}, .{c.state});
    }
    log.info("User has checked out, cart workflow exiting", .{});
}

pub const CartWorkflow = workflow.Def("CartWorkflow", cartWorkflow);
