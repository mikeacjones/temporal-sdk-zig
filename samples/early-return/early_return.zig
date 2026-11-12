//! Early return: a workflow processes a transaction in two phases. It first initializes the
//! transaction with a local activity, then completes it, or cancels it when initialization
//! failed, with a regular activity.
//!
//! A client starts the workflow and sends it the "early-return" update in one call
//! (update-with-start). The update returns as soon as initialization is done, with the
//! initialized transaction or the initialization's failure, while the workflow goes on to
//! complete or cancel the transaction on its own.

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;
const activity = temporal.activity;

pub const task_queue = "early-return-tq";

pub const TransactionRequest = struct {
    source_account: []const u8,
    target_account: []const u8,
    /// In cents.
    amount: i64,
};

pub const Transaction = struct {
    id: []const u8,
    status: []const u8,
};

/// Returns the initialized transaction, or rejects with the initialization's failure.
pub const EarlyReturn = workflow.UpdateDef("early-return", fn () Transaction);

fn initTransaction(ctx: *activity.Context, request: TransactionRequest) !Transaction {
    if (request.amount <= 0) {
        // Retrying can't fix the amount, so the transaction is canceled at once.
        return ctx.fail(.{
            .message = try std.fmt.allocPrint(ctx.allocator(), "invalid Amount: {d}", .{request.amount}),
            .type = "InvalidAmount",
            .non_retryable = true,
        });
    }
    try ctx.io().sleep(.fromMilliseconds(500), .awake);
    ctx.logger().info("Transaction initialized", .{});
    return .{ .id = try temporal.client.uuid(ctx.allocator(), ctx.io()), .status = "initialized" };
}

fn cancelTransaction(ctx: *activity.Context, transaction: ?Transaction) !void {
    _ = transaction;
    try ctx.io().sleep(.fromSeconds(1), .awake);
    ctx.logger().info("Transaction cancelled", .{});
}

fn completeTransaction(ctx: *activity.Context, transaction: Transaction) !void {
    _ = transaction;
    try ctx.io().sleep(.fromSeconds(1), .awake);
    ctx.logger().info("Transaction completed", .{});
}

pub const Init = activity.Def("Init", initTransaction);
pub const CancelTransaction = activity.Def("CancelTransaction", cancelTransaction);
pub const CompleteTransaction = activity.Def("CompleteTransaction", completeTransaction);

/// Shared by the workflow function and the update handler, in `ctx.allocator()` memory so it
/// outlives the handler's coroutine.
const State = struct {
    ctx: *workflow.Context,
    /// Set once initialization has resolved.
    init: ?workflow.ActivityFuture(Transaction) = null,

    fn initDone(s: *State) bool {
        return s.init != null;
    }

    fn earlyReturn(s: *State) !Transaction {
        try s.ctx.await(s, initDone);
        // The future has resolved, so `get` doesn't block. Its error rejects the update with
        // the initialization's failure.
        return s.init.?.get();
    }
};

fn transactionWorkflow(ctx: *workflow.Context, request: TransactionRequest) !Transaction {
    const log = ctx.logger();
    const state = try ctx.allocator().create(State);
    state.* = .{ .ctx = ctx };
    try ctx.setUpdateHandler(EarlyReturn, state, State.earlyReturn);

    // Phase 1: initialize the transaction. A local activity saves a server round trip, and its
    // short timeout keeps the update's caller from waiting long.
    const init = ctx.executeLocalActivity(Init, .{ .schedule_to_close_timeout = .fromSeconds(5) }, .{request});
    const initialized = init.get();
    state.init = init;

    // Phase 2: complete or cancel the transaction while the update returns its result.
    const options: workflow.ActivityOptions = .{ .start_to_close_timeout = .fromSeconds(30) };
    const transaction = initialized catch |init_err| {
        const reason = if (init.failure()) |f| temporal.failure.rootCause(f).message else @errorName(init_err);
        log.warn("Cancelling transaction due to init error", .{ .err = reason });
        try ctx.executeActivity(CancelTransaction, options, .{null}).get();
        // Fails the workflow with the initialization's failure.
        return init.get();
    };

    log.info("Completing transaction", .{});
    try ctx.executeActivity(CompleteTransaction, options, .{transaction}).get();
    log.info("Transaction completed successfully", .{ .id = transaction.id });
    return transaction;
}

pub const Workflow = workflow.Def("Workflow", transactionWorkflow);
