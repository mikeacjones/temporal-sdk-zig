//! Starts a transaction workflow and sends it the "early-return" update in one call, prints
//! the initialized transaction as soon as the update returns, then waits for the workflow to
//! complete the transaction.

const std = @import("std");
const temporal = @import("temporal");
const early_return = @import("early_return.zig");

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

    const request: early_return.TransactionRequest = .{ .source_account = "Bob", .target_account = "Alice", .amount = 100 };

    var handle = client.updateWithStartWorkflow(early_return.Workflow, .{
        .id = try std.fmt.allocPrint(arena, "early-return-workflow-ID-{s}", .{try temporal.client.uuid(arena, io)}),
        .task_queue = early_return.task_queue,
        // Update-with-start requires a conflict policy. The ID is new, so a running workflow
        // with it would be an error.
        .id_conflict_policy = .fail,
    }, .{request}, early_return.EarlyReturn, .{}, .{}) catch |err| {
        std.log.err("Error issuing update-with-start: {t}", .{err});
        std.process.exit(1);
    };
    defer handle.deinit();
    std.log.info("Started workflow WorkflowID={s} RunID={s}", .{ handle.workflow_id, handle.run_id });

    const initialized = handle.get(arena) catch |err| {
        // The workflow keeps running and cancels the transaction.
        const message = if (handle.failure()) |f| temporal.failure.rootCause(f).message else "";
        std.log.err("Error obtaining update result: {t} {s}", .{ err, message });
        std.process.exit(1);
    };
    std.log.info("Transaction initialized successfully WorkflowID={s} RunID={s} TxID={s}", .{ handle.workflow_id, handle.run_id, initialized.id });

    // The workflow keeps running and completes the transaction.
    var run = try client.getWorkflow(early_return.Transaction, handle.workflow_id, handle.run_id);
    defer run.deinit();
    const completed = run.get(arena) catch |err| {
        std.log.err("Unable to get workflow result: {t}", .{err});
        std.process.exit(1);
    };
    std.log.info("Transaction completed TxID={s}", .{completed.id});
}
