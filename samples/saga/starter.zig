//! Starts a money transfer and waits for it. Its last step always fails, so the transfer ends
//! failed after the deposit and the withdrawal have been compensated.

const std = @import("std");
const temporal = @import("temporal");
const saga = @import("saga.zig");

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

    const transfer_details: saga.TransferDetails = .{
        .amount = 54.99,
        .from_account = "001-001",
        .to_account = "002-002",
        .reference_id = try temporal.client.uuid(arena, io),
    };
    var run = client.executeWorkflow(saga.TransferMoney, .{
        .id = "transfer-money-workflow",
        .task_queue = saga.task_queue,
    }, .{transfer_details}) catch |err| {
        std.log.err("Error starting TransferMoney workflow: {t}", .{err});
        std.process.exit(1);
    };
    defer run.deinit();
    std.log.info("Transfer of ${d:.2} from account {s} to account {s} is processing. ReferenceID: {s}", .{
        transfer_details.amount,
        transfer_details.from_account,
        transfer_details.to_account,
        transfer_details.reference_id,
    });
    std.log.info("WorkflowID: {s} RunID: {s}", .{ run.id, run.run_id });

    run.get(arena) catch |err| switch (err) {
        error.WorkflowFailed => {
            const message = if (run.failure()) |f| temporal.failure.rootCause(f).message else "";
            std.log.info("Transfer failed and was compensated: {s}", .{message});
            return;
        },
        else => {
            std.log.err("Unable to get workflow result: {t}", .{err});
            std.process.exit(1);
        },
    };
    std.log.info("Transfer completed", .{});
}
