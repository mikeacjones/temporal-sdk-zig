const std = @import("std");
const temporal = @import("temporal");
const saga = @import("saga.zig");

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;

    // The client and worker are heavyweight objects that should be created once per process.
    const options = try temporal.client.loadDefaultOptions(init.arena.allocator());
    var client = temporal.client.Client.connect(gpa, io, options) catch |err| {
        std.log.err("Unable to create client: {t}", .{err});
        std.process.exit(1);
    };
    defer client.close();

    // This worker hosts both the workflow and its activities.
    var w = try temporal.worker.Worker.init(gpa, io, &client, saga.task_queue, .{});
    defer w.deinit();

    try w.registerWorkflow(saga.TransferMoney);
    try w.registerActivity(saga.Withdraw);
    try w.registerActivity(saga.WithdrawCompensation);
    try w.registerActivity(saga.Deposit);
    try w.registerActivity(saga.DepositCompensation);
    try w.registerActivity(saga.StepWithError);

    w.runUntilSignal() catch |err| {
        std.log.err("Unable to start worker: {t}", .{err});
        std.process.exit(1);
    };
}
