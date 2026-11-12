const std = @import("std");
const temporal = @import("temporal");
const expense = @import("expense.zig");

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

    var w = try temporal.worker.Worker.init(gpa, io, &client, expense.task_queue, .{});
    defer w.deinit();

    try w.registerWorkflow(expense.SampleExpenseWorkflow);
    var activities: expense.Activities = .{
        .server_url = std.fmt.comptimePrint("http://{s}:{d}", .{ expense.expense_server_host, expense.expense_server_port }),
    };
    try w.registerActivitiesWith(&activities, .{ expense.CreateExpenseActivity, expense.WaitForDecisionActivity, expense.PaymentActivity });

    w.runUntilSignal() catch |err| {
        std.log.err("Unable to start worker: {t}", .{err});
        std.process.exit(1);
    };
}
