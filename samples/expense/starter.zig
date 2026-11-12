//! Starts an expense workflow and approves the expense.
//!
//! The starter hosts the expense system (expense_system.zig) in-process, so the worker's
//! activities can reach it without a third program, and it plays the person who approves the
//! expense: once the workflow's activity is waiting for the decision, it presses the
//! system's approve button. Run the expense system program instead to decide by hand.

const std = @import("std");
const temporal = @import("temporal");
const expense = @import("expense.zig");
const ExpenseSystem = @import("expense_system.zig").ExpenseSystem;

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

    var system: ExpenseSystem = .init(gpa, io, &client);
    defer system.deinit();
    const address = try std.Io.net.IpAddress.parse(expense.expense_server_host, expense.expense_server_port);
    var listener = address.listen(io, .{ .reuse_address = true }) catch |err| {
        std.log.err("Unable to start the expense system on {s}:{d}: {t}", .{ expense.expense_server_host, expense.expense_server_port, err });
        std.process.exit(1);
    };
    defer listener.deinit(io);
    var serving = io.concurrent(ExpenseSystem.serve, .{ &system, &listener }) catch |err| {
        std.log.err("Unable to start the expense system: {t}", .{err});
        std.process.exit(1);
    };
    defer serving.cancel(io) catch {};

    const expense_id = try temporal.client.uuid(arena, io);
    var run = client.executeWorkflow(expense.SampleExpenseWorkflow, .{
        .id = try std.fmt.allocPrint(arena, "expense_{s}", .{expense_id}),
        .task_queue = expense.task_queue,
    }, .{expense_id}) catch |err| {
        std.log.err("Unable to execute workflow: {t}", .{err});
        std.process.exit(1);
    };
    defer run.deinit();
    std.log.info("Started workflow WorkflowID={s} RunID={s}", .{ run.id, run.run_id });

    // Approve the expense once the activity waits for the decision, as a person would from
    // the expense list page.
    try system.waitForCallback(expense_id);
    var http: std.http.Client = .{ .allocator = gpa, .io = io };
    defer http.deinit();
    const approve_url = try std.fmt.allocPrint(arena, "http://{s}:{d}/action?type=approve&id={s}", .{ expense.expense_server_host, expense.expense_server_port, expense_id });
    const approval = http.fetch(.{ .location = .{ .url = approve_url } }) catch |err| {
        std.log.err("Unable to approve expense {s}: {t}", .{ expense_id, err });
        std.process.exit(1);
    };
    if (approval.status != .ok) {
        std.log.err("Approving expense {s} answered HTTP {d}", .{ expense_id, @backingInt(approval.status) });
        std.process.exit(1);
    }
    std.log.info("Approved expense {s}", .{expense_id});

    const result = run.get(arena) catch |err| {
        std.log.err("Unable get workflow result: {t}", .{err});
        std.process.exit(1);
    };
    std.log.info("Workflow result: {s}", .{result});
}
