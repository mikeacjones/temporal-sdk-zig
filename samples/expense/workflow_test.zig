//! Tests for the expense workflow: with test doubles for its activities, and with its real
//! activities against an expense system that completes the waiting activity.

const std = @import("std");
const temporal = @import("temporal");
const expense = @import("expense.zig");
const ExpenseSystem = @import("expense_system.zig").ExpenseSystem;

const testing = std.testing;

const Doubles = struct {
    var decision: []const u8 = "APPROVED";
    var paid: bool = false;

    fn createExpense(ctx: *temporal.activity.Context, expense_id: []const u8) !void {
        _ = ctx;
        try testing.expectEqualStrings("test-expense-id", expense_id);
    }
    fn waitForDecision(ctx: *temporal.activity.Context, expense_id: []const u8) ![]const u8 {
        _ = ctx;
        try testing.expectEqualStrings("test-expense-id", expense_id);
        return decision;
    }
    fn payment(ctx: *temporal.activity.Context, expense_id: []const u8) !void {
        _ = ctx;
        try testing.expectEqualStrings("test-expense-id", expense_id);
        paid = true;
    }

    fn start(the_decision: []const u8) !*temporal.testing.WorkflowEnvironment {
        decision = the_decision;
        paid = false;
        const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
        errdefer env.deinit();
        try env.registerWorkflow(expense.SampleExpenseWorkflow);
        try env.registerActivityAs(expense.CreateExpenseActivity, createExpense);
        try env.registerActivityAs(expense.WaitForDecisionActivity, waitForDecision);
        try env.registerActivityAs(expense.PaymentActivity, payment);
        return env;
    }
};

test "an approved expense is paid, with test doubles for the activities" {
    const env = try Doubles.start("APPROVED");
    defer env.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    try testing.expectEqualStrings("COMPLETED", try env.executeWorkflow(expense.SampleExpenseWorkflow, .{"test-expense-id"}, arena.allocator(), null));
    try testing.expect(Doubles.paid);
}

test "a rejected expense is not paid, with test doubles for the activities" {
    const env = try Doubles.start("REJECTED");
    defer env.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    try testing.expectEqualStrings("", try env.executeWorkflow(expense.SampleExpenseWorkflow, .{"test-expense-id"}, arena.allocator(), null));
    try testing.expect(!Doubles.paid);
}

test "the expense system completes the waiting activity with the decision" {
    const io = testing.io;
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, io);
    defer env.deinit();
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var system: ExpenseSystem = .init(testing.allocator, io, &env.client);
    defer system.deinit();
    const address = try std.Io.net.IpAddress.parse("127.0.0.1", 0);
    var listener = try address.listen(io, .{});
    defer listener.deinit(io);
    var serving = try io.concurrent(ExpenseSystem.serve, .{ &system, &listener });
    defer serving.cancel(io) catch {};

    var activities: expense.Activities = .{ .server_url = try std.fmt.allocPrint(arena, "http://127.0.0.1:{d}", .{listener.socket.address.getPort()}) };
    try env.registerWorkflow(expense.SampleExpenseWorkflow);
    try env.registerActivitiesWith(&activities, .{ expense.CreateExpenseActivity, expense.WaitForDecisionActivity, expense.PaymentActivity });

    var approved = try env.startWorkflow(expense.SampleExpenseWorkflow, .{ .task_queue = "" }, .{"approved-expense"});
    defer approved.deinit();
    try system.waitForCallback("approved-expense");
    _ = try system.handle(arena, "/action?type=approve&id=approved-expense", "");
    try testing.expectEqualStrings("COMPLETED", try env.getResult(expense.SampleExpenseWorkflow, &approved, arena, null));
    try testing.expectEqualStrings("COMPLETED", (try system.handle(arena, "/status?id=approved-expense", "")).body);

    var rejected = try env.startWorkflow(expense.SampleExpenseWorkflow, .{ .task_queue = "" }, .{"rejected-expense"});
    defer rejected.deinit();
    try system.waitForCallback("rejected-expense");
    _ = try system.handle(arena, "/action?type=reject&id=rejected-expense", "");
    try testing.expectEqualStrings("", try env.getResult(expense.SampleExpenseWorkflow, &rejected, arena, null));
    try testing.expectEqualStrings("REJECTED", (try system.handle(arena, "/status?id=rejected-expense", "")).body);
}
