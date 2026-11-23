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

/// A worker interceptor that holds `WaitForDecisionActivity` back before it registers with the
/// expense system: it sets `entered` when an attempt starts and lets it go on once `release`
/// is set. Attempts after the release go on at once.
const HoldRegistration = struct {
    io: std.Io,
    entered: std.Io.Event = .unset,
    release: std.Io.Event = .unset,

    pub fn interceptActivity(h: *HoldRegistration, ctx: *temporal.activity.Context) error{OutOfMemory}!temporal.interceptor.Activity {
        _ = ctx;
        return .{ .inbound = .from(HoldRegistration, h) };
    }

    pub fn execute(h: *HoldRegistration, input: temporal.interceptor.ExecuteActivityInput, next: temporal.interceptor.ActivityInbound.Next) anyerror!temporal.converter.Payload {
        if (std.mem.eql(u8, input.activity_type, expense.WaitForDecisionActivity.activity_name)) {
            h.entered.set(h.io);
            try h.release.wait(h.io);
        }
        return next.execute(input);
    }
};

/// Decides expense `id` with `action` while its `WaitForDecisionActivity` is held back before
/// registering, then releases it, and returns the workflow's result.
fn decideBeforeRegistration(id: []const u8, action: []const u8, arena: std.mem.Allocator) ![]const u8 {
    const io = testing.io;
    var hold: HoldRegistration = .{ .io = io };
    // Set on every way out, so a failing test doesn't leave the activity waiting.
    defer hold.release.set(io);
    const interceptors = [_]temporal.interceptor.Worker{.from(HoldRegistration, &hold)};
    const env = try temporal.testing.WorkflowEnvironment.startWithOptions(testing.allocator, io, .{ .worker_interceptors = &interceptors });
    defer env.deinit();

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

    // The execution timeout makes a stranded workflow fail the test instead of hanging it.
    var run = try env.startWorkflow(expense.SampleExpenseWorkflow, .{ .task_queue = "", .execution_timeout = .fromSeconds(60) }, .{id});
    defer run.deinit();
    // Bounded like the run, so a workflow that never reaches the activity fails the test.
    try hold.entered.waitTimeout(io, .{ .duration = .{ .raw = .fromSeconds(60), .clock = .awake } });
    try testing.expectEqualStrings("SUCCEED", (try system.handle(arena, try std.fmt.allocPrint(arena, "/action?is_api_call=true&type={s}&id={s}", .{ action, id }), "")).body);
    hold.release.set(io);
    return env.getResult(expense.SampleExpenseWorkflow, &run, arena, null);
}

test "an expense approved before the activity registers is paid" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    try testing.expectEqualStrings("COMPLETED", try decideBeforeRegistration("early-approved", "approve", arena.allocator()));
}

test "an expense rejected before the activity registers completes the workflow" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    try testing.expectEqualStrings("", try decideBeforeRegistration("early-rejected", "reject", arena.allocator()));
}

test "creating an expense again from the same workflow succeeds, as a retry after a lost answer" {
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

    var activity_env: temporal.testing.ActivityEnvironment = .init(io);
    activity_env.info.workflow_id = "expense-workflow";
    // The first attempt creates the expense; its answer is dropped, as if the worker had
    // died or the timeout had passed, and the retry creates it again.
    try activity_env.executeActivityWith(expense.CreateExpenseActivity, &activities, .{"retried-expense"}, arena);
    activity_env.info.attempt = 2;
    try activity_env.executeActivityWith(expense.CreateExpenseActivity, &activities, .{"retried-expense"}, arena);
    try testing.expectEqual(1, system.expenses.count());
    try testing.expectEqualStrings("expense-workflow", system.expenses.get("retried-expense").?.owner);

    // Another workflow can't take the expense over.
    activity_env.info.workflow_id = "another-workflow";
    try testing.expectError(error.ApplicationFailure, activity_env.executeActivityWith(expense.CreateExpenseActivity, &activities, .{"retried-expense"}, arena));
    try testing.expectEqual(1, system.expenses.count());
}

test "the activities percent-encode the expense ID in their requests" {
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

    const id = "a<b\"c&d e#f+g";
    var activity_env: temporal.testing.ActivityEnvironment = .init(io);
    try activity_env.executeActivityWith(expense.CreateExpenseActivity, &activities, .{id}, arena);
    try testing.expectEqualStrings("CREATED", system.expenses.get(id).?.state.label());
    _ = try system.handle(arena, "/action?is_api_call=true&type=approve&id=a%3Cb%22c%26d%20e%23f%2Bg", "");
    // Decided before it registers, the activity returns the decision at once.
    try testing.expectEqualStrings("APPROVED", try activity_env.executeActivityWith(expense.WaitForDecisionActivity, &activities, .{id}, arena));
    try activity_env.executeActivityWith(expense.PaymentActivity, &activities, .{id}, arena);
    try testing.expectEqualStrings("COMPLETED", system.expenses.get(id).?.state.label());
    try testing.expectEqual(1, system.expenses.count());
}
