//! Tests for the early-return sample, on the dev server. Each starts the workflow with
//! update-with-start, checks the update's early result, then the workflow's own outcome. Init
//! is the real activity; completing and cancelling the transaction are test doubles.

const std = @import("std");
const temporal = @import("temporal");
const early_return = @import("early_return.zig");

const testing = std.testing;
const WorkflowEnvironment = temporal.testing.WorkflowEnvironment;

const valid: early_return.TransactionRequest = .{ .source_account = "Bob", .target_account = "Alice", .amount = 100 };
const invalid: early_return.TransactionRequest = .{ .source_account = "Bob", .target_account = "Alice", .amount = -1 };

const Doubles = struct {
    fn complete(ctx: *temporal.activity.Context, transaction: early_return.Transaction) !void {
        _ = ctx;
        try testing.expect(transaction.id.len > 0);
    }
    fn cancel(ctx: *temporal.activity.Context, transaction: ?early_return.Transaction) !void {
        _ = ctx;
        try testing.expectEqual(null, transaction);
    }
    // Non-retryable, so the workflow fails at once instead of retrying the activity.
    fn completeCrashes(ctx: *temporal.activity.Context, transaction: early_return.Transaction) !void {
        _ = transaction;
        return ctx.fail(.{ .message = "crash", .non_retryable = true });
    }
    fn cancelCrashes(ctx: *temporal.activity.Context, transaction: ?early_return.Transaction) !void {
        _ = transaction;
        return ctx.fail(.{ .message = "crash", .non_retryable = true });
    }
};

fn startEnv(comptime complete: anytype, comptime cancel: anytype) !*WorkflowEnvironment {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    errdefer env.deinit();
    try env.registerWorkflow(early_return.Workflow);
    try env.registerActivity(early_return.Init);
    try env.registerActivityAs(early_return.CompleteTransaction, complete);
    try env.registerActivityAs(early_return.CancelTransaction, cancel);
    return env;
}

fn updateWithStart(env: *WorkflowEnvironment, request: early_return.TransactionRequest) !temporal.client.WorkflowUpdateHandle(early_return.Transaction) {
    return env.updateWithStartWorkflow(early_return.Workflow, .{
        .task_queue = "",
        .id_conflict_policy = .fail,
    }, .{request}, early_return.EarlyReturn, .{}, .{});
}

test "the update returns the initialized transaction and the workflow completes it" {
    const env = try startEnv(Doubles.complete, Doubles.cancel);
    defer env.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var handle = try updateWithStart(env, valid);
    defer handle.deinit();
    const initialized = try handle.get(a);
    try testing.expect(initialized.id.len > 0);
    try testing.expectEqualStrings("initialized", initialized.status);

    var run = try env.client.getWorkflow(early_return.Transaction, handle.workflow_id, handle.run_id);
    defer run.deinit();
    const result = try env.getResult(early_return.Workflow, &run, a, null);
    try testing.expectEqualStrings(initialized.id, result.id);
}

test "the update returns the initialized transaction and the workflow fails when completing fails" {
    const env = try startEnv(Doubles.completeCrashes, Doubles.cancel);
    defer env.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var handle = try updateWithStart(env, valid);
    defer handle.deinit();
    const initialized = try handle.get(a);
    try testing.expect(initialized.id.len > 0);

    var run = try env.client.getWorkflow(early_return.Transaction, handle.workflow_id, handle.run_id);
    defer run.deinit();
    var failure: ?temporal.Failure = null;
    try testing.expectError(error.WorkflowFailed, env.getResult(early_return.Workflow, &run, a, &failure));
    try testing.expectEqualStrings("crash", temporal.failure.rootCause(&failure.?).message);
}

test "an invalid amount rejects the update and the workflow cancels the transaction" {
    const env = try startEnv(Doubles.complete, Doubles.cancel);
    defer env.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var handle = try updateWithStart(env, invalid);
    defer handle.deinit();
    try testing.expectError(error.UpdateFailed, handle.get(a));
    const update_cause = temporal.failure.rootCause(handle.failure().?);
    try testing.expect(std.mem.indexOf(u8, update_cause.message, "invalid Amount") != null);
    try testing.expectEqualStrings("InvalidAmount", temporal.failure.applicationType(update_cause.*).?);

    var run = try env.client.getWorkflow(early_return.Transaction, handle.workflow_id, handle.run_id);
    defer run.deinit();
    var failure: ?temporal.Failure = null;
    try testing.expectError(error.WorkflowFailed, env.getResult(early_return.Workflow, &run, a, &failure));
    try testing.expect(std.mem.indexOf(u8, temporal.failure.rootCause(&failure.?).message, "invalid Amount") != null);
}

test "an invalid amount rejects the update and the workflow fails when cancelling fails" {
    const env = try startEnv(Doubles.complete, Doubles.cancelCrashes);
    defer env.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var handle = try updateWithStart(env, invalid);
    defer handle.deinit();
    try testing.expectError(error.UpdateFailed, handle.get(a));
    try testing.expect(std.mem.indexOf(u8, temporal.failure.rootCause(handle.failure().?).message, "invalid Amount") != null);

    var run = try env.client.getWorkflow(early_return.Transaction, handle.workflow_id, handle.run_id);
    defer run.deinit();
    var failure: ?temporal.Failure = null;
    try testing.expectError(error.WorkflowFailed, env.getResult(early_return.Workflow, &run, a, &failure));
    try testing.expectEqualStrings("crash", temporal.failure.rootCause(&failure.?).message);
}
