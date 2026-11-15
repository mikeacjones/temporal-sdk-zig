//! Tests for the saga sample.

const std = @import("std");
const temporal = @import("temporal");
const saga = @import("saga.zig");

const testing = std.testing;

const test_details: saga.TransferDetails = .{
    .amount = 1.00,
    .from_account = "001-001",
    .to_account = "002-002",
    .reference_id = "1234",
};

// Records the activities the workflow ran, in order, and checks each got the transfer details.
const Recorder = struct {
    var mutex: std.Io.Mutex = .init;
    var calls: [16][]const u8 = undefined;
    var count: usize = 0;

    fn reset() void {
        count = 0;
    }

    fn recorded() []const []const u8 {
        return calls[0..count];
    }

    fn record(ctx: *temporal.activity.Context, name: []const u8, details: saga.TransferDetails) !void {
        try testing.expectEqual(test_details.amount, details.amount);
        try testing.expectEqualStrings(test_details.from_account, details.from_account);
        try testing.expectEqualStrings(test_details.to_account, details.to_account);
        try testing.expectEqualStrings(test_details.reference_id, details.reference_id);
        try mutex.lock(ctx.io());
        defer mutex.unlock(ctx.io());
        if (count < calls.len) calls[count] = name;
        count += 1;
    }

    fn withdraw(ctx: *temporal.activity.Context, details: saga.TransferDetails) !void {
        try record(ctx, "Withdraw", details);
    }
    fn withdrawCompensation(ctx: *temporal.activity.Context, details: saga.TransferDetails) !void {
        try record(ctx, "WithdrawCompensation", details);
    }
    fn deposit(ctx: *temporal.activity.Context, details: saga.TransferDetails) !void {
        try record(ctx, "Deposit", details);
    }
    fn depositCompensation(ctx: *temporal.activity.Context, details: saga.TransferDetails) !void {
        try record(ctx, "DepositCompensation", details);
    }
    fn stepWithError(ctx: *temporal.activity.Context, details: saga.TransferDetails) !void {
        try record(ctx, "StepWithError", details);
        return ctx.fail(.{ .message = "some error" });
    }
    fn stepWithoutError(ctx: *temporal.activity.Context, details: saga.TransferDetails) !void {
        try record(ctx, "StepWithError", details);
    }
    fn depositCompensationFails(ctx: *temporal.activity.Context, details: saga.TransferDetails) !void {
        try record(ctx, "DepositCompensation", details);
        return ctx.fail(.{ .message = "compensation failed", .non_retryable = true });
    }
    fn withdrawRefused(ctx: *temporal.activity.Context, details: saga.TransferDetails) !void {
        try record(ctx, "Withdraw", details);
        return ctx.fail(.{ .message = "insufficient funds", .non_retryable = true });
    }
};

fn registerDoubles(env: *temporal.testing.WorkflowEnvironment) !void {
    try env.registerWorkflow(saga.TransferMoney);
    try env.registerActivityAs(saga.WithdrawCompensation, Recorder.withdrawCompensation);
    try env.registerActivityAs(saga.Deposit, Recorder.deposit);
}

test "a failing step compensates the deposit, then the withdrawal" {
    Recorder.reset();
    const env = try temporal.testing.WorkflowEnvironment.startTimeSkipping(testing.allocator, testing.io);
    defer env.deinit();
    try registerDoubles(env);
    try env.registerActivityAs(saga.DepositCompensation, Recorder.depositCompensation);
    try env.registerActivityAs(saga.Withdraw, Recorder.withdraw);
    try env.registerActivityAs(saga.StepWithError, Recorder.stepWithError);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var failure: ?temporal.failure.Failure = null;
    try testing.expectError(error.WorkflowFailed, env.executeWorkflow(saga.TransferMoney, .{test_details}, arena.allocator(), &failure));
    // The workflow fails with the step's failure, after its three attempts.
    try testing.expectEqualStrings("some error", temporal.failure.rootCause(&failure.?).message);
    try testing.expectEqualDeep(@as([]const []const u8, &.{
        "Withdraw",
        "Deposit",
        "StepWithError",
        "StepWithError",
        "StepWithError",
        "DepositCompensation",
        "WithdrawCompensation",
    }), Recorder.recorded());
}

test "a compensation that fails doesn't replace the failed step's failure" {
    Recorder.reset();
    const env = try temporal.testing.WorkflowEnvironment.startTimeSkipping(testing.allocator, testing.io);
    defer env.deinit();
    try registerDoubles(env);
    try env.registerActivityAs(saga.DepositCompensation, Recorder.depositCompensationFails);
    try env.registerActivityAs(saga.Withdraw, Recorder.withdraw);
    try env.registerActivityAs(saga.StepWithError, Recorder.stepWithError);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var failure: ?temporal.failure.Failure = null;
    try testing.expectError(error.WorkflowFailed, env.executeWorkflow(saga.TransferMoney, .{test_details}, arena.allocator(), &failure));
    try testing.expectEqualStrings("some error", temporal.failure.rootCause(&failure.?).message);
    try testing.expectEqualStrings("StepWithError", failure.?.failure_info.?.activity_failure_info.activity_type.?.name);
    // The failed compensation is logged, and the next one still runs.
    try testing.expectEqualDeep(@as([]const []const u8, &.{
        "Withdraw",
        "Deposit",
        "StepWithError",
        "StepWithError",
        "StepWithError",
        "DepositCompensation",
        "WithdrawCompensation",
    }), Recorder.recorded());
}

test "a transfer whose steps all succeed runs no compensation" {
    Recorder.reset();
    const env = try temporal.testing.WorkflowEnvironment.startTimeSkipping(testing.allocator, testing.io);
    defer env.deinit();
    try registerDoubles(env);
    try env.registerActivityAs(saga.DepositCompensation, Recorder.depositCompensation);
    try env.registerActivityAs(saga.Withdraw, Recorder.withdraw);
    try env.registerActivityAs(saga.StepWithError, Recorder.stepWithoutError);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    try env.executeWorkflow(saga.TransferMoney, .{test_details}, arena.allocator(), null);
    try testing.expectEqualDeep(@as([]const []const u8, &.{ "Withdraw", "Deposit", "StepWithError" }), Recorder.recorded());
}

test "a failed withdrawal has nothing to compensate" {
    Recorder.reset();
    const env = try temporal.testing.WorkflowEnvironment.startTimeSkipping(testing.allocator, testing.io);
    defer env.deinit();
    try registerDoubles(env);
    try env.registerActivityAs(saga.DepositCompensation, Recorder.depositCompensation);
    try env.registerActivityAs(saga.Withdraw, Recorder.withdrawRefused);
    try env.registerActivityAs(saga.StepWithError, Recorder.stepWithError);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var failure: ?temporal.failure.Failure = null;
    try testing.expectError(error.WorkflowFailed, env.executeWorkflow(saga.TransferMoney, .{test_details}, arena.allocator(), &failure));
    try testing.expectEqualStrings("insufficient funds", temporal.failure.rootCause(&failure.?).message);
    try testing.expectEqualDeep(@as([]const []const u8, &.{"Withdraw"}), Recorder.recorded());
}

test "the real StepWithError always fails" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var env: temporal.testing.ActivityEnvironment = .init(testing.io);
    try testing.expectError(error.ApplicationFailure, env.executeActivity(saga.StepWithError, .{test_details}, arena.allocator()));
}

// A step that fails in the same workflow task as a cancel request: the step's get returns
// error.Canceled, and the workflow is canceled, as the recorded history ends.
const failed_step_then_canceled_history =
    \\{"events": [{"eventId": "1", "eventTime": "2026-11-15T10:00:00Z", "eventType": "EVENT_TYPE_WORKFLOW_EXECUTION_STARTED", "workflowExecutionStartedEventAttributes": {"workflowType": {"name": "TransferMoney"}, "taskQueue": {"name": "TRANSFER_MONEY_TASK_QUEUE", "kind": "TASK_QUEUE_KIND_NORMAL"}, "input": {"payloads": [{"metadata": {"encoding": "anNvbi9wbGFpbg=="}, "data": "eyJhbW91bnQiOjEsImZyb21fYWNjb3VudCI6IjAwMS0wMDEiLCJ0b19hY2NvdW50IjoiMDAyLTAwMiIsInJlZmVyZW5jZV9pZCI6IjEyMzQifQ=="}]}, "workflowTaskTimeout": "10s", "originalExecutionRunId": "r1", "firstExecutionRunId": "r1", "attempt": 1, "workflowId": "saga-cancel"}}, {"eventId": "2", "eventTime": "2026-11-15T10:00:00Z", "eventType": "EVENT_TYPE_WORKFLOW_TASK_SCHEDULED", "workflowTaskScheduledEventAttributes": {"taskQueue": {"name": "TRANSFER_MONEY_TASK_QUEUE", "kind": "TASK_QUEUE_KIND_NORMAL"}, "startToCloseTimeout": "10s", "attempt": 1}}, {"eventId": "3", "eventTime": "2026-11-15T10:00:00Z", "eventType": "EVENT_TYPE_WORKFLOW_TASK_STARTED", "workflowTaskStartedEventAttributes": {"scheduledEventId": "2"}}, {"eventId": "4", "eventTime": "2026-11-15T10:00:00Z", "eventType": "EVENT_TYPE_WORKFLOW_TASK_COMPLETED", "workflowTaskCompletedEventAttributes": {"scheduledEventId": "2", "startedEventId": "3"}}, {"eventId": "5", "eventTime": "2026-11-15T10:00:00Z", "eventType": "EVENT_TYPE_ACTIVITY_TASK_SCHEDULED", "activityTaskScheduledEventAttributes": {"activityId": "1", "activityType": {"name": "Withdraw"}, "taskQueue": {"name": "TRANSFER_MONEY_TASK_QUEUE", "kind": "TASK_QUEUE_KIND_NORMAL"}, "input": {"payloads": [{"metadata": {"encoding": "anNvbi9wbGFpbg=="}, "data": "eyJhbW91bnQiOjEsImZyb21fYWNjb3VudCI6IjAwMS0wMDEiLCJ0b19hY2NvdW50IjoiMDAyLTAwMiIsInJlZmVyZW5jZV9pZCI6IjEyMzQifQ=="}]}, "startToCloseTimeout": "60s", "workflowTaskCompletedEventId": "4"}}, {"eventId": "6", "eventTime": "2026-11-15T10:00:00Z", "eventType": "EVENT_TYPE_ACTIVITY_TASK_STARTED", "activityTaskStartedEventAttributes": {"scheduledEventId": "5", "attempt": 1}}, {"eventId": "7", "eventTime": "2026-11-15T10:00:00Z", "eventType": "EVENT_TYPE_ACTIVITY_TASK_FAILED", "activityTaskFailedEventAttributes": {"failure": {"message": "insufficient funds", "applicationFailureInfo": {"nonRetryable": true}}, "scheduledEventId": "5", "startedEventId": "6", "retryState": "RETRY_STATE_NON_RETRYABLE_FAILURE"}}, {"eventId": "8", "eventTime": "2026-11-15T10:00:00Z", "eventType": "EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED", "workflowExecutionCancelRequestedEventAttributes": {"identity": "test"}}, {"eventId": "9", "eventTime": "2026-11-15T10:00:00Z", "eventType": "EVENT_TYPE_WORKFLOW_TASK_SCHEDULED", "workflowTaskScheduledEventAttributes": {"taskQueue": {"name": "TRANSFER_MONEY_TASK_QUEUE", "kind": "TASK_QUEUE_KIND_NORMAL"}, "startToCloseTimeout": "10s", "attempt": 1}}, {"eventId": "10", "eventTime": "2026-11-15T10:00:00Z", "eventType": "EVENT_TYPE_WORKFLOW_TASK_STARTED", "workflowTaskStartedEventAttributes": {"scheduledEventId": "9"}}, {"eventId": "11", "eventTime": "2026-11-15T10:00:00Z", "eventType": "EVENT_TYPE_WORKFLOW_TASK_COMPLETED", "workflowTaskCompletedEventAttributes": {"scheduledEventId": "9", "startedEventId": "10"}}, {"eventId": "12", "eventTime": "2026-11-15T10:00:00Z", "eventType": "EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED", "workflowExecutionCanceledEventAttributes": {"workflowTaskCompletedEventId": "11"}}]}
;

test "a transfer canceled as its step fails is canceled, not failed with the step's failure" {
    var replayer: temporal.testing.WorkflowReplayer = .init(testing.allocator, testing.io);
    defer replayer.deinit();
    try replayer.registerWorkflow(saga.TransferMoney);
    try replayer.replayHistoryJson(failed_step_then_canceled_history);
}
