//! Tests for the mutex sample.

const std = @import("std");
const temporal = @import("temporal");
const mutex = @import("mutex.zig");

const testing = std.testing;
const workflow = temporal.workflow;
const Duration = std.Io.Duration;

const mock_resource_id = "mockResourceID";
const mock_release_lock_channel_name = "mockReleaseLockChannelName";
const mock_sender_workflow_id = "mockSenderWorkflowID";
const unlock_timeout: Duration = .fromSeconds(10 * 60);

const Doubles = struct {
    /// Grants nothing itself: the test sends acquire-lock-event, and "mockID" is a stub
    /// mutex workflow the test starts.
    fn signalWithStart(_: *temporal.activity.Context, _: []const u8, resource_id: []const u8, _: []const u8, _: Duration) !mutex.Execution {
        try testing.expectEqualStrings(mock_resource_id, resource_id);
        return .{ .workflow_id = "mockID", .run_id = "" };
    }

    fn signalWithStartFails(ctx: *temporal.activity.Context, _: []const u8, resource_id: []const u8, _: []const u8, _: Duration) !mutex.Execution {
        try testing.expectEqualStrings(mock_resource_id, resource_id);
        // Non-retryable, so the test doesn't wait out the lock's retries.
        return ctx.fail(.{ .message = "bad-error", .non_retryable = true });
    }
};

const Ack = struct {
    value: ?[]const u8 = null,
    fn set(a: *Ack, value: []const u8) void {
        a.value = value;
    }
    fn received(a: *Ack) bool {
        return a.value != null;
    }
};

/// Stands in for the mutex workflow: returns what the release signal carried.
fn stubMutex(ctx: *workflow.Context) ![]const u8 {
    const ack = try ctx.allocator().create(Ack);
    ack.* = .{};
    try ctx.setSignalHandlerByName(mock_release_lock_channel_name, struct { []const u8 }, ack, Ack.set);
    try ctx.await(ack, Ack.received);
    return ack.value.?;
}
const StubMutex = workflow.Def("StubMutex", stubMutex);

/// Stands in for a workflow waiting for the lock: returns the release signal's name it was
/// granted, after releasing the lock if `release` is set.
fn stubSender(ctx: *workflow.Context, mutex_workflow_id: []const u8, release: bool) ![]const u8 {
    const acquired = try ctx.allocator().create(Ack);
    acquired.* = .{};
    try ctx.setSignalHandler(mutex.AcquireLock, acquired, Ack.set);
    try ctx.await(acquired, Ack.received);
    const unlock: mutex.Unlock = .{
        .execution = .{ .workflow_id = mutex_workflow_id, .run_id = "" },
        .release_lock_channel_name = acquired.value.?,
    };
    if (release) try unlock.release(ctx);
    return acquired.value.?;
}
const StubSender = workflow.Def("StubSender", stubSender);

test "workflow locks the resource, runs its critical section, and releases the lock" {
    const env = try temporal.testing.WorkflowEnvironment.startTimeSkipping(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(mutex.SampleWorkflowWithMutex);
    try env.registerWorkflow(StubMutex);
    try env.registerActivityAs(mutex.SignalWithStartMutexWorkflowActivity, Doubles.signalWithStart);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var stub = try env.startWorkflow(StubMutex, .{ .id = "mockID", .task_queue = "" }, .{});
    defer stub.deinit();
    var run = try env.startWorkflow(mutex.SampleWorkflowWithMutex, .{ .task_queue = "" }, .{mock_resource_id});
    defer run.deinit();
    try env.signalWorkflow(mutex.AcquireLock, run.id, .{mock_release_lock_channel_name});

    try env.getResult(mutex.SampleWorkflowWithMutex, &run, a, null);
    try testing.expectEqualStrings("releaseLock", try env.getResult(StubMutex, &stub, a, null));
}

test "workflow fails with the error from requesting the lock" {
    const env = try temporal.testing.WorkflowEnvironment.startTimeSkipping(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(mutex.SampleWorkflowWithMutex);
    try env.registerActivityAs(mutex.SignalWithStartMutexWorkflowActivity, Doubles.signalWithStartFails);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();

    var failure: ?temporal.failure.Failure = null;
    try testing.expectError(error.WorkflowFailed, env.executeWorkflow(mutex.SampleWorkflowWithMutex, .{mock_resource_id}, arena.allocator(), &failure));
    try testing.expectEqualStrings("bad-error", temporal.failure.rootCause(&failure.?).message);
}

/// Requests the lock for a stub sender, then waits for the stub sender and the mutex
/// workflow. Returns how much server time the mutex workflow took.
fn runMutex(env: *temporal.testing.WorkflowEnvironment, a: std.mem.Allocator, release: bool) !Duration {
    try env.registerWorkflow(mutex.MutexWorkflow);
    try env.registerWorkflow(StubSender);
    const started = try env.now();

    const mutex_id = "mutex:mockNamespace:" ++ mock_resource_id;
    var sender = try env.startWorkflow(StubSender, .{ .id = mock_sender_workflow_id, .task_queue = "" }, .{ mutex_id, release });
    defer sender.deinit();
    var run = try env.signalWithStartWorkflow(mutex.MutexWorkflow, .{ .id = mutex_id, .task_queue = "" }, .{
        "mockNamespace", mock_resource_id, unlock_timeout,
    }, mutex.RequestLock, .{mock_sender_workflow_id});
    defer run.deinit();

    try testing.expectEqualStrings("unlock-event-" ++ mock_sender_workflow_id, try env.getResult(StubSender, &sender, a, null));
    try env.getResult(mutex.MutexWorkflow, &run, a, null);
    return started.durationTo(try env.now());
}

test "mutex workflow grants the lock and ends once it is released" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();

    const elapsed = try runMutex(env, arena.allocator(), true);
    try testing.expect(elapsed.nanoseconds < unlock_timeout.nanoseconds);
}

test "mutex workflow takes the lock back after the unlock timeout" {
    const env = try temporal.testing.WorkflowEnvironment.startTimeSkipping(testing.allocator, testing.io);
    defer env.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();

    const elapsed = try runMutex(env, arena.allocator(), false);
    try testing.expect(elapsed.nanoseconds >= unlock_timeout.nanoseconds);
}

test "mutex workflow skips a sender that has closed" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(mutex.MutexWorkflow);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();

    var run = try env.signalWithStartWorkflow(mutex.MutexWorkflow, .{ .task_queue = "" }, .{
        "mockNamespace", mock_resource_id, unlock_timeout,
    }, mutex.RequestLock, .{"no-such-workflow"});
    defer run.deinit();
    try env.getResult(mutex.MutexWorkflow, &run, arena.allocator(), null);
}
