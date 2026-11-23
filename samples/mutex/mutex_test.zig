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

    try testing.expectEqualStrings("unlock-event-" ++ mock_sender_workflow_id ++ "-0", try env.getResult(StubSender, &sender, a, null));
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

/// Tells a `Holder` to release the grant with this index (0 for its first grant).
const ReleaseNow = workflow.SignalDef("release-now", fn (grant: u32) void);
/// How many times a `Holder` has been granted the lock.
const Granted = workflow.QueryDef("granted", fn () u32);
/// The release signal names a `Holder` has been granted, in order.
const GrantNames = workflow.QueryDef("grant-names", fn () []const []const u8);

/// Stands in for a workflow that holds the lock as long as the test says: it records each
/// grant, and releases a grant when the test signals `ReleaseNow`. It ends after `releases`
/// releases.
const Holder = struct {
    ctx: *workflow.Context,
    names: std.ArrayList([]const u8) = .empty,
    /// Grants the test asked to release, not yet released.
    pending: std.ArrayList(u32) = .empty,

    fn acquire(h: *Holder, name: []const u8) !void {
        try h.names.append(h.ctx.allocator(), name);
    }
    fn releaseNow(h: *Holder, grant: u32) !void {
        try h.pending.append(h.ctx.allocator(), grant);
    }
    fn hasPending(h: *Holder) bool {
        return h.pending.items.len > 0;
    }
    fn granted(h: *Holder) u32 {
        return @intCast(h.names.items.len);
    }
    fn grantNames(h: *Holder) []const []const u8 {
        return h.names.items;
    }

    fn run(ctx: *workflow.Context, mutex_workflow_id: []const u8, releases: u32) !void {
        const h = try ctx.allocator().create(Holder);
        h.* = .{ .ctx = ctx };
        try ctx.setSignalHandler(mutex.AcquireLock, h, Holder.acquire);
        try ctx.setSignalHandler(ReleaseNow, h, Holder.releaseNow);
        try ctx.setQueryHandler(Granted, h, Holder.granted);
        try ctx.setQueryHandler(GrantNames, h, Holder.grantNames);
        for (0..releases) |_| {
            try ctx.await(h, Holder.hasPending);
            const grant = h.pending.orderedRemove(0);
            if (grant >= h.names.items.len) return error.GrantNotReceived;
            const unlock: mutex.Unlock = .{
                .execution = .{ .workflow_id = mutex_workflow_id, .run_id = "" },
                .release_lock_channel_name = h.names.items[grant],
            };
            try unlock.release(ctx);
        }
    }
};
const HolderWorkflow = workflow.Def("Holder", Holder.run);

/// Polls `holder_id` until it has been granted the lock `expected` times. Time stays locked.
fn waitForGrants(env: *temporal.testing.WorkflowEnvironment, holder_id: []const u8, expected: u32) !void {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    var granted: u32 = 0;
    for (0..400) |_| {
        granted = try env.queryWorkflow(Granted, scratch.allocator(), holder_id, .{});
        if (granted == expected) return;
        try testing.io.sleep(.fromMilliseconds(50), .awake);
    }
    std.debug.print("holder {s} was granted the lock {d} times, expected {d}\n", .{ holder_id, granted, expected });
    return error.TestUnexpectedGrantCount;
}

/// Polls the history of `workflow_id` until `done` returns non-null for it, and returns
/// that. Time stays locked, unlike with `getResult`, and the time-skipping server doesn't
/// hold a history request open for new events, so this polls.
fn pollHistory(
    env: *temporal.testing.WorkflowEnvironment,
    workflow_id: []const u8,
    comptime T: type,
    context: anytype,
    comptime done: fn (@TypeOf(context), []const temporal.history.HistoryEvent) ?T,
) !T {
    for (0..400) |_| {
        var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
        defer scratch.deinit();
        const history = try env.client.fetchHistory(scratch.allocator(), workflow_id, "");
        if (done(context, history.events.items)) |result| return result;
        try testing.io.sleep(.fromMilliseconds(50), .awake);
    }
    std.debug.print("the history of {s} never reached the expected state\n", .{workflow_id});
    return error.TestHistoryTimeout;
}

/// Waits for `workflow_id` to complete.
fn waitForCompletion(env: *temporal.testing.WorkflowEnvironment, workflow_id: []const u8) !void {
    _ = try pollHistory(env, workflow_id, void, {}, struct {
        fn done(_: void, events: []const temporal.history.HistoryEvent) ?void {
            for (events) |event| {
                const attributes = event.attributes orelse continue;
                if (attributes == .workflow_execution_completed_event_attributes) return {};
            }
            return null;
        }
    }.done);
}

/// Waits until the mutex workflow has completed a workflow task that started after it
/// recorded the signal `signal_name`, so the signal's effect, if any, is in its history.
/// Returns how many grants (signals to holders) its history records by then.
fn waitForSignalHandled(env: *temporal.testing.WorkflowEnvironment, mutex_id: []const u8, signal_name: []const u8) !usize {
    return pollHistory(env, mutex_id, usize, signal_name, struct {
        fn done(name: []const u8, events: []const temporal.history.HistoryEvent) ?usize {
            var stage: enum { signal, task_started, task_completed } = .signal;
            var grants: usize = 0;
            var handled = false;
            for (events) |event| {
                const attributes = event.attributes orelse continue;
                if (attributes == .signal_external_workflow_execution_initiated_event_attributes) grants += 1;
                switch (stage) {
                    .signal => if (attributes == .workflow_execution_signaled_event_attributes and
                        std.mem.eql(u8, attributes.workflow_execution_signaled_event_attributes.signal_name, name))
                    {
                        stage = .task_started;
                    },
                    .task_started => if (attributes == .workflow_task_started_event_attributes) {
                        stage = .task_completed;
                    },
                    // The task's commands are recorded with its completion, so the grants
                    // counted through the end of the history include any it made.
                    .task_completed => if (attributes == .workflow_task_completed_event_attributes) {
                        handled = true;
                    },
                }
            }
            return if (handled) grants else null;
        }
    }.done);
}

fn expectMutexStatus(env: *temporal.testing.WorkflowEnvironment, mutex_id: []const u8, expected: temporal.client.WorkflowExecutionStatus) !void {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const description = try env.client.describeWorkflow(scratch.allocator(), mutex_id, "");
    try testing.expectEqual(expected, description.status);
}

test "a late release from an expired holder doesn't unlock the next holder's lock" {
    const env = try temporal.testing.WorkflowEnvironment.startTimeSkipping(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(mutex.MutexWorkflow);
    try env.registerWorkflow(HolderWorkflow);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const mutex_id = "mutex:mockNamespace:late-release";
    var holder_a = try env.startWorkflow(HolderWorkflow, .{ .id = "holder-a", .task_queue = "" }, .{ mutex_id, 1 });
    defer holder_a.deinit();
    var holder_b = try env.startWorkflow(HolderWorkflow, .{ .id = "holder-b", .task_queue = "" }, .{ mutex_id, 1 });
    defer holder_b.deinit();
    var holder_c = try env.startWorkflow(HolderWorkflow, .{ .id = "holder-c", .task_queue = "" }, .{ mutex_id, 1 });
    defer holder_c.deinit();
    var run = try env.signalWithStartWorkflow(mutex.MutexWorkflow, .{ .id = mutex_id, .task_queue = "" }, .{
        "mockNamespace", mock_resource_id, unlock_timeout,
    }, mutex.RequestLock, .{"holder-a"});
    defer run.deinit();
    try env.signalWorkflow(mutex.RequestLock, mutex_id, .{"holder-b"});
    try env.signalWorkflow(mutex.RequestLock, mutex_id, .{"holder-c"});

    // A holds the lock past its lease, so B gets it.
    try waitForGrants(env, "holder-a", 1);
    try env.sleep(.fromSeconds(10 * 60 + 1));
    try waitForGrants(env, "holder-b", 1);

    // A's release arrives while B holds the lock.
    try env.signalWorkflow(ReleaseNow, "holder-a", .{0});
    try waitForCompletion(env, "holder-a");
    const a_names = try env.queryWorkflow(GrantNames, a, "holder-a", .{});
    try testing.expectEqual(2, try waitForSignalHandled(env, mutex_id, a_names[0]));
    try env.sleep(.fromSeconds(60));
    try waitForGrants(env, "holder-c", 0);
    try expectMutexStatus(env, mutex_id, .running);

    // B's own release passes the lock to C.
    try env.signalWorkflow(ReleaseNow, "holder-b", .{0});
    try waitForGrants(env, "holder-c", 1);
    try env.signalWorkflow(ReleaseNow, "holder-c", .{0});

    try env.getResult(mutex.MutexWorkflow, &run, a, null);
    try env.getResult(HolderWorkflow, &holder_a, a, null);
    try env.getResult(HolderWorkflow, &holder_b, a, null);
    try env.getResult(HolderWorkflow, &holder_c, a, null);
    try env.replayRun(run.id, run.run_id);
}

test "two grants to the same workflow ID get different release signals" {
    const env = try temporal.testing.WorkflowEnvironment.startTimeSkipping(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(mutex.MutexWorkflow);
    try env.registerWorkflow(HolderWorkflow);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const mutex_id = "mutex:mockNamespace:same-holder";
    var holder = try env.startWorkflow(HolderWorkflow, .{ .id = "holder-x", .task_queue = "" }, .{ mutex_id, 2 });
    defer holder.deinit();
    var run = try env.signalWithStartWorkflow(mutex.MutexWorkflow, .{ .id = mutex_id, .task_queue = "" }, .{
        "mockNamespace", mock_resource_id, unlock_timeout,
    }, mutex.RequestLock, .{"holder-x"});
    defer run.deinit();
    try env.signalWorkflow(mutex.RequestLock, mutex_id, .{"holder-x"});

    try waitForGrants(env, "holder-x", 1);
    try env.sleep(.fromSeconds(10 * 60 + 1));
    try waitForGrants(env, "holder-x", 2);
    const names = try env.queryWorkflow(GrantNames, a, "holder-x", .{});
    try testing.expectEqual(2, names.len);
    try testing.expectEqualStrings("unlock-event-holder-x-0", names[0]);
    try testing.expectEqualStrings("unlock-event-holder-x-1", names[1]);

    // The late release of the first grant leaves the second one held.
    try env.signalWorkflow(ReleaseNow, "holder-x", .{0});
    try testing.expectEqual(2, try waitForSignalHandled(env, mutex_id, "unlock-event-holder-x-0"));
    try env.sleep(.fromSeconds(60));
    try expectMutexStatus(env, mutex_id, .running);

    try env.signalWorkflow(ReleaseNow, "holder-x", .{1});
    try env.getResult(mutex.MutexWorkflow, &run, a, null);
    try env.getResult(HolderWorkflow, &holder, a, null);
    try env.replayRun(run.id, run.run_id);
}
