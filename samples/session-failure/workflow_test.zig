//! Tests for the session failure sample. The activities are test doubles: on the
//! environment's worker for the happy path, and on separate session hosts for the recovery,
//! where the first host stops while the long activity runs on it.

const std = @import("std");
const temporal = @import("temporal");
const sample = @import("workflow.zig");
const activities = @import("activities.zig");

const testing = std.testing;
const activity = temporal.activity;
const workflow = temporal.workflow;

/// Test-only global state: the activities run on the workers' threads.
const Doubles = struct {
    var prepare_calls: std.atomic.Value(u32) = .init(0);
    var long_calls: std.atomic.Value(u32) = .init(0);
    /// Set when the long activity started on the first host.
    var holding: std.Io.Event = .unset;

    fn reset() void {
        prepare_calls.store(0, .release);
        long_calls.store(0, .release);
        holding = .unset;
    }

    fn prepare(ctx: *activity.Context) !void {
        try testing.expect(ctx.info().session_id.len > 0);
        _ = prepare_calls.fetchAdd(1, .acq_rel);
    }

    fn longQuick(ctx: *activity.Context) !void {
        try testing.expect(ctx.info().session_id.len > 0);
        _ = long_calls.fetchAdd(1, .acq_rel);
    }

    /// Runs until it is canceled, as the long activity does on a host that stops.
    fn longHold(ctx: *activity.Context) !void {
        _ = long_calls.fetchAdd(1, .acq_rel);
        holding.set(ctx.io());
        try ctx.io().sleep(.fromSeconds(600), .awake);
    }

    const Prepare = activity.Def(activities.PrepareWorkerActivity.activity_name, prepare);
    const LongQuick = activity.Def(activities.LongRunningActivity.activity_name, longQuick);
    const LongHold = activity.Def(activities.LongRunningActivity.activity_name, longHold);
};

test "the workflow completes when both activities succeed in a session" {
    Doubles.reset();
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    // Important for the worker to take part in sessions; set before the first workflow starts.
    env.worker.options.sessions = .{};
    try env.registerWorkflow(sample.SampleSessionFailureRecoveryWorkflow);
    try env.registerActivityAs(activities.PrepareWorkerActivity, Doubles.prepare);
    try env.registerActivityAs(activities.LongRunningActivity, Doubles.longQuick);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    try env.executeWorkflow(sample.SampleSessionFailureRecoveryWorkflow, .{}, arena.allocator(), null);
    try testing.expectEqual(1, Doubles.prepare_calls.load(.acquire));
    try testing.expectEqual(1, Doubles.long_calls.load(.acquire));
}

/// The sample's workflow, waiting one second rather than five minutes between sessions.
fn quickRecovery(ctx: *workflow.Context) !void {
    return sample.recoverFromSessionFailures(ctx, .fromSeconds(1));
}
const QuickRecovery = workflow.Def(sample.SampleSessionFailureRecoveryWorkflow.workflow_name, quickRecovery);

fn runWorker(w: *temporal.worker.Worker) temporal.worker.Worker.RunError!void {
    return w.run();
}

test "the workflow recovers in a new session when its host stops" {
    Doubles.reset();
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    // The environment's worker runs the workflow only; the two hosts take the sessions.
    try env.registerWorkflow(QuickRecovery);

    var first = try temporal.worker.Worker.init(testing.allocator, testing.io, &env.client, env.task_queue, .{ .sessions = .{} });
    defer first.deinit();
    try first.registerActivity(Doubles.Prepare);
    try first.registerActivity(Doubles.LongHold);
    var first_running = try testing.io.concurrent(runWorker, .{&first});
    defer {
        first.stop();
        first_running.await(testing.io) catch {};
    }

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var run = try env.startWorkflow(QuickRecovery, .{ .task_queue = "" }, .{});
    defer run.deinit();
    try Doubles.holding.waitTimeout(testing.io, .{ .duration = .{ .raw = .fromSeconds(60), .clock = .awake } });

    // A second host is a second process with its own client. One client cannot hold two
    // workers that poll the same kinds of tasks on one task queue: sdk-core refuses the second
    // ("Registration of multiple workers with overlapping worker task types").
    var second_client = try temporal.client.Client.connect(testing.allocator, testing.io, env.server.dev.clientOptions());
    defer second_client.close();
    var second = try temporal.worker.Worker.init(testing.allocator, testing.io, &second_client, env.task_queue, .{ .sessions = .{} });
    defer second.deinit();
    try second.registerActivity(Doubles.Prepare);
    try second.registerActivity(Doubles.LongQuick);
    var second_running = try testing.io.concurrent(runWorker, .{&second});
    defer {
        second.stop();
        second_running.await(testing.io) catch {};
    }
    try second.waitUntilRunning();

    // The first host goes away while the long activity runs on it: its session fails.
    first.stop();
    try env.getResult(QuickRecovery, &run, arena.allocator(), null);
    try testing.expectEqual(2, Doubles.prepare_calls.load(.acquire));
    try testing.expectEqual(2, Doubles.long_calls.load(.acquire));
}

test "the long-running activity works for its work time, then returns" {
    var acts: activities.Activities = .{ .work_time = .fromMilliseconds(50), .heartbeat_interval = .fromMilliseconds(10) };
    var env: temporal.testing.ActivityEnvironment = .init(testing.io);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const started = std.Io.Timestamp.now(testing.io, .awake);
    try env.executeActivityWith(activities.LongRunningActivity, &acts, .{}, arena.allocator());
    try testing.expect(started.untilNow(testing.io, .awake).nanoseconds >= acts.work_time.nanoseconds);
    try env.executeActivityWith(activities.PrepareWorkerActivity, &acts, .{}, arena.allocator());
}
