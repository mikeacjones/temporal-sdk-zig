//! Tests for the recovery sample: the trip workflow's counting and continue-as-new, and the
//! recovery workflow restarting running trip workflows from their histories.

const std = @import("std");
const temporal = @import("temporal");
const trip_workflow = @import("trip_workflow.zig");
const recovery_workflow = @import("recovery_workflow.zig");

const testing = std.testing;
const Allocator = std.mem.Allocator;
const WorkflowEnvironment = temporal.testing.WorkflowEnvironment;
const TripWorkflow = trip_workflow.TripWorkflow;

/// A dev server environment with the sample's workflows and activities. The trip workflow
/// waits on signals, which the time-skipping server would skip past to its timeout.
const Env = struct {
    env: *WorkflowEnvironment,
    cache: recovery_workflow.ExecutionsCache,
    activities: recovery_workflow.Activities,

    fn start(e: *Env) !void {
        e.env = try WorkflowEnvironment.start(testing.allocator, testing.io);
        errdefer e.env.deinit();
        e.cache = .init(testing.allocator, testing.io, 10);
        e.activities = .{ .cache = &e.cache, .host_id = "recovery_test" };
        try e.env.registerWorkflow(TripWorkflow);
        try e.env.registerWorkflow(recovery_workflow.RecoverWorkflow);
        try e.env.registerActivitiesWith(&e.activities, .{ recovery_workflow.ListOpenExecutions, recovery_workflow.RecoverExecutions });
    }

    fn deinit(e: *Env) void {
        // The worker stops before the cache its activities use goes away.
        e.env.deinit();
        e.cache.deinit();
    }
};

fn signalTrips(env: *WorkflowEnvironment, workflow_id: []const u8, count: usize) !void {
    for (0..count) |i| {
        var buf: [16]u8 = undefined;
        const id = try std.fmt.bufPrint(&buf, "Trip{d}", .{i + 1});
        try env.signalWorkflow(trip_workflow.TripSignal, workflow_id, .{.{ .id = id, .total = @intCast(10 * (i + 1)) }});
    }
}

/// Signals are recorded before the workflow counts them, so the count catches up shortly.
fn waitForCounter(env: *WorkflowEnvironment, arena: Allocator, workflow_id: []const u8, expected: i64) !void {
    var counter: i64 = 0;
    for (0..200) |_| {
        counter = try env.queryWorkflow(trip_workflow.Counter, arena, workflow_id, .{});
        if (counter == expected) return;
        try testing.io.sleep(.fromMilliseconds(50), .awake);
    }
    std.debug.print("workflow {s} counted {d} trips, expected {d}\n", .{ workflow_id, counter, expected });
    return error.TestUnexpectedTripCount;
}

/// Waits for a run to close and returns the event that closed it.
fn waitForClose(env: *WorkflowEnvironment, arena: Allocator, workflow_id: []const u8, run_id: []const u8) !temporal.history.HistoryEvent {
    var it = try env.client.historyIterator(workflow_id, run_id, .{ .wait_new_event = true, .filter = .close_event });
    defer it.deinit();
    return try it.next(arena) orelse error.TestNoCloseEvent;
}

/// Waits until visibility lists `expected` running trip workflows.
fn waitForRunningTrips(env: *WorkflowEnvironment, expected: u64) !void {
    const query = "WorkflowType = 'TripWorkflow' AND ExecutionStatus = 'Running'";
    for (0..200) |_| {
        if (try env.client.countWorkflows(query) == expected) return;
        try testing.io.sleep(.fromMilliseconds(50), .awake);
    }
    return error.TestTripsNotVisible;
}

test "the trip workflow counts trips and continues as new after ten" {
    var e: Env = undefined;
    try e.start();
    defer e.deinit();
    const env = e.env;
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var run = try env.startWorkflow(TripWorkflow, .{ .task_queue = "" }, .{.{ .trip_counter = 5 }});
    defer run.deinit();
    try signalTrips(env, run.id, 3);
    try waitForCounter(env, a, run.id, 8);

    try signalTrips(env, run.id, trip_workflow.trips_per_run - 3);
    const closed = try waitForClose(env, a, run.id, run.run_id);
    const continued = closed.attributes.?.workflow_execution_continued_as_new_event_attributes;
    const state = (try temporal.converter.fromPayloads(struct { trip_workflow.UserState }, a, continued.input.?.payloads.items))[0];
    try testing.expectEqual(15, state.trip_counter);
    try env.replayRun(run.id, run.run_id);

    // The next run starts from the count the last one carried forward.
    try testing.expectEqual(15, try env.queryWorkflow(trip_workflow.Counter, a, run.id, .{}));
    try env.client.terminateWorkflow(run.id, "", "test finished");
}

test "the recovery workflow restarts running trips with the trips signaled to them" {
    var e: Env = undefined;
    try e.start();
    defer e.deinit();
    const env = e.env;
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // One trip workflow in its first run, and one that has continued as new once.
    var first = try env.startWorkflow(TripWorkflow, .{ .task_queue = "" }, .{.{}});
    defer first.deinit();
    try signalTrips(env, first.id, 3);
    try waitForCounter(env, a, first.id, 3);

    var second = try env.startWorkflow(TripWorkflow, .{ .task_queue = "" }, .{.{}});
    defer second.deinit();
    try signalTrips(env, second.id, trip_workflow.trips_per_run);
    _ = try waitForClose(env, a, second.id, second.run_id);
    try signalTrips(env, second.id, 2);
    try waitForCounter(env, a, second.id, 12);
    const second_run_before = (try env.client.describeWorkflow(a, second.id, "")).run_id;

    try waitForRunningTrips(env, 2);
    try env.executeWorkflow(recovery_workflow.RecoverWorkflow, .{.{ .workflow_type = "TripWorkflow", .concurrency = 2 }}, a, null);

    const cases = [_]struct { id: []const u8, old_run_id: []const u8, counter: i64, started_with: i64 }{
        .{ .id = first.id, .old_run_id = first.run_id, .counter = 3, .started_with = 0 },
        .{ .id = second.id, .old_run_id = second_run_before, .counter = 12, .started_with = 10 },
    };
    for (cases) |case| {
        const closed = try waitForClose(env, a, case.id, case.old_run_id);
        try testing.expect(closed.attributes.? == .workflow_execution_terminated_event_attributes);

        const latest = try env.client.describeWorkflow(a, case.id, "");
        try testing.expect(!std.mem.eql(u8, latest.run_id, case.old_run_id));
        try testing.expectEqual(.running, latest.status);
        try waitForCounter(env, a, case.id, case.counter);

        // The new run starts from the old run's argument and gets its trips again.
        const history = try env.client.fetchHistory(a, case.id, latest.run_id);
        const started = history.events.items[0].attributes.?.workflow_execution_started_event_attributes;
        const state = (try temporal.converter.fromPayloads(struct { trip_workflow.UserState }, a, started.input.?.payloads.items))[0];
        try testing.expectEqual(case.started_with, state.trip_counter);
        var signals: usize = 0;
        for (history.events.items) |event| {
            if (event.attributes.? == .workflow_execution_signaled_event_attributes) signals += 1;
        }
        try testing.expectEqual(case.counter - case.started_with, @as(i64, @intCast(signals)));

        try env.client.terminateWorkflow(case.id, "", "test finished");
    }
    try testing.expectEqual(1, e.cache.len());
}

test "the recovery workflow finishes when nothing is running" {
    var e: Env = undefined;
    try e.start();
    defer e.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();

    try e.env.executeWorkflow(recovery_workflow.RecoverWorkflow, .{.{ .workflow_type = "TripWorkflow", .concurrency = 2 }}, arena.allocator(), null);
}

test "recovering a batch carries on after the last heartbeat" {
    var cache: recovery_workflow.ExecutionsCache = .init(testing.allocator, testing.io, 10);
    defer cache.deinit();
    var activities: recovery_workflow.Activities = .{ .cache = &cache, .host_id = "recovery_test" };
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const ids = try testing.allocator.alloc([]const u8, 2);
    ids[0] = try testing.allocator.dupe(u8, "trip-a");
    ids[1] = try testing.allocator.dupe(u8, "trip-b");
    try cache.put("key", .{ .workflow_ids = ids });

    // A previous attempt recovered both executions of the batch, so there is nothing left to
    // do (and no client is needed to do it).
    var env: temporal.testing.ActivityEnvironment = .init(testing.io);
    const details = [_]temporal.converter.Payload{try temporal.converter.toPayload(a, @as(usize, 1))};
    env.heartbeat_details = &details;
    try env.executeActivityWith(recovery_workflow.RecoverExecutions, &activities, .{ "key", 0, 2 }, a);
}

test "recovering a batch fails without retrying when its executions aren't cached" {
    var cache: recovery_workflow.ExecutionsCache = .init(testing.allocator, testing.io, 10);
    defer cache.deinit();
    var activities: recovery_workflow.Activities = .{ .cache = &cache, .host_id = "recovery_test" };
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();

    const env: temporal.testing.ActivityEnvironment = .init(testing.io);
    try testing.expectError(error.ApplicationFailure, env.executeActivityWith(recovery_workflow.RecoverExecutions, &activities, .{ "missing", 0, 2 }, arena.allocator()));
}
