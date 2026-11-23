//! Tests for the recovery sample: the trip workflow's counting and continue-as-new, and the
//! recovery workflow restarting running trip workflows from their histories.

const std = @import("std");
const temporal = @import("temporal");
const trip_workflow = @import("trip_workflow.zig");
const recovery_workflow = @import("recovery_workflow.zig");

const testing = std.testing;
const interceptor = temporal.interceptor;
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
        return e.startWith(.{});
    }

    fn startWith(e: *Env, options: WorkflowEnvironment.Options) !void {
        e.env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, options);
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

/// Starts a trip workflow and signals it `count` trips while no worker polls, so its first
/// workflow task delivers them all at once. `env.client` is used directly: the environment's
/// `startWorkflow` would start the worker first.
fn startWithQueuedTrips(env: *WorkflowEnvironment, workflow_id: []const u8, count: usize) !temporal.client.WorkflowRun(void) {
    var run = try env.client.executeWorkflow(TripWorkflow, .{ .id = workflow_id, .task_queue = env.task_queue }, .{.{}});
    errdefer run.deinit();
    for (0..count) |i| {
        var buf: [16]u8 = undefined;
        const id = try std.fmt.bufPrint(&buf, "Trip{d}", .{i + 1});
        try env.client.signalWorkflow(trip_workflow.TripSignal, workflow_id, run.run_id, .{.{ .id = id, .total = @intCast(10 * (i + 1)) }});
    }
    return run;
}

test "trips delivered in one task beyond the tenth are counted before continuing as new" {
    var e: Env = undefined;
    try e.start();
    defer e.deinit();
    const env = e.env;
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const sent = trip_workflow.trips_per_run + 2;
    var run = try startWithQueuedTrips(env, "trip-burst", sent);
    defer run.deinit();

    // The query starts the worker, whose first task holds every signal.
    _ = try env.queryWorkflow(trip_workflow.Counter, a, run.id, .{});
    const closed = try waitForClose(env, a, run.id, run.run_id);
    const continued = closed.attributes.?.workflow_execution_continued_as_new_event_attributes;
    const state = (try temporal.converter.fromPayloads(struct { trip_workflow.UserState }, a, continued.input.?.payloads.items))[0];
    try testing.expectEqual(sent, state.trip_counter);
    try waitForCounter(env, a, run.id, sent);

    try env.replayRun(run.id, run.run_id);
    try env.replayRun(run.id, continued.new_execution_run_id);
    try env.client.terminateWorkflow(run.id, "", "test finished");
}

test "recovery restores a trip workflow that counted more than ten trips in one task" {
    var e: Env = undefined;
    try e.start();
    defer e.deinit();
    const env = e.env;
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const sent = trip_workflow.trips_per_run + 2;
    var run = try startWithQueuedTrips(env, "trip-burst-recovered", sent);
    defer run.deinit();

    // Recovery runs while no worker polls, so the replacement's first task also holds every
    // trip it signals.
    const ids = try testing.allocator.alloc([]const u8, 1);
    ids[0] = testing.allocator.dupe(u8, run.id) catch |err| {
        testing.allocator.free(ids);
        return err;
    };
    try e.cache.put("key", .{ .workflow_ids = ids });
    var activity_env: temporal.testing.ActivityEnvironment = .init(testing.io);
    activity_env.client = &env.client;
    try activity_env.executeActivityWith(recovery_workflow.RecoverExecutions, &e.activities, .{ "key", 0, 1 }, a);

    const closed = try waitForClose(env, a, run.id, run.run_id);
    try testing.expect(closed.attributes.? == .workflow_execution_terminated_event_attributes);
    try waitForCounter(env, a, run.id, sent);
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

/// A client interceptor that, on the first TerminateWorkflowExecution call, signals `trips`
/// trips to run `run_id` before it lets the terminate through: as if they arrived while
/// recovery read the run. With `wait_for_continue`, it also waits for the run to continue as
/// new.
const SignalBeforeTerminate = struct {
    client: ?*temporal.client.Client = null,
    workflow_id: []const u8 = "",
    run_id: []const u8 = "",
    trips: usize,
    wait_for_continue: bool,
    fired: std.atomic.Value(bool) = .init(false),

    pub fn call(c: *SignalBeforeTerminate, input: *interceptor.ClientCall, next: interceptor.Client.Next) interceptor.ClientError!void {
        if (std.mem.eql(u8, input.method, "TerminateWorkflowExecution") and !c.fired.swap(true, .acq_rel)) {
            c.continueRun(input.arena) catch |err| {
                std.debug.print("signaling run {s} before its terminate failed: {t}\n", .{ c.run_id, err });
                return error.RpcFailed;
            };
        }
        return next.call(input);
    }

    fn continueRun(c: *SignalBeforeTerminate, arena: Allocator) !void {
        const client = c.client.?;
        for (0..c.trips) |i| {
            var buf: [16]u8 = undefined;
            const id = try std.fmt.bufPrint(&buf, "Late{d}", .{i + 1});
            try client.signalWorkflow(trip_workflow.TripSignal, c.workflow_id, c.run_id, .{.{ .id = id, .total = 1 }});
        }
        if (!c.wait_for_continue) return;
        var it = try client.historyIterator(c.workflow_id, c.run_id, .{ .wait_new_event = true, .filter = .close_event });
        defer it.deinit();
        const closed = try it.next(arena) orelse return error.TestNoCloseEvent;
        if (closed.attributes.? != .workflow_execution_continued_as_new_event_attributes) return error.TestRunDidNotContinue;
    }
};

test "a run that continues as new during recovery is followed, not terminated" {
    var continue_first: SignalBeforeTerminate = .{ .trips = trip_workflow.trips_per_run - 3, .wait_for_continue = true };
    const interceptors = [_]interceptor.Client{.from(SignalBeforeTerminate, &continue_first)};
    var e: Env = undefined;
    try e.startWith(.{ .interceptors = &interceptors });
    defer e.deinit();
    const env = e.env;
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var run = try env.startWorkflow(TripWorkflow, .{ .task_queue = "" }, .{.{}});
    defer run.deinit();
    try signalTrips(env, run.id, 3);
    try waitForCounter(env, a, run.id, 3);
    continue_first.client = &env.client;
    continue_first.workflow_id = run.id;
    continue_first.run_id = run.run_id;

    try waitForRunningTrips(env, 1);
    try env.executeWorkflow(recovery_workflow.RecoverWorkflow, .{.{ .workflow_type = "TripWorkflow" }}, a, null);
    try testing.expect(continue_first.fired.load(.acquire));

    // The run recovery read continued as new on its own; the successor was terminated.
    const first_closed = try waitForClose(env, a, run.id, run.run_id);
    const successor = first_closed.attributes.?.workflow_execution_continued_as_new_event_attributes.new_execution_run_id;
    const successor_closed = try waitForClose(env, a, run.id, successor);
    try testing.expect(successor_closed.attributes.? == .workflow_execution_terminated_event_attributes);

    // The replacement starts from the successor's count, ten, and gets no trips again.
    const latest = try env.client.describeWorkflow(a, run.id, "");
    try testing.expect(!std.mem.eql(u8, latest.run_id, run.run_id));
    try testing.expect(!std.mem.eql(u8, latest.run_id, successor));
    const history = try env.client.fetchHistory(a, run.id, latest.run_id);
    const started = history.events.items[0].attributes.?.workflow_execution_started_event_attributes;
    const state = (try temporal.converter.fromPayloads(struct { trip_workflow.UserState }, a, started.input.?.payloads.items))[0];
    try testing.expectEqual(10, state.trip_counter);
    for (history.events.items) |event| {
        try testing.expect(event.attributes.? != .workflow_execution_signaled_event_attributes);
    }
    try waitForCounter(env, a, run.id, 10);
    try env.client.terminateWorkflow(run.id, "", "test finished");
}

test "a trip recorded between the read and the terminate reaches the replacement" {
    var late_trip: SignalBeforeTerminate = .{ .trips = 1, .wait_for_continue = false };
    const interceptors = [_]interceptor.Client{.from(SignalBeforeTerminate, &late_trip)};
    var e: Env = undefined;
    try e.startWith(.{ .interceptors = &interceptors });
    defer e.deinit();
    const env = e.env;
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var run = try env.startWorkflow(TripWorkflow, .{ .task_queue = "" }, .{.{}});
    defer run.deinit();
    try signalTrips(env, run.id, 3);
    try waitForCounter(env, a, run.id, 3);
    late_trip.client = &env.client;
    late_trip.workflow_id = run.id;
    late_trip.run_id = run.run_id;

    try waitForRunningTrips(env, 1);
    try env.executeWorkflow(recovery_workflow.RecoverWorkflow, .{.{ .workflow_type = "TripWorkflow" }}, a, null);
    try testing.expect(late_trip.fired.load(.acquire));

    const closed = try waitForClose(env, a, run.id, run.run_id);
    try testing.expect(closed.attributes.? == .workflow_execution_terminated_event_attributes);
    // The three trips recovery read, and the one recorded after that read.
    try waitForCounter(env, a, run.id, 4);
    try env.client.terminateWorkflow(run.id, "", "test finished");
}

/// A client interceptor that, once armed, fails the second SignalWorkflowExecution call it
/// sees, once, as if the RPC had been lost.
const FailSecondSignal = struct {
    armed: std.atomic.Value(bool) = .init(false),
    signals: std.atomic.Value(usize) = .init(0),

    pub fn call(c: *FailSecondSignal, input: *interceptor.ClientCall, next: interceptor.Client.Next) interceptor.ClientError!void {
        if (c.armed.load(.acquire) and std.mem.eql(u8, input.method, "SignalWorkflowExecution")) {
            if (c.signals.fetchAdd(1, .acq_rel) == 1) return error.RpcFailed;
        }
        return next.call(input);
    }
};

test "a trip signal that fails during recovery is retried and arrives exactly once" {
    var fail_second: FailSecondSignal = .{};
    const interceptors = [_]interceptor.Client{.from(FailSecondSignal, &fail_second)};
    var e: Env = undefined;
    try e.startWith(.{ .interceptors = &interceptors });
    defer e.deinit();
    const env = e.env;
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var run = try env.startWorkflow(TripWorkflow, .{ .task_queue = "" }, .{.{}});
    defer run.deinit();
    try signalTrips(env, run.id, 3);
    try waitForCounter(env, a, run.id, 3);
    fail_second.armed.store(true, .release);

    try waitForRunningTrips(env, 1);
    try env.executeWorkflow(recovery_workflow.RecoverWorkflow, .{.{ .workflow_type = "TripWorkflow" }}, a, null);
    // The first signal, the failed second, and its retry and the third.
    try testing.expectEqual(4, fail_second.signals.load(.acquire));

    const closed = try waitForClose(env, a, run.id, run.run_id);
    try testing.expect(closed.attributes.? == .workflow_execution_terminated_event_attributes);

    // The replacement got each of the source's three trips once.
    const latest = try env.client.describeWorkflow(a, run.id, "");
    try testing.expect(!std.mem.eql(u8, latest.run_id, run.run_id));
    try waitForCounter(env, a, run.id, 3);
    const history = try env.client.fetchHistory(a, run.id, latest.run_id);
    var trips: usize = 0;
    for (history.events.items) |event| switch (event.attributes.?) {
        .workflow_execution_signaled_event_attributes => |signaled| {
            if (std.mem.eql(u8, signaled.signal_name, trip_workflow.TripSignal.signal_name)) trips += 1;
        },
        else => {},
    };
    try testing.expectEqual(3, trips);
    try env.client.terminateWorkflow(run.id, "", "test finished");
}

/// A client interceptor that, once armed, lets the second SignalWorkflowExecution call it
/// sees reach the server and then fails it, once, as if the response had been lost (a
/// deadline that expired after the server recorded the signal).
const LoseSecondSignalResponse = struct {
    armed: std.atomic.Value(bool) = .init(false),
    signals: std.atomic.Value(usize) = .init(0),

    pub fn call(c: *LoseSecondSignalResponse, input: *interceptor.ClientCall, next: interceptor.Client.Next) interceptor.ClientError!void {
        if (c.armed.load(.acquire) and std.mem.eql(u8, input.method, "SignalWorkflowExecution")) {
            if (c.signals.fetchAdd(1, .acq_rel) == 1) {
                try next.call(input);
                return error.RpcFailed;
            }
        }
        return next.call(input);
    }
};

test "a trip signal whose response is lost during recovery arrives exactly once" {
    var lose_second: LoseSecondSignalResponse = .{};
    const interceptors = [_]interceptor.Client{.from(LoseSecondSignalResponse, &lose_second)};
    var e: Env = undefined;
    try e.startWith(.{ .interceptors = &interceptors });
    defer e.deinit();
    const env = e.env;
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var run = try env.startWorkflow(TripWorkflow, .{ .task_queue = "" }, .{.{}});
    defer run.deinit();
    try signalTrips(env, run.id, 3);
    try waitForCounter(env, a, run.id, 3);
    lose_second.armed.store(true, .release);

    try waitForRunningTrips(env, 1);
    try env.executeWorkflow(recovery_workflow.RecoverWorkflow, .{.{ .workflow_type = "TripWorkflow" }}, a, null);

    const closed = try waitForClose(env, a, run.id, run.run_id);
    try testing.expect(closed.attributes.? == .workflow_execution_terminated_event_attributes);

    // The server recorded the second trip although its response was lost, so the retry must
    // not send it again: the replacement holds each of the source's three trips once.
    const latest = try env.client.describeWorkflow(a, run.id, "");
    try testing.expect(!std.mem.eql(u8, latest.run_id, run.run_id));
    const history = try env.client.fetchHistory(a, run.id, latest.run_id);
    var trips: usize = 0;
    for (history.events.items) |event| switch (event.attributes.?) {
        .workflow_execution_signaled_event_attributes => |signaled| {
            if (std.mem.eql(u8, signaled.signal_name, trip_workflow.TripSignal.signal_name)) trips += 1;
        },
        else => {},
    };
    try env.client.terminateWorkflow(run.id, "", "test finished");
    try testing.expectEqual(3, trips);
}

test "a run that ended by continuing as new counts as completed" {
    const continued: temporal.history.HistoryEvent = .{
        .event_type = .EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW,
        .attributes = .{ .workflow_execution_continued_as_new_event_attributes = .{ .new_execution_run_id = "next" } },
    };
    try testing.expect(recovery_workflow.isExecutionCompleted(continued));

    const terminated: temporal.history.HistoryEvent = .{ .attributes = .{ .workflow_execution_terminated_event_attributes = .{} } };
    try testing.expect(recovery_workflow.isExecutionCompleted(terminated));
    const signaled: temporal.history.HistoryEvent = .{ .attributes = .{ .workflow_execution_signaled_event_attributes = .{} } };
    try testing.expect(!recovery_workflow.isExecutionCompleted(signaled));
    try testing.expect(!recovery_workflow.isExecutionCompleted(.{}));
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
    const details = [_]temporal.converter.Payload{try temporal.converter.toPayload(a, recovery_workflow.RecoveryProgress{ .next = 2 })};
    env.heartbeat_details = &details;
    try env.executeActivityWith(recovery_workflow.RecoverExecutions, &activities, .{ "key", 0, 2 }, a);
}

test "recovering a batch starts over on heartbeat details it can't decode" {
    var cache: recovery_workflow.ExecutionsCache = .init(testing.allocator, testing.io, 10);
    defer cache.deinit();
    var activities: recovery_workflow.Activities = .{ .cache = &cache, .host_id = "recovery_test" };
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const ids = try testing.allocator.alloc([]const u8, 1);
    ids[0] = try testing.allocator.dupe(u8, "trip-a");
    try cache.put("key", .{ .workflow_ids = ids });

    // An older build's bare index is ignored, so the batch recovers its execution again; it
    // needs a client for that, and this environment has none.
    var env: temporal.testing.ActivityEnvironment = .init(testing.io);
    const details = [_]temporal.converter.Payload{try temporal.converter.toPayload(a, @as(usize, 0))};
    env.heartbeat_details = &details;
    try testing.expectError(error.NoClient, env.executeActivityWith(recovery_workflow.RecoverExecutions, &activities, .{ "key", 0, 1 }, a));
}

test "recovering a batch fails without retrying when its executions aren't cached" {
    var cache: recovery_workflow.ExecutionsCache = .init(testing.allocator, testing.io, 10);
    defer cache.deinit();
    var activities: recovery_workflow.Activities = .{ .cache = &cache, .host_id = "recovery_test" };
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();

    var env: temporal.testing.ActivityEnvironment = .init(testing.io);
    try testing.expectError(error.ApplicationFailure, env.executeActivityWith(recovery_workflow.RecoverExecutions, &activities, .{ "missing", 0, 2 }, arena.allocator()));
}
