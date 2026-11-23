//! The recovery workflow restarts every running trip workflow from its history: useful when
//! a bad change left workflows stuck or their state corrupted. One activity finds the running
//! trips with a visibility query and keeps their IDs in the worker's cache; then concurrent
//! activities recover them in batches. Recovering a trip names its latest run by run ID once
//! and works on that run only: it terminates the run if it is still open, reads the trip
//! count the run started with and the trips signaled to it from its final history, starts a
//! new run with that count, and signals the trips to it again. A run that continued as new
//! is not terminated; its successor is recovered in its place. The recovery activities
//! heartbeat how far they got (`RecoveryProgress`): the run being recovered, its replacement
//! and how many trips reached it, so a retry carries on from there and sends no trip twice.

const std = @import("std");
const temporal = @import("temporal");
const trip_workflow = @import("trip_workflow.zig");
const lru = @import("lru.zig");
const workflow = temporal.workflow;
const activity = temporal.activity;
const client_mod = temporal.client;
const Allocator = std.mem.Allocator;

pub const Params = struct {
    /// The type of the workflows to recover.
    workflow_type: []const u8,
    /// How many batches to recover at once; 0 means one.
    concurrency: i64 = 0,
};

pub const ListOpenExecutionsResult = struct {
    /// Where the cache holds the executions.
    id: []const u8,
    count: i64,
    host_id: []const u8,
};

/// The IDs of workflows to recover, as the cache holds them.
pub const Executions = struct {
    workflow_ids: []const []const u8,

    pub fn deinit(e: *Executions, gpa: Allocator) void {
        for (e.workflow_ids) |id| gpa.free(id);
        gpa.free(e.workflow_ids);
    }

    pub fn clone(e: Executions, arena: Allocator) error{OutOfMemory}!Executions {
        const ids = try arena.alloc([]const u8, e.workflow_ids.len);
        for (ids, e.workflow_ids) |*copy, id| copy.* = try arena.dupe(u8, id);
        return .{ .workflow_ids = ids };
    }
};

pub const ExecutionsCache = lru.Lru(Executions);

/// The recovery activities. They share the worker's cache, so the executions one lists are
/// only found by activities on the same worker; `host_id` names that worker in logs.
pub const Activities = struct {
    cache: *ExecutionsCache,
    host_id: []const u8,

    /// Lists the running workflows of `workflow_type` and stores their IDs in the cache under
    /// a new key.
    pub fn listOpenExecutions(a: *Activities, ctx: *activity.Context, workflow_type: []const u8) !ListOpenExecutionsResult {
        const logger = ctx.logger();
        logger.info("List all open executions of type.", .{ .workflow_type = workflow_type, .host_id = a.host_id });
        const client = try ctx.client();
        const arena = ctx.allocator();
        const key = try client_mod.uuid(arena, ctx.io());

        if (std.mem.indexOfScalar(u8, workflow_type, '\'') != null) return ctx.fail(.{
            .message = try std.fmt.allocPrint(arena, "workflow type \"{s}\" can't contain a quote", .{workflow_type}),
            .non_retryable = true,
        });
        const query = try std.fmt.allocPrint(arena, "WorkflowType = '{s}' AND ExecutionStatus = 'Running'", .{workflow_type});
        var it = try client.listWorkflows(query, .{ .page_size = 10 });
        defer it.deinit();

        const gpa = a.cache.gpa;
        var ids: std.ArrayList([]const u8) = .empty;
        defer {
            for (ids.items) |id| gpa.free(id);
            ids.deinit(gpa);
        }
        while (try it.next(arena)) |execution| {
            try ids.ensureUnusedCapacity(gpa, 1);
            ids.appendAssumeCapacity(try gpa.dupe(u8, execution.workflow_id));
            ctx.heartbeat(ids.items.len);
        }

        const count = ids.items.len;
        var executions: Executions = .{ .workflow_ids = try ids.toOwnedSlice(gpa) };
        a.cache.put(key, executions) catch |err| {
            executions.deinit(gpa);
            return err;
        };
        return .{ .id = key, .count = @intCast(count), .host_id = a.host_id };
    }

    /// Recovers the executions at `start_index` up to `start_index + batch_size` of those
    /// cached under `key`. Heartbeats a `RecoveryProgress` as it goes, so a retry carries on
    /// where the last attempt stopped: after the last execution it finished, and inside the
    /// one it was recovering, from the same source run and the first trip not yet signaled.
    pub fn recoverExecutions(a: *Activities, ctx: *activity.Context, key: []const u8, start_index: i64, batch_size: i64) !void {
        const logger = ctx.logger();
        logger.info("Starting execution recovery.", .{ .host_id = a.host_id, .key = key, .start_index = start_index, .batch_size = batch_size });
        const arena = ctx.allocator();

        const executions = try a.cache.get(arena, key) orelse return ctx.fail(.{
            .message = try std.fmt.allocPrint(arena, "no executions cached under key {s} on worker {s}; list them again", .{ key, a.host_id }),
            .non_retryable = true,
        });
        if (start_index < 0 or batch_size < 0) return ctx.fail(.{
            .message = try std.fmt.allocPrint(arena, "start index {d} and batch size {d} must not be negative", .{ start_index, batch_size }),
            .non_retryable = true,
        });

        const first: usize = @intCast(start_index);
        const end_index = @min(first +| @as(usize, @intCast(batch_size)), executions.workflow_ids.len);
        // Details this build can't decode, such as a bare index from an older one, are
        // ignored: the batch starts over, and recovering a replacement again rebuilds it from
        // its own history.
        var progress: RecoveryProgress = .{ .next = first };
        if (ctx.hasHeartbeatDetails()) {
            if (ctx.heartbeatDetails(RecoveryProgress)) |saved| {
                if (saved.next >= first) progress = saved;
            } else |err| {
                logger.warn("Ignoring heartbeat details from a previous attempt.", .{ .err = err });
            }
        }

        while (progress.next < end_index) {
            const index = progress.next;
            const workflow_id = executions.workflow_ids[index];
            recoverSingleExecution(ctx, workflow_id, &progress) catch |err| {
                logger.warn("Failed to recover execution.", .{ .workflow_id = workflow_id, .err = err });
                return err;
            };
            progress = .{ .next = index + 1 };
            ctx.heartbeat(progress);
        }
    }
};

pub const ListOpenExecutions = activity.Def("ListOpenExecutions", Activities.listOpenExecutions);
pub const RecoverExecutions = activity.Def("RecoverExecutions", Activities.recoverExecutions);

/// RecoverExecutions' heartbeat details: how far a batch got, so a retry resumes there.
pub const RecoveryProgress = struct {
    /// Index, among all executions cached under the key, of the next execution to recover.
    next: usize,
    /// How far recovering execution `next` got; null when it hasn't begun.
    current: ?ExecutionProgress = null,
};

pub const ExecutionProgress = struct {
    /// The run being recovered. It was read, and terminated if open; the replacement's start
    /// input and trips come from its final history.
    source_run_id: []const u8,
    /// The run the trips are being signaled to: the replacement, or the run it continued as
    /// new to. Empty until the replacement has started.
    replacement_run_id: []const u8 = "",
    /// How many of the source's trips, in history order, have reached the replacement chain.
    signaled: usize = 0,
};

/// One run of a trip workflow as its history records it.
const RunHistory = struct {
    /// The run's argument.
    state: trip_workflow.UserState,
    /// How to start a run like it.
    options: client_mod.StartWorkflowOptions,
    /// Every trip signaled to the run, in history order.
    trips: []const trip_workflow.TripEvent,
    last_event: temporal.history.HistoryEvent,
};

/// Reads the whole history of run `run_id` of `workflow_id`, as recorded so far. Returns null
/// for an empty history. An empty `run_id` would name the latest run, so it is an error.
fn readRun(ctx: *activity.Context, workflow_id: []const u8, run_id: []const u8) !?RunHistory {
    if (run_id.len == 0) return error.EmptyRunId;
    const logger = ctx.logger();
    const client = try ctx.client();
    const arena = ctx.allocator();

    var it = try client.historyIterator(workflow_id, run_id, .{});
    defer it.deinit();
    const first_event = try it.next(arena) orelse return null;
    var last_event = first_event;

    const started = switch (first_event.attributes orelse return error.UnknownEventType) {
        .workflow_execution_started_event_attributes => |attributes| attributes,
        else => {
            logger.warn("The first event of a workflow's history isn't its start.", .{ .workflow_id = workflow_id, .run_id = run_id, .event_type = first_event.event_type });
            return error.UnknownEventType;
        },
    };
    const input = if (started.input) |payloads| payloads.payloads.items else &.{};
    // Fails on corrupted workflow state.
    const state = (try temporal.converter.fromPayloads(struct { trip_workflow.UserState }, arena, input))[0];
    const options: client_mod.StartWorkflowOptions = .{
        .id = workflow_id,
        .task_queue = if (started.task_queue) |q| q.name else "",
        .task_timeout = if (started.workflow_task_timeout) |t| .fromNanoseconds(@as(i96, t.seconds) * std.time.ns_per_s + t.nanos) else null,
    };

    var trips: std.ArrayList(trip_workflow.TripEvent) = .empty;
    while (try it.next(arena)) |event| {
        last_event = event;
        const signaled = switch (event.attributes orelse continue) {
            .workflow_execution_signaled_event_attributes => |attributes| attributes,
            else => continue,
        };
        if (!std.mem.eql(u8, signaled.signal_name, trip_workflow.TripSignal.signal_name)) continue;
        const payloads = signaled.input orelse continue;
        // Fails on a corrupted signal payload.
        const data = (try temporal.converter.fromPayloads(trip_workflow.TripSignal.Args, arena, payloads.payloads.items))[0];
        try trips.append(arena, data);
    }
    return .{ .state = state, .options = options, .trips = trips.items, .last_event = last_event };
}

/// How many runs one attempt follows through continue-as-new before it gives up and lets the
/// activity retry.
const max_runs_followed = 10;

/// Recovers the latest run of `workflow_id`, the execution at `progress.next`, heartbeating
/// `progress` as it goes. The source run is named by its ID from the start (or by the
/// checkpoint of an earlier attempt), so every read and the terminate apply to that run and
/// never to whichever run is latest by then. A run that continued as new is not terminated:
/// its successor is recovered instead. The replacement is built from the history read after
/// the terminate, so a trip the server recorded while recovery ran is not lost.
///
/// It returns only once every trip of the source has reached the replacement. A failed
/// signal is returned, so the activity retries. The heartbeat names the replacement, but a
/// signal can reach the server and still fail on the client (its response lost, its deadline
/// passed after the server recorded it), and a worker process can die before its last
/// heartbeat arrives. So an attempt that finds the replacement already started counts the
/// trips its chain of runs has recorded, and sends only the ones after them. That count is
/// right only if nothing else signals the replacement while recovery replays.
fn recoverSingleExecution(ctx: *activity.Context, workflow_id: []const u8, progress: *RecoveryProgress) !void {
    const logger = ctx.logger();
    const client = try ctx.client();
    const arena = ctx.allocator();
    const index = progress.next;

    var current: ExecutionProgress = progress.current orelse .{
        .source_run_id = (try client.describeWorkflow(arena, workflow_id, "")).run_id,
    };
    if (current.source_run_id.len == 0) {
        logger.warn("The server named no run for the workflow; retrying.", .{ .workflow_id = workflow_id });
        return error.EmptyRunId;
    }

    const source: RunHistory = for (0..max_runs_followed) |_| {
        // Recorded before anything is terminated, so a retry works on this run, not the
        // replacement it starts.
        progress.* = .{ .next = index, .current = current };
        ctx.heartbeat(progress.*);
        var run = try readRun(ctx, workflow_id, current.source_run_id) orelse return; // Nothing to recover.
        if (!isExecutionCompleted(run.last_event)) {
            client.terminateWorkflow(workflow_id, current.source_run_id, "Recover") catch |err| switch (err) {
                // The run closed after it was read; its final history says how.
                error.NotFound => {},
                else => |e| return e,
            };
            run = try readRun(ctx, workflow_id, current.source_run_id) orelse return error.EmptyHistory;
            if (!isExecutionCompleted(run.last_event)) {
                logger.warn("A run is still open after it was terminated.", .{ .workflow_id = workflow_id, .run_id = current.source_run_id });
                return error.RunStillOpen;
            }
        }
        switch (run.last_event.attributes.?) {
            .workflow_execution_continued_as_new_event_attributes => |continued| {
                logger.info("The run continued as new; recovering its successor.", .{ .workflow_id = workflow_id, .run_id = current.source_run_id, .new_run_id = continued.new_execution_run_id });
                if (continued.new_execution_run_id.len == 0) return error.InvalidResponse;
                // A replacement is started only from a run that closed otherwise, so none
                // has been started yet.
                current = .{ .source_run_id = continued.new_execution_run_id };
            },
            else => break run,
        }
    } else {
        logger.warn("The workflow keeps continuing as new; retrying.", .{ .workflow_id = workflow_id, .runs_followed = max_runs_followed });
        return error.SourceKeepsContinuing;
    };

    if (current.replacement_run_id.len == 0) {
        var new_run = client.executeWorkflow(trip_workflow.TripWorkflow, source.options, .{source.state}) catch |err| switch (err) {
            error.WorkflowExecutionAlreadyStarted => return ctx.fail(.{
                .message = try std.fmt.allocPrint(arena, "workflow {s} is already running again after its run {s} was recovered; recovery can't tell whether that run is the replacement it started, so it leaves it alone", .{ workflow_id, current.source_run_id }),
                .non_retryable = true,
            }),
            else => |e| return e,
        };
        defer new_run.deinit();
        current.replacement_run_id = try arena.dupe(u8, new_run.run_id);
        progress.current = current;
        ctx.heartbeat(progress.*);
    } else {
        // An earlier attempt started the replacement: its history, not that attempt's
        // heartbeat, says which trips arrived.
        const chain = try replacementChainTrips(ctx, workflow_id, current.replacement_run_id, source.trips.len);
        if (chain.trips > source.trips.len) logger.warn("The replacement holds more trips than its source; something else signaled it.", .{ .workflow_id = workflow_id, .run_id = chain.last_run_id, .trips = chain.trips, .source_trips = source.trips.len });
        current.replacement_run_id = chain.last_run_id;
        current.signaled = @min(chain.trips, source.trips.len);
        progress.current = current;
        ctx.heartbeat(progress.*);
    }

    while (current.signaled < source.trips.len) {
        const trip = source.trips[current.signaled];
        client.signalWorkflow(trip_workflow.TripSignal, workflow_id, current.replacement_run_id, .{trip}) catch |err| switch (err) {
            error.NotFound => {
                current.replacement_run_id = try replacementSuccessor(ctx, workflow_id, current, source.trips.len);
                progress.current = current;
                ctx.heartbeat(progress.*);
                continue;
            },
            else => |e| {
                logger.warn("Failed to signal a trip to the new run; retrying.", .{ .workflow_id = workflow_id, .trip = trip.id, .err = e });
                return e;
            },
        };
        current.signaled += 1;
        progress.current = current;
        ctx.heartbeat(progress.*);
    }

    logger.info("Successfully restarted workflow.", .{ .workflow_id = workflow_id, .run_id = current.source_run_id, .new_run_id = current.replacement_run_id });
}

/// The run that replacement run `current.replacement_run_id`, which a signal no longer
/// reaches, continued as new to. Fails the activity without retrying if it closed any other
/// way: the trips it is missing can no longer be delivered to it.
fn replacementSuccessor(ctx: *activity.Context, workflow_id: []const u8, current: ExecutionProgress, trips: usize) ![]const u8 {
    const client = try ctx.client();
    const arena = ctx.allocator();
    var it = try client.historyIterator(workflow_id, current.replacement_run_id, .{ .filter = .close_event });
    defer it.deinit();
    // A run a signal can't find is closed or gone; a missing close event is retried.
    const closed = try it.next(arena) orelse return error.NotFound;
    switch (closed.attributes orelse return error.UnknownEventType) {
        .workflow_execution_continued_as_new_event_attributes => |continued| {
            if (continued.new_execution_run_id.len == 0) return error.InvalidResponse;
            ctx.logger().info("The replacement continued as new; signaling its successor.", .{ .workflow_id = workflow_id, .run_id = current.replacement_run_id, .new_run_id = continued.new_execution_run_id });
            return try arena.dupe(u8, continued.new_execution_run_id);
        },
        else => return ctx.fail(.{
            .message = try std.fmt.allocPrint(arena, "replacement run {s} of workflow {s} closed before it got {d} of the {d} trips of run {s}", .{ current.replacement_run_id, workflow_id, trips - current.signaled, trips, current.source_run_id }),
            .non_retryable = true,
        }),
    }
}

const ChainTrips = struct {
    /// The last run of the chain: open, or closed other than by continuing as new.
    last_run_id: []const u8,
    /// The trip signals recorded across every run of the chain.
    trips: usize,
};

/// Counts the trip signals recorded by the replacement chain that run `run_id` belongs to: it
/// follows continue-as-new forward to the chain's last run, then each run's start back to the
/// replacement's first run, which was started rather than continued to.
fn replacementChainTrips(ctx: *activity.Context, workflow_id: []const u8, run_id: []const u8, source_trips: usize) !ChainTrips {
    const logger = ctx.logger();
    const client = try ctx.client();
    const arena = ctx.allocator();
    // Recovery signals each run at least one trip before it can continue as new, so a chain
    // holding the source's trips has at most this many runs.
    const max_runs = source_trips + max_runs_followed;

    var last_run_id = run_id;
    for (0..max_runs) |_| {
        var it = try client.historyIterator(workflow_id, last_run_id, .{ .filter = .close_event });
        defer it.deinit();
        const closed = try it.next(arena) orelse break; // Still open.
        switch (closed.attributes orelse break) {
            .workflow_execution_continued_as_new_event_attributes => |continued| {
                if (continued.new_execution_run_id.len == 0) return error.InvalidResponse;
                last_run_id = try arena.dupe(u8, continued.new_execution_run_id);
            },
            else => break,
        }
    } else {
        logger.warn("The replacement keeps continuing as new; retrying.", .{ .workflow_id = workflow_id, .run_id = run_id });
        return error.ReplacementKeepsContinuing;
    }

    var trips: usize = 0;
    var counted_run_id = last_run_id;
    for (0..max_runs) |_| {
        var it = try client.historyIterator(workflow_id, counted_run_id, .{});
        defer it.deinit();
        const first_event = try it.next(arena) orelse return error.EmptyHistory;
        const started = switch (first_event.attributes orelse return error.UnknownEventType) {
            .workflow_execution_started_event_attributes => |attributes| attributes,
            else => return error.UnknownEventType,
        };
        while (try it.next(arena)) |event| switch (event.attributes orelse continue) {
            .workflow_execution_signaled_event_attributes => |signaled| {
                if (std.mem.eql(u8, signaled.signal_name, trip_workflow.TripSignal.signal_name)) trips += 1;
            },
            else => {},
        };
        if (started.continued_execution_run_id.len == 0) return .{ .last_run_id = last_run_id, .trips = trips };
        counted_run_id = try arena.dupe(u8, started.continued_execution_run_id);
    }
    logger.warn("The replacement's chain of runs is longer than recovery can have made it.", .{ .workflow_id = workflow_id, .run_id = last_run_id });
    return error.ReplacementKeepsContinuing;
}

/// Whether `event` closes its run, by completing, failing, timing out, being canceled or
/// terminated, or continuing as new.
pub fn isExecutionCompleted(event: temporal.history.HistoryEvent) bool {
    return switch (event.attributes orelse return false) {
        .workflow_execution_completed_event_attributes,
        .workflow_execution_terminated_event_attributes,
        .workflow_execution_canceled_event_attributes,
        .workflow_execution_failed_event_attributes,
        .workflow_execution_timed_out_event_attributes,
        .workflow_execution_continued_as_new_event_attributes,
        => true,
        else => false,
    };
}

const RecoveryState = struct {
    pending: usize,

    fn done(s: *const RecoveryState) bool {
        return s.pending == 0;
    }
};

fn recoverBatch(ctx: *workflow.Context, options: workflow.ActivityOptions, key: []const u8, start_index: i64, batch_size: i64, state: *RecoveryState) void {
    defer state.pending -= 1;
    const logger = ctx.logger();
    ctx.executeActivity(RecoverExecutions, options, .{ key, start_index, batch_size }).get() catch |err| {
        logger.err("Recover executions failed.", .{ .start_index = start_index, .err = err });
        return;
    };
    logger.info("Recover executions completed.", .{ .start_index = start_index });
}

fn recoverWorkflow(ctx: *workflow.Context, params: Params) !void {
    const logger = ctx.logger();
    logger.info("Recover workflow started.", .{});

    const result = ctx.executeActivity(ListOpenExecutions, .{
        .start_to_close_timeout = .fromSeconds(10 * std.time.s_per_min),
        .heartbeat_timeout = .fromSeconds(30),
    }, .{params.workflow_type}).get() catch |err| {
        logger.err("Failed to list open workflow executions.", .{ .err = err });
        return err;
    };
    if (result.count == 0) {
        logger.info("No open executions to recover.", .{ .workflow_type = params.workflow_type });
        return;
    }

    const concurrency = @min(@max(params.concurrency, 1), result.count);
    const batch_size = std.math.divCeil(i64, result.count, concurrency) catch unreachable;

    // The recovery activities may take as long as the workflow is allowed to run.
    const options: workflow.ActivityOptions = .{
        .start_to_close_timeout = ctx.info().execution_timeout orelse .fromSeconds(10 * std.time.s_per_min),
        .heartbeat_timeout = .fromSeconds(30),
        .retry_policy = .{
            .initial_interval = .fromSeconds(1),
            .backoff_coefficient = 2,
            .maximum_interval = .fromSeconds(10),
            .maximum_attempts = 100,
        },
    };

    var state: RecoveryState = .{ .pending = @intCast(concurrency) };
    var group: std.Io.Group = .init;
    defer group.cancel(ctx.io());
    for (0..@intCast(concurrency)) |i| {
        const start_index = @as(i64, @intCast(i)) * batch_size;
        group.async(ctx.io(), recoverBatch, .{ ctx, options, result.id, start_index, batch_size, &state });
    }
    try ctx.await(&state, RecoveryState.done);

    logger.info("Workflow completed.", .{ .result = result.count });
}

pub const RecoverWorkflow = workflow.Def("RecoverWorkflow", recoverWorkflow);
