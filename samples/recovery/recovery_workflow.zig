//! The recovery workflow restarts every running trip workflow from its history: useful when
//! a bad change left workflows stuck or their state corrupted. One activity finds the running
//! trips with a visibility query and keeps their IDs in the worker's cache; then concurrent
//! activities recover them in batches. Recovering a trip reads the trip count it started
//! with and the trips signaled to it from its history, terminates the run, starts a new run
//! with that count, and signals the trips to it again.

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
    /// cached under `key`. Heartbeats the index of each one it recovered, so a retry carries
    /// on after it.
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

        var index: usize = @intCast(start_index);
        const end_index = @min(index + @as(usize, @intCast(batch_size)), executions.workflow_ids.len);
        if (ctx.hasHeartbeatDetails()) {
            if (ctx.heartbeatDetails(usize)) |finished| index = finished + 1 else |_| {}
        }

        while (index < end_index) : (index += 1) {
            const workflow_id = executions.workflow_ids[index];
            recoverSingleExecution(ctx, workflow_id) catch |err| {
                logger.err("Failed to recover execution.", .{ .workflow_id = workflow_id, .err = err });
                return err;
            };
            ctx.heartbeat(index);
        }
    }
};

pub const ListOpenExecutions = activity.Def("ListOpenExecutions", Activities.listOpenExecutions);
pub const RecoverExecutions = activity.Def("RecoverExecutions", Activities.recoverExecutions);

/// A signal to send again to the new run.
const SignalParams = struct {
    name: []const u8,
    data: trip_workflow.TripEvent,
};

fn recoverSingleExecution(ctx: *activity.Context, workflow_id: []const u8) !void {
    const logger = ctx.logger();
    const client = try ctx.client();
    const arena = ctx.allocator();

    var it = try client.historyIterator(workflow_id, "", .{});
    defer it.deinit();
    const first_event = try it.next(arena) orelse return; // Nothing to recover.
    var last_event = first_event;

    // How to start the new run, from the run's started event.
    const started = switch (first_event.attributes orelse return error.UnknownEventType) {
        .workflow_execution_started_event_attributes => |attributes| attributes,
        else => {
            logger.warn("The first event of a workflow's history isn't its start.", .{ .workflow_id = workflow_id, .event_type = first_event.event_type });
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

    // Every trip signaled to the run, to signal again to the new one.
    var signals: std.ArrayList(SignalParams) = .empty;
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
        try signals.append(arena, .{ .name = signaled.signal_name, .data = data });
    }

    if (!isExecutionCompleted(last_event)) try client.terminateWorkflow(workflow_id, "", "Recover");

    var new_run = try client.executeWorkflow(trip_workflow.TripWorkflow, options, .{state});
    defer new_run.deinit();

    for (signals.items) |s| {
        client.signalWorkflow(trip_workflow.TripSignal, workflow_id, new_run.run_id, .{s.data}) catch |err| {
            logger.warn("Failed to signal a trip to the new run.", .{ .workflow_id = workflow_id, .trip = s.data.id, .err = err });
        };
    }

    logger.info("Successfully restarted workflow.", .{ .workflow_id = workflow_id, .new_run_id = new_run.run_id });
}

fn isExecutionCompleted(event: temporal.history.HistoryEvent) bool {
    return switch (event.attributes orelse return false) {
        .workflow_execution_completed_event_attributes,
        .workflow_execution_terminated_event_attributes,
        .workflow_execution_canceled_event_attributes,
        .workflow_execution_failed_event_attributes,
        .workflow_execution_timed_out_event_attributes,
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
