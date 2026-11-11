//! Workers: poll a task queue and run the workflows and
//! activities registered on it.
//!
//!     var w = try temporal.worker.Worker.init(gpa, io, &client, "hello-world", .{});
//!     defer w.deinit();
//!     try w.registerWorkflow(HelloWorld);
//!     try w.registerActivity(Greet);
//!     try w.runUntilSignal(); // stops on SIGINT or SIGTERM
//!
//! Workflow activations are processed one at a time on one thread, each on its run's
//! deterministic Io. Activities run concurrently on the worker's Io, each on its own task so
//! a cancel from the core reaches it as `std.Io` cancellation.

const std = @import("std");
const bridge = @import("bridge.zig");
const proto = @import("proto.zig");
const converter = @import("converter.zig");
const failure_mod = @import("failure.zig");
const log_mod = @import("log.zig");
const workflow = @import("workflow.zig");
const activity = @import("activity.zig");
const client_mod = @import("client.zig");
const Instance = @import("workflow/Instance.zig");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const act_pb = proto.coresdk.workflow_activation;
const done_pb = proto.coresdk.workflow_completion;
const task_pb = proto.coresdk.activity_task;
const result_pb = proto.coresdk.activity_result;
const log = std.log.scoped(.temporal);

pub const Options = struct {
    max_cached_workflows: u32 = 1000,
    max_concurrent_workflow_task_executions: usize = 100,
    max_concurrent_activity_executions: usize = 100,
    max_concurrent_workflow_task_pollers: usize = 2,
    max_concurrent_activity_task_pollers: usize = 2,
    /// Empty uses the client's identity.
    identity: []const u8 = "",
    /// Stack size for each workflow coroutine.
    workflow_stack_size: usize = 256 * 1024,
    /// How long running activities may keep running after `stop` before they are canceled
    /// with reason `worker_shutdown`.
    graceful_shutdown_period: Io.Duration = .zero,
};

pub const Worker = struct {
    gpa: Allocator,
    io: Io,
    client: *client_mod.Client,
    task_queue: []u8,
    options: Options,
    workflows: std.StringHashMapUnmanaged(workflow.Registered) = .empty,
    activities: std.StringHashMapUnmanaged(activity.Registered) = .empty,
    /// The core worker while `run` is active, for `stop` on other threads.
    /// Guards `core_ptr` and `stop_requested`, which `stop` (any thread) and `run` share.
    stop_mutex: Io.Mutex = .init,
    core_ptr: ?*bridge.c.TemporalCoreWorker = null,
    stop_requested: bool = false,

    pub fn init(gpa: Allocator, io: Io, client: *client_mod.Client, task_queue: []const u8, options: Options) error{OutOfMemory}!Worker {
        return .{ .gpa = gpa, .io = io, .client = client, .task_queue = try gpa.dupe(u8, task_queue), .options = options };
    }

    pub fn deinit(w: *Worker) void {
        w.workflows.deinit(w.gpa);
        w.activities.deinit(w.gpa);
        w.gpa.free(w.task_queue);
        w.* = undefined;
    }

    /// Registers a workflow defined with `temporal.workflow.Def`.
    pub fn registerWorkflow(w: *Worker, comptime Workflow: type) error{ OutOfMemory, AlreadyRegistered }!void {
        const gop = try w.workflows.getOrPut(w.gpa, Workflow.workflow_name);
        if (gop.found_existing) return error.AlreadyRegistered;
        gop.value_ptr.* = Workflow.registered;
    }

    /// Registers an activity defined with `temporal.activity.Def`.
    pub fn registerActivity(w: *Worker, comptime Activity: type) error{ OutOfMemory, AlreadyRegistered }!void {
        return w.registerActivityImpl(Activity.registered);
    }

    /// Registers an activity that is a method (see `temporal.activity.Def`), bound to
    /// `receiver`. The receiver must outlive the worker and be safe to use from several
    /// activities at once.
    pub fn registerActivityWith(w: *Worker, comptime Activity: type, receiver: *Activity.Receiver) error{ OutOfMemory, AlreadyRegistered }!void {
        return w.registerActivityImpl(Activity.with(receiver));
    }

    /// Registers several method activities bound to one receiver: `activities` is a tuple of
    /// their `Def`s.
    pub fn registerActivitiesWith(w: *Worker, receiver: anytype, comptime activities: anytype) error{ OutOfMemory, AlreadyRegistered }!void {
        inline for (activities) |Activity| try w.registerActivityWith(Activity, receiver);
    }

    /// Registers an activity implementation under another activity's name, for example a
    /// test double standing in for `Activity`.
    pub fn registerActivityImpl(w: *Worker, registered: activity.Registered) error{ OutOfMemory, AlreadyRegistered }!void {
        const gop = try w.activities.getOrPut(w.gpa, registered.name);
        if (gop.found_existing) return error.AlreadyRegistered;
        gop.value_ptr.* = registered;
    }

    pub const RunError = error{ OutOfMemory, NothingRegistered, WorkerInitFailed, InvalidOptions, ValidationFailed, ShutdownFailed, ConcurrencyUnavailable };

    /// Polls and runs tasks until `stop` is called. Returns after in-flight work finishes.
    pub fn run(w: *Worker) RunError!void {
        if (w.workflows.count() == 0 and w.activities.count() == 0) return error.NothingRegistered;
        var message: ?[]u8 = null;
        defer if (message) |m| w.gpa.free(m);

        const core = bridge.Worker.init(w.gpa, w.client.connection, .{
            .namespace = w.client.namespace,
            .task_queue = w.task_queue,
            .identity_override = w.options.identity,
            .max_cached_workflows = w.options.max_cached_workflows,
            .tuner = .{
                .workflow = .{ .fixed = w.options.max_concurrent_workflow_task_executions },
                .activity = .{ .fixed = w.options.max_concurrent_activity_executions },
            },
            .task_types = .{
                .workflows = w.workflows.count() > 0,
                .remote_activities = w.activities.count() > 0,
                // The core runs local activities only on a worker that also polls workflows.
                .local_activities = w.workflows.count() > 0 and w.activities.count() > 0,
                .nexus = false,
            },
            .workflow_task_pollers = .{ .simple_maximum = w.options.max_concurrent_workflow_task_pollers },
            .activity_task_pollers = .{ .simple_maximum = w.options.max_concurrent_activity_task_pollers },
            .graceful_shutdown_period_ms = std.math.cast(u64, w.options.graceful_shutdown_period.toMilliseconds()) orelse 0,
        }, &message) catch |err| {
            log.err("creating the worker failed: {s}", .{message orelse @errorName(err)});
            return err;
        };
        defer core.deinit();
        core.validate(w.gpa, w.io, &message) catch |err| {
            log.err("worker validation failed: {s}", .{message orelse @errorName(err)});
            return err;
        };

        {
            w.stop_mutex.lockUncancelable(w.io);
            defer w.stop_mutex.unlock(w.io);
            w.core_ptr = core.ptr;
            if (w.stop_requested) core.initiateShutdown();
        }
        // On early returns, clear the pointer before `core.deinit` (deferred above) so `stop`
        // never uses a freed worker.
        defer {
            w.stop_mutex.lockUncancelable(w.io);
            w.core_ptr = null;
            w.stop_mutex.unlock(w.io);
        }
        log.info("Started Worker Namespace={s} TaskQueue={s}", .{ w.client.namespace, w.task_queue });

        var workflows_done = if (w.workflows.count() > 0) try w.io.concurrent(workflowLoop, .{ w, core }) else null;
        var activities_done = if (w.activities.count() > 0) try w.io.concurrent(activityLoop, .{ w, core }) else null;
        if (workflows_done) |*f| f.await(w.io);
        if (activities_done) |*f| f.await(w.io);

        // Finalizing moves the core worker out of its handle, without waiting for an
        // initiate_shutdown that `stop` may be running on another thread. A worker moved while
        // that call holds one of its locks keeps the lock held forever, and finalizing then
        // hangs. Taking the mutex waits for any such call; clearing the pointer stops new ones.
        {
            w.stop_mutex.lockUncancelable(w.io);
            defer w.stop_mutex.unlock(w.io);
            w.core_ptr = null;
        }
        core.finalizeShutdown(w.gpa, w.io, &message) catch |err| {
            log.err("worker shutdown failed: {s}", .{message orelse @errorName(err)});
            return err;
        };
        log.info("Stopped Worker Namespace={s} TaskQueue={s}", .{ w.client.namespace, w.task_queue });
    }

    /// Asks `run` to stop: polling ends, in-flight work finishes, then `run` returns. Running
    /// activities are canceled (reason `worker_shutdown`) once `graceful_shutdown_period`
    /// has passed. Safe to call from any thread, before or during `run`.
    pub fn stop(w: *Worker) void {
        w.stop_mutex.lockUncancelable(w.io);
        defer w.stop_mutex.unlock(w.io);
        w.stop_requested = true;
        if (w.core_ptr) |ptr| bridge.c.temporal_core_worker_initiate_shutdown(ptr);
    }

    /// Runs until the process receives SIGINT or SIGTERM.
    pub fn runUntilSignal(w: *Worker) RunError!void {
        const watcher = try SignalWatcher.start(w);
        defer watcher.finish();
        return w.run();
    }
};

// ---------------------------------------------------------------- workflows

fn workflowLoop(w: *Worker, core: bridge.Worker) void {
    var runner: WorkflowRunner = .{
        .gpa = w.gpa,
        .workflows = &w.workflows,
        .task_queue = w.task_queue,
        .namespace = w.client.namespace,
        .stack_size = w.options.workflow_stack_size,
    };
    defer runner.deinit();
    var message: ?[]u8 = null;
    defer if (message) |m| w.gpa.free(m);

    while (true) {
        const polled = core.pollWorkflowActivation(w.gpa, w.io, &message) catch |err| {
            log.err("polling for workflow tasks failed, stopping the worker: {s}", .{message orelse @errorName(err)});
            w.stop();
            continue;
        } orelse break;
        defer polled.deinit();
        runner.process(core, w.io, polled.bytes());
    }
}

/// Turns workflow activations into completions for one worker (or replayer): keeps the cache
/// of running workflow instances, creates them on InitializeWorkflow, and destroys them on
/// eviction. Used from a single thread.
pub const WorkflowRunner = struct {
    gpa: Allocator,
    workflows: *const std.StringHashMapUnmanaged(workflow.Registered),
    task_queue: []const u8,
    namespace: []const u8,
    stack_size: usize,
    runs: std.StringHashMapUnmanaged(*Instance) = .empty,
    /// Called for each eviction with the core's reason and message (the replayer uses it to
    /// detect nondeterminism).
    on_evict: ?*const fn (ctx: *anyopaque, reason: act_pb.RemoveFromCache.EvictionReason, message: []const u8) void = null,
    on_evict_ctx: *anyopaque = undefined,

    pub fn deinit(r: *WorkflowRunner) void {
        var it = r.runs.iterator();
        while (it.next()) |kv| {
            kv.value_ptr.*.destroy();
            r.gpa.free(kv.key_ptr.*);
        }
        r.runs.deinit(r.gpa);
    }

    /// Handles one serialized activation and sends its completion to `core`.
    pub fn process(r: *WorkflowRunner, core: bridge.Worker, io: Io, bytes: []const u8) void {
        var arena_state: std.heap.ArenaAllocator = .init(r.gpa);
        defer arena_state.deinit();
        const a = arena_state.allocator();
        const completion = r.handle(bytes, a) catch |err| {
            log.err("processing a workflow activation failed: {t}", .{err});
            return;
        };
        var encoded: Io.Writer.Allocating = .init(a);
        completion.encode(&encoded.writer, a) catch return;
        var message: ?[]u8 = null;
        defer if (message) |m| r.gpa.free(m);
        core.completeWorkflowActivation(r.gpa, io, encoded.written(), &message) catch |err| {
            log.err("completing a workflow activation failed: {s}", .{message orelse @errorName(err)});
        };
    }

    fn handle(r: *WorkflowRunner, bytes: []const u8, a: Allocator) !done_pb.WorkflowActivationCompletion {
        var reader: Io.Reader = .fixed(bytes);
        const act = try act_pb.WorkflowActivation.decode(&reader, a);

        // An eviction comes alone; complete it, then drop the run.
        for (act.jobs.items) |job| if (job.variant) |v| if (v == .remove_from_cache) {
            if (r.on_evict) |cb| cb(r.on_evict_ctx, v.remove_from_cache.reason, v.remove_from_cache.message);
            if (r.runs.fetchRemove(act.run_id)) |kv| {
                kv.value.destroy();
                r.gpa.free(kv.key);
            }
            return .{ .run_id = act.run_id, .status = .{ .successful = .{} } };
        };

        const inst = r.runs.get(act.run_id) orelse blk: {
            const workflow_type = for (act.jobs.items) |job| {
                if (job.variant) |v| if (v == .initialize_workflow) break v.initialize_workflow.workflow_type;
            } else return try taskFailure(a, act.run_id, "activation for a workflow run that is not cached and has no InitializeWorkflow job");
            const entry = r.workflows.get(workflow_type) orelse {
                const known = try registeredNames(a, r.workflows.*);
                return try taskFailure(a, act.run_id, try std.fmt.allocPrint(a, "workflow type \"{s}\" is not registered on this worker; registered: {s}", .{ workflow_type, known }));
            };
            const inst = try Instance.create(r.gpa, entry, .{
                .task_queue = r.task_queue,
                .namespace = r.namespace,
                .stack_size = r.stack_size,
            });
            errdefer inst.destroy();
            const key = try r.gpa.dupe(u8, act.run_id);
            errdefer r.gpa.free(key);
            try r.runs.put(r.gpa, key, inst);
            break :blk inst;
        };
        return inst.activate(&act, a) catch |err| try taskFailure(a, act.run_id, try std.fmt.allocPrint(a, "workflow activation failed: {t}", .{err}));
    }
};

fn taskFailure(a: Allocator, run_id: []const u8, message: []const u8) !done_pb.WorkflowActivationCompletion {
    log.warn("failing workflow task: {s}", .{message});
    return .{ .run_id = run_id, .status = .{ .failed = .{ .failure = try failure_mod.application(a, .{ .message = message }) } } };
}

fn registeredNames(a: Allocator, map: anytype) ![]const u8 {
    var out: std.ArrayList(u8) = .empty;
    var it = map.keyIterator();
    while (it.next()) |k| {
        if (out.items.len > 0) try out.appendSlice(a, ", ");
        try out.appendSlice(a, k.*);
    }
    return out.items;
}

// ---------------------------------------------------------------- activities

fn activityLoop(w: *Worker, core: bridge.Worker) void {
    var group: Io.Group = .init;
    var running: RunningActivities = .{};
    defer running.map.deinit(w.gpa);
    var message: ?[]u8 = null;
    defer if (message) |m| w.gpa.free(m);
    while (true) {
        const polled = core.pollActivityTask(w.gpa, w.io, &message) catch |err| {
            log.err("polling for activity tasks failed, stopping the worker: {s}", .{message orelse @errorName(err)});
            w.stop();
            continue;
        } orelse break;
        const run = receiveActivityTask(w, &running, polled) orelse continue;
        // Each activity gets its own thread: `async` would run it inline in this loop once the
        // Io has as many busy threads as CPUs, running activities one at a time. The core's
        // activity slots bound how many run at once.
        group.concurrent(w.io, superviseActivity, .{ w, core, &running, run }) catch {
            log.warn("no thread available for an activity; running it on the polling thread", .{});
            superviseActivity(w, core, &running, run);
        };
    }
    group.await(w.io) catch {};
}

/// The activities a worker is running, by task token, so the poll loop can deliver Cancel
/// tasks to them. The poll loop adds runs; each run's supervisor removes and frees its own.
/// A run is only touched by the poll loop while holding `mutex` and finding it in `map`.
const RunningActivities = struct {
    mutex: Io.Mutex = .init,
    /// Keys are the runs' task tokens, owned by the runs.
    map: std.StringHashMapUnmanaged(*RunningActivity) = .empty,
};

/// One activity, from its Start task until its result is reported.
const RunningActivity = struct {
    arena: std.heap.ArenaAllocator,
    polled: bridge.Worker.Polled,
    task_token: []const u8,
    start: task_pb.Start,
    ctx: activity.Context,
    /// Set when the activity returns or a cancel arrives; wakes the supervisor.
    wake: Io.Event = .unset,

    fn destroy(run: *RunningActivity, gpa: Allocator) void {
        run.arena.deinit();
        run.polled.deinit();
        gpa.destroy(run);
    }
};

/// Decodes a polled activity task. Returns a new run for a Start task; delivers a Cancel
/// task to its run and returns null.
fn receiveActivityTask(w: *Worker, running: *RunningActivities, polled: bridge.Worker.Polled) ?*RunningActivity {
    const run = w.gpa.create(RunningActivity) catch {
        log.err("out of memory receiving an activity task; the core will time it out", .{});
        polled.deinit();
        return null;
    };
    run.* = .{
        .arena = .init(w.gpa),
        .polled = polled,
        .task_token = &.{},
        .start = .{},
        .ctx = .{ .arena = undefined, .io_ = w.io, .info_ = undefined, .logger_ = .{} },
    };
    var keep = false;
    defer if (!keep) run.destroy(w.gpa);

    var reader: Io.Reader = .fixed(polled.bytes());
    const task = task_pb.ActivityTask.decode(&reader, run.arena.allocator()) catch |err| {
        log.err("decoding an activity task failed: {t}", .{err});
        return null;
    };
    switch (task.variant orelse return null) {
        .start => |s| {
            run.task_token = task.task_token;
            run.start = s;
            running.mutex.lockUncancelable(w.io);
            defer running.mutex.unlock(w.io);
            running.map.put(w.gpa, run.task_token, run) catch
                log.warn("out of memory tracking activity {s}; it runs, but cancels won't reach it", .{s.activity_id});
            keep = true;
            return run;
        },
        .cancel => |c| {
            running.mutex.lockUncancelable(w.io);
            defer running.mutex.unlock(w.io);
            // A run that already finished has nothing to cancel.
            const target = running.map.get(task.task_token) orelse return null;
            if (target.ctx.setCancelReason(cancelReason(c.reason))) target.wake.set(w.io);
            return null;
        },
    }
}

fn cancelReason(reason: task_pb.ActivityCancelReason) activity.CancelReason {
    return switch (reason) {
        .NOT_FOUND => .not_found,
        .CANCELLED => .canceled,
        .TIMED_OUT => .timed_out,
        .WORKER_SHUTDOWN => .worker_shutdown,
        .PAUSED => .paused,
        .RESET => .reset,
        _ => .canceled,
    };
}

/// Runs an activity on a task of its own, waits for it to return or for a cancel, cancels the
/// task through its `std.Io` on a cancel, and reports the result. Reporting happens here, not
/// on the activity's task, so a cancel arriving late can't interrupt the report.
fn superviseActivity(w: *Worker, core: bridge.Worker, running: *RunningActivities, run: *RunningActivity) void {
    defer {
        running.mutex.lockUncancelable(w.io);
        _ = running.map.remove(run.task_token);
        running.mutex.unlock(w.io);
        run.destroy(w.gpa);
    }
    const outcome = blk: {
        var task = w.io.concurrent(callActivity, .{ w, core, run }) catch {
            log.warn("no thread available for activity {s}; running it here, where cancels can't reach it", .{run.start.activity_id});
            break :blk callActivity(w, core, run);
        };
        run.wake.waitUncancelable(w.io);
        break :blk if (run.ctx.cancelReason() != null) task.cancel(w.io) else task.await(w.io);
    };
    const a = run.arena.allocator();
    const result = outcome catch return log.err("out of memory reporting activity {s}; the core will time it out", .{run.start.activity_id});
    var encoded: Io.Writer.Allocating = .init(a);
    (proto.coresdk.self.ActivityTaskCompletion{ .task_token = run.task_token, .result = result }).encode(&encoded.writer, a) catch return;
    var message: ?[]u8 = null;
    defer if (message) |m| w.gpa.free(m);
    core.completeActivityTask(w.gpa, w.io, encoded.written(), &message) catch |err| {
        log.err("completing an activity task failed: {s}", .{message orelse @errorName(err)});
    };
}

fn callActivity(w: *Worker, core: bridge.Worker, run: *RunningActivity) error{OutOfMemory}!result_pb.ActivityExecutionResult {
    defer run.wake.set(w.io);
    const a = run.arena.allocator();
    return executeActivity(w, core, run) catch |err| .{
        .status = .{ .failed = .{ .failure = try failure_mod.fromError(a, err) } },
    };
}

/// Sends an activity's heartbeats to the core, which throttles them and reports them to the
/// server.
const Heartbeats = struct {
    w: *Worker,
    core: bridge.Worker,
    task_token: []const u8,

    fn record(context: *anyopaque, details: []const proto.temporal.api.common.v1.Payload) void {
        const h: *Heartbeats = @ptrCast(@alignCast(context));
        var scratch: std.heap.ArenaAllocator = .init(h.w.gpa);
        defer scratch.deinit();
        const a = scratch.allocator();
        var heartbeat: proto.coresdk.self.ActivityHeartbeat = .{ .task_token = h.task_token };
        heartbeat.details.appendSlice(a, details) catch return;
        var encoded: Io.Writer.Allocating = .init(a);
        heartbeat.encode(&encoded.writer, a) catch return;
        var message: ?[]u8 = null;
        defer if (message) |m| h.w.gpa.free(m);
        h.core.recordActivityHeartbeat(h.w.gpa, encoded.written(), &message) catch {
            log.warn("recording an activity heartbeat failed: {s}", .{message orelse "unknown error"});
        };
    }
};

fn executeActivity(w: *Worker, core: bridge.Worker, run: *RunningActivity) !result_pb.ActivityExecutionResult {
    const a = run.arena.allocator();
    const start = &run.start;
    const entry = w.activities.get(start.activity_type) orelse {
        const known = try registeredNames(a, w.activities);
        return .{ .status = .{ .failed = .{ .failure = try failure_mod.application(a, .{
            .message = try std.fmt.allocPrint(a, "activity type \"{s}\" is not registered on this worker; registered: {s}", .{ start.activity_type, known }),
            .type = "ActivityNotRegistered",
        }) } } };
    };
    const execution = start.workflow_execution orelse proto.temporal.api.common.v1.WorkflowExecution{};
    // The context already exists: a cancel may have set its reason.
    const ctx = &run.ctx;
    ctx.arena = a;
    ctx.info_ = .{
        .activity_id = start.activity_id,
        .activity_type = start.activity_type,
        .task_queue = w.task_queue,
        .workflow_id = execution.workflow_id,
        .workflow_run_id = execution.run_id,
        .workflow_type = start.workflow_type,
        .workflow_namespace = start.workflow_namespace,
        .attempt = start.attempt,
    };
    ctx.logger_ = .{ .context = try log_mod.contextString(a, .{
        .ActivityID = start.activity_id,
        .ActivityType = start.activity_type,
        .Attempt = start.attempt,
        .WorkflowType = start.workflow_type,
        .WorkflowID = execution.workflow_id,
        .RunID = execution.run_id,
    }) };
    var heartbeats: Heartbeats = .{ .w = w, .core = core, .task_token = run.task_token };
    ctx.heartbeat_details = start.heartbeat_details.items;
    ctx.heartbeat_sink = .{ .gpa = w.gpa, .context = &heartbeats, .record = Heartbeats.record };
    const payload = entry.invoke(entry.receiver, ctx, start.input.items) catch |err| {
        if (err == error.Canceled) if (ctx.cancelReason()) |reason| return .{ .status = .{ .cancelled = .{ .failure = .{
            .message = try std.fmt.allocPrint(a, "activity canceled: {t}", .{reason}),
            .source = failure_mod.source,
            .failure_info = .{ .canceled_failure_info = .{} },
        } } } };
        const f = if (ctx.pending_failure) |p| try failure_mod.application(a, p) else try failure_mod.fromError(a, err);
        return .{ .status = .{ .failed = .{ .failure = f } } };
    };
    return .{ .status = .{ .completed = .{ .result = payload } } };
}

// ---------------------------------------------------------------- signals

/// Turns SIGINT and SIGTERM into `Worker.stop`. A signal handler may only do
/// async-signal-safe work, so it writes a byte to a pipe; a thread reading the pipe calls
/// `stop`.
const SignalWatcher = struct {
    thread: std.Thread,

    var pipe_fds: [2]std.c.fd_t = .{ -1, -1 };
    var previous: [2]std.posix.Sigaction = undefined;
    const signals = [_]std.posix.SIG{ .INT, .TERM };

    fn start(w: *Worker) !SignalWatcher {
        if (std.c.pipe(&pipe_fds) != 0) return error.OutOfMemory;
        const action: std.posix.Sigaction = .{
            .handler = .{ .handler = onSignal },
            .mask = std.posix.sigemptyset(),
            .flags = 0,
        };
        for (signals, &previous) |sig, *prev| std.posix.sigaction(sig, &action, prev);
        const thread = std.Thread.spawn(.{}, watch, .{w}) catch return error.OutOfMemory;
        return .{ .thread = thread };
    }

    fn finish(s: SignalWatcher) void {
        for (signals, &previous) |sig, *prev| std.posix.sigaction(sig, prev, null);
        // Wake the watcher if no signal came, then let it exit.
        _ = std.c.write(pipe_fds[1], "q", 1);
        s.thread.join();
        _ = std.c.close(pipe_fds[0]);
        _ = std.c.close(pipe_fds[1]);
        pipe_fds = .{ -1, -1 };
    }

    fn onSignal(_: std.posix.SIG) callconv(.c) void {
        _ = std.c.write(pipe_fds[1], "s", 1);
    }

    fn watch(w: *Worker) void {
        var byte: [1]u8 = undefined;
        while (true) {
            const n = std.c.read(pipe_fds[0], &byte, 1);
            if (n <= 0) return;
            if (byte[0] == 's') {
                log.info("Worker received a stop signal, shutting down", .{});
                w.stop();
            }
            if (byte[0] == 'q') return;
        }
    }
};

// ---------------------------------------------------------------- tests

const testing = std.testing;

const HelloTest = struct {
    fn greet(ctx: *activity.Context, name: []const u8) ![]const u8 {
        ctx.logger().info("Activity", .{ .name = name });
        return std.fmt.allocPrint(ctx.allocator(), "Hello {s}!", .{name});
    }
    const Greet = activity.Def("Activity", greet);

    fn hello(ctx: *workflow.Context, name: []const u8) ![]const u8 {
        ctx.logger().info("HelloWorld workflow started", .{ .name = name });
        const greeting = ctx.executeActivity(Greet, .{ .start_to_close_timeout = .fromSeconds(10) }, .{name});
        const result = try greeting.get();
        ctx.logger().info("HelloWorld workflow completed.", .{ .result = result });
        return result;
    }
    const Hello = workflow.Def("Workflow", hello);

    fn failing(ctx: *workflow.Context) ![]const u8 {
        _ = ctx;
        return error.NotToday;
    }
    const Failing = workflow.Def("Failing", failing);

    fn runWorker(w: *Worker) Worker.RunError!void {
        return w.run();
    }
};

test "a worker runs a workflow and its activity started by a client" {
    const rt = try bridge.Runtime.init(testing.allocator, .{}, null);
    defer rt.deinit();
    var server = try bridge.EphemeralServer.startDevServer(testing.allocator, testing.io, rt, .{}, null);
    defer server.shutdown(testing.io, null) catch {};

    var client = try client_mod.Client.connect(testing.allocator, testing.io, .{ .host_port = server.target });
    defer client.close();

    var w = try Worker.init(testing.allocator, testing.io, &client, "hello-world", .{});
    defer w.deinit();
    try w.registerWorkflow(HelloTest.Hello);
    try w.registerWorkflow(HelloTest.Failing);
    try w.registerActivity(HelloTest.Greet);
    var running = try testing.io.concurrent(HelloTest.runWorker, .{&w});
    defer {
        w.stop();
        running.await(testing.io) catch {};
    }

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var run = try client.executeWorkflow(HelloTest.Hello, .{ .id = "hello_world_workflowID", .task_queue = "hello-world" }, .{"Temporal"});
    defer run.deinit();
    try testing.expectEqualStrings("Hello Temporal!", try run.get(arena.allocator()));

    var failed = try client.executeWorkflow(HelloTest.Failing, .{ .id = "failing", .task_queue = "hello-world" }, .{});
    defer failed.deinit();
    try testing.expectError(error.WorkflowFailed, failed.get(arena.allocator()));
    try testing.expectEqualStrings("NotToday", failure_mod.applicationType(failed.failure().?.*).?);
}

// Small machines: std.Io.Threaded runs `async` work inline once it has as many busy threads
// as CPUs. Activities must still run concurrently, and the worker must still stop.
const SlowActivities = struct {
    fn nap(ctx: *activity.Context, ms: i64) !void {
        try ctx.io().sleep(.fromMilliseconds(ms), .awake);
    }
    const Nap = activity.Def("Nap", nap);

    fn napsInParallel(ctx: *workflow.Context, n: usize) !void {
        var futures: std.ArrayList(workflow.ActivityFuture(void)) = .empty;
        for (0..n) |_| try futures.append(ctx.allocator(), ctx.executeActivity(Nap, .{ .start_to_close_timeout = .fromSeconds(30) }, .{@as(i64, 1000)}));
        for (futures.items) |f| try f.get();
    }
    const Naps = workflow.Def("Naps", napsInParallel);
};

test "activities run concurrently and the worker stops when async work can't get a thread" {
    var threaded: std.Io.Threaded = .init(testing.allocator, .{ .async_limit = .nothing });
    defer threaded.deinit();
    const io = threaded.io();

    const rt = try bridge.Runtime.init(testing.allocator, .{}, null);
    defer rt.deinit();
    var server = try bridge.EphemeralServer.startDevServer(testing.allocator, io, rt, .{}, null);
    defer server.shutdown(io, null) catch {};
    var client = try client_mod.Client.connect(testing.allocator, io, .{ .host_port = server.target });
    defer client.close();

    var w = try Worker.init(testing.allocator, io, &client, "naps", .{});
    defer w.deinit();
    try w.registerWorkflow(SlowActivities.Naps);
    try w.registerActivity(SlowActivities.Nap);
    var running = try io.concurrent(HelloTest.runWorker, .{&w});

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const start = std.Io.Timestamp.now(io, .awake);
    var run = try client.executeWorkflow(SlowActivities.Naps, .{ .task_queue = "naps" }, .{5});
    defer run.deinit();
    try run.get(arena.allocator());
    const elapsed = start.durationTo(std.Io.Timestamp.now(io, .awake));
    w.stop();
    try running.await(io);
    // Five one-second activities in parallel, not in sequence.
    try testing.expect(elapsed.nanoseconds < 4 * std.time.ns_per_s);
}

const StopHammer = struct {
    w: *Worker,
    done: std.atomic.Value(bool) = .init(false),

    fn run(h: *StopHammer) void {
        while (!h.done.load(.acquire)) h.w.stop();
    }
};

// finalizeShutdown moves the core worker out of its handle, so a `stop` racing with the end of
// `run` must never reach the core worker once finalizing has begun (it used to abort the process
// or hang finalizing).
test "stop called repeatedly from another thread while the worker finishes shutting down" {
    const rt = try bridge.Runtime.init(testing.allocator, .{}, null);
    defer rt.deinit();
    var server = try bridge.EphemeralServer.startDevServer(testing.allocator, testing.io, rt, .{}, null);
    defer server.shutdown(testing.io, null) catch {};
    var client = try client_mod.Client.connect(testing.allocator, testing.io, .{ .host_port = server.target });
    defer client.close();

    for (0..5) |_| {
        var w = try Worker.init(testing.allocator, testing.io, &client, "stop-race", .{});
        defer w.deinit();
        try w.registerWorkflow(HelloTest.Hello);
        try w.registerActivity(HelloTest.Greet);
        var running = try testing.io.concurrent(HelloTest.runWorker, .{&w});
        var hammer: StopHammer = .{ .w = &w };
        const thread = try std.Thread.spawn(.{}, StopHammer.run, .{&hammer});
        const result = running.await(testing.io);
        hammer.done.store(true, .release);
        thread.join();
        try result;
    }
}

const CancelTest = struct {
    const Env = @import("testing.zig").WorkflowEnvironment;
    const EventType = proto.temporal.api.enums.v1.EventType;

    var started: Io.Event = .unset;
    /// Set once `beat` has seen a cancel; `seen_reason` is written before.
    var observed: Io.Event = .unset;
    var seen_reason: ?activity.CancelReason = null;

    /// Heartbeats until canceled. With `ignore`, swallows the cancel and returns a value.
    fn beat(ctx: *activity.Context, ignore: bool) ![]const u8 {
        started.set(ctx.io());
        while (true) {
            ctx.heartbeat({});
            ctx.io().sleep(.fromMilliseconds(50), .awake) catch |err| {
                seen_reason = ctx.cancelReason();
                observed.set(ctx.io());
                if (ignore) return "finished anyway";
                return err;
            };
        }
    }
    const Beat = activity.Def("Beat", beat);

    fn waitStarted(ctx: *activity.Context) !void {
        try started.wait(ctx.io());
    }
    const WaitStarted = activity.Def("WaitStarted", waitStarted);

    const beat_options: workflow.ActivityOptions = .{
        .start_to_close_timeout = .fromSeconds(60),
        .heartbeat_timeout = .fromSeconds(2),
        .cancellation_type = .wait_cancellation_completed,
    };

    /// Cancels `Beat` once it is running and returns its result, or "canceled".
    fn cancelBeat(ctx: *workflow.Context, ignore: bool) ![]const u8 {
        const beating = ctx.executeActivity(Beat, beat_options, .{ignore});
        try ctx.executeActivity(WaitStarted, .{ .start_to_close_timeout = .fromSeconds(60) }, .{}).get();
        beating.cancel();
        return beating.get() catch |err| switch (err) {
            error.ActivityFailed => if (failure_mod.rootCause(beating.failure().?).failure_info.? == .canceled_failure_info) "canceled" else err,
            else => err,
        };
    }
    const CancelBeat = workflow.Def("CancelBeat", cancelBeat);

    /// Runs `Beat`. When the workflow is canceled, `get` waits for the activity's
    /// cancellation to complete (`wait_cancellation_completed`), so the run closes canceled
    /// after the activity does.
    fn runBeat(ctx: *workflow.Context) ![]const u8 {
        return ctx.executeActivity(Beat, beat_options, .{false}).get();
    }
    const RunBeat = workflow.Def("RunBeat", runBeat);

    fn reset() void {
        started = .unset;
        observed = .unset;
        seen_reason = null;
    }

    fn countEvents(env: *Env, run: anytype, event_type: EventType) !usize {
        var arena: std.heap.ArenaAllocator = .init(testing.allocator);
        defer arena.deinit();
        const history = try env.client.fetchHistory(arena.allocator(), run.id, run.run_id);
        var n: usize = 0;
        for (history.events.items) |e| n += @intFromBool(e.event_type == event_type);
        return n;
    }

    fn start() !*Env {
        reset();
        const env = try Env.start(testing.allocator, testing.io);
        errdefer env.deinit();
        try env.registerWorkflow(CancelBeat);
        try env.registerWorkflow(RunBeat);
        try env.registerActivity(Beat);
        try env.registerActivity(WaitStarted);
        return env;
    }
};

test "a heartbeating activity canceled by its workflow sees the cancel and reports canceled" {
    const env = try CancelTest.start();
    defer env.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();

    var run = try env.startWorkflow(CancelTest.CancelBeat, .{ .task_queue = "" }, .{false});
    defer run.deinit();
    try testing.expectEqualStrings("canceled", try env.getResult(CancelTest.CancelBeat, &run, arena.allocator(), null));
    try testing.expectEqual(@as(?activity.CancelReason, .canceled), CancelTest.seen_reason);
    try testing.expectEqual(1, try CancelTest.countEvents(env, &run, .EVENT_TYPE_ACTIVITY_TASK_CANCELED));
}

test "an activity that ignores a cancel and returns a value completes normally" {
    const env = try CancelTest.start();
    defer env.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();

    var run = try env.startWorkflow(CancelTest.CancelBeat, .{ .task_queue = "" }, .{true});
    defer run.deinit();
    try testing.expectEqualStrings("finished anyway", try env.getResult(CancelTest.CancelBeat, &run, arena.allocator(), null));
    try testing.expectEqual(@as(?activity.CancelReason, .canceled), CancelTest.seen_reason);
    try testing.expectEqual(0, try CancelTest.countEvents(env, &run, .EVENT_TYPE_ACTIVITY_TASK_CANCELED));
    try testing.expectEqual(2, try CancelTest.countEvents(env, &run, .EVENT_TYPE_ACTIVITY_TASK_COMPLETED));
}

test "canceling a workflow cancels its heartbeating activity" {
    const env = try CancelTest.start();
    defer env.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();

    var run = try env.startWorkflow(CancelTest.RunBeat, .{ .task_queue = "" }, .{});
    defer run.deinit();
    try CancelTest.started.wait(testing.io);
    try env.client.cancelWorkflow(run.id, run.run_id, "testing activity cancellation");
    try testing.expectError(error.WorkflowCanceled, env.getResult(CancelTest.RunBeat, &run, arena.allocator(), null));
    try testing.expectEqual(@as(?activity.CancelReason, .canceled), CancelTest.seen_reason);
    try testing.expectEqual(1, try CancelTest.countEvents(env, &run, .EVENT_TYPE_ACTIVITY_TASK_CANCELED));
}

test "stopping the worker cancels running activities with reason worker_shutdown" {
    const env = try CancelTest.start();
    defer env.deinit();

    var run = try env.startWorkflow(CancelTest.RunBeat, .{ .task_queue = "" }, .{});
    defer run.deinit();
    try CancelTest.started.wait(testing.io);
    env.worker.stop();
    try env.running.?.await(testing.io);
    env.running = null;
    try testing.expectEqual(@as(?activity.CancelReason, .worker_shutdown), CancelTest.seen_reason);
}
