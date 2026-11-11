//! Workers (Go `worker` package parity): poll a task queue and run the workflows and
//! activities registered on it.
//!
//!     var w = try temporal.worker.Worker.init(gpa, io, &client, "hello-world", .{});
//!     defer w.deinit();
//!     try w.registerWorkflow(HelloWorld);
//!     try w.registerActivity(Greet);
//!     try w.runUntilSignal(); // stops on SIGINT or SIGTERM
//!
//! Workflow activations are processed one at a time on one thread, each on its run's
//! deterministic Io. Activities run concurrently on the worker's Io.

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
    core_ptr: std.atomic.Value(?*bridge.c.TemporalCoreWorker) = .init(null),
    stop_requested: std.atomic.Value(bool) = .init(false),

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
                .local_activities = false,
                .nexus = false,
            },
            .workflow_task_pollers = .{ .simple_maximum = w.options.max_concurrent_workflow_task_pollers },
            .activity_task_pollers = .{ .simple_maximum = w.options.max_concurrent_activity_task_pollers },
        }, &message) catch |err| {
            log.err("creating the worker failed: {s}", .{message orelse @errorName(err)});
            return err;
        };
        defer core.deinit();
        core.validate(w.gpa, w.io, &message) catch |err| {
            log.err("worker validation failed: {s}", .{message orelse @errorName(err)});
            return err;
        };

        w.core_ptr.store(core.ptr, .release);
        defer w.core_ptr.store(null, .release);
        if (w.stop_requested.load(.acquire)) core.initiateShutdown();
        log.info("Started Worker Namespace={s} TaskQueue={s}", .{ w.client.namespace, w.task_queue });

        var workflows_done = if (w.workflows.count() > 0) try w.io.concurrent(workflowLoop, .{ w, core }) else null;
        var activities_done = if (w.activities.count() > 0) try w.io.concurrent(activityLoop, .{ w, core }) else null;
        if (workflows_done) |*f| f.await(w.io);
        if (activities_done) |*f| f.await(w.io);

        core.finalizeShutdown(w.gpa, w.io, &message) catch |err| {
            log.err("worker shutdown failed: {s}", .{message orelse @errorName(err)});
            return err;
        };
        log.info("Stopped Worker Namespace={s} TaskQueue={s}", .{ w.client.namespace, w.task_queue });
    }

    /// Asks `run` to stop: polling ends, in-flight work finishes, then `run` returns. Safe to
    /// call from any thread, before or during `run`.
    pub fn stop(w: *Worker) void {
        w.stop_requested.store(true, .release);
        if (w.core_ptr.load(.acquire)) |ptr| bridge.c.temporal_core_worker_initiate_shutdown(ptr);
    }

    /// Runs until the process receives SIGINT or SIGTERM. Go `worker.Run(worker.InterruptCh())`.
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
    var message: ?[]u8 = null;
    defer if (message) |m| w.gpa.free(m);
    while (true) {
        const polled = core.pollActivityTask(w.gpa, w.io, &message) catch |err| {
            log.err("polling for activity tasks failed, stopping the worker: {s}", .{message orelse @errorName(err)});
            w.stop();
            continue;
        } orelse break;
        group.async(w.io, runActivityTask, .{ w, core, polled });
    }
    group.await(w.io) catch {};
}

fn runActivityTask(w: *Worker, core: bridge.Worker, polled: bridge.Worker.Polled) void {
    defer polled.deinit();
    var arena_state: std.heap.ArenaAllocator = .init(w.gpa);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var reader: Io.Reader = .fixed(polled.bytes());
    const task = task_pb.ActivityTask.decode(&reader, a) catch |err| {
        log.err("decoding an activity task failed: {t}", .{err});
        return;
    };
    const variant = task.variant orelse return;
    const start = switch (variant) {
        .start => |s| s,
        // Cancellation of running activities is not delivered yet in this version; the
        // activity's own result completes the task.
        .cancel => return,
    };

    const result = executeActivity(w, a, &start) catch |err| result_pb.ActivityExecutionResult{
        .status = .{ .failed = .{ .failure = failure_mod.fromError(a, err) catch return } },
    };
    var encoded: Io.Writer.Allocating = .init(a);
    (proto.coresdk.self.ActivityTaskCompletion{ .task_token = task.task_token, .result = result }).encode(&encoded.writer, a) catch return;
    var message: ?[]u8 = null;
    defer if (message) |m| w.gpa.free(m);
    core.completeActivityTask(w.gpa, w.io, encoded.written(), &message) catch |err| {
        log.err("completing an activity task failed: {s}", .{message orelse @errorName(err)});
    };
}

fn executeActivity(w: *Worker, a: Allocator, start: *const task_pb.Start) !result_pb.ActivityExecutionResult {
    const entry = w.activities.get(start.activity_type) orelse {
        const known = try registeredNames(a, w.activities);
        return .{ .status = .{ .failed = .{ .failure = try failure_mod.application(a, .{
            .message = try std.fmt.allocPrint(a, "activity type \"{s}\" is not registered on this worker; registered: {s}", .{ start.activity_type, known }),
            .type = "ActivityNotRegistered",
        }) } } };
    };
    const execution = start.workflow_execution orelse proto.temporal.api.common.v1.WorkflowExecution{};
    var ctx: activity.Context = .{
        .arena = a,
        .io_ = w.io,
        .info_ = .{
            .activity_id = start.activity_id,
            .activity_type = start.activity_type,
            .task_queue = w.task_queue,
            .workflow_id = execution.workflow_id,
            .workflow_run_id = execution.run_id,
            .workflow_type = start.workflow_type,
            .workflow_namespace = start.workflow_namespace,
            .attempt = start.attempt,
        },
        .logger_ = .{ .context = try log_mod.contextString(a, .{
            .ActivityID = start.activity_id,
            .ActivityType = start.activity_type,
            .Attempt = start.attempt,
            .WorkflowType = start.workflow_type,
            .WorkflowID = execution.workflow_id,
            .RunID = execution.run_id,
        }) },
    };
    const payload = entry.invoke(&ctx, start.input.items) catch |err| {
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
