//! Workers: poll a task queue and run the workflows and
//! activities registered on it.
//!
//!     var w = try temporal.worker.Worker.init(gpa, io, &client, "hello-world", .{});
//!     defer w.deinit();
//!     try w.registerWorkflow(HelloWorld);
//!     try w.registerActivity(Greet);
//!     try w.runUntilSignal(); // stops on SIGINT or SIGTERM
//!
//! To start and stop a worker without blocking, run `run` on a concurrent task; there is no
//! separate start function. `run`'s error, such as `error.PollFailed`, arrives at `await`:
//!
//!     var running = try io.concurrent(temporal.worker.Worker.run, .{&w});
//!     try w.waitUntilRunning();
//!     // ... the worker polls while this code does other work ...
//!     w.stop();
//!     try running.await(io); // error.PollFailed if polling failed; see fatalMessage
//!
//! Workflow activations of different runs are processed in parallel, each on a task of its
//! own and on its run's deterministic Io; `max_concurrent_workflow_task_executions` bounds how
//! many at once. A run is processed by one thread at a time, but may move to another thread
//! between activations. Activities run concurrently on the worker's Io, each on its own task
//! so a cancel from the core reaches it as `std.Io` cancellation.

const std = @import("std");
const bridge = @import("bridge.zig");
const proto = @import("proto.zig");
const converter = @import("converter.zig");
const failure_mod = @import("failure.zig");
const log_mod = @import("log.zig");
const workflow = @import("workflow.zig");
const activity = @import("activity.zig");
const client_mod = @import("client.zig");
const propagation = @import("propagation.zig");
const interceptor = @import("interceptor.zig");
const metrics_mod = @import("metrics.zig");
const Instance = @import("workflow/Instance.zig");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const act_pb = proto.coresdk.workflow_activation;
const done_pb = proto.coresdk.workflow_completion;
const task_pb = proto.coresdk.activity_task;
const result_pb = proto.coresdk.activity_result;
const log = std.log.scoped(.temporal);

pub const Options = struct {
    /// How many workflow runs the worker keeps in memory between workflow tasks. Over the
    /// limit, the least recently used run is evicted, and replayed from its history when its
    /// next task arrives. 0 disables the cache: every task replays its run from the start.
    max_cached_workflows: u32 = 1000,
    /// How many workflow activations the worker processes at once, each on its own thread,
    /// for different runs.
    max_concurrent_workflow_task_executions: usize = 100,
    max_concurrent_activity_executions: usize = 100,
    max_concurrent_workflow_task_pollers: usize = 2,
    max_concurrent_activity_task_pollers: usize = 2,
    /// Empty uses the client's identity.
    identity: []const u8 = "",
    /// Stack size for each workflow coroutine.
    workflow_stack_size: usize = 256 * 1024,
    /// The most memory, in bytes, one cached workflow run may hold. Null (the default): no
    /// limit.
    ///
    /// Counted: the live bytes the run allocated from `ctx.allocator()` (and the runtime's
    /// own bookkeeping for the run, which uses the same allocator), plus one
    /// `workflow_stack_size` for each coroutine stack the run holds, running or kept for
    /// reuse. The figure depends on what the workflow did, so a run rebuilt by replay counts
    /// the same as one cached since its start, with one exception: a `ctx.sideEffect` value
    /// is encoded and recorded only when it first runs, so a cached run counts those
    /// encodings and a replayed one doesn't. A run within that margin of the limit can fail
    /// its task on the worker that has it cached and pass once evicted and replayed.
    ///
    /// Checked once at the end of each activation that runs workflow code, after every
    /// coroutine has blocked. A run over the limit fails that workflow task with an
    /// application failure of type `WorkflowMemoryLimitExceeded` whose message gives the
    /// numbers; the workflow itself does not fail, and the server retries the task, which
    /// replays the history and fails at the same point until the limit is raised or the
    /// workflow keeps less state (for example, continues as new sooner). Allocations are never
    /// refused, so one activation can go over before the check. Activations that only answer
    /// queries, and one that finishes the workflow, are not checked. The workflow logger
    /// also records the failure, except while replaying unless `log_during_replay` is set.
    workflow_memory_limit: ?usize = null,
    /// How long running activities may keep running after `stop` before they are canceled
    /// with reason `worker_shutdown`.
    graceful_shutdown_period: Io.Duration = .zero,
    /// Where workflow and activity loggers write. Null uses the client's
    /// (`client.Options.logger`). Borrowed: it must outlive the worker. Called concurrently
    /// from workflow and activity threads.
    logger: ?log_mod.Sink = null,
    /// Keep workflow loggers on while workflows replay history. By default they are silent
    /// during replay, so each message is logged once.
    log_during_replay: bool = false,
    /// Interceptors of this worker's workflows and activities, after the client's
    /// (`client.Options.worker_interceptors`); the first is the outermost. Borrowed: they
    /// must outlive the worker. Called concurrently from workflow and activity threads.
    interceptors: []const interceptor.Worker = &.{},
    /// Makes this worker a version of a Worker Deployment (see `temporal.deployment`). Null
    /// leaves it unversioned.
    deployment: ?DeploymentOptions = null,
    /// Identifies this build of the worker's code to the server, without a deployment. Must
    /// be empty with `deployment`, which carries its own.
    build_id: []const u8 = "",
    /// Legacy build-ID versioning: the server routes tasks by `build_id` with the task
    /// queue's build-ID compatibility or assignment rules, which the server must have
    /// enabled. Superseded by `deployment`; requires `build_id`.
    use_build_id_versioning: bool = false,
    /// Makes this worker a session host (see `workflow.Session`): it also takes new sessions
    /// from `<task_queue>__temporal_sessions` and runs session activities on a task queue of
    /// its own, `<task_queue>@<host name>@<UUID>`, unique to each `run`. Null takes no
    /// sessions.
    sessions: ?SessionWorkerOptions = null,
};

/// How a worker hosts sessions (`Options.sessions`).
///
/// Session activities run on the host queue with a slot pool of their own,
/// `max_concurrent_activity_executions + max_concurrent_sessions` slots, separate from the
/// main task queue's `max_concurrent_activity_executions`: one host may run both pools'
/// activities at once.
pub const SessionWorkerOptions = struct {
    /// How many sessions this worker holds open at once, recreated ones included; must be
    /// positive. A full worker stops taking new sessions from the shared queue, and refuses a
    /// session recreated on its own queue with a non-retryable `SessionWorkerFull` failure.
    ///
    /// Known limit: a worker filled by recreated sessions still takes new sessions from the
    /// shared queue and refuses them the same way, even when another worker has room.
    ///
    /// `std.math.maxInt(usize)` means no limit; the host queue's slot count saturates.
    max_concurrent_sessions: usize = 1000,
};

/// A worker's Worker Deployment Version and how the server uses it.
pub const DeploymentOptions = struct {
    deployment_name: []const u8,
    /// This build of the worker's code; with `deployment_name`, the worker's version.
    build_id: []const u8,
    /// Whether the server routes workflow tasks by version: to the deployment's current or
    /// ramping version, and pinned runs to their own. False only reports the version, and
    /// sends no versioning behaviors.
    use_worker_versioning: bool = true,
    /// The behavior of workflows whose `Def` doesn't declare one (`workflow.DefOptions`).
    /// With `use_worker_versioning` and no default, every registered `Def` must declare
    /// one, or `run` returns `error.InvalidOptions`. Must be `.unspecified` without
    /// `use_worker_versioning`.
    default_versioning_behavior: workflow.VersioningBehavior = .unspecified,
};

pub const Worker = struct {
    gpa: Allocator,
    io: Io,
    client: *client_mod.Client,
    task_queue: []u8,
    options: Options,
    workflows: std.StringHashMapUnmanaged(workflow.Registered) = .empty,
    activities: std.StringHashMapUnmanaged(activity.Registered) = .empty,
    /// Runs workflow types with no entry in `workflows`, if set.
    dynamic_workflow: ?workflow.Registered = null,
    /// Runs activity types with no entry in `activities`, if set.
    dynamic_activity: ?activity.Registered = null,
    /// The core workers while `run` is active, for `stop` on other threads: the task queue's,
    /// then with sessions, the session queue's and the host queue's.
    /// Guards `core_ptrs`, `stop_requested`, `fatal` and `fatal_message`, which `stop` (any
    /// thread), the poll loops and `run` share.
    stop_mutex: Io.Mutex = .init,
    core_ptrs: [3]?*bridge.c.TemporalCoreWorker = @splat(null),
    stop_requested: bool = false,
    /// The first poll failure of the current or last run, and its text (owned; null if it
    /// couldn't be allocated). See `fatalMessage`.
    fatal: ?PollError = null,
    fatal_message: ?[]u8 = null,
    /// Set once `run` has registered the worker with its client and started polling, or has
    /// failed before getting there (`start_failed`). See `waitUntilRunning`.
    started: Io.Event = .unset,
    start_failed: bool = false,
    /// The client's worker interceptors, then `options.interceptors`, while `run` is active.
    interceptors: []const interceptor.Worker = &.{},
    /// The sessions this worker holds, while `run` is active with `options.sessions`.
    session_host: ?*SessionHost = null,

    pub fn init(gpa: Allocator, io: Io, client: *client_mod.Client, task_queue: []const u8, options: Options) error{OutOfMemory}!Worker {
        return .{ .gpa = gpa, .io = io, .client = client, .task_queue = try gpa.dupe(u8, task_queue), .options = options };
    }

    pub fn deinit(w: *Worker) void {
        w.workflows.deinit(w.gpa);
        w.activities.deinit(w.gpa);
        w.gpa.free(w.task_queue);
        if (w.fatal_message) |m| w.gpa.free(m);
        w.* = undefined;
    }

    /// Registers a workflow defined with `temporal.workflow.Def`.
    pub fn registerWorkflow(w: *Worker, comptime Workflow: type) error{ OutOfMemory, AlreadyRegistered }!void {
        if (!@hasDecl(Workflow, "workflow_name"))
            @compileError("registerWorkflow takes a temporal.workflow.Def; register a temporal.workflow.DynamicDef with registerDynamicWorkflow");
        const gop = try w.workflows.getOrPut(w.gpa, Workflow.workflow_name);
        if (gop.found_existing) return error.AlreadyRegistered;
        gop.value_ptr.* = Workflow.registered;
    }

    /// Registers an activity defined with `temporal.activity.Def`, under the Def's name.
    /// Every activity registration follows the naming rules on `temporal.activity.Def`: a
    /// name already registered returns `error.AlreadyRegistered`, names starting with
    /// "__temporal_" return `error.ReservedActivityName`, and Defs naming one function under
    /// different names register as separate activities.
    pub fn registerActivity(w: *Worker, comptime Activity: type) RegisterActivityError!void {
        return w.registerActivityImpl(Activity.registered);
    }

    /// Registers an activity that is a method (see `temporal.activity.Def`), bound to
    /// `receiver`. The receiver must outlive the worker and be safe to use from several
    /// activities at once. Naming rules: see `temporal.activity.Def`.
    pub fn registerActivityWith(w: *Worker, comptime Activity: type, receiver: *Activity.Receiver) RegisterActivityError!void {
        return w.registerActivityImpl(Activity.with(receiver));
    }

    /// Registers several method activities bound to one receiver: `activities` is a tuple of
    /// their `Def`s. Naming rules: see `temporal.activity.Def`.
    pub fn registerActivitiesWith(w: *Worker, receiver: anytype, comptime activities: anytype) RegisterActivityError!void {
        inline for (activities) |Activity| try w.registerActivityWith(Activity, receiver);
    }

    /// Registers every activity method of the struct `impl` points to, bound to `impl`.
    /// `acts` is the value of `temporal.activity.defs(T, options)` for `impl`'s type `T`, the
    /// same value workflows call the activities through, so the worker registers exactly the
    /// names workflows schedule: `options.prefix` plus each method name. Declare it once, for
    /// example as a pub constant next to the workflow, outside `T`: no pub declaration of `T`
    /// may depend on it (see `defs`).
    /// Naming rules: see `temporal.activity.Def`. `impl` must outlive the worker and be safe
    /// to use from several activities at once. On an error, the methods before the failing
    /// one stay registered.
    pub fn registerActivities(w: *Worker, impl: anytype, comptime acts: anytype) RegisterActivityError!void {
        const bad = "registerActivities takes a mutable pointer to a struct instance, not " ++ @typeName(@TypeOf(impl));
        const T = switch (@typeInfo(@TypeOf(impl))) {
            .pointer => |p| if (p.size == .one and !p.attrs.@"const") p.child else @compileError(bad),
            else => @compileError(bad),
        };
        const Acts = @TypeOf(acts);
        const bad_acts = "registerActivities takes temporal.activity.defs(" ++ @typeName(T) ++ ", options) for its activities, not " ++ @typeName(Acts);
        const methods = switch (@typeInfo(Acts)) {
            .@"struct" => |s| s.field_names,
            else => @compileError(bad_acts),
        };
        if (methods.len == 0) @compileError(bad_acts);
        inline for (methods) |method| {
            const Activity = @field(acts, method);
            if (@TypeOf(Activity) != type) @compileError(bad_acts);
            if (!@hasDecl(Activity, "Receiver") or Activity.Receiver != T) @compileError(bad_acts);
            try w.registerActivityImpl(Activity.with(impl));
        }
    }

    /// Registers an activity implementation under its `name`, for example a test double
    /// standing in for another activity: build it with `temporal.activity.Def(name, impl)`
    /// and pass `.registered` or `.with(receiver)`.
    pub fn registerActivityImpl(w: *Worker, registered: activity.Registered) RegisterActivityError!void {
        if (std.mem.startsWith(u8, registered.name, Instance.reserved_activity_prefix)) {
            log.warn("registering activity type \"{s}\" failed: activity types starting with \"{s}\" are reserved for the SDK; rename the activity", .{ registered.name, Instance.reserved_activity_prefix });
            return error.ReservedActivityName;
        }
        const gop = try w.activities.getOrPut(w.gpa, registered.name);
        if (gop.found_existing) {
            log.warn("registering activity type \"{s}\" failed: the worker already has an activity registered under that name; register each name once (registerActivities names each method as its prefix plus the method name)", .{registered.name});
            return error.AlreadyRegistered;
        }
        gop.value_ptr.* = registered;
    }

    pub const RegisterActivityError = error{ OutOfMemory, AlreadyRegistered, ReservedActivityName };

    /// Registers a dynamic workflow (`temporal.workflow.DynamicDef`), which runs every
    /// workflow type that has no `Def` registered: a registered `Def` always wins. A worker
    /// has at most one; a second returns `error.AlreadyRegistered`.
    pub fn registerDynamicWorkflow(w: *Worker, comptime Dynamic: type) error{AlreadyRegistered}!void {
        if (@hasDecl(Dynamic, "workflow_name"))
            @compileError("registerDynamicWorkflow takes a temporal.workflow.DynamicDef; register workflow \"" ++ Dynamic.workflow_name ++ "\" with registerWorkflow");
        if (w.dynamic_workflow != null) {
            log.warn("registering a dynamic workflow failed: this worker already has one; a worker runs at most one dynamic workflow", .{});
            return error.AlreadyRegistered;
        }
        w.dynamic_workflow = Dynamic.registered;
    }

    /// Registers a dynamic activity (`temporal.activity.DynamicDef`), which runs every
    /// activity type that has no `Def` registered: a registered `Def` always wins. A worker
    /// has at most one; a second returns `error.AlreadyRegistered`.
    pub fn registerDynamicActivity(w: *Worker, comptime Dynamic: type) error{AlreadyRegistered}!void {
        return w.registerDynamicActivityImpl(Dynamic.registered);
    }

    /// Registers a dynamic activity that is a method, bound to `receiver`, as
    /// `registerActivityWith` does for a `Def`.
    pub fn registerDynamicActivityWith(w: *Worker, comptime Dynamic: type, receiver: *Dynamic.Receiver) error{AlreadyRegistered}!void {
        return w.registerDynamicActivityImpl(Dynamic.with(receiver));
    }

    fn registerDynamicActivityImpl(w: *Worker, registered: activity.Registered) error{AlreadyRegistered}!void {
        if (w.dynamic_activity != null) {
            log.warn("registering a dynamic activity failed: this worker already has one; a worker runs at most one dynamic activity", .{});
            return error.AlreadyRegistered;
        }
        w.dynamic_activity = registered;
    }

    fn hasWorkflows(w: *const Worker) bool {
        return w.workflows.count() > 0 or w.dynamic_workflow != null;
    }

    fn hasActivities(w: *const Worker) bool {
        return w.activities.count() > 0 or w.dynamic_activity != null;
    }

    const PollError = bridge.Worker.PollError;

    /// Errors from `run`:
    /// - `NothingRegistered`, `InvalidOptions`: the worker's registrations or options, before
    ///   anything starts (the reason is logged).
    /// - `WorkerInitFailed`, `ValidationFailed`: the core rejected the worker or the server
    ///   rejected its namespace or task queue (the core's message is logged).
    /// - `ConcurrencyUnavailable`: the worker's Io couldn't start a poll loop.
    /// - `PollFailed`: the core gave up polling for tasks; `fatalMessage` has its text.
    /// - `ShutdownFailed`: finalizing a core worker failed (logged).
    /// - `OutOfMemory`.
    pub const RunError = error{ OutOfMemory, NothingRegistered, WorkerInitFailed, InvalidOptions, ValidationFailed, ShutdownFailed, ConcurrencyUnavailable, PollFailed };

    /// Polls and runs tasks until `stop` is called, or until polling fails for good. Returns
    /// after in-flight work finishes and every core worker is shut down:
    /// - success after `stop` (or a signal, with `runUntilSignal`);
    /// - `error.PollFailed` (or `error.OutOfMemory`) when the core gave up polling, after
    ///   shutting down as `stop` does; `fatalMessage` has the task queue and the core's text;
    /// - an error from starting the worker or from shutting it down. A loop that couldn't
    ///   start is returned before a poll failure, and a poll failure before a failed shutdown.
    pub fn run(w: *Worker) RunError!void {
        {
            w.stop_mutex.lockUncancelable(w.io);
            defer w.stop_mutex.unlock(w.io);
            if (w.fatal_message) |m| w.gpa.free(m);
            w.fatal_message = null;
            w.fatal = null;
        }
        var running = false;
        defer if (!running) {
            w.start_failed = true;
            w.started.set(w.io);
        };
        if (!w.hasWorkflows() and !w.hasActivities()) return error.NothingRegistered;
        if (w.options.sessions) |o| if (o.max_concurrent_sessions == 0) {
            log.warn("worker for task queue {s}: sessions.max_concurrent_sessions must be positive", .{w.task_queue});
            return error.InvalidOptions;
        };
        const interceptors = try std.mem.concat(w.gpa, interceptor.Worker, &.{ w.client.worker_interceptors, w.options.interceptors });
        defer w.gpa.free(interceptors);
        w.interceptors = interceptors;
        defer w.interceptors = &.{};
        var message: ?[]u8 = null;
        defer if (message) |m| w.gpa.free(m);

        const strategy = try w.versioningStrategy();
        const created_on = w.client.currentConnection();
        const core = try w.createCore(created_on, .{
            .namespace = w.client.namespace,
            .task_queue = w.task_queue,
            .versioning = strategy,
            .identity_override = w.options.identity,
            .max_cached_workflows = w.options.max_cached_workflows,
            .tuner = .{
                .workflow = .{ .fixed = w.options.max_concurrent_workflow_task_executions },
                .activity = .{ .fixed = w.options.max_concurrent_activity_executions },
            },
            .task_types = .{
                .workflows = w.hasWorkflows(),
                .remote_activities = w.hasActivities(),
                // The core runs local activities only on a worker that also polls workflows.
                // Every workflow worker runs them: side effects are recorded by one.
                .local_activities = w.hasWorkflows(),
                .nexus = false,
            },
            .workflow_task_pollers = .{ .simple_maximum = w.options.max_concurrent_workflow_task_pollers },
            .activity_task_pollers = .{ .simple_maximum = w.options.max_concurrent_activity_task_pollers },
            .graceful_shutdown_period_ms = std.math.cast(u64, w.options.graceful_shutdown_period.toMilliseconds()) orelse 0,
        }, &message);
        defer core.deinit();

        // Polled even without registered activities: a workflow's side effects run as local
        // activities.
        var activity_cores: [3]ActivityCore = undefined;
        activity_cores[0] = .{ .core = core, .task_queue = w.task_queue };
        var core_count: usize = 1;
        defer for (activity_cores[1..core_count]) |c| c.core.deinit();
        var host: ?SessionHost = null;
        defer if (host) |*h| h.deinit();
        if (w.options.sessions) |o| {
            host = try SessionHost.init(w, o.max_concurrent_sessions);
            // Session queues are unversioned, so a rollout can't strand a host's sessions.
            for ([_][]const u8{ host.?.creation_queue, host.?.host_queue }, sessionSlots(w.options.max_concurrent_activity_executions, o.max_concurrent_sessions)) |queue, slots| {
                activity_cores[core_count] = .{ .task_queue = queue, .core = try w.createCore(created_on, .{
                    .namespace = w.client.namespace,
                    .task_queue = queue,
                    .identity_override = w.options.identity,
                    .max_cached_workflows = w.options.max_cached_workflows,
                    .tuner = .{ .activity = .{ .fixed = slots } },
                    .task_types = .{ .workflows = false, .remote_activities = true, .local_activities = false, .nexus = false },
                    .activity_task_pollers = .{ .simple_maximum = w.options.max_concurrent_activity_task_pollers },
                    .graceful_shutdown_period_ms = std.math.cast(u64, w.options.graceful_shutdown_period.toMilliseconds()) orelse 0,
                }, &message) };
                core_count += 1;
            }
        }
        const cores = activity_cores[0..core_count];

        // `Client.reconnect` moves the workers to its new connection while they are tracked.
        var tracked: usize = 0;
        defer for (cores[0..tracked]) |c| w.client.untrackWorker(c.core.ptr);
        for (cores) |c| {
            try w.client.trackWorker(c.core.ptr, created_on);
            tracked += 1;
        }
        w.session_host = if (host) |*h| h else null;
        defer w.session_host = null;
        {
            w.stop_mutex.lockUncancelable(w.io);
            defer w.stop_mutex.unlock(w.io);
            for (cores, 0..) |c, i| w.core_ptrs[i] = c.core.ptr;
            if (w.stop_requested) for (cores) |c| c.core.initiateShutdown();
        }
        // On early returns, clear the pointers before the cores are freed (deferred above) so
        // `stop` never uses a freed worker.
        defer {
            w.stop_mutex.lockUncancelable(w.io);
            w.core_ptrs = @splat(null);
            w.stop_mutex.unlock(w.io);
        }
        log.info("Started Worker Namespace={s} TaskQueue={s}", .{ w.client.namespace, w.task_queue });
        if (host) |h| log.info("Worker hosts sessions TaskQueue={s} SessionTaskQueue={s} HostTaskQueue={s}", .{ w.task_queue, h.creation_queue, h.host_queue });
        running = true;
        w.started.set(w.io);

        var workflows_done = if (w.hasWorkflows()) try w.io.concurrent(workflowLoop, .{ w, core }) else null;
        // Once a loop polls a core, returning early would free the core worker under it: shut
        // down like a normal stop, drain every poller (finalizing waits for each to return
        // null), finalize, then return the error.
        var start_failed: ?RunError = null;
        var activity_loops: [3]?Io.Future(void) = @splat(null);
        for (cores, activity_loops[0..core_count], 0..) |c, *loop, i| {
            if (start_failed == null) {
                if (w.io.concurrent(activityLoop, .{ w, c.core, c.task_queue })) |f| {
                    loop.* = f;
                    continue;
                } else |err| {
                    if (workflows_done == null and i == 0) return err;
                    log.warn("starting the activity loop for task queue {s} failed, stopping the worker: {t}", .{ c.task_queue, err });
                    start_failed = err;
                    w.stop_mutex.lockUncancelable(w.io);
                    defer w.stop_mutex.unlock(w.io);
                    for (cores) |other| other.core.initiateShutdown();
                }
            }
            activityLoop(w, c.core, c.task_queue);
        }
        if (workflows_done) |*f| f.await(w.io);
        for (activity_loops[0..core_count]) |*loop| if (loop.*) |*f| f.await(w.io);

        // Finalizing moves a core worker out of its handle, without waiting for an
        // initiate_shutdown that `stop` may be running on another thread. A worker moved while
        // that call holds one of its locks keeps the lock held forever, and finalizing then
        // hangs. Taking the mutex waits for any such call; clearing the pointers stops new ones.
        {
            w.stop_mutex.lockUncancelable(w.io);
            defer w.stop_mutex.unlock(w.io);
            w.core_ptrs = @splat(null);
        }
        var shutdown_failed: ?RunError = null;
        for (cores) |c| {
            w.client.untrackWorker(c.core.ptr);
            c.core.finalizeShutdown(w.gpa, w.io, &message) catch |err| {
                log.err("worker shutdown for task queue {s} failed: {s}", .{ c.task_queue, message orelse @errorName(err) });
                shutdown_failed = shutdown_failed orelse err;
                // The next core's failure would overwrite it.
                if (message) |m| w.gpa.free(m);
                message = null;
            };
        }
        const poll_failed = poll_failed: {
            w.stop_mutex.lockUncancelable(w.io);
            defer w.stop_mutex.unlock(w.io);
            break :poll_failed w.fatal;
        };
        if (shutdown_failed == null) log.info("Stopped Worker Namespace={s} TaskQueue={s}", .{ w.client.namespace, w.task_queue });
        if (start_failed) |err| return err;
        if (poll_failed) |err| return err;
        if (shutdown_failed) |err| return err;
    }

    /// Creates and validates a core worker; logs and returns the error on failure.
    /// The activity slots of the session queue and of the host queue, which runs session
    /// activities beside the main queue's own pool. Saturates, so "unlimited" counts can't
    /// overflow; the bridge lowers them to what the core takes.
    fn sessionSlots(max_activities: usize, max_sessions: usize) [2]usize {
        return .{ max_sessions, max_activities +| max_sessions };
    }

    fn createCore(w: *Worker, connection: bridge.Connection, options: bridge.Worker.Options, message: *?[]u8) RunError!bridge.Worker {
        const core = bridge.Worker.init(w.gpa, connection, options, message) catch |err| {
            log.err("creating the worker for task queue {s} failed: {s}", .{ options.task_queue, message.* orelse @errorName(err) });
            return err;
        };
        errdefer core.deinit();
        core.validate(w.gpa, w.io, message) catch |err| {
            log.err("worker validation for task queue {s} failed: {s}", .{ options.task_queue, message.* orelse @errorName(err) });
            return err;
        };
        return core;
    }

    /// The core's versioning strategy for `options`, or `error.InvalidOptions` (logged) for
    /// options that contradict each other or a versioned worker whose workflows lack a
    /// behavior.
    fn versioningStrategy(w: *const Worker) error{ OutOfMemory, InvalidOptions }!bridge.Worker.Options.Versioning {
        const o = w.options;
        const d = o.deployment orelse {
            if (!o.use_build_id_versioning) return .{ .none = .{ .build_id = o.build_id } };
            if (o.build_id.len == 0) {
                log.warn("worker for task queue {s}: use_build_id_versioning requires a build_id", .{w.task_queue});
                return error.InvalidOptions;
            }
            return .{ .legacy_build_id = .{ .build_id = o.build_id } };
        };
        if (o.build_id.len > 0 or o.use_build_id_versioning) {
            log.warn("worker for task queue {s}: build_id and use_build_id_versioning must be unset with deployment; set the build ID in deployment.build_id", .{w.task_queue});
            return error.InvalidOptions;
        }
        if (d.use_worker_versioning and d.default_versioning_behavior == .unspecified) {
            var missing: std.ArrayList(u8) = .empty;
            defer missing.deinit(w.gpa);
            var it = w.workflows.iterator();
            while (it.next()) |kv| if (kv.value_ptr.versioning_behavior == .unspecified) {
                if (missing.items.len > 0) try missing.appendSlice(w.gpa, ", ");
                try missing.appendSlice(w.gpa, kv.key_ptr.*);
            };
            if (missing.items.len > 0) {
                log.warn("worker for task queue {s} in deployment {s}: workflows {s} declare no versioning behavior; declare one with temporal.workflow.DefWithOptions or set deployment.default_versioning_behavior", .{ w.task_queue, d.deployment_name, missing.items });
                return error.InvalidOptions;
            }
        }
        return .{ .deployment = .{
            .deployment_name = d.deployment_name,
            .build_id = d.build_id,
            .use_worker_versioning = d.use_worker_versioning,
            .default_versioning_behavior = @backingInt(d.default_versioning_behavior),
        } };
    }

    /// Asks `run` to stop: polling ends, in-flight work finishes, then `run` returns. Running
    /// activities are canceled (reason `worker_shutdown`) once `graceful_shutdown_period`
    /// has passed. Safe to call from any thread, before or during `run`.
    pub fn stop(w: *Worker) void {
        w.stop_mutex.lockUncancelable(w.io);
        defer w.stop_mutex.unlock(w.io);
        w.stop_requested = true;
        for (w.core_ptrs) |p| if (p) |ptr| bridge.c.temporal_core_worker_initiate_shutdown(ptr);
    }

    /// The first poll failure of the last `run`, as `polling for <workflow|activity> tasks on
    /// task queue <queue> failed: <the core's message>`, or null if that run had none (or its
    /// text couldn't be allocated). Owned by the worker: valid until the next `run` starts or
    /// `deinit`. Safe to call from any thread.
    pub fn fatalMessage(w: *Worker) ?[]const u8 {
        w.stop_mutex.lockUncancelable(w.io);
        defer w.stop_mutex.unlock(w.io);
        return w.fatal_message;
    }

    const PollKind = enum { workflow, activity };

    /// Reports a failed poll from a poll loop: the core has already retried what it considers
    /// transient, so the worker logs it, records it for `run` to return, and stops.
    fn pollFailed(w: *Worker, kind: PollKind, task_queue: []const u8, err: PollError, detail: ?[]const u8) void {
        log.err("polling for {t} tasks on task queue {s} failed: {s}; stopping the worker", .{ kind, task_queue, detail orelse @errorName(err) });
        w.recordPollFailure(kind, task_queue, err, detail);
    }

    /// `pollFailed` without the log line (the test runner fails any test that logs an error):
    /// keeps the run's first failure and its text, and stops the worker.
    fn recordPollFailure(w: *Worker, kind: PollKind, task_queue: []const u8, err: PollError, detail: ?[]const u8) void {
        const text = std.fmt.allocPrint(w.gpa, "polling for {t} tasks on task queue {s} failed: {s}", .{ kind, task_queue, detail orelse @errorName(err) }) catch null;
        {
            w.stop_mutex.lockUncancelable(w.io);
            defer w.stop_mutex.unlock(w.io);
            if (w.fatal == null) {
                w.fatal = err;
                w.fatal_message = text;
            } else if (text) |t| w.gpa.free(t);
        }
        w.stop();
    }

    /// Waits until `run` (on another task) has registered the worker with its client and
    /// started polling, for example before an eager workflow start, which only reaches a worker
    /// the client already knows. Returns `error.WorkerFailedToStart` if `run` returned before
    /// getting there (its error says why).
    pub fn waitUntilRunning(w: *Worker) error{ Canceled, WorkerFailedToStart }!void {
        try w.started.wait(w.io);
        if (w.start_failed) return error.WorkerFailedToStart;
    }

    /// The sink this worker's workflow and activity loggers write to.
    fn logSink(w: *const Worker) log_mod.Sink {
        return w.options.logger orelse w.client.logger;
    }

    /// Runs until the process receives SIGINT or SIGTERM. One signal stops every worker in
    /// the process that is running this way; while any is, the previous handlers for those
    /// signals are replaced. Returns what `run` returns.
    pub fn runUntilSignal(w: *Worker) RunError!void {
        var node: SignalWatcher.Node = .{ .worker = w };
        try SignalWatcher.register(&node);
        defer SignalWatcher.unregister(&node);
        return w.run();
    }
};

// ---------------------------------------------------------------- workflows

fn workflowLoop(w: *Worker, core: bridge.Worker) void {
    var runner: WorkflowRunner = .{
        .gpa = w.gpa,
        .workflows = &w.workflows,
        .dynamic_workflow = w.dynamic_workflow,
        .task_queue = w.task_queue,
        .namespace = w.client.namespace,
        .stack_size = w.options.workflow_stack_size,
        .memory_limit = w.options.workflow_memory_limit,
        .codecs = w.client.codecs,
        .propagators = w.client.propagators,
        .interceptors = w.interceptors,
        .logger = w.logSink(),
        .log_during_replay = w.options.log_during_replay,
        .metrics = w.client.metrics(),
        .send_versioning_behavior = if (w.options.deployment) |d| d.use_worker_versioning else false,
    };
    defer runner.deinit();
    var message: ?[]u8 = null;
    defer if (message) |m| w.gpa.free(m);

    // The core allows one poll at a time, so this loop is the only poller; each activation
    // gets its own task. The core's workflow task slots bound how many run at once.
    var group: Io.Group = .init;
    while (true) {
        const polled = core.pollWorkflowActivation(w.gpa, w.io, &message) catch |err| {
            w.pollFailed(.workflow, w.task_queue, err, message);
            continue;
        } orelse break;
        group.concurrent(w.io, processActivation, .{ &runner, core, w.io, polled }) catch {
            log.warn("no thread available for a workflow activation; processing it on the polling thread", .{});
            processActivation(&runner, core, w.io, polled);
        };
    }
    group.await(w.io) catch {};
}

fn processActivation(runner: *WorkflowRunner, core: bridge.Worker, io: Io, polled: bridge.Worker.Polled) void {
    defer polled.deinit();
    runner.process(core, io, polled.bytes());
}

/// Turns workflow activations into completions for one worker (or replayer): keeps the cache
/// of running workflow instances, creates them on InitializeWorkflow, and destroys them on
/// eviction. Decodes each activation's payloads with `codecs` before a workflow sees them and
/// encodes the completion's after, so codecs never run inside workflow code.
///
/// `process` and `handle` may be called from several threads at once for different runs. A
/// run is processed by one thread at a time (the core sends one activation per run at a time;
/// a second one while the first is processed fails its task), but each activation may run on
/// a different thread. `deinit` must be called once, after every activation has finished.
pub const WorkflowRunner = struct {
    gpa: Allocator,
    workflows: *const std.StringHashMapUnmanaged(workflow.Registered),
    /// Runs workflow types with no entry in `workflows`, if set.
    dynamic_workflow: ?workflow.Registered = null,
    task_queue: []const u8,
    namespace: []const u8,
    stack_size: usize,
    /// See `Options.workflow_memory_limit`.
    memory_limit: ?usize = null,
    /// Borrowed; see `client.Options.codecs`.
    codecs: []const converter.PayloadCodec = &.{},
    /// Borrowed; see `client.Options.propagators`.
    propagators: []const propagation.ContextPropagator = &.{},
    /// Borrowed; see `Options.interceptors`.
    interceptors: []const interceptor.Worker = &.{},
    /// Receives the runner's own records (evictions, activations it couldn't answer) and the
    /// workflows' loggers.
    logger: log_mod.Sink = .std_log,
    log_during_replay: bool = false,
    /// Borrowed; see `Client.metrics`.
    metrics: metrics_mod.Handler = .noop,
    /// See `Instance.Options.send_versioning_behavior`.
    send_versioning_behavior: bool = false,
    /// Guards `runs` and each entry's `busy`. Held only to look up, insert, or remove an entry:
    /// never across workflow code, codecs, or teardown.
    mutex: Io.Mutex = .init,
    /// Keys (run IDs) are owned.
    runs: std.StringHashMapUnmanaged(CachedRun) = .empty,
    /// Called for each eviction with the core's reason and message, in place of logging it
    /// (the replayer uses it to detect nondeterminism).
    on_evict: ?*const fn (ctx: *anyopaque, reason: act_pb.RemoveFromCache.EvictionReason, message: []const u8) void = null,
    on_evict_ctx: *anyopaque = undefined,

    const CachedRun = struct {
        inst: *Instance,
        /// Set while one of the run's activations is being processed.
        busy: bool = false,
        /// Owned copies, for eviction records: `inst.info` is only set once an activation
        /// initializes the run.
        workflow_id: []const u8,
        workflow_type: []const u8,
    };

    /// What `process` sends the core for one activation.
    const Response = union(enum) {
        completion: struct {
            bytes: []const u8,
            /// Completes an eviction: a rejection gets no fallback, the run is already gone.
            eviction: bool = false,
            /// The fallback failed completion: a rejection leaves only an eviction request.
            fallback: bool = false,
        },
        /// No completion could be built for the run.
        evict: struct { run_id: []const u8, reason: []const u8 },
        /// The activation's run ID can't be read, so nothing can be sent.
        none,
    };

    /// Room for the fallback failed completion: half for building it, half for its encoding.
    const fallback_buffer_size = 1024;

    pub fn deinit(r: *WorkflowRunner) void {
        var it = r.runs.iterator();
        while (it.next()) |kv| r.destroyRun(kv.key_ptr.*, kv.value_ptr.*);
        r.runs.deinit(r.gpa);
    }

    fn destroyRun(r: *WorkflowRunner, key: []const u8, run: CachedRun) void {
        run.inst.destroy();
        r.gpa.free(run.workflow_id);
        r.gpa.free(run.workflow_type);
        r.gpa.free(key);
    }

    /// Handles one serialized activation and sends its completion to `core`. Every activation
    /// whose run ID can be read is answered: one that can't be handled gets a failed completion
    /// (the core then fails the task and evicts the run), and if even that can't be delivered
    /// the core is asked to evict the run.
    pub fn process(r: *WorkflowRunner, core: bridge.Worker, io: Io, bytes: []const u8) void {
        var arena_state: std.heap.ArenaAllocator = .init(r.gpa);
        defer arena_state.deinit();
        var fallback_buf: [fallback_buffer_size]u8 = undefined;
        var response = r.respond(bytes, arena_state.allocator(), io, &fallback_buf);
        while (true) switch (response) {
            .completion => |c| {
                var message: ?[]u8 = null;
                defer if (message) |m| r.gpa.free(m);
                core.completeWorkflowActivation(r.gpa, io, c.bytes, &message) catch |err| {
                    const reason = message orelse @errorName(err);
                    r.logger.log(.err, "the core rejected a workflow activation completion", &.{
                        .{ .key = "run_id", .value = runId(bytes) orelse "" },
                        .{ .key = "error", .value = reason },
                    });
                    if (c.eviction) return;
                    if (c.fallback) {
                        response = .{ .evict = .{ .run_id = runId(bytes).?, .reason = "the core rejected the fallback failed completion" } };
                        continue;
                    }
                    response = fallbackCompletion(bytes, @errorName(err), &fallback_buf);
                    continue;
                };
                return;
            },
            .evict => |e| {
                core.requestWorkflowEviction(e.run_id);
                r.logger.log(.err, "no completion could be delivered for a workflow activation; asked the core to evict the run", &.{
                    .{ .key = "run_id", .value = e.run_id },
                    .{ .key = "reason", .value = e.reason },
                });
                return;
            },
            .none => {
                r.logger.log(.err, "a workflow activation has no readable run ID, so no completion can be sent; its run stays stuck until the worker restarts", &.{});
                return;
            },
        };
    }

    /// Decides what to send the core for `bytes`, without sending it. Falls back to a failed
    /// completion built in `fallback_buf` when the activation can't be handled.
    fn respond(r: *WorkflowRunner, bytes: []const u8, a: Allocator, io: Io, fallback_buf: []u8) Response {
        const handled = r.handle(bytes, a, io) catch |err| {
            r.logger.log(.err, "processing a workflow activation failed", &.{
                .{ .key = "run_id", .value = runId(bytes) orelse "" },
                .{ .key = "error", .value = @errorName(err) },
            });
            return fallbackCompletion(bytes, @errorName(err), fallback_buf);
        };
        var encoded: Io.Writer.Allocating = .init(a);
        handled.completion.encode(&encoded.writer, a) catch |err| {
            r.logger.log(.err, "encoding a workflow activation completion failed", &.{
                .{ .key = "run_id", .value = runId(bytes) orelse "" },
                .{ .key = "error", .value = @errorName(err) },
            });
            return fallbackCompletion(bytes, @errorName(err), fallback_buf);
        };
        return .{ .completion = .{ .bytes = encoded.written(), .eviction = handled.eviction } };
    }

    /// A failed completion for the activation's run, built in `buf` without the heap: the
    /// likeliest reason to need it is running out of memory.
    fn fallbackCompletion(bytes: []const u8, err_name: []const u8, buf: []u8) Response {
        const run_id = runId(bytes) orelse return .none;
        const too_big: Response = .{ .evict = .{ .run_id = run_id, .reason = "the run ID is too long for the fallback failed completion" } };
        var fba: std.heap.FixedBufferAllocator = .init(buf[0 .. buf.len / 2]);
        const fa = fba.allocator();
        const message = std.fmt.allocPrint(fa, "the worker could not process this workflow activation: {s}; see the worker's log", .{err_name}) catch return too_big;
        const failure = failure_mod.application(fa, .{ .message = message }) catch return too_big;
        const completion: done_pb.WorkflowActivationCompletion = .{ .run_id = run_id, .status = .{ .failed = .{ .failure = failure } } };
        var out: Io.Writer = .fixed(buf[buf.len / 2 ..]);
        completion.encode(&out, fa) catch return too_big;
        return .{ .completion = .{ .bytes = out.buffered(), .fallback = true } };
    }

    /// Decodes `act`'s payloads with the codecs, in the context of its run: `cached` if the
    /// run is cached. A run that isn't cached yet starts with this activation: its start
    /// header is decoded first, without propagated values, so the rest is decoded with the
    /// values the propagators extract from it, the same ones the workflow will see.
    fn decodeActivation(r: *WorkflowRunner, act: *act_pb.WorkflowActivation, cached: ?*const Instance, a: Allocator, io: Io) converter.CodecError!void {
        if (r.codecs.len == 0) return;
        if (cached) |inst| return converter.decodeMessage(act_pb.WorkflowActivation, act, r.codecs, a, io, &runCodecContext(r.namespace, inst));
        const index = for (act.jobs.items, 0..) |job, i| {
            if (job.variant) |v| if (v == .initialize_workflow) break i;
        } else return converter.decodeMessage(act_pb.WorkflowActivation, act, r.codecs, a, io, &.{ .namespace = r.namespace });
        const init = &act.jobs.items[index].variant.?.initialize_workflow;
        var context: converter.CodecContext = .{ .namespace = r.namespace, .workflow_id = init.workflow_id, .workflow_type = init.workflow_type };
        var headers = init.headers;
        try converter.decodeMessage(@TypeOf(headers), &headers, r.codecs, a, io, &context);
        const values = try a.create(propagation.Values);
        values.* = .init(a);
        try propagation.extractAll(r.propagators, a, try .fromEntries(a, headers.items), values, "the workflow start");
        context.values = values;
        init.headers = .empty;
        try converter.decodeMessage(act_pb.WorkflowActivation, act, r.codecs, a, io, &context);
        // Decoding copied the jobs; the header goes back into the copy.
        act.jobs.items[index].variant.?.initialize_workflow.headers = headers;
    }

    const Handled = struct {
        completion: done_pb.WorkflowActivationCompletion,
        /// The activation evicted its run.
        eviction: bool = false,
    };

    fn handle(r: *WorkflowRunner, bytes: []const u8, a: Allocator, io: Io) !Handled {
        var reader: Io.Reader = .fixed(bytes);
        var act = try act_pb.WorkflowActivation.decode(&reader, a);

        // An eviction comes alone; complete it, then drop the run.
        for (act.jobs.items) |job| if (job.variant) |v| if (v == .remove_from_cache) {
            const removed = blk: {
                r.mutex.lockUncancelable(io);
                defer r.mutex.unlock(io);
                if (r.runs.get(act.run_id)) |run| if (run.busy) break :blk error.RunBusy;
                break :blk r.runs.fetchRemove(act.run_id);
            } catch return .{ .completion = try busyFailure(a, act.run_id) };
            const run: ?CachedRun = if (removed) |kv| kv.value else null;
            if (r.on_evict) |cb|
                cb(r.on_evict_ctx, v.remove_from_cache.reason, v.remove_from_cache.message)
            else
                r.logEviction(act.run_id, run, v.remove_from_cache);
            if (removed) |kv| r.destroyRun(kv.key, kv.value);
            return .{ .completion = .{ .run_id = act.run_id, .status = .{ .successful = .{} } }, .eviction = true };
        };

        const cached = r.acquire(act.run_id, io) catch return .{ .completion = try busyFailure(a, act.run_id) };
        const inst = cached orelse blk: {
            const init = for (act.jobs.items) |job| {
                if (job.variant) |v| if (v == .initialize_workflow) break v.initialize_workflow;
            } else return .{ .completion = try taskFailure(a, act.run_id, "activation for a workflow run that is not cached and has no InitializeWorkflow job") };
            const entry = r.workflows.get(init.workflow_type) orelse r.dynamic_workflow orelse {
                const known = try registeredNames(a, r.workflows.*);
                return .{ .completion = try taskFailure(a, act.run_id, try std.fmt.allocPrint(a, "workflow type \"{s}\" is not registered on this worker; registered: {s}", .{ init.workflow_type, known })) };
            };
            break :blk r.insert(act.run_id, init, entry, io) catch |err| switch (err) {
                error.OutOfMemory => return error.OutOfMemory,
                error.RunBusy => return .{ .completion = try busyFailure(a, act.run_id) },
            };
        };
        defer r.release(act.run_id, io);

        r.decodeActivation(&act, cached, a, io) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            // The task fails and is retried; the workflow never sees undecoded payloads.
            error.Canceled, error.CodecFailed => return .{ .completion = try taskFailure(a, act.run_id, try std.fmt.allocPrint(a, "decoding the workflow activation's payloads with the client's codecs failed: {t}", .{err})) },
        };
        var completion = inst.activate(&act, a) catch |err| return .{ .completion = try taskFailure(a, act.run_id, try std.fmt.allocPrint(a, "workflow activation failed: {t}", .{err})) };
        converter.encodeMessage(done_pb.WorkflowActivationCompletion, &completion, r.codecs, a, io, &runCodecContext(r.namespace, inst)) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            error.Canceled, error.CodecFailed => return .{ .completion = try taskFailure(a, act.run_id, try std.fmt.allocPrint(a, "encoding the workflow activation completion's payloads with the client's codecs failed: {t}", .{err})) },
        };
        return .{ .completion = completion };
    }

    /// Marks a cached run busy and returns its instance; null if it isn't cached.
    fn acquire(r: *WorkflowRunner, run_id: []const u8, io: Io) error{RunBusy}!?*Instance {
        r.mutex.lockUncancelable(io);
        defer r.mutex.unlock(io);
        const run = r.runs.getPtr(run_id) orelse return null;
        if (run.busy) return error.RunBusy;
        run.busy = true;
        return run.inst;
    }

    fn release(r: *WorkflowRunner, run_id: []const u8, io: Io) void {
        r.mutex.lockUncancelable(io);
        defer r.mutex.unlock(io);
        if (r.runs.getPtr(run_id)) |run| run.busy = false;
    }

    /// Creates an instance for a new run and caches it, busy. The instance is created outside
    /// the lock.
    fn insert(r: *WorkflowRunner, run_id: []const u8, init: act_pb.InitializeWorkflow, entry: workflow.Registered, io: Io) error{ OutOfMemory, RunBusy }!*Instance {
        const inst = try Instance.create(r.gpa, entry, .{
            .task_queue = r.task_queue,
            .namespace = r.namespace,
            .stack_size = r.stack_size,
            .memory_limit = r.memory_limit,
            .logger = r.logger,
            .log_during_replay = r.log_during_replay,
            .propagators = r.propagators,
            .interceptors = r.interceptors,
            .metrics = r.metrics,
            .send_versioning_behavior = r.send_versioning_behavior,
        });
        errdefer inst.destroy();
        const key = try r.gpa.dupe(u8, run_id);
        errdefer r.gpa.free(key);
        const workflow_id = try r.gpa.dupe(u8, init.workflow_id);
        errdefer r.gpa.free(workflow_id);
        const workflow_type = try r.gpa.dupe(u8, init.workflow_type);
        errdefer r.gpa.free(workflow_type);

        r.mutex.lockUncancelable(io);
        defer r.mutex.unlock(io);
        const gop = try r.runs.getOrPut(r.gpa, key);
        // Another activation of the same run got here first.
        if (gop.found_existing) return error.RunBusy;
        gop.value_ptr.* = .{ .inst = inst, .busy = true, .workflow_id = workflow_id, .workflow_type = workflow_type };
        return inst;
    }

    fn busyFailure(a: Allocator, run_id: []const u8) !done_pb.WorkflowActivationCompletion {
        return taskFailure(a, run_id, try std.fmt.allocPrint(a, "workflow run {s} received an activation while another was still being processed; the core must send one at a time", .{run_id}));
    }

    /// Logs an eviction at a level that fits its reason: error for what needs a code or
    /// operator fix, debug for routine cache traffic.
    fn logEviction(r: *WorkflowRunner, run_id: []const u8, run: ?CachedRun, job: act_pb.RemoveFromCache) void {
        const level: log_mod.Level, const message: []const u8 = switch (job.reason) {
            .NONDETERMINISM => .{ .err, "workflow run evicted: its code produced commands that don't match its history (nondeterminism); the workflow task fails and is retried until the code matches" },
            .FATAL => .{ .err, "workflow run evicted after a fatal error in the core" },
            .PAGINATION_OR_HISTORY_FETCH => .{ .warn, "workflow run evicted: fetching its history failed" },
            else => .{ .debug, "workflow run evicted from the cache" },
        };
        var reason_buf: [16]u8 = undefined;
        const reason = std.enums.tagName(act_pb.RemoveFromCache.EvictionReason, job.reason) orelse
            std.fmt.bufPrint(&reason_buf, "{d}", .{@backingInt(job.reason)}) catch unreachable;
        var fields: [5]log_mod.Field = .{
            .{ .key = "run_id", .value = run_id },
            .{ .key = "reason", .value = reason },
            .{ .key = "message", .value = job.message },
            undefined,
            undefined,
        };
        var n: usize = 3;
        if (run) |c| {
            fields[3] = .{ .key = "workflow_id", .value = c.workflow_id };
            fields[4] = .{ .key = "workflow_type", .value = c.workflow_type };
            n = 5;
        }
        r.logger.log(level, message, fields[0..n]);
    }
};

/// The run ID of a serialized WorkflowActivation (field 1), read without decoding or
/// allocating; null if it is missing, empty, or the bytes are malformed.
fn runId(bytes: []const u8) ?[]const u8 {
    var i: usize = 0;
    while (i < bytes.len) {
        const tag = readVarint(bytes, &i) orelse return null;
        switch (tag & 7) {
            0 => _ = readVarint(bytes, &i) orelse return null,
            1 => i = std.math.add(usize, i, 8) catch return null,
            2 => {
                const len = std.math.cast(usize, readVarint(bytes, &i) orelse return null) orelse return null;
                if (len > bytes.len - i) return null;
                if (tag >> 3 == 1) return if (len > 0) bytes[i..][0..len] else null;
                i += len;
            },
            5 => i = std.math.add(usize, i, 4) catch return null,
            else => return null,
        }
    }
    return null;
}

fn readVarint(bytes: []const u8, i: *usize) ?u64 {
    var value: u64 = 0;
    var shift: u7 = 0;
    while (shift < 64) : (shift += 7) {
        if (i.* >= bytes.len) return null;
        const b = bytes[i.*];
        i.* += 1;
        value |= @as(u64, b & 0x7f) << @intCast(shift);
        if (b & 0x80 == 0) return value;
    }
    return null;
}

/// The codec context of a cached run: its identity and the values `ctx.propagated()` returns.
fn runCodecContext(namespace: []const u8, inst: *const Instance) converter.CodecContext {
    return .{ .values = &inst.propagated, .namespace = namespace, .workflow_id = inst.info.workflow_id, .workflow_type = inst.info.workflow_type };
}

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

/// A core worker that `run` polls for activities, and its task queue.
const ActivityCore = struct { core: bridge.Worker, task_queue: []const u8 };

fn activityLoop(w: *Worker, core: bridge.Worker, task_queue: []const u8) void {
    var group: Io.Group = .init;
    var running: RunningActivities = .{};
    defer running.map.deinit(w.gpa);
    var message: ?[]u8 = null;
    defer if (message) |m| w.gpa.free(m);
    while (true) {
        const polled = core.pollActivityTask(w.gpa, w.io, &message) catch |err| {
            w.pollFailed(.activity, task_queue, err, message);
            continue;
        } orelse break;
        const run = receiveActivityTask(w, &running, polled, task_queue) orelse continue;
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
    /// The task queue of the core that polled the activity; outlives the run.
    task_queue: []const u8,
    task_token: []const u8,
    start: task_pb.Start,
    ctx: activity.Context,
    /// What the codecs see for this activity's payloads: its identity, and once its header is
    /// decoded, `ctx.propagated_`.
    codec_context: converter.CodecContext = .{},
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
fn receiveActivityTask(w: *Worker, running: *RunningActivities, polled: bridge.Worker.Polled, task_queue: []const u8) ?*RunningActivity {
    const run = w.gpa.create(RunningActivity) catch {
        log.err("out of memory receiving an activity task; the core will time it out", .{});
        polled.deinit();
        return null;
    };
    run.* = .{
        .arena = .init(w.gpa),
        .polled = polled,
        .task_queue = task_queue,
        .task_token = &.{},
        .start = .{},
        .ctx = .{ .arena = undefined, .io_ = w.io, .info_ = undefined, .logger_ = .{}, .client_ = w.client },
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
    var result = outcome catch return log.err("out of memory reporting activity {s}; the core will time it out", .{run.start.activity_id});
    converter.encodeMessage(result_pb.ActivityExecutionResult, &result, w.client.codecs, a, w.io, &run.codec_context) catch |err| {
        if (err == error.OutOfMemory) return log.err("out of memory reporting activity {s}; the core will time it out", .{run.start.activity_id});
        // Report a failure without the payloads that couldn't be encoded.
        result = .{ .status = .{ .failed = .{ .failure = failure_mod.application(a, .{
            .message = std.fmt.allocPrint(a, "encoding the result of activity {s} with the client's codecs failed: {t}", .{ run.start.activity_id, err }) catch return,
            .type = "PayloadCodecFailed",
        }) catch return } } };
    };
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
    codec_context: *const converter.CodecContext,

    fn record(context: *anyopaque, details: []const proto.temporal.api.common.v1.Payload) void {
        const h: *Heartbeats = @ptrCast(@alignCast(context));
        var scratch: std.heap.ArenaAllocator = .init(h.w.gpa);
        defer scratch.deinit();
        const a = scratch.allocator();
        var heartbeat: proto.coresdk.self.ActivityHeartbeat = .{ .task_token = h.task_token };
        heartbeat.details.appendSlice(a, details) catch return;
        converter.encodeMessage(proto.coresdk.self.ActivityHeartbeat, &heartbeat, h.w.client.codecs, a, h.w.io, h.codec_context) catch |err| {
            log.warn("recording an activity heartbeat failed: encoding its details with the client's codecs failed: {t}", .{err});
            return;
        };
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
    const execution = start.workflow_execution orelse proto.temporal.api.common.v1.WorkflowExecution{};
    run.codec_context = .{
        .namespace = w.client.namespace,
        .workflow_id = execution.workflow_id,
        .workflow_type = start.workflow_type,
        .activity_type = start.activity_type,
        .activity_id = start.activity_id,
    };
    const ctx = &run.ctx;
    ctx.propagated_ = .init(a);
    decodeActivityStart(w, run) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        error.Canceled, error.CodecFailed => return .{ .status = .{ .failed = .{ .failure = try failure_mod.application(a, .{
            .message = try std.fmt.allocPrint(a, "decoding the input of activity {s} with the client's codecs failed: {t}", .{ start.activity_id, err }),
            .type = "PayloadCodecFailed",
        }) } } },
    };
    const side_effect = start.is_local and std.mem.eql(u8, start.activity_type, Instance.side_effect_activity_type);
    const session_entry = if (start.is_local) null else if (w.session_host) |h| h.registered(start.activity_type) else null;
    const internal = side_effect or session_entry != null;
    const entry = if (side_effect) side_effect_activity else session_entry orelse w.activities.get(start.activity_type) orelse w.dynamic_activity orelse {
        const known = try registeredNames(a, w.activities);
        return .{ .status = .{ .failed = .{ .failure = try failure_mod.application(a, .{
            .message = try std.fmt.allocPrint(a, "activity type \"{s}\" is not registered on this worker; registered: {s}", .{ start.activity_type, known }),
            .type = "ActivityNotRegistered",
        }) } } };
    };
    // The context already exists: a cancel may have set its reason.
    ctx.arena = a;
    ctx.info_ = activityInfo(start, run.task_queue, run.task_token);
    ctx.info_.session_id = if (ctx.header_.value([]const u8, a, Instance.session_id_header)) |id| id orelse "" else |err| blk: {
        log.warn("activity {s}: its session ID header doesn't decode: {t}", .{ start.activity_id, err });
        break :blk "";
    };
    ctx.logger_ = try (log_mod.Logger{ .sink = w.logSink() }).with(a, .{
        .ActivityID = start.activity_id,
        .ActivityType = start.activity_type,
        .Attempt = start.attempt,
        .WorkflowType = start.workflow_type,
        .WorkflowID = execution.workflow_id,
        .RunID = execution.run_id,
    });
    ctx.metrics_ = try w.client.metrics().with(.{
        .namespace = w.client.namespace,
        .task_queue = run.task_queue,
        .activity_type = start.activity_type,
        .workflow_type = start.workflow_type,
    });
    defer ctx.metrics_.deinit();
    var heartbeats: Heartbeats = .{ .w = w, .core = core, .task_token = run.task_token, .codec_context = &run.codec_context };
    ctx.heartbeat_details = start.heartbeat_details.items;
    // The core keeps no heartbeat state for local activities and warns on each heartbeat.
    if (!start.is_local) ctx.heartbeat_sink = .{ .gpa = w.gpa, .context = &heartbeats, .record = Heartbeats.record };
    var inbound: std.ArrayList(interceptor.ActivityInbound) = .empty;
    var outbound: std.ArrayList(interceptor.ActivityOutbound) = .empty;
    // Side effects and sessions belong to the SDK: interceptors see their activities in the
    // workflow.
    if (!internal) for (w.interceptors) |wi| {
        const attempt = try wi.interceptActivity(ctx);
        if (attempt.inbound) |i| try inbound.append(a, i);
        if (attempt.outbound) |o| try outbound.append(a, o);
    };
    ctx.outbound = outbound.items;
    var call: ActivityCall = .{ .entry = entry, .ctx = ctx };
    const next: interceptor.ActivityInbound.Next = .{ .chain = inbound.items, .root = .{ .ptr = &call, .vtable = &ActivityCall.vtable } };
    const payload = next.execute(.{ .activity_type = start.activity_type, .args = .{ .payloads = start.input.items }, .header = ctx.header_ }) catch |err| {
        if (err == error.CompleteAsync) {
            if (!start.is_local) return .{ .status = .{ .will_complete_async = .{} } };
            return .{ .status = .{ .failed = .{ .failure = try failure_mod.application(a, .{
                .message = try std.fmt.allocPrint(a, "local activity \"{s}\" returned completeAsync, but only activities scheduled with executeActivity can complete asynchronously", .{start.activity_type}),
                .type = "LocalActivityCompleteAsync",
                .non_retryable = true,
            }) } } };
        }
        if (err == error.Canceled) if (ctx.cancelReason()) |reason| return .{ .status = .{ .cancelled = .{ .failure = .{
            .message = try std.fmt.allocPrint(a, "activity canceled: {t}", .{reason}),
            .source = failure_mod.source,
            .failure_info = .{ .canceled_failure_info = .{} },
        } } } };
        const f = if (ctx.pending_failure) |p| try failure_mod.application(a, p) else try failure_mod.fromError(a, err);
        if (failure_mod.applicationCategory(f) == .benign)
            ctx.logger_.debug("Activity failed", .{ .Error = f.message })
        else
            ctx.logger_.warn("Activity failed", .{ .Error = f.message });
        return .{ .status = .{ .failed = .{ .failure = f } } };
    };
    return .{ .status = .{ .completed = .{ .result = payload } } };
}

/// An activity's info from its Start task; borrows `start`'s memory.
fn activityInfo(start: *const task_pb.Start, task_queue: []const u8, task_token: []const u8) activity.Info {
    const execution = start.workflow_execution orelse proto.temporal.api.common.v1.WorkflowExecution{};
    return .{
        .activity_id = start.activity_id,
        .activity_type = start.activity_type,
        .task_queue = task_queue,
        .workflow_id = execution.workflow_id,
        .workflow_run_id = execution.run_id,
        .workflow_type = start.workflow_type,
        .workflow_namespace = start.workflow_namespace,
        .attempt = start.attempt,
        .is_local = start.is_local,
        .task_token = task_token,
        .scheduled_time = zigTimestamp(start.scheduled_time),
        .current_attempt_scheduled_time = zigTimestamp(start.current_attempt_scheduled_time),
        .started_time = zigTimestamp(start.started_time),
    };
}

fn zigTimestamp(t: ?proto.google.protobuf.Timestamp) ?Io.Timestamp {
    const v = t orelse return null;
    return .fromNanoseconds(@as(i96, v.seconds) * std.time.ns_per_s + v.nanos);
}

/// Decodes the activity's header, extracts its propagated values into `run.ctx`, and decodes
/// the rest of its Start task with them in `run.codec_context`.
fn decodeActivityStart(w: *Worker, run: *RunningActivity) converter.CodecError!void {
    const a = run.arena.allocator();
    const start = &run.start;
    const codecs = w.client.codecs;
    try converter.decodeMessage(@TypeOf(start.header_fields), &start.header_fields, codecs, a, w.io, &run.codec_context);
    run.ctx.header_ = try .fromEntries(a, start.header_fields.items);
    try propagation.extractAll(w.client.propagators, a, run.ctx.header_, &run.ctx.propagated_, "an activity task");
    run.codec_context.values = &run.ctx.propagated_;
    const header_fields = start.header_fields;
    start.header_fields = .empty;
    defer start.header_fields = header_fields;
    try converter.decodeMessage(task_pb.Start, start, codecs, a, w.io, &run.codec_context);
}

/// The end of an activity's inbound interceptor chain: the registered activity itself.
const ActivityCall = struct {
    entry: activity.Registered,
    ctx: *activity.Context,

    const vtable: interceptor.ActivityInbound.VTable = .{ .execute = execute };

    fn execute(ptr: *anyopaque, input: interceptor.ExecuteActivityInput, _: interceptor.ActivityInbound.Next) anyerror!converter.Payload {
        const call: *ActivityCall = @ptrCast(@alignCast(ptr));
        return call.entry.invoke(call.entry.receiver, call.ctx, input.args.payloads);
    }
};

/// Every worker's implementation of `Instance.side_effect_activity_type`: returns the value the
/// workflow computed, so the core records it in history. A replayed side effect is scheduled
/// with no input; it reaches here only if history has no recorded value.
const side_effect_activity: activity.Registered = .{
    .name = Instance.side_effect_activity_type,
    .invoke = struct {
        fn echo(_: ?*anyopaque, ctx: *activity.Context, payloads: []const converter.Payload) anyerror!converter.Payload {
            if (payloads.len == 1) return payloads[0];
            return ctx.fail(.{
                .message = "the side effect has no recorded value: the workflow replayed it, but history holds no result for it",
                .type = "SideEffectNotRecorded",
                .non_retryable = true,
            });
        }
    }.echo,
};

// ---------------------------------------------------------------- sessions

/// The sessions a worker holds open (`Options.sessions`), for one `run`. Its two activities
/// run on the worker's activity threads; `mutex` guards `open`.
const SessionHost = struct {
    gpa: Allocator,
    io: Io,
    max_sessions: usize,
    /// `<task_queue>__temporal_sessions`, shared by the task queue's session workers.
    creation_queue: []u8,
    /// `<task_queue>@<host name>@<UUID>`, this worker's own.
    host_queue: []u8,
    host_name: []u8,
    mutex: Io.Mutex = .init,
    /// The open sessions by ID (keys owned), each with the event its creation activity waits
    /// on until the session completes.
    open: std.StringHashMapUnmanaged(*Io.Event) = .empty,

    const Create = activity.Def(Instance.session_create_activity_type, create);
    const Complete = activity.Def(Instance.session_complete_activity_type, complete);
    const Opened = workflow.SignalDef(Instance.session_opened_signal, fn (Instance.SessionOpened) void);

    fn init(w: *Worker, max_sessions: usize) error{OutOfMemory}!SessionHost {
        var name_buf: [std.posix.HOST_NAME_MAX]u8 = undefined;
        const host_name = try w.gpa.dupe(u8, std.posix.gethostname(&name_buf) catch "unknown-host");
        errdefer w.gpa.free(host_name);
        const creation_queue = try std.fmt.allocPrint(w.gpa, "{s}" ++ Instance.session_queue_suffix, .{w.task_queue});
        errdefer w.gpa.free(creation_queue);
        var scratch: std.heap.ArenaAllocator = .init(w.gpa);
        defer scratch.deinit();
        const host_queue = try std.fmt.allocPrint(w.gpa, "{s}@{s}@{s}", .{ w.task_queue, host_name, try client_mod.uuid(scratch.allocator(), w.io) });
        return .{ .gpa = w.gpa, .io = w.io, .max_sessions = max_sessions, .creation_queue = creation_queue, .host_queue = host_queue, .host_name = host_name };
    }

    fn deinit(h: *SessionHost) void {
        var it = h.open.keyIterator();
        while (it.next()) |k| h.gpa.free(k.*);
        h.open.deinit(h.gpa);
        h.gpa.free(h.creation_queue);
        h.gpa.free(h.host_queue);
        h.gpa.free(h.host_name);
    }

    /// The session activity named `activity_type`, bound to this host, if it is one.
    fn registered(h: *SessionHost, activity_type: []const u8) ?activity.Registered {
        if (std.mem.eql(u8, activity_type, Create.activity_name)) return Create.with(h);
        if (std.mem.eql(u8, activity_type, Complete.activity_name)) return Complete.with(h);
        return null;
    }

    /// Holds a slot for the session until it completes or is canceled: tells the workflow the
    /// host's queue, then heartbeats until then.
    fn create(h: *SessionHost, ctx: *activity.Context, input: Instance.SessionCreateInput) !void {
        var done: Io.Event = .unset;
        try h.take(ctx, input.session_id, &done);
        defer h.release(input.session_id);
        const info = ctx.info();
        const client = try ctx.client();
        client.signalWorkflow(Opened, info.workflow_id, info.workflow_run_id, .{.{
            .session_id = input.session_id,
            .task_queue = h.host_queue,
            .host_name = h.host_name,
        }}) catch |err| {
            if (err == error.Canceled) return h.canceled(ctx);
            return ctx.fail(.{
                .message = try std.fmt.allocPrint(ctx.allocator(), "session {s}: telling workflow {s} that the session opened on task queue {s} failed: {t}", .{ input.session_id, info.workflow_id, h.host_queue, err }),
                .type = "SessionSignalFailed",
                .non_retryable = true,
            });
        };
        const interval: Io.Duration = .fromMilliseconds(@max(1, input.heartbeat_interval_ms));
        while (true) {
            ctx.heartbeat({});
            done.waitTimeout(ctx.io(), .{ .duration = .{ .raw = interval, .clock = .awake } }) catch |err| switch (err) {
                error.Timeout => continue,
                error.Canceled => return h.canceled(ctx),
            };
            return;
        }
    }

    /// The creation activity's outcome on a cancel: the worker stopping fails the session for
    /// good; any other cancel (the workflow completing it, a timeout) is reported as one.
    fn canceled(h: *SessionHost, ctx: *activity.Context) anyerror {
        if (ctx.cancelReason() != .worker_shutdown) return error.Canceled;
        return ctx.fail(.{
            .message = try std.fmt.allocPrint(ctx.allocator(), "the session's worker stopped (host task queue {s})", .{h.host_queue}),
            .type = "SessionWorkerStopped",
            .non_retryable = true,
        });
    }

    /// Frees the session's slot and wakes its creation activity.
    fn complete(h: *SessionHost, ctx: *activity.Context, session_id: []const u8) !void {
        _ = ctx;
        h.mutex.lockUncancelable(h.io);
        defer h.mutex.unlock(h.io);
        const kv = h.open.fetchRemove(session_id) orelse return;
        kv.value.set(h.io);
        h.gpa.free(kv.key);
    }

    fn take(h: *SessionHost, ctx: *activity.Context, session_id: []const u8, done: *Io.Event) !void {
        h.mutex.lockUncancelable(h.io);
        defer h.mutex.unlock(h.io);
        if (h.open.count() >= h.max_sessions) return ctx.fail(.{
            .message = try std.fmt.allocPrint(ctx.allocator(), "session {s}: the worker on host task queue {s} already holds its maximum of {d} sessions (worker.SessionWorkerOptions.max_concurrent_sessions)", .{ session_id, h.host_queue, h.max_sessions }),
            .type = "SessionWorkerFull",
            .non_retryable = true,
        });
        const gop = try h.open.getOrPut(h.gpa, session_id);
        if (gop.found_existing) return ctx.fail(.{
            .message = try std.fmt.allocPrint(ctx.allocator(), "session {s} is already open on host task queue {s}", .{ session_id, h.host_queue }),
            .type = "SessionAlreadyOpen",
            .non_retryable = true,
        });
        gop.key_ptr.* = h.gpa.dupe(u8, session_id) catch |err| {
            h.open.removeByPtr(gop.key_ptr);
            return err;
        };
        gop.value_ptr.* = done;
    }

    fn release(h: *SessionHost, session_id: []const u8) void {
        h.mutex.lockUncancelable(h.io);
        defer h.mutex.unlock(h.io);
        if (h.open.fetchRemove(session_id)) |kv| h.gpa.free(kv.key);
    }
};

// ---------------------------------------------------------------- signals

/// Turns SIGINT and SIGTERM into `Worker.stop`. A signal handler may only do
/// async-signal-safe work, so it writes a byte to a pipe; a thread reading the pipe calls
/// `stop`.
/// Stops every worker running in `runUntilSignal` when the process gets SIGINT or SIGTERM.
/// Process-wide: the first worker to register installs the signal handlers and starts a
/// thread that waits on a pipe the handler writes to; the last one to leave restores the
/// previous handlers and ends the thread. Workers register through a node on their
/// `runUntilSignal` stack frame.
const SignalWatcher = struct {
    const signals = [_]std.posix.SIG{ .INT, .TERM };

    const Node = struct {
        worker: *Worker,
        node: std.DoublyLinkedList.Node = .{},
    };

    /// Guards everything below except `signal_fd`, which the signal handler reads.
    var mutex: Io.Mutex = .init;
    var io: Io = undefined;
    var workers: std.DoublyLinkedList = .{};
    var thread: ?std.Thread = null;
    var pipe_fds: [2]std.c.fd_t = .{ -1, -1 };
    var previous: [2]std.posix.Sigaction = undefined;
    var signal_fd: std.atomic.Value(std.c.fd_t) = .init(-1);

    fn register(n: *Node) error{OutOfMemory}!void {
        const w = n.worker;
        mutex.lockUncancelable(w.io);
        defer mutex.unlock(w.io);
        if (thread == null) {
            var fds: [2]std.c.fd_t = undefined;
            if (std.c.pipe(&fds) != 0) return error.OutOfMemory;
            const t = std.Thread.spawn(.{}, watch, .{fds[0]}) catch {
                _ = std.c.close(fds[0]);
                _ = std.c.close(fds[1]);
                return error.OutOfMemory;
            };
            io = w.io;
            pipe_fds = fds;
            thread = t;
            signal_fd.store(fds[1], .release);
            const action: std.posix.Sigaction = .{
                .handler = .{ .handler = onSignal },
                .mask = std.posix.sigemptyset(),
                .flags = 0,
            };
            for (signals, &previous) |sig, *prev| std.posix.sigaction(sig, &action, prev);
        }
        workers.append(&n.node);
    }

    fn unregister(n: *Node) void {
        const w = n.worker;
        mutex.lockUncancelable(w.io);
        workers.remove(&n.node);
        if (workers.first != null) {
            mutex.unlock(w.io);
            return;
        }
        // The last worker out: hand the thread and pipe to this call, so a worker that
        // registers meanwhile starts fresh ones.
        for (signals, &previous) |sig, *prev| std.posix.sigaction(sig, prev, null);
        signal_fd.store(-1, .release);
        const t = thread.?;
        const fds = pipe_fds;
        thread = null;
        pipe_fds = .{ -1, -1 };
        mutex.unlock(w.io);

        _ = std.c.write(fds[1], "q", 1);
        t.join();
        _ = std.c.close(fds[0]);
        _ = std.c.close(fds[1]);
    }

    fn onSignal(_: std.posix.SIG) callconv(.c) void {
        const fd = signal_fd.load(.acquire);
        if (fd >= 0) _ = std.c.write(fd, "s", 1);
    }

    fn watch(read_fd: std.c.fd_t) void {
        var byte: [1]u8 = undefined;
        while (true) {
            const n = std.c.read(read_fd, &byte, 1);
            if (n <= 0 or byte[0] == 'q') return;
            log.info("Worker received a stop signal, shutting down", .{});
            mutex.lockUncancelable(io);
            defer mutex.unlock(io);
            var it = workers.first;
            while (it) |node| : (it = node.next) @as(*Node, @fieldParentPtr("node", node)).worker.stop();
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

    fn runWorkerUntilSignal(w: *Worker) Worker.RunError!void {
        return w.runUntilSignal();
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

test "registering an activity type with the SDK's reserved prefix is rejected" {
    var client: client_mod.Client = undefined; // registration never touches the client
    var w = try Worker.init(testing.allocator, testing.io, &client, "reserved", .{});
    defer w.deinit();
    const Impostor = activity.Def(Instance.side_effect_activity_type, HelloTest.greet);
    try testing.expectError(error.ReservedActivityName, w.registerActivity(Impostor));
    try testing.expectError(error.ReservedActivityName, w.registerActivityImpl(activity.Def("__temporal_mine", HelloTest.greet).registered));
    try testing.expectEqual(0, w.activities.count());
    try w.registerActivity(HelloTest.Greet);
}

const Counter = struct {
    step: u32,

    pub const unit = "count";

    pub fn next(c: *Counter, ctx: *activity.Context, n: u32) !u32 {
        _ = ctx;
        return n + c.step;
    }

    pub fn reset(c: *Counter, ctx: *activity.Context) !u32 {
        _ = ctx;
        return c.step;
    }

    pub fn describe(c: *const Counter) u32 {
        return c.step;
    }

    pub fn peek(c: *Counter, ctx: *activity.Context) u32 {
        _ = ctx;
        return c.step;
    }

    pub const advance = next;
};

fn invokeRegistered(w: *Worker, arena: Allocator, name: []const u8, args: anytype) ![]const u8 {
    const reg = w.activities.get(name) orelse return error.NotRegistered;
    var ctx: activity.Context = .{ .arena = arena, .io_ = testing.io, .info_ = undefined, .logger_ = .{} };
    const payloads = try converter.toPayloads(arena, args);
    return (try reg.invoke(reg.receiver, &ctx, payloads.items)).data;
}

test "registerActivities registers each activity method under the prefix, bound to the instance" {
    var client: client_mod.Client = undefined; // registration never touches the client
    var w = try Worker.init(testing.allocator, testing.io, &client, "struct", .{});
    defer w.deinit();
    var counter: Counter = .{ .step = 3 };
    try w.registerActivities(&counter, activity.defs(Counter, .{}));
    try w.registerActivities(&counter, activity.defs(Counter, .{ .prefix = "Counter." }));
    try testing.expectEqual(6, w.activities.count());
    for ([_][]const u8{ "next", "reset", "advance", "Counter.next", "Counter.reset", "Counter.advance" }) |name|
        try testing.expect(w.activities.contains(name));

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    try testing.expectEqualStrings("10", try invokeRegistered(&w, arena.allocator(), "Counter.next", .{@as(u32, 7)}));
    counter.step = 4;
    try testing.expectEqualStrings("4", try invokeRegistered(&w, arena.allocator(), "reset", .{}));
    try testing.expectEqualStrings("9", try invokeRegistered(&w, arena.allocator(), "advance", .{@as(u32, 5)}));
}

test "registerActivities leaves a struct's dynamic method to registerDynamicActivityWith" {
    const Router = struct {
        pub fn ping(r: *@This(), ctx: *activity.Context) ![]const u8 {
            _ = r;
            _ = ctx;
            return "pong";
        }

        pub fn fallback(r: *@This(), ctx: *activity.Context, args: converter.EncodedValues) ![]const u8 {
            _ = r;
            return std.fmt.allocPrint(ctx.allocator(), "{d} arguments", .{args.len()});
        }
    };
    var client: client_mod.Client = undefined; // registration never touches the client
    var w = try Worker.init(testing.allocator, testing.io, &client, "struct", .{});
    defer w.deinit();
    var router: Router = .{};
    try w.registerActivities(&router, activity.defs(Router, .{ .prefix = "Router." }));
    try w.registerDynamicActivityWith(activity.DynamicDef(Router.fallback), &router);
    try testing.expectEqual(1, w.activities.count());
    try testing.expect(!w.activities.contains("Router.fallback"));

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    try testing.expectEqualStrings("\"pong\"", try invokeRegistered(&w, arena.allocator(), "Router.ping", .{}));
    var ctx: activity.Context = .{ .arena = arena.allocator(), .io_ = testing.io, .info_ = undefined, .logger_ = .{} };
    const dynamic = w.dynamic_activity.?;
    const payloads = try converter.toPayloads(arena.allocator(), .{ @as(u32, 1), @as(u32, 2) });
    try testing.expectEqualStrings("\"2 arguments\"", (try dynamic.invoke(dynamic.receiver, &ctx, payloads.items)).data);
}

test "registerActivities rejects a prefix already registered or reserved for the SDK" {
    var client: client_mod.Client = undefined; // registration never touches the client
    var w = try Worker.init(testing.allocator, testing.io, &client, "struct", .{});
    defer w.deinit();
    var counter: Counter = .{ .step = 1 };
    const acts = activity.defs(Counter, .{ .prefix = "Counter." });
    try w.registerActivities(&counter, acts);
    try testing.expectError(error.AlreadyRegistered, w.registerActivities(&counter, acts));
    try testing.expectError(error.ReservedActivityName, w.registerActivities(&counter, activity.defs(Counter, .{ .prefix = "__temporal_" })));
    try testing.expectEqual(3, w.activities.count());
}

test "registerActivities keeps the methods registered before a failing one" {
    var client: client_mod.Client = undefined; // registration never touches the client
    var w = try Worker.init(testing.allocator, testing.io, &client, "struct", .{});
    defer w.deinit();
    var counter: Counter = .{ .step = 1 };
    const acts = activity.defs(Counter, .{ .prefix = "Counter." });
    try w.registerActivity(activity.Def("Counter.reset", HelloTest.greet));
    // Methods register in declaration order: next, reset, advance.
    try testing.expectError(error.AlreadyRegistered, w.registerActivities(&counter, acts));
    try testing.expectEqual(2, w.activities.count());
    try testing.expect(w.activities.contains("Counter.next"));
    try testing.expect(!w.activities.contains("Counter.advance"));
}

test "one function registered under two names runs under each" {
    var client: client_mod.Client = undefined; // registration never touches the client
    var w = try Worker.init(testing.allocator, testing.io, &client, "aliases", .{});
    defer w.deinit();
    const Current = activity.Def("Greet", HelloTest.greet);
    const Legacy = activity.Def("LegacyGreet", HelloTest.greet);
    try w.registerActivity(Current);
    try w.registerActivity(Legacy);
    try testing.expectError(error.AlreadyRegistered, w.registerActivity(activity.Def("Greet", HelloTest.greet)));

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const name: []const u8 = "Temporal";
    try testing.expectEqualStrings("\"Hello Temporal!\"", try invokeRegistered(&w, arena.allocator(), "Greet", .{name}));
    try testing.expectEqualStrings("\"Hello Temporal!\"", try invokeRegistered(&w, arena.allocator(), "LegacyGreet", .{name}));
}

test "a session worker with no session slots is rejected before it starts" {
    var client: client_mod.Client = undefined; // rejected before the client is used
    var w = try Worker.init(testing.allocator, testing.io, &client, "sessions", .{ .sessions = .{ .max_concurrent_sessions = 0 } });
    defer w.deinit();
    try w.registerActivity(HelloTest.Greet);
    try testing.expectError(error.InvalidOptions, w.run());
    try testing.expectError(error.WorkerFailedToStart, w.waitUntilRunning());
}

test "session slot counts saturate instead of overflowing" {
    const max = std.math.maxInt(usize);
    try testing.expectEqual([2]usize{ 3, 103 }, Worker.sessionSlots(100, 3));
    try testing.expectEqual([2]usize{ max, max }, Worker.sessionSlots(100, max));
    try testing.expectEqual([2]usize{ 1, max }, Worker.sessionSlots(max, 1));
}

test "versioning options map to the core's strategy, and contradictions are rejected" {
    var client: client_mod.Client = undefined; // the strategy never touches the client
    const Pinned = workflow.DefWithOptions("Pinned", HelloTest.hello, .{ .versioning_behavior = .pinned });
    const cases = .{
        .{ Options{}, "none", "" },
        .{ Options{ .build_id = "b1" }, "none", "b1" },
        .{ Options{ .build_id = "b1", .use_build_id_versioning = true }, "legacy_build_id", "b1" },
        .{ Options{ .deployment = .{ .deployment_name = "shop", .build_id = "b2" } }, "deployment", "b2" },
        .{ Options{ .deployment = .{ .deployment_name = "shop", .build_id = "b2", .use_worker_versioning = false } }, "deployment", "b2" },
    };
    inline for (cases) |case| {
        var w = try Worker.init(testing.allocator, testing.io, &client, "versioned", case[0]);
        defer w.deinit();
        try w.registerWorkflow(Pinned);
        const strategy = try w.versioningStrategy();
        try testing.expectEqualStrings(case[1], @tagName(strategy));
        switch (strategy) {
            inline else => |v| try testing.expectEqualStrings(case[2], v.build_id),
        }
    }

    const invalid = [_]Options{
        .{ .use_build_id_versioning = true },
        .{ .build_id = "b1", .deployment = .{ .deployment_name = "shop", .build_id = "b2" } },
        // HelloTest.Hello declares no behavior and the deployment has no default.
        .{ .deployment = .{ .deployment_name = "shop", .build_id = "b2" } },
    };
    for (invalid) |options| {
        var w = try Worker.init(testing.allocator, testing.io, &client, "versioned", options);
        defer w.deinit();
        try w.registerWorkflow(HelloTest.Hello);
        try testing.expectError(error.InvalidOptions, w.versioningStrategy());
    }

    // A default behavior covers workflows that declare none.
    var w = try Worker.init(testing.allocator, testing.io, &client, "versioned", .{ .deployment = .{ .deployment_name = "shop", .build_id = "b2", .default_versioning_behavior = .auto_upgrade } });
    defer w.deinit();
    try w.registerWorkflow(HelloTest.Hello);
    try testing.expectEqual(@as(i32, 2), (try w.versioningStrategy()).deployment.default_versioning_behavior);
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

/// An Io whose `concurrent` succeeds `allowed` times and then fails, standing in for a
/// machine that runs out of threads between the worker's two poll loops.
const ScarceConcurrency = struct {
    var inner: Io = undefined;
    var allowed: std.atomic.Value(usize) = .init(0);
    var vtable: Io.VTable = undefined;

    fn io(base: Io, n: usize) Io {
        inner = base;
        allowed.store(n, .release);
        vtable = base.vtable.*;
        vtable.concurrent = concurrent;
        return .{ .userdata = base.userdata, .vtable = &vtable };
    }

    fn concurrent(
        userdata: ?*anyopaque,
        result_len: usize,
        result_alignment: std.mem.Alignment,
        context: []const u8,
        context_alignment: std.mem.Alignment,
        start: *const fn (context: *const anyopaque, result: *anyopaque) void,
    ) Io.ConcurrentError!*Io.AnyFuture {
        var n = allowed.load(.acquire);
        while (true) {
            if (n == 0) return error.ConcurrencyUnavailable;
            n = allowed.cmpxchgWeak(n, n - 1, .acq_rel, .acquire) orelse break;
        }
        return inner.vtable.concurrent(userdata, result_len, result_alignment, context, context_alignment, start);
    }
};

// The workflow loop is already polling the core worker when the activity loop can't start, so
// `run` must drain it before freeing the core worker.
test "run shuts down the started workflow loop when the activity loop can't start" {
    const rt = try bridge.Runtime.init(testing.allocator, .{}, null);
    defer rt.deinit();
    var server = try bridge.EphemeralServer.startDevServer(testing.allocator, testing.io, rt, .{}, null);
    defer server.shutdown(testing.io, null) catch {};
    var client = try client_mod.Client.connect(testing.allocator, testing.io, .{ .host_port = server.target });
    defer client.close();

    var w = try Worker.init(testing.allocator, ScarceConcurrency.io(testing.io, 1), &client, "scarce-concurrency", .{});
    defer w.deinit();
    try w.registerWorkflow(HelloTest.Hello);
    try w.registerActivity(HelloTest.Greet);
    try testing.expectError(error.ConcurrencyUnavailable, w.run());
    try testing.expectEqual(0, ScarceConcurrency.allowed.load(.acquire));
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

    var release: Io.Event = .unset;

    /// Runs until `release` is set, then returns a value; records a cancel in `seen_reason`.
    fn finish(ctx: *activity.Context) ![]const u8 {
        started.set(ctx.io());
        release.wait(ctx.io()) catch |err| {
            seen_reason = ctx.cancelReason();
            return err;
        };
        return "finished";
    }
    const Finish = activity.Def("Finish", finish);

    fn runFinish(ctx: *workflow.Context) ![]const u8 {
        return ctx.executeActivity(Finish, .{ .start_to_close_timeout = .fromSeconds(60) }, .{}).get();
    }
    const RunFinish = workflow.Def("RunFinish", runFinish);

    fn reset() void {
        started = .unset;
        observed = .unset;
        release = .unset;
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
        try env.registerWorkflow(RunFinish);
        try env.registerActivity(Finish);
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

test "a graceful shutdown period lets a running activity finish" {
    const env = try CancelTest.start();
    defer env.deinit();
    const period: Io.Duration = .fromSeconds(30);
    env.worker.options.graceful_shutdown_period = period;

    var run = try env.startWorkflow(CancelTest.RunFinish, .{ .task_queue = "" }, .{});
    defer run.deinit();
    try CancelTest.started.wait(testing.io);
    const stopped_at = Io.Timestamp.now(testing.io, .awake);
    env.worker.stop();
    CancelTest.release.set(testing.io);
    try env.running.?.await(testing.io);
    env.running = null;
    const elapsed = stopped_at.durationTo(Io.Timestamp.now(testing.io, .awake));

    try testing.expectEqual(@as(?activity.CancelReason, null), CancelTest.seen_reason);
    try testing.expectEqual(1, try CancelTest.countEvents(env, &run, .EVENT_TYPE_ACTIVITY_TASK_COMPLETED));
    try testing.expect(elapsed.nanoseconds < period.nanoseconds);
}

test "waitUntilRunning returns once run polls, or fails if run returned first" {
    const rt = try bridge.Runtime.init(testing.allocator, .{}, null);
    defer rt.deinit();
    var server = try bridge.EphemeralServer.startDevServer(testing.allocator, testing.io, rt, .{}, null);
    defer server.shutdown(testing.io, null) catch {};
    var client = try client_mod.Client.connect(testing.allocator, testing.io, .{ .host_port = server.target });
    defer client.close();

    var empty = try Worker.init(testing.allocator, testing.io, &client, "wait-empty", .{});
    defer empty.deinit();
    try testing.expectError(error.NothingRegistered, empty.run());
    try testing.expectError(error.WorkerFailedToStart, empty.waitUntilRunning());

    var w = try Worker.init(testing.allocator, testing.io, &client, "wait-running", .{});
    defer w.deinit();
    try w.registerActivity(HelloTest.Greet);
    var running = try testing.io.concurrent(HelloTest.runWorker, .{&w});
    try w.waitUntilRunning();
    {
        w.stop_mutex.lockUncancelable(testing.io);
        defer w.stop_mutex.unlock(testing.io);
        try testing.expect(w.core_ptrs[0] != null);
    }
    w.stop();
    try running.await(testing.io);
    try testing.expectEqual(@as(?[]const u8, null), w.fatalMessage());
}

test "the first poll failure is recorded with its kind, task queue and detail" {
    var client: client_mod.Client = undefined; // never touched: nothing is registered
    var w = try Worker.init(testing.allocator, testing.io, &client, "fatal", .{});
    defer w.deinit();
    w.recordPollFailure(.activity, "q1", error.PollFailed, "permission denied");
    w.recordPollFailure(.workflow, "q2", error.PollFailed, "later");
    try testing.expectEqualStrings("polling for activity tasks on task queue q1 failed: permission denied", w.fatalMessage().?);
    try testing.expectEqual(@as(?Worker.PollError, error.PollFailed), w.fatal);

    // Without a message from the core, the error's name is the detail.
    var bare = try Worker.init(testing.allocator, testing.io, &client, "bare", .{});
    defer bare.deinit();
    bare.recordPollFailure(.workflow, "q3", error.OutOfMemory, null);
    try testing.expectEqualStrings("polling for workflow tasks on task queue q3 failed: OutOfMemory", bare.fatalMessage().?);

    // A run clears the last run's failure first, even one that returns early.
    try testing.expectError(error.NothingRegistered, w.run());
    try testing.expectEqual(@as(?[]const u8, null), w.fatalMessage());
}

/// Runs a worker on a dev server, reports a poll failure to it as a poll loop does, and
/// expects `run` (through `runFn`) to shut down and return it.
fn expectPollFailureReturned(comptime runFn: fn (*Worker) Worker.RunError!void, task_queue: []const u8) !void {
    const rt = try bridge.Runtime.init(testing.allocator, .{}, null);
    defer rt.deinit();
    var server = try bridge.EphemeralServer.startDevServer(testing.allocator, testing.io, rt, .{}, null);
    defer server.shutdown(testing.io, null) catch {};
    var client = try client_mod.Client.connect(testing.allocator, testing.io, .{ .host_port = server.target });
    defer client.close();

    var w = try Worker.init(testing.allocator, testing.io, &client, task_queue, .{});
    defer w.deinit();
    try w.registerWorkflow(HelloTest.Hello);
    try w.registerActivity(HelloTest.Greet);
    var running = try testing.io.concurrent(runFn, .{&w});
    try w.waitUntilRunning();
    w.recordPollFailure(.activity, w.task_queue, error.PollFailed, "boom");
    try testing.expectError(error.PollFailed, running.await(testing.io));
    const expected = try std.fmt.allocPrint(testing.allocator, "polling for activity tasks on task queue {s} failed: boom", .{task_queue});
    defer testing.allocator.free(expected);
    try testing.expectEqualStrings(expected, w.fatalMessage().?);
    w.stop_mutex.lockUncancelable(testing.io);
    defer w.stop_mutex.unlock(testing.io);
    try testing.expectEqual(@as(?*bridge.c.TemporalCoreWorker, null), w.core_ptrs[0]);
}

test "run returns a poll failure after shutting down" {
    try expectPollFailureReturned(HelloTest.runWorker, "poll-failure");
}

test "runUntilSignal returns a poll failure after shutting down" {
    try expectPollFailureReturned(HelloTest.runWorkerUntilSignal, "poll-failure-signal");
    SignalWatcher.mutex.lockUncancelable(testing.io);
    defer SignalWatcher.mutex.unlock(testing.io);
    try testing.expectEqual(0, SignalWatcher.workers.len());
}

test "one SIGTERM stops every worker running until a signal, then the previous handler is back" {
    const rt = try bridge.Runtime.init(testing.allocator, .{}, null);
    defer rt.deinit();
    var server = try bridge.EphemeralServer.startDevServer(testing.allocator, testing.io, rt, .{}, null);
    defer server.shutdown(testing.io, null) catch {};
    var client = try client_mod.Client.connect(testing.allocator, testing.io, .{ .host_port = server.target });
    defer client.close();

    var a = try Worker.init(testing.allocator, testing.io, &client, "signal-a", .{});
    defer a.deinit();
    try a.registerActivity(HelloTest.Greet);
    var b = try Worker.init(testing.allocator, testing.io, &client, "signal-b", .{});
    defer b.deinit();
    try b.registerActivity(HelloTest.Greet);

    var running_a = try testing.io.concurrent(HelloTest.runWorkerUntilSignal, .{&a});
    var running_b = try testing.io.concurrent(HelloTest.runWorkerUntilSignal, .{&b});
    // Both must be registered before the signal, or the second would start after it.
    while (true) {
        SignalWatcher.mutex.lockUncancelable(testing.io);
        const both = SignalWatcher.workers.len() == 2;
        SignalWatcher.mutex.unlock(testing.io);
        if (both) break;
        try testing.io.sleep(.fromMilliseconds(10), .awake);
    }
    try std.posix.kill(std.c.getpid(), .TERM);
    try running_a.await(testing.io);
    try running_b.await(testing.io);

    var current: std.posix.Sigaction = undefined;
    std.posix.sigaction(.TERM, null, &current);
    try testing.expect(current.handler.handler != SignalWatcher.onSignal);
    try testing.expectEqual(@as(?std.Thread, null), SignalWatcher.thread);
}

/// A thread-safe sink that keeps SDK records and counts forwarded core records (those with a
/// `target` field) without keeping them.
const RecordingSink = struct {
    mutex: Io.Mutex = .init,
    arena: std.heap.ArenaAllocator = .init(testing.allocator),
    records: std.ArrayList(Record) = .empty,
    core_records: usize = 0,
    /// Broadcast for each record kept.
    changed: Io.Condition = .init,

    const Record = struct { level: log_mod.Level, message: []const u8, fields: []const log_mod.Field };

    fn sink(r: *RecordingSink) log_mod.Sink {
        return .{ .ptr = r, .vtable = &.{ .log = record } };
    }

    fn deinit(r: *RecordingSink) void {
        r.arena.deinit();
    }

    fn record(ptr: ?*anyopaque, level: log_mod.Level, message: []const u8, fields: []const log_mod.Field) void {
        const r: *RecordingSink = @ptrCast(@alignCast(ptr.?));
        r.mutex.lockUncancelable(testing.io);
        defer r.mutex.unlock(testing.io);
        for (fields) |f| if (std.mem.eql(u8, f.key, "target")) {
            r.core_records += 1;
            return;
        };
        const a = r.arena.allocator();
        const copy = a.alloc(log_mod.Field, fields.len) catch return;
        for (fields, copy) |f, *c| c.* = .{ .key = a.dupe(u8, f.key) catch return, .value = a.dupe(u8, f.value) catch return };
        r.records.append(a, .{ .level = level, .message = a.dupe(u8, message) catch return, .fields = copy }) catch return;
        r.changed.broadcast(testing.io);
    }

    /// How many records have `message` and, for each pair in `want`, a field with that value.
    fn count(r: *RecordingSink, message: []const u8, want: []const log_mod.Field) usize {
        r.mutex.lockUncancelable(testing.io);
        defer r.mutex.unlock(testing.io);
        return r.countLocked(message, want, null);
    }

    /// Like `count`, only counting records at `level`.
    fn countAt(r: *RecordingSink, level: log_mod.Level, message: []const u8, want: []const log_mod.Field) usize {
        r.mutex.lockUncancelable(testing.io);
        defer r.mutex.unlock(testing.io);
        return r.countLocked(message, want, level);
    }

    /// Waits up to `timeout` for a record that `count` would count.
    fn waitFor(r: *RecordingSink, message: []const u8, want: []const log_mod.Field, timeout: Io.Duration) !void {
        const deadline = Io.Clock.Timestamp.fromNow(testing.io, .{ .raw = timeout, .clock = .awake });
        r.mutex.lockUncancelable(testing.io);
        defer r.mutex.unlock(testing.io);
        while (r.countLocked(message, want, null) == 0) try r.changed.waitTimeout(testing.io, &r.mutex, .{ .deadline = deadline });
    }

    fn countLocked(r: *RecordingSink, message: []const u8, want: []const log_mod.Field, level: ?log_mod.Level) usize {
        var n: usize = 0;
        records: for (r.records.items) |rec| {
            if (!std.mem.eql(u8, rec.message, message)) continue;
            if (level) |l| if (rec.level != l) continue;
            for (want) |w| {
                const found = for (rec.fields) |f| {
                    if (std.mem.eql(u8, f.key, w.key)) break std.mem.eql(u8, f.value, w.value);
                } else false;
                if (!found) continue :records;
            }
            n += 1;
        }
        return n;
    }

    fn coreRecords(r: *RecordingSink) usize {
        r.mutex.lockUncancelable(testing.io);
        defer r.mutex.unlock(testing.io);
        return r.core_records;
    }
};

// With no workflow cache, every activation replays the run from the start, so the workflow's
// first log call runs again (replaying) once the activity's result arrives.
test "workflow and activity logs reach the worker's sink with their context fields, and replays log only when asked" {
    const rt = try bridge.Runtime.init(testing.allocator, .{}, null);
    defer rt.deinit();
    var server = try bridge.EphemeralServer.startDevServer(testing.allocator, testing.io, rt, .{}, null);
    defer server.shutdown(testing.io, null) catch {};

    var client_sink: RecordingSink = .{};
    defer client_sink.deinit();
    var client = try client_mod.Client.connect(testing.allocator, testing.io, .{
        .host_port = server.target,
        .logger = client_sink.sink(),
        .core_log_level = .debug,
    });
    defer client.close();

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    for ([_]bool{ false, true }) |log_during_replay| {
        var sink: RecordingSink = .{};
        defer sink.deinit();
        const task_queue = if (log_during_replay) "logs-replaying" else "logs";
        var w = try Worker.init(testing.allocator, testing.io, &client, task_queue, .{
            .max_cached_workflows = 0,
            .logger = sink.sink(),
            .log_during_replay = log_during_replay,
        });
        defer w.deinit();
        try w.registerWorkflow(HelloTest.Hello);
        try w.registerActivity(HelloTest.Greet);
        var running = try testing.io.concurrent(HelloTest.runWorker, .{&w});
        defer {
            w.stop();
            running.await(testing.io) catch {};
        }

        const id = if (log_during_replay) "logs-replaying-wf" else "logs-wf";
        var run = try client.executeWorkflow(HelloTest.Hello, .{ .id = id, .task_queue = task_queue }, .{"Temporal"});
        defer run.deinit();
        try testing.expectEqualStrings("Hello Temporal!", try run.get(arena.allocator()));

        const workflow_fields = [_]log_mod.Field{
            .{ .key = "name", .value = "Temporal" },
            .{ .key = "WorkflowType", .value = "Workflow" },
            .{ .key = "WorkflowID", .value = id },
            .{ .key = "RunID", .value = run.run_id },
        };
        const started = sink.count("HelloWorld workflow started", &workflow_fields);
        if (log_during_replay) try testing.expect(started >= 2) else try testing.expectEqual(1, started);
        try testing.expectEqual(1, sink.count("HelloWorld workflow completed.", &.{.{ .key = "result", .value = "Hello Temporal!" }}));
        try testing.expectEqual(1, sink.count("Activity", &.{
            .{ .key = "name", .value = "Temporal" },
            .{ .key = "ActivityType", .value = "Activity" },
            .{ .key = "Attempt", .value = "1" },
            .{ .key = "WorkflowType", .value = "Workflow" },
            .{ .key = "WorkflowID", .value = id },
            .{ .key = "RunID", .value = run.run_id },
        }));
        // Nothing went to the client's sink: the worker's overrides it.
        try testing.expectEqual(0, client_sink.count("Activity", &.{}));
    }
    // The core logs at debug level while it polls and completes tasks.
    try testing.expect(client_sink.coreRecords() > 0);
}

test "a worker without its own sink uses its client's" {
    const rt = try bridge.Runtime.init(testing.allocator, .{}, null);
    defer rt.deinit();
    var server = try bridge.EphemeralServer.startDevServer(testing.allocator, testing.io, rt, .{}, null);
    defer server.shutdown(testing.io, null) catch {};

    var sink: RecordingSink = .{};
    defer sink.deinit();
    var client = try client_mod.Client.connect(testing.allocator, testing.io, .{ .host_port = server.target, .logger = sink.sink() });
    defer client.close();
    var w = try Worker.init(testing.allocator, testing.io, &client, "client-logs", .{});
    defer w.deinit();
    try w.registerWorkflow(HelloTest.Hello);
    try w.registerActivity(HelloTest.Greet);
    var running = try testing.io.concurrent(HelloTest.runWorker, .{&w});
    defer {
        w.stop();
        running.await(testing.io) catch {};
    }

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var run = try client.executeWorkflow(HelloTest.Hello, .{ .id = "client-logs-wf", .task_queue = "client-logs" }, .{"Temporal"});
    defer run.deinit();
    _ = try run.get(arena.allocator());
    try testing.expectEqual(1, sink.count("HelloWorld workflow started", &.{.{ .key = "WorkflowID", .value = "client-logs-wf" }}));
    try testing.expectEqual(1, sink.count("Activity", &.{.{ .key = "WorkflowID", .value = "client-logs-wf" }}));
}

/// Passes payloads through and records, for each call, the propagated tenant and the
/// workflow its context names. Used from one thread.
const ContextRecordingCodec = struct {
    const tenant = propagation.key([]const u8, "tenant");
    arena: Allocator,
    calls: std.ArrayList([]const u8) = .empty,

    fn codec(r: *ContextRecordingCodec) converter.PayloadCodec {
        return .{ .ptr = r, .vtable = &.{ .encode = plain, .decode = plain, .encodeWithContext = encode, .decodeWithContext = decode } };
    }

    fn plain(_: *anyopaque, arena: Allocator, _: Io, payloads: []const converter.Payload) converter.CodecError![]converter.Payload {
        return arena.dupe(converter.Payload, payloads);
    }

    fn encode(ptr: *anyopaque, arena: Allocator, _: Io, context: *const converter.CodecContext, payloads: []const converter.Payload) converter.CodecError![]converter.Payload {
        try record(ptr, "encode", context);
        return arena.dupe(converter.Payload, payloads);
    }

    fn decode(ptr: *anyopaque, arena: Allocator, _: Io, context: *const converter.CodecContext, payloads: []const converter.Payload) converter.CodecError![]converter.Payload {
        try record(ptr, "decode", context);
        return arena.dupe(converter.Payload, payloads);
    }

    fn record(ptr: *anyopaque, what: []const u8, context: *const converter.CodecContext) !void {
        const r: *ContextRecordingCodec = @ptrCast(@alignCast(ptr));
        const call = try std.fmt.allocPrint(r.arena, "{s} tenant={s} {s}/{s}/{s}", .{ what, context.values.get(tenant) orelse "-", context.namespace, context.workflow_type, context.workflow_id });
        try r.calls.append(r.arena, call);
    }
};

test "an activity's info carries its Start task's schedule and start times" {
    var start: task_pb.Start = .{ .activity_id = "1", .activity_type = "Charge", .attempt = 2 };
    try testing.expect(activityInfo(&start, "tq", "token").started_time == null);
    start.scheduled_time = .{ .seconds = 100 };
    start.current_attempt_scheduled_time = .{ .seconds = 105, .nanos = 7 };
    start.started_time = .{ .seconds = 106 };
    const info = activityInfo(&start, "tq", "token");
    try testing.expectEqual(@as(i96, 100 * std.time.ns_per_s), info.scheduled_time.?.nanoseconds);
    try testing.expectEqual(@as(i96, 105 * std.time.ns_per_s + 7), info.current_attempt_scheduled_time.?.nanoseconds);
    try testing.expectEqual(@as(i96, 106 * std.time.ns_per_s), info.started_time.?.nanoseconds);
    try testing.expectEqualStrings("Charge", info.activity_type);
    try testing.expectEqual(@as(u32, 2), info.attempt);
}

const CodecContextTest = struct {
    fn echo(ctx: *workflow.Context, text: []const u8) ![]const u8 {
        _ = ctx;
        return text;
    }
    const Echo = workflow.Def("Echo", echo);
};

const MemoryLimitTest = struct {
    fn nap(ctx: *workflow.Context) ![]const u8 {
        try ctx.io().sleep(.fromSeconds(30), .awake);
        return "rested";
    }
    const Nap = workflow.Def("Nap", nap);
};

test "the workflow runner passes the memory limit to its runs" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var workflows: std.StringHashMapUnmanaged(workflow.Registered) = .empty;
    defer workflows.deinit(testing.allocator);
    try workflows.put(testing.allocator, MemoryLimitTest.Nap.workflow_name, MemoryLimitTest.Nap.registered);

    for ([_]?usize{ null, 1 }) |limit| {
        var runner: WorkflowRunner = .{
            .gpa = testing.allocator,
            .workflows = &workflows,
            .task_queue = "tq",
            .namespace = "ns",
            .stack_size = 64 * 1024,
            .memory_limit = limit,
            .logger = .{ .ptr = null, .vtable = &.{ .log = struct {
                fn f(_: ?*anyopaque, _: log_mod.Level, _: []const u8, _: []const log_mod.Field) void {}
            }.f } },
        };
        defer runner.deinit();
        const init: act_pb.InitializeWorkflow = .{ .workflow_type = "Nap", .workflow_id = "wf-1", .randomness_seed = 7, .attempt = 1 };
        var act: act_pb.WorkflowActivation = .{ .run_id = "run-1", .timestamp = .{ .seconds = 1_700_000_000 } };
        try act.jobs.append(a, .{ .variant = .{ .initialize_workflow = init } });
        var bytes: Io.Writer.Allocating = .init(a);
        try act.encode(&bytes.writer, a);
        const completion = (try runner.handle(bytes.written(), a, testing.io)).completion;
        if (limit == null) {
            try testing.expect(completion.status.?.successful.commands.items[0].variant.? == .start_timer);
        } else {
            const f = completion.status.?.failed.failure.?;
            try testing.expectEqualStrings("WorkflowMemoryLimitExceeded", failure_mod.applicationType(f).?);
        }
    }
}

test "the workflow runner hands codecs the run's propagated values and identity" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var recorder: ContextRecordingCodec = .{ .arena = a };
    const codecs = [_]converter.PayloadCodec{recorder.codec()};
    const propagators = [_]propagation.ContextPropagator{propagation.HeaderPropagator(ContextRecordingCodec.tenant).propagator};
    var workflows: std.StringHashMapUnmanaged(workflow.Registered) = .empty;
    defer workflows.deinit(testing.allocator);
    try workflows.put(testing.allocator, CodecContextTest.Echo.workflow_name, CodecContextTest.Echo.registered);
    var runner: WorkflowRunner = .{
        .gpa = testing.allocator,
        .workflows = &workflows,
        .task_queue = "tq",
        .namespace = "ns",
        .stack_size = 256 * 1024,
        .codecs = &codecs,
        .propagators = &propagators,
    };
    defer runner.deinit();

    var init: act_pb.InitializeWorkflow = .{ .workflow_type = "Echo", .workflow_id = "wf-1", .randomness_seed = 7, .attempt = 1 };
    init.arguments = try converter.toPayloads(a, .{@as([]const u8, "hi")});
    try init.headers.append(a, .{ .key = "tenant", .value = try converter.toPayload(a, @as([]const u8, "acme")) });
    var act: act_pb.WorkflowActivation = .{ .run_id = "run-1", .timestamp = .{ .seconds = 1_700_000_000 } };
    try act.jobs.append(a, .{ .variant = .{ .initialize_workflow = init } });
    var bytes: Io.Writer.Allocating = .init(a);
    try act.encode(&bytes.writer, a);

    const completion = (try runner.handle(bytes.written(), a, testing.io)).completion;
    const commands = completion.status.?.successful.commands.items;
    try testing.expectEqualStrings("\"hi\"", commands[0].variant.?.complete_workflow_execution.result.?.data);
    // The start header is decoded before its values are known; the arguments and the
    // completion see them.
    const want = [_][]const u8{
        "decode tenant=- ns/Echo/wf-1",
        "decode tenant=acme ns/Echo/wf-1",
        "encode tenant=acme ns/Echo/wf-1",
    };
    try testing.expectEqual(want.len, recorder.calls.items.len);
    for (want, recorder.calls.items) |w, got| try testing.expectEqualStrings(w, got);
    // The workflow saw the header decoded once, not dropped by the two-step decode.
    try testing.expectEqualStrings("acme", runner.runs.get("run-1").?.inst.propagated.get(ContextRecordingCodec.tenant).?);
}

/// Workflows that call `hook` from their code, so a test can hold an activation in workflow
/// code or see two runs' code at the same time. The hook is test code that blocks on the
/// test's Io, outside the workflow's: real workflow code must never do this.
const ParallelTest = struct {
    var hook: *const fn (name: []const u8) bool = noHook;

    fn noHook(_: []const u8) bool {
        return true;
    }

    /// Calls the hook, then waits on two timers, so each run has three activations.
    fn hold(ctx: *workflow.Context, name: []const u8) ![]const u8 {
        _ = hook(name);
        try ctx.io().sleep(.fromSeconds(1), .awake);
        try ctx.io().sleep(.fromSeconds(1), .awake);
        return name;
    }
    const Hold = workflow.Def("ParallelHold", hold);

    fn meet(ctx: *workflow.Context, name: []const u8) ![]const u8 {
        _ = ctx;
        return if (hook(name)) "met" else "alone";
    }
    const Meet = workflow.Def("ParallelMeet", meet);

    /// Holds run "A" in its hook until `release` is set; `entered` says it got there.
    const Gate = struct {
        var entered: Io.Event = .unset;
        var release: Io.Event = .unset;

        fn reset() void {
            entered = .unset;
            release = .unset;
        }

        fn hook(name: []const u8) bool {
            if (!std.mem.eql(u8, name, "A")) return true;
            entered.set(testing.io);
            release.waitUncancelable(testing.io);
            return true;
        }
    };

    /// Two arrivals: each waits, with a timeout below the workflow task timeout, for the other.
    const Latch = struct {
        var arrived: std.atomic.Value(u32) = .init(0);
        var both: Io.Event = .unset;

        fn hook(_: []const u8) bool {
            if (arrived.fetchAdd(1, .acq_rel) + 1 == 2) both.set(testing.io);
            both.waitTimeout(testing.io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } }) catch return false;
            return true;
        }
    };

    fn runner(workflows: *const std.StringHashMapUnmanaged(workflow.Registered), sink: log_mod.Sink) WorkflowRunner {
        return .{ .gpa = testing.allocator, .workflows = workflows, .task_queue = "tq", .namespace = "ns", .stack_size = 64 * 1024, .logger = sink };
    }

    fn registry(gpa: Allocator) !std.StringHashMapUnmanaged(workflow.Registered) {
        var workflows: std.StringHashMapUnmanaged(workflow.Registered) = .empty;
        errdefer workflows.deinit(gpa);
        try workflows.put(gpa, Hold.workflow_name, Hold.registered);
        try workflows.put(gpa, MemoryLimitTest.Nap.workflow_name, MemoryLimitTest.Nap.registered);
        return workflows;
    }

    fn encode(a: Allocator, act: act_pb.WorkflowActivation) ![]const u8 {
        var bytes: Io.Writer.Allocating = .init(a);
        try act.encode(&bytes.writer, a);
        return bytes.written();
    }

    /// An activation starting run `run_id` of `workflow_type` with argument `name`.
    fn start(a: Allocator, run_id: []const u8, workflow_type: []const u8, name: []const u8) ![]const u8 {
        var init: act_pb.InitializeWorkflow = .{ .workflow_type = workflow_type, .workflow_id = try std.fmt.allocPrint(a, "wf-{s}", .{run_id}), .randomness_seed = 7, .attempt = 1 };
        init.arguments = try converter.toPayloads(a, .{name});
        var act: act_pb.WorkflowActivation = .{ .run_id = run_id, .timestamp = .{ .seconds = 1_700_000_000 } };
        try act.jobs.append(a, .{ .variant = .{ .initialize_workflow = init } });
        return encode(a, act);
    }

    fn fireTimer(a: Allocator, run_id: []const u8, seq: u32) ![]const u8 {
        var act: act_pb.WorkflowActivation = .{ .run_id = run_id, .timestamp = .{ .seconds = 1_700_000_001 } };
        try act.jobs.append(a, .{ .variant = .{ .fire_timer = .{ .seq = seq } } });
        return encode(a, act);
    }

    fn evict(a: Allocator, run_id: []const u8, reason: act_pb.RemoveFromCache.EvictionReason) ![]const u8 {
        var act: act_pb.WorkflowActivation = .{ .run_id = run_id };
        try act.jobs.append(a, .{ .variant = .{ .remove_from_cache = .{ .message = "core says why", .reason = reason } } });
        return encode(a, act);
    }

    /// Handles `bytes` on a thread of its own and stores the encoded completion in `out`.
    const OnThread = struct {
        r: *WorkflowRunner,
        bytes: []const u8,
        arena: std.heap.ArenaAllocator = .init(testing.allocator),
        out: anyerror![]const u8 = error.NotRun,

        fn run(t: *OnThread) void {
            t.out = handleEncoded(t.r, t.bytes, t.arena.allocator());
        }

        fn deinit(t: *OnThread) void {
            t.arena.deinit();
        }
    };

    fn handleEncoded(r: *WorkflowRunner, bytes: []const u8, a: Allocator) ![]const u8 {
        const handled = try r.handle(bytes, a, testing.io);
        var out: Io.Writer.Allocating = .init(a);
        try handled.completion.encode(&out.writer, a);
        return out.written();
    }

    fn decodeCompletion(a: Allocator, bytes: []const u8) !done_pb.WorkflowActivationCompletion {
        var reader: Io.Reader = .fixed(bytes);
        return done_pb.WorkflowActivationCompletion.decode(&reader, a);
    }

    fn timerSeq(completion: done_pb.WorkflowActivationCompletion) !u32 {
        const commands = completion.status.?.successful.commands.items;
        try testing.expectEqual(1, commands.len);
        return commands[0].variant.?.start_timer.seq;
    }
};

test "activations of different runs are processed in parallel" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var workflows = try ParallelTest.registry(testing.allocator);
    defer workflows.deinit(testing.allocator);
    var sink: RecordingSink = .{};
    defer sink.deinit();
    var runner = ParallelTest.runner(&workflows, sink.sink());
    defer runner.deinit();
    ParallelTest.Gate.reset();
    ParallelTest.hook = ParallelTest.Gate.hook;
    defer ParallelTest.hook = ParallelTest.noHook;

    var run_a: ParallelTest.OnThread = .{ .r = &runner, .bytes = try ParallelTest.start(a, "A", "ParallelHold", "A") };
    defer run_a.deinit();
    const thread = try std.Thread.spawn(.{}, ParallelTest.OnThread.run, .{&run_a});
    try ParallelTest.Gate.entered.wait(testing.io);

    // A is held in its workflow code; B runs to the end of its activation meanwhile.
    const b = try ParallelTest.decodeCompletion(a, try ParallelTest.handleEncoded(&runner, try ParallelTest.start(a, "B", "ParallelHold", "B"), a));
    try testing.expectEqualStrings("B", b.run_id);
    _ = try ParallelTest.timerSeq(b);
    try testing.expect(!ParallelTest.Gate.release.isSet());

    ParallelTest.Gate.release.set(testing.io);
    thread.join();
    _ = try ParallelTest.timerSeq(try ParallelTest.decodeCompletion(a, try run_a.out));
}

test "a run's activations may each run on a different thread" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var workflows = try ParallelTest.registry(testing.allocator);
    defer workflows.deinit(testing.allocator);
    var sink: RecordingSink = .{};
    defer sink.deinit();
    var same_thread = ParallelTest.runner(&workflows, sink.sink());
    defer same_thread.deinit();
    var migrating = ParallelTest.runner(&workflows, sink.sink());
    defer migrating.deinit();

    var bytes = try ParallelTest.start(a, "run-1", "ParallelHold", "x");
    for (0..3) |i| {
        const want = try ParallelTest.handleEncoded(&same_thread, bytes, a);
        // A new thread for each activation, so the run's coroutine resumes on another stack.
        var on: ParallelTest.OnThread = .{ .r = &migrating, .bytes = bytes };
        defer on.deinit();
        const thread = try std.Thread.spawn(.{}, ParallelTest.OnThread.run, .{&on});
        thread.join();
        try testing.expectEqualSlices(u8, want, try on.out);

        const completion = try ParallelTest.decodeCompletion(a, want);
        if (i < 2) {
            bytes = try ParallelTest.fireTimer(a, "run-1", try ParallelTest.timerSeq(completion));
        } else {
            const result = completion.status.?.successful.commands.items[0].variant.?.complete_workflow_execution.result.?;
            try testing.expectEqualStrings("\"x\"", result.data);
        }
    }
}

test "a second activation for a busy run fails its task" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var workflows = try ParallelTest.registry(testing.allocator);
    defer workflows.deinit(testing.allocator);
    var sink: RecordingSink = .{};
    defer sink.deinit();
    var runner = ParallelTest.runner(&workflows, sink.sink());
    defer runner.deinit();
    ParallelTest.Gate.reset();
    ParallelTest.hook = ParallelTest.Gate.hook;
    defer ParallelTest.hook = ParallelTest.noHook;

    var run_a: ParallelTest.OnThread = .{ .r = &runner, .bytes = try ParallelTest.start(a, "A", "ParallelHold", "A") };
    defer run_a.deinit();
    const thread = try std.Thread.spawn(.{}, ParallelTest.OnThread.run, .{&run_a});
    try ParallelTest.Gate.entered.wait(testing.io);

    const want = "workflow run A received an activation while another was still being processed; the core must send one at a time";
    for ([_][]const u8{ try ParallelTest.fireTimer(a, "A", 1), try ParallelTest.evict(a, "A", .CACHE_FULL) }) |bytes| {
        const handled = try runner.handle(bytes, a, testing.io);
        try testing.expect(!handled.eviction);
        try testing.expectEqualStrings(want, handled.completion.status.?.failed.failure.?.message);
    }

    ParallelTest.Gate.release.set(testing.io);
    thread.join();
    _ = try ParallelTest.timerSeq(try ParallelTest.decodeCompletion(a, try run_a.out));
    // The guard left the run cached and no longer busy.
    _ = try ParallelTest.timerSeq((try runner.handle(try ParallelTest.fireTimer(a, "A", 1), a, testing.io)).completion);
}

test "an activation that can't be handled gets a fallback failed completion" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var workflows = try ParallelTest.registry(testing.allocator);
    defer workflows.deinit(testing.allocator);
    var sink: RecordingSink = .{};
    defer sink.deinit();
    var failing: std.testing.FailingAllocator = .init(testing.allocator, .{ .fail_index = 0 });
    var runner = ParallelTest.runner(&workflows, sink.sink());
    runner.gpa = failing.allocator();
    defer runner.deinit();
    var buf: [WorkflowRunner.fallback_buffer_size]u8 = undefined;

    const response = runner.respond(try ParallelTest.start(a, "run-1", "ParallelHold", "x"), failing.allocator(), testing.io, &buf);
    try testing.expect(response.completion.fallback);
    const completion = try ParallelTest.decodeCompletion(a, response.completion.bytes);
    try testing.expectEqualStrings("run-1", completion.run_id);
    try testing.expectEqualStrings("the worker could not process this workflow activation: OutOfMemory; see the worker's log", completion.status.?.failed.failure.?.message);
    try testing.expectEqual(1, sink.countAt(.err, "processing a workflow activation failed", &.{ .{ .key = "run_id", .value = "run-1" }, .{ .key = "error", .value = "OutOfMemory" } }));

    // No run ID: nothing can be sent.
    var no_run_id: act_pb.WorkflowActivation = .{ .timestamp = .{ .seconds = 1 } };
    try no_run_id.jobs.append(a, .{ .variant = .{ .fire_timer = .{ .seq = 1 } } });
    try testing.expect(runner.respond(try ParallelTest.encode(a, no_run_id), failing.allocator(), testing.io, &buf) == .none);

    // A run ID too long for the buffer: the core is asked to evict the run.
    const long_id = try a.alloc(u8, 2 * WorkflowRunner.fallback_buffer_size);
    @memset(long_id, 'r');
    const evict = runner.respond(try ParallelTest.fireTimer(a, long_id, 1), failing.allocator(), testing.io, &buf).evict;
    try testing.expectEqualStrings(long_id, evict.run_id);
}

test "a run ID is read from an activation's bytes without decoding it" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    try testing.expectEqualStrings("run-1", runId(try ParallelTest.fireTimer(a, "run-1", 3)).?);
    // Fields before the run ID are skipped.
    try testing.expectEqualStrings("run-2", runId(&.{ 0x10, 0x96, 0x01, 0x0a, 5, 'r', 'u', 'n', '-', '2' }).?);
    try testing.expect(runId(&.{}) == null);
    try testing.expect(runId(&.{ 0x0a, 0 }) == null);
    try testing.expect(runId(&.{ 0x0a, 9, 'r' }) == null);
    try testing.expect(runId(&.{ 0x10, 0xff }) == null);
}

test "evictions are logged by reason unless a callback takes them" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var workflows = try ParallelTest.registry(testing.allocator);
    defer workflows.deinit(testing.allocator);
    var sink: RecordingSink = .{};
    defer sink.deinit();
    var runner = ParallelTest.runner(&workflows, sink.sink());
    defer runner.deinit();

    const Case = struct { reason: act_pb.RemoveFromCache.EvictionReason, name: []const u8, level: log_mod.Level, message: []const u8 };
    const routine = "workflow run evicted from the cache";
    const cases = [_]Case{
        .{ .reason = .NONDETERMINISM, .name = "NONDETERMINISM", .level = .err, .message = "workflow run evicted: its code produced commands that don't match its history (nondeterminism); the workflow task fails and is retried until the code matches" },
        .{ .reason = .FATAL, .name = "FATAL", .level = .err, .message = "workflow run evicted after a fatal error in the core" },
        .{ .reason = .PAGINATION_OR_HISTORY_FETCH, .name = "PAGINATION_OR_HISTORY_FETCH", .level = .warn, .message = "workflow run evicted: fetching its history failed" },
        .{ .reason = .CACHE_FULL, .name = "CACHE_FULL", .level = .debug, .message = routine },
        .{ .reason = .CACHE_MISS, .name = "CACHE_MISS", .level = .debug, .message = routine },
        .{ .reason = .LANG_FAIL, .name = "LANG_FAIL", .level = .debug, .message = routine },
        .{ .reason = .LANG_REQUESTED, .name = "LANG_REQUESTED", .level = .debug, .message = routine },
        .{ .reason = .TASK_NOT_FOUND, .name = "TASK_NOT_FOUND", .level = .debug, .message = routine },
        .{ .reason = .UNHANDLED_COMMAND, .name = "UNHANDLED_COMMAND", .level = .debug, .message = routine },
        .{ .reason = .WORKFLOW_EXECUTION_ENDING, .name = "WORKFLOW_EXECUTION_ENDING", .level = .debug, .message = routine },
        .{ .reason = .UNSPECIFIED, .name = "UNSPECIFIED", .level = .debug, .message = routine },
        .{ .reason = @fromBackingInt(99), .name = "99", .level = .debug, .message = routine },
    };
    for (cases) |c| {
        const run_id = try std.fmt.allocPrint(a, "run-{s}", .{c.name});
        _ = try runner.handle(try ParallelTest.start(a, run_id, "Nap", ""), a, testing.io);
        const handled = try runner.handle(try ParallelTest.evict(a, run_id, c.reason), a, testing.io);
        try testing.expect(handled.eviction);
        try testing.expect(handled.completion.status.? == .successful);
        try testing.expect(runner.runs.get(run_id) == null);
        try testing.expectEqual(1, sink.countAt(c.level, c.message, &.{
            .{ .key = "run_id", .value = run_id },
            .{ .key = "reason", .value = c.name },
            .{ .key = "message", .value = "core says why" },
            .{ .key = "workflow_id", .value = try std.fmt.allocPrint(a, "wf-{s}", .{run_id}) },
            .{ .key = "workflow_type", .value = "Nap" },
        }));
    }
    // A run that isn't cached is logged without its workflow.
    _ = try runner.handle(try ParallelTest.evict(a, "unknown", .NONDETERMINISM), a, testing.io);
    try testing.expectEqual(1, sink.countAt(.err, cases[0].message, &.{.{ .key = "run_id", .value = "unknown" }}));

    const Evictions = struct {
        n: usize = 0,
        fn record(ctx: *anyopaque, reason: act_pb.RemoveFromCache.EvictionReason, message: []const u8) void {
            const e: *@This() = @ptrCast(@alignCast(ctx));
            if (reason == .NONDETERMINISM and std.mem.eql(u8, message, "core says why")) e.n += 1;
        }
    };
    var evictions: Evictions = .{};
    runner.on_evict = Evictions.record;
    runner.on_evict_ctx = &evictions;
    _ = try runner.handle(try ParallelTest.start(a, "run-cb", "Nap", ""), a, testing.io);
    _ = try runner.handle(try ParallelTest.evict(a, "run-cb", .NONDETERMINISM), a, testing.io);
    try testing.expectEqual(1, evictions.n);
    try testing.expectEqual(0, sink.count(cases[0].message, &.{.{ .key = "run_id", .value = "run-cb" }}));
}

const CacheTest = struct {
    const Release = workflow.SignalDef("release", fn () void);

    const State = struct {
        released: bool = false,
        fn release(s: *State) void {
            s.released = true;
        }
        fn isReleased(s: *State) bool {
            return s.released;
        }
    };

    fn waitForRelease(ctx: *workflow.Context, name: []const u8) ![]const u8 {
        const state = try ctx.allocator().create(State);
        state.* = .{};
        try ctx.setSignalHandler(Release, state, State.release);
        try ctx.await(state, State.isReleased);
        return name;
    }
    const WaitForRelease = workflow.Def("WaitForRelease", waitForRelease);

    /// Waits on a long timer the first time any run executes, and schedules an activity every
    /// time after: a replay of the first run's history doesn't match. Test-only global state.
    var executed: std.atomic.Value(bool) = .init(false);

    fn drift(ctx: *workflow.Context) ![]const u8 {
        if (!executed.swap(true, .acq_rel)) {
            try ctx.io().sleep(.fromSeconds(3600), .awake);
        } else {
            _ = try ctx.executeActivity(HelloTest.Greet, .{ .start_to_close_timeout = .fromSeconds(10) }, .{"drift"}).get();
        }
        return "done";
    }
    const Drift = workflow.Def("Drift", drift);
};

test "a full cache evicts a waiting run, which replays when signaled" {
    const rt = try bridge.Runtime.init(testing.allocator, .{}, null);
    defer rt.deinit();
    var server = try bridge.EphemeralServer.startDevServer(testing.allocator, testing.io, rt, .{}, null);
    defer server.shutdown(testing.io, null) catch {};
    var client = try client_mod.Client.connect(testing.allocator, testing.io, .{ .host_port = server.target });
    defer client.close();

    var sink: RecordingSink = .{};
    defer sink.deinit();
    var w = try Worker.init(testing.allocator, testing.io, &client, "cache-full", .{ .max_cached_workflows = 1, .logger = sink.sink() });
    defer w.deinit();
    try w.registerWorkflow(CacheTest.WaitForRelease);
    var running = try testing.io.concurrent(HelloTest.runWorker, .{&w});
    defer {
        w.stop();
        running.await(testing.io) catch {};
    }

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var first = try client.executeWorkflow(CacheTest.WaitForRelease, .{ .id = "cache-full-1", .task_queue = "cache-full" }, .{"first"});
    defer first.deinit();
    var second = try client.executeWorkflow(CacheTest.WaitForRelease, .{ .id = "cache-full-2", .task_queue = "cache-full" }, .{"second"});
    defer second.deinit();
    // Two runs, one cache slot: whichever ran first is evicted for the other.
    try sink.waitFor("workflow run evicted from the cache", &.{.{ .key = "reason", .value = "CACHE_FULL" }}, .fromSeconds(30));

    try client.signalWorkflow(CacheTest.Release, "cache-full-1", first.run_id, .{});
    try client.signalWorkflow(CacheTest.Release, "cache-full-2", second.run_id, .{});
    try testing.expectEqualStrings("first", try first.get(arena.allocator()));
    try testing.expectEqualStrings("second", try second.get(arena.allocator()));
}

// With no cache at all the core asks for a CACHE_FULL eviction on every activation, and that
// reason sticks, so the run is pushed out of a one-run cache by another run instead.
test "a run evicted for nondeterminism is logged with the core's message" {
    const rt = try bridge.Runtime.init(testing.allocator, .{}, null);
    defer rt.deinit();
    var server = try bridge.EphemeralServer.startDevServer(testing.allocator, testing.io, rt, .{}, null);
    defer server.shutdown(testing.io, null) catch {};
    // The core's own warnings about the failing task go here, not to std.log.
    var client_sink: RecordingSink = .{};
    defer client_sink.deinit();
    var client = try client_mod.Client.connect(testing.allocator, testing.io, .{ .host_port = server.target, .logger = client_sink.sink() });
    defer client.close();

    var sink: RecordingSink = .{};
    defer sink.deinit();
    var w = try Worker.init(testing.allocator, testing.io, &client, "nondeterminism", .{ .max_cached_workflows = 1, .logger = sink.sink() });
    defer w.deinit();
    try w.registerWorkflow(CacheTest.Drift);
    try w.registerWorkflow(CacheTest.WaitForRelease);
    CacheTest.executed.store(false, .release);
    var running = try testing.io.concurrent(HelloTest.runWorker, .{&w});
    defer {
        w.stop();
        running.await(testing.io) catch {};
    }

    var drift = try client.executeWorkflow(CacheTest.Drift, .{ .id = "nondeterminism-wf", .task_queue = "nondeterminism" }, .{});
    defer drift.deinit();
    var other = try client.executeWorkflow(CacheTest.WaitForRelease, .{ .id = "nondeterminism-other", .task_queue = "nondeterminism" }, .{"other"});
    defer other.deinit();
    defer client.terminateWorkflow("nondeterminism-other", other.run_id, "test done") catch {};
    try sink.waitFor("workflow run evicted from the cache", &.{ .{ .key = "run_id", .value = drift.run_id }, .{ .key = "reason", .value = "CACHE_FULL" } }, .fromSeconds(30));

    // The signal's task replays the run, whose code now schedules an activity, not a timer.
    try client.signalWorkflow(CacheTest.Release, "nondeterminism-wf", drift.run_id, .{});
    const message = "workflow run evicted: its code produced commands that don't match its history (nondeterminism); the workflow task fails and is retried until the code matches";
    const fields = [_]log_mod.Field{
        .{ .key = "run_id", .value = drift.run_id },
        .{ .key = "reason", .value = "NONDETERMINISM" },
        .{ .key = "workflow_id", .value = "nondeterminism-wf" },
        .{ .key = "workflow_type", .value = "Drift" },
    };
    try sink.waitFor(message, &fields, .fromSeconds(30));
    try testing.expect(sink.countAt(.err, message, &fields) >= 1);
    {
        sink.mutex.lockUncancelable(testing.io);
        defer sink.mutex.unlock(testing.io);
        const rec = for (sink.records.items) |rec| {
            if (std.mem.eql(u8, rec.message, message)) break rec;
        } else unreachable;
        const core_message = for (rec.fields) |f| {
            if (std.mem.eql(u8, f.key, "message")) break f.value;
        } else "";
        try testing.expect(std.mem.indexOf(u8, core_message, "ondeterminism") != null);
    }
    try client.terminateWorkflow("nondeterminism-wf", drift.run_id, "test done");
}

test "two workflows run their code at the same time on one worker" {
    const rt = try bridge.Runtime.init(testing.allocator, .{}, null);
    defer rt.deinit();
    var server = try bridge.EphemeralServer.startDevServer(testing.allocator, testing.io, rt, .{}, null);
    defer server.shutdown(testing.io, null) catch {};
    var client = try client_mod.Client.connect(testing.allocator, testing.io, .{ .host_port = server.target });
    defer client.close();

    var w = try Worker.init(testing.allocator, testing.io, &client, "parallel", .{});
    defer w.deinit();
    try w.registerWorkflow(ParallelTest.Meet);
    ParallelTest.Latch.arrived.store(0, .release);
    ParallelTest.Latch.both = .unset;
    ParallelTest.hook = ParallelTest.Latch.hook;
    defer ParallelTest.hook = ParallelTest.noHook;
    var running = try testing.io.concurrent(HelloTest.runWorker, .{&w});
    defer {
        w.stop();
        running.await(testing.io) catch {};
    }

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var first = try client.executeWorkflow(ParallelTest.Meet, .{ .id = "parallel-1", .task_queue = "parallel" }, .{"first"});
    defer first.deinit();
    var second = try client.executeWorkflow(ParallelTest.Meet, .{ .id = "parallel-2", .task_queue = "parallel" }, .{"second"});
    defer second.deinit();
    // Each run's code waits for the other's: processed one at a time, both would time out.
    try testing.expectEqualStrings("met", try first.get(arena.allocator()));
    try testing.expectEqualStrings("met", try second.get(arena.allocator()));
}
