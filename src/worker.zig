//! Workers: poll a task queue and run the workflows, activities, and Nexus services
//! registered on it.
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
//! between activations. Activities and Nexus requests run concurrently on the worker's Io, each
//! on its own task so a cancel from the core reaches it as `std.Io` cancellation.

const std = @import("std");
const bridge = @import("bridge.zig");
const proto = @import("proto.zig");
const proto_time = @import("proto_time.zig");
const converter = @import("converter.zig");
const failure_mod = @import("failure.zig");
const log_mod = @import("log.zig");
const workflow = @import("workflow.zig");
const activity = @import("activity.zig");
const nexus = @import("nexus.zig");
const wire = @import("nexus/wire.zig");
const client_mod = @import("client.zig");
const propagation = @import("propagation.zig");
const interceptor = @import("interceptor.zig");
const metrics_mod = @import("metrics.zig");
const Instance = @import("workflow/Instance.zig");
const Scheduler = @import("workflow/Scheduler.zig");
const CancelBlockingAllocator = @import("CancelBlockingAllocator.zig");
const io_tasks = @import("io_tasks.zig");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const act_pb = proto.coresdk.workflow_activation;
const done_pb = proto.coresdk.workflow_completion;
const task_pb = proto.coresdk.activity_task;
const result_pb = proto.coresdk.activity_result;
const builtin = @import("builtin");
const log = std.log.scoped(.temporal);

/// The smallest `Options.workflow_stack_size`: 64 KiB. `Worker.run` returns
/// `error.InvalidOptions` (logged) for less.
pub const min_workflow_stack_size: usize = 64 * 1024;

pub const Options = struct {
    /// How many workflow runs the worker keeps in memory between workflow tasks. Over the
    /// limit, the least recently used run is evicted, and replayed from its history when its
    /// next task arrives. 0 disables the cache: every task replays its run from the start.
    max_cached_workflows: u32 = 1000,
    /// How many workflow activations the worker processes at once, each on its own thread,
    /// for different runs.
    max_concurrent_workflow_task_executions: usize = 100,
    max_concurrent_activity_executions: usize = 100,
    /// How many local activities the worker runs at once, each on its own task: a fixed slot
    /// count, separate from `max_concurrent_activity_executions`. A local activity waits for a
    /// free slot up to its `schedule_to_start_timeout`.
    max_concurrent_local_activity_executions: usize = 100,
    /// How many Nexus requests (starts and cancels of operations) the worker handles at
    /// once, each on its own task. Only used with a registered Nexus service.
    max_concurrent_nexus_task_executions: usize = 100,
    /// How many workflow task polls the worker keeps open at once, split between the task
    /// queue and the worker's sticky queue by `nonsticky_to_sticky_poll_ratio`. Must be at
    /// least 2 while the cache is on (`max_cached_workflows` above 0). Ignored when
    /// `workflow_task_poller_behavior` is set.
    max_concurrent_workflow_task_pollers: usize = 2,
    /// How many activity task polls the worker keeps open at once; must be positive. Ignored
    /// when `activity_task_poller_behavior` is set.
    max_concurrent_activity_task_pollers: usize = 2,
    /// How many Nexus task polls the worker keeps open at once. Only used with a registered
    /// Nexus service; must be positive. Ignored when `nexus_task_poller_behavior` is set.
    max_concurrent_nexus_task_pollers: usize = 2,
    /// How the worker polls for workflow tasks: a fixed maximum, or autoscaling
    /// (`PollerBehavior` says what it does and what it needs from the server). Null: a
    /// simple maximum of `max_concurrent_workflow_task_pollers`. With the cache on, a simple
    /// maximum must be at least 2, and an autoscaling behavior applies to the task queue and
    /// to the worker's sticky queue each, so up to twice `maximum` polls can be open.
    workflow_task_poller_behavior: ?PollerBehavior = null,
    /// How the worker polls for activity tasks (`PollerBehavior`). Null: a simple maximum
    /// of `max_concurrent_activity_task_pollers`. Session queues (`sessions`) poll with the
    /// same behavior, so with autoscaling each of them scales on its own between the same
    /// bounds.
    activity_task_poller_behavior: ?PollerBehavior = null,
    /// How the worker polls for Nexus tasks (`PollerBehavior`). Null: a simple maximum of
    /// `max_concurrent_nexus_task_pollers`. Only used with a registered Nexus service.
    nexus_task_poller_behavior: ?PollerBehavior = null,
    /// The share of the workflow task pollers that poll the task queue itself rather than
    /// this worker's sticky queue, which brings the tasks of runs this worker has cached.
    /// Applies only when the workflow pollers are a simple maximum: the task queue gets the
    /// maximum times this ratio, rounded down and at least 1, and with the cache on the
    /// sticky queue gets the rest, at least 1. With the defaults, 2 pollers, that is one
    /// each. The task queue's share is the same with the cache off, when there is no sticky
    /// queue. Must be from 0 to 1; `run` returns `error.InvalidOptions` otherwise.
    nonsticky_to_sticky_poll_ratio: f32 = 0.2,
    /// Empty uses the client's identity.
    identity: []const u8 = "",
    /// How long a workflow task may wait on this worker's sticky queue before the server
    /// times it out and hands it to any worker on the task queue, which replays the run from
    /// its history. Must not be negative; counted in whole milliseconds.
    sticky_schedule_to_start_timeout: Io.Duration = .fromSeconds(10),
    /// The most activities per second this worker starts from its task queue; it polls no
    /// more while starting another would go over. Null: no limit. Must be positive and finite.
    /// A limit also turns off eager activity execution, whose starts would bypass it. Applies
    /// to the task queue's own activities, not to session activities (`sessions`).
    max_activities_per_second: ?f64 = null,
    /// The most activities per second the server hands out from this task queue, across all
    /// of its workers; set on each activity poll, so workers with different values override
    /// each other. Null: no limit. Must be positive and finite. A limit also turns off eager
    /// activity execution. Applies to the task queue's own activities, not to session
    /// activities (`sessions`).
    max_task_queue_activities_per_second: ?f64 = null,
    /// How often an activity's heartbeats reach the server when the activity has no heartbeat
    /// timeout; with one, they are sent at 80% of it, at most every
    /// `max_heartbeat_throttle_interval`. Heartbeats in between are batched, keeping only the
    /// latest details. Applies to session activities too. Must not be negative or above
    /// `max_heartbeat_throttle_interval`.
    default_heartbeat_throttle_interval: Io.Duration = .fromSeconds(30),
    /// The longest interval between an activity's heartbeats reaching the server. Applies to
    /// session activities too. Must not be negative.
    max_heartbeat_throttle_interval: Io.Duration = .fromSeconds(60),
    /// How many activities a workflow task may ask the server to start on this worker
    /// eagerly, in the answer to the task's completion, instead of through the task queue.
    /// Only activities on the worker's own task queue, and only while activity slots are
    /// free, are asked for. 0 turns eager activity execution off for the worker; so does
    /// either per-second limit.
    max_eager_activity_reservations_per_workflow_task: u32 = 3,
    /// Whether the worker polls its task queue for activity tasks. False runs the
    /// registered activities only as local activities of this worker's workflows
    /// (`workflow.Context.executeLocalActivity`): activities scheduled through the task queue
    /// wait for another worker, or time out. Needs a registered workflow when activities are
    /// registered, and can't be combined with `sessions`; `run` returns
    /// `error.InvalidOptions` otherwise.
    poll_activity_tasks: bool = true,
    /// Stack size for each workflow coroutine. At least `min_workflow_stack_size` (64 KiB),
    /// and small enough to round up to whole pages plus a guard page; `run` returns
    /// `error.InvalidOptions` otherwise.
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
    /// Deadlock detection: the longest that workflow code may run without returning to the
    /// workflow scheduler, that is, without blocking on the workflow's `std.Io` (a future,
    /// timer, condition, or other wait), yielding, or finishing. 2 seconds by default; null
    /// turns detection off, for example while a debugger may stop workflow code at a
    /// breakpoint. Must be positive.
    ///
    /// A watchdog on the worker's own `Io` and its real clock (`.awake`), never the
    /// workflow's deterministic `std.Io`, times each stretch of an activation's workflow code
    /// between returns to the scheduler. Work outside workflow code, such as payload codecs
    /// decoding the activation and encoding its completion, isn't counted. Nor is the SDK's
    /// reading of debug info for the stack trace query or for a failure's stack trace
    /// (`failure_stack_traces`). Workflow code that runs longer (a loop that never waits, or
    /// a call that blocks outside the workflow's `std.Io`) fails that workflow task with an
    /// application failure of type `deadlock_failure_type`, whose message names the workflow
    /// type and this limit, within about 1.25 times the limit. The worker logs one error with the workflow and run IDs,
    /// and the run is evicted, so its next workflow task replays it from history. The
    /// workflow itself does not fail: the server retries the task. Only the first attempt's
    /// failure is reported to the server; code that gets stuck on later attempts sends no
    /// failure, so those attempts end in a workflow task timeout. Detection never adds or
    /// changes a command, so replay is unaffected.
    ///
    /// The SDK cannot stop stuck code. It keeps its thread until it returns; then whatever
    /// it produced (commands, its completion) is discarded, no second completion is sent, and
    /// only then is the run's memory freed. Other runs keep being processed meanwhile, and a
    /// new task of the same run replays on a fresh copy of the run. `stop` doesn't wait for
    /// stuck code, but `run` returns only after every activation has returned, so a worker
    /// whose workflow code never returns never finishes `run`. Each activation's watchdog is
    /// a concurrent task on the worker's `Io`; an activation that can't get one runs
    /// unwatched, with a warning.
    deadlock_detection_timeout: ?Io.Duration = .fromSeconds(2),
    /// Put the error's return trace, as text with source locations, in `Failure.stack_trace`
    /// when the worker builds a failure from an error a workflow function, signal or update
    /// handler, or activity function returned (from the error's name or from `ctx.fail`). Only
    /// builds with error return tracing have a trace (Debug by default; in ReleaseSafe, set
    /// `.error_tracing = true` on the executable's root module and the module with the
    /// workflows and activities), and only one whose debug info the worker can read;
    /// otherwise the stack trace is empty. A failure passed on unchanged (a future's failure,
    /// `ctx.failWith`) keeps its own. The first worker of a process reads the executable's
    /// debug info before it starts polling, so its start takes that read's time (in a large
    /// Debug build, seconds) and a failed activity's completion doesn't. `stop` doesn't wait
    /// for the read; `Worker.deinit` does.
    failure_stack_traces: bool = true,
    /// Errors that fail the workflow task instead of the workflow when a workflow function,
    /// signal handler, or update handler returns them, for every workflow of this worker. A
    /// workflow definition adds its own with `workflow.DefOptions.task_failure_errors`; an
    /// error either lists fails the task. Empty (the default) keeps the default policy:
    /// error.OutOfMemory fails the task, error.Canceled cancels the workflow, and every other
    /// error fails the workflow.
    ///
    /// The task failure is an application failure whose type is the error's name and whose
    /// message names the error and what returned it. The workflow stays running: the run is
    /// evicted, the server retries the task, and the retry replays the history, so code that
    /// still returns the error fails again, and a worker whose code no longer does completes
    /// the task. Only a task's first failed attempt is reported to the server; later ones
    /// send nothing and end in a workflow task timeout before the next retry.
    ///
    /// For a listed error, sdk-core logs the warning "Failing workflow task" with the run ID
    /// and the failure through the client's logger when it reports the task's first attempt.
    /// A later attempt gets only a debug line from sdk-core, which the default
    /// `client.Options.core_log_level` (`.warn`) hides, so at the default levels a task that
    /// keeps failing on a listed error shows that one warning. The worker also logs the
    /// eviction that follows each such failed attempt at debug level through `logger`, which
    /// the default `std.log` sink shows in Debug builds. A nondeterministic run, deadlock
    /// detection and `workflow_memory_limit` log at error level on every attempt.
    ///
    /// A listed error from an update handler doesn't reject or fail the update: the retried
    /// task runs the handler again and answers it. An update delivered in the failed task
    /// was never accepted, so the server delivers it again and its validator runs again.
    /// Errors from update validators, and an update handler's unlisted errors
    /// (error.OutOfMemory included), reject the update.
    ///
    /// Listing error.Canceled doesn't stop a cancellation: the workflow function or a signal
    /// handler that returns it cancels the workflow, listed or not. An update handler that
    /// returns a listed error.Canceled fails the task, where unlisted it rejects the update.
    /// Listing error.ContinueAsNew doesn't stop a continue-as-new the workflow function
    /// requested; only its misuse (returned without `ctx.continueAsNew`, or by a signal
    /// handler) then fails the task instead of the workflow.
    workflow_task_failure_errors: []const anyerror = &.{},
    /// Fail the workflow, instead of its workflow task, when a run's code no longer matches
    /// its history (nondeterminism), for every workflow of this worker. A workflow definition
    /// can ask for it alone with `workflow.DefOptions.nondeterminism_fails_workflow`. Off
    /// (the default): the task fails and is retried until the code matches, so a fixed
    /// worker can still finish the run.
    nondeterminism_fails_workflow: bool = false,
    /// How long running activities and Nexus handlers may keep running after `stop` before
    /// they are canceled with reason `worker_shutdown`.
    graceful_shutdown_period: Io.Duration = .zero,
    /// Where workflow and activity loggers write. Null uses the client's
    /// (`client.Options.logger`). Borrowed: it must outlive the worker. Called concurrently
    /// from workflow and activity threads.
    logger: ?log_mod.Sink = null,
    /// Keep workflow loggers on while workflows replay history and while a run is evicted. By
    /// default they are silent during replay, so each message is logged once, and while a
    /// run is evicted, when its code unwinds with `error.Canceled` though nothing failed.
    log_during_replay: bool = false,
    /// Interceptors of this worker's workflows, activities and Nexus requests, after the
    /// client's (`client.Options.worker_interceptors`); the first is the outermost. Borrowed:
    /// they must outlive the worker. Called concurrently from workflow and activity threads
    /// and Nexus handler tasks.
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

/// The application failure type of a workflow task that deadlock detection failed
/// (`Options.deadlock_detection_timeout`).
pub const deadlock_failure_type = "WorkflowDeadlockDetected";

/// How many polls of one kind (workflow, activity or Nexus tasks) a worker keeps open at
/// once. Written as `.{ .simple_maximum = 4 }` or `.{ .autoscaling = .{} }`. A poll is
/// opened only while a slot for its task is free, whichever the behavior.
pub const PollerBehavior = union(enum) {
    /// Up to this many polls at once; must be positive.
    simple_maximum: usize,
    /// The worker opens and closes polls between `minimum` and `maximum`, starting from
    /// `initial`, following the server's feedback.
    ///
    /// The server's poll responses carry a suggestion to add or remove polls. The worker
    /// always follows a suggestion to remove them, down to `minimum`, and follows one to
    /// add them, up to `maximum`, only while the worker is taking in more tasks than it
    /// did a moment before (more polls would not help a worker that can't keep up). Once
    /// the server has sent a suggestion, or when the namespace reports that it supports
    /// poller autoscaling, an empty poll also removes one poll, and a poll refused for
    /// exhausted resources halves the count. Failed polls are retried with backoff.
    ///
    /// The feedback needs a server that sends it. Against one that sends no suggestions
    /// and doesn't report the capability, the worker keeps `initial` polls open.
    autoscaling: Autoscaling,

    /// Bounds of an autoscaling poller: `minimum` must be positive, `maximum` at least
    /// `minimum`, and `initial` from `minimum` to `maximum`; `run` returns
    /// `error.InvalidOptions` otherwise.
    pub const Autoscaling = struct {
        minimum: usize = 1,
        maximum: usize = 100,
        initial: usize = 5,
    };
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
    /// Nexus services by name; `run` polls Nexus tasks when there is one.
    nexus_services: std.StringHashMapUnmanaged(nexus.Registered) = .empty,
    /// The core workers while `run` is active, for `stop` on other threads: the task queue's,
    /// then with sessions, the session queue's and the host queue's.
    /// Guards `core_ptrs`, `stop_requested`, `fatal`, `fatal_message` and the start outcome
    /// (`run_generation`, `started_generation`, `start_failed`), which `stop` (any thread), the
    /// poll loops, `waitUntilRunning` and `run` share.
    stop_mutex: Io.Mutex = .init,
    core_ptrs: [3]?*bridge.c.TemporalCoreWorker = @splat(null),
    /// Set by `stop` only, and never cleared: a Worker the user stopped stays stopped.
    stop_requested: bool = false,
    /// The activities of each running activity loop, so `stop` can tell them the worker is
    /// stopping (`activity.Context.isWorkerStopping`). Guarded by `stop_mutex`.
    activity_runs: [3]?*RunningActivities = @splat(null),
    /// Set before `activity_runs` are notified, by `stop`, by a poll failure (`shutDownRun`) and
    /// by a poll loop that couldn't start (`startFailed`); an activity starting later reads it,
    /// and so does `run` while its client makes its first connection. `run` resets it to
    /// `stop_requested` when it starts, so only a user's stop carries over to the next run.
    stopping: std.atomic.Value(bool) = .init(false),
    /// Whether `run` is waiting for its client's first connection, which `stop` then wakes.
    /// Guarded by `stop_mutex`.
    waiting_for_connection: bool = false,
    /// The first poll failure of the current or last run, and its text (owned; null if it
    /// couldn't be allocated). See `fatalMessage`.
    fatal: ?PollError = null,
    fatal_message: ?[]u8 = null,
    /// The current or last run's number: `run` raises it when it starts. Guarded by
    /// `stop_mutex`. `testing.WorkflowEnvironment` also raises it, and settles it if the spawn
    /// fails, before it spawns `run` on another task, so its `waitUntilRunning` can't report
    /// the previous run; it is never lowered, and a run settles the number it ends at.
    run_generation: u64 = 0,
    /// The number of the last run that settled its start (`settleStart`): it registered the
    /// worker with its client and started polling, or it failed before getting there
    /// (`start_failed`). A run settles once, so this equals `run_generation` once the current
    /// run has settled. Guarded by `stop_mutex`; see `waitUntilRunning`.
    started_generation: u64 = 0,
    /// Whether run `started_generation` failed to start. Guarded by `stop_mutex`.
    start_failed: bool = false,
    /// Broadcast, with `stop_mutex` held, whenever `run_generation` or `started_generation`
    /// changes.
    /// `waitUntilRunning` waits on it and re-checks the generations when woken.
    start_changed: Io.Condition = .init,
    /// The client's worker interceptors, then `options.interceptors`, while `run` is active.
    interceptors: []const interceptor.Worker = &.{},
    /// The sessions this worker holds, while `run` is active with `options.sessions`.
    session_host: ?*SessionHost = null,
    /// The process's first read of the executable's debug info, when this worker started it
    /// (`warmDebugInfo`). `deinit` joins it; `run` doesn't, so that `stop` ends `run` at once.
    debug_info_read: ?Io.Future(void) = null,
    /// Set when `debug_info_read` ends and by `stop`: wakes `run`'s wait for the read. Never
    /// reset (`awaitDebugInfo`).
    debug_info_wake: Io.Event = .unset,

    pub fn init(gpa: Allocator, io: Io, client: *client_mod.Client, task_queue: []const u8, options: Options) error{OutOfMemory}!Worker {
        return .{ .gpa = gpa, .io = io, .client = client, .task_queue = try gpa.dupe(u8, task_queue), .options = options };
    }

    pub fn deinit(w: *Worker) void {
        if (w.debug_info_read) |*f| f.await(w.io);
        w.workflows.deinit(w.gpa);
        w.activities.deinit(w.gpa);
        w.nexus_services.deinit(w.gpa);
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

    /// Registers a Nexus service handler defined with `temporal.nexus.ServiceHandler`, under
    /// its service's name. Use `registerNexusServiceWith` when its handlers take a receiver.
    /// A service name already registered returns `error.AlreadyRegistered`. Call it before
    /// `run`, from the thread that owns the worker; `run` then polls the task queue's Nexus
    /// tasks and calls the handlers on the worker's Io, each request on a task of its own (see
    /// `temporal.nexus.Context` for what a handler's errors and cancellation come to).
    pub fn registerNexusService(w: *Worker, comptime Handler: type) RegisterNexusServiceError!void {
        checkNexusHandler(Handler);
        return w.registerNexusServiceImpl(Handler.registered);
    }

    /// Registers a Nexus service handler whose handlers take a receiver, bound to `receiver`.
    /// The receiver must outlive the worker and be safe to use from several requests at once.
    /// A service name already registered returns `error.AlreadyRegistered`.
    pub fn registerNexusServiceWith(w: *Worker, comptime Handler: type, receiver: *Handler.Receiver) RegisterNexusServiceError!void {
        checkNexusHandler(Handler);
        return w.registerNexusServiceImpl(Handler.with(receiver));
    }

    /// `AlreadyRegistered`: the worker already has a handler for the service's name.
    pub const RegisterNexusServiceError = error{ OutOfMemory, AlreadyRegistered };

    fn checkNexusHandler(comptime Handler: type) void {
        if (@typeInfo(Handler) != .@"struct" or !@hasDecl(Handler, "service_name") or !@hasDecl(Handler, "operations") or !@hasDecl(Handler, "Receiver"))
            @compileError("registerNexusService takes a temporal.nexus.ServiceHandler, not " ++ @typeName(Handler));
    }

    fn registerNexusServiceImpl(w: *Worker, registered: nexus.Registered) RegisterNexusServiceError!void {
        const gop = try w.nexus_services.getOrPut(w.gpa, registered.name);
        if (gop.found_existing) {
            log.warn("registering Nexus service \"{s}\" failed: the worker already has a handler for that service; register each service once", .{registered.name});
            return error.AlreadyRegistered;
        }
        gop.value_ptr.* = registered;
    }

    /// The operation `operation` of the registered Nexus service `service`, ready to start or
    /// cancel. Valid while the worker is. Safe to call from several threads once registration
    /// is done. Returns `error.ServiceNotFound` when no service of that name is registered,
    /// and `error.OperationNotFound` when the service has no operation of that name.
    pub fn nexusOperation(w: *const Worker, service: []const u8, operation: []const u8) nexus.LookupError!nexus.BoundOperation {
        const registered = w.nexus_services.getPtr(service) orelse return error.ServiceNotFound;
        return registered.find(operation) orelse error.OperationNotFound;
    }

    fn hasWorkflows(w: *const Worker) bool {
        return w.workflows.count() > 0 or w.dynamic_workflow != null;
    }

    fn hasActivities(w: *const Worker) bool {
        return w.activities.count() > 0 or w.dynamic_activity != null;
    }

    fn hasNexusServices(w: *const Worker) bool {
        return w.nexus_services.count() > 0;
    }

    const PollError = bridge.Worker.PollError;

    /// Errors from `run`:
    /// - `NothingRegistered`: the worker has no workflow, activity, or Nexus service; nothing
    ///   starts.
    /// - `InvalidOptions`: the worker's options, before anything starts (the reason is
    ///   logged), or a TLS file that a client from `client.Client.init` couldn't read when
    ///   connecting.
    /// - `ConnectFailed`: the worker's client hadn't connected yet (`client.Client.init`) and
    ///   couldn't, or the calling task was canceled while it connected (the cancel request
    ///   is kept); the reason is logged, and the client tries again on its next call.
    /// - `WorkerInitFailed`, `ValidationFailed`: the core rejected the worker or the server
    ///   rejected its namespace or task queue (the core's message is logged).
    /// - `ConcurrencyUnavailable`: the worker's Io couldn't start a poll loop.
    /// - `PollFailed`: the core gave up polling for tasks; `fatalMessage` has its text.
    /// - `ShutdownFailed`: finalizing a core worker failed (logged).
    /// - `OutOfMemory`.
    pub const RunError = error{ OutOfMemory, NothingRegistered, WorkerInitFailed, InvalidOptions, ConnectFailed, ValidationFailed, ShutdownFailed, ConcurrencyUnavailable, PollFailed };

    /// Polls and runs tasks until `stop` is called, or until polling fails for good: workflow
    /// tasks when a workflow is registered, activity tasks when an activity or a workflow is
    /// (workflows' side effects run as local activities; with `poll_activity_tasks` false,
    /// only local ones), and Nexus tasks when a Nexus service is. Returns after in-flight
    /// work finishes and every core worker is shut down:
    /// - success after `stop` (or a signal, with `runUntilSignal`), at once if the worker's
    ///   client (from `client.Client.init`) hasn't connected yet: `run` stops waiting for the
    ///   connection, and nothing has started (`waitUntilRunning` returns
    ///   `error.WorkerFailedToStart`);
    /// - `error.PollFailed` (or `error.OutOfMemory`) when the core gave up polling, after
    ///   shutting down its core workers (this is not a `stop`: `run` can be called again);
    ///   `fatalMessage` has the task queue and the core's text;
    /// - an error from starting the worker or from shutting it down. A loop that couldn't
    ///   start is returned before a poll failure, and a poll failure before a failed shutdown.
    ///
    /// A Worker can run again after `run` returned, including after `error.PollFailed` or an
    /// error from starting. Only one `run` of a Worker may be active at a time.
    ///
    /// A Worker that `stop` was called on stays stopped. Every later `run` returns without
    /// error once it has shut down the cores it created, and it polls nothing (`waitUntilRunning`
    /// reports it as failed to start). To poll again, make a new Worker.
    ///
    /// `waitUntilRunning` reports this run while it is active and until the next `run` begins;
    /// a waiter blocked across a run boundary reports the run it sees when it is woken (see
    /// `waitUntilRunning`). A run begins only when its task enters `run`, so on a reused Worker,
    /// `waitUntilRunning` called right after `io.concurrent(run)` may report the previous
    /// run's outcome, such as its failed start. To retry a start that failed, make a new
    /// Worker and run that.
    pub fn run(w: *Worker) RunError!void {
        {
            w.stop_mutex.lockUncancelable(w.io);
            defer w.stop_mutex.unlock(w.io);
            if (w.fatal_message) |m| w.gpa.free(m);
            w.fatal_message = null;
            w.fatal = null;
            // The last run's poll failure or failed loop start shut that run down; it doesn't
            // stop this one. Everything that run started has ended, and runs don't overlap. A
            // `stop` racing with this takes the same mutex, so it is kept either way.
            w.stopping.store(w.stop_requested, .release);
            // A new run: `waitUntilRunning` now waits for this one to settle.
            w.run_generation += 1;
            w.start_changed.broadcast(w.io);
        }
        var running = false;
        defer if (!running) w.settleStart(true);
        if (!w.hasWorkflows() and !w.hasActivities() and !w.hasNexusServices()) return error.NothingRegistered;
        const registered: Registered = .{ .workflows = w.hasWorkflows(), .activities = w.hasActivities(), .nexus = w.hasNexusServices() };
        try checkOptions(w.options, w.task_queue, registered);
        const strategy = try w.versioningStrategy();
        // Read while the worker connects and creates its core; `awaitDebugInfo` waits for it
        // before the worker polls, so no activity fails before it is done.
        if (w.readsDebugInfo() and w.debug_info_read == null and !debug_info_warm.load(.acquire))
            w.debug_info_read = io_tasks.concurrent(w.io, warmDebugInfo, .{w}) catch null;
        var core_options = coreOptions(w.options, w.client.namespace, w.task_queue, strategy, registered);
        const nondeterminism_types = try w.nondeterminismFailTypes();
        defer w.gpa.free(nondeterminism_types);
        core_options.nondeterminism_as_workflow_fail_for_types = nondeterminism_types;
        const interceptors = try std.mem.concat(w.gpa, interceptor.Worker, &.{ w.client.worker_interceptors, w.options.interceptors });
        defer w.gpa.free(interceptors);
        w.interceptors = interceptors;
        defer w.interceptors = &.{};
        var message: ?[]u8 = null;
        defer if (message) |m| w.gpa.free(m);

        // A client from `Client.init` connects here if it hasn't yet, and logs the reason of a
        // failure. `stop` ends the wait: without a connection there is nothing to shut down.
        {
            w.stop_mutex.lockUncancelable(w.io);
            defer w.stop_mutex.unlock(w.io);
            w.waiting_for_connection = true;
        }
        const connected = w.client.currentConnectionUnless(&w.stopping);
        {
            w.stop_mutex.lockUncancelable(w.io);
            defer w.stop_mutex.unlock(w.io);
            w.waiting_for_connection = false;
        }
        const created_on = connected catch |err| {
            if (err == error.Stopped) {
                log.info("Worker stopped before its client connected TaskQueue={s}", .{w.task_queue});
                return;
            }
            log.warn("starting the worker for task queue {s} failed: the client couldn't connect ({t})", .{ w.task_queue, err });
            return switch (err) {
                error.OutOfMemory => error.OutOfMemory,
                error.InvalidOptions => error.InvalidOptions,
                error.RuntimeInitFailed, error.ConnectFailed => error.ConnectFailed,
                error.Stopped => unreachable,
                error.Canceled => {
                    // `run` can't return Canceled: keep the request for the caller's next
                    // cancelation point.
                    w.io.recancel();
                    return error.ConnectFailed;
                },
            };
        };
        // Set once the finalizing below has run. A return before it frees cores that never
        // polled: they are shut down first, because `bridge.Runtime.release` may keep a freed
        // core worker, and one not shut down stays registered with the client.
        var finalized = false;
        const core = try w.createCore(created_on, core_options, &message);
        defer if (finalized) core.deinit() else w.freeUnpolledCore(core, w.task_queue, core_options.task_types);

        // Declared before the session cores, so it is freed after them: their task queues
        // are its strings, which `freeUnpolledCore` logs.
        var host: ?SessionHost = null;
        defer if (host) |*h| h.deinit();
        // Polled even without registered activities: a workflow's side effects run as local
        // activities.
        var activity_cores: [3]ActivityCore = undefined;
        activity_cores[0] = .{ .core = core, .task_queue = w.task_queue };
        var core_count: usize = 1;
        defer for (activity_cores[1..core_count]) |c| if (finalized) c.core.deinit() else w.freeUnpolledCore(c.core, c.task_queue, session_task_types);
        if (w.options.sessions) |o| {
            host = try SessionHost.init(w, o.max_concurrent_sessions);
            for ([_][]const u8{ host.?.creation_queue, host.?.host_queue }, sessionSlots(w.options.max_concurrent_activity_executions, o.max_concurrent_sessions)) |queue, slots| {
                activity_cores[core_count] = .{ .task_queue = queue, .core = try w.createCore(created_on, sessionCoreOptions(core_options, queue, slots), &message) };
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
        // Stopped first, the worker has started nothing (`waitUntilRunning` says so), but
        // its loops still run: a core is finalized only after each of its pollers has seen
        // the shutdown `stop` began, and one finalized without ever being polled for
        // activities never finishes. They return at once.
        // `awaitDebugInfo` checks `stopping` only while it waits for the read: check it here too,
        // so a run after `stop` never reports that it started.
        if (w.awaitDebugInfo() and !w.stopping.load(.acquire)) {
            log.info("Started Worker Namespace={s} TaskQueue={s}", .{ w.client.namespace, w.task_queue });
            if (host) |h| log.info("Worker hosts sessions TaskQueue={s} SessionTaskQueue={s} HostTaskQueue={s}", .{ w.task_queue, h.creation_queue, h.host_queue });
            running = true;
            w.settleStart(false);
        } else log.info("Worker stopped before it polled TaskQueue={s}", .{w.task_queue});

        var workflows_done = if (w.hasWorkflows()) try io_tasks.concurrent(w.io, workflowLoop, .{ w, core }) else null;
        // Once a loop polls a core, returning early would free the core worker under it: shut
        // down like a normal stop, drain every poller (finalizing waits for each to return
        // null), finalize, then return the error.
        var start_failed: ?RunError = null;
        var polling = workflows_done != null;
        var nexus_done: ?Io.Future(void) = null;
        if (w.hasNexusServices()) {
            if (io_tasks.concurrent(w.io, nexusLoop, .{ w, core })) |f| {
                nexus_done = f;
                polling = true;
            } else |err| {
                if (!polling) return err;
                w.startFailed(cores, "the Nexus loop", w.task_queue, err);
                start_failed = err;
                nexusLoop(w, core);
            }
        }
        // The task queue's own core polls activities only when it runs them, local ones
        // included; the session queues' always do.
        const first_activity_core: usize = if (core_options.task_types.local_activities or core_options.task_types.remote_activities) 0 else 1;
        var activity_loops: [3]?Io.Future(void) = @splat(null);
        for (cores[first_activity_core..], activity_loops[first_activity_core..core_count]) |c, *loop| {
            if (start_failed == null) {
                if (io_tasks.concurrent(w.io, activityLoop, .{ w, c.core, c.task_queue })) |f| {
                    loop.* = f;
                    polling = true;
                    continue;
                } else |err| {
                    if (!polling) return err;
                    w.startFailed(cores, "the activity loop", c.task_queue, err);
                    start_failed = err;
                }
            }
            activityLoop(w, c.core, c.task_queue);
        }
        if (workflows_done) |*f| f.await(w.io);
        if (nexus_done) |*f| f.await(w.io);
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
        finalized = true;
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

    /// Records how the current run's start ended, for `waitUntilRunning`, and wakes its waiters.
    /// Called once per run: when it starts polling, or when it returns without having done so.
    fn settleStart(w: *Worker, failed: bool) void {
        w.stop_mutex.lockUncancelable(w.io);
        defer w.stop_mutex.unlock(w.io);
        w.started_generation = w.run_generation;
        w.start_failed = failed;
        w.start_changed.broadcast(w.io);
    }

    /// Whether this worker puts stack traces in failures, which read the executable's debug
    /// info (`warmDebugInfo`).
    fn readsDebugInfo(w: *const Worker) bool {
        return w.options.failure_stack_traces and @errorReturnTrace() != null;
    }

    /// Waits until the executable's debug info is read (`warmDebugInfo`), or until `stop`;
    /// false when stopped first. `run` calls it before it polls, so no failed activity's
    /// completion waits for the read (bead temporal-sdk-zig-d51.1.34), and `stop` still ends
    /// `run` at once. Reads it here when it couldn't start beside the worker.
    fn awaitDebugInfo(w: *Worker) bool {
        if (!w.readsDebugInfo()) return true;
        if (w.debug_info_read == null and !debug_info_warm.load(.acquire)) warmDebugInfo(w);
        // Never reset: both conditions only turn true, and each is stored before the event is
        // set (`warmDebugInfo`, `stop`), so a set event means one of them holds. A reset here
        // could be ordered after a set and lose it (store buffering on x86-TSO).
        while (true) {
            if (debug_info_warm.load(.acquire)) return true;
            if (w.stopping.load(.acquire)) return false;
            w.debug_info_wake.waitUncancelable(w.io);
        }
    }

    /// Reports a poll loop that couldn't start while others already poll, and shuts every
    /// core down so they drain.
    fn startFailed(w: *Worker, cores: []const ActivityCore, comptime what: []const u8, task_queue: []const u8, err: RunError) void {
        log.warn("starting " ++ what ++ " for task queue {s} failed, stopping the worker: {t}", .{ task_queue, err });
        w.stop_mutex.lockUncancelable(w.io);
        defer w.stop_mutex.unlock(w.io);
        w.notifyActivitiesOfStop();
        for (cores) |other| other.core.initiateShutdown();
    }

    /// The activity slots of the session queue and of the host queue, which runs session
    /// activities beside the main queue's own pool. Saturates, so "unlimited" counts can't
    /// overflow; the bridge lowers them to what the core takes.
    fn sessionSlots(max_activities: usize, max_sessions: usize) [2]usize {
        return .{ max_sessions, max_activities +| max_sessions };
    }

    /// What a worker has registered, which decides the task types its core worker polls.
    const Registered = struct { workflows: bool, activities: bool, nexus: bool };

    /// Returns `error.InvalidOptions`, logging the option and why, for options the core
    /// would reject or misread, or that contradict each other. `coreOptions` relies on it.
    fn checkOptions(o: Options, task_queue: []const u8, registered: Registered) error{InvalidOptions}!void {
        // A coroutine's arguments and header go at the top of its stack; a stack too small
        // for them would be written outside its mapping.
        if (o.workflow_stack_size < min_workflow_stack_size) {
            log.warn("worker for task queue {s}: workflow_stack_size is {d}; it must be at least {d} (worker.min_workflow_stack_size)", .{ task_queue, o.workflow_stack_size, min_workflow_stack_size });
            return error.InvalidOptions;
        }
        if (Scheduler.stackMappingLen(o.workflow_stack_size) == null) {
            log.warn("worker for task queue {s}: workflow_stack_size is {d}; it is too large to round up to whole pages plus a guard page", .{ task_queue, o.workflow_stack_size });
            return error.InvalidOptions;
        }
        if (o.sessions) |s| {
            if (s.max_concurrent_sessions == 0) {
                log.warn("worker for task queue {s}: sessions.max_concurrent_sessions must be positive", .{task_queue});
                return error.InvalidOptions;
            }
            if (!o.poll_activity_tasks) {
                log.warn("worker for task queue {s}: poll_activity_tasks = false can't be combined with sessions, whose activities arrive through task queues", .{task_queue});
                return error.InvalidOptions;
            }
        }
        if (!o.poll_activity_tasks and registered.activities and !registered.workflows) {
            log.warn("worker for task queue {s}: poll_activity_tasks = false runs activities only as local activities of this worker's workflows, and no workflow is registered", .{task_queue});
            return error.InvalidOptions;
        }
        inline for (.{ "max_activities_per_second", "max_task_queue_activities_per_second" }) |name| {
            if (@field(o, name)) |rate| if (!(rate > 0) or !std.math.isNormal(rate)) {
                log.warn("worker for task queue {s}: " ++ name ++ " is {d}; it must be a positive, finite number, or null for no limit", .{ task_queue, rate });
                return error.InvalidOptions;
            };
        }
        // The core spaces this worker's polls 1/rate seconds apart, computing up to two
        // spacings past now on a monotonic instant of signed 64-bit seconds since boot; an
        // overflow panics its activity poller, and no activity ever starts. A spacing below
        // 2^61 seconds keeps now + 2/rate under 2^62 seconds plus the uptime.
        if (o.max_activities_per_second) |rate| if (1 / rate >= max_poll_spacing_seconds) {
            log.warn("worker for task queue {s}: max_activities_per_second is {e}; it must be at least one activity per 2^61 seconds (about 4.4e-19 per second), or null for no limit", .{ task_queue, rate });
            return error.InvalidOptions;
        };
        inline for (.{ "sticky_schedule_to_start_timeout", "default_heartbeat_throttle_interval", "max_heartbeat_throttle_interval" }) |name| {
            const d: Io.Duration = @field(o, name);
            if (d.nanoseconds < 0) {
                log.warn("worker for task queue {s}: " ++ name ++ " is {f}; it must not be negative", .{ task_queue, d });
                return error.InvalidOptions;
            }
        }
        if (o.deadlock_detection_timeout) |d| if (d.nanoseconds <= 0) {
            log.warn("worker for task queue {s}: deadlock_detection_timeout is {f}; it must be positive, or null to turn deadlock detection off", .{ task_queue, d });
            return error.InvalidOptions;
        };
        if (o.default_heartbeat_throttle_interval.nanoseconds > o.max_heartbeat_throttle_interval.nanoseconds) {
            log.warn("worker for task queue {s}: default_heartbeat_throttle_interval ({f}) must not be above max_heartbeat_throttle_interval ({f})", .{ task_queue, o.default_heartbeat_throttle_interval, o.max_heartbeat_throttle_interval });
            return error.InvalidOptions;
        }
        inline for (.{ "workflow_task_poller_behavior", "activity_task_poller_behavior", "nexus_task_poller_behavior" }) |name| {
            if (@field(o, name)) |behavior| try checkPollerBehavior(behavior, name, task_queue);
        }
        const ratio = o.nonsticky_to_sticky_poll_ratio;
        if (!(ratio >= 0 and ratio <= 1)) {
            log.warn("worker for task queue {s}: nonsticky_to_sticky_poll_ratio is {d}; it must be from 0 to 1", .{ task_queue, ratio });
            return error.InvalidOptions;
        }
    }

    fn checkPollerBehavior(behavior: PollerBehavior, comptime name: []const u8, task_queue: []const u8) error{InvalidOptions}!void {
        switch (behavior) {
            .simple_maximum => |m| if (m == 0) {
                log.warn("worker for task queue {s}: " ++ name ++ ".simple_maximum is 0; it must be positive", .{task_queue});
                return error.InvalidOptions;
            },
            .autoscaling => |a| {
                if (a.minimum == 0) {
                    log.warn("worker for task queue {s}: " ++ name ++ ".autoscaling.minimum is 0; it must be positive", .{task_queue});
                    return error.InvalidOptions;
                }
                if (a.maximum < a.minimum) {
                    log.warn("worker for task queue {s}: " ++ name ++ ".autoscaling.maximum is {d}; it must be at least minimum ({d})", .{ task_queue, a.maximum, a.minimum });
                    return error.InvalidOptions;
                }
                if (a.initial < a.minimum or a.initial > a.maximum) {
                    log.warn("worker for task queue {s}: " ++ name ++ ".autoscaling.initial is {d}; it must be from minimum ({d}) to maximum ({d})", .{ task_queue, a.initial, a.minimum, a.maximum });
                    return error.InvalidOptions;
                }
            },
        }
    }

    /// The longest poll spacing, 1 / `max_activities_per_second`, that `checkOptions` accepts.
    const max_poll_spacing_seconds: f64 = 0x1p61;

    /// The core options of the worker's own task queue, from options `checkOptions` accepted.
    fn coreOptions(o: Options, namespace: []const u8, task_queue: []const u8, versioning: bridge.Worker.Options.Versioning, registered: Registered) bridge.Worker.Options {
        return .{
            .namespace = namespace,
            .task_queue = task_queue,
            .versioning = versioning,
            .identity_override = o.identity,
            .max_cached_workflows = o.max_cached_workflows,
            .tuner = .{
                .workflow = .{ .fixed = o.max_concurrent_workflow_task_executions },
                .activity = .{ .fixed = o.max_concurrent_activity_executions },
                .local_activity = .{ .fixed = o.max_concurrent_local_activity_executions },
                .nexus = .{ .fixed = o.max_concurrent_nexus_task_executions },
            },
            .task_types = .{
                .workflows = registered.workflows,
                .remote_activities = registered.activities and o.poll_activity_tasks,
                // The core runs local activities only on a worker that also polls workflows.
                // Every workflow worker runs them: side effects are recorded by one.
                .local_activities = registered.workflows,
                .nexus = registered.nexus,
            },
            .sticky_queue_schedule_to_start_timeout_ms = millis(o.sticky_schedule_to_start_timeout),
            .default_heartbeat_throttle_interval_ms = millis(o.default_heartbeat_throttle_interval),
            .max_heartbeat_throttle_interval_ms = millis(o.max_heartbeat_throttle_interval),
            // The core reads 0 as no limit.
            .max_activities_per_second = o.max_activities_per_second orelse 0,
            .max_task_queue_activities_per_second = o.max_task_queue_activities_per_second orelse 0,
            // Eagerly started activities skip the core's poll rate limiter, so a per-worker
            // limit turns eager execution off. The core does that itself for the task queue's.
            .max_eager_activity_reservations_per_workflow_task = if (o.max_activities_per_second == null) o.max_eager_activity_reservations_per_workflow_task else 0,
            .workflow_task_pollers = corePollerBehavior(o.workflow_task_poller_behavior, o.max_concurrent_workflow_task_pollers),
            .nonsticky_to_sticky_poll_ratio = o.nonsticky_to_sticky_poll_ratio,
            .activity_task_pollers = corePollerBehavior(o.activity_task_poller_behavior, o.max_concurrent_activity_task_pollers),
            // The core checks this even on a worker that polls no Nexus tasks.
            .nexus_task_pollers = if (registered.nexus) corePollerBehavior(o.nexus_task_poller_behavior, o.max_concurrent_nexus_task_pollers) else .{ .simple_maximum = 1 },
            .graceful_shutdown_period_ms = millis(o.graceful_shutdown_period),
            .nondeterminism_as_workflow_fail = o.nondeterminism_fails_workflow,
        };
    }

    /// `behavior` when set, else a simple maximum of the legacy `max_concurrent_*` field.
    fn corePollerBehavior(behavior: ?PollerBehavior, legacy_maximum: usize) bridge.Worker.Options.PollerBehavior {
        return switch (behavior orelse return .{ .simple_maximum = legacy_maximum }) {
            .simple_maximum => |m| .{ .simple_maximum = m },
            .autoscaling => |a| .{ .autoscaling = .{ .minimum = a.minimum, .maximum = a.maximum, .initial = a.initial } },
        };
    }

    /// The registered workflow types whose definitions set `nondeterminism_fails_workflow`,
    /// sorted. Allocated in `gpa`; the names are the definitions' own.
    fn nondeterminismFailTypes(w: *const Worker) error{OutOfMemory}![]const []const u8 {
        var types: std.ArrayList([]const u8) = .empty;
        errdefer types.deinit(w.gpa);
        var it = w.workflows.iterator();
        while (it.next()) |kv| if (kv.value_ptr.nondeterminism_fails_workflow) try types.append(w.gpa, kv.value_ptr.name);
        std.mem.sort([]const u8, types.items, {}, struct {
            fn lessThan(_: void, x: []const u8, y: []const u8) bool {
                return std.mem.lessThan(u8, x, y);
            }
        }.lessThan);
        return types.toOwnedSlice(w.gpa);
    }

    /// The core options of a session queue: activities only, unversioned so a rollout can't
    /// strand a host's sessions, with `slots` activity slots. The heartbeat throttling of the
    /// worker's own queue applies; its per-second limits don't.
    /// A session queue's core polls only remote activities.
    const session_task_types: bridge.Worker.Options.TaskTypes = .{ .workflows = false, .remote_activities = true, .local_activities = false, .nexus = false };

    fn sessionCoreOptions(main: bridge.Worker.Options, task_queue: []const u8, slots: usize) bridge.Worker.Options {
        return .{
            .namespace = main.namespace,
            .task_queue = task_queue,
            .identity_override = main.identity_override,
            .max_cached_workflows = main.max_cached_workflows,
            .tuner = .{ .activity = .{ .fixed = slots } },
            .task_types = session_task_types,
            .default_heartbeat_throttle_interval_ms = main.default_heartbeat_throttle_interval_ms,
            .max_heartbeat_throttle_interval_ms = main.max_heartbeat_throttle_interval_ms,
            .activity_task_pollers = main.activity_task_pollers,
            .graceful_shutdown_period_ms = main.graceful_shutdown_period_ms,
        };
    }

    /// Whole milliseconds of `d`; negative is 0 and too long saturates.
    fn millis(d: Io.Duration) u64 {
        if (d.nanoseconds <= 0) return 0;
        return std.math.cast(u64, @divTrunc(d.nanoseconds, std.time.ns_per_ms)) orelse std.math.maxInt(u64);
    }

    /// Creates and validates a core worker; logs and returns the error on failure.
    fn createCore(w: *Worker, connection: bridge.Connection, options: bridge.Worker.Options, message: *?[]u8) RunError!bridge.Worker {
        const core = bridge.Worker.init(w.gpa, connection, options, message) catch |err| {
            log.err("creating the worker for task queue {s} failed: {s}", .{ options.task_queue, message.* orelse @errorName(err) });
            return err;
        };
        errdefer w.freeUnpolledCore(core, options.task_queue, options.task_types);
        core.validate(w.gpa, w.io, message) catch |err| {
            log.err("worker validation for task queue {s} failed: {s}", .{ options.task_queue, message.* orelse @errorName(err) });
            return err;
        };
        return core;
    }

    /// Shuts down and frees a core worker nothing polls, nor will. As for any core worker,
    /// each task type it polls (`task_types`) is polled until it returns null before
    /// finalizing: a core polling activities otherwise waits in `finalizeShutdown` forever
    /// for its activity poller to see the shutdown. The polls go through `pollCore`, so one
    /// whose cell `w.gpa` refuses is followed by one that reaches the core.
    fn freeUnpolledCore(w: *Worker, core: bridge.Worker, task_queue: []const u8, task_types: bridge.Worker.Options.TaskTypes) void {
        core.initiateShutdown();
        var message: ?[]u8 = null;
        defer if (message) |m| w.gpa.free(m);
        // Nothing was polled, so no task is outstanding; a poll returns null, or fails.
        const polls = [_]struct { bool, PollTask }{
            .{ task_types.workflows, bridge.Worker.pollWorkflowActivation },
            .{ task_types.local_activities or task_types.remote_activities, bridge.Worker.pollActivityTask },
            .{ task_types.nexus, bridge.Worker.pollNexusTask },
        };
        var gpa_refused = false;
        for (polls) |p| if (p[0]) while (true) {
            const refused_before = gpa_refused;
            const polled = w.pollCore(core, p[1], null, &gpa_refused) catch |err| switch (err) {
                // `w.gpa` refused the cell before the poll reached the core: the next poll
                // takes its cell elsewhere. A refusal after that one, or a failed poll, ends
                // the polls as before.
                error.OutOfMemory => if (refused_before) break else continue,
                error.PollFailed => break,
            } orelse break;
            polled.deinit();
        };
        core.finalizeShutdown(w.gpa, w.io, &message) catch |err| {
            log.warn("shutting down the unstarted worker for task queue {s} failed: {s}", .{ task_queue, message orelse @errorName(err) });
        };
        core.deinit();
    }

    const PollTask = *const fn (bridge.Worker, Allocator, Io, ?*?[]u8) PollError!?bridge.Worker.Polled;

    /// Polls `core` for its next task with `poll`. The poll's cell comes from `w.gpa` until
    /// `w.gpa` refuses one: that poll returns `error.OutOfMemory` and sets `gpa_refused`, and
    /// every later one takes its cell from `std.heap.page_allocator`, without `message`
    /// (which belongs to `w.gpa`). A core worker is finalized only once each of its pollers
    /// has polled it to null, so a poll loop, or `freeUnpolledCore`, must still reach the
    /// core after `w.gpa` has run out, or `run` never returns. A poll loop reports the
    /// refusal as a poll failure, which shuts the run down, so its later polls only drain
    /// the core; `freeUnpolledCore`, draining a core already shut down, polls again.
    fn pollCore(w: *Worker, core: bridge.Worker, poll: PollTask, message: ?*?[]u8, gpa_refused: *bool) PollError!?bridge.Worker.Polled {
        if (gpa_refused.*) return poll(core, std.heap.page_allocator, w.io, null);
        return poll(core, w.gpa, w.io, message) catch |err| {
            if (err == error.OutOfMemory) gpa_refused.* = true;
            return err;
        };
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
    /// has passed. Safe to call from any thread, before or during `run`. The stop is
    /// permanent: `run` called afterwards returns at once (see `run`). A poll failure does not
    /// count as a stop.
    pub fn stop(w: *Worker) void {
        w.stop_mutex.lockUncancelable(w.io);
        defer w.stop_mutex.unlock(w.io);
        w.stop_requested = true;
        // Sets `stopping`, which a wait for the client's first connection or for the debug
        // info (`awaitDebugInfo`) checks once woken.
        w.shutDownLocked();
        w.debug_info_wake.set(w.io);
        if (w.waiting_for_connection) w.client.wakeConnecting();
    }

    /// Shuts the current run's core workers down and tells its activities the worker is
    /// stopping. Called with `stop_mutex` held.
    fn shutDownLocked(w: *Worker) void {
        w.notifyActivitiesOfStop();
        for (w.core_ptrs) |p| if (p) |ptr| bridge.c.temporal_core_worker_initiate_shutdown(ptr);
    }

    /// Ends the current run without stopping the Worker: a later `run` polls again. Sets
    /// neither `stop_requested` nor `debug_info_wake` (with `stopping` reset by the next run,
    /// a set event would make `awaitDebugInfo` spin); it runs only once polling has started,
    /// after `awaitDebugInfo` returned.
    fn shutDownRun(w: *Worker) void {
        w.stop_mutex.lockUncancelable(w.io);
        defer w.stop_mutex.unlock(w.io);
        w.shutDownLocked();
    }

    /// Tells every running activity, and those that start later, that the worker is
    /// stopping. Called with `stop_mutex` held.
    fn notifyActivitiesOfStop(w: *Worker) void {
        // Before the runs are visited: `receiveActivityTask` reads it after adding a run, under
        // the same mutex, so each run is notified by one side or the other.
        w.stopping.store(true, .release);
        for (w.activity_runs) |r| if (r) |running| {
            running.mutex.lockUncancelable(w.io);
            defer running.mutex.unlock(w.io);
            var it = running.map.valueIterator();
            while (it.next()) |activity_run| activity_run.*.ctx.notifyWorkerStop();
        };
    }

    /// The first poll failure of the last `run`, as `polling for <workflow|activity|nexus> tasks on
    /// task queue <queue> failed: <the core's message>`, or null if that run had none (or its
    /// text couldn't be allocated). Owned by the worker: valid until the next `run` starts or
    /// `deinit`. Safe to call from any thread.
    pub fn fatalMessage(w: *Worker) ?[]const u8 {
        w.stop_mutex.lockUncancelable(w.io);
        defer w.stop_mutex.unlock(w.io);
        return w.fatal_message;
    }

    const PollKind = enum { workflow, activity, nexus };

    /// Reports a failed poll from a poll loop: the core has already retried what it considers
    /// transient, so the worker logs it, records it for `run` to return, and shuts the run down.
    fn pollFailed(w: *Worker, kind: PollKind, task_queue: []const u8, err: PollError, detail: ?[]const u8) void {
        log.err("polling for {t} tasks on task queue {s} failed: {s}; stopping the worker", .{ kind, task_queue, detail orelse @errorName(err) });
        w.recordPollFailure(kind, task_queue, err, detail);
    }

    /// `pollFailed` without the log line (the test runner fails any test that logs an error):
    /// keeps the run's first failure and its text, and shuts the run's core workers down
    /// (`shutDownRun`; not a `stop`, so the Worker can run again).
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
        w.shutDownRun();
    }

    /// Waits until `run` (on another task) has registered the worker with its client and
    /// started polling, for example before an eager workflow start, which only reaches a worker
    /// the client already knows. Returns `error.WorkerFailedToStart` if `run` returned before
    /// getting there (its error says why).
    ///
    /// It reports the run that is active when it is called, or else the most recent run;
    /// before any run, it waits for the first. So once a run has returned, it reports that run
    /// (a failed start, even when an earlier run polled) until the next `run` begins, and then
    /// waits for the new one. A waiter blocked when one run ends and the next begins reports
    /// whichever run it sees once it is woken: the run that ended if it was woken before the
    /// next began, otherwise the new one. A run after `stop` polls nothing, so it reports
    /// `error.WorkerFailedToStart`.
    ///
    /// On a reused Worker, calling it right after `io.concurrent(run)` may report the previous
    /// run's outcome: the new task may not have entered `run` yet. To retry a start that
    /// failed, make a new Worker, start its `run`, and wait on that.
    pub fn waitUntilRunning(w: *Worker) error{ Canceled, WorkerFailedToStart }!void {
        w.stop_mutex.lockUncancelable(w.io);
        defer w.stop_mutex.unlock(w.io);
        // `wait` relocks the mutex before it returns, canceled or not.
        while (w.started_generation < @max(w.run_generation, 1)) try w.start_changed.wait(w.io, &w.stop_mutex);
        if (w.start_failed) return error.WorkerFailedToStart;
    }

    /// The sink this worker's workflow and activity loggers write to.
    fn logSink(w: *const Worker) log_mod.Sink {
        return w.options.logger orelse w.client.logger;
    }

    /// A logger with no context fields on `logSink`, for the worker's own records.
    fn logger(w: *const Worker) log_mod.Logger {
        return .{ .sink = w.logSink() };
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
        .deadlock_timeout = w.options.deadlock_detection_timeout,
        .failure_stack_traces = w.options.failure_stack_traces,
        .task_failure_errors = w.options.workflow_task_failure_errors,
        .codecs = w.client.codecs,
        .encode_failure_attributes = w.client.encode_failure_attributes,
        .failure_converter = w.client.failure_converter,
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
    var gpa_refused = false;
    while (true) {
        const polled = w.pollCore(core, bridge.Worker.pollWorkflowActivation, &message, &gpa_refused) catch |err| {
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
    /// See `Options.deadlock_detection_timeout`. Null: workflow code isn't watched.
    deadlock_timeout: ?Io.Duration = null,
    /// See `Options.failure_stack_traces`.
    failure_stack_traces: bool = true,
    /// Borrowed; see `Options.workflow_task_failure_errors`.
    task_failure_errors: []const anyerror = &.{},
    /// Borrowed; see `client.Options.codecs`.
    codecs: []const converter.PayloadCodec = &.{},
    /// See `client.Options.encode_failure_attributes`.
    encode_failure_attributes: bool = false,
    /// Borrowed; see `client.Options.failure_converter`.
    failure_converter: ?converter.FailureConverter = null,
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
        /// The deadlock watchdog already answered the activation; its code's results are
        /// dropped.
        discarded,
    };

    /// Where the deadlock watchdog sends a stuck activation's response, from the watchdog's
    /// thread while the activation's thread is still in workflow code. `response` and its
    /// bytes are valid only during the call.
    pub const Sender = struct {
        ctx: *anyopaque,
        send: *const fn (ctx: *anyopaque, run_id: []const u8, response: Response) void,
    };

    /// Sends the watchdog's response to a core worker, as `process` sends its own.
    const CoreSender = struct {
        r: *WorkflowRunner,
        core: bridge.Worker,
        io: Io,

        fn sender(c: *CoreSender) Sender {
            return .{ .ctx = c, .send = send };
        }

        fn send(ctx: *anyopaque, run_id: []const u8, response: Response) void {
            const c: *CoreSender = @ptrCast(@alignCast(ctx));
            var fallback_buf: [fallback_buffer_size]u8 = undefined;
            c.r.deliver(c.core, c.io, run_id, response, &fallback_buf);
        }
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
        var to_core: CoreSender = .{ .r = r, .core = core, .io = io };
        const response = r.respond(bytes, arena_state.allocator(), io, &fallback_buf, to_core.sender());
        r.deliver(core, io, runId(bytes), response, &fallback_buf);
    }

    /// Sends `first` for run `run_id` to `core`, falling back to a failed completion and then
    /// to an eviction request when the core rejects it.
    fn deliver(r: *WorkflowRunner, core: bridge.Worker, io: Io, run_id: ?[]const u8, first: Response, fallback_buf: []u8) void {
        var response = first;
        while (true) switch (response) {
            .completion => |c| {
                var message: ?[]u8 = null;
                defer if (message) |m| r.gpa.free(m);
                r.sendCompletion(core, io, c.bytes, &message) catch |err| {
                    r.logger.log(.err, switch (err) {
                        error.CompletionFailed => "the core rejected a workflow activation completion",
                        error.OutOfMemory => "a workflow activation completion could not be sent to the core: out of memory",
                    }, &.{
                        .{ .key = "run_id", .value = run_id orelse "" },
                        .{ .key = "error", .value = message orelse @errorName(err) },
                    });
                    // An eviction has nothing to fall back to: a failed completion or an
                    // eviction request for a run that is being evicted changes nothing.
                    if (c.eviction) return;
                    if (c.fallback) {
                        response = .{ .evict = .{ .run_id = run_id.?, .reason = "the core rejected the fallback failed completion" } };
                        continue;
                    }
                    response = fallbackCompletion(run_id, @errorName(err), fallback_buf);
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
            .discarded => return,
        };
    }

    /// Sends `completion` to the core. The bridge allocates the cell it waits in before the
    /// core sees the completion, so `error.OutOfMemory` means the core never got it; it is sent
    /// again with the cell on the stack, which is safe because completions are always waited
    /// for and the cell freed before the bridge returns. The core's rejection text is dropped
    /// on that second try, since it would come from the stack buffer too.
    fn sendCompletion(r: *WorkflowRunner, core: bridge.Worker, io: Io, completion: []const u8, message: *?[]u8) bridge.Worker.CompleteError!void {
        core.completeWorkflowActivation(r.gpa, io, completion, message) catch |err| switch (err) {
            error.CompletionFailed => return err,
            error.OutOfMemory => {
                var cell: [256]u8 = undefined;
                var fba: std.heap.FixedBufferAllocator = .init(&cell);
                return core.completeWorkflowActivation(fba.allocator(), io, completion, null);
            },
        };
    }

    /// Decides what to send the core for `bytes`, without sending it. Falls back to a failed
    /// completion built in `fallback_buf` when the activation can't be handled.
    /// `sender` takes the watchdog's response when the activation's workflow code gets stuck.
    fn respond(r: *WorkflowRunner, bytes: []const u8, a: Allocator, io: Io, fallback_buf: []u8, sender: ?Sender) Response {
        const run_id = runId(bytes);
        var handled = r.handleWatched(bytes, a, io, sender) catch |err| {
            r.logger.log(.err, "processing a workflow activation failed", &.{
                .{ .key = "run_id", .value = run_id orelse "" },
                .{ .key = "error", .value = @errorName(err) },
            });
            return fallbackCompletion(run_id, @errorName(err), fallback_buf);
        };
        if (handled.discarded) return .discarded;
        // A task failure `handle` returned early (before the codecs, which it skips: it holds
        // no payloads) is recorded in history too. One already encoded is left as it is. The
        // heap-free fallback below isn't encoded: its message is only an error name.
        if (r.encode_failure_attributes) if (handled.completion.status) |*st| if (st.* == .failed)
            failure_mod.encodeAttributes(done_pb.WorkflowActivationCompletion, &handled.completion, a) catch
                return fallbackCompletion(run_id, "OutOfMemory", fallback_buf);
        var encoded: Io.Writer.Allocating = .init(a);
        handled.completion.encode(&encoded.writer, a) catch |err| {
            r.logger.log(.err, "encoding a workflow activation completion failed", &.{
                .{ .key = "run_id", .value = run_id orelse "" },
                .{ .key = "error", .value = @errorName(err) },
            });
            return fallbackCompletion(run_id, @errorName(err), fallback_buf);
        };
        return .{ .completion = .{ .bytes = encoded.written(), .eviction = handled.eviction } };
    }

    /// A failed completion for the activation's run, built in `buf` without the heap: the
    /// likeliest reason to need it is running out of memory.
    fn fallbackCompletion(maybe_run_id: ?[]const u8, err_name: []const u8, buf: []u8) Response {
        const run_id = maybe_run_id orelse return .none;
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
        try r.decodeActivationPayloads(act, cached, a, io);
        try failure_mod.decodeAttributes(act_pb.WorkflowActivation, act, a);
    }

    fn decodeActivationPayloads(r: *WorkflowRunner, act: *act_pb.WorkflowActivation, cached: ?*const Instance, a: Allocator, io: Io) converter.CodecError!void {
        if (r.codecs.len == 0) return;
        if (cached) |inst| return codeWithOwnContexts(act_pb.WorkflowActivation, act, &act.jobs, jobCodecContext, inst, r.codecs, a, io, &runCodecContext(r.namespace, inst), .decode);
        const index = for (act.jobs.items, 0..) |job, i| {
            if (job.variant) |v| if (v == .initialize_workflow) break i;
        } else return converter.decodeMessage(act_pb.WorkflowActivation, act, r.codecs, a, io, &.{ .namespace = r.namespace });
        const init = &act.jobs.items[index].variant.?.initialize_workflow;
        var context: converter.Context = .{ .namespace = r.namespace, .workflow_id = init.workflow_id, .workflow_type = init.workflow_type };
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

    /// Encodes the completion's payloads: an activity's, local activity's or child's start
    /// command with that call's context (as the payload hooks got), the rest with the run's.
    fn encodeCompletionPayloads(r: *WorkflowRunner, completion: *done_pb.WorkflowActivationCompletion, inst: *const Instance, a: Allocator, io: Io) converter.CodecError!void {
        if (r.codecs.len == 0) return;
        const run_context = runCodecContext(r.namespace, inst);
        const commands = if (completion.status) |*status| switch (status.*) {
            .successful => |*success| &success.commands,
            .failed => null,
        } else null;
        const list = commands orelse return converter.encodeMessage(done_pb.WorkflowActivationCompletion, completion, r.codecs, a, io, &run_context);
        return codeWithOwnContexts(done_pb.WorkflowActivationCompletion, completion, list, commandCodecContext, inst, r.codecs, a, io, &run_context, .encode);
    }

    const Handled = struct {
        completion: done_pb.WorkflowActivationCompletion,
        /// The activation evicted its run.
        eviction: bool = false,
        /// The deadlock watchdog answered the activation; `completion` must not be sent.
        discarded: bool = false,
    };

    fn handle(r: *WorkflowRunner, bytes: []const u8, a: Allocator, io: Io) !Handled {
        return r.handleWatched(bytes, a, io, null);
    }

    /// `handle`, watching the activation's workflow code for deadlocks when
    /// `deadlock_timeout` and `sender` are set.
    fn handleWatched(r: *WorkflowRunner, bytes: []const u8, a: Allocator, io: Io, sender: ?Sender) !Handled {
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
        // An abandoned run is no longer in `runs`, and its instance is already freed.
        var abandoned = false;
        defer if (!abandoned) r.release(act.run_id, io);

        r.decodeActivation(&act, cached, a, io) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            // The task fails and is retried; the workflow never sees undecoded payloads.
            error.Canceled, error.CodecFailed => return .{ .completion = try taskFailure(a, act.run_id, try std.fmt.allocPrint(a, "decoding the workflow activation's payloads with the client's codecs failed: {t}", .{err})) },
        };
        const activated = switch (r.activateWatched(inst, &act, a, io, sender)) {
            .returned => |result| result,
            .abandoned => {
                abandoned = true;
                return .{ .completion = .{ .run_id = act.run_id }, .discarded = true };
            },
        };
        var completion = activated catch |err| return .{ .completion = try taskFailure(a, act.run_id, try std.fmt.allocPrint(a, "workflow activation failed: {t}", .{err})) };
        if (r.encode_failure_attributes) try encodeCompletionAttributes(&completion, a);
        r.encodeCompletionPayloads(&completion, inst, a, io) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            error.Canceled, error.CodecFailed => return .{ .completion = try taskFailure(a, act.run_id, try std.fmt.allocPrint(a, "encoding the workflow activation completion's payloads with the client's codecs failed: {t}", .{err})) },
        };
        return .{ .completion = completion };
    }

    const Activated = union(enum) {
        returned: @typeInfo(@TypeOf(Instance.activate)).@"fn".return_type.?,
        /// The watchdog gave up on the code and answered the activation; the code has since
        /// returned, and the run's instance is freed.
        abandoned,
    };

    /// Runs `inst.activate`, watched for deadlocks when `deadlock_timeout` and `sender` are
    /// set. Codecs run before and after this, so they aren't counted.
    fn activateWatched(r: *WorkflowRunner, inst: *Instance, act: *const act_pb.WorkflowActivation, a: Allocator, io: Io, sender: ?Sender) Activated {
        const limit = r.deadlock_timeout orelse return .{ .returned = inst.activate(act, a) };
        var watch: Watch = .{
            .r = r,
            .io = io,
            .limit = limit,
            .run_id = act.run_id,
            .switches = &inst.sched.switches,
            .sdk_work = &inst.sched.sdk_work,
            .sender = sender orelse return .{ .returned = inst.activate(act, a) },
        };
        var watchdog = io_tasks.concurrent(io, Watch.run, .{&watch}) catch {
            r.logger.log(.warn, "no concurrent task available for the deadlock watchdog; this workflow activation runs unwatched", &.{.{ .key = "run_id", .value = act.run_id }});
            return .{ .returned = inst.activate(act, a) };
        };
        const result = inst.activate(act, a);
        watch.returned.set(io);
        watchdog.await(io);
        if (!watch.abandoned) return .{ .returned = result };
        // The watchdog took the run out of `runs` and answered for it; nothing else holds the
        // instance now that its code has returned.
        if (watch.orphan) |o| r.destroyRun(o.key, o.value);
        return .abandoned;
    }

    /// One activation's workflow code, as its deadlock watchdog sees it. Lives on the
    /// activation thread's stack; the watchdog uses it until the activation thread has
    /// awaited the watchdog.
    const Watch = struct {
        r: *WorkflowRunner,
        io: Io,
        limit: Io.Duration,
        run_id: []const u8,
        /// The run's `Scheduler.switches`, which advances each time workflow code returns to
        /// the scheduler.
        switches: *const std.atomic.Value(u64),
        /// The run's `Scheduler.sdk_work`, set while the scheduler's thread does SDK work
        /// that isn't workflow code (reading debug info for the stack trace query or a
        /// failure's stack trace).
        sdk_work: *const std.atomic.Value(bool),
        sender: Sender,
        /// Set by the activation thread once workflow code has returned.
        returned: Io.Event = .unset,
        /// Written by the watchdog, read by the activation thread after awaiting it: the
        /// watchdog gave up on the code, and `orphan` is the cache entry it removed from
        /// `runs`, for the activation thread to free once the code has returned.
        abandoned: bool = false,
        orphan: ?std.StringHashMapUnmanaged(CachedRun).KV = null,

        /// Samples `switches` every quarter of the limit on the worker's real clock. A count
        /// unchanged since it was first seen at least `limit` ago means the code hasn't
        /// returned to the scheduler for at least that long. A sample that finds `sdk_work`
        /// set starts the clock again, so that time isn't counted; SDK work begun and ended
        /// between two samples is counted.
        fn run(w: *Watch) void {
            const io = w.io;
            const tick: Io.Clock.Duration = .{ .raw = .fromNanoseconds(@max(@divTrunc(w.limit.nanoseconds, 4), 1)), .clock = .awake };
            var seen = w.switches.load(.acquire);
            var since = Io.Clock.awake.now(io);
            while (true) {
                if (w.returned.waitTimeout(io, .{ .duration = tick })) |_| {
                    return;
                } else |err| switch (err) {
                    error.Timeout => {},
                    error.Canceled => return,
                }
                const now = Io.Clock.awake.now(io);
                const switches = w.switches.load(.acquire);
                if (switches != seen or w.sdk_work.load(.acquire)) {
                    seen = switches;
                    since = now;
                    continue;
                }
                if (since.durationTo(now).nanoseconds < w.limit.nanoseconds) continue;
                // Returned between the timeout and now: it ran under the limit, or so close
                // to it that answering normally is as right.
                if (w.returned.isSet()) return;
                w.r.abandon(w);
                return;
            }
        }
    };

    /// Gives up on a stuck activation: takes its run out of `runs` (so the core's eviction and
    /// a later replay of the run don't wait for it), logs, and sends the deadlock failure.
    fn abandon(r: *WorkflowRunner, w: *Watch) void {
        const io = w.io;
        w.abandoned = true;
        {
            r.mutex.lockUncancelable(io);
            defer r.mutex.unlock(io);
            w.orphan = r.runs.fetchRemove(w.run_id);
        }
        const workflow_id = if (w.orphan) |o| o.value.workflow_id else "";
        const workflow_type = if (w.orphan) |o| o.value.workflow_type else "";
        var arena_state: std.heap.ArenaAllocator = .init(r.gpa);
        defer arena_state.deinit();
        const a = arena_state.allocator();
        var fallback_buf: [fallback_buffer_size]u8 = undefined;
        var limit_buf: [64]u8 = undefined;
        const limit = std.fmt.bufPrint(&limit_buf, "{f}", .{w.limit}) catch unreachable;
        const message = deadlockMessage(a, workflow_type, limit) catch "workflow code ran longer than worker.Options.deadlock_detection_timeout without returning to the workflow scheduler";
        r.logger.log(.err, message, &.{
            .{ .key = "workflow_id", .value = workflow_id },
            .{ .key = "run_id", .value = w.run_id },
            .{ .key = "workflow_type", .value = workflow_type },
            .{ .key = "deadlock_detection_timeout", .value = limit },
        });
        const response = r.deadlockResponse(a, w.run_id, message) catch fallbackCompletion(w.run_id, "OutOfMemory", &fallback_buf);
        w.sender.send(w.sender.ctx, w.run_id, response);
    }

    fn deadlockMessage(a: Allocator, workflow_type: []const u8, limit: []const u8) error{OutOfMemory}![]const u8 {
        return std.fmt.allocPrint(a, "potential deadlock: workflow code of type \"{s}\" ran longer than worker.Options.deadlock_detection_timeout ({s}) without returning to the workflow scheduler; look for a loop that never waits, or a call that blocks outside the workflow's std.Io. The workflow task fails and the run replays from history on its next attempt", .{ workflow_type, limit });
    }

    fn deadlockResponse(r: *WorkflowRunner, a: Allocator, run_id: []const u8, message: []const u8) error{OutOfMemory}!Response {
        var completion: done_pb.WorkflowActivationCompletion = .{ .run_id = run_id, .status = .{ .failed = .{
            .failure = try failure_mod.application(a, .{ .message = message, .type = deadlock_failure_type }),
        } } };
        if (r.encode_failure_attributes) try failure_mod.encodeAttributes(done_pb.WorkflowActivationCompletion, &completion, a);
        var encoded: Io.Writer.Allocating = .init(a);
        completion.encode(&encoded.writer, a) catch return error.OutOfMemory;
        return .{ .completion = .{ .bytes = encoded.written() } };
    }

    /// `failure.encodeAttributes` for the completion, except query answers: they never reach
    /// history, and the server reports a failed query's message to the caller as its gRPC
    /// status message (`QueryOptions.failure_message`), which no codec decodes.
    fn encodeCompletionAttributes(completion: *done_pb.WorkflowActivationCompletion, a: Allocator) error{OutOfMemory}!void {
        const status = if (completion.status) |*s| s else return;
        switch (status.*) {
            .successful => |*success| {
                const commands = try a.dupe(proto.coresdk.workflow_commands.WorkflowCommand, success.commands.items);
                for (commands) |*cmd| {
                    if (cmd.variant) |v| if (v == .respond_to_query) continue;
                    try failure_mod.encodeAttributes(@TypeOf(cmd.*), cmd, a);
                }
                success.commands = .fromOwnedSlice(commands);
            },
            else => try failure_mod.encodeAttributes(done_pb.WorkflowActivationCompletion, completion, a),
        }
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
            .failure_stack_traces = r.failure_stack_traces,
            .task_failure_errors = r.task_failure_errors,
            .failure_converter = r.failure_converter,
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
    const id = bytesField(bytes, 1) orelse return null;
    return if (id.len > 0) id else null;
}

/// The first length-delimited field `number` of serialized protobuf `bytes`, read without
/// decoding or allocating. A field that is present but empty gives an empty slice; a missing
/// field, or malformed bytes before it, gives null.
fn bytesField(bytes: []const u8, number: u64) ?[]const u8 {
    var i: usize = 0;
    while (i < bytes.len) {
        const field = nextField(bytes, &i) orelse return null;
        if (field.number == number) switch (field.value) {
            .bytes => |b| return b,
            else => {},
        };
    }
    return null;
}

/// The first varint field `number` of serialized protobuf `bytes`, read as `bytesField` reads
/// its field; null if it is missing or the bytes before it are malformed.
fn varintField(bytes: []const u8, number: u64) ?u64 {
    var i: usize = 0;
    while (i < bytes.len) {
        const field = nextField(bytes, &i) orelse return null;
        if (field.number == number) switch (field.value) {
            .varint => |v| return v,
            else => {},
        };
    }
    return null;
}

/// The protobuf field at `i.*`, which is moved past it; null for malformed bytes.
fn nextField(bytes: []const u8, i: *usize) ?struct { number: u64, value: union(enum) { varint: u64, bytes: []const u8, fixed } } {
    const tag = readVarint(bytes, i) orelse return null;
    const number = tag >> 3;
    switch (tag & 7) {
        0 => return .{ .number = number, .value = .{ .varint = readVarint(bytes, i) orelse return null } },
        2 => {
            const len = std.math.cast(usize, readVarint(bytes, i) orelse return null) orelse return null;
            if (len > bytes.len - i.*) return null;
            defer i.* += len;
            return .{ .number = number, .value = .{ .bytes = bytes[i.*..][0..len] } };
        },
        1, 5 => |t| {
            const size: usize = if (t == 1) 8 else 4;
            if (size > bytes.len - i.*) return null;
            i.* += size;
            return .{ .number = number, .value = .fixed };
        },
        else => return null,
    }
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

/// Codes `msg` (`.encode` or `.decode`) with `run_context`, except the items of `list`, a list
/// inside `msg`, that `contextOf` gives a context of their own: each of those is coded alone
/// with it. `list` is replaced by a copy first, so the caller's items are left as they were.
fn codeWithOwnContexts(
    comptime Msg: type,
    msg: *Msg,
    list: anytype,
    comptime contextOf: anytype,
    inst: *const Instance,
    codecs: []const converter.PayloadCodec,
    a: Allocator,
    io: Io,
    run_context: *const converter.Context,
    comptime direction: enum { encode, decode },
) converter.CodecError!void {
    const code = switch (direction) {
        .encode => converter.encodeMessage,
        .decode => converter.decodeMessage,
    };
    const Item = @typeInfo(@TypeOf(list.items)).pointer.child;
    const items = try a.dupe(Item, list.items);
    list.* = .fromOwnedSlice(items);
    var own: std.ArrayList(struct { index: usize, item: Item }) = .empty;
    for (items, 0..) |*item, i| {
        const context = contextOf(inst, item) orelse continue;
        var copy = item.*;
        try code(Item, &copy, codecs, a, io, context);
        try own.append(a, .{ .index = i, .item = copy });
        item.* = .{};
    }
    try code(Msg, msg, codecs, a, io, run_context);
    // Coding copied the list, keeping its length and order; the items coded alone go back
    // into the copy.
    for (own.items) |o| list.items[o.index] = o.item;
}

/// The codec context of an activation job that resolves an activity or a child workflow:
/// that call's, as the payload hooks get it. Null for every other job.
fn jobCodecContext(inst: *const Instance, job: *const act_pb.WorkflowActivationJob) ?*const converter.Context {
    const variant = job.variant orelse return null;
    return switch (variant) {
        .resolve_activity => |j| inst.activityCodecContext(j.seq),
        .resolve_child_workflow_execution_start => |j| inst.childCodecContext(j.seq),
        .resolve_child_workflow_execution => |j| inst.childCodecContext(j.seq),
        else => null,
    };
}

/// The codec context of a command that schedules an activity or local activity or starts a
/// child workflow: that call's, as its payload hooks got it. Null for every other command.
fn commandCodecContext(inst: *const Instance, cmd: *const proto.coresdk.workflow_commands.WorkflowCommand) ?*const converter.Context {
    const variant = cmd.variant orelse return null;
    return switch (variant) {
        .schedule_activity => |c| inst.activityCodecContext(c.seq),
        .schedule_local_activity => |c| inst.activityCodecContext(c.seq),
        .start_child_workflow_execution => |c| inst.childCodecContext(c.seq),
        else => null,
    };
}

/// The codec context of a cached run: its identity and the values `ctx.propagated()` returns.
fn runCodecContext(namespace: []const u8, inst: *const Instance) converter.Context {
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
    const slot = slot: {
        w.stop_mutex.lockUncancelable(w.io);
        defer w.stop_mutex.unlock(w.io);
        // One loop per core, and a `run` has at most as many cores as slots; only `run`
        // called again while running fills them.
        const i = std.mem.indexOfScalar(?*RunningActivities, &w.activity_runs, null) orelse {
            log.warn("worker for task queue {s}: more activity loops than expected (is run called twice at once?); activities on {s} won't see the worker stop", .{ w.task_queue, task_queue });
            break :slot null;
        };
        w.activity_runs[i] = &running;
        break :slot i;
    };
    defer if (slot) |i| {
        w.stop_mutex.lockUncancelable(w.io);
        w.activity_runs[i] = null;
        w.stop_mutex.unlock(w.io);
    };
    var message: ?[]u8 = null;
    defer if (message) |m| w.gpa.free(m);
    var gpa_refused = false;
    while (true) {
        const polled = w.pollCore(core, bridge.Worker.pollActivityTask, &message, &gpa_refused) catch |err| {
            w.pollFailed(.activity, task_queue, err, message);
            continue;
        } orelse break;
        const run = receiveActivityTask(w, core, &running, polled, task_queue) orelse continue;
        // Each activity gets its own thread: `async` would run it inline in this loop once the
        // Io has as many busy threads as CPUs, running activities one at a time. The core's
        // activity slots bound how many run at once. Never run on this thread: the cancel for
        // an activity arrives in a later poll of this loop, as do other activities' tasks.
        group.concurrent(w.io, superviseActivity, .{ w, core, &running, run }) catch
            finishActivity(w, core, &running, run, noThreadForActivity(run));
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
    /// w.gpa with cancels blocked around each call; backs arena.
    task_gpa: CancelBlockingAllocator,
    arena: std.heap.ArenaAllocator,
    polled: bridge.Worker.Polled,
    /// The task queue of the core that polled the activity; outlives the run.
    task_queue: []const u8,
    task_token: []const u8,
    start: task_pb.Start,
    ctx: activity.Context,
    /// What the codecs see for this activity's payloads: its identity, set when the Start task
    /// is received, and once its header is decoded, `ctx.propagated_`.
    codec_context: converter.Context = .{},
    /// Set when the activity returns or a cancel arrives; wakes the supervisor.
    wake: Io.Event = .unset,

    fn destroy(run: *RunningActivity, gpa: Allocator) void {
        run.arena.deinit();
        run.polled.deinit();
        gpa.destroy(run);
    }
};

/// An activity task's token and kind, read from its bytes without decoding or allocating.
/// Borrows `bytes`.
const ActivityTaskHead = struct {
    token: []const u8,
    kind: union(enum) { start, cancel: task_pb.ActivityCancelReason },

    /// Null without a token or a variant, or for malformed bytes.
    fn read(bytes: []const u8) ?ActivityTaskHead {
        const token = bytesField(bytes, 1) orelse return null;
        if (token.len == 0) return null;
        if (bytesField(bytes, 3) != null) return .{ .token = token, .kind = .start };
        // A Cancel with reason NOT_FOUND (0) and no details is empty on the wire.
        const cancel = bytesField(bytes, 4) orelse return null;
        const reason = varintField(cancel, 1) orelse 0;
        return .{ .token = token, .kind = .{ .cancel = @fromBackingInt(@as(i32, @bitCast(@as(u32, @truncate(reason))))) } };
    }
};

/// Room for `encodeFailedActivityCompletion`: the first 4 KiB build the failure, the rest
/// hold its encoding.
const failed_activity_completion_len = 3 * 4096;

/// A failed ActivityTaskCompletion for `token`, built and encoded in `buffer` without the
/// heap, for a worker out of memory. Null when it doesn't fit (a token or message longer than
/// about 7 KiB).
fn encodeFailedActivityCompletion(buffer: *[failed_activity_completion_len]u8, token: []const u8, failure: failure_mod.Application) ?[]const u8 {
    var fba: std.heap.FixedBufferAllocator = .init(buffer[0..4096]);
    const fa = fba.allocator();
    const f = failure_mod.application(fa, failure) catch return null;
    const completion: proto.coresdk.self.ActivityTaskCompletion = .{ .task_token = token, .result = .{ .status = .{ .failed = .{ .failure = f } } } };
    var writer: Io.Writer = .fixed(buffer[4096..]);
    completion.encode(&writer, fa) catch return null;
    return writer.buffered();
}

/// Completes the activity task `token` as failed with `failure`, without the heap: the core
/// keeps a task's slot until it is completed, and the likeliest reason to need this is
/// running out of memory. `token` must stay valid during the call.
fn failActivityTask(w: *Worker, core: bridge.Worker, token: []const u8, failure: failure_mod.Application) void {
    var buffer: [failed_activity_completion_len]u8 = undefined;
    const completion = encodeFailedActivityCompletion(&buffer, token, failure) orelse
        return w.logger().err("Activity task token is too long to fail without memory; its slot stays held until the worker stops", .{ .TaskTokenLength = token.len });
    sendActivityCompletion(w, core, completion);
}

/// Hands an encoded ActivityTaskCompletion to the core. The bridge allocates the cell it waits
/// in before the core sees the completion, so on `error.OutOfMemory` it is sent again with
/// the cell on the stack (safe for the reason `WorkflowRunner.sendCompletion` gives).
fn sendActivityCompletion(w: *Worker, core: bridge.Worker, completion: []const u8) void {
    var message: ?[]u8 = null;
    defer if (message) |m| w.gpa.free(m);
    core.completeActivityTask(w.gpa, w.io, completion, &message) catch |err| switch (err) {
        error.OutOfMemory => {
            var cell: [256]u8 = undefined;
            var fba: std.heap.FixedBufferAllocator = .init(&cell);
            core.completeActivityTask(fba.allocator(), w.io, completion, null) catch |e|
                log.err("completing an activity task failed: {t}", .{e});
        },
        error.CompletionFailed => log.err("completing an activity task failed: {s}", .{message orelse @errorName(err)}),
    };
}

/// Fails the activity task `token` as retryable after running out of memory, with
/// `message_fmt` formatted on the stack (the plain text of `fallback` if it doesn't fit).
fn failActivityOutOfMemory(w: *Worker, core: bridge.Worker, token: []const u8, comptime message_fmt: []const u8, args: anytype, fallback: []const u8) void {
    var buf: [512]u8 = undefined;
    const message = std.fmt.bufPrint(&buf, message_fmt, args) catch fallback;
    failActivityTask(w, core, token, .{ .message = message, .type = @errorName(error.OutOfMemory) });
}

/// Delivers a Cancel task to the run it names, if this worker is running it. Allocates
/// nothing, so a cancel reaches its activity however short of memory the worker is.
fn deliverActivityCancel(w: *Worker, running: *RunningActivities, token: []const u8, reason: task_pb.ActivityCancelReason) void {
    running.mutex.lockUncancelable(w.io);
    defer running.mutex.unlock(w.io);
    const r = cancelReason(reason);
    // A run that already finished has nothing to cancel.
    const target = running.map.get(token) orelse {
        log.debug("a cancel task ({t}) arrived for an activity this worker is not running; ignoring it", .{r});
        return;
    };
    if (target.ctx.setCancelReason(r)) {
        log.debug("activity {s}: cancel task received ({t}); waking its supervisor", .{ target.start.activity_id, r });
        target.wake.set(w.io);
    } else log.debug("activity {s}: cancel task received ({t}) after an earlier cancel ({t}); nothing to do", .{ target.start.activity_id, r, target.ctx.cancelReason().? });
}

/// Takes a polled activity task. Returns a new run for a Start task, tracked in `running` so
/// cancels reach it; delivers a Cancel task to its run and returns null. A Start task that
/// can't be decoded or tracked is failed, so the core releases its slot, and never runs.
fn receiveActivityTask(w: *Worker, core: bridge.Worker, running: *RunningActivities, polled: bridge.Worker.Polled, task_queue: []const u8) ?*RunningActivity {
    // `head.token` borrows `polled`: every failure below that uses it comes before the free.
    const head = ActivityTaskHead.read(polled.bytes()) orelse {
        w.logger().err("An activity task with no task token; it can't be completed", .{ .TaskQueue = task_queue });
        polled.deinit();
        return null;
    };
    switch (head.kind) {
        .cancel => |reason| {
            deliverActivityCancel(w, running, head.token, reason);
            polled.deinit();
            return null;
        },
        .start => {},
    }
    const run = w.gpa.create(RunningActivity) catch {
        w.logger().err("Out of memory receiving an activity task; failing it as retryable so the server retries it", .{ .TaskQueue = task_queue });
        failActivityTask(w, core, head.token, .{ .message = "the worker ran out of memory receiving the activity task", .type = @errorName(error.OutOfMemory) });
        polled.deinit();
        return null;
    };
    run.* = .{
        .task_gpa = .{ .io = w.io, .backing = w.gpa },
        .arena = undefined,
        .polled = polled,
        .task_queue = task_queue,
        .task_token = head.token,
        .start = .{},
        .ctx = .{ .arena = undefined, .io_ = w.io, .info_ = undefined, .logger_ = .{}, .client_ = w.client },
    };
    run.arena = .init(run.task_gpa.allocator());
    var keep = false;
    defer if (!keep) run.destroy(w.gpa);

    var reader: Io.Reader = .fixed(polled.bytes());
    const task = task_pb.ActivityTask.decode(&reader, run.arena.allocator()) catch |err| {
        if (err == error.OutOfMemory) {
            w.logger().err("Out of memory decoding an activity task; failing it as retryable so the server retries it", .{ .TaskQueue = task_queue });
            failActivityTask(w, core, head.token, .{ .message = "the worker ran out of memory decoding the activity task", .type = @errorName(error.OutOfMemory) });
        } else {
            w.logger().err("Decoding an activity task failed; failing it as non-retryable", .{ .TaskQueue = task_queue, .Error = @errorName(err) });
            var buf: [256]u8 = undefined;
            const message = std.fmt.bufPrint(&buf, "the worker couldn't decode the activity task: {t}", .{err}) catch "the worker couldn't decode the activity task";
            failActivityTask(w, core, head.token, .{ .message = message, .type = "ActivityTaskDecodeFailed", .non_retryable = true });
        }
        return null;
    };
    const start: ?task_pb.Start = if (task.variant) |v| switch (v) {
        .start => |s| s,
        .cancel => null,
    } else null;
    run.start = start orelse {
        w.logger().err("An activity task read as a Start decoded as something else; failing it as non-retryable", .{ .TaskQueue = task_queue });
        failActivityTask(w, core, head.token, .{ .message = "the worker couldn't decode the activity task: it holds no Start", .type = "ActivityTaskDecodeFailed", .non_retryable = true });
        return null;
    };
    // Set before anything can report the attempt: a failure the worker reports without
    // running the activity (no thread for it) is encoded in the activity's context too.
    const execution = run.start.workflow_execution orelse proto.temporal.api.common.v1.WorkflowExecution{};
    run.codec_context = .{
        .namespace = w.client.namespace,
        .workflow_id = execution.workflow_id,
        .workflow_type = run.start.workflow_type,
        .activity_type = run.start.activity_type,
        .activity_id = run.start.activity_id,
    };
    // A side effect's value is its workflow run's own: the workflow's worker codes it with the
    // run's identity, which is all this worker can know of it (its command has no header).
    if (run.start.is_local and std.mem.eql(u8, run.start.activity_type, Instance.side_effect_activity_type)) {
        run.codec_context.activity_type = "";
        run.codec_context.activity_id = "";
    }
    const tracked = tracked: {
        running.mutex.lockUncancelable(w.io);
        defer running.mutex.unlock(w.io);
        running.map.put(w.gpa, run.task_token, run) catch break :tracked false;
        if (w.stopping.load(.acquire)) run.ctx.notifyWorkerStop();
        break :tracked true;
    };
    if (tracked) {
        keep = true;
        return run;
    }
    // Untracked, the run couldn't be interrupted by a cancel or the worker's shutdown, so it
    // doesn't run at all.
    w.logger().err("Out of memory tracking an activity for cancellation; failing it as retryable so the server retries it", .{ .ActivityID = run.start.activity_id });
    failActivityOutOfMemory(w, core, head.token, "the worker ran out of memory tracking activity {s} for cancellation", .{run.start.activity_id}, "the worker ran out of memory tracking the activity for cancellation");
    return null;
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
/// on the activity's task, so a cancel arriving late can't interrupt the report. Without a
/// thread for the activity the attempt fails with a retryable failure: run here, the activity
/// couldn't be canceled.
fn superviseActivity(w: *Worker, core: bridge.Worker, running: *RunningActivities, run: *RunningActivity) void {
    const outcome = blk: {
        var task = io_tasks.concurrent(w.io, callActivity, .{ w, core, run }) catch break :blk noThreadForActivity(run);
        run.wake.waitUncancelable(w.io);
        const reason = run.ctx.cancelReason() orelse break :blk task.await(w.io);
        log.debug("activity {s}: canceling its task ({t})", .{ run.start.activity_id, reason });
        const outcome = task.cancel(w.io);
        log.debug("activity {s}: its task returned after the cancel", .{run.start.activity_id});
        break :blk outcome;
    };
    finishActivity(w, core, running, run, outcome);
}

/// The `failure.Application.type` of an activity attempt the worker had no thread for, so a
/// retry policy's non-retryable types can name it.
const no_thread_failure_type = "NoThreadAvailable";

/// The result of an activity attempt the worker's Io has no thread for: a retryable failure,
/// so the activity's retry policy runs it again.
fn noThreadForActivity(run: *RunningActivity) error{OutOfMemory}!result_pb.ActivityExecutionResult {
    log.warn("no thread available for activity {s}; failing the attempt so it is retried", .{run.start.activity_id});
    const a = run.arena.allocator();
    return .{ .status = .{ .failed = .{ .failure = try failure_mod.application(a, .{
        .message = try std.fmt.allocPrint(a, "the worker's Io has no thread free to run activity {s}", .{run.start.activity_id}),
        .type = no_thread_failure_type,
    }) } } };
}

/// Reports an activity's result to the core, then forgets and frees the run. Every run
/// `receiveActivityTask` returns ends here exactly once.
fn finishActivity(w: *Worker, core: bridge.Worker, running: *RunningActivities, run: *RunningActivity, outcome: error{OutOfMemory}!result_pb.ActivityExecutionResult) void {
    defer {
        running.mutex.lockUncancelable(w.io);
        _ = running.map.remove(run.task_token);
        running.mutex.unlock(w.io);
        run.destroy(w.gpa);
    }
    const a = run.arena.allocator();
    // Out of memory anywhere below, the task is still completed, as a retryable failure from
    // stack memory: the core holds its slot until it is. The defer above frees `run` after.
    var result = outcome catch return reportOutOfMemory(w, core, run);
    if (w.client.encode_failure_attributes) failure_mod.encodeAttributes(result_pb.ActivityExecutionResult, &result, a) catch
        return reportOutOfMemory(w, core, run);
    converter.encodeMessage(result_pb.ActivityExecutionResult, &result, w.client.codecs, a, w.io, &run.codec_context) catch |err| {
        if (err == error.OutOfMemory) return reportOutOfMemory(w, core, run);
        // Report a failure without the payloads that couldn't be encoded.
        result = .{ .status = .{ .failed = .{ .failure = failure_mod.application(a, .{
            .message = std.fmt.allocPrint(a, "encoding the result of activity {s} with the client's codecs failed: {t}", .{ run.start.activity_id, err }) catch
                return reportOutOfMemory(w, core, run),
            .type = "PayloadCodecFailed",
        }) catch return reportOutOfMemory(w, core, run) } } };
    };
    var encoded: Io.Writer.Allocating = .init(a);
    (proto.coresdk.self.ActivityTaskCompletion{ .task_token = run.task_token, .result = result }).encode(&encoded.writer, a) catch
        return reportOutOfMemory(w, core, run);
    sendActivityCompletion(w, core, encoded.written());
}

/// Fails `run`'s task as retryable after running out of memory reporting its result.
fn reportOutOfMemory(w: *Worker, core: bridge.Worker, run: *const RunningActivity) void {
    w.logger().err("Out of memory reporting an activity; failing it as retryable so the server retries it", .{ .ActivityID = run.start.activity_id });
    failActivityOutOfMemory(w, core, run.task_token, "the worker ran out of memory reporting activity {s}", .{run.start.activity_id}, "the worker ran out of memory reporting the activity");
}

// `std.Io.Threaded` futures overrun their allocation for results aligned above 8; see `runNexusRequest`.
comptime {
    std.debug.assert(@alignOf(error{OutOfMemory}!result_pb.ActivityExecutionResult) <= @alignOf(usize));
}

fn callActivity(w: *Worker, core: bridge.Worker, run: *RunningActivity) error{OutOfMemory}!result_pb.ActivityExecutionResult {
    defer run.wake.set(w.io);
    // The worker cancels an activity's task once. SDK code here can wait without returning
    // the error to the activity (in Debug even an allocation can: std's stack capture locks
    // its debug info and drops a cancel), so it runs with cancels blocked, and only the
    // activity's interceptors and function (`executeActivity`) see them.
    const prev = w.io.swapCancelProtection(.blocked);
    defer _ = w.io.swapCancelProtection(prev);
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
    codec_context: *const converter.Context,
    /// The latest details, before the codecs: the attempt's last heartbeat's, or those it
    /// started with. `last_arena` owns them once the attempt has heartbeated.
    last: []const converter.Payload,
    last_arena: std.heap.ArenaAllocator,
    mutex: Io.Mutex = .init,

    fn deinit(h: *Heartbeats) void {
        h.last_arena.deinit();
    }

    fn record(context: *anyopaque, details: []const proto.temporal.api.common.v1.Payload) void {
        const h: *Heartbeats = @ptrCast(@alignCast(context));
        h.remember(details);
        var scratch: std.heap.ArenaAllocator = .init(h.w.gpa);
        defer scratch.deinit();
        const a = scratch.allocator();
        var heartbeat: proto.coresdk.self.ActivityHeartbeat = .{ .task_token = h.task_token };
        heartbeat.details.appendSlice(a, details) catch return;
        // Runs with cancels blocked (`activity.Context.heartbeat`): a codec waiting here can't
        // take the activity's cancel.
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

    fn remember(h: *Heartbeats, details: []const converter.Payload) void {
        h.mutex.lockUncancelable(h.w.io);
        defer h.mutex.unlock(h.w.io);
        // Copy before resetting: `details` may be the previous copy.
        var next: std.heap.ArenaAllocator = .init(h.w.gpa);
        const copy = next.allocator().alloc(converter.Payload, details.len) catch return next.deinit();
        for (details, copy) |d, *c| c.* = d.dupe(next.allocator()) catch return next.deinit();
        h.last_arena.deinit();
        h.last_arena = next;
        h.last = copy;
    }

    /// Whether the server has a cancel request for the attempt, asked with a heartbeat that
    /// repeats the latest details so the request doesn't clear them. False when the server
    /// no longer knows the attempt or the call fails.
    fn cancelRequested(h: *Heartbeats) bool {
        h.mutex.lockUncancelable(h.w.io);
        defer h.mutex.unlock(h.w.io);
        var scratch: std.heap.ArenaAllocator = .init(h.w.gpa);
        defer scratch.deinit();
        const response = h.w.client.recordWorkerHeartbeat(scratch.allocator(), h.task_token, h.last, h.codec_context) catch |err| {
            log.debug("asking whether a paused or reset activity was canceled failed: {t}", .{err});
            return false;
        };
        return response.cancel_requested;
    }
};

fn executeActivity(w: *Worker, core: bridge.Worker, run: *RunningActivity) !result_pb.ActivityExecutionResult {
    const a = run.arena.allocator();
    const start = &run.start;
    const execution = start.workflow_execution orelse proto.temporal.api.common.v1.WorkflowExecution{};
    const ctx = &run.ctx;
    ctx.propagated_ = .init(a);
    decodeActivityStart(w, run) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        error.Canceled, error.CodecFailed => return .{ .status = .{ .failed = .{ .failure = try failure_mod.application(a, .{
            .message = try std.fmt.allocPrint(a, "decoding the input of activity {s} with the client's codecs failed: {t}", .{ start.activity_id, err }),
            .type = "PayloadCodecFailed",
        }) } } },
    };
    // The payload converter sees what the codecs see, propagated values included.
    ctx.converter_context_ = run.codec_context;
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
    });
    // A standalone activity has no workflow: leave the workflow fields out rather than empty.
    if (execution.workflow_id.len > 0) ctx.logger_ = try ctx.logger_.with(a, .{
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
    var heartbeats: Heartbeats = .{ .w = w, .core = core, .task_token = run.task_token, .codec_context = &run.codec_context, .last = start.heartbeat_details.items, .last_arena = .init(w.gpa) };
    defer heartbeats.deinit();
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
    // The activity's failure gets the frames its own error recorded, not earlier ones of the
    // pooled thread it runs on.
    const trace_scope: failure_mod.TraceScope = .begin(@errorReturnTrace());
    defer trace_scope.end();
    const payload = user: {
        const sdk = w.io.swapCancelProtection(.unblocked);
        defer _ = w.io.swapCancelProtection(sdk);
        break :user next.execute(.{ .activity_type = start.activity_type, .args = .{ .payloads = start.input.items, .context = ctx.converterContext() }, .header = ctx.header_ });
    } catch |err| {
        if (err == error.CompleteAsync) {
            if (!start.is_local) return .{ .status = .{ .will_complete_async = .{} } };
            return .{ .status = .{ .failed = .{ .failure = try failure_mod.application(a, .{
                .message = try std.fmt.allocPrint(a, "local activity \"{s}\" returned completeAsync, but only activities scheduled with executeActivity can complete asynchronously", .{start.activity_type}),
                .type = "LocalActivityCompleteAsync",
                .non_retryable = true,
            }) } } };
        }
        // The server refuses a canceled completion it didn't ask for: a paused or reset
        // attempt ends as a retryable failure, retried once the activity is unpaused. A
        // cancel requested since the pause reached the attempt still wins, but only a
        // heartbeat would tell the core of it, so ask the server.
        if (err == error.Canceled) if (ctx.cancelReason()) |reason| if ((reason == .paused or reason == .reset) and !heartbeats.cancelRequested()) {
            ctx.logger_.debug("Activity attempt ended", .{ .Reason = @tagName(reason) });
            return .{ .status = .{ .failed = .{ .failure = try failure_mod.application(a, .{
                .message = try std.fmt.allocPrint(a, "activity attempt ended because the activity was {s}", .{if (reason == .paused) "paused" else "reset"}),
                .type = if (reason == .paused) "ActivityPause" else "ActivityReset",
            }) } } };
        };
        if (err == error.Canceled) if (ctx.cancelReason()) |reason| return .{ .status = .{ .cancelled = .{ .failure = .{
            .message = try std.fmt.allocPrint(a, "activity canceled: {t}", .{if (reason == .paused or reason == .reset) .canceled else reason}),
            .source = failure_mod.source,
            .failure_info = .{ .canceled_failure_info = .{} },
        } } } };
        // Copied before anything else can return an error and add to the trace.
        const trace = if (w.options.failure_stack_traces) try failure_mod.captureTrace(a, @errorReturnTrace()) else &.{};
        var f = if (ctx.pending_failure) |p| try failure_mod.application(a, p) else try failure_mod.fromError(a, err);
        // The worker read the debug info before it polled (`warmDebugInfo`): this only looks
        // the addresses up.
        f.stack_trace = try failure_mod.addressesText(a, trace);
        if (failure_mod.applicationCategory(f) == .benign)
            ctx.logger_.debug("Activity failed", .{ .Error = f.message })
        else
            ctx.logger_.warn("Activity failed", .{ .Error = f.message });
        if (w.client.failure_converter) |c| f = try c.toFailure(a, ctx.converterContext(), err, f);
        return .{ .status = .{ .failed = .{ .failure = f } } };
    };
    return .{ .status = .{ .completed = .{ .result = payload } } };
}

/// Set once `warmDebugInfo` has read the executable's debug info, the first time in the
/// process; stack traces resolve without loading it from then on.
var debug_info_warm: std.atomic.Value(bool) = .init(false);

/// Reads the executable's debug info for stack traces (`failure.addressesText`), so a failed
/// activity doesn't, then wakes `w`'s `run` (`Worker.awaitDebugInfo`). The first read loads
/// and parses the debug info, and its time grows with the executable: bead
/// temporal-sdk-zig-d51.1.34 measured 0.6 s alone and 2.1 s within the unit tests on an
/// Apple-silicon Mac in Debug, all of it on the failed activity's thread before its
/// completion, and on an x86_64 macOS runner the activity's 10 s StartToClose timeout ran
/// out first. `Worker.run` starts it while the worker connects and doesn't poll before it
/// ends; `Worker.deinit` joins it.
noinline fn warmDebugInfo(w: *Worker) void {
    defer w.debug_info_wake.set(w.io);
    defer debug_info_warm.store(true, .release);
    // Scratch that belongs to no worker or activity, freed here.
    var scratch: std.heap.ArenaAllocator = .init(std.heap.page_allocator);
    defer scratch.deinit();
    // An address in the SDK's code and one in std's: where a build has a compile unit per
    // module, each unit's line table is parsed on its first lookup. `addressesText` looks up
    // the byte before each address.
    const addresses = [_]usize{ @intFromPtr(&warmDebugInfo) + 1, @intFromPtr(std.heap.page_allocator.vtable.alloc) + 1 };
    _ = failure_mod.addressesText(scratch.allocator(), &addresses) catch {};
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
        .scheduled_time = proto_time.timestampFromProto(start.scheduled_time),
        .current_attempt_scheduled_time = proto_time.timestampFromProto(start.current_attempt_scheduled_time),
        .started_time = proto_time.timestampFromProto(start.started_time),
        .schedule_to_close_timeout = proto_time.positiveDurationFromProto(start.schedule_to_close_timeout),
        .start_to_close_timeout = proto_time.positiveDurationFromProto(start.start_to_close_timeout),
        .heartbeat_timeout = proto_time.positiveDurationFromProto(start.heartbeat_timeout),
        .retry_policy = if (start.retry_policy) |p| .{
            .initial_interval = proto_time.positiveDurationFromProto(p.initial_interval),
            .backoff_coefficient = p.backoff_coefficient,
            .maximum_interval = proto_time.positiveDurationFromProto(p.maximum_interval),
            .maximum_attempts = p.maximum_attempts,
            .non_retryable_error_types = p.non_retryable_error_types.items,
        } else null,
        .priority = if (start.priority) |p| .{
            .priority_key = p.priority_key,
            .fairness_key = p.fairness_key,
            .fairness_weight = p.fairness_weight,
        } else .{},
    };
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

// ---------------------------------------------------------------- Nexus

const nexus_pb = proto.coresdk.nexus;

/// Polls the core's Nexus tasks: each request runs on a task of its own, and a cancel from
/// the core reaches its request's handler as `std.Io` cancellation. The core's Nexus slots
/// bound how many run at once.
fn nexusLoop(w: *Worker, core: bridge.Worker) void {
    var group: Io.Group = .init;
    var running: RunningNexusRequests = .{};
    defer running.map.deinit(w.gpa);
    var message: ?[]u8 = null;
    defer if (message) |m| w.gpa.free(m);
    var gpa_refused = false;
    while (true) {
        const polled = w.pollCore(core, bridge.Worker.pollNexusTask, &message, &gpa_refused) catch |err| {
            w.pollFailed(.nexus, w.task_queue, err, message);
            continue;
        } orelse break;
        const req = receiveNexusTask(w, core, &running, polled) orelse continue;
        // Never handled on this thread: the core's cancel for a request arrives in a later
        // poll, so a handler waiting for it here would wait forever.
        group.concurrent(w.io, superviseNexusRequest, .{ w, core, &running, req }) catch {
            req.outcome = .{ .failed = no_thread_for_nexus_request };
            finishNexusRequest(w, core, &running, req);
        };
    }
    group.await(w.io) catch {};
}

const no_thread_for_nexus_request: nexus.HandlerError = .{
    .type = .resource_exhausted,
    .message = "the worker's Io has no thread free to handle the Nexus request",
    .retry = .retryable,
};

/// The Nexus requests a worker is handling, by task token, so the poll loop can deliver the
/// core's cancels to them. The poll loop adds requests; each request's supervisor removes and
/// frees its own. A request is only touched by the poll loop while holding `mutex` and finding
/// it in `map`.
const RunningNexusRequests = struct {
    mutex: Io.Mutex = .init,
    /// Keys are the requests' task tokens, owned by the requests.
    map: std.StringHashMapUnmanaged(*NexusRequest) = .empty,
};

/// One Nexus request, from its task until its completion has been sent. Everything it holds
/// lives in `arena` or `polled`, which the handler's context borrows.
const NexusRequest = struct {
    /// w.gpa with cancels blocked around each call; backs arena.
    task_gpa: CancelBlockingAllocator,
    arena: std.heap.ArenaAllocator,
    polled: bridge.Worker.Polled,
    task_token: []const u8 = &.{},
    request: proto.temporal.api.nexus.v1.Request = .{},
    endpoint: []const u8 = "",
    deadline: ?Io.Timestamp = null,
    ctx: nexus.Context,
    outcome: NexusOutcome = .{ .failed = .{ .type = .internal, .message = "the Nexus request was never handled" } },
    /// Set when the handler returns or a cancel arrives; wakes the supervisor.
    wake: Io.Event = .unset,

    fn destroy(req: *NexusRequest, gpa: Allocator) void {
        req.arena.deinit();
        req.polled.deinit();
        gpa.destroy(req);
    }
};

/// What handling a request came to: a handler's result, or a handler error from before any
/// handler ran (no such service or operation, a request of neither kind).
const NexusOutcome = union(enum) {
    start: nexus.StartResult,
    cancel: nexus.CancelResult,
    failed: nexus.HandlerError,
};

/// A Nexus task's token and kind, read from its bytes without decoding or allocating.
/// Borrows `bytes`.
const NexusTaskHead = struct {
    token: []const u8,
    kind: union(enum) { task, cancel: nexus_pb.NexusTaskCancelReason },

    /// Null without a token or a variant, or for malformed bytes.
    fn read(bytes: []const u8) ?NexusTaskHead {
        if (bytesField(bytes, 1)) |task| {
            const token = bytesField(task, 1) orelse return null;
            return if (token.len > 0) .{ .token = token, .kind = .task } else null;
        }
        // A cancel with reason TIMED_OUT (0) has only its token on the wire.
        const cancel = bytesField(bytes, 2) orelse return null;
        const token = bytesField(cancel, 1) orelse return null;
        if (token.len == 0) return null;
        const reason = varintField(cancel, 2) orelse 0;
        return .{ .token = token, .kind = .{ .cancel = @fromBackingInt(@as(i32, @bitCast(@as(u32, @truncate(reason))))) } };
    }
};

/// Delivers a cancel from the core to the request it names, if it is still running.
/// Allocates nothing, so a cancel reaches its handler however short of memory the worker is.
fn deliverNexusCancel(w: *Worker, running: *RunningNexusRequests, token: []const u8, reason: nexus_pb.NexusTaskCancelReason) void {
    running.mutex.lockUncancelable(w.io);
    defer running.mutex.unlock(w.io);
    // A request that already finished has nothing to cancel.
    const target = running.map.get(token) orelse return;
    const r: nexus.CancelReason = switch (reason) {
        .WORKER_SHUTDOWN => .worker_shutdown,
        else => .timed_out,
    };
    if (target.ctx.setCancelReason(r)) target.wake.set(w.io);
}

/// Completes the Nexus task `token` as failed with `e`, without the heap: the likeliest reason
/// to need this is running out of memory. A token too long for the stack buffer falls back to
/// `dropNexusTask`. `token` must stay valid during the call.
fn failNexusTask(w: *Worker, core: bridge.Worker, token: []const u8, e: nexus.HandlerError, logger: log_mod.Logger) void {
    var buffer: [wire.failed_completion_buffer_len]u8 = undefined;
    const completion = wire.encodeFailedCompletion(&buffer, token, e) orelse return dropNexusTask(w, core, token, logger);
    completeNexusTask(w, core, completion, logger);
}

/// Takes a polled Nexus task. Returns a new request for a server request, tracked in `running`
/// so the core's cancels reach it; delivers a cancel from the core to its request and returns
/// null. A request that can't be decoded or tracked is failed and never runs.
fn receiveNexusTask(w: *Worker, core: bridge.Worker, running: *RunningNexusRequests, polled: bridge.Worker.Polled) ?*NexusRequest {
    const sink_logger: log_mod.Logger = .{ .sink = w.logSink() };
    // `head.token` borrows `polled`: every failure below that uses it comes before the free.
    const head = NexusTaskHead.read(polled.bytes()) orelse {
        log.err("a Nexus task on task queue {s} with no task token; it can't be completed", .{w.task_queue});
        polled.deinit();
        return null;
    };
    switch (head.kind) {
        .cancel => |reason| {
            deliverNexusCancel(w, running, head.token, reason);
            polled.deinit();
            return null;
        },
        .task => {},
    }
    const req = w.gpa.create(NexusRequest) catch {
        log.err("out of memory receiving a Nexus task on task queue {s}; failing the request as retryable", .{w.task_queue});
        failNexusTask(w, core, head.token, .{ .type = .internal, .retry = .retryable, .message = "the worker ran out of memory receiving the Nexus request" }, sink_logger);
        polled.deinit();
        return null;
    };
    req.* = .{
        .task_gpa = .{ .io = w.io, .backing = w.gpa },
        .arena = undefined,
        .polled = polled,
        .ctx = .{ .arena = undefined, .io_ = w.io, .info_ = .{ .service = "", .operation = "" }, .logger_ = sink_logger, .client_ = w.client, .codecs = w.client.codecs },
    };
    req.arena = .init(req.task_gpa.allocator());
    var keep = false;
    defer if (!keep) req.destroy(w.gpa);

    var reader: Io.Reader = .fixed(polled.bytes());
    const task = nexus_pb.NexusTask.decode(&reader, req.arena.allocator()) catch |err| {
        failUndecodableNexusTask(w, core, polled.bytes(), err);
        return null;
    };
    const request_task = if (task.variant) |v| switch (v) {
        .task => |t| t,
        .cancel_task => null,
    } else null;
    const polled_task = request_task orelse {
        log.err("a Nexus task on task queue {s} read as a request decoded as something else; failing it as a bad request", .{w.task_queue});
        failNexusTask(w, core, head.token, .{ .type = .bad_request, .retry = .non_retryable, .message = "the worker couldn't decode the Nexus request: it holds no request" }, sink_logger);
        return null;
    };
    req.task_token = polled_task.task_token;
    req.request = polled_task.request orelse .{};
    req.endpoint = if (task.endpoint.len > 0) task.endpoint else req.request.endpoint;
    req.deadline = proto_time.timestampFromProto(task.request_deadline);
    // Named here, before anything can fail the request, so every failure's log names
    // them; without memory for the fields it logs to the worker's sink without them.
    const info = &req.ctx.info_;
    if (req.request.variant) |v| switch (v) {
        .start_operation => |s| info.* = .{ .service = s.service, .operation = s.operation },
        .cancel_operation => |c| info.* = .{ .service = c.service, .operation = c.operation },
    };
    req.ctx.logger_ = sink_logger.with(req.arena.allocator(), .{
        .NexusService = info.service,
        .NexusOperation = info.operation,
        .NexusEndpoint = req.endpoint,
        .TaskQueue = w.task_queue,
    }) catch sink_logger;
    const tracked = tracked: {
        running.mutex.lockUncancelable(w.io);
        defer running.mutex.unlock(w.io);
        running.map.put(w.gpa, req.task_token, req) catch break :tracked false;
        break :tracked true;
    };
    if (tracked) {
        keep = true;
        return req;
    }
    // Untracked, the request couldn't be reached by a cancel from the core or the worker's
    // shutdown, so it doesn't run at all.
    log.err("out of memory tracking a Nexus request on task queue {s} for cancellation; failing it as retryable", .{w.task_queue});
    failNexusTask(w, core, head.token, .{ .type = .internal, .retry = .retryable, .message = "the worker ran out of memory tracking the Nexus request for cancellation" }, req.ctx.logger_);
    return null;
}

/// Fails a request whose task doesn't decode, so the core doesn't wait for it forever: its
/// task token is read from the raw bytes (the token is field 1 of the poll response, itself
/// field 1 of the task). A malformed task (for example a header that isn't UTF-8) fails as a
/// non-retryable bad request; one that ran out of memory decoding fails as a retryable
/// internal error, so the caller retries it. Uses no heap, since it may run out of memory.
fn failUndecodableNexusTask(w: *Worker, core: bridge.Worker, bytes: []const u8, err: anyerror) void {
    const token = if (runId(bytes)) |response| runId(response) else null;
    const t = token orelse return log.err("decoding a Nexus task on task queue {s} failed: {t}; it has no task token to complete it with", .{ w.task_queue, err });
    if (err == error.OutOfMemory)
        log.err("out of memory decoding a Nexus task on task queue {s}; failing the request as retryable", .{w.task_queue})
    else
        log.warn("decoding a Nexus task on task queue {s} failed: {t}; failing the request as a bad request", .{ w.task_queue, err });
    var message_buf: [512]u8 = undefined;
    const e = undecodableNexusError(err, w.task_queue, &message_buf);
    failNexusTask(w, core, t, e, .{ .sink = w.logSink() });
}

/// The handler error a Nexus task that failed to decode with `err` is failed with: retryable
/// for running out of memory, a non-retryable bad request otherwise. Its message is formatted
/// into `message_buf`, or left without the task queue when that doesn't fit.
fn undecodableNexusError(err: anyerror, task_queue: []const u8, message_buf: []u8) nexus.HandlerError {
    if (err == error.OutOfMemory) return .{
        .type = .internal,
        .retry = .retryable,
        .message = std.fmt.bufPrint(message_buf, "the worker on task queue {s} ran out of memory decoding the Nexus request", .{task_queue}) catch
            "the worker ran out of memory decoding the Nexus request",
    };
    return .{
        .type = .bad_request,
        .retry = .non_retryable,
        .message = std.fmt.bufPrint(message_buf, "the worker on task queue {s} couldn't decode the Nexus request: {t}", .{ task_queue, err }) catch
            "the worker couldn't decode the Nexus request",
    };
}

/// Runs a request's handler on a task of its own, waits for it to return or for a cancel,
/// cancels the task through its `std.Io` on a cancel, and sends the completion. Sending
/// happens here, not on the handler's task, so a late cancel can't interrupt it. Without a
/// thread for the handler the request fails as `resource_exhausted`, which the server
/// retries: run here, the handler couldn't be canceled.
fn superviseNexusRequest(w: *Worker, core: bridge.Worker, running: *RunningNexusRequests, req: *NexusRequest) void {
    if (io_tasks.concurrent(w.io, runNexusRequest, .{ w, req })) |started| {
        var task = started;
        req.wake.waitUncancelable(w.io);
        if (req.ctx.cancelReason() != null) task.cancel(w.io) else task.await(w.io);
    } else |_| req.outcome = .{ .failed = no_thread_for_nexus_request };
    finishNexusRequest(w, core, running, req);
}

/// Sends the completion of `req.outcome`, then forgets and frees the request.
fn finishNexusRequest(w: *Worker, core: bridge.Worker, running: *RunningNexusRequests, req: *NexusRequest) void {
    completeNexusRequest(w, core, req, req.outcome);
    running.mutex.lockUncancelable(w.io);
    _ = running.map.remove(req.task_token);
    running.mutex.unlock(w.io);
    req.destroy(w.gpa);
}

/// Runs the request into `req.outcome`. The outcome is stored rather than returned: its
/// alignment (16, from `Io.Duration`) is more than `std.Io.Threaded` futures place results at,
/// which overruns their allocation.
fn runNexusRequest(w: *Worker, req: *NexusRequest) void {
    defer req.wake.set(w.io);
    req.outcome = handleNexusRequest(w, req);
}

/// Builds the request's context, asks the worker's interceptors for the request's own, and
/// calls the operation's handler through them.
fn handleNexusRequest(w: *Worker, req: *NexusRequest) NexusOutcome {
    const a = req.arena.allocator();
    const ctx = &req.ctx;
    // The context already exists: a cancel may have set its reason.
    ctx.arena = a;
    const r = &req.request;
    const variant = r.variant orelse return .{ .failed = .{
        .type = .bad_request,
        .message = "the Nexus request is neither a start nor a cancel of an operation",
        .retry = .non_retryable,
    } };
    const service, const operation = switch (variant) {
        .start_operation => |s| .{ s.service, s.operation },
        .cancel_operation => |c| .{ c.service, c.operation },
    };
    const oom: NexusOutcome = .{ .failed = .{ .type = .internal, .message = "the worker ran out of memory handling the Nexus request" } };
    ctx.info_ = .{
        .service = service,
        .operation = operation,
        .headers = nexusHeaders(a, r.header.items) catch return oom,
        .endpoint = req.endpoint,
        .namespace = w.client.namespace,
        .task_queue = w.task_queue,
        .request_deadline = req.deadline,
    };
    ctx.metrics_ = w.client.metrics().with(.{
        .namespace = w.client.namespace,
        .task_queue = w.task_queue,
        .nexus_endpoint = req.endpoint,
        .nexus_service = service,
        .nexus_operation = operation,
    }) catch return oom;
    defer {
        ctx.metrics_.deinit();
        ctx.metrics_ = .noop;
    }
    if (variant == .start_operation) {
        const s = &variant.start_operation;
        ctx.info_.request_id = s.request_id;
        ctx.info_.callback_url = s.callback;
        ctx.info_.callback_headers = nexusHeaders(a, s.callback_header.items) catch return oom;
        var links: std.ArrayList(nexus.Link) = .empty;
        for (s.links.items) |link| {
            var why: []const u8 = "";
            const converted = wire.fromNexusLink(a, link, &why) catch |err| switch (err) {
                error.OutOfMemory => return oom,
                error.InvalidLink => {
                    ctx.logger_.warn("Dropping a link of the Nexus request that doesn't parse", .{ .Url = link.url, .Type = link.type, .Reason = why });
                    continue;
                },
            };
            links.append(a, converted) catch return oom;
        }
        ctx.info_.links = links.items;
    }
    // Read from the request as it arrived, before an inbound interceptor can change its headers.
    ctx.propagated_ = .init(a);
    if (w.client.propagators.len > 0) if (ctx.info_.header(propagation.nexus_header_name)) |value|
        propagation.extractNexusHeader(w.client.propagators, a, value, &ctx.propagated_) catch return oom;
    var inbound: std.ArrayList(interceptor.NexusOperationInbound) = .empty;
    var outbound: std.ArrayList(interceptor.NexusOperationOutbound) = .empty;
    for (w.interceptors) |wi| {
        const request = wi.interceptNexusOperation(ctx) catch return oom;
        if (request.inbound) |i| inbound.append(a, i) catch return oom;
        if (request.outbound) |o| outbound.append(a, o) catch return oom;
    }
    ctx.outbound = outbound.items;
    var call: NexusCall = .{ .w = w, .ctx = ctx };
    const next: interceptor.NexusOperationInbound.Next = .{ .chain = inbound.items, .root = .{ .ptr = &call, .vtable = &NexusCall.vtable } };
    switch (variant) {
        .start_operation => |s| return .{ .start = next.startOperation(.{
            .service = service,
            .operation = operation,
            .payload = s.payload,
            .headers = ctx.info_.headers,
        }) },
        .cancel_operation => |c| return .{ .cancel = next.cancelOperation(.{
            .service = service,
            .operation = operation,
            .token = if (c.operation_token.len > 0) c.operation_token else c.operation_id,
        }) },
    }
}

/// The end of a Nexus request's inbound interceptor chain: looks up the registered operation
/// by the names the interceptors pass on and runs its handler, with `ctx.info()` reporting
/// them.
const NexusCall = struct {
    w: *Worker,
    ctx: *nexus.Context,

    const vtable: interceptor.NexusOperationInbound.VTable = .{ .startOperation = start, .cancelOperation = cancel };

    fn start(ptr: *anyopaque, input: interceptor.StartNexusOperationInput, _: interceptor.NexusOperationInbound.Next) nexus.StartResult {
        const call: *NexusCall = @ptrCast(@alignCast(ptr));
        var missing: nexus.HandlerError = undefined;
        const bound = call.lookup(input.service, input.operation, &missing) orelse return .{ .handler_error = missing };
        call.ctx.info_.headers = input.headers;
        return bound.start(call.ctx, input.payload);
    }

    fn cancel(ptr: *anyopaque, input: interceptor.CancelNexusOperationInput, _: interceptor.NexusOperationInbound.Next) nexus.CancelResult {
        const call: *NexusCall = @ptrCast(@alignCast(ptr));
        var missing: nexus.HandlerError = undefined;
        const bound = call.lookup(input.service, input.operation, &missing) orelse return .{ .handler_error = missing };
        return bound.cancel(call.ctx, input.token);
    }

    /// The operation, with the context's service and operation set to its names; null with
    /// a `not_found` handler error in `missing` when it isn't registered.
    fn lookup(call: *NexusCall, service: []const u8, operation: []const u8, missing: *nexus.HandlerError) ?nexus.BoundOperation {
        const w = call.w;
        const a = call.ctx.arena;
        const bound = w.nexusOperation(service, operation) catch |err| {
            missing.* = .{
                .type = .not_found,
                .message = switch (err) {
                    error.ServiceNotFound => std.fmt.allocPrint(a, "Nexus service \"{s}\" is not registered on the worker for task queue {s} (looking for its operation \"{s}\")", .{ service, w.task_queue, operation }),
                    error.OperationNotFound => std.fmt.allocPrint(a, "Nexus service \"{s}\" has no operation \"{s}\" on the worker for task queue {s}", .{ service, operation, w.task_queue }),
                } catch "the Nexus service or operation is not registered on this worker",
                .retry = .non_retryable,
            };
            return null;
        };
        call.ctx.info_.service = service;
        call.ctx.info_.operation = operation;
        return bound;
    }
};

fn nexusHeaders(a: Allocator, entries: anytype) error{OutOfMemory}![]const nexus.Header {
    const out = try a.alloc(nexus.Header, entries.len);
    for (entries, out) |e, *h| h.* = .{ .name = e.key, .value = e.value };
    return out;
}

/// Turns a request's outcome into its completion and sends it. A handler the worker
/// canceled that gave up with `error.Canceled` acknowledges the cancel instead (see
/// `nexus.Context`).
fn completeNexusRequest(w: *Worker, core: bridge.Worker, req: *NexusRequest, outcome: NexusOutcome) void {
    const a = req.arena.allocator();
    const ctx = &req.ctx;
    const logger = ctx.logger_;
    const handler_error: ?nexus.HandlerError = switch (outcome) {
        .start => |r| if (r == .handler_error) r.handler_error else null,
        .cancel => |r| if (r == .handler_error) r.handler_error else null,
        .failed => |e| e,
    };
    if (handler_error) |e| {
        if (wire.canceledByWorker(ctx, e)) {
            logger.debug("Nexus handler canceled; acknowledging the cancel", .{ .Reason = @tagName(ctx.cancelReason().?) });
            return sendNexusCompletion(w, core, a, wire.ackCancel(req.task_token), logger);
        }
        logger.warn("Nexus request failed", .{ .Error = e.message, .Type = e.type.wireName(), .Retryable = e.retryable() });
    }
    if (outcome == .start and outcome.start == .operation_error)
        logger.debug("Nexus operation ended unsuccessfully", .{ .Error = outcome.start.operation_error.failure.message, .State = @tagName(outcome.start.operation_error.state) });
    var completion = switch (outcome) {
        .start => |r| wire.startCompletion(a, req.task_token, r, logger),
        .cancel => |r| wire.cancelCompletion(a, req.task_token, r),
        .failed => |e| wire.failedCompletion(a, req.task_token, e),
    } catch return dropNexusTask(w, core, req.task_token, logger);
    encodeNexusFailure(w, a, &completion, ctx) catch |err| {
        logger.warn("Preparing the Nexus request's failure (the client's failure converter, encoded attributes and codecs) failed; sending it without its details", .{ .Error = @errorName(err) });
        completion = wire.failedCompletion(a, req.task_token, .{
            .type = .internal,
            .message = std.fmt.allocPrint(a, "preparing the failure of Nexus operation \"{s}\" of service \"{s}\" with the client's failure converter, encoded attributes and codecs failed: {t}", .{ ctx.info_.operation, ctx.info_.service, err }) catch "preparing the Nexus request's failure with the client's failure converter, encoded attributes and codecs failed",
        }) catch return dropNexusTask(w, core, req.task_token, logger);
    };
    sendNexusCompletion(w, core, a, completion, logger);
}

/// Passes the completion's failure, if it has one, through the client's failure converter
/// (when the handler's own error built it), then encodes its attributes and payloads with the
/// client's codecs. A synchronous result is already encoded (by `nexus.Operation.start`).
fn encodeNexusFailure(w: *Worker, a: Allocator, completion: *wire.Completion, ctx: *const nexus.Context) converter.CodecError!void {
    const context: converter.Context = .{ .namespace = ctx.info_.namespace, .values = ctx.propagated() };
    const status = if (completion.status) |*st| st else return;
    const f: *failure_mod.Failure = switch (status.*) {
        .failure => |*f| f,
        .completed => |*response| f: {
            const variant = if (response.variant) |*v| v else return;
            const start = switch (variant.*) {
                .start_operation => |*so| so,
                else => return,
            };
            const outcome = if (start.variant) |*v| v else return;
            break :f switch (outcome.*) {
                .failure => |*f| f,
                else => return,
            };
        },
        else => return,
    };
    // Only a handler's own error is user code's; the SDK's reports go as they are.
    if (ctx.returned_error_) |err| if (w.client.failure_converter) |c| {
        f.* = try c.toFailure(a, &context, err, f.*);
    };
    if (w.client.encode_failure_attributes) try failure_mod.encodeAttributes(failure_mod.Failure, f, a);
    try converter.encodeMessage(failure_mod.Failure, f, w.client.codecs, a, w.io, &context);
}

fn sendNexusCompletion(w: *Worker, core: bridge.Worker, a: Allocator, completion: wire.Completion, logger: log_mod.Logger) void {
    var encoded: Io.Writer.Allocating = .init(a);
    completion.encode(&encoded.writer, a) catch return dropNexusTask(w, core, completion.task_token, logger);
    completeNexusTask(w, core, encoded.written(), logger);
}

/// Completes a task whose response couldn't be built for lack of memory, with a completion
/// that needs none: an acknowledged cancel, which the core takes for any task it is waiting
/// on and doesn't send to the server, so the caller's attempt times out and is retried.
fn dropNexusTask(w: *Worker, core: bridge.Worker, token: []const u8, logger: log_mod.Logger) void {
    var buffer: [wire.max_ack_cancel_len]u8 = undefined;
    const completion = wire.encodeAckCancel(&buffer, token) orelse
        return logger.err("Out of memory completing a Nexus request, and its task token is too long to acknowledge without memory; the core keeps the task, so the worker can't finish stopping", .{ .TaskTokenLength = token.len });
    logger.err("Out of memory completing a Nexus request; sending no response, so the caller's attempt times out and is retried", .{});
    completeNexusTask(w, core, completion, logger);
}

/// Hands an encoded completion to the core, which forgets a task whose completion it
/// receives, even one it then fails to deliver. The bridge allocates the cell it waits in
/// before the core sees the completion, so on `error.OutOfMemory` it is sent again with the
/// cell on the stack (safe for the reason `WorkflowRunner.sendCompletion` gives).
fn completeNexusTask(w: *Worker, core: bridge.Worker, completion: []const u8, logger: log_mod.Logger) void {
    var message: ?[]u8 = null;
    defer if (message) |m| w.gpa.free(m);
    core.completeNexusTask(w.gpa, w.io, completion, &message) catch |err| switch (err) {
        error.OutOfMemory => {
            var cell: [256]u8 = undefined;
            var fba: std.heap.FixedBufferAllocator = .init(&cell);
            core.completeNexusTask(fba.allocator(), w.io, completion, null) catch |e|
                logger.err("Completing a Nexus task failed", .{ .Error = @errorName(e) });
        },
        error.CompletionFailed => logger.err("Completing a Nexus task failed", .{ .Error = message orelse @errorName(err) }),
    };
}

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
        // The return type is an error set, not an error union, so `try` doesn't apply.
        const message = std.fmt.allocPrint(ctx.allocator(), "the session's worker stopped (host task queue {s})", .{h.host_queue}) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
        };
        return ctx.fail(.{
            .message = message,
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

test "a worker with autoscaling workflow and activity pollers runs a workflow and its activity" {
    const rt = try bridge.Runtime.init(testing.allocator, .{}, null);
    defer rt.deinit();
    var server = try bridge.EphemeralServer.startDevServer(testing.allocator, testing.io, rt, .{}, null);
    defer server.shutdown(testing.io, null) catch {};

    var client = try client_mod.Client.connect(testing.allocator, testing.io, .{ .host_port = server.target });
    defer client.close();

    var w = try Worker.init(testing.allocator, testing.io, &client, "autoscaling-pollers", .{
        .workflow_task_poller_behavior = .{ .autoscaling = .{ .minimum = 1, .maximum = 10, .initial = 2 } },
        .activity_task_poller_behavior = .{ .autoscaling = .{} },
    });
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
    var run = try client.executeWorkflow(HelloTest.Hello, .{ .id = "autoscaling_pollers", .task_queue = "autoscaling-pollers" }, .{"Temporal"});
    defer run.deinit();
    try testing.expectEqualStrings("Hello Temporal!", try run.get(arena.allocator()));
}

test "a worker on a client from init connects it and runs a workflow" {
    var server = try @import("testing.zig").DevServer.start(testing.allocator, testing.io, .{});
    defer server.stop(testing.io);
    var client = try client_mod.Client.init(testing.allocator, testing.io, server.clientOptions());
    defer client.close();

    var w = try Worker.init(testing.allocator, testing.io, &client, "lazy-hello-world", .{});
    defer w.deinit();
    try w.registerWorkflow(HelloTest.Hello);
    try w.registerActivity(HelloTest.Greet);
    var running = try testing.io.concurrent(HelloTest.runWorker, .{&w});
    defer {
        w.stop();
        running.await(testing.io) catch {};
    }
    try w.waitUntilRunning();
    try testing.expect(client.shared.connection != null);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var run = try client.executeWorkflow(HelloTest.Hello, .{ .id = "lazy_hello_world", .task_queue = "lazy-hello-world" }, .{"Temporal"});
    defer run.deinit();
    try testing.expectEqualStrings("Hello Temporal!", try run.get(arena.allocator()));
}

test "a worker on a client from init that can't connect returns ConnectFailed, and can run again" {
    // The failed connections are logged as warnings.
    testing.log_level = .err;
    var client = try client_mod.Client.init(testing.allocator, testing.io, .{ .host_port = "127.0.0.1:1", .core_log_level = null });
    defer client.close();
    var w = try Worker.init(testing.allocator, testing.io, &client, "lazy-unreachable", .{});
    defer w.deinit();
    try w.registerWorkflow(HelloTest.Hello);
    try testing.expectError(error.ConnectFailed, w.run());
    try testing.expectError(error.WorkerFailedToStart, w.waitUntilRunning());
    try testing.expectError(error.ConnectFailed, w.run());
    try testing.expect(client.shared.connection == null);
}

test "stopping a worker while its client from init makes its first connection returns from run at once" {
    // The abandoned connection is logged as a warning.
    testing.log_level = .err;
    // A listener that never accepts: connecting waits until the client gives up.
    const any: Io.net.IpAddress = try .parseIp4("127.0.0.1", 0);
    var silent = try any.listen(testing.io, .{});
    defer silent.deinit(testing.io);
    var buf: [32]u8 = undefined;
    const host_port = try std.fmt.bufPrint(&buf, "127.0.0.1:{d}", .{silent.socket.address.getPort()});
    var client = try client_mod.Client.init(testing.allocator, testing.io, .{ .host_port = host_port, .core_log_level = null, .rpc_timeout = .fromSeconds(20) });
    defer client.close();
    var w = try Worker.init(testing.allocator, testing.io, &client, "lazy-stopped-while-connecting", .{});
    defer w.deinit();
    try w.registerWorkflow(HelloTest.Hello);

    var running = try testing.io.concurrent(HelloTest.runWorker, .{&w});
    // Wait until `run` is making the client's first connection.
    while (client.shared.connect_mutex.tryLock()) client.shared.connect_mutex.unlock(testing.io);
    const start = Io.Clock.awake.now(testing.io);
    w.stop();
    const result = running.await(testing.io);
    const waited = start.durationTo(Io.Clock.awake.now(testing.io));
    // `stop` asks `run` to return, and `runUntilSignal` calls it on SIGTERM: it doesn't wait
    // out the first connection's deadline (20 s here, 30 s without `rpc_timeout`).
    try testing.expect(waited.nanoseconds < 5 * std.time.ns_per_s);
    // `run` returns success after `stop`.
    try result;
}

test "a worker stopped before run on a client from init returns from run at once" {
    // The abandoned connection is logged as a warning.
    testing.log_level = .err;
    // A listener that never accepts: connecting waits until the client gives up.
    const any: Io.net.IpAddress = try .parseIp4("127.0.0.1", 0);
    var silent = try any.listen(testing.io, .{});
    defer silent.deinit(testing.io);
    var buf: [32]u8 = undefined;
    const host_port = try std.fmt.bufPrint(&buf, "127.0.0.1:{d}", .{silent.socket.address.getPort()});
    var client = try client_mod.Client.init(testing.allocator, testing.io, .{ .host_port = host_port, .core_log_level = null, .rpc_timeout = .fromSeconds(20) });
    defer client.close();
    var w = try Worker.init(testing.allocator, testing.io, &client, "lazy-stopped-before-run", .{});
    defer w.deinit();
    try w.registerWorkflow(HelloTest.Hello);

    // `stop` is documented as safe before `run`, and a worker on a connected client then
    // returns success from `run` at once.
    w.stop();
    const start = Io.Clock.awake.now(testing.io);
    const result = w.run();
    const waited = start.durationTo(Io.Clock.awake.now(testing.io));
    try testing.expect(waited.nanoseconds < 5 * std.time.ns_per_s);
    try result;
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

const NexusTest = struct {
    const Echo = nexus.OperationDef("echo", []const u8, []const u8);
    const Count = nexus.OperationDef("count", void, u32);
    const Service = nexus.ServiceDef("nexus-test", .{ Echo, Count });

    fn echo(ctx: *nexus.Context, input: []const u8) ![]const u8 {
        return std.fmt.allocPrint(ctx.allocator(), "{s}: {s}", .{ ctx.info().service, input });
    }
    const Handler = nexus.ServiceHandler(Service, .{
        nexus.OperationHandler(Echo, echo),
        nexus.OperationHandler(Count, Counter.count),
    });

    const Other = nexus.ServiceDef("nexus-other", .{Echo});
    const OtherHandler = nexus.ServiceHandler(Other, .{nexus.OperationHandler(Echo, echo)});
    const SameName = nexus.ServiceHandler(nexus.ServiceDef("nexus-test", .{Echo}), .{nexus.OperationHandler(Echo, echo)});
};

test "registered Nexus services are found by service and operation name, once each" {
    var client: client_mod.Client = undefined; // registration never touches the client
    var w = try Worker.init(testing.allocator, testing.io, &client, "nexus", .{});
    defer w.deinit();
    var counter: Counter = .{ .step = 7 };
    try w.registerNexusServiceWith(NexusTest.Handler, &counter);
    try w.registerNexusService(NexusTest.OtherHandler);
    try testing.expectError(error.AlreadyRegistered, w.registerNexusService(NexusTest.SameName));
    try testing.expectError(error.AlreadyRegistered, w.registerNexusServiceWith(NexusTest.Handler, &counter));
    try testing.expectError(error.ServiceNotFound, w.nexusOperation("no-such-service", "echo"));
    try testing.expectError(error.OperationNotFound, w.nexusOperation("nexus-test", "no-such-operation"));
    try testing.expectError(error.OperationNotFound, w.nexusOperation("nexus-other", "count"));

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var ctx: nexus.Context = .{ .arena = a, .io_ = testing.io, .info_ = .{ .service = "nexus-test", .operation = "echo" } };
    const echoed = (try w.nexusOperation("nexus-test", "echo")).start(&ctx, try converter.toPayload(a, @as([]const u8, "hi")));
    try testing.expectEqualStrings("\"nexus-test: hi\"", echoed.succeeded.result.data);
    const counted = (try w.nexusOperation("nexus-test", "count")).start(&ctx, null);
    try testing.expectEqualStrings("7", counted.succeeded.result.data);
    try testing.expect(!w.hasWorkflows() and !w.hasActivities() and w.hasNexusServices());
}

const Counter = struct {
    step: u32,

    fn count(c: *Counter, ctx: *nexus.Context) !u32 {
        _ = ctx;
        return c.step;
    }

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

test "worker options map to the core options of the task queue" {
    const options: Options = .{
        .identity = "id-1",
        .max_cached_workflows = 7,
        .max_concurrent_workflow_task_executions = 11,
        .max_concurrent_activity_executions = 12,
        .max_concurrent_local_activity_executions = 13,
        .max_concurrent_nexus_task_executions = 14,
        .max_concurrent_workflow_task_pollers = 3,
        .max_concurrent_activity_task_pollers = 4,
        .max_concurrent_nexus_task_pollers = 5,
        .sticky_schedule_to_start_timeout = .fromMilliseconds(2500),
        .max_activities_per_second = 1.5,
        .max_task_queue_activities_per_second = 2.5,
        .default_heartbeat_throttle_interval = .fromSeconds(4),
        .max_heartbeat_throttle_interval = .fromSeconds(9),
        .max_eager_activity_reservations_per_workflow_task = 0,
        .poll_activity_tasks = false,
        .graceful_shutdown_period = .fromSeconds(6),
    };
    const all: Worker.Registered = .{ .workflows = true, .activities = true, .nexus = true };
    try Worker.checkOptions(options, "mapped", all);
    const core = Worker.coreOptions(options, "ns", "mapped", .{ .none = .{ .build_id = "b" } }, all);
    try testing.expectEqualStrings("ns", core.namespace);
    try testing.expectEqualStrings("mapped", core.task_queue);
    try testing.expectEqualStrings("b", core.versioning.none.build_id);
    try testing.expectEqualStrings("id-1", core.identity_override);
    try testing.expectEqual(7, core.max_cached_workflows);
    try testing.expectEqual(11, core.tuner.workflow.fixed);
    try testing.expectEqual(12, core.tuner.activity.fixed);
    try testing.expectEqual(13, core.tuner.local_activity.fixed);
    try testing.expectEqual(14, core.tuner.nexus.fixed);
    try testing.expectEqual(3, core.workflow_task_pollers.simple_maximum);
    try testing.expectEqual(4, core.activity_task_pollers.simple_maximum);
    try testing.expectEqual(5, core.nexus_task_pollers.simple_maximum);
    try testing.expectEqual(2500, core.sticky_queue_schedule_to_start_timeout_ms);
    try testing.expectEqual(1.5, core.max_activities_per_second);
    try testing.expectEqual(2.5, core.max_task_queue_activities_per_second);
    try testing.expectEqual(4000, core.default_heartbeat_throttle_interval_ms);
    try testing.expectEqual(9000, core.max_heartbeat_throttle_interval_ms);
    try testing.expectEqual(0, core.max_eager_activity_reservations_per_workflow_task);
    try testing.expectEqual(6000, core.graceful_shutdown_period_ms);
    // Activities still run, as local activities of the worker's workflows.
    try testing.expectEqual(bridge.Worker.Options.TaskTypes{ .workflows = true, .local_activities = true, .remote_activities = false, .nexus = true }, core.task_types);
}

test "default worker options give the core its own defaults" {
    const defaults: bridge.Worker.Options = .{ .namespace = "ns", .task_queue = "q" };
    const all: Worker.Registered = .{ .workflows = true, .activities = true, .nexus = false };
    try Worker.checkOptions(.{}, "q", all);
    const core = Worker.coreOptions(.{}, "ns", "q", .{ .none = .{} }, all);
    try testing.expectEqual(defaults.sticky_queue_schedule_to_start_timeout_ms, core.sticky_queue_schedule_to_start_timeout_ms);
    try testing.expectEqual(defaults.default_heartbeat_throttle_interval_ms, core.default_heartbeat_throttle_interval_ms);
    try testing.expectEqual(defaults.max_heartbeat_throttle_interval_ms, core.max_heartbeat_throttle_interval_ms);
    try testing.expectEqual(defaults.max_activities_per_second, core.max_activities_per_second);
    try testing.expectEqual(defaults.max_task_queue_activities_per_second, core.max_task_queue_activities_per_second);
    try testing.expectEqual(defaults.max_eager_activity_reservations_per_workflow_task, core.max_eager_activity_reservations_per_workflow_task);
    try testing.expectEqual(defaults.tuner.local_activity.fixed, core.tuner.local_activity.fixed);
    try testing.expectEqual(defaults.graceful_shutdown_period_ms, core.graceful_shutdown_period_ms);
    try testing.expectEqual(1000, core.max_cached_workflows);
    try testing.expectEqual(100, core.tuner.workflow.fixed);
    try testing.expectEqual(100, core.tuner.activity.fixed);
    try testing.expectEqual(100, core.tuner.nexus.fixed);
    try testing.expectEqual(2, core.workflow_task_pollers.simple_maximum);
    try testing.expectEqual(2, core.activity_task_pollers.simple_maximum);
    try testing.expectEqual(1, core.nexus_task_pollers.simple_maximum);
    try testing.expectEqual(bridge.Worker.Options.TaskTypes{ .workflows = true, .local_activities = true, .remote_activities = true, .nexus = false }, core.task_types);
}

test "poller behaviors and the poll ratio reach the core options of all three poller kinds" {
    const autoscaling: PollerBehavior = .{ .autoscaling = .{ .minimum = 2, .maximum = 20, .initial = 4 } };
    const options: Options = .{
        .max_concurrent_workflow_task_pollers = 3,
        .max_concurrent_activity_task_pollers = 4,
        .max_concurrent_nexus_task_pollers = 5,
        .workflow_task_poller_behavior = autoscaling,
        .activity_task_poller_behavior = .{ .autoscaling = .{} },
        .nexus_task_poller_behavior = .{ .autoscaling = .{ .minimum = 1, .maximum = 1, .initial = 1 } },
        .nonsticky_to_sticky_poll_ratio = 0.5,
    };
    const all: Worker.Registered = .{ .workflows = true, .activities = true, .nexus = true };
    try Worker.checkOptions(options, "q", all);
    const core = Worker.coreOptions(options, "ns", "q", .{ .none = .{} }, all);
    try testing.expectEqual(bridge.Worker.Options.PollerBehavior{ .autoscaling = .{ .minimum = 2, .maximum = 20, .initial = 4 } }, core.workflow_task_pollers);
    try testing.expectEqual(bridge.Worker.Options.PollerBehavior{ .autoscaling = .{ .minimum = 1, .maximum = 100, .initial = 5 } }, core.activity_task_pollers);
    try testing.expectEqual(bridge.Worker.Options.PollerBehavior{ .autoscaling = .{ .minimum = 1, .maximum = 1, .initial = 1 } }, core.nexus_task_pollers);
    try testing.expectEqual(0.5, core.nonsticky_to_sticky_poll_ratio);

    // Session queues poll the way the main queue polls activities.
    const session = Worker.sessionCoreOptions(core, "q@host", 42);
    try testing.expectEqual(core.activity_task_pollers, session.activity_task_pollers);

    // A simple maximum overrides the legacy field too.
    const simple: Options = .{
        .max_concurrent_workflow_task_pollers = 3,
        .max_concurrent_activity_task_pollers = 4,
        .max_concurrent_nexus_task_pollers = 5,
        .workflow_task_poller_behavior = .{ .simple_maximum = 7 },
        .activity_task_poller_behavior = .{ .simple_maximum = 8 },
        .nexus_task_poller_behavior = .{ .simple_maximum = 9 },
    };
    try Worker.checkOptions(simple, "q", all);
    const simple_core = Worker.coreOptions(simple, "ns", "q", .{ .none = .{} }, all);
    try testing.expectEqual(bridge.Worker.Options.PollerBehavior{ .simple_maximum = 7 }, simple_core.workflow_task_pollers);
    try testing.expectEqual(bridge.Worker.Options.PollerBehavior{ .simple_maximum = 8 }, simple_core.activity_task_pollers);
    try testing.expectEqual(bridge.Worker.Options.PollerBehavior{ .simple_maximum = 9 }, simple_core.nexus_task_pollers);
    try testing.expectEqual(0.2, simple_core.nonsticky_to_sticky_poll_ratio);

    // Without a Nexus service the core still gets its one Nexus poller, whatever is set.
    const no_nexus: Worker.Registered = .{ .workflows = true, .activities = true, .nexus = false };
    try Worker.checkOptions(options, "q", no_nexus);
    const without = Worker.coreOptions(options, "ns", "q", .{ .none = .{} }, no_nexus);
    try testing.expectEqual(bridge.Worker.Options.PollerBehavior{ .simple_maximum = 1 }, without.nexus_task_pollers);
    try testing.expectEqual(core.workflow_task_pollers, without.workflow_task_pollers);
    try testing.expectEqual(core.activity_task_pollers, without.activity_task_pollers);
}

test "invalid poller behaviors and poll ratios are rejected, whatever is registered" {
    const registrations = [_]Worker.Registered{
        .{ .workflows = true, .activities = true, .nexus = true },
        .{ .workflows = false, .activities = false, .nexus = false },
    };
    const bad = [_]PollerBehavior{
        .{ .simple_maximum = 0 },
        .{ .autoscaling = .{ .minimum = 0, .maximum = 10, .initial = 1 } },
        .{ .autoscaling = .{ .minimum = 0, .maximum = 0, .initial = 0 } },
        .{ .autoscaling = .{ .minimum = 5, .maximum = 4, .initial = 4 } },
        .{ .autoscaling = .{ .minimum = 2, .maximum = 10, .initial = 1 } },
        .{ .autoscaling = .{ .minimum = 2, .maximum = 10, .initial = 11 } },
    };
    for (registrations) |registered| {
        for (bad) |b| {
            try testing.expectError(error.InvalidOptions, Worker.checkOptions(.{ .workflow_task_poller_behavior = b }, "q", registered));
            try testing.expectError(error.InvalidOptions, Worker.checkOptions(.{ .activity_task_poller_behavior = b }, "q", registered));
            try testing.expectError(error.InvalidOptions, Worker.checkOptions(.{ .nexus_task_poller_behavior = b }, "q", registered));
        }
        for ([_]f32{ -0.01, 1.01, std.math.nan(f32), std.math.inf(f32), -std.math.inf(f32) }) |ratio| {
            try testing.expectError(error.InvalidOptions, Worker.checkOptions(.{ .nonsticky_to_sticky_poll_ratio = ratio }, "q", registered));
        }

        const good = [_]PollerBehavior{
            .{ .simple_maximum = 1 },
            .{ .autoscaling = .{} },
            .{ .autoscaling = .{ .minimum = 1, .maximum = 1, .initial = 1 } },
            .{ .autoscaling = .{ .minimum = 2, .maximum = 10, .initial = 2 } },
            .{ .autoscaling = .{ .minimum = 2, .maximum = 10, .initial = 10 } },
        };
        for (good) |g| try Worker.checkOptions(.{ .workflow_task_poller_behavior = g, .activity_task_poller_behavior = g, .nexus_task_poller_behavior = g }, "q", registered);
        for ([_]f32{ 0, 1, 0.5 }) |ratio| try Worker.checkOptions(.{ .nonsticky_to_sticky_poll_ratio = ratio }, "q", registered);
    }
}

test "session queues get the heartbeat throttling but not the per-second limits" {
    const options: Options = .{
        .identity = "id-1",
        .max_concurrent_activity_task_pollers = 4,
        .max_activities_per_second = 1.5,
        .max_task_queue_activities_per_second = 2.5,
        .default_heartbeat_throttle_interval = .fromSeconds(4),
        .max_heartbeat_throttle_interval = .fromSeconds(9),
        .graceful_shutdown_period = .fromSeconds(6),
        .sessions = .{},
    };
    const registered: Worker.Registered = .{ .workflows = false, .activities = true, .nexus = false };
    try Worker.checkOptions(options, "q", registered);
    const session = Worker.sessionCoreOptions(Worker.coreOptions(options, "ns", "q", .{ .none = .{} }, registered), "q@host", 42);
    try testing.expectEqualStrings("ns", session.namespace);
    try testing.expectEqualStrings("q@host", session.task_queue);
    try testing.expectEqualStrings("id-1", session.identity_override);
    try testing.expectEqual(42, session.tuner.activity.fixed);
    try testing.expectEqual(4, session.activity_task_pollers.simple_maximum);
    try testing.expectEqual(4000, session.default_heartbeat_throttle_interval_ms);
    try testing.expectEqual(9000, session.max_heartbeat_throttle_interval_ms);
    try testing.expectEqual(6000, session.graceful_shutdown_period_ms);
    try testing.expectEqual(0, session.max_activities_per_second);
    try testing.expectEqual(0, session.max_task_queue_activities_per_second);
    try testing.expectEqual(bridge.Worker.Options.TaskTypes{ .workflows = false, .local_activities = false, .remote_activities = true, .nexus = false }, session.task_types);
}

test "a per-worker activity rate turns eager activities off; without one the reservations pass through" {
    const all: Worker.Registered = .{ .workflows = true, .activities = true, .nexus = false };
    const limited = Worker.coreOptions(.{ .max_activities_per_second = 1, .max_eager_activity_reservations_per_workflow_task = 5 }, "ns", "q", .{ .none = .{} }, all);
    try testing.expectEqual(0, limited.max_eager_activity_reservations_per_workflow_task);
    const unlimited = Worker.coreOptions(.{ .max_eager_activity_reservations_per_workflow_task = 5, .max_task_queue_activities_per_second = 1 }, "ns", "q", .{ .none = .{} }, all);
    try testing.expectEqual(5, unlimited.max_eager_activity_reservations_per_workflow_task);
}

test "durations the core takes in milliseconds are truncated, and saturate when too long" {
    try testing.expectEqual(1, Worker.millis(.fromMicroseconds(1999)));
    try testing.expectEqual(0, Worker.millis(.zero));
    try testing.expectEqual(std.math.maxInt(u64), Worker.millis(.max));
}

test "invalid worker options are rejected before the worker starts" {
    const all: Worker.Registered = .{ .workflows = true, .activities = true, .nexus = false };
    const invalid = [_]Options{
        .{ .max_activities_per_second = 0 },
        .{ .max_activities_per_second = -1 },
        .{ .max_activities_per_second = std.math.nan(f64) },
        .{ .max_activities_per_second = std.math.inf(f64) },
        // The core's poll spacing, 1/rate seconds, would overflow.
        .{ .max_activities_per_second = 1e-25 },
        .{ .max_activities_per_second = 0x1p-64 },
        // Two spacings past now would overflow the core's monotonic instant.
        .{ .max_activities_per_second = 1e-19 },
        .{ .max_activities_per_second = 1 / Worker.max_poll_spacing_seconds },
        .{ .max_task_queue_activities_per_second = 0 },
        .{ .max_task_queue_activities_per_second = -0.5 },
        .{ .max_task_queue_activities_per_second = std.math.nan(f64) },
        .{ .sticky_schedule_to_start_timeout = .fromMilliseconds(-1) },
        .{ .default_heartbeat_throttle_interval = .fromMilliseconds(-1) },
        .{ .max_heartbeat_throttle_interval = .fromSeconds(-1) },
        .{ .default_heartbeat_throttle_interval = .fromSeconds(61) },
        .{ .default_heartbeat_throttle_interval = .fromSeconds(5), .max_heartbeat_throttle_interval = .fromSeconds(4) },
        .{ .poll_activity_tasks = false, .sessions = .{} },
        .{ .workflow_stack_size = 0 },
        .{ .workflow_stack_size = min_workflow_stack_size - 1 },
        .{ .workflow_stack_size = std.math.maxInt(usize) },
    };
    for (invalid) |options| try testing.expectError(error.InvalidOptions, Worker.checkOptions(options, "q", all));
    // Activities that could run only as local activities need a workflow to call them.
    try testing.expectError(error.InvalidOptions, Worker.checkOptions(.{ .poll_activity_tasks = false }, "q", .{ .workflows = false, .activities = true, .nexus = false }));

    const valid = [_]Options{
        .{ .max_activities_per_second = 0.001, .max_task_queue_activities_per_second = 1e6 },
        .{ .max_activities_per_second = 1e-18, .max_task_queue_activities_per_second = 1e-25 },
        .{ .max_activities_per_second = 0x1.0000000000001p-61 },
        .{ .sticky_schedule_to_start_timeout = .zero, .default_heartbeat_throttle_interval = .zero, .max_heartbeat_throttle_interval = .zero },
        .{ .default_heartbeat_throttle_interval = .fromSeconds(60) },
        .{ .poll_activity_tasks = false },
        .{ .workflow_stack_size = min_workflow_stack_size },
    };
    for (valid) |options| try Worker.checkOptions(options, "q", all);
    try Worker.checkOptions(.{ .poll_activity_tasks = false }, "q", .{ .workflows = false, .activities = false, .nexus = true });
}

test "run returns InvalidOptions for an invalid option, before it uses the client" {
    var client: client_mod.Client = undefined; // rejected before the client is used
    const invalid = [_]Options{
        .{ .max_activities_per_second = std.math.nan(f64) },
        .{ .max_heartbeat_throttle_interval = .fromSeconds(-1) },
        .{ .poll_activity_tasks = false, .sessions = .{} },
        .{ .workflow_task_poller_behavior = .{ .simple_maximum = 0 } },
        .{ .activity_task_poller_behavior = .{ .autoscaling = .{ .minimum = 3, .maximum = 2, .initial = 2 } } },
        .{ .nexus_task_poller_behavior = .{ .autoscaling = .{ .initial = 101 } } },
        .{ .nonsticky_to_sticky_poll_ratio = std.math.nan(f32) },
        .{ .workflow_stack_size = 0 },
    };
    for (invalid) |options| {
        var w = try Worker.init(testing.allocator, testing.io, &client, "invalid", options);
        defer w.deinit();
        try w.registerWorkflow(HelloTest.Hello);
        try w.registerActivity(HelloTest.Greet);
        try testing.expectError(error.InvalidOptions, w.run());
        try testing.expectError(error.WorkerFailedToStart, w.waitUntilRunning());
    }
}

const LocalOnlyTest = struct {
    var active: std.atomic.Value(u32) = .init(0);
    var most_active: std.atomic.Value(u32) = .init(0);

    fn track(ctx: *activity.Context, name: []const u8) ![]const u8 {
        const now = active.fetchAdd(1, .acq_rel) + 1;
        _ = most_active.fetchMax(now, .acq_rel);
        defer _ = active.fetchSub(1, .acq_rel);
        try ctx.io().sleep(.fromMilliseconds(300), .awake);
        return std.fmt.allocPrint(ctx.allocator(), "Hello {s}!", .{name});
    }
    const Track = activity.Def("Track", track);

    fn twoLocal(ctx: *workflow.Context) ![]const u8 {
        const options: workflow.LocalActivityOptions = .{ .start_to_close_timeout = .fromSeconds(10) };
        const a = ctx.executeLocalActivity(Track, options, .{"a"});
        const b = ctx.executeLocalActivity(Track, options, .{"b"});
        return std.fmt.allocPrint(ctx.allocator(), "{s} {s}", .{ try a.get(), try b.get() });
    }
    const TwoLocal = workflow.Def("TwoLocal", twoLocal);

    /// Runs `Track` as a local activity, then as a regular one, and reports how each ended.
    fn localThenRemote(ctx: *workflow.Context) ![]const u8 {
        const local = try ctx.executeLocalActivity(Track, .{ .start_to_close_timeout = .fromSeconds(10) }, .{"local"}).get();
        const remote = ctx.executeActivity(Track, .{
            .schedule_to_start_timeout = .fromSeconds(1),
            .start_to_close_timeout = .fromSeconds(10),
        }, .{"remote"});
        const outcome: []const u8 = if (remote.get()) |_| "completed" else |err| switch (err) {
            error.ActivityFailed => outcome: {
                const cause = failure_mod.rootCause(remote.failure().?);
                const info = cause.failure_info orelse break :outcome "failed";
                break :outcome switch (info) {
                    .timeout_failure_info => |t| @tagName(t.timeout_type),
                    else => "failed",
                };
            },
            else => |e| return e,
        };
        return std.fmt.allocPrint(ctx.allocator(), "{s}; remote: {s}", .{ local, outcome });
    }
    const LocalThenRemote = workflow.Def("LocalThenRemote", localThenRemote);

    fn start(task_queue: []const u8, options: Options, comptime Workflow: type) ![]const u8 {
        active.store(0, .release);
        most_active.store(0, .release);
        const rt = try bridge.Runtime.init(testing.allocator, .{}, null);
        defer rt.deinit();
        var server = try bridge.EphemeralServer.startDevServer(testing.allocator, testing.io, rt, .{}, null);
        defer server.shutdown(testing.io, null) catch {};
        var client = try client_mod.Client.connect(testing.allocator, testing.io, .{ .host_port = server.target });
        defer client.close();

        var w = try Worker.init(testing.allocator, testing.io, &client, task_queue, options);
        defer w.deinit();
        try w.registerWorkflow(Workflow);
        try w.registerActivity(Track);
        var running = try testing.io.concurrent(HelloTest.runWorker, .{&w});
        defer {
            w.stop();
            running.await(testing.io) catch {};
        }
        try w.waitUntilRunning();

        var run = try client.executeWorkflow(Workflow, .{ .task_queue = task_queue }, .{});
        defer run.deinit();
        var arena: std.heap.ArenaAllocator = .init(testing.allocator);
        defer arena.deinit();
        return testing.allocator.dupe(u8, try run.get(arena.allocator()));
    }
};

test "one local activity slot runs local activities one at a time" {
    const result = try LocalOnlyTest.start("one-local-slot", .{ .max_concurrent_local_activity_executions = 1 }, LocalOnlyTest.TwoLocal);
    defer testing.allocator.free(result);
    try testing.expectEqualStrings("Hello a! Hello b!", result);
    try testing.expectEqual(1, LocalOnlyTest.most_active.load(.acquire));
}

test "a worker that doesn't poll activity tasks runs its activities only as local activities" {
    const result = try LocalOnlyTest.start("local-activities-only", .{ .poll_activity_tasks = false }, LocalOnlyTest.LocalThenRemote);
    defer testing.allocator.free(result);
    try testing.expectEqualStrings("Hello local!; remote: TIMEOUT_TYPE_SCHEDULE_TO_START", result);
}

const RateLimitTest = struct {
    /// When each `Stamp` started, on the awake clock, in start order.
    var started_at: [5]i96 = @splat(0);
    var started: std.atomic.Value(usize) = .init(0);

    fn stamp(ctx: *activity.Context, n: u32) !u32 {
        const i = started.fetchAdd(1, .acq_rel);
        if (i < started_at.len) started_at[i] = Io.Timestamp.now(ctx.io(), .awake).nanoseconds;
        return n;
    }
    const Stamp = activity.Def("Stamp", stamp);

    /// Schedules five activities on the worker's own task queue at once (eager execution
    /// requested, the default) and waits for all of them.
    fn fiveAtOnce(ctx: *workflow.Context) !u32 {
        const options: workflow.ActivityOptions = .{ .start_to_close_timeout = .fromSeconds(10) };
        var pending: [5]@TypeOf(ctx.executeActivity(Stamp, options, .{@as(u32, 0)})) = undefined;
        for (&pending, 0..) |*p, i| p.* = ctx.executeActivity(Stamp, options, .{@as(u32, @intCast(i))});
        var sum: u32 = 0;
        for (&pending) |*p| sum += try p.get();
        return sum;
    }
    const FiveAtOnce = workflow.Def("FiveAtOnce", fiveAtOnce);
};

test "max_activities_per_second also limits activities a workflow task asks to start eagerly" {
    RateLimitTest.started.store(0, .release);
    const rt = try bridge.Runtime.init(testing.allocator, .{}, null);
    defer rt.deinit();
    var server = try bridge.EphemeralServer.startDevServer(testing.allocator, testing.io, rt, .{}, null);
    defer server.shutdown(testing.io, null) catch {};
    var client = try client_mod.Client.connect(testing.allocator, testing.io, .{ .host_port = server.target });
    defer client.close();

    // One activity per second, through one poller, with the default eager reservations.
    var w = try Worker.init(testing.allocator, testing.io, &client, "one-activity-per-second", .{
        .max_activities_per_second = 1,
        .max_concurrent_activity_task_pollers = 1,
    });
    defer w.deinit();
    try w.registerWorkflow(RateLimitTest.FiveAtOnce);
    try w.registerActivity(RateLimitTest.Stamp);
    var running = try testing.io.concurrent(HelloTest.runWorker, .{&w});
    defer {
        w.stop();
        running.await(testing.io) catch {};
    }
    try w.waitUntilRunning();

    var run = try client.executeWorkflow(RateLimitTest.FiveAtOnce, .{ .task_queue = "one-activity-per-second", .execution_timeout = .fromSeconds(30) }, .{});
    defer run.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    try testing.expectEqual(0 + 1 + 2 + 3 + 4, try run.get(arena.allocator()));
    try testing.expectEqual(5, RateLimitTest.started.load(.acquire));

    // At one per second, five activities can't all start within two seconds, even counting
    // the poll already in flight when they are scheduled. Eagerly started ones bypass the
    // poll limiter, so without this they all start within milliseconds.
    const spread: Io.Duration = .{ .nanoseconds = RateLimitTest.started_at[4] - RateLimitTest.started_at[0] };
    try testing.expect(spread.nanoseconds >= Io.Duration.fromSeconds(2).nanoseconds);
}

test "a max_activities_per_second too small for the core's poll spacing is rejected or still runs the first activity" {
    // One activity per 1e19 seconds passes the 2^64-second check, but the core adds the
    // poll spacing to a monotonic instant with signed 64-bit seconds, which overflows from
    // about 2^63 seconds: the core's activity poller panics, and no activity ever starts.
    const rt = try bridge.Runtime.init(testing.allocator, .{}, null);
    defer rt.deinit();
    var server = try bridge.EphemeralServer.startDevServer(testing.allocator, testing.io, rt, .{}, null);
    defer server.shutdown(testing.io, null) catch {};
    var client = try client_mod.Client.connect(testing.allocator, testing.io, .{ .host_port = server.target });
    defer client.close();

    var w = try Worker.init(testing.allocator, testing.io, &client, "one-activity-per-1e19-seconds", .{ .max_activities_per_second = 1e-19 });
    defer w.deinit();
    try w.registerWorkflow(HelloTest.Hello);
    try w.registerActivity(HelloTest.Greet);
    var running = try testing.io.concurrent(HelloTest.runWorker, .{&w});
    var awaited = false;
    defer if (!awaited) {
        w.stop();
        running.await(testing.io) catch {};
    };
    w.waitUntilRunning() catch |err| {
        // Rejecting the rate up front fixes it too.
        try testing.expectEqual(error.WorkerFailedToStart, err);
        awaited = true;
        try testing.expectError(error.InvalidOptions, running.await(testing.io));
        return;
    };

    // Accepted, the limit spaces the polls after the first, which goes at once.
    var run = try client.executeWorkflow(HelloTest.Hello, .{ .task_queue = "one-activity-per-1e19-seconds", .execution_timeout = .fromSeconds(10) }, .{"rate"});
    defer run.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    try testing.expectEqualStrings("Hello rate!", try run.get(arena.allocator()));
}

test "the smallest max_activities_per_second accepted still runs the first activity" {
    // Just inside the bound: the core computes two poll spacings, almost 2^62 seconds, past
    // now without overflowing, and the first poll goes at once.
    const rate: f64 = 0x1.0000000000001p-61;
    try testing.expect(1 / rate < Worker.max_poll_spacing_seconds);
    const rt = try bridge.Runtime.init(testing.allocator, .{}, null);
    defer rt.deinit();
    var server = try bridge.EphemeralServer.startDevServer(testing.allocator, testing.io, rt, .{}, null);
    defer server.shutdown(testing.io, null) catch {};
    var client = try client_mod.Client.connect(testing.allocator, testing.io, .{ .host_port = server.target });
    defer client.close();

    var w = try Worker.init(testing.allocator, testing.io, &client, "slowest-accepted-rate", .{ .max_activities_per_second = rate });
    defer w.deinit();
    try w.registerWorkflow(HelloTest.Hello);
    try w.registerActivity(HelloTest.Greet);
    var running = try testing.io.concurrent(HelloTest.runWorker, .{&w});
    defer {
        w.stop();
        running.await(testing.io) catch {};
    }
    try w.waitUntilRunning();

    var run = try client.executeWorkflow(HelloTest.Hello, .{ .task_queue = "slowest-accepted-rate", .execution_timeout = .fromSeconds(10) }, .{"rate"});
    defer run.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    try testing.expectEqualStrings("Hello rate!", try run.get(arena.allocator()));
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

// An activity task that ran out of memory must still be completed: the core holds its slot
// until it is, and doesn't time it out.
const OutOfMemoryTest = struct {
    /// A result large enough that encoding it needs memory of its own.
    fn fill(ctx: *activity.Context, len: u32) ![]const u8 {
        const result = try ctx.allocator().alloc(u8, len);
        @memset(result, 'x');
        return result;
    }
    const Fill = activity.Def("OutOfMemoryFill", fill);
    const result_len = 64 * 1024;

    const task_queue = "out-of-memory-activities";

    fn start(client: *client_mod.Client, id: []const u8) !client_mod.ActivityHandle([]const u8) {
        return client.executeActivity(Fill, .{
            .id = id,
            .task_queue = task_queue,
            .start_to_close_timeout = .fromSeconds(30),
            .retry_policy = .{ .maximum_attempts = 1 },
        }, .{result_len});
    }

    /// A poll of `core` that the test gives up on after 10 s, shutting the core down, so a
    /// slot that is never released fails the test instead of hanging it.
    const BoundedPoll = struct {
        core: bridge.Worker,
        polled: ?bridge.Worker.Polled = null,
        done: Io.Event = .unset,

        fn run(p: *BoundedPoll) void {
            defer p.done.set(testing.io);
            p.polled = p.core.pollActivityTask(testing.allocator, testing.io, null) catch null;
        }
    };

    fn poll(core: bridge.Worker, shut_down: *bool) !bridge.Worker.Polled {
        var p: BoundedPoll = .{ .core = core };
        var future = try testing.io.concurrent(BoundedPoll.run, .{&p});
        p.done.waitTimeout(testing.io, .{ .duration = .{ .raw = .fromSeconds(10), .clock = .awake } }) catch {
            std.debug.print("no activity task within 10 s: a slot was never released\n", .{});
            core.initiateShutdown();
            shut_down.* = true;
            future.await(testing.io);
            if (p.polled) |polled| complete(core, polled);
            return error.TestUnexpectedResult;
        };
        future.await(testing.io);
        return p.polled orelse error.TestUnexpectedResult;
    }

    /// A `FailingAllocator` that also counts the allocations it refused. The sweep can't tell
    /// which allocation fails from its index alone: how many allocations a task takes varies
    /// from one iteration to the next (the running map keeps the capacity an earlier task gave
    /// it, and the server's IDs, timestamps and token move arena chunk boundaries). Once the
    /// failure is induced every later allocation fails too, so one refusal means the induced
    /// failure was the last allocation the task tried.
    const CountingFailures = struct {
        failing: std.testing.FailingAllocator,
        refused: std.atomic.Value(usize) = .init(0),

        fn allocator(c: *CountingFailures) std.mem.Allocator {
            return .{ .ptr = c, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
        }

        fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
            const c: *CountingFailures = @ptrCast(@alignCast(ctx));
            const ptr = c.failing.allocator().rawAlloc(len, alignment, ra);
            if (ptr == null) _ = c.refused.fetchAdd(1, .monotonic);
            return ptr;
        }

        fn resize(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ra: usize) bool {
            const c: *CountingFailures = @ptrCast(@alignCast(ctx));
            return c.failing.allocator().rawResize(memory, alignment, new_len, ra);
        }

        fn remap(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ra: usize) ?[*]u8 {
            const c: *CountingFailures = @ptrCast(@alignCast(ctx));
            return c.failing.allocator().rawRemap(memory, alignment, new_len, ra);
        }

        fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ra: usize) void {
            const c: *CountingFailures = @ptrCast(@alignCast(ctx));
            c.failing.allocator().rawFree(memory, alignment, ra);
        }
    };

    /// Completes a task the test holds, as a failure, and frees it.
    fn complete(core: bridge.Worker, polled: bridge.Worker.Polled) void {
        defer polled.deinit();
        const head = ActivityTaskHead.read(polled.bytes()) orelse return;
        var buffer: [failed_activity_completion_len]u8 = undefined;
        const completion = encodeFailedActivityCompletion(&buffer, head.token, .{ .message = "released by the test" }) orelse return;
        core.completeActivityTask(testing.allocator, testing.io, completion, null) catch {};
    }
};

test "an activity task that runs out of memory anywhere before its completion is failed retryably and its slot released" {
    const rt = try bridge.Runtime.init(testing.allocator, .{}, null);
    defer rt.deinit();
    var server = try bridge.EphemeralServer.startDevServer(testing.allocator, testing.io, rt, .{ .extra_args = &@import("testing.zig").DevServer.standalone_activity_args }, null);
    defer server.shutdown(testing.io, null) catch {};
    var client = try client_mod.Client.connect(testing.allocator, testing.io, .{ .host_port = server.target });
    defer client.close();
    var message: ?[]u8 = null;
    defer if (message) |m| testing.allocator.free(m);
    const core = try bridge.Worker.init(testing.allocator, try client.currentConnection(), .{
        .namespace = "default",
        .task_queue = OutOfMemoryTest.task_queue,
        .task_types = .{ .workflows = false, .remote_activities = true, .local_activities = false, .nexus = false },
        .tuner = .{ .activity = .{ .fixed = 2 } },
    }, &message);
    defer core.deinit();
    var shut_down = false;
    defer {
        if (!shut_down) core.initiateShutdown();
        // The core cancels each task still outstanding; completing them lets it finish.
        while (core.pollActivityTask(testing.allocator, testing.io, null) catch null) |polled| OutOfMemoryTest.complete(core, polled);
        core.finalizeShutdown(testing.allocator, testing.io, null) catch {};
    }
    // The out-of-memory paths log errors; they go to this sink, not to std.log.
    var sink: RecordingSink = .{};
    defer sink.deinit();
    var w = try Worker.init(testing.allocator, testing.io, &client, OutOfMemoryTest.task_queue, .{ .logger = sink.sink() });
    defer w.deinit();
    try w.registerActivity(OutOfMemoryTest.Fill);
    var running: RunningActivities = .{};
    defer running.map.deinit(testing.allocator);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // Each allocation from the poll to the completion fails in turn (with every one after it).
    var sent_from_stack: usize = 0;
    var fail_index: usize = 0;
    while (true) : (fail_index += 1) {
        const id = try std.fmt.allocPrint(a, "out-of-memory-{d}", .{fail_index});
        var handle = try OutOfMemoryTest.start(&client, id);
        defer handle.deinit();
        const polled = try OutOfMemoryTest.poll(core, &shut_down);
        var counting: OutOfMemoryTest.CountingFailures = .{ .failing = .init(testing.allocator, .{ .fail_index = fail_index }) };
        w.gpa = counting.allocator();
        defer w.gpa = testing.allocator;
        if (receiveActivityTask(&w, core, &running, polled, OutOfMemoryTest.task_queue)) |run| superviseActivity(&w, core, &running, run);
        w.gpa = testing.allocator;
        try testing.expectEqual(0, running.map.count());
        const result = handle.get(a);
        if (!counting.failing.has_induced_failure) {
            try testing.expectEqual(OutOfMemoryTest.result_len, (try result).len);
            break;
        }
        if (result) |r| {
            // Only the bridge's cell for the completion failed, the last allocation: the
            // completion was sent again with the cell on the stack. An allocation failure
            // swallowed before it would show as a second refusal, the cell's.
            try testing.expectEqual(OutOfMemoryTest.result_len, r.len);
            try testing.expectEqual(1, counting.refused.load(.monotonic));
            sent_from_stack += 1;
            continue;
        } else |err| try testing.expectEqual(error.ActivityFailed, err);
        // Failed retryably, as an activity returning error.OutOfMemory is, wherever the
        // failure hit; never run untracked to a result. The retry policy allows no second
        // attempt, so the server reports the failure.
        const f = handle.failure().?;
        if (fail_index == 0) try testing.expectEqualStrings("the worker ran out of memory receiving the activity task", f.message);
        const info = f.failure_info.?.application_failure_info;
        try testing.expectEqualStrings("OutOfMemory", info.type);
        try testing.expect(!info.non_retryable);
    }
    // The sweep reached the last allocation, the bridge's cell, at least once.
    try testing.expect(sent_from_stack > 0);
    // Each path that runs out of memory was reached.
    inline for (.{ "receiving an activity task", "decoding an activity task", "tracking an activity for cancellation", "reporting an activity" }) |what|
        try testing.expect(sink.countAt(.err, "Out of memory " ++ what ++ "; failing it as retryable so the server retries it", &.{}) > 0);

    // Both slots are free: two activities run at once.
    var first = try OutOfMemoryTest.start(&client, "out-of-memory-held-1");
    defer first.deinit();
    var second = try OutOfMemoryTest.start(&client, "out-of-memory-held-2");
    defer second.deinit();
    const held = try OutOfMemoryTest.poll(core, &shut_down);
    defer OutOfMemoryTest.complete(core, held);
    const also_held = try OutOfMemoryTest.poll(core, &shut_down);
    OutOfMemoryTest.complete(core, also_held);
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

/// An Io whose `concurrent` and `groupConcurrent` succeed until the test sets the matching
/// flag, then fail, standing in for a machine that runs out of threads while the worker's
/// poll loops run.
const RefusingConcurrency = struct {
    var inner: Io = undefined;
    var vtable: Io.VTable = undefined;
    var refuse_concurrent: std.atomic.Value(bool) = .init(false);
    var refuse_group: std.atomic.Value(bool) = .init(false);
    var refused: std.atomic.Value(usize) = .init(0);

    fn io(base: Io) Io {
        inner = base;
        refuse_concurrent.store(false, .release);
        refuse_group.store(false, .release);
        refused.store(0, .release);
        vtable = base.vtable.*;
        vtable.concurrent = concurrent;
        vtable.groupConcurrent = groupConcurrent;
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
        if (refuse_concurrent.load(.acquire)) {
            _ = refused.fetchAdd(1, .acq_rel);
            return error.ConcurrencyUnavailable;
        }
        return inner.vtable.concurrent(userdata, result_len, result_alignment, context, context_alignment, start);
    }

    fn groupConcurrent(
        userdata: ?*anyopaque,
        group: *Io.Group,
        context: []const u8,
        context_alignment: std.mem.Alignment,
        start: *const fn (context: *const anyopaque) void,
    ) Io.ConcurrentError!void {
        if (refuse_group.load(.acquire)) {
            _ = refused.fetchAdd(1, .acq_rel);
            return error.ConcurrencyUnavailable;
        }
        return inner.vtable.groupConcurrent(userdata, group, context, context_alignment, start);
    }
};

const NoThreadTest = struct {
    /// How many times `record` ran.
    var calls: std.atomic.Value(u32) = .init(0);
    var last_attempt: std.atomic.Value(u32) = .init(0);

    fn record(ctx: *activity.Context) !u32 {
        _ = calls.fetchAdd(1, .acq_rel);
        const attempt = ctx.info().attempt;
        last_attempt.store(attempt, .release);
        return attempt;
    }
    const Record = activity.Def("RecordAttempt", record);

    fn once(ctx: *workflow.Context) !u32 {
        return ctx.executeActivity(Record, .{
            .start_to_close_timeout = .fromSeconds(30),
            .retry_policy = .{ .initial_interval = .fromMilliseconds(100), .backoff_coefficient = 1, .maximum_attempts = 0 },
        }, .{}).get();
    }
    const Once = workflow.Def("NoThreadOnce", once);

    /// Whether the run's activity has failed an attempt for want of a thread, retryably.
    fn refusedAttemptSeen(client: *client_mod.Client, arena: Allocator, id: []const u8, run_id: []const u8) !bool {
        const d = try client.describeWorkflow(arena, id, run_id);
        for (d.raw.pending_activities.items) |p| {
            const f = p.last_failure orelse continue;
            if (std.mem.indexOf(u8, f.message, "no thread") == null) continue;
            const info = f.failure_info orelse continue;
            switch (info) {
                .application_failure_info => |app| if (std.mem.eql(u8, app.type, no_thread_failure_type) and !app.non_retryable) return true,
                else => {},
            }
        }
        return false;
    }
};

// Both places an activity can be refused a thread: the poll loop's group (no supervisor) and the
// supervisor's `concurrent` (no task for the activity). Neither may run the activity where a
// cancel can't reach it, nor stop the poll loop; the attempt fails retryably instead.
test "an activity the worker has no thread for fails its attempt as retryable, from the poll loop and from its supervisor, and a later attempt runs" {
    // Each refusal is logged as a warning.
    testing.log_level = .err;
    const rt = try bridge.Runtime.init(testing.allocator, .{}, null);
    defer rt.deinit();
    var server = try bridge.EphemeralServer.startDevServer(testing.allocator, testing.io, rt, .{}, null);
    defer server.shutdown(testing.io, null) catch {};
    var client = try client_mod.Client.connect(testing.allocator, testing.io, .{ .host_port = server.target });
    defer client.close();

    var w = try Worker.init(testing.allocator, RefusingConcurrency.io(testing.io), &client, "no-thread", .{});
    defer w.deinit();
    try w.registerWorkflow(HelloTest.Hello);
    try w.registerActivity(HelloTest.Greet);
    try w.registerWorkflow(NoThreadTest.Once);
    try w.registerActivity(NoThreadTest.Record);
    var running = try testing.io.concurrent(HelloTest.runWorker, .{&w});
    var stopped = false;
    defer if (!stopped) {
        RefusingConcurrency.refuse_group.store(false, .release);
        RefusingConcurrency.refuse_concurrent.store(false, .release);
        w.stop();
        running.await(testing.io) catch {};
    };

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // A completed workflow and activity show both poll loops are polling.
    var warm = try client.executeWorkflow(HelloTest.Hello, .{ .id = "no-thread-warm-up", .task_queue = "no-thread" }, .{"Temporal"});
    defer warm.deinit();
    try testing.expectEqualStrings("Hello Temporal!", try warm.get(a));

    const flags = [_]*std.atomic.Value(bool){ &RefusingConcurrency.refuse_group, &RefusingConcurrency.refuse_concurrent };
    for (flags, 0..) |flag, i| {
        NoThreadTest.calls.store(0, .release);
        RefusingConcurrency.refused.store(0, .release);
        flag.store(true, .release);
        const id = try std.fmt.allocPrint(a, "no-thread-{d}", .{i});
        var run = try client.executeWorkflow(NoThreadTest.Once, .{ .id = id, .task_queue = "no-thread", .execution_timeout = .fromSeconds(60) }, .{});
        defer run.deinit();

        const deadline = Io.Clock.awake.now(testing.io).addDuration(.fromSeconds(30));
        while (!try NoThreadTest.refusedAttemptSeen(&client, a, run.id, run.run_id)) {
            if (deadline.durationTo(Io.Clock.awake.now(testing.io)).nanoseconds > 0) return error.TestNoRefusedAttempt;
            try testing.io.sleep(.fromMilliseconds(100), .awake);
        }
        try testing.expectEqual(0, NoThreadTest.calls.load(.acquire));
        try testing.expect(RefusingConcurrency.refused.load(.acquire) > 0);

        flag.store(false, .release);
        const attempt = try run.get(a);
        try testing.expect(attempt >= 2);
        try testing.expectEqual(attempt, NoThreadTest.last_attempt.load(.acquire));
        try testing.expectEqual(1, NoThreadTest.calls.load(.acquire));
    }

    // Nothing left behind holds up shutdown.
    stopped = true;
    w.stop();
    try running.await(testing.io);
}

/// Records the context the codecs encode the no-thread failure's attributes with. The worker
/// encodes from several threads, so it records once, under an atomic flag.
const NoThreadContextCodec = struct {
    var claimed: std.atomic.Value(bool) = .init(false);
    var seen: std.atomic.Value(bool) = .init(false);
    var buf: [256]u8 = undefined;
    var recorded: []const u8 = "";

    fn codec() converter.PayloadCodec {
        return .{ .ptr = &buf, .vtable = &.{ .encode = plain, .decode = plain, .encodeWithContext = encode } };
    }

    fn plain(_: *anyopaque, arena: Allocator, _: Io, payloads: []const converter.Payload) converter.CodecError![]converter.Payload {
        return arena.dupe(converter.Payload, payloads);
    }

    fn encode(_: *anyopaque, arena: Allocator, _: Io, context: *const converter.Context, payloads: []const converter.Payload) converter.CodecError![]converter.Payload {
        for (payloads) |p| if (std.mem.indexOf(u8, p.data, "no thread") != null and claimed.cmpxchgStrong(false, true, .acq_rel, .acquire) == null) {
            recorded = std.fmt.bufPrint(&buf, "{s}/{s}/{s}/{s}", .{ context.namespace, context.workflow_id, context.activity_type, context.activity_id }) catch "too long";
            seen.store(true, .release);
        };
        return arena.dupe(converter.Payload, payloads);
    }
};

// The no-thread failure is an activity's failure, so a context-aware codec (one picking its
// encryption key per workflow, say) must see the activity's identity when it encodes it, as it
// does for every other failure of the activity.
test "the codecs encode an activity's no-thread failure in the activity's context" {
    testing.log_level = .err;
    const rt = try bridge.Runtime.init(testing.allocator, .{}, null);
    defer rt.deinit();
    var server = try bridge.EphemeralServer.startDevServer(testing.allocator, testing.io, rt, .{}, null);
    defer server.shutdown(testing.io, null) catch {};
    NoThreadContextCodec.claimed.store(false, .release);
    NoThreadContextCodec.seen.store(false, .release);
    const codecs = [_]converter.PayloadCodec{NoThreadContextCodec.codec()};
    var client = try client_mod.Client.connect(testing.allocator, testing.io, .{ .host_port = server.target, .codecs = &codecs, .encode_failure_attributes = true });
    defer client.close();

    var w = try Worker.init(testing.allocator, RefusingConcurrency.io(testing.io), &client, "no-thread-context", .{});
    defer w.deinit();
    try w.registerWorkflow(HelloTest.Hello);
    try w.registerActivity(HelloTest.Greet);
    try w.registerWorkflow(NoThreadTest.Once);
    try w.registerActivity(NoThreadTest.Record);
    var running = try testing.io.concurrent(HelloTest.runWorker, .{&w});
    var stopped = false;
    defer if (!stopped) {
        RefusingConcurrency.refuse_concurrent.store(false, .release);
        w.stop();
        running.await(testing.io) catch {};
    };

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var warm = try client.executeWorkflow(HelloTest.Hello, .{ .id = "no-thread-context-warm-up", .task_queue = "no-thread-context" }, .{"Temporal"});
    defer warm.deinit();
    try testing.expectEqualStrings("Hello Temporal!", try warm.get(a));

    RefusingConcurrency.refuse_concurrent.store(true, .release);
    var run = try client.executeWorkflow(NoThreadTest.Once, .{ .id = "no-thread-context", .task_queue = "no-thread-context", .execution_timeout = .fromSeconds(60) }, .{});
    defer run.deinit();
    const deadline = Io.Clock.awake.now(testing.io).addDuration(.fromSeconds(30));
    while (!NoThreadContextCodec.seen.load(.acquire)) {
        if (deadline.durationTo(Io.Clock.awake.now(testing.io)).nanoseconds > 0) return error.TestNoRefusedAttempt;
        try testing.io.sleep(.fromMilliseconds(100), .awake);
    }
    RefusingConcurrency.refuse_concurrent.store(false, .release);
    _ = try run.get(a);
    const expected = try std.fmt.allocPrint(a, "{s}/{s}/RecordAttempt/", .{ client.namespace, run.id });
    try testing.expectStringStartsWith(NoThreadContextCodec.recorded, expected);
    try testing.expect(NoThreadContextCodec.recorded.len > expected.len);

    stopped = true;
    w.stop();
    try running.await(testing.io);
}

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

// No loop polls the core worker when the first loop can't start, so `run` frees a core worker
// that was created and validated but never polled. One that polls activities must not wait,
// while it is freed, for pollers that never ran: `run` returns the error. A worker whose
// validation fails (a namespace that doesn't exist) is freed the same way. Freeing it must
// not end the client's core log forwarding either: sdk-core turns forwarding off for the
// whole runtime when any core worker's runtime clone is dropped.
test "run returns when the Io of a worker with activities can't start its first poll loop" {
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

    {
        var w = try Worker.init(testing.allocator, ScarceConcurrency.io(testing.io, 0), &client, "no-concurrency", .{});
        defer w.deinit();
        try w.registerWorkflow(HelloTest.Hello);
        try w.registerActivity(HelloTest.Greet);
        try testing.expectError(error.ConcurrencyUnavailable, w.run());
    }

    // The unpolled core worker is freed: a core record while another worker runs shows
    // forwarding went on.
    const seen = client_sink.coreRecords();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var w = try Worker.init(testing.allocator, testing.io, &client, "after-no-concurrency", .{});
    defer w.deinit();
    try w.registerWorkflow(HelloTest.Hello);
    try w.registerActivity(HelloTest.Greet);
    var running = try testing.io.concurrent(HelloTest.runWorker, .{&w});
    defer {
        w.stop();
        running.await(testing.io) catch {};
    }
    var run = try client.executeWorkflow(HelloTest.Hello, .{ .id = "after-no-concurrency", .task_queue = "after-no-concurrency" }, .{"Temporal"});
    defer run.deinit();
    try testing.expectEqualStrings("Hello Temporal!", try run.get(arena.allocator()));
    try client_sink.waitForCoreRecordsPast(seen, .fromSeconds(30));
}

/// An Io whose `concurrent` refuses every call, first making `failing` refuse every later
/// allocation: a worker that runs out of threads and memory once its core workers exist.
const NoThreadsNorMemory = struct {
    var failing: *std.testing.FailingAllocator = undefined;
    var vtable: Io.VTable = undefined;

    fn io(base: Io, f: *std.testing.FailingAllocator) Io {
        failing = f;
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
        _ = .{ userdata, result_len, result_alignment, context, context_alignment, start };
        failing.fail_index = failing.alloc_index;
        return error.ConcurrencyUnavailable;
    }
};

// A sessions worker whose first poll loop can't start frees its session core workers unpolled,
// and finalizing one that can't allocate logs its task queue: one of the session host's
// strings. `run` freed the host first, so the log read freed memory (found by
// tools/checks/worker_exits.zig's allocation sweep, as a segfault).
test "a sessions worker that can't start logs its session queues while they're still allocated" {
    // Finalizing without memory is logged as a warning, which must be formatted (the test
    // runner formats only records at `testing.log_level`) for the queue's name to be read.
    testing.log_level = .warn;
    const rt = try bridge.Runtime.init(testing.allocator, .{}, null);
    defer rt.deinit();
    var server = try bridge.EphemeralServer.startDevServer(testing.allocator, testing.io, rt, .{}, null);
    defer server.shutdown(testing.io, null) catch {};
    var client = try client_mod.Client.connect(testing.allocator, testing.io, .{ .host_port = server.target });
    defer client.close();

    // page_allocator unmaps each allocation when it is freed, so reading one afterwards faults.
    var failing: std.testing.FailingAllocator = .init(std.heap.page_allocator, .{});
    // No debug-info read, whose start would be the first `concurrent` call.
    var w = try Worker.init(failing.allocator(), NoThreadsNorMemory.io(testing.io, &failing), &client, "sessions-no-threads-nor-memory", .{ .sessions = .{}, .failure_stack_traces = false });
    try w.registerWorkflow(HelloTest.Hello);
    try w.registerActivity(HelloTest.Greet);
    try testing.expectError(error.ConcurrencyUnavailable, w.run());
    try testing.expect(failing.has_induced_failure);
    w.deinit();
    try testing.expectEqual(failing.allocated_bytes, failing.freed_bytes);
}

// The poll loops ran forever once the worker's allocator refused a poll's cell: each poll
// failed before reaching the core, so none saw the shutdown the failure began, and `run`
// never returned (found by tools/checks/worker_exits.zig's allocation sweep). After the
// refusal, which a loop reports, the polls get their cells elsewhere and drain the core.
test "a poll the worker's allocator refuses is reported, and the polls after it still reach the core" {
    const rt = try bridge.Runtime.init(testing.allocator, .{}, null);
    defer rt.deinit();
    var server = try bridge.EphemeralServer.startDevServer(testing.allocator, testing.io, rt, .{}, null);
    defer server.shutdown(testing.io, null) catch {};
    var client = try client_mod.Client.connect(testing.allocator, testing.io, .{ .host_port = server.target });
    defer client.close();

    var failing: std.testing.FailingAllocator = .init(testing.allocator, .{});
    var w = try Worker.init(failing.allocator(), testing.io, &client, "poll-out-of-memory", .{});
    defer w.deinit();
    const core = try bridge.Worker.init(testing.allocator, try client.currentConnection(), .{
        .namespace = "default",
        .task_queue = "poll-out-of-memory",
        .task_types = .{ .workflows = false, .remote_activities = true, .local_activities = false, .nexus = false },
    }, null);
    defer core.deinit();
    core.initiateShutdown();
    failing.fail_index = failing.alloc_index;

    var gpa_refused = false;
    try testing.expectError(error.OutOfMemory, w.pollCore(core, bridge.Worker.pollActivityTask, null, &gpa_refused));
    try testing.expect(gpa_refused);
    // Shut down and never polled, the core has nothing more to give: the poll returns null.
    try testing.expectEqual(null, try w.pollCore(core, bridge.Worker.pollActivityTask, null, &gpa_refused));
    try core.finalizeShutdown(testing.allocator, testing.io, null);
}

/// An allocator that refuses one allocation, the first once `armed` is set, and gives every other
/// from `parent`: a transient out-of-memory, which `std.testing.FailingAllocator` (refusing
/// every allocation from its fail index on) can't express.
const RefuseOnce = struct {
    parent: Allocator,
    armed: std.atomic.Value(bool) = .init(false),
    refused: std.atomic.Value(usize) = .init(0),

    fn allocator(r: *RefuseOnce) Allocator {
        return .{ .ptr = r, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }

    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        const r: *RefuseOnce = @ptrCast(@alignCast(ctx));
        if (r.armed.cmpxchgStrong(true, false, .acq_rel, .acquire) == null) {
            _ = r.refused.fetchAdd(1, .acq_rel);
            return null;
        }
        return r.parent.rawAlloc(len, alignment, ra);
    }

    fn resize(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ra: usize) bool {
        const r: *RefuseOnce = @ptrCast(@alignCast(ctx));
        return r.parent.rawResize(memory, alignment, new_len, ra);
    }

    fn remap(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ra: usize) ?[*]u8 {
        const r: *RefuseOnce = @ptrCast(@alignCast(ctx));
        return r.parent.rawRemap(memory, alignment, new_len, ra);
    }

    fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ra: usize) void {
        const r: *RefuseOnce = @ptrCast(@alignCast(ctx));
        r.parent.rawFree(memory, alignment, ra);
    }

    /// An Io whose `concurrent` refuses every call, first arming `r`: a worker that can't
    /// start its poll loops, and whose next allocation after that is refused.
    const NoThreads = struct {
        var refuse: *RefuseOnce = undefined;
        var vtable: Io.VTable = undefined;

        fn io(base: Io, r: *RefuseOnce) Io {
            refuse = r;
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
            _ = .{ userdata, result_len, result_alignment, context, context_alignment, start };
            refuse.armed.store(true, .release);
            return error.ConcurrencyUnavailable;
        }
    };
};

// `run` frees a core worker that no loop polls by polling it to null before finalizing it.
// Those polls ended at the first failure, so one whose cell the worker's allocator refused
// never reached the core: its activity poller never saw the shutdown, and finalizing, which
// could allocate again, waited for it forever. A refused poll now drains on like a poll loop's.
test "run returns when the worker's allocator refuses one poll of a core worker it frees unpolled" {
    const rt = try bridge.Runtime.init(testing.allocator, .{}, null);
    defer rt.deinit();
    var server = try bridge.EphemeralServer.startDevServer(testing.allocator, testing.io, rt, .{}, null);
    defer server.shutdown(testing.io, null) catch {};
    var client = try client_mod.Client.connect(testing.allocator, testing.io, .{ .host_port = server.target });
    defer client.close();

    var refuse: RefuseOnce = .{ .parent = testing.allocator };
    // No debug-info read, whose start would be the first `concurrent` call, and no workflow:
    // the first `concurrent` is the activity loop's, and the next allocation the poll's cell.
    var w = try Worker.init(refuse.allocator(), RefuseOnce.NoThreads.io(testing.io, &refuse), &client, "refuse-one-poll", .{ .failure_stack_traces = false });
    defer w.deinit();
    try w.registerActivity(HelloTest.Greet);

    const Run = struct {
        result: Worker.RunError!void = {},
        done: Io.Event = .unset,
        fn run(r: *@This(), worker: *Worker) void {
            r.result = worker.run();
            r.done.set(testing.io);
        }
    };
    var r: Run = .{};
    // On the real Io: the worker's refuses `concurrent`.
    var future = try testing.io.concurrent(Run.run, .{ &r, &w });
    r.done.waitTimeout(testing.io, .{ .duration = .{ .raw = .fromSeconds(30), .clock = .awake } }) catch |err| switch (err) {
        // Run is stuck in `finalizeShutdown` for good, and nothing can free what it holds.
        error.Timeout => std.debug.panic("Worker.run did not return within 30 s after one refused allocation", .{}),
        error.Canceled => |e| return e,
    };
    future.await(testing.io);
    try testing.expectError(error.ConcurrencyUnavailable, r.result);
    try testing.expectEqual(1, refuse.refused.load(.acquire));
}

const CancelTest = struct {
    const Env = @import("testing.zig").WorkflowEnvironment;
    const EventType = proto.temporal.api.enums.v1.EventType;

    /// Bounds one named wait of a test.
    const wait_limit: Io.Duration = .fromSeconds(30);
    /// Set by `stop`: the looping fixtures return, whatever happened to their cancel.
    var abandon: std.atomic.Value(bool) = .init(false);
    /// The server ends a run whose activity never ends, so `getResult` can't wait for good.
    const run_options: client_mod.StartWorkflowOptions = .{ .task_queue = "", .execution_timeout = .fromSeconds(45) };

    var started: Io.Event = .unset;
    /// Set once `beat` has seen a cancel; `seen_reason` is written before.
    var observed: Io.Event = .unset;
    var seen_reason: ?activity.CancelReason = null;

    /// Heartbeats until canceled. With `ignore`, swallows the cancel and returns a value.
    fn beat(ctx: *activity.Context, ignore: bool) ![]const u8 {
        started.set(ctx.io());
        while (!abandon.load(.acquire)) {
            ctx.heartbeat({});
            ctx.io().sleep(.fromMilliseconds(50), .awake) catch |err| {
                seen_reason = ctx.cancelReason();
                observed.set(ctx.io());
                if (ignore) return "finished anyway";
                return err;
            };
        }
        return error.TestAbandoned;
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

    /// Set when `BeatThroughCodec`'s heartbeat has reached `HoldingCodec`.
    var in_codec: Io.Event = .unset;
    /// The thread `BeatThroughCodec` runs on, whose encodes `HoldingCodec` holds; 0 for none.
    var holding_thread: std.atomic.Value(std.Thread.Id) = .init(0);
    /// `BeatThroughCodec`'s context, written before `holding_thread`.
    var beat_ctx: ?*activity.Context = null;

    /// Passes payloads through. On `holding_thread` it waits on its `io` until the activity
    /// is canceled and a while after, so the worker's cancel lands while it waits.
    const HoldingCodec = struct {
        var unused: u8 = 0;

        fn codec() converter.PayloadCodec {
            return .{ .ptr = &unused, .vtable = &.{ .encode = encode, .decode = decode } };
        }

        fn encode(_: *anyopaque, arena: Allocator, io: Io, payloads: []const converter.Payload) converter.CodecError![]converter.Payload {
            if (holding_thread.load(.acquire) == std.Thread.getCurrentId()) {
                in_codec.set(io);
                while (beat_ctx.?.cancelReason() == null and !abandon.load(.acquire)) try io.sleep(.fromMilliseconds(5), .awake);
                try io.sleep(.fromMilliseconds(500), .awake);
                holding_thread.store(0, .release);
            }
            return arena.dupe(converter.Payload, payloads);
        }

        fn decode(_: *anyopaque, arena: Allocator, _: Io, payloads: []const converter.Payload) converter.CodecError![]converter.Payload {
            return arena.dupe(converter.Payload, payloads);
        }
    };

    /// Like `beat`, with heartbeat details for `HoldingCodec` to encode.
    fn beatThroughCodec(ctx: *activity.Context) ![]const u8 {
        beat_ctx = ctx;
        holding_thread.store(std.Thread.getCurrentId(), .release);
        while (!abandon.load(.acquire)) {
            ctx.heartbeat(@as(u32, 1));
            ctx.io().sleep(.fromMilliseconds(50), .awake) catch |err| {
                seen_reason = ctx.cancelReason();
                observed.set(ctx.io());
                return err;
            };
        }
        return error.TestAbandoned;
    }
    const BeatThroughCodec = activity.Def("BeatThroughCodec", beatThroughCodec);

    fn runBeatThroughCodec(ctx: *workflow.Context) ![]const u8 {
        return ctx.executeActivity(BeatThroughCodec, .{ .start_to_close_timeout = .fromSeconds(60) }, .{}).get();
    }
    const RunBeatThroughCodec = workflow.Def("RunBeatThroughCodec", runBeatThroughCodec);

    /// Set by the test once the activity's cancel is pending and std's debug info lock is
    /// held; `BeatWhenTold` heartbeats then.
    var go: Io.Event = .unset;

    /// Waits for `go` without taking a cancel, so the worker's cancel stays pending on its
    /// task, then heartbeats like `beat`.
    fn beatWhenTold(ctx: *activity.Context) ![]const u8 {
        beat_ctx = ctx;
        started.set(ctx.io());
        go.waitUncancelable(ctx.io());
        while (!abandon.load(.acquire)) {
            ctx.heartbeat(@as(u32, 1));
            ctx.io().sleep(.fromMilliseconds(50), .awake) catch |err| {
                seen_reason = ctx.cancelReason();
                observed.set(ctx.io());
                return err;
            };
        }
        return error.TestAbandoned;
    }
    const BeatWhenTold = activity.Def("BeatWhenTold", beatWhenTold);

    fn runBeatWhenTold(ctx: *workflow.Context) ![]const u8 {
        return ctx.executeActivity(BeatWhenTold, .{ .start_to_close_timeout = .fromSeconds(60) }, .{}).get();
    }
    const RunBeatWhenTold = workflow.Def("RunBeatWhenTold", runBeatWhenTold);

    /// Like `beatWhenTold`, but allocates from `ctx.allocator()` once told, before it sleeps.
    fn allocWhenTold(ctx: *activity.Context) ![]const u8 {
        beat_ctx = ctx;
        started.set(ctx.io());
        go.waitUncancelable(ctx.io());
        const block = try ctx.allocator().alloc(u8, 1 << 20);
        @memset(block, 0);
        while (!abandon.load(.acquire)) {
            ctx.io().sleep(.fromMilliseconds(50), .awake) catch |err| {
                seen_reason = ctx.cancelReason();
                observed.set(ctx.io());
                return err;
            };
        }
        return error.TestAbandoned;
    }
    const AllocWhenTold = activity.Def("AllocWhenTold", allocWhenTold);

    fn runAllocWhenTold(ctx: *workflow.Context) ![]const u8 {
        return ctx.executeActivity(AllocWhenTold, .{ .start_to_close_timeout = .fromSeconds(60) }, .{}).get();
    }
    const RunAllocWhenTold = workflow.Def("RunAllocWhenTold", runAllocWhenTold);

    /// Sets `go` while holding std's debug info lock, and holds it until a thread waits for
    /// it and a while after, so the fixture's next allocation waits with its cancel pending.
    /// Nothing here may allocate: with the lock held that would wait for this thread.
    fn goWithDebugInfoLockHeld(si: anytype) !void {
        si.mutex.lockUncancelable(testing.io);
        defer si.mutex.unlock(testing.io);
        go.set(testing.io);
        const deadline = Io.Clock.Timestamp.fromNow(testing.io, .{ .raw = wait_limit, .clock = .awake });
        while (si.mutex.state.load(.monotonic) != .contended) {
            if (deadline.durationFromNow(testing.io).raw.nanoseconds <= 0) break;
            try testing.io.sleep(.fromMilliseconds(1), .awake);
        }
        // Long enough for the supervisor's cancel of the task to land while the lock is held.
        try testing.io.sleep(.fromMilliseconds(200), .awake);
    }

    fn reset() void {
        go = .unset;
        started = .unset;
        observed = .unset;
        release = .unset;
        seen_reason = null;
        abandon.store(false, .release);
        in_codec = .unset;
        holding_thread.store(0, .release);
        beat_ctx = null;
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
        return startWithOptions(.{});
    }

    fn startWithOptions(options: Env.Options) !*Env {
        reset();
        testing.log_level = .debug;
        errdefer testing.log_level = .warn;
        const env = try Env.startWithOptions(testing.allocator, testing.io, options);
        errdefer env.deinit();
        try env.registerWorkflow(CancelBeat);
        try env.registerWorkflow(RunBeat);
        try env.registerActivity(Beat);
        try env.registerActivity(WaitStarted);
        try env.registerWorkflow(RunFinish);
        try env.registerActivity(Finish);
        try env.registerWorkflow(RunBeatThroughCodec);
        try env.registerActivity(BeatThroughCodec);
        try env.registerWorkflow(RunBeatWhenTold);
        try env.registerActivity(BeatWhenTold);
        try env.registerWorkflow(RunAllocWhenTold);
        try env.registerActivity(AllocWhenTold);
        return env;
    }

    /// Waits up to `wait_limit` for `event`, logging `what` when it doesn't come.
    fn waitFor(event: *Io.Event, comptime what: []const u8) error{ Timeout, Canceled }!void {
        event.waitTimeout(testing.io, .{ .duration = .{ .raw = wait_limit, .clock = .awake } }) catch |err| switch (err) {
            error.Timeout => {
                log.err("CancelTest: timed out after {d} s waiting for " ++ what, .{wait_limit.toSeconds()});
                return error.Timeout;
            },
            error.Canceled => |e| return e,
        };
    }

    /// Waits up to `wait_limit` for `beat_ctx`'s activity to have a cancel reason.
    fn waitForCancelReason(comptime what: []const u8) error{ Timeout, Canceled }!void {
        const deadline = Io.Clock.Timestamp.fromNow(testing.io, .{ .raw = wait_limit, .clock = .awake });
        while (beat_ctx.?.cancelReason() == null) {
            if (deadline.durationFromNow(testing.io).raw.nanoseconds <= 0) {
                log.err("CancelTest: timed out after {d} s waiting for " ++ what, .{wait_limit.toSeconds()});
                return error.Timeout;
            }
            try testing.io.sleep(.fromMilliseconds(5), .awake);
        }
    }

    /// Awaits the worker's run and returns its error.
    fn awaitWorker(env: *Env) !void {
        defer env.running = null;
        return env.running.?.await(testing.io);
    }

    /// What every test of the group defers: tells the fixtures to end, so the worker can
    /// stop whatever happened to their cancel, then tears the environment down.
    fn stop(env: *Env) void {
        // Every event a fixture waits on, so they all return whatever the test did.
        abandon.store(true, .release);
        release.set(testing.io);
        go.set(testing.io);
        started.set(testing.io);
        env.deinit();
        testing.log_level = .warn;
    }
};

test "a heartbeating activity canceled by its workflow sees the cancel and reports canceled" {
    const env = try CancelTest.start();
    defer CancelTest.stop(env);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();

    var run = try env.startWorkflow(CancelTest.CancelBeat, CancelTest.run_options, .{false});
    defer run.deinit();
    errdefer env.client.terminateWorkflow(run.id, run.run_id, "test failed") catch {};
    try CancelTest.waitFor(&CancelTest.observed, "Beat to see the cancel its workflow requested");
    try testing.expectEqualStrings("canceled", try env.getResult(CancelTest.CancelBeat, &run, arena.allocator(), null));
    try testing.expectEqual(@as(?activity.CancelReason, .canceled), CancelTest.seen_reason);
    try testing.expectEqual(1, try CancelTest.countEvents(env, &run, .EVENT_TYPE_ACTIVITY_TASK_CANCELED));
}

test "an activity that ignores a cancel and returns a value completes normally" {
    const env = try CancelTest.start();
    defer CancelTest.stop(env);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();

    var run = try env.startWorkflow(CancelTest.CancelBeat, CancelTest.run_options, .{true});
    defer run.deinit();
    errdefer env.client.terminateWorkflow(run.id, run.run_id, "test failed") catch {};
    try CancelTest.waitFor(&CancelTest.observed, "Beat to see the cancel its workflow requested");
    try testing.expectEqualStrings("finished anyway", try env.getResult(CancelTest.CancelBeat, &run, arena.allocator(), null));
    try testing.expectEqual(@as(?activity.CancelReason, .canceled), CancelTest.seen_reason);
    try testing.expectEqual(0, try CancelTest.countEvents(env, &run, .EVENT_TYPE_ACTIVITY_TASK_CANCELED));
    try testing.expectEqual(2, try CancelTest.countEvents(env, &run, .EVENT_TYPE_ACTIVITY_TASK_COMPLETED));
}

test "canceling a workflow cancels its heartbeating activity" {
    const env = try CancelTest.start();
    defer CancelTest.stop(env);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();

    var run = try env.startWorkflow(CancelTest.RunBeat, CancelTest.run_options, .{});
    defer run.deinit();
    errdefer env.client.terminateWorkflow(run.id, run.run_id, "test failed") catch {};
    try CancelTest.waitFor(&CancelTest.started, "Beat to start");
    try env.client.cancelWorkflow(run.id, run.run_id, "testing activity cancellation");
    try CancelTest.waitFor(&CancelTest.observed, "Beat to see the cancel of its canceled workflow");
    try testing.expectError(error.WorkflowCanceled, env.getResult(CancelTest.RunBeat, &run, arena.allocator(), null));
    try testing.expectEqual(@as(?activity.CancelReason, .canceled), CancelTest.seen_reason);
    try testing.expectEqual(1, try CancelTest.countEvents(env, &run, .EVENT_TYPE_ACTIVITY_TASK_CANCELED));
}

test "stopping the worker cancels running activities with reason worker_shutdown" {
    const env = try CancelTest.start();
    defer CancelTest.stop(env);

    var run = try env.startWorkflow(CancelTest.RunBeat, CancelTest.run_options, .{});
    defer run.deinit();
    errdefer env.client.terminateWorkflow(run.id, run.run_id, "test failed") catch {};
    try CancelTest.waitFor(&CancelTest.started, "Beat to start");
    env.worker.stop();
    try CancelTest.waitFor(&CancelTest.observed, "Beat to see the worker-stop cancel");
    try CancelTest.awaitWorker(env);
    try testing.expectEqual(@as(?activity.CancelReason, .worker_shutdown), CancelTest.seen_reason);
}

test "a graceful shutdown period lets a running activity finish" {
    const env = try CancelTest.start();
    defer CancelTest.stop(env);
    const period: Io.Duration = .fromSeconds(30);
    env.worker.options.graceful_shutdown_period = period;

    var run = try env.startWorkflow(CancelTest.RunFinish, CancelTest.run_options, .{});
    defer run.deinit();
    errdefer env.client.terminateWorkflow(run.id, run.run_id, "test failed") catch {};
    try CancelTest.waitFor(&CancelTest.started, "Finish to start");
    const stopped_at = Io.Timestamp.now(testing.io, .awake);
    env.worker.stop();
    CancelTest.release.set(testing.io);
    try CancelTest.awaitWorker(env);
    const elapsed = stopped_at.durationTo(Io.Timestamp.now(testing.io, .awake));

    try testing.expectEqual(@as(?activity.CancelReason, null), CancelTest.seen_reason);
    try testing.expectEqual(1, try CancelTest.countEvents(env, &run, .EVENT_TYPE_ACTIVITY_TASK_COMPLETED));
    try testing.expect(elapsed.nanoseconds < period.nanoseconds);
}

test "a cancel that arrives while a heartbeat's codec waits still reaches the activity" {
    const codecs = [_]converter.PayloadCodec{CancelTest.HoldingCodec.codec()};
    const env = try CancelTest.startWithOptions(.{ .codecs = &codecs });
    defer CancelTest.stop(env);

    var run = try env.startWorkflow(CancelTest.RunBeatThroughCodec, CancelTest.run_options, .{});
    defer run.deinit();
    errdefer env.client.terminateWorkflow(run.id, run.run_id, "test failed") catch {};
    try CancelTest.waitFor(&CancelTest.in_codec, "BeatThroughCodec's heartbeat to reach the codec");
    // Worker stop is the cancel source: it needs no heartbeat to reach the worker.
    env.worker.stop();
    try CancelTest.waitFor(&CancelTest.observed, "BeatThroughCodec to see error.Canceled after its heartbeat's codec returned");
    try CancelTest.awaitWorker(env);
    try testing.expectEqual(@as(?activity.CancelReason, .worker_shutdown), CancelTest.seen_reason);
}

// In a Debug build `testing.allocator` captures a stack trace on every allocation, and on macOS
// std's unwinder takes the debug info lock with a wait that can be canceled and drops the
// error. A heartbeat that allocates while another thread holds that lock must not take the
// activity's one cancel.
test "a cancel pending while a heartbeat waits for std's debug info lock still reaches the activity" {
    const SelfInfo = std.debug.SelfInfo;
    if (comptime builtin.mode != .debug or !std.debug.sys_can_stack_trace or SelfInfo == void or !@hasField(SelfInfo, "mutex"))
        return error.SkipZigTest;
    const env = try CancelTest.start();
    defer CancelTest.stop(env);

    var run = try env.startWorkflow(CancelTest.RunBeatWhenTold, CancelTest.run_options, .{});
    defer run.deinit();
    errdefer env.client.terminateWorkflow(run.id, run.run_id, "test failed") catch {};
    try CancelTest.waitFor(&CancelTest.started, "BeatWhenTold to start");
    const si = try std.debug.getSelfDebugInfo();
    env.worker.stop();
    try CancelTest.waitForCancelReason("BeatWhenTold's worker-stop cancel to reach its run");
    {
        // Nothing here may allocate: with the lock held that would wait for this thread.
        si.mutex.lockUncancelable(testing.io);
        defer si.mutex.unlock(testing.io);
        CancelTest.go.set(testing.io);
        const deadline = Io.Clock.Timestamp.fromNow(testing.io, .{ .raw = CancelTest.wait_limit, .clock = .awake });
        while (si.mutex.state.load(.monotonic) != .contended) {
            if (deadline.durationFromNow(testing.io).raw.nanoseconds <= 0) break;
            try testing.io.sleep(.fromMilliseconds(1), .awake);
        }
        // Long enough for the supervisor's cancel of the task to land while the lock is held.
        try testing.io.sleep(.fromMilliseconds(200), .awake);
    }
    try CancelTest.waitFor(&CancelTest.observed, "BeatWhenTold to see error.Canceled after its heartbeat waited for the debug info lock");
    try CancelTest.awaitWorker(env);
    try testing.expectEqual(@as(?activity.CancelReason, .worker_shutdown), CancelTest.seen_reason);
}

// Bead temporal-sdk-zig-d51.1.34: an activity's failure resolves its stack trace before the
// worker completes it, and that read used std's debug info, whose one lock every Debug
// allocation's stack capture takes and whose first read holds it for seconds. A failed
// activity then waited on whatever held the lock, past its StartToClose timeout on a slow
// runner.
test "a failed activity's completion doesn't wait for std's debug info lock, and its stack trace still resolves" {
    const SelfInfo = std.debug.SelfInfo;
    if (comptime builtin.mode != .debug or !std.debug.sys_can_stack_trace or SelfInfo == void or !@hasField(SelfInfo, "mutex"))
        return error.SkipZigTest;
    // An activity that fails while the test holds std's debug info lock, on a worker whose
    // allocations and threads never take that lock: `testing.allocator` captures a stack trace
    // on each allocation, and on macOS that takes the lock, so neither it nor `testing.io`
    // (which spawns threads with it) is used here.
    const TraceLockTest = struct {
        /// How long the test holds the lock at most; the activity's failure must arrive well
        /// within it.
        const hold_limit: Io.Duration = .fromSeconds(20);
        const arrival_limit: Io.Duration = .fromSeconds(5);

        var io: Io = undefined;
        var go: Io.Event = .unset;
        var seen: Io.Event = .unset;
        var trace_resolved: bool = false;

        fn charge(ctx: *activity.Context) !void {
            go.waitUncancelable(ctx.io());
            return ctx.fail(.{ .message = "card declined", .type = "Declined", .non_retryable = true });
        }
        const Charge = activity.Def("TraceLockCharge", charge);

        fn order(ctx: *workflow.Context) ![]const u8 {
            const charged = ctx.executeActivity(Charge, .{ .start_to_close_timeout = .fromSeconds(60), .retry_policy = .{ .maximum_attempts = 1 } }, .{});
            charged.get() catch |err| switch (err) {
                error.ActivityFailed => {
                    trace_resolved = std.mem.indexOf(u8, failure_mod.rootCause(charged.failure().?).stack_trace, "worker.zig:") != null;
                    seen.set(io);
                    return "failed";
                },
                else => return err,
            };
            return "completed";
        }
        const Order = workflow.Def("TraceLockOrder", order);

        /// Runs `Order` and holds std's debug info lock from `Charge`'s start until its failure
        /// reaches the workflow, or `hold_limit`. Returns how long that took.
        fn run(trace_was_resolved: *bool) !Io.Duration {
            go = .unset;
            seen = .unset;
            trace_resolved = false;
            // No stack traces: capturing one takes the lock this test holds.
            var gpa_state: std.heap.DebugAllocator(.{ .stack_trace_frames = 0 }) = .init;
            defer if (gpa_state.deinit() == .leak) @panic("TraceLockTest leaked");
            const gpa = gpa_state.allocator();
            var threaded: Io.Threaded = .init(gpa, .{ .environ = testing.environ });
            defer threaded.deinit();
            io = threaded.io();

            const env = try CancelTest.Env.start(gpa, io);
            defer env.deinit();
            // Before the environment's teardown, whatever the test did.
            defer go.set(io);
            try env.registerWorkflow(Order);
            try env.registerActivity(Charge);
            var arena: std.heap.ArenaAllocator = .init(gpa);
            defer arena.deinit();
            var started = try env.startWorkflow(Order, .{ .task_queue = "", .execution_timeout = .fromSeconds(60) }, .{});
            defer started.deinit();
            // Reads the executable's debug info the way stack traces do, before the hold, so the
            // time measured is the activity's and not the first read's.
            _ = try failure_mod.addressesText(arena.allocator(), &.{@returnAddress()});

            const si = try std.debug.getSelfDebugInfo();
            const elapsed = hold: {
                // Nothing here may allocate or log.
                si.mutex.lockUncancelable(io);
                defer si.mutex.unlock(io);
                const begin = Io.Clock.awake.now(io);
                go.set(io);
                seen.waitTimeout(io, .{ .duration = .{ .raw = hold_limit, .clock = .awake } }) catch {};
                break :hold begin.durationTo(Io.Clock.awake.now(io));
            };
            try testing.expectEqualStrings("failed", try env.getResult(Order, &started, arena.allocator(), null));
            trace_was_resolved.* = trace_resolved;
            return elapsed;
        }
    };

    var trace_resolved = false;
    const elapsed = try TraceLockTest.run(&trace_resolved);
    if (elapsed.nanoseconds >= TraceLockTest.arrival_limit.nanoseconds) {
        log.err("the activity's failure took {d} ms to reach its workflow while std's debug info lock was held", .{elapsed.toMilliseconds()});
        return error.TestUnexpectedResult;
    }
    try testing.expect(trace_resolved);
}

/// Holds an activity attempt in `interceptActivity`, before the SDK decodes the activity's
/// arguments, so the worker's cancel is pending while that decoding allocates.
const PreambleCancelTest = struct {
    const Hold = struct {
        pub fn interceptActivity(_: *Hold, ctx: *activity.Context) error{OutOfMemory}!interceptor.Activity {
            if (!std.mem.eql(u8, ctx.info().activity_type, "BeatWithInput")) return .{};
            CancelTest.beat_ctx = ctx;
            CancelTest.started.set(ctx.io());
            CancelTest.go.waitUncancelable(ctx.io());
            return .{};
        }
    };

    /// Like `CancelTest.beat`, with an input large enough that decoding it grows the
    /// activity's arena.
    fn beatWithInput(ctx: *activity.Context, input: []const u64) ![]const u8 {
        std.debug.assert(input.len == input_len);
        while (!CancelTest.abandon.load(.acquire)) {
            ctx.heartbeat({});
            ctx.io().sleep(.fromMilliseconds(50), .awake) catch |err| {
                CancelTest.seen_reason = ctx.cancelReason();
                CancelTest.observed.set(ctx.io());
                return err;
            };
        }
        return error.TestAbandoned;
    }
    const BeatWithInput = activity.Def("BeatWithInput", beatWithInput);

    const input_len = 100_000;

    fn runBeatWithInput(ctx: *workflow.Context) ![]const u8 {
        const input = try ctx.allocator().alloc(u64, input_len);
        @memset(input, 0);
        return ctx.executeActivity(BeatWithInput, .{ .start_to_close_timeout = .fromSeconds(60) }, .{input}).get();
    }
    const RunBeatWithInput = workflow.Def("RunBeatWithInput", runBeatWithInput);
};

// The SDK decodes an activity's arguments on the activity's task, after its interceptors and
// before its function. In a Debug build on macOS, `testing.allocator` captures a stack trace on
// each allocation and waits for std's debug info lock with a wait that can be canceled and
// drops the error. A cancel pending then must still reach the activity's function.
test "a cancel pending while the SDK decodes an activity's arguments and std's debug info lock is held still reaches the activity" {
    const SelfInfo = std.debug.SelfInfo;
    if (comptime builtin.mode != .debug or !std.debug.sys_can_stack_trace or SelfInfo == void or !@hasField(SelfInfo, "mutex"))
        return error.SkipZigTest;
    const env = try CancelTest.start();
    defer CancelTest.stop(env);
    var hold: PreambleCancelTest.Hold = .{};
    const interceptors = [_]interceptor.Worker{interceptor.Worker.from(PreambleCancelTest.Hold, &hold)};
    env.worker.options.interceptors = &interceptors;
    try env.registerWorkflow(PreambleCancelTest.RunBeatWithInput);
    try env.registerActivity(PreambleCancelTest.BeatWithInput);

    var run = try env.startWorkflow(PreambleCancelTest.RunBeatWithInput, CancelTest.run_options, .{});
    defer run.deinit();
    errdefer env.client.terminateWorkflow(run.id, run.run_id, "test failed") catch {};
    try CancelTest.waitFor(&CancelTest.started, "BeatWithInput's interceptor to run");
    const si = try std.debug.getSelfDebugInfo();
    env.worker.stop();
    try CancelTest.waitForCancelReason("BeatWithInput's worker-stop cancel to reach its run");
    {
        // Nothing here may allocate: with the lock held that would wait for this thread.
        si.mutex.lockUncancelable(testing.io);
        defer si.mutex.unlock(testing.io);
        CancelTest.go.set(testing.io);
        const deadline = Io.Clock.Timestamp.fromNow(testing.io, .{ .raw = CancelTest.wait_limit, .clock = .awake });
        while (si.mutex.state.load(.monotonic) != .contended) {
            if (deadline.durationFromNow(testing.io).raw.nanoseconds <= 0) break;
            try testing.io.sleep(.fromMilliseconds(1), .awake);
        }
        try testing.io.sleep(.fromMilliseconds(200), .awake);
    }
    try CancelTest.waitFor(&CancelTest.observed, "BeatWithInput to see error.Canceled after its arguments were decoded");
    try CancelTest.awaitWorker(env);
    try testing.expectEqual(@as(?activity.CancelReason, .worker_shutdown), CancelTest.seen_reason);
}

// The debug-info-lock tests hold `BeatWhenTold` (or `BeatWithInput`'s interceptor) on
// `CancelTest.go` until the cancel has reached the run. When that wait fails, as it does when
// the cancel is lost, the test returns before setting `go`, and `CancelTest.stop` must still
// end the fixture so the test fails with its message instead of waiting without bound.
test "CancelTest.stop ends a fixture still held on go, as when a test fails before setting it" {
    const env = try CancelTest.start();
    var stop_task: ?Io.Future(void) = null;
    var stopped: Io.Event = .unset;
    // On a hang, release the fixture so the test fails rather than waits for good.
    defer if (stop_task) |*t| {
        CancelTest.go.set(testing.io);
        t.await(testing.io);
    } else CancelTest.stop(env);

    {
        var run = try env.startWorkflow(CancelTest.RunBeatWhenTold, CancelTest.run_options, .{});
        defer run.deinit();
        try CancelTest.waitFor(&CancelTest.started, "BeatWhenTold to start");
    }
    // What a test of the group does when a wait fails before `go.set`: its deferred stop.
    const S = struct {
        fn stopAndSignal(e: *CancelTest.Env, done: *Io.Event) void {
            CancelTest.stop(e);
            done.set(testing.io);
        }
    };
    stop_task = try testing.io.concurrent(S.stopAndSignal, .{ env, &stopped });
    stopped.waitTimeout(testing.io, .{ .duration = .{ .raw = .fromSeconds(20), .clock = .awake } }) catch |err| switch (err) {
        error.Timeout => {
            log.err("CancelTest.stop did not end within 20 s: BeatWhenTold is still waiting on CancelTest.go", .{});
            return error.TestUnexpectedResult;
        },
        error.Canceled => |e| return e,
    };
}

// As above, for memory the activity itself allocates from `ctx.allocator()`, the arena the SDK
// gives it (and a dynamic activity decodes its arguments into).
test "a cancel pending while an activity allocates from its context and std's debug info lock is held still reaches the activity" {
    const SelfInfo = std.debug.SelfInfo;
    if (comptime builtin.mode != .debug or !std.debug.sys_can_stack_trace or SelfInfo == void or !@hasField(SelfInfo, "mutex"))
        return error.SkipZigTest;
    const env = try CancelTest.start();
    defer CancelTest.stop(env);

    var run = try env.startWorkflow(CancelTest.RunAllocWhenTold, CancelTest.run_options, .{});
    defer run.deinit();
    errdefer env.client.terminateWorkflow(run.id, run.run_id, "test failed") catch {};
    try CancelTest.waitFor(&CancelTest.started, "AllocWhenTold to start");
    const si = try std.debug.getSelfDebugInfo();
    env.worker.stop();
    try CancelTest.waitForCancelReason("AllocWhenTold's worker-stop cancel to reach its run");
    try CancelTest.goWithDebugInfoLockHeld(si);
    try CancelTest.waitFor(&CancelTest.observed, "AllocWhenTold to see error.Canceled after allocating from ctx.allocator()");
    try CancelTest.awaitWorker(env);
    try testing.expectEqual(@as(?activity.CancelReason, .worker_shutdown), CancelTest.seen_reason);
}

/// An activity that reports the timeouts, retry policy, priority and deadline in its info.
const InfoTest = struct {
    const Env = @import("testing.zig").WorkflowEnvironment;

    fn report(ctx: *activity.Context) ![]const u8 {
        const i = ctx.info();
        var out: Io.Writer.Allocating = .init(ctx.allocator());
        const w = &out.writer;
        try w.print("s2c={?f} st2c={?f} hb={?f}", .{ i.schedule_to_close_timeout, i.start_to_close_timeout, i.heartbeat_timeout });
        if (i.retry_policy) |p| {
            try w.print(" retry={?f},{d},{?f},{d},", .{ p.initial_interval, p.backoff_coefficient, p.maximum_interval, p.maximum_attempts });
            for (p.non_retryable_error_types, 0..) |t, n| try w.print("{s}{s}", .{ if (n > 0) "+" else "", t });
        } else try w.writeAll(" retry=null");
        try w.print(" priority={d},{s},{d}", .{ i.priority.priority_key, i.priority.fairness_key, i.priority.fairness_weight });
        const deadline = i.deadline() orelse return error.NoDeadline;
        const expected = i.started_time.?.addDuration(i.start_to_close_timeout.?);
        try w.print(" deadline_is_start_plus_start_to_close={}", .{deadline.nanoseconds == expected.nanoseconds});
        return out.written();
    }
    const Report = activity.Def("ReportInfo", report);

    const retry: workflow.RetryPolicy = .{
        .initial_interval = .fromSeconds(1),
        .backoff_coefficient = 3,
        .maximum_interval = .fromSeconds(20),
        .maximum_attempts = 4,
        .non_retryable_error_types = &.{ "Fatal", "Bad" },
    };

    fn regular(ctx: *workflow.Context) ![]const u8 {
        return ctx.executeActivity(Report, .{
            .schedule_to_close_timeout = .fromSeconds(300),
            .start_to_close_timeout = .fromSeconds(30),
            .heartbeat_timeout = .fromSeconds(10),
            .retry_policy = retry,
            .priority = .{ .priority_key = 2, .fairness_key = "tenant-a", .fairness_weight = 1.5 },
        }, .{}).get();
    }
    const Regular = workflow.Def("ReportInfoRegular", regular);

    fn local(ctx: *workflow.Context) ![]const u8 {
        return ctx.executeLocalActivity(Report, .{
            .schedule_to_close_timeout = .fromSeconds(300),
            .start_to_close_timeout = .fromSeconds(30),
            .retry_policy = retry,
        }, .{}).get();
    }
    const Local = workflow.Def("ReportInfoLocal", local);
};

test "an activity's info reports the timeouts, retry policy and priority it was scheduled with" {
    const env = try InfoTest.Env.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(InfoTest.Regular);
    try env.registerWorkflow(InfoTest.Local);
    try env.registerActivity(InfoTest.Report);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    try testing.expectEqualStrings(
        "s2c=5m st2c=30s hb=10s retry=1s,3,20s,4,Fatal+Bad priority=2,tenant-a,1.5 deadline_is_start_plus_start_to_close=true",
        try env.executeWorkflow(InfoTest.Regular, .{}, a, null),
    );
    try testing.expectEqualStrings(
        "s2c=5m st2c=30s hb=null retry=1s,3,20s,4,Fatal+Bad priority=0,,0 deadline_is_start_plus_start_to_close=true",
        try env.executeWorkflow(InfoTest.Local, .{}, a, null),
    );
}

/// Activities that wait for the worker to stop, run as a regular activity, a local activity
/// and an activity in a session.
const WorkerStopTest = struct {
    const Env = @import("testing.zig").WorkflowEnvironment;

    var started: Io.Event = .unset;
    var returned: Io.Event = .unset;
    var returned_at: Io.Timestamp = .zero;
    var stopping_before: bool = true;

    /// Waits for the worker to stop on a coroutine of its own and returns.
    fn waitStop(ctx: *activity.Context) ![]const u8 {
        stopping_before = ctx.isWorkerStopping();
        var waiting = try ctx.io().concurrent(activity.Context.waitForWorkerStop, .{ctx});
        started.set(ctx.io());
        try waiting.await(ctx.io());
        if (!ctx.isWorkerStopping()) return error.NotStopping;
        returned_at = .now(ctx.io(), .awake);
        returned.set(ctx.io());
        return "drained";
    }
    const WaitStop = activity.Def("WaitForWorkerStop", waitStop);

    fn regular(ctx: *workflow.Context) ![]const u8 {
        return ctx.executeActivity(WaitStop, .{ .start_to_close_timeout = .fromSeconds(60) }, .{}).get();
    }
    const Regular = workflow.Def("WaitStopRegular", regular);

    fn local(ctx: *workflow.Context) ![]const u8 {
        return ctx.executeLocalActivity(WaitStop, .{ .start_to_close_timeout = .fromSeconds(60) }, .{}).get();
    }
    const Local = workflow.Def("WaitStopLocal", local);

    fn inSession(ctx: *workflow.Context) ![]const u8 {
        const s = try ctx.createSession(.{ .creation_timeout = .fromSeconds(30), .execution_timeout = .fromSeconds(120) });
        return ctx.executeActivity(WaitStop, .{ .start_to_close_timeout = .fromSeconds(60), .session = s }, .{}).get();
    }
    const InSession = workflow.Def("WaitStopInSession", inSession);

    /// Starts `Workflow` on a worker with `period` to shut down gracefully, stops the worker
    /// once the activity waits, and expects the activity to have returned before the period
    /// passed, without a cancel. Returns the environment and the run for more checks.
    fn stopWhileWaiting(comptime Workflow: type, period: Io.Duration, sessions: bool) !struct { *Env, client_mod.WorkflowRun([]const u8) } {
        started = .unset;
        returned = .unset;
        stopping_before = true;
        const env = try Env.start(testing.allocator, testing.io);
        errdefer env.deinit();
        env.worker.options.graceful_shutdown_period = period;
        if (sessions) env.worker.options.sessions = .{};
        try env.registerWorkflow(Workflow);
        try env.registerActivity(WaitStop);

        var run = try env.startWorkflow(Workflow, .{ .task_queue = "" }, .{});
        errdefer run.deinit();
        try started.wait(testing.io);
        const stopped_at = Io.Timestamp.now(testing.io, .awake);
        env.worker.stop();
        try env.running.?.await(testing.io);
        env.running = null;

        try testing.expect(!stopping_before);
        try testing.expect(returned.isSet());
        try testing.expect(stopped_at.durationTo(returned_at).nanoseconds < period.nanoseconds);
        return .{ env, run };
    }
};

test "an activity waiting in waitForWorkerStop returns when the worker stops and completes" {
    const period: Io.Duration = .fromSeconds(30);
    const env, var run = try WorkerStopTest.stopWhileWaiting(WorkerStopTest.Regular, period, false);
    defer env.deinit();
    defer run.deinit();
    try testing.expectEqual(1, try CancelTest.countEvents(env, &run, .EVENT_TYPE_ACTIVITY_TASK_COMPLETED));
    try testing.expectEqual(0, try CancelTest.countEvents(env, &run, .EVENT_TYPE_ACTIVITY_TASK_CANCELED));
}

test "a local activity waiting in waitForWorkerStop returns when the worker stops" {
    const env, var run = try WorkerStopTest.stopWhileWaiting(WorkerStopTest.Local, .fromSeconds(30), false);
    defer env.deinit();
    defer run.deinit();
}

test "an activity in a session waiting in waitForWorkerStop returns when the worker stops" {
    // The session itself is held until the graceful period ends, so keep it short.
    const env, var run = try WorkerStopTest.stopWhileWaiting(WorkerStopTest.InSession, .fromSeconds(5), true);
    defer env.deinit();
    defer run.deinit();
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

test "a worker runs again after a poll failure, and stays stopped after stop" {
    const rt = try bridge.Runtime.init(testing.allocator, .{}, null);
    defer rt.deinit();
    var server = try bridge.EphemeralServer.startDevServer(testing.allocator, testing.io, rt, .{}, null);
    defer server.shutdown(testing.io, null) catch {};
    var client = try client_mod.Client.connect(testing.allocator, testing.io, .{ .host_port = server.target });
    defer client.close();

    var w = try Worker.init(testing.allocator, testing.io, &client, "run-after-poll-failure", .{});
    defer w.deinit();
    try w.registerWorkflow(HelloTest.Hello);
    try w.registerActivity(HelloTest.Greet);

    // Run 1 ends with a poll failure.
    var first = try testing.io.concurrent(HelloTest.runWorker, .{&w});
    try w.waitUntilRunning();
    w.recordPollFailure(.activity, w.task_queue, error.PollFailed, "boom");
    try testing.expectError(error.PollFailed, first.await(testing.io));

    // Run 2 polls again and runs a workflow and its activity; the workflow's result is what
    // shows run 2 polls.
    var second = try testing.io.concurrent(HelloTest.runWorker, .{&w});
    var second_done = false;
    defer if (!second_done) {
        w.stop();
        second.await(testing.io) catch {};
    };
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var run = try client.executeWorkflow(HelloTest.Hello, .{ .id = "run_after_poll_failure", .task_queue = "run-after-poll-failure", .execution_timeout = .fromSeconds(20) }, .{"Temporal"});
    defer run.deinit();
    try testing.expectEqualStrings("Hello Temporal!", try run.get(arena.allocator()));
    w.stop();
    second_done = true;
    try second.await(testing.io);
    try testing.expectEqual(@as(?[]const u8, null), w.fatalMessage());

    // A user's stop is permanent: run 3 shuts down what it created and returns at once.
    try w.run();
    // It polled nothing, so `waitUntilRunning` reports it as failed to start, not run 2.
    try testing.expectError(error.WorkerFailedToStart, w.waitUntilRunning());
    try testing.expectEqual(@as(?[]const u8, null), w.fatalMessage());
    w.stop_mutex.lockUncancelable(testing.io);
    defer w.stop_mutex.unlock(testing.io);
    try testing.expect(w.stop_requested);
    try testing.expectEqual(@as(?*bridge.c.TemporalCoreWorker, null), w.core_ptrs[0]);
}

test "waitUntilRunning reports the current run, not a first run that failed to start" {
    const rt = try bridge.Runtime.init(testing.allocator, .{}, null);
    defer rt.deinit();
    var server = try bridge.EphemeralServer.startDevServer(testing.allocator, testing.io, rt, .{}, null);
    defer server.shutdown(testing.io, null) catch {};
    var client = try client_mod.Client.connect(testing.allocator, testing.io, .{ .host_port = server.target });
    defer client.close();

    var w = try Worker.init(testing.allocator, testing.io, &client, "wait-until-running-current", .{});
    defer w.deinit();

    // Before any run, a waiter blocks for the first, until it is canceled.
    var canceled = try testing.io.concurrent(Worker.waitUntilRunning, .{&w});
    try testing.expectError(error.Canceled, canceled.cancel(testing.io));

    // Run 1 fails to start.
    try testing.expectError(error.NothingRegistered, w.run());
    try testing.expectError(error.WorkerFailedToStart, w.waitUntilRunning());

    // Run 2 polls: the workflow's result shows it, and `waitUntilRunning` then reports run 2.
    try w.registerWorkflow(HelloTest.Hello);
    try w.registerActivity(HelloTest.Greet);
    var second = try testing.io.concurrent(HelloTest.runWorker, .{&w});
    var second_done = false;
    defer if (!second_done) {
        w.stop();
        second.await(testing.io) catch {};
    };
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var run = try client.executeWorkflow(HelloTest.Hello, .{ .id = "wait_until_running_current", .task_queue = "wait-until-running-current", .execution_timeout = .fromSeconds(20) }, .{"Temporal"});
    defer run.deinit();
    try testing.expectEqualStrings("Hello Temporal!", try run.get(arena.allocator()));
    try w.waitUntilRunning();

    // After it returns, it is still the most recent run.
    w.stop();
    second_done = true;
    try second.await(testing.io);
    try w.waitUntilRunning();

    // A run after `stop` polls nothing, so it failed to start.
    try w.run();
    try testing.expectError(error.WorkerFailedToStart, w.waitUntilRunning());
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
            r.changed.broadcast(testing.io);
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

    /// Waits up to `timeout` for more than `seen` core records.
    fn waitForCoreRecordsPast(r: *RecordingSink, seen: usize, timeout: Io.Duration) !void {
        const deadline = Io.Clock.Timestamp.fromNow(testing.io, .{ .raw = timeout, .clock = .awake });
        r.mutex.lockUncancelable(testing.io);
        defer r.mutex.unlock(testing.io);
        while (r.core_records <= seen) try r.changed.waitTimeout(testing.io, &r.mutex, .{ .deadline = deadline });
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

const StandaloneLogTest = struct {
    fn refuse(ctx: *activity.Context) !void {
        ctx.logger().info("Refusing", .{});
        return ctx.fail(.{ .message = "not today", .non_retryable = true });
    }
    const Refuse = activity.Def("Refuse", refuse);
};

test "a standalone activity's logs carry its activity fields and no empty workflow fields" {
    const rt = try bridge.Runtime.init(testing.allocator, .{}, null);
    defer rt.deinit();
    var server = try bridge.EphemeralServer.startDevServer(testing.allocator, testing.io, rt, .{ .extra_args = &@import("testing.zig").DevServer.standalone_activity_args }, null);
    defer server.shutdown(testing.io, null) catch {};
    var client = try client_mod.Client.connect(testing.allocator, testing.io, .{ .host_port = server.target });
    defer client.close();
    var sink: RecordingSink = .{};
    defer sink.deinit();
    var w = try Worker.init(testing.allocator, testing.io, &client, "standalone-logs", .{ .logger = sink.sink() });
    defer w.deinit();
    try w.registerActivity(StandaloneLogTest.Refuse);
    var running = try testing.io.concurrent(HelloTest.runWorker, .{&w});
    defer {
        w.stop();
        running.await(testing.io) catch {};
    }

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var handle = try client.executeActivity(StandaloneLogTest.Refuse, .{ .id = "standalone-logs-act", .task_queue = "standalone-logs", .start_to_close_timeout = .fromSeconds(10) }, .{});
    defer handle.deinit();
    try testing.expectError(error.ActivityFailed, handle.get(arena.allocator()));
    const fields = [_]log_mod.Field{
        .{ .key = "ActivityID", .value = "standalone-logs-act" },
        .{ .key = "ActivityType", .value = "Refuse" },
        .{ .key = "Attempt", .value = "1" },
    };
    try sink.waitFor("Activity failed", &fields, .fromSeconds(10));
    try testing.expectEqual(1, sink.count("Refusing", &fields));
    sink.mutex.lockUncancelable(testing.io);
    defer sink.mutex.unlock(testing.io);
    var checked: usize = 0;
    for (sink.records.items) |rec| {
        if (!std.mem.eql(u8, rec.message, "Activity failed") and !std.mem.eql(u8, rec.message, "Refusing")) continue;
        checked += 1;
        for (rec.fields) |f| for ([_][]const u8{ "WorkflowType", "WorkflowID", "RunID" }) |absent| {
            if (std.mem.eql(u8, f.key, absent)) {
                std.log.err("record \"{s}\" has the field {s}=\"{s}\"", .{ rec.message, f.key, f.value });
                return error.TestUnexpectedResult;
            }
        };
    }
    try testing.expectEqual(2, checked);
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

    fn encode(ptr: *anyopaque, arena: Allocator, _: Io, context: *const converter.Context, payloads: []const converter.Payload) converter.CodecError![]converter.Payload {
        try record(ptr, "encode", context);
        return arena.dupe(converter.Payload, payloads);
    }

    fn decode(ptr: *anyopaque, arena: Allocator, _: Io, context: *const converter.Context, payloads: []const converter.Payload) converter.CodecError![]converter.Payload {
        try record(ptr, "decode", context);
        return arena.dupe(converter.Payload, payloads);
    }

    fn record(ptr: *anyopaque, what: []const u8, context: *const converter.Context) !void {
        const r: *ContextRecordingCodec = @ptrCast(@alignCast(ptr));
        const call = try std.fmt.allocPrint(r.arena, "{s} tenant={s} {s}/{s}/{s}", .{ what, context.values.get(tenant) orelse "-", context.namespace, context.workflow_type, context.workflow_id });
        try r.calls.append(r.arena, call);
    }
};

test "an activity's info carries its Start task's times, timeouts, retry policy and priority" {
    var start: task_pb.Start = .{ .activity_id = "1", .activity_type = "Charge", .attempt = 2 };
    {
        const bare = activityInfo(&start, "tq", "token");
        try testing.expect(bare.started_time == null);
        try testing.expectEqual(null, bare.schedule_to_close_timeout);
        try testing.expectEqual(null, bare.start_to_close_timeout);
        try testing.expectEqual(null, bare.heartbeat_timeout);
        try testing.expect(bare.retry_policy == null);
        try testing.expectEqual(@as(i32, 0), bare.priority.priority_key);
        try testing.expectEqualStrings("", bare.priority.fairness_key);
        try testing.expectEqual(@as(f32, 0), bare.priority.fairness_weight);
        try testing.expectEqual(null, bare.deadline());
    }
    // Zero durations mean unset.
    start.schedule_to_close_timeout = .{};
    start.heartbeat_timeout = .{ .seconds = 0 };
    {
        const zero = activityInfo(&start, "tq", "token");
        try testing.expectEqual(null, zero.schedule_to_close_timeout);
        try testing.expectEqual(null, zero.heartbeat_timeout);
    }

    var non_retryable = [_][]const u8{"Fatal"};
    start.schedule_to_close_timeout = .{ .seconds = 300 };
    start.start_to_close_timeout = .{ .seconds = 30, .nanos = 5 };
    start.heartbeat_timeout = .{ .seconds = 10 };
    start.retry_policy = .{
        .initial_interval = .{ .seconds = 1 },
        .backoff_coefficient = 2,
        .maximum_attempts = 5,
        .non_retryable_error_types = .fromOwnedSlice(&non_retryable),
    };
    start.priority = .{ .priority_key = 2, .fairness_key = "tenant-a", .fairness_weight = 1.5 };
    start.scheduled_time = .{ .seconds = 100 };
    start.current_attempt_scheduled_time = .{ .seconds = 105, .nanos = 7 };
    start.started_time = .{ .seconds = 106 };
    const info = activityInfo(&start, "tq", "token");
    try testing.expectEqual(@as(i96, 100 * std.time.ns_per_s), info.scheduled_time.?.nanoseconds);
    try testing.expectEqual(@as(i96, 105 * std.time.ns_per_s + 7), info.current_attempt_scheduled_time.?.nanoseconds);
    try testing.expectEqual(@as(i96, 106 * std.time.ns_per_s), info.started_time.?.nanoseconds);
    try testing.expectEqualStrings("Charge", info.activity_type);
    try testing.expectEqual(@as(u32, 2), info.attempt);
    try testing.expectEqual(@as(i96, 300 * std.time.ns_per_s), info.schedule_to_close_timeout.?.nanoseconds);
    try testing.expectEqual(@as(i96, 30 * std.time.ns_per_s + 5), info.start_to_close_timeout.?.nanoseconds);
    try testing.expectEqual(@as(i96, 10 * std.time.ns_per_s), info.heartbeat_timeout.?.nanoseconds);
    const rp = info.retry_policy.?;
    try testing.expectEqual(@as(i96, std.time.ns_per_s), rp.initial_interval.?.nanoseconds);
    try testing.expectEqual(@as(f64, 2), rp.backoff_coefficient);
    try testing.expectEqual(null, rp.maximum_interval);
    try testing.expectEqual(@as(i32, 5), rp.maximum_attempts);
    try testing.expectEqual(1, rp.non_retryable_error_types.len);
    try testing.expectEqualStrings("Fatal", rp.non_retryable_error_types[0]);
    try testing.expectEqual(@as(i32, 2), info.priority.priority_key);
    try testing.expectEqualStrings("tenant-a", info.priority.fairness_key);
    try testing.expectEqual(@as(f32, 1.5), info.priority.fairness_weight);
    // Started at 106 s with 30 s to run, before the schedule-to-close end at 400 s.
    try testing.expectEqual(@as(i96, 136 * std.time.ns_per_s + 5), info.deadline().?.nanoseconds);
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

const NexusCodecTest = struct {
    const Echo = nexus.OperationDef("echo", []const u8, []const u8);
    const Service = nexus.ServiceDef("hello-service", .{Echo});

    fn call(ctx: *workflow.Context) ![]const u8 {
        return ctx.executeNexusOperation(Service, Echo, .{ .endpoint = "ep", .summary = "echo it" }, "hi").get();
    }
    const Call = workflow.Def("CallNexus", call);
};

test "the workflow runner encodes a Nexus operation's input and decodes its result with the codecs" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const zlib: converter.ZlibCodec = .{ .always_encode = true };
    const codecs = [_]converter.PayloadCodec{zlib.codec()};
    var workflows: std.StringHashMapUnmanaged(workflow.Registered) = .empty;
    defer workflows.deinit(testing.allocator);
    try workflows.put(testing.allocator, NexusCodecTest.Call.workflow_name, NexusCodecTest.Call.registered);
    var runner: WorkflowRunner = .{
        .gpa = testing.allocator,
        .workflows = &workflows,
        .task_queue = "tq",
        .namespace = "ns",
        .stack_size = 256 * 1024,
        .codecs = &codecs,
    };
    defer runner.deinit();

    const init: act_pb.InitializeWorkflow = .{ .workflow_type = "CallNexus", .workflow_id = "wf-1", .randomness_seed = 7, .attempt = 1 };
    var act: act_pb.WorkflowActivation = .{ .run_id = "run-1", .timestamp = .{ .seconds = 1_700_000_000 } };
    try act.jobs.append(a, .{ .variant = .{ .initialize_workflow = init } });
    var bytes: Io.Writer.Allocating = .init(a);
    try act.encode(&bytes.writer, a);
    const first = (try runner.handle(bytes.written(), a, testing.io)).completion;
    const command = first.status.?.successful.commands.items[0];
    const input = command.variant.?.schedule_nexus_operation.input.?;
    try testing.expectEqualStrings(converter.ZlibCodec.encoding, converter.encodingOf(input).?);
    try testing.expectEqualStrings(converter.ZlibCodec.encoding, converter.encodingOf(command.user_metadata.?.summary.?).?);
    const decoded_input = try converter.decodePayloads(&codecs, a, testing.io, &.{input});
    try testing.expectEqualStrings("\"hi\"", decoded_input[0].data);

    const output = try converter.encodePayloads(&codecs, a, testing.io, &.{try converter.toPayload(a, @as([]const u8, "hi back"))});
    var resolve: act_pb.WorkflowActivation = .{ .run_id = "run-1", .timestamp = .{ .seconds = 1_700_000_001 } };
    try resolve.jobs.append(a, .{ .variant = .{ .resolve_nexus_operation_start = .{ .seq = 1, .status = .{ .started_sync = true } } } });
    try resolve.jobs.append(a, .{ .variant = .{ .resolve_nexus_operation = .{ .seq = 1, .result = .{ .status = .{ .completed = output[0] } } } } });
    bytes.clearRetainingCapacity();
    try resolve.encode(&bytes.writer, a);
    const second = (try runner.handle(bytes.written(), a, testing.io)).completion;
    const result = second.status.?.successful.commands.items[0].variant.?.complete_workflow_execution.result.?;
    // The workflow saw the decoded result; its own result is encoded again.
    try testing.expectEqualStrings("\"hi back\"", (try converter.decodePayloads(&codecs, a, testing.io, &.{result}))[0].data);
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

    const response = runner.respond(try ParallelTest.start(a, "run-1", "ParallelHold", "x"), failing.allocator(), testing.io, &buf, null);
    try testing.expect(response.completion.fallback);
    const completion = try ParallelTest.decodeCompletion(a, response.completion.bytes);
    try testing.expectEqualStrings("run-1", completion.run_id);
    try testing.expectEqualStrings("the worker could not process this workflow activation: OutOfMemory; see the worker's log", completion.status.?.failed.failure.?.message);
    try testing.expectEqual(1, sink.countAt(.err, "processing a workflow activation failed", &.{ .{ .key = "run_id", .value = "run-1" }, .{ .key = "error", .value = "OutOfMemory" } }));

    // No run ID: nothing can be sent.
    var no_run_id: act_pb.WorkflowActivation = .{ .timestamp = .{ .seconds = 1 } };
    try no_run_id.jobs.append(a, .{ .variant = .{ .fire_timer = .{ .seq = 1 } } });
    try testing.expect(runner.respond(try ParallelTest.encode(a, no_run_id), failing.allocator(), testing.io, &buf, null) == .none);

    // A run ID too long for the buffer: the core is asked to evict the run.
    const long_id = try a.alloc(u8, 2 * WorkflowRunner.fallback_buffer_size);
    @memset(long_id, 'r');
    const evict = runner.respond(try ParallelTest.fireTimer(a, long_id, 1), failing.allocator(), testing.io, &buf, null).evict;
    try testing.expectEqualStrings(long_id, evict.run_id);
}

// With `encode_failure_attributes`, a task failure `handle` returns early has its attributes
// encoded in `respond`; running out of memory there still answers the activation's own run.
test "a fallback failed completion after encoding an early task failure's attributes runs out of memory names the activation's run" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var workflows = try ParallelTest.registry(testing.allocator);
    defer workflows.deinit(testing.allocator);
    var sink: RecordingSink = .{};
    defer sink.deinit();
    var runner = ParallelTest.runner(&workflows, sink.sink());
    runner.encode_failure_attributes = true;
    defer runner.deinit();
    var buf: [WorkflowRunner.fallback_buffer_size]u8 = undefined;
    // Not registered: `handle` returns a task failure without running workflow code. That
    // failure's std.log warning is expected.
    testing.log_level = .err;
    const bytes = try ParallelTest.start(a, "run-1", "NotRegistered", "x");

    var fallbacks: usize = 0;
    var fail_index: usize = 0;
    while (fail_index < 1000) : (fail_index += 1) {
        var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
        defer scratch.deinit();
        var failing: std.testing.FailingAllocator = .init(scratch.allocator(), .{ .fail_index = fail_index });
        const response = runner.respond(bytes, failing.allocator(), testing.io, &buf, null);
        if (!failing.has_induced_failure) break;
        switch (response) {
            .completion => |c| if (c.fallback) {
                fallbacks += 1;
                const completion = try ParallelTest.decodeCompletion(a, c.bytes);
                try testing.expectEqualStrings("run-1", completion.run_id);
            },
            .evict => |e| try testing.expectEqualStrings("run-1", e.run_id),
            else => {},
        }
    }
    try testing.expect(fallbacks > 0);
}

// A core worker with workflows disabled rejects every workflow completion, which drives
// `process` through each of its send paths with a real core.
test "a completion the core rejects falls back to a failed completion, then to an eviction request" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const rt = try bridge.Runtime.init(testing.allocator, .{}, null);
    defer rt.deinit();
    var server = try bridge.EphemeralServer.startDevServer(testing.allocator, testing.io, rt, .{}, null);
    defer server.shutdown(testing.io, null) catch {};
    // The core logs the eviction request it can't act on; that goes here, not to std.log.
    var client_sink: RecordingSink = .{};
    defer client_sink.deinit();
    var client = try client_mod.Client.connect(testing.allocator, testing.io, .{ .host_port = server.target, .logger = client_sink.sink() });
    defer client.close();
    var message: ?[]u8 = null;
    defer if (message) |m| testing.allocator.free(m);
    const core = try bridge.Worker.init(testing.allocator, try client.currentConnection(), .{
        .namespace = "default",
        .task_queue = "rejecting",
        .task_types = .{ .workflows = false, .remote_activities = true, .local_activities = false, .nexus = false },
    }, &message);
    defer core.deinit();
    defer {
        core.initiateShutdown();
        while (core.pollActivityTask(testing.allocator, testing.io, null) catch null) |polled| polled.deinit();
        core.finalizeShutdown(testing.allocator, testing.io, null) catch {};
    }

    var workflows = try ParallelTest.registry(testing.allocator);
    defer workflows.deinit(testing.allocator);
    var sink: RecordingSink = .{};
    defer sink.deinit();
    var runner = ParallelTest.runner(&workflows, sink.sink());
    defer runner.deinit();

    // Handled normally, rejected; the fallback, rejected; then the eviction request.
    runner.process(core, testing.io, try ParallelTest.start(a, "run-1", "ParallelHold", "x"));
    const run_1 = [_]log_mod.Field{.{ .key = "run_id", .value = "run-1" }};
    try testing.expectEqual(2, sink.countAt(.err, "the core rejected a workflow activation completion", &run_1));
    try testing.expectEqual(1, sink.countAt(.err, "no completion could be delivered for a workflow activation; asked the core to evict the run", &.{
        .{ .key = "run_id", .value = "run-1" },
        .{ .key = "reason", .value = "the core rejected the fallback failed completion" },
    }));

    // An eviction's completion that is rejected is only logged.
    runner.process(core, testing.io, try ParallelTest.evict(a, "run-1", .CACHE_FULL));
    try testing.expectEqual(3, sink.countAt(.err, "the core rejected a workflow activation completion", &run_1));
    try testing.expectEqual(1, sink.count("no completion could be delivered for a workflow activation; asked the core to evict the run", &.{}));

    // Out of memory before the core saw the completion: it is sent again with the bridge's
    // cell on the stack, and reaches the core, which rejects it here.
    var failing: std.testing.FailingAllocator = .init(testing.allocator, .{ .fail_index = 0 });
    runner.gpa = failing.allocator();
    defer runner.gpa = testing.allocator;
    var completion: Io.Writer.Allocating = .init(a);
    try (done_pb.WorkflowActivationCompletion{ .run_id = "run-2", .status = .{ .successful = .{} } }).encode(&completion.writer, a);
    var no_message: ?[]u8 = null;
    try testing.expectError(error.CompletionFailed, runner.sendCompletion(core, testing.io, completion.written(), &no_message));
    try testing.expect(no_message == null);
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

test "an activity task's token and kind are read from its bytes without decoding it" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var encoded: Io.Writer.Allocating = .init(a);
    try (task_pb.ActivityTask{ .task_token = "token-1", .variant = .{ .start = .{ .activity_id = "act-1", .activity_type = "Echo" } } }).encode(&encoded.writer, a);
    const start = encoded.written();
    const head = ActivityTaskHead.read(start).?;
    try testing.expectEqualStrings("token-1", head.token);
    try testing.expect(head.kind == .start);
    // Truncated anywhere, the bytes don't read.
    for (0..start.len) |n| try testing.expect(ActivityTaskHead.read(start[0..n]) == null);

    for ([_]task_pb.ActivityCancelReason{ .CANCELLED, .TIMED_OUT, .WORKER_SHUTDOWN, .PAUSED, .RESET, @fromBackingInt(42), @fromBackingInt(-1) }) |reason| {
        var out: Io.Writer.Allocating = .init(a);
        try (task_pb.ActivityTask{ .task_token = "token-2", .variant = .{ .cancel = .{ .reason = reason } } }).encode(&out.writer, a);
        const cancel = ActivityTaskHead.read(out.written()).?;
        try testing.expectEqualStrings("token-2", cancel.token);
        try testing.expectEqual(reason, cancel.kind.cancel);
    }
    // A Cancel with reason NOT_FOUND and no details is an empty submessage.
    const not_found = ActivityTaskHead.read(&.{ 0x0a, 3, 't', 'o', 'k', 0x22, 0 }).?;
    try testing.expectEqualStrings("tok", not_found.token);
    try testing.expectEqual(task_pb.ActivityCancelReason.NOT_FOUND, not_found.kind.cancel);
    // No token, an empty one, or no variant.
    try testing.expect(ActivityTaskHead.read(&.{ 0x22, 0 }) == null);
    try testing.expect(ActivityTaskHead.read(&.{ 0x0a, 0, 0x22, 0 }) == null);
    try testing.expect(ActivityTaskHead.read(&.{ 0x0a, 3, 't', 'o', 'k' }) == null);
}

test "a Nexus task's token and kind are read from its bytes without decoding it" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var encoded: Io.Writer.Allocating = .init(a);
    try (nexus_pb.NexusTask{ .endpoint = "endpoint", .variant = .{ .task = .{
        .task_token = "token-1",
        .request = .{ .variant = .{ .start_operation = .{ .service = "svc", .operation = "op" } } },
    } } }).encode(&encoded.writer, a);
    const task = encoded.written();
    const head = NexusTaskHead.read(task).?;
    try testing.expectEqualStrings("token-1", head.token);
    try testing.expect(head.kind == .task);
    for (0..task.len) |n| try testing.expect(NexusTaskHead.read(task[0..n]) == null);

    for ([_]nexus_pb.NexusTaskCancelReason{ .TIMED_OUT, .WORKER_SHUTDOWN, @fromBackingInt(7) }) |reason| {
        var out: Io.Writer.Allocating = .init(a);
        try (nexus_pb.NexusTask{ .variant = .{ .cancel_task = .{ .task_token = "token-2", .reason = reason } } }).encode(&out.writer, a);
        const cancel = NexusTaskHead.read(out.written()).?;
        try testing.expectEqualStrings("token-2", cancel.token);
        try testing.expectEqual(reason, cancel.kind.cancel);
    }
    // A cancel without a token, or a task with neither variant.
    try testing.expect(NexusTaskHead.read(&.{ 0x12, 2, 0x10, 1 }) == null);
    try testing.expect(NexusTaskHead.read(&.{ 0x22, 1, 'e' }) == null);
}

test "a failed activity completion is built and encoded without the heap" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var buffer: [failed_activity_completion_len]u8 = undefined;
    const encoded = encodeFailedActivityCompletion(&buffer, "task-token", .{ .message = "the worker ran out of memory", .type = "OutOfMemory" }).?;
    var reader: Io.Reader = .fixed(encoded);
    const completion = try proto.coresdk.self.ActivityTaskCompletion.decode(&reader, arena.allocator());
    try testing.expectEqualStrings("task-token", completion.task_token);
    const failure = completion.result.?.status.?.failed.failure.?;
    try testing.expectEqualStrings("the worker ran out of memory", failure.message);
    const info = failure.failure_info.?.application_failure_info;
    try testing.expectEqualStrings("OutOfMemory", info.type);
    try testing.expect(!info.non_retryable);

    const long: [8 * 1024]u8 = @splat('t');
    try testing.expect(encodeFailedActivityCompletion(&buffer, &long, .{ .message = "m" }) == null);
}

test "a cancel task reaches its running activity without allocating" {
    var client: client_mod.Client = undefined;
    var w = try Worker.init(testing.allocator, testing.io, &client, "cancels", .{});
    defer w.deinit();
    var running: RunningActivities = .{};
    defer running.map.deinit(testing.allocator);
    var run: RunningActivity = .{
        .task_gpa = .{ .io = testing.io, .backing = testing.allocator },
        .arena = .init(testing.allocator),
        .polled = undefined,
        .task_queue = "cancels",
        .task_token = "token-1",
        .start = .{ .activity_id = "act-1" },
        .ctx = .{ .arena = undefined, .io_ = testing.io, .info_ = undefined, .logger_ = .{} },
    };
    defer run.arena.deinit();
    try running.map.put(testing.allocator, run.task_token, &run);

    var failing: std.testing.FailingAllocator = .init(testing.allocator, .{ .fail_index = 0 });
    w.gpa = failing.allocator();
    defer w.gpa = testing.allocator;
    deliverActivityCancel(&w, &running, "token-1", .CANCELLED);
    try testing.expectEqual(activity.CancelReason.canceled, run.ctx.cancelReason().?);
    try testing.expect(run.wake.isSet());
    // A later cancel keeps the first reason; one for an activity not running is ignored.
    deliverActivityCancel(&w, &running, "token-1", .TIMED_OUT);
    deliverActivityCancel(&w, &running, "token-2", .TIMED_OUT);
    try testing.expectEqual(activity.CancelReason.canceled, run.ctx.cancelReason().?);
    try testing.expectEqual(0, failing.allocations);
    try testing.expect(!failing.has_induced_failure);
}

test "a Nexus task that ran out of memory decoding fails retryably, a malformed one as a bad request" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var header = [_]proto.temporal.api.nexus.v1.Request.HeaderEntry{.{ .key = "request-id", .value = "r-1" }};
    const task: nexus_pb.NexusTask = .{ .endpoint = "endpoint", .variant = .{ .task = .{
        .task_token = "task-token",
        .request = .{ .header = .fromOwnedSlice(&header), .variant = .{ .start_operation = .{ .service = "svc", .operation = "op" } } },
    } } };
    var encoded: Io.Writer.Allocating = .init(a);
    try task.encode(&encoded.writer, a);
    const valid = encoded.written();

    // The completion `failUndecodableNexusTask` sends for a decode that failed with `err`.
    const Sent = struct {
        fn failure(arena_a: Allocator, bytes: []const u8, err: anyerror) !proto.temporal.api.failure.v1.Failure {
            const token = runId(runId(bytes).?).?;
            var message_buf: [512]u8 = undefined;
            var buffer: [wire.failed_completion_buffer_len]u8 = undefined;
            const completion = wire.encodeFailedCompletion(&buffer, token, undecodableNexusError(err, "queue", &message_buf)).?;
            var reader: Io.Reader = .fixed(completion);
            const decoded = try wire.Completion.decode(&reader, arena_a);
            try testing.expectEqualStrings("task-token", decoded.task_token);
            return decoded.status.?.failure;
        }
    };

    // A one-time allocation failure while decoding a valid task.
    var failing: std.testing.FailingAllocator = .init(a, .{ .fail_index = 0 });
    var reader: Io.Reader = .fixed(valid);
    const oom = if (nexus_pb.NexusTask.decode(&reader, failing.allocator())) |_| return error.TestUnexpectedResult else |err| err;
    try testing.expectEqual(error.OutOfMemory, oom);
    const retryable = try Sent.failure(a, valid, oom);
    try testing.expectEqualStrings("the worker on task queue queue ran out of memory decoding the Nexus request", retryable.message);
    try testing.expectEqualStrings("INTERNAL", retryable.failure_info.?.nexus_handler_failure_info.type);
    try testing.expectEqual(.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_RETRYABLE, retryable.failure_info.?.nexus_handler_failure_info.retry_behavior);

    // The same task with an endpoint that isn't UTF-8 appended (field 4; the last one read wins).
    const malformed = try std.mem.concat(a, u8, &.{ valid, &.{ 0x22, 2, 0xff, 0xfe } });
    reader = .fixed(malformed);
    const bad = if (nexus_pb.NexusTask.decode(&reader, a)) |_| return error.TestUnexpectedResult else |err| err;
    try testing.expect(bad != error.OutOfMemory);
    const refused = try Sent.failure(a, malformed, bad);
    try testing.expect(std.mem.startsWith(u8, refused.message, "the worker on task queue queue couldn't decode the Nexus request: "));
    try testing.expectEqualStrings("BAD_REQUEST", refused.failure_info.?.nexus_handler_failure_info.type);
    try testing.expectEqual(.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_NON_RETRYABLE, refused.failure_info.?.nexus_handler_failure_info.retry_behavior);

    // Without room for the task queue, the message leaves it out.
    var small: [8]u8 = undefined;
    try testing.expectEqualStrings("the worker ran out of memory decoding the Nexus request", undecodableNexusError(error.OutOfMemory, "queue", &small).message);
    try testing.expectEqualStrings("the worker couldn't decode the Nexus request", undecodableNexusError(error.InvalidInput, "queue", &small).message);
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

    /// Waits until the run's history has a started timer: its first workflow task completed.
    fn waitForTimer(c: *client_mod.Client, id: []const u8, run_id: []const u8) !void {
        const deadline = Io.Clock.Timestamp.fromNow(testing.io, .{ .raw = .fromSeconds(30), .clock = .awake });
        while (true) {
            var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
            defer scratch.deinit();
            const history = try c.fetchHistory(scratch.allocator(), id, run_id);
            for (history.events.items) |event| switch (event.attributes orelse continue) {
                .timer_started_event_attributes => return,
                else => {},
            };
            if (deadline.durationFromNow(testing.io).raw.nanoseconds <= 0) return error.Timeout;
            try testing.io.sleep(.fromMilliseconds(20), .awake);
        }
    }
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

// The cache holds one run, not none: with max_cached_workflows = 0 the core requests a
// CACHE_FULL eviction after every completion, including the one in which it finds the
// nondeterminism, and it keeps only the most recent reason requested for a run, so the eviction
// would arrive as CACHE_FULL. Here the run is pushed out by another run and replayed later.
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
    // Started before Drift's first task completed, the other run could take the cache slot
    // first, and Drift's task would evict it instead of the other way round.
    try CacheTest.waitForTimer(&client, "nondeterminism-wf", drift.run_id);
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

/// Deadlock detection. Workflow code here blocks or spins on the test's real Io, outside the
/// workflow's: it stands in for code that never returns to the workflow scheduler. Real
/// workflow code must never do this.
const DeadlockTest = struct {
    /// Collects what the watchdog sends, in place of a core.
    const Sent = struct {
        mutex: Io.Mutex = .init,
        arena: std.heap.ArenaAllocator = .init(testing.allocator),
        responses: std.ArrayList([]const u8) = .empty,
        received: Io.Event = .unset,

        fn sender(s: *Sent) WorkflowRunner.Sender {
            return .{ .ctx = s, .send = send };
        }

        fn send(ctx: *anyopaque, run_id: []const u8, response: WorkflowRunner.Response) void {
            const s: *Sent = @ptrCast(@alignCast(ctx));
            s.mutex.lockUncancelable(testing.io);
            defer s.mutex.unlock(testing.io);
            const bytes = switch (response) {
                .completion => |c| c.bytes,
                else => "not a completion",
            };
            const copy = s.arena.allocator().dupe(u8, bytes) catch return;
            _ = run_id;
            s.responses.append(s.arena.allocator(), copy) catch return;
            s.received.set(testing.io);
        }

        fn count(s: *Sent) usize {
            s.mutex.lockUncancelable(testing.io);
            defer s.mutex.unlock(testing.io);
            return s.responses.items.len;
        }

        fn deinit(s: *Sent) void {
            s.arena.deinit();
        }
    };

    /// Responds to `bytes` on a thread of its own, watched, as `process` does.
    const Watched = struct {
        r: *WorkflowRunner,
        bytes: []const u8,
        sender: WorkflowRunner.Sender,
        arena: std.heap.ArenaAllocator = .init(testing.allocator),
        fallback_buf: [WorkflowRunner.fallback_buffer_size]u8 = undefined,
        response: ?WorkflowRunner.Response = null,

        fn run(t: *Watched) void {
            t.response = t.r.respond(t.bytes, t.arena.allocator(), testing.io, &t.fallback_buf, t.sender);
        }

        fn deinit(t: *Watched) void {
            t.arena.deinit();
        }
    };

    /// Each step runs in a coroutine of its own that the workflow awaits, so the workflow
    /// returns to the scheduler between steps.
    fn steps(ctx: *workflow.Context, n: u32, step_ms: u32) ![]const u8 {
        for (0..n) |_| {
            var f = ctx.io().async(step, .{step_ms});
            f.await(ctx.io());
        }
        return "stepped";
    }
    fn step(ms: u32) void {
        testing.io.sleep(.fromMilliseconds(ms), .awake) catch {};
    }
    const Steps = workflow.Def("DeadlockSteps", steps);

    /// Codecs that take `delay` on the real clock for each call.
    const SlowCodec = struct {
        delay: Io.Duration,
        calls: std.atomic.Value(u32) = .init(0),

        fn codec(c: *SlowCodec) converter.PayloadCodec {
            return .{ .ptr = c, .vtable = &.{ .encode = slow, .decode = slow } };
        }

        fn slow(ptr: *anyopaque, arena: Allocator, io: Io, payloads: []const converter.Payload) converter.CodecError![]converter.Payload {
            const c: *SlowCodec = @ptrCast(@alignCast(ptr));
            _ = c.calls.fetchAdd(1, .acq_rel);
            io.sleep(c.delay, .awake) catch return error.Canceled;
            return arena.dupe(converter.Payload, payloads);
        }
    };

    /// Spins on the real clock for `spin_for` the first time any run executes, then returns.
    var spun: std.atomic.Value(bool) = .init(false);
    var spin_for: Io.Duration = .fromMilliseconds(600);

    fn spinOnce(ctx: *workflow.Context) ![]const u8 {
        _ = ctx;
        if (!spun.swap(true, .acq_rel)) {
            const until = Io.Clock.Timestamp.fromNow(testing.io, .{ .raw = spin_for, .clock = .awake });
            while (until.durationFromNow(testing.io).raw.nanoseconds > 0) std.atomic.spinLoopHint();
        }
        return "done";
    }
    const SpinOnce = workflow.Def("DeadlockSpinOnce", spinOnce);

    fn expectedMessage(a: Allocator, workflow_type: []const u8, limit: []const u8) ![]const u8 {
        return WorkflowRunner.deadlockMessage(a, workflow_type, limit);
    }

    /// The failure messages of the run's failed workflow tasks.
    fn taskFailures(a: Allocator, c: *client_mod.Client, id: []const u8, run_id: []const u8) ![]const []const u8 {
        const history = try c.fetchHistory(a, id, run_id);
        var out: std.ArrayList([]const u8) = .empty;
        for (history.events.items) |event| switch (event.attributes orelse continue) {
            .workflow_task_failed_event_attributes => |f| try out.append(a, if (f.failure) |fl| fl.message else ""),
            else => {},
        };
        return out.items;
    }
};

test "workflow code stuck past the deadlock limit fails its task once, and its results are dropped when it returns" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var workflows = try ParallelTest.registry(testing.allocator);
    defer workflows.deinit(testing.allocator);
    var sink: RecordingSink = .{};
    defer sink.deinit();
    var runner = ParallelTest.runner(&workflows, sink.sink());
    runner.deadlock_timeout = .fromMilliseconds(50);
    defer runner.deinit();
    var sent: DeadlockTest.Sent = .{};
    defer sent.deinit();
    ParallelTest.Gate.reset();
    ParallelTest.hook = ParallelTest.Gate.hook;
    defer ParallelTest.hook = ParallelTest.noHook;

    // Run A is held in its workflow code until released.
    var run_a: DeadlockTest.Watched = .{ .r = &runner, .bytes = try ParallelTest.start(a, "A", "ParallelHold", "A"), .sender = sent.sender() };
    defer run_a.deinit();
    const thread = try std.Thread.spawn(.{}, DeadlockTest.Watched.run, .{&run_a});
    var joined = false;
    defer if (!joined) {
        ParallelTest.Gate.release.set(testing.io);
        thread.join();
    };
    try sent.received.wait(testing.io);

    // The watchdog failed the task while the code is still stuck.
    try testing.expect(!ParallelTest.Gate.release.isSet());
    const failed = try ParallelTest.decodeCompletion(a, sent.responses.items[0]);
    try testing.expectEqualStrings("A", failed.run_id);
    const failure = failed.status.?.failed.failure.?;
    try testing.expectEqualStrings(deadlock_failure_type, failure_mod.applicationType(failure).?);
    const message = try DeadlockTest.expectedMessage(a, "ParallelHold", "50ms");
    try testing.expectEqualStrings(message, failure.message);
    try testing.expect(std.mem.indexOf(u8, message, "\"ParallelHold\"") != null);
    try testing.expect(std.mem.indexOf(u8, message, "(50ms)") != null);
    try testing.expectEqual(1, sink.countAt(.err, message, &.{
        .{ .key = "workflow_id", .value = "wf-A" },
        .{ .key = "run_id", .value = "A" },
    }));
    try testing.expectEqual(1, sink.count(message, &.{}));
    // The run left the cache; its instance stays allocated for the stuck code.
    {
        runner.mutex.lockUncancelable(testing.io);
        defer runner.mutex.unlock(testing.io);
        try testing.expect(runner.runs.get("A") == null);
    }

    // Other runs are processed meanwhile; so are the core's eviction of A and A's next task,
    // which replays the run on a fresh instance.
    _ = try ParallelTest.timerSeq(try ParallelTest.decodeCompletion(a, try ParallelTest.handleEncoded(&runner, try ParallelTest.start(a, "B", "ParallelHold", "B"), a)));
    const evicted = try runner.handle(try ParallelTest.evict(a, "A", .LANG_FAIL), a, testing.io);
    try testing.expect(evicted.eviction);
    try testing.expect(evicted.completion.status.? == .successful);
    _ = try ParallelTest.timerSeq(try ParallelTest.decodeCompletion(a, try ParallelTest.handleEncoded(&runner, try ParallelTest.start(a, "A", "ParallelHold", "replayed"), a)));

    // The stuck code returns: its timer command is dropped, and nothing more is sent.
    ParallelTest.Gate.release.set(testing.io);
    thread.join();
    joined = true;
    try testing.expect(run_a.response.? == .discarded);
    try testing.expectEqual(1, sent.count());
    // The replayed copy of A is untouched by the stuck code's teardown.
    _ = try ParallelTest.timerSeq((try runner.handle(try ParallelTest.fireTimer(a, "A", 1), a, testing.io)).completion);
}

test "time with Scheduler.sdk_work set doesn't count toward the deadlock limit, and time in workflow code still does" {
    var workflows = try ParallelTest.registry(testing.allocator);
    defer workflows.deinit(testing.allocator);
    var sink: RecordingSink = .{};
    defer sink.deinit();
    var runner = ParallelTest.runner(&workflows, sink.sink());
    defer runner.deinit();
    const limit: Io.Duration = .fromMilliseconds(40);
    var switches: std.atomic.Value(u64) = .init(0);
    var sdk_work: std.atomic.Value(bool) = .init(false);

    // SDK work for five limits, with no switch: the watch keeps waiting.
    {
        var sent: DeadlockTest.Sent = .{};
        defer sent.deinit();
        var watch: WorkflowRunner.Watch = .{ .r = &runner, .io = testing.io, .limit = limit, .run_id = "run-1", .switches = &switches, .sdk_work = &sdk_work, .sender = sent.sender() };
        sdk_work.store(true, .release);
        const thread = try std.Thread.spawn(.{}, WorkflowRunner.Watch.run, .{&watch});
        try testing.io.sleep(.fromMilliseconds(5 * 40), .awake);
        sdk_work.store(false, .release);
        watch.returned.set(testing.io);
        thread.join();
        try testing.expect(!watch.abandoned);
        try testing.expectEqual(0, sent.count());
    }

    // The same stretch with the flag clear is workflow code that never returned.
    {
        var sent: DeadlockTest.Sent = .{};
        defer sent.deinit();
        var watch: WorkflowRunner.Watch = .{ .r = &runner, .io = testing.io, .limit = limit, .run_id = "run-1", .switches = &switches, .sdk_work = &sdk_work, .sender = sent.sender() };
        const thread = try std.Thread.spawn(.{}, WorkflowRunner.Watch.run, .{&watch});
        try sent.received.wait(testing.io);
        watch.returned.set(testing.io);
        thread.join();
        try testing.expect(watch.abandoned);
        try testing.expectEqual(1, sent.count());
    }
}

test "workflow code that returns to the scheduler between steps runs past the deadlock limit in total" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var workflows: std.StringHashMapUnmanaged(workflow.Registered) = .empty;
    defer workflows.deinit(testing.allocator);
    try workflows.put(testing.allocator, DeadlockTest.Steps.workflow_name, DeadlockTest.Steps.registered);
    var sink: RecordingSink = .{};
    defer sink.deinit();
    var runner = ParallelTest.runner(&workflows, sink.sink());
    runner.deadlock_timeout = .fromMilliseconds(200);
    defer runner.deinit();
    var sent: DeadlockTest.Sent = .{};
    defer sent.deinit();

    // Ten 30 ms steps: 300 ms in all, none near the limit.
    var init: act_pb.InitializeWorkflow = .{ .workflow_type = "DeadlockSteps", .workflow_id = "wf-1", .randomness_seed = 7, .attempt = 1 };
    init.arguments = try converter.toPayloads(a, .{ @as(u32, 10), @as(u32, 30) });
    var act: act_pb.WorkflowActivation = .{ .run_id = "run-1", .timestamp = .{ .seconds = 1_700_000_000 } };
    try act.jobs.append(a, .{ .variant = .{ .initialize_workflow = init } });
    var buf: [WorkflowRunner.fallback_buffer_size]u8 = undefined;
    const response = runner.respond(try ParallelTest.encode(a, act), a, testing.io, &buf, sent.sender());
    const completion = try ParallelTest.decodeCompletion(a, response.completion.bytes);
    const result = completion.status.?.successful.commands.items[0].variant.?.complete_workflow_execution.result.?;
    try testing.expectEqualStrings("\"stepped\"", result.data);
    try testing.expectEqual(0, sent.count());
}

test "time payload codecs spend decoding an activation and encoding its completion doesn't count toward the deadlock limit" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var slow: DeadlockTest.SlowCodec = .{ .delay = .fromMilliseconds(150) };
    const codecs = [_]converter.PayloadCodec{slow.codec()};
    var workflows: std.StringHashMapUnmanaged(workflow.Registered) = .empty;
    defer workflows.deinit(testing.allocator);
    try workflows.put(testing.allocator, CodecContextTest.Echo.workflow_name, CodecContextTest.Echo.registered);
    var sink: RecordingSink = .{};
    defer sink.deinit();
    var runner = ParallelTest.runner(&workflows, sink.sink());
    runner.codecs = &codecs;
    runner.deadlock_timeout = .fromMilliseconds(50);
    defer runner.deinit();
    var sent: DeadlockTest.Sent = .{};
    defer sent.deinit();

    var init: act_pb.InitializeWorkflow = .{ .workflow_type = "Echo", .workflow_id = "wf-1", .randomness_seed = 7, .attempt = 1 };
    init.arguments = try converter.toPayloads(a, .{@as([]const u8, "hi")});
    var act: act_pb.WorkflowActivation = .{ .run_id = "run-1", .timestamp = .{ .seconds = 1_700_000_000 } };
    try act.jobs.append(a, .{ .variant = .{ .initialize_workflow = init } });
    var buf: [WorkflowRunner.fallback_buffer_size]u8 = undefined;
    const response = runner.respond(try ParallelTest.encode(a, act), a, testing.io, &buf, sent.sender());
    const completion = try ParallelTest.decodeCompletion(a, response.completion.bytes);
    const result = completion.status.?.successful.commands.items[0].variant.?.complete_workflow_execution.result.?;
    try testing.expectEqualStrings("\"hi\"", result.data);
    // Decoded and encoded, each call three times the limit.
    try testing.expect(slow.calls.load(.acquire) >= 2);
    try testing.expectEqual(0, sent.count());
}

test "a non-positive deadlock_detection_timeout is rejected; null turns detection off" {
    const all: Worker.Registered = .{ .workflows = true, .activities = true, .nexus = false };
    try testing.expectError(error.InvalidOptions, Worker.checkOptions(.{ .deadlock_detection_timeout = .zero }, "q", all));
    try testing.expectError(error.InvalidOptions, Worker.checkOptions(.{ .deadlock_detection_timeout = .fromMilliseconds(-1) }, "q", all));
    try Worker.checkOptions(.{ .deadlock_detection_timeout = null }, "q", all);
    try Worker.checkOptions(.{ .deadlock_detection_timeout = .fromNanoseconds(1) }, "q", all);
    const defaults: Options = .{};
    try testing.expectEqual(Io.Duration.fromSeconds(2), defaults.deadlock_detection_timeout.?);
}

test "a worker fails the workflow task of code that spins past the deadlock limit, and the workflow completes on a later attempt" {
    const rt = try bridge.Runtime.init(testing.allocator, .{}, null);
    defer rt.deinit();
    var server = try bridge.EphemeralServer.startDevServer(testing.allocator, testing.io, rt, .{}, null);
    defer server.shutdown(testing.io, null) catch {};
    // The core's warnings about the failed task go here, not to std.log.
    var client_sink: RecordingSink = .{};
    defer client_sink.deinit();
    var client = try client_mod.Client.connect(testing.allocator, testing.io, .{ .host_port = server.target, .logger = client_sink.sink() });
    defer client.close();

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    for ([_]?Io.Duration{ .fromMilliseconds(200), null }, [_][]const u8{ "deadlock-on", "deadlock-off" }) |limit, queue| {
        var sink: RecordingSink = .{};
        defer sink.deinit();
        var w = try Worker.init(testing.allocator, testing.io, &client, queue, .{ .deadlock_detection_timeout = limit, .logger = sink.sink() });
        defer w.deinit();
        try w.registerWorkflow(DeadlockTest.SpinOnce);
        DeadlockTest.spun.store(false, .release);
        var running = try testing.io.concurrent(HelloTest.runWorker, .{&w});
        defer {
            w.stop();
            running.await(testing.io) catch {};
        }

        var run = try client.executeWorkflow(DeadlockTest.SpinOnce, .{ .id = queue, .task_queue = queue }, .{});
        defer run.deinit();
        try testing.expectEqualStrings("done", try run.get(a));
        try testing.expect(DeadlockTest.spun.load(.acquire));
        const failures = try DeadlockTest.taskFailures(a, &client, queue, run.run_id);
        if (limit == null) {
            try testing.expectEqual(0, failures.len);
            try testing.expectEqual(0, sink.count(try DeadlockTest.expectedMessage(a, "DeadlockSpinOnce", "200ms"), &.{}));
        } else {
            const message = try DeadlockTest.expectedMessage(a, "DeadlockSpinOnce", "200ms");
            try testing.expectEqual(1, failures.len);
            try testing.expectEqualStrings(message, failures[0]);
            try testing.expectEqual(1, sink.countAt(.err, message, &.{
                .{ .key = "workflow_id", .value = queue },
                .{ .key = "run_id", .value = run.run_id },
            }));
        }
    }
}

// Nondeterminism as a workflow failure: a run whose code changed after its first task fails
// as a workflow with the worker's or the definition's option set, and fails only its task
// without either.
const NondeterminismPolicy = struct {
    /// False: the code waits on a timer; true: it schedules an activity instead, which the
    /// timer in the history doesn't match. Test-only global state.
    var drifted: std.atomic.Value(bool) = .init(false);

    fn drift(ctx: *workflow.Context) ![]const u8 {
        if (!drifted.load(.acquire)) {
            try ctx.io().sleep(.fromSeconds(3600), .awake);
        } else {
            _ = try ctx.executeActivity(HelloTest.Greet, .{ .start_to_close_timeout = .fromSeconds(10) }, .{"drift"}).get();
        }
        return "done";
    }
    const Plain = workflow.Def("NondeterminismPlain", drift);
    const Listed = workflow.DefWithOptions("NondeterminismListed", drift, .{ .nondeterminism_fails_workflow = true });

    /// Waits for the run's history to record a workflow task that failed for nondeterminism.
    fn waitForTaskFailure(c: *client_mod.Client, id: []const u8, run_id: []const u8) !void {
        const deadline = Io.Clock.Timestamp.fromNow(testing.io, .{ .raw = .fromSeconds(30), .clock = .awake });
        while (true) {
            var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
            defer scratch.deinit();
            const history = try c.fetchHistory(scratch.allocator(), id, run_id);
            for (history.events.items) |event| switch (event.attributes orelse continue) {
                .workflow_task_failed_event_attributes => |f| if (f.cause == .WORKFLOW_TASK_FAILED_CAUSE_NON_DETERMINISTIC_ERROR) return,
                .workflow_execution_failed_event_attributes => return error.TestUnexpectedResult,
                else => {},
            };
            if (deadline.durationFromNow(testing.io).raw.nanoseconds <= 0) return error.Timeout;
            try testing.io.sleep(.fromMilliseconds(20), .awake);
        }
    }

    fn expectWorkflowFailed(run: anytype) !void {
        var arena: std.heap.ArenaAllocator = .init(testing.allocator);
        defer arena.deinit();
        try testing.expectError(error.WorkflowFailed, run.get(arena.allocator()));
        const f = run.failure() orelse return error.TestExpectedFailure;
        try testing.expect(std.mem.indexOf(u8, f.message, "ondeterminism") != null);
    }
};

test "nondeterminism fails the workflow with the worker's or the definition's option, and only the task without" {
    const rt = try bridge.Runtime.init(testing.allocator, .{}, null);
    defer rt.deinit();
    var server = try bridge.EphemeralServer.startDevServer(testing.allocator, testing.io, rt, .{}, null);
    defer server.shutdown(testing.io, null) catch {};
    // The core's warnings about the failing task go here, not to std.log.
    var client_sink: RecordingSink = .{};
    defer client_sink.deinit();
    var client = try client_mod.Client.connect(testing.allocator, testing.io, .{ .host_port = server.target, .logger = client_sink.sink() });
    defer client.close();
    var sink: RecordingSink = .{};
    defer sink.deinit();
    NondeterminismPolicy.drifted.store(false, .release);

    // No cache: the signal's task replays each run from its history with the changed code.
    var plain_worker = try Worker.init(testing.allocator, testing.io, &client, "nd-definition", .{ .max_cached_workflows = 0, .logger = sink.sink() });
    defer plain_worker.deinit();
    try plain_worker.registerWorkflow(NondeterminismPolicy.Plain);
    try plain_worker.registerWorkflow(NondeterminismPolicy.Listed);
    {
        const types = try plain_worker.nondeterminismFailTypes();
        defer testing.allocator.free(types);
        try testing.expectEqual(@as(usize, 1), types.len);
        try testing.expectEqualStrings("NondeterminismListed", types[0]);
    }
    var all_worker = try Worker.init(testing.allocator, testing.io, &client, "nd-worker", .{ .max_cached_workflows = 0, .nondeterminism_fails_workflow = true, .logger = sink.sink() });
    defer all_worker.deinit();
    try all_worker.registerWorkflow(NondeterminismPolicy.Plain);
    var running_plain = try testing.io.concurrent(HelloTest.runWorker, .{&plain_worker});
    defer {
        plain_worker.stop();
        running_plain.await(testing.io) catch {};
    }
    var running_all = try testing.io.concurrent(HelloTest.runWorker, .{&all_worker});
    defer {
        all_worker.stop();
        running_all.await(testing.io) catch {};
    }

    var plain = try client.executeWorkflow(NondeterminismPolicy.Plain, .{ .id = "nd-plain", .task_queue = "nd-definition" }, .{});
    defer plain.deinit();
    defer client.terminateWorkflow("nd-plain", plain.run_id, "test done") catch {};
    // A run whose task kept failing would time out instead of failing, ending the test.
    var listed = try client.executeWorkflow(NondeterminismPolicy.Listed, .{ .id = "nd-listed", .task_queue = "nd-definition", .execution_timeout = .fromSeconds(60) }, .{});
    defer listed.deinit();
    var by_worker = try client.executeWorkflow(NondeterminismPolicy.Plain, .{ .id = "nd-by-worker", .task_queue = "nd-worker", .execution_timeout = .fromSeconds(60) }, .{});
    defer by_worker.deinit();
    for ([_]*const client_mod.WorkflowRun([]const u8){ &plain, &listed, &by_worker }) |run| try CacheTest.waitForTimer(&client, run.id, run.run_id);

    NondeterminismPolicy.drifted.store(true, .release);
    for ([_]*const client_mod.WorkflowRun([]const u8){ &plain, &listed, &by_worker }) |run| try client.signalWorkflow(CacheTest.Release, run.id, run.run_id, .{});
    try NondeterminismPolicy.expectWorkflowFailed(&listed);
    try NondeterminismPolicy.expectWorkflowFailed(&by_worker);
    try NondeterminismPolicy.waitForTaskFailure(&client, "nd-plain", plain.run_id);
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    try testing.expect((try client.describeWorkflow(scratch.allocator(), "nd-plain", plain.run_id)).status == .running);
}

test {
    _ = io_tasks;
}

test "io_tasks.resultFits refuses results aligned above 8 and accepts the worker's own" {
    try testing.expect(!io_tasks.resultFits(Io.Duration));
    try testing.expect(!io_tasks.resultFits(failure_mod.Application));
    try testing.expect(!io_tasks.resultFits(@Vector(4, u32)));
    try testing.expect(!io_tasks.resultFits(NexusOutcome));
    // An activity's outcome, as `callActivity` returns it, and a worker run's.
    try testing.expect(io_tasks.resultFits(@typeInfo(@TypeOf(callActivity)).@"fn".return_type.?));
    try testing.expect(io_tasks.resultFits(Worker.RunError!void));
}

/// Hands out every allocation aligned to 8 or less at an address that is 8 mod 16, as a
/// general-purpose allocator may, with canary bytes after the requested length that `free`
/// checks. Thread-safe as its child is.
const OffsetCanaryAllocator = struct {
    child: Allocator,
    overruns: std.atomic.Value(u32) = .init(0),

    const canary_len = 16;
    const canary_byte = 0xa5;

    fn allocator(self: *OffsetCanaryAllocator) Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = Allocator.noResize, .remap = Allocator.noRemap, .free = free } };
    }

    fn lead(alignment: std.mem.Alignment) usize {
        return if (alignment.toByteUnits() <= 8) 8 else 0;
    }

    fn outer(alignment: std.mem.Alignment) std.mem.Alignment {
        return alignment.max(.@"16");
    }

    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *OffsetCanaryAllocator = @ptrCast(@alignCast(ctx));
        const pad = lead(alignment);
        const base = self.child.rawAlloc(pad + len + canary_len, outer(alignment), ret_addr) orelse return null;
        @memset(base[pad + len ..][0..canary_len], canary_byte);
        return base + pad;
    }

    fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *OffsetCanaryAllocator = @ptrCast(@alignCast(ctx));
        const pad = lead(alignment);
        const base = memory.ptr - pad;
        for (base[pad + memory.len ..][0..canary_len]) |b| {
            if (b != canary_byte) {
                _ = self.overruns.fetchAdd(1, .monotonic);
                break;
            }
        }
        self.child.rawFree(base[0 .. pad + memory.len + canary_len], outer(alignment), ret_addr);
    }
};

test "ActivityEnvironment returns 16-aligned activity results without overrunning a Threaded future" {
    const Fixtures = struct {
        fn none(_: *activity.Context) Io.Duration {
            return .fromNanoseconds(-1);
        }
        fn one(_: *activity.Context, x: u64) Io.Duration {
            return .fromNanoseconds(-@as(i96, x));
        }
        const None = activity.Def("AlignedNone", none);
        const One = activity.Def("AlignedOne", one);
    };
    comptime std.debug.assert(@alignOf(Io.Duration) > 8);
    var checking: OffsetCanaryAllocator = .{ .child = testing.allocator };
    var threaded: Io.Threaded = .init(checking.allocator(), .{ .environ = testing.environ });
    defer threaded.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var env: @import("testing.zig").ActivityEnvironment = .init(threaded.io());
    // The two argument tuples differ by 8 bytes, so one of the two futures starts 8 mod 16
    // from where its result would need to be, whatever `@sizeOf` the future has.
    try testing.expectEqual(@as(i96, -1), (try env.executeActivity(Fixtures.None, .{}, arena.allocator())).toNanoseconds());
    try testing.expectEqual(@as(i96, -2), (try env.executeActivity(Fixtures.One, .{2}, arena.allocator())).toNanoseconds());
    try testing.expectEqual(@as(u32, 0), checking.overruns.load(.monotonic));
}
