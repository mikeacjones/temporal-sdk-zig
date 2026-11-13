//! Workflows.
//!
//! A workflow is a function `fn (ctx: *workflow.Context, args...) !Result`, named with `Def`:
//!
//!     fn helloWorld(ctx: *temporal.workflow.Context, name: []const u8) ![]const u8 {
//!         var greeting = ctx.executeActivity(Greet, .{ .start_to_close_timeout = .fromSeconds(10) }, .{name});
//!         return greeting.get();
//!     }
//!     pub const HelloWorld = temporal.workflow.Def("Workflow", helloWorld);
//!
//! A workflow with queries, signals, or updates can instead be a struct type: its fields are
//! the run's state, `pub fn run` is the workflow, and `pub const queries`, `signals`, and
//! `updates` declare which methods handle what. Each run starts from `Approval{}` (so every
//! field needs a default), and the SDK registers the declared handlers before the run's first
//! signal or update is delivered. Handler methods take no context; one that needs it reads a
//! field that `run` sets before it first blocks. `Def` lists the rules:
//!
//!     pub const Approval = struct {
//!         ctx: *temporal.workflow.Context = undefined, // run sets it before any handler runs
//!         approver: ?[]const u8 = null,
//!
//!         pub const queries = .{ .currentApprover = CurrentApprover };
//!         pub const signals = .{ .approve = Approve };
//!
//!         pub fn run(a: *Approval, ctx: *temporal.workflow.Context) ![]const u8 {
//!             a.ctx = ctx;
//!             try ctx.await(a, approved);
//!             return a.approver.?;
//!         }
//!         pub fn currentApprover(a: *Approval) ?[]const u8 {
//!             return a.approver;
//!         }
//!         pub fn approve(a: *Approval, approver: []const u8) void {
//!             a.approver = approver;
//!         }
//!         fn approved(a: *Approval) bool {
//!             return a.approver != null;
//!         }
//!     };
//!     pub const ApprovalWorkflow = temporal.workflow.Def("Approval", Approval);
//!
//! Workflow code must be deterministic: it is replayed from history. Use `ctx.io()` for
//! concurrency, timers (`io.sleep`), time (`io.now`), and randomness; everything on it is
//! recorded or derived from history. Memory from `ctx.allocator()` lives as long as the
//! workflow run stays cached on the worker.
//!
//! A value the workflow can't derive deterministically, such as an ID from an outside
//! generator, is computed once with `ctx.sideEffect`, recorded in history, and read back from
//! history on replay:
//!
//!     fn newOrderId(_: void) u64 {
//!         return order_ids.next();
//!     }
//!     const order_id = try ctx.sideEffect(u64, {}, newOrderId);
//!
//! Concurrency in a workflow uses `std.Io`, which `ctx.io()` implements deterministically:
//!
//! - Coroutines: `io.async(fn, args)`, or `std.Io.Group.async` for a set of them.
//! - Waiting for whichever result comes first: `std.Io.Select`.
//! - Channels between coroutines: `std.Io.Queue`; locks: `std.Io.Mutex`, `std.Io.Semaphore`.
//! - Timers: `io.sleep`, or `io.async(std.Io.sleep, ...)` for a timer to race or cancel.
//! - Waiting for workflow state to change: `ctx.await`, `ctx.awaitTimeout`.
//! - Time: `ctx.now()`.
//!
//! Canceling a future cancels what it is waiting on: a pending `io.sleep` cancels its timer and
//! a pending `ActivityFuture.get` requests the activity's cancellation, a pending
//! `ChildWorkflowFuture.get` or `started` the child workflow's, and a pending
//! `SignalExternalFuture.get` withdraws its signal if the server hasn't recorded it yet.
//!
//! Queries read a workflow's state from outside. Name one with `QueryDef` and answer it with
//! `ctx.setQueryHandler`; the handler runs synchronously and must not change state or block:
//!
//!     pub const Greeting = temporal.workflow.QueryDef("greeting", fn () []const u8);
//!
//!     const State = struct {
//!         greeting: []const u8 = "",
//!         fn current(s: *State) []const u8 {
//!             return s.greeting;
//!         }
//!     };
//!     const state = try ctx.allocator().create(State); // outlives the workflow function
//!     state.* = .{};
//!     try ctx.setQueryHandler(Greeting, state, State.current);
//!
//! A struct workflow declares it instead: `pub const queries = .{ .current = Greeting };`
//! with `pub fn current(self: *W) []const u8`. A query can arrive before `run` starts, so its
//! handler may see `W{}`.
//!
//! Signals change a workflow's state from outside. Name one with `SignalDef` and handle it
//! with `ctx.setSignalHandler`. Each signal runs its handler in a coroutine of its own, so a
//! handler may block (on `ctx.await`, an activity, a timer, a `std.Io.Mutex`). Handlers start
//! in the order their signals arrived, before the coroutines that the same workflow task's
//! other events wake. A signal that arrives before its handler is registered waits for it:
//!
//!     pub const Approve = temporal.workflow.SignalDef("approve", fn (approver: []const u8) void);
//!
//!     const State = struct {
//!         approver: ?[]const u8 = null,
//!         fn approve(s: *State, approver: []const u8) void {
//!             s.approver = approver;
//!         }
//!         fn approved(s: *State) bool {
//!             return s.approver != null;
//!         }
//!     };
//!     const state = try ctx.allocator().create(State);
//!     state.* = .{};
//!     try ctx.setSignalHandler(Approve, state, State.approve);
//!     try ctx.await(state, State.approved);
//!
//! A struct workflow declares it as `pub const signals = .{ .approve = Approve };`. Its
//! handler is registered before the run's first signal arrives, so even a signal sent with the
//! start (signal-with-start) goes straight to it; it starts once `run` is called.
//!
//! A signal whose name is only known at run time, such as one that includes a workflow ID,
//! is handled with `ctx.setSignalHandlerByName`, which takes the argument tuple type in place
//! of a `SignalDef`, and sent with `ctx.signalExternalWorkflowByName`. A handler from
//! `ctx.setDynamicSignalHandler` receives every signal with no handler of its own, with its
//! name and its arguments still encoded (`converter.EncodedValues`):
//!
//!     const name = try std.fmt.allocPrint(ctx.allocator(), "release-{s}", .{ctx.info().workflow_id});
//!     try ctx.setSignalHandlerByName(name, struct { []const u8 }, state, State.approve);
//!     try ctx.setDynamicSignalHandler(state, State.other); // fn (*State, []const u8, EncodedValues) void
//!
//! A workflow signals another with `ctx.signalExternalWorkflow`, by workflow ID (and run ID,
//! empty for the latest run), and its child with `ChildWorkflowFuture.signal`. The future's
//! `get` waits until the server delivered the signal; it returns
//! `error.ExternalWorkflowNotFound` if the target doesn't exist or has closed. A child finds its
//! parent in `ctx.info().parent`:
//!
//!     const parent = ctx.info().parent orelse return error.NoParent;
//!     try ctx.signalExternalWorkflow(Approve, parent.workflow_id, parent.run_id, .{"child"}).get();
//!
//! `ctx.cancelExternalWorkflow` requests another workflow's cancellation the same way:
//!
//!     ctx.cancelExternalWorkflow(other_id, "").get() catch |err| switch (err) {
//!         error.ExternalWorkflowNotFound => {}, // it has already closed
//!         else => return err,
//!     };
//!
//! Updates change a workflow's state and return a result to their caller. Name one with
//! `UpdateDef` and handle it with `ctx.setUpdateHandler`, or with
//! `ctx.setUpdateHandlerWithValidator` to check it first. The validator runs synchronously when
//! the update is delivered, like a query handler: it must only read state, and its error
//! rejects the update, which then leaves no trace in history. An accepted update runs its
//! handler in a coroutine of its own, like a signal: it may block, and its result (or error)
//! goes back to the caller. A handler's error rejects the update the way the workflow
//! function's error fails the workflow: with the failure described by `ctx.fail`, or the
//! failure of the activity or child workflow whose error it returns. A signal handler's error
//! fails the workflow the same way. Each handler keeps these details apart from the workflow
//! function's and other handlers'; a coroutine a handler starts with `io.async` shares the
//! workflow function's. An update that arrives before its handler is registered waits
//! until the workflow's coroutines have all blocked; if no handler was registered by then, it
//! is rejected:
//!
//!     pub const SetLimit = temporal.workflow.UpdateDef("set_limit", fn (limit: u32) u32);
//!
//!     const State = struct {
//!         limit: u32 = 10,
//!         /// Returns the previous limit.
//!         fn set(s: *State, limit: u32) u32 {
//!             defer s.limit = limit;
//!             return s.limit;
//!         }
//!         fn check(_: *State, limit: u32) error{LimitMustBePositive}!void {
//!             if (limit == 0) return error.LimitMustBePositive;
//!         }
//!     };
//!     const state = try ctx.allocator().create(State);
//!     state.* = .{};
//!     try ctx.setUpdateHandlerWithValidator(SetLimit, state, State.set, State.check);
//!
//! A struct workflow declares it as `pub const updates = .{ .set = .{ SetLimit, .check } };`
//! (or `.{ .set = SetLimit }` without a validator). An update sent with the start
//! (update-with-start) reaches its handler rather than being rejected as unregistered. Its
//! validator runs on delivery, before `run` starts, so it may see `W{}`.
//!
//! A handler or validator reads which update it is running for with `ctx.currentUpdate()`.
//! An update's ID is unique within the workflow execution, so it serves as an idempotency key:
//!
//!     const id = ctx.currentUpdate().?.id;
//!
//! The workflow ends when its function returns, even while signal or update handlers are still
//! running; to let them finish first,
//! `try ctx.await(ctx, temporal.workflow.Context.allHandlersFinished)`.
//!
//! A long-running workflow keeps its history small by continuing as new: the workflow
//! function returns `ctx.continueAsNew`, which ends the run and starts a fresh one with the
//! same workflow ID and the arguments given, carrying state forward. The server suggests when
//! (`ctx.continueAsNewSuggested()`); `ctx.historyLength()` and `ctx.historySizeBytes()` show
//! the history's size. A client waiting on the workflow follows it to the last run's result:
//!
//!     fn counter(ctx: *temporal.workflow.Context, start: u64) !u64 {
//!         var count = start;
//!         while (count < 100_000) : (count += 1) {
//!             if (ctx.continueAsNewSuggested()) return ctx.continueAsNew(Counter, .{}, .{count});
//!             try ctx.io().sleep(.fromSeconds(60), .awake);
//!         }
//!         return count;
//!     }
//!     pub const Counter = temporal.workflow.Def("Counter", counter);
//!
//! Changing a workflow's code changes the commands it produces, so runs that started before
//! the change would no longer replay. `ctx.patched` keeps both paths until the old runs are
//! gone (then `ctx.deprecatePatch`, then nothing); `ctx.getVersion` does the same with
//! version numbers:
//!
//!     if (ctx.patched("send-receipt")) _ = try ctx.executeActivity(SendReceipt, opts, .{order}).get();
//!
//! A dynamic workflow runs every workflow type the worker has no `Def` registered for:
//! define it with `DynamicDef`, register it with `Worker.registerDynamicWorkflow`, and read
//! the type from `ctx.info().workflow_type` and the arguments from `converter.EncodedValues`.
//! Clients start it with `executeWorkflowByName`:
//!
//!     fn anyWorkflow(ctx: *temporal.workflow.Context, args: temporal.converter.EncodedValues) ![]const u8 {
//!         const name = try args.get([]const u8, ctx.allocator(), 0);
//!         return std.fmt.allocPrint(ctx.allocator(), "{s} greets {s}", .{ ctx.info().workflow_type, name });
//!     }
//!     pub const AnyWorkflow = temporal.workflow.DynamicDef(anyWorkflow);
//!     // worker: try w.registerDynamicWorkflow(AnyWorkflow);

const std = @import("std");
const converter = @import("converter.zig");
const failure_mod = @import("failure.zig");
const log = @import("log.zig");
const proto = @import("proto.zig");
const propagation = @import("propagation.zig");
const search_attributes = @import("search_attributes.zig");
const metrics_mod = @import("metrics.zig");
const deployment = @import("deployment.zig");
const Instance = @import("workflow/Instance.zig");
const WorkflowIo = @import("workflow/WorkflowIo.zig");
const Allocator = std.mem.Allocator;
const Duration = std.Io.Duration;

/// A run of a workflow: its namespace, workflow ID, and run ID.
pub const WorkflowExecution = struct {
    namespace: []const u8,
    workflow_id: []const u8,
    run_id: []const u8,
};

pub const Info = struct {
    workflow_id: []const u8,
    run_id: []const u8,
    workflow_type: []const u8,
    task_queue: []const u8,
    namespace: []const u8,
    attempt: u32,
    /// The cron schedule the workflow was started with, or empty.
    cron_schedule: []const u8 = "",
    /// The first run in this chain of runs (continue-as-new, cron, retries).
    first_run_id: []const u8 = "",
    /// The run this one continued from, or empty.
    continued_run_id: []const u8 = "",
    execution_timeout: ?Duration = null,
    run_timeout: ?Duration = null,
    task_timeout: ?Duration = null,
    /// The workflow that started this one as a child, or null for a top-level workflow.
    parent: ?WorkflowExecution = null,
    /// The top-level workflow at the root of this one's chain of parents, or null when this
    /// workflow is the root. Its namespace is the parent's: a child runs in its parent's.
    root: ?WorkflowExecution = null,
    /// When this run started, as the server recorded it; the same on every replay. Null when
    /// the server didn't send it.
    start_time: ?std.Io.Timestamp = null,
    /// The retry policy the workflow was started with, or null without one. With `attempt`, it
    /// tells a run whether it is the last attempt. Intervals the server didn't set (or set to
    /// zero) are null. Arena memory of the run.
    retry_policy: ?RetryPolicy = null,
    /// The priority the workflow was started with. All fields are zero when it was started
    /// without one, which means the same as the server's default. `fairness_key` is arena
    /// memory of the run.
    priority: Priority = .{},
    /// The run ID before any reset of this run, or empty when the server didn't send one. A
    /// run that was never reset has its own run ID here. Arena memory of the run.
    original_run_id: []const u8 = "",
};

/// The update a handler or validator is running for (`Context.currentUpdate`). Both strings
/// live as long as the run is cached.
pub const UpdateInfo = struct {
    /// The update's ID: unique within the workflow execution, so it serves as an idempotency
    /// key.
    id: []const u8,
    /// The update's name, which selected the handler.
    name: []const u8,
};

pub const RetryPolicy = struct {
    initial_interval: ?Duration = null,
    backoff_coefficient: f64 = 0,
    maximum_interval: ?Duration = null,
    /// 0 means unlimited.
    maximum_attempts: i32 = 0,
    non_retryable_error_types: []const []const u8 = &.{},
};

/// How the server orders and shares out a workflow's tasks among others on the same task
/// queue. Every zero field inherits its value from the parent or caller, or else uses the
/// server's default.
pub const Priority = struct {
    /// 1 is the highest priority; larger keys run later. 0 uses the default, the middle of
    /// the server's range.
    priority_key: i32 = 0,
    /// Groups tasks that share dispatch capacity fairly with other keys at the same priority,
    /// such as a tenant ID. Empty uses the default key.
    fairness_key: []const u8 = "",
    /// This fairness key's share relative to others. 0 uses the default weight, 1.0.
    fairness_weight: f32 = 0,
};

/// What canceling an activity's future does: `try_cancel` requests the cancellation and
/// reports it at once, `wait_cancellation_completed` requests it and waits for the activity to
/// end, and `abandon` reports it at once without telling the activity.
pub const ActivityCancellationType = enum { try_cancel, wait_cancellation_completed, abandon };

/// How an activity is scheduled. At least one of `start_to_close_timeout` and
/// `schedule_to_close_timeout` is required.
pub const ActivityOptions = struct {
    /// Empty runs the activity on the workflow's task queue.
    task_queue: []const u8 = "",
    /// Empty uses the activity's sequence number, as other SDKs do.
    activity_id: []const u8 = "",
    schedule_to_close_timeout: ?Duration = null,
    schedule_to_start_timeout: ?Duration = null,
    start_to_close_timeout: ?Duration = null,
    heartbeat_timeout: ?Duration = null,
    retry_policy: ?RetryPolicy = null,
    cancellation_type: ActivityCancellationType = .try_cancel,
    disable_eager_execution: bool = false,
    /// Runs the activity in a session (`Context.createSession`): on the session's host, with
    /// `activity.Info.session_id` set. Leave `task_queue` empty. A failed session makes the
    /// future's `get` return `error.SessionFailed`, and a closed one `error.InvalidOptions`
    /// (logged); the activity isn't scheduled then.
    session: ?*const Session = null,
};

/// How `Context.createSession` and `recreateSession` open a session. A session is held by a
/// creation activity that runs on the host for the session's whole life, so its timeouts are
/// that activity's, and it gets one attempt.
pub const SessionOptions = struct {
    /// The task queue whose session workers (`worker.Options.sessions`) may take the session.
    /// Empty uses the workflow's. `recreateSession` ignores it.
    task_queue: []const u8 = "",
    /// How long to wait for a host to take the session: the creation activity's
    /// schedule-to-start timeout. Past it, creating returns `error.SessionCreationFailed`.
    creation_timeout: Duration,
    /// How long the session may stay open: the creation activity's start-to-close timeout.
    /// Past it, the session fails.
    execution_timeout: Duration,
    /// How long the host may go silent before the session fails: the creation activity's
    /// heartbeat timeout. The host heartbeats every third of it.
    heartbeat_timeout: Duration = .fromSeconds(20),
};

/// Where a session is in its life; see `Session.state`.
pub const SessionState = enum { open, failed, closed };

/// A session as `Session.info` describes it. The strings live as long as the run is cached.
pub const SessionInfo = struct {
    /// Unique to the session; activities in it read it from `activity.Info.session_id`.
    session_id: []const u8,
    /// The host's own task queue, which the session's activities run on. Pass it to
    /// `recreateSession` to open another session on the same host, from this run or a later
    /// one.
    task_queue: []const u8,
    /// The host's name, as its operating system reports it.
    host_name: []const u8,
    state: SessionState,
};

/// A group of activities that run on one worker host, for work that needs state local to
/// that host (files on its disk, a process it started). Open one with `Context.createSession`,
/// run activities in it with `ActivityOptions.session`, and end it with
/// `Context.completeSession`. Arena memory of the run; only the workflow reads and changes it.
///
/// A session holds one of its host's `worker.SessionWorkerOptions.max_concurrent_sessions`
/// slots from creation until it is completed or fails. It fails when its host stops, stays
/// silent past `SessionOptions.heartbeat_timeout`, or `execution_timeout` passes. Neither
/// completing nor failing cancels the session's running activities: they finish, or time out,
/// on their own.
///
/// Its state comes only from the run's history, so it is the same on replay.
pub const Session = struct {
    id: []const u8,
    /// Set once the host took the session.
    task_queue: []const u8 = "",
    host_name: []const u8 = "",
    heartbeat_timeout: Duration,
    /// The creation activity; it ends when the session fails or completes.
    creation: *Instance.ActivityState,
    opened: bool = false,
    closed: bool = false,

    /// `closed` after `Context.completeSession`; otherwise `failed` once its creation activity
    /// has ended; otherwise `open`.
    pub fn state(s: *const Session) SessionState {
        if (s.closed) return .closed;
        if (s.creation.outcome != null) return .failed;
        return .open;
    }

    pub fn info(s: *const Session) SessionInfo {
        return .{ .session_id = s.id, .task_queue = s.task_queue, .host_name = s.host_name, .state = s.state() };
    }

    /// Why the session failed (for example `SessionWorkerStopped` when its host stopped, or a
    /// heartbeat timeout), or null while it is open and once it is closed.
    pub fn failure(s: *const Session) ?*const failure_mod.Failure {
        if (s.closed) return null;
        const outcome = s.creation.outcome orelse return null;
        return switch (outcome) {
            .completed => null,
            .failed, .cancelled => |f| f,
        };
    }

    fn settled(s: *const Session) bool {
        return s.opened or s.creation.outcome != null;
    }
};

/// How a local activity is scheduled. A local activity runs on the worker running the
/// workflow, without a task queue round trip; its result is recorded in the workflow's
/// history as a marker. At least one of `start_to_close_timeout` and
/// `schedule_to_close_timeout` is required: without either, the future's `get` returns
/// `error.InvalidOptions`.
pub const LocalActivityOptions = struct {
    /// Empty uses the activity's sequence number, which changes when a retry waits on a
    /// workflow timer.
    activity_id: []const u8 = "",
    /// How long the workflow waits for the activity, retries included. Null means the
    /// workflow's execution timeout, if any.
    schedule_to_close_timeout: ?Duration = null,
    /// How long the activity may wait for a free local activity slot on the worker. Not
    /// retried.
    schedule_to_start_timeout: ?Duration = null,
    /// How long one attempt may run. Retried.
    start_to_close_timeout: ?Duration = null,
    /// Null retries indefinitely with the default backoff.
    retry_policy: ?RetryPolicy = null,
    /// Retries whose backoff is longer than this wait on a durable workflow timer instead of
    /// on the worker, so the workflow task doesn't stay open. Null means one minute.
    local_retry_threshold: ?Duration = null,
    cancellation_type: ActivityCancellationType = .try_cancel,
};

/// What canceling a child workflow's future does: `try_cancel` requests the child's
/// cancellation and reports it at once, `wait_cancellation_completed` requests it and waits for
/// the child to end, and `abandon` reports it at once and leaves the child running.
pub const ChildWorkflowCancellationType = enum { try_cancel, wait_cancellation_completed, abandon };

/// What the server does to a child workflow still running when its parent closes.
pub const ParentClosePolicy = enum { terminate, abandon, request_cancel };

/// How a child workflow is started.
pub const ChildWorkflowOptions = struct {
    /// Empty generates a UUID from the workflow's seeded randomness, the same on replay.
    id: []const u8 = "",
    /// Empty runs the child on the parent's task queue.
    task_queue: []const u8 = "",
    /// Limits the child's whole chain of runs (retries, cron runs, continue-as-new).
    execution_timeout: ?Duration = null,
    /// Limits one run of the child.
    run_timeout: ?Duration = null,
    /// Limits one workflow task of the child.
    task_timeout: ?Duration = null,
    /// Null runs the child once.
    retry_policy: ?RetryPolicy = null,
    /// Runs the child on a cron schedule, such as "0 * * * *": its future resolves only when
    /// the schedule ends (it times out or is canceled or terminated).
    cron_schedule: []const u8 = "",
    parent_close_policy: ParentClosePolicy = .terminate,
    id_reuse_policy: enum { unspecified, allow_duplicate, allow_duplicate_failed_only, reject_duplicate } = .unspecified,
    cancellation_type: ChildWorkflowCancellationType = .try_cancel,
    /// Non-indexed values recorded with the child, built with `converter.toFields`. Only read
    /// while starting.
    memo: []const converter.Field = &.{},
    /// The child's search attributes, built with `search_attributes.Key.set`; unsets are
    /// ignored. A value the server can't index fails the start with `error.InvalidOptions`.
    search_attributes: []const search_attributes.Update = &.{},
};

/// How `Context.continueAsNew` starts the next run.
pub const ContinueAsNewOptions = struct {
    /// Empty continues on this run's task queue.
    task_queue: []const u8 = "",
    /// Limits the new run. Null keeps this run's run timeout.
    run_timeout: ?Duration = null,
    /// Limits one workflow task of the new run. Null keeps this run's task timeout.
    task_timeout: ?Duration = null,
    /// The new run's memo, built with `converter.toFields`. Null (or an empty slice, which the
    /// command can't tell apart from null) keeps this run's memo, `upsertMemo` changes included.
    memo: ?[]const converter.Field = null,
    /// The new run's search attributes, built with `search_attributes.Key.set` (unsets are
    /// ignored). Null keeps this run's current search attributes, upserts included.
    search_attributes: ?[]const search_attributes.Update = null,
    /// Null keeps this run's retry policy.
    retry_policy: ?RetryPolicy = null,
};

pub const Context = struct {
    instance: *Instance,
    /// Set by `fail` in the workflow function (or a coroutine it started) and reported
    /// instead of the returned error's name. Signal and update handlers keep their own.
    pending_failure: ?failure_mod.Application = null,
    /// The last activity or child workflow failure a future returned to the workflow function
    /// (or a coroutine it started); reported as the workflow's failure when the workflow
    /// returns the future's error. Signal and update handlers keep their own.
    last_failure: ?*const failure_mod.Failure = null,

    /// The deterministic Io for this workflow: concurrency, timers, time, randomness.
    pub fn io(ctx: *Context) std.Io {
        return ctx.instance.wio.io();
    }

    /// Memory that lives as long as the workflow run is cached.
    pub fn allocator(ctx: *Context) Allocator {
        return ctx.instance.arena();
    }

    pub fn info(ctx: *const Context) *const Info {
        return &ctx.instance.info;
    }

    /// A logger carrying the workflow's type, ID, and run ID as fields, as the workflow's
    /// outbound interceptors (`interceptor.WorkflowOutbound.logger`) return it. It is silent
    /// while the workflow replays unless `worker.Options.log_during_replay` is set.
    pub fn logger(ctx: *const Context) log.Logger {
        return ctx.instance.currentLogger();
    }

    /// The header the workflow was started with, as sent by its client's interceptors and
    /// propagators. Lives as long as the run is cached.
    pub fn headers(ctx: *const Context) converter.Header {
        return ctx.instance.header;
    }

    /// The values the client's propagators (`client.Options.propagators`) extracted from the
    /// start header. They are injected again into the header of every activity, local
    /// activity, child workflow, external signal, and continue-as-new this workflow sends.
    pub fn propagated(ctx: *const Context) *const propagation.Values {
        return &ctx.instance.propagated;
    }

    /// Records metrics through the client's exporter (`client.Options.metrics`) with
    /// `namespace`, `task_queue`, and `workflow_type` attributes, or records nothing without
    /// one. Nothing is recorded while the workflow replays history, so each value is recorded
    /// once. Valid while the run is cached; recording doesn't affect determinism. See
    /// `temporal.metrics`.
    pub fn metrics(ctx: *const Context) metrics_mod.Handler {
        var h = ctx.instance.metrics;
        h.owned = false;
        return h;
    }

    pub fn isReplaying(ctx: *const Context) bool {
        return ctx.instance.replaying;
    }

    /// Workflow time: the time of the current workflow task, the same on replay. Equal to
    /// `ctx.io().now(...)`.
    pub fn now(ctx: *const Context) std.Io.Timestamp {
        return .{ .nanoseconds = ctx.instance.now_ns };
    }

    /// Blocks until `condition(state)` returns true.
    ///
    /// `state` is usually a pointer to the variables the condition reads; other coroutines,
    /// activity results, and timers change them. The condition is re-checked every time the
    /// workflow's coroutines have all blocked, so it must be cheap and free of side effects.
    pub fn await(ctx: *Context, state: anytype, comptime condition: fn (@TypeOf(state)) bool) error{ Canceled, OutOfMemory }!void {
        _ = try ctx.awaitInner(state, condition, null);
    }

    /// Like `await`, but gives up after `timeout` (a durable timer) and returns false if the
    /// condition still doesn't hold.
    pub fn awaitTimeout(ctx: *Context, timeout: Duration, state: anytype, comptime condition: fn (@TypeOf(state)) bool) error{ Canceled, OutOfMemory }!bool {
        return ctx.awaitInner(state, condition, timeout);
    }

    fn awaitInner(ctx: *Context, state: anytype, comptime condition: fn (@TypeOf(state)) bool, timeout: ?Duration) error{ Canceled, OutOfMemory }!bool {
        const S = @TypeOf(state);
        const Erased = struct {
            fn check(p: *const anyopaque) bool {
                const s: *const S = @ptrCast(@alignCast(p));
                return condition(s.*);
            }
        };
        return ctx.instance.awaitCondition(Erased.check, @ptrCast(&state), timeout);
    }

    /// Fails the workflow with a failure you describe. Return the error this gives back. In
    /// an update handler, returning it rejects the update with that failure instead.
    pub fn fail(ctx: *Context, a: failure_mod.Application) error{ApplicationFailure} {
        ctx.instance.setPendingFailure(a);
        return error.ApplicationFailure;
    }

    /// Ends this run and starts a new run of `Workflow` (a `Def`, usually this workflow's own)
    /// with the same workflow ID and `args`, which are encoded here. Return the error this
    /// gives back from the workflow function:
    ///
    ///     return ctx.continueAsNew(Counter, .{}, .{count + 1});
    ///
    /// Only the workflow function's return continues as new. Calling this again replaces the
    /// earlier request. The error from a signal handler fails the workflow and from an update
    /// handler rejects the update, like any other error.
    ///
    /// `error.InvalidSearchAttribute` means `options.search_attributes` holds a value the
    /// server can't index; the run doesn't continue as new.
    pub fn continueAsNew(ctx: *Context, comptime Workflow: type, options: ContinueAsNewOptions, args: Workflow.Args) error{ ContinueAsNew, OutOfMemory, InvalidSearchAttribute } {
        try ctx.instance.requestContinueAsNew(Workflow.workflow_name, options, args);
        return error.ContinueAsNew;
    }

    /// Whether the server suggests continuing as new because the history is getting large.
    /// Updated with each workflow task, the same way on replay.
    pub fn continueAsNewSuggested(ctx: *const Context) bool {
        return ctx.instance.continue_as_new_suggested;
    }

    /// The number of events in the history so far, as of the current workflow task.
    pub fn historyLength(ctx: *const Context) u32 {
        return ctx.instance.history_length;
    }

    /// The size of the history in bytes, as of the current workflow task's start.
    pub fn historySizeBytes(ctx: *const Context) u64 {
        return ctx.instance.history_size_bytes;
    }

    /// Sets the versioning behavior this run's workflow task completions send from now on,
    /// in place of its `Def`'s (see `VersioningBehavior`). A dynamic workflow, which has no
    /// `Def` options, chooses one per workflow type here:
    ///
    ///     ctx.setVersioningBehavior(if (std.mem.startsWith(u8, ctx.info().workflow_type, "Long")) .pinned else .auto_upgrade);
    ///
    /// Set it before the workflow first blocks, so the run's first workflow task carries it.
    pub fn setVersioningBehavior(ctx: *Context, behavior: VersioningBehavior) void {
        ctx.instance.versioning_behavior = behavior;
    }

    /// The Worker Deployment Version that ran the current workflow task (on replay, the one
    /// history recorded), or null when the worker had neither a deployment nor a build ID;
    /// a worker with only `worker.Options.build_id` has an empty deployment name. Lives as
    /// long as the run is cached.
    pub fn currentDeploymentVersion(ctx: *const Context) ?deployment.Version {
        return ctx.instance.deployment_version;
    }

    /// Whether this run takes the new code path of the change named `patch_id`, so a changed
    /// workflow still replays the histories of runs that started before the change:
    ///
    ///     if (ctx.patched("send-receipt")) {
    ///         _ = try ctx.executeActivity(SendReceipt, opts, .{order}).get();
    ///     }
    ///
    /// True when the code runs for the first time, which records a marker in history; on
    /// replay, true only if the history has that marker, so runs that passed this point
    /// before the change keep their old path. The first call decides for the whole run; later
    /// calls with the same ID return the same answer.
    ///
    /// Once no run that took the old path is left, replace the call with
    /// `deprecatePatch(patch_id)` and remove the old path; once no run that recorded the
    /// marker is left either, remove the `deprecatePatch` call. Removing the `patched` call
    /// directly fails replay of the runs that recorded its marker.
    ///
    /// On a worker that belongs to a Worker Deployment, a pinned workflow
    /// doesn't need patches: each run stays on the deployment version it started on, whose
    /// code doesn't change. An auto-upgrade workflow moves to the new version's code in the
    /// middle of its history, so an incompatible change to it needs a patch.
    ///
    /// Out of memory fails the workflow task, which the server retries.
    pub fn patched(ctx: *Context, patch_id: []const u8) bool {
        return ctx.instance.patch(patch_id, false);
    }

    /// Marks the change named `patch_id` as one whose old code path is gone: the run records
    /// the marker as `patched` does, but code without the call also replays it. Call it where
    /// `patched(patch_id)` was, once no run that took the old path is left; remove it once no
    /// run that recorded the marker is left.
    ///
    ///     ctx.deprecatePatch("send-receipt");
    ///     _ = try ctx.executeActivity(SendReceipt, opts, .{order}).get();
    pub fn deprecatePatch(ctx: *Context, patch_id: []const u8) void {
        _ = ctx.instance.patch(patch_id, true);
    }

    /// The version of the change named `change_id` this run uses, for changes made a version
    /// number at a time: `max_supported` when the code reaches this point for the first time
    /// (recorded in history), the recorded version on replay, and `default_version` when
    /// replaying a run that passed this point before the change existed:
    ///
    ///     const v = try ctx.getVersion("pricing", temporal.workflow.default_version, 2);
    ///     const price = switch (v) {
    ///         temporal.workflow.default_version => oldPrice(order),
    ///         1 => try ctx.executeActivity(PriceV1, opts, .{order}).get(),
    ///         else => try ctx.executeActivity(PriceV2, opts, .{order}).get(),
    ///     };
    ///
    /// Raise `max_supported` for each new version, and raise `min_supported` (dropping the
    /// branches below it) once no run uses the older versions. The first call decides for
    /// the whole run. Each version is recorded as a patch named `"<change_id>@v<version>"`,
    /// so the patch rules above apply: a version stays readable by the code that supports
    /// it, and on versioned workers only auto-upgrade workflows need this.
    ///
    /// `error.UnsupportedVersion`: the run uses a version outside
    /// `min_supported..max_supported`. Return it: the workflow task fails (with a message
    /// naming the change and the versions) and is retried, so a worker whose code supports
    /// the version can run it. Out of memory also fails the task.
    pub fn getVersion(ctx: *Context, change_id: []const u8, min_supported: i32, max_supported: i32) error{UnsupportedVersion}!i32 {
        return ctx.instance.getVersion(change_id, min_supported, max_supported);
    }

    /// Schedules an activity defined with `temporal.activity.Def` and returns a future for its
    /// result. `args` is a tuple of the activity's parameters after its context.
    pub fn executeActivity(ctx: *Context, comptime Activity: type, options: ActivityOptions, args: Activity.Args) ActivityFuture(Activity.Result) {
        return .{ .ctx = ctx, .state = ctx.instance.executeActivity(Activity.activity_name, options, args).state };
    }

    /// Schedules an activity by its type name, for activities this program has no `Def` for
    /// (for example ones implemented in another language). `args` is a tuple of argument
    /// values; `Result` is the type to decode the result as.
    pub fn executeActivityByName(ctx: *Context, comptime Result: type, activity_type: []const u8, options: ActivityOptions, args: anytype) ActivityFuture(Result) {
        return .{ .ctx = ctx, .state = ctx.instance.executeActivity(activity_type, options, args).state };
    }

    /// Schedules a local activity: one the worker running this workflow executes itself, for
    /// short activities where a task queue round trip costs more than the work. The activity
    /// must be registered on this worker. Returns the same future as `executeActivity`.
    /// A retry whose backoff exceeds `local_retry_threshold` waits on a workflow timer;
    /// canceling the future then cancels the retry at once, whatever the cancellation type.
    pub fn executeLocalActivity(ctx: *Context, comptime Activity: type, options: LocalActivityOptions, args: Activity.Args) ActivityFuture(Activity.Result) {
        return .{ .ctx = ctx, .state = ctx.instance.executeLocalActivity(Activity.activity_name, options, args).state };
    }

    /// Schedules a local activity by its type name, decoding its result as `Result`. `args`
    /// is a tuple of argument values.
    pub fn executeLocalActivityByName(ctx: *Context, comptime Result: type, activity_type: []const u8, options: LocalActivityOptions, args: anytype) ActivityFuture(Result) {
        return .{ .ctx = ctx, .state = ctx.instance.executeLocalActivity(activity_type, options, args).state };
    }

    /// `SideEffectFailed`: the worker couldn't record the value (it shut down first, or a
    /// replay found no recorded value). Returned from the workflow, it fails the workflow with
    /// the recording's failure.
    /// `Canceled`: the calling coroutine was canceled before the value was recorded.
    pub const SideEffectError = error{ SideEffectFailed, Canceled, OutOfMemory, InvalidPayload, UnknownEncoding };

    /// Runs `func(context)` once and records its result in history, so a value the workflow
    /// can't derive deterministically stays the same on replay:
    ///
    ///     const id = try ctx.sideEffect([]const u8, generator, Generator.newId);
    ///
    /// The first time, `func` runs in the workflow and a local activity that every worker
    /// runs records its value; this returns the recorded value, decoded as `T` into
    /// `ctx.allocator()`. On replay `func` is not called and the value comes from history.
    /// `func` must be quick, must not block or touch the workflow context, and returns `T`,
    /// not an error union: a call that failed the first time and was skipped on replay would
    /// change the workflow's commands. Return failures as values (an optional or a union).
    ///
    /// For a value that changes over the run, keep it in workflow state and record each new
    /// reading with another `sideEffect`.
    pub fn sideEffect(ctx: *Context, comptime T: type, context: anytype, comptime func: fn (@TypeOf(context)) T) SideEffectError!T {
        const value: ?converter.Payload = if (ctx.instance.replaying) null else try converter.toPayload(ctx.allocator(), func(context));
        const future: ActivityFuture(T) = .{ .ctx = ctx, .state = ctx.instance.sideEffect(value).state };
        return future.get() catch |err| switch (err) {
            error.ActivityFailed => error.SideEffectFailed,
            error.InvalidOptions, error.SessionFailed => {
                ctx.instance.setLastFailure(null);
                return error.SideEffectFailed;
            },
            error.WrongArgumentCount => error.InvalidPayload,
            else => |e| e,
        };
    }

    /// `SessionCreationFailed`: no host took the session within `creation_timeout`, or the host
    /// refused it (`SessionWorkerFull`: it already runs its maximum of sessions). Returned
    /// from the workflow, it fails the workflow with the creation activity's failure.
    /// `Canceled`: the calling coroutine was canceled while waiting for a host.
    /// `InvalidOptions`: a timeout isn't positive, or `recreateSession` got no task queue
    /// (logged).
    pub const SessionError = error{ SessionCreationFailed, Canceled, InvalidOptions, OutOfMemory };

    /// Opens a session (see `Session`) on a worker host polling `options.task_queue` with
    /// sessions enabled (`worker.Options.sessions`), and returns it once a host has taken
    /// it. Its ID is a UUID from the workflow's seeded randomness, the same on replay.
    ///
    ///     const session = try ctx.createSession(.{ .creation_timeout = .fromSeconds(60), .execution_timeout = .fromSeconds(600) });
    ///     defer ctx.completeSession(session) catch {};
    ///     const path = try ctx.executeActivity(Download, .{ .start_to_close_timeout = .fromSeconds(60), .session = session }, .{url}).get();
    ///     try ctx.executeActivity(Process, .{ .start_to_close_timeout = .fromSeconds(60), .session = session }, .{path}).get();
    pub fn createSession(ctx: *Context, options: SessionOptions) SessionError!*Session {
        const task_queue = if (options.task_queue.len > 0) options.task_queue else ctx.info().task_queue;
        const queue = try std.fmt.allocPrint(ctx.allocator(), "{s}" ++ Instance.session_queue_suffix, .{task_queue});
        return ctx.openSession(queue, options, "createSession");
    }

    /// Opens a new session, with a new ID, on the host whose task queue is `task_queue`: the
    /// `task_queue` of an earlier session's `info()`, from this run or an earlier one (passed
    /// to it as an argument). The host must still run, and have a free slot: recreated
    /// sessions count toward its `max_concurrent_sessions`. Otherwise this returns
    /// `error.SessionCreationFailed`, after `creation_timeout` for a host that is gone.
    /// `options.task_queue` is ignored. Errors are those of `createSession`.
    pub fn recreateSession(ctx: *Context, task_queue: []const u8, options: SessionOptions) SessionError!*Session {
        if (task_queue.len == 0) {
            ctx.instance.logger.warn("recreateSession: the task queue is empty; pass the task_queue of an earlier session's info()", .{});
            return error.InvalidOptions;
        }
        return ctx.openSession(try ctx.allocator().dupe(u8, task_queue), options, "recreateSession");
    }

    fn openSession(ctx: *Context, queue: []const u8, options: SessionOptions, comptime what: []const u8) SessionError!*Session {
        inline for (.{ "creation_timeout", "execution_timeout", "heartbeat_timeout" }) |name| {
            if (@field(options, name).nanoseconds <= 0) {
                ctx.instance.logger.warn(what ++ ": SessionOptions." ++ name ++ " must be positive", .{ .TaskQueue = queue });
                return error.InvalidOptions;
            }
        }
        const a = ctx.allocator();
        const input: Instance.SessionCreateInput = .{
            .session_id = try @import("client.zig").uuid(a, ctx.io()),
            .heartbeat_interval_ms = @max(1, @divTrunc(options.heartbeat_timeout.toMilliseconds(), 3)),
        };
        const creation = ctx.executeActivityByName(void, Instance.session_create_activity_type, .{
            .task_queue = queue,
            .schedule_to_start_timeout = options.creation_timeout,
            .start_to_close_timeout = options.execution_timeout,
            .heartbeat_timeout = options.heartbeat_timeout,
            .retry_policy = .{ .maximum_attempts = 1 },
            .disable_eager_execution = true,
        }, .{input});
        const state = creation.state catch |err| return switch (err) {
            error.OutOfMemory => error.OutOfMemory,
            error.InvalidOptions, error.SessionFailed => error.InvalidOptions,
        };
        const session = try a.create(Session);
        session.* = .{ .id = input.session_id, .heartbeat_timeout = options.heartbeat_timeout, .creation = state };
        try ctx.instance.sessions.append(a, session);
        ctx.await(@as(*const Session, session), Session.settled) catch |err| {
            if (err == error.Canceled) ctx.instance.cancelActivity(state);
            session.closed = true;
            return err;
        };
        if (session.opened) return session;
        session.closed = true;
        ctx.instance.setLastFailure(creation.failure());
        return error.SessionCreationFailed;
    }

    /// Ends `session`: its creation activity is canceled, and an activity on its host frees
    /// its slot at once. Waits for that activity, even if the calling coroutine is canceled
    /// meanwhile; if it fails, the slot is freed once the host sees the cancel, and the
    /// failure is logged. The session's running activities aren't canceled. Afterwards the
    /// session is `closed`. A closed session is left as it is, and a failed one just closes.
    pub fn completeSession(ctx: *Context, session: *Session) error{OutOfMemory}!void {
        if (session.closed) return;
        const was_open = session.state() == .open;
        session.closed = true;
        if (!was_open) return;
        const inst = ctx.instance;
        inst.cancelActivity(session.creation);
        const wio = ctx.io();
        const protection = wio.swapCancelProtection(.blocked);
        defer _ = wio.swapCancelProtection(protection);
        const done = ctx.executeActivityByName(void, Instance.session_complete_activity_type, .{
            .task_queue = session.task_queue,
            .schedule_to_start_timeout = session.heartbeat_timeout,
            .start_to_close_timeout = session.heartbeat_timeout,
            .retry_policy = .{ .maximum_attempts = 1 },
            .disable_eager_execution = true,
        }, .{session.id});
        const state = done.state catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            else => {
                inst.logger.warn("completeSession: scheduling the activity that frees the session's slot failed; the host frees it once it sees the session canceled", .{ .SessionID = session.id, .error_name = err });
                return;
            },
        };
        inst.awaitActivity(state) catch return; // only eviction ends this wait early
        if (done.failure()) |f| inst.logger.warn("completeSession: the activity that frees the session's slot failed; the host frees it once it sees the session canceled", .{ .SessionID = session.id, .TaskQueue = session.task_queue, .Error = f.message });
    }

    /// Starts a child workflow defined with `Def` and returns a future for its start and its
    /// result. `args` is a tuple of the child's parameters after its context. The child runs
    /// on whichever worker polls its task queue, this one included.
    pub fn executeChildWorkflow(ctx: *Context, comptime Workflow: type, options: ChildWorkflowOptions, args: Workflow.Args) ChildWorkflowFuture(Workflow.Result) {
        return .{ .ctx = ctx, .state = ctx.instance.executeChildWorkflow(Workflow.workflow_name, options, args).state };
    }

    /// Starts a child workflow by its type name, decoding its result as `Result`. `args` is a
    /// tuple of argument values.
    pub fn executeChildWorkflowByName(ctx: *Context, comptime Result: type, workflow_type: []const u8, options: ChildWorkflowOptions, args: anytype) ChildWorkflowFuture(Result) {
        return .{ .ctx = ctx, .state = ctx.instance.executeChildWorkflow(workflow_type, options, args).state };
    }

    /// Sends `Signal` (a `SignalDef`) with `args` to the workflow `workflow_id` and returns a
    /// future for its delivery. An empty `run_id` targets the workflow's latest run. The
    /// target runs in this workflow's namespace. `args` are encoded here; the IDs are copied.
    pub fn signalExternalWorkflow(ctx: *Context, comptime Signal: type, workflow_id: []const u8, run_id: []const u8, args: Signal.Args) SignalExternalFuture {
        return ctx.signalExternalWorkflowByName(Signal.signal_name, workflow_id, run_id, args);
    }

    /// Sends the signal called `signal_name`, named at run time, with `args` (a tuple of
    /// argument values) to the workflow `workflow_id`, like `signalExternalWorkflow`. The
    /// name and IDs are copied; `args` are encoded here.
    pub fn signalExternalWorkflowByName(ctx: *Context, signal_name: []const u8, workflow_id: []const u8, run_id: []const u8, args: anytype) SignalExternalFuture {
        return ctx.instance.signalExternal(.{ .execution = .{ .workflow_id = workflow_id, .run_id = run_id } }, signal_name, args);
    }

    /// Requests cancellation of the workflow `workflow_id` and returns a future for the
    /// request's delivery. An empty `run_id` targets the workflow's latest run. The target runs
    /// in this workflow's namespace and decides what canceling means: its code sees the
    /// cancellation and may clean up, or ignore it. To cancel a child, cancel its future
    /// instead. The IDs are copied.
    pub fn cancelExternalWorkflow(ctx: *Context, workflow_id: []const u8, run_id: []const u8) CancelExternalFuture {
        return ctx.instance.cancelExternalWorkflow(workflow_id, run_id);
    }

    /// The workflow's memo: the memo it started with, with this run's `upsertMemo` calls
    /// applied, sorted by name. The slice lives in the run's arena; a later upsert leaves a
    /// slice returned earlier unchanged.
    pub fn memo(ctx: *const Context) []const converter.Field {
        return ctx.instance.memo;
    }

    /// The memo value called `name` decoded as `T` into `ctx.allocator()`, or null if the
    /// memo has no such key.
    pub fn memoValue(ctx: *Context, comptime T: type, name: []const u8) error{ OutOfMemory, InvalidPayload, UnknownEncoding }!?T {
        return converter.fieldValue(T, ctx.allocator(), ctx.instance.memo, name) catch |err| switch (err) {
            error.WrongArgumentCount => error.InvalidPayload,
            else => |e| e,
        };
    }

    /// Adds or replaces memo keys: `values` is a struct literal, one field per key, encoded
    /// like `converter.toFields`. A null value removes the key, and so does an empty slice,
    /// which the server treats the same way. Records the change in history and updates
    /// `memo()` right away, the same way on replay.
    ///
    ///     try ctx.upsertMemo(.{ .status = "shipped", .reviewer = @as(?[]const u8, null) });
    pub fn upsertMemo(ctx: *Context, values: anytype) error{OutOfMemory}!void {
        return ctx.instance.upsertMemo(try converter.toFields(ctx.allocator(), values));
    }

    /// The workflow's search attributes: those it started with, with this run's
    /// `upsertSearchAttributes` calls applied, sorted by name. Read a value with
    /// `get(ctx.allocator(), key)`. The view lives in the run's arena; a later upsert leaves a
    /// view returned earlier unchanged.
    pub fn searchAttributes(ctx: *const Context) search_attributes.View {
        return ctx.instance.search_attributes;
    }

    /// Sets and removes search attributes, in order: `updates` come from
    /// `search_attributes.Key.set` and `unset`. Records the change in history and updates
    /// `searchAttributes()` right away, the same way on replay. The attributes must exist on
    /// the server; an unknown one fails the workflow task, which retries until it is
    /// registered. `error.InvalidSearchAttribute` (logged) means a value the server can't
    /// index, such as a NaN Double; nothing is recorded then.
    ///
    ///     try ctx.upsertSearchAttributes(&.{ status.set("shipped"), reviewer.unset() });
    pub fn upsertSearchAttributes(ctx: *Context, updates: []const search_attributes.Update) search_attributes.EncodeError!void {
        return ctx.instance.upsertSearchAttributes(updates);
    }

    pub const SetQueryHandlerError = error{
        OutOfMemory,
        /// `context` points into the calling coroutine's stack (logged with the query type).
        QueryContextOnStack,
    };

    /// Answers queries of type `Query` (a `QueryDef`) with `handler(context, args...)`, which
    /// returns `Query.Result` or an error union of it; an error fails the query with the
    /// error's name. Registering the same query type again replaces its handler.
    ///
    /// Handlers run synchronously on the worker thread, outside any coroutine, possibly after
    /// the workflow function has returned. They must only read workflow state: not change it,
    /// not block, and not call into the workflow context. Arguments and the result use
    /// memory that lives until the answer is sent.
    ///
    /// `context` is usually a pointer to the state the handler reads. It must outlive the run,
    /// so allocate it with `ctx.allocator()` (or point to a global); a pointer into the calling
    /// coroutine's stack returns `error.QueryContextOnStack`.
    ///
    ///     const state = try ctx.allocator().create(State);
    ///     try ctx.setQueryHandler(GetStatus, state, State.status);
    pub fn setQueryHandler(ctx: *Context, comptime Query: type, context: anytype, comptime handler: anytype) SetQueryHandlerError!void {
        const C = @TypeOf(context);
        comptime checkHandler("query handler for \"" ++ Query.query_name ++ "\"", Query.Args, C, @TypeOf(handler), Query.Result);
        const Erased = struct {
            fn call(p: *const anyopaque, out: Allocator, payloads: []const converter.Payload) anyerror!converter.Payload {
                const c: *const C = @ptrCast(@alignCast(p));
                const args = try converter.fromPayloads(Query.Args, out, payloads);
                const result = @call(.auto, handler, .{c.*} ++ args);
                const value = if (@typeInfo(@TypeOf(result)) == .error_union) try result else result;
                return converter.toPayload(out, value);
            }
        };
        const stored = try ctx.allocator().create(C);
        stored.* = context;
        return ctx.instance.setQueryHandler(Query.query_name, .{ .context = @ptrCast(stored), .call = Erased.call }, contextAddress(context));
    }

    pub const SetSignalHandlerError = error{
        OutOfMemory,
        /// `context` points into the calling coroutine's stack (logged with the signal name).
        SignalContextOnStack,
    };

    /// Handles signals named by `Signal` (a `SignalDef`) with `handler(context, args...)`,
    /// which returns `void` or `!void`. Registering the same signal again replaces its
    /// handler for the signals that arrive afterwards.
    ///
    /// Each signal runs the handler in a new workflow coroutine, so the handler may block and
    /// may use the workflow context. Handlers start in the order their signals arrived.
    /// Signals that arrived before this call are buffered, and each starts its handler
    /// coroutine here, in arrival order; they run once the caller blocks. A signal whose
    /// arguments don't decode as the handler's parameters is dropped with a warning.
    ///
    /// An error from the handler fails the workflow as if the workflow function had returned
    /// it (`error.Canceled` cancels it), unless the workflow has already finished.
    ///
    /// `context` is usually a pointer to the state the handler changes. It must outlive the
    /// run, so allocate it with `ctx.allocator()` (or point to a global); a pointer into the
    /// calling coroutine's stack returns `error.SignalContextOnStack`. Decoded arguments live
    /// in `ctx.allocator()`, so the handler may keep them.
    ///
    ///     const state = try ctx.allocator().create(State);
    ///     try ctx.setSignalHandler(Approve, state, State.approve);
    pub fn setSignalHandler(ctx: *Context, comptime Signal: type, context: anytype, comptime handler: anytype) SetSignalHandlerError!void {
        comptime checkHandler("signal handler for \"" ++ Signal.signal_name ++ "\"", Signal.Args, @TypeOf(context), @TypeOf(handler), void);
        return ctx.setSignalHandlerInner(Signal.signal_name, Signal.Args, context, handler);
    }

    /// `setSignalHandler` for a signal named at run time, such as one whose name includes a
    /// workflow ID: handles signals called `name`, whose arguments are the tuple type `Args`,
    /// with `handler(context, args...)`. `name` is copied into `ctx.allocator()`. Buffering,
    /// replacement, and errors work as in `setSignalHandler`.
    ///
    ///     const name = try std.fmt.allocPrint(ctx.allocator(), "unlock-{s}", .{holder_id});
    ///     try ctx.setSignalHandlerByName(name, struct { []const u8 }, state, State.unlock);
    pub fn setSignalHandlerByName(ctx: *Context, name: []const u8, comptime Args: type, context: anytype, comptime handler: anytype) SetSignalHandlerError!void {
        comptime checkHandler("signal handler for a signal named at run time", Args, @TypeOf(context), @TypeOf(handler), void);
        return ctx.setSignalHandlerInner(try ctx.allocator().dupe(u8, name), Args, context, handler);
    }

    fn setSignalHandlerInner(ctx: *Context, name: []const u8, comptime Args: type, context: anytype, comptime handler: anytype) SetSignalHandlerError!void {
        const C = @TypeOf(context);
        const Erased = struct {
            fn decode(out: Allocator, _: []const u8, payloads: []const converter.Payload) anyerror!*const anyopaque {
                const args = try out.create(Args);
                args.* = try converter.fromPayloads(Args, out, payloads);
                return @ptrCast(args);
            }
            fn call(p: *const anyopaque, a: *const anyopaque) anyerror!void {
                const c: *const C = @ptrCast(@alignCast(p));
                const args: *const Args = @ptrCast(@alignCast(a));
                const result = @call(.auto, handler, .{c.*} ++ args.*);
                if (@typeInfo(@TypeOf(result)) == .error_union) try result;
            }
        };
        const stored = try ctx.allocator().create(C);
        stored.* = context;
        return ctx.instance.setSignalHandler(name, .{ .context = @ptrCast(stored), .decode = Erased.decode, .call = Erased.call }, contextAddress(context));
    }

    /// Handles every signal that has no handler registered for its name with
    /// `handler(context, name, args)`, where `name` is the signal's name and `args` its
    /// undecoded arguments (`converter.EncodedValues`); it returns `void` or `!void`. Both
    /// live in `ctx.allocator()`, so the handler may keep them. Registering again replaces
    /// the dynamic handler for the signals that arrive afterwards.
    ///
    /// Signals buffered before this call, of any name, start the handler here in arrival
    /// order. A handler registered for a signal's name later still takes that name's later
    /// signals. Coroutines, errors, and `context` work as in `setSignalHandler`; an argument
    /// that doesn't decode is the handler's error to handle or return.
    ///
    ///     fn onSignal(s: *State, name: []const u8, args: temporal.converter.EncodedValues) !void {
    ///         const amount = try args.get(i64, s.ctx.allocator(), 0);
    ///         try s.record(name, amount);
    ///     }
    ///     try ctx.setDynamicSignalHandler(state, State.onSignal);
    pub fn setDynamicSignalHandler(ctx: *Context, context: anytype, comptime handler: anytype) SetSignalHandlerError!void {
        const C = @TypeOf(context);
        const Args = struct { []const u8, converter.EncodedValues };
        comptime checkHandler("dynamic signal handler", Args, C, @TypeOf(handler), void);
        const Erased = struct {
            fn decode(out: Allocator, name: []const u8, payloads: []const converter.Payload) anyerror!*const anyopaque {
                const args = try out.create(Args);
                args.* = .{ name, .{ .payloads = payloads } };
                return @ptrCast(args);
            }
            fn call(p: *const anyopaque, a: *const anyopaque) anyerror!void {
                const c: *const C = @ptrCast(@alignCast(p));
                const args: *const Args = @ptrCast(@alignCast(a));
                const result = @call(.auto, handler, .{c.*} ++ args.*);
                if (@typeInfo(@TypeOf(result)) == .error_union) try result;
            }
        };
        const stored = try ctx.allocator().create(C);
        stored.* = context;
        return ctx.instance.setDynamicSignalHandler(.{ .context = @ptrCast(stored), .decode = Erased.decode, .call = Erased.call }, contextAddress(context));
    }

    pub const SetUpdateHandlerError = error{
        OutOfMemory,
        /// `context` points into the calling coroutine's stack (logged with the update name).
        UpdateContextOnStack,
    };

    /// Handles updates named by `Update` (an `UpdateDef`) with `handler(context, args...)`,
    /// which returns `Update.Result` or an error union of it. Registering the same update
    /// again replaces its handler (and validator) for the updates delivered afterwards.
    ///
    /// Each accepted update runs the handler in a new workflow coroutine, so the handler may
    /// block and may use the workflow context. Its result completes the update; its error
    /// rejects the update with the error's name, without failing the workflow. Updates of
    /// this name that arrived in the current workflow task, before this call, are validated
    /// and accepted here in arrival order; their handlers run once the caller blocks. Updates
    /// with no handler once every coroutine has blocked are rejected, so register handlers
    /// before the workflow function first blocks. An update whose arguments don't decode as
    /// the handler's parameters is rejected.
    ///
    /// `context` must outlive the run, as for `setSignalHandler`; a pointer into the calling
    /// coroutine's stack returns `error.UpdateContextOnStack`. Decoded arguments live in
    /// `ctx.allocator()`.
    pub fn setUpdateHandler(ctx: *Context, comptime Update: type, context: anytype, comptime handler: anytype) SetUpdateHandlerError!void {
        return ctx.setUpdateHandlerInner(Update, context, handler, null);
    }

    /// `setUpdateHandler` with a validator: `validator(context, args...)`, returning `void`
    /// or `!void`, runs when the update is delivered, before it is accepted. Its error
    /// rejects the update with the error's name; the update then never runs and leaves no
    /// trace in history.
    ///
    /// The validator runs synchronously, outside the update's coroutine, like a query
    /// handler: it must only read workflow state, not change it, block, or call into the
    /// workflow context. It doesn't run on replay, where every update in history was
    /// accepted.
    ///
    ///     try ctx.setUpdateHandlerWithValidator(SetLimit, state, State.set, State.check);
    pub fn setUpdateHandlerWithValidator(ctx: *Context, comptime Update: type, context: anytype, comptime handler: anytype, comptime validator: anytype) SetUpdateHandlerError!void {
        return ctx.setUpdateHandlerInner(Update, context, handler, validator);
    }

    fn setUpdateHandlerInner(ctx: *Context, comptime Update: type, context: anytype, comptime handler: anytype, comptime validator: anytype) SetUpdateHandlerError!void {
        const C = @TypeOf(context);
        const what = "update handler for \"" ++ Update.update_name ++ "\"";
        comptime checkHandler(what, Update.Args, C, @TypeOf(handler), Update.Result);
        const has_validator = @TypeOf(validator) != @TypeOf(null);
        if (has_validator) comptime checkHandler("update validator for \"" ++ Update.update_name ++ "\"", Update.Args, C, @TypeOf(validator), void);
        const Erased = struct {
            fn decode(out: Allocator, payloads: []const converter.Payload) anyerror!*const anyopaque {
                const args = try out.create(Update.Args);
                args.* = try converter.fromPayloads(Update.Args, out, payloads);
                return @ptrCast(args);
            }
            fn validate(p: *const anyopaque, a: *const anyopaque) anyerror!void {
                const c: *const C = @ptrCast(@alignCast(p));
                const args: *const Update.Args = @ptrCast(@alignCast(a));
                const result = @call(.auto, validator, .{c.*} ++ args.*);
                if (@typeInfo(@TypeOf(result)) == .error_union) try result;
            }
            fn call(p: *const anyopaque, out: Allocator, a: *const anyopaque) anyerror!converter.Payload {
                const c: *const C = @ptrCast(@alignCast(p));
                const args: *const Update.Args = @ptrCast(@alignCast(a));
                const result = @call(.auto, handler, .{c.*} ++ args.*);
                const value = if (@typeInfo(@TypeOf(result)) == .error_union) try result else result;
                return converter.toPayload(out, value);
            }
        };
        const stored = try ctx.allocator().create(C);
        stored.* = context;
        return ctx.instance.setUpdateHandler(Update.update_name, .{
            .context = @ptrCast(stored),
            .decode = Erased.decode,
            .validate = if (has_validator) Erased.validate else null,
            .call = Erased.call,
        }, contextAddress(context));
    }

    /// Whether no signal or update handler coroutine is running: every delivered signal's
    /// and accepted update's handler has returned. Wait for it before returning so handlers
    /// aren't cut short:
    ///
    ///     try ctx.await(ctx, temporal.workflow.Context.allHandlersFinished);
    pub fn allHandlersFinished(ctx: *Context) bool {
        return ctx.instance.handlers.items.len == 0;
    }

    /// The update being handled: its ID and name, read from the update's handler or its
    /// validator. Null in the workflow function, in signal and query handlers, and outside any
    /// coroutine. It is also null in a coroutine an update handler starts with `io.async` or a
    /// `Group`: read it in the handler and pass it down. On replay the validator doesn't run,
    /// and the handler sees the same ID and name. The strings live as long as the run is cached.
    pub fn currentUpdate(ctx: *const Context) ?UpdateInfo {
        return ctx.instance.currentUpdate();
    }

    /// Whether the previous run of this cron (or retried) workflow completed with a result.
    /// When the previous run failed instead, `previousRunFailure` returns its failure.
    pub fn hasLastCompletionResult(ctx: *const Context) bool {
        return ctx.instance.last_completion_result != null;
    }

    /// The result of the previous run of this cron (or retried) workflow, decoded as `T`.
    ///
    pub fn lastCompletionResult(ctx: *Context, comptime T: type) error{ NoLastCompletionResult, OutOfMemory, InvalidPayload, UnknownEncoding }!T {
        const p = ctx.instance.last_completion_result orelse return error.NoLastCompletionResult;
        return converter.fromPayload(T, ctx.allocator(), p) catch |err| switch (err) {
            error.WrongArgumentCount => error.InvalidPayload,
            else => |e| e,
        };
    }

    /// The failure the previous run of this retried (or cron) workflow ended with, or null when
    /// there was no previous run or it didn't fail. It comes from the run's start, so it is the
    /// same on every replay, and reading it emits no command. Lives as long as the run is
    /// cached. `temporal.failure.applicationType` reads an application failure's type.
    pub fn previousRunFailure(ctx: *const Context) ?*const failure_mod.Failure {
        return ctx.instance.previous_run_failure;
    }
};

/// `InvalidOptions`: the options were rejected before scheduling (the worker logs why).
/// `SessionFailed`: the activity's session (`ActivityOptions.session`) had failed, so it
/// wasn't scheduled; read why with the session's `failure`.
pub const ActivityError = error{ ActivityFailed, Canceled, OutOfMemory, InvalidOptions, InvalidPayload, UnknownEncoding, WrongArgumentCount, SessionFailed };

/// The pending result of an activity. `ActivityFuture(converter.Payload)` returns the result
/// still encoded, as workflow interceptors (`interceptor.WorkflowOutbound`) handle it.
pub fn ActivityFuture(comptime T: type) type {
    return struct {
        const Self = @This();

        ctx: *Context,
        /// The scheduling error, if scheduling failed; `get` then returns it.
        state: Instance.ActivityScheduleError!*Instance.ActivityState,

        /// Waits for the activity and returns its result. On `error.ActivityFailed`, read the
        /// failure with `failure`. If the calling coroutine is canceled while waiting (the
        /// workflow is canceled, or an enclosing future, `Select`, or `Group` is), the
        /// activity's cancellation is requested and this returns `error.Canceled`: at once
        /// for `try_cancel` and `abandon`, and once the activity has ended for
        /// `wait_cancellation_completed`. If that activity completes or fails instead of
        /// acknowledging the cancel, this returns its result or `error.ActivityFailed`, and
        /// the next cancelable wait returns `error.Canceled`.
        pub fn get(f: Self) ActivityError!T {
            const state = try f.state;
            try f.ctx.instance.awaitActivity(state);
            switch (state.outcome.?) {
                .completed => |maybe| {
                    if (T == void) return;
                    const p = maybe orelse return error.InvalidPayload;
                    return converter.fromPayload(T, f.ctx.allocator(), p);
                },
                .failed, .cancelled => |fl| {
                    f.ctx.instance.setLastFailure(fl);
                    return error.ActivityFailed;
                },
            }
        }

        /// The activity's failure, after `get` returned `error.ActivityFailed`.
        pub fn failure(f: Self) ?*const failure_mod.Failure {
            const state = f.state catch return null;
            const outcome = state.outcome orelse return null;
            return switch (outcome) {
                .completed => null,
                .failed, .cancelled => |fl| fl,
            };
        }

        /// Whether the activity has resolved, so `get` won't block.
        pub fn isReady(f: Self) bool {
            const state = f.state catch return true;
            return state.outcome != null;
        }

        /// Requests cancellation of the activity.
        pub fn cancel(f: Self) void {
            if (f.state) |s| f.ctx.instance.cancelActivity(s) else |_| {}
        }
    };
}

/// `ChildWorkflowFailed`: read the failure with the future's `failure`.
/// `ChildWorkflowAlreadyStarted`: a workflow with the child's ID is already running (or, per
/// `id_reuse_policy`, has run); `failure` describes it too.
/// `InvalidOptions`: the options were rejected before starting (the worker logs why).
pub const ChildWorkflowError = error{ ChildWorkflowFailed, ChildWorkflowAlreadyStarted, Canceled, OutOfMemory, InvalidOptions, InvalidPayload, UnknownEncoding, WrongArgumentCount };

/// The pending start and result of a child workflow. `ChildWorkflowFuture(converter.Payload)`
/// returns the result still encoded, as workflow interceptors handle it.
pub fn ChildWorkflowFuture(comptime T: type) type {
    return struct {
        const Self = @This();

        ctx: *Context,
        /// The validation error, if the start was rejected; `started` and `get` return it.
        state: Instance.ScheduleError!*Instance.ChildState,

        /// Waits until the server has started the child and returns its execution (arena
        /// memory of the parent run; the child runs in the parent's namespace). Returns
        /// `error.ChildWorkflowAlreadyStarted` if its ID is taken, and `error.ChildWorkflowFailed`
        /// if it could not start for another reason or was canceled before it started.
        /// Canceling the calling coroutine cancels the child as `get` does.
        pub fn started(f: Self) ChildWorkflowError!WorkflowExecution {
            const state = try f.state;
            try f.ctx.instance.awaitChild(state, .started);
            if (state.run_id) |run_id| return .{ .namespace = f.ctx.instance.namespace, .workflow_id = state.workflow_id, .run_id = run_id };
            return f.fail(state);
        }

        /// Waits for the child to close and returns its result. On `error.ChildWorkflowFailed`
        /// (the child failed, timed out, was terminated or canceled, or never started), read
        /// the failure with `failure`. If the calling coroutine is canceled while waiting (the
        /// workflow is canceled, or an enclosing future, `Select`, or `Group` is), the child's
        /// cancellation is requested and this returns `error.Canceled`: at once for
        /// `try_cancel` and `abandon`, and once the child has closed for
        /// `wait_cancellation_completed`. If that child completes or fails instead, this
        /// returns its result or error, and the next cancelable wait returns `error.Canceled`.
        pub fn get(f: Self) ChildWorkflowError!T {
            const state = try f.state;
            try f.ctx.instance.awaitChild(state, .result);
            switch (state.outcome.?) {
                .completed => |maybe| {
                    if (T == void) return;
                    const p = maybe orelse return error.InvalidPayload;
                    return converter.fromPayload(T, f.ctx.allocator(), p);
                },
                .failed, .cancelled => return f.fail(state),
            }
        }

        fn fail(f: Self, state: *const Instance.ChildState) ChildWorkflowError {
            f.ctx.instance.setLastFailure(f.failure());
            return if (state.already_started) error.ChildWorkflowAlreadyStarted else error.ChildWorkflowFailed;
        }

        /// The child's failure, after `get` or `started` returned `error.ChildWorkflowFailed` or
        /// `error.ChildWorkflowAlreadyStarted`.
        pub fn failure(f: Self) ?*const failure_mod.Failure {
            const state = f.state catch return null;
            const outcome = state.outcome orelse return null;
            return switch (outcome) {
                .completed => null,
                .failed, .cancelled => |fl| fl,
            };
        }

        /// Whether the child has closed (or failed to start), so `get` won't block.
        pub fn isReady(f: Self) bool {
            const state = f.state catch return true;
            return state.outcome != null;
        }

        /// Requests cancellation of the child, as its `cancellation_type` says.
        pub fn cancel(f: Self) void {
            if (f.state) |s| f.ctx.instance.cancelChild(s) else |_| {}
        }

        /// Sends `Signal` (a `SignalDef`) with `args` to the child and returns a future for its
        /// delivery. Waits for the child to start first (see `started`, whose errors this
        /// returns); the signal then goes to the child's run, and only to a child of this
        /// workflow.
        pub fn signal(f: Self, comptime Signal: type, args: Signal.Args) ChildWorkflowError!SignalExternalFuture {
            const exec = try f.started();
            return f.ctx.instance.signalExternal(.{ .child = exec.workflow_id }, Signal.signal_name, args);
        }
    };
}

/// `ExternalWorkflowNotFound`: the target workflow (or its namespace) doesn't exist, or it has
/// closed. `ExternalWorkflowRequestFailed`: the server refused for another reason, such as the
/// target's signal limit. Read the failure with the future's `failure` after either.
/// `InvalidOptions`: the target workflow ID is empty (the worker logs it).
pub const ExternalWorkflowError = error{ ExternalWorkflowNotFound, ExternalWorkflowRequestFailed, Canceled, OutOfMemory, InvalidOptions };

/// The pending delivery of a signal to another workflow, from `Context.signalExternalWorkflow`
/// or `ChildWorkflowFuture.signal`.
pub const SignalExternalFuture = ExternalFuture;

/// The pending delivery of a cancellation request to another workflow, from
/// `Context.cancelExternalWorkflow`.
pub const CancelExternalFuture = ExternalFuture;

const ExternalFuture = struct {
    const Self = @This();

    ctx: *Context,
    /// The validation error, if the request was rejected before it was sent; `get`
    /// returns it.
    state: Instance.ScheduleError!*Instance.ExternalState,

    /// Waits until the server has delivered the signal or cancellation request. If the
    /// calling coroutine is canceled while waiting, this returns `error.Canceled` at once.
    /// A signal is also withdrawn if the server hasn't recorded it yet; once recorded it is
    /// delivered anyway. A cancellation request is never withdrawn.
    pub fn get(f: Self) ExternalWorkflowError!void {
        const state = try f.state;
        try f.ctx.instance.awaitExternal(state);
        switch (state.outcome.?) {
            .delivered => {},
            .not_found => |fl| {
                f.ctx.instance.setLastFailure(fl);
                return error.ExternalWorkflowNotFound;
            },
            .failed => |fl| {
                f.ctx.instance.setLastFailure(fl);
                return error.ExternalWorkflowRequestFailed;
            },
            .canceled => return error.Canceled,
        }
    }

    /// Why the request failed, after `get` returned `error.ExternalWorkflowNotFound` or
    /// `error.ExternalWorkflowRequestFailed`.
    pub fn failure(f: Self) ?*const failure_mod.Failure {
        const state = f.state catch return null;
        const outcome = state.outcome orelse return null;
        return switch (outcome) {
            .delivered => null,
            .not_found, .failed, .canceled => |fl| fl,
        };
    }

    /// Whether the server has answered, so `get` won't block.
    pub fn isReady(f: Self) bool {
        const state = f.state catch return true;
        return state.outcome != null;
    }
};

/// The version `Context.getVersion` returns for a run that passed the change's point before
/// the change existed.
pub const default_version: i32 = -1;

/// What a worker registers: a name, a type-erased call that decodes arguments, runs the
/// workflow, and encodes its result, and the workflow's versioning behavior.
pub const Registered = struct {
    name: []const u8,
    invoke: *const fn (ctx: *Context, args: []const converter.Payload) anyerror!converter.Payload,
    versioning_behavior: VersioningBehavior = .unspecified,
    /// Set for a struct workflow: called when each run is initialized, before its first
    /// coroutine starts and before the activation's signals and updates are delivered. It
    /// allocates the run's state in `ctx.allocator()`, registers the declared handlers, and
    /// returns the state, which `invoke` reads back. Null for function and dynamic workflows.
    init_run: ?*const fn (ctx: *Context) anyerror!*anyopaque = null,
};

pub const VersioningBehavior = enum(i32) {
    /// The worker's `default_versioning_behavior` applies. A versioned worker without one
    /// refuses to run a workflow with this behavior.
    unspecified = 0,
    /// Each run stays on the version it started on, until it closes or continues as new.
    /// Changing such a workflow's code needs no patches: each version keeps its own code.
    pinned = 1,
    /// Each run moves to the deployment's current (or ramping) version at its next workflow
    /// task, and runs the new version's code from there: an incompatible change needs a
    /// patch (`Context.patched`).
    auto_upgrade = 2,
};

/// Options of a workflow definition, for `DefWithOptions`.
pub const DefOptions = struct {
    /// Sent with each of the run's workflow task completions; see `VersioningBehavior`.
    versioning_behavior: VersioningBehavior = .unspecified,
};

/// Names a workflow. The result is a type that clients pass to `executeWorkflow` and workers
/// pass to `registerWorkflow`. `definition` takes one of two forms:
///
/// - A function `fn (ctx: *Context, args...) !Result`.
/// - A struct type `W` whose `pub fn run(self: *W, ctx: *Context, args...) !Result` is the
///   workflow and whose fields are the run's state. `Args` and `Result` come from `run` as
///   they do from a function.
///
/// Rules of the struct form, each checked at compile time with an error that names the
/// workflow, the declaration, and the signature to write:
///
/// - Each run gets one `W`, initialized as `W{}` in `ctx.allocator()`, so every field needs a
///   default value. It lives as long as the run stays cached.
/// - `pub const queries`, `signals`, and `updates`, each optional, are struct literals that map
///   a pub method of `W` to a `QueryDef`, `SignalDef`, or `UpdateDef`. An update entry may also
///   name a validator method: `.{ UpdateDef, .validatorMethod }`.
/// - A handler method takes `self: *W` followed by the Def's arguments and returns what
///   `Context.setQueryHandler`, `setSignalHandler`, or `setUpdateHandler` accepts. A
///   validator returns `void` or `!void`. Each Def appears once per declaration.
/// - Other pub declarations of `W` are ignored.
///
///     pub const Counter = struct {
///         ctx: *temporal.workflow.Context = undefined, // run sets it before any handler runs
///         value: i64 = 0,
///
///         pub const queries = .{ .current = Current };
///         pub const signals = .{ .add = Add };
///         pub const updates = .{ .reset = .{ Reset, .checkReset } };
///
///         pub fn run(c: *Counter, ctx: *temporal.workflow.Context) !i64 { ... }
///         pub fn current(c: *Counter) i64 { ... }
///         pub fn add(c: *Counter, amount: i64) void { ... }
///         pub fn reset(c: *Counter, to: i64) i64 { ... }
///         pub fn checkReset(_: *Counter, to: i64) error{Negative}!void { ... }
///     };
///     pub const CounterWorkflow = temporal.workflow.Def("Counter", Counter);
///
/// The declared handlers are registered as each run is initialized, before the signals and
/// updates of its first workflow task are delivered. So those sent with the start
/// (signal-with-start, update-with-start) reach their handlers in arrival order, and none
/// waits in a buffer or is rejected as unregistered. Validators and query handlers run when
/// their requests arrive, possibly before `run` starts, so they may see `W{}`. Signal and
/// update handlers start, in arrival order, only once `run` is called, and run when it first
/// blocks or returns. `run` can finish setting up `W` first, and no handler runs for a run
/// whose arguments didn't decode. Handler methods take no `Context`: a handler that logs, runs
/// an activity, or allocates reads a `*Context` field that `run` sets before it first blocks.
/// `run` may call the `Context` setters too, with `self` as the context; registering a
/// declared name again replaces its handler.
pub fn Def(comptime name: []const u8, comptime definition: anytype) type {
    return DefWithOptions(name, definition, .{});
}

/// `Def` with options, such as the workflow's versioning behavior on versioned workers:
///
///     pub const Checkout = temporal.workflow.DefWithOptions("Checkout", checkout, .{ .versioning_behavior = .pinned });
pub fn DefWithOptions(comptime name: []const u8, comptime definition: anytype, comptime options: DefOptions) type {
    if (comptime defError(name, definition)) |message| @compileError(message);
    if (@TypeOf(definition) == type) return StructDef(name, definition, options);
    const F = @TypeOf(definition);
    const Ret = @typeInfo(F).@"fn".return_type.?;
    return struct {
        pub const workflow_name = name;
        pub const function = definition;
        pub const Args = @import("activity.zig").ArgsOf(F);
        pub const Result = switch (@typeInfo(Ret)) {
            .error_union => |eu| eu.payload,
            else => Ret,
        };

        pub fn invoke(ctx: *Context, payloads: []const converter.Payload) anyerror!converter.Payload {
            const args = try converter.fromPayloads(Args, ctx.allocator(), payloads);
            const result = try @call(.auto, definition, .{ctx} ++ args);
            return converter.toPayload(ctx.allocator(), result);
        }

        pub const versioning_behavior = options.versioning_behavior;
        pub const registered: Registered = .{ .name = name, .invoke = invoke, .versioning_behavior = options.versioning_behavior };
    };
}

fn StructDef(comptime name: []const u8, comptime W: type, comptime options: DefOptions) type {
    const params = @typeInfo(@TypeOf(W.run)).@"fn".param_types;
    const Ret = @typeInfo(@TypeOf(W.run)).@"fn".return_type.?;
    return struct {
        pub const workflow_name = name;
        pub const Args = Args: {
            var types: [params.len - 2]type = undefined;
            for (params[2..], &types) |p, *t| t.* = p.?;
            break :Args @Tuple(&types);
        };
        pub const Result = switch (@typeInfo(Ret)) {
            .error_union => |eu| eu.payload,
            else => Ret,
        };

        pub fn invoke(ctx: *Context, payloads: []const converter.Payload) anyerror!converter.Payload {
            const args = try converter.fromPayloads(Args, ctx.allocator(), payloads);
            const state = ctx.instance.run_state orelse return error.StructWorkflowNotInitialized;
            try ctx.instance.releaseHandlers();
            const w: *W = @ptrCast(@alignCast(state));
            const result = try @call(.auto, W.run, .{ w, ctx } ++ args);
            return converter.toPayload(ctx.allocator(), result);
        }

        fn initRun(ctx: *Context) anyerror!*anyopaque {
            const w = try ctx.allocator().create(W);
            w.* = .{};
            inline for (comptime entryNames(W, "queries")) |entry| {
                const D = comptime handlerEntry("queries", @field(W.queries, entry)).?.def;
                try ctx.setQueryHandler(D, w, @field(W, entry));
            }
            inline for (comptime entryNames(W, "signals")) |entry| {
                const D = comptime handlerEntry("signals", @field(W.signals, entry)).?.def;
                try ctx.setSignalHandler(D, w, @field(W, entry));
            }
            inline for (comptime entryNames(W, "updates")) |entry| {
                const e = comptime handlerEntry("updates", @field(W.updates, entry)).?;
                if (e.validator) |v|
                    try ctx.setUpdateHandlerWithValidator(e.def, w, @field(W, entry), @field(W, v))
                else
                    try ctx.setUpdateHandler(e.def, w, @field(W, entry));
            }
            return w;
        }

        pub const versioning_behavior = options.versioning_behavior;
        pub const registered: Registered = .{
            .name = name,
            .invoke = invoke,
            .versioning_behavior = options.versioning_behavior,
            .init_run = initRun,
        };
    };
}

/// Why `definition` can't define workflow `name`, as the compile error `DefWithOptions`
/// reports, or null if it can.
fn defError(comptime name: []const u8, comptime definition: anytype) ?[]const u8 {
    const prefix = "workflow \"" ++ name ++ "\": ";
    if (@TypeOf(definition) == type) return structDefError(prefix, definition);
    const F = @TypeOf(definition);
    const want = "a function fn (ctx: *temporal.workflow.Context, args...) !Result or a struct type with pub fn run";
    const f = switch (@typeInfo(F)) {
        .@"fn" => |f| f,
        else => return prefix ++ "Def takes " ++ want ++ ", not " ++ @typeName(F),
    };
    if (f.param_types.len == 0 or f.param_types[0] != *Context)
        return prefix ++ "the workflow function must take *temporal.workflow.Context as its first parameter";
    if (f.is_generic or f.return_type == null)
        return prefix ++ "the workflow function must not be generic: give every parameter a concrete type, not " ++ @typeName(F);
    return null;
}

const handler_declarations = [_][]const u8{ "queries", "signals", "updates" };

fn structDefError(comptime prefix: []const u8, comptime W: type) ?[]const u8 {
    const info = switch (@typeInfo(W)) {
        .@"struct" => |s| s,
        else => return prefix ++ "Def takes a function or a struct type with pub fn run, not " ++ @typeName(W),
    };
    if (info.is_tuple) return prefix ++ "Def takes a function or a struct type with pub fn run, not the tuple " ++ @typeName(W);
    for (info.field_names, info.field_attrs) |field, attrs| if (attrs.default_value_ptr == null)
        return prefix ++ "field " ++ field ++ " of " ++ @typeName(W) ++ " has no default value; each run starts from " ++ @typeName(W) ++
            "{}, so give every field one (`= undefined` for one that run sets before it is read)";

    const want_run = "pub fn run(self: *" ++ @typeName(W) ++ ", ctx: *temporal.workflow.Context, args...) !Result";
    if (!@hasDecl(W, "run")) return prefix ++ @typeName(W) ++ " has no pub fn run; declare " ++ want_run;
    const Run = @TypeOf(W.run);
    const bad_run = prefix ++ @typeName(W) ++ ".run must be " ++ want_run ++ " with concrete argument types, not " ++ @typeName(Run);
    const f = switch (@typeInfo(Run)) {
        .@"fn" => |f| f,
        else => return bad_run,
    };
    if (f.is_generic or f.return_type == null or f.param_types.len < 2 or f.param_types[0] != *W or f.param_types[1] != *Context)
        return bad_run;

    for (handler_declarations) |decl| {
        if (!@hasDecl(W, decl)) continue;
        const entries = @field(W, decl);
        const bad_decl = prefix ++ @typeName(W) ++ "." ++ decl ++ " must be a struct literal that maps pub method names to " ++
            defKind(decl).def ++ "s, such as " ++ defKind(decl).example ++ ", not " ++ @typeName(@TypeOf(entries));
        const e_info = switch (@typeInfo(@TypeOf(entries))) {
            .@"struct" => |s| s,
            else => return bad_decl,
        };
        if (e_info.is_tuple and e_info.field_names.len > 0) return bad_decl;
        for (e_info.field_names, 0..) |entry, i| {
            const where = prefix ++ decl ++ "." ++ entry;
            const value = @field(entries, entry);
            const e = handlerEntry(decl, value) orelse return where ++ " must be " ++ defKind(decl).want ++ ", not " ++ describe(value);
            const def_name = @field(e.def, defKind(decl).name_decl);
            for (e_info.field_names[0..i]) |earlier| {
                const other = handlerEntry(decl, @field(entries, earlier)).?.def;
                if (std.mem.eql(u8, @field(other, defKind(decl).name_decl), def_name))
                    return where ++ " and " ++ decl ++ "." ++ earlier ++ " both handle " ++ defKind(decl).noun ++ " \"" ++ def_name ++ "\"; keep one";
            }
            const Result = if (std.mem.eql(u8, decl, "signals")) void else e.def.Result;
            const want = handlerSignature(e.def.Args, *W, Result);
            if (!@hasDecl(W, entry))
                return where ++ " names no pub method of " ++ @typeName(W) ++ "; declare pub fn " ++ entry ++ " as " ++ want;
            if (handlerMismatch(where, e.def.Args, *W, @TypeOf(@field(W, entry)), Result)) |message| return message;
            const v = e.validator orelse continue;
            const v_where = where ++ " validator ." ++ v;
            if (!@hasDecl(W, v))
                return v_where ++ " names no pub method of " ++ @typeName(W) ++ "; declare pub fn " ++ v ++ " as " ++ handlerSignature(e.def.Args, *W, void);
            if (handlerMismatch(v_where, e.def.Args, *W, @TypeOf(@field(W, v)), void)) |message| return message;
        }
    }
    return null;
}

fn defKind(comptime decl: []const u8) struct { def: []const u8, name_decl: []const u8, noun: []const u8, example: []const u8, want: []const u8 } {
    if (std.mem.eql(u8, decl, "queries")) return .{
        .def = "QueryDef",
        .name_decl = "query_name",
        .noun = "query",
        .example = "`.{ .status = Status }`",
        .want = "a QueryDef (temporal.workflow.QueryDef(name, fn (args...) Result))",
    };
    if (std.mem.eql(u8, decl, "signals")) return .{
        .def = "SignalDef",
        .name_decl = "signal_name",
        .noun = "signal",
        .example = "`.{ .approve = Approve }`",
        .want = "a SignalDef (temporal.workflow.SignalDef(name, fn (args...) void))",
    };
    return .{
        .def = "UpdateDef",
        .name_decl = "update_name",
        .noun = "update",
        .example = "`.{ .setLimit = SetLimit }` or `.{ .setLimit = .{ SetLimit, .checkLimit } }`",
        .want = "an UpdateDef (temporal.workflow.UpdateDef(name, fn (args...) Result)), or .{ UpdateDef, .validatorMethod }",
    };
}

/// The Def of an entry of `W.queries`, `signals`, or `updates`, and the name of its validator
/// method, or null if the entry isn't one of `decl`'s kind.
fn handlerEntry(comptime decl: []const u8, comptime value: anytype) ?struct { def: type, validator: ?[]const u8 } {
    const name_decl = defKind(decl).name_decl;
    if (@TypeOf(value) == type) return if (isDef(value, name_decl)) .{ .def = value, .validator = null } else null;
    if (!std.mem.eql(u8, decl, "updates")) return null;
    const s = switch (@typeInfo(@TypeOf(value))) {
        .@"struct" => |s| s,
        else => return null,
    };
    if (!s.is_tuple or s.field_types.len != 2 or s.field_types[0] != type or @typeInfo(s.field_types[1]) != .enum_literal) return null;
    if (!isDef(value[0], name_decl)) return null;
    return .{ .def = value[0], .validator = @tagName(value[1]) };
}

fn isDef(comptime T: type, comptime name_decl: []const u8) bool {
    return switch (@typeInfo(T)) {
        .@"struct" => @hasDecl(T, name_decl),
        else => false,
    };
}

fn describe(comptime value: anytype) []const u8 {
    return if (@TypeOf(value) == type) @typeName(value) else "a value of type " ++ @typeName(@TypeOf(value));
}

/// The entry names of `W`'s `decl` declaration (`queries`, `signals`, or `updates`), in
/// declaration order; none if `W` doesn't declare it.
fn entryNames(comptime W: type, comptime decl: []const u8) []const [:0]const u8 {
    if (!@hasDecl(W, decl)) return &.{};
    return @typeInfo(@TypeOf(@field(W, decl))).@"struct".field_names;
}

/// Defines a dynamic workflow: one function that runs every workflow type the worker has no
/// `Def` registered for. `func` is `fn (*Context, converter.EncodedValues) !Result`; it reads
/// the type it runs as from `ctx.info().workflow_type` and decodes its arguments itself. The
/// result is a type workers pass to `registerDynamicWorkflow`. On a versioned worker it
/// chooses each run's versioning behavior with `Context.setVersioningBehavior`, or the
/// worker's default applies.
pub fn DynamicDef(comptime func: anytype) type {
    const F = @TypeOf(func);
    const bad = "a dynamic workflow must be fn (*temporal.workflow.Context, temporal.converter.EncodedValues) !Result, not " ++ @typeName(F);
    const fn_info = switch (@typeInfo(F)) {
        .@"fn" => |f| f,
        else => @compileError(bad),
    };
    if (fn_info.param_types.len != 2 or fn_info.param_types[0] != *Context or fn_info.param_types[1] != converter.EncodedValues)
        @compileError(bad);
    const Ret = fn_info.return_type.?;
    return struct {
        pub const function = func;
        pub const Result = switch (@typeInfo(Ret)) {
            .error_union => |eu| eu.payload,
            else => Ret,
        };

        pub fn invoke(ctx: *Context, payloads: []const converter.Payload) anyerror!converter.Payload {
            const result = try func(ctx, .{ .payloads = payloads });
            return converter.toPayload(ctx.allocator(), result);
        }

        /// The registration. Its name is empty: it answers to every unregistered type.
        pub const registered: Registered = .{ .name = "", .invoke = invoke };
    };
}

/// Names a query and its signature: `F` is a function type whose parameters are the query's
/// arguments and whose return type is its result. Workflows answer it with
/// `Context.setQueryHandler`; clients send it by the same type.
///
///     pub const Progress = temporal.workflow.QueryDef("progress", fn (step: u32) []const u8);
pub fn QueryDef(comptime name: []const u8, comptime F: type) type {
    if (@typeInfo(F) != .@"fn")
        @compileError("QueryDef(\"" ++ name ++ "\", F): F must be a function type such as `fn (u32) []const u8`, not " ++ @typeName(F));
    const Ret = @typeInfo(F).@"fn".return_type orelse
        @compileError("QueryDef(\"" ++ name ++ "\", F): F must have a concrete return type");
    return struct {
        pub const query_name = name;
        /// The query's arguments, as a tuple.
        pub const Args = std.meta.ArgsTuple(F);
        pub const Result = switch (@typeInfo(Ret)) {
            .error_union => |eu| eu.payload,
            else => Ret,
        };
    };
}

/// Names a signal and its arguments: `F` is a function type whose parameters are the
/// signal's arguments and whose return type is `void`. Workflows handle it with
/// `Context.setSignalHandler`; clients send it by the same type.
///
///     pub const Approve = temporal.workflow.SignalDef("approve", fn (approver: []const u8) void);
pub fn SignalDef(comptime name: []const u8, comptime F: type) type {
    if (@typeInfo(F) != .@"fn")
        @compileError("SignalDef(\"" ++ name ++ "\", F): F must be a function type such as `fn ([]const u8) void`, not " ++ @typeName(F));
    const Ret = @typeInfo(F).@"fn".return_type orelse
        @compileError("SignalDef(\"" ++ name ++ "\", F): F must have a concrete return type");
    if (Ret != void)
        @compileError("SignalDef(\"" ++ name ++ "\", F): a signal returns nothing, so F must return void, not " ++ @typeName(Ret));
    return struct {
        pub const signal_name = name;
        /// The signal's arguments, as a tuple.
        pub const Args = std.meta.ArgsTuple(F);
    };
}

/// Names an update and its signature: `F` is a function type whose parameters are the
/// update's arguments and whose return type is its result (an error union is allowed; only its
/// payload is sent). Workflows handle it with `Context.setUpdateHandler`; clients send it by
/// the same type.
///
///     pub const SetValue = temporal.workflow.UpdateDef("set", fn (value: i64) i64);
pub fn UpdateDef(comptime name: []const u8, comptime F: type) type {
    if (@typeInfo(F) != .@"fn")
        @compileError("UpdateDef(\"" ++ name ++ "\", F): F must be a function type such as `fn (i64) i64`, not " ++ @typeName(F));
    const Ret = @typeInfo(F).@"fn".return_type orelse
        @compileError("UpdateDef(\"" ++ name ++ "\", F): F must have a concrete return type");
    return struct {
        pub const update_name = name;
        /// The update's arguments, as a tuple.
        pub const Args = std.meta.ArgsTuple(F);
        pub const Result = switch (@typeInfo(Ret)) {
            .error_union => |eu| eu.payload,
            else => Ret,
        };
    };
}

/// The address a handler context points to, for the on-stack check, or null if it isn't a
/// pointer.
fn contextAddress(context: anytype) ?usize {
    return switch (@typeInfo(@TypeOf(context))) {
        .pointer => |p| switch (p.size) {
            .slice => if (context.len > 0) @intFromPtr(context.ptr) else null,
            else => @intFromPtr(context),
        },
        else => null,
    };
}

/// Compile error unless `H` is `fn (C, Args...) Result` or returns an error union of
/// `Result`. `what` names the handler in the message, such as `query handler for "status"`.
fn checkHandler(comptime what: []const u8, comptime Args: type, comptime C: type, comptime H: type, comptime Result: type) void {
    if (handlerMismatch(what, Args, C, H, Result)) |message| @compileError(message);
}

/// The signature `checkHandler` wants, such as `fn (*State, u32) u32 (or an error union of it)`.
fn handlerSignature(comptime Args: type, comptime C: type, comptime Result: type) []const u8 {
    var want: []const u8 = "fn (" ++ @typeName(C);
    for (@typeInfo(Args).@"struct".field_types) |T| want = want ++ ", " ++ @typeName(T);
    return want ++ ") " ++ @typeName(Result) ++ (if (Result == void) " (or !void)" else " (or an error union of it)");
}

/// `checkHandler`'s message for a handler `H` of the wrong shape, or null if it fits.
fn handlerMismatch(comptime what: []const u8, comptime Args: type, comptime C: type, comptime H: type, comptime Result: type) ?[]const u8 {
    const arg_types = @typeInfo(Args).@"struct".field_types;
    const bad = what ++ " must be " ++ handlerSignature(Args, C, Result) ++ ", not " ++ @typeName(H);
    const info = switch (@typeInfo(H)) {
        .@"fn" => |f| f,
        else => return bad,
    };
    if (info.param_types.len != 1 + arg_types.len) return bad;
    if (info.param_types[0] != C) return bad;
    for (arg_types, info.param_types[1..]) |want_t, got| if (got != want_t) return bad;
    const Ret = info.return_type orelse return bad;
    const payload = switch (@typeInfo(Ret)) {
        .error_union => |eu| eu.payload,
        else => Ret,
    };
    if (payload != Result) return bad;
    return null;
}

test {
    _ = @import("workflow/Scheduler.zig");
    _ = WorkflowIo;
    _ = Instance;
}

const StructDefTests = struct {
    const Add = SignalDef("add", fn (amount: i64) void);
    const Total = QueryDef("total", fn () i64);
    const Reset = UpdateDef("reset", fn (to: i64) i64);

    const Counter = struct {
        total: i64 = 0,
        ctx: *Context = undefined,

        pub const queries = .{ .currentTotal = Total };
        pub const signals = .{ .add = Add };
        pub const updates = .{ .reset = .{ Reset, .checkReset } };
        pub const not_a_handler = 42;

        pub fn run(c: *Counter, ctx: *Context, start: i64, label: []const u8) !i64 {
            _ = ctx;
            _ = label;
            return c.total + start;
        }
        pub fn currentTotal(c: *Counter) i64 {
            return c.total;
        }
        pub fn add(c: *Counter, amount: i64) void {
            c.total += amount;
        }
        pub fn reset(c: *Counter, to: i64) i64 {
            defer c.total = to;
            return c.total;
        }
        pub fn checkReset(_: *Counter, to: i64) error{Negative}!void {
            if (to < 0) return error.Negative;
        }
    };

    fn expectMessage(comptime message: ?[]const u8, comptime parts: []const []const u8) !void {
        const m = message orelse return error.TestExpectedCompileError;
        inline for (parts) |part| if (std.mem.indexOf(u8, m, part) == null) {
            std.debug.print("message {s}\n  lacks {s}\n", .{ m, part });
            return error.TestUnexpectedMessage;
        };
    }
};

test "a struct Def declares the same interface as a function Def, from run" {
    const D = DefWithOptions("Counter", StructDefTests.Counter, .{ .versioning_behavior = .pinned });
    try std.testing.expectEqualStrings("Counter", D.workflow_name);
    try std.testing.expect(D.Args == struct { i64, []const u8 });
    try std.testing.expect(D.Result == i64);
    try std.testing.expectEqual(VersioningBehavior.pinned, D.versioning_behavior);
    try std.testing.expectEqual(VersioningBehavior.pinned, D.registered.versioning_behavior);
    try std.testing.expectEqualStrings("Counter", D.registered.name);
    try std.testing.expect(D.registered.init_run != null);
    try std.testing.expect(Def("Plain", StructDefTests.Counter).versioning_behavior == .unspecified);

    const F = Def("Fn", struct {
        fn f(_: *Context) !void {}
    }.f);
    try std.testing.expect(F.registered.init_run == null);
    try std.testing.expect(comptime defError("Counter", StructDefTests.Counter) == null);
}

test "a struct Def's compile errors name the workflow and what to write" {
    const T = StructDefTests;
    try T.expectMessage(comptime defError("W", @as(u32, 5)), &.{ "workflow \"W\"", "Def takes a function", "struct type with pub fn run", "u32" });
    try T.expectMessage(comptime defError("W", enum { a }), &.{ "workflow \"W\"", "Def takes a function or a struct type" });
    try T.expectMessage(comptime defError("W", struct { u8 }), &.{ "workflow \"W\"", "not the tuple" });
    try T.expectMessage(comptime defError("W", struct {
        fn f(_: u8) !void {}
    }.f), &.{ "workflow \"W\"", "*temporal.workflow.Context as its first parameter" });

    // Fields need defaults.
    const NoDefault = struct {
        value: i64,
        pub fn run(_: *@This(), _: *Context) !void {}
    };
    try T.expectMessage(comptime defError("Counter", NoDefault), &.{ "workflow \"Counter\"", "field value", "has no default value" });

    // `run`: missing, of the wrong shape, or generic.
    try T.expectMessage(comptime defError("W", struct {
        x: u8 = 0,
    }), &.{ "workflow \"W\"", "has no pub fn run", "pub fn run(self: *", "ctx: *temporal.workflow.Context, args...) !Result" });
    const NoCtx = struct {
        pub fn run(_: *@This(), _: i64) !void {}
    };
    try T.expectMessage(comptime defError("W", NoCtx), &.{ "workflow \"W\"", ".run must be pub fn run(self: *", "not fn (*" });
    const ConstSelf = struct {
        pub fn run(_: *const @This(), _: *Context) !void {}
    };
    try T.expectMessage(comptime defError("W", ConstSelf), &.{".run must be pub fn run(self: *"});
    const Generic = struct {
        pub fn run(_: *@This(), _: *Context, x: anytype) !void {
            _ = x;
        }
    };
    try T.expectMessage(comptime defError("W", Generic), &.{ ".run must be", "concrete argument types" });
    const NotFn = struct {
        pub const run = 5;
    };
    try T.expectMessage(comptime defError("W", NotFn), &.{ ".run must be", "comptime_int" });
}

test "a struct Def's handler declaration errors name the workflow, the entry, and the signature" {
    const T = StructDefTests;
    const Base = struct {
        fn With(comptime decls: type) type {
            return struct {
                pub const signals = if (@hasDecl(decls, "signals")) decls.signals else .{};
                pub const queries = if (@hasDecl(decls, "queries")) decls.queries else .{};
                pub const updates = if (@hasDecl(decls, "updates")) decls.updates else .{};
                pub fn run(_: *@This(), _: *Context) !void {}
                pub fn add(_: *@This(), _: i64) void {}
                pub fn wrongAdd(_: *@This(), _: []const u8) void {}
                pub fn currentTotal(_: *@This()) i64 {
                    return 0;
                }
                pub fn reset(_: *@This(), to: i64) i64 {
                    return to;
                }
                pub fn badCheck(_: *@This(), _: i64) bool {
                    return true;
                }
            };
        }
    };

    // A declaration that isn't a struct literal mapping names.
    try T.expectMessage(comptime defError("W", Base.With(struct {
        pub const signals = 3;
    })), &.{ "workflow \"W\"", ".signals must be a struct literal", "SignalDefs", ".{ .approve = Approve }" });
    try T.expectMessage(comptime defError("W", Base.With(struct {
        pub const updates = .{ T.Reset, T.Reset };
    })), &.{ ".updates must be a struct literal", "UpdateDefs" });

    // An entry of the wrong kind.
    try T.expectMessage(comptime defError("Counter", Base.With(struct {
        pub const signals = .{ .add = T.Reset };
    })), &.{ "workflow \"Counter\"", "signals.add must be a SignalDef", "UpdateDef(\"reset\"" });
    try T.expectMessage(comptime defError("W", Base.With(struct {
        pub const queries = .{ .currentTotal = 7 };
    })), &.{ "queries.currentTotal must be a QueryDef", "a value of type comptime_int" });
    try T.expectMessage(comptime defError("W", Base.With(struct {
        pub const signals = .{ .add = .{ T.Add, .add } };
    })), &.{"signals.add must be a SignalDef"});
    try T.expectMessage(comptime defError("W", Base.With(struct {
        pub const updates = .{ .reset = .{ T.Reset, "checkReset" } };
    })), &.{ "updates.reset must be an UpdateDef", ".{ UpdateDef, .validatorMethod }" });

    // An entry that names no pub method.
    try T.expectMessage(comptime defError("W", Base.With(struct {
        pub const signals = .{ .missing = T.Add };
    })), &.{ "workflow \"W\"", "signals.missing names no pub method of", "declare pub fn missing as fn (*", ", i64) void (or !void)" });

    // Two entries for the same name.
    try T.expectMessage(comptime defError("W", Base.With(struct {
        pub const signals = .{ .add = T.Add, .wrongAdd = SignalDef("add", fn ([]const u8) void) };
    })), &.{"signals.wrongAdd and signals.add both handle signal \"add\""});

    // A method with the wrong signature.
    try T.expectMessage(comptime defError("Counter", Base.With(struct {
        pub const signals = .{ .wrongAdd = T.Add };
    })), &.{ "workflow \"Counter\": signals.wrongAdd must be fn (*", ", i64) void (or !void), not fn (*", "[]const u8) void" });
    try T.expectMessage(comptime defError("W", Base.With(struct {
        pub const queries = .{ .add = T.Total };
    })), &.{ "queries.add must be fn (*", ") i64 (or an error union of it)" });
    try T.expectMessage(comptime defError("W", Base.With(struct {
        pub const updates = .{ .currentTotal = T.Reset };
    })), &.{"updates.currentTotal must be fn (*"});

    // Validators: missing, or of the wrong signature.
    try T.expectMessage(comptime defError("W", Base.With(struct {
        pub const updates = .{ .reset = .{ T.Reset, .checkReset } };
    })), &.{ "updates.reset validator .checkReset names no pub method of", "declare pub fn checkReset as fn (*", ", i64) void (or !void)" });
    try T.expectMessage(comptime defError("W", Base.With(struct {
        pub const updates = .{ .reset = .{ T.Reset, .badCheck } };
    })), &.{ "workflow \"W\": updates.reset validator .badCheck must be fn (*", ", i64) void (or !void), not fn (*" });

    // Valid entries pass; other pub declarations are ignored.
    try std.testing.expect(comptime defError("W", Base.With(struct {
        pub const signals = .{ .add = T.Add };
        pub const queries = .{ .currentTotal = T.Total };
        pub const updates = .{ .reset = T.Reset };
    })) == null);
}
