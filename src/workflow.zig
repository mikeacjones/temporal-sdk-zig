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
//! workflow run stays cached on the worker. A worker can cap what one run holds
//! (`worker.Options.workflow_memory_limit`, counting that memory and the run's coroutine
//! stacks). Workflow code sees nothing when a run goes over it: allocations still succeed,
//! and the workflow task fails at the end of the activation and is retried, until the limit
//! is raised or the workflow keeps less state.
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
//! A value read often that changes rarely uses `ctx.mutableSideEffect` instead, which records
//! a new value only when it differs from the last one recorded.
//!
//! A library or interceptor that keeps state for each run declares a `LocalVar`: a typed
//! value with a default, which any code holding the run's `*Context` reads and sets, and
//! which each run has its own copy of. Query handlers, update validators and side-effect
//! functions may read it but not set it:
//!
//!     const depth: temporal.workflow.LocalVar(u32) = .{ .name = "audit.depth", .default = 0 };
//!     try depth.set(ctx, depth.get(ctx) + 1);
//!
//! Concurrency in a workflow uses `std.Io`, which `ctx.io()` implements deterministically:
//!
//! - Coroutines: `io.async(fn, args)`, or `std.Io.Group.async` for a set of them.
//! - Waiting for whichever result comes first: `std.Io.Select`.
//! - Channels between coroutines: `std.Io.Queue`; locks: `std.Io.Mutex`, `std.Io.Semaphore`.
//! - A value one coroutine sets and another waits for: a field plus a `std.Io.Event`;
//!   `std.Io.Condition` for waits on shared state guarded by a `std.Io.Mutex`.
//! - Naming a coroutine for stack-trace queries: `ctx.setCoroutineName`.
//! - Timers: `io.sleep`, or `io.async(std.Io.sleep, ...)` for a timer to race or cancel.
//! - Waiting for workflow state to change: `ctx.await`, `ctx.awaitTimeout`.
//! - Time: `ctx.now()`.
//!
//! Canceling a future cancels what it is waiting on: a pending `io.sleep` cancels its timer and
//! a pending `ActivityFuture.get` requests the activity's cancellation, a pending
//! `ChildWorkflowFuture.get` or `started` the child workflow's, a pending
//! `NexusOperationFuture.get` or `started` the Nexus operation's, and a pending
//! `SignalExternalFuture.get` withdraws its signal if the server hasn't recorded it yet.
//!
//! A workflow calls a Nexus operation through an endpoint by the operation's contract
//! (`nexus.OperationDef` and `nexus.ServiceDef`), so its input and output are checked at
//! compile time:
//!
//!     const echo = ctx.executeNexusOperation(HelloService, Echo, .{
//!         .endpoint = "hello-endpoint",
//!         .schedule_to_close_timeout = .fromSeconds(10),
//!     }, .{ .message = "hi" });
//!     const output = try echo.get();
//!
//! Queries read a workflow's state from outside. Name one with `QueryDef` and answer it with
//! `ctx.setQueryHandler`; the handler runs synchronously and must only read state. A call
//! that would start an activity, timer, child workflow or other command, or block, fails the
//! query with a `ReadOnlyViolation` failure naming the call, and nothing is sent:
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
//! `ctx.setDynamicQueryHandler` answers every query with no handler of its own, with the query
//! type and its arguments still encoded (`converter.EncodedValues`):
//!
//!     try ctx.setDynamicQueryHandler(state, State.anyQuery); // fn (*State, []const u8, EncodedValues) !T
//!
//! The worker answers three queries for every workflow without any registration:
//! `__stack_trace` (`StackTraceQuery`, which the workflow may answer itself instead),
//! `__enhanced_stack_trace` (`EnhancedStackTraceQuery`) and `__temporal_workflow_metadata`
//! (`WorkflowMetadataQuery`). The last two names are reserved: registering a handler for
//! either is a compile error. The metadata lists each registered query, signal and update
//! with the description given in its options (`QueryHandlerOptions`, `HandlerOptions`):
//!
//!     try ctx.setQueryHandlerWithOptions(Greeting, state, State.current, .{ .description = "The current greeting" });
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
//! Workflow code that would rather receive a signal where it needs it than in a handler opens
//! a `SignalChannel` with `ctx.signalChannel` (or `signalChannelByName`), which takes the place
//! of the signal's handler. Its `receive` blocks and works as a `std.Io.Select` case:
//!
//!     const approvals = try ctx.signalChannel(Approve);
//!     const approver = try approvals.receive(); // []const u8, in arrival order
//!
//! `ctx.removeSignalHandler` (and `removeSignalHandlerByName`, `removeDynamicSignalHandler`)
//! unregisters a handler or channel; `ctx.unhandledSignalNames` lists the signals nothing has
//! taken, including those unread in a channel.
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
//! `ctx.setDynamicUpdateHandler` (or `setDynamicUpdateHandlerWithValidator`) takes every
//! update with no handler of its own, with its name and arguments still encoded, in a
//! coroutine of its own like a typed handler:
//!
//!     try ctx.setDynamicUpdateHandler(state, State.anyUpdate); // fn (*State, []const u8, EncodedValues) !T
//!
//! The workflow ends when its function returns, even while signal or update handlers are still
//! running; to let them finish first,
//! `try ctx.await(ctx, temporal.workflow.Context.allHandlersFinished)`. Handlers still running
//! when the workflow completes, is canceled, or continues as new are named in one warning,
//! unless registered (or declared) with `.unfinished_policy = .abandon` (`HandlerOptions`).
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
const proto_time = @import("proto_time.zig");
const propagation = @import("propagation.zig");
const search_attributes = @import("search_attributes.zig");
const metrics_mod = @import("metrics.zig");
const deployment = @import("deployment.zig");
const nexus = @import("nexus.zig");
const call_check = @import("nexus/call_check.zig");
const system_nexus = @import("nexus/system.zig");
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
    /// Null when the workflow was started without one: the server sends zero for unset, and zero is read as null.
    execution_timeout: ?Duration = null,
    /// Null when the workflow was started without one: the server sends zero for unset, and zero is read as null.
    run_timeout: ?Duration = null,
    /// Null when the workflow was started without one: the server sends zero for unset, and zero is read as null.
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

/// What the SDK does about a signal or update handler that is still running when the
/// workflow completes, is canceled, or continues as new. Either way the handler is cut
/// short: its coroutine never resumes, and an unfinished update gets no result.
pub const HandlerUnfinishedPolicy = enum {
    /// Logs one warning at the end of the run naming every such handler (update names and
    /// IDs, signal names).
    warn_and_abandon,
    /// Leaves the handler out of that warning, for one whose work may be cut short.
    abandon,
};

/// Options for registering a signal or update handler (`Context.setSignalHandlerWithOptions`,
/// `setUpdateHandlerWithOptions`, and their dynamic and by-name variants).
pub const HandlerOptions = struct {
    unfinished_policy: HandlerUnfinishedPolicy = .warn_and_abandon,
    /// Shown for the signal or update in the answer to `__temporal_workflow_metadata`
    /// (`WorkflowMetadataQuery`). Copied into the run's memory. Ignored by the dynamic
    /// handlers, which have no name.
    description: []const u8 = "",
};

/// Options for registering a query handler (`Context.setQueryHandlerWithOptions`, or a
/// struct workflow's `queries` entry).
pub const QueryHandlerOptions = struct {
    /// Shown for the query in the answer to `__temporal_workflow_metadata`
    /// (`WorkflowMetadataQuery`). Copied into the run's memory.
    description: []const u8 = "",
};

/// A duration longer than protobuf can carry (about 10,000 years), such as `Duration.max`, is
/// sent as the longest one it can; a negative one is sent as it is.
pub const RetryPolicy = struct {
    initial_interval: ?Duration = null,
    backoff_coefficient: f64 = 0,
    maximum_interval: ?Duration = null,
    /// 0 means unlimited.
    maximum_attempts: i32 = 0,
    non_retryable_error_types: []const []const u8 = &.{},
};

/// How the server orders and shares out a workflow's or an activity's tasks among others on
/// the same task queue. Every zero field inherits its value from the parent or caller, or else uses the
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

/// Which build an activity runs on when its task queue uses build-ID-based versioning rules.
/// `compatible` runs it on a worker compatible with the workflow's build, and `default` on the
/// task queue's current default build. `unspecified` means `compatible` when the activity runs
/// on the workflow's task queue and `default` otherwise. It has no effect on task queues that
/// use worker deployments or no versioning; history records the choice as the scheduled
/// event's `use_workflow_build_id`.
pub const VersioningIntent = enum { unspecified, compatible, default };

/// What canceling an activity's future does: `try_cancel` requests the cancellation and
/// reports it at once, `wait_cancellation_completed` requests it and waits for the activity to
/// end, and `abandon` reports it at once without telling the activity.
pub const ActivityCancellationType = enum { try_cancel, wait_cancellation_completed, abandon };

/// How an activity is scheduled. At least one of `start_to_close_timeout` and
/// `schedule_to_close_timeout` is required: without either, the future's `get` returns
/// `error.InvalidOptions` (logged); the activity isn't scheduled.
/// A duration longer than protobuf can carry (about 10,000 years), such as `Duration.max`, is
/// sent as the longest one it can; a negative one is sent as it is.
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
    /// A short, single-line description of this activity, shown by the UI and CLI on its
    /// scheduled event. Sent as a JSON string payload, through the worker's payload codecs.
    /// Empty sends none.
    summary: []const u8 = "",
    /// The priority of this activity's task. Null uses the workflow's priority; inside a set
    /// priority, each zero field inherits the workflow's value. The server checks the values:
    /// one it rejects fails the workflow task.
    priority: ?Priority = null,
    /// Only matters on task queues with build-ID-based versioning rules; see
    /// `VersioningIntent`.
    versioning_intent: VersioningIntent = .unspecified,
    /// Runs the activity in a session (`Context.createSession`): on the session's host, with
    /// `activity.Info.session_id` set. Leave `task_queue` empty. A failed session makes the
    /// future's `get` return `error.SessionFailed`, and a closed one `error.InvalidOptions`
    /// (logged); the activity isn't scheduled then.
    session: ?*const Session = null,
};

/// How `Context.createSession` and `recreateSession` open a session. A session is held by a
/// creation activity that runs on the host for the session's whole life, so its timeouts are
/// that activity's, and it gets one attempt.
/// A duration longer than protobuf can carry (about 10,000 years), such as `Duration.max`, is
/// sent as the longest one it can; a negative one is sent as it is.
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
/// A duration longer than protobuf can carry (about 10,000 years), such as `Duration.max`, is
/// sent as the longest one it can; a negative one is sent as it is.
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
    /// A short, single-line description of this activity, recorded on its marker event and
    /// shown by the UI and CLI. Sent as a JSON string payload, through the worker's payload
    /// codecs, with every attempt. Empty sends none.
    summary: []const u8 = "",
    /// Record the activity's arguments in its local activity marker, so they show in the
    /// workflow's history beside the result. The marker holds the argument payloads as the
    /// worker's payload codecs encoded them. Off by default: a marker then holds only the result.
    arguments_in_marker: bool = false,
};

/// What canceling a child workflow does: `ChildWorkflowFuture.cancel`, or canceling the
/// coroutine waiting in its `get` or `started`. A canceled coroutine's wait returns
/// `error.Canceled` when the type says; after `ChildWorkflowFuture.cancel`, `get` returns
/// `error.ChildWorkflowFailed` then, with a canceled failure as the root cause.
///
/// - `try_cancel`, the default: requests the child's cancellation and reports it at once (in
///   the same activation), without waiting for the server. The request may not reach the
///   server if this workflow closes first.
/// - `wait_cancellation_requested`: requests the cancellation and reports it once the server
///   has recorded the request (the parent's history has the child's cancel-requested event),
///   while the child may still be running its cleanup. If the child closes first, it reports
///   how the child closed instead, as `wait_cancellation_completed` does.
/// - `wait_cancellation_completed`: requests the cancellation and waits for the child to close.
///   If it completes or fails instead of being canceled, that result is reported.
/// - `abandon`: sends no request; reports the cancellation at once and leaves the child
///   running. Its `parent_close_policy` still applies when this workflow closes.
///
/// A child canceled before its start was sent to the server is never started, with any type:
/// it reports a canceled start ("cancelled before scheduled") in the next activation. One whose
/// start was already sent is canceled as its type says; `wait_cancellation_requested` then
/// waits for the child to start before it can confirm the request.
pub const ChildWorkflowCancellationType = enum { try_cancel, wait_cancellation_requested, wait_cancellation_completed, abandon };

/// What the server does to a child workflow still running when its parent closes.
pub const ParentClosePolicy = enum { terminate, abandon, request_cancel };

/// How a child workflow is started.
///
/// Canceling the parent workflow cancels only the children it is waiting on. The cancellation
/// reaches the workflow function's coroutine, and from it every coroutine it waits on in
/// `Future.await`, `Group.await` or `Select` (and so on down); a coroutine canceled while it
/// waits in a child's `get` or `started` cancels that child as its `cancellation_type` says.
/// A child whose future no canceled coroutine is waiting on (one started and left, or awaited
/// by a coroutine started with `io.async` that nothing awaits) is left running; call its
/// future's `cancel` to cancel it, or rely on its `parent_close_policy`, which the server
/// applies when the parent closes (by default it terminates the child).
/// A duration longer than protobuf can carry (about 10,000 years), such as `Duration.max`, is
/// sent as the longest one it can; a negative one is sent as it is.
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
    /// A short single-line description of the child, shown in the UI and CLI on its started
    /// event. Sent as a JSON string payload (through the codecs); empty sends none.
    static_summary: []const u8 = "",
    /// A longer description of the child, shown in the UI and CLI. Sent as a JSON string
    /// payload (through the codecs); empty sends none.
    static_details: []const u8 = "",
    /// The priority of the child's tasks. Null uses the parent workflow's priority; inside a
    /// set priority, each zero field inherits the parent's value. The child reads the result
    /// in `Info.priority`. The server checks the values: one it rejects fails the workflow
    /// task.
    priority: ?Priority = null,
    /// Experimental: runs the child with a versioning behavior other than its definition's,
    /// independent of the parent's version: pinned to a deployment version, auto-upgrading, or
    /// routed once to a version (see `deployment.VersioningOverride`). Null sends none. Needs
    /// Temporal Server 1.32.0 or later. An override the server refuses fails the future with
    /// `error.ChildWorkflowInvalidVersioningOverride`. Only read while starting.
    versioning_override: ?deployment.VersioningOverride = null,
};

/// What canceling a Nexus operation does: `NexusOperationFuture.cancel`, or canceling the
/// coroutine waiting in its `get` or `started`. The core carries each type out; the future
/// resolves when the core says so, and a canceled operation's `failure()` describes the
/// cancellation (a Nexus operation failure whose cause is a canceled failure).
///
/// Once the operation has started (the server recorded its start, or it is waiting for one):
///
/// - `wait_cancellation_completed`, the default: requests the cancellation from the handler
///   and resolves once the operation has ended, as canceled, or as completed, failed, or
///   timed out if it ended that way first.
/// - `wait_cancellation_requested`: requests the cancellation and resolves as canceled once
///   the handler has accepted the request. If the handler refuses it, the future resolves as
///   failed with the refusal as its failure's cause.
/// - `try_cancel`: requests the cancellation and resolves as canceled at once (in the next
///   activation), without waiting for the handler. The request may not reach the handler if
///   this workflow closes first.
/// - `abandon`: sends no request; resolves as canceled at once and leaves the operation
///   running. Caution: sdk-core v1.2.0, which this SDK is built on, rejects the event that
///   records an abandoned operation ending (completing, failing, or timing out) as
///   nondeterministic, so a workflow that is still running when that happens fails its
///   workflow tasks from then on. Use `abandon` only when the workflow closes before the
///   operation can end, or use `try_cancel`.
///
/// An operation canceled in the same workflow task that scheduled it is never sent: with any
/// type, it resolves at once, its start failed with a canceled failure ("Nexus Operation
/// cancelled before scheduled"). One the server scheduled but the handler hasn't started yet
/// resolves as above for `try_cancel` and `abandon`; with the other two it resolves when the
/// server ends it (its start fails, canceled), or as for a started operation if it starts
/// first.
pub const NexusOperationCancellationType = enum { wait_cancellation_completed, wait_cancellation_requested, try_cancel, abandon };

/// How a Nexus operation is scheduled (`Context.executeNexusOperation`). Only read while
/// scheduling; strings are copied.
/// A duration longer than protobuf can carry (about 10,000 years), such as `Duration.max`, is
/// sent as the longest one it can; a negative one is sent as it is.
pub const NexusOperationOptions = struct {
    /// The name of the Nexus endpoint to call through. It must exist on the server: an
    /// operator creates it, routing to a namespace and task queue (in tests,
    /// `testing.WorkflowEnvironment.createNexusEndpoint`). An endpoint the server doesn't
    /// know fails the workflow task, which is retried, rather than the operation. Empty
    /// makes the future return `error.InvalidOptions` (logged).
    endpoint: []const u8,
    /// How long the workflow waits for the operation to complete, its start included; the
    /// server retries the start until then. Past it, the operation times out. Null leaves
    /// the server's limit.
    schedule_to_close_timeout: ?Duration = null,
    /// How long the handler may take to start the operation (or complete it, if it is
    /// synchronous). Past it, the operation times out. Null: no limit.
    schedule_to_start_timeout: ?Duration = null,
    /// How long an asynchronous operation may run once it started. Past it, it times out.
    /// Synchronous operations ignore it. Null: no limit.
    start_to_close_timeout: ?Duration = null,
    cancellation_type: NexusOperationCancellationType = .wait_cancellation_completed,
    /// Nexus headers sent with the request to start the operation (names are
    /// case-insensitive). They are recorded in history and reach the handler as they are,
    /// without the payload codecs, so don't put secrets in them. When the context propagators
    /// (`client.Options.propagators`) write values, the request carries these headers in
    /// their order, without any named `propagation.nexus_header_name` (ignoring case),
    /// followed by that header holding the values; otherwise exactly these. The slice
    /// isn't modified.
    headers: []const nexus.Header = &.{},
    /// A short, single-line description of this operation, shown by the UI and CLI on its
    /// scheduled event. Sent as a JSON string payload, through the worker's payload codecs.
    /// Empty sends none.
    summary: []const u8 = "",
};

/// Experimental: how the server routes the first workflow task of the run a continue-as-new starts.
pub const InitialVersioningBehavior = enum {
    /// The new run starts as an auto-upgrade run: its first workflow task goes to the task
    /// queue's current version (or, for its share of workflows, the ramping version). After that
    /// task the run follows the versioning behavior its definition declares.
    auto_upgrade,
    /// The new run's first workflow task goes to the task queue's ramping version, or to the
    /// current version when nothing is ramping. After that task the run follows its definition's behavior.
    ramping_version,
};

/// How `Context.continueAsNew` starts the next run.
/// A duration longer than protobuf can carry (about 10,000 years), such as `Duration.max`, is
/// sent as the longest one it can; a negative one is sent as it is.
pub const ContinueAsNewOptions = struct {
    /// Empty continues on this run's task queue.
    task_queue: []const u8 = "",
    /// Limits the new run. Null keeps this run's run timeout.
    run_timeout: ?Duration = null,
    /// Limits one workflow task of the new run. Null keeps this run's task timeout.
    task_timeout: ?Duration = null,
    /// The new run's memo, built with `converter.toFields`. Null or an empty slice keeps this
    /// run's memo, `upsertMemo` changes included. If that memo is empty because `upsertMemo`
    /// removed every key, the new run gets the memo this run started with instead: an empty
    /// memo on the command tells sdk-core to keep the start memo, and no command can ask for an
    /// empty one.
    memo: ?[]const converter.Field = null,
    /// The new run's search attributes, built with `search_attributes.Key.set` (unsets are
    /// ignored). Null keeps this run's current search attributes, upserts included.
    search_attributes: ?[]const search_attributes.Update = null,
    /// Null keeps this run's retry policy.
    retry_policy: ?RetryPolicy = null,
    /// Experimental: null lets the server decide as before: a pinned run continues on its own
    /// version, an auto-upgrade run moves only after the new run's first workflow task. A pinned
    /// versioning override on the run is inherited by the new run and wins over this field.
    initial_versioning_behavior: ?InitialVersioningBehavior = null,
};

/// How `Context.sleep` starts its timer. Only read while starting it; strings are copied.
pub const SleepOptions = struct {
    /// A short, single-line description of this timer, shown by the UI and CLI on its
    /// started event. Sent as a JSON string payload, through the worker's payload codecs.
    /// Empty sends none.
    summary: []const u8 = "",
};

pub const Context = struct {
    instance: *Instance,
    /// Set by `fail` or `failWith` in the workflow function (or a coroutine it started) and
    /// reported instead of the returned error's name. Signal and update handlers keep their own.
    pending_failure: ?Instance.PendingFailure = null,
    /// The last activity or child workflow failure a future returned to the workflow function
    /// (or a coroutine it started); reported as the workflow's failure when the workflow
    /// returns the future's error. Signal and update handlers keep their own.
    last_failure: ?*const failure_mod.Failure = null,

    /// The deterministic Io for this workflow: concurrency, timers, time, randomness.
    pub fn io(ctx: *Context) std.Io {
        return ctx.instance.wio.io();
    }

    /// Memory that lives as long as the workflow run is cached. In a query handler it is
    /// instead the query's memory, freed once the answer is sent, so answering queries
    /// doesn't grow the run.
    pub fn allocator(ctx: *Context) Allocator {
        return ctx.instance.allocator();
    }

    /// Names the calling coroutine (the workflow function, a handler, or one started with
    /// `io.async`, `io.concurrent`, a `std.Io.Group`, or a `std.Io.Select`) so stack-trace
    /// queries can tell coroutines apart. The name is copied into `allocator()`, so a
    /// coroutine that renames itself often grows the run's memory. Unnamed coroutines are
    /// "workflow", "signal", "update", "async", or "group". Outside a coroutine, as in a
    /// query handler, there is nothing to name: it does nothing and allocates nothing.
    pub fn setCoroutineName(ctx: *Context, name: []const u8) error{OutOfMemory}!void {
        if (ctx.instance.wio.scheduler.currentFiber() == null or ctx.instance.wio.refuse("setCoroutineName")) return;
        const copy = try ctx.allocator().dupe(u8, name);
        ctx.instance.wio.setCurrentName(copy);
    }

    /// The calling coroutine's name; see `setCoroutineName`. Valid while the run is cached.
    pub fn coroutineName(ctx: *const Context) []const u8 {
        return ctx.instance.wio.currentName();
    }

    pub fn info(ctx: *const Context) *const Info {
        return &ctx.instance.info;
    }

    /// A logger carrying the workflow's type, ID, and run ID as fields, as the workflow's
    /// outbound interceptors (`interceptor.WorkflowOutbound.logger`) return it. It is silent
    /// while the workflow replays, and while its run is evicted from the worker's cache (its
    /// coroutines unwinding after the worker stops or drops the run), unless
    /// `worker.Options.log_during_replay` is set.
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

    /// The run's converter context: the namespace, the workflow ID and type, and
    /// `propagated()`, as the run's arguments, result, signals, queries, updates and side
    /// effects are converted with. Encode failure details with it,
    /// `converter.toPayloadWithContext(arena, ctx.converterContext(), value)`. Valid for the
    /// run; workflow code only.
    ///
    /// A run a Nexus handler started, when the server propagated the caller's context to it,
    /// converts its result and failure with that context instead (`converter.Context.nexus`),
    /// as does an update whose task carries one for its result and rejections; this returns
    /// the run's own context all the same.
    pub fn converterContext(ctx: *const Context) *const converter.Context {
        return &ctx.instance.converter_context;
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

    /// A random (version 4) UUID as lowercase text (`xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx`),
    /// drawn from the run's seeded randomness (`ctx.io().random`), so a replay gets the same
    /// values in the same order and each call returns a new one. Use it for IDs workflow code
    /// sends in commands, such as child workflow IDs or activity arguments. Not secure
    /// randomness. The text lives in `allocator()`; in a query handler or update validator it
    /// draws from the handler's own copy of the generator and leaves the run's sequence as it
    /// was.
    pub fn randomUuid(ctx: *Context) error{OutOfMemory}![]const u8 {
        return @import("client.zig").uuid(ctx.allocator(), ctx.io());
    }

    /// Blocks the calling coroutine on a durable timer, as `ctx.io().sleep` does, with
    /// `options` on the timer. To race or cancel it, run it as a coroutine (`io.async`, or a
    /// `std.Io.Select` case): canceling the coroutine cancels the timer and returns
    /// `error.Canceled`. A zero or negative duration returns at once and starts no timer. A
    /// duration longer than protobuf can carry (about 10,000 years), such as `Duration.max`,
    /// starts a timer of the longest one it can.
    pub fn sleep(ctx: *Context, duration: Duration, options: SleepOptions) std.Io.Cancelable!void {
        return ctx.instance.sleep(duration, options.summary);
    }

    /// Blocks until `condition(state)` returns true.
    ///
    /// `state` is usually a pointer to the variables the condition reads; other coroutines,
    /// activity results, and timers change them. The condition is re-checked every time the
    /// workflow's coroutines have all blocked, so it must be cheap and free of side effects.
    pub fn await(ctx: *Context, state: anytype, comptime condition: fn (@TypeOf(state)) bool) error{ Canceled, OutOfMemory }!void {
        _ = try ctx.awaitInner(state, condition, null);
    }

    /// Like `await`, but gives up after `timeout` (a durable timer). Returns whether the
    /// condition holds when it returns: true also when the timer fired in the same workflow task
    /// as the change that made it true; false only if it still doesn't hold. A fired timer
    /// takes effect once the workflow task's other work (handlers, results, and the
    /// coroutines they wake) has run with no awaited condition left to hold. A zero or
    /// negative timeout returns false at once unless the condition already holds. A duration
    /// longer than protobuf can carry (about 10,000 years), such as `Duration.max`, starts a
    /// timer of the longest one it can.
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
    ///
    /// The `a.details` list and the `a.cause` chain (a future's failure, for example) are
    /// copied into the run's memory here, so they may live on the stack; strings and payload
    /// data must live as long as the run, as `allocator()` memory does. The failure's stack
    /// trace is the return trace of the error the workflow function or handler returns (see
    /// `worker.Options.failure_stack_traces`).
    pub fn fail(ctx: *Context, a: failure_mod.Application) error{ApplicationFailure} {
        const app = failure_mod.keep(ctx.allocator(), a) catch blk: {
            ctx.instance.logger.warn("out of memory copying the details and cause of a workflow failure; the failure is sent without them", .{ .message = a.message });
            var bare = a;
            bare.details = &.{};
            bare.cause = null;
            break :blk bare;
        };
        ctx.instance.setPendingFailure(.{ .application = app });
        return error.ApplicationFailure;
    }

    /// Fails the workflow with `f` as given (its kind, message, details and cause chain), and
    /// returns the error to return, as `fail` does. Use it to keep a failure that cleanup on
    /// the error path (an `errdefer`, a `defer`, a `catch`) would otherwise replace: by
    /// default a returned activity or child error reports the failure the latest `get`
    /// returned, so a failed cleanup activity would win.
    ///
    ///     const charge = ctx.executeActivity(Charge, opts, .{order});
    ///     charge.get() catch |err| {
    ///         ctx.executeActivity(Release, opts, .{order}).get() catch {};
    ///         return switch (err) {
    ///             error.ActivityFailed => ctx.failWith(charge.failure().?),
    ///             else => err,
    ///         };
    ///     };
    ///
    /// Call it only for the future's failure error (`error.ActivityFailed` and the like):
    /// a `get` can return `error.Canceled` while `failure()` holds the activity's failure,
    /// and returning `failWith`'s error then would fail a workflow that should be canceled.
    ///
    /// `fail` and `failWith` share one slot: the later call wins, and later failed `get`s
    /// don't change it. In an update handler, returning the error rejects the update with
    /// `f`. `f` itself is copied, but what it points to (message, details, cause) is read
    /// after the function returns, so it must live as long as the run: a failure read from
    /// a future (`future.failure()`) or built in `allocator()` does.
    pub fn failWith(ctx: *Context, f: *const failure_mod.Failure) error{ApplicationFailure} {
        ctx.instance.setPendingFailure(.{ .failure = f.* });
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

    /// Ends this run and starts a new run of the workflow type `workflow_type`, named at run
    /// time, with the same workflow ID and `args` (a tuple of argument values, encoded here),
    /// like `continueAsNew`. A dynamic workflow continues as the type it runs as by passing
    /// `ctx.info().workflow_type`; an empty name does the same. The name is copied.
    ///
    ///     return ctx.continueAsNewByName(ctx.info().workflow_type, .{}, .{ count + 1 });
    ///
    /// Return the error this gives back from the workflow function; everything else is as for
    /// `continueAsNew`, and workflow outbound interceptors see both alike.
    pub fn continueAsNewByName(ctx: *Context, workflow_type: []const u8, options: ContinueAsNewOptions, args: anytype) error{ ContinueAsNew, OutOfMemory, InvalidSearchAttribute } {
        const name = if (workflow_type.len == 0) ctx.info().workflow_type else workflow_type;
        try ctx.instance.requestContinueAsNew(name, options, args);
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
        ctx.instance.setVersioningBehavior(behavior);
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
    /// For a value that is read often and changes rarely, use `mutableSideEffect`, which
    /// records a new value only when it differs from the last one recorded.
    pub fn sideEffect(ctx: *Context, comptime T: type, context: anytype, comptime func: fn (@TypeOf(context)) T) SideEffectError!T {
        const value: ?converter.Payload = if (ctx.instance.replaying) null else try converter.toPayloadWithContext(ctx.allocator(), ctx.converterContext(), ctx.callSideEffectFn(T, func, .{context}));
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

    /// Reads a nondeterministic value that is read often and changes rarely (a configuration
    /// setting, for example), recording it in history only when it changed:
    ///
    ///     const limit = try ctx.mutableSideEffect(u32, "rate-limit", config, Config.rateLimit, eqlU32);
    ///
    /// `id` names the value within the run: calls with the same ID share one recorded value,
    /// and calls with different IDs are independent. It is used as given, with no validation,
    /// and must be valid UTF-8. Patch IDs starting with `__temporal_mse:` belong to the SDK.
    ///
    /// When not replaying, `func(context)` runs on every call. If no value is recorded for
    /// `id` yet, or `eql(recorded, current)` is false, the new value is recorded as a side
    /// effect is (`sideEffect`). Otherwise nothing is recorded, no history event is added,
    /// and no interceptor is called. On replay neither `func` nor `eql` runs: the value comes
    /// from what history recorded at or before this call.
    ///
    /// This returns the value recorded for `id`, decoded as `T` into `ctx.allocator()` on
    /// every call: never `func`'s fresh result, so first execution and replay return the same
    /// value. `eql` gets the recorded value first and the new one second; it must be pure,
    /// and must not block or touch the workflow context. `std.meta.eql` compares slices by
    /// pointer, so it is no use for a `T` holding slices. `func` follows `sideEffect`'s
    /// rules: quick, no blocking, no context use, and it returns `T`, not an error union.
    ///
    /// A call waits for any recording of `id` in flight, and for its own when the value
    /// changed, so other coroutines may run during it. A changed value adds a patch marker
    /// (with an update of the `TemporalChangeVersion` search attribute) and a side effect's
    /// marker to history. Errors are those of `sideEffect`, and `SideEffectFailed` also when
    /// called from read-only code (a query handler or update validator), or when a replayed
    /// call finds no recorded value, which also fails the workflow task.
    pub fn mutableSideEffect(
        ctx: *Context,
        comptime T: type,
        id: []const u8,
        context: anytype,
        comptime func: fn (@TypeOf(context)) T,
        comptime eql: fn (recorded: T, current: T) bool,
    ) SideEffectError!T {
        const inst = ctx.instance;
        const call = try inst.mutableSideEffectBegin(id);
        var value: ?converter.Payload = null;
        const changed = if (inst.replaying) try inst.mutableSideEffectNotified(id, call) else changed: {
            const current = ctx.callSideEffectFn(T, func, .{context});
            if (inst.mutableSideEffectValue(id)) |recorded| {
                // The previous value is decoded into scratch memory freed before returning, so
                // an unchanged call holds as much of the run's memory as it does on replay.
                var scratch: std.heap.ArenaAllocator = .init(inst.arena());
                defer scratch.deinit();
                const previous = converter.fromPayloadWithContext(T, scratch.allocator(), ctx.converterContext(), recorded) catch |err| return mapDecodeError(err);
                if (ctx.callSideEffectFn(bool, eql, .{ previous, current })) break :changed false;
            }
            value = try converter.toPayloadWithContext(ctx.allocator(), ctx.converterContext(), current);
            break :changed true;
        };
        if (changed) try inst.mutableSideEffectRecord(id, call, value);
        const recorded = inst.mutableSideEffectValue(id) orelse {
            inst.mutableSideEffectNotRecorded(id);
            return error.SideEffectFailed;
        };
        return converter.fromPayloadWithContext(T, ctx.allocator(), ctx.converterContext(), recorded) catch |err| mapDecodeError(err);
    }

    /// Calls a side-effect function, marking it as one for `LocalVar.set` (see
    /// `Instance.beginSideEffect`).
    fn callSideEffectFn(ctx: *Context, comptime R: type, comptime f: anytype, args: anytype) R {
        const previous = ctx.instance.beginSideEffect();
        defer ctx.instance.endSideEffect(previous);
        return @call(.auto, f, args);
    }

    fn mapDecodeError(err: converter.DecodeError) SideEffectError {
        return switch (err) {
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
            // In i96 and clamped like the timeout itself, so `Duration.max` cannot overflow.
            .heartbeat_interval_ms = @max(1, @as(i64, @intCast(@divTrunc(
                @min(options.heartbeat_timeout.nanoseconds, proto_time.max_duration.nanoseconds),
                3 * std.time.ns_per_ms,
            )))),
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
        if (ctx.instance.wio.refuse("completeSession")) return;
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

    /// Schedules the Nexus operation `Operation` (a `nexus.OperationDef`) of `Service` (a
    /// `nexus.ServiceDef`) through the endpoint `options.endpoint`, and returns a future for
    /// its start and its result. `input` is encoded here, by the payload converter and then
    /// the worker's payload codecs; a `void` input sends the converter's null payload. The
    /// operation runs wherever the endpoint routes, often a worker in another namespace.
    ///
    ///     const echo = ctx.executeNexusOperation(HelloService, Echo, .{ .endpoint = "hello-endpoint" }, .{ .message = "hi" });
    ///     const output = try echo.get();
    ///
    /// `Service` must have `Operation`, with the same input and output types: otherwise this
    /// is a compile error naming the service and the operation. An input of the wrong type is
    /// a compile error at the call. The call passes through the workflow's outbound interceptors
    /// (`interceptor.WorkflowOutbound.executeNexusOperation`).
    pub fn executeNexusOperation(ctx: *Context, comptime Service: type, comptime Operation: type, options: NexusOperationOptions, input: Operation.Input) NexusOperationFuture(Operation.Output) {
        if (comptime call_check.callError("executeNexusOperation", Service, Operation)) |message| @compileError(message);
        return .{ .ctx = ctx, .state = ctx.instance.executeNexusOperation(Service.service_name, Operation.operation_name, options, input).state };
    }

    /// Schedules a Nexus operation by its service and operation names, for a service this
    /// program has no contract for, like `executeNexusOperation`. `input` is any value the
    /// payload converter encodes; `Output` is the type to decode the result as. The names are
    /// copied. An empty service or operation name makes the future return
    /// `error.InvalidOptions` (logged).
    pub fn executeNexusOperationByName(ctx: *Context, comptime Output: type, service: []const u8, operation: []const u8, options: NexusOperationOptions, input: anytype) NexusOperationFuture(Output) {
        return .{ .ctx = ctx, .state = ctx.instance.executeNexusOperation(service, operation, options, input).state };
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

    /// Experimental: starts the workflow `Workflow` (a `Def`) with `args` if no run of
    /// `options.id` is running, and sends it `Signal` (a `SignalDef`) with `signal_args`
    /// either way, in one server call through the server's system Nexus endpoint. The target
    /// is in this workflow's namespace. Wrong argument types are compile errors. Returns a
    /// future for the target's run ID and whether this call started it; every error comes
    /// from its `get`.
    ///
    ///     const sent = ctx.signalWithStartWorkflow(Counter, .{ .id = "counter-1", .id_conflict_policy = .use_existing }, .{0}, Add, .{5});
    ///     const result = try sent.get();
    ///
    /// The payload converter encodes `args` and `signal_args` here, with the target's
    /// converter context (namespace, `options.id`, and the workflow type); the worker's payload
    /// codecs encode them, and the memo, when the command is sent. The call is a Nexus
    /// operation on endpoint `__temporal_system` in history, but no interceptor's
    /// `executeNexusOperation` sees it.
    ///
    /// The server must have `history.enableSignalWithStartFromWorkflow` turned on (the dev
    /// server: `--dynamic-config-value history.enableSignalWithStartFromWorkflow=true`).
    /// Without it, the server refuses to complete the workflow task that sends the call
    /// (gRPC Unimplemented, "SignalWithStart operation is disabled", which the worker logs as
    /// a warning). The SDK can't tell beforehand, so no error is returned: the task times out
    /// and is retried until the setting is turned on, and `get` waits meanwhile.
    ///
    /// Options the SDK can check are checked before anything is sent: `error.InvalidOptions`
    /// (logged) for an empty ID or name, `.fail` as the conflict policy, a negative timeout or
    /// start delay, `start_delay` with `cron_schedule`, strings that aren't UTF-8, a retry
    /// policy the server refuses, or a search attribute value that can't be indexed. Other
    /// request errors only the server finds (a search attribute it doesn't know, a workflow
    /// ID over its length limit, a reserved task queue) refuse the workflow task the same way
    /// as the missing setting, as for child workflows: the task is retried and `get` waits.
    pub fn signalWithStartWorkflow(ctx: *Context, comptime Workflow: type, options: SignalWithStartOptions, args: Workflow.Args, comptime Signal: type, signal_args: Signal.Args) SignalWithStartFuture {
        return ctx.instance.signalWithStart(Workflow.workflow_name, options, args, Signal.signal_name, signal_args);
    }

    /// Experimental: `signalWithStartWorkflow` with the workflow type and the signal named at
    /// run time. `args` and `signal_args` are tuples of argument values. An empty
    /// `workflow_type` or `signal_name` makes `get` return `error.InvalidOptions` (logged).
    pub fn signalWithStartWorkflowByName(ctx: *Context, workflow_type: []const u8, options: SignalWithStartOptions, args: anytype, signal_name: []const u8, signal_args: anytype) SignalWithStartFuture {
        return ctx.instance.signalWithStart(workflow_type, options, args, signal_name, signal_args);
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
        /// `context` points into a live workflow coroutine's stack (logged with the query type).
        QueryContextOnStack,
    };

    /// Answers queries of type `Query` (a `QueryDef`) with `handler(context, args...)`, which
    /// returns `Query.Result` or an error union of it; an error fails the query with the
    /// error's name. Registering the same query type again replaces its handler. A handler
    /// for `__stack_trace` (`StackTraceQuery`) replaces the worker's own answer to it. The
    /// worker answers `__enhanced_stack_trace` (`EnhancedStackTraceQuery`) and
    /// `__temporal_workflow_metadata` (`WorkflowMetadataQuery`) itself, so registering a
    /// handler for either is a compile error.
    ///
    /// Handlers run synchronously on the worker thread, outside any coroutine, possibly after
    /// the workflow function has returned. They must only read workflow state. A handler that
    /// calls something that would emit a command (`executeActivity`, `sleep`,
    /// `executeChildWorkflow`, `upsertMemo`, `signalExternalWorkflow`, and so on), register or
    /// remove a handler (`setSignalHandler`, `signalChannel`, `setUpdateHandler`, ...), take a
    /// signal from a channel, change the run's settings (`setVersioningBehavior`), start a
    /// coroutine, or block (`await`, a pending future's `get`) is refused: the call returns
    /// `error.InvalidOptions` or `error.Canceled` (or, where it can't fail, does nothing), and
    /// the query fails with a `ReadOnlyViolation` failure naming the call, whatever the
    /// handler returns. Nothing is sent and the workflow task doesn't fail. Waking a coroutine
    /// through `std.Io` (`Event.set`, `Condition.signal`, `Queue.put`) fails the query too,
    /// but the wake happens: `std.Io` has already changed the primitive. The SDK can't undo
    /// what a handler writes to workflow data or a `std.Io` primitive.
    /// `ctx.fail` and `ctx.failWith` describe the query's failure instead of the workflow's.
    /// Arguments and the result use memory that lives until the answer is sent, and so does
    /// `ctx.allocator()` while the handler runs.
    ///
    /// `context` is usually a pointer to the state the handler reads. It must outlive the run,
    /// so allocate it with `ctx.allocator()` (or point to a global); a pointer into a live
    /// workflow coroutine's stack returns `error.QueryContextOnStack`.
    ///
    ///     const state = try ctx.allocator().create(State);
    ///     try ctx.setQueryHandler(GetStatus, state, State.status);
    pub fn setQueryHandler(ctx: *Context, comptime Query: type, context: anytype, comptime handler: anytype) SetQueryHandlerError!void {
        if (comptime reservedQueryError(Query.query_name)) |message| @compileError(message);
        return ctx.setQueryHandlerWithOptions(Query, context, handler, .{});
    }

    /// `setQueryHandler` with options: a description, which the answer to
    /// `__temporal_workflow_metadata` shows for the query.
    ///
    ///     try ctx.setQueryHandlerWithOptions(GetStatus, state, State.status, .{ .description = "The order's status" });
    pub fn setQueryHandlerWithOptions(ctx: *Context, comptime Query: type, context: anytype, comptime handler: anytype, options: QueryHandlerOptions) SetQueryHandlerError!void {
        if (comptime reservedQueryError(Query.query_name)) |message| @compileError(message);
        const C = @TypeOf(context);
        comptime checkHandler("query handler for \"" ++ Query.query_name ++ "\"", Query.Args, C, @TypeOf(handler), Query.Result);
        const Erased = struct {
            fn call(p: *const anyopaque, out: Allocator, conv: *const converter.Context, payloads: []const converter.Payload) anyerror!converter.Payload {
                const c: *const C = @ptrCast(@alignCast(p));
                const args = try converter.fromPayloadsWithContext(Query.Args, out, conv, payloads);
                const result = @call(.auto, handler, .{c.*} ++ args);
                const value = if (@typeInfo(@TypeOf(result)) == .error_union) try result else result;
                return converter.toPayloadWithContext(out, conv, value);
            }
        };
        const stored = try ctx.allocator().create(C);
        stored.* = context;
        const description = try ctx.copyDescription(options.description);
        return ctx.instance.setQueryHandler(Query.query_name, .{ .context = @ptrCast(stored), .call = Erased.call, .description = description }, contextAddress(context));
    }

    /// `description` in the run's memory, or "" for an empty one.
    fn copyDescription(ctx: *Context, description: []const u8) error{OutOfMemory}![]const u8 {
        if (description.len == 0) return "";
        return ctx.allocator().dupe(u8, description);
    }

    /// Answers every query that has no handler registered for its type with
    /// `handler(context, name, args)`, where `name` is the query type and `args` its
    /// undecoded arguments (`converter.EncodedValues`), decoded with
    /// `args.get(T, ctx.allocator(), index)`: in a query handler that is the answer's memory,
    /// freed once it is sent. It returns the answer (any type the converter
    /// encodes) or an error union of it; an error fails the query with the error's name.
    /// Registering again replaces the dynamic handler. A handler registered for a query type
    /// with `setQueryHandler` still answers that type. Without a dynamic handler, a query of
    /// an unregistered type fails with `QueryNotRegistered`. The query types Temporal's tools
    /// send never reach the dynamic handler: the worker answers `__stack_trace`
    /// (`StackTraceQuery`), `__enhanced_stack_trace` (`EnhancedStackTraceQuery`) and
    /// `__temporal_workflow_metadata` (`WorkflowMetadataQuery`) itself, and other
    /// `__temporal...` types fail with `QueryNotRegistered`.
    ///
    /// The handler is read-only and its `context` must outlive the run, as in
    /// `setQueryHandler`; a pointer into a live workflow coroutine's stack returns
    /// `error.QueryContextOnStack`.
    ///
    ///     fn anyQuery(s: *State, name: []const u8, args: temporal.converter.EncodedValues) ![]const u8 {
    ///         _ = args;
    ///         return if (std.mem.eql(u8, name, "status")) s.status else error.UnknownQuery;
    ///     }
    ///     try ctx.setDynamicQueryHandler(state, State.anyQuery);
    pub fn setDynamicQueryHandler(ctx: *Context, context: anytype, comptime handler: anytype) SetQueryHandlerError!void {
        const C = @TypeOf(context);
        const Args = struct { []const u8, converter.EncodedValues };
        const Result = comptime dynamicResult("dynamic query handler", C, @TypeOf(handler));
        comptime checkHandler("dynamic query handler", Args, C, @TypeOf(handler), Result);
        const Erased = struct {
            fn call(p: *const anyopaque, out: Allocator, conv: *const converter.Context, name: []const u8, payloads: []const converter.Payload) anyerror!converter.Payload {
                const c: *const C = @ptrCast(@alignCast(p));
                const result = handler(c.*, name, converter.EncodedValues{ .payloads = payloads, .context = conv });
                const value = if (@typeInfo(@TypeOf(result)) == .error_union) try result else result;
                return converter.toPayloadWithContext(out, conv, value);
            }
        };
        const stored = try ctx.allocator().create(C);
        stored.* = context;
        return ctx.instance.setDynamicQueryHandler(.{ .context = @ptrCast(stored), .call = Erased.call }, contextAddress(context));
    }

    pub const SetSignalHandlerError = error{
        OutOfMemory,
        /// `context` points into a live workflow coroutine's stack (logged with the signal name).
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
    /// run, so allocate it with `ctx.allocator()` (or point to a global); a pointer into a live
    /// workflow coroutine's stack returns `error.SignalContextOnStack`. Decoded arguments live
    /// in `ctx.allocator()`, so the handler may keep them.
    ///
    ///     const state = try ctx.allocator().create(State);
    ///     try ctx.setSignalHandler(Approve, state, State.approve);
    pub fn setSignalHandler(ctx: *Context, comptime Signal: type, context: anytype, comptime handler: anytype) SetSignalHandlerError!void {
        return ctx.setSignalHandlerWithOptions(Signal, context, handler, .{});
    }

    /// `setSignalHandler` with options, such as `.unfinished_policy = .abandon` for a handler
    /// whose work may be cut short when the workflow ends (see `HandlerUnfinishedPolicy`), or
    /// a `.description` for the answer to `__temporal_workflow_metadata`.
    ///
    ///     try ctx.setSignalHandlerWithOptions(Ping, state, State.ping, .{ .unfinished_policy = .abandon });
    pub fn setSignalHandlerWithOptions(ctx: *Context, comptime Signal: type, context: anytype, comptime handler: anytype, options: HandlerOptions) SetSignalHandlerError!void {
        comptime checkHandler("signal handler for \"" ++ Signal.signal_name ++ "\"", Signal.Args, @TypeOf(context), @TypeOf(handler), void);
        return ctx.setSignalHandlerInner(Signal.signal_name, Signal.Args, context, handler, options);
    }

    /// `setSignalHandler` for a signal named at run time, such as one whose name includes a
    /// workflow ID: handles signals called `name`, whose arguments are the tuple type `Args`,
    /// with `handler(context, args...)`. `name` is copied into `ctx.allocator()`. Buffering,
    /// replacement, and errors work as in `setSignalHandler`.
    ///
    ///     const name = try std.fmt.allocPrint(ctx.allocator(), "unlock-{s}", .{holder_id});
    ///     try ctx.setSignalHandlerByName(name, struct { []const u8 }, state, State.unlock);
    pub fn setSignalHandlerByName(ctx: *Context, name: []const u8, comptime Args: type, context: anytype, comptime handler: anytype) SetSignalHandlerError!void {
        return ctx.setSignalHandlerByNameWithOptions(name, Args, context, handler, .{});
    }

    /// `setSignalHandlerByName` with options (see `setSignalHandlerWithOptions`).
    pub fn setSignalHandlerByNameWithOptions(ctx: *Context, name: []const u8, comptime Args: type, context: anytype, comptime handler: anytype, options: HandlerOptions) SetSignalHandlerError!void {
        comptime checkHandler("signal handler for a signal named at run time", Args, @TypeOf(context), @TypeOf(handler), void);
        return ctx.setSignalHandlerInner(try ctx.allocator().dupe(u8, name), Args, context, handler, options);
    }

    fn setSignalHandlerInner(ctx: *Context, name: []const u8, comptime Args: type, context: anytype, comptime handler: anytype, options: HandlerOptions) SetSignalHandlerError!void {
        const C = @TypeOf(context);
        const Erased = struct {
            fn decode(out: Allocator, conv: *const converter.Context, _: []const u8, payloads: []const converter.Payload) anyerror!*const anyopaque {
                const args = try out.create(Args);
                args.* = try converter.fromPayloadsWithContext(Args, out, conv, payloads);
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
        const description = try ctx.copyDescription(options.description);
        return ctx.instance.setSignalHandler(name, .{ .context = @ptrCast(stored), .decode = Erased.decode, .call = Erased.call, .unfinished_policy = options.unfinished_policy, .description = description }, contextAddress(context));
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
        return ctx.setDynamicSignalHandlerWithOptions(context, handler, .{});
    }

    /// `setDynamicSignalHandler` with options (see `setSignalHandlerWithOptions`).
    pub fn setDynamicSignalHandlerWithOptions(ctx: *Context, context: anytype, comptime handler: anytype, options: HandlerOptions) SetSignalHandlerError!void {
        const C = @TypeOf(context);
        const Args = struct { []const u8, converter.EncodedValues };
        comptime checkHandler("dynamic signal handler", Args, C, @TypeOf(handler), void);
        const Erased = struct {
            fn decode(out: Allocator, conv: *const converter.Context, name: []const u8, payloads: []const converter.Payload) anyerror!*const anyopaque {
                const args = try out.create(Args);
                args.* = .{ name, .{ .payloads = payloads, .context = conv } };
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
        return ctx.instance.setDynamicSignalHandler(.{ .context = @ptrCast(stored), .decode = Erased.decode, .call = Erased.call, .unfinished_policy = options.unfinished_policy }, contextAddress(context));
    }

    /// The channel of the signals named by `Signal` (a `SignalDef`), for workflow code that
    /// receives them where it needs them instead of in a handler (see `SignalChannel`).
    /// Signals of that name buffered before this call reach the channel in arrival order,
    /// once the calling coroutine blocks; later ones as they arrive.
    ///
    /// The channel takes the place of the signal's handler, the same way registering a
    /// handler replaces the one before it; `setSignalHandler` or `removeSignalHandler` for
    /// the name ends its deliveries, keeping the signals it took. Calling this again for the
    /// same signal, while its channel is still the handler, returns the same channel.
    ///
    ///     const approvals = try ctx.signalChannel(Approve);
    ///     const approver = try approvals.receive(); // []const u8
    pub fn signalChannel(ctx: *Context, comptime Signal: type) error{OutOfMemory}!*SignalChannel(Signal.Args) {
        return SignalChannel(Signal.Args).open(ctx, Signal.signal_name);
    }

    /// `signalChannel` for a signal named at run time, whose arguments are the tuple type
    /// `Args`. `name` is copied into `ctx.allocator()`. A call for the same name and `Args`,
    /// while that channel is still the handler, returns the same channel.
    ///
    ///     const unlocks = try ctx.signalChannelByName(name, struct { []const u8 });
    pub fn signalChannelByName(ctx: *Context, name: []const u8, comptime Args: type) error{OutOfMemory}!*SignalChannel(Args) {
        if (ctx.instance.signalChannelFor(name, SignalChannel(Args).decode)) |core| return @fieldParentPtr("core", core);
        return SignalChannel(Args).open(ctx, try ctx.allocator().dupe(u8, name));
    }

    /// Unregisters the handler or channel for the signals named by `Signal` (a `SignalDef`),
    /// if any. Later signals of that name go to the dynamic handler if one is set, and are
    /// otherwise buffered until a handler or channel is registered for them again. Handler
    /// coroutines already started run to completion, and a channel keeps the signals it
    /// already took, still readable.
    pub fn removeSignalHandler(ctx: *Context, comptime Signal: type) void {
        ctx.instance.removeSignalHandler(Signal.signal_name);
    }

    /// `removeSignalHandler` for a signal named at run time.
    pub fn removeSignalHandlerByName(ctx: *Context, name: []const u8) void {
        ctx.instance.removeSignalHandler(name);
    }

    /// Unregisters the dynamic signal handler, if any: later signals with no handler of their
    /// own are buffered until one is registered. Handler coroutines already started run to
    /// completion.
    pub fn removeDynamicSignalHandler(ctx: *Context) void {
        ctx.instance.removeDynamicSignalHandler();
    }

    /// The distinct names of the signals that nothing has taken: those buffered because no
    /// handler, channel, or dynamic handler was registered when they arrived (and none has
    /// been since), and those unread in a signal channel or on their way to one. In the
    /// order each name first arrived; allocated in `ctx.allocator()`. When the workflow
    /// finishes with any, it logs a warning counting them, since they are dropped.
    pub fn unhandledSignalNames(ctx: *Context) error{OutOfMemory}![]const []const u8 {
        return ctx.instance.unhandledSignalNames(ctx.allocator());
    }

    pub const SetUpdateHandlerError = error{
        OutOfMemory,
        /// `context` points into a live workflow coroutine's stack (logged with the update name).
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
    /// with no handler (nor a dynamic one, `setDynamicUpdateHandler`) once every coroutine has
    /// blocked are rejected, so register handlers before the workflow function first blocks. An update whose arguments don't decode as
    /// the handler's parameters is rejected.
    ///
    /// `context` must outlive the run, as for `setSignalHandler`; a pointer into a live workflow
    /// coroutine's stack returns `error.UpdateContextOnStack`. Decoded arguments live in
    /// `ctx.allocator()`.
    pub fn setUpdateHandler(ctx: *Context, comptime Update: type, context: anytype, comptime handler: anytype) SetUpdateHandlerError!void {
        return ctx.setUpdateHandlerWithOptions(Update, context, handler, null, .{});
    }

    /// `setUpdateHandler` with a validator: `validator(context, args...)`, returning `void`
    /// or `!void`, runs when the update is delivered, before it is accepted. Its error
    /// rejects the update with the error's name; the update then never runs and leaves no
    /// trace in history.
    ///
    /// The validator runs synchronously, outside the update's coroutine, like a query
    /// handler: it must only read workflow state. A call that would emit a command, start a
    /// coroutine, or block is refused as in a query handler (see `setQueryHandler`), and the
    /// update is rejected with a `ReadOnlyViolation` failure naming the call. It doesn't run
    /// on replay, where every update in history was accepted.
    ///
    ///     try ctx.setUpdateHandlerWithValidator(SetLimit, state, State.set, State.check);
    pub fn setUpdateHandlerWithValidator(ctx: *Context, comptime Update: type, context: anytype, comptime handler: anytype, comptime validator: anytype) SetUpdateHandlerError!void {
        return ctx.setUpdateHandlerWithOptions(Update, context, handler, validator, .{});
    }

    /// `setUpdateHandlerWithValidator` with options, such as `.unfinished_policy = .abandon`
    /// for a handler whose work may be cut short when the workflow ends (see
    /// `HandlerUnfinishedPolicy`), or a `.description` for the answer to
    /// `__temporal_workflow_metadata`. `validator` is `null` for an update without one.
    ///
    ///     try ctx.setUpdateHandlerWithOptions(SetLimit, state, State.set, null, .{ .unfinished_policy = .abandon });
    pub fn setUpdateHandlerWithOptions(ctx: *Context, comptime Update: type, context: anytype, comptime handler: anytype, comptime validator: anytype, options: HandlerOptions) SetUpdateHandlerError!void {
        const C = @TypeOf(context);
        const what = "update handler for \"" ++ Update.update_name ++ "\"";
        comptime checkHandler(what, Update.Args, C, @TypeOf(handler), Update.Result);
        const has_validator = @TypeOf(validator) != @TypeOf(null);
        if (has_validator) comptime checkHandler("update validator for \"" ++ Update.update_name ++ "\"", Update.Args, C, @TypeOf(validator), void);
        const Erased = struct {
            fn decode(out: Allocator, conv: *const converter.Context, _: []const u8, payloads: []const converter.Payload) anyerror!*const anyopaque {
                const args = try out.create(Update.Args);
                args.* = try converter.fromPayloadsWithContext(Update.Args, out, conv, payloads);
                return @ptrCast(args);
            }
            fn validate(p: *const anyopaque, a: *const anyopaque) anyerror!void {
                const c: *const C = @ptrCast(@alignCast(p));
                const args: *const Update.Args = @ptrCast(@alignCast(a));
                const result = @call(.auto, validator, .{c.*} ++ args.*);
                if (@typeInfo(@TypeOf(result)) == .error_union) try result;
            }
            fn call(p: *const anyopaque, out: Allocator, conv: *const converter.Context, a: *const anyopaque) anyerror!converter.Payload {
                const c: *const C = @ptrCast(@alignCast(p));
                const args: *const Update.Args = @ptrCast(@alignCast(a));
                const result = @call(.auto, handler, .{c.*} ++ args.*);
                const value = if (@typeInfo(@TypeOf(result)) == .error_union) try result else result;
                return converter.toPayloadWithContext(out, conv, value);
            }
        };
        const stored = try ctx.allocator().create(C);
        stored.* = context;
        const description = try ctx.copyDescription(options.description);
        return ctx.instance.setUpdateHandler(Update.update_name, .{
            .context = @ptrCast(stored),
            .decode = Erased.decode,
            .validate = if (has_validator) Erased.validate else null,
            .call = Erased.call,
            .unfinished_policy = options.unfinished_policy,
            .description = description,
        }, contextAddress(context));
    }

    /// Handles every update that has no handler registered for its name with
    /// `handler(context, name, args)`, where `name` is the update's name and `args` its
    /// undecoded arguments (`converter.EncodedValues`), decoded with
    /// `args.get(T, ctx.allocator(), index)`. It returns the update's result (any type the
    /// converter encodes, or `void`) or an error union of it. Registering again replaces the
    /// dynamic handler for the updates delivered afterwards.
    ///
    /// Updates buffered before this call, of any name, are accepted here in arrival order. A
    /// handler registered for an update's name later still takes that name's later updates.
    /// Each accepted update runs the handler in a coroutine of its own, and
    /// `ctx.currentUpdate()` names it there; results, errors, and `context` work as in
    /// `setUpdateHandler`. An argument that doesn't decode is the handler's error to return,
    /// which rejects the update after it was accepted; reject it before acceptance with
    /// `setDynamicUpdateHandlerWithValidator`.
    ///
    ///     fn anyUpdate(s: *State, name: []const u8, args: temporal.converter.EncodedValues) !i64 {
    ///         const amount = try args.get(i64, s.ctx.allocator(), 0);
    ///         return s.record(name, amount);
    ///     }
    ///     try ctx.setDynamicUpdateHandler(state, State.anyUpdate);
    pub fn setDynamicUpdateHandler(ctx: *Context, context: anytype, comptime handler: anytype) SetUpdateHandlerError!void {
        return ctx.setDynamicUpdateHandlerWithOptions(context, handler, null, .{});
    }

    /// `setDynamicUpdateHandler` with a validator: `validator(context, name, args)`,
    /// returning `void` or `!void`, runs when the update is delivered, before it is accepted,
    /// read-only as in `setUpdateHandlerWithValidator`. Its error rejects the update.
    pub fn setDynamicUpdateHandlerWithValidator(ctx: *Context, context: anytype, comptime handler: anytype, comptime validator: anytype) SetUpdateHandlerError!void {
        return ctx.setDynamicUpdateHandlerWithOptions(context, handler, validator, .{});
    }

    /// `setDynamicUpdateHandlerWithValidator` with options (see
    /// `setUpdateHandlerWithOptions`); `validator` is `null` for none.
    pub fn setDynamicUpdateHandlerWithOptions(ctx: *Context, context: anytype, comptime handler: anytype, comptime validator: anytype, options: HandlerOptions) SetUpdateHandlerError!void {
        const C = @TypeOf(context);
        const Args = struct { []const u8, converter.EncodedValues };
        const Result = comptime dynamicResult("dynamic update handler", C, @TypeOf(handler));
        comptime checkHandler("dynamic update handler", Args, C, @TypeOf(handler), Result);
        const has_validator = @TypeOf(validator) != @TypeOf(null);
        if (has_validator) comptime checkHandler("dynamic update validator", Args, C, @TypeOf(validator), void);
        const Erased = struct {
            fn decode(out: Allocator, conv: *const converter.Context, name: []const u8, payloads: []const converter.Payload) anyerror!*const anyopaque {
                const args = try out.create(Args);
                args.* = .{ name, .{ .payloads = payloads, .context = conv } };
                return @ptrCast(args);
            }
            fn validate(p: *const anyopaque, a: *const anyopaque) anyerror!void {
                const c: *const C = @ptrCast(@alignCast(p));
                const args: *const Args = @ptrCast(@alignCast(a));
                const result = @call(.auto, validator, .{c.*} ++ args.*);
                if (@typeInfo(@TypeOf(result)) == .error_union) try result;
            }
            fn call(p: *const anyopaque, out: Allocator, conv: *const converter.Context, a: *const anyopaque) anyerror!converter.Payload {
                const c: *const C = @ptrCast(@alignCast(p));
                const args: *const Args = @ptrCast(@alignCast(a));
                const result = @call(.auto, handler, .{c.*} ++ args.*);
                const value = if (@typeInfo(@TypeOf(result)) == .error_union) try result else result;
                return converter.toPayloadWithContext(out, conv, value);
            }
        };
        const stored = try ctx.allocator().create(C);
        stored.* = context;
        return ctx.instance.setDynamicUpdateHandler(.{
            .context = @ptrCast(stored),
            .decode = Erased.decode,
            .validate = if (has_validator) Erased.validate else null,
            .call = Erased.call,
            .unfinished_policy = options.unfinished_policy,
        }, contextAddress(context));
    }

    /// Whether no signal or update handler coroutine is running: every delivered signal's
    /// and accepted update's handler has returned. Wait for it before returning so handlers
    /// aren't cut short (and the run doesn't log a warning naming them, see
    /// `HandlerUnfinishedPolicy`):
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
        return converter.fromPayloadWithContext(T, ctx.allocator(), ctx.converterContext(), p) catch |err| switch (err) {
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
        ///
        /// Any number of coroutines may wait on the same future; all of them return once it
        /// resolves, in the order they began waiting. Canceling one of them requests the operation's
        /// cancellation as described above, which the others then see in its outcome.
        pub fn get(f: Self) ActivityError!T {
            const state = try f.state;
            try f.ctx.instance.awaitActivity(state);
            switch (state.outcome.?) {
                .completed => |maybe| {
                    if (T == void) return;
                    const p = maybe orelse return error.InvalidPayload;
                    return converter.fromPayloadWithContext(T, f.ctx.allocator(), &state.context, p);
                },
                .failed, .cancelled => |fl| {
                    f.ctx.instance.setLastFailure(fl);
                    return error.ActivityFailed;
                },
            }
        }

        /// The converter context of the activity's result and failure: the run's (see
        /// `Context.converterContext`) with the activity type and the scheduled activity ID.
        /// Decode failure details with it: `details.withContext(future.converterContext())`.
        /// The run's own context when scheduling was refused, and for a side effect. Valid
        /// for the run; workflow code only.
        pub fn converterContext(f: Self) *const converter.Context {
            const state = f.state catch return f.ctx.converterContext();
            return &state.context;
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
/// `ChildWorkflowInvalidVersioningOverride`: experimental; the server refused to start the
/// child because of `ChildWorkflowOptions.versioning_override`; `failure` describes it too.
/// `InvalidOptions`: the options were rejected before starting (the worker logs why).
pub const ChildWorkflowError = error{ ChildWorkflowFailed, ChildWorkflowAlreadyStarted, ChildWorkflowInvalidVersioningOverride, Canceled, OutOfMemory, InvalidOptions, InvalidPayload, UnknownEncoding, WrongArgumentCount };

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
        /// `error.ChildWorkflowAlreadyStarted` if its ID is taken,
        /// `error.ChildWorkflowInvalidVersioningOverride` if the server refused its
        /// `versioning_override`, and `error.ChildWorkflowFailed` if it could not start for
        /// another reason or was canceled before it started.
        /// Canceling the calling coroutine cancels the child as `get` does.
        ///
        /// Any number of coroutines may wait on the same future; all of them return once it
        /// resolves, in the order they began waiting. Canceling one of them requests the operation's
        /// cancellation as described above, which the others then see in its outcome.
        pub fn started(f: Self) ChildWorkflowError!WorkflowExecution {
            const state = try f.state;
            try f.ctx.instance.awaitChild(state, .started);
            if (state.run_id) |run_id| return .{ .namespace = f.ctx.instance.namespace, .workflow_id = state.workflow_id, .run_id = run_id };
            return f.fail(state);
        }

        /// Waits for the child to close and returns its result. On `error.ChildWorkflowFailed`
        /// (the child failed, timed out, was terminated or canceled, or never started), read
        /// the failure with `failure`; a start refused for its ID or its versioning override
        /// returns `error.ChildWorkflowAlreadyStarted` or
        /// `error.ChildWorkflowInvalidVersioningOverride` as `started` does. If the calling coroutine is canceled while waiting (the
        /// workflow is canceled, or an enclosing future, `Select`, or `Group` is), the child's
        /// cancellation is requested and this returns `error.Canceled`: at once for
        /// `try_cancel` and `abandon`, once the server has recorded the request for
        /// `wait_cancellation_requested`, and once the child has closed for
        /// `wait_cancellation_completed` (see `ChildWorkflowCancellationType`). If the child
        /// completes or fails first, this returns its result or error, and the next
        /// cancelable wait returns `error.Canceled`.
        ///
        /// Any number of coroutines may wait on the same future; all of them return once it
        /// resolves, in the order they began waiting. Canceling one of them requests the operation's
        /// cancellation as described above, which the others then see in its outcome.
        pub fn get(f: Self) ChildWorkflowError!T {
            const state = try f.state;
            try f.ctx.instance.awaitChild(state, .result);
            switch (state.outcome.?) {
                .completed => |maybe| {
                    if (T == void) return;
                    const p = maybe orelse return error.InvalidPayload;
                    return converter.fromPayloadWithContext(T, f.ctx.allocator(), &state.context, p);
                },
                .failed, .cancelled => return f.fail(state),
            }
        }

        fn fail(f: Self, state: *const Instance.ChildState) ChildWorkflowError {
            f.ctx.instance.setLastFailure(f.failure());
            return switch (state.start_refusal) {
                .none => error.ChildWorkflowFailed,
                .already_started => error.ChildWorkflowAlreadyStarted,
                .invalid_versioning_override => error.ChildWorkflowInvalidVersioningOverride,
            };
        }

        /// The converter context of the child's result, failure and signals: the namespace,
        /// the child's workflow ID and type, and the run's propagated values. Decode failure
        /// details with it: `details.withContext(future.converterContext())`. The run's own
        /// context when the start was refused. Valid for the run; workflow code only.
        pub fn converterContext(f: Self) *const converter.Context {
            const state = f.state catch return f.ctx.converterContext();
            return &state.context;
        }

        /// The child's failure, after `get` or `started` returned `error.ChildWorkflowFailed`,
        /// `error.ChildWorkflowAlreadyStarted` or `error.ChildWorkflowInvalidVersioningOverride`.
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

        /// Requests cancellation of the child, as its `cancellation_type` says; `get` then
        /// returns `error.ChildWorkflowFailed` with a canceled failure once the type reports
        /// the cancellation. Nothing else cancels a child no coroutine is waiting on (see
        /// `ChildWorkflowOptions`).
        pub fn cancel(f: Self) void {
            if (f.state) |s| f.ctx.instance.cancelChild(s) else |_| {}
        }

        /// Sends `Signal` (a `SignalDef`) with `args` to the child and returns a future for its
        /// delivery. Waits for the child to start first (see `started`, whose errors this
        /// returns); the signal then goes to the child's run, and only to a child of this
        /// workflow.
        pub fn signal(f: Self, comptime Signal: type, args: Signal.Args) ChildWorkflowError!SignalExternalFuture {
            _ = try f.started();
            return f.ctx.instance.signalChildWorkflow(try f.state, Signal.signal_name, args);
        }
    };
}

/// `NexusOperationFailed`: the operation failed, timed out, or was canceled, or it failed to
/// start; read why with the future's `failure`, a Nexus operation failure (`failure_info` is
/// `nexus_operation_execution_failure_info`, naming the endpoint, service and operation) whose
/// cause is the handler's failure, a timeout failure, or a canceled failure.
/// `Canceled`: the calling coroutine was canceled while waiting, and the operation then
/// resolved as canceled (see `NexusOperationCancellationType`).
/// `InvalidOptions`: the options were rejected before scheduling (the worker logs why);
/// nothing was sent.
/// `InvalidPayload`, `UnknownEncoding`, `WrongArgumentCount`: the result doesn't decode as
/// the output type.
pub const NexusOperationError = error{ NexusOperationFailed, Canceled, OutOfMemory, InvalidOptions, InvalidPayload, UnknownEncoding, WrongArgumentCount };

/// The pending start and result of a Nexus operation, from `Context.executeNexusOperation`.
/// `NexusOperationFuture(converter.Payload)` returns the result still encoded.
pub fn NexusOperationFuture(comptime T: type) type {
    return struct {
        const Self = @This();

        ctx: *Context,
        /// The validation error, if scheduling was rejected; `started` and `get` return it.
        state: Instance.ScheduleError!*Instance.NexusOperationState,

        /// Waits until the handler has started the operation and returns its operation token,
        /// or null when the operation's result arrived with its start (its result is then
        /// ready): it completed synchronously, or its completion was recorded before its
        /// start. The token is arena memory of the run. Returns `error.NexusOperationFailed` if the
        /// operation failed, timed out, or was canceled before it started. Canceling the
        /// calling coroutine cancels the operation as `get` does.
        ///
        /// Any number of coroutines may wait on the same future; all of them return once it
        /// resolves, in the order they began waiting. Canceling one of them requests the operation's
        /// cancellation as described above, which the others then see in its outcome.
        pub fn started(f: Self) NexusOperationError!?[]const u8 {
            const state = try f.state;
            try f.ctx.instance.awaitNexusOperation(state, .started);
            if (state.started) return state.token;
            return f.fail();
        }

        /// Waits for the operation to end and returns its output, decoded into
        /// `ctx.allocator()`. On `error.NexusOperationFailed` read the failure with `failure`.
        /// If the calling coroutine is canceled while waiting (the workflow is canceled, or an
        /// enclosing future, `Select`, or `Group` is), the operation's cancellation is
        /// requested and this waits for the core to resolve it, which it does as the
        /// cancellation type says: it returns `error.Canceled` once the operation resolved as
        /// canceled. If it completed or failed instead, this returns its result or error, and
        /// the coroutine's next cancelable wait returns `error.Canceled`. Eviction ends the
        /// wait with `error.Canceled`.
        ///
        /// Any number of coroutines may wait on the same future; all of them return once it
        /// resolves, in the order they began waiting. Canceling one of them requests the operation's
        /// cancellation as described above, which the others then see in its outcome.
        pub fn get(f: Self) NexusOperationError!T {
            const state = try f.state;
            try f.ctx.instance.awaitNexusOperation(state, .result);
            switch (state.outcome.?) {
                .completed => |p| {
                    if (T == void) return;
                    return converter.fromPayloadWithContext(T, f.ctx.allocator(), &state.context, p);
                },
                .failed, .canceled, .timed_out => return f.fail(),
            }
        }

        fn fail(f: Self) NexusOperationError {
            f.ctx.instance.setLastFailure(f.failure());
            return error.NexusOperationFailed;
        }

        /// Why the operation failed, timed out, or was canceled, after `get` or `started`
        /// returned `error.NexusOperationFailed` or `error.Canceled`; null before it ended or
        /// when it completed. Arena memory of the run.
        pub fn failure(f: Self) ?*const failure_mod.Failure {
            const state = f.state catch return null;
            const outcome = state.outcome orelse return null;
            return switch (outcome) {
                .completed => null,
                .failed, .canceled, .timed_out => |fl| fl,
            };
        }

        /// Whether the operation has ended (or failed to start), so `get` won't block.
        pub fn isReady(f: Self) bool {
            const state = f.state catch return true;
            return state.outcome != null;
        }

        /// Experimental: the converter context of the operation's input, result and failure:
        /// the namespace, `Context.propagated()` and the operation's names in `nexus`, as the
        /// handler's worker converts them. Decode failure details with it:
        /// `details.withContext(future.converterContext())`. The run's own context when
        /// scheduling was refused. Valid for the run; workflow code only.
        pub fn converterContext(f: Self) *const converter.Context {
            const state = f.state catch return f.ctx.converterContext();
            return &state.context;
        }

        /// Requests cancellation of the operation, as its `cancellation_type` says; `get`
        /// then returns `error.NexusOperationFailed` with the cancellation as the failure once
        /// the core resolves it (or the operation's own result, if it ended first). Does
        /// nothing once the operation has ended or a cancellation was requested.
        pub fn cancel(f: Self) void {
            if (f.state) |s| f.ctx.instance.cancelNexusOperation(s) else |_| {}
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
    ///
    /// Any number of coroutines may wait on the same future; all of them return once it
    /// resolves, in the order they began waiting. Canceling one of them requests the operation's
    /// cancellation as described above, which the others then see in its outcome.
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

/// Experimental: how `Context.signalWithStartWorkflow` starts and signals the target. Only
/// read during the call; strings are copied.
/// A duration longer than protobuf can carry (about 10,000 years), such as `Duration.max`, is
/// sent as the longest one it can; a negative one is sent as it is.
pub const SignalWithStartOptions = struct {
    /// The target's workflow ID. Required: empty makes `get` return `error.InvalidOptions`
    /// (logged).
    id: []const u8,
    /// Empty: this run's task queue.
    task_queue: []const u8 = "",
    /// Limits the target's whole chain of runs, as in `client.StartWorkflowOptions`.
    execution_timeout: ?Duration = null,
    /// Limits one run of the target.
    run_timeout: ?Duration = null,
    /// Limits one workflow task of the target.
    task_timeout: ?Duration = null,
    /// Delay before the target's first workflow task. Not with `cron_schedule`: setting both
    /// is `error.InvalidOptions` (logged).
    start_delay: ?Duration = null,
    /// Runs the target on a cron schedule, such as "0 * * * *".
    cron_schedule: []const u8 = "",
    /// Whether a closed run's ID may be used again. `.unspecified` uses the server's default,
    /// `.allow_duplicate`. A closed run the policy keeps from being reused makes `get` return
    /// `error.WorkflowExecutionAlreadyStarted`.
    id_reuse_policy: enum { unspecified, allow_duplicate, allow_duplicate_failed_only, reject_duplicate } = .unspecified,
    /// `.unspecified` and `.use_existing` signal a running target. `.terminate_existing`
    /// replaces it. `.fail` is `error.InvalidOptions` (logged), because the server refuses it
    /// for signal-with-start.
    id_conflict_policy: enum { unspecified, fail, use_existing, terminate_existing } = .unspecified,
    /// How the server retries the target when a run fails or times out. Null retries none.
    retry_policy: ?RetryPolicy = null,
    /// Non-indexed values recorded with the target, built with `converter.toFields`, as for
    /// `ChildWorkflowOptions.memo`.
    memo: []const converter.Field = &.{},
    /// The target's search attributes, built with `search_attributes.Key.set`; unsets are
    /// ignored. A value that can't be indexed is `error.InvalidOptions` (logged).
    search_attributes: []const search_attributes.Update = &.{},
};

/// Experimental: what a signal-with-start did. Memory comes from the run's arena
/// (`ctx.allocator()`).
pub const SignalWithStartResult = struct {
    workflow_id: []const u8,
    run_id: []const u8,
    /// True when this call started the run; false when it only signaled a running one.
    started: bool,
};

/// Experimental: errors of `SignalWithStartFuture.get`.
///
/// `WorkflowExecutionAlreadyStarted`: the server refused because the workflow ID is taken in
/// a way `id_reuse_policy` forbids, for example a closed run under `.reject_duplicate`.
/// `SignalWithStartFailed`: any other refusal or failure: the operation failed or timed out,
/// or the server rejected the request. Read the failure with the future's `failure` after
/// either.
/// `Canceled`: the coroutine waiting in `get` was canceled and the operation was canceled, or
/// the run is being evicted.
/// `InvalidOptions`: the options were rejected before anything was sent (the worker logs
/// why).
/// `InvalidPayload`: the server's response doesn't decode.
pub const SignalWithStartError = error{
    WorkflowExecutionAlreadyStarted,
    SignalWithStartFailed,
    Canceled,
    OutOfMemory,
    InvalidOptions,
    InvalidPayload,
};

/// Experimental: the pending result of `Context.signalWithStartWorkflow` or
/// `signalWithStartWorkflowByName`.
pub const SignalWithStartFuture = struct {
    ctx: *Context,
    /// The target's ID, copied into the run's arena.
    workflow_id: []const u8,
    /// The validation error, if the call was rejected before anything was sent; `get`
    /// returns it.
    state: Instance.ScheduleError!*Instance.NexusOperationState,

    /// Experimental: waits for the server's answer. If the calling coroutine is canceled
    /// while waiting (the workflow is canceled, or an enclosing future, `Select`, or `Group`
    /// is), the operation's cancellation is requested and this waits for the core to resolve
    /// it: it returns `error.Canceled` once the operation resolved as canceled. If the server
    /// answered first, this returns that answer, and the coroutine's next cancelable wait
    /// returns `error.Canceled`. Eviction ends the wait with `error.Canceled`.
    pub fn get(f: SignalWithStartFuture) SignalWithStartError!SignalWithStartResult {
        const state = try f.state;
        try f.ctx.instance.awaitNexusOperation(state, .result);
        switch (state.outcome.?) {
            .completed => |p| {
                const response = try system_nexus.decodeSignalWithStartResponse(f.ctx.allocator(), p);
                return .{ .workflow_id = f.workflow_id, .run_id = response.run_id, .started = response.started };
            },
            .canceled => |fl| {
                f.ctx.instance.setLastFailure(fl);
                return error.Canceled;
            },
            .failed, .timed_out => |fl| {
                f.ctx.instance.setLastFailure(fl);
                return if (alreadyStarted(fl)) error.WorkflowExecutionAlreadyStarted else error.SignalWithStartFailed;
            },
        }
    }

    /// Experimental: the failure after `get` returned `error.WorkflowExecutionAlreadyStarted`,
    /// `error.SignalWithStartFailed` or `error.Canceled`; null otherwise. Arena memory of the
    /// run.
    pub fn failure(f: SignalWithStartFuture) ?*const failure_mod.Failure {
        const state = f.state catch return null;
        const outcome = state.outcome orelse return null;
        return switch (outcome) {
            .completed => null,
            .failed, .canceled, .timed_out => |fl| fl,
        };
    }

    /// Experimental: whether `get` won't block.
    pub fn isReady(f: SignalWithStartFuture) bool {
        const state = f.state catch return true;
        return state.outcome != null;
    }
};

/// The start of the message of the server failure that refuses a taken workflow ID. The
/// system Nexus endpoint passes the server's error on as the operation failure's cause, a
/// server failure whose message is the gRPC error's type and text, as the dev server
/// (CLI 1.10.1, server 1.32.2) sends it: "WorkflowExecutionAlreadyStarted: Workflow execution already
/// finished. WorkflowId: <id>, RunId: <run>. Workflow Id reuse policy: reject duplicate
/// workflow Id."
const already_started_prefix = "WorkflowExecutionAlreadyStarted: ";

/// The start of the same message without the error type's name, as a Nexus handler failure
/// of type BAD_REQUEST carries it.
const handler_already_started_prefix = "Workflow execution already";

/// Whether `f`, or a failure in its cause chain, is the server's refusal of a taken
/// workflow ID: a server failure whose message starts with `already_started_prefix`, or a
/// BAD_REQUEST Nexus handler failure whose message starts with
/// `handler_already_started_prefix`. It reads the failure history recorded, so it decides the same on replay.
fn alreadyStarted(f: *const failure_mod.Failure) bool {
    var next: ?*const failure_mod.Failure = f;
    while (next) |cur| : (next = cur.cause) {
        const info = cur.failure_info orelse continue;
        switch (info) {
            .server_failure_info => if (std.mem.startsWith(u8, cur.message, already_started_prefix)) return true,
            // The form the server's Nexus error conversion gives AlreadyExists (common/nexus
            // ConvertGRPCError): a bad-request handler failure carrying the gRPC message.
            .nexus_handler_failure_info => |h| if (std.mem.eql(u8, h.type, "BAD_REQUEST") and std.mem.startsWith(u8, cur.message, handler_already_started_prefix)) return true,
            else => {},
        }
    }
    return false;
}

/// A variable scoped to one workflow run, for libraries and interceptors that keep per-run
/// state without threading it through the workflow's code. Declare it once, usually as a
/// container-level constant, and build it with a struct literal:
///
///     const depth: temporal.workflow.LocalVar(u32) = .{ .name = "audit.depth", .default = 0 };
///
///     try depth.set(ctx, depth.get(ctx) + 1);
///
/// Any code of the run that holds its `*Context` reaches the same value: the workflow
/// function, the coroutines it starts, its signal and update handlers, and the run's
/// workflow interceptors (with the `Context` `interceptor.Worker.interceptWorkflow` got).
/// Each run has its own values, which start at `default` and are freed with the run, when it
/// completes or is evicted; a run rebuilt from history gets them back by replaying the code
/// that set them.
///
/// A variable is its `name` and its `T`: copies are the same variable, and so are two
/// declarations with the same name and type, so a library should prefix its names
/// ("mylib.depth"). The same name with another `T` is another variable.
///
/// The value is immutable and may be shared by every run and thread; `get` and `set` are
/// workflow code, on the run's worker thread only.
pub fn LocalVar(comptime T: type) type {
    return struct {
        /// Identifies the variable within a run, together with `T`. Copied into the run's
        /// memory the first time the run sets it.
        name: []const u8,
        /// What `get` returns in a run that has not set the variable.
        default: T,

        pub const Value = T;

        /// Identifies `T` within a run's table by its address, which no other type's
        /// `LocalVar` shares, even one with the same `@typeName`. Never written.
        var type_id: u8 = 0;

        /// `ReadOnly`: the caller is a query handler, an update validator, or a side-effect
        /// function (`func` of `Context.sideEffect`, `func` or `eql` of
        /// `Context.mutableSideEffect`), none of which runs again when the run is replayed.
        /// The value is unchanged. In a query handler or validator the query fails, or the
        /// update is rejected, with a `ReadOnlyViolation` failure naming `LocalVar.set`; in a
        /// side-effect function a warning naming the variable is logged.
        /// `OutOfMemory`: the run's first set of the variable could not allocate it; it stays
        /// unset.
        pub const SetError = error{ OutOfMemory, ReadOnly };

        /// The value the run last set, or `default`. Allowed everywhere, read-only code
        /// included.
        pub fn get(v: @This(), ctx: *const Context) T {
            if (@sizeOf(T) == 0) return v.default;
            const box = ctx.instance.localValue(v.name, &type_id) orelse return v.default;
            return @as(*const T, @ptrCast(@alignCast(box))).*;
        }

        /// Sets the run's value. `value` is copied shallowly into the run's memory; memory it
        /// points to (a string's bytes) is borrowed and must outlive the run or come from
        /// `ctx.allocator()`. The first set of a variable in a run allocates it; later sets
        /// overwrite it in place.
        pub fn set(v: @This(), ctx: *Context, value: T) SetError!void {
            const inst = ctx.instance;
            if (inst.refuseLocalSet()) {
                if (!inst.wio.refuse("LocalVar.set"))
                    inst.logger.warn("LocalVar.set in a side-effect function is refused: side-effect functions don't run on replay, so the value would differ there; set it from the code that calls sideEffect or mutableSideEffect", .{ .LocalVar = v.name });
                return error.ReadOnly;
            }
            if (@sizeOf(T) == 0) return;
            if (inst.localValue(v.name, &type_id)) |box| {
                @as(*T, @ptrCast(@alignCast(box))).* = value;
                return;
            }
            const a = inst.arena();
            const box = try a.create(T);
            errdefer a.destroy(box);
            box.* = value;
            try inst.addLocalValue(v.name, &type_id, @ptrCast(box));
        }
    };
}

/// The signals of one name, from `Context.signalChannel` or `signalChannelByName`, read in
/// arrival order. `Args` is the signal's argument tuple; each receive returns a `Value`: the
/// argument itself for a signal with one argument, else the whole tuple. Allocated in
/// `ctx.allocator()`; the decoded arguments live there too.
///
/// The channel is the signal's handler: each signal of its name runs the inbound interceptors
/// and decodes its arguments in a handler coroutine of its own, as a handler would, and is
/// appended to the channel. A signal whose arguments don't decode is dropped with a warning.
/// Use it from workflow coroutines only.
pub fn SignalChannel(comptime Args: type) type {
    const field_types = @typeInfo(Args).@"struct".field_types;
    return struct {
        const Self = @This();

        core: Instance.SignalChannel,

        /// What a receive returns: the signal's argument for a signal with one, else `Args`.
        pub const Value = if (field_types.len == 1) field_types[0] else Args;

        /// Waits for the next signal and returns its arguments. If the calling coroutine is
        /// canceled while it waits (the workflow is canceled, or an enclosing future, `Select`,
        /// or `Group` is), returns `error.Canceled` and leaves the channel's signals unread,
        /// so it can be a `std.Io.Select` case. A receive woken by a signal runs before the
        /// coroutines the same workflow task's other events woke, so a signal that arrives with
        /// a raced timer wins the race.
        ///
        /// A receive that returned has taken its signal from the channel. A Select returns
        /// one finished case per `await`; when more than one has finished (receives on several
        /// channels, or a Select awaited only after its cases had time to finish),
        /// `cancelDiscard` drops the others, and a received signal with them. To keep every
        /// signal, cancel with `cancel` until it returns null and handle each result it
        /// returns; a receive it cancels returns `error.Canceled` and took nothing:
        ///
        ///     var select: std.Io.Select(union(enum) { approval: error{Canceled}!Ch.Value, timeout: std.Io.Cancelable!void }) = .init(io, &buffer);
        ///     select.async(.approval, Ch.receive, .{approvals});
        ///     select.async(.timeout, std.Io.sleep, .{ io, .fromSeconds(60), .awake });
        ///     var next: ?@TypeOf(buffer[0]) = try select.await();
        ///     while (next) |result| : (next = select.cancel()) handle(result);
        pub fn receive(ch: *Self) error{Canceled}!Value {
            return value(try ch.core.receive());
        }

        /// The next signal's arguments without waiting, or null if no signal is unread.
        pub fn tryReceive(ch: *Self) ?Value {
            if (ch.core.inst.wio.refuse("SignalChannel.tryReceive")) return null;
            return value(ch.core.take() orelse return null);
        }

        /// How many signals are unread.
        pub fn len(ch: *const Self) usize {
            return ch.core.len();
        }

        fn value(p: *const anyopaque) Value {
            const args: *const Args = @ptrCast(@alignCast(p));
            return if (field_types.len == 1) args[0] else args.*;
        }

        fn decode(out: Allocator, conv: *const converter.Context, _: []const u8, payloads: []const converter.Payload) anyerror!*const anyopaque {
            const args = try out.create(Args);
            args.* = try converter.fromPayloadsWithContext(Args, out, conv, payloads);
            return @ptrCast(args);
        }

        fn open(ctx: *Context, name: []const u8) error{OutOfMemory}!*Self {
            const inst = ctx.instance;
            if (inst.signalChannelFor(name, decode)) |core| return @fieldParentPtr("core", core);
            const ch = try ctx.allocator().create(Self);
            try inst.createSignalChannel(name, &ch.core, decode);
            return ch;
        }
    };
}

/// The version `Context.getVersion` returns for a run that passed the change's point before
/// the change existed.
pub const default_version: i32 = -1;

/// What a worker registers: a name, a type-erased call that decodes arguments, runs the
/// workflow, and encodes its result, and the workflow's versioning behavior.
pub const Registered = struct {
    name: []const u8,
    invoke: *const fn (ctx: *Context, args: []const converter.Payload) anyerror!converter.Payload,
    versioning_behavior: VersioningBehavior = .unspecified,
    /// See `DefOptions.task_failure_errors`.
    task_failure_errors: []const anyerror = &.{},
    /// See `DefOptions.nondeterminism_fails_workflow`.
    nondeterminism_fails_workflow: bool = false,
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
    /// Errors that fail the workflow task instead of the workflow when the workflow
    /// function, a signal handler, or an update handler returns them, in addition to the
    /// worker's `worker.Options.workflow_task_failure_errors`: an error either lists fails the
    /// task. The task failure names the error; the workflow stays running, and the server
    /// retries the task until code that no longer returns the error completes it. An update
    /// handler's listed error doesn't reject or fail the update; the retried task runs the
    /// handler again. Listing `error.Canceled` doesn't stop a cancellation: the workflow
    /// function or a signal handler that returns it cancels the workflow, listed or not. An
    /// update handler that returns a listed `error.Canceled` fails the task, where unlisted it
    /// rejects the update. Listing `error.ContinueAsNew` doesn't stop a continue-as-new the
    /// workflow function requested; only its misuse (returned without
    /// `Context.continueAsNew`, or by a signal handler) then fails the task instead of the
    /// workflow. `error.OutOfMemory` from the workflow function or a signal handler fails the
    /// task whether listed or not; from an update handler it rejects the update unless listed.
    ///
    ///     pub const Charge = temporal.workflow.DefWithOptions("Charge", charge, .{ .task_failure_errors = &.{error.ConfigMissing} });
    task_failure_errors: []const anyerror = &.{},
    /// Fail the workflow, instead of its workflow task, when a run's code no longer matches
    /// its history (nondeterminism). Without it, or the worker's
    /// `worker.Options.nondeterminism_fails_workflow`, the task fails and is retried until
    /// the code matches. Not available for a dynamic workflow, which has no type name the
    /// worker can declare; use the worker option.
    nondeterminism_fails_workflow: bool = false,
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
///   name a validator method: `.{ UpdateDef, .validatorMethod }`. A signal or update entry
///   may end with `HandlerOptions`, such as `.{ Add, .{ .unfinished_policy = .abandon } }` or
///   `.{ Reset, .checkReset, .{ .description = "Sets the counter" } }`, and a query entry
///   with `QueryHandlerOptions`, such as `.{ Current, .{ .description = "The counter" } }`.
///   A query entry for `__enhanced_stack_trace` or `__temporal_workflow_metadata`, which the
///   worker answers itself, is a compile error.
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
            const args = try converter.fromPayloadsWithContext(Args, ctx.allocator(), ctx.converterContext(), payloads);
            const result = try @call(.auto, definition, .{ctx} ++ args);
            return converter.toPayloadWithContext(ctx.allocator(), ctx.instance.resultConverterContext(), result);
        }

        pub const versioning_behavior = options.versioning_behavior;
        pub const registered: Registered = .{
            .name = name,
            .invoke = invoke,
            .versioning_behavior = options.versioning_behavior,
            .task_failure_errors = options.task_failure_errors,
            .nondeterminism_fails_workflow = options.nondeterminism_fails_workflow,
        };
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
            const args = try converter.fromPayloadsWithContext(Args, ctx.allocator(), ctx.converterContext(), payloads);
            const state = ctx.instance.run_state orelse return error.StructWorkflowNotInitialized;
            try ctx.instance.releaseHandlers();
            const w: *W = @ptrCast(@alignCast(state));
            const result = try @call(.auto, W.run, .{ w, ctx } ++ args);
            return converter.toPayloadWithContext(ctx.allocator(), ctx.instance.resultConverterContext(), result);
        }

        fn initRun(ctx: *Context) anyerror!*anyopaque {
            const w = try ctx.allocator().create(W);
            w.* = .{};
            inline for (comptime entryNames(W, "queries")) |entry| {
                const e = comptime handlerEntry("queries", @field(W.queries, entry)).?;
                try ctx.setQueryHandlerWithOptions(e.def, w, @field(W, entry), .{ .description = e.options.description });
            }
            inline for (comptime entryNames(W, "signals")) |entry| {
                const e = comptime handlerEntry("signals", @field(W.signals, entry)).?;
                try ctx.setSignalHandlerWithOptions(e.def, w, @field(W, entry), e.options);
            }
            inline for (comptime entryNames(W, "updates")) |entry| {
                const e = comptime handlerEntry("updates", @field(W.updates, entry)).?;
                if (e.validator) |v|
                    try ctx.setUpdateHandlerWithOptions(e.def, w, @field(W, entry), @field(W, v), e.options)
                else
                    try ctx.setUpdateHandlerWithOptions(e.def, w, @field(W, entry), null, e.options);
            }
            return w;
        }

        pub const versioning_behavior = options.versioning_behavior;
        pub const registered: Registered = .{
            .name = name,
            .invoke = invoke,
            .versioning_behavior = options.versioning_behavior,
            .task_failure_errors = options.task_failure_errors,
            .nondeterminism_fails_workflow = options.nondeterminism_fails_workflow,
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
            if (std.mem.eql(u8, decl, "queries")) if (reservedQueryError(def_name)) |message| return where ++ ": " ++ message;
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
        .want = "a QueryDef (temporal.workflow.QueryDef(name, fn (args...) Result)), or .{ QueryDef, QueryHandlerOptions } such as .{ Status, .{ .description = \"...\" } }",
    };
    if (std.mem.eql(u8, decl, "signals")) return .{
        .def = "SignalDef",
        .name_decl = "signal_name",
        .noun = "signal",
        .example = "`.{ .approve = Approve }`",
        .want = "a SignalDef (temporal.workflow.SignalDef(name, fn (args...) void)), or .{ SignalDef, HandlerOptions }",
    };
    return .{
        .def = "UpdateDef",
        .name_decl = "update_name",
        .noun = "update",
        .example = "`.{ .setLimit = SetLimit }` or `.{ .setLimit = .{ SetLimit, .checkLimit } }`",
        .want = "an UpdateDef (temporal.workflow.UpdateDef(name, fn (args...) Result)), or .{ UpdateDef, .validatorMethod }, with HandlerOptions such as .{ .unfinished_policy = .abandon } last if any",
    };
}

/// The Def of an entry of `W.queries`, `signals`, or `updates`, the name of its validator
/// method, and its handler options, or null if the entry isn't one of `decl`'s kind. A query
/// or signal entry is `Def` or `.{ Def, options }`; an update entry is also
/// `.{ Def, .validator }` or `.{ Def, .validator, options }`. A query entry's options are
/// `QueryHandlerOptions`, given here as the `HandlerOptions` with the same description.
fn handlerEntry(comptime decl: []const u8, comptime value: anytype) ?struct { def: type, validator: ?[]const u8 = null, options: HandlerOptions = .{} } {
    const name_decl = defKind(decl).name_decl;
    if (@TypeOf(value) == type) return if (isDef(value, name_decl)) .{ .def = value } else null;
    const s = switch (@typeInfo(@TypeOf(value))) {
        .@"struct" => |s| s,
        else => return null,
    };
    if (!s.is_tuple or s.field_types.len < 2 or s.field_types.len > 3 or s.field_types[0] != type) return null;
    if (!isDef(value[0], name_decl)) return null;
    if (std.mem.eql(u8, decl, "queries")) {
        if (s.field_types.len != 2) return null;
        const options = handlerOptions(QueryHandlerOptions, value[1]) orelse return null;
        return .{ .def = value[0], .options = .{ .description = options.description } };
    }
    const updates = std.mem.eql(u8, decl, "updates");
    const has_validator = updates and @typeInfo(s.field_types[1]) == .enum_literal;
    if (s.field_types.len == 3 and !has_validator) return null;
    if (s.field_types.len == 2 and has_validator) return .{ .def = value[0], .validator = @tagName(value[1]) };
    const options = handlerOptions(HandlerOptions, value[s.field_types.len - 1]) orelse return null;
    return .{ .def = value[0], .validator = if (has_validator) @tagName(value[1]) else null, .options = options };
}

/// `value` as `Options` (`HandlerOptions` or `QueryHandlerOptions`): an `Options`, or a
/// struct literal of its fields such as `.{ .unfinished_policy = .abandon }` or
/// `.{ .description = "..." }`. Null if it is neither.
fn handlerOptions(comptime Options: type, comptime value: anytype) ?Options {
    const T = @TypeOf(value);
    if (T == Options) return value;
    const s = switch (@typeInfo(T)) {
        .@"struct" => |s| s,
        else => return null,
    };
    if (s.is_tuple) return null;
    var options: Options = .{};
    for (s.field_names, s.field_types) |name, F| {
        if (!@hasField(Options, name)) return null;
        const Want = @FieldType(Options, name);
        if (F == Want) {
            @field(options, name) = @field(value, name);
        } else if (@typeInfo(F) == .enum_literal and @typeInfo(Want) == .@"enum") {
            @field(options, name) = std.meta.stringToEnum(Want, @tagName(@field(value, name))) orelse return null;
        } else if (Want == []const u8 and isString(F)) {
            @field(options, name) = @field(value, name);
        } else return null;
    }
    return options;
}

/// Whether a value of type `T` is a string that coerces to `[]const u8`: a string literal
/// (`*const [n:0]u8`), or a slice or pointer to an array of `u8`.
fn isString(comptime T: type) bool {
    const p = switch (@typeInfo(T)) {
        .pointer => |p| p,
        else => return false,
    };
    return switch (p.size) {
        .slice => p.child == u8,
        .one => switch (@typeInfo(p.child)) {
            .array => |a| a.child == u8,
            else => false,
        },
        else => false,
    };
}

/// Why a query handler can't be registered for queries of type `query_name`, as the compile
/// error `Context.setQueryHandler`, `Context.setQueryHandlerWithOptions` and a struct
/// workflow's `queries` entry report, or null if it can: the worker answers
/// `__enhanced_stack_trace` and `__temporal_workflow_metadata` itself, for every workflow.
fn reservedQueryError(comptime query_name: []const u8) ?[]const u8 {
    for ([_][]const u8{ Instance.enhanced_stack_trace_query, Instance.workflow_metadata_query }) |reserved| {
        if (std.mem.eql(u8, query_name, reserved))
            return "query type \"" ++ query_name ++ "\" is reserved: the worker answers it itself for every workflow, so a handler registered for it would never run; give the query another name";
    }
    return null;
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
    return DynamicDefWithOptions(func, .{});
}

/// `DynamicDef` with options. Only `task_failure_errors` applies to a dynamic workflow;
/// setting `versioning_behavior` (choose it with `Context.setVersioningBehavior`) or
/// `nondeterminism_fails_workflow` (set `worker.Options.nondeterminism_fails_workflow`) is a
/// compile error.
///
///     pub const AnyWorkflow = temporal.workflow.DynamicDefWithOptions(anyWorkflow, .{ .task_failure_errors = &.{error.ConfigMissing} });
pub fn DynamicDefWithOptions(comptime func: anytype, comptime options: DefOptions) type {
    if (options.versioning_behavior != .unspecified)
        @compileError("DynamicDefWithOptions: a dynamic workflow has no versioning_behavior of its own; choose each run's with ctx.setVersioningBehavior");
    if (options.nondeterminism_fails_workflow)
        @compileError("DynamicDefWithOptions: nondeterminism_fails_workflow needs a workflow type name, which a dynamic workflow doesn't have; set worker.Options.nondeterminism_fails_workflow instead");
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
            const result = try func(ctx, .{ .payloads = payloads, .context = ctx.converterContext() });
            return converter.toPayloadWithContext(ctx.allocator(), ctx.instance.resultConverterContext(), result);
        }

        /// The registration. Its name is empty: it answers to every unregistered type.
        pub const registered: Registered = .{ .name = "", .invoke = invoke, .task_failure_errors = options.task_failure_errors };
    };
}

/// The query Temporal's UI and CLI send to see where a running workflow waits, which a worker
/// answers for every workflow it runs without any registration: text with an entry per live
/// coroutine of the run (the workflow function, running signal and update handlers, and the
/// coroutines the workflow started), saying what it waits on and its call sites in workflow
/// code. Clients send it like any query:
///
///     const where = try client.queryWorkflow(temporal.workflow.StackTraceQuery, arena, id, "", .{});
///
/// A handler the workflow registers for `__stack_trace` with `Context.setQueryHandler`
/// answers it instead; a dynamic query handler never gets it. README.md's "Stack trace query"
/// section shows the answer.
///
/// Answering is read-only: it emits no command and changes no workflow state, so a run
/// restored from history answers as the run that first got there did. When no query comes,
/// the cost is a few stores at each wait, recording what it waits on (an activity's type is
/// copied, cut to 64 bytes, to the waiting coroutine's stack), and no allocation; the
/// coroutines' stacks are unwound and debug info is read only to answer.
pub const StackTraceQuery = QueryDef(Instance.stack_trace_query, fn () []const u8);

/// The answer to `__enhanced_stack_trace` (`EnhancedStackTraceQuery`), which the worker
/// encodes as JSON (`json/plain`) with these field names, the shape Temporal's UI reads.
/// `stacks` has one entry per live coroutine, in the order and set of `__stack_trace`'s
/// entries, each listing its call sites in workflow code, innermost first. `sources` maps
/// each file those call sites are in, where the worker can read it at the path its debug
/// info names, to excerpts of the lines around them.
pub const EnhancedStackTrace = struct {
    sdk: Sdk = .{},
    stacks: []const Stack = &.{},
    sources: std.json.ArrayHashMap([]const FileSlice) = .{},

    /// The SDK that answered: `temporal-zig` and `temporal.version`.
    pub const Sdk = struct { name: []const u8 = "", version: []const u8 = "" };
    /// One coroutine's call sites; none when the build has no debug info for them.
    pub const Stack = struct { locations: []const Location = &.{} };
    pub const Location = struct {
        file_path: []const u8 = "",
        /// 1-based.
        line: i32 = 0,
        column: i32 = 0,
        function_name: []const u8 = "",
        /// Always false: SDK and standard library frames are left out.
        internal_code: bool = false,
    };
    /// The lines of a file after its first `line_offset` lines.
    pub const FileSlice = struct { line_offset: u32 = 0, content: []const u8 = "" };
};

/// The query Temporal's UI sends to show where a running workflow waits, with the source
/// around each place, which a worker answers for every workflow it runs without any
/// registration (see `EnhancedStackTrace`). Registering a handler for it is a compile error.
///
///     const trace = try client.queryWorkflow(temporal.workflow.EnhancedStackTraceQuery, arena, id, "", .{});
///
/// Answering is read-only, as for `StackTraceQuery`. Source excerpts are read from the files
/// at the paths in the worker's debug info, which on a machine other than the one that
/// built the worker are usually missing; those files are left out of `sources`.
pub const EnhancedStackTraceQuery = QueryDef(Instance.enhanced_stack_trace_query, fn () EnhancedStackTrace);

/// The query Temporal's UI sends to list a workflow's handlers, which a worker answers for
/// every workflow it runs without any registration: the workflow type and every query,
/// signal and update name registered in the run, each with the description it was registered
/// with, sorted by name. The queries include the three the worker answers itself. Dynamic
/// handlers aren't listed, and `current_details` is empty. Registering a handler for it is a
/// compile error.
///
///     const metadata = try client.queryWorkflow(temporal.workflow.WorkflowMetadataQuery, arena, id, "", .{});
///     for (metadata.definition.?.query_definitions.items) |q| std.debug.print("{s}: {s}\n", .{ q.name, q.description });
pub const WorkflowMetadataQuery = QueryDef(Instance.workflow_metadata_query, fn () proto.temporal.api.sdk.v1.WorkflowMetadata);

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

/// The result type of a dynamic query or update handler `H` (`what` names which): its
/// return type, without an error union. A query must answer something; an update may return
/// `void`.
fn dynamicResult(comptime what: []const u8, comptime C: type, comptime H: type) type {
    const bad = what ++ " must be fn (" ++ @typeName(C) ++ ", []const u8, temporal.converter.EncodedValues) Result (or an error union of it), not " ++ @typeName(H);
    const info = switch (@typeInfo(H)) {
        .@"fn" => |f| f,
        else => @compileError(bad),
    };
    const Ret = info.return_type orelse @compileError(bad);
    const Result = switch (@typeInfo(Ret)) {
        .error_union => |eu| eu.payload,
        else => Ret,
    };
    if (Result == void and std.mem.eql(u8, what, "dynamic query handler")) @compileError(bad);
    return Result;
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

    // Handler options: only on signals and updates, last, and with HandlerOptions' fields.
    try T.expectMessage(comptime defError("W", Base.With(struct {
        pub const queries = .{ .currentTotal = .{ T.Total, .{ .unfinished_policy = .abandon } } };
    })), &.{"queries.currentTotal must be a QueryDef"});
    try T.expectMessage(comptime defError("W", Base.With(struct {
        pub const signals = .{ .add = .{ T.Add, .{ .unfinished_policy = .later } } };
    })), &.{ "signals.add must be a SignalDef", ".{ SignalDef, HandlerOptions }" });
    try T.expectMessage(comptime defError("W", Base.With(struct {
        pub const updates = .{ .reset = .{ T.Reset, .{ .policy = .abandon } } };
    })), &.{ "updates.reset must be an UpdateDef", "HandlerOptions such as .{ .unfinished_policy = .abandon } last" });
    try T.expectMessage(comptime defError("W", Base.With(struct {
        pub const updates = .{ .reset = .{ T.Reset, HandlerOptions{}, .checkReset } };
    })), &.{"updates.reset must be an UpdateDef"});

    // Valid entries pass; other pub declarations are ignored.
    try std.testing.expect(comptime defError("W", Base.With(struct {
        pub const signals = .{ .add = T.Add };
        pub const queries = .{ .currentTotal = T.Total };
        pub const updates = .{ .reset = T.Reset };
    })) == null);
    try std.testing.expect(comptime defError("W", Base.With(struct {
        pub const signals = .{ .add = .{ T.Add, .{ .unfinished_policy = .abandon } } };
        pub const updates = .{ .reset = .{ T.Reset, .add2, HandlerOptions{ .unfinished_policy = .abandon } } };
    })) != null); // .add2 names no method
    const declared = comptime handlerEntry("updates", .{ T.Reset, .checkReset, .{ .unfinished_policy = .abandon } }).?;
    try std.testing.expectEqualStrings("checkReset", declared.validator.?);
    try std.testing.expectEqual(HandlerUnfinishedPolicy.abandon, declared.options.unfinished_policy);
    const plain = comptime handlerEntry("signals", .{ T.Add, HandlerOptions{} }).?;
    try std.testing.expectEqual(HandlerUnfinishedPolicy.warn_and_abandon, plain.options.unfinished_policy);
    try std.testing.expect(comptime defError("W", Base.With(struct {
        pub const signals = .{ .add = .{ T.Add, .{ .unfinished_policy = .abandon } } };
        pub const updates = .{ .reset = .{ T.Reset, HandlerOptions{ .unfinished_policy = .abandon } } };
    })) == null);
}

test "a query handler for __enhanced_stack_trace or __temporal_workflow_metadata is refused with an error naming the query type" {
    const T = StructDefTests;
    inline for (.{ "__enhanced_stack_trace", "__temporal_workflow_metadata" }) |name| {
        try T.expectMessage(comptime reservedQueryError(name), &.{ "query type \"", name, "\" is reserved", "the worker answers it itself", "give the query another name" });
    }
    try std.testing.expect(comptime reservedQueryError(EnhancedStackTraceQuery.query_name) != null);
    try std.testing.expect(comptime reservedQueryError(WorkflowMetadataQuery.query_name) != null);
    // `__stack_trace` stays the workflow's to answer; other names are free.
    try std.testing.expect(comptime reservedQueryError("__stack_trace") == null);
    try std.testing.expect(comptime reservedQueryError("__temporal_other") == null);
    try std.testing.expect(comptime reservedQueryError("status") == null);

    // A struct workflow's `queries` entry, plain or with options.
    const W = struct {
        pub const queries = .{ .meta = .{ WorkflowMetadataQuery, .{ .description = "mine" } } };
        pub fn run(_: *@This(), _: *Context) !void {}
        pub fn meta(_: *@This()) proto.temporal.api.sdk.v1.WorkflowMetadata {
            return .{};
        }
    };
    try T.expectMessage(comptime defError("W", W), &.{ "workflow \"W\": queries.meta: query type \"__temporal_workflow_metadata\" is reserved", "give the query another name" });
    const Trace = struct {
        pub const queries = .{ .trace = EnhancedStackTraceQuery };
        pub fn run(_: *@This(), _: *Context) !void {}
        pub fn trace(_: *@This()) EnhancedStackTrace {
            return .{};
        }
    };
    try T.expectMessage(comptime defError("W", Trace), &.{"queries.trace: query type \"__enhanced_stack_trace\" is reserved"});
}

test "struct workflow entries take a description: string literals, slices and the options types" {
    const T = StructDefTests;
    const literal = comptime handlerEntry("queries", .{ T.Total, .{ .description = "The total" } }).?;
    try std.testing.expectEqualStrings("The total", literal.options.description);
    const typed = comptime handlerEntry("queries", .{ T.Total, QueryHandlerOptions{ .description = "typed" } }).?;
    try std.testing.expectEqualStrings("typed", typed.options.description);
    const slice: []const u8 = "a slice";
    const sliced = comptime handlerEntry("signals", .{ T.Add, .{ .description = slice, .unfinished_policy = .abandon } }).?;
    try std.testing.expectEqualStrings("a slice", sliced.options.description);
    try std.testing.expectEqual(HandlerUnfinishedPolicy.abandon, sliced.options.unfinished_policy);
    const update = comptime handlerEntry("updates", .{ T.Reset, .checkReset, .{ .description = "Resets" } }).?;
    try std.testing.expectEqualStrings("Resets", update.options.description);
    try std.testing.expectEqualStrings("checkReset", update.validator.?);
    // Not a description: a number, or a query option a query doesn't have.
    try std.testing.expect(comptime handlerEntry("queries", .{ T.Total, .{ .description = 5 } }) == null);
    try std.testing.expect(comptime handlerEntry("signals", .{ T.Add, .{ .description = .abandon } }) == null);
    try std.testing.expect(comptime handlerEntry("queries", .{ T.Total, .{ .description = "x", .unfinished_policy = .abandon } }) == null);
    try std.testing.expect(comptime handlerEntry("queries", .{ T.Total, HandlerOptions{} }) == null);
}

test "executeNexusOperation's compile errors name the service and the operation" {
    const In = struct { message: []const u8 };
    const Echo = nexus.OperationDef("echo", In, []const u8);
    const Ping = nexus.OperationDef("ping", void, void);
    const Service = nexus.ServiceDef("hello-service", .{ Echo, Ping });
    const T = StructDefTests;

    try std.testing.expect(comptime call_check.callError("executeNexusOperation", Service, Echo) == null);
    try std.testing.expect(comptime call_check.callError("executeNexusOperation", Service, Ping) == null);
    // An equal definition elsewhere is the same operation.
    try std.testing.expect(comptime call_check.callError("executeNexusOperation", Service, nexus.OperationDef("echo", In, []const u8)) == null);

    try T.expectMessage(comptime call_check.callError("executeNexusOperation", Service, nexus.OperationDef("shout", In, []const u8)), &.{
        "executeNexusOperation on nexus service \"hello-service\"",
        "has no operation \"shout\"",
        "its operations are \"echo\", \"ping\"",
    });
    try T.expectMessage(comptime call_check.callError("executeNexusOperation", Service, nexus.OperationDef("echo", u32, []const u8)), &.{
        "nexus service \"hello-service\"",
        "its operation \"echo\" takes",
        "and returns []const u8, not u32 and []const u8",
        "call it with the service's own OperationDef",
    });
    try T.expectMessage(comptime call_check.callError("executeNexusOperation", u8, Echo), &.{"Service must be a nexus.ServiceDef, not u8"});
    try T.expectMessage(comptime call_check.callError("executeNexusOperation", Service, In), &.{ "nexus service \"hello-service\"", "Operation must be a nexus.OperationDef" });
}
