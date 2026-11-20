//! One cached workflow run: turns sdk-core activations into commands.
//!
//! The worker feeds each `WorkflowActivation` for the run to `activate`. Jobs are applied in
//! the order the core sends them (it orders them for determinism), then every coroutine runs
//! until it blocks, and the commands they produced become the activation's completion.
//!
//! The run's memory (workflow state, decoded arguments, results kept by futures, and the
//! coroutines' Task blocks) lives in an arena owned by the instance, reached through
//! `arena()`, which counts the bytes requested from it. `memoryUsed` adds the coroutine
//! stacks the run holds (in use or pooled) at the configured stack size. Both depend on what
//! the run did, so the same activations give the same figure on any worker and on replay,
//! except for allocations made only when not replaying: a side effect's encoded value (and
//! the argument list carrying it) is counted where it first ran, and not when replayed. With
//! `Options.memory_limit` set, `activate` compares the figure with the limit once all
//! coroutines have blocked in an activation that ran them, and fails the workflow task (not
//! the workflow) when the run is over it.
//!
//! When the core evicts the run, `destroy` tears it down in order: `teardown` marks the run
//! evicting (from then on `emit` drops every command, so nothing reaches the core), cancels
//! the root and handler coroutines, wakes waits that ignore cancellation, and runs the
//! scheduler so their defers run. Then the scheduler, the metrics handler, and the arena are
//! freed. A coroutine that still hasn't exited (it blocked with cancel protection on) keeps
//! its stack mapped and is reported; everything else is freed.

const Instance = @This();

const std = @import("std");
const proto = @import("../proto.zig");
const proto_time = @import("../proto_time.zig");
const converter = @import("../converter.zig");
const failure_mod = @import("../failure.zig");
const log = @import("../log.zig");
const workflow = @import("../workflow.zig");
const client = @import("../client.zig");
const sa = @import("../search_attributes.zig");
const propagation = @import("../propagation.zig");
const interceptor = @import("../interceptor.zig");
const metrics_mod = @import("../metrics.zig");
const deployment = @import("../deployment.zig");
const builtin = @import("builtin");
const Scheduler = @import("Scheduler.zig");
const WorkflowIo = @import("WorkflowIo.zig");
const call_sites = @import("call_sites.zig");
const system = @import("../nexus/system.zig");
const Allocator = std.mem.Allocator;

const act_pb = proto.coresdk.workflow_activation;
const cmd_pb = proto.coresdk.workflow_commands;
const done_pb = proto.coresdk.workflow_completion;
const result_pb = proto.coresdk.activity_result;
const common = proto.temporal.api.common.v1;
const sdk_pb = proto.temporal.api.sdk.v1;
const Failure = failure_mod.Failure;

/// Activity types starting with this belong to the SDK; workers refuse to register them.
pub const reserved_activity_prefix = "__temporal_";
/// The local activity that records a side effect's value by returning its input. Histories
/// name it in their markers, so it must never change.
pub const side_effect_activity_type = reserved_activity_prefix ++ "side_effect";

// Sessions (`workflow.Context.createSession`). Histories name these, so they must never change.
/// The activity that holds a session's slot on its host, from creation until the session
/// completes; it signals the workflow once the host has taken the session.
pub const session_create_activity_type = reserved_activity_prefix ++ "session_create";
/// The activity that frees a session's slot on its host when the workflow completes it.
pub const session_complete_activity_type = reserved_activity_prefix ++ "session_complete";
/// The signal the creation activity sends with `SessionOpened`.
pub const session_opened_signal = reserved_activity_prefix ++ "session_opened";
/// The header carrying the session ID of an activity run in a session.
pub const session_id_header = reserved_activity_prefix ++ "session_id";
/// Appended to a task queue to name the queue that session workers take new sessions from.
pub const session_queue_suffix = reserved_activity_prefix ++ "sessions";
/// The creation activity's argument.
pub const SessionCreateInput = struct { session_id: []const u8, heartbeat_interval_ms: i64 };
/// The opened signal's argument: the host's own task queue and its name.
pub const SessionOpened = struct { session_id: []const u8, task_queue: []const u8, host_name: []const u8 };

gpa: Allocator,
arena_state: std.heap.ArenaAllocator,
/// Live bytes requested through `arena()`: allocations add, frees subtract.
allocated: usize = 0,
/// See `Options.memory_limit`.
memory_limit: ?usize = null,
sched: Scheduler,
wio: WorkflowIo,
ctx: workflow.Context,
entry: workflow.Registered,
info: workflow.Info = undefined,
logger: log.Logger = .{},
/// The worker's handler until initialization, then one with the run's attributes that is
/// silent while replaying, freed by `destroy`.
metrics: metrics_mod.Handler = .noop,
now_ns: i96 = 0,
/// How many activations `activate` has begun, for telling whether memory a call borrowed for
/// the activation it was made in is still valid (see `ActivityState.activity_type`).
activations: u64 = 0,
replaying: bool = false,
/// Commands produced during the current activation.
commands: std.ArrayList(cmd_pb.WorkflowCommand) = .empty,
next_activity_seq: u32 = 1,
next_timer_seq: u32 = 1,
next_child_seq: u32 = 1,
/// Signals and cancellation requests to other workflows share one sequence space; the core
/// keys each kind separately, so their seqs never meet.
next_external_seq: u32 = 1,
next_nexus_seq: u32 = 1,
activities: std.AutoHashMapUnmanaged(u32, *ActivityState) = .empty,
timers: std.AutoHashMapUnmanaged(u32, *WorkflowIo.Timer) = .empty,
/// The summary of the timer `sleep` is about to start; `hostStartTimer` sends it.
timer_summary: []const u8 = "",
/// Child workflows that have not closed, by seq.
children: std.AutoHashMapUnmanaged(u32, *ChildState) = .empty,
/// Signals and cancellation requests to other workflows that the core has not resolved, by seq.
externals: std.AutoHashMapUnmanaged(u32, *ExternalState) = .empty,
/// Nexus operations the core has not resolved, by seq.
nexus_operations: std.AutoHashMapUnmanaged(u32, *NexusOperationState) = .empty,
/// Local activities waiting on a workflow timer before their next attempt, by timer seq.
backoffs: std.AutoHashMapUnmanaged(u32, *ActivityState) = .empty,
/// Coroutines blocked in `awaitCondition`, in the order they started waiting.
conditions: std.ArrayList(*Condition) = .empty,
/// Every session the run created, in creation order, for the opened signal to find. Arena
/// memory.
sessions: std.ArrayList(*workflow.Session) = .empty,
args: []const converter.Payload = &.{},
/// The start header (InitializeWorkflow's headers), sorted by name. Arena memory.
header: converter.Header = .{},
propagators: []const propagation.ContextPropagator = &.{},
/// What the propagators extracted from `header`, injected again into outgoing commands.
/// Arena memory.
propagated: propagation.Values = .empty,
/// The run's converter context (`workflow.Context.converterContext`): the namespace, and from
/// `initialize` the workflow ID and type, with `values` pointing at `propagated`.
converter_context: converter.Context = .{},
/// Activity and workflow type names the run's converter contexts name, each copied into the
/// arena once (see `intern`). Arena memory.
type_names: std.StringHashMapUnmanaged(void) = .empty,
interceptors: []const interceptor.Worker = &.{},
/// The run's interceptors, from `interceptors` at initialization. Arena memory.
inbound: []const interceptor.WorkflowInbound = &.{},
outbound: []const interceptor.WorkflowOutbound = &.{},
last_completion_result: ?converter.Payload = null,
/// The failure the previous run of a retried or cron workflow ended with. Arena memory.
previous_run_failure: ?*const Failure = null,
/// The memo as the workflow sees it: the start memo with this run's upserts applied, sorted
/// by name so its order doesn't depend on the wire order of the start memo's map. Each upsert
/// replaces the slice, so a slice handed out earlier keeps its contents. Arena memory.
memo: []const converter.Field = &.{},
/// The search attributes as the workflow sees them: the start values with this run's upserts
/// applied, sorted by name. Each upsert replaces the view's slice, so a view handed out
/// earlier keeps its contents. Arena memory.
search_attributes: sa.View = .{},
task_queue: []const u8 = "",
namespace: []const u8 = "",
root: ?*WorkflowIo.Task = null,
root_outcome: ?RootOutcome = null,
/// The signal handler whose error failed the workflow, if one did; its failure details
/// describe the workflow's failure. Arena memory.
failed_handler: ?*HandlerRun = null,
/// The return addresses of the error the workflow function returned, for the stack trace of
/// the failure built from it. Arena memory; empty without error return tracing.
root_trace: []const usize = &.{},
/// See `Options.failure_stack_traces`.
failure_stack_traces: bool = true,
/// See `Options.task_failure_errors`. Borrowed.
task_failure_errors: []const anyerror = &.{},
/// See `Options.failure_converter`. Borrowed.
failure_converter: ?converter.FailureConverter = null,
/// Update rejections built in a handler coroutine from a returned error, finished once the
/// coroutines have run: their stack trace is filled in then, because reading debug info to
/// symbolize a trace can take more stack than a coroutine has, and the failure converter
/// runs after it. Listed only when there is a trace to fill in or a converter to run. Arena
/// memory.
update_traces: std.ArrayList(UpdateTrace) = .empty,
finished: bool = false,
/// Set by `destroy`: waits that ignore workflow cancellation (an activity's cancellation
/// completing) end with error.Canceled instead, so every coroutine can exit.
evicting: bool = false,
/// Set when an activation must fail the workflow task (it is retried) instead of producing
/// commands, for example on an unsupported job.
task_failure: ?[]const u8 = null,
/// The application failure type of `task_failure`.
task_failure_type: []const u8 = "UnsupportedJob",
/// Whether `failCoroutineStart` has logged `wio.start_failure`, which it does once per run.
logged_start_failure: bool = false,
/// Query handlers by query type, in registration order (the order an unknown query's failure
/// lists them in). Arena memory; they outlive the workflow function.
queries: std.array_hash_map.String(QueryHandler) = .empty,
/// Answers queries with no handler registered for their type, if set. Arena memory.
dynamic_query_handler: ?DynamicQueryHandler = null,
/// Signal handlers by signal name, in registration order. Arena memory.
signal_handlers: std.array_hash_map.String(SignalHandler) = .empty,
/// Handles signals with no handler registered for their name, if set. Arena memory.
dynamic_signal_handler: ?SignalHandler = null,
/// Signals that arrived before a handler for their name (or a dynamic handler), in arrival
/// order. Arena memory.
buffered_signals: std.ArrayList(*const Signal) = .empty,
/// Every signal channel the run created, in creation order, for `unhandledSignalNames`.
/// Arena memory.
signal_channels: std.ArrayList(*SignalChannel) = .empty,
/// How many signals have been delivered: the next one's `Signal.arrival`.
signals_delivered: u64 = 0,
/// Update handlers by update name, in registration order (the order an unregistered update's
/// rejection lists them in). Arena memory.
update_handlers: std.array_hash_map.String(UpdateHandler) = .empty,
/// Handles updates with no handler registered for their name, if set. Arena memory.
dynamic_update_handler: ?UpdateHandler = null,
/// Updates delivered before a handler for their name (or a dynamic handler), in arrival order. Whatever is still here
/// once the activation's coroutines have all blocked is rejected. Arena memory.
buffered_updates: std.ArrayList(*const Update) = .empty,
/// Set when the current activation accepted an update.
accepted_update: bool = false,
/// Signal and update handler coroutines that have not returned, in start order. Each removes
/// itself when its handler returns; `destroy` cancels the rest.
handlers: std.ArrayList(*HandlerRun) = .empty,
/// A struct workflow's state (`W`), from `workflow.Registered.init_run`. Arena memory.
run_state: ?*anyopaque = null,
/// Set from a struct workflow's initialization until its `run` is called: handler runs go to
/// `held_handlers` instead of starting, so none runs before `run` or for a run whose
/// arguments didn't decode.
holding_handlers: bool = false,
/// Signal and update handler runs waiting for `releaseHandlers`, in arrival order. Arena memory.
held_handlers: std.ArrayList(*HandlerRun) = .empty,
/// The latest `requestContinueAsNew`, sent if the workflow function returns
/// error.ContinueAsNew. Arena memory.
continue_as_new: ?cmd_pb.ContinueAsNewWorkflowExecution = null,
/// From the latest activation that wasn't query-only.
history_length: u32 = 0,
history_size_bytes: u64 = 0,
continue_as_new_suggested: bool = false,
/// Patch IDs the core reported in history (NotifyHasPatch), in arrival order so that
/// `getVersion` scans them deterministically. Arena memory.
notified_patches: std.array_hash_map.String(void) = .empty,
/// What the run's first `patch` call returned for each patch ID; later calls return the same.
/// Arena memory.
patches: std.StringHashMapUnmanaged(bool) = .empty,
/// What the run's first `getVersion` call returned for each change ID. Arena memory.
versions: std.StringHashMapUnmanaged(i32) = .empty,
/// Sent with each successful completion when `send_versioning_behavior`; starts as the
/// registered workflow's.
versioning_behavior: workflow.VersioningBehavior = .unspecified,
send_versioning_behavior: bool = false,
/// The deployment version of the latest activation that wasn't query-only. Arena memory,
/// copied again only when it changes.
deployment_version: ?deployment.Version = null,

/// A type-erased query handler: decodes `args` into `out`, calls the handler with `context`,
/// and encodes its result into `out`, converting with `conv` (the run's converter context).
/// Runs on the worker thread, outside any coroutine.
pub const QueryHandler = struct {
    context: *const anyopaque,
    call: *const fn (context: *const anyopaque, out: Allocator, conv: *const converter.Context, args: []const converter.Payload) anyerror!converter.Payload,
};

/// A type-erased dynamic query handler: like `QueryHandler`, but `call` also gets the query
/// type, and `args` stay encoded for the handler to decode.
pub const DynamicQueryHandler = struct {
    context: *const anyopaque,
    call: *const fn (context: *const anyopaque, out: Allocator, conv: *const converter.Context, name: []const u8, args: []const converter.Payload) anyerror!converter.Payload,
};

/// A type-erased signal handler. `decode` turns a signal's name and payloads (both arena
/// memory of the run) into the handler's arguments, allocated in `out`, converting with
/// `conv` (the run's converter context, valid for the run); `call` runs the handler with
/// `context` and those arguments, inside the signal's coroutine.
pub const SignalHandler = struct {
    context: *const anyopaque,
    decode: SignalDecode,
    call: *const fn (context: *const anyopaque, args: *const anyopaque) anyerror!void,
    unfinished_policy: workflow.HandlerUnfinishedPolicy = .warn_and_abandon,
};

/// A type-erased update handler. `decode` turns an update's name and payloads into the
/// handler's argument tuple, allocated in `out`; `validate` (if set) runs the validator with
/// `context` and those arguments, outside any coroutine; `call` runs the handler inside the
/// update's coroutine and encodes its result into `out`. Both convert with `conv`, the run's
/// converter context.
pub const UpdateHandler = struct {
    context: *const anyopaque,
    decode: SignalDecode,
    validate: ?*const fn (context: *const anyopaque, args: *const anyopaque) anyerror!void,
    call: *const fn (context: *const anyopaque, out: Allocator, conv: *const converter.Context, args: *const anyopaque) anyerror!converter.Payload,
    unfinished_policy: workflow.HandlerUnfinishedPolicy = .warn_and_abandon,
};

/// Decodes a signal's or update's payloads into the handler's arguments (see `SignalHandler`).
pub const SignalDecode = *const fn (out: Allocator, conv: *const converter.Context, name: []const u8, args: []const converter.Payload) anyerror!*const anyopaque;

/// A delivered signal, copied into the arena so it outlives its activation. `arrival` orders
/// signals by delivery.
const Signal = struct { name: []const u8, args: []const converter.Payload, header: converter.Header, arrival: u64 };

/// The untyped part of `workflow.SignalChannel`: the signals its handler took, with their
/// decoded arguments, until a receive reads them. Arena memory, created by
/// `createSignalChannel`.
pub const SignalChannel = struct {
    inst: *Instance,
    /// The signal name, which outlives the run (see `setSignalHandler`).
    name: []const u8 = "",
    /// Unread signals are `items[head..]`, oldest first.
    items: std.ArrayList(Item) = .empty,
    head: usize = 0,
    /// Coroutines blocked in `receive`, each on its own stack.
    waiters: ?*Waiter = null,

    const Item = struct { signal: *const Signal, args: *const anyopaque };
    const Waiter = struct { wait: WorkflowIo.Wait, next: ?*Waiter = null };

    pub fn len(ch: *const SignalChannel) usize {
        return ch.items.items.len - ch.head;
    }

    /// The oldest unread signal's decoded arguments, removed from the channel, or null.
    pub fn take(ch: *SignalChannel) ?*const anyopaque {
        if (ch.head == ch.items.items.len) return null;
        const item = ch.items.items[ch.head];
        ch.head += 1;
        if (ch.head == ch.items.items.len) {
            ch.items.clearRetainingCapacity();
            ch.head = 0;
        }
        return item.args;
    }

    /// Like `take`, but blocks the calling coroutine until a signal arrives. Returns
    /// error.Canceled when the coroutine is canceled while it waits (by an enclosing future,
    /// `Select`, or `Group`, or with the workflow), leaving the channel's signals unread.
    pub fn receive(ch: *SignalChannel) error{Canceled}!*const anyopaque {
        while (true) {
            if (ch.inst.wio.refuse("SignalChannel.receive")) return error.Canceled;
            if (ch.take()) |args| return args;
            var waiter: Waiter = .{ .wait = ch.inst.wio.newWaitPoint(.{ .what = "signal channel", .name = ch.name }), .next = ch.waiters };
            ch.waiters = &waiter;
            defer ch.removeWaiter(&waiter);
            try ch.inst.wio.waitFor(&waiter.wait, true);
        }
    }

    fn removeWaiter(ch: *SignalChannel, waiter: *Waiter) void {
        var link = &ch.waiters;
        while (link.*) |w| : (link = &w.next) if (w == waiter) {
            link.* = w.next;
            return;
        };
    }

    /// Appends a signal its handler coroutine decoded and wakes every waiting receive; each
    /// takes one signal or waits again.
    ///
    /// The receives run before the coroutines that are already ready, such as a timer's
    /// sleep that the same activation fired (core delivers signals before timers). A receive
    /// raced against that timer in a `std.Io.Select` then completes first, so the Select
    /// returns the signal; otherwise the receive would take the signal after the timer won,
    /// and canceling the Select would discard it.
    fn push(ch: *SignalChannel, signal: *const Signal, args: *const anyopaque) error{OutOfMemory}!void {
        try ch.items.append(ch.inst.arena(), .{ .signal = signal, .args = args });
        // Each is woken to the front, so the last woken runs first. `waiters` is newest
        // first, which leaves the oldest receive running first.
        var next = ch.waiters;
        ch.waiters = null;
        while (next) |w| {
            next = w.next;
            ch.inst.wio.wakeFirst(&w.wait);
        }
    }

    fn handlerCall(context: *const anyopaque, args: *const anyopaque) anyerror!void {
        const ch: *SignalChannel = @ptrCast(@alignCast(@constCast(context)));
        const run = ch.inst.currentHandler() orelse return error.HandlerNotFound;
        run.taken = true;
        return ch.push(run.kind.signal.signal, args);
    }

    fn isHandler(handler: SignalHandler) bool {
        return handler.call == handlerCall;
    }
};

/// A delivered update, copied into the arena so it outlives its activation.
const Update = struct {
    id: []const u8,
    protocol_instance_id: []const u8,
    name: []const u8,
    args: []const converter.Payload,
    header: converter.Header,
    run_validator: bool,

    fn input(u: *const Update, context: *const converter.Context) interceptor.UpdateInput {
        return .{ .update_name = u.name, .update_id = u.id, .args = .{ .payloads = u.args, .context = context }, .header = u.header };
    }
};

/// An update rejection `fillUpdateTraces` finishes: `addresses` is its stack trace (empty for
/// none), and `err` the handler's error, for the failure converter.
const UpdateTrace = struct { protocol_instance_id: []const u8, addresses: []const usize, err: anyerror };

/// A running signal or update handler coroutine. Arena memory.
const HandlerRun = struct {
    name: []const u8,
    kind: union(enum) {
        signal: struct { handler: SignalHandler, signal: *const Signal },
        update: struct { handler: UpdateHandler, update: *const Update },
    },
    /// Set after spawning, before the coroutine first runs; freed when it returns.
    task: *WorkflowIo.Task = undefined,
    /// This handler's `Context.fail` or `Context.failWith` and the failure of the last activity
    /// or child future it awaited; the workflow function's are in `Context`.
    pending_failure: ?PendingFailure = null,
    last_failure: ?*const Failure = null,
    /// The return addresses of the error the handler returned; see `Instance.root_trace`.
    trace: []const usize = &.{},
    /// Set when a signal channel's handler appended the signal to its channel.
    taken: bool = false,
};

/// The failure a coroutine chose with `Context.fail` or `Context.failWith`; the later call
/// replaces the earlier. `failure` is copied when recorded, so a failure on the caller's
/// stack survives the return; what it points to (message, details, cause) is borrowed.
pub const PendingFailure = union(enum) {
    application: failure_mod.Application,
    failure: Failure,

    /// The failure to send. A failure described with `fail` gets the stack trace `trace`
    /// (text); one given to `failWith` keeps its own.
    fn resolve(p: PendingFailure, a: Allocator, trace: []const u8) error{OutOfMemory}!Failure {
        return switch (p) {
            .application => |app| blk: {
                var f = try failure_mod.application(a, app);
                f.stack_trace = trace;
                break :blk f;
            },
            .failure => |f| f,
        };
    }
};

/// `continued_as_new`: the workflow function returned error.ContinueAsNew after
/// `requestContinueAsNew`; the request is in `continue_as_new`.
const RootOutcome = union(enum) { completed: converter.Payload, failed: anyerror, continued_as_new };

pub const ActivityState = struct {
    seq: u32,
    waiter: ?*WorkflowIo.Wait = null,
    outcome: ?Outcome = null,
    cancel_requested: bool = false,
    cancellation_type: CancellationType = .try_cancel,
    /// Borrowed from the caller of `executeActivity`, and valid only during activation
    /// `scheduled_in`: the ScheduleActivity command borrows it as long, and nothing more is
    /// promised. Read only then (by `awaitActivity`), and never copied into the run's memory,
    /// so recording waits allocates nothing. Empty for an activity an interceptor refused.
    activity_type: []const u8 = "",
    /// The `Instance.activations` count when the activity was scheduled.
    scheduled_in: u64 = 0,
    /// Set for a local activity; null for a remote one.
    local: ?*Local = null,
    /// The converter context of the activity's result and failure: the run's, with the
    /// activity type and the scheduled activity ID (arena memory, updated when a local
    /// activity is retried under a new ID). The run's own for a side effect and for an
    /// activity an interceptor refused.
    context: converter.Context = .{},

    pub const CancellationType = workflow.ActivityCancellationType;

    /// A local activity's last ScheduleLocalActivity command (arena memory), sent again with
    /// a new seq for each attempt that follows a backoff timer.
    pub const Local = struct {
        cmd: cmd_pb.ScheduleLocalActivity,
        /// The activity ID is the attempt's seq, not one the workflow chose.
        id_from_seq: bool,
        /// The backoff timer's seq while the activity waits for its next attempt.
        backoff_timer: ?u32 = null,
        /// The summary, sent with every attempt: the core records the last attempt's on the
        /// marker.
        user_metadata: ?sdk_pb.UserMetadata = null,
    };

    pub const Outcome = union(enum) {
        completed: ?converter.Payload,
        failed: *const Failure,
        cancelled: *const Failure,
    };
};

/// A child workflow, from its StartChildWorkflowExecution command until it closes. Arena
/// memory; the future keeps pointing at it after it closes.
pub const ChildState = struct {
    seq: u32,
    workflow_id: []const u8,
    workflow_type: []const u8,
    cancellation_type: workflow.ChildWorkflowCancellationType,
    /// The coroutines blocked in `started` and in `get`.
    start_waiter: ?*WorkflowIo.Wait = null,
    result_waiter: ?*WorkflowIo.Wait = null,
    /// Set once the server started the child.
    run_id: ?[]const u8 = null,
    /// Set once the child closed, or failed to start (or was canceled before it started).
    outcome: ?Outcome = null,
    /// The start failed because the workflow ID is taken.
    already_started: bool = false,
    cancel_requested: bool = false,
    /// With `wait_cancellation_requested`: the RequestCancelExternalWorkflowExecution this
    /// workflow sent itself once the child started, whose resolution reports the cancellation.
    cancel_request: ?*ExternalState = null,
    /// The converter context of the child's result, failure and signals: the namespace, the
    /// child's workflow ID and type, and the run's propagated values. The run's own for a
    /// child an interceptor refused.
    context: converter.Context = .{},

    pub const Outcome = ActivityState.Outcome;

    pub const Phase = enum { started, result };

    fn reached(state: *const ChildState, phase: Phase) bool {
        return state.outcome != null or (phase == .started and state.run_id != null);
    }

    fn wake(state: *ChildState, wio: *WorkflowIo) void {
        if (state.start_waiter) |w| wio.wake(w);
        if (state.result_waiter) |w| wio.wake(w);
    }
};

/// A Nexus operation, from its ScheduleNexusOperation command until the core resolves it.
/// Arena memory; the future keeps pointing at it after it ends.
pub const NexusOperationState = struct {
    seq: u32,
    endpoint: []const u8,
    service: []const u8,
    operation: []const u8,
    cancellation_type: workflow.NexusOperationCancellationType,
    /// The coroutines blocked in `started` and in `get`.
    start_waiter: ?*WorkflowIo.Wait = null,
    result_waiter: ?*WorkflowIo.Wait = null,
    /// Set once the handler started the operation, or completed it synchronously.
    started: bool = false,
    /// The operation token of an asynchronous start; null for a synchronous one.
    token: ?[]const u8 = null,
    /// Set once the operation ended, or its start failed.
    outcome: ?Outcome = null,
    cancel_requested: bool = false,

    /// `completed` holds the result as the core sent it. A failed start is `canceled` or
    /// `timed_out` when its failure says so, else `failed`.
    pub const Outcome = union(enum) {
        completed: converter.Payload,
        failed: *const Failure,
        canceled: *const Failure,
        timed_out: *const Failure,
    };

    pub const Phase = enum { started, result };

    fn reached(state: *const NexusOperationState, phase: Phase) bool {
        return state.outcome != null or (phase == .started and state.started);
    }

    fn wake(state: *NexusOperationState, wio: *WorkflowIo) void {
        if (state.start_waiter) |w| wio.wake(w);
        if (state.result_waiter) |w| wio.wake(w);
    }
};

/// A signal or cancellation request sent to another workflow, from its command until the
/// core resolves it. Arena memory; the future keeps pointing at it after it resolves.
pub const ExternalState = struct {
    seq: u32,
    kind: Kind,
    /// The target's workflow ID, named in the failure's message.
    workflow_id: []const u8,
    waiter: ?*WorkflowIo.Wait = null,
    outcome: ?Outcome = null,
    cancel_requested: bool = false,
    /// Set for a request canceling this child under `wait_cancellation_requested`.
    child: ?*ChildState = null,

    pub const Kind = enum { signal, cancel };

    /// `not_found`: the workflow (or its namespace) doesn't exist or has closed. `failed`: the
    /// server refused for another reason. `canceled`: a signal canceled before it was sent.
    pub const Outcome = union(enum) {
        delivered,
        not_found: *const Failure,
        failed: *const Failure,
        canceled: *const Failure,
    };
};

pub const Options = struct {
    /// The worker's task queue and namespace, reported in `Info` and used as the default task
    /// queue for activities.
    task_queue: []const u8 = "",
    namespace: []const u8 = "default",
    stack_size: usize = 256 * 1024,
    /// The most memory the run may hold (`memoryUsed`), checked at the end of each
    /// activation that runs workflow code; over it, the activation fails the workflow task.
    /// Null: no limit. See `worker.Options.workflow_memory_limit`.
    memory_limit: ?usize = null,
    /// Put the error's return trace in the stack trace of a failure built from an error the
    /// workflow function or a handler returned. See `worker.Options.failure_stack_traces`.
    failure_stack_traces: bool = true,
    /// Errors that fail the workflow task instead of the workflow, besides the registered
    /// workflow's `task_failure_errors`. Borrowed: must outlive the instance. See
    /// `worker.Options.workflow_task_failure_errors`.
    task_failure_errors: []const anyerror = &.{},
    /// Transforms the failures the run sends and receives. Borrowed: must outlive the
    /// instance. See `client.Options.failure_converter`.
    failure_converter: ?converter.FailureConverter = null,
    /// Where the workflow's logger writes.
    logger: log.Sink = .std_log,
    /// Keep the workflow's logger on while it replays history.
    log_during_replay: bool = false,
    /// Extract `Context.propagated` from the start header and inject it into outgoing
    /// commands. Borrowed: must outlive the instance.
    propagators: []const propagation.ContextPropagator = &.{},
    /// Supply the run's interceptors at initialization. Borrowed: must outlive the instance.
    interceptors: []const interceptor.Worker = &.{},
    /// What the workflow's metrics handler records through. Borrowed: must outlive the
    /// instance.
    metrics: metrics_mod.Handler = .noop,
    /// Send the run's versioning behavior with its completions: only a worker whose
    /// deployment uses worker versioning may (the server rejects it from any other).
    send_versioning_behavior: bool = false,
};

/// Creates the instance for a run. `entry` is the registered workflow named by the run's
/// InitializeWorkflow job.
pub fn create(gpa: Allocator, entry: workflow.Registered, options: Options) !*Instance {
    const inst = try gpa.create(Instance);
    errdefer gpa.destroy(inst);
    inst.* = .{
        .gpa = gpa,
        .arena_state = .init(gpa),
        .sched = .init(gpa, .{ .stack_size = options.stack_size }),
        .wio = undefined,
        .ctx = undefined,
        .entry = entry,
        .versioning_behavior = entry.versioning_behavior,
        .send_versioning_behavior = options.send_versioning_behavior,
        .memory_limit = options.memory_limit,
        .failure_stack_traces = options.failure_stack_traces,
        .task_failure_errors = options.task_failure_errors,
        .failure_converter = options.failure_converter,
    };
    // `destroy` can't run on a half-built instance; free what's been taken so far instead.
    // `Scheduler.init` and `WorkflowIo.init` allocate nothing.
    errdefer inst.arena_state.deinit();
    inst.wio = .init(inst.arena(), &inst.sched, .{ .ctx = inst, .vtable = &host_vtable }, 0);
    inst.ctx = .{ .instance = inst };
    inst.task_queue = try inst.arena().dupe(u8, options.task_queue);
    inst.namespace = try inst.arena().dupe(u8, options.namespace);
    inst.logger = .{ .sink = options.logger, .replaying = if (options.log_during_replay) null else &inst.replaying };
    inst.propagators = options.propagators;
    inst.interceptors = options.interceptors;
    inst.metrics = options.metrics;
    inst.propagated = .init(inst.arena());
    inst.converter_context = .{ .values = &inst.propagated, .namespace = inst.namespace };
    return inst;
}

/// Tears the run down: cancels its coroutines, lets them exit, and frees everything. A
/// coroutine that refuses to exit keeps its stack mapped (it is never resumed), unless a
/// coroutine start of the run failed (see `teardown`); the rest of the run, including that
/// coroutine's Task block, is freed.
pub fn destroy(inst: *Instance) void {
    const stuck = inst.teardown();
    const gpa = inst.gpa;
    if (stuck == 0) {
        inst.sched.deinit();
    } else {
        // Freeing a stack under a coroutine that may still reference it isn't safe, so its
        // stack is leaked; the arena below frees its Task block and anything else it held.
        inst.logger.warn("workflow coroutines did not exit on eviction (blocked with cancel protection on?); leaking their stacks", .{
            .count = stuck,
            .stack_bytes = stuck * inst.sched.options.stack_size,
        });
        inst.sched.deinitLeakingLive();
    }
    inst.metrics.deinit();
    inst.arena_state.deinit();
    gpa.destroy(inst);
}

/// The first half of `destroy`: marks the run evicting, so `emit` drops every command from
/// here on, cancels the root and handler coroutines, wakes waits that ignore cancellation,
/// and runs the scheduler so they exit and their defers run. Returns how many coroutines are
/// still alive, which is always 0 for a run whose coroutine start failed (see the end). Calling
/// it again returns that count without doing anything else.
pub fn teardown(inst: *Instance) usize {
    if (inst.evicting) return inst.sched.live;
    inst.evicting = true;
    inst.commands.clearRetainingCapacity();
    // The root and handler coroutines may still be blocked even after the workflow finished
    // (a handler's error finishes it, and handlers outlive the root).
    if (inst.root) |root| inst.wio.cancelTask(root);
    for (inst.handlers.items) |run| inst.wio.cancelTask(run.task);
    // Coroutines already waiting for an activity's cancellation to complete don't see the
    // cancel above; wake them. The order doesn't matter: nothing they do is sent to the core.
    var pending = inst.activities.valueIterator();
    while (pending.next()) |state| if (state.*.waiter) |w| inst.wio.wake(w);
    var backing_off = inst.backoffs.valueIterator();
    while (backing_off.next()) |state| if (state.*.waiter) |w| inst.wio.wake(w);
    var children = inst.children.valueIterator();
    while (children.next()) |state| state.*.wake(&inst.wio);
    var operations = inst.nexus_operations.valueIterator();
    while (operations.next()) |state| state.*.wake(&inst.wio);
    inst.wio.run();
    std.debug.assert(inst.commands.items.len == 0);
    if (inst.wio.start_failure != null and inst.sched.live > 0) {
        // A failed coroutine start ran its function inline in the coroutine that started it,
        // which can leave that coroutine waiting, uncancelably, on itself: a Select case's
        // result put into a full queue that only the caller drains. No cancel reaches such a
        // wait, and leaking its stack would hold the mappings whose lack failed the start.
        // The run is never resumed, and everything that points into these stacks is the
        // run's own state, which `destroy` frees without following, so the stacks go now.
        const abandoned = inst.sched.abandonLive();
        inst.logger.warn("workflow coroutines of a run whose coroutine start failed did not exit on eviction; freeing their stacks without running their defers", .{ .count = abandoned });
    }
    return inst.sched.live;
}

/// The run's allocator: the arena, counting live requested bytes into `allocated`. Freed
/// as a whole by `destroy`.
pub fn arena(inst: *Instance) Allocator {
    return .{ .ptr = inst, .vtable = &run_vtable };
}

/// Bytes the run holds: live bytes requested from `arena()` plus its coroutine stacks, in
/// use or pooled, at the configured stack size. Independent of how the arena or the heap
/// placed the allocations.
pub fn memoryUsed(inst: *const Instance) usize {
    return inst.allocated + inst.sched.stackBytes();
}

const run_vtable: Allocator.VTable = .{
    .alloc = runAlloc,
    .resize = runResize,
    .remap = runRemap,
    .free = runFree,
};

fn runAlloc(ptr: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
    const inst: *Instance = @ptrCast(@alignCast(ptr));
    const p = inst.arena_state.allocator().rawAlloc(len, alignment, ret_addr) orelse return null;
    inst.allocated += len;
    return p;
}

fn runResize(ptr: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
    const inst: *Instance = @ptrCast(@alignCast(ptr));
    if (!inst.arena_state.allocator().rawResize(memory, alignment, new_len, ret_addr)) return false;
    inst.allocated = inst.allocated - memory.len + new_len;
    return true;
}

fn runRemap(ptr: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
    const inst: *Instance = @ptrCast(@alignCast(ptr));
    const p = inst.arena_state.allocator().rawRemap(memory, alignment, new_len, ret_addr) orelse return null;
    inst.allocated = inst.allocated - memory.len + new_len;
    return p;
}

fn runFree(ptr: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
    const inst: *Instance = @ptrCast(@alignCast(ptr));
    inst.arena_state.allocator().rawFree(memory, alignment, ret_addr);
    inst.allocated -= memory.len;
}

/// Adds a command to the activation's completion. Every command goes through here, and none
/// is added once eviction has started (`teardown`): coroutines unwinding at eviction may
/// still cancel timers or activities, but the core has dropped the run, so nothing is sent.
fn emit(inst: *Instance, command: cmd_pb.WorkflowCommand) error{OutOfMemory}!void {
    if (inst.evicting) return;
    // Every caller refuses read-only code itself, naming the call; this keeps the invariant if
    // one is missed.
    if (inst.wio.refuse("a call that emits a command")) return;
    try inst.commands.append(inst.arena(), command);
}

/// Applies an activation and returns its completion. `out` holds the completion's memory
/// until the caller has sent it. RemoveFromCache jobs are the caller's to handle (complete
/// the activation, then `destroy`); they are ignored here.
///
/// The core sends queries in an activation of their own, after any jobs that change state. A
/// query-only activation just answers: coroutines don't run, so it emits no other command and
/// leaves workflow time and state as they were.
pub fn activate(inst: *Instance, act: *const act_pb.WorkflowActivation, out: Allocator) !done_pb.WorkflowActivationCompletion {
    inst.activations += 1;
    const query_only = act.jobs.items.len > 0 and for (act.jobs.items) |job| {
        const v = job.variant orelse break false;
        if (v != .query_workflow) break false;
    } else true;
    const update_only = act.jobs.items.len > 0 and for (act.jobs.items) |job| {
        const v = job.variant orelse break false;
        if (v != .do_update) break false;
    } else true;
    if (!query_only) {
        if (act.timestamp) |ts| inst.now_ns = @as(i96, ts.seconds) * std.time.ns_per_s + ts.nanos;
        inst.history_length = act.history_length;
        inst.history_size_bytes = act.history_size_bytes;
        inst.continue_as_new_suggested = act.continue_as_new_suggested;
        try inst.setDeploymentVersion(act.deployment_version_for_current_task);
    }
    inst.replaying = act.is_replaying;
    inst.commands.clearRetainingCapacity();
    inst.task_failure = null;
    inst.task_failure_type = "UnsupportedJob";
    inst.accepted_update = false;
    // Query answers go to `out`, not the run's arena, so answering doesn't grow the run.
    var answers: std.ArrayList(cmd_pb.WorkflowCommand) = .empty;

    // A run whose coroutine start failed in an earlier activation, and that wasn't evicted,
    // has state no replay would produce: it never runs or answers again.
    const jobs = if (inst.wio.start_failure != null) act.jobs.items[0..0] else act.jobs.items;
    for (jobs) |job| {
        const variant = job.variant orelse {
            // A job of a kind this SDK's protos don't know, from a newer core.
            if (job._unknown_fields.len > 0) inst.task_failure = "workflow activation has a job of a kind this SDK version doesn't know; upgrade the SDK";
            continue;
        };
        switch (variant) {
            .query_workflow => |q| try answers.append(out, .{ .variant = .{ .respond_to_query = try inst.answerQuery(&q, out) } }),
            .initialize_workflow => |init| try inst.initialize(&init, act.run_id),
            .fire_timer => |t| if (inst.timers.fetchRemove(t.seq)) |kv| {
                inst.wio.fireTimer(kv.value);
            } else if (inst.backoffs.fetchRemove(t.seq)) |kv| {
                try inst.retryLocalActivity(kv.value);
            },
            .update_random_seed => |s| inst.wio.setRandomSeed(s.randomness_seed),
            .signal_workflow => |s| try inst.deliverSignal(&s),
            .do_update => |u| try inst.deliverUpdate(&u),
            .resolve_activity => |r| try inst.resolveActivity(&r),
            .notify_has_patch => |p| try inst.notified_patches.put(inst.arena(), try inst.arena().dupe(u8, p.patch_id), {}),
            .resolve_child_workflow_execution_start => |r| try inst.resolveChildStart(&r),
            .resolve_child_workflow_execution => |r| try inst.resolveChild(&r),
            .resolve_nexus_operation_start => |r| try inst.resolveNexusOperationStart(&r),
            .resolve_nexus_operation => |r| try inst.resolveNexusOperation(&r),
            .resolve_signal_external_workflow => |r| try inst.resolveExternal(r.seq, r.failure, switch (r.cause) {
                .SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_EXTERNAL_WORKFLOW_EXECUTION_NOT_FOUND,
                .SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_NAMESPACE_NOT_FOUND,
                => true,
                else => false,
            }),
            .resolve_request_cancel_external_workflow => |r| try inst.resolveExternal(r.seq, r.failure, switch (r.cause) {
                .CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_EXTERNAL_WORKFLOW_EXECUTION_NOT_FOUND,
                .CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_NAMESPACE_NOT_FOUND,
                => true,
                else => false,
            }),
            .cancel_workflow => if (!inst.finished) if (inst.root) |root| inst.wio.cancelTask(root),
            .remove_from_cache => {},
        }
    }

    // Once the workflow has finished, coroutines that are still blocked (handlers, or the root
    // after a handler failed it) never run again: their commands would follow the completion.
    // An update-only activation whose updates were all rejected is not in history, so on
    // replay its coroutines could not run: they run only once an update is accepted (and core
    // rejects anything but rejections from it).
    // The memory check, too, runs only when coroutines ran: an activation that only rejected
    // updates is not in history either, so failing it would fail the task on this worker only.
    if (inst.task_failure == null and inst.wio.start_failure == null and !query_only and !inst.finished) {
        const ran = !update_only or inst.accepted_update;
        if (ran) inst.runUntilQuiet();
        if (inst.wio.start_failure == null) {
            try inst.fillUpdateTraces(out);
            try inst.rejectUnhandledUpdates();
            try inst.finishIfDone(out);
            if (ran and !inst.finished and inst.task_failure == null) try inst.checkMemoryLimit(out);
        }
    }
    // The root cause: another failure (such as "out of memory starting a timer") is usually
    // its consequence.
    if (inst.wio.start_failure) |f| inst.failCoroutineStart(out, f);

    var completion: done_pb.WorkflowActivationCompletion = .{ .run_id = act.run_id };
    if (inst.task_failure) |message| {
        completion.status = .{ .failed = .{ .failure = try failure_mod.application(out, .{ .message = message, .type = inst.task_failure_type }) } };
    } else {
        var success: done_pb.Success = .{};
        if (inst.send_versioning_behavior) success.versioning_behavior = @fromBackingInt(@backingInt(inst.versioning_behavior));
        try success.commands.appendSlice(out, inst.commands.items);
        try success.commands.appendSlice(out, answers.items);
        completion.status = .{ .successful = success };
    }
    return completion;
}

/// Fails the activation for a coroutine `io.async` or `Group.async` couldn't start. The run
/// went on with the function run inline, which no replay would do, so the activation's
/// commands are dropped; the core evicts the run and a later task replays it from history.
fn failCoroutineStart(inst: *Instance, out: Allocator, f: WorkflowIo.StartFailure) void {
    inst.task_failure = std.fmt.allocPrint(out, "{f}; the workflow task fails, and the run is evicted and replayed from history on a later task", .{f}) catch
        "a workflow coroutine could not be started; the workflow task fails";
    inst.task_failure_type = "WorkflowCoroutineStartFailed";
    if (inst.logged_start_failure) return;
    inst.logged_start_failure = true;
    // A warning, as for other failed workflow tasks: the server records the failure and the
    // run is retried from history.
    inst.logger.log(.warn, inst.task_failure.?, .{ .operation = f.operation, .cause = @tagName(f.cause) });
}

/// Fails the workflow task when the run holds more than `memory_limit`. Called once per
/// activation that ran workflow code, after every coroutine blocked, so the figure is the
/// same on every execution of the activation, replay included, apart from the side-effect
/// values a cached run encoded when they first ran (see the namespace docs).
fn checkMemoryLimit(inst: *Instance, out: Allocator) error{OutOfMemory}!void {
    const limit = inst.memory_limit orelse return;
    const used = inst.memoryUsed();
    if (used <= limit) return;
    const stack_bytes = inst.sched.stackBytes();
    const stacks = inst.sched.stackCount();
    const message = try std.fmt.allocPrint(out, "workflow run holds {d} bytes ({d} allocated from ctx.allocator() and the runtime, {d} in {d} coroutine stacks), over worker.Options.workflow_memory_limit = {d}; raise the limit, or keep less state in the run (continue-as-new sooner)", .{ used, inst.allocated, stack_bytes, stacks, limit });
    inst.task_failure = message;
    inst.task_failure_type = "WorkflowMemoryLimitExceeded";
    inst.logger.log(.err, message, .{
        .used = used,
        .allocated = inst.allocated,
        .stack_bytes = stack_bytes,
        .stacks = stacks,
        .limit = limit,
    });
}

fn setDeploymentVersion(inst: *Instance, v: ?proto.coresdk.common.WorkerDeploymentVersion) error{OutOfMemory}!void {
    const next = v orelse {
        inst.deployment_version = null;
        return;
    };
    if (inst.deployment_version) |cur| if (std.mem.eql(u8, cur.deployment_name, next.deployment_name) and std.mem.eql(u8, cur.build_id, next.build_id)) return;
    const a = inst.arena();
    inst.deployment_version = .{ .deployment_name = try a.dupe(u8, next.deployment_name), .build_id = try a.dupe(u8, next.build_id) };
}

// ---------------------------------------------------------------- queries

/// Registers `handler` for queries of type `name`, replacing any handler registered under it.
/// `name` and the handler's context must outlive the run. `context_addr` is the address the
/// context points to, if it is a pointer: one into the calling coroutine's stack is rejected,
/// since that stack is reused once the coroutine returns and queries can arrive after that.
pub fn setQueryHandler(inst: *Instance, name: []const u8, handler: QueryHandler, context_addr: ?usize) error{ OutOfMemory, QueryContextOnStack }!void {
    if (inst.wio.refuse("setQueryHandler")) return;
    if (context_addr) |addr| if (inst.sched.onCurrentStack(addr)) {
        inst.logger.warn("setQueryHandler: the handler context points into the workflow coroutine's stack, which is gone once the coroutine returns; allocate it with ctx.allocator()", .{ .QueryType = name });
        return error.QueryContextOnStack;
    };
    try inst.queries.put(inst.arena(), name, handler);
}

/// Registers `handler` for queries with no handler registered for their type, replacing any
/// earlier one. The handler's context must outlive the run; `context_addr` is checked as in
/// `setQueryHandler`.
pub fn setDynamicQueryHandler(inst: *Instance, handler: DynamicQueryHandler, context_addr: ?usize) error{QueryContextOnStack}!void {
    if (inst.wio.refuse("setDynamicQueryHandler")) return;
    if (context_addr) |addr| if (inst.sched.onCurrentStack(addr)) {
        inst.logger.warn("setDynamicQueryHandler: the handler context points into the workflow coroutine's stack, which is gone once the coroutine returns; allocate it with ctx.allocator()", .{});
        return error.QueryContextOnStack;
    };
    inst.dynamic_query_handler = handler;
}

/// The answer to one query: the handler's encoded result, or a failure for an unknown query
/// type, a handler error, or a call the handler may not make (see `ReadOnlySection`). Memory
/// comes from `out`.
fn answerQuery(inst: *Instance, q: *const act_pb.QueryWorkflow, out: Allocator) !cmd_pb.QueryResult {
    const header: converter.Header = try .fromEntries(out, q.headers.items);
    var section: ReadOnlySection = .{ .memory = out };
    inst.wio.beginReadOnly(&section.io);
    const result = inst.inboundNext().handleQuery(.{
        .query_type = q.query_type,
        .args = .{ .payloads = q.arguments.items, .context = &inst.converter_context },
        .header = header,
        .arena = out,
    });
    inst.wio.endReadOnly(&section.io);
    if (section.io.refused) |call| return .{ .query_id = q.query_id, .variant = .{ .failed = try readOnlyFailure(out, "query handler", q.query_type, call) } };
    const response = result catch |err| {
        if (err == error.OutOfMemory) return error.OutOfMemory;
        if (err != error.QueryNotRegistered) return .{ .query_id = q.query_id, .variant = .{ .failed = try inst.sendFailure(out, &inst.converter_context, err, try section.failureOf(out, err)) } };
        const names = try std.mem.join(out, ", ", inst.queries.keys());
        return .{ .query_id = q.query_id, .variant = .{ .failed = try failure_mod.application(out, .{
            .message = try std.fmt.allocPrint(out, "query type \"{s}\" is not registered on workflow \"{s}\"; registered query types: [{s}]", .{ q.query_type, inst.info.workflow_type, names }),
            .type = "QueryNotRegistered",
        }) } };
    };
    return .{ .query_id = q.query_id, .variant = .{ .succeeded = .{ .response = response } } };
}

/// A query handler or an update validator running now, under the read-only invariant: the
/// SDK changes none of the run state it owns on its behalf (commands, sequence numbers,
/// handler tables, buffered signals and updates, signal channels, the random generator,
/// pending failures, patches and versions, memo, search attributes, sessions, versioning
/// behavior, task failure, and the cancel request and protection of the coroutine a
/// validator runs in), and nothing the run keeps points into the query's memory, because
/// none of it is in history and replay runs no query. Its only effect is the answer, or the
/// update's acceptance or rejection. What the code itself writes to workflow data or to a
/// std.Io primitive (an Event, Condition, Queue, Mutex) can't be intercepted or undone: a
/// write that wakes a coroutine is reported (the wake still happens, as std.Io already
/// changed the primitive), and other writes are the handler's error.
///
/// The section belongs to the code it began in: outside any coroutine (queries, and updates
/// validated on delivery) or in one handler's coroutine (an update validated when a handler
/// registers its update handler). Everything here applies to that code alone, so a validator
/// blocked in a wait that can't be refused leaves the rest of the run as it was, and two
/// sections in different coroutines don't share anything. Every SDK call that would change
/// run state checks `WorkflowIo.refuse` first and does nothing; `emit` refuses as a backstop.
/// What the section's own code may use for itself is kept here, not in the run: the random
/// generator (in `io`), the query's memory (`Context.allocator`), the update being validated,
/// and the failures `ctx.fail` and futures record. Lives on the stack of `answerQuery` or
/// `startUpdate`.
const ReadOnlySection = struct {
    io: WorkflowIo.ReadOnly = .{},
    /// The query's memory, freed once its answer is sent; null in a validator.
    memory: ?Allocator = null,
    /// The update being validated and its handler; null in a query handler.
    update: ?*const Update = null,
    update_handler: ?UpdateHandler = null,
    pending_failure: ?PendingFailure = null,
    last_failure: ?*const Failure = null,

    /// The failure that describes the handler's or validator's error `err`: the one it chose
    /// with `ctx.fail` or `ctx.failWith`, or that of the future whose error it returned, as
    /// for an update handler's error. Memory from `a`.
    fn failureOf(r: *const ReadOnlySection, a: Allocator, err: anyerror) error{OutOfMemory}!Failure {
        switch (err) {
            error.ApplicationFailure => if (r.pending_failure) |p| return p.resolve(a, ""),
            error.ActivityFailed, error.SideEffectFailed, error.ChildWorkflowFailed, error.ChildWorkflowAlreadyStarted, error.ExternalWorkflowNotFound, error.ExternalWorkflowRequestFailed, error.SessionCreationFailed, error.NexusOperationFailed => if (r.last_failure) |f| return f.*,
            else => {},
        }
        return failure_mod.fromError(a, err);
    }
};

/// The read-only section of the code running now, or null.
pub fn readOnly(inst: *const Instance) ?*ReadOnlySection {
    const io_section = inst.wio.currentReadOnly() orelse return null;
    return @alignCast(@fieldParentPtr("io", io_section));
}

/// The memory `Context.allocator` returns: the query's while its handler runs, otherwise the
/// run's.
pub fn allocator(inst: *Instance) Allocator {
    if (inst.readOnly()) |r| if (r.memory) |m| return m;
    return inst.arena();
}

/// The failure of a query, or the rejection of an update, whose handler or validator made a
/// call its `ReadOnlySection` refuses. `what` is "query handler" or "update validator".
fn readOnlyFailure(a: Allocator, what: []const u8, name: []const u8, call: []const u8) error{OutOfMemory}!Failure {
    return failure_mod.application(a, .{
        .message = try std.fmt.allocPrint(a, "{s} for \"{s}\" called {s}, which query handlers and update validators may not call: they only read workflow state, so they must not start activities, timers, child workflows or other commands, or block", .{ what, name, call }),
        .type = "ReadOnlyViolation",
    });
}

// ---------------------------------------------------------------- stack trace query

/// The query type Temporal's UI and CLI send to show where a running workflow waits. The
/// worker answers it for every workflow with `stackTrace`, unless the workflow registered a
/// handler of its own for it.
pub const stack_trace_query = "__stack_trace";

/// The answer to the `__stack_trace` query: a line naming the workflow and how many
/// coroutines it has, then an entry per live coroutine in the order they started, each a
/// line saying which coroutine it is and what it waits on, followed by its call sites in
/// workflow code (see `call_sites`). Once the workflow has closed, the answer says so in one
/// line instead. Memory comes from `out`.
///
/// Read-only: it reads the coroutines' recorded waits and unwinds their stacks, and changes
/// nothing, so a replayed run answers as the run that first got there did. Recording costs
/// each wait a few stores (`WorkflowIo.Wait.Reason` and `Task.waiting_on`), and an activity's
/// wait a copy of up to `max_wait_name` bytes of its type to the waiting coroutine's stack; it
/// allocates nothing, so a long run grows no more for it. Stacks are unwound and debug info is
/// read only when the query arrives. From the read of debug info to the end of the answer,
/// `Scheduler.sdk_work` marks the time as SDK work, which the worker's deadlock detection
/// doesn't count: the first read of debug info in a process can outlast
/// `worker.Options.deadlock_detection_timeout`, and isn't workflow code. The stack trace text
/// of a failed workflow or update is built under the same mark (`failureTraceText`), so a
/// failure's stack trace doesn't count either. When workflow code asks for the answer from
/// its own coroutine (an interceptor calling `WorkflowInbound.Next.handleQuery`), the flag
/// stays clear and the time counts as its own.
fn stackTrace(inst: *Instance, out: Allocator) error{OutOfMemory}![]const u8 {
    var text: std.Io.Writer.Allocating = .init(out);
    errdefer text.deinit();
    inst.writeStackTrace(out, &text.writer) catch |err| switch (err) {
        error.OutOfMemory, error.WriteFailed => return error.OutOfMemory,
    };
    return text.toOwnedSlice() catch return error.OutOfMemory;
}

fn writeStackTrace(inst: *Instance, out: Allocator, w: *std.Io.Writer) (error{OutOfMemory} || std.Io.Writer.Error)!void {
    if (inst.finished) {
        const how = switch (inst.root_outcome orelse return w.print("workflow \"{f}\" has closed: no coroutine is waiting\n", .{call_sites.text(inst.info.workflow_type)})) {
            .completed => "completed",
            .continued_as_new => "continued as new",
            .failed => |err| if (err == error.Canceled) "been canceled" else "failed",
        };
        return w.print("workflow \"{f}\" has {s} and closed: no coroutine is waiting\n", .{ call_sites.text(inst.info.workflow_type), how });
    }
    var count: usize = 0;
    var it = inst.sched.firstLive();
    while (it) |f| : (it = f.live_next) count += 1;
    try w.print("workflow \"{f}\": {d} coroutine{s}\n", .{ call_sites.text(inst.info.workflow_type), count, if (count == 1) "" else "s" });

    // Reading debug info the first time can take seconds, in one call into std; it isn't
    // workflow code, so the deadlock detector doesn't count it. Workflow code can ask for the
    // answer itself (an interceptor calling `WorkflowInbound.Next.handleQuery`); then the
    // time is the workflow code's, and the flag stays clear, as it must while a fiber runs.
    const sdk_work = inst.sched.currentFiber() == null;
    if (sdk_work) inst.sched.sdk_work.store(true, .release);
    defer if (sdk_work) inst.sched.sdk_work.store(false, .release);
    var reader = try call_sites.Reader.init(out);
    defer if (reader) |*r| r.deinit();
    it = inst.sched.firstLive();
    while (it) |f| : (it = f.live_next) {
        try w.print("\ncoroutine {d} \"{f}\", ", .{ f.id, call_sites.text(f.name) });
        try inst.writeCoroutineRole(w, f);
        const task: ?*const WorkflowIo.Task = @ptrCast(@alignCast(f.user));
        const wait = if (task) |t| t.waiting_on else null;
        if (f.state != .blocked) {
            try w.writeAll(", is ready to run\n");
        } else if (wait) |wt| {
            try w.print(", waits on {f}\n", .{wt.reason});
        } else {
            try w.writeAll(", waits\n");
        }
        try call_sites.write(if (reader) |*r| r else null, w, f, "  ");
    }
}

/// Which part of the workflow coroutine `f` runs.
fn writeCoroutineRole(inst: *const Instance, w: *std.Io.Writer, f: *const Scheduler.Fiber) std.Io.Writer.Error!void {
    if (inst.root) |root| if (root.fiber == f) return w.writeAll("the workflow function");
    for (inst.handlers.items) |run| if (run.task.fiber == f) return switch (run.kind) {
        .signal => w.print("signal handler \"{f}\"", .{call_sites.text(run.name)}),
        .update => |u| w.print("update handler \"{f}\" (update ID {f})", .{ call_sites.text(run.name), call_sites.text(u.update.id) }),
    };
    const task: *const WorkflowIo.Task = @ptrCast(@alignCast(f.user orelse return w.writeAll("a coroutine")));
    return w.writeAll(switch (task.kind) {
        .future => "started with io.async or io.concurrent",
        .group => "started in a std.Io.Group",
        .detached => "a coroutine",
    });
}

// ---------------------------------------------------------------- signals

/// Registers `handler` for signals called `name`, replacing any handler registered under it,
/// then starts a handler coroutine for each buffered signal of that name, in arrival order.
/// `name` and the handler's context must outlive the run. `context_addr` is checked as in
/// `setQueryHandler`: a handler coroutine can run after the registering coroutine returned.
pub fn setSignalHandler(inst: *Instance, name: []const u8, handler: SignalHandler, context_addr: ?usize) error{ OutOfMemory, SignalContextOnStack }!void {
    // Registering can start buffered signals' handler coroutines.
    if (inst.wio.refuse("setSignalHandler")) return;
    if (context_addr) |addr| if (inst.sched.onCurrentStack(addr)) {
        inst.logger.warn("setSignalHandler: the handler context points into the workflow coroutine's stack, which is gone once the coroutine returns; allocate it with ctx.allocator()", .{ .SignalName = name });
        return error.SignalContextOnStack;
    };
    try inst.signal_handlers.put(inst.arena(), name, handler);
    var i: usize = 0;
    while (i < inst.buffered_signals.items.len) {
        const signal = inst.buffered_signals.items[i];
        if (!std.mem.eql(u8, signal.name, name)) {
            i += 1;
            continue;
        }
        _ = inst.buffered_signals.orderedRemove(i);
        try inst.startSignalHandler(handler, signal);
    }
}

/// Registers `handler` for signals with no handler registered for their name, replacing any
/// earlier one, then starts a handler coroutine for each buffered signal, in arrival order.
/// The handler's context must outlive the run; `context_addr` is checked as in
/// `setSignalHandler`.
pub fn setDynamicSignalHandler(inst: *Instance, handler: SignalHandler, context_addr: ?usize) error{ OutOfMemory, SignalContextOnStack }!void {
    if (inst.wio.refuse("setDynamicSignalHandler")) return;
    if (context_addr) |addr| if (inst.sched.onCurrentStack(addr)) {
        inst.logger.warn("setDynamicSignalHandler: the handler context points into the workflow coroutine's stack, which is gone once the coroutine returns; allocate it with ctx.allocator()", .{});
        return error.SignalContextOnStack;
    };
    inst.dynamic_signal_handler = handler;
    // Starting a handler only spawns its coroutine, so nothing is buffered meanwhile.
    const buffered = inst.buffered_signals.items;
    inst.buffered_signals = .empty;
    for (buffered) |signal| try inst.startSignalHandler(handler, signal);
}

/// Creates a signal channel and registers it as the handler for signals called `name` (which
/// must outlive the run), as `setSignalHandler` would. Each signal of that name runs the
/// inbound interceptors and `decode` in a handler coroutine of its own, which appends the
/// decoded arguments to the channel; buffered signals of that name start theirs here.
/// `ch` is arena memory the caller allocated.
pub fn createSignalChannel(inst: *Instance, name: []const u8, ch: *SignalChannel, decode: SignalDecode) error{OutOfMemory}!void {
    ch.* = .{ .inst = inst, .name = name };
    if (inst.wio.refuse("signalChannel")) return;
    try inst.signal_channels.append(inst.arena(), ch);
    inst.setSignalHandler(name, .{ .context = ch, .decode = decode, .call = SignalChannel.handlerCall }, null) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        error.SignalContextOnStack => unreachable, // no context address is checked
    };
}

/// The channel registered for signals called `name` whose arguments `decode` decodes, or null
/// if another handler (or a channel of other argument types) handles them, or none does.
pub fn signalChannelFor(inst: *Instance, name: []const u8, decode: SignalDecode) ?*SignalChannel {
    const handler = inst.signal_handlers.get(name) orelse return null;
    if (handler.call != SignalChannel.handlerCall or handler.decode != decode) return null;
    return @ptrCast(@alignCast(@constCast(handler.context)));
}

/// Sets the versioning behavior sent with each completion from now on.
pub fn setVersioningBehavior(inst: *Instance, behavior: workflow.VersioningBehavior) void {
    if (inst.wio.refuse("setVersioningBehavior")) return;
    inst.versioning_behavior = behavior;
}

/// Unregisters the handler (or channel) for signals called `name`, if any. Later signals of
/// that name go to the dynamic handler, or are buffered. Handler coroutines already started
/// run to completion, and a channel keeps the signals it already took.
pub fn removeSignalHandler(inst: *Instance, name: []const u8) void {
    if (inst.wio.refuse("removeSignalHandler")) return;
    _ = inst.signal_handlers.orderedRemove(name);
}

/// Unregisters the dynamic signal handler, if any: later signals with no handler of their own
/// are buffered. Handler coroutines already started run to completion.
pub fn removeDynamicSignalHandler(inst: *Instance) void {
    if (inst.wio.refuse("removeDynamicSignalHandler")) return;
    inst.dynamic_signal_handler = null;
}

/// The signals nothing has taken: buffered ones and those still unread in a channel, oldest
/// first. Allocated in `a`.
fn unhandledSignals(inst: *Instance, a: Allocator) error{OutOfMemory}![]*const Signal {
    var all: std.ArrayList(*const Signal) = .empty;
    try all.appendSlice(a, inst.buffered_signals.items);
    for (inst.signal_channels.items) |ch| for (ch.items.items[ch.head..]) |item| try all.append(a, item.signal);
    // A signal on its way to a channel, whose handler coroutine hasn't appended it yet.
    for ([_][]const *HandlerRun{ inst.handlers.items, inst.held_handlers.items }) |runs| for (runs) |run| switch (run.kind) {
        .signal => |s| if (!run.taken and SignalChannel.isHandler(s.handler)) try all.append(a, s.signal),
        .update => {},
    };
    std.mem.sort(*const Signal, all.items, {}, struct {
        fn lessThan(_: void, l: *const Signal, r: *const Signal) bool {
            return l.arrival < r.arrival;
        }
    }.lessThan);
    return all.items;
}

/// The distinct names of the signals nothing has taken (buffered, or unread in a channel), in
/// the order each name first arrived. Allocated in `a`; the names are arena memory of the run.
pub fn unhandledSignalNames(inst: *Instance, a: Allocator) error{OutOfMemory}![]const []const u8 {
    var names: std.ArrayList([]const u8) = .empty;
    outer: for (try inst.unhandledSignals(a)) |signal| {
        for (names.items) |n| if (std.mem.eql(u8, n, signal.name)) continue :outer;
        try names.append(a, signal.name);
    }
    return names.items;
}

/// Copies a SignalWorkflow job into the arena and starts its handler, or buffers it until a
/// handler for its name is registered.
fn deliverSignal(inst: *Instance, job: *const act_pb.SignalWorkflow) !void {
    if (inst.finished) return;
    if (std.mem.eql(u8, job.signal_name, session_opened_signal)) return inst.openSession(job.input.items);
    const a = inst.arena();
    const args = try a.alloc(converter.Payload, job.input.items.len);
    for (job.input.items, args) |p, *d| d.* = try p.dupe(a);
    const signal = try a.create(Signal);
    signal.* = .{ .name = try a.dupe(u8, job.signal_name), .args = args, .header = try .fromEntries(a, job.headers.items), .arrival = inst.signals_delivered };
    inst.signals_delivered += 1;
    if (inst.signal_handlers.get(signal.name)) |handler| return inst.startSignalHandler(handler, signal);
    if (inst.dynamic_signal_handler) |handler| return inst.startSignalHandler(handler, signal);
    try inst.buffered_signals.append(a, signal);
}

/// Handles the opened signal of a session's creation activity: the session that is still
/// waiting with that ID gets its host. Other signals (a stale or duplicate one) are dropped.
fn openSession(inst: *Instance, payloads: []const converter.Payload) error{OutOfMemory}!void {
    const a = inst.arena();
    const opened = (converter.fromPayloads(struct { SessionOpened }, a, payloads) catch |err| {
        if (err == error.OutOfMemory) return error.OutOfMemory;
        inst.logger.warn("dropping a session opened signal whose argument doesn't decode", .{ .error_name = err });
        return;
    })[0];
    for (inst.sessions.items) |s| if (!s.opened and !s.closed and std.mem.eql(u8, s.id, opened.session_id)) {
        s.task_queue = try a.dupe(u8, opened.task_queue);
        s.host_name = try a.dupe(u8, opened.host_name);
        s.opened = true;
        return;
    };
}

/// Spawns the signal's handler coroutine, which runs the inbound interceptors and then the
/// handler. A signal whose arguments don't decode is dropped there, the same way on replay.
fn startSignalHandler(inst: *Instance, handler: SignalHandler, signal: *const Signal) error{OutOfMemory}!void {
    const run = try inst.arena().create(HandlerRun);
    run.* = .{ .name = signal.name, .kind = .{ .signal = .{ .handler = handler, .signal = signal } } };
    try inst.startHandler(run);
}

// ---------------------------------------------------------------- updates

/// Registers `handler` for updates called `name`, replacing any handler registered under it,
/// then delivers each buffered update of that name to it, in arrival order. `name` and the
/// handler's context must outlive the run; `context_addr` is checked as in `setQueryHandler`.
pub fn setUpdateHandler(inst: *Instance, name: []const u8, handler: UpdateHandler, context_addr: ?usize) error{ OutOfMemory, UpdateContextOnStack }!void {
    // Registering can validate and accept buffered updates and start their coroutines.
    if (inst.wio.refuse("setUpdateHandler")) return;
    if (context_addr) |addr| if (inst.sched.onCurrentStack(addr)) {
        inst.logger.warn("setUpdateHandler: the handler context points into the workflow coroutine's stack, which is gone once the coroutine returns; allocate it with ctx.allocator()", .{ .UpdateName = name });
        return error.UpdateContextOnStack;
    };
    try inst.update_handlers.put(inst.arena(), name, handler);
    var i: usize = 0;
    while (i < inst.buffered_updates.items.len) {
        const update = inst.buffered_updates.items[i];
        if (!std.mem.eql(u8, update.name, name)) {
            i += 1;
            continue;
        }
        _ = inst.buffered_updates.orderedRemove(i);
        try inst.startUpdate(handler, update);
    }
}

/// Copies a DoUpdate job into the arena and starts it, or buffers it until a handler for its
/// name is registered. Once the workflow has finished nothing would run its handler, so the
/// update is rejected.
fn deliverUpdate(inst: *Instance, job: *const act_pb.DoUpdate) !void {
    const a = inst.arena();
    const protocol_instance_id = try a.dupe(u8, job.protocol_instance_id);
    if (inst.finished) return inst.respondToUpdate(protocol_instance_id, .{ .rejected = try failure_mod.application(a, .{
        .message = try std.fmt.allocPrint(a, "update \"{s}\" arrived after workflow \"{s}\" finished", .{ job.name, inst.info.workflow_type }),
        .type = "WorkflowFinished",
    }) });
    const args = try a.alloc(converter.Payload, job.input.items.len);
    for (job.input.items, args) |p, *d| d.* = try p.dupe(a);
    const update = try a.create(Update);
    update.* = .{
        .id = try a.dupe(u8, job.id),
        .protocol_instance_id = protocol_instance_id,
        .name = try a.dupe(u8, job.name),
        .args = args,
        .header = try .fromEntries(a, job.headers.items),
        .run_validator = job.run_validator,
    };
    if (inst.update_handlers.get(update.name)) |handler| return inst.startUpdate(handler, update);
    if (inst.dynamic_update_handler) |handler| return inst.startUpdate(handler, update);
    try inst.buffered_updates.append(a, update);
}

/// Registers `handler` for updates with no handler registered for their name, replacing any
/// earlier one, then delivers each buffered update to it, in arrival order. The handler's
/// context must outlive the run; `context_addr` is checked as in `setUpdateHandler`.
pub fn setDynamicUpdateHandler(inst: *Instance, handler: UpdateHandler, context_addr: ?usize) error{ OutOfMemory, UpdateContextOnStack }!void {
    if (inst.wio.refuse("setDynamicUpdateHandler")) return;
    if (context_addr) |addr| if (inst.sched.onCurrentStack(addr)) {
        inst.logger.warn("setDynamicUpdateHandler: the handler context points into the workflow coroutine's stack, which is gone once the coroutine returns; allocate it with ctx.allocator()", .{});
        return error.UpdateContextOnStack;
    };
    inst.dynamic_update_handler = handler;
    // A validator can't register handlers, so nothing is buffered meanwhile.
    const buffered = inst.buffered_updates.items;
    inst.buffered_updates = .empty;
    for (buffered) |update| try inst.startUpdate(handler, update);
}

/// Validates the update through the inbound interceptors, which end in decoding its
/// arguments and running its validator: an error rejects it. Skipped on replay, where every
/// update in history was accepted. Otherwise accepts it and spawns its handler coroutine.
fn startUpdate(inst: *Instance, handler: UpdateHandler, update: *const Update) error{OutOfMemory}!void {
    const a = inst.arena();
    if (update.run_validator) {
        var section: ReadOnlySection = .{ .update = update, .update_handler = handler };
        inst.wio.beginReadOnly(&section.io);
        const result = inst.inboundNext().validateUpdate(update.input(&inst.converter_context));
        inst.wio.endReadOnly(&section.io);
        if (section.io.refused) |call| return inst.respondToUpdate(update.protocol_instance_id, .{ .rejected = try readOnlyFailure(a, "update validator", update.name, call) });
        result catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            else => return inst.respondToUpdate(update.protocol_instance_id, .{ .rejected = try inst.sendFailure(a, &inst.converter_context, err, try section.failureOf(a, err)) }),
        };
    }
    try inst.respondToUpdate(update.protocol_instance_id, .{ .accepted = .{} });
    inst.accepted_update = true;
    const run = try a.create(HandlerRun);
    run.* = .{ .name = update.name, .kind = .{ .update = .{ .handler = handler, .update = update } } };
    try inst.startHandler(run);
}

/// Rejects the updates no handler was registered for while the activation's coroutines ran,
/// in arrival order. Core needs every update accepted or rejected in the completion of the
/// activation that delivered it.
fn rejectUnhandledUpdates(inst: *Instance) error{OutOfMemory}!void {
    if (inst.buffered_updates.items.len == 0) return;
    const a = inst.arena();
    const names = try std.mem.join(a, ", ", inst.update_handlers.keys());
    for (inst.buffered_updates.items) |update| try inst.respondToUpdate(update.protocol_instance_id, .{ .rejected = try failure_mod.application(a, .{
        .message = try std.fmt.allocPrint(a, "update \"{s}\" is not registered on workflow \"{s}\"; registered updates: [{s}]", .{ update.name, inst.info.workflow_type, names }),
        .type = "UpdateNotRegistered",
    }) });
    inst.buffered_updates.clearRetainingCapacity();
}

fn respondToUpdate(inst: *Instance, protocol_instance_id: []const u8, response: cmd_pb.UpdateResponse.response_union) error{OutOfMemory}!void {
    try inst.emit(.{ .variant = .{ .update_response = .{
        .protocol_instance_id = protocol_instance_id,
        .response = response,
    } } });
}

/// Spawns a handler coroutine for `run` and tracks it in `handlers`, or holds it until
/// `releaseHandlers`.
fn startHandler(inst: *Instance, run: *HandlerRun) error{OutOfMemory}!void {
    if (inst.holding_handlers) return inst.held_handlers.append(inst.arena(), run);
    try inst.handlers.ensureUnusedCapacity(inst.arena(), 1);
    run.task = inst.wio.spawn(@tagName(run.kind), handlerMain, .{ inst, run }) catch |err| {
        inst.logger.warn("starting a handler coroutine failed", .{ .HandlerName = run.name, .error_name = err });
        return error.OutOfMemory;
    };
    inst.handlers.appendAssumeCapacity(run);
}

/// Starts the handler runs held since a struct workflow's initialization, in arrival order;
/// later ones start on delivery. Called by the struct workflow's `invoke` before `run`.
pub fn releaseHandlers(inst: *Instance) error{OutOfMemory}!void {
    if (!inst.holding_handlers) return;
    inst.holding_handlers = false;
    const held = inst.held_handlers.items;
    inst.held_handlers = .empty;
    for (held) |run| try inst.startHandler(run);
}

fn handlerMain(inst: *Instance, run: *HandlerRun) void {
    switch (run.kind) {
        .signal => |s| {
            const result = inst.inboundNext().handleSignal(.{ .signal_name = s.signal.name, .args = .{ .payloads = s.signal.args, .context = &inst.converter_context }, .header = s.signal.header });
            inst.removeHandler(run);
            result catch |err| {
                if (inst.evicting) return;
                if (inst.failure_stack_traces) run.trace = failure_mod.captureTrace(inst.arena(), @errorReturnTrace()) catch &.{};
                if (inst.root_outcome == null) {
                    inst.root_outcome = .{ .failed = err };
                    inst.failed_handler = run;
                } else {
                    inst.logger.warn("a signal handler failed after the workflow finished; its error is ignored", .{ .SignalName = run.name, .error_name = err });
                }
            };
        },
        .update => |u| {
            const a = inst.arena();
            const result = inst.inboundNext().executeUpdate(u.update.input(&inst.converter_context));
            inst.removeHandler(run);
            if (inst.evicting) return;
            const response: cmd_pb.UpdateResponse.response_union = if (result) |payload|
                .{ .completed = payload }
            else |err| rejected: {
                // The update stays accepted and unanswered; the retried task runs it again.
                if (inst.failsTask(err)) {
                    inst.failTaskWith("update handler \"{s}\" (update ID {s})", .{ u.update.name, u.update.id }, err);
                    return;
                }
                if (inst.failure_stack_traces) run.trace = failure_mod.captureTrace(a, @errorReturnTrace()) catch &.{};
                const rejected = updateFailure(a, run, err) catch {
                    inst.task_failure = "out of memory rejecting an update whose handler failed";
                    return;
                };
                const addresses: []const usize = if (builtFromError(run, err)) run.trace else &.{};
                if (addresses.len > 0 or inst.failure_converter != null) inst.update_traces.append(a, .{ .protocol_instance_id = u.update.protocol_instance_id, .addresses = addresses, .err = err }) catch {
                    inst.task_failure = "out of memory rejecting an update whose handler failed";
                    return;
                };
                break :rejected .{ .rejected = rejected };
            };
            inst.respondToUpdate(u.update.protocol_instance_id, response) catch {
                inst.task_failure = "out of memory completing an update";
            };
        },
    }
}

/// An update handler's error as its rejection: the failure it chose with `ctx.fail` or
/// `ctx.failWith`, or the failure of the activity or child workflow whose error it returned.
fn updateFailure(a: Allocator, run: *const HandlerRun, err: anyerror) error{OutOfMemory}!Failure {
    switch (err) {
        error.ApplicationFailure => if (run.pending_failure) |p| return p.resolve(a, ""),
        error.ActivityFailed, error.SideEffectFailed, error.ChildWorkflowFailed, error.ChildWorkflowAlreadyStarted, error.ExternalWorkflowNotFound, error.ExternalWorkflowRequestFailed, error.SessionCreationFailed, error.NexusOperationFailed => if (run.last_failure) |f| return f.*,
        else => {},
    }
    return failure_mod.fromError(a, err);
}

/// Whether `updateFailure` builds the rejection from `err` (its name, or `ctx.fail`), as
/// opposed to passing on a failure unchanged.
fn builtFromError(run: *const HandlerRun, err: anyerror) bool {
    switch (err) {
        error.ApplicationFailure => if (run.pending_failure) |p| return p == .application,
        error.ActivityFailed, error.SideEffectFailed, error.ChildWorkflowFailed, error.ChildWorkflowAlreadyStarted, error.ExternalWorkflowNotFound, error.ExternalWorkflowRequestFailed, error.SessionCreationFailed, error.NexusOperationFailed => if (run.last_failure != null) return false,
        else => {},
    }
    return true;
}

/// Finishes the update rejections `update_traces` lists, among the commands so far: fills in
/// their stack traces, then passes each through the failure converter. Runs outside the
/// coroutines. The text and the converter's memory go to `out`, the activation's memory,
/// like query answers: they would otherwise count toward the memory limit in a cached run
/// but not a replayed one, where the rejection isn't rebuilt.
fn fillUpdateTraces(inst: *Instance, out: Allocator) error{OutOfMemory}!void {
    if (inst.update_traces.items.len == 0) return;
    defer inst.update_traces.clearRetainingCapacity();
    for (inst.commands.items) |*cmd| {
        const variant = if (cmd.variant) |*v| v else continue;
        const response = switch (variant.*) {
            .update_response => |*r| r,
            else => continue,
        };
        const rejected = if (response.response) |*r| switch (r.*) {
            .rejected => |*f| f,
            else => continue,
        } else continue;
        for (inst.update_traces.items) |t| if (std.mem.eql(u8, t.protocol_instance_id, response.protocol_instance_id)) {
            if (t.addresses.len > 0) rejected.stack_trace = try inst.failureTraceText(out, t.addresses);
            rejected.* = try inst.sendFailure(out, &inst.converter_context, t.err, rejected.*);
        };
    }
}

/// The text of a failure's stack trace (`failure.addressesText`), built as SDK work: as for
/// the stack trace query (`writeStackTrace`), the first read of debug info in a process can
/// outlast `worker.Options.deadlock_detection_timeout` and isn't workflow code, so
/// `Scheduler.sdk_work` marks the time and the worker's deadlock detection doesn't count it.
/// Called from inside a coroutine, the time stays the workflow code's and the flag clear, as
/// it must while a fiber runs. Memory comes from `out`.
fn failureTraceText(inst: *Instance, out: Allocator, addresses: []const usize) error{OutOfMemory}![]const u8 {
    const sdk_work = inst.sched.currentFiber() == null;
    if (sdk_work) inst.sched.sdk_work.store(true, .release);
    defer if (sdk_work) inst.sched.sdk_work.store(false, .release);
    return failure_mod.addressesText(out, addresses);
}

/// The handler run executing now, or null in the workflow function, the coroutines it
/// started, and outside any coroutine. A coroutine that a handler starts with `io.async` or a
/// `Group` isn't matched: its failure details go to the workflow function's.
fn currentHandler(inst: *Instance) ?*HandlerRun {
    const fiber = inst.sched.currentFiber() orelse return null;
    for (inst.handlers.items) |run| if (run.task.fiber == fiber) return run;
    return null;
}

/// The update being validated, or else the update whose handler is running now. The
/// validator comes first: a handler that registers an update handler validates buffered
/// updates inline in its own coroutine, and its read-only section names the update only for
/// that coroutine.
pub fn currentUpdate(inst: *Instance) ?workflow.UpdateInfo {
    if (inst.readOnly()) |r| if (r.update) |u| return .{ .id = u.id, .name = u.name };
    const run = inst.currentHandler() orelse return null;
    return switch (run.kind) {
        .update => |u| .{ .id = u.update.id, .name = u.update.name },
        .signal => null,
    };
}

/// Records `Context.fail`'s or `Context.failWith`'s failure for the running coroutine: its
/// handler run's, or the workflow function's.
pub fn setPendingFailure(inst: *Instance, a: PendingFailure) void {
    if (inst.readOnly()) |r| {
        r.pending_failure = a;
    } else if (inst.currentHandler()) |run| run.pending_failure = a else inst.ctx.pending_failure = a;
}

/// Records the failure a future's `get` returned for the running coroutine, as
/// `setPendingFailure` does. `f` must live as long as the run (arena memory).
pub fn setLastFailure(inst: *Instance, f: ?*const Failure) void {
    if (inst.readOnly()) |r| {
        r.last_failure = f;
    } else if (inst.currentHandler()) |run| run.last_failure = f else inst.ctx.last_failure = f;
}

fn removeHandler(inst: *Instance, run: *HandlerRun) void {
    for (inst.handlers.items, 0..) |r, i| if (r == run) {
        _ = inst.handlers.orderedRemove(i);
        return;
    };
}

/// Runs coroutines until all are blocked and no awaited condition holds. Conditions are
/// checked in the order they were awaited, one wake-up at a time, so a coroutine woken by
/// its condition can change the state another condition reads before that one is checked.
fn runUntilQuiet(inst: *Instance) void {
    while (true) {
        inst.wio.run();
        const met = for (inst.conditions.items, 0..) |c, i| {
            if (c.check(c.context)) break i;
        } else return;
        const c = inst.conditions.orderedRemove(met);
        c.met = true;
        inst.wio.wake(c.wait);
    }
}

/// A coroutine blocked until a predicate over workflow state holds (`Context.await`).
pub const Condition = struct {
    check: *const fn (context: *const anyopaque) bool,
    context: *const anyopaque,
    wait: *WorkflowIo.Wait,
    met: bool = false,
};

/// Blocks the calling coroutine until `check(context)` returns true. With a timeout, also
/// returns when a durable timer fires; returns whether the condition was met.
pub fn awaitCondition(inst: *Instance, check: *const fn (context: *const anyopaque) bool, context: *const anyopaque, timeout: ?std.Io.Duration) error{ Canceled, OutOfMemory }!bool {
    if (check(context)) return true;
    const timed = if (timeout) |d| d.nanoseconds > 0 else false;
    if (timeout != null and !timed) return false;
    if (inst.wio.refuse(if (timeout == null) "await" else "awaitTimeout")) return error.Canceled;
    var wait = inst.wio.newWaitPoint(if (timed)
        .{ .what = "a condition, with a timeout (Context.awaitTimeout)", .duration = timeout.? }
    else
        .{ .what = "a condition (Context.await)" });
    var cond: Condition = .{ .check = check, .context = context, .wait = &wait };
    try inst.conditions.append(inst.arena(), &cond);
    defer if (!cond.met) inst.removeCondition(&cond);

    var timer: WorkflowIo.Timer = .{ .wait = &wait };
    if (timed) hostStartTimer(inst, &timer, timeout.?);
    defer if (timed and !timer.fired) hostCancelTimer(inst, &timer);

    try inst.wio.waitFor(&wait, true);
    return cond.met;
}

fn removeCondition(inst: *Instance, cond: *Condition) void {
    for (inst.conditions.items, 0..) |c, i| if (c == cond) {
        _ = inst.conditions.orderedRemove(i);
        return;
    };
}

fn initialize(inst: *Instance, init: *const act_pb.InitializeWorkflow, run_id: []const u8) !void {
    const a = inst.arena();
    inst.info = .{
        .workflow_id = try a.dupe(u8, init.workflow_id),
        .run_id = try a.dupe(u8, run_id),
        .workflow_type = try a.dupe(u8, init.workflow_type),
        .task_queue = inst.task_queue,
        .namespace = inst.namespace,
        .attempt = @intCast(@max(init.attempt, 0)),
        .cron_schedule = try a.dupe(u8, init.cron_schedule),
        .first_run_id = try a.dupe(u8, init.first_execution_run_id),
        .continued_run_id = try a.dupe(u8, init.continued_from_execution_run_id),
        .execution_timeout = proto_time.positiveDurationFromProto(init.workflow_execution_timeout),
        .run_timeout = proto_time.positiveDurationFromProto(init.workflow_run_timeout),
        .task_timeout = proto_time.positiveDurationFromProto(init.workflow_task_timeout),
        .start_time = proto_time.timestampFromProto(init.start_time),
    };
    if (init.parent_workflow_info) |p| inst.info.parent = .{
        .namespace = try a.dupe(u8, p.namespace),
        .workflow_id = try a.dupe(u8, p.workflow_id),
        .run_id = try a.dupe(u8, p.run_id),
    };
    if (init.root_workflow) |r| inst.info.root = .{
        .namespace = if (inst.info.parent) |p| p.namespace else inst.namespace,
        .workflow_id = try a.dupe(u8, r.workflow_id),
        .run_id = try a.dupe(u8, r.run_id),
    };
    if (init.retry_policy) |p| inst.info.retry_policy = try zigRetryPolicy(a, p);
    if (init.priority) |p| inst.info.priority = .{
        .priority_key = p.priority_key,
        .fairness_key = try a.dupe(u8, p.fairness_key),
        .fairness_weight = p.fairness_weight,
    };
    inst.info.original_run_id = try a.dupe(u8, init.original_execution_run_id);
    if (init.last_completion_result) |r| if (r.payloads.items.len > 0) {
        inst.last_completion_result = try r.payloads.items[0].dupe(a);
    };
    if (init.continued_failure) |f| {
        const copy = try a.create(Failure);
        copy.* = try f.dupe(a);
        inst.previous_run_failure = copy;
    }
    inst.logger = try inst.logger.with(a, .{
        .WorkflowType = inst.info.workflow_type,
        .WorkflowID = inst.info.workflow_id,
        .RunID = inst.info.run_id,
    });
    inst.metrics = try inst.metrics.with(.{
        .namespace = inst.namespace,
        .task_queue = inst.task_queue,
        .workflow_type = inst.info.workflow_type,
    });
    inst.metrics.replaying = &inst.replaying;
    if (init.memo) |m| {
        const fields = try a.alloc(converter.Field, m.fields.items.len);
        for (m.fields.items, fields) |entry, *f| f.* = .{
            .name = try a.dupe(u8, entry.key),
            .value = if (entry.value) |v| try v.dupe(a) else .{},
        };
        std.mem.sort(converter.Field, fields, {}, fieldLessThan);
        inst.memo = fields;
    }
    inst.search_attributes = try (try sa.View.fromProto(a, init.search_attributes)).dupe(a);
    const args = try a.alloc(converter.Payload, init.arguments.items.len);
    for (init.arguments.items, args) |p, *d| d.* = try p.dupe(a);
    inst.args = args;
    inst.header = try .fromEntries(a, init.headers.items);
    try propagation.extractAll(inst.propagators, a, inst.header, &inst.propagated, "the workflow start");
    inst.converter_context.workflow_id = inst.info.workflow_id;
    inst.converter_context.workflow_type = inst.info.workflow_type;
    if (inst.previous_run_failure) |f| if (inst.failure_converter) |c| {
        const converted = try a.create(Failure);
        converted.* = try c.fromFailure(a, &inst.converter_context, f.*);
        inst.previous_run_failure = converted;
    };
    var inbound: std.ArrayList(interceptor.WorkflowInbound) = .empty;
    var outbound: std.ArrayList(interceptor.WorkflowOutbound) = .empty;
    for (inst.interceptors) |w| {
        const run = try w.interceptWorkflow(&inst.ctx);
        if (run.inbound) |i| try inbound.append(a, i);
        if (run.outbound) |o| try outbound.append(a, o);
    }
    inst.inbound = inbound.items;
    inst.outbound = outbound.items;
    inst.wio.setRandomSeed(init.randomness_seed);
    // Registered before the activation's signals and updates, which core delivers after
    // InitializeWorkflow, so they reach the declared handlers.
    if (inst.entry.init_run) |init_run| {
        inst.holding_handlers = true;
        inst.run_state = try init_run(&inst.ctx);
    }
    inst.root = try inst.wio.spawn("workflow", rootMain, .{inst});
}

fn rootMain(inst: *Instance) void {
    const started = inst.inboundNext().executeWorkflow(.{
        .workflow_type = inst.info.workflow_type,
        .args = .{ .payloads = inst.args, .context = &inst.converter_context },
        .header = inst.header,
    });
    const outcome: RootOutcome = if (started) |result|
        .{ .completed = result }
    else |err| if (err == error.ContinueAsNew and inst.continue_as_new != null)
        .continued_as_new
    else blk: {
        if (inst.failure_stack_traces) inst.root_trace = failure_mod.captureTrace(inst.arena(), @errorReturnTrace()) catch &.{};
        break :blk .{ .failed = err };
    };
    // A signal handler's error may have finished the workflow first.
    if (inst.root_outcome == null) inst.root_outcome = outcome;
    inst.root = null; // the task frees itself when this returns
}

/// Whether the worker's or the workflow definition's `task_failure_errors` list `err`, so
/// that the workflow function or a signal or update handler returning it fails the workflow
/// task. The workflow function's and signal handlers' error.OutOfMemory fails it too (see
/// `finishIfDone`); an unlisted one from an update handler rejects the update.
fn failsTask(inst: *const Instance, err: anyerror) bool {
    return std.mem.findScalar(anyerror, inst.task_failure_errors, err) != null or
        std.mem.findScalar(anyerror, inst.entry.task_failure_errors, err) != null;
}

/// Fails the workflow task because `what` returned `err`, which `failsTask` picks. The
/// failure's type is the error's name.
fn failTaskWith(inst: *Instance, comptime what: []const u8, args: anytype, err: anyerror) void {
    inst.task_failure_type = @errorName(err);
    if (err == error.OutOfMemory) {
        inst.task_failure = "out of memory running the workflow";
        return;
    }
    inst.task_failure = std.fmt.allocPrint(inst.arena(), what ++ " of workflow \"{s}\" returned error.{t}, which task_failure_errors lists (worker.Options.workflow_task_failure_errors or the workflow's DefOptions), so the workflow task fails and is retried; the workflow keeps running", args ++ .{ inst.info.workflow_type, err }) catch "workflow code returned an error that task_failure_errors lists; the workflow task fails and is retried";
}

fn finishIfDone(inst: *Instance, out: Allocator) !void {
    if (inst.finished) return;
    const outcome = inst.root_outcome orelse return;
    inst.finished = true;
    // Canceled always cancels: the workflow accepted its cancellation.
    if (outcome == .failed and outcome.failed != error.Canceled and (outcome.failed == error.OutOfMemory or inst.failsTask(outcome.failed))) {
        if (inst.failed_handler) |run|
            inst.failTaskWith("signal handler \"{s}\"", .{run.name}, outcome.failed)
        else
            inst.failTaskWith("the workflow function", .{}, outcome.failed);
        return;
    }
    const a = inst.arena();
    const unhandled = try inst.unhandledSignals(a);
    if (unhandled.len > 0) {
        const names = try std.mem.join(a, ", ", try inst.unhandledSignalNames(a));
        inst.logger.warn("the workflow finished with signals that no handler took or that are unread in a signal channel; they are dropped", .{ .count = unhandled.len, .SignalNames = names });
    }
    if (inst.held_handlers.items.len > 0) {
        var names: std.ArrayList(u8) = .empty;
        for (inst.held_handlers.items, 0..) |run, i| {
            if (i > 0) try names.appendSlice(a, ", ");
            try names.appendSlice(a, run.name);
        }
        inst.logger.warn("the workflow finished before its run method was called (its arguments didn't decode, or an interceptor didn't call next); its signals and updates are dropped", .{ .count = inst.held_handlers.items.len, .HandlerNames = names.items });
    }
    switch (outcome) {
        .completed, .continued_as_new => try inst.warnUnfinishedHandlers(),
        .failed => |err| if (err == error.Canceled) try inst.warnUnfinishedHandlers(),
    }
    switch (outcome) {
        .completed => |result| try inst.emit(.{ .variant = .{
            .complete_workflow_execution = .{ .result = result },
        } }),
        .continued_as_new => try inst.emit(.{ .variant = .{
            .continue_as_new_workflow_execution = inst.continue_as_new.?,
        } }),
        .failed => |err| switch (err) {
            error.Canceled => try inst.emit(.{ .variant = .{ .cancel_workflow_execution = .{} } }),
            else => try inst.emit(.{ .variant = .{
                .fail_workflow_execution = .{ .failure = try inst.workflowFailure(out, err) },
            } }),
        },
    }
}

/// Logs one warning naming the signal and update handlers still running as the workflow
/// completes, is canceled, or continues as new, except those registered with the
/// `abandon` unfinished policy. Their coroutines never resume, so their work is cut short.
fn warnUnfinishedHandlers(inst: *Instance) error{OutOfMemory}!void {
    const a = inst.arena();
    var updates: std.ArrayList(u8) = .empty;
    var signals: std.ArrayList([]const u8) = .empty;
    for (inst.handlers.items) |run| switch (run.kind) {
        .update => |u| if (u.handler.unfinished_policy == .warn_and_abandon) {
            if (updates.items.len > 0) try updates.appendSlice(a, ", ");
            try updates.print(a, "{s} (ID {s})", .{ u.update.name, u.update.id });
        },
        .signal => |s| if (s.handler.unfinished_policy == .warn_and_abandon) {
            for (signals.items) |n| {
                if (std.mem.eql(u8, n, s.signal.name)) break;
            } else try signals.append(a, s.signal.name);
        },
    };
    if (updates.items.len == 0 and signals.items.len == 0) return;
    inst.logger.warn("the workflow finished while signal or update handlers were still running; they are abandoned. Wait for them before returning with ctx.await(ctx, Context.allHandlersFinished), or register a handler whose work may be cut short with unfinished_policy .abandon", .{
        .UpdateHandlers = updates.items,
        .SignalNames = try std.mem.join(a, ", ", signals.items),
    });
}

/// The failure that fails the workflow with `err`. Its stack trace text goes to `out`, the
/// activation's memory, like an update rejection's (`fillUpdateTraces`); the rest to the
/// run's arena.
fn workflowFailure(inst: *Instance, out: Allocator, err: anyerror) !Failure {
    if (err == error.ContinueAsNew) return inst.builtWorkflowFailure(out, err);
    return inst.sendFailure(out, &inst.converter_context, err, try inst.builtWorkflowFailure(out, err));
}

/// `workflowFailure` as the SDK builds it, before the failure converter. The failure of an
/// error.ContinueAsNew misuse is the SDK's own report.
fn builtWorkflowFailure(inst: *Instance, out: Allocator, err: anyerror) !Failure {
    const a = inst.arena();
    const pending, const last, const addresses = if (inst.failed_handler) |run|
        .{ run.pending_failure, run.last_failure, run.trace }
    else
        .{ inst.ctx.pending_failure, inst.ctx.last_failure, inst.root_trace };
    if (pending) |p| return p.resolve(a, try inst.failureTraceText(out, addresses));
    switch (err) {
        error.ActivityFailed, error.SideEffectFailed, error.ChildWorkflowFailed, error.ChildWorkflowAlreadyStarted, error.ExternalWorkflowNotFound, error.ExternalWorkflowRequestFailed, error.SessionCreationFailed, error.NexusOperationFailed => if (last) |f| return f.*,
        // Only the workflow function's return of ctx.continueAsNew's error continues as new.
        error.ContinueAsNew => return failure_mod.application(a, .{
            .message = if (inst.continue_as_new == null)
                try std.fmt.allocPrint(a, "workflow \"{s}\" returned error.ContinueAsNew without calling ctx.continueAsNew; return ctx.continueAsNew(Workflow, options, args) to continue as new", .{inst.info.workflow_type})
            else
                try std.fmt.allocPrint(a, "a signal handler of workflow \"{s}\" returned error.ContinueAsNew; only the workflow function can continue as new, by returning ctx.continueAsNew(...)", .{inst.info.workflow_type}),
            .type = "ContinueAsNew",
        }),
        else => {},
    }
    var f = try failure_mod.fromError(a, err);
    f.stack_trace = try inst.failureTraceText(out, addresses);
    return f;
}

// ---------------------------------------------------------------- continue-as-new

/// Encodes `args` into the arena and passes the request through the outbound interceptors to
/// `recordContinueAsNew`.
pub fn requestContinueAsNew(inst: *Instance, workflow_type: []const u8, options: workflow.ContinueAsNewOptions, args: anytype) sa.EncodeError!void {
    if (inst.wio.refuse("continueAsNew")) return;
    const context = try inst.contextWith(.{ .workflow_type = workflow_type });
    const out = try inst.outgoing(context, args);
    return inst.outboundNext().continueAsNew(.{ .workflow_type = workflow_type, .options = options, .args = out.args, .header = out.header });
}

/// Records the ContinueAsNewWorkflowExecution command to send if the workflow function returns
/// error.ContinueAsNew, replacing any earlier request. `args` and `header` are arena memory;
/// `workflow_type` is copied.
fn recordContinueAsNew(inst: *Instance, workflow_type: []const u8, options: workflow.ContinueAsNewOptions, args: []const converter.Payload, header: converter.Header) sa.EncodeError!void {
    const a = inst.arena();
    var cmd: cmd_pb.ContinueAsNewWorkflowExecution = .{
        // Copied: a name given at run time (or by an interceptor) may not outlive the call.
        .workflow_type = try a.dupe(u8, workflow_type),
        // Empty: core and the server keep this run's task queue.
        .task_queue = try a.dupe(u8, options.task_queue),
        .arguments = try payloadList(a, args),
        .headers = try header.toEntries(cmd_pb.ContinueAsNewWorkflowExecution.HeadersEntry, a),
        .workflow_run_timeout = proto_time.durationToProto(options.run_timeout),
        .workflow_task_timeout = proto_time.durationToProto(options.task_timeout),
    };
    if (options.retry_policy) |rp| cmd.retry_policy = try protoRetryPolicy(a, rp);
    // Core fills an empty memo with the memo this run started with, which would drop
    // `upsertMemo` changes; sending the current view keeps them.
    const memo = options.memo orelse inst.memo;
    try cmd.memo.ensureTotalCapacity(a, memo.len);
    for (memo) |f| cmd.memo.appendAssumeCapacity(.{ .key = try a.dupe(u8, f.name), .value = try f.value.dupe(a) });
    // Core fills missing search attributes with the start values plus upserts, removals
    // included; the current view (an empty one too) carries only the values that are set.
    cmd.search_attributes = if (options.search_attributes) |updates|
        try sa.toProto(a, updates, .start)
    else
        try inst.search_attributes.toProto(a);
    inst.continue_as_new = cmd;
}

// ---------------------------------------------------------------- memo

/// Emits one ModifyWorkflowProperties command upserting `fields` (in their order) and applies
/// them to `inst.memo`. `fields` must stay valid for the run (arena memory). Upserting no
/// fields does nothing.
pub fn upsertMemo(inst: *Instance, fields: []const converter.Field) error{OutOfMemory}!void {
    if (fields.len == 0 or inst.wio.refuse("upsertMemo")) return;
    const a = inst.arena();
    var upserted: common.Memo = .{};
    try upserted.fields.ensureTotalCapacity(a, fields.len);
    for (fields) |f| upserted.fields.appendAssumeCapacity(.{ .key = f.name, .value = f.value });

    var merged: std.ArrayList(converter.Field) = try .initCapacity(a, inst.memo.len + fields.len);
    merged.appendSliceAssumeCapacity(inst.memo);
    for (fields) |f| {
        const i = std.sort.lowerBound(converter.Field, merged.items, f.name, fieldOrder);
        const found = i < merged.items.len and std.mem.eql(u8, merged.items[i].name, f.name);
        if (removesMemoKey(f.value)) {
            if (found) _ = merged.orderedRemove(i);
        } else if (found) {
            merged.items[i] = f;
        } else {
            merged.insertAssumeCapacity(i, f);
        }
    }
    try inst.emit(.{ .variant = .{ .modify_workflow_properties = .{ .upserted_memo = upserted } } });
    inst.memo = merged.items;
}

/// Whether the server deletes a memo key upserted with this value: it does for a null and for
/// an empty JSON array, compared with the whole payload.
fn removesMemoKey(p: converter.Payload) bool {
    if (p.metadata.items.len != 1 or p.external_payloads.items.len != 0) return false;
    const enc = converter.encodingOf(p) orelse return false;
    if (std.mem.eql(u8, enc, converter.Encoding.null.text())) return p.data.len == 0;
    if (std.mem.eql(u8, enc, converter.Encoding.json.text())) return std.mem.eql(u8, p.data, "[]");
    return false;
}

fn fieldLessThan(_: void, x: converter.Field, y: converter.Field) bool {
    return std.mem.lessThan(u8, x.name, y.name);
}

fn fieldOrder(name: []const u8, f: converter.Field) std.math.Order {
    return std.mem.order(u8, name, f.name);
}

// ---------------------------------------------------------------- search attributes

/// Emits one UpsertWorkflowSearchAttributes command with `updates` (in their order) and
/// applies them to `inst.search_attributes`. Upserting nothing does nothing.
pub fn upsertSearchAttributes(inst: *Instance, updates: []const sa.Update) sa.EncodeError!void {
    if (updates.len == 0 or inst.wio.refuse("upsertSearchAttributes")) return;
    const a = inst.arena();
    const encoded = try sa.toProto(a, updates, .upsert);
    const view = try inst.search_attributes.withEntries(a, encoded.indexed_fields.items);
    try inst.emit(.{ .variant = .{ .upsert_workflow_search_attributes = .{ .search_attributes = encoded } } });
    inst.search_attributes = view;
}

// ---------------------------------------------------------------- patches

/// Whether the run takes the patched code path for `patch_id`, as the core's patch rules say:
/// always when not replaying, and on replay only when the history has the patch's marker.
/// The first call decides for the run; when it says yes, it emits a SetPatchMarker command
/// (which the core sends at most once per patch). `deprecated` marks the marker as one that
/// code without the patch may replay. Out of memory fails the workflow task, which retries.
pub fn patch(inst: *Instance, patch_id: []const u8, deprecated: bool) bool {
    if (inst.patches.get(patch_id)) |used| return used;
    const used = !inst.replaying or inst.notified_patches.contains(patch_id);
    // Read-only code gets the answer without deciding it for the run.
    if (inst.wio.refuse("patched")) return used;
    inst.recordPatch(patch_id, used, deprecated) catch {
        inst.task_failure = "out of memory recording a patch";
    };
    return used;
}

fn recordPatch(inst: *Instance, patch_id: []const u8, used: bool, deprecated: bool) error{OutOfMemory}!void {
    const a = inst.arena();
    const id = try a.dupe(u8, patch_id);
    try inst.patches.put(a, id, used);
    if (used) try inst.emit(.{ .variant = .{ .set_patch_marker = .{ .patch_id = id, .deprecated = deprecated } } });
}

/// The patch ID that records version `version` of change `change_id`. A version is an
/// integer, which never contains "@v", so the ID names one change and version only.
fn versionPatchId(a: Allocator, change_id: []const u8, version: i32) error{OutOfMemory}![]const u8 {
    return std.fmt.allocPrint(a, "{s}@v{d}", .{ change_id, version });
}

/// The version of `change_id` that history recorded, from the patches the core reported.
fn recordedVersion(inst: *const Instance, change_id: []const u8) ?i32 {
    for (inst.notified_patches.keys()) |id| {
        if (id.len <= change_id.len + 2 or !std.mem.startsWith(u8, id, change_id)) continue;
        const rest = id[change_id.len..];
        if (!std.mem.startsWith(u8, rest, "@v")) continue;
        return std.fmt.parseInt(i32, rest[2..], 10) catch continue;
    }
    return null;
}

/// See `workflow.Context.getVersion`. The version is a patch named `versionPatchId`; the
/// default version has none.
pub fn getVersion(inst: *Instance, change_id: []const u8, min_supported: i32, max_supported: i32) error{UnsupportedVersion}!i32 {
    const a = inst.arena();
    const version = inst.versions.get(change_id) orelse blk: {
        const v = inst.recordedVersion(change_id) orelse if (inst.replaying) workflow.default_version else max_supported;
        if (inst.wio.refuse("getVersion")) return v;
        inst.versions.put(a, a.dupe(u8, change_id) catch return inst.versionOutOfMemory(v), v) catch return inst.versionOutOfMemory(v);
        break :blk v;
    };
    if (version < min_supported or version > max_supported) {
        if (inst.readOnly() != null) return error.UnsupportedVersion; // the workflow task is not the query's to fail
        // Failing the task, not the workflow, lets a worker whose code supports the
        // version run it. The message reaches the server's WorkflowTaskFailed event; the
        // workflow's logger is silent during the replay that usually finds this.
        inst.task_failure = std.fmt.allocPrint(a, "getVersion(\"{s}\", {d}, {d}): the run uses version {d} of this change, which this code doesn't support", .{ change_id, min_supported, max_supported, version }) catch "getVersion: the run uses a version of a change this code doesn't support";
        return error.UnsupportedVersion;
    }
    // Read-only code only reads the version: there is no marker to record.
    if (version == workflow.default_version or inst.readOnly() != null) return version;
    _ = inst.patch(versionPatchId(a, change_id, version) catch return inst.versionOutOfMemory(version), false);
    return version;
}

fn versionOutOfMemory(inst: *Instance, version: i32) i32 {
    inst.task_failure = "out of memory recording a version";
    return version;
}

/// An outgoing call's encoded arguments and its header, holding what the propagators write
/// for `propagated`, for the outbound interceptors. Arena memory.
const Outgoing = struct { args: converter.EncodedValues, header: *converter.HeaderWriter };

/// `context` is what the arguments are encoded with and what `args` decodes with; it must
/// live as long as the run (arena memory, or the run's own).
fn outgoing(inst: *Instance, context: *const converter.Context, args: anytype) error{OutOfMemory}!Outgoing {
    const a = inst.arena();
    const payloads = try converter.toPayloadsWithContext(a, context, args);
    const header = try a.create(converter.HeaderWriter);
    header.* = .init(a);
    try propagation.injectAll(inst.propagators, a, &inst.propagated, header);
    return .{ .args = .{ .payloads = payloads.items, .context = context }, .header = header };
}

/// A copy of the run's converter context in the arena, with `workflow_type`, `workflow_id`,
/// `activity_type` and `activity_id` replaced where given (copied into the arena too).
fn contextWith(inst: *Instance, fields: struct {
    workflow_id: ?[]const u8 = null,
    workflow_type: ?[]const u8 = null,
    activity_type: ?[]const u8 = null,
    activity_id: ?[]const u8 = null,
}) error{OutOfMemory}!*converter.Context {
    const a = inst.arena();
    const context = try a.create(converter.Context);
    context.* = inst.converter_context;
    if (fields.workflow_id) |id| context.workflow_id = try a.dupe(u8, id);
    if (fields.workflow_type) |t| context.workflow_type = try inst.intern(t);
    if (fields.activity_type) |t| context.activity_type = try inst.intern(t);
    if (fields.activity_id) |id| context.activity_id = try a.dupe(u8, id);
    return context;
}

/// `name` copied into the arena once per run: a run that schedules many activities of a few
/// types holds each type name once, so its memory doesn't grow with the names' length.
fn intern(inst: *Instance, name: []const u8) error{OutOfMemory}![]const u8 {
    const a = inst.arena();
    const entry = try inst.type_names.getOrPut(a, name);
    if (!entry.found_existing) entry.key_ptr.* = a.dupe(u8, name) catch |err| {
        inst.type_names.removeByPtr(entry.key_ptr);
        return err;
    };
    return entry.key_ptr.*;
}

/// The converter context of an activity's arguments, which are encoded before the activity
/// is scheduled: the run's, with the activity type and `activity_id`, or the ID the next
/// activity gets when that is empty (an interceptor that schedules another activity first,
/// or changes the ID, makes it differ from the scheduled one).
fn activityArgsContext(inst: *Instance, activity_type: []const u8, activity_id: []const u8) error{OutOfMemory}!*converter.Context {
    const context = try inst.contextWith(.{ .activity_type = activity_type, .activity_id = activity_id });
    if (activity_id.len == 0) context.activity_id = try std.fmt.allocPrint(inst.arena(), "{d}", .{inst.next_activity_seq});
    return context;
}

fn payloadList(a: Allocator, payloads: []const converter.Payload) error{OutOfMemory}!std.ArrayList(converter.Payload) {
    var list: std.ArrayList(converter.Payload) = .empty;
    try list.appendSlice(a, payloads);
    return list;
}

fn inboundNext(inst: *Instance) interceptor.WorkflowInbound.Next {
    return .{ .chain = inst.inbound, .root = .{ .ptr = inst, .vtable = &inbound_root } };
}

fn outboundNext(inst: *Instance) interceptor.WorkflowOutbound.Next {
    return .{ .chain = inst.outbound, .root = .{ .ptr = inst, .vtable = &outbound_root } };
}

/// The logger workflow code gets: the run's, through the outbound interceptors.
pub fn currentLogger(inst: *Instance) log.Logger {
    if (inst.outbound.len == 0) return inst.logger;
    return inst.outboundNext().logger(.{});
}

/// Encodes `args` and schedules an activity through the outbound interceptors.
pub fn executeActivity(inst: *Instance, activity_type: []const u8, options: workflow.ActivityOptions, args: anytype) workflow.ActivityFuture(converter.Payload) {
    if (inst.wio.refuse("executeActivity")) return .{ .ctx = &inst.ctx, .state = error.InvalidOptions };
    const context = inst.activityArgsContext(activity_type, options.activity_id) catch |err| return .{ .ctx = &inst.ctx, .state = err };
    const out = inst.outgoing(context, args) catch |err| return .{ .ctx = &inst.ctx, .state = err };
    return inst.outboundNext().executeActivity(.{ .activity_type = activity_type, .options = options, .args = out.args, .header = out.header });
}

/// Encodes `args` and schedules a local activity through the outbound interceptors.
pub fn executeLocalActivity(inst: *Instance, activity_type: []const u8, options: workflow.LocalActivityOptions, args: anytype) workflow.ActivityFuture(converter.Payload) {
    if (inst.wio.refuse("executeLocalActivity")) return .{ .ctx = &inst.ctx, .state = error.InvalidOptions };
    const context = inst.activityArgsContext(activity_type, options.activity_id) catch |err| return .{ .ctx = &inst.ctx, .state = err };
    const out = inst.outgoing(context, args) catch |err| return .{ .ctx = &inst.ctx, .state = err };
    return inst.outboundNext().executeLocalActivity(.{ .activity_type = activity_type, .options = options, .args = out.args, .header = out.header });
}

/// Records a side effect's value (null on replay) through the outbound interceptors.
pub fn sideEffect(inst: *Instance, value: ?converter.Payload) workflow.ActivityFuture(converter.Payload) {
    if (inst.wio.refuse("sideEffect")) return .{ .ctx = &inst.ctx, .state = error.InvalidOptions };
    return inst.outboundNext().sideEffect(.{ .value = value });
}

/// Encodes `args` and starts a child workflow through the outbound interceptors.
pub fn executeChildWorkflow(inst: *Instance, workflow_type: []const u8, options: workflow.ChildWorkflowOptions, args: anytype) workflow.ChildWorkflowFuture(converter.Payload) {
    if (inst.wio.refuse("executeChildWorkflow")) return .{ .ctx = &inst.ctx, .state = error.InvalidOptions };
    // The ID is empty when the SDK will generate it: it is drawn when the child is started,
    // not earlier, so the run's random values stay as they were.
    const context = inst.contextWith(.{ .workflow_id = options.id, .workflow_type = workflow_type }) catch |err| return .{ .ctx = &inst.ctx, .state = err };
    const out = inst.outgoing(context, args) catch |err| return .{ .ctx = &inst.ctx, .state = err };
    return inst.outboundNext().executeChildWorkflow(.{ .workflow_type = workflow_type, .options = options, .args = out.args, .header = out.header });
}

/// Encodes `args` and sends a signal through the outbound interceptors.
pub fn signalExternal(inst: *Instance, target: SignalTarget, signal_name: []const u8, args: anytype) workflow.SignalExternalFuture {
    return inst.signalWithContext(target, null, signal_name, args);
}

/// `signalExternal` to the child `state`, encoding `args` with the child's converter context.
pub fn signalChildWorkflow(inst: *Instance, state: *const ChildState, signal_name: []const u8, args: anytype) workflow.SignalExternalFuture {
    return inst.signalWithContext(.{ .child = state.workflow_id }, &state.context, signal_name, args);
}

/// `context` null encodes `args` with the run's context naming the target workflow ID.
fn signalWithContext(inst: *Instance, target: SignalTarget, context: ?*const converter.Context, signal_name: []const u8, args: anytype) workflow.SignalExternalFuture {
    if (inst.wio.refuse("signalExternalWorkflow")) return .{ .ctx = &inst.ctx, .state = error.InvalidOptions };
    const workflow_id, const run_id, const child = switch (target) {
        .execution => |e| .{ e.workflow_id, e.run_id, false },
        .child => |id| .{ id, "", true },
    };
    const conv = context orelse inst.contextWith(.{ .workflow_id = workflow_id, .workflow_type = "" }) catch |err| return .{ .ctx = &inst.ctx, .state = err };
    const out = inst.outgoing(conv, args) catch |err| return .{ .ctx = &inst.ctx, .state = err };
    return inst.outboundNext().signalExternalWorkflow(.{
        .signal_name = signal_name,
        .workflow_id = workflow_id,
        .run_id = run_id,
        .child = child,
        .args = out.args,
        .header = out.header,
    });
}

/// Encodes `args` and `signal_args` with the target's converter context and sends a
/// signal-with-start through the outbound interceptors. The propagators write into the
/// request's header, as for a child workflow's start, not into Nexus headers.
pub fn signalWithStart(inst: *Instance, workflow_type: []const u8, options: workflow.SignalWithStartOptions, args: anytype, signal_name: []const u8, signal_args: anytype) workflow.SignalWithStartFuture {
    if (inst.wio.refuse("signalWithStartWorkflow")) return .{ .ctx = &inst.ctx, .workflow_id = "", .state = error.InvalidOptions };
    const fail: workflow.SignalWithStartFuture = .{ .ctx = &inst.ctx, .workflow_id = "", .state = error.OutOfMemory };
    const context = inst.contextWith(.{ .workflow_id = options.id, .workflow_type = workflow_type }) catch return fail;
    const out = inst.outgoing(context, args) catch return fail;
    const signal_payloads = converter.toPayloadsWithContext(inst.arena(), context, signal_args) catch return fail;
    return inst.outboundNext().signalWithStartWorkflow(.{
        .workflow_type = workflow_type,
        .signal_name = signal_name,
        .options = options,
        .args = out.args,
        .signal_args = .{ .payloads = signal_payloads.items, .context = context },
        .header = out.header,
    });
}

/// Encodes `input`, adds the propagated values to the Nexus headers (see
/// `nexusHeadersWithPropagated`), and schedules a Nexus operation through the outbound
/// interceptors.
pub fn executeNexusOperation(inst: *Instance, service: []const u8, operation: []const u8, options: workflow.NexusOperationOptions, input: anytype) workflow.NexusOperationFuture(converter.Payload) {
    if (inst.wio.refuse("executeNexusOperation")) return .{ .ctx = &inst.ctx, .state = error.InvalidOptions };
    const context = inst.nexusContext();
    const payload = converter.toPayloadWithContext(inst.arena(), &context, input) catch return .{ .ctx = &inst.ctx, .state = error.OutOfMemory };
    var with_propagated = options;
    with_propagated.headers = inst.nexusHeadersWithPropagated(options.headers) catch return .{ .ctx = &inst.ctx, .state = error.OutOfMemory };
    return inst.outboundNext().executeNexusOperation(.{ .service = service, .operation = operation, .options = with_propagated, .payload = payload });
}

/// The converter context of a Nexus operation's input and result: the namespace and the
/// propagated values only, since the handler is no workflow or activity of this run.
pub fn nexusContext(inst: *const Instance) converter.Context {
    return .{ .values = &inst.propagated, .namespace = inst.namespace };
}

/// The caller's Nexus headers with the propagated values added: `headers` as given when the
/// propagators write nothing, otherwise `headers` in order without any named
/// `propagation.nexus_header_name` (ignoring case), followed by that header. `headers` is not
/// modified; a new list is arena memory.
fn nexusHeadersWithPropagated(inst: *Instance, headers: []const client.NexusHeader) error{OutOfMemory}![]const client.NexusHeader {
    const a = inst.arena();
    const value = (try propagation.injectNexusHeader(inst.propagators, a, &inst.propagated)) orelse return headers;
    var out: std.ArrayList(client.NexusHeader) = try .initCapacity(a, headers.len + 1);
    for (headers) |h| if (!std.ascii.eqlIgnoreCase(h.name, propagation.nexus_header_name)) out.appendAssumeCapacity(h);
    out.appendAssumeCapacity(.{ .name = propagation.nexus_header_name, .value = value });
    return out.items;
}

/// Requests another workflow's cancellation through the outbound interceptors.
pub fn cancelExternalWorkflow(inst: *Instance, workflow_id: []const u8, run_id: []const u8) workflow.CancelExternalFuture {
    if (inst.wio.refuse("cancelExternalWorkflow")) return .{ .ctx = &inst.ctx, .state = error.InvalidOptions };
    return inst.outboundNext().cancelExternalWorkflow(.{ .workflow_id = workflow_id, .run_id = run_id });
}

/// The state of an activity an interceptor refused: already failed with `f` (wrapped as
/// an activity failure), never scheduled. Arena memory.
pub fn rejectedActivity(inst: *Instance, activity_type: []const u8, f: failure_mod.Application) ScheduleError!*ActivityState {
    const a = inst.arena();
    const cause = try a.create(Failure);
    cause.* = try failure_mod.application(a, f);
    const out = try a.create(Failure);
    out.* = .{
        .message = try std.fmt.allocPrint(a, "activity \"{s}\" was refused by a workflow interceptor", .{activity_type}),
        .source = failure_mod.source,
        .cause = cause,
        .failure_info = .{ .activity_failure_info = .{ .activity_type = .{ .name = try a.dupe(u8, activity_type) } } },
    };
    const state = try a.create(ActivityState);
    state.* = .{ .seq = 0, .outcome = .{ .failed = out }, .context = inst.converter_context };
    return state;
}

/// The state of a child workflow an interceptor refused, as `rejectedActivity`.
pub fn rejectedChild(inst: *Instance, workflow_type: []const u8, f: failure_mod.Application) ScheduleError!*ChildState {
    const a = inst.arena();
    const cause = try a.create(Failure);
    cause.* = try failure_mod.application(a, f);
    const out = try a.create(Failure);
    out.* = .{
        .message = try std.fmt.allocPrint(a, "child workflow \"{s}\" was refused by a workflow interceptor", .{workflow_type}),
        .source = failure_mod.source,
        .cause = cause,
        .failure_info = .{ .child_workflow_execution_failure_info = .{
            .namespace = inst.namespace,
            .workflow_type = .{ .name = try a.dupe(u8, workflow_type) },
        } },
    };
    const state = try a.create(ChildState);
    state.* = .{ .seq = 0, .workflow_id = "", .workflow_type = out.failure_info.?.child_workflow_execution_failure_info.workflow_type.?.name, .cancellation_type = .try_cancel, .outcome = .{ .failed = out }, .context = inst.converter_context };
    return state;
}

/// The state of a Nexus operation an interceptor refused, as `rejectedActivity`: a Nexus
/// operation failure naming the endpoint, service and operation, caused by `f`.
pub fn rejectedNexusOperation(inst: *Instance, endpoint: []const u8, service: []const u8, operation: []const u8, f: failure_mod.Application) ScheduleError!*NexusOperationState {
    const a = inst.arena();
    const cause = try a.create(Failure);
    cause.* = try failure_mod.application(a, f);
    const endpoint_copy = try a.dupe(u8, endpoint);
    const service_copy = try a.dupe(u8, service);
    const operation_copy = try a.dupe(u8, operation);
    const out = try a.create(Failure);
    out.* = .{
        .message = try std.fmt.allocPrint(a, "nexus operation \"{s}\" of service \"{s}\" was refused by a workflow interceptor", .{ operation, service }),
        .source = failure_mod.source,
        .cause = cause,
        .failure_info = .{ .nexus_operation_execution_failure_info = .{
            .endpoint = endpoint_copy,
            .service = service_copy,
            .operation = operation_copy,
        } },
    };
    const state = try a.create(NexusOperationState);
    state.* = .{
        .seq = 0,
        .endpoint = endpoint_copy,
        .service = service_copy,
        .operation = operation_copy,
        .cancellation_type = .abandon,
        .outcome = .{ .failed = out },
    };
    return state;
}

// ---------------------------------------------------------------- activities

pub const ScheduleError = error{ OutOfMemory, InvalidOptions };
/// `SessionFailed`: the activity's session had failed, so it wasn't scheduled.
pub const ActivityScheduleError = ScheduleError || error{SessionFailed};

/// Creates the state of a new activity, local or remote: both share one sequence space.
fn newActivity(inst: *Instance, activity_type: []const u8, cancellation_type: ActivityState.CancellationType) !*ActivityState {
    const a = inst.arena();
    const state = try a.create(ActivityState);
    state.* = .{ .seq = inst.next_activity_seq, .cancellation_type = cancellation_type, .activity_type = activity_type, .scheduled_in = inst.activations };
    try inst.activities.put(a, state.seq, state);
    inst.next_activity_seq += 1;
    return state;
}

fn scheduleActivity(inst: *Instance, activity_type: []const u8, options: workflow.ActivityOptions, args: []const converter.Payload, header: converter.Header) ScheduleError!*ActivityState {
    const a = inst.arena();
    const state = try inst.newActivity(activity_type, options.cancellation_type);
    const seq = state.seq;

    const activity_id = if (options.activity_id.len > 0) try a.dupe(u8, options.activity_id) else try std.fmt.allocPrint(a, "{d}", .{seq});
    state.context = inst.converter_context;
    state.context.activity_type = try inst.intern(activity_type);
    state.context.activity_id = activity_id;
    var cmd: cmd_pb.ScheduleActivity = .{
        .seq = seq,
        .activity_id = activity_id,
        .activity_type = activity_type,
        .task_queue = if (options.task_queue.len > 0) options.task_queue else inst.info.task_queue,
        .arguments = try payloadList(a, args),
        .headers = try header.toEntries(cmd_pb.ScheduleActivity.HeadersEntry, a),
        .schedule_to_close_timeout = proto_time.durationToProto(options.schedule_to_close_timeout),
        .schedule_to_start_timeout = proto_time.durationToProto(options.schedule_to_start_timeout),
        .start_to_close_timeout = proto_time.durationToProto(options.start_to_close_timeout),
        .heartbeat_timeout = proto_time.durationToProto(options.heartbeat_timeout),
        .cancellation_type = protoCancellationType(options.cancellation_type),
        .do_not_eagerly_execute = options.disable_eager_execution,
    };
    if (options.retry_policy) |rp| cmd.retry_policy = try protoRetryPolicy(a, rp);
    if (options.priority) |p| cmd.priority = .{
        .priority_key = p.priority_key,
        .fairness_key = p.fairness_key,
        .fairness_weight = p.fairness_weight,
    };
    cmd.versioning_intent = switch (options.versioning_intent) {
        .unspecified => .UNSPECIFIED,
        .compatible => .COMPATIBLE,
        .default => .DEFAULT,
    };
    try inst.emit(.{
        .user_metadata = try summaryMetadata(a, options.summary),
        .variant = .{ .schedule_activity = cmd },
    });
    return state;
}

/// A command's `user_metadata` for a summary, or null for none.
fn summaryMetadata(a: Allocator, summary: []const u8) error{OutOfMemory}!?sdk_pb.UserMetadata {
    if (summary.len == 0) return null;
    return .{ .summary = try converter.toPayload(a, summary) };
}

fn scheduleLocalActivity(inst: *Instance, activity_type: []const u8, options: workflow.LocalActivityOptions, args: []const converter.Payload, header: converter.Header) ScheduleError!*ActivityState {
    if (options.start_to_close_timeout == null and options.schedule_to_close_timeout == null) {
        inst.logger.warn("local activity options need start_to_close_timeout or schedule_to_close_timeout", .{ .ActivityType = activity_type });
        return error.InvalidOptions;
    }
    const a = inst.arena();
    const state = try inst.newActivity(activity_type, options.cancellation_type);
    const activity_id = if (options.activity_id.len > 0) try a.dupe(u8, options.activity_id) else try std.fmt.allocPrint(a, "{d}", .{state.seq});
    state.context = inst.converter_context;
    state.context.activity_type = try inst.intern(activity_type);
    state.context.activity_id = activity_id;
    const local = try a.create(ActivityState.Local);
    local.* = .{
        .cmd = .{
            .seq = state.seq,
            .activity_id = activity_id,
            .activity_type = activity_type,
            .arguments = try payloadList(a, args),
            .headers = try header.toEntries(cmd_pb.ScheduleLocalActivity.HeadersEntry, a),
            .schedule_to_close_timeout = proto_time.durationToProto(options.schedule_to_close_timeout),
            .schedule_to_start_timeout = proto_time.durationToProto(options.schedule_to_start_timeout),
            .start_to_close_timeout = proto_time.durationToProto(options.start_to_close_timeout),
            .local_retry_threshold = proto_time.durationToProto(options.local_retry_threshold),
            .cancellation_type = protoCancellationType(options.cancellation_type),
        },
        .id_from_seq = options.activity_id.len == 0,
        .user_metadata = try summaryMetadata(a, options.summary),
    };
    if (options.retry_policy) |rp| local.cmd.retry_policy = try protoRetryPolicy(a, rp);
    state.local = local;
    try inst.emit(.{ .user_metadata = local.user_metadata, .variant = .{ .schedule_local_activity = local.cmd } });
    return state;
}

/// Handles a local activity's DoBackoff: the core gave up retrying it on the worker, so the
/// workflow waits on a timer, then schedules the attempt the core named (`retryLocalActivity`).
fn startLocalBackoff(inst: *Instance, state: *ActivityState, b: result_pb.DoBackoff) !void {
    const a = inst.arena();
    const local = state.local orelse {
        inst.task_failure = "the core asked to back off a remote activity";
        return;
    };
    local.cmd.attempt = b.attempt;
    local.cmd.original_schedule_time = b.original_schedule_time;
    const seq = inst.next_timer_seq;
    inst.next_timer_seq += 1;
    try inst.backoffs.put(a, seq, state);
    local.backoff_timer = seq;
    try inst.emit(.{ .variant = .{ .start_timer = .{
        .seq = seq,
        .start_to_fire_timeout = b.backoff_duration orelse .{},
    } } });
}

/// Schedules a local activity's next attempt after its backoff timer fired. The attempt gets
/// a new seq, as the core requires.
fn retryLocalActivity(inst: *Instance, state: *ActivityState) !void {
    const a = inst.arena();
    const local = state.local.?;
    local.backoff_timer = null;
    state.seq = inst.next_activity_seq;
    try inst.activities.put(a, state.seq, state);
    inst.next_activity_seq += 1;
    local.cmd.seq = state.seq;
    if (local.id_from_seq) {
        local.cmd.activity_id = try std.fmt.allocPrint(a, "{d}", .{state.seq});
        state.context.activity_id = local.cmd.activity_id;
    }
    try inst.emit(.{ .user_metadata = local.user_metadata, .variant = .{ .schedule_local_activity = local.cmd } });
}

fn protoCancellationType(t: ActivityState.CancellationType) cmd_pb.ActivityCancellationType {
    return switch (t) {
        .try_cancel => .TRY_CANCEL,
        .wait_cancellation_completed => .WAIT_CANCELLATION_COMPLETED,
        .abandon => .ABANDON,
    };
}

/// A start's retry policy as the workflow sees it: absent and zero intervals are null, which
/// is what `workflow.RetryPolicy` means by unset. Type names are copied into `a`.
fn zigRetryPolicy(a: Allocator, p: common.RetryPolicy) error{OutOfMemory}!workflow.RetryPolicy {
    const types = try a.alloc([]const u8, p.non_retryable_error_types.items.len);
    for (p.non_retryable_error_types.items, types) |t, *d| d.* = try a.dupe(u8, t);
    return .{
        .initial_interval = proto_time.positiveDurationFromProto(p.initial_interval),
        .backoff_coefficient = p.backoff_coefficient,
        .maximum_interval = proto_time.positiveDurationFromProto(p.maximum_interval),
        .maximum_attempts = p.maximum_attempts,
        .non_retryable_error_types = types,
    };
}

fn protoRetryPolicy(a: Allocator, rp: workflow.RetryPolicy) !common.RetryPolicy {
    var p: common.RetryPolicy = .{
        .initial_interval = proto_time.durationToProto(rp.initial_interval),
        .backoff_coefficient = rp.backoff_coefficient,
        .maximum_interval = proto_time.durationToProto(rp.maximum_interval),
        .maximum_attempts = rp.maximum_attempts,
    };
    try p.non_retryable_error_types.appendSlice(a, rp.non_retryable_error_types);
    return p;
}

/// The longest activity type name a wait records; longer ones end in "...".
const max_wait_name = 64;

/// `name`, or as much of it as fits in `buf` with "..." after it, cut before a UTF-8 lead
/// byte so no character is split. The answer escapes what's left anyway
/// (`call_sites.text`); the cut keeps a valid name readable.
fn truncatedName(buf: *[max_wait_name]u8, name: []const u8) []const u8 {
    if (name.len <= buf.len) {
        @memcpy(buf[0..name.len], name);
        return buf[0..name.len];
    }
    var cut: usize = buf.len - 3;
    while (cut > 0 and name[cut] & 0xc0 == 0x80) cut -= 1;
    @memcpy(buf[0..cut], name[0..cut]);
    @memcpy(buf[cut..][0..3], "...");
    return buf[0 .. cut + 3];
}

/// Blocks the calling coroutine until the activity resolves. When the coroutine is canceled
/// while waiting, requests the activity's cancellation and returns error.Canceled: at once
/// for `try_cancel` and `abandon`, and for `wait_cancellation_completed` once the core
/// resolves the activity (the server recorded its end). If the activity completed or failed
/// instead of acknowledging the cancel, that outcome is kept for the caller and the cancel
/// request is re-armed, so the coroutine's next cancelable wait returns error.Canceled.
/// Eviction ends any of these waits with error.Canceled.
pub fn awaitActivity(inst: *Instance, state: *ActivityState) error{Canceled}!void {
    if (state.outcome != null) return;
    if (inst.wio.refuse("ActivityFuture.get")) return error.Canceled;
    // The type name is copied to this coroutine's stack while it is still valid (the wait
    // began in the activation that scheduled the activity); later, only the seq is known.
    var name_buf: [max_wait_name]u8 = undefined;
    const known = state.scheduled_in == inst.activations;
    const name = if (known) truncatedName(&name_buf, state.activity_type) else "";
    const seq: ?u32 = if (known) null else state.seq;
    const reason: WorkflowIo.Wait.Reason = if (state.local == null)
        .{ .what = "activity", .name = name, .seq = seq }
    else if (state.activity_type.ptr == side_effect_activity_type.ptr)
        .{ .what = "a side effect (Context.sideEffect)" }
    else
        .{ .what = "local activity", .name = name, .seq = seq };
    var wait = inst.wio.newWaitPoint(reason);
    state.waiter = &wait;
    defer state.waiter = null;
    var canceled = false;
    while (state.outcome == null) {
        if (inst.evicting) return error.Canceled;
        // A cancel also wakes the wait point, so each block starts from a fresh one.
        wait = inst.wio.newWaitPoint(reason);
        inst.wio.waitFor(&wait, !canceled) catch |err| {
            inst.cancelActivity(state);
            if (state.cancellation_type != .wait_cancellation_completed) return err;
            canceled = true;
        };
    }
    if (!canceled) return;
    if (state.outcome.? == .cancelled) return error.Canceled;
    inst.wio.io().recancel();
}

pub fn cancelActivity(inst: *Instance, state: *ActivityState) void {
    if (state.outcome != null or state.cancel_requested) return;
    if (inst.wio.refuse("ActivityFuture.cancel")) return;
    state.cancel_requested = true;
    inst.requestCancel(state) catch {
        inst.task_failure = "out of memory requesting activity cancellation";
    };
}

fn requestCancel(inst: *Instance, state: *ActivityState) !void {
    const a = inst.arena();
    const local = state.local orelse
        return inst.emit(.{ .variant = .{ .request_cancel_activity = .{ .seq = state.seq } } });
    const timer = local.backoff_timer orelse
        return inst.emit(.{ .variant = .{ .request_cancel_local_activity = .{ .seq = state.seq } } });
    // Between attempts nothing runs: cancel the backoff timer and resolve here, whatever the
    // cancellation type.
    _ = inst.backoffs.remove(timer);
    local.backoff_timer = null;
    try inst.emit(.{ .variant = .{ .cancel_timer = .{ .seq = timer } } });
    state.outcome = .{ .cancelled = try localCancelFailure(a, local) };
    if (state.waiter) |w| inst.wio.wake(w);
}

fn localCancelFailure(a: Allocator, local: *const ActivityState.Local) !*const Failure {
    const cause = try a.create(Failure);
    cause.* = .{ .message = "local activity canceled while waiting to retry", .source = failure_mod.source, .failure_info = .{ .canceled_failure_info = .{} } };
    const out = try a.create(Failure);
    out.* = .{
        .message = "Local Activity cancelled",
        .source = failure_mod.source,
        .cause = cause,
        .failure_info = .{ .activity_failure_info = .{
            .activity_type = .{ .name = local.cmd.activity_type },
            .activity_id = local.cmd.activity_id,
            .retry_state = .RETRY_STATE_CANCEL_REQUESTED,
        } },
    };
    return out;
}

fn resolveActivity(inst: *Instance, r: *const act_pb.ResolveActivity) !void {
    const kv = inst.activities.fetchRemove(r.seq) orelse return;
    const state = kv.value;
    const a = inst.arena();
    const status = (r.result orelse return).status orelse return;
    state.outcome = switch (status) {
        .completed => |s| .{ .completed = if (s.result) |p| try p.dupe(a) else null },
        .failed => |f| .{ .failed = try inst.receivedFailure(&state.context, f.failure) },
        .cancelled => |c| .{ .cancelled = try inst.receivedFailure(&state.context, c.failure) },
        .backoff => |b| {
            if (state.cancel_requested) if (state.local) |local| {
                // Canceled while the attempt ran: don't start another.
                state.outcome = .{ .cancelled = try localCancelFailure(a, local) };
                if (state.waiter) |w| inst.wio.wake(w);
                return;
            };
            return inst.startLocalBackoff(state, b);
        },
    };
    if (state.waiter) |w| inst.wio.wake(w);
}

// ---------------------------------------------------------------- child workflows

/// Emits StartChildWorkflowExecution for `workflow_type`. A child without an ID gets a UUID
/// drawn from the workflow's seeded randomness, so replay generates the same one.
fn startChildWorkflow(inst: *Instance, workflow_type: []const u8, options: workflow.ChildWorkflowOptions, args: []const converter.Payload, header: converter.Header) ScheduleError!*ChildState {
    if (workflow_type.len == 0) {
        inst.logger.warn("executeChildWorkflowByName: the child workflow type is empty", .{ .ChildWorkflowID = options.id });
        return error.InvalidOptions;
    }
    const a = inst.arena();
    const state = try a.create(ChildState);
    state.* = .{
        .seq = inst.next_child_seq,
        .workflow_id = if (options.id.len > 0) try a.dupe(u8, options.id) else try client.uuid(a, inst.wio.io()),
        .workflow_type = try a.dupe(u8, workflow_type),
        .cancellation_type = options.cancellation_type,
    };
    state.context = inst.converter_context;
    state.context.workflow_id = state.workflow_id;
    state.context.workflow_type = state.workflow_type;
    var cmd: cmd_pb.StartChildWorkflowExecution = .{
        .seq = state.seq,
        .namespace = inst.namespace,
        .workflow_id = state.workflow_id,
        .workflow_type = workflow_type,
        .task_queue = if (options.task_queue.len > 0) options.task_queue else inst.info.task_queue,
        .input = try payloadList(a, args),
        .headers = try header.toEntries(cmd_pb.StartChildWorkflowExecution.HeadersEntry, a),
        .workflow_execution_timeout = proto_time.durationToProto(options.execution_timeout),
        .workflow_run_timeout = proto_time.durationToProto(options.run_timeout),
        .workflow_task_timeout = proto_time.durationToProto(options.task_timeout),
        .parent_close_policy = switch (options.parent_close_policy) {
            .terminate => .PARENT_CLOSE_POLICY_TERMINATE,
            .abandon => .PARENT_CLOSE_POLICY_ABANDON,
            .request_cancel => .PARENT_CLOSE_POLICY_REQUEST_CANCEL,
        },
        .workflow_id_reuse_policy = switch (options.id_reuse_policy) {
            .unspecified => .WORKFLOW_ID_REUSE_POLICY_UNSPECIFIED,
            .allow_duplicate => .WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
            .allow_duplicate_failed_only => .WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY,
            .reject_duplicate => .WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
        },
        .cron_schedule = options.cron_schedule,
        .cancellation_type = switch (options.cancellation_type) {
            .try_cancel => .TRY_CANCEL,
            .wait_cancellation_requested => .WAIT_CANCELLATION_REQUESTED,
            .wait_cancellation_completed => .WAIT_CANCELLATION_COMPLETED,
            .abandon => .ABANDON,
        },
    };
    if (options.retry_policy) |rp| cmd.retry_policy = try protoRetryPolicy(a, rp);
    if (options.priority) |p| cmd.priority = .{
        .priority_key = p.priority_key,
        .fairness_key = p.fairness_key,
        .fairness_weight = p.fairness_weight,
    };
    try cmd.memo.ensureTotalCapacity(a, options.memo.len);
    for (options.memo) |f| cmd.memo.appendAssumeCapacity(.{ .key = f.name, .value = f.value });
    if (options.search_attributes.len > 0) cmd.search_attributes = sa.toProto(a, options.search_attributes, .start) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        error.InvalidSearchAttribute => return error.InvalidOptions,
    };
    try inst.children.put(a, state.seq, state);
    inst.next_child_seq += 1;
    try inst.emit(.{
        .user_metadata = if (options.static_summary.len > 0 or options.static_details.len > 0) .{
            .summary = if (options.static_summary.len > 0) try converter.toPayload(a, options.static_summary) else null,
            .details = if (options.static_details.len > 0) try converter.toPayload(a, options.static_details) else null,
        } else null,
        .variant = .{ .start_child_workflow_execution = cmd },
    });
    return state;
}

/// Blocks the calling coroutine until the child reaches `phase` (started, or closed), or
/// can't: a child that fails to start or closes before it started reaches both. Cancellation
/// works as in `awaitActivity`: it cancels the child, and with `wait_cancellation_completed`
/// waits for the core to resolve it (the child closed, or never started), and with
/// `wait_cancellation_requested` until the server recorded the request too. Eviction ends
/// the wait with error.Canceled.
pub fn awaitChild(inst: *Instance, state: *ChildState, phase: ChildState.Phase) error{Canceled}!void {
    if (state.reached(phase)) return;
    if (inst.wio.refuse(switch (phase) {
        .started => "ChildWorkflowFuture.started",
        .result => "ChildWorkflowFuture.get",
    })) return error.Canceled;
    const reason: WorkflowIo.Wait.Reason = .{ .what = switch (phase) {
        .started => "the start of child workflow",
        .result => "child workflow",
    }, .name = state.workflow_type };
    var wait = inst.wio.newWaitPoint(reason);
    const waiter = switch (phase) {
        .started => &state.start_waiter,
        .result => &state.result_waiter,
    };
    waiter.* = &wait;
    defer waiter.* = null;
    var canceled = false;
    while (!state.reached(phase)) {
        if (inst.evicting) return error.Canceled;
        wait = inst.wio.newWaitPoint(reason);
        inst.wio.waitFor(&wait, !canceled) catch |err| {
            inst.cancelChild(state);
            switch (state.cancellation_type) {
                .try_cancel, .abandon => return err,
                .wait_cancellation_requested, .wait_cancellation_completed => canceled = true,
            }
        };
    }
    if (!canceled) return;
    if (state.outcome) |o| if (o == .cancelled) return error.Canceled;
    inst.wio.io().recancel();
}

/// Sends CancelChildWorkflowExecution; the core resolves the child as its cancellation type
/// says (and resolves a child whose start it hasn't sent yet as canceled before starting).
/// The core never reports when the server recorded the request it sends, so for a started
/// child under `wait_cancellation_requested` this sends RequestCancelExternalWorkflowExecution
/// itself instead, and `resolveExternal` resolves the child as canceled once it succeeds. A
/// child not started yet goes to the core, which cancels it before it is sent or requests its
/// cancellation; `resolveChildStart` then sends that request once the child starts.
pub fn cancelChild(inst: *Instance, state: *ChildState) void {
    if (state.outcome != null or state.cancel_requested) return;
    if (inst.wio.refuse("ChildWorkflowFuture.cancel")) return;
    state.cancel_requested = true;
    if (state.cancellation_type == .wait_cancellation_requested and state.run_id != null) return inst.requestChildCancel(state);
    inst.emit(.{ .variant = .{ .cancel_child_workflow_execution = .{
        .child_workflow_seq = state.seq,
        .reason = "the parent canceled the child workflow",
    } } }) catch {
        inst.task_failure = "out of memory requesting child workflow cancellation";
    };
}

/// Sends RequestCancelExternalWorkflowExecution for a started child canceled under
/// `wait_cancellation_requested`.
fn requestChildCancel(inst: *Instance, state: *ChildState) void {
    const ext = inst.requestCancelExternal(state.workflow_id, state.run_id.?) catch {
        inst.task_failure = "out of memory requesting child workflow cancellation";
        return;
    };
    ext.child = state;
    state.cancel_request = ext;
}

/// The server recorded the cancellation request `requestChildCancel` sent: the child resolves
/// as canceled unless it already closed. A refused request (the child already closed) leaves
/// it to the core's resolution.
fn childCancelRecorded(inst: *Instance, state: *ChildState) !void {
    if (state.outcome != null) return;
    if (!inst.children.remove(state.seq)) return;
    const a = inst.arena();
    const cause = try a.create(Failure);
    cause.* = .{ .message = "the server recorded the child workflow's cancellation request", .source = failure_mod.source, .failure_info = .{ .canceled_failure_info = .{} } };
    const out = try a.create(Failure);
    out.* = .{
        .message = "Child Workflow execution cancelled",
        .source = failure_mod.source,
        .cause = cause,
        .failure_info = .{ .child_workflow_execution_failure_info = .{
            .namespace = inst.namespace,
            .workflow_execution = .{ .workflow_id = state.workflow_id, .run_id = state.run_id orelse "" },
            .workflow_type = .{ .name = state.workflow_type },
        } },
    };
    state.outcome = .{ .cancelled = out };
    state.wake(&inst.wio);
}

fn resolveChildStart(inst: *Instance, r: *const act_pb.ResolveChildWorkflowExecutionStart) !void {
    const state = inst.children.get(r.seq) orelse return;
    const a = inst.arena();
    switch (r.status orelse return) {
        .succeeded => |s| {
            state.run_id = try a.dupe(u8, s.run_id);
            if (state.cancel_requested and state.cancellation_type == .wait_cancellation_requested and state.cancel_request == null) inst.requestChildCancel(state);
        },
        .failed => |f| {
            _ = inst.children.remove(r.seq);
            state.already_started = f.cause == .START_CHILD_WORKFLOW_EXECUTION_FAILED_CAUSE_WORKFLOW_ALREADY_EXISTS;
            state.outcome = .{ .failed = try startFailure(inst, state, f.cause) };
        },
        .cancelled => |c| {
            _ = inst.children.remove(r.seq);
            state.outcome = .{ .cancelled = try inst.receivedFailure(&state.context, c.failure) };
        },
    }
    state.wake(&inst.wio);
}

/// A failure for a child the server refused to start; the core reports only the cause.
fn startFailure(inst: *Instance, state: *const ChildState, cause: proto.coresdk.child_workflow.StartChildWorkflowExecutionFailedCause) !*const Failure {
    const a = inst.arena();
    const reason = switch (cause) {
        .START_CHILD_WORKFLOW_EXECUTION_FAILED_CAUSE_WORKFLOW_ALREADY_EXISTS => "a workflow with this ID already exists",
        .START_CHILD_WORKFLOW_EXECUTION_FAILED_CAUSE_NAMESPACE_NOT_FOUND => "the namespace was not found",
        .START_CHILD_WORKFLOW_EXECUTION_FAILED_CAUSE_INVALID_VERSIONING_OVERRIDE => "the versioning override is invalid",
        else => "the server gave no cause",
    };
    const out = try a.create(Failure);
    out.* = .{
        .message = try std.fmt.allocPrint(a, "starting child workflow \"{s}\" with ID \"{s}\" failed: {s}", .{ state.workflow_type, state.workflow_id, reason }),
        .source = failure_mod.source,
        .failure_info = .{ .child_workflow_execution_failure_info = .{
            .namespace = inst.namespace,
            .workflow_execution = .{ .workflow_id = state.workflow_id },
            .workflow_type = .{ .name = state.workflow_type },
        } },
    };
    return out;
}

fn resolveChild(inst: *Instance, r: *const act_pb.ResolveChildWorkflowExecution) !void {
    const kv = inst.children.fetchRemove(r.seq) orelse return;
    const state = kv.value;
    const a = inst.arena();
    const status = (r.result orelse return).status orelse return;
    state.outcome = switch (status) {
        .completed => |s| .{ .completed = if (s.result) |p| try p.dupe(a) else null },
        .failed => |f| .{ .failed = try inst.receivedFailure(&state.context, f.failure) },
        .cancelled => |c| .{ .cancelled = try inst.receivedFailure(&state.context, c.failure) },
    };
    state.wake(&inst.wio);
}

// ---------------------------------------------------------------- Nexus operations

/// Validates the options and emits ScheduleNexusOperation with the encoded `payload`: the
/// root of the outbound interceptors' `executeNexusOperation`, so validation sees what they
/// pass on. The names, endpoint and headers are copied into the arena; `payload` must be
/// arena memory or outlive the activation.
pub fn scheduleNexusOperation(inst: *Instance, service: []const u8, operation: []const u8, options: workflow.NexusOperationOptions, payload: converter.Payload) ScheduleError!*NexusOperationState {
    const fields = .{ .NexusEndpoint = options.endpoint, .NexusService = service, .NexusOperation = operation };
    if (options.endpoint.len == 0) {
        inst.logger.warn("executeNexusOperation: NexusOperationOptions.endpoint is empty; name the Nexus endpoint to call through", fields);
        return error.InvalidOptions;
    }
    if (service.len == 0 or operation.len == 0) {
        inst.logger.warn("executeNexusOperationByName: the service or operation name is empty; name both", fields);
        return error.InvalidOptions;
    }
    inline for (.{ "schedule_to_close_timeout", "schedule_to_start_timeout", "start_to_close_timeout" }) |name| {
        if (@field(options, name)) |d| if (d.nanoseconds < 0) {
            inst.logger.warn("executeNexusOperation: NexusOperationOptions." ++ name ++ " is negative; leave it null for no limit", fields);
            return error.InvalidOptions;
        };
    }
    const a = inst.arena();
    const state = try a.create(NexusOperationState);
    state.* = .{
        .seq = inst.next_nexus_seq,
        .endpoint = try a.dupe(u8, options.endpoint),
        .service = try a.dupe(u8, service),
        .operation = try a.dupe(u8, operation),
        .cancellation_type = options.cancellation_type,
    };
    var cmd: cmd_pb.ScheduleNexusOperation = .{
        .seq = state.seq,
        .endpoint = state.endpoint,
        .service = state.service,
        .operation = state.operation,
        .input = payload,
        .schedule_to_close_timeout = proto_time.durationToProto(options.schedule_to_close_timeout),
        .schedule_to_start_timeout = proto_time.durationToProto(options.schedule_to_start_timeout),
        .start_to_close_timeout = proto_time.durationToProto(options.start_to_close_timeout),
        .cancellation_type = switch (options.cancellation_type) {
            .wait_cancellation_completed => .WAIT_CANCELLATION_COMPLETED,
            .wait_cancellation_requested => .WAIT_CANCELLATION_REQUESTED,
            .try_cancel => .TRY_CANCEL,
            .abandon => .ABANDON,
        },
    };
    try cmd.nexus_header.ensureTotalCapacity(a, options.headers.len);
    for (options.headers) |h| cmd.nexus_header.appendAssumeCapacity(.{ .key = try a.dupe(u8, h.name), .value = try a.dupe(u8, h.value) });
    try inst.nexus_operations.put(a, state.seq, state);
    inst.next_nexus_seq += 1;
    try inst.emit(.{
        .user_metadata = try summaryMetadata(a, options.summary),
        .variant = .{ .schedule_nexus_operation = cmd },
    });
    return state;
}

/// Blocks the calling coroutine until the operation reaches `phase` (started, or ended), or
/// can't: an operation whose start failed, or that ended before it started, reaches both.
/// When the coroutine is canceled while waiting, requests the operation's cancellation and
/// keeps waiting, uncanceled, for the core to resolve it: the core decides when, by the
/// cancellation type. Returns error.Canceled if it resolved as canceled; otherwise its
/// outcome is kept for the caller and the cancel request is re-armed, so the coroutine's next
/// cancelable wait returns error.Canceled. Eviction ends the wait with error.Canceled.
pub fn awaitNexusOperation(inst: *Instance, state: *NexusOperationState, phase: NexusOperationState.Phase) error{Canceled}!void {
    if (state.reached(phase)) return;
    if (inst.wio.refuse(switch (phase) {
        .started => "NexusOperationFuture.started",
        .result => "NexusOperationFuture.get",
    })) return error.Canceled;
    const reason: WorkflowIo.Wait.Reason = .{ .what = switch (phase) {
        .started => "the start of Nexus operation",
        .result => "Nexus operation",
    }, .name = state.operation };
    var wait = inst.wio.newWaitPoint(reason);
    const waiter = switch (phase) {
        .started => &state.start_waiter,
        .result => &state.result_waiter,
    };
    waiter.* = &wait;
    defer waiter.* = null;
    var canceled = false;
    while (!state.reached(phase)) {
        if (inst.evicting) return error.Canceled;
        wait = inst.wio.newWaitPoint(reason);
        inst.wio.waitFor(&wait, !canceled) catch {
            inst.cancelNexusOperation(state);
            canceled = true;
        };
    }
    if (!canceled) return;
    if (state.outcome) |o| if (o == .canceled) return error.Canceled;
    inst.wio.io().recancel();
}

/// Sends RequestCancelNexusOperation; the core resolves the operation as its cancellation
/// type says (see `workflow.NexusOperationCancellationType`).
pub fn cancelNexusOperation(inst: *Instance, state: *NexusOperationState) void {
    if (state.outcome != null or state.cancel_requested) return;
    if (inst.wio.refuse("NexusOperationFuture.cancel")) return;
    state.cancel_requested = true;
    inst.emit(.{ .variant = .{ .request_cancel_nexus_operation = .{ .seq = state.seq } } }) catch {
        inst.task_failure = "out of memory requesting Nexus operation cancellation";
    };
}

/// Applies ResolveNexusOperationStart. A failed start ends the operation: the core sends no
/// ResolveNexusOperation after it. A job for an operation already resolved (the core may
/// resolve a canceled operation before its start) is ignored.
fn resolveNexusOperationStart(inst: *Instance, r: *const act_pb.ResolveNexusOperationStart) !void {
    const state = inst.nexus_operations.get(r.seq) orelse return;
    const a = inst.arena();
    switch (r.status orelse return) {
        .operation_token => |token| {
            state.token = try a.dupe(u8, token);
            state.started = true;
        },
        .started_sync => state.started = true,
        .failed => |f| {
            _ = inst.nexus_operations.remove(r.seq);
            const copy = try inst.receivedFailure(&inst.nexusContext(), f);
            state.outcome = if (hasFailureInfo(copy, .canceled_failure_info))
                .{ .canceled = copy }
            else if (hasFailureInfo(copy, .timeout_failure_info))
                .{ .timed_out = copy }
            else
                .{ .failed = copy };
        },
    }
    state.wake(&inst.wio);
}

/// Whether `f` or its direct cause carries `info`: the core wraps a cancellation or timeout in
/// a Nexus operation failure.
fn hasFailureInfo(f: *const Failure, info: std.meta.Tag(Failure.failure_info_union)) bool {
    if (f.failure_info) |i| if (i == info) return true;
    const cause = f.cause orelse return false;
    const i = cause.failure_info orelse return false;
    return i == info;
}

fn resolveNexusOperation(inst: *Instance, r: *const act_pb.ResolveNexusOperation) !void {
    const kv = inst.nexus_operations.fetchRemove(r.seq) orelse return;
    const state = kv.value;
    const a = inst.arena();
    const status = (r.result orelse NexusResult{}).status orelse {
        state.outcome = .{ .failed = try dupeFailure(a, .{ .message = "the core resolved the Nexus operation without a result" }) };
        state.wake(&inst.wio);
        return;
    };
    state.outcome = switch (status) {
        .completed => |p| .{ .completed = try p.dupe(a) },
        .failed => |f| .{ .failed = try inst.receivedFailure(&inst.nexusContext(), f) },
        .cancelled => |f| .{ .canceled = try inst.receivedFailure(&inst.nexusContext(), f) },
        .timed_out => |f| .{ .timed_out = try inst.receivedFailure(&inst.nexusContext(), f) },
    };
    state.wake(&inst.wio);
}

const NexusResult = proto.coresdk.nexus.NexusOperationResult;

// ---------------------------------------------------------------- signal-with-start

/// Validates `options` and schedules the signal-with-start: a ScheduleNexusOperation on the
/// system endpoint whose input is the request's envelope. The request names no request ID,
/// clock, or random value, so it is the same bytes on replay. `args`, `signal_args` and
/// `header` must be arena memory; the request's strings are serialized into the envelope.
fn sendSignalWithStart(
    inst: *Instance,
    workflow_type: []const u8,
    signal_name: []const u8,
    options: workflow.SignalWithStartOptions,
    args: []const converter.Payload,
    signal_args: []const converter.Payload,
    header: converter.Header,
) ScheduleError!*NexusOperationState {
    const fields = .{ .WorkflowID = options.id, .WorkflowType = workflow_type, .SignalName = signal_name };
    if (options.id.len == 0) {
        inst.logger.warn("signalWithStartWorkflow: SignalWithStartOptions.id is empty; name the workflow to start and signal", fields);
        return error.InvalidOptions;
    }
    if (workflow_type.len == 0 or signal_name.len == 0) {
        inst.logger.warn("signalWithStartWorkflowByName: the workflow type or signal name is empty; name both", fields);
        return error.InvalidOptions;
    }
    if (options.id_conflict_policy == .fail) {
        inst.logger.warn("signalWithStartWorkflow: SignalWithStartOptions.id_conflict_policy .fail is refused by the server for signal-with-start; use .use_existing", fields);
        return error.InvalidOptions;
    }
    inline for (.{ "execution_timeout", "run_timeout", "task_timeout", "start_delay" }) |name| {
        if (@field(options, name)) |d| if (d.nanoseconds < 0) {
            inst.logger.warn("signalWithStartWorkflow: SignalWithStartOptions." ++ name ++ " is negative; leave it null for none", fields);
            return error.InvalidOptions;
        };
    }
    // The server checks these while validating the command, and a command it refuses fails
    // the workflow task on every attempt, so `get` would never return.
    if (options.start_delay != null and options.cron_schedule.len > 0) {
        inst.logger.warn("signalWithStartWorkflow: SignalWithStartOptions.start_delay and cron_schedule can't be used together; the server refuses the pair", fields);
        return error.InvalidOptions;
    }
    inline for (.{ "id", "task_queue", "cron_schedule" }) |name| {
        if (!std.unicode.utf8ValidateSlice(@field(options, name))) {
            inst.logger.warn("signalWithStartWorkflow: SignalWithStartOptions." ++ name ++ " is not valid UTF-8", fields);
            return error.InvalidOptions;
        }
    }
    if (!std.unicode.utf8ValidateSlice(workflow_type) or !std.unicode.utf8ValidateSlice(signal_name)) {
        inst.logger.warn("signalWithStartWorkflowByName: the workflow type or signal name is not valid UTF-8", fields);
        return error.InvalidOptions;
    }
    if (options.retry_policy) |rp| if (retryPolicyError(rp)) |problem| {
        inst.logger.warn("signalWithStartWorkflow: SignalWithStartOptions.retry_policy is one the server refuses", .{ .WorkflowID = options.id, .WorkflowType = workflow_type, .Problem = problem });
        return error.InvalidOptions;
    };
    const a = inst.arena();
    var request: system.SignalWithStartRequest = .{
        .namespace = inst.namespace,
        .workflow_id = options.id,
        .workflow_type = .{ .name = workflow_type },
        .task_queue = .{ .name = if (options.task_queue.len > 0) options.task_queue else inst.info.task_queue, .kind = .TASK_QUEUE_KIND_NORMAL },
        .input = .{ .payloads = try payloadList(a, args) },
        .workflow_execution_timeout = proto_time.durationToProto(options.execution_timeout),
        .workflow_run_timeout = proto_time.durationToProto(options.run_timeout),
        .workflow_task_timeout = proto_time.durationToProto(options.task_timeout),
        .workflow_start_delay = proto_time.durationToProto(options.start_delay),
        .workflow_id_reuse_policy = switch (options.id_reuse_policy) {
            .unspecified => .WORKFLOW_ID_REUSE_POLICY_UNSPECIFIED,
            .allow_duplicate => .WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
            .allow_duplicate_failed_only => .WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY,
            .reject_duplicate => .WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
        },
        .workflow_id_conflict_policy = switch (options.id_conflict_policy) {
            .unspecified => .WORKFLOW_ID_CONFLICT_POLICY_UNSPECIFIED,
            .fail => unreachable,
            .use_existing => .WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
            .terminate_existing => .WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING,
        },
        .signal_name = signal_name,
        .signal_input = .{ .payloads = try payloadList(a, signal_args) },
        .cron_schedule = options.cron_schedule,
    };
    if (options.retry_policy) |rp| request.retry_policy = try protoRetryPolicy(a, rp);
    if (options.memo.len > 0) {
        var memo: common.Memo = .{};
        try memo.fields.ensureTotalCapacity(a, options.memo.len);
        for (options.memo) |f| memo.fields.appendAssumeCapacity(.{ .key = f.name, .value = f.value });
        request.memo = memo;
    }
    if (options.search_attributes.len > 0) request.search_attributes = sa.toProto(a, options.search_attributes, .start) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        error.InvalidSearchAttribute => {
            inst.logger.warn("signalWithStartWorkflow: SignalWithStartOptions.search_attributes holds a value the server can't index", fields);
            return error.InvalidOptions;
        },
    };
    if (header.fields.len > 0) request.header = .{ .fields = try header.toEntries(common.Header.FieldsEntry, a) };
    const payload = try system.envelope(a, request);
    return inst.scheduleNexusOperation(system.workflow_service, system.signal_with_start_operation, .{ .endpoint = system.endpoint }, payload);
}

/// Why the server would refuse `rp` (temporalio/temporal common/retrypolicy
/// `ValidateRetryPolicy`), or null if it accepts it. Zero values mean the server's defaults.
fn retryPolicyError(rp: workflow.RetryPolicy) ?[]const u8 {
    if (rp.maximum_attempts < 0) return "maximum_attempts is negative; use 0 for unlimited";
    if (rp.backoff_coefficient != 0 and !(rp.backoff_coefficient >= 1)) return "backoff_coefficient is below 1; use 0 for the default";
    const initial: i96 = if (rp.initial_interval) |d| d.nanoseconds else 0;
    const maximum: i96 = if (rp.maximum_interval) |d| d.nanoseconds else 0;
    if (initial < 0) return "initial_interval is negative";
    if (maximum < 0) return "maximum_interval is negative";
    if (maximum > 0 and maximum < initial) return "maximum_interval is less than initial_interval";
    return null;
}

// ---------------------------------------------------------------- external workflows

/// Whom a signal goes to: a workflow by ID (and run ID, empty for its latest run), or a
/// child of this workflow by its workflow ID.
pub const SignalTarget = union(enum) {
    execution: struct { workflow_id: []const u8, run_id: []const u8 },
    child: []const u8,
};

/// Emits SignalExternalWorkflowExecution sending `signal_name` with `args` and `header` (arena
/// memory) to `target`. The name and IDs are copied into the arena.
fn sendSignalExternal(inst: *Instance, target: SignalTarget, signal_name: []const u8, args: []const converter.Payload, header: converter.Header) ScheduleError!*ExternalState {
    const workflow_id = switch (target) {
        .execution => |e| e.workflow_id,
        .child => |id| id,
    };
    if (workflow_id.len == 0) {
        inst.logger.warn("signalExternalWorkflow: the target workflow ID is empty", .{ .SignalName = signal_name });
        return error.InvalidOptions;
    }
    const a = inst.arena();
    const state = try inst.newExternal(.signal, workflow_id);
    try inst.emit(.{ .variant = .{ .signal_external_workflow_execution = .{
        .seq = state.seq,
        .signal_name = try a.dupe(u8, signal_name),
        .args = try payloadList(a, args),
        .headers = try header.toEntries(cmd_pb.SignalExternalWorkflowExecution.HeadersEntry, a),
        .target = switch (target) {
            .execution => |e| .{ .workflow_execution = .{
                .namespace = inst.namespace,
                .workflow_id = state.workflow_id,
                .run_id = try a.dupe(u8, e.run_id),
            } },
            .child => .{ .child_workflow_id = state.workflow_id },
        },
    } } });
    return state;
}

/// Emits RequestCancelExternalWorkflowExecution for the workflow `workflow_id` (`run_id` empty
/// for its latest run). The IDs are copied into the arena.
fn requestCancelExternal(inst: *Instance, workflow_id: []const u8, run_id: []const u8) ScheduleError!*ExternalState {
    if (workflow_id.len == 0) {
        inst.logger.warn("cancelExternalWorkflow: the target workflow ID is empty", .{});
        return error.InvalidOptions;
    }
    const a = inst.arena();
    const state = try inst.newExternal(.cancel, workflow_id);
    try inst.emit(.{ .variant = .{ .request_cancel_external_workflow_execution = .{
        .seq = state.seq,
        .workflow_execution = .{
            .namespace = inst.namespace,
            .workflow_id = state.workflow_id,
            .run_id = try a.dupe(u8, run_id),
        },
    } } });
    return state;
}

fn newExternal(inst: *Instance, kind: ExternalState.Kind, workflow_id: []const u8) !*ExternalState {
    const a = inst.arena();
    const state = try a.create(ExternalState);
    state.* = .{ .seq = inst.next_external_seq, .kind = kind, .workflow_id = try a.dupe(u8, workflow_id) };
    try inst.externals.put(a, state.seq, state);
    inst.next_external_seq += 1;
    return state;
}

/// Blocks the calling coroutine until the core resolves the signal or cancellation request.
/// When the coroutine is canceled while waiting, returns error.Canceled at once; for a signal
/// it also sends CancelSignalWorkflow, which withdraws the signal if the server hasn't
/// recorded it yet (a recorded signal is delivered anyway). A cancellation request can't be
/// withdrawn. Eviction ends the wait with error.Canceled.
pub fn awaitExternal(inst: *Instance, state: *ExternalState) error{Canceled}!void {
    if (state.outcome != null) return;
    if (inst.wio.refuse(switch (state.kind) {
        .signal => "SignalExternalFuture.get",
        .cancel => "CancelExternalFuture.get",
    })) return error.Canceled;
    const reason: WorkflowIo.Wait.Reason = .{ .what = switch (state.kind) {
        .signal => "a signal to external workflow",
        .cancel => "a cancellation request to external workflow",
    }, .name = state.workflow_id };
    var wait = inst.wio.newWaitPoint(reason);
    state.waiter = &wait;
    defer state.waiter = null;
    while (state.outcome == null) {
        if (inst.evicting) return error.Canceled;
        wait = inst.wio.newWaitPoint(reason);
        inst.wio.waitFor(&wait, true) catch |err| {
            inst.cancelExternal(state);
            return err;
        };
    }
}

fn cancelExternal(inst: *Instance, state: *ExternalState) void {
    if (state.kind != .signal or state.outcome != null or state.cancel_requested) return;
    state.cancel_requested = true;
    inst.emit(.{ .variant = .{ .cancel_signal_workflow = .{ .seq = state.seq } } }) catch {
        inst.task_failure = "out of memory canceling a signal to an external workflow";
    };
}

/// Applies a ResolveSignalExternalWorkflow or ResolveRequestCancelExternalWorkflow job: no
/// failure means the server delivered it. A canceled failure is a signal withdrawn before it
/// was sent; `not_found` is from the job's cause.
fn resolveExternal(inst: *Instance, seq: u32, failure: ?Failure, not_found: bool) !void {
    const kv = inst.externals.fetchRemove(seq) orelse return;
    const state = kv.value;
    const f = failure orelse {
        state.outcome = .delivered;
        if (state.waiter) |w| inst.wio.wake(w);
        if (state.child) |child| try inst.childCancelRecorded(child);
        return;
    };
    const a = inst.arena();
    const copy = try a.create(Failure);
    copy.* = try f.dupe(a);
    if (inst.failure_converter) |c| copy.* = try c.fromFailure(a, &inst.converter_context, copy.*);
    if (copy.failure_info != null and copy.failure_info.? == .canceled_failure_info) {
        state.outcome = .{ .canceled = copy };
    } else {
        copy.message = try std.fmt.allocPrint(a, "{s} (workflow ID \"{s}\")", .{ copy.message, state.workflow_id });
        state.outcome = if (not_found) .{ .not_found = copy } else .{ .failed = copy };
    }
    if (state.waiter) |w| inst.wio.wake(w);
}

/// A failure user code will read, from the core: a copy in the run's arena of `f`, passed
/// through the failure converter with `context`. A missing failure is the SDK's own report
/// and isn't converted. Called from the job loop outside the coroutines, where
/// `Scheduler.sdk_work` is clear, so a converter that blocks counts as workflow code.
fn receivedFailure(inst: *Instance, context: *const converter.Context, f: ?Failure) !*const Failure {
    const a = inst.arena();
    const v = f orelse return dupeFailure(a, null);
    const out = try a.create(Failure);
    out.* = try v.dupe(a);
    if (inst.failure_converter) |c| out.* = try c.fromFailure(a, context, out.*);
    return out;
}

/// The failure `f` that user code's error `err` came to, as the failure converter sends it
/// with `context`, or `f` without a converter. Memory from `a`. Never called under
/// `Scheduler.sdk_work`, so a converter that blocks counts as workflow code.
fn sendFailure(inst: *const Instance, a: Allocator, context: *const converter.Context, err: anyerror, f: Failure) error{OutOfMemory}!Failure {
    const c = inst.failure_converter orelse return f;
    return c.toFailure(a, context, err, f);
}

fn dupeFailure(a: Allocator, f: ?Failure) !*const Failure {
    const out = try a.create(Failure);
    out.* = if (f) |v| try v.dupe(a) else .{ .message = "failed without a failure from the core" };
    return out;
}

// ---------------------------------------------------------------- interceptor chain ends

const inbound_root: interceptor.WorkflowInbound.VTable = .{
    .executeWorkflow = rootExecuteWorkflow,
    .handleSignal = rootHandleSignal,
    .handleQuery = rootHandleQuery,
    .validateUpdate = rootValidateUpdate,
    .executeUpdate = rootExecuteUpdate,
};

fn rootExecuteWorkflow(ptr: *anyopaque, input: interceptor.ExecuteWorkflowInput, _: interceptor.WorkflowInbound.Next) anyerror!converter.Payload {
    const inst: *Instance = @ptrCast(@alignCast(ptr));
    return inst.entry.invoke(&inst.ctx, input.args.payloads);
}

/// The handler run of the calling coroutine, for the chains that end in a handler. An
/// interceptor that calls `next` from another coroutine gets `error.HandlerNotFound`.
fn handlerOfCaller(inst: *Instance, comptime kind: std.meta.Tag(@FieldType(HandlerRun, "kind"))) error{HandlerNotFound}!@FieldType(@FieldType(HandlerRun, "kind"), @tagName(kind)) {
    const run = inst.currentHandler() orelse {
        inst.logger.warn("a workflow interceptor called next for a " ++ @tagName(kind) ++ " from another coroutine than the handler's; call it from the coroutine the interceptor was called on", .{});
        return error.HandlerNotFound;
    };
    return @field(run.kind, @tagName(kind));
}

fn rootHandleSignal(ptr: *anyopaque, input: interceptor.HandleSignalInput, _: interceptor.WorkflowInbound.Next) anyerror!void {
    const inst: *Instance = @ptrCast(@alignCast(ptr));
    const handler = (try inst.handlerOfCaller(.signal)).handler;
    const args = handler.decode(inst.arena(), &inst.converter_context, input.signal_name, input.args.payloads) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => {
            inst.logger.warn("dropping a signal whose arguments don't decode as its handler's parameters", .{ .SignalName = input.signal_name, .error_name = err });
            return;
        },
    };
    return handler.call(handler.context, args);
}

fn rootHandleQuery(ptr: *anyopaque, input: interceptor.HandleQueryInput, _: interceptor.WorkflowInbound.Next) anyerror!converter.Payload {
    const inst: *Instance = @ptrCast(@alignCast(ptr));
    if (inst.queries.get(input.query_type)) |handler| return handler.call(handler.context, input.arena, &inst.converter_context, input.args.payloads);
    if (std.mem.eql(u8, input.query_type, stack_trace_query)) return converter.toPayloadWithContext(input.arena, &inst.converter_context, try inst.stackTrace(input.arena));
    if (reservedQuery(input.query_type)) return error.QueryNotRegistered;
    const dynamic = inst.dynamic_query_handler orelse return error.QueryNotRegistered;
    return dynamic.call(dynamic.context, input.arena, &inst.converter_context, input.query_type, input.args.payloads);
}

/// Whether `query_type` names one of the queries Temporal's tools send for themselves (stack
/// traces, workflow metadata), which a dynamic query handler doesn't answer.
fn reservedQuery(query_type: []const u8) bool {
    return std.mem.eql(u8, query_type, "__stack_trace") or
        std.mem.eql(u8, query_type, "__enhanced_stack_trace") or
        std.mem.startsWith(u8, query_type, "__temporal");
}

fn rootValidateUpdate(ptr: *anyopaque, input: interceptor.UpdateInput, _: interceptor.WorkflowInbound.Next) anyerror!void {
    const inst: *Instance = @ptrCast(@alignCast(ptr));
    const section = inst.readOnly() orelse return error.HandlerNotFound;
    const handler = section.update_handler orelse return error.HandlerNotFound;
    const args = try handler.decode(inst.arena(), &inst.converter_context, input.update_name, input.args.payloads);
    if (handler.validate) |validate| try validate(handler.context, args);
}

fn rootExecuteUpdate(ptr: *anyopaque, input: interceptor.UpdateInput, _: interceptor.WorkflowInbound.Next) anyerror!converter.Payload {
    const inst: *Instance = @ptrCast(@alignCast(ptr));
    const handler = (try inst.handlerOfCaller(.update)).handler;
    const a = inst.arena();
    return handler.call(handler.context, a, &inst.converter_context, try handler.decode(a, &inst.converter_context, input.update_name, input.args.payloads));
}

const outbound_root: interceptor.WorkflowOutbound.VTable = .{
    .executeActivity = rootExecuteActivity,
    .executeLocalActivity = rootExecuteLocalActivity,
    .executeChildWorkflow = rootExecuteChildWorkflow,
    .signalExternalWorkflow = rootSignalExternalWorkflow,
    .cancelExternalWorkflow = rootCancelExternalWorkflow,
    .continueAsNew = rootContinueAsNew,
    .sideEffect = rootSideEffect,
    .executeNexusOperation = rootExecuteNexusOperation,
    .signalWithStartWorkflow = rootSignalWithStartWorkflow,
    .logger = rootLogger,
};

fn rootExecuteActivity(ptr: *anyopaque, input: interceptor.ActivityInput, _: interceptor.WorkflowOutbound.Next) workflow.ActivityFuture(converter.Payload) {
    const inst: *Instance = @ptrCast(@alignCast(ptr));
    const options = if (input.options.session) |s| inst.sessionActivityOptions(input, s) catch |err| return .{ .ctx = &inst.ctx, .state = err } else input.options;
    return .{ .ctx = &inst.ctx, .state = inst.scheduleActivity(input.activity_type, options, input.args.payloads, input.header.header()) };
}

/// The options that run an activity in `session`, on its host's task queue, after writing the
/// session's ID into the activity's header. Nothing is scheduled for a session that isn't
/// open.
fn sessionActivityOptions(inst: *Instance, input: interceptor.ActivityInput, session: *const workflow.Session) ActivityScheduleError!workflow.ActivityOptions {
    if (input.options.task_queue.len > 0) {
        inst.logger.warn("executeActivity: ActivityOptions.session and task_queue are both set; an activity in a session runs on its host's task queue, so leave task_queue empty", .{ .ActivityType = input.activity_type, .SessionID = session.id });
        return error.InvalidOptions;
    }
    switch (session.state()) {
        .open => {},
        .failed => return error.SessionFailed,
        .closed => {
            inst.logger.warn("executeActivity: the activity's session is closed: completeSession was called on it; create or recreate a session", .{ .ActivityType = input.activity_type, .SessionID = session.id });
            return error.InvalidOptions;
        },
    }
    try input.header.set(session_id_header, session.id);
    var options = input.options;
    options.task_queue = session.task_queue;
    options.session = null;
    return options;
}

fn rootExecuteLocalActivity(ptr: *anyopaque, input: interceptor.LocalActivityInput, _: interceptor.WorkflowOutbound.Next) workflow.ActivityFuture(converter.Payload) {
    const inst: *Instance = @ptrCast(@alignCast(ptr));
    return .{ .ctx = &inst.ctx, .state = inst.scheduleLocalActivity(input.activity_type, input.options, input.args.payloads, input.header.header()) };
}

fn rootSideEffect(ptr: *anyopaque, input: interceptor.SideEffectInput, _: interceptor.WorkflowOutbound.Next) workflow.ActivityFuture(converter.Payload) {
    const inst: *Instance = @ptrCast(@alignCast(ptr));
    const options: workflow.LocalActivityOptions = .{ .start_to_close_timeout = .fromSeconds(10) };
    const value: [1]converter.Payload = .{input.value orelse .{}};
    const args: []const converter.Payload = if (input.value != null) &value else &.{};
    const state = inst.scheduleLocalActivity(side_effect_activity_type, options, args, .{}) catch |err| return .{ .ctx = &inst.ctx, .state = err };
    // A side effect's value is the run's own, encoded in the workflow with its context.
    state.context = inst.converter_context;
    return .{ .ctx = &inst.ctx, .state = state };
}

fn rootExecuteChildWorkflow(ptr: *anyopaque, input: interceptor.ChildWorkflowInput, _: interceptor.WorkflowOutbound.Next) workflow.ChildWorkflowFuture(converter.Payload) {
    const inst: *Instance = @ptrCast(@alignCast(ptr));
    return .{ .ctx = &inst.ctx, .state = inst.startChildWorkflow(input.workflow_type, input.options, input.args.payloads, input.header.header()) };
}

fn rootExecuteNexusOperation(ptr: *anyopaque, input: interceptor.NexusOperationInput, _: interceptor.WorkflowOutbound.Next) workflow.NexusOperationFuture(converter.Payload) {
    const inst: *Instance = @ptrCast(@alignCast(ptr));
    return .{ .ctx = &inst.ctx, .state = inst.scheduleNexusOperation(input.service, input.operation, input.options, input.payload) };
}

fn rootSignalWithStartWorkflow(ptr: *anyopaque, input: interceptor.SignalWithStartInput, _: interceptor.WorkflowOutbound.Next) workflow.SignalWithStartFuture {
    const inst: *Instance = @ptrCast(@alignCast(ptr));
    const workflow_id = inst.arena().dupe(u8, input.options.id) catch return .{ .ctx = &inst.ctx, .workflow_id = "", .state = error.OutOfMemory };
    return .{
        .ctx = &inst.ctx,
        .workflow_id = workflow_id,
        .state = inst.sendSignalWithStart(input.workflow_type, input.signal_name, input.options, input.args.payloads, input.signal_args.payloads, input.header.header()),
    };
}

fn rootSignalExternalWorkflow(ptr: *anyopaque, input: interceptor.SignalExternalInput, _: interceptor.WorkflowOutbound.Next) workflow.SignalExternalFuture {
    const inst: *Instance = @ptrCast(@alignCast(ptr));
    const target: SignalTarget = if (input.child) .{ .child = input.workflow_id } else .{ .execution = .{ .workflow_id = input.workflow_id, .run_id = input.run_id } };
    return .{ .ctx = &inst.ctx, .state = inst.sendSignalExternal(target, input.signal_name, input.args.payloads, input.header.header()) };
}

fn rootCancelExternalWorkflow(ptr: *anyopaque, input: interceptor.CancelExternalInput, _: interceptor.WorkflowOutbound.Next) workflow.CancelExternalFuture {
    const inst: *Instance = @ptrCast(@alignCast(ptr));
    return .{ .ctx = &inst.ctx, .state = inst.requestCancelExternal(input.workflow_id, input.run_id) };
}

fn rootContinueAsNew(ptr: *anyopaque, input: interceptor.ContinueAsNewInput, _: interceptor.WorkflowOutbound.Next) interceptor.WorkflowOutbound.ContinueAsNewError!void {
    const inst: *Instance = @ptrCast(@alignCast(ptr));
    return inst.recordContinueAsNew(input.workflow_type, input.options, input.args.payloads, input.header.header());
}

fn rootLogger(ptr: *anyopaque, _: interceptor.LoggerInput, _: interceptor.WorkflowOutbound.Next) log.Logger {
    const inst: *Instance = @ptrCast(@alignCast(ptr));
    return inst.logger;
}

// ---------------------------------------------------------------- timers (WorkflowIo host)

const host_vtable: WorkflowIo.Host.VTable = .{
    .now = hostNow,
    .startTimer = hostStartTimer,
    .cancelTimer = hostCancelTimer,
};

fn hostNow(ctx: *anyopaque) std.Io.Timestamp {
    const inst: *Instance = @ptrCast(@alignCast(ctx));
    return .{ .nanoseconds = inst.now_ns };
}

/// `io.sleep` with a summary on its StartTimer command. Like `io.sleep`, it checks for
/// cancellation first and emits CancelTimer when canceled while the timer runs.
pub fn sleep(inst: *Instance, duration: std.Io.Duration, summary: []const u8) std.Io.Cancelable!void {
    // `io.sleep` starts its timer before it first yields, so no other coroutine can start a
    // timer while the summary is set.
    inst.timer_summary = summary;
    defer inst.timer_summary = "";
    return inst.wio.io().sleep(duration, .awake);
}

fn hostStartTimer(ctx: *anyopaque, timer: *WorkflowIo.Timer, duration: std.Io.Duration) void {
    const inst: *Instance = @ptrCast(@alignCast(ctx));
    const seq = inst.next_timer_seq;
    inst.next_timer_seq += 1;
    timer.host_data = seq;
    inst.timers.put(inst.arena(), seq, timer) catch {
        inst.task_failure = "out of memory starting a timer";
        return;
    };
    const summary = inst.timer_summary;
    inst.timer_summary = "";
    const metadata = summaryMetadata(inst.arena(), summary) catch {
        inst.task_failure = "out of memory starting a timer";
        return;
    };
    inst.emit(.{ .user_metadata = metadata, .variant = .{ .start_timer = .{
        .seq = seq,
        .start_to_fire_timeout = proto_time.durationToProto(duration),
    } } }) catch {
        inst.task_failure = "out of memory starting a timer";
    };
}

fn hostCancelTimer(ctx: *anyopaque, timer: *WorkflowIo.Timer) void {
    const inst: *Instance = @ptrCast(@alignCast(ctx));
    const seq: u32 = @intCast(timer.host_data);
    _ = inst.timers.remove(seq);
    inst.emit(.{ .variant = .{ .cancel_timer = .{ .seq = seq } } }) catch {
        inst.task_failure = "out of memory canceling a timer";
    };
}

// ---------------------------------------------------------------- tests

const testing = std.testing;
const activity = @import("../activity.zig");

const TestActivities = struct {
    fn greet(ctx: *activity.Context, name: []const u8) ![]const u8 {
        return std.fmt.allocPrint(ctx.allocator(), "Hello {s}!", .{name});
    }
};
const Greet = activity.Def("Activity", TestActivities.greet);

const TestWorkflows = struct {
    fn hello(ctx: *workflow.Context, name: []const u8) ![]const u8 {
        ctx.logger().info("HelloWorld workflow started", .{ .name = name });
        const greeting = ctx.executeActivity(Greet, .{ .start_to_close_timeout = .fromSeconds(10) }, .{name});
        return greeting.get();
    }

    fn napThenDone(ctx: *workflow.Context) ![]const u8 {
        try ctx.io().sleep(.fromSeconds(30), .awake);
        return "rested";
    }
};
const Hello = workflow.Def("Workflow", TestWorkflows.hello);
const Nap = workflow.Def("Nap", TestWorkflows.napThenDone);

fn initActivation(a: Allocator, workflow_type: []const u8, args: anytype) !act_pb.WorkflowActivation {
    var init: act_pb.InitializeWorkflow = .{ .workflow_type = workflow_type, .workflow_id = "wf-1", .randomness_seed = 7, .attempt = 1 };
    init.arguments = try converter.toPayloads(a, args);
    var act: act_pb.WorkflowActivation = .{ .run_id = "run-1", .timestamp = .{ .seconds = 1_700_000_000 } };
    try act.jobs.append(a, .{ .variant = .{ .initialize_workflow = init } });
    return act;
}

fn oneJob(a: Allocator, job: act_pb.WorkflowActivationJob.variant_union) !act_pb.WorkflowActivation {
    var act: act_pb.WorkflowActivation = .{ .run_id = "run-1", .timestamp = .{ .seconds = 1_700_000_010 } };
    try act.jobs.append(a, .{ .variant = job });
    return act;
}

fn commandsOf(c: done_pb.WorkflowActivationCompletion) []const cmd_pb.WorkflowCommand {
    return c.status.?.successful.commands.items;
}

test "hello world: schedule the activity, then complete with its result" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();

    const inst = try create(testing.allocator, Hello.registered, .{});
    defer inst.destroy();

    const first = try inst.activate(&try initActivation(a, "Workflow", .{@as([]const u8, "Temporal")}), a);
    const cmds = commandsOf(first);
    try testing.expectEqual(@as(usize, 1), cmds.len);
    const sched = cmds[0].variant.?.schedule_activity;
    try testing.expectEqualStrings("Activity", sched.activity_type);
    try testing.expectEqualStrings("\"Temporal\"", sched.arguments.items[0].data);
    try testing.expectEqual(@as(i64, 10), sched.start_to_close_timeout.?.seconds);

    var resolution: result_pb.ActivityResolution = .{ .status = .{ .completed = .{ .result = try converter.toPayload(a, @as([]const u8, "Hello Temporal!")) } } };
    const second = try inst.activate(&try oneJob(a, .{ .resolve_activity = .{ .seq = sched.seq, .result = resolution } }), a);
    _ = &resolution;
    const done = commandsOf(second);
    try testing.expectEqual(@as(usize, 1), done.len);
    try testing.expectEqualStrings("\"Hello Temporal!\"", done[0].variant.?.complete_workflow_execution.result.?.data);
}

fn createAndDestroy(gpa: Allocator, task_queue: []const u8, namespace: []const u8) !void {
    const inst = try create(gpa, Hello.registered, .{ .task_queue = task_queue, .namespace = namespace });
    inst.destroy();
}

test "Instance.create frees everything it acquired when an allocation fails" {
    // Longer than the arena's first chunk, so each copy takes a chunk of its own.
    const task_queue: [4096]u8 = @splat('q');
    const namespace: [4096]u8 = @splat('n');
    try testing.checkAllAllocationFailures(testing.allocator, createAndDestroy, .{ &task_queue, &namespace });
}

test "a failed activity fails the workflow with the activity's failure" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();

    const inst = try create(testing.allocator, Hello.registered, .{});
    defer inst.destroy();
    _ = try inst.activate(&try initActivation(a, "Workflow", .{@as([]const u8, "Temporal")}), a);

    const f = try failure_mod.application(a, .{ .message = "boom", .type = "Boom" });
    const second = try inst.activate(&try oneJob(a, .{ .resolve_activity = .{ .seq = 1, .result = .{ .status = .{ .failed = .{ .failure = f } } } } }), a);
    const fail = commandsOf(second)[0].variant.?.fail_workflow_execution.failure.?;
    try testing.expectEqualStrings("boom", fail.message);
    try testing.expectEqualStrings("Boom", failure_mod.applicationType(fail).?);
}

test "sleep becomes a durable timer that fires on FireTimer" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();

    const inst = try create(testing.allocator, Nap.registered, .{});
    defer inst.destroy();
    const first = try inst.activate(&try initActivation(a, "Nap", .{}), a);
    const timer = commandsOf(first)[0].variant.?.start_timer;
    try testing.expectEqual(@as(i64, 30), timer.start_to_fire_timeout.?.seconds);

    const second = try inst.activate(&try oneJob(a, .{ .fire_timer = .{ .seq = timer.seq } }), a);
    try testing.expectEqualStrings("\"rested\"", commandsOf(second)[0].variant.?.complete_workflow_execution.result.?.data);
}

/// Workflows that pass durations beyond protobuf's range; the mode picks the call.
const LongTimes = struct {
    const max_retry: workflow.RetryPolicy = .{ .initial_interval = .max, .maximum_interval = .max };

    fn never(_: *const bool) bool {
        return false;
    }

    fn run(ctx: *workflow.Context, mode: []const u8) ![]const u8 {
        if (std.mem.eql(u8, mode, "sleep")) {
            try ctx.sleep(.max, .{});
        } else if (std.mem.eql(u8, mode, "await")) {
            const flag = false;
            _ = try ctx.awaitTimeout(.max, @as(*const bool, &flag), never);
        } else if (std.mem.eql(u8, mode, "activity")) {
            _ = ctx.executeActivityByName(void, "Act", .{
                .schedule_to_close_timeout = .max,
                .schedule_to_start_timeout = .max,
                .start_to_close_timeout = .max,
                .heartbeat_timeout = .max,
                .retry_policy = max_retry,
            }, .{}).get() catch {};
        } else if (std.mem.eql(u8, mode, "negative")) {
            _ = ctx.executeActivityByName(void, "Act", .{
                .start_to_close_timeout = .fromSeconds(10),
                .heartbeat_timeout = .fromMilliseconds(-1500),
            }, .{}).get() catch {};
        } else if (std.mem.eql(u8, mode, "local")) {
            _ = ctx.executeLocalActivityByName(void, "Act", .{
                .schedule_to_close_timeout = .max,
                .schedule_to_start_timeout = .max,
                .start_to_close_timeout = .max,
                .local_retry_threshold = .max,
                .retry_policy = max_retry,
            }, .{}).get() catch {};
        } else if (std.mem.eql(u8, mode, "child")) {
            _ = ctx.executeChildWorkflowByName(void, "Kid", .{
                .execution_timeout = .max,
                .run_timeout = .max,
                .task_timeout = .max,
                .retry_policy = max_retry,
            }, .{}).get() catch {};
        } else if (std.mem.eql(u8, mode, "session")) {
            _ = try ctx.createSession(.{ .creation_timeout = .max, .execution_timeout = .max, .heartbeat_timeout = .max });
        } else if (std.mem.eql(u8, mode, "continue")) {
            return ctx.continueAsNewByName("LongTimes", .{
                .run_timeout = .max,
                .task_timeout = .max,
                .retry_policy = max_retry,
            }, .{mode});
        }
        return "done";
    }
    const Def = workflow.Def("LongTimes", run);
};

/// The protobuf Duration that `Duration.max` is clamped to.
const longest: proto.google.protobuf.Duration = .{ .seconds = 315_576_000_000, .nanos = 999_999_999 };

/// The commands of a LongTimes run's first activation in `mode`; they live as long as `inst`.
fn longTimesCommands(inst: *Instance, a: Allocator, mode: []const u8) ![]const cmd_pb.WorkflowCommand {
    return commandsOf(try inst.activate(&try initActivation(a, "LongTimes", .{mode}), a));
}

fn expectLongestRetry(rp: ?common.RetryPolicy) !void {
    try testing.expectEqual(longest, rp.?.initial_interval.?);
    try testing.expectEqual(longest, rp.?.maximum_interval.?);
}

test "durations beyond protobuf's range are clamped in workflow commands, and negative ones keep one sign" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();

    // sleep and awaitTimeout of Duration.max start a timer of the longest protobuf Duration.
    for ([_][]const u8{ "sleep", "await" }) |mode| {
        const inst = try create(testing.allocator, LongTimes.Def.registered, .{});
        defer inst.destroy();
        const cmds = try longTimesCommands(inst, a, mode);
        try testing.expectEqual(@as(usize, 1), cmds.len);
        try testing.expectEqual(longest, cmds[0].variant.?.start_timer.start_to_fire_timeout.?);
    }

    // Activity, local activity, child, continue-as-new and session timeouts and retry intervals.
    const act_inst = try create(testing.allocator, LongTimes.Def.registered, .{});
    defer act_inst.destroy();
    const sched = (try longTimesCommands(act_inst, a, "activity"))[0].variant.?.schedule_activity;
    try testing.expectEqual(longest, sched.schedule_to_close_timeout.?);
    try testing.expectEqual(longest, sched.schedule_to_start_timeout.?);
    try testing.expectEqual(longest, sched.start_to_close_timeout.?);
    try testing.expectEqual(longest, sched.heartbeat_timeout.?);
    try expectLongestRetry(sched.retry_policy);

    const local_inst = try create(testing.allocator, LongTimes.Def.registered, .{});
    defer local_inst.destroy();
    const local = (try longTimesCommands(local_inst, a, "local"))[0].variant.?.schedule_local_activity;
    try testing.expectEqual(longest, local.schedule_to_close_timeout.?);
    try testing.expectEqual(longest, local.schedule_to_start_timeout.?);
    try testing.expectEqual(longest, local.start_to_close_timeout.?);
    try testing.expectEqual(longest, local.local_retry_threshold.?);
    try expectLongestRetry(local.retry_policy);

    const child_inst = try create(testing.allocator, LongTimes.Def.registered, .{});
    defer child_inst.destroy();
    const child = (try longTimesCommands(child_inst, a, "child"))[0].variant.?.start_child_workflow_execution;
    try testing.expectEqual(longest, child.workflow_execution_timeout.?);
    try testing.expectEqual(longest, child.workflow_run_timeout.?);
    try testing.expectEqual(longest, child.workflow_task_timeout.?);
    try expectLongestRetry(child.retry_policy);

    const can_inst = try create(testing.allocator, LongTimes.Def.registered, .{});
    defer can_inst.destroy();
    const can_cmds = try longTimesCommands(can_inst, a, "continue");
    const can = can_cmds[can_cmds.len - 1].variant.?.continue_as_new_workflow_execution;
    try testing.expectEqual(longest, can.workflow_run_timeout.?);
    try testing.expectEqual(longest, can.workflow_task_timeout.?);
    try expectLongestRetry(can.retry_policy);

    // A session's host heartbeats every third of the clamped heartbeat timeout.
    const session_inst = try create(testing.allocator, LongTimes.Def.registered, .{});
    defer session_inst.destroy();
    const creation = (try longTimesCommands(session_inst, a, "session"))[0].variant.?.schedule_activity;
    try testing.expectEqual(longest, creation.schedule_to_start_timeout.?);
    try testing.expectEqual(longest, creation.start_to_close_timeout.?);
    try testing.expectEqual(longest, creation.heartbeat_timeout.?);
    try testing.expect(std.mem.indexOf(u8, creation.arguments.items[0].data, "\"heartbeat_interval_ms\":105192000000333") != null);

    // A negative heartbeat timeout is sent with seconds and nanos of the same sign.
    const neg_inst = try create(testing.allocator, LongTimes.Def.registered, .{});
    defer neg_inst.destroy();
    const negative = (try longTimesCommands(neg_inst, a, "negative"))[0].variant.?.schedule_activity;
    try testing.expectEqual(proto.google.protobuf.Duration{ .seconds = -1, .nanos = -500_000_000 }, negative.heartbeat_timeout.?);
}

/// Waits with a deadline at an end of i96, through `sleep` or a futex wait; the mode picks which.
const FarDeadline = struct {
    fn run(ctx: *workflow.Context, mode: []const u8) ![]const u8 {
        const ns: i96 = if (std.mem.endsWith(u8, mode, "min")) std.math.minInt(i96) else std.math.maxInt(i96);
        const timeout: std.Io.Timeout = .{ .deadline = .{ .raw = .fromNanoseconds(ns), .clock = .awake } };
        if (std.mem.startsWith(u8, mode, "sleep")) {
            try timeout.sleep(ctx.io());
        } else {
            // `Io.Event.waitTimeout` would reach the same wait, but recomputes the remaining
            // time itself with std's unchecked `durationFromNow` after the wait returns.
            const word: u32 = 0;
            try ctx.io().futexWaitTimeout(u32, &word, 0, timeout);
        }
        return "done";
    }
    const Def = workflow.Def("FarDeadline", run);
};

test "a wait whose deadline is at either end of i96 doesn't overflow: the past one has expired, the future one starts the longest timer" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();

    for ([_][]const u8{ "sleep-min", "futex-min" }) |mode| {
        const inst = try create(testing.allocator, FarDeadline.Def.registered, .{});
        defer inst.destroy();
        const cmds = commandsOf(try inst.activate(&try initActivation(a, "FarDeadline", .{mode}), a));
        try testing.expectEqual(@as(usize, 1), cmds.len);
        try testing.expectEqualStrings("\"done\"", cmds[0].variant.?.complete_workflow_execution.result.?.data);
    }

    // With workflow time before 1970, maxInt(i96) minus now doesn't fit in i96.
    for ([_][]const u8{ "sleep-max", "futex-max" }) |mode| {
        const inst = try create(testing.allocator, FarDeadline.Def.registered, .{});
        defer inst.destroy();
        var act = try initActivation(a, "FarDeadline", .{mode});
        act.timestamp = .{ .seconds = -1 };
        const cmds = commandsOf(try inst.activate(&act, a));
        try testing.expectEqual(@as(usize, 1), cmds.len);
        try testing.expectEqual(proto_time.durationToProto(proto_time.max_duration).?, cmds[0].variant.?.start_timer.start_to_fire_timeout.?);
    }
}

test "workflow cancellation cancels the pending activity and the workflow" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();

    const inst = try create(testing.allocator, Hello.registered, .{});
    defer inst.destroy();
    _ = try inst.activate(&try initActivation(a, "Workflow", .{@as([]const u8, "Temporal")}), a);
    const second = try inst.activate(&try oneJob(a, .{ .cancel_workflow = .{} }), a);
    const cmds = commandsOf(second);
    try testing.expectEqual(@as(usize, 2), cmds.len);
    try testing.expectEqual(@as(u32, 1), cmds[0].variant.?.request_cancel_activity.seq);
    try testing.expect(cmds[1].variant.? == .cancel_workflow_execution);
}

test "eviction mid-run frees everything" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, Hello.registered, .{});
    _ = try inst.activate(&try initActivation(a, "Workflow", .{@as([]const u8, "Temporal")}), a);
    inst.destroy(); // testing.allocator reports leaks
}

test "a job of a kind this SDK doesn't know fails the workflow task" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, Hello.registered, .{});
    defer inst.destroy();
    _ = try inst.activate(&try initActivation(a, "Workflow", .{@as([]const u8, "Temporal")}), a);
    // Every job kind the protos define is handled; a newer core's job decodes as unknown
    // fields: here field 99, an empty message.
    var reader: std.Io.Reader = .fixed("\x9a\x06\x00");
    const job = try act_pb.WorkflowActivationJob.decode(&reader, a);
    try testing.expect(job.variant == null);
    var act: act_pb.WorkflowActivation = .{ .run_id = "run-1" };
    try act.jobs.append(a, job);
    const c = try inst.activate(&act, a);
    const f = c.status.?.failed.failure.?;
    try testing.expectEqualStrings("UnsupportedJob", failure_mod.applicationType(f).?);
    try testing.expect(std.mem.indexOf(u8, f.message, "doesn't know") != null);
}

// Patches: the workflow reports what `patched`, `deprecatePatch` and `getVersion` decided.
const Patches = struct {
    fn run(ctx: *workflow.Context, min_version: i32) ![]const u8 {
        const first = ctx.patched("p1");
        const again = ctx.patched("p1");
        ctx.deprecatePatch("old");
        const v = try ctx.getVersion("pricing", min_version, 2);
        const v_again = try ctx.getVersion("pricing", min_version, 2);
        return std.fmt.allocPrint(ctx.allocator(), "{} {} {d} {d}", .{ first, again, v, v_again });
    }
};
const PatchesWf = workflow.Def("Patches", Patches.run);

/// Runs `PatchesWf` on `inst` with one activation, replaying (with `notified` patches) or not.
/// The completion points into the run's arena.
fn runPatches(inst: *Instance, a: Allocator, replaying: bool, notified: []const []const u8, min_version: i32) !done_pb.WorkflowActivationCompletion {
    var act = try initActivation(a, "Patches", .{min_version});
    act.is_replaying = replaying;
    for (notified) |id| try act.jobs.append(a, .{ .variant = .{ .notify_has_patch = .{ .patch_id = id } } });
    return inst.activate(&act, a);
}

fn expectMarker(cmd: cmd_pb.WorkflowCommand, id: []const u8, deprecated: bool) !void {
    const m = cmd.variant.?.set_patch_marker;
    try testing.expectEqualStrings(id, m.patch_id);
    try testing.expectEqual(deprecated, m.deprecated);
}

test "patches: a first run takes every new path and records each marker once" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, PatchesWf.registered, .{});
    defer inst.destroy();
    const cmds = commandsOf(try runPatches(inst, a, false, &.{}, workflow.default_version));
    try testing.expectEqual(@as(usize, 4), cmds.len);
    try expectMarker(cmds[0], "p1", false);
    try expectMarker(cmds[1], "old", true);
    try expectMarker(cmds[2], "pricing@v2", false);
    try testing.expectEqualStrings("\"true true 2 2\"", cmds[3].variant.?.complete_workflow_execution.result.?.data);
}

test "patches: a replay without markers takes the old paths and records nothing" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, PatchesWf.registered, .{});
    defer inst.destroy();
    const cmds = commandsOf(try runPatches(inst, a, true, &.{}, workflow.default_version));
    try testing.expectEqual(@as(usize, 1), cmds.len);
    try testing.expectEqualStrings("\"false false -1 -1\"", cmds[0].variant.?.complete_workflow_execution.result.?.data);
}

test "patches: a replay with markers takes the recorded paths and versions" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, PatchesWf.registered, .{});
    defer inst.destroy();
    // A marker of a change with a longer name sharing the prefix isn't this change's version.
    const cmds = commandsOf(try runPatches(inst, a, true, &.{ "p1", "pricing-old@v2", "pricing@v1" }, workflow.default_version));
    try testing.expectEqual(@as(usize, 3), cmds.len);
    try expectMarker(cmds[0], "p1", false);
    try expectMarker(cmds[1], "pricing@v1", false);
    try testing.expectEqualStrings("\"true true 1 1\"", cmds[2].variant.?.complete_workflow_execution.result.?.data);
}

test "patches: getVersion fails the task for a recorded version below min_supported" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, PatchesWf.registered, .{});
    defer inst.destroy();
    const old_run = try runPatches(inst, a, true, &.{"pricing@v1"}, 2);
    const message = old_run.status.?.failed.failure.?.message;
    try testing.expect(std.mem.indexOf(u8, message, "getVersion(\"pricing\", 2, 2): the run uses version 1") != null);
    // A run from before the change has the default version, also below 2.
    const older = try create(testing.allocator, PatchesWf.registered, .{});
    defer older.destroy();
    try testing.expect((try runPatches(older, a, true, &.{}, 2)).status.? == .failed);
}

// Versioning behavior: the Def's goes with every completion until the run sets another.
const Versioning = struct {
    fn run(ctx: *workflow.Context, switch_to: workflow.VersioningBehavior) ![]const u8 {
        const before = ctx.currentDeploymentVersion();
        try ctx.io().sleep(.fromSeconds(1), .awake);
        ctx.setVersioningBehavior(switch_to);
        const after = ctx.currentDeploymentVersion().?;
        return std.fmt.allocPrint(ctx.allocator(), "{} {s}.{s}", .{ before == null, after.deployment_name, after.build_id });
    }
};
const PinnedWf = workflow.DefWithOptions("Pinned", Versioning.run, .{ .versioning_behavior = .pinned });

test "versioning: completions carry the Def's behavior, then the one the run sets, and the task's version" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    try testing.expectEqual(workflow.VersioningBehavior.pinned, PinnedWf.versioning_behavior);
    const inst = try create(testing.allocator, PinnedWf.registered, .{ .send_versioning_behavior = true });
    defer inst.destroy();
    const first = try inst.activate(&try initActivation(a, "Pinned", .{workflow.VersioningBehavior.auto_upgrade}), a);
    try testing.expectEqual(proto.temporal.api.enums.v1.VersioningBehavior.VERSIONING_BEHAVIOR_PINNED, first.status.?.successful.versioning_behavior);
    const timer = commandsOf(first)[0].variant.?.start_timer;

    var fire = try oneJob(a, .{ .fire_timer = .{ .seq = timer.seq } });
    fire.deployment_version_for_current_task = .{ .deployment_name = "shop", .build_id = "b2" };
    const second = try inst.activate(&fire, a);
    try testing.expectEqual(proto.temporal.api.enums.v1.VersioningBehavior.VERSIONING_BEHAVIOR_AUTO_UPGRADE, second.status.?.successful.versioning_behavior);
    try testing.expectEqualStrings("\"true shop.b2\"", commandsOf(second)[0].variant.?.complete_workflow_execution.result.?.data);
}

test "versioning: a worker that doesn't use worker versioning sends no behavior" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, PinnedWf.registered, .{});
    defer inst.destroy();
    const first = try inst.activate(&try initActivation(a, "Pinned", .{workflow.VersioningBehavior.auto_upgrade}), a);
    try testing.expectEqual(proto.temporal.api.enums.v1.VersioningBehavior.VERSIONING_BEHAVIOR_UNSPECIFIED, first.status.?.successful.versioning_behavior);
}

// Signal handlers. `State.log` records what handlers and the workflow function did, in order.
const Signals = struct {
    const Approve = workflow.SignalDef("approve", fn (approver: []const u8) void);
    const Reject = workflow.SignalDef("reject", fn () void);
    const opts: workflow.ActivityOptions = .{ .start_to_close_timeout = .fromSeconds(10) };

    const State = struct {
        ctx: *workflow.Context,
        log: std.ArrayList([]const u8) = .empty,
        want: usize,

        fn create(ctx: *workflow.Context, want: usize) !*State {
            const s = try ctx.allocator().create(State);
            s.* = .{ .ctx = ctx, .want = want };
            return s;
        }
        fn add(s: *State, entry: []const u8) !void {
            try s.log.append(s.ctx.allocator(), entry);
        }
        fn approve(s: *State, approver: []const u8) !void {
            try s.add(approver);
        }
        /// Records the approver, then greets it with an activity.
        fn approveSlowly(s: *State, approver: []const u8) !void {
            try s.add(approver);
            try s.add(try s.ctx.executeActivity(Greet, opts, .{approver}).get());
        }
        /// Records the approver, then sleeps past the workflow's end.
        fn approveThenNap(s: *State, approver: []const u8) !void {
            try s.add(approver);
            try s.ctx.io().sleep(.fromSeconds(60), .awake);
        }
        fn reject(_: *State) error{Rejected}!void {
            return error.Rejected;
        }
        fn enough(s: *State) bool {
            return s.log.items.len >= s.want;
        }
        fn joined(s: *State) ![]const u8 {
            return std.mem.join(s.ctx.allocator(), ",", s.log.items);
        }
    };

    /// Waits for `want` approvals and returns them in order.
    fn collect(ctx: *workflow.Context, want: usize) ![]const u8 {
        const s = try State.create(ctx, want);
        try ctx.setSignalHandler(Approve, s, State.approve);
        try ctx.await(s, State.enough);
        return s.joined();
    }

    /// `collect`, after sleeping 10 s without a handler.
    fn collectLate(ctx: *workflow.Context, want: usize) ![]const u8 {
        try ctx.io().sleep(.fromSeconds(10), .awake);
        return collect(ctx, want);
    }

    /// Waits for a handler to start, then for every handler to finish.
    fn drain(ctx: *workflow.Context) ![]const u8 {
        const s = try State.create(ctx, 1);
        try ctx.setSignalHandler(Approve, s, State.approveSlowly);
        try ctx.await(s, State.enough);
        try ctx.await(ctx, workflow.Context.allHandlersFinished);
        return s.joined();
    }

    /// Returns as soon as one approval arrived, while its handler sleeps on.
    fn returnEarly(ctx: *workflow.Context) ![]const u8 {
        const s = try State.create(ctx, 1);
        try ctx.setSignalHandler(Approve, s, State.approveThenNap);
        try ctx.await(s, State.enough);
        return s.joined();
    }

    /// Records the activity's result after waiting for it.
    fn ordered(ctx: *workflow.Context) ![]const u8 {
        const s = try State.create(ctx, 0);
        try ctx.setSignalHandler(Approve, s, State.approve);
        _ = try ctx.executeActivity(Greet, opts, .{"x"}).get();
        try s.add("activity");
        return s.joined();
    }

    /// Sleeps for an hour; a `reject` signal fails it.
    fn rejectable(ctx: *workflow.Context) ![]const u8 {
        const s = try State.create(ctx, 0);
        try ctx.setSignalHandler(Reject, s, State.reject);
        try ctx.io().sleep(.fromSeconds(3600), .awake);
        return "slept";
    }

    fn onStack(ctx: *workflow.Context) ![]const u8 {
        var s: State = .{ .ctx = ctx, .want = 0 };
        ctx.setSignalHandler(Approve, &s, State.approve) catch |err| return @errorName(err);
        return "registered";
    }
};
const CollectWf = workflow.Def("Collect", Signals.collect);
const CollectLateWf = workflow.Def("CollectLate", Signals.collectLate);
const DrainWf = workflow.Def("Drain", Signals.drain);
const ReturnEarlyWf = workflow.Def("ReturnEarly", Signals.returnEarly);
const OrderedWf = workflow.Def("Ordered", Signals.ordered);
const RejectableWf = workflow.Def("Rejectable", Signals.rejectable);
const SignalOnStackWf = workflow.Def("SignalOnStack", Signals.onStack);

fn signalJob(a: Allocator, name: []const u8, args: anytype) !act_pb.WorkflowActivationJob.variant_union {
    var job: act_pb.SignalWorkflow = .{ .signal_name = name };
    job.input = try converter.toPayloads(a, args);
    return .{ .signal_workflow = job };
}

fn completedWith(c: done_pb.WorkflowActivationCompletion) []const u8 {
    const cmds = commandsOf(c);
    return cmds[cmds.len - 1].variant.?.complete_workflow_execution.result.?.data;
}

test "a signal runs its handler, in arrival order, and wakes awaiting code" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, CollectWf.registered, .{});
    defer inst.destroy();

    try testing.expectEqual(@as(usize, 0), commandsOf(try inst.activate(&try initActivation(a, "Collect", .{@as(usize, 3)}), a)).len);
    try testing.expectEqual(@as(usize, 0), commandsOf(try inst.activate(&try oneJob(a, try signalJob(a, "approve", .{@as([]const u8, "ann")})), a)).len);
    // Two in one activation, and one whose argument doesn't decode, which is dropped.
    var act = try oneJob(a, try signalJob(a, "approve", .{@as(i64, 5)}));
    try act.jobs.append(a, .{ .variant = try signalJob(a, "approve", .{@as([]const u8, "bob")}) });
    try act.jobs.append(a, .{ .variant = try signalJob(a, "approve", .{@as([]const u8, "cy")}) });
    try testing.expectEqualStrings("\"ann,bob,cy\"", completedWith(try inst.activate(&act, a)));
}

test "signals before their handler is registered are buffered until it is" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, CollectLateWf.registered, .{});
    defer inst.destroy();

    // The first signal arrives with the start, the second while the workflow sleeps; a third
    // with no handler is dropped (with a warning) when the workflow finishes.
    var start = try initActivation(a, "CollectLate", .{@as(usize, 2)});
    try start.jobs.append(a, .{ .variant = try signalJob(a, "approve", .{@as([]const u8, "ann")}) });
    const first = commandsOf(try inst.activate(&start, a));
    try testing.expectEqual(@as(usize, 1), first.len);
    try testing.expect(first[0].variant.? == .start_timer);
    var later = try oneJob(a, try signalJob(a, "approve", .{@as([]const u8, "bob")}));
    try later.jobs.append(a, .{ .variant = try signalJob(a, "unknown", .{}) });
    try testing.expectEqual(@as(usize, 0), commandsOf(try inst.activate(&later, a)).len);
    try testing.expectEqual(@as(usize, 3), inst.buffered_signals.items.len);
    try testing.expectEqualStrings("\"ann,bob\"", completedWith(try inst.activate(&try oneJob(a, .{ .fire_timer = .{ .seq = 1 } }), a)));
    try testing.expectEqual(@as(usize, 1), inst.buffered_signals.items.len);
}

test "a handler may block, and allHandlersFinished waits for it" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, DrainWf.registered, .{});
    defer inst.destroy();

    _ = try inst.activate(&try initActivation(a, "Drain", .{}), a);
    try testing.expect(inst.ctx.allHandlersFinished());
    const blocked = commandsOf(try inst.activate(&try oneJob(a, try signalJob(a, "approve", .{@as([]const u8, "ann")})), a));
    try testing.expectEqual(@as(usize, 1), blocked.len);
    try testing.expectEqualStrings("\"ann\"", blocked[0].variant.?.schedule_activity.arguments.items[0].data);
    try testing.expect(!inst.ctx.allHandlersFinished());
    try testing.expectEqualStrings("\"ann,Hello ann!\"", completedWith(try inst.activate(&try resolveOk(a, 1, "Hello ann!"), a)));
    try testing.expect(inst.ctx.allHandlersFinished());
}

test "the workflow completes while a handler is still blocked, and eviction frees it" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, ReturnEarlyWf.registered, .{});
    defer inst.destroy();

    _ = try inst.activate(&try initActivation(a, "ReturnEarly", .{}), a);
    const done = commandsOf(try inst.activate(&try oneJob(a, try signalJob(a, "approve", .{@as([]const u8, "ann")})), a));
    try testing.expectEqual(@as(usize, 2), done.len);
    try testing.expect(done[0].variant.? == .start_timer);
    try testing.expectEqualStrings("\"ann\"", done[1].variant.?.complete_workflow_execution.result.?.data);
    try testing.expect(!inst.ctx.allHandlersFinished());
    // Nothing runs after the workflow finished: the handler's timer firing emits nothing.
    try testing.expectEqual(@as(usize, 0), commandsOf(try inst.activate(&try oneJob(a, .{ .fire_timer = .{ .seq = 1 } }), a)).len);
}

test "eviction cancels a handler blocked on an activity" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, DrainWf.registered, .{});
    _ = try inst.activate(&try initActivation(a, "Drain", .{}), a);
    _ = try inst.activate(&try oneJob(a, try signalJob(a, "approve", .{@as([]const u8, "ann")})), a);
    inst.destroy(); // testing.allocator reports leaks
}

test "handlers start before coroutines woken by the same activation's other jobs" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, OrderedWf.registered, .{});
    defer inst.destroy();

    _ = try inst.activate(&try initActivation(a, "Ordered", .{}), a);
    var act = try oneJob(a, try signalJob(a, "approve", .{@as([]const u8, "ann")}));
    try act.jobs.append(a, (try resolveOk(a, 1, "Hello x!")).jobs.items[0]);
    try testing.expectEqualStrings("\"ann,activity\"", completedWith(try inst.activate(&act, a)));
}

test "a handler's error fails the workflow" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, RejectableWf.registered, .{});
    defer inst.destroy();

    _ = try inst.activate(&try initActivation(a, "Rejectable", .{}), a);
    const failed = commandsOf(try inst.activate(&try oneJob(a, try signalJob(a, "reject", .{})), a));
    try testing.expectEqual(@as(usize, 1), failed.len);
    try testing.expectEqualStrings("Rejected", failed[0].variant.?.fail_workflow_execution.failure.?.message);
    // The workflow function, still sleeping, never runs again.
    try testing.expectEqual(@as(usize, 0), commandsOf(try inst.activate(&try oneJob(a, .{ .fire_timer = .{ .seq = 1 } }), a)).len);
}

test "a signal handler context on the coroutine's stack is rejected" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, SignalOnStackWf.registered, .{});
    defer inst.destroy();
    try testing.expectEqualStrings("\"SignalContextOnStack\"", completedWith(try inst.activate(&try initActivation(a, "SignalOnStack", .{}), a)));
}

// Signals named at run time, and the dynamic signal handler.
const RuntimeSignals = struct {
    /// Sleeps 10 s without handlers, then handles "unlock-<workflow ID>" by name and every
    /// other signal dynamically, and returns the first `want` entries.
    fn byName(ctx: *workflow.Context, want: usize) ![]const u8 {
        try ctx.io().sleep(.fromSeconds(10), .awake);
        const s = try Signals.State.create(ctx, want);
        var buf: [64]u8 = undefined;
        const name = try std.fmt.bufPrint(&buf, "unlock-{s}", .{ctx.info().workflow_id});
        try ctx.setSignalHandlerByName(name, struct { []const u8 }, s, Signals.State.approve);
        @memset(&buf, 'x'); // the handler keeps its own copy of the name
        try ctx.setDynamicSignalHandler(s, dynamic);
        try ctx.await(s, Signals.State.enough);
        return s.joined();
    }
    fn dynamic(s: *Signals.State, name: []const u8, args: converter.EncodedValues) !void {
        const value = args.get([]const u8, s.ctx.allocator(), 0) catch |err| @errorName(err);
        try s.add(try std.fmt.allocPrint(s.ctx.allocator(), "{s}={s}", .{ name, value }));
    }

    /// Sends "unlock-<target>" to `target`, with the name built at run time.
    fn unlock(ctx: *workflow.Context, target: []const u8) !void {
        var buf: [64]u8 = undefined;
        const name = try std.fmt.bufPrint(&buf, "unlock-{s}", .{target});
        const future = ctx.signalExternalWorkflowByName(name, target, "", .{"releaser"});
        @memset(&buf, 'x');
        try future.get();
    }
};
const ByNameWf = workflow.Def("ByName", RuntimeSignals.byName);
const UnlockWf = workflow.Def("Unlock", RuntimeSignals.unlock);

test "a handler named at run time takes buffered signals of its name, the dynamic handler the rest" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, ByNameWf.registered, .{});
    defer inst.destroy();

    var start = try initActivation(a, "ByName", .{@as(usize, 6)});
    try start.jobs.append(a, .{ .variant = try signalJob(a, "unlock-wf-1", .{@as([]const u8, "ann")}) });
    try start.jobs.append(a, .{ .variant = try signalJob(a, "color", .{@as([]const u8, "red")}) });
    _ = try inst.activate(&start, a);
    _ = try inst.activate(&try oneJob(a, try signalJob(a, "unlock-wf-1", .{@as([]const u8, "bob")})), a);
    try testing.expectEqual(@as(usize, 3), inst.buffered_signals.items.len);
    // Registering by name starts ann and bob; the dynamic handler then takes color.
    try testing.expectEqual(@as(usize, 0), commandsOf(try inst.activate(&try oneJob(a, .{ .fire_timer = .{ .seq = 1 } }), a)).len);
    try testing.expectEqual(@as(usize, 0), inst.buffered_signals.items.len);
    // A named handler wins over the dynamic one; an argument of the wrong type is an error
    // the dynamic handler sees, not a dropped signal.
    var last = try oneJob(a, try signalJob(a, "unlock-wf-1", .{@as([]const u8, "cy")}));
    try last.jobs.append(a, .{ .variant = try signalJob(a, "size", .{@as(i64, 9)}) });
    try last.jobs.append(a, .{ .variant = try signalJob(a, "empty", .{}) });
    try testing.expectEqualStrings("\"ann,bob,color=red,cy,size=InvalidPayload,empty=WrongArgumentCount\"", completedWith(try inst.activate(&last, a)));
}

test "signal external by name: the command carries a copy of the name built at run time" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, UnlockWf.registered, .{});
    defer inst.destroy();

    const first = commandsOf(try inst.activate(&try initActivation(a, "Unlock", .{@as([]const u8, "holder")}), a));
    try testing.expectEqual(@as(usize, 1), first.len);
    const cmd = first[0].variant.?.signal_external_workflow_execution;
    try testing.expectEqualStrings("unlock-holder", cmd.signal_name);
    try testing.expectEqualStrings("holder", cmd.target.?.workflow_execution.workflow_id);
    try testing.expectEqualStrings("\"releaser\"", cmd.args.items[0].data);
    const done = commandsOf(try inst.activate(&try signalResolved(a, 1, null, .SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_UNSPECIFIED), a));
    try testing.expect(done[done.len - 1].variant.? == .complete_workflow_execution);
}

// Signal channels, unhandled signal names, and removing handlers.
const ChannelSignals = struct {
    const Approve = Signals.Approve;
    const Pair = workflow.SignalDef("pair", fn (a: i64, b: i64) void);
    const ApproveCh = workflow.SignalChannel(Approve.Args);
    const PairCh = workflow.SignalChannel(Pair.Args);

    fn join(ctx: *workflow.Context, items: []const []const u8) ![]const u8 {
        return std.mem.join(ctx.allocator(), ",", items);
    }
    fn hasOne(ch: *ApproveCh) bool {
        return ch.len() >= 1;
    }
    fn hasTwo(ch: *PairCh) bool {
        return ch.len() >= 2;
    }

    /// Sleeps 10 s with no handler, then receives `want` approvals through a channel.
    fn receiveLate(ctx: *workflow.Context, want: usize) ![]const u8 {
        try ctx.io().sleep(.fromSeconds(10), .awake);
        const ch = try ctx.signalChannel(Approve);
        if (ch != try ctx.signalChannel(Approve)) return "a second channel";
        var got: std.ArrayList([]const u8) = .empty;
        for (0..want) |_| try got.append(ctx.allocator(), try ch.receive());
        if (ch.tryReceive() != null) return "an extra approval";
        return join(ctx, got.items);
    }

    /// Waits until two `pair` signals are unread, then reads them without blocking.
    fn peek(ctx: *workflow.Context) ![]const u8 {
        const ch = try ctx.signalChannel(Pair);
        const before = ch.len();
        try ctx.await(ch, hasTwo);
        const first = ch.tryReceive().?;
        const second = ch.tryReceive().?;
        return std.fmt.allocPrint(ctx.allocator(), "{} {}+{} {}+{} {} {}", .{ before, first[0], first[1], second[0], second[1], ch.tryReceive() == null, ch.len() });
    }

    /// Waits for an approval; returns how the wait ended.
    fn cancelable(ctx: *workflow.Context) ![]const u8 {
        const ch = try ctx.signalChannelByName("approve", Approve.Args);
        const approver = ch.receive() catch |err| return @errorName(err);
        return approver;
    }

    const Race = union(enum) { approval: error{Canceled}!ApproveCh.Value, timeout: std.Io.Cancelable!void };

    /// Collects approvals until a minute passes without one, then sleeps 10 s more and
    /// reports what is left unread.
    fn untilQuiet(ctx: *workflow.Context) ![]const u8 {
        const io = ctx.io();
        const ch = try ctx.signalChannel(Approve);
        var got: std.ArrayList([]const u8) = .empty;
        while (true) {
            var buf: [2]Race = undefined;
            var select: std.Io.Select(Race) = .init(io, &buf);
            select.async(.approval, ApproveCh.receive, .{ch});
            select.async(.timeout, std.Io.sleep, .{ io, .fromSeconds(60), .awake });
            const first = try select.await();
            select.cancelDiscard();
            switch (first) {
                .approval => |r| try got.append(ctx.allocator(), try r),
                .timeout => |r| {
                    try r;
                    break;
                },
            }
        }
        try io.sleep(.fromSeconds(10), .awake);
        const left = try join(ctx, try ctx.unhandledSignalNames());
        return std.fmt.allocPrint(ctx.allocator(), "{s}|{}|{s}", .{ try join(ctx, got.items), ch.len(), left });
    }

    /// Takes approvals through a channel that it never reads, sleeps 10 s, and returns the
    /// unhandled signal names.
    fn leaveUnread(ctx: *workflow.Context) ![]const u8 {
        _ = try ctx.signalChannel(Approve);
        try ctx.io().sleep(.fromSeconds(10), .awake);
        return join(ctx, try ctx.unhandledSignalNames());
    }

    /// Handles approvals with a handler that records the approver, sleeps 5 s, and records
    /// it again; removes it once the first arrives and sleeps 10 s; then handles what came
    /// meanwhile dynamically; removes that too, and sleeps 10 s more.
    fn removal(ctx: *workflow.Context) ![]const u8 {
        const s = try Signals.State.create(ctx, 1);
        try ctx.setSignalHandler(Approve, s, approveThenSleep);
        try ctx.setSignalHandlerByName("unlock", struct { []const u8 }, s, Signals.State.approve);
        try ctx.await(s, Signals.State.enough);
        ctx.removeSignalHandler(Approve);
        ctx.removeSignalHandlerByName("unlock");
        try ctx.io().sleep(.fromSeconds(10), .awake);
        try s.add(try std.fmt.allocPrint(ctx.allocator(), "[{s}]", .{try join(ctx, try ctx.unhandledSignalNames())}));
        try ctx.setDynamicSignalHandler(s, RuntimeSignals.dynamic);
        try ctx.await(ctx, workflow.Context.allHandlersFinished);
        ctx.removeDynamicSignalHandler();
        try ctx.io().sleep(.fromSeconds(10), .awake);
        try s.add(try std.fmt.allocPrint(ctx.allocator(), "[{s}]", .{try join(ctx, try ctx.unhandledSignalNames())}));
        return s.joined();
    }
    fn approveThenSleep(s: *Signals.State, approver: []const u8) !void {
        try s.add(approver);
        try s.ctx.io().sleep(.fromSeconds(5), .awake);
        try s.add(try std.fmt.allocPrint(s.ctx.allocator(), "{s}!", .{approver}));
    }

    /// Sleeps 10 s, opens a channel over the buffered approvals, and reports at once, without
    /// blocking, the unhandled names, `len`, and whether `tryReceive` got one.
    fn openNoYield(ctx: *workflow.Context) ![]const u8 {
        try ctx.io().sleep(.fromSeconds(10), .awake);
        const ch = try ctx.signalChannel(Approve);
        const names = try join(ctx, try ctx.unhandledSignalNames());
        return std.fmt.allocPrint(ctx.allocator(), "{s}|{}|{}", .{ names, ch.len(), ch.tryReceive() != null });
    }

    const Reject = workflow.SignalDef("reject", fn (who: []const u8) void);
    const RejectCh = workflow.SignalChannel(Reject.Args);
    const Either = union(enum) { approve: error{Canceled}![]const u8, reject: error{Canceled}![]const u8 };

    /// Records a received signal; a receive that the Select canceled got none.
    fn either(ctx: *workflow.Context, got: *std.ArrayList([]const u8), result: Either) !void {
        const entry = switch (result) {
            .approve => |r| try std.fmt.allocPrint(ctx.allocator(), "a:{s}", .{r catch return}),
            .reject => |r| try std.fmt.allocPrint(ctx.allocator(), "r:{s}", .{r catch return}),
        };
        try got.append(ctx.allocator(), entry);
    }

    /// Opens channels for approvals and rejections, sleeps `sleep_s` seconds if positive,
    /// then races a receive on each in a new Select until two signals arrived. Each round
    /// cancels its Select with `cancel` until it returns null, keeping a receive that finished
    /// after the first result, as `cancelDiscard` wouldn't.
    fn twoChannels(ctx: *workflow.Context, sleep_s: i64) ![]const u8 {
        const io = ctx.io();
        const approvals = try ctx.signalChannel(Approve);
        const rejections = try ctx.signalChannel(Reject);
        if (sleep_s > 0) try io.sleep(.fromSeconds(sleep_s), .awake);
        var got: std.ArrayList([]const u8) = .empty;
        while (got.items.len < 2) {
            var buf: [2]Either = undefined;
            var select: std.Io.Select(Either) = .init(io, &buf);
            select.async(.approve, ApproveCh.receive, .{approvals});
            select.async(.reject, RejectCh.receive, .{rejections});
            try either(ctx, &got, try select.await());
            while (select.cancel()) |late| try either(ctx, &got, late);
        }
        return join(ctx, got.items);
    }

    /// Races a receive on each channel in one Select and awaits it twice.
    fn selectTwice(ctx: *workflow.Context) ![]const u8 {
        const io = ctx.io();
        var buf: [2]Either = undefined;
        var select: std.Io.Select(Either) = .init(io, &buf);
        defer select.cancelDiscard();
        select.async(.approve, ApproveCh.receive, .{try ctx.signalChannel(Approve)});
        select.async(.reject, RejectCh.receive, .{try ctx.signalChannel(Reject)});
        var got: std.ArrayList([]const u8) = .empty;
        try either(ctx, &got, try select.await());
        try either(ctx, &got, try select.await());
        return join(ctx, got.items);
    }

    /// Removes its channel once an approval is unread; later approvals wait for a new one.
    fn channelRemoved(ctx: *workflow.Context) ![]const u8 {
        const ch = try ctx.signalChannel(Approve);
        try ctx.await(ch, hasOne);
        ctx.removeSignalHandler(Approve);
        try ctx.io().sleep(.fromSeconds(10), .awake);
        const left = try join(ctx, try ctx.unhandledSignalNames());
        const kept = ch.tryReceive().?;
        const again = try ctx.signalChannel(Approve);
        if (again == ch) return "the removed channel";
        return std.fmt.allocPrint(ctx.allocator(), "{s}|{s}|{s}|{}", .{ kept, left, try again.receive(), ch.len() });
    }
};
const ChannelLateWf = workflow.Def("ChannelLate", ChannelSignals.receiveLate);
const OpenNoYieldWf = workflow.Def("OpenNoYield", ChannelSignals.openNoYield);
const TwoChannelsWf = workflow.Def("TwoChannels", ChannelSignals.twoChannels);
const SelectTwiceWf = workflow.Def("SelectTwice", ChannelSignals.selectTwice);
const ChannelPeekWf = workflow.Def("ChannelPeek", ChannelSignals.peek);
const ChannelCancelWf = workflow.Def("ChannelCancel", ChannelSignals.cancelable);
const ChannelQuietWf = workflow.Def("ChannelQuiet", ChannelSignals.untilQuiet);
const LeaveUnreadWf = workflow.Def("LeaveUnread", ChannelSignals.leaveUnread);
const RemovalWf = workflow.Def("Removal", ChannelSignals.removal);
const ChannelRemovedWf = workflow.Def("ChannelRemoved", ChannelSignals.channelRemoved);

test "a signal channel receives buffered signals, then later ones, in arrival order" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, ChannelLateWf.registered, .{});
    defer inst.destroy();

    var start = try initActivation(a, "ChannelLate", .{@as(usize, 3)});
    try start.jobs.append(a, .{ .variant = try signalJob(a, "approve", .{@as([]const u8, "ann")}) });
    try testing.expect(commandsOf(try inst.activate(&start, a))[0].variant.? == .start_timer);
    // One whose argument doesn't decode is dropped; one of another name stays buffered.
    var early = try oneJob(a, try signalJob(a, "approve", .{@as([]const u8, "bob")}));
    try early.jobs.append(a, .{ .variant = try signalJob(a, "approve", .{@as(i64, 5)}) });
    try early.jobs.append(a, .{ .variant = try signalJob(a, "unknown", .{}) });
    _ = try inst.activate(&early, a);
    try testing.expectEqual(@as(usize, 0), commandsOf(try inst.activate(&try oneJob(a, .{ .fire_timer = .{ .seq = 1 } }), a)).len);
    const names = try inst.ctx.unhandledSignalNames();
    try testing.expectEqual(@as(usize, 1), names.len);
    try testing.expectEqualStrings("unknown", names[0]);
    try testing.expectEqualStrings("\"ann,bob,cy\"", completedWith(try inst.activate(&try oneJob(a, try signalJob(a, "approve", .{@as([]const u8, "cy")})), a)));
}

test "a signal channel's tryReceive and len read without blocking; a multi-argument signal arrives as its tuple" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, ChannelPeekWf.registered, .{});
    defer inst.destroy();

    _ = try inst.activate(&try initActivation(a, "ChannelPeek", .{}), a);
    try testing.expectEqual(@as(usize, 0), commandsOf(try inst.activate(&try oneJob(a, try signalJob(a, "pair", .{ @as(i64, 1), @as(i64, 2) })), a)).len);
    try testing.expectEqualStrings("\"0 1+2 3+4 true 0\"", completedWith(try inst.activate(&try oneJob(a, try signalJob(a, "pair", .{ @as(i64, 3), @as(i64, 4) })), a)));
}

test "a signal channel receive returns error.Canceled when the workflow is canceled" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, ChannelCancelWf.registered, .{});
    defer inst.destroy();

    try testing.expectEqual(@as(usize, 0), commandsOf(try inst.activate(&try initActivation(a, "ChannelCancel", .{}), a)).len);
    try testing.expectEqualStrings("\"Canceled\"", completedWith(try inst.activate(&try oneJob(a, .{ .cancel_workflow = .{} }), a)));
}

test "a signal channel receive races a timer in a Select: a signal cancels the timer, the timer cancels the receive" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, ChannelQuietWf.registered, .{});
    defer inst.destroy();

    const first = commandsOf(try inst.activate(&try initActivation(a, "ChannelQuiet", .{}), a));
    try testing.expectEqual(@as(usize, 1), first.len);
    try testing.expectEqual(@as(u32, 1), first[0].variant.?.start_timer.seq);
    // The approval wins: its timer is canceled and the next round starts another.
    const won = commandsOf(try inst.activate(&try oneJob(a, try signalJob(a, "approve", .{@as([]const u8, "ann")})), a));
    try testing.expectEqual(@as(usize, 2), won.len);
    try testing.expectEqual(@as(u32, 1), won[0].variant.?.cancel_timer.seq);
    try testing.expectEqual(@as(u32, 2), won[1].variant.?.start_timer.seq);
    // The timer wins: the receive is canceled, and an approval after it stays unread.
    const quiet = commandsOf(try inst.activate(&try oneJob(a, .{ .fire_timer = .{ .seq = 2 } }), a));
    try testing.expectEqual(@as(usize, 1), quiet.len);
    try testing.expectEqual(@as(u32, 3), quiet[0].variant.?.start_timer.seq);
    try testing.expectEqual(@as(usize, 0), commandsOf(try inst.activate(&try oneJob(a, try signalJob(a, "approve", .{@as([]const u8, "bob")})), a)).len);
    try testing.expectEqualStrings("\"ann|1|approve\"", completedWith(try inst.activate(&try oneJob(a, .{ .fire_timer = .{ .seq = 3 } }), a)));
}

test "a signal channel receive wins a Select over a timer that fires in the same activation, so the signal isn't discarded" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, ChannelQuietWf.registered, .{});
    defer inst.destroy();

    _ = try inst.activate(&try initActivation(a, "ChannelQuiet", .{}), a);
    // Core puts signal jobs before timer jobs in one activation.
    var both = try oneJob(a, try signalJob(a, "approve", .{@as([]const u8, "ann")}));
    try both.jobs.append(a, .{ .variant = .{ .fire_timer = .{ .seq = 1 } } });
    const next = commandsOf(try inst.activate(&both, a));
    try testing.expectEqual(@as(usize, 1), next.len);
    try testing.expectEqual(@as(u32, 2), next[0].variant.?.start_timer.seq);
    try testing.expectEqual(@as(u32, 3), commandsOf(try inst.activate(&try oneJob(a, .{ .fire_timer = .{ .seq = 2 } }), a))[0].variant.?.start_timer.seq);
    try testing.expectEqualStrings("\"ann|0|\"", completedWith(try inst.activate(&try oneJob(a, .{ .fire_timer = .{ .seq = 3 } }), a)));
}

test "a Select over two signal channels returns one signal and leaves the other unread when both arrive together" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, TwoChannelsWf.registered, .{});
    defer inst.destroy();

    _ = try inst.activate(&try initActivation(a, "TwoChannels", .{@as(i64, 0)}), a);
    var both = try oneJob(a, try signalJob(a, "approve", .{@as([]const u8, "ann")}));
    try both.jobs.append(a, .{ .variant = try signalJob(a, "reject", .{@as([]const u8, "bob")}) });
    try testing.expectEqualStrings("\"a:ann,r:bob\"", completedWith(try inst.activate(&both, a)));
}

test "a Select over two signal channels that both hold an unread signal takes only one per round" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, TwoChannelsWf.registered, .{});
    defer inst.destroy();

    _ = try inst.activate(&try initActivation(a, "TwoChannels", .{@as(i64, 10)}), a);
    _ = try inst.activate(&try oneJob(a, try signalJob(a, "reject", .{@as([]const u8, "bob")})), a);
    _ = try inst.activate(&try oneJob(a, try signalJob(a, "approve", .{@as([]const u8, "ann")})), a);
    try testing.expectEqualStrings("\"a:ann,r:bob\"", completedWith(try inst.activate(&try oneJob(a, .{ .fire_timer = .{ .seq = 1 } }), a)));
}

test "a Select awaited again lets its other signal channel receive take its signal" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, SelectTwiceWf.registered, .{});
    defer inst.destroy();

    _ = try inst.activate(&try initActivation(a, "SelectTwice", .{}), a);
    var both = try oneJob(a, try signalJob(a, "reject", .{@as([]const u8, "bob")}));
    try both.jobs.append(a, .{ .variant = try signalJob(a, "approve", .{@as([]const u8, "ann")}) });
    try testing.expectEqualStrings("\"r:bob,a:ann\"", completedWith(try inst.activate(&both, a)));
}

// Signal channel receives in a std.Io.Group, and in a Select awaited for every result: a
// member that finished doesn't hold back a receive whose owner still waits for more.
const GroupReceivers = struct {
    const Approve = Signals.Approve;
    const Ch = workflow.SignalChannel(Approve.Args);

    fn sleeper(io: std.Io) void {
        io.sleep(.fromSeconds(1), .awake) catch {};
    }
    fn receiver(ch: *Ch, out: *[]const u8) void {
        out.* = ch.receive() catch "canceled";
    }

    /// A Group with a 1 s sleeper and a receiver; awaits the whole group.
    fn groupRun(ctx: *workflow.Context) ![]const u8 {
        const io = ctx.io();
        const ch = try ctx.signalChannel(Approve);
        var out: []const u8 = "none";
        var group: std.Io.Group = .init;
        group.async(io, sleeper, .{io});
        group.async(io, receiver, .{ ch, &out });
        try group.await(io);
        return out;
    }

    /// A Group of two receivers on one channel.
    fn twoReceivers(ctx: *workflow.Context) ![]const u8 {
        const io = ctx.io();
        const ch = try ctx.signalChannel(Approve);
        var a: []const u8 = "none";
        var b: []const u8 = "none";
        var group: std.Io.Group = .init;
        group.async(io, receiver, .{ ch, &a });
        group.async(io, receiver, .{ ch, &b });
        try group.await(io);
        return std.fmt.allocPrint(ctx.allocator(), "{s},{s}", .{ a, b });
    }

    const St = struct { got: ?[]const u8 = null };
    fn gotIt(s: *St) bool {
        return s.got != null;
    }
    fn receiveInto(ch: *Ch, st: *St) void {
        st.got = ch.receive() catch "canceled";
    }
    fn receiveThenSleep(ch: *Ch, st: *St, io: std.Io) void {
        st.got = ch.receive() catch "canceled";
        io.sleep(.fromSeconds(7), .awake) catch {};
    }

    /// A Group with a 1 s sleeper and a receiver; the owner waits with `ctx.await` for what
    /// the receiver got.
    fn ownerAwaits(ctx: *workflow.Context) ![]const u8 {
        const io = ctx.io();
        const ch = try ctx.signalChannel(Approve);
        var st: St = .{};
        var group: std.Io.Group = .init;
        defer group.cancel(io);
        group.async(io, sleeper, .{io});
        group.async(io, receiveInto, .{ ch, &st });
        try ctx.await(&st, gotIt);
        return st.got.?;
    }

    /// A Group with a 1 s sleeper and a receiver that sleeps 7 s once it got its signal; the
    /// owner sleeps 100 s before awaiting the Group.
    fn ownerSleeps(ctx: *workflow.Context) ![]const u8 {
        const io = ctx.io();
        const ch = try ctx.signalChannel(Approve);
        var st: St = .{};
        var group: std.Io.Group = .init;
        group.async(io, sleeper, .{io});
        group.async(io, receiveThenSleep, .{ ch, &st, io });
        try io.sleep(.fromSeconds(100), .awake);
        try group.await(io);
        return st.got orelse "none";
    }

    fn setAfter(io: std.Io, ev: *std.Io.Event) void {
        io.sleep(.fromSeconds(5), .awake) catch {};
        ev.set(io);
    }
    /// Races a receive against a 1 s timer, but waits on an Event set after 5 s before it
    /// awaits the Select; then cancels the Select with `cancel` until it returns null.
    fn selectLate(ctx: *workflow.Context) ![]const u8 {
        const io = ctx.io();
        const ch = try ctx.signalChannel(Approve);
        var ev: std.Io.Event = .unset;
        var setter = io.async(setAfter, .{ io, &ev });
        var buf: [2]Race = undefined;
        var select: std.Io.Select(Race) = .init(io, &buf);
        select.async(.approval, Ch.receive, .{ch});
        select.async(.timeout, std.Io.sleep, .{ io, .fromSeconds(1), .awake });
        try ev.wait(io);
        setter.await(io);
        var got: std.ArrayList([]const u8) = .empty;
        var next: ?Race = try select.await();
        while (next) |r| : (next = select.cancel()) switch (r) {
            .approval => |v| try got.append(ctx.allocator(), v catch "canceled"),
            .timeout => |v| if (v) try got.append(ctx.allocator(), "timeout") else |_| {},
        };
        return std.fmt.allocPrint(ctx.allocator(), "{s}|left={d}", .{ try std.mem.join(ctx.allocator(), ",", got.items), ch.len() });
    }

    const Race = union(enum) { approval: error{Canceled}![]const u8, timeout: std.Io.Cancelable!void };
    /// A Select whose owner waits for both results with awaitMany.
    fn selectBoth(ctx: *workflow.Context) ![]const u8 {
        const io = ctx.io();
        const ch = try ctx.signalChannel(Approve);
        var buf: [2]Race = undefined;
        var select: std.Io.Select(Race) = .init(io, &buf);
        defer select.cancelDiscard();
        select.async(.approval, Ch.receive, .{ch});
        select.async(.timeout, std.Io.sleep, .{ io, .fromSeconds(1), .awake });
        var got: [2]Race = undefined;
        const n = try select.awaitMany(&got, 2);
        for (got[0..n]) |r| switch (r) {
            .approval => |v| return v,
            .timeout => {},
        };
        return "no approval";
    }
};
const GroupReceiveWf = workflow.Def("GroupReceive", GroupReceivers.groupRun);
const GroupTwoReceiversWf = workflow.Def("GroupTwoReceivers", GroupReceivers.twoReceivers);
const SelectBothWf = workflow.Def("SelectBoth", GroupReceivers.selectBoth);
const GroupOwnerAwaitsWf = workflow.Def("GroupOwnerAwaits", GroupReceivers.ownerAwaits);
const GroupOwnerSleepsWf = workflow.Def("GroupOwnerSleeps", GroupReceivers.ownerSleeps);
const SelectLateWf = workflow.Def("SelectLate", GroupReceivers.selectLate);

test "a receive in a Group gets its signal at once while the owner waits in ctx.await after a sibling finished" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, GroupOwnerAwaitsWf.registered, .{});
    defer inst.destroy();

    _ = try inst.activate(&try initActivation(a, "GroupOwnerAwaits", .{}), a);
    try testing.expectEqual(@as(usize, 0), commandsOf(try inst.activate(&try oneJob(a, .{ .fire_timer = .{ .seq = 1 } }), a)).len);
    try testing.expectEqualStrings("\"ann\"", completedWith(try inst.activate(&try oneJob(a, try signalJob(a, "approve", .{@as([]const u8, "ann")})), a)));
}

test "a receive in a Group gets its signal at once while the owner sleeps after a sibling finished" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, GroupOwnerSleepsWf.registered, .{});
    defer inst.destroy();

    try testing.expectEqual(@as(usize, 2), commandsOf(try inst.activate(&try initActivation(a, "GroupOwnerSleeps", .{}), a)).len);
    try testing.expectEqual(@as(usize, 0), commandsOf(try inst.activate(&try oneJob(a, .{ .fire_timer = .{ .seq = 2 } }), a)).len);
    // The receiver's 7 s timer starts in the signal's activation, not after the owner's 100 s.
    const got = commandsOf(try inst.activate(&try oneJob(a, try signalJob(a, "approve", .{@as([]const u8, "ann")})), a));
    try testing.expectEqual(@as(usize, 1), got.len);
    try testing.expectEqual(@as(i64, 7), got[0].variant.?.start_timer.start_to_fire_timeout.?.seconds);
    _ = try inst.activate(&try oneJob(a, .{ .fire_timer = .{ .seq = 3 } }), a);
    try testing.expectEqualStrings("\"ann\"", completedWith(try inst.activate(&try oneJob(a, .{ .fire_timer = .{ .seq = 1 } }), a)));
}

test "a Select canceled with cancel until null returns a receive that finished after the first result" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, SelectLateWf.registered, .{});
    defer inst.destroy();

    _ = try inst.activate(&try initActivation(a, "SelectLate", .{}), a);
    _ = try inst.activate(&try oneJob(a, .{ .fire_timer = .{ .seq = 2 } }), a); // the Select's 1 s timer
    _ = try inst.activate(&try oneJob(a, try signalJob(a, "approve", .{@as([]const u8, "ann")})), a);
    try testing.expectEqualStrings("\"timeout,ann|left=0\"", completedWith(try inst.activate(&try oneJob(a, .{ .fire_timer = .{ .seq = 1 } }), a)));
}

test "a receive in a Group gets its signal after a sibling member finished, while the owner awaits the Group" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, GroupReceiveWf.registered, .{});
    defer inst.destroy();
    const first = commandsOf(try inst.activate(&try initActivation(a, "GroupReceive", .{}), a));
    try testing.expectEqual(@as(usize, 1), first.len);
    _ = try inst.activate(&try oneJob(a, .{ .fire_timer = .{ .seq = 1 } }), a);
    const c = try inst.activate(&try oneJob(a, try signalJob(a, "approve", .{@as([]const u8, "ann")})), a);
    try testing.expectEqualStrings("\"ann\"", completedWith(c));
}

test "two receives on one channel in a Group each get one of two signals that arrive together" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, GroupTwoReceiversWf.registered, .{});
    defer inst.destroy();
    _ = try inst.activate(&try initActivation(a, "GroupTwoReceivers", .{}), a);
    var both = try oneJob(a, try signalJob(a, "approve", .{@as([]const u8, "ann")}));
    try both.jobs.append(a, .{ .variant = try signalJob(a, "approve", .{@as([]const u8, "bob")}) });
    const c = try inst.activate(&both, a);
    try testing.expectEqualStrings("\"ann,bob\"", completedWith(c));
}

test "a receive in a Select awaited for both results gets its signal after the timer finished" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, SelectBothWf.registered, .{});
    defer inst.destroy();
    _ = try inst.activate(&try initActivation(a, "SelectBoth", .{}), a);
    _ = try inst.activate(&try oneJob(a, .{ .fire_timer = .{ .seq = 1 } }), a);
    const c = try inst.activate(&try oneJob(a, try signalJob(a, "approve", .{@as([]const u8, "ann")})), a);
    try testing.expectEqualStrings("\"ann\"", completedWith(c));
}

test "signals on their way to a just-opened channel are listed as unhandled" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, OpenNoYieldWf.registered, .{});
    defer inst.destroy();

    var start = try initActivation(a, "OpenNoYield", .{});
    try start.jobs.append(a, .{ .variant = try signalJob(a, "approve", .{@as([]const u8, "ann")}) });
    _ = try inst.activate(&start, a);
    // len and tryReceive see the signal once the caller blocks, as documented.
    try testing.expectEqualStrings("\"approve|0|false\"", completedWith(try inst.activate(&try oneJob(a, .{ .fire_timer = .{ .seq = 1 } }), a)));
}

test "unhandled signal names list buffered and unread channel signals once each, in first-arrival order, and the finish warning counts both" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    var recorder: LogRecorder = .{ .arena = .init(testing.allocator), .min = .warn };
    defer recorder.arena.deinit();
    const inst = try create(testing.allocator, LeaveUnreadWf.registered, .{ .logger = recorder.sink() });
    defer inst.destroy();

    var start = try initActivation(a, "LeaveUnread", .{});
    try start.jobs.append(a, .{ .variant = try signalJob(a, "x", .{}) });
    _ = try inst.activate(&start, a);
    var more = try oneJob(a, try signalJob(a, "approve", .{@as([]const u8, "ann")}));
    try more.jobs.append(a, .{ .variant = try signalJob(a, "y", .{}) });
    try more.jobs.append(a, .{ .variant = try signalJob(a, "approve", .{@as([]const u8, "bob")}) });
    try more.jobs.append(a, .{ .variant = try signalJob(a, "x", .{}) });
    _ = try inst.activate(&more, a);
    try testing.expectEqual(@as(usize, 0), recorder.count);
    try testing.expectEqualStrings("\"x,approve,y\"", completedWith(try inst.activate(&try oneJob(a, .{ .fire_timer = .{ .seq = 1 } }), a)));
    try testing.expectEqual(@as(usize, 1), recorder.count);
    try testing.expectEqualStrings("5", recorder.get("count").?);
    try testing.expectEqualStrings("x, approve, y", recorder.get("SignalNames").?);
}

test "a removed signal handler finishes its running coroutines, and later signals are buffered or go to the dynamic handler" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, RemovalWf.registered, .{});
    defer inst.destroy();

    _ = try inst.activate(&try initActivation(a, "Removal", .{}), a);
    const handling = commandsOf(try inst.activate(&try oneJob(a, try signalJob(a, "approve", .{@as([]const u8, "ann")})), a));
    try testing.expectEqual(@as(usize, 2), handling.len); // the handler's 5 s timer, the workflow's 10 s
    var later = try oneJob(a, try signalJob(a, "approve", .{@as([]const u8, "bob")}));
    try later.jobs.append(a, .{ .variant = try signalJob(a, "unlock", .{@as([]const u8, "key")}) });
    try testing.expectEqual(@as(usize, 0), commandsOf(try inst.activate(&later, a)).len);
    try testing.expectEqual(@as(usize, 2), inst.buffered_signals.items.len);
    try testing.expectEqual(@as(usize, 0), commandsOf(try inst.activate(&try oneJob(a, .{ .fire_timer = .{ .seq = 1 } }), a)).len);
    try testing.expectEqual(@as(u32, 3), commandsOf(try inst.activate(&try oneJob(a, .{ .fire_timer = .{ .seq = 2 } }), a))[0].variant.?.start_timer.seq);
    _ = try inst.activate(&try oneJob(a, try signalJob(a, "approve", .{@as([]const u8, "cy")})), a);
    try testing.expectEqualStrings("\"ann,ann!,[approve,unlock],approve=bob,unlock=key,[approve]\"", completedWith(try inst.activate(&try oneJob(a, .{ .fire_timer = .{ .seq = 3 } }), a)));
}

test "a removed signal channel keeps the signals it took, and later ones wait for a new channel" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, ChannelRemovedWf.registered, .{});
    defer inst.destroy();

    _ = try inst.activate(&try initActivation(a, "ChannelRemoved", .{}), a);
    try testing.expectEqual(@as(u32, 1), commandsOf(try inst.activate(&try oneJob(a, try signalJob(a, "approve", .{@as([]const u8, "ann")})), a))[0].variant.?.start_timer.seq);
    _ = try inst.activate(&try oneJob(a, try signalJob(a, "approve", .{@as([]const u8, "bob")})), a);
    try testing.expectEqualStrings("\"ann|approve|bob|0\"", completedWith(try inst.activate(&try oneJob(a, .{ .fire_timer = .{ .seq = 1 } }), a)));
}

// Update handlers. `State.value` is what `set` updates change; a `done` signal ends the run.
const Updates = struct {
    const Set = workflow.UpdateDef("set", fn (value: i64) i64);
    const SetSlowly = workflow.UpdateDef("set_slowly", fn (value: i64) i64);
    const Break = workflow.UpdateDef("break", fn () void);
    const Done = workflow.SignalDef("done", fn () void);

    const State = struct {
        ctx: *workflow.Context,
        value: i64 = 0,
        done: bool = false,

        fn create(ctx: *workflow.Context) !*State {
            const s = try ctx.allocator().create(State);
            s.* = .{ .ctx = ctx };
            return s;
        }
        /// Returns the previous value.
        fn set(s: *State, value: i64) i64 {
            defer s.value = value;
            return s.value;
        }
        fn setSlowly(s: *State, value: i64) !i64 {
            try s.ctx.io().sleep(.fromSeconds(5), .awake);
            return s.set(value);
        }
        fn positive(_: *State, value: i64) error{NotPositive}!void {
            if (value <= 0) return error.NotPositive;
        }
        fn breaks(_: *State) error{Broken}!void {
            return error.Broken;
        }
        fn finish(s: *State) void {
            s.done = true;
        }
        fn finished(s: *State) bool {
            return s.done;
        }
    };

    /// Handles updates until a `done` signal, waits for running handlers, and returns the
    /// value.
    fn run(ctx: *workflow.Context) !i64 {
        const s = try State.create(ctx);
        try ctx.setUpdateHandlerWithValidator(Set, s, State.set, State.positive);
        try ctx.setUpdateHandler(SetSlowly, s, State.setSlowly);
        try ctx.setUpdateHandler(Break, s, State.breaks);
        try ctx.setSignalHandler(Done, s, State.finish);
        try ctx.await(s, State.finished);
        try ctx.await(ctx, workflow.Context.allHandlersFinished);
        return s.value;
    }

    /// Registers `set` only after sleeping 10 s.
    fn late(ctx: *workflow.Context) !i64 {
        const s = try State.create(ctx);
        try ctx.io().sleep(.fromSeconds(10), .awake);
        try ctx.setUpdateHandler(Set, s, State.set);
        try ctx.await(s, State.finished);
        return s.value;
    }

    /// Returns at once, leaving `set` registered.
    fn quick(ctx: *workflow.Context) !i64 {
        const s = try State.create(ctx);
        try ctx.setUpdateHandler(Set, s, State.set);
        return 7;
    }

    fn onStack(ctx: *workflow.Context) ![]const u8 {
        var s: State = .{ .ctx = ctx };
        ctx.setUpdateHandler(Set, &s, State.set) catch |err| return @errorName(err);
        return "registered";
    }
};
const UpdatesWf = workflow.Def("Updates", Updates.run);
const LateUpdatesWf = workflow.Def("LateUpdates", Updates.late);
const QuickUpdatesWf = workflow.Def("QuickUpdates", Updates.quick);
const UpdateOnStackWf = workflow.Def("UpdateOnStack", Updates.onStack);

fn updateJob(a: Allocator, id: []const u8, name: []const u8, args: anytype, run_validator: bool) !act_pb.WorkflowActivationJob.variant_union {
    var job: act_pb.DoUpdate = .{ .id = id, .protocol_instance_id = id, .name = name, .run_validator = run_validator };
    job.input = try converter.toPayloads(a, args);
    return .{ .do_update = job };
}

/// The update response in `cmd`, checking it is for update `id`.
fn updateResponse(cmd: cmd_pb.WorkflowCommand, id: []const u8) !cmd_pb.UpdateResponse.response_union {
    const r = cmd.variant.?.update_response;
    try testing.expectEqualStrings(id, r.protocol_instance_id);
    return r.response.?;
}

test "update: accepted, then completed with the handler's result" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, UpdatesWf.registered, .{});
    defer inst.destroy();

    _ = try inst.activate(&try initActivation(a, "Updates", .{}), a);
    const cmds = commandsOf(try inst.activate(&try oneJob(a, try updateJob(a, "u1", "set", .{@as(i64, 5)}, true)), a));
    try testing.expectEqual(@as(usize, 2), cmds.len);
    try testing.expect(try updateResponse(cmds[0], "u1") == .accepted);
    try testing.expectEqualStrings("0", (try updateResponse(cmds[1], "u1")).completed.data);
    try testing.expectEqualStrings("5", completedWith(try inst.activate(&try oneJob(a, try signalJob(a, "done", .{})), a)));
}

test "update: a validator's error rejects it, and nothing else runs" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, UpdatesWf.registered, .{});
    defer inst.destroy();

    _ = try inst.activate(&try initActivation(a, "Updates", .{}), a);
    // The argument doesn't decode, or the validator rejects it.
    var act = try oneJob(a, try updateJob(a, "u1", "set", .{@as(i64, -1)}, true));
    try act.jobs.append(a, .{ .variant = try updateJob(a, "u2", "set", .{@as([]const u8, "five")}, true) });
    const cmds = commandsOf(try inst.activate(&act, a));
    try testing.expectEqual(@as(usize, 2), cmds.len);
    const rejected = (try updateResponse(cmds[0], "u1")).rejected;
    try testing.expectEqualStrings("NotPositive", rejected.message);
    try testing.expectEqualStrings("NotPositive", failure_mod.applicationType(rejected).?);
    try testing.expect(try updateResponse(cmds[1], "u2") == .rejected);
    try testing.expectEqualStrings("0", completedWith(try inst.activate(&try oneJob(a, try signalJob(a, "done", .{})), a)));
}

test "update: on replay the validator doesn't run and the update is accepted" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, UpdatesWf.registered, .{});
    defer inst.destroy();

    _ = try inst.activate(&try initActivation(a, "Updates", .{}), a);
    const cmds = commandsOf(try inst.activate(&try oneJob(a, try updateJob(a, "u1", "set", .{@as(i64, -1)}, false)), a));
    try testing.expectEqual(@as(usize, 2), cmds.len);
    try testing.expect(try updateResponse(cmds[0], "u1") == .accepted);
    try testing.expect(try updateResponse(cmds[1], "u1") == .completed);
}

test "update: a handler's error rejects it after acceptance without failing the workflow" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, UpdatesWf.registered, .{});
    defer inst.destroy();

    _ = try inst.activate(&try initActivation(a, "Updates", .{}), a);
    const cmds = commandsOf(try inst.activate(&try oneJob(a, try updateJob(a, "u1", "break", .{}, true)), a));
    try testing.expectEqual(@as(usize, 2), cmds.len);
    try testing.expect(try updateResponse(cmds[0], "u1") == .accepted);
    try testing.expectEqualStrings("Broken", (try updateResponse(cmds[1], "u1")).rejected.message);
    try testing.expectEqualStrings("0", completedWith(try inst.activate(&try oneJob(a, try signalJob(a, "done", .{})), a)));
}

test "update: a handler that blocks on a timer completes in a later activation, and allHandlersFinished waits for it" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, UpdatesWf.registered, .{});
    defer inst.destroy();

    _ = try inst.activate(&try initActivation(a, "Updates", .{}), a);
    try testing.expect(inst.ctx.allHandlersFinished());
    var act = try oneJob(a, try updateJob(a, "u1", "set_slowly", .{@as(i64, 9)}, true));
    try act.jobs.append(a, .{ .variant = try signalJob(a, "done", .{}) });
    const blocked = commandsOf(try inst.activate(&act, a));
    try testing.expectEqual(@as(usize, 2), blocked.len);
    try testing.expect(try updateResponse(blocked[0], "u1") == .accepted);
    try testing.expect(blocked[1].variant.? == .start_timer);
    try testing.expect(!inst.ctx.allHandlersFinished());

    const done = commandsOf(try inst.activate(&try oneJob(a, .{ .fire_timer = .{ .seq = 1 } }), a));
    try testing.expectEqual(@as(usize, 2), done.len);
    try testing.expectEqualStrings("0", (try updateResponse(done[0], "u1")).completed.data);
    try testing.expectEqualStrings("9", done[1].variant.?.complete_workflow_execution.result.?.data);
    try testing.expect(inst.ctx.allHandlersFinished());
}

test "update: one delivered with the start waits for the workflow function to register its handler" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, UpdatesWf.registered, .{});
    defer inst.destroy();

    var start = try initActivation(a, "Updates", .{});
    try start.jobs.append(a, .{ .variant = try updateJob(a, "u1", "set", .{@as(i64, 3)}, true) });
    const cmds = commandsOf(try inst.activate(&start, a));
    try testing.expectEqual(@as(usize, 2), cmds.len);
    try testing.expect(try updateResponse(cmds[0], "u1") == .accepted);
    try testing.expect(try updateResponse(cmds[1], "u1") == .completed);
    try testing.expectEqual(@as(usize, 0), inst.buffered_updates.items.len);
}

test "update: one with no handler once the coroutines block is rejected, naming the registered updates" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();

    const registered = try create(testing.allocator, UpdatesWf.registered, .{});
    defer registered.destroy();
    _ = try registered.activate(&try initActivation(a, "Updates", .{}), a);
    const cmds = commandsOf(try registered.activate(&try oneJob(a, try updateJob(a, "u1", "nope", .{}, true)), a));
    try testing.expectEqual(@as(usize, 1), cmds.len);
    const rejected = (try updateResponse(cmds[0], "u1")).rejected;
    try testing.expectEqualStrings("UpdateNotRegistered", failure_mod.applicationType(rejected).?);
    try testing.expectEqualStrings("update \"nope\" is not registered on workflow \"Updates\"; registered updates: [set, set_slowly, break]", rejected.message);

    // Sent with the start, before a handler that is registered only after a timer.
    const late = try create(testing.allocator, LateUpdatesWf.registered, .{});
    defer late.destroy();
    var start = try initActivation(a, "LateUpdates", .{});
    try start.jobs.append(a, .{ .variant = try updateJob(a, "u2", "set", .{@as(i64, 3)}, true) });
    const first = commandsOf(try late.activate(&start, a));
    try testing.expectEqual(@as(usize, 2), first.len);
    try testing.expect(first[0].variant.? == .start_timer);
    try testing.expectEqualStrings("update \"set\" is not registered on workflow \"LateUpdates\"; registered updates: []", (try updateResponse(first[1], "u2")).rejected.message);
    try testing.expectEqual(@as(usize, 0), late.buffered_updates.items.len);
}

test "update: one delivered after the workflow finished is rejected" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, QuickUpdatesWf.registered, .{});
    defer inst.destroy();

    try testing.expectEqualStrings("7", completedWith(try inst.activate(&try initActivation(a, "QuickUpdates", .{}), a)));
    const cmds = commandsOf(try inst.activate(&try oneJob(a, try updateJob(a, "u1", "set", .{@as(i64, 1)}, true)), a));
    try testing.expectEqual(@as(usize, 1), cmds.len);
    try testing.expectEqualStrings("WorkflowFinished", failure_mod.applicationType((try updateResponse(cmds[0], "u1")).rejected).?);
}

test "update: eviction cancels a handler blocked on a timer" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, UpdatesWf.registered, .{});
    _ = try inst.activate(&try initActivation(a, "Updates", .{}), a);
    _ = try inst.activate(&try oneJob(a, try updateJob(a, "u1", "set_slowly", .{@as(i64, 1)}, true)), a);
    inst.destroy(); // testing.allocator reports leaks
}

test "an update handler context on the coroutine's stack is rejected" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, UpdateOnStackWf.registered, .{});
    defer inst.destroy();
    try testing.expectEqualStrings("\"UpdateContextOnStack\"", completedWith(try inst.activate(&try initActivation(a, "UpdateOnStack", .{}), a)));
}

// Failure details of handlers: each signal or update handler keeps its own `ctx.fail` and
// failed-future details, apart from the workflow function's.
const HandlerFailures = struct {
    const Charge = workflow.UpdateDef("charge", fn (amount: i64) i64);
    const Refuse = workflow.UpdateDef("refuse", fn () void);
    const Plain = workflow.UpdateDef("plain", fn () void);
    const Explode = workflow.SignalDef("explode", fn () void);
    const Done = workflow.SignalDef("done", fn () void);
    const KeepUpdate = workflow.UpdateDef("keep", fn () void);
    const KeepSignal = workflow.SignalDef("keep", fn () void);

    const State = struct {
        ctx: *workflow.Context,
        done: bool = false,

        fn charge(s: *State, amount: i64) !i64 {
            _ = try s.ctx.executeActivity(Greet, .{ .start_to_close_timeout = .fromSeconds(10) }, .{"charge"}).get();
            return amount;
        }
        fn refuse(s: *State) error{ApplicationFailure}!void {
            return s.ctx.fail(.{ .message = "refused", .type = "Refusal", .non_retryable = true });
        }
        fn plain(_: *State) error{ApplicationFailure}!void {
            return error.ApplicationFailure;
        }
        fn explode(s: *State) !void {
            _ = try s.ctx.executeActivity(Greet, .{ .start_to_close_timeout = .fromSeconds(10) }, .{"explode"}).get();
        }
        /// A charge that fails, then a cleanup that fails too; fails with the charge's failure.
        fn keepCharge(s: *State) !void {
            const opts: workflow.ActivityOptions = .{ .start_to_close_timeout = .fromSeconds(10) };
            const charged = s.ctx.executeActivity(Greet, opts, .{"charge"});
            _ = charged.get() catch |err| {
                _ = s.ctx.executeActivity(Greet, opts, .{"cleanup"}).get() catch "";
                return switch (err) {
                    error.ActivityFailed => s.ctx.failWith(charged.failure().?),
                    else => err,
                };
            };
        }
        fn finish(s: *State) void {
            s.done = true;
        }
        fn finished(s: *State) bool {
            return s.done;
        }
    };

    /// Handles updates and signals until `done`. With `fail_first`, calls `ctx.fail` before
    /// waiting and returns its error; otherwise fails with error.Finished.
    fn run(ctx: *workflow.Context, fail_first: bool) !i64 {
        const s = try ctx.allocator().create(State);
        s.* = .{ .ctx = ctx };
        try ctx.setUpdateHandler(Charge, s, State.charge);
        try ctx.setUpdateHandler(Refuse, s, State.refuse);
        try ctx.setUpdateHandler(Plain, s, State.plain);
        try ctx.setSignalHandler(Explode, s, State.explode);
        try ctx.setUpdateHandler(KeepUpdate, s, State.keepCharge);
        try ctx.setSignalHandler(KeepSignal, s, State.keepCharge);
        try ctx.setSignalHandler(Done, s, State.finish);
        const early: ?error{ApplicationFailure} = if (fail_first) ctx.fail(.{ .message = "workflow failed", .type = "WorkflowFailed" }) else null;
        try ctx.await(s, State.finished);
        if (early) |e| return e;
        return error.Finished;
    }
};
const HandlerFailuresWf = workflow.Def("HandlerFailures", HandlerFailures.run);

/// An activity failure caused by an application failure, as the server reports one.
fn activityFailure(a: Allocator, message: []const u8, app_type: []const u8) !*Failure {
    const cause = try a.create(Failure);
    cause.* = try failure_mod.application(a, .{ .message = message, .type = app_type });
    const f = try a.create(Failure);
    f.* = .{ .message = "activity error", .failure_info = .{ .activity_failure_info = .{ .activity_type = .{ .name = "Activity" } } }, .cause = cause };
    return f;
}

fn resolveFailed(a: Allocator, seq: u32, f: *const Failure) !act_pb.WorkflowActivation {
    return oneJob(a, .{ .resolve_activity = .{ .seq = seq, .result = .{ .status = .{ .failed = .{ .failure = f.* } } } } });
}

test "update: a handler whose activity fails is rejected with the activity's failure" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, HandlerFailuresWf.registered, .{});
    defer inst.destroy();

    _ = try inst.activate(&try initActivation(a, "HandlerFailures", .{false}), a);
    const scheduled = commandsOf(try inst.activate(&try oneJob(a, try updateJob(a, "u1", "charge", .{@as(i64, -5)}, true)), a));
    try testing.expectEqual(@as(usize, 2), scheduled.len);
    try testing.expect(scheduled[1].variant.? == .schedule_activity);

    const cmds = commandsOf(try inst.activate(&try resolveFailed(a, 1, try activityFailure(a, "invalid Amount", "InvalidAmount")), a));
    try testing.expectEqual(@as(usize, 1), cmds.len);
    const rejected = (try updateResponse(cmds[0], "u1")).rejected;
    try testing.expectEqualStrings("activity error", rejected.message);
    try testing.expect(rejected.failure_info.? == .activity_failure_info);
    try testing.expectEqualStrings("invalid Amount", rejected.cause.?.message);
    try testing.expectEqualStrings("InvalidAmount", failure_mod.applicationType(rejected.cause.?.*).?);

    // The handler's activity failure doesn't become the workflow's.
    const done = commandsOf(try inst.activate(&try oneJob(a, try signalJob(a, "done", .{})), a));
    try testing.expectEqualStrings("Finished", done[0].variant.?.fail_workflow_execution.failure.?.message);
}

test "update: ctx.fail in a handler rejects it with that failure, and doesn't fail the workflow" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, HandlerFailuresWf.registered, .{});
    defer inst.destroy();

    _ = try inst.activate(&try initActivation(a, "HandlerFailures", .{false}), a);
    const cmds = commandsOf(try inst.activate(&try oneJob(a, try updateJob(a, "u1", "refuse", .{}, true)), a));
    try testing.expectEqual(@as(usize, 2), cmds.len);
    const rejected = (try updateResponse(cmds[1], "u1")).rejected;
    try testing.expectEqualStrings("refused", rejected.message);
    try testing.expectEqualStrings("Refusal", failure_mod.applicationType(rejected).?);
    try testing.expect(rejected.failure_info.?.application_failure_info.non_retryable);

    const done = commandsOf(try inst.activate(&try oneJob(a, try signalJob(a, "done", .{})), a));
    const fail = done[0].variant.?.fail_workflow_execution.failure.?;
    try testing.expectEqualStrings("Finished", fail.message);
    try testing.expectEqualStrings("Finished", failure_mod.applicationType(fail).?);
}

test "update: the workflow function's ctx.fail doesn't describe a handler's rejection" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, HandlerFailuresWf.registered, .{});
    defer inst.destroy();

    _ = try inst.activate(&try initActivation(a, "HandlerFailures", .{true}), a);
    const cmds = commandsOf(try inst.activate(&try oneJob(a, try updateJob(a, "u1", "plain", .{}, true)), a));
    try testing.expectEqual(@as(usize, 2), cmds.len);
    try testing.expectEqualStrings("ApplicationFailure", (try updateResponse(cmds[1], "u1")).rejected.message);

    const done = commandsOf(try inst.activate(&try oneJob(a, try signalJob(a, "done", .{})), a));
    const fail = done[0].variant.?.fail_workflow_execution.failure.?;
    try testing.expectEqualStrings("workflow failed", fail.message);
    try testing.expectEqualStrings("WorkflowFailed", failure_mod.applicationType(fail).?);
}

test "a signal handler's activity failure fails the workflow with that failure" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, HandlerFailuresWf.registered, .{});
    defer inst.destroy();

    _ = try inst.activate(&try initActivation(a, "HandlerFailures", .{false}), a);
    const scheduled = commandsOf(try inst.activate(&try oneJob(a, try signalJob(a, "explode", .{})), a));
    try testing.expectEqual(@as(usize, 1), scheduled.len);
    const cmds = commandsOf(try inst.activate(&try resolveFailed(a, 1, try activityFailure(a, "invalid Amount", "InvalidAmount")), a));
    try testing.expectEqual(@as(usize, 1), cmds.len);
    const fail = cmds[0].variant.?.fail_workflow_execution.failure.?;
    try testing.expectEqualStrings("activity error", fail.message);
    try testing.expectEqualStrings("invalid Amount", fail.cause.?.message);
}

// `ctx.failWith` keeps a failure that cleanup on the error path would otherwise replace.
const KeepFailure = struct {
    const opts: workflow.ActivityOptions = .{ .start_to_close_timeout = .fromSeconds(10) };

    /// Runs "charge"; when it fails, runs "cleanup", then fails as `mode` says: "plain"
    /// returns the charge's error, "keep" fails with the charge's failure, "fail_then_keep"
    /// calls ctx.fail and then ctx.failWith, and "keep_then_fail" the other way round.
    fn run(ctx: *workflow.Context, mode: []const u8) ![]const u8 {
        if (std.mem.eql(u8, mode, "errdefer_keep")) return withErrdefer(ctx, true);
        if (std.mem.eql(u8, mode, "errdefer_plain")) return withErrdefer(ctx, false);
        const charged = ctx.executeActivity(Greet, opts, .{"charge"});
        _ = charged.get() catch |err| {
            _ = ctx.executeActivity(Greet, opts, .{"cleanup"}).get() catch "";
            if (std.mem.eql(u8, mode, "keep")) return switch (err) {
                error.ActivityFailed => ctx.failWith(charged.failure().?),
                else => err,
            };
            // Both calls return error.ApplicationFailure; the switch discards the first's.
            if (std.mem.eql(u8, mode, "fail_then_keep")) return switch (ctx.fail(.{ .message = "replaced", .type = "Replaced" })) {
                error.ApplicationFailure => ctx.failWith(charged.failure().?),
            };
            if (std.mem.eql(u8, mode, "keep_then_fail")) return switch (ctx.failWith(charged.failure().?)) {
                error.ApplicationFailure => ctx.fail(.{ .message = "chosen later", .type = "Later" }),
            };
            return err;
        };
        return "charged";
    }

    /// The cleanup runs from an errdefer, after the charge's failure is chosen (or not).
    fn withErrdefer(ctx: *workflow.Context, keep: bool) ![]const u8 {
        errdefer _ = ctx.executeActivity(Greet, opts, .{"cleanup"}).get() catch "";
        const charged = ctx.executeActivity(Greet, opts, .{"charge"});
        _ = charged.get() catch |err| return if (keep) ctx.failWith(charged.failure().?) else err;
        return "charged";
    }
};
const KeepFailureWf = workflow.Def("KeepFailure", KeepFailure.run);

/// Runs `KeepFailureWf` in `mode` with a charge (seq 1) and a cleanup (seq 2) that both
/// fail, and returns the workflow's failure, which lives as long as `inst`.
fn failureAfterCleanup(a: Allocator, inst: *Instance, mode: []const u8) !Failure {
    const scheduled = commandsOf(try inst.activate(&try initActivation(a, "KeepFailure", .{mode}), a));
    try testing.expectEqual(@as(usize, 1), scheduled.len);
    const cleanup = commandsOf(try inst.activate(&try resolveFailed(a, 1, try activityFailure(a, "card declined", "Declined")), a));
    try testing.expectEqual(@as(usize, 1), cleanup.len);
    try testing.expectEqualStrings("\"cleanup\"", cleanup[0].variant.?.schedule_activity.arguments.items[0].data);
    const done = commandsOf(try inst.activate(&try resolveFailed(a, 2, try activityFailure(a, "cleanup failed", "CleanupFailed")), a));
    try testing.expectEqual(@as(usize, 1), done.len);
    return done[0].variant.?.fail_workflow_execution.failure.?;
}

/// Checks that `f` is the charge's activity failure, cause and all.
fn expectChargeFailure(f: Failure) !void {
    try testing.expectEqualStrings("activity error", f.message);
    try testing.expect(f.failure_info.? == .activity_failure_info);
    try testing.expectEqualStrings("card declined", f.cause.?.message);
    try testing.expectEqualStrings("Declined", failure_mod.applicationType(f.cause.?.*).?);
}

test "failWith: a caught cleanup failure doesn't replace the failure the workflow chose" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();

    const kept = try create(testing.allocator, KeepFailureWf.registered, .{});
    defer kept.destroy();
    try expectChargeFailure(try failureAfterCleanup(a, kept, "keep"));

    // Without failWith, the returned error reports the last failure a get returned.
    const plain = try create(testing.allocator, KeepFailureWf.registered, .{});
    defer plain.destroy();
    const f = try failureAfterCleanup(a, plain, "plain");
    try testing.expectEqualStrings("activity error", f.message);
    try testing.expectEqualStrings("cleanup failed", f.cause.?.message);
}

/// Fails with the failure of its activity under its own, as `ctx.fail` with a future's
/// failure as `cause` does in the README.
const WrapFailure = struct {
    fn run(ctx: *workflow.Context) ![]const u8 {
        const opts: workflow.ActivityOptions = .{ .start_to_close_timeout = .fromSeconds(10) };
        const charged = ctx.executeActivity(Greet, opts, .{"charge"});
        _ = charged.get() catch |err| return switch (err) {
            error.ActivityFailed => ctx.fail(.{ .message = "order failed", .type = "OrderFailed", .cause = charged.failure().? }),
            else => err,
        };
        return "charged";
    }
};
const WrapFailureWf = workflow.Def("WrapFailure", WrapFailure.run);

test "ctx.fail copies a deep cause chain without overflowing the workflow coroutine's stack" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, WrapFailureWf.registered, .{});
    defer inst.destroy();

    const scheduled = commandsOf(try inst.activate(&try initActivation(a, "WrapFailure", .{}), a));
    try testing.expectEqual(@as(usize, 1), scheduled.len);
    // The activity's failure carries a long chain, as one from a tree of child workflows
    // that each fail with their child's failure as the cause does. The copy `ctx.fail`
    // makes runs on the workflow's coroutine stack (`Options.stack_size`, 256 KiB here).
    var chain = try activityFailure(a, "card declined", "Declined");
    for (0..200) |_| {
        const next = try a.create(Failure);
        next.* = .{ .message = "child workflow failed", .failure_info = .{ .child_workflow_execution_failure_info = .{} }, .cause = chain };
        chain = next;
    }
    const done = commandsOf(try inst.activate(&try resolveFailed(a, 1, chain), a));
    try testing.expectEqual(@as(usize, 1), done.len);
    const fail = done[0].variant.?.fail_workflow_execution.failure.?;
    try testing.expectEqualStrings("order failed", fail.message);
    try testing.expectEqualStrings("child workflow failed", fail.cause.?.message);
    try testing.expectEqualStrings("card declined", failure_mod.rootCause(&fail).message);
}

test "failWith: a workflow canceled as its activity fails is canceled, not failed" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, KeepFailureWf.registered, .{});
    defer inst.destroy();

    _ = try inst.activate(&try initActivation(a, "KeepFailure", .{@as([]const u8, "keep")}), a);
    var act = try resolveFailed(a, 1, try activityFailure(a, "card declined", "Declined"));
    try act.jobs.append(a, .{ .variant = .{ .cancel_workflow = .{} } });
    const cleanup = commandsOf(try inst.activate(&act, a));
    try testing.expectEqual(@as(usize, 1), cleanup.len);
    const done = commandsOf(try inst.activate(&try resolveFailed(a, 2, try activityFailure(a, "cleanup failed", "CleanupFailed")), a));
    try testing.expectEqual(@as(usize, 1), done.len);
    try testing.expect(done[0].variant.? == .cancel_workflow_execution);
}

test "failWith: an errdefer's failed cleanup activity doesn't replace the chosen failure" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();

    const kept = try create(testing.allocator, KeepFailureWf.registered, .{});
    defer kept.destroy();
    try expectChargeFailure(try failureAfterCleanup(a, kept, "errdefer_keep"));

    const plain = try create(testing.allocator, KeepFailureWf.registered, .{});
    defer plain.destroy();
    try testing.expectEqualStrings("cleanup failed", (try failureAfterCleanup(a, plain, "errdefer_plain")).cause.?.message);
}

test "failWith and fail share one slot: the later call is reported" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();

    const with_last = try create(testing.allocator, KeepFailureWf.registered, .{});
    defer with_last.destroy();
    try expectChargeFailure(try failureAfterCleanup(a, with_last, "fail_then_keep"));

    const fail_last = try create(testing.allocator, KeepFailureWf.registered, .{});
    defer fail_last.destroy();
    const f = try failureAfterCleanup(a, fail_last, "keep_then_fail");
    try testing.expectEqualStrings("chosen later", f.message);
    try testing.expectEqualStrings("Later", failure_mod.applicationType(f).?);
}

test "update: failWith in a handler rejects it with that failure after a failed cleanup" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, HandlerFailuresWf.registered, .{});
    defer inst.destroy();

    _ = try inst.activate(&try initActivation(a, "HandlerFailures", .{false}), a);
    const scheduled = commandsOf(try inst.activate(&try oneJob(a, try updateJob(a, "u1", "keep", .{}, true)), a));
    try testing.expectEqual(@as(usize, 2), scheduled.len);
    try testing.expect(scheduled[1].variant.? == .schedule_activity);
    const cleanup = commandsOf(try inst.activate(&try resolveFailed(a, 1, try activityFailure(a, "card declined", "Declined")), a));
    try testing.expectEqual(@as(usize, 1), cleanup.len);
    const cmds = commandsOf(try inst.activate(&try resolveFailed(a, 2, try activityFailure(a, "cleanup failed", "CleanupFailed")), a));
    try testing.expectEqual(@as(usize, 1), cmds.len);
    try expectChargeFailure((try updateResponse(cmds[0], "u1")).rejected);

    // The handler's chosen failure doesn't become the workflow's.
    const done = commandsOf(try inst.activate(&try oneJob(a, try signalJob(a, "done", .{})), a));
    try testing.expectEqualStrings("Finished", done[0].variant.?.fail_workflow_execution.failure.?.message);
}

test "failWith in a signal handler fails the workflow with that failure after a failed cleanup" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, HandlerFailuresWf.registered, .{});
    defer inst.destroy();

    _ = try inst.activate(&try initActivation(a, "HandlerFailures", .{false}), a);
    const scheduled = commandsOf(try inst.activate(&try oneJob(a, try signalJob(a, "keep", .{})), a));
    try testing.expectEqual(@as(usize, 1), scheduled.len);
    _ = try inst.activate(&try resolveFailed(a, 1, try activityFailure(a, "card declined", "Declined")), a);
    const cmds = commandsOf(try inst.activate(&try resolveFailed(a, 2, try activityFailure(a, "cleanup failed", "CleanupFailed")), a));
    try testing.expectEqual(@as(usize, 1), cmds.len);
    try expectChargeFailure(cmds[0].variant.?.fail_workflow_execution.failure.?);
}

// An activity raced against a timer.
const TimerRace = struct {
    const Done = union(enum) { processed: workflow.ActivityError![]const u8, timer: std.Io.Cancelable!void };

    fn run(ctx: *workflow.Context, threshold_s: i64) ![]const u8 {
        const io = ctx.io();
        const opts: workflow.ActivityOptions = .{ .start_to_close_timeout = .fromSeconds(10) };
        const processing = ctx.executeActivity(Greet, opts, .{"order"});
        var buf: [2]Done = undefined;
        var select: std.Io.Select(Done) = .init(io, &buf);
        select.async(.processed, workflow.ActivityFuture([]const u8).get, .{processing});
        select.async(.timer, std.Io.sleep, .{ io, .fromSeconds(threshold_s), .awake });
        switch (try select.await()) {
            .processed => |r| {
                select.cancelDiscard();
                _ = try r;
                return "fast";
            },
            .timer => {
                _ = try ctx.executeActivity(Greet, opts, .{"email"}).get();
                select.cancelDiscard(); // the activity is still pending: request its cancellation too
                return "slow";
            },
        }
    }
};
const TimerRaceWf = workflow.Def("TimerRace", TimerRace.run);

fn resolveOk(a: Allocator, seq: u32, value: []const u8) !act_pb.WorkflowActivation {
    return oneJob(a, .{ .resolve_activity = .{ .seq = seq, .result = .{ .status = .{ .completed = .{ .result = try converter.toPayload(a, value) } } } } });
}

test "an activity that wins the race cancels the timer" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, TimerRaceWf.registered, .{});
    defer inst.destroy();

    const first = commandsOf(try inst.activate(&try initActivation(a, "TimerRace", .{@as(i64, 3)}), a));
    try testing.expectEqual(@as(usize, 2), first.len);
    try testing.expect(first[0].variant.? == .schedule_activity);
    try testing.expectEqual(@as(i64, 3), first[1].variant.?.start_timer.start_to_fire_timeout.?.seconds);

    const second = commandsOf(try inst.activate(&try resolveOk(a, 1, "done"), a));
    try testing.expectEqual(@as(usize, 2), second.len);
    try testing.expectEqual(@as(u32, 1), second[0].variant.?.cancel_timer.seq);
    try testing.expectEqualStrings("\"fast\"", second[1].variant.?.complete_workflow_execution.result.?.data);
}

test "a timer that wins the race runs the fallback activity" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, TimerRaceWf.registered, .{});
    defer inst.destroy();

    _ = try inst.activate(&try initActivation(a, "TimerRace", .{@as(i64, 3)}), a);
    const fired = commandsOf(try inst.activate(&try oneJob(a, .{ .fire_timer = .{ .seq = 1 } }), a));
    try testing.expectEqual(@as(usize, 1), fired.len);
    try testing.expectEqualStrings("\"email\"", fired[0].variant.?.schedule_activity.arguments.items[0].data);

    const done = commandsOf(try inst.activate(&try resolveOk(a, 2, "sent"), a));
    try testing.expectEqual(@as(usize, 2), done.len);
    try testing.expectEqual(@as(u32, 1), done[0].variant.?.request_cancel_activity.seq);
    try testing.expectEqualStrings("\"slow\"", done[1].variant.?.complete_workflow_execution.result.?.data);
}

// ctx.sleep with a summary: alone, as an io.async coroutine that is canceled, and racing an
// activity in a Select.
const SummaryTimers = struct {
    fn nap(ctx: *workflow.Context, summary: []const u8) ![]const u8 {
        try ctx.sleep(.fromSeconds(30), .{ .summary = summary });
        return "rested";
    }

    fn canceledNap(ctx: *workflow.Context) ![]const u8 {
        const io = ctx.io();
        var nap_future = io.async(workflow.Context.sleep, .{ ctx, .fromSeconds(30), .{ .summary = "short nap" } });
        _ = try ctx.executeActivity(Greet, .{ .start_to_close_timeout = .fromSeconds(10) }, .{"order"}).get();
        nap_future.cancel(io) catch |err| return @errorName(err);
        return "fired";
    }

    const Done = union(enum) { processed: workflow.ActivityError![]const u8, timer: std.Io.Cancelable!void };

    fn race(ctx: *workflow.Context) ![]const u8 {
        const io = ctx.io();
        const processing = ctx.executeActivity(Greet, .{ .start_to_close_timeout = .fromSeconds(10) }, .{"order"});
        var buf: [2]Done = undefined;
        var select: std.Io.Select(Done) = .init(io, &buf);
        defer select.cancelDiscard();
        select.async(.processed, workflow.ActivityFuture([]const u8).get, .{processing});
        select.async(.timer, workflow.Context.sleep, .{ ctx, .fromSeconds(3), .{ .summary = "processing deadline" } });
        return switch (try select.await()) {
            .processed => |r| try r,
            .timer => "late",
        };
    }
};
const SummaryNapWf = workflow.Def("SummaryNap", SummaryTimers.nap);
const CanceledNapWf = workflow.Def("CanceledNap", SummaryTimers.canceledNap);
const SummaryRaceWf = workflow.Def("SummaryRace", SummaryTimers.race);

test "ctx.sleep sends its summary as the StartTimer command's user_metadata" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, SummaryNapWf.registered, .{});
    defer inst.destroy();

    const first = commandsOf(try inst.activate(&try initActivation(a, "SummaryNap", .{@as([]const u8, "wait for the cooldown")}), a));
    try testing.expectEqual(@as(usize, 1), first.len);
    const timer = first[0].variant.?.start_timer;
    try testing.expectEqual(@as(i64, 30), timer.start_to_fire_timeout.?.seconds);
    try testing.expectEqualStrings("\"wait for the cooldown\"", first[0].user_metadata.?.summary.?.data);
    try testing.expectEqualStrings("json/plain", first[0].user_metadata.?.summary.?.metadata.items[0].value);
    try testing.expect(first[0].user_metadata.?.details == null);

    const done = commandsOf(try inst.activate(&try oneJob(a, .{ .fire_timer = .{ .seq = timer.seq } }), a));
    try testing.expectEqualStrings("\"rested\"", done[0].variant.?.complete_workflow_execution.result.?.data);
}

test "ctx.sleep with an empty summary sends the StartTimer command io.sleep sends" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, SummaryNapWf.registered, .{});
    defer inst.destroy();

    const first = commandsOf(try inst.activate(&try initActivation(a, "SummaryNap", .{@as([]const u8, "")}), a));
    try testing.expectEqual(@as(usize, 1), first.len);
    try testing.expect(first[0].user_metadata == null);
    try testing.expectEqual(@as(u32, 1), first[0].variant.?.start_timer.seq);
    try testing.expectEqual(@as(i64, 30), first[0].variant.?.start_timer.start_to_fire_timeout.?.seconds);
}

test "canceling a ctx.sleep coroutine emits CancelTimer and delivers error.Canceled" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, CanceledNapWf.registered, .{});
    defer inst.destroy();

    const first = commandsOf(try inst.activate(&try initActivation(a, "CanceledNap", .{}), a));
    try testing.expectEqual(@as(usize, 2), first.len);
    var timer_seq: ?u32 = null;
    for (first) |c| if (c.variant.? == .start_timer) {
        timer_seq = c.variant.?.start_timer.seq;
        try testing.expectEqualStrings("\"short nap\"", c.user_metadata.?.summary.?.data);
    };
    const done = commandsOf(try inst.activate(&try resolveOk(a, 1, "done"), a));
    try testing.expectEqual(@as(usize, 2), done.len);
    try testing.expectEqual(timer_seq.?, done[0].variant.?.cancel_timer.seq);
    try testing.expectEqualStrings("\"Canceled\"", done[1].variant.?.complete_workflow_execution.result.?.data);
}

test "an activity that wins the race against a ctx.sleep with a summary cancels the timer" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, SummaryRaceWf.registered, .{});
    defer inst.destroy();

    const first = commandsOf(try inst.activate(&try initActivation(a, "SummaryRace", .{}), a));
    try testing.expectEqual(@as(usize, 2), first.len);
    try testing.expect(first[0].variant.? == .schedule_activity);
    try testing.expect(first[0].user_metadata == null);
    try testing.expectEqual(@as(i64, 3), first[1].variant.?.start_timer.start_to_fire_timeout.?.seconds);
    try testing.expectEqualStrings("\"processing deadline\"", first[1].user_metadata.?.summary.?.data);

    const done = commandsOf(try inst.activate(&try resolveOk(a, 1, "done"), a));
    try testing.expectEqual(@as(usize, 2), done.len);
    try testing.expectEqual(@as(u32, 1), done[0].variant.?.cancel_timer.seq);
    try testing.expectEqualStrings("\"done\"", done[1].variant.?.complete_workflow_execution.result.?.data);
}

test "a ctx.sleep with a summary that wins the race cancels the activity" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, SummaryRaceWf.registered, .{});
    defer inst.destroy();

    const first = commandsOf(try inst.activate(&try initActivation(a, "SummaryRace", .{}), a));
    try testing.expectEqual(@as(u32, 1), first[1].variant.?.start_timer.seq);
    const done = commandsOf(try inst.activate(&try oneJob(a, .{ .fire_timer = .{ .seq = 1 } }), a));
    try testing.expectEqual(@as(usize, 2), done.len);
    try testing.expectEqual(@as(u32, 1), done[0].variant.?.request_cancel_activity.seq);
    try testing.expectEqualStrings("\"late\"", done[1].variant.?.complete_workflow_execution.result.?.data);
}

// Coroutines appending results, and the
// workflow awaiting a condition over them.
const Goroutines = struct {
    const State = struct { results: std.ArrayList([]const u8) = .empty, err: ?anyerror = null, want: usize };

    fn branch(ctx: *workflow.Context, state: *State, input: []const u8) void {
        const opts: workflow.ActivityOptions = .{ .start_to_close_timeout = .fromSeconds(10) };
        const r = ctx.executeActivity(Greet, opts, .{input}).get() catch |err| {
            state.err = err;
            return;
        };
        state.results.append(ctx.allocator(), r) catch |err| {
            state.err = err;
        };
    }

    fn done(state: *const State) bool {
        return state.err != null or state.results.items.len == state.want;
    }

    fn run(ctx: *workflow.Context, n: usize) ![]const []const u8 {
        var state: State = .{ .want = n };
        var group: std.Io.Group = .init;
        defer group.cancel(ctx.io());
        for (0..n) |i| {
            const input = try std.fmt.allocPrint(ctx.allocator(), "{d}", .{i});
            group.async(ctx.io(), branch, .{ ctx, &state, input });
        }
        try ctx.await(&state, done);
        if (state.err) |err| return err;
        return state.results.items;
    }

    fn timesOut(ctx: *workflow.Context) !bool {
        var never = false;
        return ctx.awaitTimeout(.fromSeconds(5), &never, struct {
            fn f(v: *bool) bool {
                return v.*;
            }
        }.f);
    }
};
const GoroutinesWf = workflow.Def("Goroutines", Goroutines.run);
const AwaitTimeoutWf = workflow.Def("AwaitTimeout", Goroutines.timesOut);

test "await wakes when coroutines make the condition true" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, GoroutinesWf.registered, .{});
    defer inst.destroy();

    const first = commandsOf(try inst.activate(&try initActivation(a, "Goroutines", .{@as(usize, 3)}), a));
    try testing.expectEqual(@as(usize, 3), first.len);
    for (first, 0..) |c, i| try testing.expectEqual(@as(u32, @intCast(i + 1)), c.variant.?.schedule_activity.seq);

    // Resolve out of order; the result order follows completion order.
    try testing.expectEqual(@as(usize, 0), commandsOf(try inst.activate(&try resolveOk(a, 3, "c"), a)).len);
    try testing.expectEqual(@as(usize, 0), commandsOf(try inst.activate(&try resolveOk(a, 1, "a"), a)).len);
    const done = commandsOf(try inst.activate(&try resolveOk(a, 2, "b"), a));
    try testing.expectEqual(@as(usize, 1), done.len);
    try testing.expectEqualStrings("[\"c\",\"a\",\"b\"]", done[0].variant.?.complete_workflow_execution.result.?.data);
}

test "awaitTimeout returns false when its timer fires first" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, AwaitTimeoutWf.registered, .{});
    defer inst.destroy();

    const first = commandsOf(try inst.activate(&try initActivation(a, "AwaitTimeout", .{}), a));
    try testing.expectEqual(@as(i64, 5), first[0].variant.?.start_timer.start_to_fire_timeout.?.seconds);
    const done = commandsOf(try inst.activate(&try oneJob(a, .{ .fire_timer = .{ .seq = 1 } }), a));
    try testing.expectEqualStrings("false", done[0].variant.?.complete_workflow_execution.result.?.data);
}

test "eviction while awaiting a condition frees everything" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, GoroutinesWf.registered, .{});
    _ = try inst.activate(&try initActivation(a, "Goroutines", .{@as(usize, 2)}), a);
    inst.destroy();
}

/// Sleeps, then starts a coroutine that sleeps again, through io.async (mode 0), Group.async
/// (1) or a Select case (2), and waits for it.
const CoroutineStart = struct {
    const Picked = union(enum) { nap: std.Io.Cancelable!void };

    fn run(ctx: *workflow.Context, mode: u8) ![]const u8 {
        const io = ctx.io();
        try io.sleep(.fromSeconds(10), .awake);
        switch (mode) {
            0 => {
                var f = io.async(std.Io.sleep, .{ io, .fromSeconds(5), .awake });
                try f.await(io);
            },
            1 => {
                var g: std.Io.Group = .init;
                g.async(io, std.Io.sleep, .{ io, .fromSeconds(5), .awake });
                try g.await(io);
            },
            else => {
                var buf: [1]Picked = undefined;
                var select: std.Io.Select(Picked) = .init(io, &buf);
                defer select.cancelDiscard();
                select.async(.nap, std.Io.sleep, .{ io, .fromSeconds(5), .awake });
                switch (try select.await()) {
                    .nap => |r| try r,
                }
            },
        }
        return "done";
    }
};
const CoroutineStartWf = workflow.Def("CoroutineStart", CoroutineStart.run);

const coroutine_start_operations = [_][]const u8{ "io.async", "Group.async", "Group.async" };

fn expectCoroutineStartFailed(c: done_pb.WorkflowActivationCompletion, mode: u8, cause: WorkflowIo.StartFailure.Cause) !void {
    const f = c.status.?.failed.failure.?;
    try testing.expectEqualStrings("WorkflowCoroutineStartFailed", failure_mod.applicationType(f).?);
    var buf: [512]u8 = undefined;
    const want = try std.fmt.bufPrint(&buf, "{f}", .{WorkflowIo.StartFailure{ .operation = coroutine_start_operations[mode], .cause = cause }});
    try testing.expect(std.mem.startsWith(u8, f.message, want));
    try testing.expect(std.mem.endsWith(u8, f.message, "; the workflow task fails, and the run is evicted and replayed from history on a later task"));
}

test "a coroutine that can't start fails the workflow task, and replay without the fault completes" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();

    for (0..3) |m| for ([_]WorkflowIo.StartFailure.Cause{ .task_block, .stack }) |cause| {
        const mode: u8 = @intCast(m);
        const init = try initActivation(a, "CoroutineStart", .{mode});
        const fire_first = try oneJob(a, .{ .fire_timer = .{ .seq = 1 } });
        const fire_second = try oneJob(a, .{ .fire_timer = .{ .seq = 2 } });

        {
            var recorder: LogRecorder = .{ .arena = .init(testing.allocator), .min = .warn };
            defer recorder.arena.deinit();
            const inst = try create(testing.allocator, CoroutineStartWf.registered, .{ .logger = recorder.sink() });
            defer inst.destroy();
            const first = commandsOf(try inst.activate(&init, a));
            try testing.expectEqual(@as(u32, 1), first[0].variant.?.start_timer.seq);

            // The second activation's start is the run's first block of its size class, so
            // the pool can't serve it.
            var failing: std.testing.FailingAllocator = .init(inst.arena(), .{ .fail_index = 0 });
            switch (cause) {
                .task_block => inst.wio.memory = failing.allocator(),
                .stack => inst.sched.fail_stack_maps = 1,
            }
            const failed = try inst.activate(&fire_first, a);
            inst.wio.memory = inst.arena();
            try testing.expectEqual(@as(u32, 0), inst.sched.fail_stack_maps);
            try expectCoroutineStartFailed(failed, mode, cause);
            try testing.expectEqual(@as(usize, 1), recorder.count);
            try testing.expectEqualStrings(failed.status.?.failed.failure.?.message, recorder.message);
            try testing.expectEqualStrings(coroutine_start_operations[mode], recorder.get("operation").?);
            try testing.expectEqualStrings(@tagName(cause), recorder.get("cause").?);

            // Sticky until the run is evicted, whatever the activation holds.
            try expectCoroutineStartFailed(try inst.activate(&fire_second, a), mode, cause);
            try testing.expectEqual(@as(usize, 1), recorder.count);
            // Eviction ends every coroutine, the inline run's caller included.
            try testing.expectEqual(@as(usize, 0), inst.teardown());
        }

        const inst = try create(testing.allocator, CoroutineStartWf.registered, .{});
        defer inst.destroy();
        _ = try inst.activate(&init, a);
        const second = commandsOf(try inst.activate(&fire_first, a));
        try testing.expectEqual(@as(usize, 1), second.len);
        try testing.expectEqual(@as(u32, 2), second[0].variant.?.start_timer.seq);
        try testing.expectEqual(@as(i64, 5), second[0].variant.?.start_timer.start_to_fire_timeout.?.seconds);
        const done = commandsOf(try inst.activate(&fire_second, a));
        try testing.expectEqualStrings("\"done\"", done[0].variant.?.complete_workflow_execution.result.?.data);
        try testing.expectEqual(@as(?WorkflowIo.StartFailure, null), inst.wio.start_failure);
    };
}

/// Races a case that is ready at once against a timeout, with room in the select's buffer for
/// one result (the usual first-wins race). With `fail_timeout_start`, the timeout's coroutine
/// can't get a stack, so its sleep runs inline in the workflow coroutine.
const RaceReadyAgainstTimeout = struct {
    const U = union(enum) { ready: u32, timeout: std.Io.Cancelable!void };
    var fail_timeout_start = false;

    fn ready() u32 {
        return 7;
    }

    fn run(ctx: *workflow.Context) ![]const u8 {
        const io = ctx.io();
        try io.sleep(.fromSeconds(10), .awake);
        var buf: [1]U = undefined;
        var sel: std.Io.Select(U) = .init(io, &buf);
        defer sel.cancelDiscard();
        sel.async(.ready, ready, .{});
        if (fail_timeout_start) ctx.instance.sched.fail_stack_maps = 1;
        sel.async(.timeout, std.Io.sleep, .{ io, .fromSeconds(60), .awake });
        return switch (try sel.await()) {
            .ready => "ready",
            .timeout => "timeout",
        };
    }
};
const RaceReadyAgainstTimeoutWf = workflow.Def("RaceReadyAgainstTimeout", RaceReadyAgainstTimeout.run);

test "eviction after a Select case failed to start frees every coroutine" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const init = try initActivation(a, "RaceReadyAgainstTimeout", .{});
    const fire_first = try oneJob(a, .{ .fire_timer = .{ .seq = 1 } });

    // Undisturbed, the ready case wins and the run completes.
    {
        const inst = try create(testing.allocator, RaceReadyAgainstTimeoutWf.registered, .{});
        defer inst.destroy();
        _ = try inst.activate(&init, a);
        const done = commandsOf(try inst.activate(&fire_first, a));
        try testing.expectEqualStrings("\"ready\"", done[done.len - 1].variant.?.complete_workflow_execution.result.?.data);
        try testing.expectEqual(@as(usize, 0), inst.teardown());
    }

    RaceReadyAgainstTimeout.fail_timeout_start = true;
    defer RaceReadyAgainstTimeout.fail_timeout_start = false;
    const inst = try create(testing.allocator, RaceReadyAgainstTimeoutWf.registered, .{});
    defer inst.destroy();
    _ = try inst.activate(&init, a);
    const failed = try inst.activate(&fire_first, a);
    try testing.expectEqualStrings("WorkflowCoroutineStartFailed", failure_mod.applicationType(failed.status.?.failed.failure.?).?);
    // The core evicts the failed run. Every coroutine must exit, or its stack stays mapped:
    // the resource whose exhaustion failed the start in the first place.
    try testing.expectEqual(@as(usize, 0), inst.teardown());
}

// The workflow function and a coroutine it starts with io.async name themselves; the child
// reads its name back after a durable timer.
const CoroutineNames = struct {
    fn child(ctx: *workflow.Context, seen: *[2][]const u8) !void {
        seen[0] = ctx.coroutineName();
        var name = "fetcher".*;
        try ctx.setCoroutineName(&name);
        name[0] = 'X'; // setCoroutineName copied it
        try ctx.io().sleep(.fromSeconds(1), .awake);
        seen[1] = ctx.coroutineName();
    }

    fn run(ctx: *workflow.Context) ![]const u8 {
        const before = ctx.coroutineName();
        try ctx.setCoroutineName("main");
        var seen: [2][]const u8 = undefined;
        var f = ctx.io().async(child, .{ ctx, &seen });
        try f.await(ctx.io());
        return std.mem.join(ctx.allocator(), ",", &.{ before, ctx.coroutineName(), seen[0], seen[1] });
    }
};
const CoroutineNamesWf = workflow.Def("CoroutineNames", CoroutineNames.run);

test "setCoroutineName names the calling coroutine, including one started with io.async" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, CoroutineNamesWf.registered, .{});
    defer inst.destroy();

    const first = commandsOf(try inst.activate(&try initActivation(a, "CoroutineNames", .{}), a));
    try testing.expectEqual(@as(usize, 1), first.len);
    try testing.expectEqual(@as(u32, 1), first[0].variant.?.start_timer.seq);
    const done = commandsOf(try inst.activate(&try oneJob(a, .{ .fire_timer = .{ .seq = 1 } }), a));
    try testing.expectEqualStrings("\"workflow,main,async,fetcher\"", done[0].variant.?.complete_workflow_execution.result.?.data);
}

// Names itself, sleeps, and returns its name. Its query handler tries to rename the caller,
// which outside a coroutine names nothing.
const QueryRename = struct {
    const Name = workflow.QueryDef("name", fn () []const u8);

    fn run(ctx: *workflow.Context) ![]const u8 {
        try ctx.setCoroutineName("main");
        try ctx.setQueryHandler(Name, ctx, name);
        try ctx.io().sleep(.fromSeconds(1), .awake);
        return ctx.coroutineName();
    }

    fn name(ctx: *workflow.Context) []const u8 {
        ctx.setCoroutineName("from-query") catch return "OutOfMemory";
        return ctx.coroutineName();
    }
};
const QueryRenameWf = workflow.Def("QueryRename", QueryRename.run);

test "setCoroutineName in a query handler names nothing and allocates nothing" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, QueryRenameWf.registered, .{});
    defer inst.destroy();

    const first = commandsOf(try inst.activate(&try initActivation(a, "QueryRename", .{}), a));
    try testing.expectEqual(@as(u32, 1), first[0].variant.?.start_timer.seq);
    for (0..3) |_| {
        const allocated = inst.allocated;
        const c = try inst.activate(&try queryActivation(a, &.{.{ "q1", "name", &.{} }}), a);
        try testing.expectEqualStrings("\"\"", answer(c, 0).variant.?.succeeded.response.?.data);
        try testing.expectEqual(allocated, inst.allocated);
    }
    const done = commandsOf(try inst.activate(&try oneJob(a, .{ .fire_timer = .{ .seq = 1 } }), a));
    try testing.expectEqualStrings("\"main\"", done[0].variant.?.complete_workflow_execution.result.?.data);
}

// Waits on an activity with the cancellation type named by `mode`. If the activity completes
// despite a cancel, sleeps before returning, which a re-armed cancel interrupts.
const WaitCancel = struct {
    fn run(ctx: *workflow.Context, mode: []const u8) ![]const u8 {
        const cancellation_type = std.meta.stringToEnum(ActivityState.CancellationType, mode) orelse return error.InvalidPayload;
        const opts: workflow.ActivityOptions = .{ .start_to_close_timeout = .fromSeconds(10), .cancellation_type = cancellation_type };
        const r = try ctx.executeActivity(Greet, opts, .{"x"}).get();
        try ctx.io().sleep(.fromSeconds(1), .awake);
        return r;
    }
};
const WaitCancelWf = workflow.Def("WaitCancel", WaitCancel.run);

fn resolveCancelled(a: Allocator, seq: u32) !act_pb.WorkflowActivation {
    const f = try failure_mod.application(a, .{ .message = "activity canceled" });
    return oneJob(a, .{ .resolve_activity = .{ .seq = seq, .result = .{ .status = .{ .cancelled = .{ .failure = f } } } } });
}

test "wait_cancellation_completed: a canceled get waits for the activity to resolve" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, WaitCancelWf.registered, .{});
    defer inst.destroy();

    const first = commandsOf(try inst.activate(&try initActivation(a, "WaitCancel", .{@as([]const u8, "wait_cancellation_completed")}), a));
    try testing.expect(first[0].variant.?.schedule_activity.cancellation_type == .WAIT_CANCELLATION_COMPLETED);
    const canceled = commandsOf(try inst.activate(&try oneJob(a, .{ .cancel_workflow = .{} }), a));
    try testing.expectEqual(@as(usize, 1), canceled.len);
    try testing.expectEqual(@as(u32, 1), canceled[0].variant.?.request_cancel_activity.seq);
    try testing.expect(!inst.finished);

    const resolved = commandsOf(try inst.activate(&try resolveCancelled(a, 1), a));
    try testing.expectEqual(@as(usize, 1), resolved.len);
    try testing.expect(resolved[0].variant.? == .cancel_workflow_execution);
}

test "wait_cancellation_completed: an activity that completes despite the cancel returns its result" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, WaitCancelWf.registered, .{});
    defer inst.destroy();

    _ = try inst.activate(&try initActivation(a, "WaitCancel", .{@as([]const u8, "wait_cancellation_completed")}), a);
    try testing.expectEqual(@as(usize, 1), commandsOf(try inst.activate(&try oneJob(a, .{ .cancel_workflow = .{} }), a)).len);
    // get returns the result; the re-armed cancel then ends the sleep, and the workflow.
    const done = commandsOf(try inst.activate(&try resolveOk(a, 1, "finished anyway"), a));
    try testing.expect(done[done.len - 1].variant.? == .cancel_workflow_execution);
    for (done) |c| try testing.expect(c.variant.? != .complete_workflow_execution);
}

test "try_cancel and abandon: a canceled get returns at once" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    for ([_][]const u8{ "try_cancel", "abandon" }) |mode| {
        const inst = try create(testing.allocator, WaitCancelWf.registered, .{});
        defer inst.destroy();
        _ = try inst.activate(&try initActivation(a, "WaitCancel", .{mode}), a);
        const cmds = commandsOf(try inst.activate(&try oneJob(a, .{ .cancel_workflow = .{} }), a));
        try testing.expectEqual(@as(usize, 2), cmds.len);
        try testing.expectEqual(@as(u32, 1), cmds[0].variant.?.request_cancel_activity.seq);
        try testing.expect(cmds[1].variant.? == .cancel_workflow_execution);
        // The core's cancelled resolution that follows changes nothing.
        try testing.expectEqual(@as(usize, 0), commandsOf(try inst.activate(&try resolveCancelled(a, 1), a)).len);
    }
}

test "eviction ends a wait for an activity's cancellation to complete" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const mode: []const u8 = "wait_cancellation_completed";

    // Evicted while waiting for the cancellation to complete.
    const waiting = try create(testing.allocator, WaitCancelWf.registered, .{});
    _ = try waiting.activate(&try initActivation(a, "WaitCancel", .{mode}), a);
    _ = try waiting.activate(&try oneJob(a, .{ .cancel_workflow = .{} }), a);
    waiting.destroy();

    // Evicted before any cancel: eviction's own cancel must not wait for the activity.
    const running = try create(testing.allocator, WaitCancelWf.registered, .{});
    _ = try running.activate(&try initActivation(a, "WaitCancel", .{mode}), a);
    running.destroy();
}

// Runs `Greet` as a local activity with the cancellation type named by `mode`; "no_timeouts"
// leaves out both close timeouts.
const LocalGreet = struct {
    fn run(ctx: *workflow.Context, mode: []const u8) ![]const u8 {
        var opts: workflow.LocalActivityOptions = .{
            .start_to_close_timeout = .fromSeconds(5),
            .retry_policy = .{ .initial_interval = .fromSeconds(10), .maximum_attempts = 5 },
            .local_retry_threshold = .fromSeconds(2),
        };
        if (std.mem.eql(u8, mode, "no_timeouts")) {
            opts.start_to_close_timeout = null;
        } else {
            opts.cancellation_type = std.meta.stringToEnum(ActivityState.CancellationType, mode) orelse return error.InvalidPayload;
        }
        return ctx.executeLocalActivity(Greet, opts, .{"local"}).get();
    }
};
const LocalGreetWf = workflow.Def("LocalGreet", LocalGreet.run);

fn resolveLocal(a: Allocator, seq: u32, status: result_pb.ActivityResolution.status_union) !act_pb.WorkflowActivation {
    return oneJob(a, .{ .resolve_activity = .{ .seq = seq, .is_local = true, .result = .{ .status = status } } });
}

fn localBackoff(a: Allocator, seq: u32) !act_pb.WorkflowActivation {
    return resolveLocal(a, seq, .{ .backoff = .{
        .attempt = 3,
        .backoff_duration = .{ .seconds = 10 },
        .original_schedule_time = .{ .seconds = 1_700_000_000 },
    } });
}

test "local activity: ScheduleLocalActivity, then the result of its local resolution" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, LocalGreetWf.registered, .{});
    defer inst.destroy();

    const first = commandsOf(try inst.activate(&try initActivation(a, "LocalGreet", .{@as([]const u8, "try_cancel")}), a));
    try testing.expectEqual(@as(usize, 1), first.len);
    const sched = first[0].variant.?.schedule_local_activity;
    try testing.expectEqual(@as(u32, 1), sched.seq);
    try testing.expectEqualStrings("1", sched.activity_id);
    try testing.expectEqualStrings("Activity", sched.activity_type);
    try testing.expectEqualStrings("\"local\"", sched.arguments.items[0].data);
    try testing.expectEqual(@as(i64, 5), sched.start_to_close_timeout.?.seconds);
    try testing.expect(sched.schedule_to_close_timeout == null);
    try testing.expectEqual(@as(i64, 2), sched.local_retry_threshold.?.seconds);
    try testing.expectEqual(@as(i64, 10), sched.retry_policy.?.initial_interval.?.seconds);
    try testing.expectEqual(@as(u32, 0), sched.attempt);
    try testing.expect(sched.cancellation_type == .TRY_CANCEL);

    const done = commandsOf(try inst.activate(&try resolveLocal(a, 1, .{ .completed = .{ .result = try converter.toPayload(a, @as([]const u8, "Hello local!")) } }), a));
    try testing.expectEqualStrings("\"Hello local!\"", done[0].variant.?.complete_workflow_execution.result.?.data);
}

test "local activity backoff: a timer, then the next attempt with a new seq" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, LocalGreetWf.registered, .{});
    defer inst.destroy();
    _ = try inst.activate(&try initActivation(a, "LocalGreet", .{@as([]const u8, "try_cancel")}), a);

    const backoff = commandsOf(try inst.activate(&try localBackoff(a, 1), a));
    try testing.expectEqual(@as(usize, 1), backoff.len);
    const timer = backoff[0].variant.?.start_timer;
    try testing.expectEqual(@as(i64, 10), timer.start_to_fire_timeout.?.seconds);

    const retry = commandsOf(try inst.activate(&try oneJob(a, .{ .fire_timer = .{ .seq = timer.seq } }), a));
    try testing.expectEqual(@as(usize, 1), retry.len);
    const sched = retry[0].variant.?.schedule_local_activity;
    try testing.expectEqual(@as(u32, 2), sched.seq);
    try testing.expectEqualStrings("2", sched.activity_id);
    try testing.expectEqual(@as(u32, 3), sched.attempt);
    try testing.expectEqual(@as(i64, 1_700_000_000), sched.original_schedule_time.?.seconds);
    try testing.expectEqualStrings("\"local\"", sched.arguments.items[0].data);

    // A resolution for the first attempt's seq is stale.
    try testing.expectEqual(@as(usize, 0), commandsOf(try inst.activate(&try resolveLocal(a, 1, .{ .completed = .{} }), a)).len);
    const done = commandsOf(try inst.activate(&try resolveLocal(a, 2, .{ .completed = .{ .result = try converter.toPayload(a, @as([]const u8, "third time")) } }), a));
    try testing.expectEqualStrings("\"third time\"", done[0].variant.?.complete_workflow_execution.result.?.data);
}

test "local activity cancel: RequestCancelLocalActivity while it runs" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();

    const try_cancel = try create(testing.allocator, LocalGreetWf.registered, .{});
    defer try_cancel.destroy();
    _ = try try_cancel.activate(&try initActivation(a, "LocalGreet", .{@as([]const u8, "try_cancel")}), a);
    const cmds = commandsOf(try try_cancel.activate(&try oneJob(a, .{ .cancel_workflow = .{} }), a));
    try testing.expectEqual(@as(usize, 2), cmds.len);
    try testing.expectEqual(@as(u32, 1), cmds[0].variant.?.request_cancel_local_activity.seq);
    try testing.expect(cmds[1].variant.? == .cancel_workflow_execution);
    const f = try failure_mod.application(a, .{ .message = "Local Activity cancelled" });
    try testing.expectEqual(@as(usize, 0), commandsOf(try try_cancel.activate(&try resolveLocal(a, 1, .{ .cancelled = .{ .failure = f } }), a)).len);

    const waiting = try create(testing.allocator, LocalGreetWf.registered, .{});
    defer waiting.destroy();
    _ = try waiting.activate(&try initActivation(a, "LocalGreet", .{@as([]const u8, "wait_cancellation_completed")}), a);
    const requested = commandsOf(try waiting.activate(&try oneJob(a, .{ .cancel_workflow = .{} }), a));
    try testing.expectEqual(@as(usize, 1), requested.len);
    try testing.expectEqual(@as(u32, 1), requested[0].variant.?.request_cancel_local_activity.seq);
    const ended = commandsOf(try waiting.activate(&try resolveLocal(a, 1, .{ .cancelled = .{ .failure = f } }), a));
    try testing.expect(ended[0].variant.? == .cancel_workflow_execution);
}

test "local activity cancel during backoff cancels the timer and resolves at once" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    for ([_][]const u8{ "try_cancel", "wait_cancellation_completed", "abandon" }) |mode| {
        const inst = try create(testing.allocator, LocalGreetWf.registered, .{});
        defer inst.destroy();
        _ = try inst.activate(&try initActivation(a, "LocalGreet", .{mode}), a);
        _ = try inst.activate(&try localBackoff(a, 1), a);
        const cmds = commandsOf(try inst.activate(&try oneJob(a, .{ .cancel_workflow = .{} }), a));
        try testing.expectEqual(@as(usize, 2), cmds.len);
        try testing.expectEqual(@as(u32, 1), cmds[0].variant.?.cancel_timer.seq);
        try testing.expect(cmds[1].variant.? == .cancel_workflow_execution);
    }
}

test "local activity without close timeouts: get returns error.InvalidOptions" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, LocalGreetWf.registered, .{});
    defer inst.destroy();
    const cmds = commandsOf(try inst.activate(&try initActivation(a, "LocalGreet", .{@as([]const u8, "no_timeouts")}), a));
    try testing.expectEqual(@as(usize, 1), cmds.len);
    try testing.expectEqualStrings("InvalidOptions", cmds[0].variant.?.fail_workflow_execution.failure.?.message);
}

// Each mode passes options that the session API rejects before scheduling anything.
const SessionOptionsCheck = struct {
    const valid: workflow.SessionOptions = .{ .creation_timeout = .fromSeconds(10), .execution_timeout = .fromSeconds(60) };

    fn run(ctx: *workflow.Context, mode: []const u8) ![]const u8 {
        var opts = valid;
        if (std.mem.eql(u8, mode, "session_and_task_queue")) {
            // Rejected before the session's state is read, so its creation activity is unused.
            const session: workflow.Session = .{ .id = "s1", .heartbeat_timeout = .fromSeconds(20), .creation = undefined, .opened = true };
            return ctx.executeActivity(Greet, .{ .start_to_close_timeout = .fromSeconds(5), .task_queue = "elsewhere", .session = &session }, .{"x"}).get();
        } else if (std.mem.eql(u8, mode, "empty_recreate_queue")) {
            _ = try ctx.recreateSession("", valid);
        } else if (std.mem.eql(u8, mode, "zero_creation_timeout")) {
            opts.creation_timeout = .zero;
        } else if (std.mem.eql(u8, mode, "negative_execution_timeout")) {
            opts.execution_timeout = .fromSeconds(-1);
        } else if (std.mem.eql(u8, mode, "zero_heartbeat_timeout")) {
            opts.heartbeat_timeout = .zero;
        } else return error.InvalidPayload;
        _ = try ctx.createSession(opts);
        return "created";
    }
};
const SessionOptionsCheckWf = workflow.Def("SessionOptionsCheck", SessionOptionsCheck.run);

test "sessions: invalid options fail with error.InvalidOptions and schedule nothing" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    for ([_][]const u8{ "session_and_task_queue", "empty_recreate_queue", "zero_creation_timeout", "negative_execution_timeout", "zero_heartbeat_timeout" }) |mode| {
        const inst = try create(testing.allocator, SessionOptionsCheckWf.registered, .{});
        defer inst.destroy();
        const cmds = commandsOf(try inst.activate(&try initActivation(a, "SessionOptionsCheck", .{mode}), a));
        try testing.expectEqual(@as(usize, 1), cmds.len);
        try testing.expectEqualStrings("InvalidOptions", cmds[0].variant.?.fail_workflow_execution.failure.?.message);
    }
}

const ActivityMeta = struct {
    const charge: workflow.ActivityOptions = .{
        .start_to_close_timeout = .fromSeconds(10),
        .summary = "charge the card",
        .priority = .{ .priority_key = 1, .fairness_key = "tenant-a", .fairness_weight = 1.5 },
        .versioning_intent = .default,
    };

    fn run(ctx: *workflow.Context, mode: []const u8) ![]const u8 {
        if (std.mem.eql(u8, mode, "all")) return ctx.executeActivity(Greet, charge, .{"x"}).get();
        if (std.mem.eql(u8, mode, "compatible")) return ctx.executeActivity(Greet, .{ .start_to_close_timeout = .fromSeconds(10), .versioning_intent = .compatible }, .{"x"}).get();
        if (std.mem.eql(u8, mode, "defaults")) return ctx.executeActivity(Greet, .{ .start_to_close_timeout = .fromSeconds(10) }, .{"x"}).get();
        if (std.mem.eql(u8, mode, "session")) {
            var creation: ActivityState = .{ .seq = 0 };
            const session: workflow.Session = .{ .id = "s1", .task_queue = "host-q", .heartbeat_timeout = .fromSeconds(20), .creation = &creation, .opened = true };
            var opts = charge;
            opts.session = &session;
            return ctx.executeActivity(Greet, opts, .{"x"}).get();
        }
        if (std.mem.eql(u8, mode, "local")) return ctx.executeLocalActivity(Greet, .{
            .start_to_close_timeout = .fromSeconds(5),
            .local_retry_threshold = .fromSeconds(2),
            .summary = "look up the rate",
        }, .{"x"}).get();
        if (std.mem.eql(u8, mode, "side_effect")) {
            const value = try ctx.sideEffect(u32, {}, fortyTwo);
            return if (value == 42) "42" else error.InvalidPayload;
        }
        return error.InvalidPayload;
    }

    fn fortyTwo(_: void) u32 {
        return 42;
    }

    /// Sets a summary on every activity before scheduling it.
    const Summarize = struct {
        pub fn interceptWorkflow(s: *Summarize, _: *workflow.Context) error{OutOfMemory}!interceptor.Workflow {
            return .{ .outbound = interceptor.WorkflowOutbound.from(Summarize, s) };
        }

        pub fn executeActivity(_: *Summarize, input: interceptor.ActivityInput, next: interceptor.WorkflowOutbound.Next) workflow.ActivityFuture(converter.Payload) {
            var changed = input;
            changed.options.summary = "set by an interceptor";
            return next.executeActivity(changed);
        }
    };
};
const ActivityMetaWf = workflow.Def("ActivityMeta", ActivityMeta.run);

/// The one command `inst`'s first activation sends; it lives as long as `inst`.
fn firstCommand(inst: *Instance, a: Allocator, mode: []const u8) !cmd_pb.WorkflowCommand {
    const cmds = commandsOf(try inst.activate(&try initActivation(a, "ActivityMeta", .{mode}), a));
    try testing.expectEqual(@as(usize, 1), cmds.len);
    return cmds[0];
}

test "activity options: summary, priority and versioning intent reach the ScheduleActivity command" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();

    const all_inst = try create(testing.allocator, ActivityMetaWf.registered, .{});
    defer all_inst.destroy();
    const all = try firstCommand(all_inst, a, "all");
    try testing.expectEqualStrings("\"charge the card\"", all.user_metadata.?.summary.?.data);
    try testing.expect(all.user_metadata.?.details == null);
    const sched = all.variant.?.schedule_activity;
    try testing.expectEqual(@as(i32, 1), sched.priority.?.priority_key);
    try testing.expectEqualStrings("tenant-a", sched.priority.?.fairness_key);
    try testing.expectEqual(@as(f32, 1.5), sched.priority.?.fairness_weight);
    try testing.expect(sched.versioning_intent == .DEFAULT);

    const compatible_inst = try create(testing.allocator, ActivityMetaWf.registered, .{});
    defer compatible_inst.destroy();
    const compatible = try firstCommand(compatible_inst, a, "compatible");
    try testing.expect(compatible.variant.?.schedule_activity.versioning_intent == .COMPATIBLE);

    const defaults_inst = try create(testing.allocator, ActivityMetaWf.registered, .{});
    defer defaults_inst.destroy();
    const defaults = try firstCommand(defaults_inst, a, "defaults");
    try testing.expect(defaults.user_metadata == null);
    try testing.expect(defaults.variant.?.schedule_activity.priority == null);
    try testing.expect(defaults.variant.?.schedule_activity.versioning_intent == .UNSPECIFIED);
}

test "activity options: a session activity keeps them on the host's task queue" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, ActivityMetaWf.registered, .{});
    defer inst.destroy();
    const cmd = try firstCommand(inst, a, "session");
    const sched = cmd.variant.?.schedule_activity;
    try testing.expectEqualStrings("host-q", sched.task_queue);
    try testing.expectEqualStrings("\"charge the card\"", cmd.user_metadata.?.summary.?.data);
    try testing.expectEqualStrings("tenant-a", sched.priority.?.fairness_key);
    try testing.expect(sched.versioning_intent == .DEFAULT);
}

test "activity options: an outbound interceptor that sets a summary changes the command" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    var impl: ActivityMeta.Summarize = .{};
    const interceptors = [_]interceptor.Worker{.from(ActivityMeta.Summarize, &impl)};
    const inst = try create(testing.allocator, ActivityMetaWf.registered, .{ .interceptors = &interceptors });
    defer inst.destroy();
    const cmd = try firstCommand(inst, a, "defaults");
    try testing.expectEqualStrings("\"set by an interceptor\"", cmd.user_metadata.?.summary.?.data);
}

test "local activity summary: sent with the first attempt and with the one after a backoff timer" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, ActivityMetaWf.registered, .{});
    defer inst.destroy();

    const first = commandsOf(try inst.activate(&try initActivation(a, "ActivityMeta", .{@as([]const u8, "local")}), a));
    try testing.expectEqual(@as(usize, 1), first.len);
    try testing.expectEqualStrings("\"look up the rate\"", first[0].user_metadata.?.summary.?.data);
    _ = first[0].variant.?.schedule_local_activity;

    const backoff = commandsOf(try inst.activate(&try localBackoff(a, 1), a));
    try testing.expect(backoff[0].user_metadata == null);
    const retry = commandsOf(try inst.activate(&try oneJob(a, .{ .fire_timer = .{ .seq = backoff[0].variant.?.start_timer.seq } }), a));
    try testing.expectEqual(@as(usize, 1), retry.len);
    try testing.expectEqual(@as(u32, 2), retry[0].variant.?.schedule_local_activity.seq);
    try testing.expectEqualStrings("\"look up the rate\"", retry[0].user_metadata.?.summary.?.data);
}

test "side effects send no summary" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, ActivityMetaWf.registered, .{});
    defer inst.destroy();
    const cmd = try firstCommand(inst, a, "side_effect");
    try testing.expectEqualStrings(side_effect_activity_type, cmd.variant.?.schedule_local_activity.activity_type);
    try testing.expect(cmd.user_metadata == null);
}

test "eviction while a local activity backs off frees everything" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, LocalGreetWf.registered, .{});
    _ = try inst.activate(&try initActivation(a, "LocalGreet", .{@as([]const u8, "wait_cancellation_completed")}), a);
    _ = try inst.activate(&try localBackoff(a, 1), a);
    inst.destroy();
}

const Cron = struct {
    fn run(ctx: *workflow.Context) !i64 {
        const last: i64 = if (ctx.hasLastCompletionResult()) try ctx.lastCompletionResult(i64) else 0;
        _ = try ctx.executeActivityByName(void, "Tick", .{ .start_to_close_timeout = .fromSeconds(10) }, .{last}).get();
        return last + 1;
    }
};
const CronWf = workflow.Def("Cron", Cron.run);

test "last completion result, cron info, and activities by name" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, CronWf.registered, .{});
    defer inst.destroy();

    var act = try initActivation(a, "Cron", .{});
    var init = &act.jobs.items[0].variant.?.initialize_workflow;
    init.cron_schedule = "* * * * *";
    init.last_completion_result = .{};
    try init.last_completion_result.?.payloads.append(a, try converter.toPayload(a, @as(i64, 41)));
    const first = commandsOf(try inst.activate(&act, a));
    try testing.expectEqualStrings("* * * * *", inst.info.cron_schedule);
    try testing.expect(inst.info.start_time == null);
    const sched = first[0].variant.?.schedule_activity;
    try testing.expectEqualStrings("Tick", sched.activity_type);
    try testing.expectEqualStrings("41", sched.arguments.items[0].data);

    const done = commandsOf(try inst.activate(&try oneJob(a, .{ .resolve_activity = .{ .seq = 1, .result = .{ .status = .{ .completed = .{} } } } }), a));
    try testing.expectEqualStrings("42", done[0].variant.?.complete_workflow_execution.result.?.data);
}

test "info: parent and root are null for a top-level workflow and set for a child; start time from the start" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    {
        const inst = try create(testing.allocator, Nap.registered, .{ .namespace = "ns" });
        defer inst.destroy();
        _ = try inst.activate(&try initActivation(a, "Nap", .{}), a);
        try testing.expect(inst.info.parent == null);
        try testing.expect(inst.info.root == null);
    }
    const inst = try create(testing.allocator, Nap.registered, .{ .namespace = "ns" });
    defer inst.destroy();
    var act = try initActivation(a, "Nap", .{});
    const init = &act.jobs.items[0].variant.?.initialize_workflow;
    init.parent_workflow_info = .{ .namespace = "parent-ns", .workflow_id = "parent-id", .run_id = "parent-run" };
    init.root_workflow = .{ .workflow_id = "root-id", .run_id = "root-run" };
    init.start_time = .{ .seconds = 1_700_000_000, .nanos = 5 };
    _ = try inst.activate(&act, a);
    try testing.expectEqual(@as(i96, 1_700_000_000 * std.time.ns_per_s + 5), inst.ctx.info().start_time.?.nanoseconds);
    const parent = inst.ctx.info().parent.?;
    try testing.expectEqualStrings("parent-ns", parent.namespace);
    try testing.expectEqualStrings("parent-id", parent.workflow_id);
    try testing.expectEqualStrings("parent-run", parent.run_id);
    const root = inst.ctx.info().root.?;
    try testing.expectEqualStrings("parent-ns", root.namespace);
    try testing.expectEqualStrings("root-id", root.workflow_id);
    try testing.expectEqualStrings("root-run", root.run_id);
}

const StartInfoWf = struct {
    /// Returns what the run reads of its start:
    /// "<retry policy>|<priority>|<original run ID>|<previous failure> <- <its cause>".
    fn run(ctx: *workflow.Context) ![]const u8 {
        const a = ctx.allocator();
        const info = ctx.info();
        const policy = if (info.retry_policy) |p| try std.fmt.allocPrint(a, "attempts={d},initial={?d},max={?d},types={s}", .{
            p.maximum_attempts,
            if (p.initial_interval) |d| @divTrunc(d.nanoseconds, std.time.ns_per_s) else null,
            if (p.maximum_interval) |d| @divTrunc(d.nanoseconds, std.time.ns_per_s) else null,
            try std.mem.join(a, ",", p.non_retryable_error_types),
        }) else "none";
        const previous = if (ctx.previousRunFailure()) |f|
            try std.fmt.allocPrint(a, "{s} <- {s}", .{ f.message, if (f.cause) |c| c.message else "none" })
        else
            "none";
        return std.fmt.allocPrint(a, "{s}|{d}/{s}/{d}|{s}|{s}", .{
            policy,
            info.priority.priority_key,
            info.priority.fairness_key,
            info.priority.fairness_weight,
            info.original_run_id,
            previous,
        });
    }
};
const StartInfoWfDef = workflow.Def("StartInfo", StartInfoWf.run);

/// A start activation with a retry policy, a priority, an original run ID, and a previous
/// failure with a cause, every string allocated in `a`.
fn startWithEverything(a: Allocator, replaying: bool) !act_pb.WorkflowActivation {
    var act = try initActivation(a, "StartInfo", .{});
    act.is_replaying = replaying;
    const init = &act.jobs.items[0].variant.?.initialize_workflow;
    init.retry_policy = .{
        .initial_interval = .{ .seconds = 1 },
        .backoff_coefficient = 2,
        .maximum_interval = .{ .seconds = 0 },
        .maximum_attempts = 3,
    };
    try init.retry_policy.?.non_retryable_error_types.append(a, try a.dupe(u8, "Fatal"));
    init.priority = .{ .priority_key = 2, .fairness_key = try a.dupe(u8, "tenant-a"), .fairness_weight = 1.5 };
    init.original_execution_run_id = try a.dupe(u8, "run-0");
    const cause = try a.create(Failure);
    cause.* = .{ .message = try a.dupe(u8, "root cause") };
    init.continued_failure = try failure_mod.application(a, .{ .message = try a.dupe(u8, "first attempt"), .type = try a.dupe(u8, "Flaky") });
    init.continued_failure.?.cause = cause;
    return act;
}

test "info: retry policy, priority, original run ID, and the previous run's failure from the start" {
    const want = "\"attempts=3,initial=1,max=null,types=Fatal|2/tenant-a/1.5|run-0|first attempt <- root cause\"";
    for ([_]bool{ false, true }) |replaying| {
        var source: std.heap.ArenaAllocator = .init(testing.allocator);
        var source_freed = false;
        defer if (!source_freed) source.deinit();
        var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
        defer scratch.deinit();
        const inst = try create(testing.allocator, StartInfoWfDef.registered, .{});
        defer inst.destroy();

        try testing.expectEqualStrings(want, completedWith(try inst.activate(&try startWithEverything(source.allocator(), replaying), scratch.allocator())));
        // The run keeps its own copies once the activation is gone.
        source.deinit();
        source_freed = true;
        const info = inst.ctx.info();
        const policy = info.retry_policy.?;
        try testing.expectEqual(@as(i32, 3), policy.maximum_attempts);
        try testing.expectEqual(@as(f64, 2), policy.backoff_coefficient);
        try testing.expectEqual(@as(i96, std.time.ns_per_s), policy.initial_interval.?.nanoseconds);
        try testing.expect(policy.maximum_interval == null);
        try testing.expectEqual(@as(usize, 1), policy.non_retryable_error_types.len);
        try testing.expectEqualStrings("Fatal", policy.non_retryable_error_types[0]);
        try testing.expectEqual(@as(i32, 2), info.priority.priority_key);
        try testing.expectEqualStrings("tenant-a", info.priority.fairness_key);
        try testing.expectEqual(@as(f32, 1.5), info.priority.fairness_weight);
        try testing.expectEqualStrings("run-0", info.original_run_id);
        const previous = inst.ctx.previousRunFailure().?;
        try testing.expectEqualStrings("first attempt", previous.message);
        try testing.expectEqualStrings("Flaky", failure_mod.applicationType(previous.*).?);
        try testing.expectEqualStrings("root cause", previous.cause.?.message);
    }

    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, StartInfoWfDef.registered, .{});
    defer inst.destroy();
    try testing.expectEqualStrings("\"none|0//0||none\"", completedWith(try inst.activate(&try initActivation(a, "StartInfo", .{}), a)));
    try testing.expect(inst.ctx.info().retry_policy == null);
    try testing.expectEqualDeep(workflow.Priority{}, inst.ctx.info().priority);
    try testing.expect(inst.ctx.previousRunFailure() == null);
}

const TimeoutInfoWf = struct {
    /// Returns "<execution>/<run>/<task>" in whole seconds, "null" for each unset one.
    fn run(ctx: *workflow.Context) ![]const u8 {
        const info = ctx.info();
        return std.fmt.allocPrint(ctx.allocator(), "{?d}/{?d}/{?d}", .{
            seconds(info.execution_timeout),
            seconds(info.run_timeout),
            seconds(info.task_timeout),
        });
    }

    fn seconds(d: ?std.Io.Duration) ?i96 {
        return if (d) |v| @divTrunc(v.nanoseconds, std.time.ns_per_s) else null;
    }
};
const TimeoutInfoWfDef = workflow.Def("TimeoutInfo", TimeoutInfoWf.run);

test "info: timeouts are null when absent or zero and the start values when set" {
    const Case = struct { execution: ?proto.google.protobuf.Duration, run: ?proto.google.protobuf.Duration, task: ?proto.google.protobuf.Duration, want: []const u8 };
    const cases = [_]Case{
        .{ .execution = null, .run = null, .task = null, .want = "\"null/null/null\"" },
        // The server sends a zero Duration for a timeout the workflow was started without.
        .{ .execution = .{}, .run = .{}, .task = .{}, .want = "\"null/null/null\"" },
        .{ .execution = .{ .seconds = -1 }, .run = .{ .nanos = -1 }, .task = .{}, .want = "\"null/null/null\"" },
        .{ .execution = .{ .seconds = 3600 }, .run = .{ .seconds = 60 }, .task = .{ .seconds = 10 }, .want = "\"3600/60/10\"" },
    };
    for (cases) |c| {
        var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
        defer scratch.deinit();
        const a = scratch.allocator();
        const inst = try create(testing.allocator, TimeoutInfoWfDef.registered, .{});
        defer inst.destroy();
        var act = try initActivation(a, "TimeoutInfo", .{});
        const init = &act.jobs.items[0].variant.?.initialize_workflow;
        init.workflow_execution_timeout = c.execution;
        init.workflow_run_timeout = c.run;
        init.workflow_task_timeout = c.task;
        try testing.expectEqualStrings(c.want, completedWith(try inst.activate(&act, a)));
    }
}

const CurrentUpdateWf = struct {
    const Who = workflow.UpdateDef("who", fn (expected_id: []const u8) []const u8);
    const Probe = workflow.SignalDef("probe", fn () void);
    const Done = workflow.SignalDef("done", fn () void);

    const State = struct {
        ctx: *workflow.Context,
        /// What the workflow function, the signal handler, and the coroutine an update
        /// handler starts saw, in that order: 'n' for no update, 'u' for an update.
        seen: std.ArrayList(u8) = .empty,
        done: bool = false,

        fn note(s: *State) !void {
            try s.seen.append(s.ctx.allocator(), if (s.ctx.currentUpdate() == null) 'n' else 'u');
        }
        fn check(s: *State, expected_id: []const u8) error{ NoUpdate, WrongUpdate }!void {
            const u = s.ctx.currentUpdate() orelse return error.NoUpdate;
            if (!std.mem.eql(u8, u.id, expected_id) or !std.mem.eql(u8, u.name, "who")) return error.WrongUpdate;
        }
        /// Returns "<ID>/<name>" as read after a timer, so concurrent handlers interleave.
        fn who(s: *State, _: []const u8) ![]const u8 {
            try s.ctx.io().sleep(.fromSeconds(5), .awake);
            var inner = s.ctx.io().async(note, .{s});
            try inner.await(s.ctx.io());
            const u = s.ctx.currentUpdate() orelse return error.NoUpdate;
            return std.fmt.allocPrint(s.ctx.allocator(), "{s}/{s}", .{ u.id, u.name });
        }
        fn probe(s: *State) void {
            s.note() catch {};
        }
        fn finish(s: *State) void {
            s.done = true;
        }
        fn finished(s: *State) bool {
            return s.done;
        }
    };

    /// Returns what the coroutines without an update saw.
    fn run(ctx: *workflow.Context) ![]const u8 {
        const s = try ctx.allocator().create(State);
        s.* = .{ .ctx = ctx };
        try s.note();
        try ctx.setUpdateHandlerWithValidator(Who, s, State.who, State.check);
        try ctx.setSignalHandler(Probe, s, State.probe);
        try ctx.setSignalHandler(Done, s, State.finish);
        try ctx.await(s, State.finished);
        try ctx.await(ctx, workflow.Context.allHandlersFinished);
        return s.seen.items;
    }
};
const CurrentUpdateWfDef = workflow.Def("CurrentUpdate", CurrentUpdateWf.run);

test "update: the validator and the handler see the current update; other coroutines see none" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, CurrentUpdateWfDef.registered, .{});
    defer inst.destroy();

    _ = try inst.activate(&try initActivation(a, "CurrentUpdate", .{}), a);
    try testing.expect(inst.ctx.currentUpdate() == null);
    // u-3's validator expects another ID, so it sees its own and rejects the update.
    var act = try oneJob(a, try updateJob(a, "u-1", "who", .{@as([]const u8, "u-1")}, true));
    try act.jobs.append(a, .{ .variant = try updateJob(a, "u-2", "who", .{@as([]const u8, "u-2")}, true) });
    try act.jobs.append(a, .{ .variant = try updateJob(a, "u-3", "who", .{@as([]const u8, "u-1")}, true) });
    try act.jobs.append(a, .{ .variant = try signalJob(a, "probe", .{}) });
    const blocked = commandsOf(try inst.activate(&act, a));
    try testing.expect(try updateResponse(blocked[0], "u-1") == .accepted);
    try testing.expect(try updateResponse(blocked[1], "u-2") == .accepted);
    try testing.expectEqualStrings("WrongUpdate", (try updateResponse(blocked[2], "u-3")).rejected.message);
    try testing.expect(inst.ctx.currentUpdate() == null);

    var fire = try oneJob(a, .{ .fire_timer = .{ .seq = 2 } });
    try fire.jobs.append(a, .{ .variant = .{ .fire_timer = .{ .seq = 1 } } });
    try fire.jobs.append(a, .{ .variant = try signalJob(a, "done", .{}) });
    const done = commandsOf(try inst.activate(&fire, a));
    try testing.expectEqual(@as(usize, 3), done.len);
    try testing.expectEqualStrings("\"u-2/who\"", (try updateResponse(done[0], "u-2")).completed.data);
    try testing.expectEqualStrings("\"u-1/who\"", (try updateResponse(done[1], "u-1")).completed.data);
    try testing.expectEqualStrings("\"nnnn\"", done[2].variant.?.complete_workflow_execution.result.?.data);
}

// A handler that registers an update handler validates the buffered updates of that name
// inline in its own coroutine; the validator must still see its own update.
const NestedValidatorWf = struct {
    const Register = workflow.SignalDef("register", fn () void);
    const Set = workflow.UpdateDef("set", fn () void);
    const A = workflow.UpdateDef("a", fn () []const u8);
    const B = workflow.UpdateDef("b", fn () void);
    const Done = workflow.SignalDef("done", fn () void);

    const State = struct {
        ctx: *workflow.Context,
        /// What each coroutine saw as "<ID>/<name>", or "none" for no update.
        set_validator: []const u8 = "",
        signal_after: []const u8 = "",
        b_validator: []const u8 = "",
        done: bool = false,

        fn seen(s: *State) ![]const u8 {
            const u = s.ctx.currentUpdate() orelse return "none";
            return std.fmt.allocPrint(s.ctx.allocator(), "{s}/{s}", .{ u.id, u.name });
        }
        fn register(s: *State) !void {
            try s.ctx.setUpdateHandlerWithValidator(Set, s, State.noop, State.validateSet);
            s.signal_after = try s.seen();
        }
        fn validateSet(s: *State) !void {
            s.set_validator = try s.seen();
        }
        fn handleA(s: *State) ![]const u8 {
            try s.ctx.setUpdateHandlerWithValidator(B, s, State.noop, State.validateB);
            return s.seen();
        }
        fn validateB(s: *State) !void {
            s.b_validator = try s.seen();
        }
        fn noop(_: *State) void {}
        fn finish(s: *State) void {
            s.done = true;
        }
        fn finished(s: *State) bool {
            return s.done;
        }
    };

    /// Returns "<set's validator>|<the signal handler after registering>|<b's validator>".
    fn run(ctx: *workflow.Context) ![]const u8 {
        const s = try ctx.allocator().create(State);
        s.* = .{ .ctx = ctx };
        try ctx.setSignalHandler(Register, s, State.register);
        try ctx.setUpdateHandler(A, s, State.handleA);
        try ctx.setSignalHandler(Done, s, State.finish);
        try ctx.await(s, State.finished);
        try ctx.await(ctx, workflow.Context.allHandlersFinished);
        return std.fmt.allocPrint(ctx.allocator(), "{s}|{s}|{s}", .{ s.set_validator, s.signal_after, s.b_validator });
    }
};
const NestedValidatorWfDef = workflow.Def("NestedValidator", NestedValidatorWf.run);

test "update: a validator run from inside another handler sees its own update" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, NestedValidatorWfDef.registered, .{});
    defer inst.destroy();

    _ = try inst.activate(&try initActivation(a, "NestedValidator", .{}), a);
    // u-1 and u-2 wait for their handlers, which the signal handler and u-3's handler register.
    var act = try oneJob(a, try updateJob(a, "u-1", "set", .{}, true));
    try act.jobs.append(a, .{ .variant = try updateJob(a, "u-2", "b", .{}, true) });
    try act.jobs.append(a, .{ .variant = try signalJob(a, "register", .{}) });
    try act.jobs.append(a, .{ .variant = try updateJob(a, "u-3", "a", .{}, true) });
    try act.jobs.append(a, .{ .variant = try signalJob(a, "done", .{}) });
    const cmds = commandsOf(try inst.activate(&act, a));
    var accepted: usize = 0;
    var completed: usize = 0;
    var result: []const u8 = "";
    for (cmds) |c| switch (c.variant.?) {
        .update_response => |r| switch (r.response.?) {
            .accepted => accepted += 1,
            .completed => |p| {
                completed += 1;
                if (std.mem.eql(u8, r.protocol_instance_id, "u-3")) try testing.expectEqualStrings("\"u-3/a\"", p.data);
            },
            .rejected => return error.TestUnexpectedResult,
        },
        .complete_workflow_execution => |c2| result = c2.result.?.data,
        else => {},
    };
    try testing.expectEqual(@as(usize, 3), accepted);
    try testing.expectEqual(@as(usize, 3), completed);
    try testing.expectEqualStrings("\"u-1/set|none|u-2/b\"", result);
}

const MemoWf = struct {
    fn keys(ctx: *workflow.Context, fields: []const converter.Field) ![]const u8 {
        var names: std.ArrayList([]const u8) = .empty;
        for (fields) |f| try names.append(ctx.allocator(), f.name);
        return std.mem.join(ctx.allocator(), ",", names.items);
    }

    /// Returns "<start keys>|<keys after the upsert>|<start status>-><status>".
    fn run(ctx: *workflow.Context) ![]const u8 {
        const start = ctx.memo();
        const before = (try ctx.memoValue([]const u8, "status")).?;
        try ctx.upsertMemo(.{
            .status = "shipped",
            .owner = @as(?[]const u8, null),
            .missing = @as(?i64, null),
            .tags = @as([]const []const u8, &.{}),
            .added = @as(i64, 1),
        });
        try ctx.upsertMemo(.{});
        const after = (try ctx.memoValue([]const u8, "status")).?;
        return std.fmt.allocPrint(ctx.allocator(), "{s}|{s}|{s}->{s}", .{ try keys(ctx, start), try keys(ctx, ctx.memo()), before, after });
    }
};
const MemoWfDef = workflow.Def("Memo", MemoWf.run);

test "memo: the start memo is visible, sorted; an upsert emits ModifyWorkflowProperties and updates the view" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, MemoWfDef.registered, .{});
    defer inst.destroy();

    var act = try initActivation(a, "Memo", .{});
    var start_memo: common.Memo = .{};
    for (try converter.toFields(a, .{ .zeta = "z", .status = "queued", .owner = "data", .tags = @as([]const []const u8, &.{"x"}) })) |f|
        try start_memo.fields.append(a, .{ .key = f.name, .value = f.value });
    act.jobs.items[0].variant.?.initialize_workflow.memo = start_memo;

    const cmds = commandsOf(try inst.activate(&act, a));
    try testing.expectEqual(@as(usize, 2), cmds.len); // upserting nothing emits no command
    const upserted = cmds[0].variant.?.modify_workflow_properties.upserted_memo.?.fields.items;
    const want = [_][]const u8{ "status", "owner", "missing", "tags", "added" };
    try testing.expectEqual(want.len, upserted.len);
    for (want, upserted) |name, entry| try testing.expectEqualStrings(name, entry.key);
    try testing.expectEqualStrings("binary/null", converter.encodingOf(upserted[1].value.?).?);
    try testing.expectEqualStrings("[]", upserted[3].value.?.data);

    // Null and an empty slice remove keys (a missing key stays missing); others replace or add.
    try testing.expectEqualStrings("\"owner,status,tags,zeta|added,status,zeta|queued->shipped\"", cmds[1].variant.?.complete_workflow_execution.result.?.data);
}

const SearchAttributesWf = struct {
    const status = sa.key(.keyword, "Status");
    const priority = sa.key(.int, "Priority");
    const tags = sa.key(.keyword_list, "Tags");

    /// Returns "<start status>|<names after the upserts>|<priority>", then continues as new
    /// with the default options when `again` is set.
    fn run(ctx: *workflow.Context, again: bool) ![]const u8 {
        const a = ctx.allocator();
        const start = ctx.searchAttributes();
        const before = (try start.get(a, status)).?;
        try ctx.upsertSearchAttributes(&.{ priority.set(2), status.unset(), tags.set(&.{ "x", "y" }), priority.set(3) });
        try ctx.upsertSearchAttributes(&.{});
        if (start.len() != 2) return error.StartViewChanged;
        if (again) return ctx.continueAsNew(Def, .{}, .{false});
        var names: std.ArrayList([]const u8) = .empty;
        for (ctx.searchAttributes().fields) |f| try names.append(a, f.name);
        return std.fmt.allocPrint(a, "{s}|{s}|{d}", .{ before, try std.mem.join(a, ",", names.items), (try ctx.searchAttributes().get(a, priority)).? });
    }
    const Def = workflow.Def("SearchAttributes", run);

    fn badDouble(ctx: *workflow.Context) !void {
        try ctx.upsertSearchAttributes(&.{sa.key(.double, "Score").set(std.math.nan(f64))});
    }
    const BadDouble = workflow.Def("BadDouble", badDouble);
};

fn startWithSearchAttributes(a: Allocator, workflow_type: []const u8, again: bool) !act_pb.WorkflowActivation {
    var act = try initActivation(a, workflow_type, .{again});
    act.jobs.items[0].variant.?.initialize_workflow.search_attributes = try sa.toProto(a, &.{
        SearchAttributesWf.status.set("queued"),
        SearchAttributesWf.priority.set(1),
    }, .start);
    return act;
}

test "search attributes: the start values are visible; an upsert emits UpsertWorkflowSearchAttributes and updates the view" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, SearchAttributesWf.Def.registered, .{});
    defer inst.destroy();

    const cmds = commandsOf(try inst.activate(&try startWithSearchAttributes(a, "SearchAttributes", false), a));
    try testing.expectEqual(@as(usize, 2), cmds.len); // upserting nothing emits no command
    const upserted = cmds[0].variant.?.upsert_workflow_search_attributes.search_attributes.?.indexed_fields.items;
    const want = [_][]const u8{ "Priority", "Status", "Tags", "Priority" };
    try testing.expectEqual(want.len, upserted.len);
    for (want, upserted) |name, entry| try testing.expectEqualStrings(name, entry.key);
    try testing.expectEqualStrings("binary/null", converter.encodingOf(upserted[1].value.?).?);
    try testing.expectEqualStrings("[\"x\",\"y\"]", upserted[2].value.?.data);
    try testing.expectEqualStrings("\"queued|Priority,Tags|3\"", cmds[1].variant.?.complete_workflow_execution.result.?.data);
}

test "search attributes: continue-as-new with default options sends the current values" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, SearchAttributesWf.Def.registered, .{});
    defer inst.destroy();

    const cmds = commandsOf(try inst.activate(&try startWithSearchAttributes(a, "SearchAttributes", true), a));
    const can = cmds[1].variant.?.continue_as_new_workflow_execution;
    const view = try sa.View.fromProto(a, can.search_attributes);
    try testing.expectEqual(@as(usize, 2), view.len());
    try testing.expectEqual(@as(?i64, 3), try view.get(a, SearchAttributesWf.priority));
    try testing.expectEqualStrings("y", (try view.get(a, SearchAttributesWf.tags)).?[1]);
}

test "search attributes: a value the server can't index fails the workflow, with no command" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, SearchAttributesWf.BadDouble.registered, .{});
    defer inst.destroy();

    const cmds = commandsOf(try inst.activate(&try initActivation(a, "BadDouble", .{}), a));
    try testing.expectEqual(@as(usize, 1), cmds.len);
    try testing.expectEqualStrings("InvalidSearchAttribute", cmds[0].variant.?.fail_workflow_execution.failure.?.message);
}

const QueryWf = struct {
    const Status = workflow.QueryDef("status", fn () []const u8);
    const Add = workflow.QueryDef("add", fn (i64, i64) i64);
    const Fails = workflow.QueryDef("fails", fn () i64);

    const State = struct {
        phase: []const u8 = "starting",
        base: i64 = 0,

        fn status(s: *State) []const u8 {
            return s.phase;
        }
        fn add(s: *State, x: i64, y: i64) i64 {
            return s.base + x + y;
        }
        fn fails(_: *State) error{NotReady}!i64 {
            return error.NotReady;
        }
        fn replaced(_: *State) []const u8 {
            return "replaced";
        }
    };

    /// Registers the handlers, sleeps, and changes the queried phase around the sleep.
    fn run(ctx: *workflow.Context) ![]const u8 {
        const s = try ctx.allocator().create(State);
        s.* = .{ .base = 100 };
        try ctx.setQueryHandler(Status, s, State.status);
        try ctx.setQueryHandler(Add, s, State.add);
        try ctx.setQueryHandler(Fails, s, State.fails);
        s.phase = "sleeping";
        try ctx.io().sleep(.fromSeconds(10), .awake);
        s.phase = "done";
        return "finished";
    }

    fn reregister(ctx: *workflow.Context) ![]const u8 {
        const s = try ctx.allocator().create(State);
        s.* = .{};
        try ctx.setQueryHandler(Status, s, State.status);
        try ctx.setQueryHandler(Status, s, State.replaced);
        try ctx.io().sleep(.fromSeconds(10), .awake);
        return "finished";
    }

    fn onStack(ctx: *workflow.Context) ![]const u8 {
        var s: State = .{};
        ctx.setQueryHandler(Status, &s, State.status) catch |err| return @errorName(err);
        return "accepted";
    }
};
const QueryWfDef = workflow.Def("Queries", QueryWf.run);
const ReregisterWf = workflow.Def("Reregister", QueryWf.reregister);
const OnStackWf = workflow.Def("OnStack", QueryWf.onStack);

fn queryActivation(a: Allocator, jobs: []const struct { []const u8, []const u8, []const converter.Payload }) !act_pb.WorkflowActivation {
    var act: act_pb.WorkflowActivation = .{ .run_id = "run-1", .timestamp = .{ .seconds = 1_700_000_999 } };
    for (jobs) |j| {
        var q: act_pb.QueryWorkflow = .{ .query_id = j[0], .query_type = j[1] };
        try q.arguments.appendSlice(a, j[2]);
        try act.jobs.append(a, .{ .variant = .{ .query_workflow = q } });
    }
    return act;
}

fn answer(c: done_pb.WorkflowActivationCompletion, i: usize) cmd_pb.QueryResult {
    return commandsOf(c)[i].variant.?.respond_to_query;
}

test "query: answers with the handler's result, decodes arguments, and emits nothing else" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, QueryWfDef.registered, .{});
    defer inst.destroy();
    const first = commandsOf(try inst.activate(&try initActivation(a, "Queries", .{}), a));
    try testing.expectEqual(@as(usize, 1), first.len);
    const now_before = inst.now_ns;

    const c = try inst.activate(&try queryActivation(a, &.{
        .{ "q1", "status", &.{} },
        .{ "q2", "add", (try converter.toPayloads(a, .{ @as(i64, 2), @as(i64, 3) })).items },
    }), a);
    try testing.expectEqual(@as(usize, 2), commandsOf(c).len);
    try testing.expectEqualStrings("q1", answer(c, 0).query_id);
    try testing.expectEqualStrings("\"sleeping\"", answer(c, 0).variant.?.succeeded.response.?.data);
    try testing.expectEqualStrings("q2", answer(c, 1).query_id);
    try testing.expectEqualStrings("105", answer(c, 1).variant.?.succeeded.response.?.data);
    try testing.expectEqual(now_before, inst.now_ns); // a query doesn't move workflow time

    // Arguments that don't decode fail the query, not the task.
    const bad = try inst.activate(&try queryActivation(a, &.{.{ "q3", "add", &.{} }}), a);
    try testing.expectEqualStrings("WrongArgumentCount", answer(bad, 0).variant.?.failed.message);
}

test "query: an unknown query type fails naming the registered types; a failing handler fails the query" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, QueryWfDef.registered, .{});
    defer inst.destroy();
    _ = try inst.activate(&try initActivation(a, "Queries", .{}), a);

    const c = try inst.activate(&try queryActivation(a, &.{
        .{ "q1", "nope", &.{} },
        .{ "q2", "__stack_trace", &.{} },
        .{ "q3", "fails", &.{} },
    }), a);
    const unknown = answer(c, 0).variant.?.failed;
    try testing.expectEqualStrings("query type \"nope\" is not registered on workflow \"Queries\"; registered query types: [status, add, fails]", unknown.message);
    try testing.expectEqualStrings("QueryNotRegistered", failure_mod.applicationType(unknown).?);
    // The worker answers the stack trace query itself (src/workflow/stack_trace_test.zig).
    try testing.expect(answer(c, 1).variant.? == .succeeded);
    const failed = answer(c, 2).variant.?.failed;
    try testing.expectEqualStrings("NotReady", failed.message);
    try testing.expectEqualStrings("NotReady", failure_mod.applicationType(failed).?);
}

test "query: answered after the workflow function returned, reading state that outlives it" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, QueryWfDef.registered, .{});
    defer inst.destroy();
    const timer = commandsOf(try inst.activate(&try initActivation(a, "Queries", .{}), a))[0].variant.?.start_timer;
    const done = commandsOf(try inst.activate(&try oneJob(a, .{ .fire_timer = .{ .seq = timer.seq } }), a));
    try testing.expectEqualStrings("\"finished\"", done[0].variant.?.complete_workflow_execution.result.?.data);
    try testing.expect(inst.finished);

    const c = try inst.activate(&try queryActivation(a, &.{.{ "q1", "status", &.{} }}), a);
    try testing.expectEqual(@as(usize, 1), commandsOf(c).len);
    try testing.expectEqualStrings("\"done\"", answer(c, 0).variant.?.succeeded.response.?.data);
}

test "query: registering a query type again replaces its handler" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, ReregisterWf.registered, .{});
    defer inst.destroy();
    _ = try inst.activate(&try initActivation(a, "Reregister", .{}), a);
    const c = try inst.activate(&try queryActivation(a, &.{.{ "q1", "status", &.{} }}), a);
    try testing.expectEqualStrings("\"replaced\"", answer(c, 0).variant.?.succeeded.response.?.data);
    try testing.expectEqual(@as(usize, 1), inst.queries.count());
}

test "query: a handler context on the workflow coroutine's stack is rejected" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, OnStackWf.registered, .{});
    defer inst.destroy();
    const done = commandsOf(try inst.activate(&try initActivation(a, "OnStack", .{}), a));
    try testing.expectEqualStrings("\"QueryContextOnStack\"", done[0].variant.?.complete_workflow_execution.result.?.data);
    try testing.expectEqual(@as(usize, 0), inst.queries.count());
}

// The dynamic update handler. `State.log` records each update the dynamic handler ran.
const DynamicUpdates = struct {
    const Set = workflow.UpdateDef("set", fn (value: i64) i64);
    const Done = workflow.SignalDef("done", fn () void);

    const State = struct {
        ctx: *workflow.Context,
        value: i64 = 0,
        done: bool = false,
        log: std.ArrayList(u8) = .empty,

        fn create(ctx: *workflow.Context) !*State {
            const s = try ctx.allocator().create(State);
            s.* = .{ .ctx = ctx };
            return s;
        }
        fn set(s: *State, value: i64) i64 {
            defer s.value = value;
            return s.value;
        }
        /// Fails unless `ctx.currentUpdate()` names update `name`.
        fn expectCurrent(s: *State, name: []const u8) ![]const u8 {
            const u = s.ctx.currentUpdate() orelse return error.NoCurrentUpdate;
            if (!std.mem.eql(u8, u.name, name)) return error.WrongUpdate;
            return u.id;
        }
        /// Returns ten times its argument and records "<ID>:<name>=<arg>"; "slow" sleeps 5 s
        /// first, and "bad" fails after it was accepted.
        fn any(s: *State, name: []const u8, args: converter.EncodedValues) !i64 {
            _ = try s.expectCurrent(name);
            if (std.mem.eql(u8, name, "bad")) return error.Unsupported;
            if (std.mem.eql(u8, name, "slow")) try s.ctx.io().sleep(.fromSeconds(5), .awake);
            const value = try args.get(i64, s.ctx.allocator(), 0);
            // Read again after the sleep: each coroutine keeps its own update.
            const id = try s.expectCurrent(name);
            try s.log.print(s.ctx.allocator(), "{s}:{s}={d};", .{ id, name, value });
            return value * 10;
        }
        fn check(s: *State, name: []const u8, args: converter.EncodedValues) !void {
            _ = try s.expectCurrent(name);
            if (try args.get(i64, s.ctx.allocator(), 0) < 0) return error.Negative;
        }
        fn finish(s: *State) void {
            s.done = true;
        }
        fn finished(s: *State) bool {
            return s.done;
        }
    };

    fn wait(ctx: *workflow.Context, s: *State) ![]const u8 {
        try ctx.setSignalHandler(Done, s, State.finish);
        try ctx.await(s, State.finished);
        try ctx.await(ctx, workflow.Context.allHandlersFinished);
        return std.fmt.allocPrint(ctx.allocator(), "{d} {s}", .{ s.value, s.log.items });
    }

    /// Handles `set` with its own handler and every other update with the dynamic handler and
    /// its validator, until a `done` signal and the handlers have finished.
    fn run(ctx: *workflow.Context) ![]const u8 {
        const s = try State.create(ctx);
        try ctx.setUpdateHandler(Set, s, State.set);
        try ctx.setDynamicUpdateHandlerWithValidator(s, State.any, State.check);
        return wait(ctx, s);
    }

    /// The dynamic handler without a validator, after a context on the stack was refused.
    fn plain(ctx: *workflow.Context) ![]const u8 {
        var local: State = .{ .ctx = ctx };
        if (ctx.setDynamicUpdateHandler(&local, State.any)) |_| return error.ContextOnStackAccepted else |err| if (err != error.UpdateContextOnStack) return err;
        const s = try State.create(ctx);
        try ctx.setDynamicUpdateHandler(s, State.any);
        return wait(ctx, s);
    }
};
const DynamicUpdatesWf = workflow.Def("DynamicUpdates", DynamicUpdates.run);
const PlainDynamicUpdatesWf = workflow.Def("PlainDynamicUpdates", DynamicUpdates.plain);

/// "<ID>:<response>" for each update response in `cmds`, space-separated, in order.
fn updateResponses(a: Allocator, cmds: []const cmd_pb.WorkflowCommand) ![]const u8 {
    var out: std.ArrayList(u8) = .empty;
    for (cmds) |cmd| switch (cmd.variant.?) {
        .update_response => |r| {
            if (out.items.len > 0) try out.append(a, ' ');
            try out.print(a, "{s}:", .{r.protocol_instance_id});
            switch (r.response.?) {
                .accepted => try out.appendSlice(a, "accepted"),
                .completed => |p| try out.print(a, "completed({s})", .{p.data}),
                .rejected => |f| try out.print(a, "rejected({s})", .{f.message}),
            }
        },
        else => {},
    };
    return out.items;
}

test "dynamic update: handles every update without a handler of its own, with its name and encoded arguments, in a coroutine of its own" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, DynamicUpdatesWf.registered, .{});
    defer inst.destroy();

    // Sent with the start, before the handlers are registered: they wait for them.
    var start = try initActivation(a, "DynamicUpdates", .{});
    try start.jobs.append(a, .{ .variant = try updateJob(a, "u1", "add", .{@as(i64, 4)}, true) });
    try start.jobs.append(a, .{ .variant = try updateJob(a, "u2", "set", .{@as(i64, 3)}, true) });
    try start.jobs.append(a, .{ .variant = try updateJob(a, "u3", "add", .{@as(i64, -1)}, true) });
    try start.jobs.append(a, .{ .variant = try updateJob(a, "u4", "slow", .{@as(i64, 2)}, true) });
    const first = commandsOf(try inst.activate(&start, a));
    try testing.expectEqualStrings("u2:accepted u1:accepted u3:rejected(Negative) u4:accepted u2:completed(0) u1:completed(40)", try updateResponses(a, first));
    try testing.expect(first[first.len - 1].variant.? == .start_timer);
    try testing.expectEqual(@as(usize, 0), inst.buffered_updates.items.len);

    // Delivered later; on replay (no validator run) a negative value is accepted.
    var later = try oneJob(a, try updateJob(a, "u5", "add", .{@as(i64, -2)}, false));
    try later.jobs.append(a, .{ .variant = try updateJob(a, "u6", "bad", .{@as(i64, 1)}, true) });
    try later.jobs.append(a, .{ .variant = try updateJob(a, "u7", "add", .{@as([]const u8, "five")}, true) });
    try later.jobs.append(a, .{ .variant = try signalJob(a, "done", .{}) });
    const second = commandsOf(try inst.activate(&later, a));
    try testing.expectEqualStrings("u5:accepted u6:accepted u7:rejected(InvalidPayload) u5:completed(-20) u6:rejected(Unsupported)", try updateResponses(a, second));

    const last = try inst.activate(&try oneJob(a, .{ .fire_timer = .{ .seq = 1 } }), a);
    try testing.expectEqualStrings("u4:completed(20)", try updateResponses(a, commandsOf(last)));
    try testing.expectEqualStrings("\"3 u1:add=4;u5:add=-2;u4:slow=2;\"", completedWith(last));
}

test "dynamic update: without a validator an argument that doesn't decode rejects the update after acceptance; a context on the stack is refused" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, PlainDynamicUpdatesWf.registered, .{});
    defer inst.destroy();

    _ = try inst.activate(&try initActivation(a, "PlainDynamicUpdates", .{}), a);
    var act = try oneJob(a, try updateJob(a, "u1", "add", .{@as([]const u8, "five")}, true));
    try act.jobs.append(a, .{ .variant = try updateJob(a, "u2", "add", .{@as(i64, -1)}, true) });
    try act.jobs.append(a, .{ .variant = try signalJob(a, "done", .{}) });
    const c = try inst.activate(&act, a);
    try testing.expectEqualStrings("u1:accepted u2:accepted u1:rejected(InvalidPayload) u2:completed(-10)", try updateResponses(a, commandsOf(c)));
    try testing.expectEqualStrings("\"0 u2:add=-1;\"", completedWith(c));
}

// Handlers still running when the workflow ends: one warning names them, except those
// registered or declared with the `abandon` unfinished policy.
const Unfinished = struct {
    const Slow = workflow.UpdateDef("slow", fn () void);
    const QuietSlow = workflow.UpdateDef("quiet_slow", fn () void);
    const Wait = workflow.SignalDef("wait", fn () void);
    const QuietWait = workflow.SignalDef("quiet_wait", fn () void);

    const State = struct {
        ctx: *workflow.Context,

        fn nap(s: *State) !void {
            try s.ctx.io().sleep(.fromSeconds(60), .awake);
        }
        fn napAny(s: *State, _: []const u8, _: converter.EncodedValues) !void {
            try s.nap();
        }
    };

    /// Registers handlers that sleep a minute, sleeps 10 s, then ends as `mode` says:
    /// "continue" continues as new, "fail" fails, anything else completes. With "quiet" every
    /// handler opts out of the warning.
    fn run(ctx: *workflow.Context, mode: []const u8) ![]const u8 {
        const s = try ctx.allocator().create(State);
        s.* = .{ .ctx = ctx };
        const quiet = std.mem.eql(u8, mode, "quiet");
        const loud: workflow.HandlerOptions = if (quiet) .{ .unfinished_policy = .abandon } else .{};
        try ctx.setUpdateHandlerWithOptions(Slow, s, State.nap, null, loud);
        try ctx.setUpdateHandlerWithOptions(QuietSlow, s, State.nap, null, .{ .unfinished_policy = .abandon });
        try ctx.setSignalHandlerWithOptions(Wait, s, State.nap, loud);
        try ctx.setSignalHandlerWithOptions(QuietWait, s, State.nap, .{ .unfinished_policy = .abandon });
        if (quiet) {
            try ctx.setDynamicUpdateHandlerWithOptions(s, State.napAny, null, .{ .unfinished_policy = .abandon });
            try ctx.setDynamicSignalHandlerWithOptions(s, State.napAny, .{ .unfinished_policy = .abandon });
            try ctx.setSignalHandlerByNameWithOptions("by_name", @TypeOf(.{}), s, State.nap, .{ .unfinished_policy = .abandon });
        } else {
            try ctx.setDynamicUpdateHandler(s, State.napAny);
            try ctx.setDynamicSignalHandler(s, State.napAny);
            try ctx.setSignalHandlerByName("by_name", @TypeOf(.{}), s, State.nap);
        }
        try ctx.io().sleep(.fromSeconds(10), .awake);
        if (std.mem.eql(u8, mode, "continue")) return ctx.continueAsNew(UnfinishedWf, .{}, .{mode});
        if (std.mem.eql(u8, mode, "fail")) return error.Failed;
        return "done";
    }

    /// Declares its handlers: `wait` and `slow` opt out of the warning, `loud` doesn't.
    const Declared = struct {
        ctx: *workflow.Context = undefined,

        const Loud = workflow.SignalDef("loud", fn () void);
        const Other = workflow.UpdateDef("other", fn () void);

        pub const signals = .{
            .wait = .{ Wait, .{ .unfinished_policy = .abandon } },
            .loud = Loud,
        };
        pub const updates = .{
            .slow = .{ Slow, .check, .{ .unfinished_policy = .abandon } },
            .other = .{ Other, workflow.HandlerOptions{ .unfinished_policy = .abandon } },
        };

        pub fn run(d: *Declared, ctx: *workflow.Context) ![]const u8 {
            d.ctx = ctx;
            try ctx.io().sleep(.fromSeconds(10), .awake);
            return "done";
        }
        pub fn wait(d: *Declared) !void {
            try d.ctx.io().sleep(.fromSeconds(60), .awake);
        }
        pub fn loud(d: *Declared) !void {
            try d.wait();
        }
        pub fn slow(d: *Declared) !void {
            try d.wait();
        }
        pub fn other(d: *Declared) !void {
            try d.wait();
        }
        pub fn check(_: *Declared) void {}
    };
};
const UnfinishedWf = workflow.Def("Unfinished", Unfinished.run);
const DeclaredUnfinishedWf = workflow.Def("DeclaredUnfinished", Unfinished.Declared);

/// Starts `workflow_type` with `mode`, then starts a handler for each signal and update in
/// `signals` and `updates` (update IDs are "u-<name>"), each of which sleeps past the end.
fn startUnfinished(inst: *Instance, a: Allocator, workflow_type: []const u8, args: anytype, signals: []const []const u8, updates: []const []const u8) !void {
    _ = try inst.activate(&try initActivation(a, workflow_type, args), a);
    var act: act_pb.WorkflowActivation = .{ .run_id = "run-1", .timestamp = .{ .seconds = 1_700_000_005 } };
    for (signals) |name| try act.jobs.append(a, .{ .variant = try signalJob(a, name, .{}) });
    for (updates) |name| try act.jobs.append(a, .{ .variant = try updateJob(a, try std.fmt.allocPrint(a, "u-{s}", .{name}), name, .{}, true) });
    for (commandsOf(try inst.activate(&act, a))) |cmd| switch (cmd.variant.?) {
        .update_response => |r| try testing.expect(r.response.? == .accepted),
        .start_timer => {},
        else => return error.UnexpectedCommand,
    };
}

const unfinished_signals = [_][]const u8{ "wait", "quiet_wait", "ping", "by_name", "wait" };
const unfinished_updates = [_][]const u8{ "slow", "quiet_slow", "other" };

test "unfinished handlers: completing, canceling or continuing as new logs one warning naming the running handlers" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();

    for ([_][]const u8{ "complete", "cancel", "continue" }) |mode| {
        var recorder: LogRecorder = .{ .arena = .init(testing.allocator), .min = .warn };
        defer recorder.arena.deinit();
        const inst = try create(testing.allocator, UnfinishedWf.registered, .{ .logger = recorder.sink() });
        defer inst.destroy();

        try startUnfinished(inst, a, "Unfinished", .{mode}, &unfinished_signals, &unfinished_updates);
        try testing.expectEqual(@as(usize, 0), recorder.count);
        const end: act_pb.WorkflowActivationJob.variant_union = if (std.mem.eql(u8, mode, "cancel")) .{ .cancel_workflow = .{} } else .{ .fire_timer = .{ .seq = 1 } };
        const cmds = commandsOf(try inst.activate(&try oneJob(a, end), a));
        const want: std.meta.Tag(cmd_pb.WorkflowCommand.variant_union) = if (std.mem.eql(u8, mode, "cancel"))
            .cancel_workflow_execution
        else if (std.mem.eql(u8, mode, "continue"))
            .continue_as_new_workflow_execution
        else
            .complete_workflow_execution;
        try testing.expectEqual(want, std.meta.activeTag(cmds[cmds.len - 1].variant.?));
        try testing.expectEqual(@as(usize, 1), recorder.count);
        try testing.expect(std.mem.indexOf(u8, recorder.message, "allHandlersFinished") != null);
        try testing.expectEqualStrings("slow (ID u-slow), other (ID u-other)", recorder.get("UpdateHandlers").?);
        try testing.expectEqualStrings("wait, ping, by_name", recorder.get("SignalNames").?);
    }
}

test "unfinished handlers: a failed workflow logs no warning, nor does one whose running handlers all opted out" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();

    for ([_][]const u8{ "fail", "quiet" }) |mode| {
        var recorder: LogRecorder = .{ .arena = .init(testing.allocator), .min = .warn };
        defer recorder.arena.deinit();
        const inst = try create(testing.allocator, UnfinishedWf.registered, .{ .logger = recorder.sink() });
        defer inst.destroy();

        try startUnfinished(inst, a, "Unfinished", .{mode}, &unfinished_signals, &unfinished_updates);
        const cmds = commandsOf(try inst.activate(&try oneJob(a, .{ .fire_timer = .{ .seq = 1 } }), a));
        const want: std.meta.Tag(cmd_pb.WorkflowCommand.variant_union) = if (std.mem.eql(u8, mode, "fail")) .fail_workflow_execution else .complete_workflow_execution;
        try testing.expectEqual(want, std.meta.activeTag(cmds[cmds.len - 1].variant.?));
        try testing.expectEqual(@as(usize, 0), recorder.count);
    }
}

test "unfinished handlers: handlers that already returned aren't named" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    var recorder: LogRecorder = .{ .arena = .init(testing.allocator), .min = .warn };
    defer recorder.arena.deinit();
    const inst = try create(testing.allocator, UnfinishedWf.registered, .{ .logger = recorder.sink() });
    defer inst.destroy();

    try startUnfinished(inst, a, "Unfinished", .{"complete"}, &.{"wait"}, &.{"slow"});
    // The handlers' timers fire first; the workflow's then completes it.
    var act = try oneJob(a, .{ .fire_timer = .{ .seq = 2 } });
    try act.jobs.append(a, .{ .variant = .{ .fire_timer = .{ .seq = 3 } } });
    _ = try inst.activate(&act, a);
    try testing.expect(inst.ctx.allHandlersFinished());
    try testing.expectEqualStrings("\"done\"", completedWith(try inst.activate(&try oneJob(a, .{ .fire_timer = .{ .seq = 1 } }), a)));
    try testing.expectEqual(@as(usize, 0), recorder.count);
}

test "unfinished handlers: a struct workflow's declared handlers opt out with HandlerOptions" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    var recorder: LogRecorder = .{ .arena = .init(testing.allocator), .min = .warn };
    defer recorder.arena.deinit();
    const inst = try create(testing.allocator, DeclaredUnfinishedWf.registered, .{ .logger = recorder.sink() });
    defer inst.destroy();

    try startUnfinished(inst, a, "DeclaredUnfinished", .{}, &.{ "wait", "loud" }, &.{ "slow", "other" });
    try testing.expectEqualStrings("\"done\"", completedWith(try inst.activate(&try oneJob(a, .{ .fire_timer = .{ .seq = 1 } }), a)));
    try testing.expectEqual(@as(usize, 1), recorder.count);
    try testing.expectEqualStrings("", recorder.get("UpdateHandlers").?);
    try testing.expectEqualStrings("loud", recorder.get("SignalNames").?);
}

// Read-only query handlers and update validators, and the dynamic query handler.
const ReadOnlyWf = struct {
    const StartActivity = workflow.QueryDef("start_activity", fn () []const u8);
    const Sleep = workflow.QueryDef("sleep", fn () []const u8);
    const StartChild = workflow.QueryDef("start_child", fn () []const u8);
    const AwaitDone = workflow.QueryDef("await_done", fn () []const u8);
    const GetPending = workflow.QueryDef("get_pending", fn () []const u8);
    const Memo = workflow.QueryDef("memo", fn () []const u8);
    const Inline = workflow.QueryDef("inline", fn () i64);
    const Set = workflow.UpdateDef("set", fn (i64) i64);
    const Done = workflow.SignalDef("done", fn () void);

    const State = struct {
        ctx: *workflow.Context,
        value: i64 = 0,
        done: bool = false,
        pending: ?workflow.ActivityFuture([]const u8) = null,

        // Each of these swallows the refused call's error: the query fails anyway.
        fn startActivity(s: *State) []const u8 {
            _ = s.ctx.executeActivity(Greet, .{ .start_to_close_timeout = .fromSeconds(10) }, .{"query"}).get() catch {};
            return "ignored the error";
        }
        fn sleep(s: *State) []const u8 {
            s.ctx.sleep(.fromSeconds(1), .{}) catch {};
            return "slept";
        }
        fn startChild(s: *State) []const u8 {
            _ = s.ctx.executeChildWorkflow(ChildWf.Kid, .{}, .{"query"}).get() catch {};
            return "started";
        }
        fn awaitDone(s: *State) ![]const u8 {
            try s.ctx.await(s, State.finished);
            return "done";
        }
        fn getPending(s: *State) ![]const u8 {
            return s.pending.?.get();
        }
        fn memo(s: *State) ![]const u8 {
            try s.ctx.upsertMemo(.{ .owner = "query" });
            return "upserted";
        }
        /// `io.async` runs its function inline in a query handler, so this is allowed.
        fn inlineAsync(s: *State) i64 {
            var f = s.ctx.io().async(double, .{s.value});
            return f.await(s.ctx.io());
        }
        fn double(x: i64) i64 {
            return 2 * x;
        }
        fn anyQuery(s: *State, name: []const u8, args: converter.EncodedValues) ![]const u8 {
            const a = s.ctx.allocator();
            if (std.mem.eql(u8, name, "dynamic_fails")) return error.NoAnswer;
            if (std.mem.eql(u8, name, "dynamic_sleep")) try s.ctx.sleep(.fromSeconds(1), .{});
            const arg = if (args.payloads.len > 0) try args.get([]const u8, a, 0) else "none";
            return std.fmt.allocPrint(a, "{s}({s}) value={d}", .{ name, arg, s.value });
        }

        fn set(s: *State, value: i64) i64 {
            defer s.value = value;
            return s.value;
        }
        /// Rejects updates by sleeping, for a negative value, or by starting an activity, for
        /// zero; accepts the rest.
        fn validateSet(s: *State, value: i64) !void {
            if (value < 0) try s.ctx.sleep(.fromSeconds(1), .{});
            if (value == 0) _ = s.ctx.executeActivity(Greet, .{ .start_to_close_timeout = .fromSeconds(10) }, .{"validator"});
        }
        fn finish(s: *State) void {
            s.done = true;
        }
        fn finished(s: *State) bool {
            return s.done;
        }
    };

    /// Starts an activity it never awaits (sequence 1), waits for `done`, then starts another
    /// activity and a timer and returns the value. Refused calls must not use up a sequence
    /// number, so those are activity 2 and timer 1.
    fn run(ctx: *workflow.Context) !i64 {
        const s = try ctx.allocator().create(State);
        s.* = .{ .ctx = ctx };
        try ctx.setQueryHandler(StartActivity, s, State.startActivity);
        try ctx.setQueryHandler(Sleep, s, State.sleep);
        try ctx.setQueryHandler(StartChild, s, State.startChild);
        try ctx.setQueryHandler(AwaitDone, s, State.awaitDone);
        try ctx.setQueryHandler(GetPending, s, State.getPending);
        try ctx.setQueryHandler(Memo, s, State.memo);
        try ctx.setQueryHandler(Inline, s, State.inlineAsync);
        try ctx.setDynamicQueryHandler(s, State.anyQuery);
        try ctx.setUpdateHandlerWithValidator(Set, s, State.set, State.validateSet);
        try ctx.setSignalHandler(Done, s, State.finish);
        s.pending = ctx.executeActivity(Greet, .{ .start_to_close_timeout = .fromSeconds(10) }, .{"workflow"});
        try ctx.await(s, State.finished);
        _ = ctx.executeActivity(Greet, .{ .start_to_close_timeout = .fromSeconds(10) }, .{"after"});
        try ctx.sleep(.fromSeconds(5), .{});
        return s.value;
    }

    fn dynamicOnStack(ctx: *workflow.Context) ![]const u8 {
        var s: State = .{ .ctx = ctx };
        ctx.setDynamicQueryHandler(&s, State.anyQuery) catch |err| return @errorName(err);
        return "accepted";
    }
};
const ReadOnlyWfDef = workflow.Def("ReadOnly", ReadOnlyWf.run);
const DynamicOnStackWf = workflow.Def("DynamicOnStack", ReadOnlyWf.dynamicOnStack);

/// Checks that a query or update failed for calling `call` read-only.
fn expectReadOnlyViolation(f: Failure, what: []const u8, name: []const u8, call: []const u8) !void {
    try testing.expectEqualStrings("ReadOnlyViolation", failure_mod.applicationType(f).?);
    const prefix = try std.fmt.allocPrint(testing.allocator, "{s} for \"{s}\" called {s}, ", .{ what, name, call });
    defer testing.allocator.free(prefix);
    if (!std.mem.startsWith(u8, f.message, prefix)) {
        std.debug.print("message: {s}\nexpected prefix: {s}\n", .{ f.message, prefix });
        return error.TestUnexpectedResult;
    }
}

const RenamingValidator = struct {
    const Rename = workflow.UpdateDef("rename", fn () void);
    fn handle(_: *workflow.Context) void {}
    fn validate(ctx: *workflow.Context) !void {
        try ctx.setCoroutineName("validator");
    }
    /// Registers `rename`, whose validator names its coroutine, sleeps 10 s, and returns the
    /// workflow coroutine's name.
    fn run(ctx: *workflow.Context) ![]const u8 {
        try ctx.setUpdateHandlerWithValidator(Rename, ctx, handle, validate);
        try ctx.io().sleep(.fromSeconds(10), .awake);
        return ctx.coroutineName();
    }
};
const RenamingValidatorWf = workflow.Def("RenamingValidator", RenamingValidator.run);

test "update: setCoroutineName is refused in a validator run in a coroutine, and does nothing in one run on delivery" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, RenamingValidatorWf.registered, .{});
    defer inst.destroy();

    // Sent with the start: the validator runs in the workflow coroutine as it registers.
    var start = try initActivation(a, "RenamingValidator", .{});
    try start.jobs.append(a, .{ .variant = try updateJob(a, "u1", "rename", .{}, true) });
    const first = commandsOf(try inst.activate(&start, a));
    try expectReadOnlyViolation((try updateResponse(first[0], "u1")).rejected, "update validator", "rename", "setCoroutineName");
    // Delivered later: the validator runs outside any coroutine, and the update goes on.
    const later = commandsOf(try inst.activate(&try oneJob(a, try updateJob(a, "u2", "rename", .{}, true)), a));
    try testing.expectEqualStrings("u2:accepted u2:completed()", try updateResponses(a, later));
    try testing.expectEqualStrings("\"workflow\"", completedWith(try inst.activate(&try oneJob(a, .{ .fire_timer = .{ .seq = 1 } }), a)));
}

test "query: a dynamic handler answers queries with no handler of their own, with the name and encoded arguments" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, ReadOnlyWfDef.registered, .{});
    defer inst.destroy();
    _ = try inst.activate(&try initActivation(a, "ReadOnly", .{}), a);

    const c = try inst.activate(&try queryActivation(a, &.{
        .{ "q1", "anything", (try converter.toPayloads(a, .{"arg"})).items },
        .{ "q2", "other", &.{} },
        .{ "q3", "inline", &.{} },
        .{ "q4", "dynamic_fails", &.{} },
        .{ "q5", "anything", (try converter.toPayloads(a, .{@as(i64, 5)})).items },
    }), a);
    try testing.expectEqual(@as(usize, 5), commandsOf(c).len);
    try testing.expectEqualStrings("\"anything(arg) value=0\"", answer(c, 0).variant.?.succeeded.response.?.data);
    try testing.expectEqualStrings("\"other(none) value=0\"", answer(c, 1).variant.?.succeeded.response.?.data);
    // A registered handler still answers its own query type.
    try testing.expectEqualStrings("0", answer(c, 2).variant.?.succeeded.response.?.data);
    // The dynamic handler's error, or an argument it can't decode, fails the query.
    const failed = answer(c, 3).variant.?.failed;
    try testing.expectEqualStrings("NoAnswer", failure_mod.applicationType(failed).?);
    try testing.expect(answer(c, 4).variant.? == .failed);
}

test "query: a dynamic handler context on the workflow coroutine's stack is rejected" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, DynamicOnStackWf.registered, .{});
    defer inst.destroy();
    const done = commandsOf(try inst.activate(&try initActivation(a, "DynamicOnStack", .{}), a));
    try testing.expectEqualStrings("\"QueryContextOnStack\"", done[0].variant.?.complete_workflow_execution.result.?.data);
    try testing.expect(inst.dynamic_query_handler == null);
    // Without a dynamic handler an unknown query fails as before.
    const c = try inst.activate(&try queryActivation(a, &.{.{ "q1", "nope", &.{} }}), a);
    try testing.expectEqualStrings("QueryNotRegistered", failure_mod.applicationType(answer(c, 0).variant.?.failed).?);
}

test "query: a handler that starts a command or blocks fails the query naming the call, and nothing is sent" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, ReadOnlyWfDef.registered, .{});
    defer inst.destroy();
    const first = commandsOf(try inst.activate(&try initActivation(a, "ReadOnly", .{}), a));
    try testing.expectEqual(@as(usize, 1), first.len);
    try testing.expectEqual(@as(u32, 1), first[0].variant.?.schedule_activity.seq);

    const c = try inst.activate(&try queryActivation(a, &.{
        .{ "q1", "start_activity", &.{} },
        .{ "q2", "sleep", &.{} },
        .{ "q3", "start_child", &.{} },
        .{ "q4", "await_done", &.{} },
        .{ "q5", "get_pending", &.{} },
        .{ "q6", "memo", &.{} },
        .{ "q7", "dynamic_sleep", &.{} },
        .{ "q8", "inline", &.{} },
    }), a);
    try testing.expect(c.status.? == .successful); // the workflow task doesn't fail
    const cmds = commandsOf(c);
    try testing.expectEqual(@as(usize, 8), cmds.len); // the answers, and nothing else
    for (cmds) |cmd| try testing.expect(cmd.variant.? == .respond_to_query);
    try expectReadOnlyViolation(answer(c, 0).variant.?.failed, "query handler", "start_activity", "executeActivity");
    try expectReadOnlyViolation(answer(c, 1).variant.?.failed, "query handler", "sleep", "sleep");
    try expectReadOnlyViolation(answer(c, 2).variant.?.failed, "query handler", "start_child", "executeChildWorkflow");
    try expectReadOnlyViolation(answer(c, 3).variant.?.failed, "query handler", "await_done", "await");
    try expectReadOnlyViolation(answer(c, 4).variant.?.failed, "query handler", "get_pending", "ActivityFuture.get");
    try expectReadOnlyViolation(answer(c, 5).variant.?.failed, "query handler", "memo", "upsertMemo");
    try expectReadOnlyViolation(answer(c, 6).variant.?.failed, "query handler", "dynamic_sleep", "sleep");
    try testing.expectEqualStrings("0", answer(c, 7).variant.?.succeeded.response.?.data);
    try testing.expectEqual(@as(usize, 0), inst.memo.len);

    // The refused calls used up no sequence numbers, and the run goes on as before.
    const after = commandsOf(try inst.activate(&try oneJob(a, try signalJob(a, "done", .{})), a));
    try testing.expectEqual(@as(usize, 2), after.len);
    try testing.expectEqual(@as(u32, 2), after[0].variant.?.schedule_activity.seq);
    try testing.expectEqual(@as(u32, 1), after[1].variant.?.start_timer.seq);
}

test "update: a validator that starts a command or blocks rejects the update naming the call, and nothing is sent" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, ReadOnlyWfDef.registered, .{});
    defer inst.destroy();
    _ = try inst.activate(&try initActivation(a, "ReadOnly", .{}), a);

    var act = try oneJob(a, try updateJob(a, "u1", "set", .{@as(i64, -1)}, true));
    try act.jobs.append(a, .{ .variant = try updateJob(a, "u2", "set", .{@as(i64, 0)}, true) });
    const c = try inst.activate(&act, a);
    try testing.expect(c.status.? == .successful);
    const cmds = commandsOf(c);
    try testing.expectEqual(@as(usize, 2), cmds.len); // the rejections, and nothing else
    try expectReadOnlyViolation((try updateResponse(cmds[0], "u1")).rejected, "update validator", "set", "sleep");
    try expectReadOnlyViolation((try updateResponse(cmds[1], "u2")).rejected, "update validator", "set", "executeActivity");

    // A validator that only reads accepts; the run's sequence numbers are untouched.
    const ok = commandsOf(try inst.activate(&try oneJob(a, try updateJob(a, "u3", "set", .{@as(i64, 4)}, true)), a));
    try testing.expectEqual(@as(usize, 2), ok.len);
    try testing.expect(try updateResponse(ok[0], "u3") == .accepted);
    try testing.expectEqualStrings("0", (try updateResponse(ok[1], "u3")).completed.data);
    const after = commandsOf(try inst.activate(&try oneJob(a, try signalJob(a, "done", .{})), a));
    try testing.expectEqual(@as(u32, 2), after[0].variant.?.schedule_activity.seq);
    try testing.expectEqual(@as(u32, 1), after[1].variant.?.start_timer.seq);
}

// A query handler or update validator that registers a signal handler while a signal of
// that name is buffered would start the handler's coroutine: a coroutine (and the commands
// it emits) that only exists because of a query, which history doesn't record.
const LazySignalWf = struct {
    const Register = workflow.QueryDef("register", fn () []const u8);
    const Check = workflow.UpdateDef("check", fn (i64) i64);
    const Ping = workflow.SignalDef("ping", fn () void);

    const State = struct {
        ctx: *workflow.Context,
        fn register(s: *State) []const u8 {
            s.ctx.setSignalHandler(Ping, s, State.onPing) catch {};
            return "registered";
        }
        fn check(s: *State, v: i64) i64 {
            _ = s;
            return v;
        }
        fn validateCheck(s: *State, v: i64) !void {
            _ = v;
            try s.ctx.setSignalHandler(Ping, s, State.onPing);
        }
        fn onPing(s: *State) void {
            _ = s.ctx.executeActivity(Greet, .{ .start_to_close_timeout = .fromSeconds(10) }, .{"ping"});
        }
    };

    fn run(ctx: *workflow.Context) !i64 {
        const s = try ctx.allocator().create(State);
        s.* = .{ .ctx = ctx };
        try ctx.setQueryHandler(Register, s, State.register);
        try ctx.setUpdateHandlerWithValidator(Check, s, State.check, State.validateCheck);
        try ctx.sleep(.fromSeconds(60), .{});
        return 1;
    }
};
const LazySignalWfDef = workflow.Def("LazySignal", LazySignalWf.run);

test "query: a handler that registers a signal handler for a buffered signal fails the query and starts no handler" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, LazySignalWfDef.registered, .{});
    defer inst.destroy();
    const timer = commandsOf(try inst.activate(&try initActivation(a, "LazySignal", .{}), a))[0].variant.?.start_timer;
    // No handler for "ping" yet: it is buffered.
    try testing.expectEqual(@as(usize, 0), commandsOf(try inst.activate(&try oneJob(a, try signalJob(a, "ping", .{})), a)).len);
    try testing.expectEqual(@as(usize, 1), inst.buffered_signals.items.len);

    const c = try inst.activate(&try queryActivation(a, &.{.{ "q1", "register", &.{} }}), a);
    try testing.expect(c.status.? == .successful);
    try testing.expect(answer(c, 0).variant.? == .failed);
    try testing.expectEqualStrings("ReadOnlyViolation", failure_mod.applicationType(answer(c, 0).variant.?.failed).?);
    try testing.expectEqual(@as(usize, 1), inst.buffered_signals.items.len);

    // The query started no coroutine, so the run goes on as history says: it completes.
    const done = commandsOf(try inst.activate(&try oneJob(a, .{ .fire_timer = .{ .seq = timer.seq } }), a));
    try testing.expectEqual(@as(usize, 1), done.len);
    try testing.expect(done[0].variant.? == .complete_workflow_execution);
}

test "update: a validator that registers a signal handler for a buffered signal rejects the update and starts no handler" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, LazySignalWfDef.registered, .{});
    defer inst.destroy();
    const timer = commandsOf(try inst.activate(&try initActivation(a, "LazySignal", .{}), a))[0].variant.?.start_timer;
    try testing.expectEqual(@as(usize, 0), commandsOf(try inst.activate(&try oneJob(a, try signalJob(a, "ping", .{})), a)).len);

    const c = try inst.activate(&try oneJob(a, try updateJob(a, "u1", "check", .{@as(i64, 1)}, true)), a);
    try testing.expect(c.status.? == .successful);
    const cmds = commandsOf(c);
    try testing.expectEqual(@as(usize, 1), cmds.len); // the rejection, and nothing else
    try testing.expectEqualStrings("ReadOnlyViolation", failure_mod.applicationType((try updateResponse(cmds[0], "u1")).rejected).?);
    try testing.expectEqual(@as(usize, 1), inst.buffered_signals.items.len);

    const done = commandsOf(try inst.activate(&try oneJob(a, .{ .fire_timer = .{ .seq = timer.seq } }), a));
    try testing.expectEqual(@as(usize, 1), done.len);
    try testing.expect(done[0].variant.? == .complete_workflow_execution);
}

// A handler that registers an update handler while an update of that name is buffered runs
// the validator inline, in the handler's own coroutine. A validator there that waits where
// no wait can be refused (an uncancelable `std.Io` wait) blocks that coroutine: the
// read-only section must not outlive it and govern the rest of the run's coroutines.
const BlockedValidatorWf = struct {
    const Register = workflow.SignalDef("register", fn () void);
    const Set = workflow.UpdateDef("set", fn () void);

    const State = struct {
        ctx: *workflow.Context,
        loaded: std.Io.Event = .unset,
        fn register(s: *State) !void {
            try s.ctx.setUpdateHandlerWithValidator(Set, s, State.set, State.validate);
        }
        fn validate(s: *State) void {
            s.loaded.waitUncancelable(s.ctx.io());
        }
        fn set(_: *State) void {}
    };

    fn run(ctx: *workflow.Context) ![]const u8 {
        const s = try ctx.allocator().create(State);
        s.* = .{ .ctx = ctx };
        try ctx.setSignalHandler(Register, s, State.register);
        try ctx.sleep(.fromSeconds(1), .{});
        s.loaded.set(ctx.io());
        return ctx.executeActivity(Greet, .{ .start_to_close_timeout = .fromSeconds(10) }, .{"after"}).get();
    }
};
const BlockedValidatorWfDef = workflow.Def("BlockedValidator", BlockedValidatorWf.run);

test "update: a validator blocked in a handler's coroutine leaves the rest of the run free to emit commands" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, BlockedValidatorWfDef.registered, .{});
    defer inst.destroy();
    const timer = commandsOf(try inst.activate(&try initActivation(a, "BlockedValidator", .{}), a))[0].variant.?.start_timer;

    // u1 is buffered, then the signal handler registers its handler and runs the validator.
    var act = try oneJob(a, try updateJob(a, "u1", "set", .{}, true));
    try act.jobs.append(a, .{ .variant = try signalJob(a, "register", .{}) });
    const blocked = try inst.activate(&act, a);
    try testing.expect(blocked.status.? == .successful);
    var responded = false;
    for (commandsOf(blocked)) |c| if (c.variant.? == .update_response) {
        responded = true;
    };

    // The workflow function wakes, sets the event and starts its activity: its own code, not
    // the validator's, so the activity is scheduled.
    const fired = try inst.activate(&try oneJob(a, .{ .fire_timer = .{ .seq = timer.seq } }), a);
    try testing.expect(fired.status.? == .successful);
    var scheduled = false;
    for (commandsOf(fired)) |c| switch (c.variant.?) {
        .schedule_activity => scheduled = true,
        .update_response => responded = true,
        else => {},
    };
    try testing.expect(scheduled);
    try testing.expect(responded);
    try testing.expect(!inst.wio.read_only);
}

// A dynamic query handler gets its arguments encoded and no allocator of its own, so it
// decodes them with `ctx.allocator()`, as the README's example does. A query's memory must
// not stay in the run: a workflow queried every second would grow without bound (and, with
// `memory_limit`, fail its workflow tasks).
const DynamicMemoryWf = struct {
    const Echo = workflow.QueryDef("echo", fn ([]const u8) usize);
    const State = struct {
        ctx: *workflow.Context,
        fn echo(_: *State, text: []const u8) usize {
            return text.len;
        }
        fn anyQuery(s: *State, name: []const u8, args: converter.EncodedValues) !usize {
            _ = name;
            const text = try args.get([]const u8, s.ctx.allocator(), 0);
            return text.len;
        }
    };
    fn run(ctx: *workflow.Context) !void {
        const s = try ctx.allocator().create(State);
        s.* = .{ .ctx = ctx };
        try ctx.setQueryHandler(Echo, s, State.echo);
        try ctx.setDynamicQueryHandler(s, State.anyQuery);
        try ctx.sleep(.fromSeconds(60), .{});
    }
};
const DynamicMemoryWfDef = workflow.Def("DynamicMemory", DynamicMemoryWf.run);

test "query: a dynamic handler decoding its arguments leaves no memory in the run, like a typed handler" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, DynamicMemoryWfDef.registered, .{});
    defer inst.destroy();
    _ = try inst.activate(&try initActivation(a, "DynamicMemory", .{}), a);
    const args = (try converter.toPayloads(a, .{"a query argument"})).items;

    const before = inst.allocated;
    for (0..100) |_| {
        const c = try inst.activate(&try queryActivation(a, &.{.{ "q1", "echo", args }}), a);
        try testing.expectEqualStrings("16", answer(c, 0).variant.?.succeeded.response.?.data);
    }
    try testing.expectEqual(before, inst.allocated); // a typed handler decodes into the answer's memory
    for (0..100) |_| {
        const c = try inst.activate(&try queryActivation(a, &.{.{ "q1", "anything", args }}), a);
        try testing.expectEqualStrings("16", answer(c, 0).variant.?.succeeded.response.?.data);
    }
    try testing.expectEqual(before, inst.allocated);
}

// `ctx.fail` copies its failure's cause chain into `ctx.allocator()` and records it as the
// workflow's pending failure. In a query handler `ctx.allocator()` is the answer's memory,
// freed once the answer is sent, so the run must not keep a failure that lives there.
const QueryFailWf = struct {
    const Describe = workflow.QueryDef("describe", fn () []const u8);
    const Go = workflow.SignalDef("go", fn () void);
    const State = struct {
        ctx: *workflow.Context,
        go: bool = false,
        fn describe(s: *State) ![]const u8 {
            const cause: Failure = .{ .message = "query cause" };
            return s.ctx.fail(.{ .message = "query failure", .cause = &cause });
        }
        fn onGo(s: *State) void {
            s.go = true;
        }
        fn ready(s: *State) bool {
            return s.go;
        }
    };
    fn run(ctx: *workflow.Context) !void {
        const s = try ctx.allocator().create(State);
        s.* = .{ .ctx = ctx };
        try ctx.setQueryHandler(Describe, s, State.describe);
        try ctx.setSignalHandler(Go, s, State.onGo);
        try ctx.await(s, State.ready);
        return error.Boom;
    }
};
const QueryFailWfDef = workflow.Def("QueryFail", QueryFailWf.run);

test "query: ctx.fail in a query handler leaves the run no failure in the query's freed memory" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, QueryFailWfDef.registered, .{});
    defer inst.destroy();
    _ = try inst.activate(&try initActivation(a, "QueryFail", .{}), a);

    // The query's activation memory, reused (here: zeroed) once its completion is sent.
    var query_buffer: [64 * 1024]u8 = undefined;
    var query_memory: std.heap.FixedBufferAllocator = .init(&query_buffer);
    const c = try inst.activate(&try queryActivation(a, &.{.{ "q1", "describe", &.{} }}), query_memory.allocator());
    try testing.expect(answer(c, 0).variant.? == .failed);
    @memset(&query_buffer, 0);

    // The workflow then fails on its own. Whatever failure it reports must be intact.
    const done = commandsOf(try inst.activate(&try oneJob(a, try signalJob(a, "go", .{})), a));
    try testing.expectEqual(@as(usize, 1), done.len);
    const f = done[0].variant.?.fail_workflow_execution.failure.?;
    if (f.cause) |cause| try testing.expectEqualStrings("query cause", cause.message);
}

// A signal channel's `receive` and `tryReceive` take a signal from the channel. A query
// handler that calls one while signals are unread must not take them: the workflow would
// never see the signal live, but would on replay, where there is no query.
const ChannelQueryWf = struct {
    const Receive = workflow.QueryDef("receive", fn () []const u8);
    const TryReceive = workflow.QueryDef("try_receive", fn () []const u8);
    const Msg = workflow.SignalDef("msg", fn ([]const u8) void);
    const State = struct {
        ch: *workflow.SignalChannel(Msg.Args),
        fn receive(s: *State) ![]const u8 {
            return s.ch.receive();
        }
        fn tryReceive(s: *State) []const u8 {
            return s.ch.tryReceive() orelse "none";
        }
    };
    fn run(ctx: *workflow.Context) ![]const u8 {
        const s = try ctx.allocator().create(State);
        s.* = .{ .ch = try ctx.signalChannel(Msg) };
        try ctx.setQueryHandler(Receive, s, State.receive);
        try ctx.setQueryHandler(TryReceive, s, State.tryReceive);
        try ctx.sleep(.fromSeconds(60), .{});
        const first = s.ch.tryReceive() orelse "none";
        const second = s.ch.tryReceive() orelse "none";
        return std.fmt.allocPrint(ctx.allocator(), "{s},{s}", .{ first, second });
    }
};
const ChannelQueryWfDef = workflow.Def("ChannelQuery", ChannelQueryWf.run);

test "query: a handler that receives from a signal channel leaves its signals for the workflow" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, ChannelQueryWfDef.registered, .{});
    defer inst.destroy();
    const timer = commandsOf(try inst.activate(&try initActivation(a, "ChannelQuery", .{}), a))[0].variant.?.start_timer;
    var signals = try oneJob(a, try signalJob(a, "msg", .{"a"}));
    try signals.jobs.append(a, .{ .variant = try signalJob(a, "msg", .{"b"}) });
    try testing.expectEqual(@as(usize, 0), commandsOf(try inst.activate(&signals, a)).len);

    const c = try inst.activate(&try queryActivation(a, &.{
        .{ "q1", "receive", &.{} },
        .{ "q2", "try_receive", &.{} },
    }), a);
    try testing.expect(c.status.? == .successful);

    // As on replay, where no query ran, the workflow reads both signals.
    const done = commandsOf(try inst.activate(&try oneJob(a, .{ .fire_timer = .{ .seq = timer.seq } }), a));
    try testing.expectEqual(@as(usize, 1), done.len);
    try testing.expectEqualStrings("\"a,b\"", done[0].variant.?.complete_workflow_execution.result.?.data);
}

// The rest of the read-only invariant: what read-only code may use for itself (`ctx.fail`,
// zero sleeps, random draws) works and leaves the run as it was; anything else that changes
// SDK state (waking a coroutine, the versioning behavior, a task failure) is refused.
const ReadOnlyStateWf = struct {
    const Q = workflow.QueryDef("q", fn ([]const u8) []const u8);
    const State = struct {
        ctx: *workflow.Context,
        ev: std.Io.Event = .unset,
        fn q(s: *State, what: []const u8) ![]const u8 {
            const ctx = s.ctx;
            const a = ctx.allocator();
            if (std.mem.eql(u8, what, "fail")) return ctx.fail(.{ .message = "chosen by the query", .type = "QueryChose" });
            if (std.mem.eql(u8, what, "zero_sleep")) {
                try ctx.sleep(.fromSeconds(0), .{});
                _ = try ctx.awaitTimeout(.fromSeconds(0), s, State.never);
                return "slept";
            }
            if (std.mem.eql(u8, what, "random")) {
                var x: u64 = undefined;
                var y: u64 = undefined;
                ctx.io().random(std.mem.asBytes(&x));
                ctx.io().random(std.mem.asBytes(&y));
                return std.fmt.allocPrint(a, "{d},{d}", .{ x, y });
            }
            if (std.mem.eql(u8, what, "set_event")) {
                s.ev.set(ctx.io());
                return "set";
            }
            if (std.mem.eql(u8, what, "versioning")) {
                ctx.setVersioningBehavior(.pinned);
                return "set";
            }
            if (std.mem.eql(u8, what, "version")) {
                _ = try ctx.getVersion("change", workflow.default_version, 1);
                return "supported";
            }
            return error.UnknownWhat;
        }
        fn never(_: *State) bool {
            return false;
        }
        fn anyQuery(_: *State, name: []const u8, _: converter.EncodedValues) []const u8 {
            return name;
        }
        fn release(s: *State) void {
            s.ev.set(s.ctx.io());
        }
    };
    const Release = workflow.SignalDef("release", fn () void);
    fn run(ctx: *workflow.Context) !u64 {
        const s = try ctx.allocator().create(State);
        s.* = .{ .ctx = ctx };
        try ctx.setQueryHandler(Q, s, State.q);
        try ctx.setDynamicQueryHandler(s, State.anyQuery);
        try ctx.setSignalHandler(Release, s, State.release);
        _ = try ctx.getVersion("change", workflow.default_version, 2);
        try s.ev.wait(ctx.io());
        var x: u64 = undefined;
        ctx.io().random(std.mem.asBytes(&x));
        return x;
    }
};
const ReadOnlyStateWfDef = workflow.Def("ReadOnlyState", ReadOnlyStateWf.run);

fn readOnlyQuery(a: Allocator, inst: *Instance, what: []const u8) !done_pb.WorkflowActivationCompletion {
    const c = try inst.activate(&try queryActivation(a, &.{.{ "q", "q", (try converter.toPayloads(a, .{what})).items }}), a);
    try testing.expect(c.status.? == .successful);
    try testing.expectEqual(@as(usize, 1), commandsOf(c).len);
    return c;
}

test "query: read-only code keeps the run's state as replay sees it" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, ReadOnlyStateWfDef.registered, .{ .send_versioning_behavior = true });
    defer inst.destroy();
    _ = try inst.activate(&try initActivation(a, "ReadOnlyState", .{}), a);
    const rng_before = inst.wio.rng;

    // ctx.fail describes the query's failure, not the run's.
    const failed = answer(try readOnlyQuery(a, inst, "fail"), 0).variant.?.failed;
    try testing.expectEqualStrings("chosen by the query", failed.message);
    try testing.expectEqualStrings("QueryChose", failure_mod.applicationType(failed).?);
    try testing.expect(inst.ctx.pending_failure == null);

    try testing.expectEqualStrings("\"slept\"", answer(try readOnlyQuery(a, inst, "zero_sleep"), 0).variant.?.succeeded.response.?.data);

    // Two draws differ, and the same query draws the same again: the generator is restored.
    const draws = answer(try readOnlyQuery(a, inst, "random"), 0).variant.?.succeeded.response.?.data;
    var parts = std.mem.splitScalar(u8, std.mem.trim(u8, draws, "\""), ',');
    try testing.expect(!std.mem.eql(u8, parts.next().?, parts.next().?));
    try testing.expectEqualStrings(draws, answer(try readOnlyQuery(a, inst, "random"), 0).variant.?.succeeded.response.?.data);
    try testing.expectEqual(rng_before.s, inst.wio.rng.s);

    const woke = answer(try readOnlyQuery(a, inst, "set_event"), 0).variant.?.failed;
    try expectReadOnlyViolation(woke, "query handler", "q", "a futex wake that resumes a workflow coroutine (std.Io.Event.set, Mutex.unlock, Queue.put, ...)");
    try testing.expect(!inst.wio.read_only);

    try expectReadOnlyViolation(answer(try readOnlyQuery(a, inst, "versioning"), 0).variant.?.failed, "query handler", "q", "setVersioningBehavior");
    try testing.expectEqual(workflow.VersioningBehavior.unspecified, inst.versioning_behavior);

    // An unsupported version fails the query, not the workflow task.
    try testing.expectEqualStrings("UnsupportedVersion", answer(try readOnlyQuery(a, inst, "version"), 0).variant.?.failed.message);

    // Reserved query types don't go to the dynamic handler: the worker answers the stack
    // trace query itself, here for a workflow function the refused set woke, which hasn't
    // run since.
    const c = try inst.activate(&try queryActivation(a, &.{
        .{ "q1", "__stack_trace", &.{} },
        .{ "q2", "__temporal_workflow_metadata", &.{} },
        .{ "q3", "custom", &.{} },
    }), a);
    const trace = try converter.fromPayload([]const u8, a, answer(c, 0).variant.?.succeeded.response.?);
    try testing.expect(std.mem.startsWith(u8, trace, "workflow \"ReadOnlyState\": 1 coroutine\n\ncoroutine 1 \"workflow\", the workflow function, is ready to run\n"));
    try testing.expectEqualStrings("QueryNotRegistered", failure_mod.applicationType(answer(c, 1).variant.?.failed).?);
    try testing.expectEqualStrings("\"custom\"", answer(c, 2).variant.?.succeeded.response.?.data);

    // The workflow (woken by the query's refused set, or by this signal's) draws the number
    // replay draws: the queries' draws left the run's generator alone.
    const done = commandsOf(try inst.activate(&try oneJob(a, try signalJob(a, "release", .{})), a));
    try testing.expectEqual(@as(usize, 1), done.len);
    var expected_rng = rng_before;
    var expected: u64 = undefined;
    expected_rng.fill(std.mem.asBytes(&expected));
    try testing.expectEqualStrings(try std.fmt.allocPrint(a, "{d}", .{expected}), done[0].variant.?.complete_workflow_execution.result.?.data);
}

test "update: a validator blocked in a handler's coroutine is rejected once it wakes, and others see no update" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, BlockedValidatorWfDef.registered, .{});
    defer inst.destroy();
    const timer = commandsOf(try inst.activate(&try initActivation(a, "BlockedValidator", .{}), a))[0].variant.?.start_timer;
    var act = try oneJob(a, try updateJob(a, "u1", "set", .{}, true));
    try act.jobs.append(a, .{ .variant = try signalJob(a, "register", .{}) });
    _ = try inst.activate(&act, a);
    // The validator's section is open, but only for its coroutine.
    try testing.expect(inst.wio.read_only);
    try testing.expect(inst.readOnly() == null);
    try testing.expect(inst.currentUpdate() == null);

    const fired = commandsOf(try inst.activate(&try oneJob(a, .{ .fire_timer = .{ .seq = timer.seq } }), a));
    var rejected: ?Failure = null;
    for (fired) |c| if (c.variant.? == .update_response) {
        rejected = (try updateResponse(c, "u1")).rejected;
    };
    try expectReadOnlyViolation(rejected.?, "update validator", "set", "a wait that can't be refused (an uncancelable std.Io wait, Future.await, Group.cancel)");
    try testing.expect(!inst.wio.read_only);
}

// The workflow waits on a std.Io.Condition; a query signals it (refused), then a signal
// handler signals it for real.
const ReadOnlyConditionWf = struct {
    const Q = workflow.QueryDef("poke", fn () []const u8);
    const State = struct {
        ctx: *workflow.Context,
        mutex: std.Io.Mutex = .init,
        cond: std.Io.Condition = .init,
        released: bool = false,
        fn poke(s: *State) []const u8 {
            s.cond.signal(s.ctx.io());
            return "poked";
        }
        fn release(s: *State) !void {
            try s.mutex.lock(s.ctx.io());
            defer s.mutex.unlock(s.ctx.io());
            s.released = true;
            s.cond.signal(s.ctx.io());
        }
    };
    const Release = workflow.SignalDef("release", fn () void);
    fn run(ctx: *workflow.Context) ![]const u8 {
        const s = try ctx.allocator().create(State);
        s.* = .{ .ctx = ctx };
        try ctx.setQueryHandler(Q, s, State.poke);
        try ctx.setSignalHandler(Release, s, State.release);
        try s.mutex.lock(ctx.io());
        defer s.mutex.unlock(ctx.io());
        while (!s.released) try s.cond.wait(ctx.io(), &s.mutex);
        return "released";
    }
};
const ReadOnlyConditionWfDef = workflow.Def("ReadOnlyCondition", ReadOnlyConditionWf.run);

test "query: a refused Condition.signal leaves the condition so the workflow's own signal still wakes its waiter" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, ReadOnlyConditionWfDef.registered, .{});
    defer inst.destroy();
    try testing.expectEqual(@as(usize, 0), commandsOf(try inst.activate(&try initActivation(a, "ReadOnlyCondition", .{}), a)).len);

    const c = try inst.activate(&try queryActivation(a, &.{.{ "q", "poke", &.{} }}), a);
    try testing.expect(c.status.? == .successful);
    try expectReadOnlyViolation(answer(c, 0).variant.?.failed, "query handler", "poke", "a futex wake that resumes a workflow coroutine (std.Io.Event.set, Mutex.unlock, Queue.put, ...)");

    // Replay runs no query: there, this signal wakes the workflow and it completes.
    const done = commandsOf(try inst.activate(&try oneJob(a, try signalJob(a, "release", .{})), a));
    try testing.expectEqual(@as(usize, 1), done.len);
    try testing.expectEqualStrings("\"released\"", done[0].variant.?.complete_workflow_execution.result.?.data);
}

// The workflow waits on a std.Io.Queue; a query puts into it (refused), then a signal
// handler puts the value the workflow should get.
const ReadOnlyQueueWf = struct {
    const Q = workflow.QueryDef("put", fn () []const u8);
    const State = struct {
        ctx: *workflow.Context,
        buf: [4]u32 = undefined,
        queue: std.Io.Queue(u32) = undefined,
        fn put(s: *State) ![]const u8 {
            try s.queue.putOne(s.ctx.io(), 666);
            return "put";
        }
        fn send(s: *State) !void {
            try s.queue.putOne(s.ctx.io(), 7);
        }
    };
    const Send = workflow.SignalDef("send", fn () void);
    /// The latest run's state, for the test to inspect the queue.
    var last: ?*State = null;
    fn run(ctx: *workflow.Context) !u32 {
        const s = try ctx.allocator().create(State);
        s.* = .{ .ctx = ctx };
        s.queue = .init(&s.buf);
        last = s;
        try ctx.setQueryHandler(Q, s, State.put);
        try ctx.setSignalHandler(Send, s, State.send);
        return s.queue.getOne(ctx.io());
    }
};
const ReadOnlyQueueWfDef = workflow.Def("ReadOnlyQueue", ReadOnlyQueueWf.run);

// Ruled by the foreman in round 4: this test first asked that the workflow still get only
// the 7 its own code puts, as on replay. That can't be done: std.Io's Queue.put hands the
// query's 666 to the waiting getter (pops it off the getters list and copies the value into
// its buffer) under an uncontended mutex before any Io call the SDK sees, so the SDK can't
// undo it, and any rollback leaves the queue inconsistent. A handler that changes workflow
// data or a std.Io primitive is the user's error, which the SDK reports but can't take back.
// So the test now checks what the SDK does promise: the query fails with ReadOnlyViolation,
// the wake goes through so the workflow isn't left blocked, and the queue stays consistent
// (the 7 the workflow's own code puts is in the queue). It keeps its original name, which the
// bead's regression tracking looks for.
test "query: a refused Queue.put leaves the queue so the workflow gets what its own code puts" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, ReadOnlyQueueWfDef.registered, .{});
    defer inst.destroy();
    try testing.expectEqual(@as(usize, 0), commandsOf(try inst.activate(&try initActivation(a, "ReadOnlyQueue", .{}), a)).len);

    const c = try inst.activate(&try queryActivation(a, &.{.{ "q", "put", &.{} }}), a);
    try testing.expect(c.status.? == .successful);
    try expectReadOnlyViolation(answer(c, 0).variant.?.failed, "query handler", "put", "a futex wake that resumes a workflow coroutine (std.Io.Event.set, Mutex.unlock, Queue.put, ...)");
    try testing.expectEqual(@as(usize, 1), commandsOf(c).len); // the answer, and no command

    // A query-only activation runs no coroutine; the next one does. The workflow ends with the
    // value the query handed over, and the signal's 7 waits in the queue's buffer.
    const done = commandsOf(try inst.activate(&try oneJob(a, try signalJob(a, "send", .{})), a));
    try testing.expectEqual(@as(usize, 1), done.len);
    try testing.expectEqualStrings("666", done[0].variant.?.complete_workflow_execution.result.?.data);
    const q = &ReadOnlyQueueWf.last.?.queue.type_erased;
    try testing.expect(q.getters.first == null);
    try testing.expect(q.putters.first == null);
    try testing.expectEqual(@as(usize, @sizeOf(u32)), q.len); // bytes: one u32
    try testing.expectEqual(@as(u32, 7), std.mem.bytesToValue(u32, q.buffer[q.start..][0..@sizeOf(u32)]));
}

test "query: Event.set on an event a coroutine waits for wakes it and fails the query" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, ReadOnlyStateWfDef.registered, .{});
    defer inst.destroy();
    _ = try inst.activate(&try initActivation(a, "ReadOnlyState", .{}), a);

    const woke = answer(try readOnlyQuery(a, inst, "set_event"), 0).variant.?.failed;
    try expectReadOnlyViolation(woke, "query handler", "q", "a futex wake that resumes a workflow coroutine (std.Io.Event.set, Mutex.unlock, Queue.put, ...)");

    // The waiter was woken: the next activation that runs coroutines (here, one whose signal
    // has no handler) lets the workflow finish, so it doesn't hang.
    const done = commandsOf(try inst.activate(&try oneJob(a, try signalJob(a, "unrelated", .{})), a));
    try testing.expectEqual(@as(usize, 1), done.len);
    try testing.expect(done[0].variant.? == .complete_workflow_execution);
}

// Read-only code that only looks at cancellation (a zero sleep, checkCancel, recancel,
// swapCancelProtection) in a canceled workflow's coroutine must leave the cancel request to
// the workflow: the validator accepts, and the workflow is canceled as on replay.
const CancelProbeValidatorWf = struct {
    const Set = workflow.UpdateDef("set", fn () void);
    const State = struct {
        ctx: *workflow.Context,
        fn set(_: *State) void {}
        fn validate(s: *State) !void {
            const io = s.ctx.io();
            try s.ctx.sleep(.fromSeconds(0), .{});
            try io.checkCancel();
            const old = io.swapCancelProtection(.blocked);
            _ = io.swapCancelProtection(old);
            io.recancel();
        }
    };
    fn run(ctx: *workflow.Context) ![]const u8 {
        const s = try ctx.allocator().create(State);
        s.* = .{ .ctx = ctx };
        try ctx.setUpdateHandlerWithValidator(Set, s, State.set, State.validate);
        try ctx.sleep(.fromSeconds(10), .{});
        return "slept";
    }
};
const CancelProbeValidatorWfDef = workflow.Def("CancelProbeValidator", CancelProbeValidatorWf.run);

test "update: a validator that only looks at cancellation in a canceled workflow's coroutine leaves the cancel to the workflow" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, CancelProbeValidatorWfDef.registered, .{});
    defer inst.destroy();
    var act = try initActivation(a, "CancelProbeValidator", .{});
    try act.jobs.append(a, .{ .variant = try updateJob(a, "u1", "set", .{}, true) });
    try act.jobs.append(a, .{ .variant = .{ .cancel_workflow = .{} } });
    const cmds = commandsOf(try inst.activate(&act, a));
    var accepted = false;
    var canceled = false;
    for (cmds) |cmd| switch (cmd.variant.?) {
        .update_response => accepted = accepted or (try updateResponse(cmd, "u1")) == .accepted,
        .cancel_workflow_execution => canceled = true,
        .start_timer => return error.TestUnexpectedResult,
        else => {},
    };
    try testing.expect(accepted);
    try testing.expect(canceled);
}

// Child workflows. `Kid` itself never runs here: the tests resolve it.
const ChildWf = struct {
    fn kid(ctx: *workflow.Context, name: []const u8) ![]const u8 {
        return std.fmt.allocPrint(ctx.allocator(), "hi {s}", .{name});
    }
    const Kid = workflow.Def("Kid", kid);

    /// Starts `Kid` with every option set, waits for its start, then its result.
    fn run(ctx: *workflow.Context) ![]const u8 {
        const a = ctx.allocator();
        const f = ctx.executeChildWorkflow(Kid, .{
            .execution_timeout = .fromSeconds(60),
            .run_timeout = .fromSeconds(30),
            .task_timeout = .fromSeconds(5),
            .retry_policy = .{ .maximum_attempts = 2 },
            .cron_schedule = "0 * * * *",
            .parent_close_policy = .abandon,
            .id_reuse_policy = .reject_duplicate,
            .memo = try converter.toFields(a, .{ .owner = "parent" }),
            .search_attributes = &.{ sa.key(.keyword, "Owner").set("parent"), sa.key(.int, "Skipped").unset() },
            .priority = .{ .priority_key = 2, .fairness_key = "tenant-b" },
        }, .{"kid"});
        const exec = f.started() catch |err| switch (err) {
            error.ChildWorkflowAlreadyStarted => return std.fmt.allocPrint(a, "already started: {s}", .{f.failure().?.message}),
            else => return err,
        };
        if (!std.mem.eql(u8, exec.workflow_id, ctx.info().workflow_id) and f.isReady()) return error.ReadyTooSoon;
        return std.fmt.allocPrint(a, "{s} {s}: {s}", .{ exec.workflow_id, exec.run_id, try f.get() });
    }
    const Run = workflow.Def("RunChild", run);

    /// Waits on `Kid` with the cancellation type named by `mode`; if `mode` ends in "+future",
    /// cancels it through the future once it started. Sleeps after a result, which a re-armed
    /// cancel interrupts.
    fn cancel(ctx: *workflow.Context, mode: []const u8) ![]const u8 {
        const via_future = std.mem.endsWith(u8, mode, "+future");
        const name = if (via_future) mode[0 .. mode.len - "+future".len] else mode;
        const cancellation_type = std.meta.stringToEnum(workflow.ChildWorkflowCancellationType, name) orelse return error.InvalidPayload;
        const f = ctx.executeChildWorkflow(Kid, .{ .id = "kid-1", .cancellation_type = cancellation_type }, .{"kid"});
        if (via_future) {
            _ = try f.started();
            f.cancel();
        }
        const r = f.get() catch |err| switch (err) {
            error.ChildWorkflowFailed => return switch (failure_mod.rootCause(f.failure().?).failure_info.?) {
                .canceled_failure_info => "child canceled",
                else => "child failed",
            },
            else => return err,
        };
        try ctx.io().sleep(.fromSeconds(1), .awake);
        return r;
    }
    const Cancel = workflow.Def("CancelChild", cancel);

    fn byName(ctx: *workflow.Context) ![]const u8 {
        return ctx.executeChildWorkflowByName([]const u8, "", .{}, .{}).get();
    }
    const ByName = workflow.Def("ByName", byName);

    /// Starts `Kid` with the given static summary and details and returns its result.
    fn described(ctx: *workflow.Context, summary: []const u8, details: []const u8) ![]const u8 {
        return ctx.executeChildWorkflow(Kid, .{ .id = "kid-1", .static_summary = summary, .static_details = details }, .{"kid"}).get();
    }
    const Described = workflow.Def("DescribedChild", described);

    /// Starts `Kid` and waits only for its start, then sleeps: no coroutine waits on the
    /// child's future when the workflow is canceled.
    fn unawaited(ctx: *workflow.Context) !void {
        const f = ctx.executeChildWorkflow(Kid, .{ .id = "kid-1", .parent_close_policy = .request_cancel }, .{"kid"});
        _ = try f.started();
        try ctx.io().sleep(.fromSeconds(60), .awake);
    }
    const Unawaited = workflow.Def("UnawaitedChild", unawaited);
};

/// Runs `DescribedChild` with `summary` and `details` and checks its StartChildWorkflowExecution
/// command's user_metadata: null fields expect none, and both null expects no user_metadata.
fn expectDescribedChildStart(summary: []const u8, details: []const u8, want_summary: ?[]const u8, want_details: ?[]const u8) !void {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, ChildWf.Described.registered, .{});
    defer inst.destroy();
    const first = commandsOf(try inst.activate(&try initActivation(a, "DescribedChild", .{ summary, details }), a));
    try testing.expectEqual(@as(usize, 1), first.len);
    try testing.expectEqualStrings("kid-1", first[0].variant.?.start_child_workflow_execution.workflow_id);
    if (want_summary == null and want_details == null) return testing.expect(first[0].user_metadata == null);
    const md = first[0].user_metadata.?;
    inline for (.{ .{ want_summary, md.summary }, .{ want_details, md.details } }) |pair| {
        if (pair[0]) |want| {
            try testing.expectEqualStrings(want, pair[1].?.data);
            try testing.expectEqualStrings("json/plain", pair[1].?.metadata.items[0].value);
        } else try testing.expect(pair[1] == null);
    }
}

test "child workflow: static summary and details are sent as the start command's user_metadata" {
    try expectDescribedChildStart("Charge the card", "Charges **once** per order.", "\"Charge the card\"", "\"Charges **once** per order.\"");
}

test "child workflow: a static summary alone sends no details" {
    try expectDescribedChildStart("Charge the card", "", "\"Charge the card\"", null);
}

test "child workflow: no static summary or details sends no user_metadata" {
    try expectDescribedChildStart("", "", null, null);
}

fn childStarted(a: Allocator, seq: u32, run_id: []const u8) !act_pb.WorkflowActivation {
    return oneJob(a, .{ .resolve_child_workflow_execution_start = .{ .seq = seq, .status = .{ .succeeded = .{ .run_id = run_id } } } });
}

fn childResolved(a: Allocator, seq: u32, status: proto.coresdk.child_workflow.ChildWorkflowResult.status_union) !act_pb.WorkflowActivation {
    return oneJob(a, .{ .resolve_child_workflow_execution = .{ .seq = seq, .result = .{ .status = status } } });
}

fn childCanceled(a: Allocator, seq: u32) !act_pb.WorkflowActivation {
    const cause = try a.create(Failure);
    cause.* = .{ .failure_info = .{ .canceled_failure_info = .{} } };
    return childResolved(a, seq, .{ .cancelled = .{ .failure = .{ .message = "Child Workflow execution cancelled", .cause = cause } } });
}

test "child workflow: StartChildWorkflowExecution with the options, then the start and the result" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, ChildWf.Run.registered, .{ .task_queue = "parent-tq", .namespace = "ns" });
    defer inst.destroy();

    const first = commandsOf(try inst.activate(&try initActivation(a, "RunChild", .{}), a));
    try testing.expectEqual(@as(usize, 1), first.len);
    const start = first[0].variant.?.start_child_workflow_execution;
    try testing.expectEqual(@as(u32, 1), start.seq);
    try testing.expectEqualStrings("Kid", start.workflow_type);
    try testing.expectEqualStrings("parent-tq", start.task_queue);
    try testing.expectEqualStrings("ns", start.namespace);
    try testing.expectEqualStrings("\"kid\"", start.input.items[0].data);
    try testing.expectEqual(@as(i64, 60), start.workflow_execution_timeout.?.seconds);
    try testing.expectEqual(@as(i64, 30), start.workflow_run_timeout.?.seconds);
    try testing.expectEqual(@as(i64, 5), start.workflow_task_timeout.?.seconds);
    try testing.expectEqual(@as(i32, 2), start.retry_policy.?.maximum_attempts);
    try testing.expectEqualStrings("0 * * * *", start.cron_schedule);
    try testing.expect(start.parent_close_policy == .PARENT_CLOSE_POLICY_ABANDON);
    try testing.expect(start.workflow_id_reuse_policy == .WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE);
    try testing.expect(start.cancellation_type == .TRY_CANCEL);
    try testing.expectEqualStrings("owner", start.memo.items[0].key);
    try testing.expectEqualStrings("\"parent\"", start.memo.items[0].value.?.data);
    // A start leaves unsets out.
    const child_sa = start.search_attributes.?.indexed_fields.items;
    try testing.expectEqual(@as(usize, 1), child_sa.len);
    try testing.expectEqualStrings("Owner", child_sa[0].key);
    try testing.expectEqualStrings("\"parent\"", child_sa[0].value.?.data);
    // Zero fields are sent as zero, for the server to inherit them from the parent.
    try testing.expectEqual(@as(i32, 2), start.priority.?.priority_key);
    try testing.expectEqualStrings("tenant-b", start.priority.?.fairness_key);
    try testing.expectEqual(@as(f32, 0), start.priority.?.fairness_weight);
    // A version 4 UUID from the seeded randomness: the same seed generates the same ID.
    try testing.expectEqual(@as(usize, 36), start.workflow_id.len);
    try testing.expectEqual(@as(u8, '4'), start.workflow_id[14]);
    {
        const again = try create(testing.allocator, ChildWf.Run.registered, .{});
        defer again.destroy();
        const cmds = commandsOf(try again.activate(&try initActivation(a, "RunChild", .{}), a));
        try testing.expectEqualStrings(start.workflow_id, cmds[0].variant.?.start_child_workflow_execution.workflow_id);
    }

    try testing.expectEqual(@as(usize, 0), commandsOf(try inst.activate(&try childStarted(a, 1, "kid-run"), a)).len);
    const done = commandsOf(try inst.activate(&try childResolved(a, 1, .{ .completed = .{ .result = try converter.toPayload(a, @as([]const u8, "hi kid")) } }), a));
    try testing.expectEqual(@as(usize, 1), done.len);
    const want = try std.fmt.allocPrint(a, "\"{s} kid-run: hi kid\"", .{start.workflow_id});
    try testing.expectEqualStrings(want, done[0].variant.?.complete_workflow_execution.result.?.data);
}

test "child workflow: a start failure on a taken ID returns error.ChildWorkflowAlreadyStarted" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, ChildWf.Run.registered, .{});
    defer inst.destroy();
    _ = try inst.activate(&try initActivation(a, "RunChild", .{}), a);
    const done = commandsOf(try inst.activate(&try oneJob(a, .{ .resolve_child_workflow_execution_start = .{ .seq = 1, .status = .{ .failed = .{
        .workflow_type = "Kid",
        .cause = .START_CHILD_WORKFLOW_EXECUTION_FAILED_CAUSE_WORKFLOW_ALREADY_EXISTS,
    } } } }), a));
    const result = done[0].variant.?.complete_workflow_execution.result.?.data;
    try testing.expect(std.mem.startsWith(u8, result, "\"already started: starting child workflow \\\"Kid\\\" with ID"));
    try testing.expect(std.mem.endsWith(u8, result, "failed: a workflow with this ID already exists\""));
}

test "child workflow: a failed child fails the parent with the child's failure" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, ChildWf.Cancel.registered, .{});
    defer inst.destroy();
    _ = try inst.activate(&try initActivation(a, "CancelChild", .{@as([]const u8, "try_cancel")}), a);
    _ = try inst.activate(&try childStarted(a, 1, "kid-run"), a);
    const cause = try a.create(Failure);
    cause.* = try failure_mod.application(a, .{ .message = "kid broke", .type = "Broken" });
    const done = commandsOf(try inst.activate(&try childResolved(a, 1, .{ .failed = .{ .failure = .{ .message = "Child Workflow execution failed", .cause = cause } } }), a));
    try testing.expectEqualStrings("\"child failed\"", done[0].variant.?.complete_workflow_execution.result.?.data);
}

test "child workflow: an unhandled child failure becomes the workflow's failure" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, ChildWf.Run.registered, .{});
    defer inst.destroy();
    _ = try inst.activate(&try initActivation(a, "RunChild", .{}), a);
    _ = try inst.activate(&try childStarted(a, 1, "kid-run"), a);
    const failed = try failure_mod.application(a, .{ .message = "kid broke", .type = "Broken" });
    const done = commandsOf(try inst.activate(&try childResolved(a, 1, .{ .failed = .{ .failure = failed } }), a));
    const fail = done[0].variant.?.fail_workflow_execution.failure.?;
    try testing.expectEqualStrings("kid broke", fail.message);
}

test "child workflow: workflow cancellation with try_cancel and abandon returns at once" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    for ([_][]const u8{ "try_cancel", "abandon" }) |mode| for ([_]bool{ false, true }) |started| {
        const inst = try create(testing.allocator, ChildWf.Cancel.registered, .{});
        defer inst.destroy();
        const first = commandsOf(try inst.activate(&try initActivation(a, "CancelChild", .{mode}), a));
        try testing.expectEqualStrings("kid-1", first[0].variant.?.start_child_workflow_execution.workflow_id);
        if (started) _ = try inst.activate(&try childStarted(a, 1, "kid-run"), a);
        const cmds = commandsOf(try inst.activate(&try oneJob(a, .{ .cancel_workflow = .{} }), a));
        try testing.expectEqual(@as(usize, 2), cmds.len);
        try testing.expectEqual(@as(u32, 1), cmds[0].variant.?.cancel_child_workflow_execution.child_workflow_seq);
        try testing.expect(cmds[1].variant.? == .cancel_workflow_execution);
        // The core's cancelled resolution that follows changes nothing.
        try testing.expectEqual(@as(usize, 0), commandsOf(try inst.activate(&try childCanceled(a, 1), a)).len);
    };
}

test "child workflow: wait_cancellation_completed waits for the child to close canceled" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, ChildWf.Cancel.registered, .{});
    defer inst.destroy();

    const first = commandsOf(try inst.activate(&try initActivation(a, "CancelChild", .{@as([]const u8, "wait_cancellation_completed")}), a));
    try testing.expect(first[0].variant.?.start_child_workflow_execution.cancellation_type == .WAIT_CANCELLATION_COMPLETED);
    _ = try inst.activate(&try childStarted(a, 1, "kid-run"), a);
    const canceled = commandsOf(try inst.activate(&try oneJob(a, .{ .cancel_workflow = .{} }), a));
    try testing.expectEqual(@as(usize, 1), canceled.len);
    try testing.expect(canceled[0].variant.? == .cancel_child_workflow_execution);
    try testing.expect(!inst.finished);

    const resolved = commandsOf(try inst.activate(&try childCanceled(a, 1), a));
    try testing.expectEqual(@as(usize, 1), resolved.len);
    try testing.expect(resolved[0].variant.? == .cancel_workflow_execution);
}

test "child workflow: wait_cancellation_completed, a child that completes anyway returns its result" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, ChildWf.Cancel.registered, .{});
    defer inst.destroy();
    _ = try inst.activate(&try initActivation(a, "CancelChild", .{@as([]const u8, "wait_cancellation_completed")}), a);
    _ = try inst.activate(&try childStarted(a, 1, "kid-run"), a);
    _ = try inst.activate(&try oneJob(a, .{ .cancel_workflow = .{} }), a);
    // get returns the result; the re-armed cancel then ends the sleep, and the workflow.
    const done = commandsOf(try inst.activate(&try childResolved(a, 1, .{ .completed = .{ .result = try converter.toPayload(a, @as([]const u8, "hi")) } }), a));
    try testing.expect(done[done.len - 1].variant.? == .cancel_workflow_execution);
    for (done) |c| try testing.expect(c.variant.? != .complete_workflow_execution);
}

test "child workflow: wait_cancellation_completed canceled before it started ends on the start's cancellation" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, ChildWf.Cancel.registered, .{});
    defer inst.destroy();
    _ = try inst.activate(&try initActivation(a, "CancelChild", .{@as([]const u8, "wait_cancellation_completed")}), a);
    try testing.expectEqual(@as(usize, 1), commandsOf(try inst.activate(&try oneJob(a, .{ .cancel_workflow = .{} }), a)).len);
    const done = commandsOf(try inst.activate(&try oneJob(a, .{ .resolve_child_workflow_execution_start = .{ .seq = 1, .status = .{ .cancelled = .{
        .failure = .{ .message = "Child Workflow Execution cancelled before scheduled" },
    } } } }), a));
    try testing.expectEqual(@as(usize, 1), done.len);
    try testing.expect(done[0].variant.? == .cancel_workflow_execution);
}

test "child workflow: no priority sends none, so the child inherits the parent's" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, ChildWf.Cancel.registered, .{});
    defer inst.destroy();
    const first = commandsOf(try inst.activate(&try initActivation(a, "CancelChild", .{@as([]const u8, "try_cancel")}), a));
    try testing.expect(first[0].variant.?.start_child_workflow_execution.priority == null);
}

test "child workflow: wait_cancellation_requested requests the started child's cancellation and returns once the server recorded it" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, ChildWf.Cancel.registered, .{ .namespace = "ns" });
    defer inst.destroy();

    const first = commandsOf(try inst.activate(&try initActivation(a, "CancelChild", .{@as([]const u8, "wait_cancellation_requested")}), a));
    try testing.expect(first[0].variant.?.start_child_workflow_execution.cancellation_type == .WAIT_CANCELLATION_REQUESTED);
    _ = try inst.activate(&try childStarted(a, 1, "kid-run"), a);

    // The workflow sends the request itself, to the child's run, so that it learns when the
    // server recorded it; the core isn't told.
    const canceled = commandsOf(try inst.activate(&try oneJob(a, .{ .cancel_workflow = .{} }), a));
    try testing.expectEqual(@as(usize, 1), canceled.len);
    const req = canceled[0].variant.?.request_cancel_external_workflow_execution;
    try testing.expectEqual(@as(u32, 1), req.seq);
    try testing.expectEqualStrings("ns", req.workflow_execution.?.namespace);
    try testing.expectEqualStrings("kid-1", req.workflow_execution.?.workflow_id);
    try testing.expectEqualStrings("kid-run", req.workflow_execution.?.run_id);
    try testing.expect(!inst.finished);

    // Recorded: get returns error.Canceled while the child still runs.
    const recorded = commandsOf(try inst.activate(&try cancelResolved(a, 1, null, .CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_UNSPECIFIED), a));
    try testing.expectEqual(@as(usize, 1), recorded.len);
    try testing.expect(recorded[0].variant.? == .cancel_workflow_execution);
    // The child closing canceled afterwards changes nothing.
    try testing.expectEqual(@as(usize, 0), commandsOf(try inst.activate(&try childCanceled(a, 1), a)).len);
}

test "child workflow: wait_cancellation_requested through the future, get reports the cancellation once recorded" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, ChildWf.Cancel.registered, .{});
    defer inst.destroy();
    _ = try inst.activate(&try initActivation(a, "CancelChild", .{@as([]const u8, "wait_cancellation_requested+future")}), a);
    const cmds = commandsOf(try inst.activate(&try childStarted(a, 1, "kid-run"), a));
    try testing.expectEqual(@as(usize, 1), cmds.len);
    try testing.expectEqualStrings("kid-run", cmds[0].variant.?.request_cancel_external_workflow_execution.workflow_execution.?.run_id);
    const done = commandsOf(try inst.activate(&try cancelResolved(a, 1, null, .CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_UNSPECIFIED), a));
    try testing.expectEqualStrings("\"child canceled\"", done[0].variant.?.complete_workflow_execution.result.?.data);
}

test "child workflow: wait_cancellation_requested canceled before its start was sent ends on the start's cancellation" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, ChildWf.Cancel.registered, .{});
    defer inst.destroy();
    _ = try inst.activate(&try initActivation(a, "CancelChild", .{@as([]const u8, "wait_cancellation_requested")}), a);
    // Not started yet: the core cancels it, before sending its start.
    const canceled = commandsOf(try inst.activate(&try oneJob(a, .{ .cancel_workflow = .{} }), a));
    try testing.expectEqual(@as(usize, 1), canceled.len);
    try testing.expectEqual(@as(u32, 1), canceled[0].variant.?.cancel_child_workflow_execution.child_workflow_seq);
    try testing.expect(!inst.finished);
    const done = commandsOf(try inst.activate(&try oneJob(a, .{ .resolve_child_workflow_execution_start = .{ .seq = 1, .status = .{ .cancelled = .{
        .failure = .{ .message = "Child Workflow Execution cancelled before scheduled" },
    } } } }), a));
    try testing.expectEqual(@as(usize, 1), done.len);
    try testing.expect(done[0].variant.? == .cancel_workflow_execution);
}

test "child workflow: wait_cancellation_requested canceled while its start is in flight sends the request once it started" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, ChildWf.Cancel.registered, .{});
    defer inst.destroy();
    _ = try inst.activate(&try initActivation(a, "CancelChild", .{@as([]const u8, "wait_cancellation_requested")}), a);
    _ = try inst.activate(&try oneJob(a, .{ .cancel_workflow = .{} }), a);
    // The start was already sent: the child starts, and the workflow then sends the request
    // whose recording it waits for.
    const started = commandsOf(try inst.activate(&try childStarted(a, 1, "kid-run"), a));
    try testing.expectEqual(@as(usize, 1), started.len);
    try testing.expectEqualStrings("kid-run", started[0].variant.?.request_cancel_external_workflow_execution.workflow_execution.?.run_id);
    try testing.expect(!inst.finished);
    const done = commandsOf(try inst.activate(&try cancelResolved(a, 1, null, .CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_UNSPECIFIED), a));
    try testing.expectEqual(@as(usize, 1), done.len);
    try testing.expect(done[0].variant.? == .cancel_workflow_execution);
}

test "child workflow: wait_cancellation_requested, a child that closes before the request is recorded returns how it closed" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    {
        // It completes: get returns its result, and the re-armed cancel ends the sleep.
        const inst = try create(testing.allocator, ChildWf.Cancel.registered, .{});
        defer inst.destroy();
        _ = try inst.activate(&try initActivation(a, "CancelChild", .{@as([]const u8, "wait_cancellation_requested")}), a);
        _ = try inst.activate(&try childStarted(a, 1, "kid-run"), a);
        _ = try inst.activate(&try oneJob(a, .{ .cancel_workflow = .{} }), a);
        const done = commandsOf(try inst.activate(&try childResolved(a, 1, .{ .completed = .{ .result = try converter.toPayload(a, @as([]const u8, "hi")) } }), a));
        try testing.expect(done[done.len - 1].variant.? == .cancel_workflow_execution);
        for (done) |c| try testing.expect(c.variant.? != .complete_workflow_execution);
    }
    {
        // The server refuses the request (the child closed), and the core then reports the
        // child canceled.
        const inst = try create(testing.allocator, ChildWf.Cancel.registered, .{});
        defer inst.destroy();
        _ = try inst.activate(&try initActivation(a, "CancelChild", .{@as([]const u8, "wait_cancellation_requested")}), a);
        _ = try inst.activate(&try childStarted(a, 1, "kid-run"), a);
        _ = try inst.activate(&try oneJob(a, .{ .cancel_workflow = .{} }), a);
        const refused = commandsOf(try inst.activate(&try cancelResolved(a, 1, .{ .message = "not found" }, .CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_EXTERNAL_WORKFLOW_EXECUTION_NOT_FOUND), a));
        try testing.expectEqual(@as(usize, 0), refused.len);
        try testing.expect(!inst.finished);
        const done = commandsOf(try inst.activate(&try childCanceled(a, 1), a));
        try testing.expectEqual(@as(usize, 1), done.len);
        try testing.expect(done[0].variant.? == .cancel_workflow_execution);
    }
}

test "child workflow: canceling the workflow leaves a child nobody awaits to its parent close policy" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, ChildWf.Unawaited.registered, .{});
    defer inst.destroy();
    const first = commandsOf(try inst.activate(&try initActivation(a, "UnawaitedChild", .{}), a));
    try testing.expect(first[0].variant.?.start_child_workflow_execution.parent_close_policy == .PARENT_CLOSE_POLICY_REQUEST_CANCEL);
    _ = try inst.activate(&try childStarted(a, 1, "kid-run"), a);
    const canceled = commandsOf(try inst.activate(&try oneJob(a, .{ .cancel_workflow = .{} }), a));
    try testing.expect(canceled[canceled.len - 1].variant.? == .cancel_workflow_execution);
    for (canceled) |c| switch (c.variant.?) {
        .cancel_child_workflow_execution, .request_cancel_external_workflow_execution => return error.TestUnexpectedResult,
        else => {},
    };
}

/// Waits in the child's `get` from a coroutine of its own, which the workflow function awaits.
const SideAwaitedChild = struct {
    fn getChild(f: workflow.ChildWorkflowFuture([]const u8)) workflow.ChildWorkflowError![]const u8 {
        return f.get();
    }

    fn run(ctx: *workflow.Context) ![]const u8 {
        const io = ctx.io();
        const f = ctx.executeChildWorkflow(ChildWf.Kid, .{ .id = "kid-1" }, .{"kid"});
        var waiting = io.async(getChild, .{f});
        return waiting.await(io);
    }
    const Def = workflow.Def("SideAwaitedChild", run);
};

test "child workflow: canceling the workflow cancels a child that a coroutine other than the workflow function waits on" {
    // ChildWorkflowOptions and README.md: "Canceling the parent cancels only the children a
    // coroutine is waiting on in `get` or `started`" and "the coroutines blocked in a child's
    // `get` or `started` are canceled with the rest". Here a coroutine the workflow function
    // awaits waits in `get`, so the child (`try_cancel`) is canceled and the workflow ends.
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, SideAwaitedChild.Def.registered, .{});
    defer inst.destroy();
    const first = commandsOf(try inst.activate(&try initActivation(a, "SideAwaitedChild", .{}), a));
    try testing.expect(first[0].variant.? == .start_child_workflow_execution);
    _ = try inst.activate(&try childStarted(a, 1, "kid-run"), a);
    const canceled = commandsOf(try inst.activate(&try oneJob(a, .{ .cancel_workflow = .{} }), a));
    var child_canceled = false;
    for (canceled) |c| switch (c.variant.?) {
        .cancel_child_workflow_execution => |cc| child_canceled = cc.child_workflow_seq == 1,
        .cancel_workflow_execution => try testing.expect(child_canceled),
        else => {},
    };
    try testing.expect(child_canceled);
}

test "child workflow: canceling the future cancels the child, and get reports the cancellation" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    for ([_][]const u8{ "try_cancel+future", "wait_cancellation_completed+future", "abandon+future" }) |mode| {
        const inst = try create(testing.allocator, ChildWf.Cancel.registered, .{});
        defer inst.destroy();
        _ = try inst.activate(&try initActivation(a, "CancelChild", .{mode}), a);
        const cmds = commandsOf(try inst.activate(&try childStarted(a, 1, "kid-run"), a));
        try testing.expectEqual(@as(usize, 1), cmds.len);
        try testing.expectEqual(@as(u32, 1), cmds[0].variant.?.cancel_child_workflow_execution.child_workflow_seq);
        const done = commandsOf(try inst.activate(&try childCanceled(a, 1), a));
        try testing.expectEqualStrings("\"child canceled\"", done[0].variant.?.complete_workflow_execution.result.?.data);
    }
}

test "child workflow: eviction ends waits for the start and for the cancellation to complete" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();

    const starting = try create(testing.allocator, ChildWf.Run.registered, .{});
    _ = try starting.activate(&try initActivation(a, "RunChild", .{}), a);
    starting.destroy();

    const canceling = try create(testing.allocator, ChildWf.Cancel.registered, .{});
    _ = try canceling.activate(&try initActivation(a, "CancelChild", .{@as([]const u8, "wait_cancellation_completed")}), a);
    _ = try canceling.activate(&try childStarted(a, 1, "kid-run"), a);
    _ = try canceling.activate(&try oneJob(a, .{ .cancel_workflow = .{} }), a);
    canceling.destroy();
}

test "child workflow: an empty workflow type returns error.InvalidOptions from get" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, ChildWf.ByName.registered, .{});
    defer inst.destroy();
    const done = commandsOf(try inst.activate(&try initActivation(a, "ByName", .{}), a));
    try testing.expectEqual(@as(usize, 1), done.len);
    try testing.expectEqualStrings("InvalidOptions", done[0].variant.?.fail_workflow_execution.failure.?.message);
}

// Nexus operations. No handler runs here: the tests resolve the operations as the core does.
const NexusWf = struct {
    const nexus_mod = @import("../nexus.zig");
    const EchoInput = struct { message: []const u8 };
    const Echo = nexus_mod.OperationDef("echo", EchoInput, []const u8);
    const Ping = nexus_mod.OperationDef("ping", void, void);
    const Service = nexus_mod.ServiceDef("hello-service", .{ Echo, Ping });
    const Future = workflow.NexusOperationFuture([]const u8);

    /// Calls `Echo` with every option set and reports how it went (`describe`).
    fn run(ctx: *workflow.Context) ![]const u8 {
        const f = ctx.executeNexusOperation(Service, Echo, .{
            .endpoint = "hello-endpoint",
            .schedule_to_close_timeout = .fromSeconds(60),
            .schedule_to_start_timeout = .fromSeconds(10),
            .start_to_close_timeout = .fromMilliseconds(30_500),
            .cancellation_type = .wait_cancellation_requested,
            .headers = &.{ .{ .name = "trace-id", .value = "abc" }, .{ .name = "tenant", .value = "acme" } },
            .summary = "say hi",
        }, .{ .message = "hi" });
        return describe(ctx, f);
    }
    const Run = workflow.Def("RunNexus", run);

    /// "<token or sync>: <result>", or "<error>: <failure's message> <- <root cause's
    /// message> (<outcome>)" for an error from `started` or `get`.
    fn describe(ctx: *workflow.Context, f: Future) ![]const u8 {
        const a = ctx.allocator();
        const token = f.started() catch |err| return failed(ctx, f, err);
        if (token == null and !f.isReady()) return error.SyncStartWithoutResult;
        const result = f.get() catch |err| return failed(ctx, f, err);
        return std.fmt.allocPrint(a, "{s}: {s}", .{ token orelse "sync", result });
    }

    fn failed(ctx: *workflow.Context, f: Future, err: workflow.NexusOperationError) ![]const u8 {
        if (err != error.NexusOperationFailed) return err;
        const fl = f.failure().?;
        const state = try f.state;
        return std.fmt.allocPrint(ctx.allocator(), "{t}: {s} <- {s} ({s})", .{ err, fl.message, failure_mod.rootCause(fl).message, @tagName(state.outcome.?) });
    }

    /// Calls `Echo` with the cancellation type named by `mode`, and returns its result after
    /// a sleep, which a re-armed cancel interrupts. A mode ending in "+future" cancels through
    /// the future once the operation started, and one ending in "+now" right after
    /// scheduling it.
    fn cancel(ctx: *workflow.Context, mode: []const u8) ![]const u8 {
        const end = std.mem.indexOfScalar(u8, mode, '+') orelse mode.len;
        const cancellation_type = std.meta.stringToEnum(workflow.NexusOperationCancellationType, mode[0..end]) orelse return error.InvalidPayload;
        const f = ctx.executeNexusOperation(Service, Echo, .{ .endpoint = "hello-endpoint", .cancellation_type = cancellation_type }, .{ .message = "hi" });
        if (std.mem.endsWith(u8, mode, "+now")) f.cancel();
        if (std.mem.endsWith(u8, mode, "+future")) {
            _ = f.started() catch |err| return failed(ctx, f, err);
            f.cancel();
        }
        const r = f.get() catch |err| return failed(ctx, f, err);
        try ctx.io().sleep(.fromSeconds(1), .awake);
        return r;
    }
    const Cancel = workflow.Def("CancelNexus", cancel);

    /// Calls `Ping`, whose input and output are void.
    fn ping(ctx: *workflow.Context) ![]const u8 {
        try ctx.executeNexusOperation(Service, Ping, .{ .endpoint = "hello-endpoint" }, {}).get();
        return "pong";
    }
    const PingWf = workflow.Def("PingNexus", ping);

    /// Calls `Echo` twice at once and waits for the first, then the second.
    fn two(ctx: *workflow.Context) ![]const u8 {
        const first = ctx.executeNexusOperation(Service, Echo, .{ .endpoint = "hello-endpoint" }, .{ .message = "one" });
        const second = ctx.executeNexusOperation(Service, Echo, .{ .endpoint = "hello-endpoint" }, .{ .message = "two" });
        const one = try first.get();
        return std.fmt.allocPrint(ctx.allocator(), "{s} {s}", .{ one, try second.get() });
    }
    const Two = workflow.Def("TwoNexus", two);

    /// Returns `Echo`'s error unhandled, so it fails the workflow.
    fn unhandled(ctx: *workflow.Context) ![]const u8 {
        return ctx.executeNexusOperation(Service, Echo, .{ .endpoint = "hello-endpoint" }, .{ .message = "hi" }).get();
    }
    const Unhandled = workflow.Def("UnhandledNexus", unhandled);

    /// Calls an operation by name with the endpoint, service and operation from `names`
    /// ("endpoint/service/operation"); returns the error's name if scheduling fails.
    fn byName(ctx: *workflow.Context, names: []const u8) ![]const u8 {
        var it = std.mem.splitScalar(u8, names, '/');
        const endpoint = it.next().?;
        const service = it.next().?;
        const operation = it.next().?;
        const f = ctx.executeNexusOperationByName([]const u8, service, operation, .{ .endpoint = endpoint }, @as(u32, 7));
        return f.get() catch |err| @errorName(err);
    }
    const ByName = workflow.Def("ByNameNexus", byName);
};

fn nexusFailure(a: Allocator, message: []const u8, cause: Failure) !Failure {
    const c = try a.create(Failure);
    c.* = cause;
    return .{ .message = message, .cause = c, .failure_info = .{ .nexus_operation_execution_failure_info = .{
        .endpoint = "hello-endpoint",
        .service = "hello-service",
        .operation = "echo",
    } } };
}

fn nexusCanceledFailure(a: Allocator, message: []const u8) !Failure {
    return nexusFailure(a, message, .{ .message = "", .failure_info = .{ .canceled_failure_info = .{} } });
}

fn nexusStart(a: Allocator, seq: u32, status: act_pb.ResolveNexusOperationStart.status_union) !act_pb.WorkflowActivation {
    return oneJob(a, .{ .resolve_nexus_operation_start = .{ .seq = seq, .status = status } });
}

fn nexusResolved(a: Allocator, seq: u32, status: NexusResult.status_union) !act_pb.WorkflowActivation {
    return oneJob(a, .{ .resolve_nexus_operation = .{ .seq = seq, .result = .{ .status = status } } });
}

fn nexusOutput(a: Allocator, text: []const u8) !NexusResult.status_union {
    return .{ .completed = try converter.toPayload(a, text) };
}

/// The workflow's result, from a completion that completed it.
fn nexusResultOf(c: done_pb.WorkflowActivationCompletion) []const u8 {
    const cmds = commandsOf(c);
    return cmds[cmds.len - 1].variant.?.complete_workflow_execution.result.?.data;
}

test "nexus: ScheduleNexusOperation carries the options; a synchronous start and its result arrive together" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, NexusWf.Run.registered, .{});
    defer inst.destroy();

    const first = commandsOf(try inst.activate(&try initActivation(a, "RunNexus", .{}), a));
    try testing.expectEqual(@as(usize, 1), first.len);
    const cmd = first[0].variant.?.schedule_nexus_operation;
    try testing.expectEqual(@as(u32, 1), cmd.seq);
    try testing.expectEqualStrings("hello-endpoint", cmd.endpoint);
    try testing.expectEqualStrings("hello-service", cmd.service);
    try testing.expectEqualStrings("echo", cmd.operation);
    try testing.expectEqualStrings("{\"message\":\"hi\"}", cmd.input.?.data);
    try testing.expectEqualStrings("json/plain", converter.encodingOf(cmd.input.?).?);
    try testing.expectEqual(@as(i64, 60), cmd.schedule_to_close_timeout.?.seconds);
    try testing.expectEqual(@as(i64, 10), cmd.schedule_to_start_timeout.?.seconds);
    try testing.expectEqual(@as(i64, 30), cmd.start_to_close_timeout.?.seconds);
    try testing.expectEqual(@as(i32, 500_000_000), cmd.start_to_close_timeout.?.nanos);
    try testing.expect(cmd.cancellation_type == .WAIT_CANCELLATION_REQUESTED);
    try testing.expectEqual(@as(usize, 2), cmd.nexus_header.items.len);
    try testing.expectEqualStrings("trace-id", cmd.nexus_header.items[0].key);
    try testing.expectEqualStrings("abc", cmd.nexus_header.items[0].value);
    try testing.expectEqualStrings("tenant", cmd.nexus_header.items[1].key);
    try testing.expectEqualStrings("acme", cmd.nexus_header.items[1].value);
    try testing.expectEqualStrings("\"say hi\"", first[0].user_metadata.?.summary.?.data);

    var act = try nexusStart(a, 1, .{ .started_sync = true });
    try act.jobs.append(a, .{ .variant = .{ .resolve_nexus_operation = .{ .seq = 1, .result = .{ .status = try nexusOutput(a, "hi back") } } } });
    try testing.expectEqualStrings("\"sync: hi back\"", nexusResultOf(try inst.activate(&act, a)));
}

test "nexus: the default options send no timeouts, headers or summary, and wait_cancellation_completed" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, NexusWf.Unhandled.registered, .{});
    defer inst.destroy();
    const first = commandsOf(try inst.activate(&try initActivation(a, "UnhandledNexus", .{}), a));
    const cmd = first[0].variant.?.schedule_nexus_operation;
    try testing.expect(cmd.schedule_to_close_timeout == null and cmd.schedule_to_start_timeout == null and cmd.start_to_close_timeout == null);
    try testing.expectEqual(@as(usize, 0), cmd.nexus_header.items.len);
    try testing.expect(cmd.cancellation_type == .WAIT_CANCELLATION_COMPLETED);
    try testing.expect(first[0].user_metadata == null);
}

test "nexus: propagated values replace a caller header of the reserved name, after the caller's headers" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const tenant = comptime propagation.key([]const u8, "tenant");
    const propagators = [_]propagation.ContextPropagator{propagation.HeaderPropagator(tenant).propagator};
    const W = struct {
        fn run(ctx: *workflow.Context) ![]const u8 {
            const caller = [_]client.NexusHeader{
                .{ .name = "Temporal-Zig-Context", .value = "stale" },
                .{ .name = "trace-id", .value = "abc" },
            };
            _ = ctx.executeNexusOperation(NexusWf.Service, NexusWf.Echo, .{ .endpoint = "hello-endpoint", .headers = &caller }, .{ .message = "hi" });
            try testing.expectEqualStrings("stale", caller[0].value);
            return "";
        }
        const Def = workflow.Def("PropagatingNexus", run);
    };

    // With a propagated value: the caller's other headers in order, then the propagated one.
    {
        const inst = try create(testing.allocator, W.Def.registered, .{ .propagators = &propagators });
        defer inst.destroy();
        var act = try initActivation(a, "PropagatingNexus", .{});
        try act.jobs.items[0].variant.?.initialize_workflow.headers.append(a, .{ .key = "tenant", .value = try converter.toPayload(a, @as([]const u8, "acme")) });
        const cmd = commandsOf(try inst.activate(&act, a))[0].variant.?.schedule_nexus_operation;
        try testing.expectEqual(@as(usize, 2), cmd.nexus_header.items.len);
        try testing.expectEqualStrings("trace-id", cmd.nexus_header.items[0].key);
        try testing.expectEqualStrings(propagation.nexus_header_name, cmd.nexus_header.items[1].key);
        var received: propagation.Values = .init(a);
        try propagation.extractNexusHeader(&propagators, a, cmd.nexus_header.items[1].value, &received);
        try testing.expectEqualStrings("acme", received.get(tenant).?);
    }
    // Nothing propagated: the caller's headers exactly as given.
    {
        const inst = try create(testing.allocator, W.Def.registered, .{ .propagators = &propagators });
        defer inst.destroy();
        const cmd = commandsOf(try inst.activate(&try initActivation(a, "PropagatingNexus", .{}), a))[0].variant.?.schedule_nexus_operation;
        try testing.expectEqual(@as(usize, 2), cmd.nexus_header.items.len);
        try testing.expectEqualStrings("Temporal-Zig-Context", cmd.nexus_header.items[0].key);
        try testing.expectEqualStrings("stale", cmd.nexus_header.items[0].value);
        try testing.expectEqualStrings("trace-id", cmd.nexus_header.items[1].key);
    }
}

test "nexus: an asynchronous start returns its token, and the result follows in a later activation" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, NexusWf.Run.registered, .{});
    defer inst.destroy();
    _ = try inst.activate(&try initActivation(a, "RunNexus", .{}), a);
    try testing.expectEqual(@as(usize, 0), commandsOf(try inst.activate(&try nexusStart(a, 1, .{ .operation_token = "tok-1" }), a)).len);
    try testing.expect(!inst.finished);
    try testing.expectEqualStrings("\"tok-1: hi later\"", nexusResultOf(try inst.activate(&try nexusResolved(a, 1, try nexusOutput(a, "hi later")), a)));
}

test "nexus: a failed start returns error.NexusOperationFailed from started, with the core's failure" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const cases = [_]struct { Failure, []const u8 }{
        .{ try nexusFailure(a, "nexus operation completed unsuccessfully", try failure_mod.application(a, .{ .message = "no such user", .type = "NotFound" })), "nexus operation completed unsuccessfully <- no such user (failed)" },
        .{ try nexusFailure(a, "nexus operation completed unsuccessfully", .{ .message = "operation timed out", .failure_info = .{ .timeout_failure_info = .{ .timeout_type = .TIMEOUT_TYPE_SCHEDULE_TO_CLOSE } } }), "nexus operation completed unsuccessfully <- operation timed out (timed_out)" },
        .{ try nexusFailure(a, "nexus operation completed unsuccessfully", .{ .message = "operation canceled", .failure_info = .{ .canceled_failure_info = .{} } }), "nexus operation completed unsuccessfully <- operation canceled (canceled)" },
    };
    for (cases) |c| {
        const inst = try create(testing.allocator, NexusWf.Run.registered, .{});
        defer inst.destroy();
        _ = try inst.activate(&try initActivation(a, "RunNexus", .{}), a);
        const want = try std.fmt.allocPrint(a, "\"NexusOperationFailed: {s}\"", .{c[1]});
        try testing.expectEqualStrings(want, nexusResultOf(try inst.activate(&try nexusStart(a, 1, .{ .failed = c[0] }), a)));
        // No result follows a failed start; one that did anyway would be ignored.
        try testing.expectEqual(@as(usize, 0), inst.nexus_operations.count());
    }
}

test "nexus: a started operation completes, fails, is canceled, or times out" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const cases = [_]struct { NexusResult.status_union, []const u8 }{
        .{ try nexusOutput(a, "done"), "\"tok: done\"" },
        .{ .{ .failed = try nexusFailure(a, "nexus operation completed unsuccessfully", try failure_mod.application(a, .{ .message = "broken" })) }, "\"NexusOperationFailed: nexus operation completed unsuccessfully <- broken (failed)\"" },
        .{ .{ .cancelled = try nexusFailure(a, "nexus operation completed unsuccessfully", .{ .message = "operation canceled", .failure_info = .{ .canceled_failure_info = .{} } }) }, "\"NexusOperationFailed: nexus operation completed unsuccessfully <- operation canceled (canceled)\"" },
        .{ .{ .timed_out = try nexusFailure(a, "nexus operation completed unsuccessfully", .{ .message = "operation timed out", .failure_info = .{ .timeout_failure_info = .{} } }) }, "\"NexusOperationFailed: nexus operation completed unsuccessfully <- operation timed out (timed_out)\"" },
    };
    for (cases) |c| {
        const inst = try create(testing.allocator, NexusWf.Run.registered, .{});
        defer inst.destroy();
        _ = try inst.activate(&try initActivation(a, "RunNexus", .{}), a);
        _ = try inst.activate(&try nexusStart(a, 1, .{ .operation_token = "tok" }), a);
        try testing.expectEqualStrings(c[1], nexusResultOf(try inst.activate(&try nexusResolved(a, 1, c[0]), a)));
    }
}

test "nexus: an unhandled operation failure becomes the workflow's failure" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, NexusWf.Unhandled.registered, .{});
    defer inst.destroy();
    _ = try inst.activate(&try initActivation(a, "UnhandledNexus", .{}), a);
    const f = try nexusFailure(a, "nexus operation completed unsuccessfully", try failure_mod.application(a, .{ .message = "broken", .type = "Broken" }));
    const done = commandsOf(try inst.activate(&try nexusStart(a, 1, .{ .failed = f }), a));
    const fail = done[0].variant.?.fail_workflow_execution.failure.?;
    try testing.expectEqualStrings("nexus operation completed unsuccessfully", fail.message);
    try testing.expect(fail.failure_info.? == .nexus_operation_execution_failure_info);
    try testing.expectEqualStrings("Broken", failure_mod.applicationType(fail.cause.?.*).?);
}

test "nexus: void input sends the null payload, and void output ignores the result" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, NexusWf.PingWf.registered, .{});
    defer inst.destroy();
    const first = commandsOf(try inst.activate(&try initActivation(a, "PingNexus", .{}), a));
    const cmd = first[0].variant.?.schedule_nexus_operation;
    try testing.expectEqualStrings("ping", cmd.operation);
    try testing.expectEqualStrings("binary/null", converter.encodingOf(cmd.input.?).?);
    try testing.expectEqual(@as(usize, 0), cmd.input.?.data.len);
    // A synchronous void operation's result is an empty payload.
    var act = try nexusStart(a, 1, .{ .started_sync = true });
    try act.jobs.append(a, .{ .variant = .{ .resolve_nexus_operation = .{ .seq = 1, .result = .{ .status = .{ .completed = .{} } } } } });
    try testing.expectEqualStrings("\"pong\"", nexusResultOf(try inst.activate(&act, a)));
}

test "nexus: two operations get their own seqs and resolve in the opposite order" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, NexusWf.Two.registered, .{});
    defer inst.destroy();
    const first = commandsOf(try inst.activate(&try initActivation(a, "TwoNexus", .{}), a));
    try testing.expectEqual(@as(usize, 2), first.len);
    try testing.expectEqual(@as(u32, 1), first[0].variant.?.schedule_nexus_operation.seq);
    try testing.expectEqualStrings("{\"message\":\"one\"}", first[0].variant.?.schedule_nexus_operation.input.?.data);
    try testing.expectEqual(@as(u32, 2), first[1].variant.?.schedule_nexus_operation.seq);
    try testing.expectEqualStrings("{\"message\":\"two\"}", first[1].variant.?.schedule_nexus_operation.input.?.data);

    _ = try inst.activate(&try nexusStart(a, 2, .{ .operation_token = "t2" }), a);
    _ = try inst.activate(&try nexusStart(a, 1, .{ .operation_token = "t1" }), a);
    try testing.expectEqual(@as(usize, 0), commandsOf(try inst.activate(&try nexusResolved(a, 2, try nexusOutput(a, "second")), a)).len);
    try testing.expect(!inst.finished);
    try testing.expectEqualStrings("\"first second\"", nexusResultOf(try inst.activate(&try nexusResolved(a, 1, try nexusOutput(a, "first")), a)));
}

test "nexus: an empty endpoint, service or operation returns error.InvalidOptions and sends nothing" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    for ([_][]const u8{ "/hello-service/echo", "hello-endpoint//echo", "hello-endpoint/hello-service/" }) |names| {
        const inst = try create(testing.allocator, NexusWf.ByName.registered, .{});
        defer inst.destroy();
        const done = commandsOf(try inst.activate(&try initActivation(a, "ByNameNexus", .{names}), a));
        try testing.expectEqual(@as(usize, 1), done.len);
        try testing.expectEqualStrings("\"InvalidOptions\"", done[0].variant.?.complete_workflow_execution.result.?.data);
        try testing.expectEqual(@as(u32, 1), inst.next_nexus_seq);
    }
    // By name, the input is any value and the names are the ones given.
    const inst = try create(testing.allocator, NexusWf.ByName.registered, .{});
    defer inst.destroy();
    const first = commandsOf(try inst.activate(&try initActivation(a, "ByNameNexus", .{@as([]const u8, "ep/other-service/other-op")}), a));
    const cmd = first[0].variant.?.schedule_nexus_operation;
    try testing.expectEqualStrings("ep", cmd.endpoint);
    try testing.expectEqualStrings("other-service", cmd.service);
    try testing.expectEqualStrings("other-op", cmd.operation);
    try testing.expectEqualStrings("7", cmd.input.?.data);
}

test "nexus: workflow cancellation before the start resolves sends RequestCancelNexusOperation; each type resolves as the core does" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    for ([_][]const u8{ "try_cancel", "abandon", "wait_cancellation_requested", "wait_cancellation_completed" }) |mode| {
        const inst = try create(testing.allocator, NexusWf.Cancel.registered, .{});
        defer inst.destroy();
        _ = try inst.activate(&try initActivation(a, "CancelNexus", .{mode}), a);
        const cmds = commandsOf(try inst.activate(&try oneJob(a, .{ .cancel_workflow = .{} }), a));
        // The request goes to the core whatever the type; the core decides what reaches the
        // server, and nothing resolves until it says so.
        try testing.expectEqual(@as(usize, 1), cmds.len);
        try testing.expectEqual(@as(u32, 1), cmds[0].variant.?.request_cancel_nexus_operation.seq);
        try testing.expect(!inst.finished);
        // try_cancel and abandon: the core resolves the operation as canceled at once, with no
        // start. The other two: the server canceled it before it started, so its start fails.
        const resolution = if (std.mem.eql(u8, mode, "try_cancel") or std.mem.eql(u8, mode, "abandon"))
            try nexusResolved(a, 1, .{ .cancelled = try nexusCanceledFailure(a, "Nexus operation cancelled after starting") })
        else
            try nexusStart(a, 1, .{ .failed = try nexusCanceledFailure(a, "nexus operation completed unsuccessfully") });
        const done = commandsOf(try inst.activate(&resolution, a));
        try testing.expectEqual(@as(usize, 1), done.len);
        try testing.expect(done[0].variant.? == .cancel_workflow_execution);
        // A start the core sends after resolving a try_cancel operation is ignored.
        try testing.expectEqual(@as(usize, 0), commandsOf(try inst.activate(&try nexusStart(a, 1, .{ .operation_token = "late" }), a)).len);
    }
}

test "nexus: workflow cancellation after the start: each type ends the wait when the core resolves the operation" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const cases = [_]struct { []const u8, []const u8 }{
        .{ "try_cancel", "Nexus operation cancelled after starting" },
        .{ "abandon", "Nexus operation cancelled after starting" },
        .{ "wait_cancellation_requested", "Nexus operation cancellation request completed" },
        .{ "wait_cancellation_completed", "nexus operation completed unsuccessfully" },
    };
    for (cases) |c| {
        const inst = try create(testing.allocator, NexusWf.Cancel.registered, .{});
        defer inst.destroy();
        const first = commandsOf(try inst.activate(&try initActivation(a, "CancelNexus", .{c[0]}), a));
        const want_type: proto.coresdk.nexus.NexusOperationCancellationType = if (std.mem.eql(u8, c[0], "try_cancel")) .TRY_CANCEL else if (std.mem.eql(u8, c[0], "abandon")) .ABANDON else if (std.mem.eql(u8, c[0], "wait_cancellation_requested")) .WAIT_CANCELLATION_REQUESTED else .WAIT_CANCELLATION_COMPLETED;
        try testing.expect(first[0].variant.?.schedule_nexus_operation.cancellation_type == want_type);
        _ = try inst.activate(&try nexusStart(a, 1, .{ .operation_token = "tok" }), a);
        const cmds = commandsOf(try inst.activate(&try oneJob(a, .{ .cancel_workflow = .{} }), a));
        try testing.expectEqual(@as(usize, 1), cmds.len);
        try testing.expectEqual(@as(u32, 1), cmds[0].variant.?.request_cancel_nexus_operation.seq);
        try testing.expect(!inst.finished);
        const done = commandsOf(try inst.activate(&try nexusResolved(a, 1, .{ .cancelled = try nexusCanceledFailure(a, c[1]) }), a));
        try testing.expectEqual(@as(usize, 1), done.len);
        try testing.expect(done[0].variant.? == .cancel_workflow_execution);
    }
}

test "nexus: wait_cancellation_completed, an operation that completes despite the cancel returns its result" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, NexusWf.Cancel.registered, .{});
    defer inst.destroy();
    _ = try inst.activate(&try initActivation(a, "CancelNexus", .{@as([]const u8, "wait_cancellation_completed")}), a);
    _ = try inst.activate(&try nexusStart(a, 1, .{ .operation_token = "tok" }), a);
    _ = try inst.activate(&try oneJob(a, .{ .cancel_workflow = .{} }), a);
    // get returns the result; the re-armed cancel then ends the sleep, and the workflow.
    const done = commandsOf(try inst.activate(&try nexusResolved(a, 1, try nexusOutput(a, "finished anyway")), a));
    try testing.expect(done[done.len - 1].variant.? == .cancel_workflow_execution);
    for (done) |cmd| try testing.expect(cmd.variant.? != .complete_workflow_execution);
}

test "nexus: wait_cancellation_requested, a refused cancel request resolves the operation as failed" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, NexusWf.Cancel.registered, .{});
    defer inst.destroy();
    _ = try inst.activate(&try initActivation(a, "CancelNexus", .{@as([]const u8, "wait_cancellation_requested+future")}), a);
    try testing.expectEqual(@as(u32, 1), commandsOf(try inst.activate(&try nexusStart(a, 1, .{ .operation_token = "tok" }), a))[0].variant.?.request_cancel_nexus_operation.seq);
    const refused = try nexusFailure(a, "Nexus operation cancellation request failed", try failure_mod.application(a, .{ .message = "cannot cancel" }));
    try testing.expectEqualStrings(
        "\"NexusOperationFailed: Nexus operation cancellation request failed <- cannot cancel (failed)\"",
        nexusResultOf(try inst.activate(&try nexusResolved(a, 1, .{ .failed = refused }), a)),
    );
}

test "nexus: canceling the future sends RequestCancelNexusOperation, and get reports the cancellation" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    for ([_][]const u8{ "try_cancel+future", "abandon+future", "wait_cancellation_requested+future", "wait_cancellation_completed+future" }) |mode| {
        const inst = try create(testing.allocator, NexusWf.Cancel.registered, .{});
        defer inst.destroy();
        _ = try inst.activate(&try initActivation(a, "CancelNexus", .{mode}), a);
        const cmds = commandsOf(try inst.activate(&try nexusStart(a, 1, .{ .operation_token = "tok" }), a));
        try testing.expectEqual(@as(usize, 1), cmds.len);
        try testing.expectEqual(@as(u32, 1), cmds[0].variant.?.request_cancel_nexus_operation.seq);
        try testing.expectEqualStrings(
            "\"NexusOperationFailed: Nexus operation cancelled after starting <-  (canceled)\"",
            nexusResultOf(try inst.activate(&try nexusResolved(a, 1, .{ .cancelled = try nexusCanceledFailure(a, "Nexus operation cancelled after starting") }), a)),
        );
    }
}

test "nexus: canceled in the workflow task that scheduled it, the operation's start fails canceled" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, NexusWf.Cancel.registered, .{});
    defer inst.destroy();
    const first = commandsOf(try inst.activate(&try initActivation(a, "CancelNexus", .{@as([]const u8, "wait_cancellation_completed+now")}), a));
    try testing.expectEqual(@as(usize, 2), first.len);
    try testing.expect(first[0].variant.? == .schedule_nexus_operation);
    try testing.expectEqual(@as(u32, 1), first[1].variant.?.request_cancel_nexus_operation.seq);
    try testing.expectEqualStrings(
        "\"NexusOperationFailed: Nexus Operation cancelled before scheduled <-  (canceled)\"",
        nexusResultOf(try inst.activate(&try nexusStart(a, 1, .{ .failed = try nexusCanceledFailure(a, "Nexus Operation cancelled before scheduled") }), a)),
    );
}

test "nexus: eviction ends waits for the start and for the cancellation to resolve" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();

    const starting = try create(testing.allocator, NexusWf.Run.registered, .{});
    _ = try starting.activate(&try initActivation(a, "RunNexus", .{}), a);
    starting.destroy();

    const canceling = try create(testing.allocator, NexusWf.Cancel.registered, .{});
    _ = try canceling.activate(&try initActivation(a, "CancelNexus", .{@as([]const u8, "wait_cancellation_completed")}), a);
    _ = try canceling.activate(&try nexusStart(a, 1, .{ .operation_token = "tok" }), a);
    _ = try canceling.activate(&try oneJob(a, .{ .cancel_workflow = .{} }), a);
    canceling.destroy();
}

// Signals to other workflows. `ExternalWf.Kid` never runs here: the tests resolve it.
const ExternalWf = struct {
    const Approve = workflow.SignalDef("approve", fn (approver: []const u8) void);

    /// Describes how `get` ended: "delivered", or the error and the failure's message.
    fn outcome(ctx: *workflow.Context, f: workflow.SignalExternalFuture) ![]const u8 {
        f.get() catch |err| switch (err) {
            error.ExternalWorkflowNotFound, error.ExternalWorkflowRequestFailed => return std.fmt.allocPrint(ctx.allocator(), "{t}: {s}", .{ err, f.failure().?.message }),
            else => return err,
        };
        return "delivered";
    }

    /// Signals the workflow `workflow_id` with an approval from "ann".
    fn signal(ctx: *workflow.Context, workflow_id: []const u8, run_id: []const u8) ![]const u8 {
        return outcome(ctx, ctx.signalExternalWorkflow(Approve, workflow_id, run_id, .{"ann"}));
    }
    const Signal = workflow.Def("SignalOther", signal);

    /// Signals the same workflow twice; a returned error fails the workflow.
    fn signalTwice(ctx: *workflow.Context) !void {
        try ctx.signalExternalWorkflow(Approve, "target", "", .{"ann"}).get();
        try ctx.signalExternalWorkflow(Approve, "target", "", .{"bob"}).get();
    }
    const SignalTwice = workflow.Def("SignalTwice", signalTwice);

    fn kid(ctx: *workflow.Context) !void {
        _ = ctx;
    }
    const Kid = workflow.Def("Kid", kid);

    /// Starts `Kid`, signals it, then waits for it.
    fn signalChild(ctx: *workflow.Context) ![]const u8 {
        const child = ctx.executeChildWorkflow(Kid, .{ .id = "kid-1" }, .{});
        const delivered = try outcome(ctx, try child.signal(Approve, .{"ann"}));
        try child.get();
        return delivered;
    }
    const SignalChild = workflow.Def("SignalChild", signalChild);

    /// Requests cancellation of the run "target-run" of the workflow `workflow_id`.
    fn cancel(ctx: *workflow.Context, workflow_id: []const u8) ![]const u8 {
        return outcome(ctx, ctx.cancelExternalWorkflow(workflow_id, "target-run"));
    }
    const Cancel = workflow.Def("CancelOther", cancel);
};

fn signalResolved(a: Allocator, seq: u32, failure: ?Failure, cause: proto.temporal.api.enums.v1.SignalExternalWorkflowExecutionFailedCause) !act_pb.WorkflowActivation {
    return oneJob(a, .{ .resolve_signal_external_workflow = .{ .seq = seq, .failure = failure, .cause = cause } });
}

test "signal external: SignalExternalWorkflowExecution to the workflow, then its delivery" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, ExternalWf.Signal.registered, .{ .namespace = "ns" });
    defer inst.destroy();

    const first = commandsOf(try inst.activate(&try initActivation(a, "SignalOther", .{ @as([]const u8, "target"), @as([]const u8, "target-run") }), a));
    try testing.expectEqual(@as(usize, 1), first.len);
    const cmd = first[0].variant.?.signal_external_workflow_execution;
    try testing.expectEqual(@as(u32, 1), cmd.seq);
    try testing.expectEqualStrings("approve", cmd.signal_name);
    try testing.expectEqualStrings("\"ann\"", cmd.args.items[0].data);
    const target = cmd.target.?.workflow_execution;
    try testing.expectEqualStrings("ns", target.namespace);
    try testing.expectEqualStrings("target", target.workflow_id);
    try testing.expectEqualStrings("target-run", target.run_id);

    const done = try inst.activate(&try signalResolved(a, 1, null, .SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_UNSPECIFIED), a);
    try testing.expectEqualStrings("\"delivered\"", completedWith(done));
}

test "signal external: a workflow that isn't found returns error.ExternalWorkflowNotFound with the failure" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, ExternalWf.Signal.registered, .{});
    defer inst.destroy();
    _ = try inst.activate(&try initActivation(a, "SignalOther", .{ @as([]const u8, "missing"), @as([]const u8, "") }), a);
    const f = try failure_mod.application(a, .{ .message = "Unable to signal external workflow because it was not found", .type = "ExternalWorkflowExecutionNotFound" });
    const done = try inst.activate(&try signalResolved(a, 1, f, .SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_EXTERNAL_WORKFLOW_EXECUTION_NOT_FOUND), a);
    try testing.expectEqualStrings("\"ExternalWorkflowNotFound: Unable to signal external workflow because it was not found (workflow ID \\\"missing\\\")\"", completedWith(done));

    // Another cause is ExternalWorkflowRequestFailed.
    const other = try create(testing.allocator, ExternalWf.Signal.registered, .{});
    defer other.destroy();
    _ = try other.activate(&try initActivation(a, "SignalOther", .{ @as([]const u8, "busy"), @as([]const u8, "") }), a);
    const limit = try failure_mod.application(a, .{ .message = "Unable to signal external workflow because The per-workflow signal limit was exceeded", .type = "SignalCountLimitExceeded" });
    const failed = try other.activate(&try signalResolved(a, 1, limit, .SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_SIGNAL_COUNT_LIMIT_EXCEEDED), a);
    try testing.expect(std.mem.startsWith(u8, completedWith(failed), "\"ExternalWorkflowRequestFailed: "));
}

test "signal external: an unhandled failure fails the workflow with it; seqs count up" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, ExternalWf.SignalTwice.registered, .{});
    defer inst.destroy();
    _ = try inst.activate(&try initActivation(a, "SignalTwice", .{}), a);
    const second = commandsOf(try inst.activate(&try signalResolved(a, 1, null, .SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_UNSPECIFIED), a));
    try testing.expectEqual(@as(u32, 2), second[0].variant.?.signal_external_workflow_execution.seq);
    const f = try failure_mod.application(a, .{ .message = "Unable to signal external workflow because it was not found", .type = "ExternalWorkflowExecutionNotFound" });
    const done = commandsOf(try inst.activate(&try signalResolved(a, 2, f, .SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_EXTERNAL_WORKFLOW_EXECUTION_NOT_FOUND), a));
    const fail = done[0].variant.?.fail_workflow_execution.failure.?;
    try testing.expectEqualStrings("Unable to signal external workflow because it was not found (workflow ID \"target\")", fail.message);
    try testing.expectEqualStrings("ExternalWorkflowExecutionNotFound", failure_mod.applicationType(fail).?);
}

test "signal external: canceling the waiting coroutine sends CancelSignalWorkflow and returns error.Canceled" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, ExternalWf.Signal.registered, .{});
    defer inst.destroy();
    _ = try inst.activate(&try initActivation(a, "SignalOther", .{ @as([]const u8, "target"), @as([]const u8, "") }), a);
    const cmds = commandsOf(try inst.activate(&try oneJob(a, .{ .cancel_workflow = .{} }), a));
    try testing.expectEqual(@as(usize, 2), cmds.len);
    try testing.expectEqual(@as(u32, 1), cmds[0].variant.?.cancel_signal_workflow.seq);
    try testing.expect(cmds[1].variant.? == .cancel_workflow_execution);
    // The core's resolution of the withdrawn signal that follows changes nothing.
    const cause = .SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_UNSPECIFIED;
    const canceled: Failure = .{ .message = "Signal was cancelled before being sent", .failure_info = .{ .canceled_failure_info = .{} } };
    try testing.expectEqual(@as(usize, 0), commandsOf(try inst.activate(&try signalResolved(a, 1, canceled, cause), a)).len);
}

test "signal external: an empty workflow ID returns error.InvalidOptions and sends nothing" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, ExternalWf.Signal.registered, .{});
    defer inst.destroy();
    const done = commandsOf(try inst.activate(&try initActivation(a, "SignalOther", .{ @as([]const u8, ""), @as([]const u8, "") }), a));
    try testing.expectEqual(@as(usize, 1), done.len);
    try testing.expectEqualStrings("InvalidOptions", done[0].variant.?.fail_workflow_execution.failure.?.message);
}

test "signal external: a child's signal waits for its start and targets it by workflow ID" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, ExternalWf.SignalChild.registered, .{});
    defer inst.destroy();
    const first = commandsOf(try inst.activate(&try initActivation(a, "SignalChild", .{}), a));
    try testing.expectEqual(@as(usize, 1), first.len);
    try testing.expect(first[0].variant.? == .start_child_workflow_execution);
    const started = commandsOf(try inst.activate(&try childStarted(a, 1, "kid-run"), a));
    try testing.expectEqual(@as(usize, 1), started.len);
    const cmd = started[0].variant.?.signal_external_workflow_execution;
    try testing.expectEqualStrings("kid-1", cmd.target.?.child_workflow_id);
    try testing.expectEqual(@as(usize, 0), commandsOf(try inst.activate(&try signalResolved(a, 1, null, .SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_UNSPECIFIED), a)).len);
    const done = try inst.activate(&try childResolved(a, 1, .{ .completed = .{} }), a);
    try testing.expectEqualStrings("\"delivered\"", completedWith(done));
}

test "signal external: eviction ends a wait for a delivery" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, ExternalWf.Signal.registered, .{});
    _ = try inst.activate(&try initActivation(a, "SignalOther", .{ @as([]const u8, "target"), @as([]const u8, "") }), a);
    inst.destroy(); // testing.allocator reports leaks
}

fn cancelResolved(a: Allocator, seq: u32, failure: ?Failure, cause: proto.temporal.api.enums.v1.CancelExternalWorkflowExecutionFailedCause) !act_pb.WorkflowActivation {
    return oneJob(a, .{ .resolve_request_cancel_external_workflow = .{ .seq = seq, .failure = failure, .cause = cause } });
}

test "cancel external: RequestCancelExternalWorkflowExecution to the workflow, then its delivery" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, ExternalWf.Cancel.registered, .{ .namespace = "ns" });
    defer inst.destroy();

    const first = commandsOf(try inst.activate(&try initActivation(a, "CancelOther", .{@as([]const u8, "target")}), a));
    try testing.expectEqual(@as(usize, 1), first.len);
    const cmd = first[0].variant.?.request_cancel_external_workflow_execution;
    try testing.expectEqual(@as(u32, 1), cmd.seq);
    try testing.expectEqualStrings("ns", cmd.workflow_execution.?.namespace);
    try testing.expectEqualStrings("target", cmd.workflow_execution.?.workflow_id);
    try testing.expectEqualStrings("target-run", cmd.workflow_execution.?.run_id);

    const done = try inst.activate(&try cancelResolved(a, 1, null, .CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_UNSPECIFIED), a);
    try testing.expectEqualStrings("\"delivered\"", completedWith(done));
}

test "cancel external: a workflow that isn't found returns error.ExternalWorkflowNotFound with the failure" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, ExternalWf.Cancel.registered, .{});
    defer inst.destroy();
    _ = try inst.activate(&try initActivation(a, "CancelOther", .{@as([]const u8, "missing")}), a);
    const f = try failure_mod.application(a, .{ .message = "Unable to cancel external workflow because not found", .type = "ExternalWorkflowExecutionNotFound" });
    const done = try inst.activate(&try cancelResolved(a, 1, f, .CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_EXTERNAL_WORKFLOW_EXECUTION_NOT_FOUND), a);
    try testing.expectEqualStrings("\"ExternalWorkflowNotFound: Unable to cancel external workflow because not found (workflow ID \\\"missing\\\")\"", completedWith(done));
}

test "cancel external: canceling the waiting coroutine returns error.Canceled and leaves the request" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, ExternalWf.Cancel.registered, .{});
    defer inst.destroy();
    _ = try inst.activate(&try initActivation(a, "CancelOther", .{@as([]const u8, "target")}), a);
    const cmds = commandsOf(try inst.activate(&try oneJob(a, .{ .cancel_workflow = .{} }), a));
    try testing.expectEqual(@as(usize, 1), cmds.len);
    try testing.expect(cmds[0].variant.? == .cancel_workflow_execution);
    try testing.expectEqual(@as(usize, 0), commandsOf(try inst.activate(&try cancelResolved(a, 1, null, .CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_UNSPECIFIED), a)).len);
}

test "cancel external: an empty workflow ID returns error.InvalidOptions and sends nothing" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, ExternalWf.Cancel.registered, .{});
    defer inst.destroy();
    const done = commandsOf(try inst.activate(&try initActivation(a, "CancelOther", .{@as([]const u8, "")}), a));
    try testing.expectEqual(@as(usize, 1), done.len);
    try testing.expectEqualStrings("InvalidOptions", done[0].variant.?.fail_workflow_execution.failure.?.message);
}

// Continue-as-new.
const Continue = struct {
    const Again = workflow.SignalDef("again", fn () void);

    /// Upserts the memo, then continues as new with every option set.
    fn withOptions(ctx: *workflow.Context, n: u32) !u32 {
        try ctx.upsertMemo(.{ .status = "upserted" });
        return ctx.continueAsNew(WithOptions, .{
            .task_queue = "elsewhere",
            .run_timeout = .fromSeconds(60),
            .task_timeout = .fromSeconds(5),
            .memo = try converter.toFields(ctx.allocator(), .{ .round = n + 1 }),
            .retry_policy = .{ .maximum_attempts = 3, .non_retryable_error_types = &.{"Fatal"} },
        }, .{n + 1});
    }
    /// Upserts the memo, then continues as new with the default options.
    fn defaults(ctx: *workflow.Context, n: u32) !u32 {
        try ctx.upsertMemo(.{ .status = "upserted" });
        return ctx.continueAsNew(Defaults, .{}, .{n + 1});
    }
    fn bare(_: *workflow.Context) !u32 {
        return error.ContinueAsNew;
    }
    fn fromSignal(ctx: *workflow.Context) !u32 {
        try ctx.setSignalHandler(Again, ctx, continueFromHandler);
        try ctx.io().sleep(.fromSeconds(3600), .awake);
        return 0;
    }
    fn continueFromHandler(ctx: *workflow.Context) !void {
        return ctx.continueAsNew(FromSignal, .{}, .{});
    }
    const WithOptions = workflow.Def("WithOptions", withOptions);
    const Defaults = workflow.Def("Defaults", defaults);
    const Bare = workflow.Def("Bare", bare);
    const FromSignal = workflow.Def("FromSignal", fromSignal);
};

test "continue-as-new: ContinueAsNewWorkflowExecution carries the type, arguments and options" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, Continue.WithOptions.registered, .{ .task_queue = "tq" });
    defer inst.destroy();

    const cmds = commandsOf(try inst.activate(&try initActivation(a, "WithOptions", .{@as(u32, 1)}), a));
    try testing.expectEqual(@as(usize, 2), cmds.len);
    const can = cmds[1].variant.?.continue_as_new_workflow_execution;
    try testing.expectEqualStrings("WithOptions", can.workflow_type);
    try testing.expectEqualStrings("elsewhere", can.task_queue);
    try testing.expectEqual(@as(usize, 1), can.arguments.items.len);
    try testing.expectEqualStrings("2", can.arguments.items[0].data);
    try testing.expectEqual(@as(i64, 60), can.workflow_run_timeout.?.seconds);
    try testing.expectEqual(@as(i64, 5), can.workflow_task_timeout.?.seconds);
    try testing.expectEqual(@as(usize, 1), can.memo.items.len);
    try testing.expectEqualStrings("round", can.memo.items[0].key);
    try testing.expectEqualStrings("2", can.memo.items[0].value.?.data);
    try testing.expectEqual(@as(i32, 3), can.retry_policy.?.maximum_attempts);
    try testing.expectEqualStrings("Fatal", can.retry_policy.?.non_retryable_error_types.items[0]);
    try testing.expect(inst.finished);
}

test "continue-as-new: default options keep the task queue, timeouts and retry policy, and send the current memo" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, Continue.Defaults.registered, .{ .task_queue = "tq" });
    defer inst.destroy();

    var act = try initActivation(a, "Defaults", .{@as(u32, 7)});
    var start_memo: common.Memo = .{};
    for (try converter.toFields(a, .{ .owner = "data", .status = "queued" })) |f|
        try start_memo.fields.append(a, .{ .key = f.name, .value = f.value });
    act.jobs.items[0].variant.?.initialize_workflow.memo = start_memo;

    const cmds = commandsOf(try inst.activate(&act, a));
    const can = cmds[1].variant.?.continue_as_new_workflow_execution;
    try testing.expectEqualStrings("Defaults", can.workflow_type);
    try testing.expectEqualStrings("", can.task_queue);
    try testing.expectEqualStrings("8", can.arguments.items[0].data);
    try testing.expect(can.workflow_run_timeout == null);
    try testing.expect(can.workflow_task_timeout == null);
    try testing.expect(can.retry_policy == null);
    try testing.expectEqual(@as(usize, 2), can.memo.items.len);
    try testing.expectEqualStrings("owner", can.memo.items[0].key);
    try testing.expectEqualStrings("status", can.memo.items[1].key);
    try testing.expectEqualStrings("\"upserted\"", can.memo.items[1].value.?.data);
}

test "continue-as-new: error.ContinueAsNew without ctx.continueAsNew fails the workflow saying so" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, Continue.Bare.registered, .{});
    defer inst.destroy();

    const cmds = commandsOf(try inst.activate(&try initActivation(a, "Bare", .{}), a));
    try testing.expectEqual(@as(usize, 1), cmds.len);
    const f = cmds[0].variant.?.fail_workflow_execution.failure.?;
    try testing.expect(std.mem.indexOf(u8, f.message, "without calling ctx.continueAsNew") != null);
    try testing.expectEqualStrings("ContinueAsNew", failure_mod.applicationType(f).?);
}

test "continue-as-new: a signal handler's error.ContinueAsNew fails the workflow instead" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, Continue.FromSignal.registered, .{});
    defer inst.destroy();

    _ = try inst.activate(&try initActivation(a, "FromSignal", .{}), a);
    const cmds = commandsOf(try inst.activate(&try oneJob(a, .{ .signal_workflow = .{ .signal_name = "again" } }), a));
    try testing.expectEqual(@as(usize, 1), cmds.len);
    const f = cmds[0].variant.?.fail_workflow_execution.failure.?;
    try testing.expect(std.mem.indexOf(u8, f.message, "only the workflow function can continue as new") != null);
}

test "continue-as-new: history length, size and suggestion follow each activation but not query-only ones" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, Nap.registered, .{});
    defer inst.destroy();

    var init = try initActivation(a, "Nap", .{});
    init.history_length = 3;
    init.history_size_bytes = 300;
    _ = try inst.activate(&init, a);
    try testing.expectEqual(@as(u32, 3), inst.ctx.historyLength());
    try testing.expectEqual(@as(u64, 300), inst.ctx.historySizeBytes());
    try testing.expect(!inst.ctx.continueAsNewSuggested());

    var query = try oneJob(a, .{ .query_workflow = .{ .query_id = "q", .query_type = "none" } });
    query.history_length = 99;
    query.history_size_bytes = 9900;
    query.continue_as_new_suggested = true;
    _ = try inst.activate(&query, a);
    try testing.expectEqual(@as(u32, 3), inst.ctx.historyLength());
    try testing.expectEqual(@as(u64, 300), inst.ctx.historySizeBytes());
    try testing.expect(!inst.ctx.continueAsNewSuggested());

    var fire = try oneJob(a, .{ .fire_timer = .{ .seq = 1 } });
    fire.history_length = 8;
    fire.history_size_bytes = 800;
    fire.continue_as_new_suggested = true;
    _ = try inst.activate(&fire, a);
    try testing.expectEqual(@as(u32, 8), inst.ctx.historyLength());
    try testing.expectEqual(@as(u64, 800), inst.ctx.historySizeBytes());
    try testing.expect(inst.ctx.continueAsNewSuggested());
}

// Struct workflows: handlers declared on the workflow type and registered at initialization.
// `Account.events` records what `run` and the handlers did, in order.
const StructWorkflows = struct {
    const Deposit = workflow.SignalDef("deposit", fn (amount: i64) void);
    const Withdraw = workflow.SignalDef("withdraw", fn (amount: i64) void);
    const Close = workflow.SignalDef("close", fn () void);
    const Balance = workflow.QueryDef("balance", fn () i64);
    const SetLimit = workflow.UpdateDef("set_limit", fn (limit: i64) i64);

    const Account = struct {
        ctx: *workflow.Context = undefined,
        events: std.ArrayList(u8) = .empty,
        balance: i64 = 0,
        limit: i64 = 100,
        closed: bool = false,

        // `deposit` is declared before `withdraw`, so arrival order differs from declaration
        // order in the tests that send `withdraw` first.
        pub const queries = .{ .currentBalance = Balance };
        pub const signals = .{ .deposit = Deposit, .withdraw = Withdraw, .close = Close };
        pub const updates = .{ .setLimit = .{ SetLimit, .checkLimit } };

        pub fn run(s: *Account, ctx: *workflow.Context, opening: i64) ![]const u8 {
            s.ctx = ctx;
            try s.note("run {d}", .{opening});
            s.balance += opening;
            try ctx.await(s, isClosed);
            try ctx.await(ctx, workflow.Context.allHandlersFinished);
            return s.events.items;
        }
        fn note(s: *Account, comptime fmt: []const u8, args: anytype) !void {
            const a = s.ctx.allocator();
            if (s.events.items.len > 0) try s.events.append(a, ',');
            try s.events.print(a, fmt, args);
        }
        pub fn currentBalance(s: *Account) i64 {
            return s.balance;
        }
        pub fn deposit(s: *Account, amount: i64) !void {
            try s.note("deposit {d}", .{amount});
            s.balance += amount;
        }
        pub fn withdraw(s: *Account, amount: i64) !void {
            try s.note("withdraw {d}", .{amount});
            s.balance -= amount;
        }
        pub fn close(s: *Account) void {
            s.closed = true;
        }
        /// Returns the previous limit.
        pub fn setLimit(s: *Account, limit: i64) !i64 {
            try s.note("limit {d}", .{limit});
            defer s.limit = limit;
            return s.limit;
        }
        /// Reads only `balance`, which is 0 in `Account{}` and set by `run`.
        pub fn checkLimit(s: *Account, limit: i64) error{LimitBelowBalance}!void {
            if (limit < s.balance) return error.LimitBelowBalance;
        }
        fn isClosed(s: *Account) bool {
            return s.closed;
        }
    };

    const Ping = workflow.SignalDef("ping", fn () void);
    const Extra = workflow.SignalDef("extra", fn () void);

    /// Declares `ping`, then replaces its handler from `run` and adds `extra` at run time.
    const Rewired = struct {
        pings: []const u8 = "",
        extras: u32 = 0,

        pub const signals = .{ .declaredPing = Ping };

        pub fn run(s: *Rewired, ctx: *workflow.Context) ![]const u8 {
            try ctx.setSignalHandler(Ping, s, Rewired.runtimePing);
            try ctx.setSignalHandler(Extra, s, Rewired.extra);
            try ctx.await(s, twoExtras);
            return s.pings;
        }
        pub fn declaredPing(s: *Rewired) void {
            s.pings = "declared";
        }
        fn runtimePing(s: *Rewired) void {
            s.pings = "runtime";
        }
        fn extra(s: *Rewired) void {
            s.extras += 1;
        }
        fn twoExtras(s: *Rewired) bool {
            return s.extras == 2;
        }
    };
};
const AccountWf = workflow.Def("Account", StructWorkflows.Account);
const RewiredWf = workflow.Def("Rewired", StructWorkflows.Rewired);

fn accountOf(inst: *Instance) *StructWorkflows.Account {
    return @ptrCast(@alignCast(inst.run_state.?));
}

test "struct workflow: signals and updates sent with the start reach the declared handlers in arrival order, once run blocks" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, AccountWf.registered, .{});
    defer inst.destroy();

    // The validator runs on delivery, before `run`, and sees Account{} (balance 0), so a limit
    // of 10 passes it although `run` makes the balance 50.
    var start = try initActivation(a, "Account", .{@as(i64, 50)});
    try start.jobs.append(a, .{ .variant = try signalJob(a, "withdraw", .{@as(i64, 20)}) });
    try start.jobs.append(a, .{ .variant = try updateJob(a, "u1", "set_limit", .{@as(i64, 10)}, true) });
    try start.jobs.append(a, .{ .variant = try signalJob(a, "deposit", .{@as(i64, 5)}) });
    const first = commandsOf(try inst.activate(&start, a));
    try testing.expectEqual(@as(usize, 2), first.len);
    try testing.expect(try updateResponse(first[0], "u1") == .accepted);
    try testing.expectEqualStrings("100", (try updateResponse(first[1], "u1")).completed.data);
    try testing.expectEqual(@as(usize, 0), inst.buffered_signals.items.len);
    try testing.expectEqual(@as(usize, 0), inst.buffered_updates.items.len);
    try testing.expectEqualStrings("run 50,withdraw 20,limit 10,deposit 5", accountOf(inst).events.items);

    // Now the validator sees the balance `run` and the handlers left (35). On replay it
    // doesn't run, so the same update is accepted.
    var later = try oneJob(a, try updateJob(a, "u2", "set_limit", .{@as(i64, 10)}, true));
    try later.jobs.append(a, .{ .variant = try updateJob(a, "u3", "set_limit", .{@as(i64, 20)}, false) });
    const second = commandsOf(try inst.activate(&later, a));
    try testing.expectEqual(@as(usize, 3), second.len);
    try testing.expectEqualStrings("LimitBelowBalance", (try updateResponse(second[0], "u2")).rejected.message);
    try testing.expect(try updateResponse(second[1], "u3") == .accepted);
    try testing.expectEqualStrings("10", (try updateResponse(second[2], "u3")).completed.data);

    const balance = try inst.activate(&try queryActivation(a, &.{.{ "q1", "balance", &.{} }}), a);
    try testing.expectEqualStrings("35", answer(balance, 0).variant.?.succeeded.response.?.data);

    try testing.expectEqualStrings("\"run 50,withdraw 20,limit 10,deposit 5,limit 20\"", completedWith(try inst.activate(&try oneJob(a, try signalJob(a, "close", .{})), a)));
    // The declared query handler still answers after `run` returned.
    const after = try inst.activate(&try queryActivation(a, &.{.{ "q2", "balance", &.{} }}), a);
    try testing.expectEqualStrings("35", answer(after, 0).variant.?.succeeded.response.?.data);
}

test "struct workflow: undecodable start arguments fail the run without running its handlers" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, AccountWf.registered, .{});
    defer inst.destroy();

    var start = try initActivation(a, "Account", .{@as([]const u8, "fifty")});
    try start.jobs.append(a, .{ .variant = try signalJob(a, "deposit", .{@as(i64, 5)}) });
    try start.jobs.append(a, .{ .variant = try updateJob(a, "u1", "set_limit", .{@as(i64, 10)}, true) });
    const cmds = commandsOf(try inst.activate(&start, a));
    try testing.expectEqual(@as(usize, 2), cmds.len);
    try testing.expect(try updateResponse(cmds[0], "u1") == .accepted);
    try testing.expect(cmds[1].variant.? == .fail_workflow_execution);
    try testing.expectEqual(@as(usize, 0), accountOf(inst).events.items.len);
    try testing.expectEqual(@as(i64, 0), accountOf(inst).balance);
    try testing.expect(inst.ctx.allHandlersFinished());
}

test "struct workflow: each run starts from W{}, and eviction frees it" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();

    const first = try create(testing.allocator, AccountWf.registered, .{});
    _ = try first.activate(&try initActivation(a, "Account", .{@as(i64, 50)}), a);
    _ = try first.activate(&try oneJob(a, try signalJob(a, "deposit", .{@as(i64, 5)})), a);
    try testing.expectEqual(@as(i64, 55), accountOf(first).balance);
    first.destroy(); // testing.allocator fails the test if the state or its events leak

    const second = try create(testing.allocator, AccountWf.registered, .{});
    defer second.destroy();
    _ = try second.activate(&try initActivation(a, "Account", .{@as(i64, 7)}), a);
    try testing.expectEqual(@as(i64, 7), accountOf(second).balance);
    try testing.expectEqualStrings("run 7", accountOf(second).events.items);
}

test "struct workflow: run may register handlers at run time and replace a declared one" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, RewiredWf.registered, .{});
    defer inst.destroy();

    _ = try inst.activate(&try initActivation(a, "Rewired", .{}), a);
    var act = try oneJob(a, try signalJob(a, "ping", .{}));
    try act.jobs.append(a, .{ .variant = try signalJob(a, "extra", .{}) });
    try act.jobs.append(a, .{ .variant = try signalJob(a, "extra", .{}) });
    try testing.expectEqualStrings("\"runtime\"", completedWith(try inst.activate(&act, a)));
}

// ---------------------------------------------------------------- per-run memory

/// Counts log records and keeps the last one at or above `min`.
const LogRecorder = struct {
    arena: std.heap.ArenaAllocator,
    min: log.Level,
    count: usize = 0,
    message: []const u8 = "",
    fields: []const log.Field = &.{},

    fn sink(r: *LogRecorder) log.Sink {
        return .{ .ptr = r, .vtable = &.{ .log = record } };
    }

    fn record(ptr: ?*anyopaque, level: log.Level, message: []const u8, fields: []const log.Field) void {
        const r: *LogRecorder = @ptrCast(@alignCast(ptr.?));
        if (@backingInt(level) > @backingInt(r.min)) return;
        const a = r.arena.allocator();
        r.count += 1;
        r.message = a.dupe(u8, message) catch "";
        const copy = a.alloc(log.Field, fields.len) catch return;
        for (fields, copy) |f, *c| c.* = .{ .key = a.dupe(u8, f.key) catch "", .value = a.dupe(u8, f.value) catch "" };
        r.fields = copy;
    }

    fn get(r: *const LogRecorder, key: []const u8) ?[]const u8 {
        for (r.fields) |f| if (std.mem.eql(u8, f.key, key)) return f.value;
        return null;
    }
};

const MemoryHog = struct {
    /// Allocates `chunk` bytes, then sleeps a second, `rounds` times; then allocates once more
    /// and completes. Answers the `status` query.
    fn run(ctx: *workflow.Context, chunk: usize, rounds: usize) ![]const u8 {
        try ctx.setQueryHandler(QueryWf.Status, ctx, status);
        for (0..rounds) |_| {
            @memset(try ctx.allocator().alloc(u8, chunk), 0);
            try ctx.io().sleep(.fromSeconds(1), .awake);
        }
        @memset(try ctx.allocator().alloc(u8, chunk), 0);
        return "done";
    }

    fn status(_: *workflow.Context) []const u8 {
        return "hogging";
    }

    /// Starts and awaits `n` futures one after another, then sleeps.
    fn futures(ctx: *workflow.Context, n: usize) !u64 {
        const io = ctx.io();
        var sum: u64 = 0;
        for (0..n) |i| {
            var f = io.async(square, .{@as(u64, i)});
            sum += f.await(io);
        }
        try io.sleep(.fromSeconds(1), .awake);
        return sum;
    }

    fn square(x: u64) u64 {
        return x * x;
    }

    /// Waits on an activity while a group member sleeps.
    fn timerAndActivity(ctx: *workflow.Context) ![]const u8 {
        const io = ctx.io();
        var group: std.Io.Group = .init;
        defer group.cancel(io);
        group.async(io, napQuietly, .{io});
        return ctx.executeActivity(Greet, .{ .start_to_close_timeout = .fromSeconds(10) }, .{"x"}).get();
    }

    fn napQuietly(io: std.Io) void {
        io.sleep(.fromSeconds(10), .awake) catch {};
    }

    /// Blocks cancellation, then sleeps: eviction can't end the sleep.
    fn stuck(ctx: *workflow.Context) ![]const u8 {
        _ = ctx.io().swapCancelProtection(.blocked);
        try ctx.io().sleep(.fromSeconds(60), .awake);
        return "woke";
    }
};
const Oversize = struct {
    /// More than the largest pooled block size class.
    const Big = [100_000]u8;
    /// More aligned than pooled blocks.
    const Wide = struct { v: u64 align(64) };

    /// `rounds` times: starts and awaits, one after another, `n` coroutines with an oversize
    /// result and `n` with an over-aligned argument, then sleeps.
    fn run(ctx: *workflow.Context, n: usize, rounds: usize) !u64 {
        const io = ctx.io();
        var sum: u64 = 0;
        for (0..rounds) |_| {
            for (0..n) |i| {
                var big = io.async(fill, .{ io, @as(u64, i) });
                sum += big.await(io)[@sizeOf(Big) - 1];
                var wide = io.async(widen, .{ io, Wide{ .v = i } });
                sum += wide.await(io);
            }
            try io.sleep(.fromSeconds(1), .awake);
        }
        return sum;
    }

    // Each starts a small coroutine whose block, pooled once it finishes, is allocated after
    // the oversize block: an arena can't reclaim a block with a later allocation behind it.
    fn fill(io: std.Io, i: u64) Big {
        var inner = io.async(MemoryHog.square, .{i});
        var b: Big = undefined;
        @memset(&b, @truncate(inner.await(io)));
        return b;
    }

    fn widen(io: std.Io, w: Wide) u64 {
        var inner = io.async(MemoryHog.square, .{w.v});
        return inner.await(io);
    }
};
const OversizeWf = workflow.Def("Oversize", Oversize.run);
const MemoryHogWf = workflow.Def("MemoryHog", MemoryHog.run);
const FuturesWf = workflow.Def("Futures", MemoryHog.futures);
const StuckWf = workflow.Def("Stuck", MemoryHog.stuck);
const TimerAndActivityWf = workflow.Def("TimerAndActivity", MemoryHog.timerAndActivity);

const hog_stack = 64 * 1024;

/// Runs MemoryHog(chunk, 3) to completion with no limit and returns `memoryUsed` after each
/// of its four activations.
fn hogUsage(a: Allocator, chunk: usize, replaying: bool) ![4]usize {
    const inst = try create(testing.allocator, MemoryHogWf.registered, .{ .stack_size = hog_stack });
    defer inst.destroy();
    var used: [4]usize = undefined;
    for (&used, 0..) |*u, i| {
        var act = if (i == 0)
            try initActivation(a, "MemoryHog", .{ chunk, @as(usize, 3) })
        else
            try oneJob(a, .{ .fire_timer = .{ .seq = @intCast(i) } });
        act.is_replaying = replaying;
        const c = try inst.activate(&act, a);
        try testing.expect(c.status.? == .successful);
        u.* = inst.memoryUsed();
    }
    return used;
}

test "memoryUsed counts requested bytes and the run's stacks" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, MemoryHogWf.registered, .{ .stack_size = hog_stack });
    defer inst.destroy();
    _ = try inst.activate(&try initActivation(a, "MemoryHog", .{ @as(usize, 1000), @as(usize, 3) }), a);
    // The workflow coroutine is blocked in its sleep: one stack, nothing pooled.
    try testing.expectEqual(@as(usize, 1), inst.sched.stackCount());
    try testing.expectEqual(inst.allocated + hog_stack, inst.memoryUsed());

    // Only the workflow's own allocations differ between these runs (the arguments encode
    // to the same length).
    const small = try hogUsage(a, 1000, false);
    const large = try hogUsage(a, 2000, false);
    for (small, large, 1..) |s, l, rounds| try testing.expectEqual(s + 1000 * rounds, l);
}

test "memoryUsed is the same whether or not the activations replay" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    try testing.expectEqual(try hogUsage(a, 4096, false), try hogUsage(a, 4096, true));
}

test "a run over its memory limit fails the workflow task, the same way when replaying" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const used = try hogUsage(a, 4096, false);
    const limit = used[1];

    for ([_]bool{ false, true }) |replaying| {
        var recorder: LogRecorder = .{ .arena = .init(testing.allocator), .min = .err };
        defer recorder.arena.deinit();
        const inst = try create(testing.allocator, MemoryHogWf.registered, .{
            .stack_size = hog_stack,
            .memory_limit = limit,
            .logger = recorder.sink(),
            .log_during_replay = true,
        });
        defer inst.destroy();
        var first = try initActivation(a, "MemoryHog", .{ @as(usize, 4096), @as(usize, 3) });
        first.is_replaying = replaying;
        try testing.expect((try inst.activate(&first, a)).status.? == .successful);
        var second = try oneJob(a, .{ .fire_timer = .{ .seq = 1 } });
        second.is_replaying = replaying;
        try testing.expect((try inst.activate(&second, a)).status.? == .successful);
        try testing.expectEqual(@as(usize, 0), recorder.count);

        var third = try oneJob(a, .{ .fire_timer = .{ .seq = 2 } });
        third.is_replaying = replaying;
        const c = try inst.activate(&third, a);
        const f = c.status.?.failed.failure.?;
        try testing.expectEqualStrings("WorkflowMemoryLimitExceeded", failure_mod.applicationType(f).?);
        const want = try std.fmt.allocPrint(a, "workflow run holds {d} bytes ({d} allocated from ctx.allocator() and the runtime, {d} in 1 coroutine stacks), over worker.Options.workflow_memory_limit = {d}; raise the limit, or keep less state in the run (continue-as-new sooner)", .{ used[2], used[2] - hog_stack, hog_stack, limit });
        try testing.expectEqualStrings(want, f.message);
        try testing.expectEqual(@as(usize, 1), recorder.count);
        try testing.expectEqualStrings(want, recorder.message);
        try testing.expectEqualStrings(try std.fmt.allocPrint(a, "{d}", .{used[2]}), recorder.get("used").?);
        try testing.expectEqualStrings(try std.fmt.allocPrint(a, "{d}", .{limit}), recorder.get("limit").?);
    }
}

test "a run under its limit, or with none, is not failed; nor is one that finishes or answers a query over it" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const used = try hogUsage(a, 4096, false);

    for ([_]?usize{ null, used[3], used[2] }) |limit| {
        const inst = try create(testing.allocator, MemoryHogWf.registered, .{ .stack_size = hog_stack, .memory_limit = limit });
        defer inst.destroy();
        _ = try inst.activate(&try initActivation(a, "MemoryHog", .{ @as(usize, 4096), @as(usize, 3) }), a);
        _ = try inst.activate(&try oneJob(a, .{ .fire_timer = .{ .seq = 1 } }), a);
        _ = try inst.activate(&try oneJob(a, .{ .fire_timer = .{ .seq = 2 } }), a);
        // With the limit at used[2], the run goes over it in the activation that completes it.
        const done = try inst.activate(&try oneJob(a, .{ .fire_timer = .{ .seq = 3 } }), a);
        try testing.expectEqualStrings("\"done\"", commandsOf(done)[0].variant.?.complete_workflow_execution.result.?.data);
    }

    // A query-only activation of a run already over its limit answers.
    var recorder: LogRecorder = .{ .arena = .init(testing.allocator), .min = .err };
    defer recorder.arena.deinit();
    const inst = try create(testing.allocator, MemoryHogWf.registered, .{ .stack_size = hog_stack, .memory_limit = used[0] - 1, .logger = recorder.sink() });
    defer inst.destroy();
    try testing.expect((try inst.activate(&try initActivation(a, "MemoryHog", .{ @as(usize, 4096), @as(usize, 3) }), a)).status.? == .failed);
    const q = try inst.activate(&try queryActivation(a, &.{.{ "q1", "status", &.{} }}), a);
    try testing.expectEqual(@as(usize, 1), recorder.count);
    try testing.expectEqualStrings("\"hogging\"", answer(q, 0).variant.?.succeeded.response.?.data);
}

test "finished coroutines' blocks and stacks are reused" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    var used: [2]usize = undefined;
    for (&used, [_]usize{ 100, 999 }) |*u, n| {
        const inst = try create(testing.allocator, FuturesWf.registered, .{ .stack_size = hog_stack });
        defer inst.destroy();
        _ = try inst.activate(&try initActivation(a, "Futures", .{n}), a);
        u.* = inst.memoryUsed();
    }
    try testing.expectEqual(used[0], used[1]);
}

test "oversize and over-aligned coroutine blocks are reused: neither memoryUsed nor the arena grows" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, OversizeWf.registered, .{ .stack_size = 2 * 1024 * 1024 });
    defer inst.destroy();
    // Each activation runs 20 of each coroutine. Freed blocks that an arena couldn't reclaim
    // would add 2 MB of arena per activation while memoryUsed() stayed flat.
    var used: [3]usize = undefined;
    var capacity: [3]usize = undefined;
    for (&used, &capacity, 0..) |*u, *cap, i| {
        const act = if (i == 0)
            try initActivation(a, "Oversize", .{ @as(usize, 20), @as(usize, 3) })
        else
            try oneJob(a, .{ .fire_timer = .{ .seq = @intCast(i) } });
        try testing.expect((try inst.activate(&act, a)).status.? == .successful);
        u.* = inst.memoryUsed();
        cap.* = inst.arena_state.queryCapacity();
    }
    // The finished oversize block is held for reuse, and counted.
    try testing.expect(inst.allocated > @sizeOf(Oversize.Big));
    try testing.expectEqual(used[0], used[1]);
    try testing.expectEqual(used[1], used[2]);
    try testing.expectEqual(capacity[0], capacity[1]);
    try testing.expectEqual(capacity[1], capacity[2]);
}

test "an update-only activation that only rejects updates is not checked against the limit" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const used = try hogUsage(a, 4096, false);
    var recorder: LogRecorder = .{ .arena = .init(testing.allocator), .min = .err };
    defer recorder.arena.deinit();
    const inst = try create(testing.allocator, MemoryHogWf.registered, .{ .stack_size = hog_stack, .memory_limit = used[0] - 1, .logger = recorder.sink() });
    defer inst.destroy();
    try testing.expect((try inst.activate(&try initActivation(a, "MemoryHog", .{ @as(usize, 4096), @as(usize, 3) }), a)).status.? == .failed);
    try testing.expectEqual(@as(usize, 1), recorder.count);

    // Still over the limit, but no coroutine runs: the update is rejected and the task passes.
    const rejected = commandsOf(try inst.activate(&try oneJob(a, try updateJob(a, "u1", "nope", .{}, true)), a));
    try testing.expectEqual(@as(usize, 1), rejected.len);
    try testing.expectEqualStrings("UpdateNotRegistered", failure_mod.applicationType((try updateResponse(rejected[0], "u1")).rejected).?);
    try testing.expectEqual(@as(usize, 1), recorder.count);

    // An activation that runs the coroutines is checked.
    try testing.expect((try inst.activate(&try oneJob(a, .{ .fire_timer = .{ .seq = 1 } }), a)).status.? == .failed);
    try testing.expectEqual(@as(usize, 2), recorder.count);
}

test "a coroutine that refuses to exit on eviction is reported, and the rest of the run is freed" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    var recorder: LogRecorder = .{ .arena = .init(testing.allocator), .min = .warn };
    defer recorder.arena.deinit();
    const inst = try create(testing.allocator, StuckWf.registered, .{ .stack_size = hog_stack, .logger = recorder.sink() });
    _ = try inst.activate(&try initActivation(a, "Stuck", .{}), a);
    try testing.expectEqual(@as(usize, 1), inst.teardown());
    inst.destroy(); // testing.allocator reports leaks; the stuck stack is the only thing kept
    try testing.expectEqual(@as(usize, 1), recorder.count);
    try testing.expectEqualStrings("workflow coroutines did not exit on eviction (blocked with cancel protection on?); leaking their stacks", recorder.message);
    try testing.expectEqualStrings("1", recorder.get("count").?);
    try testing.expectEqualStrings(try std.fmt.allocPrint(a, "{d}", .{hog_stack}), recorder.get("stack_bytes").?);
}

test "eviction sends nothing: commands from unwinding coroutines are dropped" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, TimerAndActivityWf.registered, .{});
    // Blocked on an activity and on a timer; unwinding cancels both.
    const first = commandsOf(try inst.activate(&try initActivation(a, "TimerAndActivity", .{}), a));
    try testing.expectEqual(@as(usize, 2), first.len);
    // Outside eviction, the same cancellation produces commands.
    const canceled = commandsOf(try inst.activate(&try oneJob(a, .{ .cancel_workflow = .{} }), a));
    try testing.expect(canceled.len > 0);
    inst.destroy();

    const evicted = try create(testing.allocator, TimerAndActivityWf.registered, .{});
    _ = try evicted.activate(&try initActivation(a, "TimerAndActivity", .{}), a);
    try testing.expectEqual(@as(usize, 0), evicted.teardown());
    try testing.expectEqual(@as(usize, 0), evicted.commands.items.len);
    evicted.destroy();
}

// A workflow canceled in the task that starts it, with an update already waiting: the
// workflow function registers the update's handler with a pending cancel, so the validator
// runs inline in the workflow's own coroutine. A validator that sleeps there starts a timer,
// which read-only code may not do: the sleep must be refused (naming it) before it looks at
// the coroutine's cancel request, which belongs to the workflow, not the validator. On
// replay the rejected update isn't in history and the validator doesn't run, so the workflow
// function sees that cancel at its own sleep and the run is canceled.
const SleepingValidatorWf = struct {
    const Set = workflow.UpdateDef("set", fn () void);
    const State = struct {
        ctx: *workflow.Context,
        fn set(_: *State) void {}
        fn validate(s: *State) !void {
            try s.ctx.sleep(.fromSeconds(1), .{});
        }
    };
    fn run(ctx: *workflow.Context) ![]const u8 {
        const s = try ctx.allocator().create(State);
        s.* = .{ .ctx = ctx };
        try ctx.setUpdateHandlerWithValidator(Set, s, State.set, State.validate);
        try ctx.sleep(.fromSeconds(10), .{});
        return "slept";
    }
};
const SleepingValidatorWfDef = workflow.Def("SleepingValidator", SleepingValidatorWf.run);

test "update: a validator that sleeps in a canceled workflow's coroutine is refused and leaves the cancel to the workflow" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, SleepingValidatorWfDef.registered, .{});
    defer inst.destroy();
    var act = try initActivation(a, "SleepingValidator", .{});
    try act.jobs.append(a, .{ .variant = try updateJob(a, "u1", "set", .{}, true) });
    try act.jobs.append(a, .{ .variant = .{ .cancel_workflow = .{} } });
    const c = try inst.activate(&act, a);
    try testing.expect(c.status.? == .successful);
    var rejected: ?Failure = null;
    var canceled = false;
    for (commandsOf(c)) |cmd| switch (cmd.variant.?) {
        .update_response => rejected = (try updateResponse(cmd, "u1")).rejected,
        .cancel_workflow_execution => canceled = true,
        .start_timer => return error.TestUnexpectedResult, // the workflow lost its cancel
        else => {},
    };
    try expectReadOnlyViolation(rejected.?, "update validator", "set", "sleep");
    try testing.expect(canceled);
}

// Failure policy: which errors fail the workflow task instead of the workflow.
const Policy = struct {
    fn missing(_: *workflow.Context) ![]const u8 {
        return error.ConfigMissing;
    }
    fn outOfMemory(_: *workflow.Context) ![]const u8 {
        return error.OutOfMemory;
    }
    fn canceled(ctx: *workflow.Context) ![]const u8 {
        try ctx.io().sleep(.fromSeconds(3600), .awake);
        return "slept";
    }
    fn dynamic(_: *workflow.Context, _: converter.EncodedValues) ![]const u8 {
        return error.ConfigMissing;
    }
    const Stateful = struct {
        calls: u32 = 0,
        pub fn run(s: *Stateful, _: *workflow.Context) ![]const u8 {
            s.calls += 1;
            return error.ConfigMissing;
        }
    };
    const Missing = workflow.Def("Missing", missing);
    const MissingListed = workflow.DefWithOptions("MissingListed", missing, .{ .task_failure_errors = &.{error.ConfigMissing} });
    const OutOfMemoryWf = workflow.Def("OutOfMemory", outOfMemory);
    const Canceled = workflow.DefWithOptions("Canceled", canceled, .{ .task_failure_errors = &.{error.Canceled} });
    const StructListed = workflow.DefWithOptions("StructListed", Stateful, .{ .task_failure_errors = &.{error.ConfigMissing} });
    const StructPlain = workflow.Def("StructPlain", Stateful);
    const DynamicListed = workflow.DynamicDefWithOptions(dynamic, .{ .task_failure_errors = &.{error.ConfigMissing} });
    const DynamicPlain = workflow.DynamicDef(dynamic);
};

/// The workflow task failure of `c`, checking it has no commands.
fn taskFailure(c: done_pb.WorkflowActivationCompletion) !Failure {
    return switch (c.status.?) {
        .failed => |f| f.failure.?,
        .successful => error.TestUnexpectedResult,
    };
}

/// Asserts `c` fails the workflow task for error.ConfigMissing returned by `what`.
fn expectConfigMissingTask(c: done_pb.WorkflowActivationCompletion, what: []const u8) !void {
    const f = try taskFailure(c);
    try testing.expectEqualStrings("ConfigMissing", failure_mod.applicationType(f).?);
    try testing.expect(std.mem.indexOf(u8, f.message, "error.ConfigMissing") != null);
    try testing.expect(std.mem.indexOf(u8, f.message, what) != null);
}

fn expectWorkflowFailed(c: done_pb.WorkflowActivationCompletion, message: []const u8) !void {
    const cmds = commandsOf(c);
    try testing.expectEqual(@as(usize, 1), cmds.len);
    try testing.expectEqualStrings(message, cmds[0].variant.?.fail_workflow_execution.failure.?.message);
}

test "failure policy: error.OutOfMemory from workflow code fails the workflow task by default" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, Policy.OutOfMemoryWf.registered, .{});
    defer inst.destroy();
    const f = try taskFailure(try inst.activate(&try initActivation(a, "OutOfMemory", .{}), a));
    try testing.expectEqualStrings("OutOfMemory", failure_mod.applicationType(f).?);
    try testing.expectEqualStrings("out of memory running the workflow", f.message);
}

test "failure policy: with no lists, an error fails the workflow" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    inline for (.{ Policy.Missing, Policy.StructPlain, Policy.DynamicPlain }) |D| {
        const inst = try create(testing.allocator, D.registered, .{});
        defer inst.destroy();
        try expectWorkflowFailed(try inst.activate(&try initActivation(a, "Missing", .{}), a), "ConfigMissing");
    }
}

test "failure policy: the worker's list fails the task for function, struct and dynamic workflows" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const listed: []const anyerror = &.{ error.Unrelated, error.ConfigMissing };
    inline for (.{ Policy.Missing, Policy.StructPlain, Policy.DynamicPlain }) |D| {
        const inst = try create(testing.allocator, D.registered, .{ .task_failure_errors = listed });
        defer inst.destroy();
        try expectConfigMissingTask(try inst.activate(&try initActivation(a, "Missing", .{}), a), "the workflow function of workflow \"Missing\"");
    }
    // A list without the error leaves it failing the workflow.
    const inst = try create(testing.allocator, Policy.Missing.registered, .{ .task_failure_errors = &.{error.Unrelated} });
    defer inst.destroy();
    try expectWorkflowFailed(try inst.activate(&try initActivation(a, "Missing", .{}), a), "ConfigMissing");
}

test "failure policy: the definition's list fails the task for function, struct and dynamic workflows" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    inline for (.{ Policy.MissingListed, Policy.StructListed, Policy.DynamicListed }) |D| {
        // Either list is enough: the worker's lists something else.
        const inst = try create(testing.allocator, D.registered, .{ .task_failure_errors = &.{error.Unrelated} });
        defer inst.destroy();
        try expectConfigMissingTask(try inst.activate(&try initActivation(a, "Listed", .{}), a), "returned error.ConfigMissing");
    }
}

test "failure policy: a listed error from a signal handler fails the task, not the workflow" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, RejectableWf.registered, .{ .task_failure_errors = &.{error.Rejected} });
    defer inst.destroy();

    _ = try inst.activate(&try initActivation(a, "Rejectable", .{}), a);
    const f = try taskFailure(try inst.activate(&try oneJob(a, try signalJob(a, "reject", .{})), a));
    try testing.expectEqualStrings("Rejected", failure_mod.applicationType(f).?);
    try testing.expect(std.mem.indexOf(u8, f.message, "signal handler \"reject\" of workflow \"Rejectable\" returned error.Rejected") != null);
}

test "failure policy: a listed error from an update handler fails the task without rejecting the update" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, UpdatesWf.registered, .{ .task_failure_errors = &.{error.Broken} });
    defer inst.destroy();

    _ = try inst.activate(&try initActivation(a, "Updates", .{}), a);
    const f = try taskFailure(try inst.activate(&try oneJob(a, try updateJob(a, "u1", "break", .{}, true)), a));
    try testing.expectEqualStrings("Broken", failure_mod.applicationType(f).?);
    try testing.expect(std.mem.indexOf(u8, f.message, "update handler \"break\" (update ID u1) of workflow \"Updates\" returned error.Broken") != null);
}

test "failure policy: listing error.Canceled doesn't stop an accepted cancellation" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, Policy.Canceled.registered, .{ .task_failure_errors = &.{error.Canceled} });
    defer inst.destroy();
    _ = try inst.activate(&try initActivation(a, "Canceled", .{}), a);
    const cmds = commandsOf(try inst.activate(&try oneJob(a, .{ .cancel_workflow = .{} }), a));
    try testing.expect(cmds[cmds.len - 1].variant.? == .cancel_workflow_execution);
}

test "failure policy: listing error.ContinueAsNew keeps a requested continue-as-new and fails the task on its misuse" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const listed: []const anyerror = &.{error.ContinueAsNew};
    {
        const inst = try create(testing.allocator, Continue.Defaults.registered, .{ .task_failure_errors = listed });
        defer inst.destroy();
        const cmds = commandsOf(try inst.activate(&try initActivation(a, "Defaults", .{@as(u32, 1)}), a));
        try testing.expect(cmds[cmds.len - 1].variant.? == .continue_as_new_workflow_execution);
    }
    {
        const inst = try create(testing.allocator, Continue.Bare.registered, .{ .task_failure_errors = listed });
        defer inst.destroy();
        const f = try taskFailure(try inst.activate(&try initActivation(a, "Bare", .{}), a));
        try testing.expectEqualStrings("ContinueAsNew", failure_mod.applicationType(f).?);
    }
}

test "failure policy: by default an update handler's error.OutOfMemory rejects the update" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, PolicyUpdatesWf.registered, .{});
    defer inst.destroy();
    _ = try inst.activate(&try initActivation(a, "PolicyUpdates", .{}), a);
    const cmds = commandsOf(try inst.activate(&try oneJob(a, try updateJob(a, "u1", "exhaust", .{}, true)), a));
    try testing.expectEqualStrings("OutOfMemory", (try updateResponse(cmds[1], "u1")).rejected.message);
}

const PolicyUpdates = struct {
    const Exhaust = workflow.UpdateDef("exhaust", fn () void);
    fn exhaust(_: *workflow.Context) error{OutOfMemory}!void {
        return error.OutOfMemory;
    }
    fn run(ctx: *workflow.Context) ![]const u8 {
        try ctx.setUpdateHandler(Exhaust, ctx, exhaust);
        try ctx.io().sleep(.fromSeconds(3600), .awake);
        return "slept";
    }
};
const PolicyUpdatesWf = workflow.Def("PolicyUpdates", PolicyUpdates.run);

/// A failure converter that prefixes each failure's message with "to <error> <context>: "
/// or "from <context>: ", where the context is "<workflow ID>/<workflow type>/<activity
/// type>/<activity ID>", and notes whether `Scheduler.sdk_work` was set at any call.
const StampingFailures = struct {
    sdk_work: ?*const std.atomic.Value(bool) = null,
    calls: usize = 0,
    saw_sdk_work: bool = false,

    fn failureConverter(s: *StampingFailures) converter.FailureConverter {
        return .{ .ptr = s, .vtable = &.{ .toFailure = toFailure, .fromFailure = fromFailure } };
    }

    fn note(ptr: *anyopaque) void {
        const s: *StampingFailures = @ptrCast(@alignCast(ptr));
        s.calls += 1;
        if (s.sdk_work) |w| if (w.load(.acquire)) {
            s.saw_sdk_work = true;
        };
    }

    fn toFailure(ptr: *anyopaque, a: Allocator, c: *const converter.Context, err: ?anyerror, f: Failure) error{OutOfMemory}!Failure {
        note(ptr);
        var out = f;
        out.message = try std.fmt.allocPrint(a, "to {s} {s}/{s}/{s}/{s}: {s}", .{ if (err) |e| @errorName(e) else "null", c.workflow_id, c.workflow_type, c.activity_type, c.activity_id, f.message });
        return out;
    }

    fn fromFailure(ptr: *anyopaque, a: Allocator, c: *const converter.Context, f: Failure) error{OutOfMemory}!Failure {
        note(ptr);
        var out = f;
        out.message = try std.fmt.allocPrint(a, "from {s}/{s}/{s}/{s}: {s}", .{ c.workflow_id, c.workflow_type, c.activity_type, c.activity_id, f.message });
        return out;
    }
};

test "failure converter: an activity's failure is read through it, and the workflow's failure that passes it on is sent through it, outside sdk_work" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    var stamping: StampingFailures = .{};
    const inst = try create(testing.allocator, Hello.registered, .{ .failure_converter = stamping.failureConverter() });
    defer inst.destroy();
    stamping.sdk_work = &inst.sched.sdk_work;
    _ = try inst.activate(&try initActivation(a, "Workflow", .{@as([]const u8, "Temporal")}), a);

    const f = try failure_mod.application(a, .{ .message = "boom", .type = "Boom" });
    const second = try inst.activate(&try oneJob(a, .{ .resolve_activity = .{ .seq = 1, .result = .{ .status = .{ .failed = .{ .failure = f } } } } }), a);
    const fail = commandsOf(second)[0].variant.?.fail_workflow_execution.failure.?;
    try testing.expectEqualStrings("to ActivityFailed wf-1/Workflow//: from wf-1/Workflow/Activity/1: boom", fail.message);
    try testing.expectEqualStrings("Boom", failure_mod.applicationType(fail).?);
    try testing.expectEqual(@as(usize, 2), stamping.calls);
    try testing.expect(!stamping.saw_sdk_work);
}

test "failure converter: the previous run's failure is read through it, on replay too" {
    const want = "\"attempts=3,initial=1,max=null,types=Fatal|2/tenant-a/1.5|run-0|from wf-1/StartInfo//: first attempt <- root cause\"";
    for ([_]bool{ false, true }) |replaying| {
        var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
        defer scratch.deinit();
        const a = scratch.allocator();
        var stamping: StampingFailures = .{};
        const inst = try create(testing.allocator, StartInfoWfDef.registered, .{ .failure_converter = stamping.failureConverter() });
        defer inst.destroy();
        stamping.sdk_work = &inst.sched.sdk_work;
        try testing.expectEqualStrings(want, completedWith(try inst.activate(&try startWithEverything(a, replaying), a)));
        try testing.expectEqual(@as(usize, 1), stamping.calls);
        try testing.expect(!stamping.saw_sdk_work);
    }
}

test "failure converter: update rejections from the validator and the handler are sent through it, with stack traces off" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    var stamping: StampingFailures = .{};
    const inst = try create(testing.allocator, UpdatesWf.registered, .{ .failure_converter = stamping.failureConverter(), .failure_stack_traces = false });
    defer inst.destroy();
    stamping.sdk_work = &inst.sched.sdk_work;

    _ = try inst.activate(&try initActivation(a, "Updates", .{}), a);
    var act = try oneJob(a, try updateJob(a, "u1", "set", .{@as(i64, -1)}, true));
    try act.jobs.append(a, .{ .variant = try updateJob(a, "u2", "break", .{}, true) });
    const cmds = commandsOf(try inst.activate(&act, a));
    try testing.expectEqual(@as(usize, 3), cmds.len);
    try testing.expectEqualStrings("to NotPositive wf-1/Updates//: NotPositive", (try updateResponse(cmds[0], "u1")).rejected.message);
    try testing.expect(try updateResponse(cmds[1], "u2") == .accepted);
    const rejected = (try updateResponse(cmds[2], "u2")).rejected;
    try testing.expectEqualStrings("to Broken wf-1/Updates//: Broken", rejected.message);
    try testing.expectEqualStrings("", rejected.stack_trace);
    try testing.expectEqual(@as(usize, 2), stamping.calls);
    try testing.expect(!stamping.saw_sdk_work);
}

test "failure converter: a query handler's failure is sent through it, and the SDK's own reports are not" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    var stamping: StampingFailures = .{};
    const queries = try create(testing.allocator, QueryWfDef.registered, .{ .failure_converter = stamping.failureConverter() });
    defer queries.destroy();
    stamping.sdk_work = &queries.sched.sdk_work;
    _ = try queries.activate(&try initActivation(a, "Queries", .{}), a);
    const c = try queries.activate(&try queryActivation(a, &.{
        .{ "q1", "nope", &.{} },
        .{ "q2", "fails", &.{} },
    }), a);
    try testing.expectStringStartsWith(answer(c, 0).variant.?.failed.message, "query type \"nope\" is not registered");
    try testing.expectEqualStrings("to NotReady wf-1/Queries//: NotReady", answer(c, 1).variant.?.failed.message);
    try testing.expectEqual(@as(usize, 1), stamping.calls);
    try testing.expect(!stamping.saw_sdk_work);

    // Returning error.ContinueAsNew without calling ctx.continueAsNew is the SDK's report.
    stamping = .{};
    const bare = try create(testing.allocator, Continue.Bare.registered, .{ .failure_converter = stamping.failureConverter() });
    defer bare.destroy();
    const done = commandsOf(try bare.activate(&try initActivation(a, "Bare", .{}), a));
    try testing.expectStringStartsWith(done[0].variant.?.fail_workflow_execution.failure.?.message, "workflow \"Bare\" returned error.ContinueAsNew");
    try testing.expectEqual(@as(usize, 0), stamping.calls);
}

const SignalWithStartWf = struct {
    const Add = workflow.SignalDef("add", fn (value: []const u8) void);
    fn target(_: *workflow.Context, start: []const u8) ![]const u8 {
        return start;
    }
    const Target = workflow.Def("Target", target);
    const Tier = sa.key(.keyword, "Tier");

    /// Signals-with-start `Target` with every option set; returns what `get` returned, as
    /// "<id> <run> <started>" or the error's name.
    fn full(ctx: *workflow.Context) ![]const u8 {
        const memo = try converter.toFields(ctx.allocator(), .{ .note = @as([]const u8, "hello") });
        const f = ctx.signalWithStartWorkflow(Target, .{
            .id = "target-1",
            .execution_timeout = .fromSeconds(60),
            .run_timeout = .fromSeconds(30),
            .task_timeout = .fromSeconds(5),
            .start_delay = .fromMilliseconds(1500),
            .id_reuse_policy = .allow_duplicate_failed_only,
            .id_conflict_policy = .use_existing,
            .retry_policy = .{ .maximum_attempts = 3 },
            .memo = memo,
            .search_attributes = &.{Tier.set("gold")},
        }, .{"go"}, Add, .{"one"});
        const r = f.get() catch |err| return @errorName(err);
        return std.fmt.allocPrint(ctx.allocator(), "{s} {s} {}", .{ r.workflow_id, r.run_id, r.started });
    }
    const Full = workflow.Def("FullSignalWithStart", full);

    /// The by-name form with the default options and a task queue.
    fn byName(ctx: *workflow.Context) ![]const u8 {
        const f = ctx.signalWithStartWorkflowByName("Target", .{ .id = "target-2", .task_queue = "other-tq" }, .{"go"}, "add", .{});
        const r = f.get() catch |err| return @errorName(err);
        return std.fmt.allocPrint(ctx.allocator(), "{s} {s} {}", .{ r.workflow_id, r.run_id, r.started });
    }
    const ByName = workflow.Def("ByNameSignalWithStart", byName);
};

/// The first activation's only command, which must be a ScheduleNexusOperation.
fn signalWithStartCommand(inst: *Instance, a: Allocator, workflow_type: []const u8) !cmd_pb.ScheduleNexusOperation {
    const cmds = commandsOf(try inst.activate(&try initActivation(a, workflow_type, .{}), a));
    try testing.expectEqual(@as(usize, 1), cmds.len);
    try testing.expect(cmds[0].user_metadata == null);
    return cmds[0].variant.?.schedule_nexus_operation;
}

test "signal-with-start: the command schedules the system operation with the request's envelope, the same bytes every run" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, SignalWithStartWf.Full.registered, .{ .task_queue = "tq-1", .namespace = "ns-1" });
    defer inst.destroy();
    const cmd = try signalWithStartCommand(inst, a, "FullSignalWithStart");
    try testing.expectEqualStrings("__temporal_system", cmd.endpoint);
    try testing.expectEqualStrings("temporal.api.workflowservice.v1.WorkflowService", cmd.service);
    try testing.expectEqualStrings("SignalWithStartWorkflowExecution", cmd.operation);
    try testing.expect(cmd.cancellation_type == .WAIT_CANCELLATION_COMPLETED);
    try testing.expect(cmd.schedule_to_close_timeout == null and cmd.schedule_to_start_timeout == null and cmd.start_to_close_timeout == null);
    try testing.expectEqual(@as(usize, 0), cmd.nexus_header.items.len);

    const input = cmd.input.?;
    try testing.expect(system.isMarked(input));
    const req = (try system.decode(a, input)).signal_with_start_request;
    try testing.expectEqualStrings("ns-1", req.namespace);
    try testing.expectEqualStrings("target-1", req.workflow_id);
    try testing.expectEqualStrings("Target", req.workflow_type.?.name);
    try testing.expectEqualStrings("tq-1", req.task_queue.?.name);
    try testing.expect(req.task_queue.?.kind == .TASK_QUEUE_KIND_NORMAL);
    try testing.expectEqualStrings("\"go\"", req.input.?.payloads.items[0].data);
    try testing.expectEqualStrings("add", req.signal_name);
    try testing.expectEqualStrings("\"one\"", req.signal_input.?.payloads.items[0].data);
    try testing.expectEqual(@as(i64, 60), req.workflow_execution_timeout.?.seconds);
    try testing.expectEqual(@as(i64, 30), req.workflow_run_timeout.?.seconds);
    try testing.expectEqual(@as(i64, 5), req.workflow_task_timeout.?.seconds);
    try testing.expectEqual(@as(i64, 1), req.workflow_start_delay.?.seconds);
    try testing.expectEqual(@as(i32, 500_000_000), req.workflow_start_delay.?.nanos);
    try testing.expect(req.workflow_id_reuse_policy == .WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY);
    try testing.expect(req.workflow_id_conflict_policy == .WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING);
    try testing.expectEqual(@as(i32, 3), req.retry_policy.?.maximum_attempts);
    try testing.expectEqualStrings("note", req.memo.?.fields.items[0].key);
    try testing.expectEqualStrings("Tier", req.search_attributes.?.indexed_fields.items[0].key);
    // Nothing the server refuses or that could differ on replay.
    try testing.expectEqualStrings("", req.request_id);
    try testing.expectEqualStrings("", req.identity);
    try testing.expectEqualStrings("", req.control);
    try testing.expectEqual(@as(usize, 0), req.links.items.len);
    try testing.expect(req.header == null and req.user_metadata == null and req.versioning_override == null and req.priority == null and req.time_skipping_config == null);

    // A second run of the same workflow (as on replay) sends the same bytes.
    const again = try create(testing.allocator, SignalWithStartWf.Full.registered, .{ .task_queue = "tq-1", .namespace = "ns-1" });
    defer again.destroy();
    try testing.expectEqualSlices(u8, input.data, (try signalWithStartCommand(again, a, "FullSignalWithStart")).input.?.data);

    // The response completes `get`.
    const response = try system.envelope(a, system.SignalWithStartResponse{ .run_id = "run-9", .started = true });
    try testing.expectEqualStrings("\"target-1 run-9 true\"", nexusResultOf(try inst.activate(&try nexusResolved(a, 1, .{ .completed = response }), a)));
}

test "signal-with-start: the by-name form takes the task queue and a response without the mark" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, SignalWithStartWf.ByName.registered, .{ .task_queue = "tq-1" });
    defer inst.destroy();
    const cmd = try signalWithStartCommand(inst, a, "ByNameSignalWithStart");
    const req = (try system.decode(a, cmd.input.?)).signal_with_start_request;
    try testing.expectEqualStrings("other-tq", req.task_queue.?.name);
    try testing.expectEqualStrings("Target", req.workflow_type.?.name);
    try testing.expectEqualStrings("\"go\"", req.input.?.payloads.items[0].data);
    try testing.expectEqual(@as(usize, 0), req.signal_input.?.payloads.items.len);
    try testing.expect(req.workflow_id_conflict_policy == .WORKFLOW_ID_CONFLICT_POLICY_UNSPECIFIED);
    try testing.expect(req.memo == null and req.search_attributes == null and req.retry_policy == null);

    const plain = try (converter.ProtoBinary(system.SignalWithStartResponse){ .message = .{ .run_id = "run-3" } }).toPayload(a);
    try testing.expectEqualStrings("\"target-2 run-3 false\"", nexusResultOf(try inst.activate(&try nexusResolved(a, 1, .{ .completed = plain }), a)));
}

test "signal-with-start: get maps a taken ID, other failures, cancellation and a bad response to their errors" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const taken: Failure = .{
        .message = "WorkflowExecutionAlreadyStarted: Workflow execution already finished. WorkflowId: target-1, RunId: r. Workflow Id reuse policy: reject duplicate workflow Id.",
        .failure_info = .{ .server_failure_info = .{} },
    };
    const other: Failure = .{ .message = "InvalidArgument: bad cron", .failure_info = .{ .server_failure_info = .{} } };
    const handler_taken: Failure = .{
        .message = "Workflow execution already finished successfully. WorkflowId: target-1, RunId: r.",
        .failure_info = .{ .nexus_handler_failure_info = .{ .type = "BAD_REQUEST" } },
    };
    const handler_other: Failure = .{ .message = handler_taken.message, .failure_info = .{ .nexus_handler_failure_info = .{ .type = "INTERNAL" } } };
    // The prefix only counts on a server failure.
    const app: Failure = .{ .message = taken.message, .failure_info = .{ .application_failure_info = .{} } };
    const cases = [_]struct { status: NexusResult.status_union, want: []const u8 }{
        .{ .status = .{ .failed = try nexusFailure(a, "nexus operation completed unsuccessfully", taken) }, .want = "WorkflowExecutionAlreadyStarted" },
        .{ .status = .{ .failed = try nexusFailure(a, "nexus operation completed unsuccessfully", other) }, .want = "SignalWithStartFailed" },
        .{ .status = .{ .failed = try nexusFailure(a, "nexus operation completed unsuccessfully", handler_taken) }, .want = "WorkflowExecutionAlreadyStarted" },
        .{ .status = .{ .failed = try nexusFailure(a, "nexus operation completed unsuccessfully", handler_other) }, .want = "SignalWithStartFailed" },
        .{ .status = .{ .failed = try nexusFailure(a, "nexus operation completed unsuccessfully", app) }, .want = "SignalWithStartFailed" },
        .{ .status = .{ .timed_out = try nexusFailure(a, "timed out", .{ .message = "", .failure_info = .{ .timeout_failure_info = .{} } }) }, .want = "SignalWithStartFailed" },
        .{ .status = .{ .cancelled = try nexusCanceledFailure(a, "canceled") }, .want = "Canceled" },
        .{ .status = .{ .completed = try converter.toPayload(a, @as([]const u8, "not a response")) }, .want = "InvalidPayload" },
    };
    for (cases) |c| {
        const inst = try create(testing.allocator, SignalWithStartWf.Full.registered, .{});
        defer inst.destroy();
        _ = try signalWithStartCommand(inst, a, "FullSignalWithStart");
        const want = try std.fmt.allocPrint(a, "\"{s}\"", .{c.want});
        try testing.expectEqualStrings(want, nexusResultOf(try inst.activate(&try nexusResolved(a, 1, c.status), a)));
    }
}

test "signal-with-start: refused in a query handler like the other commands" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const W = struct {
        fn query(ctx: *workflow.Context) []const u8 {
            const f = ctx.signalWithStartWorkflow(SignalWithStartWf.Target, .{ .id = "t" }, .{"go"}, SignalWithStartWf.Add, .{"one"});
            _ = f.get() catch |err| return @errorName(err);
            return "sent";
        }
        fn run(ctx: *workflow.Context) ![]const u8 {
            try ctx.setQueryHandler(workflow.QueryDef("try", fn () []const u8), ctx, query);
            try ctx.sleep(.fromSeconds(60), .{});
            return "";
        }
        const Def = workflow.Def("QuerySignalWithStart", run);
    };
    const inst = try create(testing.allocator, W.Def.registered, .{});
    defer inst.destroy();
    _ = try inst.activate(&try initActivation(a, "QuerySignalWithStart", .{}), a);
    const c = try inst.activate(&try queryActivation(a, &.{.{ "q1", "try", &.{} }}), a);
    try testing.expectEqual(@as(usize, 1), commandsOf(c).len);
    const failed = answer(c, 0).variant.?.failed;
    try testing.expect(std.mem.find(u8, failed.message, "signalWithStartWorkflow") != null);
}

test "signal-with-start: start_delay with cron_schedule is InvalidOptions and sends nothing" {
    // The server refuses this pair while validating the ScheduleNexusOperation command
    // ("BadScheduleNexusOperationAttributes: CronSchedule and WorkflowStartDelay may not be
    // used together."), which fails the caller's workflow task on every attempt: `get` would
    // never return. The SDK knows the pair is invalid, so it must refuse it before sending.
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const W = struct {
        fn run(ctx: *workflow.Context) ![]const u8 {
            const f = ctx.signalWithStartWorkflow(SignalWithStartWf.Target, .{
                .id = "target-cron",
                .cron_schedule = "* * * * *",
                .start_delay = .fromSeconds(5),
            }, .{"go"}, SignalWithStartWf.Add, .{"one"});
            _ = f.get() catch |err| return @errorName(err);
            return "sent";
        }
        const Def = workflow.Def("CronDelaySignalWithStart", run);
    };
    const inst = try create(testing.allocator, W.Def.registered, .{});
    defer inst.destroy();
    const cmds = commandsOf(try inst.activate(&try initActivation(a, "CronDelaySignalWithStart", .{}), a));
    for (cmds) |c| try testing.expect(c.variant.? != .schedule_nexus_operation);
    try testing.expectEqual(@as(usize, 1), cmds.len);
    try testing.expectEqualStrings("\"InvalidOptions\"", cmds[0].variant.?.complete_workflow_execution.result.?.data);
}

test "signal-with-start: invalid UTF-8 and retry policies the server refuses are InvalidOptions and send nothing" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const W = struct {
        fn run(ctx: *workflow.Context) ![]const u8 {
            const T = SignalWithStartWf.Target;
            const S = SignalWithStartWf.Add;
            const calls = [_]workflow.SignalWithStartFuture{
                ctx.signalWithStartWorkflow(T, .{ .id = "bad\xff" }, .{"go"}, S, .{"one"}),
                ctx.signalWithStartWorkflow(T, .{ .id = "t", .task_queue = "\xc3" }, .{"go"}, S, .{"one"}),
                ctx.signalWithStartWorkflowByName("T\xff", .{ .id = "t" }, .{"go"}, "add", .{"one"}),
                ctx.signalWithStartWorkflowByName("Target", .{ .id = "t" }, .{"go"}, "a\xff", .{"one"}),
                ctx.signalWithStartWorkflow(T, .{ .id = "t", .retry_policy = .{ .maximum_attempts = -1 } }, .{"go"}, S, .{"one"}),
                ctx.signalWithStartWorkflow(T, .{ .id = "t", .retry_policy = .{ .backoff_coefficient = 0.5 } }, .{"go"}, S, .{"one"}),
                ctx.signalWithStartWorkflow(T, .{ .id = "t", .retry_policy = .{ .initial_interval = .fromSeconds(-1) } }, .{"go"}, S, .{"one"}),
                ctx.signalWithStartWorkflow(T, .{ .id = "t", .retry_policy = .{ .initial_interval = .fromSeconds(10), .maximum_interval = .fromSeconds(5) } }, .{"go"}, S, .{"one"}),
            };
            for (calls) |f| if (f.get()) |_| return "sent" else |err| if (err != error.InvalidOptions) return @errorName(err);
            return "all invalid";
        }
        const Def = workflow.Def("InvalidMoreSignalWithStart", run);
    };
    const inst = try create(testing.allocator, W.Def.registered, .{});
    defer inst.destroy();
    const cmds = commandsOf(try inst.activate(&try initActivation(a, "InvalidMoreSignalWithStart", .{}), a));
    try testing.expectEqual(@as(usize, 1), cmds.len);
    try testing.expectEqualStrings("\"all invalid\"", cmds[0].variant.?.complete_workflow_execution.result.?.data);
}

test "signal-with-start: a valid retry policy with defaults is sent" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const W = struct {
        fn run(ctx: *workflow.Context) ![]const u8 {
            _ = ctx.signalWithStartWorkflow(SignalWithStartWf.Target, .{ .id = "t", .retry_policy = .{ .backoff_coefficient = 2, .initial_interval = .fromSeconds(1), .maximum_interval = .fromSeconds(1) } }, .{"go"}, SignalWithStartWf.Add, .{"one"});
            return "";
        }
        const Def = workflow.Def("ValidRetrySignalWithStart", run);
    };
    const inst = try create(testing.allocator, W.Def.registered, .{});
    defer inst.destroy();
    const cmds = commandsOf(try inst.activate(&try initActivation(a, "ValidRetrySignalWithStart", .{}), a));
    try testing.expect(cmds[0].variant.? == .schedule_nexus_operation);
}
