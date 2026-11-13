//! One cached workflow run: turns sdk-core activations into commands.
//!
//! The worker feeds each `WorkflowActivation` for the run to `activate`. Jobs are applied in
//! the order the core sends them (it orders them for determinism), then every coroutine runs
//! until it blocks, and the commands they produced become the activation's completion.
//!
//! The run's memory (workflow state, decoded arguments, results kept by futures) lives in an
//! arena owned by the instance and freed by `deinit` when the core evicts the run.

const Instance = @This();

const std = @import("std");
const proto = @import("../proto.zig");
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
const Scheduler = @import("Scheduler.zig");
const WorkflowIo = @import("WorkflowIo.zig");
const Allocator = std.mem.Allocator;

const act_pb = proto.coresdk.workflow_activation;
const cmd_pb = proto.coresdk.workflow_commands;
const done_pb = proto.coresdk.workflow_completion;
const result_pb = proto.coresdk.activity_result;
const common = proto.temporal.api.common.v1;
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
replaying: bool = false,
/// Commands produced during the current activation.
commands: std.ArrayList(cmd_pb.WorkflowCommand) = .empty,
next_activity_seq: u32 = 1,
next_timer_seq: u32 = 1,
next_child_seq: u32 = 1,
/// Signals and cancellation requests to other workflows share one sequence space; the core
/// keys each kind separately, so their seqs never meet.
next_external_seq: u32 = 1,
activities: std.AutoHashMapUnmanaged(u32, *ActivityState) = .empty,
timers: std.AutoHashMapUnmanaged(u32, *WorkflowIo.Timer) = .empty,
/// Child workflows that have not closed, by seq.
children: std.AutoHashMapUnmanaged(u32, *ChildState) = .empty,
/// Signals and cancellation requests to other workflows that the core has not resolved, by seq.
externals: std.AutoHashMapUnmanaged(u32, *ExternalState) = .empty,
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
interceptors: []const interceptor.Worker = &.{},
/// The run's interceptors, from `interceptors` at initialization. Arena memory.
inbound: []const interceptor.WorkflowInbound = &.{},
outbound: []const interceptor.WorkflowOutbound = &.{},
/// The handler of the update being validated, for the validation chain's end.
validating: ?UpdateHandler = null,
last_completion_result: ?converter.Payload = null,
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
finished: bool = false,
/// Set by `destroy`: waits that ignore workflow cancellation (an activity's cancellation
/// completing) end with error.Canceled instead, so every coroutine can exit.
evicting: bool = false,
/// Set when an activation must fail the workflow task (it is retried) instead of producing
/// commands, for example on an unsupported job.
task_failure: ?[]const u8 = null,
/// Query handlers by query type, in registration order (the order an unknown query's failure
/// lists them in). Arena memory; they outlive the workflow function.
queries: std.array_hash_map.String(QueryHandler) = .empty,
/// Signal handlers by signal name, in registration order. Arena memory.
signal_handlers: std.array_hash_map.String(SignalHandler) = .empty,
/// Handles signals with no handler registered for their name, if set. Arena memory.
dynamic_signal_handler: ?SignalHandler = null,
/// Signals that arrived before a handler for their name (or a dynamic handler), in arrival
/// order. Arena memory.
buffered_signals: std.ArrayList(*const Signal) = .empty,
/// Update handlers by update name, in registration order (the order an unregistered update's
/// rejection lists them in). Arena memory.
update_handlers: std.array_hash_map.String(UpdateHandler) = .empty,
/// Updates delivered before a handler for their name, in arrival order. Whatever is still here
/// once the activation's coroutines have all blocked is rejected. Arena memory.
buffered_updates: std.ArrayList(*const Update) = .empty,
/// Set when the current activation accepted an update.
accepted_update: bool = false,
/// Signal and update handler coroutines that have not returned, in start order. Each removes
/// itself when its handler returns; `destroy` cancels the rest.
handlers: std.ArrayList(*HandlerRun) = .empty,
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
/// and encodes its result into `out`. Runs on the worker thread, outside any coroutine.
pub const QueryHandler = struct {
    context: *const anyopaque,
    call: *const fn (context: *const anyopaque, out: Allocator, args: []const converter.Payload) anyerror!converter.Payload,
};

/// A type-erased signal handler. `decode` turns a signal's name and payloads (both arena
/// memory of the run) into the handler's arguments, allocated in `out`; `call` runs the
/// handler with `context` and those arguments, inside the signal's coroutine.
pub const SignalHandler = struct {
    context: *const anyopaque,
    decode: *const fn (out: Allocator, name: []const u8, args: []const converter.Payload) anyerror!*const anyopaque,
    call: *const fn (context: *const anyopaque, args: *const anyopaque) anyerror!void,
};

/// A type-erased update handler. `decode` turns an update's payloads into the handler's
/// argument tuple, allocated in `out`; `validate` (if set) runs the validator with `context`
/// and those arguments, outside any coroutine; `call` runs the handler inside the update's
/// coroutine and encodes its result into `out`.
pub const UpdateHandler = struct {
    context: *const anyopaque,
    decode: *const fn (out: Allocator, args: []const converter.Payload) anyerror!*const anyopaque,
    validate: ?*const fn (context: *const anyopaque, args: *const anyopaque) anyerror!void,
    call: *const fn (context: *const anyopaque, out: Allocator, args: *const anyopaque) anyerror!converter.Payload,
};

/// A delivered signal, copied into the arena so it outlives its activation.
const Signal = struct { name: []const u8, args: []const converter.Payload, header: converter.Header };

/// A delivered update, copied into the arena so it outlives its activation.
const Update = struct {
    id: []const u8,
    protocol_instance_id: []const u8,
    name: []const u8,
    args: []const converter.Payload,
    header: converter.Header,
    run_validator: bool,

    fn input(u: *const Update) interceptor.UpdateInput {
        return .{ .update_name = u.name, .update_id = u.id, .args = .{ .payloads = u.args }, .header = u.header };
    }
};

/// A running signal or update handler coroutine. Arena memory.
const HandlerRun = struct {
    name: []const u8,
    kind: union(enum) {
        signal: struct { handler: SignalHandler, signal: *const Signal },
        update: struct { handler: UpdateHandler, update: *const Update },
    },
    /// Set after spawning, before the coroutine first runs; freed when it returns.
    task: *WorkflowIo.Task = undefined,
    /// This handler's `Context.fail` and the failure of the last activity or child future it
    /// awaited; the workflow function's are in `Context`.
    pending_failure: ?failure_mod.Application = null,
    last_failure: ?*const Failure = null,
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
    /// Set for a local activity; null for a remote one.
    local: ?*Local = null,

    pub const CancellationType = workflow.ActivityCancellationType;

    /// A local activity's last ScheduleLocalActivity command (arena memory), sent again with
    /// a new seq for each attempt that follows a backoff timer.
    pub const Local = struct {
        cmd: cmd_pb.ScheduleLocalActivity,
        /// The activity ID is the attempt's seq, not one the workflow chose.
        id_from_seq: bool,
        /// The backoff timer's seq while the activity waits for its next attempt.
        backoff_timer: ?u32 = null,
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
    inst.* = .{
        .gpa = gpa,
        .arena_state = .init(gpa),
        .sched = .init(gpa, .{ .stack_size = options.stack_size }),
        .wio = undefined,
        .ctx = undefined,
        .entry = entry,
        .versioning_behavior = entry.versioning_behavior,
        .send_versioning_behavior = options.send_versioning_behavior,
    };
    inst.wio = .init(gpa, &inst.sched, .{ .ctx = inst, .vtable = &host_vtable }, 0);
    inst.ctx = .{ .instance = inst };
    inst.task_queue = try inst.arena().dupe(u8, options.task_queue);
    inst.namespace = try inst.arena().dupe(u8, options.namespace);
    inst.logger = .{ .sink = options.logger, .replaying = if (options.log_during_replay) null else &inst.replaying };
    inst.propagators = options.propagators;
    inst.interceptors = options.interceptors;
    inst.metrics = options.metrics;
    inst.propagated = .init(inst.arena());
    return inst;
}

/// Tears the run down: cancels its coroutines, lets them exit, and frees everything.
pub fn destroy(inst: *Instance) void {
    inst.evicting = true;
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
    inst.wio.run();
    const gpa = inst.gpa;
    if (inst.sched.live == 0) {
        inst.sched.deinit();
    } else {
        // A coroutine ignored cancellation (for example it blocked with cancel protection on).
        // Its stack can't be reclaimed safely, so it is leaked rather than freed under it.
        inst.logger.warn("workflow coroutines did not exit on eviction; leaking their stacks", .{ .count = inst.sched.live });
    }
    inst.metrics.deinit();
    inst.arena_state.deinit();
    gpa.destroy(inst);
}

pub fn arena(inst: *Instance) Allocator {
    return inst.arena_state.allocator();
}

/// Applies an activation and returns its completion. `out` holds the completion's memory
/// until the caller has sent it. RemoveFromCache jobs are the caller's to handle (complete
/// the activation, then `destroy`); they are ignored here.
///
/// The core sends queries in an activation of their own, after any jobs that change state. A
/// query-only activation just answers: coroutines don't run, so it emits no other command and
/// leaves workflow time and state as they were.
pub fn activate(inst: *Instance, act: *const act_pb.WorkflowActivation, out: Allocator) !done_pb.WorkflowActivationCompletion {
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
    inst.commands = .empty;
    inst.task_failure = null;
    inst.accepted_update = false;
    // Query answers go to `out`, not the run's arena, so answering doesn't grow the run.
    var answers: std.ArrayList(cmd_pb.WorkflowCommand) = .empty;

    for (act.jobs.items) |job| {
        const variant = job.variant orelse continue;
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
            else => |other| {
                inst.task_failure = try std.fmt.allocPrint(out, "workflow activation job '{t}' is not supported by this SDK version yet", .{other});
            },
        }
    }

    // Once the workflow has finished, coroutines that are still blocked (handlers, or the root
    // after a handler failed it) never run again: their commands would follow the completion.
    // An update-only activation whose updates were all rejected is not in history, so on
    // replay its coroutines could not run: they run only once an update is accepted (and core
    // rejects anything but rejections from it).
    if (inst.task_failure == null and !query_only and !inst.finished) {
        if (!update_only or inst.accepted_update) inst.runUntilQuiet();
        try inst.rejectUnhandledUpdates();
        try inst.finishIfDone();
    }

    var completion: done_pb.WorkflowActivationCompletion = .{ .run_id = act.run_id };
    if (inst.task_failure) |message| {
        completion.status = .{ .failed = .{ .failure = try failure_mod.application(out, .{ .message = message, .type = "UnsupportedJob" }) } };
    } else {
        var success: done_pb.Success = .{};
        if (inst.send_versioning_behavior) success.versioning_behavior = @fromBackingInt(@backingInt(inst.versioning_behavior));
        try success.commands.appendSlice(out, inst.commands.items);
        try success.commands.appendSlice(out, answers.items);
        completion.status = .{ .successful = success };
    }
    return completion;
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
    if (context_addr) |addr| if (inst.sched.onCurrentStack(addr)) {
        inst.logger.warn("setQueryHandler: the handler context points into the workflow coroutine's stack, which is gone once the coroutine returns; allocate it with ctx.allocator()", .{ .QueryType = name });
        return error.QueryContextOnStack;
    };
    try inst.queries.put(inst.arena(), name, handler);
}

/// The answer to one query: the handler's encoded result, or a failure for an unknown query
/// type or a handler error. Memory comes from `out`.
fn answerQuery(inst: *Instance, q: *const act_pb.QueryWorkflow, out: Allocator) !cmd_pb.QueryResult {
    const response = inst.inboundNext().handleQuery(.{
        .query_type = q.query_type,
        .args = .{ .payloads = q.arguments.items },
        .header = try .fromEntries(out, q.headers.items),
        .arena = out,
    }) catch |err| {
        if (err == error.OutOfMemory) return error.OutOfMemory;
        if (err != error.QueryNotRegistered) return .{ .query_id = q.query_id, .variant = .{ .failed = try failure_mod.fromError(out, err) } };
        const names = try std.mem.join(out, ", ", inst.queries.keys());
        return .{ .query_id = q.query_id, .variant = .{ .failed = try failure_mod.application(out, .{
            .message = try std.fmt.allocPrint(out, "query type \"{s}\" is not registered on workflow \"{s}\"; registered query types: [{s}]", .{ q.query_type, inst.info.workflow_type, names }),
            .type = "QueryNotRegistered",
        }) } };
    };
    return .{ .query_id = q.query_id, .variant = .{ .succeeded = .{ .response = response } } };
}

// ---------------------------------------------------------------- signals

/// Registers `handler` for signals called `name`, replacing any handler registered under it,
/// then starts a handler coroutine for each buffered signal of that name, in arrival order.
/// `name` and the handler's context must outlive the run. `context_addr` is checked as in
/// `setQueryHandler`: a handler coroutine can run after the registering coroutine returned.
pub fn setSignalHandler(inst: *Instance, name: []const u8, handler: SignalHandler, context_addr: ?usize) error{ OutOfMemory, SignalContextOnStack }!void {
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

/// Copies a SignalWorkflow job into the arena and starts its handler, or buffers it until a
/// handler for its name is registered.
fn deliverSignal(inst: *Instance, job: *const act_pb.SignalWorkflow) !void {
    if (inst.finished) return;
    if (std.mem.eql(u8, job.signal_name, session_opened_signal)) return inst.openSession(job.input.items);
    const a = inst.arena();
    const args = try a.alloc(converter.Payload, job.input.items.len);
    for (job.input.items, args) |p, *d| d.* = try p.dupe(a);
    const signal = try a.create(Signal);
    signal.* = .{ .name = try a.dupe(u8, job.signal_name), .args = args, .header = try .fromEntries(a, job.headers.items) };
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
    try inst.buffered_updates.append(a, update);
}

/// Validates the update through the inbound interceptors, which end in decoding its
/// arguments and running its validator: an error rejects it. Skipped on replay, where every
/// update in history was accepted. Otherwise accepts it and spawns its handler coroutine.
fn startUpdate(inst: *Instance, handler: UpdateHandler, update: *const Update) error{OutOfMemory}!void {
    const a = inst.arena();
    if (update.run_validator) {
        inst.validating = handler;
        defer inst.validating = null;
        inst.inboundNext().validateUpdate(update.input()) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            else => return inst.respondToUpdate(update.protocol_instance_id, .{ .rejected = try failure_mod.fromError(a, err) }),
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
    try inst.commands.append(inst.arena(), .{ .variant = .{ .update_response = .{
        .protocol_instance_id = protocol_instance_id,
        .response = response,
    } } });
}

/// Spawns a handler coroutine for `run` and tracks it in `handlers`.
fn startHandler(inst: *Instance, run: *HandlerRun) error{OutOfMemory}!void {
    try inst.handlers.ensureUnusedCapacity(inst.arena(), 1);
    run.task = inst.wio.spawn(@tagName(run.kind), handlerMain, .{ inst, run }) catch |err| {
        inst.logger.warn("starting a handler coroutine failed", .{ .HandlerName = run.name, .error_name = err });
        return error.OutOfMemory;
    };
    inst.handlers.appendAssumeCapacity(run);
}

fn handlerMain(inst: *Instance, run: *HandlerRun) void {
    switch (run.kind) {
        .signal => |s| {
            const result = inst.inboundNext().handleSignal(.{ .signal_name = s.signal.name, .args = .{ .payloads = s.signal.args }, .header = s.signal.header });
            inst.removeHandler(run);
            result catch |err| {
                if (inst.evicting) return;
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
            const result = inst.inboundNext().executeUpdate(u.update.input());
            inst.removeHandler(run);
            if (inst.evicting) return;
            const response: cmd_pb.UpdateResponse.response_union = if (result) |payload|
                .{ .completed = payload }
            else |err|
                .{ .rejected = updateFailure(a, run, err) catch {
                    inst.task_failure = "out of memory rejecting an update whose handler failed";
                    return;
                } };
            inst.respondToUpdate(u.update.protocol_instance_id, response) catch {
                inst.task_failure = "out of memory completing an update";
            };
        },
    }
}

/// An update handler's error as its rejection: the Application failure it described with
/// `ctx.fail`, or the failure of the activity or child workflow whose error it returned.
fn updateFailure(a: Allocator, run: *const HandlerRun, err: anyerror) error{OutOfMemory}!Failure {
    switch (err) {
        error.ApplicationFailure => if (run.pending_failure) |p| return failure_mod.application(a, p),
        error.ActivityFailed, error.SideEffectFailed, error.ChildWorkflowFailed, error.ChildWorkflowAlreadyStarted, error.ExternalWorkflowNotFound, error.ExternalWorkflowRequestFailed, error.SessionCreationFailed => if (run.last_failure) |f| return f.*,
        else => {},
    }
    return failure_mod.fromError(a, err);
}

/// The handler run executing now, or null in the workflow function, the coroutines it
/// started, and outside any coroutine. A coroutine that a handler starts with `io.async` or a
/// `Group` isn't matched: its failure details go to the workflow function's.
fn currentHandler(inst: *Instance) ?*HandlerRun {
    const fiber = inst.sched.currentFiber() orelse return null;
    for (inst.handlers.items) |run| if (run.task.fiber == fiber) return run;
    return null;
}

/// Records `Context.fail`'s failure for the running coroutine: its handler run's, or the
/// workflow function's.
pub fn setPendingFailure(inst: *Instance, a: failure_mod.Application) void {
    if (inst.currentHandler()) |run| run.pending_failure = a else inst.ctx.pending_failure = a;
}

/// Records the failure a future's `get` returned for the running coroutine, as
/// `setPendingFailure` does. `f` must live as long as the run (arena memory).
pub fn setLastFailure(inst: *Instance, f: ?*const Failure) void {
    if (inst.currentHandler()) |run| run.last_failure = f else inst.ctx.last_failure = f;
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
    var wait = inst.wio.newWaitPoint();
    var cond: Condition = .{ .check = check, .context = context, .wait = &wait };
    try inst.conditions.append(inst.arena(), &cond);
    defer if (!cond.met) inst.removeCondition(&cond);

    var timer: WorkflowIo.Timer = .{ .wait = &wait };
    const timed = if (timeout) |d| d.nanoseconds > 0 else false;
    if (timeout != null and !timed) return false;
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
        .execution_timeout = zigDuration(init.workflow_execution_timeout),
        .run_timeout = zigDuration(init.workflow_run_timeout),
        .task_timeout = zigDuration(init.workflow_task_timeout),
        .start_time = zigTimestamp(init.start_time),
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
    if (init.last_completion_result) |r| if (r.payloads.items.len > 0) {
        inst.last_completion_result = try r.payloads.items[0].dupe(a);
    };
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
    inst.root = try inst.wio.spawn("workflow", rootMain, .{inst});
}

fn rootMain(inst: *Instance) void {
    const started = inst.inboundNext().executeWorkflow(.{
        .workflow_type = inst.info.workflow_type,
        .args = .{ .payloads = inst.args },
        .header = inst.header,
    });
    const outcome: RootOutcome = if (started) |result|
        .{ .completed = result }
    else |err| if (err == error.ContinueAsNew and inst.continue_as_new != null)
        .continued_as_new
    else
        .{ .failed = err };
    // A signal handler's error may have finished the workflow first.
    if (inst.root_outcome == null) inst.root_outcome = outcome;
    inst.root = null; // the task frees itself when this returns
}

fn finishIfDone(inst: *Instance) !void {
    if (inst.finished) return;
    const outcome = inst.root_outcome orelse return;
    inst.finished = true;
    const a = inst.arena();
    if (inst.buffered_signals.items.len > 0) {
        var names: std.ArrayList(u8) = .empty;
        for (inst.buffered_signals.items, 0..) |signal, i| {
            if (i > 0) try names.appendSlice(a, ", ");
            try names.appendSlice(a, signal.name);
        }
        inst.logger.warn("the workflow finished with signals no handler was registered for; they are dropped", .{ .count = inst.buffered_signals.items.len, .SignalNames = names.items });
    }
    switch (outcome) {
        .completed => |result| try inst.commands.append(a, .{ .variant = .{
            .complete_workflow_execution = .{ .result = result },
        } }),
        .continued_as_new => try inst.commands.append(a, .{ .variant = .{
            .continue_as_new_workflow_execution = inst.continue_as_new.?,
        } }),
        .failed => |err| switch (err) {
            // The workflow can't make progress; fail the task so the server retries it.
            error.OutOfMemory => inst.task_failure = "out of memory running the workflow",
            error.Canceled => try inst.commands.append(a, .{ .variant = .{ .cancel_workflow_execution = .{} } }),
            else => try inst.commands.append(a, .{ .variant = .{
                .fail_workflow_execution = .{ .failure = try inst.workflowFailure(err) },
            } }),
        },
    }
}

fn workflowFailure(inst: *Instance, err: anyerror) !Failure {
    const a = inst.arena();
    const pending, const last = if (inst.failed_handler) |run|
        .{ run.pending_failure, run.last_failure }
    else
        .{ inst.ctx.pending_failure, inst.ctx.last_failure };
    if (pending) |p| return failure_mod.application(a, p);
    switch (err) {
        error.ActivityFailed, error.SideEffectFailed, error.ChildWorkflowFailed, error.ChildWorkflowAlreadyStarted, error.ExternalWorkflowNotFound, error.ExternalWorkflowRequestFailed, error.SessionCreationFailed => if (last) |f| return f.*,
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
    return failure_mod.fromError(a, err);
}

// ---------------------------------------------------------------- continue-as-new

/// Encodes `args` into the arena and passes the request through the outbound interceptors to
/// `recordContinueAsNew`.
pub fn requestContinueAsNew(inst: *Instance, workflow_type: []const u8, options: workflow.ContinueAsNewOptions, args: anytype) sa.EncodeError!void {
    const out = try inst.outgoing(args);
    return inst.outboundNext().continueAsNew(.{ .workflow_type = workflow_type, .options = options, .args = out.args, .header = out.header });
}

/// Records the ContinueAsNewWorkflowExecution command to send if the workflow function returns
/// error.ContinueAsNew, replacing any earlier request. `args` and `header` are arena memory.
fn recordContinueAsNew(inst: *Instance, workflow_type: []const u8, options: workflow.ContinueAsNewOptions, args: []const converter.Payload, header: converter.Header) sa.EncodeError!void {
    const a = inst.arena();
    var cmd: cmd_pb.ContinueAsNewWorkflowExecution = .{
        .workflow_type = workflow_type,
        // Empty: core and the server keep this run's task queue.
        .task_queue = try a.dupe(u8, options.task_queue),
        .arguments = try payloadList(a, args),
        .headers = try header.toEntries(cmd_pb.ContinueAsNewWorkflowExecution.HeadersEntry, a),
        .workflow_run_timeout = protoDuration(options.run_timeout),
        .workflow_task_timeout = protoDuration(options.task_timeout),
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
    if (fields.len == 0) return;
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
    try inst.commands.append(a, .{ .variant = .{ .modify_workflow_properties = .{ .upserted_memo = upserted } } });
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
    if (updates.len == 0) return;
    const a = inst.arena();
    const encoded = try sa.toProto(a, updates, .upsert);
    const view = try inst.search_attributes.withEntries(a, encoded.indexed_fields.items);
    try inst.commands.append(a, .{ .variant = .{ .upsert_workflow_search_attributes = .{ .search_attributes = encoded } } });
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
    inst.recordPatch(patch_id, used, deprecated) catch {
        inst.task_failure = "out of memory recording a patch";
    };
    return used;
}

fn recordPatch(inst: *Instance, patch_id: []const u8, used: bool, deprecated: bool) error{OutOfMemory}!void {
    const a = inst.arena();
    const id = try a.dupe(u8, patch_id);
    try inst.patches.put(a, id, used);
    if (used) try inst.commands.append(a, .{ .variant = .{ .set_patch_marker = .{ .patch_id = id, .deprecated = deprecated } } });
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
        inst.versions.put(a, a.dupe(u8, change_id) catch return inst.versionOutOfMemory(v), v) catch return inst.versionOutOfMemory(v);
        break :blk v;
    };
    if (version < min_supported or version > max_supported) {
        // Failing the task, not the workflow, lets a worker whose code supports the
        // version run it. The message reaches the server's WorkflowTaskFailed event; the
        // workflow's logger is silent during the replay that usually finds this.
        inst.task_failure = std.fmt.allocPrint(a, "getVersion(\"{s}\", {d}, {d}): the run uses version {d} of this change, which this code doesn't support", .{ change_id, min_supported, max_supported, version }) catch "getVersion: the run uses a version of a change this code doesn't support";
        return error.UnsupportedVersion;
    }
    if (version == workflow.default_version) return version;
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

fn outgoing(inst: *Instance, args: anytype) error{OutOfMemory}!Outgoing {
    const a = inst.arena();
    const payloads = try converter.toPayloads(a, args);
    const header = try a.create(converter.HeaderWriter);
    header.* = .init(a);
    try propagation.injectAll(inst.propagators, a, &inst.propagated, header);
    return .{ .args = .{ .payloads = payloads.items }, .header = header };
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
    const out = inst.outgoing(args) catch |err| return .{ .ctx = &inst.ctx, .state = err };
    return inst.outboundNext().executeActivity(.{ .activity_type = activity_type, .options = options, .args = out.args, .header = out.header });
}

/// Encodes `args` and schedules a local activity through the outbound interceptors.
pub fn executeLocalActivity(inst: *Instance, activity_type: []const u8, options: workflow.LocalActivityOptions, args: anytype) workflow.ActivityFuture(converter.Payload) {
    const out = inst.outgoing(args) catch |err| return .{ .ctx = &inst.ctx, .state = err };
    return inst.outboundNext().executeLocalActivity(.{ .activity_type = activity_type, .options = options, .args = out.args, .header = out.header });
}

/// Records a side effect's value (null on replay) through the outbound interceptors.
pub fn sideEffect(inst: *Instance, value: ?converter.Payload) workflow.ActivityFuture(converter.Payload) {
    return inst.outboundNext().sideEffect(.{ .value = value });
}

/// Encodes `args` and starts a child workflow through the outbound interceptors.
pub fn executeChildWorkflow(inst: *Instance, workflow_type: []const u8, options: workflow.ChildWorkflowOptions, args: anytype) workflow.ChildWorkflowFuture(converter.Payload) {
    const out = inst.outgoing(args) catch |err| return .{ .ctx = &inst.ctx, .state = err };
    return inst.outboundNext().executeChildWorkflow(.{ .workflow_type = workflow_type, .options = options, .args = out.args, .header = out.header });
}

/// Encodes `args` and sends a signal through the outbound interceptors.
pub fn signalExternal(inst: *Instance, target: SignalTarget, signal_name: []const u8, args: anytype) workflow.SignalExternalFuture {
    const out = inst.outgoing(args) catch |err| return .{ .ctx = &inst.ctx, .state = err };
    const workflow_id, const run_id, const child = switch (target) {
        .execution => |e| .{ e.workflow_id, e.run_id, false },
        .child => |id| .{ id, "", true },
    };
    return inst.outboundNext().signalExternalWorkflow(.{
        .signal_name = signal_name,
        .workflow_id = workflow_id,
        .run_id = run_id,
        .child = child,
        .args = out.args,
        .header = out.header,
    });
}

/// Requests another workflow's cancellation through the outbound interceptors.
pub fn cancelExternalWorkflow(inst: *Instance, workflow_id: []const u8, run_id: []const u8) workflow.CancelExternalFuture {
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
    state.* = .{ .seq = 0, .outcome = .{ .failed = out } };
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
    state.* = .{ .seq = 0, .workflow_id = "", .workflow_type = out.failure_info.?.child_workflow_execution_failure_info.workflow_type.?.name, .cancellation_type = .try_cancel, .outcome = .{ .failed = out } };
    return state;
}

// ---------------------------------------------------------------- activities

pub const ScheduleError = error{ OutOfMemory, InvalidOptions };
/// `SessionFailed`: the activity's session had failed, so it wasn't scheduled.
pub const ActivityScheduleError = ScheduleError || error{SessionFailed};

/// Creates the state of a new activity, local or remote: both share one sequence space.
fn newActivity(inst: *Instance, cancellation_type: ActivityState.CancellationType) !*ActivityState {
    const a = inst.arena();
    const state = try a.create(ActivityState);
    state.* = .{ .seq = inst.next_activity_seq, .cancellation_type = cancellation_type };
    try inst.activities.put(a, state.seq, state);
    inst.next_activity_seq += 1;
    return state;
}

fn scheduleActivity(inst: *Instance, activity_type: []const u8, options: workflow.ActivityOptions, args: []const converter.Payload, header: converter.Header) ScheduleError!*ActivityState {
    const a = inst.arena();
    const state = try inst.newActivity(options.cancellation_type);
    const seq = state.seq;

    var cmd: cmd_pb.ScheduleActivity = .{
        .seq = seq,
        .activity_id = if (options.activity_id.len > 0) options.activity_id else try std.fmt.allocPrint(a, "{d}", .{seq}),
        .activity_type = activity_type,
        .task_queue = if (options.task_queue.len > 0) options.task_queue else inst.info.task_queue,
        .arguments = try payloadList(a, args),
        .headers = try header.toEntries(cmd_pb.ScheduleActivity.HeadersEntry, a),
        .schedule_to_close_timeout = protoDuration(options.schedule_to_close_timeout),
        .schedule_to_start_timeout = protoDuration(options.schedule_to_start_timeout),
        .start_to_close_timeout = protoDuration(options.start_to_close_timeout),
        .heartbeat_timeout = protoDuration(options.heartbeat_timeout),
        .cancellation_type = protoCancellationType(options.cancellation_type),
        .do_not_eagerly_execute = options.disable_eager_execution,
    };
    if (options.retry_policy) |rp| cmd.retry_policy = try protoRetryPolicy(a, rp);
    try inst.commands.append(a, .{ .variant = .{ .schedule_activity = cmd } });
    return state;
}

fn scheduleLocalActivity(inst: *Instance, activity_type: []const u8, options: workflow.LocalActivityOptions, args: []const converter.Payload, header: converter.Header) ScheduleError!*ActivityState {
    if (options.start_to_close_timeout == null and options.schedule_to_close_timeout == null) {
        inst.logger.warn("local activity options need start_to_close_timeout or schedule_to_close_timeout", .{ .ActivityType = activity_type });
        return error.InvalidOptions;
    }
    const a = inst.arena();
    const state = try inst.newActivity(options.cancellation_type);
    const local = try a.create(ActivityState.Local);
    local.* = .{
        .cmd = .{
            .seq = state.seq,
            .activity_id = if (options.activity_id.len > 0) options.activity_id else try std.fmt.allocPrint(a, "{d}", .{state.seq}),
            .activity_type = activity_type,
            .arguments = try payloadList(a, args),
            .headers = try header.toEntries(cmd_pb.ScheduleLocalActivity.HeadersEntry, a),
            .schedule_to_close_timeout = protoDuration(options.schedule_to_close_timeout),
            .schedule_to_start_timeout = protoDuration(options.schedule_to_start_timeout),
            .start_to_close_timeout = protoDuration(options.start_to_close_timeout),
            .local_retry_threshold = protoDuration(options.local_retry_threshold),
            .cancellation_type = protoCancellationType(options.cancellation_type),
        },
        .id_from_seq = options.activity_id.len == 0,
    };
    if (options.retry_policy) |rp| local.cmd.retry_policy = try protoRetryPolicy(a, rp);
    state.local = local;
    try inst.commands.append(a, .{ .variant = .{ .schedule_local_activity = local.cmd } });
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
    try inst.commands.append(a, .{ .variant = .{ .start_timer = .{
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
    if (local.id_from_seq) local.cmd.activity_id = try std.fmt.allocPrint(a, "{d}", .{state.seq});
    try inst.commands.append(a, .{ .variant = .{ .schedule_local_activity = local.cmd } });
}

fn protoCancellationType(t: ActivityState.CancellationType) cmd_pb.ActivityCancellationType {
    return switch (t) {
        .try_cancel => .TRY_CANCEL,
        .wait_cancellation_completed => .WAIT_CANCELLATION_COMPLETED,
        .abandon => .ABANDON,
    };
}

fn protoRetryPolicy(a: Allocator, rp: workflow.RetryPolicy) !common.RetryPolicy {
    var p: common.RetryPolicy = .{
        .initial_interval = protoDuration(rp.initial_interval),
        .backoff_coefficient = rp.backoff_coefficient,
        .maximum_interval = protoDuration(rp.maximum_interval),
        .maximum_attempts = rp.maximum_attempts,
    };
    try p.non_retryable_error_types.appendSlice(a, rp.non_retryable_error_types);
    return p;
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
    var wait = inst.wio.newWaitPoint();
    state.waiter = &wait;
    defer state.waiter = null;
    var canceled = false;
    while (state.outcome == null) {
        if (inst.evicting) return error.Canceled;
        // A cancel also wakes the wait point, so each block starts from a fresh one.
        wait = inst.wio.newWaitPoint();
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
    state.cancel_requested = true;
    inst.requestCancel(state) catch {
        inst.task_failure = "out of memory requesting activity cancellation";
    };
}

fn requestCancel(inst: *Instance, state: *ActivityState) !void {
    const a = inst.arena();
    const local = state.local orelse
        return inst.commands.append(a, .{ .variant = .{ .request_cancel_activity = .{ .seq = state.seq } } });
    const timer = local.backoff_timer orelse
        return inst.commands.append(a, .{ .variant = .{ .request_cancel_local_activity = .{ .seq = state.seq } } });
    // Between attempts nothing runs: cancel the backoff timer and resolve here, whatever the
    // cancellation type.
    _ = inst.backoffs.remove(timer);
    local.backoff_timer = null;
    try inst.commands.append(a, .{ .variant = .{ .cancel_timer = .{ .seq = timer } } });
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
        .failed => |f| .{ .failed = try dupeFailure(a, f.failure) },
        .cancelled => |c| .{ .cancelled = try dupeFailure(a, c.failure) },
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
    var cmd: cmd_pb.StartChildWorkflowExecution = .{
        .seq = state.seq,
        .namespace = inst.namespace,
        .workflow_id = state.workflow_id,
        .workflow_type = workflow_type,
        .task_queue = if (options.task_queue.len > 0) options.task_queue else inst.info.task_queue,
        .input = try payloadList(a, args),
        .headers = try header.toEntries(cmd_pb.StartChildWorkflowExecution.HeadersEntry, a),
        .workflow_execution_timeout = protoDuration(options.execution_timeout),
        .workflow_run_timeout = protoDuration(options.run_timeout),
        .workflow_task_timeout = protoDuration(options.task_timeout),
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
            .wait_cancellation_completed => .WAIT_CANCELLATION_COMPLETED,
            .abandon => .ABANDON,
        },
    };
    if (options.retry_policy) |rp| cmd.retry_policy = try protoRetryPolicy(a, rp);
    try cmd.memo.ensureTotalCapacity(a, options.memo.len);
    for (options.memo) |f| cmd.memo.appendAssumeCapacity(.{ .key = f.name, .value = f.value });
    if (options.search_attributes.len > 0) cmd.search_attributes = sa.toProto(a, options.search_attributes, .start) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        error.InvalidSearchAttribute => return error.InvalidOptions,
    };
    try inst.children.put(a, state.seq, state);
    inst.next_child_seq += 1;
    try inst.commands.append(a, .{ .variant = .{ .start_child_workflow_execution = cmd } });
    return state;
}

/// Blocks the calling coroutine until the child reaches `phase` (started, or closed), or
/// can't: a child that fails to start or closes before it started reaches both. Cancellation
/// works as in `awaitActivity`: it cancels the child, and with `wait_cancellation_completed`
/// waits for the core to resolve it (the child closed, or never started). Eviction ends the
/// wait with error.Canceled.
pub fn awaitChild(inst: *Instance, state: *ChildState, phase: ChildState.Phase) error{Canceled}!void {
    if (state.reached(phase)) return;
    var wait = inst.wio.newWaitPoint();
    const waiter = switch (phase) {
        .started => &state.start_waiter,
        .result => &state.result_waiter,
    };
    waiter.* = &wait;
    defer waiter.* = null;
    var canceled = false;
    while (!state.reached(phase)) {
        if (inst.evicting) return error.Canceled;
        wait = inst.wio.newWaitPoint();
        inst.wio.waitFor(&wait, !canceled) catch |err| {
            inst.cancelChild(state);
            if (state.cancellation_type != .wait_cancellation_completed) return err;
            canceled = true;
        };
    }
    if (!canceled) return;
    if (state.outcome) |o| if (o == .cancelled) return error.Canceled;
    inst.wio.io().recancel();
}

/// Sends CancelChildWorkflowExecution; the core resolves the child as its cancellation type
/// says (and resolves a child whose start it hasn't sent yet as canceled before starting).
pub fn cancelChild(inst: *Instance, state: *ChildState) void {
    if (state.outcome != null or state.cancel_requested) return;
    state.cancel_requested = true;
    inst.commands.append(inst.arena(), .{ .variant = .{ .cancel_child_workflow_execution = .{
        .child_workflow_seq = state.seq,
        .reason = "the parent canceled the child workflow",
    } } }) catch {
        inst.task_failure = "out of memory requesting child workflow cancellation";
    };
}

fn resolveChildStart(inst: *Instance, r: *const act_pb.ResolveChildWorkflowExecutionStart) !void {
    const state = inst.children.get(r.seq) orelse return;
    const a = inst.arena();
    switch (r.status orelse return) {
        .succeeded => |s| state.run_id = try a.dupe(u8, s.run_id),
        .failed => |f| {
            _ = inst.children.remove(r.seq);
            state.already_started = f.cause == .START_CHILD_WORKFLOW_EXECUTION_FAILED_CAUSE_WORKFLOW_ALREADY_EXISTS;
            state.outcome = .{ .failed = try startFailure(inst, state, f.cause) };
        },
        .cancelled => |c| {
            _ = inst.children.remove(r.seq);
            state.outcome = .{ .cancelled = try dupeFailure(a, c.failure) };
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
        .failed => |f| .{ .failed = try dupeFailure(a, f.failure) },
        .cancelled => |c| .{ .cancelled = try dupeFailure(a, c.failure) },
    };
    state.wake(&inst.wio);
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
    try inst.commands.append(a, .{ .variant = .{ .signal_external_workflow_execution = .{
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
    try inst.commands.append(a, .{ .variant = .{ .request_cancel_external_workflow_execution = .{
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
    var wait = inst.wio.newWaitPoint();
    state.waiter = &wait;
    defer state.waiter = null;
    while (state.outcome == null) {
        if (inst.evicting) return error.Canceled;
        wait = inst.wio.newWaitPoint();
        inst.wio.waitFor(&wait, true) catch |err| {
            inst.cancelExternal(state);
            return err;
        };
    }
}

fn cancelExternal(inst: *Instance, state: *ExternalState) void {
    if (state.kind != .signal or state.outcome != null or state.cancel_requested) return;
    state.cancel_requested = true;
    inst.commands.append(inst.arena(), .{ .variant = .{ .cancel_signal_workflow = .{ .seq = state.seq } } }) catch {
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
        return;
    };
    const a = inst.arena();
    const copy = try a.create(Failure);
    copy.* = try f.dupe(a);
    if (f.failure_info != null and f.failure_info.? == .canceled_failure_info) {
        state.outcome = .{ .canceled = copy };
    } else {
        copy.message = try std.fmt.allocPrint(a, "{s} (workflow ID \"{s}\")", .{ f.message, state.workflow_id });
        state.outcome = if (not_found) .{ .not_found = copy } else .{ .failed = copy };
    }
    if (state.waiter) |w| inst.wio.wake(w);
}

fn dupeFailure(a: Allocator, f: ?Failure) !*const Failure {
    const out = try a.create(Failure);
    out.* = if (f) |v| try v.dupe(a) else .{ .message = "failed without a failure from the core" };
    return out;
}

fn zigTimestamp(t: ?proto.google.protobuf.Timestamp) ?std.Io.Timestamp {
    const v = t orelse return null;
    return .fromNanoseconds(@as(i96, v.seconds) * std.time.ns_per_s + v.nanos);
}

fn zigDuration(d: ?proto.google.protobuf.Duration) ?std.Io.Duration {
    const v = d orelse return null;
    return .{ .nanoseconds = @as(i96, v.seconds) * std.time.ns_per_s + v.nanos };
}

fn protoDuration(d: ?std.Io.Duration) ?proto.google.protobuf.Duration {
    const v = d orelse return null;
    return .{
        .seconds = @intCast(@divFloor(v.nanoseconds, std.time.ns_per_s)),
        .nanos = @intCast(@mod(v.nanoseconds, std.time.ns_per_s)),
    };
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
    const args = handler.decode(inst.arena(), input.signal_name, input.args.payloads) catch |err| switch (err) {
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
    const handler = inst.queries.get(input.query_type) orelse return error.QueryNotRegistered;
    return handler.call(handler.context, input.arena, input.args.payloads);
}

fn rootValidateUpdate(ptr: *anyopaque, input: interceptor.UpdateInput, _: interceptor.WorkflowInbound.Next) anyerror!void {
    const inst: *Instance = @ptrCast(@alignCast(ptr));
    const handler = inst.validating orelse return error.HandlerNotFound;
    const args = try handler.decode(inst.arena(), input.args.payloads);
    if (handler.validate) |validate| try validate(handler.context, args);
}

fn rootExecuteUpdate(ptr: *anyopaque, input: interceptor.UpdateInput, _: interceptor.WorkflowInbound.Next) anyerror!converter.Payload {
    const inst: *Instance = @ptrCast(@alignCast(ptr));
    const handler = (try inst.handlerOfCaller(.update)).handler;
    const a = inst.arena();
    return handler.call(handler.context, a, try handler.decode(a, input.args.payloads));
}

const outbound_root: interceptor.WorkflowOutbound.VTable = .{
    .executeActivity = rootExecuteActivity,
    .executeLocalActivity = rootExecuteLocalActivity,
    .executeChildWorkflow = rootExecuteChildWorkflow,
    .signalExternalWorkflow = rootSignalExternalWorkflow,
    .cancelExternalWorkflow = rootCancelExternalWorkflow,
    .continueAsNew = rootContinueAsNew,
    .sideEffect = rootSideEffect,
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
    return .{ .ctx = &inst.ctx, .state = inst.scheduleLocalActivity(side_effect_activity_type, options, args, .{}) };
}

fn rootExecuteChildWorkflow(ptr: *anyopaque, input: interceptor.ChildWorkflowInput, _: interceptor.WorkflowOutbound.Next) workflow.ChildWorkflowFuture(converter.Payload) {
    const inst: *Instance = @ptrCast(@alignCast(ptr));
    return .{ .ctx = &inst.ctx, .state = inst.startChildWorkflow(input.workflow_type, input.options, input.args.payloads, input.header.header()) };
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

fn hostStartTimer(ctx: *anyopaque, timer: *WorkflowIo.Timer, duration: std.Io.Duration) void {
    const inst: *Instance = @ptrCast(@alignCast(ctx));
    const seq = inst.next_timer_seq;
    inst.next_timer_seq += 1;
    timer.host_data = seq;
    inst.timers.put(inst.arena(), seq, timer) catch {
        inst.task_failure = "out of memory starting a timer";
        return;
    };
    inst.commands.append(inst.arena(), .{ .variant = .{ .start_timer = .{
        .seq = seq,
        .start_to_fire_timeout = protoDuration(duration),
    } } }) catch {
        inst.task_failure = "out of memory starting a timer";
    };
}

fn hostCancelTimer(ctx: *anyopaque, timer: *WorkflowIo.Timer) void {
    const inst: *Instance = @ptrCast(@alignCast(ctx));
    const seq: u32 = @intCast(timer.host_data);
    _ = inst.timers.remove(seq);
    inst.commands.append(inst.arena(), .{ .variant = .{ .cancel_timer = .{ .seq = seq } } }) catch {
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

test "unsupported jobs fail the workflow task" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try create(testing.allocator, Hello.registered, .{});
    defer inst.destroy();
    _ = try inst.activate(&try initActivation(a, "Workflow", .{@as([]const u8, "Temporal")}), a);
    const c = try inst.activate(&try oneJob(a, .{ .resolve_nexus_operation_start = .{ .seq = 1 } }), a);
    try testing.expect(c.status.? == .failed);
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
    try testing.expect(answer(c, 1).variant.? == .failed);
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
};

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
