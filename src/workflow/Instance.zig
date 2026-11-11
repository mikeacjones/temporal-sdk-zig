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
const Scheduler = @import("Scheduler.zig");
const WorkflowIo = @import("WorkflowIo.zig");
const Allocator = std.mem.Allocator;

const act_pb = proto.coresdk.workflow_activation;
const cmd_pb = proto.coresdk.workflow_commands;
const done_pb = proto.coresdk.workflow_completion;
const result_pb = proto.coresdk.activity_result;
const common = proto.temporal.api.common.v1;
const Failure = failure_mod.Failure;

gpa: Allocator,
arena_state: std.heap.ArenaAllocator,
sched: Scheduler,
wio: WorkflowIo,
ctx: workflow.Context,
entry: workflow.Registered,
info: workflow.Info = undefined,
logger: log.Logger = .{},
now_ns: i96 = 0,
replaying: bool = false,
/// Commands produced during the current activation.
commands: std.ArrayList(cmd_pb.WorkflowCommand) = .empty,
next_activity_seq: u32 = 1,
next_timer_seq: u32 = 1,
activities: std.AutoHashMapUnmanaged(u32, *ActivityState) = .empty,
timers: std.AutoHashMapUnmanaged(u32, *WorkflowIo.Timer) = .empty,
/// Local activities waiting on a workflow timer before their next attempt, by timer seq.
backoffs: std.AutoHashMapUnmanaged(u32, *ActivityState) = .empty,
/// Coroutines blocked in `awaitCondition`, in the order they started waiting.
conditions: std.ArrayList(*Condition) = .empty,
args: []const converter.Payload = &.{},
last_completion_result: ?converter.Payload = null,
/// The memo as the workflow sees it: the start memo with this run's upserts applied, sorted
/// by name so its order doesn't depend on the wire order of the start memo's map. Each upsert
/// replaces the slice, so a slice handed out earlier keeps its contents. Arena memory.
memo: []const converter.Field = &.{},
task_queue: []const u8 = "",
namespace: []const u8 = "",
root: ?*WorkflowIo.Task = null,
root_outcome: ?RootOutcome = null,
finished: bool = false,
/// Set by `destroy`: waits that ignore workflow cancellation (an activity's cancellation
/// completing) end with error.Canceled instead, so every coroutine can exit.
evicting: bool = false,
/// Set when an activation must fail the workflow task (it is retried) instead of producing
/// commands, for example on an unsupported job.
task_failure: ?[]const u8 = null,

const RootOutcome = union(enum) { completed: converter.Payload, failed: anyerror };

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

pub const Options = struct {
    /// The worker's task queue and namespace, reported in `Info` and used as the default task
    /// queue for activities.
    task_queue: []const u8 = "",
    namespace: []const u8 = "default",
    stack_size: usize = 256 * 1024,
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
    };
    inst.wio = .init(gpa, &inst.sched, .{ .ctx = inst, .vtable = &host_vtable }, 0);
    inst.ctx = .{ .instance = inst };
    inst.task_queue = try inst.arena().dupe(u8, options.task_queue);
    inst.namespace = try inst.arena().dupe(u8, options.namespace);
    return inst;
}

/// Tears the run down: cancels its coroutines, lets them exit, and frees everything.
pub fn destroy(inst: *Instance) void {
    inst.evicting = true;
    if (!inst.finished) if (inst.root) |root| {
        inst.wio.cancelTask(root);
        inst.root = null;
    };
    // Coroutines already waiting for an activity's cancellation to complete don't see the
    // cancel above; wake them. The order doesn't matter: nothing they do is sent to the core.
    var pending = inst.activities.valueIterator();
    while (pending.next()) |state| if (state.*.waiter) |w| inst.wio.wake(w);
    var backing_off = inst.backoffs.valueIterator();
    while (backing_off.next()) |state| if (state.*.waiter) |w| inst.wio.wake(w);
    inst.wio.run();
    const gpa = inst.gpa;
    if (inst.sched.live == 0) {
        inst.sched.deinit();
    } else {
        // A coroutine ignored cancellation (for example it blocked with cancel protection on).
        // Its stack can't be reclaimed safely, so it is leaked rather than freed under it.
        inst.logger.warn("workflow coroutines did not exit on eviction; leaking their stacks", .{ .count = inst.sched.live });
    }
    inst.arena_state.deinit();
    gpa.destroy(inst);
}

pub fn arena(inst: *Instance) Allocator {
    return inst.arena_state.allocator();
}

/// Applies an activation and returns its completion. `out` holds the completion's memory
/// until the caller has sent it. RemoveFromCache jobs are the caller's to handle (complete
/// the activation, then `destroy`); they are ignored here.
pub fn activate(inst: *Instance, act: *const act_pb.WorkflowActivation, out: Allocator) !done_pb.WorkflowActivationCompletion {
    if (act.timestamp) |ts| inst.now_ns = @as(i96, ts.seconds) * std.time.ns_per_s + ts.nanos;
    inst.replaying = act.is_replaying;
    inst.commands = .empty;
    inst.task_failure = null;

    for (act.jobs.items) |job| {
        const variant = job.variant orelse continue;
        switch (variant) {
            .initialize_workflow => |init| try inst.initialize(&init, act.run_id),
            .fire_timer => |t| if (inst.timers.fetchRemove(t.seq)) |kv| {
                inst.wio.fireTimer(kv.value);
            } else if (inst.backoffs.fetchRemove(t.seq)) |kv| {
                try inst.retryLocalActivity(kv.value);
            },
            .update_random_seed => |s| inst.wio.setRandomSeed(s.randomness_seed),
            .resolve_activity => |r| try inst.resolveActivity(&r),
            .cancel_workflow => if (!inst.finished) if (inst.root) |root| inst.wio.cancelTask(root),
            .remove_from_cache => {},
            else => |other| {
                inst.task_failure = try std.fmt.allocPrint(out, "workflow activation job '{t}' is not supported by this SDK version yet", .{other});
            },
        }
    }

    if (inst.task_failure == null) {
        inst.runUntilQuiet();
        try inst.finishIfDone();
    }

    var completion: done_pb.WorkflowActivationCompletion = .{ .run_id = act.run_id };
    if (inst.task_failure) |message| {
        completion.status = .{ .failed = .{ .failure = try failure_mod.application(out, .{ .message = message, .type = "UnsupportedJob" }) } };
    } else {
        var success: done_pb.Success = .{};
        try success.commands.appendSlice(out, inst.commands.items);
        completion.status = .{ .successful = success };
    }
    return completion;
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
    };
    if (init.last_completion_result) |r| if (r.payloads.items.len > 0) {
        inst.last_completion_result = try r.payloads.items[0].dupe(a);
    };
    inst.logger = .{
        .context = try log.contextString(a, .{
            .WorkflowType = inst.info.workflow_type,
            .WorkflowID = inst.info.workflow_id,
            .RunID = inst.info.run_id,
        }),
        .replaying = &inst.replaying,
    };
    if (init.memo) |m| {
        const fields = try a.alloc(converter.Field, m.fields.items.len);
        for (m.fields.items, fields) |entry, *f| f.* = .{
            .name = try a.dupe(u8, entry.key),
            .value = if (entry.value) |v| try v.dupe(a) else .{},
        };
        std.mem.sort(converter.Field, fields, {}, fieldLessThan);
        inst.memo = fields;
    }
    const args = try a.alloc(converter.Payload, init.arguments.items.len);
    for (init.arguments.items, args) |p, *d| d.* = try p.dupe(a);
    inst.args = args;
    inst.wio.setRandomSeed(init.randomness_seed);
    inst.root = try inst.wio.spawn("workflow", rootMain, .{inst});
}

fn rootMain(inst: *Instance) void {
    if (inst.entry.invoke(&inst.ctx, inst.args)) |result| {
        inst.root_outcome = .{ .completed = result };
    } else |err| {
        inst.root_outcome = .{ .failed = err };
    }
}

fn finishIfDone(inst: *Instance) !void {
    if (inst.finished) return;
    const outcome = inst.root_outcome orelse return;
    inst.finished = true;
    inst.root = null; // the task freed itself
    const a = inst.arena();
    switch (outcome) {
        .completed => |result| try inst.commands.append(a, .{ .variant = .{
            .complete_workflow_execution = .{ .result = result },
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
    if (inst.ctx.pending_failure) |p| return failure_mod.application(a, p);
    if (err == error.ActivityFailed) if (inst.ctx.last_failure) |f| return f.*;
    return failure_mod.fromError(a, err);
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

// ---------------------------------------------------------------- activities

pub const ScheduleError = error{ OutOfMemory, InvalidOptions };

/// Creates the state of a new activity, local or remote: both share one sequence space.
fn newActivity(inst: *Instance, cancellation_type: ActivityState.CancellationType) !*ActivityState {
    const a = inst.arena();
    const state = try a.create(ActivityState);
    state.* = .{ .seq = inst.next_activity_seq, .cancellation_type = cancellation_type };
    try inst.activities.put(a, state.seq, state);
    inst.next_activity_seq += 1;
    return state;
}

pub fn scheduleActivity(inst: *Instance, activity_type: []const u8, options: workflow.ActivityOptions, args: anytype) ScheduleError!*ActivityState {
    const a = inst.arena();
    const state = try inst.newActivity(options.cancellation_type);
    const seq = state.seq;

    var cmd: cmd_pb.ScheduleActivity = .{
        .seq = seq,
        .activity_id = if (options.activity_id.len > 0) options.activity_id else try std.fmt.allocPrint(a, "{d}", .{seq}),
        .activity_type = activity_type,
        .task_queue = if (options.task_queue.len > 0) options.task_queue else inst.info.task_queue,
        .arguments = try converter.toPayloads(a, args),
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

pub fn scheduleLocalActivity(inst: *Instance, activity_type: []const u8, options: workflow.LocalActivityOptions, args: anytype) ScheduleError!*ActivityState {
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
            .arguments = try converter.toPayloads(a, args),
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

fn dupeFailure(a: Allocator, f: ?Failure) !*const Failure {
    const out = try a.create(Failure);
    out.* = if (f) |v| try v.dupe(a) else .{ .message = "activity failed" };
    return out;
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
    const c = try inst.activate(&try oneJob(a, .{ .signal_workflow = .{ .signal_name = "s" } }), a);
    try testing.expect(c.status.? == .failed);
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
    const sched = first[0].variant.?.schedule_activity;
    try testing.expectEqualStrings("Tick", sched.activity_type);
    try testing.expectEqualStrings("41", sched.arguments.items[0].data);

    const done = commandsOf(try inst.activate(&try oneJob(a, .{ .resolve_activity = .{ .seq = 1, .result = .{ .status = .{ .completed = .{} } } } }), a));
    try testing.expectEqualStrings("42", done[0].variant.?.complete_workflow_execution.result.?.data);
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
