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
args: []const converter.Payload = &.{},
task_queue: []const u8 = "",
namespace: []const u8 = "",
root: ?*WorkflowIo.Task = null,
root_outcome: ?RootOutcome = null,
finished: bool = false,
/// Set when an activation must fail the workflow task (it is retried) instead of producing
/// commands, for example on an unsupported job.
task_failure: ?[]const u8 = null,

const RootOutcome = union(enum) { completed: converter.Payload, failed: anyerror };

pub const ActivityState = struct {
    seq: u32,
    waiter: ?*WorkflowIo.Wait = null,
    outcome: ?Outcome = null,
    cancel_requested: bool = false,

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
    if (!inst.finished) if (inst.root) |root| {
        inst.wio.cancelTask(root);
        inst.root = null;
    };
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
            .fire_timer => |t| if (inst.timers.fetchRemove(t.seq)) |kv| inst.wio.fireTimer(kv.value),
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
        inst.wio.run();
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

fn initialize(inst: *Instance, init: *const act_pb.InitializeWorkflow, run_id: []const u8) !void {
    const a = inst.arena();
    inst.info = .{
        .workflow_id = try a.dupe(u8, init.workflow_id),
        .run_id = try a.dupe(u8, run_id),
        .workflow_type = try a.dupe(u8, init.workflow_type),
        .task_queue = inst.task_queue,
        .namespace = inst.namespace,
        .attempt = @intCast(@max(init.attempt, 0)),
    };
    inst.logger = .{
        .context = try log.contextString(a, .{
            .WorkflowType = inst.info.workflow_type,
            .WorkflowID = inst.info.workflow_id,
            .RunID = inst.info.run_id,
        }),
        .replaying = &inst.replaying,
    };
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

// ---------------------------------------------------------------- activities

pub fn scheduleActivity(inst: *Instance, activity_type: []const u8, options: workflow.ActivityOptions, args: anytype) ?*ActivityState {
    return inst.scheduleActivityInner(activity_type, options, args) catch null;
}

fn scheduleActivityInner(inst: *Instance, activity_type: []const u8, options: workflow.ActivityOptions, args: anytype) !*ActivityState {
    const a = inst.arena();
    const seq = inst.next_activity_seq;
    inst.next_activity_seq += 1;
    const state = try a.create(ActivityState);
    state.* = .{ .seq = seq };
    try inst.activities.put(a, seq, state);

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
        .cancellation_type = switch (options.cancellation_type) {
            .try_cancel => .TRY_CANCEL,
            .wait_cancellation_completed => .WAIT_CANCELLATION_COMPLETED,
            .abandon => .ABANDON,
        },
        .do_not_eagerly_execute = options.disable_eager_execution,
    };
    if (options.retry_policy) |rp| {
        var p: common.RetryPolicy = .{
            .initial_interval = protoDuration(rp.initial_interval),
            .backoff_coefficient = rp.backoff_coefficient,
            .maximum_interval = protoDuration(rp.maximum_interval),
            .maximum_attempts = rp.maximum_attempts,
        };
        try p.non_retryable_error_types.appendSlice(a, rp.non_retryable_error_types);
        cmd.retry_policy = p;
    }
    try inst.commands.append(a, .{ .variant = .{ .schedule_activity = cmd } });
    return state;
}

/// Blocks the calling coroutine until the activity resolves. On workflow cancellation,
/// requests the activity's cancellation and returns error.Canceled.
pub fn awaitActivity(inst: *Instance, state: *ActivityState) error{Canceled}!void {
    if (state.outcome != null) return;
    var wait = inst.wio.newWaitPoint();
    state.waiter = &wait;
    defer state.waiter = null;
    inst.wio.waitFor(&wait, true) catch |err| {
        inst.cancelActivity(state);
        return err;
    };
}

pub fn cancelActivity(inst: *Instance, state: *ActivityState) void {
    if (state.outcome != null or state.cancel_requested) return;
    state.cancel_requested = true;
    inst.commands.append(inst.arena(), .{ .variant = .{ .request_cancel_activity = .{ .seq = state.seq } } }) catch {
        inst.task_failure = "out of memory requesting activity cancellation";
    };
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
        .backoff => {
            inst.task_failure = "local activity backoff is not supported by this SDK version yet";
            return;
        },
    };
    if (state.waiter) |w| inst.wio.wake(w);
}

fn dupeFailure(a: Allocator, f: ?Failure) !*const Failure {
    const out = try a.create(Failure);
    out.* = if (f) |v| try v.dupe(a) else .{ .message = "activity failed" };
    return out;
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
