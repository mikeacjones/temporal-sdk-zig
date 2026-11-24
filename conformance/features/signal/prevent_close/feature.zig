//! A signal that arrives while a workflow task is completing the workflow keeps it open:
//! the server refuses the completion, and the workflow processes the signal before it
//! closes. A signal sent after the workflow closed finds no workflow.
//!
//! The suite's version sleeps 300 ms inside the workflow, after its first signal, and sends
//! the second signal 200 ms after the first. Here the workflow instead holds its thread until
//! `execute` has sent the second signal, so the signal arrives while the completing task is
//! in flight whatever the timing. The hold is armed only by `execute`, once, so neither a
//! replay of the run on the next workflow task nor `zig build test`'s replay holds.
const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");

const Io = std.Io;
const workflow = temporal.workflow;

const Workflow = workflow.Def("Workflow", runWorkflow);
const Add = workflow.SignalDef("add", fn (i64) void);

/// The workflow's result: the signals' values in arrival order, and whether the workflow
/// function started on a replay.
const Result = struct { values: []const i64, replayed: bool };

pub const feature = harness.define(.{
    .workflows = .{Workflow},
    .execute = execute,
});

/// Set by `execute` to make the next workflow that has a value hold its thread once.
var hold_armed: std.atomic.Value(bool) = .init(false);
/// Set by the workflow when it starts holding.
var hold_reached: std.atomic.Value(bool) = .init(false);
/// Set by `execute` to end the hold.
var hold_released: std.atomic.Value(bool) = .init(false);
/// Written by `execute` before it arms the hold, read only by a holding workflow.
var hold_io: Io = undefined;
/// The longest a workflow holds, well under the worker's 2 s deadlock detection timeout.
const hold_limit: Io.Duration = .fromMilliseconds(1000);
/// The longest `execute` waits for the workflow to start holding.
const reach_limit: Io.Duration = .fromSeconds(10);

const State = struct {
    values: std.ArrayList(i64) = .empty,
    allocator: std.mem.Allocator,

    fn add(state: *State, arg: i64) !void {
        try state.values.append(state.allocator, arg);
    }

    fn hasValues(state: *State) bool {
        return state.values.items.len != 0;
    }
};

fn runWorkflow(ctx: *workflow.Context) !Result {
    const replayed = ctx.isReplaying();
    const state = try ctx.allocator().create(State);
    state.* = .{ .allocator = ctx.allocator() };
    try ctx.setSignalHandler(Add, state, State.add);
    try ctx.await(state, State.hasValues);
    holdOnce();
    return .{ .values = state.values.items, .replayed = replayed };
}

/// Holds the workflow thread until `execute` releases it or `hold_limit` passes, if
/// `execute` armed the hold; otherwise returns at once.
fn holdOnce() void {
    if (hold_armed.cmpxchgStrong(true, false, .acq_rel, .acquire) != null) return;
    const io = hold_io;
    hold_reached.store(true, .release);
    const deadline = Io.Timestamp.now(io, .boot).addDuration(hold_limit);
    while (!hold_released.load(.acquire)) {
        if (Io.Timestamp.now(io, .boot).durationTo(deadline).nanoseconds <= 0) return;
        io.sleep(.fromMilliseconds(5), .boot) catch return;
    }
}

fn execute(ctx: *harness.Context) !void {
    try signalOutOfExecution(ctx);
    try preventClose(ctx);
}

/// One signal completes the workflow without a replay, and a second signal sent after it
/// closed returns `error.NotFound`.
fn signalOutOfExecution(ctx: *harness.Context) !void {
    var options = ctx.startOptions();
    options.id = try std.fmt.allocPrint(ctx.arena, "{s}-out-of-execution", .{ctx.task_queue});
    var run = try ctx.client.executeWorkflow(Workflow, options, .{});
    defer run.deinit();
    try ctx.client.signalWorkflow(Add, run.id, run.run_id, .{1});
    const result = try run.get(ctx.arena);
    if (ctx.client.signalWorkflow(Add, run.id, run.run_id, .{2})) {
        return ctx.fail("signaling the completed workflow succeeded; expected error.NotFound", .{});
    } else |err| switch (err) {
        error.NotFound => {},
        else => return err,
    }
    try ctx.expect(std.mem.eql(i64, result.values, &.{1}), "expected the values {{ 1 }}, got {any}", .{result.values});
    try ctx.expect(!result.replayed, "expected the workflow not to be replayed", .{});
}

/// A second signal sent while the workflow task that completes the workflow is in flight
/// is processed before the workflow closes.
fn preventClose(ctx: *harness.Context) !void {
    var options = ctx.startOptions();
    options.id = try std.fmt.allocPrint(ctx.arena, "{s}-prevent-close", .{ctx.task_queue});
    var run = try ctx.client.executeWorkflow(Workflow, options, .{});
    defer run.deinit();

    hold_io = ctx.io;
    hold_reached.store(false, .release);
    hold_released.store(false, .release);
    hold_armed.store(true, .release);
    defer {
        hold_armed.store(false, .release);
        hold_released.store(true, .release);
    }

    try ctx.client.signalWorkflow(Add, run.id, run.run_id, .{1});
    const deadline = Io.Timestamp.now(ctx.io, .boot).addDuration(reach_limit);
    while (!hold_reached.load(.acquire)) {
        if (Io.Timestamp.now(ctx.io, .boot).durationTo(deadline).nanoseconds <= 0)
            return ctx.fail("the workflow did not process the first signal within {f}", .{reach_limit});
        try ctx.io.sleep(.fromMilliseconds(5), .boot);
    }
    try ctx.client.signalWorkflow(Add, run.id, run.run_id, .{2});
    hold_released.store(true, .release);

    const result = try run.get(ctx.arena);
    try ctx.expect(std.mem.eql(i64, result.values, &.{ 1, 2 }), "expected the values {{ 1, 2 }}: the second signal arrived while the workflow was completing; got {any}", .{result.values});

    // The server refused the task that tried to complete the workflow, because the second
    // signal had arrived, and the next task processed it.
    const recorded = try ctx.fetchHistory(run.id, run.run_id);
    try ctx.expectEventsInOrder(recorded, &.{
        .EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
        .EVENT_TYPE_WORKFLOW_TASK_FAILED,
        .EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
        .EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
    });
}
