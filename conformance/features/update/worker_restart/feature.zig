//! The update's activity blocks until the driver has stopped the worker that runs it.
//! Stopping the worker cancels the blocked attempt; the driver then lets the activity
//! through and starts a new worker, whose retry of the activity completes the update.
const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");
const Io = std.Io;
const activity = temporal.activity;
const workflow = temporal.workflow;

const Workflow = workflow.Def("Workflow", runWorkflow);
const Block = activity.Def("Block", block);
const FetchAndAdd = workflow.UpdateDef("fetchAndAdd", fn (i64) i64);
const Shutdown = workflow.SignalDef("shutdown_signal", fn () void);
const addend = 1;

pub const feature = harness.define(.{
    .workflows = .{Workflow},
    .activities = .{Block},
    .execute = execute,
});

/// Set by the activity once it runs; read by `execute`.
var update_started: Io.Event = .unset;
/// Set by `execute` once the worker has stopped; the activity waits for it.
var update_continue: Io.Event = .unset;

fn block(ctx: *activity.Context) !void {
    const io = ctx.io();
    update_started.set(io);
    // Canceled when the first worker stops; the retry on the new worker finds it set.
    try update_continue.wait(io);
}

const State = struct {
    ctx: *workflow.Context,
    counter: i64 = 0,
    shutdown: bool = false,

    fn fetchAndAdd(state: *State, i: i64) !i64 {
        try state.ctx.executeActivity(Block, .{ .schedule_to_close_timeout = .fromSeconds(10) }, .{}).get();
        const previous = state.counter;
        state.counter += i;
        return previous;
    }

    fn stop(state: *State) void {
        state.shutdown = true;
    }

    fn stopped(state: *State) bool {
        return state.shutdown;
    }
};

fn runWorkflow(ctx: *workflow.Context) !i64 {
    const state = try ctx.allocator().create(State);
    state.* = .{ .ctx = ctx };
    try ctx.setUpdateHandler(FetchAndAdd, state, State.fetchAndAdd);
    try ctx.setSignalHandler(Shutdown, state, State.stop);
    try ctx.await(state, State.stopped);
    return state.counter;
}

const UpdateOutcome = union(enum) {
    result: i64,
    failed: []const u8,
    err: anyerror,
};

/// Sends the update and waits for its outcome, on a task of its own while `execute`
/// restarts the worker. Messages are allocated from `gpa` and freed by `execute`.
fn sendUpdate(client: *temporal.client.Client, gpa: std.mem.Allocator, workflow_id: []const u8, run_id: []const u8) UpdateOutcome {
    var update = client.updateWorkflow(FetchAndAdd, workflow_id, run_id, .{addend}, .{}) catch |err| return .{ .err = err };
    defer update.deinit();
    var scratch: std.heap.ArenaAllocator = .init(gpa);
    defer scratch.deinit();
    const result = update.get(scratch.allocator()) catch |err| switch (err) {
        error.UpdateFailed => {
            const message = if (update.failure()) |f| f.message else "no failure details";
            return .{ .failed = gpa.dupe(u8, message) catch return .{ .err = error.OutOfMemory } };
        },
        else => return .{ .err = err },
    };
    return .{ .result = result };
}

fn execute(ctx: *harness.Context) !void {
    const io = ctx.io;
    var run = try ctx.client.executeWorkflow(Workflow, ctx.startOptions(), .{});
    defer run.deinit();

    var pending = try io.concurrent(sendUpdate, .{ ctx.client, ctx.gpa, run.id, run.run_id });
    var awaited = false;
    defer if (!awaited) switch (pending.cancel(io)) {
        .failed => |message| ctx.gpa.free(message),
        else => {},
    };

    const start_timeout: Io.Duration = .fromSeconds(30);
    update_started.waitTimeout(io, .{ .duration = .{ .raw = start_timeout, .clock = .boot } }) catch |err| switch (err) {
        error.Timeout => return ctx.fail("the update's activity did not start within {f}", .{start_timeout}),
        error.Canceled => return error.Canceled,
    };
    try ctx.stopWorker();
    update_continue.set(io);
    try ctx.startWorker();

    awaited = true;
    switch (pending.await(io)) {
        .result => |result| try ctx.expect(result == 0, "expected the update to return 0, got {d}", .{result}),
        .failed => |message| {
            defer ctx.gpa.free(message);
            return ctx.fail("update failed: {s}", .{message});
        },
        .err => |err| return err,
    }

    try ctx.client.signalWorkflow(Shutdown, run.id, run.run_id, .{});
    const result = try run.get(ctx.arena);
    try ctx.expect(result == addend, "expected the workflow to return {d}, got {d}", .{ addend, result });

    const recorded = try ctx.fetchHistory(run.id, run.run_id);
    try ctx.expectNoEvent(recorded, .EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_REJECTED);
    try ctx.expectEventsInOrder(recorded, &.{
        .EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED,
        .EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
        .EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_COMPLETED,
        .EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
    });
}
