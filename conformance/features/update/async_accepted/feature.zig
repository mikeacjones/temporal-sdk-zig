//! Go bounds the last `handle.Get` with a context deadline and expects DeadlineExceeded.
//! The Zig equivalent of a context deadline is canceling the task waiting on `get`, so the
//! feature races `get` against a timer and expects the timer to win.
const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");
const workflow = temporal.workflow;
const Io = std.Io;

const Workflow = workflow.Def("Workflow", runWorkflow);
/// The handler's arguments are the sleep in nanoseconds (Go's `time.Duration`) and the
/// disposition.
const TheUpdate = workflow.UpdateDef("theUpdate", fn (i64, i64) i64);
const Shutdown = workflow.SignalDef("shutdown_signal", fn () void);
const Handle = temporal.client.WorkflowUpdateHandle(i64);
const the_update_result = 123;
const succeed = 0;
const fail_with_error = 1;
const requested_sleep: Io.Duration = .fromSeconds(2);

pub const feature = harness.define(.{
    .workflows = .{Workflow},
    .execute = execute,
});

const State = struct {
    ctx: *workflow.Context,
    shutdown: bool = false,

    fn update(state: *State, sleep_ns: i64, disposition: i64) !i64 {
        switch (disposition) {
            succeed => try state.ctx.sleep(.fromNanoseconds(sleep_ns), .{}),
            fail_with_error => return error.IWasToldIShouldFail,
            else => {},
        }
        return the_update_result;
    }

    fn stop(state: *State) void {
        state.shutdown = true;
    }

    fn stopped(state: *State) bool {
        return state.shutdown;
    }
};

fn runWorkflow(ctx: *workflow.Context) !void {
    const state = try ctx.allocator().create(State);
    state.* = .{ .ctx = ctx };
    try ctx.setUpdateHandler(TheUpdate, state, State.update);
    try ctx.setSignalHandler(Shutdown, state, State.stop);
    try ctx.await(state, State.stopped);
}

fn execute(ctx: *harness.Context) !void {
    const io = ctx.io;
    var run = try ctx.client.executeWorkflow(Workflow, ctx.startOptions(), .{});
    defer run.deinit();
    const sleep_ns: i64 = @intCast(requested_sleep.nanoseconds);

    // An update accepted asynchronously returns before its handler finishes.
    const start = Io.Timestamp.now(io, .boot);
    var original = try ctx.client.updateWorkflow(TheUpdate, run.id, run.run_id, .{ sleep_ns, succeed }, .{
        .update_id = "update:1",
        .wait_for_stage = .accepted,
    });
    defer original.deinit();
    const elapsed = start.durationTo(Io.Timestamp.now(io, .boot));
    try ctx.expect(elapsed.nanoseconds < requested_sleep.nanoseconds, "requesting the async update took {f}, expected less than the update's {f}", .{ elapsed, requested_sleep });

    // A second handle to the same update waits for the in-flight update.
    var another = try ctx.client.getWorkflowUpdateHandle(i64, run.id, run.run_id, original.update_id);
    defer another.deinit();
    try expectResult(ctx, &another, "update:1 through a new handle");
    // The update has completed, so the original handle reads its stored outcome.
    try expectResult(ctx, &original, "update:1 through the original handle");

    var failing = try ctx.client.updateWorkflow(TheUpdate, run.id, run.run_id, .{ sleep_ns, fail_with_error }, .{
        .update_id = "update:3",
        .wait_for_stage = .accepted,
    });
    defer failing.deinit();
    if (failing.get(ctx.arena)) |result| {
        return ctx.fail("expected update:3 to fail, it returned {d}", .{result});
    } else |err| switch (err) {
        error.UpdateFailed => {
            const failure = failing.failure() orelse return ctx.fail("failed update:3 has no failure details", .{});
            try ctx.expect(temporal.failure.kind(failure) == .application, "expected an application failure for update:3, got {t}", .{temporal.failure.kind(failure)});
            try ctx.expectEqualStrings("IWasToldIShouldFail", failure.message);
        },
        else => return err,
    }

    var last = try ctx.client.updateWorkflow(TheUpdate, run.id, run.run_id, .{ sleep_ns, succeed }, .{
        .update_id = "update:4",
        .wait_for_stage = .accepted,
    });
    defer last.deinit();
    // The update sleeps for `requested_sleep`, so waiting a tenth of that must time out.
    const wait: Io.Duration = .fromNanoseconds(@divTrunc(requested_sleep.nanoseconds, 10));
    switch (try raceGet(ctx, &last, wait)) {
        .timeout => {},
        .update => |result| {
            const value = result catch |err| return ctx.fail("expected waiting {f} for update:4 to time out, get returned error.{t}", .{ wait, err });
            return ctx.fail("expected waiting {f} for update:4 to time out, it returned {d}", .{ wait, value });
        },
    }

    try ctx.client.signalWorkflow(Shutdown, run.id, run.run_id, .{});
    try run.get(ctx.arena);

    const recorded = try ctx.fetchHistory(run.id, run.run_id);
    try ctx.expectNoEvent(recorded, .EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_REJECTED);
}

fn expectResult(ctx: *harness.Context, handle: *Handle, what: []const u8) !void {
    const result = handle.get(ctx.arena) catch |err| switch (err) {
        error.UpdateFailed => return ctx.fail("{s} failed: {s}", .{ what, if (handle.failure()) |f| f.message else "no failure details" }),
        else => return err,
    };
    try ctx.expect(result == the_update_result, "expected {s} to return {d}, got {d}", .{ what, the_update_result, result });
}

const Raced = union(enum) { update: temporal.client.UpdateResultError!i64, timeout: Io.Cancelable!void };

/// Waits for the update's result for at most `wait`, canceling the wait when time is up.
fn raceGet(ctx: *harness.Context, handle: *Handle, wait: Io.Duration) !Raced {
    const io = ctx.io;
    // `get` runs on its own task, which must not share the feature's task-local arena.
    var scratch: std.heap.ArenaAllocator = .init(ctx.gpa);
    defer scratch.deinit();
    const deadline = Io.Clock.Timestamp.now(io, .boot).addDuration(.{ .raw = wait, .clock = .boot });
    var buffer: [2]Raced = undefined;
    var race: Io.Select(Raced) = .init(io, &buffer);
    defer race.cancelDiscard();
    try race.concurrent(.timeout, Io.Clock.Timestamp.wait, .{ deadline, io });
    try race.concurrent(.update, Handle.get, .{ handle, scratch.allocator() });
    const raced = try race.await();
    switch (raced) {
        .timeout => |result| try result,
        .update => {},
    }
    return raced;
}
