//! The driver sends an update that the server admits while the first run's first workflow
//! task is in flight; the workflow then answers that task with continue-as-new, and the
//! update must be handled on the new run.
//!
//! To be sure of the timing, the workflow blocks its thread (not just its fiber) inside the
//! first task until the driver has seen the update admitted. Real workflows must never do
//! this. The block is armed only by `execute`, so a replay of the stored history in another
//! process neither blocks nor touches the events.
const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");
const Io = std.Io;
const workflow = temporal.workflow;

const Workflow = workflow.Def("Workflow", runWorkflow);
const Update = workflow.UpdateDef("update", fn () []const u8);
const update_id = "update-id";

/// How long the workflow blocks its thread at most, waiting for the driver.
const block_limit: Io.Duration = .fromSeconds(20);
/// How long the driver waits for the update to be admitted; below `block_limit`, so the
/// update is admitted while the first task is still in flight.
const admit_limit: Io.Duration = .fromSeconds(10);

pub const feature = harness.define(.{
    .workflows = .{Workflow},
    // The first task blocks its thread on purpose for up to `block_limit`.
    .worker_options = .{ .deadlock_detection_timeout = Io.Duration.fromSeconds(30) },
    .execute = execute,
});

/// Set by `execute` only; the first workflow task to see it clears it and blocks.
var block_first_task: std.atomic.Value(bool) = .init(false);
/// Set by the workflow while its first task is in flight and blocked.
var first_task_in_progress: Io.Event = .unset;
/// Set by `execute` once the server has admitted the update, or when it gives up.
var update_admitted: Io.Event = .unset;
/// Set by the workflow when it stopped blocking without `update_admitted`.
var block_timed_out: std.atomic.Value(bool) = .init(false);

const State = struct {
    ctx: *workflow.Context,
    received_update: bool = false,

    fn update(state: *State) []const u8 {
        state.received_update = true;
        return state.ctx.info().run_id;
    }

    fn received(state: *State) bool {
        return state.received_update;
    }
};

/// Continues as new once, then returns the run ID of the new run.
fn runWorkflow(ctx: *workflow.Context) ![]const u8 {
    const state = try ctx.allocator().create(State);
    state.* = .{ .ctx = ctx };
    try ctx.setUpdateHandler(Update, state, State.update);

    if (block_first_task.swap(false, .acq_rel)) {
        // Blocks the worker's thread, not the workflow fiber, so the task stays in flight.
        const io = Io.Threaded.global_single_threaded.io();
        first_task_in_progress.set(io);
        update_admitted.waitTimeout(io, .{ .duration = .{ .raw = block_limit, .clock = .boot } }) catch
            block_timed_out.store(true, .release);
    }

    const info = ctx.info();
    if (info.continued_run_id.len != 0) {
        // The update usually arrives in the new run's first task, but need not.
        try ctx.await(state, State.received);
        return info.run_id;
    }
    return ctx.continueAsNew(Workflow, .{}, .{});
}

const UpdateOutcome = union(enum) {
    run_id: []const u8,
    failed: []const u8,
    err: anyerror,
};

/// Sends the update to the workflow's latest run and waits for its result, on a task of
/// its own while `execute` waits for the server to admit it. Sets `finished` when it
/// returns, so that wait can tell an update call that already ended. Strings are
/// allocated from `gpa` and freed by `execute`.
fn sendUpdate(client: *temporal.client.Client, gpa: std.mem.Allocator, workflow_id: []const u8, io: Io, finished: *Io.Event) UpdateOutcome {
    defer finished.set(io);
    var update = client.updateWorkflow(Update, workflow_id, "", .{}, .{ .update_id = update_id }) catch |err| return .{ .err = err };
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
    return .{ .run_id = gpa.dupe(u8, result) catch return .{ .err = error.OutOfMemory } };
}

fn freeOutcome(gpa: std.mem.Allocator, outcome: UpdateOutcome) void {
    switch (outcome) {
        .run_id, .failed => |s| gpa.free(s),
        .err => {},
    }
}

const Admission = enum { admitted, update_call_ended };

/// Polls until the server has admitted the update, or `sendUpdate` has returned (its
/// `finished` is set) without it, or `admit_limit` passes.
fn waitUntilAdmitted(ctx: *harness.Context, workflow_id: []const u8, finished: *Io.Event) !Admission {
    const io = ctx.io;
    const deadline = Io.Clock.Timestamp.now(io, .boot).addDuration(.{ .raw = admit_limit, .clock = .boot });
    var scratch: std.heap.ArenaAllocator = .init(ctx.gpa);
    defer scratch.deinit();
    while (true) {
        _ = scratch.reset(.retain_capacity);
        if (ctx.client.callService(.workflow, "PollWorkflowExecutionUpdate", scratch.allocator(), .{
            .namespace = ctx.namespace,
            .update_ref = .{ .workflow_execution = .{ .workflow_id = workflow_id }, .update_id = update_id },
            .identity = ctx.client.identity,
            .wait_policy = .{ .lifecycle_stage = .UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ADMITTED },
        }, null)) |_| {
            return .admitted;
        } else |err| switch (err) {
            error.NotFound => {},
            else => return err,
        }
        // An update call that failed or was rejected is never admitted: report its own
        // outcome instead of waiting out `admit_limit`.
        if (finished.isSet()) return .update_call_ended;
        if (Io.Clock.Timestamp.now(io, .boot).durationTo(deadline).raw.nanoseconds <= 0)
            return ctx.fail("update {s} of workflow {s} was not admitted within {f}", .{ update_id, workflow_id, admit_limit });
        // Wakes early when the update call ends.
        finished.waitTimeout(io, .{ .duration = .{ .raw = .fromMilliseconds(50), .clock = .boot } }) catch |err| switch (err) {
            error.Timeout => {},
            error.Canceled => return error.Canceled,
        };
    }
}

fn execute(ctx: *harness.Context) !void {
    const io = ctx.io;
    block_first_task.store(true, .release);
    // Never leave the workflow's thread blocked, whatever happens here.
    defer update_admitted.set(io);

    var run = try ctx.client.executeWorkflow(Workflow, ctx.startOptions(), .{});
    defer run.deinit();
    // Leave no run open on a failure path: terminate the workflow's current run, the first
    // or the one after continue-as-new, unless it completed. A run already closed answers
    // NotFound, which is ignored like every other cleanup error.
    var completed = false;
    defer if (!completed) ctx.client.terminateWorkflow(run.id, "", "feature cleanup") catch {};
    const first_run_id = try ctx.arena.dupe(u8, run.run_id);

    first_task_in_progress.waitTimeout(io, .{ .duration = .{ .raw = .fromSeconds(30), .clock = .boot } }) catch |err| switch (err) {
        error.Timeout => return ctx.fail("the first workflow task of workflow {s} did not start within 30s", .{run.id}),
        error.Canceled => return error.Canceled,
    };

    var update_finished: Io.Event = .unset;
    var pending = try io.concurrent(sendUpdate, .{ ctx.client, ctx.gpa, run.id, io, &update_finished });
    var awaited = false;
    defer if (!awaited) freeOutcome(ctx.gpa, pending.cancel(io));

    const admission = try waitUntilAdmitted(ctx, run.id, &update_finished);
    // The workflow now answers its first task with continue-as-new.
    update_admitted.set(io);

    awaited = true;
    const outcome = pending.await(io);
    defer freeOutcome(ctx.gpa, outcome);
    const update_run_id = switch (outcome) {
        .run_id => |id| id,
        .failed => |message| return switch (admission) {
            .admitted => ctx.fail("expected update {s} to complete, but it failed: {s}", .{ update_id, message }),
            .update_call_ended => ctx.fail("expected update {s} to be admitted, but it failed before admission: {s}", .{ update_id, message }),
        },
        .err => |err| return switch (admission) {
            .admitted => err,
            .update_call_ended => ctx.fail("expected update {s} to be admitted, but the update call returned error.{s} before admission", .{ update_id, @errorName(err) }),
        },
    };
    try ctx.expect(!block_timed_out.load(.acquire), "the first workflow task stopped waiting for the update to be admitted after {f}", .{block_limit});

    const result = try run.get(ctx.arena);
    completed = true;
    try ctx.expect(!std.mem.eql(u8, update_run_id, first_run_id), "expected the update to be handled on the run after continue-as-new, but it was handled on the first run {s}", .{first_run_id});
    try ctx.expectEqualStrings(result, update_run_id);

    const first = try ctx.fetchHistory(run.id, first_run_id);
    try ctx.expectEvent(first, .EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW);
    try ctx.expectNoEvent(first, .EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED);
    try ctx.expectNoEvent(first, .EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_COMPLETED);

    const second = try ctx.fetchHistory(run.id, update_run_id);
    try ctx.expectNoEvent(second, .EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_REJECTED);
    try ctx.expectEventsInOrder(second, &.{
        .EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED,
        .EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_COMPLETED,
        .EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
    });
}
