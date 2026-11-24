//! Resets a workflow to its second completed workflow task, finishes the run the reset
//! made, deletes that run, and checks that the original run is still readable and listed
//! as terminated. The suite's README says this is mostly a test of the server.
//!
//! The SDK has no delete call, so the new run is deleted with the raw
//! `DeleteWorkflowExecution` request through `Client.callService`.
const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");

const Io = std.Io;
const workflow = temporal.workflow;
const Workflow = workflow.Def("Workflow", runWorkflow);
const DriveSignal = workflow.SignalDef("drivesignal", fn ([]const u8) void);
const signal_count = 5;
const reset_reason = "because I feel like it";

pub const feature = harness.define(.{
    .workflows = .{Workflow},
    .execute = execute,
});

const SignalState = struct {
    finished: bool = false,

    fn receive(state: *SignalState, value: []const u8) void {
        if (value.len != 0) state.finished = true;
    }

    fn done(state: *SignalState) bool {
        return state.finished;
    }
};

/// Waits for a `drivesignal` carrying a non-empty string, then returns "".
fn runWorkflow(ctx: *workflow.Context) ![]const u8 {
    const state = try ctx.allocator().create(SignalState);
    state.* = .{};
    try ctx.setSignalHandler(DriveSignal, state, SignalState.receive);
    try ctx.await(state, SignalState.done);
    return "";
}

fn execute(ctx: *harness.Context) !void {
    const client = ctx.client;
    var run = try client.executeWorkflow(Workflow, ctx.startOptions(), .{});
    defer run.deinit();
    // Closes whichever run is still open if the feature fails part way.
    defer client.terminateWorkflow(run.id, "", "feature cleanup") catch {};
    const original_run_id = run.run_id;

    for (0..signal_count) |_| try client.signalWorkflow(DriveSignal, run.id, original_run_id, .{""});

    const reset_point = try secondWorkflowTaskCompleted(ctx, run.id, original_run_id);
    const new_run_id = try client.resetWorkflow(ctx.arena, run.id, original_run_id, .{
        .workflow_task_finish_event_id = reset_point,
        .reason = reset_reason,
    });

    // Drive, then finish, the run the reset made, through the workflow's latest run.
    try client.signalWorkflow(DriveSignal, run.id, "", .{""});
    try client.signalWorkflow(DriveSignal, run.id, "", .{"finish"});
    var new_run = try client.getWorkflow([]const u8, run.id, new_run_id);
    defer new_run.deinit();
    try ctx.expectEqualStrings("", try new_run.get(ctx.arena));

    try deleteRun(ctx, run.id, new_run_id);
    try waitUntilGone(ctx, run.id, new_run_id);

    const original = try ctx.fetchHistory(run.id, original_run_id);
    const events = original.events.items;
    const last: harness.history.EventType = if (events.len == 0) .EVENT_TYPE_UNSPECIFIED else events[events.len - 1].event_type;
    try ctx.expect(last == .EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED, "expected the original run {s} to end with EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED, its last event is {t}", .{ original_run_id, last });

    try waitUntilListedTerminated(ctx, run.id, original_run_id);
}

/// The event ID of the run's second WorkflowTaskCompleted, once all the signals have been
/// recorded and no workflow task is outstanding.
fn secondWorkflowTaskCompleted(ctx: *harness.Context, workflow_id: []const u8, run_id: []const u8) !i64 {
    const timeout: Io.Duration = .fromSeconds(10);
    const deadline = Io.Timestamp.now(ctx.io, .boot).addDuration(timeout);
    var scratch: std.heap.ArenaAllocator = .init(ctx.gpa);
    defer scratch.deinit();
    var completed: usize = 0;
    var signaled: usize = 0;
    while (true) {
        _ = scratch.reset(.retain_capacity);
        const recorded = try ctx.client.fetchHistory(scratch.allocator(), workflow_id, run_id);
        const events = recorded.events.items;
        signaled = harness.history.count(recorded, .EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED);
        completed = harness.history.count(recorded, .EVENT_TYPE_WORKFLOW_TASK_COMPLETED);
        const settled = events.len != 0 and events[events.len - 1].event_type == .EVENT_TYPE_WORKFLOW_TASK_COMPLETED;
        if (signaled == signal_count and completed >= 2 and settled) {
            var seen: usize = 0;
            for (events) |event| {
                if (event.event_type != .EVENT_TYPE_WORKFLOW_TASK_COMPLETED) continue;
                seen += 1;
                if (seen == 2) return event.event_id;
            }
            unreachable;
        }
        if (Io.Timestamp.now(ctx.io, .boot).durationTo(deadline).nanoseconds <= 0) break;
        try ctx.io.sleep(.fromMilliseconds(100), .boot);
    }
    return ctx.fail("expected {d} signals and at least 2 completed workflow tasks in run {s} within {f}, saw {d} signals and {d} completed workflow tasks", .{ signal_count, run_id, timeout, signaled, completed });
}

/// Deletes the run, retrying while the server refuses (it can answer that the workflow's
/// state is not ready just after the run closes).
fn deleteRun(ctx: *harness.Context, workflow_id: []const u8, run_id: []const u8) !void {
    const timeout: Io.Duration = .fromSeconds(30);
    const deadline = Io.Timestamp.now(ctx.io, .boot).addDuration(timeout);
    var scratch: std.heap.ArenaAllocator = .init(ctx.gpa);
    defer scratch.deinit();
    var failure: ?temporal.client.RpcFailure = null;
    while (true) {
        _ = scratch.reset(.retain_capacity);
        failure = null;
        const result = ctx.client.callService(.workflow, "DeleteWorkflowExecution", scratch.allocator(), .{
            .namespace = ctx.namespace,
            .workflow_execution = .{ .workflow_id = workflow_id, .run_id = run_id },
        }, &failure);
        if (result) |_| return else |err| switch (err) {
            error.OutOfMemory, error.Canceled => |e| return e,
            else => if (Io.Timestamp.now(ctx.io, .boot).durationTo(deadline).nanoseconds <= 0) {
                const message = if (failure) |f| f.message else "";
                return ctx.fail("DeleteWorkflowExecution of run {s} still returned error.{t} after {f}: {s}", .{ run_id, err, timeout, message });
            },
        }
        try ctx.io.sleep(.fromSeconds(1), .boot);
    }
}

/// Deletion is asynchronous: waits until describing the run finds nothing.
fn waitUntilGone(ctx: *harness.Context, workflow_id: []const u8, run_id: []const u8) !void {
    const timeout: Io.Duration = .fromSeconds(60);
    const deadline = Io.Timestamp.now(ctx.io, .boot).addDuration(timeout);
    var scratch: std.heap.ArenaAllocator = .init(ctx.gpa);
    defer scratch.deinit();
    while (true) {
        _ = scratch.reset(.retain_capacity);
        if (ctx.client.describeWorkflow(scratch.allocator(), workflow_id, run_id)) |_| {} else |err| switch (err) {
            error.NotFound => return,
            error.OutOfMemory, error.Canceled => |e| return e,
            else => {},
        }
        if (Io.Timestamp.now(ctx.io, .boot).durationTo(deadline).nanoseconds <= 0)
            return ctx.fail("expected describing the deleted run {s} to return error.NotFound within {f}", .{ run_id, timeout });
        try ctx.io.sleep(.fromSeconds(1), .boot);
    }
}

/// Visibility is eventually consistent: waits until the workflow ID lists exactly the
/// original run, terminated.
fn waitUntilListedTerminated(ctx: *harness.Context, workflow_id: []const u8, run_id: []const u8) !void {
    const timeout: Io.Duration = .fromSeconds(60);
    const deadline = Io.Timestamp.now(ctx.io, .boot).addDuration(timeout);
    const query = try std.fmt.allocPrint(ctx.arena, "WorkflowId = '{s}'", .{workflow_id});
    var scratch: std.heap.ArenaAllocator = .init(ctx.gpa);
    defer scratch.deinit();
    var listed: usize = 0;
    var status: temporal.client.WorkflowExecutionStatus = .unspecified;
    var listed_run: []const u8 = "";
    while (true) {
        _ = scratch.reset(.retain_capacity);
        listed = 0;
        listed_run = "";
        var executions = try ctx.client.listWorkflows(query, .{});
        defer executions.deinit();
        while (executions.next(scratch.allocator()) catch |err| switch (err) {
            error.OutOfMemory, error.Canceled => |e| return e,
            else => null,
        }) |info| {
            listed += 1;
            status = info.status;
            listed_run = info.run_id;
        }
        if (listed == 1 and status == .terminated and std.mem.eql(u8, listed_run, run_id)) return;
        if (Io.Timestamp.now(ctx.io, .boot).durationTo(deadline).nanoseconds <= 0) break;
        try ctx.io.sleep(.fromMilliseconds(200), .boot);
    }
    if (listed != 1)
        return ctx.fail("expected visibility to list 1 run of workflow {s} within {f}, it listed {d}", .{ workflow_id, timeout, listed });
    return ctx.fail("expected visibility to list run {s} of workflow {s} as terminated within {f}, it listed run {s} as {t}", .{ run_id, workflow_id, timeout, listed_run, status });
}
