//! A workflow that started on a version keeps running only on workers of that version's
//! compatible set. The queue has {1.0} and {2.0, 2.1}, with 2.1 the default; the workflow
//! starts on the feature's 2.1 worker and waits for a signal; that worker stops; a 2.0 and
//! then a 1.0 worker in turn must not get its next task; a 2.1 worker again completes it.
const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");
const Io = std.Io;
const workflow = temporal.workflow;

const Workflow = workflow.Def("Workflow", runWorkflow);
const Waiting = workflow.QueryDef("waiting", fn () bool);
const Unblocker = workflow.SignalDef("unblocker", fn () void);

pub const feature = harness.define(.{
    .workflows = .{Workflow},
    .worker_options = temporal.worker.Options{ .build_id = "2.1", .use_build_id_versioning = true },
    .dev_server_args = &harness.build_ids.dev_server_args,
    .execute = execute,
});

const State = struct {
    waiting: bool = false,
    unblocked: bool = false,

    fn isWaiting(state: *const State) bool {
        return state.waiting;
    }

    fn unblock(state: *State) void {
        state.unblocked = true;
    }

    fn isUnblocked(state: *State) bool {
        return state.unblocked;
    }
};

fn runWorkflow(ctx: *workflow.Context) !void {
    const state = try ctx.allocator().create(State);
    state.* = .{};
    try ctx.setQueryHandler(Waiting, @as(*const State, state), State.isWaiting);
    try ctx.setSignalHandler(Unblocker, state, State.unblock);
    state.waiting = true;
    try ctx.await(state, State.isUnblocked);
}

fn execute(ctx: *harness.Context) !void {
    try harness.build_ids.addSomeVersions(ctx);
    // A worker started after the versions exist polls with them at once.
    try ctx.stopWorker();
    try ctx.startWorker();

    var run = try ctx.client.executeWorkflow(Workflow, ctx.startOptions(), .{});
    defer run.deinit();
    // Ends the run on a failed check, when no 2.1 worker may be left to complete it.
    defer ctx.client.terminateWorkflow(run.id, run.run_id, "feature cleanup") catch {};
    try waitUntilWaiting(ctx, run.id, run.run_id);
    try ctx.stopWorker();
    try harness.build_ids.resetStickyQueue(ctx, run.id, run.run_id);
    // A compatible worker started below completes the workflow with this signal.
    try ctx.client.signalWorkflow(Unblocker, run.id, run.run_id, .{});

    inline for (.{ "2.0", "1.0" }) |build_id| {
        try ctx.startWorkerWith(.{ .build_id = build_id, .use_build_id_versioning = true });
        try mustNotAnswer(ctx, run.id, run.run_id, "worker with version " ++ build_id);
        try ctx.stopWorker();
        try harness.build_ids.resetStickyQueue(ctx, run.id, run.run_id);
    }

    try ctx.startWorkerWith(.{ .build_id = "2.1", .use_build_id_versioning = true });
    run.get(ctx.arena) catch |err| {
        const message = if (run.failure()) |failure| failure.message else "no failure details";
        return ctx.fail("workflow returned error.{t}: {s}", .{ err, message });
    };
    try expectAllCompletedBy(ctx, run.id, run.run_id, "2.1");
}

/// Queries "waiting" every 50 ms until it answers true, for at most 5 s, as the suite's
/// QueryUntilEventually does. Each query has a 1 s deadline.
fn waitUntilWaiting(ctx: *harness.Context, workflow_id: []const u8, run_id: []const u8) !void {
    const io = ctx.io;
    const limit: Io.Duration = .fromSeconds(5);
    const deadline = Io.Clock.Timestamp.now(io, .boot).addDuration(.{ .raw = limit, .clock = .boot });
    var short = try ctx.client.withRpcOptions(.{ .timeout = .fromSeconds(1), .retry = false });
    var scratch: std.heap.ArenaAllocator = .init(ctx.gpa);
    defer scratch.deinit();
    var last: []const u8 = "no answer";
    while (true) {
        _ = scratch.reset(.retain_capacity);
        if (short.queryWorkflow(Waiting, scratch.allocator(), workflow_id, run_id, .{})) |waiting| {
            if (waiting) return;
            last = "false";
        } else |err| switch (err) {
            error.Canceled => return error.Canceled,
            else => last = @errorName(err),
        }
        if (deadline.compare(.lte, Io.Clock.Timestamp.now(io, .boot)))
            return ctx.fail("query \"waiting\" did not answer true within {f}; last: {s}", .{ limit, last });
        try io.sleep(.fromMilliseconds(50), .boot);
    }
}

/// The suite's MustTimeoutQuery: a query with a 3 s deadline must not be answered, since no
/// worker polling may take the run's tasks. Any error is the expected outcome.
fn mustNotAnswer(ctx: *harness.Context, workflow_id: []const u8, run_id: []const u8, comptime who: []const u8) !void {
    var short = try ctx.client.withRpcOptions(.{ .timeout = .fromSeconds(3), .retry = false });
    const answer = short.queryWorkflow(Waiting, ctx.arena, workflow_id, run_id, .{}) catch |err| switch (err) {
        error.Canceled => return error.Canceled,
        else => return,
    };
    return ctx.fail(who ++ " should not have gotten a task, but answered query \"waiting\" with {}", .{answer});
}

/// Checks that every workflow task of the run was completed by build `expected` (empty for
/// an unversioned worker), as the `worker_version` of each WorkflowTaskCompleted event says:
/// a query that goes unanswered does not show which worker ran a task.
fn expectAllCompletedBy(ctx: *harness.Context, workflow_id: []const u8, run_id: []const u8, comptime expected: []const u8) !void {
    const recorded = try ctx.fetchHistory(workflow_id, run_id);
    var completed: usize = 0;
    for (recorded.events.items) |event| {
        if (event.event_type != .EVENT_TYPE_WORKFLOW_TASK_COMPLETED) continue;
        completed += 1;
        const attributes = switch (event.attributes orelse return ctx.fail("event {d}, a workflow task completion, has no attributes", .{event.event_id})) {
            .workflow_task_completed_event_attributes => |a| a,
            else => return ctx.fail("event {d} is a workflow task completion without its attributes", .{event.event_id}),
        };
        const actual = if (attributes.worker_version) |v| v.build_id else "";
        if (!std.mem.eql(u8, actual, expected))
            return ctx.fail("workflow task completed at event {d} by build \"{s}\"; expected build \"" ++ expected ++ "\"", .{ event.event_id, actual });
    }
    if (completed == 0) return ctx.fail("history has no completed workflow task", .{});
}
