//! A worker gets a run's tasks once its version is added to the run's compatible set while
//! it polls, and stops getting them once a newer version becomes that set's default. The
//! queue has {1.0}; the feature's 1.0 worker counts one "add1" signal; a 1.1 worker then
//! must not get the next one until 1.1 is added compatible with 1.0, after which it counts
//! it; once 1.2 is added compatible with 1.1 a 1.1 worker must not get the next signal; a
//! 1.2 worker gets it, and the workflow is canceled to end it. The history must show each
//! signal's task completed by the build that should have it.
const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");
const Io = std.Io;
const workflow = temporal.workflow;

const Workflow = workflow.Def("Workflow", runWorkflow);
const Counter = workflow.QueryDef("counter", fn () i64);
const Add1 = workflow.SignalDef("add1", fn () void);

pub const feature = harness.define(.{
    .workflows = .{Workflow},
    .worker_options = temporal.worker.Options{ .build_id = "1.0", .use_build_id_versioning = true },
    .dev_server_args = &harness.build_ids.dev_server_args,
    .execute = execute,
});

const State = struct {
    counter: i64 = 0,

    fn count(state: *const State) i64 {
        return state.counter;
    }

    fn add1(state: *State) void {
        state.counter += 1;
    }

    fn never(_: *State) bool {
        return false;
    }
};

/// Counts "add1" signals until canceled.
fn runWorkflow(ctx: *workflow.Context) !void {
    const state = try ctx.allocator().create(State);
    state.* = .{};
    try ctx.setQueryHandler(Counter, @as(*const State, state), State.count);
    try ctx.setSignalHandler(Add1, state, State.add1);
    try ctx.await(state, State.never);
}

fn execute(ctx: *harness.Context) !void {
    try harness.build_ids.addNewDefault(ctx, "1.0");
    // A worker started after the version exists polls with it at once.
    try ctx.stopWorker();
    try ctx.startWorker();

    var run = try ctx.client.executeWorkflow(Workflow, ctx.startOptions(), .{});
    defer run.deinit();
    // Ends the run on a failed check before the cancel below does.
    defer ctx.client.terminateWorkflow(run.id, run.run_id, "feature cleanup") catch {};
    try ctx.client.signalWorkflow(Add1, run.id, run.run_id, .{});
    try waitForCounter(ctx, run.id, run.run_id, 1, "the 1.0 worker");

    try ctx.stopWorker();
    try harness.build_ids.resetStickyQueue(ctx, run.id, run.run_id);
    try ctx.startWorkerWith(.{ .build_id = "1.1", .use_build_id_versioning = true });
    try ctx.client.signalWorkflow(Add1, run.id, run.run_id, .{});
    try mustNotAnswer(ctx, run.id, run.run_id, "the 1.1 worker, before 1.1 was added to the sets,");

    try harness.build_ids.addCompatible(ctx, "1.1", "1.0", false);
    try ctx.stopWorker();
    try ctx.startWorkerWith(.{ .build_id = "1.1", .use_build_id_versioning = true });
    try waitForCounter(ctx, run.id, run.run_id, 2, "the 1.1 worker, once 1.1 was compatible with 1.0,");

    try harness.build_ids.addCompatible(ctx, "1.2", "1.1", false);
    // A poll the 1.1 worker started before 1.2 became the default still receives the run's
    // next task, from the sticky queue or the normal one, even 5 s after the change, so
    // the server evidently checks a poller's build ID only when the poll arrives. The 1.1
    // worker restarts, as the suite restarts it after adding 1.1, and its new polls see
    // 1.2; and the run goes back to the normal queue, which the new worker polls. The
    // suite's Go check, which only waits for the counter to equal 2, would not notice a 1.1
    // worker counting to 3; `expectCompletedBy` below does.
    try ctx.stopWorker();
    try harness.build_ids.resetStickyQueue(ctx, run.id, run.run_id);
    try ctx.startWorkerWith(.{ .build_id = "1.1", .use_build_id_versioning = true });
    try ctx.client.signalWorkflow(Add1, run.id, run.run_id, .{});
    try mustNotAnswer(ctx, run.id, run.run_id, "the 1.1 worker, once 1.2 was the set's default,");

    try ctx.stopWorker();
    try harness.build_ids.resetStickyQueue(ctx, run.id, run.run_id);
    try ctx.startWorkerWith(.{ .build_id = "1.2", .use_build_id_versioning = true });
    try waitForCounter(ctx, run.id, run.run_id, 3, "the 1.2 worker");

    // A query that goes unanswered does not show which worker ran a task, so the history
    // must: the task after each signal is completed by the build that owns the run then.
    try expectCompletedBy(ctx, run.id, run.run_id, &.{ "1.0", "1.1", "1.2" });

    try ctx.client.cancelWorkflow(run.id, run.run_id, "feature complete");
    if (run.get(ctx.arena)) |_| {
        return ctx.fail("workflow completed; expected it to end canceled", .{});
    } else |err| switch (err) {
        error.WorkflowCanceled => {},
        else => {
            const message = if (run.failure()) |failure| failure.message else "no failure details";
            return ctx.fail("workflow returned error.{t}: {s}; expected error.WorkflowCanceled", .{ err, message });
        },
    }
}

/// Queries "counter" every 200 ms until it answers `expected`, as the suite's
/// QueryUntilEventually does, for at most 5 s where the suite waits 2 s, which leaves room
/// for a worker restart on a loaded machine. Each query has a 1 s deadline.
fn waitForCounter(ctx: *harness.Context, workflow_id: []const u8, run_id: []const u8, expected: i64, comptime who: []const u8) !void {
    const io = ctx.io;
    const limit: Io.Duration = .fromSeconds(5);
    const deadline = Io.Clock.Timestamp.now(io, .boot).addDuration(.{ .raw = limit, .clock = .boot });
    var short = try ctx.client.withRpcOptions(.{ .timeout = .fromSeconds(1), .retry = false });
    var scratch: std.heap.ArenaAllocator = .init(ctx.gpa);
    defer scratch.deinit();
    var last: []const u8 = "no answer";
    var last_buf: [32]u8 = undefined;
    while (true) {
        _ = scratch.reset(.retain_capacity);
        if (short.queryWorkflow(Counter, scratch.allocator(), workflow_id, run_id, .{})) |counter| {
            if (counter == expected) return;
            last = std.fmt.bufPrint(&last_buf, "{d}", .{counter}) catch "?";
        } else |err| switch (err) {
            error.Canceled => return error.Canceled,
            else => last = @errorName(err),
        }
        if (deadline.compare(.lte, Io.Clock.Timestamp.now(io, .boot)))
            return ctx.fail(who ++ " should have counted to {d} within {f}; query \"counter\" last answered {s}", .{ expected, limit, last });
        try io.sleep(.fromMilliseconds(200), .boot);
    }
}

/// The suite's MustTimeoutQuery: a query with a 3 s deadline must not be answered, since no
/// worker polling may take the run's tasks. Any error is the expected outcome.
fn mustNotAnswer(ctx: *harness.Context, workflow_id: []const u8, run_id: []const u8, comptime who: []const u8) !void {
    var short = try ctx.client.withRpcOptions(.{ .timeout = .fromSeconds(3), .retry = false });
    const answer = short.queryWorkflow(Counter, ctx.arena, workflow_id, run_id, .{}) catch |err| switch (err) {
        error.Canceled => return error.Canceled,
        else => return,
    };
    return ctx.fail(who ++ " should not have gotten a task, but answered query \"counter\" with {d}", .{answer});
}

/// Checks that the first workflow task completed after the n-th "add1" signal was completed
/// by `build_ids[n - 1]`, as the `worker_version` of its WorkflowTaskCompleted event says.
fn expectCompletedBy(ctx: *harness.Context, workflow_id: []const u8, run_id: []const u8, build_ids: []const []const u8) !void {
    const recorded = try ctx.fetchHistory(workflow_id, run_id);
    var signals: usize = 0;
    var checked: usize = 0;
    for (recorded.events.items) |event| switch (event.event_type) {
        .EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED => signals += 1,
        .EVENT_TYPE_WORKFLOW_TASK_COMPLETED => {
            if (signals == 0 or signals > build_ids.len or checked == signals) continue;
            checked = signals;
            const expected = build_ids[signals - 1];
            const attributes = event.attributes orelse
                return ctx.fail("event {d}, the workflow task completed after signal {d}, has no attributes", .{ event.event_id, signals });
            const completed = switch (attributes) {
                .workflow_task_completed_event_attributes => |a| a,
                else => return ctx.fail("event {d} is a workflow task completion without its attributes", .{event.event_id}),
            };
            const actual = if (completed.worker_version) |v| v.build_id else "";
            if (!std.mem.eql(u8, actual, expected))
                return ctx.fail("the workflow task after add1 signal {d} was completed by build \"{s}\" (event {d}); expected build \"{s}\"", .{ signals, actual, event.event_id, expected });
        },
        else => {},
    };
    if (checked != build_ids.len)
        return ctx.fail("history has a completed workflow task after {d} of the {d} add1 signals", .{ checked, build_ids.len });
}
