//! With no worker polling, a query times out at the deadline the client gave it. The driver
//! stops the worker, queries with a 1 s deadline and expects the call to fail when it
//! passes with error.DeadlineExceeded, as the suite's feature.go checks, then starts a new
//! worker and finishes the workflow with the "finish" signal.
//!
//! The client reports a deadline as error.RpcFailed, like any other gRPC failure, so this
//! feature fails until bead temporal-sdk-zig-ad0 ("Client: report a query's deadline as
//! error.DeadlineExceeded") is done. Every path out of `execute` terminates the run, because
//! the worker may be stopped when the check fails and no signal could finish it.
const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");

const gap_bead = "temporal-sdk-zig-ad0";
const workflow = temporal.workflow;

const Workflow = workflow.Def("Workflow", runWorkflow);
const SomeQuery = workflow.QueryDef("somequery", fn () bool);
const Finish = workflow.SignalDef("finish", fn () void);

pub const feature = harness.define(.{
    .workflows = .{Workflow},
    .execute = execute,
});

const State = struct {
    finished: bool = false,

    fn someQuery(_: *const State) bool {
        return true;
    }

    fn finish(state: *State) void {
        state.finished = true;
    }

    fn isFinished(state: *State) bool {
        return state.finished;
    }
};

fn runWorkflow(ctx: *workflow.Context) !void {
    const state = try ctx.allocator().create(State);
    state.* = .{};
    try ctx.setQueryHandler(SomeQuery, @as(*const State, state), State.someQuery);
    try ctx.setSignalHandler(Finish, state, State.finish);
    try ctx.await(state, State.isFinished);
}

fn execute(ctx: *harness.Context) !void {
    var run = try ctx.client.executeWorkflow(Workflow, ctx.startOptions(), .{});
    defer run.deinit();
    // Runs before run.deinit. The run has normally completed by now and the terminate fails
    // harmlessly; after a failed expectation the worker is stopped, so only a terminate
    // closes the run.
    defer ctx.client.terminateWorkflow(run.id, run.run_id, "feature cleanup") catch {};
    // The worker must have run the first workflow task, or the server would hold the query
    // for that task instead of sending it to a worker.
    try ctx.waitForEvent(run.id, run.run_id, .EVENT_TYPE_WORKFLOW_TASK_COMPLETED, .fromSeconds(10));

    try ctx.stopWorker();

    var short = try ctx.client.withRpcOptions(.{ .timeout = .fromSeconds(1) });
    var message: []const u8 = "";
    const result = short.queryWorkflowWithOptions(SomeQuery, ctx.arena, run.id, run.run_id, .{}, .{ .failure_message = &message });
    if (result) |answer| {
        return ctx.fail("query \"somequery\" with no worker answered {}; expected error.DeadlineExceeded at its 1s deadline (gap: " ++ gap_bead ++ ")", .{answer});
    } else |err| {
        // QueryError has no DeadlineExceeded member yet, so compare by name.
        try ctx.expect(std.mem.eql(u8, @errorName(err), "DeadlineExceeded"), "query \"somequery\" with no worker returned error.{s} (\"{s}\"); expected error.DeadlineExceeded at its 1s deadline (gap: " ++ gap_bead ++ ")", .{ @errorName(err), message });
    }

    try ctx.startWorker();
    try ctx.client.signalWorkflow(Finish, run.id, run.run_id, .{});
    try run.get(ctx.arena);
}
