//! With no worker polling, a query times out at the deadline the client gave it. The driver
//! stops the worker, queries with a 1 s deadline and expects the call to fail when it
//! passes, then starts a new worker and finishes the workflow with the "finish" signal.
const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");

const Io = std.Io;
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
    // The worker must have run the first workflow task, or the server would hold the query
    // for that task instead of sending it to a worker.
    try ctx.waitForEvent(run.id, run.run_id, .EVENT_TYPE_WORKFLOW_TASK_COMPLETED, .fromSeconds(10));

    try ctx.stopWorker();

    const timeout: Io.Duration = .fromSeconds(1);
    const latest_failure: Io.Duration = .fromSeconds(5);
    var short = try ctx.client.withRpcOptions(.{ .timeout = timeout });
    var message: []const u8 = "";
    const started = Io.Timestamp.now(ctx.io, .awake);
    const result = short.queryWorkflowWithOptions(SomeQuery, ctx.arena, run.id, run.run_id, .{}, .{ .failure_message = &message });
    const elapsed = started.durationTo(Io.Timestamp.now(ctx.io, .awake));
    if (result) |answer| {
        return ctx.fail("query \"somequery\" with no worker answered {}; expected it to fail at its 1s deadline", .{answer});
    } else |err| {
        try ctx.expect(err == error.RpcFailed, "query \"somequery\" with no worker returned error.{s} (\"{s}\"); expected error.RpcFailed at its 1s deadline", .{ @errorName(err), message });
    }
    // The client reports the deadline as error.RpcFailed, as it does any other gRPC failure;
    // when it failed tells the deadline apart, as Client.callService's rule for CANCELLED does.
    try ctx.expect(elapsed.nanoseconds >= timeout.nanoseconds and elapsed.nanoseconds < latest_failure.nanoseconds, "query \"somequery\" with no worker failed after {d} ms (\"{s}\"); expected it to fail at its 1s deadline", .{ elapsed.toMilliseconds(), message });

    try ctx.startWorker();
    try ctx.client.signalWorkflow(Finish, run.id, run.run_id, .{});
    try run.get(ctx.arena);
}
