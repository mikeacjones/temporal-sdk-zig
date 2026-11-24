//! A query of a type the workflow has no handler for is refused: the client's call fails
//! with `error.QueryFailed`, as Go's QueryFailed service error. The workflow then finishes
//! on the "finish" signal.
const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");
const workflow = temporal.workflow;

const Workflow = workflow.Def("Workflow", runWorkflow);
const Finish = workflow.SignalDef("finish", fn () void);

pub const feature = harness.define(.{
    .workflows = .{Workflow},
    .execute = execute,
});

const State = struct {
    finished: bool = false,

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
    try ctx.setSignalHandler(Finish, state, State.finish);
    try ctx.await(state, State.isFinished);
}

fn execute(ctx: *harness.Context) !void {
    var run = try ctx.client.executeWorkflow(Workflow, ctx.startOptions(), .{});
    defer run.deinit();

    var message: []const u8 = "";
    if (ctx.client.queryWorkflowByNameWithOptions("nonexistent", ctx.arena, run.id, run.run_id, .{}, .{ .failure_message = &message })) |_| {
        return ctx.fail("query \"nonexistent\" succeeded; expected error.QueryFailed", .{});
    } else |err| try ctx.expect(err == error.QueryFailed, "query \"nonexistent\" returned error.{s}; expected error.QueryFailed", .{@errorName(err)});
    try ctx.expect(std.mem.indexOf(u8, message, "nonexistent") != null, "query \"nonexistent\" failed with message \"{s}\"; expected it to name the query type", .{message});

    try ctx.client.signalWorkflow(Finish, run.id, run.run_id, .{});
    try run.get(ctx.arena);
}
