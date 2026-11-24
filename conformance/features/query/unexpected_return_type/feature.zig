//! A query answered with a string and read by the caller as an integer fails to decode.
//!
//! In Zig the compile-time case cannot arise: a `workflow.QueryDef` names the result type
//! for both the handler and the caller, so a caller cannot read a typed query's answer as
//! another type. The run-time case that stands in for it is a query sent by name
//! (`Client.queryWorkflowByName`), whose `converter.EncodedValue` answer is decoded as
//! `i64` and must fail, as Go's `q.Get(&intResult)` does.
const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");
const workflow = temporal.workflow;

const Workflow = workflow.Def("Workflow", runWorkflow);
const TheQuery = workflow.QueryDef("theQuery", fn () []const u8);
const Finish = workflow.SignalDef("finish", fn () void);
const answer = "hi bob";

pub const feature = harness.define(.{
    .workflows = .{Workflow},
    .execute = execute,
});

const State = struct {
    finished: bool = false,

    fn theQuery(_: *const State) []const u8 {
        return answer;
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
    try ctx.setQueryHandler(TheQuery, @as(*const State, state), State.theQuery);
    try ctx.setSignalHandler(Finish, state, State.finish);
    try ctx.await(state, State.isFinished);
}

fn execute(ctx: *harness.Context) !void {
    var run = try ctx.client.executeWorkflow(Workflow, ctx.startOptions(), .{});
    defer run.deinit();

    const value = try ctx.client.queryWorkflowByName(TheQuery.query_name, ctx.arena, run.id, run.run_id, .{});
    try ctx.expect(value.hasValue(), "query \"theQuery\" returned no value; expected \"" ++ answer ++ "\"", .{});
    if (value.get(i64, ctx.arena)) |number| {
        return ctx.fail("query \"theQuery\" answer decoded as the integer {d}; expected decoding it as an integer to fail", .{number});
    } else |err| if (err == error.OutOfMemory) return err;
    // The answer itself is intact: read as the handler's type, it decodes.
    try ctx.expectEqualStrings(answer, try value.get([]const u8, ctx.arena));

    try ctx.client.signalWorkflow(Finish, run.id, run.run_id, .{});
    try run.get(ctx.arena);
}
