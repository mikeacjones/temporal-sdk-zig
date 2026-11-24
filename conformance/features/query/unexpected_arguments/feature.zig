//! A query whose arguments don't match its handler's is refused as far as the SDK can tell:
//! one argument of the wrong type and no argument where one is expected both fail the query.
//! An extra argument is dropped and the query answered, as Go, Java, TypeScript and PHP do:
//! this SDK decodes the arguments of every handler that way (`converter.EncodedValues.decode`).
//! The handler takes one integer and answers "got <n>".
//!
//! In Zig the compile-time case cannot arise: a `workflow.QueryDef` gives the caller and the
//! handler the same argument types, so `Client.queryWorkflow` won't compile with other
//! arguments. The run-time case that stands in for it is the query sent by name
//! (`Client.queryWorkflowByName`) with other arguments, as Go's and Python's dynamic calls do.
const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");
const workflow = temporal.workflow;

const Workflow = workflow.Def("Workflow", runWorkflow);
const TheQuery = workflow.QueryDef("theQuery", fn (i64) anyerror![]const u8);
const Finish = workflow.SignalDef("finish", fn () void);

pub const feature = harness.define(.{
    .workflows = .{Workflow},
    .execute = execute,
});

const State = struct {
    ctx: *workflow.Context,
    finished: bool = false,

    fn theQuery(state: *const State, a: i64) ![]const u8 {
        return std.fmt.allocPrint(state.ctx.allocator(), "got {d}", .{a});
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
    state.* = .{ .ctx = ctx };
    try ctx.setQueryHandler(TheQuery, @as(*const State, state), State.theQuery);
    try ctx.setSignalHandler(Finish, state, State.finish);
    try ctx.await(state, State.isFinished);
}

fn execute(ctx: *harness.Context) !void {
    var run = try ctx.client.executeWorkflow(Workflow, ctx.startOptions(), .{});
    defer run.deinit();

    const answered = try ctx.client.queryWorkflow(TheQuery, ctx.arena, run.id, run.run_id, .{123});
    try ctx.expectEqualStrings("got 123", answered);

    try expectRefused(ctx, run.id, run.run_id, "one argument of the wrong type", .{true});
    const extra = try ctx.client.queryWorkflowByName(TheQuery.query_name, ctx.arena, run.id, run.run_id, .{ @as(i64, 123), true });
    try ctx.expectEqualStrings("got 123", try extra.get([]const u8, ctx.arena));
    try expectRefused(ctx, run.id, run.run_id, "no argument", .{});

    try ctx.client.signalWorkflow(Finish, run.id, run.run_id, .{});
    try run.get(ctx.arena);
}

/// Sends "theQuery" by name with `args` and expects the workflow to refuse it.
fn expectRefused(ctx: *harness.Context, workflow_id: []const u8, run_id: []const u8, comptime case: []const u8, args: anytype) !void {
    var message: []const u8 = "";
    const value = ctx.client.queryWorkflowByNameWithOptions(TheQuery.query_name, ctx.arena, workflow_id, run_id, args, .{ .failure_message = &message }) catch |err| {
        return ctx.expect(err == error.QueryFailed, "query \"theQuery\" with " ++ case ++ " returned error.{s}; expected error.QueryFailed", .{@errorName(err)});
    };
    const answer = value.get([]const u8, ctx.arena) catch "<undecodable>";
    return ctx.fail("query \"theQuery\" with " ++ case ++ " was answered \"{s}\"; expected error.QueryFailed", .{answer});
}
