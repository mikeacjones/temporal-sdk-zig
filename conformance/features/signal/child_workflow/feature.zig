//! A workflow starts a child with an argument, signals it through the child's future, and
//! returns the child's result, which joins the argument and the signal's value.
const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");
const workflow = temporal.workflow;

const Workflow = workflow.Def("Workflow", runWorkflow);
const ChildWorkflow = workflow.Def("ChildWorkflow", runChildWorkflow);
const MySignal = workflow.SignalDef("my_signal", fn ([]const u8) void);
const child_argument = "child-wf-arg";
const signal_argument = "signal-arg";

pub const feature = harness.define(.{
    .workflows = .{ Workflow, ChildWorkflow },
    .execute = execute,
});

fn runWorkflow(ctx: *workflow.Context) ![]const u8 {
    const child_id = try std.fmt.allocPrint(ctx.allocator(), "{s}-child", .{ctx.info().workflow_id});
    const child = ctx.executeChildWorkflow(ChildWorkflow, .{ .id = child_id }, .{child_argument});
    const delivered = try child.signal(MySignal, .{signal_argument});
    try delivered.get();
    return child.get();
}

const ChildState = struct {
    value: []const u8 = "",

    fn receive(state: *ChildState, value: []const u8) void {
        state.value = value;
    }

    fn received(state: *ChildState) bool {
        return state.value.len != 0;
    }
};

fn runChildWorkflow(ctx: *workflow.Context, input: []const u8) ![]const u8 {
    const state = try ctx.allocator().create(ChildState);
    state.* = .{};
    try ctx.setSignalHandler(MySignal, state, ChildState.receive);
    try ctx.await(state, ChildState.received);
    return std.fmt.allocPrint(ctx.allocator(), "{s} {s}", .{ input, state.value });
}

fn execute(ctx: *harness.Context) !void {
    var run = try ctx.client.executeWorkflow(Workflow, ctx.startOptions(), .{});
    defer run.deinit();
    try ctx.expectEqualStrings(child_argument ++ " " ++ signal_argument, try run.get(ctx.arena));
}
