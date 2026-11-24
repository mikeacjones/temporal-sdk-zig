//! A workflow starts a child that waits for a signal, signals it through the child's future,
//! and returns the child's result: the signal's argument.
const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");
const workflow = temporal.workflow;

const Workflow = workflow.Def("Workflow", runWorkflow);
const ChildWorkflow = workflow.Def("ChildWorkflow", runChildWorkflow);
const UnblockSignal = workflow.SignalDef("unblock-signal", fn ([]const u8) void);
const unblock_message = "unblock";

pub const feature = harness.define(.{
    .workflows = .{ Workflow, ChildWorkflow },
    .execute = execute,
});

fn runWorkflow(ctx: *workflow.Context) ![]const u8 {
    const child_id = try std.fmt.allocPrint(ctx.allocator(), "{s}-child", .{ctx.info().workflow_id});
    const child = ctx.executeChildWorkflow(ChildWorkflow, .{
        .id = child_id,
        .execution_timeout = .fromSeconds(600),
        .task_timeout = .fromSeconds(60),
    }, .{});
    const delivered = try child.signal(UnblockSignal, .{unblock_message});
    try delivered.get();
    return child.get();
}

fn runChildWorkflow(ctx: *workflow.Context) ![]const u8 {
    const channel = try ctx.signalChannel(UnblockSignal);
    return channel.receive();
}

fn execute(ctx: *harness.Context) !void {
    var run = try ctx.client.executeWorkflow(Workflow, ctx.startOptions(), .{});
    defer run.deinit();

    try ctx.expectEqualStrings(unblock_message, try run.get(ctx.arena));
}
