const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");
const workflow = temporal.workflow;

const Workflow = workflow.Def("Workflow", runWorkflow);
const ChildWorkflow = workflow.Def("ChildWorkflow", runChildWorkflow);
const child_input = "test";

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
    }, .{child_input});
    return child.get();
}

fn runChildWorkflow(_: *workflow.Context, input: []const u8) ![]const u8 {
    return input;
}

fn execute(ctx: *harness.Context) !void {
    var run = try ctx.client.executeWorkflow(Workflow, ctx.startOptions(), .{});
    defer run.deinit();

    try ctx.expectEqualStrings(child_input, try run.get(ctx.arena));
}
