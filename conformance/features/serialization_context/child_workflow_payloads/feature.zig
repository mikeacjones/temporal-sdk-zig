const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");
const sercontext = harness.sercontext;
const workflow = temporal.workflow;

const Workflow = workflow.Def("Workflow", runWorkflow);
const ChildWorkflow = workflow.Def("ChildWorkflow", runChildWorkflow);

const workflow_input = "hello";

pub const feature = harness.define(.{
    .workflows = .{ Workflow, ChildWorkflow },
    .configureClient = sercontext.configureClient,
    .execute = execute,
});

fn runWorkflow(ctx: *workflow.Context, input: []const u8) ![]const u8 {
    const child_id = try std.fmt.allocPrint(ctx.allocator(), "{s}_child", .{ctx.info().workflow_id});
    return ctx.executeChildWorkflow(ChildWorkflow, .{ .id = child_id, .run_timeout = .fromSeconds(60) }, .{input}).get();
}

fn runChildWorkflow(ctx: *workflow.Context, input: []const u8) ![]const u8 {
    return std.fmt.allocPrint(ctx.allocator(), "{s}|child", .{input});
}

fn execute(ctx: *harness.Context) !void {
    var run = try ctx.client.executeWorkflow(Workflow, ctx.startOptions(), .{workflow_input});
    defer run.deinit();
    const result = run.get(ctx.arena) catch |err| {
        try sercontext.checkMismatch(ctx, "workflow result", null);
        if (run.failure()) |failure| return ctx.fail("workflow failed: {s}", .{failure.message});
        return err;
    };
    try ctx.expectEqualStrings(workflow_input ++ "|child", result);

    const recorded = try ctx.fetchRawHistory(run.id, run.run_id);
    const child_id = try std.fmt.allocPrint(ctx.arena, "{s}_child", .{run.id});
    try sercontext.expectChildPayloads(ctx, run.id, recorded, child_id);
}
