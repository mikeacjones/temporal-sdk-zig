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

// No child ID: the SDK generates one.
fn runWorkflow(ctx: *workflow.Context, input: []const u8) ![]const u8 {
    return ctx.executeChildWorkflow(ChildWorkflow, .{ .run_timeout = .fromSeconds(60) }, .{input}).get();
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
    const child_started = try sercontext.findAttributes(ctx, recorded, .child_workflow_execution_started_event_attributes);
    const child_id = if (child_started.workflow_execution) |execution| execution.workflow_id else "";
    if (child_id.len == 0) return ctx.fail("ChildWorkflowExecutionStarted names no child workflow ID", .{});
    if (std.mem.eql(u8, child_id, run.id)) return ctx.fail("generated child workflow ID {s} is the parent's", .{child_id});
    try sercontext.expectChildPayloads(ctx, run.id, recorded, child_id);
}
