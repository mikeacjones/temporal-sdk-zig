const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");
const sercontext = harness.sercontext;
const workflow = temporal.workflow;

const Workflow = workflow.Def("Workflow", runWorkflow);

pub const feature = harness.define(.{
    .workflows = .{Workflow},
    .configureClient = sercontext.configureClient,
    .execute = execute,
});

fn runWorkflow(ctx: *workflow.Context, remaining: i32) ![]const u8 {
    if (remaining > 0) return ctx.continueAsNew(Workflow, .{}, .{remaining - 1});
    return "done";
}

fn execute(ctx: *harness.Context) !void {
    var run = try ctx.client.executeWorkflow(Workflow, ctx.startOptions(), .{1});
    defer run.deinit();
    // get replaces the handle's run ID as it follows the chain.
    const first_run_id = try ctx.arena.dupe(u8, run.run_id);
    const result = run.get(ctx.arena) catch |err| {
        try sercontext.checkMismatch(ctx, "workflow result", null);
        if (run.failure()) |failure| return ctx.fail("workflow failed: {s}", .{failure.message});
        return err;
    };
    try ctx.expectEqualStrings("done", result);
    try ctx.expect(!std.mem.eql(u8, first_run_id, run.run_id), "workflow {s} did not continue as new from run {s}", .{ run.id, first_run_id });

    const expected = try sercontext.workflowSignature(ctx.arena, ctx.namespace, run.id);
    const first = try ctx.fetchRawHistory(run.id, first_run_id);
    const continued = try sercontext.findAttributes(ctx, first, .workflow_execution_continued_as_new_event_attributes);
    try sercontext.expectSignature(ctx, "WorkflowExecutionContinuedAsNew input", expected, sercontext.firstSignature(continued.input));

    const last = try ctx.fetchRawHistory(run.id, run.run_id);
    const started = try sercontext.findAttributes(ctx, last, .workflow_execution_started_event_attributes);
    try sercontext.expectSignature(ctx, "last run's WorkflowExecutionStarted input", expected, sercontext.firstSignature(started.input));
    const completed = try sercontext.findAttributes(ctx, last, .workflow_execution_completed_event_attributes);
    try sercontext.expectSignature(ctx, "last run's WorkflowExecutionCompleted result", expected, sercontext.firstSignature(completed.result));
}
