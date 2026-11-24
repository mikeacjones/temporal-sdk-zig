const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");
const sercontext = harness.sercontext;
const activity = temporal.activity;
const workflow = temporal.workflow;

const Workflow = workflow.Def("Workflow", runWorkflow);
const Activity = activity.Def("Activity", runActivity);

const workflow_input = "hello";
const heartbeat_data = "beat";

pub const feature = harness.define(.{
    .workflows = .{Workflow},
    .activities = .{Activity},
    .configureClient = sercontext.configureClient,
    .execute = execute,
});

fn runWorkflow(ctx: *workflow.Context, input: []const u8) ![]const u8 {
    return ctx.executeActivity(Activity, .{
        .start_to_close_timeout = .fromSeconds(10),
        .heartbeat_timeout = .fromSeconds(5),
        .retry_policy = .{ .initial_interval = .fromMilliseconds(1), .maximum_attempts = 2 },
    }, .{input}).get();
}

// The first attempt heartbeats and fails, so the second has to decode the heartbeat details
// the first recorded.
fn runActivity(ctx: *activity.Context, input: []const u8) ![]const u8 {
    if (ctx.info().attempt == 1) {
        ctx.heartbeat(heartbeat_data);
        return ctx.fail(.{ .message = "retrying to read back heartbeat details", .type = "Retry" });
    }
    const details = try ctx.heartbeatDetails([]const u8);
    return std.fmt.allocPrint(ctx.allocator(), "{s}|{s}", .{ input, details });
}

fn execute(ctx: *harness.Context) !void {
    var run = try ctx.client.executeWorkflow(Workflow, ctx.startOptions(), .{workflow_input});
    defer run.deinit();
    const result = run.get(ctx.arena) catch |err| {
        try sercontext.checkMismatch(ctx, "workflow result", null);
        if (run.failure()) |failure| return ctx.fail("workflow failed: {s}", .{failure.message});
        return err;
    };
    try ctx.expectEqualStrings(workflow_input ++ "|" ++ heartbeat_data, result);

    const recorded = try ctx.fetchRawHistory(run.id, run.run_id);
    const expected = try sercontext.scheduledActivitySignature(ctx, recorded, run.id);
    const scheduled = try sercontext.findAttributes(ctx, recorded, .activity_task_scheduled_event_attributes);
    try sercontext.expectSignature(ctx, "ActivityTaskScheduled input", expected, sercontext.firstSignature(scheduled.input));
    const completed = try sercontext.findAttributes(ctx, recorded, .activity_task_completed_event_attributes);
    try sercontext.expectSignature(ctx, "ActivityTaskCompleted result", expected, sercontext.firstSignature(completed.result));
    const workflow_completed = try sercontext.findAttributes(ctx, recorded, .workflow_execution_completed_event_attributes);
    try sercontext.expectSignature(
        ctx,
        "WorkflowExecutionCompleted result",
        try sercontext.workflowSignature(ctx.arena, ctx.namespace, run.id),
        sercontext.firstSignature(workflow_completed.result),
    );
}
