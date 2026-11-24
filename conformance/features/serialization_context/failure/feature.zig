const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");
const sercontext = harness.sercontext;
const activity = temporal.activity;
const workflow = temporal.workflow;

const Workflow = workflow.Def("Workflow", runWorkflow);
const Activity = activity.Def("Activity", runActivity);

const activity_error_message = "activity failed";
const workflow_error_message = "workflow failed";

pub const feature = harness.define(.{
    .workflows = .{Workflow},
    .activities = .{Activity},
    .configureClient = sercontext.configureClient,
    .execute = execute,
});

// The activity fails and then the workflow fails, so history records one failure converted
// with the activity's context and one with the workflow's.
fn runWorkflow(ctx: *workflow.Context) !void {
    ctx.executeActivity(Activity, .{
        .start_to_close_timeout = .fromSeconds(10),
        .retry_policy = .{ .maximum_attempts = 1 },
    }, .{}).get() catch |err| switch (err) {
        error.ActivityFailed => return ctx.fail(.{ .message = workflow_error_message, .type = "WorkflowError" }),
        else => |e| return e,
    };
    return ctx.fail(.{ .message = "expected the activity to fail", .type = "UnexpectedSuccess" });
}

fn runActivity(ctx: *activity.Context) !void {
    return ctx.fail(.{ .message = activity_error_message, .type = "ActivityError", .non_retryable = true });
}

fn execute(ctx: *harness.Context) !void {
    var run = try ctx.client.executeWorkflow(Workflow, ctx.startOptions(), .{});
    defer run.deinit();
    if (run.get(ctx.arena)) |_| {
        return ctx.fail("workflow completed, expected it to fail with \"{s}\"", .{workflow_error_message});
    } else |err| switch (err) {
        error.WorkflowFailed => {},
        else => {
            try sercontext.checkMismatch(ctx, "workflow result", null);
            return err;
        },
    }
    const failure = run.failure() orelse return ctx.fail("workflow failure has no details", .{});
    try ctx.expect(
        std.mem.indexOf(u8, failure.message, workflow_error_message) != null,
        "workflow failure message is \"{s}\", expected it to contain \"{s}\"",
        .{ failure.message, workflow_error_message },
    );

    const recorded = try ctx.fetchRawHistory(run.id, run.run_id);
    const activity_failed = try sercontext.findAttributes(ctx, recorded, .activity_task_failed_event_attributes);
    const activity_source = if (activity_failed.failure) |f| f.source else null;
    try sercontext.expectSignature(ctx, "ActivityTaskFailed failure source", try sercontext.scheduledActivitySignature(ctx, recorded, run.id), activity_source);
    const workflow_failed = try sercontext.findAttributes(ctx, recorded, .workflow_execution_failed_event_attributes);
    const workflow_source = if (workflow_failed.failure) |f| f.source else null;
    try sercontext.expectSignature(ctx, "WorkflowExecutionFailed failure source", try sercontext.workflowSignature(ctx.arena, ctx.namespace, run.id), workflow_source);
}
