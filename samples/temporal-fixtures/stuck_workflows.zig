//! Stuck workflows: a workflow whose activity fails on every attempt and is retried without
//! end, and, through the starter, a workflow on a task queue no worker polls. Both stay
//! running.

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;
const activity = temporal.activity;

pub const task_queue = "stuck-workflows";
/// No worker polls this task queue, so a workflow started on it never runs.
pub const no_worker_task_queue = "no-worker";

fn stuckWorkflowActivity(ctx: *activity.Context) !void {
    ctx.logger().info("Activity failed, will retry...", .{});
    return ctx.fail(.{ .message = "some retryable error", .type = "SomeType" });
}

pub const StuckWorkflowActivity = activity.Def("StuckWorkflowActivity", stuckWorkflowActivity);

fn stuckWorkflow(ctx: *workflow.Context) !void {
    const logger = ctx.logger();
    ctx.executeActivity(StuckWorkflowActivity, .{
        .start_to_close_timeout = .fromSeconds(24 * std.time.s_per_hour),
        .heartbeat_timeout = .fromSeconds(10),
        .retry_policy = .{
            .initial_interval = .fromSeconds(1),
            .backoff_coefficient = 2.0,
            .maximum_interval = .fromSeconds(std.time.s_per_min),
            // Unlimited.
            .maximum_attempts = 0,
        },
    }, .{}).get() catch |err| {
        logger.info("Workflow completed with error.", .{ .err = err });
        return err;
    };
    logger.info("Workflow completed.", .{});
}

pub const StuckWorkflow = workflow.Def("StuckWorkflow", stuckWorkflow);
