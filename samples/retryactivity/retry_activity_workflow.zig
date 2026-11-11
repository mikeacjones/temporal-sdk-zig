//! Retry activity: a workflow runs an unreliable activity with a retry policy. The activity heartbeats its progress, so a retry resumes from the last reported
//! progress instead of starting over.

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;
const activity = temporal.activity;

fn retryActivity(ctx: *workflow.Context) !void {
    const ao: workflow.ActivityOptions = .{
        .start_to_close_timeout = .fromSeconds(2 * 60),
        .heartbeat_timeout = .fromSeconds(10),
        .retry_policy = .{
            .initial_interval = .fromSeconds(1),
            .backoff_coefficient = 2.0,
            .maximum_interval = .fromSeconds(60),
            .maximum_attempts = 5,
        },
    };

    ctx.executeActivity(ProcessBatch, ao, .{ 0, 20, 1000 }).get() catch |err| {
        ctx.logger().info("Workflow completed with error.", .{ .err = err });
        return err;
    };
    ctx.logger().info("Workflow completed.", .{});
}

pub const RetryActivity = workflow.Def("RetryActivity", retryActivity);

/// Processes tasks first_task_id..first_task_id+batch_size, failing (to be retried) after each
/// third of the batch. Each task takes `task_ms` milliseconds.
pub fn processBatch(ctx: *activity.Context, first_task_id: i64, batch_size: i64, task_ms: i64) !void {
    const logger = ctx.logger();

    var i = first_task_id;
    if (ctx.hasHeartbeatDetails()) {
        if (ctx.heartbeatDetails(i64)) |completed_idx| {
            i = completed_idx + 1;
            logger.info("Resuming from failed attempt", .{ .reported_progress = completed_idx });
        } else |_| {}
    }

    var task_processed_in_this_attempt: i64 = 0; // used to determine when to fail (simulate failure)
    while (i < first_task_id + batch_size) : (i += 1) {
        logger.info("processing task", .{ .task_id = i });
        try ctx.io().sleep(.fromMilliseconds(task_ms), .awake); // simulate time spent on processing each task
        ctx.heartbeat(i);
        task_processed_in_this_attempt += 1;

        if (task_processed_in_this_attempt >= @divTrunc(batch_size, 3) and i < first_task_id + batch_size - 1) {
            logger.info("Activity failed, will retry...", .{});
            return ctx.fail(.{ .message = "simulated failure", .type = "SimulatedFailure" });
        }
    }

    logger.info("Activity succeeded.", .{});
}

pub const ProcessBatch = activity.Def("ProcessBatch", processBatch);
