//! Cancellation: a workflow that runs a long, heartbeating activity until the workflow is
//! canceled. It waits for the activity to stop, runs a cleanup activity, and then ends
//! canceled.

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;
const activity = temporal.activity;

fn yourWorkflow(ctx: *workflow.Context) !void {
    const logger = ctx.logger();
    logger.info("cancel workflow started", .{});

    const to_be_canceled = ctx.executeActivity(ActivityToBeCanceled, .{
        .start_to_close_timeout = .fromSeconds(30 * std.time.s_per_min),
        .heartbeat_timeout = .fromSeconds(5),
        // On cancel, `get` waits until the activity has stopped before it returns.
        .cancellation_type = .wait_cancellation_completed,
    }, .{});
    to_be_canceled.get() catch |err| switch (err) {
        error.Canceled => {
            // `get` consumed the workflow's cancel, so the cleanup activity's wait runs
            // normally rather than being canceled too.
            ctx.executeActivity(CleanupActivity, .{ .start_to_close_timeout = .fromSeconds(2) }, .{}).get() catch |cleanup_err| {
                logger.err("CleanupActivity failed", .{ .err = cleanup_err });
            };
            return error.Canceled;
        },
        else => return err,
    };

    logger.info("Workflow Execution complete.", .{});
}

pub const YourWorkflow = workflow.Def("YourWorkflow", yourWorkflow);

/// Heartbeats every second until it is canceled; it never finishes on its own.
fn activityToBeCanceled(ctx: *activity.Context) !void {
    const logger = ctx.logger();
    logger.info("activity started, to cancel the Workflow Execution, use 'temporal workflow cancel -w <WorkflowID>'", .{});
    while (true) {
        ctx.io().sleep(.fromSeconds(1), .awake) catch |err| {
            const reason = if (ctx.cancelReason()) |r| @tagName(r) else "unknown";
            logger.info("activity canceled", .{ .reason = reason });
            return err;
        };
        logger.info("heartbeating...", .{});
        ctx.heartbeat({});
    }
}

pub const ActivityToBeCanceled = activity.Def("ActivityToBeCanceled", activityToBeCanceled);

fn cleanupActivity(ctx: *activity.Context) !void {
    ctx.logger().info("Cleanup Activity started", .{});
}

pub const CleanupActivity = activity.Def("CleanupActivity", cleanupActivity);
