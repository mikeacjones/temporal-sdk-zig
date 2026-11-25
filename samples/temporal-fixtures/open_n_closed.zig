//! Open and closed: a workflow whose activity either returns at once, so the run closes, or
//! first sleeps ten minutes, so the run stays open.

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;
const activity = temporal.activity;

pub const task_queue = "open-n-closed";

/// Sleeps ten minutes first when `keep_open` is set. It does not heartbeat, so the only
/// cancel that reaches it is the worker's stop. That must not close the run: the attempt is
/// left open on the server, where it ends at its start-to-close timeout or with its run, as
/// if the worker's process had exited. Any other cancel ends the attempt canceled.
fn activityFn(ctx: *activity.Context, keep_open: bool) ![]const u8 {
    ctx.logger().info("OpenNClosedActivity", .{ .keep_open = keep_open });
    if (keep_open) ctx.io().sleep(.fromSeconds(10 * std.time.s_per_min), .awake) catch |err| {
        if (ctx.cancelReason() == .worker_shutdown) return ctx.completeAsync();
        return err;
    };
    return "Hello!";
}

pub const Activity = activity.Def("Activity", activityFn);

fn openNClosedWorkflow(ctx: *workflow.Context, keep_open: bool) ![]const u8 {
    const logger = ctx.logger();
    logger.info("OpenNClosedWorkflow workflow started", .{ .keep_open = keep_open });

    const result = ctx.executeActivity(Activity, .{
        .start_to_close_timeout = .fromSeconds(std.time.s_per_hour),
        .retry_policy = .{ .maximum_attempts = 1 },
    }, .{keep_open}).get() catch |err| {
        logger.err("Activity failed.", .{ .err = err });
        return err;
    };

    logger.info("OpenNClosedWorkflow workflow completed.", .{ .result = result });
    return result;
}

pub const OpenNClosedWorkflow = workflow.Def("OpenNClosedWorkflow", openNClosedWorkflow);
