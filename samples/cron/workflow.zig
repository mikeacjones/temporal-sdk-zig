//! Cron: a workflow started with a cron schedule. Each run hands its time to the next run as
//! its result, and passes the previous run's time and its own to an activity.

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;
const activity = temporal.activity;
const Time = temporal.converter.Time;

/// The workflow result, which the next cron run reads.
pub const CronResult = struct {
    run_time: Time,
};

fn cron(ctx: *workflow.Context) !CronResult {
    const this_run_time = Time.fromTimestamp(ctx.now());
    var buf: [Time.max_len]u8 = undefined;
    ctx.logger().info("Cron workflow started.", .{ .start_time = this_run_time.format(&buf) });

    const ao: workflow.ActivityOptions = .{ .start_to_close_timeout = .fromSeconds(10) };

    // Null on the first run.
    const last_run_time: ?Time = if (ctx.hasLastCompletionResult())
        if (ctx.lastCompletionResult(CronResult)) |last| last.run_time else |_| null
    else
        null;

    ctx.executeActivity(RunCronJob, ao, .{ last_run_time, this_run_time }).get() catch |err| {
        ctx.logger().err("Cron job failed.", .{ .err = err });
        return err;
    };

    return .{ .run_time = this_run_time };
}

pub const Cron = workflow.Def("Cron", cron);

/// The scheduled job: covers the time after `last_run_time` (exclusive) up to
/// `this_run_time` (inclusive).
fn runCronJob(ctx: *activity.Context, last_run_time: ?Time, this_run_time: Time) !void {
    var last: [Time.max_len]u8 = undefined;
    var this: [Time.max_len]u8 = undefined;
    ctx.logger().info("Cron job running.", .{
        .last_run_time = if (last_run_time) |t| t.format(&last) else "none",
        .this_run_time = this_run_time.format(&this),
    });
}

pub const RunCronJob = activity.Def("RunCronJob", runCronJob);
