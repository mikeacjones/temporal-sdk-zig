//! Port of samples-go/cron: a workflow started with a cron schedule. Each run passes the
//! previous run's time (its last completion result) and its own time to an activity.

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;
const activity = temporal.activity;
const Time = temporal.converter.Time;

/// The workflow result, which the next cron run reads. Shaped like the Go sample's JSON.
pub const CronResult = struct {
    RunTime: Time = .{},
};

fn sampleCronWorkflow(ctx: *workflow.Context) !CronResult {
    var buf: [Time.max_len]u8 = undefined;
    ctx.logger().info("Cron workflow started.", .{ .StartTime = Time.fromTimestamp(ctx.now()).format(&buf) });

    const ao: workflow.ActivityOptions = .{ .start_to_close_timeout = .fromSeconds(10) };

    var last_run_time: Time = .zero;
    if (ctx.hasLastCompletionResult()) {
        if (ctx.lastCompletionResult(CronResult)) |last_result| {
            last_run_time = last_result.RunTime;
        } else |_| {}
    }
    const this_run_time = Time.fromTimestamp(ctx.now());

    ctx.executeActivity(DoSomething, ao, .{ last_run_time, this_run_time }).get() catch |err| {
        ctx.logger().err("Cron job failed.", .{ .Error = err });
        return err;
    };

    return .{ .RunTime = this_run_time };
}

pub const SampleCronWorkflow = workflow.Def("SampleCronWorkflow", sampleCronWorkflow);

fn doSomething(ctx: *activity.Context, last_run_time: Time, this_run_time: Time) !void {
    var last: [Time.max_len]u8 = undefined;
    var this: [Time.max_len]u8 = undefined;
    ctx.logger().info("Cron job running.", .{ .lastRunTime_exclude = last_run_time.format(&last), .thisRunTime_include = this_run_time.format(&this) });
}

pub const DoSomething = activity.Def("DoSomething", doSomething);
