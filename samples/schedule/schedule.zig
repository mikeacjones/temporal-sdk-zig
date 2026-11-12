//! Schedule: a workflow that a schedule starts. It reads which schedule started it, and the
//! time the run was scheduled for, from the search attributes the server sets on every
//! scheduled run, and hands both to an activity.

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;
const activity = temporal.activity;
const schedule = temporal.schedule;
const Time = temporal.converter.Time;

fn sampleScheduleWorkflow(ctx: *workflow.Context) !void {
    const logger = ctx.logger();
    var now: [Time.max_len]u8 = undefined;
    logger.info("Schedule workflow started.", .{ .start_time = Time.fromTimestamp(ctx.now()).format(&now) });

    const ao: workflow.ActivityOptions = .{ .start_to_close_timeout = .fromSeconds(10) };

    // Runs a schedule starts carry these search attributes; a run started any other way
    // has neither.
    const sa = ctx.searchAttributes();
    const scheduled_by_id = try sa.get(ctx.allocator(), schedule.scheduled_by_id) orelse
        return ctx.fail(.{ .message = "expected the TemporalScheduledById search attribute: was this run started by a schedule?" });
    const start_time = try sa.get(ctx.allocator(), schedule.scheduled_start_time) orelse
        return ctx.fail(.{ .message = "expected the TemporalScheduledStartTime search attribute: was this run started by a schedule?" });

    ctx.executeActivity(DoSomething, ao, .{ scheduled_by_id, start_time }).get() catch |err| {
        // Logged at warn: an error-level log fails the test that exercises this path.
        logger.warn("schedule workflow failed.", .{ .err = err });
        return err;
    };
}

pub const SampleScheduleWorkflow = workflow.Def("SampleScheduleWorkflow", sampleScheduleWorkflow);

/// The scheduled job. Query a database, call an external API, or take any other
/// non-deterministic action here.
fn doSomething(ctx: *activity.Context, schedule_by_id: []const u8, start_time: Time) !void {
    var buf: [Time.max_len]u8 = undefined;
    ctx.logger().info("Schedule job running.", .{ .schedule_by_id = schedule_by_id, .start_time = start_time.format(&buf) });
}

pub const DoSomething = activity.Def("DoSomething", doSomething);
