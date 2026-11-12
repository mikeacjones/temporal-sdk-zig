//! Metrics: a workflow that sleeps, then runs an activity that records its own metrics (how
//! long it waited to start, how often it started, succeeded, or failed, and how long it ran)
//! through `ctx.metrics()`. The worker's client exports them, with sdk-core's own metrics, on
//! a Prometheus endpoint.

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;
const activity = temporal.activity;
const metrics = temporal.metrics;

pub const task_queue = "metrics";

pub const activity_latency = "activity_latency";
pub const schedule_to_start_latency = "schedule_to_start_latency";
pub const activity_started_count = "activity_started";
pub const activity_failed_count = "activity_failed";
pub const activity_success_count = "activity_succeeded";

fn metricsWorkflow(ctx: *workflow.Context) !void {
    const logger = ctx.logger();
    logger.info("Metrics workflow started.", .{});

    const scheduled_time_ns: i64 = @intCast(ctx.now().nanoseconds);
    try ctx.io().sleep(.fromMilliseconds(500), .awake);
    ctx.executeActivity(Activity, .{ .start_to_close_timeout = .fromSeconds(10) }, .{scheduled_time_ns}).get() catch |err| {
        logger.err("Activity failed.", .{ .err = err });
        return err;
    };

    logger.info("Metrics workflow completed.", .{});
}

pub const Workflow = workflow.Def("Workflow", metricsWorkflow);

fn metricsActivity(ctx: *activity.Context, scheduled_time_ns: i64) !void {
    const io = ctx.io();
    const handler = try recordActivityStart(io, ctx.metrics(), ctx.info().activity_type, scheduled_time_ns);
    defer handler.deinit();
    const start_time: std.Io.Timestamp = .now(io, .awake);
    errdefer recordActivityEnd(io, handler, start_time, false);

    try io.sleep(.fromSeconds(1), .awake);
    ctx.logger().info("Metrics reported.", .{});
    recordActivityEnd(io, handler, start_time, true);
}

pub const Activity = activity.Def("Activity", metricsActivity);

/// Records how long the activity waited since `scheduled_time_ns` (workflow time, before its
/// sleep) and counts the start. Returns a handler that adds the `operation` attribute to the
/// activity's other metrics; the caller frees it with `deinit`.
fn recordActivityStart(io: std.Io, base: metrics.Handler, activity_type: []const u8, scheduled_time_ns: i64) !metrics.Handler {
    const handler = try base.with(.{ .operation = activity_type });
    errdefer handler.deinit();
    const scheduled: std.Io.Timestamp = .{ .nanoseconds = scheduled_time_ns };
    const latency = try handler.histogramDuration(schedule_to_start_latency, .{});
    defer latency.deinit();
    latency.record(scheduled.durationTo(.now(io, .real)), .{});
    const started = try handler.counter(activity_started_count, .{});
    defer started.deinit();
    started.add(1, .{});
    return handler;
}

/// Records the activity's run time and counts its outcome. Metrics it can't create are
/// skipped: the activity's outcome doesn't depend on them.
fn recordActivityEnd(io: std.Io, handler: metrics.Handler, start_time: std.Io.Timestamp, succeeded: bool) void {
    if (handler.histogramDuration(activity_latency, .{})) |latency| {
        defer latency.deinit();
        latency.record(start_time.durationTo(.now(io, .awake)), .{});
    } else |_| {}
    if (handler.counter(if (succeeded) activity_success_count else activity_failed_count, .{})) |outcome| {
        defer outcome.deinit();
        outcome.add(1, .{});
    } else |_| {}
}
