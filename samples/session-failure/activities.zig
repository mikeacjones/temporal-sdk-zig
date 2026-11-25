//! Activities of the session failure sample. Both run in one session, on the host that took
//! it: the first prepares the host, the second stands for long work on it. Neither holds a
//! resource, so a canceled attempt leaves nothing behind.

const std = @import("std");
const temporal = @import("temporal");
const activity = temporal.activity;

/// How long the long-running activity works and how often it heartbeats. Its state outlives
/// the worker.
pub const Activities = struct {
    /// How long `longRunningActivity` works before it returns.
    work_time: std.Io.Duration = .fromSeconds(5 * 60),
    /// How often `longRunningActivity` heartbeats while it works.
    heartbeat_interval: std.Io.Duration = .fromSeconds(20),

    /// Stands for preparing this host for the session's work.
    pub fn prepareWorkerActivity(a: *Activities, ctx: *activity.Context) !void {
        _ = a;
        ctx.logger().info("Preparing session worker", .{});
    }

    /// Works for `work_time`, heartbeating every `heartbeat_interval`. Returns
    /// `error.Canceled` as soon as the activity is canceled, for example when the workflow
    /// gives up on a session whose host went away.
    pub fn longRunningActivity(a: *Activities, ctx: *activity.Context) !void {
        ctx.logger().info("Started running long running activity.", .{});
        const io = ctx.io();
        const deadline = std.Io.Timestamp.now(io, .awake).addDuration(a.work_time);
        while (true) {
            const remaining = std.Io.Timestamp.now(io, .awake).durationTo(deadline);
            if (remaining.nanoseconds <= 0) return;
            const wait: std.Io.Duration = if (remaining.nanoseconds < a.heartbeat_interval.nanoseconds) remaining else a.heartbeat_interval;
            try io.sleep(wait, .awake);
            ctx.heartbeat({});
        }
    }
};

pub const PrepareWorkerActivity = activity.Def("PrepareWorkerActivity", Activities.prepareWorkerActivity);
pub const LongRunningActivity = activity.Def("LongRunningActivity", Activities.longRunningActivity);

/// The activities, for `Worker.registerActivitiesWith`.
pub const all = .{ PrepareWorkerActivity, LongRunningActivity };
