//! Frequent polling: the activity polls the service itself, every second or faster, and
//! heartbeats on each iteration. The heartbeat timeout is much shorter than the
//! start-to-close timeout, so if the worker running the poll dies, the activity is retried
//! on another worker within the heartbeat timeout instead of after a day.

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;
const activity = temporal.activity;
const TestService = @import("test_service.zig").TestService;

pub const task_queue = "pollingFrequentlySampleQueue";

pub const PollingActivities = struct {
    test_service: *TestService,
    poll_interval: std.Io.Duration,

    /// Polls until the service answers. Service errors are hidden and polled again until the
    /// activity is canceled; a real poller would fail on errors that polling can't fix.
    pub fn doPoll(a: *PollingActivities, ctx: *activity.Context) ![]const u8 {
        while (true) {
            if (a.test_service.getServiceResult()) |result| return result else |_| {}
            ctx.heartbeat({});
            try ctx.io().sleep(a.poll_interval, .awake);
        }
    }
};

pub const DoPoll = activity.Def("DoPoll", PollingActivities.doPoll);

fn frequentPolling(ctx: *workflow.Context) ![]const u8 {
    const ao: workflow.ActivityOptions = .{
        .start_to_close_timeout = .fromSeconds(24 * std.time.s_per_hour),
        .heartbeat_timeout = .fromSeconds(30),
    };
    return ctx.executeActivity(DoPoll, ao, .{}).get() catch |err| {
        ctx.logger().err("Frequent polling activity failed.", .{ .err = err });
        return err;
    };
}

pub const FrequentPolling = workflow.Def("FrequentPolling", frequentPolling);
