//! Infrequent polling: each activity attempt polls the service once, and the activity's retry
//! policy does the waiting. With a backoff coefficient of 1 and the poll interval as the
//! initial interval, the activity is retried exactly every poll interval until the service
//! answers. Retries aren't recorded in the workflow's history, so this can poll for a long
//! time without growing it.

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;
const activity = temporal.activity;
const test_service = @import("test_service.zig");

pub const task_queue = "pollingInfrequentlySampleQueue";

/// Suits polls of a minute or slower.
pub const default_poll_interval: std.Io.Duration = .fromSeconds(60);

pub const PollingActivities = struct {
    test_service: *test_service.TestService,

    pub fn doPoll(a: *PollingActivities, ctx: *activity.Context) ![]const u8 {
        return a.test_service.getServiceResult() catch ctx.fail(test_service.service_down);
    }
};

pub const DoPoll = activity.Def("DoPoll", PollingActivities.doPoll);

fn infrequentPolling(ctx: *workflow.Context, poll_interval: std.Io.Duration) ![]const u8 {
    const ao: workflow.ActivityOptions = .{
        .start_to_close_timeout = .fromSeconds(2),
        .retry_policy = .{ .backoff_coefficient = 1, .initial_interval = poll_interval },
    };
    return ctx.executeActivity(DoPoll, ao, .{}).get() catch |err| {
        ctx.logger().err("Polling failed.", .{ .err = err });
        return err;
    };
}

/// Takes the poll interval, usually `default_poll_interval`.
pub const InfrequentPolling = workflow.Def("InfrequentPolling", infrequentPolling);
