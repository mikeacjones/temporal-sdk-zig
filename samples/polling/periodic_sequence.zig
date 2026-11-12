//! Periodic sequence polling: for polls that run a sequence of activities, or whose activity
//! arguments change between polls. A child workflow polls a fixed number of times, sleeping
//! between polls, and then continues as new to keep its history short. The parent waits on
//! the child and sees only the result of its last run.

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;
const activity = temporal.activity;
const test_service = @import("test_service.zig");

pub const task_queue = "pollingPeriodicSequenceSampleQueue";

/// Polls in each run of the child workflow before it continues as new.
pub const polls_per_run = 10;

pub const PollingActivities = struct {
    test_service: *test_service.TestService,

    pub fn doPoll(a: *PollingActivities, ctx: *activity.Context) ![]const u8 {
        return a.test_service.getServiceResult() catch ctx.fail(test_service.service_down);
    }
};

pub const DoPoll = activity.Def("DoPoll", PollingActivities.doPoll);

pub const ChildWorkflowParams = struct {
    polling_interval: std.Io.Duration,
};

fn pollingChildWorkflow(ctx: *workflow.Context, params: ChildWorkflowParams) ![]const u8 {
    const logger = ctx.logger();
    logger.info("Starting child workflow with params", .{ .polling_interval_ms = params.polling_interval.toMilliseconds() });
    const ao: workflow.ActivityOptions = .{
        .start_to_close_timeout = .fromSeconds(4),
        .retry_policy = .{ .maximum_attempts = 1 },
    };

    for (0..polls_per_run) |_| {
        // A real poll would run a sequence of activities here; this sample runs one.
        if (ctx.executeActivity(DoPoll, ao, .{}).get()) |result| return result else |err| {
            // Logged at warn, not error: the service being down is expected while polling.
            logger.warn("Error in activity, sleeping and retrying", .{ .err = err });
        }
        try ctx.io().sleep(params.polling_interval, .awake);
    }
    return ctx.continueAsNew(PollingChildWorkflow, .{}, .{params});
}

pub const PollingChildWorkflow = workflow.Def("PollingChildWorkflow", pollingChildWorkflow);

fn periodicSequencePolling(ctx: *workflow.Context, polling_interval: std.Io.Duration) ![]const u8 {
    return ctx.executeChildWorkflow(PollingChildWorkflow, .{}, .{.{ .polling_interval = polling_interval }}).get();
}

/// Takes the interval between the child workflow's polls.
pub const PeriodicSequencePolling = workflow.Def("PeriodicSequencePolling", periodicSequencePolling);
