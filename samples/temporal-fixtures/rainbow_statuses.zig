//! Rainbow statuses: one workflow that ends in whichever status it is started for, so a
//! server holds a run in every status the UI shows. The starter starts one per status and
//! then cancels, terminates and signals the ones whose status comes from outside.
//!
//! The starter sets the search attributes CustomKeywordField, CustomIntField,
//! CustomDoubleField, CustomBoolField, CustomDatetimeField and CustomTextField. samples-test's
//! dev server registers them; to run against a dev server by hand, start it with
//!
//!     temporal server start-dev --search-attribute CustomKeywordField=Keyword \
//!         --search-attribute CustomIntField=Int --search-attribute CustomDoubleField=Double \
//!         --search-attribute CustomBoolField=Bool --search-attribute CustomDatetimeField=Datetime \
//!         --search-attribute CustomTextField=Text
//!
//! or add them to a running one with `temporal operator search-attribute create --name
//! <name> --type <type>`.
//!
//! The workflow takes this sample's own `Status` enum, not the server's status number.

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;
const activity = temporal.activity;

pub const task_queue = "rainbow-statuses";

/// The status a run is started to end in.
pub const Status = enum { running, completed, failed, canceled, terminated, continued_as_new, timed_out };

/// "Running", "Completed", "Failed", "Canceled", "Terminated", "ContinuedAsNew" or
/// "TimedOut": the suffix of the run's workflow ID.
pub fn label(s: Status) []const u8 {
    return switch (s) {
        .running => "Running",
        .completed => "Completed",
        .failed => "Failed",
        .canceled => "Canceled",
        .terminated => "Terminated",
        .continued_as_new => "ContinuedAsNew",
        .timed_out => "TimedOut",
    };
}

fn completedActivity(ctx: *activity.Context) !void {
    _ = ctx;
}

pub const CompletedActivity = activity.Def("CompletedActivity", completedActivity);

fn failedActivity(ctx: *activity.Context) !void {
    return ctx.fail(.{ .message = "manual failure" });
}

pub const FailedActivity = activity.Def("FailedActivity", failedActivity);

/// Sleeps for a day. It does not heartbeat, so the only cancel that reaches it is the
/// worker's stop. That must not decide the run's status: the attempt is left open on the
/// server, where it ends at its start-to-close timeout or with its run, as if the worker's
/// process had exited. Any other cancel ends the attempt canceled.
fn longActivity(ctx: *activity.Context) !void {
    ctx.io().sleep(.fromSeconds(24 * std.time.s_per_hour), .awake) catch |err| {
        if (ctx.cancelReason() == .worker_shutdown) return ctx.completeAsync();
        return err;
    };
}

pub const LongActivity = activity.Def("LongActivity", longActivity);

fn rainbowStatusesWorkflow(ctx: *workflow.Context, status: Status) !void {
    const logger = ctx.logger();
    logger.info("RainbowStatusesWorkflow workflow started", .{ .status = @tagName(status) });

    const ao: workflow.ActivityOptions = .{
        .start_to_close_timeout = .fromSeconds(std.time.s_per_hour),
        .retry_policy = .{ .maximum_attempts = 1 },
    };
    const result = switch (status) {
        .completed => ctx.executeActivity(CompletedActivity, ao, .{}).get(),
        .running, .terminated, .canceled, .timed_out => ctx.executeActivity(LongActivity, ao, .{}).get(),
        .failed => ctx.executeActivity(FailedActivity, ao, .{}).get(),
        .continued_as_new => return ctx.continueAsNew(RainbowStatusesWorkflow, .{}, .{.completed}),
    };
    // A cancel request makes `get` return error.Canceled, which ends the run canceled.
    result catch |err| {
        logger.err("Activity failed.", .{ .err = err });
        return err;
    };

    logger.info("RainbowStatusesWorkflow workflow completed.", .{});
}

pub const RainbowStatusesWorkflow = workflow.Def("RainbowStatusesWorkflow", rainbowStatusesWorkflow);
