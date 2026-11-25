//! Large event history: a workflow that runs one small activity after another, about six
//! history events each, until its history is as long as asked, and then fails if asked to.

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;
const activity = temporal.activity;

pub const task_queue = "largeeventhistory";

fn activityFn(ctx: *activity.Context) !void {
    _ = ctx;
}

pub const Activity = activity.Def("Activity", activityFn);

fn largeEventHistoryWorkflow(ctx: *workflow.Context, length_of_history: u32, will_fail: bool) !void {
    const ao: workflow.ActivityOptions = .{ .start_to_close_timeout = .fromSeconds(std.time.s_per_min) };

    // Each activity adds about six events. Only the last one's error counts, except a
    // cancel: the workflow's cancel is used up by the one wait that returns error.Canceled,
    // and later activities would run normally, so the loop stops there and the run ends
    // canceled.
    var last: anyerror!void = {};
    for (0..length_of_history / 6) |_| {
        last = ctx.executeActivity(Activity, ao, .{}).get();
        last catch |err| if (err == error.Canceled) return error.Canceled;
    }
    last catch return ctx.fail(.{ .message = "unexpected Activity failure" });

    if (will_fail) return ctx.fail(.{ .message = "intentional workflow failure due to WillFailOrNot parameter" });
}

pub const LargeEventHistoryWorkflow = workflow.Def("LargeEventHistoryWorkflow", largeEventHistoryWorkflow);
