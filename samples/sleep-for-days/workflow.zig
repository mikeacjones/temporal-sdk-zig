//! Sleep for days: a workflow that sends an email every 30 days until it is told to stop with
//! the "complete" signal. Its 30-day waits are durable timers, so the workflow keeps no
//! worker busy between emails and survives worker restarts.

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;
const activity = temporal.activity;

/// How long the workflow waits between emails.
pub const email_interval: std.Io.Duration = .fromSeconds(30 * std.time.s_per_day);

/// Ends the workflow, after the email it is sending (if any) has been sent.
pub const Complete = workflow.SignalDef("complete", fn () void);

/// Set by the "complete" signal. Allocated with `ctx.allocator()` so the handler can reach it
/// for the whole run.
const State = struct {
    complete: bool = false,

    fn onComplete(s: *State) void {
        s.complete = true;
    }

    fn isComplete(s: *State) bool {
        return s.complete;
    }
};

fn sleepForDaysWorkflow(ctx: *workflow.Context) ![]const u8 {
    const state = try ctx.allocator().create(State);
    state.* = .{};
    try ctx.setSignalHandler(Complete, state, State.onComplete);

    const ao: workflow.ActivityOptions = .{ .start_to_close_timeout = .fromSeconds(10) };
    while (!state.complete) {
        // Waiting for the email keeps the run from ending with one still in flight.
        try ctx.executeActivity(SendEmailActivity, ao, .{"Sleeping for 30 days"}).get();
        _ = try ctx.awaitTimeout(email_interval, state, State.isComplete);
    }
    return "done";
}

pub const SleepForDaysWorkflow = workflow.Def("SleepForDaysWorkflow", sleepForDaysWorkflow);

/// A stand-in for sending an email: it only logs the message.
fn sendEmail(ctx: *activity.Context, msg: []const u8) !void {
    ctx.logger().info("Sending email.", .{ .msg = msg });
}

pub const SendEmailActivity = activity.Def("SendEmailActivity", sendEmail);
