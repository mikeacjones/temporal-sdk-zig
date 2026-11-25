//! Session failure recovery: a workflow opens a session (`workflow.Context.createSession`),
//! prepares the host that took it, and runs a long activity there. When the host goes away,
//! the session fails; the workflow then waits and runs the whole sequence again in a new
//! session, on whichever host takes that one.
//!
//! The workflow learns that a host went away only from the session's heartbeats, so its view
//! of the session is a little behind. An activity whose timeout is shorter than about twice
//! the session's heartbeat timeout can time out before the session is marked failed. A host
//! that restarts is a new host: it doesn't take activities scheduled on its old session.

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;
const activities = @import("activities.zig");

pub const task_queue = "session-failure";

/// Sessions the workflow tries before it gives up.
pub const max_session_attempts = 10;

/// How long the workflow waits after a session's host went away before it tries a new one.
pub const session_retry_delay: std.Io.Duration = .fromSeconds(5 * 60);

const session_options: workflow.SessionOptions = .{
    .creation_timeout = .fromSeconds(60),
    .execution_timeout = .fromSeconds(20 * 60),
};

const activity_options: workflow.ActivityOptions = .{
    .start_to_close_timeout = .fromSeconds(10 * 60),
    // The session's own heartbeat notices a lost host; this one catches an activity that is
    // stuck on a host that is still up.
    .heartbeat_timeout = .fromSeconds(40),
    .retry_policy = .{
        .initial_interval = .fromSeconds(1),
        .backoff_coefficient = 2.0,
        .maximum_interval = .fromSeconds(60),
    },
};

/// The workflow's whole body: runs the session's work, and when its host went away
/// (`error.SessionHostDown`), sleeps `retry_delay` and tries again in a new session, up to
/// `max_session_attempts` sessions. Any other error fails the workflow at once.
pub fn recoverFromSessionFailures(ctx: *workflow.Context, retry_delay: std.Io.Duration) !void {
    const logger = ctx.logger();
    var attempt: u32 = 1;
    while (true) : (attempt += 1) {
        if (runSession(ctx)) |_| {
            logger.info("Workflow completed.", .{});
            return;
        } else |err| switch (err) {
            error.SessionHostDown => {
                if (attempt == max_session_attempts) {
                    // warn rather than err: the workflow's own failure is reported to
                    // whoever waits on it.
                    logger.warn("Workflow failed after multiple session retries.", .{ .Error = err });
                    return err;
                }
                logger.warn("Session host down, trying again in a new session", .{ .attempt = attempt });
                try ctx.sleep(retry_delay, .{});
            },
            else => {
                logger.warn("Workflow failed.", .{ .Error = err });
                return err;
            },
        }
    }
}

fn sampleSessionFailureRecoveryWorkflow(ctx: *workflow.Context) !void {
    return recoverFromSessionFailures(ctx, session_retry_delay);
}

pub const SampleSessionFailureRecoveryWorkflow = workflow.Def("SampleSessionFailureRecoveryWorkflow", sampleSessionFailureRecoveryWorkflow);

/// One attempt, in a session of its own. Returns `error.SessionHostDown` when no host took
/// the session, or when the session failed by the time the attempt ended, whatever the
/// activities returned: the error is then the lost host's, not theirs.
fn runSession(ctx: *workflow.Context) !void {
    const session = ctx.createSession(session_options) catch |err| switch (err) {
        // No host took the session within its creation timeout, or the host had no free
        // session slot. Either is worth trying again later.
        error.SessionCreationFailed => {
            ctx.logger().warn("Session failed", .{ .Error = err });
            return error.SessionHostDown;
        },
        else => return err,
    };
    defer ctx.completeSession(session) catch {};

    runActivities(ctx, session) catch |err| switch (err) {
        error.Canceled, error.OutOfMemory => return err,
        else => {
            if (session.state() == .failed) return error.SessionHostDown;
            return err;
        },
    };
    if (session.state() == .failed) return error.SessionHostDown;
}

fn runActivities(ctx: *workflow.Context, session: *const workflow.Session) !void {
    try executeInSession(ctx, session, activities.PrepareWorkerActivity);
    try executeInSession(ctx, session, activities.LongRunningActivity);
}

/// Runs `Activity` in `session`, and returns once either the activity ended or the session
/// failed. A failed session doesn't cancel the activities running in it, and an activity
/// retried onto a host that has stopped would never start, so waiting for the activity alone
/// could wait until its timeout. When the session fails first, the activity's cancellation is
/// requested and this returns `error.SessionFailed` without waiting for it.
fn executeInSession(ctx: *workflow.Context, session: *const workflow.Session, comptime Activity: type) !void {
    var options = activity_options;
    options.session = session;
    const future = ctx.executeActivity(Activity, options, .{});

    const Wait = struct {
        future: @TypeOf(future),
        session: *const workflow.Session,

        fn done(w: *const @This()) bool {
            return w.future.isReady() or w.session.state() == .failed;
        }
    };
    try ctx.await(&Wait{ .future = future, .session = session }, Wait.done);

    if (!future.isReady()) {
        future.cancel();
        return error.SessionFailed;
    }
    return future.get();
}
