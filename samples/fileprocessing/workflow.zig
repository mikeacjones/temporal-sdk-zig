//! File processing: a workflow downloads a file, processes it, and uploads the result, all on
//! one host, because each step works on a file the one before it left on that host's disk. A
//! session (`workflow.Context.createSession`) pins the three activities to the worker host
//! that took it. When an attempt fails, perhaps because its host went away, the whole
//! sequence is tried again in a new session, on whichever host takes that one.

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;
const activities = @import("activities.zig");

const Failure = temporal.failure.Failure;

pub const task_queue = "fileprocessing";

/// Attempts at the whole create-session, download, process and upload sequence.
const max_attempts = 4;

const activity_options: workflow.ActivityOptions = .{
    .start_to_close_timeout = .fromSeconds(60),
    // Short, so a lost host is noticed within seconds and the activity retried or failed.
    .heartbeat_timeout = .fromSeconds(2),
    .retry_policy = .{
        .initial_interval = .fromSeconds(1),
        .backoff_coefficient = 2.0,
        .maximum_interval = .fromSeconds(60),
    },
};

const session_options: workflow.SessionOptions = .{
    .creation_timeout = .fromSeconds(60),
    .execution_timeout = .fromSeconds(60),
};

fn sampleFileProcessingWorkflow(ctx: *workflow.Context, file_name: []const u8) !void {
    const logger = ctx.logger();
    // Retry the whole sequence from the first activity on any error, so it can move to a
    // different host. A real application might also retry single activities, and tell
    // kinds of errors apart.
    var attempt: u32 = 1;
    while (true) : (attempt += 1) {
        var failure: ?*const Failure = null;
        if (processFile(ctx, file_name, &failure)) |_| {
            logger.info("Workflow completed.", .{});
            return;
        } else |err| switch (err) {
            error.Canceled, error.OutOfMemory => return err,
            else => {
                if (attempt == max_attempts) {
                    // warn rather than err: the workflow's own failure is reported to
                    // whoever waits on it.
                    logger.warn("Workflow failed.", .{ .Error = err });
                    // Fail with the step's own failure, so the activity's error reaches
                    // whoever waits on the workflow.
                    if (failure) |f| return ctx.failWith(f);
                    return err;
                }
                logger.warn("Attempt failed, trying again in a new session", .{ .attempt = attempt, .Error = err });
            },
        }
    }
}

/// One attempt, in a session of its own. When a step fails, `failure` is set to why.
fn processFile(ctx: *workflow.Context, file_name: []const u8, failure: *?*const Failure) !void {
    const session = try ctx.createSession(session_options);
    defer ctx.completeSession(session) catch {};

    const downloaded = try executeInSession(ctx, session, activities.DownloadFileActivity, .{file_name}, failure);
    const processed = try executeInSession(ctx, session, activities.ProcessFileActivity, .{downloaded}, failure);
    try executeInSession(ctx, session, activities.UploadFileActivity, .{processed}, failure);
}

/// Runs `Activity` in `session` and returns its result, or fails as soon as either the
/// activity fails or the session does. A failed session doesn't cancel the activities
/// running in it, and an activity retried onto a host that has stopped would never start, so
/// waiting for the activity alone could wait until its timeout. When the session fails
/// first, the activity's cancellation is requested and this returns `error.SessionFailed`
/// without waiting for it. On `error.ActivityFailed` and `error.SessionFailed`, `failure` is
/// set to the activity's or the session's failure.
fn executeInSession(
    ctx: *workflow.Context,
    session: *const workflow.Session,
    comptime Activity: type,
    args: Activity.Args,
    failure: *?*const Failure,
) !Activity.Result {
    var options = activity_options;
    options.session = session;
    const future = ctx.executeActivity(Activity, options, args);

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
        failure.* = session.failure();
        return error.SessionFailed;
    }
    return future.get() catch |err| {
        switch (err) {
            error.ActivityFailed => failure.* = future.failure(),
            error.SessionFailed => failure.* = session.failure(),
            else => {},
        }
        return err;
    };
}

pub const SampleFileProcessingWorkflow = workflow.Def("SampleFileProcessingWorkflow", sampleFileProcessingWorkflow);
