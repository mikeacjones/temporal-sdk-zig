//! Worker-specific task queues: a file processing workflow whose activities must all run on
//! one host, because they share a file on its disk. Every worker also polls a task queue of
//! its own; the first activity, on the shared task queue, returns that queue, and the file
//! activities are scheduled on it.

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;
const activities = @import("activities.zig");

pub const task_queue = "shared-task-queue";

/// Attempts at the whole download, process, and delete sequence.
const max_attempts = 5;

fn fileProcessing(ctx: *workflow.Context) !void {
    const logger = ctx.logger();
    // A failure on the worker-specific task queue may mean that worker is gone, so retry the
    // whole sequence, on whichever worker picks up the first activity.
    var attempt: u32 = 1;
    while (true) : (attempt += 1) {
        if (processFile(ctx)) |_| {
            logger.info("Workflow completed.", .{});
            return;
        } else |err| {
            // warn rather than err: the failure is handled here, and the workflow's own
            // failure (after the last attempt) is reported to whoever waits on it.
            if (attempt == max_attempts) {
                logger.warn("Workflow failed after multiple retries.", .{ .err = err });
                return err;
            }
            logger.warn("Attempt failed, trying on new worker", .{ .attempt = attempt, .err = err });
        }
    }
}

fn processFile(ctx: *workflow.Context) !void {
    const worker_task_queue = try ctx.executeActivity(activities.GetWorkerSpecificTaskQueue, .{
        .start_to_close_timeout = .fromSeconds(60),
        .retry_policy = .{ .maximum_attempts = 1 },
    }, .{}).get();

    const ao: workflow.ActivityOptions = .{
        // Schedule-to-close rather than start-to-close: only one worker polls this task queue,
        // so if it goes away the activity is never started and only this timeout ends it.
        .schedule_to_close_timeout = .fromSeconds(60),
        .task_queue = worker_task_queue,
    };

    // The UUID comes from the workflow's seeded randomness, so replay builds the same path.
    const a = ctx.allocator();
    const download_path = try std.fs.path.join(a, &.{ "/tmp", try temporal.client.uuid(a, ctx.io()) });
    try ctx.executeActivity(activities.DownloadFile, ao, .{ "https://temporal.io", download_path }).get();

    // The file is deleted whether or not processing succeeded. If processing failed, the
    // attempt fails with that error, not the delete's: it is the one worth retrying for. (If
    // both fail, the failure details recorded are the delete's, the last activity to fail.)
    const processed = ctx.executeActivity(activities.ProcessFile, ao, .{download_path}).get();
    const deleted = ctx.executeActivity(activities.DeleteFile, ao, .{download_path}).get();
    try processed;
    try deleted;
}

pub const FileProcessingWorkflow = workflow.Def("FileProcessingWorkflow", fileProcessing);
