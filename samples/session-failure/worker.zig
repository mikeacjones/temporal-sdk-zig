//! Runs a session worker for the session failure sample: it serves the workflow and also
//! takes sessions, running the session's activities on this host. Run several, then stop the
//! one running the long activity, to see the workflow recover in a new session.
//!
//! Usage: worker [work-seconds]. How long the long-running activity works; defaults to 5
//! minutes. samples-test passes a few seconds.

const std = @import("std");
const temporal = @import("temporal");
const sample = @import("workflow.zig");
const activities = @import("activities.zig");

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;

    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.skip();
    const work_seconds: u32 = if (args.next()) |a| std.fmt.parseInt(u32, a, 10) catch {
        std.log.err("work-seconds must be a whole number of seconds, got \"{s}\"", .{a});
        std.process.exit(2);
    } else 5 * 60;

    // The client and worker are heavyweight objects that should be created once per process.
    const options = try temporal.client.loadDefaultOptions(init.arena.allocator());
    var client = temporal.client.Client.connect(gpa, io, options) catch |err| {
        std.log.err("Unable to create client: {t}", .{err});
        std.process.exit(1);
    };
    defer client.close();

    // The activities' state outlives the worker, which is deinitialized first.
    var acts: activities.Activities = .{ .work_time = .fromSeconds(work_seconds) };

    // `sessions` makes this worker a session host: important for it to take part in sessions.
    var w = try temporal.worker.Worker.init(gpa, io, &client, sample.task_queue, .{ .sessions = .{} });
    defer w.deinit();

    try w.registerWorkflow(sample.SampleSessionFailureRecoveryWorkflow);
    try w.registerActivitiesWith(&acts, activities.all);

    w.runUntilSignal() catch |err| {
        std.log.err("Unable to start worker: {t}", .{err});
        std.process.exit(1);
    };
}
