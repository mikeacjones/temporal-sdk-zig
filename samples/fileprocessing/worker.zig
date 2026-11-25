//! Runs a session worker for the file processing sample: it serves the workflow and also
//! takes sessions, running the file activities of each session it took on this host.

const std = @import("std");
const temporal = @import("temporal");
const sample = @import("workflow.zig");
const activities = @import("activities.zig");

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;

    // The client and worker are heavyweight objects that should be created once per process.
    const options = try temporal.client.loadDefaultOptions(init.arena.allocator());
    var client = temporal.client.Client.connect(gpa, io, options) catch |err| {
        std.log.err("Unable to create client: {t}", .{err});
        std.process.exit(1);
    };
    defer client.close();

    // The activities' state outlives the worker, which is deinitialized first.
    var blob_store: activities.BlobStore = .{};
    var acts: activities.Activities = .{ .blob_store = &blob_store };

    // `sessions` makes this worker a session host: important for it to take part in sessions.
    var w = try temporal.worker.Worker.init(gpa, io, &client, sample.task_queue, .{ .sessions = .{} });
    defer w.deinit();

    try w.registerWorkflow(sample.SampleFileProcessingWorkflow);
    try w.registerActivitiesWith(&acts, activities.all);

    w.runUntilSignal() catch |err| {
        std.log.err("Unable to start worker: {t}", .{err});
        std.process.exit(1);
    };
}
