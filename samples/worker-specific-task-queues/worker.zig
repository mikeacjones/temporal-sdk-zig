//! Runs two workers on one client: one on the shared task queue for the workflow and
//! `GetWorkerSpecificTaskQueue`, and one on a task queue unique to this process for the file
//! activities. One SIGINT or SIGTERM stops both.

const std = @import("std");
const temporal = @import("temporal");
const sample = @import("workflow.zig");
const activities = @import("activities.zig");

const Worker = temporal.worker.Worker;

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;
    const arena = init.arena.allocator();

    // The client and workers are heavyweight objects that should be created once per process.
    // This process needs two workers, one per task queue it polls, sharing one client.
    const options = try temporal.client.loadDefaultOptions(arena);
    var client = temporal.client.Client.connect(gpa, io, options) catch |err| {
        std.log.err("Unable to create client: {t}", .{err});
        std.process.exit(1);
    };
    defer client.close();

    var unique_task_queue: activities.WorkerSpecificTaskQueue = .{ .task_queue = try temporal.client.uuid(arena, io) };

    var shared = try Worker.init(gpa, io, &client, sample.task_queue, .{});
    defer shared.deinit();
    try shared.registerWorkflow(sample.FileProcessingWorkflow);
    try shared.registerActivityWith(activities.GetWorkerSpecificTaskQueue, &unique_task_queue);

    var unique = try Worker.init(gpa, io, &client, unique_task_queue.task_queue, .{});
    defer unique.deinit();
    try unique.registerActivity(activities.DownloadFile);
    try unique.registerActivity(activities.ProcessFile);
    try unique.registerActivity(activities.DeleteFile);

    var group: std.Io.Group = .init;
    try group.concurrent(io, runWorker, .{&shared});
    try group.concurrent(io, runWorker, .{&unique});
    // Wait for both workers to stop.
    try group.await(io);
}

fn runWorker(w: *Worker) void {
    w.runUntilSignal() catch |err| {
        std.log.err("Unable to start worker for task queue {s}: {t}", .{ w.task_queue, err });
        std.process.exit(1);
    };
}
