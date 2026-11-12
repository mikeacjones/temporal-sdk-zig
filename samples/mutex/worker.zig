const std = @import("std");
const temporal = @import("temporal");
const mutex = @import("mutex.zig");

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

    var w = try temporal.worker.Worker.init(gpa, io, &client, mutex.task_queue, .{});
    defer w.deinit();

    // The activity signals-with-start the mutex workflow through this worker's client.
    var activities: mutex.Activities = .{ .client = &client };
    try w.registerActivityWith(mutex.SignalWithStartMutexWorkflowActivity, &activities);
    try w.registerWorkflow(mutex.MutexWorkflow);
    try w.registerWorkflow(mutex.SampleWorkflowWithMutex);

    w.runUntilSignal() catch |err| {
        std.log.err("Unable to start worker: {t}", .{err});
        std.process.exit(1);
    };
}
