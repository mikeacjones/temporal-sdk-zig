const std = @import("std");
const temporal = @import("temporal");
const sample = @import("batch_sliding_window.zig");

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

    var w = try temporal.worker.Worker.init(gpa, io, &client, sample.task_queue, .{});
    defer w.deinit();

    try w.registerWorkflow(sample.ProcessBatchWorkflow);
    try w.registerWorkflow(sample.SlidingWindowWorkflow);
    try w.registerWorkflow(sample.RecordProcessorWorkflow);

    var loader: sample.RecordLoader = .{ .record_count = 90 };
    try w.registerActivitiesWith(&loader, .{ sample.GetRecordCount, sample.GetRecords });

    w.runUntilSignal() catch |err| {
        std.log.err("Unable to start worker: {t}", .{err});
        std.process.exit(1);
    };
}
