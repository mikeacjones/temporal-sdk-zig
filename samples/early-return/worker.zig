const std = @import("std");
const temporal = @import("temporal");
const early_return = @import("early_return.zig");

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

    var w = try temporal.worker.Worker.init(gpa, io, &client, early_return.task_queue, .{});
    defer w.deinit();

    try w.registerWorkflow(early_return.Workflow);
    try w.registerActivity(early_return.Init);
    try w.registerActivity(early_return.CompleteTransaction);
    try w.registerActivity(early_return.CancelTransaction);

    w.runUntilSignal() catch |err| {
        std.log.err("Unable to start worker: {t}", .{err});
        std.process.exit(1);
    };
}
