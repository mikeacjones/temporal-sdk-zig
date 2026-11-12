//! A plain worker for the eager_wf_start task queue. Eager starts only reach the worker that
//! runs in the starter's process; this one polls for the workflows that start normally.

const std = @import("std");
const temporal = @import("temporal");
const helloworld = @import("helloworld");

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

    var w = try temporal.worker.Worker.init(gpa, io, &client, "eager_wf_start", .{});
    defer w.deinit();

    try w.registerWorkflow(helloworld.HelloWorld);
    try w.registerActivity(helloworld.Greet);

    w.runUntilSignal() catch |err| {
        std.log.err("Unable to start worker: {t}", .{err});
        std.process.exit(1);
    };
}
