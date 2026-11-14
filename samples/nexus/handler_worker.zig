//! The handler worker on its own: serves `my-hello-service` and runs the workflow behind
//! say-hello, in namespace my-target-namespace on task queue nexus-handler. worker.zig says
//! what to create on the server first.

const std = @import("std");
const temporal = @import("temporal");
const handler = @import("handler.zig");

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;

    // The client and worker are heavyweight objects that should be created once per process.
    var client = handler.connect(gpa, io, init.arena.allocator()) catch |err| {
        std.log.err("Unable to create client for namespace {s}: {t}", .{ handler.namespace, err });
        std.process.exit(1);
    };
    defer client.close();

    var w = try temporal.worker.Worker.init(gpa, io, &client, handler.task_queue, .{});
    defer w.deinit();
    try handler.register(&w);

    w.runUntilSignal() catch |err| {
        std.log.err("Unable to start worker: {t}", .{err});
        std.process.exit(1);
    };
}
