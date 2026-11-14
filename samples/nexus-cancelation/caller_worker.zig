//! The caller worker on its own: runs the caller workflow in namespace
//! my-cancelation-caller-namespace on task queue nexus-cancelation-caller. worker.zig says
//! what to create on the server first.

const std = @import("std");
const temporal = @import("temporal");
const caller = @import("caller.zig");

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;

    // The client and worker are heavyweight objects that should be created once per process.
    var client = caller.connect(gpa, io, init.arena.allocator()) catch |err| {
        std.log.err("Unable to create client for namespace {s}: {t}", .{ caller.namespace, err });
        std.process.exit(1);
    };
    defer client.close();

    var w = try temporal.worker.Worker.init(gpa, io, &client, caller.task_queue, .{});
    defer w.deinit();
    try caller.register(&w);

    w.runUntilSignal() catch |err| {
        std.log.err("Unable to start worker: {t}", .{err});
        std.process.exit(1);
    };
}
