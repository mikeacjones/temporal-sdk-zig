const std = @import("std");
const temporal = @import("temporal");
const snappycompress = @import("snappycompress.zig");
const codec = @import("codec.zig");

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;

    // The client and worker are heavyweight objects that should be created once per process.
    // The codec on the client makes its workers compress activity inputs and workflow and
    // activity results.
    const codecs = [_]temporal.converter.PayloadCodec{codec.always_compress.codec()};
    var options = try temporal.client.loadDefaultOptions(init.arena.allocator());
    options.codecs = &codecs;
    var client = temporal.client.Client.connect(gpa, io, options) catch |err| {
        std.log.err("Unable to create client: {t}", .{err});
        std.process.exit(1);
    };
    defer client.close();

    var w = try temporal.worker.Worker.init(gpa, io, &client, snappycompress.task_queue, .{});
    defer w.deinit();

    try w.registerWorkflow(snappycompress.Workflow);
    try w.registerActivity(snappycompress.Activity);

    w.runUntilSignal() catch |err| {
        std.log.err("Unable to start worker: {t}", .{err});
        std.process.exit(1);
    };
}
