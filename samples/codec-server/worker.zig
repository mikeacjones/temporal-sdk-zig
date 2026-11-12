const std = @import("std");
const temporal = @import("temporal");
const codec_server = @import("codec_server.zig");

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;

    // The client and worker are heavyweight objects that should be created once per process.
    var options = try temporal.client.loadDefaultOptions(init.arena.allocator());
    // The worker uses its client's codecs, so workflow and activity inputs and results are
    // compressed too.
    const snappy: codec_server.SnappyCodec = .{};
    const codecs = [_]temporal.converter.PayloadCodec{snappy.codec()};
    options.codecs = &codecs;
    var client = temporal.client.Client.connect(gpa, io, options) catch |err| {
        std.log.err("Unable to create client: {t}", .{err});
        std.process.exit(1);
    };
    defer client.close();

    var w = try temporal.worker.Worker.init(gpa, io, &client, codec_server.task_queue, .{});
    defer w.deinit();

    try w.registerWorkflow(codec_server.Workflow);
    try w.registerActivity(codec_server.Activity);

    w.runUntilSignal() catch |err| {
        std.log.err("Unable to start worker: {t}", .{err});
        std.process.exit(1);
    };
}
