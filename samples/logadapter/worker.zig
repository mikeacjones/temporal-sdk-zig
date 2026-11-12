const std = @import("std");
const temporal = @import("temporal");
const logadapter = @import("logadapter.zig");

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;

    // Every log record of the client and its worker, sdk-core's included, goes to stderr as
    // a JSON line, down to debug level.
    var stderr_buffer: [4096]u8 = undefined;
    var stderr = std.Io.File.stderr().writerStreaming(io, &stderr_buffer);
    var json: logadapter.JsonSink = .{ .io = io, .out = &stderr.interface, .min_level = .debug };

    // The client and worker are heavyweight objects that should be created once per process.
    var options = try temporal.client.loadDefaultOptions(init.arena.allocator());
    options.logger = json.sink();
    var client = temporal.client.Client.connect(gpa, io, options) catch |err| {
        std.log.err("Unable to create client: {t}", .{err});
        std.process.exit(1);
    };
    defer client.close();

    var w = try temporal.worker.Worker.init(gpa, io, &client, "slog-logger", .{});
    defer w.deinit();

    try w.registerWorkflow(logadapter.Workflow);
    try w.registerActivity(logadapter.LoggingActivity);
    try w.registerActivity(logadapter.LoggingErrorActivity);

    w.runUntilSignal() catch |err| {
        std.log.err("Unable to start worker: {t}", .{err});
        std.process.exit(1);
    };
}
