const std = @import("std");
const temporal = @import("temporal");
const dynamic = @import("dynamic.zig");

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

    var w = try temporal.worker.Worker.init(gpa, io, &client, "dynamic", .{});
    defer w.deinit();

    try w.registerWorkflow(dynamic.SampleGreetingsWorkflow);
    try w.registerActivity(dynamic.GetGreeting);
    try w.registerActivity(dynamic.GetName);
    try w.registerActivity(dynamic.SayGreeting);

    w.runUntilSignal() catch |err| {
        std.log.err("Unable to start worker: {t}", .{err});
        std.process.exit(1);
    };
}
