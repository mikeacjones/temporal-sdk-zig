const std = @import("std");
const temporal = @import("temporal");
const greetings = @import("greetings.zig");

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

    var w = try temporal.worker.Worker.init(gpa, io, &client, "greetings", .{});
    defer w.deinit();

    try w.registerWorkflow(greetings.Greetings);
    var activities: greetings.Activities = .{ .name = "Temporal", .greeting = "Hello" };
    try w.registerActivities(&activities, .{});

    w.runUntilSignal() catch |err| {
        std.log.err("Unable to start worker: {t}", .{err});
        std.process.exit(1);
    };
}
