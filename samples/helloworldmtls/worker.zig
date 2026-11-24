const std = @import("std");
const temporal = @import("temporal");
const helloworld = @import("helloworld.zig");

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;
    const arena = init.arena.allocator();

    const args = try helloworld.programArgs(arena, init);
    // parseClientOptions logs what is wrong with the arguments.
    const options = helloworld.parseClientOptions(arena, args, init.environ_map) catch std.process.exit(1);
    // The client and worker are heavyweight objects that should be created once per process.
    var client = temporal.client.Client.connect(gpa, io, options) catch |err| {
        std.log.err("Unable to create client: {t}", .{err});
        std.process.exit(1);
    };
    defer client.close();

    var w = try temporal.worker.Worker.init(gpa, io, &client, helloworld.task_queue, .{});
    defer w.deinit();

    try w.registerWorkflow(helloworld.Workflow);
    try w.registerActivity(helloworld.Activity);

    w.runUntilSignal() catch |err| {
        std.log.err("Unable to start worker: {t}", .{err});
        std.process.exit(1);
    };
}
