const std = @import("std");
const temporal = @import("temporal");
const externalenvconf = @import("externalenvconf.zig");

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;

    const options = externalenvconf.loadProfile(init.arena.allocator(), init.environ_map) catch |err| {
        std.log.err("Unable to load client config from {s}: {t}", .{ externalenvconf.config_file, err });
        std.process.exit(1);
    };
    // The client and worker are heavyweight objects that should be created once per process.
    var client = temporal.client.Client.connect(gpa, io, options) catch |err| {
        std.log.err("Unable to create client: {t}", .{err});
        std.process.exit(1);
    };
    defer client.close();

    var w = try temporal.worker.Worker.init(gpa, io, &client, "external-env-conf", .{});
    defer w.deinit();

    try w.registerWorkflow(externalenvconf.Workflow);
    try w.registerActivity(externalenvconf.Activity);

    w.runUntilSignal() catch |err| {
        std.log.err("Unable to start worker: {t}", .{err});
        std.process.exit(1);
    };
}
