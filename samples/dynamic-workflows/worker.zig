const std = @import("std");
const temporal = @import("temporal");
const dynamic_workflows = @import("dynamic_workflows.zig");

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

    var w = try temporal.worker.Worker.init(gpa, io, &client, "dynamic-workflows", .{});
    defer w.deinit();

    // A dynamic workflow could also choose a versioning behavior per workflow type (for
    // example pinned for one type and auto-upgrade for another). Worker versioning isn't
    // available in this SDK yet, so every type runs unversioned.
    try w.registerDynamicWorkflow(dynamic_workflows.DynamicWorkflow);
    try w.registerDynamicActivity(dynamic_workflows.DynamicActivity);

    w.runUntilSignal() catch |err| {
        std.log.err("Unable to start worker: {t}", .{err});
        std.process.exit(1);
    };
}
