const std = @import("std");
const temporal = @import("temporal");
const pso = @import("pso.zig");

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

    var w = try temporal.worker.Worker.init(gpa, io, &client, pso.task_queue, .{
        // The activities do the math, which is CPU bound, so run one at a time. Run more
        // workers to spread the work.
        .max_concurrent_activity_executions = 1,
    });
    defer w.deinit();

    try w.registerWorkflow(pso.PsoWorkflow);
    try w.registerWorkflow(pso.PsoChildWorkflow);
    try w.registerActivity(pso.InitParticle);
    try w.registerActivity(pso.UpdateParticle);

    w.runUntilSignal() catch |err| {
        std.log.err("Unable to start worker: {t}", .{err});
        std.process.exit(1);
    };
}
