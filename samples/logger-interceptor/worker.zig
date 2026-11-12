const std = @import("std");
const temporal = @import("temporal");
const logger_interceptor = @import("logger_interceptor.zig");

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

    // The interceptor puts the start time on every workflow and activity log line.
    var logging: logger_interceptor.WorkerInterceptor = .{ .options = .{
        .workflow_fields = logger_interceptor.workflowStartTime,
        .activity_fields = logger_interceptor.activityStartTime,
    } };
    const interceptors = [_]temporal.interceptor.Worker{.from(logger_interceptor.WorkerInterceptor, &logging)};
    var w = try temporal.worker.Worker.init(gpa, io, &client, "logger-interceptor", .{ .interceptors = &interceptors });
    defer w.deinit();

    try w.registerWorkflow(logger_interceptor.Workflow);
    try w.registerActivity(logger_interceptor.Activity);

    w.runUntilSignal() catch |err| {
        std.log.err("Unable to start worker: {t}", .{err});
        std.process.exit(1);
    };
}
