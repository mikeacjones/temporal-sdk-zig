const std = @import("std");
const temporal = @import("temporal");
const security_interceptor = @import("security_interceptor.zig");

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

    var security: security_interceptor.WorkerInterceptor = .{};
    const interceptors = [_]temporal.interceptor.Worker{.from(security_interceptor.WorkerInterceptor, &security)};
    var w = try temporal.worker.Worker.init(gpa, io, &client, "security-interceptor", .{ .interceptors = &interceptors });
    defer w.deinit();

    try w.registerWorkflow(security_interceptor.Workflow);
    try w.registerWorkflow(security_interceptor.ChildWorkflow);
    try w.registerWorkflow(security_interceptor.ProhibitedChildWorkflow);
    // The activity the interceptor uses.
    try w.registerActivity(security_interceptor.ValidateChildWorkflowTypeActivity);

    w.runUntilSignal() catch |err| {
        std.log.err("Unable to start worker: {t}", .{err});
        std.process.exit(1);
    };
}
