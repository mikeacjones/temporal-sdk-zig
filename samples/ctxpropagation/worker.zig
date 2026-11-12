const std = @import("std");
const temporal = @import("temporal");
const ctxpropagation = @import("ctxpropagation.zig");

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;

    var tracer: ctxpropagation.Tracer = .{ .service_name = "ctx-propagation-sample" };
    const interceptors = [_]temporal.interceptor.Client{tracer.client()};
    const propagators = [_]temporal.propagation.ContextPropagator{ctxpropagation.propagator};

    // The client and worker are heavyweight objects that should be created once per process.
    // The worker uses the client's propagators to read the value from each workflow's and
    // activity's header.
    var options = try temporal.client.loadDefaultOptions(init.arena.allocator());
    options.interceptors = &interceptors;
    options.propagators = &propagators;
    var client = temporal.client.Client.connect(gpa, io, options) catch |err| {
        std.log.err("Unable to create client: {t}", .{err});
        std.process.exit(1);
    };
    defer client.close();

    var w = try temporal.worker.Worker.init(gpa, io, &client, "ctx-propagation", .{});
    defer w.deinit();

    try w.registerWorkflow(ctxpropagation.CtxPropWorkflow);
    try w.registerActivity(ctxpropagation.SampleActivity);

    w.runUntilSignal() catch |err| {
        std.log.err("Unable to start worker: {t}", .{err});
        std.process.exit(1);
    };
}
