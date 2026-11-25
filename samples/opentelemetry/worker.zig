//! Runs the workflow and the activity on a client that traces through OpenTelemetry.

const std = @import("std");
const temporal = @import("temporal");
const sample = @import("opentelemetry.zig");

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;

    // Destroyed after the worker and the client, so every span they ended is exported.
    const tracing = sample.setup.Tracing.create(gpa, io, init.environ_map, .{ .scope_name = "temporal-sdk-zig/samples/opentelemetry" }) catch |err| {
        std.log.err("Unable to create a trace provider: {t}", .{err});
        std.process.exit(1);
    };
    defer tracing.destroy();
    const interceptors = [_]temporal.interceptor.Client{tracing.interceptor.client()};
    const worker_interceptors = [_]temporal.interceptor.Worker{tracing.interceptor.worker()};
    const propagators = [_]temporal.propagation.ContextPropagator{tracing.interceptor.propagator()};

    // The client and worker are heavyweight objects that should be created once per process.
    var options = try temporal.client.loadDefaultOptions(init.arena.allocator());
    options.interceptors = &interceptors;
    options.worker_interceptors = &worker_interceptors;
    options.propagators = &propagators;
    var client = temporal.client.Client.connect(gpa, io, options) catch |err| {
        std.log.err("Unable to create client: {t}", .{err});
        std.process.exit(1);
    };
    defer client.close();

    var activities: sample.Activities = .{ .gpa = gpa, .tracer = tracing.tracer };
    var w = try temporal.worker.Worker.init(gpa, io, &client, sample.task_queue, .{});
    defer w.deinit();

    try w.registerWorkflow(sample.Workflow);
    try w.registerActivityWith(sample.Activity, &activities);

    w.runUntilSignal() catch |err| {
        std.log.err("Unable to start worker: {t}", .{err});
        std.process.exit(1);
    };
}
