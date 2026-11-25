//! Starts the workflow on a client that traces through OpenTelemetry, and waits for it.

const std = @import("std");
const temporal = @import("temporal");
const sample = @import("opentelemetry.zig");

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;
    const arena = init.arena.allocator();

    // Destroyed after the client, so the client's spans are exported before the program ends.
    const tracing = sample.setup.Tracing.create(gpa, io, init.environ_map, .{ .scope_name = "temporal-sdk-zig/samples/opentelemetry" }) catch |err| {
        std.log.err("Unable to create a trace provider: {t}", .{err});
        std.process.exit(1);
    };
    defer tracing.destroy();
    const interceptors = [_]temporal.interceptor.Client{tracing.interceptor.client()};
    const worker_interceptors = [_]temporal.interceptor.Worker{tracing.interceptor.worker()};
    const propagators = [_]temporal.propagation.ContextPropagator{tracing.interceptor.propagator()};

    // The client is a heavyweight object that should be created once per process.
    var options = try temporal.client.loadDefaultOptions(arena);
    options.interceptors = &interceptors;
    options.worker_interceptors = &worker_interceptors;
    options.propagators = &propagators;
    var client = temporal.client.Client.connect(gpa, io, options) catch |err| {
        std.log.err("Unable to create client: {t}", .{err});
        std.process.exit(1);
    };
    defer client.close();

    var run = client.executeWorkflow(sample.Workflow, .{
        .id = "otel_workflowID",
        .task_queue = sample.task_queue,
    }, .{"Temporal"}) catch |err| {
        std.log.err("Unable to execute workflow: {t}", .{err});
        std.process.exit(1);
    };
    defer run.deinit();
    std.log.info("Started workflow WorkflowID={s} RunID={s}", .{ run.id, run.run_id });

    // Synchronously wait for the workflow completion.
    run.get(arena) catch |err| {
        std.log.err("Unable to wait for workflow completion: {t}", .{err});
        std.process.exit(1);
    };
    std.log.info("Workflow completed.", .{});
}
