//! Starts the workflow on a client that traces over OTLP, and waits for it.

const std = @import("std");
const temporal = @import("temporal");
const tracing_sample = @import("opentelemetry");
const sample = @import("datadog.zig");

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;
    const arena = init.arena.allocator();

    // Destroyed after the client, so the client's spans are exported before the program ends.
    const tracing = tracing_sample.setup.Tracing.create(gpa, io, init.environ_map, .{
        .service_name = "temporal-datadog-sample",
        .scope_name = "temporal-sdk-zig/samples/datadog",
        .default_exporter = .otlp,
    }) catch |err| {
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
        .id = "datadog_workflow_id",
        .task_queue = sample.task_queue,
    }, .{"<param to log>"}) catch |err| {
        std.log.err("Unable to execute workflow: {t}", .{err});
        std.process.exit(1);
    };
    defer run.deinit();
    std.log.info("Started workflow WorkflowID={s} RunID={s}", .{ run.id, run.run_id });

    // Synchronously wait for the workflow completion.
    run.get(arena) catch |err| {
        std.log.err("Unable get workflow result: {t}", .{err});
        std.process.exit(1);
    };
    std.log.info("Workflow completed. Check worker logs.", .{});
}
