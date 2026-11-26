//! Runs a worker for each of the sample's three parts, each on its own task queue, sharing one
//! client that traces through OpenTelemetry and serves its metrics for Prometheus to scrape.
//! One SIGINT or SIGTERM stops all three.
//!
//! Usage: worker [metrics-bind-address]. The address defaults to 127.0.0.1:9090; the endpoint
//! is http://<metrics-bind-address>/metrics.

const std = @import("std");
const temporal = @import("temporal");
const sample = @import("opentelemetry-v2.zig");

const Worker = temporal.worker.Worker;
const automatic = sample.automatic_instrumentation;
const propagation = sample.workflow_activity_propagation;
const update = sample.client_update_propagation;

const default_bind_address = "127.0.0.1:9090";

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;

    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.skip();
    const bind_address = args.next() orelse default_bind_address;

    // Destroyed after the workers and the client, so every span they ended is exported.
    const tracing = sample.setup.Tracing.create(gpa, io, init.environ_map, .{
        .service_name = "temporal-otel-v2-worker",
        .scope_name = "temporal-sdk-zig/samples/opentelemetry-v2",
    }) catch |err| {
        std.log.err("Unable to create a trace provider: {t}", .{err});
        std.process.exit(1);
    };
    defer tracing.destroy();
    const interceptors = [_]temporal.interceptor.Client{tracing.interceptor.client()};
    const worker_interceptors = [_]temporal.interceptor.Worker{tracing.interceptor.worker()};
    const propagators = [_]temporal.propagation.ContextPropagator{tracing.interceptor.propagator()};

    // The client and workers are heavyweight objects that should be created once per process.
    var options = try temporal.client.loadDefaultOptions(init.arena.allocator());
    options.interceptors = &interceptors;
    options.worker_interceptors = &worker_interceptors;
    options.propagators = &propagators;
    options.metrics = .{ .exporter = .{ .prometheus = .{ .bind_address = bind_address } } };
    var client = temporal.client.Client.connect(gpa, io, options) catch |err| {
        std.log.err("Unable to create client with a Prometheus exporter on {s}: {t}", .{ bind_address, err });
        std.process.exit(1);
    };
    defer client.close();
    std.log.info("Serving metrics on http://{s}/metrics", .{bind_address});

    var automatic_worker = try Worker.init(gpa, io, &client, automatic.task_queue, .{});
    defer automatic_worker.deinit();
    try automatic_worker.registerWorkflow(automatic.Workflow);
    try automatic_worker.registerActivity(automatic.Activity);

    var activities: propagation.Activities = .{ .gpa = gpa, .tracer = tracing.tracer };
    var propagation_worker = try Worker.init(gpa, io, &client, propagation.task_queue, .{});
    defer propagation_worker.deinit();
    try propagation_worker.registerWorkflow(propagation.Workflow);
    try propagation_worker.registerActivityWith(propagation.Activity, &activities);

    var update_worker = try Worker.init(gpa, io, &client, update.task_queue, .{});
    defer update_worker.deinit();
    try update_worker.registerWorkflow(update.Workflow);

    // If starting a later worker fails, stop and join the ones already running before their
    // `defer deinit` and `client.close()` run. Declared after those, so it runs first; a stop
    // before `run` is harmless and permanent.
    var group: std.Io.Group = .init;
    errdefer {
        automatic_worker.stop();
        propagation_worker.stop();
        update_worker.stop();
        group.await(io) catch group.cancel(io);
    }
    try group.concurrent(io, runWorker, .{&automatic_worker});
    try group.concurrent(io, runWorker, .{&propagation_worker});
    try group.concurrent(io, runWorker, .{&update_worker});
    // Wait for all three workers to stop.
    try group.await(io);
}

fn runWorker(w: *Worker) void {
    w.runUntilSignal() catch |err| {
        std.log.err("Unable to start worker for task queue {s}: {t}", .{ w.task_queue, err });
        std.process.exit(1);
    };
}
