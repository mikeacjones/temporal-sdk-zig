//! Runs the workflows and the activity on a client that traces over OTLP, serves its metrics
//! for Prometheus to scrape, and logs as JSON.
//!
//! Usage: worker [metrics-bind-address] [log-file]. The address defaults to 127.0.0.1:9090;
//! the endpoint is http://<metrics-bind-address>/metrics. Log records go to stderr and, when
//! `log-file` is given, are also appended to it.

const std = @import("std");
const temporal = @import("temporal");
const tracing_sample = @import("opentelemetry");
const logadapter = @import("logadapter");
const sample = @import("datadog.zig");

const default_bind_address = "127.0.0.1:9090";

/// Hands every record to each of `sinks`.
const TeeSink = struct {
    sinks: []const temporal.log.Sink,

    fn sink(t: *TeeSink) temporal.log.Sink {
        return .{ .ptr = t, .vtable = &.{ .log = write } };
    }

    fn write(ptr: ?*anyopaque, level: temporal.log.Level, message: []const u8, fields: []const temporal.log.Field) void {
        const t: *TeeSink = @ptrCast(@alignCast(ptr.?));
        for (t.sinks) |s| s.log(level, message, fields);
    }
};

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;

    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.skip();
    const bind_address = args.next() orelse default_bind_address;
    const log_path = args.next();

    // Every log record of the client and its worker at info level and above, as JSON lines.
    var stderr_buffer: [4096]u8 = undefined;
    var stderr = std.Io.File.stderr().writerStreaming(io, &stderr_buffer);
    var stderr_json: logadapter.JsonSink = .{ .io = io, .out = &stderr.interface, .min_level = .info };

    var file_buffer: [4096]u8 = undefined;
    var log_file: ?std.Io.File = null;
    defer if (log_file) |f| f.close(io);
    var file_writer: std.Io.File.Writer = undefined;
    var file_json: logadapter.JsonSink = undefined;
    var sinks: [2]temporal.log.Sink = .{ stderr_json.sink(), undefined };
    var sink_count: usize = 1;
    if (log_path) |path| {
        const file = std.Io.Dir.cwd().createFile(io, path, .{ .truncate = false }) catch |err| {
            std.log.err("Unable to open log file {s}: {t}", .{ path, err });
            std.process.exit(1);
        };
        log_file = file;
        file_writer = file.writer(io, &file_buffer);
        const end = file.length(io) catch |err| {
            std.log.err("Unable to open log file {s}: {t}", .{ path, err });
            std.process.exit(1);
        };
        file_writer.seekTo(end) catch |err| {
            std.log.err("Unable to open log file {s}: {t}", .{ path, err });
            std.process.exit(1);
        };
        file_json = .{ .io = io, .out = &file_writer.interface, .min_level = .info };
        sinks[1] = file_json.sink();
        sink_count = 2;
    }
    var tee: TeeSink = .{ .sinks = sinks[0..sink_count] };

    // Destroyed after the worker and the client, so every span they ended is exported.
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

    // The client and worker are heavyweight objects that should be created once per process.
    var options = try temporal.client.loadDefaultOptions(init.arena.allocator());
    options.logger = tee.sink();
    options.interceptors = &interceptors;
    options.worker_interceptors = &worker_interceptors;
    options.propagators = &propagators;
    options.metrics = .{
        .exporter = .{ .prometheus = .{ .bind_address = bind_address } },
        .metric_prefix = sample.metric_prefix,
    };
    var client = temporal.client.Client.connect(gpa, io, options) catch |err| {
        std.log.err("Unable to create client with a Prometheus exporter on {s}: {t}", .{ bind_address, err });
        std.process.exit(1);
    };
    defer client.close();
    std.log.info("Serving metrics on http://{s}/metrics", .{bind_address});

    var w = try temporal.worker.Worker.init(gpa, io, &client, sample.task_queue, .{});
    defer w.deinit();

    try w.registerWorkflow(sample.Workflow);
    try w.registerWorkflow(sample.ChildWorkflow);
    try w.registerActivity(sample.Activity);

    w.runUntilSignal() catch |err| {
        std.log.err("Unable to start worker: {t}", .{err});
        std.process.exit(1);
    };
}
