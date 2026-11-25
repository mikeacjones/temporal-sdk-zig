//! The OpenTelemetry setup the tracing samples share: a tracer provider with a batching
//! processor and an exporter, the Temporal tracer on it, and the tracing interceptor.
//!
//! Spans go out over OTLP (HTTP/protobuf) when the environment sets
//! OTEL_EXPORTER_OTLP_ENDPOINT or OTEL_EXPORTER_OTLP_TRACES_ENDPOINT, and to the exporter
//! `Options.default_exporter` names otherwise. The other OTEL_* variables the OpenTelemetry
//! SDK reads (the service name, the sampler, the batching settings, the OTLP headers and
//! timeout) apply as usual.

const std = @import("std");
const temporal = @import("temporal");
const temporal_opentelemetry = @import("temporal_opentelemetry");
const otel = @import("opentelemetry-sdk");

pub const Options = struct {
    /// service.name, unless the environment sets OTEL_SERVICE_NAME.
    service_name: []const u8 = "temporal-example",
    /// The instrumentation scope of the tracer. Borrowed until `Tracing.destroy`.
    scope_name: []const u8,
    /// Used when the environment sets neither OTEL_EXPORTER_OTLP_ENDPOINT nor
    /// OTEL_EXPORTER_OTLP_TRACES_ENDPOINT; with either set, spans go out over OTLP.
    default_exporter: enum { stdout, otlp } = .stdout,
};

/// A tracer provider with a batching processor and its exporter, the Temporal tracer on
/// it, and the tracing interceptor. Heap-allocated because everything borrows it.
pub const Tracing = struct {
    /// For client.Options.interceptors, worker_interceptors and propagators.
    interceptor: temporal.tracing.Interceptor,
    /// For the application's own spans. Borrowed from the provider.
    tracer: *otel.api.trace.Tracer,

    gpa: std.mem.Allocator,
    io: std.Io,
    /// The SDK's configuration from the environment, installed as its global configuration
    /// for as long as this lives.
    config: *otel.config.Configuration,
    provider: *otel.trace.TracerProvider,
    processor: *otel.trace.BatchingProcessor,
    exporter: Exporter,
    /// Span and trace IDs: the provider draws them with no lock of its own, from many threads.
    source: std.Random.IoSource,
    temporal_tracer: temporal_opentelemetry.Tracer,

    const Exporter = union(enum) {
        stdout: struct {
            exporter: otel.trace.StdOutExporter,
            writer: std.Io.File.Writer,
            buffer: [4096]u8,
        },
        otlp: struct {
            exporter: *otel.trace.OTLPExporter,
            config: *otel.otlp.ConfigOptions,
        },
    };

    /// `gpa` must be thread-safe: spans are started and ended on client, workflow and
    /// activity threads. `environ` is read during the call only. On error, everything created
    /// so far has been released.
    pub fn create(gpa: std.mem.Allocator, io: std.Io, environ: *const std.process.Environ.Map, options: Options) !*Tracing {
        const t = try gpa.create(Tracing);
        errdefer gpa.destroy(t);
        t.gpa = gpa;
        t.io = io;
        t.source = .{ .io = io };

        var env = try environ.clone(gpa);
        defer env.deinit();
        if (env.get("OTEL_SERVICE_NAME") == null) try env.put("OTEL_SERVICE_NAME", options.service_name);

        // The provider reads the global configuration when it is created.
        t.config = try otel.config.Configuration.init(gpa, io, &env);
        otel.config.Configuration.set(t.config);
        errdefer t.config.deinit();

        const use_otlp = env.get("OTEL_EXPORTER_OTLP_ENDPOINT") != null or
            env.get("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") != null or
            options.default_exporter == .otlp;
        const span_exporter = if (use_otlp) blk: {
            const config = try otel.otlp.ConfigOptions.init(gpa, &env);
            errdefer config.deinit();
            const exporter = try otel.trace.OTLPExporter.init(gpa, io, config);
            t.exporter = .{ .otlp = .{ .exporter = exporter, .config = config } };
            break :blk exporter.asSpanExporter();
        } else blk: {
            t.exporter = .{ .stdout = undefined };
            const s = &t.exporter.stdout;
            // Streaming: a positional writer would write from offset 0 of a file stdout shares with
            // stderr (`> out.log 2>&1`), over the program's own log.
            s.writer = std.Io.File.stdout().writerStreaming(io, &s.buffer);
            s.exporter = .init(&s.writer.interface);
            break :blk s.exporter.asSpanExporter();
        };
        errdefer t.deinitExporter();

        // OTEL_BSP_* set its queue and batch sizes and its delays.
        const bsp = t.config.trace_config;
        t.processor = try otel.trace.BatchingProcessor.init(gpa, io, span_exporter, .{
            .max_queue_size = bsp.bsp_max_queue_size,
            .scheduled_delay_millis = bsp.bsp_schedule_delay_ms,
            .export_timeout_millis = bsp.bsp_export_timeout_ms,
            .max_export_batch_size = bsp.bsp_max_export_batch_size,
        });
        // Runs after the provider's: once the provider has it, the provider shuts it down.
        errdefer {
            t.processor.asSpanProcessor().shutdown() catch {};
            t.processor.deinit();
        }
        t.provider = try otel.trace.TracerProvider.init(gpa, io, .{ .Random = .init(t.source.interface()) });
        errdefer t.provider.deinit();
        try t.provider.addSpanProcessor(t.processor.asSpanProcessor());

        t.tracer = try t.provider.getTracer(.{ .name = options.scope_name });
        t.temporal_tracer = .init(gpa, io, t.tracer);
        t.interceptor = .init(t.temporal_tracer.tracer());
        return t;
    }

    /// Shuts the provider down, which exports every ended span, then frees everything. Call
    /// it after the workers and the client that use `interceptor` are closed.
    pub fn destroy(t: *Tracing) void {
        t.provider.shutdown();
        // After the shutdown: the spans it exported borrowed the strings this frees.
        t.temporal_tracer.deinit();
        t.provider.deinit();
        t.processor.deinit();
        t.deinitExporter();
        t.config.deinit();
        t.gpa.destroy(t);
    }

    fn deinitExporter(t: *Tracing) void {
        switch (t.exporter) {
            .stdout => {},
            .otlp => |o| {
                o.exporter.deinit();
                o.config.deinit();
            },
        }
    }
};
