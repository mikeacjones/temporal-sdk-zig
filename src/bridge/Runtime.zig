//! The sdk-core runtime: the thread pool, telemetry (logging and metrics), and process-wide
//! worker settings that clients and workers are created from. A runtime must outlive every
//! client, worker, and byte array created from it.

const Runtime = @This();

const std = @import("std");
const builtin = @import("builtin");
const c = @import("core_c");
const bytes = @import("bytes.zig");
const Allocator = std.mem.Allocator;

ptr: *c.TemporalCoreRuntime,

pub const Options = struct {
    logging: ?Logging = null,
    metrics: ?Metrics = null,
    /// How often workers report heartbeats to the server, in milliseconds. Must be between
    /// 1000 and 60000; 0 turns worker heartbeats off.
    worker_heartbeat_interval_ms: u64 = 60_000,
    /// Leave runtime, hosting, and platform details out of worker heartbeats.
    disable_environment_info: bool = false,
};

pub const Logging = struct {
    /// A tracing filter for core logs, such as "temporalio_sdk_core=WARN,temporalio_client=WARN".
    filter: []const u8 = "WARN",
    /// Receives each core log record. The record is only valid during the call, which may
    /// happen on any thread. Null prints logs to stderr instead.
    forward: c.TemporalCoreForwardedLogCallback = null,
};

pub const Metrics = struct {
    exporter: Exporter,
    /// Add a `service_name` attribute to every metric.
    attach_service_name: bool = true,
    /// Attributes added to every metric.
    global_tags: []const bytes.MetadataEntry = &.{},
    /// Prefix for metric names. Empty uses the core default ("temporal_").
    prefix: []const u8 = "",

    pub const Exporter = union(enum) {
        /// Serve metrics for Prometheus to scrape.
        prometheus: Prometheus,
        /// Push metrics to an OpenTelemetry collector.
        opentelemetry: OpenTelemetry,
        /// Hand metrics to SDK code. The core frees it through its own `meter_free` callback.
        custom: *const c.TemporalCoreCustomMetricMeter,
    };

    pub const Prometheus = struct {
        /// Address to serve /metrics on, such as "127.0.0.1:9090".
        bind_address: []const u8,
        counters_total_suffix: bool = false,
        unit_suffix: bool = false,
        durations_as_seconds: bool = false,
        histogram_bucket_overrides: []const BucketOverride = &.{},
    };

    pub const OpenTelemetry = struct {
        /// Collector URL, such as "http://localhost:4317".
        url: []const u8,
        headers: []const bytes.MetadataEntry = &.{},
        /// How often to export, in milliseconds. 0 uses the core default.
        metric_periodicity_ms: u32 = 0,
        temporality: enum { cumulative, delta } = .cumulative,
        protocol: enum { grpc, http } = .grpc,
        durations_as_seconds: bool = false,
        histogram_bucket_overrides: []const BucketOverride = &.{},
    };

    pub const BucketOverride = struct {
        metric: []const u8,
        buckets: []const f64,
    };
};

pub const InitError = error{ OutOfMemory, InvalidOptions, RuntimeInitFailed };

/// Creates a runtime. On `error.RuntimeInitFailed`, `failure` (if given) receives the core's
/// message, allocated with `gpa`; the caller frees it.
pub fn init(gpa: Allocator, options: Options, failure: ?*?[]u8) InitError!Runtime {
    // Everything the core reads from the options only needs to live through this call.
    var arena_state: std.heap.ArenaAllocator = .init(gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    if (options.worker_heartbeat_interval_ms != 0 and
        (options.worker_heartbeat_interval_ms < 1000 or options.worker_heartbeat_interval_ms > 60_000))
        return error.InvalidOptions;

    var telemetry: c.TemporalCoreTelemetryOptions = .{ .logging = null, .metrics = null };
    var logging: c.TemporalCoreLoggingOptions = undefined;
    if (options.logging) |l| {
        logging = .{ .filter = bytes.ref(l.filter), .forward_to = l.forward };
        telemetry.logging = &logging;
    }
    var metrics: c.TemporalCoreMetricsOptions = undefined;
    var prometheus: c.TemporalCorePrometheusOptions = undefined;
    var otel: c.TemporalCoreOpenTelemetryOptions = undefined;
    if (options.metrics) |m| {
        metrics = .{
            .opentelemetry = null,
            .prometheus = null,
            .custom_meter = null,
            .attach_service_name = m.attach_service_name,
            .global_tags = newlineMap(arena, m.global_tags) catch |err| return mapEncodeError(err),
            .metric_prefix = bytes.ref(m.prefix),
        };
        switch (m.exporter) {
            .prometheus => |p| {
                prometheus = .{
                    .bind_address = bytes.ref(p.bind_address),
                    .counters_total_suffix = p.counters_total_suffix,
                    .unit_suffix = p.unit_suffix,
                    .durations_as_seconds = p.durations_as_seconds,
                    .histogram_bucket_overrides = try bucketOverrides(arena, p.histogram_bucket_overrides),
                };
                metrics.prometheus = &prometheus;
            },
            .opentelemetry => |o| {
                otel = std.mem.zeroes(c.TemporalCoreOpenTelemetryOptions);
                otel.url = bytes.ref(o.url);
                otel.headers = newlineMap(arena, o.headers) catch |err| return mapEncodeError(err);
                otel.metric_periodicity_millis = o.metric_periodicity_ms;
                otel.metric_temporality = switch (o.temporality) {
                    .cumulative => c.TemporalCoreOpenTelemetryMetricTemporality_Cumulative,
                    .delta => c.TemporalCoreOpenTelemetryMetricTemporality_Delta,
                };
                otel.protocol = switch (o.protocol) {
                    .grpc => c.TemporalCoreOpenTelemetryProtocol_Grpc,
                    .http => c.TemporalCoreOpenTelemetryProtocol_Http,
                };
                otel.durations_as_seconds = o.durations_as_seconds;
                otel.histogram_bucket_overrides = try bucketOverrides(arena, o.histogram_bucket_overrides);
                otel.exponential_histogram_name_suffix = bytes.ref("");
                metrics.opentelemetry = &otel;
            },
            .custom => |meter| metrics.custom_meter = meter,
        }
        telemetry.metrics = &metrics;
    }

    // Reported in worker heartbeats. There is no Zig runtime type in the core; "native" is
    // the closest.
    const runtime_info: [1]c.TemporalCoreRuntimeInfo = .{.{
        .runtime_type = c.TemporalCoreRuntimeType_Native,
        .version = bytes.ref("zig " ++ builtin.zig_version_string),
    }};
    const core_options: c.TemporalCoreRuntimeOptions = .{
        .telemetry = &telemetry,
        .worker_heartbeat_interval_millis = options.worker_heartbeat_interval_ms,
        .runtime_info = .{ .data = &runtime_info, .size = runtime_info.len },
        .disable_environment_info = options.disable_environment_info,
    };

    const result = c.temporal_core_runtime_new(&core_options);
    const ptr = result.runtime orelse return error.RuntimeInitFailed;
    if (bytes.Owned.take(ptr, result.fail)) |fail| {
        // The core returns a runtime even on failure; it may only be used to free the message.
        if (failure) |f| f.* = fail.dupe(gpa) catch null;
        fail.deinit();
        c.temporal_core_runtime_free(ptr);
        return error.RuntimeInitFailed;
    }
    return .{ .ptr = ptr };
}

pub fn deinit(r: Runtime) void {
    c.temporal_core_runtime_free(r.ptr);
}

fn mapEncodeError(err: bytes.EncodeError) InitError {
    return switch (err) {
        error.OutOfMemory => error.OutOfMemory,
        error.NewlineInKey, error.NewlineInValue => error.InvalidOptions,
    };
}

fn newlineMap(arena: Allocator, entries: []const bytes.MetadataEntry) bytes.EncodeError!bytes.Ref {
    return bytes.newlineMap(arena, entries);
}

/// `metric1\n1,2,3\nmetric2\n4,5` as the core expects.
fn bucketOverrides(arena: Allocator, overrides: []const Metrics.BucketOverride) InitError!bytes.Ref {
    const entries = try arena.alloc(bytes.MetadataEntry, overrides.len);
    for (overrides, entries) |o, *e| {
        var buckets: std.Io.Writer.Allocating = .init(arena);
        for (o.buckets, 0..) |b, i| {
            if (i > 0) buckets.writer.writeByte(',') catch return error.OutOfMemory;
            buckets.writer.print("{d}", .{b}) catch return error.OutOfMemory;
        }
        e.* = .{ .key = o.metric, .value = buckets.written() };
    }
    return newlineMap(arena, entries) catch |err| mapEncodeError(err);
}

// ---------------------------------------------------------------- tests

const testing = std.testing;

test "a runtime with default options starts and stops" {
    const rt = try Runtime.init(testing.allocator, .{}, null);
    rt.deinit();
}

test "a runtime with logging and Prometheus metrics starts" {
    const rt = try Runtime.init(testing.allocator, .{
        .logging = .{ .filter = "temporalio_sdk_core=WARN" },
        .metrics = .{
            .exporter = .{ .prometheus = .{
                .bind_address = "127.0.0.1:0",
                .histogram_bucket_overrides = &.{.{ .metric = "workflow_task_latency", .buckets = &.{ 1, 5, 25.5 } }},
            } },
            .global_tags = &.{.{ .key = "service", .value = "samples" }},
        },
        .worker_heartbeat_interval_ms = 0,
    }, null);
    rt.deinit();
}

test "invalid options fail with the core's message" {
    var message: ?[]u8 = null;
    defer if (message) |m| testing.allocator.free(m);
    try testing.expectError(error.RuntimeInitFailed, Runtime.init(testing.allocator, .{
        .metrics = .{ .exporter = .{ .prometheus = .{ .bind_address = "not an address" } } },
    }, &message));
    try testing.expect(message != null and message.?.len > 0);
}

test "option errors are caught before reaching the core" {
    try testing.expectError(error.InvalidOptions, Runtime.init(testing.allocator, .{ .worker_heartbeat_interval_ms = 500 }, null));
    try testing.expectError(error.InvalidOptions, Runtime.init(testing.allocator, .{
        .metrics = .{
            .exporter = .{ .prometheus = .{ .bind_address = "127.0.0.1:0" } },
            .global_tags = &.{.{ .key = "bad\nkey", .value = "v" }},
        },
    }, null));
}

test "bucket overrides are encoded as the core expects" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const r = try bucketOverrides(arena.allocator(), &.{
        .{ .metric = "a", .buckets = &.{ 1, 2.5 } },
        .{ .metric = "b", .buckets = &.{100} },
    });
    try testing.expectEqualStrings("a\n1,2.5\nb\n100", bytes.slice(r));
}
