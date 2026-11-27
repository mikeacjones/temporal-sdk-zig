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
/// The forwarding slot this runtime's core logs go through, released by `deinit`.
forward_slot: ?usize = null,

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
    /// Receives each core log record. Null prints logs to stderr instead.
    forward: ?Forward = null,
};

/// A core log callback with its context. `func` runs on the core's threads, possibly several
/// at once; `ctx` must stay valid until `Runtime.deinit` returns, which waits for calls in
/// progress to finish. On the core's threads, `func` runs with SIGPIPE blocked (see `init`).
pub const Forward = struct {
    ctx: *anyopaque,
    func: *const fn (ctx: *anyopaque, record: *const LogRecord) void,
};

/// One core log record, only valid during the `Forward.func` call.
pub const LogRecord = struct {
    level: Level,
    /// The Rust module path the record came from, such as "temporalio_sdk_core::worker".
    target: []const u8,
    message: []const u8,
    timestamp_ms: u64,
    raw: *const c.TemporalCoreForwardedLog,

    pub const Level = enum { trace, debug, info, warn, err };

    /// The record's structured fields as a JSON object.
    pub fn fieldsJson(r: *const LogRecord) []const u8 {
        return bytes.slice(c.temporal_core_forwarded_log_fields_json(r.raw));
    }
};

// The core's log callback carries no context pointer, so each runtime that forwards logs
// takes one of a fixed set of slots, each with its own C callback that finds the slot's
// `Forward`.
pub const max_forwarders = 32;

const ForwardSlot = struct {
    forward: Forward = undefined,
    claimed: std.atomic.Value(bool) = .init(false),
    /// Set while the slot's runtime may forward; `releaseForwardSlot` clears it, then waits
    /// for `busy` (callbacks in progress) to drain before the slot's context may be freed.
    live: std.atomic.Value(bool) = .init(false),
    busy: std.atomic.Value(u32) = .init(0),
    /// The core objects `release` keeps for the slot's runtime. A spin mutex, because
    /// `release` runs on the core's threads, which have no `Io`; it is held only to check
    /// `live` and append, or to take the list.
    kept_mutex: std.atomic.Mutex = .unlocked,
    kept: std.ArrayList(CoreObject) = .empty,
};

var forward_slots: [max_forwarders]ForwardSlot = @splat(.{});

const forward_callbacks = blk: {
    var callbacks: [max_forwarders]c.TemporalCoreForwardedLogCallback = undefined;
    for (&callbacks, 0..) |*cb, i| cb.* = &ForwardTrampoline(i).call;
    break :blk callbacks;
};

fn ForwardTrampoline(comptime i: usize) type {
    return struct {
        fn call(level: c.TemporalCoreForwardedLogLevel, raw: ?*const c.TemporalCoreForwardedLog) callconv(.c) void {
            const slot = &forward_slots[i];
            _ = slot.busy.fetchAdd(1, .seq_cst);
            defer _ = slot.busy.fetchSub(1, .release);
            if (!slot.live.load(.seq_cst)) return;
            const log = raw orelse return;
            const record: LogRecord = .{
                .level = switch (level) {
                    c.TemporalCoreForwardedLogLevel_Trace => .trace,
                    c.TemporalCoreForwardedLogLevel_Debug => .debug,
                    c.TemporalCoreForwardedLogLevel_Info => .info,
                    c.TemporalCoreForwardedLogLevel_Warn => .warn,
                    else => .err,
                },
                .target = bytes.slice(c.temporal_core_forwarded_log_target(log)),
                .message = bytes.slice(c.temporal_core_forwarded_log_message(log)),
                .timestamp_ms = c.temporal_core_forwarded_log_timestamp_millis(log),
                .raw = log,
            };
            slot.forward.func(slot.forward.ctx, &record);
        }
    };
}

fn claimForwardSlot(forward: Forward) ?usize {
    for (&forward_slots, 0..) |*slot, i| {
        if (slot.claimed.cmpxchgStrong(false, true, .acquire, .monotonic) != null) continue;
        slot.forward = forward;
        slot.live.store(true, .seq_cst);
        return i;
    }
    return null;
}

fn releaseForwardSlot(i: usize) void {
    const slot = &forward_slots[i];
    slot.live.store(false, .seq_cst);
    while (slot.busy.load(.seq_cst) != 0) std.atomic.spinLoopHint();
    slot.claimed.store(false, .release);
}

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
        /// Hand metrics to SDK code (see `metric.CustomMeter`). `init` takes ownership: the
        /// core frees it through its own `meter_free` callback, and `init` does so on errors
        /// found before the core sees it.
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

    /// `buckets` must be finite (no NaN or infinity) and strictly increasing (sorted, no
    /// duplicates); an empty list keeps the exporter's default buckets. `init` returns
    /// `error.InvalidOptions` otherwise.
    pub const BucketOverride = struct {
        metric: []const u8,
        buckets: []const f64,
    };
};

pub const InitError = error{
    OutOfMemory,
    InvalidOptions,
    RuntimeInitFailed,
    /// `max_forwarders` runtimes that forward core logs already exist in this process.
    TooManyLogForwarders,
};

/// Creates a runtime. On `error.RuntimeInitFailed`, `failure` (if given) receives the core's
/// message, allocated with `gpa`; the caller frees it. On `error.InvalidOptions` from a histogram
/// bucket override, `failure` (if given) receives a message naming the metric, allocated with
/// `gpa`.
///
/// Outside Windows, the core's threads run with SIGPIPE blocked, so a write by sdk-core to a
/// connection its peer has closed fails with EPIPE instead of raising SIGPIPE, whose default
/// action ends the process. The process's SIGPIPE disposition is not changed, and the calling
/// thread's signal mask is the same when `init` returns as before. Code the core runs on its
/// threads, such as a `Forward.func` or a custom metric meter's callbacks, runs with SIGPIPE
/// blocked.
pub fn init(gpa: Allocator, options: Options, failure: ?*?[]u8) InitError!Runtime {
    var custom_meter: ?*const c.TemporalCoreCustomMetricMeter = if (options.metrics) |m| switch (m.exporter) {
        .custom => |meter| meter,
        else => null,
    } else null;
    errdefer if (custom_meter) |m| m.meter_free.?(m);

    // Everything the core reads from the options only needs to live through this call.
    var arena_state: std.heap.ArenaAllocator = .init(gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    if (options.worker_heartbeat_interval_ms != 0 and
        (options.worker_heartbeat_interval_ms < 1000 or options.worker_heartbeat_interval_ms > 60_000))
        return error.InvalidOptions;

    var telemetry: c.TemporalCoreTelemetryOptions = .{ .logging = null, .metrics = null };
    var logging: c.TemporalCoreLoggingOptions = undefined;
    var forward_slot: ?usize = null;
    errdefer if (forward_slot) |i| releaseForwardSlot(i);
    if (options.logging) |l| {
        logging = .{ .filter = bytes.ref(l.filter), .forward_to = null };
        if (l.forward) |f| {
            forward_slot = claimForwardSlot(f) orelse return error.TooManyLogForwarders;
            logging.forward_to = forward_callbacks[forward_slot.?];
        }
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
                    .histogram_bucket_overrides = try bucketOverrides(gpa, arena, p.histogram_bucket_overrides, failure),
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
                otel.histogram_bucket_overrides = try bucketOverrides(gpa, arena, o.histogram_bucket_overrides, failure);
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

    // From here the core owns the custom meter, on failure too.
    custom_meter = null;
    // sdk-core writes sockets with plain `writev`, which raises SIGPIPE when the peer has
    // closed, and a process that doesn't ignore or handle SIGPIPE then dies (Linux; on macOS
    // the core's sockets set SO_NOSIGPIPE). The process's disposition isn't the SDK's to
    // change, so SIGPIPE is blocked on this thread while the core creates its runtime, and
    // the core's threads inherit that mask, which makes such a write fail with EPIPE:
    // - tokio's multi-thread runtime starts its worker threads in `build()`, here, from this
    //   thread (tokio's worker.rs `launch`, through blocking/pool.rs `spawn_thread`), and the
    //   C bridge builds it inside this call with no thread-start hook;
    // - later blocking-pool threads are started by tasks already on those threads (the bridge
    //   has no `block_on`, the core no `block_in_place`), so they inherit it too.
    // Processes the core spawns (the dev and test servers) inherit it as well: Rust's
    // `Command` resets the disposition but keeps the mask, and those servers handle EPIPE.
    // The previous mask, not an unblock, is restored, so a caller that had SIGPIPE blocked
    // keeps it blocked.
    var previous: std.posix.sigset_t = undefined;
    if (builtin.os.tag != .windows) {
        var pipe = std.posix.sigemptyset();
        std.posix.sigaddset(&pipe, .PIPE);
        threadSigmask(std.posix.SIG.BLOCK, &pipe, &previous);
    }
    const result = c.temporal_core_runtime_new(&core_options);
    if (builtin.os.tag != .windows) threadSigmask(std.posix.SIG.SETMASK, &previous, null);
    const ptr = result.runtime orelse return error.RuntimeInitFailed;
    if (bytes.Owned.take(ptr, result.fail)) |fail| {
        // The core returns a runtime even on failure; it may only be used to free the message.
        if (failure) |f| f.* = fail.dupe(gpa) catch null;
        fail.deinit();
        c.temporal_core_runtime_free(ptr);
        return error.RuntimeInitFailed;
    }
    return .{ .ptr = ptr, .forward_slot = forward_slot };
}

/// Changes this thread's signal mask only. On macOS, sigprocmask changes every thread's mask.
fn threadSigmask(how: c_int, set: *const std.posix.sigset_t, old: ?*std.posix.sigset_t) void {
    if (builtin.link_libc) {
        var ignored: std.posix.sigset_t = undefined;
        _ = std.c.pthread_sigmask(how, set, old orelse &ignored);
    } else {
        std.posix.sigprocmask(@intCast(how), set, old);
    }
}

/// A core object sdk-core built holding a clone of a runtime.
pub const CoreObject = union(enum) {
    connection: *c.TemporalCoreConnection,
    worker: *c.TemporalCoreWorker,
    ephemeral_server: *c.TemporalCoreEphemeralServer,

    fn free(object: CoreObject) void {
        switch (object) {
            .connection => |p| c.temporal_core_client_free(p),
            .worker => |p| c.temporal_core_worker_free(p),
            .ephemeral_server => |p| c.temporal_core_ephemeral_server_free(p),
        }
    }
};

// Each connection, worker and ephemeral server holds a clone of the core runtime, and
// sdk-core's `Drop for Runtime` turns log forwarding off for every clone when any one is
// dropped: freeing one such object would end forwarding for the whole runtime. So
// `release` keeps those objects of a forwarding runtime until `deinit` or `abandon`.
// Known limits, inside sdk-core, where a clone is dropped that no Zig object holds, so
// forwarding ends then:
// - a connection with a gRPC override drops one for each call through the override and when
//   its connect fails (only tests use overrides);
// - a worker call that fails drops a temporary one to allocate its failure message: a failed
//   `validate`, poll, completion, `finalizeShutdown` or `replaceClient`, and an activity
//   heartbeat that doesn't decode.
const kept_allocator = std.heap.smp_allocator;

/// Frees `object`, one of this runtime's: at once when the runtime forwards no logs or has
/// stopped forwarding (`abandon`), otherwise when `deinit` or `abandon` runs, so that its
/// runtime clone isn't dropped while forwarding goes on. A kept connection holds its socket
/// until then. A worker must have been shut down (`Worker.finalizeShutdown`) first, or it
/// stays registered with its client while kept. If keeping `object` fails for lack of
/// memory, it is freed at once and forwarding ends: the only way this loses forwarding.
/// Callable from any thread, the core's included.
pub fn release(r: Runtime, object: CoreObject) void {
    const i = r.forward_slot orelse return object.free();
    const slot = &forward_slots[i];
    const kept = keep: {
        while (!slot.kept_mutex.tryLock()) std.atomic.spinLoopHint();
        defer slot.kept_mutex.unlock();
        if (!slot.live.load(.seq_cst)) break :keep false;
        slot.kept.append(kept_allocator, object) catch break :keep false;
        break :keep true;
    };
    if (!kept) object.free();
}

/// Stops keeping objects for the slot's runtime (`live` must be clear) and frees the kept
/// ones: workers, then ephemeral servers, then connections.
fn freeKept(i: usize) void {
    const slot = &forward_slots[i];
    var list = list: {
        while (!slot.kept_mutex.tryLock()) std.atomic.spinLoopHint();
        defer slot.kept_mutex.unlock();
        std.debug.assert(!slot.live.load(.seq_cst));
        const list = slot.kept;
        slot.kept = .empty;
        break :list list;
    };
    defer list.deinit(kept_allocator);
    for ([_]std.meta.Tag(CoreObject){ .worker, .ephemeral_server, .connection }) |tag| {
        for (list.items) |object| if (object == tag) object.free();
    }
}

/// Frees the objects `release` kept, then the runtime. Once this returns, its log forwarding
/// callback is no longer running and won't be called again.
pub fn deinit(r: Runtime) void {
    if (r.forward_slot) |i| {
        // Freeing the first kept object ends forwarding anyway; this stops `release` keeping
        // more.
        forward_slots[i].live.store(false, .seq_cst);
        freeKept(i);
    }
    c.temporal_core_runtime_free(r.ptr);
    if (r.forward_slot) |i| releaseForwardSlot(i);
}

/// Gives up the runtime without freeing it, for an owner that can't free it because a core
/// task still uses it: its log forwarding stops (once this returns, the forwarding callback
/// isn't running and forwards nothing more), the objects `release` kept are freed, and the
/// runtime, its threads and its forwarding slot stay in use for the rest of the process.
pub fn abandon(r: Runtime) void {
    const i = r.forward_slot orelse return;
    const slot = &forward_slots[i];
    slot.live.store(false, .seq_cst);
    while (slot.busy.load(.seq_cst) != 0) std.atomic.spinLoopHint();
    // Still claimed: the core may still call this slot's callback, which must not reach
    // another runtime's sink.
    freeKept(i);
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

/// `metric1\n1,2,3\nmetric2\n4,5` as the core expects. Every override is checked first: the
/// core panics (aborting the process) on buckets that aren't finite and strictly increasing.
/// On `error.InvalidOptions`, `failure` (if given) receives a message naming the metric,
/// allocated with `gpa`, or stays null when that allocation fails.
fn bucketOverrides(gpa: Allocator, arena: Allocator, overrides: []const Metrics.BucketOverride, failure: ?*?[]u8) InitError!bytes.Ref {
    for (overrides) |o| {
        for (o.buckets, 0..) |b, i| {
            if (!std.math.isFinite(b)) return rejectBuckets(gpa, failure, "histogram bucket override for metric \"{s}\": bucket {d} is {d}; buckets must be finite and strictly increasing", .{ o.metric, i, b });
            // -0.0 after 0.0 compares equal, so it counts as a duplicate.
            if (i > 0 and b <= o.buckets[i - 1]) return rejectBuckets(gpa, failure, "histogram bucket override for metric \"{s}\": bucket {d} ({d}) is not above bucket {d} ({d}); buckets must be finite and strictly increasing", .{ o.metric, i, b, i - 1, o.buckets[i - 1] });
        }
    }
    // An empty list keeps the exporter's default buckets; the core can't parse one, so it is
    // left out.
    const entries = try arena.alloc(bytes.MetadataEntry, overrides.len);
    var n: usize = 0;
    for (overrides) |o| {
        if (o.buckets.len == 0) continue;
        const e = &entries[n];
        n += 1;
        var buckets: std.Io.Writer.Allocating = .init(arena);
        for (o.buckets, 0..) |b, i| {
            if (i > 0) buckets.writer.writeByte(',') catch return error.OutOfMemory;
            buckets.writer.print("{d}", .{b}) catch return error.OutOfMemory;
        }
        e.* = .{ .key = o.metric, .value = buckets.written() };
    }
    return newlineMap(arena, entries[0..n]) catch |err| mapEncodeError(err);
}

fn rejectBuckets(gpa: Allocator, failure: ?*?[]u8, comptime fmt: []const u8, args: anytype) InitError {
    if (failure) |f| f.* = std.fmt.allocPrint(gpa, fmt, args) catch null;
    return error.InvalidOptions;
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

fn expectBucketsRejected(exporter: Metrics.Exporter, buckets: []const f64) !void {
    var message: ?[]u8 = null;
    defer if (message) |m| testing.allocator.free(m);
    var with_buckets = exporter;
    const overrides: []const Metrics.BucketOverride = &.{
        .{ .metric = "fine_metric", .buckets = &.{ 1, 2 } },
        .{ .metric = "bad_metric", .buckets = buckets },
    };
    switch (with_buckets) {
        .prometheus => |*p| p.histogram_bucket_overrides = overrides,
        .opentelemetry => |*o| o.histogram_bucket_overrides = overrides,
        .custom => unreachable,
    }
    try testing.expectError(error.InvalidOptions, Runtime.init(testing.allocator, .{
        .logging = .{ .filter = "temporalio_sdk_core=WARN", .forward = .{ .ctx = @ptrCast(&ignore_log_ctx), .func = ignoreLog } },
        .metrics = .{ .exporter = with_buckets },
    }, &message));
    const m = message orelse return error.TestExpectedMessage;
    try testing.expect(std.mem.indexOf(u8, m, "\"bad_metric\"") != null);
    try testing.expect(std.mem.indexOf(u8, m, "finite and strictly increasing") != null);
}

var ignore_log_ctx: u8 = 0;

fn ignoreLog(_: *anyopaque, _: *const LogRecord) void {}

test "histogram bucket overrides that aren't finite and strictly increasing are rejected before the core" {
    const exporters = [_]Metrics.Exporter{
        .{ .prometheus = .{ .bind_address = "127.0.0.1:0" } },
        .{ .opentelemetry = .{ .url = "http://localhost:4317" } },
    };
    const nan = std.math.nan(f64);
    const inf = std.math.inf(f64);
    const shapes = [_][]const f64{
        &.{ 2, 1 },
        &.{ 1, 1 },
        &.{ 0.0, -0.0 },
        &.{nan},
        &.{inf},
        &.{-inf},
        &.{ 1, nan },
        &.{ 1, 2, inf },
    };
    // More rejections than there are log forwarding slots: each must release its slot.
    comptime std.debug.assert(3 * exporters.len * shapes.len > max_forwarders);
    for (0..3) |_| for (exporters) |e| for (shapes) |b| try expectBucketsRejected(e, b);
}

test "a rejected bucket override names the metric and the offending buckets" {
    var message: ?[]u8 = null;
    defer if (message) |m| testing.allocator.free(m);
    try testing.expectError(error.InvalidOptions, Runtime.init(testing.allocator, .{
        .metrics = .{ .exporter = .{ .opentelemetry = .{
            .url = "http://localhost:4317",
            .histogram_bucket_overrides = &.{.{ .metric = "workflow_task_latency", .buckets = &.{ 1, 5, 3 } }},
        } } },
    }, &message));
    try testing.expectEqualStrings(
        "histogram bucket override for metric \"workflow_task_latency\": bucket 2 (3) is not above bucket 1 (5); buckets must be finite and strictly increasing",
        message.?,
    );
    testing.allocator.free(message.?);
    message = null;
    try testing.expectError(error.InvalidOptions, Runtime.init(testing.allocator, .{
        .metrics = .{ .exporter = .{ .prometheus = .{
            .bind_address = "127.0.0.1:0",
            .histogram_bucket_overrides = &.{.{ .metric = "m", .buckets = &.{std.math.nan(f64)} }},
        } } },
    }, &message));
    try testing.expectEqualStrings(
        "histogram bucket override for metric \"m\": bucket 0 is nan; buckets must be finite and strictly increasing",
        message.?,
    );
}

test "a rejected bucket override without a failure slot still returns InvalidOptions" {
    try testing.expectError(error.InvalidOptions, Runtime.init(testing.allocator, .{
        .metrics = .{ .exporter = .{ .prometheus = .{
            .bind_address = "127.0.0.1:0",
            .histogram_bucket_overrides = &.{.{ .metric = "m", .buckets = &.{ 2, 1 } }},
        } } },
    }, null));
    // When allocating the message fails, the error is still InvalidOptions and nothing leaks.
    var message: ?[]u8 = null;
    var failing: std.testing.FailingAllocator = .init(testing.allocator, .{ .fail_index = 0 });
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    try testing.expectError(error.InvalidOptions, bucketOverrides(failing.allocator(), arena.allocator(), &.{
        .{ .metric = "m", .buckets = &.{ 2, 1 } },
    }, &message));
    try testing.expect(message == null);
}

test "valid and empty bucket overrides are accepted by both exporters" {
    const overrides: []const Metrics.BucketOverride = &.{
        .{ .metric = "workflow_task_latency", .buckets = &.{ 1, 5, 25.5 } },
        .{ .metric = "activity_latency", .buckets = &.{} },
        .{ .metric = "negative_start", .buckets = &.{ -1, 0, 1 } },
    };
    var message: ?[]u8 = null;
    defer if (message) |m| testing.allocator.free(m);
    const prom = try Runtime.init(testing.allocator, .{ .metrics = .{ .exporter = .{ .prometheus = .{
        .bind_address = "127.0.0.1:0",
        .histogram_bucket_overrides = overrides,
    } } } }, &message);
    prom.deinit();
    const otel = try Runtime.init(testing.allocator, .{ .metrics = .{ .exporter = .{ .opentelemetry = .{
        .url = "http://localhost:4317",
        .histogram_bucket_overrides = overrides,
    } } } }, &message);
    otel.deinit();
    try testing.expect(message == null);
}

test "bucket overrides are encoded as the core expects" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const r = try bucketOverrides(testing.allocator, arena.allocator(), &.{
        .{ .metric = "a", .buckets = &.{ 1, 2.5 } },
        .{ .metric = "empty", .buckets = &.{} },
        .{ .metric = "b", .buckets = &.{100} },
    }, null);
    try testing.expectEqualStrings("a\n1,2.5\nb\n100", bytes.slice(r));
}

test "release keeps a forwarding runtime's core objects until deinit, and frees a quiet runtime's at once" {
    const Worker = @import("Worker.zig");
    const worker_options: Worker.Options = .{
        .namespace = "default",
        .task_queue = "runtime-release",
        .task_types = .{ .local_activities = false, .remote_activities = false, .nexus = false },
        .max_cached_workflows = 1,
    };
    var ctx: u8 = 0;
    for ([_]bool{ true, false }) |forwarding| {
        const rt = try Runtime.init(testing.allocator, .{
            .logging = .{ .forward = if (forwarding) .{ .ctx = &ctx, .func = ignoreLog } else null },
            .worker_heartbeat_interval_ms = 0,
        }, null);
        var replayer = try Worker.initReplayer(testing.allocator, rt, worker_options, null);
        replayer.finishPushing();
        replayer.worker.initiateShutdown();
        while (try replayer.worker.pollWorkflowActivation(testing.allocator, testing.io, null)) |polled| polled.deinit();
        try replayer.worker.finalizeShutdown(testing.allocator, testing.io, null);
        replayer.worker.deinit();
        if (forwarding) {
            const slot = &forward_slots[rt.forward_slot.?];
            try testing.expectEqual(1, slot.kept.items.len);
            rt.deinit();
            try testing.expectEqual(0, slot.kept.items.len);
        } else {
            try testing.expectEqual(null, rt.forward_slot);
            rt.deinit();
        }
    }
}

/// The test thread's signal mask, read without changing it.
fn currentSigmask() std.posix.sigset_t {
    var empty = std.posix.sigemptyset();
    var mask: std.posix.sigset_t = undefined;
    threadSigmask(std.posix.SIG.BLOCK, &empty, &mask);
    return mask;
}

/// On its first call from a thread other than the test's, writes to a pipe whose reader has
/// closed and records the errno.
const ClosedPipeWrite = struct {
    test_thread: std.Thread.Id,
    taken: std.atomic.Value(bool) = .init(false),
    /// The write's errno, or `unset` until it has run.
    errno: std.atomic.Value(u16) = .init(unset),
    /// Whether SIGPIPE was blocked on the thread that wrote.
    blocked: std.atomic.Value(bool) = .init(false),

    const unset = std.math.maxInt(u16);

    fn func(ctx: *anyopaque, _: *const LogRecord) void {
        const w: *ClosedPipeWrite = @ptrCast(@alignCast(ctx));
        if (std.Thread.getCurrentId() == w.test_thread) return;
        if (w.taken.swap(true, .seq_cst)) return;
        const mask = currentSigmask();
        w.blocked.store(std.posix.sigismember(&mask, .PIPE), .seq_cst);
        var fds: [2]std.posix.fd_t = undefined;
        if (std.c.pipe(&fds) != 0) {
            w.errno.store(@backingInt(std.posix.errno(-1)), .seq_cst);
            return;
        }
        _ = std.c.close(fds[0]);
        const rc = std.c.write(fds[1], "x", 1);
        const e: std.posix.E = if (rc < 0) std.posix.errno(rc) else .SUCCESS;
        _ = std.c.close(fds[1]);
        w.errno.store(@backingInt(e), .seq_cst);
    }
};

test "a write on a core thread to a pipe whose reader has closed fails with EPIPE" {
    if (builtin.os.tag == .windows) return error.SkipZigTest;
    var w: ClosedPipeWrite = .{ .test_thread = std.Thread.getCurrentId() };
    const before = currentSigmask();
    const rt = try Runtime.init(testing.allocator, .{
        .logging = .{ .filter = "hyper_util=TRACE", .forward = .{ .ctx = &w, .func = ClosedPipeWrite.func } },
        .worker_heartbeat_interval_ms = 0,
    }, null);
    defer rt.deinit();
    const after = currentSigmask();
    try testing.expectEqualSlices(u8, std.mem.asBytes(&before), std.mem.asBytes(&after));

    // hyper_util logs on a core thread while it connects, and the core completes the connect
    // only after that, so the write has run once `connect` returns. Nothing listens on port 1.
    const Connection = @import("Connection.zig");
    var failure: ?[]u8 = null;
    defer if (failure) |f| testing.allocator.free(f);
    try testing.expectError(error.ConnectFailed, Connection.connect(testing.allocator, testing.io, rt, .{
        .target_url = "http://127.0.0.1:1",
        .identity = "sigpipe-test",
    }, &failure));
    try testing.expectEqual(@backingInt(std.posix.E.PIPE), w.errno.load(.seq_cst));
    // While a test runs, the runner's `Io.Threaded` handles SIGPIPE with a handler that does
    // nothing, so an unblocked SIGPIPE wouldn't end this process; what ends one is the
    // default action, which the runner restores after each test while the core's threads may
    // still write. The mask is what keeps the signal from them either way.
    try testing.expect(w.blocked.load(.seq_cst));
}

test "Runtime.init leaves the calling thread's signal mask as it found it, on success and on failure" {
    if (builtin.os.tag == .windows) return error.SkipZigTest;
    // Once with SIGPIPE unblocked on this thread, once with it blocked.
    var pipe = std.posix.sigemptyset();
    std.posix.sigaddset(&pipe, .PIPE);
    var entry: std.posix.sigset_t = undefined;
    threadSigmask(std.posix.SIG.UNBLOCK, &pipe, &entry);
    defer threadSigmask(std.posix.SIG.SETMASK, &entry, null);
    for ([_]bool{ false, true }) |blocked| {
        if (blocked) threadSigmask(std.posix.SIG.BLOCK, &pipe, null);
        const before = currentSigmask();
        const rt = try Runtime.init(testing.allocator, .{ .worker_heartbeat_interval_ms = 0 }, null);
        rt.deinit();
        var after = currentSigmask();
        try testing.expectEqualSlices(u8, std.mem.asBytes(&before), std.mem.asBytes(&after));
        try testing.expectError(error.RuntimeInitFailed, Runtime.init(testing.allocator, .{
            .metrics = .{ .exporter = .{ .prometheus = .{ .bind_address = "not an address" } } },
        }, null));
        after = currentSigmask();
        try testing.expectEqualSlices(u8, std.mem.asBytes(&before), std.mem.asBytes(&after));
    }
}
