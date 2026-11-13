//! The Temporal client: connect to a server, start workflows,
//! and wait for their results.
//!
//!     var client = try temporal.client.Client.connect(gpa, io, try temporal.client.loadDefaultOptions(arena));
//!     defer client.close();
//!     var run = try client.executeWorkflow(HelloWorld, .{ .id = "hello", .task_queue = "hello-world" }, .{"Temporal"});
//!     defer run.deinit();
//!     const greeting = try run.get(arena);
//!
//! Errors follow the SDK's pattern (see `temporal.failure`): `run.get` returns
//! `error.WorkflowFailed` and the failure is read with `run.failure()`. RPC failures are
//! logged with the server's message under the `temporal` log scope. Calls that send or
//! receive payloads return `error.CodecFailed`, also logged, when a payload codec
//! (`Options.codecs`) fails.

const std = @import("std");
const bridge = @import("bridge.zig");
const proto = @import("proto.zig");
const converter = @import("converter.zig");
const search_attributes = @import("search_attributes.zig");
const failure_mod = @import("failure.zig");
const history_mod = @import("history.zig");
const schedule = @import("schedule.zig");
const deployment = @import("deployment.zig");
const workflow = @import("workflow.zig");
const log_mod = @import("log.zig");
const propagation = @import("propagation.zig");
const interceptor = @import("interceptor.zig");
const metrics_mod = @import("metrics.zig");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const Duration = Io.Duration;

const wfs = proto.temporal.api.workflowservice.v1;
const enums = proto.temporal.api.enums.v1;
const log = std.log.scoped(.temporal);

pub const Options = struct {
    /// host:port of the server's frontend.
    host_port: []const u8 = "localhost:7233",
    namespace: []const u8 = "default",
    /// Empty uses "<pid>@<hostname>".
    identity: []const u8 = "",
    api_key: []const u8 = "",
    tls: ?Tls = null,
    /// gRPC headers sent with every call.
    headers: []const bridge.bytes.MetadataEntry = &.{},
    /// Payload codecs, such as `converter.ZlibCodec` or encryption, applied in order to every
    /// payload this client sends and in reverse to every payload it receives (results,
    /// failures, memos, history events), and by every worker on this client to its workflow
    /// and activity payloads. Search attributes are never encoded. A codec that sets
    /// `encodeWithContext` and `decodeWithContext` sees each call's `converter.CodecContext`:
    /// its propagated values and the workflow or activity it is for. Borrowed: the slice and
    /// the codecs must outlive the client and its workers.
    codecs: []const converter.PayloadCodec = &.{},
    /// Where sdk-core's logs go, and the default for every worker on this client (see
    /// `worker.Options.logger`). Null uses `log.Sink.std_log`. Borrowed: it must outlive the
    /// client and its workers. Called concurrently from worker threads and sdk-core's threads.
    logger: ?log_mod.Sink = null,
    /// The least severe sdk-core log level forwarded to `logger`; null leaves sdk-core's logs
    /// off. Logs from libraries sdk-core uses are forwarded from `warn` up.
    core_log_level: ?log_mod.Level = .warn,
    /// Context propagators, run in order: they write the `.context` values of each call that
    /// carries a header into that header, and every worker on this client reads them back out
    /// for its workflows and activities (see `temporal.propagation`). Borrowed: the slice and
    /// the propagators must outlive the client and its workers.
    propagators: []const propagation.ContextPropagator = &.{},
    /// Interceptors of the workflows and activities of every worker on this client, before
    /// the worker's own (`worker.Options.interceptors`). Borrowed: the slice and the
    /// interceptors must outlive the client and its workers.
    worker_interceptors: []const interceptor.Worker = &.{},
    /// Interceptors of every WorkflowService request this client sends, in order (the first
    /// is the outermost); see `interceptor.Client`. `Client.checkHealth` bypasses them.
    /// Borrowed: the slice and the interceptors must outlive the client. Called
    /// concurrently from every thread using the client.
    interceptors: []const interceptor.Client = &.{},
    /// Where metrics go: sdk-core's, from every worker on this client, and those recorded
    /// through `Client.metrics()` and the workflow and activity contexts. Null records none.
    /// See `temporal.metrics`.
    metrics: ?metrics_mod.Options = null,
    /// Keep-alive pings on the connection. The default is sdk-core's: a ping every 30 s,
    /// each answered within 15 s. Null sends no pings.
    keep_alive: ?KeepAlive = .{},
    /// Connect through an HTTP CONNECT proxy. Null connects directly.
    http_connect_proxy: ?HttpConnectProxy = null,
    /// How sdk-core retries failed calls: this client's, and its workers' calls other than
    /// polls, which have their own policy. Null is sdk-core's default policy, the same as
    /// `.{}`.
    retry: ?Retry = null,
    /// Sent to the server with every call, as the `client-name` header. Visible ASCII,
    /// space and tab.
    client_name: []const u8 = bridge.Connection.default_client_name,
    /// Sent to the server with every call, as the `client-version` header. Visible ASCII,
    /// space and tab.
    client_version: []const u8 = bridge.Connection.default_client_version,
    /// How often workers on this client heartbeat to the server: from 1 s to 60 s, or
    /// `.zero` for no heartbeats. Null is sdk-core's 60 s.
    worker_heartbeat_interval: ?Duration = null,
    /// The deadline of each call this client makes, including each retry attempt, and of
    /// `Client.checkHealth`. A call that runs out fails with `error.RpcFailed`, logged with
    /// gRPC status 1 (CANCELLED, "Timeout expired"), or 4 (DEADLINE_EXCEEDED) when the server
    /// gives up first, and isn't retried. Calls that wait on the server, such as
    /// waiting for a workflow's result or an update's outcome, aren't limited by it, and
    /// workers' own calls aren't affected. Null keeps sdk-core's default of 30 s. Must be
    /// positive; under 1 ms counts as 1 ms.
    rpc_timeout: ?Duration = null,

    pub const KeepAlive = struct {
        /// Time between pings. Must be positive.
        interval: Duration = .fromMilliseconds(bridge_keep_alive.interval_ms),
        /// How long to wait for each ping's answer before the connection is dropped. Must be
        /// positive.
        timeout: Duration = .fromMilliseconds(bridge_keep_alive.timeout_ms),

        const bridge_keep_alive: bridge.Connection.Options.KeepAlive = .{};
    };

    pub const HttpConnectProxy = struct {
        /// host:port of the proxy.
        host_port: []const u8,
        /// Basic auth credentials for the proxy: both or neither.
        username: []const u8 = "",
        password: []const u8 = "",
    };

    /// Each field defaults to sdk-core's default policy, so setting one changes only that
    /// one. Durations must not be negative.
    pub const Retry = struct {
        /// The wait before the first retry.
        initial_interval: Duration = .fromMilliseconds(100),
        /// The fraction of each wait added or subtracted at random. Not negative.
        randomization_factor: f64 = 0.2,
        /// How much the wait grows after each retry, up to `max_interval`. Not negative.
        multiplier: f64 = 1.7,
        max_interval: Duration = .fromMilliseconds(5_000),
        /// How long to keep retrying a call in all. Zero means no limit.
        max_elapsed_time: Duration = .fromMilliseconds(10_000),
        /// Zero means no limit.
        max_retries: usize = 10,
    };

    pub const Tls = struct {
        /// PEM; empty uses the system roots.
        server_root_ca_cert: []const u8 = "",
        server_name: []const u8 = "",
        /// PEM client certificate and key for mutual TLS: both or neither.
        client_cert: []const u8 = "",
        client_private_key: []const u8 = "",
        /// Files to read the PEM above from instead, each time the client connects (`connect`
        /// and `Client.reconnect`), so a reconnect picks up certificates rotated on disk. Each
        /// path excludes its PEM field.
        server_root_ca_cert_path: []const u8 = "",
        client_cert_path: []const u8 = "",
        client_private_key_path: []const u8 = "",
    };

    /// The options the connection uses, for `Client.reconnect`.
    pub fn connectionOptions(o: Options) ConnectionOptions {
        return .{
            .host_port = o.host_port,
            .api_key = o.api_key,
            .tls = o.tls,
            .headers = o.headers,
            .keep_alive = o.keep_alive,
            .http_connect_proxy = o.http_connect_proxy,
            .retry = o.retry,
            .client_name = o.client_name,
            .client_version = o.client_version,
        };
    }
};

/// What `Client.reconnect` replaces: the `Options` fields the connection is made with, each
/// as documented there. The others (namespace, identity, codecs, `rpc_timeout`,
/// `worker_heartbeat_interval`, ...) stay as `connect` set them.
pub const ConnectionOptions = struct {
    host_port: []const u8 = "localhost:7233",
    api_key: []const u8 = "",
    tls: ?Options.Tls = null,
    headers: []const bridge.bytes.MetadataEntry = &.{},
    keep_alive: ?Options.KeepAlive = .{},
    http_connect_proxy: ?Options.HttpConnectProxy = null,
    retry: ?Options.Retry = null,
    client_name: []const u8 = bridge.Connection.default_client_name,
    client_version: []const u8 = bridge.Connection.default_client_version,
};

/// Why an option was rejected, naming the option and its value, for the caller to log.
const Rejection = struct {
    buf: [256]u8 = undefined,
    message: []const u8 = "",

    fn reject(r: *Rejection, comptime fmt: []const u8, args: anytype) error{InvalidOptions} {
        r.message = std.fmt.bufPrint(&r.buf, fmt, args) catch r.buf[0..];
        return error.InvalidOptions;
    }
};

/// Milliseconds for the core: a positive duration under 1 ms counts as 1 ms, and a duration
/// too long for a u64 as the longest.
fn durationMs(d: Duration) u64 {
    if (d.nanoseconds <= 0) return 0;
    return std.math.cast(u64, @max(@divTrunc(d.nanoseconds, std.time.ns_per_ms), 1)) orelse std.math.maxInt(u64);
}

/// The bridge's options for connecting to `url` with `options`. `tls` is `options.tls`
/// with its files read. Checks the options sdk-core would otherwise silently drop or
/// misread, filling `rejection` when it returns `error.InvalidOptions`.
fn bridgeConnectionOptions(url: []const u8, identity: []const u8, options: ConnectionOptions, tls: ?bridge.Connection.Options.Tls, rejection: *Rejection) error{InvalidOptions}!bridge.Connection.Options {
    var keep_alive: ?bridge.Connection.Options.KeepAlive = null;
    if (options.keep_alive) |k| {
        if (k.interval.nanoseconds <= 0) return rejection.reject("Options.keep_alive.interval must be positive, not {f}", .{k.interval});
        if (k.timeout.nanoseconds <= 0) return rejection.reject("Options.keep_alive.timeout must be positive, not {f}", .{k.timeout});
        keep_alive = .{ .interval_ms = durationMs(k.interval), .timeout_ms = durationMs(k.timeout) };
    }
    var proxy: ?bridge.Connection.Options.HttpConnectProxy = null;
    if (options.http_connect_proxy) |p| {
        if (p.host_port.len == 0) return rejection.reject("Options.http_connect_proxy.host_port is empty; set the proxy's host:port, or set http_connect_proxy to null to connect directly", .{});
        if ((p.username.len == 0) != (p.password.len == 0))
            return rejection.reject("Options.http_connect_proxy.username and .password go together: set both for basic auth, or neither (proxy {s})", .{p.host_port});
        proxy = .{ .target_host = p.host_port, .username = p.username, .password = p.password };
    }
    var retry: ?bridge.Connection.Options.Retry = null;
    if (options.retry) |r| {
        inline for (.{ "initial_interval", "max_interval", "max_elapsed_time" }) |field| {
            const d = @field(r, field);
            if (d.nanoseconds < 0) return rejection.reject("Options.retry." ++ field ++ " must not be negative, not {f}", .{d});
        }
        inline for (.{ "randomization_factor", "multiplier" }) |field| {
            const v = @field(r, field);
            if (!std.math.isFinite(v) or v < 0) return rejection.reject("Options.retry." ++ field ++ " must be finite and not negative, not {d}", .{v});
        }
        retry = .{
            .initial_interval_ms = durationMs(r.initial_interval),
            .randomization_factor = r.randomization_factor,
            .multiplier = r.multiplier,
            .max_interval_ms = durationMs(r.max_interval),
            .max_elapsed_time_ms = durationMs(r.max_elapsed_time),
            .max_retries = r.max_retries,
        };
    }
    inline for (.{ "client_name", "client_version" }) |field| {
        const v = @field(options, field);
        if (v.len == 0) return rejection.reject("Options." ++ field ++ " is empty", .{});
        for (v) |ch| if (ch != '\t' and (ch < ' ' or ch > '~'))
            return rejection.reject("Options." ++ field ++ " \"{s}\" has a byte (0x{x:0>2}) other than visible ASCII, space or tab", .{ v, ch });
    }
    return .{
        .target_url = url,
        .identity = identity,
        .client_name = options.client_name,
        .client_version = options.client_version,
        .api_key = options.api_key,
        .metadata = options.headers,
        .tls = tls,
        .retry = retry,
        .keep_alive = keep_alive,
        .http_connect_proxy = proxy,
    };
}

/// `Options.worker_heartbeat_interval` in milliseconds for the runtime.
fn heartbeatIntervalMs(interval: ?Duration, rejection: *Rejection) error{InvalidOptions}!u64 {
    const d = interval orelse return (bridge.Runtime.Options{}).worker_heartbeat_interval_ms;
    if (d.nanoseconds == 0) return 0;
    if (d.nanoseconds < std.time.ns_per_s or d.nanoseconds > 60 * std.time.ns_per_s)
        return rejection.reject("Options.worker_heartbeat_interval must be from 1s to 1m, or zero to turn worker heartbeats off, not {f}", .{d});
    return durationMs(d);
}

/// `Options.rpc_timeout` in milliseconds for `CallOptions.timeout_ms`: 0 sends none.
fn rpcTimeoutMs(timeout: ?Duration, rejection: *Rejection) error{InvalidOptions}!u32 {
    const d = timeout orelse return 0;
    if (d.nanoseconds <= 0) return rejection.reject("Options.rpc_timeout must be positive, not {f}; set it to null for sdk-core's default", .{d});
    return std.math.cast(u32, durationMs(d)) orelse std.math.maxInt(u32);
}

/// Connects to the server `options` names, reading the TLS files it names, and checks it.
/// Logs why on failure: as an error, or as a warning for a `reconnect`, after which the
/// client still works.
fn openConnection(gpa: Allocator, io: Io, runtime: bridge.Runtime, identity: []const u8, options: ConnectionOptions, comptime reconnecting: bool) ConnectError!bridge.Connection {
    var arena_state: std.heap.ArenaAllocator = .init(gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    var tls: ?bridge.Connection.Options.Tls = null;
    if (options.tls) |t| tls = .{
        .server_root_ca_cert = try tlsFile(arena, io, t.server_root_ca_cert, t.server_root_ca_cert_path, "server_root_ca_cert"),
        .domain = t.server_name,
        .client_cert = try tlsFile(arena, io, t.client_cert, t.client_cert_path, "client_cert"),
        .client_private_key = try tlsFile(arena, io, t.client_private_key, t.client_private_key_path, "client_private_key"),
    };
    const scheme = if (options.tls != null) "https" else "http";
    const url = try std.fmt.allocPrint(arena, "{s}://{s}", .{ scheme, options.host_port });
    var rejection: Rejection = .{};
    const bridge_options = bridgeConnectionOptions(url, identity, options, tls, &rejection) catch |err| {
        if (reconnecting)
            log.warn("reconnecting to {s} failed, keeping the current connection: {s}", .{ options.host_port, rejection.message })
        else
            log.err("connecting to {s} failed: {s}", .{ options.host_port, rejection.message });
        return err;
    };
    var message: ?[]u8 = null;
    defer if (message) |m| gpa.free(m);
    return bridge.Connection.connect(gpa, io, runtime, bridge_options, &message) catch |err| {
        const reason = message orelse @errorName(err);
        if (reconnecting)
            log.warn("reconnecting to {s} failed, keeping the current connection: {s}", .{ options.host_port, reason })
        else
            log.err("connecting to {s} failed: {s}", .{ options.host_port, reason });
        return err;
    };
}

/// The PEM in `pem`, or else read from `path` into `arena`.
fn tlsFile(arena: Allocator, io: Io, pem: []const u8, path: []const u8, comptime field: []const u8) ConnectError![]const u8 {
    if (path.len == 0) return pem;
    if (pem.len > 0) {
        log.err("client options: set Options.tls." ++ field ++ " or Options.tls." ++ field ++ "_path ({s}), not both", .{path});
        return error.InvalidOptions;
    }
    return Io.Dir.cwd().readFileAlloc(io, path, arena, .limited(1 << 20)) catch |err| switch (err) {
        error.OutOfMemory => error.OutOfMemory,
        else => {
            log.err("client options: reading Options.tls." ++ field ++ "_path {s} failed: {t}", .{ path, err });
            return error.InvalidOptions;
        },
    };
}

/// sdk-core's crates log at `level`; the libraries they use at `level` or `warn`, whichever is
/// less verbose.
fn coreLogFilter(buf: []u8, level: log_mod.Level) []const u8 {
    const core = switch (level) {
        .err => "ERROR",
        .warn => "WARN",
        .info => "INFO",
        .debug => "DEBUG",
    };
    const other = if (@backingInt(level) < @backingInt(log_mod.Level.warn)) core else "WARN";
    return std.fmt.bufPrint(buf, "{s},temporalio_sdk_core={s},temporalio_client={s},temporalio_common={s},temporalio_sdk_core_c_bridge={s}", .{ other, core, core, core, core }) catch unreachable;
}

/// Passes a core log record to the client's sink: the message, a `target` field naming the
/// Rust module it came from, and the record's own fields. Runs on the core's threads.
fn forwardCoreLog(ctx: *anyopaque, record: *const bridge.Runtime.LogRecord) void {
    const sink: *const log_mod.Sink = @ptrCast(@alignCast(ctx));
    var fields: [1 + log_mod.max_context_fields]log_mod.Field = undefined;
    fields[0] = .{ .key = "target", .value = record.target };
    var buf: [8 * 1024]u8 = undefined;
    var fba: std.heap.FixedBufferAllocator = .init(&buf);
    const n = coreLogFields(fba.allocator(), record.fieldsJson(), fields[1..]);
    sink.log(switch (record.level) {
        .trace, .debug => .debug,
        .info => .info,
        .warn => .warn,
        .err => .err,
    }, record.message, fields[0 .. 1 + n]);
}

/// Splits a core log record's JSON fields into `out`: strings as they are, other values as
/// JSON. A record whose fields don't parse within `a` gets one `fields` field with the JSON.
fn coreLogFields(a: Allocator, json: []const u8, out: []log_mod.Field) usize {
    if (out.len == 0 or json.len == 0 or std.mem.eql(u8, json, "{}")) return 0;
    const value = std.json.parseFromSliceLeaky(std.json.Value, a, json, .{}) catch null;
    if (value == null or value.? != .object) {
        out[0] = .{ .key = "fields", .value = json };
        return 1;
    }
    var n: usize = 0;
    var it = value.?.object.iterator();
    while (it.next()) |kv| {
        if (n == out.len) break;
        out[n] = .{ .key = kv.key_ptr.*, .value = switch (kv.value_ptr.*) {
            .string, .number_string => |s| s,
            else => |v| std.json.Stringify.valueAlloc(a, v, .{}) catch continue,
        } };
        n += 1;
    }
    return n;
}

/// The runtime's metrics options for `options`. A custom handler is wrapped in a core custom
/// meter, which `bridge.Runtime.init` owns from then on.
fn runtimeMetrics(gpa: Allocator, options: metrics_mod.Options) ConnectError!bridge.Runtime.Metrics {
    const exporter: bridge.Runtime.Metrics.Exporter = switch (options.exporter) {
        .prometheus => |p| .{ .prometheus = .{
            .bind_address = p.bind_address,
            .counters_total_suffix = p.counters_total_suffix,
            .unit_suffix = p.unit_suffix,
            .durations_as_seconds = p.durations_as_seconds,
            .histogram_bucket_overrides = p.histogram_bucket_overrides,
        } },
        .opentelemetry => |o| .{ .opentelemetry = .{
            .url = o.url,
            .headers = o.headers,
            .metric_periodicity_ms = if (o.periodicity) |d| std.math.cast(u32, @max(d.toMilliseconds(), 1)) orelse std.math.maxInt(u32) else 0,
            .temporality = switch (o.temporality) {
                .cumulative => .cumulative,
                .delta => .delta,
            },
            .protocol = switch (o.protocol) {
                .grpc => .grpc,
                .http => .http,
            },
            .durations_as_seconds = o.durations_as_seconds,
            .histogram_bucket_overrides = o.histogram_bucket_overrides,
        } },
        .custom => |h| blk: {
            if (options.global_tags.len > 0) {
                log.err("client metrics: global_tags apply only to the prometheus and opentelemetry exporters; add them in the custom handler instead", .{});
                return error.InvalidOptions;
            }
            break :blk .{ .custom = bridge.metric.CustomMeter.create(gpa, h.ptr, h.vtable) catch |err| switch (err) {
                error.OutOfMemory => return error.OutOfMemory,
                error.TooManyCustomMeters => {
                    log.err("creating the core runtime failed: {d} clients in this process already have a custom metrics handler; close one", .{bridge.metric.max_custom_meters});
                    return error.RuntimeInitFailed;
                },
            } };
        },
    };
    return .{
        .exporter = exporter,
        .attach_service_name = options.attach_service_name,
        .global_tags = options.global_tags,
        .prefix = options.metric_prefix,
    };
}

pub const ConnectError = error{ OutOfMemory, InvalidOptions, RuntimeInitFailed, ConnectFailed };

pub const Client = struct {
    gpa: Allocator,
    io: Io,
    runtime: bridge.Runtime,
    /// Read it with `currentConnection`: `reconnect` replaces it.
    connection: bridge.Connection,
    /// Guards `connection`, `retired_connections` and `running_workers`.
    connection_mutex: Io.Mutex = .init,
    /// Connections `reconnect` replaced, freed by `close`: calls already sent on one may
    /// still be using it.
    retired_connections: std.ArrayList(bridge.Connection) = .empty,
    /// The core workers running on this client, which `reconnect` moves to the new
    /// connection. Registered by `worker.Worker.run`.
    running_workers: std.ArrayList(*bridge.c.TemporalCoreWorker) = .empty,
    namespace: []u8,
    identity: []u8,
    /// `Options.codecs`, borrowed.
    codecs: []const converter.PayloadCodec,
    /// `Options.logger` or the default sink; workers on this client inherit it.
    logger: log_mod.Sink,
    /// A copy of `logger` that the runtime's core log forwarding points at, freed after the
    /// runtime.
    core_log_sink: ?*log_mod.Sink,
    /// `Options.propagators`, borrowed; workers on this client use them too.
    propagators: []const propagation.ContextPropagator,
    /// `Options.worker_interceptors`, borrowed.
    worker_interceptors: []const interceptor.Worker,
    /// `Options.interceptors`, borrowed.
    interceptors: []const interceptor.Client,
    /// The runtime's meter; null without `Options.metrics`.
    meter: ?*bridge.metric.Meter,
    /// `Options.rpc_timeout` in milliseconds; 0 sends none.
    rpc_timeout_ms: u32 = 0,

    /// Records through this client's metrics exporter (`Options.metrics`) with the runtime's
    /// default attributes, or records nothing without one. Valid until `close`; metrics
    /// created from it, and handlers from its `with`, must be freed first. Thread-safe.
    pub fn metrics(c: *const Client) metrics_mod.Handler {
        const m = c.meter orelse return .noop;
        return .{ .ptr = m, .vtable = &bridge.metric.Meter.vtable };
    }

    /// Connects and checks the server. Close with `close`.
    pub fn connect(gpa: Allocator, io: Io, options: Options) ConnectError!Client {
        var message: ?[]u8 = null;
        defer if (message) |m| gpa.free(m);

        var rejection: Rejection = .{};
        const heartbeat_ms = heartbeatIntervalMs(options.worker_heartbeat_interval, &rejection) catch |err| {
            log.err("connecting to {s} failed: {s}", .{ options.host_port, rejection.message });
            return err;
        };
        const rpc_timeout_ms = rpcTimeoutMs(options.rpc_timeout, &rejection) catch |err| {
            log.err("connecting to {s} failed: {s}", .{ options.host_port, rejection.message });
            return err;
        };

        const logger = options.logger orelse log_mod.Sink.std_log;
        var core_log_sink: ?*log_mod.Sink = null;
        errdefer if (core_log_sink) |s| gpa.destroy(s);
        var filter_buf: [256]u8 = undefined;
        var logging: ?bridge.Runtime.Logging = null;
        if (options.core_log_level) |level| {
            core_log_sink = try gpa.create(log_mod.Sink);
            core_log_sink.?.* = logger;
            logging = .{
                .filter = coreLogFilter(&filter_buf, level),
                .forward = .{ .ctx = core_log_sink.?, .func = forwardCoreLog },
            };
        }
        const runtime_metrics = if (options.metrics) |m| try runtimeMetrics(gpa, m) else null;
        const runtime = bridge.Runtime.init(gpa, .{ .logging = logging, .metrics = runtime_metrics, .worker_heartbeat_interval_ms = heartbeat_ms }, &message) catch |err| switch (err) {
            error.TooManyLogForwarders => {
                log.err("creating the core runtime failed: {d} clients in this process already forward sdk-core's logs; close one, or set Options.core_log_level to null", .{bridge.Runtime.max_forwarders});
                return error.RuntimeInitFailed;
            },
            error.OutOfMemory => return error.OutOfMemory,
            error.InvalidOptions => {
                log.err("creating the core runtime failed: an option is out of range, or a metrics global tag, OpenTelemetry header, or histogram bucket override contains a newline", .{});
                return error.InvalidOptions;
            },
            error.RuntimeInitFailed => {
                if (message) |m| log.err("creating the core runtime failed: {s}", .{m});
                return error.RuntimeInitFailed;
            },
        };
        errdefer runtime.deinit();
        const meter = try bridge.metric.Meter.create(gpa, runtime);
        errdefer if (meter) |m| m.destroy();

        const identity = if (options.identity.len > 0) try gpa.dupe(u8, options.identity) else try defaultIdentity(gpa);
        errdefer gpa.free(identity);
        const namespace = try gpa.dupe(u8, options.namespace);
        errdefer gpa.free(namespace);

        const connection = try openConnection(gpa, io, runtime, identity, options.connectionOptions(), false);
        return .{
            .gpa = gpa,
            .io = io,
            .runtime = runtime,
            .connection = connection,
            .namespace = namespace,
            .identity = identity,
            .codecs = options.codecs,
            .logger = logger,
            .core_log_sink = core_log_sink,
            .propagators = options.propagators,
            .worker_interceptors = options.worker_interceptors,
            .interceptors = options.interceptors,
            .meter = meter,
            .rpc_timeout_ms = rpc_timeout_ms,
        };
    }

    /// The gRPC health service name `checkHealth` asks about: the service this client calls.
    const health_service = "temporal.api.workflowservice.v1.WorkflowService";

    /// Asks the server, through the gRPC health service (grpc.health.v1.Health/Check),
    /// whether it is serving the WorkflowService this client calls, and returns if it is.
    /// Each failure is logged as a warning. Makes one attempt, without retries, within
    /// `Options.rpc_timeout`; canceling the calling task cancels it. Bypasses
    /// `Options.interceptors` and the codecs. Thread-safe.
    pub fn checkHealth(c: *Client) CheckHealthError!void {
        const health = proto.grpc.health.v1;
        var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
        defer arena_state.deinit();
        const a = arena_state.allocator();
        var encoded: Io.Writer.Allocating = .init(a);
        (health.HealthCheckRequest{ .service = health_service }).encode(&encoded.writer, a) catch return error.OutOfMemory;
        var failure: ?bridge.Connection.RpcFailure = null;
        defer if (failure) |f| f.deinit(c.gpa);
        const bytes = c.currentConnection().call(c.gpa, c.io, .health, "Check", encoded.written(), .{ .retry = false, .timeout_ms = c.rpc_timeout_ms }, &failure) catch |err| switch (err) {
            error.RpcFailed, error.InvalidOptions => {
                const f = failure orelse {
                    log.warn("checking the health of " ++ health_service ++ " failed: {t}", .{err});
                    return error.RpcFailed;
                };
                log.warn("checking the health of " ++ health_service ++ " failed: {s} (gRPC status {d})", .{ f.message, f.status_code });
                return error.RpcFailed;
            },
            error.OutOfMemory, error.Canceled => |e| return e,
        };
        defer c.gpa.free(bytes);
        return healthResponse(a, bytes);
    }

    /// Whether a serialized HealthCheckResponse reports SERVING, logging why not.
    fn healthResponse(a: Allocator, bytes: []const u8) CheckHealthError!void {
        const health = proto.grpc.health.v1;
        var reader: Io.Reader = .fixed(bytes);
        const response = health.HealthCheckResponse.decode(&reader, a) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            else => {
                log.warn("checking the health of " ++ health_service ++ " failed: the response doesn't decode as a HealthCheckResponse ({t})", .{err});
                return error.InvalidResponse;
            },
        };
        if (response.status == .SERVING) return;
        log.warn("checking the health of " ++ health_service ++ " failed: the server reports {s}, not SERVING", .{std.enums.tagName(health.HealthCheckResponse.ServingStatus, response.status) orelse "an unknown status"});
        return error.NotServing;
    }

    pub fn close(c: *Client) void {
        c.running_workers.deinit(c.gpa);
        for (c.retired_connections.items) |conn| conn.deinit();
        c.retired_connections.deinit(c.gpa);
        c.connection.deinit();
        if (c.meter) |m| m.destroy();
        c.runtime.deinit();
        if (c.core_log_sink) |s| c.gpa.destroy(s);
        c.gpa.free(c.namespace);
        c.gpa.free(c.identity);
        c.* = undefined;
    }

    /// The connection calls go over now. It stays valid until `close`, even after a
    /// `reconnect` replaces it. Thread-safe.
    pub fn currentConnection(c: *Client) bridge.Connection {
        c.connection_mutex.lockUncancelable(c.io);
        defer c.connection_mutex.unlock(c.io);
        return c.connection;
    }

    /// Replaces the API key sent with every call from now on, by this client and every worker
    /// on it; empty sends none. Calls already sent keep the key they had. Use it to rotate a
    /// key before it expires. A later `reconnect` sends its own `ConnectionOptions.api_key`
    /// instead. Thread-safe.
    pub fn updateApiKey(c: *Client, api_key: []const u8) void {
        c.connection_mutex.lockUncancelable(c.io);
        defer c.connection_mutex.unlock(c.io);
        c.connection.setApiKey(api_key);
    }

    /// Replaces the gRPC headers (`Options.headers`) sent with every call from now on, by
    /// this client and every worker on it. Returns `error.InvalidOptions` (logged) for a key
    /// that isn't a valid header name or ends in "-bin" (binary headers aren't supported), or
    /// a value with characters other than visible ASCII, space and tab; the headers are then
    /// unchanged. A later `reconnect` sends its own `ConnectionOptions.headers` instead.
    /// Thread-safe.
    pub fn updateHeaders(c: *Client, headers: []const bridge.bytes.MetadataEntry) error{ OutOfMemory, InvalidOptions }!void {
        for (headers) |h| try validateHeader(h);
        c.connection_mutex.lockUncancelable(c.io);
        defer c.connection_mutex.unlock(c.io);
        c.connection.setMetadata(c.gpa, headers) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            // validateHeader already rejects newlines.
            error.NewlineInKey, error.NewlineInValue => unreachable,
        };
    }

    /// Connects again with `options` (address, API key, TLS, headers, keep-alive, proxy,
    /// retry policy, and client name and version) and sends every
    /// later call over the new connection: this client's, and those of the workers running
    /// on it, which move over without stopping (their polls already waiting finish on the old
    /// connection). Workers started later use it too. TLS files named by `options.tls` are
    /// read again, so this is how to rotate a client certificate: sdk-core can't change the
    /// certificate of a live connection. The new connection is checked first; if that fails
    /// (`ConnectError`, logged as a warning), the client keeps the current one. The old
    /// connection is freed by `close`, since calls already sent may still be using it. A
    /// worker that is already shutting down can't move and finishes on the old connection
    /// (logged as a warning). Thread-safe.
    pub fn reconnect(c: *Client, options: ConnectionOptions) ConnectError!void {
        const next = try openConnection(c.gpa, c.io, c.runtime, c.identity, options, true);
        c.connection_mutex.lockUncancelable(c.io);
        defer c.connection_mutex.unlock(c.io);
        c.retired_connections.append(c.gpa, c.connection) catch {
            next.deinit();
            return error.OutOfMemory;
        };
        c.connection = next;
        for (c.running_workers.items) |ptr| replaceWorkerClient(c, ptr, next);
    }

    /// For `worker.Worker.run`: registers a core worker it created on `created_on`, so
    /// `reconnect` moves it, and moves it at once if a `reconnect` came in between. Pair with
    /// `untrackWorker` before the core worker is freed.
    pub fn trackWorker(c: *Client, ptr: *bridge.c.TemporalCoreWorker, created_on: bridge.Connection) error{OutOfMemory}!void {
        c.connection_mutex.lockUncancelable(c.io);
        defer c.connection_mutex.unlock(c.io);
        try c.running_workers.append(c.gpa, ptr);
        if (created_on.ptr != c.connection.ptr) replaceWorkerClient(c, ptr, c.connection);
    }

    /// For `worker.Worker.run`: unregisters a core worker `trackWorker` registered; does
    /// nothing if it isn't registered.
    pub fn untrackWorker(c: *Client, ptr: *bridge.c.TemporalCoreWorker) void {
        c.connection_mutex.lockUncancelable(c.io);
        defer c.connection_mutex.unlock(c.io);
        for (c.running_workers.items, 0..) |p, i| if (p == ptr) {
            _ = c.running_workers.swapRemove(i);
            return;
        };
    }

    /// sdk-core refuses to move a worker that is shutting down: it finishes on the old
    /// connection.
    fn replaceWorkerClient(c: *Client, ptr: *bridge.c.TemporalCoreWorker, connection: bridge.Connection) void {
        const w: bridge.Worker = .{ .ptr = ptr, .runtime = c.runtime };
        var message: ?[]u8 = null;
        defer if (message) |m| c.gpa.free(m);
        w.replaceClient(c.gpa, connection, &message) catch {
            log.warn("moving a running worker to the new connection failed; it keeps the old one until it stops: {s}", .{message orelse "no reason given"});
        };
    }

    /// Starts a workflow defined with `temporal.workflow.Def` and returns a handle to its run.
    /// `args` is a tuple of the workflow's parameters after its context.
    pub fn executeWorkflow(c: *Client, comptime Workflow: type, options: StartWorkflowOptions, args: Workflow.Args) StartError!WorkflowRun(Workflow.Result) {
        var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
        defer arena_state.deinit();
        const id = if (options.id.len > 0) options.id else try uuid(arena_state.allocator(), c.io);
        const started = try c.startWorkflow(Workflow.workflow_name, id, options, args);
        errdefer c.gpa.free(started.run_id);
        return .{ .client = c, .id = try c.gpa.dupe(u8, id), .run_id = started.run_id, .eagerly_started = started.eager };
    }

    /// Starts a workflow by its type name, for a workflow this program has no definition of.
    /// `args` is a tuple of its arguments and `Result` the type its result decodes to (`void`
    /// to ignore it).
    pub fn executeWorkflowByName(c: *Client, comptime Result: type, workflow_type: []const u8, options: StartWorkflowOptions, args: anytype) StartError!WorkflowRun(Result) {
        var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
        defer arena_state.deinit();
        const id = if (options.id.len > 0) options.id else try uuid(arena_state.allocator(), c.io);
        const started = try c.startWorkflow(workflow_type, id, options, args);
        errdefer c.gpa.free(started.run_id);
        return .{ .client = c, .id = try c.gpa.dupe(u8, id), .run_id = started.run_id, .eagerly_started = started.eager };
    }

    /// The run ID is owned with the client's allocator.
    fn startWorkflow(c: *Client, workflow_type: []const u8, id: []const u8, options: StartWorkflowOptions, args: anytype) StartError!struct { run_id: []u8, eager: bool } {
        var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
        defer arena_state.deinit();
        const a = arena_state.allocator();
        var request = try c.startRequest(a, workflow_type, id, options, args);
        // Set here rather than in `startRequest`: the core reserves a workflow task slot only
        // for a plain start, so signal-with-start and update-with-start never ask for it.
        request.request_eager_execution = options.enable_eager_start;
        var header = try c.outgoingHeader(a, options.context);
        // The status is only needed to find the existing run; without it `send` logs failures.
        var status: ?RpcStatus = null;
        const status_out: ?*?RpcStatus = if (options.error_when_already_started) null else &status;
        const response = c.rpcCall(wfs.StartWorkflowExecutionResponse, "StartWorkflowExecution", request, a, status_out, &header, options.context) catch |err| switch (err) {
            error.AlreadyExists => {
                const s = status orelse return error.WorkflowExecutionAlreadyStarted;
                const run_id = s.alreadyStartedRunId(a) orelse return error.WorkflowExecutionAlreadyStarted;
                return .{ .run_id = try c.gpa.dupe(u8, run_id), .eager = false };
            },
            error.RpcFailed => {
                const s = status orelse return error.RpcFailed;
                log.err("StartWorkflowExecution failed: {s} (gRPC status {d})", .{ s.message, s.code });
                return error.RpcFailed;
            },
            else => |e| return e,
        };
        // The core has already handed the eager task to the worker; the response keeps a copy.
        return .{ .run_id = try c.gpa.dupe(u8, response.run_id), .eager = response.eager_workflow_task != null };
    }

    /// The start request for `options`, allocated from `a`. Signal-with-start copies it.
    fn startRequest(c: *Client, a: Allocator, workflow_type: []const u8, id: []const u8, options: StartWorkflowOptions, args: anytype) (search_attributes.EncodeError || error{InvalidOptions})!wfs.StartWorkflowExecutionRequest {
        if (options.task_queue.len == 0) {
            log.warn("starting workflow \"{s}\" of type \"{s}\": StartWorkflowOptions.task_queue is empty; set it to the task queue a worker polls (WorkflowEnvironment fills in its own)", .{ id, workflow_type });
            return error.InvalidOptions;
        }
        var request: wfs.StartWorkflowExecutionRequest = .{
            .namespace = c.namespace,
            .workflow_id = id,
            .workflow_type = .{ .name = workflow_type },
            .task_queue = .{ .name = options.task_queue, .kind = .TASK_QUEUE_KIND_NORMAL },
            .input = .{ .payloads = try converter.toPayloads(a, args) },
            .workflow_execution_timeout = protoDuration(options.execution_timeout),
            .workflow_run_timeout = protoDuration(options.run_timeout),
            .workflow_task_timeout = protoDuration(options.task_timeout),
            .workflow_start_delay = protoDuration(options.start_delay),
            .cron_schedule = options.cron_schedule,
            .identity = c.identity,
            .request_id = try uuid(a, c.io),
            .workflow_id_reuse_policy = switch (options.id_reuse_policy) {
                .unspecified => .WORKFLOW_ID_REUSE_POLICY_UNSPECIFIED,
                .allow_duplicate => .WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
                .allow_duplicate_failed_only => .WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY,
                .reject_duplicate => .WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
            },
            .workflow_id_conflict_policy = switch (options.id_conflict_policy) {
                .unspecified => .WORKFLOW_ID_CONFLICT_POLICY_UNSPECIFIED,
                .fail => .WORKFLOW_ID_CONFLICT_POLICY_FAIL,
                .use_existing => .WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
                .terminate_existing => .WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING,
            },
        };
        if (options.memo.len > 0) {
            var memo: proto.temporal.api.common.v1.Memo = .{};
            try memo.fields.ensureTotalCapacity(a, options.memo.len);
            for (options.memo) |field| memo.fields.appendAssumeCapacity(.{ .key = field.name, .value = field.value });
            request.memo = memo;
        }
        if (options.search_attributes.len > 0) request.search_attributes = try search_attributes.toProto(a, options.search_attributes, .start);
        if (options.retry_policy) |rp| request.retry_policy = try protoRetryPolicy(a, rp);
        if (options.static_summary.len > 0 or options.static_details.len > 0) request.user_metadata = .{
            .summary = if (options.static_summary.len > 0) try converter.toPayload(a, options.static_summary) else null,
            .details = if (options.static_details.len > 0) try converter.toPayload(a, options.static_details) else null,
        };
        if (options.versioning_override) |o| request.versioning_override = .{ .override = switch (o) {
            .pinned => |v| .{ .pinned = .{
                .behavior = .PINNED_OVERRIDE_BEHAVIOR_PINNED,
                .version = .{ .deployment_name = v.deployment_name, .build_id = v.build_id },
            } },
            .auto_upgrade => .{ .auto_upgrade = true },
        } };
        if (options.priority) |p| request.priority = .{ .priority_key = p.priority_key, .fairness_key = p.fairness_key, .fairness_weight = p.fairness_weight };
        return request;
    }

    fn protoRetryPolicy(a: Allocator, rp: workflow.RetryPolicy) error{OutOfMemory}!proto.temporal.api.common.v1.RetryPolicy {
        var p: proto.temporal.api.common.v1.RetryPolicy = .{
            .initial_interval = protoDuration(rp.initial_interval),
            .backoff_coefficient = rp.backoff_coefficient,
            .maximum_interval = protoDuration(rp.maximum_interval),
            .maximum_attempts = rp.maximum_attempts,
        };
        try p.non_retryable_error_types.appendSlice(a, rp.non_retryable_error_types);
        return p;
    }

    /// Sends `Signal` (a `workflow.SignalDef`) with `args` to a workflow. An empty `run_id`
    /// signals the latest run. Returns `error.NotFound` if no such workflow exists or the
    /// run has closed. The call returns once the server has recorded the signal, not once a
    /// handler has run it.
    pub fn signalWorkflow(c: *Client, comptime Signal: type, workflow_id: []const u8, run_id: []const u8, args: Signal.Args) SignalError!void {
        return c.signalWorkflowWithOptions(Signal, workflow_id, run_id, args, .{});
    }

    /// `signalWorkflow` with options.
    pub fn signalWorkflowWithOptions(c: *Client, comptime Signal: type, workflow_id: []const u8, run_id: []const u8, args: Signal.Args, options: SignalOptions) SignalError!void {
        var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
        defer arena_state.deinit();
        const a = arena_state.allocator();
        var header = try c.outgoingHeader(a, options.context);
        _ = c.rpcCall(wfs.SignalWorkflowExecutionResponse, "SignalWorkflowExecution", wfs.SignalWorkflowExecutionRequest{
            .namespace = c.namespace,
            .workflow_execution = .{ .workflow_id = workflow_id, .run_id = run_id },
            .signal_name = Signal.signal_name,
            .input = .{ .payloads = try converter.toPayloads(a, args) },
            .identity = c.identity,
            .request_id = try uuid(a, c.io),
        }, a, null, &header, options.context) catch |err| switch (err) {
            error.AlreadyExists => return error.InvalidResponse,
            else => |e| return e,
        };
    }

    /// Sends `Signal` with `signal_args` to the workflow `options.id`, first starting it
    /// with `args` if it isn't running, in one atomic call. A new run's first workflow task
    /// carries the signal, which waits until the workflow registers its handler. If the
    /// workflow is already running, it is only signaled: `args` and the other start options
    /// are ignored and the returned run is the running one. An empty `options.id` generates
    /// a UUID, so the call always starts a new workflow.
    pub fn signalWithStartWorkflow(
        c: *Client,
        comptime Workflow: type,
        options: StartWorkflowOptions,
        args: Workflow.Args,
        comptime Signal: type,
        signal_args: Signal.Args,
    ) StartError!WorkflowRun(Workflow.Result) {
        var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
        defer arena_state.deinit();
        const a = arena_state.allocator();
        const id = if (options.id.len > 0) options.id else try uuid(a, c.io);

        const start = try c.startRequest(a, Workflow.workflow_name, id, options, args);
        var request: wfs.SignalWithStartWorkflowExecutionRequest = .{
            .signal_name = Signal.signal_name,
            .signal_input = .{ .payloads = try converter.toPayloads(a, signal_args) },
        };
        // Every start field the two requests share, so a start option mapped in
        // `startRequest` reaches signal-with-start too.
        const Start = wfs.StartWorkflowExecutionRequest;
        const info = @typeInfo(wfs.SignalWithStartWorkflowExecutionRequest).@"struct";
        inline for (info.field_names, info.field_types) |name, F| {
            if (comptime @hasField(Start, name) and @FieldType(Start, name) == F and !std.mem.eql(u8, name, "_unknown_fields"))
                @field(request, name) = @field(start, name);
        }

        var header = try c.outgoingHeader(a, options.context);
        const response = c.rpcCall(wfs.SignalWithStartWorkflowExecutionResponse, "SignalWithStartWorkflowExecution", request, a, null, &header, options.context) catch |err| switch (err) {
            error.AlreadyExists => return error.WorkflowExecutionAlreadyStarted,
            else => |e| return e,
        };
        const run_id = try c.gpa.dupe(u8, response.run_id);
        errdefer c.gpa.free(run_id);
        return .{ .client = c, .id = try c.gpa.dupe(u8, id), .run_id = run_id };
    }

    /// Terminates a workflow execution: it stops at once, with no chance to clean up. An empty
    /// `run_id` targets the latest run.
    pub fn terminateWorkflow(c: *Client, workflow_id: []const u8, run_id: []const u8, reason: []const u8) error{ OutOfMemory, Canceled, RpcFailed, NotFound, InvalidResponse, CodecFailed }!void {
        var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
        defer arena_state.deinit();
        const a = arena_state.allocator();
        _ = c.rpc(wfs.TerminateWorkflowExecutionResponse, "TerminateWorkflowExecution", wfs.TerminateWorkflowExecutionRequest{
            .namespace = c.namespace,
            .workflow_execution = .{ .workflow_id = workflow_id, .run_id = run_id },
            .reason = reason,
            .identity = c.identity,
        }, a) catch |err| switch (err) {
            error.AlreadyExists => return error.InvalidResponse,
            else => |e| return e,
        };
    }

    /// Requests cancellation of a workflow execution. The workflow sees it as `error.Canceled`
    /// from its next cancelable wait and may clean up before it returns; the run ends
    /// canceled when the workflow returns `error.Canceled`. An empty `run_id` targets the
    /// latest run. Returns `error.NotFound` if no such workflow exists.
    pub fn cancelWorkflow(c: *Client, workflow_id: []const u8, run_id: []const u8, reason: []const u8) error{ OutOfMemory, Canceled, RpcFailed, NotFound, InvalidResponse, CodecFailed }!void {
        var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
        defer arena_state.deinit();
        const a = arena_state.allocator();
        _ = c.rpc(wfs.RequestCancelWorkflowExecutionResponse, "RequestCancelWorkflowExecution", wfs.RequestCancelWorkflowExecutionRequest{
            .namespace = c.namespace,
            .workflow_execution = .{ .workflow_id = workflow_id, .run_id = run_id },
            .identity = c.identity,
            .request_id = try uuid(a, c.io),
            .reason = reason,
        }, a) catch |err| switch (err) {
            error.AlreadyExists => return error.InvalidResponse,
            else => |e| return e,
        };
    }

    /// Completes an activity that returned `ctx.completeAsync()`, identified by the task token
    /// it handed off, with `result` (a value of the activity's result type, or `{}` for
    /// `void`). Returns `error.NotFound` when the activity is no longer waiting: it already
    /// completed, failed, or timed out, or its workflow closed. Safe to call from any process
    /// that can reach the server, not only the worker's.
    pub fn completeActivity(c: *Client, task_token: []const u8, result: anytype) ActivityCompletionError!void {
        var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
        defer arena_state.deinit();
        const a = arena_state.allocator();
        _ = try c.activityRpc(wfs.RespondActivityTaskCompletedResponse, "RespondActivityTaskCompleted", wfs.RespondActivityTaskCompletedRequest{
            .namespace = c.namespace,
            .task_token = task_token,
            .result = try resultPayloads(a, result),
            .identity = c.identity,
        }, a);
    }

    /// `completeActivity` for the activity `id`.
    pub fn completeActivityById(c: *Client, id: ActivityId, result: anytype) ActivityCompletionError!void {
        var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
        defer arena_state.deinit();
        const a = arena_state.allocator();
        _ = try c.activityRpc(wfs.RespondActivityTaskCompletedByIdResponse, "RespondActivityTaskCompletedById", wfs.RespondActivityTaskCompletedByIdRequest{
            .namespace = c.namespace,
            .workflow_id = id.workflow_id,
            .run_id = id.run_id,
            .activity_id = id.activity_id,
            .result = try resultPayloads(a, result),
            .identity = c.identity,
        }, a);
    }

    /// Fails an activity that returned `ctx.completeAsync()` with the failure `f`. The
    /// activity's retry policy applies, as for a failure returned by the activity itself.
    /// Returns `error.NotFound` when the activity is no longer waiting.
    pub fn failActivity(c: *Client, task_token: []const u8, f: failure_mod.Application) ActivityCompletionError!void {
        var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
        defer arena_state.deinit();
        const a = arena_state.allocator();
        _ = try c.activityRpc(wfs.RespondActivityTaskFailedResponse, "RespondActivityTaskFailed", wfs.RespondActivityTaskFailedRequest{
            .namespace = c.namespace,
            .task_token = task_token,
            .failure = try failure_mod.application(a, f),
            .identity = c.identity,
        }, a);
    }

    /// `failActivity` for the activity `id`.
    pub fn failActivityById(c: *Client, id: ActivityId, f: failure_mod.Application) ActivityCompletionError!void {
        var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
        defer arena_state.deinit();
        const a = arena_state.allocator();
        _ = try c.activityRpc(wfs.RespondActivityTaskFailedByIdResponse, "RespondActivityTaskFailedById", wfs.RespondActivityTaskFailedByIdRequest{
            .namespace = c.namespace,
            .workflow_id = id.workflow_id,
            .run_id = id.run_id,
            .activity_id = id.activity_id,
            .failure = try failure_mod.application(a, f),
            .identity = c.identity,
        }, a);
    }

    /// Reports that an activity that returned `ctx.completeAsync()` was canceled, with
    /// `details` (any value the converter encodes, or `{}` for none). Call it after
    /// `recordActivityHeartbeat` returns true. Returns `error.NotFound` when the activity is
    /// no longer waiting.
    pub fn reportActivityCancellation(c: *Client, task_token: []const u8, details: anytype) ActivityCompletionError!void {
        var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
        defer arena_state.deinit();
        const a = arena_state.allocator();
        _ = try c.activityRpc(wfs.RespondActivityTaskCanceledResponse, "RespondActivityTaskCanceled", wfs.RespondActivityTaskCanceledRequest{
            .namespace = c.namespace,
            .task_token = task_token,
            .details = try detailPayloads(a, details),
            .identity = c.identity,
        }, a);
    }

    /// `reportActivityCancellation` for the activity `id`.
    pub fn reportActivityCancellationById(c: *Client, id: ActivityId, details: anytype) ActivityCompletionError!void {
        var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
        defer arena_state.deinit();
        const a = arena_state.allocator();
        _ = try c.activityRpc(wfs.RespondActivityTaskCanceledByIdResponse, "RespondActivityTaskCanceledById", wfs.RespondActivityTaskCanceledByIdRequest{
            .namespace = c.namespace,
            .workflow_id = id.workflow_id,
            .run_id = id.run_id,
            .activity_id = id.activity_id,
            .details = try detailPayloads(a, details),
            .identity = c.identity,
        }, a);
    }

    /// Heartbeats an activity that returned `ctx.completeAsync()`, with progress `details`
    /// (any value the converter encodes, or `{}` for none), which a retry reads with
    /// `ctx.heartbeatDetails`. Returns true when cancellation of the activity has been
    /// requested: finish it with `reportActivityCancellation` (or complete or fail it). Not
    /// throttled: each call is a request to the server. Returns `error.NotFound` when the
    /// activity is no longer waiting.
    pub fn recordActivityHeartbeat(c: *Client, task_token: []const u8, details: anytype) ActivityCompletionError!bool {
        var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
        defer arena_state.deinit();
        const a = arena_state.allocator();
        const response = try c.activityRpc(wfs.RecordActivityTaskHeartbeatResponse, "RecordActivityTaskHeartbeat", wfs.RecordActivityTaskHeartbeatRequest{
            .namespace = c.namespace,
            .task_token = task_token,
            .details = try detailPayloads(a, details),
            .identity = c.identity,
        }, a);
        return response.cancel_requested;
    }

    /// `recordActivityHeartbeat` for the activity `id`.
    pub fn recordActivityHeartbeatById(c: *Client, id: ActivityId, details: anytype) ActivityCompletionError!bool {
        var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
        defer arena_state.deinit();
        const a = arena_state.allocator();
        const response = try c.activityRpc(wfs.RecordActivityTaskHeartbeatByIdResponse, "RecordActivityTaskHeartbeatById", wfs.RecordActivityTaskHeartbeatByIdRequest{
            .namespace = c.namespace,
            .workflow_id = id.workflow_id,
            .run_id = id.run_id,
            .activity_id = id.activity_id,
            .details = try detailPayloads(a, details),
            .identity = c.identity,
        }, a);
        return response.cancel_requested;
    }

    fn activityRpc(c: *Client, comptime Response: type, comptime method: []const u8, request: anytype, arena: Allocator) ActivityCompletionError!Response {
        return c.rpc(Response, method, request, arena) catch |err| switch (err) {
            error.AlreadyExists => error.InvalidResponse,
            else => |e| e,
        };
    }

    fn resultPayloads(a: Allocator, result: anytype) error{OutOfMemory}!proto.temporal.api.common.v1.Payloads {
        var payloads: proto.temporal.api.common.v1.Payloads = .{};
        try payloads.payloads.append(a, try converter.toPayload(a, result));
        return payloads;
    }

    fn detailPayloads(a: Allocator, details: anytype) error{OutOfMemory}!?proto.temporal.api.common.v1.Payloads {
        if (@TypeOf(details) == void) return null;
        return try resultPayloads(a, details);
    }

    /// Returns the full event history of a workflow run (all pages). An empty `run_id` reads
    /// the latest run. Memory comes from `arena`.
    pub fn fetchHistory(c: *Client, arena: Allocator, workflow_id: []const u8, run_id: []const u8) HistoryError!History {
        var it = try c.historyIterator(workflow_id, run_id, .{});
        defer it.deinit();
        var history: History = .{};
        while (try it.next(arena)) |event| try history.events.append(arena, event);
        return history;
    }

    /// Iterates over the events of a workflow run's history, oldest first. An empty `run_id`
    /// reads the latest run. The iterator fetches a page at a time as `next` reaches it;
    /// nothing is fetched here, so a missing workflow shows up as `error.NotFound` from
    /// `next`. Free it with `deinit`.
    pub fn historyIterator(c: *Client, workflow_id: []const u8, run_id: []const u8, options: HistoryIteratorOptions) error{OutOfMemory}!HistoryIterator {
        const id = try c.gpa.dupe(u8, workflow_id);
        errdefer c.gpa.free(id);
        return .{ .client = c, .workflow_id = id, .run_id = try c.gpa.dupe(u8, run_id), .options = options, .page = .init(c.gpa) };
    }

    /// Describes a workflow run: its status, type, times, memo and search attributes. An
    /// empty `run_id` describes the latest run. Returns `error.NotFound` if no such workflow
    /// exists. The description and all its memory come from `arena`.
    pub fn describeWorkflow(c: *Client, arena: Allocator, workflow_id: []const u8, run_id: []const u8) DescribeError!WorkflowDescription {
        const response = c.rpc(wfs.DescribeWorkflowExecutionResponse, "DescribeWorkflowExecution", wfs.DescribeWorkflowExecutionRequest{
            .namespace = c.namespace,
            .execution = .{ .workflow_id = workflow_id, .run_id = run_id },
        }, arena) catch |err| switch (err) {
            error.AlreadyExists => return error.InvalidResponse,
            else => |e| return e,
        };
        const info = response.workflow_execution_info orelse {
            log.err("DescribeWorkflowExecution for workflow {s} returned no execution info", .{workflow_id});
            return error.InvalidResponse;
        };
        const lifted = try executionInfo(arena, info);
        var d: WorkflowDescription = undefined;
        inline for (@typeInfo(WorkflowDescription).@"struct".field_names) |name| {
            if (comptime std.mem.eql(u8, name, "raw")) continue;
            @field(d, name) = @field(lifted, name);
        }
        d.raw = response;
        return d;
    }

    /// Lists the workflow executions matching the visibility `query`, such as
    /// `WorkflowType = 'Greet' AND ExecutionStatus = 'Running'` or a condition on a search
    /// attribute (`CustomerId = 'c-42'`); an empty query lists them all. Visibility is
    /// eventually consistent: a workflow started or changed a moment ago may not match yet.
    /// The iterator fetches a page at a time as `next` reaches it; nothing is fetched here.
    /// Free it with `deinit`.
    pub fn listWorkflows(c: *Client, query: []const u8, options: ListWorkflowsOptions) error{OutOfMemory}!WorkflowExecutionIterator {
        return .{ .client = c, .query = try c.gpa.dupe(u8, query), .page_size = options.page_size, .page = .init(c.gpa) };
    }

    /// The number of workflow executions matching the visibility `query` (see
    /// `listWorkflows`); an empty query counts them all. Eventually consistent, like
    /// `listWorkflows`.
    pub fn countWorkflows(c: *Client, query: []const u8) ListError!u64 {
        var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
        defer arena_state.deinit();
        const a = arena_state.allocator();
        var status: ?RpcStatus = null;
        const response = c.rpcWithStatus(wfs.CountWorkflowExecutionsResponse, "CountWorkflowExecutions", wfs.CountWorkflowExecutionsRequest{
            .namespace = c.namespace,
            .query = query,
        }, a, &status) catch |err| return visibilityError(err, status, "counting workflows", query);
        return std.math.cast(u64, response.count) orelse {
            log.err("CountWorkflowExecutions for query \"{s}\" returned the count {d}", .{ query, response.count });
            return error.InvalidResponse;
        };
    }

    fn visibilityError(err: RpcError, status: ?RpcStatus, comptime what: []const u8, query: []const u8) ListError {
        switch (err) {
            error.AlreadyExists => return error.InvalidResponse,
            error.RpcFailed => {
                const s = status orelse return error.RpcFailed;
                log.err(what ++ " with the query \"{s}\" failed: {s} (gRPC status {d})", .{ query, s.message, s.code });
                return error.RpcFailed;
            },
            else => |e| return e,
        }
    }

    /// Starts `Activity` (a `temporal.activity.Def`) on its own, without a workflow, and
    /// returns a handle to it: a worker polling `options.task_queue` runs it, and the server
    /// applies its timeouts and retry policy. `args` is a tuple of the activity's parameters
    /// after its context. The server must have standalone activities enabled (see
    /// `testing.DevServer.standalone_activity_args`); otherwise, and for options it rejects
    /// (it requires `start_to_close_timeout` or `schedule_to_close_timeout`), the call fails
    /// with `error.RpcFailed`, logging the server's reason.
    pub fn executeActivity(c: *Client, comptime Activity: type, options: StartActivityOptions, args: Activity.Args) StartActivityError!ActivityHandle(Activity.Result) {
        return c.executeActivityByName(Activity.Result, Activity.activity_name, options, args);
    }

    /// `executeActivity` for an activity this program has no definition of, by its type
    /// name. `args` is a tuple of its arguments and `Result` the type its result decodes to
    /// (`void` to ignore it).
    pub fn executeActivityByName(c: *Client, comptime Result: type, activity_type: []const u8, options: StartActivityOptions, args: anytype) StartActivityError!ActivityHandle(Result) {
        var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
        defer arena_state.deinit();
        const a = arena_state.allocator();
        const id = if (options.id.len > 0) options.id else try uuid(a, c.io);
        var request: wfs.StartActivityExecutionRequest = .{
            .namespace = c.namespace,
            .identity = c.identity,
            .request_id = try uuid(a, c.io),
            .activity_id = id,
            .activity_type = .{ .name = activity_type },
            .task_queue = .{ .name = options.task_queue, .kind = .TASK_QUEUE_KIND_NORMAL },
            .schedule_to_close_timeout = protoDuration(options.schedule_to_close_timeout),
            .schedule_to_start_timeout = protoDuration(options.schedule_to_start_timeout),
            .start_to_close_timeout = protoDuration(options.start_to_close_timeout),
            .heartbeat_timeout = protoDuration(options.heartbeat_timeout),
            .input = .{ .payloads = try converter.toPayloads(a, args) },
            .id_reuse_policy = switch (options.id_reuse_policy) {
                .unspecified => .ACTIVITY_ID_REUSE_POLICY_UNSPECIFIED,
                .allow_duplicate => .ACTIVITY_ID_REUSE_POLICY_ALLOW_DUPLICATE,
                .allow_duplicate_failed_only => .ACTIVITY_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY,
                .reject_duplicate => .ACTIVITY_ID_REUSE_POLICY_REJECT_DUPLICATE,
            },
            .id_conflict_policy = switch (options.id_conflict_policy) {
                .unspecified => .ACTIVITY_ID_CONFLICT_POLICY_UNSPECIFIED,
                .fail => .ACTIVITY_ID_CONFLICT_POLICY_FAIL,
                .use_existing => .ACTIVITY_ID_CONFLICT_POLICY_USE_EXISTING,
            },
        };
        if (options.retry_policy) |rp| request.retry_policy = try protoRetryPolicy(a, rp);
        if (options.search_attributes.len > 0) request.search_attributes = try search_attributes.toProto(a, options.search_attributes, .start);
        var status: ?RpcStatus = null;
        var header = try c.outgoingHeader(a, options.context);
        const response = c.rpcCall(wfs.StartActivityExecutionResponse, "StartActivityExecution", request, a, &status, &header, options.context) catch |err| switch (err) {
            error.AlreadyExists => return error.ActivityExecutionAlreadyStarted,
            error.RpcFailed => {
                const st = status orelse return error.RpcFailed;
                log.err("starting activity {s} of type \"{s}\" on task queue \"{s}\" failed: {s} (gRPC status {d})", .{ id, activity_type, options.task_queue, st.message, st.code });
                return error.RpcFailed;
            },
            else => |e| return e,
        };
        return c.getActivityHandle(Result, id, response.run_id);
    }

    /// A handle to an activity started with `executeActivity`, for example by another
    /// process. An empty `run_id` follows the latest run of `activity_id`. No call is made,
    /// so a missing activity shows up as `error.NotFound` from the handle's methods.
    pub fn getActivityHandle(c: *Client, comptime Result: type, activity_id: []const u8, run_id: []const u8) error{OutOfMemory}!ActivityHandle(Result) {
        const id = try c.gpa.dupe(u8, activity_id);
        errdefer c.gpa.free(id);
        return .{ .client = c, .id = id, .run_id = try c.gpa.dupe(u8, run_id) };
    }

    /// Lists the activities started with `executeActivity` that match the visibility
    /// `query`, such as `ActivityType = 'Charge' AND ExecutionStatus = 'Running'`; an empty
    /// query lists them all. Activities run by workflows aren't listed. Eventually
    /// consistent, like `listWorkflows`. The iterator fetches a page at a time as `next`
    /// reaches it; nothing is fetched here. Free it with `deinit`.
    pub fn listActivities(c: *Client, query: []const u8, options: ListActivitiesOptions) error{OutOfMemory}!ActivityExecutionIterator {
        return .{ .client = c, .query = try c.gpa.dupe(u8, query), .page_size = options.page_size, .page = .init(c.gpa) };
    }

    /// The number of activities started with `executeActivity` that match the visibility
    /// `query` (see `listActivities`); an empty query counts them all.
    pub fn countActivities(c: *Client, query: []const u8) ListError!u64 {
        var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
        defer arena_state.deinit();
        const a = arena_state.allocator();
        var status: ?RpcStatus = null;
        const response = c.rpcWithStatus(wfs.CountActivityExecutionsResponse, "CountActivityExecutions", wfs.CountActivityExecutionsRequest{
            .namespace = c.namespace,
            .query = query,
        }, a, &status) catch |err| return visibilityError(err, status, "counting activities", query);
        return std.math.cast(u64, response.count) orelse {
            log.err("CountActivityExecutions for query \"{s}\" returned the count {d}", .{ query, response.count });
            return error.InvalidResponse;
        };
    }

    /// Creates the schedule `id` and returns a handle to it. Returns
    /// `error.ScheduleAlreadyExists` if a schedule with that ID exists. The server rejects an
    /// invalid spec or action with `error.RpcFailed`, logging why.
    pub fn createSchedule(c: *Client, id: []const u8, options: schedule.CreateOptions) CreateScheduleError!ScheduleHandle {
        var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
        defer arena_state.deinit();
        const a = arena_state.allocator();
        var request: wfs.CreateScheduleRequest = .{
            .namespace = c.namespace,
            .schedule_id = id,
            .schedule = try options.schedule.toProto(a, null),
            .identity = c.identity,
            .request_id = try uuid(a, c.io),
        };
        if (options.trigger_immediately or options.backfill.len > 0) {
            var patch: proto.temporal.api.schedule.v1.SchedulePatch = .{};
            if (options.trigger_immediately) patch.trigger_immediately = .{};
            try patch.backfill_request.ensureTotalCapacity(a, options.backfill.len);
            for (options.backfill) |b| patch.backfill_request.appendAssumeCapacity(b.toProto());
            request.initial_patch = patch;
        }
        if (options.memo.len > 0) {
            var memo: proto.temporal.api.common.v1.Memo = .{};
            try memo.fields.ensureTotalCapacity(a, options.memo.len);
            for (options.memo) |field| memo.fields.appendAssumeCapacity(.{ .key = field.name, .value = field.value });
            request.memo = memo;
        }
        if (options.search_attributes.len > 0) request.search_attributes = try search_attributes.toProto(a, options.search_attributes, .start);
        _ = c.rpc(wfs.CreateScheduleResponse, "CreateSchedule", request, a) catch |err| switch (err) {
            error.AlreadyExists => return error.ScheduleAlreadyExists,
            else => |e| return e,
        };
        return c.getScheduleHandle(id);
    }

    /// A handle to the schedule `id`. No call is made, so a missing schedule shows up as
    /// `error.NotFound` from the handle's methods.
    pub fn getScheduleHandle(c: *Client, id: []const u8) error{OutOfMemory}!ScheduleHandle {
        return .{ .client = c, .id = try c.gpa.dupe(u8, id) };
    }

    /// Lists the namespace's schedules, or those matching `options.query`. Eventually
    /// consistent: a schedule created or changed a moment ago may be missing or out of date.
    /// The iterator fetches a page at a time as `next` reaches it; nothing is fetched here.
    /// Free it with `deinit`.
    pub fn listSchedules(c: *Client, options: schedule.ListOptions) error{OutOfMemory}!ScheduleIterator {
        return .{ .client = c, .query = try c.gpa.dupe(u8, options.query), .page_size = options.page_size, .page = .init(c.gpa) };
    }

    /// A handle to the Worker Deployment `name` in the client's namespace (see
    /// `temporal.deployment`). Nothing is sent here: a deployment that doesn't exist makes
    /// the handle's calls return `error.NotFound`. Free it with `deinit`.
    pub fn workerDeployment(c: *Client, name: []const u8) error{OutOfMemory}!WorkerDeploymentHandle {
        return .{ .client = c, .name = try c.gpa.dupe(u8, name) };
    }

    /// The client's namespace's Worker Deployments. The iterator fetches a page at a time as
    /// `next` reaches it; nothing is fetched here. Free it with `deinit`.
    pub fn listWorkerDeployments(c: *Client, options: ListWorkerDeploymentsOptions) WorkerDeploymentIterator {
        return .{ .client = c, .page_size = options.page_size, .page = .init(c.gpa) };
    }

    /// Sends `Query` (a `workflow.QueryDef`) with `args` to a workflow and returns the
    /// handler's result, decoded into `arena`. An empty `run_id` queries the latest run. A
    /// closed workflow still answers: the server has a worker replay its history. The server
    /// holds a query sent before the workflow's first workflow task completes until it does;
    /// with no worker polling, the call fails with `error.RpcFailed` when its deadline passes.
    pub fn queryWorkflow(c: *Client, comptime Query: type, arena: Allocator, workflow_id: []const u8, run_id: []const u8, args: Query.Args) QueryError!Query.Result {
        return c.queryWorkflowWithOptions(Query, arena, workflow_id, run_id, args, .{});
    }

    /// `queryWorkflow` with options. Returns `error.QueryFailed` when the handler fails or
    /// the workflow has no handler for the query type, and `error.QueryRejected` when
    /// `options.reject_condition` matches the workflow's state.
    pub fn queryWorkflowWithOptions(c: *Client, comptime Query: type, arena: Allocator, workflow_id: []const u8, run_id: []const u8, args: Query.Args, options: QueryOptions) QueryError!Query.Result {
        const result = try c.sendQuery(Query.query_name, arena, workflow_id, run_id, try converter.toPayloads(arena, args), options);
        if (Query.Result == void) return;
        return converter.fromPayload(Query.Result, arena, result orelse return error.InvalidPayload);
    }

    /// Sends the query named `query_type` with the tuple `args` to a workflow, for a caller
    /// with no `workflow.QueryDef`, and returns the handler's result undecoded: decode it
    /// with `get(T, arena)`. The value is absent when the server returns no result payload,
    /// as for a handler that returns nothing. The payload lives in `arena`. Otherwise
    /// behaves as `queryWorkflow`.
    pub fn queryWorkflowByName(c: *Client, query_type: []const u8, arena: Allocator, workflow_id: []const u8, run_id: []const u8, args: anytype) QueryError!converter.EncodedValue {
        return c.queryWorkflowByNameWithOptions(query_type, arena, workflow_id, run_id, args, .{});
    }

    /// `queryWorkflowByName` with options, which behave as in `queryWorkflowWithOptions`:
    /// `error.QueryFailed` when the handler fails or the workflow has no handler named
    /// `query_type`, and `error.QueryRejected` when `options.reject_condition` matches.
    pub fn queryWorkflowByNameWithOptions(c: *Client, query_type: []const u8, arena: Allocator, workflow_id: []const u8, run_id: []const u8, args: anytype, options: QueryOptions) QueryError!converter.EncodedValue {
        return .{ .payload = try c.sendQuery(query_type, arena, workflow_id, run_id, try converter.toPayloads(arena, args), options) };
    }

    /// Sends a query and returns its first result payload, or null when there is none.
    fn sendQuery(c: *Client, query_type: []const u8, arena: Allocator, workflow_id: []const u8, run_id: []const u8, query_args: std.ArrayList(converter.Payload), options: QueryOptions) QueryError!?converter.Payload {
        var status: ?RpcStatus = null;
        var header = try c.outgoingHeader(arena, options.context);
        const response = c.rpcCall(wfs.QueryWorkflowResponse, "QueryWorkflow", wfs.QueryWorkflowRequest{
            .namespace = c.namespace,
            .execution = .{ .workflow_id = workflow_id, .run_id = run_id },
            .query = .{ .query_type = query_type, .query_args = .{ .payloads = query_args } },
            .query_reject_condition = switch (options.reject_condition) {
                .none => .QUERY_REJECT_CONDITION_NONE,
                .not_open => .QUERY_REJECT_CONDITION_NOT_OPEN,
                .not_completed_cleanly => .QUERY_REJECT_CONDITION_NOT_COMPLETED_CLEANLY,
            },
        }, arena, &status, &header, options.context) catch |err| switch (err) {
            error.AlreadyExists => return error.InvalidResponse,
            error.RpcFailed => {
                const s = status orelse return error.RpcFailed;
                if (options.failure_message) |m| m.* = s.message;
                // The server reports a failed query (a handler error or an unknown query
                // type) as INVALID_ARGUMENT carrying a QueryFailedFailure detail.
                if (s.code == 3 and s.hasDetail("temporal.api.errordetails.v1.QueryFailedFailure", arena)) return error.QueryFailed;
                log.err("QueryWorkflow \"{s}\" on workflow {s} failed: {s} (gRPC status {d})", .{ query_type, workflow_id, s.message, s.code });
                return error.RpcFailed;
            },
            else => |e| return e,
        };
        if (response.query_rejected) |rejected| {
            if (options.rejected_status) |st| st.* = @fromBackingInt(@intCast(@backingInt(rejected.status)));
            return error.QueryRejected;
        }
        const payloads = response.query_result orelse return null;
        if (payloads.payloads.items.len == 0) return null;
        return payloads.payloads.items[0];
    }

    /// Sends `Update` (a `workflow.UpdateDef`) with `args` to a workflow and waits until the
    /// update reaches `options.wait_for_stage`. An empty `run_id` targets the latest run.
    /// Read the result with `get` on the returned handle; a rejected update (by its
    /// validator, its handler, or because the workflow has no handler for it) is
    /// `error.UpdateFailed` from `get`, not an error here. Returns `error.NotFound` if no
    /// such workflow exists.
    pub fn updateWorkflow(c: *Client, comptime Update: type, workflow_id: []const u8, run_id: []const u8, args: Update.Args, options: UpdateOptions) UpdateError!WorkflowUpdateHandle(Update.Result) {
        var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
        defer arena_state.deinit();
        const a = arena_state.allocator();
        const request = try c.updateRequest(a, Update.update_name, workflow_id, run_id, args, options);
        var header = try c.outgoingHeader(a, options.context);
        const response = try c.updateUntilStage(a, request, &header, options.context);
        const ref_run_id = if (response.update_ref) |r| if (r.workflow_execution) |e| e.run_id else "" else "";
        return .init(c, workflow_id, if (ref_run_id.len > 0) ref_run_id else run_id, request.request.?.meta.?.update_id, response.outcome);
    }

    /// A handle to the update `update_id` already sent to a workflow, for example by another
    /// process. `get` waits for its outcome. An empty `run_id` targets the latest run.
    pub fn getWorkflowUpdateHandle(c: *Client, comptime Result: type, workflow_id: []const u8, run_id: []const u8, update_id: []const u8) error{OutOfMemory}!WorkflowUpdateHandle(Result) {
        return .init(c, workflow_id, run_id, update_id, null);
    }

    /// A handle to an existing workflow, for waiting for its result with `get`. An empty
    /// `run_id` follows the latest run. No call is made, so a missing workflow shows up as
    /// `error.NotFound` from `get`.
    pub fn getWorkflow(c: *Client, comptime T: type, workflow_id: []const u8, run_id: []const u8) error{OutOfMemory}!WorkflowRun(T) {
        const id = try c.gpa.dupe(u8, workflow_id);
        errdefer c.gpa.free(id);
        return .{ .client = c, .id = id, .run_id = try c.gpa.dupe(u8, run_id) };
    }

    /// Sends `Update` with `update_args` to the workflow `start_options.id`, first starting it
    /// with `args` if it isn't running, in one atomic call. `start_options.id_conflict_policy`
    /// is required: `.use_existing` updates a running workflow, `.fail` returns
    /// `error.WorkflowExecutionAlreadyStarted` for one. The handle's `run_id` is the run that
    /// was started or updated; wait for the workflow's result with `getWorkflow`. An empty
    /// `start_options.id` generates a UUID, so the call always starts a new workflow.
    pub fn updateWithStartWorkflow(
        c: *Client,
        comptime Workflow: type,
        start_options: StartWorkflowOptions,
        args: Workflow.Args,
        comptime Update: type,
        update_args: Update.Args,
        update_options: UpdateOptions,
    ) UpdateWithStartError!WorkflowUpdateHandle(Update.Result) {
        if (start_options.id_conflict_policy == .unspecified) {
            log.warn("updateWithStartWorkflow for update \"{s}\" on workflow type \"{s}\": StartWorkflowOptions.id_conflict_policy must be set (for example .use_existing), the server requires it for update-with-start", .{ Update.update_name, Workflow.workflow_name });
            return error.InvalidOptions;
        }
        var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
        defer arena_state.deinit();
        const a = arena_state.allocator();
        const id = if (start_options.id.len > 0) start_options.id else try uuid(a, c.io);

        var update = try c.updateRequest(a, Update.update_name, id, "", update_args, update_options);
        update.request.?.input.?.header = try protoHeader(a, try c.outgoingHeader(a, update_options.context));
        var start = try c.startRequest(a, Workflow.workflow_name, id, start_options, args);
        start.header = try protoHeader(a, try c.outgoingHeader(a, start_options.context));
        const Operation = wfs.ExecuteMultiOperationRequest.Operation;
        var operations: std.ArrayList(Operation) = .empty;
        try operations.appendSlice(a, &.{
            .{ .operation = .{ .start_workflow = start } },
            .{ .operation = .{ .update_workflow = update } },
        });

        var status: ?RpcStatus = null;
        // Both operations' payloads go through the codecs with the start's values.
        const response = c.rpcCall(wfs.ExecuteMultiOperationResponse, "ExecuteMultiOperation", wfs.ExecuteMultiOperationRequest{
            .namespace = c.namespace,
            .operations = operations,
        }, a, &status, null, start_options.context) catch |err| switch (err) {
            // The call fails with the failing operation's status code, and only the start
            // operation can fail with ALREADY_EXISTS.
            error.AlreadyExists => return error.WorkflowExecutionAlreadyStarted,
            error.RpcFailed => {
                const s = status orelse return error.RpcFailed;
                return multiOperationError(s, a, id, Update.update_name);
            },
            else => |e| return e,
        };

        const responses = response.responses.items;
        if (responses.len != 2 or responses[0].response == null or responses[0].response.? != .start_workflow or
            responses[1].response == null or responses[1].response.? != .update_workflow)
        {
            log.err("ExecuteMultiOperation for workflow {s} returned {d} responses, want a start and an update response", .{ id, responses.len });
            return error.InvalidResponse;
        }
        const started = responses[0].response.?.start_workflow;
        var updated = responses[1].response.?.update_workflow;
        // The workflow exists now, so a wait the server cut short continues as a plain update
        // with the same update ID, which the server deduplicates.
        if (!reachedStage(updated, update_options.wait_for_stage)) {
            update.workflow_execution.?.run_id = started.run_id;
            updated = try c.updateUntilStage(a, update, null, update_options.context);
        }
        return .init(c, id, started.run_id, update.request.?.meta.?.update_id, updated.outcome);
    }

    /// Maps a failed ExecuteMultiOperation to an error, logging which operation failed.
    fn multiOperationError(s: RpcStatus, arena: Allocator, workflow_id: []const u8, update_name: []const u8) UpdateWithStartError {
        const detail_name = "temporal.api.errordetails.v1.MultiOperationExecutionFailure";
        var reader: Io.Reader = .fixed(s.details);
        const decoded = proto.google.rpc.Status.decode(&reader, arena) catch proto.google.rpc.Status{};
        for (decoded.details.items) |any| {
            if (!std.mem.endsWith(u8, any.type_url, "/" ++ detail_name)) continue;
            var detail_reader: Io.Reader = .fixed(any.value);
            const detail = proto.temporal.api.errordetails.v1.MultiOperationExecutionFailure.decode(&detail_reader, arena) catch break;
            for (detail.statuses.items, 0..) |op, i| {
                // The server marks the operations it didn't run because another failed as
                // ABORTED; only the failing one says why.
                if (op.code == 0 or op.code == 10) continue;
                const what = if (i == 0) "starting the workflow" else "the update";
                log.err("update-with-start of update \"{s}\" on workflow {s} failed: {s} failed: {s} (gRPC status {d})", .{ update_name, workflow_id, what, op.message, op.code });
            }
            return error.RpcFailed;
        }
        log.err("update-with-start of update \"{s}\" on workflow {s} failed: {s} (gRPC status {d})", .{ update_name, workflow_id, s.message, s.code });
        return error.RpcFailed;
    }

    fn updateRequest(c: *Client, a: Allocator, name: []const u8, workflow_id: []const u8, run_id: []const u8, args: anytype, options: UpdateOptions) error{OutOfMemory}!wfs.UpdateWorkflowExecutionRequest {
        return .{
            .namespace = c.namespace,
            .workflow_execution = .{ .workflow_id = workflow_id, .run_id = run_id },
            .wait_policy = .{ .lifecycle_stage = options.wait_for_stage.toProto() },
            .request = .{
                .meta = .{ .update_id = if (options.update_id.len > 0) options.update_id else try uuid(a, c.io), .identity = c.identity },
                .input = .{ .name = name, .args = .{ .payloads = try converter.toPayloads(a, args) } },
            },
        };
    }

    /// Sends `request` until the update reaches the stage it waits for: the server returns
    /// early, below that stage, when its long poll times out. `header` (null keeps the
    /// request's own) is sent as the update's header; `values` are the update's propagated
    /// values, for the codecs.
    fn updateUntilStage(c: *Client, a: Allocator, request: wfs.UpdateWorkflowExecutionRequest, header: ?*converter.HeaderWriter, values: ?*const propagation.Values) UpdateError!wfs.UpdateWorkflowExecutionResponse {
        const want: UpdateStage = if (request.wait_policy.?.lifecycle_stage == .UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED) .accepted else .completed;
        while (true) {
            const response = c.rpcCall(wfs.UpdateWorkflowExecutionResponse, "UpdateWorkflowExecution", request, a, null, header, values) catch |err| switch (err) {
                error.AlreadyExists => return error.InvalidResponse,
                else => |e| return e,
            };
            if (reachedStage(response, want)) return response;
        }
    }

    fn reachedStage(response: wfs.UpdateWorkflowExecutionResponse, want: UpdateStage) bool {
        return response.outcome != null or @backingInt(response.stage) >= @backingInt(want.toProto());
    }

    /// The header of an outgoing call, holding what the client's propagators write for
    /// `context` (no values when null), for the interceptors. Allocated from `a`.
    fn outgoingHeader(c: *Client, a: Allocator, context: ?*const propagation.Values) error{OutOfMemory}!converter.HeaderWriter {
        var w: converter.HeaderWriter = .init(a);
        try propagation.injectAll(c.propagators, a, context orelse &propagation.Values.empty, &w);
        return w;
    }

    /// `w`'s fields as a request header, or null when there are none. Allocated from `a`.
    fn protoHeader(a: Allocator, w: converter.HeaderWriter) error{OutOfMemory}!?proto.temporal.api.common.v1.Header {
        if (w.fields.items.len == 0) return null;
        return .{ .fields = try w.header().toEntries(proto.temporal.api.common.v1.Header.FieldsEntry, a) };
    }

    const RpcError = interceptor.ClientError;

    /// Calls a WorkflowService method with a typed request and response. The response's
    /// memory comes from `arena`.
    fn rpc(c: *Client, comptime Response: type, comptime method: []const u8, request: anytype, arena: Allocator) RpcError!Response {
        return c.rpcWithStatus(Response, method, request, arena, null);
    }

    /// `rpcWithStatus` without a header to send.
    fn rpcWithStatus(c: *Client, comptime Response: type, comptime method: []const u8, request: anytype, arena: Allocator, status: ?*?RpcStatus) RpcError!Response {
        return c.rpcCall(Response, method, request, arena, status, null, null);
    }

    /// Passes the call through the client's interceptors, which end in `send`. `header`, for
    /// a request that carries one, replaces the request's header (null leaves it as it is).
    /// `values` are the call's propagated values (its `.context`), which `send` hands the
    /// codecs.
    fn rpcCall(c: *Client, comptime Response: type, comptime method: []const u8, request: anytype, arena: Allocator, status: ?*?RpcStatus, header: ?*converter.HeaderWriter, values: ?*const propagation.Values) RpcError!Response {
        const Request = @TypeOf(request);
        var req = request;
        var resp: Response = .{};
        var call: interceptor.ClientCall = .{
            .method = method,
            .header = header,
            .arena = arena,
            .request_ptr = &req,
            .request_type = @typeName(Request),
            .response_ptr = &resp,
            .response_type = @typeName(Response),
        };
        const End = struct {
            client: *Client,
            status: ?*?RpcStatus,
            values: ?*const propagation.Values,

            const vtable: interceptor.Client.VTable = .{ .call = end };

            fn end(ptr: *anyopaque, input: *interceptor.ClientCall, _: interceptor.Client.Next) RpcError!void {
                const e: *@This() = @ptrCast(@alignCast(ptr));
                const r = input.request(Request).?;
                if (comptime carriesHeader(Request)) if (input.header) |w| setHeader(Request, r, try protoHeader(input.arena, w.*));
                input.response(Response).?.* = try e.client.send(Response, method, r.*, input.arena, e.status, e.values);
            }
        };
        var end: End = .{ .client = c, .status = status, .values = values };
        const next: interceptor.Client.Next = .{ .chain = c.interceptors, .root = .{ .ptr = &end, .vtable = &End.vtable } };
        try next.call(&call);
        return resp;
    }

    /// Fills `context`'s workflow and activity fields from those `r` names, for the codecs.
    fn requestIdentity(comptime Request: type, r: *const Request, context: *converter.CodecContext) void {
        const common = proto.temporal.api.common.v1;
        if (comptime Request == wfs.ExecuteMultiOperationRequest) {
            for (r.operations.items) |op| if (op.operation) |o| if (o == .start_workflow) return requestIdentity(wfs.StartWorkflowExecutionRequest, &o.start_workflow, context);
            return;
        }
        if (comptime @hasField(Request, "workflow_id") and @FieldType(Request, "workflow_id") == []const u8) context.workflow_id = r.workflow_id;
        inline for (.{ "workflow_execution", "execution" }) |name| {
            if (comptime @hasField(Request, name) and @FieldType(Request, name) == ?common.WorkflowExecution) {
                if (@field(r, name)) |e| context.workflow_id = e.workflow_id;
            }
        }
        if (comptime @hasField(Request, "workflow_type") and @FieldType(Request, "workflow_type") == ?common.WorkflowType) {
            if (r.workflow_type) |t| context.workflow_type = t.name;
        }
        if (comptime @hasField(Request, "activity_type") and @FieldType(Request, "activity_type") == ?common.ActivityType) {
            if (r.activity_type) |t| context.activity_type = t.name;
        }
        if (comptime @hasField(Request, "activity_id") and @FieldType(Request, "activity_id") == []const u8) context.activity_id = r.activity_id;
    }

    fn carriesHeader(comptime Request: type) bool {
        return @hasField(Request, "header") or Request == wfs.QueryWorkflowRequest or Request == wfs.UpdateWorkflowExecutionRequest;
    }

    /// Sets the header of a request that carries one (`carriesHeader`).
    fn setHeader(comptime Request: type, r: *Request, header: ?proto.temporal.api.common.v1.Header) void {
        if (comptime @hasField(Request, "header")) {
            r.header = header;
        } else if (Request == wfs.QueryWorkflowRequest) {
            if (r.query) |*q| q.header = header;
        } else if (Request == wfs.UpdateWorkflowExecutionRequest) {
            if (r.request) |*u| if (u.input) |*i| {
                i.header = header;
            };
        } else @compileError(@typeName(Request) ++ " carries no header");
    }

    /// A failed call's gRPC status; its slices borrow the call's arena.
    const RpcStatus = struct {
        code: u32,
        message: []const u8,
        /// Serialized google.rpc.Status, possibly empty.
        details: []const u8,

        /// Whether the status carries a detail of the protobuf message type `full_name`.
        fn hasDetail(s: RpcStatus, comptime full_name: []const u8, arena: Allocator) bool {
            var reader: Io.Reader = .fixed(s.details);
            const decoded = proto.google.rpc.Status.decode(&reader, arena) catch return false;
            for (decoded.details.items) |any| {
                if (std.mem.endsWith(u8, any.type_url, "/" ++ full_name)) return true;
            }
            return false;
        }

        /// The run ID an ALREADY_EXISTS start status names as holding the workflow ID, or
        /// null if it names none.
        fn alreadyStartedRunId(s: RpcStatus, arena: Allocator) ?[]const u8 {
            const full_name = "temporal.api.errordetails.v1.WorkflowExecutionAlreadyStartedFailure";
            var reader: Io.Reader = .fixed(s.details);
            const decoded = proto.google.rpc.Status.decode(&reader, arena) catch return null;
            for (decoded.details.items) |any| {
                if (!std.mem.endsWith(u8, any.type_url, "/" ++ full_name)) continue;
                var detail_reader: Io.Reader = .fixed(any.value);
                const detail = proto.temporal.api.errordetails.v1.WorkflowExecutionAlreadyStartedFailure.decode(&detail_reader, arena) catch return null;
                return if (detail.run_id.len > 0) detail.run_id else null;
            }
            return null;
        }
    };

    /// The deadline `send` gives a call: `rpc_timeout_ms`, or 0 (sdk-core's default) for a
    /// call that waits on the server for as long as it takes.
    fn callTimeoutMs(comptime Request: type, comptime method: []const u8, request: *const Request, rpc_timeout_ms: u32) u32 {
        const waits = comptime for ([_][]const u8{ "UpdateWorkflowExecution", "ExecuteMultiOperation", "PollWorkflowExecutionUpdate", "PollActivityExecution" }) |m| {
            if (std.mem.eql(u8, m, method)) break true;
        } else false;
        if (waits) return 0;
        if (comptime Request == wfs.GetWorkflowExecutionHistoryRequest) if (request.wait_new_event) return 0;
        return rpc_timeout_ms;
    }

    /// Sends the request to the server: the end of the interceptor chain. When `status` is
    /// given, a failure other than NOT_FOUND is stored there (allocated from `arena`) for the
    /// caller to report instead of being logged; ALREADY_EXISTS still returns
    /// `error.AlreadyExists`. The codecs encode the
    /// request and decode the response with `values` and the workflow or activity the request
    /// names.
    fn send(c: *Client, comptime Response: type, comptime method: []const u8, request: anytype, arena: Allocator, status: ?*?RpcStatus, values: ?*const propagation.Values) RpcError!Response {
        var coded = request;
        var codec_context: converter.CodecContext = .{ .values = values orelse &propagation.Values.empty, .namespace = c.namespace };
        requestIdentity(@TypeOf(coded), &coded, &codec_context);
        converter.encodeMessage(@TypeOf(coded), &coded, c.codecs, arena, c.io, &codec_context) catch |err| {
            if (err == error.CodecFailed) log.warn("{s} failed: encoding its payloads with the client's codecs failed", .{method});
            return err;
        };
        var encoded: Io.Writer.Allocating = .init(arena);
        coded.encode(&encoded.writer, arena) catch return error.OutOfMemory;
        var failure: ?bridge.Connection.RpcFailure = null;
        defer if (failure) |f| f.deinit(c.gpa);
        const call_options: bridge.Connection.CallOptions = .{ .retry = true, .timeout_ms = callTimeoutMs(@TypeOf(coded), method, &coded, c.rpc_timeout_ms) };
        const bytes = c.currentConnection().call(c.gpa, c.io, .workflow, method, encoded.written(), call_options, &failure) catch |err| switch (err) {
            error.RpcFailed => {
                const f = failure.?;
                // gRPC ALREADY_EXISTS and NOT_FOUND are expected outcomes callers handle.
                if (f.status_code == 5) return error.NotFound;
                if (status) |out| {
                    out.* = .{ .code = f.status_code, .message = try arena.dupe(u8, f.message), .details = try arena.dupe(u8, f.details) };
                    return if (f.status_code == 6) error.AlreadyExists else error.RpcFailed;
                }
                if (f.status_code == 6) return error.AlreadyExists;
                log.err("{s} failed: {s} (gRPC status {d})", .{ method, f.message, f.status_code });
                return error.RpcFailed;
            },
            error.InvalidOptions => return error.RpcFailed,
            else => |e| return e,
        };
        defer c.gpa.free(bytes);
        const owned = try arena.dupe(u8, bytes);
        var reader: Io.Reader = .fixed(owned);
        var response = Response.decode(&reader, arena) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            else => return error.InvalidResponse,
        };
        converter.decodeMessage(Response, &response, c.codecs, arena, c.io, &codec_context) catch |err| {
            if (err == error.CodecFailed) log.warn("{s} failed: decoding the response's payloads with the client's codecs failed", .{method});
            return err;
        };
        return response;
    }
};

pub const StartWorkflowOptions = struct {
    /// Workflow ID. Empty generates a random UUID; read it from the run.
    id: []const u8 = "",
    /// Task queue a worker polls for the workflow's tasks. Required for client starts, which
    /// return `error.InvalidOptions` when it is empty; `WorkflowEnvironment` fills in its own.
    task_queue: []const u8 = "",
    execution_timeout: ?Duration = null,
    run_timeout: ?Duration = null,
    task_timeout: ?Duration = null,
    /// Delay before the first workflow task is dispatched.
    start_delay: ?Duration = null,
    /// Runs the workflow on a cron schedule ("* * * * *"); each run starts a new run of the
    /// same workflow ID. Prefer Schedules for new code.
    cron_schedule: []const u8 = "",
    /// Whether a closed run's ID may be used again. `.unspecified` uses the server's default,
    /// `.allow_duplicate`.
    id_reuse_policy: enum { unspecified, allow_duplicate, allow_duplicate_failed_only, reject_duplicate } = .unspecified,
    /// What a start does when a run with the same ID is running. `.unspecified` and `.fail`
    /// fail the start with `error.WorkflowExecutionAlreadyStarted` (unless
    /// `error_when_already_started` is false), `.use_existing` returns the running run, and
    /// `.terminate_existing` terminates it and starts a new one.
    id_conflict_policy: enum { unspecified, fail, use_existing, terminate_existing } = .unspecified,
    /// When false, a start the server refuses because the workflow ID is taken returns a
    /// handle to the run that holds it instead of `error.WorkflowExecutionAlreadyStarted`:
    /// a running run under `id_conflict_policy` `.unspecified` or `.fail`, or a closed run
    /// `id_reuse_policy` keeps from being reused. Unlike `.use_existing`, the start request
    /// itself is unchanged. Only `executeWorkflow` and `executeWorkflowByName` use it.
    error_when_already_started: bool = true,
    /// How the server retries the whole workflow when a run fails or times out; each retry is
    /// a new run of the same ID. Null retries none.
    retry_policy: ?workflow.RetryPolicy = null,
    /// A short single-line description of the workflow, shown in the UI and CLI. Sent as a
    /// JSON string payload (through the codecs); empty sends none.
    static_summary: []const u8 = "",
    /// A longer description of the workflow, shown in the UI and CLI. Sent as a JSON string
    /// payload (through the codecs); empty sends none.
    static_details: []const u8 = "",
    /// Runs the workflow with a versioning behavior other than its definition's: pinned to a
    /// worker deployment version, or auto-upgrading. Null keeps the definition's.
    versioning_override: ?deployment.VersioningOverride = null,
    /// The workflow's task priority and fairness. Null uses the server's defaults.
    priority: ?workflow.Priority = null,
    /// Non-indexed values recorded with the workflow, such as a description. Build them with
    /// `converter.toFields`; they are only read while starting.
    memo: []const converter.Field = &.{},
    /// Indexed values for visibility queries, built with `search_attributes.Key.set`; unsets
    /// are ignored. Only read while starting. A value the server can't index fails the start
    /// with `error.InvalidSearchAttribute`.
    search_attributes: []const search_attributes.Update = &.{},
    /// Asks the server to hand the first workflow task back in the start response, to a
    /// worker for `task_queue` running on this same client, saving that worker a poll. The
    /// client reserves a workflow task slot on such a worker first; with none (or none free)
    /// the workflow starts normally. `WorkflowRun.eagerly_started` says which happened.
    /// Only `executeWorkflow` and `executeWorkflowByName` use it.
    enable_eager_start: bool = false,
    /// Values the client's propagators (`Options.propagators`) write into the start's header;
    /// the workflow reads them with `ctx.propagated()`. Signal-with-start and update-with-start
    /// send them with the start. Only read during the call.
    context: ?*const propagation.Values = null,
};

/// How `Client.signalWorkflowWithOptions` sends a signal.
pub const SignalOptions = struct {
    /// Values the client's propagators write into the signal's header. Only read during the
    /// call.
    context: ?*const propagation.Values = null,
};

/// Names an activity by its workflow and activity ID, for completing it without its task
/// token. An empty `run_id` targets the workflow's latest run.
pub const ActivityId = struct {
    workflow_id: []const u8,
    run_id: []const u8 = "",
    activity_id: []const u8,
};

pub const ActivityCompletionError = error{
    /// A payload codec (`Options.codecs`) failed; logged.
    CodecFailed,
    OutOfMemory,
    Canceled,
    RpcFailed,
    /// The activity is no longer waiting: it completed, failed, or timed out, or its
    /// workflow closed.
    NotFound,
    InvalidResponse,
};

pub const StartError = error{
    /// A payload codec (`Options.codecs`) failed; logged.
    CodecFailed,
    OutOfMemory,
    Canceled,
    RpcFailed,
    NotFound,
    InvalidResponse,
    WorkflowExecutionAlreadyStarted,
    /// `StartWorkflowOptions.search_attributes` holds a value the server can't index (logged).
    InvalidSearchAttribute,
    /// `StartWorkflowOptions.task_queue` is empty (logged). Returned before any call.
    InvalidOptions,
};

pub const SignalError = error{ OutOfMemory, Canceled, RpcFailed, NotFound, InvalidResponse, CodecFailed };

pub const DescribeError = error{ OutOfMemory, Canceled, RpcFailed, NotFound, InvalidResponse, CodecFailed };

pub const CheckHealthError = error{
    OutOfMemory,
    /// The calling task was canceled.
    Canceled,
    /// The call failed, for example because the server is unreachable or didn't answer
    /// within `Options.rpc_timeout`; logged with its gRPC status.
    RpcFailed,
    /// The server answered with a status other than SERVING; logged.
    NotServing,
    /// The server's answer isn't a health check response; logged.
    InvalidResponse,
};

pub const ListError = error{
    /// A payload codec (`Options.codecs`) failed; logged.
    CodecFailed,
    OutOfMemory,
    Canceled,
    /// Also an invalid query; the server's reason is logged with the query.
    RpcFailed,
    /// The client's namespace doesn't exist.
    NotFound,
    InvalidResponse,
};

pub const ListWorkflowsOptions = struct {
    /// Executions per page; 0 uses the server's default.
    page_size: u31 = 0,
};

/// A workflow run as a visibility query (`Client.listWorkflows`) reports it. Every slice
/// borrows the arena passed to `WorkflowExecutionIterator.next`.
pub const WorkflowExecutionInfo = struct {
    workflow_id: []const u8,
    run_id: []const u8,
    workflow_type: []const u8,
    task_queue: []const u8,
    status: WorkflowExecutionStatus,
    start_time: ?Io.Timestamp,
    /// When the first workflow task was, or will be, scheduled: later than `start_time` for a
    /// delayed start or a cron or retry backoff.
    execution_time: ?Io.Timestamp,
    /// Null while the run is open.
    close_time: ?Io.Timestamp,
    history_length: i64,
    /// Decode a value with `memoValue` or `converter.fieldValue`.
    memo: []const converter.Field,
    /// Read a value with `search_attributes.get(arena, key)`.
    search_attributes: search_attributes.View,
    /// The server's record, for anything not lifted into the fields above.
    raw: proto.temporal.api.workflow.v1.WorkflowExecutionInfo,

    /// Decodes the memo value called `name` as `T`, or returns null if the memo has no such
    /// value. Memory comes from `arena`.
    pub fn memoValue(i: *const WorkflowExecutionInfo, comptime T: type, arena: Allocator, name: []const u8) converter.DecodeError!?T {
        return converter.fieldValue(T, arena, i.memo, name);
    }
};

/// Lifts the fields of `info` into a `WorkflowExecutionInfo` that borrows it.
fn executionInfo(arena: Allocator, info: proto.temporal.api.workflow.v1.WorkflowExecutionInfo) error{OutOfMemory}!WorkflowExecutionInfo {
    const execution = info.execution orelse proto.temporal.api.common.v1.WorkflowExecution{};
    return .{
        .workflow_id = execution.workflow_id,
        .run_id = execution.run_id,
        .workflow_type = if (info.type) |t| t.name else "",
        .task_queue = info.task_queue,
        .status = @fromBackingInt(@intCast(@backingInt(info.status))),
        .start_time = timestamp(info.start_time),
        .execution_time = timestamp(info.execution_time),
        .close_time = timestamp(info.close_time),
        .history_length = info.history_length,
        .memo = if (info.memo) |m| try converter.fieldsFromMap(arena, m.fields.items) else &.{},
        .search_attributes = try .fromProto(arena, info.search_attributes),
        .raw = info,
    };
}

/// The workflow executions matching a visibility query, from `Client.listWorkflows`. Fetches
/// a page from the server whenever `next` runs past the last one. Owns its memory with the
/// client's allocator: free it with `deinit`. Not thread-safe.
pub const WorkflowExecutionIterator = struct {
    client: *Client,
    query: []u8,
    page_size: u31,
    /// Holds the current page.
    page: std.heap.ArenaAllocator,
    executions: []const proto.temporal.api.workflow.v1.WorkflowExecutionInfo = &.{},
    index: usize = 0,
    /// Where the next page starts; empty after the last page. Owned with the client's
    /// allocator.
    next_page_token: []u8 = &.{},
    fetched: bool = false,

    pub fn deinit(it: *WorkflowExecutionIterator) void {
        it.page.deinit();
        it.client.gpa.free(it.query);
        it.client.gpa.free(it.next_page_token);
        it.* = undefined;
    }

    /// The next matching execution, copied into `arena`, or null after the last one. A
    /// failed fetch can be retried by calling `next` again.
    pub fn next(it: *WorkflowExecutionIterator, arena: Allocator) ListError!?WorkflowExecutionInfo {
        // The server may return an empty page that still has a token.
        while (it.index == it.executions.len) {
            if (it.fetched and it.next_page_token.len == 0) return null;
            try it.fetch();
        }
        const raw = try it.executions[it.index].dupe(arena);
        it.index += 1;
        return try executionInfo(arena, raw);
    }

    fn fetch(it: *WorkflowExecutionIterator) ListError!void {
        const c = it.client;
        it.executions = &.{};
        it.index = 0;
        _ = it.page.reset(.retain_capacity);
        const a = it.page.allocator();
        var status: ?Client.RpcStatus = null;
        const response = c.rpcWithStatus(wfs.ListWorkflowExecutionsResponse, "ListWorkflowExecutions", wfs.ListWorkflowExecutionsRequest{
            .namespace = c.namespace,
            .page_size = it.page_size,
            .next_page_token = it.next_page_token,
            .query = it.query,
        }, a, &status) catch |err| return Client.visibilityError(err, status, "listing workflows", it.query);
        const token = try c.gpa.dupe(u8, response.next_page_token);
        c.gpa.free(it.next_page_token);
        it.next_page_token = token;
        it.executions = response.executions.items;
        it.fetched = true;
    }
};

/// How `Client.executeActivity` starts an activity. At least one of
/// `start_to_close_timeout` and `schedule_to_close_timeout` is required.
pub const StartActivityOptions = struct {
    /// Activity ID, unique among the namespace's running standalone activities. Empty
    /// generates a random UUID; read it from the handle.
    id: []const u8 = "",
    task_queue: []const u8,
    /// How long the activity may take, retries included.
    schedule_to_close_timeout: ?Duration = null,
    /// How long an attempt may wait for a worker to pick it up. Not retried.
    schedule_to_start_timeout: ?Duration = null,
    /// How long one attempt may run. Retried.
    start_to_close_timeout: ?Duration = null,
    /// The longest gap between heartbeats before an attempt times out. Also how soon a
    /// running activity learns of `cancel`, which reaches it through a heartbeat.
    heartbeat_timeout: ?Duration = null,
    /// Null retries with the server's default policy.
    retry_policy: ?workflow.RetryPolicy = null,
    /// Whether an ID of an activity that has closed may be reused.
    id_reuse_policy: enum { unspecified, allow_duplicate, allow_duplicate_failed_only, reject_duplicate } = .unspecified,
    /// What starting an ID that is still running does: `.fail` returns
    /// `error.ActivityExecutionAlreadyStarted`, `.use_existing` returns a handle to the
    /// running activity.
    id_conflict_policy: enum { unspecified, fail, use_existing } = .unspecified,
    /// Indexed values for `Client.listActivities` queries, built with
    /// `search_attributes.Key.set`; unsets are ignored. Only read while starting.
    search_attributes: []const search_attributes.Update = &.{},
    /// Values the client's propagators write into the activity's header; the activity reads
    /// them with `ctx.propagated()`. Only read during the call.
    context: ?*const propagation.Values = null,
};

pub const StartActivityError = error{
    /// A payload codec (`Options.codecs`) failed; logged.
    CodecFailed,
    OutOfMemory,
    Canceled,
    /// Also options or a server the call can't work with; the server's reason is logged.
    RpcFailed,
    /// The client's namespace doesn't exist.
    NotFound,
    InvalidResponse,
    /// An activity with this ID is running and `id_conflict_policy` isn't `.use_existing`.
    ActivityExecutionAlreadyStarted,
    /// `StartActivityOptions.search_attributes` holds a value the server can't index (logged).
    InvalidSearchAttribute,
};

pub const ActivityResultError = error{
    /// A payload codec (`Options.codecs`) failed; logged.
    CodecFailed,
    OutOfMemory,
    Canceled,
    RpcFailed,
    /// No such activity.
    NotFound,
    InvalidResponse,
    InvalidPayload,
    UnknownEncoding,
    WrongArgumentCount,
    /// The activity failed, retries exhausted or not retryable; read why with `failure()`.
    ActivityFailed,
    /// The activity was canceled after `ActivityHandle.cancel`.
    ActivityCanceled,
    ActivityTerminated,
    /// A timeout of `StartActivityOptions` passed.
    ActivityTimedOut,
};

pub const ActivityError = error{
    /// A payload codec (`Options.codecs`) failed; logged.
    CodecFailed,
    OutOfMemory,
    Canceled,
    RpcFailed,
    /// No such activity.
    NotFound,
    InvalidResponse,
    /// `cancel` or `terminate` of an activity that has already closed.
    ActivityClosed,
};

/// Where a standalone activity is in its lifecycle. Values match the server's; the enum is
/// open so a status added by a newer server is kept rather than rejected.
pub const ActivityExecutionStatus = enum(i32) {
    unspecified = 0,
    running = 1,
    completed = 2,
    failed = 3,
    canceled = 4,
    terminated = 5,
    timed_out = 6,
    paused = 7,
    _,
};

/// A standalone activity as `ActivityHandle.describe` reports it. Every slice borrows the
/// arena passed to `describe`.
pub const ActivityDescription = struct {
    activity_id: []const u8,
    run_id: []const u8,
    activity_type: []const u8,
    task_queue: []const u8,
    status: ActivityExecutionStatus,
    /// The current attempt, from 1.
    attempt: i32,
    schedule_time: ?Io.Timestamp,
    /// When the latest attempt started; null before a worker picked up the first.
    last_started_time: ?Io.Timestamp,
    /// Null while the activity is running.
    close_time: ?Io.Timestamp,
    /// Read a value with `search_attributes.get(arena, key)`.
    search_attributes: search_attributes.View,
    /// The server's record, with the last heartbeat's details and the last attempt's
    /// failure, for anything not lifted into the fields above.
    raw: proto.temporal.api.activity.v1.ActivityExecutionInfo,
};

pub const ListActivitiesOptions = struct {
    /// Activities per page; 0 uses the server's default.
    page_size: u31 = 0,
};

/// A standalone activity as a visibility query (`Client.listActivities`) reports it. Every
/// slice borrows the arena passed to `ActivityExecutionIterator.next`.
pub const ActivityExecutionInfo = struct {
    activity_id: []const u8,
    run_id: []const u8,
    activity_type: []const u8,
    task_queue: []const u8,
    status: ActivityExecutionStatus,
    schedule_time: ?Io.Timestamp,
    /// Null while the activity is running.
    close_time: ?Io.Timestamp,
    /// Read a value with `search_attributes.get(arena, key)`.
    search_attributes: search_attributes.View,
    /// The server's record, for anything not lifted into the fields above.
    raw: proto.temporal.api.activity.v1.ActivityExecutionListInfo,
};

/// A standalone activity, from `Client.executeActivity` or `Client.getActivityHandle`.
/// Owns its IDs with the client's allocator; free with `deinit`. Not thread-safe.
pub fn ActivityHandle(comptime T: type) type {
    return struct {
        const Self = @This();

        client: *Client,
        id: []u8,
        /// Empty when the handle follows the latest run of `id`.
        run_id: []u8,
        failure_: ?failure_mod.Failure = null,

        pub fn deinit(h: *Self) void {
            h.client.gpa.free(h.id);
            h.client.gpa.free(h.run_id);
            h.* = undefined;
        }

        /// Waits for the activity to close and returns its result. The result and any
        /// failure are allocated from `arena`.
        pub fn get(h: *Self, arena: Allocator) ActivityResultError!T {
            const c = h.client;
            const outcome = while (true) {
                // Each call is a long poll the server may end before the activity closes.
                var status: ?Client.RpcStatus = null;
                const response = c.rpcWithStatus(wfs.PollActivityExecutionResponse, "PollActivityExecution", wfs.PollActivityExecutionRequest{
                    .namespace = c.namespace,
                    .activity_id = h.id,
                    .run_id = h.run_id,
                }, arena, &status) catch |err| switch (err) {
                    error.AlreadyExists => return error.InvalidResponse,
                    error.RpcFailed => {
                        const s = status orelse return error.RpcFailed;
                        // DEADLINE_EXCEEDED: the long poll ran out of time.
                        if (s.code == 4) continue;
                        log.err("waiting for the result of activity {s} failed: {s} (gRPC status {d})", .{ h.id, s.message, s.code });
                        return error.RpcFailed;
                    },
                    else => |e| return e,
                };
                if (response.outcome) |o| break o;
            };
            switch (outcome.value orelse return error.InvalidResponse) {
                .result => |payloads| {
                    if (T == void) return;
                    if (payloads.payloads.items.len == 0) return error.InvalidPayload;
                    return converter.fromPayload(T, arena, payloads.payloads.items[0]);
                },
                .failure => |f| {
                    h.failure_ = f;
                    const info = f.failure_info orelse return error.ActivityFailed;
                    return switch (info) {
                        .canceled_failure_info => error.ActivityCanceled,
                        .terminated_failure_info => error.ActivityTerminated,
                        .timeout_failure_info => error.ActivityTimedOut,
                        else => error.ActivityFailed,
                    };
                },
            }
        }

        /// Why the activity didn't complete, after `get` returned `error.ActivityFailed`,
        /// `ActivityCanceled`, `ActivityTerminated` or `ActivityTimedOut`. Borrows the arena
        /// passed to `get`.
        pub fn failure(h: *const Self) ?*const failure_mod.Failure {
            return if (h.failure_) |*f| f else null;
        }

        /// The activity's status, type, attempt and times. The description and all its
        /// memory come from `arena`.
        pub fn describe(h: *const Self, arena: Allocator) ActivityError!ActivityDescription {
            const c = h.client;
            const response = c.rpc(wfs.DescribeActivityExecutionResponse, "DescribeActivityExecution", wfs.DescribeActivityExecutionRequest{
                .namespace = c.namespace,
                .activity_id = h.id,
                .run_id = h.run_id,
                .include_heartbeat_details = true,
                .include_last_failure = true,
            }, arena) catch |err| switch (err) {
                error.AlreadyExists => return error.InvalidResponse,
                else => |e| return e,
            };
            const info = response.info orelse {
                log.err("DescribeActivityExecution for activity {s} returned no activity info", .{h.id});
                return error.InvalidResponse;
            };
            return .{
                .activity_id = info.activity_id,
                .run_id = info.run_id,
                .activity_type = if (info.activity_type) |t| t.name else "",
                .task_queue = info.task_queue,
                .status = @fromBackingInt(@intCast(@backingInt(info.status))),
                .attempt = info.attempt,
                .schedule_time = timestamp(info.schedule_time),
                .last_started_time = timestamp(info.last_started_time),
                .close_time = timestamp(info.close_time),
                .search_attributes = try .fromProto(arena, info.search_attributes),
                .raw = info,
            };
        }

        /// Requests the activity's cancellation. A running attempt learns of it from its
        /// next heartbeat (see `StartActivityOptions.heartbeat_timeout`) as `std.Io`
        /// cancellation, and the activity ends canceled once it returns `error.Canceled`.
        /// `reason` is recorded with the request. Returns `error.ActivityClosed` if the
        /// activity has already closed.
        pub fn cancel(h: *const Self, reason: []const u8) ActivityError!void {
            const c = h.client;
            var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
            defer arena_state.deinit();
            const a = arena_state.allocator();
            var status: ?Client.RpcStatus = null;
            _ = c.rpcWithStatus(wfs.RequestCancelActivityExecutionResponse, "RequestCancelActivityExecution", wfs.RequestCancelActivityExecutionRequest{
                .namespace = c.namespace,
                .activity_id = h.id,
                .run_id = h.run_id,
                .identity = c.identity,
                .request_id = try uuid(a, c.io),
                .reason = reason,
            }, a, &status) catch |err| return h.controlError(err, status, "canceling");
        }

        fn controlError(h: *const Self, err: Client.RpcError, status: ?Client.RpcStatus, comptime what: []const u8) ActivityError {
            switch (err) {
                error.AlreadyExists => return error.InvalidResponse,
                error.RpcFailed => {
                    const s = status orelse return error.RpcFailed;
                    // FAILED_PRECONDITION: the activity is in a terminal state.
                    if (s.code == 9) return error.ActivityClosed;
                    log.err(what ++ " activity {s} failed: {s} (gRPC status {d})", .{ h.id, s.message, s.code });
                    return error.RpcFailed;
                },
                else => |e| return e,
            }
        }

        /// Terminates the activity: it closes at once, and a running attempt's result is
        /// discarded. Returns `error.ActivityClosed` if the activity has already closed.
        pub fn terminate(h: *const Self, reason: []const u8) ActivityError!void {
            const c = h.client;
            var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
            defer arena_state.deinit();
            const a = arena_state.allocator();
            var status: ?Client.RpcStatus = null;
            _ = c.rpcWithStatus(wfs.TerminateActivityExecutionResponse, "TerminateActivityExecution", wfs.TerminateActivityExecutionRequest{
                .namespace = c.namespace,
                .activity_id = h.id,
                .run_id = h.run_id,
                .identity = c.identity,
                .request_id = try uuid(a, c.io),
                .reason = reason,
            }, a, &status) catch |err| return h.controlError(err, status, "terminating");
        }
    };
}

/// The standalone activities matching a visibility query, from `Client.listActivities`.
/// Fetches a page from the server whenever `next` runs past the last one. Owns its memory
/// with the client's allocator: free it with `deinit`. Not thread-safe.
pub const ActivityExecutionIterator = struct {
    client: *Client,
    query: []u8,
    page_size: u31,
    /// Holds the current page.
    page: std.heap.ArenaAllocator,
    executions: []const proto.temporal.api.activity.v1.ActivityExecutionListInfo = &.{},
    index: usize = 0,
    /// Where the next page starts; empty after the last page. Owned with the client's
    /// allocator.
    next_page_token: []u8 = &.{},
    fetched: bool = false,

    pub fn deinit(it: *ActivityExecutionIterator) void {
        it.page.deinit();
        it.client.gpa.free(it.query);
        it.client.gpa.free(it.next_page_token);
        it.* = undefined;
    }

    /// The next matching activity, copied into `arena`, or null after the last one. A
    /// failed fetch can be retried by calling `next` again.
    pub fn next(it: *ActivityExecutionIterator, arena: Allocator) ListError!?ActivityExecutionInfo {
        // The server may return an empty page that still has a token.
        while (it.index == it.executions.len) {
            if (it.fetched and it.next_page_token.len == 0) return null;
            try it.fetch();
        }
        const raw = try it.executions[it.index].dupe(arena);
        it.index += 1;
        return .{
            .activity_id = raw.activity_id,
            .run_id = raw.run_id,
            .activity_type = if (raw.activity_type) |t| t.name else "",
            .task_queue = raw.task_queue,
            .status = @fromBackingInt(@intCast(@backingInt(raw.status))),
            .schedule_time = timestamp(raw.schedule_time),
            .close_time = timestamp(raw.close_time),
            .search_attributes = try .fromProto(arena, raw.search_attributes),
            .raw = raw,
        };
    }

    fn fetch(it: *ActivityExecutionIterator) ListError!void {
        const c = it.client;
        it.executions = &.{};
        it.index = 0;
        _ = it.page.reset(.retain_capacity);
        const a = it.page.allocator();
        var status: ?Client.RpcStatus = null;
        const response = c.rpcWithStatus(wfs.ListActivityExecutionsResponse, "ListActivityExecutions", wfs.ListActivityExecutionsRequest{
            .namespace = c.namespace,
            .page_size = it.page_size,
            .next_page_token = it.next_page_token,
            .query = it.query,
        }, a, &status) catch |err| return Client.visibilityError(err, status, "listing activities", it.query);
        const token = try c.gpa.dupe(u8, response.next_page_token);
        c.gpa.free(it.next_page_token);
        it.next_page_token = token;
        it.executions = response.executions.items;
        it.fetched = true;
    }
};

pub const CreateScheduleError = error{
    /// A payload codec (`Options.codecs`) failed; logged.
    CodecFailed,
    OutOfMemory,
    Canceled,
    /// Also an invalid schedule; the server's reason is logged.
    RpcFailed,
    /// The client's namespace doesn't exist.
    NotFound,
    InvalidResponse,
    ScheduleAlreadyExists,
    /// A search attribute value the server can't index (logged).
    InvalidSearchAttribute,
};

pub const ScheduleError = error{
    /// A payload codec (`Options.codecs`) failed; logged.
    CodecFailed,
    OutOfMemory,
    Canceled,
    RpcFailed,
    /// No such schedule.
    NotFound,
    InvalidResponse,
};

/// A schedule on the server, from `Client.createSchedule` or `Client.getScheduleHandle`.
/// Owns its ID with the client's allocator; free with `deinit`. Each method is one call to
/// the server, so a handle can be used from several threads at once.
pub const ScheduleHandle = struct {
    client: *Client,
    id: []u8,

    pub fn deinit(h: *ScheduleHandle) void {
        h.client.gpa.free(h.id);
        h.* = undefined;
    }

    /// The schedule, its recent and upcoming actions, memo and search attributes. The
    /// description and all its memory come from `arena`.
    pub fn describe(h: *const ScheduleHandle, arena: Allocator) ScheduleError!schedule.Description {
        const c = h.client;
        const response = c.rpc(wfs.DescribeScheduleResponse, "DescribeSchedule", wfs.DescribeScheduleRequest{
            .namespace = c.namespace,
            .schedule_id = h.id,
        }, arena) catch |err| switch (err) {
            error.AlreadyExists => return error.InvalidResponse,
            else => |e| return e,
        };
        return schedule.Description.fromProto(arena, h.id, response);
    }

    /// Changes the schedule by read-modify-write: describes it, calls
    /// `f(context, arena, &description)`, and sends the `schedule.Update` it returns; null
    /// leaves the schedule as it is. The update carries the description's conflict token, so
    /// the server doesn't apply it over a change made since the describe. The server applies
    /// the update's search attributes a moment after the call returns.
    ///
    /// The description lives in `arena`, and anything the callback allocates from it lives
    /// until the request is sent; `update` frees it all before returning.
    ///
    ///     try handle.update(Io.Duration.fromSeconds(3600), struct {
    ///         fn f(every: Io.Duration, arena: Allocator, d: *schedule.Description) !?schedule.Update {
    ///             const intervals = try arena.alloc(schedule.Interval, d.schedule.spec.intervals.len + 1);
    ///             @memcpy(intervals[1..], d.schedule.spec.intervals);
    ///             intervals[0] = .{ .every = every };
    ///             d.schedule.spec.intervals = intervals;
    ///             return .{ .schedule = d.schedule };
    ///         }
    ///     }.f);
    pub fn update(h: *const ScheduleHandle, context: anytype, comptime f: anytype) (UpdateScheduleError || CallbackError(f))!void {
        const c = h.client;
        var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
        defer arena_state.deinit();
        const a = arena_state.allocator();
        var description = try h.describe(a);
        const changes: schedule.Update = (try f(context, a, &description)) orelse return;
        _ = c.rpc(wfs.UpdateScheduleResponse, "UpdateSchedule", wfs.UpdateScheduleRequest{
            .namespace = c.namespace,
            .schedule_id = h.id,
            .schedule = try changes.schedule.toProto(a, description.raw.schedule),
            .conflict_token = description.raw.conflict_token,
            .identity = c.identity,
            .request_id = try uuid(a, c.io),
            .search_attributes = if (changes.search_attributes) |updates| try search_attributes.toProto(a, updates, .start) else null,
        }, a) catch |err| switch (err) {
            error.AlreadyExists => return error.InvalidResponse,
            else => |e| return e,
        };
    }

    fn CallbackError(comptime f: anytype) type {
        const Return = @typeInfo(@TypeOf(f)).@"fn".return_type.?;
        return switch (@typeInfo(Return)) {
            .error_union => |eu| eu.error_set,
            else => error{},
        };
    }

    /// Takes the schedule's action now, whatever its spec and even while it is paused.
    /// `.unspecified` uses the schedule's overlap policy.
    pub fn trigger(h: *const ScheduleHandle, overlap: schedule.OverlapPolicy) ScheduleError!void {
        return h.patch(.{ .trigger_immediately = .{ .overlap_policy = @fromBackingInt(@backingInt(overlap)) } });
    }

    /// Takes the actions the schedule would have taken in each period of `periods`.
    pub fn backfill(h: *const ScheduleHandle, periods: []const schedule.Backfill) ScheduleError!void {
        var arena_state: std.heap.ArenaAllocator = .init(h.client.gpa);
        defer arena_state.deinit();
        const requests = try arena_state.allocator().alloc(proto.temporal.api.schedule.v1.BackfillRequest, periods.len);
        for (periods, requests) |p, *r| r.* = p.toProto();
        return h.patch(.{ .backfill_request = .fromOwnedSlice(requests) });
    }

    /// Pauses the schedule: it takes no action until `unpause` (a `trigger` still runs it).
    /// `note` says why; empty records "Paused".
    pub fn pause(h: *const ScheduleHandle, note: []const u8) ScheduleError!void {
        return h.patch(.{ .pause = if (note.len > 0) note else "Paused" });
    }

    /// Resumes a paused schedule. `note` says why; empty records "Unpaused".
    pub fn unpause(h: *const ScheduleHandle, note: []const u8) ScheduleError!void {
        return h.patch(.{ .unpause = if (note.len > 0) note else "Unpaused" });
    }

    fn patch(h: *const ScheduleHandle, p: proto.temporal.api.schedule.v1.SchedulePatch) ScheduleError!void {
        const c = h.client;
        var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
        defer arena_state.deinit();
        const a = arena_state.allocator();
        _ = c.rpc(wfs.PatchScheduleResponse, "PatchSchedule", wfs.PatchScheduleRequest{
            .namespace = c.namespace,
            .schedule_id = h.id,
            .patch = p,
            .identity = c.identity,
            .request_id = try uuid(a, c.io),
        }, a) catch |err| switch (err) {
            error.AlreadyExists => return error.InvalidResponse,
            else => |e| return e,
        };
    }

    /// Deletes the schedule. Workflows it started keep running.
    pub fn delete(h: *const ScheduleHandle) ScheduleError!void {
        const c = h.client;
        var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
        defer arena_state.deinit();
        _ = c.rpc(wfs.DeleteScheduleResponse, "DeleteSchedule", wfs.DeleteScheduleRequest{
            .namespace = c.namespace,
            .schedule_id = h.id,
            .identity = c.identity,
        }, arena_state.allocator()) catch |err| switch (err) {
            error.AlreadyExists => return error.InvalidResponse,
            else => |e| return e,
        };
    }
};

pub const UpdateScheduleError = ScheduleError || error{
    /// A search attribute value in the update the server can't index (logged).
    InvalidSearchAttribute,
};

/// The schedules from `Client.listSchedules`. Fetches a page from the server whenever `next`
/// runs past the last one. Owns its memory with the client's allocator: free it with
/// `deinit`. Not thread-safe.
pub const ScheduleIterator = struct {
    client: *Client,
    query: []u8,
    page_size: u31,
    /// Holds the current page.
    page: std.heap.ArenaAllocator,
    entries: []const proto.temporal.api.schedule.v1.ScheduleListEntry = &.{},
    index: usize = 0,
    /// Where the next page starts; empty after the last page. Owned with the client's
    /// allocator.
    next_page_token: []u8 = &.{},
    fetched: bool = false,

    pub fn deinit(it: *ScheduleIterator) void {
        it.page.deinit();
        it.client.gpa.free(it.query);
        it.client.gpa.free(it.next_page_token);
        it.* = undefined;
    }

    /// The next schedule, copied into `arena`, or null after the last one. A failed fetch
    /// can be retried by calling `next` again.
    pub fn next(it: *ScheduleIterator, arena: Allocator) ListError!?schedule.ListEntry {
        // The server may return an empty page that still has a token.
        while (it.index == it.entries.len) {
            if (it.fetched and it.next_page_token.len == 0) return null;
            try it.fetch();
        }
        const raw = try it.entries[it.index].dupe(arena);
        it.index += 1;
        return try schedule.ListEntry.fromProto(arena, raw);
    }

    fn fetch(it: *ScheduleIterator) ListError!void {
        const c = it.client;
        it.entries = &.{};
        it.index = 0;
        _ = it.page.reset(.retain_capacity);
        const a = it.page.allocator();
        var status: ?Client.RpcStatus = null;
        const response = c.rpcWithStatus(wfs.ListSchedulesResponse, "ListSchedules", wfs.ListSchedulesRequest{
            .namespace = c.namespace,
            .maximum_page_size = it.page_size,
            .next_page_token = it.next_page_token,
            .query = it.query,
        }, a, &status) catch |err| switch (err) {
            error.AlreadyExists => return error.InvalidResponse,
            error.RpcFailed => {
                const s = status orelse return error.RpcFailed;
                log.err("listing schedules with the query \"{s}\" failed: {s} (gRPC status {d})", .{ it.query, s.message, s.code });
                return error.RpcFailed;
            },
            else => |e| return e,
        };
        const token = try c.gpa.dupe(u8, response.next_page_token);
        c.gpa.free(it.next_page_token);
        it.next_page_token = token;
        it.entries = response.schedules.items;
        it.fetched = true;
    }
};

pub const WorkerDeploymentError = error{
    /// A payload codec (`Options.codecs`) failed; logged.
    CodecFailed,
    OutOfMemory,
    Canceled,
    /// The server refused; its reason is logged with the deployment, such as a version
    /// with no pollers or one still current, ramping, or draining.
    RpcFailed,
    /// No such deployment, or no such version of it.
    NotFound,
    InvalidResponse,
};

pub const ListWorkerDeploymentsOptions = struct {
    /// Deployments per page; 0 lets the server choose.
    page_size: u31 = 0,
};

/// A Worker Deployment on the server, from `Client.workerDeployment`. Owns its name with the
/// client's allocator; free with `deinit`. Each method is one call to the server, so a
/// handle can be used from several threads at once. Changes are recorded with the client's
/// identity. Build IDs name the deployment's versions.
pub const WorkerDeploymentHandle = struct {
    client: *Client,
    name: []u8,

    pub fn deinit(h: *WorkerDeploymentHandle) void {
        h.client.gpa.free(h.name);
        h.* = undefined;
    }

    /// The deployment: its routing, its versions and their status. All memory comes from
    /// `arena`.
    pub fn describe(h: *const WorkerDeploymentHandle, arena: Allocator) WorkerDeploymentError!deployment.Description {
        const c = h.client;
        const response = try h.call(wfs.DescribeWorkerDeploymentResponse, "DescribeWorkerDeployment", wfs.DescribeWorkerDeploymentRequest{
            .namespace = c.namespace,
            .deployment_name = h.name,
        }, arena, "");
        const info = response.worker_deployment_info orelse return error.InvalidResponse;
        return deployment.Description.fromProto(arena, response.conflict_token, info);
    }

    /// Routes the deployment's new workflows, and its auto-upgrade runs at their next
    /// workflow task, to version `build_id`; null routes them to unversioned workers. A
    /// version becomes known once one of its workers has polled. The change reaches the
    /// task queues a moment after this returns (`Description.routing_update`). The result's
    /// memory comes from `arena`.
    pub fn setCurrentVersion(h: *const WorkerDeploymentHandle, arena: Allocator, build_id: ?[]const u8, options: deployment.RoutingOptions) WorkerDeploymentError!deployment.RoutingChange {
        const c = h.client;
        const response = try h.call(wfs.SetWorkerDeploymentCurrentVersionResponse, "SetWorkerDeploymentCurrentVersion", wfs.SetWorkerDeploymentCurrentVersionRequest{
            .namespace = c.namespace,
            .deployment_name = h.name,
            .build_id = build_id orelse "",
            .conflict_token = options.conflict_token,
            .identity = c.identity,
            .ignore_missing_task_queues = options.ignore_missing_task_queues,
            .allow_no_pollers = options.allow_no_pollers,
        }, arena, build_id orelse "");
        return .fromVersion(response.conflict_token, response.previous_deployment_version, 0);
    }

    /// Routes `percentage` percent (0 to 100) of what goes to the current version to version
    /// `build_id` instead; null ramps unversioned workers. A new ramping version replaces
    /// the previous one. The result's memory comes from `arena`.
    pub fn setRampingVersion(h: *const WorkerDeploymentHandle, arena: Allocator, build_id: ?[]const u8, percentage: f32, options: deployment.RoutingOptions) WorkerDeploymentError!deployment.RoutingChange {
        return h.ramp(arena, build_id orelse "", percentage, options);
    }

    /// Stops ramping: everything goes to the current version. The result's memory comes
    /// from `arena`.
    pub fn unsetRampingVersion(h: *const WorkerDeploymentHandle, arena: Allocator, options: deployment.RoutingOptions) WorkerDeploymentError!deployment.RoutingChange {
        return h.ramp(arena, "", 0, options);
    }

    fn ramp(h: *const WorkerDeploymentHandle, arena: Allocator, build_id: []const u8, percentage: f32, options: deployment.RoutingOptions) WorkerDeploymentError!deployment.RoutingChange {
        const c = h.client;
        const response = try h.call(wfs.SetWorkerDeploymentRampingVersionResponse, "SetWorkerDeploymentRampingVersion", wfs.SetWorkerDeploymentRampingVersionRequest{
            .namespace = c.namespace,
            .deployment_name = h.name,
            .build_id = build_id,
            .percentage = percentage,
            .conflict_token = options.conflict_token,
            .identity = c.identity,
            .ignore_missing_task_queues = options.ignore_missing_task_queues,
            .allow_no_pollers = options.allow_no_pollers,
        }, arena, build_id);
        return .fromVersion(response.conflict_token, response.previous_deployment_version, response.previous_percentage);
    }

    /// Version `build_id` of the deployment: its status, task queues, drainage and
    /// metadata. All memory comes from `arena`.
    pub fn describeVersion(h: *const WorkerDeploymentHandle, arena: Allocator, build_id: []const u8) WorkerDeploymentError!deployment.VersionDescription {
        const c = h.client;
        const response = try h.call(wfs.DescribeWorkerDeploymentVersionResponse, "DescribeWorkerDeploymentVersion", wfs.DescribeWorkerDeploymentVersionRequest{
            .namespace = c.namespace,
            .deployment_version = .{ .deployment_name = h.name, .build_id = build_id },
        }, arena, build_id);
        const info = response.worker_deployment_version_info orelse return error.InvalidResponse;
        return deployment.VersionDescription.fromProto(arena, info);
    }

    /// Deletes version `build_id`. The server refuses (`error.RpcFailed`, logged) while
    /// the version is current or ramping, while its workers poll, and while it drains,
    /// unless `skip_drainage`.
    pub fn deleteVersion(h: *const WorkerDeploymentHandle, build_id: []const u8, options: deployment.DeleteVersionOptions) WorkerDeploymentError!void {
        const c = h.client;
        var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
        defer arena_state.deinit();
        _ = try h.call(wfs.DeleteWorkerDeploymentVersionResponse, "DeleteWorkerDeploymentVersion", wfs.DeleteWorkerDeploymentVersionRequest{
            .namespace = c.namespace,
            .deployment_version = .{ .deployment_name = h.name, .build_id = build_id },
            .skip_drainage = options.skip_drainage,
            .identity = c.identity,
        }, arena_state.allocator(), build_id);
    }

    /// Sets and removes metadata of version `build_id`: `upserts` is a struct literal, one
    /// field per key, encoded like `converter.toFields`; `removals` are keys to delete.
    ///
    ///     try shop.updateVersionMetadata("b2", .{ .release = "2026-11", .approved = true }, &.{"draft"});
    pub fn updateVersionMetadata(h: *const WorkerDeploymentHandle, build_id: []const u8, upserts: anytype, removals: []const []const u8) WorkerDeploymentError!void {
        const c = h.client;
        var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
        defer arena_state.deinit();
        const a = arena_state.allocator();
        const fields = try converter.toFields(a, upserts);
        var request: wfs.UpdateWorkerDeploymentVersionMetadataRequest = .{
            .namespace = c.namespace,
            .deployment_version = .{ .deployment_name = h.name, .build_id = build_id },
            .identity = c.identity,
        };
        try request.upsert_entries.ensureTotalCapacity(a, fields.len);
        for (fields) |f| request.upsert_entries.appendAssumeCapacity(.{ .key = f.name, .value = f.value });
        try request.remove_entries.appendSlice(a, removals);
        _ = try h.call(wfs.UpdateWorkerDeploymentVersionMetadataResponse, "UpdateWorkerDeploymentVersionMetadata", request, a, build_id);
    }

    /// Sets the deployment's manager identity, the only identity then allowed to change its
    /// routing: `identity`, or this client's when null; an empty one lets anyone again.
    /// Returns the previous manager identity, allocated from `arena`.
    pub fn setManagerIdentity(h: *const WorkerDeploymentHandle, arena: Allocator, identity: ?[]const u8, options: deployment.ManagerOptions) WorkerDeploymentError![]const u8 {
        const c = h.client;
        const response = try h.call(wfs.SetWorkerDeploymentManagerResponse, "SetWorkerDeploymentManager", wfs.SetWorkerDeploymentManagerRequest{
            .namespace = c.namespace,
            .deployment_name = h.name,
            .conflict_token = options.conflict_token,
            .identity = c.identity,
            .new_manager_identity = if (identity) |i| .{ .manager_identity = i } else .{ .self = true },
        }, arena, "");
        return response.previous_manager_identity;
    }

    /// Deletes the deployment. The server refuses (`error.RpcFailed`, logged) while it has
    /// versions; delete them first.
    pub fn delete(h: *const WorkerDeploymentHandle) WorkerDeploymentError!void {
        const c = h.client;
        var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
        defer arena_state.deinit();
        _ = try h.call(wfs.DeleteWorkerDeploymentResponse, "DeleteWorkerDeployment", wfs.DeleteWorkerDeploymentRequest{
            .namespace = c.namespace,
            .deployment_name = h.name,
            .identity = c.identity,
        }, arena_state.allocator(), "");
    }

    /// Sends a request about the deployment (or its version `build_id`, when not empty),
    /// logging the server's reason for a refusal.
    fn call(h: *const WorkerDeploymentHandle, comptime Response: type, comptime method: []const u8, request: anytype, arena: Allocator, build_id: []const u8) WorkerDeploymentError!Response {
        var status: ?Client.RpcStatus = null;
        return h.client.rpcWithStatus(Response, method, request, arena, &status) catch |err| switch (err) {
            error.AlreadyExists => error.InvalidResponse,
            error.RpcFailed => {
                const s = status orelse return error.RpcFailed;
                log.warn("{s} for worker deployment {s}{s}{s} failed: {s} (gRPC status {d})", .{ method, h.name, if (build_id.len > 0) ", build ID " else "", build_id, s.message, s.code });
                return error.RpcFailed;
            },
            else => |e| e,
        };
    }
};

/// The deployments from `Client.listWorkerDeployments`. Fetches a page from the server
/// whenever `next` runs past the last one. Owns its memory with the client's allocator: free
/// it with `deinit`. Not thread-safe.
pub const WorkerDeploymentIterator = struct {
    client: *Client,
    page_size: u31,
    /// Holds the current page.
    page: std.heap.ArenaAllocator,
    entries: []const wfs.ListWorkerDeploymentsResponse.WorkerDeploymentSummary = &.{},
    index: usize = 0,
    /// Where the next page starts; empty after the last page. Owned with the client's
    /// allocator.
    next_page_token: []u8 = &.{},
    fetched: bool = false,

    pub fn deinit(it: *WorkerDeploymentIterator) void {
        it.page.deinit();
        it.client.gpa.free(it.next_page_token);
        it.* = undefined;
    }

    /// The next deployment, copied into `arena`, or null after the last one. A failed fetch
    /// can be retried by calling `next` again.
    pub fn next(it: *WorkerDeploymentIterator, arena: Allocator) ListError!?deployment.ListEntry {
        while (it.index == it.entries.len) {
            if (it.fetched and it.next_page_token.len == 0) return null;
            try it.fetch();
        }
        const raw = try it.entries[it.index].dupe(arena);
        it.index += 1;
        return deployment.ListEntry.fromProto(raw);
    }

    fn fetch(it: *WorkerDeploymentIterator) ListError!void {
        const c = it.client;
        it.entries = &.{};
        it.index = 0;
        _ = it.page.reset(.retain_capacity);
        const a = it.page.allocator();
        const response = c.rpc(wfs.ListWorkerDeploymentsResponse, "ListWorkerDeployments", wfs.ListWorkerDeploymentsRequest{
            .namespace = c.namespace,
            .page_size = it.page_size,
            .next_page_token = it.next_page_token,
        }, a) catch |err| switch (err) {
            error.AlreadyExists => return error.InvalidResponse,
            else => |e| return e,
        };
        const token = try c.gpa.dupe(u8, response.next_page_token);
        c.gpa.free(it.next_page_token);
        it.next_page_token = token;
        it.entries = response.worker_deployments.items;
        it.fetched = true;
    }
};

pub const History = history_mod.History;
pub const HistoryEvent = history_mod.HistoryEvent;

pub const HistoryError = error{
    /// A payload codec (`Options.codecs`) failed; logged.
    CodecFailed,
    OutOfMemory,
    Canceled,
    RpcFailed,
    /// No such workflow or run.
    NotFound,
    InvalidResponse,
};

pub const HistoryIteratorOptions = struct {
    /// Keep a run that is still open going: `next` waits (long-polling the server) for the
    /// events it records next, and returns null only once the run has closed. Without it,
    /// `next` returns null after the events recorded so far.
    wait_new_event: bool = false,
    filter: enum {
        all,
        /// Only the event that closed the run (completed, failed, continued as new, ...);
        /// with `wait_new_event`, `next` waits for the run to close.
        close_event,
    } = .all,
    /// The most events per page; 0 uses the server's default.
    page_size: u31 = 0,
};

/// The events of a workflow run's history, from `Client.historyIterator`. Fetches a page
/// from the server whenever `next` runs past the last one. Owns its memory with the client's
/// allocator: free it with `deinit`. Not thread-safe.
pub const HistoryIterator = struct {
    client: *Client,
    workflow_id: []u8,
    run_id: []u8,
    options: HistoryIteratorOptions,
    /// Holds the current page.
    page: std.heap.ArenaAllocator,
    events: []const HistoryEvent = &.{},
    index: usize = 0,
    /// Where the next page starts; empty after the last page. Owned with the client's
    /// allocator.
    next_page_token: []u8 = &.{},
    fetched: bool = false,

    pub fn deinit(it: *HistoryIterator) void {
        it.page.deinit();
        const gpa = it.client.gpa;
        gpa.free(it.workflow_id);
        gpa.free(it.run_id);
        gpa.free(it.next_page_token);
        it.* = undefined;
    }

    /// The next event, copied into `arena`, or null after the last one. With
    /// `wait_new_event`, blocks until the run records another event or closes. A failed
    /// fetch can be retried by calling `next` again.
    pub fn next(it: *HistoryIterator, arena: Allocator) HistoryError!?HistoryEvent {
        // A long poll that times out returns an empty page that still has a token.
        while (it.index == it.events.len) {
            if (it.fetched and it.next_page_token.len == 0) return null;
            try it.fetch();
        }
        const event = try it.events[it.index].dupe(arena);
        it.index += 1;
        return event;
    }

    fn fetch(it: *HistoryIterator) HistoryError!void {
        const c = it.client;
        it.events = &.{};
        it.index = 0;
        _ = it.page.reset(.retain_capacity);
        const a = it.page.allocator();
        const response = c.rpc(wfs.GetWorkflowExecutionHistoryResponse, "GetWorkflowExecutionHistory", wfs.GetWorkflowExecutionHistoryRequest{
            .namespace = c.namespace,
            .execution = .{ .workflow_id = it.workflow_id, .run_id = it.run_id },
            .maximum_page_size = it.options.page_size,
            .next_page_token = it.next_page_token,
            .wait_new_event = it.options.wait_new_event,
            .history_event_filter_type = switch (it.options.filter) {
                .all => .HISTORY_EVENT_FILTER_TYPE_ALL_EVENT,
                .close_event => .HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT,
            },
            .skip_archival = true,
        }, a) catch |err| switch (err) {
            error.AlreadyExists => return error.InvalidResponse,
            else => |e| return e,
        };
        const token = try c.gpa.dupe(u8, response.next_page_token);
        c.gpa.free(it.next_page_token);
        it.next_page_token = token;
        if (response.history) |h| it.events = h.events.items;
        it.fetched = true;
    }
};

pub const QueryOptions = struct {
    /// When the server rejects the query instead of sending it to the workflow.
    reject_condition: QueryRejectCondition = .none,
    /// On `error.QueryFailed` (or `error.RpcFailed`), receives the server's message, allocated
    /// from the query's arena: the handler's error name, or for an unknown query type a
    /// message listing the workflow's registered query types.
    failure_message: ?*[]const u8 = null,
    /// On `error.QueryRejected`, receives the workflow's status.
    rejected_status: ?*WorkflowExecutionStatus = null,
    /// Values the client's propagators write into the query's header. Only read during the
    /// call.
    context: ?*const propagation.Values = null,
};

pub const QueryRejectCondition = enum {
    /// Never reject: closed workflows are queried too.
    none,
    /// Reject the query if the workflow is closed.
    not_open,
    /// Reject the query if the workflow closed other than by completing.
    not_completed_cleanly,
};

pub const QueryError = error{
    /// A payload codec (`Options.codecs`) failed; logged.
    CodecFailed,
    OutOfMemory,
    Canceled,
    RpcFailed,
    NotFound,
    InvalidResponse,
    InvalidPayload,
    UnknownEncoding,
    WrongArgumentCount,
    /// The handler failed, or the workflow has no handler for the query type.
    QueryFailed,
    /// The workflow's state matched `QueryOptions.reject_condition`.
    QueryRejected,
};

/// Where a workflow run is in its lifecycle. Values match the server's; the enum is open so
/// a status added by a newer server is kept rather than rejected.
pub const WorkflowExecutionStatus = enum(i32) {
    unspecified = 0,
    running = 1,
    completed = 2,
    failed = 3,
    canceled = 4,
    terminated = 5,
    continued_as_new = 6,
    timed_out = 7,
    paused = 8,
    _,
};

/// A workflow run as `Client.describeWorkflow` reports it. Every slice borrows the arena
/// passed to `describeWorkflow`.
pub const WorkflowDescription = struct {
    workflow_id: []const u8,
    run_id: []const u8,
    workflow_type: []const u8,
    task_queue: []const u8,
    status: WorkflowExecutionStatus,
    start_time: ?Io.Timestamp,
    /// When the first workflow task was, or will be, scheduled: later than `start_time` for a
    /// delayed start or a cron or retry backoff.
    execution_time: ?Io.Timestamp,
    /// Null while the run is open.
    close_time: ?Io.Timestamp,
    history_length: i64,
    /// Decode a value with `memoValue` or `converter.fieldValue`.
    memo: []const converter.Field,
    /// Read a value with `search_attributes.get(arena, key)`.
    search_attributes: search_attributes.View,
    /// The full server response, for anything not lifted into the fields above (pending
    /// activities, children, versioning info, ...).
    raw: wfs.DescribeWorkflowExecutionResponse,

    /// Decodes the memo value called `name` as `T`, or returns null if the memo has no such
    /// value. Memory comes from `arena`.
    pub fn memoValue(d: *const WorkflowDescription, comptime T: type, arena: Allocator, name: []const u8) converter.DecodeError!?T {
        return converter.fieldValue(T, arena, d.memo, name);
    }
};

pub const ResultError = error{
    /// A payload codec (`Options.codecs`) failed; logged.
    CodecFailed,
    OutOfMemory,
    Canceled,
    RpcFailed,
    NotFound,
    InvalidResponse,
    InvalidPayload,
    UnknownEncoding,
    WrongArgumentCount,
    WorkflowFailed,
    WorkflowCanceled,
    WorkflowTerminated,
    WorkflowTimedOut,
    WorkflowContinuedAsNew,
};

/// A started workflow run.
pub fn WorkflowRun(comptime T: type) type {
    return struct {
        const Self = @This();

        client: *Client,
        id: []u8,
        /// The run this handle follows. `get` moves it along continue-as-new chains.
        run_id: []u8,
        /// The start handed the first workflow task straight to a worker on this client
        /// (`StartWorkflowOptions.enable_eager_start`).
        eagerly_started: bool = false,
        failure_: ?failure_mod.Failure = null,

        pub fn deinit(run: *Self) void {
            run.client.gpa.free(run.id);
            run.client.gpa.free(run.run_id);
            run.* = undefined;
        }

        /// Waits for the workflow to close and returns its result, following continue-as-new
        /// (and cron and retry runs). The result and any failure are allocated from `arena`.
        pub fn get(run: *Self, arena: Allocator) ResultError!T {
            return run.getWithOptions(arena, .{});
        }

        pub const GetOptions = struct {
            /// Return the result of this run instead of following it to the runs it starts
            /// (continue-as-new, cron, retries). A run that
            /// continued as new returns `error.WorkflowContinuedAsNew`.
            follow_runs: bool = true,
        };

        /// `get` with options.
        pub fn getWithOptions(run: *Self, arena: Allocator, options: GetOptions) ResultError!T {
            const c = run.client;
            var token: []const u8 = "";
            while (true) {
                const response = c.rpc(wfs.GetWorkflowExecutionHistoryResponse, "GetWorkflowExecutionHistory", wfs.GetWorkflowExecutionHistoryRequest{
                    .namespace = c.namespace,
                    .execution = .{ .workflow_id = run.id, .run_id = run.run_id },
                    .wait_new_event = true,
                    .history_event_filter_type = .HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT,
                    .skip_archival = true,
                    .next_page_token = token,
                }, arena) catch |err| switch (err) {
                    error.AlreadyExists => return error.InvalidResponse,
                    else => |e| return e,
                };
                const history = response.history orelse {
                    token = response.next_page_token;
                    continue;
                };
                if (history.events.items.len == 0) {
                    token = response.next_page_token;
                    continue;
                }
                const event = history.events.items[history.events.items.len - 1];
                const attrs = event.attributes orelse return error.InvalidResponse;
                // A cron or retry run that closes names the run it started; follow it.
                const next_run: []const u8 = switch (attrs) {
                    .workflow_execution_completed_event_attributes => |e| e.new_execution_run_id,
                    .workflow_execution_failed_event_attributes => |e| e.new_execution_run_id,
                    .workflow_execution_timed_out_event_attributes => |e| e.new_execution_run_id,
                    .workflow_execution_continued_as_new_event_attributes => |e| e.new_execution_run_id,
                    else => "",
                };
                if (attrs == .workflow_execution_continued_as_new_event_attributes and !options.follow_runs)
                    return error.WorkflowContinuedAsNew;
                if (options.follow_runs and next_run.len > 0) {
                    const new_run = try c.gpa.dupe(u8, next_run);
                    c.gpa.free(run.run_id);
                    run.run_id = new_run;
                    token = "";
                    continue;
                }
                switch (attrs) {
                    .workflow_execution_completed_event_attributes => |done| {
                        if (T == void) return;
                        const payloads = done.result orelse return error.InvalidPayload;
                        if (payloads.payloads.items.len == 0) return error.InvalidPayload;
                        return converter.fromPayload(T, arena, payloads.payloads.items[0]);
                    },
                    .workflow_execution_failed_event_attributes => |failed| {
                        run.failure_ = failed.failure;
                        return error.WorkflowFailed;
                    },
                    .workflow_execution_canceled_event_attributes => return error.WorkflowCanceled,
                    .workflow_execution_terminated_event_attributes => return error.WorkflowTerminated,
                    .workflow_execution_timed_out_event_attributes => return error.WorkflowTimedOut,
                    else => return error.InvalidResponse,
                }
            }
        }

        /// The workflow's failure after `get` returned `error.WorkflowFailed`.
        pub fn failure(run: *const Self) ?*const failure_mod.Failure {
            return if (run.failure_) |*f| f else null;
        }
    };
}

/// How far an update must get before `Client.updateWorkflow` returns. (The server can't wait
/// for an update only to be admitted.)
pub const UpdateStage = enum {
    /// The workflow's validator accepted the update and its handler started.
    accepted,
    /// The handler finished, or the update was rejected.
    completed,

    fn toProto(s: UpdateStage) enums.UpdateWorkflowExecutionLifecycleStage {
        return switch (s) {
            .accepted => .UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED,
            .completed => .UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED,
        };
    }
};

pub const UpdateOptions = struct {
    /// Identifies the update in the workflow; an update ID sent again returns the first
    /// update's outcome instead of running it twice. Empty generates a UUID.
    update_id: []const u8 = "",
    wait_for_stage: UpdateStage = .completed,
    /// Values the client's propagators write into the update's header. Only read during the
    /// call.
    context: ?*const propagation.Values = null,
};

pub const UpdateError = error{ OutOfMemory, Canceled, RpcFailed, NotFound, InvalidResponse, CodecFailed };

pub const UpdateWithStartError = error{
    /// A payload codec (`Options.codecs`) failed; logged.
    CodecFailed,
    OutOfMemory,
    Canceled,
    RpcFailed,
    NotFound,
    InvalidResponse,
    /// `StartWorkflowOptions.id_conflict_policy` is unspecified or `task_queue` is empty
    /// (logged). Returned before any call.
    InvalidOptions,
    /// The workflow is running and `id_conflict_policy` is `.fail`.
    WorkflowExecutionAlreadyStarted,
    /// `StartWorkflowOptions.search_attributes` holds a value the server can't index (logged).
    InvalidSearchAttribute,
};

pub const UpdateResultError = error{
    /// A payload codec (`Options.codecs`) failed; logged.
    CodecFailed,
    OutOfMemory,
    Canceled,
    RpcFailed,
    NotFound,
    InvalidResponse,
    InvalidPayload,
    UnknownEncoding,
    WrongArgumentCount,
    /// The update was rejected or its handler failed; read why with `failure()`.
    UpdateFailed,
};

/// An update sent to a workflow. Owns its IDs with the client's allocator; free with
/// `deinit`. Not thread-safe.
pub fn WorkflowUpdateHandle(comptime T: type) type {
    return struct {
        const Self = @This();

        client: *Client,
        workflow_id: []u8,
        /// Empty when the handle was made for the latest run.
        run_id: []u8,
        update_id: []u8,
        /// The encoded outcome when the update call already returned one.
        outcome: ?[]u8,
        failure_: ?failure_mod.Failure = null,

        fn init(c: *Client, workflow_id: []const u8, run_id: []const u8, update_id: []const u8, outcome: ?proto.temporal.api.update.v1.Outcome) error{OutOfMemory}!Self {
            const wid = try c.gpa.dupe(u8, workflow_id);
            errdefer c.gpa.free(wid);
            const rid = try c.gpa.dupe(u8, run_id);
            errdefer c.gpa.free(rid);
            const uid = try c.gpa.dupe(u8, update_id);
            errdefer c.gpa.free(uid);
            var encoded: ?[]u8 = null;
            if (outcome) |o| {
                var w: Io.Writer.Allocating = .init(c.gpa);
                errdefer w.deinit();
                // The encoder allocates its scratch from the same allocator; an arena keeps
                // that out of the handle.
                var scratch: std.heap.ArenaAllocator = .init(c.gpa);
                defer scratch.deinit();
                o.encode(&w.writer, scratch.allocator()) catch return error.OutOfMemory;
                encoded = try w.toOwnedSlice();
            }
            return .{ .client = c, .workflow_id = wid, .run_id = rid, .update_id = uid, .outcome = encoded };
        }

        pub fn deinit(h: *Self) void {
            const gpa = h.client.gpa;
            gpa.free(h.workflow_id);
            gpa.free(h.run_id);
            gpa.free(h.update_id);
            if (h.outcome) |o| gpa.free(o);
            h.* = undefined;
        }

        /// Waits for the update to complete and returns its result. The result and any failure
        /// are allocated from `arena`.
        pub fn get(h: *Self, arena: Allocator) UpdateResultError!T {
            const c = h.client;
            const outcome: proto.temporal.api.update.v1.Outcome = if (h.outcome) |bytes| blk: {
                var reader: Io.Reader = .fixed(try arena.dupe(u8, bytes));
                break :blk proto.temporal.api.update.v1.Outcome.decode(&reader, arena) catch |err| switch (err) {
                    error.OutOfMemory => return error.OutOfMemory,
                    else => return error.InvalidResponse,
                };
            } else while (true) {
                // Each call is a long poll the server may end before the update completes.
                const response = c.rpc(wfs.PollWorkflowExecutionUpdateResponse, "PollWorkflowExecutionUpdate", wfs.PollWorkflowExecutionUpdateRequest{
                    .namespace = c.namespace,
                    .update_ref = .{ .workflow_execution = .{ .workflow_id = h.workflow_id, .run_id = h.run_id }, .update_id = h.update_id },
                    .identity = c.identity,
                    .wait_policy = .{ .lifecycle_stage = .UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED },
                }, arena) catch |err| switch (err) {
                    error.AlreadyExists => return error.InvalidResponse,
                    else => |e| return e,
                };
                if (response.outcome) |o| break o;
            };
            switch (outcome.value orelse return error.InvalidResponse) {
                .success => |payloads| {
                    if (T == void) return;
                    if (payloads.payloads.items.len == 0) return error.InvalidPayload;
                    return converter.fromPayload(T, arena, payloads.payloads.items[0]);
                },
                .failure => |f| {
                    h.failure_ = f;
                    return error.UpdateFailed;
                },
            }
        }

        /// Why the update failed, after `get` returned `error.UpdateFailed`. Borrows the arena
        /// passed to `get`.
        pub fn failure(h: *const Self) ?*const failure_mod.Failure {
            return if (h.failure_) |*f| f else null;
        }
    };
}

/// Where `loadOptions` reads a client profile from. The config file is TOML with one table
/// per profile, `[profile.<name>]`, holding `address`, `namespace`, `api_key`, `grpc_meta`,
/// and a `tls` table (`disabled`, `server_name`, and `server_ca_cert`, `client_cert` and
/// `client_key`, each as `_path` or `_data`). The environment variables TEMPORAL_ADDRESS,
/// TEMPORAL_NAMESPACE, TEMPORAL_API_KEY, TEMPORAL_TLS, TEMPORAL_TLS_SERVER_NAME,
/// TEMPORAL_TLS_SERVER_CA_CERT_PATH (or `_DATA`), TEMPORAL_TLS_CLIENT_CERT_PATH (or `_DATA`),
/// TEMPORAL_TLS_CLIENT_KEY_PATH (or `_DATA`) and TEMPORAL_GRPC_META_<NAME> override the
/// profile's values.
pub const LoadOptions = struct {
    /// The profile to read from the config file. Empty uses TEMPORAL_PROFILE, then "default";
    /// a profile named here or by TEMPORAL_PROFILE must exist, "default" need not.
    profile: []const u8 = "",
    /// The config file. Empty uses `config_data`, then TEMPORAL_CONFIG_FILE, then
    /// `temporal/temporal.toml` in the user's config directory. A file that doesn't exist
    /// reads as empty, as sdk-core does, so only the environment applies.
    config_file: []const u8 = "",
    /// The config file's contents (TOML), instead of reading a file.
    config_data: []const u8 = "",
    /// Don't read a config file: only the environment.
    disable_file: bool = false,
    /// Don't read the environment: only the config file. Can't be set with `disable_file`.
    disable_env: bool = false,
    /// Reject keys the config file format doesn't define, instead of ignoring them.
    strict: bool = false,
    /// The environment variables to read instead of the process's.
    env: ?*const std.process.Environ.Map = null,
};

pub const LoadError = error{ OutOfMemory, InvalidConfig };

/// Client options from the Temporal config file and environment variables, as `options`
/// says (see `LoadOptions`). A profile with an API key, or with TLS settings, uses TLS unless
/// its `tls.disabled` (or TEMPORAL_TLS=false) says not to. Certificate paths are kept as
/// paths (`Options.Tls.client_cert_path`, ...) and read when the client connects. A config
/// that can't be read or parsed, a missing named profile, a key `strict` rejects, or `path`
/// and `data` both set for one certificate return `error.InvalidConfig`, with the reason
/// logged. Strings are allocated from `arena`.
pub fn loadOptions(arena: Allocator, options: LoadOptions) LoadError!Options {
    var env_json: Io.Writer.Allocating = .init(arena);
    if (options.env) |env| {
        var json: std.json.Stringify = .{ .writer = &env_json.writer };
        json.beginObject() catch return error.OutOfMemory;
        for (env.keys(), env.values()) |k, v| {
            json.objectField(k) catch return error.OutOfMemory;
            json.write(v) catch return error.OutOfMemory;
        }
        json.endObject() catch return error.OutOfMemory;
    }
    const result = bridge.c.temporal_core_client_env_config_profile_load(&.{
        .profile = bridge.bytes.ref(options.profile),
        .path = bridge.bytes.ref(options.config_file),
        .data = bridge.bytes.ref(options.config_data),
        .disable_file = options.disable_file,
        .disable_env = options.disable_env,
        .config_file_strict = options.strict,
        .env_vars = bridge.bytes.ref(env_json.written()),
    });
    if (bridge.bytes.Owned.take(null, result.fail)) |fail| {
        defer fail.deinit();
        log.warn("loading client configuration failed: {s}", .{fail.bytes()});
        return error.InvalidConfig;
    }
    const json = bridge.bytes.Owned.take(null, result.success) orelse return .{};
    defer json.deinit();
    return profileOptions(arena, json.bytes());
}

/// `loadOptions` with the default `LoadOptions`: the process's environment and the config
/// file it names, or the default one.
pub fn loadDefaultOptions(arena: Allocator) LoadError!Options {
    return loadOptions(arena, .{});
}

/// Options from sdk-core's JSON for a profile. A certificate's `data` is a JSON array of
/// bytes.
fn profileOptions(arena: Allocator, json: []const u8) LoadError!Options {
    const Source = struct { path: ?[]const u8 = null, data: ?[]const u8 = null };
    const Profile = struct {
        address: ?[]const u8 = null,
        namespace: ?[]const u8 = null,
        api_key: ?[]const u8 = null,
        tls: ?struct {
            disabled: ?bool = null,
            server_name: ?[]const u8 = null,
            server_ca_cert: ?Source = null,
            client_cert: ?Source = null,
            client_key: ?Source = null,
        } = null,
        grpc_meta: ?std.json.ArrayHashMap([]const u8) = null,
    };
    const profile = std.json.parseFromSliceLeaky(Profile, arena, json, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    }) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => {
            log.warn("loading client configuration failed: sdk-core returned a profile that doesn't parse ({t}): {s}", .{ err, json });
            return error.InvalidConfig;
        },
    };

    var options: Options = .{};
    if (profile.address) |v| options.host_port = v;
    if (profile.namespace) |v| options.namespace = v;
    if (profile.api_key) |v| options.api_key = v;
    if (profile.tls) |t| {
        if (!(t.disabled orelse false)) {
            const none: Source = .{};
            const ca = t.server_ca_cert orelse none;
            const cert = t.client_cert orelse none;
            const key = t.client_key orelse none;
            options.tls = .{
                .server_name = t.server_name orelse "",
                .server_root_ca_cert = ca.data orelse "",
                .server_root_ca_cert_path = ca.path orelse "",
                .client_cert = cert.data orelse "",
                .client_cert_path = cert.path orelse "",
                .client_private_key = key.data orelse "",
                .client_private_key_path = key.path orelse "",
            };
        }
    } else if (options.api_key.len > 0) {
        options.tls = .{};
    }
    if (profile.grpc_meta) |meta| {
        const headers = try arena.alloc(bridge.bytes.MetadataEntry, meta.map.count());
        for (headers, meta.map.keys(), meta.map.values()) |*h, k, v| h.* = .{ .key = k, .value = v };
        options.headers = headers;
    }
    return options;
}

/// Rejects what sdk-core would silently drop from `Client.updateHeaders`: gRPC ASCII headers
/// only.
fn validateHeader(h: bridge.bytes.MetadataEntry) error{InvalidOptions}!void {
    const token_symbols = "!#$%&'*+-.^_`|~";
    const key_ok = h.key.len > 0 and !std.ascii.endsWithIgnoreCase(h.key, "-bin") and for (h.key) |ch| {
        if (!std.ascii.isAlphanumeric(ch) and std.mem.indexOfScalar(u8, token_symbols, ch) == null) break false;
    } else true;
    if (!key_ok) {
        log.warn("updating the client's headers failed: \"{s}\" is not a valid header name (letters, digits and {s}, not ending in -bin)", .{ h.key, token_symbols });
        return error.InvalidOptions;
    }
    for (h.value) |ch| if (ch != '\t' and (ch < ' ' or ch > '~')) {
        log.warn("updating the client's headers failed: the value of header {s} has a byte (0x{x:0>2}) other than visible ASCII, space or tab", .{ h.key, ch });
        return error.InvalidOptions;
    };
}

fn defaultIdentity(gpa: Allocator) error{OutOfMemory}![]u8 {
    var host_buf: [std.posix.HOST_NAME_MAX]u8 = undefined;
    const host = std.posix.gethostname(&host_buf) catch "unknown-host";
    return std.fmt.allocPrint(gpa, "{d}@{s}", .{ std.c.getpid(), host });
}

/// A random (version 4) UUID string, for example to make unique workflow IDs.
pub fn uuid(arena: Allocator, io: Io) error{OutOfMemory}![]const u8 {
    var b: [16]u8 = undefined;
    io.random(&b);
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    const hex = std.fmt.bytesToHex(b, .lower);
    return std.fmt.allocPrint(arena, "{s}-{s}-{s}-{s}-{s}", .{ hex[0..8], hex[8..12], hex[12..16], hex[16..20], hex[20..32] });
}

fn timestamp(t: ?proto.google.protobuf.Timestamp) ?Io.Timestamp {
    const v = t orelse return null;
    return .fromNanoseconds(@as(i96, v.seconds) * std.time.ns_per_s + v.nanos);
}

fn protoDuration(d: ?Duration) ?proto.google.protobuf.Duration {
    const v = d orelse return null;
    return .{
        .seconds = @intCast(@divFloor(v.nanoseconds, std.time.ns_per_s)),
        .nanos = @intCast(@mod(v.nanoseconds, std.time.ns_per_s)),
    };
}

test "uuids are version 4 and well formed" {
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const id = try uuid(arena.allocator(), std.testing.io);
    try std.testing.expectEqual(@as(usize, 36), id.len);
    try std.testing.expectEqual(@as(u8, '4'), id[14]);
}

test "workflow execution statuses match the server's values" {
    const info = @typeInfo(WorkflowExecutionStatus).@"enum";
    inline for (info.field_names, info.field_values) |name, value| {
        const upper = comptime blk: {
            var buf: [name.len]u8 = undefined;
            _ = std.ascii.upperString(&buf, name);
            break :blk buf;
        };
        const server = @field(enums.WorkflowExecutionStatus, "WORKFLOW_EXECUTION_STATUS_" ++ upper);
        try std.testing.expectEqual(value, @backingInt(server));
    }
}

test "activity execution statuses match the server's values" {
    const info = @typeInfo(ActivityExecutionStatus).@"enum";
    inline for (info.field_names, info.field_values) |name, value| {
        const upper = comptime blk: {
            var buf: [name.len]u8 = undefined;
            _ = std.ascii.upperString(&buf, name);
            break :blk buf;
        };
        const server = @field(enums.ActivityExecutionStatus, "ACTIVITY_EXECUTION_STATUS_" ++ upper);
        try std.testing.expectEqual(value, @backingInt(server));
    }
}

const testing = std.testing;

test "core log fields split into keys, with strings unquoted and other values as JSON" {
    var buf: [1024]u8 = undefined;
    var fba: std.heap.FixedBufferAllocator = .init(&buf);
    var out: [4]log_mod.Field = undefined;
    const n = coreLogFields(fba.allocator(), "{\"task_queue\":\"q\",\"attempt\":2,\"sticky\":true}", &out);
    try testing.expectEqual(3, n);
    try testing.expectEqualStrings("task_queue", out[0].key);
    try testing.expectEqualStrings("q", out[0].value);
    try testing.expectEqualStrings("2", out[1].value);
    try testing.expectEqualStrings("true", out[2].value);
    try testing.expectEqual(0, coreLogFields(fba.allocator(), "{}", &out));

    var tiny: [8]u8 = undefined;
    var small: std.heap.FixedBufferAllocator = .init(&tiny);
    try testing.expectEqual(1, coreLogFields(small.allocator(), "{\"a\":\"long enough to run out\"}", &out));
    try testing.expectEqualStrings("fields", out[0].key);
}

test "the core log filter keeps libraries at warn or quieter" {
    var buf: [256]u8 = undefined;
    try testing.expectStringStartsWith(coreLogFilter(&buf, .debug), "WARN,temporalio_sdk_core=DEBUG,");
    try testing.expectStringStartsWith(coreLogFilter(&buf, .err), "ERROR,temporalio_sdk_core=ERROR,");
}

test "codec contexts name the workflow or activity a request names" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var context: converter.CodecContext = .{};
    Client.requestIdentity(wfs.StartWorkflowExecutionRequest, &.{ .workflow_id = "wf", .workflow_type = .{ .name = "Order" } }, &context);
    try testing.expectEqualStrings("wf", context.workflow_id);
    try testing.expectEqualStrings("Order", context.workflow_type);

    context = .{};
    Client.requestIdentity(wfs.SignalWorkflowExecutionRequest, &.{ .workflow_execution = .{ .workflow_id = "signaled" } }, &context);
    try testing.expectEqualStrings("signaled", context.workflow_id);
    context = .{};
    Client.requestIdentity(wfs.QueryWorkflowRequest, &.{ .execution = .{ .workflow_id = "queried" } }, &context);
    try testing.expectEqualStrings("queried", context.workflow_id);

    context = .{};
    Client.requestIdentity(wfs.StartActivityExecutionRequest, &.{ .activity_id = "act", .activity_type = .{ .name = "Charge" } }, &context);
    try testing.expectEqualStrings("act", context.activity_id);
    try testing.expectEqualStrings("Charge", context.activity_type);
    try testing.expectEqualStrings("", context.workflow_id);

    var multi: wfs.ExecuteMultiOperationRequest = .{};
    try multi.operations.append(a, .{ .operation = .{ .start_workflow = .{ .workflow_id = "uws", .workflow_type = .{ .name = "Cart" } } } });
    context = .{};
    Client.requestIdentity(wfs.ExecuteMultiOperationRequest, &multi, &context);
    try testing.expectEqualStrings("uws", context.workflow_id);
    try testing.expectEqualStrings("Cart", context.workflow_type);
}

test "a named profile from inline TOML maps to client options" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const toml =
        \\[profile.default]
        \\address = "localhost:7233"
        \\
        \\[profile.prod]
        \\address = "prod.example.com:7233"
        \\namespace = "orders"
        \\[profile.prod.grpc_meta]
        \\Team_Name = "payments"
    ;
    const prod = try loadOptions(arena.allocator(), .{ .profile = "prod", .config_data = toml, .disable_env = true });
    try testing.expectEqualStrings("prod.example.com:7233", prod.host_port);
    try testing.expectEqualStrings("orders", prod.namespace);
    try testing.expect(prod.tls == null);
    try testing.expectEqual(1, prod.headers.len);
    try testing.expectEqualStrings("team-name", prod.headers[0].key);
    try testing.expectEqualStrings("payments", prod.headers[0].value);

    const default = try loadOptions(arena.allocator(), .{ .config_data = toml, .disable_env = true });
    try testing.expectEqualStrings("localhost:7233", default.host_port);
    try testing.expectEqualStrings("default", default.namespace);
}

test "profile TLS certificates map to paths or PEM data" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const options = try loadOptions(arena.allocator(), .{ .disable_env = true, .config_data =
        \\[profile.default]
        \\address = "secure.example.com:7233"
        \\[profile.default.tls]
        \\server_name = "secure.example.com"
        \\server_ca_cert_path = "/etc/temporal/ca.pem"
        \\client_cert_data = "-----BEGIN CERTIFICATE-----"
        \\client_key_path = "/etc/temporal/client.key"
    });
    const tls = options.tls.?;
    try testing.expectEqualStrings("secure.example.com", tls.server_name);
    try testing.expectEqualStrings("/etc/temporal/ca.pem", tls.server_root_ca_cert_path);
    try testing.expectEqualStrings("", tls.server_root_ca_cert);
    try testing.expectEqualStrings("-----BEGIN CERTIFICATE-----", tls.client_cert);
    try testing.expectEqualStrings("", tls.client_cert_path);
    try testing.expectEqualStrings("/etc/temporal/client.key", tls.client_private_key_path);
}

test "environment variables override the profile, including TLS certificate paths" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var env: std.process.Environ.Map = .init(testing.allocator);
    defer env.deinit();
    try env.put("TEMPORAL_PROFILE", "staging");
    try env.put("TEMPORAL_NAMESPACE", "from-env");
    try env.put("TEMPORAL_TLS_SERVER_CA_CERT_PATH", "/certs/ca.pem");
    try env.put("TEMPORAL_TLS_CLIENT_CERT_PATH", "/certs/client.pem");
    try env.put("TEMPORAL_TLS_CLIENT_KEY_PATH", "/certs/client.key");
    try env.put("TEMPORAL_TLS_SERVER_NAME", "localhost");
    try env.put("TEMPORAL_GRPC_META_REGION", "eu");
    const options = try loadOptions(arena.allocator(), .{ .env = &env, .config_data =
        \\[profile.staging]
        \\address = "staging:7233"
        \\namespace = "from-file"
    });
    try testing.expectEqualStrings("staging:7233", options.host_port);
    try testing.expectEqualStrings("from-env", options.namespace);
    const tls = options.tls.?;
    try testing.expectEqualStrings("/certs/ca.pem", tls.server_root_ca_cert_path);
    try testing.expectEqualStrings("/certs/client.pem", tls.client_cert_path);
    try testing.expectEqualStrings("/certs/client.key", tls.client_private_key_path);
    try testing.expectEqualStrings("localhost", tls.server_name);
    try testing.expectEqualStrings("region", options.headers[0].key);

    // Only the environment: an empty map sets nothing.
    var empty: std.process.Environ.Map = .init(testing.allocator);
    defer empty.deinit();
    const defaults = try loadOptions(arena.allocator(), .{ .env = &empty, .disable_file = true });
    try testing.expectEqualStrings("localhost:7233", defaults.host_port);
    try testing.expect(defaults.tls == null);
}

test "an API key implies TLS unless the profile disables it" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const keyed = try loadOptions(arena.allocator(), .{ .disable_env = true, .config_data =
        \\[profile.default]
        \\api_key = "secret"
    });
    try testing.expectEqualStrings("secret", keyed.api_key);
    try testing.expect(keyed.tls != null);

    const plain = try loadOptions(arena.allocator(), .{ .disable_env = true, .config_data =
        \\[profile.default]
        \\api_key = "secret"
        \\tls = { disabled = true }
    });
    try testing.expect(plain.tls == null);

    var env: std.process.Environ.Map = .init(testing.allocator);
    defer env.deinit();
    try env.put("TEMPORAL_API_KEY", "from-env");
    try env.put("TEMPORAL_TLS", "false");
    const env_plain = try loadOptions(arena.allocator(), .{ .env = &env, .disable_file = true });
    try testing.expectEqualStrings("from-env", env_plain.api_key);
    try testing.expect(env_plain.tls == null);
}

test "invalid configs fail: strict keys, missing profiles, conflicting sources, bad TOML, no source" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const unknown_key =
        \\[profile.default]
        \\address = "localhost:7233"
        \\adress = "typo:7233"
    ;
    _ = try loadOptions(a, .{ .config_data = unknown_key, .disable_env = true });
    try testing.expectError(error.InvalidConfig, loadOptions(a, .{ .config_data = unknown_key, .disable_env = true, .strict = true }));
    try testing.expectError(error.InvalidConfig, loadOptions(a, .{ .profile = "missing", .config_data = unknown_key, .disable_env = true }));
    try testing.expectError(error.InvalidConfig, loadOptions(a, .{ .disable_env = true, .config_data =
        \\[profile.default.tls]
        \\client_cert_path = "/a.pem"
        \\client_cert_data = "PEM"
    }));
    try testing.expectError(error.InvalidConfig, loadOptions(a, .{ .config_data = "[profile.default", .disable_env = true }));
    try testing.expectError(error.InvalidConfig, loadOptions(a, .{ .disable_env = true, .disable_file = true }));
    // A missing file is no error; a named profile in it is.
    try testing.expectEqualStrings("localhost:7233", (try loadOptions(a, .{ .config_file = "/does/not/exist.toml", .disable_env = true })).host_port);
    try testing.expectError(error.InvalidConfig, loadOptions(a, .{ .profile = "prod", .config_file = "/does/not/exist.toml", .disable_env = true }));
}

test "TLS PEM is read from the paths when connecting" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(testing.io, .{ .sub_path = "ca.pem", .data = "PEM BYTES" });
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const path = try std.fs.path.join(arena.allocator(), &.{ ".zig-cache", "tmp", &tmp.sub_path, "ca.pem" });
    try testing.expectEqualStrings("PEM BYTES", try tlsFile(arena.allocator(), testing.io, "", path, "server_root_ca_cert"));
    try testing.expectEqualStrings("inline", try tlsFile(arena.allocator(), testing.io, "inline", "", "server_root_ca_cert"));
}

test "header updates accept gRPC ASCII headers only" {
    try validateHeader(.{ .key = "X-Tenant_ID", .value = "a b\tc~" });
    try testing.expectError(error.InvalidOptions, validateHeader(.{ .key = "", .value = "v" }));
    try testing.expectError(error.InvalidOptions, validateHeader(.{ .key = "bad key", .value = "v" }));
    try testing.expectError(error.InvalidOptions, validateHeader(.{ .key = "blob-bin", .value = "v" }));
    try testing.expectError(error.InvalidOptions, validateHeader(.{ .key = "k", .value = "caf\xc3\xa9" }));
    try testing.expectError(error.InvalidOptions, validateHeader(.{ .key = "k", .value = "a\nb" }));
}

test "default options send what the client always sent" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const loaded = try loadOptions(arena.allocator(), .{ .config_file = "/does/not/exist.toml", .disable_env = true });
    var rejection: Rejection = .{};
    for ([_]Options{ .{}, loaded }) |o| {
        const b = try bridgeConnectionOptions("http://localhost:7233", "id", o.connectionOptions(), null, &rejection);
        try testing.expectEqualStrings("temporal-zig", b.client_name);
        try testing.expectEqualStrings(@import("temporal.zig").version, b.client_version);
        try testing.expectEqual(@as(u64, 30_000), b.keep_alive.?.interval_ms);
        try testing.expectEqual(@as(u64, 15_000), b.keep_alive.?.timeout_ms);
        try testing.expect(b.retry == null);
        try testing.expect(b.http_connect_proxy == null);
        try testing.expect(b.tls == null);
        try testing.expectEqual(@as(u64, 60_000), try heartbeatIntervalMs(o.worker_heartbeat_interval, &rejection));
        try testing.expectEqual(@as(u32, 0), try rpcTimeoutMs(o.rpc_timeout, &rejection));
    }
}

test "custom connection options map to the bridge, through connectionOptions too" {
    var rejection: Rejection = .{};
    const o: Options = .{
        .keep_alive = .{ .interval = .fromSeconds(10) },
        .http_connect_proxy = .{ .host_port = "proxy.corp:3128", .username = "svc", .password = "pw" },
        .retry = .{ .max_retries = 3, .max_elapsed_time = .zero, .initial_interval = .fromMicroseconds(10) },
        .client_name = "wrapper",
        .client_version = "1.2.3 beta",
    };
    const b = try bridgeConnectionOptions("http://h:1", "id", o.connectionOptions(), null, &rejection);
    try testing.expectEqual(@as(u64, 10_000), b.keep_alive.?.interval_ms);
    try testing.expectEqual(@as(u64, 15_000), b.keep_alive.?.timeout_ms);
    try testing.expectEqualStrings("proxy.corp:3128", b.http_connect_proxy.?.target_host);
    try testing.expectEqualStrings("svc", b.http_connect_proxy.?.username);
    try testing.expectEqualStrings("pw", b.http_connect_proxy.?.password);
    const retry = b.retry.?;
    try testing.expectEqual(@as(usize, 3), retry.max_retries);
    try testing.expectEqual(@as(u64, 0), retry.max_elapsed_time_ms);
    try testing.expectEqual(@as(u64, 1), retry.initial_interval_ms);
    try testing.expectEqual(@as(u64, 5_000), retry.max_interval_ms);
    try testing.expectEqual(@as(f64, 1.7), retry.multiplier);
    try testing.expectEqual(@as(f64, 0.2), retry.randomization_factor);
    try testing.expectEqualStrings("wrapper", b.client_name);
    try testing.expectEqualStrings("1.2.3 beta", b.client_version);

    // The client's Retry defaults are the bridge's, which are sdk-core's.
    const zig_default = (try bridgeConnectionOptions("http://h:1", "id", .{ .retry = .{} }, null, &rejection)).retry.?;
    try testing.expectEqual(bridge.Connection.Options.Retry{}, zig_default);

    const off = try bridgeConnectionOptions("http://h:1", "id", .{ .keep_alive = null }, null, &rejection);
    try testing.expect(off.keep_alive == null);

    try testing.expectEqual(@as(u64, 0), try heartbeatIntervalMs(.zero, &rejection));
    try testing.expectEqual(@as(u64, 15_000), try heartbeatIntervalMs(.fromSeconds(15), &rejection));
    try testing.expectEqual(@as(u64, 1_000), try heartbeatIntervalMs(.fromSeconds(1), &rejection));
    try testing.expectEqual(@as(u64, 60_000), try heartbeatIntervalMs(.fromSeconds(60), &rejection));
    try testing.expectEqual(@as(u32, 5_000), try rpcTimeoutMs(.fromSeconds(5), &rejection));
    try testing.expectEqual(@as(u32, 1), try rpcTimeoutMs(.fromNanoseconds(1), &rejection));
    try testing.expectEqual(@as(u32, std.math.maxInt(u32)), try rpcTimeoutMs(.max, &rejection));
}

fn expectRejected(result: anytype, rejection: *const Rejection, comptime option: []const u8) !void {
    try testing.expectError(error.InvalidOptions, result);
    if (std.mem.indexOf(u8, rejection.message, option) == null) {
        std.debug.print("rejection \"{s}\" doesn't name {s}\n", .{ rejection.message, option });
        return error.TestUnexpectedResult;
    }
}

test "invalid connection options are rejected, naming the option" {
    var r: Rejection = .{};
    const url = "http://h:1";
    try expectRejected(bridgeConnectionOptions(url, "id", .{ .keep_alive = .{ .interval = .zero } }, null, &r), &r, "keep_alive.interval");
    try expectRejected(bridgeConnectionOptions(url, "id", .{ .keep_alive = .{ .timeout = .fromSeconds(-1) } }, null, &r), &r, "keep_alive.timeout");
    try expectRejected(bridgeConnectionOptions(url, "id", .{ .http_connect_proxy = .{ .host_port = "" } }, null, &r), &r, "http_connect_proxy.host_port");
    try expectRejected(bridgeConnectionOptions(url, "id", .{ .http_connect_proxy = .{ .host_port = "p:1", .username = "svc" } }, null, &r), &r, "http_connect_proxy.username and .password");
    try expectRejected(bridgeConnectionOptions(url, "id", .{ .http_connect_proxy = .{ .host_port = "p:1", .password = "pw" } }, null, &r), &r, "http_connect_proxy.username and .password");
    try expectRejected(bridgeConnectionOptions(url, "id", .{ .retry = .{ .initial_interval = .fromMilliseconds(-5) } }, null, &r), &r, "retry.initial_interval");
    try expectRejected(bridgeConnectionOptions(url, "id", .{ .retry = .{ .max_elapsed_time = .fromSeconds(-1) } }, null, &r), &r, "retry.max_elapsed_time");
    try expectRejected(bridgeConnectionOptions(url, "id", .{ .retry = .{ .multiplier = -1 } }, null, &r), &r, "retry.multiplier");
    try expectRejected(bridgeConnectionOptions(url, "id", .{ .retry = .{ .randomization_factor = std.math.nan(f64) } }, null, &r), &r, "retry.randomization_factor");
    try expectRejected(bridgeConnectionOptions(url, "id", .{ .retry = .{ .multiplier = std.math.inf(f64) } }, null, &r), &r, "retry.multiplier");
    try expectRejected(bridgeConnectionOptions(url, "id", .{ .client_name = "" }, null, &r), &r, "client_name");
    try expectRejected(bridgeConnectionOptions(url, "id", .{ .client_version = "caf\xc3\xa9" }, null, &r), &r, "client_version");
    try expectRejected(bridgeConnectionOptions(url, "id", .{ .client_name = "a\nb" }, null, &r), &r, "client_name");
    try expectRejected(heartbeatIntervalMs(.fromMilliseconds(500), &r), &r, "1s to 1m");
    try expectRejected(heartbeatIntervalMs(.fromSeconds(61), &r), &r, "worker_heartbeat_interval");
    try expectRejected(heartbeatIntervalMs(.fromSeconds(-1), &r), &r, "worker_heartbeat_interval");
    try expectRejected(rpcTimeoutMs(.zero, &r), &r, "rpc_timeout");
    try expectRejected(rpcTimeoutMs(.fromSeconds(-3), &r), &r, "rpc_timeout");
}

test "calls that wait on the server get no rpc timeout" {
    const t: u32 = 750;
    try testing.expectEqual(@as(u32, 0), Client.callTimeoutMs(wfs.UpdateWorkflowExecutionRequest, "UpdateWorkflowExecution", &.{}, t));
    try testing.expectEqual(@as(u32, 0), Client.callTimeoutMs(wfs.ExecuteMultiOperationRequest, "ExecuteMultiOperation", &.{}, t));
    try testing.expectEqual(@as(u32, 0), Client.callTimeoutMs(wfs.PollWorkflowExecutionUpdateRequest, "PollWorkflowExecutionUpdate", &.{}, t));
    try testing.expectEqual(@as(u32, 0), Client.callTimeoutMs(wfs.PollActivityExecutionRequest, "PollActivityExecution", &.{}, t));
    try testing.expectEqual(@as(u32, 0), Client.callTimeoutMs(wfs.GetWorkflowExecutionHistoryRequest, "GetWorkflowExecutionHistory", &.{ .wait_new_event = true }, t));
    try testing.expectEqual(t, Client.callTimeoutMs(wfs.GetWorkflowExecutionHistoryRequest, "GetWorkflowExecutionHistory", &.{}, t));
    try testing.expectEqual(t, Client.callTimeoutMs(wfs.QueryWorkflowRequest, "QueryWorkflow", &.{}, t));
    try testing.expectEqual(t, Client.callTimeoutMs(wfs.StartWorkflowExecutionRequest, "StartWorkflowExecution", &.{}, t));
}

test "health check responses other than SERVING fail" {
    const health = proto.grpc.health.v1;
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const Status = health.HealthCheckResponse.ServingStatus;
    for ([_]Status{ .SERVING, .NOT_SERVING, .UNKNOWN, .SERVICE_UNKNOWN, @fromBackingInt(@intCast(7)) }) |status| {
        var w: Io.Writer.Allocating = .init(a);
        try (health.HealthCheckResponse{ .status = status }).encode(&w.writer, a);
        const result = Client.healthResponse(a, w.written());
        if (status == .SERVING) try result else try testing.expectError(error.NotServing, result);
    }
    try testing.expectError(error.InvalidResponse, Client.healthResponse(a, "\x08\xff"));
}

test "rpc_timeout cuts short a query the server holds" {
    var server = try @import("testing.zig").DevServer.start(testing.allocator, testing.io, .{});
    defer server.stop(testing.io);
    var options = server.clientOptions();
    options.rpc_timeout = .fromMilliseconds(500);
    var c = try Client.connect(testing.allocator, testing.io, options);
    defer c.close();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // No worker polls this task queue, so the server holds a query until its deadline.
    const id = try uuid(a, testing.io);
    _ = try c.rpc(wfs.StartWorkflowExecutionResponse, "StartWorkflowExecution", wfs.StartWorkflowExecutionRequest{
        .namespace = c.namespace,
        .workflow_id = id,
        .workflow_type = .{ .name = "Unpolled" },
        .task_queue = .{ .name = try uuid(a, testing.io), .kind = .TASK_QUEUE_KIND_NORMAL },
        .request_id = try uuid(a, testing.io),
        .identity = c.identity,
    }, a);
    const start = Io.Timestamp.now(testing.io, .awake);
    var status: ?Client.RpcStatus = null;
    try testing.expectError(error.RpcFailed, c.rpcWithStatus(wfs.QueryWorkflowResponse, "QueryWorkflow", wfs.QueryWorkflowRequest{
        .namespace = c.namespace,
        .execution = .{ .workflow_id = id },
        .query = .{ .query_type = "status" },
    }, a, &status));
    const waited = start.durationTo(Io.Timestamp.now(testing.io, .awake));
    // sdk-core's gRPC transport reports its own deadline as CANCELLED; the server's would be
    // DEADLINE_EXCEEDED. Either way, well before sdk-core's default deadline of 30 s.
    const s = status.?;
    if (!(s.code == 1 and std.mem.indexOf(u8, s.message, "Timeout expired") != null) and s.code != 4) {
        std.debug.print("want a timeout, got gRPC status {d}: {s}\n", .{ s.code, s.message });
        return error.TestUnexpectedResult;
    }
    try testing.expect(waited.nanoseconds < 10 * std.time.ns_per_s);
}
