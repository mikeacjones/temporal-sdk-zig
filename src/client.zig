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
const builtin = @import("builtin");
const bridge = @import("bridge.zig");
const proto = @import("proto.zig");
const proto_time = @import("proto_time.zig");
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
    /// gRPC headers sent with every call. A view from `Client.withRpcOptions` sends its own
    /// `RpcOptions.metadata` in place of these for the same name.
    headers: []const bridge.bytes.MetadataEntry = &.{},
    /// Payload codecs, such as `converter.ZlibCodec` or encryption, applied in order to every
    /// payload this client sends and in reverse to every payload it receives (results,
    /// failures, memos, history events), and by every worker on this client to its workflow
    /// and activity payloads. Search attributes are never encoded. A codec that sets
    /// `encodeWithContext` and `decodeWithContext` sees each call's `converter.Context`:
    /// its propagated values and the workflow or activity it is for. Borrowed: the slice and
    /// the codecs must outlive the client and its workers. Called concurrently: workers run
    /// them from the threads that process workflow activations and run activities.
    codecs: []const converter.PayloadCodec = &.{},
    /// Move each outgoing failure's message and stack trace, and those of every cause in its
    /// chain, into its `encoded_attributes` before the codecs run, so a codec that encrypts
    /// payloads hides them too: the failure travels with the message "Encoded failure" and
    /// an empty stack trace, and `encoded_attributes` holds a `json/plain` payload of
    /// `{"message": ..., "stack_trace": ...}` (`failure.EncodedAttributes`), the format other
    /// Temporal SDKs write. Applies to this client and every worker on it, as `codecs` does.
    /// A failure that already carries encoded attributes is sent as it is, and a failed
    /// query's answer is not encoded: it never reaches history, and the server gives its
    /// message to the caller in the clear (`QueryOptions.failure_message`).
    /// Failures received are always decoded back, after the codecs, whatever this is set to.
    encode_failure_attributes: bool = false,
    /// Transforms the failures this client and every worker on it send and receive (see
    /// `converter.FailureConverter` for where it runs): `toFailure` runs before
    /// `encode_failure_attributes` and the codecs, and `fromFailure` after they are undone.
    /// Null sends and reads failures as the SDK builds and receives them. The converter also
    /// runs inside workflow code, for the failures a workflow sends and receives, so it must
    /// be deterministic and must not block or do I/O; it is called concurrently from worker
    /// threads and the client's callers, so it must be thread-safe. A replayer of this
    /// client's histories must be given the same one
    /// (`testing.WorkflowReplayer.Options.failure_converter`). Borrowed: it must outlive the
    /// client and its workers.
    failure_converter: ?converter.FailureConverter = null,
    /// Where sdk-core's logs go, and the default for every worker on this client (see
    /// `worker.Options.logger`). Null uses `log.Sink.std_log`. Borrowed: it must outlive the
    /// client and its workers. Called concurrently from worker threads and sdk-core's threads.
    logger: ?log_mod.Sink = null,
    /// The least severe sdk-core log level forwarded to `logger`; null leaves sdk-core's logs
    /// off. Logs from libraries sdk-core uses are forwarded from `warn` up.
    core_log_level: ?log_mod.Level = .warn,
    /// Context propagators, run in order: they write the `.context` values of each call that
    /// carries a header into that header, and every worker on this client reads them back out
    /// for its workflows, activities and the Nexus requests it handles. A workflow's values
    /// travel with the Nexus operations it calls, in one Nexus header
    /// (`propagation.nexus_header_name`); see `temporal.propagation`. Borrowed: the slice and
    /// the propagators must outlive the client and its workers.
    propagators: []const propagation.ContextPropagator = &.{},
    /// Interceptors of the workflows, activities and Nexus requests of every worker on this
    /// client (the requests a worker handles and the operations a workflow calls), before the
    /// worker's own (`worker.Options.interceptors`). Borrowed: the slice and the interceptors
    /// must outlive the client and its workers.
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
    /// workers' own calls aren't affected. It also bounds `Client.connect`, and the first
    /// connection of a client from `Client.init`, which a call makes. `RpcOptions.timeout` replaces it for the calls
    /// of a view from `Client.withRpcOptions`. Null keeps sdk-core's default of 30 s. Must be
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
    /// one. Durations must not be negative: `Client.init` returns `error.InvalidOptions` for
    /// one that is. A duration longer than a u64 of milliseconds, such as `Duration.max`, is
    /// sent as the longest one it can.
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

/// Why `openConnection` connects, for its log message on failure.
const Attempt = enum {
    /// `Client.connect`, which then fails.
    connect,
    /// The first use of a client `Client.init` created, which then stays unconnected.
    first_use,
    /// `Client.reconnect` on a connected client, which keeps its connection.
    reconnect,
};

/// Starts connecting to the server `options` names, reading the TLS files it names, and
/// checking it. Logs a rejected option as `logConnectFailure` does. Release the attempt
/// with `unref`.
fn startConnecting(gpa: Allocator, io: Io, runtime: bridge.Runtime, identity: []const u8, options: ConnectionOptions, comptime attempt: Attempt) error{ OutOfMemory, InvalidOptions }!*bridge.Connection.Connecting {
    var arena_state: std.heap.ArenaAllocator = .init(gpa);
    defer arena_state.deinit();
    var rejection: Rejection = .{};
    const bridge_options = connectionBridgeOptions(arena_state.allocator(), io, identity, options, &rejection) catch |err| {
        if (err == error.InvalidOptions) logConnectFailure(attempt, options.host_port, rejection.message);
        return err;
    };
    return bridge.Connection.Connecting.start(gpa, io, runtime, bridge_options) catch |err| {
        // The headers were checked above; the core rejects what else contains a newline.
        if (err == error.InvalidOptions) logConnectFailure(attempt, options.host_port, "an option contains a newline");
        return err;
    };
}

/// Connects as `startConnecting` does and waits, without cancellation, for the connection.
/// Logs why on failure: as an error for `Client.connect`, or as a warning when the client
/// still works after it (a first connection on use, or a `reconnect`).
fn openConnection(gpa: Allocator, io: Io, runtime: bridge.Runtime, identity: []const u8, options: ConnectionOptions, comptime attempt: Attempt) ConnectError!bridge.Connection {
    const connecting = try startConnecting(gpa, io, runtime, identity, options, attempt);
    // Done once the wait returns (nothing supersedes an attempt that isn't shared), so the
    // last reference frees it here rather than orphaning it.
    defer std.debug.assert(connecting.unref() != .orphaned);
    connecting.waitUncancelable();
    return connecting.take() orelse {
        logConnectFailure(attempt, options.host_port, connecting.failureMessage());
        return error.ConnectFailed;
    };
}

/// The bridge's options for `options`, with the TLS files read into `arena`.
fn connectionBridgeOptions(arena: Allocator, io: Io, identity: []const u8, options: ConnectionOptions, rejection: *Rejection) error{ OutOfMemory, InvalidOptions }!bridge.Connection.Options {
    for (options.headers) |h| try headerRejection(h, rejection);
    var tls: ?bridge.Connection.Options.Tls = null;
    if (options.tls) |t| tls = .{
        .server_root_ca_cert = try tlsFile(arena, io, t.server_root_ca_cert, t.server_root_ca_cert_path, "server_root_ca_cert", rejection),
        .domain = t.server_name,
        .client_cert = try tlsFile(arena, io, t.client_cert, t.client_cert_path, "client_cert", rejection),
        .client_private_key = try tlsFile(arena, io, t.client_private_key, t.client_private_key_path, "client_private_key", rejection),
    };
    const scheme = if (options.tls != null) "https" else "http";
    const url = try std.fmt.allocPrint(arena, "{s}://{s}", .{ scheme, options.host_port });
    return bridgeConnectionOptions(url, identity, options, tls, rejection);
}

fn logConnectFailure(comptime attempt: Attempt, host_port: []const u8, reason: []const u8) void {
    const format = switch (attempt) {
        .connect => "connecting to {s} failed: {s}",
        .first_use => "connecting to {s} failed; the client stays unconnected and its next call tries again: {s}",
        .reconnect => "reconnecting to {s} failed, keeping the current connection: {s}",
    };
    const args = .{ host_port, reason };
    if (builtin.is_test) if (test_connect_failure_log) |w| {
        w.print(format ++ "\n", args) catch {};
        return;
    };
    switch (attempt) {
        .connect => log.err(format, args),
        .first_use, .reconnect => log.warn(format, args),
    }
}

// Test hook, read only when builtin.is_test. Tests that set it reset it.
/// When set, `logConnectFailure` writes its line here, with a newline, instead of logging it.
var test_connect_failure_log: ?*Io.Writer = null;

/// Rejects, as `openConnection` would, connection options that are wrong whatever the
/// server: everything but the TLS files' contents, which are read when the client connects.
/// Fills `rejection` when it returns `error.InvalidOptions`.
fn checkConnectionOptions(options: ConnectionOptions, rejection: *Rejection) error{InvalidOptions}!void {
    if (options.tls) |t| inline for (.{ "server_root_ca_cert", "client_cert", "client_private_key" }) |field| {
        const path = @field(t, field ++ "_path");
        if (path.len > 0 and @field(t, field).len > 0)
            return rejection.reject("set Options.tls." ++ field ++ " or Options.tls." ++ field ++ "_path ({s}), not both", .{path});
    };
    for (options.headers) |h| try headerRejection(h, rejection);
    _ = try bridgeConnectionOptions("", "", options, null, rejection);
}

/// The connection options a client that hasn't connected yet will connect with, copied
/// into their own arena.
const PendingConnection = struct {
    arena: std.heap.ArenaAllocator,
    options: ConnectionOptions,

    fn init(gpa: Allocator, options: ConnectionOptions) error{OutOfMemory}!PendingConnection {
        var arena_state: std.heap.ArenaAllocator = .init(gpa);
        errdefer arena_state.deinit();
        const a = arena_state.allocator();
        var o = options;
        o.host_port = try a.dupe(u8, options.host_port);
        o.api_key = try a.dupe(u8, options.api_key);
        if (options.tls) |t| {
            var copy = t;
            inline for (@typeInfo(Options.Tls).@"struct".field_names) |name| @field(copy, name) = try a.dupe(u8, @field(t, name));
            o.tls = copy;
        }
        const headers = try a.alloc(bridge.bytes.MetadataEntry, options.headers.len);
        for (headers, options.headers) |*to, from| to.* = .{ .key = try a.dupe(u8, from.key), .value = try a.dupe(u8, from.value) };
        o.headers = headers;
        if (options.http_connect_proxy) |p| o.http_connect_proxy = .{
            .host_port = try a.dupe(u8, p.host_port),
            .username = try a.dupe(u8, p.username),
            .password = try a.dupe(u8, p.password),
        };
        o.client_name = try a.dupe(u8, options.client_name);
        o.client_version = try a.dupe(u8, options.client_version);
        return .{ .arena = arena_state, .options = o };
    }

    fn deinit(p: *PendingConnection) void {
        p.arena.deinit();
    }
};

/// The PEM in `pem`, or else read from `path` into `arena`. Fills `rejection` when it
/// returns `error.InvalidOptions`.
fn tlsFile(arena: Allocator, io: Io, pem: []const u8, path: []const u8, comptime field: []const u8, rejection: *Rejection) error{ OutOfMemory, InvalidOptions }![]const u8 {
    if (path.len == 0) return pem;
    if (pem.len > 0) return rejection.reject("set Options.tls." ++ field ++ " or Options.tls." ++ field ++ "_path ({s}), not both", .{path});
    return Io.Dir.cwd().readFileAlloc(io, path, arena, .limited(1 << 20)) catch |err| switch (err) {
        error.OutOfMemory => error.OutOfMemory,
        else => rejection.reject("reading Options.tls." ++ field ++ "_path {s} failed: {t}", .{ path, err }),
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
/// meter, which `bridge.Runtime.init` owns from then on. The OpenTelemetry periodicity is
/// converted in i96, not through `Duration.toMilliseconds`, which panics past i64.
fn runtimeMetrics(gpa: Allocator, options: metrics_mod.Options) InitError!bridge.Runtime.Metrics {
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
            .metric_periodicity_ms = if (o.periodicity) |d| std.math.cast(u32, @max(@divTrunc(d.nanoseconds, std.time.ns_per_ms), 1)) orelse std.math.maxInt(u32) else 0,
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

/// What `Client.init` returns: an option was rejected (logged), or the core runtime couldn't
/// be created (logged).
pub const InitError = error{ OutOfMemory, InvalidOptions, RuntimeInitFailed };

/// What `Client.connect`, `Client.ensureConnected` and `Client.reconnect` return:
/// `InitError`, or `ConnectFailed` when the server couldn't be reached or refused the
/// connection, or didn't answer within its deadline (`Options.rpc_timeout` for `connect`
/// and for a first connection on use), logged with the reason.
/// `InvalidOptions` also covers a TLS file that couldn't be read when connecting.
/// `Canceled` comes only from a call that makes a first connection on use
/// (`ensureConnected`, `currentConnection`) when its task is canceled. The attempt goes on
/// for the calls still waiting for it; once none is, it is dropped and the next call starts
/// another. `connect` and `reconnect` wait without cancellation.
pub const ConnectError = InitError || error{ ConnectFailed, Canceled };

/// What a client shares with the clients `Client.forNamespace` makes from it: the sdk-core
/// runtime and the connection, with everything `reconnect`, `updateApiKey` and
/// `updateHeaders` change. Reference-counted: each client holds one reference, and the
/// client that drops the last one frees it in `close`.
const Shared = struct {
    gpa: Allocator,
    io: Io,
    /// The clients holding it.
    refs: std.atomic.Value(u32) = .init(1),
    runtime: bridge.Runtime,
    /// The runtime's meter; null without `Options.metrics`.
    meter: ?*bridge.metric.Meter,
    /// A copy of the first client's logger that the runtime's core log forwarding points
    /// at, freed after the runtime.
    core_log_sink: ?*log_mod.Sink,
    /// Null until the client connects: `connect` connects at once, a client from `init` on
    /// first use. Read it with `currentConnection`: `reconnect` replaces it.
    connection: ?bridge.Connection = null,
    /// What the client connects with while `connection` is null; freed once it connects.
    pending_connection: ?PendingConnection = null,
    /// Guards `connection`, `pending_connection`, `pending_updates`, `connecting` and the
    /// fields after it, `retired_connections` and `running_workers`. Never held while waiting
    /// on the network.
    connection_mutex: Io.Mutex = .init,
    /// Counts the changes to `pending_connection`, so a first connection started before an
    /// update can be brought up to date.
    pending_updates: u64 = 0,
    /// The first connection in progress, which every call that needs the server waits for,
    /// each until its own deadline, instead of starting its own. Holds a reference. Set while
    /// some call waits for it; the call that sees it done, or the last one to give up on
    /// it, clears it, and the next call starts another.
    connecting: ?*bridge.Connection.Connecting = null,
    /// The calls waiting for `connecting`; calls still waiting for an attempt that was
    /// cleared aren't counted.
    connecting_waiters: u32 = 0,
    /// `pending_updates` when `connecting` started, or null when it was rejoined from
    /// `abandoned_attempts`, so the latest `api_key` and `headers` are always applied to the
    /// connection it makes.
    connecting_updates: ?u64 = null,
    /// Locked exactly while `connecting` is set, by the call that sets it and unlocked by the
    /// call that clears it; nothing waits on it.
    connect_mutex: Io.Mutex = .init,
    /// Cleared attempts whose callback hadn't run when they were cleared, each with a
    /// reference, oldest abandoned first: `destroy` frees those called back since, and leaves
    /// the runtime to the rest. Together with `connecting`, at most
    /// `Client.max_connect_attempts_in_flight`: at that many, `firstConnection` rejoins the
    /// oldest instead of starting another.
    abandoned_attempts: std.ArrayList(*bridge.Connection.Connecting) = .empty,
    /// Connections `reconnect` replaced, freed by `destroy`: calls already sent on one may
    /// still be using it.
    retired_connections: std.ArrayList(bridge.Connection) = .empty,
    /// The core workers running on any client sharing the connection, which `reconnect`
    /// moves to the new one. Registered by `worker.Worker.run`.
    running_workers: std.ArrayList(*bridge.c.TemporalCoreWorker) = .empty,

    /// Frees the connections, the runtime and `s`. Called by the last client to close.
    fn destroy(s: *Shared) void {
        const gpa = s.gpa;
        s.running_workers.deinit(gpa);
        for (s.retired_connections.items) |conn| conn.deinit();
        s.retired_connections.deinit(gpa);
        if (s.connection) |conn| conn.deinit();
        if (s.pending_connection) |*p| p.deinit();
        // Set only while a call is still waiting for it, which `close` must not race.
        // `firstConnection` made room for it when it started the attempt.
        if (s.connecting) |p| s.abandoned_attempts.appendAssumeCapacity(p);
        // Frees, here, every attempt the core has called back and any connection it made that
        // nobody took. The core's connect task uses the runtime until it calls back, so
        // the runtime of an attempt still in flight is left to the core, never freed.
        var in_flight = false;
        for (s.abandoned_attempts.items) |p| {
            if (p.unref() == .orphaned) in_flight = true;
        }
        s.abandoned_attempts.deinit(gpa);
        if (s.meter) |m| m.destroy();
        if (in_flight) s.runtime.abandon() else s.runtime.deinit();
        if (s.core_log_sink) |sink| gpa.destroy(sink);
        gpa.destroy(s);
    }
};

pub const Client = struct {
    gpa: Allocator,
    io: Io,
    /// The runtime and the connection, shared with the clients `forNamespace` makes from
    /// this one and those made from them. Not for use outside the SDK.
    shared: *Shared,
    namespace: []u8,
    identity: []u8,
    /// `Options.codecs`, borrowed.
    codecs: []const converter.PayloadCodec,
    /// `Options.encode_failure_attributes`.
    encode_failure_attributes: bool = false,
    /// `Options.failure_converter`, borrowed; workers on this client use it too.
    failure_converter: ?converter.FailureConverter = null,
    /// `Options.logger` or the default sink; workers on this client inherit it.
    logger: log_mod.Sink,
    /// `Options.propagators`, borrowed; workers on this client use them too.
    propagators: []const propagation.ContextPropagator,
    /// `Options.worker_interceptors`, borrowed.
    worker_interceptors: []const interceptor.Worker,
    /// `Options.interceptors`, borrowed.
    interceptors: []const interceptor.Client,
    /// `Options.rpc_timeout` in milliseconds; 0 sends none.
    rpc_timeout_ms: u32 = 0,
    /// Set on a view from `withRpcOptions`, which borrows the rest from the client it was
    /// made from: the timeout and metadata its calls send.
    rpc_options: ?RpcOptions = null,

    /// Records through this client's metrics exporter (`Options.metrics`) with the runtime's
    /// default attributes, or records nothing without one. Clients made by `forNamespace`
    /// share it. Valid until the last client sharing it closes (see `close`); metrics
    /// created from it, and handlers from its `with`, must be freed first. Thread-safe.
    pub fn metrics(c: *const Client) metrics_mod.Handler {
        const m = c.shared.meter orelse return .noop;
        return .{ .ptr = m, .vtable = &bridge.metric.Meter.vtable };
    }

    /// Connects and checks the server. Close with `close`. Fails, with the reason logged,
    /// when an option is rejected or the server can't be reached. Waits for the server until
    /// `Options.rpc_timeout` (30 s without one), without cancellation; a server that hasn't
    /// answered by then fails it with `error.ConnectFailed`. sdk-core may still be making that
    /// attempt (it has no deadline of its own), and the client's runtime is then left to it
    /// as `close` describes.
    pub fn connect(gpa: Allocator, io: Io, options: Options) ConnectError!Client {
        var c = try create(gpa, io, options, .connect);
        errdefer c.close();
        _ = try c.currentConnectionAs(.connect);
        return c;
    }

    /// Creates a client that connects on first use, so a process can build its client before
    /// the server is reachable: nothing goes over the network here, and an address nobody
    /// listens on doesn't fail it. Options are checked as `connect` checks them
    /// (`error.InvalidOptions`, logged), except the TLS files, which are read when the client
    /// connects. The first call that needs the server (a workflow, schedule or activity call,
    /// `checkHealth`, `currentConnection`, `ensureConnected`, or `worker.Worker.run` on this
    /// client) connects and checks the server. Calls made together from several threads share
    /// one attempt, and each waits for it only until its own deadline, `Options.rpc_timeout`
    /// (30 s without one) from when the call began; canceling a call's task returns
    /// `error.Canceled` from it. The attempt goes on for the calls still waiting, and once
    /// none is, a later call starts another. `updateApiKey` and `updateHeaders` don't wait
    /// for it, and what they change is sent on the new connection. A connection that fails or
    /// doesn't answer in time is logged as a warning with the reason and leaves the client
    /// unconnected, so a later call tries again: `ensureConnected` and `currentConnection`
    /// return the `ConnectError`, the client's other calls `error.RpcFailed`, and
    /// `worker.Worker.run` `error.ConnectFailed`. A client keeps at most 4 connection
    /// attempts in flight, the one in progress and those every call gave up on while
    /// sdk-core still makes them. Once 4 are, a call that would start another waits instead,
    /// until its own deadline, for the one abandoned longest ago. Close with `close`,
    /// connected or not, once no call is running.
    pub fn init(gpa: Allocator, io: Io, options: Options) InitError!Client {
        return create(gpa, io, options, .first_use);
    }

    /// The client without a connection: its runtime, and `options`' connection options kept
    /// for the first connection.
    fn create(gpa: Allocator, io: Io, options: Options, comptime attempt: Attempt) InitError!Client {
        var message: ?[]u8 = null;
        defer if (message) |m| gpa.free(m);

        const what = switch (attempt) {
            .connect => "connecting to",
            else => "creating a client for",
        };
        var rejection: Rejection = .{};
        const heartbeat_ms = heartbeatIntervalMs(options.worker_heartbeat_interval, &rejection) catch |err| {
            log.err(what ++ " {s} failed: {s}", .{ options.host_port, rejection.message });
            return err;
        };
        const rpc_timeout_ms = rpcTimeoutMs(options.rpc_timeout, &rejection) catch |err| {
            log.err(what ++ " {s} failed: {s}", .{ options.host_port, rejection.message });
            return err;
        };
        checkConnectionOptions(options.connectionOptions(), &rejection) catch |err| {
            log.err(what ++ " {s} failed: {s}", .{ options.host_port, rejection.message });
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
        const shared = try gpa.create(Shared);
        errdefer gpa.destroy(shared);
        const pending: PendingConnection = try .init(gpa, options.connectionOptions());
        shared.* = .{
            .gpa = gpa,
            .io = io,
            .runtime = runtime,
            .meter = meter,
            .core_log_sink = core_log_sink,
            .pending_connection = pending,
        };

        return .{
            .gpa = gpa,
            .io = io,
            .shared = shared,
            .namespace = namespace,
            .identity = identity,
            .codecs = options.codecs,
            .encode_failure_attributes = options.encode_failure_attributes,
            .failure_converter = options.failure_converter,
            .logger = logger,
            .propagators = options.propagators,
            .worker_interceptors = options.worker_interceptors,
            .interceptors = options.interceptors,
            .rpc_timeout_ms = rpc_timeout_ms,
        };
    }

    /// Returns a client for `namespace` on this client's connection and sdk-core runtime:
    /// nothing goes over the network, no connection is opened and no runtime is created. A
    /// program that works in several namespaces of one server connects once and makes a
    /// client for each of the others with this. Workers on the new client poll `namespace`.
    /// The new client has this one's identity, codecs, `encode_failure_attributes`,
    /// failure converter, logger, propagators, interceptors (client and worker), metrics
    /// (`metrics`) and `Options.rpc_timeout`; they are borrowed from the same `Options` as
    /// this client's, so those must outlive it too. `namespace` is copied.
    ///
    /// The clients share the connection and the runtime, and own them together: closing
    /// one leaves both to the others, and the last of them to close frees them. The clients
    /// may be closed in any order, each once, on any thread, by the rules `close` gives
    /// for one client. `reconnect`, `updateApiKey` and `updateHeaders` on any of them change
    /// the connection of all of them and move the workers running on any of them. A client
    /// from `init` that hasn't connected yet shares its first connection the same way:
    /// whichever client needs the server first connects for all. Clients made from the new
    /// one share the same connection too. Thread-safe.
    pub fn forNamespace(c: *Client, namespace: []const u8) error{OutOfMemory}!Client {
        const ns = try c.gpa.dupe(u8, namespace);
        errdefer c.gpa.free(ns);
        const identity = try c.gpa.dupe(u8, c.identity);
        const prev = c.shared.refs.fetchAdd(1, .monotonic);
        std.debug.assert(prev > 0); // `c` was closed.
        var other = c.*;
        other.shared = c.shared;
        other.namespace = ns;
        other.identity = identity;
        other.rpc_options = null;
        return other;
    }

    /// Returns a view of this client whose calls send `options`: their own deadline, retries
    /// and extra gRPC metadata, without changing `Options` or any other call. Every call made
    /// through the view uses them: workflow start, signal, query, update, describe, list and
    /// count, cancel, terminate and reset, history, schedules, worker deployments, standalone
    /// activities, asynchronous activity completion and heartbeats, `checkHealth`,
    /// `callService`, and the calls of the handles and iterators the view returns.
    ///
    /// A view is for single calls: build workers on this client, not on a view. A worker
    /// built on a view keeps the view, so `ctx.client()` in its activities and Nexus
    /// handlers returns the view and their calls send its timeout and metadata, and if this
    /// client is from `init` and hasn't connected yet, the worker's first connection waits
    /// only as long as the view's timeout.
    ///
    /// `options.timeout` replaces `Options.rpc_timeout` for each call, as that option
    /// applies: it is the deadline of each call and of each of its retries, a call that runs
    /// out fails with `error.RpcFailed` (gRPC status 1, "Timeout expired", or 4,
    /// DEADLINE_EXCEEDED, logged), and a first connection made by a call waits for it. Calls
    /// that long-poll the server until something happens aren't limited by it: waiting for a
    /// workflow's or an activity's result (`get` on the handles), for an update to reach its
    /// wait stage or its outcome, and following history with `wait_new_event`. Cancel the
    /// calling task to stop those. Null keeps `Options.rpc_timeout`.
    ///
    /// `options.retry`, when set, decides for each call whether sdk-core retries it with
    /// `Options.retry`'s policy. `true` makes a workflow reset retry too, which can reset the
    /// workflow twice when the transport fails after the server applied the first attempt.
    ///
    /// `options.metadata` and `options.binary_metadata` are sent with each call, the
    /// long-polls included, along with `Options.headers` (as `updateHeaders` last set them)
    /// and the API key: for a key in both, the view's value is sent. An `authorization`
    /// entry replaces the API key's bearer token.
    ///
    /// The view is a `Client` that borrows this client's state and `options`' slices: both
    /// must outlive it and everything it returns. It owns nothing; `close` on it does nothing,
    /// and closing this client ends it. `withRpcOptions` on a view replaces its options, and
    /// `forNamespace` on a view returns a full client without them. Returns
    /// `error.InvalidOptions`, logged, for a timeout that isn't positive, a metadata name
    /// that isn't a gRPC header name (an ASCII one ending in "-bin", or a binary one that
    /// doesn't), or an ASCII value with bytes other than visible ASCII, space and tab.
    pub fn withRpcOptions(c: *const Client, options: RpcOptions) error{InvalidOptions}!Client {
        var rejection: Rejection = .{};
        options.check(&rejection) catch |err| {
            log.err("withRpcOptions failed: {s}", .{rejection.message});
            return err;
        };
        var view = c.*;
        view.rpc_options = options;
        return view;
    }

    /// The deadline of this client's calls in milliseconds: `RpcOptions.timeout` on a view
    /// that sets one, otherwise `Options.rpc_timeout`; 0 sends none.
    fn callDeadlineMs(c: *const Client) u32 {
        if (c.rpc_options) |o| if (o.timeout) |t| return std.math.cast(u32, durationMs(t)) orelse std.math.maxInt(u32);
        return c.rpc_timeout_ms;
    }

    /// Connects now if the client hasn't yet (see `init`), and checks the server; does
    /// nothing on a connected client. On failure, logged with the reason, the client stays
    /// unconnected and a later call tries again. Waits until `Options.rpc_timeout`, and
    /// returns `error.Canceled` if the calling task is canceled. Thread-safe: calls made
    /// together share one attempt.
    pub fn ensureConnected(c: *Client) ConnectError!void {
        _ = try c.currentConnection();
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
        const connection = c.currentConnection() catch |err| return switch (err) {
            error.OutOfMemory => error.OutOfMemory,
            error.Canceled => error.Canceled,
            else => error.RpcFailed,
        };
        const bytes = connection.call(c.gpa, c.io, .health, "Check", encoded.written(), c.callOptions(false, c.callDeadlineMs()), &failure) catch |err| switch (err) {
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

    /// Calls `method` of `service` (the method's name as the service's .proto file spells
    /// it, such as "DescribeNamespace") with the generated request type of that method, and
    /// returns its generated response type, allocated from `arena`. For example
    /// `client.callService(.workflow, "DescribeNamespace", arena, .{ .namespace = "billing" }, &failure)`.
    /// The methods are those of the generated service declarations under `temporal.proto`:
    /// `temporal.api.workflowservice.v1.WorkflowService`, `operatorservice.v1.OperatorService`,
    /// `cloud.cloudservice.v1.CloudService`, `testservice.v1.TestService` and
    /// `grpc.health.v1.Health`. A method the service doesn't have is a compile error naming
    /// both, and so is a streaming method (`Health.Watch`): calls are unary.
    ///
    /// The request is sent exactly as given: the payload codecs (`Options.codecs`) and
    /// `Options.encode_failure_attributes` are applied to neither the request nor the
    /// response, the client's namespace and identity are not filled in, and the call does not
    /// pass through `Options.interceptors`.
    ///
    /// The call's deadline is `RpcOptions.timeout` on a view from `withRpcOptions`, else
    /// `Options.rpc_timeout`, else sdk-core's default for the method; it applies to every
    /// method alike, long-polls included. The call makes one attempt unless a view sets
    /// `RpcOptions.retry`, and sends a view's metadata with the client's headers and API key.
    /// Canceling the calling task cancels the call and returns `error.Canceled`.
    ///
    /// A call the server refuses returns the error of its gRPC status (see
    /// `CallServiceError`); `error.RpcFailed` means a failure without one. When `failure` is
    /// given, it is set to null first, then to the failure for `error.RpcFailed` (with a null
    /// `code`) and for every status error, its slices allocated from `arena`; nothing is
    /// logged. Without it, a failure other than `error.NotFound` and `error.AlreadyExists` is
    /// logged as a warning. Thread-safe.
    pub fn callService(
        c: *Client,
        comptime service: Service,
        comptime method: []const u8,
        arena: Allocator,
        request: ServiceRequest(service, method),
        failure: ?*?RpcFailure,
    ) CallServiceError!ServiceResponse(service, method) {
        const Response = ServiceResponse(service, method);
        const what = @tagName(service) ++ " service call " ++ method;
        if (failure) |out| out.* = null;
        var encoded: Io.Writer.Allocating = .init(arena);
        request.encode(&encoded.writer, arena) catch return error.OutOfMemory;
        const connection = c.currentConnection() catch |err| switch (err) {
            error.OutOfMemory, error.Canceled => |e| return e,
            // Logged by `currentConnection`.
            else => {
                if (failure) |out| out.* = .{ .code = null, .message = "connecting to the server failed", .details = "" };
                return error.RpcFailed;
            },
        };
        var bridge_failure: ?bridge.Connection.RpcFailure = null;
        defer if (bridge_failure) |f| f.deinit(c.gpa);
        const deadline_ms = c.callDeadlineMs();
        const started = Io.Timestamp.now(c.io, .awake);
        const bytes = connection.call(c.gpa, c.io, service, method, encoded.written(), c.callOptions(false, deadline_ms), &bridge_failure) catch |err| switch (err) {
            error.OutOfMemory, error.Canceled => |e| return e,
            error.InvalidOptions => {
                const message = "sdk-core rejected the call's metadata";
                if (failure) |out| out.* = .{ .code = null, .message = message, .details = "" } else log.warn(what ++ " failed: " ++ message, .{});
                return error.RpcFailed;
            },
            error.RpcFailed => {
                const f = bridge_failure.?;
                const code: ?RpcCode = if (f.status_code == 0) null else @fromBackingInt(@intCast(f.status_code));
                var result = statusError(f.status_code, f.message);
                // The server ends the call at the same deadline (sent as grpc-timeout) by
                // resetting the stream, which sdk-core reports as CANCELLED without "Timeout
                // expired" when the reset arrives first.
                if (result == error.RpcCanceled and deadline_ms > 0 and
                    started.durationTo(Io.Timestamp.now(c.io, .awake)).toMilliseconds() >= deadline_ms) result = error.DeadlineExceeded;
                if (failure) |out| {
                    out.* = .{ .code = code, .message = try arena.dupe(u8, f.message), .details = try arena.dupe(u8, f.details) };
                } else if (result != error.NotFound and result != error.AlreadyExists) {
                    if (code) |s| {
                        log.warn(what ++ " failed: {s} (gRPC status {d})", .{ f.message, @backingInt(s) });
                    } else log.warn(what ++ " failed: {s}", .{f.message});
                }
                return result;
            },
        };
        defer c.gpa.free(bytes);
        const owned = try arena.dupe(u8, bytes);
        var reader: Io.Reader = .fixed(owned);
        return Response.decode(&reader, arena) catch |err| switch (err) {
            error.OutOfMemory => error.OutOfMemory,
            else => {
                if (failure == null) log.warn(what ++ " failed: the response doesn't decode as a {s} ({t})", .{ comptime converter.protoMessageType(Response), err });
                return error.InvalidResponse;
            },
        };
    }

    /// Frees the client, connected or not, once no call on it is running and the workers on
    /// it have stopped. Doesn't wait on the network. Close each client exactly once.
    ///
    /// The connection and the runtime belong to every client that shares them (see
    /// `forNamespace`), and `close` frees them only when it closes the last of those
    /// clients, whichever that is: they may be closed in any order. Until then, the others
    /// keep working, and so do the connections `currentConnection` returned and the
    /// handlers `metrics` returned on the closed one; sdk-core's logs keep reaching the
    /// first client's `Options.logger`.
    ///
    /// A first connection attempt that every call gave up on (see `init`) may
    /// still be running in sdk-core, which has no deadline of its own for it (a server that
    /// never answers a TLS handshake keeps it running for good). sdk-core's task uses the
    /// client's runtime until the attempt ends, so `close` then leaves the runtime to it
    /// instead of freeing it: the runtime, its threads, and the attempt stay allocated for
    /// the rest of the process (the attempt frees itself if it ends). sdk-core's logs stop
    /// reaching `Options.logger` when the last client sharing the runtime closes, but a
    /// custom metrics handler (`Options.metrics`) stays in use by that runtime, so it must
    /// then outlive the process.
    pub fn close(c: *Client) void {
        // A client made by `forNamespace` shares the connection and the runtime: the last
        // client to close frees them. Acquire pairs with the releases of the clients closed
        // before, so their last uses of the shared state happen before it is freed.
        // A view from `withRpcOptions` owns nothing.
        if (c.rpc_options != null) return;
        const prev = c.shared.refs.fetchSub(1, .acq_rel);
        std.debug.assert(prev > 0); // Closed more times than there are clients.
        if (prev == 1) c.shared.destroy();
        c.gpa.free(c.namespace);
        c.gpa.free(c.identity);
        c.* = undefined;
    }

    /// The connection calls go over now, connecting first on a client that hasn't connected
    /// yet (see `init`; the failure is logged, and the client stays unconnected). It stays
    /// valid until the last client sharing it closes (see `forNamespace`), even after a
    /// `reconnect` replaces it. Thread-safe.
    pub fn currentConnection(c: *Client) ConnectError!bridge.Connection {
        return c.currentConnectionAs(.first_use);
    }

    fn currentConnectionAs(c: *Client, comptime attempt: Attempt) ConnectError!bridge.Connection {
        return c.firstConnection(attempt, null) catch |err| switch (err) {
            error.Stopped => unreachable, // Only with a stop flag.
            else => |e| e,
        };
    }

    /// For `worker.Worker.run`: `currentConnection`, which also returns `error.Stopped`, without
    /// connecting, once `stop` is set and `wakeConnecting` has been called after it.
    pub fn currentConnectionUnless(c: *Client, stop: *const std.atomic.Value(bool)) (ConnectError || error{Stopped})!bridge.Connection {
        return c.firstConnection(.first_use, stop);
    }

    /// For `worker.Worker.stop`: wakes the calls waiting for a first connection to check their
    /// stop flags. Thread-safe.
    pub fn wakeConnecting(c: *Client) void {
        c.shared.connection_mutex.lockUncancelable(c.io);
        defer c.shared.connection_mutex.unlock(c.io);
        if (c.shared.connecting) |p| p.wakeWaiters();
    }

    /// The connection, connecting first if there is none: every call shares one attempt and
    /// waits for it until its own deadline (`Options.rpc_timeout` from now), its `stop` flag,
    /// or a cancel of its task, whichever comes first. A `.connect` waits until the same
    /// deadline, without cancellation. Keeps at most `max_connect_attempts_in_flight`
    /// attempts in flight: at that many, it rejoins the oldest abandoned one.
    fn firstConnection(c: *Client, comptime attempt: Attempt, stop: ?*const std.atomic.Value(bool)) (ConnectError || error{Stopped})!bridge.Connection {
        // sdk-core's connect has no deadline of its own: a first connection gets the call's,
        // or `connect`'s, counted from now.
        const deadline: ?Io.Clock.Timestamp = switch (attempt) {
            .first_use, .connect => .fromNow(c.io, .{ .raw = .fromMilliseconds(if (c.callDeadlineMs() > 0) c.callDeadlineMs() else default_rpc_timeout_ms), .clock = .awake }),
            .reconnect => null,
        };
        const connecting = blk: {
            c.shared.connection_mutex.lockUncancelable(c.io);
            defer c.shared.connection_mutex.unlock(c.io);
            if (c.shared.connection) |conn| return conn;
            if (stop) |f| if (f.load(.acquire)) return error.Stopped;
            if (c.shared.connecting == null) {
                // Make room to keep the attempt for `close` if it is cleared before the core
                // calls back, dropping those whose callback has run.
                var i: usize = 0;
                while (i < c.shared.abandoned_attempts.items.len) {
                    const p = c.shared.abandoned_attempts.items[i];
                    if (p.calledBack()) {
                        _ = p.unref();
                        // Ordered: the oldest abandoned stays first, for a rejoin.
                        _ = c.shared.abandoned_attempts.orderedRemove(i);
                    } else i += 1;
                }
                try c.shared.abandoned_attempts.ensureUnusedCapacity(c.gpa, 1);
                if (c.shared.abandoned_attempts.items.len >= max_connect_attempts_in_flight) {
                    // As many attempts are stuck as a client keeps: wait for the oldest, which
                    // connects where a new one would (only `reconnect` changes the address,
                    // and it connects the client), rather than start another. The list's
                    // reference moves to `connecting`, and the slot it frees is the room
                    // `stopWaitingForConnecting` needs.
                    c.shared.connecting = c.shared.abandoned_attempts.orderedRemove(0);
                    // Started before updates this can't count: apply the latest to it.
                    c.shared.connecting_updates = null;
                } else {
                    // Starting doesn't wait on the network: the core connects on its own threads.
                    c.shared.connecting = try startConnecting(c.gpa, c.io, c.shared.runtime, c.identity, c.shared.pending_connection.?.options, attempt);
                    c.shared.connecting_updates = c.shared.pending_updates;
                }
                c.shared.connect_mutex.lockUncancelable(c.io);
            }
            const connecting = c.shared.connecting.?;
            connecting.ref();
            c.shared.connecting_waiters += 1;
            break :blk connecting;
        };
        // Never the reference that orphans it: `connecting` or `abandoned_attempts` holds one
        // until the callback has finished.
        defer _ = connecting.unref();
        const waited: error{ Timeout, Canceled, Stopped }!void = if (attempt == .connect) waited: {
            // `connect` documents that it can't be canceled.
            const prev = c.io.swapCancelProtection(.blocked);
            defer _ = c.io.swapCancelProtection(prev);
            connecting.wait(deadline, null) catch |err| switch (err) {
                error.Canceled, error.Stopped => unreachable, // Blocked, and no stop flag.
                error.Timeout => break :waited error.Timeout,
            };
            break :waited {};
        } else connecting.wait(deadline, stop);

        c.shared.connection_mutex.lockUncancelable(c.io);
        defer c.shared.connection_mutex.unlock(c.io);
        if (c.shared.connecting == connecting) c.shared.connecting_waiters -= 1;
        if (connecting.isDone()) return c.finishConnecting(connecting, attempt);
        if (c.shared.connecting == connecting and c.shared.connecting_waiters == 0) c.stopWaitingForConnecting();
        // A `reconnect` connected meanwhile and woke the waiters (`supersede`).
        if (c.shared.connection) |conn| return conn;
        waited catch |err| switch (err) {
            error.Canceled => return error.Canceled,
            error.Stopped => return error.Stopped,
            error.Timeout => {
                const host_port = c.shared.pending_connection.?.options.host_port;
                switch (attempt) {
                    .connect => logConnectFailure(.connect, host_port, "the server didn't answer within Options.rpc_timeout (30 s without one)"),
                    else => logConnectFailure(attempt, host_port, "the server didn't answer within the call's deadline (Options.rpc_timeout)"),
                }
                return error.ConnectFailed;
            },
        };
        unreachable; // A wait returns without an error once the attempt is done or superseded.
    }

    /// Ends a first connection the core has finished, for each call that waited for it:
    /// the first takes the connection, or logs why there is none. Call with
    /// `connection_mutex` held.
    fn finishConnecting(c: *Client, connecting: *bridge.Connection.Connecting, comptime attempt: Attempt) ConnectError!bridge.Connection {
        // Only the first call to see it done finds it still set.
        if (c.shared.connecting != connecting) {
            if (c.shared.connection) |conn| return conn;
            return error.ConnectFailed;
        }
        const updates = c.shared.connecting_updates;
        c.stopWaitingForConnecting();
        // A `reconnect` connected meanwhile: the attempt's connection is freed with it.
        if (c.shared.connection) |conn| return conn;
        const conn = connecting.take() orelse {
            logConnectFailure(attempt, c.shared.pending_connection.?.options.host_port, connecting.failureMessage());
            return error.ConnectFailed;
        };
        if (updates == null or c.shared.pending_updates != updates.?) {
            const latest = c.shared.pending_connection.?.options;
            conn.setApiKey(latest.api_key);
            conn.setMetadata(c.gpa, latest.headers) catch |err| {
                conn.deinit();
                return switch (err) {
                    error.OutOfMemory => error.OutOfMemory,
                    // The headers were checked when they were set.
                    error.NewlineInKey, error.NewlineInValue => unreachable,
                };
            };
        }
        c.connected(conn);
        return conn;
    }

    /// Clears `connecting`, so the next call starts another attempt. Call with
    /// `connection_mutex` held.
    fn stopWaitingForConnecting(c: *Client) void {
        const p = c.shared.connecting.?;
        // `firstConnection` made room before it started the attempt.
        if (p.calledBack()) _ = p.unref() else c.shared.abandoned_attempts.appendAssumeCapacity(p);
        c.shared.connecting = null;
        c.shared.connecting_waiters = 0;
        c.shared.connect_mutex.unlock(c.io);
    }

    /// sdk-core's deadline for a call, which `Options.rpc_timeout` replaces.
    const default_rpc_timeout_ms = 30_000;

    /// The most connection attempts a client keeps in flight: `Shared.connecting` and
    /// `Shared.abandoned_attempts` together. sdk-core's connect can't be canceled, so against
    /// a server that never answers each would otherwise hold a socket and a core task for
    /// good.
    const max_connect_attempts_in_flight = 4;

    fn connectionIfAny(c: *Client) ?bridge.Connection {
        c.shared.connection_mutex.lockUncancelable(c.io);
        defer c.shared.connection_mutex.unlock(c.io);
        return c.shared.connection;
    }

    /// Takes `conn` as the first connection. Call with `connection_mutex` held.
    fn connected(c: *Client, conn: bridge.Connection) void {
        c.shared.connection = conn;
        c.shared.pending_connection.?.deinit();
        c.shared.pending_connection = null;
    }

    /// Replaces the API key sent with every call from now on, by this client and every worker
    /// on it, and by the clients sharing its connection (see `forNamespace`) and their
    /// workers; empty sends none. Calls already sent keep the key they had. Use it to rotate a
    /// key before it expires. On a client that hasn't connected yet (see `init`), it is the
    /// key the client connects with. A later `reconnect` sends its own
    /// `ConnectionOptions.api_key` instead. Thread-safe.
    pub fn updateApiKey(c: *Client, api_key: []const u8) error{OutOfMemory}!void {
        c.shared.connection_mutex.lockUncancelable(c.io);
        defer c.shared.connection_mutex.unlock(c.io);
        if (c.shared.connection) |conn| return conn.setApiKey(api_key);
        var options = c.shared.pending_connection.?.options;
        options.api_key = api_key;
        try c.replacePending(options);
    }

    /// Replaces the gRPC headers (`Options.headers`) sent with every call from now on, by
    /// this client and every worker on it, and by the clients sharing its connection (see
    /// `forNamespace`) and their workers. Returns `error.InvalidOptions` (logged) for a key
    /// that isn't a valid header name or ends in "-bin" (binary headers aren't supported), or
    /// a value with characters other than visible ASCII, space and tab; the headers are then
    /// unchanged. On a client that hasn't connected yet (see `init`), they are the headers
    /// the client connects with. A later `reconnect` sends its own `ConnectionOptions.headers`
    /// instead. Thread-safe.
    pub fn updateHeaders(c: *Client, headers: []const bridge.bytes.MetadataEntry) error{ OutOfMemory, InvalidOptions }!void {
        for (headers) |h| try validateHeader(h);
        c.shared.connection_mutex.lockUncancelable(c.io);
        defer c.shared.connection_mutex.unlock(c.io);
        if (c.shared.connection) |conn| return conn.setMetadata(c.gpa, headers) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            // validateHeader already rejects newlines.
            error.NewlineInKey, error.NewlineInValue => unreachable,
        };
        var options = c.shared.pending_connection.?.options;
        options.headers = headers;
        try c.replacePending(options);
    }

    /// Replaces the options an unconnected client connects with by copies of `options`, which
    /// may borrow from the current ones. Call with `connection_mutex` held.
    fn replacePending(c: *Client, options: ConnectionOptions) error{OutOfMemory}!void {
        const next: PendingConnection = try .init(c.gpa, options);
        c.shared.pending_connection.?.deinit();
        c.shared.pending_connection = next;
        c.shared.pending_updates += 1;
    }

    /// Connects again with `options` (address, API key, TLS, headers, keep-alive, proxy,
    /// retry policy, and client name and version) and sends every later call over the new
    /// connection: this client's, and those of the workers running on it, which move over
    /// without stopping (their polls already waiting finish on the old connection). Workers
    /// started later use it too. The clients sharing this client's connection (see
    /// `forNamespace`) and their workers move with it. TLS files named by `options.tls` are
    /// read again, so this is how to rotate a client certificate: sdk-core can't change the
    /// certificate of a live connection. The new connection is checked first; if that fails
    /// (`ConnectError`, logged as a warning), the client keeps the current one. The old
    /// connection is freed when the last client sharing it closes, since calls already sent
    /// may still be using it. A worker that is already shutting down can't move and
    /// finishes on the old connection (logged as a warning). On a client that hasn't connected yet (see `init`), it makes
    /// the first connection with `options` in place of those the client was created with;
    /// if that fails (logged as a warning), the client stays unconnected and keeps its own
    /// options for its next call. Calls waiting for a first connection in progress then get
    /// this one. Like a reconnect, it waits without a deadline or cancellation, but other
    /// calls don't wait for it. Thread-safe.
    pub fn reconnect(c: *Client, options: ConnectionOptions) ConnectError!void {
        const next = if (c.connectionIfAny() == null)
            try openConnection(c.gpa, c.io, c.shared.runtime, c.identity, options, .first_use)
        else
            try openConnection(c.gpa, c.io, c.shared.runtime, c.identity, options, .reconnect);
        c.shared.connection_mutex.lockUncancelable(c.io);
        defer c.shared.connection_mutex.unlock(c.io);
        if (c.shared.connection == null) {
            // Calls waiting for a first connection in progress take this one instead.
            if (c.shared.connecting) |p| p.supersede();
            return c.connected(next);
        }
        c.shared.retired_connections.append(c.gpa, c.shared.connection.?) catch {
            next.deinit();
            return error.OutOfMemory;
        };
        c.shared.connection = next;
        for (c.shared.running_workers.items) |ptr| replaceWorkerClient(c, ptr, next);
    }

    /// For `worker.Worker.run`: registers a core worker it created on `created_on`, so
    /// `reconnect` moves it, and moves it at once if a `reconnect` came in between. Pair with
    /// `untrackWorker` before the core worker is freed.
    pub fn trackWorker(c: *Client, ptr: *bridge.c.TemporalCoreWorker, created_on: bridge.Connection) error{OutOfMemory}!void {
        c.shared.connection_mutex.lockUncancelable(c.io);
        defer c.shared.connection_mutex.unlock(c.io);
        try c.shared.running_workers.append(c.gpa, ptr);
        // A worker runs on a connection `currentConnection` returned, so there is one.
        const current = c.shared.connection.?;
        if (created_on.ptr != current.ptr) replaceWorkerClient(c, ptr, current);
    }

    /// For `worker.Worker.run`: unregisters a core worker `trackWorker` registered; does
    /// nothing if it isn't registered.
    pub fn untrackWorker(c: *Client, ptr: *bridge.c.TemporalCoreWorker) void {
        c.shared.connection_mutex.lockUncancelable(c.io);
        defer c.shared.connection_mutex.unlock(c.io);
        for (c.shared.running_workers.items, 0..) |p, i| if (p == ptr) {
            _ = c.shared.running_workers.swapRemove(i);
            return;
        };
    }

    /// sdk-core refuses to move a worker that is shutting down: it finishes on the old
    /// connection.
    fn replaceWorkerClient(c: *Client, ptr: *bridge.c.TemporalCoreWorker, connection: bridge.Connection) void {
        const w: bridge.Worker = .{ .ptr = ptr, .runtime = c.shared.runtime };
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

    /// Starts the workflow behind a Nexus workflow-run operation (`temporal.nexus`), so the
    /// server completes the operation when the workflow closes. Applications don't call
    /// this: return a `nexus.WorkflowStart` from an operation handler instead.
    ///
    /// The request is a normal start of `workflow_type` with `options` and `args`, plus
    /// `attach`: its request ID replaces the generated one, so a retried start with the same
    /// request ID finds the workflow the first one started; the completion callback (when
    /// `attach.callback_url` is set) and `attach.links` are recorded with the workflow; and
    /// a start that finds the workflow already running under `id_conflict_policy`
    /// `.use_existing` attaches the request ID, callback, and links to it.
    /// `options.id` must be set. `enable_eager_start` and `error_when_already_started` are
    /// ignored: a workflow ID held by another start returns
    /// `error.WorkflowExecutionAlreadyStarted`.
    ///
    /// The result's memory, and everything the call allocates, comes from `arena`. Errors are
    /// `executeWorkflow`'s.
    pub fn startNexusWorkflow(c: *Client, arena: Allocator, workflow_type: []const u8, options: StartWorkflowOptions, args: anytype, attach: NexusWorkflowAttachments) StartError!NexusWorkflowStarted {
        if (options.id.len == 0) {
            log.warn("starting workflow of type \"{s}\" for a Nexus operation: StartWorkflowOptions.id is empty; the operation token names the workflow by ID, so set it", .{workflow_type});
            return error.InvalidOptions;
        }
        var request = try c.startRequest(arena, workflow_type, options.id, options, args);
        request.request_id = attach.request_id;
        try request.links.appendSlice(arena, attach.links);
        if (attach.callback_url.len > 0) {
            var callback: proto.temporal.api.common.v1.Callback = .{ .variant = .{ .nexus = .{ .url = attach.callback_url } } };
            for (attach.callback_headers) |h| try callback.variant.?.nexus.header.append(arena, .{ .key = h.name, .value = h.value });
            try callback.links.appendSlice(arena, attach.links);
            try request.completion_callbacks.append(arena, callback);
        }
        if (attach.callback_url.len > 0 or attach.links.len > 0)
            request.on_conflict_options = .{ .attach_request_id = true, .attach_completion_callbacks = true, .attach_links = true };
        var header = try c.outgoingHeader(arena, options.context);
        var status: ?RpcStatus = null;
        const status_out: ?*?RpcStatus = if (attach.rejection != null) &status else null;
        const response = c.rpcCall(wfs.StartWorkflowExecutionResponse, "StartWorkflowExecution", request, arena, status_out, &header, options.context) catch |err| switch (err) {
            error.AlreadyExists => return error.WorkflowExecutionAlreadyStarted,
            error.RpcFailed => {
                if (attach.rejection) |out| out.* = if (status) |s| rpcRejection(s) else null;
                return error.RpcFailed;
            },
            else => |e| return e,
        };
        return .{
            .run_id = response.run_id,
            .link = response.link orelse .{ .variant = .{ .workflow_event = .{
                .namespace = c.namespace,
                .workflow_id = options.id,
                .run_id = response.run_id,
                .reference = .{ .request_id_ref = .{ .request_id = attach.request_id, .event_type = .EVENT_TYPE_WORKFLOW_EXECUTION_STARTED } },
            } } },
        };
    }

    /// The start request for `options`, allocated from `a`. Signal-with-start copies it.
    fn startRequest(c: *Client, a: Allocator, workflow_type: []const u8, id: []const u8, options: StartWorkflowOptions, args: anytype) (search_attributes.EncodeError || error{InvalidOptions})!wfs.StartWorkflowExecutionRequest {
        if (options.task_queue.len == 0) {
            log.warn("starting workflow \"{s}\" of type \"{s}\": StartWorkflowOptions.task_queue is empty; set it to the task queue a worker polls (WorkflowEnvironment fills in its own)", .{ id, workflow_type });
            return error.InvalidOptions;
        }
        const context: converter.Context = .{
            .values = options.context orelse &.empty,
            .namespace = c.namespace,
            .workflow_id = id,
            .workflow_type = workflow_type,
        };
        var request: wfs.StartWorkflowExecutionRequest = .{
            .namespace = c.namespace,
            .workflow_id = id,
            .workflow_type = .{ .name = workflow_type },
            .task_queue = .{ .name = options.task_queue, .kind = .TASK_QUEUE_KIND_NORMAL },
            .input = .{ .payloads = try converter.toPayloadsWithContext(a, &context, args) },
            .workflow_execution_timeout = proto_time.durationToProto(options.execution_timeout),
            .workflow_run_timeout = proto_time.durationToProto(options.run_timeout),
            .workflow_task_timeout = proto_time.durationToProto(options.task_timeout),
            .workflow_start_delay = proto_time.durationToProto(options.start_delay),
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
        if (options.versioning_override) |o| request.versioning_override = versioningOverrideProto(o);
        if (options.priority) |p| request.priority = .{ .priority_key = p.priority_key, .fairness_key = p.fairness_key, .fairness_weight = p.fairness_weight };
        return request;
    }

    fn protoRetryPolicy(a: Allocator, rp: workflow.RetryPolicy) error{OutOfMemory}!proto.temporal.api.common.v1.RetryPolicy {
        var p: proto.temporal.api.common.v1.RetryPolicy = .{
            .initial_interval = proto_time.durationToProto(rp.initial_interval),
            .backoff_coefficient = rp.backoff_coefficient,
            .maximum_interval = proto_time.durationToProto(rp.maximum_interval),
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
        return c.signalWorkflowByNameWithOptions(Signal.signal_name, workflow_id, run_id, args, options);
    }

    /// Sends the signal named `signal_name` with the tuple `args` to a workflow, for a caller
    /// with no `workflow.SignalDef`: a workflow's handler for that name, or its dynamic
    /// signal handler, receives it. Otherwise behaves as `signalWorkflow`.
    pub fn signalWorkflowByName(c: *Client, signal_name: []const u8, workflow_id: []const u8, run_id: []const u8, args: anytype) SignalError!void {
        return c.signalWorkflowByNameWithOptions(signal_name, workflow_id, run_id, args, .{});
    }

    /// `signalWorkflowByName` with options, which behave as in `signalWorkflowWithOptions`.
    pub fn signalWorkflowByNameWithOptions(c: *Client, signal_name: []const u8, workflow_id: []const u8, run_id: []const u8, args: anytype, options: SignalOptions) SignalError!void {
        var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
        defer arena_state.deinit();
        const a = arena_state.allocator();
        var header = try c.outgoingHeader(a, options.context);
        const context = c.workflowContext(workflow_id, options.context);
        _ = c.rpcCall(wfs.SignalWorkflowExecutionResponse, "SignalWorkflowExecution", wfs.SignalWorkflowExecutionRequest{
            .namespace = c.namespace,
            .workflow_execution = .{ .workflow_id = workflow_id, .run_id = run_id },
            .signal_name = signal_name,
            .input = .{ .payloads = try converter.toPayloadsWithContext(a, &context, args) },
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
        return c.signalWithStartWorkflowByName(Workflow.Result, Workflow.workflow_name, options, args, Signal.signal_name, signal_args);
    }

    /// `signalWithStartWorkflow` with the workflow type and the signal named at run time:
    /// `args` and `signal_args` are tuples of their arguments, and `Result` the type the
    /// run's result decodes to (`void` to ignore it). Pass a definition's `workflow_name` to
    /// name only the signal at run time. Otherwise behaves as `signalWithStartWorkflow`.
    pub fn signalWithStartWorkflowByName(
        c: *Client,
        comptime Result: type,
        workflow_type: []const u8,
        options: StartWorkflowOptions,
        args: anytype,
        signal_name: []const u8,
        signal_args: anytype,
    ) StartError!WorkflowRun(Result) {
        var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
        defer arena_state.deinit();
        const a = arena_state.allocator();
        const id = if (options.id.len > 0) options.id else try uuid(a, c.io);

        const start = try c.startRequest(a, workflow_type, id, options, args);
        const context: converter.Context = .{
            .values = options.context orelse &.empty,
            .namespace = c.namespace,
            .workflow_id = id,
            .workflow_type = workflow_type,
        };
        var request: wfs.SignalWithStartWorkflowExecutionRequest = .{
            .signal_name = signal_name,
            .signal_input = .{ .payloads = try converter.toPayloadsWithContext(a, &context, signal_args) },
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
    /// `run_id` targets the latest run. Returns `error.NotFound` if no such workflow exists
    /// or the run has closed. `terminateWorkflowWithOptions` attaches details.
    pub fn terminateWorkflow(c: *Client, workflow_id: []const u8, run_id: []const u8, reason: []const u8) error{ OutOfMemory, Canceled, RpcFailed, NotFound, InvalidResponse, CodecFailed }!void {
        return c.terminateWorkflowWithOptions(workflow_id, run_id, reason, {}, .{});
    }

    /// `terminateWorkflow` with `details` recorded in the terminated event (any value the
    /// converter encodes, encoded with the client's codecs, or `{}` for none) and
    /// `options`. A `first_execution_run_id` that isn't the chain's first run is
    /// `error.NotFound`, and the workflow keeps running. `options.links` are recorded in the
    /// terminated event. Only read during the call.
    pub fn terminateWorkflowWithOptions(c: *Client, workflow_id: []const u8, run_id: []const u8, reason: []const u8, details: anytype, options: TerminateOptions) TerminateError!void {
        var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
        defer arena_state.deinit();
        const a = arena_state.allocator();
        var request: wfs.TerminateWorkflowExecutionRequest = .{
            .namespace = c.namespace,
            .workflow_execution = .{ .workflow_id = workflow_id, .run_id = run_id },
            .reason = reason,
            .details = try detailPayloads(a, &c.workflowContext(workflow_id, null), details),
            .identity = c.identity,
            .first_execution_run_id = options.first_execution_run_id,
        };
        try request.links.appendSlice(a, options.links);
        _ = c.rpc(wfs.TerminateWorkflowExecutionResponse, "TerminateWorkflowExecution", request, a) catch |err| switch (err) {
            error.AlreadyExists => return error.InvalidResponse,
            else => |e| return e,
        };
    }

    /// Resets a workflow: a new run of `workflow_id` starts with the history of run `run_id`
    /// (the latest run when empty) up to `options.workflow_task_finish_event_id`, and goes on
    /// from there. The base run, if still running, ends terminated, as does whichever run of
    /// the workflow is current; a closed base run stays as it was. Events after the reset
    /// point are reapplied to the new run as `options.reapply_exclude` allows. A repeated
    /// call may reset again, even with the same `request_id`.
    ///
    /// Returns the new run's ID, allocated from `arena` (as is everything the call
    /// allocates); `getWorkflow` makes a handle to it. A handle that names the base run's ID,
    /// such as one `executeWorkflow` returned, doesn't follow the reset: its `get` reports
    /// `error.WorkflowTerminated` (or the run's own result if it had closed). A handle made
    /// with an empty run ID reads the latest run when `get` runs, so it gets the new run's
    /// result.
    ///
    /// A non-positive event ID is `error.InvalidOptions` before any call. An event ID the
    /// server refuses (not at a workflow task's boundary, or beyond the history) is
    /// `error.RpcFailed`; refusals are logged as warnings with the server's reason. The
    /// request is sent once, without the automatic retries other calls get. A transport
    /// failure is `error.RpcFailed` too, and after it the reset may or may not have
    /// happened: before resetting again, describe the base run, whose
    /// `raw.workflow_extended_info.reset_run_id` names the new run once it has been reset.
    pub fn resetWorkflow(c: *Client, arena: Allocator, workflow_id: []const u8, run_id: []const u8, options: ResetOptions) ResetError![]const u8 {
        if (options.workflow_task_finish_event_id <= 0) {
            log.warn("resetting workflow {s}: ResetOptions.workflow_task_finish_event_id is {d}; set it to the event ID of a workflow task's completed, failed, timed-out, or started event in the run's history (see Client.fetchHistory)", .{ workflow_id, options.workflow_task_finish_event_id });
            return error.InvalidOptions;
        }
        var request: wfs.ResetWorkflowExecutionRequest = .{
            .namespace = c.namespace,
            .workflow_execution = .{ .workflow_id = workflow_id, .run_id = run_id },
            .reason = options.reason,
            .workflow_task_finish_event_id = options.workflow_task_finish_event_id,
            .request_id = if (options.request_id.len > 0) options.request_id else try uuid(arena, c.io),
            .identity = c.identity,
        };
        const exclude = options.reapply_exclude;
        for ([_]struct { bool, enums.ResetReapplyExcludeType }{
            .{ exclude.signals, .RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL },
            .{ exclude.updates, .RESET_REAPPLY_EXCLUDE_TYPE_UPDATE },
            .{ exclude.nexus, .RESET_REAPPLY_EXCLUDE_TYPE_NEXUS },
        }) |pair| if (pair[0]) try request.reset_reapply_exclude_types.append(arena, pair[1]);
        const response = try c.workflowChange(wfs.ResetWorkflowExecutionResponse, "ResetWorkflowExecution", request, arena, workflow_id);
        if (response.run_id.len == 0) {
            log.warn("ResetWorkflowExecution for workflow {s} returned no new run ID", .{workflow_id});
            return error.InvalidResponse;
        }
        return response.run_id;
    }

    /// Changes the execution options of run `run_id` of `workflow_id` (the latest run when
    /// empty): each non-null field of `changes` is applied, and the others are left as they
    /// are. The server records a change in the workflow's history, once any workflow task in
    /// progress has ended; one that leaves the options as they were (removing an override the
    /// workflow doesn't have, setting the one it has) succeeds and records nothing. Returns
    /// the run's options after the change, allocated from `arena` (as is everything the call
    /// allocates).
    ///
    /// A `changes` that changes nothing is `error.InvalidOptions` before any call. An unknown
    /// or closed workflow is `error.NotFound`. A change the server refuses, such as a pin to
    /// a version whose workers haven't polled the workflow's task queue, is
    /// `error.RpcFailed`; refusals are logged as warnings with the server's reason.
    pub fn updateWorkflowExecutionOptions(c: *Client, arena: Allocator, workflow_id: []const u8, run_id: []const u8, changes: WorkflowExecutionOptionsChanges) UpdateExecutionOptionsError!WorkflowExecutionOptions {
        var request: wfs.UpdateWorkflowExecutionOptionsRequest = .{
            .namespace = c.namespace,
            .workflow_execution = .{ .workflow_id = workflow_id, .run_id = run_id },
            // The server requires the options even when the change only clears a field.
            .workflow_execution_options = .{},
            .update_mask = .{},
            .identity = c.identity,
        };
        if (changes.versioning_override) |change| {
            request.workflow_execution_options.?.versioning_override = switch (change) {
                .set => |o| versioningOverrideProto(o),
                .remove => null,
            };
            try request.update_mask.?.paths.append(arena, "versioning_override");
        }
        if (request.update_mask.?.paths.items.len == 0) {
            log.warn("updating the execution options of workflow {s}: WorkflowExecutionOptionsChanges changes nothing; set a field such as versioning_override", .{workflow_id});
            return error.InvalidOptions;
        }
        const response = try c.workflowChange(wfs.UpdateWorkflowExecutionOptionsResponse, "UpdateWorkflowExecutionOptions", request, arena, workflow_id);
        return .{
            .versioning_override = if (response.workflow_execution_options) |o| deployment.VersioningOverride.fromProto(o.versioning_override) else null,
            .raw = response,
        };
    }

    /// Sends a request that changes `workflow_id`, logging the server's reason for a refusal
    /// as a warning: these are refused for ordinary mistakes the caller handles.
    fn workflowChange(c: *Client, comptime Response: type, comptime method: []const u8, request: anytype, arena: Allocator, workflow_id: []const u8) error{ OutOfMemory, Canceled, RpcFailed, NotFound, InvalidResponse, CodecFailed }!Response {
        var status: ?RpcStatus = null;
        return c.rpcWithStatus(Response, method, request, arena, &status) catch |err| switch (err) {
            error.AlreadyExists => error.InvalidResponse,
            error.RpcFailed => {
                const s = status orelse return error.RpcFailed;
                log.warn("{s} for workflow {s} failed: {s} (gRPC status {d})", .{ method, workflow_id, s.message, s.code });
                return error.RpcFailed;
            },
            else => |e| e,
        };
    }

    /// Requests cancellation of a workflow execution. The workflow sees it as `error.Canceled`
    /// from its next cancelable wait and may clean up before it returns; the run ends
    /// canceled when the workflow returns `error.Canceled`. An empty `run_id` targets the
    /// latest run. Returns `error.NotFound` if no such workflow exists.
    /// `cancelWorkflowWithOptions` takes options.
    pub fn cancelWorkflow(c: *Client, workflow_id: []const u8, run_id: []const u8, reason: []const u8) error{ OutOfMemory, Canceled, RpcFailed, NotFound, InvalidResponse, CodecFailed }!void {
        return c.cancelWorkflowWithOptions(workflow_id, run_id, reason, .{});
    }

    /// `cancelWorkflow` with `options`. A `first_execution_run_id` that isn't the chain's
    /// first run is `error.NotFound`, and nothing is canceled. `options.links` are recorded
    /// in the cancel-requested event. Only read during the call.
    pub fn cancelWorkflowWithOptions(c: *Client, workflow_id: []const u8, run_id: []const u8, reason: []const u8, options: CancelOptions) CancelError!void {
        var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
        defer arena_state.deinit();
        const a = arena_state.allocator();
        var request: wfs.RequestCancelWorkflowExecutionRequest = .{
            .namespace = c.namespace,
            .workflow_execution = .{ .workflow_id = workflow_id, .run_id = run_id },
            .identity = c.identity,
            .request_id = try uuid(a, c.io),
            .reason = reason,
            .first_execution_run_id = options.first_execution_run_id,
        };
        try request.links.appendSlice(a, options.links);
        _ = c.rpc(wfs.RequestCancelWorkflowExecutionResponse, "RequestCancelWorkflowExecution", request, a) catch |err| switch (err) {
            error.AlreadyExists => return error.InvalidResponse,
            else => |e| return e,
        };
    }

    /// Requests cancellation of the latest run of `workflow_id` for a Nexus workflow-run
    /// operation's cancel (`temporal.nexus`), as `cancelWorkflow` does. Applications don't
    /// call this. A rejection the server reports with `error.RpcFailed` isn't logged: its gRPC
    /// status is stored in `rejected` (null without one, such as a transport error) for the
    /// caller to report, its message allocated from `arena`, as is everything the call
    /// allocates. Other errors are `cancelWorkflow`'s.
    pub fn cancelNexusWorkflow(c: *Client, arena: Allocator, workflow_id: []const u8, rejected: *?RpcRejection) error{ OutOfMemory, Canceled, RpcFailed, NotFound, InvalidResponse, CodecFailed }!void {
        var status: ?RpcStatus = null;
        rejected.* = null;
        _ = c.rpcWithStatus(wfs.RequestCancelWorkflowExecutionResponse, "RequestCancelWorkflowExecution", wfs.RequestCancelWorkflowExecutionRequest{
            .namespace = c.namespace,
            .workflow_execution = .{ .workflow_id = workflow_id },
            .identity = c.identity,
            .request_id = try uuid(arena, c.io),
        }, arena, &status) catch |err| switch (err) {
            error.AlreadyExists, error.RpcFailed => {
                if (status) |s| rejected.* = rpcRejection(s);
                return error.RpcFailed;
            },
            else => |e| return e,
        };
    }

    fn rpcRejection(s: RpcStatus) ?RpcRejection {
        return if (s.code == 0) null else .{ .code = @fromBackingInt(s.code), .message = s.message };
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
            .result = try resultPayloads(a, &.{ .namespace = c.namespace }, result),
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
            .result = try resultPayloads(a, &c.activityIdContext(id), result),
            .identity = c.identity,
        }, a);
    }

    /// Fails an activity that returned `ctx.completeAsync()` with the failure `f`. The
    /// activity's retry policy applies, as for a failure returned by the activity itself.
    /// Returns `error.NotFound` when the activity is no longer waiting. `f.cause` is read
    /// until this returns.
    pub fn failActivity(c: *Client, task_token: []const u8, f: failure_mod.Application) ActivityCompletionError!void {
        var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
        defer arena_state.deinit();
        const a = arena_state.allocator();
        _ = try c.activityRpc(wfs.RespondActivityTaskFailedResponse, "RespondActivityTaskFailed", wfs.RespondActivityTaskFailedRequest{
            .namespace = c.namespace,
            .task_token = task_token,
            .failure = try c.sentFailure(a, &.{ .namespace = c.namespace }, try failure_mod.application(a, f)),
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
            .failure = try c.sentFailure(a, &c.activityIdContext(id), try failure_mod.application(a, f)),
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
            .details = try detailPayloads(a, &.{ .namespace = c.namespace }, details),
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
            .details = try detailPayloads(a, &c.activityIdContext(id), details),
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
            .details = try detailPayloads(a, &.{ .namespace = c.namespace }, details),
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
            .details = try detailPayloads(a, &c.activityIdContext(id), details),
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

    /// `f`, which user code gave the client to send, as the failure converter sends it.
    fn sentFailure(c: *const Client, a: Allocator, context: *const converter.Context, f: failure_mod.Failure) error{OutOfMemory}!failure_mod.Failure {
        const fc = c.failure_converter orelse return f;
        return fc.toFailure(a, context, null, f);
    }

    /// `f`, a failure the server returned, as the failure converter gives it to user code.
    fn receivedFailure(c: *const Client, a: Allocator, context: *const converter.Context, f: failure_mod.Failure) error{OutOfMemory}!failure_mod.Failure {
        const fc = c.failure_converter orelse return f;
        return fc.fromFailure(a, context, f);
    }

    fn resultPayloads(a: Allocator, context: *const converter.Context, result: anytype) error{OutOfMemory}!proto.temporal.api.common.v1.Payloads {
        var payloads: proto.temporal.api.common.v1.Payloads = .{};
        try payloads.payloads.append(a, try converter.toPayloadWithContext(a, context, result));
        return payloads;
    }

    fn detailPayloads(a: Allocator, context: *const converter.Context, details: anytype) error{OutOfMemory}!?proto.temporal.api.common.v1.Payloads {
        if (@TypeOf(details) == void) return null;
        return try resultPayloads(a, context, details);
    }

    /// The converter context of a call naming the workflow `workflow_id`. Borrows both.
    fn workflowContext(c: *const Client, workflow_id: []const u8, values: ?*const propagation.Values) converter.Context {
        return .{ .values = values orelse &.empty, .namespace = c.namespace, .workflow_id = workflow_id };
    }

    /// The converter context of a call naming an activity by its ID. Borrows `id`.
    fn activityIdContext(c: *const Client, id: ActivityId) converter.Context {
        return .{ .namespace = c.namespace, .workflow_id = id.workflow_id, .activity_id = id.activity_id };
    }

    /// A converter context like `context`, allocated in `arena` with its strings copied, for
    /// values the caller decodes after the call returns. `values` stays borrowed.
    fn dupeContext(arena: Allocator, context: converter.Context) error{OutOfMemory}!*const converter.Context {
        const out = try arena.create(converter.Context);
        out.* = .{
            .values = context.values,
            .namespace = try arena.dupe(u8, context.namespace),
            .workflow_id = try arena.dupe(u8, context.workflow_id),
            .workflow_type = try arena.dupe(u8, context.workflow_type),
            .activity_type = try arena.dupe(u8, context.activity_type),
            .activity_id = try arena.dupe(u8, context.activity_id),
        };
        return out;
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
            .schedule_to_close_timeout = proto_time.durationToProto(options.schedule_to_close_timeout),
            .schedule_to_start_timeout = proto_time.durationToProto(options.schedule_to_start_timeout),
            .start_to_close_timeout = proto_time.durationToProto(options.start_to_close_timeout),
            .heartbeat_timeout = proto_time.durationToProto(options.heartbeat_timeout),
            .input = .{ .payloads = try converter.toPayloadsWithContext(a, &.{
                .values = options.context orelse &.empty,
                .namespace = c.namespace,
                .activity_type = activity_type,
                .activity_id = id,
            }, args) },
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
        if (options.summary.len > 0) request.user_metadata = .{ .summary = try converter.toPayload(a, options.summary) };
        if (options.priority) |p| request.priority = .{ .priority_key = p.priority_key, .fairness_key = p.fairness_key, .fairness_weight = p.fairness_weight };
        request.start_delay = proto_time.durationToProto(options.start_delay);
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
        var handle = try c.getActivityHandle(Result, id, response.run_id);
        errdefer handle.deinit();
        handle.activity_type = try c.gpa.dupe(u8, activity_type);
        handle.started = response.started;
        return handle;
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
        const context = c.workflowContext(workflow_id, options.context);
        const result = try c.sendQuery(Query.query_name, arena, workflow_id, run_id, try converter.toPayloadsWithContext(arena, &context, args), options);
        if (Query.Result == void) return;
        return converter.fromPayloadWithContext(Query.Result, arena, &context, result orelse return error.InvalidPayload);
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
        const context = try dupeContext(arena, c.workflowContext(workflow_id, options.context));
        return .{ .payload = try c.sendQuery(query_type, arena, workflow_id, run_id, try converter.toPayloadsWithContext(arena, context, args), options), .context = context };
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
        return c.updateWorkflowByName(Update.Result, Update.update_name, workflow_id, run_id, args, options);
    }

    /// Sends the update named `update_name` with the tuple `args` to a workflow, for a caller
    /// with no `workflow.UpdateDef`: a workflow's handler for that name, or its dynamic
    /// update handler, runs it. The handle's `get` decodes the result as `Result` (`void` to
    /// ignore it). Options, wait stages and errors are `updateWorkflow`'s.
    pub fn updateWorkflowByName(c: *Client, comptime Result: type, update_name: []const u8, workflow_id: []const u8, run_id: []const u8, args: anytype, options: UpdateOptions) UpdateError!WorkflowUpdateHandle(Result) {
        var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
        defer arena_state.deinit();
        const a = arena_state.allocator();
        const request = try c.updateRequest(a, update_name, workflow_id, run_id, args, options);
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
        return c.updateWithStartWorkflowByName(Update.Result, Workflow.workflow_name, start_options, args, Update.update_name, update_args, update_options);
    }

    /// `updateWithStartWorkflow` with the workflow type and the update named at run time:
    /// `args` and `update_args` are tuples of their arguments, and the handle's `get`
    /// decodes the update's result as `Result` (`void` to ignore it). Pass a definition's
    /// `workflow_name` to name only the update at run time. Otherwise behaves as
    /// `updateWithStartWorkflow`.
    pub fn updateWithStartWorkflowByName(
        c: *Client,
        comptime Result: type,
        workflow_type: []const u8,
        start_options: StartWorkflowOptions,
        args: anytype,
        update_name: []const u8,
        update_args: anytype,
        update_options: UpdateOptions,
    ) UpdateWithStartError!WorkflowUpdateHandle(Result) {
        if (start_options.id_conflict_policy == .unspecified) {
            log.warn("update-with-start of update \"{s}\" on workflow type \"{s}\": StartWorkflowOptions.id_conflict_policy must be set (for example .use_existing), the server requires it for update-with-start", .{ update_name, workflow_type });
            return error.InvalidOptions;
        }
        var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
        defer arena_state.deinit();
        const a = arena_state.allocator();
        const id = if (start_options.id.len > 0) start_options.id else try uuid(a, c.io);

        var update = try c.updateRequest(a, update_name, id, "", update_args, update_options);
        update.request.?.input.?.header = try protoHeader(a, try c.outgoingHeader(a, update_options.context));
        var start = try c.startRequest(a, workflow_type, id, start_options, args);
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
                return multiOperationError(s, a, id, update_name);
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
        var line: Io.Writer.Allocating = .init(arena);
        const err = multiOperationFailure(&line.writer, s, arena, workflow_id, update_name);
        if (err != error.OutOfMemory) log.err("{s}", .{line.written()});
        return err;
    }

    /// The error for a failed ExecuteMultiOperation, after writing its log line to `w`: the
    /// line names the operation that failed when the status's MultiOperationExecutionFailure
    /// detail says which, and has the call's own status otherwise. A failed write is
    /// `error.OutOfMemory`.
    fn multiOperationFailure(w: *Io.Writer, s: RpcStatus, arena: Allocator, workflow_id: []const u8, update_name: []const u8) UpdateWithStartError {
        const detail_name = "temporal.api.errordetails.v1.MultiOperationExecutionFailure";
        w.print("update-with-start of update \"{s}\" on workflow {s} failed: ", .{ update_name, workflow_id }) catch return error.OutOfMemory;
        var reader: Io.Reader = .fixed(s.details);
        const decoded = proto.google.rpc.Status.decode(&reader, arena) catch proto.google.rpc.Status{};
        for (decoded.details.items) |any| {
            if (!std.mem.endsWith(u8, any.type_url, "/" ++ detail_name)) continue;
            var detail_reader: Io.Reader = .fixed(any.value);
            const detail = proto.temporal.api.errordetails.v1.MultiOperationExecutionFailure.decode(&detail_reader, arena) catch break;
            var named = false;
            for (detail.statuses.items, 0..) |op, i| {
                // The server marks the operations it didn't run because another failed as
                // ABORTED; only the failing one says why.
                if (op.code == 0 or op.code == 10) continue;
                const what = if (i == 0) "starting the workflow" else "the update";
                w.print("{s}{s} failed: {s} (gRPC status {d})", .{ if (named) "; " else "", what, op.message, op.code }) catch return error.OutOfMemory;
                named = true;
            }
            if (named) return error.RpcFailed;
            break;
        }
        w.print("{s} (gRPC status {d})", .{ s.message, s.code }) catch return error.OutOfMemory;
        return error.RpcFailed;
    }

    fn updateRequest(c: *Client, a: Allocator, name: []const u8, workflow_id: []const u8, run_id: []const u8, args: anytype, options: UpdateOptions) error{OutOfMemory}!wfs.UpdateWorkflowExecutionRequest {
        return .{
            .namespace = c.namespace,
            .workflow_execution = .{ .workflow_id = workflow_id, .run_id = run_id },
            .wait_policy = .{ .lifecycle_stage = options.wait_for_stage.toProto() },
            .request = .{
                .meta = .{ .update_id = if (options.update_id.len > 0) options.update_id else try uuid(a, c.io), .identity = c.identity },
                .input = .{ .name = name, .args = .{ .payloads = try converter.toPayloadsWithContext(a, &c.workflowContext(workflow_id, options.context), args) } },
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
    fn requestIdentity(comptime Request: type, r: *const Request, context: *converter.Context) void {
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

    /// The bridge's options for one call: `timeout_ms`, and the metadata of a view from
    /// `withRpcOptions`, which sdk-core sends in place of the connection's headers for the
    /// same key. The view's `retry`, when set, replaces `retry`.
    fn callOptions(c: *const Client, retry: bool, timeout_ms: u32) bridge.Connection.CallOptions {
        const o = c.rpc_options orelse return .{ .retry = retry, .timeout_ms = timeout_ms };
        return .{ .retry = o.retry orelse retry, .timeout_ms = timeout_ms, .metadata = o.metadata, .binary_metadata = o.binary_metadata };
    }

    /// Whether the core retries a failed call. Not a reset: an attempt the server applied
    /// before the transport failed would be applied again, and the server doesn't
    /// deduplicate resets.
    fn callRetries(comptime method: []const u8) bool {
        return !std.mem.eql(u8, method, "ResetWorkflowExecution");
    }

    /// Sends the request to the server: the end of the interceptor chain. When `status` is
    /// given, a failure other than NOT_FOUND is stored there (allocated from `arena`) for the
    /// caller to report instead of being logged; ALREADY_EXISTS still returns
    /// `error.AlreadyExists`. The codecs encode the
    /// request and decode the response with `values` and the workflow or activity the request
    /// names.
    fn send(c: *Client, comptime Response: type, comptime method: []const u8, request: anytype, arena: Allocator, status: ?*?RpcStatus, values: ?*const propagation.Values) RpcError!Response {
        var codec_context: converter.Context = .{ .values = values orelse &propagation.Values.empty, .namespace = c.namespace };
        requestIdentity(@TypeOf(request), &request, &codec_context);
        return c.sendInContext(Response, method, request, arena, status, &codec_context);
    }

    /// For `worker.Worker`: records a heartbeat of the activity attempt `task_token` with
    /// `details`, which the codecs encode under the worker's `context` for the activity, as
    /// its other heartbeats are, and returns the server's answer (whether the activity has
    /// been canceled, paused or reset). Like the worker's other heartbeats, it bypasses the
    /// client's interceptors. Memory comes from `arena`.
    pub fn recordWorkerHeartbeat(c: *Client, arena: Allocator, task_token: []const u8, details: []const converter.Payload, context: *const converter.Context) RpcError!wfs.RecordActivityTaskHeartbeatResponse {
        var request: wfs.RecordActivityTaskHeartbeatRequest = .{ .namespace = c.namespace, .task_token = task_token, .identity = c.identity };
        if (details.len > 0) {
            var payloads: proto.temporal.api.common.v1.Payloads = .{};
            try payloads.payloads.appendSlice(arena, details);
            request.details = payloads;
        }
        return c.sendInContext(wfs.RecordActivityTaskHeartbeatResponse, "RecordActivityTaskHeartbeat", request, arena, null, context);
    }

    fn sendInContext(c: *Client, comptime Response: type, comptime method: []const u8, request: anytype, arena: Allocator, status: ?*?RpcStatus, codec_context: *const converter.Context) RpcError!Response {
        var coded = request;
        if (c.encode_failure_attributes) try failure_mod.encodeAttributes(@TypeOf(coded), &coded, arena);
        converter.encodeMessage(@TypeOf(coded), &coded, c.codecs, arena, c.io, codec_context) catch |err| {
            if (err == error.CodecFailed) log.warn("{s} failed: encoding its payloads with the client's codecs failed", .{method});
            return err;
        };
        var encoded: Io.Writer.Allocating = .init(arena);
        coded.encode(&encoded.writer, arena) catch return error.OutOfMemory;
        var failure: ?bridge.Connection.RpcFailure = null;
        defer if (failure) |f| f.deinit(c.gpa);
        const call_options = c.callOptions(callRetries(method), callTimeoutMs(@TypeOf(coded), method, &coded, c.callDeadlineMs()));
        // A failed first connection was logged by `currentConnection`.
        const connection = c.currentConnection() catch |err| return switch (err) {
            error.OutOfMemory => error.OutOfMemory,
            error.Canceled => error.Canceled,
            else => error.RpcFailed,
        };
        const bytes = connection.call(c.gpa, c.io, .workflow, method, encoded.written(), call_options, &failure) catch |err| switch (err) {
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
        converter.decodeMessage(Response, &response, c.codecs, arena, c.io, codec_context) catch |err| {
            if (err == error.CodecFailed) log.warn("{s} failed: decoding the response's payloads with the client's codecs failed", .{method});
            return err;
        };
        try failure_mod.decodeAttributes(Response, &response, arena);
        return response;
    }
};

/// How `Client.startWorkflow` and `executeWorkflow` start a workflow.
/// A duration longer than protobuf can carry (about 10,000 years), such as `Duration.max`, is
/// sent as the longest one it can; a negative one is sent as it is.
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

/// The gRPC services of a connection, for `Client.callService`. `.test_service` is the
/// time-skipping test server's.
pub const Service = bridge.Connection.Service;

/// The generated declaration of `service` (such as
/// `temporal.api.workflowservice.v1.WorkflowService`), whose fields are its methods.
fn ServiceDecl(comptime service: Service) type {
    return switch (service) {
        .workflow => proto.temporal.api.workflowservice.v1.WorkflowService(anyopaque, error{}),
        .operator => proto.temporal.api.operatorservice.v1.OperatorService(anyopaque, error{}),
        .cloud => proto.temporal.api.cloud.cloudservice.v1.CloudService(anyopaque, error{}),
        .test_service => proto.temporal.api.testservice.v1.TestService(anyopaque, error{}),
        .health => proto.grpc.health.v1.Health(anyopaque, error{}),
    };
}

/// The signature of `method` on `service` as the generated declaration gives it: the
/// userdata, the request, and the response in an error union.
fn serviceMethod(comptime service: Service, comptime method: []const u8) std.builtin.Type.Fn {
    comptime {
        const Decl = ServiceDecl(service);
        const what = "the " ++ @tagName(service) ++ " service (" ++ Decl.package ++ "." ++ Decl.service_name ++ ")";
        if (!@hasField(Decl, method)) @compileError(what ++ " has no method \"" ++ method ++ "\"");
        const field = @typeInfo(@FieldType(Decl, method));
        if (field != .pointer or @typeInfo(field.pointer.child) != .@"fn")
            @compileError(what ++ " has no method \"" ++ method ++ "\"");
        const f = @typeInfo(field.pointer.child).@"fn";
        if (f.param_types.len != 2 or f.param_types[1] == null or f.return_type == null or @typeInfo(f.return_type.?) != .error_union)
            @compileError(what ++ " method \"" ++ method ++ "\" streams; Client.callService makes unary calls only");
        return f;
    }
}

/// The generated request type of `method` on `service`, such as
/// `ServiceRequest(.workflow, "DescribeNamespace")`, which is
/// `temporal.api.workflowservice.v1.DescribeNamespaceRequest`. A method `service` doesn't
/// have is a compile error naming both.
pub fn ServiceRequest(comptime service: Service, comptime method: []const u8) type {
    return serviceMethod(service, method).param_types[1].?;
}

/// The generated response type of `method` on `service`; see `ServiceRequest`.
pub fn ServiceResponse(comptime service: Service, comptime method: []const u8) type {
    return @typeInfo(serviceMethod(service, method).return_type.?).error_union.payload;
}

/// How `Client.callService` fails. A call the server refuses returns the error of its gRPC
/// status:
///
/// - CANCELLED (1): `RpcCanceled`, except the call's deadline (`RpcOptions.timeout` or
///   `Options.rpc_timeout`), which arrives as CANCELLED, with the message "Timeout expired"
///   when sdk-core ends the call and another when the server's reset of the stream comes
///   first, and returns `DeadlineExceeded`: CANCELLED is the deadline when its message is
///   "Timeout expired" or the call ran for its whole deadline;
/// - UNKNOWN (2): `Unknown`, as is any code outside 1 to 16;
/// - INVALID_ARGUMENT (3): `InvalidArgument`;
/// - DEADLINE_EXCEEDED (4): `DeadlineExceeded`;
/// - NOT_FOUND (5): `NotFound`;
/// - ALREADY_EXISTS (6): `AlreadyExists`;
/// - PERMISSION_DENIED (7): `PermissionDenied`;
/// - RESOURCE_EXHAUSTED (8): `ResourceExhausted`;
/// - FAILED_PRECONDITION (9): `FailedPrecondition`;
/// - ABORTED (10): `Aborted`;
/// - OUT_OF_RANGE (11): `OutOfRange`;
/// - UNIMPLEMENTED (12): `Unimplemented`;
/// - INTERNAL (13): `Internal`;
/// - UNAVAILABLE (14): `Unavailable`;
/// - DATA_LOSS (15): `DataLoss`;
/// - UNAUTHENTICATED (16): `Unauthenticated`.
pub const CallServiceError = error{
    OutOfMemory,
    /// The calling task was canceled; the call was canceled in sdk-core.
    Canceled,
    /// The call failed without a gRPC status: the client's first connection failed (see
    /// `Client.init`), the transport failed, or sdk-core doesn't dispatch the method.
    RpcFailed,
    /// The server's answer didn't decode as the method's response type.
    InvalidResponse,
    RpcCanceled,
    Unknown,
    InvalidArgument,
    DeadlineExceeded,
    NotFound,
    AlreadyExists,
    PermissionDenied,
    ResourceExhausted,
    FailedPrecondition,
    Aborted,
    OutOfRange,
    Unimplemented,
    Internal,
    Unavailable,
    DataLoss,
    Unauthenticated,
};

/// The `CallServiceError` of gRPC status `code`, whose message is `message`: see there.
fn statusError(code: u32, message: []const u8) CallServiceError {
    return switch (code) {
        0 => error.RpcFailed,
        1 => if (std.mem.indexOf(u8, message, "Timeout expired") != null) error.DeadlineExceeded else error.RpcCanceled,
        3 => error.InvalidArgument,
        4 => error.DeadlineExceeded,
        5 => error.NotFound,
        6 => error.AlreadyExists,
        7 => error.PermissionDenied,
        8 => error.ResourceExhausted,
        9 => error.FailedPrecondition,
        10 => error.Aborted,
        11 => error.OutOfRange,
        12 => error.Unimplemented,
        13 => error.Internal,
        14 => error.Unavailable,
        15 => error.DataLoss,
        16 => error.Unauthenticated,
        else => error.Unknown,
    };
}

/// Why a `Client.callService` call failed. The slices borrow the arena passed to the call.
pub const RpcFailure = struct {
    /// The gRPC status code, or null when the call failed without a gRPC status
    /// (`error.RpcFailed`).
    code: ?RpcCode,
    /// The server's message, or sdk-core's for a failure without a status.
    message: []const u8,
    /// Serialized google.rpc.Status, possibly empty.
    details: []const u8,

    /// The first detail of the status that is a `T`, a generated protobuf message type such
    /// as `temporal.proto.temporal.api.errordetails.v1.NamespaceNotFoundFailure`, decoded
    /// from `arena`; null when there is none or it doesn't decode. A detail is a `T` when its
    /// type URL ends in "/" and `converter.protoMessageType(T)`.
    pub fn detail(f: RpcFailure, comptime T: type, arena: Allocator) error{OutOfMemory}!?T {
        var reader: Io.Reader = .fixed(f.details);
        const status = proto.google.rpc.Status.decode(&reader, arena) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            else => return null,
        };
        for (status.details.items) |any| {
            if (!std.mem.endsWith(u8, any.type_url, comptime "/" ++ converter.protoMessageType(T))) continue;
            var detail_reader: Io.Reader = .fixed(any.value);
            return T.decode(&detail_reader, arena) catch |err| switch (err) {
                error.OutOfMemory => error.OutOfMemory,
                else => null,
            };
        }
        return null;
    }
};

/// A call's own deadline, retries and gRPC metadata, which replace or add to the client's
/// (`Options.rpc_timeout` and `Options.headers`) for the calls made with them; see
/// `Client.withRpcOptions`. The slices are borrowed.
pub const RpcOptions = struct {
    /// The deadline of each call, which replaces `Options.rpc_timeout`. Calls that long-poll
    /// the server (waiting for a result or an update's outcome) aren't limited by it. Null
    /// keeps the client's. Must be positive; under 1 ms counts as 1 ms.
    timeout: ?Duration = null,
    /// gRPC ASCII headers sent with each call. For a name in `Options.headers` too, this
    /// value is sent. Names are gRPC header names, which gRPC sends in lower case, not
    /// ending in "-bin"; values are visible ASCII, space and tab.
    metadata: []const bridge.bytes.MetadataEntry = &.{},
    /// gRPC binary headers sent with each call: names end in "-bin", and values are any
    /// bytes (gRPC sends them base64-encoded).
    binary_metadata: []const bridge.bytes.MetadataEntry = &.{},
    /// Whether sdk-core retries a call that fails with a retryable status, with the
    /// connection's retry policy (`Options.retry`). Null keeps each call's own choice: the
    /// client's operations retry (a workflow reset does not), and `Client.callService` makes
    /// one attempt. Set, it replaces that choice for every call made with these options:
    /// `true` makes a workflow reset retry too, which can reset the workflow twice when the
    /// transport fails after the server applied the first attempt.
    retry: ?bool = null,

    fn check(o: RpcOptions, rejection: *Rejection) error{InvalidOptions}!void {
        if (o.timeout) |t| if (t.nanoseconds <= 0)
            return rejection.reject("RpcOptions.timeout must be positive, not {f}; set it to null for the client's Options.rpc_timeout", .{t});
        for (o.metadata) |h| {
            var header: Rejection = .{};
            headerRejection(h, &header) catch return rejection.reject("RpcOptions.metadata: {s}", .{header.message});
        }
        for (o.binary_metadata) |h| {
            if (!std.ascii.endsWithIgnoreCase(h.key, "-bin") or !isHeaderName(h.key))
                return rejection.reject("RpcOptions.binary_metadata: \"{s}\" is not a valid binary header name (letters, digits and {s}, ending in -bin)", .{ h.key, header_name_symbols });
        }
    }
};

/// What `Client.startNexusWorkflow` attaches to the workflow it starts. Borrowed for the call.
pub const NexusWorkflowAttachments = struct {
    /// The Nexus start request's ID, sent as the start's request ID.
    request_id: []const u8,
    /// Where the server delivers the operation's completion; empty attaches no callback.
    callback_url: []const u8 = "",
    /// Headers the server sends with the completion, the operation token among them.
    callback_headers: []const NexusHeader = &.{},
    /// Links back to what scheduled the operation, recorded with the workflow's start.
    links: []const proto.temporal.api.common.v1.Link = &.{},
    /// When set, a start the server rejects with `error.RpcFailed` stores the call's gRPC
    /// status here (null for a failure without one, such as a transport error) and is not
    /// logged, so the caller can map and report it. Its message comes from the call's arena.
    rejection: ?*?RpcRejection = null,
};

/// The gRPC status of a call the server rejected: its code, and the server's message, which
/// says what it rejected.
pub const RpcRejection = struct {
    code: RpcCode,
    message: []const u8,
};

/// A gRPC status code (https://grpc.io/docs/guides/status-codes/), as a failed call reports
/// it.
pub const RpcCode = enum(u32) {
    ok = 0,
    cancelled = 1,
    unknown = 2,
    invalid_argument = 3,
    deadline_exceeded = 4,
    not_found = 5,
    already_exists = 6,
    permission_denied = 7,
    resource_exhausted = 8,
    failed_precondition = 9,
    aborted = 10,
    out_of_range = 11,
    unimplemented = 12,
    internal = 13,
    unavailable = 14,
    data_loss = 15,
    unauthenticated = 16,
    _,
};

/// A Nexus header: names are case-insensitive.
pub const NexusHeader = struct { name: []const u8, value: []const u8 };

/// The workflow `Client.startNexusWorkflow` started, or found already started by the same
/// request ID or under `.use_existing`.
pub const NexusWorkflowStarted = struct {
    run_id: []const u8,
    /// Links to the workflow's start event: the server's, or one naming the event by the
    /// request ID when the server sends none.
    link: proto.temporal.api.common.v1.Link,
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

pub const TerminateOptions = struct {
    /// Terminates only if the workflow's chain of runs started with the run of this ID; a
    /// chain that didn't is `error.NotFound`. The chain's first run stays the original one
    /// across resets and continue-as-new. Empty checks nothing.
    first_execution_run_id: []const u8 = "",
    /// Links to related executions or events, recorded in the terminated event.
    links: []const proto.temporal.api.common.v1.Link = &.{},
};

/// How `Client.cancelWorkflowWithOptions` requests cancellation.
pub const CancelOptions = struct {
    /// Requests cancellation only if the workflow's chain of runs started with the run of
    /// this ID; a chain that didn't is `error.NotFound`. The chain's first run stays the
    /// original one across resets and continue-as-new. Empty checks nothing.
    first_execution_run_id: []const u8 = "",
    /// Links to related executions or events, recorded in the cancel-requested event.
    links: []const proto.temporal.api.common.v1.Link = &.{},
};

pub const CancelError = error{
    OutOfMemory,
    Canceled,
    /// The server refused; logged with its reason.
    RpcFailed,
    /// No such workflow, the run has closed, or it isn't the chain `first_execution_run_id`
    /// names.
    NotFound,
    InvalidResponse,
    CodecFailed,
};

pub const TerminateError = error{
    /// A payload codec (`Options.codecs`) failed encoding the details; logged.
    CodecFailed,
    OutOfMemory,
    Canceled,
    /// The server refused; logged with its reason.
    RpcFailed,
    /// No such workflow, the run has closed, or it isn't the chain `first_execution_run_id`
    /// names. Also when an automatic retry of the call finds that an earlier attempt
    /// terminated the run.
    NotFound,
    InvalidResponse,
};

pub const ResetOptions = struct {
    /// The event the new run's history ends at: the ID of a workflow task's completed,
    /// failed, or timed-out event in the base run's history (see `Client.fetchHistory`);
    /// the server also accepts the task's started event. The new run goes on from the end
    /// of that workflow task. Required.
    workflow_task_finish_event_id: i64,
    /// Recorded in the new run's history and in the terminated event of the run the reset
    /// terminates, if one was running.
    reason: []const u8 = "",
    /// The request's ID, recorded by the server; empty generates a UUID. Don't rely on it to
    /// deduplicate: a repeated request may reset again.
    request_id: []const u8 = "",
    /// Which kinds of events recorded after the reset point are not reapplied to the new run.
    reapply_exclude: ResetReapplyExclude = .{},
};

/// Kinds of events, recorded in the base run after the reset point, that a reset doesn't
/// reapply to the new run. By default it reapplies all of them.
pub const ResetReapplyExclude = struct {
    signals: bool = false,
    updates: bool = false,
    /// Nexus operation events.
    nexus: bool = false,
};

pub const ResetError = error{
    /// `ResetOptions.workflow_task_finish_event_id` isn't positive; logged, and nothing was
    /// sent.
    InvalidOptions,
    /// A payload codec failed encoding the request (logged), or an interceptor returned it.
    CodecFailed,
    OutOfMemory,
    Canceled,
    /// The server refused, such as an event ID that isn't at a workflow task's boundary or
    /// is beyond the history; logged as a warning with its reason. Also a transport failure,
    /// which isn't retried: the reset may have happened (see `Client.resetWorkflow`).
    RpcFailed,
    /// No such workflow or run.
    NotFound,
    InvalidResponse,
};

/// The changes `Client.updateWorkflowExecutionOptions` makes. A null field leaves that
/// option as it is; at least one must be set.
pub const WorkflowExecutionOptionsChanges = struct {
    versioning_override: ?VersioningOverrideChange = null,
};

pub const VersioningOverrideChange = union(enum) {
    /// Replaces the workflow's versioning override, if any, with this one. A pin needs a
    /// worker of that version to have polled the workflow's task queue; auto-upgrade needs
    /// none.
    set: deployment.VersioningOverride,
    /// Clears the override: the workflow's definition decides its behavior again.
    remove,
};

/// A workflow run's execution options, as `Client.updateWorkflowExecutionOptions` returns
/// them. Every slice borrows the arena passed to the call.
pub const WorkflowExecutionOptions = struct {
    /// Null when the run has no override, or one of a kind this SDK has no case for (read
    /// it from `raw`).
    versioning_override: ?deployment.VersioningOverride,
    /// The full server response, for anything not lifted into the fields above.
    raw: wfs.UpdateWorkflowExecutionOptionsResponse,
};

pub const UpdateExecutionOptionsError = error{
    /// The changes change nothing; logged, and nothing was sent.
    InvalidOptions,
    /// A payload codec failed encoding the request (logged), or an interceptor returned it.
    CodecFailed,
    OutOfMemory,
    Canceled,
    /// The server refused, such as a pin to a version whose workers haven't polled the
    /// workflow's task queue; logged as a warning with its reason.
    RpcFailed,
    /// No such workflow or run, or it has closed.
    NotFound,
    InvalidResponse,
};

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

/// The override as the server's requests carry it; borrows `o`'s slices.
fn versioningOverrideProto(o: deployment.VersioningOverride) proto.temporal.api.workflow.v1.VersioningOverride {
    return .{ .override = switch (o) {
        .pinned => |v| .{ .pinned = .{
            .behavior = .PINNED_OVERRIDE_BEHAVIOR_PINNED,
            .version = .{ .deployment_name = v.deployment_name, .build_id = v.build_id },
        } },
        .auto_upgrade => .{ .auto_upgrade = true },
    } };
}

/// Lifts the fields of `info` into a `WorkflowExecutionInfo` that borrows it.
fn executionInfo(arena: Allocator, info: proto.temporal.api.workflow.v1.WorkflowExecutionInfo) error{OutOfMemory}!WorkflowExecutionInfo {
    const execution = info.execution orelse proto.temporal.api.common.v1.WorkflowExecution{};
    return .{
        .workflow_id = execution.workflow_id,
        .run_id = execution.run_id,
        .workflow_type = if (info.type) |t| t.name else "",
        .task_queue = info.task_queue,
        .status = @fromBackingInt(@intCast(@backingInt(info.status))),
        .start_time = proto_time.timestampFromProto(info.start_time),
        .execution_time = proto_time.timestampFromProto(info.execution_time),
        .close_time = proto_time.timestampFromProto(info.close_time),
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
/// A duration longer than protobuf can carry (about 10,000 years), such as `Duration.max`, is
/// sent as the longest one it can; a negative one is sent as it is.
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
    /// How long the server waits before scheduling the first attempt. The activity counts
    /// as running meanwhile, but no worker gets it before the delay has passed. Null
    /// schedules it at once.
    start_delay: ?Duration = null,
    /// The activity's task priority and fairness. Null uses the server's defaults.
    priority: ?workflow.Priority = null,
    /// A single-line description of the activity for people reading it in the UI or CLI,
    /// stored as a JSON string payload (through the codecs); empty sends none. Read it back
    /// as `ActivityDescription.summary`.
    summary: []const u8 = "",
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
    /// `cancel`, `terminate`, `pause` or `unpause` of an activity that has already closed.
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
    /// The details of the latest heartbeat, decoded through the client's codecs; read each
    /// value with `heartbeat_details.get(T, arena, index)`. Empty before the first heartbeat
    /// with details.
    heartbeat_details: converter.EncodedValues,
    /// When the latest heartbeat was recorded; null before the first.
    last_heartbeat_time: ?Io.Timestamp,
    /// `StartActivityOptions.start_delay`; null without one, or for a zero delay.
    start_delay: ?Duration,
    /// `StartActivityOptions.priority`; null when every field is at its default (the
    /// server's defaults).
    priority: ?workflow.Priority,
    /// `StartActivityOptions.summary`; empty without one.
    summary: []const u8,
    /// The server's record, with the last attempt's failure, for anything not lifted into the
    /// fields above.
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
        /// From `Client.executeActivity`: true when the call started a new activity, false
        /// when `StartActivityOptions.id_conflict_policy = .use_existing` returned the one
        /// already running with the ID. Always false from `Client.getActivityHandle`.
        started: bool = false,
        /// The activity's type, from `Client.executeActivity`; `get` decodes the result and
        /// any failure with it in the converter context. Empty from
        /// `Client.getActivityHandle`, which doesn't know it.
        activity_type: []u8 = &.{},
        failure_: ?failure_mod.Failure = null,

        pub fn deinit(h: *Self) void {
            h.client.gpa.free(h.id);
            h.client.gpa.free(h.run_id);
            h.client.gpa.free(h.activity_type);
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
            const context = h.converterContext();
            switch (outcome.value orelse return error.InvalidResponse) {
                .result => |payloads| {
                    if (T == void) return;
                    if (payloads.payloads.items.len == 0) return error.InvalidPayload;
                    return converter.fromPayloadWithContext(T, arena, &context, payloads.payloads.items[0]);
                },
                .failure => |received| {
                    const f = try c.receivedFailure(arena, &context, received);
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

        /// The converter context `get` decodes the result and any failure with. Borrows the
        /// handle.
        fn converterContext(h: *const Self) converter.Context {
            return .{ .namespace = h.client.namespace, .activity_id = h.id, .activity_type = h.activity_type };
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
                .schedule_time = proto_time.timestampFromProto(info.schedule_time),
                .last_started_time = proto_time.timestampFromProto(info.last_started_time),
                .close_time = proto_time.timestampFromProto(info.close_time),
                .search_attributes = try .fromProto(arena, info.search_attributes),
                .heartbeat_details = .{
                    .payloads = if (info.heartbeat_details) |d| d.payloads.items else &.{},
                    .context = try Client.dupeContext(arena, .{ .namespace = c.namespace, .activity_id = info.activity_id, .activity_type = if (info.activity_type) |t| t.name else "" }),
                },
                .last_heartbeat_time = proto_time.timestampFromProto(info.last_heartbeat_time),
                .start_delay = if (proto_time.durationFromProto(info.start_delay)) |d| if (d.nanoseconds == 0) null else d else null,
                .priority = if (info.priority) |p| if (p.priority_key == 0 and p.fairness_key.len == 0 and p.fairness_weight == 0) null else .{ .priority_key = p.priority_key, .fairness_key = p.fairness_key, .fairness_weight = p.fairness_weight } else null,
                .summary = summary: {
                    const md = info.user_metadata orelse break :summary "";
                    const p = md.summary orelse break :summary "";
                    break :summary converter.fromPayload([]const u8, arena, p) catch |err| {
                        log.err("DescribeActivityExecution for activity {s} returned a summary that doesn't decode as a string: {t}", .{ h.id, err });
                        return error.InvalidResponse;
                    };
                },
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

        /// `controlError` for `pause` and `unpause`, whose FAILED_PRECONDITION means either a
        /// closed activity or one in the wrong state (already pausing, or not paused): the
        /// activity's status tells them apart.
        fn pauseError(h: *const Self, err: Client.RpcError, status: ?Client.RpcStatus, comptime what: []const u8) ActivityError {
            const s = status orelse return h.controlError(err, status, what);
            if (err != error.RpcFailed or s.code != 9) return h.controlError(err, status, what);
            var arena_state: std.heap.ArenaAllocator = .init(h.client.gpa);
            defer arena_state.deinit();
            const d = try h.describe(arena_state.allocator());
            switch (d.status) {
                .running, .paused, .unspecified, _ => {
                    log.warn(what ++ " activity {s} failed: {s} (gRPC status {d})", .{ h.id, s.message, s.code });
                    return error.RpcFailed;
                },
                .completed, .failed, .canceled, .terminated, .timed_out => return error.ActivityClosed,
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

        /// Pauses the activity: no new attempt is scheduled until `unpause`, and a running
        /// attempt learns of the pause from its next heartbeat as `std.Io` cancellation, with
        /// `ctx.cancelReason()` `.paused`; once it returns `error.Canceled`, the worker ends
        /// the attempt as a retryable `ActivityPause` failure. `describe` reports the status
        /// `.paused`. That failure counts against the retry policy: pausing a running attempt
        /// that is the last `maximum_attempts` allows fails the activity instead. A cancel
        /// requested before the paused attempt ends still ends the activity canceled.
        /// `reason` is recorded with the request.
        /// Returns `error.NotFound` for no such activity, `error.ActivityClosed` if it has
        /// already closed, and `error.RpcFailed` (logging the server's reason) for anything
        /// else the server refuses, such as an activity that is already paused or pausing.
        pub fn pause(h: *const Self, reason: []const u8) ActivityError!void {
            const c = h.client;
            var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
            defer arena_state.deinit();
            const a = arena_state.allocator();
            var status: ?Client.RpcStatus = null;
            _ = c.rpcWithStatus(wfs.PauseActivityExecutionResponse, "PauseActivityExecution", wfs.PauseActivityExecutionRequest{
                .namespace = c.namespace,
                .activity_id = h.id,
                .run_id = h.run_id,
                .identity = c.identity,
                .reason = reason,
                .resource_id = try std.fmt.allocPrint(a, "activity:{s}", .{h.id}),
                .request_id = try uuid(a, c.io),
            }, a, &status) catch |err| return h.pauseError(err, status, "pausing");
        }

        /// Resumes a paused activity: its next attempt is scheduled at once, or at a random
        /// time within `options.jitter`. Errors as for `pause`; unpausing an activity that
        /// isn't paused returns `error.RpcFailed`.
        pub fn unpause(h: *const Self, options: UnpauseActivityOptions) ActivityError!void {
            const c = h.client;
            var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
            defer arena_state.deinit();
            const a = arena_state.allocator();
            var status: ?Client.RpcStatus = null;
            _ = c.rpcWithStatus(wfs.UnpauseActivityExecutionResponse, "UnpauseActivityExecution", wfs.UnpauseActivityExecutionRequest{
                .namespace = c.namespace,
                .activity_id = h.id,
                .run_id = h.run_id,
                .identity = c.identity,
                .reason = options.reason,
                .jitter = proto_time.durationToProto(options.jitter),
                .resource_id = try std.fmt.allocPrint(a, "activity:{s}", .{h.id}),
                .request_id = try uuid(a, c.io),
            }, a, &status) catch |err| return h.pauseError(err, status, "unpausing");
        }

        /// Deletes the activity and its record; a running activity is terminated first. The
        /// server removes the record in a task of its own after it accepts the request, and
        /// `delete` waits for that: it returns once the server reports the deleted run as not
        /// found, so afterwards `describe` and `get` return `error.NotFound`. Visibility
        /// queries stop listing it once they catch up. A handle with no run ID deletes the
        /// latest run. Returns `error.NotFound` for no such activity, `error.Canceled` if
        /// canceled before the record is gone (the server may still delete it), and
        /// `error.RpcFailed` (logging the server's reason) for anything else the server
        /// refuses, or if checking that the record is gone fails.
        pub fn delete(h: *const Self) ActivityError!void {
            const c = h.client;
            var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
            defer arena_state.deinit();
            const a = arena_state.allocator();
            // The wait needs a fixed run: describing the latest run could see one started
            // under the same ID after the delete.
            const run_id = if (h.run_id.len > 0) h.run_id else run_id: {
                const response = c.rpc(wfs.DescribeActivityExecutionResponse, "DescribeActivityExecution", wfs.DescribeActivityExecutionRequest{
                    .namespace = c.namespace,
                    .activity_id = h.id,
                }, a) catch |err| switch (err) {
                    error.AlreadyExists => return error.InvalidResponse,
                    else => |e| return e,
                };
                const latest = if (response.run_id.len > 0) response.run_id else if (response.info) |info| info.run_id else "";
                if (latest.len == 0) {
                    log.err("DescribeActivityExecution for activity {s} returned no run ID, so deleting it cannot wait for its record to go", .{h.id});
                    return error.InvalidResponse;
                }
                break :run_id latest;
            };
            var status: ?Client.RpcStatus = null;
            _ = c.rpcWithStatus(wfs.DeleteActivityExecutionResponse, "DeleteActivityExecution", wfs.DeleteActivityExecutionRequest{
                .namespace = c.namespace,
                .activity_id = h.id,
                .run_id = run_id,
            }, a, &status) catch |err| return h.controlError(err, status, "deleting");
            try h.waitUntilDeleted(run_id);
        }

        /// Describes run `run_id` until the server reports it not found, with backoff.
        fn waitUntilDeleted(h: *const Self, run_id: []const u8) ActivityError!void {
            const c = h.client;
            var arena_state: std.heap.ArenaAllocator = .init(c.gpa);
            defer arena_state.deinit();
            // Doubling from 10 ms; the cap keeps a long wait under the namespace's rate limit.
            var delay: Io.Duration = .fromMilliseconds(10);
            const max_delay: Io.Duration = .fromMilliseconds(500);
            while (true) {
                _ = arena_state.reset(.retain_capacity);
                var status: ?Client.RpcStatus = null;
                _ = c.rpcWithStatus(wfs.DescribeActivityExecutionResponse, "DescribeActivityExecution", wfs.DescribeActivityExecutionRequest{
                    .namespace = c.namespace,
                    .activity_id = h.id,
                    .run_id = run_id,
                }, arena_state.allocator(), &status) catch |err| switch (err) {
                    error.NotFound => return,
                    error.AlreadyExists => return error.InvalidResponse,
                    error.RpcFailed => {
                        if (status) |s| {
                            log.err("the server accepted deleting activity {s} run {s}, but checking that its record is gone failed: {s} (gRPC status {d})", .{ h.id, run_id, s.message, s.code });
                        } else {
                            log.err("the server accepted deleting activity {s} run {s}, but checking that its record is gone failed", .{ h.id, run_id });
                        }
                        return error.RpcFailed;
                    },
                    else => |e| return e,
                };
                try c.io.sleep(delay, .awake);
                delay = .fromNanoseconds(@min(delay.nanoseconds * 2, max_delay.nanoseconds));
            }
        }
    };
}

/// How `ActivityHandle.unpause` resumes an activity.
pub const UnpauseActivityOptions = struct {
    /// Recorded with the request.
    reason: []const u8 = "",
    /// Schedules the next attempt at a random time within this long, to spread out many
    /// activities unpaused at once. Null schedules it at once. A duration longer than
    /// protobuf can carry (about 10,000 years), such as `Duration.max`, is sent as the longest
    /// one it can; a negative one is sent as it is.
    jitter: ?Duration = null,
};

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
            .schedule_time = proto_time.timestampFromProto(raw.schedule_time),
            .close_time = proto_time.timestampFromProto(raw.close_time),
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
                        return converter.fromPayloadWithContext(T, arena, &c.workflowContext(run.id, null), payloads.payloads.items[0]);
                    },
                    .workflow_execution_failed_event_attributes => |failed| {
                        run.failure_ = if (failed.failure) |f| try c.receivedFailure(arena, &c.workflowContext(run.id, null), f) else null;
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
                    return converter.fromPayloadWithContext(T, arena, &c.workflowContext(h.workflow_id, null), payloads.payloads.items[0]);
                },
                .failure => |f| {
                    h.failure_ = try c.receivedFailure(arena, &c.workflowContext(h.workflow_id, null), f);
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
/// only. Logs why as a warning.
fn validateHeader(h: bridge.bytes.MetadataEntry) error{InvalidOptions}!void {
    var rejection: Rejection = .{};
    headerRejection(h, &rejection) catch |err| {
        log.warn("updating the client's headers failed: {s}", .{rejection.message});
        return err;
    };
}

/// `validateHeader` without the log: fills `rejection` instead.
fn headerRejection(h: bridge.bytes.MetadataEntry, rejection: *Rejection) error{InvalidOptions}!void {
    if (std.ascii.endsWithIgnoreCase(h.key, "-bin") or !isHeaderName(h.key))
        return rejection.reject("\"{s}\" is not a valid header name (letters, digits and {s}, not ending in -bin)", .{ h.key, header_name_symbols });
    for (h.value) |ch| if (ch != '\t' and (ch < ' ' or ch > '~'))
        return rejection.reject("the value of header {s} has a byte (0x{x:0>2}) other than visible ASCII, space or tab", .{ h.key, ch });
}

/// The symbols an HTTP/2 header name may hold besides letters and digits.
const header_name_symbols = "!#$%&'*+-.^_`|~";

fn isHeaderName(key: []const u8) bool {
    if (key.len == 0) return false;
    for (key) |ch| if (!std.ascii.isAlphanumeric(ch) and std.mem.indexOfScalar(u8, header_name_symbols, ch) == null) return false;
    return true;
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
    var context: converter.Context = .{};
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
    var r: Rejection = .{};
    try testing.expectEqualStrings("PEM BYTES", try tlsFile(arena.allocator(), testing.io, "", path, "server_root_ca_cert", &r));
    try testing.expectEqualStrings("inline", try tlsFile(arena.allocator(), testing.io, "inline", "", "server_root_ca_cert", &r));
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
        try testing.expectEqualStrings(@import("build_options").version, b.client_version);
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

test "a reset is sent once; other calls are retried" {
    try testing.expect(!Client.callRetries("ResetWorkflowExecution"));
    inline for ([_][]const u8{ "StartWorkflowExecution", "TerminateWorkflowExecution", "UpdateWorkflowExecutionOptions", "SignalWorkflowExecution" }) |method| {
        try testing.expect(Client.callRetries(method));
    }
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

test "retry policy intervals and the OpenTelemetry periodicity are clamped, not narrowed" {
    // Retry intervals: clamped to the longest protobuf Duration; a negative one keeps one sign.
    {
        var arena: std.heap.ArenaAllocator = .init(testing.allocator);
        defer arena.deinit();
        const longest: proto.google.protobuf.Duration = .{ .seconds = 315_576_000_000, .nanos = 999_999_999 };
        const p = try Client.protoRetryPolicy(arena.allocator(), .{ .initial_interval = .max, .maximum_interval = .max });
        try testing.expectEqual(longest, p.initial_interval.?);
        try testing.expectEqual(longest, p.maximum_interval.?);
        const negative = try Client.protoRetryPolicy(arena.allocator(), .{ .initial_interval = .fromMilliseconds(-1500) });
        try testing.expectEqual(proto.google.protobuf.Duration{ .seconds = -1, .nanos = -500_000_000 }, negative.initial_interval.?);
    }
    // The OpenTelemetry periodicity: clamped to 1 ms through maxInt(u32) ms.
    {
        const cases = [_]struct { Duration, u32 }{
            .{ .max, std.math.maxInt(u32) },
            .{ .{ .nanoseconds = std.math.minInt(i96) }, 1 },
            .{ .fromSeconds(-1), 1 },
            .{ .zero, 1 },
            .{ .fromMicroseconds(1500), 1 },
            .{ .fromSeconds(30), 30_000 },
            .{ .fromMilliseconds(std.math.maxInt(u32)), std.math.maxInt(u32) },
            .{ .fromMilliseconds(@as(i64, std.math.maxInt(u32)) + 1), std.math.maxInt(u32) },
        };
        for (cases) |case| {
            const m = try runtimeMetrics(testing.allocator, .{ .exporter = .{ .opentelemetry = .{ .url = "http://localhost:4317", .periodicity = case[0] } } });
            try testing.expectEqual(case[1], m.exporter.opentelemetry.metric_periodicity_ms);
        }
    }
}

test "the server accepts a workflow started with Duration.max execution and run timeouts" {
    var server = try @import("testing.zig").DevServer.start(testing.allocator, testing.io, .{});
    defer server.stop(testing.io);
    var c = try Client.connect(testing.allocator, testing.io, server.clientOptions());
    defer c.close();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // No worker polls this task queue; the start alone shows the server takes the values.
    var run = try c.executeWorkflowByName(void, "Unpolled", .{
        .task_queue = try uuid(a, testing.io),
        .execution_timeout = .max,
        .run_timeout = .max,
        .task_timeout = .fromSeconds(10),
    }, .{});
    defer run.deinit();
    const d = try c.describeWorkflow(a, run.id, run.run_id);
    try testing.expectEqual(WorkflowExecutionStatus.running, d.status);
    try c.terminateWorkflow(run.id, run.run_id, "test done");
}

test "RpcOptions are rejected for a timeout that isn't positive and metadata gRPC can't send" {
    var r: Rejection = .{};
    try expectRejected(RpcOptions.check(.{ .timeout = .zero }, &r), &r, "RpcOptions.timeout");
    try expectRejected(RpcOptions.check(.{ .timeout = .fromSeconds(-1) }, &r), &r, "RpcOptions.timeout");
    try expectRejected(RpcOptions.check(.{ .metadata = &.{.{ .key = "bad key", .value = "v" }} }, &r), &r, "RpcOptions.metadata");
    try expectRejected(RpcOptions.check(.{ .metadata = &.{.{ .key = "x-ok", .value = "a\nb" }} }, &r), &r, "RpcOptions.metadata");
    try expectRejected(RpcOptions.check(.{ .metadata = &.{.{ .key = "x-trace-bin", .value = "v" }} }, &r), &r, "x-trace-bin");
    try expectRejected(RpcOptions.check(.{ .binary_metadata = &.{.{ .key = "x-trace", .value = "v" }} }, &r), &r, "RpcOptions.binary_metadata");
    try expectRejected(RpcOptions.check(.{ .binary_metadata = &.{.{ .key = "bad key-bin", .value = "v" }} }, &r), &r, "bad key-bin");
    // Binary values may hold any bytes, and a timeout under 1 ms counts as 1 ms.
    try RpcOptions.check(.{ .timeout = .fromNanoseconds(1), .binary_metadata = &.{.{ .key = "x-trace-bin", .value = "\n\x00" }} }, &r);
    var c = try Client.init(testing.allocator, testing.io, .{ .host_port = closed_port, .core_log_level = null, .rpc_timeout = .fromSeconds(9) });
    defer c.close();
    try testing.expectEqual(@as(u32, 9000), c.callDeadlineMs());
    var view = try c.withRpcOptions(.{ .timeout = .fromNanoseconds(1) });
    try testing.expectEqual(@as(u32, 1), view.callDeadlineMs());
    // Closing a view leaves the client untouched.
    view.close();
    try testing.expectEqual(@as(u32, 1), c.shared.refs.load(.monotonic));
    try testing.expectEqual(@as(u32, 9000), (try c.withRpcOptions(.{})).callDeadlineMs());
}

const RpcOptionsFixtures = struct {
    const Status = workflow.QueryDef("status", fn () []const u8);
    const Answers = struct {
        pub const queries = .{ .status = Status };
        pub fn run(_: *Answers, _: *workflow.Context) !void {}
        pub fn status(_: *Answers) []const u8 {
            return "ready";
        }
    };
    const AnswersWorkflow = workflow.Def("rpc_options_answers", Answers);

    fn query(c: *Client, id: []const u8) ![]const u8 {
        var arena: std.heap.ArenaAllocator = .init(testing.allocator);
        defer arena.deinit();
        const answer = try c.queryWorkflow(Status, arena.allocator(), id, "", .{});
        if (!std.mem.eql(u8, answer, "ready")) return error.TestUnexpectedResult;
        return "ready";
    }

    fn runWorker(w: *@import("worker.zig").Worker) @import("worker.zig").Worker.RunError!void {
        return w.run();
    }
};

test "a per-call timeout cuts short a query the client's own deadline lets finish" {
    const Worker = @import("worker.zig").Worker;
    var server = try @import("testing.zig").DevServer.start(testing.allocator, testing.io, .{});
    defer server.stop(testing.io);
    var options = server.clientOptions();
    options.rpc_timeout = .fromSeconds(60);
    var c = try Client.connect(testing.allocator, testing.io, options);
    defer c.close();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // No worker polls the task queue yet, so the server holds queries until one does.
    const id = try uuid(a, testing.io);
    const task_queue = try uuid(a, testing.io);
    var run = try c.executeWorkflow(RpcOptionsFixtures.AnswersWorkflow, .{ .id = id, .task_queue = task_queue }, .{});
    defer run.deinit();
    // The same query through a view with a short timeout fails with the deadline error. A
    // typed call would log it as an error, which fails the test, so this sends the query
    // the way the typed calls do and reads the status.
    var view = try c.withRpcOptions(.{ .timeout = .fromMilliseconds(500) });
    const start = Io.Timestamp.now(testing.io, .awake);
    var status: ?Client.RpcStatus = null;
    try testing.expectError(error.RpcFailed, view.rpcWithStatus(wfs.QueryWorkflowResponse, "QueryWorkflow", wfs.QueryWorkflowRequest{
        .namespace = view.namespace,
        .execution = .{ .workflow_id = id },
        .query = .{ .query_type = RpcOptionsFixtures.Status.query_name },
    }, a, &status));
    const waited = start.durationTo(Io.Timestamp.now(testing.io, .awake));
    // sdk-core reports its own deadline as CANCELLED; the server's would be DEADLINE_EXCEEDED.
    const s = status.?;
    if (!(s.code == 1 and std.mem.indexOf(u8, s.message, "Timeout expired") != null) and s.code != 4) {
        std.debug.print("want a timeout, got gRPC status {d}: {s}\n", .{ s.code, s.message });
        return error.TestUnexpectedResult;
    }
    try testing.expect(waited.nanoseconds < 10 * std.time.ns_per_s);

    // The same query without the per-call timeout waits, under the client's 60 s, until a
    // worker answers it. (The server buffers one query per workflow before its first task.)
    var without = try testing.io.concurrent(RpcOptionsFixtures.query, .{ &c, id });
    defer _ = without.cancel(testing.io) catch {};
    var w = try Worker.init(testing.allocator, testing.io, &c, task_queue, .{});
    defer w.deinit();
    try w.registerWorkflow(RpcOptionsFixtures.AnswersWorkflow);
    var running = try testing.io.concurrent(RpcOptionsFixtures.runWorker, .{&w});
    defer {
        w.stop();
        running.await(testing.io) catch {};
    }
    try testing.expectEqualStrings("ready", try without.await(testing.io));
}

// Port 1 on the loopback address: nothing listens there, so connecting fails at once.
const closed_port = "127.0.0.1:1";

test "a client from init makes no connection, and a failed first connection leaves it to try again" {
    // The failed connections and the rejected header are logged as warnings.
    testing.log_level = .err;
    var c = try Client.init(testing.allocator, testing.io, .{ .host_port = closed_port, .core_log_level = null });
    defer c.close();
    try testing.expect(c.shared.connection == null);

    try testing.expectError(error.ConnectFailed, c.ensureConnected());
    try testing.expect(c.shared.connection == null);
    try testing.expectError(error.ConnectFailed, c.currentConnection());
    // The client's own calls return RpcFailed for a connection that fails.
    try testing.expectError(error.RpcFailed, c.checkHealth());
    try testing.expectError(error.RpcFailed, c.countWorkflows(""));
    try testing.expectError(error.ConnectFailed, c.ensureConnected());
    try testing.expect(c.shared.connection == null);
    try testing.expectEqualStrings(closed_port, c.shared.pending_connection.?.options.host_port);
}

test "a client that hasn't connected keeps an updated API key and headers for its connection" {
    // The failed connections and the rejected header are logged as warnings.
    testing.log_level = .err;
    var key: [6]u8 = "secret".*;
    var header_value: [4]u8 = "blue".*;
    var c = try Client.init(testing.allocator, testing.io, .{
        .host_port = closed_port,
        .core_log_level = null,
        .api_key = "first",
        .headers = &.{.{ .key = "x-deployment", .value = "green" }},
        .tls = .{ .server_name = "temporal.example" },
        .http_connect_proxy = .{ .host_port = "proxy:3128", .username = "svc", .password = "pw" },
    });
    defer c.close();

    // The client keeps its own copies: the caller's buffers may change after the update.
    try c.updateApiKey(&key);
    key = "XXXXXX".*;
    try c.updateHeaders(&.{.{ .key = "x-deployment", .value = &header_value }});
    header_value = "XXXX".*;
    try testing.expectError(error.InvalidOptions, c.updateHeaders(&.{.{ .key = "bad key", .value = "v" }}));

    const o = c.shared.pending_connection.?.options;
    try testing.expectEqualStrings("secret", o.api_key);
    try testing.expectEqual(@as(usize, 1), o.headers.len);
    try testing.expectEqualStrings("x-deployment", o.headers[0].key);
    try testing.expectEqualStrings("blue", o.headers[0].value);
    // The other options are kept as they were.
    try testing.expectEqualStrings(closed_port, o.host_port);
    try testing.expectEqualStrings("temporal.example", o.tls.?.server_name);
    try testing.expectEqualStrings("svc", o.http_connect_proxy.?.username);
    try testing.expectEqualStrings(bridge.Connection.default_client_name, o.client_name);

    try c.updateApiKey("");
    try c.updateHeaders(&.{});
    try testing.expectEqualStrings("", c.shared.pending_connection.?.options.api_key);
    try testing.expectEqual(@as(usize, 0), c.shared.pending_connection.?.options.headers.len);
    try testing.expect(c.shared.connection == null);
}

test "a failed reconnect of a client that hasn't connected keeps its own options" {
    // The failed connections and the rejected header are logged as warnings.
    testing.log_level = .err;
    var c = try Client.init(testing.allocator, testing.io, .{ .host_port = closed_port, .core_log_level = null, .api_key = "mine" });
    defer c.close();
    try testing.expectError(error.ConnectFailed, c.reconnect(.{ .host_port = "127.0.0.1:2", .api_key = "theirs" }));
    try testing.expect(c.shared.connection == null);
    try testing.expectEqualStrings(closed_port, c.shared.pending_connection.?.options.host_port);
    try testing.expectEqualStrings("mine", c.shared.pending_connection.?.options.api_key);
    try testing.expectEqual(@as(usize, 0), c.shared.retired_connections.items.len);
}

test "a client from init is closed without ever connecting" {
    var c = try Client.init(testing.allocator, testing.io, .{
        .host_port = closed_port,
        .identity = "lazy",
        .headers = &.{.{ .key = "x-a", .value = "1" }},
    });
    try c.updateApiKey("k");
    c.close();
}

test "creating a client rejects the options connect rejects" {
    var r: Rejection = .{};
    try checkConnectionOptions(.{}, &r);
    try checkConnectionOptions(.{ .tls = .{ .server_root_ca_cert_path = "/does/not/exist.pem" } }, &r);
    try expectRejected(checkConnectionOptions(.{ .tls = .{ .client_cert = "PEM", .client_cert_path = "cert.pem" } }, &r), &r, "Options.tls.client_cert");
    try expectRejected(checkConnectionOptions(.{ .tls = .{ .server_root_ca_cert = "PEM", .server_root_ca_cert_path = "ca.pem" } }, &r), &r, "Options.tls.server_root_ca_cert");
    try expectRejected(checkConnectionOptions(.{ .tls = .{ .client_private_key = "PEM", .client_private_key_path = "key.pem" } }, &r), &r, "Options.tls.client_private_key");
    try expectRejected(checkConnectionOptions(.{ .headers = &.{.{ .key = "bad key", .value = "v" }} }, &r), &r, "header name");
    try expectRejected(checkConnectionOptions(.{ .headers = &.{.{ .key = "k", .value = "a\nb" }} }, &r), &r, "value of header k");
    try expectRejected(checkConnectionOptions(.{ .keep_alive = .{ .interval = .zero } }, &r), &r, "keep_alive.interval");
    try expectRejected(checkConnectionOptions(.{ .retry = .{ .max_interval = .fromSeconds(-1) } }, &r), &r, "retry.max_interval");
    try expectRejected(checkConnectionOptions(.{ .http_connect_proxy = .{ .host_port = "" } }, &r), &r, "http_connect_proxy.host_port");
    try expectRejected(checkConnectionOptions(.{ .client_version = "" }, &r), &r, "client_version");
}

test "a client from init connects on its first call to a dev server" {
    var server = try @import("testing.zig").DevServer.start(testing.allocator, testing.io, .{});
    defer server.stop(testing.io);
    var c = try Client.init(testing.allocator, testing.io, server.clientOptions());
    defer c.close();
    try testing.expect(c.shared.connection == null);
    _ = try c.countWorkflows("");
    try testing.expect(c.shared.connection != null);
    try testing.expect(c.shared.pending_connection == null);
    try c.checkHealth();
    try c.ensureConnected();
    // Once connected, the updates go to the connection.
    try c.updateApiKey("");
    try c.updateHeaders(&.{.{ .key = "x-deployment", .value = "blue" }});
    _ = try c.countWorkflows("");
}

test "reconnect makes the first connection of a client from init" {
    var server = try @import("testing.zig").DevServer.start(testing.allocator, testing.io, .{});
    defer server.stop(testing.io);
    var options = server.clientOptions();
    options.host_port = closed_port;
    options.core_log_level = null;
    var c = try Client.init(testing.allocator, testing.io, options);
    defer c.close();
    try c.reconnect(server.clientOptions().connectionOptions());
    try testing.expect(c.shared.connection != null);
    try testing.expectEqual(@as(usize, 0), c.shared.retired_connections.items.len);
    try c.checkHealth();
}

const LazyConnectTest = struct {
    fn firstCall(c: *Client, out: *?bridge.Connection, failed: *std.atomic.Value(bool)) void {
        out.* = c.currentConnection() catch {
            failed.store(true, .release);
            return;
        };
    }
};

test "first calls made together from several threads open one connection" {
    var server = try @import("testing.zig").DevServer.start(testing.allocator, testing.io, .{});
    defer server.stop(testing.io);
    var c = try Client.init(testing.allocator, testing.io, server.clientOptions());
    defer c.close();

    var connections: [8]?bridge.Connection = @splat(null);
    var failed: std.atomic.Value(bool) = .init(false);
    var threads: [connections.len]std.Thread = undefined;
    for (&threads, &connections) |*t, *out| t.* = try std.Thread.spawn(.{}, LazyConnectTest.firstCall, .{ &c, out, &failed });
    for (threads) |t| t.join();
    try testing.expect(!failed.load(.acquire));
    for (connections) |conn| try testing.expectEqual(c.shared.connection.?.ptr, conn.?.ptr);
    try testing.expectEqual(@as(usize, 0), c.shared.retired_connections.items.len);
}

test "the first call of a client from init gives up within rpc_timeout when the server never answers" {
    // The failed connection is logged as a warning.
    testing.log_level = .err;
    // A listener that never accepts: the kernel completes the TCP handshake, and nothing
    // ever answers, as with a load balancer whose backend isn't up yet.
    const any: Io.net.IpAddress = try .parseIp4("127.0.0.1", 0);
    var server = try any.listen(testing.io, .{});
    defer server.deinit(testing.io);
    var buf: [32]u8 = undefined;
    const host_port = try std.fmt.bufPrint(&buf, "127.0.0.1:{d}", .{server.socket.address.getPort()});

    var c = try Client.init(testing.allocator, testing.io, .{ .host_port = host_port, .core_log_level = null, .rpc_timeout = .fromSeconds(1) });
    defer c.close();
    const start = Io.Clock.awake.now(testing.io);
    try testing.expectError(error.RpcFailed, c.checkHealth());
    const waited = start.durationTo(Io.Clock.awake.now(testing.io));
    // `Options.rpc_timeout` is the deadline of `checkHealth`, the first call included.
    try testing.expect(waited.nanoseconds < 10 * std.time.ns_per_s);
}

const SilentServerTest = struct {
    fn firstCall(c: *Client, done: *std.atomic.Value(bool)) void {
        _ = c.checkHealth() catch {};
        done.store(true, .release);
    }
};

test "updates to a client from init don't wait for a first connection in progress, and apply to it" {
    // The failed connection is logged as a warning.
    testing.log_level = .err;
    // A listener that never accepts: connecting waits until the client gives up.
    const any: Io.net.IpAddress = try .parseIp4("127.0.0.1", 0);
    var server = try any.listen(testing.io, .{});
    var buf: [32]u8 = undefined;
    const host_port = try std.fmt.bufPrint(&buf, "127.0.0.1:{d}", .{server.socket.address.getPort()});

    var c = try Client.init(testing.allocator, testing.io, .{ .host_port = host_port, .core_log_level = null, .rpc_timeout = .fromSeconds(3) });
    defer c.close();
    // Before `close`, which waits for the core's attempt: this ends it at once.
    defer server.deinit(testing.io);
    var done: std.atomic.Value(bool) = .init(false);
    const thread = try std.Thread.spawn(.{}, SilentServerTest.firstCall, .{ &c, &done });
    defer thread.join();
    // Wait until the first call holds the connect lock.
    while (c.shared.connect_mutex.tryLock()) c.shared.connect_mutex.unlock(testing.io);

    try c.updateApiKey("rotated");
    try c.updateHeaders(&.{.{ .key = "x-tenant", .value = "blue" }});
    // Both returned while the first call was still connecting.
    try testing.expect(!done.load(.acquire));
    try testing.expectEqual(@as(u64, 2), c.shared.pending_updates);
    try testing.expectEqualStrings("rotated", c.shared.pending_connection.?.options.api_key);
}

test "reconnect rejects the headers connect rejects" {
    // The rejections are logged as warnings.
    testing.log_level = .err;
    var c = try Client.init(testing.allocator, testing.io, .{ .host_port = closed_port, .core_log_level = null });
    defer c.close();
    try testing.expectError(error.InvalidOptions, c.reconnect(.{ .host_port = closed_port, .headers = &.{.{ .key = "bad key", .value = "v" }} }));
    try testing.expectError(error.InvalidOptions, c.reconnect(.{ .host_port = closed_port, .headers = &.{.{ .key = "k", .value = "caf\xc3\xa9" }} }));
    try testing.expectError(error.InvalidOptions, c.reconnect(.{ .host_port = closed_port, .tls = .{ .client_cert_path = "/does/not/exist.pem" } }));
}

const CanceledFirstCallTest = struct {
    fn health(c: *Client) CheckHealthError!void {
        return c.checkHealth();
    }
    fn count(c: *Client) interceptor.ClientError!void {
        _ = try c.countWorkflows("");
    }
};

test "canceling a call while a client from init makes its first connection returns Canceled" {
    // The abandoned connections are logged as warnings.
    testing.log_level = .err;
    // A listener that never accepts: connecting waits until the client gives up.
    const any: Io.net.IpAddress = try .parseIp4("127.0.0.1", 0);
    var server = try any.listen(testing.io, .{});
    defer server.deinit(testing.io);
    var buf: [32]u8 = undefined;
    const host_port = try std.fmt.bufPrint(&buf, "127.0.0.1:{d}", .{server.socket.address.getPort()});

    var c = try Client.init(testing.allocator, testing.io, .{ .host_port = host_port, .core_log_level = null, .rpc_timeout = .fromSeconds(60) });
    defer c.close();

    // `checkHealth` documents that canceling the calling task cancels it, as a connected
    // client's calls return Canceled.
    var health = try testing.io.concurrent(CanceledFirstCallTest.health, .{&c});
    while (c.shared.connect_mutex.tryLock()) c.shared.connect_mutex.unlock(testing.io);
    try testing.expectError(error.Canceled, health.cancel(testing.io));

    var count = try testing.io.concurrent(CanceledFirstCallTest.count, .{&c});
    while (c.shared.connect_mutex.tryLock()) c.shared.connect_mutex.unlock(testing.io);
    try testing.expectError(error.Canceled, count.cancel(testing.io));
}

const QueuedFirstCallTest = struct {
    fn health(c: *Client, waited: *Io.Duration) void {
        const start = Io.Clock.awake.now(testing.io);
        _ = c.checkHealth() catch {};
        waited.* = start.durationTo(Io.Clock.awake.now(testing.io));
    }
};

test "first calls made together on a client from init each give up within rpc_timeout when the server never answers" {
    // The failed connections are logged as warnings.
    testing.log_level = .err;
    // A listener that never accepts: connecting waits until the client gives up.
    const any: Io.net.IpAddress = try .parseIp4("127.0.0.1", 0);
    var server = try any.listen(testing.io, .{});
    defer server.deinit(testing.io);
    var buf: [32]u8 = undefined;
    const host_port = try std.fmt.bufPrint(&buf, "127.0.0.1:{d}", .{server.socket.address.getPort()});

    var c = try Client.init(testing.allocator, testing.io, .{ .host_port = host_port, .core_log_level = null, .rpc_timeout = .fromSeconds(1) });
    defer c.close();

    var waited: [6]Io.Duration = @splat(.zero);
    var threads: [waited.len]std.Thread = undefined;
    for (&threads, &waited) |*t, *w| t.* = try std.Thread.spawn(.{}, QueuedFirstCallTest.health, .{ &c, w });
    for (threads) |t| t.join();
    // Each call's deadline is `Options.rpc_timeout`, the first connection included: none
    // waits for the others' attempts one after another (6 s here).
    for (waited) |w| try testing.expect(w.nanoseconds < 4 * std.time.ns_per_s);
}

const OvertakenFirstCallTest = struct {
    fn first(c: *Client, out: *?bridge.Connection, waited: *Io.Duration) void {
        const start = Io.Clock.awake.now(testing.io);
        out.* = c.currentConnection() catch null;
        waited.* = start.durationTo(Io.Clock.awake.now(testing.io));
    }
};

test "a reconnect that connects a client from init first hands its connection to the calls waiting" {
    var dev = try @import("testing.zig").DevServer.start(testing.allocator, testing.io, .{});
    defer dev.stop(testing.io);
    // A listener that never accepts: the first call's own attempt never finishes in time.
    const any: Io.net.IpAddress = try .parseIp4("127.0.0.1", 0);
    var silent = try any.listen(testing.io, .{});
    var buf: [32]u8 = undefined;
    var options = dev.clientOptions();
    options.host_port = try std.fmt.bufPrint(&buf, "127.0.0.1:{d}", .{silent.socket.address.getPort()});
    options.rpc_timeout = .fromSeconds(60);

    var c = try Client.init(testing.allocator, testing.io, options);
    defer c.close();
    // Before `close`, which waits for the core's attempt: this ends it at once.
    defer silent.deinit(testing.io);
    var got: ?bridge.Connection = null;
    var waited: Io.Duration = .zero;
    const thread = try std.Thread.spawn(.{}, OvertakenFirstCallTest.first, .{ &c, &got, &waited });
    // Wait until the first call's attempt is in progress.
    while (c.shared.connect_mutex.tryLock()) c.shared.connect_mutex.unlock(testing.io);
    try c.reconnect(dev.clientOptions().connectionOptions());
    thread.join();
    try testing.expectEqual(c.shared.connection.?.ptr, got.?.ptr);
    try testing.expect(waited.nanoseconds < 30 * std.time.ns_per_s);
    try testing.expect(c.shared.connecting == null);
    try c.checkHealth();
}

test "closing a client from init frees a first connection attempt the core is still making" {
    // The abandoned connection is logged as a warning.
    testing.log_level = .err;
    // A listener that never accepts: the attempt outlives the call that started it.
    const any: Io.net.IpAddress = try .parseIp4("127.0.0.1", 0);
    var silent = try any.listen(testing.io, .{});
    var buf: [32]u8 = undefined;
    const host_port = try std.fmt.bufPrint(&buf, "127.0.0.1:{d}", .{silent.socket.address.getPort()});
    var c = try Client.init(testing.allocator, testing.io, .{ .host_port = host_port, .core_log_level = null, .rpc_timeout = .fromMilliseconds(50) });
    try testing.expectError(error.RpcFailed, c.checkHealth());
    try testing.expectError(error.RpcFailed, c.checkHealth());
    // Both attempts were cleared before the core called back. `close` frees those the core
    // has called back by then, and leaves the runtime to the others, which free themselves.
    try testing.expect(c.shared.connecting == null);
    try testing.expectEqual(@as(usize, 2), c.shared.abandoned_attempts.items.len);
    try testing.expect(!c.shared.abandoned_attempts.items[0].calledBack());
    // Closing the listener resets the connections it never accepted, so the core's
    // attempts end soon instead of after its own 30 s.
    silent.deinit(testing.io);
    c.close();
}

const StuckTlsCloseTest = struct {
    fn close(c: *Client, done: *std.atomic.Value(u32)) void {
        c.close();
        done.store(1, .release);
        testing.io.futexWake(u32, &done.raw, 1);
    }
};

test "closing a client from init returns while a first connection it gave up on is stuck in the TLS handshake" {
    // The failed first call is logged as a warning.
    testing.log_level = .err;
    // A listener that never accepts: the kernel completes the TCP handshake, and the core's
    // TLS handshake then waits for a server hello that never comes.
    const any: Io.net.IpAddress = try .parseIp4("127.0.0.1", 0);
    var silent = try any.listen(testing.io, .{});
    var buf: [32]u8 = undefined;
    const host_port = try std.fmt.bufPrint(&buf, "127.0.0.1:{d}", .{silent.socket.address.getPort()});
    var c = try Client.init(testing.allocator, testing.io, .{ .host_port = host_port, .core_log_level = null, .tls = .{}, .rpc_timeout = .fromMilliseconds(100) });
    {
        errdefer c.close();
        errdefer silent.deinit(testing.io);
        try testing.expectError(error.RpcFailed, c.checkHealth());
    }

    var done: std.atomic.Value(u32) = .init(0);
    const thread = std.Thread.spawn(.{}, StuckTlsCloseTest.close, .{ &c, &done }) catch |err| {
        silent.deinit(testing.io);
        c.close();
        return err;
    };
    // Well past the 30 s `close` documents for a server that never answers.
    const deadline: Io.Clock.Timestamp = .fromNow(testing.io, .{ .raw = .fromSeconds(60), .clock = .awake });
    while (done.load(.acquire) == 0 and deadline.durationFromNow(testing.io).raw.nanoseconds > 0) {
        testing.io.futexWaitTimeout(u32, &done.raw, 0, .{ .deadline = deadline }) catch {};
    }
    const returned = done.load(.acquire) != 0;
    // Closing the listener resets the connection it never accepted, which ends the core's
    // attempt, so a `close` still waiting returns and the thread can be joined.
    silent.deinit(testing.io);
    thread.join();
    try testing.expect(returned);
}

test "connect gives up within rpc_timeout on a server that never answers the TLS handshake" {
    // A listener that never accepts: the kernel completes the TCP handshake, and the core's
    // TLS handshake then waits for a server hello that never comes.
    const any: Io.net.IpAddress = try .parseIp4("127.0.0.1", 0);
    var silent = try any.listen(testing.io, .{});
    // Resets the connection the core's abandoned attempt made, which then ends.
    defer silent.deinit(testing.io);
    var buf: [32]u8 = undefined;
    const host_port = try std.fmt.bufPrint(&buf, "127.0.0.1:{d}", .{silent.socket.address.getPort()});

    // `connect` logs its failure as an error, which would fail the test: capture it.
    var logged: Io.Writer.Allocating = .init(testing.allocator);
    defer logged.deinit();
    test_connect_failure_log = &logged.writer;
    defer test_connect_failure_log = null;

    const start = Io.Clock.awake.now(testing.io);
    try testing.expectError(error.ConnectFailed, Client.connect(testing.allocator, testing.io, .{ .host_port = host_port, .core_log_level = null, .tls = .{}, .rpc_timeout = .fromMilliseconds(100) }));
    const waited = start.durationTo(Io.Clock.awake.now(testing.io));
    try testing.expect(waited.nanoseconds < 10 * std.time.ns_per_s);
    const line = logged.written();
    try testing.expect(std.mem.indexOf(u8, line, "didn't answer within Options.rpc_timeout") != null);
    try testing.expect(std.mem.indexOf(u8, line, host_port) != null);
}

test "a client from init keeps at most max_connect_attempts_in_flight connection attempts however many calls give up" {
    // The failed first calls are logged as warnings.
    testing.log_level = .err;
    // A listener that never accepts: every attempt is stuck in the TLS handshake.
    const any: Io.net.IpAddress = try .parseIp4("127.0.0.1", 0);
    var silent = try any.listen(testing.io, .{});
    var buf: [32]u8 = undefined;
    const host_port = try std.fmt.bufPrint(&buf, "127.0.0.1:{d}", .{silent.socket.address.getPort()});
    var c = try Client.init(testing.allocator, testing.io, .{ .host_port = host_port, .core_log_level = null, .tls = .{}, .rpc_timeout = .fromMilliseconds(20) });
    defer c.close();
    // Before `close`: closing the listener resets the connections it never accepted, so the
    // core's attempts end soon.
    defer silent.deinit(testing.io);

    for (0..3 * Client.max_connect_attempts_in_flight) |_| {
        try testing.expectError(error.RpcFailed, c.checkHealth());
        const in_flight = c.shared.abandoned_attempts.items.len + @intFromBool(c.shared.connecting != null);
        try testing.expect(in_flight <= Client.max_connect_attempts_in_flight);
    }
    try testing.expect(c.shared.connecting == null);
    try testing.expectEqual(@as(usize, Client.max_connect_attempts_in_flight), c.shared.abandoned_attempts.items.len);
    for (c.shared.abandoned_attempts.items) |p| try testing.expect(!p.calledBack());
}

test "connect keeps failing with ConnectFailed however many times it gives up on a server that never answers the TLS handshake" {
    // A process that retries `connect` while its server hangs. With the default
    // `Options.core_log_level`, each client's runtime forwards sdk-core's logs through one of
    // `bridge.Runtime.max_forwarders` slots, and a timed-out `connect` leaves its runtime to
    // the attempt sdk-core is still making: that must not use up the slots, or every later
    // client in the process fails with `error.RuntimeInitFailed`.
    testing.log_level = .err;
    const any: Io.net.IpAddress = try .parseIp4("127.0.0.1", 0);
    var silent = try any.listen(testing.io, .{});
    // Resets the connections the core's abandoned attempts made, which then end.
    defer silent.deinit(testing.io);
    var buf: [32]u8 = undefined;
    const host_port = try std.fmt.bufPrint(&buf, "127.0.0.1:{d}", .{silent.socket.address.getPort()});

    var logged: Io.Writer.Allocating = .init(testing.allocator);
    defer logged.deinit();
    test_connect_failure_log = &logged.writer;
    defer test_connect_failure_log = null;

    for (0..bridge.Runtime.max_forwarders + 1) |_| {
        if (Client.connect(testing.allocator, testing.io, .{ .host_port = host_port, .tls = .{}, .rpc_timeout = .fromMilliseconds(20) })) |connected| {
            var c = connected;
            c.close();
            return error.TestUnexpectedResult;
        } else |err| try testing.expectEqual(error.ConnectFailed, err);
    }
}

/// A failed ExecuteMultiOperation's status: `code` and `message`, with a
/// MultiOperationExecutionFailure detail holding `statuses` unless it's null.
fn multiOperationStatus(arena: Allocator, code: u32, message: []const u8, statuses: ?[]const proto.temporal.api.errordetails.v1.MultiOperationExecutionFailure.OperationStatus) !Client.RpcStatus {
    var status: proto.google.rpc.Status = .{ .code = @intCast(code), .message = message };
    if (statuses) |ops| {
        var failure: proto.temporal.api.errordetails.v1.MultiOperationExecutionFailure = .{};
        try failure.statuses.appendSlice(arena, ops);
        var value: Io.Writer.Allocating = .init(arena);
        try failure.encode(&value.writer, arena);
        try status.details.append(arena, .{ .type_url = "type.googleapis.com/temporal.api.errordetails.v1.MultiOperationExecutionFailure", .value = value.written() });
    }
    var details: Io.Writer.Allocating = .init(arena);
    try status.encode(&details.writer, arena);
    return .{ .code = code, .message = message, .details = details.written() };
}

test "a failed update-with-start names the operation that failed" {
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const prefix = "update-with-start of update \"set\" on workflow cart-1 failed: ";
    const Case = struct {
        status: Client.RpcStatus,
        line: []const u8,
    };
    const cases = [_]Case{
        .{
            .status = try multiOperationStatus(a, 3, "MultiOperation could not be executed.", &.{
                .{ .code = 3, .message = "missing task queue" },
                .{ .code = 10, .message = "Operation was aborted." },
            }),
            .line = prefix ++ "starting the workflow failed: missing task queue (gRPC status 3)",
        },
        .{
            .status = try multiOperationStatus(a, 8, "MultiOperation could not be executed.", &.{
                .{ .code = 10, .message = "Operation was aborted." },
                .{ .code = 8, .message = "too many outstanding updates" },
            }),
            .line = prefix ++ "the update failed: too many outstanding updates (gRPC status 8)",
        },
        // Without the detail, or with one that names no failing operation, the line has the
        // call's own status.
        .{
            .status = try multiOperationStatus(a, 14, "connection reset", null),
            .line = prefix ++ "connection reset (gRPC status 14)",
        },
        .{
            .status = try multiOperationStatus(a, 10, "MultiOperation could not be executed.", &.{
                .{ .code = 10, .message = "Operation was aborted." },
                .{ .code = 10, .message = "Operation was aborted." },
            }),
            .line = prefix ++ "MultiOperation could not be executed. (gRPC status 10)",
        },
        .{
            .status = .{ .code = 13, .message = "internal", .details = "\xff\xff" },
            .line = prefix ++ "internal (gRPC status 13)",
        },
    };
    for (cases) |case| {
        var line: Io.Writer.Allocating = .init(testing.allocator);
        defer line.deinit();
        try testing.expectEqual(error.RpcFailed, Client.multiOperationFailure(&line.writer, case.status, a, "cart-1", "set"));
        try testing.expectEqualStrings(case.line, line.written());
    }
}
