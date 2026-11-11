//! A connection to a Temporal server through sdk-core, and raw gRPC calls over it.
//!
//! Requests and responses are serialized protobuf bytes; typed calls are built on top of
//! `call`. A connection is safe to use from many threads and must not outlive its runtime.

const Connection = @This();

const std = @import("std");
const c = @import("core_c");
const bytes = @import("bytes.zig");
const Runtime = @import("Runtime.zig");
const Completion = @import("completion.zig").Completion;
const Allocator = std.mem.Allocator;
const Io = std.Io;

ptr: *c.TemporalCoreConnection,
runtime: Runtime,

pub const Options = struct {
    /// Server URL including the scheme, such as "http://localhost:7233" or
    /// "https://my-namespace.a1b2c.tmprl.cloud:7233".
    target_url: []const u8,
    /// Identity reported to the server for this client and its workers.
    identity: []const u8,
    client_name: []const u8 = "temporal-zig",
    client_version: []const u8 = @import("../temporal.zig").version,
    /// gRPC headers sent with every call.
    metadata: []const bytes.MetadataEntry = &.{},
    /// Binary gRPC headers (keys end in "-bin") sent with every call.
    binary_metadata: []const bytes.MetadataEntry = &.{},
    /// Sent as a bearer token. Empty sends none.
    api_key: []const u8 = "",
    tls: ?Tls = null,
    retry: ?Retry = null,
    /// Null turns keep-alive pings off.
    keep_alive: ?KeepAlive = .{},
    http_connect_proxy: ?HttpConnectProxy = null,
    /// Re-resolve DNS this often (milliseconds) and spread calls over all addresses. Null
    /// connects to one address.
    dns_load_balancing_interval_ms: ?u64 = null,
    compression: enum { gzip, none } = .gzip,

    pub const Tls = struct {
        /// PEM. Empty uses the system roots.
        server_root_ca_cert: []const u8 = "",
        /// Overrides the server name checked against the certificate.
        domain: []const u8 = "",
        /// PEM client certificate and key for mutual TLS. Empty for none.
        client_cert: []const u8 = "",
        client_private_key: []const u8 = "",
    };

    /// Retries of failed calls inside the core. Null uses the core defaults (200 ms initial
    /// interval, doubling to 10 s, retrying indefinitely).
    pub const Retry = struct {
        initial_interval_ms: u64 = 200,
        randomization_factor: f64 = 0.2,
        multiplier: f64 = 2.0,
        max_interval_ms: u64 = 10_000,
        /// 0 means no limit.
        max_elapsed_time_ms: u64 = 0,
        /// 0 means no limit.
        max_retries: usize = 0,
    };

    /// The core's defaults.
    pub const KeepAlive = struct {
        interval_ms: u64 = 30_000,
        timeout_ms: u64 = 15_000,
    };

    pub const HttpConnectProxy = struct {
        /// host:port of the proxy.
        target_host: []const u8,
        username: []const u8 = "",
        password: []const u8 = "",
    };
};

pub const ConnectError = error{ OutOfMemory, InvalidOptions, ConnectFailed };

/// Connects and checks the server (GetSystemInfo). Waits without cancellation, because the
/// core reads `options` until it calls back. On `error.ConnectFailed`, `failure` (if given)
/// receives the core's message, allocated with `gpa`.
pub fn connect(gpa: Allocator, io: Io, runtime: Runtime, options: Options, failure: ?*?[]u8) ConnectError!Connection {
    var arena_state: std.heap.ArenaAllocator = .init(gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var tls: c.TemporalCoreClientTlsOptions = undefined;
    var retry: c.TemporalCoreClientRetryOptions = undefined;
    var keep_alive: c.TemporalCoreClientKeepAliveOptions = undefined;
    var proxy: c.TemporalCoreClientHttpConnectProxyOptions = undefined;
    var dns: c.TemporalCoreClientDnsLoadBalancingOptions = undefined;

    var core: c.TemporalCoreConnectionOptions = std.mem.zeroes(c.TemporalCoreConnectionOptions);
    core.target_url = bytes.ref(options.target_url);
    core.client_name = bytes.ref(options.client_name);
    core.client_version = bytes.ref(options.client_version);
    core.metadata = bytes.metadata(arena, options.metadata) catch |err| return encodeError(err);
    core.binary_metadata = bytes.metadata(arena, options.binary_metadata) catch |err| return encodeError(err);
    core.api_key = bytes.ref(options.api_key);
    core.identity = bytes.ref(options.identity);
    if (options.tls) |t| {
        tls = .{
            .server_root_ca_cert = bytes.ref(t.server_root_ca_cert),
            .domain = bytes.ref(t.domain),
            .client_cert = bytes.ref(t.client_cert),
            .client_private_key = bytes.ref(t.client_private_key),
        };
        core.tls_options = &tls;
    }
    if (options.retry) |r| {
        retry = .{
            .initial_interval_millis = r.initial_interval_ms,
            .randomization_factor = r.randomization_factor,
            .multiplier = r.multiplier,
            .max_interval_millis = r.max_interval_ms,
            .max_elapsed_time_millis = r.max_elapsed_time_ms,
            .max_retries = r.max_retries,
        };
        core.retry_options = &retry;
    }
    if (options.keep_alive) |k| {
        keep_alive = .{ .interval_millis = k.interval_ms, .timeout_millis = k.timeout_ms };
        core.keep_alive_options = &keep_alive;
    }
    if (options.http_connect_proxy) |p| {
        proxy = .{ .target_host = bytes.ref(p.target_host), .username = bytes.ref(p.username), .password = bytes.ref(p.password) };
        core.http_connect_proxy_options = &proxy;
    }
    if (options.dns_load_balancing_interval_ms) |ms| {
        dns = .{ .resolution_interval_millis = ms };
        core.dns_load_balancing_options = &dns;
    }
    core.grpc_compression = switch (options.compression) {
        .gzip => c.TemporalCoreClientGrpcCompression_Gzip,
        .none => c.TemporalCoreClientGrpcCompression_None,
    };

    const done = try Completion(ConnectResult).create(gpa, io, ConnectResult.free);
    c.temporal_core_client_connect(runtime.ptr, &core, done, onConnect);
    const result = done.waitUncancelable();
    const conn = result.conn orelse {
        defer if (result.fail) |f| f.deinit();
        if (failure) |f| f.* = if (result.fail) |msg| msg.dupe(gpa) catch null else null;
        return error.ConnectFailed;
    };
    if (result.fail) |f| f.deinit();
    return .{ .ptr = conn, .runtime = runtime };
}

pub fn deinit(conn: Connection) void {
    c.temporal_core_client_free(conn.ptr);
}

/// Replaces the gRPC headers sent with every call.
pub fn setMetadata(conn: Connection, gpa: Allocator, entries: []const bytes.MetadataEntry) bytes.EncodeError!void {
    var arena_state: std.heap.ArenaAllocator = .init(gpa);
    defer arena_state.deinit();
    c.temporal_core_client_update_metadata(conn.ptr, try bytes.metadata(arena_state.allocator(), entries));
}

/// Replaces the binary gRPC headers sent with every call.
pub fn setBinaryMetadata(conn: Connection, gpa: Allocator, entries: []const bytes.MetadataEntry) bytes.EncodeError!void {
    var arena_state: std.heap.ArenaAllocator = .init(gpa);
    defer arena_state.deinit();
    c.temporal_core_client_update_binary_metadata(conn.ptr, try bytes.metadata(arena_state.allocator(), entries));
}

/// Replaces the API key. Empty stops sending one.
pub fn setApiKey(conn: Connection, api_key: []const u8) void {
    c.temporal_core_client_update_api_key(conn.ptr, bytes.ref(api_key));
}

pub const Service = enum {
    workflow,
    operator,
    cloud,
    test_service,
    health,

    fn toC(s: Service) c.TemporalCoreRpcService {
        return switch (s) {
            .workflow => c.TemporalCoreRpcService_Workflow,
            .operator => c.TemporalCoreRpcService_Operator,
            .cloud => c.TemporalCoreRpcService_Cloud,
            .test_service => c.TemporalCoreRpcService_Test,
            .health => c.TemporalCoreRpcService_Health,
        };
    }
};

pub const CallOptions = struct {
    /// Retry retryable failures with the connection's retry policy.
    retry: bool = false,
    metadata: []const bytes.MetadataEntry = &.{},
    binary_metadata: []const bytes.MetadataEntry = &.{},
    /// 0 means no timeout.
    timeout_ms: u32 = 0,
};

/// Details of a failed call, owned by the allocator passed to `call`.
pub const RpcFailure = struct {
    /// gRPC status code (https://grpc.io/docs/guides/status-codes/). 0 when the call failed
    /// without a gRPC status, for example a transport error.
    status_code: u32,
    message: []u8,
    /// Serialized google.rpc.Status, possibly empty. Its details carry Temporal's typed
    /// failures (temporal.api.errordetails.v1).
    details: []u8,

    pub fn deinit(f: RpcFailure, gpa: Allocator) void {
        gpa.free(f.message);
        gpa.free(f.details);
    }
};

pub const CallError = error{ OutOfMemory, InvalidOptions, Canceled, RpcFailed };

/// Calls `rpc` (for example "GetSystemInfo") on `service` with a serialized request and
/// returns the serialized response, allocated with `gpa`. On `error.RpcFailed`, `failure`
/// (if given) receives the status. Canceling the calling task cancels the call in the core.
pub fn call(
    conn: Connection,
    gpa: Allocator,
    io: Io,
    service: Service,
    rpc: []const u8,
    request: []const u8,
    options: CallOptions,
    failure: ?*?RpcFailure,
) CallError![]u8 {
    // The core reads these until it calls back, and the wait below always lasts that long.
    var arena_state: std.heap.ArenaAllocator = .init(gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const token = c.temporal_core_cancellation_token_new() orelse return error.OutOfMemory;
    defer c.temporal_core_cancellation_token_free(token);

    const core: c.TemporalCoreRpcCallOptions = .{
        .service = service.toC(),
        .rpc = bytes.ref(rpc),
        .req = bytes.ref(request),
        .retry = options.retry,
        .metadata = bytes.metadata(arena, options.metadata) catch |err| return encodeError(err),
        .binary_metadata = bytes.metadata(arena, options.binary_metadata) catch |err| return encodeError(err),
        .timeout_millis = options.timeout_ms,
        .cancellation_token = token,
    };
    const done = try Completion(CallResult).create(gpa, io, CallResult.free);
    c.temporal_core_client_rpc_call(conn.ptr, &core, done, onCall);
    const result = try done.waitCancelingWith(token, cancelToken);
    defer result.free();

    if (result.success) |s| return gpa.dupe(u8, s.bytes());
    if (failure) |f| {
        const message = try gpa.dupe(u8, if (result.message) |m| m.bytes() else "");
        errdefer gpa.free(message);
        f.* = .{
            .status_code = result.status_code,
            .message = message,
            .details = try gpa.dupe(u8, if (result.details) |d| d.bytes() else ""),
        };
    }
    return error.RpcFailed;
}

fn cancelToken(ctx: *anyopaque) void {
    c.temporal_core_cancellation_token_cancel(@ptrCast(@alignCast(ctx)));
}

fn encodeError(err: bytes.EncodeError) error{ OutOfMemory, InvalidOptions } {
    return switch (err) {
        error.OutOfMemory => error.OutOfMemory,
        error.NewlineInKey, error.NewlineInValue => error.InvalidOptions,
    };
}

const ConnectResult = struct {
    conn: ?*c.TemporalCoreConnection,
    fail: ?bytes.Owned,

    fn free(r: ConnectResult) void {
        if (r.fail) |f| f.deinit();
        if (r.conn) |p| c.temporal_core_client_free(p);
    }
};

fn onConnect(user_data: ?*anyopaque, conn: ?*c.TemporalCoreConnection, fail: ?*const c.TemporalCoreByteArray) callconv(.c) void {
    const done: *Completion(ConnectResult) = @ptrCast(@alignCast(user_data.?));
    done.complete(.{ .conn = conn, .fail = bytes.Owned.take(null, fail) });
}

const CallResult = struct {
    success: ?bytes.Owned,
    status_code: u32,
    message: ?bytes.Owned,
    details: ?bytes.Owned,

    fn free(r: CallResult) void {
        if (r.success) |v| v.deinit();
        if (r.message) |v| v.deinit();
        if (r.details) |v| v.deinit();
    }
};

fn onCall(
    user_data: ?*anyopaque,
    success: ?*const c.TemporalCoreByteArray,
    status_code: u32,
    message: ?*const c.TemporalCoreByteArray,
    details: ?*const c.TemporalCoreByteArray,
) callconv(.c) void {
    const done: *Completion(CallResult) = @ptrCast(@alignCast(user_data.?));
    done.complete(.{
        .success = bytes.Owned.take(null, success),
        .status_code = status_code,
        .message = bytes.Owned.take(null, message),
        .details = bytes.Owned.take(null, details),
    });
}

// ---------------------------------------------------------------- tests

const testing = std.testing;
const EphemeralServer = @import("EphemeralServer.zig");
const proto = @import("../proto.zig");
const wfs = proto.temporal.api.workflowservice.v1;

const TestEnv = struct {
    rt: Runtime,
    server: EphemeralServer,
    conn: Connection,
    url: []u8,

    fn init(e: *TestEnv) !void {
        e.rt = try Runtime.init(testing.allocator, .{}, null);
        errdefer e.rt.deinit();
        e.server = try EphemeralServer.startDevServer(testing.allocator, testing.io, e.rt, .{}, null);
        errdefer e.server.shutdown(testing.io, null) catch {};
        e.url = try std.fmt.allocPrint(testing.allocator, "http://{s}", .{e.server.target});
        errdefer testing.allocator.free(e.url);
        e.conn = try connect(testing.allocator, testing.io, e.rt, .{ .target_url = e.url, .identity = "test@zig" }, null);
    }

    fn deinit(e: *TestEnv) void {
        e.conn.deinit();
        testing.allocator.free(e.url);
        e.server.shutdown(testing.io, null) catch {};
        e.rt.deinit();
    }
};

fn encode(msg: anytype, a: Allocator) ![]const u8 {
    var aw: std.Io.Writer.Allocating = .init(a);
    try msg.encode(&aw.writer, a);
    return aw.written();
}

test "connect to a dev server and make typed calls through raw bytes" {
    var e: TestEnv = undefined;
    try e.init();
    defer e.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // GetSystemInfo succeeds and reports server capabilities.
    const info_bytes = try e.conn.call(testing.allocator, testing.io, .workflow, "GetSystemInfo", try encode(wfs.GetSystemInfoRequest{}, a), .{}, null);
    defer testing.allocator.free(info_bytes);
    var r: std.Io.Reader = .fixed(info_bytes);
    const info = try wfs.GetSystemInfoResponse.decode(&r, a);
    try testing.expect(info.server_version.len > 0);

    // A missing namespace fails with gRPC NOT_FOUND (5) and a message.
    var failure: ?RpcFailure = null;
    defer if (failure) |f| f.deinit(testing.allocator);
    try testing.expectError(error.RpcFailed, e.conn.call(testing.allocator, testing.io, .workflow, "DescribeNamespace", try encode(wfs.DescribeNamespaceRequest{ .namespace = "does-not-exist" }, a), .{}, &failure));
    try testing.expectEqual(@as(u32, 5), failure.?.status_code);
    try testing.expect(failure.?.message.len > 0);

    // Header and API key updates apply to later calls.
    try e.conn.setMetadata(testing.allocator, &.{.{ .key = "x-test", .value = "1" }});
    e.conn.setApiKey("");
    const again = try e.conn.call(testing.allocator, testing.io, .workflow, "GetSystemInfo", try encode(wfs.GetSystemInfoRequest{}, a), .{ .metadata = &.{.{ .key = "x-call", .value = "2" }} }, null);
    testing.allocator.free(again);
}

fn longPoll(conn: Connection, request: []const u8) CallError!void {
    const resp = try conn.call(testing.allocator, testing.io, .workflow, "PollWorkflowTaskQueue", request, .{}, null);
    testing.allocator.free(resp);
}

test "canceling the calling task cancels a long-poll in the core" {
    var e: TestEnv = undefined;
    try e.init();
    defer e.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const request = try encode(wfs.PollWorkflowTaskQueueRequest{
        .namespace = "default",
        .task_queue = .{ .name = "nobody-starts-workflows-here", .kind = .TASK_QUEUE_KIND_NORMAL },
        .identity = "test@zig",
    }, arena.allocator());

    // The poll would wait about a minute for a task that never comes.
    var poll = try testing.io.concurrent(longPoll, .{ e.conn, request });
    try testing.io.sleep(.fromMilliseconds(200), .awake);
    const start = std.Io.Timestamp.now(testing.io, .awake);
    try testing.expectError(error.Canceled, poll.cancel(testing.io));
    const waited = start.durationTo(std.Io.Timestamp.now(testing.io, .awake));
    try testing.expect(waited.nanoseconds < 5 * std.time.ns_per_s);
}

test "an invalid URL fails to connect with the core's message" {
    const rt = try Runtime.init(testing.allocator, .{}, null);
    defer rt.deinit();
    var failure: ?[]u8 = null;
    defer if (failure) |f| testing.allocator.free(f);
    try testing.expectError(error.ConnectFailed, connect(testing.allocator, testing.io, rt, .{ .target_url = "not a url", .identity = "test" }, &failure));
    try testing.expect(failure != null and failure.?.len > 0);
}
