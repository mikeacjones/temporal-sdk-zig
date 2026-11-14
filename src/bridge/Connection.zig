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

/// The client name and version sent to the server (as the `client-name` and
/// `client-version` headers) unless the options name others.
pub const default_client_name = "temporal-zig";
pub const default_client_version = @import("../temporal.zig").version;

pub const Options = struct {
    /// Server URL including the scheme, such as "http://localhost:7233" or
    /// "https://my-namespace.a1b2c.tmprl.cloud:7233".
    target_url: []const u8,
    /// Identity reported to the server for this client and its workers.
    identity: []const u8,
    client_name: []const u8 = default_client_name,
    client_version: []const u8 = default_client_version,
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
    /// Serves every gRPC call of the connection, including the GetSystemInfo that `connect`
    /// makes, in Zig instead of over the network. `target_url` must still be a valid URL;
    /// TLS, keep-alive, proxy, DNS load balancing and compression are ignored. The pointee
    /// must outlive the connection.
    grpc_override: ?*const GrpcOverride = null,

    pub const Tls = struct {
        /// PEM. Empty uses the system roots.
        server_root_ca_cert: []const u8 = "",
        /// Overrides the server name checked against the certificate.
        domain: []const u8 = "",
        /// PEM client certificate and key for mutual TLS. Empty for none.
        client_cert: []const u8 = "",
        client_private_key: []const u8 = "",
    };

    /// Retries of failed calls inside the core: calls made with `CallOptions.retry`, and
    /// workers' calls other than polls (which have their own policy). Null, and the field
    /// defaults, are the core's default policy.
    pub const Retry = struct {
        initial_interval_ms: u64 = 100,
        randomization_factor: f64 = 0.2,
        multiplier: f64 = 1.7,
        max_interval_ms: u64 = 5_000,
        /// 0 means no limit.
        max_elapsed_time_ms: u64 = 10_000,
        /// 0 means no limit.
        max_retries: usize = 10,
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

/// A handler for all of a connection's gRPC calls (see `Options.grpc_override`).
///
/// `handle` runs on one of the core's blocking threads, once per call and possibly
/// concurrently, so it and `ctx` must be thread-safe. It must call `GrpcRequest.respond`
/// exactly once for each request, during the call or later from any thread: until then the
/// caller waits and the request's memory stays allocated. It should return quickly and do
/// slow work elsewhere.
pub const GrpcOverride = struct {
    ctx: ?*anyopaque = null,
    handle: *const fn (ctx: ?*anyopaque, request: GrpcRequest) void,

    fn onRequest(req: ?*c.TemporalCoreClientGrpcOverrideRequest, user_data: ?*anyopaque) callconv(.c) void {
        const o: *const GrpcOverride = @ptrCast(@alignCast(user_data.?));
        o.handle(o.ctx, .{ .ptr = req.? });
    }
};

/// One gRPC call handed to a `GrpcOverride`. Everything read from it is borrowed from the
/// core and valid only until `respond`.
pub const GrpcRequest = struct {
    ptr: *c.TemporalCoreClientGrpcOverrideRequest,

    /// Fully qualified service name, such as "temporal.api.workflowservice.v1.WorkflowService".
    pub fn service(r: GrpcRequest) []const u8 {
        return bytes.slice(c.temporal_core_client_grpc_override_request_service(r.ptr));
    }

    /// Method name, such as "GetSystemInfo".
    pub fn rpc(r: GrpcRequest) []const u8 {
        return bytes.slice(c.temporal_core_client_grpc_override_request_rpc(r.ptr));
    }

    /// Serialized request message.
    pub fn proto(r: GrpcRequest) []const u8 {
        return bytes.slice(c.temporal_core_client_grpc_override_request_proto(r.ptr));
    }

    /// The call's ASCII headers (the core leaves out values that aren't visible ASCII).
    pub fn headers(r: GrpcRequest) HeaderIterator {
        const md = c.temporal_core_client_grpc_override_request_headers(r.ptr);
        return .{ .refs = if (md.size == 0) &.{} else md.data[0..md.size] };
    }

    /// The value of the first header named `key` (lower-case, as gRPC sends names), or null.
    pub fn header(r: GrpcRequest, key: []const u8) ?[]const u8 {
        var it = r.headers();
        while (it.next()) |e| if (std.mem.eql(u8, e.key, key)) return e.value;
        return null;
    }

    pub const HeaderIterator = struct {
        refs: []const bytes.Ref,
        i: usize = 0,

        pub fn next(it: *HeaderIterator) ?bytes.MetadataEntry {
            while (it.i < it.refs.len) {
                const entry = bytes.slice(it.refs[it.i]);
                it.i += 1;
                const nl = std.mem.indexOfScalar(u8, entry, '\n') orelse continue;
                return .{ .key = entry[0..nl], .value = entry[nl + 1 ..] };
            }
            return null;
        }
    };

    /// Completes the call and frees the request; `r` and everything read from it is invalid
    /// afterwards. The core copies `response` before this returns. `gpa` is used only while
    /// encoding the response headers: if that fails (out of memory, or a newline in a key),
    /// the call fails with INTERNAL instead.
    pub fn respond(r: GrpcRequest, gpa: Allocator, response: GrpcResponse) void {
        var arena_state: std.heap.ArenaAllocator = .init(gpa);
        defer arena_state.deinit();
        const md = bytes.metadata(arena_state.allocator(), response.headers) catch |err| {
            c.temporal_core_client_grpc_override_request_respond(r.ptr, .{
                .status_code = grpc_internal,
                .headers = bytes.refArray(&.{}),
                .success_proto = bytes.ref(""),
                .fail_message = bytes.ref(switch (err) {
                    error.OutOfMemory => "gRPC override response: out of memory encoding the response headers",
                    error.NewlineInKey, error.NewlineInValue => "gRPC override response: a response header key contains a newline",
                }),
                .fail_details = bytes.ref(""),
            });
            return;
        };
        c.temporal_core_client_grpc_override_request_respond(r.ptr, .{
            .status_code = @bitCast(response.status_code),
            .headers = md,
            .success_proto = bytes.ref(response.proto),
            .fail_message = bytes.ref(response.message),
            .fail_details = bytes.ref(response.details),
        });
    }

    const grpc_internal = 13;
};

/// The answer to a `GrpcRequest`. Borrowed by `GrpcRequest.respond` until it returns.
pub const GrpcResponse = struct {
    /// gRPC status code; 0 is success.
    status_code: u32 = 0,
    /// Response headers (user metadata, not gRPC's own like content-type).
    headers: []const bytes.MetadataEntry = &.{},
    /// Serialized response message. Ignored unless `status_code` is 0.
    proto: []const u8 = "",
    /// Failure message. Ignored when `status_code` is 0.
    message: []const u8 = "",
    /// Serialized google.rpc.Status, or empty. Ignored when `status_code` is 0.
    details: []const u8 = "",
};

pub const ConnectError = error{ OutOfMemory, InvalidOptions, ConnectFailed };

/// Connects and checks the server (GetSystemInfo). Waits without cancellation, because the
/// core reads `options` until it calls back. On `error.ConnectFailed`, `failure` (if given)
/// receives the core's message, allocated with `gpa`.
pub fn connect(gpa: Allocator, io: Io, runtime: Runtime, options: Options, failure: ?*?[]u8) ConnectError!Connection {
    var arena_state: std.heap.ArenaAllocator = .init(gpa);
    defer arena_state.deinit();
    var core: CoreOptions = undefined;
    try core.init(arena_state.allocator(), options);

    const done = try Completion(ConnectResult).create(gpa, io, ConnectResult.free);
    c.temporal_core_client_connect(runtime.ptr, &core.connection, done, onConnect);
    const result = done.waitUncancelable();
    const conn = result.conn orelse {
        defer if (result.fail) |f| f.deinit();
        if (failure) |f| f.* = if (result.fail) |msg| msg.dupe(gpa) catch null else null;
        return error.ConnectFailed;
    };
    if (result.fail) |f| f.deinit();
    return .{ .ptr = conn, .runtime = runtime };
}

/// The core's connection options and the structs they point at, which point into this value
/// and the arena passed to `init`: don't move it while the core reads it.
const CoreOptions = struct {
    connection: c.TemporalCoreConnectionOptions,
    tls: c.TemporalCoreClientTlsOptions,
    retry: c.TemporalCoreClientRetryOptions,
    keep_alive: c.TemporalCoreClientKeepAliveOptions,
    proxy: c.TemporalCoreClientHttpConnectProxyOptions,
    dns: c.TemporalCoreClientDnsLoadBalancingOptions,

    /// Borrows the strings in `options`.
    fn init(o: *CoreOptions, arena: Allocator, options: Options) error{ OutOfMemory, InvalidOptions }!void {
        const core = &o.connection;
        core.* = std.mem.zeroes(c.TemporalCoreConnectionOptions);
        core.target_url = bytes.ref(options.target_url);
        core.client_name = bytes.ref(options.client_name);
        core.client_version = bytes.ref(options.client_version);
        core.metadata = bytes.metadata(arena, options.metadata) catch |err| return encodeError(err);
        core.binary_metadata = bytes.metadata(arena, options.binary_metadata) catch |err| return encodeError(err);
        core.api_key = bytes.ref(options.api_key);
        core.identity = bytes.ref(options.identity);
        if (options.tls) |t| {
            o.tls = .{
                .server_root_ca_cert = bytes.ref(t.server_root_ca_cert),
                .domain = bytes.ref(t.domain),
                .client_cert = bytes.ref(t.client_cert),
                .client_private_key = bytes.ref(t.client_private_key),
            };
            core.tls_options = &o.tls;
        }
        if (options.retry) |r| {
            o.retry = .{
                .initial_interval_millis = r.initial_interval_ms,
                .randomization_factor = r.randomization_factor,
                .multiplier = r.multiplier,
                .max_interval_millis = r.max_interval_ms,
                .max_elapsed_time_millis = r.max_elapsed_time_ms,
                .max_retries = r.max_retries,
            };
            core.retry_options = &o.retry;
        }
        if (options.keep_alive) |k| {
            o.keep_alive = .{ .interval_millis = k.interval_ms, .timeout_millis = k.timeout_ms };
            core.keep_alive_options = &o.keep_alive;
        }
        if (options.http_connect_proxy) |p| {
            o.proxy = .{ .target_host = bytes.ref(p.target_host), .username = bytes.ref(p.username), .password = bytes.ref(p.password) };
            core.http_connect_proxy_options = &o.proxy;
        }
        if (options.dns_load_balancing_interval_ms) |ms| {
            o.dns = .{ .resolution_interval_millis = ms };
            core.dns_load_balancing_options = &o.dns;
        }
        if (options.grpc_override) |handler| {
            core.grpc_override_callback = GrpcOverride.onRequest;
            core.grpc_override_callback_user_data = @constCast(handler);
        }
        core.grpc_compression = switch (options.compression) {
            .gzip => c.TemporalCoreClientGrpcCompression_Gzip,
            .none => c.TemporalCoreClientGrpcCompression_None,
        };
    }
};

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

test "connection options reach the core's C structs" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // Defaults: keep-alive at the core's values, no retry options or proxy, the SDK's name.
    var core: CoreOptions = undefined;
    try core.init(a, .{ .target_url = "http://localhost:7233", .identity = "id" });
    try testing.expectEqualStrings(default_client_name, bytes.slice(core.connection.client_name));
    try testing.expectEqualStrings(default_client_version, bytes.slice(core.connection.client_version));
    try testing.expectEqual(@as(u64, 30_000), core.connection.keep_alive_options.*.interval_millis);
    try testing.expectEqual(@as(u64, 15_000), core.connection.keep_alive_options.*.timeout_millis);
    try testing.expect(core.connection.retry_options == null);
    try testing.expect(core.connection.http_connect_proxy_options == null);

    // Custom values, and null keep-alive.
    try core.init(a, .{
        .target_url = "http://localhost:7233",
        .identity = "id",
        .client_name = "wrapper",
        .client_version = "9.9.9",
        .keep_alive = null,
        .retry = .{ .max_elapsed_time_ms = 0, .max_retries = 3 },
        .http_connect_proxy = .{ .target_host = "proxy.corp:3128", .username = "svc", .password = "pw" },
    });
    try testing.expectEqualStrings("wrapper", bytes.slice(core.connection.client_name));
    try testing.expectEqualStrings("9.9.9", bytes.slice(core.connection.client_version));
    try testing.expect(core.connection.keep_alive_options == null);
    const retry = core.connection.retry_options.*;
    try testing.expectEqual(@as(u64, 100), retry.initial_interval_millis);
    try testing.expectEqual(@as(f64, 0.2), retry.randomization_factor);
    try testing.expectEqual(@as(f64, 1.7), retry.multiplier);
    try testing.expectEqual(@as(u64, 5_000), retry.max_interval_millis);
    try testing.expectEqual(@as(u64, 0), retry.max_elapsed_time_millis);
    try testing.expectEqual(@as(usize, 3), retry.max_retries);
    const proxy = core.connection.http_connect_proxy_options.*;
    try testing.expectEqualStrings("proxy.corp:3128", bytes.slice(proxy.target_host));
    try testing.expectEqualStrings("svc", bytes.slice(proxy.username));
    try testing.expectEqualStrings("pw", bytes.slice(proxy.password));

    try core.init(a, .{ .target_url = "http://localhost:7233", .identity = "id", .keep_alive = .{ .interval_ms = 10_000, .timeout_ms = 2_000 } });
    try testing.expectEqual(@as(u64, 10_000), core.connection.keep_alive_options.*.interval_millis);
    try testing.expectEqual(@as(u64, 2_000), core.connection.keep_alive_options.*.timeout_millis);
}

/// Answers calls from Zig: GetSystemInfo, DescribeNamespace (found only for "fake"), and
/// SignalWorkflowExecution, which it leaves pending for the test to answer.
const FakeService = struct {
    requests: std.atomic.Value(u32) = .init(0),
    responses: std.atomic.Value(u32) = .init(0),
    system_info_calls: std.atomic.Value(u32) = .init(0),
    pending: ?GrpcRequest = null,
    pending_set: std.Io.Event = .unset,
    override: GrpcOverride = .{ .handle = handle },

    fn respond(f: *FakeService, req: GrpcRequest, response: GrpcResponse) void {
        _ = f.responses.fetchAdd(1, .monotonic);
        req.respond(testing.allocator, response);
    }

    fn handle(ctx: ?*anyopaque, req: GrpcRequest) void {
        const f: *FakeService = @ptrCast(@alignCast(ctx.?));
        _ = f.requests.fetchAdd(1, .monotonic);
        var arena: std.heap.ArenaAllocator = .init(testing.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        const service = "temporal.api.workflowservice.v1.WorkflowService";
        if (!std.mem.eql(u8, req.service(), service))
            return f.respond(req, .{ .status_code = 12, .message = "unexpected service" });

        const rpc = req.rpc();
        if (std.mem.eql(u8, rpc, "GetSystemInfo")) {
            _ = f.system_info_calls.fetchAdd(1, .monotonic);
            const resp = encode(wfs.GetSystemInfoResponse{ .server_version = "zig-fake" }, a) catch
                return f.respond(req, .{ .status_code = 13, .message = "encode failed" });
            return f.respond(req, .{ .proto = resp, .headers = &.{.{ .key = "x-fake", .value = "1" }} });
        }
        if (std.mem.eql(u8, rpc, "DescribeNamespace")) {
            var r: std.Io.Reader = .fixed(req.proto());
            const in = wfs.DescribeNamespaceRequest.decode(&r, a) catch
                return f.respond(req, .{ .status_code = 3, .message = "bad request" });
            if (!std.mem.eql(u8, in.namespace, "fake")) {
                const message = std.fmt.allocPrint(a, "namespace {s} not found (x-call={s})", .{ in.namespace, req.header("x-call") orelse "" }) catch "oom";
                return f.respond(req, .{ .status_code = 5, .message = message, .details = "\x08\x05" });
            }
            const resp = encode(wfs.DescribeNamespaceResponse{ .namespace_info = .{ .name = "fake" } }, a) catch
                return f.respond(req, .{ .status_code = 13, .message = "encode failed" });
            return f.respond(req, .{ .proto = resp });
        }
        if (std.mem.eql(u8, rpc, "SignalWorkflowExecution")) {
            f.pending = req;
            f.pending_set.set(testing.io);
            return;
        }
        if (std.mem.eql(u8, rpc, "DeleteWorkflowExecution"))
            return f.respond(req, .{ .headers = &.{.{ .key = "bad\nkey", .value = "v" }} });
        f.respond(req, .{ .status_code = 12, .message = "unimplemented" });
    }
};

fn signalCall(conn: Connection, request: []const u8) CallError!void {
    const resp = try conn.call(testing.allocator, testing.io, .workflow, "SignalWorkflowExecution", request, .{}, null);
    testing.allocator.free(resp);
}

test "a gRPC override serves the connection's calls from Zig and answers each once" {
    const rt = try Runtime.init(testing.allocator, .{}, null);
    defer rt.deinit();
    var fake: FakeService = .{};
    fake.override.ctx = &fake;
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    {
        // Nothing listens on this port: every call, including connect's, goes to the fake.
        const conn = try connect(testing.allocator, testing.io, rt, .{
            .target_url = "http://127.0.0.1:1",
            .identity = "test@zig",
            .grpc_override = &fake.override,
        }, null);
        defer conn.deinit();
        try testing.expect(fake.system_info_calls.load(.monotonic) >= 1);

        const info_bytes = try conn.call(testing.allocator, testing.io, .workflow, "GetSystemInfo", try encode(wfs.GetSystemInfoRequest{}, a), .{}, null);
        defer testing.allocator.free(info_bytes);
        var r: std.Io.Reader = .fixed(info_bytes);
        try testing.expectEqualStrings("zig-fake", (try wfs.GetSystemInfoResponse.decode(&r, a)).server_version);

        // The request bytes reach the handler and its response comes back.
        const ns_bytes = try conn.call(testing.allocator, testing.io, .workflow, "DescribeNamespace", try encode(wfs.DescribeNamespaceRequest{ .namespace = "fake" }, a), .{}, null);
        defer testing.allocator.free(ns_bytes);
        r = .fixed(ns_bytes);
        try testing.expectEqualStrings("fake", (try wfs.DescribeNamespaceResponse.decode(&r, a)).namespace_info.?.name);

        // A failure carries the handler's status, message (which saw the call's header) and details.
        var failure: ?RpcFailure = null;
        defer if (failure) |f| f.deinit(testing.allocator);
        try testing.expectError(error.RpcFailed, conn.call(testing.allocator, testing.io, .workflow, "DescribeNamespace", try encode(wfs.DescribeNamespaceRequest{ .namespace = "other" }, a), .{ .metadata = &.{.{ .key = "x-call", .value = "42" }} }, &failure));
        try testing.expectEqual(@as(u32, 5), failure.?.status_code);
        try testing.expectEqualStrings("namespace other not found (x-call=42)", failure.?.message);
        try testing.expectEqualStrings("\x08\x05", failure.?.details);

        // A response header that can't be encoded fails the call with INTERNAL.
        var bad: ?RpcFailure = null;
        defer if (bad) |f| f.deinit(testing.allocator);
        try testing.expectError(error.RpcFailed, conn.call(testing.allocator, testing.io, .workflow, "DeleteWorkflowExecution", "", .{}, &bad));
        try testing.expectEqual(@as(u32, 13), bad.?.status_code);
        try testing.expect(std.mem.indexOf(u8, bad.?.message, "newline") != null);

        // A request answered after the handler returned, from another thread.
        var signal = try testing.io.concurrent(signalCall, .{ conn, try encode(wfs.SignalWorkflowExecutionRequest{ .namespace = "fake" }, a) });
        try fake.pending_set.wait(testing.io);
        fake.respond(fake.pending.?, .{});
        try signal.await(testing.io);
    }
    try testing.expectEqual(fake.requests.load(.monotonic), fake.responses.load(.monotonic));
    try testing.expect(fake.requests.load(.monotonic) >= 6);
}

test "connection options pass a gRPC override to the core" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var core: CoreOptions = undefined;
    try core.init(arena.allocator(), .{ .target_url = "http://localhost:7233", .identity = "id" });
    try testing.expect(core.connection.grpc_override_callback == null);
    var fake: FakeService = .{};
    try core.init(arena.allocator(), .{ .target_url = "http://localhost:7233", .identity = "id", .grpc_override = &fake.override });
    try testing.expect(core.connection.grpc_override_callback == GrpcOverride.onRequest);
    try testing.expectEqual(@as(?*anyopaque, @ptrCast(&fake.override)), core.connection.grpc_override_callback_user_data);
}

test "Retry defaults are the core's default policy" {
    // deps/sdk-core/crates/client/src/retry.rs: RetryOptions::default().
    const r: Options.Retry = .{};
    try testing.expectEqual(@as(u64, 100), r.initial_interval_ms);
    try testing.expectEqual(@as(f64, 0.2), r.randomization_factor);
    try testing.expectEqual(@as(f64, 1.7), r.multiplier);
    try testing.expectEqual(@as(u64, 5_000), r.max_interval_ms);
    try testing.expectEqual(@as(u64, 10_000), r.max_elapsed_time_ms);
    try testing.expectEqual(@as(usize, 10), r.max_retries);
}
