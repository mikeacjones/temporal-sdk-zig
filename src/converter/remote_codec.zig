//! A payload codec that has a codec server do the work: the client side of `codecHandler`,
//! speaking the same wire format as it and as other Temporal codec servers. Re-exported by
//! `converter`.

const std = @import("std");
const codec = @import("codec.zig");
const protojson = @import("../protojson.zig");
const proto = @import("../proto.zig");
const io_tasks = @import("../io_tasks.zig");
const CancelBlockingAllocator = @import("../CancelBlockingAllocator.zig");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const http = std.http;
const Payload = proto.temporal.api.common.v1.Payload;
const Payloads = proto.temporal.api.common.v1.Payloads;
const PayloadCodec = codec.PayloadCodec;
const CodecError = codec.CodecError;

const log = std.log.scoped(.temporal);

/// Options of `RemoteCodec.init`. Everything is copied by `init`; nothing is borrowed afterwards.
pub const RemoteCodecOptions = struct {
    /// The codec server's base URL, "http://..." or "https://...", such as
    /// "https://codec.example.com" or "http://localhost:8081/default". Requests go to
    /// `<endpoint>/encode` and `<endpoint>/decode`. One trailing "/" is ignored.
    /// Required (no default).
    endpoint: []const u8,
    /// Extra headers sent with every request, such as Authorization. Never logged.
    headers: []const http.Header = &.{},
    /// Sent as the `X-Namespace` header with every request, as the Temporal Web UI and CLI
    /// send it, for a codec server that picks its codecs by namespace. Empty sends no such header.
    namespace: []const u8 = "",
    /// Bounds one encode or decode call, from connecting to the end of the response body,
    /// including the one retry. Null sets no limit. Must be positive.
    timeout: ?Io.Duration = .fromSeconds(10),
};

/// A payload codec that has a codec server do the work: POSTs each payload list to the
/// server's /encode or /decode and returns the list the server answers with.
///
/// The body of both requests is ProtoJSON of a `temporal.api.common.v1.Payloads`
/// (`{"payloads":[...]}`) with `content-type: application/json`. Only an answer with status
/// 200 whose body is such a list, holding as many payloads as were sent, succeeds. Anything
/// else is `error.CodecFailed`, logged under the `temporal` scope; the input payloads are
/// never passed through. A transport failure (connecting, writing or reading) is retried
/// once at once, on a new connection, as connections are pooled and a server may close
/// idle ones without notice: before the retry every idle pooled connection is closed. No context (`converter.Context`) is sent.
pub const RemoteCodec = struct {
    state: *State,

    pub const InitError = error{ OutOfMemory, InvalidOptions };

    /// Checks and copies `options`. Makes no network call. `gpa` must be thread-safe and
    /// outlive the codec; `io` is the Io every request runs on (pass the client's): the `io`
    /// that `PayloadCodec.encode` and `decode` are given is not used. With a `timeout`, each
    /// call starts a concurrent task on `io`, so `io` must be able to start one.
    ///
    /// Returns `error.InvalidOptions`, logged naming the option (the endpoint without its user
    /// info), for:
    /// - an endpoint that doesn't parse as a URL (a port out of range included), whose scheme
    ///   is not "http" or "https" in lower case, whose host is missing
    ///   or one std.http.Client refuses (an IPv6 literal, a name with '_'), whose path holds a
    ///   byte that is not visible ASCII, or that has a query or fragment;
    /// - a timeout that is not positive;
    /// - a header name that is empty or holds ':', CR or LF, or a header value or namespace
    ///   that holds CR or LF;
    /// - a header the codec sets itself (content-type, content-length, transfer-encoding, host,
    ///   connection, accept-encoding, x-namespace), or authorization or user-agent given twice.
    ///
    /// A user and password in the endpoint are accepted at any length: the codec sends them as
    /// basic authorization itself, unless `headers` has an authorization header.
    pub fn init(gpa: Allocator, io: Io, options: RemoteCodecOptions) InitError!RemoteCodec {
        const state = try gpa.create(State);
        errdefer gpa.destroy(state);
        state.* = .{
            .gpa = gpa,
            .io = io,
            .allocator = .{ .io = io, .backing = gpa },
            .client = undefined,
            .arena = .init(gpa),
            .encode_url = undefined,
            .decode_url = undefined,
            .encode_uri = undefined,
            .decode_uri = undefined,
            .headers = undefined,
            .timeout = options.timeout,
        };
        errdefer state.arena.deinit();
        try state.configure(options);
        state.client = .{ .allocator = state.allocator.allocator(), .io = io };
        return .{ .state = state };
    }

    /// Closes pooled connections and frees everything. Call after the client and workers
    /// that use the codec are closed; no call may be in flight.
    pub fn deinit(r: *RemoteCodec) void {
        const state = r.state;
        state.client.deinit();
        state.arena.deinit();
        state.gpa.destroy(state);
        r.* = undefined;
    }

    /// The codec to put in `client.Options.codecs`. Valid until `deinit`; it points at the heap
    /// state, so the `RemoteCodec` value itself may be moved or copied.
    pub fn codec(r: *const RemoteCodec) PayloadCodec {
        return .{ .ptr = r.state, .vtable = &vtable };
    }

    const vtable: PayloadCodec.VTable = .{ .encode = encode, .decode = decode };

    fn encode(ptr: *anyopaque, arena: Allocator, _: Io, payloads: []const Payload) CodecError![]Payload {
        const state: *State = @ptrCast(@alignCast(ptr));
        return state.call(arena, .encode, payloads);
    }

    fn decode(ptr: *anyopaque, arena: Allocator, _: Io, payloads: []const Payload) CodecError![]Payload {
        const state: *State = @ptrCast(@alignCast(ptr));
        return state.call(arena, .decode, payloads);
    }
};

/// Headers the codec sets itself, which `RemoteCodecOptions.headers` may not name.
const reserved_headers = [_][]const u8{ "content-type", "content-length", "transfer-encoding", "host", "connection", "accept-encoding", "x-namespace" };

/// The longest timeout used; a longer one is cut to it. About 292 billion years.
const max_timeout: Io.Duration = .fromSeconds(std.math.maxInt(i64));

/// Formats `endpoint` for a log, escaped, with any user info before an '@' in its authority
/// shown as "***", so a password never reaches the log. Works on text that doesn't parse as
/// a URL too.
fn redacted(endpoint: []const u8) Redacted {
    return .{ .endpoint = endpoint };
}

const Redacted = struct {
    endpoint: []const u8,

    pub fn format(r: Redacted, w: *Io.Writer) Io.Writer.Error!void {
        const e = r.endpoint;
        const start = if (std.mem.indexOf(u8, e, "://")) |i| i + 3 else 0;
        // Through the last '@' anywhere: a password may hold an unencoded '/', '?' or '#', and
        // hiding a path's '@' with it costs a log nothing.
        const at = std.mem.lastIndexOfScalar(u8, e[start..], '@') orelse
            return w.print("{f}", .{std.zig.fmtString(e)});
        return w.print("{f}***{f}", .{ std.zig.fmtString(e[0..start]), std.zig.fmtString(e[start + at ..]) });
    }
};

fn reject(comptime fmt: []const u8, args: anytype) error{InvalidOptions} {
    log.warn("converter.RemoteCodec.init: " ++ fmt, args);
    return error.InvalidOptions;
}

const Op = enum {
    encode,
    decode,

    fn verb(op: Op) []const u8 {
        return switch (op) {
            .encode => "encoding",
            .decode => "decoding",
        };
    }
};

/// Immutable after `init`, so calls on any number of threads share it. Never moves: the
/// `http.Client`'s allocator points at `allocator`.
const State = struct {
    gpa: Allocator,
    io: Io,
    /// Wraps `gpa` for the `http.Client`, whose allocations happen inside the exchange.
    allocator: CancelBlockingAllocator,
    client: http.Client,
    /// Holds the copied options, URLs and headers.
    arena: std.heap.ArenaAllocator,
    /// The URLs as logged: without the endpoint's user and password.
    encode_url: []const u8,
    decode_url: []const u8,
    encode_uri: std.Uri,
    decode_uri: std.Uri,
    /// The user's headers other than authorization and user-agent, then `x-namespace` when set.
    headers: []const http.Header,
    /// The user's authorization and user-agent headers replace the ones std.http.Client would
    /// send (basic authorization from the endpoint's user and password, its own agent), so
    /// neither is sent twice.
    authorization: http.Client.Request.Headers.Value = .default,
    user_agent: http.Client.Request.Headers.Value = .default,
    /// Whether the endpoint is https, whose certificate checks need the current time.
    tls: bool = false,
    /// Held shared across `client.request` on https calls, where std's TLS handshake reads
    /// `client.now` without a lock, and exclusively only by `refreshCertificateClock`, through
    /// a try-lock, to write it.
    handshake_lock: Io.RwLock = .init,
    timeout: ?Io.Duration,

    fn configure(s: *State, options: RemoteCodecOptions) (error{ OutOfMemory, InvalidOptions })!void {
        const arena = s.arena.allocator();
        const endpoint = options.endpoint;
        const uri = std.Uri.parse(endpoint) catch |err|
            return reject("RemoteCodecOptions.endpoint \"{f}\" is not a URL ({t}); give the codec server's base URL, such as \"https://codec.example.com\"", .{ redacted(endpoint), err });
        // std.http.Client matches the scheme exactly, so "HTTP://" would fail every call.
        if (!std.mem.eql(u8, uri.scheme, "http") and !std.mem.eql(u8, uri.scheme, "https"))
            return reject("RemoteCodecOptions.endpoint \"{f}\" has the scheme \"{f}\"; use an http:// or https:// URL, in lower case", .{ redacted(endpoint), std.zig.fmtString(uri.scheme) });
        s.tls = std.mem.eql(u8, uri.scheme, "https");
        const has_host = if (uri.host) |h| !h.isEmpty() else false;
        if (!has_host)
            return reject("RemoteCodecOptions.endpoint \"{f}\" has no host; give the codec server's base URL, such as \"https://codec.example.com\"", .{redacted(endpoint)});
        // The check std.http.Client makes on every request, so a host it would refuse (an IPv6
        // literal, a name with '_') is refused here instead of failing every call.
        var host_buffer: [Io.net.HostName.max_len]u8 = undefined;
        _ = Io.net.HostName.fromUri(uri, &host_buffer) catch |err|
            return reject("RemoteCodecOptions.endpoint \"{f}\" has a host std.http.Client can't connect to ({t}); use a host name of letters, digits, '-' and '.', or an IPv4 address (IPv6 literals aren't supported)", .{ redacted(endpoint), err });
        // std.http.Client writes the path into the request line as it is.
        const path = switch (uri.path) {
            .raw, .percent_encoded => |p| p,
        };
        for (path) |c| if (c < 0x21 or c > 0x7e)
            return reject("RemoteCodecOptions.endpoint \"{f}\" has a space, control character or non-ASCII byte in its path; percent-encode it", .{redacted(endpoint)});
        if (uri.query != null or uri.fragment != null)
            return reject("RemoteCodecOptions.endpoint \"{f}\" has a query or fragment; give the base URL that /encode and /decode are appended to", .{redacted(endpoint)});
        if (options.timeout) |t| {
            if (t.nanoseconds <= 0)
                return reject("RemoteCodecOptions.timeout must be positive, not {f}; set it to null for no limit", .{t});
            // A deadline past this overflows when added to the clock.
            s.timeout = .fromNanoseconds(@min(t.nanoseconds, max_timeout.nanoseconds));
        }
        for (options.headers) |h| {
            if (h.name.len == 0 or std.mem.indexOfAny(u8, h.name, ":\r\n") != null)
                return reject("RemoteCodecOptions.headers has the header name \"{f}\"; a name must be non-empty and hold no ':', CR or LF", .{std.zig.fmtString(h.name)});
            if (std.mem.indexOfAny(u8, h.value, "\r\n") != null)
                return reject("RemoteCodecOptions.headers: the value of header \"{s}\" holds CR or LF", .{h.name});
            for (reserved_headers) |reserved| if (std.ascii.eqlIgnoreCase(h.name, reserved)) {
                if (std.mem.eql(u8, reserved, "x-namespace"))
                    return reject("RemoteCodecOptions.headers names \"{s}\"; set RemoteCodecOptions.namespace instead", .{h.name});
                return reject("RemoteCodecOptions.headers names \"{s}\", which the codec sets itself; remove it", .{h.name});
            };
        }
        for ([_][]const u8{ "authorization", "user-agent" }) |single| {
            var count: usize = 0;
            for (options.headers) |h| count += @intFromBool(std.ascii.eqlIgnoreCase(h.name, single));
            if (count > 1) return reject("RemoteCodecOptions.headers names \"{s}\" {d} times; give it once", .{ single, count });
        }
        if (std.mem.indexOfAny(u8, options.namespace, "\r\n") != null)
            return reject("RemoteCodecOptions.namespace \"{f}\" holds CR or LF", .{std.zig.fmtString(options.namespace)});

        const base = if (std.mem.endsWith(u8, endpoint, "/")) endpoint[0 .. endpoint.len - 1] else endpoint;
        // The endpoint parsed above, so these parse too.
        s.encode_uri = std.Uri.parse(try std.mem.concat(arena, u8, &.{ base, "/encode" })) catch unreachable;
        s.decode_uri = std.Uri.parse(try std.mem.concat(arena, u8, &.{ base, "/decode" })) catch unreachable;
        // The codec writes basic authorization from the endpoint's user and password itself, and
        // std.http.Client never sees them: it would write them through a fixed 511-byte buffer
        // that panics on a longer pair. A user's authorization header replaces this one below.
        if (uri.user != null or uri.password != null) {
            const user = if (uri.user) |u| try u.toRawMaybeAlloc(arena) else "";
            const password = if (uri.password) |pw| try pw.toRawMaybeAlloc(arena) else "";
            const pair = try std.mem.concat(arena, u8, &.{ user, ":", password });
            const encoder = std.base64.standard.Encoder;
            const value = try arena.alloc(u8, "Basic ".len + encoder.calcSize(pair.len));
            @memcpy(value[0.."Basic ".len], "Basic ");
            _ = encoder.encode(value["Basic ".len..], pair);
            s.authorization = .{ .override = value };
        }
        for ([_]*std.Uri{ &s.encode_uri, &s.decode_uri }) |u| {
            u.user = null;
            u.password = null;
        }
        const log_flags: std.Uri.Format.Flags = .{ .scheme = true, .authority = true, .path = true };
        s.encode_url = try std.fmt.allocPrint(arena, "{f}", .{s.encode_uri.fmt(log_flags)});
        s.decode_url = try std.fmt.allocPrint(arena, "{f}", .{s.decode_uri.fmt(log_flags)});

        var headers: std.ArrayList(http.Header) = try .initCapacity(arena, options.headers.len + 1);
        for (options.headers) |h| {
            const value = try arena.dupe(u8, h.value);
            if (std.ascii.eqlIgnoreCase(h.name, "authorization")) {
                s.authorization = .{ .override = value };
            } else if (std.ascii.eqlIgnoreCase(h.name, "user-agent")) {
                s.user_agent = .{ .override = value };
            } else headers.appendAssumeCapacity(.{ .name = try arena.dupe(u8, h.name), .value = value });
        }
        if (options.namespace.len > 0)
            headers.appendAssumeCapacity(.{ .name = "x-namespace", .value = try arena.dupe(u8, options.namespace) });
        s.headers = headers.items;
    }

    /// std.http.Client checks every server certificate as of `client.now`, which it reads once,
    /// on its first https request, and never again: in a codec that lives as long as a worker,
    /// a certificate the server renewed later would be refused as not yet valid, and one that
    /// expired later accepted. So each https attempt moves `client.now` to the current time when
    /// it is a minute or more off. `client.now` stays null until std's first request has loaded
    /// the CA bundle (null is how std knows to load it), and is never set back to null, which a
    /// handshake under way would assert against. A handshake reads `client.now` without the
    /// lock, so the write happens only with `handshake_lock` held exclusively, which no
    /// handshake of this codec can run under. When another call holds it (a slow connect, say)
    /// the refresh is skipped rather than waited for; a later call makes it.
    fn refreshCertificateClock(s: *State) void {
        if (!s.tls) return;
        const ca_lock = &s.client.ca_bundle_lock;
        const now = Io.Clock.real.now(s.io);
        {
            ca_lock.lockSharedUncancelable(s.io);
            defer ca_lock.unlockShared(s.io);
            const as_of = s.client.now orelse return;
            if (@abs(as_of.durationTo(now).nanoseconds) < std.time.ns_per_min) return;
        }
        if (!s.handshake_lock.tryLock(s.io)) return;
        defer s.handshake_lock.unlock(s.io);
        // std writes `client.now` (on its first https request) under the CA bundle lock.
        ca_lock.lockUncancelable(s.io);
        defer ca_lock.unlock(s.io);
        if (s.client.now != null) s.client.now = now;
    }

    /// Closes every pooled connection not in use. Those in use by other calls stay open.
    fn closeIdleConnections(s: *State) void {
        const pool = &s.client.connection_pool;
        pool.mutex.lockUncancelable(s.io);
        defer pool.mutex.unlock(s.io);
        while (pool.free.popFirst()) |node| {
            const connection: *http.Client.Connection = @alignCast(@fieldParentPtr("pool_node", node));
            pool.free_len -= 1;
            connection.destroy(s.io);
        }
    }

    fn url(s: *const State, op: Op) []const u8 {
        return switch (op) {
            .encode => s.encode_url,
            .decode => s.decode_url,
        };
    }

    fn call(s: *State, call_arena: Allocator, op: Op, payloads: []const Payload) CodecError![]Payload {
        if (payloads.len == 0) return call_arena.alloc(Payload, 0);
        // Every allocation of the call blocks cancels, so a Debug allocator waiting on the Io
        // can't swallow a cancel meant for the exchange. The memory is plain arena memory.
        var blocking: CancelBlockingAllocator = .{ .io = s.io, .backing = call_arena };
        const arena = blocking.allocator();

        const body = try protojson.encode(arena, Payloads{ .payloads = .fromOwnedSlice(try arena.dupe(Payload, payloads)) });
        var job: Job = .{ .state = s, .arena = arena, .uri = switch (op) {
            .encode => s.encode_uri,
            .decode => s.decode_uri,
        }, .body = body };
        if (s.timeout) |timeout| {
            try s.runWithTimeout(&job, op, timeout);
        } else job.run();
        const exchange = try job.result;

        switch (exchange) {
            .transport => |err| {
                log.warn("converter.RemoteCodec: {s} payloads with {s} failed: {t} (also on the one retry)", .{ op.verb(), s.url(op), err });
                return error.CodecFailed;
            },
            .response => |response| {
                if (response.status != .ok) {
                    const preview = response.body[0..@min(response.body.len, 256)];
                    log.warn("converter.RemoteCodec: {s} payloads with {s} failed: the codec server answered HTTP {d}: \"{f}\"", .{ op.verb(), s.url(op), @backingInt(response.status), std.zig.fmtString(preview) });
                    return error.CodecFailed;
                }
                const list = protojson.decode(Payloads, arena, response.body) catch |err| switch (err) {
                    error.OutOfMemory => return error.OutOfMemory,
                    error.InvalidProtoJson => {
                        log.warn("converter.RemoteCodec: {s} payloads with {s} failed: the codec server's 200 response is not ProtoJSON of a payload list ({{\"payloads\":[...]}})", .{ op.verb(), s.url(op) });
                        return error.CodecFailed;
                    },
                };
                if (list.payloads.items.len != payloads.len) {
                    log.warn("converter.RemoteCodec: {s} payloads with {s} failed: the codec server answered with {d} payloads for the {d} sent", .{ op.verb(), s.url(op), list.payloads.items.len, payloads.len });
                    return error.CodecFailed;
                }
                return list.payloads.items;
            },
        }
    }

    /// Runs `job` as a concurrent task and waits for it at most `timeout`. The task is always
    /// awaited or canceled before this returns, so it never outlives the call.
    fn runWithTimeout(s: *State, job: *Job, op: Op, timeout: Io.Duration) CodecError!void {
        const io = s.io;
        // A cancel swallowed while the task starts (Debug allocator, see
        // CancelBlockingAllocator) would leave the task running; keep it pending instead.
        const prev = io.swapCancelProtection(.blocked);
        const started = io_tasks.concurrent(io, Job.run, .{job});
        _ = io.swapCancelProtection(prev);
        var future = started catch |err| switch (err) {
            error.ConcurrencyUnavailable => {
                log.warn("converter.RemoteCodec: {s} payloads with {s} failed: RemoteCodecOptions.timeout needs a concurrent task, and the Io refused one (error.ConcurrencyUnavailable); give the codec an Io that can start one, or set timeout to null", .{ op.verb(), s.url(op) });
                return error.CodecFailed;
            },
        };
        job.done.waitTimeout(io, .{ .duration = .{ .raw = timeout, .clock = .awake } }) catch |err| {
            job.gave_up.store(true, .release);
            future.cancel(io);
            switch (err) {
                error.Canceled => return error.Canceled,
                error.Timeout => {
                    // The exchange may have finished just as the wait timed out.
                    if (job.result) |exchange| if (exchange == .response) return else {} else |_| {}
                    log.warn("converter.RemoteCodec: {s} payloads with {s} failed: no answer within RemoteCodecOptions.timeout ({f})", .{ op.verb(), s.url(op), timeout });
                    return error.CodecFailed;
                },
            }
        };
        future.await(io);
    }

    /// One POST of `body` to `uri`. `OutOfMemory` and `Canceled` are errors; any other
    /// failure is a `.transport` exchange.
    /// `gave_up` is set by a caller that stopped waiting, before it cancels the task.
    fn attempt(s: *State, arena: Allocator, uri: std.Uri, body: []const u8, gave_up: *const std.atomic.Value(bool)) error{ OutOfMemory, Canceled }!Exchange {
        s.refreshCertificateClock();
        if (s.tls) s.handshake_lock.lockSharedUncancelable(s.io);
        const requested = s.client.request(.POST, uri, .{
            .redirect_behavior = .unhandled,
            .headers = .{
                .content_type = .{ .override = "application/json" },
                .authorization = s.authorization,
                .user_agent = s.user_agent,
            },
            .extra_headers = s.headers,
        });
        if (s.tls) s.handshake_lock.unlockShared(s.io);
        // std's connect allocates its tasks from the Io's own allocator, which in a Debug build
        // (Zig 0.17.1's DebugAllocator) can swallow the cancel that a timeout or the caller's
        // cancel sends; every later read and write would then wait without a limit. The caller
        // sets `gave_up` before it cancels, so checking it here ends the attempt either way.
        // A cancel swallowed inside std's own TLS handshake can still leave that handshake
        // uncancelable in such a build.
        var request = requested catch |err| {
            if (gave_up.load(.acquire)) return error.Canceled;
            return transportFailure(err, null);
        };
        defer request.deinit();
        if (gave_up.load(.acquire)) {
            if (request.connection) |c| c.closing = true;
            return error.Canceled;
        }
        var response_body: Io.Writer.Allocating = .init(arena);
        const status = send(&request, arena, body, &response_body.writer) catch |err| {
            // `Request.deinit` would put a connection that failed while sending back into the
            // pool, and would read the rest of a response body that failed, without a limit.
            if (request.connection) |c| c.closing = true;
            return transportFailure(err, request.connection);
        };
        return .{ .response = .{ .status = status, .body = response_body.written() } };
    }
};

/// What came back from the codec server.
const Exchange = union(enum) {
    response: struct { status: http.Status, body: []const u8 },
    /// The cause of the last attempt's failure, for the log.
    transport: anyerror,
};

/// One encode or decode exchange, up to two attempts. Owned by the calling `call`.
const Job = struct {
    state: *State,
    arena: Allocator,
    uri: std.Uri,
    body: []const u8,
    result: error{ OutOfMemory, Canceled }!Exchange = error.Canceled,
    done: Io.Event = .unset,
    /// Set by the caller before it cancels the task: no retry after that.
    gave_up: std.atomic.Value(bool) = .init(false),

    fn run(job: *Job) void {
        defer job.done.set(job.state.io);
        job.result = job.exchange();
    }

    fn exchange(job: *Job) error{ OutOfMemory, Canceled }!Exchange {
        const first = try job.state.attempt(job.arena, job.uri, job.body, &job.gave_up);
        if (first == .response or job.gave_up.load(.acquire)) return first;
        // The pool would hand the retry the next idle connection, which a server that closed
        // the first one has most likely closed too.
        job.state.closeIdleConnections();
        return job.state.attempt(job.arena, job.uri, job.body, &job.gave_up);
    }
};

/// `std.http.Client.fetch`'s exchange, returning its errors with the connection still held so
/// the caller can mark it closing, and without its panic on a failed read of a body with a
/// content length.
fn send(request: *http.Client.Request, arena: Allocator, body: []const u8, out: *Io.Writer) !http.Status {
    request.transfer_encoding = .{ .content_length = body.len };
    var body_writer = try request.sendBodyUnflushed(&.{});
    try body_writer.writer.writeAll(body);
    try body_writer.end();
    try request.connection.?.flush();

    var response = try request.receiveHead(&.{});
    const decompress_buffer: []u8 = switch (response.head.content_encoding) {
        .identity => &.{},
        .zstd => try arena.alloc(u8, std.compress.zstd.default_window_len),
        .deflate, .gzip => try arena.alloc(u8, std.compress.flate.max_window_len),
        .compress => return error.UnsupportedCompressionMethod,
    };
    var transfer_buffer: [64]u8 = undefined;
    var decompress: http.Decompress = undefined;
    const reader = response.readerDecompressing(&transfer_buffer, &decompress, decompress_buffer);
    _ = reader.streamRemaining(out) catch |err| switch (err) {
        error.ReadFailed => return response.bodyErr() orelse error.ReadFailed,
        error.WriteFailed => return error.OutOfMemory, // `out` is an allocating writer
    };
    return response.head.status;
}

/// Sorts a failed attempt's error: a cancel or allocation failure is returned, and anything
/// else is a transport failure named by its cause on the connection where there is one.
fn transportFailure(err: anyerror, connection: ?*http.Client.Connection) error{ OutOfMemory, Canceled }!Exchange {
    switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        error.Canceled => return error.Canceled,
        else => {},
    }
    const cause: ?anyerror = if (connection) |c| switch (err) {
        error.ReadFailed => c.stream_reader.err,
        error.WriteFailed => c.stream_writer.err,
        else => null,
    } else null;
    if (cause) |e| {
        if (e == error.Canceled) return error.Canceled;
        return .{ .transport = e };
    }
    return .{ .transport = err };
}

// ---------------------------------------------------------------- tests

const testing = std.testing;
const converter = @import("../converter.zig");
const codec_handler = @import("codec_handler.zig");

/// A local HTTP server for the tests, on 127.0.0.1 at a port of its own. It records what each
/// request carried and answers as its `mode` says.
const TestServer = struct {
    io: Io,
    listener: Io.net.Server,
    mode: Mode,
    codecs: []const PayloadCodec = &.{},
    serving: Io.Future(void) = undefined,
    mutex: Io.Mutex = .init,
    /// Guarded by `mutex`, as is everything in `record`.
    record_arena: std.heap.ArenaAllocator,
    requests: std.ArrayList(Seen) = .empty,
    connections: usize = 0,
    /// Set when a request has been read, for the modes that never answer.
    received: Io.Event = .unset,
    /// Never set: what the silent modes wait on until the test stops the server.
    never: Io.Event = .unset,
    endpoint_buf: [64]u8 = undefined,

    const Mode = enum {
        /// `codecHandler` over `codecs`.
        handler,
        /// `codecHandler`, then closes the connection without saying so.
        close_after_one,
        status_500,
        not_json,
        fewer,
        /// Reads the request and never answers.
        silent,
        /// Sends the head of a 200 with a body it never finishes.
        silent_body,
    };

    const Seen = struct {
        target: []const u8,
        authorization: ?[]const u8 = null,
        authorizations: usize = 0,
        user_agent: ?[]const u8 = null,
        namespace: ?[]const u8 = null,
        content_type: ?[]const u8 = null,
    };

    fn start(s: *TestServer, mode: Mode, codecs: []const PayloadCodec) !void {
        const address = try Io.net.IpAddress.parse("127.0.0.1", 0);
        s.* = .{
            .io = testing.io,
            .listener = try address.listen(testing.io, .{ .reuse_address = true }),
            .mode = mode,
            .codecs = codecs,
            .record_arena = .init(testing.allocator),
        };
        errdefer s.listener.deinit(s.io);
        s.serving = try io_tasks.concurrent(s.io, serve, .{s});
    }

    fn stop(s: *TestServer) void {
        s.serving.cancel(s.io);
        s.listener.deinit(s.io);
        s.record_arena.deinit();
    }

    fn endpoint(s: *TestServer, path: []const u8) []const u8 {
        return std.fmt.bufPrint(&s.endpoint_buf, "http://127.0.0.1:{d}{s}", .{ s.listener.socket.address.getPort(), path }) catch unreachable;
    }

    fn snapshot(s: *TestServer) struct { requests: []const Seen, connections: usize } {
        s.mutex.lockUncancelable(s.io);
        defer s.mutex.unlock(s.io);
        return .{ .requests = s.requests.items, .connections = s.connections };
    }

    fn serve(s: *TestServer) void {
        var group: Io.Group = .init;
        defer group.cancel(s.io);
        while (true) {
            const stream = s.listener.accept(s.io) catch return;
            group.concurrent(s.io, serveConnection, .{ s, stream }) catch {
                stream.close(s.io);
                return;
            };
        }
    }

    fn serveConnection(s: *TestServer, stream: Io.net.Stream) Io.Cancelable!void {
        defer stream.close(s.io);
        {
            s.mutex.lockUncancelable(s.io);
            defer s.mutex.unlock(s.io);
            s.connections += 1;
        }
        var recv_buffer: [8192]u8 = undefined;
        var send_buffer: [4096]u8 = undefined;
        var conn_reader = stream.reader(s.io, &recv_buffer);
        var conn_writer = stream.writer(s.io, &send_buffer);
        var server = http.Server.init(&conn_reader.interface, &conn_writer.interface);
        while (server.reader.state == .ready) {
            var request = server.receiveHead() catch return;
            var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
            defer arena_state.deinit();
            s.handle(arena_state.allocator(), &request) catch return;
            if (s.mode == .close_after_one) return;
        }
    }

    fn handle(s: *TestServer, arena: Allocator, request: *http.Server.Request) !void {
        {
            s.mutex.lockUncancelable(s.io);
            defer s.mutex.unlock(s.io);
            const a = s.record_arena.allocator();
            var seen: Seen = .{ .target = try a.dupe(u8, request.head.target) };
            if (request.head.content_type) |v| seen.content_type = try a.dupe(u8, v);
            var it = request.iterateHeaders();
            while (it.next()) |h| {
                if (std.ascii.eqlIgnoreCase(h.name, "authorization")) {
                    seen.authorization = try a.dupe(u8, h.value);
                    seen.authorizations += 1;
                }
                if (std.ascii.eqlIgnoreCase(h.name, "user-agent")) seen.user_agent = try a.dupe(u8, h.value);
                if (std.ascii.eqlIgnoreCase(h.name, "x-namespace")) seen.namespace = try a.dupe(u8, h.value);
            }
            try s.requests.append(a, seen);
        }
        switch (s.mode) {
            .handler, .close_after_one => return codec_handler.codecHandler(s.codecs, .{}).handle(arena, s.io, request),
            else => {},
        }
        var transfer: [4096]u8 = undefined;
        _ = try (try request.readerExpectContinue(&transfer)).allocRemaining(arena, .unlimited);
        switch (s.mode) {
            .handler, .close_after_one => unreachable,
            .status_500 => try request.respond("the codec failed", .{ .status = .internal_server_error }),
            .not_json => try request.respond("not json", .{}),
            .fewer => try request.respond("{\"payloads\":[]}", .{}),
            .silent => {
                s.received.set(s.io);
                try s.never.wait(s.io);
            },
            .silent_body => {
                try request.server.out.writeAll("HTTP/1.1 200 OK\r\ncontent-length: 100\r\n\r\n{\"payl");
                try request.server.out.flush();
                s.received.set(s.io);
                try s.never.wait(s.io);
            },
        }
    }
};

fn testPayloads(arena: Allocator) ![]Payload {
    return (try converter.toPayloads(arena, .{ @as([]const u8, "hello"), @as(i64, 42) })).items;
}

fn waitReceived(s: *TestServer) !void {
    s.received.waitTimeout(testing.io, .{ .duration = .{ .raw = .fromSeconds(60), .clock = .awake } }) catch |err| {
        std.debug.print("the test server did not receive a request within 60 s: {t}\n", .{err});
        return error.TestUnexpectedResult;
    };
}

test "a remote codec round-trips payloads through codecHandler" {
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const zlib: codec.ZlibCodec = .{ .always_encode = true };
    var server: TestServer = undefined;
    try server.start(.handler, &.{zlib.codec()});
    defer server.stop();

    var remote = try RemoteCodec.init(testing.allocator, testing.io, .{ .endpoint = server.endpoint("/codec/") });
    defer remote.deinit();
    const c = remote.codec();

    const original = try testPayloads(arena);
    const encoded = try c.encode(arena, testing.io, original);
    try testing.expectEqual(@as(usize, 2), encoded.len);
    for (encoded) |p| try testing.expectEqualStrings("binary/zlib", converter.encodingOf(p).?);
    const decoded = try c.decode(arena, testing.io, encoded);
    const values = try converter.fromPayloads(struct { []const u8, i64 }, arena, decoded);
    try testing.expectEqualStrings("hello", values[0]);
    try testing.expectEqual(@as(i64, 42), values[1]);

    // A second call reuses the pooled connection.
    _ = try c.encode(arena, testing.io, original);
    const seen = server.snapshot();
    try testing.expectEqual(@as(usize, 3), seen.requests.len);
    try testing.expectEqual(@as(usize, 1), seen.connections);
}

test "a remote codec sends its headers, the namespace and the content type" {
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const zlib: codec.ZlibCodec = .{ .always_encode = true };
    var server: TestServer = undefined;
    try server.start(.handler, &.{zlib.codec()});
    defer server.stop();

    {
        // The options' memory may go away after init.
        const token = try testing.allocator.dupe(u8, "Bearer codec-token");
        const namespace = try testing.allocator.dupe(u8, "orders");
        var remote = RemoteCodec.init(testing.allocator, testing.io, .{
            .endpoint = server.endpoint("/tenant-a"),
            .headers = &.{.{ .name = "Authorization", .value = token }},
            .namespace = namespace,
        }) catch |err| {
            testing.allocator.free(token);
            testing.allocator.free(namespace);
            return err;
        };
        defer remote.deinit();
        @memset(token, 'x');
        @memset(namespace, 'x');
        testing.allocator.free(token);
        testing.allocator.free(namespace);
        _ = try remote.codec().decode(arena, testing.io, try remote.codec().encode(arena, testing.io, try testPayloads(arena)));
    }
    var no_namespace = try RemoteCodec.init(testing.allocator, testing.io, .{ .endpoint = server.endpoint("/tenant-b") });
    defer no_namespace.deinit();
    _ = try no_namespace.codec().encode(arena, testing.io, try testPayloads(arena));

    const seen = server.snapshot().requests;
    try testing.expectEqual(@as(usize, 3), seen.len);
    try testing.expectEqualStrings("/tenant-a/encode", seen[0].target);
    try testing.expectEqualStrings("/tenant-a/decode", seen[1].target);
    for (seen[0..2]) |r| {
        try testing.expectEqualStrings("Bearer codec-token", r.authorization.?);
        try testing.expectEqualStrings("orders", r.namespace.?);
        try testing.expectEqualStrings("application/json", r.content_type.?);
    }
    try testing.expectEqualStrings("/tenant-b/encode", seen[2].target);
    try testing.expectEqual(@as(?[]const u8, null), seen[2].namespace);
    try testing.expectEqual(@as(?[]const u8, null), seen[2].authorization);
    try testing.expectEqualStrings("application/json", seen[2].content_type.?);
}

test "a remote codec fails on an answer other than 200, a malformed body, or a wrong count" {
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const payloads = try testPayloads(arena);

    {
        // codecHandler answers 400 for payloads its zlib codec can't decode.
        const zlib: codec.ZlibCodec = .{};
        var server: TestServer = undefined;
        try server.start(.handler, &.{zlib.codec()});
        defer server.stop();
        var remote = try RemoteCodec.init(testing.allocator, testing.io, .{ .endpoint = server.endpoint("") });
        defer remote.deinit();
        var bad: Payload = .{ .data = "not zlib" };
        try bad.metadata.append(arena, .{ .key = "encoding", .value = "binary/zlib" });
        try testing.expectError(error.CodecFailed, remote.codec().decode(arena, testing.io, &.{bad}));
    }
    for ([_]TestServer.Mode{ .status_500, .not_json, .fewer }) |mode| {
        var server: TestServer = undefined;
        try server.start(mode, &.{});
        defer server.stop();
        var remote = try RemoteCodec.init(testing.allocator, testing.io, .{ .endpoint = server.endpoint("") });
        defer remote.deinit();
        try testing.expectError(error.CodecFailed, remote.codec().encode(arena, testing.io, payloads));
        try testing.expectError(error.CodecFailed, remote.codec().decode(arena, testing.io, payloads));
        // No retry for an answer.
        try testing.expectEqual(@as(usize, 2), server.snapshot().requests.len);
    }
}

test "a remote codec gives up after its timeout on a server that doesn't answer" {
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    for ([_]TestServer.Mode{ .silent, .silent_body }) |mode| {
        var server: TestServer = undefined;
        try server.start(mode, &.{});
        defer server.stop();
        var remote = try RemoteCodec.init(testing.allocator, testing.io, .{ .endpoint = server.endpoint(""), .timeout = .fromMilliseconds(200) });
        defer remote.deinit();
        try testing.expectError(error.CodecFailed, remote.codec().encode(arena, testing.io, try testPayloads(arena)));
        // A timeout is not retried.
        try testing.expectEqual(@as(usize, 1), server.snapshot().requests.len);
    }
}

fn encodeTask(c: PayloadCodec, arena: Allocator, payloads: []const Payload) CodecError![]Payload {
    return c.encode(arena, testing.io, payloads);
}

test "canceling the caller of a remote codec returns error.Canceled" {
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const payloads = try testPayloads(arena);
    for ([_]?Io.Duration{ null, .fromSeconds(60) }) |timeout| {
        for ([_]TestServer.Mode{ .silent, .silent_body }) |mode| {
            var server: TestServer = undefined;
            try server.start(mode, &.{});
            defer server.stop();
            var remote = try RemoteCodec.init(testing.allocator, testing.io, .{ .endpoint = server.endpoint(""), .timeout = timeout });
            defer remote.deinit();
            var call_arena: std.heap.ArenaAllocator = .init(testing.allocator);
            defer call_arena.deinit();
            var future = try io_tasks.concurrent(testing.io, encodeTask, .{ remote.codec(), call_arena.allocator(), payloads });
            waitReceived(&server) catch |err| {
                _ = future.cancel(testing.io) catch {};
                return err;
            };
            try testing.expectError(error.Canceled, future.cancel(testing.io));
            // Canceled, not retried.
            try testing.expectEqual(@as(usize, 1), server.snapshot().requests.len);
        }
    }
}

test "a remote codec fails when the connection is refused" {
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    // A port that was just free: listen on it, then close it.
    const address = try Io.net.IpAddress.parse("127.0.0.1", 0);
    var listener = try address.listen(testing.io, .{});
    var buf: [64]u8 = undefined;
    const endpoint = try std.fmt.bufPrint(&buf, "http://127.0.0.1:{d}", .{listener.socket.address.getPort()});
    listener.deinit(testing.io);

    for ([_]?Io.Duration{ null, .fromSeconds(60) }) |timeout| {
        var remote = try RemoteCodec.init(testing.allocator, testing.io, .{ .endpoint = endpoint, .timeout = timeout });
        defer remote.deinit();
        try testing.expectError(error.CodecFailed, remote.codec().encode(arena, testing.io, try testPayloads(arena)));
    }
}

test "a remote codec retries once on a pooled connection the server closed" {
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const zlib: codec.ZlibCodec = .{ .always_encode = true };
    var server: TestServer = undefined;
    try server.start(.close_after_one, &.{zlib.codec()});
    defer server.stop();
    var remote = try RemoteCodec.init(testing.allocator, testing.io, .{ .endpoint = server.endpoint("") });
    defer remote.deinit();

    const payloads = try testPayloads(arena);
    for (0..3) |_| {
        const encoded = try remote.codec().encode(arena, testing.io, payloads);
        try testing.expectEqualStrings("binary/zlib", converter.encodingOf(encoded[0]).?);
    }
    try testing.expectEqual(@as(usize, 3), server.snapshot().connections);
}

test "a remote codec's authorization and user-agent headers replace std's, so each is sent once" {
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const zlib: codec.ZlibCodec = .{ .always_encode = true };
    var server: TestServer = undefined;
    try server.start(.handler, &.{zlib.codec()});
    defer server.stop();

    var buf: [96]u8 = undefined;
    const port = server.listener.socket.address.getPort();
    const with_user = try std.fmt.bufPrint(&buf, "http://user:s3cret@127.0.0.1:{d}/ns", .{port});
    var remote = try RemoteCodec.init(testing.allocator, testing.io, .{
        .endpoint = with_user,
        .headers = &.{ .{ .name = "Authorization", .value = "Bearer codec-token" }, .{ .name = "User-Agent", .value = "orders-worker" } },
        // The longest timeout is cut to one the clock can add.
        .timeout = .{ .nanoseconds = std.math.maxInt(i96) },
    });
    defer remote.deinit();
    _ = try remote.codec().encode(arena, testing.io, try testPayloads(arena));

    var plain = try RemoteCodec.init(testing.allocator, testing.io, .{ .endpoint = with_user });
    defer plain.deinit();
    _ = try plain.codec().encode(arena, testing.io, try testPayloads(arena));

    const seen = server.snapshot().requests;
    try testing.expectEqual(@as(usize, 2), seen.len);
    try testing.expectEqual(@as(usize, 1), seen[0].authorizations);
    try testing.expectEqualStrings("Bearer codec-token", seen[0].authorization.?);
    try testing.expectEqualStrings("orders-worker", seen[0].user_agent.?);
    // Without the header, the endpoint's user and password go as basic authorization.
    try testing.expectEqual(@as(usize, 1), seen[1].authorizations);
    try testing.expectEqualStrings("Basic dXNlcjpzM2NyZXQ=", seen[1].authorization.?); // user:s3cret
}

test "a remote codec makes no request for an empty list" {
    var server: TestServer = undefined;
    try server.start(.status_500, &.{});
    defer server.stop();
    var remote = try RemoteCodec.init(testing.allocator, testing.io, .{ .endpoint = server.endpoint("") });
    defer remote.deinit();
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    try testing.expectEqual(@as(usize, 0), (try remote.codec().encode(arena_state.allocator(), testing.io, &.{})).len);
    try testing.expectEqual(@as(usize, 0), (try remote.codec().decode(arena_state.allocator(), testing.io, &.{})).len);
    try testing.expectEqual(@as(usize, 0), server.snapshot().requests.len);
}

test "RemoteCodec.init rejects invalid options without leaking" {
    const cases = [_]RemoteCodecOptions{
        .{ .endpoint = "" },
        .{ .endpoint = "codec.example.com" },
        .{ .endpoint = "ftp://codec.example.com" },
        .{ .endpoint = "http://" },
        .{ .endpoint = "http:///path" },
        .{ .endpoint = "http://codec.example.com/x?y=1" },
        .{ .endpoint = "http://codec.example.com/x#frag" },
        .{ .endpoint = "http://codec.example.com:99999" },
        .{ .endpoint = "http://[::1]:8081" },
        .{ .endpoint = "HTTP://127.0.0.1:1/ns" },
        .{ .endpoint = "http://codec.example.com/a\r\nx-evil: 1" },
        .{ .endpoint = "http://codec.example.com/a b" },
        .{ .endpoint = "http://codec.example.com/caf\xc3\xa9" },
        .{ .endpoint = "Https://codec.example.com" },
        .{ .endpoint = "ws://codec.example.com" },
        .{ .endpoint = "http://codec_server:8081" },
        .{ .endpoint = "http://codec.example.com", .headers = &.{.{ .name = "Connection", .value = "close" }} },
        .{ .endpoint = "http://codec.example.com", .headers = &.{.{ .name = "accept-encoding", .value = "gzip" }} },
        .{ .endpoint = "http://codec.example.com", .headers = &.{ .{ .name = "authorization", .value = "a" }, .{ .name = "Authorization", .value = "b" } } },
        .{ .endpoint = "http://codec.example.com", .timeout = .zero },
        .{ .endpoint = "http://codec.example.com", .timeout = .fromSeconds(-1) },
        .{ .endpoint = "http://codec.example.com", .headers = &.{.{ .name = "", .value = "v" }} },
        .{ .endpoint = "http://codec.example.com", .headers = &.{.{ .name = "a:b", .value = "v" }} },
        .{ .endpoint = "http://codec.example.com", .headers = &.{.{ .name = "a\rb", .value = "v" }} },
        .{ .endpoint = "http://codec.example.com", .headers = &.{.{ .name = "a\nb", .value = "v" }} },
        .{ .endpoint = "http://codec.example.com", .headers = &.{.{ .name = "authorization", .value = "a\rb" }} },
        .{ .endpoint = "http://codec.example.com", .headers = &.{.{ .name = "authorization", .value = "a\nb" }} },
        .{ .endpoint = "http://codec.example.com", .namespace = "ns\r\nx-evil: 1" },
        .{ .endpoint = "http://codec.example.com", .headers = &.{.{ .name = "Content-Type", .value = "text/plain" }} },
        .{ .endpoint = "http://codec.example.com", .headers = &.{.{ .name = "content-length", .value = "1" }} },
        .{ .endpoint = "http://codec.example.com", .headers = &.{.{ .name = "Transfer-Encoding", .value = "chunked" }} },
        .{ .endpoint = "http://codec.example.com", .headers = &.{.{ .name = "HOST", .value = "h" }} },
        .{ .endpoint = "http://codec.example.com", .headers = &.{ .{ .name = "authorization", .value = "t" }, .{ .name = "X-Namespace", .value = "default" } } },
    };
    for (cases) |options| {
        try testing.expectError(error.InvalidOptions, RemoteCodec.init(testing.allocator, testing.io, options));
    }
    // Valid ones, for contrast; init makes no network call.
    for ([_]RemoteCodecOptions{
        .{ .endpoint = "https://codec.example.com/" },
        .{ .endpoint = "http://127.0.0.1:1/ns", .timeout = null, .namespace = "default" },
        .{ .endpoint = "http://codec-server:8081", .timeout = .{ .nanoseconds = std.math.maxInt(i96) } },
        .{ .endpoint = "http://localhost:8081", .headers = &.{.{ .name = "authorization", .value = "" }} },
    }) |options| {
        var remote = try RemoteCodec.init(testing.allocator, testing.io, options);
        remote.deinit();
    }
    {
        // The URLs that failures log leave out the endpoint's user and password.
        var remote = try RemoteCodec.init(testing.allocator, testing.io, .{ .endpoint = "http://user:s3cret@127.0.0.1:1/ns/" });
        defer remote.deinit();
        try testing.expectEqualStrings("http://127.0.0.1:1/ns/encode", remote.state.encode_url);
        try testing.expectEqualStrings("http://127.0.0.1:1/ns/decode", remote.state.decode_url);
    }
    {
        // The codec writes basic authorization itself, from the percent-decoded user and
        // password, whatever their length; std.http.Client never sees them.
        var remote = try RemoteCodec.init(testing.allocator, testing.io, .{ .endpoint = "http://us%65r:p%40ss@127.0.0.1:1" });
        defer remote.deinit();
        try testing.expectEqualStrings("Basic dXNlcjpwQHNz", remote.state.authorization.override); // user:p@ss
        try testing.expectEqual(@as(?std.Uri.Component, null), remote.state.encode_uri.user);
        try testing.expectEqual(@as(?std.Uri.Component, null), remote.state.decode_uri.password);
    }
    {
        // Rejections log the endpoint without its user info.
        var buf: [128]u8 = undefined;
        for ([_][2][]const u8{
            .{ "https://user:s3cret@codec.example.com/x?y=1", "https://***@codec.example.com/x?y=1" },
            .{ "user:s3cret@codec.example.com", "***@codec.example.com" },
            .{ "http://tok@en:s3c@ret@h:1#f", "http://***@h:1#f" },
            .{ "https://user:p#ss@codec.example.com", "https://***@codec.example.com" },
            .{ "http://user:12/cd@127.0.0.1:1", "http://***@127.0.0.1:1" },
            .{ "http://codec.example.com/a@b", "http://***@b" },
            .{ "http://codec.example.com/ab", "http://codec.example.com/ab" },
        }) |case| try testing.expectEqualStrings(case[1], try std.fmt.bufPrint(&buf, "{f}", .{redacted(case[0])}));
    }
    try testing.checkAllAllocationFailures(testing.allocator, struct {
        fn initAndDeinit(gpa: Allocator) !void {
            var remote = try RemoteCodec.init(gpa, testing.io, .{
                .endpoint = "http://codec.example.com/",
                .headers = &.{.{ .name = "authorization", .value = "t" }},
                .namespace = "default",
            });
            remote.deinit();
        }
    }.initAndDeinit, .{});
}

const activity = @import("../activity.zig");
const workflow = @import("../workflow.zig");

fn remoteGreet(ctx: *activity.Context, name: []const u8) ![]const u8 {
    return std.fmt.allocPrint(ctx.allocator(), "Hello, {s}!", .{name});
}

fn remoteGreeting(ctx: *workflow.Context, name: []const u8) ![]const u8 {
    return ctx.executeActivity(RemoteGreet, .{ .start_to_close_timeout = .fromSeconds(10) }, .{name}).get();
}

const RemoteGreet = activity.Def("RemoteCodecGreet", remoteGreet);
const RemoteGreeting = workflow.Def("RemoteCodecGreeting", remoteGreeting);

test "a client and worker with a remote codec run a workflow on the dev server" {
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const zlib: codec.ZlibCodec = .{ .always_encode = true };
    var server: TestServer = undefined;
    try server.start(.handler, &.{zlib.codec()});
    defer server.stop();

    var remote = try RemoteCodec.init(testing.allocator, testing.io, .{ .endpoint = server.endpoint("/default"), .namespace = "default" });
    defer remote.deinit();
    const codecs = [_]PayloadCodec{remote.codec()};
    const env = try @import("../testing.zig").WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{ .codecs = &codecs });
    defer env.deinit();
    try env.registerWorkflow(RemoteGreeting);
    try env.registerActivity(RemoteGreet);
    const result = try env.executeWorkflow(RemoteGreeting, .{"remote"}, arena, null);
    try testing.expectEqualStrings("Hello, remote!", result);

    var encodes: usize = 0;
    var decodes: usize = 0;
    for (server.snapshot().requests) |r| {
        if (std.mem.eql(u8, r.target, "/default/encode")) encodes += 1;
        if (std.mem.eql(u8, r.target, "/default/decode")) decodes += 1;
    }
    try testing.expect(encodes > 0);
    try testing.expect(decodes > 0);
}

/// A codec server that holds the first `held` requests until all of them have arrived, so the
/// client opens that many connections, and closes every connection after one answer without
/// saying so, as a server's idle timeout closes all of a client's idle connections at once.
const OneShotServer = struct {
    listener: Io.net.Server,
    codecs: []const PayloadCodec,
    held: usize,
    serving: Io.Future(void) = undefined,
    mutex: Io.Mutex = .init,
    arrived: usize = 0,
    connections: usize = 0,
    all_arrived: Io.Event = .unset,
    endpoint_buf: [64]u8 = undefined,

    fn start(s: *OneShotServer, held: usize, codecs: []const PayloadCodec) !void {
        const address = try Io.net.IpAddress.parse("127.0.0.1", 0);
        s.* = .{ .listener = try address.listen(testing.io, .{ .reuse_address = true }), .codecs = codecs, .held = held };
        errdefer s.listener.deinit(testing.io);
        s.serving = try io_tasks.concurrent(testing.io, serve, .{s});
    }

    fn stop(s: *OneShotServer) void {
        s.serving.cancel(testing.io);
        s.listener.deinit(testing.io);
    }

    fn endpoint(s: *OneShotServer) []const u8 {
        return std.fmt.bufPrint(&s.endpoint_buf, "http://127.0.0.1:{d}", .{s.listener.socket.address.getPort()}) catch unreachable;
    }

    fn serve(s: *OneShotServer) void {
        var group: Io.Group = .init;
        defer group.cancel(testing.io);
        while (true) {
            const stream = s.listener.accept(testing.io) catch return;
            group.concurrent(testing.io, serveConnection, .{ s, stream }) catch {
                stream.close(testing.io);
                return;
            };
        }
    }

    fn serveConnection(s: *OneShotServer, stream: Io.net.Stream) Io.Cancelable!void {
        defer stream.close(testing.io);
        var recv_buffer: [8192]u8 = undefined;
        var send_buffer: [4096]u8 = undefined;
        var conn_reader = stream.reader(testing.io, &recv_buffer);
        var conn_writer = stream.writer(testing.io, &send_buffer);
        var server = http.Server.init(&conn_reader.interface, &conn_writer.interface);
        var request = server.receiveHead() catch return;
        {
            s.mutex.lockUncancelable(testing.io);
            defer s.mutex.unlock(testing.io);
            s.connections += 1;
            s.arrived += 1;
            if (s.arrived == s.held) s.all_arrived.set(testing.io);
        }
        try s.all_arrived.wait(testing.io);
        var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
        defer arena_state.deinit();
        codec_handler.codecHandler(s.codecs, .{}).handle(arena_state.allocator(), testing.io, &request) catch return;
    }
};

test "a remote codec's retry gets a new connection when the server closed every pooled one" {
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const zlib: codec.ZlibCodec = .{ .always_encode = true };
    var server: OneShotServer = undefined;
    try server.start(2, &.{zlib.codec()});
    defer server.stop();
    var remote = try RemoteCodec.init(testing.allocator, testing.io, .{ .endpoint = server.endpoint() });
    defer remote.deinit();
    const payloads = try testPayloads(arena);

    // Two calls at once leave two pooled connections, both of which the server then closes.
    var first_arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer first_arena.deinit();
    var second_arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer second_arena.deinit();
    var first = try io_tasks.concurrent(testing.io, encodeTask, .{ remote.codec(), first_arena.allocator(), payloads });
    var second = io_tasks.concurrent(testing.io, encodeTask, .{ remote.codec(), second_arena.allocator(), payloads }) catch |err| {
        _ = first.cancel(testing.io) catch {};
        return err;
    };
    const first_result = first.await(testing.io);
    const second_result = second.await(testing.io);
    _ = try first_result;
    _ = try second_result;

    // The next call meets a closed connection first; its one retry must not take the other.
    const encoded = try remote.codec().encode(arena, testing.io, payloads);
    try testing.expectEqualStrings("binary/zlib", converter.encodingOf(encoded[0]).?);
}

test "a remote codec verifies the codec server certificate as of now, not as of its first https call" {
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    // A port that was just free, so the call fails at once; std.http.Client's TLS setup runs
    // before it connects.
    const address = try Io.net.IpAddress.parse("127.0.0.1", 0);
    var listener = try address.listen(testing.io, .{});
    var buf: [64]u8 = undefined;
    const endpoint = try std.fmt.bufPrint(&buf, "https://127.0.0.1:{d}", .{listener.socket.address.getPort()});
    listener.deinit(testing.io);

    var remote = try RemoteCodec.init(testing.allocator, testing.io, .{ .endpoint = endpoint });
    defer remote.deinit();
    // std.http.Client verifies every certificate as of `client.now`, which its first https
    // request sets. As if that had been 30 days ago in a worker still running: a certificate the
    // codec server has renewed since then is rejected as not yet valid, and one that has expired
    // since then is accepted.
    const client = &remote.state.client;
    const now = Io.Clock.real.now(testing.io);
    client.now = now.subDuration(.fromSeconds(30 * std.time.s_per_day));
    try testing.expectError(error.CodecFailed, remote.codec().encode(arena, testing.io, try testPayloads(arena)));
    if (client.now) |verify_as_of| {
        const age = verify_as_of.durationTo(now).toSeconds();
        if (age > 3600) {
            std.debug.print("the codec's TLS client verifies certificates as of {d} s ago\n", .{age});
            return error.TestUnexpectedResult;
        }
    }
}

test "a remote codec whose endpoint holds a user and password over 511 bytes rejects it or sends it, without a panic" {
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const zlib: codec.ZlibCodec = .{ .always_encode = true };
    var server: TestServer = undefined;
    try server.start(.handler, &.{zlib.codec()});
    defer server.stop();

    // std.http.Client writes basic authorization from the endpoint's user and password through a
    // 511-byte buffer with `catch unreachable`, so a longer pair, such as a token as the password,
    // panics on every call unless init refuses it or the codec writes the header itself.
    const token: [600]u8 = @splat('t');
    const endpoint = try std.fmt.allocPrint(arena, "http://user:{s}@127.0.0.1:{d}", .{ &token, server.listener.socket.address.getPort() });
    var remote = RemoteCodec.init(testing.allocator, testing.io, .{ .endpoint = endpoint }) catch |err| {
        try testing.expectEqual(error.InvalidOptions, err);
        return;
    };
    defer remote.deinit();
    _ = try remote.codec().encode(arena, testing.io, try testPayloads(arena));
    const seen = server.snapshot().requests;
    try testing.expectEqual(@as(usize, 1), seen.len);
    try testing.expect(std.mem.startsWith(u8, seen[0].authorization.?, "Basic "));
}

/// The allocator of an `Io.Threaded`, standing in for a Debug allocator: like its stack capture
/// (see CancelBlockingAllocator), the first allocation made while cancels are not blocked waits
/// on the Io, here for up to 2 s, and drops the `error.Canceled` it gets.
const CancelDroppingAllocator = struct {
    child: Allocator,
    io: Io = undefined,
    armed: std.atomic.Value(bool) = .init(true),
    never: Io.Event = .unset,

    fn allocator(self: *CancelDroppingAllocator) Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }

    fn alloc(ptr: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *CancelDroppingAllocator = @ptrCast(@alignCast(ptr));
        if (self.armed.load(.acquire)) {
            const prev = self.io.swapCancelProtection(.blocked);
            _ = self.io.swapCancelProtection(prev);
            if (prev == .unblocked and self.armed.swap(false, .acq_rel)) self.never.waitTimeout(self.io, .{ .duration = .{ .raw = .fromSeconds(2), .clock = .awake } }) catch {};
        }
        return self.child.rawAlloc(len, alignment, ret_addr);
    }

    fn resize(ptr: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
        const self: *CancelDroppingAllocator = @ptrCast(@alignCast(ptr));
        return self.child.rawResize(memory, alignment, new_len, ret_addr);
    }

    fn remap(ptr: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        const self: *CancelDroppingAllocator = @ptrCast(@alignCast(ptr));
        return self.child.rawRemap(memory, alignment, new_len, ret_addr);
    }

    fn free(ptr: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *CancelDroppingAllocator = @ptrCast(@alignCast(ptr));
        self.child.rawFree(memory, alignment, ret_addr);
    }
};

const TimedEncode = struct {
    codec: PayloadCodec,
    arena: Allocator,
    payloads: []const Payload,
    result: CodecError![]Payload = error.CodecFailed,
    done: Io.Event = .unset,

    fn run(t: *TimedEncode) void {
        defer t.done.set(testing.io);
        t.result = t.codec.encode(t.arena, testing.io, t.payloads);
    }
};

test "a remote codec's timeout ends a call even when an allocation of its Io drops the cancel while connecting" {
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    // std.http.Client's connect starts tasks on the codec's Io (`HostName.connect`), and the Io
    // allocates them from its own allocator, outside the codec's CancelBlockingAllocator. In a
    // Debug build that allocator is a Debug allocator, which can drop a cancel.
    var dropping: CancelDroppingAllocator = .{ .child = testing.allocator };
    var threaded: Io.Threaded = .init(dropping.allocator(), .{ .environ = testing.environ });
    defer threaded.deinit();
    dropping.io = threaded.io();

    var server: TestServer = undefined;
    try server.start(.silent, &.{});
    var stopped = false;
    defer if (!stopped) server.stop();
    var remote = try RemoteCodec.init(testing.allocator, threaded.io(), .{ .endpoint = server.endpoint(""), .timeout = .fromMilliseconds(200) });
    defer remote.deinit();

    var call: TimedEncode = .{ .codec = remote.codec(), .arena = arena, .payloads = try testPayloads(arena) };
    var future = try io_tasks.concurrent(testing.io, TimedEncode.run, .{&call});
    const returned = if (call.done.waitTimeout(testing.io, .{ .duration = .{ .raw = .fromSeconds(30), .clock = .awake } })) true else |_| false;
    if (!returned) {
        // The exchange is stuck reading from the silent server; closing the connection ends it.
        server.stop();
        stopped = true;
    }
    future.await(testing.io);
    if (!returned) {
        std.debug.print("a remote codec with a 200 ms timeout had not returned after 30 s\n", .{});
        return error.TestUnexpectedResult;
    }
    try testing.expectError(error.CodecFailed, call.result);
}

test "a remote codec moves its certificate clock only while no handshake of its own runs" {
    var remote = try RemoteCodec.init(testing.allocator, testing.io, .{ .endpoint = "https://codec.example.com" });
    defer remote.deinit();
    const s = remote.state;
    const old = Io.Clock.real.now(testing.io).subDuration(.fromSeconds(30 * std.time.s_per_day));
    s.client.now = old;

    // A call in `client.request` holds the lock shared; the refresh skips rather than waits.
    s.handshake_lock.lockSharedUncancelable(testing.io);
    s.refreshCertificateClock();
    s.handshake_lock.unlockShared(testing.io);
    try testing.expectEqual(old.nanoseconds, s.client.now.?.nanoseconds);

    s.refreshCertificateClock();
    try testing.expect(old.durationTo(s.client.now.?).toSeconds() >= 30 * std.time.s_per_day - 60);

    // Never set while std hasn't loaded the CA bundle.
    s.client.now = null;
    s.refreshCertificateClock();
    try testing.expectEqual(@as(?Io.Timestamp, null), s.client.now);
}
