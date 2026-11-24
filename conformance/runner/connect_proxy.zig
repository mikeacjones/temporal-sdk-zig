//! The HTTP CONNECT proxy of the client features whose suite config.json has
//! `expectUnauthedProxyCount` or `expectAuthedProxyCount`, after the suite's
//! harness/go/harness/httpproxy.go: it tunnels CONNECT requests, accepts Basic
//! `proxy-user:proxy-pass` when credentials are sent, and counts the tunnels it opened
//! with and without them.
const std = @import("std");
const harness = @import("harness");

const Allocator = std.mem.Allocator;
const Io = std.Io;
const net = Io.net;

/// The longest request head (request line and headers) the proxy reads.
const max_head_bytes = 8 * 1024;

pub const Counts = struct { unauthed: u32 = 0, authed: u32 = 0 };

pub const Proxy = struct {
    gpa: Allocator,
    io: Io,
    server: net.Server,
    host_port_buffer: [32]u8,
    host_port_len: usize,
    unauthed: std.atomic.Value(u32) = .init(0),
    authed: std.atomic.Value(u32) = .init(0),
    accepting: Io.Future(void),
    /// The tunnel tasks. Only the accept task adds to it, and `stop` cancels it only after
    /// the accept task has ended.
    tunnels: Io.Group = .init,
};

/// Listens on 127.0.0.1 on a free port and serves HTTP CONNECT until `stop`.
pub fn start(gpa: Allocator, io: Io) !*Proxy {
    const proxy = try gpa.create(Proxy);
    errdefer gpa.destroy(proxy);
    const address: net.IpAddress = .{ .ip4 = .loopback(0) };
    proxy.* = .{
        .gpa = gpa,
        .io = io,
        .server = try address.listen(io, .{}),
        .host_port_buffer = undefined,
        .host_port_len = 0,
        .accepting = undefined,
    };
    errdefer proxy.server.deinit(io);
    const port = proxy.server.socket.address.getPort();
    proxy.host_port_len = (std.fmt.bufPrint(&proxy.host_port_buffer, "127.0.0.1:{d}", .{port}) catch unreachable).len;
    proxy.accepting = try io.concurrent(acceptLoop, .{proxy});
    return proxy;
}

/// "127.0.0.1:<port>", valid until `stop`.
pub fn hostPort(proxy: *const Proxy) []const u8 {
    return proxy.host_port_buffer[0..proxy.host_port_len];
}

/// Tunnels established so far.
pub fn counts(proxy: *const Proxy) Counts {
    return .{ .unauthed = proxy.unauthed.load(.acquire), .authed = proxy.authed.load(.acquire) };
}

/// Closes the listener and every tunnel, waits for their tasks, frees the proxy.
pub fn stop(proxy: *Proxy) void {
    const io = proxy.io;
    proxy.accepting.cancel(io);
    proxy.tunnels.cancel(io);
    proxy.server.deinit(io);
    proxy.gpa.destroy(proxy);
}

/// The failure message when `actual` differs from `expected`, else null.
pub fn mismatch(arena: Allocator, expected: Counts, actual: Counts) error{OutOfMemory}!?[]const u8 {
    if (actual.unauthed != expected.unauthed)
        return try std.fmt.allocPrint(arena, "expected {d} unauthed HTTP proxy connections, got {d}", .{ expected.unauthed, actual.unauthed });
    if (actual.authed != expected.authed)
        return try std.fmt.allocPrint(arena, "expected {d} authed HTTP proxy connections, got {d}", .{ expected.authed, actual.authed });
    return null;
}

fn acceptLoop(proxy: *Proxy) void {
    const io = proxy.io;
    while (true) {
        const stream = proxy.server.accept(io) catch |err| switch (err) {
            error.Canceled => return,
            error.ConnectionAborted => continue,
            else => {
                std.log.warn("HTTP CONNECT proxy {s} stopped accepting: {t}", .{ hostPort(proxy), err });
                return;
            },
        };
        proxy.tunnels.concurrent(io, tunnel, .{ proxy, stream }) catch |err| {
            std.log.warn("HTTP CONNECT proxy {s} cannot serve a connection: {t}", .{ hostPort(proxy), err });
            stream.close(io);
        };
    }
}

const Request = struct {
    status: enum { connect, bad_request, method_not_allowed, auth_failed },
    target: []const u8 = "",
    authed: bool = false,
};

/// Reads one request head from `client`, answers it, and on a 200 copies bytes both ways
/// until either side ends or the task is canceled.
fn tunnel(proxy: *Proxy, client: net.Stream) Io.Cancelable!void {
    const io = proxy.io;
    defer client.close(io);
    var head_buffer: [max_head_bytes]u8 = undefined;
    var reader = client.reader(io, &head_buffer);
    const in = &reader.interface;
    const head_len = while (true) {
        if (std.mem.indexOf(u8, in.buffered(), "\r\n\r\n")) |end| break end + 4;
        if (in.bufferedLen() == head_buffer.len) return respond(io, client, "400 Bad Request");
        in.fillMore() catch |err| switch (err) {
            error.EndOfStream => return,
            error.ReadFailed => return if (reader.err.? == error.Canceled) error.Canceled,
        };
    };
    const request = parseRequest(in.buffered()[0..head_len]);
    switch (request.status) {
        .connect => {},
        .bad_request => return respond(io, client, "400 Bad Request"),
        .method_not_allowed => return respond(io, client, "405 Method Not Allowed"),
        .auth_failed => return respond(io, client, "407 Proxy Authentication Required\r\nProxy-Authenticate: Basic realm=\"conformance\""),
    }
    const target = connectTarget(io, request.target) catch |err| switch (err) {
        error.Canceled => return error.Canceled,
        error.InvalidTarget => return respond(io, client, "400 Bad Request"),
        else => {
            std.log.debug("HTTP CONNECT proxy cannot reach {s}: {t}", .{ request.target, err });
            return respond(io, client, "502 Bad Gateway");
        },
    };
    defer target.close(io);
    in.toss(head_len);
    try writeAll(io, client, "HTTP/1.1 200 Connection established\r\n\r\n");
    _ = (if (request.authed) &proxy.authed else &proxy.unauthed).fetchAdd(1, .acq_rel);
    // Bytes the client sent after the head belong to the tunnel.
    try writeAll(io, target, in.buffered());

    var downstream = io.concurrent(pumpThenShutdown, .{ io, target, client }) catch return;
    defer downstream.cancel(io) catch {};
    const upstream = pump(io, client, target);
    // Shutdown is a cancelation point: a cancel it reports is consumed, so pass it on, or
    // the await below could never be canceled and `stop` would wait forever.
    target.shutdown(io, .send) catch |err| if (err == error.Canceled) return error.Canceled;
    try upstream;
    try downstream.await(io);
}

fn parseRequest(head: []const u8) Request {
    var lines = std.mem.splitSequence(u8, head, "\r\n");
    var parts = std.mem.splitScalar(u8, lines.first(), ' ');
    const method = parts.next() orelse return .{ .status = .bad_request };
    const target = parts.next() orelse return .{ .status = .bad_request };
    const version = parts.next() orelse return .{ .status = .bad_request };
    if (parts.next() != null or method.len == 0 or target.len == 0 or !std.mem.startsWith(u8, version, "HTTP/1."))
        return .{ .status = .bad_request };
    if (!std.mem.eql(u8, method, "CONNECT")) return .{ .status = .method_not_allowed };
    var authorization: ?[]const u8 = null;
    while (lines.next()) |line| {
        if (line.len == 0) continue;
        const colon = std.mem.indexOfScalar(u8, line, ':') orelse return .{ .status = .bad_request };
        if (!std.ascii.eqlIgnoreCase(std.mem.trim(u8, line[0..colon], " \t"), "Proxy-Authorization")) continue;
        if (authorization != null) return .{ .status = .auth_failed };
        authorization = std.mem.trim(u8, line[colon + 1 ..], " \t");
    }
    const value = authorization orelse return .{ .status = .connect, .target = target };
    if (!credentialsMatch(value)) return .{ .status = .auth_failed };
    return .{ .status = .connect, .target = target, .authed = true };
}

/// Whether a Proxy-Authorization value is Basic with the harness's user and password.
fn credentialsMatch(value: []const u8) bool {
    const scheme = "Basic ";
    if (value.len < scheme.len or !std.ascii.eqlIgnoreCase(value[0..scheme.len], scheme)) return false;
    const encoded = std.mem.trim(u8, value[scheme.len..], " \t");
    const expected = harness.Context.http_proxy_username ++ ":" ++ harness.Context.http_proxy_password;
    const decoder = std.base64.standard.Decoder;
    const size = decoder.calcSizeForSlice(encoded) catch return false;
    if (size != expected.len) return false;
    var decoded: [expected.len]u8 = undefined;
    decoder.decode(&decoded, encoded) catch return false;
    return std.mem.eql(u8, &decoded, expected);
}

fn connectTarget(io: Io, target: []const u8) !net.Stream {
    // No timeout: Zig 0.17.1's threaded Io panics on a connect timeout. A connect that
    // hangs ends when `stop` cancels the tunnel's task, after the feature's child has ended.
    const options: net.IpAddress.ConnectOptions = .{ .mode = .stream };
    if (net.IpAddress.parseLiteral(target)) |address| {
        if (address.getPort() == 0) return error.InvalidTarget;
        return address.connect(io, options);
    } else |_| {}
    const colon = std.mem.lastIndexOfScalar(u8, target, ':') orelse return error.InvalidTarget;
    const port = std.fmt.parseInt(u16, target[colon + 1 ..], 10) catch return error.InvalidTarget;
    if (port == 0) return error.InvalidTarget;
    const host = net.HostName.init(target[0..colon]) catch return error.InvalidTarget;
    return host.connect(io, port, options);
}

fn respond(io: Io, client: net.Stream, comptime status: []const u8) Io.Cancelable!void {
    return writeAll(io, client, "HTTP/1.1 " ++ status ++ "\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
}

/// Writes `bytes`; a write that fails other than by cancellation is dropped, as the
/// connection is then gone.
fn writeAll(io: Io, stream: net.Stream, bytes: []const u8) Io.Cancelable!void {
    var writer = stream.writer(io, &.{});
    writer.interface.writeAll(bytes) catch {
        if (writer.err) |err| if (err == error.Canceled) return error.Canceled;
    };
}

/// Copies `from` to `to` until `from` ends or a read or write fails.
fn pump(io: Io, from: net.Stream, to: net.Stream) Io.Cancelable!void {
    var buffer: [16 * 1024]u8 = undefined;
    var writer = to.writer(io, &.{});
    while (true) {
        var data = [_][]u8{&buffer};
        const n = from.read(io, &data) catch |err| switch (err) {
            error.Canceled => return error.Canceled,
            else => return,
        };
        if (n == 0) return;
        writer.interface.writeAll(buffer[0..n]) catch {
            if (writer.err) |err| if (err == error.Canceled) return error.Canceled;
            return;
        };
    }
}

fn pumpThenShutdown(io: Io, from: net.Stream, to: net.Stream) Io.Cancelable!void {
    // A cancel the shutdown consumes is re-armed for whoever runs next in this task.
    defer to.shutdown(io, .send) catch |err| if (err == error.Canceled) io.recancel();
    return pump(io, from, to);
}

const testing = std.testing;

/// A target that echoes what it reads, one connection at a time, until canceled.
const Echo = struct {
    server: net.Server,

    fn serve(echo: *Echo) void {
        while (true) {
            const stream = echo.server.accept(testing.io) catch return;
            defer stream.close(testing.io);
            pump(testing.io, stream, stream) catch return;
        }
    }

    fn hostPort(echo: *const Echo, buffer: []u8) []const u8 {
        return std.fmt.bufPrint(buffer, "127.0.0.1:{d}", .{echo.server.socket.address.getPort()}) catch unreachable;
    }
};

fn connectTo(proxy: *const Proxy) !net.Stream {
    const address = try net.IpAddress.parseLiteral(hostPort(proxy));
    return address.connect(testing.io, .{ .mode = .stream });
}

/// Sends `head` and reads the answer's status line.
fn sendHead(stream: net.Stream, head: []const u8, buffer: []u8) ![]const u8 {
    var writer = stream.writer(testing.io, &.{});
    try writer.interface.writeAll(head);
    var reader = stream.reader(testing.io, buffer);
    return reader.interface.takeDelimiterInclusive('\n');
}

fn startEcho() !*Echo {
    const echo = try testing.allocator.create(Echo);
    errdefer testing.allocator.destroy(echo);
    const address: net.IpAddress = .{ .ip4 = .loopback(0) };
    echo.* = .{ .server = try address.listen(testing.io, .{}) };
    return echo;
}

test "connect proxy tunnels and counts requests with and without credentials" {
    const io = testing.io;
    const echo = try startEcho();
    defer testing.allocator.destroy(echo);
    defer echo.server.deinit(io);
    var echoing = try io.concurrent(Echo.serve, .{echo});
    defer echoing.cancel(io);

    const proxy = try start(testing.allocator, io);
    defer stop(proxy);
    var target_buffer: [32]u8 = undefined;
    const target = echo.hostPort(&target_buffer);
    try testing.expectEqualDeep(Counts{}, counts(proxy));

    for ([_]struct { auth: []const u8, expected: Counts }{
        .{ .auth = "", .expected = .{ .unauthed = 1 } },
        // proxy-user:proxy-pass
        .{ .auth = "Proxy-Authorization: Basic cHJveHktdXNlcjpwcm94eS1wYXNz\r\n", .expected = .{ .unauthed = 1, .authed = 1 } },
    }) |case| {
        const stream = try connectTo(proxy);
        defer stream.close(io);
        var head_buffer: [256]u8 = undefined;
        const head = try std.fmt.bufPrint(&head_buffer, "CONNECT {s} HTTP/1.1\r\nHost: {s}\r\n{s}\r\nhello", .{ target, target, case.auth });
        var writer = stream.writer(io, &.{});
        try writer.interface.writeAll(head);
        var read_buffer: [256]u8 = undefined;
        var reader = stream.reader(io, &read_buffer);
        try testing.expectEqualStrings("HTTP/1.1 200 Connection established\r\n\r\n", try reader.interface.take(39));
        // Bytes sent with the head, then bytes sent after the answer, come back through the tunnel.
        try testing.expectEqualStrings("hello", try reader.interface.take(5));
        try writer.interface.writeAll("again");
        try testing.expectEqualStrings("again", try reader.interface.take(5));
        try testing.expectEqualDeep(case.expected, counts(proxy));
    }
}

test "connect proxy refuses wrong credentials, other methods, bad requests and unreachable targets without counting" {
    const io = testing.io;
    const proxy = try start(testing.allocator, io);
    defer stop(proxy);
    // A port nothing listens on: listen, note the port, close.
    const closed_port = blk: {
        const address: net.IpAddress = .{ .ip4 = .loopback(0) };
        var server = try address.listen(io, .{});
        defer server.deinit(io);
        break :blk server.socket.address.getPort();
    };
    var head_buffer: [256]u8 = undefined;
    for ([_]struct { head: []const u8, status: []const u8 }{
        // proxy-user:wrong
        .{ .head = "CONNECT 127.0.0.1:1 HTTP/1.1\r\nProxy-Authorization: Basic cHJveHktdXNlcjp3cm9uZw==\r\n\r\n", .status = "407" },
        .{ .head = "CONNECT 127.0.0.1:1 HTTP/1.1\r\nproxy-authorization: Bearer cHJveHktdXNlcjpwcm94eS1wYXNz\r\n\r\n", .status = "407" },
        .{ .head = "CONNECT 127.0.0.1:1 HTTP/1.1\r\nProxy-Authorization: Basic %%%\r\n\r\n", .status = "407" },
        .{ .head = "GET http://127.0.0.1:1/ HTTP/1.1\r\n\r\n", .status = "405" },
        .{ .head = "CONNECT\r\n\r\n", .status = "400" },
        .{ .head = "CONNECT 127.0.0.1 HTTP/1.1\r\n\r\n", .status = "400" },
        .{ .head = try std.fmt.bufPrint(&head_buffer, "CONNECT 127.0.0.1:{d} HTTP/1.1\r\n\r\n", .{closed_port}), .status = "502" },
    }) |case| {
        const stream = try connectTo(proxy);
        defer stream.close(io);
        var read_buffer: [256]u8 = undefined;
        const line = try sendHead(stream, case.head, &read_buffer);
        try testing.expect(std.mem.startsWith(u8, line, "HTTP/1.1 "));
        try testing.expectEqualStrings(case.status, line["HTTP/1.1 ".len..][0..3]);
    }
    try testing.expectEqualDeep(Counts{}, counts(proxy));
}

test "connect proxy stops with a tunnel and a half-sent request still open" {
    const io = testing.io;
    const echo = try startEcho();
    defer testing.allocator.destroy(echo);
    defer echo.server.deinit(io);
    var echoing = try io.concurrent(Echo.serve, .{echo});
    defer echoing.cancel(io);

    const proxy = try start(testing.allocator, io);
    var target_buffer: [32]u8 = undefined;
    var head_buffer: [128]u8 = undefined;
    const head = try std.fmt.bufPrint(&head_buffer, "CONNECT {s} HTTP/1.1\r\n\r\n", .{echo.hostPort(&target_buffer)});
    const open = try connectTo(proxy);
    defer open.close(io);
    var read_buffer: [256]u8 = undefined;
    try testing.expectEqualStrings("HTTP/1.1 200 Connection established\r\n", try sendHead(open, head, &read_buffer));
    const partial = try connectTo(proxy);
    defer partial.close(io);
    var writer = partial.writer(io, &.{});
    try writer.interface.writeAll("CONNECT ");
    stop(proxy);
    // The proxy closed its end of the tunnel and of the unfinished request.
    var reader = open.reader(io, &read_buffer);
    try testing.expectError(error.EndOfStream, reader.interface.takeByte());
    // Closed with unread bytes, which may reset the connection instead of ending it.
    var partial_reader = partial.reader(io, &read_buffer);
    if (partial_reader.interface.takeByte()) |_| return error.TestUnexpectedByte else |_| {}
}

test "connect proxy mismatch names the count that differs" {
    var state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer state.deinit();
    const arena = state.allocator();
    try testing.expectEqual(null, try mismatch(arena, .{ .unauthed = 1 }, .{ .unauthed = 1 }));
    try testing.expectEqualStrings("expected 1 unauthed HTTP proxy connections, got 0", (try mismatch(arena, .{ .unauthed = 1 }, .{})).?);
    try testing.expectEqualStrings("expected 1 authed HTTP proxy connections, got 2", (try mismatch(arena, .{ .authed = 1 }, .{ .authed = 2 })).?);
    try testing.expectEqualStrings("expected 0 unauthed HTTP proxy connections, got 1", (try mismatch(arena, .{ .authed = 1 }, .{ .unauthed = 1, .authed = 1 })).?);
}

/// An Io that passes every call to `testing.io`, except that the first send shutdown after
/// `armed` is set waits until its task is canceled and returns error.Canceled, as Threaded's
/// shutdown does, without shutting down, when `stop` cancels the task just before the call.
const CancelBeforeShutdown = struct {
    var vtable: Io.VTable = undefined;
    var armed: std.atomic.Value(bool) = .init(false);
    var entered: std.atomic.Value(bool) = .init(false);

    fn io() Io {
        vtable = testing.io.vtable.*;
        vtable.netShutdown = netShutdown;
        return .{ .userdata = testing.io.userdata, .vtable = &vtable };
    }

    fn netShutdown(userdata: ?*anyopaque, handle: net.Socket.Handle, how: net.ShutdownHow) net.ShutdownError!void {
        const inner = testing.io.vtable;
        if (how == .send and armed.cmpxchgStrong(true, false, .acq_rel, .acquire) == null) {
            entered.store(true, .release);
            const protection = inner.swapCancelProtection(userdata, .unblocked);
            _ = inner.swapCancelProtection(userdata, protection);
            if (protection == .unblocked) while (true) {
                inner.checkCancel(userdata) catch return error.Canceled;
                std.Thread.yield() catch {};
            };
        }
        return inner.netShutdown(userdata, handle, how);
    }

    fn stopAndMark(proxy: *Proxy, stopped: *std.atomic.Value(bool)) void {
        stop(proxy);
        stopped.store(true, .release);
    }

    /// Waits until `flag` is set or 10 s have passed, and returns the flag.
    fn waitFor(flag: *const std.atomic.Value(bool)) !bool {
        const started = Io.Clock.Timestamp.now(testing.io, .boot);
        while (!flag.load(.acquire)) {
            if (started.untilNow(testing.io).raw.nanoseconds > 10 * std.time.ns_per_s) return false;
            try testing.io.sleep(.fromMilliseconds(1), .boot);
        }
        return true;
    }
};

test "connect proxy stop ends a tunnel whose target shutdown is canceled after its client finished" {
    const io = testing.io;
    const echo = try startEcho();
    defer testing.allocator.destroy(echo);
    defer echo.server.deinit(io);
    var echoing = try io.concurrent(Echo.serve, .{echo});
    defer echoing.cancel(io);

    CancelBeforeShutdown.armed.store(false, .release);
    CancelBeforeShutdown.entered.store(false, .release);
    const proxy = try start(testing.allocator, CancelBeforeShutdown.io());
    var target_buffer: [32]u8 = undefined;
    var head_buffer: [128]u8 = undefined;
    const head = try std.fmt.bufPrint(&head_buffer, "CONNECT {s} HTTP/1.1\r\n\r\nhi", .{echo.hostPort(&target_buffer)});
    const stream = try connectTo(proxy);
    defer stream.close(io);
    var writer = stream.writer(io, &.{});
    try writer.interface.writeAll(head);
    var read_buffer: [64]u8 = undefined;
    var reader = stream.reader(io, &read_buffer);
    try testing.expectEqualStrings("HTTP/1.1 200 Connection established\r\n\r\nhi", try reader.interface.take(41));

    // The client is done sending, as a feature's client is when its process ends; `stop`
    // cancels the tunnel just as it starts to pass that on to the target.
    CancelBeforeShutdown.armed.store(true, .release);
    try stream.shutdown(io, .send);
    _ = try CancelBeforeShutdown.waitFor(&CancelBeforeShutdown.entered);
    var stopped: std.atomic.Value(bool) = .init(false);
    var stopping = try io.concurrent(CancelBeforeShutdown.stopAndMark, .{ proxy, &stopped });
    const returned = try CancelBeforeShutdown.waitFor(&stopped);
    // A stop that still waits for the tunnel is let go by ending the target's side of it.
    if (!returned) echoing.cancel(io);
    stopping.await(io);
    try testing.expect(returned);
}
