//! The codec server: serves the sample's codec over HTTP so the Temporal CLI and Web UI can
//! show the payloads it compressed.
//!
//!     codec-server-server [--port 8081] [--web http://localhost:8233]
//!
//! Codecs are set per namespace (here only `default`). A namespace's endpoints are
//! `/{namespace}/encode` and `/{namespace}/decode`; `/encode` and `/decode` serve the
//! namespace named by the `X-Namespace` header, which the CLI and Web UI send. `--web` names
//! the Web UI's origin, which a browser needs CORS headers for. Point the CLI at it with
//! `temporal workflow show -w codecserver_workflowID --codec-endpoint http://localhost:8081/{namespace}`.
//!
//! The server doesn't check who is asking: put it behind a proxy that authenticates callers
//! where the payloads are sensitive.

const std = @import("std");
const temporal = @import("temporal");
const codec_server = @import("codec_server.zig");
const converter = temporal.converter;
const Allocator = std.mem.Allocator;
const Io = std.Io;
const http = std.http;

pub const default_port = 8081;

/// A namespace's codec chain, borrowed.
pub const Namespace = struct {
    name: []const u8,
    codecs: []const converter.PayloadCodec,
};

/// Routes codec requests to their namespace's codecs. Safe to use from several tasks at once:
/// it holds no mutable state, and the codecs are thread-safe.
pub const CodecServer = struct {
    gpa: Allocator,
    io: Io,
    /// Borrowed; must outlive the server.
    namespaces: []const Namespace,
    /// The browser origin allowed to call the server, such as the Web UI's; empty sends no
    /// CORS headers.
    allowed_origin: []const u8 = "",

    /// Serves HTTP on `listener` until canceled, handling each connection in its own task.
    pub fn serve(s: *const CodecServer, listener: *Io.net.Server) Io.net.Server.AcceptError!void {
        var group: Io.Group = .init;
        defer group.cancel(s.io);
        while (true) {
            const stream = try listener.accept(s.io);
            // Without a task of its own, a kept-alive connection would hold up the others.
            group.concurrent(s.io, serveConnection, .{ s, stream }) catch try serveConnection(s, stream);
        }
    }

    fn serveConnection(s: *const CodecServer, stream: Io.net.Stream) Io.Cancelable!void {
        defer stream.close(s.io);
        var recv_buffer: [8192]u8 = undefined;
        var send_buffer: [4096]u8 = undefined;
        var conn_reader = stream.reader(s.io, &recv_buffer);
        var conn_writer = stream.writer(s.io, &send_buffer);
        var server = http.Server.init(&conn_reader.interface, &conn_writer.interface);
        while (server.reader.state == .ready) {
            var request = server.receiveHead() catch |err| switch (err) {
                error.HttpConnectionClosing => return,
                else => return std.log.warn("closing a codec server connection: {t}", .{err}),
            };
            var arena_state: std.heap.ArenaAllocator = .init(s.gpa);
            defer arena_state.deinit();
            s.handle(arena_state.allocator(), &request) catch |err| switch (err) {
                error.Canceled => return error.Canceled,
                else => return std.log.warn("unable to answer a codec server request for {s}: {t}", .{ request.head.target, err }),
            };
        }
    }

    /// Answers one request, with memory from `arena`.
    pub fn handle(s: *const CodecServer, arena: Allocator, request: *http.Server.Request) !void {
        const options: converter.CodecHandlerOptions = .{ .allowed_origin = s.allowed_origin };
        // Preflight requests carry no namespace header; any namespace answers them alike.
        if (request.head.method == .OPTIONS) return converter.codecHandler(&.{}, options).handle(arena, s.io, request);
        if (s.namespaceOf(request)) |ns| return converter.codecHandler(ns.codecs, options).handle(arena, s.io, request);

        var headers_buf: [2]http.Header = undefined;
        var headers: std.ArrayList(http.Header) = .initBuffer(&headers_buf);
        if (s.allowed_origin.len > 0) headers.appendAssumeCapacity(.{ .name = "access-control-allow-origin", .value = s.allowed_origin });
        headers.appendAssumeCapacity(.{ .name = "content-type", .value = "text/plain" });
        return request.respond("not found: no codecs for this namespace; use /{namespace}/encode, /{namespace}/decode, or the X-Namespace header", .{
            .status = .not_found,
            .extra_headers = headers.items,
        });
    }

    /// The namespace leading the request's path, or else the one its X-Namespace header names.
    fn namespaceOf(s: *const CodecServer, request: *const http.Server.Request) ?Namespace {
        const target = request.head.target;
        for (s.namespaces) |ns| {
            if (target.len > ns.name.len + 1 and target[0] == '/' and
                std.mem.startsWith(u8, target[1..], ns.name) and target[ns.name.len + 1] == '/') return ns;
        }
        var it = request.iterateHeaders();
        while (it.next()) |header| {
            if (!std.ascii.eqlIgnoreCase(header.name, "x-namespace")) continue;
            for (s.namespaces) |ns| if (std.mem.eql(u8, ns.name, header.value)) return ns;
        }
        return null;
    }
};

/// Sends `payloads` to the codec endpoint `url` (ending in /encode or /decode) as the CLI and
/// Web UI do, and returns the payloads it answers with, allocated from `arena`. `namespace`,
/// if not empty, is sent as the X-Namespace header.
pub fn postPayloads(client: *http.Client, arena: Allocator, url: []const u8, namespace: []const u8, payloads: []const converter.Payload) ![]converter.Payload {
    const list: converter.Payloads = .{ .payloads = .fromOwnedSlice(try arena.dupe(converter.Payload, payloads)) };
    const body = if (payloads.len == 0) "{\"payloads\":[]}" else try list.jsonEncode(.{}, .{ .emit_oneof_field_name = false }, arena);
    var namespace_header = [_]http.Header{.{ .name = "x-namespace", .value = namespace }};
    var response_body: Io.Writer.Allocating = .init(arena);
    const response = try client.fetch(.{
        .location = .{ .url = url },
        .method = .POST,
        .payload = body,
        .headers = .{ .content_type = .{ .override = "application/json" } },
        .extra_headers = if (namespace.len > 0) &namespace_header else &.{},
        .response_writer = &response_body.writer,
    });
    if (response.status != .ok) {
        std.log.warn("codec server {s} answered HTTP {d}: {s}", .{ url, @backingInt(response.status), response_body.written() });
        return error.CodecServerFailed;
    }

    // ProtoJSON of a Payloads message: bytes, including metadata values, are base64.
    const Json = struct {
        payloads: []const struct {
            metadata: std.json.ArrayHashMap([]const u8) = .{},
            data: []const u8 = "",
        } = &.{},
    };
    const parsed = try std.json.parseFromSliceLeaky(Json, arena, response_body.written(), .{ .ignore_unknown_fields = true });
    const out = try arena.alloc(converter.Payload, parsed.payloads.len);
    for (parsed.payloads, out) |in, *p| {
        p.* = .{ .data = try decodeBase64(arena, in.data) };
        var it = in.metadata.map.iterator();
        while (it.next()) |entry| try p.metadata.append(arena, .{ .key = entry.key_ptr.*, .value = try decodeBase64(arena, entry.value_ptr.*) });
    }
    return out;
}

fn decodeBase64(arena: Allocator, text: []const u8) ![]u8 {
    const decoder = std.base64.standard.Decoder;
    const out = try arena.alloc(u8, try decoder.calcSizeForSlice(text));
    try decoder.decode(out, text);
    return out;
}

pub fn main(init: std.process.Init) !void {
    var port: u16 = default_port;
    var web: []const u8 = "";
    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.skip();
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--port")) {
            const value = args.next() orelse "";
            port = std.fmt.parseInt(u16, value, 10) catch {
                std.log.err("Invalid --port \"{s}\": expected a port number such as {d}", .{ value, default_port });
                std.process.exit(1);
            };
        } else if (std.mem.eql(u8, arg, "--web")) {
            web = args.next() orelse "";
        } else {
            std.log.err("Unknown argument \"{s}\"; usage: codec-server-server [--port {d}] [--web <Temporal Web UI URL>]", .{ arg, default_port });
            std.process.exit(1);
        }
    }

    // Set codecs per namespace here. Only the default namespace has codecs in this example.
    const snappy: codec_server.SnappyCodec = .{};
    const default_codecs = [_]converter.PayloadCodec{snappy.codec()};
    const namespaces = [_]Namespace{.{ .name = "default", .codecs = &default_codecs }};
    for (namespaces) |ns| std.log.info("Handling namespace: {s}", .{ns.name});
    if (web.len > 0) std.log.info("CORS enabled for Origin: {s}", .{web});

    const server: CodecServer = .{ .gpa = init.gpa, .io = init.io, .namespaces = &namespaces, .allowed_origin = web };
    const address = try Io.net.IpAddress.parse("0.0.0.0", port);
    var listener = address.listen(init.io, .{ .reuse_address = true }) catch |err| {
        std.log.err("Unable to listen on port {d}: {t}", .{ port, err });
        std.process.exit(1);
    };
    defer listener.deinit(init.io);
    server.serve(&listener) catch |err| {
        std.log.err("Codec server stopped serving: {t}", .{err});
        std.process.exit(1);
    };
}
