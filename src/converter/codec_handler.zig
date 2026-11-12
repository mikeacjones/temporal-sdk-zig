//! An HTTP handler that runs payload codecs for others: the Temporal Web UI and CLI call a
//! codec server to show payloads that only the application's codecs can decode. Re-exported
//! by `converter`.

const std = @import("std");
const protobuf = @import("protobuf");
const proto = @import("../proto.zig");
const codec = @import("codec.zig");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const http = std.http;
const Payload = proto.temporal.api.common.v1.Payload;
const Payloads = proto.temporal.api.common.v1.Payloads;

const log = std.log.scoped(.temporal);

pub const CodecHandlerOptions = struct {
    /// The origin allowed to call the handler from a browser, such as the Web UI's
    /// "http://localhost:8233", sent as Access-Control-Allow-Origin with credentials allowed;
    /// "*" allows any origin. Empty sends no CORS headers, for callers that aren't browsers.
    allowed_origin: []const u8 = "",
    /// Larger request bodies are refused with 413.
    max_body_len: usize = 64 * 1024 * 1024,
};

pub const HandleError = error{ OutOfMemory, Canceled, ReadFailed, WriteFailed, HttpExpectationFailed };

/// Serves `POST <prefix>/encode` and `POST <prefix>/decode`: the body is ProtoJSON of a
/// `temporal.api.common.v1.Payloads` (`{"payloads":[...]}`), and the response is the same
/// list encoded or decoded with `codecs`, as `client.Options.codecs` would. Preflight
/// `OPTIONS` requests get the CORS headers. The `X-Namespace` header the Web UI sends is
/// ignored: the same codecs serve every namespace.
///
/// Answers 400 for a body that isn't a payload list or that the codecs reject, 404 for other
/// paths, and 405 for other methods. Borrows `codecs`; safe to use from several threads at
/// once, as the codecs are.
pub const CodecHandler = struct {
    codecs: []const codec.PayloadCodec,
    options: CodecHandlerOptions,

    /// Answers one request received from a `std.http.Server`. Request and response memory
    /// comes from `arena`. Errors are failures to read the request or write the response;
    /// bad requests get an error response instead.
    pub fn handle(h: CodecHandler, arena: Allocator, io: Io, request: *http.Server.Request) HandleError!void {
        var headers_buf: [5]http.Header = undefined;
        var headers: std.ArrayList(http.Header) = .initBuffer(&headers_buf);
        if (h.options.allowed_origin.len > 0) headers.appendSliceAssumeCapacity(&.{
            .{ .name = "access-control-allow-origin", .value = h.options.allowed_origin },
            .{ .name = "access-control-allow-credentials", .value = "true" },
            .{ .name = "access-control-allow-methods", .value = "POST, OPTIONS" },
            .{ .name = "access-control-allow-headers", .value = "content-type, x-namespace, authorization" },
        });

        if (request.head.method == .OPTIONS) return request.respond("", .{ .status = .ok, .extra_headers = headers.items });
        const target = request.head.target;
        const path = target[0 .. std.mem.indexOfScalar(u8, target, '?') orelse target.len];
        const encode = std.mem.endsWith(u8, path, "/encode");
        if (!encode and !std.mem.endsWith(u8, path, "/decode"))
            return respondText(request, .not_found, "not found: the codec server serves POST .../encode and POST .../decode", headers.items);
        if (request.head.method != .POST)
            return respondText(request, .method_not_allowed, "method not allowed: send payloads with POST", headers.items);

        var transfer: [4096]u8 = undefined;
        const body_reader = try request.readerExpectContinue(&transfer);
        const body = body_reader.allocRemaining(arena, .limited(h.options.max_body_len)) catch |err| switch (err) {
            error.StreamTooLong => return respondText(request, .payload_too_large, "the request body is larger than the codec server's max_body_len", headers.items),
            error.OutOfMemory => return error.OutOfMemory,
            error.ReadFailed => return error.ReadFailed,
        };
        const payloads = parsePayloads(arena, body) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            else => return respondText(request, .bad_request, try std.fmt.allocPrint(arena, "the body is not a JSON payload list ({{\"payloads\":[...]}}): {t}", .{err}), headers.items),
        };

        const result = (if (encode)
            codec.encodePayloads(h.codecs, arena, io, payloads)
        else
            codec.decodePayloads(h.codecs, arena, io, payloads)) catch |err| switch (err) {
            error.OutOfMemory, error.Canceled => |e| return e,
            error.CodecFailed => return respondText(request, .bad_request, if (encode) "encoding the payloads failed" else "decoding the payloads failed", headers.items),
        };
        const json = if (result.len == 0)
            "{\"payloads\":[]}"
        else
            (Payloads{ .payloads = .fromOwnedSlice(result) }).jsonEncode(.{}, .{ .emit_oneof_field_name = false }, arena) catch return error.OutOfMemory;
        headers.appendAssumeCapacity(.{ .name = "content-type", .value = "application/json" });
        return request.respond(json, .{ .status = .ok, .extra_headers = headers.items });
    }

    fn respondText(request: *http.Server.Request, status: http.Status, text: []const u8, cors: []const http.Header) HandleError!void {
        if (status != .not_found) log.warn("codec server: {d} {s}", .{ @backingInt(status), text });
        var headers_buf: [5]http.Header = undefined;
        var headers: std.ArrayList(http.Header) = .initBuffer(&headers_buf);
        headers.appendSliceAssumeCapacity(cors);
        headers.appendAssumeCapacity(.{ .name = "content-type", .value = "text/plain" });
        return request.respond(text, .{ .status = status, .extra_headers = headers.items });
    }
};

/// Parses ProtoJSON of a `Payloads` by hand: the protobuf library doesn't base64-decode the
/// bytes values of maps such as `metadata` yet (beads d51.2.6), and metadata values may be
/// any bytes. External payload references are dropped.
fn parsePayloads(arena: Allocator, json: []const u8) ![]Payload {
    const Json = struct {
        payloads: []const struct {
            metadata: std.json.ArrayHashMap([]const u8) = .{},
            data: []const u8 = "",
        } = &.{},
    };
    const parsed = try std.json.parseFromSliceLeaky(Json, arena, json, .{ .ignore_unknown_fields = true });
    const out = try arena.alloc(Payload, parsed.payloads.len);
    for (parsed.payloads, out) |in, *p| {
        p.* = .{ .data = try protobuf.json.decodeBase64Lenient(arena, in.data) };
        var it = in.metadata.map.iterator();
        while (it.next()) |entry| try p.metadata.append(arena, .{
            .key = entry.key_ptr.*,
            .value = try protobuf.json.decodeBase64Lenient(arena, entry.value_ptr.*),
        });
    }
    return out;
}

/// A handler that serves `codecs` (borrowed) over HTTP; see `CodecHandler`.
pub fn codecHandler(codecs: []const codec.PayloadCodec, options: CodecHandlerOptions) CodecHandler {
    return .{ .codecs = codecs, .options = options };
}

// ---------------------------------------------------------------- tests

const testing = std.testing;
const converter = @import("../converter.zig");

/// Sends the raw HTTP `request` through `h` and returns the raw response.
fn exchange(h: CodecHandler, arena: Allocator, request: []const u8) ![]const u8 {
    var in: Io.Reader = .fixed(try arena.dupe(u8, request));
    var out: Io.Writer.Allocating = .init(arena);
    var server: http.Server = .init(&in, &out.writer);
    var req = try server.receiveHead();
    try h.handle(arena, testing.io, &req);
    return out.written();
}

fn post(arena: Allocator, path: []const u8, body: []const u8) ![]const u8 {
    return std.fmt.allocPrint(arena, "POST {s} HTTP/1.1\r\nhost: localhost\r\ncontent-type: application/json\r\nx-namespace: default\r\ncontent-length: {d}\r\n\r\n{s}", .{ path, body.len, body });
}

fn bodyOf(response: []const u8) []const u8 {
    const end = std.mem.indexOf(u8, response, "\r\n\r\n") orelse return "";
    return response[end + 4 ..];
}

test "the codec handler encodes and decodes payload lists over HTTP" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const zlib: codec.ZlibCodec = .{ .always_encode = true };
    const h = codecHandler(&.{zlib.codec()}, .{ .allowed_origin = "http://localhost:8233" });

    var original: Payloads = .{};
    try original.payloads.appendSlice(a, (try converter.toPayloads(a, .{ @as([]const u8, "hello"), @as(i64, 42) })).items);
    const request_json = try original.jsonEncode(.{}, .{ .emit_oneof_field_name = false }, a);

    const encoded = try exchange(h, a, try post(a, "/codec/encode", request_json));
    try testing.expect(std.mem.startsWith(u8, encoded, "HTTP/1.1 200 OK\r\n"));
    try testing.expect(std.mem.indexOf(u8, encoded, "access-control-allow-origin: http://localhost:8233\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, encoded, "content-type: application/json\r\n") != null);
    const encoded_list = try parsePayloads(a, bodyOf(encoded));
    try testing.expectEqual(@as(usize, 2), encoded_list.len);
    try testing.expectEqualStrings("binary/zlib", converter.encodingOf(encoded_list[0]).?);

    const decoded = try exchange(h, a, try post(a, "/codec/decode?ignored=1", bodyOf(encoded)));
    try testing.expect(std.mem.startsWith(u8, decoded, "HTTP/1.1 200 OK\r\n"));
    const decoded_list = try parsePayloads(a, bodyOf(decoded));
    const values = try converter.fromPayloads(struct { []const u8, i64 }, a, decoded_list);
    try testing.expectEqualStrings("hello", values[0]);
    try testing.expectEqual(@as(i64, 42), values[1]);

    try testing.expectEqualStrings("{\"payloads\":[]}", bodyOf(try exchange(h, a, try post(a, "/decode", "{}"))));
}

test "the codec handler answers preflight, unknown paths, and bad bodies" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const zlib: codec.ZlibCodec = .{};
    const h = codecHandler(&.{zlib.codec()}, .{ .allowed_origin = "*" });

    const preflight = try exchange(h, a, "OPTIONS /encode HTTP/1.1\r\nhost: localhost\r\norigin: http://ui\r\n\r\n");
    try testing.expect(std.mem.startsWith(u8, preflight, "HTTP/1.1 200 OK\r\n"));
    try testing.expect(std.mem.indexOf(u8, preflight, "access-control-allow-headers: content-type, x-namespace, authorization\r\n") != null);

    try testing.expect(std.mem.startsWith(u8, try exchange(h, a, try post(a, "/other", "{}")), "HTTP/1.1 404 "));
    try testing.expect(std.mem.startsWith(u8, try exchange(h, a, "GET /encode HTTP/1.1\r\nhost: localhost\r\n\r\n"), "HTTP/1.1 405 "));
    try testing.expect(std.mem.startsWith(u8, try exchange(h, a, try post(a, "/encode", "[1, 2")), "HTTP/1.1 400 "));
    const bad_zlib = "{\"payloads\":[{\"metadata\":{\"encoding\":\"YmluYXJ5L3psaWI=\"},\"data\":\"bm90IHpsaWI=\"}]}";
    try testing.expect(std.mem.startsWith(u8, try exchange(h, a, try post(a, "/decode", bad_zlib)), "HTTP/1.1 400 "));

    const no_cors = codecHandler(&.{}, .{});
    const plain = try exchange(no_cors, a, try post(a, "/encode", "{\"payloads\":[]}"));
    try testing.expect(std.mem.indexOf(u8, plain, "access-control") == null);
}
