//! Tests for the codec server program's routing, over HTTP.

const std = @import("std");
const temporal = @import("temporal");
const codec_server = @import("codec_server.zig");
const server = @import("server.zig");
const converter = temporal.converter;

const testing = std.testing;

test "the codec server encodes and decodes for its namespaces only" {
    const io = testing.io;
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    const snappy: codec_server.SnappyCodec = .{};
    const codecs = [_]converter.PayloadCodec{snappy.codec()};
    const namespaces = [_]server.Namespace{.{ .name = "default", .codecs = &codecs }};
    const codec_service: server.CodecServer = .{ .gpa = testing.allocator, .io = io, .namespaces = &namespaces, .allowed_origin = "http://localhost:8233" };
    const address = try std.Io.net.IpAddress.parse("127.0.0.1", 0);
    var listener = try address.listen(io, .{});
    defer listener.deinit(io);
    var serving = try io.concurrent(server.CodecServer.serve, .{ &codec_service, &listener });
    defer serving.cancel(io) catch {};
    const base = try std.fmt.allocPrint(a, "http://127.0.0.1:{d}", .{listener.socket.address.getPort()});

    var http: std.http.Client = .{ .allocator = testing.allocator, .io = io };
    defer http.deinit();

    const original = try converter.toPayloads(a, .{ @as([]const u8, "Plain text input"), @as(i64, 3) });
    // The namespace in the path.
    const encoded = try server.postPayloads(&http, a, try std.fmt.allocPrint(a, "{s}/default/encode", .{base}), "", original.items);
    try testing.expectEqual(@as(usize, 2), encoded.len);
    try testing.expectEqualStrings("binary/snappy", converter.encodingOf(encoded[0]).?);
    // The namespace in the X-Namespace header.
    const decoded = try server.postPayloads(&http, a, try std.fmt.allocPrint(a, "{s}/decode", .{base}), "default", encoded);
    const values = try converter.fromPayloads(struct { []const u8, i64 }, a, decoded);
    try testing.expectEqualStrings("Plain text input", values[0]);
    try testing.expectEqual(@as(i64, 3), values[1]);

    const Status = struct {
        fn of(client: *std.http.Client, url: []const u8, method: std.http.Method, namespace: []const u8, body: ?[]const u8) !std.http.Status {
            var header = [_]std.http.Header{.{ .name = "x-namespace", .value = namespace }};
            const response = try client.fetch(.{
                .location = .{ .url = url },
                .method = method,
                .payload = body,
                .extra_headers = if (namespace.len > 0) &header else &.{},
            });
            return response.status;
        }
    };
    const empty = "{\"payloads\":[]}";
    try testing.expectEqual(.not_found, try Status.of(&http, try std.fmt.allocPrint(a, "{s}/other/decode", .{base}), .POST, "", empty));
    try testing.expectEqual(.not_found, try Status.of(&http, try std.fmt.allocPrint(a, "{s}/decode", .{base}), .POST, "other", empty));
    try testing.expectEqual(.not_found, try Status.of(&http, try std.fmt.allocPrint(a, "{s}/decode", .{base}), .POST, "", empty));
    try testing.expectEqual(.not_found, try Status.of(&http, try std.fmt.allocPrint(a, "{s}/default/other", .{base}), .POST, "", empty));
    try testing.expectEqual(.ok, try Status.of(&http, try std.fmt.allocPrint(a, "{s}/decode", .{base}), .OPTIONS, "", null));
    // Not a Snappy block.
    const corrupt = "{\"payloads\":[{\"metadata\":{\"encoding\":\"YmluYXJ5L3NuYXBweQ==\"},\"data\":\"bm90IHNuYXBweQ==\"}]}";
    try testing.expectEqual(.bad_request, try Status.of(&http, try std.fmt.allocPrint(a, "{s}/default/decode", .{base}), .POST, "", corrupt));
    try testing.expectEqual(.bad_request, try Status.of(&http, try std.fmt.allocPrint(a, "{s}/default/decode", .{base}), .POST, "", "[1, 2"));
}

test "the codec server answers preflight requests with CORS headers" {
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const codec_service: server.CodecServer = .{ .gpa = testing.allocator, .io = testing.io, .namespaces = &.{}, .allowed_origin = "http://localhost:8233" };

    var in: std.Io.Reader = .fixed("OPTIONS /default/decode HTTP/1.1\r\nhost: localhost\r\norigin: http://localhost:8233\r\n\r\n");
    var out: std.Io.Writer.Allocating = .init(a);
    var http_server: std.http.Server = .init(&in, &out.writer);
    var request = try http_server.receiveHead();
    try codec_service.handle(a, &request);
    try testing.expect(std.mem.startsWith(u8, out.written(), "HTTP/1.1 200 OK\r\n"));
    try testing.expect(std.mem.find(u8, out.written(), "access-control-allow-origin: http://localhost:8233\r\n") != null);
    try testing.expect(std.mem.find(u8, out.written(), "x-namespace") != null);
}

test "the codec server program compiles" {
    _ = &server.main;
}
