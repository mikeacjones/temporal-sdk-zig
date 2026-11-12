//! Tests for the snappy payload codec and the codec server's handler.

const std = @import("std");
const temporal = @import("temporal");
const codec = @import("codec.zig");
const converter = temporal.converter;

const testing = std.testing;

test "the always-compress codec changes a payload's data and decodes it back" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const default_payload = try converter.toPayload(a, @as([]const u8, "Testing"));
    const compressed = try codec.always_compress.codec().encode(a, testing.io, &.{default_payload});
    try testing.expectEqual(@as(usize, 1), compressed.len);
    try testing.expectEqualStrings("binary/snappy", converter.encodingOf(compressed[0]).?);
    try testing.expect(!std.mem.eql(u8, default_payload.data, compressed[0].data));

    const decoded = try codec.always_compress.codec().decode(a, testing.io, compressed);
    try testing.expectEqualStrings("Testing", try converter.fromPayload([]const u8, a, decoded[0]));
}

test "the default codec compresses only payloads it shrinks and decodes only its own" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const snappy_codec: codec.SnappyCodec = .{};

    const text = try a.alloc(u8, 4000);
    for (text, 0..) |*c, i| c.* = "compress me "[i % 12];
    const big = try converter.toPayload(a, @as([]const u8, text));
    const small = try converter.toPayload(a, @as(i64, 7));
    const encoded = try snappy_codec.codec().encode(a, testing.io, &.{ big, small });
    try testing.expectEqualStrings("binary/snappy", converter.encodingOf(encoded[0]).?);
    try testing.expect(encoded[0].data.len < big.data.len / 10);
    try testing.expectEqualStrings("json/plain", converter.encodingOf(encoded[1]).?);
    try testing.expectEqualStrings("7", encoded[1].data);

    const decoded = try snappy_codec.codec().decode(a, testing.io, encoded);
    try testing.expectEqualStrings(text, try converter.fromPayload([]const u8, a, decoded[0]));
    try testing.expectEqual(@as(i64, 7), try converter.fromPayload(i64, a, decoded[1]));

    var corrupt = encoded[0];
    corrupt.data = "not snappy";
    try testing.expectError(error.CodecFailed, snappy_codec.codec().decode(a, testing.io, &.{corrupt}));
    const tight: codec.SnappyCodec = .{ .max_decoded_len = 100 };
    try testing.expectError(error.CodecFailed, tight.codec().decode(a, testing.io, encoded[0..1]));
}

test "the codec server decodes compressed payloads over HTTP" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const codecs = [_]converter.PayloadCodec{codec.always_compress.codec()};

    var compressed: converter.Payloads = .{};
    try compressed.payloads.appendSlice(a, try codecs[0].encode(a, testing.io, &.{try converter.toPayload(a, @as([]const u8, "Testing"))}));
    const body = try compressed.jsonEncode(.{}, .{ .emit_oneof_field_name = false }, a);
    const request = try std.fmt.allocPrint(a, "POST /decode HTTP/1.1\r\nhost: localhost\r\ncontent-type: application/json\r\ncontent-length: {d}\r\n\r\n{s}", .{ body.len, body });

    var in: std.Io.Reader = .fixed(request);
    var out: std.Io.Writer.Allocating = .init(a);
    var server: std.http.Server = .init(&in, &out.writer);
    var req = try server.receiveHead();
    try converter.codecHandler(&codecs, .{}).handle(a, testing.io, &req);

    const response = out.written();
    try testing.expect(std.mem.startsWith(u8, response, "HTTP/1.1 200 OK\r\n"));
    // Base64 of the encoding "json/plain" and of the data "\"Testing\"".
    try testing.expect(std.mem.indexOf(u8, response, "anNvbi9wbGFpbg==") != null);
    try testing.expect(std.mem.indexOf(u8, response, "IlRlc3Rpbmci") != null);
}
