//! Tests for the encryption codec and the codec server's handler.

const std = @import("std");
const temporal = @import("temporal");
const encryption = @import("encryption.zig");
const codec = @import("codec.zig");
const converter = temporal.converter;

const testing = std.testing;

fn testValues(a: std.mem.Allocator, key_id: []const u8) !temporal.propagation.Values {
    var values: temporal.propagation.Values = .init(a);
    try values.put(encryption.propagate_key, .{ .key_id = key_id });
    return values;
}

test "the codecs encrypt with the propagated key and decode without a context" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const values = try testValues(a, "test");
    const context: converter.CodecContext = .{ .values = &values };
    const default_payload = try converter.toPayload(a, @as([]const u8, "Testing"));
    const encrypted = try converter.encodePayloadsWithContext(&codec.codecs, a, testing.io, &context, &.{default_payload});
    try testing.expectEqual(@as(usize, 1), encrypted.len);
    try testing.expect(!std.mem.eql(u8, default_payload.data, encrypted[0].data));
    try testing.expectEqualStrings("binary/encrypted", converter.encodingOf(encrypted[0]).?);
    try testing.expectEqualStrings("test", codec.keyIdOf(encrypted[0]).?);

    const decoded = try converter.decodePayloads(&codec.codecs, a, testing.io, encrypted);
    try testing.expectEqualStrings("Testing", try converter.fromPayload([]const u8, a, decoded[0]));

    // A fresh nonce each time: the same payload never encrypts to the same bytes.
    const again = try converter.encodePayloadsWithContext(&codec.codecs, a, testing.io, &context, &.{default_payload});
    try testing.expect(!std.mem.eql(u8, encrypted[0].data, again[0].data));
}

test "the codec falls back to its own key ID and refuses calls with no key" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const payload = try converter.toPayload(a, @as(i64, 7));

    const keyless: codec.EncryptionCodec = .{ .keys = &codec.demo_keys };
    try testing.expectError(error.CodecFailed, keyless.codec().encode(a, testing.io, &.{payload}));
    const unknown = try testValues(a, "other");
    try testing.expectError(error.CodecFailed, keyless.codec().encodeWithContext(a, testing.io, &.{ .values = &unknown }, &.{payload}));

    const defaulted: codec.EncryptionCodec = .{ .keys = &codec.demo_keys, .key_id = "test" };
    const encrypted = try defaulted.codec().encode(a, testing.io, &.{payload});
    try testing.expectEqualStrings("test", codec.keyIdOf(encrypted[0]).?);
    try testing.expectEqual(@as(i64, 7), try converter.fromPayload(i64, a, (try keyless.codec().decode(a, testing.io, encrypted))[0]));
}

test "decoding passes plain payloads through and rejects tampered or unknown-key data" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const c = codec.demo_encryption.codec();
    const values = try testValues(a, "test");

    const encrypted = try c.encodeWithContext(a, testing.io, &.{ .values = &values }, &.{try converter.toPayload(a, @as([]const u8, "secret"))});
    const plain = try converter.toPayload(a, @as([]const u8, "not encrypted"));
    const decoded = try c.decode(a, testing.io, &.{ plain, encrypted[0] });
    try testing.expectEqualStrings("not encrypted", try converter.fromPayload([]const u8, a, decoded[0]));
    try testing.expectEqualStrings("secret", try converter.fromPayload([]const u8, a, decoded[1]));

    var tampered = encrypted[0];
    const data = try a.dupe(u8, tampered.data);
    data[data.len / 2] ^= 1;
    tampered.data = data;
    try testing.expectError(error.CodecFailed, c.decode(a, testing.io, &.{tampered}));

    var short = encrypted[0];
    short.data = short.data[0..10];
    try testing.expectError(error.CodecFailed, c.decode(a, testing.io, &.{short}));

    const other_keys = [_]codec.Key{.{ .id = "other", .bytes = @splat(1) }};
    const other: codec.EncryptionCodec = .{ .keys = &other_keys };
    try testing.expectError(error.CodecFailed, other.codec().decode(a, testing.io, encrypted));

    var unnamed = try converter.payloadWithEncoding(a, codec.encoding, encrypted[0].data);
    _ = &unnamed;
    try testing.expectError(error.CodecFailed, c.decode(a, testing.io, &.{unnamed}));
}

test "the codec server decrypts payloads over HTTP" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const values = try testValues(a, "test");

    var encrypted: converter.Payloads = .{};
    try encrypted.payloads.appendSlice(a, try converter.encodePayloadsWithContext(&codec.codecs, a, testing.io, &.{ .values = &values }, &.{try converter.toPayload(a, @as([]const u8, "Testing"))}));
    const body = try encrypted.jsonEncode(.{}, .{ .emit_oneof_field_name = false }, a);
    const request = try std.fmt.allocPrint(a, "POST /decode HTTP/1.1\r\nhost: localhost\r\ncontent-type: application/json\r\ncontent-length: {d}\r\n\r\n{s}", .{ body.len, body });

    var in: std.Io.Reader = .fixed(request);
    var out: std.Io.Writer.Allocating = .init(a);
    var server: std.http.Server = .init(&in, &out.writer);
    var req = try server.receiveHead();
    try converter.codecHandler(&codec.codecs, .{}).handle(a, testing.io, &req);

    const response = out.written();
    try testing.expect(std.mem.startsWith(u8, response, "HTTP/1.1 200 OK\r\n"));
    // Base64 of the encoding "json/plain" and of the data "\"Testing\"".
    try testing.expect(std.mem.indexOf(u8, response, "anNvbi9wbGFpbg==") != null);
    try testing.expect(std.mem.indexOf(u8, response, "IlRlc3Rpbmci") != null);
}

test "the codec server program compiles" {
    _ = &@import("codec_server.zig").main;
}
