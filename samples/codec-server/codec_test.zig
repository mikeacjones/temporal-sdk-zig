//! Tests for the Snappy compression and the codec built on it.

const std = @import("std");
const temporal = @import("temporal");
const codec_server = @import("codec_server.zig");
const snappy = @import("snappy.zig");
const converter = temporal.converter;

const testing = std.testing;

fn expectRoundTrip(a: std.mem.Allocator, data: []const u8) ![]const u8 {
    const compressed = try snappy.compress(a, data);
    try testing.expectEqualSlices(u8, data, try snappy.decompress(a, compressed, data.len));
    return compressed;
}

test "snappy round-trips short, repetitive, and multi-block data" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    try testing.expectEqualSlices(u8, "\x00", try expectRoundTrip(a, ""));
    try testing.expectEqualSlices(u8, "\x05\x10hello", try expectRoundTrip(a, "hello"));

    const repetitive = try a.alloc(u8, 200_000);
    for (repetitive, 0..) |*b, i| b.* = "codec server "[i % 13];
    try testing.expect((try expectRoundTrip(a, repetitive)).len < repetitive.len / 20);

    // Random bytes don't compress: long literals, with every length encoding.
    var prng: std.Random.DefaultPrng = .init(1);
    const random = try a.alloc(u8, 3 * 65536 + 7);
    prng.random().bytes(random);
    _ = try expectRoundTrip(a, random);
    for ([_]usize{ 59, 60, 61, 255, 256, 257, 4095 }) |len| _ = try expectRoundTrip(a, random[0..len]);

    // Matches at offsets that need two-byte offsets, and runs that overlap their copy.
    const mixed = try a.alloc(u8, 10_000);
    for (mixed, 0..) |*b, i| b.* = if (i % 3000 < 100) random[i % 100] else @truncate(i * 7);
    _ = try expectRoundTrip(a, mixed);
    _ = try expectRoundTrip(a, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
}

test "snappy reads copies written by other encoders and rejects corrupt blocks" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // A 4-byte literal, then a one-byte-offset copy of 8 bytes from 4 back.
    try testing.expectEqualStrings("abcdabcdabcd", try snappy.decompress(a, "\x0c\x0cabcd\x11\x04", 100));
    // The same with a two-byte-offset copy, and a four-byte one.
    try testing.expectEqualStrings("abcdabcdabcd", try snappy.decompress(a, "\x0c\x0cabcd\x1e\x04\x00", 100));
    try testing.expectEqualStrings("abcdabcdabcd", try snappy.decompress(a, "\x0c\x0cabcd\x1f\x04\x00\x00\x00", 100));

    try testing.expectError(error.TooLarge, snappy.decompress(a, "\x0c\x0cabcd\x11\x04", 11));
    try testing.expectError(error.Corrupt, snappy.decompress(a, "", 100));
    // Shorter or longer than declared.
    try testing.expectError(error.Corrupt, snappy.decompress(a, "\x0d\x0cabcd\x11\x04", 100));
    try testing.expectError(error.Corrupt, snappy.decompress(a, "\x04\x0cabcd\x11\x04", 100));
    // A copy reaching before the start, and a truncated literal.
    try testing.expectError(error.Corrupt, snappy.decompress(a, "\x0c\x0cabcd\x11\x05", 100));
    try testing.expectError(error.Corrupt, snappy.decompress(a, "\x0c\x0cab", 100));
    try testing.expectError(error.Corrupt, snappy.decompress(a, "\xff\xff\xff\xff\xff\xff", 100));
}

test "the snappy codec compresses every payload and decodes only its own" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const snappy_codec: codec_server.SnappyCodec = .{};
    const codec = snappy_codec.codec();

    const original = try converter.toPayloads(a, .{ @as([]const u8, "Plain text input"), @as(i64, 7) });
    const encoded = try codec.encode(a, testing.io, original.items);
    try testing.expectEqual(@as(usize, 2), encoded.len);
    for (encoded) |p| {
        // Only the encoding is visible: the original metadata is compressed with the data.
        try testing.expectEqual(@as(usize, 1), p.metadata.items.len);
        try testing.expectEqualStrings("binary/snappy", converter.encodingOf(p).?);
    }

    const plain = try converter.toPayload(a, @as([]const u8, "not compressed"));
    const decoded = try codec.decode(a, testing.io, &.{ encoded[0], plain, encoded[1] });
    try testing.expectEqualStrings("Plain text input", try converter.fromPayload([]const u8, a, decoded[0]));
    try testing.expectEqualStrings("not compressed", try converter.fromPayload([]const u8, a, decoded[1]));
    try testing.expectEqual(@as(i64, 7), try converter.fromPayload(i64, a, decoded[2]));

    var corrupt = encoded[0];
    corrupt.data = "\x05\x10abc";
    try testing.expectError(error.CodecFailed, codec.decode(a, testing.io, &.{corrupt}));
    // Valid Snappy that doesn't hold a serialized payload.
    corrupt.data = try snappy.compress(a, "\x12\x10ab");
    try testing.expectError(error.CodecFailed, codec.decode(a, testing.io, &.{corrupt}));
    const tight: codec_server.SnappyCodec = .{ .max_decoded_len = 4 };
    try testing.expectError(error.CodecFailed, tight.codec().decode(a, testing.io, encoded[0..1]));
}
