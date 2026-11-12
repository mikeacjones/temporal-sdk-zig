//! Tests for the Snappy block format: exact encodings from the format's definition, round
//! trips of compressible and incompressible data, and rejection of corrupt data.

const std = @import("std");
const snappy = @import("snappy.zig");

const testing = std.testing;

fn expectRoundTrip(src: []const u8) !void {
    const compressed = try snappy.encode(testing.allocator, src);
    defer testing.allocator.free(compressed);
    try testing.expect(compressed.len <= snappy.maxEncodedLen(src.len));
    try testing.expectEqual(src.len, try snappy.decodedLen(compressed));
    const plain = try snappy.decode(testing.allocator, compressed, src.len);
    defer testing.allocator.free(plain);
    try testing.expectEqualSlices(u8, src, plain);
}

test "encodes a literal and a back-reference as the format defines them" {
    // Length 10, a 3-byte literal "abc", then a copy of 7 bytes from 3 back (overlapping
    // the bytes it writes), in the 1-byte-offset form.
    const expected = [_]u8{ 0x0a, 0x08, 'a', 'b', 'c', 0x0d, 0x03 };
    const compressed = try snappy.encode(testing.allocator, "abcabcabca");
    defer testing.allocator.free(compressed);
    try testing.expectEqualSlices(u8, &expected, compressed);

    const plain = try snappy.decode(testing.allocator, &expected, 100);
    defer testing.allocator.free(plain);
    try testing.expectEqualStrings("abcabcabca", plain);
}

test "decodes copies with 2- and 4-byte offsets" {
    // "ab", then 6 bytes from 2 back.
    for ([_][]const u8{
        &.{ 0x08, 0x04, 'a', 'b', 0x16, 0x02, 0x00 },
        &.{ 0x08, 0x04, 'a', 'b', 0x17, 0x02, 0x00, 0x00, 0x00 },
    }) |compressed| {
        const plain = try snappy.decode(testing.allocator, compressed, 100);
        defer testing.allocator.free(plain);
        try testing.expectEqualStrings("abababab", plain);
    }
}

test "round-trips empty, short, and repetitive data" {
    try expectRoundTrip("");
    try expectRoundTrip("a");
    try expectRoundTrip("Testing");
    try expectRoundTrip("\"My Compressed Friend\"");

    // One long run: copies longer than a single copy element holds.
    const run: [1000]u8 = @splat('z');
    try expectRoundTrip(&run);
    const compressed = try snappy.encode(testing.allocator, &run);
    defer testing.allocator.free(compressed);
    try testing.expect(compressed.len < 60);

    // Text across several 64 KiB blocks, with literals long enough for every length form.
    var text: std.ArrayList(u8) = .empty;
    defer text.deinit(testing.allocator);
    var i: usize = 0;
    while (text.items.len < 200_000) : (i += 1) {
        try text.print(testing.allocator, "line {d}: the quick brown fox jumps over the lazy dog\n", .{i * 7919});
    }
    try expectRoundTrip(text.items);
}

test "round-trips incompressible data, which grows only by the format's overhead" {
    var prng: std.Random.DefaultPrng = .init(0x5eed);
    for ([_]usize{ 1, 59, 60, 61, 300, 70_000, 300_000 }) |len| {
        const data = try testing.allocator.alloc(u8, len);
        defer testing.allocator.free(data);
        prng.random().bytes(data);
        try expectRoundTrip(data);
    }
}

test "rejects corrupt data" {
    const cases = [_][]const u8{
        "", // no length
        &.{ 0x80, 0x80, 0x80, 0x80, 0x80, 0x01 }, // length varint too long
        &.{ 0x05, 0x08, 'a', 'b', 'c' }, // declares 5 bytes, holds 3
        &.{ 0x03, 0x08, 'a' }, // truncated literal
        &.{ 0x05, 0x01, 0x00 }, // copy before any output
        &.{ 0x08, 0x04, 'a', 'b', 0x16, 0x03, 0x00 }, // offset past the start
        &.{ 0x08, 0x04, 'a', 'b', 0x16 }, // truncated copy
        &.{ 0x03, 0x08, 'a', 'b', 'c', 0x01, 0x01 }, // copy past the declared length
    };
    for (cases) |compressed| {
        try testing.expectError(error.Corrupt, snappy.decode(testing.allocator, compressed, 1024));
    }
    try testing.expectError(error.TooLarge, snappy.decode(testing.allocator, &.{ 0x0a, 0x08, 'a', 'b', 'c', 0x0d, 0x03 }, 9));
}
