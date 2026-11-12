//! The Snappy block format: a varint of the uncompressed length, then a sequence of literals
//! (bytes copied from the input) and copies (back-references into the output already
//! written). Data compressed here decodes with any Snappy implementation, and the other way
//! round. The framed stream format is not implemented: payloads are compressed whole.

const std = @import("std");
const Allocator = std.mem.Allocator;

/// The format stores the uncompressed length in 32 bits.
pub const max_len = std.math.maxInt(u32);

/// Compression works on independent 64 KiB blocks, so every copy offset fits in 16 bits and
/// the hash table can hold 16-bit positions.
const block_len = 1 << 16;
const hash_bits = 14;
const min_match = 4;

const tag_literal = 0b00;
const tag_copy1 = 0b01;
const tag_copy2 = 0b10;
const tag_copy4 = 0b11;

/// The most bytes `encode` can produce for `src_len` input bytes.
pub fn maxEncodedLen(src_len: usize) usize {
    return 32 + src_len + src_len / 6;
}

/// Compresses `src`. The caller owns the result, allocated from `gpa`.
pub fn encode(gpa: Allocator, src: []const u8) error{ OutOfMemory, InputTooLarge }![]u8 {
    if (src.len > max_len) return error.InputTooLarge;
    var out: std.ArrayList(u8) = try .initCapacity(gpa, maxEncodedLen(src.len));
    errdefer out.deinit(gpa);
    var len = src.len;
    while (len >= 0x80) : (len >>= 7) out.appendAssumeCapacity(@as(u8, @truncate(len)) | 0x80);
    out.appendAssumeCapacity(@intCast(len));
    var start: usize = 0;
    while (start < src.len) : (start += block_len) {
        encodeBlock(&out, src[start..@min(src.len, start + block_len)]);
    }
    return out.toOwnedSlice(gpa);
}

/// Greedy matching: a hash of the next 4 bytes finds the last position in the block that
/// hashed the same; if its bytes match, the match is extended and emitted as a copy.
fn encodeBlock(out: *std.ArrayList(u8), block: []const u8) void {
    var table: [1 << hash_bits]u16 = @splat(0);
    var literal_start: usize = 0;
    var i: usize = 0;
    while (i + min_match <= block.len) {
        const v = load32(block, i);
        const h = hash(v);
        const candidate: usize = table[h];
        table[h] = @intCast(i);
        if (candidate >= i or load32(block, candidate) != v) {
            i += 1;
            continue;
        }
        var match_len: usize = min_match;
        while (i + match_len < block.len and block[candidate + match_len] == block[i + match_len]) match_len += 1;
        emitLiteral(out, block[literal_start..i]);
        emitCopy(out, i - candidate, match_len);
        i += match_len;
        literal_start = i;
    }
    emitLiteral(out, block[literal_start..]);
}

fn load32(b: []const u8, i: usize) u32 {
    return std.mem.readInt(u32, b[i..][0..4], .little);
}

fn hash(v: u32) usize {
    return (v *% 0x1e35a7bd) >> (32 - hash_bits);
}

fn emitLiteral(out: *std.ArrayList(u8), literal: []const u8) void {
    if (literal.len == 0) return;
    const n = literal.len - 1;
    if (n < 60) {
        out.appendAssumeCapacity(@as(u8, @intCast(n)) << 2 | tag_literal);
    } else {
        // 60 to 63 say that the length follows in 1 to 4 little-endian bytes.
        const bytes: u8 = if (n < 1 << 8) 1 else if (n < 1 << 16) 2 else if (n < 1 << 24) 3 else 4;
        out.appendAssumeCapacity((59 + bytes) << 2 | tag_literal);
        var rest = n;
        for (0..bytes) |_| {
            out.appendAssumeCapacity(@truncate(rest));
            rest >>= 8;
        }
    }
    out.appendSliceAssumeCapacity(literal);
}

fn emitCopy(out: *std.ArrayList(u8), offset: usize, len: usize) void {
    var rest = len;
    // A 2-byte-offset copy holds at most 64 bytes. Splitting off 60 rather than 64 when 65
    // to 67 remain leaves at least 4 for the last copy.
    while (rest >= 68) : (rest -= 64) emitCopy2(out, offset, 64);
    if (rest > 64) {
        emitCopy2(out, offset, 60);
        rest -= 60;
    }
    if (rest >= 12 or offset >= 2048) return emitCopy2(out, offset, rest);
    // 4 to 11 bytes at an offset below 2048 fit in two bytes.
    out.appendAssumeCapacity(@as(u8, @intCast(offset >> 8)) << 5 | @as(u8, @intCast(rest - 4)) << 2 | tag_copy1);
    out.appendAssumeCapacity(@truncate(offset));
}

fn emitCopy2(out: *std.ArrayList(u8), offset: usize, len: usize) void {
    out.appendAssumeCapacity(@as(u8, @intCast(len - 1)) << 2 | tag_copy2);
    out.appendAssumeCapacity(@truncate(offset));
    out.appendAssumeCapacity(@truncate(offset >> 8));
}

pub const DecodeError = error{
    OutOfMemory,
    /// The data is not in the Snappy block format, or is truncated.
    Corrupt,
    /// The data decompresses to more than the limit the caller gave.
    TooLarge,
};

/// The uncompressed length that `src` declares.
pub fn decodedLen(src: []const u8) error{Corrupt}!usize {
    return (try readLength(src)).len;
}

fn readLength(src: []const u8) error{Corrupt}!struct { len: usize, header_len: usize } {
    var value: u64 = 0;
    for (src[0..@min(src.len, 5)], 0..) |b, i| {
        value |= @as(u64, b & 0x7f) << @intCast(7 * i);
        if (b & 0x80 == 0) {
            if (value > max_len) return error.Corrupt;
            return .{ .len = @intCast(value), .header_len = i + 1 };
        }
    }
    return error.Corrupt;
}

/// Decompresses `src`, refusing data that declares more than `limit` bytes so corrupt or
/// hostile input can't exhaust memory. The caller owns the result, allocated from `gpa`.
pub fn decode(gpa: Allocator, src: []const u8, limit: usize) DecodeError![]u8 {
    const header = try readLength(src);
    if (header.len > limit) return error.TooLarge;
    const dst = try gpa.alloc(u8, header.len);
    errdefer gpa.free(dst);
    var s = header.header_len;
    var d: usize = 0;
    while (s < src.len) {
        const tag = src[s];
        s += 1;
        switch (tag & 0b11) {
            tag_literal => {
                var n: u64 = tag >> 2;
                if (n >= 60) {
                    const bytes: usize = n - 59;
                    if (src.len - s < bytes) return error.Corrupt;
                    n = 0;
                    for (src[s..][0..bytes], 0..) |b, i| n |= @as(u64, b) << @intCast(8 * i);
                    s += bytes;
                }
                const len = n + 1;
                if (len > src.len - s or len > dst.len - d) return error.Corrupt;
                @memcpy(dst[d..][0..@intCast(len)], src[s..][0..@intCast(len)]);
                s += @intCast(len);
                d += @intCast(len);
                continue;
            },
            tag_copy1 => {
                if (src.len - s < 1) return error.Corrupt;
                try copy(dst, &d, @as(usize, tag >> 5) << 8 | src[s], (tag >> 2 & 0b111) + 4);
                s += 1;
            },
            tag_copy2 => {
                if (src.len - s < 2) return error.Corrupt;
                try copy(dst, &d, std.mem.readInt(u16, src[s..][0..2], .little), (tag >> 2) + 1);
                s += 2;
            },
            tag_copy4 => {
                if (src.len - s < 4) return error.Corrupt;
                try copy(dst, &d, std.mem.readInt(u32, src[s..][0..4], .little), (tag >> 2) + 1);
                s += 4;
            },
            else => unreachable,
        }
    }
    if (d != dst.len) return error.Corrupt;
    return dst;
}

/// Byte by byte, because a copy may overlap the bytes it produces (an offset shorter than
/// the length repeats them).
fn copy(dst: []u8, d: *usize, offset: usize, len: usize) error{Corrupt}!void {
    if (offset == 0 or offset > d.* or len > dst.len - d.*) return error.Corrupt;
    for (0..len) |_| {
        dst[d.*] = dst[d.* - offset];
        d.* += 1;
    }
}
