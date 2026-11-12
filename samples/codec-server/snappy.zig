//! Snappy block compression: the unframed format, a varint of the uncompressed length
//! followed by literal and copy elements. Any Snappy implementation reads what `compress`
//! writes and writes what `decompress` reads.

const std = @import("std");
const Allocator = std.mem.Allocator;

pub const DecompressError = error{ OutOfMemory, Corrupt, TooLarge };

/// Matches never reach back across blocks of this size, so every copy offset fits in the
/// two-byte copy element.
const block_len = 1 << 16;
const hash_bits = 14;

/// Compresses `src` into memory from `arena`.
pub fn compress(arena: Allocator, src: []const u8) error{OutOfMemory}![]u8 {
    var out: std.ArrayList(u8) = try .initCapacity(arena, 32 + src.len + src.len / 6);
    var len = src.len;
    while (len >= 0x80) : (len >>= 7) try out.append(arena, @as(u8, @truncate(len)) | 0x80);
    try out.append(arena, @intCast(len));
    var start: usize = 0;
    while (start < src.len) : (start += block_len) {
        try compressBlock(arena, &out, src[start..@min(src.len, start + block_len)]);
    }
    return out.items;
}

fn compressBlock(arena: Allocator, out: *std.ArrayList(u8), block: []const u8) !void {
    // Positions of earlier 4-byte sequences by hash. A stale or colliding entry is caught
    // by comparing the bytes.
    var table: [1 << hash_bits]u16 = @splat(0);
    var literal_start: usize = 0;
    var i: usize = 0;
    while (i + 4 <= block.len) {
        const v = std.mem.readInt(u32, block[i..][0..4], .little);
        const h = (v *% 0x1e35a7bd) >> (32 - hash_bits);
        const candidate: usize = table[h];
        table[h] = @intCast(i);
        if (candidate >= i or std.mem.readInt(u32, block[candidate..][0..4], .little) != v) {
            i += 1;
            continue;
        }
        var match_len: usize = 4;
        while (i + match_len < block.len and block[candidate + match_len] == block[i + match_len]) match_len += 1;
        try emitLiteral(arena, out, block[literal_start..i]);
        try emitCopy(arena, out, i - candidate, match_len);
        i += match_len;
        literal_start = i;
    }
    try emitLiteral(arena, out, block[literal_start..]);
}

fn emitLiteral(arena: Allocator, out: *std.ArrayList(u8), literal: []const u8) !void {
    if (literal.len == 0) return;
    const n = literal.len - 1;
    if (n < 60) {
        try out.append(arena, @intCast(n << 2));
    } else {
        const extra: u8 = if (n < 1 << 8) 1 else if (n < 1 << 16) 2 else if (n < 1 << 24) 3 else 4;
        try out.append(arena, (59 + extra) << 2);
        for (0..extra) |k| try out.append(arena, @truncate(n >> @intCast(8 * k)));
    }
    try out.appendSlice(arena, literal);
}

fn emitCopy(arena: Allocator, out: *std.ArrayList(u8), offset: usize, len: usize) !void {
    var remaining = len;
    // A copy element holds at most 64 bytes; stop early enough to leave at least 4 behind.
    while (remaining >= 68) : (remaining -= 64) try emitCopy2(arena, out, offset, 64);
    if (remaining > 64) {
        try emitCopy2(arena, out, offset, 60);
        remaining -= 60;
    }
    if (remaining >= 12 or offset >= 2048) return emitCopy2(arena, out, offset, remaining);
    try out.appendSlice(arena, &.{ @intCast(1 | ((remaining - 4) << 2) | ((offset >> 8) << 5)), @truncate(offset) });
}

fn emitCopy2(arena: Allocator, out: *std.ArrayList(u8), offset: usize, len: usize) !void {
    try out.appendSlice(arena, &.{ @intCast(2 | ((len - 1) << 2)), @truncate(offset), @intCast(offset >> 8) });
}

/// Decompresses `src` into memory from `arena`. `error.TooLarge` if it declares more than
/// `max_len` bytes; `error.Corrupt` if it isn't a Snappy block.
pub fn decompress(arena: Allocator, src: []const u8, max_len: usize) DecompressError![]u8 {
    var pos: usize = 0;
    var declared: u64 = 0;
    var shift: u6 = 0;
    while (true) : (shift += 7) {
        if (pos == src.len or shift > 28) return error.Corrupt;
        const b = src[pos];
        pos += 1;
        declared |= @as(u64, b & 0x7f) << shift;
        if (b < 0x80) break;
    }
    if (declared > std.math.maxInt(u32)) return error.Corrupt;
    if (declared > max_len) return error.TooLarge;
    const out = try arena.alloc(u8, @intCast(declared));

    var d: usize = 0;
    while (pos < src.len) {
        const tag = src[pos];
        pos += 1;
        var len: usize = undefined;
        var offset: usize = undefined;
        switch (@as(u2, @truncate(tag))) {
            0 => {
                len = tag >> 2;
                if (len >= 60) {
                    const extra = len - 59;
                    if (src.len - pos < extra) return error.Corrupt;
                    len = 0;
                    for (0..extra) |k| len |= @as(usize, src[pos + k]) << @intCast(8 * k);
                    pos += extra;
                }
                len += 1;
                if (src.len - pos < len or out.len - d < len) return error.Corrupt;
                @memcpy(out[d..][0..len], src[pos..][0..len]);
                pos += len;
                d += len;
                continue;
            },
            1 => {
                if (pos == src.len) return error.Corrupt;
                len = 4 + ((tag >> 2) & 7);
                offset = (@as(usize, tag >> 5) << 8) | src[pos];
                pos += 1;
            },
            2 => {
                if (src.len - pos < 2) return error.Corrupt;
                len = (tag >> 2) + 1;
                offset = std.mem.readInt(u16, src[pos..][0..2], .little);
                pos += 2;
            },
            3 => {
                if (src.len - pos < 4) return error.Corrupt;
                len = (tag >> 2) + 1;
                offset = std.mem.readInt(u32, src[pos..][0..4], .little);
                pos += 4;
            },
        }
        if (offset == 0 or offset > d or out.len - d < len) return error.Corrupt;
        // Byte by byte: a copy may overlap its own output to repeat a short run.
        for (0..len) |k| out[d + k] = out[d + k - offset];
        d += len;
    }
    if (d != out.len) return error.Corrupt;
    return out;
}
