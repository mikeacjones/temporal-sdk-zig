//! A payload codec that compresses payloads with Snappy. Set it as the client's codec
//! (`client.Options.codecs`) in every program that talks to these workflows: the client
//! compresses workflow inputs and decompresses results, and its workers do the same for
//! activity inputs and results. Workflow and activity code never see compressed data.

const std = @import("std");
const temporal = @import("temporal");
const snappy = @import("snappy.zig");
const converter = temporal.converter;
const Allocator = std.mem.Allocator;
const Io = std.Io;
const Payload = converter.Payload;

pub const SnappyCodec = struct {
    /// Compress every payload, even when that makes it larger. Without it, payloads that
    /// compression doesn't shrink are left as they are.
    always_encode: bool = false,
    /// Decoding fails for a payload that declares more decompressed bytes than this, so
    /// corrupt or hostile data can't exhaust memory.
    max_decoded_len: usize = 256 * 1024 * 1024,

    pub const encoding = "binary/snappy";

    /// The codec, borrowing `s`, which must outlive every client that uses it. Safe to use
    /// from several threads at once.
    pub fn codec(s: *const SnappyCodec) converter.PayloadCodec {
        return .{ .ptr = @constCast(s), .vtable = &.{ .encode = encode, .decode = decode } };
    }

    /// Each payload is serialized whole (data and metadata) and replaced by a payload of
    /// encoding `binary/snappy` holding the compressed bytes.
    fn encode(ptr: *anyopaque, arena: Allocator, _: Io, payloads: []const Payload) converter.CodecError![]Payload {
        const s: *const SnappyCodec = @ptrCast(@alignCast(ptr));
        const out = try arena.dupe(Payload, payloads);
        for (out, 0..) |*p, i| {
            var serialized: Io.Writer.Allocating = .init(arena);
            p.encode(&serialized.writer, arena) catch return error.OutOfMemory;
            const plain = serialized.written();
            const compressed = snappy.encode(arena, plain) catch |err| switch (err) {
                error.OutOfMemory => return error.OutOfMemory,
                error.InputTooLarge => {
                    std.log.warn("encoding payload {d} with the snappy codec failed: it is {d} bytes, more than snappy's limit of {d}", .{ i, plain.len, snappy.max_len });
                    return error.CodecFailed;
                },
            };
            if (!s.always_encode and compressed.len >= plain.len) continue;
            p.* = try converter.payloadWithEncoding(arena, encoding, compressed);
        }
        return out;
    }

    /// Decompresses `binary/snappy` payloads and passes the others through.
    fn decode(ptr: *anyopaque, arena: Allocator, _: Io, payloads: []const Payload) converter.CodecError![]Payload {
        const s: *const SnappyCodec = @ptrCast(@alignCast(ptr));
        const out = try arena.dupe(Payload, payloads);
        for (out, 0..) |*p, i| {
            const enc = converter.encodingOf(p.*) orelse continue;
            if (!std.mem.eql(u8, enc, encoding)) continue;
            const plain = snappy.decode(arena, p.data, s.max_decoded_len) catch |err| switch (err) {
                error.OutOfMemory => return error.OutOfMemory,
                error.TooLarge => {
                    std.log.warn("decoding payload {d} with the snappy codec failed: it decompresses to more than max_decoded_len ({d} bytes)", .{ i, s.max_decoded_len });
                    return error.CodecFailed;
                },
                error.Corrupt => {
                    std.log.warn("decoding payload {d} with the snappy codec failed: its data is not snappy-compressed", .{i});
                    return error.CodecFailed;
                },
            };
            var reader: Io.Reader = .fixed(plain);
            p.* = Payload.decode(&reader, arena) catch |err| switch (err) {
                error.OutOfMemory => return error.OutOfMemory,
                else => {
                    std.log.warn("decoding payload {d} with the snappy codec failed: the decompressed data is not a serialized payload ({t})", .{ i, err });
                    return error.CodecFailed;
                },
            };
        }
        return out;
    }
};

/// Compresses every payload, even when compression makes it larger, so every payload of the
/// sample shows up compressed in its history.
pub const always_compress: SnappyCodec = .{ .always_encode = true };
