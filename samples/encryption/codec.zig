//! A payload codec that encrypts payloads with AES-256-GCM. Each encrypted payload names the
//! key that encrypted it in its metadata, so decoding needs no context: the codec server
//! (`codec_server.zig`) decodes with the same codec.
//!
//! Every program sets the same chain in `client.Options.codecs`: `zlib` first, so payloads
//! are compressed before they are encrypted (encrypted data doesn't compress), then the
//! encryption codec.

const std = @import("std");
const temporal = @import("temporal");
const encryption = @import("encryption.zig");
const converter = temporal.converter;
const Allocator = std.mem.Allocator;
const Io = std.Io;
const Payload = converter.Payload;
const Aes256Gcm = std.crypto.aead.aes_gcm.Aes256Gcm;

const log = std.log.scoped(.encryption);

/// The encoding of an encrypted payload.
pub const encoding = "binary/encrypted";
/// The metadata entry naming the key an encrypted payload was encrypted with.
pub const key_id_metadata = "encryption-key-id";

/// An encryption key and the ID payloads name it by.
pub const Key = struct {
    id: []const u8,
    bytes: [Aes256Gcm.key_length]u8,
};

/// The keys the sample's programs share. In production, fetch keys from secure storage
/// such as a key management service instead; this one is for the demo only.
pub const demo_keys = [_]Key{.{ .id = "test", .bytes = "test-key-test-key-test-key-test!".* }};

pub const EncryptionCodec = struct {
    /// The keys this codec encrypts and decrypts with, borrowed.
    keys: []const Key,
    /// The key ID to encrypt with when the call doesn't propagate one (`encryption.CryptContext`).
    /// Empty means every encrypting call must propagate one.
    key_id: []const u8 = "",

    /// The codec, borrowing `e`, which must outlive every client that uses it. Safe to use
    /// from several threads at once.
    pub fn codec(e: *const EncryptionCodec) converter.PayloadCodec {
        return .{ .ptr = @constCast(e), .vtable = &.{
            .encode = encode,
            .decode = decode,
            .encodeWithContext = encodeWithContext,
            .decodeWithContext = decodeWithContext,
        } };
    }

    fn keyFor(e: *const EncryptionCodec, id: []const u8) ?*const [Aes256Gcm.key_length]u8 {
        for (e.keys) |*k| if (std.mem.eql(u8, k.id, id)) return &k.bytes;
        return null;
    }

    fn encode(ptr: *anyopaque, arena: Allocator, io: Io, payloads: []const Payload) converter.CodecError![]Payload {
        return encodeWithContext(ptr, arena, io, &.{}, payloads);
    }

    /// Each payload is serialized whole (data and metadata) and replaced by a payload of
    /// encoding `binary/encrypted` holding the nonce, the ciphertext, and the tag, encrypted
    /// with the key the call propagates, or else `key_id`.
    fn encodeWithContext(ptr: *anyopaque, arena: Allocator, io: Io, context: *const converter.CodecContext, payloads: []const Payload) converter.CodecError![]Payload {
        const e: *const EncryptionCodec = @ptrCast(@alignCast(ptr));
        const id = if (context.values.get(encryption.propagate_key)) |c| c.key_id else e.key_id;
        if (id.len == 0) {
            log.warn("encrypting payloads for workflow \"{s}\" failed: the call propagates no key ID and the codec has no default key_id", .{context.workflow_id});
            return error.CodecFailed;
        }
        const key = e.keyFor(id) orelse {
            log.warn("encrypting payloads for workflow \"{s}\" failed: no key has the ID \"{s}\"", .{ context.workflow_id, id });
            return error.CodecFailed;
        };
        const out = try arena.alloc(Payload, payloads.len);
        for (payloads, out) |p, *o| {
            var serialized: Io.Writer.Allocating = .init(arena);
            p.encode(&serialized.writer, arena) catch return error.OutOfMemory;
            const plain = serialized.written();
            const sealed = try arena.alloc(u8, Aes256Gcm.nonce_length + plain.len + Aes256Gcm.tag_length);
            const nonce = sealed[0..Aes256Gcm.nonce_length];
            io.random(nonce);
            Aes256Gcm.encrypt(sealed[Aes256Gcm.nonce_length..][0..plain.len], sealed[sealed.len - Aes256Gcm.tag_length ..][0..Aes256Gcm.tag_length], plain, "", nonce.*, key.*);
            o.* = try converter.payloadWithEncoding(arena, encoding, sealed);
            try o.metadata.append(arena, .{ .key = key_id_metadata, .value = id });
        }
        return out;
    }

    fn decodeWithContext(ptr: *anyopaque, arena: Allocator, io: Io, _: *const converter.CodecContext, payloads: []const Payload) converter.CodecError![]Payload {
        return decode(ptr, arena, io, payloads);
    }

    /// Decrypts `binary/encrypted` payloads with the key their metadata names and passes the
    /// others through.
    fn decode(ptr: *anyopaque, arena: Allocator, _: Io, payloads: []const Payload) converter.CodecError![]Payload {
        const e: *const EncryptionCodec = @ptrCast(@alignCast(ptr));
        const out = try arena.dupe(Payload, payloads);
        for (out, 0..) |*p, i| {
            const enc = converter.encodingOf(p.*) orelse continue;
            if (!std.mem.eql(u8, enc, encoding)) continue;
            const id = keyIdOf(p.*) orelse {
                log.warn("decrypting payload {d} failed: it has no {s} metadata entry", .{ i, key_id_metadata });
                return error.CodecFailed;
            };
            const key = e.keyFor(id) orelse {
                log.warn("decrypting payload {d} failed: no key has the ID \"{s}\" it names", .{ i, id });
                return error.CodecFailed;
            };
            if (p.data.len < Aes256Gcm.nonce_length + Aes256Gcm.tag_length) {
                log.warn("decrypting payload {d} failed: its {d} bytes are too short to hold a nonce and a tag", .{ i, p.data.len });
                return error.CodecFailed;
            }
            const nonce = p.data[0..Aes256Gcm.nonce_length];
            const ciphertext = p.data[Aes256Gcm.nonce_length .. p.data.len - Aes256Gcm.tag_length];
            const tag = p.data[p.data.len - Aes256Gcm.tag_length ..][0..Aes256Gcm.tag_length];
            const plain = try arena.alloc(u8, ciphertext.len);
            Aes256Gcm.decrypt(plain, ciphertext, tag.*, "", nonce.*, key.*) catch {
                log.warn("decrypting payload {d} with key \"{s}\" failed: the data doesn't authenticate (wrong key or tampered data)", .{ i, id });
                return error.CodecFailed;
            };
            var reader: Io.Reader = .fixed(plain);
            p.* = Payload.decode(&reader, arena) catch |err| switch (err) {
                error.OutOfMemory => return error.OutOfMemory,
                else => {
                    log.warn("decrypting payload {d} failed: the decrypted data is not a serialized payload ({t})", .{ i, err });
                    return error.CodecFailed;
                },
            };
        }
        return out;
    }
};

/// The ID of the key that encrypted `p`, or null if it names none.
pub fn keyIdOf(p: Payload) ?[]const u8 {
    for (p.metadata.items) |m| if (std.mem.eql(u8, m.key, key_id_metadata)) return m.value;
    return null;
}

/// Compresses every payload before it is encrypted, even when that makes it larger.
pub const zlib: converter.ZlibCodec = .{ .always_encode = true };

/// Encrypts with the demo keys, with the key ID each call propagates.
pub const demo_encryption: EncryptionCodec = .{ .keys = &demo_keys };

/// The chain every program sets in `client.Options.codecs`.
pub const codecs = [_]converter.PayloadCodec{ zlib.codec(), demo_encryption.codec() };
