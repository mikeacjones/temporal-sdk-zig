//! A payload codec that picks an AES-128-GCM key by the Nexus endpoint a payload belongs to.
//! The SDK hands every codec call a `converter.Context`; for the payloads of a Nexus operation
//! (the caller's input, the handler's result and failures, and the result of the workflow a
//! handler started) its `nexus` field names the endpoint, service and operation, on the
//! caller's side and on the handler's alike. Any other payload has no `nexus` and gets the
//! codec's default key.
//!
//! An endpoint's payloads may also be compressed with zlib before they are encrypted
//! (encrypted data doesn't compress). Each encrypted payload names its key and endpoint in its
//! metadata, and the endpoint is the cipher's associated data, so a payload encrypted for one
//! endpoint fails to decode as another's.
//!
//! AES-GCM is symmetric: the caller and the handler share each endpoint's key, for requests and
//! results alike. The keys here are for the demo only; in production, keep keys in a key
//! management service and use an established encryption scheme.

const std = @import("std");
const temporal = @import("temporal");
const service = @import("service.zig");
const converter = temporal.converter;
const Allocator = std.mem.Allocator;
const Io = std.Io;
const Payload = converter.Payload;
pub const Aes128Gcm = std.crypto.aead.aes_gcm.Aes128Gcm;

const log = std.log.scoped(.nexus_serialization_context);

/// The encoding of an encrypted payload.
pub const encoding = "binary/aes-gcm";
/// The metadata entry naming the key an encrypted payload was encrypted with.
pub const key_id_metadata = "encryption-key-id";
/// The metadata entry naming the Nexus endpoint of an encrypted Nexus payload; absent on
/// other payloads.
pub const endpoint_metadata = "nexus-endpoint";

/// An encryption key and the ID payloads name it by.
pub const Key = struct {
    id: []const u8,
    bytes: [Aes128Gcm.key_length]u8,
};

/// How the payloads of one Nexus endpoint are encoded.
pub const EndpointKey = struct {
    endpoint: []const u8,
    key: Key,
    /// Compress with zlib before encrypting.
    compress: bool,
};

/// Picks the key by `context.nexus.?.endpoint`, and `default_key` for a payload with no Nexus
/// endpoint. A Nexus endpoint it has no key for fails the call.
pub const NexusCodec = struct {
    /// Borrowed.
    endpoints: []const EndpointKey,
    /// For the payloads that belong to no Nexus endpoint, such as workflow inputs. These are
    /// encrypted without compression or endpoint metadata.
    default_key: Key,

    /// The codec, borrowing `c`, which must outlive every client that uses it. Safe to use
    /// from several threads at once: it keeps no state between calls, so concurrent calls
    /// with different contexts each use their own key.
    pub fn codec(c: *const NexusCodec) converter.PayloadCodec {
        return .{ .ptr = @constCast(c), .vtable = &.{
            .encode = encode,
            .decode = decode,
            .encodeWithContext = encodeWithContext,
            .decodeWithContext = decodeWithContext,
        } };
    }

    /// The key, endpoint and compression for payloads coded with `context`.
    const Choice = struct {
        key: *const Key,
        /// Empty for a payload of no Nexus endpoint.
        endpoint: []const u8,
        compress: bool,
    };

    fn choose(c: *const NexusCodec, context: *const converter.Context) error{CodecFailed}!Choice {
        const nexus = context.nexus orelse return .{ .key = &c.default_key, .endpoint = "", .compress = false };
        if (nexus.endpoint.len == 0) return .{ .key = &c.default_key, .endpoint = "", .compress = false };
        for (c.endpoints) |*e| if (std.mem.eql(u8, e.endpoint, nexus.endpoint))
            return .{ .key = &e.key, .endpoint = e.endpoint, .compress = e.compress };
        log.warn("no key is configured for Nexus endpoint \"{s}\" (operation \"{s}\" of service \"{s}\")", .{ nexus.endpoint, nexus.operation, nexus.service });
        return error.CodecFailed;
    }

    fn encode(ptr: *anyopaque, arena: Allocator, io: Io, payloads: []const Payload) converter.CodecError![]Payload {
        return encodeWithContext(ptr, arena, io, &.{}, payloads);
    }

    fn decode(ptr: *anyopaque, arena: Allocator, io: Io, payloads: []const Payload) converter.CodecError![]Payload {
        return decodeWithContext(ptr, arena, io, &.{}, payloads);
    }

    /// Each payload is (compressed, for an endpoint that says so, then) serialized whole and
    /// replaced by a payload of encoding `binary/aes-gcm` holding the nonce, the ciphertext and
    /// the tag.
    fn encodeWithContext(ptr: *anyopaque, arena: Allocator, io: Io, context: *const converter.Context, payloads: []const Payload) converter.CodecError![]Payload {
        const c: *const NexusCodec = @ptrCast(@alignCast(ptr));
        const choice = try c.choose(context);
        const plain = if (choice.compress) try zlib.codec().encode(arena, io, payloads) else payloads;
        const out = try arena.alloc(Payload, plain.len);
        for (plain, out) |p, *o| {
            var serialized: Io.Writer.Allocating = .init(arena);
            p.encode(&serialized.writer, arena) catch return error.OutOfMemory;
            const bytes = serialized.written();
            const sealed = try arena.alloc(u8, Aes128Gcm.nonce_length + bytes.len + Aes128Gcm.tag_length);
            // A nonce must never repeat for a key; a fresh random one is stored before the
            // ciphertext for decoding.
            const nonce = sealed[0..Aes128Gcm.nonce_length];
            io.random(nonce);
            Aes128Gcm.encrypt(sealed[Aes128Gcm.nonce_length..][0..bytes.len], sealed[sealed.len - Aes128Gcm.tag_length ..][0..Aes128Gcm.tag_length], bytes, choice.endpoint, nonce.*, choice.key.bytes);
            o.* = try converter.payloadWithEncoding(arena, encoding, sealed);
            try o.metadata.append(arena, .{ .key = key_id_metadata, .value = choice.key.id });
            if (choice.endpoint.len > 0) try o.metadata.append(arena, .{ .key = endpoint_metadata, .value = choice.endpoint });
        }
        return out;
    }

    /// Decrypts `binary/aes-gcm` payloads, which must name the key and the endpoint `context`
    /// picks, and passes the others through.
    fn decodeWithContext(ptr: *anyopaque, arena: Allocator, io: Io, context: *const converter.Context, payloads: []const Payload) converter.CodecError![]Payload {
        const c: *const NexusCodec = @ptrCast(@alignCast(ptr));
        const choice = try c.choose(context);
        const out = try arena.dupe(Payload, payloads);
        for (out, 0..) |*p, i| {
            const enc = converter.encodingOf(p.*) orelse continue;
            if (!std.mem.eql(u8, enc, encoding)) continue;
            const key_id = metadataValue(p.*, key_id_metadata) orelse "";
            const endpoint = metadataValue(p.*, endpoint_metadata) orelse "";
            if (!std.mem.eql(u8, key_id, choice.key.id) or !std.mem.eql(u8, endpoint, choice.endpoint)) {
                log.warn("decrypting payload {d} failed: it names key \"{s}\" and endpoint \"{s}\", but its context picks key \"{s}\" and endpoint \"{s}\"", .{ i, key_id, endpoint, choice.key.id, choice.endpoint });
                return error.CodecFailed;
            }
            if (p.data.len < Aes128Gcm.nonce_length + Aes128Gcm.tag_length) {
                log.warn("decrypting payload {d} failed: its {d} bytes are too short to hold a nonce and a tag", .{ i, p.data.len });
                return error.CodecFailed;
            }
            const nonce = p.data[0..Aes128Gcm.nonce_length];
            const ciphertext = p.data[Aes128Gcm.nonce_length .. p.data.len - Aes128Gcm.tag_length];
            const tag = p.data[p.data.len - Aes128Gcm.tag_length ..][0..Aes128Gcm.tag_length];
            const plain = try arena.alloc(u8, ciphertext.len);
            Aes128Gcm.decrypt(plain, ciphertext, tag.*, choice.endpoint, nonce.*, choice.key.bytes) catch {
                log.warn("decrypting payload {d} with key \"{s}\" failed: the data doesn't authenticate (wrong key or endpoint, or tampered data)", .{ i, key_id });
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
        return if (choice.compress) zlib.codec().decode(arena, io, out) else out;
    }
};

/// The value of `p`'s metadata entry `key`, or null if it has none.
pub fn metadataValue(p: Payload, key: []const u8) ?[]const u8 {
    for (p.metadata.items) |m| if (std.mem.eql(u8, m.key, key)) return m.value;
    return null;
}

/// Compresses every payload of a compressing endpoint, even when that makes it larger.
const zlib: converter.ZlibCodec = .{ .always_encode = true };

// The demo keys, 16 bytes each.
const compressed_encrypted_key_1: Key = .{ .id = "compressed-encrypted-1", .bytes = "handler-a-key-01".* };
const compressed_encrypted_key_2: Key = .{ .id = "compressed-encrypted-2", .bytes = "handler-a-key-02".* };
const encrypted_key_1: Key = .{ .id = "encrypted-1", .bytes = "handler-b-key-01".* };
const encrypted_key_2: Key = .{ .id = "encrypted-2", .bytes = "handler-b-key-02".* };
const caller_workflow_key: Key = .{ .id = "caller-workflow", .bytes = "sample-key-C-123".* };

const compressed_encrypted_endpoints = [_]EndpointKey{
    .{ .endpoint = service.compressed_encrypted_1, .key = compressed_encrypted_key_1, .compress = true },
    .{ .endpoint = service.compressed_encrypted_2, .key = compressed_encrypted_key_2, .compress = true },
};
const encrypted_endpoints = [_]EndpointKey{
    .{ .endpoint = service.encrypted_1, .key = encrypted_key_1, .compress = false },
    .{ .endpoint = service.encrypted_2, .key = encrypted_key_2, .compress = false },
};

/// The caller's: every endpoint's key, and a key of its own for its workflow's input and
/// result, so those don't depend on the keys it shares with the handlers.
pub const caller: NexusCodec = .{
    .endpoints = &(compressed_encrypted_endpoints ++ encrypted_endpoints),
    .default_key = caller_workflow_key,
};

/// The compressed-encrypted handler's: its two endpoints' keys, and its first endpoint's key
/// for the rest, such as the inputs of the workflows it starts.
pub const compressed_encrypted_handler: NexusCodec = .{
    .endpoints = &compressed_encrypted_endpoints,
    .default_key = compressed_encrypted_key_1,
};

/// The encrypted handler's, laid out like `compressed_encrypted_handler`.
pub const encrypted_handler: NexusCodec = .{
    .endpoints = &encrypted_endpoints,
    .default_key = encrypted_key_1,
};
