//! Tests of the codec configuration. They pass the context to the codecs themselves, to check
//! the keys the sample picks; caller_test.zig checks that the SDK passes it.

const std = @import("std");
const temporal = @import("temporal");
const service = @import("service.zig");
const codec = @import("codec.zig");
const converter = temporal.converter;
const Aes128Gcm = codec.Aes128Gcm;

const testing = std.testing;

fn nexusContext(endpoint: []const u8, operation: []const u8) converter.Context {
    return .{ .namespace = "nexus-serialization-caller", .nexus = .{ .endpoint = endpoint, .service = service.EchoService.service_name, .operation = operation } };
}

/// Encodes `value` with `c` and `context`, as a client with `c` as its only codec does.
fn encode(a: std.mem.Allocator, c: *const codec.NexusCodec, context: *const converter.Context, value: anytype) !converter.Payload {
    const codecs = [_]converter.PayloadCodec{c.codec()};
    const encoded = try converter.encodePayloadsWithContext(&codecs, a, testing.io, context, &.{try converter.toPayloadWithContext(a, context, value)});
    try testing.expectEqual(1, encoded.len);
    return encoded[0];
}

fn decode(comptime T: type, a: std.mem.Allocator, c: *const codec.NexusCodec, context: *const converter.Context, p: converter.Payload) !T {
    const codecs = [_]converter.PayloadCodec{c.codec()};
    const decoded = try converter.decodePayloadsWithContext(&codecs, a, testing.io, context, &.{p});
    return converter.fromPayloadWithContext(T, a, context, decoded[0]);
}

/// Checks that `p` authenticates with the key `key` and the associated data `endpoint`, given
/// here independently of the codec and of the key ID the payload names, so that the same
/// mistake in encoding and decoding can't hide a wrong key.
fn expectEncryptedWith(p: converter.Payload, key: *const [Aes128Gcm.key_length]u8, endpoint: []const u8) !void {
    try testing.expect(p.data.len >= Aes128Gcm.nonce_length + Aes128Gcm.tag_length);
    const nonce = p.data[0..Aes128Gcm.nonce_length];
    const ciphertext = p.data[Aes128Gcm.nonce_length .. p.data.len - Aes128Gcm.tag_length];
    const tag = p.data[p.data.len - Aes128Gcm.tag_length ..][0..Aes128Gcm.tag_length];
    const plain = try testing.allocator.alloc(u8, ciphertext.len);
    defer testing.allocator.free(plain);
    try Aes128Gcm.decrypt(plain, ciphertext, tag.*, endpoint, nonce.*, key.*);
}

const EndpointCase = struct {
    endpoint: []const u8,
    key_id: []const u8,
    key: *const [16]u8,
    handler: *const codec.NexusCodec,
};

const endpoint_cases = [_]EndpointCase{
    .{ .endpoint = service.compressed_encrypted_1, .key_id = "compressed-encrypted-1", .key = "handler-a-key-01", .handler = &codec.compressed_encrypted_handler },
    .{ .endpoint = service.compressed_encrypted_2, .key_id = "compressed-encrypted-2", .key = "handler-a-key-02", .handler = &codec.compressed_encrypted_handler },
    .{ .endpoint = service.encrypted_1, .key_id = "encrypted-1", .key = "handler-b-key-01", .handler = &codec.encrypted_handler },
    .{ .endpoint = service.encrypted_2, .key_id = "encrypted-2", .key = "handler-b-key-02", .handler = &codec.encrypted_handler },
};

test "each endpoint's requests and responses round-trip between the caller and its handler with the endpoint's key" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    for (endpoint_cases) |case| {
        const context = nexusContext(case.endpoint, service.Echo.operation_name);
        // The request goes from the caller to the handler, the response back.
        const directions = [_][2]*const codec.NexusCodec{ .{ &codec.caller, case.handler }, .{ case.handler, &codec.caller } };
        for (directions) |d| {
            const p = try encode(a, d[0], &context, @as([]const u8, "Hello from Nexus"));
            try testing.expectEqualStrings(codec.encoding, converter.encodingOf(p).?);
            try testing.expectEqualStrings(case.key_id, codec.metadataValue(p, codec.key_id_metadata).?);
            try testing.expectEqualStrings(case.endpoint, codec.metadataValue(p, codec.endpoint_metadata).?);
            try expectEncryptedWith(p, case.key, case.endpoint);
            try testing.expectEqualStrings("Hello from Nexus", try decode([]const u8, a, d[1], &context, p));
        }
    }

    // A payload of one endpoint doesn't decode as another's, even with the same handler.
    const first = nexusContext(service.encrypted_1, service.Echo.operation_name);
    const second = nexusContext(service.encrypted_2, service.Echo.operation_name);
    const p = try encode(a, &codec.caller, &first, @as([]const u8, "Hello from Nexus"));
    try testing.expectError(error.CodecFailed, decode([]const u8, a, &codec.encrypted_handler, &second, p));
}

test "the compressing endpoints compress before they encrypt" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var repeated: std.ArrayList(u8) = .empty;
    for (0..100) |_| try repeated.appendSlice(a, "compress this repeated message ");
    const message = repeated.items;
    const pairs = [_][2][]const u8{ .{ service.compressed_encrypted_1, service.encrypted_1 }, .{ service.compressed_encrypted_2, service.encrypted_2 } };
    for (pairs) |pair| {
        const compressed_context = nexusContext(pair[0], service.Echo.operation_name);
        const encrypted_context = nexusContext(pair[1], service.Echo.operation_name);
        const small = try encode(a, &codec.caller, &compressed_context, @as([]const u8, message));
        const large = try encode(a, &codec.caller, &encrypted_context, @as([]const u8, message));
        try testing.expect(small.data.len < large.data.len);
        try testing.expectEqualStrings(message, try decode([]const u8, a, &codec.caller, &compressed_context, small));
    }
}

test "payloads of no Nexus endpoint get each program's default key, without compression or endpoint metadata" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const cases = [_]struct { c: *const codec.NexusCodec, key_id: []const u8, key: *const [16]u8 }{
        .{ .c = &codec.caller, .key_id = "caller-workflow", .key = "sample-key-C-123" },
        .{ .c = &codec.compressed_encrypted_handler, .key_id = "compressed-encrypted-1", .key = "handler-a-key-01" },
        .{ .c = &codec.encrypted_handler, .key_id = "encrypted-1", .key = "handler-b-key-01" },
    };
    for (cases) |case| {
        const context: converter.Context = .{ .namespace = "handler", .workflow_id = "echo", .workflow_type = "EchoWorkflow" };
        const p = try encode(a, case.c, &context, @as([]const u8, "workflow input"));
        try testing.expectEqualStrings(codec.encoding, converter.encodingOf(p).?);
        try testing.expectEqualStrings(case.key_id, codec.metadataValue(p, codec.key_id_metadata).?);
        try testing.expectEqual(null, codec.metadataValue(p, codec.endpoint_metadata));
        try expectEncryptedWith(p, case.key, "");
        try testing.expectEqualStrings("workflow input", try decode([]const u8, a, case.c, &context, p));
    }

    // The caller's result is a workflow payload too, though it holds the Nexus replies.
    const replies = [_]service.EndpointReplies{
        .{ .endpoint = service.compressed_encrypted_1, .sync_message = "c1 sync", .async_message = "c1 async" },
        .{ .endpoint = service.compressed_encrypted_2, .sync_message = "c2 sync", .async_message = "c2 async" },
        .{ .endpoint = service.encrypted_1, .sync_message = "e1 sync", .async_message = "e1 async" },
        .{ .endpoint = service.encrypted_2, .sync_message = "e2 sync", .async_message = "e2 async" },
    };
    const context: converter.Context = .{ .namespace = service.caller_namespace, .workflow_id = "caller", .workflow_type = "NexusSerializationCallerWorkflow" };
    const p = try encode(a, &codec.caller, &context, @as([]const service.EndpointReplies, &replies));
    try testing.expectEqualStrings("caller-workflow", codec.metadataValue(p, codec.key_id_metadata).?);
    try expectEncryptedWith(p, "sample-key-C-123", "");
    const decoded = try decode([]const service.EndpointReplies, a, &codec.caller, &context, p);
    try testing.expectEqual(replies.len, decoded.len);
    for (replies, decoded) |want, got| {
        try testing.expectEqualStrings(want.endpoint, got.endpoint);
        try testing.expectEqualStrings(want.sync_message, got.sync_message);
        try testing.expectEqualStrings(want.async_message, got.async_message);
    }
}

test "calls with different contexts on one codec each keep their own key" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const c = &codec.encrypted_handler;
    const first = nexusContext(service.encrypted_1, service.Echo.operation_name);
    const second = nexusContext(service.encrypted_2, service.EchoAsync.operation_name);
    const plain: converter.Context = .{};
    const workflow: converter.Context = .{ .namespace = "handler", .workflow_id = "echo" };
    // Encode with every context before decoding any: the codec keeps nothing between calls.
    const cases = [_]struct { context: *const converter.Context, endpoint: []const u8, key: *const [16]u8 }{
        .{ .context = &first, .endpoint = service.encrypted_1, .key = "handler-b-key-01" },
        .{ .context = &second, .endpoint = service.encrypted_2, .key = "handler-b-key-02" },
        .{ .context = &plain, .endpoint = "", .key = "handler-b-key-01" },
        .{ .context = &workflow, .endpoint = "", .key = "handler-b-key-01" },
    };
    var payloads: [cases.len]converter.Payload = undefined;
    for (cases, &payloads) |case, *p| p.* = try encode(a, c, case.context, @as([]const u8, "Hello from Nexus"));
    for (cases, payloads) |case, p| {
        try testing.expectEqualStrings(case.endpoint, codec.metadataValue(p, codec.endpoint_metadata) orelse "");
        try expectEncryptedWith(p, case.key, case.endpoint);
        try testing.expectEqualStrings("Hello from Nexus", try decode([]const u8, a, c, case.context, p));
    }
    // Without a context, as `encode` and `decode` are called, the default key applies.
    const no_context = try c.codec().encode(a, testing.io, &.{try converter.toPayload(a, @as([]const u8, "plain"))});
    try expectEncryptedWith(no_context[0], "handler-b-key-01", "");
    try testing.expectEqualStrings("plain", try converter.fromPayload([]const u8, a, (try c.codec().decode(a, testing.io, no_context))[0]));
}

test "encrypting the same input twice uses a fresh nonce" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const first = try encode(a, &codec.caller, &.{}, @as([]const u8, "same input"));
    const second = try encode(a, &codec.caller, &.{}, @as([]const u8, "same input"));
    try testing.expect(!std.mem.eql(u8, first.data[0..Aes128Gcm.nonce_length], second.data[0..Aes128Gcm.nonce_length]));
    try testing.expect(!std.mem.eql(u8, first.data, second.data));
}

test "damaged ciphertext is refused" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const p = try encode(a, &codec.caller, &.{}, @as([]const u8, "Hello from Nexus"));
    const altered = try a.dupe(u8, p.data);
    altered[altered.len / 2] ^= 1;
    const damaged = [_][]const u8{
        altered,
        // An incomplete nonce.
        p.data[0..1],
        // A truncated tag.
        p.data[0 .. p.data.len - 1],
    };
    for (damaged) |data| {
        var bad = p;
        bad.data = data;
        try testing.expectError(error.CodecFailed, decode([]const u8, a, &codec.caller, &.{}, bad));
    }
}

test "an endpoint with no key fails encoding and decoding" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const unknown = nexusContext("unknown", service.Echo.operation_name);
    for ([_]*const codec.NexusCodec{ &codec.caller, &codec.compressed_encrypted_handler, &codec.encrypted_handler }) |c| {
        const p = try encode(a, c, &.{}, @as([]const u8, "ordinary workflow input"));
        try testing.expectError(error.CodecFailed, encode(a, c, &unknown, @as([]const u8, "request")));
        try testing.expectError(error.CodecFailed, decode([]const u8, a, c, &unknown, p));
    }
    // A handler has no key for the other handler's endpoints.
    const other = nexusContext(service.encrypted_1, service.Echo.operation_name);
    try testing.expectError(error.CodecFailed, encode(a, &codec.compressed_encrypted_handler, &other, @as([]const u8, "request")));
}
