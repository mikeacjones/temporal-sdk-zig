//! Tests for the JWT sample: the key file, the tokens, the JWKS, and the headers sent.

const std = @import("std");
const temporal = @import("temporal");
const serverjwtauth = @import("serverjwtauth.zig");

const testing = std.testing;
const Ecdsa = std.crypto.sign.ecdsa.EcdsaP256Sha256;
const base64 = std.base64.url_safe_no_pad;
const HeaderEntry = std.meta.Elem(@FieldType(temporal.client.Options, "headers"));

/// Collects what `readKey` would log, and restores logging when done.
const ErrorLog = struct {
    log: std.Io.Writer.Allocating,

    fn init(e: *ErrorLog) void {
        e.log = .init(testing.allocator);
        serverjwtauth.test_error_log = &e.log.writer;
    }

    fn deinit(e: *ErrorLog) void {
        serverjwtauth.test_error_log = null;
        e.log.deinit();
    }
};

fn decode(arena: std.mem.Allocator, text: []const u8) ![]u8 {
    const out = try arena.alloc(u8, try base64.Decoder.calcSizeForSlice(text));
    try base64.Decoder.decode(out, text);
    return out;
}

fn tmpPath(arena: std.mem.Allocator, tmp: *testing.TmpDir, name: []const u8) ![]const u8 {
    const dir = try tmp.dir.realPathFileAlloc(testing.io, ".", arena);
    return std.fs.path.join(arena, &.{ dir, name });
}

test "a generated key is written and read back" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try tmpPath(a, &tmp, "key.priv.json");

    const key = try serverjwtauth.generateKey(a, testing.io);
    // A SHA-256 thumbprint, base64url without padding.
    try testing.expectEqual(43, key.id.len);
    try serverjwtauth.writeKey(a, testing.io, path, key);

    const read = try serverjwtauth.readKey(a, testing.io, path);
    try testing.expectEqualStrings(key.id, read.id);
    try testing.expectEqualSlices(u8, &key.pair.secret_key.toBytes(), &read.pair.secret_key.toBytes());
    try testing.expectEqualSlices(u8, &key.pair.public_key.toUncompressedSec1(), &read.pair.public_key.toUncompressedSec1());

    if (std.Io.File.Permissions.has_executable_bit) {
        const stat = try tmp.dir.statFile(testing.io, "key.priv.json", .{});
        try testing.expectEqual(0o600, stat.permissions.toMode() & 0o777);
    }
}

test "writing a key over an existing file replaces it and makes it private" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(testing.io, .{ .sub_path = "key.priv.json", .data = &@as([2000]u8, @splat('x')) });
    const path = try tmpPath(a, &tmp, "key.priv.json");

    const key = try serverjwtauth.generateKey(a, testing.io);
    try serverjwtauth.writeKey(a, testing.io, path, key);
    const read = try serverjwtauth.readKey(a, testing.io, path);
    try testing.expectEqualStrings(key.id, read.id);
    if (std.Io.File.Permissions.has_executable_bit) {
        const stat = try tmp.dir.statFile(testing.io, "key.priv.json", .{});
        try testing.expectEqual(0o600, stat.permissions.toMode() & 0o777);
    }
}

test "two generated keys differ" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = try serverjwtauth.generateKey(arena.allocator(), testing.io);
    const b = try serverjwtauth.generateKey(arena.allocator(), testing.io);
    try testing.expect(!std.mem.eql(u8, a.id, b.id));
}

/// A private JWK as the samples-test runner writes it to SAMPLE_JWT_KEY.
fn runnerJwk(arena: std.mem.Allocator, pair: Ecdsa.KeyPair) ![]const u8 {
    const point = pair.public_key.toUncompressedSec1();
    var x: [43]u8 = undefined;
    var y: [43]u8 = undefined;
    var d: [43]u8 = undefined;
    return std.fmt.allocPrint(arena, "{{\"kty\":\"EC\",\"crv\":\"P-256\",\"alg\":\"ES256\",\"use\":\"sig\",\"kid\":\"samples-runner\",\"x\":\"{s}\",\"y\":\"{s}\",\"d\":\"{s}\"}}", .{
        base64.Encoder.encode(&x, point[1..33]),
        base64.Encoder.encode(&y, point[33..65]),
        base64.Encoder.encode(&d, &pair.secret_key.toBytes()),
    });
}

test "readKey reads the key the samples-test runner writes" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const pair = Ecdsa.KeyPair.generate(testing.io);
    try tmp.dir.writeFile(testing.io, .{ .sub_path = "runner.json", .data = try runnerJwk(a, pair) });

    const key = try serverjwtauth.readKey(a, testing.io, try tmpPath(a, &tmp, "runner.json"));
    try testing.expectEqualStrings("samples-runner", key.id);
    try testing.expectEqualSlices(u8, &pair.public_key.toUncompressedSec1(), &key.pair.public_key.toUncompressedSec1());
}

test "keyPath uses SAMPLE_JWT_KEY, else key.priv.json" {
    var env: std.process.Environ.Map = .init(testing.allocator);
    defer env.deinit();
    try testing.expectEqualStrings("key.priv.json", serverjwtauth.keyPath(&env));
    try env.put("SAMPLE_JWT_KEY", "");
    try testing.expectEqualStrings("key.priv.json", serverjwtauth.keyPath(&env));
    try env.put("SAMPLE_JWT_KEY", "/run/key.json");
    try testing.expectEqualStrings("/run/key.json", serverjwtauth.keyPath(&env));
}

test "readKey rejects what is not a P-256 private JWK" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try tmpPath(a, &tmp, "key.json");
    var d: [43]u8 = undefined;
    const good_d = base64.Encoder.encode(&d, &Ecdsa.KeyPair.generate(testing.io).secret_key.toBytes());
    // The P-256 group order, which is not a valid scalar.
    const order = [_]u8{ 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xbc, 0xe6, 0xfa, 0xad, 0xa7, 0x17, 0x9e, 0x84, 0xf3, 0xb9, 0xca, 0xc2, 0xfc, 0x63, 0x25, 0x51 };
    var order_text: [43]u8 = undefined;
    var zero_text: [43]u8 = undefined;

    const Case = struct { json: []const u8, reason: []const u8 };
    const cases = [_]Case{
        .{ .json = "not json", .reason = "it is not JSON" },
        .{ .json = "[1,2]", .reason = "it is not a JSON object" },
        .{ .json = try std.fmt.allocPrint(a, "{{\"kty\":\"RSA\",\"crv\":\"P-256\",\"kid\":\"k\",\"d\":\"{s}\"}}", .{good_d}), .reason = "its kty is not \"EC\"" },
        .{ .json = try std.fmt.allocPrint(a, "{{\"crv\":\"P-256\",\"kid\":\"k\",\"d\":\"{s}\"}}", .{good_d}), .reason = "its kty is not \"EC\"" },
        .{ .json = try std.fmt.allocPrint(a, "{{\"kty\":\"EC\",\"crv\":\"P-384\",\"kid\":\"k\",\"d\":\"{s}\"}}", .{good_d}), .reason = "its crv is not \"P-256\"" },
        .{ .json = try std.fmt.allocPrint(a, "{{\"kty\":\"EC\",\"crv\":\"P-256\",\"d\":\"{s}\"}}", .{good_d}), .reason = "it has no kid" },
        .{ .json = try std.fmt.allocPrint(a, "{{\"kty\":\"EC\",\"crv\":\"P-256\",\"kid\":\"\",\"d\":\"{s}\"}}", .{good_d}), .reason = "it has no kid" },
        .{ .json = try std.fmt.allocPrint(a, "{{\"kty\":\"EC\",\"crv\":\"P-256\",\"kid\":7,\"d\":\"{s}\"}}", .{good_d}), .reason = "it has no kid" },
        .{ .json = "{\"kty\":\"EC\",\"crv\":\"P-256\",\"kid\":\"k\"}", .reason = "it has no d" },
        .{ .json = "{\"kty\":\"EC\",\"crv\":\"P-256\",\"kid\":\"k\",\"d\":\"AAAA\"}", .reason = "its d is not 32 bytes" },
        .{ .json = "{\"kty\":\"EC\",\"crv\":\"P-256\",\"kid\":\"k\",\"d\":\"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\"}", .reason = "its d is not base64url" },
        .{ .json = try std.fmt.allocPrint(a, "{{\"kty\":\"EC\",\"crv\":\"P-256\",\"kid\":\"k\",\"d\":\"{s}\"}}", .{base64.Encoder.encode(&order_text, &order)}), .reason = "its d is not less than the P-256 group order" },
        .{ .json = try std.fmt.allocPrint(a, "{{\"kty\":\"EC\",\"crv\":\"P-256\",\"kid\":\"k\",\"d\":\"{s}\"}}", .{base64.Encoder.encode(&zero_text, &@as([32]u8, @splat(0)))}), .reason = "its d is zero" },
    };
    for (cases) |case| {
        try tmp.dir.writeFile(testing.io, .{ .sub_path = "key.json", .data = case.json });
        var e: ErrorLog = undefined;
        e.init();
        defer e.deinit();
        testing.expectError(error.InvalidKey, serverjwtauth.readKey(a, testing.io, path)) catch |err| {
            std.debug.print("key file: {s}\n", .{case.json});
            return err;
        };
        try testing.expectStringStartsWith(e.log.written(), try std.fmt.allocPrint(a, "the JWT key file {s} is not a P-256 private key: {s}", .{ path, case.reason }));
    }
}

test "readKey reports a key file it cannot read" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try tmpPath(a, &tmp, "missing.json");
    var e: ErrorLog = undefined;
    e.init();
    defer e.deinit();
    try testing.expectError(error.KeyFileUnreadable, serverjwtauth.readKey(a, testing.io, path));
    try testing.expectStringStartsWith(e.log.written(), try std.fmt.allocPrint(a, "cannot read the JWT key file {s}: FileNotFound", .{path}));
}

test "a minted token decodes, holds its claims and verifies with the public key" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const key = try serverjwtauth.generateKey(a, testing.io);
    const config: serverjwtauth.JwtConfig = .{
        .key = key,
        .permissions = try serverjwtauth.namespacePermissions(a, "my-namespace"),
        .expiration = .fromSeconds(120),
    };
    const token = try config.mint(a, testing.io);

    var parts = std.mem.splitScalar(u8, token, '.');
    const header_text = parts.next().?;
    const claims_text = parts.next().?;
    const signature_text = parts.next().?;
    try testing.expectEqual(null, parts.next());

    const Header = struct { alg: []const u8, typ: []const u8, kid: []const u8 };
    const header = try std.json.parseFromSliceLeaky(Header, a, try decode(a, header_text), .{});
    try testing.expectEqualStrings("ES256", header.alg);
    try testing.expectEqualStrings("JWT", header.typ);
    try testing.expectEqualStrings(key.id, header.kid);

    const Claims = struct { sub: []const u8, permissions: []const []const u8, iat: i64, exp: i64 };
    const claims = try std.json.parseFromSliceLeaky(Claims, a, try decode(a, claims_text), .{});
    try testing.expectEqualStrings("temporal-samples-zig", claims.sub);
    try testing.expectEqual(2, claims.permissions.len);
    try testing.expectEqualStrings("my-namespace:read", claims.permissions[0]);
    try testing.expectEqualStrings("my-namespace:write", claims.permissions[1]);
    try testing.expectEqual(120, claims.exp - claims.iat);

    const signature_bytes = try decode(a, signature_text);
    try testing.expectEqual(Ecdsa.Signature.encoded_length, signature_bytes.len);
    const signature: Ecdsa.Signature = .fromBytes(signature_bytes[0..Ecdsa.Signature.encoded_length].*);
    try signature.verify(token[0 .. header_text.len + 1 + claims_text.len], key.pair.public_key);
    try testing.expectError(error.SignatureVerificationFailed, signature.verify(token[0..header_text.len], key.pair.public_key));
}

test "a namespace with quotes stays valid JSON in the token" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const config: serverjwtauth.JwtConfig = .{
        .key = try serverjwtauth.generateKey(a, testing.io),
        .permissions = try serverjwtauth.namespacePermissions(a, "odd\"ns"),
        .subject = "a \"subject\"",
    };
    const token = try config.mint(a, testing.io);
    var parts = std.mem.splitScalar(u8, token, '.');
    _ = parts.next();
    const Claims = struct { sub: []const u8, permissions: []const []const u8 };
    const claims = try std.json.parseFromSliceLeaky(Claims, a, try decode(a, parts.next().?), .{ .ignore_unknown_fields = true });
    try testing.expectEqualStrings("a \"subject\"", claims.sub);
    try testing.expectEqualStrings("odd\"ns:read", claims.permissions[0]);
}

test "publicJwks holds the key's ID and public point" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const key = try serverjwtauth.generateKey(a, testing.io);
    const Jwk = struct { kty: []const u8, crv: []const u8, alg: []const u8, use: []const u8, kid: []const u8, x: []const u8, y: []const u8 };
    const jwks = try std.json.parseFromSliceLeaky(struct { keys: []const Jwk }, a, try serverjwtauth.publicJwks(a, key), .{});
    try testing.expectEqual(1, jwks.keys.len);
    const jwk = jwks.keys[0];
    try testing.expectEqualStrings("EC", jwk.kty);
    try testing.expectEqualStrings("P-256", jwk.crv);
    try testing.expectEqualStrings("ES256", jwk.alg);
    try testing.expectEqualStrings("sig", jwk.use);
    try testing.expectEqualStrings(key.id, jwk.kid);
    const point = key.pair.public_key.toUncompressedSec1();
    try testing.expectEqualSlices(u8, point[1..33], try decode(a, jwk.x));
    try testing.expectEqualSlices(u8, point[33..65], try decode(a, jwk.y));
}

test "the refresher's headers keep the base headers and add one authorization header" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const base = [_]HeaderEntry{
        .{ .key = "x-team", .value = "samples" },
        .{ .key = "Authorization", .value = "Bearer from-the-environment" },
        .{ .key = "trace-bin", .value = "\x00\x01" },
    };
    const refresher: serverjwtauth.HeaderRefresher = .{
        .config = .{ .key = try serverjwtauth.generateKey(a, testing.io), .permissions = try serverjwtauth.namespacePermissions(a, "default") },
        .base_headers = &base,
    };
    const headers = try refresher.headers(a, testing.io);
    try testing.expectEqual(3, headers.len);
    try testing.expectEqualStrings("x-team", headers[0].key);
    try testing.expectEqualStrings("samples", headers[0].value);
    try testing.expectEqualStrings("trace-bin", headers[1].key);
    try testing.expectEqualStrings("authorization", headers[2].key);
    try testing.expectStringStartsWith(headers[2].value, "Bearer ");
    try testing.expectEqual(3, std.mem.count(u8, headers[2].value["Bearer ".len..], ".") + 1);

    const plain: serverjwtauth.HeaderRefresher = .{ .config = refresher.config };
    const only = try plain.headers(a, testing.io);
    try testing.expectEqual(1, only.len);
    try testing.expectEqualStrings("authorization", only[0].key);
}

fn realAt(nanoseconds: i96) std.Io.Clock.Timestamp {
    return .{ .raw = .{ .nanoseconds = nanoseconds }, .clock = serverjwtauth.HeaderRefresher.clock };
}

test "the refresher is due by the real-time clock, which counts system sleep" {
    try testing.expectEqual(std.Io.Clock.real, serverjwtauth.HeaderRefresher.clock);
}

test "a refresh is due expiration minus leeway after the token was minted" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const key = try serverjwtauth.generateKey(a, testing.io);
    const permissions = try serverjwtauth.namespacePermissions(a, "default");
    const minted = realAt(1_700_000_000 * std.time.ns_per_s + 123);

    const defaults: serverjwtauth.HeaderRefresher = .{ .config = .{ .key = key, .permissions = permissions } };
    const due = defaults.refreshAt(minted);
    try testing.expectEqual(std.Io.Clock.real, due.clock);
    try testing.expectEqual(minted.raw.nanoseconds + 540 * std.time.ns_per_s, due.raw.nanoseconds);

    const custom: serverjwtauth.HeaderRefresher = .{
        .config = .{ .key = key, .permissions = permissions, .expiration = .fromSeconds(30) },
        .leeway = .fromMilliseconds(2500),
    };
    const custom_due = custom.refreshAt(minted);
    try testing.expectEqual(std.Io.Clock.real, custom_due.clock);
    try testing.expectEqual(minted.raw.nanoseconds + 27_500 * std.time.ns_per_ms, custom_due.raw.nanoseconds);
}

test "the refresher sleeps at most check_interval, and only until the refresh is due" {
    const R = serverjwtauth.HeaderRefresher;
    const due = realAt(1_000 * std.time.ns_per_s);
    try testing.expectEqual(R.check_interval, R.sleepBefore(due, realAt(due.raw.nanoseconds - 500 * std.time.ns_per_s)));
    try testing.expectEqual(R.check_interval, R.sleepBefore(due, realAt(due.raw.nanoseconds - std.time.ns_per_s)));
    try testing.expectEqual(std.Io.Duration.fromMilliseconds(250), R.sleepBefore(due, realAt(due.raw.nanoseconds - 250 * std.time.ns_per_ms)));
    try testing.expectEqual(std.Io.Duration.fromNanoseconds(1), R.sleepBefore(due, realAt(due.raw.nanoseconds - 1)));
    try testing.expectEqual(std.Io.Duration.zero, R.sleepBefore(due, due));
}

test "a refresh is due at once when the machine wakes hours past the deadline" {
    const R = serverjwtauth.HeaderRefresher;
    const due = realAt(1_000 * std.time.ns_per_s);
    try testing.expectEqual(std.Io.Duration.zero, R.sleepBefore(due, realAt(due.raw.nanoseconds + 1)));
    try testing.expectEqual(std.Io.Duration.zero, R.sleepBefore(due, realAt(due.raw.nanoseconds + 3 * std.time.ns_per_hour)));
}
