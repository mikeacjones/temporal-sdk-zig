//! Server-side authorization with JWTs: the programs sign their own short-lived token with an
//! ES256 key whose public half the server trusts (it reads a JWKS), and send it as the
//! `authorization: Bearer <token>` header of every call. The token grants
//! "<namespace>:read" and "<namespace>:write", the permissions the server's default
//! authorizer checks. The worker replaces it with a new one before it expires; the workflow
//! and activity are the helloworld sample's.
//!
//! The key is a private JWK (JSON) at SAMPLE_JWT_KEY, else `key.priv.json`; the
//! `serverjwtauth-key` program writes one and serves its public half (see key.zig).

const std = @import("std");
const builtin = @import("builtin");
const temporal = @import("temporal");

const Allocator = std.mem.Allocator;
const Io = std.Io;
const Ecdsa = std.crypto.sign.ecdsa.EcdsaP256Sha256;
const Sha256 = std.crypto.hash.sha2.Sha256;
const base64 = std.base64.url_safe_no_pad;
/// The element type of `client.Options.headers`; the SDK exports no name for it.
const HeaderEntry = std.meta.Elem(@FieldType(temporal.client.Options, "headers"));

pub const task_queue = "server-jwt-auth";
pub const workflow_id = "server-jwt-auth";
pub const default_key_file = "key.priv.json";
pub const key_file_variable = "SAMPLE_JWT_KEY";

/// An ES256 signing key and the key ID the server finds its public half by.
pub const Key = struct { pair: Ecdsa.KeyPair, id: []const u8 };

// Test hook, read only when builtin.is_test: when set, `readKey` writes its error line here,
// with a newline, instead of logging it (the test runner fails a test that logs an error).
pub var test_error_log: ?*std.Io.Writer = null;

fn logError(comptime format: []const u8, args: anytype) void {
    if (builtin.is_test) if (test_error_log) |w| {
        w.print(format ++ "\n", args) catch {};
        return;
    };
    std.log.err(format, args);
}

/// The key file's path: SAMPLE_JWT_KEY from `env`, else `default_key_file`.
pub fn keyPath(env: *const std.process.Environ.Map) []const u8 {
    if (env.get(key_file_variable)) |path| if (path.len > 0) return path;
    return default_key_file;
}

/// A new P-256 key whose ID is its RFC 7638 SHA-256 thumbprint, base64url without padding.
pub fn generateKey(arena: Allocator, io: Io) error{OutOfMemory}!Key {
    const pair = Ecdsa.KeyPair.generate(io);
    const point = pair.public_key.toUncompressedSec1();
    // RFC 7638: the required members only, in lexicographic order, with no whitespace.
    const canonical = try std.fmt.allocPrint(arena, "{{\"crv\":\"P-256\",\"kty\":\"EC\",\"x\":\"{s}\",\"y\":\"{s}\"}}", .{
        try base64Url(arena, point[1..33]),
        try base64Url(arena, point[33..65]),
    });
    var thumbprint: [Sha256.digest_length]u8 = undefined;
    Sha256.hash(canonical, &thumbprint, .{});
    return .{ .pair = pair, .id = try base64Url(arena, &thumbprint) };
}

pub const WriteKeyError = Io.File.OpenError || Io.File.SetPermissionsError || Io.File.Writer.Error || error{OutOfMemory};

/// Writes `key` as a private JWK (JSON object with kty, crv, alg, use, kid, x, y, d), mode 0600.
/// An existing file is replaced, and its mode set to 0600 before the key is written.
pub fn writeKey(arena: Allocator, io: Io, path: []const u8, key: Key) WriteKeyError!void {
    const public = try publicJwk(arena, key);
    const d = try base64Url(arena, &key.pair.secret_key.toBytes());
    const private = try std.fmt.allocPrint(arena, "{s},\"d\":\"{s}\"}}\n", .{ public[0 .. public.len - 1], d });
    const owner_only: Io.File.Permissions = if (Io.File.Permissions.has_executable_bit) .fromMode(0o600) else .default_file;
    var file = try Io.Dir.cwd().createFile(io, path, .{ .permissions = owner_only });
    defer file.close(io);
    if (Io.File.Permissions.has_executable_bit) try file.setPermissions(io, owner_only);
    try file.writeStreamingAll(io, private);
}

pub const ReadKeyError = error{ KeyFileUnreadable, InvalidKey, OutOfMemory };

/// A JWK file is a few hundred bytes; a larger file is not a key.
const max_key_file_size = 64 * 1024;

/// Reads a private JWK file; the ID is the file's `kid`. Logs what is wrong with the file.
pub fn readKey(arena: Allocator, io: Io, path: []const u8) ReadKeyError!Key {
    const bytes = Io.Dir.cwd().readFileAlloc(io, path, arena, .limited(max_key_file_size)) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => {
            logError("cannot read the JWT key file {s}: {t}", .{ path, err });
            return error.KeyFileUnreadable;
        },
    };
    return parseKey(arena, bytes, path);
}

fn parseKey(arena: Allocator, bytes: []const u8, path: []const u8) error{ InvalidKey, OutOfMemory }!Key {
    const invalid = struct {
        fn f(p: []const u8, reason: []const u8) error{InvalidKey} {
            logError("the JWT key file {s} is not a P-256 private key: {s}", .{ p, reason });
            return error.InvalidKey;
        }
    }.f;
    const value = std.json.parseFromSliceLeaky(std.json.Value, arena, bytes, .{}) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return invalid(path, "it is not JSON"),
    };
    if (value != .object) return invalid(path, "it is not a JSON object");
    const object = value.object;
    if (!memberIs(object, "kty", "EC")) return invalid(path, "its kty is not \"EC\"");
    if (!memberIs(object, "crv", "P-256")) return invalid(path, "its crv is not \"P-256\"");
    const kid = stringMember(object, "kid") orelse "";
    if (kid.len == 0) return invalid(path, "it has no kid");
    const d = stringMember(object, "d") orelse return invalid(path, "it has no d (the private key)");
    var scalar: [Ecdsa.SecretKey.encoded_length]u8 = undefined;
    const size = base64.Decoder.calcSizeForSlice(d) catch return invalid(path, "its d is not base64url");
    if (size != scalar.len) return invalid(path, "its d is not 32 bytes");
    base64.Decoder.decode(&scalar, d) catch return invalid(path, "its d is not base64url");
    std.crypto.ecc.P256.scalar.rejectNonCanonical(scalar, .big) catch return invalid(path, "its d is not less than the P-256 group order");
    const secret = Ecdsa.SecretKey.fromBytes(scalar) catch return invalid(path, "its d is not a P-256 scalar");
    const pair = Ecdsa.KeyPair.fromSecretKey(secret) catch return invalid(path, "its d is zero");
    return .{ .pair = pair, .id = kid };
}

fn stringMember(object: std.json.ObjectMap, name: []const u8) ?[]const u8 {
    const v = object.get(name) orelse return null;
    return if (v == .string) v.string else null;
}

fn memberIs(object: std.json.ObjectMap, name: []const u8, expected: []const u8) bool {
    const v = stringMember(object, name) orelse return false;
    return std.mem.eql(u8, v, expected);
}

/// `{"keys":[<public JWK>]}`: what the server's JWT key provider reads.
pub fn publicJwks(arena: Allocator, key: Key) error{OutOfMemory}![]const u8 {
    return std.fmt.allocPrint(arena, "{{\"keys\":[{s}]}}", .{try publicJwk(arena, key)});
}

/// The public half of `key` as a JWK object.
fn publicJwk(arena: Allocator, key: Key) error{OutOfMemory}![]const u8 {
    const point = key.pair.public_key.toUncompressedSec1();
    var out: Io.Writer.Allocating = .init(arena);
    var json: std.json.Stringify = .{ .writer = &out.writer };
    json.write(.{
        .kty = "EC",
        .crv = "P-256",
        .alg = "ES256",
        .use = "sig",
        .kid = key.id,
        .x = try base64Url(arena, point[1..33]),
        .y = try base64Url(arena, point[33..65]),
    }) catch return error.OutOfMemory;
    return out.written();
}

/// "<namespace>:read" and "<namespace>:write".
pub fn namespacePermissions(arena: Allocator, namespace: []const u8) error{OutOfMemory}![]const []const u8 {
    const permissions = try arena.alloc([]const u8, 2);
    permissions[0] = try std.fmt.allocPrint(arena, "{s}:read", .{namespace});
    permissions[1] = try std.fmt.allocPrint(arena, "{s}:write", .{namespace});
    return permissions;
}

pub const JwtConfig = struct {
    key: Key,
    permissions: []const []const u8,
    subject: []const u8 = "temporal-samples-zig",
    expiration: Io.Duration = .fromSeconds(600),

    pub const MintError = error{ OutOfMemory, SigningFailed };

    /// A compact JWT: header {alg: ES256, typ: JWT, kid}, claims {sub, permissions, iat, exp}.
    /// It is valid from now, by the real-time clock, for `expiration`.
    pub fn mint(c: JwtConfig, arena: Allocator, io: Io) MintError![]const u8 {
        const now = Io.Clock.real.now(io).toSeconds();
        const header = try base64Url(arena, try std.json.Stringify.valueAlloc(arena, .{ .alg = "ES256", .typ = "JWT", .kid = c.key.id }, .{}));
        const claims = try base64Url(arena, try std.json.Stringify.valueAlloc(arena, .{
            .sub = c.subject,
            .permissions = c.permissions,
            .iat = now,
            .exp = now + c.expiration.toSeconds(),
        }, .{}));
        const signed = try std.fmt.allocPrint(arena, "{s}.{s}", .{ header, claims });
        const signature = c.key.pair.sign(signed, null) catch return error.SigningFailed;
        return std.fmt.allocPrint(arena, "{s}.{s}", .{ signed, try base64Url(arena, &signature.toBytes()) });
    }
};

/// Keeps a client's `authorization` header holding a token that has not expired.
pub const HeaderRefresher = struct {
    config: JwtConfig,
    /// Sent with every call beside the token (what `loadOptions` returned). Borrowed. An
    /// `authorization` entry among them is left out: the token replaces it.
    base_headers: []const HeaderEntry = &.{},
    /// How long before a token expires its replacement is sent. Must be less than `config.expiration`.
    leeway: Io.Duration = .fromSeconds(60),

    /// How long a failed round waits before it tries again.
    const retry_delay: Io.Duration = .fromSeconds(1);

    /// `base_headers` plus `authorization: Bearer <a new token>`, in `arena`.
    pub fn headers(r: *const HeaderRefresher, arena: Allocator, io: Io) JwtConfig.MintError![]const HeaderEntry {
        const token = try r.config.mint(arena, io);
        var list: std.ArrayList(HeaderEntry) = try .initCapacity(arena, r.base_headers.len + 1);
        for (r.base_headers) |h| if (!std.ascii.eqlIgnoreCase(h.key, "authorization")) list.appendAssumeCapacity(h);
        list.appendAssumeCapacity(.{ .key = "authorization", .value = try std.fmt.allocPrint(arena, "Bearer {s}", .{token}) });
        return list.items;
    }

    /// Until canceled: sleeps `config.expiration - leeway`, then `c.updateHeaders(headers)`.
    /// A failed mint or update is logged as a warning and retried after 1 s; the client keeps
    /// sending the previous token meanwhile.
    pub fn run(r: *const HeaderRefresher, gpa: Allocator, io: Io, c: *temporal.client.Client) Io.Cancelable!void {
        std.debug.assert(r.leeway.toNanoseconds() < r.config.expiration.toNanoseconds());
        const interval: Io.Duration = .fromNanoseconds(r.config.expiration.toNanoseconds() - r.leeway.toNanoseconds());
        var delay = interval;
        while (true) {
            try io.sleep(delay, .awake);
            r.refresh(gpa, io, c) catch |err| {
                std.log.warn("Unable to refresh the JWT, will retry in 1s: {t}", .{err});
                delay = retry_delay;
                continue;
            };
            delay = interval;
        }
    }

    fn refresh(r: *const HeaderRefresher, gpa: Allocator, io: Io, c: *temporal.client.Client) !void {
        // The client copies the headers, so they are freed as soon as it has them.
        var arena: std.heap.ArenaAllocator = .init(gpa);
        defer arena.deinit();
        try c.updateHeaders(try r.headers(arena.allocator(), io));
    }
};

fn base64Url(arena: Allocator, bytes: []const u8) error{OutOfMemory}![]const u8 {
    const out = try arena.alloc(u8, base64.Encoder.calcSize(bytes.len));
    return base64.Encoder.encode(out, bytes);
}
