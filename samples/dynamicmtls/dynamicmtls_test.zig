//! Tests for the certificate-reloading sample: reading the client options and the reload
//! interval from flags and the environment, and noticing a changed certificate or key.

const std = @import("std");
const temporal = @import("temporal");
const dynamicmtls = @import("dynamicmtls.zig");

const testing = std.testing;
const Environ = std.process.Environ;
const CertificateWatcher = dynamicmtls.CertificateWatcher;

/// The environment the samples-test fixture gives the programs, which also offers an API key
/// the sample must not send.
fn fixtureEnv(env: *Environ.Map) !void {
    try env.put("TEMPORAL_ADDRESS", "127.0.0.1:7999");
    try env.put("TEMPORAL_NAMESPACE", "from-env");
    try env.put("TEMPORAL_API_KEY", "env-key");
    try env.put("TEMPORAL_TLS", "true");
    try env.put("TEMPORAL_TLS_SERVER_CA_CERT_PATH", "/env/ca.pem");
    try env.put("TEMPORAL_TLS_SERVER_NAME", "env-server");
    try env.put("TEMPORAL_TLS_CLIENT_CERT_PATH", "/env/client.pem");
    try env.put("TEMPORAL_TLS_CLIENT_KEY_PATH", "/env/client.key");
}

/// The certificate and key are given as paths only, so a reconnect reads them again, and no
/// API key or header is sent.
fn expectOnlyPaths(options: temporal.client.Options) !void {
    const tls = options.tls.?;
    try testing.expectEqualStrings("", tls.client_cert);
    try testing.expectEqualStrings("", tls.client_private_key);
    try testing.expectEqualStrings("", tls.server_root_ca_cert);
    try testing.expectEqualStrings("", options.api_key);
    try testing.expectEqual(0, options.headers.len);
}

test "options from flags" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var env: Environ.Map = .init(testing.allocator);
    defer env.deinit();

    const args: []const []const u8 = &.{
        "--target-host",         "example.tmprl.cloud:7233",
        "--namespace",           "my-namespace",
        "--client-cert",         "/flag/client.pem",
        "--client-key",          "/flag/client.key",
        "--server-root-ca-cert", "/flag/ca.pem",
        "--server-name",         "flag-server",
        "--reload-interval",     "5",
    };
    const options = try dynamicmtls.parseClientOptions(arena.allocator(), args, &env);
    try testing.expectEqualStrings("example.tmprl.cloud:7233", options.host_port);
    try testing.expectEqualStrings("my-namespace", options.namespace);
    try testing.expectEqualStrings("/flag/client.pem", options.tls.?.client_cert_path);
    try testing.expectEqualStrings("/flag/client.key", options.tls.?.client_private_key_path);
    try testing.expectEqualStrings("/flag/ca.pem", options.tls.?.server_root_ca_cert_path);
    try testing.expectEqualStrings("flag-server", options.tls.?.server_name);
    try expectOnlyPaths(options);
    try testing.expectEqual(std.Io.Duration.fromSeconds(5), try dynamicmtls.reloadInterval(args));
}

test "defaults: only the client certificate and key are given, TLS trusts the system roots, and the interval is 30 s" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var env: Environ.Map = .init(testing.allocator);
    defer env.deinit();

    const args: []const []const u8 = &.{
        "--client-cert", "/flag/client.pem",
        "--client-key",  "/flag/client.key",
    };
    const options = try dynamicmtls.parseClientOptions(arena.allocator(), args, &env);
    try testing.expectEqualStrings("localhost:7233", options.host_port);
    try testing.expectEqualStrings("default", options.namespace);
    try testing.expectEqualStrings("/flag/client.pem", options.tls.?.client_cert_path);
    try testing.expectEqualStrings("/flag/client.key", options.tls.?.client_private_key_path);
    try testing.expectEqualStrings("", options.tls.?.server_root_ca_cert_path);
    try testing.expectEqualStrings("", options.tls.?.server_name);
    try expectOnlyPaths(options);
    try testing.expectEqual(std.Io.Duration.fromSeconds(30), try dynamicmtls.reloadInterval(args));
    try testing.expectEqual(std.Io.Duration.fromSeconds(30), try dynamicmtls.reloadInterval(&.{}));
}

test "options from the environment, without the API key it offers" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var env: Environ.Map = .init(testing.allocator);
    defer env.deinit();
    try fixtureEnv(&env);

    const options = try dynamicmtls.parseClientOptions(arena.allocator(), &.{}, &env);
    try testing.expectEqualStrings("127.0.0.1:7999", options.host_port);
    try testing.expectEqualStrings("from-env", options.namespace);
    try testing.expectEqualStrings("/env/client.pem", options.tls.?.client_cert_path);
    try testing.expectEqualStrings("/env/client.key", options.tls.?.client_private_key_path);
    try testing.expectEqualStrings("/env/ca.pem", options.tls.?.server_root_ca_cert_path);
    try testing.expectEqualStrings("env-server", options.tls.?.server_name);
    try expectOnlyPaths(options);
}

test "a flag overrides its environment variable" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var env: Environ.Map = .init(testing.allocator);
    defer env.deinit();
    try fixtureEnv(&env);

    const options = try dynamicmtls.parseClientOptions(arena.allocator(), &.{
        "--client-cert",         "/flag/client.pem",
        "--target-host",         "flag-host:7233",
        // An empty CA path trusts the system roots, though the environment names a file.
        "--server-root-ca-cert", "",
    }, &env);
    try testing.expectEqualStrings("flag-host:7233", options.host_port);
    try testing.expectEqualStrings("/flag/client.pem", options.tls.?.client_cert_path);
    try testing.expectEqualStrings("/env/client.key", options.tls.?.client_private_key_path);
    try testing.expectEqualStrings("", options.tls.?.server_root_ca_cert_path);
    try testing.expectEqualStrings("from-env", options.namespace);
    try testing.expectEqualStrings("env-server", options.tls.?.server_name);
    try expectOnlyPaths(options);
}

/// Captures the one error line `parseClientOptions` or `reloadInterval` writes and checks
/// that it names each of `logged`.
const ErrorLog = struct {
    log: std.Io.Writer.Allocating,

    fn init() ErrorLog {
        return .{ .log = .init(testing.allocator) };
    }

    fn attach(e: *ErrorLog) void {
        dynamicmtls.test_error_log = &e.log.writer;
    }

    fn deinit(e: *ErrorLog) void {
        dynamicmtls.test_error_log = null;
        e.log.deinit();
    }

    fn expectOneLineNaming(e: *ErrorLog, logged: []const []const u8) !void {
        const line = e.log.written();
        try testing.expectEqual(1, std.mem.count(u8, line, "\n"));
        for (logged) |text| {
            if (std.mem.indexOf(u8, line, text) == null) {
                std.debug.print("logged \"{s}\", which does not name {s}\n", .{ line, text });
                return error.TestExpectedEqual;
            }
        }
    }
};

fn expectInvalid(args: []const []const u8, env: *const Environ.Map, logged: []const []const u8) !void {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var log: ErrorLog = .init();
    defer log.deinit();
    log.attach();
    try testing.expectError(error.InvalidArguments, dynamicmtls.parseClientOptions(arena.allocator(), args, env));
    try log.expectOneLineNaming(logged);
}

fn expectInvalidInterval(args: []const []const u8, logged: []const []const u8) !void {
    var log: ErrorLog = .init();
    defer log.deinit();
    log.attach();
    try testing.expectError(error.InvalidArguments, dynamicmtls.reloadInterval(args));
    try log.expectOneLineNaming(logged);
}

test "a missing client certificate is an error naming the flag and the variable" {
    var env: Environ.Map = .init(testing.allocator);
    defer env.deinit();
    const named: []const []const u8 = &.{ "--client-cert", "TEMPORAL_TLS_CLIENT_CERT_PATH" };
    try expectInvalid(&.{ "--client-key", "/c.key" }, &env, named);
    try env.put("TEMPORAL_TLS_CLIENT_CERT_PATH", "");
    try expectInvalid(&.{ "--client-key", "/c.key" }, &env, named);
    try env.put("TEMPORAL_TLS_CLIENT_CERT_PATH", "/env/client.pem");
    try expectInvalid(&.{ "--client-cert", "", "--client-key", "/c.key" }, &env, named);
}

test "a missing client key is an error naming the flag and the variable" {
    var env: Environ.Map = .init(testing.allocator);
    defer env.deinit();
    const named: []const []const u8 = &.{ "--client-key", "TEMPORAL_TLS_CLIENT_KEY_PATH" };
    try expectInvalid(&.{ "--client-cert", "/c.pem" }, &env, named);
    try env.put("TEMPORAL_TLS_CLIENT_KEY_PATH", "");
    try expectInvalid(&.{ "--client-cert", "/c.pem" }, &env, named);
    try env.put("TEMPORAL_TLS_CLIENT_KEY_PATH", "/env/client.key");
    try expectInvalid(&.{ "--client-cert", "/c.pem", "--client-key", "" }, &env, named);
}

test "an API key in the environment does not stand in for the client certificate" {
    var env: Environ.Map = .init(testing.allocator);
    defer env.deinit();
    try env.put("TEMPORAL_API_KEY", "env-key");
    try expectInvalid(&.{}, &env, &.{ "--client-cert", "TEMPORAL_TLS_CLIENT_CERT_PATH" });
}

test "an unknown flag is an error naming it" {
    var env: Environ.Map = .init(testing.allocator);
    defer env.deinit();
    try fixtureEnv(&env);
    try expectInvalid(&.{"--insecure-skip-verify"}, &env, &.{"--insecure-skip-verify"});
    try expectInvalid(&.{ "--api-key", "k" }, &env, &.{"--api-key"});
    try expectInvalid(&.{"positional"}, &env, &.{"positional"});
    try expectInvalid(&.{"-client-cert=x"}, &env, &.{"-client-cert=x"});
    try expectInvalidInterval(&.{ "--refresh-interval", "5" }, &.{"--refresh-interval"});
}

test "a flag without a value is an error naming it" {
    var env: Environ.Map = .init(testing.allocator);
    defer env.deinit();
    try fixtureEnv(&env);
    try expectInvalid(&.{"--client-key"}, &env, &.{ "--client-key", "TEMPORAL_TLS_CLIENT_KEY_PATH" });
    try expectInvalid(&.{ "--namespace", "ns", "--target-host" }, &env, &.{ "--target-host", "TEMPORAL_ADDRESS" });
    try expectInvalid(&.{"--reload-interval"}, &env, &.{ "--reload-interval", "30" });
    try expectInvalidInterval(&.{"--reload-interval"}, &.{ "--reload-interval", "30" });
}

test "a reload interval that is not a positive whole number of seconds is an error naming it" {
    var env: Environ.Map = .init(testing.allocator);
    defer env.deinit();
    try fixtureEnv(&env);
    for ([_][]const u8{ "0", "-5", "", "1.5", "5s", "abc", "0x10", "99999999999" }) |value| {
        const args: []const []const u8 = &.{ "--reload-interval", value };
        const named: []const []const u8 = &.{ "--reload-interval", value };
        try expectInvalid(args, &env, named);
        try expectInvalidInterval(args, named);
    }
}

/// Two files in a temporary directory, with their absolute paths.
const CertFiles = struct {
    tmp: testing.TmpDir,
    cert_path: [:0]const u8,
    key_path: [:0]const u8,

    fn init() !CertFiles {
        var tmp = testing.tmpDir(.{});
        errdefer tmp.cleanup();
        try tmp.dir.writeFile(testing.io, .{ .sub_path = "client.pem", .data = "certificate 1" });
        try tmp.dir.writeFile(testing.io, .{ .sub_path = "client.key", .data = "key 1" });
        const cert_path = try tmp.dir.realPathFileAlloc(testing.io, "client.pem", testing.allocator);
        errdefer testing.allocator.free(cert_path);
        const key_path = try tmp.dir.realPathFileAlloc(testing.io, "client.key", testing.allocator);
        return .{ .tmp = tmp, .cert_path = cert_path, .key_path = key_path };
    }

    fn deinit(f: *CertFiles) void {
        testing.allocator.free(f.cert_path);
        testing.allocator.free(f.key_path);
        f.tmp.cleanup();
    }

    fn write(f: *CertFiles, name: []const u8, data: []const u8) !void {
        try f.tmp.dir.writeFile(testing.io, .{ .sub_path = name, .data = data });
    }

    fn watcher(f: *const CertFiles) CertificateWatcher {
        return .init(testing.allocator, testing.io, f.cert_path, f.key_path);
    }
};

test "unchanged certificate files are no change" {
    var files: CertFiles = try .init();
    defer files.deinit();
    const w = files.watcher();
    try testing.expect(w.seen[0] != null and w.seen[1] != null);
    try testing.expectEqual(null, w.changed());
    // Rewriting a file with the same bytes is no change.
    try files.write("client.pem", "certificate 1");
    try testing.expectEqual(null, w.changed());
}

test "a rewritten certificate is a change, and once accepted is no longer one" {
    var files: CertFiles = try .init();
    defer files.deinit();
    var w = files.watcher();
    const before = w.seen;

    try files.write("client.pem", "certificate 2");
    const now = w.changed() orelse return error.TestExpectedChange;
    try testing.expect(!std.mem.eql(u8, &now[0].?, &before[0].?));
    try testing.expectEqualSlices(u8, &before[1].?, &now[1].?);
    // Asking again without accepting gives the same digests, as a retried round does.
    try testing.expectEqual(now, w.changed().?);

    w.seen = now;
    try testing.expectEqual(null, w.changed());
}

test "a rewritten key alone is a change" {
    var files: CertFiles = try .init();
    defer files.deinit();
    const w = files.watcher();
    try files.write("client.key", "key 2");
    const now = w.changed() orelse return error.TestExpectedChange;
    try testing.expectEqualSlices(u8, &w.seen[0].?, &now[0].?);
    try testing.expect(!std.mem.eql(u8, &now[1].?, &w.seen[1].?));
}

test "a missing key is no change until it is back, even with a new certificate" {
    var files: CertFiles = try .init();
    defer files.deinit();
    const w = files.watcher();
    try files.write("client.pem", "certificate 2");
    try files.tmp.dir.deleteFile(testing.io, "client.key");
    try testing.expectEqual(null, w.changed());
    try files.write("client.key", "key 2");
    try testing.expect(w.changed() != null);
}

test "restoring the same bytes is no change" {
    var files: CertFiles = try .init();
    defer files.deinit();
    const w = files.watcher();
    try files.write("client.pem", "certificate 2");
    try testing.expect(w.changed() != null);
    try files.write("client.pem", "certificate 1");
    try testing.expectEqual(null, w.changed());
}

test "files missing at the start are a change once both can be read" {
    var files: CertFiles = try .init();
    defer files.deinit();
    try files.tmp.dir.deleteFile(testing.io, "client.pem");
    const w = files.watcher();
    try testing.expectEqual(null, w.seen[0]);
    try testing.expectEqual(null, w.changed());
    try files.write("client.pem", "certificate 1");
    try testing.expect(w.changed() != null);
}

test "a directory in place of a file is unreadable, not an error" {
    var files: CertFiles = try .init();
    defer files.deinit();
    const w = files.watcher();
    try files.tmp.dir.deleteFile(testing.io, "client.key");
    try files.tmp.dir.createDir(testing.io, "client.key", .default_dir);
    try files.write("client.pem", "certificate 2");
    try testing.expectEqual(null, w.changed());
}
