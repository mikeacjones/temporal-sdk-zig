//! Tests for the mutual TLS sample: the workflow with a test double for its activity, the
//! activity on its own, and reading the client options from flags and the environment.

const std = @import("std");
const temporal = @import("temporal");
const helloworld = @import("helloworld.zig");

const testing = std.testing;
const Environ = std.process.Environ;

fn activityDouble(ctx: *temporal.activity.Context, name: []const u8) ![]const u8 {
    try testing.expectEqualStrings("Temporal", name);
    return ctx.allocator().dupe(u8, "Hello Temporal!");
}

test "workflow with a test double for its activity" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(helloworld.Workflow);
    try env.registerActivityAs(helloworld.Activity, activityDouble);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const result = try env.executeWorkflow(helloworld.Workflow, .{"Temporal"}, arena.allocator(), null);
    try testing.expectEqualStrings("Hello Temporal!", result);
}

test "activity" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var env: temporal.testing.ActivityEnvironment = .init(testing.io);
    const result = try env.executeActivity(helloworld.Activity, .{"World"}, arena.allocator());
    try testing.expectEqualStrings("Hello World!", result);
}

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

/// The certificate and key are given as paths only, and no API key or header is sent.
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

    const options = try helloworld.parseClientOptions(arena.allocator(), &.{
        "--target-host",         "example.tmprl.cloud:7233",
        "--namespace",           "my-namespace",
        "--client-cert",         "/flag/client.pem",
        "--client-key",          "/flag/client.key",
        "--server-root-ca-cert", "/flag/ca.pem",
        "--server-name",         "flag-server",
    }, &env);
    try testing.expectEqualStrings("example.tmprl.cloud:7233", options.host_port);
    try testing.expectEqualStrings("my-namespace", options.namespace);
    try testing.expectEqualStrings("/flag/client.pem", options.tls.?.client_cert_path);
    try testing.expectEqualStrings("/flag/client.key", options.tls.?.client_private_key_path);
    try testing.expectEqualStrings("/flag/ca.pem", options.tls.?.server_root_ca_cert_path);
    try testing.expectEqualStrings("flag-server", options.tls.?.server_name);
    try expectOnlyPaths(options);
}

test "defaults: only the client certificate and key are given, and TLS trusts the system roots" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var env: Environ.Map = .init(testing.allocator);
    defer env.deinit();

    const options = try helloworld.parseClientOptions(arena.allocator(), &.{
        "--client-cert", "/flag/client.pem",
        "--client-key",  "/flag/client.key",
    }, &env);
    try testing.expectEqualStrings("localhost:7233", options.host_port);
    try testing.expectEqualStrings("default", options.namespace);
    try testing.expectEqualStrings("/flag/client.pem", options.tls.?.client_cert_path);
    try testing.expectEqualStrings("/flag/client.key", options.tls.?.client_private_key_path);
    try testing.expectEqualStrings("", options.tls.?.server_root_ca_cert_path);
    try testing.expectEqualStrings("", options.tls.?.server_name);
    try expectOnlyPaths(options);
}

test "options from the environment, without the API key it offers" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var env: Environ.Map = .init(testing.allocator);
    defer env.deinit();
    try fixtureEnv(&env);

    const options = try helloworld.parseClientOptions(arena.allocator(), &.{}, &env);
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

    const options = try helloworld.parseClientOptions(arena.allocator(), &.{
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

fn expectInvalid(args: []const []const u8, env: *const Environ.Map, logged: []const []const u8) !void {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var log: std.Io.Writer.Allocating = .init(testing.allocator);
    defer log.deinit();
    helloworld.test_error_log = &log.writer;
    defer helloworld.test_error_log = null;

    try testing.expectError(error.InvalidArguments, helloworld.parseClientOptions(arena.allocator(), args, env));
    const line = log.written();
    try testing.expectEqual(1, std.mem.count(u8, line, "\n"));
    for (logged) |text| {
        if (std.mem.indexOf(u8, line, text) == null) {
            std.debug.print("logged \"{s}\", which does not name {s}\n", .{ line, text });
            return error.TestExpectedEqual;
        }
    }
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
}

test "a flag without a value is an error naming it" {
    var env: Environ.Map = .init(testing.allocator);
    defer env.deinit();
    try fixtureEnv(&env);
    try expectInvalid(&.{"--client-key"}, &env, &.{ "--client-key", "TEMPORAL_TLS_CLIENT_KEY_PATH" });
    try expectInvalid(&.{ "--namespace", "ns", "--target-host" }, &env, &.{ "--target-host", "TEMPORAL_ADDRESS" });
}
