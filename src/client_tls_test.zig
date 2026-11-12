//! Client credentials against a Temporal server in Docker with mutual TLS and JWT
//! authorization (the samples runner's fixture: tools/fixtures/server/config.yaml and the
//! certificates in tools/fixtures/tls): options loaded from the environment connect with a
//! client certificate, an API key update takes effect on the next call, and a reconnect
//! replaces the connection under a running worker. Skipped when Docker isn't available.

const std = @import("std");
const client_mod = @import("client.zig");
const worker_mod = @import("worker.zig");
const workflow = @import("workflow.zig");
const bridge = @import("bridge.zig");
const proto = @import("proto.zig");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const Ecdsa = std.crypto.sign.ecdsa.EcdsaP256Sha256;

const testing = std.testing;
const wfs = proto.temporal.api.workflowservice.v1;

/// The image tools/samples_runner.zig runs.
const server_image = "temporalio/server:1.32.2";
const jwt_key_id = "zig-sdk-tests";
const permission_denied = 7;

const Fixtures = struct {
    fn greet(ctx: *workflow.Context, name: []const u8) ![]const u8 {
        return std.fmt.allocPrint(ctx.allocator(), "Hello {s}!", .{name});
    }
    const Greet = workflow.Def("TlsGreet", greet);
};

/// A server container requiring a client certificate signed by the test CA and a JWT with
/// the permissions for each call, with the "default" namespace registered.
const DockerServer = struct {
    arena_state: std.heap.ArenaAllocator,
    name: []const u8,
    /// host:port of the frontend's gRPC port.
    address: []const u8,
    ca_path: []const u8,
    client_cert_path: []const u8,
    client_key_path: []const u8,
    /// The server's own certificate: signed by the test CA, but not for client auth.
    server_cert_path: []const u8,
    server_key_path: []const u8,
    /// Grants "default:admin".
    token: []const u8,
    /// Grants a namespace that isn't "default".
    other_token: []const u8,
    jwks_path: []const u8,

    fn start(s: *DockerServer) !void {
        s.arena_state = .init(testing.allocator);
        errdefer s.arena_state.deinit();
        const a = s.arena_state.allocator();
        const io = testing.io;

        const root = try repoRoot(a) orelse {
            std.log.warn("skipping: tools/fixtures not found above the working directory", .{});
            return error.SkipZigTest;
        };
        if (run(a, &.{ "docker", "info", "--format", "{{.ServerVersion}}" }, 30)) |_| {} else |err| {
            std.log.warn("skipping: Docker is not available ({t})", .{err});
            return error.SkipZigTest;
        }
        if (run(a, &.{ "docker", "image", "inspect", "--format", "{{.Id}}", server_image }, 30)) |_| {} else |_| {
            _ = try run(a, &.{ "docker", "pull", "--quiet", server_image }, 300);
        }

        const tls_dir = try std.fs.path.join(a, &.{ root, "tools", "fixtures", "tls" });
        s.ca_path = try std.fs.path.join(a, &.{ tls_dir, "ca.pem" });
        s.client_cert_path = try std.fs.path.join(a, &.{ tls_dir, "client.pem" });
        s.client_key_path = try std.fs.path.join(a, &.{ tls_dir, "client.key" });
        s.server_cert_path = try std.fs.path.join(a, &.{ tls_dir, "server.pem" });
        s.server_key_path = try std.fs.path.join(a, &.{ tls_dir, "server.key" });

        var suffix: [6]u8 = undefined;
        io.random(&suffix);
        s.name = try std.fmt.allocPrint(a, "zig-sdk-test-{d}-{x}", .{ std.c.getpid(), &suffix });
        // The server reads the JWKS file while it runs: it stays until `stop`. Docker mounts
        // need an absolute path.
        const key = Ecdsa.KeyPair.generate(io);
        s.jwks_path = try std.fmt.allocPrint(a, "{s}/.zig-cache/tmp/{s}-jwks.json", .{ root, s.name });
        try Io.Dir.cwd().createDirPath(io, std.fs.path.dirname(s.jwks_path).?);
        try Io.Dir.cwd().writeFile(io, .{ .sub_path = s.jwks_path, .data = try std.fmt.allocPrint(a, "{{\"keys\":[{s}]}}", .{try publicJwk(a, key)}) });
        errdefer Io.Dir.cwd().deleteFile(io, s.jwks_path) catch {};
        s.token = try mintJwt(a, key, "default:admin");
        s.other_token = try mintJwt(a, key, "another-namespace:admin");
        const admin_token = try mintJwt(a, key, "temporal-system:admin");

        var argv: std.ArrayList([]const u8) = .empty;
        try argv.appendSlice(a, &.{ "docker", "run", "--detach", "--rm", "--name", s.name, "--publish", "127.0.0.1::7233" });
        const config = try std.fs.path.join(a, &.{ root, "tools", "fixtures", "server", "config.yaml" });
        try argv.appendSlice(a, &.{ "--volume", try std.fmt.allocPrint(a, "{s}:/etc/temporal/config.yaml:ro", .{config}) });
        try argv.appendSlice(a, &.{ "--volume", try std.fmt.allocPrint(a, "{s}:/etc/temporal/tls:ro", .{tls_dir}) });
        try argv.appendSlice(a, &.{ "--volume", try std.fmt.allocPrint(a, "{s}:/etc/temporal/jwks.json:ro", .{s.jwks_path}) });
        try argv.appendSlice(a, &.{
            "--env",      "TEMPORAL_SERVER_CONFIG_FILE_PATH=/etc/temporal/config.yaml",
            "--env",      "TEMPORAL_SERVICES=frontend,internal-frontend,history,matching,worker",
            "--env",      "FIXTURE_REQUIRE_CLIENT_CERT=true",
            "--env",      "FIXTURE_JWKS=file:///etc/temporal/jwks.json",
            server_image,
        });
        // Also after a failed `docker run`, which may have started the container.
        errdefer s.removeContainer();
        _ = try run(a, argv.items, 60);
        const port_line = try run(a, &.{ "docker", "port", s.name, "7233/tcp" }, 30);
        const colon = std.mem.lastIndexOfScalar(u8, port_line, ':') orelse return error.UnexpectedDockerOutput;
        s.address = try std.fmt.allocPrint(a, "127.0.0.1:{s}", .{port_line[colon + 1 ..]});
        try s.registerNamespace(admin_token);
    }

    fn stop(s: *DockerServer) void {
        s.removeContainer();
        s.arena_state.deinit();
    }

    fn removeContainer(s: *DockerServer) void {
        _ = run(s.arena_state.allocator(), &.{ "docker", "rm", "--force", s.name }, 60) catch |err|
            std.log.warn("could not remove the server container {s}: {t}", .{ s.name, err });
        Io.Dir.cwd().deleteFile(testing.io, s.jwks_path) catch {};
    }

    /// Registers "default", which also tells the frontend is up: retried until it answers.
    fn registerNamespace(s: *DockerServer, admin_token: []const u8) !void {
        const a = s.arena_state.allocator();
        const rt = try bridge.Runtime.init(testing.allocator, .{}, null);
        defer rt.deinit();
        const url = try std.fmt.allocPrint(a, "https://{s}", .{s.address});
        const tls: bridge.Connection.Options.Tls = .{
            .server_root_ca_cert = try readFile(a, s.ca_path),
            .domain = "localhost",
            .client_cert = try readFile(a, s.client_cert_path),
            .client_private_key = try readFile(a, s.client_key_path),
        };
        const request = try encode(a, wfs.RegisterNamespaceRequest{
            .namespace = "default",
            .workflow_execution_retention_period = .{ .seconds = std.time.s_per_day },
        });
        const deadline = Io.Timestamp.now(testing.io, .awake).addDuration(.fromSeconds(90));
        var last: []const u8 = "";
        while (Io.Timestamp.now(testing.io, .awake).nanoseconds < deadline.nanoseconds) {
            var message: ?[]u8 = null;
            defer if (message) |m| testing.allocator.free(m);
            const conn = bridge.Connection.connect(testing.allocator, testing.io, rt, .{
                .target_url = url,
                .identity = "zig-sdk-tests",
                .api_key = admin_token,
                .tls = tls,
            }, &message) catch {
                last = try a.dupe(u8, message orelse "connect failed");
                try testing.io.sleep(.fromMilliseconds(250), .awake);
                continue;
            };
            defer conn.deinit();
            var failure: ?bridge.Connection.RpcFailure = null;
            defer if (failure) |f| f.deinit(testing.allocator);
            if (conn.call(testing.allocator, testing.io, .workflow, "RegisterNamespace", request, .{}, &failure)) |bytes| {
                testing.allocator.free(bytes);
                return;
            } else |_| {
                // ALREADY_EXISTS: an earlier attempt got through.
                if (failure != null and failure.?.status_code == 6) return;
                last = try a.dupe(u8, if (failure) |f| f.message else "RegisterNamespace failed");
            }
            try testing.io.sleep(.fromMilliseconds(250), .awake);
        }
        std.log.warn("the server container {s} did not register the default namespace within 90s: {s}", .{ s.name, last });
        return error.ServerNotReady;
    }

    /// The environment a deployment would set for a client with mutual TLS and `token`.
    fn env(s: *const DockerServer, map: *std.process.Environ.Map, token: []const u8) !void {
        try map.put("TEMPORAL_ADDRESS", s.address);
        try map.put("TEMPORAL_NAMESPACE", "default");
        try map.put("TEMPORAL_API_KEY", token);
        try map.put("TEMPORAL_TLS_SERVER_NAME", "localhost");
        try map.put("TEMPORAL_TLS_SERVER_CA_CERT_PATH", s.ca_path);
        try map.put("TEMPORAL_TLS_CLIENT_CERT_PATH", s.client_cert_path);
        try map.put("TEMPORAL_TLS_CLIENT_KEY_PATH", s.client_key_path);
    }

    /// Client options loaded from the environment `env` describes.
    fn clientOptions(s: *const DockerServer, arena: Allocator, token: []const u8) !client_mod.Options {
        var map: std.process.Environ.Map = .init(testing.allocator);
        defer map.deinit();
        try s.env(&map, token);
        var options = try client_mod.loadOptions(arena, .{ .env = &map, .disable_file = true });
        // Worker polls refused while a test swaps credentials would be logged as errors.
        options.core_log_level = null;
        return options;
    }
};

/// DescribeNamespace over the client's current connection: its gRPC status code, 0 for OK.
fn describeStatus(c: *client_mod.Client, arena: Allocator) !u32 {
    var failure: ?bridge.Connection.RpcFailure = null;
    defer if (failure) |f| f.deinit(testing.allocator);
    const bytes = c.currentConnection().call(testing.allocator, testing.io, .workflow, "DescribeNamespace", try encode(arena, wfs.DescribeNamespaceRequest{ .namespace = "default" }), .{}, &failure) catch |err| switch (err) {
        error.RpcFailed => return failure.?.status_code,
        else => return err,
    };
    testing.allocator.free(bytes);
    return 0;
}

test "client credentials against a server with mutual TLS and JWT authorization" {
    var server: DockerServer = undefined;
    try server.start();
    defer server.stop();
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    // Options from the environment: certificate paths, read when connecting. The server
    // checks the API key per call, so a client with the wrong one still connects.
    const wrong_key = try server.clientOptions(a, server.other_token);
    try testing.expectEqualStrings(server.client_cert_path, wrong_key.tls.?.client_cert_path);
    var c = try client_mod.Client.connect(testing.allocator, testing.io, wrong_key);
    defer c.close();

    // An API key update applies to the next call.
    try testing.expectEqual(permission_denied, try describeStatus(&c, a));
    c.updateApiKey(server.token);
    try testing.expectEqual(0, try describeStatus(&c, a));
    try c.updateHeaders(&.{.{ .key = "x-deployment", .value = "blue" }});
    try testing.expectError(error.InvalidOptions, c.updateHeaders(&.{.{ .key = "x-deployment", .value = "gr\xc3\xbcn" }}));
    try testing.expectEqual(0, try describeStatus(&c, a));

    // A reconnect moves a running worker: the workflow started after it runs on the worker
    // started before it. The reconnect reads the certificate files again, as after a
    // rotation on disk.
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const cert_path = try std.fs.path.join(a, &.{ ".zig-cache", "tmp", &tmp.sub_path, "client.pem" });
    const key_path = try std.fs.path.join(a, &.{ ".zig-cache", "tmp", &tmp.sub_path, "client.key" });
    const good_cert = try readFile(a, server.client_cert_path);
    const good_key = try readFile(a, server.client_key_path);
    try tmp.dir.writeFile(testing.io, .{ .sub_path = "client.pem", .data = good_cert });
    try tmp.dir.writeFile(testing.io, .{ .sub_path = "client.key", .data = good_key });
    const options = try server.clientOptions(a, server.token);
    var rotated = options.connectionOptions();
    rotated.tls.?.client_cert_path = cert_path;
    rotated.tls.?.client_private_key_path = key_path;

    var w = try worker_mod.Worker.init(testing.allocator, testing.io, &c, "tls-rotation", .{});
    defer w.deinit();
    try w.registerWorkflow(Fixtures.Greet);
    var running: ?Io.Future(worker_mod.Worker.RunError!void) = try testing.io.concurrent(worker_mod.Worker.run, .{&w});
    defer if (running) |*r| {
        w.stop();
        r.await(testing.io) catch {};
    };
    try w.waitUntilRunning();
    const before = c.currentConnection();
    try c.reconnect(rotated);
    try testing.expect(c.currentConnection().ptr != before.ptr);
    var run_handle = try c.executeWorkflow(Fixtures.Greet, .{ .task_queue = "tls-rotation" }, .{"rotation"});
    defer run_handle.deinit();
    try testing.expectEqualStrings("Hello rotation!", try run_handle.get(a));
    w.stop();
    try running.?.await(testing.io);
    running = null;

    // Rotated to a certificate the server refuses: the reconnect fails and the client keeps
    // its connection. Rotated back: the reconnect succeeds.
    try tmp.dir.writeFile(testing.io, .{ .sub_path = "client.pem", .data = try readFile(a, server.server_cert_path) });
    try tmp.dir.writeFile(testing.io, .{ .sub_path = "client.key", .data = try readFile(a, server.server_key_path) });
    const kept = c.currentConnection();
    try testing.expectError(error.ConnectFailed, c.reconnect(rotated));
    try testing.expectEqual(kept.ptr, c.currentConnection().ptr);
    try testing.expectEqual(0, try describeStatus(&c, a));
    try tmp.dir.writeFile(testing.io, .{ .sub_path = "client.pem", .data = good_cert });
    try tmp.dir.writeFile(testing.io, .{ .sub_path = "client.key", .data = good_key });
    try c.reconnect(rotated);
    try testing.expectEqual(0, try describeStatus(&c, a));

    // Calls go over the new connection: one made with another key is refused, and a
    // reconnect with the right key is accepted again.
    var other = options.connectionOptions();
    other.api_key = server.other_token;
    try c.reconnect(other);
    try testing.expectEqual(permission_denied, try describeStatus(&c, a));
    try c.reconnect(options.connectionOptions());
    try testing.expectEqual(0, try describeStatus(&c, a));
}

/// The directory above the working directory that holds tools/fixtures, or null.
fn repoRoot(a: Allocator) !?[]const u8 {
    var dir: ?[]const u8 = try Io.Dir.cwd().realPathFileAlloc(testing.io, ".", a);
    while (dir) |d| : (dir = std.fs.path.dirname(d)) {
        const config = try std.fs.path.join(a, &.{ d, "tools", "fixtures", "server", "config.yaml" });
        Io.Dir.cwd().access(testing.io, config, .{}) catch continue;
        return d;
    }
    return null;
}

/// Runs `argv` and returns its trimmed stdout, or fails if it doesn't exit 0 in `seconds`.
fn run(a: Allocator, argv: []const []const u8, seconds: i64) ![]const u8 {
    const r = try std.process.run(a, testing.io, .{
        .argv = argv,
        .timeout = .{ .duration = .{ .raw = .fromSeconds(seconds), .clock = .awake } },
    });
    switch (r.term) {
        .exited => |code| if (code == 0) return std.mem.trim(u8, r.stdout, " \t\r\n"),
        else => {},
    }
    std.log.warn("{s} {s} failed: {s}", .{ argv[0], argv[1], std.mem.trim(u8, r.stderr, " \t\r\n") });
    return error.CommandFailed;
}

fn readFile(a: Allocator, path: []const u8) ![]const u8 {
    return Io.Dir.cwd().readFileAlloc(testing.io, path, a, .limited(1 << 20));
}

fn encode(a: Allocator, msg: anytype) ![]const u8 {
    var w: Io.Writer.Allocating = .init(a);
    try msg.encode(&w.writer, a);
    return w.written();
}

fn publicJwk(a: Allocator, key: Ecdsa.KeyPair) ![]const u8 {
    const point = key.public_key.toUncompressedSec1();
    return std.fmt.allocPrint(a, "{{\"kty\":\"EC\",\"crv\":\"P-256\",\"alg\":\"ES256\",\"use\":\"sig\",\"kid\":\"{s}\",\"x\":\"{s}\",\"y\":\"{s}\"}}", .{
        jwt_key_id,
        try base64Url(a, point[1..33]),
        try base64Url(a, point[33..65]),
    });
}

/// An ES256 JWT granting `permission` ("<namespace>:<role>"), valid for a day.
fn mintJwt(a: Allocator, key: Ecdsa.KeyPair, permission: []const u8) ![]const u8 {
    const now = Io.Clock.real.now(testing.io).toSeconds();
    const header = try base64Url(a, "{\"alg\":\"ES256\",\"typ\":\"JWT\",\"kid\":\"" ++ jwt_key_id ++ "\"}");
    const claims = try base64Url(a, try std.fmt.allocPrint(a, "{{\"sub\":\"zig-sdk-tests\",\"permissions\":[\"{s}\"],\"iat\":{d},\"exp\":{d}}}", .{ permission, now, now + std.time.s_per_day }));
    const signed = try std.fmt.allocPrint(a, "{s}.{s}", .{ header, claims });
    const signature = try key.sign(signed, null);
    return std.fmt.allocPrint(a, "{s}.{s}", .{ signed, try base64Url(a, &signature.toBytes()) });
}

fn base64Url(a: Allocator, bytes: []const u8) ![]const u8 {
    const encoder = std.base64.url_safe_no_pad.Encoder;
    return encoder.encode(try a.alloc(u8, encoder.calcSize(bytes.len)), bytes);
}
