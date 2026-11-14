//! Tests for the external configuration sample: each way of loading the client options, the
//! environment overriding config.toml, the workflow with a test double for its activity, the
//! activity on its own, and the worker against a dev server with options from config.toml.

const std = @import("std");
const temporal = @import("temporal");
const externalenvconf = @import("externalenvconf.zig");

const testing = std.testing;
const Environ = std.process.Environ;

fn header(options: temporal.client.Options, key: []const u8) ?[]const u8 {
    for (options.headers) |h| if (std.mem.eql(u8, h.key, key)) return h.value;
    return null;
}

test "the custom file's default profile, with its gRPC headers" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var env: Environ.Map = .init(testing.allocator);
    defer env.deinit();

    const options = try externalenvconf.loadFromCustomFile(arena.allocator(), &env);
    try testing.expectEqualStrings("localhost:7233", options.host_port);
    try testing.expectEqualStrings("default", options.namespace);
    try testing.expect(options.tls == null);
    try testing.expectEqualStrings("", options.api_key);
    try testing.expectEqual(2, options.headers.len);
    try testing.expectEqualStrings("development-value", header(options, "my-custom-header").?);
    try testing.expectEqualStrings("dev-trace-123", header(options, "trace-id").?);
}

test "environment variables override the custom file" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var env: Environ.Map = .init(testing.allocator);
    defer env.deinit();
    try env.put("TEMPORAL_ADDRESS", "127.0.0.1:7999");
    try env.put("TEMPORAL_NAMESPACE", "from-env");
    try env.put("TEMPORAL_GRPC_META_TRACE_ID", "env-trace");

    const options = try externalenvconf.loadProfile(arena.allocator(), &env);
    try testing.expectEqualStrings("127.0.0.1:7999", options.host_port);
    try testing.expectEqualStrings("from-env", options.namespace);
    try testing.expectEqualStrings("env-trace", header(options, "trace-id").?);
    try testing.expectEqualStrings("development-value", header(options, "my-custom-header").?);
}

test "TEMPORAL_PROFILE picks a profile with inline certificates" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var env: Environ.Map = .init(testing.allocator);
    defer env.deinit();
    try env.put("TEMPORAL_PROFILE", "staging");

    const options = try externalenvconf.loadFromCustomFile(arena.allocator(), &env);
    try testing.expectEqualStrings("staging.temporal.example.com:7233", options.host_port);
    try testing.expectEqualStrings("staging", options.namespace);
    const tls = options.tls.?;
    try testing.expect(std.mem.startsWith(u8, tls.client_cert, "-----BEGIN CERTIFICATE-----"));
    try testing.expect(std.mem.startsWith(u8, tls.client_private_key, "-----BEGIN PRIVATE KEY-----"));
    try testing.expectEqualStrings("", tls.client_cert_path);
}

test "a specific profile, with an API key and certificate paths" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var env: Environ.Map = .init(testing.allocator);
    defer env.deinit();

    const options = try externalenvconf.loadSpecificProfile(arena.allocator(), &env);
    try testing.expectEqualStrings("your-namespace.a1b2c.tmprl.cloud:7233", options.host_port);
    try testing.expectEqualStrings("your-namespace", options.namespace);
    try testing.expectEqualStrings("your-api-key-here", options.api_key);
    const tls = options.tls.?;
    try testing.expectEqualStrings("/etc/temporal/certs/client.pem", tls.client_cert_path);
    try testing.expectEqualStrings("/etc/temporal/certs/client.key", tls.client_private_key_path);
    try testing.expectEqualStrings("production", header(options, "environment").?);
    try testing.expectEqualStrings("v1.2.3", header(options, "service-version").?);
}

test "a profile that doesn't exist is an error" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var env: Environ.Map = .init(testing.allocator);
    defer env.deinit();
    try env.put("TEMPORAL_PROFILE", "missing");

    try testing.expectError(error.InvalidConfig, externalenvconf.loadProfile(arena.allocator(), &env));
}

test "the default profile, and overriding options after loading" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var env: Environ.Map = .init(testing.allocator);
    defer env.deinit();
    // A config file that doesn't exist reads as empty, so only the environment applies.
    try env.put("TEMPORAL_CONFIG_FILE", "does-not-exist/temporal.toml");
    try env.put("TEMPORAL_ADDRESS", "127.0.0.1:7999");

    const defaults = try externalenvconf.loadDefaultProfile(arena.allocator(), &env);
    try testing.expectEqualStrings("127.0.0.1:7999", defaults.host_port);
    try testing.expectEqualStrings("default", defaults.namespace);
    try testing.expectEqual(0, defaults.headers.len);

    const overridden = try externalenvconf.overrideAfterLoading(arena.allocator(), &env);
    try testing.expectEqualStrings("localhost:7233", overridden.host_port);
    try testing.expectEqualStrings("test-namespace", overridden.namespace);
}

fn activityDouble(ctx: *temporal.activity.Context, name: []const u8) ![]const u8 {
    try testing.expectEqualStrings("Temporal", name);
    return ctx.allocator().dupe(u8, "Hello Temporal!");
}

test "workflow with a test double for its activity" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(externalenvconf.Workflow);
    try env.registerActivityAs(externalenvconf.Activity, activityDouble);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const result = try env.executeWorkflow(externalenvconf.Workflow, .{"Temporal"}, arena.allocator(), null);
    try testing.expectEqualStrings("Hello Temporal!", result);
}

test "activity" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const env: temporal.testing.ActivityEnvironment = .init(testing.io);
    const result = try env.executeActivity(externalenvconf.Activity, .{"World"}, arena.allocator());
    try testing.expectEqualStrings("Hello World!", result);
}

fn runWorker(w: *temporal.worker.Worker) temporal.worker.Worker.RunError!void {
    return w.run();
}

test "worker against a dev server, connected with the custom file's options" {
    var server = try temporal.testing.DevServer.start(testing.allocator, testing.io, .{});
    defer server.stop(testing.io);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var env: Environ.Map = .init(testing.allocator);
    defer env.deinit();
    try env.put("TEMPORAL_ADDRESS", server.address());
    const options = try externalenvconf.loadProfile(arena.allocator(), &env);

    var client = try temporal.client.Client.connect(testing.allocator, testing.io, options);
    defer client.close();

    var w = try temporal.worker.Worker.init(testing.allocator, testing.io, &client, "external-env-conf", .{});
    defer w.deinit();
    try w.registerWorkflow(externalenvconf.Workflow);
    try w.registerActivity(externalenvconf.Activity);
    var running = try testing.io.concurrent(runWorker, .{&w});
    defer {
        w.stop();
        running.await(testing.io) catch {};
    }

    var run = try client.executeWorkflow(externalenvconf.Workflow, .{ .id = "external_env_conf_workflowID", .task_queue = "external-env-conf" }, .{"Temporal"});
    defer run.deinit();
    try testing.expectEqualStrings("Hello Temporal!", try run.get(arena.allocator()));
}
