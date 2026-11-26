//! Tests of the sample against a dev server: the nexus sample's handler on the environment's
//! worker behind an endpoint of the test's own, its echo and say-hello operations run from
//! the client, and listing and counting them.

const std = @import("std");
const temporal = @import("temporal");
const nexus = @import("nexus");

const service = nexus.service;
const testing = std.testing;
const Client = temporal.client.Client;

const endpoint = "nexus-standalone-operations-test-endpoint";
const query = "Endpoint = '" ++ endpoint ++ "'";

/// Returns `error.SkipZigTest`, logging why, when `c`'s server doesn't offer standalone Nexus
/// operations (it doesn't answer CountNexusOperationExecutions).
fn skipWithoutStandaloneNexus(c: *Client) !void {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var failure: ?temporal.client.RpcFailure = null;
    _ = c.callService(.workflow, "CountNexusOperationExecutions", arena.allocator(), .{ .namespace = c.namespace }, &failure) catch |err| {
        const message = if (failure) |f| f.message else "";
        std.log.warn("skipping: the dev server doesn't offer standalone Nexus operations (CountNexusOperationExecutions returned {t}: {s})", .{ err, message });
        return error.SkipZigTest;
    };
}

/// Whether the operations listed for `query` are at least one, each with an ID and the
/// test's endpoint.
fn listed(c: *Client, arena: std.mem.Allocator) !bool {
    var it = try c.listNexusOperations(query, .{});
    defer it.deinit();
    var count: usize = 0;
    while (try it.next(arena)) |info| {
        if (info.operation_id.len == 0 or !std.mem.eql(u8, info.endpoint, endpoint)) return false;
        count += 1;
    }
    return count > 0;
}

test "standalone Nexus operations using a dev server" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    const c = &env.client;
    try skipWithoutStandaloneNexus(c);

    // The nexus sample's handler, on the environment's worker and task queue.
    try env.registerWorkflow(nexus.handler.HelloHandlerWorkflow);
    try env.registerNexusService(nexus.handler.HelloServiceHandler);
    try env.createNexusEndpoint(endpoint, .{});

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // The environment's worker starts with its first workflow: run the handler workflow once
    // so the worker polls before the operations start.
    const direct = try env.executeWorkflow(nexus.handler.HelloHandlerWorkflow, .{.{ .name = "World", .language = .en }}, a, null);
    try testing.expectEqualStrings("Hello World 👋", direct.message);

    // The synchronous echo operation; an empty ID is a generated one.
    var echo = try c.executeNexusOperation(service.HelloService, service.Echo, .{
        .endpoint = endpoint,
        .schedule_to_close_timeout = .fromSeconds(10),
    }, .{ .message = "hello-nexus" });
    defer echo.deinit();
    try testing.expect(echo.id.len > 0);
    try testing.expectEqualStrings("hello-nexus", (try echo.get(a)).message);

    // The asynchronous say-hello operation.
    var hello = try c.executeNexusOperation(service.HelloService, service.SayHello, .{
        .endpoint = endpoint,
        .schedule_to_close_timeout = .fromSeconds(10),
    }, .{ .name = "Temporal", .language = .en });
    defer hello.deinit();
    try testing.expect(hello.id.len > 0);
    try testing.expectEqualStrings("Hello Temporal 👋", (try hello.get(a)).message);

    // Visibility is eventually consistent: poll listing, then counting, for up to 10 s each.
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    var deadline = std.Io.Timestamp.now(testing.io, .awake).addDuration(.fromSeconds(10));
    while (true) {
        _ = scratch.reset(.retain_capacity);
        if (try listed(c, scratch.allocator())) break;
        if (std.Io.Timestamp.now(testing.io, .awake).nanoseconds >= deadline.nanoseconds) return error.TestOperationsNotListed;
        try testing.io.sleep(.fromMilliseconds(500), .awake);
    }
    deadline = std.Io.Timestamp.now(testing.io, .awake).addDuration(.fromSeconds(10));
    while (try c.countNexusOperations(query) == 0) {
        if (std.Io.Timestamp.now(testing.io, .awake).nanoseconds >= deadline.nanoseconds) return error.TestOperationsNotCounted;
        try testing.io.sleep(.fromMilliseconds(500), .awake);
    }
}
