//! Clients for several namespaces over one connection (`Client.forNamespace`): the clients
//! share the sdk-core runtime and the connection, and are closed in any order;
//! `reconnect`, `updateApiKey` and `updateHeaders` on any of them apply to all and move every
//! worker; against a dev server with a second namespace, each client works in its own; and the
//! core's logs keep reaching the client's logger after core objects built on its runtime are
//! freed.

const std = @import("std");
const client_mod = @import("client.zig");
const worker_mod = @import("worker.zig");
const workflow = @import("workflow.zig");
const converter = @import("converter.zig");
const propagation = @import("propagation.zig");
const interceptor = @import("interceptor.zig");
const log_mod = @import("log.zig");
const bridge = @import("bridge.zig");
const DevServer = @import("testing.zig").DevServer;
const Client = client_mod.Client;
const Io = std.Io;

const testing = std.testing;

/// Nothing listens here, so a client from `init` stays unconnected.
const closed_port = "127.0.0.1:1";
const second_namespace = "zig-sdk-second-namespace";

const Fixtures = struct {
    /// Returns the namespace it runs in.
    fn namespaceOf(ctx: *workflow.Context) ![]const u8 {
        return ctx.allocator().dupe(u8, ctx.info().namespace);
    }
    const NamespaceOf = workflow.Def("NamespaceOf", namespaceOf);

    fn runWorker(w: *worker_mod.Worker) worker_mod.Worker.RunError!void {
        return w.run();
    }
};

fn startDevServer() !DevServer {
    return DevServer.start(testing.allocator, testing.io, .{ .extra_args = &.{ "--namespace", second_namespace } });
}

test "a client for another namespace shares the runtime and the connection and carries the client's settings" {
    var recorder: bridge.metric.TestRecorder = .init(testing.allocator);
    defer recorder.deinit();
    var marker: u8 = 0;
    var codecs: [1]converter.PayloadCodec = undefined;
    var propagators: [1]propagation.ContextPropagator = undefined;
    var client_interceptors: [1]interceptor.Client = undefined;
    var worker_interceptors: [1]interceptor.Worker = undefined;
    var a = try Client.init(testing.allocator, testing.io, .{
        .host_port = closed_port,
        .namespace = "first",
        .identity = "the-identity",
        .core_log_level = null,
        .codecs = &codecs,
        .encode_failure_attributes = true,
        .propagators = &propagators,
        .interceptors = &client_interceptors,
        .worker_interceptors = &worker_interceptors,
        .logger = .{ .ptr = &marker, .vtable = log_mod.Sink.std_log.vtable },
        .metrics = .{ .exporter = .{ .custom = .{ .ptr = &recorder, .vtable = &bridge.metric.TestRecorder.vtable } } },
        .rpc_timeout = .fromSeconds(7),
    });
    defer a.close();
    var namespace_buf: [6]u8 = "second".*;
    var b = try a.forNamespace(&namespace_buf);
    defer b.close();
    // The namespace is copied.
    namespace_buf = "XXXXXX".*;

    try testing.expectEqualStrings("first", a.namespace);
    try testing.expectEqualStrings("second", b.namespace);
    // One runtime and one connection state: nothing new was created.
    try testing.expectEqual(a.shared, b.shared);
    try testing.expectEqual(@as(u32, 2), a.shared.refs.load(.monotonic));
    try testing.expect(b.shared.connection == null);
    try testing.expect(b.metrics().ptr == a.metrics().ptr);
    try testing.expect(b.metrics().ptr != null);

    try testing.expectEqualStrings("the-identity", b.identity);
    try testing.expect(b.identity.ptr != a.identity.ptr);
    try testing.expectEqual(a.codecs.ptr, b.codecs.ptr);
    try testing.expectEqual(@as(usize, 1), b.codecs.len);
    try testing.expect(b.encode_failure_attributes);
    try testing.expectEqual(a.propagators.ptr, b.propagators.ptr);
    try testing.expectEqual(a.interceptors.ptr, b.interceptors.ptr);
    try testing.expectEqual(a.worker_interceptors.ptr, b.worker_interceptors.ptr);
    try testing.expect(b.logger.ptr == @as(?*anyopaque, &marker));
    try testing.expectEqual(@as(u32, 7000), b.rpc_timeout_ms);
    try testing.expectEqual(a.gpa.ptr, b.gpa.ptr);
}

test "clients sharing a connection that never connected are closed in any order" {
    const Order = enum { first_last, first_first, chain };
    for (std.enums.values(Order)) |order| {
        var a = try Client.init(testing.allocator, testing.io, .{ .host_port = closed_port, .core_log_level = .warn });
        var b = try a.forNamespace("b");
        switch (order) {
            .first_last => {
                b.close();
                a.close();
            },
            .first_first => {
                a.close();
                // The other client keeps the shared state.
                try b.updateApiKey("k");
                try testing.expectEqualStrings("k", b.shared.pending_connection.?.options.api_key);
                b.close();
            },
            .chain => {
                // A client made from a client made by `forNamespace` shares the same state.
                var c = try b.forNamespace("c");
                try testing.expectEqual(a.shared, c.shared);
                b.close();
                a.close();
                try testing.expectEqualStrings("c", c.namespace);
                c.close();
            },
        }
    }
}

test "updates on a client that hasn't connected apply to the clients sharing its connection" {
    // The failed connections are logged as warnings.
    testing.log_level = .err;
    var a = try Client.init(testing.allocator, testing.io, .{ .host_port = closed_port, .core_log_level = null, .api_key = "first" });
    defer a.close();
    var b = try a.forNamespace("b");
    defer b.close();

    try a.updateApiKey("from-a");
    try testing.expectEqualStrings("from-a", b.shared.pending_connection.?.options.api_key);
    try b.updateApiKey("from-b");
    try testing.expectEqualStrings("from-b", a.shared.pending_connection.?.options.api_key);
    try b.updateHeaders(&.{.{ .key = "x-from", .value = "b" }});
    try testing.expectEqualStrings("b", a.shared.pending_connection.?.options.headers[0].value);
    try a.updateHeaders(&.{.{ .key = "x-from", .value = "a" }});
    try testing.expectEqualStrings("a", b.shared.pending_connection.?.options.headers[0].value);

    // A failed first connection by reconnect leaves both unconnected with their options.
    try testing.expectError(error.ConnectFailed, b.reconnect(.{ .host_port = "127.0.0.1:2" }));
    try testing.expect(a.shared.connection == null);
    try testing.expectEqualStrings("from-b", a.shared.pending_connection.?.options.api_key);
}

test "a client for another namespace uses the connection already open, and a first connection made by either serves both" {
    var server = try startDevServer();
    defer server.stop(testing.io);

    var a = try Client.connect(testing.allocator, testing.io, server.clientOptions());
    defer a.close();
    const before = try a.currentConnection();
    var b = try a.forNamespace(second_namespace);
    defer b.close();
    try testing.expectEqual(before.ptr, (try b.currentConnection()).ptr);
    try testing.expectEqual(before.runtime.ptr, (try b.currentConnection()).runtime.ptr);
    _ = try b.countWorkflows("");
    try testing.expectEqual(@as(usize, 0), b.shared.retired_connections.items.len);

    // A client from `init`: the first call on the other client connects both.
    var lazy = try Client.init(testing.allocator, testing.io, server.clientOptions());
    defer lazy.close();
    var lazy_other = try lazy.forNamespace(second_namespace);
    defer lazy_other.close();
    try lazy_other.ensureConnected();
    try testing.expect(lazy.shared.connection != null);
    try testing.expectEqual((try lazy_other.currentConnection()).ptr, (try lazy.currentConnection()).ptr);

    // Updates on a connected client go to the shared connection, which both keep using.
    try a.updateHeaders(&.{.{ .key = "x-tenant", .value = "blue" }});
    try b.updateApiKey("");
    try b.updateHeaders(&.{});
    try a.updateApiKey("");
    _ = try a.countWorkflows("");
    _ = try b.countWorkflows("");
    try testing.expectEqual(before.ptr, (try b.currentConnection()).ptr);
}

test "reconnect on either client sharing a connection moves both" {
    var server = try startDevServer();
    defer server.stop(testing.io);

    var a = try Client.connect(testing.allocator, testing.io, server.clientOptions());
    defer a.close();
    var b = try a.forNamespace(second_namespace);
    defer b.close();

    const first = try a.currentConnection();
    try b.reconnect(server.clientOptions().connectionOptions());
    const second = try a.currentConnection();
    try testing.expect(second.ptr != first.ptr);
    try testing.expectEqual(second.ptr, (try b.currentConnection()).ptr);

    try a.reconnect(server.clientOptions().connectionOptions());
    const third = try b.currentConnection();
    try testing.expect(third.ptr != second.ptr);
    try testing.expectEqual(third.ptr, (try a.currentConnection()).ptr);
    // The replaced connections stay open, once, until both clients close.
    try testing.expectEqual(@as(usize, 2), a.shared.retired_connections.items.len);
    _ = try a.countWorkflows("");
    _ = try b.countWorkflows("");
}

test "a connected client closed first leaves the connection to the client for another namespace" {
    var server = try startDevServer();
    defer server.stop(testing.io);

    var a = try Client.connect(testing.allocator, testing.io, server.clientOptions());
    var b = try a.forNamespace(second_namespace);
    try a.reconnect(server.clientOptions().connectionOptions());
    a.close();
    _ = try b.countWorkflows("");
    try b.reconnect(server.clientOptions().connectionOptions());
    _ = try b.countWorkflows("");
    b.close();
}

/// Polls `countWorkflows("")` until it reports `want`, for at most 30 s: visibility catches
/// up with starts a moment later.
fn waitForCount(c: *Client, want: u64) !void {
    var tries: usize = 0;
    while (try c.countWorkflows("") != want) : (tries += 1) {
        if (tries == 300) {
            std.log.err("namespace {s} never listed {d} workflows", .{ c.namespace, want });
            return error.TestUnexpectedResult;
        }
        try testing.io.sleep(.fromMilliseconds(100), .awake);
    }
}

/// The IDs of the workflows `c` lists, which must be exactly `want`.
fn expectListed(c: *Client, arena: std.mem.Allocator, want: []const u8) !void {
    var it = try c.listWorkflows("", .{});
    defer it.deinit();
    const info = (try it.next(arena)) orelse return error.TestUnexpectedResult;
    try testing.expectEqualStrings(want, info.workflow_id);
    try testing.expect(try it.next(arena) == null);
}

test "clients for two namespaces of a dev server each start and list their own workflows, and a worker runs in the second" {
    var server = try startDevServer();
    defer server.stop(testing.io);
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var first = try Client.connect(testing.allocator, testing.io, server.clientOptions());
    defer first.close();
    var second = try first.forNamespace(second_namespace);
    defer second.close();

    var w = try worker_mod.Worker.init(testing.allocator, testing.io, &second, "namespaces", .{});
    defer w.deinit();
    try w.registerWorkflow(Fixtures.NamespaceOf);
    var running = try testing.io.concurrent(Fixtures.runWorker, .{&w});
    defer {
        w.stop();
        running.await(testing.io) catch {};
    }
    try w.waitUntilRunning();

    // Nobody polls this task queue in "default": the workflow stays open there.
    var in_first = try first.executeWorkflow(Fixtures.NamespaceOf, .{ .id = "in-first", .task_queue = "namespaces" }, .{});
    defer in_first.deinit();
    var in_second = try second.executeWorkflow(Fixtures.NamespaceOf, .{ .id = "in-second", .task_queue = "namespaces" }, .{});
    defer in_second.deinit();
    try testing.expectEqualStrings(second_namespace, try in_second.get(arena));

    try waitForCount(&first, 1);
    try waitForCount(&second, 1);
    try expectListed(&first, arena, "in-first");
    try expectListed(&second, arena, "in-second");
    try first.terminateWorkflow("in-first", "", "test over");
}

/// Workers on `first` (namespace "default") and `second` (`second_namespace`) on the
/// clients' current connection, until `stop`.
const TwoWorkers = struct {
    first: worker_mod.Worker,
    second: worker_mod.Worker,
    first_running: Io.Future(worker_mod.Worker.RunError!void),
    second_running: Io.Future(worker_mod.Worker.RunError!void),

    fn start(t: *TwoWorkers, first: *Client, second: *Client) !void {
        t.first = try worker_mod.Worker.init(testing.allocator, testing.io, first, "moves-first", .{});
        errdefer t.first.deinit();
        t.second = try worker_mod.Worker.init(testing.allocator, testing.io, second, "moves-second", .{});
        errdefer t.second.deinit();
        try t.first.registerWorkflow(Fixtures.NamespaceOf);
        try t.second.registerWorkflow(Fixtures.NamespaceOf);
        t.first_running = try testing.io.concurrent(Fixtures.runWorker, .{&t.first});
        errdefer {
            t.first.stop();
            t.first_running.await(testing.io) catch {};
        }
        t.second_running = try testing.io.concurrent(Fixtures.runWorker, .{&t.second});
        errdefer {
            t.second.stop();
            t.second_running.await(testing.io) catch {};
        }
        try t.first.waitUntilRunning();
        try t.second.waitUntilRunning();
    }

    fn stop(t: *TwoWorkers) void {
        t.first.stop();
        t.second.stop();
        t.first_running.await(testing.io) catch {};
        t.second_running.await(testing.io) catch {};
        t.first.deinit();
        t.second.deinit();
    }
};

/// Runs a workflow on each client, which completes only if the client's worker polls the
/// server the client is connected to.
fn expectBothRun(first: *Client, second: *Client, arena: std.mem.Allocator, id: []const u8) !void {
    var on_first = try first.executeWorkflow(Fixtures.NamespaceOf, .{ .id = id, .task_queue = "moves-first" }, .{});
    defer on_first.deinit();
    var on_second = try second.executeWorkflow(Fixtures.NamespaceOf, .{ .id = id, .task_queue = "moves-second" }, .{});
    defer on_second.deinit();
    try testing.expectEqualStrings("default", try on_first.get(arena));
    try testing.expectEqualStrings(second_namespace, try on_second.get(arena));
}

test "reconnect on either client sharing a connection moves the workers running on both" {
    // Two servers: a workflow started on the one a reconnect moved to completes only if the
    // workers moved, since they started on the other.
    var one = try startDevServer();
    defer one.stop(testing.io);
    var two = try startDevServer();
    defer two.stop(testing.io);
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var first = try Client.connect(testing.allocator, testing.io, one.clientOptions());
    defer first.close();
    var second = try first.forNamespace(second_namespace);
    defer second.close();

    // Started on server one; the second client's reconnect moves both to server two.
    var workers: TwoWorkers = undefined;
    try workers.start(&first, &second);
    second.reconnect(two.clientOptions().connectionOptions()) catch |err| {
        workers.stop();
        return err;
    };
    expectBothRun(&first, &second, arena, "moved-by-second") catch |err| {
        workers.stop();
        return err;
    };
    workers.stop();

    // Started again, on server two; the first client's reconnect moves both to server one.
    try workers.start(&first, &second);
    defer workers.stop();
    try first.reconnect(one.clientOptions().connectionOptions());
    try expectBothRun(&first, &second, arena, "moved-by-first");
}

/// Counts the core's records (those with a `target` field) that a client's logger receives.
const CoreLogCounter = struct {
    mutex: Io.Mutex = .init,
    /// Broadcast for each record counted.
    changed: Io.Condition = .init,
    count: usize = 0,

    fn sink(r: *CoreLogCounter) log_mod.Sink {
        return .{ .ptr = r, .vtable = &.{ .log = record } };
    }

    fn record(ptr: ?*anyopaque, _: log_mod.Level, _: []const u8, fields: []const log_mod.Field) void {
        const r: *CoreLogCounter = @ptrCast(@alignCast(ptr.?));
        for (fields) |f| if (std.mem.eql(u8, f.key, "target")) {
            r.mutex.lockUncancelable(testing.io);
            defer r.mutex.unlock(testing.io);
            r.count += 1;
            r.changed.broadcast(testing.io);
            return;
        };
    }

    fn current(r: *CoreLogCounter) usize {
        r.mutex.lockUncancelable(testing.io);
        defer r.mutex.unlock(testing.io);
        return r.count;
    }

    /// Waits up to 30 s for more than `seen` core records.
    fn waitPast(r: *CoreLogCounter, seen: usize) !void {
        const deadline = Io.Clock.Timestamp.fromNow(testing.io, .{ .raw = .fromSeconds(30), .clock = .awake });
        r.mutex.lockUncancelable(testing.io);
        defer r.mutex.unlock(testing.io);
        while (r.count <= seen) try r.changed.waitTimeout(testing.io, &r.mutex, .{ .deadline = deadline });
    }
};

/// Runs a workflow on a new worker of `client` for `task_queue`. With `counter`, it then waits,
/// with the worker still polling, for a core record past `seen`. The worker is freed on return.
fn runOnNewWorker(client: *Client, task_queue: []const u8, counter: ?*CoreLogCounter, seen: usize) !void {
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    var w = try worker_mod.Worker.init(testing.allocator, testing.io, client, task_queue, .{});
    defer w.deinit();
    try w.registerWorkflow(Fixtures.NamespaceOf);
    var running = try testing.io.concurrent(Fixtures.runWorker, .{&w});
    defer {
        w.stop();
        running.await(testing.io) catch {};
    }
    try w.waitUntilRunning();
    var run = try client.executeWorkflow(Fixtures.NamespaceOf, .{ .id = task_queue, .task_queue = task_queue }, .{});
    defer run.deinit();
    try testing.expectEqualStrings("default", try run.get(arena_state.allocator()));
    if (counter) |r| try r.waitPast(seen);
}

// Every worker and connection sdk-core builds holds a clone of the client's runtime, and
// sdk-core turns log forwarding off for every clone when any one is dropped.
test "core logs still reach the client's logger after a worker that ran, or a first connection nobody took, is freed" {
    var server = try startDevServer();
    defer server.stop(testing.io);

    {
        var counter: CoreLogCounter = .{};
        var options = server.clientOptions();
        options.logger = counter.sink();
        options.core_log_level = .debug;
        var c = try Client.connect(testing.allocator, testing.io, options);
        defer c.close();
        try runOnNewWorker(&c, "core-logs-freed-worker", null, 0);
        // The first worker is freed: a record after this shows forwarding went on.
        try runOnNewWorker(&c, "core-logs-after-worker", &counter, counter.current());
    }
    {
        var counter: CoreLogCounter = .{};
        var options = server.clientOptions();
        options.logger = counter.sink();
        options.core_log_level = .debug;
        var c = try Client.connect(testing.allocator, testing.io, options);
        defer c.close();
        const url = try std.fmt.allocPrint(testing.allocator, "http://{s}", .{server.server.target});
        defer testing.allocator.free(url);
        const connecting = try bridge.Connection.Connecting.start(testing.allocator, testing.io, c.shared.runtime, .{
            .target_url = url,
            .identity = "core-logs-untaken",
        });
        connecting.waitUncancelable();
        // Connected, and the connection is left for `unref` to free.
        try testing.expect(connecting.conn != null);
        try testing.expectEqual(bridge.Connection.Connecting.Released.freed, connecting.unref());
        try runOnNewWorker(&c, "core-logs-after-connection", &counter, counter.current());
    }
}
