//! Typed calls to any service method (`Client.callService`): the request and response are the
//! method's generated types, a refused call returns the error of its gRPC status with the
//! status in `RpcFailure`, and a view's `RpcOptions` give a call its deadline, retries and
//! metadata. Calls bypass the client's interceptors. Against a dev server: namespaces, the
//! operator, health and cloud services, deadlines and cancellation; against the time-skipping
//! server, its test service.

const std = @import("std");
const client_mod = @import("client.zig");
const interceptor = @import("interceptor.zig");
const bridge = @import("bridge.zig");
const proto = @import("proto.zig");
const testing_mod = @import("testing.zig");
const Client = client_mod.Client;
const GrpcRequest = bridge.Connection.GrpcRequest;

const testing = std.testing;
const wfs = proto.temporal.api.workflowservice.v1;
const errordetails = proto.temporal.api.errordetails.v1;

/// Nothing listens here: the in-process tests serve the client's calls themselves.
const closed_port = "127.0.0.1:1";

/// Serves a connection's calls in Zig: answers GetSystemInfo, and answers every other call
/// with `status_code`, counting the calls and recording the headers the tests check.
const Server = struct {
    status_code: u32 = 5,
    message: []const u8 = "answered by the test",
    mutex: std.Io.Mutex = .init,
    calls: usize = 0,
    /// The headers of the last call, copied; null when it didn't send them.
    values: [names.len]?[]u8 = @splat(null),
    override: bridge.Connection.GrpcOverride = .{ .handle = handle },

    const names = [_][]const u8{ "x-call", "x-shared", "trace-bin" };

    fn deinit(s: *Server) void {
        for (s.values) |v| if (v) |b| testing.allocator.free(b);
    }

    fn handle(ctx: ?*anyopaque, req: GrpcRequest) void {
        const s: *Server = @ptrCast(@alignCast(ctx.?));
        if (std.mem.eql(u8, req.rpc(), "GetSystemInfo")) return req.respond(testing.allocator, .{});
        s.record(req) catch return req.respond(testing.allocator, .{ .status_code = 13, .message = "recording the call failed" });
        req.respond(testing.allocator, .{ .status_code = s.status_code, .message = s.message });
    }

    fn record(s: *Server, req: GrpcRequest) error{OutOfMemory}!void {
        s.mutex.lockUncancelable(testing.io);
        defer s.mutex.unlock(testing.io);
        s.calls += 1;
        for (names, &s.values) |name, *v| {
            if (v.*) |b| testing.allocator.free(b);
            v.* = null;
            if (req.header(name)) |found| v.* = try testing.allocator.dupe(u8, found);
        }
    }

    fn callCount(s: *Server) usize {
        s.mutex.lockUncancelable(testing.io);
        defer s.mutex.unlock(testing.io);
        return s.calls;
    }

    fn value(s: *Server, comptime name: []const u8) ?[]const u8 {
        inline for (names, 0..) |n, i| if (comptime std.mem.eql(u8, n, name)) return s.values[i];
        @compileError("not recorded: " ++ name);
    }
};

/// Gives `c` (from `Client.init`, unconnected) a connection whose calls `server` answers, with
/// a retry policy fast enough for a test.
fn connectToServer(c: *Client, server: *Server) !void {
    server.override.ctx = server;
    const conn = try bridge.Connection.connect(testing.allocator, testing.io, c.shared.runtime, .{
        .target_url = "http://" ++ closed_port,
        .identity = c.identity,
        .grpc_override = &server.override,
        .retry = .{ .initial_interval_ms = 1, .max_interval_ms = 2, .max_retries = 3, .max_elapsed_time_ms = 0 },
    }, null);
    c.shared.connection_mutex.lockUncancelable(testing.io);
    c.shared.connection = conn;
    c.shared.connection_mutex.unlock(testing.io);
    try c.updateHeaders(&.{.{ .key = "x-shared", .value = "client" }});
}

test "a service call does not pass through the client's interceptors; the client's own operations do" {
    const Counter = struct {
        calls: usize = 0,
        pub fn call(self: *@This(), input: *interceptor.ClientCall, next: interceptor.Client.Next) interceptor.ClientError!void {
            self.calls += 1;
            return next.call(input);
        }
    };
    var counter: Counter = .{};
    const interceptors = [_]interceptor.Client{.from(Counter, &counter)};
    var c = try Client.init(testing.allocator, testing.io, .{ .host_port = closed_port, .core_log_level = null, .interceptors = &interceptors });
    defer c.close();
    var server: Server = .{};
    defer server.deinit();
    try connectToServer(&c, &server);
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();

    try testing.expectError(error.NotFound, c.callService(.workflow, "CountWorkflowExecutions", arena_state.allocator(), .{ .namespace = "default" }, null));
    try testing.expectEqual(@as(usize, 1), server.callCount());
    try testing.expectEqual(@as(usize, 0), counter.calls);

    try testing.expectError(error.NotFound, c.countWorkflows(""));
    try testing.expectEqual(@as(usize, 2), server.callCount());
    try testing.expectEqual(@as(usize, 1), counter.calls);
}

test "a service call makes one attempt unless a view sets retry" {
    var c = try Client.init(testing.allocator, testing.io, .{ .host_port = closed_port, .core_log_level = null });
    defer c.close();
    var server: Server = .{ .status_code = 14 };
    defer server.deinit();
    try connectToServer(&c, &server);
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var failure: ?client_mod.RpcFailure = null;
    try testing.expectError(error.Unavailable, c.callService(.workflow, "GetClusterInfo", arena, .{}, &failure));
    try testing.expectEqual(@as(usize, 1), server.callCount());
    try testing.expectEqual(client_mod.RpcCode.unavailable, failure.?.code.?);
    try testing.expectEqualStrings("answered by the test", failure.?.message);

    var retrying = try c.withRpcOptions(.{ .retry = true });
    try testing.expectError(error.Unavailable, retrying.callService(.workflow, "GetClusterInfo", arena, .{}, &failure));
    try testing.expect(server.callCount() > 2);

    // A view's `retry` replaces the choice of the client's own calls too: `checkHealth`
    // makes one attempt, and several through the view.
    var before = server.callCount();
    try testing.expectError(error.RpcFailed, c.checkHealth());
    try testing.expectEqual(before + 1, server.callCount());
    before = server.callCount();
    try testing.expectError(error.RpcFailed, retrying.checkHealth());
    try testing.expect(server.callCount() > before + 1);
}

test "a service call through a view sends the view's metadata with the client's headers" {
    var c = try Client.init(testing.allocator, testing.io, .{ .host_port = closed_port, .core_log_level = null });
    defer c.close();
    var server: Server = .{};
    defer server.deinit();
    try connectToServer(&c, &server);
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    _ = c.callService(.operator, "ListSearchAttributes", arena, .{ .namespace = "default" }, null) catch {};
    try testing.expect(server.value("x-call") == null);
    try testing.expectEqualStrings("client", server.value("x-shared").?);

    var view = try c.withRpcOptions(.{
        .metadata = &.{ .{ .key = "x-call", .value = "one" }, .{ .key = "x-shared", .value = "call" } },
        .binary_metadata = &.{.{ .key = "trace-bin", .value = "\x00\xffz" }},
    });
    try testing.expectError(error.NotFound, view.callService(.operator, "ListSearchAttributes", arena, .{ .namespace = "default" }, null));
    try testing.expectEqualStrings("one", server.value("x-call").?);
    try testing.expectEqualStrings("call", server.value("x-shared").?);
    // The base64 that gRPC sends for "\x00\xffz".
    try testing.expectEqualStrings("AP96", server.value("trace-bin").?);
}

test "every gRPC status returns its error, and the failure carries the status" {
    var c = try Client.init(testing.allocator, testing.io, .{ .host_port = closed_port, .core_log_level = null });
    defer c.close();
    var server: Server = .{};
    defer server.deinit();
    try connectToServer(&c, &server);
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const cases = [_]struct { code: u32, message: []const u8 = "answered by the test", err: client_mod.CallServiceError }{
        .{ .code = 1, .err = error.RpcCanceled },
        // sdk-core's own deadline.
        .{ .code = 1, .message = "Timeout expired", .err = error.DeadlineExceeded },
        .{ .code = 2, .err = error.Unknown },
        .{ .code = 3, .err = error.InvalidArgument },
        .{ .code = 4, .err = error.DeadlineExceeded },
        .{ .code = 5, .err = error.NotFound },
        .{ .code = 6, .err = error.AlreadyExists },
        .{ .code = 7, .err = error.PermissionDenied },
        .{ .code = 8, .err = error.ResourceExhausted },
        .{ .code = 9, .err = error.FailedPrecondition },
        .{ .code = 10, .err = error.Aborted },
        .{ .code = 11, .err = error.OutOfRange },
        .{ .code = 12, .err = error.Unimplemented },
        .{ .code = 13, .err = error.Internal },
        .{ .code = 14, .err = error.Unavailable },
        .{ .code = 15, .err = error.DataLoss },
        .{ .code = 16, .err = error.Unauthenticated },
    };
    for (cases) |case| {
        server.status_code = case.code;
        server.message = case.message;
        var failure: ?client_mod.RpcFailure = null;
        try testing.expectError(case.err, c.callService(.workflow, "GetClusterInfo", arena, .{}, &failure));
        try testing.expectEqual(@as(client_mod.RpcCode, @fromBackingInt(@intCast(case.code))), failure.?.code.?);
        try testing.expectEqualStrings(case.message, failure.?.message);
        // Without `failure`, the call is logged instead, as a warning.
        try testing.expectError(case.err, c.callService(.workflow, "GetClusterInfo", arena, .{}, null));
    }

    // Success leaves `failure` null, clearing what an earlier call left there.
    server.status_code = 0;
    var failure: ?client_mod.RpcFailure = .{ .code = .internal, .message = "", .details = "" };
    _ = try c.callService(.workflow, "GetClusterInfo", arena, .{}, &failure);
    try testing.expect(failure == null);
}

test "a call that fails without a gRPC status returns error.RpcFailed with a null code" {
    // Nothing listens on the port, so the client's first connection fails (logged).
    var c = try Client.init(testing.allocator, testing.io, .{ .host_port = closed_port, .core_log_level = null, .rpc_timeout = .fromSeconds(5) });
    defer c.close();
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    var failure: ?client_mod.RpcFailure = null;
    try testing.expectError(error.RpcFailed, c.callService(.workflow, "GetSystemInfo", arena_state.allocator(), .{}, &failure));
    try testing.expect(failure.?.code == null);
    try testing.expect(failure.?.message.len > 0);
}

/// The generated declaration of each service, as `Client.callService` reads it.
fn ServiceDecl(comptime service: client_mod.Service) type {
    return switch (service) {
        .workflow => proto.temporal.api.workflowservice.v1.WorkflowService(anyopaque, error{}),
        .operator => proto.temporal.api.operatorservice.v1.OperatorService(anyopaque, error{}),
        .cloud => proto.temporal.api.cloud.cloudservice.v1.CloudService(anyopaque, error{}),
        .test_service => proto.temporal.api.testservice.v1.TestService(anyopaque, error{}),
        .health => proto.grpc.health.v1.Health(anyopaque, error{}),
    };
}

test "every method of the generated service declarations is one sdk-core dispatches" {
    var c = try Client.init(testing.allocator, testing.io, .{ .host_port = closed_port, .core_log_level = null });
    defer c.close();
    var server: Server = .{ .status_code = 0 };
    defer server.deinit();
    try connectToServer(&c, &server);
    const conn = c.shared.connection.?;
    var checked: usize = 0;
    inline for (.{ .workflow, .operator, .cloud, .test_service, .health }) |service| {
        const Decl = ServiceDecl(service);
        inline for (@typeInfo(Decl).@"struct".field_names) |name| {
            if (comptime service == .health and std.mem.eql(u8, name, "Watch")) continue;
            var failure: ?bridge.Connection.RpcFailure = null;
            defer if (failure) |f| f.deinit(testing.allocator);
            if (conn.call(testing.allocator, testing.io, service, name, "", .{}, &failure)) |bytes| {
                testing.allocator.free(bytes);
            } else |err| {
                std.debug.print("{t} {s}: {t} {s}\n", .{ service, name, err, if (failure) |f| f.message else "" });
                return err;
            }
            checked += 1;
        }
    }
    try testing.expect(checked > 200);
}

fn startDevServer() !testing_mod.DevServer {
    return testing_mod.DevServer.start(testing.allocator, testing.io, .{});
}

test "namespaces are registered, described, updated and listed through service calls" {
    var server = try startDevServer();
    defer server.stop(testing.io);
    var c = try Client.connect(testing.allocator, testing.io, server.clientOptions());
    defer c.close();
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const name = "zig-sdk-service-calls";

    _ = try c.callService(.workflow, "RegisterNamespace", arena, .{
        .namespace = name,
        .description = "registered by a test",
        .workflow_execution_retention_period = .{ .seconds = 24 * 60 * 60 },
    }, null);
    var failure: ?client_mod.RpcFailure = null;
    try testing.expectError(error.AlreadyExists, c.callService(.workflow, "RegisterNamespace", arena, .{
        .namespace = name,
        .workflow_execution_retention_period = .{ .seconds = 24 * 60 * 60 },
    }, &failure));
    try testing.expectEqual(client_mod.RpcCode.already_exists, failure.?.code.?);

    const described = try c.callService(.workflow, "DescribeNamespace", arena, .{ .namespace = name }, &failure);
    try testing.expect(failure == null);
    try testing.expectEqualStrings(name, described.namespace_info.?.name);
    try testing.expectEqualStrings("registered by a test", described.namespace_info.?.description);

    const updated = try c.callService(.workflow, "UpdateNamespace", arena, .{
        .namespace = name,
        .update_info = .{ .description = "updated by a test" },
    }, null);
    try testing.expectEqualStrings("updated by a test", updated.namespace_info.?.description);
    const again = try c.callService(.workflow, "DescribeNamespace", arena, .{ .namespace = name }, null);
    try testing.expectEqualStrings("updated by a test", again.namespace_info.?.description);

    var found = false;
    var token: []const u8 = "";
    while (true) {
        const page = try c.callService(.workflow, "ListNamespaces", arena, .{ .page_size = 2, .next_page_token = token }, null);
        for (page.namespaces.items) |ns| {
            if (std.mem.eql(u8, ns.namespace_info.?.name, name)) found = true;
        }
        if (page.next_page_token.len == 0) break;
        token = page.next_page_token;
    }
    try testing.expect(found);

    try testing.expectError(error.NotFound, c.callService(.workflow, "DescribeNamespace", arena, .{ .namespace = "zig-sdk-no-such-namespace" }, &failure));
    const f = failure.?;
    try testing.expectEqual(client_mod.RpcCode.not_found, f.code.?);
    try testing.expect(f.message.len > 0);
    const detail = (try f.detail(errordetails.NamespaceNotFoundFailure, arena)).?;
    try testing.expectEqualStrings("zig-sdk-no-such-namespace", detail.namespace);
    try testing.expect(try f.detail(errordetails.NamespaceInvalidStateFailure, arena) == null);
}

test "operator, health and cloud service calls, deadlines and cancellation against a dev server" {
    var server = try startDevServer();
    defer server.stop(testing.io);
    var c = try Client.connect(testing.allocator, testing.io, server.clientOptions());
    defer c.close();
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const attributes = try c.callService(.operator, "ListSearchAttributes", arena, .{ .namespace = "default" }, null);
    try testing.expect(attributes.system_attributes.items.len > 0);

    const health = try c.callService(.health, "Check", arena, .{ .service = "temporal.api.workflowservice.v1.WorkflowService" }, null);
    try testing.expectEqual(proto.grpc.health.v1.HealthCheckResponse.ServingStatus.SERVING, health.status);

    // The dev server has no cloud service.
    var failure: ?client_mod.RpcFailure = null;
    try testing.expectError(error.Unimplemented, c.callService(.cloud, "GetNamespaces", arena, .{}, &failure));
    try testing.expectEqual(client_mod.RpcCode.unimplemented, failure.?.code.?);

    // No worker polls the workflow's task queue, so the server holds a query until the
    // call's deadline.
    _ = try c.callService(.workflow, "StartWorkflowExecution", arena, .{
        .namespace = "default",
        .workflow_id = "zig-sdk-service-calls-unpolled",
        .workflow_type = .{ .name = "Unpolled" },
        .task_queue = .{ .name = "zig-sdk-service-calls-nobody-polls", .kind = .TASK_QUEUE_KIND_NORMAL },
        .request_id = "zig-sdk-service-calls-start",
    }, null);
    var short = try c.withRpcOptions(.{ .timeout = .fromMilliseconds(500) });
    try testing.expectError(error.DeadlineExceeded, short.callService(.workflow, "QueryWorkflow", arena, .{
        .namespace = "default",
        .execution = .{ .workflow_id = "zig-sdk-service-calls-unpolled" },
        .query = .{ .query_type = "status" },
    }, &failure));
    // sdk-core's deadline or the server's, whichever ends the call first.
    try testing.expect(failure.?.code == .cancelled or failure.?.code == .deadline_exceeded);

    // A poll for a task that never comes waits about a minute; canceling the task ends it.
    const poll: wfs.PollWorkflowTaskQueueRequest = .{
        .namespace = "default",
        .task_queue = .{ .name = "zig-sdk-service-calls-no-workflows", .kind = .TASK_QUEUE_KIND_NORMAL },
        .identity = "test@zig",
    };

    const Poll = struct {
        fn run(client: *Client, request: wfs.PollWorkflowTaskQueueRequest) client_mod.CallServiceError!void {
            var a: std.heap.ArenaAllocator = .init(testing.allocator);
            defer a.deinit();
            _ = try client.callService(.workflow, "PollWorkflowTaskQueue", a.allocator(), request, null);
        }
    };
    var task = try testing.io.concurrent(Poll.run, .{ &c, poll });
    // Cancel only once the server lists the poll, so the call is known to be in flight.
    var tries: usize = 0;
    polling: while (true) : (tries += 1) {
        const described = try c.callService(.workflow, "DescribeTaskQueue", arena, .{
            .namespace = "default",
            .task_queue = poll.task_queue,
            .task_queue_type = .TASK_QUEUE_TYPE_WORKFLOW,
        }, null);
        for (described.pollers.items) |poller| {
            if (std.mem.eql(u8, poller.identity, poll.identity)) break :polling;
        }
        if (tries == 200) {
            _ = task.cancel(testing.io) catch {};
            return error.TestUnexpectedResult;
        }
        try testing.io.sleep(.fromMilliseconds(50), .awake);
    }
    const start = std.Io.Timestamp.now(testing.io, .awake);
    try testing.expectError(error.Canceled, task.cancel(testing.io));
    try testing.expect(start.durationTo(std.Io.Timestamp.now(testing.io, .awake)).nanoseconds < 5 * std.time.ns_per_s);
}

test "a test service call against the time-skipping server" {
    var server = try testing_mod.TestServer.start(testing.allocator, testing.io, .{});
    defer server.stop(testing.io);
    var c = try Client.connect(testing.allocator, testing.io, server.clientOptions());
    defer c.close();
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();

    const now = try c.callService(.test_service, "GetCurrentTime", arena_state.allocator(), .{}, null);
    try testing.expect(now.time.?.seconds > 0);
}

test "a request string field that is not valid UTF-8 returns InvalidArgument, naming the field, without a call" {
    var c = try Client.init(testing.allocator, testing.io, .{ .host_port = closed_port, .core_log_level = null });
    defer c.close();
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const describe: wfs.DescribeWorkflowExecutionRequest = .{ .namespace = "default", .execution = .{ .workflow_id = "bad\xff" } };
    var failure: ?client_mod.RpcFailure = null;

    // Unconnected, the client doesn't connect for it: nothing listens on the port.
    try testing.expectError(error.InvalidArgument, c.callService(.workflow, "DescribeWorkflowExecution", arena, describe, &failure));
    try testing.expect(c.shared.connection == null);

    var server: Server = .{};
    defer server.deinit();
    try connectToServer(&c, &server);

    failure = null;
    try testing.expectError(error.InvalidArgument, c.callService(.workflow, "DescribeWorkflowExecution", arena, describe, &failure));
    try testing.expectEqual(client_mod.RpcCode.invalid_argument, failure.?.code.?);
    try testing.expectEqualStrings("the request's field execution.workflow_id is not valid UTF-8; no call was made", failure.?.message);
    try testing.expectEqualStrings("", failure.?.details);
    // Without `failure`, the same message is logged as a warning.
    try testing.expectError(error.InvalidArgument, c.callService(.workflow, "DescribeWorkflowExecution", arena, describe, null));

    // A typed call fails as for a server's INVALID_ARGUMENT, also without a call.
    try testing.expectError(error.RpcFailed, c.signalWorkflowByName("go", "bad\xff", "", .{}));
    try testing.expectEqual(@as(usize, 0), server.callCount());

    // Valid UTF-8 beyond ASCII goes through.
    try testing.expectError(error.NotFound, c.callService(.workflow, "DescribeWorkflowExecution", arena, .{ .namespace = "default", .execution = .{ .workflow_id = "wf-\xc3\xa9" } }, &failure));
    try testing.expectEqual(@as(usize, 1), server.callCount());
    try testing.expectError(error.NotFound, c.signalWorkflowByName("go", "wf-\xc3\xa9", "", .{}));
    try testing.expectEqual(@as(usize, 2), server.callCount());
}

test "the UTF-8 check names repeated elements, map keys and values, and oneof members" {
    var c = try Client.init(testing.allocator, testing.io, .{ .host_port = closed_port, .core_log_level = null });
    defer c.close();
    var server: Server = .{};
    defer server.deinit();
    try connectToServer(&c, &server);
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    var failure: ?client_mod.RpcFailure = null;

    // A oneof member inside a repeated field, by the member's name.
    var multi: wfs.ExecuteMultiOperationRequest = .{ .namespace = "default" };
    try multi.operations.append(arena, .{ .operation = .{ .start_workflow = .{ .workflow_id = "ok" } } });
    try multi.operations.append(arena, .{ .operation = .{ .update_workflow = .{ .request = .{ .meta = .{ .update_id = "u\xc0" } } } } });
    try testing.expectError(error.InvalidArgument, c.callService(.workflow, "ExecuteMultiOperation", arena, multi, &failure));
    try testing.expectEqualStrings("the request's field operations[1].update_workflow.request.meta.update_id is not valid UTF-8; no call was made", failure.?.message);

    // A repeated string.
    var options: wfs.UpdateWorkflowExecutionOptionsRequest = .{ .namespace = "default", .update_mask = .{} };
    try options.update_mask.?.paths.append(arena, "versioning_override");
    try options.update_mask.?.paths.append(arena, "\xed\xa0\x80");
    try testing.expectError(error.InvalidArgument, c.callService(.workflow, "UpdateWorkflowExecutionOptions", arena, options, &failure));
    try testing.expectEqualStrings("the request's field update_mask.paths[1] is not valid UTF-8; no call was made", failure.?.message);

    // A map value, by its key, and a map key.
    var register: wfs.RegisterNamespaceRequest = .{ .namespace = "n" };
    try register.data.append(arena, .{ .key = "owner", .value = "\xff" });
    try testing.expectError(error.InvalidArgument, c.callService(.workflow, "RegisterNamespace", arena, register, &failure));
    try testing.expectEqualStrings("the request's field data[\"owner\"] is not valid UTF-8; no call was made", failure.?.message);
    register.data.items[0] = .{ .key = "own\xfe", .value = "x" };
    try testing.expectError(error.InvalidArgument, c.callService(.workflow, "RegisterNamespace", arena, register, &failure));
    try testing.expectEqualStrings("the request's field data (a key) is not valid UTF-8; no call was made", failure.?.message);

    // A path longer than 200 bytes is cut, on a code point boundary.
    const key = try arena.alloc(u8, 300);
    for (0..150) |i| @memcpy(key[2 * i ..][0..2], "\xc3\xa9");
    register.data.items[0] = .{ .key = key, .value = "\xff" };
    try testing.expectError(error.InvalidArgument, c.callService(.workflow, "RegisterNamespace", arena, register, &failure));
    const message = failure.?.message;
    const path = message["the request's field ".len..std.mem.indexOf(u8, message, " is not valid").?];
    try testing.expect(path.len <= 200);
    try testing.expect(std.mem.startsWith(u8, path, "data[\"\xc3\xa9"));
    try testing.expect(std.mem.endsWith(u8, path, "\xc3\xa9..."));
    try testing.expect(std.unicode.utf8ValidateSlice(message));

    // Bytes fields may hold any bytes.
    try testing.expectError(error.NotFound, c.callService(.workflow, "RespondActivityTaskCompleted", arena, .{ .namespace = "default", .task_token = "\xff\x00" }, &failure));
    try testing.expectEqual(@as(usize, 1), server.callCount());
}
