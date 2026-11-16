//! Per-call RPC options (`Client.withRpcOptions`): every typed call made through a view sends
//! the view's deadline and metadata, merged with the client's headers and API key with the
//! view's value winning, while calls on the client itself keep the client's.

const std = @import("std");
const client_mod = @import("client.zig");
const workflow = @import("workflow.zig");
const bridge = @import("bridge.zig");
const Client = client_mod.Client;
const GrpcRequest = bridge.Connection.GrpcRequest;

const testing = std.testing;

/// Nothing listens here: the test serves the client's calls itself.
const closed_port = "127.0.0.1:1";

/// Serves a connection's calls in Zig: answers GetSystemInfo, and records the headers of every
/// other call before failing it with NOT_FOUND, which the client returns without logging.
const Recorder = struct {
    mutex: std.Io.Mutex = .init,
    calls: std.ArrayList(Call) = .empty,
    override: bridge.Connection.GrpcOverride = .{ .handle = handle },

    const Call = struct {
        rpc: []const u8,
        /// The headers the test checks, copied; null when the call didn't send it.
        values: [names.len]?[]const u8,
    };

    const names = [_][]const u8{ "x-tenant", "x-shared", "x-call", "trace-bin", "authorization", "grpc-timeout" };

    fn deinit(r: *Recorder) void {
        for (r.calls.items) |call| {
            testing.allocator.free(call.rpc);
            for (call.values) |v| if (v) |s| testing.allocator.free(s);
        }
        r.calls.deinit(testing.allocator);
    }

    fn handle(ctx: ?*anyopaque, req: GrpcRequest) void {
        const r: *Recorder = @ptrCast(@alignCast(ctx.?));
        if (std.mem.eql(u8, req.rpc(), "GetSystemInfo")) return req.respond(testing.allocator, .{});
        r.record(req) catch return req.respond(testing.allocator, .{ .status_code = 13, .message = "recording the call failed" });
        req.respond(testing.allocator, .{ .status_code = 5, .message = "recorded" });
    }

    fn record(r: *Recorder, req: GrpcRequest) error{OutOfMemory}!void {
        var call: Call = .{ .rpc = try testing.allocator.dupe(u8, req.rpc()), .values = @splat(null) };
        for (names, &call.values) |name, *v| {
            if (req.header(name)) |found| v.* = try testing.allocator.dupe(u8, found);
        }
        r.mutex.lockUncancelable(testing.io);
        defer r.mutex.unlock(testing.io);
        try r.calls.append(testing.allocator, call);
    }

    /// The calls recorded since the last `take`, which the caller frees with `free`.
    fn take(r: *Recorder) []Call {
        r.mutex.lockUncancelable(testing.io);
        defer r.mutex.unlock(testing.io);
        return r.calls.toOwnedSlice(testing.allocator) catch @panic("out of memory");
    }

    fn free(calls: []Call) void {
        for (calls) |call| {
            testing.allocator.free(call.rpc);
            for (call.values) |v| if (v) |s| testing.allocator.free(s);
        }
        testing.allocator.free(calls);
    }

    fn value(call: Call, comptime name: []const u8) ?[]const u8 {
        inline for (names, 0..) |n, i| if (comptime std.mem.eql(u8, n, name)) return call.values[i];
        @compileError("not recorded: " ++ name);
    }
};

/// Gives `c` (from `Client.init`, unconnected) a connection whose calls `recorder` serves,
/// then sets its headers and API key the way a program does, through the client.
fn connectToRecorder(c: *Client, recorder: *Recorder) !void {
    recorder.override.ctx = recorder;
    const conn = try bridge.Connection.connect(testing.allocator, testing.io, c.shared.runtime, .{
        .target_url = "http://" ++ closed_port,
        .identity = c.identity,
        .grpc_override = &recorder.override,
    }, null);
    c.shared.connection_mutex.lockUncancelable(testing.io);
    c.shared.connection = conn;
    c.shared.connection_mutex.unlock(testing.io);
    try c.updateHeaders(&.{ .{ .key = "x-tenant", .value = "client" }, .{ .key = "x-shared", .value = "client" } });
    try c.updateApiKey("secret");
}

const Fixtures = struct {
    const Ping = workflow.SignalDef("ping", fn () void);
    const SetValue = workflow.UpdateDef("set_value", fn () void);
};

/// Makes every kind of typed call that reaches the server through `c`, each answered
/// NOT_FOUND by the recorder.
fn callEverything(c: *Client, arena: std.mem.Allocator) !void {
    const NotFound = error.NotFound;
    try testing.expectError(NotFound, c.executeWorkflowByName(void, "anything", .{ .id = "wf", .task_queue = "tq" }, .{}));
    try testing.expectError(NotFound, c.signalWorkflow(Fixtures.Ping, "wf", "", .{}));
    try testing.expectError(NotFound, c.queryWorkflowByName("status", arena, "wf", "", .{}));
    try testing.expectError(NotFound, c.updateWorkflow(Fixtures.SetValue, "wf", "", .{}, .{}));
    try testing.expectError(NotFound, c.describeWorkflow(arena, "wf", ""));
    var workflows = try c.listWorkflows("", .{});
    defer workflows.deinit();
    try testing.expectError(NotFound, workflows.next(arena));
    try testing.expectError(NotFound, c.countWorkflows(""));
    try testing.expectError(NotFound, c.cancelWorkflow("wf", "", "test"));
    try testing.expectError(NotFound, c.terminateWorkflow("wf", "", "test"));
    var schedule = try c.getScheduleHandle("sched");
    defer schedule.deinit();
    try testing.expectError(NotFound, schedule.describe(arena));
    var schedules = try c.listSchedules(.{});
    defer schedules.deinit();
    try testing.expectError(NotFound, schedules.next(arena));
    try testing.expectError(NotFound, c.completeActivity("token", .{}));
    try testing.expectError(NotFound, c.completeActivityById(.{ .workflow_id = "wf", .activity_id = "act" }, .{}));
    // A long-poll: waiting for the workflow's result.
    var run = try c.getWorkflow(void, "wf", "");
    defer run.deinit();
    try testing.expectError(NotFound, run.get(arena));
}

test "every typed call through a view sends the view's timeout and metadata, merged with the client's headers" {
    var c = try Client.init(testing.allocator, testing.io, .{ .host_port = closed_port, .core_log_level = null, .rpc_timeout = .fromSeconds(60) });
    defer c.close();
    var recorder: Recorder = .{};
    defer recorder.deinit();
    try connectToRecorder(&c, &recorder);
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var view = try c.withRpcOptions(.{
        .timeout = .fromSeconds(4),
        .metadata = &.{ .{ .key = "x-shared", .value = "call" }, .{ .key = "x-call", .value = "one" } },
        .binary_metadata = &.{.{ .key = "trace-bin", .value = "\x00\xffz" }},
    });
    defer view.close();
    try callEverything(&view, arena);

    const calls = recorder.take();
    defer Recorder.free(calls);
    const expected = [_][]const u8{
        "StartWorkflowExecution",           "SignalWorkflowExecution",     "QueryWorkflow",           "UpdateWorkflowExecution",
        "DescribeWorkflowExecution",        "ListWorkflowExecutions",      "CountWorkflowExecutions", "RequestCancelWorkflowExecution",
        "TerminateWorkflowExecution",       "DescribeSchedule",            "ListSchedules",           "RespondActivityTaskCompleted",
        "RespondActivityTaskCompletedById", "GetWorkflowExecutionHistory",
    };
    try testing.expectEqual(expected.len, calls.len);
    for (calls, expected) |call, rpc| {
        errdefer std.debug.print("call {s}\n", .{call.rpc});
        try testing.expectEqualStrings(rpc, call.rpc);
        // The view's value wins for a key in both; the client's other headers and API key
        // are still sent.
        try testing.expectEqualStrings("call", Recorder.value(call, "x-shared").?);
        try testing.expectEqualStrings("one", Recorder.value(call, "x-call").?);
        try testing.expectEqualStrings("client", Recorder.value(call, "x-tenant").?);
        try testing.expectEqualStrings("Bearer secret", Recorder.value(call, "authorization").?);
        // Binary metadata goes out base64-encoded.
        try testing.expectEqualStrings("AP96", Recorder.value(call, "trace-bin").?);
        const timeout = Recorder.value(call, "grpc-timeout").?;
        const long_polls = std.mem.eql(u8, rpc, "UpdateWorkflowExecution") or std.mem.eql(u8, rpc, "GetWorkflowExecutionHistory");
        if (long_polls) {
            // Long-polls aren't limited by the view's timeout.
            try testing.expect(!std.mem.eql(u8, timeout, "4000000u"));
        } else try testing.expectEqualStrings("4000000u", timeout);
    }

    // The client's own calls keep its headers and rpc_timeout.
    try testing.expectError(error.NotFound, c.countWorkflows(""));
    const own = recorder.take();
    defer Recorder.free(own);
    try testing.expectEqual(@as(usize, 1), own.len);
    try testing.expectEqualStrings("client", Recorder.value(own[0], "x-shared").?);
    try testing.expectEqual(@as(?[]const u8, null), Recorder.value(own[0], "x-call"));
    try testing.expectEqual(@as(?[]const u8, null), Recorder.value(own[0], "trace-bin"));
    try testing.expectEqualStrings("60000000u", Recorder.value(own[0], "grpc-timeout").?);

    // An authorization entry replaces the API key's; a view without a timeout keeps the
    // client's.
    var auth = try c.withRpcOptions(.{ .metadata = &.{.{ .key = "authorization", .value = "Bearer per-call" }} });
    try testing.expectError(error.NotFound, auth.countWorkflows(""));
    const authorized = recorder.take();
    defer Recorder.free(authorized);
    try testing.expectEqualStrings("Bearer per-call", Recorder.value(authorized[0], "authorization").?);
    try testing.expectEqualStrings("60000000u", Recorder.value(authorized[0], "grpc-timeout").?);
}

test "a view of a client for another namespace sends its options in that namespace" {
    var c = try Client.init(testing.allocator, testing.io, .{ .host_port = closed_port, .core_log_level = null });
    defer c.close();
    var recorder: Recorder = .{};
    defer recorder.deinit();
    try connectToRecorder(&c, &recorder);
    var other = try c.forNamespace("other");
    defer other.close();
    var view = try other.withRpcOptions(.{ .metadata = &.{.{ .key = "x-call", .value = "two" }} });
    try testing.expectEqualStrings("other", view.namespace);
    try testing.expectError(error.NotFound, view.countWorkflows(""));
    // A client made from a view is a full client without the view's options.
    var from_view = try view.forNamespace("third");
    defer from_view.close();
    try testing.expectError(error.NotFound, from_view.countWorkflows(""));
    const calls = recorder.take();
    defer Recorder.free(calls);
    try testing.expectEqual(@as(usize, 2), calls.len);
    try testing.expectEqualStrings("two", Recorder.value(calls[0], "x-call").?);
    try testing.expectEqual(@as(?[]const u8, null), Recorder.value(calls[1], "x-call"));
}
