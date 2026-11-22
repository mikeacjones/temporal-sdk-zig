//! The client's typed operations against answers served in process: a connection whose calls
//! an RPC override answers with preset responses, so a test can give the client a response a
//! dev server won't send. Covers history pages sent as raw history (`fetchHistory`,
//! `historyIterator`, `WorkflowRun.get`) and a schedule description that outlives its
//! handle (`ScheduleHandle.describe`).

const std = @import("std");
const client_mod = @import("client.zig");
const bridge = @import("bridge.zig");
const proto = @import("proto.zig");
const converter = @import("converter.zig");
const protojson = @import("protojson.zig");
const schedule = @import("schedule.zig");
const Client = client_mod.Client;
const GrpcRequest = bridge.Connection.GrpcRequest;

const testing = std.testing;
const Allocator = std.mem.Allocator;
const wfs = proto.temporal.api.workflowservice.v1;
const history_v1 = proto.temporal.api.history.v1;
const common = proto.temporal.api.common.v1;
const enums = proto.temporal.api.enums.v1;

/// Nothing listens here: the server below answers the client's calls itself.
const closed_port = "127.0.0.1:1";

/// One call's preset answer: the method it answers and its encoded response.
const Answer = struct {
    rpc: []const u8,
    proto: []const u8,
};

/// Serves a connection's calls in Zig: answers GetSystemInfo, and every other call with the
/// next of `answers`, in order. A call for another method than the next answer names, or one
/// after the last answer, fails with FAILED_PRECONDITION. Counts the calls it answers.
const Server = struct {
    answers: []const Answer,
    mutex: std.Io.Mutex = .init,
    calls: usize = 0,
    override: bridge.Connection.GrpcOverride = .{ .handle = handle },

    fn handle(ctx: ?*anyopaque, req: GrpcRequest) void {
        const s: *Server = @ptrCast(@alignCast(ctx.?));
        if (std.mem.eql(u8, req.rpc(), "GetSystemInfo")) return req.respond(testing.allocator, .{});
        s.mutex.lockUncancelable(testing.io);
        const index = s.calls;
        s.calls += 1;
        s.mutex.unlock(testing.io);
        if (index >= s.answers.len) return req.respond(testing.allocator, .{ .status_code = 9, .message = "the test has no answer left" });
        const answer = s.answers[index];
        if (!std.mem.eql(u8, req.rpc(), answer.rpc)) return req.respond(testing.allocator, .{ .status_code = 9, .message = "the test expected another method" });
        req.respond(testing.allocator, .{ .proto = answer.proto });
    }

    fn callCount(s: *Server) usize {
        s.mutex.lockUncancelable(testing.io);
        defer s.mutex.unlock(testing.io);
        return s.calls;
    }
};

/// Gives `c` (from `Client.init`, unconnected) a connection whose calls `server` answers.
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
}

fn initClient() !Client {
    return Client.init(testing.allocator, testing.io, .{ .host_port = closed_port, .core_log_level = null });
}

fn encodeMessage(arena: Allocator, message: anytype) ![]const u8 {
    var out: std.Io.Writer.Allocating = .init(arena);
    try message.encode(&out.writer, arena);
    return out.written();
}

// ---------------------------------------------------------------- raw history

const history_rpc = "GetWorkflowExecutionHistory";

/// A closed run's history: started, three workflow-task events, and completed with the
/// result "done".
fn closedRunEvents(arena: Allocator) ![]const history_v1.HistoryEvent {
    var result: common.Payloads = .{};
    try result.payloads.append(arena, try converter.toPayload(arena, @as([]const u8, "done")));
    const events = try arena.alloc(history_v1.HistoryEvent, 5);
    events[0] = .{ .event_id = 1, .event_type = .EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, .attributes = .{ .workflow_execution_started_event_attributes = .{
        .workflow_type = .{ .name = "RawHistoryWorkflow" },
        .task_queue = .{ .name = "raw-history" },
    } } };
    events[1] = .{ .event_id = 2, .event_type = .EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, .attributes = .{ .workflow_task_scheduled_event_attributes = .{ .attempt = 1 } } };
    events[2] = .{ .event_id = 3, .event_type = .EVENT_TYPE_WORKFLOW_TASK_STARTED, .attributes = .{ .workflow_task_started_event_attributes = .{ .scheduled_event_id = 2 } } };
    events[3] = .{ .event_id = 4, .event_type = .EVENT_TYPE_WORKFLOW_TASK_COMPLETED, .attributes = .{ .workflow_task_completed_event_attributes = .{ .scheduled_event_id = 2, .started_event_id = 3 } } };
    events[4] = .{ .event_id = 5, .event_type = .EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, .attributes = .{ .workflow_execution_completed_event_attributes = .{
        .result = result,
        .workflow_task_completed_event_id = 4,
    } } };
    return events;
}

fn historyOf(arena: Allocator, events: []const history_v1.HistoryEvent) !history_v1.History {
    var h: history_v1.History = .{};
    try h.events.appendSlice(arena, events);
    return h;
}

const Blob = struct {
    encoding: enums.EncodingType = .ENCODING_TYPE_PROTO3,
    events: []const history_v1.HistoryEvent,
};

/// A history page carrying `events` in `history`.
fn historyPage(arena: Allocator, events: []const history_v1.HistoryEvent, token: []const u8) ![]const u8 {
    return encodeMessage(arena, wfs.GetWorkflowExecutionHistoryResponse{ .history = try historyOf(arena, events), .next_page_token = token });
}

/// A history page carrying `blobs` in `raw_history`, each a `History` in its encoding.
fn rawPage(arena: Allocator, blobs: []const Blob, token: []const u8) ![]const u8 {
    var response: wfs.GetWorkflowExecutionHistoryResponse = .{ .next_page_token = token };
    for (blobs) |b| {
        const h = try historyOf(arena, b.events);
        const data = switch (b.encoding) {
            .ENCODING_TYPE_JSON => try protojson.encode(arena, h),
            else => try encodeMessage(arena, h),
        };
        try response.raw_history.append(arena, .{ .encoding_type = b.encoding, .data = data });
    }
    return encodeMessage(arena, response);
}

/// The two pages of the closed run's history, as `history` or as `raw_history`. The raw
/// form's first page holds three PROTO3 blobs; its second, one JSON blob.
fn twoPages(arena: Allocator, raw: bool) ![2]Answer {
    const events = try closedRunEvents(arena);
    if (!raw) return .{
        .{ .rpc = history_rpc, .proto = try historyPage(arena, events[0..3], "page-2") },
        .{ .rpc = history_rpc, .proto = try historyPage(arena, events[3..], "") },
    };
    return .{
        .{ .rpc = history_rpc, .proto = try rawPage(arena, &.{ .{ .events = events[0..1] }, .{ .events = events[1..2] }, .{ .events = events[2..3] } }, "page-2") },
        .{ .rpc = history_rpc, .proto = try rawPage(arena, &.{.{ .encoding = .ENCODING_TYPE_JSON, .events = events[3..] }}, "") },
    };
}

fn expectSameEvents(arena: Allocator, expected: []const history_v1.HistoryEvent, actual: []const history_v1.HistoryEvent) !void {
    try testing.expectEqual(expected.len, actual.len);
    for (expected, actual) |e, a| try testing.expectEqualSlices(u8, try encodeMessage(arena, e), try encodeMessage(arena, a));
}

/// Fetches the closed run's history from `answers` with `fetchHistory` and with the iterator.
fn fetchBothWays(arena: Allocator, answers: []const Answer) !struct { fetched: []const history_v1.HistoryEvent, iterated: []const history_v1.HistoryEvent } {
    var c = try initClient();
    defer c.close();
    // Each path reads every answer once.
    const doubled = try std.mem.concat(arena, Answer, &.{ answers, answers });
    var server: Server = .{ .answers = doubled };
    try connectToServer(&c, &server);

    const fetched = try c.fetchHistory(arena, "wf-raw", "");
    try testing.expectEqual(answers.len, server.callCount());

    var it = try c.historyIterator("wf-raw", "", .{});
    defer it.deinit();
    var iterated: std.ArrayList(history_v1.HistoryEvent) = .empty;
    while (try it.next(arena)) |event| try iterated.append(arena, event);
    try testing.expectEqual(doubled.len, server.callCount());
    return .{ .fetched = fetched.events.items, .iterated = iterated.items };
}

test "fetchHistory and the history iterator read raw history pages of several PROTO3 blobs and a JSON blob as the same events as history" {
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const events = try closedRunEvents(arena);

    const plain = try fetchBothWays(arena, &try twoPages(arena, false));
    try expectSameEvents(arena, events, plain.fetched);
    try expectSameEvents(arena, events, plain.iterated);

    const raw = try fetchBothWays(arena, &try twoPages(arena, true));
    try expectSameEvents(arena, events, raw.fetched);
    try expectSameEvents(arena, events, raw.iterated);
}

test "the history iterator with the close-event filter keeps only the last event of raw history" {
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const events = try closedRunEvents(arena);
    const answers = [_]Answer{.{ .rpc = history_rpc, .proto = try rawPage(arena, &.{ .{ .events = events[0..2] }, .{ .events = events[2..] } }, "") }};
    var c = try initClient();
    defer c.close();
    var server: Server = .{ .answers = &answers };
    try connectToServer(&c, &server);

    var it = try c.historyIterator("wf-raw", "", .{ .filter = .close_event });
    defer it.deinit();
    const last = (try it.next(arena)).?;
    try expectSameEvents(arena, events[4..], &.{last});
    try testing.expect((try it.next(arena)) == null);
}

test "WorkflowRun.get returns the same result from raw history as from history" {
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const events = try closedRunEvents(arena);
    // The server sends only the close event for the close-event filter `get` asks with; the
    // raw form here sends the whole history in two blobs, and only the last event counts.
    const pages = [_][]const u8{
        try historyPage(arena, events[4..], ""),
        try rawPage(arena, &.{.{ .events = events[4..] }}, ""),
        try rawPage(arena, &.{ .{ .events = events[0..3] }, .{ .events = events[3..] } }, ""),
        try rawPage(arena, &.{.{ .encoding = .ENCODING_TYPE_JSON, .events = events }}, ""),
    };
    for (pages) |page| {
        var c = try initClient();
        defer c.close();
        const answers = [_]Answer{.{ .rpc = history_rpc, .proto = page }};
        var server: Server = .{ .answers = &answers };
        try connectToServer(&c, &server);
        var run = try c.getWorkflow([]const u8, "wf-raw", "run-1");
        defer run.deinit();
        try testing.expectEqualStrings("done", try run.get(arena));
        try testing.expectEqual(@as(usize, 1), server.callCount());
    }
}

test "raw history the client can't read is error.InvalidResponse from fetchHistory, the iterator and WorkflowRun.get" {
    testing.log_level = .err; // Each failure is logged as a warning.
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const events = try closedRunEvents(arena);
    var bad_proto: wfs.GetWorkflowExecutionHistoryResponse = .{};
    try bad_proto.raw_history.append(arena, .{ .encoding_type = .ENCODING_TYPE_PROTO3, .data = "\xff\xff\xff" });
    var bad_json: wfs.GetWorkflowExecutionHistoryResponse = .{};
    try bad_json.raw_history.append(arena, .{ .encoding_type = .ENCODING_TYPE_JSON, .data = "{not json" });
    const pages = [_][]const u8{
        // A blob of an encoding the client doesn't know, after a good one.
        try rawPage(arena, &.{ .{ .events = events[0..1] }, .{ .encoding = .ENCODING_TYPE_UNSPECIFIED, .events = events[1..] } }, ""),
        try rawPage(arena, &.{.{ .encoding = @fromBackingInt(7), .events = events }}, ""),
        // A blob that holds no events.
        try rawPage(arena, &.{ .{ .events = events }, .{ .events = &.{} } }, ""),
        try encodeMessage(arena, bad_proto),
        try encodeMessage(arena, bad_json),
    };
    for (pages) |page| {
        var c = try initClient();
        defer c.close();
        const answers: [3]Answer = @splat(.{ .rpc = history_rpc, .proto = page });
        var server: Server = .{ .answers = &answers };
        try connectToServer(&c, &server);

        try testing.expectError(error.InvalidResponse, c.fetchHistory(arena, "wf-raw", ""));
        var it = try c.historyIterator("wf-raw", "", .{});
        defer it.deinit();
        try testing.expectError(error.InvalidResponse, it.next(arena));
        var run = try c.getWorkflow([]const u8, "wf-raw", "run-1");
        defer run.deinit();
        try testing.expectError(error.InvalidResponse, run.get(arena));
        try testing.expectEqual(@as(usize, 3), server.callCount());
    }
}

/// A DescribeWorkflowExecution answer for a run in `status`.
fn describeAnswer(arena: Allocator, status: enums.WorkflowExecutionStatus) !Answer {
    return .{ .rpc = "DescribeWorkflowExecution", .proto = try encodeMessage(arena, wfs.DescribeWorkflowExecutionResponse{
        .workflow_execution_info = .{ .execution = .{ .workflow_id = "wf-raw", .run_id = "run-1" }, .status = status },
    }) };
}

test "WorkflowRun.get fails on a page with no events and no token for a closed run, and waits on one with a token" {
    testing.log_level = .err; // Each failure is logged as a warning.
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const events = try closedRunEvents(arena);
    const empty_pages = [_][]const u8{
        try encodeMessage(arena, wfs.GetWorkflowExecutionHistoryResponse{}),
        try historyPage(arena, &.{}, ""),
    };
    for (empty_pages) |page| {
        var c = try initClient();
        defer c.close();
        // The run has closed, and a look without waiting finds no close event either. The
        // last answer is one the fixed client never asks for; a loop that asked again at
        // once would get it.
        const answers = [_]Answer{
            .{ .rpc = history_rpc, .proto = page },
            try describeAnswer(arena, .WORKFLOW_EXECUTION_STATUS_COMPLETED),
            .{ .rpc = history_rpc, .proto = page },
            .{ .rpc = history_rpc, .proto = page },
        };
        var server: Server = .{ .answers = &answers };
        try connectToServer(&c, &server);
        var run = try c.getWorkflow([]const u8, "wf-raw", "run-1");
        defer run.deinit();
        try testing.expectError(error.InvalidResponse, run.get(arena));
        try testing.expectEqual(@as(usize, 3), server.callCount());
    }

    // A long poll that ended before the run closed: an empty page with a token.
    var c = try initClient();
    defer c.close();
    const answers = [_]Answer{
        .{ .rpc = history_rpc, .proto = try encodeMessage(arena, wfs.GetWorkflowExecutionHistoryResponse{ .next_page_token = "poll-again" }) },
        .{ .rpc = history_rpc, .proto = try rawPage(arena, &.{.{ .events = events[4..] }}, "") },
    };
    var server: Server = .{ .answers = &answers };
    try connectToServer(&c, &server);
    var run = try c.getWorkflow([]const u8, "wf-raw", "run-1");
    defer run.deinit();
    try testing.expectEqualStrings("done", try run.get(arena));
    try testing.expectEqual(@as(usize, 2), server.callCount());
}

// The time-skipping test server ends a close-event long poll that saw nothing after 20
// seconds with an empty page and no token (TestWorkflowService.getWorkflowExecutionHistory),
// so a run that takes longer than that, as on a loaded machine, must still be waited for.
test "WorkflowRun.get waits again after a page with no events and no token while the run is open" {
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const events = try closedRunEvents(arena);
    const empty = try encodeMessage(arena, wfs.GetWorkflowExecutionHistoryResponse{});
    const closed = try historyPage(arena, events[4..], "");

    // Still running twice, then closed.
    {
        var c = try initClient();
        defer c.close();
        const answers = [_]Answer{
            .{ .rpc = history_rpc, .proto = empty },
            try describeAnswer(arena, .WORKFLOW_EXECUTION_STATUS_RUNNING),
            .{ .rpc = history_rpc, .proto = empty },
            try describeAnswer(arena, .WORKFLOW_EXECUTION_STATUS_RUNNING),
            .{ .rpc = history_rpc, .proto = closed },
        };
        var server: Server = .{ .answers = &answers };
        try connectToServer(&c, &server);
        var run = try c.getWorkflow([]const u8, "wf-raw", "run-1");
        defer run.deinit();
        try testing.expectEqualStrings("done", try run.get(arena));
        try testing.expectEqual(answers.len, server.callCount());
    }

    // The run closed between the empty page and the description: the look without waiting
    // finds its close event.
    {
        var c = try initClient();
        defer c.close();
        const answers = [_]Answer{
            .{ .rpc = history_rpc, .proto = empty },
            try describeAnswer(arena, .WORKFLOW_EXECUTION_STATUS_COMPLETED),
            .{ .rpc = history_rpc, .proto = closed },
        };
        var server: Server = .{ .answers = &answers };
        try connectToServer(&c, &server);
        var run = try c.getWorkflow([]const u8, "wf-raw", "run-1");
        defer run.deinit();
        try testing.expectEqualStrings("done", try run.get(arena));
        try testing.expectEqual(answers.len, server.callCount());
    }
}

test "callService returns GetWorkflowExecutionHistory's raw history as the server sent it" {
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const events = try closedRunEvents(arena);
    const answers = [_]Answer{.{ .rpc = history_rpc, .proto = try rawPage(arena, &.{ .{ .events = events[0..2] }, .{ .events = events[2..] } }, "") }};
    var c = try initClient();
    defer c.close();
    var server: Server = .{ .answers = &answers };
    try connectToServer(&c, &server);
    const response = try c.callService(.workflow, history_rpc, arena, .{ .namespace = "default", .execution = .{ .workflow_id = "wf-raw" } }, null);
    try testing.expect(response.history == null);
    try testing.expectEqual(@as(usize, 2), response.raw_history.items.len);
}

// ---------------------------------------------------------------- schedules

test "a schedule description's id stays valid after its handle is freed" {
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const action = try schedule.startWorkflowByName(arena, "Report", .{}, .{ .id = "report", .task_queue = "reports" });
    const response: wfs.DescribeScheduleResponse = .{
        .schedule = try (schedule.Schedule{ .action = action, .spec = .{} }).toProto(arena, null),
        .conflict_token = "token-1",
    };
    const answers = [_]Answer{.{ .rpc = "DescribeSchedule", .proto = try encodeMessage(arena, response) }};
    var c = try initClient();
    defer c.close();
    var server: Server = .{ .answers = &answers };
    try connectToServer(&c, &server);

    var handle = try c.getScheduleHandle("nightly-report");
    const description = handle.describe(arena) catch |err| {
        handle.deinit();
        return err;
    };
    handle.deinit();
    // The description and every slice in it live in `arena`, not in the handle.
    try testing.expectEqualStrings("nightly-report", description.id);
    try testing.expectEqualStrings("token-1", description.raw.conflict_token);
    try testing.expectEqual(@as(usize, 1), server.callCount());
}
