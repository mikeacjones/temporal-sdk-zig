//! The client's typed operations against answers served in process: a connection whose calls
//! an RPC override answers with preset responses, so a test can give the client a response a
//! dev server won't send. Covers history pages sent as raw history (`fetchHistory`,
//! `historyIterator`, `WorkflowRun.get`), a schedule description that outlives its handle
//! (`ScheduleHandle.describe`), and the requests of `resetWorkflow` (reset targets and
//! post-reset operations) and `updateWorkflowExecutionOptions` (priority), with
//! `resetPointEventId`.
//!
//! Also dev-server tests of reset targets, post-reset operations and the Nexus reapply
//! exclusion, through `testing.WorkflowEnvironment`.

const std = @import("std");
const client_mod = @import("client.zig");
const bridge = @import("bridge.zig");
const proto = @import("proto.zig");
const converter = @import("converter.zig");
const protojson = @import("protojson.zig");
const schedule = @import("schedule.zig");
const nexus = @import("nexus.zig");
const testing_mod = @import("testing.zig");
const worker_mod = @import("worker.zig");
const workflow = @import("workflow.zig");
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

/// One call's preset answer: the method it answers and its encoded response, or, with a
/// nonzero `status_code`, the gRPC status it fails with.
const Answer = struct {
    rpc: []const u8,
    proto: []const u8,
    status_code: u32 = 0,
    message: []const u8 = "",
};

/// Serves a connection's calls in Zig: answers GetSystemInfo, and every other call with the
/// next of `answers`, in order. A call for another method than the next answer names, or one
/// after the last answer, fails with FAILED_PRECONDITION. Counts the calls it answers.
const Server = struct {
    answers: []const Answer,
    /// When set, the request bytes of each call answered (GetSystemInfo aside) are copied
    /// into it and listed in `requests`, in order.
    record: ?Allocator = null,
    requests: std.ArrayList([]const u8) = .empty,
    mutex: std.Io.Mutex = .init,
    calls: usize = 0,
    override: bridge.Connection.GrpcOverride = .{ .handle = handle },

    fn handle(ctx: ?*anyopaque, req: GrpcRequest) void {
        const s: *Server = @ptrCast(@alignCast(ctx.?));
        if (std.mem.eql(u8, req.rpc(), "GetSystemInfo")) return req.respond(testing.allocator, .{});
        s.mutex.lockUncancelable(testing.io);
        const index = s.calls;
        s.calls += 1;
        const recorded = if (s.record) |a| blk: {
            const copy = a.dupe(u8, req.proto()) catch break :blk false;
            s.requests.append(a, copy) catch break :blk false;
            break :blk true;
        } else true;
        s.mutex.unlock(testing.io);
        if (!recorded) return req.respond(testing.allocator, .{ .status_code = 8, .message = "the test ran out of memory recording the request" });
        if (index >= s.answers.len) return req.respond(testing.allocator, .{ .status_code = 9, .message = "the test has no answer left" });
        const answer = s.answers[index];
        if (!std.mem.eql(u8, req.rpc(), answer.rpc)) return req.respond(testing.allocator, .{ .status_code = 9, .message = "the test expected another method" });
        if (answer.status_code != 0) return req.respond(testing.allocator, .{ .status_code = answer.status_code, .message = answer.message });
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
const describe_rpc = "DescribeWorkflowExecution";

/// A DescribeWorkflowExecution response for run-1 of wf-raw in `status`.
fn describePage(arena: Allocator, status: enums.WorkflowExecutionStatus) ![]const u8 {
    return encodeMessage(arena, wfs.DescribeWorkflowExecutionResponse{ .workflow_execution_info = .{
        .execution = .{ .workflow_id = "wf-raw", .run_id = "run-1" },
        .status = status,
    } });
}

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

test "WorkflowRun.get asks again after a page with no events and no token while the run is open, and waits on one with a token" {
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const events = try closedRunEvents(arena);
    {
        // The time-skipping server ends a long poll with such a page, the run still open.
        var c = try initClient();
        defer c.close();
        const answers = [_]Answer{
            .{ .rpc = history_rpc, .proto = try encodeMessage(arena, wfs.GetWorkflowExecutionHistoryResponse{}) },
            .{ .rpc = describe_rpc, .proto = try describePage(arena, .WORKFLOW_EXECUTION_STATUS_RUNNING) },
            .{ .rpc = history_rpc, .proto = try historyPage(arena, &.{}, "") },
            .{ .rpc = describe_rpc, .proto = try describePage(arena, .WORKFLOW_EXECUTION_STATUS_RUNNING) },
            .{ .rpc = history_rpc, .proto = try rawPage(arena, &.{.{ .events = events[4..] }}, "") },
        };
        var server: Server = .{ .answers = &answers };
        try connectToServer(&c, &server);
        var run = try c.getWorkflow([]const u8, "wf-raw", "run-1");
        defer run.deinit();
        try testing.expectEqualStrings("done", try run.get(arena));
        try testing.expectEqual(@as(usize, 5), server.callCount());
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

/// A codec whose decode fails with `error.CodecFailed` on a payload of encoding
/// `binary/poison` and passes every other payload through, as do its encode.
const PoisonCodec = struct {
    const vtable: converter.PayloadCodec.VTable = .{ .encode = pass, .decode = decode };

    fn codec() converter.PayloadCodec {
        return .{ .ptr = @ptrCast(@constCast(&vtable)), .vtable = &vtable };
    }

    fn pass(_: *anyopaque, _: Allocator, _: std.Io, payloads: []const converter.Payload) converter.CodecError![]converter.Payload {
        return @constCast(payloads);
    }

    fn decode(_: *anyopaque, _: Allocator, _: std.Io, payloads: []const converter.Payload) converter.CodecError![]converter.Payload {
        for (payloads) |p| if (std.mem.eql(u8, converter.encodingOf(p) orelse "", "binary/poison")) return error.CodecFailed;
        return @constCast(payloads);
    }
};

test "WorkflowRun.get keeps waiting on an open run whose description holds a payload the codecs can't decode, which describeWorkflow still fails on" {
    testing.log_level = .err; // describeWorkflow's codec failure is logged as a warning.
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const events = try closedRunEvents(arena);

    var poisoned: common.Payload = .{ .data = "written by another codec" };
    try poisoned.metadata.append(arena, .{ .key = "encoding", .value = "binary/poison" });
    var memo: common.Memo = .{};
    try memo.fields.append(arena, .{ .key = "note", .value = poisoned });
    const describe = try encodeMessage(arena, wfs.DescribeWorkflowExecutionResponse{ .workflow_execution_info = .{
        .execution = .{ .workflow_id = "wf-raw", .run_id = "run-1" },
        .status = .WORKFLOW_EXECUTION_STATUS_RUNNING,
        .memo = memo,
    } });

    const codecs = [_]converter.PayloadCodec{PoisonCodec.codec()};
    var c = try Client.init(testing.allocator, testing.io, .{ .host_port = closed_port, .core_log_level = null, .codecs = &codecs });
    defer c.close();
    const answers = [_]Answer{
        .{ .rpc = history_rpc, .proto = try encodeMessage(arena, wfs.GetWorkflowExecutionHistoryResponse{}) },
        .{ .rpc = describe_rpc, .proto = describe },
        .{ .rpc = history_rpc, .proto = try historyPage(arena, events[4..], "") },
        .{ .rpc = describe_rpc, .proto = describe },
    };
    var server: Server = .{ .answers = &answers };
    try connectToServer(&c, &server);
    var run = try c.getWorkflow([]const u8, "wf-raw", "run-1");
    defer run.deinit();
    try testing.expectEqualStrings("done", try run.get(arena));
    try testing.expectEqual(@as(usize, 3), server.callCount());

    // The public describe still decodes the response through the codecs.
    try testing.expectError(error.CodecFailed, c.describeWorkflow(arena, "wf-raw", "run-1"));
    try testing.expectEqual(@as(usize, 4), server.callCount());
}

test "WorkflowRun.get returns error.InvalidResponse when a closed run's pages keep coming back without a close event" {
    testing.log_level = .err; // The failure is logged as a warning.
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const pair = [_]Answer{
        .{ .rpc = history_rpc, .proto = try encodeMessage(arena, wfs.GetWorkflowExecutionHistoryResponse{}) },
        .{ .rpc = describe_rpc, .proto = try describePage(arena, .WORKFLOW_EXECUTION_STATUS_COMPLETED) },
    };
    // A fourth pair the bounded loop never asks for.
    const answers = try std.mem.concat(arena, Answer, &.{ &pair, &pair, &pair, &pair });
    var c = try initClient();
    defer c.close();
    var server: Server = .{ .answers = answers };
    try connectToServer(&c, &server);
    var run = try c.getWorkflow([]const u8, "wf-raw", "run-1");
    defer run.deinit();
    try testing.expectError(error.InvalidResponse, run.get(arena));
    try testing.expectEqual(@as(usize, 6), server.callCount());
}

test "WorkflowRun.get returns error.NotFound for a run that is gone after a page with no events and no token" {
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const answers = [_]Answer{
        .{ .rpc = history_rpc, .proto = try encodeMessage(arena, wfs.GetWorkflowExecutionHistoryResponse{}) },
        .{ .rpc = describe_rpc, .proto = "", .status_code = 5, .message = "workflow not found" },
        .{ .rpc = history_rpc, .proto = try encodeMessage(arena, wfs.GetWorkflowExecutionHistoryResponse{}) },
    };
    var c = try initClient();
    defer c.close();
    var server: Server = .{ .answers = &answers };
    try connectToServer(&c, &server);
    var run = try c.getWorkflow([]const u8, "wf-raw", "run-1");
    defer run.deinit();
    try testing.expectError(error.NotFound, run.get(arena));
    try testing.expectEqual(@as(usize, 2), server.callCount());
}

test "WorkflowRun.get finds the close event of a run that closed between an empty page and its description" {
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const events = try closedRunEvents(arena);
    const answers = [_]Answer{
        .{ .rpc = history_rpc, .proto = try encodeMessage(arena, wfs.GetWorkflowExecutionHistoryResponse{}) },
        .{ .rpc = describe_rpc, .proto = try describePage(arena, .WORKFLOW_EXECUTION_STATUS_COMPLETED) },
        .{ .rpc = history_rpc, .proto = try historyPage(arena, events[4..], "") },
    };
    var c = try initClient();
    defer c.close();
    var server: Server = .{ .answers = &answers };
    try connectToServer(&c, &server);
    var run = try c.getWorkflow([]const u8, "wf-raw", "run-1");
    defer run.deinit();
    try testing.expectEqualStrings("done", try run.get(arena));
    try testing.expectEqual(answers.len, server.callCount());
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

// ---------------------------------------------------------------- reconnect

test "reconnect of a connected client gives up within rpc_timeout and keeps its connection" {
    // The failed reconnect is logged as a warning.
    testing.log_level = .err;
    // A listener that never accepts: the core's TLS handshake waits for a server hello that
    // never comes.
    const any: std.Io.net.IpAddress = try .parseIp4("127.0.0.1", 0);
    var silent = try any.listen(testing.io, .{});
    var buf: [32]u8 = undefined;
    const host_port = try std.fmt.bufPrint(&buf, "127.0.0.1:{d}", .{silent.socket.address.getPort()});
    var c = try Client.init(testing.allocator, testing.io, .{ .host_port = closed_port, .core_log_level = null, .rpc_timeout = .fromMilliseconds(100) });
    defer c.close();
    // Before `close`: resets the connection the abandoned attempt made, which then ends.
    defer silent.deinit(testing.io);
    var server: Server = .{ .answers = &.{} };
    try connectToServer(&c, &server);
    const before = c.shared.connection.?.ptr;

    const start = std.Io.Clock.awake.now(testing.io);
    try testing.expectError(error.ConnectFailed, c.reconnect(.{ .host_port = host_port, .tls = .{} }));
    try testing.expect(start.durationTo(std.Io.Clock.awake.now(testing.io)).nanoseconds < 10 * std.time.ns_per_s);
    try testing.expectEqual(before, c.shared.connection.?.ptr);
    try testing.expectEqual(@as(usize, 1), c.shared.abandoned_reconnects.items.len);
    try testing.expectEqual(@as(u32, 0), c.shared.reconnecting);
}

// ---------------------------------------------------------------- reset and execution options

const reset_rpc = "ResetWorkflowExecution";
const options_rpc = "UpdateWorkflowExecutionOptions";

fn decodeRequest(comptime Request: type, arena: Allocator, bytes: []const u8) !Request {
    var reader: std.Io.Reader = .fixed(bytes);
    return Request.decode(&reader, arena);
}

fn resetResponse(arena: Allocator) ![]const u8 {
    return encodeMessage(arena, wfs.ResetWorkflowExecutionResponse{ .run_id = "run-new" });
}

/// A server whose answers are `answers`, recording each request into its own arena, and a
/// client connected to it. Free with `deinit`.
const Recorded = struct {
    client: Client,
    server: Server,
    record_arena: std.heap.ArenaAllocator,

    fn start(r: *Recorded, answers: []const Answer) !void {
        r.record_arena = .init(testing.allocator);
        errdefer r.record_arena.deinit();
        r.client = try initClient();
        errdefer r.client.close();
        r.server = .{ .answers = answers, .record = r.record_arena.allocator() };
        try connectToServer(&r.client, &r.server);
    }

    fn deinit(r: *Recorded) void {
        r.client.close();
        r.record_arena.deinit();
    }

    fn request(r: *Recorded, comptime Request: type, arena: Allocator, index: usize) !Request {
        try testing.expect(index < r.server.requests.items.len);
        return decodeRequest(Request, arena, r.server.requests.items[index]);
    }
};

fn expectPaths(expected: []const []const u8, mask: ?proto.google.protobuf.FieldMask) !void {
    const paths = (mask orelse return error.TestExpectedMask).paths.items;
    try testing.expectEqual(expected.len, paths.len);
    for (expected, paths) |e, p| try testing.expectEqualStrings(e, p);
}

test "resetWorkflow sends each post-reset operation, in order, as an options update with its mask" {
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const answers = [_]Answer{.{ .rpc = reset_rpc, .proto = try resetResponse(arena) }};
    var r: Recorded = undefined;
    try r.start(&answers);
    defer r.deinit();

    const new_run = try r.client.resetWorkflow(arena, "wf-reset", "run-1", .{
        .workflow_task_finish_event_id = 4,
        .post_reset_operations = &.{
            .{ .update_execution_options = .{ .versioning_override = .{ .set = .{ .pinned = .{ .deployment_name = "shop", .build_id = "b2" } } } } },
            .{ .update_execution_options = .{ .priority = .{ .priority_key = 2, .fairness_key = "tenant-a", .fairness_weight = 1.5 }, .versioning_override = .remove } },
        },
    });
    try testing.expectEqualStrings("run-new", new_run);
    const request = try r.request(wfs.ResetWorkflowExecutionRequest, arena, 0);
    try testing.expectEqualStrings("run-1", request.workflow_execution.?.run_id);
    try testing.expectEqual(4, request.workflow_task_finish_event_id);
    const operations = request.post_reset_operations.items;
    try testing.expectEqual(2, operations.len);

    const first = operations[0].variant.?.update_workflow_options;
    try expectPaths(&.{"versioning_override"}, first.update_mask);
    const pinned = first.workflow_execution_options.?.versioning_override.?.override.?.pinned;
    try testing.expectEqualStrings("shop", pinned.version.?.deployment_name);
    try testing.expectEqualStrings("b2", pinned.version.?.build_id);
    try testing.expect(first.workflow_execution_options.?.priority == null);

    const second = operations[1].variant.?.update_workflow_options;
    try expectPaths(&.{ "versioning_override", "priority" }, second.update_mask);
    const options = second.workflow_execution_options.?;
    try testing.expect(options.versioning_override == null);
    try testing.expectEqual(2, options.priority.?.priority_key);
    try testing.expectEqualStrings("tenant-a", options.priority.?.fairness_key);
    try testing.expectEqual(1.5, options.priority.?.fairness_weight);
}

test "updateWorkflowExecutionOptions sends a priority with its mask path and returns the run's priority" {
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const answers = [_]Answer{
        .{ .rpc = options_rpc, .proto = try encodeMessage(arena, wfs.UpdateWorkflowExecutionOptionsResponse{ .workflow_execution_options = .{ .priority = .{ .priority_key = 3, .fairness_key = "tenant-b" } } }) },
        // Every field at its default reads back as no priority.
        .{ .rpc = options_rpc, .proto = try encodeMessage(arena, wfs.UpdateWorkflowExecutionOptionsResponse{ .workflow_execution_options = .{ .priority = .{}, .versioning_override = .{ .override = .{ .auto_upgrade = true } } } }) },
    };
    var r: Recorded = undefined;
    try r.start(&answers);
    defer r.deinit();

    const set = try r.client.updateWorkflowExecutionOptions(arena, "wf-options", "", .{ .priority = .{ .priority_key = 3, .fairness_key = "tenant-b" } });
    try testing.expectEqual(3, set.priority.?.priority_key);
    try testing.expectEqualStrings("tenant-b", set.priority.?.fairness_key);
    try testing.expect(set.versioning_override == null);
    const first = try r.request(wfs.UpdateWorkflowExecutionOptionsRequest, arena, 0);
    try expectPaths(&.{"priority"}, first.update_mask);
    try testing.expectEqual(3, first.workflow_execution_options.?.priority.?.priority_key);
    try testing.expectEqualStrings("tenant-b", first.workflow_execution_options.?.priority.?.fairness_key);

    const both = try r.client.updateWorkflowExecutionOptions(arena, "wf-options", "", .{ .versioning_override = .{ .set = .auto_upgrade }, .priority = .{} });
    try testing.expect(both.priority == null);
    try testing.expect(both.versioning_override.? == .auto_upgrade);
    const second = try r.request(wfs.UpdateWorkflowExecutionOptionsRequest, arena, 1);
    try expectPaths(&.{ "versioning_override", "priority" }, second.update_mask);
    try testing.expect(second.workflow_execution_options.?.versioning_override.?.override.? == .auto_upgrade);
    // `.{}` is sent as a priority of default fields, which puts the server's back.
    try testing.expectEqual(0, second.workflow_execution_options.?.priority.?.priority_key);
    try testing.expectEqual(2, r.server.callCount());
}

test "reset and execution options that name no reset point, two, an empty build ID, or no change are error.InvalidOptions before any call" {
    testing.log_level = .err; // Each refusal is logged as a warning.
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    var r: Recorded = undefined;
    try r.start(&.{});
    defer r.deinit();
    const c = &r.client;
    const cases = [_]client_mod.ResetOptions{
        .{},
        .{ .workflow_task_finish_event_id = -3 },
        .{ .workflow_task_finish_event_id = 4, .target = .first_workflow_task },
        .{ .workflow_task_finish_event_id = -1, .target = .last_workflow_task },
        .{ .target = .{ .build_id = "" } },
        .{ .workflow_task_finish_event_id = 4, .post_reset_operations = &.{.{ .update_execution_options = .{} }} },
        .{ .target = .first_workflow_task, .post_reset_operations = &.{
            .{ .update_execution_options = .{ .priority = .{ .priority_key = 1 } } },
            .{ .update_execution_options = .{} },
        } },
    };
    for (cases) |options| try testing.expectError(error.InvalidOptions, c.resetWorkflow(arena, "wf-reset", "", options));
    try testing.expectError(error.InvalidOptions, c.updateWorkflowExecutionOptions(arena, "wf-options", "", .{}));
    try testing.expectEqual(0, r.server.callCount());
}

fn describeWithPoints(arena: Allocator, points: []const proto.temporal.api.workflow.v1.ResetPointInfo) ![]const u8 {
    var reset_points: proto.temporal.api.workflow.v1.ResetPoints = .{};
    try reset_points.points.appendSlice(arena, points);
    return encodeMessage(arena, wfs.DescribeWorkflowExecutionResponse{ .workflow_execution_info = .{
        .execution = .{ .workflow_id = "wf-reset", .run_id = "run-described" },
        .status = .WORKFLOW_EXECUTION_STATUS_RUNNING,
        .auto_reset_points = reset_points,
    } });
}

test "a build ID target resets to the run and event of the build's reset point, and finds none that is missing, not resettable or expired" {
    testing.log_level = .err; // error.NoResetPoint is logged as a warning.
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const now_seconds: i64 = @intCast(@divFloor(std.Io.Timestamp.now(testing.io, .real).nanoseconds, std.time.ns_per_s));
    const later: proto.google.protobuf.Timestamp = .{ .seconds = now_seconds + 24 * 60 * 60 };
    const earlier: proto.google.protobuf.Timestamp = .{ .seconds = now_seconds - 60 };
    const found = [_]proto.temporal.api.workflow.v1.ResetPointInfo{
        .{ .build_id = "b0", .run_id = "run-0", .first_workflow_task_completed_id = 4, .resettable = true },
        .{ .build_id = "b1", .run_id = "run-1", .first_workflow_task_completed_id = 9, .resettable = true, .expire_time = later },
        .{ .build_id = "b1", .run_id = "run-2", .first_workflow_task_completed_id = 15, .resettable = true },
    };
    {
        const answers = [_]Answer{
            .{ .rpc = describe_rpc, .proto = try describeWithPoints(arena, &found) },
            .{ .rpc = reset_rpc, .proto = try resetResponse(arena) },
            // A point without a run ID is in the described run.
            .{ .rpc = describe_rpc, .proto = try describeWithPoints(arena, &.{.{ .build_id = "b3", .first_workflow_task_completed_id = 7, .resettable = true }}) },
            .{ .rpc = reset_rpc, .proto = try resetResponse(arena) },
        };
        var r: Recorded = undefined;
        try r.start(&answers);
        defer r.deinit();
        const build_id = try arena.dupe(u8, "b1");
        try testing.expectEqualStrings("run-new", try r.client.resetWorkflow(arena, "wf-reset", "", .{ .target = .{ .build_id = build_id }, .reason = "bad deploy" }));
        const describe = try r.request(wfs.DescribeWorkflowExecutionRequest, arena, 0);
        try testing.expectEqual(0, describe.execution.?.run_id.len);
        const request = try r.request(wfs.ResetWorkflowExecutionRequest, arena, 1);
        try testing.expectEqualStrings("run-1", request.workflow_execution.?.run_id);
        try testing.expectEqual(9, request.workflow_task_finish_event_id);
        try testing.expectEqualStrings("bad deploy", request.reason);

        _ = try r.client.resetWorkflow(arena, "wf-reset", "run-x", .{ .target = .{ .build_id = "b3" } });
        try testing.expectEqualStrings("run-x", (try r.request(wfs.DescribeWorkflowExecutionRequest, arena, 2)).execution.?.run_id);
        const without_run = try r.request(wfs.ResetWorkflowExecutionRequest, arena, 3);
        try testing.expectEqualStrings("run-described", without_run.workflow_execution.?.run_id);
        try testing.expectEqual(7, without_run.workflow_task_finish_event_id);
        try testing.expectEqual(4, r.server.callCount());
    }
    const missing = [_][]const proto.temporal.api.workflow.v1.ResetPointInfo{
        &.{},
        &.{.{ .build_id = "b0", .run_id = "run-0", .first_workflow_task_completed_id = 4, .resettable = true }},
        &.{.{ .build_id = "b1", .run_id = "run-1", .first_workflow_task_completed_id = 9 }},
        &.{.{ .build_id = "b1", .run_id = "run-1", .first_workflow_task_completed_id = 9, .resettable = true, .expire_time = earlier }},
    };
    for (missing) |points| {
        const answers = [_]Answer{.{ .rpc = describe_rpc, .proto = try describeWithPoints(arena, points) }};
        var r: Recorded = undefined;
        try r.start(&answers);
        defer r.deinit();
        try testing.expectError(error.NoResetPoint, r.client.resetWorkflow(arena, "wf-reset", "", .{ .target = .{ .build_id = "b1" } }));
        try testing.expectEqual(1, r.server.callCount());
    }
}

/// A history of two completed workflow tasks: completed events 4 and 9.
fn twoTaskEvents(arena: Allocator) ![]const history_v1.HistoryEvent {
    const events = try arena.alloc(history_v1.HistoryEvent, 10);
    const types = [_]enums.EventType{
        .EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
        .EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
        .EVENT_TYPE_WORKFLOW_TASK_STARTED,
        .EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
        .EVENT_TYPE_TIMER_STARTED,
        .EVENT_TYPE_TIMER_FIRED,
        .EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
        .EVENT_TYPE_WORKFLOW_TASK_STARTED,
        .EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
        .EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
    };
    for (events, types, 1..) |*e, t, id| e.* = .{ .event_id = @intCast(id), .event_type = t, .event_time = .{ .seconds = 1000 + @as(i64, @intCast(id)) } };
    return events;
}

test "first and last workflow task targets read the history of the run the description names" {
    testing.log_level = .err; // error.NoResetPoint is logged as a warning.
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const events = try twoTaskEvents(arena);
    for ([_]struct { client_mod.ResetTarget, i64 }{ .{ .first_workflow_task, 4 }, .{ .last_workflow_task, 9 } }) |case| {
        const answers = [_]Answer{
            .{ .rpc = describe_rpc, .proto = try describePage(arena, .WORKFLOW_EXECUTION_STATUS_RUNNING) },
            .{ .rpc = history_rpc, .proto = try historyPage(arena, events[0..5], "page-2") },
            .{ .rpc = history_rpc, .proto = try historyPage(arena, events[5..], "") },
            .{ .rpc = reset_rpc, .proto = try resetResponse(arena) },
        };
        // The first workflow task is found on the first page, so the second isn't read.
        const used: []const Answer = if (case[0] == .first_workflow_task) &.{ answers[0], answers[1], answers[3] } else &answers;
        var r: Recorded = undefined;
        try r.start(used);
        defer r.deinit();
        try testing.expectEqualStrings("run-new", try r.client.resetWorkflow(arena, "wf-raw", "", .{ .target = case[0] }));
        try testing.expectEqualStrings("run-1", (try r.request(wfs.GetWorkflowExecutionHistoryRequest, arena, 1)).execution.?.run_id);
        const request = try r.request(wfs.ResetWorkflowExecutionRequest, arena, used.len - 1);
        try testing.expectEqualStrings("run-1", request.workflow_execution.?.run_id);
        try testing.expectEqual(case[1], request.workflow_task_finish_event_id);
        try testing.expectEqual(used.len, r.server.callCount());
    }
    {
        // A run ID given is the run read, with no description.
        const answers = [_]Answer{
            .{ .rpc = history_rpc, .proto = try historyPage(arena, events, "") },
            .{ .rpc = reset_rpc, .proto = try resetResponse(arena) },
        };
        var r: Recorded = undefined;
        try r.start(&answers);
        defer r.deinit();
        _ = try r.client.resetWorkflow(arena, "wf-raw", "run-7", .{ .target = .last_workflow_task });
        const request = try r.request(wfs.ResetWorkflowExecutionRequest, arena, 1);
        try testing.expectEqualStrings("run-7", request.workflow_execution.?.run_id);
        try testing.expectEqual(9, request.workflow_task_finish_event_id);
    }
    {
        // No workflow task scheduled yet, such as a delayed start: nothing to reset to.
        const answers = [_]Answer{
            .{ .rpc = describe_rpc, .proto = try describePage(arena, .WORKFLOW_EXECUTION_STATUS_RUNNING) },
            .{ .rpc = history_rpc, .proto = try historyPage(arena, events[0..1], "") },
        };
        var r: Recorded = undefined;
        try r.start(&answers);
        defer r.deinit();
        try testing.expectError(error.NoResetPoint, r.client.resetWorkflow(arena, "wf-raw", "", .{ .target = .last_workflow_task }));
        try testing.expectEqual(2, r.server.callCount());
    }
}

test "resetPointEventId finds the first and last completed workflow task, falls back to the first task's started event, and finds the last at or before a time" {
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const events = try twoTaskEvents(arena);
    const at = struct {
        fn at(seconds: i64) std.Io.Timestamp {
            return .{ .nanoseconds = @as(i96, seconds) * std.time.ns_per_s };
        }
    }.at;
    try testing.expectEqual(4, client_mod.resetPointEventId(events, .first_workflow_task));
    try testing.expectEqual(9, client_mod.resetPointEventId(events, .last_workflow_task));
    // Event 9 completed at 1009 s: the boundary is included.
    try testing.expectEqual(9, client_mod.resetPointEventId(events, .{ .last_workflow_task_at_or_before = at(1009) }));
    try testing.expectEqual(4, client_mod.resetPointEventId(events, .{ .last_workflow_task_at_or_before = at(1008) }));
    try testing.expectEqual(4, client_mod.resetPointEventId(events, .{ .last_workflow_task_at_or_before = at(1004) }));
    try testing.expectEqual(null, client_mod.resetPointEventId(events, .{ .last_workflow_task_at_or_before = at(1003) }));

    // No completed workflow task: the first scheduled one's started event, for either end;
    // a time finds nothing.
    try testing.expectEqual(3, client_mod.resetPointEventId(events[0..3], .first_workflow_task));
    try testing.expectEqual(3, client_mod.resetPointEventId(events[0..2], .last_workflow_task));
    try testing.expectEqual(null, client_mod.resetPointEventId(events[0..3], .{ .last_workflow_task_at_or_before = at(2000) }));

    // A completed event without a time is skipped by the time variant only.
    var untimed = try arena.dupe(history_v1.HistoryEvent, events);
    untimed[8].event_time = null;
    try testing.expectEqual(9, client_mod.resetPointEventId(untimed, .last_workflow_task));
    try testing.expectEqual(4, client_mod.resetPointEventId(untimed, .{ .last_workflow_task_at_or_before = at(2000) }));

    try testing.expectEqual(null, client_mod.resetPointEventId(&.{}, .first_workflow_task));
    try testing.expectEqual(null, client_mod.resetPointEventId(&.{}, .last_workflow_task));
    try testing.expectEqual(null, client_mod.resetPointEventId(events[0..1], .last_workflow_task));
    try testing.expectEqual(null, client_mod.resetPointEventId(&.{}, .{ .last_workflow_task_at_or_before = at(2000) }));
}

// ---------------------------------------------------------------- reset on a dev server

const WorkflowEnvironment = testing_mod.WorkflowEnvironment;

const ResetFixtures = struct {
    const Finish = workflow.SignalDef("finish", fn () void);

    const Latch = struct {
        open: bool = false,
        fn set(l: *Latch) void {
            l.open = true;
        }
        fn isOpen(l: *Latch) bool {
            return l.open;
        }
    };

    fn awaitFinish(ctx: *workflow.Context) !void {
        const latch = try ctx.allocator().create(Latch);
        latch.* = .{};
        try ctx.setSignalHandler(Finish, latch, Latch.set);
        try ctx.await(latch, Latch.isOpen);
    }

    /// Completes two workflow tasks (the second after a timer), then waits for `finish`.
    fn twoTasks(ctx: *workflow.Context, name: []const u8) ![]const u8 {
        try ctx.sleep(.fromMilliseconds(1), .{});
        try awaitFinish(ctx);
        return std.fmt.allocPrint(ctx.allocator(), "done {s}", .{name});
    }
    const TwoTasks = workflow.Def("ClientTestResetTwoTasks", twoTasks);

    /// The handler workflow of the `hold` operation: waits for `finish`.
    fn held(ctx: *workflow.Context, id: []const u8) ![]const u8 {
        try awaitFinish(ctx);
        return std.fmt.allocPrint(ctx.allocator(), "released {s}", .{id});
    }
    const Held = workflow.Def("ClientTestResetHeld", held);

    const Hold = nexus.OperationDef("hold", []const u8, []const u8);
    const Service = nexus.ServiceDef("zig-client-reset-service", .{Hold});

    fn hold(_: *nexus.Context, id: []const u8) !nexus.WorkflowStart(Held) {
        return .{ .options = .{ .id = id }, .args = .{id} };
    }
    const Handler = nexus.ServiceHandler(Service, .{nexus.OperationHandler(Hold, hold)});

    /// Calls `hold` as workflow `hold_id` through `endpoint`, and returns its result.
    fn callHold(ctx: *workflow.Context, endpoint: []const u8, hold_id: []const u8) ![]const u8 {
        const options: workflow.NexusOperationOptions = .{ .endpoint = endpoint, .schedule_to_close_timeout = .fromSeconds(600) };
        const result = try ctx.executeNexusOperation(Service, Hold, options, hold_id).get();
        return std.fmt.allocPrint(ctx.allocator(), "called: {s}", .{result});
    }
    const CallHold = workflow.Def("ClientTestResetCallHold", callHold);

    fn runWorker(w: *worker_mod.Worker) worker_mod.Worker.RunError!void {
        return w.run();
    }

    fn uniqueName(a: Allocator, prefix: []const u8) ![]const u8 {
        var suffix: [4]u8 = undefined;
        testing.io.random(&suffix);
        return std.fmt.allocPrint(a, "{s}-{x}", .{ prefix, suffix });
    }

    /// Follows the history of run `run_id` until it records an event of `event_type` with an
    /// ID above `after`, and returns that ID.
    fn awaitEventAfter(c: *Client, id: []const u8, run_id: []const u8, event_type: enums.EventType, after: i64) !i64 {
        var it = try c.historyIterator(id, run_id, .{ .wait_new_event = true });
        defer it.deinit();
        var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
        defer scratch.deinit();
        while (try it.next(scratch.allocator())) |event| {
            if (event.event_type == event_type and event.event_id > after) return event.event_id;
            _ = scratch.reset(.retain_capacity);
        }
        std.debug.print("run {s} of workflow {s} closed without a {t} event after event {d}\n", .{ run_id, id, event_type, after });
        return error.TestEventNotRecorded;
    }

    /// Checks that the new run's copied history ends at `point`: the event there is the
    /// reset's workflow task failure, naming `base_run_id`.
    fn expectResetAt(c: *Client, a: Allocator, id: []const u8, new_run_id: []const u8, base_run_id: []const u8, point: i64) !void {
        const history = try c.fetchHistory(a, id, new_run_id);
        for (history.events.items) |event| if (event.event_id == point) {
            const attrs = event.attributes orelse return error.TestUnexpectedResult;
            if (attrs != .workflow_task_failed_event_attributes) {
                std.debug.print("event {d} of the new run is {t}, not the reset's workflow task failure\n", .{ point, event.event_type });
                return error.TestUnexpectedResult;
            }
            const failed = attrs.workflow_task_failed_event_attributes;
            try testing.expectEqual(.WORKFLOW_TASK_FAILED_CAUSE_RESET_WORKFLOW, failed.cause);
            try testing.expectEqualStrings(base_run_id, failed.base_run_id);
            try testing.expectEqualStrings(new_run_id, failed.new_run_id);
            return;
        };
        return error.TestResetPointNotFound;
    }

    fn countAfter(c: *Client, a: Allocator, id: []const u8, run_id: []const u8, event_type: enums.EventType, after: i64) !usize {
        var n: usize = 0;
        for ((try c.fetchHistory(a, id, run_id)).events.items) |event| {
            if (event.event_type == event_type and event.event_id > after) n += 1;
        }
        return n;
    }
};

test "dev server: first and last workflow task targets reset to the points resetPointEventId finds, with a post-reset options change and a priority" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(ResetFixtures.TwoTasks);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const c = &env.client;

    var run = try env.startWorkflow(ResetFixtures.TwoTasks, .{ .id = "client-reset-targets", .task_queue = "" }, .{"ada"});
    defer run.deinit();
    defer c.terminateWorkflow(run.id, "", "test cleanup") catch {};
    var latest_handle = try c.getWorkflow([]const u8, run.id, "");
    defer latest_handle.deinit();
    const first_completed = try ResetFixtures.awaitEventAfter(c, run.id, run.run_id, .EVENT_TYPE_WORKFLOW_TASK_COMPLETED, 0);
    _ = try ResetFixtures.awaitEventAfter(c, run.id, run.run_id, .EVENT_TYPE_WORKFLOW_TASK_COMPLETED, first_completed);
    const base_events = (try c.fetchHistory(a, run.id, run.run_id)).events.items;
    const first = client_mod.resetPointEventId(base_events, .first_workflow_task) orelse return error.TestUnexpectedResult;
    const last = client_mod.resetPointEventId(base_events, .last_workflow_task) orelse return error.TestUnexpectedResult;
    try testing.expectEqual(first_completed, first);
    try testing.expect(first < last);

    // The last workflow task of the latest run, named by an empty run ID, with an options
    // change the server applies to the new run.
    const by_last = try c.resetWorkflow(a, run.id, "", .{
        .target = .last_workflow_task,
        .post_reset_operations = &.{.{ .update_execution_options = .{ .versioning_override = .{ .set = .auto_upgrade } } }},
    });
    try ResetFixtures.expectResetAt(c, a, run.id, by_last, run.run_id, last);
    const base_info = (try c.describeWorkflow(a, run.id, run.run_id)).raw.workflow_execution_info.?;
    try testing.expect(if (base_info.versioning_info) |v| v.versioning_override == null else true);
    const new_info = (try c.describeWorkflow(a, run.id, by_last)).raw.workflow_execution_info.?;
    const override = (new_info.versioning_info orelse return error.TestUnexpectedResult).versioning_override orelse return error.TestUnexpectedResult;
    try testing.expect((override.override orelse return error.TestUnexpectedResult) == .auto_upgrade);

    // The first workflow task of the base run, named by its ID though it is no longer the
    // latest run.
    const by_first = try c.resetWorkflow(a, run.id, run.run_id, .{ .target = .first_workflow_task });
    try ResetFixtures.expectResetAt(c, a, run.id, by_first, run.run_id, first);

    const prioritized = try c.updateWorkflowExecutionOptions(a, run.id, by_first, .{ .priority = .{ .priority_key = 2 } });
    try testing.expectEqual(2, (prioritized.priority orelse return error.TestUnexpectedResult).priority_key);

    // A handle that names the base run doesn't follow the resets; one with an empty run ID
    // gets the latest run's result.
    try testing.expectError(error.WorkflowTerminated, run.get(a));
    try c.signalWorkflow(ResetFixtures.Finish, run.id, by_first, .{});
    try testing.expectEqualStrings("done ada", try latest_handle.get(a));
}

test "dev server: a build ID target resets to the first workflow task that build completed" {
    testing.log_level = .err; // error.NoResetPoint is logged as a warning.
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const c = &env.client;
    const build_id = "client-reset-b1";
    var w = try worker_mod.Worker.init(testing.allocator, testing.io, c, "client-reset-build", .{ .build_id = build_id });
    defer w.deinit();
    try w.registerWorkflow(ResetFixtures.TwoTasks);
    var running = try testing.io.concurrent(ResetFixtures.runWorker, .{&w});
    defer {
        w.stop();
        running.await(testing.io) catch {};
    }

    var run = try c.executeWorkflow(ResetFixtures.TwoTasks, .{ .id = "client-reset-build-id", .task_queue = "client-reset-build" }, .{"bo"});
    defer run.deinit();
    defer c.terminateWorkflow(run.id, "", "test cleanup") catch {};
    const first = try ResetFixtures.awaitEventAfter(c, run.id, run.run_id, .EVENT_TYPE_WORKFLOW_TASK_COMPLETED, 0);
    _ = try ResetFixtures.awaitEventAfter(c, run.id, run.run_id, .EVENT_TYPE_WORKFLOW_TASK_COMPLETED, first);
    // The server records the reset point with the workflow task that completed.
    const info = (try c.describeWorkflow(a, run.id, run.run_id)).raw.workflow_execution_info.?;
    const points = if (info.auto_reset_points) |p| p.points.items else &.{};
    const point = for (points) |p| {
        if (std.mem.eql(u8, p.build_id, build_id)) break p;
    } else return error.TestResetPointNotRecorded;
    try testing.expectEqual(first, point.first_workflow_task_completed_id);

    const new_run = try c.resetWorkflow(a, run.id, "", .{ .target = .{ .build_id = build_id } });
    try ResetFixtures.expectResetAt(c, a, run.id, new_run, run.run_id, first);
    try testing.expectError(error.NoResetPoint, c.resetWorkflow(a, run.id, "", .{ .target = .{ .build_id = "client-reset-unknown" } }));
}

test "dev server: a reset reapplies a Nexus operation's completion after the reset point unless reapply_exclude.nexus is set" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(ResetFixtures.CallHold);
    try env.registerWorkflow(ResetFixtures.Held);
    try env.registerNexusService(ResetFixtures.Handler);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const c = &env.client;
    const endpoint = try ResetFixtures.uniqueName(a, "client-reset-endpoint");
    try env.createNexusEndpoint(endpoint, .{});
    const hold_id = try ResetFixtures.uniqueName(a, "client-reset-held");

    var run = try env.startWorkflow(ResetFixtures.CallHold, .{ .task_queue = "" }, .{ endpoint, hold_id });
    defer run.deinit();
    defer c.terminateWorkflow(run.id, "", "test cleanup") catch {};
    defer c.terminateWorkflow(hold_id, "", "test cleanup") catch {};
    // The reset point: the workflow task that saw the operation start.
    const started = try ResetFixtures.awaitEventAfter(c, run.id, run.run_id, .EVENT_TYPE_NEXUS_OPERATION_STARTED, 0);
    const point = try ResetFixtures.awaitEventAfter(c, run.id, run.run_id, .EVENT_TYPE_WORKFLOW_TASK_COMPLETED, started);
    try c.signalWorkflow(ResetFixtures.Finish, hold_id, "", .{});
    _ = try ResetFixtures.awaitEventAfter(c, run.id, run.run_id, .EVENT_TYPE_NEXUS_OPERATION_COMPLETED, point);
    const expected = try std.fmt.allocPrint(a, "called: released {s}", .{hold_id});
    try testing.expectEqualStrings(expected, try run.get(a));

    // Reapplied: the new run gets the completion and completes.
    const reapplied = try c.resetWorkflow(a, run.id, run.run_id, .{ .workflow_task_finish_event_id = point });
    var reapplied_run = try c.getWorkflow([]const u8, run.id, reapplied);
    defer reapplied_run.deinit();
    try testing.expectEqualStrings(expected, try reapplied_run.get(a));
    try testing.expectEqual(1, try ResetFixtures.countAfter(c, a, run.id, reapplied, .EVENT_TYPE_NEXUS_OPERATION_COMPLETED, point));

    // Excluded: once the worker has run the new run's workflow task, it has no completion
    // and still waits for the operation.
    const excluded = try c.resetWorkflow(a, run.id, run.run_id, .{ .workflow_task_finish_event_id = point, .reapply_exclude = .{ .nexus = true } });
    _ = try ResetFixtures.awaitEventAfter(c, run.id, excluded, .EVENT_TYPE_WORKFLOW_TASK_COMPLETED, point);
    try testing.expectEqual(0, try ResetFixtures.countAfter(c, a, run.id, excluded, .EVENT_TYPE_NEXUS_OPERATION_COMPLETED, point));
    try testing.expectEqual(client_mod.WorkflowExecutionStatus.running, (try c.describeWorkflow(a, run.id, excluded)).status);
}
