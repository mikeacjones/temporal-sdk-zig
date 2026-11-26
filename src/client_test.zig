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
const deployment = @import("deployment.zig");
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

test "a one-time versioning override is sent on starts, signal-with-starts and options updates, and reported back" {
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const v1: proto.temporal.api.deployment.v1.WorkerDeploymentVersion = .{ .deployment_name = "rollout", .build_id = "v1" };
    const answers = [_]Answer{
        .{ .rpc = "StartWorkflowExecution", .proto = try encodeMessage(arena, wfs.StartWorkflowExecutionResponse{ .run_id = "run-1" }) },
        .{ .rpc = "SignalWithStartWorkflowExecution", .proto = try encodeMessage(arena, wfs.SignalWithStartWorkflowExecutionResponse{ .run_id = "run-2" }) },
        .{ .rpc = options_rpc, .proto = try encodeMessage(arena, wfs.UpdateWorkflowExecutionOptionsResponse{ .workflow_execution_options = .{ .versioning_override = .{ .override = .{ .one_time = .{ .target_deployment_version = v1 } } } } }) },
    };
    var r: Recorded = undefined;
    try r.start(&answers);
    defer r.deinit();
    const once: deployment.VersioningOverride = .{ .one_time = .{ .deployment_name = "rollout", .build_id = "v1" } };

    var run = try r.client.executeWorkflowByName(void, "WhoRuns", .{ .id = "wf-once", .task_queue = "q", .versioning_override = once }, .{});
    run.deinit();
    var signaled = try r.client.signalWithStartWorkflowByName(void, "WhoRuns", .{ .id = "wf-once-2", .task_queue = "q", .versioning_override = once }, .{}, "go", .{});
    signaled.deinit();
    const reported = try r.client.updateWorkflowExecutionOptions(arena, "wf-once", "", .{ .versioning_override = .{ .set = once } });
    try testing.expectEqualDeep(@as(?deployment.VersioningOverride, once), reported.versioning_override);

    const start = try r.request(wfs.StartWorkflowExecutionRequest, arena, 0);
    const signal_start = try r.request(wfs.SignalWithStartWorkflowExecutionRequest, arena, 1);
    const update = try r.request(wfs.UpdateWorkflowExecutionOptionsRequest, arena, 2);
    try expectPaths(&.{"versioning_override"}, update.update_mask);
    for ([_]?proto.temporal.api.workflow.v1.VersioningOverride{ start.versioning_override, signal_start.versioning_override, update.workflow_execution_options.?.versioning_override }) |sent| {
        const o = sent orelse return error.TestExpectedOverride;
        // Only the oneof is set, never the deprecated fields.
        try testing.expectEqual(enums.VersioningBehavior.VERSIONING_BEHAVIOR_UNSPECIFIED, o.behavior);
        try testing.expectEqualStrings("", o.pinned_version);
        const target = o.override.?.one_time.target_deployment_version.?;
        try testing.expectEqualStrings("rollout", target.deployment_name);
        try testing.expectEqualStrings("v1", target.build_id);
    }
    try testing.expectEqual(3, r.server.callCount());
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

// ---------------------------------------------------------------- standalone Nexus operations

const call_check = @import("nexus/call_check.zig");
const propagation = @import("propagation.zig");
const failure_v1 = proto.temporal.api.failure.v1;

const start_nexus_rpc = "StartNexusOperationExecution";
const poll_nexus_rpc = "PollNexusOperationExecution";

const NexusFixtures = struct {
    const Echo = nexus.OperationDef("echo", []const u8, []const u8);
    const Hold = nexus.OperationDef("hold", []const u8, []const u8);
    const Service = nexus.ServiceDef("zig-client-standalone-service", .{ Echo, Hold });

    fn echo(_: *nexus.Context, message: []const u8) ![]const u8 {
        return message;
    }

    /// The handler workflow of `hold`: waits for `ResetFixtures.Finish`.
    fn held(ctx: *workflow.Context, id: []const u8) ![]const u8 {
        try ResetFixtures.awaitFinish(ctx);
        return std.fmt.allocPrint(ctx.allocator(), "released {s}", .{id});
    }
    const Held = workflow.Def("ClientTestStandaloneHeld", held);

    fn hold(_: *nexus.Context, id: []const u8) !nexus.WorkflowStart(Held) {
        return .{ .options = .{ .id = id }, .args = .{id} };
    }
    const Handler = nexus.ServiceHandler(Service, .{
        nexus.OperationHandler(Echo, echo),
        nexus.OperationHandler(Hold, hold),
    });

    fn ready(_: *workflow.Context) !void {}
    /// Run once so the environment's worker polls before the first operation starts.
    const Ready = workflow.Def("ClientTestStandaloneReady", ready);

    /// The environment with the service registered, its worker polling, and an endpoint
    /// routed to it; null, after logging why, when the server offers no standalone Nexus
    /// operations. Free with `deinit`.
    fn start(a: Allocator, endpoint: *[]const u8) !*WorkflowEnvironment {
        const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
        errdefer env.deinit();
        try skipWithoutStandaloneNexus(&env.client);
        try env.registerWorkflow(Held);
        try env.registerWorkflow(Ready);
        try env.registerNexusService(Handler);
        endpoint.* = try ResetFixtures.uniqueName(a, "client-standalone-endpoint");
        try env.createNexusEndpoint(endpoint.*, .{});
        _ = try env.executeWorkflow(Ready, .{}, a, null);
        return env;
    }

    /// Returns `error.SkipZigTest`, logging why, when `c`'s server doesn't answer
    /// CountNexusOperationExecutions.
    fn skipWithoutStandaloneNexus(c: *Client) !void {
        var arena: std.heap.ArenaAllocator = .init(testing.allocator);
        defer arena.deinit();
        var failure: ?client_mod.RpcFailure = null;
        _ = c.callService(.workflow, "CountNexusOperationExecutions", arena.allocator(), .{ .namespace = c.namespace }, &failure) catch |err| {
            const message = if (failure) |f| f.message else "";
            std.log.warn("skipping: the dev server doesn't offer standalone Nexus operations (CountNexusOperationExecutions returned {t}: {s})", .{ err, message });
            return error.SkipZigTest;
        };
    }

    /// Describes `h` until its handler has started it (its operation token is set), for at
    /// most 30 s.
    fn awaitStarted(h: anytype) !void {
        var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
        defer scratch.deinit();
        const deadline = std.Io.Timestamp.now(testing.io, .awake).addDuration(.fromSeconds(30));
        while (std.Io.Timestamp.now(testing.io, .awake).nanoseconds < deadline.nanoseconds) {
            _ = scratch.reset(.retain_capacity);
            const d = try h.describe(scratch.allocator());
            if (d.operation_token.len > 0) return;
            try testing.io.sleep(.fromMilliseconds(50), .awake);
        }
        std.debug.print("Nexus operation {s} did not start within 30 s\n", .{h.id});
        return error.TestOperationNotStarted;
    }
};

test "the standalone Nexus call check names the caller, the service and the operation" {
    const Other = nexus.OperationDef("echo", i32, i32);
    const Missing = nexus.OperationDef("missing", []const u8, []const u8);
    const caller = "Client.executeNexusOperation";
    try testing.expectEqual(@as(?[]const u8, null), comptime call_check.callError(caller, NexusFixtures.Service, NexusFixtures.Echo));
    try testing.expectEqualStrings("Client.executeNexusOperation: Service must be a nexus.ServiceDef, not u8", comptime call_check.callError(caller, u8, NexusFixtures.Echo).?);
    try testing.expectEqualStrings("Client.executeNexusOperation on nexus service \"zig-client-standalone-service\": Operation must be a nexus.OperationDef, not u8", comptime call_check.callError(caller, NexusFixtures.Service, u8).?);
    try testing.expectEqualStrings("Client.executeNexusOperation on nexus service \"zig-client-standalone-service\": its operation \"echo\" takes []const u8 and returns []const u8, not i32 and i32; call it with the service's own OperationDef", comptime call_check.callError(caller, NexusFixtures.Service, Other).?);
    try testing.expectEqualStrings("Client.executeNexusOperation on nexus service \"zig-client-standalone-service\": the service has no operation \"missing\"; its operations are \"echo\", \"hold\"", comptime call_check.callError(caller, NexusFixtures.Service, Missing).?);
}

test "executeNexusOperation sends the operation, its options, its input as one payload, and the propagated values in place of a caller's header of their name" {
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const tenant = comptime propagation.key([]const u8, "tenant");
    const propagators = [_]propagation.ContextPropagator{propagation.HeaderPropagator(tenant).propagator};
    var c = try Client.init(testing.allocator, testing.io, .{ .host_port = closed_port, .core_log_level = null, .propagators = &propagators });
    defer c.close();
    const answers = [_]Answer{.{ .rpc = start_nexus_rpc, .proto = try encodeMessage(arena, wfs.StartNexusOperationExecutionResponse{ .run_id = "run-1", .started = true }) }};
    var server: Server = .{ .answers = &answers, .record = arena };
    try connectToServer(&c, &server);

    var values: propagation.Values = .init(arena);
    try values.put(tenant, "acme");
    var handle = try c.executeNexusOperation(NexusFixtures.Service, NexusFixtures.Echo, .{
        .id = "op-1",
        .endpoint = "my-endpoint",
        .schedule_to_close_timeout = .fromSeconds(60),
        .schedule_to_start_timeout = .fromSeconds(5),
        .start_to_close_timeout = .fromSeconds(30),
        .id_reuse_policy = .reject_duplicate,
        .id_conflict_policy = .use_existing,
        .headers = &.{ .{ .name = "x-first", .value = "1" }, .{ .name = "Temporal-Zig-Context", .value = "stale" }, .{ .name = "x-last", .value = "2" } },
        .summary = "echo it",
        .context = &values,
    }, "hi");
    defer handle.deinit();
    try testing.expectEqualStrings("op-1", handle.id);
    try testing.expectEqualStrings("run-1", handle.run_id);
    try testing.expect(handle.started);

    const request = try decodeRequest(wfs.StartNexusOperationExecutionRequest, arena, server.requests.items[0]);
    try testing.expectEqualStrings("default", request.namespace);
    try testing.expectEqualStrings(c.identity, request.identity);
    try testing.expectEqual(36, request.request_id.len);
    try testing.expectEqualStrings("op-1", request.operation_id);
    try testing.expectEqualStrings("my-endpoint", request.endpoint);
    try testing.expectEqualStrings("zig-client-standalone-service", request.service);
    try testing.expectEqualStrings("echo", request.operation);
    try testing.expectEqual(60, request.schedule_to_close_timeout.?.seconds);
    try testing.expectEqual(5, request.schedule_to_start_timeout.?.seconds);
    try testing.expectEqual(30, request.start_to_close_timeout.?.seconds);
    try testing.expectEqual(.NEXUS_OPERATION_ID_REUSE_POLICY_REJECT_DUPLICATE, request.id_reuse_policy);
    try testing.expectEqual(.NEXUS_OPERATION_ID_CONFLICT_POLICY_USE_EXISTING, request.id_conflict_policy);
    try testing.expectEqualStrings("hi", try converter.fromPayload([]const u8, arena, request.input.?));
    try testing.expectEqualStrings("echo it", try converter.fromPayload([]const u8, arena, request.user_metadata.?.summary.?));
    // A map on the wire: its entries' order isn't kept.
    const headers = request.nexus_header.items;
    try testing.expectEqual(3, headers.len);
    const propagated = (try propagation.injectNexusHeader(&propagators, arena, &values)).?;
    var found = [_]bool{ false, false, false };
    for (headers) |h| {
        if (std.mem.eql(u8, h.key, "x-first")) {
            try testing.expectEqualStrings("1", h.value);
            found[0] = true;
        } else if (std.mem.eql(u8, h.key, "x-last")) {
            try testing.expectEqualStrings("2", h.value);
            found[1] = true;
        } else {
            try testing.expectEqualStrings(propagation.nexus_header_name, h.key);
            try testing.expectEqualStrings(propagated, h.value);
            found[2] = true;
        }
    }
    try testing.expect(found[0] and found[1] and found[2]);
}

test "executeNexusOperation with no ID generates one, sends the caller's headers as they are without propagated values, and maps ALREADY_EXISTS" {
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const answers = [_]Answer{
        .{ .rpc = start_nexus_rpc, .proto = try encodeMessage(arena, wfs.StartNexusOperationExecutionResponse{ .run_id = "run-2" }) },
        .{ .rpc = start_nexus_rpc, .proto = "", .status_code = 6, .message = "operation already started" },
    };
    var r: Recorded = undefined;
    try r.start(&answers);
    defer r.deinit();

    var handle = try r.client.executeNexusOperationByName(void, "svc", "op", .{ .endpoint = "e", .headers = &.{.{ .name = "temporal-zig-context", .value = "kept" }} }, @as(i32, 7));
    defer handle.deinit();
    try testing.expectEqual(36, handle.id.len);
    try testing.expect(!handle.started);
    const request = try r.request(wfs.StartNexusOperationExecutionRequest, arena, 0);
    try testing.expectEqualStrings(handle.id, request.operation_id);
    try testing.expect(request.schedule_to_close_timeout == null);
    try testing.expect(request.user_metadata == null);
    try testing.expectEqual(1, request.nexus_header.items.len);
    try testing.expectEqualStrings("kept", request.nexus_header.items[0].value);
    try testing.expectEqual(7, try converter.fromPayload(i32, arena, request.input.?));

    try testing.expectError(error.NexusOperationExecutionAlreadyStarted, r.client.executeNexusOperationByName(void, "svc", "op", .{ .endpoint = "e", .id = handle.id }, {}));
}

fn pollAnswer(arena: Allocator, outcome: ?wfs.PollNexusOperationExecutionResponse.outcome_union) !Answer {
    return .{ .rpc = poll_nexus_rpc, .proto = try encodeMessage(arena, wfs.PollNexusOperationExecutionResponse{ .run_id = "run-1", .outcome = outcome }) };
}

fn failureAnswer(arena: Allocator, info: failure_v1.Failure.failure_info_union, cause: ?failure_v1.Failure.failure_info_union) !Answer {
    var f: failure_v1.Failure = .{ .message = "outer", .failure_info = info };
    if (cause) |ci| {
        const inner = try arena.create(failure_v1.Failure);
        inner.* = .{ .message = "inner", .failure_info = ci };
        f.cause = inner;
    }
    return pollAnswer(arena, .{ .failure = f });
}

test "NexusOperationHandle.get polls until an outcome, past a deadline status, and classifies failures through a Nexus operation failure's cause" {
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const nexus_info: failure_v1.Failure.failure_info_union = .{ .nexus_operation_execution_failure_info = .{} };
    const answers = [_]Answer{
        .{ .rpc = poll_nexus_rpc, .proto = "", .status_code = 4, .message = "the long poll ran out of time" },
        try pollAnswer(arena, null),
        try pollAnswer(arena, .{ .result = try converter.toPayload(arena, @as([]const u8, "done")) }),
        try failureAnswer(arena, .{ .canceled_failure_info = .{} }, null),
        try failureAnswer(arena, nexus_info, .{ .canceled_failure_info = .{} }),
        try failureAnswer(arena, .{ .terminated_failure_info = .{} }, null),
        try failureAnswer(arena, .{ .timeout_failure_info = .{} }, null),
        try failureAnswer(arena, nexus_info, .{ .timeout_failure_info = .{} }),
        try failureAnswer(arena, .{ .application_failure_info = .{} }, null),
        try failureAnswer(arena, nexus_info, .{ .application_failure_info = .{} }),
        try failureAnswer(arena, nexus_info, null),
        try pollAnswer(arena, .{ .result = try converter.toPayload(arena, @as([]const u8, "again")) }),
    };
    var r: Recorded = undefined;
    try r.start(&answers);
    defer r.deinit();

    var h = try r.client.getNexusOperationHandle([]const u8, "op-1", "");
    defer h.deinit();
    try testing.expect(!h.started);
    try testing.expectEqualStrings("done", try h.get(arena));
    try testing.expect(h.failure() == null);
    for (0..3) |i| {
        const request = try r.request(wfs.PollNexusOperationExecutionRequest, arena, i);
        try testing.expectEqualStrings("op-1", request.operation_id);
        try testing.expectEqualStrings("", request.run_id);
        try testing.expectEqual(.NEXUS_OPERATION_WAIT_STAGE_CLOSED, request.wait_stage);
    }
    try testing.expectError(error.NexusOperationCanceled, h.get(arena));
    try testing.expectEqualStrings("outer", h.failure().?.message);
    try testing.expectError(error.NexusOperationCanceled, h.get(arena));
    // The stored failure is the whole one, not its cause.
    try testing.expectEqualStrings("outer", h.failure().?.message);
    try testing.expectEqualStrings("inner", h.failure().?.cause.?.message);
    try testing.expectError(error.NexusOperationTerminated, h.get(arena));
    try testing.expectError(error.NexusOperationTimedOut, h.get(arena));
    try testing.expectError(error.NexusOperationTimedOut, h.get(arena));
    try testing.expectError(error.NexusOperationFailed, h.get(arena));
    try testing.expectError(error.NexusOperationFailed, h.get(arena));
    try testing.expectError(error.NexusOperationFailed, h.get(arena));
    // Each call asks again, and a success clears the earlier failure.
    try testing.expectEqualStrings("again", try h.get(arena));
    try testing.expect(h.failure() == null);
    try testing.expectEqual(answers.len, r.server.callCount());
}

fn describeNexusAnswer(arena: Allocator, status: proto.temporal.api.enums.v1.NexusOperationExecutionStatus, cancel_requested: bool) ![]const u8 {
    return encodeMessage(arena, wfs.DescribeNexusOperationExecutionResponse{ .run_id = "run-1", .info = .{
        .operation_id = "op-1",
        .run_id = "run-1",
        .status = status,
        .cancellation_info = if (cancel_requested) .{} else null,
    } });
}

test "NexusOperationHandle cancel and terminate send the reason, and are error.NexusOperationClosed on a closed operation, success for a repeated cancel, and error.NotFound on a missing one" {
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const answers = [_]Answer{
        .{ .rpc = "RequestCancelNexusOperationExecution", .proto = try encodeMessage(arena, wfs.RequestCancelNexusOperationExecutionResponse{}) },
        .{ .rpc = "TerminateNexusOperationExecution", .proto = try encodeMessage(arena, wfs.TerminateNexusOperationExecutionResponse{}) },
        .{ .rpc = "RequestCancelNexusOperationExecution", .proto = "", .status_code = 9, .message = "operation already completed" },
        .{ .rpc = "DescribeNexusOperationExecution", .proto = try describeNexusAnswer(arena, .NEXUS_OPERATION_EXECUTION_STATUS_COMPLETED, false) },
        .{ .rpc = "TerminateNexusOperationExecution", .proto = "", .status_code = 9, .message = "already terminated" },
        .{ .rpc = "DescribeNexusOperationExecution", .proto = try describeNexusAnswer(arena, .NEXUS_OPERATION_EXECUTION_STATUS_TERMINATED, false) },
        // The same status for a running operation: a repeated cancel succeeds, anything else
        // is error.RpcFailed.
        .{ .rpc = "RequestCancelNexusOperationExecution", .proto = "", .status_code = 9, .message = "cancellation already requested" },
        .{ .rpc = "DescribeNexusOperationExecution", .proto = try describeNexusAnswer(arena, .NEXUS_OPERATION_EXECUTION_STATUS_RUNNING, true) },
        .{ .rpc = "RequestCancelNexusOperationExecution", .proto = "", .status_code = 9, .message = "not cancelable" },
        .{ .rpc = "DescribeNexusOperationExecution", .proto = try describeNexusAnswer(arena, .NEXUS_OPERATION_EXECUTION_STATUS_RUNNING, false) },
        .{ .rpc = "TerminateNexusOperationExecution", .proto = "", .status_code = 9, .message = "not terminable" },
        .{ .rpc = "DescribeNexusOperationExecution", .proto = try describeNexusAnswer(arena, .NEXUS_OPERATION_EXECUTION_STATUS_RUNNING, true) },
        .{ .rpc = "RequestCancelNexusOperationExecution", .proto = "", .status_code = 5, .message = "operation not found" },
        .{ .rpc = "TerminateNexusOperationExecution", .proto = "", .status_code = 5, .message = "operation not found" },
        .{ .rpc = "DescribeNexusOperationExecution", .proto = "", .status_code = 5, .message = "operation not found" },
    };
    var r: Recorded = undefined;
    try r.start(&answers);
    defer r.deinit();

    var h = try r.client.getNexusOperationHandle(void, "op-1", "run-1");
    defer h.deinit();
    try h.cancel("no longer needed");
    try h.terminate("stuck");
    const cancel = try r.request(wfs.RequestCancelNexusOperationExecutionRequest, arena, 0);
    try testing.expectEqualStrings("op-1", cancel.operation_id);
    try testing.expectEqualStrings("run-1", cancel.run_id);
    try testing.expectEqualStrings("no longer needed", cancel.reason);
    try testing.expectEqualStrings(r.client.identity, cancel.identity);
    try testing.expectEqual(36, cancel.request_id.len);
    const terminate = try r.request(wfs.TerminateNexusOperationExecutionRequest, arena, 1);
    try testing.expectEqualStrings("stuck", terminate.reason);
    try testing.expectEqual(36, terminate.request_id.len);
    try testing.expectError(error.NexusOperationClosed, h.cancel(""));
    try testing.expectError(error.NexusOperationClosed, h.terminate(""));
    try h.cancel("again");
    try testing.expectError(error.RpcFailed, h.cancel(""));
    try testing.expectError(error.RpcFailed, h.terminate(""));
    try testing.expectError(error.NotFound, h.cancel(""));
    try testing.expectError(error.NotFound, h.terminate(""));
    try testing.expectError(error.NotFound, h.describe(arena));
}

/// A DescribeNexusOperationExecution response for run-1 of op-1 in `status`, whose summary
/// was encoded by a codec this client doesn't have.
fn describeNexusAnswerWithForeignSummary(arena: Allocator, status: proto.temporal.api.enums.v1.NexusOperationExecutionStatus, cancel_requested: bool) ![]const u8 {
    return encodeMessage(arena, wfs.DescribeNexusOperationExecutionResponse{ .run_id = "run-1", .info = .{
        .operation_id = "op-1",
        .run_id = "run-1",
        .status = status,
        .cancellation_info = if (cancel_requested) .{} else null,
        .user_metadata = .{ .summary = try converter.payloadWithEncoding(arena, "binary/encrypted", "\x00\x01opaque") },
    } });
}

test "NexusOperationHandle cancel and terminate tell a closed operation and a repeated cancel apart without decoding the operation's payloads" {
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    // The operation was started, with a summary, by a client whose codecs this one lacks,
    // such as an operator's tool canceling another process's operation by ID.
    const answers = [_]Answer{
        .{ .rpc = "RequestCancelNexusOperationExecution", .proto = "", .status_code = 9, .message = "operation already completed" },
        .{ .rpc = "DescribeNexusOperationExecution", .proto = try describeNexusAnswerWithForeignSummary(arena, .NEXUS_OPERATION_EXECUTION_STATUS_COMPLETED, false) },
        .{ .rpc = "TerminateNexusOperationExecution", .proto = "", .status_code = 9, .message = "already terminated" },
        .{ .rpc = "DescribeNexusOperationExecution", .proto = try describeNexusAnswerWithForeignSummary(arena, .NEXUS_OPERATION_EXECUTION_STATUS_TERMINATED, false) },
        .{ .rpc = "RequestCancelNexusOperationExecution", .proto = "", .status_code = 9, .message = "cancellation already requested" },
        .{ .rpc = "DescribeNexusOperationExecution", .proto = try describeNexusAnswerWithForeignSummary(arena, .NEXUS_OPERATION_EXECUTION_STATUS_RUNNING, true) },
    };
    var r: Recorded = undefined;
    try r.start(&answers);
    defer r.deinit();

    var h = try r.client.getNexusOperationHandle(void, "op-1", "");
    defer h.deinit();
    try testing.expectError(error.NexusOperationClosed, h.cancel("after it closed"));
    try testing.expectError(error.NexusOperationClosed, h.terminate("after it closed"));
    try h.cancel("again");
}

test "NexusOperationHandle cancel and terminate read a closed operation's state whatever its record holds that the client's codecs refuse" {
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    var poisoned: common.Payload = .{ .data = "written by another codec" };
    try poisoned.metadata.append(arena, .{ .key = "encoding", .value = "binary/poison" });
    var details: common.Payloads = .{};
    try details.payloads.append(arena, poisoned);
    var attributes: common.SearchAttributes = .{};
    try attributes.indexed_fields.append(arena, .{ .key = "CustomKeywordField", .value = poisoned });
    const record = try encodeMessage(arena, wfs.DescribeNexusOperationExecutionResponse{ .run_id = "run-1", .info = .{
        .operation_id = "op-1",
        .run_id = "run-1",
        .status = .NEXUS_OPERATION_EXECUTION_STATUS_FAILED,
        .user_metadata = .{ .summary = poisoned },
        .search_attributes = attributes,
        .last_attempt_failure = .{ .message = "boom", .failure_info = .{ .application_failure_info = .{ .details = details } } },
    } });
    const codecs = [_]converter.PayloadCodec{PoisonCodec.codec()};
    var c = try Client.init(testing.allocator, testing.io, .{ .host_port = closed_port, .core_log_level = null, .codecs = &codecs });
    defer c.close();
    const answers = [_]Answer{
        .{ .rpc = "RequestCancelNexusOperationExecution", .proto = "", .status_code = 9, .message = "operation already completed" },
        .{ .rpc = "DescribeNexusOperationExecution", .proto = record },
        .{ .rpc = "TerminateNexusOperationExecution", .proto = "", .status_code = 9, .message = "operation already completed" },
        .{ .rpc = "DescribeNexusOperationExecution", .proto = record },
    };
    var server: Server = .{ .answers = &answers };
    try connectToServer(&c, &server);
    var h = try c.getNexusOperationHandle(void, "op-1", "");
    defer h.deinit();
    try testing.expectError(error.NexusOperationClosed, h.cancel(""));
    try testing.expectError(error.NexusOperationClosed, h.terminate(""));
    try testing.expectEqual(answers.len, server.callCount());
}

test "NexusOperationHandle.describe lifts the server's record" {
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const nexus_v1 = proto.temporal.api.nexus.v1;
    const info: nexus_v1.NexusOperationExecutionInfo = .{
        .operation_id = "op-1",
        .run_id = "run-1",
        .endpoint = "e",
        .service = "svc",
        .operation = "op",
        .status = .NEXUS_OPERATION_EXECUTION_STATUS_RUNNING,
        .attempt = 2,
        .schedule_time = .{ .seconds = 100 },
        .schedule_to_close_timeout = .{ .seconds = 60 },
        .operation_token = "token",
        .cancellation_info = .{ .reason = "stop" },
        .user_metadata = .{ .summary = try converter.toPayload(arena, @as([]const u8, "a summary")) },
    };
    const answers = [_]Answer{
        .{ .rpc = "DescribeNexusOperationExecution", .proto = try encodeMessage(arena, wfs.DescribeNexusOperationExecutionResponse{ .run_id = "run-1", .info = info }) },
    };
    var r: Recorded = undefined;
    try r.start(&answers);
    defer r.deinit();

    var h = try r.client.getNexusOperationHandle(void, "op-1", "");
    defer h.deinit();
    const d = try h.describe(arena);
    try testing.expectEqualStrings("op-1", d.operation_id);
    try testing.expectEqualStrings("run-1", d.run_id);
    try testing.expectEqualStrings("e", d.endpoint);
    try testing.expectEqualStrings("svc", d.service);
    try testing.expectEqualStrings("op", d.operation);
    try testing.expectEqual(client_mod.NexusOperationExecutionStatus.running, d.status);
    try testing.expectEqual(2, d.attempt);
    try testing.expectEqual(100 * std.time.ns_per_s, d.schedule_time.?.nanoseconds);
    try testing.expect(d.close_time == null);
    try testing.expectEqual(60 * std.time.ns_per_s, d.schedule_to_close_timeout.?.nanoseconds);
    try testing.expectEqualStrings("token", d.operation_token);
    try testing.expect(d.cancel_requested);
    try testing.expectEqualStrings("a summary", d.summary);
    const request = try r.request(wfs.DescribeNexusOperationExecutionRequest, arena, 0);
    try testing.expect(!request.include_input and !request.include_outcome);
}

test "listNexusOperations follows page tokens past an empty page, and countNexusOperations returns the server's count" {
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const nexus_v1 = proto.temporal.api.nexus.v1;
    var first: wfs.ListNexusOperationExecutionsResponse = .{ .next_page_token = "page-2" };
    try first.operations.append(arena, .{ .operation_id = "op-1", .endpoint = "e", .service = "svc", .operation = "op", .status = .NEXUS_OPERATION_EXECUTION_STATUS_COMPLETED, .close_time = .{ .seconds = 5 } });
    var third: wfs.ListNexusOperationExecutionsResponse = .{};
    try third.operations.append(arena, nexus_v1.NexusOperationExecutionListInfo{ .operation_id = "op-2", .status = .NEXUS_OPERATION_EXECUTION_STATUS_RUNNING });
    const answers = [_]Answer{
        .{ .rpc = "ListNexusOperationExecutions", .proto = try encodeMessage(arena, first) },
        .{ .rpc = "ListNexusOperationExecutions", .proto = try encodeMessage(arena, wfs.ListNexusOperationExecutionsResponse{ .next_page_token = "page-3" }) },
        .{ .rpc = "ListNexusOperationExecutions", .proto = try encodeMessage(arena, third) },
        .{ .rpc = "CountNexusOperationExecutions", .proto = try encodeMessage(arena, wfs.CountNexusOperationExecutionsResponse{ .count = 2 }) },
    };
    var r: Recorded = undefined;
    try r.start(&answers);
    defer r.deinit();

    var it = try r.client.listNexusOperations("Endpoint = 'e'", .{ .page_size = 1 });
    defer it.deinit();
    const one = (try it.next(arena)).?;
    try testing.expectEqualStrings("op-1", one.operation_id);
    try testing.expectEqualStrings("e", one.endpoint);
    try testing.expectEqualStrings("svc", one.service);
    try testing.expectEqualStrings("op", one.operation);
    try testing.expectEqual(client_mod.NexusOperationExecutionStatus.completed, one.status);
    try testing.expectEqual(5 * std.time.ns_per_s, one.close_time.?.nanoseconds);
    const two = (try it.next(arena)).?;
    try testing.expectEqualStrings("op-2", two.operation_id);
    try testing.expectEqual(client_mod.NexusOperationExecutionStatus.running, two.status);
    try testing.expect(try it.next(arena) == null);
    for (0..3) |i| {
        const request = try r.request(wfs.ListNexusOperationExecutionsRequest, arena, i);
        try testing.expectEqualStrings("Endpoint = 'e'", request.query);
        try testing.expectEqual(1, request.page_size);
        try testing.expectEqualStrings(([_][]const u8{ "", "page-2", "page-3" })[i], request.next_page_token);
    }

    try testing.expectEqual(2, try r.client.countNexusOperations("Endpoint = 'e'"));
    try testing.expectEqualStrings("Endpoint = 'e'", (try r.request(wfs.CountNexusOperationExecutionsRequest, arena, 3)).query);
}

test "dev server: standalone Nexus operations, a synchronous and a workflow-run one, are read through handles obtained by ID, described, listed and counted" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var endpoint: []const u8 = undefined;
    const env = try NexusFixtures.start(a, &endpoint);
    defer env.deinit();
    const c = &env.client;
    const echo_id = try ResetFixtures.uniqueName(a, "client-standalone-echo");
    const hold_id = try ResetFixtures.uniqueName(a, "client-standalone-hold");
    const held_id = try ResetFixtures.uniqueName(a, "client-standalone-held");

    var echo = try c.executeNexusOperation(NexusFixtures.Service, NexusFixtures.Echo, .{
        .id = echo_id,
        .endpoint = endpoint,
        .schedule_to_close_timeout = .fromSeconds(60),
        .summary = "echo it",
    }, "hello");
    defer echo.deinit();
    try testing.expect(echo.started);
    try testing.expect(echo.run_id.len > 0);
    var echo_by_id = try c.getNexusOperationHandle([]const u8, echo_id, "");
    defer echo_by_id.deinit();
    try testing.expectEqualStrings("hello", try echo_by_id.get(a));
    const echo_description = try echo_by_id.describe(a);
    try testing.expectEqualStrings(echo_id, echo_description.operation_id);
    try testing.expectEqualStrings(echo.run_id, echo_description.run_id);
    try testing.expectEqualStrings(endpoint, echo_description.endpoint);
    try testing.expectEqualStrings("zig-client-standalone-service", echo_description.service);
    try testing.expectEqualStrings("echo", echo_description.operation);
    try testing.expectEqual(client_mod.NexusOperationExecutionStatus.completed, echo_description.status);
    try testing.expect(echo_description.attempt >= 1);
    try testing.expect(echo_description.schedule_time != null);
    try testing.expect(echo_description.close_time != null);
    try testing.expectEqualStrings("echo it", echo_description.summary);
    try testing.expect(!echo_description.cancel_requested);

    var hold = try c.executeNexusOperation(NexusFixtures.Service, NexusFixtures.Hold, .{ .id = hold_id, .endpoint = endpoint, .schedule_to_close_timeout = .fromSeconds(600) }, held_id);
    defer hold.deinit();
    defer c.terminateWorkflow(held_id, "", "test cleanup") catch {};
    try NexusFixtures.awaitStarted(&hold);
    try testing.expectEqual(client_mod.NexusOperationExecutionStatus.running, (try hold.describe(a)).status);
    try c.signalWorkflow(ResetFixtures.Finish, held_id, "", .{});
    var hold_by_id = try c.getNexusOperationHandle([]const u8, hold_id, hold.run_id);
    defer hold_by_id.deinit();
    const expected = try std.fmt.allocPrint(a, "released {s}", .{held_id});
    try testing.expectEqualStrings(expected, try hold_by_id.get(a));
    const hold_description = try hold_by_id.describe(a);
    try testing.expectEqual(client_mod.NexusOperationExecutionStatus.completed, hold_description.status);
    try testing.expect(hold_description.operation_token.len > 0);

    // Visibility catches up eventually: wait for both, for at most 30 s.
    const query = try std.fmt.allocPrint(a, "Endpoint = '{s}'", .{endpoint});
    const deadline = std.Io.Timestamp.now(testing.io, .awake).addDuration(.fromSeconds(30));
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    while (true) {
        _ = scratch.reset(.retain_capacity);
        var seen_echo = false;
        var seen_hold = false;
        var listed: usize = 0;
        var it = try c.listNexusOperations(query, .{});
        defer it.deinit();
        while (try it.next(scratch.allocator())) |info| {
            listed += 1;
            if (info.status != .completed) continue;
            if (std.mem.eql(u8, info.operation_id, echo_id)) {
                try testing.expectEqualStrings("echo", info.operation);
                seen_echo = true;
            }
            if (std.mem.eql(u8, info.operation_id, hold_id)) {
                try testing.expectEqualStrings("hold", info.operation);
                seen_hold = true;
            }
        }
        if (seen_echo and seen_hold and listed == 2 and try c.countNexusOperations(query) == 2) break;
        if (std.Io.Timestamp.now(testing.io, .awake).nanoseconds >= deadline.nanoseconds) {
            std.debug.print("after 30 s the query {s} listed {d} operations, echo {}, hold {}\n", .{ query, listed, seen_echo, seen_hold });
            return error.TestOperationsNotListed;
        }
        try testing.io.sleep(.fromMilliseconds(100), .awake);
    }
}

test "dev server: standalone Nexus operations, a canceled workflow-run operation ends canceled" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var endpoint: []const u8 = undefined;
    const env = try NexusFixtures.start(a, &endpoint);
    defer env.deinit();
    const c = &env.client;
    const hold_id = try ResetFixtures.uniqueName(a, "client-standalone-cancel");
    const held_id = try ResetFixtures.uniqueName(a, "client-standalone-cancel-held");

    var hold = try c.executeNexusOperation(NexusFixtures.Service, NexusFixtures.Hold, .{ .id = hold_id, .endpoint = endpoint, .schedule_to_close_timeout = .fromSeconds(600) }, held_id);
    defer hold.deinit();
    defer c.terminateWorkflow(held_id, "", "test cleanup") catch {};
    try NexusFixtures.awaitStarted(&hold);
    var by_id = try c.getNexusOperationHandle([]const u8, hold_id, "");
    defer by_id.deinit();
    try by_id.cancel("no longer needed");
    try testing.expectError(error.NexusOperationCanceled, by_id.get(a));
    try testing.expect(by_id.failure() != null);
    const d = try by_id.describe(a);
    try testing.expectEqual(client_mod.NexusOperationExecutionStatus.canceled, d.status);
    try testing.expect(d.cancel_requested);
    try testing.expectEqual(client_mod.WorkflowExecutionStatus.canceled, (try c.describeWorkflow(a, held_id, "")).status);
}

test "dev server: standalone Nexus operations, terminate, a closed operation, an ID conflict and an unknown endpoint" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var endpoint: []const u8 = undefined;
    const env = try NexusFixtures.start(a, &endpoint);
    defer env.deinit();
    const c = &env.client;
    // Nothing polls this endpoint's task queue, so its operations stay unstarted.
    const unpolled = try ResetFixtures.uniqueName(a, "client-standalone-unpolled");
    try env.createNexusEndpoint(unpolled, .{ .task_queue = try ResetFixtures.uniqueName(a, "client-standalone-nobody") });
    const id = try ResetFixtures.uniqueName(a, "client-standalone-terminate");

    var first = try c.executeNexusOperation(NexusFixtures.Service, NexusFixtures.Echo, .{ .id = id, .endpoint = unpolled }, "never");
    defer first.deinit();
    try testing.expect(first.started);
    try testing.expectError(error.NexusOperationExecutionAlreadyStarted, c.executeNexusOperation(NexusFixtures.Service, NexusFixtures.Echo, .{ .id = id, .endpoint = unpolled, .id_conflict_policy = .fail }, "again"));
    var existing = try c.executeNexusOperation(NexusFixtures.Service, NexusFixtures.Echo, .{ .id = id, .endpoint = unpolled, .id_conflict_policy = .use_existing }, "again");
    defer existing.deinit();
    try testing.expect(!existing.started);
    try testing.expectEqualStrings(first.run_id, existing.run_id);

    // A cancel the handler can't receive leaves the operation running, with the request recorded.
    try existing.cancel("before it started");
    const pending = try existing.describe(a);
    try testing.expectEqual(client_mod.NexusOperationExecutionStatus.running, pending.status);
    try testing.expect(pending.cancel_requested);

    try first.terminate("test");
    try testing.expectError(error.NexusOperationTerminated, first.get(a));
    try testing.expect(first.failure() != null);
    try testing.expectEqual(client_mod.NexusOperationExecutionStatus.terminated, (try first.describe(a)).status);
    try testing.expectError(error.NexusOperationClosed, first.cancel("after it closed"));
    try testing.expectError(error.NexusOperationClosed, first.terminate("after it closed"));

    var missing = try c.getNexusOperationHandle(void, "client-standalone-no-such-operation", "");
    defer missing.deinit();
    try testing.expectError(error.NotFound, missing.describe(a));
    try testing.expectError(error.NotFound, missing.get(a));
    try testing.expectError(error.NotFound, missing.cancel(""));

    try testing.expectError(error.NotFound, c.executeNexusOperation(NexusFixtures.Service, NexusFixtures.Echo, .{ .endpoint = "client-standalone-no-such-endpoint" }, "x"));
}

test "dev server: standalone Nexus operations, a second cancel of a running operation is not error.NexusOperationClosed" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var endpoint: []const u8 = undefined;
    const env = try NexusFixtures.start(a, &endpoint);
    defer env.deinit();
    const c = &env.client;
    // Nothing polls this endpoint's task queue, so the operation stays running with its
    // cancellation requested but undelivered.
    const unpolled = try ResetFixtures.uniqueName(a, "client-standalone-recancel");
    try env.createNexusEndpoint(unpolled, .{ .task_queue = try ResetFixtures.uniqueName(a, "client-standalone-recancel-nobody") });
    var h = try c.executeNexusOperation(NexusFixtures.Service, NexusFixtures.Echo, .{ .endpoint = unpolled }, "never");
    defer h.deinit();
    defer h.terminate("test cleanup") catch {};

    try h.cancel("first");
    // The server refuses a repeated cancel with FAILED_PRECONDITION ("cancellation already
    // requested"), the status it also gives a closed operation; this one is still running.
    if (h.cancel("second")) |_| {} else |err| try testing.expect(err != error.NexusOperationClosed);
    const d = try h.describe(a);
    try testing.expectEqual(client_mod.NexusOperationExecutionStatus.running, d.status);
    try testing.expect(d.cancel_requested);
}

/// A config extension for the tests below: it records what `loadOptions` gave it, and sets
/// `Options.api_key` from its table's `api_key`, or from the variable its table's `api_key_env`
/// names, or fails with `error.NoApiKey` when its table has `fail = true`.
const RecordingExtension = struct {
    calls: usize = 0,
    profile: []const u8 = "",
    had_table: bool = false,
    had_env: bool = false,
    seen_api_key: []const u8 = "",

    fn extension(r: *RecordingExtension, name: []const u8) client_mod.ConfigExtension {
        return .{ .name = name, .ptr = r, .apply = apply };
    }

    fn apply(ptr: ?*anyopaque, input: client_mod.ConfigExtensionInput, options: *client_mod.Options) anyerror!void {
        const r: *RecordingExtension = @ptrCast(@alignCast(ptr.?));
        r.calls += 1;
        r.profile = input.profile;
        r.had_table = input.table != null;
        r.had_env = input.env != null;
        r.seen_api_key = options.api_key;
        const table = input.table orelse return;
        if (table.get("fail")) |f| if (f == .bool and f.bool) return error.NoApiKey;
        if (table.get("api_key")) |v| options.api_key = try input.arena.dupe(u8, v.string);
        if (table.get("api_key_env")) |v| {
            const value = (input.env orelse return error.NoEnvironment).get(v.string) orelse return error.NoApiKey;
            options.api_key = try input.arena.dupe(u8, value);
        }
    }
};

test "a config extension sets the API key from its own ext table" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var secrets: RecordingExtension = .{};
    var other: RecordingExtension = .{};
    const options = try client_mod.loadOptions(arena.allocator(), .{
        .disable_env = true,
        .profile = "prod",
        .config_data =
        \\[profile.prod]
        \\address = "prod.example.com:7233"
        \\api_key = "from-profile"
        \\[profile.prod.ext.secrets]
        \\api_key = "from-extension"
        \\rotated = 1979-05-27T07:32:00Z
        \\[profile.prod.ext.other]
        \\api_key = "from-other"
        ,
        .extensions = &.{ secrets.extension("secrets"), other.extension("other") },
    });
    try testing.expectEqualStrings("from-other", options.api_key);
    try testing.expectEqualStrings("prod.example.com:7233", options.host_port);
    try testing.expect(options.tls != null);
    try testing.expectEqual(1, secrets.calls);
    try testing.expectEqualStrings("prod", secrets.profile);
    try testing.expect(secrets.had_table);
    try testing.expect(!secrets.had_env);
    // Extensions run in order, each on the options the previous one left.
    try testing.expectEqualStrings("from-profile", secrets.seen_api_key);
    try testing.expectEqualStrings("from-extension", other.seen_api_key);
}

test "a config extension's table holds parsed values, datetimes as strings" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const Check = struct {
        fn apply(_: ?*anyopaque, input: client_mod.ConfigExtensionInput, _: *client_mod.Options) anyerror!void {
            const table = input.table orelse return error.NoTable;
            try testing.expectEqualStrings("value", table.get("str").?.string);
            try testing.expectEqual(5, table.get("int").?.integer);
            try testing.expect(table.get("flag").?.bool);
            try testing.expectEqualStrings("1979-05-27T07:32:00Z", table.get("when").?.string);
            try testing.expectEqual(2, table.get("list").?.array.items.len);
            try testing.expectEqualStrings("inner", table.get("nested").?.object.get("key").?.string);
        }
    };
    _ = try client_mod.loadOptions(arena.allocator(), .{
        .disable_env = true,
        .config_data =
        \\[profile.default.ext.check]
        \\str = "value"
        \\int = 5
        \\flag = true
        \\when = 1979-05-27T07:32:00Z
        \\list = [1, "two"]
        \\nested = { key = "inner" }
        ,
        .extensions = &.{.{ .name = "check", .apply = Check.apply }},
    });
}

test "a config extension whose profile has no table for it gets null" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var secrets: RecordingExtension = .{};
    const options = try client_mod.loadOptions(arena.allocator(), .{
        .disable_env = true,
        .config_data =
        \\[profile.default]
        \\api_key = "from-profile"
        \\[profile.default.ext.someone_else]
        \\api_key = "not-ours"
        ,
        .extensions = &.{secrets.extension("secrets")},
    });
    try testing.expectEqual(1, secrets.calls);
    try testing.expect(!secrets.had_table);
    try testing.expectEqualStrings("default", secrets.profile);
    try testing.expectEqualStrings("from-profile", options.api_key);

    // No config file at all: the extension still runs, with no table.
    var empty: std.process.Environ.Map = .init(testing.allocator);
    defer empty.deinit();
    var env_only: RecordingExtension = .{};
    _ = try client_mod.loadOptions(arena.allocator(), .{ .env = &empty, .disable_file = true, .extensions = &.{env_only.extension("secrets")} });
    try testing.expectEqual(1, env_only.calls);
    try testing.expect(!env_only.had_table);
    try testing.expect(env_only.had_env);
}

test "strict loadOptions accepts keys under ext and still rejects other unknown keys" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var secrets: RecordingExtension = .{};
    const options = try client_mod.loadOptions(arena.allocator(), .{
        .disable_env = true,
        .strict = true,
        .config_data =
        \\[profile.default]
        \\address = "localhost:7233"
        \\[profile.default.ext.secrets]
        \\api_key = "from-extension"
        \\anything = { goes = "here" }
        \\[profile.default.ext.not_loaded]
        \\whatever = 1
        ,
        .extensions = &.{secrets.extension("secrets")},
    });
    try testing.expectEqualStrings("from-extension", options.api_key);

    // Without the extension, strict still accepts the tables.
    _ = try client_mod.loadOptions(arena.allocator(), .{ .disable_env = true, .strict = true, .config_data =
        \\[profile.default.ext.not_loaded]
        \\whatever = 1
    });

    var never: RecordingExtension = .{};
    try testing.expectError(error.InvalidConfig, client_mod.loadOptions(arena.allocator(), .{
        .disable_env = true,
        .strict = true,
        .config_data =
        \\[profile.default]
        \\adress = "typo:7233"
        \\[profile.default.ext.secrets]
        \\api_key = "from-extension"
        ,
        .extensions = &.{never.extension("secrets")},
    }));
    // A bad core config fails before any extension runs.
    try testing.expectEqual(0, never.calls);
}

test "a config extension that fails fails loadOptions, and later extensions don't run" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var failing: RecordingExtension = .{};
    var after: RecordingExtension = .{};
    try testing.expectError(error.InvalidConfig, client_mod.loadOptions(arena.allocator(), .{
        .disable_env = true,
        .config_data =
        \\[profile.default.ext.failing]
        \\fail = true
        ,
        .extensions = &.{ failing.extension("failing"), after.extension("after") },
    }));
    try testing.expectEqual(1, failing.calls);
    try testing.expectEqual(0, after.calls);

    const OutOfMemory = struct {
        fn apply(_: ?*anyopaque, _: client_mod.ConfigExtensionInput, _: *client_mod.Options) anyerror!void {
            return error.OutOfMemory;
        }
    };
    try testing.expectError(error.OutOfMemory, client_mod.loadOptions(arena.allocator(), .{
        .disable_env = true,
        .config_data = "",
        .extensions = &.{.{ .name = "oom", .apply = OutOfMemory.apply }},
    }));
}

test "config extensions with an empty or repeated name fail loadOptions before any runs" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var a: RecordingExtension = .{};
    var b: RecordingExtension = .{};
    try testing.expectError(error.InvalidConfig, client_mod.loadOptions(arena.allocator(), .{
        .disable_env = true,
        .config_data = "",
        .extensions = &.{ a.extension("secrets"), b.extension("") },
    }));
    try testing.expectError(error.InvalidConfig, client_mod.loadOptions(arena.allocator(), .{
        .disable_env = true,
        .config_data = "",
        .extensions = &.{ a.extension("secrets"), b.extension("other"), b.extension("secrets") },
    }));
    try testing.expectEqual(0, a.calls);
    try testing.expectEqual(0, b.calls);
}

test "a config extension reads LoadOptions.env, and none with disable_env" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var env: std.process.Environ.Map = .init(testing.allocator);
    defer env.deinit();
    try env.put("TEMPORAL_PROFILE", "staging");
    try env.put("MY_SECRET", "from-env");
    const toml =
        \\[profile.staging.ext.secrets]
        \\api_key_env = "MY_SECRET"
        \\[profile.default.ext.secrets]
        \\api_key_env = "MY_SECRET"
    ;
    var secrets: RecordingExtension = .{};
    const options = try client_mod.loadOptions(arena.allocator(), .{ .env = &env, .config_data = toml, .extensions = &.{secrets.extension("secrets")} });
    try testing.expectEqualStrings("from-env", options.api_key);
    try testing.expectEqualStrings("staging", secrets.profile);
    try testing.expect(secrets.had_env);

    // With disable_env the extension gets no environment, and TEMPORAL_PROFILE isn't read.
    var no_env: RecordingExtension = .{};
    try testing.expectError(error.InvalidConfig, client_mod.loadOptions(arena.allocator(), .{ .env = &env, .config_data = toml, .disable_env = true, .extensions = &.{no_env.extension("secrets")} }));
    try testing.expectEqualStrings("default", no_env.profile);
    try testing.expect(!no_env.had_env);
}

test "ConfigEnv reads a map, or the process environment without one" {
    var env: std.process.Environ.Map = .init(testing.allocator);
    defer env.deinit();
    try env.put("SOME_VAR", "x");
    const from_map: client_mod.ConfigEnv = .{ .map = &env };
    try testing.expectEqualStrings("x", from_map.get("SOME_VAR").?);
    try testing.expect(from_map.get("SOME") == null);

    const process: client_mod.ConfigEnv = .{ .map = null };
    const path = std.mem.span(std.c.getenv("PATH") orelse return error.SkipZigTest);
    try testing.expectEqualStrings(path, process.get("PATH").?);
    try testing.expect(process.get("PAT") == null);
    try testing.expect(process.get("temporal-sdk-zig no such variable") == null);
}

// ---------------------------------------------------------------- Nexus contexts of client reads

const failure_mod = @import("failure.zig");
const nexus_system = @import("nexus/system.zig");

/// A codec for the Nexus endpoint `endpoint`. It decodes a payload of encoding `binary/nexus`
/// only with a context whose Nexus endpoint is `endpoint`, giving it encoding `json/plain`,
/// and any other payload only with a context without Nexus; otherwise it fails with
/// `error.CodecFailed`. It records each payload's context: `nexus=<endpoint>`,
/// `wf=<workflow ID>` or `act=<activity ID>`.
const NexusEndpointCodec = struct {
    arena: Allocator,
    endpoint: []const u8,
    seen: std.ArrayList([]const u8) = .empty,

    fn codec(r: *NexusEndpointCodec) converter.PayloadCodec {
        return .{ .ptr = r, .vtable = &.{ .encode = plain, .decode = plain, .decodeWithContext = decode } };
    }

    fn plain(_: *anyopaque, arena: Allocator, _: std.Io, payloads: []const converter.Payload) converter.CodecError![]converter.Payload {
        return arena.dupe(converter.Payload, payloads);
    }

    fn decode(ptr: *anyopaque, arena: Allocator, _: std.Io, context: *const converter.Context, payloads: []const converter.Payload) converter.CodecError![]converter.Payload {
        const r: *NexusEndpointCodec = @ptrCast(@alignCast(ptr));
        const out = try arena.dupe(converter.Payload, payloads);
        for (out) |*p| {
            try r.seen.append(r.arena, try contextLabel(r.arena, context));
            const marked = std.mem.eql(u8, converter.encodingOf(p.*) orelse "", "binary/nexus");
            const ours = if (context.nexus) |n| std.mem.eql(u8, n.endpoint, r.endpoint) else false;
            if (marked != ours) return error.CodecFailed;
            if (marked) p.* = try withEncoding(arena, p.*, "json/plain");
        }
        return out;
    }

    fn count(r: *const NexusEndpointCodec, label: []const u8) usize {
        var n: usize = 0;
        for (r.seen.items) |s| n += @intFromBool(std.mem.eql(u8, s, label));
        return n;
    }
};

fn contextLabel(arena: Allocator, context: *const converter.Context) error{OutOfMemory}![]const u8 {
    if (context.nexus) |n| return std.fmt.allocPrint(arena, "nexus={s}", .{n.endpoint});
    if (context.activity_id.len > 0) return std.fmt.allocPrint(arena, "act={s}", .{context.activity_id});
    return std.fmt.allocPrint(arena, "wf={s}", .{context.workflow_id});
}

/// A failure converter that records the context of each failure it is given, as
/// `contextLabel` writes it, and returns the failure as it is.
const RecordingFailureConverter = struct {
    arena: Allocator,
    seen: std.ArrayList([]const u8) = .empty,

    fn converter_(r: *RecordingFailureConverter) converter.FailureConverter {
        return .{ .ptr = r, .vtable = &.{ .toFailure = toFailure, .fromFailure = fromFailure } };
    }

    fn toFailure(_: *anyopaque, _: Allocator, _: *const converter.Context, _: ?anyerror, f: failure_v1.Failure) error{OutOfMemory}!failure_v1.Failure {
        return f;
    }

    fn fromFailure(ptr: *anyopaque, _: Allocator, context: *const converter.Context, f: failure_v1.Failure) error{OutOfMemory}!failure_v1.Failure {
        const r: *RecordingFailureConverter = @ptrCast(@alignCast(ptr));
        try r.seen.append(r.arena, try contextLabel(r.arena, context));
        return f;
    }
};

fn withEncoding(arena: Allocator, p: converter.Payload, encoding: []const u8) error{OutOfMemory}!converter.Payload {
    var out: converter.Payload = .{ .data = p.data };
    try out.metadata.append(arena, .{ .key = "encoding", .value = encoding });
    return out;
}

/// The JSON string `text` as a payload a codec for the endpoint must decode (`nexus`) or as
/// a plain one.
fn stringPayload(arena: Allocator, text: []const u8, nexus_encoded: bool) !converter.Payload {
    const p = try converter.toPayload(arena, text);
    return if (nexus_encoded) withEncoding(arena, p, "binary/nexus") else p;
}

/// An application failure whose message is in its encoded attributes, which are encoded for
/// the endpoint (`nexus`) or plain.
fn encodedFailure(arena: Allocator, message: []const u8, nexus_encoded: bool) !failure_v1.Failure {
    var f: failure_v1.Failure = .{ .message = message, .failure_info = .{ .application_failure_info = .{} } };
    try failure_mod.encodeAttributes(failure_v1.Failure, &f, arena);
    if (nexus_encoded) f.encoded_attributes = try withEncoding(arena, f.encoded_attributes.?, "binary/nexus");
    return f;
}

const caller_context: proto.temporal.api.nexus.v1.PropagatedSerializationContext = .{ .endpoint = "caller-ep", .service = "caller-svc", .operation = "caller-op" };

const NexusReadFixture = struct {
    arena_state: std.heap.ArenaAllocator,
    codec: NexusEndpointCodec,
    failures: RecordingFailureConverter,
    codecs: [1]converter.PayloadCodec,
    client: Client,
    server: Server,

    fn init(f: *NexusReadFixture, answers: []const Answer) !void {
        f.arena_state = .init(testing.allocator);
        errdefer f.arena_state.deinit();
        const a = f.arena_state.allocator();
        f.codec = .{ .arena = a, .endpoint = caller_context.endpoint };
        f.failures = .{ .arena = a };
        f.codecs = .{f.codec.codec()};
        f.client = try Client.init(testing.allocator, testing.io, .{ .host_port = closed_port, .core_log_level = null, .codecs = &f.codecs, .failure_converter = f.failures.converter_() });
        errdefer f.client.close();
        f.server = .{ .answers = answers };
        try connectToServer(&f.client, &f.server);
    }

    fn deinit(f: *NexusReadFixture) void {
        f.client.close();
        f.arena_state.deinit();
    }
};

fn updateOutcome(arena: Allocator, success: bool, nexus_encoded: bool) !proto.temporal.api.update.v1.Outcome {
    if (!success) return .{ .value = .{ .failure = try encodedFailure(arena, "update failed", nexus_encoded) } };
    var payloads: common.Payloads = .{};
    try payloads.payloads.append(arena, try stringPayload(arena, "done", nexus_encoded));
    return .{ .value = .{ .success = payloads } };
}

fn updateResponse(arena: Allocator, success: bool, propagated: bool) !wfs.UpdateWorkflowExecutionResponse {
    return .{
        .update_ref = .{ .workflow_execution = .{ .workflow_id = "wf-u", .run_id = "run-1" }, .update_id = "upd-1" },
        .outcome = try updateOutcome(arena, success, propagated),
        .stage = .UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED,
        .propagated_nexus_serialization_context = if (propagated) caller_context else null,
    };
}

fn pollUpdateAnswer(arena: Allocator, success: bool, propagated: bool) !Answer {
    return .{ .rpc = "PollWorkflowExecutionUpdate", .proto = try encodeMessage(arena, wfs.PollWorkflowExecutionUpdateResponse{
        .outcome = try updateOutcome(arena, success, propagated),
        .stage = .UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED,
        .propagated_nexus_serialization_context = if (propagated) caller_context else null,
    }) };
}

fn multiOperationAnswer(arena: Allocator, success: bool, propagated: bool) !Answer {
    var response: wfs.ExecuteMultiOperationResponse = .{};
    try response.responses.append(arena, .{ .response = .{ .start_workflow = .{ .run_id = "run-1", .started = true } } });
    try response.responses.append(arena, .{ .response = .{ .update_workflow = try updateResponse(arena, success, propagated) } });
    return .{ .rpc = "ExecuteMultiOperation", .proto = try encodeMessage(arena, response) };
}

fn pollActivityAnswer(arena: Allocator, success: bool, propagated: bool) !Answer {
    const outcome: proto.temporal.api.activity.v1.ActivityExecutionOutcome = if (success) blk: {
        var payloads: common.Payloads = .{};
        try payloads.payloads.append(arena, try stringPayload(arena, "done", propagated));
        break :blk .{ .value = .{ .result = payloads } };
    } else .{ .value = .{ .failure = try encodedFailure(arena, "activity failed", propagated) } };
    return .{ .rpc = "PollActivityExecution", .proto = try encodeMessage(arena, wfs.PollActivityExecutionResponse{
        .run_id = "act-run",
        .outcome = outcome,
        .propagated_nexus_serialization_context = if (propagated) caller_context else null,
    }) };
}

test "describeWorkflow decodes a pending Nexus operation's last failures with the operation's context, and the rest with the workflow's" {
    testing.log_level = .err;
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    var memo: common.Memo = .{};
    try memo.fields.append(a, .{ .key = "note", .value = try stringPayload(a, "memo", false) });
    var response: wfs.DescribeWorkflowExecutionResponse = .{ .workflow_execution_info = .{
        .execution = .{ .workflow_id = "wf-d", .run_id = "run-1" },
        .status = .WORKFLOW_EXECUTION_STATUS_RUNNING,
        .memo = memo,
    } };
    try response.pending_nexus_operations.append(a, .{ .endpoint = nexus_system.endpoint, .service = "s", .operation = "o", .last_attempt_failure = try encodedFailure(a, "system failed", false) });
    try response.pending_nexus_operations.append(a, .{
        .endpoint = caller_context.endpoint,
        .service = caller_context.service,
        .operation = caller_context.operation,
        .last_attempt_failure = try encodedFailure(a, "start failed", true),
        .cancellation_info = .{ .last_attempt_failure = try encodedFailure(a, "cancel failed", true) },
    });
    const answers = [_]Answer{.{ .rpc = describe_rpc, .proto = try encodeMessage(a, response) }};
    var f: NexusReadFixture = undefined;
    try f.init(&answers);
    defer f.deinit();

    const d = try f.client.describeWorkflow(a, "wf-d", "run-1");
    const ops = d.raw.pending_nexus_operations.items;
    try testing.expectEqual(@as(usize, 2), ops.len);
    try testing.expectEqualStrings(nexus_system.endpoint, ops[0].endpoint);
    try testing.expectEqualStrings("system failed", ops[0].last_attempt_failure.?.message);
    try testing.expectEqualStrings(caller_context.endpoint, ops[1].endpoint);
    try testing.expectEqualStrings("start failed", ops[1].last_attempt_failure.?.message);
    try testing.expect(ops[1].last_attempt_failure.?.encoded_attributes == null);
    try testing.expectEqualStrings("cancel failed", ops[1].cancellation_info.?.last_attempt_failure.?.message);
    try testing.expectEqualStrings("memo", (try d.memoValue([]const u8, a, "note")).?);
    // Each payload is decoded once: the operation's two with its context, the system
    // operation's and the memo with the workflow's.
    try testing.expectEqual(@as(usize, 2), f.codec.count("nexus=caller-ep"));
    try testing.expectEqual(@as(usize, 2), f.codec.count("wf=wf-d"));
    try testing.expectEqual(@as(usize, 4), f.codec.seen.items.len);
}

test "an update handle from updateWorkflowByName decodes its result and failure with the propagated Nexus context" {
    testing.log_level = .err;
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const answers = [_]Answer{
        .{ .rpc = "UpdateWorkflowExecution", .proto = try encodeMessage(a, try updateResponse(a, true, true)) },
        .{ .rpc = "UpdateWorkflowExecution", .proto = try encodeMessage(a, try updateResponse(a, false, true)) },
    };
    var f: NexusReadFixture = undefined;
    try f.init(&answers);
    defer f.deinit();

    var ok = try f.client.updateWorkflowByName([]const u8, "upd", "wf-u", "run-1", .{}, .{});
    defer ok.deinit();
    try testing.expectEqualStrings(caller_context.operation, ok.outcome_nexus_context.?.operation);
    try testing.expectEqualStrings("done", try ok.get(a));

    var failed = try f.client.updateWorkflowByName([]const u8, "upd", "wf-u", "run-1", .{}, .{});
    defer failed.deinit();
    try testing.expectError(error.UpdateFailed, failed.get(a));
    try testing.expectEqualStrings("update failed", failed.failure().?.message);
    try testing.expectEqual(@as(usize, 1), f.failures.seen.items.len);
    try testing.expectEqualStrings("nexus=caller-ep", f.failures.seen.items[0]);
}

test "an update handle from getWorkflowUpdateHandle decodes a polled result and failure with the propagated Nexus context" {
    testing.log_level = .err;
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const answers = [_]Answer{ try pollUpdateAnswer(a, true, true), try pollUpdateAnswer(a, false, true) };
    var f: NexusReadFixture = undefined;
    try f.init(&answers);
    defer f.deinit();

    var ok = try f.client.getWorkflowUpdateHandle([]const u8, "wf-u", "run-1", "upd-1");
    defer ok.deinit();
    try testing.expect(ok.outcome_nexus_context == null);
    try testing.expectEqualStrings("done", try ok.get(a));

    var failed = try f.client.getWorkflowUpdateHandle([]const u8, "wf-u", "run-1", "upd-1");
    defer failed.deinit();
    try testing.expectError(error.UpdateFailed, failed.get(a));
    try testing.expectEqualStrings("update failed", failed.failure().?.message);
    try testing.expectEqualStrings("nexus=caller-ep", f.failures.seen.items[0]);
}

test "an update-with-start handle decodes its result and failure with the propagated Nexus context of the update's response" {
    testing.log_level = .err;
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const answers = [_]Answer{ try multiOperationAnswer(a, true, true), try multiOperationAnswer(a, false, true) };
    var f: NexusReadFixture = undefined;
    try f.init(&answers);
    defer f.deinit();
    const start: client_mod.StartWorkflowOptions = .{ .id = "wf-u", .task_queue = "tq", .id_conflict_policy = .use_existing };

    var ok = try f.client.updateWithStartWorkflowByName([]const u8, "Wf", start, .{}, "upd", .{}, .{});
    defer ok.deinit();
    try testing.expectEqualStrings("done", try ok.get(a));

    var failed = try f.client.updateWithStartWorkflowByName([]const u8, "Wf", start, .{}, "upd", .{}, .{});
    defer failed.deinit();
    try testing.expectError(error.UpdateFailed, failed.get(a));
    try testing.expectEqualStrings("update failed", failed.failure().?.message);
    try testing.expectEqualStrings("nexus=caller-ep", f.failures.seen.items[0]);
}

test "a standalone activity handle decodes its result and failure with the propagated Nexus context" {
    testing.log_level = .err;
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const answers = [_]Answer{ try pollActivityAnswer(a, true, true), try pollActivityAnswer(a, false, true) };
    var f: NexusReadFixture = undefined;
    try f.init(&answers);
    defer f.deinit();

    var ok = try f.client.getActivityHandle([]const u8, "act-1", "");
    defer ok.deinit();
    try testing.expectEqualStrings("done", try ok.get(a));

    var failed = try f.client.getActivityHandle([]const u8, "act-1", "");
    defer failed.deinit();
    try testing.expectError(error.ActivityFailed, failed.get(a));
    try testing.expectEqualStrings("activity failed", failed.failure().?.message);
    try testing.expectEqualStrings("nexus=caller-ep", f.failures.seen.items[0]);
}

test "update and standalone activity handles decode with the workflow's or activity's context when the server sends no propagated Nexus context" {
    testing.log_level = .err;
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const answers = [_]Answer{
        .{ .rpc = "UpdateWorkflowExecution", .proto = try encodeMessage(a, try updateResponse(a, true, false)) },
        try pollUpdateAnswer(a, false, false),
        try multiOperationAnswer(a, true, false),
        try pollActivityAnswer(a, true, false),
        try pollActivityAnswer(a, false, false),
    };
    var f: NexusReadFixture = undefined;
    try f.init(&answers);
    defer f.deinit();

    var updated = try f.client.updateWorkflowByName([]const u8, "upd", "wf-u", "run-1", .{}, .{});
    defer updated.deinit();
    try testing.expect(updated.outcome_nexus_context == null);
    try testing.expectEqualStrings("done", try updated.get(a));

    var polled = try f.client.getWorkflowUpdateHandle([]const u8, "wf-u", "run-1", "upd-1");
    defer polled.deinit();
    try testing.expectError(error.UpdateFailed, polled.get(a));
    try testing.expectEqualStrings("update failed", polled.failure().?.message);

    var with_start = try f.client.updateWithStartWorkflowByName([]const u8, "Wf", .{ .id = "wf-u", .task_queue = "tq", .id_conflict_policy = .use_existing }, .{}, "upd", .{}, .{});
    defer with_start.deinit();
    try testing.expectEqualStrings("done", try with_start.get(a));

    var activity = try f.client.getActivityHandle([]const u8, "act-1", "");
    defer activity.deinit();
    try testing.expectEqualStrings("done", try activity.get(a));
    var failed_activity = try f.client.getActivityHandle([]const u8, "act-1", "");
    defer failed_activity.deinit();
    try testing.expectError(error.ActivityFailed, failed_activity.get(a));

    // Five payloads, none decoded with a Nexus context. (A polled update's codec context
    // names no workflow: its request names it only inside `update_ref`.)
    try testing.expectEqual(@as(usize, 5), f.codec.seen.items.len);
    for (f.codec.seen.items) |label| try testing.expect(!std.mem.startsWith(u8, label, "nexus="));
    try testing.expectEqual(@as(usize, 2), f.codec.count("wf=wf-u"));
    try testing.expectEqual(@as(usize, 2), f.codec.count("act=act-1"));
    try testing.expectEqual(@as(usize, 2), f.failures.seen.items.len);
    try testing.expectEqualStrings("wf=wf-u", f.failures.seen.items[0]);
    try testing.expectEqualStrings("act=act-1", f.failures.seen.items[1]);
}
