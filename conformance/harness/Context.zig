//! Borrowed for the task executing one feature. Activities must use their own context.
const Context = @This();

const std = @import("std");
const temporal = @import("temporal");
const history = @import("history.zig");

/// Thread-safe allocator for values that outlive a call.
gpa: std.mem.Allocator,
io: std.Io,
/// Task-local allocator whose values live through teardown.
arena: std.mem.Allocator,
/// Shared with the worker and closed by the harness.
client: *temporal.client.Client,
feature: []const u8,
namespace: []const u8,
task_queue: []const u8,
failure_message_: ?[]const u8 = null,

pub fn startOptions(ctx: *const Context) temporal.client.StartWorkflowOptions {
    return .{ .id = ctx.task_queue, .task_queue = ctx.task_queue, .execution_timeout = .fromSeconds(60) };
}

pub fn fail(ctx: *Context, comptime fmt: []const u8, args: anytype) error{FeatureFailed} {
    const message = std.fmt.allocPrint(ctx.arena, fmt, args) catch "out of memory formatting feature failure";
    ctx.failure_message_ = if (message.len == 0) "feature failed without a message" else message;
    return error.FeatureFailed;
}

pub fn expect(ctx: *Context, ok: bool, comptime fmt: []const u8, args: anytype) error{FeatureFailed}!void {
    if (!ok) return ctx.fail(fmt, args);
}

pub fn expectEqualStrings(ctx: *Context, expected: []const u8, actual: []const u8) error{FeatureFailed}!void {
    if (!std.mem.eql(u8, expected, actual))
        return ctx.fail("expected \"{f}\", got \"{f}\"", .{ std.zig.fmtString(expected), std.zig.fmtString(actual) });
}

/// Reads all recorded events, with payloads decoded by the client's codecs.
/// The result lives in the feature arena. An empty run ID selects the latest run.
pub fn fetchHistory(ctx: *Context, workflow_id: []const u8, run_id: []const u8) temporal.client.HistoryError!temporal.history.History {
    return ctx.client.fetchHistory(ctx.arena, workflow_id, run_id);
}

/// Reads all recorded events with their stored payloads. The result lives in the feature
/// arena. An empty run ID selects the latest run.
pub fn fetchRawHistory(ctx: *Context, workflow_id: []const u8, run_id: []const u8) (temporal.client.CallServiceError || error{OutOfMemory})!temporal.history.History {
    return readRawHistory(ctx.client, ctx.arena, ctx.namespace, workflow_id, run_id);
}

fn readRawHistory(client: anytype, arena: std.mem.Allocator, namespace: []const u8, workflow_id: []const u8, run_id: []const u8) temporal.client.CallServiceError!temporal.history.History {
    var recorded: temporal.history.History = .{};
    var token: []const u8 = "";
    while (true) {
        const page = try client.callService(.workflow, "GetWorkflowExecutionHistory", arena, .{
            .namespace = namespace,
            .execution = .{ .workflow_id = workflow_id, .run_id = run_id },
            .next_page_token = token,
            .history_event_filter_type = .HISTORY_EVENT_FILTER_TYPE_ALL_EVENT,
            .skip_archival = true,
        }, null);
        if (page.history) |h| try recorded.events.appendSlice(arena, h.events.items);
        token = page.next_page_token;
        if (token.len == 0) return recorded;
    }
}

pub fn expectEvent(ctx: *Context, recorded: temporal.history.History, event_type: history.EventType) error{FeatureFailed}!void {
    try ctx.expect(history.contains(recorded, event_type), "history is missing event {f}", .{EventName{ .data = event_type }});
}

pub fn expectNoEvent(ctx: *Context, recorded: temporal.history.History, event_type: history.EventType) error{FeatureFailed}!void {
    try ctx.expect(!history.contains(recorded, event_type), "history contains unexpected event {f}", .{EventName{ .data = event_type }});
}

pub fn expectEventsInOrder(ctx: *Context, recorded: temporal.history.History, event_types: []const history.EventType) error{FeatureFailed}!void {
    const EventNames = std.fmt.Alt([]const history.EventType, formatEventTypes);
    try ctx.expect(history.containsInOrder(recorded, event_types), "history does not contain events in order {f}", .{EventNames{ .data = event_types }});
}

const EventName = std.fmt.Alt(history.EventType, formatEventType);

fn formatEventType(event_type: history.EventType, writer: *std.Io.Writer) std.Io.Writer.Error!void {
    if (std.enums.tagName(history.EventType, event_type)) |name| {
        try writer.writeAll(name);
    } else {
        try writer.print("unknown ({d})", .{@backingInt(event_type)});
    }
}

fn formatEventTypes(event_types: []const history.EventType, writer: *std.Io.Writer) std.Io.Writer.Error!void {
    try writer.writeAll("{ ");
    for (event_types, 0..) |event_type, i| {
        if (i != 0) try writer.writeAll(", ");
        try formatEventType(event_type, writer);
    }
    try writer.writeAll(" }");
}

test "conformance context defaults and assertions" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var ctx: Context = .{
        .gpa = std.testing.allocator,
        .io = std.testing.io,
        .arena = arena.allocator(),
        .client = undefined,
        .feature = "signal/basic",
        .namespace = "default",
        .task_queue = "conformance-signal/basic",
    };
    const options = ctx.startOptions();
    try std.testing.expectEqualStrings(ctx.task_queue, options.id);
    try std.testing.expectEqualStrings(ctx.task_queue, options.task_queue);
    try std.testing.expectEqual(@as(?std.Io.Duration, .fromSeconds(60)), options.execution_timeout);
    try ctx.expectEqualStrings("same", "same");
    try std.testing.expectEqual(null, ctx.failure_message_);
    try std.testing.expectError(error.FeatureFailed, ctx.expectEqualStrings("one", "two"));
    try std.testing.expectEqualStrings("expected \"one\", got \"two\"", ctx.failure_message_.?);
    try std.testing.expectError(error.FeatureFailed, ctx.expect(false, "expected {d}, got {d}", .{ 1, 2 }));
    try std.testing.expectEqualStrings("expected 1, got 2", ctx.failure_message_.?);
}

test "conformance history assertions report the expected event types" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var ctx: Context = .{
        .gpa = std.testing.allocator,
        .io = std.testing.io,
        .arena = arena.allocator(),
        .client = undefined,
        .feature = "update/basic",
        .namespace = "default",
        .task_queue = "conformance-update/basic",
    };
    const accepted: history.EventType = .EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED;
    const completed: history.EventType = .EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_COMPLETED;
    const rejected: history.EventType = .EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_REJECTED;
    var events = [_]temporal.history.HistoryEvent{
        .{ .event_type = accepted },
        .{ .event_type = completed },
    };
    var recorded: temporal.history.History = .{ .events = .initBuffer(&events) };
    recorded.events.items = &events;
    try ctx.expectEvent(recorded, accepted);
    try ctx.expectNoEvent(recorded, rejected);
    try ctx.expectEventsInOrder(recorded, &.{ accepted, completed });
    try std.testing.expectEqual(null, ctx.failure_message_);

    try std.testing.expectError(error.FeatureFailed, ctx.expectEvent(recorded, rejected));
    try std.testing.expectEqualStrings("history is missing event " ++ @tagName(rejected), ctx.failure_message_.?);
    try std.testing.expectError(error.FeatureFailed, ctx.expectNoEvent(recorded, accepted));
    try std.testing.expectEqualStrings("history contains unexpected event " ++ @tagName(accepted), ctx.failure_message_.?);
    try std.testing.expectError(error.FeatureFailed, ctx.expectEventsInOrder(recorded, &.{ completed, accepted }));
    try std.testing.expectEqualStrings(
        "history does not contain events in order { " ++ @tagName(completed) ++ ", " ++ @tagName(accepted) ++ " }",
        ctx.failure_message_.?,
    );
    try std.testing.expectError(error.FeatureFailed, ctx.expectEvent(recorded, @fromBackingInt(1234)));
    try std.testing.expectEqualStrings("history is missing event unknown (1234)", ctx.failure_message_.?);
}

test "conformance raw history preserves stored payloads across pages" {
    const Pager = struct {
        fn callService(
            _: @This(),
            comptime service: temporal.client.Service,
            comptime method: []const u8,
            arena: std.mem.Allocator,
            request: temporal.client.ServiceRequest(service, method),
            _: ?*?temporal.client.RpcFailure,
        ) temporal.client.CallServiceError!temporal.client.ServiceResponse(service, method) {
            const execution = request.execution orelse return error.InvalidArgument;
            if (!std.mem.eql(u8, request.namespace, "feature-namespace") or
                !std.mem.eql(u8, execution.workflow_id, "workflow") or
                !std.mem.eql(u8, execution.run_id, "run") or request.wait_new_event or
                request.history_event_filter_type != .HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
                return error.InvalidArgument;

            var page: temporal.history.History = .{};
            if (request.next_page_token.len == 0) {
                const payload = try temporal.converter.payloadWithEncoding(arena, "my-encoding", "opaque bytes");
                var input: temporal.converter.Payloads = .{};
                try input.payloads.append(arena, payload);
                try page.events.append(arena, .{
                    .event_id = 1,
                    .event_type = .EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
                    .attributes = .{ .workflow_execution_started_event_attributes = .{ .input = input } },
                });
                return .{ .history = page, .next_page_token = "empty" };
            }
            if (std.mem.eql(u8, request.next_page_token, "empty")) return .{ .next_page_token = "last" };
            if (!std.mem.eql(u8, request.next_page_token, "last")) return error.InvalidArgument;
            try page.events.append(arena, .{ .event_id = 2, .event_type = .EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED });
            return .{ .history = page };
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const recorded = try readRawHistory(Pager{}, arena.allocator(), "feature-namespace", "workflow", "run");
    try std.testing.expectEqual(2, recorded.events.items.len);
    try std.testing.expectEqual(1, recorded.events.items[0].event_id);
    try std.testing.expectEqual(2, recorded.events.items[1].event_id);
    const payloads = history.inputPayloads(recorded);
    try std.testing.expectEqual(1, payloads.len);
    try std.testing.expectEqualStrings("my-encoding", temporal.converter.encodingOf(payloads[0]).?);
    try std.testing.expectEqualStrings("opaque bytes", payloads[0].data);
}
