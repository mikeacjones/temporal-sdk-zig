//! Workflow histories: the types, and parsing them from JSON as `temporal workflow show
//! --output json` and the Temporal UI export them.
//!
//! Those exports are ProtoJSON, and older ones write enums by short name: `"eventType":
//! "WorkflowExecutionStarted"` for `EVENT_TYPE_WORKFLOW_EXECUTION_STARTED`. `fromJson` decodes
//! them with `protojson.decode`, which accepts both enum name forms.

const std = @import("std");
const proto = @import("proto.zig");
const protojson = @import("protojson.zig");
const Allocator = std.mem.Allocator;

pub const History = proto.temporal.api.history.v1.History;
pub const HistoryEvent = proto.temporal.api.history.v1.HistoryEvent;

/// A history with the workflow ID it ran under, which the history itself doesn't record.
/// Both are borrowed.
pub const WorkflowHistory = struct {
    workflow_id: []const u8,
    history: History,
};

pub const Error = error{ OutOfMemory, InvalidHistoryJson };

/// Parses a history JSON export with `protojson.decode`, which also accepts the short enum
/// names exports may hold. Memory comes from `arena`.
pub fn fromJson(arena: Allocator, json: []const u8) Error!History {
    return protojson.decode(History, arena, json) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        error.InvalidProtoJson => return error.InvalidHistoryJson,
    };
}

// ---------------------------------------------------------------- tests

const testing = std.testing;

test "a CLI history export with short enum names and base64 metadata parses" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const json =
        \\{"events":[
        \\ {"eventId":"1","eventType":"WorkflowExecutionStarted","eventTime":"2020-07-30T00:30:03.082421843Z",
        \\  "workflowExecutionStartedEventAttributes":{"workflowType":{"name":"Workflow"},"taskQueue":{"name":"hello-world","kind":"Normal"},
        \\   "input":{"payloads":[{"metadata":{"encoding":"anNvbi9wbGFpbg=="},"data":"IlRlbXBvcmFsIg=="}]},"workflowRunTimeout":"60s"}},
        \\ {"eventId":"2","eventType":"WorkflowTaskScheduled","workflowTaskScheduledEventAttributes":{"taskQueue":{"name":"hello-world"},"startToCloseTimeout":"10s","attempt":1}}
        \\]}
    ;
    const h = try fromJson(arena.allocator(), json);
    try testing.expectEqual(@as(usize, 2), h.events.items.len);
    const e = h.events.items[0];
    try testing.expectEqual(proto.temporal.api.enums.v1.EventType.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, e.event_type);
    const started = e.attributes.?.workflow_execution_started_event_attributes;
    try testing.expectEqual(proto.temporal.api.enums.v1.TaskQueueKind.TASK_QUEUE_KIND_NORMAL, started.task_queue.?.kind);
    const p = started.input.?.payloads.items[0];
    try testing.expectEqualStrings("json/plain", p.metadata.items[0].value);
    try testing.expectEqualStrings("\"Temporal\"", p.data);
    try testing.expectEqual(proto.temporal.api.enums.v1.EventType.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, h.events.items[1].event_type);
}

test "canonical enum names pass through unchanged" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const h = try fromJson(arena.allocator(),
        \\{"events":[{"eventId":"1","eventType":"EVENT_TYPE_WORKFLOW_EXECUTION_STARTED","workflowExecutionStartedEventAttributes":{"workflowType":{"name":"W"}}}]}
    );
    try testing.expectEqual(proto.temporal.api.enums.v1.EventType.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, h.events.items[0].event_type);
}

test "metadata values that are not UTF-8 decode" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const h = try fromJson(arena.allocator(),
        \\{"events":[{"eventId":"1","eventType":"WorkflowExecutionStarted",
        \\ "workflowExecutionStartedEventAttributes":{"input":{"payloads":[{"metadata":{"encoding":"YmluYXJ5L3BsYWlu","key":"//79"}}]}}}]}
    );
    const md = h.events.items[0].attributes.?.workflow_execution_started_event_attributes.input.?.payloads.items[0].metadata.items;
    try testing.expectEqualStrings("binary/plain", md[0].value);
    try testing.expectEqualSlices(u8, &.{ 0xff, 0xfe, 0xfd }, md[1].value);
}

test "a history that protojson.encode wrote reads back equal" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var payload: proto.temporal.api.common.v1.Payload = .{ .data = &.{ 0x00, 0xff, 0x80 } };
    try payload.metadata.append(a, .{ .key = "encoding", .value = "binary/plain" });
    try payload.metadata.append(a, .{ .key = "iv", .value = &.{ 0xff, 0xfe, 0xfd } });
    var input: proto.temporal.api.common.v1.Payloads = .{};
    try input.payloads.append(a, payload);
    var h: History = .{};
    try h.events.append(a, .{
        .event_id = 1,
        .event_type = .EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
        .attributes = .{ .workflow_execution_started_event_attributes = .{
            .workflow_type = .{ .name = "Workflow" },
            .task_queue = .{ .name = "hello-world", .kind = .TASK_QUEUE_KIND_NORMAL },
            .input = input,
        } },
    });
    try h.events.append(a, .{
        .event_id = 2,
        .event_type = .EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
        .attributes = .{ .workflow_task_scheduled_event_attributes = .{ .task_queue = .{ .name = "hello-world" }, .attempt = 1 } },
    });

    const json = try protojson.encode(a, h);
    const back = try fromJson(a, json);
    try testing.expectEqualStrings(json, try protojson.encode(a, back));
    // Comparing the encodings compares everything; check the bytes by hand as well.
    const p = back.events.items[0].attributes.?.workflow_execution_started_event_attributes.input.?.payloads.items[0];
    try testing.expectEqualSlices(u8, payload.data, p.data);
    try testing.expectEqualSlices(u8, &.{ 0xff, 0xfe, 0xfd }, p.metadata.items[1].value);
    try testing.expectEqual(proto.temporal.api.enums.v1.EventType.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, back.events.items[1].event_type);
}
