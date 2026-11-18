//! Event assertions over a fetched history in its recorded order.
const std = @import("std");
const temporal = @import("temporal");

const History = temporal.history.History;
pub const EventType = temporal.proto.temporal.api.enums.v1.EventType;

pub fn count(history: History, event_type: EventType) usize {
    var matches: usize = 0;
    for (history.events.items) |event| {
        if (event.event_type == event_type) matches += 1;
    }
    return matches;
}

pub fn contains(history: History, event_type: EventType) bool {
    return containsInOrder(history, &.{event_type});
}

/// Matches a subsequence. Each requested type consumes a distinct event.
pub fn containsInOrder(history: History, event_types: []const EventType) bool {
    var remaining = event_types;
    for (history.events.items) |event| {
        if (remaining.len == 0) break;
        if (event.event_type == remaining[0]) remaining = remaining[1..];
    }
    return remaining.len == 0;
}

pub fn inputPayloads(history: History) []const temporal.converter.Payload {
    for (history.events.items) |event| {
        if (event.event_type != .EVENT_TYPE_WORKFLOW_EXECUTION_STARTED) continue;
        const attrs = event.attributes orelse return &.{};
        if (attrs != .workflow_execution_started_event_attributes) return &.{};
        const input = attrs.workflow_execution_started_event_attributes.input orelse return &.{};
        return input.payloads.items;
    }
    return &.{};
}

pub fn resultPayloads(history: History) []const temporal.converter.Payload {
    for (history.events.items) |event| {
        if (event.event_type != .EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED) continue;
        const attrs = event.attributes orelse return &.{};
        if (attrs != .workflow_execution_completed_event_attributes) return &.{};
        const result = attrs.workflow_execution_completed_event_attributes.result orelse return &.{};
        return result.payloads.items;
    }
    return &.{};
}

test "conformance history counts present and absent event types" {
    const started: EventType = .EVENT_TYPE_WORKFLOW_EXECUTION_STARTED;
    const signaled: EventType = .EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED;
    var events = [_]temporal.history.HistoryEvent{
        .{ .event_type = started },
        .{ .event_type = signaled },
        .{ .event_type = signaled },
    };
    var recorded: History = .{ .events = .initBuffer(&events) };
    recorded.events.items = &events;
    try std.testing.expectEqual(2, count(recorded, signaled));
    try std.testing.expectEqual(0, count(recorded, .EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_REJECTED));
    try std.testing.expect(contains(recorded, started));
    try std.testing.expect(!contains(recorded, .EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED));
    try std.testing.expect(!contains(.{}, started));
}

test "conformance history matches ordered subsequences with gaps and duplicates" {
    const started: EventType = .EVENT_TYPE_WORKFLOW_EXECUTION_STARTED;
    const signaled: EventType = .EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED;
    const task: EventType = .EVENT_TYPE_WORKFLOW_TASK_COMPLETED;
    const completed: EventType = .EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED;
    var events = [_]temporal.history.HistoryEvent{
        .{ .event_type = started },
        .{ .event_type = signaled },
        .{ .event_type = task },
        .{ .event_type = signaled },
        .{ .event_type = completed },
    };
    var recorded: History = .{ .events = .initBuffer(&events) };
    recorded.events.items = &events;
    try std.testing.expect(containsInOrder(recorded, &.{ started, signaled, signaled, completed }));
    try std.testing.expect(!containsInOrder(recorded, &.{ signaled, signaled, task }));
    try std.testing.expect(!containsInOrder(recorded, &.{ started, started }));
    try std.testing.expect(!containsInOrder(recorded, &.{ .EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_REJECTED, completed }));
    try std.testing.expect(containsInOrder(recorded, &.{}));
    try std.testing.expect(containsInOrder(.{}, &.{}));
    try std.testing.expect(!containsInOrder(.{}, &.{started}));
}

test "conformance history reads start and completion payloads unchanged" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var input: temporal.converter.Payloads = .{};
    try input.payloads.append(a, try temporal.converter.payloadWithEncoding(a, "my-encoding", "raw input"));
    try input.payloads.append(a, try temporal.converter.payloadWithEncoding(a, "my-encoding", "second argument"));
    var result: temporal.converter.Payloads = .{};
    try result.payloads.append(a, try temporal.converter.payloadWithEncoding(a, "my-encoding", "raw result"));
    var events = [_]temporal.history.HistoryEvent{
        .{
            .event_type = .EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
            .attributes = .{ .workflow_execution_started_event_attributes = .{ .input = input } },
        },
        .{ .event_type = .EVENT_TYPE_WORKFLOW_TASK_COMPLETED },
        .{
            .event_type = .EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
            .attributes = .{ .workflow_execution_completed_event_attributes = .{ .result = result } },
        },
    };
    var recorded: History = .{ .events = .fromOwnedSlice(&events) };
    try std.testing.expectEqualSlices(temporal.converter.Payload, input.payloads.items, inputPayloads(recorded));
    try std.testing.expectEqualSlices(temporal.converter.Payload, result.payloads.items, resultPayloads(recorded));
    try std.testing.expectEqual(0, inputPayloads(.{}).len);
    try std.testing.expectEqual(0, resultPayloads(.{}).len);

    recorded.events.items[0].attributes = null;
    recorded.events.items[2].attributes.?.workflow_execution_completed_event_attributes.result = null;
    try std.testing.expectEqual(0, inputPayloads(recorded).len);
    try std.testing.expectEqual(0, resultPayloads(recorded).len);
}
