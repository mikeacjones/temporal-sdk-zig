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
