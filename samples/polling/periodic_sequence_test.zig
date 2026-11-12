//! Tests for periodic sequence polling.

const std = @import("std");
const temporal = @import("temporal");
const periodic_sequence = @import("periodic_sequence.zig");
const TestService = @import("test_service.zig").TestService;

const testing = std.testing;

test "periodic sequence polling returns the child workflow's result" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    var service: TestService = .init(5);
    var activities: periodic_sequence.PollingActivities = .{ .test_service = &service };
    try env.registerWorkflow(periodic_sequence.PeriodicSequencePolling);
    try env.registerWorkflow(periodic_sequence.PollingChildWorkflow);
    try env.registerActivityWith(periodic_sequence.DoPoll, &activities);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const result = try env.executeWorkflow(periodic_sequence.PeriodicSequencePolling, .{.fromMilliseconds(100)}, arena.allocator(), null);
    try testing.expectEqualStrings("OK", result);
    try testing.expectEqual(5, service.try_attempts.load(.monotonic));
}

test "the child workflow continues as new after a run of polls" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    // Answers on the third poll of the child's second run.
    var service: TestService = .init(periodic_sequence.polls_per_run + 3);
    var activities: periodic_sequence.PollingActivities = .{ .test_service = &service };
    try env.registerWorkflow(periodic_sequence.PeriodicSequencePolling);
    try env.registerWorkflow(periodic_sequence.PollingChildWorkflow);
    try env.registerActivityWith(periodic_sequence.DoPoll, &activities);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var run = try env.startWorkflow(periodic_sequence.PeriodicSequencePolling, .{ .task_queue = "" }, .{.fromMilliseconds(10)});
    defer run.deinit();
    try testing.expectEqualStrings("OK", try env.getResult(periodic_sequence.PeriodicSequencePolling, &run, a, null));

    // The parent saw one child, which finished in its second run.
    const parent_events = (try env.client.fetchHistory(a, run.id, run.run_id)).events.items;
    const child_id = for (parent_events) |event| switch (event.attributes orelse continue) {
        .child_workflow_execution_completed_event_attributes => |e| break e.workflow_execution.?.workflow_id,
        else => {},
    } else return error.NoChildCompleted;

    // An empty run ID reads the latest run.
    const last_events = (try env.client.fetchHistory(a, child_id, "")).events.items;
    try testing.expect(last_events[last_events.len - 1].attributes.? == .workflow_execution_completed_event_attributes);
    const first_run_id = last_events[0].attributes.?.workflow_execution_started_event_attributes.continued_execution_run_id;
    try testing.expect(first_run_id.len > 0);
    try env.replayRun(child_id, "");

    const first_events = (try env.client.fetchHistory(a, child_id, first_run_id)).events.items;
    try testing.expect(first_events[first_events.len - 1].attributes.? == .workflow_execution_continued_as_new_event_attributes);
    try testing.expectEqual(0, first_events[0].attributes.?.workflow_execution_started_event_attributes.continued_execution_run_id.len);
    try env.replayRun(child_id, first_run_id);
}
