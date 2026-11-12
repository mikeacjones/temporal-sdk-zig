//! Tests for infrequent polling.

const std = @import("std");
const temporal = @import("temporal");
const infrequent = @import("infrequent.zig");
const test_service = @import("test_service.zig");

const testing = std.testing;

test "infrequent polling retries the activity every poll interval until the service answers" {
    // Time skipping passes the minute between retries at once.
    const env = try temporal.testing.WorkflowEnvironment.startTimeSkipping(testing.allocator, testing.io);
    defer env.deinit();
    var service: test_service.TestService = .init(5);
    var activities: infrequent.PollingActivities = .{ .test_service = &service };
    try env.registerWorkflow(infrequent.InfrequentPolling);
    try env.registerActivityWith(infrequent.DoPoll, &activities);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const start = try env.now();
    const result = try env.executeWorkflow(infrequent.InfrequentPolling, .{infrequent.default_poll_interval}, arena.allocator(), null);
    try testing.expectEqualStrings("OK", result);
    try testing.expectEqual(5, service.try_attempts.load(.monotonic));
    // Four failed attempts, each followed by a full poll interval.
    const elapsed = start.durationTo(try env.now());
    try testing.expect(elapsed.toSeconds() >= 4 * infrequent.default_poll_interval.toSeconds());
}

test "the polling activity fails with a benign service error while the service is down" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var service: test_service.TestService = .init(2);
    var activities: infrequent.PollingActivities = .{ .test_service = &service };
    const env: temporal.testing.ActivityEnvironment = .init(testing.io);
    try testing.expectError(error.ApplicationFailure, env.executeActivityWith(infrequent.DoPoll, &activities, .{}, arena.allocator()));
    try testing.expectEqualStrings("OK", try env.executeActivityWith(infrequent.DoPoll, &activities, .{}, arena.allocator()));
}
