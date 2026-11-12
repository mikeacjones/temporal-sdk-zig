//! Tests for frequent polling.

const std = @import("std");
const temporal = @import("temporal");
const frequent = @import("frequent.zig");
const TestService = @import("test_service.zig").TestService;

const testing = std.testing;

test "frequent polling returns the service's result once it answers" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    var service: TestService = .init(5);
    var activities: frequent.PollingActivities = .{ .test_service = &service, .poll_interval = .fromMilliseconds(100) };
    try env.registerWorkflow(frequent.FrequentPolling);
    try env.registerActivityWith(frequent.DoPoll, &activities);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const result = try env.executeWorkflow(frequent.FrequentPolling, .{}, arena.allocator(), null);
    try testing.expectEqualStrings("OK", result);
    // All five polls ran in one activity attempt.
    try testing.expectEqual(5, service.try_attempts.load(.monotonic));
}
