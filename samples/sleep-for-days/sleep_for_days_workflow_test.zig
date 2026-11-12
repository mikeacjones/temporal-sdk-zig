//! Tests for the sleep-for-days workflow, on the time-skipping server so its 30-day timers
//! take no real time.

const std = @import("std");
const temporal = @import("temporal");
const sleep_for_days = @import("workflow.zig");

const testing = std.testing;

/// Counts the emails sent. Activities run on the worker's threads.
const Emails = struct {
    var count: std.atomic.Value(u32) = .init(0);

    fn send(ctx: *temporal.activity.Context, msg: []const u8) !void {
        _ = ctx;
        _ = msg;
        _ = count.fetchAdd(1, .monotonic);
    }
};

test "sends an email every 30 days until completed" {
    const env = try temporal.testing.WorkflowEnvironment.startTimeSkipping(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(sleep_for_days.SleepForDaysWorkflow);
    try env.registerActivityAs(sleep_for_days.SendEmailActivity, Emails.send);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();

    const start = try env.now();
    var run = try env.startWorkflow(sleep_for_days.SleepForDaysWorkflow, .{ .task_queue = "" }, .{});
    defer run.deinit();

    // Emails go out on days 0, 30 and 60.
    try env.sleep(.fromSeconds(90 * std.time.s_per_day));
    try testing.expectEqual(3, Emails.count.load(.monotonic));
    const elapsed = start.durationTo(try env.now());
    try testing.expect(elapsed.nanoseconds >= 90 * std.time.ns_per_day);

    // The signal ends the workflow without another email.
    try env.signalWorkflow(sleep_for_days.Complete, run.id, .{});
    try testing.expectEqualStrings("done", try env.getResult(sleep_for_days.SleepForDaysWorkflow, &run, arena.allocator(), null));
    try testing.expectEqual(3, Emails.count.load(.monotonic));
}
