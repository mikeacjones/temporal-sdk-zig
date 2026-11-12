//! Tests for the updatabletimer sample, on the time-skipping server so the half-hour sleeps
//! take no real time.

const std = @import("std");
const temporal = @import("temporal");
const updatabletimer = @import("workflow.zig");

const testing = std.testing;
const Time = temporal.converter.Time;
const Duration = std.Io.Duration;

const minute: Duration = .fromSeconds(std.time.s_per_min);

fn minutes(n: i64) Duration {
    return .fromSeconds(n * std.time.s_per_min);
}

fn at(start: std.Io.Timestamp, after: Duration) Time {
    return .fromTimestamp(start.addDuration(after));
}

/// Checks that the run closed `expected` of server time after `start`. Server time also
/// runs on in real time between skips, so up to a second more is allowed. The run's close
/// time is used rather than `env.now()`: after the run closes, the time-skipping server keeps
/// skipping time (to the next pending timeout) until `getResult` locks it again.
fn expectClosedAfter(env: *temporal.testing.WorkflowEnvironment, arena: std.mem.Allocator, run: anytype, start: std.Io.Timestamp, expected: Duration) !void {
    const description = try env.client.describeWorkflow(arena, run.id, run.run_id);
    const took = start.durationTo(description.close_time.?).nanoseconds;
    if (took < expected.nanoseconds or took > expected.nanoseconds + std.time.ns_per_s) {
        std.debug.print("run closed {d}ns after the start, expected {d}ns\n", .{ took, expected.nanoseconds });
        return error.TestUnexpectedRunDuration;
    }
}

test "the workflow sleeps until its wake-up time and reports it" {
    const env = try temporal.testing.WorkflowEnvironment.startTimeSkipping(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(updatabletimer.UpdatableTimerWorkflow);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const start = try env.now();
    const wake_up_time = at(start, minutes(30));
    var run = try env.startWorkflow(updatabletimer.UpdatableTimerWorkflow, .{ .task_queue = "" }, .{wake_up_time});
    defer run.deinit();

    try env.sleep(minutes(10));
    const queried = try env.queryWorkflow(updatabletimer.GetWakeUpTime, a, run.id, .{});
    try testing.expectEqual(wake_up_time.unix_ns, queried.unix_ns);

    try env.getResult(updatabletimer.UpdatableTimerWorkflow, &run, a, null);
    try expectClosedAfter(env, a, &run, start, minutes(30));
}

test "signals move the wake-up time earlier and later" {
    const env = try temporal.testing.WorkflowEnvironment.startTimeSkipping(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(updatabletimer.UpdatableTimerWorkflow);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const start = try env.now();
    var run = try env.startWorkflow(updatabletimer.UpdatableTimerWorkflow, .{ .task_queue = "" }, .{at(start, minutes(30))});
    defer run.deinit();

    try env.sleep(minutes(10));
    const earlier = at(start, minutes(15));
    try env.signalWorkflow(updatabletimer.UpdateWakeUpTime, run.id, .{earlier});

    try env.sleep(minute);
    const queried = try env.queryWorkflow(updatabletimer.GetWakeUpTime, a, run.id, .{});
    try testing.expectEqual(earlier.unix_ns, queried.unix_ns);

    try env.sleep(minute);
    try env.signalWorkflow(updatabletimer.UpdateWakeUpTime, run.id, .{at(start, minutes(40))});

    try env.getResult(updatabletimer.UpdatableTimerWorkflow, &run, a, null);
    try expectClosedAfter(env, a, &run, start, minutes(40));
}

test "canceling the workflow cancels the sleep" {
    const env = try temporal.testing.WorkflowEnvironment.startTimeSkipping(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(updatabletimer.UpdatableTimerWorkflow);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const start = try env.now();
    var run = try env.startWorkflow(updatabletimer.UpdatableTimerWorkflow, .{ .task_queue = "" }, .{at(start, minutes(30))});
    defer run.deinit();

    try env.sleep(minutes(10));
    try env.client.cancelWorkflow(run.id, run.run_id, "test");
    try testing.expectError(error.WorkflowCanceled, env.getResult(updatabletimer.UpdatableTimerWorkflow, &run, a, null));
    try expectClosedAfter(env, a, &run, start, minutes(10));
}
