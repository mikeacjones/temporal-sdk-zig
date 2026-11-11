//! Tests for the cron sample.

const std = @import("std");
const temporal = @import("temporal");
const cron = @import("workflow.zig");

const testing = std.testing;
const workflow = temporal.workflow;
const Time = temporal.converter.Time;

const ns_per_hour = std.time.ns_per_hour;

/// Starts the cron workflow as an hourly child, lets two hours pass, and returns whether the
/// child is still pending: a cron child never completes while its schedule runs.
fn cronParent(ctx: *workflow.Context) !bool {
    const child = ctx.executeChildWorkflow(cron.Cron, .{ .run_timeout = .fromSeconds(10 * 60), .cron_schedule = "0 * * * *" }, .{});
    try ctx.io().sleep(.fromSeconds(2 * std.time.s_per_hour), .awake);
    return !child.isReady();
}
const CronParent = workflow.Def("CronParent", cronParent);

/// Records each cron job's arguments. Activities run on the worker's threads, so the calls
/// are guarded by `mutex`.
const Recorder = struct {
    const Call = struct { last_run_time: ?Time, this_run_time: Time };

    var mutex: std.Io.Mutex = .init;
    var calls: [8]Call = undefined;
    var len: usize = 0;

    fn runCronJob(ctx: *temporal.activity.Context, last_run_time: ?Time, this_run_time: Time) !void {
        mutex.lockUncancelable(ctx.io());
        defer mutex.unlock(ctx.io());
        if (len == calls.len) return error.TooManyCronRuns;
        calls[len] = .{ .last_run_time = last_run_time, .this_run_time = this_run_time };
        len += 1;
    }
};

test "an hourly cron child runs on the hour and hands each run's time to the next" {
    const env = try temporal.testing.WorkflowEnvironment.startTimeSkipping(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(CronParent);
    try env.registerWorkflow(cron.Cron);
    try env.registerActivityAs(cron.RunCronJob, Recorder.runCronJob);

    // Skip to half past the hour, so neither the start nor the end of the parent's two hours
    // falls near an hourly boundary.
    const now = (try env.now()).nanoseconds;
    try env.sleep(.fromNanoseconds(@mod(ns_per_hour / 2 - now, ns_per_hour)));
    const start = (try env.now()).nanoseconds;

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    try testing.expect(try env.executeWorkflow(CronParent, .{}, arena.allocator(), null));

    // The time-skipping server starts a cron workflow's first run at once (a real server waits
    // for the first schedule time) and later runs at the schedule's times. Starting at half
    // past the hour, the parent's two hours cover that run and the next two hourly
    // boundaries. The child can run once more after the parent returns, before the server
    // terminates it, so later runs are left out.
    Recorder.mutex.lockUncancelable(testing.io);
    defer Recorder.mutex.unlock(testing.io);
    const end = start + 2 * ns_per_hour;
    var runs: usize = 0;
    while (runs < Recorder.len and Recorder.calls[runs].this_run_time.unix_ns < end) runs += 1;
    const calls = Recorder.calls[0..runs];
    try testing.expectEqual(3, calls.len);

    // A run's time is when its first workflow task started: its scheduled time, plus however
    // long the worker took to pick it up.
    const first_boundary = start - @mod(start, ns_per_hour) + ns_per_hour;
    const scheduled = [_]i96{ start, first_boundary, first_boundary + ns_per_hour };
    try testing.expectEqual(null, calls[0].last_run_time);
    for (calls, scheduled, 0..) |call, at, i| {
        const late = call.this_run_time.unix_ns - at;
        if (late < 0 or late >= std.time.ns_per_min) {
            var buf: [Time.max_len]u8 = undefined;
            var want: [Time.max_len]u8 = undefined;
            std.debug.print("cron run {d} ran at {s}, not just after {s}\n", .{ i, call.this_run_time.format(&buf), Time.format(.{ .unix_ns = at }, &want) });
            return error.TestUnexpectedResult;
        }
        if (i > 0) try testing.expectEqual(calls[i - 1].this_run_time, call.last_run_time.?);
    }
}
