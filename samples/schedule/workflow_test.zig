//! Tests for the schedule sample.

const std = @import("std");
const temporal = @import("temporal");
const sample = @import("schedule.zig");

const testing = std.testing;
const schedule = temporal.schedule;
const Time = temporal.converter.Time;

/// Records each call of the scheduled job. Activities run on the worker's threads, so the
/// calls are guarded by `mutex`.
const Recorder = struct {
    const Call = struct { schedule_by_id: [64]u8, id_len: usize, start_time: Time };

    var mutex: std.Io.Mutex = .init;
    var calls: [4]Call = undefined;
    var len: usize = 0;

    fn reset() void {
        len = 0;
    }

    fn doSomething(ctx: *temporal.activity.Context, schedule_by_id: []const u8, start_time: Time) !void {
        mutex.lockUncancelable(ctx.io());
        defer mutex.unlock(ctx.io());
        if (len == calls.len or schedule_by_id.len > 64) return error.UnexpectedCall;
        calls[len] = .{ .schedule_by_id = undefined, .id_len = schedule_by_id.len, .start_time = start_time };
        @memcpy(calls[len].schedule_by_id[0..schedule_by_id.len], schedule_by_id);
        len += 1;
    }

    fn only(io: std.Io) !struct { []const u8, Time } {
        mutex.lockUncancelable(io);
        defer mutex.unlock(io);
        try testing.expectEqual(1, len);
        return .{ calls[0].schedule_by_id[0..calls[0].id_len], calls[0].start_time };
    }
};

test "workflow passes the schedule's search attributes to the activity" {
    Recorder.reset();
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(sample.SampleScheduleWorkflow);
    try env.registerActivityAs(sample.DoSomething, Recorder.doSomething);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const start_time = Time.fromTimestamp(.now(testing.io, .real));
    try env.executeWorkflowWithOptions(sample.SampleScheduleWorkflow, .{
        .task_queue = "",
        .search_attributes = &.{
            schedule.scheduled_by_id.set("schedule_test_ID"),
            schedule.scheduled_start_time.set(start_time),
        },
    }, .{}, arena.allocator(), null);

    const id, const at = try Recorder.only(testing.io);
    try testing.expectEqualStrings("schedule_test_ID", id);
    // The server stores datetimes to the microsecond.
    try testing.expectEqual(@divFloor(start_time.unix_ns, 1000), @divFloor(at.unix_ns, 1000));
}

test "workflow fails when no schedule started it" {
    Recorder.reset();
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(sample.SampleScheduleWorkflow);
    try env.registerActivityAs(sample.DoSomething, Recorder.doSomething);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var failure: ?temporal.failure.Failure = null;
    try testing.expectError(error.WorkflowFailed, env.executeWorkflow(sample.SampleScheduleWorkflow, .{}, arena.allocator(), &failure));
    try testing.expect(std.mem.startsWith(u8, failure.?.message, "expected the TemporalScheduledById search attribute"));
    try testing.expectEqual(0, Recorder.len);
}

test "a triggered schedule runs the workflow, which hands the schedule ID to the activity" {
    Recorder.reset();
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(sample.SampleScheduleWorkflow);
    try env.registerActivityAs(sample.DoSomething, Recorder.doSomething);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    // The schedule starts its workflow on the environment's task queue, through its client.
    var handle = try env.client.createSchedule("schedule_test", .{ .schedule = .{
        .action = try schedule.startWorkflow(a, sample.SampleScheduleWorkflow, .{}, .{
            .id = "schedule_workflow",
            .task_queue = env.task_queue,
        }),
        .spec = .{},
    } });
    defer handle.deinit();
    defer handle.delete() catch {};
    try env.startWorker();
    try handle.trigger(.allow_all);

    // Wait, for at most 30 s, until the schedule has started the workflow.
    var polls: usize = 0;
    const action = while (true) : (polls += 1) {
        const d = try handle.describe(a);
        if (d.info.recent_actions.len > 0) break d.info.recent_actions[0];
        if (polls == 300) return error.TestUnexpectedResult;
        try testing.io.sleep(.fromMilliseconds(100), .awake);
    };
    // getResultByName waits for the run and replays its history.
    var run = try env.client.getWorkflow(void, action.workflow.workflow_id, action.workflow.run_id);
    defer run.deinit();
    try env.getResultByName(void, &run, a, null);

    const by, const at = try Recorder.only(testing.io);
    try testing.expectEqualStrings("schedule_test", by);
    // The server truncates the scheduled time to the second in the search attribute.
    const ns_per_s = std.time.ns_per_s;
    try testing.expectEqual(@divFloor(action.scheduled_at.?.nanoseconds, ns_per_s), @divFloor(at.unix_ns, ns_per_s));
}
