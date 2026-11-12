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

fn runWorker(w: *temporal.worker.Worker) temporal.worker.Worker.RunError!void {
    return w.run();
}

test "a triggered schedule runs the workflow, which hands the schedule ID to the activity" {
    Recorder.reset();
    var server = try temporal.testing.DevServer.start(testing.allocator, testing.io, .{});
    defer server.stop(testing.io);
    var client = try temporal.client.Client.connect(testing.allocator, testing.io, server.clientOptions());
    defer client.close();

    var w = try temporal.worker.Worker.init(testing.allocator, testing.io, &client, "schedule", .{});
    defer w.deinit();
    try w.registerWorkflow(sample.SampleScheduleWorkflow);
    try w.registerActivity(temporal.activity.Def(sample.DoSomething.activity_name, Recorder.doSomething));
    var running = try testing.io.concurrent(runWorker, .{&w});
    defer {
        w.stop();
        running.await(testing.io) catch {};
    }

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var handle = try client.createSchedule("schedule_test", .{ .schedule = .{
        .action = try schedule.startWorkflow(a, sample.SampleScheduleWorkflow, .{}, .{
            .id = "schedule_workflow",
            .task_queue = "schedule",
        }),
        .spec = .{},
    } });
    defer handle.deinit();
    defer handle.delete() catch {};
    try handle.trigger(.allow_all);

    // Wait, for at most 30 s, until the schedule has started the workflow.
    var polls: usize = 0;
    const action = while (true) : (polls += 1) {
        const d = try handle.describe(a);
        if (d.info.recent_actions.len > 0) break d.info.recent_actions[0];
        if (polls == 300) return error.TestUnexpectedResult;
        try testing.io.sleep(.fromMilliseconds(100), .awake);
    };
    var run = try client.getWorkflow(void, action.workflow.workflow_id, action.workflow.run_id);
    defer run.deinit();
    try run.get(a);

    const by, const at = try Recorder.only(testing.io);
    try testing.expectEqualStrings("schedule_test", by);
    // The search attribute holds the scheduled time to the second.
    const ns_per_s = std.time.ns_per_s;
    try testing.expectEqual(@divFloor(action.scheduled_at.?.nanoseconds, ns_per_s), @divFloor(at.unix_ns, ns_per_s));

    // The scheduled run replays its own history.
    var replayer = temporal.testing.WorkflowReplayer.init(testing.allocator, testing.io);
    defer replayer.deinit();
    try replayer.registerWorkflow(sample.SampleScheduleWorkflow);
    const replayed = try replayer.replayWorkflowExecution(a, &client, run.id, run.run_id);
    try testing.expectEqual(null, replayed.err);
}
