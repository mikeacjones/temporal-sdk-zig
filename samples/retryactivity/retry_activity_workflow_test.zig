//! Tests for the retryactivity sample.

const std = @import("std");
const temporal = @import("temporal");
const retryactivity = @import("retry_activity_workflow.zig");

const testing = std.testing;

// Records where each attempt started, then runs the real activity without the per-task delay.
const Recorder = struct {
    var mutex: std.Io.Mutex = .init;
    var started: [8]i64 = undefined;
    var count: usize = 0;

    fn processBatch(ctx: *temporal.activity.Context, first_task_id: i64, batch_size: i64, task_ms: i64) !void {
        _ = task_ms;
        var i = first_task_id;
        if (ctx.hasHeartbeatDetails()) {
            if (ctx.heartbeatDetails(i64)) |completed_idx| i = completed_idx + 1 else |_| {}
        }
        {
            try mutex.lock(ctx.io());
            defer mutex.unlock(ctx.io());
            if (count < started.len) started[count] = i;
            count += 1;
        }
        return retryactivity.processBatch(ctx, first_task_id, batch_size, 0);
    }
};

test "each retry resumes from the last heartbeat" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(retryactivity.RetryActivity);
    try env.registerActivityAs(retryactivity.ProcessBatch, Recorder.processBatch);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    try env.executeWorkflow(retryactivity.RetryActivity, .{}, arena.allocator(), null);
    try testing.expectEqualSlices(i64, &.{ 0, 6, 12, 18 }, Recorder.started[0..Recorder.count]);
}
