//! Port of samples-go/retryactivity/retry_activity_workflow_test.go.

const std = @import("std");
const temporal = @import("temporal");
const retryactivity = @import("retry_activity_workflow.zig");

const testing = std.testing;

// Go: OnActivity(BatchProcessingActivity, ...).Return(func ...) recording where each attempt
// started, then running the real activity with a 1ns delay.
const Recorder = struct {
    var mutex: std.Io.Mutex = .init;
    var started: [8]i64 = undefined;
    var count: usize = 0;

    fn batchProcessing(ctx: *temporal.activity.Context, first_task_id: i64, batch_size: i64, process_delay: i64) !void {
        _ = process_delay;
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
        return retryactivity.batchProcessingActivity(ctx, first_task_id, batch_size, 1); // override for test
    }
};

test "Workflow" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(retryactivity.RetryWorkflow);
    try env.registerActivityAs(retryactivity.BatchProcessingActivity, Recorder.batchProcessing);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    try env.executeWorkflow(retryactivity.RetryWorkflow, .{}, arena.allocator(), null);
    try testing.expectEqualSlices(i64, &.{ 0, 6, 12, 18 }, Recorder.started[0..Recorder.count]);
}
