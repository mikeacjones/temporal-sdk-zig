//! Port of samples-go/splitmerge-selector: run several activities in parallel and merge their
//! results as each one completes, using `std.Io.Select` (Go's `workflow.Selector`). See
//! splitmerge-future to process them in the order they were started instead.

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;
const activity = temporal.activity;

/// The activity result. Field names match the Go sample's JSON.
pub const ChunkResult = struct {
    NumberOfItemsInChunk: i64 = 0,
    SumInChunk: i64 = 0,
};

const Future = workflow.ActivityFuture(ChunkResult);
const Ready = union(enum) { chunk: workflow.ActivityError!ChunkResult };

fn sampleSplitMergeSelectorWorkflow(ctx: *workflow.Context, worker_count: i64) !ChunkResult {
    const a = ctx.allocator();
    const ao: workflow.ActivityOptions = .{ .start_to_close_timeout = .fromSeconds(10) };

    const n: usize = @intCast(@max(worker_count, 0));
    const buffer = try a.alloc(Ready, n);
    var selector: std.Io.Select(Ready) = .init(ctx.io(), buffer);
    defer selector.cancelDiscard();
    for (0..n) |i| {
        const future = ctx.executeActivity(ChunkProcessingActivity, ao, .{@as(i64, @intCast(i + 1))});
        selector.async(.chunk, Future.get, .{future});
    }

    var total: ChunkResult = .{};
    for (0..n) |_| {
        const ready = try selector.await();
        const r = try ready.chunk;
        total.NumberOfItemsInChunk += r.NumberOfItemsInChunk;
        total.SumInChunk += r.SumInChunk;
    }

    ctx.logger().info("Workflow completed.", .{});
    return total;
}

pub const SampleSplitMergeSelectorWorkflow = workflow.Def("SampleSplitMergeSelectorWorkflow", sampleSplitMergeSelectorWorkflow);

fn chunkProcessingActivity(ctx: *activity.Context, chunk_id: i64) !ChunkResult {
    // Some fake processing logic here.
    ctx.logger().info("Chunk processed", .{ .chunkID = chunk_id });
    return .{ .NumberOfItemsInChunk = chunk_id, .SumInChunk = chunk_id * chunk_id };
}

pub const ChunkProcessingActivity = activity.Def("ChunkProcessingActivity", chunkProcessingActivity);
