//! Port of samples-go/splitmerge-future: run several activities in parallel and merge their
//! results by waiting on their futures in the order the activities were started. See
//! splitmerge-selector to process them in completion order instead.

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;
const activity = temporal.activity;

/// The activity result. Field names match the Go sample's JSON.
pub const ChunkResult = struct {
    NumberOfItemsInChunk: i64 = 0,
    SumInChunk: i64 = 0,
};

fn sampleSplitMergeFutureWorkflow(ctx: *workflow.Context, processor_count: i64) !ChunkResult {
    const a = ctx.allocator();
    const ao: workflow.ActivityOptions = .{ .start_to_close_timeout = .fromSeconds(10) };

    var results: std.ArrayList(workflow.ActivityFuture(ChunkResult)) = .empty;
    var i: i64 = 0;
    while (i < processor_count) : (i += 1) {
        // executeActivity returns a future that doesn't need to be awaited immediately.
        try results.append(a, ctx.executeActivity(ChunkProcessingActivity, ao, .{i + 1}));
    }

    var total: ChunkResult = .{};
    for (results.items) |future| {
        // Blocks until the activity result is available.
        const result = try future.get();
        total.NumberOfItemsInChunk += result.NumberOfItemsInChunk;
        total.SumInChunk += result.SumInChunk;
    }

    ctx.logger().info("Workflow completed.", .{});
    return total;
}

pub const SampleSplitMergeFutureWorkflow = workflow.Def("SampleSplitMergeFutureWorkflow", sampleSplitMergeFutureWorkflow);

fn chunkProcessingActivity(ctx: *activity.Context, chunk_id: i64) !ChunkResult {
    // Some fake processing logic here.
    ctx.logger().info("Chunk processed", .{ .chunkID = chunk_id });
    return .{ .NumberOfItemsInChunk = chunk_id, .SumInChunk = chunk_id * chunk_id };
}

pub const ChunkProcessingActivity = activity.Def("ChunkProcessingActivity", chunkProcessingActivity);
