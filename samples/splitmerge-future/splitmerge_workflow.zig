//! Split-merge with futures: run several activities in parallel and merge their results by
//! waiting on their futures in the order the activities were started. See splitmerge-select
//! to merge them in completion order instead.

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;
const activity = temporal.activity;

/// What processing a chunk produced.
pub const ChunkResult = struct {
    items: i64 = 0,
    sum: i64 = 0,
};

fn splitMergeFuture(ctx: *workflow.Context, activity_count: i64) !ChunkResult {
    const a = ctx.allocator();
    const ao: workflow.ActivityOptions = .{ .start_to_close_timeout = .fromSeconds(10) };

    var results: std.ArrayList(workflow.ActivityFuture(ChunkResult)) = .empty;
    var i: i64 = 0;
    while (i < activity_count) : (i += 1) {
        // executeActivity returns a future that doesn't need to be awaited immediately.
        try results.append(a, ctx.executeActivity(ProcessChunk, ao, .{i + 1}));
    }

    var total: ChunkResult = .{};
    for (results.items) |future| {
        // Blocks until the activity result is available.
        const result = try future.get();
        total.items += result.items;
        total.sum += result.sum;
    }

    ctx.logger().info("Workflow completed.", .{});
    return total;
}

pub const SplitMergeFuture = workflow.Def("SplitMergeFuture", splitMergeFuture);

fn processChunk(ctx: *activity.Context, chunk_id: i64) !ChunkResult {
    // Some fake processing logic here.
    ctx.logger().info("Chunk processed", .{ .chunk_id = chunk_id });
    return .{ .items = chunk_id, .sum = chunk_id * chunk_id };
}

pub const ProcessChunk = activity.Def("ProcessChunk", processChunk);
