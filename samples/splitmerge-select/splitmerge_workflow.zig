//! Split-merge with `std.Io.Select`: run several activities in parallel and merge their
//! results as each one completes. See splitmerge-future to merge them in the order they were
//! started instead.

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;
const activity = temporal.activity;

/// What processing a chunk produced.
pub const ChunkResult = struct {
    items: i64 = 0,
    sum: i64 = 0,
};

const Future = workflow.ActivityFuture(ChunkResult);
const Ready = union(enum) { chunk: workflow.ActivityError!ChunkResult };

fn splitMergeSelect(ctx: *workflow.Context, activity_count: i64) !ChunkResult {
    const a = ctx.allocator();
    const ao: workflow.ActivityOptions = .{ .start_to_close_timeout = .fromSeconds(10) };

    const n: usize = @intCast(@max(activity_count, 0));
    const buffer = try a.alloc(Ready, n);
    var select: std.Io.Select(Ready) = .init(ctx.io(), buffer);
    defer select.cancelDiscard();
    for (0..n) |i| {
        const future = ctx.executeActivity(ProcessChunk, ao, .{@as(i64, @intCast(i + 1))});
        select.async(.chunk, Future.get, .{future});
    }

    var total: ChunkResult = .{};
    for (0..n) |_| {
        const ready = try select.await();
        const r = try ready.chunk;
        total.items += r.items;
        total.sum += r.sum;
    }

    ctx.logger().info("Workflow completed.", .{});
    return total;
}

pub const SplitMergeSelect = workflow.Def("SplitMergeSelect", splitMergeSelect);

fn processChunk(ctx: *activity.Context, chunk_id: i64) !ChunkResult {
    // Some fake processing logic here.
    ctx.logger().info("Chunk processed", .{ .chunk_id = chunk_id });
    return .{ .items = chunk_id, .sum = chunk_id * chunk_id };
}

pub const ProcessChunk = activity.Def("ProcessChunk", processChunk);
