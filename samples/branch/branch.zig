//! Branch: a workflow that runs a variable number of activities in parallel and collects
//! their results in the order they were started.

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;
const activity = temporal.activity;

fn processBranch(ctx: *activity.Context, input: []const u8) ![]const u8 {
    ctx.logger().info("Processing branch", .{ .input = input });
    return std.fmt.allocPrint(ctx.allocator(), "processed {s}", .{input});
}

pub const ProcessBranch = activity.Def("ProcessBranch", processBranch);

/// Calls several activities in parallel; the number of branches is the workflow's argument.
fn branch(ctx: *workflow.Context, total_branches: i64) ![]const []const u8 {
    const logger = ctx.logger();
    logger.info("Branch workflow begin", .{});
    const a = ctx.allocator();
    const ao: workflow.ActivityOptions = .{ .start_to_close_timeout = .fromSeconds(10) };

    var futures: std.ArrayList(workflow.ActivityFuture([]const u8)) = .empty;
    var i: i64 = 1;
    while (i <= total_branches) : (i += 1) {
        const input = try std.fmt.allocPrint(a, "branch {d} of {d}", .{ i, total_branches });
        try futures.append(a, ctx.executeActivity(ProcessBranch, ao, .{input}));
    }
    logger.info("Activities started", .{});

    // Accumulate results.
    var result: std.ArrayList([]const u8) = .empty;
    for (futures.items) |future| {
        const single = try future.get();
        logger.info("Activity returned with result", .{ .result = single });
        try result.append(a, single);
    }

    logger.info("Branch workflow end", .{});
    return result.items;
}

pub const Branch = workflow.Def("Branch", branch);
