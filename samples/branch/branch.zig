//! Port of samples-go/branch: a workflow that runs a variable number of activities in
//! parallel and collects their results in the order they were started.

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;
const activity = temporal.activity;

fn sampleActivity(ctx: *activity.Context, input: []const u8) ![]const u8 {
    const name = "sampleActivity";
    std.debug.print("Run {s} with input {s} \n", .{ name, input });
    return std.fmt.allocPrint(ctx.allocator(), "Result_{s}", .{input});
}

pub const SampleActivity = activity.Def("SampleActivity", sampleActivity);

/// Calls several activities in parallel; the number of branches is the workflow's argument.
fn sampleBranchWorkflow(ctx: *workflow.Context, total_branches: i64) ![]const []const u8 {
    const logger = ctx.logger();
    logger.info("SampleBranchWorkflow begin", .{});
    const a = ctx.allocator();
    const ao: workflow.ActivityOptions = .{ .start_to_close_timeout = .fromSeconds(10) };

    var futures: std.ArrayList(workflow.ActivityFuture([]const u8)) = .empty;
    var i: i64 = 1;
    while (i <= total_branches) : (i += 1) {
        const input = try std.fmt.allocPrint(a, "branch {d} of {d}.", .{ i, total_branches });
        try futures.append(a, ctx.executeActivity(SampleActivity, ao, .{input}));
    }
    logger.info("Activities started", .{});

    // Accumulate results.
    var result: std.ArrayList([]const u8) = .empty;
    for (futures.items) |future| {
        const single = try future.get();
        logger.info("Activity returned with result", .{ .resutl = single });
        try result.append(a, single);
    }

    logger.info("SampleBranchWorkflow end", .{});
    return result.items;
}

pub const SampleBranchWorkflow = workflow.Def("SampleBranchWorkflow", sampleBranchWorkflow);
