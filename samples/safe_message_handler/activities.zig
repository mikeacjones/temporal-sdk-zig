//! Activities the cluster manager runs to change and check its nodes. They stand in for calls
//! to a real cluster.

const std = @import("std");
const temporal = @import("temporal");
const activity = temporal.activity;

pub const AssignNodesToJobInput = struct {
    nodes: []const []const u8,
    job_name: []const u8,
};

pub const UnassignNodesForJobInput = struct {
    nodes: []const []const u8,
    job_name: []const u8,
};

pub const FindBadNodesInput = struct {
    nodes_to_check: []const []const u8,
};

fn assignNodesToJobs(ctx: *activity.Context, input: AssignNodesToJobInput) !void {
    ctx.logger().info("Assigning nodes to job", .{ .nodes = try std.mem.join(ctx.allocator(), ",", input.nodes), .job = input.job_name });
    try ctx.io().sleep(.fromMilliseconds(1), .awake);
}

fn unassignNodesForJob(ctx: *activity.Context, input: UnassignNodesForJobInput) !void {
    ctx.logger().info("Unassigning nodes from job", .{ .nodes = try std.mem.join(ctx.allocator(), ",", input.nodes), .job = input.job_name });
    try ctx.io().sleep(.fromMilliseconds(1), .awake);
}

/// Returns the nodes to mark bad: here, every node whose number is a multiple of 5.
fn findBadNodes(ctx: *activity.Context, input: FindBadNodesInput) ![]const []const u8 {
    try ctx.io().sleep(.fromMilliseconds(1), .awake);
    var bad: std.ArrayList([]const u8) = .empty;
    for (input.nodes_to_check) |node| {
        const i = std.fmt.parseInt(u32, node, 10) catch |err| {
            ctx.logger().err("Failed to convert node to int", .{ .node = node, .err = err });
            return err;
        };
        if (i % 5 == 0) try bad.append(ctx.allocator(), node);
    }
    if (bad.items.len > 0) {
        ctx.logger().info("Found bad nodes", .{ .nodes = try std.mem.join(ctx.allocator(), ",", bad.items) });
    } else {
        ctx.logger().info("No new bad nodes found.", .{});
    }
    return bad.items;
}

pub const AssignNodesToJobsActivity = activity.Def("AssignNodesToJobsActivity", assignNodesToJobs);
pub const UnassignNodesForJobActivity = activity.Def("UnassignNodesForJobActivity", unassignNodesForJob);
pub const FindBadNodesActivity = activity.Def("FindBadNodesActivity", findBadNodes);
