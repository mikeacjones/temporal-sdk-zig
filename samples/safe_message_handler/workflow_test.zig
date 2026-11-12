//! Tests for the safe_message_handler sample, on the time-skipping server: time stays locked
//! while the tests send messages, so the health check's timer only fires inside `env.sleep`
//! and `getResult`.

const std = @import("std");
const temporal = @import("temporal");
const safe_message_handler = @import("workflow.zig");
const activities = @import("activities.zig");

const testing = std.testing;
const WorkflowEnvironment = temporal.testing.WorkflowEnvironment;

fn startEnv() !*WorkflowEnvironment {
    const env = try WorkflowEnvironment.startTimeSkipping(testing.allocator, testing.io);
    errdefer env.deinit();
    try env.registerWorkflow(safe_message_handler.ClusterManagerWorkflow);
    try env.registerActivity(activities.AssignNodesToJobsActivity);
    try env.registerActivity(activities.UnassignNodesForJobActivity);
    try env.registerActivity(activities.FindBadNodesActivity);
    return env;
}

test "jobs sent before the cluster starts get nodes of their own, and are deleted" {
    const env = try startEnv();
    defer env.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var run = try env.startWorkflow(safe_message_handler.ClusterManagerWorkflow, .{ .task_queue = "" }, .{.{}});
    defer run.deinit();

    // The handlers wait for the cluster to start, then take the node lock one at a time.
    var handles: [6]temporal.client.WorkflowUpdateHandle(safe_message_handler.ClusterManagerAssignNodesToJobResult) = undefined;
    var sent: usize = 0;
    defer for (handles[0..sent]) |*h| h.deinit();
    for (&handles, 0..) |*h, i| {
        h.* = try env.client.updateWorkflow(safe_message_handler.AssignNodesToJobs, run.id, "", .{.{
            .job_name = try std.fmt.allocPrint(a, "TestJobID-{d}", .{i}),
            .total_num_nodes = 2,
        }}, .{ .wait_for_stage = .accepted });
        sent += 1;
    }
    try env.signalWorkflow(safe_message_handler.StartCluster, run.id, .{});

    var seen: std.StringHashMapUnmanaged(void) = .empty;
    for (&handles) |*h| {
        const result = try h.get(a);
        try testing.expectEqual(2, result.nodes_assigned.len);
        for (result.nodes_assigned) |node| {
            const gop = try seen.getOrPut(a, node);
            try testing.expect(!gop.found_existing);
        }
    }

    for (0..handles.len) |i| {
        try env.updateWorkflow(safe_message_handler.DeleteJob, a, run.id, .{.{
            .job_name = try std.fmt.allocPrint(a, "TestJobID-{d}", .{i}),
        }}, null);
    }
    try env.signalWorkflow(safe_message_handler.ShutdownCluster, run.id, .{});

    const result = try env.getResult(safe_message_handler.ClusterManagerWorkflow, &run, a, null);
    try testing.expectEqual(25, result.num_currently_assigned_nodes);
    try testing.expectEqual(0, result.num_bad_nodes);
}

test "assigning more nodes than are free fails the update, not the workflow" {
    const env = try startEnv();
    defer env.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var run = try env.startWorkflow(safe_message_handler.ClusterManagerWorkflow, .{ .task_queue = "" }, .{.{}});
    defer run.deinit();
    try env.signalWorkflow(safe_message_handler.StartCluster, run.id, .{});

    const big = try env.updateWorkflow(safe_message_handler.AssignNodesToJobs, a, run.id, .{.{ .job_name = "big-task", .total_num_nodes = 24 }}, null);
    try testing.expectEqual(24, big.nodes_assigned.len);

    var failure: ?temporal.failure.Failure = null;
    try testing.expectError(error.UpdateFailed, env.updateWorkflow(safe_message_handler.AssignNodesToJobs, a, run.id, .{.{ .job_name = "little-task", .total_num_nodes = 3 }}, &failure));
    try testing.expectEqualStrings("NotEnoughNodesToAssignToJob", failure.?.message);

    try env.signalWorkflow(safe_message_handler.ShutdownCluster, run.id, .{});
    _ = try env.getResult(safe_message_handler.ClusterManagerWorkflow, &run, a, null);
}

test "health checks mark bad nodes, and the state survives continue-as-new" {
    const env = try startEnv();
    defer env.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var run = try env.startWorkflow(safe_message_handler.ClusterManagerWorkflow, .{ .task_queue = "" }, .{.{ .test_continue_as_new = true }});
    defer run.deinit();
    try env.signalWorkflow(safe_message_handler.StartCluster, run.id, .{});

    const assign: safe_message_handler.ClusterManagerAssignNodesToJobInput = .{ .job_name = "job", .total_num_nodes = 2 };
    const first = try env.updateWorkflow(safe_message_handler.AssignNodesToJobs, a, run.id, .{assign}, null);
    try testing.expectEqual(2, first.nodes_assigned.len);
    try testing.expectEqualStrings("0", first.nodes_assigned[0]);
    try testing.expectEqualStrings("1", first.nodes_assigned[1]);

    // A health check a second; the history passes its limit within a few dozen.
    try env.sleep(.fromSeconds(60));
    const latest = try env.client.describeWorkflow(a, run.id, "");
    try testing.expect(!std.mem.eql(u8, latest.run_id, run.run_id));

    // Assigning the job again is a no-op that returns its nodes, less node 0, which the
    // health check marked bad.
    const again = try env.updateWorkflow(safe_message_handler.AssignNodesToJobs, a, run.id, .{assign}, null);
    try testing.expectEqual(1, again.nodes_assigned.len);
    try testing.expectEqualStrings("1", again.nodes_assigned[0]);

    try env.updateWorkflow(safe_message_handler.DeleteJob, a, run.id, .{.{ .job_name = "job" }}, null);
    try env.signalWorkflow(safe_message_handler.ShutdownCluster, run.id, .{});

    const result = try env.getResult(safe_message_handler.ClusterManagerWorkflow, &run, a, null);
    try testing.expectEqual(20, result.num_currently_assigned_nodes);
    try testing.expectEqual(5, result.num_bad_nodes);
}

test "the health check activity reports nodes whose number is a multiple of 5" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const env: temporal.testing.ActivityEnvironment = .init(testing.io);
    const bad = try env.executeActivity(activities.FindBadNodesActivity, .{.{ .nodes_to_check = &.{ "0", "3", "10", "12" } }}, arena.allocator());
    try testing.expectEqual(2, bad.len);
    try testing.expectEqualStrings("0", bad[0]);
    try testing.expectEqualStrings("10", bad[1]);
}
