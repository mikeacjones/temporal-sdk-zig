//! Starts a cluster manager workflow and puts it through its paces: starts the cluster,
//! assigns six jobs two nodes each, lets the health check run for a while, deletes the jobs,
//! shuts the cluster down, and prints the result. The workflow checks its nodes every second
//! and continues as new as its history grows, so messages go to its latest run.

const std = @import("std");
const temporal = @import("temporal");
const safe_message_handler = @import("workflow.zig");

const Client = temporal.client.Client;

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;
    const arena = init.arena.allocator();

    // The client is a heavyweight object that should be created once per process.
    const options = try temporal.client.loadDefaultOptions(arena);
    var client = Client.connect(gpa, io, options) catch |err| {
        std.log.err("Unable to create client: {t}", .{err});
        std.process.exit(1);
    };
    defer client.close();

    var run = client.executeWorkflow(safe_message_handler.ClusterManagerWorkflow, .{
        .id = try std.fmt.allocPrint(arena, "ClusterManagerWorkflow-{s}", .{try temporal.client.uuid(arena, io)}),
        .task_queue = "safe-message-handlers-task-queue",
    }, .{.{ .test_continue_as_new = true }}) catch |err| {
        std.log.err("Unable to execute workflow: {t}", .{err});
        std.process.exit(1);
    };
    defer run.deinit();
    std.log.info("Started workflow WorkflowID={s} RunID={s}", .{ run.id, run.run_id });

    client.signalWorkflow(safe_message_handler.StartCluster, run.id, "", .{}) catch |err| {
        std.log.err("Unable to signal workflow: {t}", .{err});
        std.process.exit(1);
    };

    var assignments: [6]safe_message_handler.ClusterManagerAssignNodesToJobInput = undefined;
    var deletions: [6]safe_message_handler.ClusterManagerDeleteJobInput = undefined;
    for (&assignments, &deletions, 0..) |*assign, *delete, i| {
        const job_name = try std.fmt.allocPrint(arena, "task-{d}", .{i});
        assign.* = .{ .total_num_nodes = 2, .job_name = job_name };
        delete.* = .{ .job_name = job_name };
    }
    try updateAll(safe_message_handler.AssignNodesToJobs, &client, arena, run.id, &assignments);

    std.log.info("Sleeping", .{});
    try io.sleep(.fromSeconds(10), .awake);

    std.log.info("Deleting jobs...", .{});
    try updateAll(safe_message_handler.DeleteJob, &client, arena, run.id, &deletions);

    client.signalWorkflow(safe_message_handler.ShutdownCluster, run.id, "", .{}) catch |err| {
        std.log.err("Unable to signal workflow: {t}", .{err});
        std.process.exit(1);
    };

    // Synchronously wait for the workflow completion, following it across continue-as-new.
    const result = run.get(arena) catch |err| {
        std.log.err("Unable get workflow result: {t}", .{err});
        std.process.exit(1);
    };
    std.log.info("Cluster shut down successfully: num_currently_assigned_nodes={d} num_bad_nodes={d}", .{ result.num_currently_assigned_nodes, result.num_bad_nodes });
}

/// Sends one `Update` per element of `inputs` to the workflow's latest run, waiting only for
/// each to be accepted, then waits for them all to complete.
fn updateAll(comptime Update: type, client: *Client, arena: std.mem.Allocator, workflow_id: []const u8, inputs: anytype) !void {
    var handles: [inputs.len]temporal.client.WorkflowUpdateHandle(Update.Result) = undefined;
    for (&handles, inputs) |*handle, input| {
        handle.* = client.updateWorkflow(Update, workflow_id, "", .{input}, .{ .wait_for_stage = .accepted }) catch |err| {
            std.log.err("Unable to update workflow: {t}", .{err});
            std.process.exit(1);
        };
    }
    defer for (&handles) |*handle| handle.deinit();
    for (&handles) |*handle| {
        _ = handle.get(arena) catch |err| {
            std.log.err("Unable to get workflow update result: {t}", .{err});
            std.process.exit(1);
        };
    }
}
