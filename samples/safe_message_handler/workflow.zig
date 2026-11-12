//! Safe message handling: a cluster manager workflow that keeps track of which jobs a cluster's
//! nodes are assigned to.
//!
//! - The StartCluster and ShutdownCluster signals start and shut down the cluster. The
//!   AssignNodesToJobs and DeleteJob updates only act between the two: they wait for the
//!   cluster to start and fail once it is shut down.
//! - The update handlers and the periodic health check each run an activity while they change
//!   the nodes. They run in coroutines of their own and interleave, so each holds `node_lock`
//!   (a `std.Io.Mutex` on the workflow's `std.Io`) across its activity.
//! - The workflow waits for every handler to finish before it continues as new or returns, so
//!   none is cut short.
//! - The workflow is long-running, so it continues as new, carrying its state, when the server
//!   suggests it or the history passes a limit set for testing.
//! - AssignNodesToJobs is idempotent: assigning a job again returns the nodes it already has.

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;
const activities = @import("activities.zig");

pub const StartCluster = workflow.SignalDef("StartCluster", fn () void);
pub const ShutdownCluster = workflow.SignalDef("ShutdownCluster", fn () void);
/// Returns the nodes the job is assigned to.
pub const AssignNodesToJobs = workflow.UpdateDef("AssignNodesToJobs", fn (ClusterManagerAssignNodesToJobInput) ClusterManagerAssignNodesToJobResult);
/// An update rather than a signal, so the caller can wait for the nodes to be freed.
pub const DeleteJob = workflow.UpdateDef("DeleteJob", fn (ClusterManagerDeleteJobInput) void);

/// Everything one run hands to the next when the workflow continues as new.
pub const ClusterManagerState = struct {
    cluster_started: bool = false,
    cluster_shutdown: bool = false,
    /// Node name to the job it is assigned to: `unassigned` when free, `bad_node` once a
    /// health check found it bad.
    nodes: std.json.ArrayHashMap([]const u8) = .{},
    jobs_assigned: []const []const u8 = &.{},
};

pub const ClusterManagerInput = struct {
    /// The previous run's state, or null for a new cluster.
    state: ?ClusterManagerState = null,
    /// Checks the nodes every second and continues as new after 120 history events, so a
    /// short run shows both.
    test_continue_as_new: bool = false,
};

pub const ClusterManagerResult = struct {
    /// Nodes not marked bad.
    num_currently_assigned_nodes: usize,
    num_bad_nodes: usize,
};

pub const ClusterManagerAssignNodesToJobInput = struct {
    /// More or fewer than the job has resizes it.
    total_num_nodes: u32,
    job_name: []const u8,
};

pub const ClusterManagerDeleteJobInput = struct {
    job_name: []const u8,
};

pub const ClusterManagerAssignNodesToJobResult = struct {
    /// In node-name order.
    nodes_assigned: []const []const u8,
};

const unassigned = "";
const bad_node = "BAD!";
const node_count = 25;

/// The workflow's state and its message handlers. Allocated with `ctx.allocator()`, so the
/// handlers' coroutines can use it for the whole run.
const ClusterManager = struct {
    ctx: *workflow.Context,
    state: ClusterManagerState,
    node_lock: std.Io.Mutex = .init,
    sleep_interval: std.Io.Duration,
    /// 0 means no limit.
    max_history_length: u32,
    test_continue_as_new: bool,

    fn startCluster(cm: *ClusterManager) void {
        cm.state.cluster_started = true;
        cm.ctx.logger().info("Cluster started", .{});
    }

    fn shutdownCluster(cm: *ClusterManager) void {
        cm.state.cluster_shutdown = true;
    }

    fn started(cm: *ClusterManager) bool {
        return cm.state.cluster_started;
    }

    fn shutDown(cm: *ClusterManager) bool {
        return cm.state.cluster_shutdown;
    }

    fn assignNodesToJobs(cm: *ClusterManager, input: ClusterManagerAssignNodesToJobInput) !ClusterManagerAssignNodesToJobResult {
        try cm.ctx.await(cm, started);
        if (cm.state.cluster_shutdown) return error.ClusterAlreadyShutDown;
        const io = cm.ctx.io();
        try cm.node_lock.lock(io);
        defer cm.node_lock.unlock(io);

        for (cm.state.jobs_assigned) |job| {
            if (std.mem.eql(u8, job, input.job_name)) return .{ .nodes_assigned = try cm.assignedNodes(input.job_name) };
        }
        const free = try cm.assignedNodes(unassigned);
        if (free.len < input.total_num_nodes) return error.NotEnoughNodesToAssignToJob;
        const nodes = free[0..input.total_num_nodes];

        // The lock keeps DeleteJob and the health check from changing the nodes while this
        // waits for the activity.
        try cm.ctx.executeActivity(activities.AssignNodesToJobsActivity, .{
            .schedule_to_close_timeout = .fromSeconds(10),
        }, .{.{ .nodes = nodes, .job_name = input.job_name }}).get();
        for (nodes) |node| cm.state.nodes.map.getPtr(node).?.* = input.job_name;
        cm.state.jobs_assigned = try std.mem.concat(cm.ctx.allocator(), []const u8, &.{ cm.state.jobs_assigned, &.{input.job_name} });
        return .{ .nodes_assigned = try cm.assignedNodes(input.job_name) };
    }

    fn deleteJob(cm: *ClusterManager, input: ClusterManagerDeleteJobInput) !void {
        try cm.ctx.await(cm, started);
        if (cm.state.cluster_shutdown) return error.ClusterAlreadyShutDown;
        const io = cm.ctx.io();
        try cm.node_lock.lock(io);
        defer cm.node_lock.unlock(io);

        const nodes = try cm.assignedNodes(input.job_name);
        try cm.ctx.executeActivity(activities.UnassignNodesForJobActivity, .{
            .schedule_to_close_timeout = .fromSeconds(10),
        }, .{.{ .nodes = nodes, .job_name = input.job_name }}).get();
        for (nodes) |node| cm.state.nodes.map.getPtr(node).?.* = unassigned;
    }

    /// Marks the nodes the health check activity reports as bad. A failed check is logged and
    /// skipped; the next one runs after `sleep_interval`.
    fn performHealthCheck(cm: *ClusterManager) !void {
        const io = cm.ctx.io();
        try cm.node_lock.lock(io);
        defer cm.node_lock.unlock(io);

        const bad = cm.ctx.executeActivity(activities.FindBadNodesActivity, .{
            .start_to_close_timeout = .fromSeconds(10),
            .retry_policy = .{ .maximum_attempts = 1 },
        }, .{.{ .nodes_to_check = try cm.assignedNodes(null) }}).get() catch |err| switch (err) {
            error.Canceled, error.OutOfMemory => |e| return e,
            else => {
                cm.ctx.logger().err("Health check failed", .{ .err = err });
                return;
            },
        };
        for (bad) |node| if (cm.state.nodes.map.getPtr(node)) |job| {
            job.* = bad_node;
        };
    }

    /// The nodes assigned to `job` (`unassigned` gives the free nodes), or with null every
    /// node not marked bad, in node-name order.
    fn assignedNodes(cm: *ClusterManager, job: ?[]const u8) ![]const []const u8 {
        var nodes: std.ArrayList([]const u8) = .empty;
        for (cm.state.nodes.map.keys(), cm.state.nodes.map.values()) |node, assigned| {
            const match = if (job) |j| std.mem.eql(u8, assigned, j) else !std.mem.eql(u8, assigned, bad_node);
            if (match) try nodes.append(cm.ctx.allocator(), node);
        }
        return nodes.items;
    }

    fn shouldContinueAsNew(cm: *ClusterManager) bool {
        if (cm.ctx.continueAsNewSuggested()) return true;
        return cm.max_history_length > 0 and cm.ctx.historyLength() > cm.max_history_length;
    }
};

/// Orders the nodes map by node name.
const ByNodeName = struct {
    names: []const []const u8,

    pub fn lessThan(c: ByNodeName, a: usize, b: usize) bool {
        return std.mem.lessThan(u8, c.names[a], c.names[b]);
    }
};

fn clusterManagerWorkflow(ctx: *workflow.Context, input: ClusterManagerInput) !ClusterManagerResult {
    const log = ctx.logger();
    const a = ctx.allocator();
    const cm = try a.create(ClusterManager);
    cm.* = .{
        .ctx = ctx,
        .state = input.state orelse .{},
        .sleep_interval = if (input.test_continue_as_new) .fromSeconds(1) else .fromSeconds(600),
        .max_history_length = if (input.test_continue_as_new) 120 else 0,
        .test_continue_as_new = input.test_continue_as_new,
    };
    if (input.state == null) {
        for (0..node_count) |i| try cm.state.nodes.map.put(a, try std.fmt.allocPrint(a, "{d}", .{i}), unassigned);
    }
    // Every walk over the nodes goes in node-name order, the same on every replay.
    cm.state.nodes.map.sort(ByNodeName{ .names = cm.state.nodes.map.keys() });

    try ctx.setSignalHandler(StartCluster, cm, ClusterManager.startCluster);
    try ctx.setSignalHandler(ShutdownCluster, cm, ClusterManager.shutdownCluster);
    try ctx.setUpdateHandler(AssignNodesToJobs, cm, ClusterManager.assignNodesToJobs);
    try ctx.setUpdateHandler(DeleteJob, cm, ClusterManager.deleteJob);

    try ctx.await(cm, ClusterManager.started);
    while (!try ctx.awaitTimeout(cm.sleep_interval, cm, ClusterManager.shutDown)) {
        try cm.performHealthCheck();
        if (cm.shouldContinueAsNew()) {
            // Let job assignments and deletions in progress finish in this run.
            try ctx.await(ctx, workflow.Context.allHandlersFinished);
            log.info("Continuing as new", .{});
            return ctx.continueAsNew(ClusterManagerWorkflow, .{}, .{.{
                .state = cm.state,
                .test_continue_as_new = cm.test_continue_as_new,
            }});
        }
    }

    // Let job deletions in progress finish before the workflow completes.
    try ctx.await(ctx, workflow.Context.allHandlersFinished);
    const bad = try cm.assignedNodes(bad_node);
    return .{
        .num_currently_assigned_nodes = (try cm.assignedNodes(null)).len,
        .num_bad_nodes = bad.len,
    };
}

/// Keeps track of the jobs a cluster's nodes are assigned to. Signals start and shut down the
/// cluster; updates assign nodes to jobs and delete jobs, one at a time.
pub const ClusterManagerWorkflow = workflow.Def("ClusterManagerWorkflow", clusterManagerWorkflow);
