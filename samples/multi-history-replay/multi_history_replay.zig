//! Multi-history replay: list the runs of a workflow ID through a visibility query, fetch
//! each run's history, and replay them all against the current workflow code through one
//! replay worker. A run that no longer replays shows that a code change isn't backward
//! compatible with workflows already on the server.

const std = @import("std");
const temporal = @import("temporal");
const helloworld = @import("helloworld");

const Allocator = std.mem.Allocator;
const WorkflowReplayer = temporal.testing.WorkflowReplayer;

pub const task_queue = "multiple-history-replay";
pub const workflow_id = "multiple_history_replay_workflowID";
pub const query = "WorkflowId='" ++ workflow_id ++ "'";

/// Feeds `WorkflowReplayer.replayHistories` the histories of the runs a visibility query
/// lists, fetching each one as replay reaches it.
const ListedHistories = struct {
    client: *temporal.client.Client,
    executions: temporal.client.WorkflowExecutionIterator,
    count: usize = 0,

    pub fn next(h: *ListedHistories, arena: Allocator) !?temporal.history.WorkflowHistory {
        const info = try h.executions.next(arena) orelse return null;
        h.count += 1;
        return .{
            .workflow_id = info.workflow_id,
            .history = try h.client.fetchHistory(arena, info.workflow_id, info.run_id),
        };
    }
};

/// Replays the history of every run matching `visibility_query` with `replayer`'s workflows
/// and returns one result per run, in listing order. A run that fails to replay doesn't stop
/// the rest; listing or fetching errors are returned. Results are allocated from `arena`.
pub fn replayListed(
    replayer: *WorkflowReplayer,
    arena: Allocator,
    client: *temporal.client.Client,
    visibility_query: []const u8,
) ![]WorkflowReplayer.ReplayResult {
    var histories: ListedHistories = .{ .client = client, .executions = try client.listWorkflows(visibility_query, .{}) };
    defer histories.executions.deinit();
    return replayer.replayHistories(arena, &histories);
}

/// Replays every run of `workflow_id` against the hello world workflow and logs each result.
/// Returns whether all of them replayed cleanly; failures to list, fetch, or replay are logged.
pub fn replayAll(gpa: Allocator, io: std.Io, arena: Allocator, client: *temporal.client.Client) bool {
    var replayer: WorkflowReplayer = .initWithOptions(gpa, io, .{ .task_queue = task_queue });
    defer replayer.deinit();
    replayer.registerWorkflow(helloworld.HelloWorld) catch |err| {
        std.log.err("Unable to register the workflow for replay: {t}", .{err});
        return false;
    };

    std.log.info("Listing workflows Query={s}", .{query});
    const results = replayListed(&replayer, arena, client, query) catch |err| {
        std.log.err("Error listing or fetching workflow histories for \"{s}\": {t}", .{ query, err });
        return false;
    };
    std.log.info("Found workflows Count={d}", .{results.len});

    var failed: usize = 0;
    for (results) |result| {
        if (result.err) |err| {
            failed += 1;
            std.log.err("Error replaying history WorkflowID={s} RunID={s}: {t}: {s}", .{ result.workflow_id, result.run_id, err, result.message });
        } else {
            std.log.info("Replayed history WorkflowID={s} RunID={s}", .{ result.workflow_id, result.run_id });
        }
    }
    if (failed != 0) {
        std.log.err("{d} of {d} workflow histories failed to replay", .{ failed, results.len });
        return false;
    }
    std.log.info("All {d} workflow histories replayed without nondeterminism", .{results.len});
    return true;
}
