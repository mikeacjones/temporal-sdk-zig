//! Multi-history replay: replay every run of a workflow ID that a visibility query lists
//! against the current workflow code through one replay worker
//! (`WorkflowReplayer.replayListedWorkflows` lists the runs and fetches their histories).
//! A run that no longer replays shows that a code change isn't backward compatible with
//! workflows already on the server.

const std = @import("std");
const temporal = @import("temporal");
const helloworld = @import("helloworld");

const Allocator = std.mem.Allocator;
const WorkflowReplayer = temporal.testing.WorkflowReplayer;

pub const task_queue = "multiple-history-replay";
pub const workflow_id = "multiple_history_replay_workflowID";
pub const query = "WorkflowId='" ++ workflow_id ++ "'";

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
    const results = replayer.replayListedWorkflows(arena, client, query) catch |err| {
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
