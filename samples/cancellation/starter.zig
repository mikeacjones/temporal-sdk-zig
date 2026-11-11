//! Starts the cancellation workflow and then cancels it, so one program shows the whole
//! sample: once the activity is heartbeating, the starter requests the workflow's
//! cancellation, waits for the run to end canceled, and checks the run's history to show that
//! the cleanup activity ran after the canceled activity stopped.

const std = @import("std");
const temporal = @import("temporal");
const cancellation = @import("workflow.zig");

const History = temporal.proto.temporal.api.history.v1.History;

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;
    const arena = init.arena.allocator();

    // The client is a heavyweight object that should be created once per process.
    const options = try temporal.client.loadDefaultOptions(arena);
    var client = temporal.client.Client.connect(gpa, io, options) catch |err| {
        std.log.err("Unable to create client: {t}", .{err});
        std.process.exit(1);
    };
    defer client.close();

    const workflow_id = try std.fmt.allocPrint(arena, "workflowID-to-cancel-{s}", .{try temporal.client.uuid(arena, io)});
    var run = client.executeWorkflow(cancellation.YourWorkflow, .{
        .id = workflow_id,
        .task_queue = "cancel-activity",
    }, .{}) catch |err| {
        std.log.err("Unable to execute workflow: {t}", .{err});
        std.process.exit(1);
    };
    defer run.deinit();
    std.log.info("Started workflow WorkflowID={s} RunID={s}", .{ run.id, run.run_id });

    waitForHeartbeat(&client, gpa, io, run.id, run.run_id) catch |err| {
        std.log.err("Waiting for the activity of workflow {s} to heartbeat failed: {t}", .{ run.id, err });
        std.process.exit(1);
    };

    client.cancelWorkflow(run.id, run.run_id, "canceled by the cancellation sample") catch |err| {
        std.log.err("Unable to cancel Workflow Execution {s}: {t}", .{ run.id, err });
        std.process.exit(1);
    };
    std.log.info("Workflow Execution cancelled WorkflowID={s}", .{run.id});

    if (run.get(arena)) |_| {
        std.log.err("Workflow {s} completed instead of ending canceled", .{run.id});
        std.process.exit(1);
    } else |err| switch (err) {
        error.WorkflowCanceled => std.log.info("Workflow canceled WorkflowID={s}", .{run.id}),
        else => {
            std.log.err("Unable get workflow result: {t}", .{err});
            std.process.exit(1);
        },
    }

    const history = client.fetchHistory(arena, run.id, run.run_id) catch |err| {
        std.log.err("Unable to fetch the history of workflow {s}: {t}", .{ run.id, err });
        std.process.exit(1);
    };
    checkCleanup(history) catch |err| {
        std.log.err("Workflow {s} did not clean up after its activity was canceled: {t}", .{ run.id, err });
        std.process.exit(1);
    };
    std.log.info("Cleanup activity completed after ActivityToBeCanceled was canceled", .{});
}

/// Polls the workflow's description until its activity has recorded a heartbeat, for up to
/// 30 seconds.
fn waitForHeartbeat(client: *temporal.client.Client, gpa: std.mem.Allocator, io: std.Io, workflow_id: []const u8, run_id: []const u8) !void {
    var scratch: std.heap.ArenaAllocator = .init(gpa);
    defer scratch.deinit();
    for (0..60) |_| {
        _ = scratch.reset(.retain_capacity);
        const description = try client.describeWorkflow(scratch.allocator(), workflow_id, run_id);
        if (description.status != .running) return error.WorkflowNotRunning;
        for (description.raw.pending_activities.items) |pending| {
            if (pending.last_heartbeat_time != null) return;
        }
        try io.sleep(.fromMilliseconds(500), .awake);
    }
    return error.NoHeartbeatWithin30Seconds;
}

/// Checks that `ActivityToBeCanceled` ended canceled, and that `CleanupActivity` was scheduled
/// after that and completed.
fn checkCleanup(history: History) !void {
    var canceled_scheduled_id: ?i64 = null;
    var canceled_at: ?i64 = null;
    var cleanup_scheduled_id: ?i64 = null;
    for (history.events.items) |event| {
        switch (event.attributes orelse continue) {
            .activity_task_scheduled_event_attributes => |a| {
                const name = if (a.activity_type) |t| t.name else "";
                if (std.mem.eql(u8, name, cancellation.ActivityToBeCanceled.activity_name)) {
                    canceled_scheduled_id = event.event_id;
                } else if (std.mem.eql(u8, name, cancellation.CleanupActivity.activity_name)) {
                    if (canceled_at == null) return error.CleanupScheduledBeforeActivityStopped;
                    cleanup_scheduled_id = event.event_id;
                }
            },
            .activity_task_canceled_event_attributes => |a| {
                if (a.scheduled_event_id == canceled_scheduled_id) canceled_at = event.event_id;
            },
            .activity_task_completed_event_attributes => |a| {
                if (a.scheduled_event_id == cleanup_scheduled_id) return;
            },
            else => {},
        }
    }
    return if (canceled_at == null) error.ActivityNotCanceled else error.CleanupNotCompleted;
}
