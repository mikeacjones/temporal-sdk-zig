//! Tests for the open and closed fixture.

const std = @import("std");
const temporal = @import("temporal");
const open_n_closed = @import("open_n_closed.zig");

const testing = std.testing;

test "a workflow not kept open returns the activity's greeting" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(open_n_closed.OpenNClosedWorkflow);
    try env.registerActivity(open_n_closed.Activity);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const result = try env.executeWorkflow(open_n_closed.OpenNClosedWorkflow, .{false}, arena.allocator(), null);
    try testing.expectEqualStrings("Hello!", result);
}

test "a run kept open stays open when its worker stops during the activity" {
    // A run kept open is for the UI's open runs. Stopping the worker (SIGINT or SIGTERM to
    // worker.zig) must not fail the sleeping activity: with one attempt allowed, a failed
    // attempt fails the run as soon as a worker polls the task queue again.
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const task_queue = "open-n-closed-worker-stop";
    var w = try temporal.worker.Worker.init(testing.allocator, testing.io, &env.client, task_queue, .{});
    defer w.deinit();
    try w.registerWorkflow(open_n_closed.OpenNClosedWorkflow);
    try w.registerActivity(open_n_closed.Activity);
    var running = try testing.io.concurrent(temporal.worker.Worker.run, .{&w});
    var stopped = false;
    defer if (!stopped) {
        w.stop();
        running.await(testing.io) catch {};
    };
    try w.waitUntilRunning();

    var run = try env.client.executeWorkflow(open_n_closed.OpenNClosedWorkflow, .{ .id = "worker-stop###___1", .task_queue = task_queue }, .{true});
    defer run.deinit();
    for (0..600) |_| {
        const d = try env.client.describeWorkflow(a, run.id, run.run_id);
        const pending = d.raw.pending_activities.items;
        if (pending.len == 1 and pending[0].state == .PENDING_ACTIVITY_STATE_STARTED) break;
        try testing.io.sleep(.fromMilliseconds(50), .awake);
    } else return error.ActivityNeverStarted;

    w.stop();
    stopped = true;
    try running.await(testing.io);

    const history = try env.client.fetchHistory(a, run.id, run.run_id);
    for (history.events.items) |event| switch (event.attributes orelse continue) {
        .activity_task_failed_event_attributes => |f| {
            std.debug.print("the activity failed when the worker stopped: {s}\n", .{if (f.failure) |x| x.message else ""});
            return error.TestUnexpectedResult;
        },
        else => {},
    };
    try testing.expectEqual(.running, (try env.client.describeWorkflow(a, run.id, run.run_id)).status);
    try env.client.terminateWorkflow(run.id, run.run_id, "test cleanup");
}
