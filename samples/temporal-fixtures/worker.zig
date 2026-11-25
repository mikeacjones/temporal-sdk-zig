//! Runs a worker for each fixture, each on its own task queue, sharing one client. No worker
//! polls `stuck_workflows.no_worker_task_queue`. One SIGINT or SIGTERM stops them all; the
//! activities still sleeping are left open on the server, so their runs stay running.

const std = @import("std");
const temporal = @import("temporal");
const rainbow_statuses = @import("rainbow_statuses.zig");
const large_event_history = @import("large_event_history.zig");
const largepayload = @import("largepayload.zig");
const open_n_closed = @import("open_n_closed.zig");
const stuck_workflows = @import("stuck_workflows.zig");

const Worker = temporal.worker.Worker;

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;

    // The client and workers are heavyweight objects that should be created once per process.
    const options = try temporal.client.loadDefaultOptions(init.arena.allocator());
    var client = temporal.client.Client.connect(gpa, io, options) catch |err| {
        std.log.err("Unable to create client: {t}", .{err});
        std.process.exit(1);
    };
    defer client.close();

    var rainbow_worker = try Worker.init(gpa, io, &client, rainbow_statuses.task_queue, .{});
    defer rainbow_worker.deinit();
    try rainbow_worker.registerWorkflow(rainbow_statuses.RainbowStatusesWorkflow);
    try rainbow_worker.registerActivity(rainbow_statuses.CompletedActivity);
    try rainbow_worker.registerActivity(rainbow_statuses.FailedActivity);
    try rainbow_worker.registerActivity(rainbow_statuses.LongActivity);

    var history_worker = try Worker.init(gpa, io, &client, large_event_history.task_queue, .{});
    defer history_worker.deinit();
    try history_worker.registerWorkflow(large_event_history.LargeEventHistoryWorkflow);
    try history_worker.registerActivity(large_event_history.Activity);

    var payload_worker = try Worker.init(gpa, io, &client, largepayload.task_queue, .{});
    defer payload_worker.deinit();
    try payload_worker.registerWorkflow(largepayload.LargePayloadWorkflow);
    try payload_worker.registerActivity(largepayload.CreateLargeResultActivity);
    try payload_worker.registerActivity(largepayload.ProcessLargeInputActivity);

    var open_worker = try Worker.init(gpa, io, &client, open_n_closed.task_queue, .{});
    defer open_worker.deinit();
    try open_worker.registerWorkflow(open_n_closed.OpenNClosedWorkflow);
    try open_worker.registerActivity(open_n_closed.Activity);

    var stuck_worker = try Worker.init(gpa, io, &client, stuck_workflows.task_queue, .{});
    defer stuck_worker.deinit();
    try stuck_worker.registerWorkflow(stuck_workflows.StuckWorkflow);
    try stuck_worker.registerActivity(stuck_workflows.StuckWorkflowActivity);

    const workers = [_]*Worker{ &rainbow_worker, &history_worker, &payload_worker, &open_worker, &stuck_worker };

    // If starting a later worker fails, stop and join the ones already running before their
    // `defer deinit` and `client.close()` run. Declared after those, so it runs first; a stop
    // before `run` is harmless and permanent.
    var group: std.Io.Group = .init;
    errdefer {
        for (workers) |w| w.stop();
        group.await(io) catch group.cancel(io);
    }
    for (workers) |w| try group.concurrent(io, runWorker, .{w});
    // Wait for all five workers to stop.
    try group.await(io);
}

fn runWorker(w: *Worker) void {
    w.runUntilSignal() catch |err| {
        std.log.err("Unable to start worker for task queue {s}: {t}", .{ w.task_queue, err });
        std.process.exit(1);
    };
}
