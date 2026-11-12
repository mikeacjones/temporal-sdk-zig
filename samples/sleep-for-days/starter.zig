//! Starts the sleep-for-days workflow and waits for its result. The workflow sends an email
//! every 30 days until it gets the "complete" signal, so the starter sends that signal itself
//! once the first email has been sent (it watches the workflow's history for it), and the run
//! ends. Pass --leave-running to leave the workflow running and return after starting it.

const std = @import("std");
const temporal = @import("temporal");
const sleep_for_days = @import("workflow.zig");

/// How long to wait for the first email.
const first_email_timeout: std.Io.Duration = .fromSeconds(30);
const history_poll_interval: std.Io.Duration = .fromMilliseconds(200);

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;
    const arena = init.arena.allocator();

    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.skip();
    const leave_running = if (args.next()) |a| std.mem.eql(u8, a, "--leave-running") else false;

    // The client is a heavyweight object that should be created once per process.
    const options = try temporal.client.loadDefaultOptions(arena);
    var client = temporal.client.Client.connect(gpa, io, options) catch |err| {
        std.log.err("Unable to create client: {t}", .{err});
        std.process.exit(1);
    };
    defer client.close();

    var run = client.executeWorkflow(sleep_for_days.SleepForDaysWorkflow, .{
        .id = try std.fmt.allocPrint(arena, "sleep-for-days-{s}", .{try temporal.client.uuid(arena, io)}),
        .task_queue = "sleep-for-days",
    }, .{}) catch |err| {
        std.log.err("Unable to execute workflow: {t}", .{err});
        std.process.exit(1);
    };
    defer run.deinit();
    std.log.info("Started sleep-for-days workflow WorkflowID={s} RunID={s}", .{ run.id, run.run_id });
    if (leave_running) return;

    awaitFirstEmail(&client, io, arena, run.id, run.run_id) catch |err| {
        std.log.err("Unable to see the first email of workflow {s}: {t}", .{ run.id, err });
        std.process.exit(1);
    };
    std.log.info("Sent the first email", .{});

    client.signalWorkflow(sleep_for_days.Complete, run.id, run.run_id, .{}) catch |err| {
        std.log.err("Unable to signal workflow {s} to complete: {t}", .{ run.id, err });
        std.process.exit(1);
    };

    const result = run.get(arena) catch |err| {
        std.log.err("Unable get workflow result: {t}", .{err});
        std.process.exit(1);
    };
    std.log.info("Workflow result: {s}", .{result});
}

/// Polls the run's history until it records a completed activity: the first email.
fn awaitFirstEmail(client: *temporal.client.Client, io: std.Io, arena: std.mem.Allocator, workflow_id: []const u8, run_id: []const u8) !void {
    const deadline = std.Io.Timestamp.now(io, .awake).addDuration(first_email_timeout);
    while (true) {
        var scratch: std.heap.ArenaAllocator = .init(arena);
        defer scratch.deinit();
        const history = try client.fetchHistory(scratch.allocator(), workflow_id, run_id);
        for (history.events.items) |event| {
            const attributes = event.attributes orelse continue;
            if (attributes == .activity_task_completed_event_attributes) return;
        }
        if (std.Io.Timestamp.now(io, .awake).durationTo(deadline).nanoseconds <= 0) return error.Timeout;
        try io.sleep(history_poll_interval, .awake);
    }
}
