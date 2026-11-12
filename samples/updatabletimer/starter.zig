//! Starts the updatable timer workflow with a wake-up time an hour away, queries it, then
//! signals it to wake up three seconds from now instead, so one program plays both the
//! starter and the updater. It waits for the workflow to wake up and complete.

const std = @import("std");
const temporal = @import("temporal");
const updatabletimer = @import("workflow.zig");

const Time = temporal.converter.Time;

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

    var buf: [Time.max_len]u8 = undefined;
    const wake_up_time: Time = .fromTimestamp(std.Io.Timestamp.now(io, .real).addDuration(.fromSeconds(std.time.s_per_hour)));
    var run = client.executeWorkflow(updatabletimer.UpdatableTimerWorkflow, .{
        .id = updatabletimer.workflow_id,
        .task_queue = updatabletimer.task_queue,
    }, .{wake_up_time}) catch |err| {
        std.log.err("Unable to start workflow: {t}", .{err});
        std.process.exit(1);
    };
    defer run.deinit();
    std.log.info("Started workflow that is going to block on an updatable timer WorkflowID={s} RunID={s} WakeUpTime={s}", .{ run.id, run.run_id, wake_up_time.format(&buf) });

    const queried = client.queryWorkflow(updatabletimer.GetWakeUpTime, arena, run.id, run.run_id, .{}) catch |err| {
        std.log.err("Unable to query workflow {s}: {t}", .{ run.id, err });
        std.process.exit(1);
    };
    std.log.info("Queried wake-up time WakeUpTime={s}", .{queried.format(&buf)});

    const new_wake_up_time: Time = .fromTimestamp(std.Io.Timestamp.now(io, .real).addDuration(.fromSeconds(3)));
    client.signalWorkflow(updatabletimer.UpdateWakeUpTime, run.id, "", .{new_wake_up_time}) catch |err| {
        std.log.err("Unable to signal workflow {s}: {t}", .{ run.id, err });
        std.process.exit(1);
    };
    std.log.info("Signaled workflow to update wake-up time WorkflowID={s} WakeUpTime={s}", .{ run.id, new_wake_up_time.format(&buf) });

    run.get(arena) catch |err| {
        std.log.err("Unable to get workflow result: {t}", .{err});
        std.process.exit(1);
    };
    std.log.info("Workflow woke up and completed WorkflowID={s}", .{run.id});
}
