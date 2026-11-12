//! Runs the hello world workflow several times under one workflow ID, one run after another,
//! to simulate existing workflows on the server. Then it plays the replayer program's part:
//! once visibility lists every run, it replays all their histories.

const std = @import("std");
const temporal = @import("temporal");
const helloworld = @import("helloworld");
const multi_history_replay = @import("multi_history_replay.zig");

const number_of_workflows = 10;

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

    // Runs from earlier invocations count too, so wait for this many more than now.
    const completed_query = multi_history_replay.query ++ " AND ExecutionStatus='Completed'";
    const before = client.countWorkflows(completed_query) catch |err| {
        std.log.err("Unable to count workflows matching \"{s}\": {t}", .{ completed_query, err });
        std.process.exit(1);
    };

    for (0..number_of_workflows) |_| {
        var run = client.executeWorkflow(helloworld.HelloWorld, .{
            .id = multi_history_replay.workflow_id,
            .task_queue = multi_history_replay.task_queue,
        }, .{"Temporal"}) catch |err| {
            std.log.err("Unable to execute workflow: {t}", .{err});
            std.process.exit(1);
        };
        defer run.deinit();
        std.log.info("Started workflow WorkflowID={s} RunID={s}", .{ run.id, run.run_id });

        // Wait for the workflow to finish.
        _ = run.get(arena) catch |err| {
            std.log.err("Unable get workflow result: {t}", .{err});
            std.process.exit(1);
        };
        std.log.info("Workflow finished WorkflowID={s} RunID={s}", .{ run.id, run.run_id });
    }

    // Visibility lists a run a moment after it closes.
    var tries: usize = 0;
    while (true) : (tries += 1) {
        const count = client.countWorkflows(completed_query) catch |err| {
            std.log.err("Unable to count workflows matching \"{s}\": {t}", .{ completed_query, err });
            std.process.exit(1);
        };
        if (count >= before + number_of_workflows) break;
        if (tries == 100) {
            std.log.err("Visibility listed {d} completed runs for \"{s}\" after {d} tries, expected {d}", .{ count, completed_query, tries, before + number_of_workflows });
            std.process.exit(1);
        }
        try io.sleep(.fromMilliseconds(100), .awake);
    }

    if (!multi_history_replay.replayAll(gpa, io, arena, &client)) std.process.exit(1);
}
