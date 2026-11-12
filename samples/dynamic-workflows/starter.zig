//! Starts three workflow types the worker registers no definition for, by name alone, and
//! prints each result: "DurableExecution" answers directly, and the two "dynamic-activity"
//! types answer through the dynamic activity.

const std = @import("std");
const temporal = @import("temporal");

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

    // This workflow ID can be user business logic identifier as well. Each run reuses it once
    // the previous one has completed.
    const workflow_options: temporal.client.StartWorkflowOptions = .{
        .id = try std.fmt.allocPrint(arena, "dynamic_workflows_{s}", .{try temporal.client.uuid(arena, io)}),
        .task_queue = "dynamic-workflows",
    };

    const runs = [_]struct { []const u8, []const u8, []const u8 }{
        .{ "DurableExecution", "Hello", "Temporal" },
        .{ "dynamic-activity", "Peanut", "Butter" },
        .{ "dynamic-activity123", "Jelly", "Time" },
    };
    for (runs) |r| {
        const workflow_type, const arg1, const arg2 = r;
        var run = client.executeWorkflowByName([]const u8, workflow_type, workflow_options, .{ arg1, arg2 }) catch |err| {
            std.log.err("Unable to execute workflow {s}: {t}", .{ workflow_type, err });
            std.process.exit(1);
        };
        defer run.deinit();
        std.log.info("Started workflow WorkflowID={s} RunID={s}", .{ run.id, run.run_id });

        const result = run.get(arena) catch |err| {
            std.log.err("Unable get workflow result for {s}: {t}", .{ workflow_type, err });
            std.process.exit(1);
        };
        std.log.info("Workflow result: {s}", .{result});
    }
}
