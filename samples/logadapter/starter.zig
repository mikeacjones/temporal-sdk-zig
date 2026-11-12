const std = @import("std");
const temporal = @import("temporal");
const logadapter = @import("logadapter.zig");

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

    var run = client.executeWorkflow(logadapter.Workflow, .{
        .id = "slog_logger_workflow_id",
        .task_queue = "slog-logger",
    }, .{"<param to log>"}) catch |err| {
        std.log.err("Unable to execute workflow: {t}", .{err});
        std.process.exit(1);
    };
    defer run.deinit();

    std.log.info("Started workflow WorkflowID={s} RunID={s}", .{ run.id, run.run_id });

    // Synchronously wait for the workflow completion.
    run.get(arena) catch |err| {
        std.log.err("Unable get workflow result: {t}", .{err});
        std.process.exit(1);
    };
    std.log.info("Workflow completed. Check worker logs.", .{});
}
