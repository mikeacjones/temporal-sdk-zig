const std = @import("std");
const temporal = @import("temporal");
const sample = @import("child_workflow_continue_as_new.zig");

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

    // This workflow ID can be a business identifier supplied by the caller.
    var run = client.executeWorkflow(sample.SampleParentWorkflow, .{
        .id = try std.fmt.allocPrint(arena, "parent-workflow_{s}", .{try temporal.client.uuid(arena, io)}),
        .task_queue = sample.task_queue,
    }, .{}) catch |err| {
        std.log.err("Unable to execute workflow: {t}", .{err});
        std.process.exit(1);
    };
    defer run.deinit();
    std.log.info("Started workflow WorkflowID={s} RunID={s}", .{ run.id, run.run_id });

    const result = run.get(arena) catch |err| {
        std.log.err("Failure getting workflow result: {t}", .{err});
        std.process.exit(1);
    };
    std.log.info("Workflow result: {s}", .{result});
}
