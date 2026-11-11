const std = @import("std");
const temporal = @import("temporal");
const branch = @import("branch.zig");

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;
    const arena = init.arena.allocator();

    // The client is a heavyweight object that should be created only once per process.
    const options = try temporal.client.loadDefaultOptions(arena);
    var client = temporal.client.Client.connect(gpa, io, options) catch |err| {
        std.log.err("Unable to create client: {t}", .{err});
        std.process.exit(1);
    };
    defer client.close();

    var run = client.executeWorkflow(branch.SampleBranchWorkflow, .{ .task_queue = "branch" }, .{10}) catch |err| {
        std.log.err("Failure starting workflow: {t}", .{err});
        std.process.exit(1);
    };
    defer run.deinit();
    std.log.info("Started Workflow Execution WorkflowID={s} RunID={s}", .{ run.id, run.run_id });

    // Wait for Workflow Execution completion.
    // This is rarely needed in real use cases when Workflow Executions are potentially long running.
    const result = run.get(arena) catch |err| {
        std.log.err("Unable get workflow result: {t}", .{err});
        std.process.exit(1);
    };
    for (result) |r| std.log.info("Workflow result: {s}", .{r});
}
