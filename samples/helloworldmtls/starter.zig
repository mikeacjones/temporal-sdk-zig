const std = @import("std");
const temporal = @import("temporal");
const helloworld = @import("helloworld.zig");

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;
    const arena = init.arena.allocator();

    const args = try helloworld.programArgs(arena, init);
    // parseClientOptions logs what is wrong with the arguments.
    const options = helloworld.parseClientOptions(arena, args, init.environ_map) catch std.process.exit(1);
    // The client is a heavyweight object that should be created once per process.
    var client = temporal.client.Client.connect(gpa, io, options) catch |err| {
        std.log.err("Unable to create client: {t}", .{err});
        std.process.exit(1);
    };
    defer client.close();

    var run = client.executeWorkflow(helloworld.Workflow, .{
        .id = helloworld.workflow_id,
        .task_queue = helloworld.task_queue,
    }, .{"Temporal"}) catch |err| {
        std.log.err("Unable to execute workflow: {t}", .{err});
        std.process.exit(1);
    };
    defer run.deinit();

    std.log.info("Started workflow WorkflowID={s} RunID={s}", .{ run.id, run.run_id });

    // Synchronously wait for the workflow completion.
    const result = run.get(arena) catch |err| {
        std.log.err("Unable get workflow result: {t}", .{err});
        std.process.exit(1);
    };
    std.log.info("Workflow result: {s}", .{result});
}
