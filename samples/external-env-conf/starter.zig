const std = @import("std");
const temporal = @import("temporal");
const externalenvconf = @import("externalenvconf.zig");

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;
    const arena = init.arena.allocator();

    const options = externalenvconf.loadProfile(arena, init.environ_map) catch |err| {
        std.log.err("Unable to load client config from {s}: {t}", .{ externalenvconf.config_file, err });
        std.process.exit(1);
    };
    // The client is a heavyweight object that should be created once per process.
    var client = temporal.client.Client.connect(gpa, io, options) catch |err| {
        std.log.err("Unable to create client: {t}", .{err});
        std.process.exit(1);
    };
    defer client.close();

    var run = client.executeWorkflow(externalenvconf.Workflow, .{
        .id = "external_env_conf_workflowID",
        .task_queue = "external-env-conf",
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
