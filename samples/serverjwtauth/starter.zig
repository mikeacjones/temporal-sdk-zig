const std = @import("std");
const temporal = @import("temporal");
const helloworld = @import("helloworld");
const serverjwtauth = @import("serverjwtauth.zig");

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;
    const arena = init.arena.allocator();

    var options = temporal.client.loadOptions(arena, .{ .env = init.environ_map }) catch |err| {
        std.log.err("Unable to load the client options: {t}", .{err});
        std.process.exit(1);
    };
    // Only the token below authorizes the calls, never an API key from the environment.
    options.api_key = "";

    // readKey logs what is wrong with the key file.
    const key = serverjwtauth.readKey(arena, io, serverjwtauth.keyPath(init.environ_map)) catch std.process.exit(1);
    // One token outlives the run, so the starter refreshes none.
    const refresher: serverjwtauth.HeaderRefresher = .{
        .config = .{ .key = key, .permissions = try serverjwtauth.namespacePermissions(arena, options.namespace) },
        .base_headers = options.headers,
    };
    options.headers = refresher.headers(arena, io) catch |err| {
        std.log.err("Unable to mint a JWT: {t}", .{err});
        std.process.exit(1);
    };

    // The client is a heavyweight object that should be created once per process.
    var client = temporal.client.Client.connect(gpa, io, options) catch |err| {
        std.log.err("Unable to create client: {t}", .{err});
        std.process.exit(1);
    };
    defer client.close();

    var run = client.executeWorkflow(helloworld.HelloWorld, .{
        .id = serverjwtauth.workflow_id,
        .task_queue = serverjwtauth.task_queue,
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
