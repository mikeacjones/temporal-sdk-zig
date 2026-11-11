//! Starts the query workflow and queries its "state", so it plays both the starter and the
//! query program. The workflow waits on a two-minute timer: by default the starter terminates
//! it after the query so nothing is left running. Pass --wait to let it finish instead and
//! query it again once it is done.

const std = @import("std");
const temporal = @import("temporal");
const query = @import("query_workflow.zig");

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;
    const arena = init.arena.allocator();

    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.skip();
    const wait = if (args.next()) |a| std.mem.eql(u8, a, "--wait") else false;

    // The client is a heavyweight object that should be created once per process.
    const options = try temporal.client.loadDefaultOptions(arena);
    var client = temporal.client.Client.connect(gpa, io, options) catch |err| {
        std.log.err("Unable to create client: {t}", .{err});
        std.process.exit(1);
    };
    defer client.close();

    const workflow_id = "query_workflow";
    var run = client.executeWorkflow(query.QueryWorkflow, .{
        .id = workflow_id,
        .task_queue = "query",
    }, .{}) catch |err| {
        std.log.err("Unable to execute workflow: {t}", .{err});
        std.process.exit(1);
    };
    defer run.deinit();
    std.log.info("Started workflow WorkflowID={s} RunID={s}", .{ run.id, run.run_id });

    // The server holds a query sent before the first workflow task completes until it does,
    // so this sees the state the workflow set before starting its timer.
    queryState(&client, arena, run.id, run.run_id);

    if (!wait) {
        client.terminateWorkflow(run.id, run.run_id, "query sample finished") catch |err| {
            std.log.err("Unable to terminate workflow {s}: {t}", .{ run.id, err });
            std.process.exit(1);
        };
        std.log.info("Terminated the query workflow", .{});
        return;
    }

    run.get(arena) catch |err| {
        std.log.err("Unable to get workflow result: {t}", .{err});
        std.process.exit(1);
    };
    // A closed workflow still answers queries.
    queryState(&client, arena, run.id, run.run_id);
}

fn queryState(client: *temporal.client.Client, arena: std.mem.Allocator, workflow_id: []const u8, run_id: []const u8) void {
    const state = client.queryWorkflow(query.State, arena, workflow_id, run_id, .{}) catch |err| {
        std.log.err("Unable to query workflow {s}: {t}", .{ workflow_id, err });
        std.process.exit(1);
    };
    std.log.info("Query result: {s}", .{state});
}
