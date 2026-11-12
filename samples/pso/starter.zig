//! Starts an optimization: `pso-starter [function]`, where the function is sphere (the
//! default), rosenbrock or griewank. The starter also does the work of the query program: it
//! queries the workflow for its child workflow's ID and the child for its progress, then
//! waits for the result.

const std = @import("std");
const temporal = @import("temporal");
const pso = @import("pso.zig");

/// How many times to query for a value that appears shortly, 100ms apart.
const max_tries = 600;

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;
    const arena = init.arena.allocator();

    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.skip();
    const function_name = args.next() orelse "sphere";

    // The client is a heavyweight object that should be created once per process.
    const options = try temporal.client.loadDefaultOptions(arena);
    var client = temporal.client.Client.connect(gpa, io, options) catch |err| {
        std.log.err("Unable to create client: {t}", .{err});
        std.process.exit(1);
    };
    defer client.close();

    var run = client.executeWorkflow(pso.PsoWorkflow, .{
        .id = try std.fmt.allocPrint(arena, "PSO_{s}", .{try temporal.client.uuid(arena, io)}),
        .task_queue = pso.task_queue,
    }, .{function_name}) catch |err| {
        std.log.err("Unable to execute workflow: {t}", .{err});
        std.process.exit(1);
    };
    defer run.deinit();
    std.log.info("Started workflow WorkflowID={s} RunID={s}", .{ run.id, run.run_id });

    // The parent names its child once the swarm is initialized.
    var child_id: []const u8 = "";
    for (0..max_tries) |_| {
        child_id = client.queryWorkflow(pso.ChildQuery, arena, run.id, run.run_id, .{}) catch |err| {
            std.log.err("Unable to query workflow {s} for its child: {t}", .{ run.id, err });
            std.process.exit(1);
        };
        if (child_id.len > 0) break;
        try io.sleep(.fromMilliseconds(100), .awake);
    } else {
        std.log.err("Workflow {s} started no child workflow after {d} tries", .{ run.id, max_tries });
        std.process.exit(1);
    }
    std.log.info("Received query result Result={s}", .{child_id});

    // The parent names the child just before starting it, so the child may not exist yet. A
    // closed child still answers. Its message stays empty if it reached the goal at its
    // first step.
    var tries: usize = 0;
    const iteration = while (true) : (tries += 1) {
        break client.queryWorkflow(pso.IterationQuery, arena, child_id, "", .{}) catch |err| {
            if (err == error.NotFound and tries < max_tries) {
                try io.sleep(.fromMilliseconds(100), .awake);
                continue;
            }
            std.log.err("Unable to query child workflow {s} for its progress: {t}", .{ child_id, err });
            std.process.exit(1);
        };
    };
    std.log.info("Child workflow progress: {s}", .{iteration});

    const result = run.get(arena) catch |err| {
        std.log.err("Unable to get workflow result: {t}", .{err});
        std.process.exit(1);
    };
    std.log.info("Workflow result: {s}", .{result});
}
