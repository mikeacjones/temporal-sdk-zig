//! Runs the caller workflow in namespace my-multiple-arguments-caller-namespace and prints
//! its result: the greeting of the workflow the say-hello operation started with two
//! arguments.

const std = @import("std");
const temporal = @import("temporal");
const caller = @import("caller.zig");

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;
    const arena = init.arena.allocator();

    var client = caller.connect(gpa, io, arena) catch |err| {
        std.log.err("Unable to create client for namespace {s}: {t}", .{ caller.namespace, err });
        std.process.exit(1);
    };
    defer client.close();

    var run = client.executeWorkflow(caller.HelloCallerWorkflow, .{
        .id = try std.fmt.allocPrint(arena, "nexus_hello_caller_workflow_{s}", .{try temporal.client.uuid(arena, io)}),
        .task_queue = caller.task_queue,
    }, .{ "Nexus", .es }) catch |err| {
        std.log.err("Unable to execute workflow: {t}", .{err});
        std.process.exit(1);
    };
    defer run.deinit();
    std.log.info("Started workflow WorkflowID={s} RunID={s}", .{ run.id, run.run_id });

    // Synchronously wait for the workflow completion.
    const result = run.get(arena) catch |err| {
        std.log.err("Unable to get workflow result: {t}", .{err});
        std.process.exit(1);
    };
    std.log.info("Workflow result: {s}", .{result});
}
