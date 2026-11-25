//! Runs the caller workflow in namespace my-context-caller-namespace with a propagated value,
//! caller-id: samples-zig, and prints its result: the greeting of the workflow the say-hello
//! operation started, which received the value over the Nexus call.

const std = @import("std");
const temporal = @import("temporal");
const context = @import("context.zig");
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

    // The client's propagators write this value into the start request's header.
    var values: temporal.propagation.Values = .init(arena);
    try values.put(context.propagate_key, .{ .key = "caller-id", .value = "samples-zig" });

    var run = client.executeWorkflow(caller.HelloCallerWorkflow, .{
        .id = try std.fmt.allocPrint(arena, "nexus_hello_caller_workflow_{s}", .{try temporal.client.uuid(arena, io)}),
        .task_queue = caller.task_queue,
        .context = &values,
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
