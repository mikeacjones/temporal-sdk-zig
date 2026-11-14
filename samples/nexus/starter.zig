//! Runs the two caller workflows in namespace my-caller-namespace and prints their results,
//! then where the say-hello operation's workflow ran, from the caller's history.

const std = @import("std");
const temporal = @import("temporal");
const caller = @import("caller.zig");

const Client = temporal.client.Client;

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;
    const arena = init.arena.allocator();

    var client = caller.connect(gpa, io, arena) catch |err| {
        std.log.err("Unable to create client for namespace {s}: {t}", .{ caller.namespace, err });
        std.process.exit(1);
    };
    defer client.close();

    _ = try runWorkflow(caller.EchoCallerWorkflow, &client, arena, io, .{"Nexus Echo 👋"});
    const hello = try runWorkflow(caller.HelloCallerWorkflow, &client, arena, io, .{ "Nexus", .es });

    // The operation's started event links to the workflow the handler started.
    const history = client.fetchHistory(arena, hello.id, hello.run_id) catch |err| {
        std.log.err("Unable to fetch the history of workflow {s}: {t}", .{ hello.id, err });
        std.process.exit(1);
    };
    for (history.events.items) |event| {
        if (event.event_type != .EVENT_TYPE_NEXUS_OPERATION_STARTED) continue;
        for (event.links.items) |link| switch (link.variant orelse continue) {
            .workflow_event => |target| std.log.info("Workflow {s} in namespace {s} called workflow {s} in namespace {s}", .{ hello.id, client.namespace, target.workflow_id, target.namespace }),
            else => {},
        };
    }
}

const Execution = struct { id: []const u8, run_id: []const u8 };

/// Executes `Workflow` on the caller task queue, waits for its result, and logs it.
fn runWorkflow(comptime Workflow: type, client: *Client, arena: std.mem.Allocator, io: std.Io, args: Workflow.Args) !Execution {
    var run = client.executeWorkflow(Workflow, .{
        .id = try std.fmt.allocPrint(arena, "nexus_hello_caller_workflow_{s}", .{try temporal.client.uuid(arena, io)}),
        .task_queue = caller.task_queue,
    }, args) catch |err| {
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
    return .{ .id = try arena.dupe(u8, run.id), .run_id = try arena.dupe(u8, run.run_id) };
}
