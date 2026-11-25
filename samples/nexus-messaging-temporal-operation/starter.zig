//! Runs the caller workflows of both patterns in namespace
//! nexus-messaging-temporal-operation-caller-namespace and prints their log entries: the
//! caller pattern's for the greeting workflow the worker started for "default-user", then the
//! on-demand pattern's, which starts two greeting workflows itself and attaches approval
//! context to them.

const std = @import("std");
const temporal = @import("temporal");
const caller_caller = @import("callerpattern/caller.zig");
const ondemand_caller = @import("ondemandpattern/caller.zig");

const Client = temporal.client.Client;

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;
    const arena = init.arena.allocator();

    // Both callers run in the same namespace.
    var client = caller_caller.connect(gpa, io, arena) catch |err| {
        std.log.err("Unable to create client for namespace {s}: {t}", .{ caller_caller.namespace, err });
        std.process.exit(1);
    };
    defer client.close();

    const caller_entries = try runWorkflow(caller_caller.CallerWorkflow, &client, arena, io, .{
        .id_prefix = "nexus-messaging-temporal-operation-caller-workflow",
        .task_queue = caller_caller.task_queue,
    }, .{"default-user"});
    for (caller_entries, 1..) |entry, i| std.log.info("callerpattern [{d}] {s}", .{ i, entry });

    const ondemand_entries = try runWorkflow(ondemand_caller.CallerRemoteWorkflow, &client, arena, io, .{
        .id_prefix = "nexus-messaging-temporal-operation-caller-remote-workflow",
        .task_queue = ondemand_caller.task_queue,
    }, .{});
    for (ondemand_entries, 1..) |entry, i| std.log.info("ondemandpattern [{d}] {s}", .{ i, entry });
}

const Start = struct {
    /// The workflow ID is this followed by "-" and a UUID.
    id_prefix: []const u8,
    task_queue: []const u8,
};

/// Runs `Workflow` and waits for its result; exits 1 when it can't start or fails.
fn runWorkflow(comptime Workflow: type, client: *Client, arena: std.mem.Allocator, io: std.Io, start: Start, args: Workflow.Args) !Workflow.Result {
    var run = client.executeWorkflow(Workflow, .{
        .id = try std.fmt.allocPrint(arena, "{s}-{s}", .{ start.id_prefix, try temporal.client.uuid(arena, io) }),
        .task_queue = start.task_queue,
    }, args) catch |err| {
        std.log.err("Unable to execute workflow {s}: {t}", .{ Workflow.workflow_name, err });
        std.process.exit(1);
    };
    defer run.deinit();
    std.log.info("Started workflow WorkflowID={s} RunID={s}", .{ run.id, run.run_id });

    return run.get(arena) catch |err| {
        std.log.err("Workflow {s} failed: {t}", .{ run.id, err });
        std.process.exit(1);
    };
}
