//! Starts the uppercase workflow and then plays the requester: it asks the workflow to
//! uppercase a few strings, one after another, printing each response. The workflow takes
//! requests until it is stopped, so the starter cancels it at the end and waits for it to
//! close. Pass --leave-running to leave it running for other requesters instead.

const std = @import("std");
const temporal = @import("temporal");
const uppercase = @import("workflow.zig");
const Requester = @import("requester.zig").Requester;

const request_count = 3;

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;
    const arena = init.arena.allocator();

    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.skip();
    const leave_running = if (args.next()) |a| std.mem.eql(u8, a, "--leave-running") else false;

    // The client is a heavyweight object that should be created once per process.
    const options = try temporal.client.loadDefaultOptions(arena);
    var client = temporal.client.Client.connect(gpa, io, options) catch |err| {
        std.log.err("Unable to create client: {t}", .{err});
        std.process.exit(1);
    };
    defer client.close();

    const workflow_id = "reqrespquery_workflow";
    var run = client.executeWorkflow(uppercase.UppercaseWorkflow, .{
        .id = workflow_id,
        .task_queue = "reqrespquery",
    }, .{}) catch |err| {
        std.log.err("Unable to execute workflow: {t}", .{err});
        std.process.exit(1);
    };
    defer run.deinit();
    std.log.info("Started workflow WorkflowID={s} RunID={s}", .{ run.id, run.run_id });

    const requester = Requester.init(&client, io, .{ .target_workflow_id = workflow_id }) catch |err| {
        std.log.err("Unable to create requester: {t}", .{err});
        std.process.exit(1);
    };
    var failed = false;
    for (0..request_count) |i| {
        const input = try std.fmt.allocPrint(arena, "foo{d}", .{i});
        std.log.info("Requesting \"{s}\" be uppercased", .{input});
        if (requester.requestUppercase(arena, input)) |result| {
            std.log.info("  Result: \"{s}\"", .{result});
        } else |err| {
            std.log.err("  Failed: {t}", .{err});
            failed = true;
        }
    }
    if (leave_running) return;

    client.cancelWorkflow(workflow_id, "", "reqrespquery sample finished") catch |err| {
        std.log.err("Unable to cancel workflow {s}: {t}", .{ workflow_id, err });
        std.process.exit(1);
    };
    if (run.get(arena)) |_| {
        std.log.err("Workflow {s} completed instead of being canceled", .{workflow_id});
        std.process.exit(1);
    } else |err| switch (err) {
        error.WorkflowCanceled => std.log.info("Canceled the uppercase workflow", .{}),
        else => {
            std.log.err("Unable to get workflow result: {t}", .{err});
            std.process.exit(1);
        },
    }
    if (failed) std.process.exit(1);
}
