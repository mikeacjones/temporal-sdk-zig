//! Starts the uppercase workflow and sends it five requests through updates, printing each
//! response, so it plays both the starter and the request program. The workflow never ends on
//! its own (it continues as new every few hundred requests), so the starter terminates it
//! afterwards.

const std = @import("std");
const temporal = @import("temporal");
const reqrespupdate = @import("reqrespupdate.zig");

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;
    const arena = init.arena.allocator();

    // The client is a heavyweight object that should be created once per process.
    const options = try temporal.client.loadDefaultOptions(arena);
    var client = temporal.client.Client.connect(gpa, io, options) catch |err| {
        std.log.err("Unable to create client: {t}", .{err});
        std.process.exit(1);
    };
    defer client.close();

    const workflow_id = "reqrespupdate_workflow";
    var run = client.executeWorkflow(reqrespupdate.UppercaseWorkflow, .{
        .id = workflow_id,
        .task_queue = "reqrespupdate",
    }, .{true}) catch |err| {
        std.log.err("Unable to execute workflow: {t}", .{err});
        std.process.exit(1);
    };
    defer run.deinit();
    std.log.info("Started workflow WorkflowID={s} RunID={s}", .{ run.id, run.run_id });

    for (0..5) |i| {
        const input = try std.fmt.allocPrint(arena, "foo{d}", .{i});
        std.log.info("Requesting \"{s}\" be uppercased", .{input});
        requestUppercase(&client, arena, workflow_id, input) catch |err| switch (err) {
            // Give the workflow an idle moment to continue as new before the next request.
            error.TryingToContinueAsNew => {
                std.log.info("Workflow trying to continue as new, backing off", .{});
                try io.sleep(.fromSeconds(2), .awake);
            },
            else => std.process.exit(1),
        };
    }

    client.terminateWorkflow(workflow_id, "", "reqrespupdate sample finished") catch |err| {
        std.log.err("Unable to terminate workflow {s}: {t}", .{ workflow_id, err });
        std.process.exit(1);
    };
    std.log.info("Terminated the reqrespupdate workflow", .{});
}

/// Sends one request to the workflow's latest run and logs the response. A request the
/// workflow rejected because it is about to continue as new is `error.TryingToContinueAsNew`;
/// other failures are logged and returned.
fn requestUppercase(client: *temporal.client.Client, arena: std.mem.Allocator, workflow_id: []const u8, input: []const u8) !void {
    var handle = client.updateWorkflow(reqrespupdate.Uppercase, workflow_id, "", .{.{ .input = input }}, .{ .wait_for_stage = .completed }) catch |err| {
        std.log.err("  Failed updating workflow {s}: {t}", .{ workflow_id, err });
        return err;
    };
    defer handle.deinit();
    const response = handle.get(arena) catch |err| {
        const message = if (handle.failure()) |f| f.message else "";
        std.log.info("  Failed: {s}", .{if (message.len > 0) message else @errorName(err)});
        if (std.mem.eql(u8, message, @errorName(reqrespupdate.BackoffError.TryingToContinueAsNew))) return error.TryingToContinueAsNew;
        return err;
    };
    std.log.info("  Result: \"{s}\"", .{response.output});
}
