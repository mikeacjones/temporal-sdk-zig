//! Starts the workflow and waits for it. The workflow is expected to fail, because its
//! second child workflow has a type the worker's interceptor refuses: the starter prints that
//! failure and succeeds, and exits non-zero if the workflow completes or fails otherwise.

const std = @import("std");
const temporal = @import("temporal");
const security_interceptor = @import("security_interceptor.zig");

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

    var run = client.executeWorkflow(security_interceptor.Workflow, .{
        .id = try std.fmt.allocPrint(arena, "security-interceptor_{s}", .{try temporal.client.uuid(arena, io)}),
        .task_queue = "security-interceptor",
    }, .{}) catch |err| {
        std.log.err("Unable to execute workflow: {t}", .{err});
        std.process.exit(1);
    };
    defer run.deinit();
    std.log.info("Started workflow WorkflowID={s} RunID={s}", .{ run.id, run.run_id });

    // Synchronously wait for the workflow completion.
    run.get(arena) catch |err| switch (err) {
        error.WorkflowFailed => {
            const f = run.failure() orelse {
                std.log.err("Workflow failed without a failure", .{});
                std.process.exit(1);
            };
            const chain: std.fmt.Alt(temporal.failure.Failure, temporal.failure.format) = .{ .data = f.* };
            if (!std.mem.eql(u8, temporal.failure.applicationType(temporal.failure.rootCause(f).*) orelse "", "not-allowed")) {
                std.log.err("Workflow failed for an unexpected reason: {f}", .{chain});
                std.process.exit(1);
            }
            std.log.info("Workflow failed as expected: {f}", .{chain});
            return;
        },
        else => {
            std.log.err("Unable to get workflow result: {t}", .{err});
            std.process.exit(1);
        },
    };
    std.log.err("Workflow completed, but its prohibited child workflow should have been refused", .{});
    std.process.exit(1);
}
