//! Starts the counter workflow and sends it ten "fetch_and_add" updates whose addends
//! alternate in sign (0, -1, 2, -3, ...), printing each result or rejection. Then it sends
//! "done" and prints the workflow's result.

const std = @import("std");
const temporal = @import("temporal");
const update = @import("update.zig");

pub fn main(init: std.process.Init) !void {
    const arena = init.arena.allocator();

    const options = try temporal.client.loadDefaultOptions(arena);
    var client = temporal.client.Client.connect(init.gpa, init.io, options) catch |err| {
        std.log.err("Unable to create client: {t}", .{err});
        std.process.exit(1);
    };
    defer client.close();

    var run = client.executeWorkflow(update.CounterWorkflow, .{
        .id = "update-workflow-ID",
        .task_queue = "update",
    }, .{}) catch |err| {
        std.log.err("Unable to execute workflow: {t}", .{err});
        std.process.exit(1);
    };
    defer run.deinit();
    std.log.info("Started workflow WorkflowID={s} RunID={s}", .{ run.id, run.run_id });

    var sign: i64 = 1;
    for (0..10) |i| {
        const addend = sign * @as(i64, @intCast(i));
        sign = -sign;
        var handle = client.updateWorkflow(update.FetchAndAdd, run.id, run.run_id, .{addend}, .{ .wait_for_stage = .completed }) catch |err| {
            std.log.err("Error issuing update request with addend {d}: {t}", .{ addend, err });
            std.process.exit(1);
        };
        defer handle.deinit();
        if (handle.get(arena)) |previous| {
            std.log.info("fetch_and_add with addend {d} succeeded: {d}", .{ addend, previous });
        } else |err| {
            const message = if (handle.failure()) |f| f.message else @errorName(err);
            std.log.info("fetch_and_add with addend {d} failed: {s}", .{ addend, message });
        }
    }

    client.signalWorkflow(update.Done, run.id, run.run_id, .{}) catch |err| {
        std.log.err("Failed to send \"done\" signal to workflow {s}: {t}", .{ run.id, err });
        std.process.exit(1);
    };
    const result = run.get(arena) catch |err| {
        std.log.err("Unable to get workflow result: {t}", .{err});
        std.process.exit(1);
    };
    std.log.info("workflow result: {d}", .{result});
}
