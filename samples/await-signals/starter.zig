//! Starts the await-signals workflow, sends it its three signals in a random order, a couple of
//! seconds apart, and waits for it to complete.

const std = @import("std");
const temporal = @import("temporal");
const await_signals = @import("workflow.zig");

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

    var run = client.executeWorkflow(await_signals.AwaitSignalsWorkflow, .{
        .id = try std.fmt.allocPrint(arena, "await_signals_{s}", .{try temporal.client.uuid(arena, io)}),
        .task_queue = "await_signals",
    }, .{}) catch |err| {
        std.log.err("Unable to execute workflow: {t}", .{err});
        std.process.exit(1);
    };
    defer run.deinit();
    std.log.info("Started workflow WorkflowID={s} RunID={s}", .{ run.id, run.run_id });

    std.log.info("Sending signals", .{});
    var signals = [_]u8{ 1, 2, 3 };
    const random_source: std.Random.IoSource = .{ .io = io };
    random_source.interface().shuffle(u8, &signals);
    for (signals) |signal| {
        const sent = switch (signal) {
            1 => client.signalWorkflow(await_signals.Signal1, run.id, run.run_id, .{}),
            2 => client.signalWorkflow(await_signals.Signal2, run.id, run.run_id, .{}),
            3 => client.signalWorkflow(await_signals.Signal3, run.id, run.run_id, .{}),
            else => unreachable,
        };
        sent catch |err| {
            std.log.err("Unable to send Signal{d} to workflow {s}: {t}", .{ signal, run.id, err });
            std.process.exit(1);
        };
        std.log.info("Sent Signal{d}", .{signal});
        try io.sleep(.fromSeconds(2), .awake);
    }

    run.get(arena) catch |err| {
        if (run.failure()) |f| {
            std.log.err("Workflow failed: {s}", .{f.message});
        } else {
            std.log.err("Unable to get workflow result: {t}", .{err});
        }
        std.process.exit(1);
    };
    std.log.info("Workflow completed", .{});
}
