//! Runs the caller workflow in namespace nexus-serialization-caller and prints, for each
//! endpoint, what its synchronous and its asynchronous operation returned. All eight replies
//! are "Hello from Nexus": each was encrypted with its endpoint's key and decrypted with the
//! same.

const std = @import("std");
const temporal = @import("temporal");
const service = @import("service.zig");
const caller = @import("caller.zig");

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;
    const arena = init.arena.allocator();

    var client = caller.connect(gpa, io, arena) catch |err| {
        std.log.err("Unable to create client for namespace {s}: {t}", .{ service.caller_namespace, err });
        std.process.exit(1);
    };
    defer client.close();

    var run = client.executeWorkflow(caller.CallerWorkflow, .{
        .id = try std.fmt.allocPrint(arena, "nexus-encryption-{s}", .{try temporal.client.uuid(arena, io)}),
        .task_queue = caller.task_queue,
    }, .{"Hello from Nexus"}) catch |err| {
        std.log.err("Unable to execute workflow: {t}", .{err});
        std.process.exit(1);
    };
    defer run.deinit();
    std.log.info("Started workflow: ID={s} RunID={s}", .{ run.id, run.run_id });

    // Synchronously wait for the workflow completion.
    const replies = run.get(arena) catch |err| {
        std.log.err("Unable to get workflow result: {t}", .{err});
        std.process.exit(1);
    };
    for (replies) |r| std.log.info("{s}: sync=\"{s}\" async=\"{s}\"", .{ r.endpoint, r.sync_message, r.async_message });
}
