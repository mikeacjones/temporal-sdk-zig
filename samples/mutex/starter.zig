//! Starts two workflows that lock the same resource, then waits for both: the second one's
//! critical section runs only after the first one's has finished.

const std = @import("std");
const temporal = @import("temporal");
const mutex = @import("mutex.zig");

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

    // This resource ID can be a business identifier as well.
    const resource_id = try temporal.client.uuid(arena, io);

    var run1 = client.executeWorkflow(mutex.SampleWorkflowWithMutex, .{
        .id = try std.fmt.allocPrint(arena, "SampleWorkflow1WithMutex_{s}", .{try temporal.client.uuid(arena, io)}),
        .task_queue = mutex.task_queue,
    }, .{resource_id}) catch |err| {
        std.log.err("Unable to execute workflow1: {t}", .{err});
        std.process.exit(1);
    };
    defer run1.deinit();
    std.log.info("Started workflow1 WorkflowID={s} RunID={s}", .{ run1.id, run1.run_id });

    var run2 = client.executeWorkflow(mutex.SampleWorkflowWithMutex, .{
        .id = try std.fmt.allocPrint(arena, "SampleWorkflow2WithMutex_{s}", .{try temporal.client.uuid(arena, io)}),
        .task_queue = mutex.task_queue,
    }, .{resource_id}) catch |err| {
        std.log.err("Unable to execute workflow2: {t}", .{err});
        std.process.exit(1);
    };
    defer run2.deinit();
    std.log.info("Started workflow2 WorkflowID={s} RunID={s}", .{ run2.id, run2.run_id });

    run1.get(arena) catch |err| {
        std.log.err("Workflow1 failed: {t}", .{err});
        std.process.exit(1);
    };
    std.log.info("Workflow1 completed", .{});
    run2.get(arena) catch |err| {
        std.log.err("Workflow2 failed: {t}", .{err});
        std.process.exit(1);
    };
    std.log.info("Workflow2 completed", .{});
}
