const std = @import("std");
const temporal = @import("temporal");
const trip_workflow = @import("trip_workflow.zig");
const recovery_workflow = @import("recovery_workflow.zig");

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;
    const arena = init.arena.allocator();

    // The client and worker are heavyweight objects that should be created once per process.
    const options = try temporal.client.loadDefaultOptions(arena);
    var client = temporal.client.Client.connect(gpa, io, options) catch |err| {
        std.log.err("Unable to create client: {t}", .{err});
        std.process.exit(1);
    };
    defer client.close();

    var w = try temporal.worker.Worker.init(gpa, io, &client, "recovery", .{});
    defer w.deinit();

    try w.registerWorkflow(recovery_workflow.RecoverWorkflow);
    try w.registerWorkflow(trip_workflow.TripWorkflow);

    var cache: recovery_workflow.ExecutionsCache = .init(gpa, io, 10);
    defer cache.deinit();
    // A new ID per process, so several workers on one machine tell their logs apart; a real
    // deployment would use the host name or address.
    var activities: recovery_workflow.Activities = .{
        .cache = &cache,
        .host_id = try std.fmt.allocPrint(arena, "recovery_{s}", .{try temporal.client.uuid(arena, io)}),
    };
    try w.registerActivitiesWith(&activities, .{ recovery_workflow.ListOpenExecutions, recovery_workflow.RecoverExecutions });

    w.runUntilSignal() catch |err| {
        std.log.err("Unable to start worker: {t}", .{err});
        std.process.exit(1);
    };
}
