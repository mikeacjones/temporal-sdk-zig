//! Runs the activity directly from the client, with no workflow around it, then lists and
//! counts the standalone activities on its task queue.

const std = @import("std");
const temporal = @import("temporal");
const helloworld = @import("helloworld.zig");

const task_queue = "standalone-activity-helloworld";
const query = "TaskQueue = '" ++ task_queue ++ "'";

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

    var handle = client.executeActivity(helloworld.Activity, .{
        .id = "standalone_activity_helloworld_ActivityID",
        .task_queue = task_queue,
        // At least one of schedule_to_close_timeout and start_to_close_timeout is required.
        .schedule_to_close_timeout = .fromSeconds(10),
    }, .{"Temporal"}) catch |err| {
        std.log.err("Unable to execute activity: {t}", .{err});
        std.process.exit(1);
    };
    defer handle.deinit();

    std.log.info("Started standalone activity ActivityID={s} RunID={s}", .{ handle.id, handle.run_id });

    // Synchronously wait for the activity completion.
    const result = handle.get(arena) catch |err| {
        std.log.err("Unable to get standalone activity result: {t}", .{err});
        std.process.exit(1);
    };
    std.log.info("Activity result: {s}", .{result});

    // Visibility is eventually consistent, so a run that just closed may not be listed or
    // counted yet.
    var it = client.listActivities(query, .{}) catch |err| {
        std.log.err("Unable to list activities: {t}", .{err});
        std.process.exit(1);
    };
    defer it.deinit();
    std.log.info("ListActivities results", .{});
    while (it.next(arena) catch |err| {
        std.log.err("Unable to list activities: {t}", .{err});
        std.process.exit(1);
    }) |info| {
        // The status enum is open, so a status from a newer server may have no name.
        const status = std.enums.tagName(temporal.client.ActivityExecutionStatus, info.status) orelse "unknown";
        std.log.info("\tActivityID: {s}, Type: {s}, Status: {s}", .{ info.activity_id, info.activity_type, status });
    }

    const count = client.countActivities(query) catch |err| {
        std.log.err("Unable to count activities: {t}", .{err});
        std.process.exit(1);
    };
    std.log.info("Total activities: {d}", .{count});
}
