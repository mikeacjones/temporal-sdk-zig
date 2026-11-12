//! Creates a schedule with no spec, triggers it once by hand, updates it to run every second
//! (and at 5pm on Fridays) starting paused with 10 actions left, unpauses it, waits until it
//! has taken those 10 actions and their workflows have finished, and deletes it.

const std = @import("std");
const temporal = @import("temporal");
const sample = @import("schedule.zig");
const schedule = temporal.schedule;

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

    // This schedule ID can be a business identifier as well.
    const schedule_id = try std.fmt.allocPrint(arena, "schedule_{s}", .{try temporal.client.uuid(arena, io)});
    const workflow_id = try std.fmt.allocPrint(arena, "schedule_workflow_{s}", .{try temporal.client.uuid(arena, io)});
    // Create the schedule with an empty spec, so it doesn't run on its own.
    var handle = client.createSchedule(schedule_id, .{ .schedule = .{
        .action = try schedule.startWorkflow(arena, sample.SampleScheduleWorkflow, .{}, .{
            .id = workflow_id,
            .task_queue = "schedule",
        }),
        .spec = .{},
    } }) catch |err| {
        std.log.err("Unable to create schedule {s}: {t}", .{ schedule_id, err });
        std.process.exit(1);
    };
    defer handle.deinit();
    std.log.info("Created schedule ScheduleID={s}", .{handle.id});

    const ok = if (runSchedule(gpa, io, &handle)) true else |err| blk: {
        std.log.err("Schedule sample failed: {t}", .{err});
        break :blk false;
    };

    // Delete the schedule once the sample is done, whether or not it succeeded.
    std.log.info("Deleting schedule ScheduleID={s}", .{handle.id});
    handle.delete() catch |err| {
        std.log.err("Unable to delete schedule {s}: {t}", .{ handle.id, err });
        std.process.exit(1);
    };
    std.log.info("Deleted schedule ScheduleID={s}", .{handle.id});
    if (!ok) std.process.exit(1);
}

fn runSchedule(gpa: std.mem.Allocator, io: std.Io, handle: *temporal.client.ScheduleHandle) !void {
    // Manually trigger the schedule once.
    std.log.info("Manually triggering schedule ScheduleID={s}", .{handle.id});
    handle.trigger(.allow_all) catch |err| {
        std.log.err("Unable to trigger schedule: {t}", .{err});
        return err;
    };

    // Give the schedule a spec, so it runs periodically.
    std.log.info("Updating schedule ScheduleID={s}", .{handle.id});
    handle.update({}, periodic) catch |err| {
        std.log.err("Unable to update schedule: {t}", .{err});
        return err;
    };

    std.log.info("Unpausing schedule ScheduleID={s}", .{handle.id});
    handle.unpause("") catch |err| {
        std.log.err("Unable to unpause schedule: {t}", .{err});
        return err;
    };

    // Wait for the schedule to take its 10 actions and for the workflows they started to
    // finish, so none is left running once the worker stops.
    std.log.info("Waiting for schedule to complete 10 actions ScheduleID={s}", .{handle.id});
    var describe_arena: std.heap.ArenaAllocator = .init(gpa);
    defer describe_arena.deinit();
    var polls: usize = 0;
    while (true) : (polls += 1) {
        _ = describe_arena.reset(.retain_capacity);
        const d = handle.describe(describe_arena.allocator()) catch |err| {
            std.log.err("Unable to describe schedule: {t}", .{err});
            return err;
        };
        const remaining = d.schedule.state.remaining_actions orelse 0;
        if (remaining == 0 and d.info.running_workflows.len == 0) break;
        if (polls == 60) {
            std.log.err("Schedule {s} still has {d} remaining actions and {d} running workflows after {d} polls", .{ handle.id, remaining, d.info.running_workflows.len, polls });
            return error.ScheduleNotDone;
        }
        std.log.info("Schedule has remaining actions ScheduleID={s} RemainingActions={d}", .{ handle.id, remaining });
        try io.sleep(.fromSeconds(1), .awake);
    }
    std.log.info("Schedule completed its actions ScheduleID={s}", .{handle.id});
}

/// Runs the schedule every second and at 5pm on Fridays, starting paused (to show
/// unpausing) with 10 actions left.
fn periodic(_: void, _: std.mem.Allocator, d: *schedule.Description) !?schedule.Update {
    d.schedule.spec = .{
        .calendars = &.{.{ .hour = &.{.{ .start = 17 }}, .day_of_week = &.{.{ .start = 5 }} }},
        .intervals = &.{.{ .every = .fromSeconds(1) }},
    };
    d.schedule.state.paused = true;
    d.schedule.state.remaining_actions = 10;
    return .{ .schedule = d.schedule };
}
