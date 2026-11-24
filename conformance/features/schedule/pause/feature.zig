//! A schedule created paused with a note is paused and unpaused, each with a custom note and
//! with the SDK's default one, and every change shows in the described state. The suite's
//! config.json says noWorkflow: the schedule never runs, so the feature declares no workflow
//! and defines one only to build the schedule's action.
//!
//! The default notes are this SDK's, "Paused" and "Unpaused" (`ScheduleHandle.pause` and
//! `unpause`), where the suite's Go version expects the Go SDK's.
const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");
const schedule = temporal.schedule;

const Workflow = temporal.workflow.Def("Workflow", echo);

pub const feature = harness.define(.{ .execute = execute });

fn echo(_: *temporal.workflow.Context, arg: []const u8) ![]const u8 {
    return arg;
}

fn execute(ctx: *harness.Context) !void {
    const options = ctx.startOptions();
    var handle = try ctx.client.createSchedule(ctx.task_queue, .{ .schedule = .{
        .action = try schedule.startWorkflow(ctx.arena, Workflow, .{"arg1"}, .{
            .id = options.id,
            .task_queue = options.task_queue,
            .execution_timeout = options.execution_timeout,
        }),
        .spec = .{ .intervals = &.{.{ .every = .fromSeconds(60) }} },
        .state = .{ .paused = true, .note = "initial note" },
    } });
    defer handle.deinit();

    var deletion: temporal.client.ScheduleError!void = {};
    const result = verify: {
        defer deletion = handle.delete();
        break :verify pauseAndUnpause(ctx, &handle);
    };
    try result;
    deletion catch |err| return ctx.fail("deleting schedule {s} failed with error.{t}", .{ handle.id, err });
}

fn pauseAndUnpause(ctx: *harness.Context, handle: *const temporal.client.ScheduleHandle) !void {
    try expectState(ctx, handle, "creating", true, "initial note");

    handle.pause("custom note1") catch |err| return ctx.fail("pausing paused schedule {s} failed with error.{t}", .{ handle.id, err });
    try expectState(ctx, handle, "pausing it again with a note", true, "custom note1");

    handle.unpause("") catch |err| return ctx.fail("unpausing schedule {s} failed with error.{t}", .{ handle.id, err });
    try expectState(ctx, handle, "unpausing it with the default note", false, "Unpaused");

    handle.unpause("custom note2") catch |err| return ctx.fail("unpausing unpaused schedule {s} failed with error.{t}", .{ handle.id, err });
    try expectState(ctx, handle, "unpausing it again with a note", false, "custom note2");

    handle.pause("") catch |err| return ctx.fail("pausing schedule {s} failed with error.{t}", .{ handle.id, err });
    try expectState(ctx, handle, "pausing it with the default note", true, "Paused");
}

fn expectState(ctx: *harness.Context, handle: *const temporal.client.ScheduleHandle, after: []const u8, paused: bool, note: []const u8) !void {
    const description = handle.describe(ctx.arena) catch |err|
        return ctx.fail("describing schedule {s} after {s} failed with error.{t}", .{ handle.id, after, err });
    const state = description.schedule.state;
    if (state.paused != paused or !std.mem.eql(u8, state.note, note))
        return ctx.fail("after {s}, expected schedule {s} paused={} with note \"{s}\", got paused={} with note \"{s}\"", .{ after, handle.id, paused, note, state.paused, state.note });
}
