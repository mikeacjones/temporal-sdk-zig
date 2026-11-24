//! Creating a schedule with the ID of one that exists fails with
//! `error.ScheduleAlreadyExists`, the SDK's counterpart of the Go SDK's
//! ErrScheduleAlreadyRunning. The schedule is paused and hourly, so it never starts a run:
//! the feature declares no workflow, as there would be no history to record, and defines one
//! only to build the schedule's action.
const temporal = @import("temporal");
const harness = @import("harness");
const schedule = temporal.schedule;

const Workflow = temporal.workflow.Def("Workflow", run);

pub const feature = harness.define(.{ .execute = execute });

fn run(_: *temporal.workflow.Context) !void {}

fn execute(ctx: *harness.Context) !void {
    const options = ctx.startOptions();
    const create: schedule.CreateOptions = .{ .schedule = .{
        .action = try schedule.startWorkflow(ctx.arena, Workflow, .{}, .{
            .id = options.id,
            .task_queue = options.task_queue,
            .execution_timeout = options.execution_timeout,
        }),
        .spec = .{ .intervals = &.{.{ .every = .fromSeconds(60 * 60) }} },
        .state = .{ .paused = true },
    } };
    var handle = try ctx.client.createSchedule(ctx.task_queue, create);
    defer handle.deinit();

    var deletion: temporal.client.ScheduleError!void = {};
    const result = verify: {
        defer deletion = handle.delete();
        break :verify createAgain(ctx, create);
    };
    try result;
    deletion catch |err| return ctx.fail("deleting schedule {s} failed with error.{t}", .{ handle.id, err });
}

fn createAgain(ctx: *harness.Context, create: schedule.CreateOptions) !void {
    if (ctx.client.createSchedule(ctx.task_queue, create)) |second| {
        // The same schedule as the first create's handle, which deletes it.
        var duplicate = second;
        duplicate.deinit();
        return ctx.fail("creating schedule {s} a second time succeeded, expected error.ScheduleAlreadyExists", .{ctx.task_queue});
    } else |err| switch (err) {
        error.ScheduleAlreadyExists => {},
        else => return ctx.fail("creating schedule {s} a second time failed with error.{t}, expected error.ScheduleAlreadyExists", .{ ctx.task_queue, err }),
    }
}
