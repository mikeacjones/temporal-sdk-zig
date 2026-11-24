//! A paused every-minute schedule is backfilled over two past periods of two minutes each,
//! one three years ago and one 30 minutes ago. Both ends of a period are inclusive, so the
//! schedule takes six actions, and their runs complete.
const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");
const schedule = temporal.schedule;

const Io = std.Io;
const Workflow = temporal.workflow.Def("Workflow", echo);
const expected_actions = 6;
const completion_limit: Io.Duration = .fromSeconds(10);

pub const feature = harness.define(.{ .workflows = .{Workflow}, .execute = execute });

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
        .state = .{ .paused = true },
    } });
    defer handle.deinit();

    var deletion: temporal.client.ScheduleError!void = {};
    const result = verify: {
        defer deletion = handle.delete();
        break :verify backfillAndWait(ctx, &handle);
    };
    try result;
    deletion catch |err| return ctx.fail("deleting schedule {s} failed with error.{t}", .{ handle.id, err });
}

fn backfillAndWait(ctx: *harness.Context, handle: *const temporal.client.ScheduleHandle) !void {
    const now = Io.Timestamp.now(ctx.io, .real);
    const three_years_ago = truncateToMinute(now.subDuration(.fromSeconds(3 * 365 * 24 * 60 * 60)));
    const thirty_minutes_ago = truncateToMinute(now.subDuration(.fromSeconds(30 * 60)));
    const two_minutes: Io.Duration = .fromSeconds(2 * 60);
    handle.backfill(&.{
        .{ .start = three_years_ago.subDuration(two_minutes), .end = three_years_ago, .overlap = .allow_all },
        .{ .start = thirty_minutes_ago.subDuration(two_minutes), .end = thirty_minutes_ago, .overlap = .allow_all },
    }) catch |err| return ctx.fail("backfilling schedule {s} failed with error.{t}", .{ handle.id, err });

    const deadline = Io.Timestamp.now(ctx.io, .awake).addDuration(completion_limit);
    // Each describe is limited to the time left, through a view of the client whose options
    // the loop replaces.
    var client = try ctx.client.withRpcOptions(.{ .timeout = completion_limit, .retry = false });
    var waiting = try client.getScheduleHandle(handle.id);
    defer waiting.deinit();
    var scratch: std.heap.ArenaAllocator = .init(ctx.gpa);
    defer scratch.deinit();
    var actions: i64 = 0;
    var running: usize = 0;
    while (true) {
        _ = scratch.reset(.retain_capacity);
        const left = Io.Timestamp.now(ctx.io, .awake).durationTo(deadline);
        if (left.nanoseconds <= 0) break;
        client = try ctx.client.withRpcOptions(.{ .timeout = left, .retry = false });
        const description = waiting.describe(scratch.allocator()) catch |err|
            return ctx.fail("describing schedule {s} failed with error.{t}", .{ handle.id, err });
        actions = description.info.action_count;
        running = description.info.running_workflows.len;
        if (actions == expected_actions and running == 0) return;
        const remaining = Io.Timestamp.now(ctx.io, .awake).durationTo(deadline);
        if (remaining.nanoseconds <= 0) break;
        try ctx.io.sleep(.fromNanoseconds(@min(Io.Duration.fromSeconds(1).nanoseconds, remaining.nanoseconds)), .awake);
    }
    return ctx.fail("expected schedule {s} to take {d} actions with no workflow running within {f}, it took {d} with {d} running", .{ handle.id, expected_actions, completion_limit, actions, running });
}

fn truncateToMinute(t: Io.Timestamp) Io.Timestamp {
    const minute = Io.Duration.fromSeconds(60).nanoseconds;
    return .fromNanoseconds(t.nanoseconds - @mod(t.nanoseconds, minute));
}
