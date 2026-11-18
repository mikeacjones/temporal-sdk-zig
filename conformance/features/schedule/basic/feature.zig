const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");
const schedule = temporal.schedule;

const Workflow = temporal.workflow.Def("BasicScheduleWorkflow", echo);
const completion_limit: std.Io.Duration = .fromSeconds(10);

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
        .spec = .{ .intervals = &.{.{ .every = .fromSeconds(2) }} },
        .policies = .{ .overlap = .buffer_one },
    } });
    defer handle.deinit();

    var deletion: temporal.client.ScheduleError!void = {};
    const result = verify: {
        defer deletion = handle.delete();
        break :verify verifySchedule(ctx, &handle, options.id);
    };
    try result;
    deletion catch |err| return ctx.fail("deleting schedule {s} failed with error.{t}", .{ handle.id, err });
}

fn verifySchedule(ctx: *harness.Context, handle: *const temporal.client.ScheduleHandle, workflow_id: []const u8) !void {
    const description = try handle.describe(ctx.arena);
    try ctx.expectEqualStrings(workflow_id, description.schedule.action.start_workflow.id);
    try waitForResult(ctx, handle.id, "arg1");
    try handle.update({}, updateArgument);
    try waitForResult(ctx, handle.id, "arg2");
}

fn updateArgument(_: void, arena: std.mem.Allocator, description: *schedule.Description) !?schedule.Update {
    description.schedule.action.start_workflow.args = (try temporal.converter.toPayloads(arena, .{@as([]const u8, "arg2")})).items;
    return .{ .schedule = description.schedule };
}

fn waitForResult(ctx: *harness.Context, schedule_id: []const u8, expected: []const u8) !void {
    const deadline = std.Io.Clock.Timestamp.fromNow(ctx.io, .{ .raw = completion_limit, .clock = .awake });
    var client = try ctx.client.withRpcOptions(.{ .timeout = completion_limit, .retry = false });
    var handle = try client.getScheduleHandle(schedule_id);
    defer handle.deinit();
    var scratch = std.heap.ArenaAllocator.init(ctx.gpa);
    defer scratch.deinit();
    const arena = scratch.allocator();
    const poll_interval: std.Io.Duration = .fromMilliseconds(100);

    while (true) {
        _ = scratch.reset(.retain_capacity);
        client = try ctx.client.withRpcOptions(.{ .timeout = try remainingTime(ctx, deadline, schedule_id, expected), .retry = false });
        const description = handle.describe(arena) catch |err|
            return ctx.fail("describing schedule {s} while waiting for {s} failed with error.{t}", .{ schedule_id, expected, err });
        for (description.info.recent_actions) |action| {
            const run = action.workflow;
            var history_client = try ctx.client.withRpcOptions(.{ .timeout = try remainingTime(ctx, deadline, schedule_id, expected) });
            const response = history_client.callService(.workflow, "GetWorkflowExecutionHistory", arena, .{
                .namespace = ctx.namespace,
                .execution = .{ .workflow_id = run.workflow_id, .run_id = run.run_id },
                .history_event_filter_type = .HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT,
                .skip_archival = true,
            }, null) catch |err|
                return ctx.fail("reading close event for scheduled run {s}/{s} failed with error.{t}", .{ run.workflow_id, run.run_id, err });
            const recorded = response.history orelse continue;
            if (recorded.events.items.len == 0) continue;
            try ctx.expectEvent(recorded, .EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED);
            const result = harness.history.resultPayloads(recorded);
            try ctx.expect(result.len == 1, "expected one result payload for scheduled run {s}/{s}, got {d}", .{ run.workflow_id, run.run_id, result.len });
            const value = try temporal.converter.fromPayload([]const u8, arena, result[0]);
            if (std.mem.eql(u8, value, expected)) {
                _ = try remainingTime(ctx, deadline, schedule_id, expected);
                return;
            }
        }
        const remaining = try remainingTime(ctx, deadline, schedule_id, expected);
        try ctx.io.sleep(.fromNanoseconds(@min(poll_interval.nanoseconds, remaining.nanoseconds)), .awake);
    }
}

fn remainingTime(ctx: *harness.Context, deadline: std.Io.Clock.Timestamp, schedule_id: []const u8, expected: []const u8) !std.Io.Duration {
    const remaining = deadline.durationFromNow(ctx.io).raw;
    if (remaining.nanoseconds <= 0)
        return ctx.fail("schedule {s} did not complete a run returning {s} within {f}", .{ schedule_id, expected, completion_limit });
    return remaining;
}
