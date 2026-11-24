//! Go's activity dials a client of its own from connection details the workflow passes
//! it; here the activity uses the worker's client (`activity.Context.client`), so the
//! workflow takes no argument.
const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");
const activity = temporal.activity;
const workflow = temporal.workflow;

const SelfUpdateWorkflow = workflow.Def("SelfUpdateWorkflow", runWorkflow);
const SelfUpdateActivity = activity.Def("SelfUpdateActivity", selfUpdate);
const SelfUpdate = workflow.UpdateDef("update!", fn () void);
const update_not_enabled_error_type = "PermissionDenied";
const expected_state = "called";

pub const feature = harness.define(.{
    .workflows = .{SelfUpdateWorkflow},
    .activities = .{SelfUpdateActivity},
    .execute = execute,
});

const State = struct {
    value: []const u8 = "not " ++ expected_state,

    fn update(state: *State) void {
        state.value = expected_state;
    }
};

fn runWorkflow(ctx: *workflow.Context) ![]const u8 {
    const state = try ctx.allocator().create(State);
    state.* = .{};
    try ctx.setUpdateHandler(SelfUpdate, state, State.update);
    try ctx.executeActivity(SelfUpdateActivity, .{
        .start_to_close_timeout = .fromSeconds(5),
        .retry_policy = .{ .non_retryable_error_types = &.{update_not_enabled_error_type} },
    }, .{}).get();
    // The update completed before the activity did, so its effect is visible now.
    if (!std.mem.eql(u8, state.value, expected_state)) {
        const message = try std.fmt.allocPrint(ctx.allocator(), "expected state == \"{s}\" but found \"{s}\"", .{ expected_state, state.value });
        return ctx.fail(.{ .message = message });
    }
    return state.value;
}

fn selfUpdate(ctx: *activity.Context) !void {
    const client = try ctx.client();
    const info = ctx.info();
    var update = try client.updateWorkflow(SelfUpdate, info.workflow_id, info.workflow_run_id, .{}, .{});
    defer update.deinit();
    update.get(ctx.allocator()) catch |err| switch (err) {
        error.UpdateFailed => return ctx.fail(.{ .message = if (update.failure()) |f| f.message else "the update failed without details" }),
        else => return err,
    };
}

fn execute(ctx: *harness.Context) !void {
    var run = try ctx.client.executeWorkflow(SelfUpdateWorkflow, ctx.startOptions(), .{});
    defer run.deinit();
    const result = run.get(ctx.arena) catch |err| {
        if (run.failure()) |failure| return ctx.fail("workflow failed: {s}", .{failure.message});
        return err;
    };
    try ctx.expectEqualStrings(expected_state, result);

    const recorded = try ctx.fetchHistory(run.id, run.run_id);
    try ctx.expectNoEvent(recorded, .EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_REJECTED);
    try ctx.expectEventsInOrder(recorded, &.{
        .EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
        .EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED,
        .EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_COMPLETED,
        .EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
        .EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
    });
}
