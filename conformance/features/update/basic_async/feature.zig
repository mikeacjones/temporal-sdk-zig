const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");
const workflow = temporal.workflow;

const Workflow = workflow.Def("Workflow", runWorkflow);
const MyUpdate = workflow.UpdateDef("my_update", fn ([]const u8) []const u8);
const update_arg = "update-arg";
const bad_update_arg = "bad-update-arg";

pub const feature = harness.define(.{
    .workflows = .{Workflow},
    .execute = execute,
});

const State = struct {
    value: []const u8 = "",

    fn validate(_: *State, arg: []const u8) error{InvalidUpdateArgument}!void {
        if (std.mem.eql(u8, arg, bad_update_arg)) return error.InvalidUpdateArgument;
    }

    fn update(state: *State, arg: []const u8) []const u8 {
        state.value = arg;
        return "update-result";
    }

    fn updated(state: *State) bool {
        return state.value.len != 0;
    }
};

fn runWorkflow(ctx: *workflow.Context) ![]const u8 {
    const state = try ctx.allocator().create(State);
    state.* = .{};
    try ctx.setUpdateHandlerWithValidator(MyUpdate, state, State.update, State.validate);
    try ctx.await(state, State.updated);
    return state.value;
}

fn execute(ctx: *harness.Context) !void {
    var run = try ctx.client.executeWorkflow(Workflow, ctx.startOptions(), .{});
    defer run.deinit();

    // Starting the update returns once it is accepted or rejected; the handle reports which.
    var bad = try ctx.client.updateWorkflow(MyUpdate, run.id, run.run_id, .{bad_update_arg}, .{ .wait_for_stage = .accepted });
    defer bad.deinit();
    if (bad.get(ctx.arena)) |result| {
        return ctx.fail("expected the validator to reject {s}, the update returned {s}", .{ bad_update_arg, result });
    } else |err| switch (err) {
        error.UpdateFailed => {
            const failure = bad.failure() orelse return ctx.fail("rejected update has no failure details", .{});
            try ctx.expectEqualStrings("InvalidUpdateArgument", failure.message);
        },
        else => return err,
    }

    var good = try ctx.client.updateWorkflow(MyUpdate, run.id, run.run_id, .{update_arg}, .{ .wait_for_stage = .accepted });
    defer good.deinit();
    const result = good.get(ctx.arena) catch |err| switch (err) {
        error.UpdateFailed => return ctx.fail("update failed: {s}", .{if (good.failure()) |f| f.message else "no failure details"}),
        else => return err,
    };
    try ctx.expectEqualStrings("update-result", result);
    try ctx.expectEqualStrings(update_arg, try run.get(ctx.arena));

    const recorded = try ctx.fetchHistory(run.id, run.run_id);
    try ctx.expectNoEvent(recorded, .EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_REJECTED);
    try ctx.expectEventsInOrder(recorded, &.{
        .EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED,
        .EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_COMPLETED,
        .EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
    });
}
