const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");
const workflow = temporal.workflow;

const Workflow = workflow.Def("Workflow", runWorkflow);
const Update = workflow.UpdateDef("my-update-name", fn ([]const u8) []const u8);
const update_arg = "my-update-arg";
const invalid_arg = "invalid-arg";

pub const feature = harness.define(.{
    .workflows = .{Workflow},
    .execute = execute,
});

const State = struct {
    allocator: std.mem.Allocator,
    value: ?[]const u8 = null,

    fn validate(_: *State, arg: []const u8) error{InvalidUpdateArgument}!void {
        if (std.mem.eql(u8, arg, invalid_arg)) return error.InvalidUpdateArgument;
    }

    fn update(state: *State, arg: []const u8) ![]const u8 {
        const result = try std.fmt.allocPrint(state.allocator, "update-result:{s}", .{arg});
        state.value = arg;
        return result;
    }

    fn updated(state: *State) bool {
        return state.value != null;
    }
};

fn runWorkflow(ctx: *workflow.Context) ![]const u8 {
    const allocator = ctx.allocator();
    const state = try allocator.create(State);
    state.* = .{ .allocator = allocator };
    try ctx.setUpdateHandlerWithValidator(Update, state, State.update, State.validate);
    try ctx.await(state, State.updated);
    return std.fmt.allocPrint(allocator, "workflow-result:{s}", .{state.value.?});
}

fn execute(ctx: *harness.Context) !void {
    var run = try ctx.client.executeWorkflow(Workflow, ctx.startOptions(), .{});
    defer run.deinit();

    var invalid = try ctx.client.updateWorkflow(Update, run.id, run.run_id, .{invalid_arg}, .{});
    defer invalid.deinit();
    if (invalid.get(ctx.arena)) |result| {
        return ctx.fail("update validator accepted {s} and returned {s}", .{ invalid_arg, result });
    } else |err| switch (err) {
        error.UpdateFailed => {
            const failure = invalid.failure() orelse return ctx.fail("rejected update has no failure details", .{});
            try ctx.expectEqualStrings("InvalidUpdateArgument", failure.message);
        },
        else => return err,
    }

    var update = try ctx.client.updateWorkflow(Update, run.id, run.run_id, .{update_arg}, .{});
    defer update.deinit();
    try ctx.expectEqualStrings("update-result:" ++ update_arg, try update.get(ctx.arena));
    try ctx.expectEqualStrings("workflow-result:" ++ update_arg, try run.get(ctx.arena));

    const recorded = try ctx.fetchHistory(run.id, run.run_id);
    try ctx.expectNoEvent(recorded, .EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_REJECTED);
    try ctx.expectEventsInOrder(recorded, &.{
        .EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED,
        .EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_COMPLETED,
        .EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
    });
}
