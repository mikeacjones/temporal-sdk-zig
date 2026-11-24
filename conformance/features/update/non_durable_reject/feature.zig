const temporal = @import("temporal");
const harness = @import("harness");
const workflow = temporal.workflow;

const NonDurableReject = workflow.Def("NonDurableReject", runWorkflow);
const UpdateAdd = workflow.UpdateDef("updateActivity", fn (i64) i64);
const Shutdown = workflow.SignalDef("shutdown_signal", fn () void);
const step = 2;
const count = 5;

pub const feature = harness.define(.{
    .workflows = .{NonDurableReject},
    .execute = execute,
});

const State = struct {
    counter: i64 = 0,
    shutdown: bool = false,

    fn nonNegative(_: *State, i: i64) error{NegativeValue}!void {
        if (i < 0) return error.NegativeValue;
    }

    fn add(state: *State, i: i64) i64 {
        state.counter += i;
        return state.counter;
    }

    fn stop(state: *State) void {
        state.shutdown = true;
    }

    fn stopped(state: *State) bool {
        return state.shutdown;
    }
};

fn runWorkflow(ctx: *workflow.Context) !i64 {
    const state = try ctx.allocator().create(State);
    state.* = .{};
    try ctx.setUpdateHandlerWithValidator(UpdateAdd, state, State.add, State.nonNegative);
    try ctx.setSignalHandler(Shutdown, state, State.stop);
    try ctx.await(state, State.stopped);
    return state.counter;
}

fn execute(ctx: *harness.Context) !void {
    var run = try ctx.client.executeWorkflow(NonDurableReject, ctx.startOptions(), .{});
    defer run.deinit();

    try expectRejected(ctx, &run, -1);
    for (0..count) |i| {
        var update = try ctx.client.updateWorkflow(UpdateAdd, run.id, run.run_id, .{step}, .{});
        defer update.deinit();
        const result = update.get(ctx.arena) catch |err| switch (err) {
            error.UpdateFailed => return ctx.fail("expected update {d} with {d} to be accepted, it failed: {s}", .{ i, step, if (update.failure()) |f| f.message else "no failure details" }),
            else => return err,
        };
        const expected: i64 = @intCast(step * (i + 1));
        try ctx.expect(result == expected, "expected update {d} to return {d}, got {d}", .{ i, expected, result });
        try expectRejected(ctx, &run, -1);
    }

    try ctx.client.signalWorkflow(Shutdown, run.id, run.run_id, .{});
    const result = try run.get(ctx.arena);
    try ctx.expect(result == step * count, "expected the workflow to return {d}, got {d}", .{ step * count, result });

    const recorded = try ctx.fetchHistory(run.id, run.run_id);
    try ctx.expectNoEvent(recorded, .EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_REJECTED);
    const accepted = harness.history.count(recorded, .EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED);
    try ctx.expect(accepted == count, "expected {d} accepted updates in history, found {d}", .{ count, accepted });
    // The start, each accepted update and the signal take one workflow task each; a task
    // that carried only a rejected update leaves none.
    const tasks = harness.history.count(recorded, .EVENT_TYPE_WORKFLOW_TASK_COMPLETED);
    const max_tasks = 1 + count + 1;
    try ctx.expect(tasks <= max_tasks, "expected at most {d} completed workflow tasks, found {d}: a task with only rejected updates reached history", .{ max_tasks, tasks });
}

fn expectRejected(ctx: *harness.Context, run: *temporal.client.WorkflowRun(i64), value: i64) !void {
    var update = try ctx.client.updateWorkflow(UpdateAdd, run.id, run.run_id, .{value}, .{});
    defer update.deinit();
    if (update.get(ctx.arena)) |result| {
        return ctx.fail("expected the validator to reject {d}, the update returned {d}", .{ value, result });
    } else |err| switch (err) {
        error.UpdateFailed => {
            const failure = update.failure() orelse return ctx.fail("rejected update has no failure details", .{});
            try ctx.expectEqualStrings("NegativeValue", failure.message);
        },
        else => return err,
    }
}
