const temporal = @import("temporal");
const harness = @import("harness");
const workflow = temporal.workflow;

const Count = workflow.Def("Count", runCount);
const IncrementCount = workflow.UpdateDef("incrementCount", fn () i64);
const Shutdown = workflow.SignalDef("shutdown_signal", fn () void);
const reused_update_id = "reused_update_id";
const expected_count = 1;

pub const feature = harness.define(.{
    .workflows = .{Count},
    .execute = execute,
});

const State = struct {
    ctx: *workflow.Context,
    counter: i64 = 0,
    shutdown: bool = false,

    fn increment(state: *State) !i64 {
        state.counter += 1;
        // Deduplication must not wait for the first update to complete.
        try state.ctx.sleep(.fromSeconds(1), .{});
        return state.counter;
    }

    fn stop(state: *State) void {
        state.shutdown = true;
    }

    fn stopped(state: *State) bool {
        return state.shutdown;
    }
};

fn runCount(ctx: *workflow.Context) !void {
    const state = try ctx.allocator().create(State);
    state.* = .{ .ctx = ctx };
    try ctx.setUpdateHandler(IncrementCount, state, State.increment);
    try ctx.setSignalHandler(Shutdown, state, State.stop);
    try ctx.await(state, State.stopped);
}

fn execute(ctx: *harness.Context) !void {
    var run = try ctx.client.executeWorkflow(Count, ctx.startOptions(), .{});
    defer run.deinit();

    const options: temporal.client.UpdateOptions = .{ .update_id = reused_update_id, .wait_for_stage = .accepted };
    var first = try ctx.client.updateWorkflow(IncrementCount, run.id, run.run_id, .{}, options);
    defer first.deinit();
    var second = try ctx.client.updateWorkflow(IncrementCount, run.id, run.run_id, .{}, options);
    defer second.deinit();

    for ([_]*temporal.client.WorkflowUpdateHandle(i64){ &first, &second }, [_][]const u8{ "first", "second" }) |handle, which| {
        const result = handle.get(ctx.arena) catch |err| switch (err) {
            error.UpdateFailed => return ctx.fail("{s} update failed: {s}", .{ which, if (handle.failure()) |f| f.message else "no failure details" }),
            else => return err,
        };
        try ctx.expect(result == expected_count, "expected the {s} update to return {d}, got {d}", .{ which, expected_count, result });
    }

    try ctx.client.signalWorkflow(Shutdown, run.id, run.run_id, .{});
    try run.get(ctx.arena);

    const recorded = try ctx.fetchHistory(run.id, run.run_id);
    try ctx.expectNoEvent(recorded, .EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_REJECTED);
    const accepted = harness.history.count(recorded, .EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED);
    const completed = harness.history.count(recorded, .EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_COMPLETED);
    try ctx.expect(accepted == expected_count, "expected {d} accepted update in history, found {d}", .{ expected_count, accepted });
    try ctx.expect(completed == expected_count, "expected {d} completed update in history, found {d}", .{ expected_count, completed });
}
