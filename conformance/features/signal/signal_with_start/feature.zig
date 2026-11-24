//! Signal-with-start of a workflow already running only signals the existing run; of a
//! workflow that is not running, it starts the workflow and delivers the signal to it before
//! the workflow's code can complete the run.
const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");
const workflow = temporal.workflow;

const Workflow = workflow.Def("Workflow", runWorkflow);
const Add = workflow.SignalDef("add", fn (i64) void);
/// The SDK gap behind the new-workflow check's failure: a signal buffered when its handler is
/// registered runs only once the workflow function blocks.
const gap_bead = "temporal-sdk-zig-d51.7.49";

pub const feature = harness.define(.{
    .workflows = .{Workflow},
    .execute = execute,
});

const State = struct {
    value: i64 = 0,

    fn add(state: *State, arg: i64) void {
        state.value += arg;
    }

    fn positive(state: *State) bool {
        return state.value > 0;
    }
};

fn runWorkflow(ctx: *workflow.Context, arg: i64) !i64 {
    const state = try ctx.allocator().create(State);
    state.* = .{};
    try ctx.setSignalHandler(Add, state, State.add);
    state.value += arg;
    try ctx.await(state, State.positive);
    return state.value;
}

fn execute(ctx: *harness.Context) !void {
    try signalToExistingWorkflow(ctx);
    try signalProcessedBeforeHandler(ctx);
}

/// Signal-with-start of a workflow that does not exist starts it with its argument and
/// delivers the signal: 1 + 42. The workflow's condition already holds after it adds its
/// argument, so it is only right if the start-carried signal was processed first.
fn signalProcessedBeforeHandler(ctx: *harness.Context) !void {
    var options = ctx.startOptions();
    options.id = try std.fmt.allocPrint(ctx.arena, "{s}-new", .{ctx.task_queue});
    var run = try ctx.client.signalWithStartWorkflow(Workflow, options, .{1}, Add, .{42});
    defer run.deinit();
    const result = try run.get(ctx.arena);
    try ctx.expect(result == 43, "signal-with-start of a new workflow: expected result 43, got {d}: the workflow completed before processing the signal sent with its start (" ++ gap_bead ++ ")", .{result});

    const recorded = try ctx.fetchHistory(run.id, run.run_id);
    try ctx.expectEventsInOrder(recorded, &.{
        .EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
        .EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
        .EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
        .EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
    });
}

/// Signal-with-start of a running workflow only signals it: its argument stays -2, the
/// start arguments are ignored, and the run is the one already running: -2 + 42.
fn signalToExistingWorkflow(ctx: *harness.Context) !void {
    var options = ctx.startOptions();
    options.id = try std.fmt.allocPrint(ctx.arena, "{s}-existing", .{ctx.task_queue});
    var existing = try ctx.client.executeWorkflow(Workflow, options, .{-2});
    defer existing.deinit();
    var run = try ctx.client.signalWithStartWorkflow(Workflow, options, .{0}, Add, .{42});
    defer run.deinit();
    try ctx.expect(std.mem.eql(u8, existing.run_id, run.run_id), "signal-with-start of a running workflow: expected its run {s}, got run {s}", .{ existing.run_id, run.run_id });
    const result = try run.get(ctx.arena);
    try ctx.expect(result == 40, "signal-with-start of a running workflow: expected result 40, got {d}", .{result});
}
