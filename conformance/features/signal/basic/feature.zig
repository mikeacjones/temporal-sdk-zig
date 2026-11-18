const temporal = @import("temporal");
const harness = @import("harness");
const workflow = temporal.workflow;

const Workflow = workflow.Def("Workflow", runWorkflow);
const MySignal = workflow.SignalDef("my_signal", fn ([]const u8) void);

pub const feature = harness.define(.{
    .workflows = .{Workflow},
    .execute = execute,
});

const SignalState = struct {
    value: []const u8 = "",

    fn receive(state: *SignalState, value: []const u8) void {
        state.value = value;
    }

    fn received(state: *SignalState) bool {
        return state.value.len != 0;
    }
};

fn runWorkflow(ctx: *workflow.Context) ![]const u8 {
    const state = try ctx.allocator().create(SignalState);
    state.* = .{};
    try ctx.setSignalHandler(MySignal, state, SignalState.receive);
    try ctx.await(state, SignalState.received);
    return state.value;
}

fn execute(ctx: *harness.Context) !void {
    var run = try ctx.client.executeWorkflow(Workflow, ctx.startOptions(), .{});
    defer run.deinit();
    try ctx.client.signalWorkflow(MySignal, run.id, run.run_id, .{"arg"});
    try ctx.expectEqualStrings("arg", try run.get(ctx.arena));
}
