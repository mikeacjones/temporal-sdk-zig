const temporal = @import("temporal");
const harness = @import("harness");
const workflow = temporal.workflow;

const Workflow = workflow.Def("Workflow", runWorkflow);
const CounterQuery = workflow.QueryDef("counterQ", fn () u32);
const CounterIncrement = workflow.SignalDef("counterInc", fn () void);
const increments_to_finish = 5;

pub const feature = harness.define(.{
    .workflows = .{Workflow},
    .execute = execute,
});

const Counter = struct {
    value: u32 = 0,

    fn read(state: *const Counter) u32 {
        return state.value;
    }

    fn increment(state: *Counter) void {
        state.value += 1;
    }

    fn finished(state: *Counter) bool {
        return state.value >= increments_to_finish;
    }
};

fn runWorkflow(ctx: *workflow.Context) !void {
    const counter = try ctx.allocator().create(Counter);
    counter.* = .{};
    try ctx.setQueryHandler(CounterQuery, @as(*const Counter, counter), Counter.read);
    try ctx.setSignalHandler(CounterIncrement, counter, Counter.increment);
    try ctx.await(counter, Counter.finished);
}

fn execute(ctx: *harness.Context) !void {
    var run = try ctx.client.executeWorkflow(Workflow, ctx.startOptions(), .{});
    defer run.deinit();

    for (0..increments_to_finish) |sent| {
        if (sent == 0 or sent == 1 or sent == 4) {
            const value = try ctx.client.queryWorkflowWithOptions(CounterQuery, ctx.arena, run.id, run.run_id, .{}, .{ .reject_condition = .not_open });
            try ctx.expect(value == sent, "counterQ after {d} increments returned {d}", .{ sent, value });
        }
        try ctx.client.signalWorkflow(CounterIncrement, run.id, run.run_id, .{});
    }
    try run.get(ctx.arena);
}
