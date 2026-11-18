const temporal = @import("temporal");
const harness = @import("harness");

const Workflow = temporal.workflow.Def("Workflow", runWorkflow);
const greeting = "Hello World";

pub const feature = harness.define(.{
    .workflows = .{Workflow},
    .execute = execute,
});

fn runWorkflow(_: *temporal.workflow.Context) ![]const u8 {
    return greeting;
}

fn execute(ctx: *harness.Context) !void {
    var options = ctx.startOptions();
    options.enable_eager_start = true;
    options.task_timeout = .fromSeconds(3600);
    var run = try ctx.client.executeWorkflow(Workflow, options, .{});
    defer run.deinit();

    try ctx.expect(run.eagerly_started, "workflow {s} did not start eagerly", .{run.id});
    try ctx.expectEqualStrings(greeting, try run.get(ctx.arena));
}
