const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");
const workflow = temporal.workflow;

const Workflow = workflow.Def("Workflow", runWorkflow);
const input_data = "InputData";
const workflow_id = "TestID";
const memo_key = "MemoKey";
const memo_value = "MemoValue";

pub const feature = harness.define(.{
    .workflows = .{Workflow},
    .execute = execute,
});

fn runWorkflow(ctx: *workflow.Context, input: []const u8) ![]const u8 {
    if (ctx.info().continued_run_id.len != 0) return input;
    return ctx.continueAsNew(Workflow, .{}, .{input});
}

fn execute(ctx: *harness.Context) !void {
    var options = ctx.startOptions();
    options.id = workflow_id;
    options.memo = try temporal.converter.toFields(ctx.arena, .{ .MemoKey = memo_value });
    var run = try ctx.client.executeWorkflow(Workflow, options, .{input_data});
    defer run.deinit();

    // get replaces the handle's run ID as it follows the chain.
    const first_run_id = try ctx.arena.dupe(u8, run.run_id);
    try ctx.expectEqualStrings(input_data, try run.get(ctx.arena));
    try ctx.expect(run.run_id.len != 0 and !std.mem.eql(u8, first_run_id, run.run_id), "workflow {s} did not advance from run {s} to a new run (got {s})", .{ run.id, first_run_id, run.run_id });

    const description = try ctx.client.describeWorkflow(ctx.arena, run.id, run.run_id);
    try ctx.expectEqualStrings(workflow_id, description.workflow_id);
    try ctx.expectEqualStrings(run.run_id, description.run_id);
    const memo = try description.memoValue([]const u8, ctx.arena, memo_key) orelse
        return ctx.fail("workflow {s} run {s} is missing memo {s}", .{ run.id, run.run_id, memo_key });
    try ctx.expectEqualStrings(memo_value, memo);
}
