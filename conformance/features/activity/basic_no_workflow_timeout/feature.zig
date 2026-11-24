//! A workflow started without an execution timeout runs an activity with only a
//! start-to-close timeout and then one with only a schedule-to-close timeout. The server
//! would otherwise fill the unset activity timeouts from the workflow's.
const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");

const activity = temporal.activity;
const workflow = temporal.workflow;

const Workflow = workflow.Def("Workflow", runWorkflow);
const Echo = activity.Def("Echo", echo);

pub const feature = harness.define(.{
    .workflows = .{Workflow},
    .activities = .{Echo},
    .execute = execute,
});

fn echo(_: *activity.Context) ![]const u8 {
    return "echo";
}

fn runWorkflow(ctx: *workflow.Context) ![]const u8 {
    _ = try ctx.executeActivity(Echo, .{ .start_to_close_timeout = .fromSeconds(60) }, .{}).get();
    return ctx.executeActivity(Echo, .{ .schedule_to_close_timeout = .fromSeconds(60) }, .{}).get();
}

fn execute(ctx: *harness.Context) !void {
    var options = ctx.startOptions();
    options.execution_timeout = null;
    var run = try ctx.client.executeWorkflow(Workflow, options, .{});
    defer run.deinit();
    const result = run.get(ctx.arena) catch |err| {
        if (run.failure()) |failure| return ctx.fail("workflow failed: {s}", .{failure.message});
        return err;
    };
    try ctx.expectEqualStrings("echo", result);

    // The server records an unset timeout as absent or as zero.
    const recorded = try ctx.fetchHistory(run.id, run.run_id);
    const first = if (recorded.events.items.len == 0) null else recorded.events.items[0].attributes;
    const attributes = first orelse return ctx.fail("history does not start with WorkflowExecutionStarted attributes", .{});
    if (attributes != .workflow_execution_started_event_attributes)
        return ctx.fail("history does not start with WorkflowExecutionStarted attributes", .{});
    if (attributes.workflow_execution_started_event_attributes.workflow_execution_timeout) |timeout| {
        try ctx.expect(timeout.seconds == 0 and timeout.nanos == 0, "expected no workflow execution timeout, got {d}s {d}ns", .{ timeout.seconds, timeout.nanos });
    }
}
