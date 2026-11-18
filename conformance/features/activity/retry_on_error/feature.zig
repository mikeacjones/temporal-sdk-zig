const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");

const activity = temporal.activity;
const workflow = temporal.workflow;

const AlwaysFail = activity.Def("AlwaysFailActivity", alwaysFail);
const Workflow = workflow.Def("Workflow", runWorkflow);

pub const feature = harness.define(.{
    .workflows = .{Workflow},
    .activities = .{AlwaysFail},
    .execute = execute,
});

fn alwaysFail(ctx: *activity.Context) !void {
    const message = try std.fmt.allocPrint(ctx.allocator(), "activity attempt {d} failed", .{ctx.info().attempt});
    return ctx.fail(.{ .message = message, .type = "AttemptFailed" });
}

fn runWorkflow(ctx: *workflow.Context) !void {
    const future = ctx.executeActivity(AlwaysFail, .{
        .schedule_to_close_timeout = .fromSeconds(60),
        .retry_policy = .{
            .initial_interval = .fromMilliseconds(1),
            .backoff_coefficient = 1,
            .maximum_attempts = 5,
        },
    }, .{});
    try future.get();
}

fn execute(ctx: *harness.Context) !void {
    var run = try ctx.client.executeWorkflow(Workflow, ctx.startOptions(), .{});
    defer run.deinit();

    if (run.get(ctx.arena)) |_| {
        return ctx.fail("workflow completed successfully after the activity should have exhausted its retries", .{});
    } else |err| switch (err) {
        error.WorkflowFailed => {},
        else => return err,
    }
    const failure = run.failure() orelse return ctx.fail("workflow failure has no details", .{});
    const info = failure.failure_info orelse return ctx.fail("workflow failure has no kind: {s}", .{failure.message});
    const retry_state = switch (info) {
        .activity_failure_info => |activity_info| activity_info.retry_state,
        else => return ctx.fail("expected an activity failure, got {s}", .{@tagName(info)}),
    };
    try ctx.expect(retry_state == .RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED, "expected maximum activity attempts reached, got {s}", .{@tagName(retry_state)});
    const cause = failure.cause orelse return ctx.fail("activity failure has no cause: {s}", .{failure.message});
    try ctx.expectEqualStrings("activity attempt 5 failed", cause.message);
}
