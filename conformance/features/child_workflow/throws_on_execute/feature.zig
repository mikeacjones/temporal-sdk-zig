//! A child workflow fails with a non-retryable application failure; the parent returns the
//! child's error, and the client sees the parent fail with a child workflow failure whose
//! cause is the child's failure, with its message, type, retryability and details.
const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");
const converter = temporal.converter;
const failure = temporal.failure;
const workflow = temporal.workflow;

const MainWorkflow = workflow.Def("MainWorkflow", runMainWorkflow);
const ChildWorkflow = workflow.Def("ChildWorkflow", runChildWorkflow);

const failure_message = "Test message";
const failure_type = "TestError";
const Detail = struct { foo: []const u8 };
const detail_value = "bar";

pub const feature = harness.define(.{
    .workflows = .{ MainWorkflow, ChildWorkflow },
    .execute = execute,
});

fn runMainWorkflow(ctx: *workflow.Context) !void {
    const child_id = try std.fmt.allocPrint(ctx.allocator(), "{s}-child", .{ctx.info().workflow_id});
    return ctx.executeChildWorkflow(ChildWorkflow, .{ .id = child_id }, .{}).get();
}

fn runChildWorkflow(ctx: *workflow.Context) !void {
    const detail = try converter.toPayload(ctx.allocator(), Detail{ .foo = detail_value });
    return ctx.fail(.{
        .message = failure_message,
        .type = failure_type,
        .non_retryable = true,
        .details = &.{detail},
    });
}

fn execute(ctx: *harness.Context) !void {
    var run = try ctx.client.executeWorkflow(MainWorkflow, ctx.startOptions(), .{});
    defer run.deinit();

    run.get(ctx.arena) catch |err| switch (err) {
        error.WorkflowFailed => return checkFailure(ctx, run.failure()),
        else => return ctx.fail("expected {s} to fail with error.WorkflowFailed, got error.{t}", .{ MainWorkflow.workflow_name, err }),
    };
    return ctx.fail("expected {s} to fail, it completed", .{MainWorkflow.workflow_name});
}

fn checkFailure(ctx: *harness.Context, maybe_top: ?*const failure.Failure) !void {
    const top = maybe_top orelse return ctx.fail("{s} failed without a failure", .{MainWorkflow.workflow_name});
    const child = switch (failure.kind(top)) {
        .child_workflow => |c| c,
        else => |k| return ctx.fail("expected {s}'s failure to be a child workflow failure, got {t} \"{s}\"", .{ MainWorkflow.workflow_name, std.meta.activeTag(k), top.message }),
    };
    try ctx.expectEqualStrings(ChildWorkflow.workflow_name, child.workflow_type);

    const cause = top.cause orelse return ctx.fail("the child workflow failure has no cause", .{});
    const application = switch (failure.kind(cause)) {
        .application => |a| a,
        else => |k| return ctx.fail("expected the child workflow failure's cause to be an application failure, got {t} \"{s}\"", .{ std.meta.activeTag(k), cause.message }),
    };
    try ctx.expect(std.mem.indexOf(u8, cause.message, failure_message) != null, "expected the application failure's message to contain \"{s}\", got \"{s}\"", .{ failure_message, cause.message });
    try ctx.expectEqualStrings(failure_type, application.type);
    try ctx.expect(application.non_retryable, "expected the application failure to be non-retryable", .{});
    try ctx.expect(application.details.len() == 1, "expected 1 application failure detail, got {d}", .{application.details.len()});
    const detail = application.details.get(Detail, ctx.arena, 0) catch |err|
        return ctx.fail("decoding the application failure's detail returned error.{t}", .{err});
    try ctx.expectEqualStrings(detail_value, detail.foo);
}
