//! A workflow cancels a running asynchronous Nexus operation and sees it end canceled. The
//! operation's backing workflow blocks until it is canceled, so cancellation never races
//! with completion. The caller waits until the operation has started, then cancels the
//! coroutine waiting for its result (the Zig form of the README's cancellable scope), which
//! requests the operation's cancellation; with the default cancellation type that wait ends
//! once the backing workflow has ended canceled. The caller completes with "canceled", and
//! the history has the cancel request and the operation's cancellation.
const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");

const failure = temporal.failure;
const nexus = temporal.nexus;
const workflow = temporal.workflow;
const BlockForever = nexus.OperationDef("block-forever", []const u8, []const u8);
const Service = nexus.ServiceDef("test-service", .{BlockForever});
const Handler = nexus.ServiceHandler(Service, .{nexus.OperationHandler(BlockForever, blockForever)});
const Workflow = workflow.Def("Workflow", runWorkflow);
const BlockingWorkflow = workflow.Def("BlockingWorkflow", runBlockingWorkflow);
const OperationFuture = workflow.NexusOperationFuture([]const u8);

/// The ID `blockForever` gives the backing workflow for the input "world".
const backing_workflow_id = "async-cancellation-world";

pub const feature = harness.define(.{
    .workflows = .{ Workflow, BlockingWorkflow },
    .nexus_services = .{Handler},
    .execute = execute,
});

fn never(_: void) bool {
    return false;
}

/// Ends only when canceled, which returns `error.Canceled` and ends the run canceled.
fn runBlockingWorkflow(ctx: *workflow.Context, name: []const u8) ![]const u8 {
    _ = name;
    try ctx.await({}, never);
    return ctx.fail(.{ .message = "the blocking workflow was unblocked", .non_retryable = true });
}

/// Starts `BlockingWorkflow` as "async-cancellation-<name>" on the worker's task queue.
fn blockForever(ctx: *nexus.Context, name: []const u8) !nexus.WorkflowStart(BlockingWorkflow) {
    const id = try std.fmt.allocPrint(ctx.allocator(), "async-cancellation-{s}", .{name});
    return .{ .options = .{ .id = id }, .args = .{name} };
}

fn runWorkflow(ctx: *workflow.Context, endpoint: []const u8) ![]const u8 {
    const io = ctx.io();
    const operation = ctx.executeNexusOperation(Service, BlockForever, .{
        .endpoint = endpoint,
        .schedule_to_close_timeout = .fromSeconds(60),
    }, "world");
    _ = try operation.started();

    var waiting = io.async(OperationFuture.get, .{operation});
    const outcome = waiting.cancel(io);
    _ = outcome catch |err| switch (err) {
        error.Canceled => {
            const top = operation.failure() orelse
                return ctx.fail(.{ .message = "the canceled operation has no failure", .non_retryable = true });
            if (!isCanceled(top)) {
                const message = try std.fmt.allocPrint(ctx.allocator(), "expected a canceled failure, got: {s}", .{top.message});
                return ctx.fail(.{ .message = message, .non_retryable = true });
            }
            return "canceled";
        },
        error.NexusOperationFailed => {
            const message = try std.fmt.allocPrint(ctx.allocator(), "expected a canceled error, the operation failed: {s}", .{if (operation.failure()) |f| f.message else "no failure"});
            return ctx.fail(.{ .message = message, .non_retryable = true });
        },
        else => return err,
    };
    return ctx.fail(.{ .message = "expected the canceled operation to fail", .non_retryable = true });
}

/// Whether `top` or a failure in its cause chain is a canceled failure.
fn isCanceled(top: *const failure.Failure) bool {
    var current: ?*const failure.Failure = top;
    while (current) |f| : (current = f.cause) {
        if (failure.kind(f) == .canceled) return true;
    }
    return false;
}

fn execute(ctx: *harness.Context) !void {
    const endpoint = ctx.nexus_endpoint orelse return ctx.fail("missing Nexus endpoint for {s}", .{ctx.task_queue});
    var run = try ctx.client.executeWorkflow(Workflow, ctx.startOptions(), .{endpoint});
    defer run.deinit();
    // The backing workflow never ends on its own: close it on a path where the caller failed
    // to cancel it. After a pass it has ended canceled, and this does nothing.
    defer ctx.client.terminateWorkflow(backing_workflow_id, "", "feature cleanup") catch {};
    const result = run.get(ctx.arena) catch |err| {
        if (run.failure()) |f| return ctx.fail("workflow failed: {s}", .{f.message});
        return err;
    };
    try ctx.expectEqualStrings("canceled", result);
    const recorded = try ctx.fetchHistory(run.id, run.run_id);
    try ctx.expectEventsInOrder(recorded, &.{
        .EVENT_TYPE_NEXUS_OPERATION_STARTED,
        .EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUESTED,
        .EVENT_TYPE_NEXUS_OPERATION_CANCELED,
    });

    // The default cancellation type resolves the operation only once the backing workflow
    // has ended, so it is closed by now.
    const backing = try ctx.client.describeWorkflow(ctx.arena, backing_workflow_id, "");
    try ctx.expect(backing.status == .canceled, "expected backing workflow {s} to have ended canceled, its status is {s}", .{ backing_workflow_id, std.enums.tagName(temporal.client.WorkflowExecutionStatus, backing.status) orelse "unknown" });
}
