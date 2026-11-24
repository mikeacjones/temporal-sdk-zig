//! A synchronous Nexus operation fails with an operation error in the failed state carrying
//! an application failure of a known type and message. The calling workflow gets a Nexus
//! operation failure with that application failure in its cause chain, and completes with
//! its type and message. The history has a failed event and no completed one.
const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");

const failure = temporal.failure;
const nexus = temporal.nexus;
const workflow = temporal.workflow;
const Fail = nexus.OperationDef("fail", []const u8, []const u8);
const Service = nexus.ServiceDef("test-service", .{Fail});
const Handler = nexus.ServiceHandler(Service, .{nexus.OperationHandler(Fail, failOperation)});
const Workflow = workflow.Def("Workflow", runWorkflow);

const error_type = "TestFailure";
const error_message = "deliberate failure";

pub const feature = harness.define(.{
    .workflows = .{Workflow},
    .nexus_services = .{Handler},
    .execute = execute,
});

fn failOperation(ctx: *nexus.Context, name: []const u8) ![]const u8 {
    _ = name;
    return ctx.fail(.{ .message = error_message, .type = error_type });
}

fn runWorkflow(ctx: *workflow.Context, endpoint: []const u8) ![]const u8 {
    const operation = ctx.executeNexusOperation(Service, Fail, .{
        .endpoint = endpoint,
        .schedule_to_close_timeout = .fromSeconds(60),
    }, "world");
    _ = operation.get() catch |err| switch (err) {
        error.NexusOperationFailed => {
            const top = operation.failure() orelse
                return ctx.fail(.{ .message = "the failed operation has no failure", .non_retryable = true });
            if (top.failure_info == null or top.failure_info.? != .nexus_operation_execution_failure_info) {
                const message = try std.fmt.allocPrint(ctx.allocator(), "expected a Nexus operation failure, got: {s}", .{top.message});
                return ctx.fail(.{ .message = message, .non_retryable = true });
            }
            const app = findApplicationFailure(top) orelse {
                const message = try std.fmt.allocPrint(ctx.allocator(), "expected an application failure of type {s} in the cause chain of: {s}", .{ error_type, top.message });
                return ctx.fail(.{ .message = message, .non_retryable = true });
            };
            return std.fmt.allocPrint(ctx.allocator(), "{s}: {s}", .{ error_type, app.message });
        },
        else => return err,
    };
    return ctx.fail(.{ .message = "expected the operation to fail", .non_retryable = true });
}

/// The first failure in `top`'s cause chain that is an application failure of `error_type`.
fn findApplicationFailure(top: *const failure.Failure) ?*const failure.Failure {
    var current: ?*const failure.Failure = top;
    while (current) |f| : (current = f.cause) {
        switch (failure.kind(f)) {
            .application => |a| if (std.mem.eql(u8, a.type, error_type)) return f,
            else => {},
        }
    }
    return null;
}

fn execute(ctx: *harness.Context) !void {
    const endpoint = ctx.nexus_endpoint orelse return ctx.fail("missing Nexus endpoint for {s}", .{ctx.task_queue});
    var run = try ctx.client.executeWorkflow(Workflow, ctx.startOptions(), .{endpoint});
    defer run.deinit();
    const result = run.get(ctx.arena) catch |err| {
        if (run.failure()) |f| return ctx.fail("workflow failed: {s}", .{f.message});
        return err;
    };
    try ctx.expectEqualStrings(error_type ++ ": " ++ error_message, result);
    const recorded = try ctx.fetchHistory(run.id, run.run_id);
    try ctx.expectEvent(recorded, .EVENT_TYPE_NEXUS_OPERATION_FAILED);
    try ctx.expectNoEvent(recorded, .EVENT_TYPE_NEXUS_OPERATION_COMPLETED);
}
