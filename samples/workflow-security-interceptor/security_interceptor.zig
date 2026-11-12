//! Workflow security interceptor: a worker interceptor that checks every child workflow a
//! workflow starts against an activity before letting it start, and refuses child workflows
//! of unapproved types.

const std = @import("std");
const temporal = @import("temporal");
const activity = temporal.activity;
const interceptor = temporal.interceptor;
const workflow = temporal.workflow;
const Payload = temporal.converter.Payload;

fn childWorkflow(_: *workflow.Context) ![]const u8 {
    return "OK";
}

pub const ChildWorkflow = workflow.Def("ChildWorkflow", childWorkflow);

/// Same as `ChildWorkflow`, but the interceptor never lets it start.
pub const ProhibitedChildWorkflow = workflow.Def("ProhibitedChildWorkflow", childWorkflow);

/// Starts an approved child workflow, then a prohibited one, and fails with the refusal of
/// the second.
fn securityWorkflow(ctx: *workflow.Context) !void {
    _ = try ctx.executeChildWorkflow(ChildWorkflow, .{}, .{}).get();
    // This fails because the child workflow type is not allowed.
    _ = try ctx.executeChildWorkflow(ProhibitedChildWorkflow, .{}, .{}).get();
}

pub const Workflow = workflow.Def("Workflow", securityWorkflow);

fn validateChildWorkflowType(_: *activity.Context, child_workflow_type: []const u8) !bool {
    return std.mem.eql(u8, child_workflow_type, ChildWorkflow.workflow_name);
}

/// Decides whether a child workflow type may start. The interceptor calls it.
pub const ValidateChildWorkflowTypeActivity = activity.Def("ValidateChildWorkflowTypeActivity", validateChildWorkflowType);

/// The worker interceptor: install it with `worker.Options.interceptors`. It holds no state,
/// so one value serves every worker and run.
pub const WorkerInterceptor = struct {
    pub fn interceptWorkflow(_: *WorkerInterceptor, ctx: *workflow.Context) error{OutOfMemory}!interceptor.Workflow {
        const run = try ctx.allocator().create(WorkflowOutbound);
        run.* = .{ .ctx = ctx };
        return .{ .outbound = interceptor.WorkflowOutbound.from(WorkflowOutbound, run) };
    }
};

/// One run's outbound interceptor, in the run's memory.
const WorkflowOutbound = struct {
    ctx: *workflow.Context,

    /// Runs inside the workflow, so it may wait on the validation activity: the workflow's
    /// call to start the child returns once the activity has decided. On replay the activity's
    /// result comes from history, so the same children start or are refused.
    pub fn executeChildWorkflow(o: *WorkflowOutbound, input: interceptor.ChildWorkflowInput, next: interceptor.WorkflowOutbound.Next) workflow.ChildWorkflowFuture(Payload) {
        const ctx = o.ctx;
        const validation = ctx.executeActivity(ValidateChildWorkflowTypeActivity, .{ .start_to_close_timeout = .fromSeconds(10) }, .{input.workflow_type});
        const allowed = validation.get() catch |err| {
            const cause = if (validation.failure()) |f| temporal.failure.rootCause(f).message else @errorName(err);
            const message = std.fmt.allocPrint(ctx.allocator(), "Validating child workflow type \"{s}\" failed: {s}", .{ input.workflow_type, cause }) catch "Validating a child workflow type failed";
            return interceptor.rejectChildWorkflow(ctx, input.workflow_type, .{ .message = message, .type = "validation-failed" });
        };
        if (!allowed) {
            const message = std.fmt.allocPrint(ctx.allocator(), "Child workflow type \"{s}\" not allowed", .{input.workflow_type}) catch "Child workflow type not allowed";
            return interceptor.rejectChildWorkflow(ctx, input.workflow_type, .{ .message = message, .type = "not-allowed" });
        }
        return next.executeChildWorkflow(input);
    }
};
