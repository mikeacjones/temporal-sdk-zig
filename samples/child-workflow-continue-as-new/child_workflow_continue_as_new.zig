//! Child workflow continue-as-new: a parent workflow runs a child workflow that continues as
//! new several times, carrying its counters from run to run. The parent waits on one child
//! future and sees only the result of the child's last run.

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;

pub const task_queue = "child-workflow-continue-as-new";

/// The parent starts the child with this many runs.
pub const child_runs = 5;

fn sampleChildWorkflow(ctx: *workflow.Context, total_count: i64, run_count: i64) ![]const u8 {
    const logger = ctx.logger();
    logger.info("Child workflow execution started.", .{});
    if (run_count <= 0) {
        return ctx.fail(.{
            .message = try std.fmt.allocPrint(ctx.allocator(), "invalid run count {d}: it must be at least 1", .{run_count}),
            .type = "InvalidRunCount",
        });
    }

    const total = total_count + 1;
    const remaining = run_count - 1;
    if (remaining == 0) {
        const result = try std.fmt.allocPrint(ctx.allocator(), "Child workflow execution completed after {d} runs", .{total});
        logger.info("Child workflow completed.", .{ .result = result });
        return result;
    }

    logger.info("Child workflow starting new run.", .{ .run_count = remaining, .total_count = total });
    return ctx.continueAsNew(SampleChildWorkflow, .{}, .{ total, remaining });
}

pub const SampleChildWorkflow = workflow.Def("SampleChildWorkflow", sampleChildWorkflow);

fn sampleParentWorkflow(ctx: *workflow.Context) ![]const u8 {
    const logger = ctx.logger();

    // The child's ID is unique per parent run. Every run of the child keeps this ID.
    const child_id = try std.fmt.allocPrint(ctx.allocator(), "child_workflow:{s}", .{ctx.info().run_id});

    const child = ctx.executeChildWorkflow(SampleChildWorkflow, .{ .id = child_id }, .{ 0, child_runs });
    const result = child.get() catch |err| {
        logger.err("Parent execution received child execution failure.", .{ .err = err });
        return err;
    };

    logger.info("Parent execution completed.", .{ .result = result });
    return result;
}

pub const SampleParentWorkflow = workflow.Def("SampleParentWorkflow", sampleParentWorkflow);
