//! Child workflow: a parent workflow starts a child workflow, waits for it to complete, and
//! returns the child's result.

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;

fn sampleChildWorkflow(ctx: *workflow.Context, name: []const u8) ![]const u8 {
    const greeting = try std.fmt.allocPrint(ctx.allocator(), "Hello {s}!", .{name});
    ctx.logger().info("Child workflow execution.", .{ .greeting = greeting });
    return greeting;
}

pub const SampleChildWorkflow = workflow.Def("SampleChildWorkflow", sampleChildWorkflow);

/// Each child run gets a new run ID, and the parent's future resolves only when the child's
/// last run closes (for example after it continues as new).
fn sampleParentWorkflow(ctx: *workflow.Context) ![]const u8 {
    const logger = ctx.logger();

    // The child's ID is derived from the parent's so that concurrent and repeated parents
    // don't collide on one child ID.
    const child_id = try std.fmt.allocPrint(ctx.allocator(), "{s}-child", .{ctx.info().workflow_id});

    const child = ctx.executeChildWorkflow(SampleChildWorkflow, .{ .id = child_id }, .{"World"});
    const result = child.get() catch |err| {
        logger.err("Parent execution received child execution failure.", .{ .err = err });
        return err;
    };

    logger.info("Parent execution completed.", .{ .result = result });
    return result;
}

pub const SampleParentWorkflow = workflow.Def("SampleParentWorkflow", sampleParentWorkflow);
