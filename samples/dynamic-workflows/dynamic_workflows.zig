//! Dynamic workflows: one workflow function and one activity function that run any workflow
//! or activity type name the worker has no definition for. The workflow reads the type it runs
//! as from its info; a type starting with "dynamic-activity" calls an activity type that is
//! also registered nowhere, so the dynamic activity runs it.

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;
const activity = temporal.activity;
const EncodedValues = temporal.converter.EncodedValues;

fn dynamicWorkflow(ctx: *workflow.Context, args: EncodedValues) ![]const u8 {
    const arg1, const arg2 = args.decode(struct { []const u8, []const u8 }, ctx.allocator()) catch |err| {
        ctx.logger().err("Failed to decode arguments.", .{ .err = err });
        return err;
    };

    const workflow_type = ctx.info().workflow_type;
    if (std.mem.startsWith(u8, workflow_type, "dynamic-activity")) {
        const ao: workflow.ActivityOptions = .{ .start_to_close_timeout = .fromSeconds(10) };
        return ctx.executeActivityByName([]const u8, "random-activity-name", ao, .{ arg1, arg2 }).get();
    }
    return std.fmt.allocPrint(ctx.allocator(), "{s} - {s} - {s}", .{ workflow_type, arg1, arg2 });
}

pub const DynamicWorkflow = workflow.DynamicDef(dynamicWorkflow);

// Reports the type of the workflow that called it, not its own activity type.
fn dynamicActivity(ctx: *activity.Context, args: EncodedValues) ![]const u8 {
    const arg1, const arg2 = args.decode(struct { []const u8, []const u8 }, ctx.allocator()) catch |err| {
        ctx.logger().err("Failed to decode arguments.", .{ .err = err });
        return err;
    };
    return std.fmt.allocPrint(ctx.allocator(), "{s} - {s} - {s}", .{ ctx.info().workflow_type, arg1, arg2 });
}

pub const DynamicActivity = activity.DynamicDef(dynamicActivity);
