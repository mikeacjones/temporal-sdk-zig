//! Port of samples-go/helloworld: a workflow that calls one activity and returns its greeting.

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;
const activity = temporal.activity;

fn helloWorld(ctx: *workflow.Context, name: []const u8) ![]const u8 {
    const logger = ctx.logger();
    logger.info("HelloWorld workflow started", .{ .name = name });

    const greeting = ctx.executeActivity(Activity, .{ .start_to_close_timeout = .fromSeconds(10) }, .{name});
    const result = greeting.get() catch |err| {
        logger.err("Activity failed.", .{ .Error = err });
        return err;
    };

    logger.info("HelloWorld workflow completed.", .{ .result = result });
    return result;
}

/// The workflow, registered under the same name as the Go sample's `helloworld.Workflow`.
pub const Workflow = workflow.Def("Workflow", helloWorld);

fn greet(ctx: *activity.Context, name: []const u8) ![]const u8 {
    ctx.logger().info("Activity", .{ .name = name });
    return std.fmt.allocPrint(ctx.allocator(), "Hello {s}!", .{name});
}

pub const Activity = activity.Def("Activity", greet);
