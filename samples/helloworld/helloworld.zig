//! Hello world: a workflow that calls one activity and returns its greeting.

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;
const activity = temporal.activity;

fn helloWorld(ctx: *workflow.Context, name: []const u8) ![]const u8 {
    const logger = ctx.logger();
    logger.info("HelloWorld workflow started", .{ .name = name });

    const greeting = ctx.executeActivity(Greet, .{ .start_to_close_timeout = .fromSeconds(10) }, .{name});
    const result = greeting.get() catch |err| {
        logger.err("Activity failed.", .{ .err = err });
        return err;
    };

    logger.info("HelloWorld workflow completed.", .{ .result = result });
    return result;
}

pub const HelloWorld = workflow.Def("HelloWorld", helloWorld);

fn greet(ctx: *activity.Context, name: []const u8) ![]const u8 {
    ctx.logger().info("Greet", .{ .name = name });
    return std.fmt.allocPrint(ctx.allocator(), "Hello {s}!", .{name});
}

pub const Greet = activity.Def("Greet", greet);
