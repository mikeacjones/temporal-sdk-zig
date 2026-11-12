//! A standalone activity: the same greeting as the hello world sample, but the client runs
//! the activity directly instead of a workflow orchestrating it.

const std = @import("std");
const temporal = @import("temporal");
const activity = temporal.activity;

fn greet(ctx: *activity.Context, name: []const u8) ![]const u8 {
    ctx.logger().info("Activity", .{ .name = name });
    return std.fmt.allocPrint(ctx.allocator(), "Hello {s}!", .{name});
}

pub const Activity = activity.Def("Activity", greet);
