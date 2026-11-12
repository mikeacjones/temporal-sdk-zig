//! Snappy-compressed payloads: a workflow that greets someone through an activity, run by a
//! client whose payload codec (`codec.zig`) compresses every input and result. The workflow
//! and activity don't know their data is compressed; only their history shows it.

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;
const activity = temporal.activity;

pub const task_queue = "snappycompress";

/// The activity's input: fields by name, encoded as a JSON object.
pub const Info = std.json.ArrayHashMap([]const u8);

fn compressedWorkflow(ctx: *workflow.Context, name: []const u8) ![]const u8 {
    const logger = ctx.logger();
    logger.info("Compressed Payloads workflow started", .{ .name = name });

    var info: Info = .{};
    try info.map.put(ctx.allocator(), "name", name);

    const greeting = ctx.executeActivity(Activity, .{ .start_to_close_timeout = .fromSeconds(10) }, .{info});
    const result = greeting.get() catch |err| {
        logger.err("Activity failed.", .{ .err = err });
        return err;
    };

    logger.info("Compressed Payloads workflow completed.", .{ .result = result });
    return result;
}

pub const Workflow = workflow.Def("Workflow", compressedWorkflow);

fn greet(ctx: *activity.Context, info: Info) ![]const u8 {
    const a = ctx.allocator();
    ctx.logger().info("Activity", .{ .info = try std.json.Stringify.valueAlloc(a, info, .{}) });
    const name = info.map.get("name") orelse "someone";
    return std.fmt.allocPrint(a, "Hello {s}!", .{name});
}

pub const Activity = activity.Def("Activity", greet);
