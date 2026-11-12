//! Dynamic: a workflow that calls its activities by their type names, given as strings at run
//! time, rather than through their definitions. The starter likewise starts the workflow by its
//! type name without importing this file.

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;
const activity = temporal.activity;

fn getName(ctx: *activity.Context) ![]const u8 {
    _ = ctx;
    return "Temporal";
}

fn getGreeting(ctx: *activity.Context) ![]const u8 {
    _ = ctx;
    return "Hello";
}

fn sayGreeting(ctx: *activity.Context, greeting: []const u8, name: []const u8) ![]const u8 {
    return std.fmt.allocPrint(ctx.allocator(), "Greeting: {s} {s}!\n", .{ greeting, name });
}

// The worker registers the activities under these names; the workflow refers to them only by
// the strings.
pub const GetName = activity.Def("GetName", getName);
pub const GetGreeting = activity.Def("GetGreeting", getGreeting);
pub const SayGreeting = activity.Def("SayGreeting", sayGreeting);

fn sampleGreetingsWorkflow(ctx: *workflow.Context) !void {
    const logger = ctx.logger();
    const ao: workflow.ActivityOptions = .{ .start_to_close_timeout = .fromSeconds(10) };

    const greet_result = ctx.executeActivityByName([]const u8, "GetGreeting", ao, .{}).get() catch |err| {
        logger.err("Get greeting failed.", .{ .err = err });
        return err;
    };

    // Get Name.
    const name_result = ctx.executeActivityByName([]const u8, "GetName", ao, .{}).get() catch |err| {
        logger.err("Get name failed.", .{ .err = err });
        return err;
    };

    // Say Greeting.
    const say_result = ctx.executeActivityByName([]const u8, "SayGreeting", ao, .{ greet_result, name_result }).get() catch |err| {
        logger.err("Say greeting failed.", .{ .err = err });
        return err;
    };

    logger.info("Workflow completed.", .{ .result = say_result });
}

pub const SampleGreetingsWorkflow = workflow.Def("SampleGreetingsWorkflow", sampleGreetingsWorkflow);
