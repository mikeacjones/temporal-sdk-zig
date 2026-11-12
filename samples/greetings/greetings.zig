//! Greetings: a workflow that runs three activities in sequence. The activities are methods
//! of a struct holding their shared state. `activities` declares their Defs once: the workflow
//! calls the activities through it, and the worker registers all of them from it, bound to an
//! instance, so both sides use the same names.

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;
const activity = temporal.activity;

pub const Activities = struct {
    name: []const u8,
    greeting: []const u8,

    pub fn getGreeting(a: *Activities, ctx: *activity.Context) ![]const u8 {
        _ = ctx;
        return a.greeting;
    }

    pub fn getName(a: *Activities, ctx: *activity.Context) ![]const u8 {
        _ = ctx;
        return a.name;
    }

    pub fn sayGreeting(a: *Activities, ctx: *activity.Context, greeting: []const u8, name: []const u8) ![]const u8 {
        _ = a;
        return std.fmt.allocPrint(ctx.allocator(), "Greeting: {s} {s}!\n", .{ greeting, name });
    }
};

/// One Def per activity method, named after the method: "getGreeting", "getName", and
/// "sayGreeting". The worker registers this same value.
pub const activities = activity.defs(Activities, .{});

fn greetings(ctx: *workflow.Context) ![]const u8 {
    const logger = ctx.logger();
    const ao: workflow.ActivityOptions = .{ .start_to_close_timeout = .fromSeconds(10) };

    const greet_result = ctx.executeActivity(activities.getGreeting, ao, .{}).get() catch |err| {
        logger.err("Get greeting failed.", .{ .err = err });
        return err;
    };

    // Get Name.
    const name_result = ctx.executeActivity(activities.getName, ao, .{}).get() catch |err| {
        logger.err("Get name failed.", .{ .err = err });
        return err;
    };

    // Say Greeting.
    const say_result = ctx.executeActivity(activities.sayGreeting, ao, .{ greet_result, name_result }).get() catch |err| {
        logger.err("Say greeting failed.", .{ .err = err });
        return err;
    };

    logger.info("Greetings completed.", .{ .result = say_result });
    return say_result;
}

pub const Greetings = workflow.Def("Greetings", greetings);
