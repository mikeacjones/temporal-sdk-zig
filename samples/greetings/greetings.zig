//! Port of samples-go/greetings: a workflow that runs three activities in sequence. The
//! activities are methods of a struct holding their shared state, like Go's activity structs.

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

pub const GetGreeting = activity.Def("GetGreeting", Activities.getGreeting);
pub const GetName = activity.Def("GetName", Activities.getName);
pub const SayGreeting = activity.Def("SayGreeting", Activities.sayGreeting);

fn greetingSample(ctx: *workflow.Context) ![]const u8 {
    const logger = ctx.logger();
    const ao: workflow.ActivityOptions = .{ .start_to_close_timeout = .fromSeconds(10) };

    const greet_result = ctx.executeActivity(GetGreeting, ao, .{}).get() catch |err| {
        logger.err("Get greeting failed.", .{ .Error = err });
        return err;
    };

    // Get Name.
    const name_result = ctx.executeActivity(GetName, ao, .{}).get() catch |err| {
        logger.err("Get name failed.", .{ .Error = err });
        return err;
    };

    // Say Greeting.
    const say_result = ctx.executeActivity(SayGreeting, ao, .{ greet_result, name_result }).get() catch |err| {
        logger.err("Marshalling failed with error.", .{ .Error = err });
        return err;
    };

    logger.info("GreetingSample completed.", .{ .Result = say_result });
    return say_result;
}

pub const GreetingSample = workflow.Def("GreetingSample", greetingSample);
