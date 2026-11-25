//! The handler side: the standalone activity that builds the greeting, and the greet
//! operation that starts it. A worker in namespace `namespace` on task queue `task_queue`,
//! where the Nexus endpoint routes, serves both.

const std = @import("std");
const temporal = @import("temporal");
const service = @import("service.zig");
const activity = temporal.activity;
const nexus = temporal.nexus;

pub const namespace = "nexus-standalone-activity-handler-namespace";
pub const task_queue = "nexus-standalone-activity-handler";

fn createGreeting(ctx: *activity.Context, input: service.GreetingInput) !service.GreetingOutput {
    ctx.logger().info("CreateGreeting", .{ .name = input.name });
    return .{ .message = try std.fmt.allocPrint(ctx.allocator(), "Hello, {s}!", .{input.name}) };
}

/// Runs as a standalone activity: no workflow stands behind it.
pub const CreateGreetingActivity = activity.Def("CreateGreetingActivity", createGreeting);

/// Starts `CreateGreetingActivity` on this worker's task queue (the empty `task_queue`), so
/// the caller's operation completes when the activity does.
fn greet(ctx: *nexus.Context, input: service.GreetingInput) !nexus.ActivityStart(CreateGreetingActivity) {
    return .{
        .options = .{
            .id = try service.greetingActivityId(ctx.allocator(), input),
            .task_queue = "",
            .start_to_close_timeout = .fromSeconds(10),
        },
        .args = .{input},
    };
}

pub const GreetingServiceHandler = nexus.ServiceHandler(service.GreetingService, .{
    nexus.OperationHandler(service.Greet, greet),
});

/// Registers the service and the activity its greet operation starts.
pub fn register(w: *temporal.worker.Worker) !void {
    try w.registerActivity(CreateGreetingActivity);
    try w.registerNexusService(GreetingServiceHandler);
}

/// Connects a client to `namespace`, with the rest of its options from the environment and
/// the Temporal config file (`temporal.client.loadDefaultOptions`). Close it with `close`.
pub fn connect(gpa: std.mem.Allocator, io: std.Io, arena: std.mem.Allocator) !temporal.client.Client {
    var options = try temporal.client.loadDefaultOptions(arena);
    options.namespace = namespace;
    return temporal.client.Client.connect(gpa, io, options);
}
