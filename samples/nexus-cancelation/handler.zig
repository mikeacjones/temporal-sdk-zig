//! The handler side: the say-hello operation of `service.HelloService` and the workflow
//! behind it, served by a worker in namespace `namespace` on task queue `task_queue`, where
//! the Nexus endpoint routes. The workflow works for a while before it greets; canceled, it
//! cleans up and then ends canceled.

const std = @import("std");
const temporal = @import("temporal");
const service = @import("service.zig");
const nexus = temporal.nexus;
const workflow = temporal.workflow;

pub const namespace = "my-cancelation-target-namespace";
pub const task_queue = "nexus-cancelation-handler";

fn helloHandlerWorkflow(ctx: *workflow.Context, input: service.HelloInput) !service.HelloOutput {
    work(ctx, input.work_ms) catch |err| switch (err) {
        error.Canceled => {
            // Delivering the cancellation consumed it, so the cleanup's wait runs to its end
            // without blocking cancellation around it.
            const cleanup = try duration(ctx, input.cleanup_ms);
            ctx.logger().info("canceled, cleaning up", .{ .language = @tagName(input.language), .cleanup_ms = cleanup.toMilliseconds() });
            try ctx.io().sleep(cleanup, .awake);
            return error.Canceled; // the workflow ends canceled
        },
        else => |e| return e,
    };
    const greeting = switch (input.language) {
        .en => "Hello",
        .fr => "Bonjour",
        .de => "Hallo",
        .es => "¡Hola!",
        .tr => "Merhaba",
    };
    return .{ .message = try std.fmt.allocPrint(ctx.allocator(), "{s} {s} 👋", .{ greeting, input.name }) };
}

pub const HelloHandlerWorkflow = workflow.Def("HelloHandlerWorkflow", helloHandlerWorkflow);

/// Stands in for real work: waits `work_ms`, or a random time.
fn work(ctx: *workflow.Context, work_ms: ?u32) !void {
    try ctx.io().sleep(try duration(ctx, work_ms), .awake);
}

/// `ms` milliseconds, or a whole number of seconds from 0 to 4 drawn at random.
fn duration(ctx: *workflow.Context, ms: ?u32) !std.Io.Duration {
    return .fromMilliseconds(ms orelse try ctx.sideEffect(u32, {}, randomMilliseconds));
}

/// Random on purpose: `ctx.sideEffect` records the value, so a replay reads it from history
/// instead of calling this. The workflow's own `io` draws seeded, replayable randomness, so
/// this takes the process-wide one.
fn randomMilliseconds(_: void) u32 {
    var bytes: [4]u8 = undefined;
    std.Io.Threaded.global_single_threaded.io().random(&bytes);
    return std.mem.readInt(u32, &bytes, .little) % 5 * std.time.ms_per_s;
}

/// Starts `HelloHandlerWorkflow` with a business ID derived from the input. The task queue
/// defaults to the one this operation is handled on.
fn sayHello(ctx: *nexus.Context, input: service.HelloInput) !nexus.WorkflowStart(HelloHandlerWorkflow) {
    return .{ .options = .{ .id = try service.helloWorkflowId(ctx.allocator(), input) }, .args = .{input} };
}

pub const HelloServiceHandler = nexus.ServiceHandler(service.HelloService, .{
    nexus.OperationHandler(service.SayHello, sayHello),
});

/// Registers the service and the workflow its say-hello operation starts.
pub fn register(w: *temporal.worker.Worker) !void {
    try w.registerWorkflow(HelloHandlerWorkflow);
    try w.registerNexusService(HelloServiceHandler);
}

/// Connects a client to `namespace`, with the rest of its options from the environment and
/// the Temporal config file (`temporal.client.loadDefaultOptions`). Close it with `close`.
pub fn connect(gpa: std.mem.Allocator, io: std.Io, arena: std.mem.Allocator) !temporal.client.Client {
    var options = try temporal.client.loadDefaultOptions(arena);
    options.namespace = namespace;
    return temporal.client.Client.connect(gpa, io, options);
}
