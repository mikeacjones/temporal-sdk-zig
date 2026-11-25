//! The caller side: a workflow in namespace `namespace` that calls the say-hello operation of
//! `service.HelloService` through the Nexus endpoint `endpoint`. The values propagated to the
//! workflow's run go with the Nexus call on their own: the workflow does nothing for it.

const std = @import("std");
const temporal = @import("temporal");
const context = @import("context.zig");
const service = @import("service.zig");
const workflow = temporal.workflow;

pub const namespace = "my-context-caller-namespace";
pub const task_queue = "nexus-context-propagation-caller";
pub const endpoint = "nexus-context-propagation-endpoint";

fn helloCallerWorkflow(ctx: *workflow.Context, name: []const u8, language: service.Language) ![]const u8 {
    const operation = ctx.executeNexusOperation(service.HelloService, service.SayHello, .{ .endpoint = endpoint }, .{ .name = name, .language = language });
    // Waiting for the start is optional. An asynchronous operation's token identifies it, for
    // example to cancel it.
    if (try operation.started()) |token| ctx.logger().info("say-hello started", .{ .token = token });
    const output = try operation.get();
    return output.message;
}

pub const HelloCallerWorkflow = workflow.Def("HelloCallerWorkflow", helloCallerWorkflow);

pub fn register(w: *temporal.worker.Worker) !void {
    try w.registerWorkflow(HelloCallerWorkflow);
}

/// Connects a client to `namespace` with the sample's propagators, which its workers use to
/// read the values propagated to a run and pass them on to the Nexus calls it makes. The rest
/// of its options come from the environment and the Temporal config file
/// (`temporal.client.loadDefaultOptions`). Close it with `close`.
pub fn connect(gpa: std.mem.Allocator, io: std.Io, arena: std.mem.Allocator) !temporal.client.Client {
    var options = try temporal.client.loadDefaultOptions(arena);
    options.namespace = namespace;
    options.propagators = &context.propagators;
    return temporal.client.Client.connect(gpa, io, options);
}
