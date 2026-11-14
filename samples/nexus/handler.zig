//! The handler side: the operations of `service.HelloService` and the workflow behind
//! say-hello, served by a worker in namespace `namespace` on task queue `task_queue`, where
//! the Nexus endpoint routes.

const std = @import("std");
const temporal = @import("temporal");
const service = @import("service.zig");
const nexus = temporal.nexus;
const workflow = temporal.workflow;

pub const namespace = "my-target-namespace";
pub const task_queue = "nexus-handler";

fn helloHandlerWorkflow(ctx: *workflow.Context, input: service.HelloInput) !service.HelloOutput {
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

/// A synchronous operation may do any I/O, such as calling other services through
/// `ctx.client()`; this one returns its input.
fn echo(_: *nexus.Context, input: service.EchoInput) !service.EchoOutput {
    return input;
}

/// Starts `HelloHandlerWorkflow` with a business ID derived from the input. The task queue
/// defaults to the one this operation is handled on.
fn sayHello(ctx: *nexus.Context, input: service.HelloInput) !nexus.WorkflowStart(HelloHandlerWorkflow) {
    return .{ .options = .{ .id = try service.helloWorkflowId(ctx.allocator(), input) }, .args = .{input} };
}

pub const HelloServiceHandler = nexus.ServiceHandler(service.HelloService, .{
    nexus.OperationHandler(service.Echo, echo),
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
