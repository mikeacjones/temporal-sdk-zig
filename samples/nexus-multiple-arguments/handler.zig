//! The handler side: the say-hello operation of `service.HelloService` and the workflow
//! behind it, served by a worker in namespace `namespace` on task queue `task_queue`, where
//! the Nexus endpoint routes.
//!
//! An operation has one input, but the workflow it starts takes two arguments, a name and a
//! language. The handler maps the one onto the other: it returns the workflow's arguments as
//! a tuple built from the input's fields.

const std = @import("std");
const temporal = @import("temporal");
const service = @import("service.zig");
const nexus = temporal.nexus;
const workflow = temporal.workflow;

pub const namespace = "my-multiple-arguments-target-namespace";
pub const task_queue = "nexus-multiple-arguments-handler";

/// Greets `name` in `language`. It takes the two as separate arguments, not the operation's
/// input struct.
fn helloHandlerWorkflow(ctx: *workflow.Context, name: []const u8, language: service.Language) !service.HelloOutput {
    const greeting = switch (language) {
        .en => "Hello",
        .fr => "Bonjour",
        .de => "Hallo",
        .es => "¡Hola!",
        .tr => "Merhaba",
    };
    return .{ .message = try std.fmt.allocPrint(ctx.allocator(), "{s} {s} 👋", .{ greeting, name }) };
}

pub const HelloHandlerWorkflow = workflow.Def("HelloHandlerWorkflow", helloHandlerWorkflow);

/// Starts `HelloHandlerWorkflow` with a business ID derived from the input, mapping the
/// operation's one input onto the workflow's two arguments: `.args` is the tuple of the
/// workflow's arguments, `.{ input.name, input.language }`. The task queue defaults to the
/// one this operation is handled on.
fn sayHello(ctx: *nexus.Context, input: service.HelloInput) !nexus.WorkflowStart(HelloHandlerWorkflow) {
    return .{
        .options = .{ .id = try service.helloWorkflowId(ctx.allocator(), input) },
        .args = .{ input.name, input.language },
    };
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
