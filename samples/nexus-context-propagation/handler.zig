//! The handler side: the say-hello operation of `service.HelloService` and the workflow
//! behind it, served by a worker in namespace `namespace` on task queue `task_queue`, where
//! the Nexus endpoint routes.
//!
//! The caller's propagated values arrive with the Nexus request: the client's propagators
//! (`connect` sets `context.propagators`) read them from the request's Nexus header, and the
//! workflow the operation starts carries them on, because `sayHello` leaves
//! `options.context` unset. No interceptor and no hand-written header is involved.
//!
//! Like every Nexus header, the propagated values skip the payload codecs and are recorded in
//! the caller's history, so don't propagate secrets this way.

const std = @import("std");
const temporal = @import("temporal");
const context = @import("context.zig");
const service = @import("service.zig");
const nexus = temporal.nexus;
const workflow = temporal.workflow;

pub const namespace = "my-context-target-namespace";
pub const task_queue = "nexus-context-propagation-handler";

/// Greets the input's name in its language. When a value was propagated to the run, the
/// name is followed by ", <key>: <value>".
fn helloHandlerWorkflow(ctx: *workflow.Context, input: service.HelloInput) !service.HelloOutput {
    var name = input.name;
    if (ctx.propagated().get(context.propagate_key)) |values| {
        name = try std.fmt.allocPrint(ctx.allocator(), "{s}, {s}: {s}", .{ name, values.key, values.value });
    }
    const greeting = switch (input.language) {
        .en => "Hello",
        .fr => "Bonjour",
        .de => "Hallo",
        .es => "¡Hola!",
        .tr => "Merhaba",
    };
    return .{ .message = try std.fmt.allocPrint(ctx.allocator(), "{s} {s} 👋", .{ greeting, name }) };
}

pub const HelloHandlerWorkflow = workflow.Def("HelloHandlerWorkflow", helloHandlerWorkflow);

/// Starts `HelloHandlerWorkflow` with a business ID derived from the input. It sets only the
/// ID: the task queue defaults to the one this operation is handled on, and with
/// `options.context` unset the workflow gets the values propagated to this request.
fn sayHello(ctx: *nexus.Context, input: service.HelloInput) !nexus.WorkflowStart(HelloHandlerWorkflow) {
    return .{
        .options = .{ .id = try service.helloWorkflowId(ctx.allocator(), input) },
        .args = .{input},
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

/// Connects a client to `namespace` with the sample's propagators, which its workers use to
/// read the propagated values from Nexus requests. The rest of its options come from the
/// environment and the Temporal config file (`temporal.client.loadDefaultOptions`). Close it
/// with `close`.
pub fn connect(gpa: std.mem.Allocator, io: std.Io, arena: std.mem.Allocator) !temporal.client.Client {
    var options = try temporal.client.loadDefaultOptions(arena);
    options.namespace = namespace;
    options.propagators = &context.propagators;
    return temporal.client.Client.connect(gpa, io, options);
}
