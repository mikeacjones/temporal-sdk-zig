//! The handler side: the operations of `service.EchoService` and the workflow behind
//! echo-async. Two workers serve them, one in each handler namespace, and differ only in their
//! codec (`codec.compressed_encrypted_handler` or `codec.encrypted_handler`).

const std = @import("std");
const temporal = @import("temporal");
const service = @import("service.zig");
const codec = @import("codec.zig");
const nexus = temporal.nexus;
const workflow = temporal.workflow;

fn echoWorkflow(_: *workflow.Context, input: service.EchoInput) !service.EchoOutput {
    return input;
}

/// Its input is encoded with the handler's default key, as the workflow's own payload; its
/// result with the key of the endpoint the operation came through, whose context the server
/// keeps with the workflow and the SDK restores when the workflow completes.
pub const EchoWorkflow = workflow.Def("EchoWorkflow", echoWorkflow);

/// The handler decodes the input and encodes the output with the context of the endpoint
/// the request came through (`ctx.converterContext()`).
pub fn echo(_: *nexus.Context, input: service.EchoInput) !service.EchoOutput {
    return input;
}

/// Starts `EchoWorkflow` with the ID "nexus-echo-<request ID>". A workflow has one result,
/// which every operation attached to it gets, so each operation that needs its result
/// encrypted with its own endpoint's key needs a workflow of its own; a retry of the same
/// request gets the same ID, and so the same workflow.
pub fn echoAsync(ctx: *nexus.Context, input: service.EchoInput) !nexus.WorkflowStart(EchoWorkflow) {
    const request_id = ctx.info().request_id;
    if (request_id.len == 0) return ctx.handlerError(.{ .type = .bad_request, .message = "the echo-async request has no request ID to derive the workflow ID from" });
    return .{ .options = .{ .id = try std.fmt.allocPrint(ctx.allocator(), "nexus-echo-{s}", .{request_id}) }, .args = .{input} };
}

pub const EchoServiceHandler = nexus.ServiceHandler(service.EchoService, .{
    nexus.OperationHandler(service.Echo, echo),
    nexus.OperationHandler(service.EchoAsync, echoAsync),
});

/// Registers the service and the workflow its echo-async operation starts.
pub fn register(w: *temporal.worker.Worker) !void {
    try w.registerWorkflow(EchoWorkflow);
    try w.registerNexusService(EchoServiceHandler);
}

/// The two handler workers: the same handlers, each in its own namespace with its own codec.
pub const Handler = enum {
    /// Compresses and then encrypts the payloads of its endpoints.
    compressed_encrypted,
    /// Encrypts the payloads of its endpoints without compressing them.
    encrypted,

    /// Also the worker's task queue, where its two endpoints route.
    pub fn namespace(h: Handler) []const u8 {
        return switch (h) {
            .compressed_encrypted => service.compressed_encrypted_namespace,
            .encrypted => service.encrypted_namespace,
        };
    }

    fn codecs(h: Handler) []const temporal.converter.PayloadCodec {
        return switch (h) {
            .compressed_encrypted => &compressed_encrypted_codecs,
            .encrypted => &encrypted_codecs,
        };
    }
};

const compressed_encrypted_codecs = [_]temporal.converter.PayloadCodec{codec.compressed_encrypted_handler.codec()};
const encrypted_codecs = [_]temporal.converter.PayloadCodec{codec.encrypted_handler.codec()};

/// Connects a client to `handler`'s namespace with its codec, with the rest of its options from
/// the environment and the Temporal config file (`temporal.client.loadDefaultOptions`). Close
/// it with `close`.
pub fn connect(gpa: std.mem.Allocator, io: std.Io, arena: std.mem.Allocator, handler: Handler) !temporal.client.Client {
    var options = try temporal.client.loadDefaultOptions(arena);
    options.namespace = handler.namespace();
    options.codecs = handler.codecs();
    return temporal.client.Client.connect(gpa, io, options);
}

/// The main of a program that runs `handler`'s worker on its own until SIGINT or SIGTERM.
pub fn serve(init: std.process.Init, handler: Handler) !void {
    const gpa = init.gpa;
    const io = init.io;

    // The client and worker are heavyweight objects that should be created once per process.
    var client = connect(gpa, io, init.arena.allocator(), handler) catch |err| {
        std.log.err("Unable to create client for namespace {s}: {t}", .{ handler.namespace(), err });
        std.process.exit(1);
    };
    defer client.close();

    var w = try temporal.worker.Worker.init(gpa, io, &client, handler.namespace(), .{});
    defer w.deinit();
    try register(&w);

    w.runUntilSignal() catch |err| {
        std.log.err("Unable to start worker: {t}", .{err});
        std.process.exit(1);
    };
}
