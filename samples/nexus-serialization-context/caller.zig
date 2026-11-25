//! The caller side: a workflow in namespace `service.caller_namespace` that calls both
//! operations of `service.EchoService` through each of the four endpoints. It knows nothing of
//! keys: the caller's codec (`codec.caller`) picks each operation's key by its endpoint.

const std = @import("std");
const temporal = @import("temporal");
const service = @import("service.zig");
const codec = @import("codec.zig");
const workflow = temporal.workflow;

pub const task_queue = service.caller_namespace;

/// Starts all eight operations before it waits for any, so operations of different endpoints,
/// and so of different keys, are in flight together; each keeps its own endpoint's context.
/// Its own input and result are encoded with the caller's default key.
fn callerWorkflow(ctx: *workflow.Context, message: []const u8) ![]const service.EndpointReplies {
    const Pending = struct {
        sync: workflow.NexusOperationFuture(service.EchoOutput),
        async: workflow.NexusOperationFuture(service.EchoOutput),
    };
    var pending: [service.endpoints.len]Pending = undefined;
    for (service.endpoints, &pending) |endpoint, *p| p.* = .{
        .sync = ctx.executeNexusOperation(service.EchoService, service.Echo, .{ .endpoint = endpoint }, .{ .message = message }),
        .async = ctx.executeNexusOperation(service.EchoService, service.EchoAsync, .{ .endpoint = endpoint }, .{ .message = message }),
    };
    const replies = try ctx.allocator().alloc(service.EndpointReplies, pending.len);
    for (service.endpoints, pending, replies) |endpoint, p, *r| r.* = .{
        .endpoint = endpoint,
        .sync_message = (try p.sync.get()).message,
        .async_message = (try p.async.get()).message,
    };
    return replies;
}

pub const CallerWorkflow = workflow.Def("NexusSerializationCallerWorkflow", callerWorkflow);

pub fn register(w: *temporal.worker.Worker) !void {
    try w.registerWorkflow(CallerWorkflow);
}

const codecs = [_]temporal.converter.PayloadCodec{codec.caller.codec()};

/// Connects a client to `service.caller_namespace` with the caller's codec, with the rest of
/// its options from the environment and the Temporal config file
/// (`temporal.client.loadDefaultOptions`). Close it with `close`.
pub fn connect(gpa: std.mem.Allocator, io: std.Io, arena: std.mem.Allocator) !temporal.client.Client {
    var options = try temporal.client.loadDefaultOptions(arena);
    options.namespace = service.caller_namespace;
    options.codecs = &codecs;
    return temporal.client.Client.connect(gpa, io, options);
}
