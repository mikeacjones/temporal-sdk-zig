//! A test of the caller workflow against the service's handlers, served by the test
//! environment's worker through four endpoints that route to it, on a dev server that keeps a
//! Nexus caller's context with the workflows its handlers start. The environment's client
//! has the caller's codec, which knows every endpoint's key.

const std = @import("std");
const temporal = @import("temporal");
const service = @import("service.zig");
const handler = @import("handler.zig");
const caller = @import("caller.zig");
const codec = @import("codec.zig");

const testing = std.testing;
const nexus = temporal.nexus;
const WorkflowEnvironment = temporal.testing.WorkflowEnvironment;

/// Prefixes the message with the endpoint and the operation it came through, so each of the
/// eight operations returns a reply of its own, then hands it to the real handler. The
/// asynchronous one still completes through the workflow it starts.
fn tagged(ctx: *nexus.Context, input: service.EchoInput, operation: []const u8) !service.EchoInput {
    return .{ .message = try std.fmt.allocPrint(ctx.allocator(), "{s}/{s}: {s}", .{ ctx.info().endpoint, operation, input.message }) };
}

fn taggedEcho(ctx: *nexus.Context, input: service.EchoInput) !service.EchoOutput {
    return handler.echo(ctx, try tagged(ctx, input, service.Echo.operation_name));
}

fn taggedEchoAsync(ctx: *nexus.Context, input: service.EchoInput) !nexus.WorkflowStart(handler.EchoWorkflow) {
    return handler.echoAsync(ctx, try tagged(ctx, input, service.EchoAsync.operation_name));
}

const TaggedEchoService = nexus.ServiceHandler(service.EchoService, .{
    nexus.OperationHandler(service.Echo, taggedEcho),
    nexus.OperationHandler(service.EchoAsync, taggedEchoAsync),
});

const codecs = [_]temporal.converter.PayloadCodec{codec.caller.codec()};

test "the caller workflow gets every operation's reply through every endpoint, each encrypted with the endpoint's key" {
    const env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{
        .codecs = &codecs,
        .dev_server = .{ .cli_version = temporal.testing.DevServer.nexus_context_cli_version },
    });
    defer env.deinit();
    try caller.register(&env.worker);
    try env.registerWorkflow(handler.EchoWorkflow);
    try env.registerNexusService(TaggedEchoService);
    for (service.endpoints) |endpoint| try env.createNexusEndpoint(endpoint, .{});

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var run = try env.startWorkflow(caller.CallerWorkflow, .{}, .{"Hello from Nexus"});
    defer run.deinit();
    // `getResult` also replays the caller's history.
    const replies = try env.getResult(caller.CallerWorkflow, &run, a, null);

    // The replies are all distinct, so each endpoint and operation ran.
    try testing.expectEqual(service.endpoints.len, replies.len);
    for (service.endpoints, replies) |endpoint, r| {
        try testing.expectEqualStrings(endpoint, r.endpoint);
        try testing.expectEqualStrings(try std.fmt.allocPrint(a, "{s}/echo: Hello from Nexus", .{endpoint}), r.sync_message);
        try testing.expectEqualStrings(try std.fmt.allocPrint(a, "{s}/echo-async: Hello from Nexus", .{endpoint}), r.async_message);
    }

    // As stored, each operation's input and result name its endpoint and that endpoint's key:
    // the asynchronous results too, which the workflows the handlers started encoded. A
    // client without the codec reads the history as the server holds it.
    var options = env.server.dev.clientOptions();
    options.namespace = env.client.namespace;
    var plain = try temporal.client.Client.connect(testing.allocator, testing.io, options);
    defer plain.close();
    const history = try plain.fetchHistory(a, run.id, run.run_id);
    var scheduled: std.AutoHashMapUnmanaged(i64, []const u8) = .empty;
    var inputs: usize = 0;
    var results: usize = 0;
    var started: std.ArrayList([]const u8) = .empty;
    for (history.events.items) |event| switch (event.attributes orelse continue) {
        .nexus_operation_scheduled_event_attributes => |s| {
            try scheduled.put(a, event.event_id, s.endpoint);
            try expectStoredFor(s.endpoint, s.input.?);
            inputs += 1;
        },
        .nexus_operation_completed_event_attributes => |c| {
            try expectStoredFor(scheduled.get(c.scheduled_event_id).?, c.result.?);
            results += 1;
        },
        .nexus_operation_started_event_attributes => for (event.links.items) |link| switch (link.variant orelse continue) {
            .workflow_event => |target| try started.append(a, target.workflow_id),
            else => {},
        },
        else => {},
    };
    try testing.expectEqual(8, inputs);
    try testing.expectEqual(8, results);

    // The workflows behind the asynchronous operations, one per operation, replay too.
    try testing.expectEqual(service.endpoints.len, started.items.len);
    for (started.items) |id| {
        try testing.expectStringStartsWith(id, "nexus-echo-");
        try env.replayRun(id, "");
    }
}

fn expectStoredFor(endpoint: []const u8, p: temporal.converter.Payload) !void {
    try testing.expectEqualStrings(codec.encoding, temporal.converter.encodingOf(p).?);
    try testing.expectEqualStrings(endpoint, codec.metadataValue(p, codec.endpoint_metadata).?);
    const key_id = codec.metadataValue(p, codec.key_id_metadata).?;
    for (codec.caller.endpoints) |e| if (std.mem.eql(u8, e.endpoint, endpoint)) return testing.expectEqualStrings(e.key.id, key_id);
    return error.TestUnknownEndpoint;
}
