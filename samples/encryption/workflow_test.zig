//! Tests for the encryption workflow and activity, run with the encrypting codecs and the
//! key ID propagator.

const std = @import("std");
const temporal = @import("temporal");
const encryption = @import("encryption.zig");
const codec = @import("codec.zig");
const converter = temporal.converter;

const testing = std.testing;
const propagators = [_]temporal.propagation.ContextPropagator{encryption.propagator};

fn activityDouble(ctx: *temporal.activity.Context, info: encryption.Info) ![]const u8 {
    try testing.expectEqualStrings("Temporal", info.map.get("name").?);
    try testing.expectEqualStrings("test", ctx.propagated().get(encryption.propagate_key).?.key_id);
    return ctx.allocator().dupe(u8, "Hello Temporal!");
}

test "workflow with a test double for its activity" {
    const env = try temporal.testing.WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{
        .codecs = &codec.codecs,
        .propagators = &propagators,
    });
    defer env.deinit();
    try env.registerWorkflow(encryption.Workflow);
    try env.registerActivityAs(encryption.Activity, activityDouble);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var values: temporal.propagation.Values = .init(a);
    try values.put(encryption.propagate_key, .{ .key_id = "test" });
    const result = try env.executeWorkflowWithOptions(encryption.Workflow, .{ .task_queue = "", .context = &values }, .{"Temporal"}, a, null);
    try testing.expectEqualStrings("Hello Temporal!", result);
}

test "activity greets the name it is given, or someone" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const env: temporal.testing.ActivityEnvironment = .init(testing.io);

    var info: encryption.Info = .{};
    try info.map.put(a, "name", "World");
    try testing.expectEqualStrings("Hello World!", try env.executeActivity(encryption.Activity, .{info}, a));
    try testing.expectEqualStrings("Hello someone!", try env.executeActivity(encryption.Activity, .{.{}}, a));
}

/// Records the encoding and key ID of every payload it visits, leaving them unchanged.
const PayloadCollector = struct {
    arena: std.mem.Allocator,
    seen: *std.ArrayList([2][]const u8),

    pub fn visit(v: PayloadCollector, payloads: []const converter.Payload) converter.CodecError![]converter.Payload {
        for (payloads) |p| try v.seen.append(v.arena, .{ converter.encodingOf(p) orelse "", codec.keyIdOf(p) orelse "" });
        return v.arena.dupe(converter.Payload, payloads);
    }
};

fn runWorker(w: *temporal.worker.Worker) temporal.worker.Worker.RunError!void {
    return w.run();
}

test "every payload in the workflow's history is encrypted with the propagated key" {
    var server = try temporal.testing.DevServer.start(testing.allocator, testing.io, .{});
    defer server.stop(testing.io);

    var options = server.clientOptions();
    options.codecs = &codec.codecs;
    options.propagators = &propagators;
    var client = try temporal.client.Client.connect(testing.allocator, testing.io, options);
    defer client.close();

    var w = try temporal.worker.Worker.init(testing.allocator, testing.io, &client, encryption.task_queue, .{});
    defer w.deinit();
    try w.registerWorkflow(encryption.Workflow);
    try w.registerActivity(encryption.Activity);
    var running = try testing.io.concurrent(runWorker, .{&w});
    defer {
        w.stop();
        running.await(testing.io) catch {};
    }

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var values: temporal.propagation.Values = .init(a);
    try values.put(encryption.propagate_key, .{ .key_id = "test" });
    var run = try client.executeWorkflow(encryption.Workflow, .{ .id = "encryption-test", .task_queue = encryption.task_queue, .context = &values }, .{"My Secret Friend"});
    defer run.deinit();
    try testing.expectEqualStrings("Hello My Secret Friend!", try run.get(a));

    // A client without the codecs sees the history as the server stores it.
    var plain_client = try temporal.client.Client.connect(testing.allocator, testing.io, server.clientOptions());
    defer plain_client.close();
    var history = try plain_client.fetchHistory(a, run.id, run.run_id);
    var seen: std.ArrayList([2][]const u8) = .empty;
    try converter.visitPayloads(temporal.client.History, &history, a, PayloadCollector{ .arena = a, .seen = &seen });
    // The workflow's input, result, and header, and the activity's input, result, and header.
    try testing.expectEqual(@as(usize, 6), seen.items.len);
    for (seen.items) |s| {
        try testing.expectEqualStrings("binary/encrypted", s[0]);
        try testing.expectEqualStrings("test", s[1]);
    }

    // The client with the codecs reads the same history decrypted.
    const decoded = try client.fetchHistory(a, run.id, run.run_id);
    const started = decoded.events.items[0].attributes.?.workflow_execution_started_event_attributes;
    try testing.expectEqualStrings("My Secret Friend", try converter.fromPayload([]const u8, a, started.input.?.payloads.items[0]));
}
