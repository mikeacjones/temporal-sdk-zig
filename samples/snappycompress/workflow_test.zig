//! Tests for the snappycompress workflow and activity, run with the compressing codec.

const std = @import("std");
const temporal = @import("temporal");
const snappycompress = @import("snappycompress.zig");
const codec = @import("codec.zig");
const converter = temporal.converter;

const testing = std.testing;
const codecs = [_]converter.PayloadCodec{codec.always_compress.codec()};

fn activityDouble(ctx: *temporal.activity.Context, info: snappycompress.Info) ![]const u8 {
    try testing.expectEqualStrings("Temporal", info.map.get("name").?);
    return ctx.allocator().dupe(u8, "Hello Temporal!");
}

test "workflow with a test double for its activity" {
    const env = try temporal.testing.WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{ .codecs = &codecs });
    defer env.deinit();
    try env.registerWorkflow(snappycompress.Workflow);
    try env.registerActivityAs(snappycompress.Activity, activityDouble);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const result = try env.executeWorkflow(snappycompress.Workflow, .{"Temporal"}, arena.allocator(), null);
    try testing.expectEqualStrings("Hello Temporal!", result);
}

test "activity greets the name it is given, or someone" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const env: temporal.testing.ActivityEnvironment = .init(testing.io);

    var info: snappycompress.Info = .{};
    try info.map.put(a, "name", "World");
    try testing.expectEqualStrings("Hello World!", try env.executeActivity(snappycompress.Activity, .{info}, a));
    try testing.expectEqualStrings("Hello someone!", try env.executeActivity(snappycompress.Activity, .{.{}}, a));
}

/// Records the encoding of every payload it visits, leaving them unchanged.
const EncodingCollector = struct {
    arena: std.mem.Allocator,
    encodings: *std.ArrayList([]const u8),

    pub fn visit(v: EncodingCollector, payloads: []const converter.Payload) converter.CodecError![]converter.Payload {
        for (payloads) |p| try v.encodings.append(v.arena, converter.encodingOf(p) orelse "");
        return v.arena.dupe(converter.Payload, payloads);
    }
};

fn runWorker(w: *temporal.worker.Worker) temporal.worker.Worker.RunError!void {
    return w.run();
}

test "every payload in the workflow's history is compressed" {
    var server = try temporal.testing.DevServer.start(testing.allocator, testing.io, .{});
    defer server.stop(testing.io);

    var options = server.clientOptions();
    options.codecs = &codecs;
    var client = try temporal.client.Client.connect(testing.allocator, testing.io, options);
    defer client.close();

    var w = try temporal.worker.Worker.init(testing.allocator, testing.io, &client, snappycompress.task_queue, .{});
    defer w.deinit();
    try w.registerWorkflow(snappycompress.Workflow);
    try w.registerActivity(snappycompress.Activity);
    var running = try testing.io.concurrent(runWorker, .{&w});
    defer {
        w.stop();
        running.await(testing.io) catch {};
    }

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var run = try client.executeWorkflow(snappycompress.Workflow, .{ .id = "snappycompress-test", .task_queue = snappycompress.task_queue }, .{"My Compressed Friend"});
    defer run.deinit();
    try testing.expectEqualStrings("Hello My Compressed Friend!", try run.get(a));

    // A client without the codec sees the history as the server stores it.
    var plain_client = try temporal.client.Client.connect(testing.allocator, testing.io, server.clientOptions());
    defer plain_client.close();
    var history = try plain_client.fetchHistory(a, run.id, run.run_id);
    var encodings: std.ArrayList([]const u8) = .empty;
    try converter.visitPayloads(temporal.client.History, &history, a, EncodingCollector{ .arena = a, .encodings = &encodings });
    // The workflow's input and result and the activity's input and result.
    try testing.expectEqual(@as(usize, 4), encodings.items.len);
    for (encodings.items) |encoding| try testing.expectEqualStrings("binary/snappy", encoding);

    // The client with the codec reads the same history decompressed.
    const decoded = try client.fetchHistory(a, run.id, run.run_id);
    const started = decoded.events.items[0].attributes.?.workflow_execution_started_event_attributes;
    try testing.expectEqualStrings("My Compressed Friend", try converter.fromPayload([]const u8, a, started.input.?.payloads.items[0]));
}
