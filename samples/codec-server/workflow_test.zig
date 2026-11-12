//! Tests for the codec server workflow, run with the Snappy codec.

const std = @import("std");
const temporal = @import("temporal");
const codec_server = @import("codec_server.zig");
const converter = temporal.converter;

const testing = std.testing;

test "the workflow runs with compressed payloads, which the server stores compressed" {
    const snappy: codec_server.SnappyCodec = .{};
    const codecs = [_]converter.PayloadCodec{snappy.codec()};
    const env = try temporal.testing.WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{ .codecs = &codecs });
    defer env.deinit();
    try env.registerWorkflow(codec_server.Workflow);
    try env.registerActivity(codec_server.Activity);

    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    var run = try env.startWorkflow(codec_server.Workflow, .{ .task_queue = "" }, .{"Plain text input"});
    defer run.deinit();
    // Also replays the history with the codec.
    try testing.expectEqualStrings("Received Plain text input", try env.getResult(codec_server.Workflow, &run, arena, null));

    // A client without the codec, as the CLI without a codec server, sees every payload the
    // workflow recorded compressed: the input, the side effect's value, the local activity's
    // result, the activity's input and result, and the workflow's result.
    var plain = try temporal.client.Client.connect(testing.allocator, testing.io, env.server.dev.clientOptions());
    defer plain.close();
    const history = try plain.fetchHistory(arena, run.id, run.run_id);
    var compressed: usize = 0;
    for (history.events.items) |event| {
        const payloads: []const converter.Payload = switch (event.attributes.?) {
            .workflow_execution_started_event_attributes => |a| a.input.?.payloads.items,
            .activity_task_scheduled_event_attributes => |a| a.input.?.payloads.items,
            .activity_task_completed_event_attributes => |a| a.result.?.payloads.items,
            .workflow_execution_completed_event_attributes => |a| a.result.?.payloads.items,
            // A local activity's marker (the side effect's too) holds its result, plus the
            // worker's own bookkeeping under "data", which no codec sees.
            .marker_recorded_event_attributes => |a| for (a.details.items) |entry| {
                if (std.mem.eql(u8, entry.key, "result")) break entry.value.?.payloads.items;
            } else &.{},
            else => &.{},
        };
        for (payloads) |p| {
            try testing.expectEqualStrings("binary/snappy", converter.encodingOf(p).?);
            compressed += 1;
        }
    }
    try testing.expectEqual(@as(usize, 6), compressed);
    try testing.expectError(error.UnknownEncoding, converter.fromPayload([]const u8, arena, history.events.items[0].attributes.?.workflow_execution_started_event_attributes.input.?.payloads.items[0]));
}

test "the activity greets its input" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const env: temporal.testing.ActivityEnvironment = .init(testing.io);
    try testing.expectEqualStrings("Received Plain text input", try env.executeActivity(codec_server.Activity, .{"Plain text input"}, arena.allocator()));
}
