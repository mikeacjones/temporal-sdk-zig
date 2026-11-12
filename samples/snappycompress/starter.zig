//! Runs the workflow with a client that compresses its input, then reads the input back from
//! the workflow's history with a client that has no codec, which sees only compressed bytes
//! (as the Temporal CLI does without the codec server, `codec_server.zig`), and decodes it
//! with the codec.

const std = @import("std");
const temporal = @import("temporal");
const snappycompress = @import("snappycompress.zig");
const codec = @import("codec.zig");

const workflow_id = "snappycompress_workflowID";

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;
    const arena = init.arena.allocator();

    // The client is a heavyweight object that should be created once per process. Its codec
    // compresses the workflow input and decompresses the result.
    const codecs = [_]temporal.converter.PayloadCodec{codec.always_compress.codec()};
    const plain_options = try temporal.client.loadDefaultOptions(arena);
    var options = plain_options;
    options.codecs = &codecs;
    var client = temporal.client.Client.connect(gpa, io, options) catch |err| {
        std.log.err("Unable to create client: {t}", .{err});
        std.process.exit(1);
    };
    defer client.close();

    // The input "My Compressed Friend" is compressed before it is sent to Temporal.
    var run = client.executeWorkflow(snappycompress.Workflow, .{
        .id = workflow_id,
        .task_queue = snappycompress.task_queue,
    }, .{"My Compressed Friend"}) catch |err| {
        std.log.err("Unable to execute workflow: {t}", .{err});
        std.process.exit(1);
    };
    defer run.deinit();

    std.log.info("Started workflow WorkflowID={s} RunID={s}", .{ run.id, run.run_id });

    // Synchronously wait for the workflow completion.
    const result = run.get(arena) catch |err| {
        std.log.err("Unable get workflow result: {t}", .{err});
        std.process.exit(1);
    };
    std.log.info("Workflow result: {s}", .{result});

    var plain_client = temporal.client.Client.connect(gpa, io, plain_options) catch |err| {
        std.log.err("Unable to create client without the codec: {t}", .{err});
        std.process.exit(1);
    };
    defer plain_client.close();
    const history = plain_client.fetchHistory(arena, run.id, run.run_id) catch |err| {
        std.log.err("Unable to fetch the history of workflow {s}: {t}", .{ run.id, err });
        std.process.exit(1);
    };
    const started = history.events.items[0].attributes.?.workflow_execution_started_event_attributes;
    const stored = started.input.?.payloads.items;
    std.log.info("Workflow input as stored: encoding {s}, {d} bytes", .{ temporal.converter.encodingOf(stored[0]) orelse "(none)", stored[0].data.len });

    const decoded = temporal.converter.decodePayloads(&codecs, arena, io, stored) catch |err| {
        std.log.err("Unable to decode the stored workflow input: {t}", .{err});
        std.process.exit(1);
    };
    const name = temporal.converter.fromPayload([]const u8, arena, decoded[0]) catch |err| {
        std.log.err("Unable to read the decoded workflow input as a string: {t}", .{err});
        std.process.exit(1);
    };
    std.log.info("Workflow input decoded with the codec: {s}", .{name});
}
