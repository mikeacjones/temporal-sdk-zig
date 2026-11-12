//! Runs the workflow with a client that encrypts its input with the key ID "test", then
//! reads the input back from the workflow's history with a client that has no codecs, which
//! sees only encrypted bytes (as the Temporal CLI does without the codec server,
//! `codec_server.zig`), and decodes it with the codecs as the codec server does.

const std = @import("std");
const temporal = @import("temporal");
const encryption = @import("encryption.zig");
const codec = @import("codec.zig");

const workflow_id = "encryption_workflowID";

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;
    const arena = init.arena.allocator();

    // The client is a heavyweight object that should be created once per process. Its codecs
    // encrypt the workflow input and decrypt the result; the propagator sends the key ID
    // with the workflow so its worker encrypts with the same key.
    const propagators = [_]temporal.propagation.ContextPropagator{encryption.propagator};
    const plain_options = try temporal.client.loadDefaultOptions(arena);
    var options = plain_options;
    options.codecs = &codec.codecs;
    options.propagators = &propagators;
    var client = temporal.client.Client.connect(gpa, io, options) catch |err| {
        std.log.err("Unable to create client: {t}", .{err});
        std.process.exit(1);
    };
    defer client.close();

    // The key this workflow's payloads are encrypted with.
    var values: temporal.propagation.Values = .init(arena);
    try values.put(encryption.propagate_key, .{ .key_id = "test" });

    // The input "My Secret Friend" is encrypted before it is sent to Temporal.
    var run = client.executeWorkflow(encryption.Workflow, .{
        .id = workflow_id,
        .task_queue = encryption.task_queue,
        .context = &values,
    }, .{"My Secret Friend"}) catch |err| {
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
        std.log.err("Unable to create client without the codecs: {t}", .{err});
        std.process.exit(1);
    };
    defer plain_client.close();
    const history = plain_client.fetchHistory(arena, run.id, run.run_id) catch |err| {
        std.log.err("Unable to fetch the history of workflow {s}: {t}", .{ run.id, err });
        std.process.exit(1);
    };
    const started = history.events.items[0].attributes.?.workflow_execution_started_event_attributes;
    const stored = started.input.?.payloads.items;
    std.log.info("Workflow input as stored: encoding {s}, key ID {s}", .{
        temporal.converter.encodingOf(stored[0]) orelse "(none)",
        codec.keyIdOf(stored[0]) orelse "(none)",
    });

    // Without a context, as the codec server decodes: the payload names its key.
    const decoded = temporal.converter.decodePayloads(&codec.codecs, arena, io, stored) catch |err| {
        std.log.err("Unable to decode the stored workflow input: {t}", .{err});
        std.process.exit(1);
    };
    const name = temporal.converter.fromPayload([]const u8, arena, decoded[0]) catch |err| {
        std.log.err("Unable to read the decoded workflow input as a string: {t}", .{err});
        std.process.exit(1);
    };
    std.log.info("Workflow input decoded with the codecs: {s}", .{name});
}
