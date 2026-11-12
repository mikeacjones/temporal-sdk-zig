//! Runs the workflow with the Snappy codec, then shows what the codec server is for.
//!
//! The starter plays the CLI's part too: it reads the run's history through a client without
//! the codec, as `temporal workflow show` would, and finds the input compressed. It then
//! starts the codec server (server.zig) in-process and has it decode that input over HTTP,
//! as the CLI does with `--codec-endpoint`.

const std = @import("std");
const temporal = @import("temporal");
const codec_server = @import("codec_server.zig");
const server = @import("server.zig");
const converter = temporal.converter;

const input = "Plain text input";

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;
    const arena = init.arena.allocator();

    // The client is a heavyweight object that should be created once per process. Its
    // codecs make sure workflow inputs and results are encoded as required.
    var options = try temporal.client.loadDefaultOptions(arena);
    const snappy: codec_server.SnappyCodec = .{};
    const codecs = [_]converter.PayloadCodec{snappy.codec()};
    options.codecs = &codecs;
    var client = temporal.client.Client.connect(gpa, io, options) catch |err| {
        std.log.err("Unable to create client: {t}", .{err});
        std.process.exit(1);
    };
    defer client.close();

    // The input is encoded by the codec before it is sent to Temporal.
    var run = client.executeWorkflow(codec_server.Workflow, .{
        .id = codec_server.workflow_id,
        .task_queue = codec_server.task_queue,
    }, .{input}) catch |err| {
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

    // Without the codec, the history's payloads stay compressed.
    var plain_client = temporal.client.Client.connect(gpa, io, try temporal.client.loadDefaultOptions(arena)) catch |err| {
        std.log.err("Unable to create a client without codecs: {t}", .{err});
        std.process.exit(1);
    };
    defer plain_client.close();
    const history = plain_client.fetchHistory(arena, run.id, run.run_id) catch |err| {
        std.log.err("Unable to fetch the history of {s}: {t}", .{ run.id, err });
        std.process.exit(1);
    };
    const started = if (history.events.items.len > 0) history.events.items[0].attributes else null;
    const encoded_input = if (started) |attributes| switch (attributes) {
        .workflow_execution_started_event_attributes => |a| if (a.input) |in| in.payloads.items else &.{},
        else => &.{},
    } else &.{};
    if (encoded_input.len != 1) {
        std.log.err("The history of {s} has no workflow input to decode", .{run.id});
        std.process.exit(1);
    }
    std.log.info("Without the codec, the workflow input has encoding {s}", .{converter.encodingOf(encoded_input[0]) orelse "(none)"});

    // Decode it with the codec server, as the CLI does with --codec-endpoint.
    const namespaces = [_]server.Namespace{.{ .name = client.namespace, .codecs = &codecs }};
    const codec_service: server.CodecServer = .{ .gpa = gpa, .io = io, .namespaces = &namespaces };
    const address = try std.Io.net.IpAddress.parse("127.0.0.1", 0);
    var listener = address.listen(io, .{}) catch |err| {
        std.log.err("Unable to start the codec server: {t}", .{err});
        std.process.exit(1);
    };
    defer listener.deinit(io);
    var serving = io.concurrent(server.CodecServer.serve, .{ &codec_service, &listener }) catch |err| {
        std.log.err("Unable to start the codec server: {t}", .{err});
        std.process.exit(1);
    };
    defer serving.cancel(io) catch {};

    var http: std.http.Client = .{ .allocator = gpa, .io = io };
    defer http.deinit();
    const url = try std.fmt.allocPrint(arena, "http://127.0.0.1:{d}/{s}/decode", .{ listener.socket.address.getPort(), client.namespace });
    const decoded = server.postPayloads(&http, arena, url, "", encoded_input) catch |err| {
        std.log.err("Unable to decode the workflow input with the codec server at {s}: {t}", .{ url, err });
        std.process.exit(1);
    };
    const decoded_input = converter.fromPayload([]const u8, arena, decoded[0]) catch |err| {
        std.log.err("The codec server's answer is not the workflow input: {t}", .{err});
        std.process.exit(1);
    };
    if (!std.mem.eql(u8, decoded_input, input)) {
        std.log.err("The codec server decoded \"{s}\", not the workflow input \"{s}\"", .{ decoded_input, input });
        std.process.exit(1);
    }
    std.log.info("Codec server decoded the workflow input: {s}", .{decoded_input});
}
