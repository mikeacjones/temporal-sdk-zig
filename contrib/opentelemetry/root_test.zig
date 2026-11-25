//! The OpenTelemetry tracer against a local OTLP HTTP receiver: spans started and finished
//! through `temporal.tracing.Tracer` arrive over HTTP/protobuf with their names, kinds,
//! attributes, parent and status.

const std = @import("std");
const temporal = @import("temporal");
const otel = @import("opentelemetry-sdk");
const proto = @import("opentelemetry-proto");
const root = @import("root.zig");
const tracing = temporal.tracing;
const testing = std.testing;
const Io = std.Io;

/// Accepts `POST /v1/traces` on 127.0.0.1 at a port of its own, decodes each body as an
/// `ExportTraceServiceRequest`, keeps its spans and answers 200.
const Receiver = struct {
    io: Io,
    server: Io.net.Server,
    arena: std.heap.ArenaAllocator,
    mutex: Io.Mutex = .init,
    spans: std.ArrayList(proto.trace_v1.Span) = .empty,
    /// Requests that were not a decodable `POST /v1/traces`.
    rejected: u32 = 0,
    stopping: std.atomic.Value(bool) = .init(false),
    task: ?Io.Future(void) = null,

    fn start(r: *Receiver, io: Io) !void {
        const address = try Io.net.IpAddress.parse("127.0.0.1", 0);
        r.* = .{ .io = io, .server = try address.listen(io, .{}), .arena = .init(testing.allocator) };
        r.task = try io.concurrent(serve, .{r});
    }

    fn port(r: *const Receiver) u16 {
        return r.server.socket.address.getPort();
    }

    /// Stops the accept loop by connecting once after setting `stopping`, then frees all.
    fn stop(r: *Receiver) void {
        r.stopping.store(true, .release);
        if (r.task) |*task| {
            if (r.server.socket.address.connect(r.io, .{ .mode = .stream })) |s| {
                s.close(r.io);
            } else |_| {}
            task.await(r.io);
        }
        r.server.deinit(r.io);
        r.arena.deinit();
    }

    fn serve(r: *Receiver) void {
        while (!r.stopping.load(.acquire)) {
            const stream = r.server.accept(r.io) catch return;
            defer stream.close(r.io);
            if (r.stopping.load(.acquire)) return;
            r.handle(stream) catch {};
        }
    }

    fn handle(r: *Receiver, stream: Io.net.Stream) !void {
        var read_buffer: [8192]u8 = undefined;
        var write_buffer: [1024]u8 = undefined;
        var reader = stream.reader(r.io, &read_buffer);
        var writer = stream.writer(r.io, &write_buffer);
        var server = std.http.Server.init(&reader.interface, &writer.interface);
        // One request per connection, closed after the answer, so the accept loop never
        // waits on an idle connection and `stop` always reaches it.
        var request = server.receiveHead() catch return;
        const ok = request.head.method == .POST and std.mem.eql(u8, request.head.target, "/v1/traces");
        var body_buffer: [8192]u8 = undefined;
        const body = try (try request.readerExpectContinue(&body_buffer)).allocRemaining(testing.allocator, .unlimited);
        defer testing.allocator.free(body);
        if (!ok or !r.keep(body)) {
            r.mutex.lockUncancelable(r.io);
            r.rejected += 1;
            r.mutex.unlock(r.io);
            return request.respond("", .{ .status = .bad_request, .keep_alive = false });
        }
        try request.respond("", .{ .status = .ok, .keep_alive = false });
    }

    fn keep(r: *Receiver, body: []const u8) bool {
        r.mutex.lockUncancelable(r.io);
        defer r.mutex.unlock(r.io);
        const a = r.arena.allocator();
        var body_reader: Io.Reader = .fixed(body);
        const message = proto.collector_trace_v1.ExportTraceServiceRequest.decode(&body_reader, a) catch return false;
        for (message.resource_spans.items) |rs| for (rs.scope_spans.items) |ss| {
            r.spans.appendSlice(a, ss.spans.items) catch return false;
        };
        return true;
    }

    fn find(r: *Receiver, name: []const u8) ?proto.trace_v1.Span {
        r.mutex.lockUncancelable(r.io);
        defer r.mutex.unlock(r.io);
        for (r.spans.items) |s| if (std.mem.eql(u8, s.name, name)) return s;
        return null;
    }
};

fn attribute(span: proto.trace_v1.Span, key: []const u8) ?[]const u8 {
    for (span.attributes.items) |kv| if (std.mem.eql(u8, kv.key, key)) {
        const v = (kv.value orelse return null).value orelse return null;
        return switch (v) {
            .string_value => |s| s,
            else => null,
        };
    };
    return null;
}

test "spans started through temporal.tracing.Tracer arrive at an OTLP HTTP receiver with their parent, kind, attributes and status" {
    const io = testing.io;
    const gpa = testing.allocator;
    var receiver: Receiver = undefined;
    try receiver.start(io);
    defer receiver.stop();

    var env_map: std.process.Environ.Map = .init(gpa);
    defer env_map.deinit();
    const config = try otel.otlp.ConfigOptions.init(gpa, &env_map);
    defer config.deinit();
    var endpoint_buffer: [32]u8 = undefined;
    config.endpoint = try std.fmt.bufPrint(&endpoint_buffer, "127.0.0.1:{d}", .{receiver.port()});
    try testing.expectEqual(otel.otlp.Protocol.http_protobuf, config.protocol);

    const exporter = try otel.trace.OTLPExporter.init(gpa, io, config);
    defer exporter.deinit();
    const processor = try otel.trace.BatchingProcessor.init(gpa, io, exporter.asSpanExporter(), .{});
    defer processor.deinit();
    var prng: std.Random.DefaultPrng = .init(0x7e57);
    const provider = try otel.trace.TracerProvider.init(gpa, io, .{ .Random = .init(prng.random()) });
    defer provider.deinit();
    try provider.addSpanProcessor(processor.asSpanProcessor());

    var t: root.Tracer = .init(gpa, io, try provider.getTracer(.{ .name = "temporal-test" }));
    defer t.deinit();
    const tracer = t.tracer();

    var arena: std.heap.ArenaAllocator = .init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    {
        // The name and tag values are copies the caller frees right away.
        const workflow_id = try gpa.dupe(u8, "order-1");
        defer gpa.free(workflow_id);
        const client = try tracer.startSpan(a, .{
            .operation = .start_workflow,
            .name = "Order",
            .tags = &.{.{ .name = "temporalWorkflowID", .value = workflow_id }},
        });
        const traceparent = client.context.get("traceparent").?;
        try testing.expectEqual(@as(usize, 55), traceparent.len);
        try testing.expectEqual(@as(?[]const u8, null), client.context.get("tracestate"));

        const server = try tracer.startSpan(a, .{
            .operation = .run_workflow,
            .name = "Order",
            .parent = client.context,
            .start_time = .fromNanoseconds(1_700_000_000 * std.time.ns_per_s),
            .tags = &.{ .{ .name = "temporalWorkflowID", .value = workflow_id }, .{ .name = "temporalRunID", .value = "run-1" } },
        });
        // The child shares the trace ID.
        try testing.expectEqualStrings(traceparent[3..35], server.context.get("traceparent").?[3..35]);
        const failure = try gpa.dupe(u8, "PaymentDeclined");
        defer gpa.free(failure);
        tracer.finishSpan(server, .{ .failure = failure });
        tracer.finishSpan(client, .{});
        // A span the tracer could not start, finished, does nothing.
        tracer.finishSpan(.{}, .{ .failure = "ignored" });

        try provider.forceFlush();

        const sent = receiver.find("StartWorkflow:Order") orelse return error.TestSpanNotReceived;
        const ran = receiver.find("RunWorkflow:Order") orelse return error.TestSpanNotReceived;
        try testing.expectEqual(proto.trace_v1.Span.SpanKind.SPAN_KIND_CLIENT, sent.kind);
        try testing.expectEqual(proto.trace_v1.Span.SpanKind.SPAN_KIND_SERVER, ran.kind);
        try testing.expectEqualStrings("order-1", attribute(sent, "temporalWorkflowID").?);
        try testing.expectEqualStrings("run-1", attribute(ran, "temporalRunID").?);
        try testing.expectEqual(@as(usize, 0), sent.parent_span_id.len);
        try testing.expectEqualSlices(u8, sent.span_id, ran.parent_span_id);
        try testing.expectEqualSlices(u8, sent.trace_id, ran.trace_id);
        var hex: [16]u8 = undefined;
        try testing.expectEqualStrings(traceparent[36..52], std.fmt.bufPrint(&hex, "{x}", .{sent.span_id}) catch unreachable);
        try testing.expectEqual(@as(u64, 1_700_000_000 * std.time.ns_per_s), ran.start_time_unix_nano);
        try testing.expectEqual(proto.trace_v1.Status.StatusCode.STATUS_CODE_ERROR, ran.status.?.code);
        try testing.expectEqualStrings("PaymentDeclined", ran.status.?.message);
        try testing.expect(sent.status == null or sent.status.?.code != .STATUS_CODE_ERROR);
        try testing.expectEqual(@as(u32, 0), receiver.rejected);
    }
    provider.shutdown();
}

test "a traceparent round-trips through spanContext and fromSpanContext" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const fields = [_]tracing.Field{
        .{ .name = "TraceParent", .value = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01" },
        .{ .name = "tracestate", .value = "vendor=v1,other=v2" },
    };
    const sc = (try root.spanContext(a, .{ .fields = &fields })).?;
    try testing.expect(sc.isRemote());
    try testing.expect(sc.trace_flags.isSampled());
    const back = try root.fromSpanContext(a, sc);
    try testing.expectEqualStrings(fields[0].value, back.get("traceparent").?);
    try testing.expectEqualStrings(fields[1].value, back.get("tracestate").?);

    // No traceparent, or one that is not valid: no span context, and nothing to send.
    try testing.expectEqual(@as(?otel.api.trace.SpanContext, null), try root.spanContext(a, .{}));
    const bad = [_]tracing.Field{.{ .name = "traceparent", .value = "00-00000000000000000000000000000000-b7ad6b7169203331-01" }};
    try testing.expectEqual(@as(?otel.api.trace.SpanContext, null), try root.spanContext(a, .{ .fields = &bad }));
    const invalid = otel.api.trace.SpanContext.init(.zero(), .zero(), .default(), .init(a), false);
    try testing.expect((try root.fromSpanContext(a, invalid)).isEmpty());
}
