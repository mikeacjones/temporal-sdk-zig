//! A minimal OTLP trace collector for samples-test: it records the name and IDs of every span
//! it receives, so the samples runner can check the spans a sample's programs exported.
//!
//! It listens on 127.0.0.1 at a port the OS picks, writes `127.0.0.1:<port>` and a newline to
//! stdout, and then serves until it is killed. It accepts `POST /v1/traces` with an OTLP
//! `ExportTraceServiceRequest` in protobuf (HTTP/protobuf, no compression), one request per
//! connection. For every span of a request it appends one line to the spans file,
//!
//!     {"name":"...","trace_id":"<32 hex>","span_id":"<16 hex>","parent_span_id":"<16 hex or empty>"}
//!
//! and only once the request's lines are written does it answer 200, so a program whose
//! exporter has seen the answer has its spans in the file. Any other request is answered 400
//! with one line on stderr naming the reason, and nothing is written. It is single-threaded
//! and handles no signals: the runner stops it with SIGTERM.
//!
//! Usage: otlp-collector <spans-file>

const std = @import("std");
const proto = @import("opentelemetry-proto");
const Io = std.Io;
const Allocator = std.mem.Allocator;

/// The largest request body accepted.
const max_body = 16 * 1024 * 1024;

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;

    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.skip();
    const path = args.next() orelse usage();
    if (args.next() != null) usage();

    const file = Io.Dir.cwd().createFile(io, path, .{ .truncate = true }) catch |err| {
        std.log.err("cannot create the spans file {s}: {t}", .{ path, err });
        std.process.exit(1);
    };
    var stderr_buffer: [1024]u8 = undefined;
    var stderr = Io.File.stderr().writerStreaming(io, &stderr_buffer);
    var collector = Collector.init(gpa, io, file, &stderr.interface) catch |err| {
        std.log.err("cannot listen on 127.0.0.1: {t}", .{err});
        std.process.exit(1);
    };

    var stdout_buffer: [64]u8 = undefined;
    var stdout = Io.File.stdout().writerStreaming(io, &stdout_buffer);
    stdout.interface.print("127.0.0.1:{d}\n", .{collector.port()}) catch |err| {
        std.log.err("cannot write the address to stdout: {t}", .{err});
        std.process.exit(1);
    };
    stdout.interface.flush() catch |err| {
        std.log.err("cannot write the address to stdout: {t}", .{err});
        std.process.exit(1);
    };
    collector.serve();
}

fn usage() noreturn {
    std.log.err("usage: otlp-collector <spans-file>", .{});
    std.process.exit(2);
}

const Collector = struct {
    gpa: Allocator,
    io: Io,
    server: Io.net.Server,
    file: Io.File,
    /// Where the spans file ends: the next request's lines go here.
    offset: u64 = 0,
    /// Gets one line for every request that is refused.
    rejections: *Io.Writer,
    /// Set by the tests to end `serve`, followed by a connection to wake its `accept`.
    stopping: std.atomic.Value(bool) = .init(false),

    /// Listens on 127.0.0.1 at a port the OS picks. `file` is borrowed, empty, and only
    /// written by this collector.
    fn init(gpa: Allocator, io: Io, file: Io.File, rejections: *Io.Writer) !Collector {
        const address = try Io.net.IpAddress.parse("127.0.0.1", 0);
        return .{ .gpa = gpa, .io = io, .server = try address.listen(io, .{}), .file = file, .rejections = rejections };
    }

    fn deinit(c: *Collector) void {
        c.server.deinit(c.io);
    }

    fn port(c: *const Collector) u16 {
        return c.server.socket.address.getPort();
    }

    /// Answers one request per connection, one connection at a time, until `stopping`.
    fn serve(c: *Collector) void {
        while (!c.stopping.load(.acquire)) {
            const stream = c.server.accept(c.io) catch |err| switch (err) {
                error.Canceled => return,
                else => {
                    c.reject("accepting a connection failed: {t}", .{err});
                    continue;
                },
            };
            defer stream.close(c.io);
            if (c.stopping.load(.acquire)) return;
            c.handle(stream);
        }
    }

    fn handle(c: *Collector, stream: Io.net.Stream) void {
        var read_buffer: [8192]u8 = undefined;
        var write_buffer: [1024]u8 = undefined;
        var reader = stream.reader(c.io, &read_buffer);
        var writer = stream.writer(c.io, &write_buffer);
        var server = std.http.Server.init(&reader.interface, &writer.interface);
        var request = server.receiveHead() catch |err| {
            c.reject("cannot read a request: {t}", .{err});
            return;
        };
        var arena: std.heap.ArenaAllocator = .init(c.gpa);
        defer arena.deinit();
        // The head points into the connection's buffer, which reading the body reuses.
        const method = @tagName(request.head.method);
        const target = arena.allocator().dupe(u8, request.head.target) catch {
            c.reject("out of memory reading a request", .{});
            return;
        };
        const status: std.http.Status = if (c.record(arena.allocator(), &request)) |outcome| switch (outcome) {
            .recorded => .ok,
            .refused => |reason| blk: {
                c.reject("refused {s} {s}: {s}", .{ method, target, reason });
                break :blk .bad_request;
            },
        } else |err| blk: {
            c.reject("{s} {s}: {t}", .{ method, target, err });
            break :blk .internal_server_error;
        };
        request.respond("", .{ .status = status, .keep_alive = false }) catch |err|
            c.reject("cannot answer {s} {s}: {t}", .{ method, target, err });
    }

    const Outcome = union(enum) {
        recorded,
        /// Why the request was not a decodable `POST /v1/traces`.
        refused: []const u8,
    };

    /// Appends a line to the spans file for every span of `request`, or none when it is
    /// refused. Fails only when the file can't be written or memory runs out.
    fn record(c: *Collector, arena: Allocator, request: *std.http.Server.Request) error{ OutOfMemory, WriteFailed }!Outcome {
        const head = request.head;
        if (head.method != .POST or !std.mem.eql(u8, head.target, "/v1/traces"))
            return .{ .refused = "only POST /v1/traces is accepted" };
        if (head.transfer_compression != .identity)
            return .{ .refused = try std.fmt.allocPrint(arena, "the body is compressed ({t}); only uncompressed protobuf is accepted", .{head.transfer_compression}) };
        if (head.content_length) |length| if (length > max_body)
            return .{ .refused = try std.fmt.allocPrint(arena, "the body of {d} bytes is over the limit of {d}", .{ length, max_body }) };

        var body_buffer: [8192]u8 = undefined;
        const body_reader = request.readerExpectContinue(&body_buffer) catch |err|
            return .{ .refused = try std.fmt.allocPrint(arena, "cannot read the body: {t}", .{err}) };
        const body = body_reader.allocRemaining(arena, .limited(max_body)) catch |err| return switch (err) {
            error.OutOfMemory => error.OutOfMemory,
            error.StreamTooLong => .{ .refused = try std.fmt.allocPrint(arena, "the body is over the limit of {d} bytes", .{max_body}) },
            error.ReadFailed => .{ .refused = "cannot read the body" },
        };
        var body_stream: Io.Reader = .fixed(body);
        const message = proto.collector_trace_v1.ExportTraceServiceRequest.decode(&body_stream, arena) catch |err|
            return .{ .refused = try std.fmt.allocPrint(arena, "the body is not an ExportTraceServiceRequest: {t}", .{err}) };

        var lines: Io.Writer.Allocating = .init(arena);
        for (message.resource_spans.items) |rs| for (rs.scope_spans.items) |ss| for (ss.spans.items) |span| {
            if (span.trace_id.len != 16 or span.span_id.len != 8 or (span.parent_span_id.len != 0 and span.parent_span_id.len != 8))
                return .{ .refused = try std.fmt.allocPrint(arena, "span \"{s}\" has a trace ID of {d} bytes, a span ID of {d} and a parent span ID of {d}; expected 16, 8, and 8 or none", .{ span.name, span.trace_id.len, span.span_id.len, span.parent_span_id.len }) };
            const line: ReceivedLine = .{
                .name = span.name,
                .trace_id = try std.fmt.allocPrint(arena, "{x}", .{span.trace_id}),
                .span_id = try std.fmt.allocPrint(arena, "{x}", .{span.span_id}),
                .parent_span_id = try std.fmt.allocPrint(arena, "{x}", .{span.parent_span_id}),
            };
            lines.writer.print("{f}\n", .{std.json.fmt(line, .{})}) catch return error.OutOfMemory;
        };
        c.file.writePositionalAll(c.io, lines.written(), c.offset) catch return error.WriteFailed;
        c.offset += lines.written().len;
        return .recorded;
    }

    fn reject(c: *Collector, comptime format: []const u8, args: anytype) void {
        c.rejections.print("otlp-collector: " ++ format ++ "\n", args) catch return;
        c.rejections.flush() catch {};
    }
};

/// One line of the spans file.
const ReceivedLine = struct {
    name: []const u8,
    trace_id: []const u8,
    span_id: []const u8,
    parent_span_id: []const u8,
};

const testing = std.testing;

/// A collector serving on its own task, writing to a file in a temporary directory.
const TestCollector = struct {
    tmp: testing.TmpDir,
    file: Io.File,
    rejections: Io.Writer.Allocating,
    collector: Collector,
    task: Io.Future(void),

    fn start(t: *TestCollector) !void {
        const io = testing.io;
        t.tmp = testing.tmpDir(.{});
        errdefer t.tmp.cleanup();
        t.file = try t.tmp.dir.createFile(io, "spans.jsonl", .{ .truncate = true, .read = true });
        errdefer t.file.close(io);
        t.rejections = .init(testing.allocator);
        errdefer t.rejections.deinit();
        t.collector = try Collector.init(testing.allocator, io, t.file, &t.rejections.writer);
        errdefer t.collector.deinit();
        t.task = try io.concurrent(Collector.serve, .{&t.collector});
    }

    /// Ends `serve` by connecting once after setting `stopping`, then frees all.
    fn stop(t: *TestCollector) void {
        const io = testing.io;
        t.collector.stopping.store(true, .release);
        if (t.collector.server.socket.address.connect(io, .{ .mode = .stream })) |s| s.close(io) else |_| {}
        t.task.await(io);
        t.collector.deinit();
        t.rejections.deinit();
        t.file.close(io);
        t.tmp.cleanup();
    }

    /// Sends one request and returns the answer's status.
    fn send(t: *TestCollector, method: std.http.Method, path: []const u8, body: ?[]const u8) !std.http.Status {
        var http: std.http.Client = .{ .allocator = testing.allocator, .io = testing.io };
        defer http.deinit();
        var url_buffer: [64]u8 = undefined;
        const url = try std.fmt.bufPrint(&url_buffer, "http://127.0.0.1:{d}{s}", .{ t.collector.port(), path });
        const response = try http.fetch(.{
            .location = .{ .url = url },
            .method = method,
            .payload = body,
            .headers = .{ .content_type = .{ .override = "application/x-protobuf" } },
        });
        return response.status;
    }

    fn spans(t: *TestCollector, gpa: Allocator) ![]u8 {
        return t.tmp.dir.readFileAlloc(testing.io, "spans.jsonl", gpa, .limited(1024 * 1024));
    }
};

fn testSpan(name: []const u8, trace_id: []const u8, span_id: []const u8, parent_span_id: []const u8) proto.trace_v1.Span {
    return .{ .name = name, .trace_id = trace_id, .span_id = span_id, .parent_span_id = parent_span_id };
}

/// An encoded `ExportTraceServiceRequest` with `spans` under one resource and scope.
fn encodeRequest(arena: Allocator, spans: []const proto.trace_v1.Span) ![]const u8 {
    var scope_spans: proto.trace_v1.ScopeSpans = .{};
    try scope_spans.spans.appendSlice(arena, spans);
    var resource_spans: proto.trace_v1.ResourceSpans = .{};
    try resource_spans.scope_spans.append(arena, scope_spans);
    var request: proto.collector_trace_v1.ExportTraceServiceRequest = .{};
    try request.resource_spans.append(arena, resource_spans);
    var out: Io.Writer.Allocating = .init(arena);
    try request.encode(&out.writer, arena);
    return out.written();
}

const trace_a = "\x0a\xf7\x65\x19\x16\xcd\x43\xdd\x84\x48\xeb\x21\x1c\x80\x31\x9c";
const span_1 = "\xb7\xad\x6b\x71\x69\x20\x33\x31";
const span_2 = "\x00\x01\x02\x03\x04\x05\x06\x07";

test "a span export is in the spans file, one line per span, once the collector has answered it" {
    var t: TestCollector = undefined;
    try t.start();
    defer t.stop();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const body = try encodeRequest(a, &.{
        testSpan("StartWorkflow:Workflow", trace_a, span_1, ""),
        testSpan("say \"hi\"", trace_a, span_2, span_1),
    });
    try testing.expectEqual(std.http.Status.ok, try t.send(.POST, "/v1/traces", body));
    // The answer comes only after the lines are written, so they are there now.
    const first = try t.spans(a);
    try testing.expectEqualStrings(
        \\{"name":"StartWorkflow:Workflow","trace_id":"0af7651916cd43dd8448eb211c80319c","span_id":"b7ad6b7169203331","parent_span_id":""}
        \\{"name":"say \"hi\"","trace_id":"0af7651916cd43dd8448eb211c80319c","span_id":"0001020304050607","parent_span_id":"b7ad6b7169203331"}
        \\
    , first);

    // A second request appends.
    try testing.expectEqual(std.http.Status.ok, try t.send(.POST, "/v1/traces", try encodeRequest(a, &.{testSpan("custom-span", trace_a, span_2, "")})));
    const second = try t.spans(a);
    try testing.expectStringStartsWith(second, first);
    try testing.expectEqualStrings(
        \\{"name":"custom-span","trace_id":"0af7651916cd43dd8448eb211c80319c","span_id":"0001020304050607","parent_span_id":""}
        \\
    , second[first.len..]);
    try testing.expectEqualStrings("", t.rejections.written());
}

test "a request that is not a decodable span export is answered 400 and writes nothing" {
    var t: TestCollector = undefined;
    try t.start();
    defer t.stop();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const good = try encodeRequest(a, &.{testSpan("RunActivity:Activity", trace_a, span_1, "")});

    try testing.expectEqual(std.http.Status.bad_request, try t.send(.POST, "/v1/metrics", good));
    try testing.expectEqual(std.http.Status.bad_request, try t.send(.PUT, "/v1/traces", good));
    try testing.expectEqual(std.http.Status.bad_request, try t.send(.POST, "/v1/traces", "\x0a\x10abc"));
    // A span ID of the wrong length refuses the whole request, the good span with it.
    try testing.expectEqual(std.http.Status.bad_request, try t.send(.POST, "/v1/traces", try encodeRequest(a, &.{
        testSpan("RunActivity:Activity", trace_a, span_1, ""),
        testSpan("short", trace_a, "\x01\x02", ""),
    })));
    try testing.expectEqualStrings("", try t.spans(a));

    var lines = std.mem.splitScalar(u8, t.rejections.written(), '\n');
    try testing.expectEqualStrings("otlp-collector: refused POST /v1/metrics: only POST /v1/traces is accepted", lines.next().?);
    try testing.expectEqualStrings("otlp-collector: refused PUT /v1/traces: only POST /v1/traces is accepted", lines.next().?);
    try testing.expectStringStartsWith(lines.next().?, "otlp-collector: refused POST /v1/traces: the body is not an ExportTraceServiceRequest: ");
    try testing.expectEqualStrings("otlp-collector: refused POST /v1/traces: span \"short\" has a trace ID of 16 bytes, a span ID of 2 and a parent span ID of 0; expected 16, 8, and 8 or none", lines.next().?);
    try testing.expectEqualStrings("", lines.next().?);
    try testing.expectEqual(null, lines.next());

    // The collector goes on serving after refusing.
    try testing.expectEqual(std.http.Status.ok, try t.send(.POST, "/v1/traces", good));
    try testing.expectStringStartsWith(try t.spans(a), "{\"name\":\"RunActivity:Activity\"");
}

test "a body over 16 MiB is answered 400 without being read" {
    const io = testing.io;
    var t: TestCollector = undefined;
    try t.start();
    defer t.stop();
    const stream = try t.collector.server.socket.address.connect(io, .{ .mode = .stream });
    defer stream.close(io);
    var write_buffer: [256]u8 = undefined;
    var writer = stream.writer(io, &write_buffer);
    try writer.interface.print("POST /v1/traces HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/x-protobuf\r\nContent-Length: {d}\r\n\r\n", .{max_body + 1});
    try writer.interface.flush();
    var read_buffer: [1024]u8 = undefined;
    var reader = stream.reader(io, &read_buffer);
    const answer = try reader.interface.allocRemaining(testing.allocator, .limited(64 * 1024));
    defer testing.allocator.free(answer);
    try testing.expectStringStartsWith(answer, "HTTP/1.1 400 ");
    try testing.expectEqualStrings("otlp-collector: refused POST /v1/traces: the body of 16777217 bytes is over the limit of 16777216\n", t.rejections.written());
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    try testing.expectEqualStrings("", try t.spans(arena.allocator()));
}
