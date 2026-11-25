//! README.md examples. The code between `// example: <id>` and `// end-example` is copied into
//! README.md by `zig build readme`; edit it here, never in README.md.

const std = @import("std");
const temporal = @import("temporal");

const converter = temporal.converter;
const workflow = temporal.workflow;
const activity = temporal.activity;
const testing = std.testing;
const Allocator = std.mem.Allocator;

/// Sent as decimal text with its own encoding instead of as JSON.
const Cents = struct {
    value: u64,

    pub fn toPayload(c: Cents, arena: Allocator) error{OutOfMemory}!converter.Payload {
        return converter.payloadWithEncoding(arena, "text/cents", try std.fmt.allocPrint(arena, "{d}", .{c.value}));
    }

    pub fn fromPayload(_: Allocator, p: converter.Payload) converter.DecodeError!Cents {
        const enc = converter.encodingOf(p) orelse return error.UnknownEncoding;
        if (!std.mem.eql(u8, enc, "text/cents")) return error.UnknownEncoding;
        return .{ .value = std.fmt.parseInt(u64, p.data, 10) catch return error.InvalidPayload };
    }
};

test "converter: values round-trip through payloads" {
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    // example: converter-payloads
    const Request = struct { name: []const u8, language: ?[]const u8 = null };
    const json = try converter.toPayload(arena, Request{ .name = "Ziggy" }); // "json/plain"
    try testing.expectEqualStrings("Ziggy", (try converter.fromPayload(Request, arena, json)).name);

    // Cents declares toPayload and fromPayload, so it encodes itself.
    const tip = try converter.toPayload(arena, Cents{ .value = 250 });
    try testing.expectEqual(250, (try converter.fromPayload(Cents, arena, tip)).value);

    const Execution = temporal.proto.temporal.api.common.v1.WorkflowExecution;
    const execution: Execution = .{ .workflow_id = "greeting-1" };
    const proto_json = try converter.toPayload(arena, execution); // "json/protobuf"
    const proto_binary = try converter.toPayload(arena, converter.ProtoBinary(Execution){ .message = execution });
    try testing.expectEqualStrings("binary/protobuf", converter.encodingOf(proto_binary).?);

    // A value whose type the caller learns later stays encoded until read.
    const later: converter.EncodedValue = .{ .payload = proto_json };
    try testing.expectEqualStrings("greeting-1", (try later.get(Execution, arena)).workflow_id);
    // end-example
    try testing.expectEqualStrings("json/plain", converter.encodingOf(json).?);
    try testing.expectEqualStrings("json/protobuf", converter.encodingOf(proto_json).?);
}

// example: converter-context-hooks
/// Records the workflow it was sent for, and refuses to decode for another.
const Ticket = struct {
    seat: []const u8,

    pub fn toPayloadWithContext(t: Ticket, arena: Allocator, context: *const converter.Context) error{OutOfMemory}!converter.Payload {
        var p = try converter.payloadWithEncoding(arena, "text/ticket", t.seat);
        try p.metadata.append(arena, .{ .key = "workflow-id", .value = context.workflow_id });
        return p;
    }

    pub fn fromPayloadWithContext(arena: Allocator, context: *const converter.Context, p: converter.Payload) converter.DecodeError!Ticket {
        const enc = converter.encodingOf(p) orelse return error.UnknownEncoding;
        if (!std.mem.eql(u8, enc, "text/ticket")) return error.UnknownEncoding;
        for (p.metadata.items) |m| {
            if (std.mem.eql(u8, m.key, "workflow-id") and !std.mem.eql(u8, m.value, context.workflow_id)) return error.InvalidPayload;
        }
        return .{ .seat = try arena.dupe(u8, p.data) };
    }
};

fn checkIn(ctx: *activity.Context, ticket: Ticket) ![]const u8 {
    // The activity's context names its workflow, its type and its ID.
    return std.fmt.allocPrint(ctx.allocator(), "{s} for {s}", .{ ticket.seat, ctx.converterContext().workflow_id });
}

fn board(ctx: *workflow.Context, ticket: Ticket) ![]const u8 {
    // The client encoded `ticket` for this workflow's ID; the activity's argument is
    // encoded and decoded with the activity's context, which names the same workflow.
    return ctx.executeActivity(CheckIn, .{ .start_to_close_timeout = .fromSeconds(10) }, .{ticket}).get();
}
// end-example

const CheckIn = activity.Def("check_in", checkIn);
const Board = workflow.Def("board", board);

test "converter: a type's context hooks see the workflow its value is sent for" {
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const for_one: converter.Context = .{ .namespace = "default", .workflow_id = "board-1" };
    const p = try converter.toPayloadWithContext(arena, &for_one, Ticket{ .seat = "12A" });
    try testing.expectEqualStrings("12A", (try converter.fromPayloadWithContext(Ticket, arena, &for_one, p)).seat);
    try testing.expectError(error.InvalidPayload, converter.fromPayloadWithContext(Ticket, arena, &.{ .workflow_id = "board-2" }, p));
    // Without a context the hooks get the empty one.
    try testing.expectError(error.InvalidPayload, converter.fromPayload(Ticket, arena, p));

    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(Board);
    try env.registerActivity(CheckIn);
    const seat = try env.executeWorkflowWithOptions(Board, .{ .id = "board-1" }, .{.{ .seat = "12A" }}, arena, null);
    try testing.expectEqualStrings("12A for board-1", seat);
}

test "converter: ProtoJSON of protobuf messages" {
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    // example: converter-protojson
    const common = temporal.proto.temporal.api.common.v1;
    var payload: common.Payload = .{ .data = &.{ 0x00, 0xff } };
    try payload.metadata.append(arena, .{ .key = "encoding", .value = "binary/plain" });
    const json = try temporal.protojson.encode(arena, common.Payloads{ .payloads = .fromOwnedSlice(try arena.dupe(common.Payload, &.{payload})) });
    // {"payloads":[{"metadata":{"encoding":"YmluYXJ5L3BsYWlu"},"data":"AP8="}]}
    const list = try temporal.protojson.decode(common.Payloads, arena, json);
    try testing.expectEqualSlices(u8, &.{ 0x00, 0xff }, list.payloads.items[0].data);

    // Older CLI and Web UI exports write enums by short name.
    const TaskQueue = temporal.proto.temporal.api.taskqueue.v1.TaskQueue;
    const queue = try temporal.protojson.decode(TaskQueue, arena,
        \\{"name":"orders","kind":"Normal"}
    );
    try testing.expectEqual(.TASK_QUEUE_KIND_NORMAL, queue.kind);
    // end-example
    try testing.expectEqualStrings(
        \\{"payloads":[{"metadata":{"encoding":"YmluYXJ5L3BsYWlu"},"data":"AP8="}]}
    , json);
}

test "converter: compress payloads with the zlib codec" {
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const io = testing.io;
    const name = try arena.alloc(u8, 400);
    @memset(name, 'z');

    // example: converter-codecs
    const zlib: converter.ZlibCodec = .{};
    const codecs = [_]converter.PayloadCodec{zlib.codec()};

    // What a client with `.codecs = &codecs` does to every payload it sends and receives.
    const plain = try converter.toPayload(arena, name);
    const encoded = try converter.encodePayloads(&codecs, arena, io, &.{plain});
    try testing.expectEqualStrings("binary/zlib", converter.encodingOf(encoded[0]).?);
    const decoded = try converter.decodePayloads(&codecs, arena, io, encoded);
    try testing.expectEqualStrings(name, try converter.fromPayload([]const u8, arena, decoded[0]));

    const env = try temporal.testing.WorkflowEnvironment.startWithOptions(testing.allocator, io, .{ .codecs = &codecs });
    defer env.deinit();
    // end-example
    try testing.expect(encoded[0].data.len < plain.data.len);
    try env.registerWorkflow(Greeting);
    try env.registerActivity(Greet);
    const result = try env.executeWorkflow(Greeting, .{name}, arena, null);
    try testing.expectEqualStrings(try std.fmt.allocPrint(arena, "Hello, {s}!", .{name}), result);
}

// example: converter-nexus-context
/// Marks each payload with the key it would be encrypted with: one per Nexus endpoint for a
/// Nexus operation's payloads, the default key for every other payload.
const EndpointKeyCodec = struct {
    const key_metadata = "key-id";

    fn codec(c: *const EndpointKeyCodec) converter.PayloadCodec {
        return .{ .ptr = @constCast(c), .vtable = &.{
            .encode = encode,
            .decode = decode,
            .encodeWithContext = encodeWithContext,
            .decodeWithContext = decodeWithContext,
        } };
    }

    fn keyOf(p: converter.Payload) ?[]const u8 {
        for (p.metadata.items) |entry| if (std.mem.eql(u8, entry.key, key_metadata)) return entry.value;
        return null;
    }

    fn keyFor(context: *const converter.Context) []const u8 {
        const n = context.nexus orelse return "default";
        // The endpoint the caller called through, on the caller's worker and the handler's.
        return if (std.mem.eql(u8, n.endpoint, "payments-endpoint")) "payments" else "default";
    }

    fn encodeWithContext(_: *anyopaque, arena: Allocator, _: std.Io, context: *const converter.Context, payloads: []const converter.Payload) converter.CodecError![]converter.Payload {
        const out = try arena.alloc(converter.Payload, payloads.len);
        for (payloads, out) |p, *o| {
            o.* = try p.dupe(arena);
            // A real codec encrypts `o.data` with the key here.
            try o.metadata.append(arena, .{ .key = key_metadata, .value = keyFor(context) });
        }
        return out;
    }

    fn decodeWithContext(_: *anyopaque, arena: Allocator, _: std.Io, context: *const converter.Context, payloads: []const converter.Payload) converter.CodecError![]converter.Payload {
        const out = try arena.dupe(converter.Payload, payloads);
        for (out) |*p| {
            const key = keyOf(p.*) orelse continue;
            if (!std.mem.eql(u8, key, keyFor(context))) return error.CodecFailed;
        }
        return out;
    }

    fn encode(ptr: *anyopaque, arena: Allocator, io: std.Io, payloads: []const converter.Payload) converter.CodecError![]converter.Payload {
        return encodeWithContext(ptr, arena, io, &.{}, payloads);
    }

    fn decode(ptr: *anyopaque, arena: Allocator, io: std.Io, payloads: []const converter.Payload) converter.CodecError![]converter.Payload {
        return decodeWithContext(ptr, arena, io, &.{}, payloads);
    }
};
// end-example

test "converter: a codec chooses its key by the Nexus endpoint" {
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const io = testing.io;
    const keyed: EndpointKeyCodec = .{};
    const codecs = [_]converter.PayloadCodec{keyed.codec()};
    const payments: converter.Context = .{ .namespace = "default", .nexus = .{ .endpoint = "payments-endpoint", .service = "payments", .operation = "charge" } };
    const plain = try converter.toPayload(arena, "card");
    const encoded = try converter.encodePayloadsWithContext(&codecs, arena, io, &payments, &.{plain});
    try testing.expectEqualStrings("payments", EndpointKeyCodec.keyOf(encoded[0]).?);
    _ = try converter.decodePayloadsWithContext(&codecs, arena, io, &payments, encoded);
    try testing.expectError(error.CodecFailed, converter.decodePayloadsWithContext(&codecs, arena, io, &.{}, encoded));
}

/// Serves `converter.codecHandler` over `codecs` on `listener` until canceled, a task per
/// connection.
fn serveCodecs(io: std.Io, listener: *std.Io.net.Server, codecs: []const converter.PayloadCodec) void {
    var group: std.Io.Group = .init;
    defer group.cancel(io);
    while (true) {
        const stream = listener.accept(io) catch return;
        group.concurrent(io, serveCodecConnection, .{ io, stream, codecs }) catch {
            stream.close(io);
            return;
        };
    }
}

fn serveCodecConnection(io: std.Io, stream: std.Io.net.Stream, codecs: []const converter.PayloadCodec) std.Io.Cancelable!void {
    defer stream.close(io);
    var recv_buffer: [8192]u8 = undefined;
    var send_buffer: [4096]u8 = undefined;
    var conn_reader = stream.reader(io, &recv_buffer);
    var conn_writer = stream.writer(io, &send_buffer);
    var server = std.http.Server.init(&conn_reader.interface, &conn_writer.interface);
    while (server.reader.state == .ready) {
        var request = server.receiveHead() catch return;
        var arena: std.heap.ArenaAllocator = .init(testing.allocator);
        defer arena.deinit();
        converter.codecHandler(codecs, .{}).handle(arena.allocator(), io, &request) catch return;
    }
}

test "converter: a remote codec calls a codec server" {
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const io = testing.io;
    const gpa = testing.allocator;

    // The codec server: codecHandler over a zlib codec, on a local port.
    const zlib: converter.ZlibCodec = .{ .always_encode = true };
    const server_codecs = [_]converter.PayloadCodec{zlib.codec()};
    var listener = try (try std.Io.net.IpAddress.parse("127.0.0.1", 0)).listen(io, .{ .reuse_address = true });
    defer listener.deinit(io);
    var serving = try io.concurrent(serveCodecs, .{ io, &listener, &server_codecs });
    defer serving.cancel(io);
    const endpoint = try std.fmt.allocPrint(arena, "http://127.0.0.1:{d}/default", .{listener.socket.address.getPort()});

    // example: converter-remote-codec
    var remote = try converter.RemoteCodec.init(gpa, io, .{
        .endpoint = endpoint,
        .headers = &.{.{ .name = "authorization", .value = "Bearer codec-token" }},
        .namespace = "default",
        .timeout = .fromSeconds(5),
    });
    defer remote.deinit();
    const codecs = [_]converter.PayloadCodec{remote.codec()};
    const env = try temporal.testing.WorkflowEnvironment.startWithOptions(testing.allocator, io, .{ .codecs = &codecs });
    defer env.deinit();
    // end-example
    const encoded = try converter.encodePayloads(&codecs, arena, io, &.{try converter.toPayload(arena, "zig")});
    try testing.expectEqualStrings("binary/zlib", converter.encodingOf(encoded[0]).?);
    try env.registerWorkflow(Greeting);
    try env.registerActivity(Greet);
    const result = try env.executeWorkflow(Greeting, .{"remote codec"}, arena, null);
    try testing.expectEqualStrings("Hello, remote codec!", result);
}

// example: converter-failures
fn greet(ctx: *activity.Context, name: []const u8) ![]const u8 {
    if (name.len == 0) return ctx.fail(.{ .message = "name is empty", .type = "InvalidName", .non_retryable = true });
    return std.fmt.allocPrint(ctx.allocator(), "Hello, {s}!", .{name});
}

fn greeting(ctx: *workflow.Context, name: []const u8) ![]const u8 {
    const future = ctx.executeActivity(Greet, .{ .start_to_close_timeout = .fromSeconds(10) }, .{name});
    return future.get() catch |err| {
        if (future.failure()) |f| {
            // For an empty name: "name is empty", type "InvalidName".
            ctx.logger().warn("greet failed", .{ .reason = temporal.failure.rootCause(f).message });
        }
        return err;
    };
}
// end-example

const Greet = activity.Def("greet", greet);
const Greeting = workflow.Def("greeting", greeting);

test "converter: a failure's cause chain reaches the caller" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(Greeting);
    try env.registerActivity(Greet);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var run = try env.startWorkflow(Greeting, .{}, .{""});
    defer run.deinit();
    try testing.expectError(error.WorkflowFailed, env.getResult(Greeting, &run, arena.allocator(), null));

    // The workflow failed with the activity's failure, caused by greet's application failure.
    const root = temporal.failure.rootCause(run.failure().?);
    try testing.expectEqualStrings("name is empty", root.message);
    try testing.expectEqualStrings("InvalidName", temporal.failure.applicationType(root.*).?);
}

// example: converter-fail-with
fn checkout(ctx: *workflow.Context, order: []const u8) !void {
    const opts: workflow.ActivityOptions = .{ .start_to_close_timeout = .fromSeconds(10) };
    try ctx.executeActivity(Hold, opts, .{order}).get();
    // Releases the hold if anything below fails; the release can fail too.
    errdefer ctx.executeActivity(Release, opts, .{order}).get() catch {};

    const charge = ctx.executeActivity(Charge, opts, .{order});
    // Keeps the charge's failure: without failWith, a failed release would replace it.
    // Other errors, such as error.Canceled, are returned as they are.
    charge.get() catch |err| return switch (err) {
        error.ActivityFailed => ctx.failWith(charge.failure().?),
        else => err,
    };
}
// end-example

fn holdFunds(_: *activity.Context, _: []const u8) !void {}
fn chargeCard(ctx: *activity.Context, _: []const u8) !void {
    return ctx.fail(.{ .message = "card declined", .type = "Declined", .non_retryable = true });
}
fn releaseFunds(ctx: *activity.Context, _: []const u8) !void {
    return ctx.fail(.{ .message = "release failed", .non_retryable = true });
}

const Hold = activity.Def("hold", holdFunds);
const Charge = activity.Def("charge", chargeCard);
const Release = activity.Def("release", releaseFunds);
const Checkout = workflow.Def("checkout", checkout);

test "converter: failWith keeps a failure that a failed cleanup would replace" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(Checkout);
    try env.registerActivity(Hold);
    try env.registerActivity(Charge);
    try env.registerActivity(Release);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var failure: ?temporal.failure.Failure = null;
    try testing.expectError(error.WorkflowFailed, env.executeWorkflow(Checkout, .{"order-1"}, arena.allocator(), &failure));
    try testing.expectEqualStrings("card declined", temporal.failure.rootCause(&failure.?).message);
    try testing.expectEqualStrings("charge", failure.?.failure_info.?.activity_failure_info.activity_type.?.name);
}

// example: converter-failure-kinds
fn reserve(ctx: *activity.Context, sku: []const u8) !void {
    const why = try temporal.failure.application(ctx.allocator(), .{ .message = "warehouse offline", .type = "WarehouseOffline" });
    return ctx.fail(.{
        .message = "reservation failed",
        .type = "ReserveFailed",
        .non_retryable = true,
        .details = &.{try converter.toPayload(ctx.allocator(), sku)},
        // Copied by ctx.fail, so it may live on the stack.
        .cause = &why,
    });
}

fn fulfill(ctx: *workflow.Context, sku: []const u8) ![]const u8 {
    const future = ctx.executeActivity(Reserve, .{ .start_to_close_timeout = .fromSeconds(5) }, .{sku});
    future.get() catch |err| {
        const f = future.failure() orelse return err;
        // f is the activity failure; its cause is the one the activity raised.
        const reason = switch (temporal.failure.kind(f.cause orelse f)) {
            .timeout => |t| try std.fmt.allocPrint(ctx.allocator(), "timed out ({t})", .{t.timeout_type}),
            .application => |app| try std.fmt.allocPrint(ctx.allocator(), "{s} for {s}: {s}", .{
                app.type,
                try app.details.get([]const u8, ctx.allocator(), 0),
                temporal.failure.rootCause(f).message,
            }),
            else => return err,
        };
        // The workflow's failure, with the activity's chain under it.
        return ctx.fail(.{ .message = reason, .type = "FulfillFailed", .cause = f });
    };
    return "reserved";
}
// end-example

const Reserve = activity.Def("reserve", reserve);
const Fulfill = workflow.Def("fulfill", fulfill);

test "converter: causes, kinds and stack traces of failures" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(Fulfill);
    try env.registerActivity(Reserve);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var failure: ?temporal.failure.Failure = null;
    try testing.expectError(error.WorkflowFailed, env.executeWorkflow(Fulfill, .{"sku-1"}, arena.allocator(), &failure));
    try testing.expectEqualStrings("ReserveFailed for sku-1: warehouse offline", failure.?.message);
    try testing.expectEqualStrings("warehouse offline", temporal.failure.rootCause(&failure.?).message);
    // Built from ctx.fail and the error the workflow returned, so it has a stack trace.
    try testing.expect(failure.?.stack_trace.len > 0);
}

test "converter: encoded failure attributes" {
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const io = testing.io;

    // example: converter-encoded-attributes
    const zlib: converter.ZlibCodec = .{};
    const codecs = [_]converter.PayloadCodec{zlib.codec()};
    const env = try temporal.testing.WorkflowEnvironment.startWithOptions(testing.allocator, io, .{
        .codecs = &codecs,
        // client.Options.encode_failure_attributes, for the client and its workers.
        .encode_failure_attributes = true,
    });
    defer env.deinit();

    // What the client and its workers do to each failure they send, before the codecs run,
    // and to each failure they receive, after them.
    var f = try temporal.failure.application(arena, .{ .message = "card 4242 declined" });
    try temporal.failure.encodeAttributes(temporal.Failure, &f, arena);
    try testing.expectEqualStrings("Encoded failure", f.message);
    try temporal.failure.decodeAttributes(temporal.Failure, &f, arena);
    try testing.expectEqualStrings("card 4242 declined", f.message);
    // end-example
}

// example: converter-failure-converter
/// Masks the digits in the messages of every failure user code sends, causes included, so
/// card numbers never reach history. It keeps no state that changes, so it is deterministic
/// and safe to call from any thread, as a failure converter must be.
const Masking = struct {
    mask: u8 = '*',

    pub fn failureConverter(m: *const Masking) converter.FailureConverter {
        return .{ .ptr = @constCast(m), .vtable = &.{ .toFailure = toFailure, .fromFailure = fromFailure } };
    }

    fn toFailure(ptr: *anyopaque, arena: Allocator, _: *const converter.Context, _: ?anyerror, f: converter.Failure) error{OutOfMemory}!converter.Failure {
        const m: *const Masking = @ptrCast(@alignCast(ptr));
        return m.masked(arena, f);
    }

    /// Failures are read as they arrive: they were masked when sent.
    fn fromFailure(_: *anyopaque, _: Allocator, _: *const converter.Context, f: converter.Failure) error{OutOfMemory}!converter.Failure {
        return f;
    }

    fn masked(m: *const Masking, arena: Allocator, f: converter.Failure) error{OutOfMemory}!converter.Failure {
        var out = f;
        const message = try arena.dupe(u8, f.message);
        for (message) |*c| if (std.ascii.isDigit(c.*)) {
            c.* = m.mask;
        };
        out.message = message;
        if (f.cause) |cause| {
            out.cause = try arena.create(converter.Failure);
            out.cause.?.* = try m.masked(arena, cause.*);
        }
        return out;
    }
};

fn chargeCardNumber(ctx: *activity.Context, card: []const u8) !void {
    return ctx.fail(.{ .message = try std.fmt.allocPrint(ctx.allocator(), "card {s} declined", .{card}), .type = "Declined", .non_retryable = true });
}

fn payByCard(ctx: *workflow.Context, card: []const u8) !void {
    return ctx.executeActivity(ChargeCardNumber, .{ .start_to_close_timeout = .fromSeconds(10) }, .{card}).get();
}
// end-example

const ChargeCardNumber = activity.Def("charge_card_number", chargeCardNumber);
const PayByCard = workflow.Def("pay_by_card", payByCard);

test "converter: a failure converter masks what failures send" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const masking: Masking = .{};
    const env = try temporal.testing.WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{
        // client.Options.failure_converter: the client, its workers and replays use it.
        .failure_converter = masking.failureConverter(),
    });
    defer env.deinit();
    try env.registerWorkflow(PayByCard);
    try env.registerActivity(ChargeCardNumber);
    var failure: ?temporal.failure.Failure = null;
    try testing.expectError(error.WorkflowFailed, env.executeWorkflow(PayByCard, .{"4242"}, arena.allocator(), &failure));
    try testing.expectEqualStrings("card **** declined", temporal.failure.rootCause(&failure.?).message);
}
