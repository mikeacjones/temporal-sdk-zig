//! The server's system Nexus endpoint, which workflow code reaches with an ordinary
//! ScheduleNexusOperation command: the endpoint, service and operation names, and the
//! envelope payload that carries a workflow service request or response as binary protobuf,
//! marked `__temporal_system_payload: "true"`. The payload converter never converts the
//! envelope itself; codecs see only the payloads inside it (see `converter/codec.zig`).
//!
//! Internal: nothing here is re-exported. Everything only reads its inputs and holds no
//! state, so it is safe on any thread.

const std = @import("std");
const proto = @import("../proto.zig");
const converter = @import("../converter.zig");
const Allocator = std.mem.Allocator;
const Payload = converter.Payload;

pub const endpoint = "__temporal_system";
pub const workflow_service = "temporal.api.workflowservice.v1.WorkflowService";
pub const signal_with_start_operation = "SignalWithStartWorkflowExecution";
pub const mark_key = "__temporal_system_payload";
pub const mark_value = "true";
pub const SignalWithStartRequest = proto.temporal.api.workflowservice.v1.SignalWithStartWorkflowExecutionRequest;
pub const SignalWithStartResponse = proto.temporal.api.workflowservice.v1.SignalWithStartWorkflowExecutionResponse;

/// Whether p's metadata has mark_key == mark_value.
pub fn isMarked(p: Payload) bool {
    for (p.metadata.items) |entry| {
        if (std.mem.eql(u8, entry.key, mark_key)) return std.mem.eql(u8, entry.value, mark_value);
    }
    return false;
}

/// One payload of message (SignalWithStartRequest or SignalWithStartResponse; any other
/// type is a compile error): binary/protobuf via converter.ProtoBinary, its messageType,
/// and the mark. Memory from arena.
pub fn envelope(arena: Allocator, message: anytype) error{OutOfMemory}!Payload {
    const T = @TypeOf(message);
    if (T != SignalWithStartRequest and T != SignalWithStartResponse)
        @compileError("a system Nexus envelope holds a SignalWithStartRequest or SignalWithStartResponse, not " ++ @typeName(T));
    var p = try (converter.ProtoBinary(T){ .message = message }).toPayload(arena);
    try p.metadata.append(arena, .{ .key = mark_key, .value = mark_value });
    return p;
}

/// A decoded envelope.
pub const Message = union(enum) {
    signal_with_start_request: SignalWithStartRequest,
    signal_with_start_response: SignalWithStartResponse,
};

pub const DecodeError = error{ OutOfMemory, InvalidPayload, UnknownMessageType };

/// The payload's `messageType` metadata value, or null if it has none.
pub fn messageTypeOf(p: Payload) ?[]const u8 {
    for (p.metadata.items) |entry| {
        if (std.mem.eql(u8, entry.key, converter.message_type_key)) return entry.value;
    }
    return null;
}

/// Decodes a marked payload by its messageType metadata. A missing or unknown type is
/// UnknownMessageType. Data that isn't binary/protobuf of that type is InvalidPayload.
pub fn decode(arena: Allocator, p: Payload) DecodeError!Message {
    const message_type = messageTypeOf(p) orelse return error.UnknownMessageType;
    if (std.mem.eql(u8, message_type, converter.protoMessageType(SignalWithStartRequest)))
        return .{ .signal_with_start_request = try decodeBinary(SignalWithStartRequest, arena, p) };
    if (std.mem.eql(u8, message_type, converter.protoMessageType(SignalWithStartResponse)))
        return .{ .signal_with_start_response = try decodeBinary(SignalWithStartResponse, arena, p) };
    return error.UnknownMessageType;
}

fn decodeBinary(comptime T: type, arena: Allocator, p: Payload) error{ OutOfMemory, InvalidPayload }!T {
    const enc = converter.encodingOf(p) orelse return error.InvalidPayload;
    if (!std.mem.eql(u8, enc, converter.Encoding.proto_binary.text())) return error.InvalidPayload;
    // Decoded messages may borrow their input, so it is copied into the arena.
    var reader: std.Io.Reader = .fixed(try arena.dupe(u8, p.data));
    return T.decode(&reader, arena) catch |err| switch (err) {
        error.OutOfMemory => error.OutOfMemory,
        else => error.InvalidPayload,
    };
}

/// message encoded again, with original's metadata kept unchanged (mark, messageType,
/// encoding). Unknown fields survive, through zig-protobuf's unknown-field preservation.
pub fn reencode(arena: Allocator, original: Payload, message: Message) error{OutOfMemory}!Payload {
    var out: std.Io.Writer.Allocating = .init(arena);
    switch (message) {
        inline else => |m| m.encode(&out.writer, arena) catch return error.OutOfMemory,
    }
    var p = original;
    p.metadata = try original.metadata.clone(arena);
    p.data = out.written();
    return p;
}

/// The response of a completed operation, marked or not.
pub fn decodeSignalWithStartResponse(arena: Allocator, p: Payload) error{ OutOfMemory, InvalidPayload }!SignalWithStartResponse {
    if (messageTypeOf(p)) |t| {
        if (!std.mem.eql(u8, t, converter.protoMessageType(SignalWithStartResponse))) return error.InvalidPayload;
    }
    return decodeBinary(SignalWithStartResponse, arena, p);
}

/// The context the request's inner payloads are converted and coded with. All fields are
/// borrowed from base and request.
pub fn targetContext(base: *const converter.Context, request: *const SignalWithStartRequest) converter.Context {
    return .{
        .values = base.values,
        .namespace = request.namespace,
        .workflow_id = request.workflow_id,
        .workflow_type = if (request.workflow_type) |t| t.name else "",
    };
}

// ---------------------------------------------------------------- tests

const testing = std.testing;

test "an envelope is binary protobuf with its message type and the mark" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const p = try envelope(a, SignalWithStartRequest{ .namespace = "ns", .workflow_id = "target", .signal_name = "add" });
    try testing.expectEqualStrings("binary/protobuf", converter.encodingOf(p).?);
    try testing.expectEqualStrings("temporal.api.workflowservice.v1.SignalWithStartWorkflowExecutionRequest", messageTypeOf(p).?);
    try testing.expect(isMarked(p));

    const r = try envelope(a, SignalWithStartResponse{ .run_id = "run", .started = true });
    try testing.expectEqualStrings("temporal.api.workflowservice.v1.SignalWithStartWorkflowExecutionResponse", messageTypeOf(r).?);
    try testing.expect(isMarked(r));
}

test "isMarked needs the mark's key with the value true" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    try testing.expect(!isMarked(try converter.toPayload(a, @as([]const u8, "plain"))));
    var p = try converter.payloadWithEncoding(a, "binary/protobuf", "");
    try p.metadata.append(a, .{ .key = mark_key, .value = "false" });
    try testing.expect(!isMarked(p));
    p.metadata.items[1].value = "true";
    try testing.expect(isMarked(p));
}

test "requests and responses round-trip through an envelope" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var request: SignalWithStartRequest = .{
        .namespace = "ns",
        .workflow_id = "target",
        .workflow_type = .{ .name = "Target" },
        .signal_name = "add",
        .input = .{},
    };
    try request.input.?.payloads.append(a, try converter.toPayload(a, @as([]const u8, "v")));
    const p = try envelope(a, request);
    const back = (try decode(a, p)).signal_with_start_request;
    try testing.expectEqualStrings("target", back.workflow_id);
    try testing.expectEqualStrings("Target", back.workflow_type.?.name);
    try testing.expectEqualStrings("v", try converter.fromPayload([]const u8, a, back.input.?.payloads.items[0]));

    const again = try reencode(a, p, .{ .signal_with_start_request = back });
    try testing.expectEqualSlices(u8, p.data, again.data);
    try testing.expectEqual(p.metadata.items.len, again.metadata.items.len);
    for (p.metadata.items, again.metadata.items) |x, y| {
        try testing.expectEqualStrings(x.key, y.key);
        try testing.expectEqualStrings(x.value, y.value);
    }

    const r = try envelope(a, SignalWithStartResponse{ .run_id = "run", .started = true });
    const response = (try decode(a, r)).signal_with_start_response;
    try testing.expectEqualStrings("run", response.run_id);
    try testing.expect(response.started);
    try testing.expectEqualStrings("run", (try decodeSignalWithStartResponse(a, r)).run_id);
}

test "decode refuses an unknown or missing message type and data that isn't binary protobuf" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const p = try envelope(a, SignalWithStartResponse{ .run_id = "run" });
    for (p.metadata.items) |*entry| {
        if (std.mem.eql(u8, entry.key, converter.message_type_key)) entry.value = "temporal.api.workflowservice.v1.UpdateWorkflowExecutionRequest";
    }
    try testing.expectError(error.UnknownMessageType, decode(a, p));

    var untyped = try converter.payloadWithEncoding(a, "binary/protobuf", "");
    try untyped.metadata.append(a, .{ .key = mark_key, .value = mark_value });
    try testing.expectError(error.UnknownMessageType, decode(a, untyped));

    var json = try (converter.Proto(SignalWithStartResponse, .{}){ .message = .{ .run_id = "run" } }).toPayload(a);
    try json.metadata.append(a, .{ .key = mark_key, .value = mark_value });
    try testing.expectError(error.InvalidPayload, decode(a, json));

    var garbage = try envelope(a, SignalWithStartResponse{});
    // Field 1 claims ten bytes and has one.
    garbage.data = "\x0a\x0aA";
    try testing.expectError(error.InvalidPayload, decode(a, garbage));
}

test "decodeSignalWithStartResponse reads an unmarked response and refuses other messages" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const plain = try (converter.ProtoBinary(SignalWithStartResponse){ .message = .{ .run_id = "run", .started = false } }).toPayload(a);
    try testing.expect(!isMarked(plain));
    const response = try decodeSignalWithStartResponse(a, plain);
    try testing.expectEqualStrings("run", response.run_id);
    try testing.expect(!response.started);

    const request = try envelope(a, SignalWithStartRequest{ .workflow_id = "target" });
    try testing.expectError(error.InvalidPayload, decodeSignalWithStartResponse(a, request));
    try testing.expectError(error.InvalidPayload, decodeSignalWithStartResponse(a, try converter.toPayload(a, @as(i64, 1))));
}

test "targetContext names the request's target and keeps the base's values" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const values: @import("../propagation.zig").Values = .init(arena.allocator());
    const base: converter.Context = .{ .values = &values, .namespace = "caller-ns", .workflow_id = "caller", .workflow_type = "Caller" };
    const request: SignalWithStartRequest = .{ .namespace = "ns", .workflow_id = "target", .workflow_type = .{ .name = "Target" } };
    const target = targetContext(&base, &request);
    try testing.expect(target.values == &values);
    try testing.expectEqualStrings("ns", target.namespace);
    try testing.expectEqualStrings("target", target.workflow_id);
    try testing.expectEqualStrings("Target", target.workflow_type);
    try testing.expectEqualStrings("", targetContext(&base, &.{}).workflow_type);
}
