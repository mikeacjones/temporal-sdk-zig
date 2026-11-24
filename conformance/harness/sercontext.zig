//! The signing codec and failure converter the serialization_context features share. The
//! codec stamps every payload it encodes with the signature of the converter context it was
//! given and refuses to decode a payload signed under another one; the failure converter
//! writes that signature into `Failure.source`. Features then assert the signatures history
//! recorded.
const std = @import("std");
const temporal = @import("temporal");
const Context = @import("Context.zig");
const history = @import("history.zig");

const converter = temporal.converter;
const Payload = converter.Payload;
const Allocator = std.mem.Allocator;

/// Payload metadata entry the codec stamps with the signature of the context it encoded under.
pub const metadata_key = "ctx-signature";
/// Signature of a conversion with no context: the vtable's `encode`/`decode`, or a context whose
/// namespace, workflow_id, workflow_type, activity_type and activity_id are all empty.
pub const no_context = "none";

/// "wf|<namespace>|<workflow_id>". Allocated from `arena`.
pub fn workflowSignature(arena: Allocator, namespace: []const u8, workflow_id: []const u8) error{OutOfMemory}![]const u8 {
    return std.fmt.allocPrint(arena, "wf|{s}|{s}", .{ namespace, workflow_id });
}

/// "act|<namespace>|<workflow_id>|<workflow_type>|<activity_type>|<activity_id>". Allocated from `arena`.
pub fn activitySignature(
    arena: Allocator,
    namespace: []const u8,
    workflow_id: []const u8,
    workflow_type: []const u8,
    activity_type: []const u8,
    activity_id: []const u8,
) error{OutOfMemory}![]const u8 {
    return std.fmt.allocPrint(arena, "act|{s}|{s}|{s}|{s}|{s}", .{ namespace, workflow_id, workflow_type, activity_type, activity_id });
}

/// The signature of `context`: `no_context` when every identity field is empty, the activity
/// form when activity_type or activity_id is non-empty, otherwise the workflow form.
/// `context.values` is ignored.
pub fn signature(arena: Allocator, context: *const converter.Context) error{OutOfMemory}![]const u8 {
    if (context.activity_type.len != 0 or context.activity_id.len != 0)
        return activitySignature(arena, context.namespace, context.workflow_id, context.workflow_type, context.activity_type, context.activity_id);
    if (context.namespace.len == 0 and context.workflow_id.len == 0 and context.workflow_type.len == 0) return no_context;
    return workflowSignature(arena, context.namespace, context.workflow_id);
}

// The codec and the failure converter keep no state of their own; `ptr` must point somewhere.
var state: u8 = 0;

/// The one-codec chain: `client.Options.codecs = &sercontext.codecs`. File-scope constant.
pub const codecs: [1]converter.PayloadCodec = .{.{
    .ptr = &state,
    .vtable = &.{
        .encode = encode,
        .decode = decode,
        .encodeWithContext = encodeWithContext,
        .decodeWithContext = decodeWithContext,
    },
}};

/// Writes its context's signature into `Failure.source` of failures the SDK has just built.
pub const failure_converter: converter.FailureConverter = .{
    .ptr = &state,
    .vtable = &.{ .toFailure = toFailure, .fromFailure = fromFailure },
};

/// Sets `options.codecs = &codecs` and `options.failure_converter = failure_converter`.
/// Used as `.configureClient = harness.sercontext.configureClient` in `harness.define`.
pub fn configureClient(options: *temporal.client.Options) void {
    options.codecs = &codecs;
    options.failure_converter = failure_converter;
}

/// The `ctx-signature` metadata value of `payload`; null when it has none.
pub fn signatureOf(payload: Payload) ?[]const u8 {
    for (payload.metadata.items) |entry| {
        if (std.mem.eql(u8, entry.key, metadata_key)) return entry.value;
    }
    return null;
}

/// `signatureOf` the first payload; null for a null or empty list.
pub fn firstSignature(payloads: ?converter.Payloads) ?[]const u8 {
    const list = payloads orelse return null;
    if (list.payloads.items.len == 0) return null;
    return signatureOf(list.payloads.items[0]);
}

/// Fails with `<what>: expected context signature "<expected>", got "<actual or (none)>"`.
pub fn expectSignature(ctx: *Context, what: []const u8, expected: []const u8, actual: ?[]const u8) error{FeatureFailed}!void {
    if (actual) |got| {
        if (std.mem.eql(u8, expected, got)) return;
        return ctx.fail("{s}: expected context signature \"{s}\", got \"{s}\"", .{ what, expected, got });
    }
    return ctx.fail("{s}: expected context signature \"{s}\", got \"(none)\"", .{ what, expected });
}

const Attributes = temporal.proto.temporal.api.history.v1.HistoryEvent.attributes_union;

/// The attributes of the first event that carries attributes of kind `tag`, or a failure
/// "history has no event with <tag>".
pub fn findAttributes(
    ctx: *Context,
    recorded: temporal.history.History,
    comptime tag: std.meta.Tag(Attributes),
) error{FeatureFailed}!@FieldType(Attributes, @tagName(tag)) {
    for (recorded.events.items) |event| {
        const attributes = event.attributes orelse continue;
        if (attributes == tag) return @field(attributes, @tagName(tag));
    }
    return ctx.fail("history has no event with {s}", .{@tagName(tag)});
}

/// `activitySignature` of the first activity `recorded` schedules: the feature's namespace,
/// `workflow_id`, the started workflow type, and the scheduled activity type and ID.
pub fn scheduledActivitySignature(ctx: *Context, recorded: temporal.history.History, workflow_id: []const u8) error{ FeatureFailed, OutOfMemory }![]const u8 {
    const started = try findAttributes(ctx, recorded, .workflow_execution_started_event_attributes);
    const scheduled = try findAttributes(ctx, recorded, .activity_task_scheduled_event_attributes);
    const workflow_type = if (started.workflow_type) |t| t.name else "";
    const activity_type = if (scheduled.activity_type) |t| t.name else "";
    return activitySignature(ctx.arena, ctx.namespace, workflow_id, workflow_type, activity_type, scheduled.activity_id);
}

/// Checks that a child's payloads carry `workflowSignature(namespace, child_id)` on both
/// sides, as the child_workflow_payloads features need: the parent's
/// `StartChildWorkflowExecutionInitiated` input and `ChildWorkflowExecutionCompleted` result,
/// and the child's own `WorkflowExecutionStarted` input. The child's signature must differ
/// from the parent's.
pub fn expectChildPayloads(ctx: *Context, parent_id: []const u8, parent: temporal.history.History, child_id: []const u8) (temporal.client.CallServiceError || error{ FeatureFailed, OutOfMemory })!void {
    const expected = try workflowSignature(ctx.arena, ctx.namespace, child_id);
    const parent_signature = try workflowSignature(ctx.arena, ctx.namespace, parent_id);
    if (std.mem.eql(u8, expected, parent_signature))
        return ctx.fail("child workflow ID {s} gives the parent's signature \"{s}\"", .{ child_id, parent_signature });
    const initiated = try findAttributes(ctx, parent, .start_child_workflow_execution_initiated_event_attributes);
    try expectSignature(ctx, "parent's StartChildWorkflowExecutionInitiated input", expected, firstSignature(initiated.input));
    const completed = try findAttributes(ctx, parent, .child_workflow_execution_completed_event_attributes);
    try expectSignature(ctx, "parent's ChildWorkflowExecutionCompleted result", expected, firstSignature(completed.result));
    const child = try ctx.fetchRawHistory(child_id, "");
    const started = try findAttributes(ctx, child, .workflow_execution_started_event_attributes);
    try expectSignature(ctx, "child's WorkflowExecutionStarted input", expected, firstSignature(started.input));
}

/// `Context.waitForEvent` over the stored payloads: polls the run's raw history until it has
/// an event of `event_type`, or fails the feature after `timeout`. `waitForEvent` reads the
/// history through the client's codecs, which refuse a payload a gap signed under the wrong
/// context.
pub fn waitForRawEvent(
    ctx: *Context,
    workflow_id: []const u8,
    run_id: []const u8,
    event_type: history.EventType,
    timeout: std.Io.Duration,
) (temporal.client.CallServiceError || error{FeatureFailed})!void {
    const io = ctx.io;
    const Io = std.Io;
    const deadline = Io.Clock.Timestamp.now(io, .boot).addDuration(.{ .raw = timeout, .clock = .boot });
    const Result = union(enum) { event: temporal.client.CallServiceError!void, timeout: Io.Cancelable!void };
    var buffer: [2]Result = undefined;
    var race: Io.Select(Result) = .init(io, &buffer);
    defer race.cancelDiscard();
    race.concurrent(.timeout, Io.Clock.Timestamp.wait, .{ deadline, io }) catch
        return ctx.fail("cannot start the timeout waiting for {t} in workflow {s} run {s}", .{ event_type, workflow_id, run_id });
    race.concurrent(.event, pollRawHistory, .{ ctx.client, io, ctx.gpa, ctx.namespace, workflow_id, run_id, event_type }) catch
        return ctx.fail("cannot start polling for {t} in workflow {s} run {s}", .{ event_type, workflow_id, run_id });
    switch (try race.await()) {
        .event => |result| return result,
        .timeout => |result| try result,
    }
    return ctx.fail("workflow {s} run {s} has no {t} event after {f}", .{ workflow_id, run_id, event_type, timeout });
}

fn pollRawHistory(
    client: *temporal.client.Client,
    io: std.Io,
    gpa: Allocator,
    namespace: []const u8,
    workflow_id: []const u8,
    run_id: []const u8,
    event_type: history.EventType,
) temporal.client.CallServiceError!void {
    var scratch: std.heap.ArenaAllocator = .init(gpa);
    defer scratch.deinit();
    while (true) {
        const recorded = try Context.readRawHistory(client, scratch.allocator(), namespace, workflow_id, run_id);
        if (history.contains(recorded, event_type)) return;
        _ = scratch.reset(.retain_capacity);
        try io.sleep(.fromMilliseconds(100), .boot);
    }
}

/// Fails the feature when the codec has refused a payload, naming both signatures, and with
/// ` (SDK gap: <gap>)` appended when `gap` names the bead of a known gap. Call it when
/// awaiting `what` returned an error, before returning that error.
pub fn checkMismatch(ctx: *Context, what: []const u8, gap: ?[]const u8) error{FeatureFailed}!void {
    const found = (mismatch(ctx.arena) catch return ctx.fail("{s} failed; out of memory reading the codec's mismatch", .{what})) orelse return;
    if (gap) |bead| return ctx.fail(
        "{s} failed: serialization context mismatch: payload encoded as \"{s}\", decoded as \"{s}\" (SDK gap: {s})",
        .{ what, found.encoded, found.decoding, bead },
    );
    return ctx.fail("{s} failed: serialization context mismatch: payload encoded as \"{s}\", decoded as \"{s}\"", .{ what, found.encoded, found.decoding });
}

/// A payload the codec refused to decode: it was encoded under `encoded` and offered under `decoding`.
pub const Mismatch = struct { encoded: []const u8, decoding: []const u8 };

/// The first refusal since the process started or `reset`, copied into `arena`; null when none.
pub fn mismatch(arena: Allocator) error{OutOfMemory}!?Mismatch {
    lock();
    defer slot.mutex.unlock();
    if (!slot.set) return null;
    return .{
        .encoded = try arena.dupe(u8, slot.encoded[0..slot.encoded_len]),
        .decoding = try arena.dupe(u8, slot.decoding[0..slot.decoding_len]),
    };
}

/// Clears the recorded mismatch. For unit tests.
pub fn reset() void {
    lock();
    defer slot.mutex.unlock();
    slot.set = false;
    slot.encoded_len = 0;
    slot.decoding_len = 0;
}

// The first refusal, kept without an allocator so nothing in it needs freeing. A signature
// longer than a buffer is truncated here only.
var slot: struct {
    mutex: std.atomic.Mutex = .unlocked,
    set: bool = false,
    encoded: [512]u8 = undefined,
    encoded_len: usize = 0,
    decoding: [512]u8 = undefined,
    decoding_len: usize = 0,
} = .{};

fn lock() void {
    while (!slot.mutex.tryLock()) std.atomic.spinLoopHint();
}

fn record(encoded: []const u8, decoding: []const u8) void {
    lock();
    defer slot.mutex.unlock();
    if (slot.set) return;
    slot.set = true;
    slot.encoded_len = @min(encoded.len, slot.encoded.len);
    @memcpy(slot.encoded[0..slot.encoded_len], encoded[0..slot.encoded_len]);
    slot.decoding_len = @min(decoding.len, slot.decoding.len);
    @memcpy(slot.decoding[0..slot.decoding_len], decoding[0..slot.decoding_len]);
}

fn encode(_: *anyopaque, arena: Allocator, _: std.Io, payloads: []const Payload) converter.CodecError![]Payload {
    return sign(arena, no_context, payloads);
}

fn encodeWithContext(_: *anyopaque, arena: Allocator, _: std.Io, context: *const converter.Context, payloads: []const Payload) converter.CodecError![]Payload {
    return sign(arena, try signature(arena, context), payloads);
}

fn decode(_: *anyopaque, arena: Allocator, _: std.Io, payloads: []const Payload) converter.CodecError![]Payload {
    return verify(arena, no_context, payloads);
}

fn decodeWithContext(_: *anyopaque, arena: Allocator, _: std.Io, context: *const converter.Context, payloads: []const Payload) converter.CodecError![]Payload {
    return verify(arena, try signature(arena, context), payloads);
}

/// A copy of `payload` whose metadata leaves out the signature entry, with room for one more.
fn withoutSignature(arena: Allocator, payload: Payload) error{OutOfMemory}!Payload {
    var copy = payload;
    copy.metadata = try .initCapacity(arena, payload.metadata.items.len + 1);
    for (payload.metadata.items) |entry| {
        if (!std.mem.eql(u8, entry.key, metadata_key)) copy.metadata.appendAssumeCapacity(entry);
    }
    return copy;
}

fn sign(arena: Allocator, sig: []const u8, payloads: []const Payload) converter.CodecError![]Payload {
    const output = try arena.alloc(Payload, payloads.len);
    for (payloads, output) |payload, *out| {
        out.* = try withoutSignature(arena, payload);
        out.metadata.appendAssumeCapacity(.{ .key = metadata_key, .value = sig });
    }
    return output;
}

fn verify(arena: Allocator, sig: []const u8, payloads: []const Payload) converter.CodecError![]Payload {
    const output = try arena.alloc(Payload, payloads.len);
    for (payloads, output) |payload, *out| {
        const encoded = signatureOf(payload) orelse {
            // Core's own payloads and most header fields were never encoded.
            out.* = payload;
            continue;
        };
        if (!std.mem.eql(u8, encoded, sig)) {
            record(encoded, sig);
            std.log.warn("serialization context mismatch: payload encoded as \"{s}\", decoded as \"{s}\"", .{ encoded, sig });
            return error.CodecFailed;
        }
        out.* = try withoutSignature(arena, payload);
    }
    return output;
}

fn toFailure(_: *anyopaque, arena: Allocator, context: *const converter.Context, _: ?anyerror, f: converter.Failure) error{OutOfMemory}!converter.Failure {
    // Only a failure the SDK has just built: one that already crossed the wire keeps its source.
    if (!std.mem.eql(u8, f.source, temporal.failure.source)) return f;
    var out = f;
    out.source = try signature(arena, context);
    return out;
}

fn fromFailure(_: *anyopaque, _: Allocator, _: *const converter.Context, f: converter.Failure) error{OutOfMemory}!converter.Failure {
    return f;
}

test "sercontext signatures name the workflow, the activity, or no context" {
    var arena_state: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    try std.testing.expectEqualStrings("wf|ns|wid", try workflowSignature(arena, "ns", "wid"));
    try std.testing.expectEqualStrings("act|ns|wid|WT|AT|1", try activitySignature(arena, "ns", "wid", "WT", "AT", "1"));
    try std.testing.expectEqualStrings(no_context, try signature(arena, &.{}));
    try std.testing.expectEqualStrings("wf|ns|wid", try signature(arena, &.{ .namespace = "ns", .workflow_id = "wid", .workflow_type = "WT" }));
    try std.testing.expectEqualStrings("wf|ns|", try signature(arena, &.{ .namespace = "ns" }));
    try std.testing.expectEqualStrings("act|ns|wid|WT|AT|1", try signature(arena, &.{
        .namespace = "ns",
        .workflow_id = "wid",
        .workflow_type = "WT",
        .activity_type = "AT",
        .activity_id = "1",
    }));
    try std.testing.expectEqualStrings("act|ns|wid|||7", try signature(arena, &.{ .namespace = "ns", .workflow_id = "wid", .activity_id = "7" }));
}

test "sercontext codec stamps and replaces the signature, and decode strips it" {
    var arena_state: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const io = std.testing.io;
    const context: converter.Context = .{ .namespace = "ns", .workflow_id = "wid" };
    var plain = try converter.payloadWithEncoding(arena, "json/plain", "\"x\"");
    try plain.metadata.append(arena, .{ .key = metadata_key, .value = "stale" });

    const encoded = try codecs[0].encodeWithContext(arena, io, &context, &.{plain});
    try std.testing.expectEqual(1, encoded.len);
    try std.testing.expectEqual(2, encoded[0].metadata.items.len);
    try std.testing.expectEqualStrings("wf|ns|wid", signatureOf(encoded[0]).?);
    try std.testing.expectEqualStrings("json/plain", converter.encodingOf(encoded[0]).?);
    try std.testing.expectEqualStrings("stale", signatureOf(plain).?);

    const decoded = try codecs[0].decodeWithContext(arena, io, &context, encoded);
    try std.testing.expectEqual(null, signatureOf(decoded[0]));
    try std.testing.expectEqualStrings("json/plain", converter.encodingOf(decoded[0]).?);
    try std.testing.expectEqualStrings("\"x\"", decoded[0].data);

    const bare = try codecs[0].encode(arena, io, &.{decoded[0]});
    try std.testing.expectEqualStrings(no_context, signatureOf(bare[0]).?);
    try std.testing.expectEqual(null, signatureOf((try codecs[0].decode(arena, io, bare))[0]));

    var list: converter.Payloads = .{};
    try std.testing.expectEqual(null, firstSignature(null));
    try std.testing.expectEqual(null, firstSignature(list));
    try list.payloads.append(arena, encoded[0]);
    try std.testing.expectEqualStrings("wf|ns|wid", firstSignature(list).?);
}

test "sercontext codec passes an unsigned payload through decode" {
    reset();
    defer reset();
    var arena_state: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const plain = try converter.payloadWithEncoding(arena, "binary/plain", "raw");
    const decoded = try codecs[0].decodeWithContext(arena, std.testing.io, &.{ .namespace = "ns", .workflow_id = "wid" }, &.{plain});
    try std.testing.expectEqualDeep(plain, decoded[0]);
    try std.testing.expectEqual(null, try mismatch(arena));
}

test "sercontext codec refuses another context's payload and records the first mismatch" {
    reset();
    defer reset();
    var arena_state: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const io = std.testing.io;
    const plain = try converter.payloadWithEncoding(arena, "json/plain", "1");
    const sender: converter.Context = .{ .namespace = "ns", .workflow_id = "sender" };
    const receiver: converter.Context = .{ .namespace = "ns", .workflow_id = "receiver" };
    const encoded = try codecs[0].encodeWithContext(arena, io, &sender, &.{plain});
    try std.testing.expectError(error.CodecFailed, codecs[0].decodeWithContext(arena, io, &receiver, encoded));
    try std.testing.expectError(error.CodecFailed, codecs[0].decode(arena, io, encoded));
    const found = (try mismatch(arena)).?;
    try std.testing.expectEqualStrings("wf|ns|sender", found.encoded);
    try std.testing.expectEqualStrings("wf|ns|receiver", found.decoding);

    reset();
    try std.testing.expectEqual(null, try mismatch(arena));

    const long: [600]u8 = @splat('x');
    const signed_long = try codecs[0].encodeWithContext(arena, io, &.{ .namespace = "ns", .workflow_id = &long }, &.{plain});
    try std.testing.expectError(error.CodecFailed, codecs[0].decode(arena, io, signed_long));
    const truncated = (try mismatch(arena)).?;
    try std.testing.expectEqual(512, truncated.encoded.len);
    try std.testing.expectEqualStrings(no_context, truncated.decoding);
}

test "sercontext failure converter overwrites only the SDK's own source" {
    var arena_state: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const context: converter.Context = .{ .namespace = "ns", .workflow_id = "wid", .workflow_type = "WT", .activity_type = "AT", .activity_id = "1" };
    var cause: converter.Failure = .{ .message = "cause", .source = temporal.failure.source };
    const fresh: converter.Failure = .{ .message = "m", .source = temporal.failure.source, .cause = &cause };
    const converted = try failure_converter.toFailure(arena, &context, error.Example, fresh);
    try std.testing.expectEqualStrings("act|ns|wid|WT|AT|1", converted.source);
    try std.testing.expectEqualStrings("m", converted.message);
    try std.testing.expectEqualStrings(temporal.failure.source, converted.cause.?.source);

    const foreign: converter.Failure = .{ .message = "m", .source = "GoSDK" };
    try std.testing.expectEqualStrings("GoSDK", (try failure_converter.toFailure(arena, &context, null, foreign)).source);
    try std.testing.expectEqualStrings(temporal.failure.source, (try failure_converter.fromFailure(arena, &context, fresh)).source);

    var options: temporal.client.Options = .{};
    configureClient(&options);
    try std.testing.expectEqual(&codecs[0], &options.codecs[0]);
    try std.testing.expectEqual(failure_converter.vtable, options.failure_converter.?.vtable);
}

test "sercontext expectSignature and findEvent report what is missing" {
    var arena_state: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena_state.deinit();
    var ctx: Context = .{
        .gpa = std.testing.allocator,
        .io = std.testing.io,
        .arena = arena_state.allocator(),
        .client = undefined,
        .feature = "serialization_context/test",
        .namespace = "default",
        .task_queue = "conformance-serialization_context/test",
    };
    try expectSignature(&ctx, "start input", "wf|a|b", "wf|a|b");
    try std.testing.expectError(error.FeatureFailed, expectSignature(&ctx, "start input", "wf|a|b", "wf|a|c"));
    try std.testing.expectEqualStrings("start input: expected context signature \"wf|a|b\", got \"wf|a|c\"", ctx.failure_message_.?);
    try std.testing.expectError(error.FeatureFailed, expectSignature(&ctx, "result", "wf|a|b", null));
    try std.testing.expectEqualStrings("result: expected context signature \"wf|a|b\", got \"(none)\"", ctx.failure_message_.?);
}

test "sercontext findAttributes and scheduledActivitySignature read the recorded events" {
    var arena_state: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena_state.deinit();
    var ctx: Context = .{
        .gpa = std.testing.allocator,
        .io = std.testing.io,
        .arena = arena_state.allocator(),
        .client = undefined,
        .feature = "serialization_context/test",
        .namespace = "default",
        .task_queue = "conformance-serialization_context/test",
    };
    var events = [_]temporal.history.HistoryEvent{
        .{
            .event_id = 1,
            .event_type = .EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
            .attributes = .{ .workflow_execution_started_event_attributes = .{ .workflow_type = .{ .name = "Workflow" } } },
        },
        .{ .event_id = 2, .event_type = .EVENT_TYPE_WORKFLOW_TASK_COMPLETED },
        .{
            .event_id = 3,
            .event_type = .EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
            .attributes = .{ .activity_task_scheduled_event_attributes = .{ .activity_id = "1", .activity_type = .{ .name = "Activity" } } },
        },
        .{
            .event_id = 4,
            .event_type = .EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
            .attributes = .{ .activity_task_scheduled_event_attributes = .{ .activity_id = "2", .activity_type = .{ .name = "Other" } } },
        },
    };
    const recorded: temporal.history.History = .{ .events = .fromOwnedSlice(&events) };
    try std.testing.expectEqualStrings("1", (try findAttributes(&ctx, recorded, .activity_task_scheduled_event_attributes)).activity_id);
    try std.testing.expectEqualStrings("act|default|wid|Workflow|Activity|1", try scheduledActivitySignature(&ctx, recorded, "wid"));
    try std.testing.expectError(error.FeatureFailed, findAttributes(&ctx, recorded, .activity_task_completed_event_attributes));
    try std.testing.expectEqualStrings("history has no event with activity_task_completed_event_attributes", ctx.failure_message_.?);
    try std.testing.expectError(error.FeatureFailed, scheduledActivitySignature(&ctx, .{}, "wid"));
    try std.testing.expectEqualStrings("history has no event with workflow_execution_started_event_attributes", ctx.failure_message_.?);
}

test "sercontext checkMismatch fails only after a refusal, naming the gap" {
    reset();
    defer reset();
    var arena_state: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    var ctx: Context = .{
        .gpa = std.testing.allocator,
        .io = std.testing.io,
        .arena = arena,
        .client = undefined,
        .feature = "serialization_context/test",
        .namespace = "default",
        .task_queue = "conformance-serialization_context/test",
    };
    try checkMismatch(&ctx, "workflow result", "temporal-sdk-zig-example");
    try std.testing.expectEqual(null, ctx.failure_message_);

    const plain = try converter.payloadWithEncoding(arena, "json/plain", "1");
    const signed = try codecs[0].encodeWithContext(arena, std.testing.io, &.{ .namespace = "ns", .workflow_id = "a" }, &.{plain});
    try std.testing.expectError(error.CodecFailed, codecs[0].decodeWithContext(arena, std.testing.io, &.{ .namespace = "ns", .workflow_id = "b" }, signed));
    try std.testing.expectError(error.FeatureFailed, checkMismatch(&ctx, "workflow result", null));
    try std.testing.expectEqualStrings("workflow result failed: serialization context mismatch: payload encoded as \"wf|ns|a\", decoded as \"wf|ns|b\"", ctx.failure_message_.?);
    try std.testing.expectError(error.FeatureFailed, checkMismatch(&ctx, "receiver result", "temporal-sdk-zig-example"));
    try std.testing.expectEqualStrings("receiver result failed: serialization context mismatch: payload encoded as \"wf|ns|a\", decoded as \"wf|ns|b\" (SDK gap: temporal-sdk-zig-example)", ctx.failure_message_.?);
}
