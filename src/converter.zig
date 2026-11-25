//! Payload conversion.
//!
//! Values cross the wire as `temporal.api.common.v1.Payload`: bytes plus an `encoding`
//! metadata entry. The default converter uses Temporal's standard encodings, so workers and
//! clients in any language understand each other:
//!
//! | Zig value                         | encoding          | data                       |
//! |-----------------------------------|-------------------|----------------------------|
//! | `void`, `null`                    | `binary/null`     | empty                      |
//! | `converter.Bytes`                 | `binary/plain`    | the bytes                  |
//! | a generated protobuf message      | `json/protobuf`   | ProtoJSON                  |
//! | `converter.ProtoBinary(T)`        | `binary/protobuf` | the message's wire format  |
//! | `converter.Payload`               | its own           | as it is (already encoded) |
//! | anything else (strings included)  | `json/plain`      | JSON from `std.json`       |
//!
//! Strings are JSON strings; use `Bytes` for raw binary data. Decoding
//! accepts any of these encodings into a compatible type: a protobuf message decodes from
//! `json/protobuf` and `binary/protobuf`. Protobuf payloads also carry a `messageType`
//! metadata entry with the message's full name (see `Proto`). Decoded slices and strings are
//! allocated from the arena passed in.
//!
//! A type converts itself by declaring both of
//!
//!     pub fn toPayload(self: T, arena: Allocator) error{OutOfMemory}!Payload
//!     pub fn fromPayload(arena: Allocator, p: Payload) DecodeError!T
//!
//! which take precedence over the defaults, for the type and for optionals of it. For
//! example, a point sent as 8 little-endian bytes:
//!
//!     const Point = struct {
//!         x: i32,
//!         y: i32,
//!
//!         pub fn toPayload(p: Point, arena: Allocator) error{OutOfMemory}!converter.Payload {
//!             const data = try arena.alloc(u8, 8);
//!             std.mem.writeInt(i32, data[0..4], p.x, .little);
//!             std.mem.writeInt(i32, data[4..8], p.y, .little);
//!             return converter.payloadWithEncoding(arena, "binary/point", data);
//!         }
//!
//!         pub fn fromPayload(_: Allocator, p: converter.Payload) converter.DecodeError!Point {
//!             const enc = converter.encodingOf(p) orelse return error.UnknownEncoding;
//!             if (!std.mem.eql(u8, enc, "binary/point")) return error.UnknownEncoding;
//!             if (p.data.len != 8) return error.InvalidPayload;
//!             return .{
//!                 .x = std.mem.readInt(i32, p.data[0..4], .little),
//!                 .y = std.mem.readInt(i32, p.data[4..8], .little),
//!             };
//!         }
//!     };
//!
//! A type that needs to know what its value is for declares both of
//!
//!     pub fn toPayloadWithContext(self: T, arena: Allocator, context: *const Context) error{OutOfMemory}!Payload
//!     pub fn fromPayloadWithContext(arena: Allocator, context: *const Context, p: Payload) DecodeError!T
//!
//! instead, or as well: they take precedence over `toPayload` and `fromPayload`, with the same
//! reach (the type and optionals of it, at the top level of a payload). Declaring only one of
//! the two is a compile error. `Context` is what payload codecs receive too: the namespace,
//! the workflow ID and type, for an activity's values the activity type and ID, for a Nexus
//! operation's values its endpoint, service and operation (`nexus`), and the propagated
//! values. `toPayloadWithContext`, `fromPayloadWithContext`,
//! `toPayloadsWithContext` and `fromPayloadsWithContext` convert with one; `toPayload`,
//! `fromPayload`, `toPayloads` and `fromPayloads` pass the empty context (every string empty,
//! no values). The SDK passes the context of the call a value belongs to:
//!
//! | Conversion                                                    | Context                                      |
//! |---------------------------------------------------------------|----------------------------------------------|
//! | client: workflow start, signal-with-start, update-with-start | namespace, workflow ID and type, `.context`  |
//! | client: signal, query, update, terminate details, results    | namespace, workflow ID, `.context`           |
//! | client: standalone activity arguments and result             | namespace, activity ID (and type when known) |
//! | client: `...ById` activity calls                             | namespace, workflow ID, activity ID          |
//! | client: activity calls by task token                         | namespace                                    |
//! | workflow: arguments, result, signals, queries, updates,      | the run's (`workflow.Context.converterContext`) |
//! |   side effects, `lastCompletionResult`                       |                                              |
//! | workflow: continue-as-new arguments                          | the run's, with the new workflow type        |
//! | workflow: activity and local activity arguments and results  | the run's, with the activity type and ID     |
//! | workflow: child arguments, result and signals                | namespace, the child's workflow ID and type  |
//! | workflow: external signal arguments                          | namespace, the target workflow ID            |
//! | workflow and Nexus handler: Nexus input, result and failure  | namespace, propagated values, `nexus`        |
//! | activity: arguments                                          | the activity's (`activity.Context.converterContext`) |
//! | activity: result, failure, heartbeat details                 | `activity.Context.resultConverterContext`    |
//! | run, update or activity a Nexus handler started: result,     | namespace, propagated values, the caller's   |
//! |   failure (an activity's heartbeat details too)              |   `nexus`, when the server propagated it     |
//!
//! Payload codecs on the workflow's worker get the run's context for its whole activation
//! and completion, except an activity's, local activity's or child's payloads: the schedule
//! or start command (arguments, header fields marked for the codecs, memo, summary) and the
//! resolutions (result and failure) get the same context as that call's hooks, with the
//! scheduled activity type and ID, or the child's workflow ID and type. So the activity's worker and the child's own
//! worker decode with the context the workflow's worker encoded with, and the reverse.
//! A Nexus operation's schedule command (input and summary) and its resolutions get the
//! operation's context, with `nexus` set and no workflow fields, as the handler's worker
//! decodes and encodes them; the operation a signal-with-start schedules keeps the run's.
//! Signals to other workflows, children included, keep the run's context for codecs. A side
//! effect's value is the run's own although an internal local activity records it: codecs
//! code it with the run's identity, without propagated values, on both workers.
//!
//! These have no context, and a context hook receives the empty one: user code calling
//! `toPayload`, `fromPayload`, `toPayloads`, `fromPayloads`, `toFields` or `fieldValue`; an
//! `EncodedValues` or `EncodedValue` built without one, including those from `failure.kind`;
//! memos; search attributes; headers and propagators (`HeaderWriter.set`, `Header.value`);
//! summaries and details in user metadata; schedule action arguments; and the codec server.
//! Failure details are user code's to convert: encode them with
//! `toPayloadWithContext(arena, ctx.converterContext(), value)` and decode them with
//! `details.withContext(future.converterContext())`.
//!
//! The two sides of a value don't always agree. The workflow and activity types are set only
//! where the caller knows them. An activity's arguments are encoded before the outbound
//! interceptors and before it is scheduled: their activity ID is `ActivityOptions.activity_id`,
//! or the ID the SDK will assign, which differs if an interceptor changes the ID or schedules
//! another activity first. A local activity retried after a backoff timer is scheduled under a
//! new ID while its arguments stay as first encoded. A child's arguments carry an empty
//! workflow ID when the SDK generates the ID. Completion by task token carries the namespace
//! only, and a Nexus handler may be in another namespace. Set `activity_id` and
//! `ChildWorkflowOptions.id` when a hook depends on them.
//!
//! Context hooks run inside workflow code when a workflow converts, so they must be
//! deterministic and must not block or do I/O. They run concurrently on workflow, activity
//! and client threads, so they must not rely on unsynchronized shared state. The context is
//! borrowed for the call: copy anything a hook keeps.
//!
//! Conversion turns values into payloads; payload codecs (`PayloadCodec`) then transform
//! the payloads themselves, for example to compress or encrypt them. `codecHandler` serves
//! codecs over HTTP, so the Web UI and CLI can show payloads only the codecs can decode.
//! `RemoteCodec` is the client side of `codecHandler`: a codec that has a codec server do
//! the work.
//!
//! Errors become failures (`Failure`) by a fixed step: an error's name, or the
//! `failure.Application` given to `ctx.fail`. A `FailureConverter`, set with
//! `client.Options.failure_converter`, then transforms each failure user code sends and each
//! failure it receives, before user code reads it.

const std = @import("std");
const proto = @import("proto.zig");
const protojson = @import("protojson.zig");
const proto_time = @import("proto_time.zig");
const Allocator = std.mem.Allocator;

pub const Payload = proto.temporal.api.common.v1.Payload;
pub const Payloads = proto.temporal.api.common.v1.Payloads;

const codec = @import("converter/codec.zig");
pub const CodecError = codec.CodecError;
pub const PayloadCodec = codec.PayloadCodec;
pub const Context = codec.Context;
pub const NexusContext = codec.NexusContext;
/// Deprecated: use `Context`.
pub const CodecContext = Context;
pub const ZlibCodec = codec.ZlibCodec;
pub const encodePayloads = codec.encodePayloads;
pub const decodePayloads = codec.decodePayloads;
pub const encodePayloadsWithContext = codec.encodePayloadsWithContext;
pub const decodePayloadsWithContext = codec.decodePayloadsWithContext;
pub const encodeMessage = codec.encodeMessage;
pub const decodeMessage = codec.decodeMessage;
pub const visitPayloads = codec.visitPayloads;
const codec_handler = @import("converter/codec_handler.zig");
pub const CodecHandler = codec_handler.CodecHandler;
pub const CodecHandlerOptions = codec_handler.CodecHandlerOptions;
pub const codecHandler = codec_handler.codecHandler;
const remote_codec = @import("converter/remote_codec.zig");
pub const RemoteCodec = remote_codec.RemoteCodec;
pub const RemoteCodecOptions = remote_codec.RemoteCodecOptions;

/// A Temporal failure, as `failure.Failure`.
pub const Failure = proto.temporal.api.failure.v1.Failure;

/// Transforms failures on their way between user code and the server, for example to
/// rewrite their messages, move details into payloads, or redact them. Set it with
/// `client.Options.failure_converter`; the client, every worker on it, and the replayer
/// (`testing.WorkflowReplayer.Options.failure_converter`) use it. Without one, failures are
/// sent and read as the SDK builds and receives them.
///
/// `toFailure` runs where an error of user code becomes a failure: the workflow's own
/// failure (its function's or a signal handler's error, `ctx.fail`, `ctx.failWith`, or a
/// future's failure it returns), update rejections from validators and handlers, query
/// failures, the failures of activities and local activities, a Nexus handler's error, and
/// `Client.failActivity` and `failActivityById`. `f` is the failure the SDK built, stack trace
/// included, and `err` the error user code returned (null only for the two client calls). It
/// returns the failure to send. `fromFailure` runs where a failure arrives, before user code
/// reads it: in a workflow, the failures of activities, local activities, child workflows,
/// Nexus operations, external signals and cancels, and `ctx.previousRunFailure`; on a
/// client, the failures `WorkflowRun.failure`, `WorkflowUpdateHandle.failure` and
/// `ActivityHandle.failure` return. Each is called once per failure with the head of its
/// cause chain; the causes are the converter's to visit.
///
/// `context` names what the failure is for, as for payloads (see the table above), and is
/// borrowed for the call. The failure returned may borrow from `f`; anything else comes from
/// `arena`, which lives as long as the failure does.
///
/// Failures the SDK makes for its own reports (workflow task failures, unregistered
/// activities, queries and updates, codec failures, cancel acknowledgements) and those it
/// builds for user code without a server (an interceptor's refusal, a child that couldn't
/// start, a canceled local activity) are not converted. `client.Options.encode_failure_attributes`
/// applies after `toFailure` on the way out and is undone before `fromFailure` on the way in.
///
/// The converter runs inside workflow code when a workflow sends or receives a failure, so it
/// must be deterministic and must not block or do I/O: a converter that blocks is reported as
/// a deadlock. It is called concurrently from workflow, activity, Nexus and client threads,
/// so it must be thread-safe. Borrowed: `ptr` and `vtable` must outlive the client, its
/// workers, and any replayer given it.
pub const FailureConverter = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        toFailure: *const fn (ptr: *anyopaque, arena: Allocator, context: *const Context, err: ?anyerror, f: Failure) error{OutOfMemory}!Failure,
        fromFailure: *const fn (ptr: *anyopaque, arena: Allocator, context: *const Context, f: Failure) error{OutOfMemory}!Failure,
    };

    pub fn toFailure(c: FailureConverter, arena: Allocator, context: *const Context, err: ?anyerror, f: Failure) error{OutOfMemory}!Failure {
        return c.vtable.toFailure(c.ptr, arena, context, err, f);
    }

    pub fn fromFailure(c: FailureConverter, arena: Allocator, context: *const Context, f: Failure) error{OutOfMemory}!Failure {
        return c.vtable.fromFailure(c.ptr, arena, context, f);
    }
};

/// Raw bytes, encoded as `binary/plain` instead of a JSON string.
pub const Bytes = struct { data: []const u8 };

pub const encoding_key = "encoding";
/// Metadata key of a protobuf payload's full message name, such as
/// "temporal.api.common.v1.WorkflowExecution".
pub const message_type_key = "messageType";

pub const Encoding = enum {
    null,
    binary,
    json,
    proto_json,
    proto_binary,

    pub fn text(e: Encoding) []const u8 {
        return switch (e) {
            .null => "binary/null",
            .binary => "binary/plain",
            .json => "json/plain",
            .proto_json => "json/protobuf",
            .proto_binary => "binary/protobuf",
        };
    }
};

pub const DecodeError = error{ OutOfMemory, UnknownEncoding, InvalidPayload, WrongArgumentCount };

/// The payload's `encoding` metadata value, or null if it has none.
pub fn encodingOf(p: Payload) ?[]const u8 {
    for (p.metadata.items) |entry| {
        if (std.mem.eql(u8, entry.key, encoding_key)) return entry.value;
    }
    return null;
}

/// Encodes one value. Memory comes from `arena`. A type's `toPayloadWithContext` hook
/// receives the empty context.
pub fn toPayload(arena: Allocator, value: anytype) error{OutOfMemory}!Payload {
    return toPayloadWithContext(arena, &.{}, value);
}

/// Encodes one value for the call `context` describes, which a type's
/// `toPayloadWithContext` hook receives; every other encoding ignores it. Memory comes from
/// `arena`; `context` is borrowed for the call.
///
/// A `proto.google.protobuf.Timestamp` inside a plain value, directly or in a
/// `std.json.ArrayHashMap`, is written as RFC 3339 text; one outside protobuf's range makes
/// the call fail. A type with its own `jsonStringify` writes what it holds itself.
pub fn toPayloadWithContext(arena: Allocator, context: *const Context, value: anytype) error{OutOfMemory}!Payload {
    const T = @TypeOf(value);
    if (T == Payload) return value;
    if (T == void or T == @TypeOf(null)) return payloadWith(arena, .null, "");
    if (@typeInfo(T) == .optional) {
        if (value) |v| return toPayloadWithContext(arena, context, v);
        return payloadWith(arena, .null, "");
    }
    if (comptime hasContextHooks(T)) return value.toPayloadWithContext(arena, context);
    if (comptime hasPayloadHooks(T)) return value.toPayload(arena);
    if (T == Bytes) return payloadWith(arena, .binary, value.data);
    if (comptime isProtoMessage(T)) return Proto(T, .{}).encode(arena, value);
    // zig-protobuf's own Timestamp writer panics on 29 February, so a type that reaches one
    // is written through `writeJson`; every other type keeps std.json's output as it is.
    const json = if (comptime holdsTimestamp(T))
        try std.json.Stringify.valueAlloc(arena, TimestampJson(T){ .value = &value }, .{})
    else
        try std.json.Stringify.valueAlloc(arena, value, .{});
    return payloadWith(arena, .json, json);
}

/// Decodes one value of type `T`. Memory comes from `arena`. A type's
/// `fromPayloadWithContext` hook receives the empty context.
pub fn fromPayload(comptime T: type, arena: Allocator, p: Payload) DecodeError!T {
    return fromPayloadWithContext(T, arena, &.{}, p);
}

/// Decodes one value of type `T` for the call `context` describes, which a type's
/// `fromPayloadWithContext` hook receives; every other encoding ignores it. Memory comes
/// from `arena`; `context` is borrowed for the call.
pub fn fromPayloadWithContext(comptime T: type, arena: Allocator, context: *const Context, p: Payload) DecodeError!T {
    if (T == Payload) return try p.dupe(arena);
    if (comptime hasContextHooks(T)) return T.fromPayloadWithContext(arena, context, p);
    if (comptime hasPayloadHooks(T)) return T.fromPayload(arena, p);
    const enc = encodingOf(p) orelse return error.UnknownEncoding;
    if (std.mem.eql(u8, enc, Encoding.null.text())) {
        if (T == void) return {};
        if (@typeInfo(T) == .optional) return null;
        return error.InvalidPayload;
    }
    if (@typeInfo(T) == .optional) {
        const Child = @typeInfo(T).optional.child;
        if (comptime hasContextHooks(Child) or hasPayloadHooks(Child) or isProtoMessage(Child))
            return try fromPayloadWithContext(Child, arena, context, p);
        // std.json parses an optional itself, so a JSON `null` decodes to null. Any other
        // encoding holds a value of the child, such as `binary/plain` for `?Bytes`.
        if (!std.mem.eql(u8, enc, Encoding.json.text()))
            return try fromPayloadWithContext(Child, arena, context, p);
    }
    if (comptime isProtoMessage(T)) return Proto(T, .{}).decode(arena, p);
    if (std.mem.eql(u8, enc, Encoding.binary.text())) {
        if (T == Bytes) return .{ .data = try arena.dupe(u8, p.data) };
        if (T == []const u8 or T == []u8) return arena.dupe(u8, p.data);
        return error.InvalidPayload;
    }
    if (std.mem.eql(u8, enc, Encoding.json.text())) {
        if (T == void) return {};
        return std.json.parseFromSliceLeaky(T, arena, p.data, .{
            .allocate = .alloc_always,
            .ignore_unknown_fields = true,
        }) catch |err| switch (err) {
            error.OutOfMemory => error.OutOfMemory,
            else => error.InvalidPayload,
        };
    }
    return error.UnknownEncoding;
}

/// Encodes each element of the tuple `args` (workflow and activity arguments). Context
/// hooks receive the empty context.
pub fn toPayloads(arena: Allocator, args: anytype) error{OutOfMemory}!std.ArrayList(Payload) {
    return toPayloadsWithContext(arena, &.{}, args);
}

/// `toPayloads` for the call `context` describes, as `toPayloadWithContext`.
pub fn toPayloadsWithContext(arena: Allocator, context: *const Context, args: anytype) error{OutOfMemory}!std.ArrayList(Payload) {
    var out: std.ArrayList(Payload) = .empty;
    inline for (args) |arg| try out.append(arena, try toPayloadWithContext(arena, context, arg));
    return out;
}

/// Decodes payloads into a tuple of type `Args`. Missing trailing optional arguments decode
/// as null; extra payloads are ignored. Context hooks receive the empty context.
pub fn fromPayloads(comptime Args: type, arena: Allocator, payloads: []const Payload) DecodeError!Args {
    return fromPayloadsWithContext(Args, arena, &.{}, payloads);
}

/// `fromPayloads` for the call `context` describes, as `fromPayloadWithContext`.
pub fn fromPayloadsWithContext(comptime Args: type, arena: Allocator, context: *const Context, payloads: []const Payload) DecodeError!Args {
    var out: Args = undefined;
    inline for (@typeInfo(Args).@"struct".field_types, 0..) |F, i| {
        if (i < payloads.len) {
            out[i] = try fromPayloadWithContext(F, arena, context, payloads[i]);
        } else if (@typeInfo(F) == .optional) {
            out[i] = null;
        } else {
            return error.WrongArgumentCount;
        }
    }
    return out;
}

pub const Direction = enum { encode, decode };

/// The type that stops `toPayload` (`.encode`) or `fromPayload` (`.decode`) from compiling
/// for `T`, or null if it compiles: `T` itself, or a type inside it that `std.json` can't
/// handle, such as a function pointer, an `anyopaque`, or an untagged union. Lets callers
/// reject a type with an error that names it instead of one deep inside `std.json`.
pub fn unconvertibleType(comptime T: type, comptime direction: Direction) ?type {
    if (T == Payload or T == void or T == @TypeOf(null) or T == Bytes) return null;
    if (comptime hasContextHooks(T) or hasPayloadHooks(T) or isProtoMessage(T)) return null;
    if (@typeInfo(T) == .optional) return unconvertibleType(@typeInfo(T).optional.child, direction);
    return jsonUnconvertible(T, direction);
}

// Mirrors the type switches of std.json's Stringify.write and innerParse. A depth-first
// worklist that visits each type once, so shared subtypes are walked once and recursive types
// terminate. Callers check many methods in one comptime evaluation, so the quota is raised
// with a wide base plus the walk's own work rather than left at the default 1000.
fn jsonUnconvertible(comptime Root: type, comptime direction: Direction) ?type {
    comptime {
        const hook = if (direction == .encode) "jsonStringify" else "jsonParse";
        var visited: []const type = &.{};
        var stack: []const type = &.{Root};
        var work: usize = 0;
        while (stack.len > 0) {
            work += visited.len + 8;
            @setEvalBranchQuota(100_000 + 8 * work);
            const T = stack[stack.len - 1];
            stack = stack[0 .. stack.len - 1];
            if (std.mem.indexOfScalar(type, visited, T) != null) continue;
            visited = visited ++ .{T};
            var children: []const type = &.{};
            switch (@typeInfo(T)) {
                .bool, .int, .float, .@"enum" => {},
                .optional => |o| children = &.{o.child},
                .error_set => if (direction == .decode) return T,
                .array => |a| children = &.{a.child},
                .vector => |v| children = &.{v.child},
                .pointer => |p| switch (p.size) {
                    .one, .slice => children = &.{p.child},
                    .many => if (direction == .encode and p.sentinel_ptr != null) {
                        children = &.{p.child};
                    } else return T,
                    .c => return T,
                },
                .@"union" => |u| if (!std.meta.hasFn(T, hook)) {
                    if (u.tag_type == null) return T;
                    for (u.field_types) |F| {
                        if (F != void) children = children ++ .{F};
                    }
                },
                .@"struct" => |s| if (!std.meta.hasFn(T, hook)) {
                    for (s.field_types, s.field_attrs) |F, attrs| {
                        if (direction == .decode and attrs.@"comptime" and !s.is_tuple) return T;
                        if (F == void) {
                            if (direction == .decode) return F;
                            continue;
                        }
                        children = children ++ .{F};
                    }
                },
                else => return T,
            }
            work += children.len;
            // Pushed in reverse so the first field is walked first, as a recursive walk would.
            var i = children.len;
            while (i > 0) {
                i -= 1;
                stack = stack ++ .{children[i]};
            }
        }
        return null;
    }
}

// Whether std.json, writing `T`, reaches `proto.google.protobuf.Timestamp`. Walks the type
// as `jsonUnconvertible` does, stopping at any other type that writes itself.
fn holdsTimestamp(comptime Root: type) bool {
    comptime {
        var visited: []const type = &.{};
        var stack: []const type = &.{Root};
        var work: usize = 0;
        while (stack.len > 0) {
            work += visited.len + 8;
            @setEvalBranchQuota(100_000 + 8 * work);
            const T = stack[stack.len - 1];
            stack = stack[0 .. stack.len - 1];
            if (T == proto.google.protobuf.Timestamp) return true;
            if (std.mem.indexOfScalar(type, visited, T) != null) continue;
            visited = visited ++ .{T};
            var children: []const type = &.{};
            switch (@typeInfo(T)) {
                .optional => |o| children = &.{o.child},
                .array => |a| children = &.{a.child},
                .vector => |v| children = &.{v.child},
                .pointer => |p| switch (p.size) {
                    .one, .slice => children = &.{p.child},
                    .many => if (p.sentinel_ptr != null) {
                        children = &.{p.child};
                    },
                    .c => {},
                },
                .@"union" => |u| if (u.tag_type != null and !std.meta.hasFn(T, "jsonStringify")) {
                    children = u.field_types;
                },
                .@"struct" => |s| if (jsonMapValue(T)) |V| {
                    children = &.{V};
                } else if (!std.meta.hasFn(T, "jsonStringify")) {
                    children = s.field_types;
                },
                else => {},
            }
            work += children.len;
            var i = children.len;
            while (i > 0) {
                i -= 1;
                stack = stack ++ .{children[i]};
            }
        }
        return false;
    }
}

// The value type `V` when `T` is `std.json.ArrayHashMap(V)`, else null. That map writes its
// values with `jws.write`, so a Timestamp in it would reach the dependency's writer; it is
// the one self-writing type in std or zig-protobuf that can hold a Timestamp.
fn jsonMapValue(comptime T: type) ?type {
    if (@typeInfo(T) != .@"struct" or !@hasField(T, "map")) return null;
    const M = @FieldType(T, "map");
    if (@typeInfo(M) != .@"struct" or !@hasDecl(M, "Data")) return null;
    if (@TypeOf(M.Data) != type) return null;
    if (@typeInfo(M.Data) != .@"struct" or !@hasField(M.Data, "value")) return null;
    const V = @FieldType(M.Data, "value");
    return if (T == std.json.ArrayHashMap(V)) V else null;
}

// Passed to std.json in place of a value of `T` that holds a Timestamp.
fn TimestampJson(comptime T: type) type {
    return struct {
        value: *const T,

        pub fn jsonStringify(self: @This(), jws: *std.json.Stringify) std.json.Stringify.Error!void {
            return writeJson(T, self.value, jws);
        }
    };
}

// Writes `v` as std.json.Stringify.write does with the default options, except that each
// Timestamp is written by `proto_time.formatTimestamp`, as `protojson` writes one. The error
// set is explicit because the function recurses.
fn writeJson(comptime T: type, v: *const T, jws: *std.json.Stringify) std.json.Stringify.Error!void {
    if (T == proto.google.protobuf.Timestamp) {
        var buf: [proto_time.timestamp_text_max]u8 = undefined;
        const text = proto_time.formatTimestamp(v.*, &buf) catch |err| switch (err) {
            error.OutOfRange => return error.WriteFailed,
        };
        return jws.write(text);
    }
    if (comptime !holdsTimestamp(T)) return jws.write(v.*);
    if (comptime jsonMapValue(T)) |V| {
        // As std.json.ArrayHashMap's own jsonStringify, with each value written here.
        try jws.beginObject();
        for (v.map.keys(), v.map.values()) |key, *item| {
            try jws.objectField(key);
            try writeJson(V, item, jws);
        }
        return jws.endObject();
    }
    switch (@typeInfo(T)) {
        .optional => |o| return if (v.*) |*payload| writeJson(o.child, payload, jws) else jws.write(null),
        .@"union" => {
            try jws.beginObject();
            switch (v.*) {
                inline else => |*payload, tag| {
                    try jws.objectField(@tagName(tag));
                    const F = @TypeOf(payload.*);
                    if (F == void) {
                        try jws.beginObject();
                        try jws.endObject();
                    } else try writeAt(F, payload, jws);
                },
            }
            return jws.endObject();
        },
        .@"struct" => |s| {
            if (s.is_tuple) try jws.beginArray() else try jws.beginObject();
            inline for (s.field_names, s.field_types, s.field_attrs) |name, F, attrs| {
                if (F == void) continue;
                if (!s.is_tuple) try jws.objectField(name);
                if (comptime !holdsTimestamp(F)) {
                    try jws.write(@field(v.*, name));
                } else if (attrs.@"comptime") {
                    // A comptime field (any comptime-known field of an anonymous struct
                    // literal) has no runtime address, so write a runtime copy.
                    var copy: F = @field(v.*, name);
                    _ = &copy;
                    try writeJson(F, &copy, jws);
                } else try writeAt(F, &@field(v.*, name), jws);
            }
            return if (s.is_tuple) jws.endArray() else jws.endObject();
        },
        .array => |a| return writeElements(a.child, v, jws),
        .vector => |info| {
            const array: [info.len]info.child = v.*;
            return writeElements(info.child, &array, jws);
        },
        .pointer => |p| {
            if (comptime coercesToConst(p)) switch (p.size) {
                .one => return writeJson(p.child, v.*, jws),
                .slice => return writeElements(p.child, v.*, jws),
                .many => return writeElements(p.child, std.mem.span(v.*), jws),
                .c => comptime unreachable,
            };
            switch (p.size) {
                .one => return writeAt(p.child, v.*, jws),
                .slice => {
                    try jws.beginArray();
                    for (0..v.*.len) |i| try writeAt(p.child, &v.*[i], jws);
                    return jws.endArray();
                },
                // holdsTimestamp reaches a many-item pointer only when it has a sentinel.
                .many => {
                    try jws.beginArray();
                    var i: usize = 0;
                    while (true) : (i += 1) {
                        const e: p.child = v.*[i];
                        if (e == comptime p.sentinel().?) break;
                        try writeJson(p.child, &e, jws);
                    }
                    return jws.endArray();
                },
                .c => comptime unreachable,
            }
        },
        else => comptime unreachable,
    }
}

// Whether a pointer of this type coerces to `*const p.child`. A volatile, allowzero,
// under-aligned or non-generic address-space pointer does not.
fn coercesToConst(comptime p: std.lang.Type.Pointer) bool {
    return !p.attrs.@"volatile" and !p.attrs.@"allowzero" and
        (p.attrs.@"addrspace" == null or p.attrs.@"addrspace".? == .generic) and
        (p.attrs.@"align" == null or p.attrs.@"align".? >= @alignOf(p.child));
}

// Writes the `T` at `ptr`, any single-item pointer to a `T`: a pointer value, or the address
// of an align(1) struct field or union payload, which is `*align(1) T`. One that coerces to
// `*const T` is written in place; any other is copied into an aligned local first, element by
// element for an array so that a large one is not copied whole onto the stack. Every address
// `writeJson` takes from a value it is writing comes through here.
fn writeAt(comptime T: type, ptr: anytype, jws: *std.json.Stringify) std.json.Stringify.Error!void {
    const p = @typeInfo(@TypeOf(ptr)).pointer;
    comptime std.debug.assert(p.size == .one and p.child == T);
    if (comptime coercesToConst(p)) return writeJson(T, ptr, jws);
    switch (@typeInfo(T)) {
        // As writeJson's own `.array` arm, which writes each element of such an array.
        .array => |a| if (comptime holdsTimestamp(T)) {
            try jws.beginArray();
            for (0..a.len) |i| try writeAt(a.child, &ptr[i], jws);
            return jws.endArray();
        },
        else => {},
    }
    const copy: T = ptr.*;
    return writeJson(T, &copy, jws);
}

fn writeElements(comptime E: type, elements: []const E, jws: *std.json.Stringify) std.json.Stringify.Error!void {
    try jws.beginArray();
    for (elements) |*e| try writeJson(E, e, jws);
    return jws.endArray();
}

/// Arguments not yet decoded, as a dynamic workflow, activity, or signal handler receives
/// them: the handler reads each one as the type it expects. A view: the payloads are owned by
/// the run or task that delivered them and live as long as its memory (`ctx.allocator()`);
/// decoded values come from the arena passed in. Values the SDK hands to user code carry the
/// context of the call they belong to, which context hooks receive when they are decoded.
pub const EncodedValues = struct {
    payloads: []const Payload,
    /// The context decoding passes to context hooks; borrowed, and must stay valid as long
    /// as the values are decoded.
    context: *const Context = &.{},

    pub fn len(v: EncodedValues) usize {
        return v.payloads.len;
    }

    /// Decodes the argument at `index` as `T`. Past the last argument, an optional `T`
    /// decodes as null and anything else returns `error.WrongArgumentCount`, as in
    /// `decode`.
    pub fn get(v: EncodedValues, comptime T: type, arena: Allocator, index: usize) DecodeError!T {
        if (index < v.payloads.len) return fromPayloadWithContext(T, arena, v.context, v.payloads[index]);
        if (@typeInfo(T) == .optional) return null;
        return error.WrongArgumentCount;
    }

    /// Decodes every argument into the tuple `Args`, by the rules for typed arguments:
    /// missing trailing optionals are null, extra arguments are ignored.
    pub fn decode(v: EncodedValues, comptime Args: type, arena: Allocator) DecodeError!Args {
        return fromPayloadsWithContext(Args, arena, v.context, v.payloads);
    }

    /// A copy that decodes with `context`, such as failure details read with
    /// `details.withContext(future.converterContext())`. Copies nothing but the struct.
    pub fn withContext(v: EncodedValues, context: *const Context) EncodedValues {
        return .{ .payloads = v.payloads, .context = context };
    }
};

/// One value not yet decoded, or no value, for a caller that learns its type only at run
/// time, such as the result of `Client.queryWorkflowByName`. A view: the payload lives as
/// long as the memory it came from; decoded values come from the arena passed to `get`.
pub const EncodedValue = struct {
    payload: ?Payload,
    /// The context decoding passes to context hooks, as for `EncodedValues`.
    context: *const Context = &.{},

    /// Whether there is a value to decode.
    pub fn hasValue(v: EncodedValue) bool {
        return v.payload != null;
    }

    /// Decodes the value as `T` into `arena`. With no value, an optional `T` decodes as null
    /// and anything else returns `error.WrongArgumentCount`, as `EncodedValues.get` does
    /// past the last argument.
    pub fn get(v: EncodedValue, comptime T: type, arena: Allocator) DecodeError!T {
        if (v.payload) |p| return fromPayloadWithContext(T, arena, v.context, p);
        if (@typeInfo(T) == .optional) return null;
        return error.WrongArgumentCount;
    }

    /// A copy that decodes with `context`. Copies nothing but the struct.
    pub fn withContext(v: EncodedValue, context: *const Context) EncodedValue {
        return .{ .payload = v.payload, .context = context };
    }
};

/// A named value, such as one entry of a workflow memo.
pub const Field = struct {
    name: []const u8,
    value: Payload,
};

/// Encodes each field of the struct `values` as a `Field`, in declaration order, so the
/// result is deterministic and usable from workflow code:
/// `toFields(arena, .{ .description = "nightly import", .owner = @as(?[]const u8, null) })`.
/// Null optional fields encode as `binary/null`. The slice and payload memory come from
/// `arena`, and the fields borrow it; names are the comptime field names.
pub fn toFields(arena: Allocator, values: anytype) error{OutOfMemory}![]Field {
    const info = @typeInfo(@TypeOf(values));
    if (info != .@"struct" or (info.@"struct".is_tuple and info.@"struct".field_names.len > 0))
        @compileError("toFields takes a struct with named fields, got " ++ @typeName(@TypeOf(values)));
    const names = info.@"struct".field_names;
    const out = try arena.alloc(Field, names.len);
    inline for (names, 0..) |name, i| {
        out[i] = .{ .name = name, .value = try toPayload(arena, @field(values, name)) };
    }
    return out;
}

/// Decodes the value of the field called `name` as `T`, or returns null if no field has that
/// name. If several fields share the name, the first wins. Memory comes from `arena`.
pub fn fieldValue(comptime T: type, arena: Allocator, fields: []const Field, name: []const u8) DecodeError!?T {
    for (fields) |field| {
        if (std.mem.eql(u8, field.name, name)) return try fromPayload(T, arena, field.value);
    }
    return null;
}

/// The header of an operation: named payloads sent alongside its arguments (a workflow
/// start, a signal, an activity, ...) for interceptors and context propagators to read.
/// Workflow code reads its start header with `ctx.headers()`, an activity its own with
/// `ctx.headers()`. Fields are sorted by name, each name once, so a header read from history
/// reads the same on replay whatever order the server sent it in. A view: the fields borrow
/// the memory they were built from (the run's or the task's).
pub const Header = struct {
    fields: []const Field = &.{},

    /// The payload of the field called `name`, or null.
    pub fn get(h: Header, name: []const u8) ?Payload {
        const i = std.sort.binarySearch(Field, h.fields, name, fieldOrder) orelse return null;
        return h.fields[i].value;
    }

    /// The field called `name` decoded as `T` into `arena`, or null if the header has none.
    pub fn value(h: Header, comptime T: type, arena: Allocator, name: []const u8) DecodeError!?T {
        const p = h.get(name) orelse return null;
        return try fromPayload(T, arena, p);
    }

    /// Copies the entries of a protobuf map of payloads (each with `key` and
    /// `value: ?Payload`, such as `temporal.api.common.v1.Header.fields` or an activation
    /// job's `headers`) into a header allocated from `arena`. A repeated key keeps its last
    /// value; an entry without a value becomes an empty payload.
    pub fn fromEntries(arena: Allocator, entries: anytype) error{OutOfMemory}!Header {
        var w: HeaderWriter = .init(arena);
        for (entries) |e| try w.put(try arena.dupe(u8, e.key), if (e.value) |v| try v.dupe(arena) else .{});
        return w.header();
    }

    /// The fields as protobuf map entries of type `Entry` (with `key` and `value`), in name
    /// order. The list comes from `arena`; the entries borrow the header.
    pub fn toEntries(h: Header, comptime Entry: type, arena: Allocator) error{OutOfMemory}!std.ArrayList(Entry) {
        var out: std.ArrayList(Entry) = try .initCapacity(arena, h.fields.len);
        for (h.fields) |f| out.appendAssumeCapacity(.{ .key = f.name, .value = f.value });
        return out;
    }
};

/// Builds a `Header`: fields stay sorted by name, and putting a name again replaces its
/// value. Names and payloads are borrowed; the lists and encoded values come from `arena`.
/// Not thread-safe.
///
/// Each field also carries a mark saying whether the client's payload codecs encode it when
/// the header is sent (`encodes`). A write marks the field when `encode_fields` is true and
/// clears its mark when it is false, so the last writer of a field decides.
/// `propagation.injectAll` sets `encode_fields` for each propagator from its
/// `ContextPropagator.encode_fields`; it is false while interceptors and other code write, so
/// their fields are sent as written.
pub const HeaderWriter = struct {
    arena: Allocator,
    fields: std.ArrayList(Field) = .empty,
    /// The names, sorted, of the fields in `fields` that the payload codecs encode when the
    /// header is sent. Kept by `put`, `set`, `merge` and `remove`; don't edit it directly.
    codec_fields: std.ArrayList([]const u8) = .empty,
    /// Whether `put`, `set` and `merge` mark what they write for the codecs. A write while it
    /// is false clears the field's mark. `propagation.injectAll` sets it for each propagator;
    /// it is false while interceptors and other code write.
    encode_fields: bool = false,

    pub fn init(arena: Allocator) HeaderWriter {
        return .{ .arena = arena };
    }

    /// Sets the field `name` to `payload`, marked for the codecs as `encode_fields` says.
    pub fn put(w: *HeaderWriter, name: []const u8, payload: Payload) error{OutOfMemory}!void {
        const i = std.sort.lowerBound(Field, w.fields.items, name, fieldOrder);
        const exists = i < w.fields.items.len and std.mem.eql(u8, w.fields.items[i].name, name);
        const m = std.sort.lowerBound([]const u8, w.codec_fields.items, name, nameOrder);
        const marked = m < w.codec_fields.items.len and std.mem.eql(u8, w.codec_fields.items[m], name);
        // Both lists are grown before either changes, so a failure leaves them as they were.
        if (!exists) try w.fields.ensureUnusedCapacity(w.arena, 1);
        if (w.encode_fields and !marked) try w.codec_fields.ensureUnusedCapacity(w.arena, 1);
        if (exists) {
            w.fields.items[i].value = payload;
        } else {
            w.fields.insertAssumeCapacity(i, .{ .name = name, .value = payload });
        }
        if (w.encode_fields and !marked) {
            w.codec_fields.insertAssumeCapacity(m, name);
        } else if (!w.encode_fields and marked) {
            _ = w.codec_fields.orderedRemove(m);
        }
    }

    /// Sets the field `name` to `v` encoded with `toPayload`.
    pub fn set(w: *HeaderWriter, name: []const u8, v: anytype) error{OutOfMemory}!void {
        return w.put(name, try toPayload(w.arena, v));
    }

    /// Sets every field of `h`, replacing fields of the same name.
    pub fn merge(w: *HeaderWriter, h: Header) error{OutOfMemory}!void {
        for (h.fields) |f| try w.put(f.name, f.value);
    }

    /// Removes the field `name`, and its mark, if present.
    pub fn remove(w: *HeaderWriter, name: []const u8) void {
        if (std.sort.binarySearch([]const u8, w.codec_fields.items, name, nameOrder)) |m| _ = w.codec_fields.orderedRemove(m);
        const i = std.sort.binarySearch(Field, w.fields.items, name, fieldOrder) orelse return;
        _ = w.fields.orderedRemove(i);
    }

    /// Whether the field `name` is marked for the codecs.
    pub fn encodes(w: *const HeaderWriter, name: []const u8) bool {
        return std.sort.binarySearch([]const u8, w.codec_fields.items, name, nameOrder) != null;
    }

    /// The fields written so far; a later write leaves the view's slice as it is only until
    /// the writer reallocates. The view carries no marks.
    pub fn header(w: *const HeaderWriter) Header {
        return .{ .fields = w.fields.items };
    }
};

fn fieldOrder(name: []const u8, f: Field) std.math.Order {
    return std.mem.order(u8, name, f.name);
}

fn nameOrder(name: []const u8, other: []const u8) std.math.Order {
    return std.mem.order(u8, name, other);
}

/// Turns the entries of a proto map of payloads, such as `Memo.fields` or
/// `SearchAttributes.indexed_fields` (each entry has `key` and `value: ?Payload`), into
/// fields in wire order. An entry without a value becomes an empty payload. The slice comes
/// from `arena`; names and payloads borrow the entries.
pub fn fieldsFromMap(arena: Allocator, entries: anytype) error{OutOfMemory}![]Field {
    const out = try arena.alloc(Field, entries.len);
    for (entries, out) |entry, *field| field.* = .{ .name = entry.key, .value = entry.value orelse .{} };
    return out;
}

fn payloadWith(arena: Allocator, enc: Encoding, data: []const u8) error{OutOfMemory}!Payload {
    return payloadWithEncoding(arena, enc.text(), data);
}

/// A payload of `data` with the `encoding` metadata entry `encoding`, for `toPayload` hooks
/// and payload codecs. Borrows `data` and `encoding`; the metadata list comes from `arena`.
pub fn payloadWithEncoding(arena: Allocator, encoding: []const u8, data: []const u8) error{OutOfMemory}!Payload {
    var p: Payload = .{ .data = data };
    try p.metadata.append(arena, .{ .key = encoding_key, .value = encoding });
    return p;
}

// Whether `T` declares the context hook pair. Half a pair is a compile error, so a hook
// that would silently never run is caught where the type is converted.
fn hasContextHooks(comptime T: type) bool {
    switch (@typeInfo(T)) {
        .@"struct", .@"union", .@"enum", .@"opaque" => {},
        else => return false,
    }
    const to = std.meta.hasFn(T, "toPayloadWithContext");
    const from = std.meta.hasFn(T, "fromPayloadWithContext");
    if (to and !from) @compileError(@typeName(T) ++ " declares toPayloadWithContext but not fromPayloadWithContext; a type's context hooks come in pairs");
    if (from and !to) @compileError(@typeName(T) ++ " declares fromPayloadWithContext but not toPayloadWithContext; a type's context hooks come in pairs");
    return to;
}

fn hasPayloadHooks(comptime T: type) bool {
    return switch (@typeInfo(T)) {
        .@"struct", .@"union", .@"enum", .@"opaque" => std.meta.hasFn(T, "toPayload") and std.meta.hasFn(T, "fromPayload"),
        else => false,
    };
}

/// Whether `T` is a message type generated by zig-protobuf (detected by its descriptor table
/// and generated codec functions).
pub fn isProtoMessage(comptime T: type) bool {
    return @typeInfo(T) == .@"struct" and @hasDecl(T, "_desc_table") and
        std.meta.hasFn(T, "encode") and std.meta.hasFn(T, "decode") and std.meta.hasFn(T, "jsonEncode");
}

/// The full protobuf name of a generated message type, derived from where the generator put
/// it: generated files mirror the proto package (`temporal/api/common/v1.pb.zig` for
/// `temporal.api.common.v1`), so the name is the type's Zig name without its first segment
/// (the module or directory the generated files are imported through) and without the file's
/// `pb` segment: `proto.temporal.api.common.v1.pb.Payload` is
/// "temporal.api.common.v1.Payload". When the generated files are imported some other way, set
/// `ProtoOptions.message_type`.
pub fn protoMessageType(comptime T: type) []const u8 {
    return comptime blk: {
        const name = @typeName(T);
        const pb = std.mem.indexOf(u8, name, ".pb.") orelse break :blk name;
        const first = std.mem.indexOfScalar(u8, name, '.').?;
        if (first == pb) break :blk name[pb + ".pb.".len ..];
        break :blk name[first + 1 .. pb] ++ name[pb + ".pb".len ..];
    };
}

pub const ProtoOptions = struct {
    encoding: enum { json, binary } = .json,
    /// The `messageType` metadata value; null derives it with `protoMessageType`.
    message_type: ?[]const u8 = null,
};

/// A protobuf message `T` converted with `options`: wrap a message to choose its encoding or
/// name, `Proto(Order, .{ .encoding = .binary }){ .message = order }`, and take the wrapper
/// type as a parameter or result to decode one. Decoding accepts both protobuf encodings
/// whatever `options` says. Unwrapped messages convert as `Proto(T, .{})`. `json/protobuf`
/// data is written by `protojson.encode` and read by `protojson.decode`, so it may hold
/// enums by short name, and invalid ProtoJSON is `error.InvalidPayload`.
pub fn Proto(comptime T: type, comptime options: ProtoOptions) type {
    if (!isProtoMessage(T)) @compileError("Proto takes a message type generated by zig-protobuf, not " ++ @typeName(T));
    return struct {
        message: T,

        const Self = @This();
        const message_type = options.message_type orelse protoMessageType(T);

        pub fn toPayload(self: Self, arena: Allocator) error{OutOfMemory}!Payload {
            return encode(arena, self.message);
        }

        pub fn fromPayload(arena: Allocator, p: Payload) DecodeError!Self {
            return .{ .message = try decode(arena, p) };
        }

        fn encode(arena: Allocator, message: T) error{OutOfMemory}!Payload {
            const enc: Encoding, const data = switch (options.encoding) {
                .json => .{ .proto_json, try protojson.encode(arena, message) },
                .binary => blk: {
                    var out: std.Io.Writer.Allocating = .init(arena);
                    message.encode(&out.writer, arena) catch return error.OutOfMemory;
                    break :blk .{ .proto_binary, out.written() };
                },
            };
            var p = try payloadWith(arena, enc, data);
            try p.metadata.append(arena, .{ .key = message_type_key, .value = message_type });
            return p;
        }

        fn decode(arena: Allocator, p: Payload) DecodeError!T {
            const enc = encodingOf(p) orelse return error.UnknownEncoding;
            if (std.mem.eql(u8, enc, Encoding.proto_json.text())) {
                return protojson.decode(T, arena, p.data) catch |err| switch (err) {
                    error.OutOfMemory => error.OutOfMemory,
                    error.InvalidProtoJson => error.InvalidPayload,
                };
            }
            if (std.mem.eql(u8, enc, Encoding.proto_binary.text())) {
                // Decoded messages may borrow the input, so it is copied into the arena.
                var reader: std.Io.Reader = .fixed(try arena.dupe(u8, p.data));
                return T.decode(&reader, arena) catch |err| switch (err) {
                    error.OutOfMemory => error.OutOfMemory,
                    else => error.InvalidPayload,
                };
            }
            return error.UnknownEncoding;
        }
    };
}

/// `Proto(T, .{ .encoding = .binary })`: the message encodes as `binary/protobuf`.
pub fn ProtoBinary(comptime T: type) type {
    return Proto(T, .{ .encoding = .binary });
}

// ---------------------------------------------------------------- tests

const testing = std.testing;

test {
    _ = codec;
    _ = codec_handler;
    _ = remote_codec;
}

/// A point in time that encodes in JSON as an RFC 3339 string with up to nanosecond
/// precision, such as "2018-08-22T16:30:00Z", the common form for times in payloads.
pub const Time = struct {
    /// Nanoseconds since the Unix epoch.
    unix_ns: i96,

    /// Room for the longest text `format` writes, "9999-12-31T23:59:59.999999999Z" (30 bytes):
    /// `format` clamps to the years 0000 to 9999.
    pub const max_len = 40;

    /// Any `std.Io.Timestamp` is accepted. `format` and `jsonStringify` (and so `toPayload`)
    /// write a time before 0000-01-01T00:00:00Z or after 9999-12-31T23:59:59.999999999Z as
    /// that bound, the range `parse` accepts; `toTimestamp` still gives back the exact value.
    pub fn fromTimestamp(ts: std.Io.Timestamp) Time {
        return .{ .unix_ns = ts.nanoseconds };
    }

    pub fn toTimestamp(t: Time) std.Io.Timestamp {
        return .{ .nanoseconds = t.unix_ns };
    }

    /// The earliest and latest instants `format` writes and `parse` accepts:
    /// 0000-01-01T00:00:00Z and 9999-12-31T23:59:59.999999999Z.
    const first_ns: i96 = @as(i96, proto_time.daysFromCivil(0, 1, 1)) * std.time.s_per_day * std.time.ns_per_s;
    const last_ns: i96 = @as(i96, proto_time.daysFromCivil(10000, 1, 1)) * std.time.s_per_day * std.time.ns_per_s - 1;

    /// Writes `t` in UTC, with trailing zeros of the fraction dropped. A time before
    /// 0000-01-01T00:00:00Z or after 9999-12-31T23:59:59.999999999Z is written as that bound,
    /// so the text always parses back with `parse`; it never panics.
    pub fn format(t: Time, buf: *[max_len]u8) []const u8 {
        const ns = std.math.clamp(t.unix_ns, first_ns, last_ns);
        const secs: i64 = @intCast(@divFloor(ns, std.time.ns_per_s));
        const nanos: u32 = @intCast(@mod(ns, std.time.ns_per_s));
        const days = @divFloor(secs, std.time.s_per_day);
        const sod: u32 = @intCast(@mod(secs, std.time.s_per_day));
        const date = proto_time.civilFromDays(days);
        var w: std.Io.Writer = .fixed(buf);
        w.print("{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}", .{
            @as(u64, @intCast(date.year)), date.month, date.day, sod / 3600, sod / 60 % 60, sod % 60,
        }) catch unreachable;
        if (nanos != 0) {
            var frac: [9]u8 = undefined;
            _ = std.fmt.bufPrint(&frac, "{d:0>9}", .{nanos}) catch unreachable;
            const digits = std.mem.trimEnd(u8, &frac, "0");
            w.print(".{s}", .{digits}) catch unreachable;
        }
        w.writeByte('Z') catch unreachable;
        return w.buffered();
    }

    /// Parses an RFC 3339 time of the form "YYYY-MM-DDTHH:MM:SS[.F+](Z|±HH:MM)", where `T` and
    /// `Z` may be lowercase. Every numeric field is ASCII digits only: no sign, underscore or
    /// space. The month is 1 to 12 and the day 1 to the month's length, leap years included
    /// (0000 is a leap year in the proleptic Gregorian calendar). The hour is at most 23, the
    /// minute at most 59 and the second at most 59: a leap second (":60") is rejected, as
    /// protobuf's Timestamp parser rejects it. The fraction has one or more digits, and digits
    /// after the ninth are dropped. An offset's hour is at most 23 and its minute at most 59.
    /// The time in UTC must fall within the years 0000 to 9999, since `format` can't write
    /// others. Anything else is `error.InvalidTime`.
    pub fn parse(text: []const u8) error{InvalidTime}!Time {
        if (text.len < 20 or text[4] != '-' or text[7] != '-' or (text[10] != 'T' and text[10] != 't') or text[13] != ':' or text[16] != ':')
            return error.InvalidTime;
        const year = try digitsOf(text[0..4]);
        const month = try digitsOf(text[5..7]);
        const day = try digitsOf(text[8..10]);
        const hour = try digitsOf(text[11..13]);
        const minute = try digitsOf(text[14..16]);
        const second = try digitsOf(text[17..19]);
        if (month < 1 or month > 12 or hour > 23 or minute > 59 or second > 59) return error.InvalidTime;
        if (day < 1 or day > std.time.epoch.getDaysInMonth(@intCast(year), @fromBackingInt(@intCast(month)))) return error.InvalidTime;
        var i: usize = 19;
        var nanos: i96 = 0;
        if (text[i] == '.') {
            i += 1;
            var scale: i96 = 100_000_000;
            const start = i;
            while (i < text.len and std.ascii.isDigit(text[i])) : (i += 1) {
                nanos += (text[i] - '0') * scale;
                scale = @divTrunc(scale, 10);
            }
            if (i == start) return error.InvalidTime;
        }
        if (i >= text.len) return error.InvalidTime;
        var offset_s: i64 = 0;
        switch (text[i]) {
            'Z', 'z' => i += 1,
            '+', '-' => {
                if (text.len < i + 6 or text[i + 3] != ':') return error.InvalidTime;
                const off_hour = try digitsOf(text[i + 1 .. i + 3]);
                const off_minute = try digitsOf(text[i + 4 .. i + 6]);
                if (off_hour > 23 or off_minute > 59) return error.InvalidTime;
                const off = @as(i64, off_hour) * 3600 + @as(i64, off_minute) * 60;
                offset_s = if (text[i] == '-') -off else off;
                i += 6;
            },
            else => return error.InvalidTime,
        }
        if (i != text.len) return error.InvalidTime;
        const days = proto_time.daysFromCivil(year, @intCast(month), @intCast(day));
        const secs = days * std.time.s_per_day + @as(i64, hour) * 3600 + @as(i64, minute) * 60 + second - offset_s;
        const unix_ns = @as(i96, secs) * std.time.ns_per_s + nanos;
        if (unix_ns < first_ns or unix_ns > last_ns) return error.InvalidTime;
        return .{ .unix_ns = unix_ns };
    }

    /// Writes `t` as a JSON string, clamped to the years 0000 to 9999 as `format` does.
    pub fn jsonStringify(t: Time, jws: anytype) !void {
        var buf: [max_len]u8 = undefined;
        try jws.write(t.format(&buf));
    }

    pub fn jsonParse(allocator: Allocator, source: anytype, options: std.json.ParseOptions) std.json.ParseError(@TypeOf(source.*))!Time {
        const text = try std.json.innerParse([]const u8, allocator, source, options);
        return parse(text) catch error.UnexpectedToken;
    }

    pub fn jsonParseFromValue(allocator: Allocator, source: std.json.Value, options: std.json.ParseOptions) std.json.ParseFromValueError!Time {
        _ = allocator;
        _ = options;
        if (source != .string) return error.UnexpectedToken;
        return parse(source.string) catch error.UnexpectedToken;
    }

    /// The value of `s`, which must be ASCII digits only: no sign, underscore or space.
    /// Callers pass at most four digits, so the value always fits.
    fn digitsOf(s: []const u8) error{InvalidTime}!u16 {
        var value: u16 = 0;
        for (s) |c| {
            if (c < '0' or c > '9') return error.InvalidTime;
            value = value * 10 + (c - '0');
        }
        return value;
    }
};

test "unconvertibleType names the type the default converter can't handle" {
    const Node = struct { value: u32, next: ?*const @This() };
    const Shape = union(enum) { circle: f32, empty };
    const Untagged = union { a: u32, b: f32 };
    inline for (.{ void, Payload, Bytes, Point, ?Point, Failure, []const u8, ?u32, Node, Shape, [3]u8, struct { a: []const []const u8 } }) |T| {
        try testing.expectEqual(null, unconvertibleType(T, .encode));
        try testing.expectEqual(null, unconvertibleType(T, .decode));
    }
    try testing.expect(unconvertibleType(*std.Io.Writer, .decode) != null);
    try testing.expect(unconvertibleType(*std.Io.Writer, .encode) != null);
    try testing.expectEqual(anyopaque, unconvertibleType(*anyopaque, .encode));
    try testing.expectEqual(fn () void, unconvertibleType(struct { f: *const fn () void }, .decode));
    try testing.expectEqual(Untagged, unconvertibleType(struct { u: Untagged }, .decode));
    try testing.expectEqual(null, unconvertibleType(error{Oops}, .encode));
    try testing.expectEqual(error{Oops}, unconvertibleType(error{Oops}, .decode));
    try testing.expectEqual(null, unconvertibleType([*:0]const u8, .encode));
    try testing.expectEqual([*:0]const u8, unconvertibleType([*:0]const u8, .decode));
}

test "strings, structs and numbers encode as JSON" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const s = try toPayload(a, @as([]const u8, "Temporal"));
    try testing.expectEqualStrings("json/plain", encodingOf(s).?);
    try testing.expectEqualStrings("\"Temporal\"", s.data);
    try testing.expectEqualStrings("Temporal", try fromPayload([]const u8, a, s));

    const Order = struct { id: u32, items: []const []const u8 };
    const o = try toPayload(a, Order{ .id = 7, .items = &.{ "a", "b" } });
    try testing.expectEqualStrings("{\"id\":7,\"items\":[\"a\",\"b\"]}", o.data);
    const back = try fromPayload(Order, a, o);
    try testing.expectEqual(@as(u32, 7), back.id);
    try testing.expectEqualStrings("b", back.items[1]);
}

test "null, void and raw bytes use the binary encodings" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const n = try toPayload(a, {});
    try testing.expectEqualStrings("binary/null", encodingOf(n).?);
    try testing.expectEqual(@as(?u32, null), try fromPayload(?u32, a, n));
    try fromPayload(void, a, n);

    const b = try toPayload(a, Bytes{ .data = "\x00\x01" });
    try testing.expectEqualStrings("binary/plain", encodingOf(b).?);
    try testing.expectEqualSlices(u8, "\x00\x01", (try fromPayload(Bytes, a, b)).data);
    try testing.expectEqualSlices(u8, "\x00\x01", try fromPayload([]const u8, a, b));
}

test "an optional decodes a binary/plain payload as its child, and JSON into the optional itself" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const full = try toPayload(a, @as(?Bytes, .{ .data = "\x00\xff" }));
    try testing.expectEqualStrings("binary/plain", encodingOf(full).?);
    try testing.expectEqualSlices(u8, "\x00\xff", (try fromPayload(?Bytes, a, full)).?.data);
    try testing.expectEqualSlices(u8, "\x00\xff", (try fromPayload(?[]const u8, a, full)).?);
    try testing.expectEqualSlices(u8, "\x00\xff", (try fromPayload(?[]u8, a, full)).?);
    try testing.expectError(error.InvalidPayload, fromPayload(?u32, a, full));

    const empty = try toPayload(a, @as(?Bytes, null));
    try testing.expectEqualStrings("binary/null", encodingOf(empty).?);
    try testing.expectEqual(@as(?Bytes, null), try fromPayload(?Bytes, a, empty));
    try testing.expectEqual(@as(?[]const u8, null), try fromPayload(?[]const u8, a, empty));

    const json_null = try payloadWith(a, .json, "null");
    try testing.expectEqual(@as(?u32, null), try fromPayload(?u32, a, json_null));
    try testing.expectEqual(@as(?[]const u8, null), try fromPayload(?[]const u8, a, json_null));
    try testing.expectEqual(@as(?u32, 7), try fromPayload(?u32, a, try toPayload(a, @as(?u32, 7))));
    try testing.expectEqualStrings("x", (try fromPayload(?[]const u8, a, try toPayload(a, @as(?[]const u8, "x")))).?);
    try testing.expectError(error.InvalidPayload, fromPayload(?u32, a, try payloadWith(a, .json, "{not json")));

    const ps = try toPayloads(a, .{ @as(?Bytes, .{ .data = "\x00\xff" }), @as(?Bytes, null) });
    const args = try fromPayloads(struct { ?Bytes, ?Bytes }, a, ps.items);
    try testing.expectEqualSlices(u8, "\x00\xff", args[0].?.data);
    try testing.expectEqual(@as(?Bytes, null), args[1]);
}

test "argument tuples round-trip, with optional trailing arguments" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const ps = try toPayloads(a, .{ @as([]const u8, "x"), @as(i64, 42) });
    const args = try fromPayloads(struct { []const u8, i64, ?bool }, a, ps.items);
    try testing.expectEqualStrings("x", args[0]);
    try testing.expectEqual(@as(i64, 42), args[1]);
    try testing.expectEqual(@as(?bool, null), args[2]);
    try testing.expectError(error.WrongArgumentCount, fromPayloads(struct { []const u8, i64, u8 }, a, ps.items));
}

test "bad payloads are reported, not crashed on" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    try testing.expectError(error.UnknownEncoding, fromPayload(u32, a, .{ .data = "1" }));
    const bad = try payloadWith(a, .json, "{not json");
    try testing.expectError(error.InvalidPayload, fromPayload(u32, a, bad));
    var odd: Payload = .{ .data = "x" };
    try odd.metadata.append(a, .{ .key = "encoding", .value = "json/protobuf" });
    try testing.expectError(error.UnknownEncoding, fromPayload(u32, a, odd));
}

test "Time encodes as RFC 3339 text" {
    var buf: [Time.max_len]u8 = undefined;
    try std.testing.expectEqualStrings("0001-01-01T00:00:00Z", (try Time.parse("0001-01-01T00:00:00Z")).format(&buf));
    const t = try Time.parse("2018-08-22T16:30:00Z");
    try std.testing.expectEqualStrings("2018-08-22T16:30:00Z", t.format(&buf));
    try std.testing.expectEqual(@as(i96, 1534955400) * std.time.ns_per_s, t.unix_ns);
    const frac = try Time.parse("2018-08-22T09:30:00.12-07:00");
    try std.testing.expectEqualStrings("2018-08-22T16:30:00.12Z", frac.format(&buf));
    try std.testing.expectError(error.InvalidTime, Time.parse("2018-08-22 16:30:00"));
    // Years format can't write: the offset or a leap second moves these out of 0000 to 9999.
    try std.testing.expectError(error.InvalidTime, Time.parse("0000-01-01T00:00:00+00:01"));
    try std.testing.expectError(error.InvalidTime, Time.parse("9999-12-31T23:59:59-00:01"));
    try std.testing.expectError(error.InvalidTime, Time.parse("9999-12-31T23:59:60Z"));
    try std.testing.expectEqualStrings("0000-01-01T00:00:00Z", (try Time.parse("0000-01-01T01:00:00+01:00")).format(&buf));
    try std.testing.expectEqualStrings("9999-12-31T23:59:59.999999999Z", (try Time.parse("9999-12-31T22:59:59.999999999-01:00")).format(&buf));

    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const S = struct { run_time: Time };
    const p = try toPayload(arena.allocator(), S{ .run_time = t });
    try std.testing.expectEqualStrings("{\"run_time\":\"2018-08-22T16:30:00Z\"}", p.data);
    try std.testing.expectEqual(t, (try fromPayload(S, arena.allocator(), p)).run_time);
}

test "Time.parse rejects impossible dates, offsets and non-digit fields" {
    const rejected = [_][]const u8{
        // Days beyond the month's length.
        "2023-02-29T00:00:00Z",
        "2023-02-31T00:00:00Z",
        "2100-02-29T00:00:00Z",
        "2024-04-31T00:00:00Z",
        "2024-01-00T00:00:00Z",
        // Offsets out of range.
        "2023-01-01T00:00:00+00:99",
        "2023-01-01T00:00:00+24:00",
        "2023-01-01T00:00:00-23:60",
        // Fields that aren't ASCII digits only.
        "20_3-01-01T00:00:00Z",
        "+023-01-01T00:00:00Z",
        " 2023-01-01T00:00:00Z",
        "2023-+1-01T00:00:00Z",
        "2023-01-01T0_:00:00Z",
        "2023-01-01T00:00:00+0_:00",
        "2023-01-01T00:00:00-+1:00",
        // A leap second.
        "2023-06-30T23:59:60Z",
    };
    for (rejected) |text| {
        _ = Time.parse(text) catch continue;
        std.debug.print("Time.parse accepted \"{s}\"\n", .{text});
        return error.TestUnexpectedResult;
    }

    var buf: [Time.max_len]u8 = undefined;
    const accepted = [_]struct { []const u8, []const u8 }{
        .{ "2024-02-29T00:00:00Z", "2024-02-29T00:00:00Z" },
        .{ "2000-02-29T00:00:00Z", "2000-02-29T00:00:00Z" },
        .{ "0000-02-29T00:00:00Z", "0000-02-29T00:00:00Z" },
        .{ "2023-01-31T00:00:00Z", "2023-01-31T00:00:00Z" },
        .{ "2023-06-15t12:00:00+23:59", "2023-06-14T12:01:00Z" },
        .{ "2023-06-15T12:00:00-23:59", "2023-06-16T11:59:00Z" },
        .{ "2023-06-30T23:59:59.123456789012z", "2023-06-30T23:59:59.123456789Z" },
    };
    for (accepted) |case| {
        const t = Time.parse(case[0]) catch |err| {
            std.debug.print("Time.parse rejected \"{s}\"\n", .{case[0]});
            return err;
        };
        try std.testing.expectEqualStrings(case[1], t.format(&buf));
    }
    // 29 February of a leap year is the day after the 28th.
    const leap = try Time.parse("2024-02-29T00:00:00Z");
    const before = try Time.parse("2024-02-28T00:00:00Z");
    try std.testing.expectEqual(@as(i96, std.time.ns_per_day), leap.unix_ns - before.unix_ns);

    // JSON decoding reports the rejected text as a malformed token.
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const S = struct { run_time: Time };
    var p = try toPayload(arena.allocator(), S{ .run_time = leap });
    p.data = "{\"run_time\":\"2023-02-29T00:00:00Z\"}";
    try std.testing.expectError(error.InvalidPayload, fromPayload(S, arena.allocator(), p));
}

test "Time outside the years 0000 to 9999 is written as the nearest bound" {
    const day_ns: i96 = std.time.ns_per_day;
    const cases = .{
        .{ std.math.maxInt(i96), "9999-12-31T23:59:59.999999999Z" },
        .{ std.math.minInt(i96), "0000-01-01T00:00:00Z" },
        .{ proto_time.daysFromCivil(10000, 6, 1) * day_ns, "9999-12-31T23:59:59.999999999Z" },
        .{ proto_time.daysFromCivil(10000, 1, 1) * day_ns, "9999-12-31T23:59:59.999999999Z" },
        .{ proto_time.daysFromCivil(-1, 6, 1) * day_ns, "0000-01-01T00:00:00Z" },
        .{ proto_time.daysFromCivil(0, 1, 1) * day_ns - 1, "0000-01-01T00:00:00Z" },
        .{ proto_time.daysFromCivil(0, 1, 1) * day_ns, "0000-01-01T00:00:00Z" },
        .{ proto_time.daysFromCivil(10000, 1, 1) * day_ns - 1, "9999-12-31T23:59:59.999999999Z" },
    };
    var buf: [Time.max_len]u8 = undefined;
    inline for (cases) |case| {
        const t = Time.fromTimestamp(.{ .nanoseconds = case[0] });
        const text = t.format(&buf);
        try std.testing.expectEqualStrings(case[1], text);
        const back = try Time.parse(text);
        try std.testing.expectEqualStrings(text, back.format(&buf));
    }

    // toPayload of a struct holding such a time succeeds with the clamped text, which decodes.
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const S = struct { run_time: Time };
    const far = try toPayload(arena.allocator(), S{ .run_time = .fromTimestamp(.{ .nanoseconds = std.math.maxInt(i96) }) });
    try std.testing.expectEqualStrings("{\"run_time\":\"9999-12-31T23:59:59.999999999Z\"}", far.data);
    const read = try fromPayload(S, arena.allocator(), far);
    try std.testing.expectEqual(proto_time.daysFromCivil(10000, 1, 1) * day_ns - 1, read.run_time.unix_ns);
    const early = try toPayload(arena.allocator(), S{ .run_time = .fromTimestamp(.{ .nanoseconds = std.math.minInt(i96) }) });
    try std.testing.expectEqualStrings("{\"run_time\":\"0000-01-01T00:00:00Z\"}", early.data);
}

test "fields encode in declaration order and decode by name" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const fields = try toFields(a, .{
        .description = "nightly import",
        .priority = 3,
        .owner = @as(?[]const u8, null),
        .raw = Bytes{ .data = "\x01" },
    });
    try testing.expectEqual(@as(usize, 4), fields.len);
    for (fields, [_][]const u8{ "description", "priority", "owner", "raw" }) |f, name|
        try testing.expectEqualStrings(name, f.name);
    try testing.expectEqualStrings("binary/null", encodingOf(fields[2].value).?);

    try testing.expectEqualStrings("nightly import", (try fieldValue([]const u8, a, fields, "description")).?);
    try testing.expectEqual(@as(?u8, 3), try fieldValue(u8, a, fields, "priority"));
    try testing.expectEqual(@as(??[]const u8, @as(?[]const u8, null)), try fieldValue(?[]const u8, a, fields, "owner"));
    try testing.expectEqualSlices(u8, "\x01", (try fieldValue(Bytes, a, fields, "raw")).?.data);
    try testing.expectEqual(@as(?u8, null), try fieldValue(u8, a, fields, "missing"));
    try testing.expectError(error.InvalidPayload, fieldValue(u8, a, fields, "description"));
    try testing.expectEqual(@as(usize, 0), (try toFields(a, .{})).len);
}

test "fields from a proto map keep wire order and decode by name" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var memo: proto.temporal.api.common.v1.Memo = .{};
    try memo.fields.append(a, .{ .key = "description", .value = try toPayload(a, "nightly import") });
    try memo.fields.append(a, .{ .key = "empty" });
    const fields = try fieldsFromMap(a, memo.fields.items);
    try testing.expectEqual(@as(usize, 2), fields.len);
    try testing.expectEqualStrings("description", fields[0].name);
    try testing.expectEqualStrings("nightly import", (try fieldValue([]const u8, a, fields, "description")).?);
    try testing.expectEqualStrings("empty", fields[1].name);
    try testing.expectEqual(@as(usize, 0), fields[1].value.data.len);
    try testing.expectEqual(@as(usize, 0), (try fieldsFromMap(a, memo.fields.items[0..0])).len);
}

test "a header writes sorted fields that survive the wire and decode by name" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var w: HeaderWriter = .init(a);
    try w.set("tenant", @as([]const u8, "acme"));
    try w.set("attempt", @as(u32, 1));
    try w.set("attempt", @as(u32, 2));
    try w.set("gone", true);
    w.remove("gone");
    try testing.expectEqual(@as(usize, 2), w.header().fields.len);
    try testing.expectEqualStrings("attempt", w.header().fields[0].name);

    // Through the wire format of a proto header and back.
    const ProtoHeader = proto.temporal.api.common.v1.Header;
    const sent: ProtoHeader = .{ .fields = try w.header().toEntries(ProtoHeader.FieldsEntry, a) };
    var bytes: std.Io.Writer.Allocating = .init(a);
    try sent.encode(&bytes.writer, a);
    var reader: std.Io.Reader = .fixed(bytes.written());
    const received = try ProtoHeader.decode(&reader, a);
    std.mem.reverse(ProtoHeader.FieldsEntry, received.fields.items); // wire order doesn't matter
    const h = try Header.fromEntries(a, received.fields.items);

    try testing.expectEqualStrings("acme", (try h.value([]const u8, a, "tenant")).?);
    try testing.expectEqual(@as(?u32, 2), try h.value(u32, a, "attempt"));
    try testing.expectEqual(@as(?u32, null), try h.value(u32, a, "missing"));
    try testing.expectEqual(@as(?Payload, null), h.get("gone"));
    try testing.expectError(error.InvalidPayload, h.value(u32, a, "tenant"));
    try testing.expectEqualStrings("attempt", h.fields[0].name);
}

test "a header writer marks the fields written while encode_fields is set, and the last writer decides" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var w: HeaderWriter = .init(a);
    try w.set("plain", @as(u32, 1));
    w.encode_fields = true;
    try w.set("trace", @as([]const u8, "t"));
    try w.set("baggage", @as([]const u8, "b"));
    try w.set("zone", @as([]const u8, "z"));
    try testing.expect(!w.encodes("plain"));
    try testing.expect(w.encodes("trace"));
    try testing.expect(!w.encodes("missing"));
    try testing.expectEqual(@as(usize, 3), w.codec_fields.items.len);
    try testing.expectEqualStrings("baggage", w.codec_fields.items[0]);
    try testing.expectEqualStrings("zone", w.codec_fields.items[2]);

    // Writing a marked field again unmarked clears its mark, and the other way round; the
    // field keeps one entry either way.
    w.encode_fields = false;
    try w.set("trace", @as([]const u8, "t2"));
    try testing.expect(!w.encodes("trace"));
    w.encode_fields = true;
    try w.set("plain", @as(u32, 2));
    try w.set("plain", @as(u32, 3));
    try testing.expect(w.encodes("plain"));
    try testing.expectEqual(@as(usize, 4), w.header().fields.len);
    try testing.expectEqual(@as(usize, 3), w.codec_fields.items.len);

    // `merge` follows the mode; `remove` drops the mark with the field.
    w.encode_fields = false;
    var other: HeaderWriter = .init(a);
    try other.set("zone", @as([]const u8, "z2"));
    try w.merge(other.header());
    try testing.expect(!w.encodes("zone"));
    w.remove("baggage");
    try testing.expect(!w.encodes("baggage"));
    try testing.expectEqual(@as(?Payload, null), w.header().get("baggage"));
    w.remove("missing");
    try testing.expectEqual(@as(usize, 1), w.codec_fields.items.len);
    try testing.expectEqualStrings("plain", w.codec_fields.items[0]);
}

test "a header writer whose put runs out of memory leaves its fields and marks as they were" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    for (0..4) |fail_index| {
        var failing: std.testing.FailingAllocator = .init(arena.allocator(), .{ .fail_index = fail_index });
        var w: HeaderWriter = .init(failing.allocator());
        w.encode_fields = true;
        var written: usize = 0;
        for ([_][]const u8{ "b", "a", "c", "d" }) |name| {
            w.put(name, .{ .data = name }) catch |err| {
                try testing.expectEqual(error.OutOfMemory, err);
                continue;
            };
            written += 1;
        }
        try testing.expectEqual(written, w.fields.items.len);
        try testing.expectEqual(written, w.codec_fields.items.len);
        for (w.fields.items, w.codec_fields.items) |f, name| try testing.expectEqualStrings(f.name, name);
    }
}

test "EncodedValues decodes arguments one at a time or as a tuple" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const ps = try toPayloads(a, .{ @as([]const u8, "x"), @as(i64, 42) });
    const values: EncodedValues = .{ .payloads = ps.items };
    try testing.expectEqual(@as(usize, 2), values.len());
    try testing.expectEqualStrings("x", try values.get([]const u8, a, 0));
    try testing.expectEqual(@as(i64, 42), try values.get(i64, a, 1));
    try testing.expectEqual(@as(?bool, null), try values.get(?bool, a, 2));
    try testing.expectError(error.WrongArgumentCount, values.get(bool, a, 2));
    try testing.expectError(error.InvalidPayload, values.get(i64, a, 0));
    const args = try values.decode(struct { []const u8, i64 }, a);
    try testing.expectEqual(@as(i64, 42), args[1]);
    try testing.expectError(error.InvalidPayload, values.decode(struct { i64, i64 }, a));
}

test "EncodedValue decodes its value on demand, or reports that it has none" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const none: EncodedValue = .{ .payload = null };
    try testing.expect(!none.hasValue());
    try testing.expectEqual(@as(?i64, null), try none.get(?i64, a));
    try testing.expectError(error.WrongArgumentCount, none.get(i64, a));

    const some: EncodedValue = .{ .payload = try toPayload(a, @as([]const u8, "sleeping")) };
    try testing.expect(some.hasValue());
    try testing.expectEqualStrings("sleeping", try some.get([]const u8, a));
    try testing.expectEqualStrings("sleeping", (try some.get(?[]const u8, a)).?);
    try testing.expectError(error.InvalidPayload, some.get(i64, a));
}

/// The example from the file's header: a type with its own payload conversion.
const Point = struct {
    x: i32,
    y: i32,

    pub fn toPayload(p: Point, arena: Allocator) error{OutOfMemory}!Payload {
        const data = try arena.alloc(u8, 8);
        std.mem.writeInt(i32, data[0..4], p.x, .little);
        std.mem.writeInt(i32, data[4..8], p.y, .little);
        return payloadWithEncoding(arena, "binary/point", data);
    }

    pub fn fromPayload(_: Allocator, p: Payload) DecodeError!Point {
        const enc = encodingOf(p) orelse return error.UnknownEncoding;
        if (!std.mem.eql(u8, enc, "binary/point")) return error.UnknownEncoding;
        if (p.data.len != 8) return error.InvalidPayload;
        return .{
            .x = std.mem.readInt(i32, p.data[0..4], .little),
            .y = std.mem.readInt(i32, p.data[4..8], .little),
        };
    }
};

test "a type's own toPayload and fromPayload take precedence" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const p = try toPayload(a, Point{ .x = -3, .y = 7 });
    try testing.expectEqualStrings("binary/point", encodingOf(p).?);
    try testing.expectEqualSlices(u8, &.{ 0xfd, 0xff, 0xff, 0xff, 7, 0, 0, 0 }, p.data);
    try testing.expectEqual(Point{ .x = -3, .y = 7 }, try fromPayload(Point, a, p));

    // Optionals of the type use the hooks too; null stays binary/null.
    const some = try toPayload(a, @as(?Point, .{ .x = 1, .y = 2 }));
    try testing.expectEqualStrings("binary/point", encodingOf(some).?);
    try testing.expectEqual(@as(?Point, .{ .x = 1, .y = 2 }), try fromPayload(?Point, a, some));
    try testing.expectEqual(@as(?Point, null), try fromPayload(?Point, a, try toPayload(a, @as(?Point, null))));

    // A hook sees every payload, so it decides which encodings it accepts.
    try testing.expectError(error.UnknownEncoding, fromPayload(Point, a, try toPayload(a, @as(u32, 1))));
    const args = try fromPayloads(struct { Point, []const u8 }, a, (try toPayloads(a, .{ Point{ .x = 5, .y = 6 }, @as([]const u8, "z") })).items);
    try testing.expectEqual(@as(i32, 6), args[0].y);
}

/// Records the context its hooks were given: encoding writes the workflow and activity IDs
/// into the payload's metadata, decoding keeps those and the ones it was given.
const Stamped = struct {
    text: []const u8,
    encoded_for: []const u8 = "",
    decoded_for: []const u8 = "",

    fn describe(arena: Allocator, context: *const Context) error{OutOfMemory}![]const u8 {
        return std.fmt.allocPrint(arena, "{s}/{s}/{s}/{s}/{s}/{d}", .{ context.namespace, context.workflow_id, context.workflow_type, context.activity_type, context.activity_id, context.values.count() });
    }

    pub fn toPayloadWithContext(v: Stamped, arena: Allocator, context: *const Context) error{OutOfMemory}!Payload {
        var p = try payloadWithEncoding(arena, "binary/stamped", v.text);
        try p.metadata.append(arena, .{ .key = "for", .value = try describe(arena, context) });
        return p;
    }

    pub fn fromPayloadWithContext(arena: Allocator, context: *const Context, p: Payload) DecodeError!Stamped {
        const enc = encodingOf(p) orelse return error.UnknownEncoding;
        if (!std.mem.eql(u8, enc, "binary/stamped")) return error.UnknownEncoding;
        var encoded_for: []const u8 = "";
        for (p.metadata.items) |e| if (std.mem.eql(u8, e.key, "for")) {
            encoded_for = e.value;
        };
        return .{ .text = try arena.dupe(u8, p.data), .encoded_for = encoded_for, .decoded_for = try describe(arena, context) };
    }
};

/// Declares both pairs: the context pair wins.
const BothPairs = struct {
    n: u8,

    pub fn toPayload(_: BothPairs, arena: Allocator) error{OutOfMemory}!Payload {
        return payloadWithEncoding(arena, "binary/plain-hook", "");
    }

    pub fn fromPayload(_: Allocator, _: Payload) DecodeError!BothPairs {
        return .{ .n = 1 };
    }

    pub fn toPayloadWithContext(_: BothPairs, arena: Allocator, _: *const Context) error{OutOfMemory}!Payload {
        return payloadWithEncoding(arena, "binary/context-hook", "");
    }

    pub fn fromPayloadWithContext(_: Allocator, _: *const Context, _: Payload) DecodeError!BothPairs {
        return .{ .n = 2 };
    }
};

test "a type's context hooks receive the context, or the empty one through the plain functions" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const propagation = @import("propagation.zig");
    var values: propagation.Values = .init(a);
    try values.put(propagation.key([]const u8, "tenant"), "acme");
    const context: Context = .{ .values = &values, .namespace = "ns", .workflow_id = "wf", .workflow_type = "Flow", .activity_type = "Act", .activity_id = "7" };
    const other: Context = .{ .namespace = "ns", .workflow_id = "other" };

    const p = try toPayloadWithContext(a, &context, Stamped{ .text = "hi" });
    const back = try fromPayloadWithContext(Stamped, a, &other, p);
    try testing.expectEqualStrings("hi", back.text);
    try testing.expectEqualStrings("ns/wf/Flow/Act/7/1", back.encoded_for);
    try testing.expectEqualStrings("ns/other////0", back.decoded_for);

    // The plain functions pass the empty context.
    const plain = try fromPayload(Stamped, a, try toPayload(a, Stamped{ .text = "x" }));
    try testing.expectEqualStrings("/////0", plain.encoded_for);
    try testing.expectEqualStrings("/////0", plain.decoded_for);

    // Optionals and argument tuples reach the hooks with the context.
    const some = try fromPayloadWithContext(?Stamped, a, &context, try toPayloadWithContext(a, &context, @as(?Stamped, .{ .text = "o" })));
    try testing.expectEqualStrings("ns/wf/Flow/Act/7/1", some.?.decoded_for);
    try testing.expectEqual(@as(?Stamped, null), try fromPayloadWithContext(?Stamped, a, &context, try toPayloadWithContext(a, &context, @as(?Stamped, null))));
    const list = try toPayloadsWithContext(a, &context, .{ Stamped{ .text = "t" }, @as(u32, 3) });
    const args = try fromPayloadsWithContext(struct { Stamped, u32 }, a, &other, list.items);
    try testing.expectEqualStrings("ns/wf/Flow/Act/7/1", args[0].encoded_for);
    try testing.expectEqualStrings("ns/other////0", args[0].decoded_for);
    try testing.expectEqual(@as(u32, 3), args[1]);

    // Encoded values decode with their context, the empty one unless given.
    const values_list: EncodedValues = .{ .payloads = list.items };
    try testing.expectEqualStrings("/////0", (try values_list.get(Stamped, a, 0)).decoded_for);
    const with = values_list.withContext(&context);
    try testing.expectEqualStrings("ns/wf/Flow/Act/7/1", (try with.get(Stamped, a, 0)).decoded_for);
    try testing.expectEqualStrings("ns/wf/Flow/Act/7/1", (try with.decode(struct { Stamped }, a))[0].decoded_for);
    const one: EncodedValue = .{ .payload = p };
    try testing.expectEqualStrings("/////0", (try one.get(Stamped, a)).decoded_for);
    try testing.expectEqualStrings("ns/other////0", (try one.withContext(&other).get(Stamped, a)).decoded_for);

    // The context pair takes precedence over the plain pair.
    const both = try toPayload(a, BothPairs{ .n = 0 });
    try testing.expectEqualStrings("binary/context-hook", encodingOf(both).?);
    try testing.expectEqual(@as(u8, 2), (try fromPayload(BothPairs, a, both)).n);
    try testing.expectEqual(null, unconvertibleType(Stamped, .encode));
    try testing.expectEqual(null, unconvertibleType(?Stamped, .decode));
}

test "the context changes nothing for built-in encodings and plain hooks" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const context: Context = .{ .namespace = "ns", .workflow_id = "wf", .activity_type = "Act", .activity_id = "1" };
    const Order = struct { id: u32, items: []const []const u8 };
    const order: Order = .{ .id = 4, .items = &.{ "a", "b" } };
    inline for (.{ order, Point{ .x = 1, .y = -1 }, Bytes{ .data = "raw" }, @as(?u8, null), {}, Time{ .unix_ns = 5 } }) |v| {
        const plain = try toPayload(a, v);
        const with = try toPayloadWithContext(a, &context, v);
        try testing.expectEqualSlices(u8, plain.data, with.data);
        try testing.expectEqual(plain.metadata.items.len, with.metadata.items.len);
        for (plain.metadata.items, with.metadata.items) |x, y| {
            try testing.expectEqualStrings(x.key, y.key);
            try testing.expectEqualStrings(x.value, y.value);
        }
    }
    const decoded = try fromPayloadWithContext(Order, a, &context, try toPayload(a, order));
    try testing.expectEqual(@as(u32, 4), decoded.id);
    try testing.expectEqualStrings("b", decoded.items[1]);
}

test "FailureConverter calls its vtable with the context, the error and the failure" {
    const Prefixing = struct {
        last_err: ?anyerror = null,
        last_workflow_id: []const u8 = "",

        fn converter(p: *@This()) FailureConverter {
            return .{ .ptr = p, .vtable = &.{ .toFailure = toFailure, .fromFailure = fromFailure } };
        }
        fn toFailure(ptr: *anyopaque, arena: Allocator, context: *const Context, err: ?anyerror, f: Failure) error{OutOfMemory}!Failure {
            const p: *@This() = @ptrCast(@alignCast(ptr));
            p.last_err = err;
            p.last_workflow_id = context.workflow_id;
            var out = f;
            out.message = try std.fmt.allocPrint(arena, "sent: {s}", .{f.message});
            return out;
        }
        fn fromFailure(ptr: *anyopaque, arena: Allocator, context: *const Context, f: Failure) error{OutOfMemory}!Failure {
            const p: *@This() = @ptrCast(@alignCast(ptr));
            p.last_workflow_id = context.workflow_id;
            var out = f;
            out.message = try std.fmt.allocPrint(arena, "read: {s}", .{f.message});
            return out;
        }
    };
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    var state: Prefixing = .{};
    const c = state.converter();
    const sent = try c.toFailure(a, &.{ .workflow_id = "wf-1" }, error.Boom, .{ .message = "boom", .stack_trace = "trace" });
    try testing.expectEqualStrings("sent: boom", sent.message);
    try testing.expectEqualStrings("trace", sent.stack_trace);
    try testing.expectEqual(@as(?anyerror, error.Boom), state.last_err);
    try testing.expectEqualStrings("wf-1", state.last_workflow_id);
    const read = try c.fromFailure(a, &.{ .workflow_id = "wf-2" }, sent);
    try testing.expectEqualStrings("read: sent: boom", read.message);
    try testing.expectEqualStrings("wf-2", state.last_workflow_id);
}

test "protobuf messages round-trip as json/protobuf and binary/protobuf" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var cause: Failure = .{ .message = "root cause" };
    const f: Failure = .{
        .message = "boom",
        .cause = &cause,
        .failure_info = .{ .application_failure_info = .{ .type = "Oops", .non_retryable = true } },
    };

    const json = try toPayload(a, f);
    try testing.expectEqualStrings("json/protobuf", encodingOf(json).?);
    try testing.expectEqualStrings("messageType", json.metadata.items[1].key);
    try testing.expectEqualStrings("temporal.api.failure.v1.Failure", json.metadata.items[1].value);
    // Standard ProtoJSON: camelCase names and the oneof's field written flat.
    try testing.expectEqualStrings(
        "{\"message\":\"boom\",\"cause\":{\"message\":\"root cause\"},\"applicationFailureInfo\":{\"type\":\"Oops\",\"nonRetryable\":true}}",
        json.data,
    );
    const from_json = try fromPayload(Failure, a, json);
    try testing.expectEqualStrings("boom", from_json.message);
    try testing.expectEqualStrings("root cause", from_json.cause.?.message);
    try testing.expectEqualStrings("Oops", from_json.failure_info.?.application_failure_info.type);

    const binary = try toPayload(a, ProtoBinary(Failure){ .message = f });
    try testing.expectEqualStrings("binary/protobuf", encodingOf(binary).?);
    try testing.expectEqualStrings("temporal.api.failure.v1.Failure", binary.metadata.items[1].value);
    // Either encoding decodes into the message or its wrapper.
    for ([_]Payload{ json, binary }) |p| {
        const back = try fromPayload(ProtoBinary(Failure), a, p);
        try testing.expect(back.message.failure_info.?.application_failure_info.non_retryable);
        try testing.expectEqualStrings("root cause", (try fromPayload(?Failure, a, p)).?.cause.?.message);
    }
    try testing.expectEqual(@as(?Failure, null), try fromPayload(?Failure, a, try toPayload(a, {})));

    const named = try toPayload(a, Proto(Failure, .{ .message_type = "example.v1.Problem" }){ .message = f });
    try testing.expectEqualStrings("example.v1.Problem", named.metadata.items[1].value);
    try testing.expectEqualStrings("temporal.api.common.v1.Payload.MetadataEntry", protoMessageType(Payload.MetadataEntry));

    try testing.expectError(error.UnknownEncoding, fromPayload(Failure, a, try toPayload(a, @as([]const u8, "x"))));
    var corrupt = binary;
    corrupt.data = "\x0a\x0aX"; // a 10-byte message field holding one byte
    try testing.expectError(error.InvalidPayload, fromPayload(Failure, a, corrupt));
}

test "a json/protobuf message holding a Timestamp on 29 February round-trips" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const HistoryEvent = proto.temporal.api.history.v1.HistoryEvent;
    const event: HistoryEvent = .{
        .event_id = 1,
        .event_time = .{ .seconds = 1_709_208_000, .nanos = 5_000_000 }, // 2024-02-29T12:00:00.005Z
        .event_type = .EVENT_TYPE_TIMER_FIRED,
    };
    const p = try toPayload(a, event);
    try testing.expectEqualStrings("json/protobuf", encodingOf(p).?);
    try testing.expectEqualStrings(
        \\{"eventId":"1","eventTime":"2024-02-29T12:00:00.005Z","eventType":"EVENT_TYPE_TIMER_FIRED"}
    , p.data);
    const back = try fromPayload(HistoryEvent, a, p);
    try testing.expectEqual(event.event_time.?.seconds, back.event_time.?.seconds);
    try testing.expectEqual(event.event_time.?.nanos, back.event_time.?.nanos);
}

test "a plain struct holding a protobuf Timestamp on 29 February writes it as std.json would write any other day" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const Timestamp = proto.google.protobuf.Timestamp;
    const Choice = union(enum) { at: Timestamp, none };
    const Node = struct { at: Timestamp, next: ?*const @This() = null };
    const Plain = struct {
        name: []const u8 = "order",
        at: Timestamp,
        maybe: ?Timestamp,
        list: []const Timestamp = &.{},
        pair: struct { Timestamp, u8 } = .{ .{}, 7 },
        choice: Choice = .none,
        node: Node = .{ .at = .{} },
        skipped: void = {},
    };
    const leap_2024: Timestamp = .{ .seconds = 1_709_208_000 }; // 2024-02-29T12:00:00Z
    const leap_2000: Timestamp = .{ .seconds = 951_782_400 }; // 2000-02-29T00:00:00Z

    for ([_]struct { Timestamp, []const u8 }{
        .{ leap_2024, "\"2024-02-29T12:00:00Z\"" },
        .{ leap_2000, "\"2000-02-29T00:00:00Z\"" },
    }) |case| {
        const ts, const text = case;
        const tail: Node = .{ .at = ts };
        const value: Plain = .{
            .at = ts,
            .maybe = ts,
            .list = &.{ ts, ts },
            .pair = .{ ts, 1 },
            .choice = .{ .at = ts },
            .node = .{ .at = ts, .next = &tail },
        };
        const p = try toPayload(a, value);
        try testing.expectEqualStrings("json/plain", encodingOf(p).?);
        try testing.expectEqual(@as(usize, 8), std.mem.count(u8, p.data, text));
        try testing.expect(std.mem.indexOf(u8, p.data, "-03-01") == null);
        const back = try fromPayload(struct { at: Timestamp, maybe: ?Timestamp }, a, p);
        try testing.expectEqual(ts, back.at);
        try testing.expectEqual(ts, back.maybe.?);
    }

    // Every other day is written as the dependency writes it, and the rest of the value as
    // std.json writes it.
    const day = std.time.s_per_day;
    for ([_]Timestamp{
        .{},
        .{ .seconds = 1_700_000_000, .nanos = 123_000_000 },
        .{ .seconds = 1_700_000_000, .nanos = 123_456_000 },
        .{ .seconds = 1_700_000_000, .nanos = 123_456_789 },
        .{ .seconds = proto_time.min_timestamp_seconds },
        .{ .seconds = proto_time.max_timestamp_seconds },
        .{ .seconds = proto_time.daysFromCivil(2023, 2, 28) * day + 3_661 },
        .{ .seconds = proto_time.daysFromCivil(2024, 3, 1) * day },
        .{ .seconds = proto_time.daysFromCivil(2024, 2, 28) * day + 86_399 },
    }) |ts| {
        try testing.expectEqualStrings(
            try std.json.Stringify.valueAlloc(a, ts, .{}),
            (try toPayload(a, ts)).data,
        );
        const tail: Node = .{ .at = ts };
        const value: Plain = .{
            .at = ts,
            .maybe = if (ts.nanos == 0) null else ts,
            .list = &.{ts},
            .pair = .{ ts, 2 },
            .choice = if (ts.seconds == 0) .none else .{ .at = ts },
            .node = .{ .at = ts, .next = &tail },
        };
        try testing.expectEqualStrings(
            try std.json.Stringify.valueAlloc(a, value, .{}),
            (try toPayload(a, value)).data,
        );
    }

    // Outside protobuf's range the write fails, as the dependency's does, without a panic.
    try testing.expectError(error.OutOfMemory, toPayload(a, Plain{ .at = .{ .seconds = proto_time.max_timestamp_seconds + 1 }, .maybe = null }));
    try testing.expectError(error.OutOfMemory, toPayload(a, Plain{ .at = .{}, .maybe = .{ .nanos = -1 } }));

    // A comptime-known Timestamp in an anonymous literal is a comptime field, nested or not.
    const comptime_known = try toPayload(a, .{ .at = leap_2000, .inner = .{ .at = leap_2024, .n = 1 }, .list = [_]Timestamp{leap_2000} });
    try testing.expectEqualStrings(
        "{\"at\":\"2000-02-29T00:00:00Z\",\"inner\":{\"at\":\"2024-02-29T12:00:00Z\",\"n\":1},\"list\":[\"2000-02-29T00:00:00Z\"]}",
        comptime_known.data,
    );

    // A std.json.ArrayHashMap writes itself; its values are written here, nested or not.
    const Step = struct { due: Timestamp, notes: std.json.ArrayHashMap(?Timestamp) = .{} };
    const Plan = struct {
        steps: std.json.ArrayHashMap(Step) = .{},
        maybe: ?std.json.ArrayHashMap(Timestamp) = null,
        empty: std.json.ArrayHashMap(Timestamp) = .{},
        plain: std.json.ArrayHashMap(u8) = .{},
    };
    for ([_]struct { Timestamp, bool }{
        .{ leap_2024, true },                                      .{ leap_2000, true },
        .{ .{ .seconds = 1_700_000_000, .nanos = 1_000 }, false }, .{ .{}, false },
    }) |case| {
        const ts, const leap = case;
        var step: Step = .{ .due = ts };
        try step.notes.map.put(a, "set", ts);
        try step.notes.map.put(a, "unset", null);
        var plan: Plan = .{ .maybe = .{} };
        try plan.steps.map.put(a, "ship", step);
        try plan.maybe.?.map.put(a, "audit", ts);
        try plan.plain.map.put(a, "n", 1);
        const p = try toPayload(a, plan);
        if (leap) {
            var buf: [proto_time.timestamp_text_max]u8 = undefined;
            const text = try proto_time.formatTimestamp(ts, &buf);
            const want = try std.fmt.allocPrint(a,
                \\{{"steps":{{"ship":{{"due":"{0s}","notes":{{"set":"{0s}","unset":null}}}}}},"maybe":{{"audit":"{0s}"}},"empty":{{}},"plain":{{"n":1}}}}
            , .{text});
            try testing.expectEqualStrings(want, p.data);
        } else {
            try testing.expectEqualStrings(try std.json.Stringify.valueAlloc(a, plan, .{}), p.data);
        }
        const back = try fromPayload(Plan, a, p);
        try testing.expectEqual(ts, back.steps.map.get("ship").?.notes.map.get("set").?.?);
        try testing.expectEqual(ts, back.maybe.?.map.get("audit").?);
    }

    // A generated message inside a plain struct is walked field by field, as std.json does.
    const HistoryEvent = proto.temporal.api.history.v1.HistoryEvent;
    const Wrapped = struct { event: HistoryEvent };
    const wrapped = try toPayload(a, Wrapped{ .event = .{ .event_id = 1, .event_time = leap_2024 } });
    try testing.expect(std.mem.indexOf(u8, wrapped.data, "\"event_time\":\"2024-02-29T12:00:00Z\"") != null);
}

test "a value holding a protobuf Timestamp beside comptime fields still encodes as std.json writes it" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const Timestamp = proto.google.protobuf.Timestamp;
    var at: Timestamp = .{ .seconds = 1_709_208_000 }; // 2024-02-29T12:00:00Z
    _ = &at;

    // An anonymous struct literal makes each comptime-known field a comptime field, as in
    // `client.startWorkflow(..., .{ .{ .at = at, .note = "rush" } })`.
    const literal = try toPayload(a, .{ .at = at, .note = "rush", .attempt = 3 });
    try testing.expectEqualStrings("{\"at\":\"2024-02-29T12:00:00Z\",\"note\":\"rush\",\"attempt\":3}", literal.data);

    const Versioned = struct { comptime version: u8 = 2, at: Timestamp };
    const versioned = try toPayload(a, Versioned{ .at = at });
    try testing.expectEqualStrings("{\"version\":2,\"at\":\"2024-02-29T12:00:00Z\"}", versioned.data);
}

test "a std.json.ArrayHashMap of protobuf Timestamps on 29 February encodes without a panic" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const Timestamp = proto.google.protobuf.Timestamp;
    // The map type the samples use for workflow state (samples/shoppingcart), holding Timestamps.
    const Deadlines = struct { by_step: std.json.ArrayHashMap(Timestamp) = .{} };
    try testing.expect(unconvertibleType(Deadlines, .encode) == null);

    var value: Deadlines = .{};
    try value.by_step.map.put(a, "ship", .{ .seconds = 1_709_208_000 }); // 2024-02-29T12:00:00Z
    try value.by_step.map.put(a, "audit", .{ .seconds = 951_782_400 }); // 2000-02-29T00:00:00Z
    const p = try toPayload(a, value);
    try testing.expectEqualStrings(
        "{\"by_step\":{\"ship\":\"2024-02-29T12:00:00Z\",\"audit\":\"2000-02-29T00:00:00Z\"}}",
        p.data,
    );
    const back = try fromPayload(Deadlines, a, p);
    try testing.expectEqual(value.by_step.map.get("ship").?, back.by_step.map.get("ship").?);
    try testing.expectEqual(value.by_step.map.get("audit").?, back.by_step.map.get("audit").?);
}

test "a field named map whose type has a non-type Data declaration encodes as std.json writes it" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    // Only std.json.ArrayHashMap is walked as a map; any other `map` field is a plain field,
    // even when its type declares a `Data` that is not a type.
    const NotAMap = struct {
        pub const Data = 5;
        x: u8,
    };
    const Holder = struct { map: NotAMap, at: proto.google.protobuf.Timestamp = .{} };
    const value: Holder = .{ .map = .{ .x = 9 } };
    try testing.expectEqualStrings(
        try std.json.Stringify.valueAlloc(a, value, .{}),
        (try toPayload(a, value)).data,
    );
    const Plain = struct { map: NotAMap };
    const plain: Plain = .{ .map = .{ .x = 4 } };
    try testing.expectEqualStrings(
        try std.json.Stringify.valueAlloc(a, plain, .{}),
        (try toPayload(a, plain)).data,
    );
}

test "under-aligned and volatile pointers to a protobuf Timestamp on 29 February encode it" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const Timestamp = proto.google.protobuf.Timestamp;
    var at: Timestamp = .{ .seconds = 1_709_208_000 }; // 2024-02-29T12:00:00Z
    var list = [_]Timestamp{ at, .{ .seconds = 951_782_400 } }; // and 2000-02-29T00:00:00Z
    var refs = [_:null]?*const Timestamp{ &at, &list[1] };
    _ = .{ &at, &list, &refs };

    const Pointers = struct {
        under: *align(1) const Timestamp,
        vol: *const volatile Timestamp,
        nested: *align(1) const struct { at: Timestamp },
        slice: []align(1) const Timestamp,
        vol_slice: []const volatile Timestamp,
        many: [*:null]const volatile ?*const Timestamp,
    };
    const nested: struct { at: Timestamp } = .{ .at = at };
    const value: Pointers = .{
        .under = &at,
        .vol = &at,
        .nested = @ptrCast(&nested),
        .slice = &list,
        .vol_slice = &list,
        .many = &refs,
    };
    const p = try toPayload(a, value);
    const leap = "\"2024-02-29T12:00:00Z\"";
    const leap_2000 = "\"2000-02-29T00:00:00Z\"";
    try testing.expectEqualStrings(
        "{\"under\":" ++ leap ++ ",\"vol\":" ++ leap ++ ",\"nested\":{\"at\":" ++ leap ++
            "},\"slice\":[" ++ leap ++ "," ++ leap_2000 ++ "],\"vol_slice\":[" ++ leap ++ "," ++ leap_2000 ++
            "],\"many\":[" ++ leap ++ "," ++ leap_2000 ++ "]}",
        p.data,
    );
    try testing.expect(std.mem.indexOf(u8, p.data, "-03-01") == null);

    // Every other day is written as through an ordinary pointer.
    var other: Timestamp = .{ .seconds = 1_700_000_000, .nanos = 123_000_000 };
    _ = &other;
    const Ordinary = struct { under: *const Timestamp, vol: *const Timestamp };
    const Unusual = struct { under: *align(1) const Timestamp, vol: *const volatile Timestamp };
    try testing.expectEqualStrings(
        (try toPayload(a, Ordinary{ .under = &other, .vol = &other })).data,
        (try toPayload(a, Unusual{ .under = &other, .vol = &other })).data,
    );
}

test "under-aligned struct fields and union payloads holding a protobuf Timestamp on 29 February encode it" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const Timestamp = proto.google.protobuf.Timestamp;
    const at: Timestamp = .{ .seconds = 1_709_208_000 }; // 2024-02-29T12:00:00Z
    // `&value.at` of an align(1) field is a `*align(1) Timestamp`, as an align(1) pointer is.
    const Choice = union(enum) { at: Timestamp align(1), none: void };
    const Fields = struct {
        tag: u8,
        at: Timestamp align(1),
        maybe: ?Timestamp align(1),
        choice: Choice,
    };
    const value: Fields = .{ .tag = 1, .at = at, .maybe = at, .choice = .{ .at = at } };
    const leap = "\"2024-02-29T12:00:00Z\"";
    try testing.expectEqualStrings(
        "{\"tag\":1,\"at\":" ++ leap ++ ",\"maybe\":" ++ leap ++ ",\"choice\":{\"at\":" ++ leap ++ "}}",
        (try toPayload(a, value)).data,
    );
}

test "under-aligned and volatile pointers to arrays and fields of protobuf Timestamps encode each one" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const Timestamp = proto.google.protobuf.Timestamp;
    const at: Timestamp = .{ .seconds = 1_709_208_000 }; // 2024-02-29T12:00:00Z
    const old: Timestamp = .{ .seconds = 951_782_400 }; // 2000-02-29T00:00:00Z
    const Inner = struct { at: Timestamp align(1), n: u8 };
    const Choice = union(enum) { inner: Inner align(1), list: [2]Timestamp align(1) };
    var list = [_]Timestamp{ at, old };
    var inner: Inner = .{ .at = old, .n = 2 };
    _ = .{ &list, &inner };
    const Holder = struct {
        under: *align(1) const [2]Timestamp,
        vol: *const volatile [2]Timestamp,
        inner: *const volatile Inner,
        choice: Choice,
        list: [2]Timestamp align(1),
    };
    const value: Holder = .{
        .under = &list,
        .vol = &list,
        .inner = &inner,
        .choice = .{ .inner = inner },
        .list = list,
    };
    const leap = "\"2024-02-29T12:00:00Z\"";
    const leap_2000 = "\"2000-02-29T00:00:00Z\"";
    const pair = "[" ++ leap ++ "," ++ leap_2000 ++ "]";
    const inner_text = "{\"at\":" ++ leap_2000 ++ ",\"n\":2}";
    try testing.expectEqualStrings(
        "{\"under\":" ++ pair ++ ",\"vol\":" ++ pair ++ ",\"inner\":" ++ inner_text ++
            ",\"choice\":{\"inner\":" ++ inner_text ++ "},\"list\":" ++ pair ++ "}",
        (try toPayload(a, value)).data,
    );
}

test "a json/protobuf message that holds a binary Payload round-trips" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var held: Payload = .{ .data = &.{ 0x00, 0xff, 0x80, 0x7f } };
    try held.metadata.append(a, .{ .key = "encoding", .value = "binary/plain" });
    try held.metadata.append(a, .{ .key = "raw", .value = &.{ 0xff, 0xfe, 0xc0 } });
    var list: Payloads = .{};
    try list.payloads.append(a, held);

    const p = try toPayload(a, list);
    try testing.expectEqualStrings("json/protobuf", encodingOf(p).?);
    try testing.expectEqualStrings(
        \\{"payloads":[{"metadata":{"encoding":"YmluYXJ5L3BsYWlu","raw":"//7A"},"data":"AP+Afw=="}]}
    , p.data);
    const back = (try fromPayload(Payloads, a, p)).payloads.items[0];
    try testing.expectEqualSlices(u8, held.data, back.data);
    try testing.expectEqual(@as(usize, 2), back.metadata.items.len);
    try testing.expectEqualStrings("binary/plain", back.metadata.items[0].value);
    try testing.expectEqualSlices(u8, &.{ 0xff, 0xfe, 0xc0 }, back.metadata.items[1].value);
}

test "a json/protobuf payload with a short enum name decodes" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const TaskQueue = proto.temporal.api.taskqueue.v1.TaskQueue;
    var p = try toPayload(a, TaskQueue{ .name = "orders", .kind = .TASK_QUEUE_KIND_STICKY });
    try testing.expectEqualStrings("{\"name\":\"orders\",\"kind\":\"TASK_QUEUE_KIND_STICKY\"}", p.data);

    p.data = "{\"name\":\"orders\",\"kind\":\"Normal\"}";
    const short = try fromPayload(TaskQueue, a, p);
    try testing.expectEqualStrings("orders", short.name);
    try testing.expectEqual(proto.temporal.api.enums.v1.TaskQueueKind.TASK_QUEUE_KIND_NORMAL, short.kind);

    p.data = "{\"kind\":\"Normal\"";
    try testing.expectError(error.InvalidPayload, fromPayload(TaskQueue, a, p));
}

test "a json/protobuf message whose submessage has only key and value fields round-trips" {
    // As protoc-gen-zig generates `message Secret { string key = 1; bytes value = 2; }` and
    // `message Envelope { Secret secret = 1; map<string, bytes> labels = 2; }` without
    // `preserve_unknown_fields`, its default: `Secret` has exactly the fields of a map entry
    // but is an ordinary message, whose bytes the generated decoder decodes, while `labels`
    // is a real map, whose bytes values it leaves as base64 text. Both must come back as
    // they went in, each decoded exactly once.
    const protobuf = @import("protobuf");
    const Envelope = struct {
        secret: ?Secret = null,
        labels: std.ArrayList(LabelsEntry) = .empty,

        pub const _desc_table = .{
            .secret = protobuf.fd(1, .submessage),
            .labels = protobuf.fd(2, .{ .repeated = .submessage }),
        };

        pub const LabelsEntry = struct {
            key: []const u8 = &.{},
            value: []const u8 = &.{},

            pub const _desc_table = .{
                .key = protobuf.fdf(1, .{ .scalar = .string }, .{ .utf8_validation = .verify }),
                .value = protobuf.fd(2, .{ .scalar = .bytes }),
            };

            pub fn deinit(self: *@This(), allocator: Allocator) void {
                return protobuf.deinit(allocator, self);
            }
        };

        pub const Secret = struct {
            key: []const u8 = &.{},
            value: []const u8 = &.{},

            pub const _desc_table = .{
                .key = protobuf.fdf(1, .{ .scalar = .string }, .{ .utf8_validation = .verify }),
                .value = protobuf.fd(2, .{ .scalar = .bytes }),
            };

            pub fn encode(self: @This(), writer: *std.Io.Writer, allocator: Allocator) (std.Io.Writer.Error || Allocator.Error)!void {
                return protobuf.encode(writer, allocator, self);
            }

            pub fn decode(reader: *std.Io.Reader, allocator: Allocator) (protobuf.DecodingError || std.Io.Reader.Error || Allocator.Error)!@This() {
                return protobuf.decode(@This(), reader, allocator);
            }

            pub fn deinit(self: *@This(), allocator: Allocator) void {
                return protobuf.deinit(allocator, self);
            }

            pub fn jsonDecode(input: []const u8, options: std.json.ParseOptions, allocator: Allocator) !std.json.Parsed(@This()) {
                return protobuf.json.decode(@This(), input, options, allocator);
            }

            pub fn jsonEncode(self: @This(), options: std.json.Stringify.Options, pb_options: protobuf.json.Options, allocator: Allocator) ![]const u8 {
                return protobuf.json.encode(self, options, pb_options, allocator);
            }

            pub fn jsonParse(allocator: Allocator, source: anytype, options: std.json.ParseOptions) !@This() {
                return protobuf.json.parse(@This(), allocator, source, options);
            }
        };

        pub fn encode(self: @This(), writer: *std.Io.Writer, allocator: Allocator) (std.Io.Writer.Error || Allocator.Error)!void {
            return protobuf.encode(writer, allocator, self);
        }

        pub fn decode(reader: *std.Io.Reader, allocator: Allocator) (protobuf.DecodingError || std.Io.Reader.Error || Allocator.Error)!@This() {
            return protobuf.decode(@This(), reader, allocator);
        }

        pub fn deinit(self: *@This(), allocator: Allocator) void {
            return protobuf.deinit(allocator, self);
        }

        pub fn jsonDecode(input: []const u8, options: std.json.ParseOptions, allocator: Allocator) !std.json.Parsed(@This()) {
            return protobuf.json.decode(@This(), input, options, allocator);
        }

        pub fn jsonEncode(self: @This(), options: std.json.Stringify.Options, pb_options: protobuf.json.Options, allocator: Allocator) ![]const u8 {
            return protobuf.json.encode(self, options, pb_options, allocator);
        }

        pub fn jsonParse(allocator: Allocator, source: anytype, options: std.json.ParseOptions) !@This() {
            return protobuf.json.parse(@This(), allocator, source, options);
        }
    };

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    // "abcd" is "YWJjZA==" in base64, and is itself valid base64 of three other bytes.
    var envelope: Envelope = .{ .secret = .{ .key = "k", .value = "abcd" } };
    try envelope.labels.append(a, .{ .key = "iv", .value = &.{ 0xff, 0xfe, 0xfd } });
    const p = try toPayload(a, envelope);
    try testing.expectEqualStrings("json/protobuf", encodingOf(p).?);
    try testing.expectEqualStrings(
        \\{"secret":{"key":"k","value":"YWJjZA=="},"labels":{"iv":"//79"}}
    , p.data);
    const back = try fromPayload(Envelope, a, p);
    try testing.expectEqualStrings("k", back.secret.?.key);
    try testing.expectEqualStrings("abcd", back.secret.?.value);
    try testing.expectEqual(@as(usize, 1), back.labels.items.len);
    try testing.expectEqualStrings("iv", back.labels.items[0].key);
    try testing.expectEqualSlices(u8, &.{ 0xff, 0xfe, 0xfd }, back.labels.items[0].value);
}
