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
//! the workflow ID and type, for an activity's values the activity type and ID, and the
//! propagated values. `toPayloadWithContext`, `fromPayloadWithContext`,
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
//! | workflow and Nexus handler: Nexus input and result           | namespace and propagated values              |
//! | activity: arguments, result, heartbeat details               | the activity's (`activity.Context.converterContext`) |
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
//!
//! Errors become failures (`Failure`) by a fixed step: an error's name, or the
//! `failure.Application` given to `ctx.fail`. A `FailureConverter`, set with
//! `client.Options.failure_converter`, then transforms each failure user code sends and each
//! failure it receives, before user code reads it.

const std = @import("std");
const proto = @import("proto.zig");
const protojson = @import("protojson.zig");
const Allocator = std.mem.Allocator;

pub const Payload = proto.temporal.api.common.v1.Payload;
pub const Payloads = proto.temporal.api.common.v1.Payloads;

const codec = @import("converter/codec.zig");
pub const CodecError = codec.CodecError;
pub const PayloadCodec = codec.PayloadCodec;
pub const Context = codec.Context;
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
    const json = try std.json.Stringify.valueAlloc(arena, value, .{});
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
/// value. Names and payloads are borrowed; the list and encoded values come from `arena`.
/// Not thread-safe.
pub const HeaderWriter = struct {
    arena: Allocator,
    fields: std.ArrayList(Field) = .empty,

    pub fn init(arena: Allocator) HeaderWriter {
        return .{ .arena = arena };
    }

    /// Sets the field `name` to `payload`.
    pub fn put(w: *HeaderWriter, name: []const u8, payload: Payload) error{OutOfMemory}!void {
        const i = std.sort.lowerBound(Field, w.fields.items, name, fieldOrder);
        if (i < w.fields.items.len and std.mem.eql(u8, w.fields.items[i].name, name)) {
            w.fields.items[i].value = payload;
        } else {
            try w.fields.insert(w.arena, i, .{ .name = name, .value = payload });
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

    /// Removes the field `name`, if present.
    pub fn remove(w: *HeaderWriter, name: []const u8) void {
        const i = std.sort.binarySearch(Field, w.fields.items, name, fieldOrder) orelse return;
        _ = w.fields.orderedRemove(i);
    }

    /// The fields written so far; a later write leaves the view's slice as it is only until
    /// the writer reallocates.
    pub fn header(w: *const HeaderWriter) Header {
        return .{ .fields = w.fields.items };
    }
};

fn fieldOrder(name: []const u8, f: Field) std.math.Order {
    return std.mem.order(u8, name, f.name);
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
}

/// A point in time that encodes in JSON as an RFC 3339 string with up to nanosecond
/// precision, such as "2018-08-22T16:30:00Z", the common form for times in payloads.
pub const Time = struct {
    /// Nanoseconds since the Unix epoch.
    unix_ns: i96,

    /// The longest text `format` writes: "-YYYYYY-MM-DDTHH:MM:SS.nnnnnnnnnZ" fits easily.
    pub const max_len = 40;

    pub fn fromTimestamp(ts: std.Io.Timestamp) Time {
        return .{ .unix_ns = ts.nanoseconds };
    }

    pub fn toTimestamp(t: Time) std.Io.Timestamp {
        return .{ .nanoseconds = t.unix_ns };
    }

    /// Writes `t` in UTC, with trailing zeros of the fraction dropped.
    pub fn format(t: Time, buf: *[max_len]u8) []const u8 {
        const secs: i64 = @intCast(@divFloor(t.unix_ns, std.time.ns_per_s));
        const nanos: u32 = @intCast(@mod(t.unix_ns, std.time.ns_per_s));
        const days = @divFloor(secs, std.time.s_per_day);
        const sod: u32 = @intCast(@mod(secs, std.time.s_per_day));
        const date = civilFromDays(days);
        var w: std.Io.Writer = .fixed(buf);
        w.print("{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}", .{
            @as(u64, @intCast(@max(date.year, 0))), date.month, date.day, sod / 3600, sod / 60 % 60, sod % 60,
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

    /// Parses an RFC 3339 time ("2006-01-02T15:04:05.999999999Z07:00"). A time whose offset
    /// moves it out of the years 0000 to 9999 in UTC is `error.InvalidTime`, since `format`
    /// can't write it.
    pub fn parse(text: []const u8) error{InvalidTime}!Time {
        if (text.len < 20 or text[4] != '-' or text[7] != '-' or (text[10] != 'T' and text[10] != 't') or text[13] != ':' or text[16] != ':')
            return error.InvalidTime;
        const year = try digitsOf(text[0..4]);
        const month = try digitsOf(text[5..7]);
        const day = try digitsOf(text[8..10]);
        const hour = try digitsOf(text[11..13]);
        const minute = try digitsOf(text[14..16]);
        const second = try digitsOf(text[17..19]);
        if (month < 1 or month > 12 or day < 1 or day > 31 or hour > 23 or minute > 59 or second > 60) return error.InvalidTime;
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
                const off = @as(i64, try digitsOf(text[i + 1 .. i + 3])) * 3600 + @as(i64, try digitsOf(text[i + 4 .. i + 6])) * 60;
                offset_s = if (text[i] == '-') -off else off;
                i += 6;
            },
            else => return error.InvalidTime,
        }
        if (i != text.len) return error.InvalidTime;
        const days = daysFromCivil(year, month, day);
        const secs = days * std.time.s_per_day + @as(i64, hour) * 3600 + @as(i64, minute) * 60 + second - offset_s;
        const first_secs = comptime daysFromCivil(0, 1, 1) * std.time.s_per_day;
        const end_secs = comptime daysFromCivil(10000, 1, 1) * std.time.s_per_day;
        if (secs < first_secs or secs >= end_secs) return error.InvalidTime;
        return .{ .unix_ns = @as(i96, secs) * std.time.ns_per_s + nanos };
    }

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

    fn digitsOf(s: []const u8) error{InvalidTime}!u32 {
        return std.fmt.parseUnsigned(u32, s, 10) catch error.InvalidTime;
    }

    // Howard Hinnant's days_from_civil / civil_from_days, valid for the proleptic Gregorian
    // calendar in both directions from the epoch.
    fn daysFromCivil(y0: i64, m: u32, d: u32) i64 {
        const y = if (m <= 2) y0 - 1 else y0;
        const era = @divFloor(y, 400);
        const yoe = y - era * 400;
        const mp: i64 = if (m > 2) m - 3 else m + 9;
        const doy = @divFloor(153 * mp + 2, 5) + d - 1;
        const doe = yoe * 365 + @divFloor(yoe, 4) - @divFloor(yoe, 100) + doy;
        return era * 146097 + doe - 719468;
    }

    fn civilFromDays(z0: i64) struct { year: i64, month: u32, day: u32 } {
        const z = z0 + 719468;
        const era = @divFloor(z, 146097);
        const doe = z - era * 146097;
        const yoe = @divFloor(doe - @divFloor(doe, 1460) + @divFloor(doe, 36524) - @divFloor(doe, 146096), 365);
        const doy = doe - (365 * yoe + @divFloor(yoe, 4) - @divFloor(yoe, 100));
        const mp = @divFloor(5 * doy + 2, 153);
        const day: u32 = @intCast(doy - @divFloor(153 * mp + 2, 5) + 1);
        const month: u32 = @intCast(if (mp < 10) mp + 3 else mp - 9);
        return .{ .year = yoe + era * 400 + @intFromBool(month <= 2), .month = month, .day = day };
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
