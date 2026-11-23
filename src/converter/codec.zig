//! Payload codecs: transforms of encoded payloads, such as compression or encryption, applied
//! after conversion on the way out and before it on the way in. Re-exported by `converter`.

const std = @import("std");
const proto = @import("../proto.zig");
const converter = @import("../converter.zig");
const propagation = @import("../propagation.zig");
const system = @import("../nexus/system.zig");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const Payload = converter.Payload;
const flate = std.compress.flate;

const log = std.log.scoped(.temporal);

/// `CodecFailed` covers payloads a codec can't transform (corrupt or foreign data, a missing
/// key); the codec logs why under the `temporal` scope. `encodeMessage` and `decodeMessage`
/// also return it, logged, for a payload marked `__temporal_system_payload: "true"` of a
/// message type the SDK doesn't know.
pub const CodecError = error{ OutOfMemory, Canceled, CodecFailed };

/// What a conversion is for: the call whose payloads a codec encodes or decodes, or whose
/// value a type's `toPayloadWithContext` / `fromPayloadWithContext` hooks convert, for code
/// that behaves differently per workflow or per tenant (an encryption codec picking its key,
/// for example). Every field may be empty: a call that names no workflow leaves the workflow
/// fields empty, and the codec server (`codecHandler`) and `encodePayloads` pass no context
/// at all.
///
/// Header payloads go through codecs like any other, so a codec can't read a propagated
/// value from the header it is encoding; it reads it from `values` instead. Everything is
/// borrowed for the duration of the codec or hook call only.
pub const Context = struct {
    /// The call's propagated values (`temporal.propagation`): a client call's `.context`, the
    /// values a workflow run extracted from its start header (`ctx.propagated()` in the
    /// workflow), or those an activity extracted from its header. Empty when there are none,
    /// and for the activation that starts a run while its own header is being decoded.
    values: *const propagation.Values = &.empty,
    /// The client's or worker's namespace.
    namespace: []const u8 = "",
    /// The workflow the payloads belong to: the one a client request names, the running
    /// workflow, or the workflow that scheduled an activity.
    ///
    /// A child workflow's payloads carry the child's ID and type on both sides: on the
    /// parent's worker, its start command's arguments, memo and header, and its result and
    /// failure in the resolutions; on the child's own worker, its run like any other.
    /// Signals sent to other workflows, children included, carry the sending run's identity.
    /// A child that continues as new under another workflow type encodes its last result with
    /// the new type, while its parent decodes it with the type it started.
    workflow_id: []const u8 = "",
    /// The type of the workflow, when the call names it (a start, a workflow's activations,
    /// an activity's task, a child's payloads as for `workflow_id`).
    workflow_type: []const u8 = "",
    /// For an activity's or local activity's payloads, the activity type; empty otherwise.
    /// Set on both sides: on the workflow's worker, for its arguments in the schedule
    /// command and its result and failure in the resolution; on the activity's worker, for
    /// its input, result, failure and heartbeats; and for a client call that names an
    /// activity. Both sides see the type and ID the activity was scheduled with. A side
    /// effect (`workflow.Context.sideEffect`) is not an activity's payload: it carries its
    /// run's identity, without propagated values, on both sides.
    activity_type: []const u8 = "",
    /// For an activity's or local activity's payloads, the activity ID, set as
    /// `activity_type` is; empty otherwise.
    activity_id: []const u8 = "",
};

/// Deprecated: use `converter.Context`.
pub const CodecContext = Context;

/// A transform of whole payload lists, plugged in at run time: `ptr` is the codec's state
/// and `vtable` its functions. Set codecs with `client.Options.codecs`; the client and every
/// worker on it apply them.
///
/// `encode` and `decode` take a list of payloads and return the transformed list, allocated
/// from `arena` (a returned payload may borrow its input). They must return as many payloads
/// as they were given, in the same order, and must leave payloads they don't recognize
/// unchanged: decoding passes through payloads a codec didn't encode, so a codec sees plain
/// payloads too. Both may be called from several threads at once, never from workflow code;
/// `io` is the client's or worker's.
///
/// A codec that needs to know whose payloads it transforms also sets `encodeWithContext`
/// and `decodeWithContext`, which the client and workers call instead with a
/// `Context`. `encode` and `decode` remain required: they serve calls with no context,
/// such as the codec server's.
pub const PayloadCodec = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        encode: *const fn (ptr: *anyopaque, arena: Allocator, io: Io, payloads: []const Payload) CodecError![]Payload,
        decode: *const fn (ptr: *anyopaque, arena: Allocator, io: Io, payloads: []const Payload) CodecError![]Payload,
        /// Used instead of `encode` when the caller has a context; `context` is borrowed for
        /// the call.
        encodeWithContext: ?*const fn (ptr: *anyopaque, arena: Allocator, io: Io, context: *const Context, payloads: []const Payload) CodecError![]Payload = null,
        /// Used instead of `decode` when the caller has a context; `context` is borrowed for
        /// the call.
        decodeWithContext: ?*const fn (ptr: *anyopaque, arena: Allocator, io: Io, context: *const Context, payloads: []const Payload) CodecError![]Payload = null,
    };

    pub fn encode(c: PayloadCodec, arena: Allocator, io: Io, payloads: []const Payload) CodecError![]Payload {
        return c.vtable.encode(c.ptr, arena, io, payloads);
    }

    pub fn decode(c: PayloadCodec, arena: Allocator, io: Io, payloads: []const Payload) CodecError![]Payload {
        return c.vtable.decode(c.ptr, arena, io, payloads);
    }

    /// `encodeWithContext` from the vtable, or `encode` for a codec without one.
    pub fn encodeWithContext(c: PayloadCodec, arena: Allocator, io: Io, context: *const Context, payloads: []const Payload) CodecError![]Payload {
        if (c.vtable.encodeWithContext) |f| return f(c.ptr, arena, io, context, payloads);
        return c.vtable.encode(c.ptr, arena, io, payloads);
    }

    /// `decodeWithContext` from the vtable, or `decode` for a codec without one.
    pub fn decodeWithContext(c: PayloadCodec, arena: Allocator, io: Io, context: *const Context, payloads: []const Payload) CodecError![]Payload {
        if (c.vtable.decodeWithContext) |f| return f(c.ptr, arena, io, context, payloads);
        return c.vtable.decode(c.ptr, arena, io, payloads);
    }
};

/// Encodes `payloads` with each codec of the chain `codecs` in order, without a context.
/// Memory comes from `arena`; with no codecs the result is a copy of the list.
pub fn encodePayloads(codecs: []const PayloadCodec, arena: Allocator, io: Io, payloads: []const Payload) CodecError![]Payload {
    return chainPayloads(codecs, arena, io, null, payloads, true);
}

/// Decodes `payloads` with each codec of the chain `codecs` in reverse order, undoing
/// `encodePayloads`. Memory comes from `arena`.
pub fn decodePayloads(codecs: []const PayloadCodec, arena: Allocator, io: Io, payloads: []const Payload) CodecError![]Payload {
    return chainPayloads(codecs, arena, io, null, payloads, false);
}

/// `encodePayloads` with `context` passed to each codec (`PayloadCodec.encodeWithContext`).
pub fn encodePayloadsWithContext(codecs: []const PayloadCodec, arena: Allocator, io: Io, context: *const Context, payloads: []const Payload) CodecError![]Payload {
    return chainPayloads(codecs, arena, io, context, payloads, true);
}

/// `decodePayloads` with `context` passed to each codec (`PayloadCodec.decodeWithContext`).
pub fn decodePayloadsWithContext(codecs: []const PayloadCodec, arena: Allocator, io: Io, context: *const Context, payloads: []const Payload) CodecError![]Payload {
    return chainPayloads(codecs, arena, io, context, payloads, false);
}

fn chainPayloads(codecs: []const PayloadCodec, arena: Allocator, io: Io, context: ?*const Context, payloads: []const Payload, encode: bool) CodecError![]Payload {
    var current = try arena.dupe(Payload, payloads);
    if (encode) {
        for (codecs) |c| {
            const out = if (context) |ctx| try c.encodeWithContext(arena, io, ctx, current) else try c.encode(arena, io, current);
            current = try checkCount(payloads.len, out, "encoding");
        }
    } else {
        var i = codecs.len;
        while (i > 0) {
            i -= 1;
            const c = codecs[i];
            const out = if (context) |ctx| try c.decodeWithContext(arena, io, ctx, current) else try c.decode(arena, io, current);
            current = try checkCount(payloads.len, out, "decoding");
        }
    }
    return current;
}

fn checkCount(want: usize, got: []Payload, comptime what: []const u8) CodecError![]Payload {
    if (got.len == want) return got;
    log.warn(what ++ " payloads failed: a codec returned {d} payloads for {d}; codecs must return one payload for each they are given", .{ got.len, want });
    return error.CodecFailed;
}

/// Encodes every payload in the protobuf message `msg` with `codecs` (see `visitPayloads`
/// for which ones), replacing them in place, passing `context` to each codec. Memory comes
/// from `arena`; `msg` may borrow memory it doesn't own, which is left unchanged. No codecs
/// leaves `msg` as it is. A payload marked `__temporal_system_payload: "true"` (a system
/// Nexus envelope) is not given to the codecs; the payloads inside it are, with the context
/// of the workflow its request names. A marked payload of a message type the SDK doesn't
/// know is `error.CodecFailed` (logged).
pub fn encodeMessage(comptime T: type, msg: *T, codecs: []const PayloadCodec, arena: Allocator, io: Io, context: *const Context) CodecError!void {
    if (codecs.len == 0) return;
    return visitPayloads(T, msg, arena, ChainVisitor{ .codecs = codecs, .arena = arena, .io = io, .context = context, .encode = true });
}

/// Decodes every payload in the protobuf message `msg` with `codecs`, as `encodeMessage`. A
/// payload marked `__temporal_system_payload: "true"` is not given to the codecs; the
/// payloads inside it are, with the context of the workflow its request names. A marked
/// payload of a message type the SDK doesn't know is `error.CodecFailed` (logged).
pub fn decodeMessage(comptime T: type, msg: *T, codecs: []const PayloadCodec, arena: Allocator, io: Io, context: *const Context) CodecError!void {
    if (codecs.len == 0) return;
    return visitPayloads(T, msg, arena, ChainVisitor{ .codecs = codecs, .arena = arena, .io = io, .context = context, .encode = false });
}

const ChainVisitor = struct {
    codecs: []const PayloadCodec,
    arena: Allocator,
    io: Io,
    context: *const Context,
    encode: bool,
    /// How many system envelopes enclose the payloads being visited.
    depth: u8 = 0,

    /// The deepest nesting of system envelopes the pass opens. The SDK writes one level;
    /// the limit keeps a payload's author, who decides how deep envelopes nest, from
    /// overflowing the stack of the thread doing the pass.
    const max_system_payload_depth = 8;

    pub fn visit(v: ChainVisitor, payloads: []const Payload) CodecError![]Payload {
        const any_marked = for (payloads) |p| {
            if (system.isMarked(p)) break true;
        } else false;
        if (!any_marked) return chainPayloads(v.codecs, v.arena, v.io, v.context, payloads, v.encode);

        // The unmarked payloads are still coded together in one call, and go back to their
        // positions; each marked one is opened instead of coded.
        var plain: std.ArrayList(Payload) = .empty;
        for (payloads) |p| if (!system.isMarked(p)) try plain.append(v.arena, p);
        const coded = if (plain.items.len == 0)
            plain.items
        else
            try chainPayloads(v.codecs, v.arena, v.io, v.context, plain.items, v.encode);
        const out = try v.arena.alloc(Payload, payloads.len);
        var next: usize = 0;
        for (payloads, out) |p, *o| {
            if (system.isMarked(p)) {
                o.* = try v.visitSystemPayload(p);
            } else {
                o.* = coded[next];
                next += 1;
            }
        }
        return out;
    }

    /// Codes the payloads inside the system Nexus envelope `p` and encodes it again with its
    /// metadata kept; the envelope's own bytes never reach a codec. A request's payloads get
    /// the context of the workflow it names.
    fn visitSystemPayload(v: ChainVisitor, p: Payload) CodecError!Payload {
        const what = if (v.encode) "encoding" else "decoding";
        if (v.depth >= max_system_payload_depth) {
            log.warn("{s} payloads failed: Temporal system payloads (marked {s}: \"{s}\") are nested more than {d} deep", .{ what, system.mark_key, system.mark_value, max_system_payload_depth });
            return error.CodecFailed;
        }
        var inner = v;
        inner.depth += 1;
        const message = system.decode(v.arena, p) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            error.UnknownMessageType => {
                log.warn("{s} payloads failed: a payload marked {s}: \"{s}\" has message type \"{s}\", which is not a Temporal system payload this SDK knows", .{ what, system.mark_key, system.mark_value, system.messageTypeOf(p) orelse "" });
                return error.CodecFailed;
            },
            error.InvalidPayload => {
                log.warn("{s} payloads failed: a payload marked {s}: \"{s}\" is not binary/protobuf of its message type \"{s}\"", .{ what, system.mark_key, system.mark_value, system.messageTypeOf(p) orelse "" });
                return error.CodecFailed;
            },
        };
        switch (message) {
            .signal_with_start_request => |request| {
                var req = request;
                const target = system.targetContext(v.context, &req);
                inner.context = &target;
                try visitPayloads(system.SignalWithStartRequest, &req, v.arena, inner);
                return system.reencode(v.arena, p, .{ .signal_with_start_request = req });
            },
            .signal_with_start_response => |response| {
                var resp = response;
                try visitPayloads(system.SignalWithStartResponse, &resp, v.arena, inner);
                return system.reencode(v.arena, p, .{ .signal_with_start_response = resp });
            },
        }
    }
};

/// Calls `visitor.visit(payloads: []const Payload) CodecError![]Payload` on every payload in
/// the generated protobuf message `value` and stores what it returns in their place: once
/// with the whole list for a repeated payload field (`Payloads`, for example), and once with
/// a one-payload list for each single payload (a memo or header value, a failure's
/// `encoded_attributes`). It reaches payloads in nested and repeated messages, oneofs, maps,
/// and recursive messages such as a failure's causes. Search attributes are skipped: the
/// server reads them, so they stay as the converter wrote them.
///
/// Visiting copies, into `arena`, every list and pointed-to message on the way to a payload
/// before replacing anything, so memory `value` borrows is never written. The visitor must
/// return as many payloads as it was given.
pub fn visitPayloads(comptime T: type, value: *T, arena: Allocator, visitor: anytype) CodecError!void {
    if (comptime !mayHoldPayloads(T)) return;
    if (T == Payload) {
        const out = try checkCount(1, try visitor.visit(@as(*const [1]Payload, value)), "visiting");
        value.* = out[0];
        return;
    }
    switch (@typeInfo(T)) {
        .optional => |o| if (value.*) |*inner| try visitPayloads(o.child, inner, arena, visitor),
        .pointer => |p| {
            const copy = try arena.create(p.child);
            copy.* = value.*.*;
            try visitPayloads(p.child, copy, arena, visitor);
            value.* = copy;
        },
        .@"union" => switch (value.*) {
            inline else => |*field| try visitPayloads(@TypeOf(field.*), field, arena, visitor),
        },
        .@"struct" => |s| if (comptime isArrayList(T)) {
            const E = @typeInfo(@FieldType(T, "items")).pointer.child;
            const items = if (E == Payload)
                try checkCount(value.items.len, try visitor.visit(value.items), "visiting")
            else
                try arena.dupe(E, value.items);
            if (E != Payload) for (items) |*item| try visitPayloads(E, item, arena, visitor);
            value.* = .fromOwnedSlice(items);
        } else {
            inline for (s.field_names) |name| try visitPayloads(@FieldType(T, name), &@field(value, name), arena, visitor);
        },
        else => {},
    }
}

fn isArrayList(comptime T: type) bool {
    return @typeInfo(T) == .@"struct" and @hasField(T, "items") and @hasField(T, "capacity");
}

/// Whether a value of `T` can reach a payload other than a search attribute.
fn mayHoldPayloads(comptime T: type) bool {
    @setEvalBranchQuota(1_000_000);
    if (T == Payload) return true;
    if (T == proto.temporal.api.common.v1.SearchAttributes) return false;
    return switch (@typeInfo(T)) {
        .optional => |o| mayHoldPayloads(o.child),
        // Generated messages refer to themselves only through single-item pointers (a
        // failure's `cause`); answering yes here keeps the check from recursing forever.
        .pointer => |p| p.size == .one,
        .@"struct" => |s| blk: {
            if (isArrayList(T)) break :blk mayHoldPayloads(@typeInfo(@FieldType(T, "items")).pointer.child);
            for (s.field_types) |F| if (mayHoldPayloads(F)) break :blk true;
            break :blk false;
        },
        .@"union" => |u| blk: {
            for (u.field_types) |F| if (mayHoldPayloads(F)) break :blk true;
            break :blk false;
        },
        else => false,
    };
}

/// Compresses payloads with zlib (`std.compress.flate`): each payload is serialized whole
/// and replaced by one with encoding `binary/zlib` holding the compressed bytes, unless
/// compression doesn't make it smaller. Decoding inflates `binary/zlib` payloads and passes
/// others through. Use it as `client.Options.codecs = &.{zlib.codec()}`; it must outlive the
/// client.
pub const ZlibCodec = struct {
    /// Compress every payload, even when that makes it larger.
    always_encode: bool = false,
    level: flate.Compress.Options = .default,
    /// Decoding fails (`error.CodecFailed`) for a payload that inflates to more bytes than
    /// this, so corrupt or hostile data can't exhaust memory.
    max_decoded_len: usize = 256 * 1024 * 1024,

    pub const encoding = "binary/zlib";

    pub fn codec(z: *const ZlibCodec) PayloadCodec {
        return .{ .ptr = @constCast(z), .vtable = &.{ .encode = encode, .decode = decode } };
    }

    fn encode(ptr: *anyopaque, arena: Allocator, _: Io, payloads: []const Payload) CodecError![]Payload {
        const z: *const ZlibCodec = @ptrCast(@alignCast(ptr));
        const out = try arena.dupe(Payload, payloads);
        if (payloads.len == 0) return out;
        // One compressor (a large struct) and window for the whole list.
        const compressor = try arena.create(flate.Compress);
        const window = try arena.alloc(u8, flate.max_window_len);
        for (out) |*p| {
            var serialized: Io.Writer.Allocating = .init(arena);
            p.encode(&serialized.writer, arena) catch return error.OutOfMemory;
            const plain = serialized.written();
            var compressed: Io.Writer.Allocating = try .initCapacity(arena, plain.len / 2 + 64);
            compressor.* = flate.Compress.init(&compressed.writer, window, .zlib, z.level) catch return error.OutOfMemory;
            compressor.writer.writeAll(plain) catch return error.OutOfMemory;
            compressor.finish() catch return error.OutOfMemory;
            const data = compressed.written();
            if (!z.always_encode and data.len >= plain.len) continue;
            p.* = try converter.payloadWithEncoding(arena, encoding, data);
        }
        return out;
    }

    fn decode(ptr: *anyopaque, arena: Allocator, _: Io, payloads: []const Payload) CodecError![]Payload {
        const z: *const ZlibCodec = @ptrCast(@alignCast(ptr));
        const out = try arena.dupe(Payload, payloads);
        var window: []u8 = &.{};
        for (out, 0..) |*p, i| {
            const enc = converter.encodingOf(p.*) orelse continue;
            if (!std.mem.eql(u8, enc, encoding)) continue;
            if (window.len == 0) window = try arena.alloc(u8, flate.max_window_len);
            var in: Io.Reader = .fixed(p.data);
            var inflate: flate.Decompress = .init(&in, .zlib, window);
            const plain = inflate.reader.allocRemaining(arena, .limited(z.max_decoded_len)) catch |err| switch (err) {
                error.OutOfMemory => return error.OutOfMemory,
                error.StreamTooLong => {
                    log.warn("decoding payload {d} with the zlib codec failed: it inflates to more than max_decoded_len ({d} bytes)", .{ i, z.max_decoded_len });
                    return error.CodecFailed;
                },
                error.ReadFailed => {
                    log.warn("decoding payload {d} with the zlib codec failed: its data is not a zlib stream ({t})", .{ i, inflate.err orelse error.ReadFailed });
                    return error.CodecFailed;
                },
            };
            var reader: Io.Reader = .fixed(plain);
            p.* = Payload.decode(&reader, arena) catch |err| switch (err) {
                error.OutOfMemory => return error.OutOfMemory,
                else => {
                    log.warn("decoding payload {d} with the zlib codec failed: the inflated data is not a serialized payload ({t})", .{ i, err });
                    return error.CodecFailed;
                },
            };
        }
        return out;
    }
};

// ---------------------------------------------------------------- tests

const testing = std.testing;

test {
    _ = system;
}

/// Replaces each payload with one of encoding `name` holding the original, serialized and
/// reversed; decoding undoes payloads of encoding `name` only.
const MarkingCodec = struct {
    name: []const u8,
    calls: std.atomic.Value(u32) = .init(0),

    fn codec(m: *MarkingCodec) PayloadCodec {
        return .{ .ptr = m, .vtable = &.{ .encode = encode, .decode = decode } };
    }

    fn encode(ptr: *anyopaque, arena: Allocator, _: Io, payloads: []const Payload) CodecError![]Payload {
        const m: *MarkingCodec = @ptrCast(@alignCast(ptr));
        _ = m.calls.fetchAdd(1, .monotonic);
        const out = try arena.alloc(Payload, payloads.len);
        for (payloads, out) |p, *o| {
            var serialized: Io.Writer.Allocating = .init(arena);
            p.encode(&serialized.writer, arena) catch return error.OutOfMemory;
            std.mem.reverse(u8, serialized.written());
            o.* = try converter.payloadWithEncoding(arena, m.name, serialized.written());
        }
        return out;
    }

    fn decode(ptr: *anyopaque, arena: Allocator, _: Io, payloads: []const Payload) CodecError![]Payload {
        const m: *MarkingCodec = @ptrCast(@alignCast(ptr));
        const out = try arena.dupe(Payload, payloads);
        for (out) |*p| {
            if (!std.mem.eql(u8, converter.encodingOf(p.*) orelse "", m.name)) continue;
            const data = try arena.dupe(u8, p.data);
            std.mem.reverse(u8, data);
            var reader: Io.Reader = .fixed(data);
            p.* = Payload.decode(&reader, arena) catch return error.CodecFailed;
        }
        return out;
    }
};

test "the zlib codec round-trips payloads and skips those compression doesn't shrink" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const zlib: ZlibCodec = .{};

    const text = try repeat(a, "squeeze ", 200);
    const big = try converter.toPayload(a, text);
    const small = try converter.toPayload(a, @as(u8, 1));
    const encoded = try zlib.codec().encode(a, testing.io, &.{ big, small });
    try testing.expectEqual(@as(usize, 2), encoded.len);
    try testing.expectEqualStrings("binary/zlib", converter.encodingOf(encoded[0]).?);
    try testing.expect(encoded[0].data.len < big.data.len / 10);
    // Compressing one byte of JSON would only add zlib's header and checksum.
    try testing.expectEqualStrings("json/plain", converter.encodingOf(encoded[1]).?);
    try testing.expectEqualStrings("1", encoded[1].data);

    const decoded = try zlib.codec().decode(a, testing.io, encoded);
    try testing.expectEqualStrings(text, try converter.fromPayload([]const u8, a, decoded[0]));
    try testing.expectEqual(@as(u8, 1), try converter.fromPayload(u8, a, decoded[1]));

    const always: ZlibCodec = .{ .always_encode = true };
    const forced = try always.codec().encode(a, testing.io, &.{small});
    try testing.expectEqualStrings("binary/zlib", converter.encodingOf(forced[0]).?);
    try testing.expectEqual(@as(u8, 1), try converter.fromPayload(u8, a, (try zlib.codec().decode(a, testing.io, forced))[0]));

    var corrupt = encoded[0];
    corrupt.data = "not zlib at all";
    try testing.expectError(error.CodecFailed, zlib.codec().decode(a, testing.io, &.{corrupt}));
    const tight: ZlibCodec = .{ .max_decoded_len = 100 };
    try testing.expectError(error.CodecFailed, tight.codec().decode(a, testing.io, encoded[0..1]));
}

test "a codec chain encodes in order and decodes in reverse" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var first: MarkingCodec = .{ .name = "test/first" };
    var second: MarkingCodec = .{ .name = "test/second" };
    const chain = [_]PayloadCodec{ first.codec(), second.codec() };

    const original = try converter.toPayloads(a, .{ @as([]const u8, "abc"), @as(i64, 12) });
    const encoded = try encodePayloads(&chain, a, testing.io, original.items);
    // The last codec in the chain wraps the others' output.
    try testing.expectEqualStrings("test/second", converter.encodingOf(encoded[0]).?);
    try testing.expectEqualStrings("json/plain", converter.encodingOf(original.items[0]).?);

    // Decoding only the outer codec leaves the inner one's payload.
    const half = try decodePayloads(chain[1..], a, testing.io, encoded);
    try testing.expectEqualStrings("test/first", converter.encodingOf(half[0]).?);

    const decoded = try decodePayloads(&chain, a, testing.io, encoded);
    const values = try converter.fromPayloads(struct { []const u8, i64 }, a, decoded);
    try testing.expectEqualStrings("abc", values[0]);
    try testing.expectEqual(@as(i64, 12), values[1]);
}

fn repeat(a: Allocator, text: []const u8, n: usize) ![]const u8 {
    const out = try a.alloc(u8, text.len * n);
    for (0..n) |i| @memcpy(out[i * text.len ..][0..text.len], text);
    return out;
}

/// Counts the payloads it visits and marks each so the test can see it was replaced.
const CountingVisitor = struct {
    arena: Allocator,
    count: *usize,

    pub fn visit(v: CountingVisitor, payloads: []const Payload) CodecError![]Payload {
        v.count.* += payloads.len;
        const out = try v.arena.dupe(Payload, payloads);
        for (out) |*p| p.* = try converter.payloadWithEncoding(v.arena, "test/visited", p.data);
        return out;
    }
};

fn countPayloads(comptime T: type, value: *T, arena: Allocator) !usize {
    var count: usize = 0;
    try visitPayloads(T, value, arena, CountingVisitor{ .arena = arena, .count = &count });
    return count;
}

fn failureWithDetails(a: Allocator, message: []const u8) !proto.temporal.api.failure.v1.Failure {
    var details: proto.temporal.api.common.v1.Payloads = .{};
    try details.payloads.append(a, try converter.toPayload(a, message));
    try details.payloads.append(a, try converter.toPayload(a, @as(i64, 7)));
    return .{
        .message = message,
        .encoded_attributes = try converter.toPayload(a, .{ .message = message }),
        .failure_info = .{ .application_failure_info = .{ .type = "Test", .details = details } },
    };
}

test "visitPayloads reaches every payload of a failure, including its causes" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const cause = try a.create(proto.temporal.api.failure.v1.Failure);
    cause.* = try failureWithDetails(a, "cause");
    var f = try failureWithDetails(a, "top");
    f.cause = cause;
    // 2 details and encoded attributes, twice.
    try testing.expectEqual(@as(usize, 6), try countPayloads(@TypeOf(f), &f, a));
    try testing.expectEqualStrings("test/visited", converter.encodingOf(f.cause.?.failure_info.?.application_failure_info.details.?.payloads.items[1]).?);
    try testing.expectEqualStrings("test/visited", converter.encodingOf(f.encoded_attributes.?).?);
    // The visit copied the cause and the lists before replacing their payloads.
    try testing.expect(f.cause.? != cause);
    try testing.expectEqualStrings("json/plain", converter.encodingOf(cause.failure_info.?.application_failure_info.details.?.payloads.items[1]).?);
}

test "visitPayloads reaches every payload of an activation and a completion, but no search attributes" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const act_pb = proto.coresdk.workflow_activation;
    const cmd_pb = proto.coresdk.workflow_commands;
    const common = proto.temporal.api.common.v1;

    var init: act_pb.InitializeWorkflow = .{ .workflow_type = "Visited" };
    try init.arguments.appendSlice(a, (try converter.toPayloads(a, .{ @as([]const u8, "a"), @as(i64, 1) })).items);
    try init.headers.append(a, .{ .key = "trace", .value = try converter.toPayload(a, @as([]const u8, "t")) });
    var memo: common.Memo = .{};
    try memo.fields.append(a, .{ .key = "note", .value = try converter.toPayload(a, @as([]const u8, "m")) });
    init.memo = memo;
    var sa: common.SearchAttributes = .{};
    try sa.indexed_fields.append(a, .{ .key = "CustomKeywordField", .value = try converter.toPayload(a, @as([]const u8, "k")) });
    init.search_attributes = sa;
    init.continued_failure = try failureWithDetails(a, "continued"); // 3
    var activation: act_pb.WorkflowActivation = .{ .run_id = "run" };
    try activation.jobs.append(a, .{ .variant = .{ .initialize_workflow = init } }); // 2 + 1 + 1 + 3
    var signal: act_pb.SignalWorkflow = .{ .signal_name = "go" };
    try signal.input.append(a, try converter.toPayload(a, @as(i64, 3)));
    try activation.jobs.append(a, .{ .variant = .{ .signal_workflow = signal } }); // 1
    try activation.jobs.append(a, .{ .variant = .{ .resolve_activity = .{ .seq = 1, .result = .{ .status = .{
        .completed = .{ .result = try converter.toPayload(a, @as(i64, 4)) },
    } } } } }); // 1
    try activation.jobs.append(a, .{ .variant = .{ .fire_timer = .{ .seq = 2 } } });
    try testing.expectEqual(@as(usize, 9), try countPayloads(@TypeOf(activation), &activation, a));
    const visited_init = activation.jobs.items[0].variant.?.initialize_workflow;
    try testing.expectEqualStrings("test/visited", converter.encodingOf(visited_init.memo.?.fields.items[0].value.?).?);
    try testing.expectEqualStrings("test/visited", converter.encodingOf(visited_init.headers.items[0].value.?).?);
    try testing.expectEqualStrings("json/plain", converter.encodingOf(visited_init.search_attributes.?.indexed_fields.items[0].value.?).?);

    var schedule: cmd_pb.ScheduleActivity = .{ .seq = 1, .activity_type = "Greet" };
    try schedule.arguments.appendSlice(a, (try converter.toPayloads(a, .{ @as([]const u8, "x"), @as([]const u8, "y") })).items);
    var upsert: cmd_pb.UpsertWorkflowSearchAttributes = .{};
    upsert.search_attributes = sa;
    var success: proto.coresdk.workflow_completion.Success = .{};
    try success.commands.append(a, .{
        .user_metadata = .{ .summary = try converter.toPayload(a, @as([]const u8, "greet x")) },
        .variant = .{ .schedule_activity = schedule },
    }); // 2 + 1
    try success.commands.append(a, .{ .variant = .{ .upsert_workflow_search_attributes = upsert } }); // 0
    try success.commands.append(a, .{ .variant = .{ .fail_workflow_execution = .{ .failure = try failureWithDetails(a, "failed") } } }); // 3
    try success.commands.append(a, .{ .variant = .{ .complete_workflow_execution = .{ .result = try converter.toPayload(a, @as(i64, 5)) } } }); // 1
    var completion: proto.coresdk.workflow_completion.WorkflowActivationCompletion = .{ .run_id = "run", .status = .{ .successful = success } };
    try testing.expectEqual(@as(usize, 7), try countPayloads(@TypeOf(completion), &completion, a));
    try testing.expectEqualStrings("test/visited", converter.encodingOf(completion.status.?.successful.commands.items[0].user_metadata.?.summary.?).?);
    const failed: proto.coresdk.workflow_completion.WorkflowActivationCompletion = .{ .run_id = "run", .status = .{ .failed = .{ .failure = try failureWithDetails(a, "task") } } };
    var failed_copy = failed;
    try testing.expectEqual(@as(usize, 3), try countPayloads(@TypeOf(failed_copy), &failed_copy, a));
}

test "encodeMessage and decodeMessage apply a codec chain to a message's payloads" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var marking: MarkingCodec = .{ .name = "test/marked" };
    const zlib: ZlibCodec = .{};
    const chain = [_]PayloadCodec{ marking.codec(), zlib.codec() };

    var request: proto.temporal.api.workflowservice.v1.SignalWorkflowExecutionRequest = .{ .signal_name = "s" };
    request.input = .{};
    const text = try repeat(a, "payload ", 50);
    try request.input.?.payloads.append(a, try converter.toPayload(a, text));
    const before = request.input.?.payloads.items;
    try encodeMessage(@TypeOf(request), &request, &chain, a, testing.io, &.{});
    try testing.expectEqualStrings("binary/zlib", converter.encodingOf(request.input.?.payloads.items[0]).?);
    try testing.expectEqualStrings("json/plain", converter.encodingOf(before[0]).?);
    try decodeMessage(@TypeOf(request), &request, &chain, a, testing.io, &.{});
    try testing.expectEqualStrings(text, try converter.fromPayload([]const u8, a, request.input.?.payloads.items[0]));
    try testing.expectEqual(@as(u32, 1), marking.calls.load(.monotonic));

    // No codecs: nothing is visited or copied.
    try encodeMessage(@TypeOf(request), &request, &.{}, a, testing.io, &.{});
    try testing.expectEqualStrings("json/plain", converter.encodingOf(request.input.?.payloads.items[0]).?);
}

/// Tags each payload it encodes with the propagated tenant it was given as metadata, and
/// checks the tag against the context's on decode; without a context it tags "none".
const TenantCodec = struct {
    const tenant = propagation.key([]const u8, "tenant");
    const tag = "test-tenant";

    fn codec(t: *TenantCodec) PayloadCodec {
        return .{ .ptr = t, .vtable = &.{ .encode = encode, .decode = decode, .encodeWithContext = encodeWithContext, .decodeWithContext = decodeWithContext } };
    }

    fn encode(ptr: *anyopaque, arena: Allocator, io: Io, payloads: []const Payload) CodecError![]Payload {
        return encodeWithContext(ptr, arena, io, &.{}, payloads);
    }

    fn decode(ptr: *anyopaque, arena: Allocator, io: Io, payloads: []const Payload) CodecError![]Payload {
        return decodeWithContext(ptr, arena, io, &.{}, payloads);
    }

    fn encodeWithContext(_: *anyopaque, arena: Allocator, _: Io, context: *const Context, payloads: []const Payload) CodecError![]Payload {
        const out = try arena.dupe(Payload, payloads);
        for (out) |*p| {
            var metadata = try p.metadata.clone(arena);
            try metadata.append(arena, .{ .key = tag, .value = context.values.get(tenant) orelse "none" });
            p.metadata = metadata;
        }
        return out;
    }

    fn decodeWithContext(_: *anyopaque, arena: Allocator, _: Io, context: *const Context, payloads: []const Payload) CodecError![]Payload {
        const out = try arena.dupe(Payload, payloads);
        for (out) |*p| {
            var metadata: std.ArrayList(Payload.MetadataEntry) = .empty;
            for (p.metadata.items) |m| {
                if (!std.mem.eql(u8, m.key, tag)) {
                    try metadata.append(arena, m);
                } else if (!std.mem.eql(u8, m.value, context.values.get(tenant) orelse "none")) return error.CodecFailed;
            }
            p.metadata = metadata;
        }
        return out;
    }

    fn tagOf(p: Payload) ?[]const u8 {
        for (p.metadata.items) |m| if (std.mem.eql(u8, m.key, tag)) return m.value;
        return null;
    }
};

test "a context-aware codec sees the context; a codec without one still works in the chain" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var tenant_codec: TenantCodec = .{};
    var marking: MarkingCodec = .{ .name = "test/marked" };
    const chain = [_]PayloadCodec{ tenant_codec.codec(), marking.codec() };

    var values: propagation.Values = .init(a);
    try values.put(TenantCodec.tenant, "acme");
    const context: Context = .{ .values = &values, .namespace = "ns", .workflow_id = "wf" };
    var request: proto.temporal.api.workflowservice.v1.SignalWorkflowExecutionRequest = .{ .signal_name = "s", .input = .{} };
    try request.input.?.payloads.append(a, try converter.toPayload(a, @as([]const u8, "hi")));
    try encodeMessage(@TypeOf(request), &request, &chain, a, testing.io, &context);
    try testing.expectEqualStrings("test/marked", converter.encodingOf(request.input.?.payloads.items[0]).?);
    try testing.expectEqual(@as(u32, 1), marking.calls.load(.monotonic));
    const inner = try decodePayloads(chain[1..], a, testing.io, request.input.?.payloads.items);
    try testing.expectEqualStrings("acme", TenantCodec.tagOf(inner[0]).?);

    // Decoding checks the tag against the context it is given.
    var decoded = request;
    try decodeMessage(@TypeOf(decoded), &decoded, &chain, a, testing.io, &context);
    try testing.expectEqualStrings("hi", try converter.fromPayload([]const u8, a, decoded.input.?.payloads.items[0]));
    try testing.expect(TenantCodec.tagOf(decoded.input.?.payloads.items[0]) == null);
    var other: propagation.Values = .init(a);
    try other.put(TenantCodec.tenant, "globex");
    var mismatched = request;
    try testing.expectError(error.CodecFailed, decodeMessage(@TypeOf(mismatched), &mismatched, &chain, a, testing.io, &.{ .values = &other }));

    // Without a context the codec's plain functions run.
    const plain = try encodePayloads(chain[0..1], a, testing.io, &.{try converter.toPayload(a, @as(i64, 1))});
    try testing.expectEqualStrings("none", TenantCodec.tagOf(plain[0]).?);
    const with_context = try encodePayloadsWithContext(chain[0..1], a, testing.io, &context, &.{try converter.toPayload(a, @as(i64, 1))});
    try testing.expectEqualStrings("acme", TenantCodec.tagOf(with_context[0]).?);
    try testing.expectEqual(@as(i64, 1), try converter.fromPayload(i64, a, (try decodePayloadsWithContext(chain[0..1], a, testing.io, &context, with_context))[0]));
}

/// Serializes and reverses each payload, as `MarkingCodec`, and records the context and the
/// payload count of every call. Records live in `records_arena`, not the pass's arena.
const RecordingCodec = struct {
    const name = "test/recorded";

    records_arena: Allocator,
    records: std.ArrayList(Record) = .empty,

    const Record = struct {
        encode: bool,
        values: *const propagation.Values,
        namespace: []const u8,
        workflow_id: []const u8,
        workflow_type: []const u8,
        count: usize,
    };

    fn codec(r: *RecordingCodec) PayloadCodec {
        return .{ .ptr = r, .vtable = &.{ .encode = encode, .decode = decode, .encodeWithContext = encodeWithContext, .decodeWithContext = decodeWithContext } };
    }

    fn encode(ptr: *anyopaque, arena: Allocator, io: Io, payloads: []const Payload) CodecError![]Payload {
        return encodeWithContext(ptr, arena, io, &.{}, payloads);
    }

    fn decode(ptr: *anyopaque, arena: Allocator, io: Io, payloads: []const Payload) CodecError![]Payload {
        return decodeWithContext(ptr, arena, io, &.{}, payloads);
    }

    fn record(r: *RecordingCodec, encoding: bool, context: *const Context, count: usize) CodecError!void {
        try r.records.append(r.records_arena, .{
            .encode = encoding,
            .values = context.values,
            .namespace = context.namespace,
            .workflow_id = context.workflow_id,
            .workflow_type = context.workflow_type,
            .count = count,
        });
    }

    fn encodeWithContext(ptr: *anyopaque, arena: Allocator, _: Io, context: *const Context, payloads: []const Payload) CodecError![]Payload {
        const r: *RecordingCodec = @ptrCast(@alignCast(ptr));
        try r.record(true, context, payloads.len);
        const out = try arena.alloc(Payload, payloads.len);
        for (payloads, out) |p, *o| {
            var serialized: Io.Writer.Allocating = .init(arena);
            p.encode(&serialized.writer, arena) catch return error.OutOfMemory;
            std.mem.reverse(u8, serialized.written());
            o.* = try converter.payloadWithEncoding(arena, name, serialized.written());
        }
        return out;
    }

    fn decodeWithContext(ptr: *anyopaque, arena: Allocator, _: Io, context: *const Context, payloads: []const Payload) CodecError![]Payload {
        const r: *RecordingCodec = @ptrCast(@alignCast(ptr));
        try r.record(false, context, payloads.len);
        const out = try arena.dupe(Payload, payloads);
        for (out) |*p| {
            if (!std.mem.eql(u8, converter.encodingOf(p.*) orelse "", name)) continue;
            const data = try arena.dupe(u8, p.data);
            std.mem.reverse(u8, data);
            var reader: Io.Reader = .fixed(data);
            p.* = Payload.decode(&reader, arena) catch |err| switch (err) {
                error.OutOfMemory => return error.OutOfMemory,
                else => return error.CodecFailed,
            };
        }
        return out;
    }

    /// The number of payloads coded in the direction `encoding` for `workflow_id`, failing
    /// the test if any of those calls had a context other than `want`'s.
    fn countFor(r: *const RecordingCodec, encoding: bool, want: Context) !usize {
        var n: usize = 0;
        for (r.records.items) |rec| {
            if (rec.encode != encoding or !std.mem.eql(u8, rec.workflow_id, want.workflow_id)) continue;
            try testing.expect(rec.values == want.values);
            try testing.expectEqualStrings(want.namespace, rec.namespace);
            try testing.expectEqualStrings(want.workflow_type, rec.workflow_type);
            n += rec.count;
        }
        return n;
    }
};

/// A request with 2 workflow arguments, 1 signal argument, a memo, a header, a summary and
/// details (7 payloads codecs see) and a search attribute (which they don't).
fn testSignalWithStartRequest(a: Allocator) !system.SignalWithStartRequest {
    var request: system.SignalWithStartRequest = .{
        .namespace = "target-ns",
        .workflow_id = "target",
        .workflow_type = .{ .name = "Target" },
        .signal_name = "add",
        .input = .{ .payloads = try converter.toPayloads(a, .{ @as([]const u8, "start"), @as(i64, 2) }) },
        .signal_input = .{ .payloads = try converter.toPayloads(a, .{@as([]const u8, "s1")}) },
        .memo = .{},
        .header = .{},
        .search_attributes = .{},
        .user_metadata = .{
            .summary = try converter.toPayload(a, @as([]const u8, "summary")),
            .details = try converter.toPayload(a, @as([]const u8, "details")),
        },
    };
    try request.memo.?.fields.append(a, .{ .key = "note", .value = try converter.toPayload(a, @as([]const u8, "memo")) });
    try request.header.?.fields.append(a, .{ .key = "trace", .value = try converter.toPayload(a, @as([]const u8, "header")) });
    try request.search_attributes.?.indexed_fields.append(a, .{ .key = "CustomKeywordField", .value = try converter.toPayload(a, @as([]const u8, "k")) });
    return request;
}

/// A completion scheduling an activity with 2 arguments and the system Nexus operation
/// whose input is `input`.
fn testCompletion(a: Allocator, input: Payload) !proto.coresdk.workflow_completion.WorkflowActivationCompletion {
    const cmd_pb = proto.coresdk.workflow_commands;
    var schedule: cmd_pb.ScheduleActivity = .{ .seq = 1, .activity_type = "Greet" };
    try schedule.arguments.appendSlice(a, (try converter.toPayloads(a, .{ @as([]const u8, "x"), @as([]const u8, "y") })).items);
    var success: proto.coresdk.workflow_completion.Success = .{};
    try success.commands.append(a, .{ .variant = .{ .schedule_activity = schedule } });
    try success.commands.append(a, .{ .variant = .{ .schedule_nexus_operation = .{
        .seq = 2,
        .endpoint = system.endpoint,
        .service = system.workflow_service,
        .operation = system.signal_with_start_operation,
        .input = input,
    } } });
    return .{ .run_id = "run", .status = .{ .successful = success } };
}

fn nexusInputOf(completion: proto.coresdk.workflow_completion.WorkflowActivationCompletion) Payload {
    return completion.status.?.successful.commands.items[1].variant.?.schedule_nexus_operation.input.?;
}

fn expectSameMetadata(want: Payload, got: Payload) !void {
    try testing.expectEqual(want.metadata.items.len, got.metadata.items.len);
    for (want.metadata.items, got.metadata.items) |w, g| {
        try testing.expectEqualStrings(w.key, g.key);
        try testing.expectEqualStrings(w.value, g.value);
    }
}

test "encodeMessage and decodeMessage code a system envelope's inner payloads with the target's context" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var recording: RecordingCodec = .{ .records_arena = a };
    const chain = [_]PayloadCodec{recording.codec()};

    var values: propagation.Values = .init(a);
    try values.put(TenantCodec.tenant, "acme");
    const run_context: Context = .{ .values = &values, .namespace = "caller-ns", .workflow_id = "caller", .workflow_type = "Caller" };
    const target_context: Context = .{ .values = &values, .namespace = "target-ns", .workflow_id = "target", .workflow_type = "Target" };

    const envelope = try system.envelope(a, try testSignalWithStartRequest(a));
    const envelope_data = try a.dupe(u8, envelope.data);
    var completion = try testCompletion(a, envelope);
    try encodeMessage(@TypeOf(completion), &completion, &chain, a, testing.io, &run_context);

    // Each inner payload coded once, with the target's context; the activity's arguments
    // with the run's.
    try testing.expectEqual(@as(usize, 7), try recording.countFor(true, target_context));
    try testing.expectEqual(@as(usize, 2), try recording.countFor(true, run_context));
    try testing.expectEqual(@as(usize, 7), recording.records.items.len);
    // The envelope the completion borrowed is unchanged.
    try testing.expectEqualSlices(u8, envelope_data, envelope.data);

    const sent = nexusInputOf(completion);
    try expectSameMetadata(envelope, sent);
    try testing.expect(system.isMarked(sent));
    const encoded = (try system.decode(a, sent)).signal_with_start_request;
    try testing.expectEqualStrings("add", encoded.signal_name);
    try testing.expectEqualStrings("target", encoded.workflow_id);
    for (encoded.input.?.payloads.items) |p| try testing.expectEqualStrings(RecordingCodec.name, converter.encodingOf(p).?);
    try testing.expectEqualStrings(RecordingCodec.name, converter.encodingOf(encoded.signal_input.?.payloads.items[0]).?);
    try testing.expectEqualStrings(RecordingCodec.name, converter.encodingOf(encoded.memo.?.fields.items[0].value.?).?);
    try testing.expectEqualStrings(RecordingCodec.name, converter.encodingOf(encoded.header.?.fields.items[0].value.?).?);
    try testing.expectEqualStrings(RecordingCodec.name, converter.encodingOf(encoded.user_metadata.?.summary.?).?);
    try testing.expectEqualStrings(RecordingCodec.name, converter.encodingOf(encoded.user_metadata.?.details.?).?);
    try testing.expectEqualStrings("json/plain", converter.encodingOf(encoded.search_attributes.?.indexed_fields.items[0].value.?).?);

    recording.records.clearRetainingCapacity();
    try decodeMessage(@TypeOf(completion), &completion, &chain, a, testing.io, &run_context);
    try testing.expectEqual(@as(usize, 7), try recording.countFor(false, target_context));
    try testing.expectEqual(@as(usize, 2), try recording.countFor(false, run_context));
    const received = nexusInputOf(completion);
    try expectSameMetadata(envelope, received);
    const decoded = (try system.decode(a, received)).signal_with_start_request;
    const args = try converter.fromPayloads(struct { []const u8, i64 }, a, decoded.input.?.payloads.items);
    try testing.expectEqualStrings("start", args[0]);
    try testing.expectEqual(@as(i64, 2), args[1]);
    try testing.expectEqualStrings("s1", try converter.fromPayload([]const u8, a, decoded.signal_input.?.payloads.items[0]));
    try testing.expectEqualStrings("memo", try converter.fromPayload([]const u8, a, decoded.memo.?.fields.items[0].value.?));
    try testing.expectEqualStrings("header", try converter.fromPayload([]const u8, a, decoded.header.?.fields.items[0].value.?));
    try testing.expectEqualStrings("summary", try converter.fromPayload([]const u8, a, decoded.user_metadata.?.summary.?));
    try testing.expectEqualStrings("details", try converter.fromPayload([]const u8, a, decoded.user_metadata.?.details.?));
    try testing.expectEqualStrings("k", try converter.fromPayload([]const u8, a, decoded.search_attributes.?.indexed_fields.items[0].value.?));
    // Coding there and back gives the envelope as it was built.
    try testing.expectEqualSlices(u8, envelope_data, received.data);
}

test "unmarked payloads beside a system envelope are coded together and keep their places" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var recording: RecordingCodec = .{ .records_arena = a };
    const chain = [_]PayloadCodec{recording.codec()};
    const run_context: Context = .{ .workflow_id = "caller" };

    const envelope = try system.envelope(a, system.SignalWithStartRequest{ .workflow_id = "target" });
    var request: proto.temporal.api.workflowservice.v1.SignalWorkflowExecutionRequest = .{ .signal_name = "s", .input = .{} };
    try request.input.?.payloads.append(a, try converter.toPayload(a, @as(i64, 1)));
    try request.input.?.payloads.append(a, envelope);
    try request.input.?.payloads.append(a, try converter.toPayload(a, @as(i64, 3)));
    try encodeMessage(@TypeOf(request), &request, &chain, a, testing.io, &run_context);
    // One call for both unmarked payloads; the empty envelope needs none.
    try testing.expectEqual(@as(usize, 1), recording.records.items.len);
    try testing.expectEqual(@as(usize, 2), recording.records.items[0].count);
    const items = request.input.?.payloads.items;
    try testing.expectEqualStrings(RecordingCodec.name, converter.encodingOf(items[0]).?);
    try testing.expect(system.isMarked(items[1]));
    try testing.expectEqualStrings(RecordingCodec.name, converter.encodingOf(items[2]).?);

    try decodeMessage(@TypeOf(request), &request, &chain, a, testing.io, &run_context);
    try testing.expectEqual(@as(i64, 1), try converter.fromPayload(i64, a, request.input.?.payloads.items[0]));
    try testing.expectEqual(@as(i64, 3), try converter.fromPayload(i64, a, request.input.?.payloads.items[2]));
    try testing.expectEqualStrings("target", (try system.decode(a, request.input.?.payloads.items[1])).signal_with_start_request.workflow_id);
}

test "a system envelope with no payloads inside is encoded again without calling the codecs" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var recording: RecordingCodec = .{ .records_arena = a };
    const chain = [_]PayloadCodec{recording.codec()};

    const envelope = try system.envelope(a, system.SignalWithStartRequest{ .namespace = "ns", .workflow_id = "target", .signal_name = "add" });
    var command: proto.coresdk.workflow_commands.ScheduleNexusOperation = .{ .endpoint = system.endpoint, .input = envelope };
    try encodeMessage(@TypeOf(command), &command, &chain, a, testing.io, &.{});
    try testing.expectEqual(@as(usize, 0), recording.records.items.len);
    try expectSameMetadata(envelope, command.input.?);
    try testing.expectEqualSlices(u8, envelope.data, command.input.?.data);
    try decodeMessage(@TypeOf(command), &command, &chain, a, testing.io, &.{});
    try testing.expectEqual(@as(usize, 0), recording.records.items.len);
    try testing.expectEqualStrings("add", (try system.decode(a, command.input.?)).signal_with_start_request.signal_name);
}

test "a marked payload of an unknown message type fails the pass and never reaches a codec" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var recording: RecordingCodec = .{ .records_arena = a };
    const chain = [_]PayloadCodec{recording.codec()};

    const unknown = try system.envelope(a, system.SignalWithStartResponse{ .run_id = "run" });
    for (unknown.metadata.items) |*entry| {
        if (std.mem.eql(u8, entry.key, converter.message_type_key)) entry.value = "temporal.api.workflowservice.v1.UpdateWorkflowExecutionRequest";
    }
    var not_binary = try converter.toPayload(a, @as([]const u8, "json"));
    try not_binary.metadata.append(a, .{ .key = system.mark_key, .value = system.mark_value });
    try not_binary.metadata.append(a, .{ .key = converter.message_type_key, .value = converter.protoMessageType(system.SignalWithStartRequest) });

    for ([_]Payload{ unknown, not_binary }) |p| {
        var command: proto.coresdk.workflow_commands.ScheduleNexusOperation = .{ .input = p };
        try testing.expectError(error.CodecFailed, encodeMessage(@TypeOf(command), &command, &chain, a, testing.io, &.{}));
        var activation: proto.coresdk.workflow_activation.ResolveNexusOperation = .{ .seq = 1, .result = .{ .status = .{ .completed = p } } };
        try testing.expectError(error.CodecFailed, decodeMessage(@TypeOf(activation), &activation, &chain, a, testing.io, &.{}));
    }
    try testing.expectEqual(@as(usize, 0), recording.records.items.len);
}

test "decodeMessage leaves a system response in an activation's Nexus resolution readable and unchanged" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var recording: RecordingCodec = .{ .records_arena = a };
    const chain = [_]PayloadCodec{recording.codec()};

    const response = try system.envelope(a, system.SignalWithStartResponse{ .run_id = "target-run", .started = true });
    var activation: proto.coresdk.workflow_activation.WorkflowActivation = .{ .run_id = "run" };
    try activation.jobs.append(a, .{ .variant = .{ .resolve_nexus_operation = .{ .seq = 2, .result = .{ .status = .{ .completed = response } } } } });
    try decodeMessage(@TypeOf(activation), &activation, &chain, a, testing.io, &.{ .workflow_id = "caller" });
    try testing.expectEqual(@as(usize, 0), recording.records.items.len);
    const got = activation.jobs.items[0].variant.?.resolve_nexus_operation.result.?.status.?.completed;
    try expectSameMetadata(response, got);
    try testing.expectEqualSlices(u8, response.data, got.data);
    const decoded = try system.decodeSignalWithStartResponse(a, got);
    try testing.expectEqualStrings("target-run", decoded.run_id);
    try testing.expect(decoded.started);
}

test "an allocation failure while coding a system envelope is error.OutOfMemory" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var recording: RecordingCodec = .{ .records_arena = a };
    const chain = [_]PayloadCodec{recording.codec()};
    const run_context: Context = .{ .workflow_id = "caller" };

    const plain = try testCompletion(a, try system.envelope(a, try testSignalWithStartRequest(a)));
    var encoded = plain;
    try encodeMessage(@TypeOf(encoded), &encoded, &chain, a, testing.io, &run_context);

    for ([_]bool{ true, false }) |encoding| {
        var fail_index: usize = 0;
        while (true) : (fail_index += 1) {
            var pass: std.heap.ArenaAllocator = .init(testing.allocator);
            defer pass.deinit();
            var failing: std.testing.FailingAllocator = .init(pass.allocator(), .{ .fail_index = fail_index });
            var completion = if (encoding) plain else encoded;
            const result = if (encoding)
                encodeMessage(@TypeOf(completion), &completion, &chain, failing.allocator(), testing.io, &run_context)
            else
                decodeMessage(@TypeOf(completion), &completion, &chain, failing.allocator(), testing.io, &run_context);
            if (!failing.has_induced_failure) {
                try result;
                break;
            }
            try testing.expectError(error.OutOfMemory, result);
        }
        // The descent into the envelope allocates, so some of those failures were inside it.
        try testing.expect(fail_index > 10);
    }
}

/// Runs `encodeMessage` over a ScheduleNexusOperation whose input is `nested` on a thread
/// with a 4 MiB stack, a quarter of a worker thread's, and stores its result.
const NestedEnvelopePass = struct {
    nested: Payload,
    result: CodecError!void = {},

    fn run(n: *NestedEnvelopePass) void {
        var arena: std.heap.ArenaAllocator = .init(testing.allocator);
        defer arena.deinit();
        var recording: RecordingCodec = .{ .records_arena = arena.allocator() };
        const chain = [_]PayloadCodec{recording.codec()};
        var command: proto.coresdk.workflow_commands.ScheduleNexusOperation = .{ .input = n.nested };
        n.result = encodeMessage(@TypeOf(command), &command, &chain, arena.allocator(), testing.io, &.{});
    }
};

test "a system envelope nested thousands deep fails the pass instead of overflowing the stack" {
    // The payload's bytes are opaque to sdk-core, which bounds the depth of the messages it
    // decodes, so whoever writes a payload (a signal's sender, a Nexus handler) chooses how
    // deep envelopes nest inside it. This one is 2,000 deep and about 300 KB.
    const depth = 2000;
    var data: []u8 = try testing.allocator.dupe(u8, "leaf");
    defer testing.allocator.free(data);
    var outer: std.heap.ArenaAllocator = .init(testing.allocator);
    defer outer.deinit();
    var nested = try converter.payloadWithEncoding(outer.allocator(), "binary/plain", data);
    for (0..depth) |_| {
        var level: std.heap.ArenaAllocator = .init(testing.allocator);
        defer level.deinit();
        const a = level.allocator();
        var request: system.SignalWithStartRequest = .{ .workflow_id = "w", .input = .{} };
        try request.input.?.payloads.append(a, nested);
        const wrapped = try system.envelope(a, request);
        const bytes = try testing.allocator.dupe(u8, wrapped.data);
        testing.allocator.free(data);
        data = bytes;
        _ = outer.reset(.retain_capacity);
        nested = try system.envelope(outer.allocator(), system.SignalWithStartRequest{});
        nested.data = data;
    }

    var pass: NestedEnvelopePass = .{ .nested = nested };
    const thread = try std.Thread.spawn(.{ .stack_size = 4 * 1024 * 1024 }, NestedEnvelopePass.run, .{&pass});
    thread.join();
    pass.result catch |err| try testing.expectEqual(error.CodecFailed, err);
}

test "system envelopes nested up to the limit are coded; one more level fails the pass" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var recording: RecordingCodec = .{ .records_arena = a };
    const chain = [_]PayloadCodec{recording.codec()};

    var nested = try converter.toPayload(a, @as([]const u8, "leaf"));
    for (0..ChainVisitor.max_system_payload_depth + 1) |level| {
        var command: proto.coresdk.workflow_commands.ScheduleNexusOperation = .{ .input = nested };
        if (level <= ChainVisitor.max_system_payload_depth) {
            try encodeMessage(@TypeOf(command), &command, &chain, a, testing.io, &.{});
            try decodeMessage(@TypeOf(command), &command, &chain, a, testing.io, &.{});
        } else {
            try testing.expectError(error.CodecFailed, encodeMessage(@TypeOf(command), &command, &chain, a, testing.io, &.{}));
            try testing.expectError(error.CodecFailed, decodeMessage(@TypeOf(command), &command, &chain, a, testing.io, &.{}));
        }
        var request: system.SignalWithStartRequest = .{ .workflow_id = "w", .input = .{} };
        try request.input.?.payloads.append(a, nested);
        nested = try system.envelope(a, request);
    }
    // The leaf was coded once in each direction for each level that passed.
    var leaf_calls: usize = 0;
    for (recording.records.items) |rec| leaf_calls += rec.count;
    try testing.expectEqual(2 * (ChainVisitor.max_system_payload_depth + 1), leaf_calls);
}
