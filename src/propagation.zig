//! Context propagation: values that travel with a workflow's calls in their headers, such as a
//! tenant, a user, or a trace ID.
//!
//! Values live in a `Values` store, under typed keys:
//!
//!     pub const tenant = temporal.propagation.key([]const u8, "tenant");
//!
//!     var values: temporal.propagation.Values = .init(arena);
//!     try values.put(tenant, "acme");
//!     var run = try client.executeWorkflow(Order, .{ .task_queue = "orders", .context = &values }, .{id});
//!
//! A `ContextPropagator` turns values into header fields when a call is sent and back when it
//! is received. Propagators are set with `client.Options.propagators`; workers on the client
//! use them too. Inside a workflow, `ctx.propagated()` holds the values extracted from the
//! start header, and they are injected again into every activity, local activity, child
//! workflow, external signal, and continue-as-new the workflow sends. Inside an activity,
//! `ctx.propagated()` holds the values extracted from its header:
//!
//!     const who = ctx.propagated().get(tenant) orelse "nobody";
//!
//! `HeaderPropagator(tenant).propagator` carries one key as a header field of the same name.
//!
//! Header fields skip the client's payload codecs (`client.Options.codecs`) unless the
//! propagator that writes them sets `ContextPropagator.encode_fields`:
//! `HeaderPropagator(tenant).encoded_propagator` is the same propagator with its field
//! encoded. Fields an interceptor or other code writes into a header are sent as written. On
//! receipt every header field goes through the codecs' decode before any propagator's
//! `extract` sees it, so a field the codecs encoded arrives decoded, and one they didn't
//! encode arrives unchanged as long as every codec passes through payloads it didn't encode
//! (the `converter.PayloadCodec` rule).
//!
//! Nexus operations carry the values too. When a workflow calls an operation, the propagators
//! write their fields into one Nexus header, `nexus_header_name`, whose value is the fields
//! as a protobuf `temporal.api.common.v1.Header`, base64url-encoded. The caller's own
//! `NexusOperationOptions.headers` are sent in their order, without any header of that name
//! (ignoring case), followed by the propagated one; with nothing to propagate the caller's
//! headers are sent exactly as given. On the handling worker the client's propagators read
//! the header back: `ctx.propagated()` in the handler holds the values, and a workflow a
//! workflow-run operation starts carries them on. Like every Nexus header, the value skips
//! the payload codecs, is recorded in the caller's history, and counts toward the server's
//! limit on a call's Nexus headers, so don't propagate secrets or large values over Nexus.

const std = @import("std");
const converter = @import("converter.zig");
const proto = @import("proto.zig");
const Allocator = std.mem.Allocator;

/// A key of a `Values` store: a name and the type of the value stored under it. Make one with
/// `key`.
pub fn Key(comptime T: type) type {
    return struct {
        name: []const u8,

        pub const Value = T;
    };
}

/// The key `name` for values of type `T`: `propagation.key([]const u8, "tenant")`.
pub fn key(comptime T: type, comptime name: []const u8) Key(T) {
    return .{ .name = name };
}

/// A small typed key-value store for propagated values, passed explicitly: to client calls as
/// `.context = &values`, and read in workflows and activities with `ctx.propagated()`.
/// Values are copied into `arena` by `put`; memory they point to (a string's bytes) is
/// borrowed and must outlive the store. Not thread-safe.
pub const Values = struct {
    arena: Allocator,
    entries: std.ArrayList(Entry) = .empty,

    const Entry = struct {
        name: []const u8,
        type_name: []const u8,
        value: *const anyopaque,
    };

    /// A store with no values, for contexts that have none. `put` fails on it.
    pub const empty: Values = .{ .arena = .failing };

    pub fn init(arena: Allocator) Values {
        return .{ .arena = arena };
    }

    /// Stores `value` under `k`, replacing the value of any key with the same name.
    pub fn put(v: *Values, k: anytype, value: @TypeOf(k).Value) error{OutOfMemory}!void {
        const T = @TypeOf(k).Value;
        const stored = try v.arena.create(T);
        stored.* = value;
        const entry: Entry = .{ .name = k.name, .type_name = @typeName(T), .value = @ptrCast(stored) };
        for (v.entries.items) |*e| if (std.mem.eql(u8, e.name, k.name)) {
            e.* = entry;
            return;
        };
        try v.entries.append(v.arena, entry);
    }

    /// The value stored under `k`, or null if there is none or it was stored with a
    /// different type.
    pub fn get(v: *const Values, k: anytype) ?@TypeOf(k).Value {
        const T = @TypeOf(k).Value;
        for (v.entries.items) |e| if (std.mem.eql(u8, e.name, k.name)) {
            if (!std.mem.eql(u8, e.type_name, @typeName(T))) return null;
            const p: *const T = @ptrCast(@alignCast(e.value));
            return p.*;
        };
        return null;
    }

    /// Removes the value stored under the name `name`, if any.
    pub fn remove(v: *Values, name: []const u8) void {
        for (v.entries.items, 0..) |e, i| if (std.mem.eql(u8, e.name, name)) {
            _ = v.entries.orderedRemove(i);
            return;
        };
    }

    pub fn count(v: *const Values) usize {
        return v.entries.items.len;
    }
};

/// Moves values between a `Values` store and a call's header, plugged in at run time: `ptr`
/// is the propagator's state and `vtable` its functions.
///
/// `inject` writes header fields for `values` when a call is sent; `extract` reads them back
/// into `values` when a call is received. Memory for encoded or decoded values comes from
/// `arena`, which outlives the call. Both run on client, worker, and activity threads, so
/// they must be thread-safe; inside a workflow they run as part of the workflow and must be
/// deterministic: no I/O, clocks, or randomness, and the same fields for the same values.
/// A header that doesn't extract is logged and skipped.
pub const ContextPropagator = struct {
    ptr: ?*anyopaque,
    vtable: *const VTable,
    /// Whether the header fields `inject` writes go through the client's payload codecs
    /// (`client.Options.codecs`) when the call is sent. False, the default, sends them as written.
    encode_fields: bool = false,

    pub const VTable = struct {
        inject: *const fn (ptr: ?*anyopaque, arena: Allocator, values: *const Values, header: *converter.HeaderWriter) error{OutOfMemory}!void,
        extract: *const fn (ptr: ?*anyopaque, arena: Allocator, header: converter.Header, values: *Values) converter.DecodeError!void,
    };

    pub fn inject(p: ContextPropagator, arena: Allocator, values: *const Values, header: *converter.HeaderWriter) error{OutOfMemory}!void {
        return p.vtable.inject(p.ptr, arena, values, header);
    }

    pub fn extract(p: ContextPropagator, arena: Allocator, header: converter.Header, values: *Values) converter.DecodeError!void {
        return p.vtable.extract(p.ptr, arena, header, values);
    }
};

/// A propagator that carries the value of `k` (a `key`) as the header field `k.name`, encoded
/// with the default converter: `HeaderPropagator(tenant).propagator`, whose field is sent as
/// written, or `HeaderPropagator(tenant).encoded_propagator`, whose field the client's payload
/// codecs encode.
pub fn HeaderPropagator(comptime k: anytype) type {
    const T = @TypeOf(k).Value;
    return struct {
        pub const propagator: ContextPropagator = .{ .ptr = null, .vtable = &vtable };
        /// `propagator` with `encode_fields` set: the payload codecs encode its field.
        pub const encoded_propagator: ContextPropagator = .{ .ptr = null, .vtable = &vtable, .encode_fields = true };

        const vtable: ContextPropagator.VTable = .{ .inject = inject, .extract = extract };

        fn inject(_: ?*anyopaque, arena: Allocator, values: *const Values, header: *converter.HeaderWriter) error{OutOfMemory}!void {
            const v = values.get(k) orelse return;
            try header.put(k.name, try converter.toPayload(arena, v));
        }

        fn extract(_: ?*anyopaque, arena: Allocator, header: converter.Header, values: *Values) converter.DecodeError!void {
            const v = (try header.value(T, arena, k.name)) orelse return;
            try values.put(k, v);
        }
    };
}

/// Runs every propagator's `inject`, in order, with `header.encode_fields` set to the
/// propagator's `encode_fields`, so the fields each writes are marked for the codecs as it
/// asks. `header.encode_fields` is put back as it was on return, also on error.
pub fn injectAll(propagators: []const ContextPropagator, arena: Allocator, values: *const Values, header: *converter.HeaderWriter) error{OutOfMemory}!void {
    const mode = header.encode_fields;
    defer header.encode_fields = mode;
    for (propagators) |p| {
        header.encode_fields = p.encode_fields;
        try p.inject(arena, values, header);
    }
}

/// Runs every propagator's `extract`, in order. A propagator whose fields don't decode is
/// logged with `what` (the call being received) and skipped; only running out of memory
/// stops extraction.
pub fn extractAll(propagators: []const ContextPropagator, arena: Allocator, header: converter.Header, values: *Values, comptime what: []const u8) error{OutOfMemory}!void {
    for (propagators, 0..) |p, i| p.extract(arena, header, values) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => std.log.scoped(.temporal).warn("extracting propagated values from the header of " ++ what ++ " failed: propagator {d} returned {t}; its values are skipped", .{ i, err }),
    };
}

/// The Nexus header that carries propagated values across a Nexus call between workers of
/// this SDK.
pub const nexus_header_name = "temporal-zig-context";

const ProtoHeader = proto.temporal.api.common.v1.Header;
const nexus_encoding = std.base64.url_safe_no_pad;

/// Runs every propagator's `inject` and returns the value of the `nexus_header_name` header
/// that carries the fields, or null when the propagators wrote none. The fields are encoded
/// in name order, so the same values give the same bytes. Like every Nexus header, the value
/// skips the payload codecs, whatever the propagators' `encode_fields`. Memory comes from
/// `arena`.
pub fn injectNexusHeader(propagators: []const ContextPropagator, arena: Allocator, values: *const Values) error{OutOfMemory}!?[]const u8 {
    if (propagators.len == 0) return null;
    var w: converter.HeaderWriter = .init(arena);
    try injectAll(propagators, arena, values, &w);
    if (w.fields.items.len == 0) return null;
    const message: ProtoHeader = .{ .fields = try w.header().toEntries(ProtoHeader.FieldsEntry, arena) };
    var bytes: std.Io.Writer.Allocating = .init(arena);
    message.encode(&bytes.writer, arena) catch return error.OutOfMemory;
    const encoded = try arena.alloc(u8, nexus_encoding.Encoder.calcSize(bytes.written().len));
    return nexus_encoding.Encoder.encode(encoded, bytes.written());
}

/// Decodes `value`, written by `injectNexusHeader`, and runs every propagator's `extract` on
/// it into `values`. A value that isn't base64url or a header message is logged and skipped,
/// as is a propagator whose fields don't decode; only running out of memory is an error.
pub fn extractNexusHeader(propagators: []const ContextPropagator, arena: Allocator, value: []const u8, values: *Values) error{OutOfMemory}!void {
    if (propagators.len == 0) return;
    const log = std.log.scoped(.temporal);
    const size = nexus_encoding.Decoder.calcSizeForSlice(value) catch {
        log.warn("the Nexus header " ++ nexus_header_name ++ " is not base64url; its propagated values are skipped", .{});
        return;
    };
    const bytes = try arena.alloc(u8, size);
    nexus_encoding.Decoder.decode(bytes, value) catch {
        log.warn("the Nexus header " ++ nexus_header_name ++ " is not base64url; its propagated values are skipped", .{});
        return;
    };
    var reader: std.Io.Reader = .fixed(bytes);
    const message = ProtoHeader.decode(&reader, arena) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => {
            log.warn("the Nexus header " ++ nexus_header_name ++ " doesn't decode as a header message ({t}); its propagated values are skipped", .{err});
            return;
        },
    };
    const header = try converter.Header.fromEntries(arena, message.fields.items);
    try extractAll(propagators, arena, header, values, "a Nexus request");
}

// ---------------------------------------------------------------- tests

const testing = std.testing;

test "values are stored and read back by typed keys" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const tenant = key([]const u8, "tenant");
    const attempt = key(u32, "attempt");
    const wrong = key(u64, "attempt");

    var values: Values = .init(arena.allocator());
    try testing.expectEqual(@as(?[]const u8, null), values.get(tenant));
    try values.put(tenant, "acme");
    try values.put(attempt, 3);
    try values.put(attempt, 4);
    try testing.expectEqualStrings("acme", values.get(tenant).?);
    try testing.expectEqual(@as(?u32, 4), values.get(attempt));
    try testing.expectEqual(@as(?u64, null), values.get(wrong));
    try testing.expectEqual(@as(usize, 2), values.count());
    values.remove("tenant");
    try testing.expectEqual(@as(?[]const u8, null), values.get(tenant));
    try testing.expectError(error.OutOfMemory, @constCast(&Values.empty).put(tenant, "x"));
}

test "a header propagator carries a key through a header" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const tenant = comptime key([]const u8, "tenant");
    const propagators = [_]ContextPropagator{HeaderPropagator(tenant).propagator};

    var sent: Values = .init(a);
    try sent.put(tenant, "acme");
    var w: converter.HeaderWriter = .init(a);
    try injectAll(&propagators, a, &sent, &w);
    try testing.expectEqualStrings("\"acme\"", w.header().get("tenant").?.data);

    var received: Values = .init(a);
    try extractAll(&propagators, a, w.header(), &received, "a test call");
    try testing.expectEqualStrings("acme", received.get(tenant).?);

    // Nothing to inject, nothing written; a field that doesn't decode is skipped.
    var none: converter.HeaderWriter = .init(a);
    try injectAll(&propagators, a, &Values.empty, &none);
    try testing.expectEqual(@as(usize, 0), none.header().fields.len);
    var bad: converter.HeaderWriter = .init(a);
    try bad.set("tenant", @as(u32, 7));
    var skipped: Values = .init(a);
    try extractAll(&propagators, a, bad.header(), &skipped, "a test call");
    try testing.expectEqual(@as(usize, 0), skipped.count());
}

test "values travel through the Nexus header and an undecodable one is skipped" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const tenant = comptime key([]const u8, "tenant");
    const user = comptime key([]const u8, "user");
    const propagators = [_]ContextPropagator{ HeaderPropagator(user).propagator, HeaderPropagator(tenant).propagator };

    var sent: Values = .init(a);
    try sent.put(user, "ann");
    try sent.put(tenant, "acme");
    const value = (try injectNexusHeader(&propagators, a, &sent)).?;
    for (value) |c| try testing.expect(std.ascii.isAlphanumeric(c) or c == '-' or c == '_');
    // The same values in another order give the same bytes.
    var reordered: Values = .init(a);
    try reordered.put(tenant, "acme");
    try reordered.put(user, "ann");
    try testing.expectEqualStrings(value, (try injectNexusHeader(&propagators, a, &reordered)).?);

    var received: Values = .init(a);
    try extractNexusHeader(&propagators, a, value, &received);
    try testing.expectEqualStrings("acme", received.get(tenant).?);
    try testing.expectEqualStrings("ann", received.get(user).?);

    // Nothing to propagate, no header.
    try testing.expectEqual(@as(?[]const u8, null), try injectNexusHeader(&propagators, a, &Values.empty));
    try testing.expectEqual(@as(?[]const u8, null), try injectNexusHeader(&.{}, a, &sent));

    // Not base64url, or not a header message (a field cut short): skipped, nothing extracted.
    const level = testing.log_level;
    defer testing.log_level = level;
    testing.log_level = .err;
    for ([_][]const u8{ "not base64!", "_w", "CgU" }) |bad| {
        var skipped: Values = .init(a);
        try extractNexusHeader(&propagators, a, bad, &skipped);
        try testing.expectEqual(@as(usize, 0), skipped.count());
    }
}

test "injectAll marks the fields of propagators that set encode_fields, and puts the writer's mode back" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const tenant = comptime key([]const u8, "tenant");
    const user = comptime key([]const u8, "user");
    const propagators = [_]ContextPropagator{ HeaderPropagator(tenant).encoded_propagator, HeaderPropagator(user).propagator };
    try testing.expect(HeaderPropagator(tenant).encoded_propagator.encode_fields);
    try testing.expect(!HeaderPropagator(tenant).propagator.encode_fields);

    var sent: Values = .init(a);
    try sent.put(tenant, "acme");
    try sent.put(user, "ann");
    var w: converter.HeaderWriter = .init(a);
    try injectAll(&propagators, a, &sent, &w);
    try testing.expect(w.encodes("tenant"));
    try testing.expect(!w.encodes("user"));
    try testing.expect(!w.encode_fields);
    // Both fields read back the same, marked or not.
    var received: Values = .init(a);
    try extractAll(&propagators, a, w.header(), &received, "a test call");
    try testing.expectEqualStrings("acme", received.get(tenant).?);
    try testing.expectEqualStrings("ann", received.get(user).?);

    // A writer already in encoding mode gets it back, also when a propagator fails.
    w.encode_fields = true;
    try injectAll(propagators[1..], a, &sent, &w);
    try testing.expect(w.encode_fields);
    try testing.expect(!w.encodes("user"));
    const failing: ContextPropagator = .{ .ptr = null, .encode_fields = false, .vtable = &.{
        .inject = struct {
            fn inject(_: ?*anyopaque, _: Allocator, _: *const Values, _: *converter.HeaderWriter) error{OutOfMemory}!void {
                return error.OutOfMemory;
            }
        }.inject,
        .extract = HeaderPropagator(user).propagator.vtable.extract,
    } };
    try testing.expectError(error.OutOfMemory, injectAll(&.{failing}, a, &sent, &w));
    try testing.expect(w.encode_fields);

    // The Nexus header carries a marked field as written: it skips the codecs.
    const value = (try injectNexusHeader(&propagators, a, &sent)).?;
    try testing.expectEqualStrings(value, (try injectNexusHeader(&.{ HeaderPropagator(tenant).propagator, HeaderPropagator(user).propagator }, a, &sent)).?);
}
