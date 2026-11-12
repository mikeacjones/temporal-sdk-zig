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

const std = @import("std");
const converter = @import("converter.zig");
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
/// with the default converter: `HeaderPropagator(tenant).propagator`.
pub fn HeaderPropagator(comptime k: anytype) type {
    const T = @TypeOf(k).Value;
    return struct {
        pub const propagator: ContextPropagator = .{ .ptr = null, .vtable = &.{ .inject = inject, .extract = extract } };

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

/// Runs every propagator's `inject`, in order.
pub fn injectAll(propagators: []const ContextPropagator, arena: Allocator, values: *const Values, header: *converter.HeaderWriter) error{OutOfMemory}!void {
    for (propagators) |p| try p.inject(arena, values, header);
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
