//! Payload conversion (Go `converter` package parity).
//!
//! Values cross the wire as `temporal.api.common.v1.Payload`: bytes plus an `encoding`
//! metadata entry. The default converter matches the Go SDK's choices for the same values, so
//! a Zig worker and a Go client (or the other way round) understand each other:
//!
//! | Zig value                         | encoding      | data                    |
//! |-----------------------------------|---------------|-------------------------|
//! | `void`, `null`                    | `binary/null` | empty                   |
//! | `converter.Bytes`                 | `binary/plain`| the bytes               |
//! | anything else (strings included)  | `json/plain`  | JSON from `std.json`    |
//!
//! Strings are JSON, as Go encodes `string`; use `Bytes` for raw binary data. Decoding
//! accepts any of the three encodings into a compatible type. Decoded slices and strings are
//! allocated from the arena passed in.

const std = @import("std");
const proto = @import("proto.zig");
const Allocator = std.mem.Allocator;

pub const Payload = proto.temporal.api.common.v1.Payload;
pub const Payloads = proto.temporal.api.common.v1.Payloads;

/// Raw bytes, encoded as `binary/plain` instead of a JSON string.
pub const Bytes = struct { data: []const u8 };

pub const encoding_key = "encoding";

pub const Encoding = enum {
    null,
    binary,
    json,

    pub fn text(e: Encoding) []const u8 {
        return switch (e) {
            .null => "binary/null",
            .binary => "binary/plain",
            .json => "json/plain",
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

/// Encodes one value. Memory comes from `arena`.
pub fn toPayload(arena: Allocator, value: anytype) error{OutOfMemory}!Payload {
    const T = @TypeOf(value);
    if (T == void or T == @TypeOf(null)) return payloadWith(arena, .null, "");
    if (@typeInfo(T) == .optional) {
        if (value) |v| return toPayload(arena, v);
        return payloadWith(arena, .null, "");
    }
    if (T == Bytes) return payloadWith(arena, .binary, value.data);
    const json = try std.json.Stringify.valueAlloc(arena, value, .{});
    return payloadWith(arena, .json, json);
}

/// Decodes one value of type `T`. Memory comes from `arena`.
pub fn fromPayload(comptime T: type, arena: Allocator, p: Payload) DecodeError!T {
    const enc = encodingOf(p) orelse return error.UnknownEncoding;
    if (std.mem.eql(u8, enc, Encoding.null.text())) {
        if (T == void) return {};
        if (@typeInfo(T) == .optional) return null;
        return error.InvalidPayload;
    }
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

/// Encodes each element of the tuple `args` (workflow and activity arguments).
pub fn toPayloads(arena: Allocator, args: anytype) error{OutOfMemory}!std.ArrayList(Payload) {
    var out: std.ArrayList(Payload) = .empty;
    inline for (args) |arg| try out.append(arena, try toPayload(arena, arg));
    return out;
}

/// Decodes payloads into a tuple of type `Args`. Missing trailing optional arguments decode
/// as null; extra payloads are ignored, as in the Go SDK.
pub fn fromPayloads(comptime Args: type, arena: Allocator, payloads: []const Payload) DecodeError!Args {
    var out: Args = undefined;
    inline for (@typeInfo(Args).@"struct".field_types, 0..) |F, i| {
        if (i < payloads.len) {
            out[i] = try fromPayload(F, arena, payloads[i]);
        } else if (@typeInfo(F) == .optional) {
            out[i] = null;
        } else {
            return error.WrongArgumentCount;
        }
    }
    return out;
}

fn payloadWith(arena: Allocator, enc: Encoding, data: []const u8) error{OutOfMemory}!Payload {
    var p: Payload = .{ .data = data };
    try p.metadata.append(arena, .{ .key = encoding_key, .value = enc.text() });
    return p;
}

// ---------------------------------------------------------------- tests

const testing = std.testing;

test "strings, structs and numbers encode as JSON like the Go SDK" {
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
