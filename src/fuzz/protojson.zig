//! Draws, in order:
//! - an enum `Message` (8 bytes: 0 is a history event, 1 a Payloads message);
//! - one slice into an 8192-byte buffer, the ProtoJSON text.
//!
//! Text that decodes must encode to text that decodes again and encodes to the same bytes.

const std = @import("std");
const fuzz = @import("../fuzz.zig");
const protojson = @import("../protojson.zig");
const proto = @import("../proto.zig");
const converter = @import("../converter.zig");

const Message = enum { history_event, payloads };

pub fn testOne(_: void, smith: *std.testing.Smith) anyerror!void {
    const message = smith.value(Message);
    var buffer: [fuzz.max_input_len]u8 = undefined;
    const json = buffer[0..smith.slice(&buffer)];
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    switch (message) {
        .history_event => try roundTrip(proto.temporal.api.history.v1.HistoryEvent, arena.allocator(), json),
        .payloads => try roundTrip(proto.temporal.api.common.v1.Payloads, arena.allocator(), json),
    }
}

fn roundTrip(comptime T: type, arena: std.mem.Allocator, json: []const u8) !void {
    const result: protojson.DecodeError!T = protojson.decode(T, arena, json);
    const first = result catch |err| switch (err) {
        error.OutOfMemory, error.InvalidProtoJson => return,
    };
    if (hasLeapDayTimestamp(first)) return fuzz.known("protojson", "temporal-sdk-zig-ljo");
    const encoded = try protojson.encode(arena, first);
    const second = try protojson.decode(T, arena, encoded);
    try std.testing.expectEqualStrings(encoded, try protojson.encode(arena, second));
}

/// Whether `value` holds a Timestamp on 29 February of a year not divisible by 400, on which
/// zig-protobuf's `Timestamp.jsonStringify` overflows in `civil_from_days`.
fn hasLeapDayTimestamp(value: anytype) bool {
    const T = @TypeOf(value);
    if (T == proto.google.protobuf.Timestamp) {
        var text: [converter.Time.max_len]u8 = undefined;
        const date = (converter.Time{ .unix_ns = @as(i96, value.seconds) * std.time.ns_per_s }).format(&text);
        const year = std.fmt.parseUnsigned(u32, date[0..4], 10) catch unreachable;
        return std.mem.eql(u8, date[4..10], "-02-29") and year % 400 != 0;
    }
    switch (@typeInfo(T)) {
        .optional => return if (value) |child| hasLeapDayTimestamp(child) else false,
        .pointer => |pointer| switch (pointer.size) {
            .one => return hasLeapDayTimestamp(value.*),
            .slice => {
                if (pointer.child == u8) return false;
                for (value) |item| if (hasLeapDayTimestamp(item)) return true;
                return false;
            },
            else => return false,
        },
        .@"struct" => |info| {
            inline for (info.field_names) |name| if (hasLeapDayTimestamp(@field(value, name))) return true;
            return false;
        },
        .@"union" => switch (value) {
            inline else => |child| return hasLeapDayTimestamp(child),
        },
        else => return false,
    }
}
