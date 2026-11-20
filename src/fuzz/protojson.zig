//! Draws, in order:
//! - an enum `Message` (8 bytes: 0 is a history event, 1 a Payloads message);
//! - one slice into an 8192-byte buffer, the ProtoJSON text.
//!
//! Text that decodes must encode to text that decodes again and encodes to the same bytes.

const std = @import("std");
const fuzz = @import("../fuzz.zig");
const protojson = @import("../protojson.zig");
const proto = @import("../proto.zig");

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
    const encoded = try protojson.encode(arena, first);
    const second = try protojson.decode(T, arena, encoded);
    try std.testing.expectEqualStrings(encoded, try protojson.encode(arena, second));
}
