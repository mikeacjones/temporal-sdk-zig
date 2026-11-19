//! Draws, in order:
//! - up to 4 metadata entries, each an `eos` (1 byte; true ends the list) followed by a
//!   key slice into a 32-byte buffer and a value slice into a 32-byte buffer;
//! - after the fourth entry no `eos` is drawn;
//! - one slice into a 4096-byte buffer, the payload's data.
//!
//! The payload is decoded as an `Order`. A value that decodes must encode with
//! `converter.toPayload` and decode again to an equal value.

const std = @import("std");
const converter = @import("../converter.zig");

const Color = enum { red, green, blue };

const Item = struct {
    name: []const u8,
    tags: []const []const u8 = &.{},
    count: ?u32 = null,
    color: Color = .red,
};

const Order = struct {
    id: u64,
    items: []const Item,
    note: ?[]const u8 = null,
    color: ?Color = null,
    placed: converter.Time,
    shipped: ?converter.Time = null,
    adjustments: []const i32 = &.{},
};

const max_metadata = 4;

pub fn testOne(_: void, smith: *std.testing.Smith) anyerror!void {
    var keys: [max_metadata][32]u8 = undefined;
    var values: [max_metadata][32]u8 = undefined;
    var entries: [max_metadata]converter.Payload.MetadataEntry = undefined;
    var count: usize = 0;
    while (count < max_metadata and !smith.eos()) : (count += 1) {
        entries[count] = .{
            .key = keys[count][0..smith.slice(&keys[count])],
            .value = values[count][0..smith.slice(&values[count])],
        };
    }
    var data: [4096]u8 = undefined;
    const payload: converter.Payload = .{
        .metadata = .fromOwnedSlice(entries[0..count]),
        .data = data[0..smith.slice(&data)],
    };

    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const result: converter.DecodeError!Order = converter.fromPayload(Order, a, payload);
    const first = result catch |err| switch (err) {
        error.OutOfMemory, error.UnknownEncoding, error.InvalidPayload, error.WrongArgumentCount => return,
    };
    const second = try converter.fromPayload(Order, a, try converter.toPayload(a, first));
    try std.testing.expectEqualDeep(first, second);
}
