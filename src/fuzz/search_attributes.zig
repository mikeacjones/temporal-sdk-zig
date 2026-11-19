//! Draws, in order, up to 6 indexed fields, each:
//! - an `eos` (1 byte; true ends the list); after the sixth field no `eos` is drawn;
//! - a slice into a 16-byte buffer, the name;
//! - an `eos` (1 byte; true means the entry has no payload) and, when false:
//!   - up to 2 metadata entries, each an `eos` (1 byte; true ends the list) followed by a
//!     key slice into a 16-byte buffer and a value slice into a 32-byte buffer; after the
//!     second entry no `eos` is drawn;
//!   - a slice into a 256-byte buffer, the payload's data.
//!
//! The view `View.fromProto` makes holds each name once, in order. Every name in it reads
//! with `View.get` as each key type, giving a value or an error of `DecodeError`. A name not
//! in it reads as null. `View.fromProto` of `View.toProto` gives an equal view.

const std = @import("std");
const sa = @import("../search_attributes.zig");
const converter = @import("../converter.zig");
const proto = @import("../proto.zig");
const Payload = converter.Payload;
const SearchAttributes = proto.temporal.api.common.v1.SearchAttributes;

const max_fields = 6;
const max_metadata = 2;

const Buffers = struct {
    name: [16]u8,
    keys: [max_metadata][16]u8,
    values: [max_metadata][32]u8,
    entries: [max_metadata]Payload.MetadataEntry,
    data: [256]u8,
};

pub fn testOne(_: void, smith: *std.testing.Smith) anyerror!void {
    var buffers: [max_fields]Buffers = undefined;
    var fields: [max_fields]SearchAttributes.IndexedFieldsEntry = undefined;
    var count: usize = 0;
    while (count < max_fields and !smith.eos()) : (count += 1) fields[count] = draw(smith, &buffers[count]);
    const attributes: SearchAttributes = .{ .indexed_fields = .fromOwnedSlice(fields[0..count]) };

    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const view = sa.View.fromProto(a, attributes) catch |err| switch (err) {
        error.OutOfMemory => return,
    };

    for (view.fields, 0..) |field, i| {
        if (i > 0 and !std.mem.lessThan(u8, view.fields[i - 1].name, field.name)) return error.ViewNotSorted;
        inline for (@typeInfo(sa.ValueType).@"enum".field_names) |type_name| {
            try readSet(@field(sa.ValueType, type_name), view, a, field.name);
        }
    }
    const missing = "a name longer than any drawn";
    if (try view.get(a, sa.key(.keyword, missing)) != null) return error.MissingAttributeRead;

    const again = try sa.View.fromProto(a, try view.toProto(a));
    try std.testing.expectEqualDeep(view, again);
}

/// Reads the set attribute `name` as `value_type`: a value or an error of `DecodeError`.
fn readSet(comptime value_type: sa.ValueType, view: sa.View, a: std.mem.Allocator, name: []const u8) !void {
    const result: sa.DecodeError!?value_type.Type() = view.get(a, sa.key(value_type, name));
    const value = result catch |err| switch (err) {
        error.OutOfMemory, error.InvalidPayload, error.UnknownEncoding => return,
    };
    if (value == null) return error.SetAttributeReadAsNull;
}

fn draw(smith: *std.testing.Smith, b: *Buffers) SearchAttributes.IndexedFieldsEntry {
    const name = b.name[0..smith.slice(&b.name)];
    if (smith.eos()) return .{ .key = name };
    var count: usize = 0;
    while (count < max_metadata and !smith.eos()) : (count += 1) {
        b.entries[count] = .{
            .key = b.keys[count][0..smith.slice(&b.keys[count])],
            .value = b.values[count][0..smith.slice(&b.values[count])],
        };
    }
    return .{ .key = name, .value = .{
        .metadata = .fromOwnedSlice(b.entries[0..count]),
        .data = b.data[0..smith.slice(&b.data)],
    } };
}
