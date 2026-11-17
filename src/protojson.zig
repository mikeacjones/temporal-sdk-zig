//! ProtoJSON, as Temporal's CLI, Web UI and codec servers write it, for the generated
//! protobuf message types.

const std = @import("std");
const protobuf = @import("protobuf");
const Allocator = std.mem.Allocator;

pub const DecodeError = error{ OutOfMemory, InvalidProtoJson };

/// Decodes ProtoJSON, as Temporal's CLI, Web UI and codec servers write it, into the
/// generated message type `T`.
///
/// Field names may be lowerCamelCase or as in the .proto file. Bytes fields and the values
/// of `map<string, bytes>` fields are base64, standard or URL-safe, padded or not. Fields
/// `T` does not know are skipped, and an enum name that matches no value decodes as the
/// enum's zero value. Messages inside a `google.protobuf.Any` are left as the generated
/// decoder returns them: their bytes-map values are not decoded.
///
/// The result lives entirely in `arena` and borrows nothing from `json`. Nothing is freed
/// piecewise, so pass an arena. Pure and deterministic; safe from any thread.
pub fn decode(comptime T: type, arena: Allocator, json: []const u8) DecodeError!T {
    const parsed = T.jsonDecode(json, .{
        .ignore_unknown_fields = true,
        .allocate = .alloc_always,
    }, arena) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return error.InvalidProtoJson,
    };
    var message = parsed.value;
    try decodeBytesMaps(T, &message, arena);
    return message;
}

/// Replaces every value of every `map<string, bytes>` field reachable from `value` with its
/// base64-decoded bytes, guided by the `_desc_table` descriptors. The values all live in
/// `arena` (the generated decoder put them there), so they are changed in place.
///
/// This works around a defect of zig-protobuf: its `json.zig` parses map values with
/// `std.json.innerParse`, which treats a bytes value as a plain string, so the base64 text is
/// never decoded. Delete this walk once the pinned zig-protobuf commit decodes them; the
/// round-trip test "a Payload's metadata decodes from base64 and encodes back" then fails by
/// decoding twice.
fn decodeBytesMaps(comptime T: type, value: *T, arena: Allocator) DecodeError!void {
    if (comptime !mayHoldBytesMap(T)) return;
    switch (@typeInfo(T)) {
        .optional => |o| if (value.*) |*inner| try decodeBytesMaps(o.child, inner, arena),
        .pointer => |p| try decodeBytesMaps(p.child, value.*, arena),
        .@"union" => switch (value.*) {
            inline else => |*member| try decodeBytesMaps(@TypeOf(member.*), member, arena),
        },
        .@"struct" => |s| if (comptime isArrayList(T)) {
            const E = ArrayListElem(T);
            for (value.items) |*item| try decodeBytesMaps(E, item, arena);
        } else if (comptime isBytesMapEntry(T)) {
            value.value = protobuf.json.decodeBase64Lenient(arena, value.value) catch |err| switch (err) {
                error.OutOfMemory => return error.OutOfMemory,
                else => return error.InvalidProtoJson,
            };
        } else {
            inline for (s.field_names) |name| {
                if (comptime @hasField(@TypeOf(T._desc_table), name))
                    try decodeBytesMaps(@FieldType(T, name), &@field(value, name), arena);
            }
        },
        else => {},
    }
}

/// Whether a value of `T` can reach a `map<string, bytes>` value.
fn mayHoldBytesMap(comptime T: type) bool {
    comptime {
        @setEvalBranchQuota(1_000_000);
        var seen: []const type = &.{};
        return reachesBytesMap(T, &seen);
    }
}

/// A depth-first search over the types a `T` holds. Generated messages refer to themselves
/// through pointers (a failure's `cause`) and through lists and map values (a descriptor's
/// `nested_type`), so each type is searched once: a type already in `seen` was either found
/// to reach none or is still being searched further up, and either way adds nothing.
fn reachesBytesMap(comptime T: type, comptime seen: *[]const type) bool {
    for (seen.*) |S| if (S == T) return false;
    seen.* = seen.* ++ .{T};
    return switch (@typeInfo(T)) {
        .optional => |o| reachesBytesMap(o.child, seen),
        .pointer => |p| p.size == .one and reachesBytesMap(p.child, seen),
        .@"union" => |u| for (u.field_types) |F| {
            if (reachesBytesMap(F, seen)) break true;
        } else false,
        .@"struct" => |s| if (isArrayList(T))
            reachesBytesMap(ArrayListElem(T), seen)
        else if (isBytesMapEntry(T))
            true
        else if (!@hasDecl(T, "_desc_table"))
            false
        else for (s.field_names, s.field_types) |name, F| {
            if (@hasField(@TypeOf(T._desc_table), name) and reachesBytesMap(F, seen)) break true;
        } else false,
        else => false,
    };
}

/// A map entry whose value is bytes. A map entry is recognized as zig-protobuf's `json.zig`
/// recognizes it (a struct of exactly `key` and `value`), so this reaches exactly the values
/// it parsed with `std.json.innerParse`.
fn isBytesMapEntry(comptime T: type) bool {
    if (@typeInfo(T) != .@"struct" or !@hasDecl(T, "_desc_table")) return false;
    const names = @typeInfo(T).@"struct".field_names;
    if (names.len != 2 or !std.mem.eql(u8, names[0], "key") or !std.mem.eql(u8, names[1], "value")) return false;
    return switch (T._desc_table.value.ftype) {
        .scalar => |s| s == .bytes,
        else => false,
    };
}

fn isArrayList(comptime T: type) bool {
    return @typeInfo(T) == .@"struct" and @hasField(T, "items") and @hasField(T, "capacity");
}

fn ArrayListElem(comptime T: type) type {
    return @typeInfo(@FieldType(T, "items")).pointer.child;
}

// ---------------------------------------------------------------- tests

const testing = std.testing;
const proto = @import("proto.zig");
const common = proto.temporal.api.common.v1;

fn encodeForTest(arena: Allocator, message: anytype) ![]const u8 {
    return message.jsonEncode(.{}, .{ .emit_oneof_field_name = false }, arena);
}

test "a Payload's metadata decodes from base64 and encodes back" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const json =
        \\{"metadata":{"encoding":"anNvbi9wbGFpbg=="},"data":"IlRlbXBvcmFsIg=="}
    ;
    const p = try decode(common.Payload, a, json);
    try testing.expectEqual(@as(usize, 1), p.metadata.items.len);
    try testing.expectEqualStrings("encoding", p.metadata.items[0].key);
    try testing.expectEqualStrings("json/plain", p.metadata.items[0].value);
    try testing.expectEqualStrings("\"Temporal\"", p.data);
    try testing.expectEqualStrings(json, try encodeForTest(a, p));
}

test "metadata values that are not UTF-8 and URL-safe unpadded base64 decode" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    // 0xff 0xfe 0xfd is "//79" in standard base64 and "__79" URL-safe; "_-8" is 0xff 0xef.
    const p = try decode(common.Payload, a,
        \\{"metadata":{"raw":"//79","url":"_-8"}}
    );
    try testing.expectEqual(@as(usize, 2), p.metadata.items.len);
    try testing.expectEqualSlices(u8, &.{ 0xff, 0xfe, 0xfd }, p.metadata.items[0].value);
    try testing.expectEqualSlices(u8, &.{ 0xff, 0xef }, p.metadata.items[1].value);
    try testing.expectEqualStrings(
        \\{"metadata":{"raw":"//79","url":"/+8="}}
    , try encodeForTest(a, p));
}

test "invalid base64 in a metadata value is InvalidProtoJson" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    try testing.expectError(error.InvalidProtoJson, decode(common.Payload, arena.allocator(),
        \\{"metadata":{"encoding":"not base64!"}}
    ));
    try testing.expectError(error.InvalidProtoJson, decode(common.Payload, arena.allocator(), "{"));
}

test "payloads in repeated fields, map values and oneofs of nested messages decode" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // Repeated: Payloads.payloads.
    const ps = try decode(common.Payloads, a,
        \\{"payloads":[{"metadata":{"encoding":"anNvbi9wbGFpbg=="},"data":"MQ=="},{"metadata":{"encoding":"YmluYXJ5L251bGw="}}]}
    );
    try testing.expectEqual(@as(usize, 2), ps.payloads.items.len);
    try testing.expectEqualStrings("json/plain", ps.payloads.items[0].metadata.items[0].value);
    try testing.expectEqualStrings("binary/null", ps.payloads.items[1].metadata.items[0].value);

    // Map values that are messages: Memo.fields.
    const memo = try decode(common.Memo, a,
        \\{"fields":{"note":{"metadata":{"encoding":"anNvbi9wbGFpbg=="},"data":"ImhpIg=="}}}
    );
    try testing.expectEqualStrings("json/plain", memo.fields.items[0].value.?.metadata.items[0].value);
    try testing.expectEqualStrings("\"hi\"", memo.fields.items[0].value.?.data);

    // A oneof member, an optional submessage, and a pointer-held submessage: Failure's
    // failure_info, encoded_attributes and cause.
    const f = try decode(proto.temporal.api.failure.v1.Failure, a,
        \\{"message":"outer","encodedAttributes":{"metadata":{"encoding":"anNvbi9wbGFpbg=="}},
        \\ "cause":{"message":"inner","cause":{"applicationFailureInfo":{"details":{"payloads":[{"metadata":{"encoding":"YmluYXJ5L251bGw="}}]}}}}}
    );
    try testing.expectEqualStrings("json/plain", f.encoded_attributes.?.metadata.items[0].value);
    const inner = f.cause.?.cause.?;
    const details = inner.failure_info.?.application_failure_info.details.?;
    try testing.expectEqualStrings("binary/null", details.payloads.items[0].metadata.items[0].value);
}

test "the result borrows nothing from the input" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const json = try testing.allocator.dupe(u8,
        \\{"payloads":[{"metadata":{"encoding":"anNvbi9wbGFpbg=="},"data":"MQ=="}]}
    );
    const ps = decode(common.Payloads, arena.allocator(), json);
    @memset(json, 'x');
    testing.allocator.free(json);
    const p = (try ps).payloads.items[0];
    try testing.expectEqualStrings("encoding", p.metadata.items[0].key);
    try testing.expectEqualStrings("json/plain", p.metadata.items[0].value);
}

test "a message that holds a repeated field of its own type decodes" {
    // google.protobuf.DescriptorProto.nested_type is `repeated DescriptorProto`: the walk's
    // compile-time pruning must end on a type that refers to itself through a list, not only
    // through a pointer.
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const d = try decode(proto.google.protobuf.DescriptorProto, arena.allocator(),
        \\{"name":"Outer","nestedType":[{"name":"Inner"}]}
    );
    try testing.expectEqualStrings("Outer", d.name.?);
    try testing.expectEqualStrings("Inner", d.nested_type.items[0].name.?);
}
