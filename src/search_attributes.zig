//! Typed search attributes: indexed values recorded with a workflow, which visibility
//! queries filter on (`Client.listWorkflows`, `Client.countWorkflows`).
//!
//! A `Key` names an attribute and fixes its type, so values written and read through it are
//! typed. Declare keys once and share them between workflows and clients:
//!
//!     const sa = temporal.search_attributes;
//!     pub const customer_id = sa.key(.keyword, "CustomerId");
//!     pub const priority = sa.key(.int, "Priority");
//!
//!     // client: start with values
//!     .search_attributes = &.{ customer_id.set("c-42"), priority.set(3) },
//!     // workflow: change them, then read them back
//!     try ctx.upsertSearchAttributes(&.{ priority.set(1), customer_id.unset() });
//!     const p = try ctx.searchAttributes().get(ctx.allocator(), priority); // ?i64
//!
//! The attributes must exist on the server (`temporal operator search-attribute create`,
//! or `DevServer.Options.search_attributes`) before a workflow uses them.
//!
//! On the wire each value is a `json/plain` payload with a `type` metadata entry naming its
//! type (`Keyword`, `Text`, `Int`, `Double`, `Bool`, `Datetime`, `KeywordList`). An unset is
//! a `binary/null` payload without a type, which the server treats as a removal.

const std = @import("std");
const proto = @import("proto.zig");
const converter = @import("converter.zig");
const Allocator = std.mem.Allocator;

const common = proto.temporal.api.common.v1;
const log = std.log.scoped(.temporal);

pub const type_metadata_key = "type";

/// The type of a search attribute, as the server indexes it.
pub const ValueType = enum {
    /// An exact-match string, such as an ID or a status.
    keyword,
    /// Full-text searchable string.
    text,
    int,
    double,
    bool,
    datetime,
    /// A list of exact-match strings; a query matches any element.
    keyword_list,

    /// The Zig type of a value of this search attribute type.
    pub fn Type(comptime t: ValueType) type {
        return switch (t) {
            .keyword, .text => []const u8,
            .int => i64,
            .double => f64,
            .bool => bool,
            .datetime => converter.Time,
            .keyword_list => []const []const u8,
        };
    }

    /// The name the server uses in the payload's `type` metadata.
    pub fn metadataName(t: ValueType) []const u8 {
        return switch (t) {
            .keyword => "Keyword",
            .text => "Text",
            .int => "Int",
            .double => "Double",
            .bool => "Bool",
            .datetime => "Datetime",
            .keyword_list => "KeywordList",
        };
    }

    /// Parses `type` metadata, in its usual form ("KeywordList") or as the server's enum
    /// value name ("INDEXED_VALUE_TYPE_KEYWORD_LIST").
    pub fn fromMetadata(text: []const u8) ?ValueType {
        inline for (@typeInfo(ValueType).@"enum".field_names) |name| {
            const t = @field(ValueType, name);
            if (std.mem.eql(u8, text, comptime t.metadataName())) return t;
            if (std.ascii.eqlIgnoreCase(text, "INDEXED_VALUE_TYPE_" ++ name)) return t;
        }
        return null;
    }
};

/// A value of any search attribute type.
pub const Value = union(ValueType) {
    keyword: []const u8,
    text: []const u8,
    int: i64,
    double: f64,
    bool: bool,
    datetime: converter.Time,
    keyword_list: []const []const u8,
};

/// One change to a workflow's search attributes, made with `Key.set` or `Key.unset`. Borrows
/// the name and value; they are copied when the updates are encoded.
pub const Update = struct {
    name: []const u8,
    /// Null removes the attribute.
    value: ?Value,
};

/// A search attribute's name and type. A plain value: declare it as a `const`, or build it at
/// run time for a name read from configuration.
pub fn Key(comptime vt: ValueType) type {
    return struct {
        const Self = @This();

        pub const value_type = vt;
        /// The Zig type of the attribute's values.
        pub const Type = vt.Type();

        name: []const u8,

        /// An update that sets the attribute to `value`. Borrows `value` (and `name`) until
        /// the update is used.
        pub fn set(k: Self, value: Type) Update {
            return .{ .name = k.name, .value = @unionInit(Value, @tagName(vt), value) };
        }

        /// An update that removes the attribute.
        pub fn unset(k: Self) Update {
            return .{ .name = k.name, .value = null };
        }
    };
}

/// The key of the search attribute `name` of type `value_type`:
/// `key(.keyword, "CustomerId")`.
pub fn key(comptime value_type: ValueType, name: []const u8) Key(value_type) {
    return .{ .name = name };
}

pub const EncodeError = error{
    OutOfMemory,
    /// A value can't be indexed: a Double that is NaN or infinite, or a string that isn't
    /// UTF-8. Logged with the attribute's name.
    InvalidSearchAttribute,
};

/// What the encoded attributes are for.
pub const Purpose = enum {
    /// Changes to a running workflow's attributes: unsets are sent as removals.
    upsert,
    /// A new workflow's attributes: unsets are left out, there is nothing to remove.
    start,
};

/// Encodes `updates` in their order as the proto the server reads. Names, values and the
/// returned proto are allocated from `arena`.
pub fn toProto(arena: Allocator, updates: []const Update, purpose: Purpose) EncodeError!common.SearchAttributes {
    var out: common.SearchAttributes = .{};
    try out.indexed_fields.ensureTotalCapacity(arena, updates.len);
    for (updates) |u| {
        if (u.value == null and purpose == .start) continue;
        out.indexed_fields.appendAssumeCapacity(.{
            .key = try arena.dupe(u8, u.name),
            .value = try encodeValue(arena, u.name, u.value),
        });
    }
    return out;
}

/// The payload for one attribute value; null encodes an unset.
pub fn encodeValue(arena: Allocator, name: []const u8, value: ?Value) EncodeError!converter.Payload {
    const v = value orelse return converter.toPayload(arena, null);
    switch (v) {
        .double => |d| if (!std.math.isFinite(d)) {
            log.warn("search attribute \"{s}\": a Double value must be finite, got {d}", .{ name, d });
            return error.InvalidSearchAttribute;
        },
        .keyword, .text => |s| try checkUtf8(name, s),
        .keyword_list => |list| for (list) |s| try checkUtf8(name, s),
        else => {},
    }
    var p = switch (v) {
        inline else => |inner| try converter.toPayload(arena, inner),
    };
    try p.metadata.append(arena, .{ .key = type_metadata_key, .value = std.meta.activeTag(v).metadataName() });
    return p;
}

fn checkUtf8(name: []const u8, s: []const u8) error{InvalidSearchAttribute}!void {
    if (std.unicode.utf8ValidateSlice(s)) return;
    log.warn("search attribute \"{s}\": the value is not valid UTF-8; the server indexes text only", .{name});
    return error.InvalidSearchAttribute;
}

/// Whether the server removes an attribute upserted with this payload: a null (empty data)
/// or an empty list, whatever the metadata.
pub fn removes(p: converter.Payload) bool {
    return p.data.len == 0 or std.mem.eql(u8, p.data, "[]");
}

pub const DecodeError = error{
    OutOfMemory,
    /// The value doesn't decode as the key's type, or its `type` metadata names another type.
    InvalidPayload,
    UnknownEncoding,
};

/// A read-only set of search attributes sorted by name, as a workflow or a description sees
/// them. Read values with `get`; `fields` holds the raw payloads.
pub const View = struct {
    fields: []const converter.Field = &.{},

    /// Sorts the entries of a proto `SearchAttributes` into a view, leaving out removed
    /// values. The slice comes from `arena`; names and payloads borrow the entries.
    pub fn fromProto(arena: Allocator, attributes: ?common.SearchAttributes) error{OutOfMemory}!View {
        const entries = if (attributes) |a| a.indexed_fields.items else return .{};
        var fields: std.ArrayList(converter.Field) = try .initCapacity(arena, entries.len);
        for (entries) |entry| {
            const p = entry.value orelse continue;
            if (removes(p)) continue;
            fields.appendAssumeCapacity(.{ .name = entry.key, .value = p });
        }
        // A stable sort, so `dedupe` keeps the later of two entries with one name.
        std.mem.sort(converter.Field, fields.items, {}, fieldLessThan);
        return .{ .fields = dedupe(fields.items) };
    }

    /// The view with `entries` (encoded updates, applied in order) merged in: a removal
    /// deletes the name, any other value adds or replaces it. Allocates a new slice from
    /// `arena`, so `v` is unchanged; the new view borrows the entries.
    pub fn withEntries(v: View, arena: Allocator, entries: []const common.SearchAttributes.IndexedFieldsEntry) error{OutOfMemory}!View {
        var merged: std.ArrayList(converter.Field) = try .initCapacity(arena, v.fields.len + entries.len);
        merged.appendSliceAssumeCapacity(v.fields);
        for (entries) |entry| {
            const p = entry.value orelse converter.Payload{};
            const i = std.sort.lowerBound(converter.Field, merged.items, entry.key, fieldOrder);
            const found = i < merged.items.len and std.mem.eql(u8, merged.items[i].name, entry.key);
            if (removes(p)) {
                if (found) _ = merged.orderedRemove(i);
            } else if (found) {
                merged.items[i] = .{ .name = entry.key, .value = p };
            } else {
                merged.insertAssumeCapacity(i, .{ .name = entry.key, .value = p });
            }
        }
        return .{ .fields = merged.items };
    }

    pub fn len(v: View) usize {
        return v.fields.len;
    }

    /// The raw payload of the attribute `name`, or null if it isn't set.
    pub fn payload(v: View, name: []const u8) ?converter.Payload {
        const i = std.sort.binarySearch(converter.Field, v.fields, name, fieldOrder) orelse return null;
        return v.fields[i].value;
    }

    /// The value of the attribute `k` names, decoded into `arena`, or null if it isn't set.
    pub fn get(v: View, arena: Allocator, k: anytype) DecodeError!?@TypeOf(k).Type {
        const K = @TypeOf(k);
        const p = v.payload(k.name) orelse return null;
        for (p.metadata.items) |entry| {
            if (!std.mem.eql(u8, entry.key, type_metadata_key)) continue;
            const t = ValueType.fromMetadata(entry.value) orelse return error.InvalidPayload;
            if (t != K.value_type) return error.InvalidPayload;
        }
        return converter.fromPayload(K.Type, arena, p) catch |err| switch (err) {
            error.WrongArgumentCount => error.InvalidPayload,
            else => |e| e,
        };
    }

    /// Copies the view and its payloads into `arena`.
    pub fn dupe(v: View, arena: Allocator) error{OutOfMemory}!View {
        const fields = try arena.alloc(converter.Field, v.fields.len);
        for (v.fields, fields) |f, *d| d.* = .{ .name = try arena.dupe(u8, f.name), .value = try f.value.dupe(arena) };
        return .{ .fields = fields };
    }

    /// The view as a proto `SearchAttributes`, for a start-like command. The entries borrow
    /// the view; the list comes from `arena`.
    pub fn toProto(v: View, arena: Allocator) error{OutOfMemory}!common.SearchAttributes {
        var out: common.SearchAttributes = .{};
        try out.indexed_fields.ensureTotalCapacity(arena, v.fields.len);
        for (v.fields) |f| out.indexed_fields.appendAssumeCapacity(.{ .key = f.name, .value = f.value });
        return out;
    }

    /// Keeps the last of each run of equal names in a sorted slice.
    fn dedupe(fields: []converter.Field) []converter.Field {
        var n: usize = 0;
        for (fields, 0..) |f, i| {
            if (i + 1 < fields.len and std.mem.eql(u8, f.name, fields[i + 1].name)) continue;
            fields[n] = f;
            n += 1;
        }
        return fields[0..n];
    }
};

fn fieldLessThan(_: void, x: converter.Field, y: converter.Field) bool {
    return std.mem.lessThan(u8, x.name, y.name);
}

fn fieldOrder(name: []const u8, f: converter.Field) std.math.Order {
    return std.mem.order(u8, name, f.name);
}

// ---------------------------------------------------------------- tests

const testing = std.testing;

fn metadataOf(p: converter.Payload, k: []const u8) ?[]const u8 {
    for (p.metadata.items) |entry| if (std.mem.eql(u8, entry.key, k)) return entry.value;
    return null;
}

test "every type encodes as typed JSON and decodes back through its key" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const kw = key(.keyword, "CustomKeywordField");
    const txt = key(.text, "CustomTextField");
    const n = key(.int, "CustomIntField");
    const d = key(.double, "CustomDoubleField");
    const b = key(.bool, "CustomBoolField");
    const dt = key(.datetime, "CustomDatetimeField");
    const kl = key(.keyword_list, "CustomKeywordListField");
    const when = try converter.Time.parse("2024-02-29T12:30:00.5Z");

    const proto_sa = try toProto(a, &.{
        kw.set("order-1"),
        txt.set("a long description"),
        n.set(-42),
        d.set(2.5),
        b.set(true),
        dt.set(when),
        kl.set(&.{ "red", "blue" }),
    }, .upsert);
    const entries = proto_sa.indexed_fields.items;
    const want = [_]struct { []const u8, []const u8, []const u8 }{
        .{ "CustomKeywordField", "Keyword", "\"order-1\"" },
        .{ "CustomTextField", "Text", "\"a long description\"" },
        .{ "CustomIntField", "Int", "-42" },
        .{ "CustomDoubleField", "Double", "2.5" },
        .{ "CustomBoolField", "Bool", "true" },
        .{ "CustomDatetimeField", "Datetime", "\"2024-02-29T12:30:00.5Z\"" },
        .{ "CustomKeywordListField", "KeywordList", "[\"red\",\"blue\"]" },
    };
    try testing.expectEqual(want.len, entries.len);
    for (want, entries) |w, e| {
        try testing.expectEqualStrings(w[0], e.key);
        try testing.expectEqualStrings("json/plain", converter.encodingOf(e.value.?).?);
        try testing.expectEqualStrings(w[1], metadataOf(e.value.?, "type").?);
        try testing.expectEqualStrings(w[2], e.value.?.data);
    }

    const view = try View.fromProto(a, proto_sa);
    try testing.expectEqual(@as(usize, 7), view.len());
    try testing.expectEqualStrings("CustomBoolField", view.fields[0].name); // sorted
    try testing.expectEqualStrings("order-1", (try view.get(a, kw)).?);
    try testing.expectEqualStrings("a long description", (try view.get(a, txt)).?);
    try testing.expectEqual(@as(?i64, -42), try view.get(a, n));
    try testing.expectEqual(@as(?f64, 2.5), try view.get(a, d));
    try testing.expectEqual(@as(?bool, true), try view.get(a, b));
    try testing.expectEqual(when, (try view.get(a, dt)).?);
    const list = (try view.get(a, kl)).?;
    try testing.expectEqual(@as(usize, 2), list.len);
    try testing.expectEqualStrings("blue", list[1]);
    try testing.expectEqual(@as(?i64, null), try view.get(a, key(.int, "Missing")));
}

test "unset is a typeless null for an upsert and left out of a start" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const kw = key(.keyword, "CustomKeywordField");

    const up = try toProto(a, &.{ kw.unset(), key(.int, "CustomIntField").set(1) }, .upsert);
    try testing.expectEqual(@as(usize, 2), up.indexed_fields.items.len);
    const unset = up.indexed_fields.items[0].value.?;
    try testing.expectEqualStrings("binary/null", converter.encodingOf(unset).?);
    try testing.expectEqual(@as(?[]const u8, null), metadataOf(unset, "type"));
    try testing.expect(removes(unset));

    const start = try toProto(a, &.{ kw.unset(), key(.int, "CustomIntField").set(1) }, .start);
    try testing.expectEqual(@as(usize, 1), start.indexed_fields.items.len);
    try testing.expectEqualStrings("CustomIntField", start.indexed_fields.items[0].key);
}

test "a view applies updates in order without changing the original" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const kw = key(.keyword, "B");
    const n = key(.int, "A");
    const kl = key(.keyword_list, "C");

    const start = try View.fromProto(a, try toProto(a, &.{ kw.set("x"), n.set(1) }, .start));
    const changes = try toProto(a, &.{ n.set(2), kw.unset(), kl.set(&.{"t"}), n.set(3), key(.int, "Gone").unset() }, .upsert);
    const after = try start.withEntries(a, changes.indexed_fields.items);
    try testing.expectEqual(@as(usize, 2), start.len());
    try testing.expectEqualStrings("x", (try start.get(a, kw)).?);
    try testing.expectEqual(@as(usize, 2), after.len());
    try testing.expectEqual(@as(?i64, 3), try after.get(a, n));
    try testing.expectEqual(@as(?[]const u8, null), try after.get(a, kw));
    try testing.expectEqualStrings("t", (try after.get(a, kl)).?[0]);

    // An empty keyword list removes the attribute, as on the server.
    const cleared = try after.withEntries(a, (try toProto(a, &.{kl.set(&.{})}, .upsert)).indexed_fields.items);
    try testing.expectEqual(@as(usize, 1), cleared.len());
    try testing.expectEqual(@as(?[]const []const u8, null), try cleared.get(a, kl));
}

test "decode errors: wrong type metadata, wrong JSON, unknown encoding" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var server: common.SearchAttributes = .{};
    var screaming = try converter.toPayload(a, @as(i64, 7));
    try screaming.metadata.append(a, .{ .key = "type", .value = "INDEXED_VALUE_TYPE_INT" });
    try server.indexed_fields.append(a, .{ .key = "Screaming", .value = screaming });
    try server.indexed_fields.append(a, .{ .key = "Untyped", .value = try converter.toPayload(a, @as(i64, 8)) });
    try server.indexed_fields.append(a, .{ .key = "Text", .value = try encodeValue(a, "Text", .{ .text = "words" }) });
    try server.indexed_fields.append(a, .{ .key = "Raw", .value = .{ .data = "1" } });
    try server.indexed_fields.append(a, .{ .key = "Removed", .value = try encodeValue(a, "Removed", null) });
    try server.indexed_fields.append(a, .{ .key = "Empty" });
    const view = try View.fromProto(a, server);
    try testing.expectEqual(@as(usize, 4), view.len());

    try testing.expectEqual(@as(?i64, 7), try view.get(a, key(.int, "Screaming")));
    try testing.expectEqual(@as(?i64, 8), try view.get(a, key(.int, "Untyped")));
    // Read with the wrong key type: Text isn't Keyword, and words aren't an Int.
    try testing.expectError(error.InvalidPayload, view.get(a, key(.keyword, "Text")));
    try testing.expectError(error.InvalidPayload, view.get(a, key(.bool, "Untyped")));
    try testing.expectError(error.UnknownEncoding, view.get(a, key(.int, "Raw")));

    var bogus = try converter.toPayload(a, @as(i64, 1));
    try bogus.metadata.append(a, .{ .key = "type", .value = "Decimal" });
    const odd = try View.fromProto(a, .{ .indexed_fields = .fromOwnedSlice(try a.dupe(common.SearchAttributes.IndexedFieldsEntry, &.{.{ .key = "Odd", .value = bogus }})) });
    try testing.expectError(error.InvalidPayload, odd.get(a, key(.int, "Odd")));
}

test "values the server can't index are rejected when encoding" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    try testing.expectError(error.InvalidSearchAttribute, toProto(a, &.{key(.double, "D").set(std.math.nan(f64))}, .upsert));
    try testing.expectError(error.InvalidSearchAttribute, toProto(a, &.{key(.double, "D").set(std.math.inf(f64))}, .start));
    try testing.expectError(error.InvalidSearchAttribute, toProto(a, &.{key(.keyword, "K").set("\xff")}, .upsert));
    try testing.expectError(error.InvalidSearchAttribute, toProto(a, &.{key(.keyword_list, "L").set(&.{ "ok", "\xc3" })}, .upsert));
}
