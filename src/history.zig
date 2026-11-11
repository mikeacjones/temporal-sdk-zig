//! Workflow histories from JSON, as `temporal workflow show --output json` and the Temporal UI
//! export them.
//!
//! Those exports are ProtoJSON with two differences the protobuf parser can't take directly,
//! so `fromJson` rewrites the JSON first, guided by the generated message types:
//!
//! - Enums may use short names: `"eventType": "WorkflowExecutionStarted"` for
//!   `EVENT_TYPE_WORKFLOW_EXECUTION_STARTED`. A short name becomes the enum's prefix (taken from
//!   its `*_UNSPECIFIED` value) plus the name in SCREAMING_SNAKE_CASE.
//! - `Payload.metadata` values are base64 (they are bytes). They are decoded here because the
//!   protobuf library doesn't base64-decode bytes values inside maps yet (beads d51.2.6). This
//!   assumes metadata values are text, which encodings and other standard keys are.

const std = @import("std");
const proto = @import("proto.zig");
const Allocator = std.mem.Allocator;

pub const History = proto.temporal.api.history.v1.History;
const Payload = proto.temporal.api.common.v1.Payload;

pub const Error = error{ OutOfMemory, InvalidHistoryJson };

/// Parses a history JSON export. Memory comes from `arena`.
pub fn fromJson(arena: Allocator, json: []const u8) Error!History {
    var root = std.json.parseFromSliceLeaky(std.json.Value, arena, json, .{}) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return error.InvalidHistoryJson,
    };
    try fix(History, arena, &root);
    const normalized = std.json.Stringify.valueAlloc(arena, root, .{}) catch return error.OutOfMemory;
    const parsed = History.jsonDecode(normalized, .{ .ignore_unknown_fields = true }, arena) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return error.InvalidHistoryJson,
    };
    return parsed.value;
}

fn fix(comptime T: type, arena: Allocator, v: *std.json.Value) Error!void {
    @setEvalBranchQuota(1_000_000);
    switch (@typeInfo(T)) {
        .optional => |o| return fix(o.child, arena, v),
        .pointer => |p| if (p.size == .one) return fix(p.child, arena, v),
        .@"enum" => return fixEnum(T, arena, v),
        .@"struct" => {
            if (comptime isArrayList(T)) {
                const Elem = ArrayListElem(T);
                if (comptime isMapEntry(Elem)) {
                    if (v.* != .object) return;
                    const V = @FieldType(Elem, "value");
                    var it = v.object.iterator();
                    while (it.next()) |kv| try fix(V, arena, kv.value_ptr);
                    return;
                }
                if (v.* != .array) return;
                for (v.array.items) |*item| try fix(Elem, arena, item);
                return;
            }
            if (!@hasDecl(T, "_desc_table")) return;
            if (v.* != .object) return;
            if (T == Payload) try decodeMetadata(arena, v);
            inline for (@typeInfo(T).@"struct".field_names, @typeInfo(T).@"struct".field_types) |name, F| {
                if (comptime std.mem.eql(u8, name, "_unknown_fields")) continue;
                if (comptime unionOf(F)) |U| {
                    // Oneof members appear directly in the parent object.
                    inline for (@typeInfo(U).@"union".field_names, @typeInfo(U).@"union".field_types) |member, M| {
                        if (fieldValue(v, member)) |child| try fix(M, arena, child);
                    }
                } else if (fieldValue(v, name)) |child| {
                    try fix(F, arena, child);
                }
            }
        },
        else => {},
    }
}

fn fieldValue(v: *std.json.Value, comptime name: []const u8) ?*std.json.Value {
    return v.object.getPtr(comptime camel(name)) orelse v.object.getPtr(name);
}

fn fixEnum(comptime E: type, arena: Allocator, v: *std.json.Value) Error!void {
    if (v.* != .string) return;
    const s = v.string;
    inline for (@typeInfo(E).@"enum".field_names) |tag| if (std.mem.eql(u8, tag, s)) return;
    const prefix = comptime enumPrefix(E);
    var out: std.ArrayList(u8) = .empty;
    try out.appendSlice(arena, prefix);
    for (s, 0..) |ch, i| {
        if (std.ascii.isUpper(ch) and i > 0 and !std.ascii.isUpper(s[i - 1])) try out.append(arena, '_');
        try out.append(arena, std.ascii.toUpper(ch));
    }
    inline for (@typeInfo(E).@"enum".field_names) |tag| {
        if (std.mem.eql(u8, tag, out.items)) {
            v.* = .{ .string = tag };
            return;
        }
    }
}

/// "EVENT_TYPE_" for an enum whose first value is EVENT_TYPE_UNSPECIFIED.
fn enumPrefix(comptime E: type) []const u8 {
    const names = @typeInfo(E).@"enum".field_names;
    if (names.len == 0) return "";
    const first = names[0];
    const suffix = "UNSPECIFIED";
    if (std.mem.endsWith(u8, first, suffix)) return first[0 .. first.len - suffix.len];
    return "";
}

fn decodeMetadata(arena: Allocator, payload: *std.json.Value) Error!void {
    const md = payload.object.getPtr("metadata") orelse return;
    if (md.* != .object) return;
    var it = md.object.iterator();
    while (it.next()) |kv| {
        if (kv.value_ptr.* != .string) continue;
        const encoded = kv.value_ptr.string;
        const decoder = std.base64.standard.Decoder;
        const len = decoder.calcSizeForSlice(encoded) catch continue;
        const buf = try arena.alloc(u8, len);
        decoder.decode(buf, encoded) catch continue;
        if (!std.unicode.utf8ValidateSlice(buf)) continue;
        kv.value_ptr.* = .{ .string = buf };
    }
}

fn camel(comptime name: []const u8) []const u8 {
    comptime {
        var out: []const u8 = "";
        var upper = false;
        for (name) |ch| {
            if (ch == '_') {
                upper = true;
                continue;
            }
            out = out ++ &[_]u8{if (upper) std.ascii.toUpper(ch) else ch};
            upper = false;
        }
        return out;
    }
}

fn unionOf(comptime F: type) ?type {
    const inner = switch (@typeInfo(F)) {
        .optional => |o| o.child,
        else => F,
    };
    return switch (@typeInfo(inner)) {
        .@"union" => inner,
        else => null,
    };
}

fn isArrayList(comptime T: type) bool {
    return @hasField(T, "items") and @hasField(T, "capacity") and @hasDecl(T, "empty");
}

fn ArrayListElem(comptime T: type) type {
    return @typeInfo(@FieldType(T, "items")).pointer.child;
}

fn isMapEntry(comptime T: type) bool {
    if (@typeInfo(T) != .@"struct") return false;
    const names = @typeInfo(T).@"struct".field_names;
    return names.len == 2 and std.mem.eql(u8, names[0], "key") and std.mem.eql(u8, names[1], "value");
}

// ---------------------------------------------------------------- tests

const testing = std.testing;

test "a CLI history export with short enum names and base64 metadata parses" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const json =
        \\{"events":[
        \\ {"eventId":"1","eventType":"WorkflowExecutionStarted","eventTime":"2020-07-30T00:30:03.082421843Z",
        \\  "workflowExecutionStartedEventAttributes":{"workflowType":{"name":"Workflow"},"taskQueue":{"name":"hello-world","kind":"Normal"},
        \\   "input":{"payloads":[{"metadata":{"encoding":"anNvbi9wbGFpbg=="},"data":"IlRlbXBvcmFsIg=="}]},"workflowRunTimeout":"60s"}},
        \\ {"eventId":"2","eventType":"WorkflowTaskScheduled","workflowTaskScheduledEventAttributes":{"taskQueue":{"name":"hello-world"},"startToCloseTimeout":"10s","attempt":1}}
        \\]}
    ;
    const h = try fromJson(arena.allocator(), json);
    try testing.expectEqual(@as(usize, 2), h.events.items.len);
    const e = h.events.items[0];
    try testing.expectEqual(proto.temporal.api.enums.v1.EventType.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, e.event_type);
    const started = e.attributes.?.workflow_execution_started_event_attributes;
    try testing.expectEqual(proto.temporal.api.enums.v1.TaskQueueKind.TASK_QUEUE_KIND_NORMAL, started.task_queue.?.kind);
    const p = started.input.?.payloads.items[0];
    try testing.expectEqualStrings("json/plain", p.metadata.items[0].value);
    try testing.expectEqualStrings("\"Temporal\"", p.data);
    try testing.expectEqual(proto.temporal.api.enums.v1.EventType.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, h.events.items[1].event_type);
}

test "canonical enum names pass through unchanged" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const h = try fromJson(arena.allocator(),
        \\{"events":[{"eventId":"1","eventType":"EVENT_TYPE_WORKFLOW_EXECUTION_STARTED","workflowExecutionStartedEventAttributes":{"workflowType":{"name":"W"}}}]}
    );
    try testing.expectEqual(proto.temporal.api.enums.v1.EventType.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, h.events.items[0].event_type);
}
