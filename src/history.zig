//! Workflow histories: the types, and parsing them from JSON as `temporal workflow show
//! --output json` and the Temporal UI export them.
//!
//! Those exports are ProtoJSON, except that enums may use short names: `"eventType":
//! "WorkflowExecutionStarted"` for `EVENT_TYPE_WORKFLOW_EXECUTION_STARTED`. `fromJson` rewrites
//! a short name, guided by the generated message types, to the enum's prefix (taken from its
//! `*_UNSPECIFIED` value) plus the name in SCREAMING_SNAKE_CASE, then decodes the result with
//! `protojson.decode`, which also base64-decodes `Payload.metadata` values.

const std = @import("std");
const proto = @import("proto.zig");
const protojson = @import("protojson.zig");
const Allocator = std.mem.Allocator;

pub const History = proto.temporal.api.history.v1.History;
pub const HistoryEvent = proto.temporal.api.history.v1.HistoryEvent;

/// A history with the workflow ID it ran under, which the history itself doesn't record.
/// Both are borrowed.
pub const WorkflowHistory = struct {
    workflow_id: []const u8,
    history: History,
};

pub const Error = error{ OutOfMemory, InvalidHistoryJson };

/// Parses a history JSON export. Memory comes from `arena`.
pub fn fromJson(arena: Allocator, json: []const u8) Error!History {
    var root = std.json.parseFromSliceLeaky(std.json.Value, arena, json, .{}) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return error.InvalidHistoryJson,
    };
    try fix(History, arena, &root);
    const normalized = std.json.Stringify.valueAlloc(arena, root, .{}) catch return error.OutOfMemory;
    return protojson.decode(History, arena, normalized) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        error.InvalidProtoJson => return error.InvalidHistoryJson,
    };
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

test "metadata values that are not UTF-8 decode" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const h = try fromJson(arena.allocator(),
        \\{"events":[{"eventId":"1","eventType":"WorkflowExecutionStarted",
        \\ "workflowExecutionStartedEventAttributes":{"input":{"payloads":[{"metadata":{"encoding":"YmluYXJ5L3BsYWlu","key":"//79"}}]}}}]}
    );
    const md = h.events.items[0].attributes.?.workflow_execution_started_event_attributes.input.?.payloads.items[0].metadata.items;
    try testing.expectEqualStrings("binary/plain", md[0].value);
    try testing.expectEqualSlices(u8, &.{ 0xff, 0xfe, 0xfd }, md[1].value);
}
