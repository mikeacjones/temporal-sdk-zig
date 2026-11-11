//! Loggers handed to workflow and activity code (Go `workflow.GetLogger` /
//! `activity.GetLogger`).
//!
//! Messages go to `std.log` under the `temporal` scope, followed by the call's key-value
//! fields and the logger's own context fields (workflow ID, run ID, and so on), in logfmt
//! style: `HelloWorld workflow started name=Temporal WorkflowType=Workflow WorkflowID=...`.
//! A workflow logger stays silent while the workflow replays history, so each message is
//! logged once, not once per replay.

const std = @import("std");

const log = std.log.scoped(.temporal);

pub const Logger = struct {
    /// Pre-rendered context fields, such as "WorkflowType=Workflow WorkflowID=wf-1".
    context: []const u8 = "",
    /// When set and true, messages are dropped (the workflow is replaying).
    replaying: ?*const bool = null,

    pub fn debug(l: Logger, comptime message: []const u8, fields: anytype) void {
        if (l.silenced()) return;
        log.debug("{s}{f}{s}{s}", .{ message, Fields(@TypeOf(fields)){ .v = fields }, sep(l.context), l.context });
    }

    pub fn info(l: Logger, comptime message: []const u8, fields: anytype) void {
        if (l.silenced()) return;
        log.info("{s}{f}{s}{s}", .{ message, Fields(@TypeOf(fields)){ .v = fields }, sep(l.context), l.context });
    }

    pub fn warn(l: Logger, comptime message: []const u8, fields: anytype) void {
        if (l.silenced()) return;
        log.warn("{s}{f}{s}{s}", .{ message, Fields(@TypeOf(fields)){ .v = fields }, sep(l.context), l.context });
    }

    pub fn err(l: Logger, comptime message: []const u8, fields: anytype) void {
        if (l.silenced()) return;
        log.err("{s}{f}{s}{s}", .{ message, Fields(@TypeOf(fields)){ .v = fields }, sep(l.context), l.context });
    }

    fn silenced(l: Logger) bool {
        const r = l.replaying orelse return false;
        return r.*;
    }

    fn sep(context: []const u8) []const u8 {
        return if (context.len > 0) " " else "";
    }
};

/// Renders a struct of fields as ` key=value key=value`. Strings print as-is, quoted when
/// they contain spaces; other values use their default formatting.
fn Fields(comptime T: type) type {
    return struct {
        v: T,

        pub fn format(self: @This(), w: *std.Io.Writer) std.Io.Writer.Error!void {
            const info = @typeInfo(T);
            if (info != .@"struct") return;
            inline for (info.@"struct".field_names) |name| {
                const value = @field(self.v, name);
                try w.print(" {s}=", .{name});
                try writeValue(w, value);
            }
        }
    };
}

fn writeValue(w: *std.Io.Writer, value: anytype) std.Io.Writer.Error!void {
    const V = @TypeOf(value);
    if (comptime isString(V)) {
        const s: []const u8 = value;
        if (std.mem.indexOfAny(u8, s, " \t\"") != null) return w.print("\"{s}\"", .{s});
        return w.writeAll(s);
    }
    if (@typeInfo(V) == .error_set) return w.writeAll(@errorName(value));
    return w.print("{any}", .{value});
}

fn isString(comptime V: type) bool {
    return switch (@typeInfo(V)) {
        .pointer => |p| (p.size == .slice and p.child == u8) or
            (p.size == .one and @typeInfo(p.child) == .array and @typeInfo(p.child).array.child == u8),
        else => false,
    };
}

/// Builds the context string for a logger. Memory comes from `arena`.
pub fn contextString(arena: std.mem.Allocator, fields: anytype) error{OutOfMemory}![]const u8 {
    var out: std.Io.Writer.Allocating = .init(arena);
    (Fields(@TypeOf(fields)){ .v = fields }).format(&out.writer) catch return error.OutOfMemory;
    const s = out.written();
    return if (s.len > 0) s[1..] else s;
}

test "fields render as logfmt and context strings drop the leading space" {
    var buf: [128]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try (Fields(@TypeOf(.{ .name = @as([]const u8, "Temporal"), .count = @as(u32, 3), .note = "two words" })){
        .v = .{ .name = @as([]const u8, "Temporal"), .count = @as(u32, 3), .note = "two words" },
    }).format(&w);
    try std.testing.expectEqualStrings(" name=Temporal count=3 note=\"two words\"", w.buffered());

    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    try std.testing.expectEqualStrings("WorkflowID=wf-1 Attempt=2", try contextString(arena.allocator(), .{ .WorkflowID = @as([]const u8, "wf-1"), .Attempt = @as(u32, 2) }));
}

test "a replaying logger is silent" {
    var replaying = true;
    const l: Logger = .{ .replaying = &replaying };
    try std.testing.expect(l.silenced());
    replaying = false;
    try std.testing.expect(!l.silenced());
}
