//! Loggers handed to workflow and activity code, and the sinks their messages go to.
//!
//! A call names a message and a struct of key-value fields:
//!
//!     ctx.logger().info("HelloWorld workflow started", .{ .name = name });
//!
//! The fields are rendered as text when the call is made and passed, together with the
//! logger's context fields (workflow ID, run ID, and so on), to a `Sink`. The default sink,
//! `Sink.std_log`, writes them to `std.log` under the `temporal` scope in logfmt style:
//! `HelloWorld workflow started name=Temporal WorkflowType=Workflow WorkflowID=...`.
//!
//! A workflow logger stays silent while the workflow replays history, so each message is
//! logged once, not once per replay (see `worker.Options.log_during_replay`).
//!
//! Set `client.Options.logger` (inherited by every worker on the client) or
//! `worker.Options.logger` to send logs elsewhere. The client's sink also receives sdk-core's
//! own logs (see `client.Options.core_log_level`). A sink that writes JSON lines:
//!
//!     const JsonLines = struct {
//!         mutex: std.Io.Mutex = .init,
//!         io: std.Io,
//!         out: *std.Io.Writer,
//!
//!         fn sink(j: *JsonLines) temporal.log.Sink {
//!             return .{ .ptr = j, .vtable = &.{ .log = write } };
//!         }
//!
//!         fn write(ptr: ?*anyopaque, level: temporal.log.Level, message: []const u8, fields: []const temporal.log.Field) void {
//!             const j: *JsonLines = @ptrCast(@alignCast(ptr.?));
//!             j.mutex.lockUncancelable(j.io);
//!             defer j.mutex.unlock(j.io);
//!             var s: std.json.Stringify = .{ .writer = j.out };
//!             s.beginObject() catch return;
//!             s.objectField("level") catch return;
//!             s.write(@tagName(level)) catch return;
//!             s.objectField("msg") catch return;
//!             s.write(message) catch return;
//!             for (fields) |f| {
//!                 s.objectField(f.key) catch return;
//!                 s.write(f.value) catch return;
//!             }
//!             s.endObject() catch return;
//!             j.out.writeByte('\n') catch return;
//!             j.out.flush() catch return;
//!         }
//!     };
//!
//! A sink is called concurrently from workflow and activity threads and from sdk-core's
//! threads, so it must be thread-safe, and it must outlive every client and worker using it.
//! The message and fields are only valid during the call.

const std = @import("std");
const Allocator = std.mem.Allocator;

pub const Level = std.log.Level;

/// One key-value pair of a log record. Values are rendered as text: strings as they are,
/// errors by name, and other values with their default formatting.
pub const Field = struct {
    key: []const u8,
    value: []const u8,
};

/// Where log records go. See the namespace documentation for thread-safety and lifetime.
pub const Sink = struct {
    ptr: ?*anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        /// `message` and `fields` (the call's fields, then the logger's context fields) are
        /// only valid during the call.
        log: *const fn (ptr: ?*anyopaque, level: Level, message: []const u8, fields: []const Field) void,
    };

    pub fn log(s: Sink, level: Level, message: []const u8, fields: []const Field) void {
        s.vtable.log(s.ptr, level, message, fields);
    }

    /// Writes to `std.log` under the `temporal` scope: the message, then ` key=value` per
    /// field, with values quoted when they contain spaces, tabs, or quotes.
    pub const std_log: Sink = .{ .ptr = null, .vtable = &.{ .log = stdLog } };

    fn stdLog(_: ?*anyopaque, level: Level, message: []const u8, fields: []const Field) void {
        const scoped = std.log.scoped(.temporal);
        switch (level) {
            inline else => |l| @field(scoped, @tagName(l))("{s}{f}", .{ message, Logfmt{ .fields = fields } }),
        }
    }
};

/// The most context fields a logger passes to its sink; `with` keeps more, but only the first
/// `max_context_fields` are logged.
pub const max_context_fields = 32;

pub const Logger = struct {
    sink: Sink = .std_log,
    /// Fields added to every message, after the call's own.
    context: []const Field = &.{},
    /// When set and true, messages are dropped (the workflow is replaying).
    replaying: ?*const bool = null,

    /// Returns a logger that adds `fields` (a struct, rendered now) to every message, after
    /// this logger's context fields. Memory comes from `arena`, which must outlive the logger.
    pub fn with(l: Logger, arena: Allocator, fields: anytype) error{OutOfMemory}!Logger {
        const names = comptime fieldNames(@TypeOf(fields));
        const context = try arena.alloc(Field, l.context.len + names.len);
        @memcpy(context[0..l.context.len], l.context);
        inline for (names, 0..) |name, i| {
            var out: std.Io.Writer.Allocating = .init(arena);
            writeValue(&out.writer, @field(fields, name)) catch return error.OutOfMemory;
            context[l.context.len + i] = .{ .key = name, .value = out.written() };
        }
        var result = l;
        result.context = context;
        return result;
    }

    pub fn debug(l: Logger, comptime message: []const u8, fields: anytype) void {
        l.log(.debug, message, fields);
    }

    pub fn info(l: Logger, comptime message: []const u8, fields: anytype) void {
        l.log(.info, message, fields);
    }

    pub fn warn(l: Logger, comptime message: []const u8, fields: anytype) void {
        l.log(.warn, message, fields);
    }

    pub fn err(l: Logger, comptime message: []const u8, fields: anytype) void {
        l.log(.err, message, fields);
    }

    /// Logs a message built at run time, such as a failure message that is also returned
    /// elsewhere, so both carry the same text.
    pub fn log(l: Logger, level: Level, message: []const u8, fields: anytype) void {
        if (l.silenced()) return;
        const names = comptime fieldNames(@TypeOf(fields));
        // Values that don't fit are truncated rather than allocated: this runs on workflow
        // coroutine stacks and must not fail.
        var text: [1024]u8 = undefined;
        var w: std.Io.Writer = .fixed(&text);
        var all: [names.len + max_context_fields]Field = undefined;
        inline for (names, 0..) |name, i| {
            const start = w.end;
            writeValue(&w, @field(fields, name)) catch {};
            all[i] = .{ .key = name, .value = text[start..w.end] };
        }
        const context = l.context[0..@min(l.context.len, max_context_fields)];
        @memcpy(all[names.len..][0..context.len], context);
        l.sink.log(level, message, all[0 .. names.len + context.len]);
    }

    fn silenced(l: Logger) bool {
        const r = l.replaying orelse return false;
        return r.*;
    }
};

fn fieldNames(comptime T: type) []const []const u8 {
    return switch (@typeInfo(T)) {
        .@"struct" => |s| s.field_names,
        else => &.{},
    };
}

fn writeValue(w: *std.Io.Writer, value: anytype) std.Io.Writer.Error!void {
    const V = @TypeOf(value);
    if (comptime isString(V)) return w.writeAll(value);
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

/// Renders fields as ` key=value key=value`, quoting values that contain spaces, tabs, or
/// quotes.
const Logfmt = struct {
    fields: []const Field,

    pub fn format(self: Logfmt, w: *std.Io.Writer) std.Io.Writer.Error!void {
        for (self.fields) |f| {
            if (std.mem.indexOfAny(u8, f.value, " \t\"") != null)
                try w.print(" {s}=\"{s}\"", .{ f.key, f.value })
            else
                try w.print(" {s}={s}", .{ f.key, f.value });
        }
    }
};

// ---------------------------------------------------------------- tests

const testing = std.testing;

/// Keeps the last record it received.
const Recorder = struct {
    arena: std.heap.ArenaAllocator,
    count: usize = 0,
    level: Level = .debug,
    message: []const u8 = "",
    fields: []const Field = &.{},

    fn sink(r: *Recorder) Sink {
        return .{ .ptr = r, .vtable = &.{ .log = record } };
    }

    fn record(ptr: ?*anyopaque, level: Level, message: []const u8, fields: []const Field) void {
        const r: *Recorder = @ptrCast(@alignCast(ptr.?));
        const a = r.arena.allocator();
        r.count += 1;
        r.level = level;
        r.message = a.dupe(u8, message) catch "";
        const copy = a.alloc(Field, fields.len) catch return;
        for (fields, copy) |f, *c| c.* = .{ .key = a.dupe(u8, f.key) catch "", .value = a.dupe(u8, f.value) catch "" };
        r.fields = copy;
    }

    fn get(r: *const Recorder, key: []const u8) ?[]const u8 {
        for (r.fields) |f| if (std.mem.eql(u8, f.key, key)) return f.value;
        return null;
    }
};

test "the default sink renders the message and fields as logfmt" {
    var recorder: Recorder = .{ .arena = .init(testing.allocator) };
    defer recorder.arena.deinit();
    const base: Logger = .{ .sink = recorder.sink() };
    const l = try base.with(recorder.arena.allocator(), .{ .WorkflowType = "Workflow", .WorkflowID = @as([]const u8, "wf-1") });
    l.info("HelloWorld workflow started", .{ .name = @as([]const u8, "Temporal"), .count = @as(u32, 3), .note = "two words" });

    var buf: [256]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try w.print("{s}{f}", .{ recorder.message, Logfmt{ .fields = recorder.fields } });
    try testing.expectEqualStrings("HelloWorld workflow started name=Temporal count=3 note=\"two words\" WorkflowType=Workflow WorkflowID=wf-1", w.buffered());
}

test "with adds context fields after the call's fields and keeps the parent's" {
    var recorder: Recorder = .{ .arena = .init(testing.allocator) };
    defer recorder.arena.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const parent = try (Logger{ .sink = recorder.sink() }).with(arena.allocator(), .{ .WorkflowID = "wf-1" });
    const child = try parent.with(arena.allocator(), .{ .Attempt = @as(u32, 2), .Error = error.Timeout });

    child.warn("retrying", .{ .after = "1s" });
    try testing.expectEqual(Level.warn, recorder.level);
    try testing.expectEqualStrings("retrying", recorder.message);
    try testing.expectEqual(4, recorder.fields.len);
    try testing.expectEqualStrings("after", recorder.fields[0].key);
    try testing.expectEqualStrings("WorkflowID", recorder.fields[1].key);
    try testing.expectEqualStrings("2", recorder.get("Attempt").?);
    try testing.expectEqualStrings("Timeout", recorder.get("Error").?);

    parent.err("failed", .{});
    try testing.expectEqual(Level.err, recorder.level);
    try testing.expectEqual(1, recorder.fields.len);
}

test "a custom sink receives workflow context fields as separate keys, unquoted" {
    var recorder: Recorder = .{ .arena = .init(testing.allocator) };
    defer recorder.arena.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const l = try (Logger{ .sink = recorder.sink() }).with(arena.allocator(), .{
        .WorkflowType = "Greeting Workflow",
        .WorkflowID = "wf-1",
        .RunID = "run-1",
    });
    l.debug("started", .{ .name = "two words" });
    try testing.expectEqual(Level.debug, recorder.level);
    try testing.expectEqualStrings("two words", recorder.get("name").?);
    try testing.expectEqualStrings("Greeting Workflow", recorder.get("WorkflowType").?);
    try testing.expectEqualStrings("wf-1", recorder.get("WorkflowID").?);
    try testing.expectEqualStrings("run-1", recorder.get("RunID").?);
}

test "a replaying logger is silent" {
    var recorder: Recorder = .{ .arena = .init(testing.allocator) };
    defer recorder.arena.deinit();
    var replaying = true;
    const l: Logger = .{ .sink = recorder.sink(), .replaying = &replaying };
    l.info("dropped", .{});
    try testing.expectEqual(0, recorder.count);
    replaying = false;
    l.info("kept", .{});
    try testing.expectEqual(1, recorder.count);
}

test "values longer than the render buffer are truncated" {
    var recorder: Recorder = .{ .arena = .init(testing.allocator) };
    defer recorder.arena.deinit();
    const long: [2000]u8 = @splat('x');
    (Logger{ .sink = recorder.sink() }).info("long", .{ .a = @as([]const u8, &long), .b = "after" });
    try testing.expectEqual(1024, recorder.get("a").?.len);
    try testing.expectEqualStrings("", recorder.get("b").?);
}
