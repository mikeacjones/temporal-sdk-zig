//! Tests for the logger interceptor sample: a sink records every log line, and the tests
//! check the fields the interceptor added.

const std = @import("std");
const temporal = @import("temporal");
const logger_interceptor = @import("logger_interceptor.zig");

const testing = std.testing;
const log = temporal.log;
const Allocator = std.mem.Allocator;

/// A `log.Sink` that copies every record into `arena`. Records arrive from workflow, activity,
/// and sdk-core threads, so they are appended under `mutex`.
const CapturingSink = struct {
    io: std.Io,
    arena: Allocator,
    mutex: std.Io.Mutex = .init,
    entries: std.ArrayList(Entry) = .empty,

    const Entry = struct {
        level: log.Level,
        message: []const u8,
        fields: []const log.Field,
    };

    fn sink(c: *CapturingSink) log.Sink {
        return .{ .ptr = c, .vtable = &.{ .log = record } };
    }

    fn record(ptr: ?*anyopaque, level: log.Level, message: []const u8, fields: []const log.Field) void {
        const c: *CapturingSink = @ptrCast(@alignCast(ptr.?));
        c.mutex.lockUncancelable(c.io);
        defer c.mutex.unlock(c.io);
        c.append(level, message, fields) catch {};
    }

    fn append(c: *CapturingSink, level: log.Level, message: []const u8, fields: []const log.Field) Allocator.Error!void {
        const copied = try c.arena.alloc(log.Field, fields.len);
        for (fields, copied) |f, *to| to.* = .{ .key = try c.arena.dupe(u8, f.key), .value = try c.arena.dupe(u8, f.value) };
        try c.entries.append(c.arena, .{ .level = level, .message = try c.arena.dupe(u8, message), .fields = copied });
    }

    /// The value of `key` on the first `level` record named `message`, or null.
    fn field(c: *CapturingSink, level: log.Level, message: []const u8, key: []const u8) ?[]const u8 {
        for (c.entries.items) |e| {
            if (e.level != level or !std.mem.eql(u8, e.message, message)) continue;
            for (e.fields) |f| if (std.mem.eql(u8, f.key, key)) return f.value;
        }
        return null;
    }
};

fn workflowTag(_: *temporal.workflow.Context, logger: log.Logger, arena: Allocator) error{OutOfMemory}!log.Logger {
    return logger.with(arena, .{ .@"workflow-tag" = "workflow-value" });
}

fn activityTag(_: *temporal.activity.Context, logger: log.Logger, arena: Allocator) error{OutOfMemory}!log.Logger {
    return logger.with(arena, .{ .@"activity-tag" = "activity-value" });
}

/// Runs the workflow with `options` on a dev server, recording its logs in `capture`.
fn run(capture: *CapturingSink, options: logger_interceptor.InterceptorOptions) !void {
    var logging: logger_interceptor.WorkerInterceptor = .{ .options = options };
    const interceptors = [_]temporal.interceptor.Worker{.from(logger_interceptor.WorkerInterceptor, &logging)};
    const env = try temporal.testing.WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{
        .logger = capture.sink(),
        .worker_interceptors = &interceptors,
    });
    defer env.deinit();
    try env.registerWorkflow(logger_interceptor.Workflow);
    try env.registerActivity(logger_interceptor.Activity);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const result = try env.executeWorkflow(logger_interceptor.Workflow, .{"Temporal"}, arena.allocator(), null);
    try testing.expectEqualStrings("Hello Temporal!", result);
}

test "the interceptor adds its fields to workflow and activity logs" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var capture: CapturingSink = .{ .io = testing.io, .arena = arena.allocator() };
    try run(&capture, .{ .workflow_fields = workflowTag, .activity_fields = activityTag });

    // The environment is closed, so nothing records any more.
    try testing.expectEqualStrings("workflow-value", capture.field(.info, "HelloWorld workflow started", "workflow-tag").?);
    try testing.expectEqualStrings("Temporal", capture.field(.info, "HelloWorld workflow started", "name").?);
    try testing.expectEqualStrings("workflow-value", capture.field(.info, "HelloWorld workflow completed", "workflow-tag").?);
    try testing.expectEqualStrings("activity-value", capture.field(.info, "Activity", "activity-tag").?);
    try testing.expectEqualStrings("Activity", capture.field(.info, "Activity", "ActivityType").?);
    try testing.expect(capture.field(.info, "Activity", "workflow-tag") == null);
    try testing.expect(capture.field(.info, "HelloWorld workflow started", "activity-tag") == null);
}

test "the worker's fields carry the workflow and activity start times" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var capture: CapturingSink = .{ .io = testing.io, .arena = arena.allocator() };
    const before = try logger_interceptor.rfc3339(arena.allocator(), .now(testing.io, .real));
    try run(&capture, .{
        .workflow_fields = logger_interceptor.workflowStartTime,
        .activity_fields = logger_interceptor.activityStartTime,
    });
    const after = try logger_interceptor.rfc3339(arena.allocator(), .now(testing.io, .real));

    const workflow_start = capture.field(.info, "HelloWorld workflow started", "WorkflowStartTime").?;
    const activity_start = capture.field(.info, "Activity", "ActivityStartTime").?;
    // RFC 3339 UTC times of equal length compare in time order as strings.
    for ([_][]const u8{ workflow_start, activity_start }) |t| {
        try testing.expectEqual(before.len, t.len);
        try testing.expect(std.mem.order(u8, before, t) != .gt);
        try testing.expect(std.mem.order(u8, t, after) != .gt);
    }
    try testing.expect(std.mem.order(u8, workflow_start, activity_start) != .gt);
}

test "rfc3339 renders UTC to the second" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const t: std.Io.Timestamp = .{ .nanoseconds = 1_782_034_245_999_000_000 };
    try testing.expectEqualStrings("2026-06-21T09:30:45Z", try logger_interceptor.rfc3339(arena.allocator(), t));
    try testing.expectEqualStrings("1970-01-01T00:00:00Z", try logger_interceptor.rfc3339(arena.allocator(), .zero));
}
