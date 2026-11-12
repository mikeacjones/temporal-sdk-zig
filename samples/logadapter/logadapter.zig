//! Plugging an application's own logger into the SDK: `JsonSink` writes every log record,
//! from workflows, activities, and sdk-core, as one JSON object per line, and the worker
//! passes it as `client.Options.logger`.

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;
const activity = temporal.activity;
const log = temporal.log;

/// A `log.Sink` that writes one JSON object per record: `level`, `msg`, then each field as a
/// key. When a key repeats, the first value wins (a call's own fields come before the
/// logger's context fields), so every line parses as a JSON object. Records less severe
/// than `min_level` are dropped.
///
/// Thread-safe: records arrive from workflow and activity threads and from sdk-core's
/// threads, and are written whole under `mutex`. `out` is borrowed and must outlive every
/// client and worker using the sink. Write errors are dropped: logging never fails a task.
pub const JsonSink = struct {
    io: std.Io,
    out: *std.Io.Writer,
    min_level: log.Level = .debug,
    mutex: std.Io.Mutex = .init,

    pub fn sink(j: *JsonSink) log.Sink {
        return .{ .ptr = j, .vtable = &.{ .log = write } };
    }

    fn write(ptr: ?*anyopaque, level: log.Level, message: []const u8, fields: []const log.Field) void {
        const j: *JsonSink = @ptrCast(@alignCast(ptr.?));
        // std.log orders levels from most severe (err) to least (debug).
        if (@backingInt(level) > @backingInt(j.min_level)) return;
        j.mutex.lockUncancelable(j.io);
        defer j.mutex.unlock(j.io);
        writeRecord(j.out, level, message, fields) catch {};
    }

    fn writeRecord(out: *std.Io.Writer, level: log.Level, message: []const u8, fields: []const log.Field) std.Io.Writer.Error!void {
        var s: std.json.Stringify = .{ .writer = out };
        try s.beginObject();
        try s.objectField("level");
        try s.write(levelName(level));
        try s.objectField("msg");
        try s.write(message);
        for (fields, 0..) |f, i| {
            if (isKey(f.key) or seen(fields[0..i], f.key)) continue;
            try s.objectField(f.key);
            try s.write(f.value);
        }
        try s.endObject();
        try out.writeByte('\n');
        try out.flush();
    }

    fn isKey(key: []const u8) bool {
        return std.mem.eql(u8, key, "level") or std.mem.eql(u8, key, "msg");
    }

    fn seen(earlier: []const log.Field, key: []const u8) bool {
        for (earlier) |f| if (std.mem.eql(u8, f.key, key)) return true;
        return false;
    }

    fn levelName(level: log.Level) []const u8 {
        return switch (level) {
            .debug => "DEBUG",
            .info => "INFO",
            .warn => "WARN",
            .err => "ERROR",
        };
    }
};

/// Logs from the workflow, then runs the two logging activities. The workflow logger is
/// silent while the workflow replays history, so each message is logged once.
fn logWorkflow(ctx: *workflow.Context, name: []const u8) !void {
    const options: workflow.ActivityOptions = .{ .start_to_close_timeout = .fromSeconds(10) };
    const logger = ctx.logger();
    logger.info("Logging from workflow", .{ .name = name });

    ctx.executeActivity(LoggingActivity, options, .{name}).get() catch |err| {
        logger.err("LoggingActivity failed.", .{ .Error = err });
        return err;
    };
    ctx.executeActivity(LoggingErrorActivity, options, .{}).get() catch |err| {
        logger.err("LoggingActivity failed.", .{ .Error = err });
        return err;
    };

    logger.info("Workflow completed.", .{});
}

pub const Workflow = workflow.Def("Workflow", logWorkflow);

fn loggingActivity(ctx: *activity.Context, name: []const u8) !void {
    // `with` adds a field to every record of the returned logger, after the activity's own
    // context fields.
    const logger = try ctx.logger().with(ctx.allocator(), .{ .activity = "LoggingActivity" });
    logger.info("Executing LoggingActivity.", .{ .name = name });
    logger.debug("Debugging LoggingActivity.", .{ .value = "important debug data" });
}

pub const LoggingActivity = activity.Def("LoggingActivity", loggingActivity);

fn loggingErrorActivity(ctx: *activity.Context) !void {
    const logger = ctx.logger();
    logger.warn("Ignore next error message. It is just for demo purpose.", .{});
    logger.err("Unable to execute LoggingErrorActivity.", .{ .@"error" = "random error" });
}

pub const LoggingErrorActivity = activity.Def("LoggingErrorActivity", loggingErrorActivity);
