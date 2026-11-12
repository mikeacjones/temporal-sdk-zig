//! Tests for the log adapter sample: the JSON sink on its own, and the workflow and
//! activities logging through it on a dev server.

const std = @import("std");
const temporal = @import("temporal");
const logadapter = @import("logadapter.zig");

const testing = std.testing;
const json = std.json;

/// Parses each line of `text` as a JSON object. Memory comes from `arena`.
fn parseLines(arena: std.mem.Allocator, text: []const u8) ![]const json.ObjectMap {
    var records: std.ArrayList(json.ObjectMap) = .empty;
    var lines = std.mem.splitScalar(u8, std.mem.trimEnd(u8, text, "\n"), '\n');
    while (lines.next()) |line| {
        const value = try json.parseFromSliceLeaky(json.Value, arena, line, .{});
        try records.append(arena, value.object);
    }
    return records.items;
}

fn expectField(record: json.ObjectMap, key: []const u8, want: []const u8) !void {
    const value = record.get(key) orelse {
        std.debug.print("record has no field \"{s}\"\n", .{key});
        return error.TestExpectedEqual;
    };
    try testing.expectEqualStrings(want, value.string);
}

/// The records whose `msg` is `message`.
fn find(arena: std.mem.Allocator, records: []const json.ObjectMap, message: []const u8) ![]const json.ObjectMap {
    var found: std.ArrayList(json.ObjectMap) = .empty;
    for (records) |r| {
        const msg = r.get("msg") orelse continue;
        if (std.mem.eql(u8, msg.string, message)) try found.append(arena, r);
    }
    return found.items;
}

test "the sink writes one JSON object per record, with every field as a key" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var out: std.Io.Writer.Allocating = .init(testing.allocator);
    defer out.deinit();
    var sink: logadapter.JsonSink = .{ .io = testing.io, .out = &out.writer };

    const base: temporal.log.Logger = .{ .sink = sink.sink() };
    const logger = try base.with(arena.allocator(), .{ .WorkflowID = "wf-1", .activity = "LoggingActivity" });
    logger.info("Executing \"LoggingActivity\".", .{ .name = "<param to log>", .count = @as(u32, 3), .activity = "first wins" });
    logger.err("Unable to execute LoggingErrorActivity.", .{ .@"error" = error.RandomError });

    const records = try parseLines(arena.allocator(), out.written());
    try testing.expectEqual(2, records.len);

    const info = records[0];
    try testing.expectEqual(6, info.count());
    try expectField(info, "level", "INFO");
    try expectField(info, "msg", "Executing \"LoggingActivity\".");
    try expectField(info, "name", "<param to log>");
    try expectField(info, "count", "3");
    try expectField(info, "activity", "first wins");
    try expectField(info, "WorkflowID", "wf-1");

    try expectField(records[1], "level", "ERROR");
    try expectField(records[1], "error", "RandomError");
}

test "records less severe than the minimum level are dropped" {
    var out: std.Io.Writer.Allocating = .init(testing.allocator);
    defer out.deinit();
    var sink: logadapter.JsonSink = .{ .io = testing.io, .out = &out.writer, .min_level = .warn };
    const logger: temporal.log.Logger = .{ .sink = sink.sink() };

    logger.debug("dropped", .{});
    logger.info("dropped", .{});
    logger.warn("kept", .{});
    logger.err("kept", .{});

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const records = try parseLines(arena.allocator(), out.written());
    try testing.expectEqual(2, records.len);
    try expectField(records[0], "level", "WARN");
    try expectField(records[1], "level", "ERROR");
    try testing.expectEqual(0, (try find(arena.allocator(), records, "dropped")).len);
}

test "workflow and activity records reach the sink with their context fields" {
    var out: std.Io.Writer.Allocating = .init(testing.allocator);
    defer out.deinit();
    var sink: logadapter.JsonSink = .{ .io = testing.io, .out = &out.writer, .min_level = .debug };

    const id = "slog_logger_workflow_id";
    {
        const env = try temporal.testing.WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{ .logger = sink.sink() });
        defer env.deinit();
        try env.registerWorkflow(logadapter.Workflow);
        try env.registerActivity(logadapter.LoggingActivity);
        try env.registerActivity(logadapter.LoggingErrorActivity);

        var arena: std.heap.ArenaAllocator = .init(testing.allocator);
        defer arena.deinit();
        try env.executeWorkflowWithOptions(logadapter.Workflow, .{ .id = id, .task_queue = "" }, .{"<param to log>"}, arena.allocator(), null);
    }

    // The environment is closed, so nothing writes to `out` any more.
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const records = try parseLines(a, out.written());

    const started = try find(a, records, "Logging from workflow");
    try testing.expectEqual(1, started.len);
    try expectField(started[0], "level", "INFO");
    try expectField(started[0], "name", "<param to log>");
    try expectField(started[0], "WorkflowType", "Workflow");
    try expectField(started[0], "WorkflowID", id);
    const run_id = started[0].get("RunID").?.string;

    const executing = try find(a, records, "Executing LoggingActivity.");
    try testing.expectEqual(1, executing.len);
    try expectField(executing[0], "level", "INFO");
    try expectField(executing[0], "name", "<param to log>");
    try expectField(executing[0], "activity", "LoggingActivity");
    try expectField(executing[0], "ActivityType", "LoggingActivity");
    try expectField(executing[0], "Attempt", "1");
    try expectField(executing[0], "WorkflowType", "Workflow");
    try expectField(executing[0], "WorkflowID", id);
    try expectField(executing[0], "RunID", run_id);

    const debugging = try find(a, records, "Debugging LoggingActivity.");
    try testing.expectEqual(1, debugging.len);
    try expectField(debugging[0], "level", "DEBUG");
    try expectField(debugging[0], "value", "important debug data");
    try expectField(debugging[0], "activity", "LoggingActivity");

    const warning = try find(a, records, "Ignore next error message. It is just for demo purpose.");
    try testing.expectEqual(1, warning.len);
    try expectField(warning[0], "level", "WARN");
    try expectField(warning[0], "ActivityType", "LoggingErrorActivity");
    try testing.expect(warning[0].get("activity") == null);

    const failed = try find(a, records, "Unable to execute LoggingErrorActivity.");
    try testing.expectEqual(1, failed.len);
    try expectField(failed[0], "level", "ERROR");
    try expectField(failed[0], "error", "random error");
    try expectField(failed[0], "WorkflowID", id);

    // Logged once: the workflow logger is silent while the run is replayed.
    const completed = try find(a, records, "Workflow completed.");
    try testing.expectEqual(1, completed.len);
    try expectField(completed[0], "RunID", run_id);
}
