//! Tests for the metrics sample. A `temporal.testing.MetricsRecorder` keeps every value
//! recorded through its handler, so the tests read back what the activity recorded.

const std = @import("std");
const temporal = @import("temporal");
const sample = @import("metrics.zig");
const metrics = temporal.metrics;

const testing = std.testing;

const Recorder = temporal.testing.MetricsRecorder;

fn expectOne(records: []const Recorder.Record) !Recorder.Record {
    try testing.expectEqual(1, records.len);
    try testing.expectEqualStrings("Activity", records[0].get("operation").?.string);
    return records[0];
}

/// Checks what one successful run of the activity recorded: one start and one success, a
/// run time of at least its one-second sleep, and a wait of at least `min_wait_ms`.
fn expectActivityMetrics(recorder: *Recorder, min_wait_ms: i64) !void {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const started = try expectOne(try recorder.recordsOf(a, sample.activity_started_count));
    try testing.expectEqual(metrics.Value{ .unsigned = 1 }, started.value);
    const succeeded = try expectOne(try recorder.recordsOf(a, sample.activity_success_count));
    try testing.expectEqual(metrics.Value{ .unsigned = 1 }, succeeded.value);
    try testing.expectEqual(0, (try recorder.recordsOf(a, sample.activity_failed_count)).len);

    const latency = try expectOne(try recorder.recordsOf(a, sample.activity_latency));
    try testing.expect(latency.value.duration.toMilliseconds() >= 1000);
    const wait = try expectOne(try recorder.recordsOf(a, sample.schedule_to_start_latency));
    try testing.expect(wait.value.duration.toMilliseconds() >= min_wait_ms);
}

test "the workflow's activity records its metrics through the client's handler" {
    var recorder: Recorder = .init(testing.allocator);
    defer recorder.deinit();
    {
        const env = try temporal.testing.WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{
            .metrics = .{ .exporter = .{ .custom = recorder.handler() } },
        });
        defer env.deinit();
        try env.registerWorkflow(sample.Workflow);
        try env.registerActivity(sample.Activity);

        var arena: std.heap.ArenaAllocator = .init(testing.allocator);
        defer arena.deinit();
        try env.executeWorkflow(sample.Workflow, .{}, arena.allocator(), null);
    }
    // The wait is measured from before the workflow's 500ms sleep.
    try expectActivityMetrics(&recorder, 500);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const wait = try recorder.recordsOf(arena.allocator(), sample.schedule_to_start_latency);
    try testing.expectEqualStrings("Workflow", wait[0].get("workflow_type").?.string);
}

test "the activity records its metrics through the context's handler" {
    var recorder: Recorder = .init(testing.allocator);
    defer recorder.deinit();
    var env: temporal.testing.ActivityEnvironment = .init(testing.io);
    env.metrics = recorder.handler();

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const scheduled: std.Io.Timestamp = .now(testing.io, .real);
    try env.executeActivity(sample.Activity, .{@intCast(scheduled.nanoseconds)}, arena.allocator());
    try expectActivityMetrics(&recorder, 0);
}
