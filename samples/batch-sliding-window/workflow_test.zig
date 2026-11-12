//! Tests for the batch-sliding-window sample. The workflows run on the time-skipping server, so
//! the record processors' random delays pass at once.

const std = @import("std");
const temporal = @import("temporal");
const sample = @import("batch_sliding_window.zig");

const testing = std.testing;

fn registerAll(env: *temporal.testing.WorkflowEnvironment, loader: *sample.RecordLoader) !void {
    try env.registerWorkflow(sample.ProcessBatchWorkflow);
    try env.registerWorkflow(sample.SlidingWindowWorkflow);
    try env.registerWorkflow(sample.RecordProcessorWorkflow);
    try env.registerActivitiesWith(loader, .{ sample.GetRecordCount, sample.GetRecords });
}

/// The runs of `workflow_id`, latest first, each with its started event's input; each run is
/// replayed on the way. Memory comes from `arena`.
fn runInputs(env: *temporal.testing.WorkflowEnvironment, arena: std.mem.Allocator, workflow_id: []const u8) ![]sample.SlidingWindowWorkflowInput {
    var inputs: std.ArrayList(sample.SlidingWindowWorkflowInput) = .empty;
    var run_id: []const u8 = "";
    while (true) {
        // An empty run ID reads the latest run.
        const events = (try env.client.fetchHistory(arena, workflow_id, run_id)).events.items;
        const last = events[events.len - 1].attributes.?;
        if (inputs.items.len == 0) {
            try testing.expect(last == .workflow_execution_completed_event_attributes);
        } else {
            try testing.expect(last == .workflow_execution_continued_as_new_event_attributes);
        }
        const started = events[0].attributes.?.workflow_execution_started_event_attributes;
        const payload = started.input.?.payloads.items[0];
        try inputs.append(arena, try temporal.converter.fromPayload(sample.SlidingWindowWorkflowInput, arena, payload));
        try env.replayRun(workflow_id, run_id);
        if (started.continued_execution_run_id.len == 0) break;
        run_id = started.continued_execution_run_id;
    }
    return inputs.items;
}

test "batch processes every record, each partition continuing as new after each page" {
    const env = try temporal.testing.WorkflowEnvironment.startTimeSkipping(testing.allocator, testing.io);
    defer env.deinit();
    var loader: sample.RecordLoader = .{ .record_count = 23 };
    try registerAll(env, &loader);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var run = try env.startWorkflow(sample.ProcessBatchWorkflow, .{ .task_queue = "" }, .{.{ .page_size = 5, .sliding_window_size = 7, .partitions = 3 }});
    defer run.deinit();
    try testing.expectEqual(23, try env.getResult(sample.ProcessBatchWorkflow, &run, a, null));

    // Partitions of 8, 8 and 7 records with windows of 3, 2 and 2: one run per page of 5,
    // then a last run that waits for the remaining children.
    const expected_offsets = [_][]const u32{ &.{ 0, 5, 8 }, &.{ 8, 13, 16 }, &.{ 16, 21, 23 } };
    const expected_windows = [_]u32{ 3, 2, 2 };
    for (expected_offsets, expected_windows, 0..) |offsets, window, i| {
        const id = try std.fmt.allocPrint(a, "{s}/{d}", .{ run.id, i });
        const inputs = try runInputs(env, a, id);
        try testing.expectEqual(offsets.len, inputs.len);
        for (offsets, 0..) |offset, run_index| {
            const input = inputs[inputs.len - 1 - run_index];
            try testing.expectEqual(offset, input.offset);
            try testing.expectEqual(offsets[offsets.len - 1], input.maximum_offset);
            try testing.expectEqual(window, input.sliding_window_size);
            // Every record before this run's offset is either done or still in the window.
            try testing.expectEqual(offset - offsets[0], input.progress + input.current_records.len);
            try testing.expect(input.current_records.len <= window);
        }
    }
}

test "continue-as-new carries the window's records and progress, and a completion counts once" {
    const env = try temporal.testing.WorkflowEnvironment.startTimeSkipping(testing.allocator, testing.io);
    defer env.deinit();
    var loader: sample.RecordLoader = .{ .record_count = 2 };
    try registerAll(env, &loader);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // As if an earlier run had finished 4 records and left record 100's child running.
    var run = try env.startWorkflow(sample.SlidingWindowWorkflow, .{ .task_queue = "" }, .{.{
        .page_size = 5,
        .sliding_window_size = 3,
        .offset = 0,
        .maximum_offset = 2,
        .progress = 4,
        .current_records = &.{100},
    }});
    defer run.deinit();
    // Record 100 has no child here: the test reports it, twice.
    try env.signalWorkflow(sample.ReportCompletion, run.id, .{100});
    try env.signalWorkflow(sample.ReportCompletion, run.id, .{100});
    try testing.expectEqual(7, try env.getResult(sample.SlidingWindowWorkflow, &run, a, null));

    const inputs = try runInputs(env, a, run.id);
    try testing.expectEqual(2, inputs.len);
    const next = inputs[0];
    try testing.expectEqual(2, next.offset);
    try testing.expectEqual(7, next.progress + next.current_records.len);
    for (next.current_records) |id| try testing.expect(id == 0 or id == 1 or id == 100);
}

test "state query shows a full window waiting for room" {
    const env = try temporal.testing.WorkflowEnvironment.startTimeSkipping(testing.allocator, testing.io);
    defer env.deinit();
    var loader: sample.RecordLoader = .{ .record_count = 5 };
    try registerAll(env, &loader);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var run = try env.startWorkflow(sample.SlidingWindowWorkflow, .{ .task_queue = "" }, .{.{
        .page_size = 5,
        .sliding_window_size = 2,
        .offset = 0,
        .maximum_offset = 5,
    }});
    defer run.deinit();

    // Time stays locked until getResult, so no child's delay passes and the window stays full
    // once two children have started.
    const state = while (true) {
        const s = try env.queryWorkflow(sample.State, a, run.id, .{});
        if (s.children_started_by_this_run == 2) break s;
    };
    try testing.expectEqualSlices(u32, &.{ 0, 1 }, state.current_records);
    try testing.expectEqual(0, state.offset);
    try testing.expectEqual(0, state.progress);

    try testing.expectEqual(5, try env.getResult(sample.SlidingWindowWorkflow, &run, a, null));
}

test "record processor without a parent completes without reporting" {
    const env = try temporal.testing.WorkflowEnvironment.startTimeSkipping(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(sample.RecordProcessorWorkflow);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    try env.executeWorkflow(sample.RecordProcessorWorkflow, .{.{ .id = 7 }}, arena.allocator(), null);
}

test "batch rejects a window smaller than the number of partitions" {
    const env = try temporal.testing.WorkflowEnvironment.startTimeSkipping(testing.allocator, testing.io);
    defer env.deinit();
    var loader: sample.RecordLoader = .{ .record_count = 10 };
    try registerAll(env, &loader);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var failure: ?temporal.failure.Failure = null;
    try testing.expectError(error.WorkflowFailed, env.executeWorkflow(sample.ProcessBatchWorkflow, .{.{ .page_size = 5, .sliding_window_size = 2, .partitions = 3 }}, arena.allocator(), &failure));
    try testing.expectEqualStrings("sliding_window_size 2 cannot be less than the number of partitions 3, which must be at least 1", failure.?.message);
    try testing.expectEqualStrings("invalidInput", failure.?.failure_info.?.application_failure_info.type);
}

test "record loader pages through its records up to the maximum offset" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var loader: sample.RecordLoader = .{ .record_count = 10 };
    const env: temporal.testing.ActivityEnvironment = .init(testing.io);

    try testing.expectEqual(10, try env.executeActivityWith(sample.GetRecordCount, &loader, .{}, a));

    const page = try env.executeActivityWith(sample.GetRecords, &loader, .{.{ .page_size = 5, .offset = 4, .max_offset = 7 }}, a);
    try testing.expectEqualSlices(sample.SingleRecord, &.{ .{ .id = 4 }, .{ .id = 5 }, .{ .id = 6 } }, page.records);

    const empty = try env.executeActivityWith(sample.GetRecords, &loader, .{.{ .page_size = 5, .offset = 7, .max_offset = 7 }}, a);
    try testing.expectEqual(0, empty.records.len);

    try testing.expectError(error.ApplicationFailure, env.executeActivityWith(sample.GetRecords, &loader, .{.{ .page_size = 5, .offset = 0, .max_offset = 11 }}, a));
}

test "partitions differ by at most one, larger first" {
    const parts = try sample.divideIntoPartitions(testing.allocator, 23, 3);
    defer testing.allocator.free(parts);
    try testing.expectEqualSlices(u32, &.{ 8, 8, 7 }, parts);

    const windows = try sample.divideIntoPartitions(testing.allocator, 7, 3);
    defer testing.allocator.free(windows);
    try testing.expectEqualSlices(u32, &.{ 3, 2, 2 }, windows);
}
