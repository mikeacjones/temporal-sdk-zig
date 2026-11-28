//! Metrics against a dev server: a workflow and an activity record through the client's
//! Prometheus exporter and through a custom handler, alongside sdk-core's own metrics, and a
//! workflow replaying history records nothing again.

const std = @import("std");
const activity = @import("activity.zig");
const workflow = @import("workflow.zig");
const client_mod = @import("client.zig");
const worker_mod = @import("worker.zig");
const metrics = @import("metrics.zig");
const bridge = @import("bridge.zig");
const testing_mod = @import("testing.zig");
const Io = std.Io;

const testing = std.testing;

const Fixtures = struct {
    fn work(ctx: *activity.Context, n: u64) !u64 {
        const m = ctx.metrics();
        const calls = try m.counter("zig_activity_calls", .{ .description = "Activity calls" });
        defer calls.deinit();
        calls.add(1, .{ .outcome = "ok" });
        const latency = try m.histogramDuration("zig_activity_latency", .{});
        defer latency.deinit();
        latency.record(.fromMilliseconds(30), .{});
        return n * 2;
    }
    const Work = activity.Def("MetricsWork", work);

    fn flow(ctx: *workflow.Context, n: u64) !u64 {
        const m = ctx.metrics();
        const runs = try m.counter("zig_workflow_runs", .{ .description = "Workflow runs" });
        defer runs.deinit();
        runs.add(1, .{ .branch = "main" });
        const items = try m.histogram("zig_workflow_items", .{});
        defer items.deinit();
        items.record(n, .{});
        // A second workflow task: with the worker's cache off, it replays the code above.
        return ctx.executeActivity(Work, .{ .start_to_close_timeout = .fromSeconds(10) }, .{n}).get();
    }
    const Flow = workflow.Def("MetricsWorkflow", flow);

    fn runWorker(w: *worker_mod.Worker) worker_mod.Worker.RunError!void {
        return w.run();
    }
};

/// Runs `Fixtures.Flow` to completion on a worker with workflow caching off, so its second
/// workflow task replays the first.
fn runFlow(client: *client_mod.Client, task_queue: []const u8) !void {
    var w = try worker_mod.Worker.init(testing.allocator, testing.io, client, task_queue, .{ .max_cached_workflows = 0 });
    defer w.deinit();
    try w.registerWorkflow(Fixtures.Flow);
    try w.registerActivity(Fixtures.Work);
    var running = try testing.io.concurrent(Fixtures.runWorker, .{&w});
    defer {
        w.stop();
        running.await(testing.io) catch {};
    }
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var run = try client.executeWorkflow(Fixtures.Flow, .{ .task_queue = task_queue }, .{5});
    defer run.deinit();
    try testing.expectEqual(10, try run.get(arena.allocator()));
}

fn scrape(a: std.mem.Allocator, url: []const u8) ![]const u8 {
    var http: std.http.Client = .{ .allocator = testing.allocator, .io = testing.io };
    defer http.deinit();
    var body: Io.Writer.Allocating = .init(a);
    const result = try http.fetch(.{ .location = .{ .url = url }, .response_writer = &body.writer });
    try testing.expectEqual(std.http.Status.ok, result.status);
    return body.written();
}

/// The value of the first sample of `name` whose labels include each of `labels` (written
/// `key="value"`), or null.
fn sample(body: []const u8, name: []const u8, labels: []const []const u8) ?[]const u8 {
    var lines = std.mem.splitScalar(u8, body, '\n');
    next: while (lines.next()) |line| {
        if (!std.mem.startsWith(u8, line, name) or line.len <= name.len or line[name.len] != '{') continue;
        const close = std.mem.lastIndexOfScalar(u8, line, '}') orelse continue;
        for (labels) |l| if (std.mem.indexOf(u8, line[name.len..close], l) == null) continue :next;
        return std.mem.trim(u8, line[close + 1 ..], " ");
    }
    return null;
}

test "workflow, activity, and core metrics are scraped from the Prometheus exporter" {
    var server = try testing_mod.DevServer.start(testing.allocator, testing.io, .{});
    defer server.stop(testing.io);
    // The core's Prometheus exporter doesn't report the port it bound, so it is given one that
    // is reserved until it binds it (see reserveLoopbackPort).
    const port = try bridge.EphemeralServer.reserveLoopbackPort();
    var addr_buf: [32]u8 = undefined;
    const bind_address = try std.fmt.bufPrint(&addr_buf, "127.0.0.1:{d}", .{port});
    var options = server.clientOptions();
    options.metrics = .{
        .exporter = .{ .prometheus = .{ .bind_address = bind_address } },
        .global_tags = &.{.{ .key = "deployment", .value = "test" }},
    };
    var client = try client_mod.Client.connect(testing.allocator, testing.io, options);
    defer client.close();

    const from_client = try client.metrics().gauge("zig_client_gauge", .{});
    defer from_client.deinit();
    from_client.set(7, .{ .source = "client" });

    try runFlow(&client, "metrics-prometheus");

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var url_buf: [64]u8 = undefined;
    const url = try std.fmt.bufPrint(&url_buf, "http://{s}/metrics", .{bind_address});
    const body = try scrape(arena.allocator(), url);

    const wf_labels: []const []const u8 = &.{ "namespace=\"default\"", "task_queue=\"metrics-prometheus\"", "workflow_type=\"MetricsWorkflow\"", "service_name=\"temporal-core-sdk\"", "deployment=\"test\"" };
    // The workflow ran its first task twice (the second replayed it), but recorded once.
    try testing.expectEqualStrings("1", sample(body, "zig_workflow_runs", wf_labels ++ &[_][]const u8{"branch=\"main\""}) orelse return error.MissingMetric);
    try testing.expectEqualStrings("5", sample(body, "zig_workflow_items_sum", wf_labels) orelse return error.MissingMetric);
    try testing.expectEqualStrings("1", sample(body, "zig_workflow_items_count", wf_labels) orelse return error.MissingMetric);
    try testing.expect(sample(body, "temporal_workflow_task_replay_latency_count", wf_labels) != null);

    const act_labels: []const []const u8 = &.{ "activity_type=\"MetricsWork\"", "workflow_type=\"MetricsWorkflow\"", "task_queue=\"metrics-prometheus\"" };
    try testing.expectEqualStrings("1", sample(body, "zig_activity_calls", act_labels ++ &[_][]const u8{"outcome=\"ok\""}) orelse return error.MissingMetric);
    try testing.expectEqualStrings("30", sample(body, "zig_activity_latency_sum", act_labels) orelse return error.MissingMetric);

    try testing.expectEqualStrings("7", sample(body, "zig_client_gauge", &.{ "source=\"client\"", "service_name=\"temporal-core-sdk\"" }) orelse return error.MissingMetric);

    // sdk-core's own metrics, with the default prefix.
    try testing.expectEqualStrings("1", sample(body, "temporal_workflow_completed", wf_labels) orelse return error.MissingMetric);
    try testing.expectEqualStrings("1", sample(body, "temporal_activity_execution_latency_count", act_labels) orelse return error.MissingMetric);
}

test "a custom handler receives sdk-core's metrics and the workflow's and activity's" {
    var recorder: bridge.metric.TestRecorder = .init(testing.allocator);
    defer recorder.deinit();
    var server = try testing_mod.DevServer.start(testing.allocator, testing.io, .{});
    defer server.stop(testing.io);
    {
        var options = server.clientOptions();
        options.metrics = .{
            .exporter = .{ .custom = .{ .ptr = &recorder, .vtable = &bridge.metric.TestRecorder.vtable } },
            .metric_prefix = "zigsdk_",
        };
        var client = try client_mod.Client.connect(testing.allocator, testing.io, options);
        defer client.close();
        try runFlow(&client, "metrics-custom");
    }

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const runs = try recorder.recordsOf(a, "zig_workflow_runs");
    try testing.expectEqual(1, runs.len);
    try testing.expectEqual(metrics.Value{ .unsigned = 1 }, runs[0].value);
    try testing.expectEqualStrings("MetricsWorkflow", runs[0].get("workflow_type").?.string);
    try testing.expectEqualStrings("metrics-custom", runs[0].get("task_queue").?.string);
    try testing.expectEqualStrings("main", runs[0].get("branch").?.string);
    try testing.expectEqualStrings("Workflow runs", runs[0].metric.definition.description);

    const items = try recorder.recordsOf(a, "zig_workflow_items");
    try testing.expectEqual(1, items.len);
    try testing.expectEqual(metrics.Kind.histogram, items[0].metric.definition.kind);

    const calls = try recorder.recordsOf(a, "zig_activity_calls");
    try testing.expectEqual(1, calls.len);
    try testing.expectEqualStrings("MetricsWork", calls[0].get("activity_type").?.string);
    try testing.expectEqualStrings("ok", calls[0].get("outcome").?.string);
    const latency = try recorder.recordsOf(a, "zig_activity_latency");
    try testing.expectEqual(30, bridge.metric.durationMs(latency[0].value.duration));

    // sdk-core's metrics carry the configured prefix; user metrics keep their names.
    const completed = try recorder.recordsOf(a, "zigsdk_workflow_completed");
    try testing.expectEqual(1, completed.len);
    try testing.expectEqualStrings("MetricsWorkflow", completed[0].get("workflow_type").?.string);
    try testing.expect(recorder.hasMetric("zigsdk_activity_execution_latency"));
    // Closing the client freed everything the core created.
    try testing.expectEqual(0, recorder.live_metrics);
    try testing.expectEqual(0, recorder.live_attributes);
}

test "an activity environment hands its activity the configured handler" {
    var recorder: testing_mod.MetricsRecorder = .init(testing.allocator);
    defer recorder.deinit();
    var env: testing_mod.ActivityEnvironment = .init(testing.io);
    env.metrics = recorder.handler();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    try testing.expectEqual(4, try env.executeActivity(Fixtures.Work, .{2}, arena.allocator()));
    const calls = try recorder.recordsOf(arena.allocator(), "zig_activity_calls");
    try testing.expectEqual(1, calls.len);
    try testing.expectEqualStrings("ok", calls[0].get("outcome").?.string);
    try testing.expectEqualStrings("Activity calls", calls[0].metric.definition.description);
    try testing.expectEqual(metrics.Value{ .unsigned = 1 }, calls[0].value);
    try testing.expect(recorder.hasMetric("zig_activity_latency"));
    try testing.expect(!recorder.hasMetric("zig_workflow_runs"));
    try testing.expectEqual(0, (try recorder.recordsOf(arena.allocator(), "zig_workflow_runs")).len);
}

test "a workflow environment with a MetricsRecorder records the workflow's, activity's and sdk-core's metrics once" {
    var recorder: testing_mod.MetricsRecorder = .init(testing.allocator);
    defer recorder.deinit();
    {
        const env = try testing_mod.WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{
            .metrics = .{ .exporter = .{ .custom = recorder.handler() } },
        });
        defer env.deinit();
        try env.registerWorkflow(Fixtures.Flow);
        try env.registerActivity(Fixtures.Work);
        var arena: std.heap.ArenaAllocator = .init(testing.allocator);
        defer arena.deinit();
        try testing.expectEqual(10, try env.executeWorkflow(Fixtures.Flow, .{5}, arena.allocator(), null));
    }

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    // Once, though getResult replayed the run: a replaying workflow records nothing.
    const runs = try recorder.recordsOf(a, "zig_workflow_runs");
    try testing.expectEqual(1, runs.len);
    try testing.expectEqualStrings("MetricsWorkflow", runs[0].get("workflow_type").?.string);
    try testing.expectEqualStrings("main", runs[0].get("branch").?.string);
    const items = try recorder.recordsOf(a, "zig_workflow_items");
    try testing.expectEqual(1, items.len);
    try testing.expectEqual(metrics.Value{ .unsigned = 5 }, items[0].value);
    const calls = try recorder.recordsOf(a, "zig_activity_calls");
    try testing.expectEqual(1, calls.len);
    try testing.expectEqualStrings("MetricsWork", calls[0].get("activity_type").?.string);
    try testing.expect(recorder.hasMetric("temporal_workflow_completed"));
    try testing.expectEqual(1, (try recorder.recordsOf(a, "temporal_workflow_completed")).len);
}

/// Adds `n` to a counter 100 times through its own handler copy, tagged with `task = n`.
/// Sets `failed` when the counter cannot be created.
fn recordMany(h: metrics.Handler, n: u64, failed: *std.atomic.Value(bool)) void {
    const c = h.counter("zig_concurrent", .{}) catch return failed.store(true, .release);
    defer c.deinit();
    for (0..100) |_| c.add(n, .{ .task = n });
}

test "MetricsRecorder keeps every value recorded from several tasks at once" {
    var recorder: testing_mod.MetricsRecorder = .init(testing.allocator);
    defer recorder.deinit();
    var failed: std.atomic.Value(bool) = .init(false);
    var group: Io.Group = .init;
    defer group.cancel(testing.io);
    for (1..5) |n| try group.concurrent(testing.io, recordMany, .{ recorder.handler(), @as(u64, n), &failed });
    try group.await(testing.io);
    try testing.expect(!failed.load(.acquire));
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const records = try recorder.recordsOf(arena.allocator(), "zig_concurrent");
    try testing.expectEqual(400, records.len);
    var per_task: [5]usize = @splat(0);
    for (records) |rec| {
        const n = rec.value.unsigned;
        try testing.expectEqual(@as(i64, @intCast(n)), rec.get("task").?.int);
        per_task[n] += 1;
    }
    try testing.expectEqualSlices(usize, &.{ 0, 100, 100, 100, 100 }, &per_task);
}

test "MetricsRecorder frees everything recorded through both environments" {
    // On testing.allocator, whose leak check fails the test if `deinit` leaves anything. The
    // recorder is deinitialized below, after both environments are closed.
    var recorder: testing_mod.MetricsRecorder = .init(testing.allocator);
    {
        errdefer recorder.deinit();
        var arena: std.heap.ArenaAllocator = .init(testing.allocator);
        defer arena.deinit();
        var act: testing_mod.ActivityEnvironment = .init(testing.io);
        act.metrics = recorder.handler();
        try testing.expectEqual(4, try act.executeActivity(Fixtures.Work, .{2}, arena.allocator()));

        const env = try testing_mod.WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{
            .metrics = .{ .exporter = .{ .custom = recorder.handler() } },
        });
        defer env.deinit();
        try env.registerWorkflow(Fixtures.Flow);
        try env.registerActivity(Fixtures.Work);
        try testing.expectEqual(10, try env.executeWorkflow(Fixtures.Flow, .{5}, arena.allocator(), null));
    }
    {
        errdefer recorder.deinit();
        try testing.expect(recorder.hasMetric("zig_workflow_runs"));
        const calls = try recorder.recordsOf(testing.allocator, "zig_activity_calls");
        defer testing.allocator.free(calls);
        try testing.expectEqual(2, calls.len);
        // Closing the environment freed every handle the core held.
        try testing.expectEqual(0, recorder.recorder.live_metrics);
        try testing.expectEqual(0, recorder.recorder.live_attributes);
    }
    recorder.deinit();
}
