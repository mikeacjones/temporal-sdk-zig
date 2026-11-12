//! Tests for the standalone activity sample: the activity on its own, and the real worker
//! running it for a client against a dev server with standalone activities enabled.

const std = @import("std");
const temporal = @import("temporal");
const helloworld = @import("helloworld.zig");

const testing = std.testing;

test "activity" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const env: temporal.testing.ActivityEnvironment = .init(testing.io);
    const result = try env.executeActivity(helloworld.Activity, .{"World"}, arena.allocator());
    try testing.expectEqualStrings("Hello World!", result);
}

fn runWorker(w: *temporal.worker.Worker) temporal.worker.Worker.RunError!void {
    return w.run();
}

test "standalone activity using a dev server" {
    var server = try temporal.testing.DevServer.start(testing.allocator, testing.io, .{
        .extra_args = &temporal.testing.DevServer.standalone_activity_args,
    });
    defer server.stop(testing.io);

    var client = try temporal.client.Client.connect(testing.allocator, testing.io, server.clientOptions());
    defer client.close();

    const task_queue = "standalone-activity-test";
    var w = try temporal.worker.Worker.init(testing.allocator, testing.io, &client, task_queue, .{});
    defer w.deinit();
    try w.registerActivity(helloworld.Activity);
    var running = try testing.io.concurrent(runWorker, .{&w});
    defer {
        w.stop();
        running.await(testing.io) catch {};
    }

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var handle = try client.executeActivity(helloworld.Activity, .{
        .id = "test-activity-id",
        .task_queue = task_queue,
        .schedule_to_close_timeout = .fromSeconds(10),
    }, .{"Temporal"});
    defer handle.deinit();
    try testing.expectEqualStrings("test-activity-id", handle.id);
    try testing.expect(handle.run_id.len > 0);
    try testing.expectEqualStrings("Hello Temporal!", try handle.get(a));

    // Visibility is eventually consistent: poll until the run is counted as completed.
    const query = "TaskQueue = '" ++ task_queue ++ "'";
    var polls: usize = 0;
    while (try client.countActivities(query ++ " AND ExecutionStatus = 'Completed'") != 1) : (polls += 1) {
        if (polls == 300) return error.ActivityNeverCounted;
        try testing.io.sleep(.fromMilliseconds(100), .awake);
    }
    var it = try client.listActivities(query, .{});
    defer it.deinit();
    const info = (try it.next(a)) orelse return error.ActivityNotListed;
    try testing.expectEqualStrings("test-activity-id", info.activity_id);
    try testing.expectEqualStrings(handle.run_id, info.run_id);
    try testing.expectEqualStrings("Activity", info.activity_type);
    try testing.expectEqual(temporal.client.ActivityExecutionStatus.completed, info.status);
    try testing.expectEqual(null, try it.next(a));
}
