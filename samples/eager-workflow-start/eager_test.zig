//! Tests for the eager workflow start sample against a dev server.

const std = @import("std");
const temporal = @import("temporal");
const helloworld = @import("helloworld");

const testing = std.testing;

fn runWorker(w: *temporal.worker.Worker) temporal.worker.Worker.RunError!void {
    return w.run();
}

test "a start on the worker's own client is eager, and one on another client is not" {
    var server = try temporal.testing.DevServer.start(testing.allocator, testing.io, .{});
    defer server.stop(testing.io);

    var client = try temporal.client.Client.connect(testing.allocator, testing.io, server.clientOptions());
    defer client.close();

    var w = try temporal.worker.Worker.init(testing.allocator, testing.io, &client, "eager_wf_start", .{});
    defer w.deinit();
    try w.registerWorkflow(helloworld.HelloWorld);
    try w.registerActivity(helloworld.Greet);
    var running = try testing.io.concurrent(runWorker, .{&w});
    defer {
        w.stop();
        running.await(testing.io) catch {};
    }
    try w.waitUntilRunning();

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var eager = try client.executeWorkflow(helloworld.HelloWorld, .{
        .id = "eager_wf",
        .task_queue = "eager_wf_start",
        .enable_eager_start = true,
    }, .{"Temporal"});
    defer eager.deinit();
    try testing.expect(eager.eagerly_started);
    try testing.expectEqualStrings("Hello Temporal!", try eager.get(a));
    // Had the worker rejected the eager task, it would have timed out and been polled again.
    const history = try client.fetchHistory(a, eager.id, eager.run_id);
    for (history.events.items) |event| try testing.expect(event.attributes.? != .workflow_task_timed_out_event_attributes);

    // A client with no worker of its own starts the workflow normally, and the worker polls
    // for its first task.
    var other = try temporal.client.Client.connect(testing.allocator, testing.io, server.clientOptions());
    defer other.close();
    var polled = try other.executeWorkflow(helloworld.HelloWorld, .{
        .id = "polled_wf",
        .task_queue = "eager_wf_start",
        .enable_eager_start = true,
    }, .{"Temporal"});
    defer polled.deinit();
    try testing.expect(!polled.eagerly_started);
    try testing.expectEqualStrings("Hello Temporal!", try polled.get(a));
}
