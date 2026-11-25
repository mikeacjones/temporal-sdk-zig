//! Tests of the greeting activity on its own, and of the greet operation run from client code
//! against a dev server of its own, where the handler's standalone activity completes it.

const std = @import("std");
const temporal = @import("temporal");
const service = @import("service.zig");
const handler = @import("handler.zig");
const caller = @import("caller.zig");

const testing = std.testing;
const DevServer = temporal.testing.DevServer;

test "the greeting activity greets by name" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var env: temporal.testing.ActivityEnvironment = .init(testing.io);
    const output = try env.executeActivity(handler.CreateGreetingActivity, .{.{ .name = "Test" }}, arena.allocator());
    try testing.expectEqualStrings("Hello, Test!", output.message);
}

test "the greeting activity ID is greeting-<name>" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    try testing.expectEqualStrings("greeting-Test", try service.greetingActivityId(arena.allocator(), .{ .name = "Test" }));
}

fn runWorker(w: *temporal.worker.Worker) temporal.worker.Worker.RunError!void {
    return w.run();
}

test "a standalone activity completes the greet operation using a dev server" {
    var server = try DevServer.start(testing.allocator, testing.io, .{
        .extra_args = &(DevServer.standalone_activity_args ++ DevServer.activity_callback_args),
    });
    defer server.stop(testing.io);

    var client = try temporal.client.Client.connect(testing.allocator, testing.io, server.clientOptions());
    defer client.close();

    const task_queue = "nexus-standalone-activity-test";
    const endpoint = "nexus-standalone-activity-test-endpoint";
    try temporal.testing.createNexusEndpoint(&client, endpoint, .{ .namespace = client.namespace, .task_queue = task_queue });

    var w = try temporal.worker.Worker.init(testing.allocator, testing.io, &client, task_queue, .{});
    defer w.deinit();
    try handler.register(&w);
    var running = try testing.io.concurrent(runWorker, .{&w});
    defer {
        w.stop();
        running.await(testing.io) catch {};
    }

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const operation_id = try std.fmt.allocPrint(a, "greeting-{s}", .{try temporal.client.uuid(a, testing.io)});
    const operation = try caller.startGreet(&client, a, testing.io, endpoint, operation_id, .{ .name = "Test" }, .fromSeconds(10));
    try testing.expectEqualStrings(operation_id, operation.id);
    try testing.expect(operation.run_id.len > 0);

    const output = try caller.greetResult(&client, a, operation);
    try testing.expectEqualStrings("Hello, Test!", output.message);
}
