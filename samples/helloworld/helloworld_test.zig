//! Tests for the hello world sample: with a test double for the activity, the activity on
//! its own, and the real worker against a dev server.

const std = @import("std");
const temporal = @import("temporal");
const helloworld = @import("helloworld.zig");

const testing = std.testing;

fn activityDouble(ctx: *temporal.activity.Context, name: []const u8) ![]const u8 {
    try testing.expectEqualStrings("Temporal", name);
    return ctx.allocator().dupe(u8, "Hello Temporal!");
}

test "workflow with a test double for its activity" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(helloworld.HelloWorld);
    try env.registerActivityAs(helloworld.Greet, activityDouble);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const result = try env.executeWorkflow(helloworld.HelloWorld, .{"Temporal"}, arena.allocator(), null);
    try testing.expectEqualStrings("Hello Temporal!", result);
}

test "activity" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const env: temporal.testing.ActivityEnvironment = .init(testing.io);
    const result = try env.executeActivity(helloworld.Greet, .{"World"}, arena.allocator());
    try testing.expectEqualStrings("Hello World!", result);
}

fn runWorker(w: *temporal.worker.Worker) temporal.worker.Worker.RunError!void {
    return w.run();
}

test "worker against a dev server" {
    var server = try temporal.testing.DevServer.start(testing.allocator, testing.io, .{});
    defer server.stop(testing.io);

    var client = try temporal.client.Client.connect(testing.allocator, testing.io, server.clientOptions());
    defer client.close();

    var w = try temporal.worker.Worker.init(testing.allocator, testing.io, &client, "hello-world", .{});
    defer w.deinit();
    try w.registerWorkflow(helloworld.HelloWorld);
    try w.registerActivity(helloworld.Greet);
    var running = try testing.io.concurrent(runWorker, .{&w});
    defer {
        w.stop();
        running.await(testing.io) catch {};
    }

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var run = try client.executeWorkflow(helloworld.HelloWorld, .{ .id = "hello-world", .task_queue = "hello-world" }, .{"Temporal"});
    defer run.deinit();
    try testing.expectEqualStrings("Hello Temporal!", try run.get(arena.allocator()));
}
