//! Tests for the shoppingcart sample. The first two run on the time-skipping server, sending
//! every update and the checkout signal before waiting for the result, so time never skips
//! while the cart is waiting for them.

const std = @import("std");
const temporal = @import("temporal");
const shoppingcart = @import("workflow.zig");

const testing = std.testing;
const WorkflowEnvironment = temporal.testing.WorkflowEnvironment;

/// Checks the cart's item IDs, in order.
fn expectItems(want: []const []const u8, cart: shoppingcart.CartState) !void {
    const got = cart.items.map.keys();
    try testing.expectEqual(want.len, got.len);
    for (want, got) |w, g| try testing.expectEqualStrings(w, g);
}

test "the cart adds and removes items, rejects an unknown action, and completes on checkout" {
    const env = try WorkflowEnvironment.startTimeSkipping(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(shoppingcart.CartWorkflow);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var handle = try env.updateWithStartWorkflow(shoppingcart.CartWorkflow, .{
        .task_queue = "",
        .id_conflict_policy = .use_existing,
    }, .{null}, shoppingcart.UpdateCart, .{ "add", "apple" }, .{});
    defer handle.deinit();
    var cart = try handle.get(a);
    try testing.expectEqual(@as(?i64, 1), cart.items.map.get("apple"));

    cart = try env.updateWorkflow(shoppingcart.UpdateCart, a, handle.workflow_id, .{ "remove", "apple" }, null);
    try testing.expect(!cart.items.map.contains("apple"));

    var failure: ?temporal.Failure = null;
    try testing.expectError(error.UpdateFailed, env.updateWorkflow(shoppingcart.UpdateCart, a, handle.workflow_id, .{ "invalid", "apple" }, &failure));
    try testing.expectEqualStrings("UnsupportedActionType", failure.?.message);

    try env.signalWorkflow(shoppingcart.Checkout, handle.workflow_id, .{});
    var run = try env.client.getWorkflow(void, handle.workflow_id, handle.run_id);
    defer run.deinit();
    try env.getResult(shoppingcart.CartWorkflow, &run, a, null);
}

test "update-with-start reuses the session's cart, and add and remove need an item" {
    const env = try WorkflowEnvironment.startTimeSkipping(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(shoppingcart.CartWorkflow);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const start: temporal.client.StartWorkflowOptions = .{ .id = "session-1", .task_queue = "", .id_conflict_policy = .use_existing };
    var first = try env.updateWithStartWorkflow(shoppingcart.CartWorkflow, start, .{null}, shoppingcart.UpdateCart, .{ "add", "apple" }, .{});
    defer first.deinit();
    _ = try first.get(a);
    var second = try env.updateWithStartWorkflow(shoppingcart.CartWorkflow, start, .{null}, shoppingcart.UpdateCart, .{ "add", "banana" }, .{});
    defer second.deinit();
    const cart = try second.get(a);
    try testing.expectEqualStrings(first.run_id, second.run_id);
    try expectItems(&.{ "apple", "banana" }, cart);

    var failure: ?temporal.Failure = null;
    try testing.expectError(error.UpdateFailed, env.updateWorkflow(shoppingcart.UpdateCart, a, "session-1", .{ "remove", "" }, &failure));
    try testing.expectEqualStrings("ItemIdRequired", failure.?.message);

    const listed = try env.updateWorkflow(shoppingcart.UpdateCart, a, "session-1", .{ "list", "" }, null);
    try expectItems(&.{ "apple", "banana" }, listed);

    try env.signalWorkflow(shoppingcart.Checkout, "session-1", .{});
    var run = try env.client.getWorkflow(void, "session-1", first.run_id);
    defer run.deinit();
    try env.getResult(shoppingcart.CartWorkflow, &run, a, null);
}

fn runWorker(w: *temporal.worker.Worker) temporal.worker.Worker.RunError!void {
    return w.run();
}

test "the cart continues as new when suggested and keeps its items" {
    // A low history threshold makes the server suggest continuing as new after a few updates.
    var server = try temporal.testing.DevServer.start(testing.allocator, testing.io, .{
        .extra_args = &.{ "--dynamic-config-value", "limit.historyCount.suggestContinueAsNew=20" },
    });
    defer server.stop(testing.io);
    var client = try temporal.client.Client.connect(testing.allocator, testing.io, server.clientOptions());
    defer client.close();

    var w = try temporal.worker.Worker.init(testing.allocator, testing.io, &client, shoppingcart.task_queue, .{});
    defer w.deinit();
    try w.registerWorkflow(shoppingcart.CartWorkflow);
    var running = try testing.io.concurrent(runWorker, .{&w});
    defer {
        w.stop();
        running.await(testing.io) catch {};
    }

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const start: temporal.client.StartWorkflowOptions = .{ .id = "session-can", .task_queue = shoppingcart.task_queue, .id_conflict_policy = .use_existing };
    var first_run_id: []const u8 = "";
    var continued = false;
    var adds: i64 = 0;
    // Each update adds about five events, so 20 adds cross the threshold several times over.
    while (adds < 20) {
        var handle = try client.updateWithStartWorkflow(shoppingcart.CartWorkflow, start, .{null}, shoppingcart.UpdateCart, .{ "add", "apple" }, .{});
        defer handle.deinit();
        const cart = try handle.get(a);
        adds += 1;
        try testing.expectEqual(@as(?i64, adds), cart.items.map.get("apple"));
        if (first_run_id.len == 0) first_run_id = try a.dupe(u8, handle.run_id);
        if (!std.mem.eql(u8, first_run_id, handle.run_id)) continued = true;
    }
    try testing.expect(continued);

    try client.signalWorkflow(shoppingcart.Checkout, "session-can", "", .{});
    var run = try client.getWorkflow(void, "session-can", "");
    defer run.deinit();
    try run.get(a);

    const history = try client.fetchHistory(a, "session-can", first_run_id);
    const events = history.events.items;
    try testing.expect(events[events.len - 1].attributes.? == .workflow_execution_continued_as_new_event_attributes);
}
