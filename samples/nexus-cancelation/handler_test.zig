//! Tests of the handler workflow on its own: it greets after its work, and canceled while it
//! works, it cleans up and then ends canceled.

const std = @import("std");
const temporal = @import("temporal");
const service = @import("service.zig");
const handler = @import("handler.zig");
const history = @import("history.zig");

const testing = std.testing;
const WorkflowEnvironment = temporal.testing.WorkflowEnvironment;
const EventType = temporal.proto.temporal.api.enums.v1.EventType;

test "the hello handler workflow greets in each language after its work" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(handler.HelloHandlerWorkflow);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const cases = [_]struct { service.Language, []const u8 }{
        .{ .en, "Hello Temporal 👋" },
        .{ .fr, "Bonjour Temporal 👋" },
        .{ .de, "Hallo Temporal 👋" },
        .{ .es, "¡Hola! Temporal 👋" },
        .{ .tr, "Merhaba Temporal 👋" },
    };
    for (cases) |case| {
        const output = try env.executeWorkflow(handler.HelloHandlerWorkflow, .{.{ .name = "Temporal", .language = case[0], .work_ms = 10 }}, arena.allocator(), null);
        try testing.expectEqualStrings(case[1], output.message);
    }
}

test "without fixed times, the hello handler workflow draws its work time in a side effect" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(handler.HelloHandlerWorkflow);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var run = try env.startWorkflow(handler.HelloHandlerWorkflow, .{}, .{.{ .name = "Temporal", .language = .fr }});
    defer run.deinit();
    // At most four seconds of work; `getResult` replays the run, side effect included.
    try testing.expectEqualStrings("Bonjour Temporal 👋", (try env.getResult(handler.HelloHandlerWorkflow, &run, a, null)).message);
    var markers: usize = 0;
    for ((try env.client.fetchHistory(a, run.id, run.run_id)).events.items) |event| {
        if (event.event_type == .EVENT_TYPE_MARKER_RECORDED) markers += 1;
    }
    try testing.expectEqual(1, markers);
}

test "canceled while it works, the hello handler workflow cleans up and then ends canceled" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(handler.HelloHandlerWorkflow);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    // The timeout bounds a workflow that ignores the cancellation.
    var run = try env.startWorkflow(handler.HelloHandlerWorkflow, .{ .execution_timeout = .fromSeconds(30) }, .{.{ .name = "Temporal", .language = .de, .work_ms = 10 * std.time.ms_per_min, .cleanup_ms = 300 }});
    defer run.deinit();

    // Once its work timer has started, the workflow is working.
    try waitForEvent(&env.client, run.id, run.run_id, .EVENT_TYPE_TIMER_STARTED);
    try env.client.cancelWorkflow(run.id, run.run_id, "test");
    try testing.expectError(error.WorkflowCanceled, env.getResult(handler.HelloHandlerWorkflow, &run, a, null));

    const cleanup = try history.cleanupAfterCancel(a, try env.client.fetchHistory(a, run.id, run.run_id));
    try testing.expectEqual(300, cleanup.toMilliseconds());
}

/// Follows the history of `workflow_id` until it records an event of type `event_type`.
fn waitForEvent(client: *temporal.client.Client, workflow_id: []const u8, run_id: []const u8, event_type: EventType) !void {
    var it = try client.historyIterator(workflow_id, run_id, .{ .wait_new_event = true });
    defer it.deinit();
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    while (try it.next(scratch.allocator())) |event| {
        if (event.event_type == event_type) return;
    }
    return error.TestEventNotRecorded;
}
