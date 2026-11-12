//! Tests for the await-signals workflow, on the time-skipping server. Each test sends its
//! signals with `env.sleep` moving server time between them, then waits for the result, which
//! skips time through any remaining timeout.

const std = @import("std");
const temporal = @import("temporal");
const await_signals = @import("workflow.zig");

const testing = std.testing;
const WorkflowEnvironment = temporal.testing.WorkflowEnvironment;
const AwaitSignalsWorkflow = await_signals.AwaitSignalsWorkflow;

fn startEnv() !*WorkflowEnvironment {
    const env = try WorkflowEnvironment.startTimeSkipping(testing.allocator, testing.io);
    errdefer env.deinit();
    try env.registerWorkflow(AwaitSignalsWorkflow);
    return env;
}

/// Checks that the workflow failed with a "timeout" application failure with `message`.
fn expectTimeoutFailure(env: *WorkflowEnvironment, run: *temporal.client.WorkflowRun(void), arena: std.mem.Allocator, message: []const u8) !void {
    var failure: ?temporal.Failure = null;
    try testing.expectError(error.WorkflowFailed, env.getResult(AwaitSignalsWorkflow, run, arena, &failure));
    try testing.expectEqualStrings(message, failure.?.message);
    try testing.expectEqualStrings("timeout", temporal.failure.applicationType(failure.?).?);
}

test "signals in order" {
    const env = try startEnv();
    defer env.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();

    var run = try env.startWorkflow(AwaitSignalsWorkflow, .{ .task_queue = "" }, .{});
    defer run.deinit();
    try env.sleep(.fromSeconds(60 * 60));
    try env.signalWorkflow(await_signals.Signal1, run.id, .{});
    try env.sleep(.fromSeconds(1));
    try env.signalWorkflow(await_signals.Signal2, run.id, .{});
    try env.sleep(.fromSeconds(2));
    try env.signalWorkflow(await_signals.Signal3, run.id, .{});
    try env.getResult(AwaitSignalsWorkflow, &run, arena.allocator(), null);
}

test "signals in reverse order" {
    const env = try startEnv();
    defer env.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();

    var run = try env.startWorkflow(AwaitSignalsWorkflow, .{ .task_queue = "" }, .{});
    defer run.deinit();
    try env.sleep(.fromSeconds(60 * 60));
    try env.signalWorkflow(await_signals.Signal3, run.id, .{});
    try env.sleep(.fromSeconds(1));
    try env.signalWorkflow(await_signals.Signal2, run.id, .{});
    try env.sleep(.fromSeconds(2));
    try env.signalWorkflow(await_signals.Signal1, run.id, .{});
    try env.getResult(AwaitSignalsWorkflow, &run, arena.allocator(), null);
}

test "fails when the next signal doesn't arrive in time" {
    const env = try startEnv();
    defer env.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();

    var run = try env.startWorkflow(AwaitSignalsWorkflow, .{ .task_queue = "" }, .{});
    defer run.deinit();
    try env.signalWorkflow(await_signals.Signal1, run.id, .{});
    try expectTimeoutFailure(env, &run, arena.allocator(), "Timed out waiting for signal2");
}

test "the total timeout counts from the first signal, whichever it is" {
    const env = try startEnv();
    defer env.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();

    var run = try env.startWorkflow(AwaitSignalsWorkflow, .{ .task_queue = "" }, .{});
    defer run.deinit();
    try env.signalWorkflow(await_signals.Signal2, run.id, .{});
    try env.sleep(.fromSeconds(40));
    try env.signalWorkflow(await_signals.Signal1, run.id, .{});
    // Signal2 is already in, so the workflow waits for Signal3 for the 20 seconds left of the
    // total timeout rather than the 30 seconds allowed between signals.
    try expectTimeoutFailure(env, &run, arena.allocator(), "Timed out waiting for signal3");

    // The workflow failed when the total timeout ran out, counted from Signal2's arrival.
    const history = try env.client.fetchHistory(arena.allocator(), run.id, run.run_id);
    const events = history.events.items;
    const first_signal = for (events) |event| {
        if (event.attributes.? == .workflow_execution_signaled_event_attributes) break event;
    } else return error.TestNoSignalInHistory;
    const failed = events[events.len - 1];
    try testing.expect(failed.attributes.? == .workflow_execution_failed_event_attributes);
    const elapsed = failed.event_time.?.seconds - first_signal.event_time.?.seconds;
    try testing.expect(elapsed >= await_signals.from_first_signal_timeout.toSeconds());
    try testing.expect(elapsed < 40 + await_signals.signal_to_signal_timeout.toSeconds());
}
