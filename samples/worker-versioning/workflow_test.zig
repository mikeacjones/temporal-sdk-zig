//! Tests for the worker versioning sample.

const std = @import("std");
const temporal = @import("temporal");
const wv = @import("worker_versioning.zig");

const testing = std.testing;
const Client = temporal.client.Client;
const WorkflowReplayer = temporal.testing.WorkflowReplayer;

/// The activity types a run scheduled, in order. Memory from `arena`.
fn activityTypes(client: *Client, arena: std.mem.Allocator, workflow_id: []const u8, run_id: []const u8) ![]const []const u8 {
    const history = try client.fetchHistory(arena, workflow_id, run_id);
    var types: std.ArrayList([]const u8) = .empty;
    for (history.events.items) |e| switch (e.attributes orelse continue) {
        .activity_task_scheduled_event_attributes => |a| try types.append(arena, a.activity_type.?.name),
        else => {},
    };
    return types.items;
}

fn expectActivities(want: []const []const u8, got: []const []const u8) !void {
    try testing.expectEqual(want.len, got.len);
    for (want, got) |w, g| try testing.expectEqualStrings(w, g);
}

/// Replays a run's history with `Workflow` as the only code for its type.
fn replay(comptime Workflow: type, client: *Client, workflow_id: []const u8, run_id: []const u8) WorkflowReplayer.ReplayError!void {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const history = client.fetchHistory(arena.allocator(), workflow_id, run_id) catch return error.InvalidHistory;
    var r: WorkflowReplayer = .initWithOptions(testing.allocator, testing.io, .{ .task_queue = wv.task_queue });
    defer r.deinit();
    r.registerWorkflow(Workflow) catch return error.OutOfMemory;
    return r.replayHistory(history);
}

test "build 1.1's auto-upgrading workflow takes the new branch on a new run, which build 1.0 can't replay" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    try env.registerWorkflow(wv.AutoUpgradingWorkflowV1b);
    try env.registerActivity(wv.SomeActivity);
    try env.registerActivity(wv.SomeIncompatibleActivity);

    var run = try env.startWorkflow(wv.AutoUpgradingWorkflowV1b, .{ .task_queue = "" }, .{});
    defer run.deinit();
    for ([_][]const u8{ "do-activity", "do-activity", "finish" }) |signal| try env.signalWorkflow(wv.DoNextSignal, run.id, .{signal});
    // getResult also replays the run with build 1.1's code.
    try env.getResult(wv.AutoUpgradingWorkflowV1b, &run, a, null);
    try expectActivities(&.{ "SomeIncompatibleActivity", "SomeIncompatibleActivity" }, try activityTypes(&env.client, a, run.id, run.run_id));
    try testing.expectError(error.Nondeterminism, replay(wv.AutoUpgradingWorkflowV1, &env.client, run.id, run.run_id));
}

test "build 1.1's auto-upgrading workflow replays a history build 1.0 wrote" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    try env.registerWorkflow(wv.AutoUpgradingWorkflowV1);
    try env.registerActivity(wv.SomeActivity);
    try env.registerActivity(wv.SomeIncompatibleActivity);

    var run = try env.startWorkflow(wv.AutoUpgradingWorkflowV1, .{ .task_queue = "" }, .{});
    defer run.deinit();
    for ([_][]const u8{ "do-activity", "do-activity", "finish" }) |signal| try env.signalWorkflow(wv.DoNextSignal, run.id, .{signal});
    try env.getResult(wv.AutoUpgradingWorkflowV1, &run, a, null);
    try expectActivities(&.{ "SomeActivity", "SomeActivity" }, try activityTypes(&env.client, a, run.id, run.run_id));
    try replay(wv.AutoUpgradingWorkflowV1b, &env.client, run.id, run.run_id);
}

test "build 2.0's pinned workflow can't replay a history build 1.0 wrote" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    try env.registerWorkflow(wv.PinnedWorkflowV1);
    try env.registerActivity(wv.SomeActivity);
    try env.registerActivity(wv.SomeIncompatibleActivity);

    var run = try env.startWorkflow(wv.PinnedWorkflowV1, .{ .task_queue = "" }, .{});
    defer run.deinit();
    for ([_][]const u8{ "some-signal", "conclude" }) |signal| try env.signalWorkflow(wv.DoNextSignal, run.id, .{signal});
    try env.getResult(wv.PinnedWorkflowV1, &run, a, null);
    try expectActivities(&.{"SomeActivity"}, try activityTypes(&env.client, a, run.id, run.run_id));
    try testing.expectError(error.Nondeterminism, replay(wv.PinnedWorkflowV2, &env.client, run.id, run.run_id));
}

test "rollout: pinned runs finish on the build they started on, auto-upgrading runs move to the current one" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const c = &env.client;
    const gpa = testing.allocator;
    const io = testing.io;

    var v1: wv.BuildWorker = undefined;
    try v1.start(gpa, io, c, .v1);
    defer v1.stop();
    try wv.waitForWorkerAndMakeCurrent(gpa, io, c, .v1);

    var auto_upgrading = try c.executeWorkflowByName(void, "AutoUpgradingWorkflow", .{ .id = "auto-upgrading", .task_queue = wv.task_queue }, .{});
    defer auto_upgrading.deinit();
    var pinned = try c.executeWorkflowByName(void, "PinnedWorkflow", .{ .id = "pinned", .task_queue = wv.task_queue }, .{});
    defer pinned.deinit();
    try wv.advanceWorkflows(c, &auto_upgrading, &pinned);

    var v1_1: wv.BuildWorker = undefined;
    try v1_1.start(gpa, io, c, .v1_1);
    defer v1_1.stop();
    try wv.waitForWorkerAndMakeCurrent(gpa, io, c, .v1_1);
    try wv.advanceWorkflows(c, &auto_upgrading, &pinned);

    var v2: wv.BuildWorker = undefined;
    try v2.start(gpa, io, c, .v2);
    defer v2.stop();
    try wv.waitForWorkerAndMakeCurrent(gpa, io, c, .v2);
    var pinned_2 = try c.executeWorkflowByName(void, "PinnedWorkflow", .{ .id = "pinned-2", .task_queue = wv.task_queue }, .{});
    defer pinned_2.deinit();

    for ([_]*wv.Run{ &auto_upgrading, &pinned, &pinned_2 }) |run| {
        try c.signalWorkflow(wv.DoNextSignal, run.id, run.run_id, .{"conclude"});
        try run.get(a);
    }
    try testing.expectEqualStrings("2.0", (try wv.runBuildId(c, a, auto_upgrading.id, auto_upgrading.run_id)).?);
    try testing.expectEqualStrings("1.0", (try wv.runBuildId(c, a, pinned.id, pinned.run_id)).?);
    try testing.expectEqualStrings("2.0", (try wv.runBuildId(c, a, pinned_2.id, pinned_2.run_id)).?);

    // Each pinned run ran one build's code; the auto-upgrading run's history, written by
    // three builds, replays with the latest.
    try expectActivities(&.{"SomeActivity"}, try activityTypes(c, a, pinned.id, pinned.run_id));
    try expectActivities(&.{ "SomeActivity", "SomeIncompatibleActivity" }, try activityTypes(c, a, pinned_2.id, pinned_2.run_id));
    try replay(wv.PinnedWorkflowV1, c, pinned.id, pinned.run_id);
    try replay(wv.PinnedWorkflowV2, c, pinned_2.id, pinned_2.run_id);
    try replay(wv.AutoUpgradingWorkflowV1b, c, auto_upgrading.id, auto_upgrading.run_id);
    try testing.expectEqual(@as(usize, 6), (try activityTypes(c, a, auto_upgrading.id, auto_upgrading.run_id)).len);
}
