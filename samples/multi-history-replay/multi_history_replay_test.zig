//! Tests for the multi-history replay sample: runs of the hello world workflow on a dev
//! server replay cleanly against its code, and every one of them fails to replay against a
//! changed workflow.

const std = @import("std");
const temporal = @import("temporal");
const helloworld = @import("helloworld");
const multi_history_replay = @import("multi_history_replay.zig");

const testing = std.testing;
const WorkflowReplayer = temporal.testing.WorkflowReplayer;

/// The hello world workflow after a change that drops its activity call.
fn helloWorldWithoutActivity(_: *temporal.workflow.Context, name: []const u8) ![]const u8 {
    _ = name;
    return "Hello Temporal!";
}

const ChangedHelloWorld = temporal.workflow.Def("HelloWorld", helloWorldWithoutActivity);

test "replays every listed run of the workflow ID and catches a change that breaks them" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(helloworld.HelloWorld);
    try env.registerActivity(helloworld.Greet);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const runs = 3;
    var run_ids: [runs][]const u8 = undefined;
    for (&run_ids) |*run_id| {
        var run = try env.startWorkflow(helloworld.HelloWorld, .{ .id = multi_history_replay.workflow_id, .task_queue = "" }, .{"Temporal"});
        defer run.deinit();
        try testing.expectEqualStrings("Hello Temporal!", try env.getResult(helloworld.HelloWorld, &run, a, null));
        run_id.* = try a.dupe(u8, run.run_id);
    }

    const completed_query = multi_history_replay.query ++ " AND ExecutionStatus='Completed'";
    var tries: usize = 0;
    while (try env.client.countWorkflows(completed_query) != runs) : (tries += 1) {
        if (tries == 300) return error.TestUnexpectedResult;
        try testing.io.sleep(.fromMilliseconds(100), .awake);
    }

    var replayer: WorkflowReplayer = .initWithOptions(testing.allocator, testing.io, .{ .task_queue = env.task_queue });
    defer replayer.deinit();
    try replayer.registerWorkflow(helloworld.HelloWorld);
    const results = try multi_history_replay.replayListed(&replayer, a, &env.client, multi_history_replay.query);
    try testing.expectEqual(@as(usize, runs), results.len);
    for (results) |result| {
        try testing.expectEqual(@as(?WorkflowReplayer.ReplayError, null), result.err);
        try testing.expectEqualStrings(multi_history_replay.workflow_id, result.workflow_id);
        for (run_ids) |run_id| {
            if (std.mem.eql(u8, run_id, result.run_id)) break;
        } else return error.TestUnexpectedResult;
    }

    var changed: WorkflowReplayer = .init(testing.allocator, testing.io);
    defer changed.deinit();
    try changed.registerWorkflow(ChangedHelloWorld);
    const changed_results = try multi_history_replay.replayListed(&changed, a, &env.client, multi_history_replay.query);
    try testing.expectEqual(@as(usize, runs), changed_results.len);
    for (changed_results) |result| {
        try testing.expectEqual(@as(?WorkflowReplayer.ReplayError, error.Nondeterminism), result.err);
    }
}
