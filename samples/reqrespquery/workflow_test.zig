//! Tests for the uppercase workflow, on the dev server: the workflow waits for requests in
//! real time, and its responses are polled for as a requester would.

const std = @import("std");
const temporal = @import("temporal");
const uppercase = @import("workflow.zig");

const testing = std.testing;
const Response = uppercase.Response;

/// Queries the latest run of `workflow_id` until it has the response to `request_id`.
fn awaitResponse(env: *temporal.testing.WorkflowEnvironment, arena: std.mem.Allocator, workflow_id: []const u8, request_id: []const u8) !Response {
    while (true) {
        if (try env.queryWorkflow(uppercase.ResponseQuery, arena, workflow_id, .{request_id})) |response| return response;
        try testing.io.sleep(.fromMilliseconds(20), .awake);
    }
}

fn expectResponse(expected: Response, actual: Response) !void {
    try testing.expectEqualStrings(expected.id, actual.id);
    try testing.expectEqualStrings(expected.output, actual.output);
    try testing.expectEqualStrings(expected.error_message, actual.error_message);
}

test "a signaled request's response is read by query" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(uppercase.UppercaseWorkflow);
    try env.registerActivity(uppercase.UppercaseActivity);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var run = try env.startWorkflow(uppercase.UppercaseWorkflow, .{ .task_queue = "" }, .{});
    defer run.deinit();
    try testing.expectEqual(null, try env.queryWorkflow(uppercase.ResponseQuery, a, run.id, .{"request1"}));

    try env.signalWorkflow(uppercase.RequestSignal, run.id, .{.{ .id = "request1", .input = "foo" }});
    try expectResponse(.{ .id = "request1", .output = "FOO" }, try awaitResponse(env, a, run.id, "request1"));

    // The workflow takes requests until it is canceled.
    try env.client.cancelWorkflow(run.id, "", "test finished");
    try testing.expectError(error.WorkflowCanceled, env.getResult(uppercase.UppercaseWorkflow, &run, a, null));
}

test "continues as new once every request has been answered" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(uppercase.UppercaseWorkflow);
    try env.registerActivity(uppercase.UppercaseActivity);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var run = try env.startWorkflow(uppercase.UppercaseWorkflow, .{ .task_queue = "" }, .{});
    defer run.deinit();
    const first_run_id = try a.dupe(u8, run.run_id);

    for (0..uppercase.requests_before_continue_as_new) |i| {
        const id = try std.fmt.allocPrint(a, "request{d}", .{i});
        const input = try std.fmt.allocPrint(a, "foo{d}", .{i});
        try env.signalWorkflow(uppercase.RequestSignal, run.id, .{.{ .id = id, .input = input }});
    }
    try testing.expectError(error.WorkflowContinuedAsNew, run.getWithOptions(a, .{ .follow_runs = false }));

    // The first run answered every request before it continued as new, and still answers
    // queries now that it is closed.
    const last = uppercase.requests_before_continue_as_new - 1;
    const last_id = try std.fmt.allocPrint(a, "request{d}", .{last});
    const last_output = try std.fmt.allocPrint(a, "FOO{d}", .{last});
    for ([_][]const u8{ "request0", last_id }, [_][]const u8{ "FOO0", last_output }) |id, output| {
        const response = try env.client.queryWorkflow(uppercase.ResponseQuery, a, run.id, first_run_id, .{id}) orelse
            return error.TestMissingResponse;
        try expectResponse(.{ .id = id, .output = output }, response);
    }
    try env.replayRun(run.id, first_run_id);

    // The new run starts with no responses and takes requests.
    try testing.expectEqual(null, try env.queryWorkflow(uppercase.ResponseQuery, a, run.id, .{"request0"}));
    try env.signalWorkflow(uppercase.RequestSignal, run.id, .{.{ .id = "next", .input = "bar" }});
    try expectResponse(.{ .id = "next", .output = "BAR" }, try awaitResponse(env, a, run.id, "next"));

    try env.client.cancelWorkflow(run.id, "", "test finished");
    var latest = try env.client.getWorkflow(void, run.id, "");
    defer latest.deinit();
    try testing.expectError(error.WorkflowCanceled, env.getResult(uppercase.UppercaseWorkflow, &latest, a, null));
}

test "the activity uppercases its input" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const env: temporal.testing.ActivityEnvironment = .init(testing.io);
    try testing.expectEqualStrings("SOME VALUE", try env.executeActivity(uppercase.UppercaseActivity, .{"SoMe VaLuE"}, arena.allocator()));
}
