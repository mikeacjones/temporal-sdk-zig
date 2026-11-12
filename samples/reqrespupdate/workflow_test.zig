//! Tests for the reqrespupdate sample, on the dev server: the workflow handles its requests
//! in real time, with no timers to skip. The tests run the workflow with a limit of 20 requests
//! per run instead of the sample's 500, so they reach continue-as-new quickly.

const std = @import("std");
const temporal = @import("temporal");
const reqrespupdate = @import("reqrespupdate.zig");

const testing = std.testing;
const limit = 20;
const UppercaseWorkflow = reqrespupdate.UppercaseWorkflowWithLimit(limit).Def;

/// Holds the request "foo {limit - 1}" in its activity until set, so the workflow has reached
/// its request limit but still has a handler running: it can't continue as new yet, and the
/// requests sent meanwhile find it waiting to.
var last_request_gate: std.Io.Event = .unset;

fn gatedUppercase(ctx: *temporal.activity.Context, input: []const u8) ![]const u8 {
    if (std.mem.eql(u8, input, std.fmt.comptimePrint("foo {d}", .{limit - 1}))) try last_request_gate.wait(ctx.io());
    return std.ascii.allocUpperString(ctx.allocator(), input);
}

fn request(arena: std.mem.Allocator, i: usize) !reqrespupdate.Uppercase.Args {
    return .{.{ .input = try std.fmt.allocPrint(arena, "foo {d}", .{i}) }};
}

fn expectResponse(arena: std.mem.Allocator, i: usize, response: reqrespupdate.Response) !void {
    try testing.expectEqualStrings(try std.fmt.allocPrint(arena, "FOO {d}", .{i}), response.output);
}

/// Sends `limit` requests, the last held in its activity, then `extra` more while the
/// workflow waits to continue as new. Then checks that the run continued as new once the held
/// request finished, that its history replays, and that the next run takes requests again.
fn runToContinueAsNew(reject_update_on_pending_continue_as_new: bool, extra: usize) !void {
    const io = testing.io;
    last_request_gate = .unset;
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, io);
    defer env.deinit();
    // Released on every path, so a failed check doesn't leave the worker stuck in the activity.
    defer last_request_gate.set(io);
    try env.registerWorkflow(UppercaseWorkflow);
    try env.registerActivityAs(reqrespupdate.UppercaseActivity, gatedUppercase);

    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var run = try env.startWorkflow(UppercaseWorkflow, .{ .task_queue = "" }, .{reject_update_on_pending_continue_as_new});
    defer run.deinit();
    const first_run_id = try arena.dupe(u8, run.run_id);

    for (0..limit - 1) |i| {
        try expectResponse(arena, i, try env.updateWorkflow(reqrespupdate.Uppercase, arena, run.id, try request(arena, i), null));
    }
    var last = try env.client.updateWorkflow(reqrespupdate.Uppercase, run.id, first_run_id, try request(arena, limit - 1), .{ .wait_for_stage = .accepted });
    defer last.deinit();

    for (limit..limit + extra) |i| {
        var failure: ?temporal.Failure = null;
        const result = env.updateWorkflow(reqrespupdate.Uppercase, arena, run.id, try request(arena, i), &failure);
        if (reject_update_on_pending_continue_as_new) {
            try testing.expectError(error.UpdateFailed, result);
            try testing.expectEqualStrings("TryingToContinueAsNew", failure.?.message);
        } else {
            try expectResponse(arena, i, try result);
        }
    }

    last_request_gate.set(io);
    try expectResponse(arena, limit - 1, try last.get(arena));

    try testing.expectError(error.WorkflowContinuedAsNew, run.getWithOptions(arena, .{ .follow_runs = false }));
    try env.replayRun(run.id, first_run_id);

    // The new run counts from zero, so it handles requests again.
    try expectResponse(arena, limit + extra, try env.updateWorkflow(reqrespupdate.Uppercase, arena, run.id, try request(arena, limit + extra), null));
    try env.client.terminateWorkflow(run.id, "", "test finished");
}

test "requests past the limit are rejected until the workflow continues as new" {
    try runToContinueAsNew(true, 10);
}

test "without the validator, requests past the limit are handled before continuing as new" {
    try runToContinueAsNew(false, 5);
}

test "UppercaseActivity uppercases its input" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const env: temporal.testing.ActivityEnvironment = .init(testing.io);
    try testing.expectEqualStrings("FOO 1", try env.executeActivityWith(reqrespupdate.UppercaseActivity, null, .{"foo 1"}, arena.allocator()));
}
