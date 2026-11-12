//! Tests for the DSL sample.

const std = @import("std");
const temporal = @import("temporal");
const dsl = @import("dsl.zig");

const testing = std.testing;
const activity = temporal.activity;

/// Stands in for every sample activity: records each call's activity name and input, in the
/// order the calls arrive, and returns what the real activities return.
const Recorder = struct {
    const Call = struct { name: []const u8, input: []const u8 };

    var mutex: std.Io.Mutex = .init;
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    var calls: std.ArrayList(Call) = .empty;

    fn reset() void {
        calls = .empty;
        _ = arena.reset(.free_all);
    }

    fn deinit() void {
        calls = .empty;
        arena.deinit();
        arena = .init(testing.allocator);
    }

    fn record(ctx: *activity.Context, input: []const []const u8) ![]const u8 {
        const name = ctx.info().activity_type;
        {
            try mutex.lock(ctx.io());
            defer mutex.unlock(ctx.io());
            const a = arena.allocator();
            try calls.append(a, .{ .name = try a.dupe(u8, name), .input = try std.mem.join(a, " ", input) });
        }
        return std.fmt.allocPrint(ctx.allocator(), "Result_{s}", .{name});
    }

    fn indexOf(name: []const u8, input: []const u8) ?usize {
        for (calls.items, 0..) |c, i| {
            if (std.mem.eql(u8, c.name, name) and std.mem.eql(u8, c.input, input)) return i;
        }
        return null;
    }
};

fn startEnv() !*temporal.testing.WorkflowEnvironment {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    errdefer env.deinit();
    try env.registerWorkflow(dsl.SimpleDslWorkflow);
    return env;
}

fn registerRecorders(env: *temporal.testing.WorkflowEnvironment) !void {
    inline for (dsl.all_activities) |Activity| try env.registerActivityAs(Activity, Recorder.record);
}

test "workflow1 runs its activities in sequence, passing results along" {
    Recorder.reset();
    defer Recorder.deinit();
    const env = try startEnv();
    defer env.deinit();
    try registerRecorders(env);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var diag: std.zon.parse.Diagnostics = undefined;
    const definition = try dsl.parseDefinition(arena.allocator(), @embedFile("workflow1.zon"), &diag);
    try env.executeWorkflow(dsl.SimpleDslWorkflow, .{definition}, arena.allocator(), null);

    const expected = [_]Recorder.Call{
        .{ .name = "SampleActivity1", .input = "value1" },
        .{ .name = "SampleActivity2", .input = "Result_SampleActivity1" },
        .{ .name = "SampleActivity3", .input = "value2 Result_SampleActivity2" },
    };
    try testing.expectEqual(expected.len, Recorder.calls.items.len);
    for (expected, Recorder.calls.items) |want, got| {
        try testing.expectEqualStrings(want.name, got.name);
        try testing.expectEqualStrings(want.input, got.input);
    }
}

test "workflow2 runs its parallel branches and then the step that joins their results" {
    Recorder.reset();
    defer Recorder.deinit();
    const env = try startEnv();
    defer env.deinit();
    try registerRecorders(env);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var diag: std.zon.parse.Diagnostics = undefined;
    const definition = try dsl.parseDefinition(arena.allocator(), @embedFile("workflow2.zon"), &diag);
    try env.executeWorkflow(dsl.SimpleDslWorkflow, .{definition}, arena.allocator(), null);

    try testing.expectEqual(@as(usize, 6), Recorder.calls.items.len);
    try testing.expectEqual(@as(?usize, 0), Recorder.indexOf("SampleActivity1", "value1"));
    try testing.expectEqual(@as(?usize, 5), Recorder.indexOf("SampleActivity1", "Result_SampleActivity3 Result_SampleActivity5"));
    // The branches interleave in no defined order, but each runs its own steps in order.
    const step2 = Recorder.indexOf("SampleActivity2", "Result_SampleActivity1").?;
    const step3 = Recorder.indexOf("SampleActivity3", "value2 Result_SampleActivity2").?;
    const step4 = Recorder.indexOf("SampleActivity4", "Result_SampleActivity1").?;
    const step5 = Recorder.indexOf("SampleActivity5", "value3 Result_SampleActivity4").?;
    try testing.expect(step2 < step3);
    try testing.expect(step4 < step5);
}

/// For the failing-branch test: SampleActivity4 runs until it is canceled (or released at the
/// end of the test), and SampleActivity2 fails once SampleActivity4 is running.
const Branches = struct {
    var blocked_started: std.Io.Event = .unset;
    var release: std.Io.Event = .unset;
    var after_parallel_ran: bool = false;

    fn blocked(ctx: *activity.Context, input: []const []const u8) ![]const u8 {
        _ = input;
        blocked_started.set(ctx.io());
        try release.wait(ctx.io());
        return "unreachable in this test";
    }

    fn failing(ctx: *activity.Context, input: []const []const u8) ![]const u8 {
        _ = input;
        try blocked_started.wait(ctx.io());
        return ctx.fail(.{ .message = "branch failed", .non_retryable = true });
    }

    fn afterParallel(ctx: *activity.Context, input: []const []const u8) ![]const u8 {
        _ = ctx;
        _ = input;
        after_parallel_ran = true;
        return "";
    }
};

test "a failing parallel branch cancels the other branches and fails the workflow" {
    const env = try startEnv();
    defer env.deinit();
    // Runs before env.deinit, so the worker isn't left waiting on the blocked activity.
    defer Branches.release.set(testing.io);
    try env.registerActivityAs(dsl.SampleActivity4, Branches.blocked);
    try env.registerActivityAs(dsl.SampleActivity2, Branches.failing);
    try env.registerActivityAs(dsl.SampleActivity3, Branches.afterParallel);

    const definition: dsl.Definition = .{ .root = .{ .sequence = .{ .elements = &.{
        .{ .parallel = .{ .branches = &.{
            .{ .activity = .{ .name = "SampleActivity4" } },
            .{ .activity = .{ .name = "SampleActivity2" } },
        } } },
        .{ .activity = .{ .name = "SampleActivity3" } },
    } } } };

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var run = try env.startWorkflow(dsl.SimpleDslWorkflow, .{ .task_queue = "" }, .{definition});
    defer run.deinit();
    var failure: ?temporal.Failure = null;
    try testing.expectError(error.WorkflowFailed, env.getResult(dsl.SimpleDslWorkflow, &run, arena.allocator(), &failure));

    // The workflow fails with the failing branch's activity failure.
    const cause = failure.?.cause orelse return error.TestExpectedCause;
    try testing.expectEqualStrings("branch failed", cause.message);
    try testing.expect(!Branches.after_parallel_ran);

    const history = try env.client.fetchHistory(arena.allocator(), run.id, run.run_id);
    const cancel_requested = for (history.events.items) |event| {
        if (event.attributes.? == .activity_task_cancel_requested_event_attributes) break true;
    } else false;
    try testing.expect(cancel_requested);
}

test "the activities return their name as their result" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var activities: dsl.Activities = .{};
    const env: temporal.testing.ActivityEnvironment = .init(testing.io);
    const input: []const []const u8 = &.{ "value2", "Result_SampleActivity2" };
    try testing.expectEqualStrings("Result_SampleActivity3", try env.executeActivityWith(dsl.SampleActivity3, &activities, .{input}, arena.allocator()));
    try testing.expectEqualStrings("Result_SampleActivity5", try env.executeActivityWith(dsl.SampleActivity5, &activities, .{input}, arena.allocator()));
}

test "definitions parse from ZON" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var diag: std.zon.parse.Diagnostics = undefined;
    const definition = try dsl.parseDefinition(arena.allocator(), @embedFile("workflow2.zon"), &diag);
    try testing.expectEqual(@as(usize, 3), definition.variables.len);
    const elements = definition.root.sequence.elements;
    try testing.expectEqual(@as(usize, 3), elements.len);
    try testing.expectEqualStrings("result1", elements[0].activity.result);
    try testing.expectEqual(@as(usize, 2), elements[1].parallel.branches.len);
    try testing.expectEqualStrings("result6", elements[2].activity.result);
}
