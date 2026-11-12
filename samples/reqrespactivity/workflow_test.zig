//! Tests for the uppercase workflow, on the dev server: the workflow waits for requests in
//! real time. Responses go to a test double of a requester's response activity, run by a
//! worker on a task queue of its own as a requester's would be. The continue-as-new test runs
//! the workflow with a limit of 5 requests per run instead of the sample's 500.

const std = @import("std");
const temporal = @import("temporal");
const uppercase = @import("workflow.zig");

const testing = std.testing;
const Response = uppercase.Response;
const ActivityContext = temporal.activity.Context;

/// Records the responses the workflow sends, in the order they arrive. Responses to a request
/// whose input was "reject me" are refused with a non-retryable failure instead.
const Recorder = struct {
    io: std.Io,
    arena: std.mem.Allocator,
    mutex: std.Io.Mutex = .init,
    responses: std.ArrayList(Response) = .empty,
    /// How many calls have been made, refused ones included. Guarded by `mutex`.
    calls: usize = 0,
    changed: std.Io.Condition = .init,

    fn record(r: *Recorder, ctx: *ActivityContext, response: Response) !void {
        try r.mutex.lock(r.io);
        defer r.mutex.unlock(r.io);
        defer r.changed.broadcast(r.io);
        r.calls += 1;
        if (std.mem.eql(u8, response.output, "REJECT ME")) {
            return ctx.fail(.{ .message = "requester went away", .non_retryable = true });
        }
        try r.responses.append(r.arena, .{
            .id = try r.arena.dupe(u8, response.id),
            .output = try r.arena.dupe(u8, response.output),
            .error_message = try r.arena.dupe(u8, response.error_message),
        });
    }

    /// Waits until the activity has been called `count` times.
    fn awaitCalls(r: *Recorder, count: usize) !void {
        try r.mutex.lock(r.io);
        defer r.mutex.unlock(r.io);
        while (r.calls < count) try r.changed.wait(r.io, &r.mutex);
    }
};

const ExternalActivity = temporal.activity.Def("external-activity", Recorder.record);

fn runWorker(w: *temporal.worker.Worker) temporal.worker.Worker.RunError!void {
    return w.run();
}

/// A dev server environment running `Workflow` and the uppercase activity, plus a worker on
/// `external_task_queue` running the recorder's activity.
fn Harness(comptime Workflow: type) type {
    return struct {
        env: *temporal.testing.WorkflowEnvironment,
        arena_state: std.heap.ArenaAllocator,
        recorder: Recorder,
        external_task_queue: []const u8,
        external: temporal.worker.Worker,
        running: std.Io.Future(temporal.worker.Worker.RunError!void),

        const Self = @This();

        /// `uppercase_impl` stands in for the uppercase activity, or null for the real one.
        fn start(h: *Self, comptime uppercase_impl: anytype) !void {
            h.env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
            errdefer h.env.deinit();
            try h.env.registerWorkflow(Workflow);
            if (@TypeOf(uppercase_impl) == @TypeOf(null)) {
                try h.env.registerActivity(uppercase.UppercaseActivity);
            } else {
                try h.env.registerActivityAs(uppercase.UppercaseActivity, uppercase_impl);
            }

            h.arena_state = .init(testing.allocator);
            errdefer h.arena_state.deinit();
            h.recorder = .{ .io = testing.io, .arena = h.arena() };
            h.external_task_queue = try std.fmt.allocPrint(h.arena(), "{s}-external", .{h.env.task_queue});
            h.external = try .init(testing.allocator, testing.io, &h.env.client, h.external_task_queue, .{});
            errdefer h.external.deinit();
            try h.external.registerActivityWith(ExternalActivity, &h.recorder);
            h.running = try testing.io.concurrent(runWorker, .{&h.external});
        }

        fn deinit(h: *Self) void {
            h.external.stop();
            h.running.await(testing.io) catch {};
            h.external.deinit();
            h.env.deinit();
            h.arena_state.deinit();
        }

        fn arena(h: *Self) std.mem.Allocator {
            return h.arena_state.allocator();
        }

        fn request(h: *Self, workflow_id: []const u8, id: []const u8, input: []const u8) !void {
            try h.env.signalWorkflow(uppercase.RequestSignal, workflow_id, .{.{
                .id = id,
                .input = input,
                .response_activity = ExternalActivity.activity_name,
                .response_task_queue = h.external_task_queue,
            }});
        }

        /// Cancels the latest run of `workflow_id` and checks that it ends canceled, which also
        /// replays its history.
        fn cancel(h: *Self, workflow_id: []const u8) !void {
            try h.env.client.cancelWorkflow(workflow_id, "", "test finished");
            var latest = try h.env.client.getWorkflow(void, workflow_id, "");
            defer latest.deinit();
            try testing.expectError(error.WorkflowCanceled, h.env.getResult(Workflow, &latest, h.arena(), null));
        }
    };
}

fn expectResponse(expected: Response, actual: Response) !void {
    try testing.expectEqualStrings(expected.id, actual.id);
    try testing.expectEqualStrings(expected.output, actual.output);
    try testing.expectEqualStrings(expected.error_message, actual.error_message);
}

test "a signaled request is answered by the response activity" {
    var h: Harness(uppercase.UppercaseWorkflow) = undefined;
    try h.start(null);
    defer h.deinit();

    var run = try h.env.startWorkflow(uppercase.UppercaseWorkflow, .{ .task_queue = "" }, .{});
    defer run.deinit();
    try h.request(run.id, "request1", "foo");
    try h.recorder.awaitCalls(1);
    try testing.expectEqual(1, h.recorder.responses.items.len);
    try expectResponse(.{ .id = "request1", .output = "FOO" }, h.recorder.responses.items[0]);

    // The workflow takes requests until it is canceled.
    try h.cancel(run.id);
}

fn refuseToUppercase(ctx: *ActivityContext, input: []const u8) ![]const u8 {
    _ = input;
    return ctx.fail(.{ .message = "no uppercase today", .non_retryable = true });
}

test "a failed request is answered with its failure" {
    var h: Harness(uppercase.UppercaseWorkflow) = undefined;
    try h.start(refuseToUppercase);
    defer h.deinit();

    var run = try h.env.startWorkflow(uppercase.UppercaseWorkflow, .{ .task_queue = "" }, .{});
    defer run.deinit();
    try h.request(run.id, "request1", "foo");
    try h.recorder.awaitCalls(1);
    try expectResponse(.{ .id = "request1", .error_message = "no uppercase today" }, h.recorder.responses.items[0]);
    try h.cancel(run.id);
}

test "a response the requester refuses doesn't stop the workflow" {
    var h: Harness(uppercase.UppercaseWorkflow) = undefined;
    try h.start(null);
    defer h.deinit();

    var run = try h.env.startWorkflow(uppercase.UppercaseWorkflow, .{ .task_queue = "" }, .{});
    defer run.deinit();
    try h.request(run.id, "refused", "reject me");
    try h.recorder.awaitCalls(1);
    try h.request(run.id, "request2", "bar");
    try h.recorder.awaitCalls(2);
    try testing.expectEqual(1, h.recorder.responses.items.len);
    try expectResponse(.{ .id = "request2", .output = "BAR" }, h.recorder.responses.items[0]);
    try h.cancel(run.id);
}

test "continues as new once every request has been answered" {
    const limit = 5;
    const Workflow = uppercase.UppercaseWorkflowWithLimit(limit).Def;
    var h: Harness(Workflow) = undefined;
    try h.start(null);
    defer h.deinit();
    const a = h.arena();

    var run = try h.env.startWorkflow(Workflow, .{ .task_queue = "" }, .{});
    defer run.deinit();
    const first_run_id = try a.dupe(u8, run.run_id);

    for (0..limit) |i| {
        try h.request(run.id, try std.fmt.allocPrint(a, "request{d}", .{i}), try std.fmt.allocPrint(a, "foo{d}", .{i}));
    }
    try testing.expectError(error.WorkflowContinuedAsNew, run.getWithOptions(a, .{ .follow_runs = false }));

    // The first run answered every request before it continued as new.
    try h.recorder.awaitCalls(limit);
    var answered: [limit]bool = @splat(false);
    for (h.recorder.responses.items) |response| {
        const i = try std.fmt.parseInt(usize, response.id["request".len..], 10);
        try expectResponse(.{ .id = response.id, .output = try std.fmt.allocPrint(a, "FOO{d}", .{i}) }, response);
        answered[i] = true;
    }
    try testing.expectEqual(@as([limit]bool, @splat(true)), answered);
    try h.env.replayRun(run.id, first_run_id);

    // The new run takes requests.
    try h.request(run.id, "next", "bar");
    try h.recorder.awaitCalls(limit + 1);
    try expectResponse(.{ .id = "next", .output = "BAR" }, h.recorder.responses.items[limit]);
    try h.cancel(run.id);
}

test "the activity uppercases its input" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const env: temporal.testing.ActivityEnvironment = .init(testing.io);
    try testing.expectEqualStrings("SOME VALUE", try env.executeActivity(uppercase.UppercaseActivity, .{"SoMe VaLuE"}, arena.allocator()));
}
