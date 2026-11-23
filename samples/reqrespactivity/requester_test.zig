//! Tests for the requester, against a test double of the uppercase workflow that answers a
//! request by calling its response activity at once, without running the uppercase activity.

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;
const uppercase = @import("workflow.zig");
const Requester = @import("requester.zig").Requester;

const testing = std.testing;

const Double = struct {
    ctx: *workflow.Context,

    fn never(_: *Double) bool {
        return false;
    }

    fn request(d: *Double, req: uppercase.Request) !void {
        const response: uppercase.Response = .{
            .id = req.id,
            .output = try std.ascii.allocUpperString(d.ctx.allocator(), req.input),
        };
        const options: workflow.ActivityOptions = .{ .task_queue = req.response_task_queue, .schedule_to_close_timeout = .fromSeconds(10) };
        try d.ctx.executeActivityByName(void, req.response_activity, options, .{response}).get();
    }
};

fn uppercaseDouble(ctx: *workflow.Context) !void {
    const d = try ctx.allocator().create(Double);
    d.* = .{ .ctx = ctx };
    try ctx.setSignalHandler(uppercase.RequestSignal, d, Double.request);
    try ctx.await(d, Double.never);
}

const UppercaseDouble = workflow.Def(uppercase.UppercaseWorkflow.workflow_name, uppercaseDouble);

test "requester signals a request and receives its response by activity" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(UppercaseDouble);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var run = try env.startWorkflow(UppercaseDouble, .{ .id = "some-workflow", .task_queue = "" }, .{});
    defer run.deinit();

    {
        const requester = try Requester.init(testing.allocator, testing.io, &env.client, .{ .target_workflow_id = "some-workflow" });
        defer requester.deinit();
        try testing.expectEqualStrings("SOME VALUE", try requester.requestUppercase(a, "SoMe VaLuE"));
    }

    try env.client.cancelWorkflow(run.id, "", "test finished");
    try testing.expectError(error.WorkflowCanceled, env.getResult(UppercaseDouble, &run, a, null));
}

/// Like `Double`, but calls the response activity with the sample's `response_options`, so
/// the response must start within their schedule-to-start limit.
const DoubleWithStartLimit = struct {
    ctx: *workflow.Context,

    fn never(_: *DoubleWithStartLimit) bool {
        return false;
    }

    fn request(d: *DoubleWithStartLimit, req: uppercase.Request) !void {
        const response: uppercase.Response = .{
            .id = req.id,
            .output = try std.ascii.allocUpperString(d.ctx.allocator(), req.input),
        };
        var options = uppercase.response_options;
        options.task_queue = req.response_task_queue;
        try d.ctx.executeActivityByName(void, req.response_activity, options, .{response}).get();
    }
};

fn uppercaseDoubleWithStartLimit(ctx: *workflow.Context) !void {
    const d = try ctx.allocator().create(DoubleWithStartLimit);
    d.* = .{ .ctx = ctx };
    try ctx.setSignalHandler(uppercase.RequestSignal, d, DoubleWithStartLimit.request);
    try ctx.await(d, DoubleWithStartLimit.never);
}

const UppercaseDoubleWithStartLimit = workflow.Def(uppercase.UppercaseWorkflow.workflow_name, uppercaseDoubleWithStartLimit);

test "requester answers the first request when its worker is slow to start" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(UppercaseDoubleWithStartLimit);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var run = try env.startWorkflow(UppercaseDoubleWithStartLimit, .{ .id = "slow-requester-workflow", .task_queue = "" }, .{});
    defer run.deinit();

    {
        // Longer than the response activity's 5 s schedule-to-start.
        const requester = try Requester.init(testing.allocator, testing.io, &env.client, .{
            .target_workflow_id = "slow-requester-workflow",
            .worker_start_delay = .fromSeconds(6),
        });
        defer requester.deinit();
        try testing.expectEqualStrings("FOO0", try requester.requestUppercase(a, "foo0"));
    }

    try env.client.cancelWorkflow(run.id, "", "test finished");
    try testing.expectError(error.WorkflowCanceled, env.getResult(UppercaseDoubleWithStartLimit, &run, a, null));
}

test "requester requires a target workflow" {
    // init checks its options before using the client.
    var client: temporal.client.Client = undefined;
    try testing.expectError(error.MissingTargetWorkflowId, Requester.init(testing.allocator, testing.io, &client, .{ .target_workflow_id = "" }));
}
