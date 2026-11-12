//! Tests for the requester, against a test double of the uppercase workflow that answers a
//! request as soon as it is signaled, without running an activity.

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;
const uppercase = @import("workflow.zig");
const Requester = @import("requester.zig").Requester;

const testing = std.testing;

const Double = struct {
    ctx: *workflow.Context,
    responses: std.StringHashMapUnmanaged(uppercase.Response) = .empty,

    fn request(d: *Double, req: uppercase.Request) !void {
        const output = try std.ascii.allocUpperString(d.ctx.allocator(), req.input);
        try d.responses.put(d.ctx.allocator(), req.id, .{ .id = req.id, .output = output });
    }

    fn responseFor(d: *Double, request_id: []const u8) ?uppercase.Response {
        return d.responses.get(request_id);
    }

    fn never(_: *Double) bool {
        return false;
    }
};

fn uppercaseDouble(ctx: *workflow.Context) !void {
    const d = try ctx.allocator().create(Double);
    d.* = .{ .ctx = ctx };
    try ctx.setQueryHandler(uppercase.ResponseQuery, d, Double.responseFor);
    try ctx.setSignalHandler(uppercase.RequestSignal, d, Double.request);
    try ctx.await(d, Double.never);
}

const UppercaseDouble = workflow.Def(uppercase.UppercaseWorkflow.workflow_name, uppercaseDouble);

test "requester signals a request and polls for its response" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(UppercaseDouble);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var run = try env.startWorkflow(UppercaseDouble, .{ .id = "some-workflow", .task_queue = "" }, .{});
    defer run.deinit();

    const requester = try Requester.init(&env.client, testing.io, .{ .target_workflow_id = "some-workflow" });
    try testing.expectEqualStrings("SOME VALUE", try requester.requestUppercase(a, "SoMe VaLuE"));

    try env.client.cancelWorkflow(run.id, "", "test finished");
    try testing.expectError(error.WorkflowCanceled, env.getResult(UppercaseDouble, &run, a, null));
}

test "requester requires a target workflow" {
    // init checks its options without using the client.
    var client: temporal.client.Client = undefined;
    try testing.expectError(error.MissingTargetWorkflowId, Requester.init(&client, testing.io, .{ .target_workflow_id = "" }));
}
