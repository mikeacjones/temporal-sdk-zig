//! The legacy build-ID versioning calls of the suite's features/build_id_versioning/common.go:
//! version sets on the feature's task queue, and resetting a run's sticky queue. They are raw
//! WorkflowService calls, as the SDK has no typed ones (PARITY.md, "Build-ID-based
//! versioning (deprecated)"). The server serves them only with `dev_server_args`.
const std = @import("std");
const temporal = @import("temporal");
const Context = @import("Context.zig");

pub const Error = error{ FeatureFailed, Canceled, OutOfMemory };

/// `.dev_server_args` of every build_id_versioning feature: the dev server refuses the
/// version-set calls without it.
pub const dev_server_args = [_][]const u8{ "--dynamic-config-value", "frontend.workerVersioningDataAPIs=true" };

const Request = temporal.proto.temporal.api.workflowservice.v1.UpdateWorkerBuildIdCompatibilityRequest;

/// UpdateWorkerBuildIdCompatibility: `build_id` in a new default set.
pub fn addNewDefault(ctx: *Context, build_id: []const u8) Error!void {
    return updateSets(ctx, ctx.client, .{ .add_new_build_id_in_new_default_set = build_id });
}

/// UpdateWorkerBuildIdCompatibility: `build_id` compatible with `existing`, the set's new
/// default; `make_set_default` also makes that set the queue's default.
pub fn addCompatible(ctx: *Context, build_id: []const u8, existing: []const u8, make_set_default: bool) Error!void {
    return updateSets(ctx, ctx.client, .{ .add_new_compatible_build_id = .{
        .new_build_id = build_id,
        .existing_compatible_build_id = existing,
        .make_set_default = make_set_default,
    } });
}

/// Sets {1.0} and {2.0, 2.1}, with 2.1 the queue's default (the suite's AddSomeVersions).
pub fn addSomeVersions(ctx: *Context) Error!void {
    try addNewDefault(ctx, "1.0");
    try addNewDefault(ctx, "2.0");
    try addCompatible(ctx, "2.1", "2.0", true);
}

/// ResetStickyTaskQueue for the run, so the next worker gets its next task.
pub fn resetStickyQueue(ctx: *Context, workflow_id: []const u8, run_id: []const u8) Error!void {
    return resetSticky(ctx, ctx.client, workflow_id, run_id);
}

/// `client` is a `*temporal.client.Client`, or a test's stand-in with its `callService`.
fn updateSets(ctx: *Context, client: anytype, operation: Request.operation_union) Error!void {
    var failure: ?temporal.client.RpcFailure = null;
    _ = client.callService(.workflow, "UpdateWorkerBuildIdCompatibility", ctx.arena, .{
        .namespace = ctx.namespace,
        .task_queue = ctx.task_queue,
        .operation = operation,
    }, &failure) catch |err| return refused(ctx, "UpdateWorkerBuildIdCompatibility", ctx.task_queue, err, failure);
}

fn resetSticky(ctx: *Context, client: anytype, workflow_id: []const u8, run_id: []const u8) Error!void {
    var failure: ?temporal.client.RpcFailure = null;
    _ = client.callService(.workflow, "ResetStickyTaskQueue", ctx.arena, .{
        .namespace = ctx.namespace,
        .execution = .{ .workflow_id = workflow_id, .run_id = run_id },
    }, &failure) catch |err| return refused(ctx, "ResetStickyTaskQueue", ctx.task_queue, err, failure);
}

fn refused(ctx: *Context, comptime method: []const u8, task_queue: []const u8, err: temporal.client.CallServiceError, failure: ?temporal.client.RpcFailure) Error {
    switch (err) {
        error.Canceled => return error.Canceled,
        error.OutOfMemory => return error.OutOfMemory,
        else => {},
    }
    if (failure) |detail| return ctx.fail(method ++ " on {s} returned error.{t}: {s}", .{ task_queue, err, detail.message });
    return ctx.fail(method ++ " on {s} returned error.{t}", .{ task_queue, err });
}

/// Records the requests it is sent and refuses each as the server does without
/// `dev_server_args`, or with `result` when set.
const TestServer = struct {
    result: ?temporal.client.CallServiceError = error.PermissionDenied,
    sets: [4]?Request.operation_union = @splat(null),
    set_count: usize = 0,
    reset: ?temporal.proto.temporal.api.common.v1.WorkflowExecution = null,

    pub fn callService(
        server: *TestServer,
        comptime service: temporal.client.Service,
        comptime method: []const u8,
        arena: std.mem.Allocator,
        request: temporal.client.ServiceRequest(service, method),
        failure: ?*?temporal.client.RpcFailure,
    ) temporal.client.CallServiceError!temporal.client.ServiceResponse(service, method) {
        if (comptime std.mem.eql(u8, method, "UpdateWorkerBuildIdCompatibility")) {
            if (!std.mem.eql(u8, request.namespace, "default") or !std.mem.eql(u8, request.task_queue, "conformance-test/build-ids"))
                return error.InvalidArgument;
            server.sets[server.set_count] = request.operation;
            server.set_count += 1;
        } else {
            server.reset = request.execution;
        }
        const err = server.result orelse return .{};
        if (failure) |out| out.* = .{
            .code = .permission_denied,
            .message = try arena.dupe(u8, "Worker versioning v0.2 (Version Set-based API) is disabled on this namespace."),
            .details = "",
        };
        return err;
    }
};

fn testContext(arena: std.mem.Allocator) Context {
    return .{
        .gpa = std.testing.allocator,
        .io = std.testing.io,
        .arena = arena,
        .client = undefined,
        .feature = "test/build-ids",
        .namespace = "default",
        .task_queue = "conformance-test/build-ids",
    };
}

test "conformance build_ids sends the version-set operations on the feature's queue" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var ctx = testContext(arena.allocator());
    var server: TestServer = .{ .result = null };
    try updateSets(&ctx, &server, .{ .add_new_build_id_in_new_default_set = "1.0" });
    try updateSets(&ctx, &server, .{ .add_new_compatible_build_id = .{ .new_build_id = "1.1", .existing_compatible_build_id = "1.0", .make_set_default = true } });
    try std.testing.expectEqual(2, server.set_count);
    try std.testing.expectEqualStrings("1.0", server.sets[0].?.add_new_build_id_in_new_default_set);
    const compatible = server.sets[1].?.add_new_compatible_build_id;
    try std.testing.expectEqualStrings("1.1", compatible.new_build_id);
    try std.testing.expectEqualStrings("1.0", compatible.existing_compatible_build_id);
    try std.testing.expect(compatible.make_set_default);

    try resetSticky(&ctx, &server, "workflow", "run");
    try std.testing.expectEqualStrings("workflow", server.reset.?.workflow_id);
    try std.testing.expectEqualStrings("run", server.reset.?.run_id);
    try std.testing.expectEqual(null, ctx.failure_message_);
}

test "conformance build_ids reports a refused call with the server's message" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var ctx = testContext(arena.allocator());
    var server: TestServer = .{};
    try std.testing.expectError(error.FeatureFailed, updateSets(&ctx, &server, .{ .add_new_build_id_in_new_default_set = "1.0" }));
    try std.testing.expectEqualStrings(
        "UpdateWorkerBuildIdCompatibility on conformance-test/build-ids returned error.PermissionDenied: Worker versioning v0.2 (Version Set-based API) is disabled on this namespace.",
        ctx.failure_message_.?,
    );
    try std.testing.expectError(error.FeatureFailed, resetSticky(&ctx, &server, "workflow", "run"));
    try std.testing.expectEqualStrings(
        "ResetStickyTaskQueue on conformance-test/build-ids returned error.PermissionDenied: Worker versioning v0.2 (Version Set-based API) is disabled on this namespace.",
        ctx.failure_message_.?,
    );
    server.result = error.Canceled;
    try std.testing.expectError(error.Canceled, updateSets(&ctx, &server, .{ .add_new_build_id_in_new_default_set = "1.0" }));
    server.result = error.OutOfMemory;
    try std.testing.expectError(error.OutOfMemory, resetSticky(&ctx, &server, "workflow", "run"));
}
