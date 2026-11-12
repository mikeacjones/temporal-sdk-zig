//! Tests for the workflow-security-interceptor sample.

const std = @import("std");
const temporal = @import("temporal");
const security_interceptor = @import("security_interceptor.zig");

const testing = std.testing;
const WorkflowEnvironment = temporal.testing.WorkflowEnvironment;

fn startEnvironment(security: *security_interceptor.WorkerInterceptor, interceptors: *[1]temporal.interceptor.Worker) !*WorkflowEnvironment {
    interceptors.* = .{.from(security_interceptor.WorkerInterceptor, security)};
    const env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{ .worker_interceptors = interceptors });
    errdefer env.deinit();
    try env.registerWorkflow(security_interceptor.Workflow);
    try env.registerWorkflow(security_interceptor.ChildWorkflow);
    try env.registerWorkflow(security_interceptor.ProhibitedChildWorkflow);
    return env;
}

test "the interceptor refuses the prohibited child workflow" {
    var security: security_interceptor.WorkerInterceptor = .{};
    var interceptors: [1]temporal.interceptor.Worker = undefined;
    const env = try startEnvironment(&security, &interceptors);
    defer env.deinit();
    try env.registerActivity(security_interceptor.ValidateChildWorkflowTypeActivity);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var failure: ?temporal.failure.Failure = null;
    try testing.expectError(error.WorkflowFailed, env.executeWorkflow(security_interceptor.Workflow, .{}, arena.allocator(), &failure));
    const cause = temporal.failure.rootCause(&failure.?);
    try testing.expectEqualStrings("Child workflow type \"ProhibitedChildWorkflow\" not allowed", cause.message);
    try testing.expectEqualStrings("not-allowed", temporal.failure.applicationType(cause.*).?);
}

fn allowAll(_: *temporal.activity.Context, _: []const u8) !bool {
    return true;
}

test "the interceptor starts every child workflow the activity allows" {
    var security: security_interceptor.WorkerInterceptor = .{};
    var interceptors: [1]temporal.interceptor.Worker = undefined;
    const env = try startEnvironment(&security, &interceptors);
    defer env.deinit();
    try env.registerActivityAs(security_interceptor.ValidateChildWorkflowTypeActivity, allowAll);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    try env.executeWorkflow(security_interceptor.Workflow, .{}, arena.allocator(), null);
}

fn validationFails(ctx: *temporal.activity.Context, _: []const u8) !bool {
    return ctx.fail(.{ .message = "policy store unavailable", .non_retryable = true });
}

test "a failed validation refuses the child workflow" {
    var security: security_interceptor.WorkerInterceptor = .{};
    var interceptors: [1]temporal.interceptor.Worker = undefined;
    const env = try startEnvironment(&security, &interceptors);
    defer env.deinit();
    try env.registerActivityAs(security_interceptor.ValidateChildWorkflowTypeActivity, validationFails);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var failure: ?temporal.failure.Failure = null;
    try testing.expectError(error.WorkflowFailed, env.executeWorkflow(security_interceptor.Workflow, .{}, arena.allocator(), &failure));
    try testing.expectEqualStrings(
        "Validating child workflow type \"ChildWorkflow\" failed: policy store unavailable",
        temporal.failure.rootCause(&failure.?).message,
    );
}

test "the validation activity approves only ChildWorkflow" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const env: temporal.testing.ActivityEnvironment = .init(testing.io);
    try testing.expect(try env.executeActivity(security_interceptor.ValidateChildWorkflowTypeActivity, .{"ChildWorkflow"}, arena.allocator()));
    try testing.expect(!try env.executeActivity(security_interceptor.ValidateChildWorkflowTypeActivity, .{"ProhibitedChildWorkflow"}, arena.allocator()));
}
