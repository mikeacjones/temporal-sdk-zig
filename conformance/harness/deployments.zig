//! Routing changes of a Worker Deployment and the waits around them, as the suite's
//! features/deployment_versioning/common.go does them. Every wait polls every 300 ms for at
//! most 10 s. The polling runs in a task of its own that is canceled at the limit, so a call
//! the server never answers cannot stretch a wait past it.
const std = @import("std");
const temporal = @import("temporal");
const Context = @import("Context.zig");
const Io = std.Io;
const Client = temporal.client.Client;
const Description = temporal.deployment.Description;

pub const Error = error{ FeatureFailed, Canceled, OutOfMemory };

const poll_interval: Io.Duration = .fromMilliseconds(300);
const wait_limit: Io.Duration = .fromSeconds(10);

/// Waits until the deployment exists and lists version `build_id`, makes it current with the
/// conflict token of the describe that listed it, then waits until the routing change has
/// reached the task queues.
pub fn setCurrent(ctx: *Context, deployment_name: []const u8, build_id: []const u8) Error!void {
    return setRouting(ctx, deployment_name, build_id, null, wait_limit);
}

/// The same for the ramping version, at `percentage` percent (0 to 100).
pub fn setRamp(ctx: *Context, deployment_name: []const u8, build_id: []const u8, percentage: f32) Error!void {
    return setRouting(ctx, deployment_name, build_id, percentage, wait_limit);
}

/// Waits until the run's status is running.
pub fn waitForWorkflowRunning(ctx: *Context, workflow_id: []const u8, run_id: []const u8) Error!void {
    return waitRunning(ctx, workflow_id, run_id, wait_limit);
}

fn setRouting(ctx: *Context, deployment_name: []const u8, build_id: []const u8, ramp: ?f32, limit: Io.Duration) Error!void {
    const what = if (ramp == null) "current" else "ramping";
    var handle = try ctx.client.workerDeployment(deployment_name);
    defer handle.deinit();

    var listed: VersionListed = .{ .handle = &handle, .build_id = build_id, .scratch = .init(ctx.gpa) };
    defer listed.scratch.deinit();
    if (!try waitUntil(ctx, limit, &listed)) {
        if (!listed.seen)
            return ctx.fail("deployment {s} did not exist within {f} (last describe error: {s})", .{ deployment_name, limit, errorName(listed.last_error) });
        return ctx.fail("deployment {s} did not list version {s} within {f}", .{ deployment_name, build_id, limit });
    }
    const options: temporal.deployment.RoutingOptions = .{ .conflict_token = listed.conflict_token };
    const changed = if (ramp) |percentage|
        handle.setRampingVersion(listed.scratch.allocator(), build_id, percentage, options)
    else
        handle.setCurrentVersion(listed.scratch.allocator(), build_id, options);
    _ = changed catch |err| switch (err) {
        error.Canceled => return error.Canceled,
        error.OutOfMemory => return error.OutOfMemory,
        else => return ctx.fail("making version {s} of deployment {s} {s} returned error.{t}", .{ build_id, deployment_name, what, err }),
    };

    var routed: RoutingDone = .{ .handle = &handle, .scratch = .init(ctx.gpa) };
    defer routed.scratch.deinit();
    if (!try waitUntil(ctx, limit, &routed)) {
        if (routed.state) |state|
            return ctx.fail("making version {s} of deployment {s} {s}: the routing update was still {t} after {f}", .{ build_id, deployment_name, what, state, limit });
        return ctx.fail("making version {s} of deployment {s} {s}: describing the deployment failed for {f} (last error: {s})", .{ build_id, deployment_name, what, limit, errorName(routed.last_error) });
    }
}

fn waitRunning(ctx: *Context, workflow_id: []const u8, run_id: []const u8, limit: Io.Duration) Error!void {
    var running: WorkflowRunning = .{ .client = ctx.client, .workflow_id = workflow_id, .run_id = run_id, .scratch = .init(ctx.gpa) };
    defer running.scratch.deinit();
    if (!try waitUntil(ctx, limit, &running)) {
        if (running.status) |status|
            return ctx.fail("workflow {s} run {s} was {t}, not running, after {f}", .{ workflow_id, run_id, status, limit });
        return ctx.fail("describing workflow {s} run {s} failed for {f} (last error: {s})", .{ workflow_id, run_id, limit, errorName(running.last_error) });
    }
}

fn errorName(err: ?anyerror) []const u8 {
    return if (err) |e| @errorName(e) else "none";
}

/// Polls the deployment until it lists the version. `conflict_token` is that description's,
/// in `scratch`.
const VersionListed = struct {
    handle: *const temporal.client.WorkerDeploymentHandle,
    build_id: []const u8,
    scratch: std.heap.ArenaAllocator,
    seen: bool = false,
    conflict_token: []const u8 = "",
    last_error: ?anyerror = null,

    fn check(w: *VersionListed) error{ Canceled, OutOfMemory }!bool {
        _ = w.scratch.reset(.retain_capacity);
        w.conflict_token = "";
        const description = w.handle.describe(w.scratch.allocator()) catch |err| return failed(&w.last_error, err);
        w.seen = true;
        for (description.versions) |summary| {
            if (std.mem.eql(u8, summary.version.build_id, w.build_id)) {
                w.conflict_token = description.conflict_token;
                return true;
            }
        }
        return false;
    }
};

/// Polls the deployment until its routing update is completed (or unspecified, as an older
/// server reports it).
const RoutingDone = struct {
    handle: *const temporal.client.WorkerDeploymentHandle,
    scratch: std.heap.ArenaAllocator,
    state: ?Description.RoutingUpdateState = null,
    last_error: ?anyerror = null,

    fn check(w: *RoutingDone) error{ Canceled, OutOfMemory }!bool {
        _ = w.scratch.reset(.retain_capacity);
        const description = w.handle.describe(w.scratch.allocator()) catch |err| return failed(&w.last_error, err);
        w.state = description.routing_update;
        return description.routing_update != .in_progress;
    }
};

const WorkflowRunning = struct {
    client: *Client,
    workflow_id: []const u8,
    run_id: []const u8,
    scratch: std.heap.ArenaAllocator,
    status: ?temporal.client.WorkflowExecutionStatus = null,
    last_error: ?anyerror = null,

    fn check(w: *WorkflowRunning) error{ Canceled, OutOfMemory }!bool {
        _ = w.scratch.reset(.retain_capacity);
        const description = w.client.describeWorkflow(w.scratch.allocator(), w.workflow_id, w.run_id) catch |err| return failed(&w.last_error, err);
        w.status = description.status;
        return description.status == .running;
    }
};

/// Records a failed call for the wait's message, to be retried; passes cancellation and
/// allocation failure through.
fn failed(last_error: *?anyerror, err: anyerror) error{ Canceled, OutOfMemory }!bool {
    switch (err) {
        error.Canceled => return error.Canceled,
        error.OutOfMemory => return error.OutOfMemory,
        else => {
            last_error.* = err;
            return false;
        },
    }
}

/// Calls `waiter.check()` every `poll_interval` until it returns true (true) or `limit` has
/// passed (false). The checks run in a task that is canceled at the limit; when this returns,
/// that task has ended and `waiter` is this task's again.
fn waitUntil(ctx: *Context, limit: Io.Duration, waiter: anytype) Error!bool {
    const W = @TypeOf(waiter.*);
    const io = ctx.io;
    const deadline = Io.Clock.Timestamp.now(io, .boot).addDuration(.{ .raw = limit, .clock = .boot });
    const Result = union(enum) { done: error{ Canceled, OutOfMemory }!void, timeout: Io.Cancelable!void };
    var buffer: [2]Result = undefined;
    var race: Io.Select(Result) = .init(io, &buffer);
    defer race.cancelDiscard();
    race.concurrent(.timeout, Io.Clock.Timestamp.wait, .{ deadline, io }) catch |err|
        return ctx.fail("cannot start the {f} limit of a deployment wait: error.{t}", .{ limit, err });
    race.concurrent(.done, struct {
        fn poll(w: *W, poll_io: Io) error{ Canceled, OutOfMemory }!void {
            while (!try w.check()) try poll_io.sleep(poll_interval, .boot);
        }
    }.poll, .{ waiter, io }) catch |err|
        return ctx.fail("cannot start polling for a deployment wait: error.{t}", .{err});
    switch (try race.await()) {
        .done => |result| {
            try result;
            return true;
        },
        .timeout => |result| {
            try result;
            return false;
        },
    }
}

const TestServer = struct {
    /// Answers DescribeWorkerDeployment with NotFound, and DescribeWorkflowExecution by
    /// waiting until canceled, as a server that never answers would.
    calls: std.atomic.Value(usize) = .init(0),

    pub fn call(fixture: *@This(), request: *temporal.interceptor.ClientCall, _: temporal.interceptor.Client.Next) temporal.interceptor.ClientError!void {
        const service = temporal.proto.temporal.api.workflowservice.v1;
        _ = fixture.calls.fetchAdd(1, .monotonic);
        if (request.request(service.DescribeWorkerDeploymentRequest) != null) return error.NotFound;
        if (request.request(service.DescribeWorkflowExecutionRequest) != null) {
            var never: Io.Event = .unset;
            never.waitTimeout(std.testing.io, .{ .duration = .{ .raw = .fromSeconds(30), .clock = .boot } }) catch |err|
                return if (err == error.Canceled) error.Canceled else error.RpcFailed;
            return error.InvalidResponse;
        }
        return error.InvalidResponse;
    }
};

fn testContext(client: *Client, arena: std.mem.Allocator) Context {
    return .{
        .gpa = std.testing.allocator,
        .io = std.testing.io,
        .arena = arena,
        .client = client,
        .feature = "test/deployments",
        .namespace = "default",
        .task_queue = "conformance-test/deployments",
    };
}

fn expectTookAbout(io: Io, started: Io.Clock.Timestamp, limit: Io.Duration) !void {
    const took = started.durationTo(Io.Clock.Timestamp.now(io, .boot)).raw;
    try std.testing.expect(took.nanoseconds >= limit.nanoseconds);
    try std.testing.expect(took.nanoseconds < limit.nanoseconds + 2 * poll_interval.nanoseconds);
}

test "conformance deployments report a deployment that never appears, at the limit" {
    const gpa = std.testing.allocator;
    const io = std.testing.io;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    var fixture: TestServer = .{};
    var client = try Client.init(gpa, io, .{ .interceptors = &.{temporal.interceptor.Client.from(TestServer, &fixture)} });
    defer client.close();
    var ctx = testContext(&client, arena.allocator());
    const limit: Io.Duration = .fromSeconds(1);
    const started = Io.Clock.Timestamp.now(io, .boot);
    try std.testing.expectError(error.FeatureFailed, setRouting(&ctx, "missing", "1.0", null, limit));
    try expectTookAbout(io, started, limit);
    try std.testing.expectEqualStrings("deployment missing did not exist within 1s (last describe error: NotFound)", ctx.failure_message_.?);
    try std.testing.expect(fixture.calls.load(.monotonic) >= 3);
}

test "conformance deployments end a wait whose call never answers at the limit" {
    const gpa = std.testing.allocator;
    const io = std.testing.io;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    var fixture: TestServer = .{};
    var client = try Client.init(gpa, io, .{ .interceptors = &.{temporal.interceptor.Client.from(TestServer, &fixture)} });
    defer client.close();
    var ctx = testContext(&client, arena.allocator());
    const limit: Io.Duration = .fromSeconds(1);
    const started = Io.Clock.Timestamp.now(io, .boot);
    try std.testing.expectError(error.FeatureFailed, waitRunning(&ctx, "workflow", "run", limit));
    try expectTookAbout(io, started, limit);
    try std.testing.expectEqualStrings("describing workflow workflow run run failed for 1s (last error: none)", ctx.failure_message_.?);
}
