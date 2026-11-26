//! Tests for the opentelemetry-v2 sample. Each part has a workflow type `Workflow`, so each
//! test runs one part on an environment of its own. The environments trace through the
//! sample's OpenTelemetry setup with the OpenTelemetry SDK turned off, so nothing is
//! exported (the default exporter writes to stdout, which is the test runner's channel to the
//! build runner) while every span still starts and carries its context. `getResult` replays
//! each closed run's history, so the workflow spans also pass through replay.

const std = @import("std");
const temporal = @import("temporal");
const sample = @import("opentelemetry-v2.zig");

const testing = std.testing;
const WorkflowEnvironment = temporal.testing.WorkflowEnvironment;

/// A test environment whose client and worker trace through `tracing`.
const Traced = struct {
    tracing: *sample.setup.Tracing,
    env: *WorkflowEnvironment,
    interceptors: [1]temporal.interceptor.Client,
    worker_interceptors: [1]temporal.interceptor.Worker,
    propagators: [1]temporal.propagation.ContextPropagator,

    fn start(t: *Traced) !void {
        var environ: std.process.Environ.Map = .init(testing.allocator);
        defer environ.deinit();
        try environ.put("OTEL_SDK_DISABLED", "true");
        t.tracing = try sample.setup.Tracing.create(testing.allocator, testing.io, &environ, .{ .scope_name = "temporal-sdk-zig/samples/opentelemetry-v2/test" });
        errdefer t.tracing.destroy();
        t.interceptors = .{t.tracing.interceptor.client()};
        t.worker_interceptors = .{t.tracing.interceptor.worker()};
        t.propagators = .{t.tracing.interceptor.propagator()};
        t.env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{
            .interceptors = &t.interceptors,
            .worker_interceptors = &t.worker_interceptors,
            .propagators = &t.propagators,
        });
    }

    /// The environment first: the tracing setup must outlive its client and worker.
    fn deinit(t: *Traced) void {
        t.env.deinit();
        t.tracing.destroy();
    }
};

test "automatic instrumentation: the workflow returns the activity's greeting" {
    var t: Traced = undefined;
    try t.start();
    defer t.deinit();
    const part = sample.automatic_instrumentation;
    try t.env.registerWorkflow(part.Workflow);
    try t.env.registerActivity(part.Activity);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    try testing.expectEqualStrings("Hello, Temporal!", try t.env.executeWorkflow(part.Workflow, .{"Temporal"}, arena.allocator(), null));
}

test "workflow-activity propagation: the activity runs inside the workflow's span and greets" {
    var t: Traced = undefined;
    try t.start();
    defer t.deinit();
    const part = sample.workflow_activity_propagation;
    var activities: part.Activities = .{ .gpa = testing.allocator, .tracer = t.tracing.tracer };
    try t.env.registerWorkflow(part.Workflow);
    try t.env.registerActivityWith(part.Activity, &activities);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    try testing.expectEqualStrings("Hello, Temporal!", try t.env.executeWorkflow(part.Workflow, .{"Temporal"}, arena.allocator(), null));
}

test "client update propagation: an empty name is rejected, and the accepted greeting is the workflow's result" {
    var t: Traced = undefined;
    try t.start();
    defer t.deinit();
    const part = sample.client_update_propagation;
    try t.env.registerWorkflow(part.Workflow);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var run = try t.env.startWorkflow(part.Workflow, .{}, .{});
    defer run.deinit();

    var rejection: ?temporal.failure.Failure = null;
    try testing.expectError(error.UpdateFailed, t.env.updateWorkflow(part.Greet, a, run.id, .{""}, &rejection));
    try testing.expectEqualStrings("EmptyName", rejection.?.message);

    try testing.expectEqualStrings("Hello, Temporal!", try t.env.updateWorkflow(part.Greet, a, run.id, .{"Temporal"}, null));
    try testing.expectEqualStrings("Hello, Temporal!", try t.env.getResult(part.Workflow, &run, a, null));
}
