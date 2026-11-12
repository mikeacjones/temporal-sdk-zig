//! Tests for the context propagation sample: the value set when the workflow starts reaches
//! the workflow and its activity, carried in the `custom-header` header field.

const std = @import("std");
const temporal = @import("temporal");
const ctxpropagation = @import("ctxpropagation.zig");

const testing = std.testing;
const interceptor = temporal.interceptor;
const Values = ctxpropagation.Values;

/// A worker interceptor that records the propagated value each workflow run and activity
/// attempt sees as it starts. Runs on worker threads, so it copies the value into `arena`
/// under `mutex`.
const Recorder = struct {
    io: std.Io,
    arena: std.mem.Allocator,
    mutex: std.Io.Mutex = .init,
    workflow: ?Values = null,
    activity: ?Values = null,

    pub fn interceptWorkflow(r: *Recorder, ctx: *temporal.workflow.Context) error{OutOfMemory}!interceptor.Workflow {
        try r.record(&r.workflow, ctx.propagated().get(ctxpropagation.propagate_key));
        return .{};
    }

    pub fn interceptActivity(r: *Recorder, ctx: *temporal.activity.Context) error{OutOfMemory}!interceptor.Activity {
        try r.record(&r.activity, ctx.propagated().get(ctxpropagation.propagate_key));
        return .{};
    }

    fn record(r: *Recorder, slot: *?Values, seen: ?Values) error{OutOfMemory}!void {
        const v = seen orelse return;
        r.mutex.lockUncancelable(r.io);
        defer r.mutex.unlock(r.io);
        slot.* = .{ .key = try r.arena.dupe(u8, v.key), .value = try r.arena.dupe(u8, v.value) };
    }

    fn get(r: *Recorder, slot: *const ?Values) ?Values {
        r.mutex.lockUncancelable(r.io);
        defer r.mutex.unlock(r.io);
        return slot.*;
    }
};

/// A client interceptor that keeps the propagated value from the header of the first
/// StartWorkflowExecution request.
const StartHeader = struct {
    arena: std.mem.Allocator,
    values: ?Values = null,

    pub fn call(s: *StartHeader, input: *interceptor.ClientCall, next: interceptor.Client.Next) interceptor.ClientError!void {
        if (s.values == null and std.mem.eql(u8, input.method, "StartWorkflowExecution")) {
            const header = input.header.?.header();
            s.values = header.value(Values, s.arena, ctxpropagation.header_key) catch return error.InvalidResponse;
        }
        return next.call(input);
    }
};

test "the propagated value reaches the workflow and its activity" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var recorder: Recorder = .{ .io = testing.io, .arena = a };
    const worker_interceptors = [_]interceptor.Worker{.from(Recorder, &recorder)};
    var tracer: ctxpropagation.Tracer = .{ .service_name = "ctx-propagation-test" };
    var start_header: StartHeader = .{ .arena = a };
    const client_interceptors = [_]interceptor.Client{ tracer.client(), .from(StartHeader, &start_header) };
    const propagators = [_]temporal.propagation.ContextPropagator{ctxpropagation.propagator};

    const env = try temporal.testing.WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{
        .propagators = &propagators,
        .worker_interceptors = &worker_interceptors,
        .interceptors = &client_interceptors,
    });
    defer env.deinit();
    try env.registerWorkflow(ctxpropagation.CtxPropWorkflow);
    try env.registerActivity(ctxpropagation.SampleActivity);

    var values: temporal.propagation.Values = .init(a);
    try values.put(ctxpropagation.propagate_key, .{ .key = "some key", .value = "some value" });
    try env.executeWorkflowWithOptions(ctxpropagation.CtxPropWorkflow, .{ .task_queue = "", .context = &values }, .{}, a, null);

    const sent = start_header.values orelse return error.TestNoHeaderSent;
    try testing.expectEqualStrings("some key", sent.key);
    try testing.expectEqualStrings("some value", sent.value);

    const in_workflow = recorder.get(&recorder.workflow) orelse return error.TestNotPropagatedToWorkflow;
    try testing.expectEqualStrings("some key", in_workflow.key);
    try testing.expectEqualStrings("some value", in_workflow.value);

    const in_activity = recorder.get(&recorder.activity) orelse return error.TestNotPropagatedToActivity;
    try testing.expectEqualStrings("some key", in_activity.key);
    try testing.expectEqualStrings("some value", in_activity.value);
}

test "the activity returns the propagated value, or null without one" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const propagators = [_]temporal.propagation.ContextPropagator{ctxpropagation.propagator};

    // On a dev server the activity's own result shows what it saw.
    {
        const env = try temporal.testing.WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{ .propagators = &propagators });
        defer env.deinit();
        try env.registerWorkflow(ActivityResult);
        try env.registerActivity(ctxpropagation.SampleActivity);

        var values: temporal.propagation.Values = .init(a);
        try values.put(ctxpropagation.propagate_key, .{ .key = "some key", .value = "some value" });
        const with = try env.executeWorkflowWithOptions(ActivityResult, .{ .task_queue = "", .context = &values }, .{}, a, null);
        try testing.expectEqualStrings("some key", with.?.key);
        try testing.expectEqualStrings("some value", with.?.value);

        const without = try env.executeWorkflow(ActivityResult, .{}, a, null);
        try testing.expectEqual(@as(?Values, null), without);
    }

    const activity_env: temporal.testing.ActivityEnvironment = .init(testing.io);
    try testing.expectEqual(@as(?Values, null), try activity_env.executeActivity(ctxpropagation.SampleActivity, .{}, a));
}

/// Runs `SampleActivity` and returns what it returned.
fn activityResult(ctx: *temporal.workflow.Context) !?Values {
    return ctx.executeActivity(ctxpropagation.SampleActivity, .{ .start_to_close_timeout = .fromSeconds(10) }, .{}).get();
}

const ActivityResult = temporal.workflow.Def("ActivityResult", activityResult);
