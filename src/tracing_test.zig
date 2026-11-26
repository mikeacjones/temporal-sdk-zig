//! The tracing interceptor against a dev server, with an in-memory tracer: spans chain from a
//! client call through workflows, activities, children and Nexus operations, handler spans
//! descend from the client calls that reached them, and replay starts no span.

const std = @import("std");
const activity = @import("activity.zig");
const workflow = @import("workflow.zig");
const interceptor = @import("interceptor.zig");
const nexus = @import("nexus.zig");
const propagation = @import("propagation.zig");
const tracing = @import("tracing.zig");
const testing_mod = @import("testing.zig");
const WorkflowEnvironment = testing_mod.WorkflowEnvironment;
const Allocator = std.mem.Allocator;
const testing = std.testing;

/// Records every span: its name, its parent's ID (the parent context's `span` field), its
/// tags and how it finished. A span's context is `{"span": "<id>"}`, IDs counting from 1.
const RecordingTracer = struct {
    mutex: std.Io.Mutex = .init,
    io: std.Io,
    arena: std.heap.ArenaAllocator,
    spans: std.ArrayList(Recorded) = .empty,
    /// `finishSpan` calls for a span already finished, or one never started.
    bad_finishes: u32 = 0,

    const Recorded = struct {
        name: []const u8,
        kind: tracing.Kind,
        parent: ?[]const u8,
        tags: []const tracing.Field,
        finished: bool = false,
        failure: ?[]const u8 = null,
    };

    fn init() RecordingTracer {
        return .{ .io = testing.io, .arena = .init(testing.allocator) };
    }

    fn deinit(r: *RecordingTracer) void {
        r.arena.deinit();
    }

    pub fn startSpan(r: *RecordingTracer, arena: Allocator, options: tracing.StartSpanOptions) error{OutOfMemory}!tracing.Span {
        r.mutex.lockUncancelable(r.io);
        defer r.mutex.unlock(r.io);
        const own = r.arena.allocator();
        const parent = if (options.parent) |p| p.get("span") else null;
        const tags = try own.alloc(tracing.Field, options.tags.len);
        for (tags, options.tags) |*t, o| t.* = .{ .name = try own.dupe(u8, o.name), .value = try own.dupe(u8, o.value) };
        try r.spans.append(own, .{
            .name = try options.spanName(own),
            .kind = options.operation.kind(),
            .parent = if (parent) |p| try own.dupe(u8, p) else null,
            .tags = tags,
        });
        const id = r.spans.items.len;
        const fields = try arena.alloc(tracing.Field, 1);
        fields[0] = .{ .name = "span", .value = try std.fmt.allocPrint(arena, "{d}", .{id}) };
        return .{ .handle = @ptrFromInt(id), .context = .{ .fields = fields } };
    }

    pub fn finishSpan(r: *RecordingTracer, span: tracing.Span, options: tracing.FinishSpanOptions) void {
        r.mutex.lockUncancelable(r.io);
        defer r.mutex.unlock(r.io);
        const id = @intFromPtr(span.handle);
        if (id == 0 or id > r.spans.items.len or r.spans.items[id - 1].finished) {
            r.bad_finishes += 1;
            return;
        }
        const s = &r.spans.items[id - 1];
        s.finished = true;
        if (options.failure) |f| s.failure = r.arena.allocator().dupe(u8, f) catch "?";
    }

    fn count(r: *RecordingTracer) usize {
        r.mutex.lockUncancelable(r.io);
        defer r.mutex.unlock(r.io);
        return r.spans.items.len;
    }

    /// The ID of the first span called `name`, or an error that lists the spans.
    fn find(r: *RecordingTracer, name: []const u8) !usize {
        r.mutex.lockUncancelable(r.io);
        defer r.mutex.unlock(r.io);
        for (r.spans.items, 1..) |s, id| if (std.mem.eql(u8, s.name, name)) return id;
        std.debug.print("no span {s}; the spans are:\n", .{name});
        r.dumpLocked();
        return error.TestSpanMissing;
    }

    fn get(r: *RecordingTracer, id: usize) Recorded {
        r.mutex.lockUncancelable(r.io);
        defer r.mutex.unlock(r.io);
        return r.spans.items[id - 1];
    }

    /// Checks that span `child` has the span called `parent` as its parent, and returns the
    /// child's ID.
    fn expectChild(r: *RecordingTracer, child: []const u8, parent: []const u8) !usize {
        const c = try r.find(child);
        const p = try r.find(parent);
        const want = try std.fmt.allocPrint(testing.allocator, "{d}", .{p});
        defer testing.allocator.free(want);
        const got = r.get(c).parent orelse "none";
        if (!std.mem.eql(u8, got, want)) {
            std.debug.print("span {s} has parent {s}, not {s} ({s}); the spans are:\n", .{ child, got, want, parent });
            r.mutex.lockUncancelable(r.io);
            defer r.mutex.unlock(r.io);
            r.dumpLocked();
            return error.TestWrongParent;
        }
        return c;
    }

    fn expectAllFinished(r: *RecordingTracer) !void {
        r.mutex.lockUncancelable(r.io);
        defer r.mutex.unlock(r.io);
        try testing.expectEqual(@as(u32, 0), r.bad_finishes);
        for (r.spans.items) |s| if (!s.finished) {
            std.debug.print("span {s} was never finished\n", .{s.name});
            return error.TestSpanUnfinished;
        };
    }

    fn dumpLocked(r: *RecordingTracer) void {
        for (r.spans.items, 1..) |s, id| std.debug.print("  {d} {s} parent={s} finished={}\n", .{ id, s.name, s.parent orelse "none", s.finished });
    }
};

fn tagValue(tags: []const tracing.Field, name: []const u8) ?[]const u8 {
    for (tags) |t| if (std.mem.eql(u8, t.name, name)) return t.value;
    return null;
}

/// A tracer, its interceptor and an environment wired with the interceptor's three options.
const Setup = struct {
    tracer: RecordingTracer,
    tracing: tracing.Interceptor,
    client_interceptors: [1]interceptor.Client,
    worker_interceptors: [1]interceptor.Worker,
    propagators: [1]propagation.ContextPropagator,
    env: *WorkflowEnvironment,
    arena: std.heap.ArenaAllocator,

    /// Allocated so the interceptor and the option slices stay at one address.
    fn start() !*Setup {
        const s = try testing.allocator.create(Setup);
        errdefer testing.allocator.destroy(s);
        s.tracer = .init();
        errdefer s.tracer.deinit();
        s.tracing = .init(tracing.Tracer.from(RecordingTracer, &s.tracer));
        s.client_interceptors = .{s.tracing.client()};
        s.worker_interceptors = .{s.tracing.worker()};
        s.propagators = .{s.tracing.propagator()};
        s.env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{
            .interceptors = &s.client_interceptors,
            .worker_interceptors = &s.worker_interceptors,
            .propagators = &s.propagators,
        });
        s.arena = .init(testing.allocator);
        return s;
    }

    fn deinit(s: *Setup) void {
        s.env.deinit();
        s.arena.deinit();
        s.tracer.deinit();
        testing.allocator.destroy(s);
    }
};

/// The `span` field of the current span in `values`, or "none".
fn currentSpan(values: *const propagation.Values) []const u8 {
    const c = values.get(tracing.span_key) orelse return "none";
    return c.get("span") orelse "none";
}

const Fixtures = struct {
    fn hello(ctx: *activity.Context, name: []const u8) ![]const u8 {
        return std.fmt.allocPrint(ctx.allocator(), "{s} in span {s}", .{ name, currentSpan(ctx.propagated()) });
    }
    const Hello = activity.Def("TracedHello", hello);
    const LocalHello = activity.Def("TracedLocalHello", hello);

    fn leaf(ctx: *workflow.Context) ![]const u8 {
        return ctx.allocator().dupe(u8, "leaf");
    }
    const Leaf = workflow.Def("TracedLeaf", leaf);

    /// Runs an activity, a local activity and a child, and returns what the activities saw
    /// as their current span.
    fn chain(ctx: *workflow.Context) ![]const u8 {
        const remote = try ctx.executeActivity(Hello, .{ .start_to_close_timeout = .fromSeconds(30) }, .{"remote"}).get();
        const local = try ctx.executeLocalActivity(LocalHello, .{ .start_to_close_timeout = .fromSeconds(30) }, .{"local"}).get();
        const child = try ctx.executeChildWorkflow(Leaf, .{}, .{}).get();
        return std.fmt.allocPrint(ctx.allocator(), "{s}|{s}|{s}", .{ remote, local, child });
    }
    const Chain = workflow.Def("TracedChain", chain);

    const Go = workflow.SignalDef("go", fn () void);
    const Status = workflow.QueryDef("status", fn () []const u8);
    const Set = workflow.UpdateDef("set", fn ([]const u8) []const u8);

    const State = struct {
        went: bool = false,
        value: ?[]const u8 = null,
        fn go(s: *State) void {
            s.went = true;
        }
        fn status(s: *State) []const u8 {
            return if (s.went) "went" else "waiting";
        }
        fn set(s: *State, v: []const u8) []const u8 {
            s.value = v;
            return v;
        }
        fn check(_: *State, v: []const u8) error{Invalid}!void {
            if (v.len == 0) return error.Invalid;
        }
        fn done(s: *State) bool {
            return s.went and s.value != null;
        }
    };

    fn handled(ctx: *workflow.Context) ![]const u8 {
        const s = try ctx.allocator().create(State);
        s.* = .{};
        try ctx.setSignalHandler(Go, s, State.go);
        try ctx.setQueryHandler(Status, s, State.status);
        try ctx.setUpdateHandlerWithValidator(Set, s, State.set, State.check);
        try ctx.await(s, State.done);
        return s.value.?;
    }
    const Handled = workflow.Def("TracedHandled", handled);
};

test "tracing: a span chain runs from the client's start through the workflow to its activities and child, and replay adds no span" {
    const s = try Setup.start();
    defer s.deinit();
    try s.env.registerWorkflow(Fixtures.Chain);
    try s.env.registerWorkflow(Fixtures.Leaf);
    try s.env.registerActivity(Fixtures.Hello);
    try s.env.registerActivity(Fixtures.LocalHello);

    var run = try s.env.startWorkflow(Fixtures.Chain, .{}, .{});
    defer run.deinit();
    // getResult also replays the finished history with the interceptor.
    const result = try s.env.getResult(Fixtures.Chain, &run, s.arena.allocator(), null);

    const t = &s.tracer;
    try testing.expectEqual(@as(?[]const u8, null), t.get(try t.find("StartWorkflow:TracedChain")).parent);
    _ = try t.expectChild("RunWorkflow:TracedChain", "StartWorkflow:TracedChain");
    _ = try t.expectChild("StartActivity:TracedHello", "RunWorkflow:TracedChain");
    const run_activity = try t.expectChild("RunActivity:TracedHello", "StartActivity:TracedHello");
    _ = try t.expectChild("StartActivity:TracedLocalHello", "RunWorkflow:TracedChain");
    const run_local = try t.expectChild("RunActivity:TracedLocalHello", "StartActivity:TracedLocalHello");
    _ = try t.expectChild("StartChildWorkflow:TracedLeaf", "RunWorkflow:TracedChain");
    _ = try t.expectChild("RunWorkflow:TracedLeaf", "StartChildWorkflow:TracedLeaf");

    // Each activity's current span is its own RunActivity span.
    const want = try std.fmt.allocPrint(s.arena.allocator(), "remote in span {d}|local in span {d}|leaf", .{ run_activity, run_local });
    try testing.expectEqualStrings(want, result);

    const started = t.get(try t.find("StartWorkflow:TracedChain"));
    try testing.expectEqual(tracing.Kind.client, started.kind);
    try testing.expectEqualStrings(run.id, tagValue(started.tags, "temporalWorkflowID").?);
    const ran = t.get(try t.find("RunWorkflow:TracedChain"));
    try testing.expectEqual(tracing.Kind.server, ran.kind);
    try testing.expectEqualStrings(run.run_id, tagValue(ran.tags, "temporalRunID").?);
    try testing.expect(tagValue(t.get(run_activity).tags, "temporalActivityID") != null);
    try testing.expectEqual(@as(?[]const u8, null), ran.failure);

    try testing.expectEqual(@as(usize, 8), t.count());
    try t.expectAllFinished();
    try s.env.replayRun(run.id, run.run_id);
    try testing.expectEqual(@as(usize, 8), t.count());
}

test "tracing: signal, query and update handler spans descend from the client calls that sent them" {
    const s = try Setup.start();
    defer s.deinit();
    try s.env.registerWorkflow(Fixtures.Handled);
    const a = s.arena.allocator();

    var run = try s.env.startWorkflow(Fixtures.Handled, .{}, .{});
    defer run.deinit();
    try testing.expectEqualStrings("waiting", try s.env.queryWorkflow(Fixtures.Status, a, run.id, .{}));
    try testing.expectEqualStrings("v", try s.env.updateWorkflow(Fixtures.Set, a, run.id, .{"v"}, null));
    try s.env.signalWorkflow(Fixtures.Go, run.id, .{});
    try testing.expectEqualStrings("v", try s.env.getResult(Fixtures.Handled, &run, a, null));

    const t = &s.tracer;
    _ = try t.expectChild("HandleQuery:status", "QueryWorkflow:status");
    _ = try t.expectChild("ValidateUpdate:set", "UpdateWorkflow:set");
    const handled = try t.expectChild("HandleUpdate:set", "UpdateWorkflow:set");
    _ = try t.expectChild("HandleSignal:go", "SignalWorkflow:go");
    const update_id = tagValue(t.get(try t.find("UpdateWorkflow:set")).tags, "temporalUpdateID").?;
    try testing.expect(update_id.len > 0);
    try testing.expectEqualStrings(update_id, tagValue(t.get(handled).tags, "temporalUpdateID").?);
    try t.expectAllFinished();

    const before = t.count();
    try s.env.replayRun(run.id, run.run_id);
    try testing.expectEqual(before, t.count());
}

test "tracing: a span context the caller puts under span_key is the parent of the client's span" {
    const s = try Setup.start();
    defer s.deinit();
    try s.env.registerWorkflow(Fixtures.Leaf);
    const fields = [_]tracing.Field{.{ .name = "span", .value = "caller" }};
    var values: propagation.Values = .init(s.arena.allocator());
    try values.put(tracing.span_key, .{ .fields = &fields });

    var run = try s.env.startWorkflow(Fixtures.Leaf, .{ .context = &values }, .{});
    defer run.deinit();
    _ = try s.env.getResult(Fixtures.Leaf, &run, s.arena.allocator(), null);
    const t = &s.tracer;
    try testing.expectEqualStrings("caller", t.get(try t.find("StartWorkflow:TracedLeaf")).parent.?);
    _ = try t.expectChild("RunWorkflow:TracedLeaf", "StartWorkflow:TracedLeaf");
}

const Echo = nexus.OperationDef("echo", []const u8, []const u8);
const Service = nexus.ServiceDef("zig-tracing-service", .{Echo});

fn echo(ctx: *nexus.Context, input: []const u8) ![]const u8 {
    return std.fmt.allocPrint(ctx.allocator(), "{s} in span {s}", .{ input, currentSpan(ctx.propagated()) });
}
const EchoHandler = nexus.ServiceHandler(Service, .{nexus.OperationHandler(Echo, echo)});

fn callEcho(ctx: *workflow.Context, endpoint: []const u8) ![]const u8 {
    return ctx.executeNexusOperation(Service, Echo, .{ .endpoint = endpoint, .schedule_to_close_timeout = .fromSeconds(60) }, "hi").get();
}
const CallEcho = workflow.Def("TracedCallEcho", callEcho);

test "tracing: a Nexus handler's span descends from the workflow's StartNexusOperation span" {
    const s = try Setup.start();
    defer s.deinit();
    try s.env.registerWorkflow(CallEcho);
    try s.env.registerNexusService(EchoHandler);
    var suffix: [4]u8 = undefined;
    testing.io.random(&suffix);
    const endpoint = try std.fmt.allocPrint(s.arena.allocator(), "zig-tracing-{x}", .{suffix});
    try s.env.createNexusEndpoint(endpoint, .{});

    var run = try s.env.startWorkflow(CallEcho, .{}, .{endpoint});
    defer run.deinit();
    const result = try s.env.getResult(CallEcho, &run, s.arena.allocator(), null);

    const t = &s.tracer;
    _ = try t.expectChild("StartNexusOperation:zig-tracing-service/echo", "RunWorkflow:TracedCallEcho");
    const handler = try t.expectChild("RunStartNexusOperationHandler:zig-tracing-service/echo", "StartNexusOperation:zig-tracing-service/echo");
    try testing.expectEqualStrings(try std.fmt.allocPrint(s.arena.allocator(), "hi in span {d}", .{handler}), result);
    try t.expectAllFinished();

    const before = t.count();
    try s.env.replayRun(run.id, run.run_id);
    try testing.expectEqual(before, t.count());
}

const wfs_probe = @import("proto.zig").temporal.api.workflowservice.v1;

/// Answers the first UpdateWorkflowExecution request as the server does when its long poll
/// runs out before the update reaches the stage the client waits for: admitted, no outcome.
/// The client then sends the request again, as it does for a long-running update.
const FirstUpdatePollTimesOut = struct {
    polls: u32 = 0,

    fn call(ptr: *anyopaque, c: *interceptor.ClientCall, next: interceptor.Client.Next) interceptor.ClientError!void {
        const f: *FirstUpdatePollTimesOut = @ptrCast(@alignCast(ptr));
        try next.call(c);
        if (c.response(wfs_probe.UpdateWorkflowExecutionResponse)) |r| {
            f.polls += 1;
            if (f.polls == 1) {
                r.outcome = null;
                r.stage = .UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ADMITTED;
            }
        }
    }
    const vtable: interceptor.Client.VTable = .{ .call = call };
};

test "tracing: every UpdateWorkflow span of an update the client polls again has the caller's context as its parent" {
    const s = try testing.allocator.create(Setup);
    defer testing.allocator.destroy(s);
    s.tracer = .init();
    defer s.tracer.deinit();
    s.tracing = .init(tracing.Tracer.from(RecordingTracer, &s.tracer));
    var stall: FirstUpdatePollTimesOut = .{};
    const client_interceptors = [_]interceptor.Client{ s.tracing.client(), .{ .ptr = &stall, .vtable = &FirstUpdatePollTimesOut.vtable } };
    s.worker_interceptors = .{s.tracing.worker()};
    s.propagators = .{s.tracing.propagator()};
    s.env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{
        .interceptors = &client_interceptors,
        .worker_interceptors = &s.worker_interceptors,
        .propagators = &s.propagators,
    });
    defer s.env.deinit();
    s.arena = .init(testing.allocator);
    defer s.arena.deinit();
    try s.env.registerWorkflow(Fixtures.Handled);
    const a = s.arena.allocator();

    var run = try s.env.startWorkflow(Fixtures.Handled, .{}, .{});
    defer run.deinit();
    const fields = [_]tracing.Field{.{ .name = "span", .value = "caller" }};
    var values: propagation.Values = .init(a);
    try values.put(tracing.span_key, .{ .fields = &fields });
    var handle = try s.env.client.updateWorkflow(Fixtures.Set, run.id, "", .{"v"}, .{ .context = &values });
    defer handle.deinit();
    try testing.expectEqualStrings("v", try handle.get(a));
    try testing.expectEqual(@as(u32, 2), stall.polls);

    // Each request the client sends for the update is a span of its own, and each is the
    // caller's child: a later poll is not a child of the one before it.
    const t = &s.tracer;
    t.mutex.lockUncancelable(t.io);
    defer t.mutex.unlock(t.io);
    var update_spans: usize = 0;
    for (t.spans.items) |span| if (std.mem.eql(u8, span.name, "UpdateWorkflow:set")) {
        update_spans += 1;
        try testing.expectEqualStrings("caller", span.parent orelse "none");
    };
    try testing.expectEqual(@as(usize, 2), update_spans);
}

const worker_probe = @import("worker.zig");
fn runWorkerProbe(w: *worker_probe.Worker) worker_probe.Worker.RunError!void {
    return w.run();
}

/// Waits, by following the run's history, until its first workflow task has completed.
fn awaitFirstTaskCompleted(client: anytype, workflow_id: []const u8, run_id: []const u8) !void {
    var it = try client.historyIterator(workflow_id, run_id, .{ .wait_new_event = true });
    defer it.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    while (try it.next(arena.allocator())) |event| {
        if (event.event_type == .EVENT_TYPE_WORKFLOW_TASK_COMPLETED) return;
    }
    return error.TestRunClosedFirst;
}

test "tracing: a run rebuilt from history for an update after its first workflow task starts no second RunWorkflow span" {
    const s = try Setup.start();
    defer s.deinit();
    // With caching off, every workflow task rebuilds the run from its history, as a worker
    // does for a run it dropped from its cache or never had.
    var w = try worker_probe.Worker.init(testing.allocator, testing.io, &s.env.client, "tracing-rebuilt", .{ .max_cached_workflows = 0 });
    defer w.deinit();
    try w.registerWorkflow(Fixtures.Handled);
    var running = try testing.io.concurrent(runWorkerProbe, .{&w});
    defer {
        w.stop();
        running.await(testing.io) catch {};
    }
    const a = s.arena.allocator();
    var run = try s.env.client.executeWorkflow(Fixtures.Handled, .{ .task_queue = "tracing-rebuilt" }, .{});
    defer run.deinit();
    // The first task runs the workflow function live, up to where it waits for the update
    // and the signal. The update adds no event before the task that delivers it, so that
    // task's history ends with the first task's completion.
    try awaitFirstTaskCompleted(&s.env.client, run.id, run.run_id);
    var update = try s.env.client.updateWorkflow(Fixtures.Set, run.id, "", .{"v"}, .{});
    defer update.deinit();
    try testing.expectEqualStrings("v", try update.get(a));
    try s.env.client.signalWorkflow(Fixtures.Go, run.id, "", .{});
    try testing.expectEqualStrings("v", try run.get(a));

    // The later tasks replay the first before they run what is new: the workflow function
    // ran live once, so the run has one RunWorkflow span.
    const t = &s.tracer;
    t.mutex.lockUncancelable(t.io);
    defer t.mutex.unlock(t.io);
    var runs: usize = 0;
    for (t.spans.items) |span| {
        if (std.mem.eql(u8, span.name, "RunWorkflow:TracedHandled")) runs += 1;
    }
    if (runs != 1) t.dumpLocked();
    try testing.expectEqual(@as(usize, 1), runs);
}

const AppSpans = struct {
    const Outside = activity.Def("TracedOutside", Fixtures.hello);

    /// Counts the `go` signals it gets, and returns once it has two.
    fn waiter(ctx: *workflow.Context) ![]const u8 {
        const n = try ctx.allocator().create(u32);
        n.* = 0;
        try ctx.setSignalHandler(Fixtures.Go, n, struct {
            fn go(c: *u32) void {
                c.* += 1;
            }
        }.go);
        try ctx.await(n, struct {
            fn two(c: *u32) bool {
                return c.* >= 2;
            }
        }.two);
        return ctx.allocator().dupe(u8, "signaled");
    }
    const Waiter = workflow.Def("TracedWaiter", waiter);

    /// Inside `workflow-operation`: an activity, a child and an external signal to it. After
    /// it ends: another activity and a signal through the child's future.
    fn operation(ctx: *workflow.Context) ![]const u8 {
        const child_id = try std.fmt.allocPrint(ctx.allocator(), "{s}-child", .{ctx.info().workflow_id});
        const span = tracing.startSpan(ctx, "workflow-operation", .{ .attributes = &.{.{ .name = "step", .value = "one" }} });
        _ = try ctx.executeActivity(Fixtures.Hello, .{ .start_to_close_timeout = .fromSeconds(30) }, .{"inside"}).get();
        const child = ctx.executeChildWorkflow(Waiter, .{ .id = child_id }, .{});
        _ = try child.started();
        try ctx.signalExternalWorkflow(Fixtures.Go, child_id, "", .{}).get();
        span.end();
        span.end();
        _ = try ctx.executeActivity(Outside, .{ .start_to_close_timeout = .fromSeconds(30) }, .{"outside"}).get();
        try (try child.signal(Fixtures.Go, .{})).get();
        return child.get();
    }
    const Operation = workflow.Def("TracedOperation", operation);

    const Greet = workflow.UpdateDef("greet", fn ([]const u8) []const u8);

    const State = struct {
        ctx: *workflow.Context,
        greeted: ?[]const u8 = null,

        fn greet(s: *State, name: []const u8) ![]const u8 {
            const span = tracing.startSpan(s.ctx, "handle-update", .{});
            defer span.end();
            const inner = tracing.startSpan(s.ctx, "handle-update-inner", .{});
            inner.endWithFailure("Inner");
            const got = try s.ctx.executeActivity(Fixtures.Hello, .{ .start_to_close_timeout = .fromSeconds(30) }, .{name}).get();
            s.greeted = got;
            return got;
        }
        fn validate(s: *State, name: []const u8) error{EmptyName}!void {
            const span = tracing.startSpan(s.ctx, "validate-update", .{ .attributes = &.{.{ .name = "name", .value = name }} });
            defer span.end();
            if (name.len == 0) return error.EmptyName;
        }
        fn done(s: *State) bool {
            return s.greeted != null;
        }
    };

    fn greeter(ctx: *workflow.Context) ![]const u8 {
        const s = try ctx.allocator().create(State);
        s.* = .{ .ctx = ctx };
        try ctx.setUpdateHandlerWithValidator(Greet, s, State.greet, State.validate);
        try ctx.await(s, State.done);
        // Left open: the safety net ends it when the workflow function returns.
        _ = tracing.startSpan(ctx, "never-ended", .{});
        return s.greeted.?;
    }
    const Greeter = workflow.Def("TracedGreeter", greeter);

    /// Whether `startSpan` gave a live span.
    fn probe(ctx: *workflow.Context) ![]const u8 {
        const span = tracing.startSpan(ctx, "probe", .{});
        defer span.end();
        return if (span.record == null) "inert" else "live";
    }
    const Probe = workflow.Def("TracedProbe", probe);
};

test "tracing: outbound calls made while an application span is open are its children, and after it ends the run's" {
    const s = try Setup.start();
    defer s.deinit();
    try s.env.registerWorkflow(AppSpans.Operation);
    try s.env.registerWorkflow(AppSpans.Waiter);
    try s.env.registerActivity(Fixtures.Hello);
    try s.env.registerActivity(AppSpans.Outside);

    var run = try s.env.startWorkflow(AppSpans.Operation, .{}, .{});
    defer run.deinit();
    try testing.expectEqualStrings("signaled", try s.env.getResult(AppSpans.Operation, &run, s.arena.allocator(), null));

    const t = &s.tracer;
    const op = try t.expectChild("workflow-operation", "RunWorkflow:TracedOperation");
    const recorded = t.get(op);
    try testing.expectEqual(tracing.Kind.internal, recorded.kind);
    try testing.expectEqualStrings("step", recorded.tags[0].name);
    try testing.expectEqualStrings("one", recorded.tags[0].value);
    try testing.expectEqualStrings(run.id, tagValue(recorded.tags, "temporalWorkflowID").?);
    try testing.expectEqualStrings(run.run_id, tagValue(recorded.tags, "temporalRunID").?);
    _ = try t.expectChild("StartActivity:TracedHello", "workflow-operation");
    _ = try t.expectChild("RunActivity:TracedHello", "StartActivity:TracedHello");
    _ = try t.expectChild("StartChildWorkflow:TracedWaiter", "workflow-operation");
    _ = try t.expectChild("SignalExternalWorkflow:go", "workflow-operation");
    _ = try t.expectChild("StartActivity:TracedOutside", "RunWorkflow:TracedOperation");
    const child_signal = try t.expectChild("SignalChildWorkflow:go", "RunWorkflow:TracedOperation");
    try testing.expectEqual(tracing.Kind.client, t.get(child_signal).kind);
    // Each signal span is the parent of the handler span it reached.
    const handled = blk: {
        t.mutex.lockUncancelable(t.io);
        defer t.mutex.unlock(t.io);
        var parents: [2][]const u8 = undefined;
        var n: usize = 0;
        for (t.spans.items) |span| if (std.mem.eql(u8, span.name, "HandleSignal:go")) {
            if (n < 2) parents[n] = span.parent orelse "none";
            n += 1;
        };
        try testing.expectEqual(@as(usize, 2), n);
        break :blk parents;
    };
    const external = try t.find("SignalExternalWorkflow:go");
    try testing.expectEqualStrings(try std.fmt.allocPrint(s.arena.allocator(), "{d}", .{external}), handled[0]);
    try testing.expectEqualStrings(try std.fmt.allocPrint(s.arena.allocator(), "{d}", .{child_signal}), handled[1]);
    try t.expectAllFinished();

    // The replayer coverage: replaying the history starts no span, application spans included.
    const before = t.count();
    try s.env.replayRun(run.id, run.run_id);
    try testing.expectEqual(before, t.count());
    try t.expectAllFinished();
}

test "tracing: spans of an update's validator and handler nest under ValidateUpdate and HandleUpdate, and one left open ends with the workflow" {
    const s = try Setup.start();
    defer s.deinit();
    try s.env.registerWorkflow(AppSpans.Greeter);
    try s.env.registerActivity(Fixtures.Hello);
    const a = s.arena.allocator();

    var run = try s.env.startWorkflow(AppSpans.Greeter, .{}, .{});
    defer run.deinit();
    try testing.expectError(error.UpdateFailed, s.env.updateWorkflow(AppSpans.Greet, a, run.id, .{""}, null));
    const greeted = try s.env.updateWorkflow(AppSpans.Greet, a, run.id, .{"Temporal"}, null);
    try testing.expectEqualStrings(greeted, try s.env.getResult(AppSpans.Greeter, &run, a, null));

    const t = &s.tracer;
    const validated = try t.expectChild("validate-update", "ValidateUpdate:greet");
    try testing.expectEqualStrings("", tagValue(t.get(validated).tags, "name").?);
    const handled = try t.expectChild("handle-update", "HandleUpdate:greet");
    try testing.expectEqual(@as(?[]const u8, null), t.get(handled).failure);
    const inner = try t.expectChild("handle-update-inner", "handle-update");
    try testing.expectEqualStrings("Inner", t.get(inner).failure.?);
    _ = try t.expectChild("StartActivity:TracedHello", "handle-update");
    _ = try t.expectChild("never-ended", "RunWorkflow:TracedGreeter");
    // Two validations ran, the rejected one and the accepted one, each with its span.
    var validations: usize = 0;
    {
        t.mutex.lockUncancelable(t.io);
        defer t.mutex.unlock(t.io);
        for (t.spans.items) |span| if (std.mem.eql(u8, span.name, "validate-update")) {
            validations += 1;
        };
    }
    try testing.expectEqual(@as(usize, 2), validations);
    try t.expectAllFinished();

    const before = t.count();
    try s.env.replayRun(run.id, run.run_id);
    try testing.expectEqual(before, t.count());
    try t.expectAllFinished();
}

test "tracing: startSpan is inert on a run without the tracing interceptor" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(AppSpans.Probe);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var run = try env.startWorkflow(AppSpans.Probe, .{}, .{});
    defer run.deinit();
    try testing.expectEqualStrings("inert", try env.getResult(AppSpans.Probe, &run, arena.allocator(), null));
    // An inert span ends without effect.
    const inert: tracing.WorkflowSpan = .{};
    inert.end();
    inert.endWithFailure("ignored");

    // With the interceptor, the same workflow gets a live span.
    const s = try Setup.start();
    defer s.deinit();
    try s.env.registerWorkflow(AppSpans.Probe);
    var traced = try s.env.startWorkflow(AppSpans.Probe, .{}, .{});
    defer traced.deinit();
    try testing.expectEqualStrings("live", try s.env.getResult(AppSpans.Probe, &traced, s.arena.allocator(), null));
    _ = try s.tracer.expectChild("probe", "RunWorkflow:TracedProbe");
}

/// A run that answers `held` with the bytes it holds in its arena, and returns on `stop`.
const QueryMemory = struct {
    const Held = workflow.QueryDef("held", fn () u64);
    const Stop = workflow.SignalDef("stop", fn () void);

    const State = struct {
        ctx: *workflow.Context,
        stopped: bool = false,
        fn held(s: *State) u64 {
            return s.ctx.instance.allocated;
        }
        fn stop(s: *State) void {
            s.stopped = true;
        }
        fn done(s: *State) bool {
            return s.stopped;
        }
    };

    fn run(ctx: *workflow.Context) ![]const u8 {
        const s = try ctx.allocator().create(State);
        s.* = .{ .ctx = ctx };
        try ctx.setQueryHandler(Held, s, State.held);
        try ctx.setSignalHandler(Stop, s, State.stop);
        try ctx.await(s, State.done);
        return ctx.allocator().dupe(u8, "stopped");
    }
    const Def = workflow.Def("TracedQueryMemory", run);
};

test "tracing: queries answered on a traced run leave no memory in the run's arena" {
    const s = try Setup.start();
    defer s.deinit();
    try s.env.registerWorkflow(QueryMemory.Def);
    const a = s.arena.allocator();
    var run = try s.env.startWorkflow(QueryMemory.Def, .{}, .{});
    defer run.deinit();
    // Queries are not in history, so what they leave in the run's arena differs between the
    // live run and its replay, and on a long-lived run that is queried often it only grows.
    const first = try s.env.queryWorkflow(QueryMemory.Held, a, run.id, .{});
    var last = first;
    for (0..20) |_| last = try s.env.queryWorkflow(QueryMemory.Held, a, run.id, .{});
    try s.env.signalWorkflow(QueryMemory.Stop, run.id, .{});
    try testing.expectEqualStrings("stopped", try s.env.getResult(QueryMemory.Def, &run, a, null));
    _ = try s.tracer.expectChild("HandleQuery:held", "QueryWorkflow:held");
    try testing.expectEqual(first, last);
}
