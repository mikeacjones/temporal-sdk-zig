//! Worker interceptors against a dev server: they observe and change a workflow's start,
//! signals, queries, updates, activities, and children, refuse a child, add logger fields, and
//! must be present again to replay the history they shaped.

const std = @import("std");
const activity = @import("activity.zig");
const workflow = @import("workflow.zig");
const converter = @import("converter.zig");
const failure_mod = @import("failure.zig");
const interceptor = @import("interceptor.zig");
const log = @import("log.zig");
const propagation = @import("propagation.zig");
const proto = @import("proto.zig");
const system_nexus = @import("nexus/system.zig");
const schedule = @import("schedule.zig");
const testing_mod = @import("testing.zig");
const WorkflowEnvironment = testing_mod.WorkflowEnvironment;
const Payload = converter.Payload;

const testing = std.testing;

/// What the interceptors saw, from workflow and activity threads.
const Observed = struct {
    mutex: std.Io.Mutex = .init,
    io: std.Io,
    arena: std.heap.ArenaAllocator,
    events: std.ArrayList([]const u8) = .empty,
    heartbeats: std.atomic.Value(u32) = .init(0),

    fn add(o: *Observed, comptime fmt: []const u8, args: anytype) void {
        o.mutex.lockUncancelable(o.io);
        defer o.mutex.unlock(o.io);
        const event = std.fmt.allocPrint(o.arena.allocator(), fmt, args) catch return;
        o.events.append(o.arena.allocator(), event) catch {};
    }

    fn has(o: *Observed, event: []const u8) bool {
        o.mutex.lockUncancelable(o.io);
        defer o.mutex.unlock(o.io);
        for (o.events.items) |e| if (std.mem.eql(u8, e, event)) return true;
        return false;
    }
};

/// Keeps every log record whose fields include `key`, as "message key=value".
const Records = struct {
    mutex: std.Io.Mutex = .init,
    io: std.Io,
    arena: std.heap.ArenaAllocator,
    lines: std.ArrayList([]const u8) = .empty,

    fn sink(r: *Records) log.Sink {
        return .{ .ptr = r, .vtable = &.{ .log = record } };
    }

    fn record(ptr: ?*anyopaque, _: log.Level, message: []const u8, fields: []const log.Field) void {
        const r: *Records = @ptrCast(@alignCast(ptr.?));
        r.mutex.lockUncancelable(r.io);
        defer r.mutex.unlock(r.io);
        for (fields) |f| if (std.mem.endsWith(u8, f.key, "Tagged")) {
            const line = std.fmt.allocPrint(r.arena.allocator(), "{s} {s}={s}", .{ message, f.key, f.value }) catch return;
            r.lines.append(r.arena.allocator(), line) catch {};
        };
    }

    fn has(r: *Records, line: []const u8) bool {
        r.mutex.lockUncancelable(r.io);
        defer r.mutex.unlock(r.io);
        for (r.lines.items) |l| if (std.mem.eql(u8, l, line)) return true;
        return false;
    }
};

/// Re-encodes `args` with the first argument, a string, suffixed.
fn suffixFirst(a: std.mem.Allocator, args: converter.EncodedValues, suffix: []const u8) !converter.EncodedValues {
    const first = try args.get([]const u8, a, 0);
    const payloads = try a.dupe(Payload, args.payloads);
    payloads[0] = try converter.toPayload(a, try std.mem.concat(a, u8, &.{ first, suffix }));
    return .{ .payloads = payloads };
}

fn suffixResult(a: std.mem.Allocator, result: Payload, suffix: []const u8) !Payload {
    const text = try converter.fromPayload([]const u8, a, result);
    return converter.toPayload(a, try std.mem.concat(a, u8, &.{ text, suffix }));
}

const Tagging = struct {
    observed: *Observed,

    pub fn interceptWorkflow(t: *Tagging, ctx: *workflow.Context) error{OutOfMemory}!interceptor.Workflow {
        const run = try ctx.allocator().create(WorkflowRun);
        run.* = .{ .t = t, .ctx = ctx };
        return .{ .inbound = interceptor.WorkflowInbound.from(WorkflowRun, run), .outbound = interceptor.WorkflowOutbound.from(WorkflowRun, run) };
    }

    pub fn interceptActivity(t: *Tagging, ctx: *activity.Context) error{OutOfMemory}!interceptor.Activity {
        const run = try ctx.allocator().create(ActivityRun);
        run.* = .{ .t = t, .ctx = ctx };
        return .{ .inbound = interceptor.ActivityInbound.from(ActivityRun, run), .outbound = interceptor.ActivityOutbound.from(ActivityRun, run) };
    }

    const WorkflowRun = struct {
        t: *Tagging,
        ctx: *workflow.Context,
        logger_: ?log.Logger = null,

        pub fn executeWorkflow(r: *WorkflowRun, input: interceptor.ExecuteWorkflowInput, next: interceptor.WorkflowInbound.Next) anyerror!Payload {
            r.t.observed.add("start:{s}", .{input.workflow_type});
            if (!std.mem.eql(u8, input.workflow_type, "Guarded")) return next.executeWorkflow(input);
            var changed = input;
            changed.args = try suffixFirst(r.ctx.allocator(), input.args, "+in");
            return next.executeWorkflow(changed);
        }

        pub fn handleSignal(r: *WorkflowRun, input: interceptor.HandleSignalInput, next: interceptor.WorkflowInbound.Next) anyerror!void {
            r.t.observed.add("signal:{s}", .{input.signal_name});
            var changed = input;
            changed.args = try suffixFirst(r.ctx.allocator(), input.args, "+sig");
            return next.handleSignal(changed);
        }

        pub fn handleQuery(r: *WorkflowRun, input: interceptor.HandleQueryInput, next: interceptor.WorkflowInbound.Next) anyerror!Payload {
            r.t.observed.add("query:{s}", .{input.query_type});
            return suffixResult(input.arena, try next.handleQuery(input), "+query");
        }

        pub fn validateUpdate(r: *WorkflowRun, input: interceptor.UpdateInput, next: interceptor.WorkflowInbound.Next) anyerror!void {
            r.t.observed.add("validate:{s}", .{input.update_name});
            if (std.mem.eql(u8, try input.args.get([]const u8, r.ctx.allocator(), 0), "bad")) return error.RefusedByInterceptor;
            return next.validateUpdate(input);
        }

        pub fn executeUpdate(r: *WorkflowRun, input: interceptor.UpdateInput, next: interceptor.WorkflowInbound.Next) anyerror!Payload {
            r.t.observed.add("update:{s}", .{input.update_name});
            return suffixResult(r.ctx.allocator(), try next.executeUpdate(input), "+update");
        }

        pub fn executeActivity(r: *WorkflowRun, input: interceptor.ActivityInput, next: interceptor.WorkflowOutbound.Next) workflow.ActivityFuture(Payload) {
            r.t.observed.add("activity:{s}", .{input.activity_type});
            input.header.set("via", @as([]const u8, "interceptor")) catch {};
            return next.executeActivity(input);
        }

        pub fn executeChildWorkflow(r: *WorkflowRun, input: interceptor.ChildWorkflowInput, next: interceptor.WorkflowOutbound.Next) workflow.ChildWorkflowFuture(Payload) {
            if (std.mem.eql(u8, input.workflow_type, "Forbidden")) {
                r.t.observed.add("refused:{s}", .{input.workflow_type});
                return interceptor.rejectChildWorkflow(r.ctx, input.workflow_type, .{ .message = "child workflow Forbidden is not allowed", .type = "Forbidden", .non_retryable = true });
            }
            r.t.observed.add("child:{s}", .{input.workflow_type});
            input.header.set("via", @as([]const u8, "interceptor")) catch {};
            return next.executeChildWorkflow(input);
        }

        pub fn logger(r: *WorkflowRun, input: interceptor.LoggerInput, next: interceptor.WorkflowOutbound.Next) log.Logger {
            if (r.logger_) |l| return l;
            const base = next.logger(input);
            r.logger_ = base.with(r.ctx.allocator(), .{ .Tagged = "workflow" }) catch return base;
            return r.logger_.?;
        }
    };

    const ActivityRun = struct {
        t: *Tagging,
        ctx: *activity.Context,

        pub fn execute(r: *ActivityRun, input: interceptor.ExecuteActivityInput, next: interceptor.ActivityInbound.Next) anyerror!Payload {
            r.t.observed.add("execute:{s}", .{input.activity_type});
            return suffixResult(r.ctx.allocator(), try next.execute(input), "+act");
        }

        pub fn logger(r: *ActivityRun, input: interceptor.LoggerInput, next: interceptor.ActivityOutbound.Next) log.Logger {
            const base = next.logger(input);
            return base.with(r.ctx.allocator(), .{ .ActTagged = "activity" }) catch base;
        }

        pub fn heartbeat(r: *ActivityRun, input: interceptor.HeartbeatInput, next: interceptor.ActivityOutbound.Next) void {
            _ = r.t.observed.heartbeats.fetchAdd(1, .monotonic);
            next.heartbeat(input);
        }
    };
};

const Fixtures = struct {
    const Approve = workflow.SignalDef("approve", fn ([]const u8) void);
    const Status = workflow.QueryDef("status", fn () []const u8);
    const Set = workflow.UpdateDef("set", fn ([]const u8) []const u8);

    fn echo(ctx: *activity.Context, name: []const u8) ![]const u8 {
        ctx.heartbeat({});
        ctx.logger().info("echo", .{});
        const via = (try ctx.headers().value([]const u8, ctx.allocator(), "via")) orelse "none";
        return std.fmt.allocPrint(ctx.allocator(), "{s}:{s}", .{ name, via });
    }
    const Echo = activity.Def("Echo", echo);

    fn kid(ctx: *workflow.Context) ![]const u8 {
        return (try ctx.headers().value([]const u8, ctx.allocator(), "via")) orelse "none";
    }
    const Kid = workflow.Def("Kid", kid);
    const Forbidden = workflow.Def("Forbidden", kid);

    const State = struct {
        ctx: *workflow.Context,
        approver: ?[]const u8 = null,
        value: ?[]const u8 = null,

        fn approve(s: *State, who: []const u8) void {
            s.approver = who;
        }
        fn status(s: *State) []const u8 {
            return s.approver orelse "none";
        }
        fn set(s: *State, v: []const u8) []const u8 {
            s.value = v;
            return v;
        }
        fn check(_: *State, v: []const u8) error{Invalid}!void {
            if (std.mem.eql(u8, v, "invalid")) return error.Invalid;
        }
        fn ready(s: *State) bool {
            return s.approver != null and s.value != null;
        }
    };

    fn guarded(ctx: *workflow.Context, name: []const u8) ![]const u8 {
        const s = try ctx.allocator().create(State);
        s.* = .{ .ctx = ctx };
        try ctx.setSignalHandler(Approve, s, State.approve);
        try ctx.setQueryHandler(Status, s, State.status);
        try ctx.setUpdateHandlerWithValidator(Set, s, State.set, State.check);
        ctx.logger().info("guarded started", .{});
        const echoed = try ctx.executeActivity(Echo, .{ .start_to_close_timeout = .fromSeconds(10) }, .{name}).get();
        const from_kid = try ctx.executeChildWorkflow(Kid, .{}, .{}).get();
        const refused = ctx.executeChildWorkflow(Forbidden, .{}, .{});
        const why = if (refused.get()) |_| "started" else |err| switch (err) {
            error.ChildWorkflowFailed => failure_mod.rootCause(refused.failure().?).message,
            else => return err,
        };
        try ctx.await(s, State.ready);
        return std.fmt.allocPrint(ctx.allocator(), "{s}|{s}|{s}|{s}|{s}|{s}", .{ name, echoed, from_kid, why, s.approver.?, s.value.? });
    }
    const Guarded = workflow.Def("Guarded", guarded);
};

test "worker interceptors observe and change a workflow's start, signal, query, update, activity and children" {
    var observed: Observed = .{ .io = testing.io, .arena = .init(testing.allocator) };
    defer observed.arena.deinit();
    var records: Records = .{ .io = testing.io, .arena = .init(testing.allocator) };
    defer records.arena.deinit();
    var tagging: Tagging = .{ .observed = &observed };
    const interceptors = [_]interceptor.Worker{interceptor.Worker.from(Tagging, &tagging)};

    const env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{ .worker_interceptors = &interceptors, .logger = records.sink() });
    defer env.deinit();
    try env.registerWorkflow(Fixtures.Guarded);
    try env.registerWorkflow(Fixtures.Kid);
    try env.registerActivity(Fixtures.Echo);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var run = try env.startWorkflow(Fixtures.Guarded, .{ .task_queue = "" }, .{"Temporal"});
    defer run.deinit();
    try testing.expectEqualStrings("none+query", try env.queryWorkflow(Fixtures.Status, a, run.id, .{}));
    var why: ?failure_mod.Failure = null;
    try testing.expectError(error.UpdateFailed, env.updateWorkflow(Fixtures.Set, a, run.id, .{"bad"}, &why));
    try testing.expectStringStartsWith(why.?.message, "RefusedByInterceptor");
    try testing.expectError(error.UpdateFailed, env.updateWorkflow(Fixtures.Set, a, run.id, .{"invalid"}, &why));
    try testing.expectStringStartsWith(why.?.message, "Invalid");
    try testing.expectEqualStrings("u1+update", try env.updateWorkflow(Fixtures.Set, a, run.id, .{"u1"}, null));
    try env.signalWorkflow(Fixtures.Approve, run.id, .{"ops"});

    const result = try env.getResult(Fixtures.Guarded, &run, a, null);
    try testing.expectEqualStrings("Temporal+in|Temporal+in:interceptor+act|interceptor|child workflow Forbidden is not allowed|ops+sig|u1", result);

    for ([_][]const u8{ "start:Guarded", "start:Kid", "query:status", "validate:set", "update:set", "signal:approve", "activity:Echo", "execute:Echo", "child:Kid", "refused:Forbidden" }) |event|
        if (!observed.has(event)) {
            std.debug.print("interceptors never saw {s}; saw {any}\n", .{ event, observed.events.items });
            return error.TestExpectedEvent;
        };
    try testing.expect(observed.heartbeats.load(.monotonic) >= 1);
    try testing.expect(records.has("guarded started Tagged=workflow"));
    try testing.expect(records.has("echo ActTagged=activity"));

    // The history replays with the interceptors that shaped it, and not without them: the
    // refused child would be started.
    const history = try env.client.fetchHistory(a, run.id, run.run_id);
    var with: testing_mod.WorkflowReplayer = .initWithOptions(testing.allocator, testing.io, .{ .task_queue = env.task_queue, .interceptors = &interceptors });
    defer with.deinit();
    try with.registerWorkflow(Fixtures.Guarded);
    try with.replayHistory(history);
    var without: testing_mod.WorkflowReplayer = .initWithOptions(testing.allocator, testing.io, .{ .task_queue = env.task_queue });
    defer without.deinit();
    try without.registerWorkflow(Fixtures.Guarded);
    try testing.expectError(error.Nondeterminism, without.replayHistory(history));
}

/// Records the method of every request, refuses terminations, and tags starts with a header.
const Recording = struct {
    mutex: std.Io.Mutex = .init,
    io: std.Io,
    arena: std.heap.ArenaAllocator,
    methods: std.ArrayList([]const u8) = .empty,

    pub fn call(r: *Recording, input: *interceptor.ClientCall, next: interceptor.Client.Next) interceptor.ClientError!void {
        {
            r.mutex.lockUncancelable(r.io);
            defer r.mutex.unlock(r.io);
            const last = if (r.methods.items.len > 0) r.methods.items[r.methods.items.len - 1] else "";
            // Long polls repeat a method; one entry per run of them.
            if (!std.mem.eql(u8, last, input.method)) try r.methods.append(r.arena.allocator(), try r.arena.allocator().dupe(u8, input.method));
        }
        if (std.mem.eql(u8, input.method, "TerminateWorkflowExecution")) return error.RpcFailed;
        if (input.request(proto.temporal.api.workflowservice.v1.StartWorkflowExecutionRequest)) |request|
            try input.header.?.set("client-tag", try std.fmt.allocPrint(input.arena, "for {s}", .{request.workflow_id}));
        try next.call(input);
        if (input.response(proto.temporal.api.workflowservice.v1.CountWorkflowExecutionsResponse)) |response| response.count += 1000;
    }
};

const ClientFixtures = struct {
    const Done = workflow.SignalDef("done", fn () void);
    const Peek = workflow.QueryDef("peek", fn () []const u8);
    const Bump = workflow.UpdateDef("bump", fn () u32);

    const State = struct {
        done: bool = false,
        bumps: u32 = 0,
        fn finish(s: *State) void {
            s.done = true;
        }
        fn peek(_: *State) []const u8 {
            return "peeked";
        }
        fn bump(s: *State) u32 {
            s.bumps += 1;
            return s.bumps;
        }
        fn finished(s: *State) bool {
            return s.done;
        }
    };

    /// Waits for `done` and returns its start header's tag.
    fn tagged(ctx: *workflow.Context) ![]const u8 {
        const s = try ctx.allocator().create(State);
        s.* = .{};
        try ctx.setSignalHandler(Done, s, State.finish);
        try ctx.setQueryHandler(Peek, s, State.peek);
        try ctx.setUpdateHandler(Bump, s, State.bump);
        try ctx.await(s, State.finished);
        return (try ctx.headers().value([]const u8, ctx.allocator(), "client-tag")) orelse "untagged";
    }
    const Tagged = workflow.Def("Tagged", tagged);

    fn double(_: *activity.Context, n: u32) !u32 {
        return 2 * n;
    }
    const Double = activity.Def("Double", double);
};

test "a client interceptor sees every operation's request in order, and can change or refuse one" {
    var recording: Recording = .{ .io = testing.io, .arena = .init(testing.allocator) };
    defer recording.arena.deinit();
    const interceptors = [_]interceptor.Client{interceptor.Client.from(Recording, &recording)};
    const env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{
        .interceptors = &interceptors,
        .dev_server = .{ .extra_args = &testing_mod.DevServer.standalone_activity_args },
    });
    defer env.deinit();
    try env.registerWorkflow(ClientFixtures.Tagged);
    try env.registerActivity(ClientFixtures.Double);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const c = &env.client;

    var run = try env.startWorkflow(ClientFixtures.Tagged, .{ .id = "tagged", .task_queue = "" }, .{});
    defer run.deinit();
    try testing.expectEqualStrings("peeked", try c.queryWorkflow(ClientFixtures.Peek, a, "tagged", "", .{}));
    var bumped = try c.updateWorkflow(ClientFixtures.Bump, "tagged", "", .{}, .{});
    defer bumped.deinit();
    try c.signalWorkflow(ClientFixtures.Done, "tagged", "", .{});
    try testing.expectEqualStrings("for tagged", try run.get(a));
    _ = try c.describeWorkflow(a, "tagged", "");

    var other = try c.signalWithStartWorkflow(ClientFixtures.Tagged, .{ .id = "other", .task_queue = env.task_queue }, .{}, ClientFixtures.Done, .{});
    defer other.deinit();
    try c.cancelWorkflow("other", "", "done with it");
    try testing.expectError(error.RpcFailed, c.terminateWorkflow("other", "", "refused"));
    var with_start = try c.updateWithStartWorkflow(ClientFixtures.Tagged, .{ .id = "third", .task_queue = env.task_queue, .id_conflict_policy = .use_existing }, .{}, ClientFixtures.Bump, .{}, .{});
    defer with_start.deinit();
    try testing.expect(try c.countWorkflows("") >= 1000);
    var listed = try c.listWorkflows("", .{});
    defer listed.deinit();
    _ = try listed.next(a);
    _ = try c.fetchHistory(a, "tagged", "");

    var handle = try c.createSchedule("tagged-schedule", .{ .schedule = .{
        .action = try schedule.startWorkflow(a, ClientFixtures.Tagged, .{}, .{ .id = "scheduled", .task_queue = env.task_queue }),
        .spec = .{},
    } });
    defer handle.deinit();
    _ = try handle.describe(a);
    try handle.delete();

    var doubled = try c.executeActivity(ClientFixtures.Double, .{ .task_queue = env.task_queue, .start_to_close_timeout = .fromSeconds(10) }, .{21});
    defer doubled.deinit();
    try testing.expectEqual(@as(u32, 42), try doubled.get(a));

    const want = [_][]const u8{
        "StartWorkflowExecution",           "QueryWorkflow",                  "UpdateWorkflowExecution",
        "SignalWorkflowExecution",          "GetWorkflowExecutionHistory",    "DescribeWorkflowExecution",
        "SignalWithStartWorkflowExecution", "RequestCancelWorkflowExecution", "TerminateWorkflowExecution",
        "ExecuteMultiOperation",            "CountWorkflowExecutions",        "ListWorkflowExecutions",
        "GetWorkflowExecutionHistory",      "CreateSchedule",                 "DescribeSchedule",
        "DeleteSchedule",                   "StartActivityExecution",         "PollActivityExecution",
    };
    const got = recording.methods.items;
    if (got.len != want.len) {
        std.debug.print("client interceptor saw {any}\n", .{got});
        return error.TestUnexpectedResult;
    }
    for (want, got) |w, g| try testing.expectEqualStrings(w, g);
}

/// Refuses every activity and child workflow with a failure whose cause it builds on its own
/// stack, then reuses that memory before returning, as a stack frame's memory is reused once
/// it returns.
const RefusingWithCause = struct {
    pub fn interceptWorkflow(_: *RefusingWithCause, ctx: *workflow.Context) error{OutOfMemory}!interceptor.Workflow {
        const run = try ctx.allocator().create(Run);
        run.* = .{ .ctx = ctx };
        return .{ .outbound = interceptor.WorkflowOutbound.from(Run, run) };
    }

    const Run = struct {
        ctx: *workflow.Context,

        pub fn executeActivity(r: *Run, input: interceptor.ActivityInput, _: interceptor.WorkflowOutbound.Next) workflow.ActivityFuture(Payload) {
            var why: failure_mod.Failure = .{ .message = "over budget" };
            const future = interceptor.rejectActivity(r.ctx, input.activity_type, .{ .message = "refused", .type = "Refused", .cause = &why });
            why = .{ .message = "reused stack" };
            return future;
        }

        pub fn executeChildWorkflow(r: *Run, input: interceptor.ChildWorkflowInput, _: interceptor.WorkflowOutbound.Next) workflow.ChildWorkflowFuture(Payload) {
            var why: failure_mod.Failure = .{ .message = "over budget" };
            const future = interceptor.rejectChildWorkflow(r.ctx, input.workflow_type, .{ .message = "refused", .type = "Refused", .cause = &why });
            why = .{ .message = "reused stack" };
            return future;
        }
    };

    fn refused(ctx: *workflow.Context) ![]const u8 {
        const act = ctx.executeActivity(Fixtures.Echo, .{ .start_to_close_timeout = .fromSeconds(10) }, .{"x"});
        _ = act.get() catch {};
        const kid = ctx.executeChildWorkflow(Fixtures.Kid, .{}, .{});
        _ = kid.get() catch {};
        return std.fmt.allocPrint(ctx.allocator(), "{s}|{s}", .{ failure_mod.rootCause(act.failure().?).message, failure_mod.rootCause(kid.failure().?).message });
    }
    const Refused = workflow.Def("RefusedWithCause", refused);
};

test "rejectActivity and rejectChildWorkflow copy the refusal's cause, which the interceptor may build on its stack" {
    var refusing: RefusingWithCause = .{};
    const interceptors = [_]interceptor.Worker{interceptor.Worker.from(RefusingWithCause, &refusing)};
    const env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{ .worker_interceptors = &interceptors });
    defer env.deinit();
    try env.registerWorkflow(RefusingWithCause.Refused);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    try testing.expectEqualStrings("over budget|over budget", try env.executeWorkflow(RefusingWithCause.Refused, .{}, arena.allocator(), null));
}

/// Records each signal and update request's method and message name, as "method name".
const MessageNames = struct {
    mutex: std.Io.Mutex = .init,
    io: std.Io,
    arena: std.heap.ArenaAllocator,
    calls: std.ArrayList([]const u8) = .empty,

    pub fn call(r: *MessageNames, input: *interceptor.ClientCall, next: interceptor.Client.Next) interceptor.ClientError!void {
        const wfs = proto.temporal.api.workflowservice.v1;
        const name: ?[]const u8 = if (input.request(wfs.SignalWorkflowExecutionRequest)) |q|
            q.signal_name
        else if (input.request(wfs.SignalWithStartWorkflowExecutionRequest)) |q|
            q.signal_name
        else if (input.request(wfs.UpdateWorkflowExecutionRequest)) |q|
            q.request.?.input.?.name
        else if (input.request(wfs.ExecuteMultiOperationRequest)) |q|
            q.operations.items[1].operation.?.update_workflow.request.?.input.?.name
        else
            null;
        if (name) |n| {
            r.mutex.lockUncancelable(r.io);
            defer r.mutex.unlock(r.io);
            try r.calls.append(r.arena.allocator(), try std.fmt.allocPrint(r.arena.allocator(), "{s} {s}", .{ input.method, n }));
        }
        try next.call(input);
    }
};

test "a client interceptor sees signals and updates sent by name as it sees typed ones" {
    var names: MessageNames = .{ .io = testing.io, .arena = .init(testing.allocator) };
    defer names.arena.deinit();
    const interceptors = [_]interceptor.Client{interceptor.Client.from(MessageNames, &names)};
    const env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{ .interceptors = &interceptors });
    defer env.deinit();
    try env.registerWorkflow(ClientFixtures.Tagged);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const c = &env.client;

    var run = try env.startWorkflow(ClientFixtures.Tagged, .{ .id = "tagged-by-name", .task_queue = "" }, .{});
    defer run.deinit();
    var bumped = try c.updateWorkflowByName(u32, "bump", "tagged-by-name", "", .{}, .{});
    defer bumped.deinit();
    try testing.expectEqual(@as(u32, 1), try bumped.get(a));
    try c.signalWorkflowByName("done", "tagged-by-name", "", .{});
    try testing.expectEqualStrings("untagged", try run.get(a));

    var other = try c.signalWithStartWorkflowByName([]const u8, "Tagged", .{ .id = "other-by-name", .task_queue = env.task_queue }, .{}, "done", .{});
    defer other.deinit();
    try testing.expectEqualStrings("untagged", try other.get(a));
    var with_start = try c.updateWithStartWorkflowByName(u32, "Tagged", .{ .id = "third-by-name", .task_queue = env.task_queue, .id_conflict_policy = .use_existing }, .{}, "bump", .{}, .{});
    defer with_start.deinit();
    try testing.expectEqual(@as(u32, 1), try with_start.get(a));
    try c.signalWorkflow(ClientFixtures.Done, "third-by-name", "", .{});

    const want = [_][]const u8{
        "UpdateWorkflowExecution bump",
        "SignalWorkflowExecution done",
        "SignalWithStartWorkflowExecution done",
        "ExecuteMultiOperation bump",
        "SignalWorkflowExecution done",
    };
    const got = names.calls.items;
    if (got.len != want.len) {
        std.debug.print("client interceptor saw {any}\n", .{got});
        return error.TestUnexpectedResult;
    }
    for (want, got) |w, g| try testing.expectEqualStrings(w, g);
}

// ---------------------------------------------------------------- Nexus, unit

const nexus = @import("nexus.zig");

/// Appends its tag to `order` on the way in and out of each Nexus method it declares.
const NexusTrace = struct {
    tag: u8,
    order: *std.ArrayList(u8),

    fn mark(t: *NexusTrace) void {
        t.order.appendAssumeCapacity(t.tag);
    }

    pub fn startOperation(t: *NexusTrace, input: interceptor.StartNexusOperationInput, next: interceptor.NexusOperationInbound.Next) nexus.StartResult {
        t.mark();
        defer t.mark();
        var changed = input;
        changed.operation = if (t.tag == 'a') "renamed" else input.operation;
        return next.startOperation(changed);
    }
};

const NexusRoot = struct {
    starts: usize = 0,
    cancels: usize = 0,
    operation: []const u8 = "",
    token: []const u8 = "",

    const vtable: interceptor.NexusOperationInbound.VTable = .{ .startOperation = start, .cancelOperation = cancel };

    fn start(ptr: *anyopaque, input: interceptor.StartNexusOperationInput, _: interceptor.NexusOperationInbound.Next) nexus.StartResult {
        const r: *NexusRoot = @ptrCast(@alignCast(ptr));
        r.starts += 1;
        r.operation = input.operation;
        return .{ .succeeded = .{ .result = .{ .data = "ok" } } };
    }

    fn cancel(ptr: *anyopaque, input: interceptor.CancelNexusOperationInput, _: interceptor.NexusOperationInbound.Next) nexus.CancelResult {
        const r: *NexusRoot = @ptrCast(@alignCast(ptr));
        r.cancels += 1;
        r.token = input.token;
        return .requested;
    }

    fn inbound(r: *NexusRoot) interceptor.NexusOperationInbound {
        return .{ .ptr = r, .vtable = &vtable };
    }
};

test "Nexus inbound interceptors run first to last around the handler, and pass through what they don't declare" {
    var buffer: [8]u8 = undefined;
    var order: std.ArrayList(u8) = .initBuffer(&buffer);
    var a: NexusTrace = .{ .tag = 'a', .order = &order };
    var b: NexusTrace = .{ .tag = 'b', .order = &order };
    const first = interceptor.NexusOperationInbound.from(NexusTrace, &a);
    try testing.expect(first.vtable.startOperation != null);
    try testing.expect(first.vtable.cancelOperation == null);

    var root: NexusRoot = .{};
    const chain = [_]interceptor.NexusOperationInbound{ first, interceptor.NexusOperationInbound.from(NexusTrace, &b) };
    const next: interceptor.NexusOperationInbound.Next = .{ .chain = &chain, .root = root.inbound() };

    const started = next.startOperation(.{ .service = "s", .operation = "op", .payload = null, .headers = &.{} });
    try testing.expectEqualStrings("ok", started.succeeded.result.data);
    try testing.expectEqualStrings("abba", order.items);
    try testing.expectEqualStrings("renamed", root.operation);

    const canceled = next.cancelOperation(.{ .service = "s", .operation = "op", .token = "tok" });
    try testing.expect(canceled == .requested);
    try testing.expectEqual(@as(usize, 1), root.cancels);
    try testing.expectEqualStrings("tok", root.token);
    try testing.expectEqualStrings("abba", order.items);
}

test "a Nexus inbound interceptor refuses a request without calling next" {
    const Refusing = struct {
        pub fn cancelOperation(_: *@This(), _: interceptor.CancelNexusOperationInput, _: interceptor.NexusOperationInbound.Next) nexus.CancelResult {
            return .{ .handler_error = .{ .type = .unauthorized, .message = "no" } };
        }
    };
    var refusing: Refusing = .{};
    var root: NexusRoot = .{};
    const chain = [_]interceptor.NexusOperationInbound{interceptor.NexusOperationInbound.from(Refusing, &refusing)};
    const next: interceptor.NexusOperationInbound.Next = .{ .chain = &chain, .root = root.inbound() };
    const result = next.cancelOperation(.{ .service = "s", .operation = "op", .token = "tok" });
    try testing.expectEqualStrings("no", result.handler_error.message);
    try testing.expectEqual(@as(usize, 0), root.cancels);
    // Starts pass through to the root.
    _ = next.startOperation(.{ .service = "s", .operation = "op", .payload = null, .headers = &.{} });
    try testing.expectEqual(@as(usize, 1), root.starts);
}

/// Adds the field `key=x` to the logger.
const NexusLogTag = struct {
    key: []const u8,
    arena: std.mem.Allocator,
    pub fn logger(t: *NexusLogTag, input: interceptor.LoggerInput, next: interceptor.NexusOperationOutbound.Next) log.Logger {
        var l = next.logger(input);
        const context = std.mem.concat(t.arena, log.Field, &.{ l.context, &.{.{ .key = t.key, .value = "x" }} }) catch return l;
        l.context = context;
        return l;
    }
};

test "Nexus outbound interceptors wrap the handler's logger, the first outermost" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var records: Records = .{ .io = testing.io, .arena = .init(testing.allocator) };
    defer records.arena.deinit();
    var outer: NexusLogTag = .{ .key = "OuterTagged", .arena = arena.allocator() };
    var inner: NexusLogTag = .{ .key = "InnerTagged", .arena = arena.allocator() };
    const Empty = struct {};
    var empty: Empty = .{};
    const pass = interceptor.NexusOperationOutbound.from(Empty, &empty);
    try testing.expect(pass.vtable.logger == null);
    const chain = [_]interceptor.NexusOperationOutbound{ interceptor.NexusOperationOutbound.from(NexusLogTag, &outer), pass, interceptor.NexusOperationOutbound.from(NexusLogTag, &inner) };

    var ctx: nexus.Context = .{
        .arena = arena.allocator(),
        .io_ = testing.io,
        .info_ = .{ .service = "s", .operation = "op" },
        .logger_ = .{ .sink = records.sink() },
    };
    ctx.logger().info("without", .{ .NoneTagged = "y" });
    ctx.outbound = &chain;
    ctx.logger().info("with", .{});
    try testing.expect(records.has("without NoneTagged=y"));
    try testing.expect(records.has("with InnerTagged=x"));
    try testing.expect(records.has("with OuterTagged=x"));
    // The innermost interceptor's field comes first: it wrapped the root logger first.
    const lines = records.lines.items;
    try testing.expectEqualStrings("with InnerTagged=x", lines[lines.len - 2]);
}

test "Worker.from hands out a Nexus request's interceptors when the type declares interceptNexusOperation" {
    const W = struct {
        calls: usize = 0,
        pub fn interceptNexusOperation(w: *@This(), _: *nexus.Context) error{OutOfMemory}!interceptor.NexusOperation {
            w.calls += 1;
            return .{};
        }
    };
    var w: W = .{};
    const with = interceptor.Worker.from(W, &w);
    try testing.expect(with.vtable.interceptWorkflow == null);
    try testing.expect(with.vtable.interceptActivity == null);
    try testing.expect(with.vtable.interceptNexusOperation != null);
    var ctx: nexus.Context = .{ .arena = testing.allocator, .io_ = testing.io, .info_ = .{ .service = "s", .operation = "op" } };
    const got = try with.interceptNexusOperation(&ctx);
    try testing.expect(got.inbound == null and got.outbound == null);
    try testing.expectEqual(@as(usize, 1), w.calls);

    const None = struct {};
    var none: None = .{};
    const without = interceptor.Worker.from(None, &none);
    try testing.expect(without.vtable.interceptNexusOperation == null);
    const passed = try without.interceptNexusOperation(&ctx);
    try testing.expect(passed.inbound == null and passed.outbound == null);
}

test "workflow outbound interceptors see a Nexus call first to last, and pass through what they don't declare" {
    const Trace = struct {
        tag: u8,
        order: *std.ArrayList(u8),
        pub fn executeNexusOperation(t: *@This(), input: interceptor.NexusOperationInput, next: interceptor.WorkflowOutbound.Next) workflow.NexusOperationFuture(Payload) {
            t.order.appendAssumeCapacity(t.tag);
            var changed = input;
            changed.options.endpoint = if (t.tag == 'a') "changed" else input.options.endpoint;
            return next.executeNexusOperation(changed);
        }
    };
    const Root = struct {
        endpoint: []const u8 = "",
        const vtable: interceptor.WorkflowOutbound.VTable = .{ .executeNexusOperation = call };
        fn call(ptr: *anyopaque, input: interceptor.NexusOperationInput, _: interceptor.WorkflowOutbound.Next) workflow.NexusOperationFuture(Payload) {
            const r: *@This() = @ptrCast(@alignCast(ptr));
            r.endpoint = input.options.endpoint;
            return .{ .ctx = undefined, .state = error.InvalidOptions };
        }
    };
    const LogsOnly = struct {
        pub fn logger(_: *@This(), input: interceptor.LoggerInput, next: interceptor.WorkflowOutbound.Next) log.Logger {
            return next.logger(input);
        }
    };
    var buffer: [4]u8 = undefined;
    var order: std.ArrayList(u8) = .initBuffer(&buffer);
    var a: Trace = .{ .tag = 'a', .order = &order };
    var b: Trace = .{ .tag = 'b', .order = &order };
    var logs: LogsOnly = .{};
    const pass = interceptor.WorkflowOutbound.from(LogsOnly, &logs);
    try testing.expect(pass.vtable.executeNexusOperation == null);
    var root: Root = .{};
    const chain = [_]interceptor.WorkflowOutbound{ interceptor.WorkflowOutbound.from(Trace, &a), pass, interceptor.WorkflowOutbound.from(Trace, &b) };
    const next: interceptor.WorkflowOutbound.Next = .{ .chain = &chain, .root = .{ .ptr = &root, .vtable = &Root.vtable } };
    const future = next.executeNexusOperation(.{ .service = "s", .operation = "op", .options = .{ .endpoint = "e" }, .payload = .{} });
    try testing.expectError(error.InvalidOptions, future.state);
    try testing.expectEqualStrings("ab", order.items);
    try testing.expectEqualStrings("changed", root.endpoint);
}

// ---------------------------------------------------------------- Nexus, replay

const NexusReplay = struct {
    const Shout = nexus.OperationDef("shout", []const u8, []const u8);
    const Service = nexus.ServiceDef("zig-interceptor-replay-service", .{Shout});

    fn shout(ctx: *nexus.Context, input: []const u8) ![]const u8 {
        return std.fmt.allocPrint(ctx.allocator(), "{s}! x-tag={s}", .{ input, ctx.info().header("x-tag") orelse "none" });
    }
    const Handler = nexus.ServiceHandler(Service, .{nexus.OperationHandler(Shout, shout)});

    /// Calls `shout` through `endpoint`.
    fn call(ctx: *workflow.Context, endpoint: []const u8, input: []const u8) ![]const u8 {
        return ctx.executeNexusOperation(Service, Shout, .{ .endpoint = endpoint, .schedule_to_close_timeout = .fromSeconds(60) }, input).get();
    }
    const Call = workflow.Def("NexusInterceptorReplay", call);

    /// Adds the header `x-tag: outbound` to every Nexus call.
    const Tag = struct {
        pub fn interceptWorkflow(_: *Tag, ctx: *workflow.Context) error{OutOfMemory}!interceptor.Workflow {
            const run = try ctx.allocator().create(Run);
            run.* = .{ .ctx = ctx };
            return .{ .outbound = interceptor.WorkflowOutbound.from(Run, run) };
        }

        const Run = struct {
            ctx: *workflow.Context,
            pub fn executeNexusOperation(r: *Run, input: interceptor.NexusOperationInput, next: interceptor.WorkflowOutbound.Next) workflow.NexusOperationFuture(Payload) {
                var changed = input;
                changed.options.headers = std.mem.concat(r.ctx.allocator(), nexus.Header, &.{ input.options.headers, &.{.{ .name = "x-tag", .value = "outbound" }} }) catch
                    return interceptor.rejectNexusOperation(r.ctx, input, .{ .message = "out of memory" });
                return next.executeNexusOperation(changed);
            }
        };
    };
};

/// A dev-server run of `NexusReplay.Call` with `NexusReplay.Tag` installed, whose Nexus call
/// carries the header the interceptor added; fetched with `Client.fetchHistory` and encoded
/// as ProtoJSON (`History.jsonEncode`).
const nexus_interceptor_history = @embedFile("testdata/nexus_interceptor_history.json");

test "WorkflowReplayer replays a Nexus call shaped by a workflow outbound interceptor" {
    var tag: NexusReplay.Tag = .{};
    const interceptors = [_]interceptor.Worker{interceptor.Worker.from(NexusReplay.Tag, &tag)};
    var replayer: testing_mod.WorkflowReplayer = .initWithOptions(testing.allocator, testing.io, .{ .interceptors = &interceptors });
    defer replayer.deinit();
    try replayer.registerWorkflow(NexusReplay.Call);
    try replayer.replayHistoryJson(nexus_interceptor_history);
}

/// Refuses every Nexus call with a failure whose cause it builds on its own stack, then reuses
/// that memory, as `RefusingWithCause` does for activities.
const RefusingNexus = struct {
    pub fn interceptWorkflow(_: *RefusingNexus, ctx: *workflow.Context) error{OutOfMemory}!interceptor.Workflow {
        const run = try ctx.allocator().create(Run);
        run.* = .{ .ctx = ctx };
        return .{ .outbound = interceptor.WorkflowOutbound.from(Run, run) };
    }

    const Run = struct {
        ctx: *workflow.Context,

        pub fn executeNexusOperation(r: *Run, input: interceptor.NexusOperationInput, _: interceptor.WorkflowOutbound.Next) workflow.NexusOperationFuture(Payload) {
            var why: failure_mod.Failure = .{ .message = "over budget" };
            const future = interceptor.rejectNexusOperation(r.ctx, input, .{ .message = "refused", .type = "Refused", .cause = &why });
            why = .{ .message = "reused stack" };
            return future;
        }
    };

    /// Calls `shout` through an endpoint that doesn't exist: nothing is scheduled, so it is
    /// never looked up.
    fn refused(ctx: *workflow.Context) ![]const u8 {
        const f = ctx.executeNexusOperation(NexusReplay.Service, NexusReplay.Shout, .{ .endpoint = "no-such-endpoint" }, "hi");
        if (!f.isReady()) return error.TestExpectedReady;
        f.cancel();
        if (f.started()) |_| return error.TestExpectedRefusal else |err| if (err != error.NexusOperationFailed) return err;
        if (f.get()) |_| return error.TestExpectedRefusal else |err| if (err != error.NexusOperationFailed) return err;
        const top = f.failure().?;
        const info = top.failure_info.?.nexus_operation_execution_failure_info;
        return std.fmt.allocPrint(ctx.allocator(), "{s}|{s}/{s}/{s}|{s}|{s}", .{ top.message, info.endpoint, info.service, info.operation, top.cause.?.message, failure_mod.rootCause(top).message });
    }
    const Refused = workflow.Def("RefusedNexus", refused);
};

test "rejectNexusOperation fails the call without scheduling it, naming the operation, and copies the refusal's cause" {
    var refusing: RefusingNexus = .{};
    const interceptors = [_]interceptor.Worker{interceptor.Worker.from(RefusingNexus, &refusing)};
    const env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{ .worker_interceptors = &interceptors });
    defer env.deinit();
    try env.registerWorkflow(RefusingNexus.Refused);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var run = try env.startWorkflow(RefusingNexus.Refused, .{ .task_queue = "" }, .{});
    defer run.deinit();
    try testing.expectEqualStrings(
        "nexus operation \"shout\" of service \"zig-interceptor-replay-service\" was refused by a workflow interceptor|no-such-endpoint/zig-interceptor-replay-service/shout|refused|over budget",
        try env.getResult(RefusingNexus.Refused, &run, a, null),
    );
    const history = try env.client.fetchHistory(a, run.id, run.run_id);
    for (history.events.items) |event| try testing.expect(event.event_type != .EVENT_TYPE_NEXUS_OPERATION_SCHEDULED);
}

// ---------------------------------------------------------------- signal-with-start

/// The dev server only accepts signal-with-start from a workflow with this setting on.
const signal_with_start_args = [_][]const u8{ "--dynamic-config-value", "history.enableSignalWithStartFromWorkflow=true" };

const SignalWithStartFixtures = struct {
    const tenant = propagation.key([]const u8, "tenant");
    const propagators = [_]propagation.ContextPropagator{propagation.HeaderPropagator(tenant).propagator};

    const Add = workflow.SignalDef("add", fn (value: []const u8) void);

    /// Waits for one `add` signal; returns its start argument, the signal, its header's `via`
    /// and its propagated tenant.
    fn target(ctx: *workflow.Context, start: []const u8) ![]const u8 {
        const signal = try (try ctx.signalChannel(Add)).receive();
        const via = (try ctx.headers().value([]const u8, ctx.allocator(), "via")) orelse "none";
        return std.fmt.allocPrint(ctx.allocator(), "start={s} signal={s} via={s} tenant={s}", .{ start, signal, via, ctx.propagated().get(tenant) orelse "none" });
    }
    const Target = workflow.Def("sws_icpt_target", target);

    /// Signals-with-start `Target` as `id` once; returns "<workflow id> <run id> <started>".
    fn caller(ctx: *workflow.Context, id: []const u8) ![]const u8 {
        const r = try ctx.signalWithStartWorkflow(Target, .{ .id = id, .execution_timeout = .fromSeconds(60) }, .{"go"}, Add, .{"one"}).get();
        return std.fmt.allocPrint(ctx.allocator(), "{s} {s} {}", .{ r.workflow_id, r.run_id, r.started });
    }
    const Caller = workflow.Def("sws_icpt_caller", caller);

    /// Records every signal-with-start and Nexus call it sees outside replay, and changes each
    /// signal-with-start: "+in" on the first argument, "+sig" on the first signal argument,
    /// "-changed" on the workflow ID, and the header field `via: interceptor`.
    const Shaping = struct {
        observed: ?*Observed = null,

        pub fn interceptWorkflow(s: *Shaping, ctx: *workflow.Context) error{OutOfMemory}!interceptor.Workflow {
            const run = try ctx.allocator().create(Run);
            run.* = .{ .s = s, .ctx = ctx };
            return .{ .outbound = interceptor.WorkflowOutbound.from(Run, run) };
        }

        const Run = struct {
            s: *Shaping,
            ctx: *workflow.Context,

            pub fn signalWithStartWorkflow(r: *Run, input: interceptor.SignalWithStartInput, next: interceptor.WorkflowOutbound.Next) workflow.SignalWithStartFuture {
                return r.shape(input, next) catch |err| .{ .ctx = r.ctx, .workflow_id = "", .state = switch (err) {
                    error.OutOfMemory => error.OutOfMemory,
                    else => error.InvalidOptions,
                } };
            }

            fn shape(r: *Run, input: interceptor.SignalWithStartInput, next: interceptor.WorkflowOutbound.Next) !workflow.SignalWithStartFuture {
                const a = r.ctx.allocator();
                if (r.s.observed) |o| if (!r.ctx.isReplaying()) o.add("sws:{s}/{s} id={s} args={s} signal={s} tenant={s}", .{
                    input.workflow_type,
                    input.signal_name,
                    input.options.id,
                    try input.args.get([]const u8, a, 0),
                    try input.signal_args.get([]const u8, a, 0),
                    if (input.header.header().get("tenant") != null) "yes" else "no",
                });
                var changed = input;
                changed.args = try suffixFirst(a, input.args, "+in");
                changed.signal_args = try suffixFirst(a, input.signal_args, "+sig");
                changed.options.id = try std.mem.concat(a, u8, &.{ input.options.id, "-changed" });
                try input.header.set("via", @as([]const u8, "interceptor"));
                return next.signalWithStartWorkflow(changed);
            }

            pub fn executeNexusOperation(r: *Run, input: interceptor.NexusOperationInput, next: interceptor.WorkflowOutbound.Next) workflow.NexusOperationFuture(Payload) {
                if (r.s.observed) |o| if (!r.ctx.isReplaying()) o.add("nexus:{s}/{s}", .{ input.service, input.operation });
                return next.executeNexusOperation(input);
            }
        };
    };

    fn startEnv(options: struct {
        interceptors: []const interceptor.Worker = &.{},
        propagators: []const propagation.ContextPropagator = &.{},
    }) !*WorkflowEnvironment {
        const env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{
            .dev_server = .{ .extra_args = &signal_with_start_args },
            .worker_interceptors = options.interceptors,
            .propagators = options.propagators,
        });
        errdefer env.deinit();
        try env.registerWorkflow(Caller);
        try env.registerWorkflow(Target);
        return env;
    }

    /// Runs `Caller` as "<id>-caller" for target `id` with `values` propagated, and returns
    /// the target's result after checking the caller's answer names `want_id` as a new run.
    fn runCaller(env: *WorkflowEnvironment, a: std.mem.Allocator, id: []const u8, want_id: []const u8, values: ?*const propagation.Values) ![]const u8 {
        const caller_id = try std.fmt.allocPrint(a, "{s}-caller", .{id});
        const result = try env.executeWorkflowWithOptions(Caller, .{ .id = caller_id, .context = values, .execution_timeout = .fromSeconds(60) }, .{id}, a, null);
        var parts = std.mem.splitScalar(u8, result, ' ');
        try testing.expectEqualStrings(want_id, parts.next().?);
        const run_id = parts.next().?;
        try testing.expect(run_id.len > 0);
        try testing.expectEqualStrings("true", parts.next().?);
        var handle = try env.client.getWorkflow([]const u8, want_id, run_id);
        defer handle.deinit();
        return env.getResult(Target, &handle, a, null);
    }
};

test "a workflow outbound interceptor sees a signal-with-start's args and header, and its changes reach the target" {
    var observed: Observed = .{ .io = testing.io, .arena = .init(testing.allocator) };
    defer observed.arena.deinit();
    var shaping: SignalWithStartFixtures.Shaping = .{ .observed = &observed };
    const interceptors = [_]interceptor.Worker{interceptor.Worker.from(SignalWithStartFixtures.Shaping, &shaping)};
    const env = try SignalWithStartFixtures.startEnv(.{ .interceptors = &interceptors, .propagators = &SignalWithStartFixtures.propagators });
    defer env.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var values: propagation.Values = .init(a);
    try values.put(SignalWithStartFixtures.tenant, "acme");
    const id = try std.fmt.allocPrint(a, "sws-icpt-{s}", .{env.task_queue});
    const changed_id = try std.fmt.allocPrint(a, "{s}-changed", .{id});
    const result = try SignalWithStartFixtures.runCaller(env, a, id, changed_id, &values);
    try testing.expectEqualStrings("start=go+in signal=one+sig via=interceptor tenant=acme", result);
    const seen = try std.fmt.allocPrint(a, "sws:sws_icpt_target/add id={s} args=go signal=one tenant=yes", .{id});
    if (!observed.has(seen)) {
        std.debug.print("the interceptor never saw {s}; saw {any}\n", .{ seen, observed.events.items });
        return error.TestExpectedEvent;
    }
}

test "a propagated value reaches a signal-with-start's target in its header, not in Nexus headers" {
    const env = try SignalWithStartFixtures.startEnv(.{ .propagators = &SignalWithStartFixtures.propagators });
    defer env.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var values: propagation.Values = .init(a);
    try values.put(SignalWithStartFixtures.tenant, "acme");
    const id = try std.fmt.allocPrint(a, "sws-propagated-{s}", .{env.task_queue});
    try testing.expectEqualStrings("start=go signal=one via=none tenant=acme", try SignalWithStartFixtures.runCaller(env, a, id, id, &values));

    // The target's start carries the value in its header; the caller's Nexus command carries
    // no Nexus header.
    const target_history = try env.client.fetchHistory(a, id, "");
    const started = target_history.events.items[0].attributes.?.workflow_execution_started_event_attributes;
    var found = false;
    for (started.header.?.fields.items) |f| found = found or std.mem.eql(u8, f.key, "tenant");
    try testing.expect(found);
    const caller_history = try env.client.fetchHistory(a, try std.fmt.allocPrint(a, "{s}-caller", .{id}), "");
    var scheduled: usize = 0;
    for (caller_history.events.items) |event| switch (event.attributes.?) {
        .nexus_operation_scheduled_event_attributes => |attrs| {
            try testing.expectEqualStrings("__temporal_system", attrs.endpoint);
            try testing.expectEqual(@as(usize, 0), attrs.nexus_header.items.len);
            scheduled += 1;
        },
        else => {},
    };
    try testing.expectEqual(@as(usize, 1), scheduled);
}

test "an interceptor implementing executeNexusOperation and signalWithStartWorkflow sees a signal-with-start once, on signalWithStartWorkflow" {
    var observed: Observed = .{ .io = testing.io, .arena = .init(testing.allocator) };
    defer observed.arena.deinit();
    var shaping: SignalWithStartFixtures.Shaping = .{ .observed = &observed };
    const interceptors = [_]interceptor.Worker{interceptor.Worker.from(SignalWithStartFixtures.Shaping, &shaping)};
    const env = try SignalWithStartFixtures.startEnv(.{ .interceptors = &interceptors });
    defer env.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const id = try std.fmt.allocPrint(a, "sws-once-{s}", .{env.task_queue});
    _ = try SignalWithStartFixtures.runCaller(env, a, id, try std.fmt.allocPrint(a, "{s}-changed", .{id}), null);
    // Both entries are declared, so a call on executeNexusOperation would be recorded.
    var signal_with_start: usize = 0;
    var nexus_calls: usize = 0;
    for (observed.events.items) |e| {
        if (std.mem.startsWith(u8, e, "sws:")) signal_with_start += 1;
        if (std.mem.startsWith(u8, e, "nexus:")) nexus_calls += 1;
    }
    try testing.expectEqual(@as(usize, 0), nexus_calls);
    try testing.expectEqual(@as(usize, 1), signal_with_start);
    const outbound = interceptor.WorkflowOutbound.from(SignalWithStartFixtures.Shaping.Run, undefined);
    try testing.expect(outbound.vtable.executeNexusOperation != null and outbound.vtable.signalWithStartWorkflow != null);
}

/// A dev-server run of `SignalWithStartFixtures.Caller` for target
/// "sws-icpt-recorded-target" with `SignalWithStartFixtures.Shaping` installed (started with
/// the flag in `signal_with_start_args`), fetched with `Client.fetchHistory` and encoded as
/// ProtoJSON (`temporal.protojson.encode`), indented.
const signal_with_start_interceptor_history = @embedFile("testdata/signal_with_start_interceptor_history.json");

test "WorkflowReplayer replays a signal-with-start shaped by a workflow outbound interceptor" {
    var shaping: SignalWithStartFixtures.Shaping = .{};
    const interceptors = [_]interceptor.Worker{interceptor.Worker.from(SignalWithStartFixtures.Shaping, &shaping)};
    var replayer: testing_mod.WorkflowReplayer = .initWithOptions(testing.allocator, testing.io, .{ .interceptors = &interceptors });
    defer replayer.deinit();
    try replayer.registerWorkflow(SignalWithStartFixtures.Caller);
    try replayer.replayHistoryJson(signal_with_start_interceptor_history);

    // The recorded request is the one the interceptor changed.
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const history = try @import("history.zig").fromJson(a, signal_with_start_interceptor_history);
    const scheduled = history.events.items[4].attributes.?.nexus_operation_scheduled_event_attributes;
    const request = (try system_nexus.decode(a, scheduled.input.?)).signal_with_start_request;
    try testing.expectEqualStrings("sws-icpt-recorded-target-changed", request.workflow_id);
    try testing.expectEqualStrings("go+in", try converter.fromPayload([]const u8, a, request.input.?.payloads.items[0]));
    try testing.expectEqualStrings("one+sig", try converter.fromPayload([]const u8, a, request.signal_input.?.payloads.items[0]));
    try testing.expectEqualStrings("via", request.header.?.fields.items[0].key);
}
