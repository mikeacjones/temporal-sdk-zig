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
const proto = @import("proto.zig");
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
        .dev_server_extra_args = &testing_mod.DevServer.standalone_activity_args,
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
