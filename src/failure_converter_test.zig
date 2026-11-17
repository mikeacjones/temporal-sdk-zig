//! The failure converter against a dev server: a converter that stamps each failure's
//! message with the direction, the error and the context it was given shows where the
//! client, the workflow, its activities, local activities, children, updates, queries,
//! external signals and Nexus operations convert failures, and that replay converts the same.

const std = @import("std");
const activity = @import("activity.zig");
const workflow = @import("workflow.zig");
const converter = @import("converter.zig");
const failure_mod = @import("failure.zig");
const nexus = @import("nexus.zig");
const testing_mod = @import("testing.zig");
const WorkflowEnvironment = testing_mod.WorkflowEnvironment;
const Allocator = std.mem.Allocator;
const Failure = converter.Failure;

const testing = std.testing;

/// Every converter call's stamp, for checking what a replay converts. The converter runs on
/// several threads, so slots are claimed atomically; nothing reads them while a worker runs.
const Recorder = struct {
    var lines: [64][160]u8 = undefined;
    var lens: [64]usize = undefined;
    var count: std.atomic.Value(usize) = .init(0);

    fn record(line: []const u8) void {
        const i = count.fetchAdd(1, .monotonic);
        if (i >= lines.len) return;
        const n = @min(line.len, lines[i].len);
        @memcpy(lines[i][0..n], line[0..n]);
        lens[i] = n;
    }

    fn reset() void {
        count.store(0, .monotonic);
    }

    /// The recorded lines, sorted, allocated from `a`.
    fn sorted(a: Allocator) ![]const []const u8 {
        const n = @min(count.load(.monotonic), lines.len);
        const out = try a.alloc([]const u8, n);
        for (out, 0..) |*line, i| line.* = try a.dupe(u8, lines[i][0..lens[i]]);
        std.mem.sort([]const u8, out, {}, lessThan);
        return out;
    }

    fn lessThan(_: void, x: []const u8, y: []const u8) bool {
        return std.mem.order(u8, x, y) == .lt;
    }
};

/// Prefixes the head of each failure's message with "<to Error ctx>" on the way out and
/// "<from ctx>" on the way in, where ctx is "namespace|workflow ID|workflow type|activity
/// type|activity ID". Leaves the causes alone.
const Stamping = struct {
    const converter_value: converter.FailureConverter = .{ .ptr = undefined, .vtable = &.{ .toFailure = toFailure, .fromFailure = fromFailure } };

    fn contextText(arena: Allocator, c: *const converter.Context) error{OutOfMemory}![]const u8 {
        return std.fmt.allocPrint(arena, "{s}|{s}|{s}|{s}|{s}", .{ c.namespace, c.workflow_id, c.workflow_type, c.activity_type, c.activity_id });
    }

    fn toFailure(_: *anyopaque, arena: Allocator, context: *const converter.Context, err: ?anyerror, f: Failure) error{OutOfMemory}!Failure {
        const stamp = try std.fmt.allocPrint(arena, "<to {s} {s}>", .{ if (err) |e| @errorName(e) else "null", try contextText(arena, context) });
        Recorder.record(stamp);
        var out = f;
        out.message = try std.fmt.allocPrint(arena, "{s} {s}", .{ stamp, f.message });
        return out;
    }

    fn fromFailure(_: *anyopaque, arena: Allocator, context: *const converter.Context, f: Failure) error{OutOfMemory}!Failure {
        const stamp = try std.fmt.allocPrint(arena, "<from {s}>", .{try contextText(arena, context)});
        Recorder.record(stamp);
        var out = f;
        out.message = try std.fmt.allocPrint(arena, "{s} {s}", .{ stamp, f.message });
        return out;
    }
};

/// The messages of `f`'s cause chain, outermost first, joined by " <- ".
fn chain(a: Allocator, f: ?*const Failure) ![]const u8 {
    var out: std.Io.Writer.Allocating = .init(a);
    var it = f;
    while (it) |cur| : (it = cur.cause) {
        if (cur != f.?) out.writer.writeAll(" <- ") catch return error.OutOfMemory;
        out.writer.writeAll(cur.message) catch return error.OutOfMemory;
    }
    return out.written();
}

fn expectLines(expected: []const []const u8, actual: []const []const u8) !void {
    const want = try std.mem.join(testing.allocator, "\n", expected);
    defer testing.allocator.free(want);
    const got = try std.mem.join(testing.allocator, "\n", actual);
    defer testing.allocator.free(got);
    try testing.expectEqualStrings(want, got);
}

const no_retries: workflow.RetryPolicy = .{ .maximum_attempts = 1 };

const Fixtures = struct {
    fn charge(_: *activity.Context) ![]const u8 {
        return error.CardDeclined;
    }
    const Charge = activity.Def("FcCharge", charge);

    /// Fails with the activity's failure, passed on unchanged.
    fn pay(ctx: *workflow.Context) ![]const u8 {
        return ctx.executeActivity(Charge, .{ .start_to_close_timeout = .fromSeconds(10), .retry_policy = no_retries }, .{}).get();
    }
    const Pay = workflow.Def("FcPay", pay);
};

const pay_id = "failure-converter-pay";
const pay_run = "default|" ++ pay_id ++ "|FcPay||";
const pay_activity = "default|" ++ pay_id ++ "|FcPay|FcCharge|1";

test "a failure converter rewrites an activity's failure, the workflow's that passes it on, and what the client reads, and replay converts the same" {
    const env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{ .failure_converter = Stamping.converter_value });
    defer env.deinit();
    try env.registerWorkflow(Fixtures.Pay);
    try env.registerActivity(Fixtures.Charge);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    Recorder.reset();
    var run = try env.startWorkflow(Fixtures.Pay, .{ .id = pay_id, .task_queue = "" }, .{});
    defer run.deinit();
    var failure: ?failure_mod.Failure = null;
    try testing.expectError(error.WorkflowFailed, env.getResult(Fixtures.Pay, &run, a, &failure));

    // The history holds what the converter sent: the activity's failure, and the
    // workflow's, which is the activity's as the workflow read it.
    const history = try env.client.fetchHistory(a, run.id, run.run_id);
    var activity_failure: ?Failure = null;
    var workflow_failure: ?Failure = null;
    for (history.events.items) |event| switch (event.attributes orelse continue) {
        .activity_task_failed_event_attributes => |e| activity_failure = e.failure,
        .workflow_execution_failed_event_attributes => |e| workflow_failure = e.failure,
        else => {},
    };
    const sent_by_activity = "<to CardDeclined " ++ pay_activity ++ "> CardDeclined";
    try testing.expectEqualStrings(sent_by_activity, activity_failure.?.message);
    try testing.expectEqualStrings(failure_mod.applicationType(activity_failure.?).?, "CardDeclined");
    const sent_by_workflow = workflow_failure.?;
    try testing.expectStringStartsWith(sent_by_workflow.message, "<to ActivityFailed " ++ pay_run ++ "> <from " ++ pay_activity ++ "> ");
    try testing.expectEqualStrings(sent_by_activity, sent_by_workflow.cause.?.message);

    // The client reads the workflow's failure through the converter.
    const read = failure.?;
    try testing.expectEqualStrings(try std.fmt.allocPrint(a, "<from default|{s}|||> {s}", .{ pay_id, sent_by_workflow.message }), read.message);
    try testing.expectEqualStrings(sent_by_activity, read.cause.?.message);

    // Replay converts the workflow's failures with the same contexts it did live.
    Recorder.reset();
    try env.replayRun(run.id, run.run_id);
    try expectLines(&.{
        "<from " ++ pay_activity ++ ">",
        "<to ActivityFailed " ++ pay_run ++ ">",
    }, try Recorder.sorted(a));
}

const HandlerFixtures = struct {
    const Finish = workflow.SignalDef("finish", fn () void);
    const Peek = workflow.QueryDef("peek", fn () []const u8);
    const Set = workflow.UpdateDef("set", fn ([]const u8) []const u8);

    const State = struct {
        done: bool = false,

        fn finish(s: *State) void {
            s.done = true;
        }
        fn isDone(s: *State) bool {
            return s.done;
        }
        fn peek(_: *State) error{NotReady}![]const u8 {
            return error.NotReady;
        }
        fn set(_: *State, _: []const u8) error{HandlerBroke}![]const u8 {
            return error.HandlerBroke;
        }
        fn check(_: *State, v: []const u8) error{BadInput}!void {
            if (std.mem.eql(u8, v, "bad")) return error.BadInput;
        }
    };

    fn kid(_: *workflow.Context) !void {
        return error.KidBroke;
    }
    const Kid = workflow.Def("FcKid", kid);

    /// Reads the failures of a local activity, a child and an external signal, then waits
    /// for `finish` and returns their cause chains, one per line.
    fn handlers(ctx: *workflow.Context) ![]const u8 {
        const a = ctx.allocator();
        const s = try a.create(State);
        s.* = .{};
        try ctx.setSignalHandler(Finish, s, State.finish);
        try ctx.setQueryHandler(Peek, s, State.peek);
        try ctx.setUpdateHandlerWithValidator(Set, s, State.set, State.check);

        const local = ctx.executeLocalActivity(Fixtures.Charge, .{ .start_to_close_timeout = .fromSeconds(10), .retry_policy = no_retries }, .{});
        const local_read = if (local.get()) |_| "completed" else |err| switch (err) {
            error.ActivityFailed => try chain(a, local.failure()),
            else => return err,
        };
        const child = ctx.executeChildWorkflow(Kid, .{ .id = handlers_id ++ "-kid" }, .{});
        const child_read = if (child.get()) |_| "completed" else |err| switch (err) {
            error.ChildWorkflowFailed => try chain(a, child.failure()),
            else => return err,
        };
        const external = ctx.signalExternalWorkflow(Finish, handlers_id ++ "-missing", "", .{});
        const external_read = if (external.get()) |_| "delivered" else |err| switch (err) {
            error.ExternalWorkflowNotFound => try chain(a, external.failure()),
            else => return err,
        };
        try ctx.await(s, State.isDone);
        return std.mem.join(a, "\n", &.{ local_read, child_read, external_read });
    }
    const Handlers = workflow.Def("FcHandlers", handlers);
};

const handlers_id = "failure-converter-handlers";
const handlers_run = "default|" ++ handlers_id ++ "|FcHandlers||";
const handlers_client = "default|" ++ handlers_id ++ "|||";
const kid_run = "default|" ++ handlers_id ++ "-kid|FcKid||";

test "a failure converter rewrites update rejections and query failures, and the failures a workflow reads of local activities, children and external signals" {
    const env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{ .failure_converter = Stamping.converter_value });
    defer env.deinit();
    try env.registerWorkflow(HandlerFixtures.Handlers);
    try env.registerWorkflow(HandlerFixtures.Kid);
    try env.registerActivity(Fixtures.Charge);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var run = try env.startWorkflow(HandlerFixtures.Handlers, .{ .id = handlers_id, .task_queue = "" }, .{});
    defer run.deinit();

    // A query's failure reaches the client as a message only, as the converter sent it.
    var message: []const u8 = "";
    try testing.expectError(error.QueryFailed, env.client.queryWorkflowWithOptions(HandlerFixtures.Peek, a, run.id, "", .{}, .{ .failure_message = &message }));
    try testing.expectStringStartsWith(message, "<to NotReady " ++ handlers_run ++ "> NotReady");

    // Rejections by the validator and by the handler, sent and read through the converter.
    var why: ?failure_mod.Failure = null;
    try testing.expectError(error.UpdateFailed, env.updateWorkflow(HandlerFixtures.Set, a, run.id, .{"bad"}, &why));
    try testing.expectEqualStrings("<from " ++ handlers_client ++ "> <to BadInput " ++ handlers_run ++ "> BadInput", why.?.message);
    try testing.expectError(error.UpdateFailed, env.updateWorkflow(HandlerFixtures.Set, a, run.id, .{"ok"}, &why));
    try testing.expectEqualStrings("<from " ++ handlers_client ++ "> <to HandlerBroke " ++ handlers_run ++ "> HandlerBroke", why.?.message);

    try env.signalWorkflow(HandlerFixtures.Finish, run.id, .{});
    // The result also replays the history with the converter.
    const result = try env.getResult(HandlerFixtures.Handlers, &run, a, null);
    var lines = std.mem.splitScalar(u8, result, '\n');
    const local_read = lines.next().?;
    const child_read = lines.next().?;
    const external_read = lines.next().?;
    // The core delivers a local activity's failure as the activity sent it.
    const local_activity = "default|" ++ handlers_id ++ "|FcHandlers|FcCharge|1";
    try testing.expectEqualStrings("<from " ++ local_activity ++ "> <to CardDeclined " ++ local_activity ++ "> CardDeclined", local_read);
    try testing.expectStringStartsWith(child_read, "<from " ++ kid_run ++ "> ");
    try testing.expectStringEndsWith(child_read, " <- <to KidBroke " ++ kid_run ++ "> KidBroke");
    try testing.expectStringStartsWith(external_read, "<from " ++ handlers_run ++ "> ");
    try testing.expect(lines.next() == null);
}

const NexusFixtures = struct {
    const Refuse = nexus.OperationDef("refuse", []const u8, []const u8);
    const Service = nexus.ServiceDef("failure-converter-service", .{Refuse});

    fn refuse(ctx: *nexus.Context, input: []const u8) ![]const u8 {
        return ctx.fail(.{ .message = try std.fmt.allocPrint(ctx.allocator(), "out of stock: {s}", .{input}), .type = "OutOfStock" });
    }
    const Handler = nexus.ServiceHandler(Service, .{nexus.OperationHandler(Refuse, refuse)});

    /// Calls `refuse` and returns the cause chain of its failure.
    fn call(ctx: *workflow.Context, endpoint: []const u8) ![]const u8 {
        const f = ctx.executeNexusOperation(Service, Refuse, .{ .endpoint = endpoint, .schedule_to_close_timeout = .fromSeconds(60) }, "socks");
        return if (f.get()) |_| "completed" else |err| switch (err) {
            error.NexusOperationFailed => chain(ctx.allocator(), f.failure()),
            else => err,
        };
    }
    const Call = workflow.Def("FcNexusCall", call);
};

test "a failure converter rewrites a Nexus handler's failure and the failure the calling workflow reads" {
    const env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{ .failure_converter = Stamping.converter_value });
    defer env.deinit();
    try env.registerWorkflow(NexusFixtures.Call);
    try env.registerNexusService(NexusFixtures.Handler);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var suffix: [4]u8 = undefined;
    testing.io.random(&suffix);
    const endpoint = try std.fmt.allocPrint(a, "failure-converter-{x}", .{suffix});
    try env.createNexusEndpoint(endpoint, .{});

    var run = try env.startWorkflow(NexusFixtures.Call, .{ .task_queue = "" }, .{endpoint});
    defer run.deinit();
    const result = try env.getResult(NexusFixtures.Call, &run, a, null);
    try testing.expectStringStartsWith(result, "<from default||||> ");
    if (std.mem.indexOf(u8, result, "<to OperationFailed default||||> out of stock: socks") == null) {
        std.debug.print("the Nexus failure's chain lacks the handler's converted failure: {s}\n", .{result});
        return error.TestUnexpectedResult;
    }
}

test "without a failure converter, failures are sent and read as the SDK builds them" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(Fixtures.Pay);
    try env.registerActivity(Fixtures.Charge);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    Recorder.reset();
    var run = try env.startWorkflow(Fixtures.Pay, .{ .task_queue = "" }, .{});
    defer run.deinit();
    var failure: ?failure_mod.Failure = null;
    try testing.expectError(error.WorkflowFailed, env.getResult(Fixtures.Pay, &run, a, &failure));
    try testing.expectEqualStrings("CardDeclined", failure.?.cause.?.message);
    try testing.expect(std.mem.indexOf(u8, failure.?.message, "<") == null);
    try testing.expectEqual(0, Recorder.count.load(.monotonic));
}

const AsyncFixtures = struct {
    var token_buf: [512]u8 = undefined;
    var token_len: std.atomic.Value(usize) = .init(0);

    fn later(ctx: *activity.Context) !void {
        const token = ctx.info().task_token;
        @memcpy(token_buf[0..token.len], token);
        token_len.store(token.len, .release);
        return ctx.completeAsync();
    }
    const Later = activity.Def("FcLater", later);

    fn waitLater(ctx: *workflow.Context) ![]const u8 {
        const f = ctx.executeActivity(Later, .{ .start_to_close_timeout = .fromSeconds(30), .retry_policy = no_retries }, .{});
        f.get() catch |err| switch (err) {
            error.ActivityFailed => return chain(ctx.allocator(), f.failure()),
            else => return err,
        };
        return "completed";
    }
    const WaitLater = workflow.Def("FcWaitLater", waitLater);
};

test "Client.failActivity and failActivityById send the failure through the converter, with no error and the call's context" {
    const env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{ .failure_converter = Stamping.converter_value });
    defer env.deinit();
    try env.registerWorkflow(AsyncFixtures.WaitLater);
    try env.registerActivity(AsyncFixtures.Later);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    for ([_]bool{ false, true }) |by_id| {
        AsyncFixtures.token_len.store(0, .release);
        var run = try env.startWorkflow(AsyncFixtures.WaitLater, .{ .task_queue = "" }, .{});
        defer run.deinit();
        while (AsyncFixtures.token_len.load(.acquire) == 0) try testing.io.sleep(.fromMilliseconds(10), .awake);
        const f: failure_mod.Application = .{ .message = "gave up", .type = "GaveUp", .non_retryable = true };
        const sent = if (by_id) sent: {
            try env.client.failActivityById(.{ .workflow_id = run.id, .run_id = run.run_id, .activity_id = "1" }, f);
            break :sent try std.fmt.allocPrint(a, "<to null default|{s}|||1> gave up", .{run.id});
        } else sent: {
            try env.client.failActivity(AsyncFixtures.token_buf[0..AsyncFixtures.token_len.load(.acquire)], f);
            break :sent "<to null default||||> gave up";
        };
        const read = try env.getResult(AsyncFixtures.WaitLater, &run, a, null);
        try testing.expectStringStartsWith(read, try std.fmt.allocPrint(a, "<from default|{s}|FcWaitLater|FcLater|1> ", .{run.id}));
        try testing.expectStringEndsWith(read, try std.fmt.allocPrint(a, " <- {s}", .{sent}));
    }
}

const RetryFixtures = struct {
    fn flaky(_: *workflow.Context) !void {
        return error.Flaky;
    }
    const Flaky = workflow.Def("FcFlaky", flaky);
};

test "the environment reads a retried run's own failure through the converter, as WorkflowRun.get does" {
    const env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{ .failure_converter = Stamping.converter_value });
    defer env.deinit();
    try env.registerWorkflow(RetryFixtures.Flaky);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const id = "failure-converter-flaky";
    var run = try env.startWorkflow(RetryFixtures.Flaky, .{ .id = id, .task_queue = "", .retry_policy = .{ .maximum_attempts = 2, .initial_interval = .fromMilliseconds(100) } }, .{});
    defer run.deinit();
    var failure: ?failure_mod.Failure = null;
    try testing.expectError(error.WorkflowFailed, env.getResultWithOptions(RetryFixtures.Flaky, &run, .{ .follow_runs = false }, a, &failure));
    try testing.expectEqualStrings("<from default|" ++ id ++ "|||> <to Flaky default|" ++ id ++ "|FcFlaky||> Flaky", failure.?.message);
}
