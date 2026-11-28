//! Workflow context APIs tested end to end against a dev server and the workflow replayer.

const std = @import("std");
const workflow = @import("workflow.zig");
const activity = @import("activity.zig");
const converter = @import("converter.zig");
const interceptor = @import("interceptor.zig");
const testing_ns = @import("testing.zig");
const log = @import("log.zig");
const worker_mod = @import("worker.zig");
const builtin = @import("builtin");

const testing = std.testing;

const Echo = activity.Def("uuid_echo", struct {
    fn echo(_: *activity.Context, id: []const u8) ![]const u8 {
        return id;
    }
}.echo);

const Child = workflow.Def("uuid_child", struct {
    fn run(ctx: *workflow.Context) ![]const u8 {
        return ctx.info().workflow_id;
    }
}.run);

/// Draws two UUIDs, starts a child with the first as its ID and an activity with the second as
/// its argument, and returns "<child id> <activity arg> <child's answer> <activity's answer>".
const UuidWorkflow = workflow.Def("uuid_workflow", struct {
    fn run(ctx: *workflow.Context) ![]const u8 {
        const a = ctx.allocator();
        const child_id = try ctx.randomUuid();
        const arg = try ctx.randomUuid();
        if (std.mem.eql(u8, child_id, arg)) return error.SameUuidTwice;
        const child = try ctx.executeChildWorkflow(Child, .{ .id = child_id, .execution_timeout = .fromSeconds(60) }, .{}).get();
        const echoed = try ctx.executeActivity(Echo, .{ .start_to_close_timeout = .fromSeconds(10) }, .{arg}).get();
        return std.fmt.allocPrint(a, "{s} {s} {s} {s}", .{ child_id, arg, child, echoed });
    }
}.run);

fn expectUuid4(text: []const u8) !void {
    try testing.expectEqual(@as(usize, 36), text.len);
    for (text, 0..) |c, i| switch (i) {
        8, 13, 18, 23 => try testing.expectEqual(@as(u8, '-'), c),
        else => try testing.expect(std.ascii.isDigit(c) or (c >= 'a' and c <= 'f')),
    };
    try testing.expectEqual(@as(u8, '4'), text[14]);
    try testing.expect(std.mem.indexOfScalar(u8, "89ab", text[19]) != null);
}

test "workflow: randomUuid gives distinct v4 UUIDs that commands carry and a replay reproduces" {
    const env = try testing_ns.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(UuidWorkflow);
    try env.registerWorkflow(Child);
    try env.registerActivity(Echo);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var run = try env.startWorkflow(UuidWorkflow, .{}, .{});
    defer run.deinit();
    const result = try env.getResult(UuidWorkflow, &run, a, null);

    var parts = std.mem.splitScalar(u8, result, ' ');
    const child_id = parts.next().?;
    const arg = parts.next().?;
    try expectUuid4(child_id);
    try expectUuid4(arg);
    try testing.expect(!std.mem.eql(u8, child_id, arg));
    try testing.expectEqualStrings(child_id, parts.next().?);
    try testing.expectEqualStrings(arg, parts.next().?);

    const history = try env.client.fetchHistory(a, run.id, run.run_id);
    var replayer: testing_ns.WorkflowReplayer = .init(testing.allocator, testing.io);
    defer replayer.deinit();
    try replayer.registerWorkflow(UuidWorkflow);
    try replayer.replayHistory(history);
}

test "workflow: two runs draw different UUIDs" {
    const env = try testing_ns.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(UuidWorkflow);
    try env.registerWorkflow(Child);
    try env.registerActivity(Echo);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const first = try env.executeWorkflow(UuidWorkflow, .{}, a, null);
    const second = try env.executeWorkflow(UuidWorkflow, .{}, a, null);
    try testing.expect(!std.mem.eql(u8, first[0..36], second[0..36]));
}

/// A workflow that logs an error when its activity wait fails, waiting on an activity sent to a
/// task queue nothing polls. Its update returns once the activity is scheduled, so the root
/// coroutine is blocked in `get` when the update's answer reaches the client.
const EvictionLog = struct {
    const Scheduled = workflow.UpdateDef("eviction_log_scheduled", fn () void);
    const message = "eviction_log: activity wait failed";

    const State = struct {
        ctx: *workflow.Context,
        scheduled: bool = false,
        fn waitScheduled(s: *State) !void {
            try s.ctx.await(s, isScheduled);
        }
        fn isScheduled(s: *State) bool {
            return s.scheduled;
        }
    };

    fn run(ctx: *workflow.Context) ![]const u8 {
        const s = try ctx.allocator().create(State);
        s.* = .{ .ctx = ctx };
        try ctx.setUpdateHandler(Scheduled, s, State.waitScheduled);
        const future = ctx.executeActivityByName([]const u8, "eviction_log_never", .{
            .task_queue = "eviction-log-unpolled",
            .start_to_close_timeout = .fromSeconds(600),
        }, .{});
        s.scheduled = true;
        return future.get() catch |err| {
            ctx.logger().err(message, .{ .err = err });
            return err;
        };
    }
    const Wf = workflow.Def("eviction_log", run);

    /// Counts the error records with `message`. Called from the worker's threads.
    const Sink = struct {
        errors: std.atomic.Value(usize) = .init(0),

        fn sink(r: *Sink) log.Sink {
            return .{ .ptr = r, .vtable = &.{ .log = record } };
        }

        fn record(ptr: ?*anyopaque, level: log.Level, text: []const u8, _: []const log.Field) void {
            const r: *Sink = @ptrCast(@alignCast(ptr.?));
            if (level == .err and std.mem.eql(u8, text, message)) _ = r.errors.fetchAdd(1, .monotonic);
        }
    };
};

test "workflow: a run evicted at worker shutdown while it waits on an activity logs no error, and its history replays" {
    var sink: EvictionLog.Sink = .{};
    const env = try testing_ns.WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{ .logger = sink.sink() });
    var env_open = true;
    defer if (env_open) env.deinit();
    try env.registerWorkflow(EvictionLog.Wf);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const history = history: {
        var run = try env.startWorkflow(EvictionLog.Wf, .{}, .{});
        defer run.deinit(); // before env.deinit, which frees the client the run points at
        try env.updateWorkflow(EvictionLog.Scheduled, a, run.id, .{}, null);
        break :history try env.client.fetchHistory(a, run.id, run.run_id);
    };

    // Stopping the worker evicts the run, which unwinds its code out of `get` with
    // error.Canceled: nothing failed, so nothing may be logged at error level.
    env_open = false;
    env.deinit();
    try testing.expectEqual(@as(usize, 0), sink.errors.load(.monotonic));

    var replayer: testing_ns.WorkflowReplayer = .init(testing.allocator, testing.io);
    defer replayer.deinit();
    try replayer.registerWorkflow(EvictionLog.Wf);
    try replayer.replayHistory(history);
}

const ContinueByName = struct {
    fn seconds(d: ?std.Io.Duration) i64 {
        return if (d) |v| @intCast(@divFloor(v.nanoseconds, std.time.ns_per_s)) else -1;
    }

    /// What the interceptor `Marking` wrote into the run's start header, or "none".
    fn mark(ctx: *workflow.Context) ![]const u8 {
        const payload = ctx.headers().get("continued_as") orelse return "none";
        return converter.fromPayload([]const u8, ctx.allocator(), payload);
    }

    /// Runs any type: counts from its first argument up to its second, continuing as new under
    /// the type it runs as, named by `ctx.info().workflow_type` on even counts and by the empty
    /// name on odd ones. The last run names its type, count, header mark, and whether it was
    /// continued.
    fn chain(ctx: *workflow.Context, args: converter.EncodedValues) ![]const u8 {
        const a = ctx.allocator();
        const count = try args.get(u32, a, 0);
        const until = try args.get(u32, a, 1);
        if (count < until) {
            const name = if (count % 2 == 0) ctx.info().workflow_type else "";
            return ctx.continueAsNewByName(name, .{}, .{ count + 1, until });
        }
        const continued = if (ctx.info().continued_run_id.len > 0) "yes" else "no";
        return std.fmt.allocPrint(a, "{s} count={d} mark={s} continued={s}", .{ ctx.info().workflow_type, count, try mark(ctx), continued });
    }
    const Chain = workflow.DynamicDef(chain);

    /// Continues as the registered type "by_name_finisher", its name built in a buffer that
    /// is gone once this returns, with a new task timeout and the rest of the options kept.
    fn handoff(ctx: *workflow.Context, name: []const u8) ![]const u8 {
        var buf: [32]u8 = undefined;
        const target = try std.fmt.bufPrint(&buf, "by_name_{s}", .{"finisher"});
        return ctx.continueAsNewByName(target, .{ .task_timeout = .fromSeconds(7) }, .{ name, @as(u32, 2) });
    }
    const Handoff = workflow.Def("by_name_handoff", handoff);

    fn finish(ctx: *workflow.Context, name: []const u8, times: u32) ![]const u8 {
        const info = ctx.info();
        return std.fmt.allocPrint(ctx.allocator(), "{s} {s} x{d} run_timeout={d} task_timeout={d} mark={s}", .{
            info.workflow_type, name, times, seconds(info.run_timeout), seconds(info.task_timeout), try mark(ctx),
        });
    }
    const Finisher = workflow.Def("by_name_finisher", finish);

    /// Writes the workflow type each continue-as-new names into its header, which becomes the
    /// next run's start header.
    const Marking = struct {
        pub fn interceptWorkflow(m: *Marking, ctx: *workflow.Context) error{OutOfMemory}!interceptor.Workflow {
            _ = ctx;
            return .{ .outbound = interceptor.WorkflowOutbound.from(Marking, m) };
        }

        pub fn continueAsNew(_: *Marking, input: interceptor.ContinueAsNewInput, next: interceptor.WorkflowOutbound.Next) interceptor.WorkflowOutbound.ContinueAsNewError!void {
            try input.header.set("continued_as", input.workflow_type);
            return next.continueAsNew(input);
        }
    };
};

test "workflow: a dynamic workflow continues as new by name under its own runtime type" {
    var marking: ContinueByName.Marking = .{};
    const interceptors = [_]interceptor.Worker{interceptor.Worker.from(ContinueByName.Marking, &marking)};
    const env = try testing_ns.WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{ .worker_interceptors = &interceptors });
    defer env.deinit();
    try env.registerDynamicWorkflow(ContinueByName.Chain);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var run = try env.startWorkflowByName([]const u8, "by_name_counter", .{ .task_queue = "" }, .{ @as(u32, 0), @as(u32, 3) });
    defer run.deinit();
    const first_run_id = try a.dupe(u8, run.run_id);
    // The client follows the chain to the last run's result.
    try testing.expectEqualStrings("by_name_counter count=3 mark=by_name_counter continued=yes", try env.getResultByName([]const u8, &run, a, null));

    // The first run's history ends in a continue-as-new under the type it ran as, with the
    // new arguments, and replays.
    const history = try env.client.fetchHistory(a, run.id, first_run_id);
    const events = history.events.items;
    const last = events[events.len - 1].attributes.?.workflow_execution_continued_as_new_event_attributes;
    try testing.expectEqualStrings("by_name_counter", last.workflow_type.?.name);
    const input = last.input.?.payloads.items;
    try testing.expectEqual(@as(usize, 2), input.len);
    try testing.expectEqual(@as(u32, 1), try converter.fromPayload(u32, a, input[0]));
    try testing.expectEqual(@as(u32, 3), try converter.fromPayload(u32, a, input[1]));

    var replayer: testing_ns.WorkflowReplayer = .initWithOptions(testing.allocator, testing.io, .{ .interceptors = &interceptors });
    defer replayer.deinit();
    try replayer.registerDynamicWorkflow(ContinueByName.Chain);
    try replayer.replayHistory(history);
}

test "workflow: continueAsNewByName continues as another registered type, keeping unset options" {
    var marking: ContinueByName.Marking = .{};
    const interceptors = [_]interceptor.Worker{interceptor.Worker.from(ContinueByName.Marking, &marking)};
    const env = try testing_ns.WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{ .worker_interceptors = &interceptors });
    defer env.deinit();
    try env.registerWorkflow(ContinueByName.Handoff);
    try env.registerWorkflow(ContinueByName.Finisher);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var run = try env.startWorkflowByName([]const u8, "by_name_handoff", .{ .task_queue = "", .run_timeout = .fromSeconds(60) }, .{"ann"});
    defer run.deinit();
    try testing.expectEqualStrings(
        "by_name_finisher ann x2 run_timeout=60 task_timeout=7 mark=by_name_finisher",
        try env.getResultByName([]const u8, &run, a, null),
    );
}

/// The first run of a dev-server run of the dynamic `ContinueByName.Chain` started as type
/// "by_name_counter" with `.{ 0, 1 }`, which continued as new by name under that type;
/// fetched with `Client.fetchHistory` and encoded as ProtoJSON (`History.jsonEncode`).
const continue_as_new_by_name_history = @embedFile("testdata/continue_as_new_by_name_history.json");

fn countDirectly(ctx: *workflow.Context, args: converter.EncodedValues) ![]const u8 {
    _ = args;
    return ctx.info().workflow_type;
}

test "WorkflowReplayer replays a run that continued as new by name and catches one that completes instead" {
    var ok: testing_ns.WorkflowReplayer = .init(testing.allocator, testing.io);
    defer ok.deinit();
    try ok.registerDynamicWorkflow(ContinueByName.Chain);
    try ok.replayHistoryJson(continue_as_new_by_name_history);

    var changed: testing_ns.WorkflowReplayer = .init(testing.allocator, testing.io);
    defer changed.deinit();
    try changed.registerDynamicWorkflow(workflow.DynamicDef(countDirectly));
    try testing.expectError(error.Nondeterminism, changed.replayHistoryJson(continue_as_new_by_name_history));
}

const ContinueAsNewMemo = struct {
    /// The first run upserts the memo's status and continues as new: with an explicit empty
    /// memo when `clear` is false, after removing the status key and with default options
    /// when it is true. The second run returns its memo's status, or "none".
    fn run(ctx: *workflow.Context, continued: bool, clear: bool) ![]const u8 {
        if (continued) return try ctx.memoValue([]const u8, "status") orelse "none";
        if (clear) {
            try ctx.upsertMemo(.{ .status = null });
            return ctx.continueAsNew(Def, .{}, .{ true, clear });
        }
        try ctx.upsertMemo(.{ .status = "updated" });
        return ctx.continueAsNew(Def, .{ .memo = &.{} }, .{ true, clear });
    }
    const Def = workflow.Def("can_memo", run);
};

test "workflow: continue-as-new with an empty memo keeps the upserted memo, and a memo emptied by upserts gets the start memo back" {
    const env = try testing_ns.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(ContinueAsNewMemo.Def);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const start_memo = try converter.toFields(a, .{ .status = "original" });
    try testing.expectEqualStrings("updated", try env.executeWorkflowWithOptions(ContinueAsNewMemo.Def, .{ .memo = start_memo }, .{ false, false }, a, null));
    // The documented limit: core fills an empty command memo with the start memo.
    try testing.expectEqualStrings("original", try env.executeWorkflowWithOptions(ContinueAsNewMemo.Def, .{ .memo = start_memo }, .{ false, true }, a, null));
}

const ChildCancel = struct {
    const Finish = workflow.SignalDef("finish", fn () void);

    /// Reports the priority it runs with as "<key>|<fairness key>|<weight>".
    fn reportPriority(ctx: *workflow.Context) ![]const u8 {
        const p = ctx.info().priority;
        return std.fmt.allocPrint(ctx.allocator(), "{d}|{s}|{d}", .{ p.priority_key, p.fairness_key, p.fairness_weight });
    }
    const ReportPriority = workflow.Def("child_report_priority", reportPriority);

    /// Starts `ReportPriority` once with a priority that sets only the key and once without,
    /// and returns both answers joined by a space.
    fn prioritizedChildren(ctx: *workflow.Context) ![]const u8 {
        const a = ctx.allocator();
        const id = ctx.info().workflow_id;
        const set = ctx.executeChildWorkflow(ReportPriority, .{ .id = try std.fmt.allocPrint(a, "{s}-set", .{id}), .priority = .{ .priority_key = 2 } }, .{});
        const unset = ctx.executeChildWorkflow(ReportPriority, .{ .id = try std.fmt.allocPrint(a, "{s}-unset", .{id}) }, .{});
        return std.fmt.allocPrint(a, "{s} {s}", .{ try set.get(), try unset.get() });
    }
    const PrioritizedChildren = workflow.Def("child_prioritized_children", prioritizedChildren);

    /// Waits until canceled, then cleans up until the `Finish` signal and ends canceled.
    fn slowCleanup(ctx: *workflow.Context) ![]const u8 {
        const finish = try ctx.signalChannel(Finish);
        ctx.io().sleep(.fromSeconds(24 * 60 * 60), .awake) catch |err| switch (err) {
            error.Canceled => {
                _ = try finish.receive();
                return error.Canceled;
            },
        };
        return "not canceled";
    }
    const SlowCleanup = workflow.Def("child_slow_cleanup", slowCleanup);

    fn getChild(f: workflow.ChildWorkflowFuture([]const u8)) workflow.ChildWorkflowError![]const u8 {
        return f.get();
    }

    /// Starts `SlowCleanup` as `child_id` with a priority under `cancellation_type`,
    /// abandoned when this workflow closes, waits in its `get` in a coroutine, and cancels
    /// that coroutine once the child started. Returns what the canceled `get` returned.
    fn cancelChild(ctx: *workflow.Context, cancellation_type: workflow.ChildWorkflowCancellationType, child_id: []const u8) ![]const u8 {
        const io = ctx.io();
        const f = ctx.executeChildWorkflow(SlowCleanup, .{
            .id = child_id,
            .cancellation_type = cancellation_type,
            .parent_close_policy = .abandon,
            .priority = .{ .priority_key = 2 },
        }, .{});
        _ = try f.started();
        var waiting = io.async(getChild, .{f});
        _ = waiting.cancel(io) catch |err| return @errorName(err);
        return "not canceled";
    }
    const CancelChild = workflow.Def("child_cancel_requested", cancelChild);

    /// `cancelChild` under another cancellation type.
    fn cancelChildCompleted(ctx: *workflow.Context, cancellation_type: workflow.ChildWorkflowCancellationType, child_id: []const u8) ![]const u8 {
        _ = cancellation_type;
        return cancelChild(ctx, .wait_cancellation_completed, child_id);
    }
    fn cancelChildAbandon(ctx: *workflow.Context, cancellation_type: workflow.ChildWorkflowCancellationType, child_id: []const u8) ![]const u8 {
        _ = cancellation_type;
        return cancelChild(ctx, .abandon, child_id);
    }
    /// `cancelChild` without the child.
    fn noChild(ctx: *workflow.Context, cancellation_type: workflow.ChildWorkflowCancellationType, child_id: []const u8) ![]const u8 {
        _ = ctx;
        _ = cancellation_type;
        _ = child_id;
        return "Canceled";
    }

    /// Sleeps for a day; canceled, it ends canceled.
    fn sleepLong(ctx: *workflow.Context) !void {
        try ctx.io().sleep(.fromSeconds(24 * 60 * 60), .awake);
    }
    const SleepLong = workflow.Def("child_sleep_long", sleepLong);

    /// Starts `SleepLong` with the default parent close policy, waits for its start but not
    /// its result, then sleeps until canceled.
    fn leaveChild(ctx: *workflow.Context) !void {
        const f = ctx.executeChildWorkflow(SleepLong, .{ .id = try std.fmt.allocPrint(ctx.allocator(), "{s}-child", .{ctx.info().workflow_id}) }, .{});
        _ = try f.started();
        try ctx.io().sleep(.fromSeconds(24 * 60 * 60), .awake);
    }
    const LeaveChild = workflow.Def("child_left_unawaited", leaveChild);

    const EventTag = std.meta.Tag(@import("proto.zig").temporal.api.history.v1.HistoryEvent.attributes_union);

    /// Waits, long-polling the server, until the run records an event of kind `tag`. Fails if
    /// the run closes first.
    fn awaitEvent(c: anytype, id: []const u8, comptime tag: EventTag) !void {
        var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
        defer scratch.deinit();
        var it = try c.historyIterator(id, "", .{ .wait_new_event = true });
        defer it.deinit();
        while (try it.next(scratch.allocator())) |event| {
            if (event.attributes) |attrs| if (attrs == tag) return;
        }
        return error.TestUnexpectedResult;
    }
};

test "workflow: a child gets the priority its parent sets, and the parent's when it sets none" {
    const env = try testing_ns.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(ChildCancel.PrioritizedChildren);
    try env.registerWorkflow(ChildCancel.ReportPriority);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const result = try env.executeWorkflowWithOptions(ChildCancel.PrioritizedChildren, .{
        .priority = .{ .priority_key = 3, .fairness_key = "tenant-a", .fairness_weight = 1.5 },
    }, .{}, arena.allocator(), null);
    // The set key overrides the parent's; the zero fields inherit its values.
    try testing.expectEqualStrings("2|tenant-a|1.5 3|tenant-a|1.5", result);
}

test "workflow: wait_cancellation_requested lets the parent go on while the canceled child cleans up" {
    const env = try testing_ns.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(ChildCancel.CancelChild);
    try env.registerWorkflow(ChildCancel.SlowCleanup);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var suffix: [4]u8 = undefined;
    testing.io.random(&suffix);
    const child_id = try std.fmt.allocPrint(a, "cancel-requested-child-{x}", .{suffix});
    var run = try env.startWorkflow(ChildCancel.CancelChild, .{}, .{ .wait_cancellation_requested, child_id });
    defer run.deinit();
    // Replays the run too.
    try testing.expectEqualStrings("Canceled", try env.getResult(ChildCancel.CancelChild, &run, a, null));

    // The parent closed while the child, its cancellation recorded, still cleans up.
    const child = try env.client.describeWorkflow(a, child_id, "");
    try testing.expectEqual(.running, child.status);
    try testing.expectEqual(@as(i32, 2), child.raw.workflow_execution_info.?.priority.?.priority_key);
    var recorded = false;
    const history = try env.client.fetchHistory(a, run.id, run.run_id);
    for (history.events.items) |event| switch (event.attributes orelse continue) {
        .external_workflow_execution_cancel_requested_event_attributes => recorded = true,
        .child_workflow_execution_canceled_event_attributes => return error.TestUnexpectedResult,
        else => {},
    };
    try testing.expect(recorded);

    try env.client.signalWorkflow(ChildCancel.Finish, child_id, "", .{});
    var child_run = try env.client.getWorkflow([]const u8, child_id, "");
    defer child_run.deinit();
    try testing.expectError(error.WorkflowCanceled, child_run.get(a));
}

test "workflow: canceling a parent leaves the child nobody awaits to its parent close policy" {
    const env = try testing_ns.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(ChildCancel.LeaveChild);
    try env.registerWorkflow(ChildCancel.SleepLong);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var run = try env.startWorkflow(ChildCancel.LeaveChild, .{}, .{});
    defer run.deinit();
    try ChildCancel.awaitEvent(&env.client, run.id, .timer_started_event_attributes);
    try env.client.cancelWorkflow(run.id, "", "stop");
    try testing.expectError(error.WorkflowCanceled, env.getResult(ChildCancel.LeaveChild, &run, a, null));

    // Not canceled by the parent (it would have ended canceled): terminated by the default
    // parent close policy when the parent closed.
    const child_id = try std.fmt.allocPrint(a, "{s}-child", .{run.id});
    try ChildCancel.awaitEvent(&env.client, child_id, .workflow_execution_terminated_event_attributes);
    const child_history = try env.client.fetchHistory(a, child_id, "");
    for (child_history.events.items) |event| switch (event.attributes orelse continue) {
        .workflow_execution_cancel_requested_event_attributes => return error.TestUnexpectedResult,
        else => {},
    };
    try testing.expectEqual(.terminated, (try env.client.describeWorkflow(a, child_id, "")).status);
}

/// A dev-server run of `ChildCancel.CancelChild` under `wait_cancellation_requested`: it
/// starts the child with priority key 2, cancels the coroutine waiting in its `get`, and
/// completes once the server recorded the request, while the child still runs. Fetched with
/// `Client.fetchHistory` and encoded as ProtoJSON (`History.jsonEncode`), the worker's
/// identity replaced.
const child_cancel_requested_history = @embedFile("testdata/child_cancel_requested_history.json");

test "WorkflowReplayer replays a child canceled under wait_cancellation_requested and catches another type or a removed child" {
    var ok: testing_ns.WorkflowReplayer = .init(testing.allocator, testing.io);
    defer ok.deinit();
    try ok.registerWorkflow(ChildCancel.CancelChild);
    try ok.replayHistoryJson(child_cancel_requested_history);

    // `try_cancel` sends the same request and isn't caught: the core accepts the completion
    // in the task after the request's, where this history has it.
    inline for (.{ ChildCancel.cancelChildCompleted, ChildCancel.cancelChildAbandon, ChildCancel.noChild }) |changed_fn| {
        var changed: testing_ns.WorkflowReplayer = .init(testing.allocator, testing.io);
        defer changed.deinit();
        try changed.registerWorkflow(workflow.Def("child_cancel_requested", changed_fn));
        try testing.expectError(error.Nondeterminism, changed.replayHistoryJson(child_cancel_requested_history));
    }
}

const StackTraceQuery = workflow.StackTraceQuery;
const ReleaseBlocked = workflow.SignalDef("release", fn () void);

/// Waits until `release` is signaled, with a coroutine sleeping for an hour beside it.
const BlockedWorkflow = workflow.Def("stack_trace_blocked", struct {
    fn release(released: *bool) void {
        released.* = true;
    }
    fn isReleased(released: *bool) bool {
        return released.*;
    }
    fn nap(ctx: *workflow.Context) void {
        ctx.sleep(.fromSeconds(3600), .{}) catch {};
    }
    fn run(ctx: *workflow.Context) ![]const u8 {
        const released = try ctx.allocator().create(bool);
        released.* = false;
        try ctx.setSignalHandler(ReleaseBlocked, released, release);
        var napping = ctx.io().async(nap, .{ctx});
        defer napping.cancel(ctx.io());
        try ctx.await(released, isReleased);
        return "released";
    }
}.run);

test "workflow: the worker answers __stack_trace with no handler, and the run's history replays" {
    const env = try testing_ns.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(BlockedWorkflow);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var run = try env.startWorkflow(BlockedWorkflow, .{}, .{});
    defer run.deinit();

    // The first answer can come before the workflow's first task has run its coroutines;
    // ask until both coroutines wait.
    const answer = for (0..200) |_| {
        const text = try env.queryWorkflow(StackTraceQuery, a, run.id, .{});
        if (std.mem.find(u8, text, "2 coroutines") != null) break text;
    } else return error.TestNeverWaited;
    try testing.expect(std.mem.find(u8, answer, "the workflow function, waits on a condition (Context.await)") != null);
    try testing.expect(std.mem.find(u8, answer, "started with io.async or io.concurrent, waits on a timer (1h)") != null);

    try env.signalWorkflow(ReleaseBlocked, run.id, .{});
    try testing.expectEqualStrings("released", try env.getResult(BlockedWorkflow, &run, a, null));
    try testing.expect(std.mem.find(u8, try env.queryWorkflow(StackTraceQuery, a, run.id, .{}), "has completed and closed") != null);

    const history = try env.client.fetchHistory(a, run.id, run.run_id);
    var replayer: testing_ns.WorkflowReplayer = .init(testing.allocator, testing.io);
    defer replayer.deinit();
    try replayer.registerWorkflow(BlockedWorkflow);
    try replayer.replayHistory(history);
}

const BlockedStatus = workflow.QueryDef("status", fn () []const u8);

/// The lines of `DescribedBlocked`'s two waits, set as the code reaches them.
var described_lines: struct { condition: u32 = 0, timer: u32 = 0 } = .{};

/// `BlockedWorkflow` with described handlers: waits until `release` is signaled, with a
/// coroutine sleeping for an hour beside it.
const DescribedBlocked = workflow.Def("metadata_blocked", struct {
    fn release(released: *bool) void {
        released.* = true;
    }
    fn isReleased(released: *bool) bool {
        return released.*;
    }
    fn status(released: *bool) []const u8 {
        return if (released.*) "released" else "waiting";
    }
    fn nap(ctx: *workflow.Context) void {
        described_lines.timer = @src().line + 1;
        ctx.sleep(.fromSeconds(3600), .{}) catch {};
    }
    fn run(ctx: *workflow.Context) ![]const u8 {
        const released = try ctx.allocator().create(bool);
        released.* = false;
        try ctx.setSignalHandlerWithOptions(ReleaseBlocked, released, release, .{ .description = "Lets the workflow finish" });
        try ctx.setQueryHandlerWithOptions(BlockedStatus, released, status, .{ .description = "Whether the workflow was released" });
        var napping = ctx.io().async(nap, .{ctx});
        defer napping.cancel(ctx.io());
        described_lines.condition = @src().line + 1;
        try ctx.await(released, isReleased);
        return "released";
    }
}.run);

fn runWorker(w: *worker_mod.Worker) worker_mod.Worker.RunError!void {
    return w.run();
}

/// Asks `id`'s run for `__enhanced_stack_trace` until both of its coroutines wait (the first
/// answer can come before the first workflow task ran them), and returns that answer.
fn twoStacks(env: *testing_ns.WorkflowEnvironment, a: std.mem.Allocator, id: []const u8) !workflow.EnhancedStackTrace {
    for (0..200) |_| {
        const trace = try env.queryWorkflow(workflow.EnhancedStackTraceQuery, a, id, .{});
        if (trace.stacks.len == 2) return trace;
    }
    return error.TestNeverWaited;
}

/// Checks that `stack`'s innermost call site is `line` of this file, with the source around
/// it in `trace.sources`. An optimized build may have no locations.
fn expectDescribedLocation(trace: workflow.EnhancedStackTrace, stack: workflow.EnhancedStackTrace.Stack, line: u32) !void {
    if (stack.locations.len == 0 and builtin.mode != .debug) return;
    try testing.expect(stack.locations.len > 0);
    const loc = stack.locations[0];
    try testing.expect(std.mem.endsWith(u8, loc.file_path, "workflow_api_test.zig"));
    try testing.expectEqual(@as(i32, @intCast(line)), loc.line);
    const slices = trace.sources.map.get(loc.file_path) orelse return error.TestExpectedSource;
    var lines = std.mem.splitScalar(u8, @embedFile("workflow_api_test.zig"), '\n');
    for (1..line) |_| _ = lines.next();
    const text = lines.next().?;
    for (slices) |slice| if (slice.line_offset < line and std.mem.find(u8, slice.content, text) != null) return;
    return error.TestExpectedSource;
}

test "workflow: the worker answers __enhanced_stack_trace and __temporal_workflow_metadata on a cached run and on one rebuilt from history, and both histories replay" {
    const env = try testing_ns.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(DescribedBlocked);

    // A second worker with its cache off rebuilds the run from history for every task,
    // queries included.
    const uncached_queue = "metadata-uncached";
    var uncached = try worker_mod.Worker.init(testing.allocator, testing.io, &env.client, uncached_queue, .{ .max_cached_workflows = 0 });
    defer uncached.deinit();
    try uncached.registerWorkflow(DescribedBlocked);
    var running = try testing.io.concurrent(runWorker, .{&uncached});
    defer {
        uncached.stop();
        running.await(testing.io) catch {};
    }

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var cached_run = try env.startWorkflow(DescribedBlocked, .{}, .{});
    defer cached_run.deinit();
    var rebuilt_run = try env.startWorkflow(DescribedBlocked, .{ .task_queue = uncached_queue }, .{});
    defer rebuilt_run.deinit();

    const trace = try twoStacks(env, a, cached_run.id);
    try testing.expectEqualStrings("temporal-zig", trace.sdk.name);
    try testing.expectEqualStrings(@import("build_options").version, trace.sdk.version);
    try expectDescribedLocation(trace, trace.stacks[0], described_lines.condition);
    try expectDescribedLocation(trace, trace.stacks[1], described_lines.timer);
    // The rebuilt run waits in the same places, so it answers the same.
    const rebuilt_trace = try twoStacks(env, a, rebuilt_run.id);
    try testing.expectEqualStrings(try std.json.Stringify.valueAlloc(a, trace, .{}), try std.json.Stringify.valueAlloc(a, rebuilt_trace, .{}));

    for ([_][]const u8{ cached_run.id, rebuilt_run.id }) |id| {
        const metadata = try env.queryWorkflow(workflow.WorkflowMetadataQuery, a, id, .{});
        try testing.expectEqualStrings("", metadata.current_details);
        const def = metadata.definition.?;
        try testing.expectEqualStrings("metadata_blocked", def.type);
        const queries = def.query_definitions.items;
        try testing.expectEqual(@as(usize, 4), queries.len);
        for (queries, [_][]const u8{ "__enhanced_stack_trace", "__stack_trace", "__temporal_workflow_metadata", "status" }) |q, name| try testing.expectEqualStrings(name, q.name);
        try testing.expectEqualStrings("Whether the workflow was released", queries[3].description);
        try testing.expect(queries[0].description.len > 0);
        try testing.expectEqual(@as(usize, 1), def.signal_definitions.items.len);
        try testing.expectEqualStrings("release", def.signal_definitions.items[0].name);
        try testing.expectEqualStrings("Lets the workflow finish", def.signal_definitions.items[0].description);
        try testing.expectEqual(@as(usize, 0), def.update_definitions.items.len);
    }

    var replayer: testing_ns.WorkflowReplayer = .init(testing.allocator, testing.io);
    defer replayer.deinit();
    try replayer.registerWorkflow(DescribedBlocked);
    for ([_]*@TypeOf(cached_run){ &cached_run, &rebuilt_run }) |run| {
        try env.signalWorkflow(ReleaseBlocked, run.id, .{});
        try testing.expectEqualStrings("released", try env.getResult(DescribedBlocked, run, a, null));
        // A closed run still answers: no stacks, the same handlers.
        try testing.expectEqual(@as(usize, 0), (try env.queryWorkflow(workflow.EnhancedStackTraceQuery, a, run.id, .{})).stacks.len);
        try testing.expectEqual(@as(usize, 4), (try env.queryWorkflow(workflow.WorkflowMetadataQuery, a, run.id, .{})).definition.?.query_definitions.items.len);
        try replayer.replayHistory(try env.client.fetchHistory(a, run.id, run.run_id));
    }
}

/// Two coroutines wait on one activity's future: first a coroutine started with `io.async`,
/// then the workflow function. Each then runs an activity of its own type, so the order of the
/// two ScheduleActivity commands is the order the coroutines returned from `get`.
const SharedFuture = struct {
    fn echo(_: *activity.Context, text: []const u8) ![]const u8 {
        return text;
    }
    const Fetch = activity.Def("shared_fetch", echo);
    const First = activity.Def("shared_first", echo);
    const Second = activity.Def("shared_second", echo);
    const opts: workflow.ActivityOptions = .{ .start_to_close_timeout = .fromSeconds(10) };

    fn Then(comptime Next: type) type {
        return struct {
            fn run(ctx: *workflow.Context, f: workflow.ActivityFuture([]const u8), ready: *std.Io.Event) ![]const u8 {
                ready.set(ctx.io());
                return ctx.executeActivity(Next, opts, .{try f.get()}).get();
            }
        };
    }

    /// The coroutine started with `io.async` runs `AsyncNext` after the shared activity, and
    /// the workflow function `RootNext`. Returns "<async's result> <root's result>".
    fn Workflow(comptime AsyncNext: type, comptime RootNext: type) type {
        return struct {
            fn run(ctx: *workflow.Context) ![]const u8 {
                const io = ctx.io();
                const f = ctx.executeActivity(Fetch, opts, .{"shared"});
                var ready: std.Io.Event = .unset;
                var other = io.async(Then(AsyncNext).run, .{ ctx, f, &ready });
                // The coroutine above begins waiting on `f` first.
                ready.waitUncancelable(io);
                const mine = try ctx.executeActivity(RootNext, opts, .{try f.get()}).get();
                const theirs = try other.await(io);
                return std.fmt.allocPrint(ctx.allocator(), "{s} {s}", .{ theirs, mine });
            }
        };
    }
    const Def = workflow.Def("shared_future", Workflow(First, Second).run);
    /// The same, with the two coroutines' next activities swapped.
    const Swapped = workflow.Def("shared_future", Workflow(Second, First).run);
};

/// A dev-server run of `SharedFuture.Def` with its three activities, fetched with
/// `Client.fetchHistory` and encoded as ProtoJSON (`History.jsonEncode`), indented, the
/// worker's identity replaced. Its second workflow task schedules `shared_first`, then
/// `shared_second`.
const shared_activity_future_history = @embedFile("testdata/shared_activity_future_history.json");

test "WorkflowReplayer replays two coroutines sharing one activity future and catches them resuming in another order" {
    var ok: testing_ns.WorkflowReplayer = .init(testing.allocator, testing.io);
    defer ok.deinit();
    try ok.registerWorkflow(SharedFuture.Def);
    try ok.replayHistoryJson(shared_activity_future_history);

    var changed: testing_ns.WorkflowReplayer = .init(testing.allocator, testing.io);
    defer changed.deinit();
    try changed.registerWorkflow(SharedFuture.Swapped);
    try testing.expectError(error.Nondeterminism, changed.replayHistoryJson(shared_activity_future_history));
}

/// The dev server only accepts signal-with-start from a workflow with this setting on.
const signal_with_start_args = [_][]const u8{ "--dynamic-config-value", "history.enableSignalWithStartFromWorkflow=true" };

const SignalWithStart = struct {
    const Add = workflow.SignalDef("add", fn (value: []const u8) void);

    /// Waits for two `add` signals and returns "started: <start>|signal: <first>|signal: <second>".
    fn target(ctx: *workflow.Context, start: []const u8) ![]const u8 {
        const ch = try ctx.signalChannel(Add);
        const first = try ch.receive();
        const second = try ch.receive();
        return std.fmt.allocPrint(ctx.allocator(), "started: {s}|signal: {s}|signal: {s}", .{ start, first, second });
    }
    const Target = workflow.Def("sws_target", target);

    /// Returns its argument at once.
    fn done(_: *workflow.Context, value: []const u8) ![]const u8 {
        return value;
    }
    const Done = workflow.Def("sws_done", done);

    /// Signals-with-start `Target` as `id` twice: typed, starting it, then by name under
    /// use-existing. Returns "<id> <run> <started> <run> <started>".
    fn caller(ctx: *workflow.Context, id: []const u8) ![]const u8 {
        const first = try ctx.signalWithStartWorkflow(Target, .{ .id = id, .execution_timeout = .fromSeconds(60) }, .{"go"}, Add, .{"one"}).get();
        const by_name = ctx.signalWithStartWorkflowByName("sws_target", .{ .id = id, .id_conflict_policy = .use_existing }, .{@as([]const u8, "ignored")}, "add", .{@as([]const u8, "two")});
        const second = try by_name.get();
        return std.fmt.allocPrint(ctx.allocator(), "{s} {s} {} {s} {}", .{ first.workflow_id, first.run_id, first.started, second.run_id, second.started });
    }
    const Caller = workflow.Def("sws_caller", caller);

    /// `caller` with its first call replaced by a timer.
    fn callerChanged(ctx: *workflow.Context, id: []const u8) ![]const u8 {
        try ctx.sleep(.fromSeconds(1), .{});
        return id;
    }

    /// Signals-with-start `Done` as `id` under reject-duplicate; returns the error's name and
    /// the failure's messages down its causes.
    fn refused(ctx: *workflow.Context, id: []const u8) ![]const u8 {
        const f = ctx.signalWithStartWorkflow(Done, .{ .id = id, .id_reuse_policy = .reject_duplicate }, .{"again"}, Add, .{"one"});
        const result = f.get() catch |err| {
            var out: std.ArrayList(u8) = .empty;
            try out.appendSlice(ctx.allocator(), @errorName(err));
            var cause = f.failure();
            while (cause) |c| : (cause = c.cause) {
                try out.print(ctx.allocator(), " <- {s}", .{c.message});
                if (c.failure_info) |i| switch (i) {
                    .nexus_handler_failure_info => |h| try out.print(ctx.allocator(), " [handler {s}]", .{h.type}),
                    .application_failure_info => |h| try out.print(ctx.allocator(), " [app {s} nonretry={}]", .{ h.type, h.non_retryable }),
                    else => |t| try out.print(ctx.allocator(), " [{s}]", .{@tagName(t)}),
                };
            }
            return out.items;
        };
        return std.fmt.allocPrint(ctx.allocator(), "sent: {s}", .{result.run_id});
    }
    const Refused = workflow.Def("sws_refused", refused);

    /// Each invalid option set, by error name, comma-separated.
    fn invalid(ctx: *workflow.Context, id: []const u8) ![]const u8 {
        const a = ctx.allocator();
        var out: std.ArrayList(u8) = .empty;
        const calls = [_]workflow.SignalWithStartFuture{
            ctx.signalWithStartWorkflow(Target, .{ .id = "" }, .{"go"}, Add, .{"one"}),
            ctx.signalWithStartWorkflowByName("", .{ .id = id }, .{"go"}, "add", .{"one"}),
            ctx.signalWithStartWorkflowByName("sws_target", .{ .id = id }, .{"go"}, "", .{"one"}),
            ctx.signalWithStartWorkflow(Target, .{ .id = id, .id_conflict_policy = .fail }, .{"go"}, Add, .{"one"}),
            ctx.signalWithStartWorkflow(Target, .{ .id = id, .run_timeout = .fromSeconds(-1) }, .{"go"}, Add, .{"one"}),
            ctx.signalWithStartWorkflow(Target, .{ .id = id, .start_delay = .fromSeconds(-1) }, .{"go"}, Add, .{"one"}),
        };
        for (calls) |f| {
            if (!f.isReady()) return error.NotReady;
            if (f.failure() != null) return error.FailureSet;
            if (f.get()) |_| try out.appendSlice(a, "sent,") else |err| try out.print(a, "{s},", .{@errorName(err)});
        }
        return out.items;
    }
    const Invalid = workflow.Def("sws_invalid", invalid);

    fn waitForResult(f: workflow.SignalWithStartFuture) workflow.SignalWithStartError!workflow.SignalWithStartResult {
        return f.get();
    }

    /// Cancels the coroutine waiting in `get` in the same workflow task; returns the error's
    /// name and whether the failure is set.
    fn cancel(ctx: *workflow.Context, id: []const u8) ![]const u8 {
        const io = ctx.io();
        const f = ctx.signalWithStartWorkflow(Target, .{ .id = id }, .{"go"}, Add, .{"one"});
        var waiting = io.async(waitForResult, .{f});
        const outcome = waiting.cancel(io);
        if (outcome) |_| return "sent" else |err| return std.fmt.allocPrint(ctx.allocator(), "{s} failure={}", .{ @errorName(err), f.failure() != null });
    }
    const Cancel = workflow.Def("sws_cancel", cancel);
};

fn startSignalWithStartEnv(codecs: []const converter.PayloadCodec) !*testing_ns.WorkflowEnvironment {
    const env = try testing_ns.WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{
        .dev_server = .{ .extra_args = &signal_with_start_args },
        .codecs = codecs,
    });
    errdefer env.deinit();
    inline for (.{ SignalWithStart.Target, SignalWithStart.Done, SignalWithStart.Caller, SignalWithStart.Refused, SignalWithStart.Invalid, SignalWithStart.Cancel }) |W| try env.registerWorkflow(W);
    return env;
}

/// Runs `SignalWithStart.Caller` and checks its answers and the target's result.
fn expectStartThenSignal(env: *testing_ns.WorkflowEnvironment, a: std.mem.Allocator) !void {
    const id = try std.fmt.allocPrint(a, "sws-target-{s}", .{env.task_queue});
    const result = try env.executeWorkflow(SignalWithStart.Caller, .{id}, a, null);
    var parts = std.mem.splitScalar(u8, result, ' ');
    try testing.expectEqualStrings(id, parts.next().?);
    const run_id = parts.next().?;
    try testing.expect(run_id.len > 0);
    try testing.expectEqualStrings("true", parts.next().?);
    try testing.expectEqualStrings(run_id, parts.next().?);
    try testing.expectEqualStrings("false", parts.next().?);

    var target = try env.client.getWorkflow([]const u8, id, run_id);
    defer target.deinit();
    try testing.expectEqualStrings("started: go|signal: one|signal: two", try env.getResult(SignalWithStart.Target, &target, a, null));
}

test "workflow: signalWithStartWorkflow starts the target, then only signals it under use-existing" {
    const env = try startSignalWithStartEnv(&.{});
    defer env.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    try expectStartThenSignal(env, arena.allocator());
}

/// Wraps each payload in one of encoding "test/wrapped" holding it serialized, so no payload
/// reaches the server as it was.
const WrappingCodec = struct {
    const encoding = "test/wrapped";
    encoded: std.atomic.Value(u32) = .init(0),

    fn codec(w: *WrappingCodec) converter.PayloadCodec {
        return .{ .ptr = w, .vtable = &.{ .encode = encode, .decode = decode } };
    }

    fn encode(ptr: *anyopaque, arena: std.mem.Allocator, _: std.Io, payloads: []const converter.Payload) converter.CodecError![]converter.Payload {
        const w: *WrappingCodec = @ptrCast(@alignCast(ptr));
        _ = w.encoded.fetchAdd(@intCast(payloads.len), .monotonic);
        const out = try arena.alloc(converter.Payload, payloads.len);
        for (payloads, out) |p, *o| {
            var serialized: std.Io.Writer.Allocating = .init(arena);
            p.encode(&serialized.writer, arena) catch return error.OutOfMemory;
            o.* = try converter.payloadWithEncoding(arena, encoding, serialized.written());
        }
        return out;
    }

    fn decode(_: *anyopaque, arena: std.mem.Allocator, _: std.Io, payloads: []const converter.Payload) converter.CodecError![]converter.Payload {
        const out = try arena.dupe(converter.Payload, payloads);
        for (out) |*p| {
            if (!std.mem.eql(u8, converter.encodingOf(p.*) orelse "", encoding)) continue;
            var reader: std.Io.Reader = .fixed(try arena.dupe(u8, p.data));
            p.* = converter.Payload.decode(&reader, arena) catch return error.CodecFailed;
        }
        return out;
    }
};

test "workflow: signalWithStartWorkflow through a worker's encoding codec reaches the target decodable" {
    var wrapping: WrappingCodec = .{};
    const codecs = [_]converter.PayloadCodec{wrapping.codec()};
    const env = try startSignalWithStartEnv(&codecs);
    defer env.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    try expectStartThenSignal(env, a);

    // The target's start argument and signals reached the server wrapped.
    const id = try std.fmt.allocPrint(a, "sws-target-{s}", .{env.task_queue});
    var raw = try @import("client.zig").Client.connect(testing.allocator, testing.io, env.server.dev.clientOptions());
    defer raw.close();
    const history = try raw.fetchHistory(a, id, "");
    var wrapped: usize = 0;
    for (history.events.items) |event| switch (event.attributes.?) {
        .workflow_execution_started_event_attributes => |attrs| {
            for (attrs.input.?.payloads.items) |p| try testing.expectEqualStrings(WrappingCodec.encoding, converter.encodingOf(p).?);
            wrapped += 1;
        },
        .workflow_execution_signaled_event_attributes => |attrs| {
            for (attrs.input.?.payloads.items) |p| try testing.expectEqualStrings(WrappingCodec.encoding, converter.encodingOf(p).?);
            wrapped += 1;
        },
        else => {},
    };
    try testing.expectEqual(@as(usize, 3), wrapped);
}

test "workflow: signalWithStartWorkflow on a closed workflow under reject-duplicate returns WorkflowExecutionAlreadyStarted" {
    const env = try startSignalWithStartEnv(&.{});
    defer env.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const id = try std.fmt.allocPrint(a, "sws-done-{s}", .{env.task_queue});
    try testing.expectEqualStrings("first", try env.executeWorkflowWithOptions(SignalWithStart.Done, .{ .id = id }, .{"first"}, a, null));
    const result = try env.executeWorkflow(SignalWithStart.Refused, .{id}, a, null);
    try testing.expect(std.mem.startsWith(u8, result, "WorkflowExecutionAlreadyStarted <- nexus operation completed unsuccessfully [nexus_operation_execution_failure_info] <- WorkflowExecutionAlreadyStarted: Workflow execution already finished."));
    try testing.expect(std.mem.endsWith(u8, result, "[server_failure_info]"));
}

test "workflow: invalid signalWithStartWorkflow options return InvalidOptions and send nothing" {
    const env = try startSignalWithStartEnv(&.{});
    defer env.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var run = try env.startWorkflow(SignalWithStart.Invalid, .{}, .{"sws-invalid"});
    defer run.deinit();
    try testing.expectEqualStrings("InvalidOptions,InvalidOptions,InvalidOptions,InvalidOptions,InvalidOptions,InvalidOptions,", try env.getResult(SignalWithStart.Invalid, &run, a, null));
    const history = try env.client.fetchHistory(a, run.id, run.run_id);
    for (history.events.items) |event| try testing.expect(event.attributes.? != .nexus_operation_scheduled_event_attributes);
}

test "workflow: canceling the coroutine waiting on signalWithStartWorkflow returns Canceled" {
    const env = try startSignalWithStartEnv(&.{});
    defer env.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const id = try std.fmt.allocPrint(a, "sws-cancel-{s}", .{env.task_queue});
    try testing.expectEqualStrings("Canceled failure=true", try env.executeWorkflow(SignalWithStart.Cancel, .{id}, a, null));
}

/// A dev-server run of `SignalWithStart.Caller` for target "sws-recorded-target" (started
/// with the flag in `signal_with_start_args`), fetched with `Client.fetchHistory` and encoded
/// as ProtoJSON (`temporal.protojson.encode`), indented.
const signal_with_start_history = @embedFile("testdata/signal_with_start_history.json");

test "WorkflowReplayer replays a signal-with-start and catches a workflow that starts a timer instead" {
    var ok: testing_ns.WorkflowReplayer = .init(testing.allocator, testing.io);
    defer ok.deinit();
    try ok.registerWorkflow(SignalWithStart.Caller);
    try ok.replayHistoryJson(signal_with_start_history);

    var changed: testing_ns.WorkflowReplayer = .init(testing.allocator, testing.io);
    defer changed.deinit();
    try changed.registerWorkflow(workflow.Def("sws_caller", SignalWithStart.callerChanged));
    try testing.expectError(error.Nondeterminism, changed.replayHistoryJson(signal_with_start_history));
}

const AwaitTimeoutTie = struct {
    const Ready = workflow.SignalDef("ready", fn () void);

    fn markReady(ready: *bool) void {
        ready.* = true;
    }

    fn isReady(ready: *bool) bool {
        return ready.*;
    }

    /// Waits up to 2 s for the "ready" signal. Completes at once if it arrived, and otherwise
    /// sleeps 1 s first.
    fn run(ctx: *workflow.Context) ![]const u8 {
        const ready = try ctx.allocator().create(bool);
        ready.* = false;
        try ctx.setSignalHandler(Ready, ready, markReady);
        if (try ctx.awaitTimeout(.fromSeconds(2), ready, isReady)) return "ready";
        try ctx.sleep(.fromSeconds(1), .{});
        return "timed out";
    }

    /// `run` as it would take the timeout branch whatever the signal did.
    fn alwaysTimesOut(ctx: *workflow.Context) ![]const u8 {
        const ready = try ctx.allocator().create(bool);
        ready.* = false;
        try ctx.setSignalHandler(Ready, ready, markReady);
        _ = try ctx.awaitTimeout(.fromSeconds(2), ready, isReady);
        try ctx.sleep(.fromSeconds(1), .{});
        return "timed out";
    }

    const Workflow = workflow.Def("await_timeout_tie", run);
};

/// A dev-server run of `AwaitTimeoutTie.Workflow`, fetched with `Client.fetchHistory` and
/// encoded as ProtoJSON (`temporal.protojson.encode`), indented. The worker was stopped once
/// the timer had started, the timer was let fire, the "ready" signal was sent, and a new
/// worker was started, so one workflow task carries `TimerFired` and
/// `WorkflowExecutionSignaled`. The run completed with "ready".
const await_timeout_tie_history = @embedFile("testdata/await_timeout_tie_history.json");

test "WorkflowReplayer replays an awaitTimeout whose timer fired in the signal's workflow task as true" {
    var ok: testing_ns.WorkflowReplayer = .init(testing.allocator, testing.io);
    defer ok.deinit();
    try ok.registerWorkflow(AwaitTimeoutTie.Workflow);
    try ok.replayHistoryJson(await_timeout_tie_history);

    var changed: testing_ns.WorkflowReplayer = .init(testing.allocator, testing.io);
    defer changed.deinit();
    try changed.registerWorkflow(workflow.Def("await_timeout_tie", AwaitTimeoutTie.alwaysTimesOut));
    try testing.expectError(error.Nondeterminism, changed.replayHistoryJson(await_timeout_tie_history));
}

const ArgumentsInMarker = struct {
    const MarkerEcho = activity.Def("la_marker_echo", struct {
        fn echo(_: *activity.Context, text: []const u8) ![]const u8 {
            return text;
        }
    }.echo);

    /// Runs `MarkerEcho` as a local activity twice: with "recorded" and `arguments_in_marker` set,
    /// then with "plain" and the option left off.
    fn run(ctx: *workflow.Context) ![]const u8 {
        const recorded = try ctx.executeLocalActivity(MarkerEcho, .{ .start_to_close_timeout = .fromSeconds(10), .arguments_in_marker = true }, .{"recorded"}).get();
        const plain = try ctx.executeLocalActivity(MarkerEcho, .{ .start_to_close_timeout = .fromSeconds(10) }, .{"plain"}).get();
        return std.fmt.allocPrint(ctx.allocator(), "{s} {s}", .{ recorded, plain });
    }

    /// `run` with the second local activity left out.
    fn runOne(ctx: *workflow.Context) ![]const u8 {
        return ctx.executeLocalActivity(MarkerEcho, .{ .start_to_close_timeout = .fromSeconds(10), .arguments_in_marker = true }, .{"recorded"}).get();
    }

    const Workflow = workflow.Def("la_arguments_in_marker", run);

    /// The arguments recorded in each local activity marker of `history`, in event order:
    /// null for a marker without them.
    fn markerInputs(a: std.mem.Allocator, history: anytype) ![]const ?[]const converter.Payload {
        var out: std.ArrayList(?[]const converter.Payload) = .empty;
        for (history.events.items) |event| switch (event.attributes.?) {
            .marker_recorded_event_attributes => |attrs| {
                var input: ?[]const converter.Payload = null;
                for (attrs.details.items) |entry| {
                    if (std.mem.eql(u8, entry.key, "input")) input = entry.value.?.payloads.items;
                }
                try out.append(a, input);
            },
            else => {},
        };
        return out.items;
    }
};

test "workflow: arguments_in_marker records a local activity's arguments, as the codecs encoded them, in its marker only" {
    var wrapping: WrappingCodec = .{};
    const codecs = [_]converter.PayloadCodec{wrapping.codec()};
    const env = try testing_ns.WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{ .codecs = &codecs });
    defer env.deinit();
    try env.registerWorkflow(ArgumentsInMarker.Workflow);
    try env.registerActivity(ArgumentsInMarker.MarkerEcho);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var run = try env.startWorkflow(ArgumentsInMarker.Workflow, .{}, .{});
    defer run.deinit();
    try testing.expectEqualStrings("recorded plain", try env.getResult(ArgumentsInMarker.Workflow, &run, a, null));

    // Read through a client without codecs, to see the payloads as the server holds them.
    var raw = try @import("client.zig").Client.connect(testing.allocator, testing.io, env.server.dev.clientOptions());
    defer raw.close();
    const history = try raw.fetchHistory(a, run.id, run.run_id);
    const inputs = try ArgumentsInMarker.markerInputs(a, history);
    try testing.expectEqual(@as(usize, 2), inputs.len);
    try testing.expect(inputs[1] == null);
    const recorded = inputs[0] orelse return error.TestExpectedArguments;
    try testing.expectEqual(@as(usize, 1), recorded.len);
    var reference: WrappingCodec = .{};
    const expected = try reference.codec().encode(a, testing.io, &.{try converter.toPayload(a, @as([]const u8, "recorded"))});
    try testing.expectEqualStrings(WrappingCodec.encoding, converter.encodingOf(recorded[0]).?);
    try testing.expectEqualSlices(u8, expected[0].data, recorded[0].data);

    var replayer: testing_ns.WorkflowReplayer = .initWithOptions(testing.allocator, testing.io, .{ .codecs = &codecs });
    defer replayer.deinit();
    try replayer.registerWorkflow(ArgumentsInMarker.Workflow);
    try replayer.replayHistory(history);
}

/// A dev-server run of `ArgumentsInMarker.Workflow` on a worker with `WrappingCodec`, fetched
/// with `Client.fetchHistory` through a client without codecs and encoded as ProtoJSON
/// (`temporal.protojson.encode`), indented. Its first local activity marker holds the
/// arguments, wrapped; the second holds none.
const local_activity_arguments_history = @embedFile("testdata/local_activity_arguments_history.json");

test "WorkflowReplayer replays local activity markers with arguments and catches a removed local activity" {
    var wrapping: WrappingCodec = .{};
    const codecs = [_]converter.PayloadCodec{wrapping.codec()};
    var ok: testing_ns.WorkflowReplayer = .initWithOptions(testing.allocator, testing.io, .{ .codecs = &codecs });
    defer ok.deinit();
    try ok.registerWorkflow(ArgumentsInMarker.Workflow);
    try ok.replayHistoryJson(local_activity_arguments_history);

    var changed: testing_ns.WorkflowReplayer = .initWithOptions(testing.allocator, testing.io, .{ .codecs = &codecs });
    defer changed.deinit();
    try changed.registerWorkflow(workflow.Def("la_arguments_in_marker", ArgumentsInMarker.runOne));
    try testing.expectError(error.Nondeterminism, changed.replayHistoryJson(local_activity_arguments_history));
}

const MutableSideEffect = struct {
    /// How many times `readValue` ran, on any run.
    var reads: usize = 0;
    /// What the query handler's call returned.
    var query_result: ?anyerror = null;

    const Read = workflow.QueryDef("mse_read", fn () u32);

    fn readValue(value: u32) u32 {
        reads += 1;
        return value;
    }

    fn eqlU32(recorded: u32, current: u32) bool {
        return recorded == current;
    }

    const QueryState = struct {
        ctx: *workflow.Context,
        fn read(s: *QueryState) workflow.Context.SideEffectError!u32 {
            const value = s.ctx.mutableSideEffect(u32, "cfg", @as(u32, 0), readValue, eqlU32) catch |err| {
                query_result = err;
                return err;
            };
            query_result = null;
            return value;
        }
    };

    /// For each of `values`, waits on a 1 ms timer when `sleep` is set, then, unless the
    /// value is 0, reads it through `mutableSideEffect` with ID "cfg". Returns the values the
    /// calls returned, separated by spaces.
    fn run(ctx: *workflow.Context, values: []const u32, sleep: bool) ![]const u8 {
        const s = try ctx.allocator().create(QueryState);
        s.* = .{ .ctx = ctx };
        try ctx.setQueryHandler(Read, s, QueryState.read);
        var out: std.ArrayList(u8) = .empty;
        for (values) |value| {
            if (sleep) try ctx.sleep(.fromMilliseconds(1), .{});
            if (value == 0) continue;
            const got = try ctx.mutableSideEffect(u32, "cfg", value, readValue, eqlU32);
            if (out.items.len > 0) try out.append(ctx.allocator(), ' ');
            try out.print(ctx.allocator(), "{d}", .{got});
        }
        return out.items;
    }
    const Def = workflow.Def("mutable_side_effect", run);

    /// `run` without the `mutableSideEffect` calls.
    fn dropped(ctx: *workflow.Context, values: []const u32, sleep: bool) ![]const u8 {
        for (values) |_| if (sleep) try ctx.sleep(.fromMilliseconds(1), .{});
        return "";
    }
    const Dropped = workflow.Def("mutable_side_effect", dropped);

    fn readIn(ctx: *workflow.Context, value: u32) workflow.Context.SideEffectError!u32 {
        return ctx.mutableSideEffect(u32, "cfg", value, readValue, eqlU32);
    }

    /// Reads 1 and 2 for ID "cfg" in two coroutines at once: the second call waits for the
    /// first one's recording. Returns "<first> <second>".
    fn concurrent(ctx: *workflow.Context) ![]const u8 {
        const io = ctx.io();
        var first = io.async(readIn, .{ ctx, 1 });
        var second = io.async(readIn, .{ ctx, 2 });
        const a = try first.await(io);
        const b = try second.await(io);
        return std.fmt.allocPrint(ctx.allocator(), "{d} {d}", .{ a, b });
    }
    const Concurrent = workflow.Def("mutable_side_effect_concurrent", concurrent);

    fn countEvents(history: anytype, event_type: anytype) usize {
        var n: usize = 0;
        for (history.events.items) |e| n += @intFromBool(e.event_type == event_type);
        return n;
    }

    /// Runs `Def` with `values` and `sleep` on `env`, checks that `readValue` ran once per
    /// call and not again while `getResult` replayed the run, and returns the result and the
    /// history.
    fn runOnce(env: *testing_ns.WorkflowEnvironment, a: std.mem.Allocator, values: []const u32, sleep: bool) !struct { []const u8, @import("proto.zig").temporal.api.history.v1.History } {
        var calls: usize = 0;
        for (values) |v| calls += @intFromBool(v != 0);
        const before = reads;
        var run_handle = try env.startWorkflow(Def, .{}, .{ values, sleep });
        defer run_handle.deinit();
        const result = try env.getResult(Def, &run_handle, a, null);
        try testing.expectEqual(before + calls, reads);
        return .{ result, try env.client.fetchHistory(a, run_handle.id, run_handle.run_id) };
    }
};

test "workflow: mutableSideEffect records an unchanged value once, adding no events for later calls" {
    const env = try testing_ns.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(MutableSideEffect.Def);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const result, const history = try MutableSideEffect.runOnce(env, a, &.{ 5, 5, 5, 5 }, true);
    try testing.expectEqualStrings("5 5 5 5", result);
    try testing.expectEqual(2, MutableSideEffect.countEvents(history, .EVENT_TYPE_MARKER_RECORDED));

    // The same timers with one call: the three unchanged calls added nothing.
    const one, const one_history = try MutableSideEffect.runOnce(env, a, &.{ 5, 0, 0, 0 }, true);
    try testing.expectEqualStrings("5", one);
    try testing.expectEqual(one_history.events.items.len, history.events.items.len);

    var replayer: testing_ns.WorkflowReplayer = .init(testing.allocator, testing.io);
    defer replayer.deinit();
    try replayer.registerWorkflow(MutableSideEffect.Def);
    const before = MutableSideEffect.reads;
    try replayer.replayHistory(history);
    try testing.expectEqual(before, MutableSideEffect.reads);
}

test "workflow: mutableSideEffect records each changed value and returns the recorded ones" {
    const env = try testing_ns.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(MutableSideEffect.Def);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const result, const history = try MutableSideEffect.runOnce(env, a, &.{ 1, 1, 2, 2, 3 }, true);
    try testing.expectEqualStrings("1 1 2 2 3", result);
    // A patch marker and the side effect's marker for each of the three changes.
    try testing.expectEqual(6, MutableSideEffect.countEvents(history, .EVENT_TYPE_MARKER_RECORDED));
}

test "workflow: mutableSideEffect changes around an unchanged call in one workflow task replay cleanly" {
    const env = try testing_ns.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(MutableSideEffect.Def);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const result, const history = try MutableSideEffect.runOnce(env, a, &.{ 1, 1, 2 }, false);
    try testing.expectEqualStrings("1 1 2", result);
    try testing.expectEqual(4, MutableSideEffect.countEvents(history, .EVENT_TYPE_MARKER_RECORDED));
    var replayer: testing_ns.WorkflowReplayer = .init(testing.allocator, testing.io);
    defer replayer.deinit();
    try replayer.registerWorkflow(MutableSideEffect.Def);
    try replayer.replayHistory(history);
}

test "workflow: mutableSideEffect calls for one ID in two coroutines wait for each other's recording and replay cleanly" {
    const env = try testing_ns.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(MutableSideEffect.Concurrent);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var run = try env.startWorkflow(MutableSideEffect.Concurrent, .{}, .{});
    defer run.deinit();
    try testing.expectEqualStrings("1 2", try env.getResult(MutableSideEffect.Concurrent, &run, a, null));
    const history = try env.client.fetchHistory(a, run.id, run.run_id);
    try testing.expectEqual(4, MutableSideEffect.countEvents(history, .EVENT_TYPE_MARKER_RECORDED));
    var replayer: testing_ns.WorkflowReplayer = .init(testing.allocator, testing.io);
    defer replayer.deinit();
    try replayer.registerWorkflow(MutableSideEffect.Concurrent);
    try replayer.replayHistory(history);
}

test "workflow: mutableSideEffect in a query handler returns SideEffectFailed" {
    const env = try testing_ns.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(MutableSideEffect.Def);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var run = try env.startWorkflow(MutableSideEffect.Def, .{}, .{ &[_]u32{4}, false });
    defer run.deinit();
    try testing.expectEqualStrings("4", try env.getResult(MutableSideEffect.Def, &run, a, null));
    const before = MutableSideEffect.reads;
    MutableSideEffect.query_result = error.TestExpectedQuery;
    try testing.expectError(error.QueryFailed, env.queryWorkflow(MutableSideEffect.Read, a, run.id, .{}));
    try testing.expectEqual(error.SideEffectFailed, MutableSideEffect.query_result.?);
    try testing.expectEqual(before, MutableSideEffect.reads);
}

/// Dev-server runs of `MutableSideEffect.Def`, fetched with `Client.fetchHistory` and encoded
/// as ProtoJSON (`protojson.encode`), indented, the host name in the worker identity and the
/// sticky task queue replaced. The changed one ran with values 1, 1, 2, 2, 3 and timers, so
/// it records three changes; the unchanged one with 5, 5, 5, 5 and timers, so it records
/// only the first call.
const mutable_side_effect_changed_history = @embedFile("testdata/mutable_side_effect_changed_history.json");
const mutable_side_effect_unchanged_history = @embedFile("testdata/mutable_side_effect_unchanged_history.json");

test "WorkflowReplayer replays mutableSideEffect histories without calling its function and catches a dropped call" {
    const before = MutableSideEffect.reads;
    for ([_][]const u8{ mutable_side_effect_changed_history, mutable_side_effect_unchanged_history }) |history| {
        var ok: testing_ns.WorkflowReplayer = .init(testing.allocator, testing.io);
        defer ok.deinit();
        try ok.registerWorkflow(MutableSideEffect.Def);
        try ok.replayHistoryJson(history);
    }
    try testing.expectEqual(before, MutableSideEffect.reads);

    var changed: testing_ns.WorkflowReplayer = .init(testing.allocator, testing.io);
    defer changed.deinit();
    try changed.registerWorkflow(MutableSideEffect.Dropped);
    try testing.expectError(error.Nondeterminism, changed.replayHistoryJson(mutable_side_effect_changed_history));
}

// ---------------------------------------------------------------- workflow-local variables

const Instance = @import("workflow/Instance.zig");
const proto = @import("proto.zig");
const act_pb = proto.coresdk.workflow_activation;
const cmd_pb = proto.coresdk.workflow_commands;
const done_pb = proto.coresdk.workflow_completion;

const LocalVars = struct {
    const tag: workflow.LocalVar([]const u8) = .{ .name = "test.tag", .default = "none" };
    const count: workflow.LocalVar(u32) = .{ .name = "test.count", .default = 0 };
    /// The same name as `count` with another type: another variable.
    const count_text: workflow.LocalVar([]const u8) = .{ .name = "test.count", .default = "text" };
    const outbound_calls: workflow.LocalVar(u32) = .{ .name = "test.outbound", .default = 0 };

    const Release = workflow.SignalDef("local_var_release", fn () void);
    const Tag = workflow.QueryDef("local_var_tag", fn () []const u8);

    /// What the last run of `run` to finish returned, on the worker or in a replay.
    var last_buf: [128]u8 = undefined;
    var last_len: usize = 0;

    const State = struct {
        ctx: *workflow.Context,
        released: bool = false,

        fn release(s: *State) void {
            s.released = true;
        }
        fn isReleased(s: *State) bool {
            return s.released;
        }
        fn currentTag(s: *State) []const u8 {
            return tag.get(s.ctx);
        }
    };

    fn bump(ctx: *workflow.Context) !void {
        try count.set(ctx, count.get(ctx) + 100);
    }

    fn one(_: void) u32 {
        return 1;
    }

    /// Reads `tag` (still the default), sets it to `name`, adds 100 to `count` from a
    /// coroutine of its own, records a side effect, and waits for `Release`. Returns
    /// "<initial tag> <tag> <count> <count_text> <outbound_calls>".
    fn run(ctx: *workflow.Context, name: []const u8) ![]const u8 {
        const initial = tag.get(ctx);
        try tag.set(ctx, try ctx.allocator().dupe(u8, name));
        const s = try ctx.allocator().create(State);
        s.* = .{ .ctx = ctx };
        try ctx.setQueryHandler(Tag, s, State.currentTag);
        try ctx.setSignalHandler(Release, s, State.release);
        var child = ctx.io().async(bump, .{ctx});
        try child.await(ctx.io());
        _ = try ctx.sideEffect(u32, {}, one);
        try ctx.await(s, State.isReleased);
        const out = try std.fmt.allocPrint(ctx.allocator(), "{s} {s} {d} {s} {d}", .{ initial, tag.get(ctx), count.get(ctx), count_text.get(ctx), outbound_calls.get(ctx) });
        @memcpy(last_buf[0..out.len], out);
        last_len = out.len;
        return out;
    }
    const Def = workflow.Def("local_vars", run);

    /// Adds 1 to `count` when the run's interceptors are built, 10 in `executeWorkflow`, and
    /// 1 to `outbound_calls` for each side effect.
    const Audit = struct {
        pub fn interceptWorkflow(_: *Audit, ctx: *workflow.Context) error{OutOfMemory}!interceptor.Workflow {
            count.set(ctx, count.get(ctx) + 1) catch |err| switch (err) {
                error.OutOfMemory => return error.OutOfMemory,
                error.ReadOnly => unreachable,
            };
            const r = try ctx.allocator().create(Run);
            r.* = .{ .ctx = ctx };
            return .{ .inbound = interceptor.WorkflowInbound.from(Run, r), .outbound = interceptor.WorkflowOutbound.from(Run, r) };
        }

        const Run = struct {
            ctx: *workflow.Context,

            pub fn executeWorkflow(r: *Run, input: interceptor.ExecuteWorkflowInput, next: interceptor.WorkflowInbound.Next) anyerror!converter.Payload {
                try count.set(r.ctx, count.get(r.ctx) + 10);
                return next.executeWorkflow(input);
            }

            pub fn sideEffect(r: *Run, input: interceptor.SideEffectInput, next: interceptor.WorkflowOutbound.Next) workflow.ActivityFuture(converter.Payload) {
                outbound_calls.set(r.ctx, outbound_calls.get(r.ctx) + 1) catch {};
                return next.sideEffect(input);
            }
        };
    };

    /// Tries to set `count` to 99 from a side-effect function: 1 if refused with the value
    /// unchanged, 2 if refused with it changed, 0 if it was set.
    fn setInFunc(ctx: *workflow.Context) u32 {
        const before = count.get(ctx);
        count.set(ctx, 99) catch |err| return if (err == error.ReadOnly and count.get(ctx) == before) 1 else 2;
        return 0;
    }

    /// The context `eqlSet` tries to set `count` in, and what that set returned.
    var eql_ctx: ?*workflow.Context = null;
    var eql_outcome: []const u8 = "not called";

    fn eqlSet(recorded: u32, current: u32) bool {
        if (eql_ctx) |c| {
            const before = count.get(c);
            eql_outcome = if (count.set(c, 77)) |_| "set" else |err| if (err == error.ReadOnly and count.get(c) == before) "refused" else @errorName(err);
        }
        return recorded == current;
    }

    /// Sets `count` to 5, then tries to set it from `sideEffect`'s function and from
    /// `mutableSideEffect`'s function and `eql`, then sets it again. Returns the three
    /// functions' results and `count`.
    fn sideEffects(ctx: *workflow.Context) ![]const u8 {
        try count.set(ctx, 5);
        const in_side_effect = try ctx.sideEffect(u32, ctx, setInFunc);
        const first = try ctx.mutableSideEffect(u32, "cfg", ctx, setInFunc, eqlSet);
        eql_ctx = ctx;
        defer eql_ctx = null;
        const second = try ctx.mutableSideEffect(u32, "cfg", ctx, setInFunc, eqlSet);
        try count.set(ctx, count.get(ctx) + 1);
        return std.fmt.allocPrint(ctx.allocator(), "{d} {d} {d} {d}", .{ in_side_effect, first, second, count.get(ctx) });
    }
    const SideEffects = workflow.Def("local_vars_side_effects", sideEffects);

    /// Counts the warnings of refused sets. Called from the worker's threads.
    const Warnings = struct {
        count: std.atomic.Value(usize) = .init(0),

        fn sink(w: *Warnings) log.Sink {
            return .{ .ptr = w, .vtable = &.{ .log = record } };
        }

        fn record(ptr: ?*anyopaque, level: log.Level, text: []const u8, fields: []const log.Field) void {
            const w: *Warnings = @ptrCast(@alignCast(ptr.?));
            if (level != .warn or !std.mem.startsWith(u8, text, "LocalVar.set in a side-effect function is refused")) return;
            for (fields) |f| if (std.mem.eql(u8, f.key, "LocalVar")) {
                _ = w.count.fetchAdd(1, .monotonic);
            };
        }
    };
};

test "workflow: LocalVar values belong to one run, reach its coroutines and interceptors, and replay" {
    var audit: LocalVars.Audit = .{};
    const interceptors = [_]interceptor.Worker{interceptor.Worker.from(LocalVars.Audit, &audit)};
    const env = try testing_ns.WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{ .worker_interceptors = &interceptors });
    defer env.deinit();
    try env.registerWorkflow(LocalVars.Def);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // Both runs set their tag and wait, cached on the worker at once; then each finishes.
    var first = try env.startWorkflow(LocalVars.Def, .{}, .{"first"});
    defer first.deinit();
    var second = try env.startWorkflow(LocalVars.Def, .{}, .{"second"});
    defer second.deinit();
    for ([_][]const u8{ "first", "second" }, [_][]const u8{ first.id, second.id }) |want, id| {
        for (0..200) |_| {
            if (std.mem.eql(u8, try env.queryWorkflow(LocalVars.Tag, a, id, .{}), want)) break;
        } else return error.TestNeverSet;
    }
    try env.signalWorkflow(LocalVars.Release, first.id, .{});
    try testing.expectEqualStrings("none first 111 text 1", try env.getResult(LocalVars.Def, &first, a, null));
    try testing.expectEqualStrings("second", try env.queryWorkflow(LocalVars.Tag, a, second.id, .{}));
    try env.signalWorkflow(LocalVars.Release, second.id, .{});
    try testing.expectEqualStrings("none second 111 text 1", try env.getResult(LocalVars.Def, &second, a, null));

    // The replay rebuilds the values from the default by running the same code again.
    const history = try env.client.fetchHistory(a, first.id, first.run_id);
    var replayer: testing_ns.WorkflowReplayer = .initWithOptions(testing.allocator, testing.io, .{ .interceptors = &interceptors });
    defer replayer.deinit();
    try replayer.registerWorkflow(LocalVars.Def);
    LocalVars.last_len = 0;
    try replayer.replayHistory(history);
    try testing.expectEqualStrings("none first 111 text 1", LocalVars.last_buf[0..LocalVars.last_len]);
}

test "workflow: LocalVar.set in a side-effect function or mutableSideEffect's eql is refused, and the run replays" {
    var warnings: LocalVars.Warnings = .{};
    const env = try testing_ns.WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{ .logger = warnings.sink() });
    defer env.deinit();
    try env.registerWorkflow(LocalVars.SideEffects);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    LocalVars.eql_outcome = "not called";
    var run = try env.startWorkflow(LocalVars.SideEffects, .{}, .{});
    defer run.deinit();
    // Each function's set was refused and left 5; the set after them worked.
    try testing.expectEqualStrings("1 1 1 6", try env.getResult(LocalVars.SideEffects, &run, a, null));
    try testing.expectEqualStrings("refused", LocalVars.eql_outcome);
    // One for each refused set: `sideEffect`'s function, `mutableSideEffect`'s function on
    // both calls, and `eql` on the second.
    try testing.expectEqual(4, warnings.count.load(.monotonic));

    const history = try env.client.fetchHistory(a, run.id, run.run_id);
    var replayer: testing_ns.WorkflowReplayer = .init(testing.allocator, testing.io);
    defer replayer.deinit();
    try replayer.registerWorkflow(LocalVars.SideEffects);
    try replayer.replayHistory(history);
}

fn localVarActivation(a: std.mem.Allocator, job: act_pb.WorkflowActivationJob.variant_union) !act_pb.WorkflowActivation {
    var act: act_pb.WorkflowActivation = .{ .run_id = "run-1", .timestamp = .{ .seconds = 1_700_000_000 } };
    try act.jobs.append(a, .{ .variant = job });
    return act;
}

fn localVarCommands(c: done_pb.WorkflowActivationCompletion) []const cmd_pb.WorkflowCommand {
    return c.status.?.successful.commands.items;
}

const LocalVarHandlers = struct {
    ctx: *workflow.Context,

    const TrySet = workflow.QueryDef("local_var_try_set", fn () u32);
    const Read = workflow.QueryDef("local_var_read", fn () u32);
    const Bump = workflow.UpdateDef("local_var_bump", fn () u32);
    const Finish = workflow.SignalDef("local_var_finish", fn () void);

    /// What the last refused set returned, and what `count` read just after it.
    var refused: ?anyerror = null;
    var after_refusal: u32 = 0;
    /// What `count` read when the run started.
    var initial: u32 = 1234;

    fn trySet(h: *LocalVarHandlers) u32 {
        h.attempt();
        return LocalVars.count.get(h.ctx);
    }
    fn read(h: *LocalVarHandlers) u32 {
        return LocalVars.count.get(h.ctx);
    }
    fn validate(h: *LocalVarHandlers) void {
        h.attempt();
    }
    fn bump(h: *LocalVarHandlers) !u32 {
        try LocalVars.count.set(h.ctx, LocalVars.count.get(h.ctx) + 1);
        return LocalVars.count.get(h.ctx);
    }
    fn attempt(h: *LocalVarHandlers) void {
        refused = null;
        LocalVars.count.set(h.ctx, 42) catch |err| {
            refused = err;
        };
        after_refusal = LocalVars.count.get(h.ctx);
    }
    fn finish(_: *LocalVarHandlers) void {
        finished = true;
    }
    var finished = false;
    fn isFinished(_: *LocalVarHandlers) bool {
        return finished;
    }

    /// Sets `count` to 5 and a string variable from the run's memory, sets `count` 100 more
    /// times, and waits for `Finish`.
    fn run(ctx: *workflow.Context) !u32 {
        initial = LocalVars.count.get(ctx);
        const h = try ctx.allocator().create(LocalVarHandlers);
        h.* = .{ .ctx = ctx };
        try ctx.setQueryHandler(TrySet, h, trySet);
        try ctx.setQueryHandler(Read, h, read);
        try ctx.setUpdateHandlerWithValidator(Bump, h, bump, validate);
        try ctx.setSignalHandler(Finish, h, finish);
        try LocalVars.tag.set(ctx, try std.fmt.allocPrint(ctx.allocator(), "run {s}", .{ctx.info().workflow_id}));
        for (0..100) |i| try LocalVars.count.set(ctx, @intCast(i));
        try LocalVars.count.set(ctx, 5);
        try ctx.await(h, isFinished);
        return LocalVars.count.get(ctx);
    }
    const Def = workflow.Def("local_var_handlers", run);

    fn start(a: std.mem.Allocator) !*Instance {
        finished = false;
        const inst = try Instance.create(testing.allocator, Def.registered, .{});
        errdefer inst.destroy();
        const init: act_pb.InitializeWorkflow = .{ .workflow_type = "local_var_handlers", .workflow_id = "wf-1", .randomness_seed = 7, .attempt = 1 };
        try testing.expectEqual(0, localVarCommands(try inst.activate(&try localVarActivation(a, .{ .initialize_workflow = init }), a)).len);
        return inst;
    }

    fn query(inst: *Instance, a: std.mem.Allocator, query_type: []const u8) !cmd_pb.QueryResult.variant_union {
        const cmds = localVarCommands(try inst.activate(&try localVarActivation(a, .{ .query_workflow = .{ .query_id = "q1", .query_type = query_type } }), a));
        try testing.expectEqual(1, cmds.len);
        return cmds[0].variant.?.respond_to_query.variant.?;
    }

    fn readCount(inst: *Instance, a: std.mem.Allocator) !u32 {
        return converter.fromPayload(u32, a, (try query(inst, a, "local_var_read")).succeeded.response.?);
    }
};

test "LocalVar.set in a query handler or an update validator returns ReadOnly, leaves the value, and fails the query or rejects the update" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try LocalVarHandlers.start(a);
    defer inst.destroy();
    try testing.expectEqual(5, try LocalVarHandlers.readCount(inst, a));

    const allocated = inst.allocated;
    const failed = (try LocalVarHandlers.query(inst, a, "local_var_try_set")).failed;
    try testing.expectEqual(error.ReadOnly, LocalVarHandlers.refused.?);
    try testing.expectEqual(5, LocalVarHandlers.after_refusal);
    try testing.expect(std.mem.find(u8, failed.message, "LocalVar.set") != null);
    try testing.expectEqualStrings("ReadOnlyViolation", failed.failure_info.?.application_failure_info.type);
    try testing.expectEqual(allocated, inst.allocated);

    const update: act_pb.DoUpdate = .{ .id = "u1", .protocol_instance_id = "u1", .name = "local_var_bump", .run_validator = true };
    const cmds = localVarCommands(try inst.activate(&try localVarActivation(a, .{ .do_update = update }), a));
    try testing.expectEqual(1, cmds.len);
    const rejected = cmds[0].variant.?.update_response.response.?.rejected;
    try testing.expect(std.mem.find(u8, rejected.message, "LocalVar.set") != null);
    try testing.expectEqual(error.ReadOnly, LocalVarHandlers.refused.?);
    try testing.expectEqual(5, LocalVarHandlers.after_refusal);
    try testing.expectEqual(5, try LocalVarHandlers.readCount(inst, a));
}

test "LocalVar values are freed with the run on completion and on eviction, and a rebuilt run starts from the default" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();

    // Completion: the signal ends the run; destroy frees what it set (testing.allocator
    // checks).
    {
        const inst = try LocalVarHandlers.start(a);
        defer inst.destroy();
        // One entry per variable, however often it was set.
        try testing.expectEqual(2, inst.local_values.items.len);
        const cmds = localVarCommands(try inst.activate(&try localVarActivation(a, .{ .signal_workflow = .{ .signal_name = "local_var_finish" } }), a));
        try testing.expectEqual(1, cmds.len);
        const result = cmds[0].variant.?.complete_workflow_execution.result.?;
        try testing.expectEqual(5, try converter.fromPayload(u32, a, result));
    }

    // Eviction mid-run, then the same run rebuilt: it starts from the default again.
    {
        const inst = try LocalVarHandlers.start(a);
        try testing.expectEqual(0, LocalVarHandlers.initial);
        try testing.expectEqual(5, try LocalVarHandlers.readCount(inst, a));
        inst.destroy();
    }
    LocalVarHandlers.initial = 1234;
    const rebuilt = try LocalVarHandlers.start(a);
    defer rebuilt.destroy();
    try testing.expectEqual(0, LocalVarHandlers.initial);
}

/// A library's per-run state, generic over what it holds. `Slot` is declared inside the
/// function, so every instantiation has the same `@typeName` ("...LocalVarSlot.Slot") though
/// the types, and their sizes, differ.
fn LocalVarSlot(comptime V: type) type {
    const Slot = struct { value: V };
    return Slot;
}

const LocalVarSameTypeName = struct {
    const small: workflow.LocalVar(LocalVarSlot(u8)) = .{ .name = "test.slot", .default = .{ .value = 0 } };
    const big: workflow.LocalVar(LocalVarSlot([64]u8)) = .{ .name = "test.slot", .default = .{ .value = @splat(0xAA) } };

    /// What `big` read after `small` was set.
    var big_read: [64]u8 = undefined;

    fn run(ctx: *workflow.Context) !u32 {
        try small.set(ctx, .{ .value = 7 });
        big_read = big.get(ctx).value;
        return small.get(ctx).value;
    }
    const Def = workflow.Def("local_var_same_type_name", run);
};

test "LocalVar: the same name with another type whose @typeName is equal is another variable" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try Instance.create(testing.allocator, LocalVarSameTypeName.Def.registered, .{});
    defer inst.destroy();
    const init: act_pb.InitializeWorkflow = .{ .workflow_type = "local_var_same_type_name", .workflow_id = "wf-1", .randomness_seed = 7, .attempt = 1 };
    const cmds = localVarCommands(try inst.activate(&try localVarActivation(a, .{ .initialize_workflow = init }), a));
    try testing.expectEqual(1, cmds.len);
    try testing.expectEqual(7, try converter.fromPayload(u32, a, cmds[0].variant.?.complete_workflow_execution.result.?));
    // `big` was never set, so it reads its default, not the 1-byte box of `small` and the
    // arena memory after it.
    try testing.expectEqualSlices(u8, &@as([64]u8, @splat(0xAA)), &LocalVarSameTypeName.big_read);
}
