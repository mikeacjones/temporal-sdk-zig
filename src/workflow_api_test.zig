//! Workflow context APIs tested end to end against a dev server and the workflow replayer.

const std = @import("std");
const workflow = @import("workflow.zig");
const activity = @import("activity.zig");
const converter = @import("converter.zig");
const interceptor = @import("interceptor.zig");
const testing_ns = @import("testing.zig");

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
