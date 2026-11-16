//! The `__stack_trace` query, answered by `Instance` for every workflow from activations, and
//! by a replay of the same activations.

const std = @import("std");
const builtin = @import("builtin");
const proto = @import("../proto.zig");
const converter = @import("../converter.zig");
const workflow = @import("../workflow.zig");
const Instance = @import("Instance.zig");
const call_sites = @import("call_sites.zig");

const testing = std.testing;
const Allocator = std.mem.Allocator;
const act_pb = proto.coresdk.workflow_activation;
const cmd_pb = proto.coresdk.workflow_commands;
const done_pb = proto.coresdk.workflow_completion;

// ---------------------------------------------------------------- activations

fn initActivation(a: Allocator, workflow_type: []const u8) !act_pb.WorkflowActivation {
    const init: act_pb.InitializeWorkflow = .{ .workflow_type = workflow_type, .workflow_id = "wf-1", .randomness_seed = 7, .attempt = 1 };
    var act: act_pb.WorkflowActivation = .{ .run_id = "run-1", .timestamp = .{ .seconds = 1_700_000_000 } };
    try act.jobs.append(a, .{ .variant = .{ .initialize_workflow = init } });
    return act;
}

fn oneJob(a: Allocator, job: act_pb.WorkflowActivationJob.variant_union) !act_pb.WorkflowActivation {
    var act: act_pb.WorkflowActivation = .{ .run_id = "run-1", .timestamp = .{ .seconds = 1_700_000_010 } };
    try act.jobs.append(a, .{ .variant = job });
    return act;
}

fn updateJob(id: []const u8, name: []const u8) act_pb.WorkflowActivationJob.variant_union {
    return .{ .do_update = .{ .id = id, .protocol_instance_id = id, .name = name, .run_validator = true } };
}

fn commandsOf(c: done_pb.WorkflowActivationCompletion) []const cmd_pb.WorkflowCommand {
    return c.status.?.successful.commands.items;
}

/// Sends the `__stack_trace` query to `inst` and returns the text it answers with, checking
/// that the activation emits nothing else.
fn stackTrace(inst: *Instance, a: Allocator) ![]const u8 {
    var act: act_pb.WorkflowActivation = .{ .run_id = "run-1", .timestamp = .{ .seconds = 1_700_000_999 } };
    try act.jobs.append(a, .{ .variant = .{ .query_workflow = .{ .query_id = "q1", .query_type = "__stack_trace" } } });
    const cmds = commandsOf(try inst.activate(&act, a));
    try testing.expectEqual(@as(usize, 1), cmds.len);
    const result = cmds[0].variant.?.respond_to_query;
    try testing.expectEqualStrings("q1", result.query_id);
    const payload = switch (result.variant.?) {
        .succeeded => |s| s.response.?,
        .failed => |f| {
            std.debug.print("__stack_trace failed: {s}\n", .{f.message});
            return error.TestUnexpectedResult;
        },
    };
    return converter.fromPayload([]const u8, a, payload);
}

/// The entry of `text` whose first line contains `header`: that line and the location lines
/// under it.
fn entry(text: []const u8, header: []const u8) ![]const u8 {
    var entries = std.mem.splitSequence(u8, text, "\n\n");
    while (entries.next()) |e| {
        const first = e[0 .. std.mem.findScalar(u8, e, '\n') orelse e.len];
        if (std.mem.find(u8, first, header) != null) return e;
    }
    std.debug.print("no entry with \"{s}\" in:\n{s}\n", .{ header, text });
    return error.TestExpectedEntry;
}

/// Checks that `e`'s innermost call site is `line` of this file. A build without debug info,
/// or one where optimization inlined the frame away, writes `unknown_location` instead; that
/// is accepted only in optimized builds, which then must say exactly that.
fn expectCallSite(e: []const u8, line: u32) !void {
    var lines = std.mem.splitScalar(u8, e, '\n');
    _ = lines.next();
    const site = lines.next() orelse return error.TestExpectedCallSite;
    if (builtin.mode != .debug and std.mem.eql(u8, site, "  " ++ call_sites.unknown_location)) return;
    var buf: [64]u8 = undefined;
    const want = try std.fmt.bufPrint(&buf, "stack_trace_test.zig:{d}:", .{line});
    if (!std.mem.startsWith(u8, site, "  at ") or std.mem.find(u8, site, want) == null) {
        std.debug.print("expected a call site at {s} in:\n{s}\n", .{ want, e });
        return error.TestExpectedCallSite;
    }
}

// ---------------------------------------------------------------- workflows

/// The lines of the waits below, set as the code reaches them.
var wait_lines: struct { activity: u32 = 0, timer: u32 = 0, condition: u32 = 0 } = .{};

const Approve = workflow.UpdateDef("approve", fn () []const u8);

const Waits = struct {
    ctx: *workflow.Context,
    approved: bool = false,

    fn approve(s: *Waits) ![]const u8 {
        wait_lines.condition = @src().line + 1;
        try s.ctx.await(s, approvedFn);
        return "approved";
    }

    fn approvedFn(s: *Waits) bool {
        return s.approved;
    }

    fn nap(ctx: *workflow.Context) void {
        wait_lines.timer = @src().line + 1;
        ctx.sleep(.fromSeconds(60), .{}) catch {};
    }

    /// Waits on an activity, starts a coroutine that waits on a timer, and handles `approve`
    /// updates with a handler that waits on a condition.
    fn run(ctx: *workflow.Context) ![]const u8 {
        const s = try ctx.allocator().create(Waits);
        s.* = .{ .ctx = ctx };
        try ctx.setUpdateHandler(Approve, s, approve);
        var napping = ctx.io().async(nap, .{ctx});
        defer napping.cancel(ctx.io());
        wait_lines.activity = @src().line + 1;
        _ = try ctx.executeActivityByName(void, "slow", .{ .start_to_close_timeout = .fromSeconds(10) }, .{}).get();
        return "done";
    }
};
const WaitsWf = workflow.Def("Waits", Waits.run);

/// Runs `Waits` until its workflow function waits on the activity, its coroutine on the
/// timer, and an `approve` update's handler on the condition.
fn startWaits(inst: *Instance, a: Allocator, replaying: bool) !void {
    var init = try initActivation(a, "Waits");
    init.is_replaying = replaying;
    const started = commandsOf(try inst.activate(&init, a));
    try testing.expectEqual(@as(usize, 2), started.len);
    try testing.expectEqualStrings("slow", started[0].variant.?.schedule_activity.activity_type);
    _ = started[1].variant.?.start_timer;
    var update = try oneJob(a, updateJob("u1", "approve"));
    update.is_replaying = replaying;
    const accepted = commandsOf(try inst.activate(&update, a));
    try testing.expectEqual(@as(usize, 1), accepted.len);
    _ = accepted[0].variant.?.update_response.response.?.accepted;
}

test "__stack_trace lists each coroutine with what it waits on and its call site" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try Instance.create(testing.allocator, WaitsWf.registered, .{});
    defer inst.destroy();
    try startWaits(inst, a, false);

    const text = try stackTrace(inst, a);
    try testing.expect(std.mem.startsWith(u8, text, "workflow \"Waits\": 3 coroutines\n"));
    try expectCallSite(try entry(text, "coroutine 1 \"workflow\", the workflow function, waits on activity \"slow\""), wait_lines.activity);
    try expectCallSite(try entry(text, "coroutine 2 \"async\", started with io.async or io.concurrent, waits on a timer (1m)"), wait_lines.timer);
    try expectCallSite(try entry(text, "coroutine 3 \"update\", update handler \"approve\" (update ID u1), waits on a condition (Context.await)"), wait_lines.condition);
}

test "__stack_trace changes nothing, and a replay of the same history answers the same" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const first = try Instance.create(testing.allocator, WaitsWf.registered, .{});
    defer first.destroy();
    try startWaits(first, a, false);

    const allocated = first.allocated;
    const seqs = .{ first.next_activity_seq, first.next_timer_seq };
    const answer = try stackTrace(first, a);
    try testing.expectEqualStrings(answer, try stackTrace(first, a));
    try testing.expectEqual(allocated, first.allocated);
    try testing.expectEqual(seqs, .{ first.next_activity_seq, first.next_timer_seq });

    const replayed = try Instance.create(testing.allocator, WaitsWf.registered, .{});
    defer replayed.destroy();
    try startWaits(replayed, a, true);
    try testing.expectEqualStrings(answer, try stackTrace(replayed, a));

    // The run goes on as if no query had come: the timer fires and its coroutine ends, and
    // the next query no longer lists it.
    try testing.expectEqual(@as(usize, 0), commandsOf(try first.activate(&try oneJob(a, .{ .fire_timer = .{ .seq = 1 } }), a)).len);
    const after = try stackTrace(first, a);
    try testing.expect(std.mem.startsWith(u8, after, "workflow \"Waits\": 2 coroutines\n"));
    try testing.expect(std.mem.find(u8, after, "a timer") == null);
}

const Quick = workflow.Def("Quick", struct {
    fn run(_: *workflow.Context) ![]const u8 {
        return "quick";
    }
}.run);

test "__stack_trace on a closed workflow says it closed" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try Instance.create(testing.allocator, Quick.registered, .{});
    defer inst.destroy();
    _ = commandsOf(try inst.activate(&try initActivation(a, "Quick"), a))[0].variant.?.complete_workflow_execution;
    try testing.expectEqualStrings("workflow \"Quick\" has completed and closed: no coroutine is waiting\n", try stackTrace(inst, a));
}

const OwnStackTrace = workflow.QueryDef("__stack_trace", fn () []const u8);
const AnyQuery = struct {
    fn answer(_: *const u8, name: []const u8, _: converter.EncodedValues) []const u8 {
        return name;
    }
};

const Custom = workflow.Def("Custom", struct {
    fn ownTrace(_: *const u8) []const u8 {
        return "the workflow's own answer";
    }
    fn run(ctx: *workflow.Context) ![]const u8 {
        try ctx.setQueryHandler(OwnStackTrace, &own_context, ownTrace);
        try ctx.io().sleep(.fromSeconds(60), .awake);
        return "done";
    }
}.run);
const own_context: u8 = 0;

const Dynamic = workflow.Def("Dynamic", struct {
    fn run(ctx: *workflow.Context) ![]const u8 {
        try ctx.setDynamicQueryHandler(&own_context, AnyQuery.answer);
        try ctx.io().sleep(.fromSeconds(60), .awake);
        return "done";
    }
}.run);

test "__stack_trace: a handler the workflow registers for it answers instead; a dynamic query handler doesn't" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();

    const custom = try Instance.create(testing.allocator, Custom.registered, .{});
    defer custom.destroy();
    _ = try custom.activate(&try initActivation(a, "Custom"), a);
    try testing.expectEqualStrings("the workflow's own answer", try stackTrace(custom, a));

    const dynamic = try Instance.create(testing.allocator, Dynamic.registered, .{});
    defer dynamic.destroy();
    _ = try dynamic.activate(&try initActivation(a, "Dynamic"), a);
    const text = try stackTrace(dynamic, a);
    _ = try entry(text, "coroutine 1 \"workflow\", the workflow function, waits on a timer (1m)");
}

const Sleeper = struct {
    pub fn run(_: *Sleeper, ctx: *workflow.Context) ![]const u8 {
        try ctx.io().sleep(.fromSeconds(5), .awake);
        return "rested";
    }
};
const StructWf = workflow.Def("StructSleeper", Sleeper);

fn anyWorkflow(ctx: *workflow.Context, _: converter.EncodedValues) ![]const u8 {
    try ctx.io().sleep(.fromSeconds(5), .awake);
    return "rested";
}
const AnyWf = workflow.DynamicDef(anyWorkflow);

test "__stack_trace is answered for struct and dynamic workflows with no registration" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    for ([_]struct { workflow.Registered, []const u8 }{
        .{ StructWf.registered, "StructSleeper" },
        .{ AnyWf.registered, "SomeType" },
    }) |case| {
        const inst = try Instance.create(testing.allocator, case[0], .{});
        defer inst.destroy();
        _ = try inst.activate(&try initActivation(a, case[1]), a);
        const text = try stackTrace(inst, a);
        try testing.expect(std.mem.startsWith(u8, text, try std.fmt.allocPrint(a, "workflow \"{s}\": 1 coroutine\n", .{case[1]})));
        _ = try entry(text, "coroutine 1 \"workflow\", the workflow function, waits on a timer (5s)");
    }
}

const Ping = workflow.SignalDef("ping", fn (u32) void);
const ChildWf = workflow.Def("Child", struct {
    fn run(_: *workflow.Context) !void {}
}.run);

const Kinds = workflow.Def("Kinds", struct {
    fn receive(ctx: *workflow.Context) void {
        const ch = ctx.signalChannel(Ping) catch return;
        _ = ch.receive() catch {};
    }
    fn lock(ctx: *workflow.Context, m: *std.Io.Mutex) void {
        m.lock(ctx.io()) catch {};
    }
    fn run(ctx: *workflow.Context) !void {
        const io = ctx.io();
        var group: std.Io.Group = .init;
        defer group.cancel(io);
        group.async(io, receive, .{ctx});
        var m: std.Io.Mutex = .init;
        try m.lock(io);
        defer m.unlock(io);
        group.async(io, lock, .{ ctx, &m });
        _ = try ctx.executeChildWorkflow(ChildWf, .{ .id = "child-1" }, .{}).get();
    }
}.run);

test "__stack_trace names signal channels, std.Io primitives and child workflows" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try Instance.create(testing.allocator, Kinds.registered, .{});
    defer inst.destroy();
    _ = try inst.activate(&try initActivation(a, "Kinds"), a);
    const text = try stackTrace(inst, a);
    _ = try entry(text, "coroutine 1 \"workflow\", the workflow function, waits on child workflow \"Child\"");
    _ = try entry(text, "coroutine 2 \"group\", started in a std.Io.Group, waits on signal channel \"ping\"");
    _ = try entry(text, "coroutine 3 \"group\", started in a std.Io.Group, waits on a std.Io Mutex, Condition, Event, Semaphore or Queue");
}

/// Runs `activity_type` activities one after another for as long as the run lasts.
fn ActivityLoop(comptime activity_type: []const u8) type {
    return workflow.Def("ActivityLoop", struct {
        fn run(ctx: *workflow.Context) ![]const u8 {
            while (true) {
                _ = try ctx.executeActivityByName(void, activity_type, .{ .start_to_close_timeout = .fromSeconds(10) }, .{}).get();
            }
        }
    }.run);
}

/// The bytes the run's memory grows by over `n` activities of `activity_type`.
fn runGrowth(comptime activity_type: []const u8, n: u32) !usize {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try Instance.create(testing.allocator, ActivityLoop(activity_type).registered, .{});
    defer inst.destroy();
    _ = try inst.activate(&try initActivation(a, "ActivityLoop"), a);
    const before = inst.allocated;
    for (1..n + 1) |seq| {
        const cmds = commandsOf(try inst.activate(&try oneJob(a, .{ .resolve_activity = .{ .seq = @intCast(seq), .result = .{ .status = .{ .completed = .{} } } } }), a));
        try testing.expectEqualStrings(activity_type, cmds[0].variant.?.schedule_activity.activity_type);
    }
    return inst.allocated - before;
}

const long_activity_type: [4096]u8 = @splat('x');

test "__stack_trace: recording what an activity waits on allocates nothing, however long the run" {
    // When no query comes, recording a wait costs a few stores and no allocation: the run
    // grows the same over 100 activities whatever the length of their type name.
    try testing.expectEqual(try runGrowth("x", 100), try runGrowth(&long_activity_type, 100));
}

const LaterAwait = workflow.Def("LaterAwait", struct {
    fn run(ctx: *workflow.Context) ![]const u8 {
        const long = ctx.executeActivityByName(void, &long_activity_type, .{ .start_to_close_timeout = .fromSeconds(10) }, .{});
        _ = try long.get();
        const later = ctx.executeActivityByName(void, "later", .{ .start_to_close_timeout = .fromSeconds(10) }, .{});
        try ctx.io().sleep(.fromSeconds(5), .awake);
        _ = try later.get();
        return "done";
    }
}.run);

test "__stack_trace: a long activity type is cut short, and one awaited in a later task is named by its seq" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try Instance.create(testing.allocator, LaterAwait.registered, .{});
    defer inst.destroy();
    _ = try inst.activate(&try initActivation(a, "LaterAwait"), a);
    _ = try entry(try stackTrace(inst, a), "waits on activity \"" ++ long_activity_type[0..61] ++ "...\"");

    // The type name the caller passed is only borrowed for the task that scheduled the
    // activity; a wait that begins in a later task gives the seq instead.
    _ = try inst.activate(&try oneJob(a, .{ .resolve_activity = .{ .seq = 1, .result = .{ .status = .{ .completed = .{} } } } }), a);
    _ = try inst.activate(&try oneJob(a, .{ .fire_timer = .{ .seq = 1 } }), a);
    _ = try entry(try stackTrace(inst, a), "the workflow function, waits on activity (seq 2)");
}

/// 90 bytes of three-byte characters: a cut at 61 bytes falls inside one.
const multibyte_activity_type = "订单处理活动订单处理活动订单处理活动订单处理活动订单处理活动";

const MultibyteType = workflow.Def("MultibyteType", struct {
    fn run(ctx: *workflow.Context) ![]const u8 {
        _ = try ctx.executeActivityByName(void, multibyte_activity_type, .{ .start_to_close_timeout = .fromSeconds(10) }, .{}).get();
        return "done";
    }
}.run);

test "__stack_trace: a long activity type of multi-byte characters is cut between characters, so the answer is a JSON string" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try Instance.create(testing.allocator, MultibyteType.registered, .{});
    defer inst.destroy();
    _ = try inst.activate(&try initActivation(a, "MultibyteType"), a);

    var act: act_pb.WorkflowActivation = .{ .run_id = "run-1", .timestamp = .{ .seconds = 1_700_000_999 } };
    try act.jobs.append(a, .{ .variant = .{ .query_workflow = .{ .query_id = "q1", .query_type = "__stack_trace" } } });
    const cmds = commandsOf(try inst.activate(&act, a));
    const data = cmds[0].variant.?.respond_to_query.variant.?.succeeded.response.?.data;
    // Temporal's UI and CLI read the answer as a JSON string; text that isn't valid UTF-8
    // encodes as an array of byte values instead, which they can't show.
    try testing.expect(data.len > 0 and data[0] == '"');
    const text = try converter.fromPayload([]const u8, a, cmds[0].variant.?.respond_to_query.variant.?.succeeded.response.?);
    try testing.expect(std.unicode.utf8ValidateSlice(text));
    const e = try entry(text, "waits on activity \"" ++ multibyte_activity_type[0..18]);
    try testing.expect(std.mem.find(u8, e, "...\"\n") != null);
}

const ForgedNames = workflow.Def("ForgedNames", struct {
    fn run(ctx: *workflow.Context) ![]const u8 {
        try ctx.setCoroutineName("main\n\ncoroutine 9 \"fake\", the workflow function, waits on nothing\xff");
        _ = try ctx.executeActivityByName(void, "bad\xe8\xae\"type\n", .{ .start_to_close_timeout = .fromSeconds(10) }, .{}).get();
        return "done";
    }
}.run);

test "__stack_trace: names that aren't valid UTF-8 or hold newlines are escaped, so the answer stays one JSON string with its own entries" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try Instance.create(testing.allocator, ForgedNames.registered, .{});
    defer inst.destroy();
    _ = try inst.activate(&try initActivation(a, "ForgedNames"), a);

    var act: act_pb.WorkflowActivation = .{ .run_id = "run-1", .timestamp = .{ .seconds = 1_700_000_999 } };
    try act.jobs.append(a, .{ .variant = .{ .query_workflow = .{ .query_id = "q1", .query_type = "__stack_trace" } } });
    const response = commandsOf(try inst.activate(&act, a))[0].variant.?.respond_to_query.variant.?.succeeded.response.?;
    try testing.expectEqual(@as(u8, '"'), response.data[0]);
    const text = try converter.fromPayload([]const u8, a, response);
    try testing.expect(std.unicode.utf8ValidateSlice(text));
    try testing.expectEqual(@as(usize, 1), std.mem.count(u8, text, "\n\ncoroutine "));
    _ = try entry(text, "coroutine 1 \"main\\n\\ncoroutine 9 \\\"fake\\\", the workflow function, waits on nothing\u{fffd}\", the workflow function, waits on activity \"bad\u{fffd}\u{fffd}\\\"type\\n\"");
}

/// Watches a scheduler's `switches` as the worker's deadlock detector does, and keeps the
/// longest stretch it saw the count unchanged.
const ProgressWatch = struct {
    switches: *const std.atomic.Value(u64),
    done: std.atomic.Value(bool) = .init(false),
    longest: std.Io.Duration = .zero,

    fn run(w: *ProgressWatch) void {
        const io = std.Options.debug_io;
        var seen = w.switches.load(.acquire);
        var since = std.Io.Clock.awake.now(io);
        while (!w.done.load(.acquire)) {
            std.Io.sleep(io, .fromMilliseconds(2), .awake) catch return;
            const now = std.Io.Clock.awake.now(io);
            const gap = since.durationTo(now);
            if (gap.nanoseconds > w.longest.nanoseconds) w.longest = gap;
            const switches = w.switches.load(.acquire);
            if (switches != seen) {
                seen = switches;
                since = now;
            }
        }
    }
};

test "__stack_trace keeps noting progress while a slow read of debug info runs, so deadlock detection doesn't fire" {
    if (builtin.single_threaded) return error.SkipZigTest;
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try Instance.create(testing.allocator, WaitsWf.registered, .{});
    defer inst.destroy();
    try startWaits(inst, a, false);

    // Stands in for a first read of debug info that is slower than the deadlock limit.
    const stall: std.Io.Duration = .fromMilliseconds(500);
    Instance.stack_trace_test_stall = stall;
    defer Instance.stack_trace_test_stall = null;
    var watch: ProgressWatch = .{ .switches = &inst.sched.switches };
    const thread = try std.Thread.spawn(.{}, ProgressWatch.run, .{&watch});
    const text = stackTrace(inst, a);
    watch.done.store(true, .release);
    thread.join();
    _ = try entry(try text, "coroutine 1 \"workflow\", the workflow function, waits on activity \"slow\"");
    if (watch.longest.nanoseconds >= @divTrunc(stall.nanoseconds, 2)) {
        std.debug.print("the progress count stood still for {f} while the answer stalled for {f}\n", .{ watch.longest, stall });
        return error.TestUnexpectedResult;
    }
}
