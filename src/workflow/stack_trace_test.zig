//! The `__stack_trace` query, answered by `Instance` for every workflow from activations, and
//! by a replay of the same activations.

const std = @import("std");
const builtin = @import("builtin");
const proto = @import("../proto.zig");
const converter = @import("../converter.zig");
const workflow = @import("../workflow.zig");
const Instance = @import("Instance.zig");
const call_sites = @import("call_sites.zig");
const interceptor = @import("../interceptor.zig");

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

/// Passes allocations to `backing`, noting whether `sdk_work` was set at each one; with
/// `fail_during_sdk_work`, refuses those made while it is set.
const SdkWorkSampler = struct {
    backing: Allocator,
    sdk_work: *const std.atomic.Value(bool),
    fail_during_sdk_work: bool = false,
    during: usize = 0,
    outside: usize = 0,

    fn allocator(s: *SdkWorkSampler) Allocator {
        return .{ .ptr = s, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }

    /// Whether the allocation may go ahead.
    fn sample(s: *SdkWorkSampler) bool {
        if (!s.sdk_work.load(.acquire)) {
            s.outside += 1;
            return true;
        }
        s.during += 1;
        return !s.fail_during_sdk_work;
    }

    fn alloc(ptr: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const s: *SdkWorkSampler = @ptrCast(@alignCast(ptr));
        if (!s.sample()) return null;
        return s.backing.rawAlloc(len, alignment, ret_addr);
    }

    fn resize(ptr: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
        const s: *SdkWorkSampler = @ptrCast(@alignCast(ptr));
        if (!s.sample()) return false;
        return s.backing.rawResize(memory, alignment, new_len, ret_addr);
    }

    fn remap(ptr: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        const s: *SdkWorkSampler = @ptrCast(@alignCast(ptr));
        if (!s.sample()) return null;
        return s.backing.rawRemap(memory, alignment, new_len, ret_addr);
    }

    fn free(ptr: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const s: *SdkWorkSampler = @ptrCast(@alignCast(ptr));
        s.backing.rawFree(memory, alignment, ret_addr);
    }
};

test "__stack_trace marks its read of debug info as SDK work for deadlock detection, and clears the mark on every way out" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try Instance.create(testing.allocator, WaitsWf.registered, .{});
    defer inst.destroy();
    try startWaits(inst, a, false);
    try testing.expect(!inst.sched.sdk_work.load(.acquire));

    var sampler: SdkWorkSampler = .{ .backing = a, .sdk_work = &inst.sched.sdk_work };
    const text = try stackTrace(inst, sampler.allocator());
    _ = try entry(text, "coroutine 1 \"workflow\", the workflow function, waits on activity \"slow\"");
    // The answer is written, and the debug info's roots copied, with the flag set.
    try testing.expect(sampler.during > 0);
    try testing.expect(sampler.outside > 0);
    try testing.expect(!inst.sched.sdk_work.load(.acquire));

    // Running out of memory while the flag is set fails the answer and clears the flag.
    var failing: SdkWorkSampler = .{ .backing = a, .sdk_work = &inst.sched.sdk_work, .fail_during_sdk_work = true };
    var act: act_pb.WorkflowActivation = .{ .run_id = "run-1", .timestamp = .{ .seconds = 1_700_000_999 } };
    try act.jobs.append(a, .{ .variant = .{ .query_workflow = .{ .query_id = "q1", .query_type = "__stack_trace" } } });
    const answered = inst.activate(&act, failing.allocator());
    try testing.expect(failing.during > 0);
    try testing.expect(!inst.sched.sdk_work.load(.acquire));
    if (answered) |completion| {
        const cmds = commandsOf(completion);
        try testing.expectEqual(@as(usize, 1), cmds.len);
        try testing.expect(cmds[0].variant.?.respond_to_query.variant.? == .failed);
    } else |err| try testing.expectEqual(error.OutOfMemory, err);

    // The run goes on: the next answer succeeds.
    _ = try entry(try stackTrace(inst, a), "coroutine 1 \"workflow\", the workflow function, waits on activity \"slow\"");
}

/// Asks for the `__stack_trace` answer through `next.handleQuery` from inside workflow code,
/// before the workflow function runs: a call `interceptor.WorkflowInbound.Next` allows.
const SelfTrace = struct {
    arena: Allocator,
    answered: ?anyerror!converter.Payload = null,

    pub fn interceptWorkflow(s: *SelfTrace, _: *workflow.Context) error{OutOfMemory}!interceptor.Workflow {
        return .{ .inbound = interceptor.WorkflowInbound.from(SelfTrace, s) };
    }

    pub fn executeWorkflow(s: *SelfTrace, input: interceptor.ExecuteWorkflowInput, next: interceptor.WorkflowInbound.Next) anyerror!converter.Payload {
        s.answered = next.handleQuery(.{ .query_type = "__stack_trace", .args = .{ .payloads = &.{} }, .header = .{}, .arena = s.arena });
        return next.executeWorkflow(input);
    }
};

test "__stack_trace asked for from inside workflow code by an interceptor doesn't crash the worker, and leaves sdk_work clear" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    var self_trace: SelfTrace = .{ .arena = a };
    const interceptors = [_]interceptor.Worker{.from(SelfTrace, &self_trace)};
    const inst = try Instance.create(testing.allocator, WaitsWf.registered, .{ .interceptors = &interceptors });
    defer inst.destroy();
    var init = try initActivation(a, "Waits");
    const started = commandsOf(try inst.activate(&init, a));
    // The workflow function still runs after the interceptor's call, whatever it answered.
    try testing.expect(self_trace.answered != null);
    try testing.expectEqual(@as(usize, 2), started.len);
    try testing.expectEqualStrings("slow", started[0].variant.?.schedule_activity.activity_type);
    try testing.expect(!inst.sched.sdk_work.load(.acquire));
}

const FailsWf = workflow.Def("Fails", struct {
    fn run(ctx: *workflow.Context) ![]const u8 {
        _ = ctx;
        return error.Broken;
    }
}.run);

const Reject = workflow.UpdateDef("reject", fn () []const u8);

/// Waits on a timer, with a `reject` update handler that fails.
const Rejects = struct {
    fn reject(_: *Rejects) ![]const u8 {
        return error.Refused;
    }

    fn run(ctx: *workflow.Context) ![]const u8 {
        const s = try ctx.allocator().create(Rejects);
        try ctx.setUpdateHandler(Reject, s, reject);
        try ctx.sleep(.fromSeconds(60), .{});
        return "done";
    }
};
const RejectsWf = workflow.Def("Rejects", Rejects.run);

const FailCase = enum { workflow, update };

/// What one failing activation of `case` did with `options` and `sampler` as its memory.
const FailedRun = struct {
    /// The failure's stack trace text, or null when the activation ran out of memory or
    /// failed its task.
    stack_trace: ?[]const u8,
};

/// Runs the activation of `case` that fails: the workflow function's return of an error, or
/// an update whose handler returns one. The flag must be clear before and after.
fn runFailing(case: FailCase, options: Instance.Options, sampler: *SdkWorkSampler) !FailedRun {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try Instance.create(testing.allocator, switch (case) {
        .workflow => FailsWf.registered,
        .update => RejectsWf.registered,
    }, options);
    defer inst.destroy();
    sampler.sdk_work = &inst.sched.sdk_work;
    var act = switch (case) {
        .workflow => try initActivation(a, "Fails"),
        .update => act: {
            var init = try initActivation(a, "Rejects");
            const started = commandsOf(try inst.activate(&init, a));
            try testing.expectEqual(@as(usize, 1), started.len);
            _ = started[0].variant.?.start_timer;
            break :act try oneJob(a, updateJob("u1", "reject"));
        },
    };
    try testing.expect(!inst.sched.sdk_work.load(.acquire));
    const done = inst.activate(&act, sampler.allocator());
    try testing.expect(!inst.sched.sdk_work.load(.acquire));
    const completion = done catch |err| {
        try testing.expectEqual(error.OutOfMemory, err);
        return .{ .stack_trace = null };
    };
    const status = completion.status orelse return error.TestUnexpectedResult;
    if (status == .failed) return .{ .stack_trace = null };
    for (status.successful.commands.items) |cmd| switch (cmd.variant.?) {
        .fail_workflow_execution => |f| if (case == .workflow) return .{ .stack_trace = try a.dupe(u8, f.failure.?.stack_trace) },
        .update_response => |r| if (case == .update) switch (r.response.?) {
            .rejected => |f| return .{ .stack_trace = try a.dupe(u8, f.stack_trace) },
            else => {},
        },
        else => {},
    };
    std.debug.print("no failure for the {t} case among {d} commands\n", .{ case, status.successful.commands.items.len });
    return error.TestExpectedFailure;
}

test "a failed workflow's and a failed update's stack trace text is built as SDK work for deadlock detection, and the mark is cleared on every way out" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    for ([_]FailCase{ .workflow, .update }) |case| {
        var with: SdkWorkSampler = .{ .backing = a, .sdk_work = undefined };
        const traced = try runFailing(case, .{}, &with);
        const text = traced.stack_trace orelse return error.TestExpectedFailure;
        if (builtin.mode == .debug) try testing.expect(text.len > 0);
        // Each allocation for the text happens with the flag set...
        if (text.len > 0) try testing.expect(with.during > 0);

        // ...and only those: without stack traces, none does, and the others are the same.
        var without: SdkWorkSampler = .{ .backing = a, .sdk_work = undefined };
        const untraced = try runFailing(case, .{ .failure_stack_traces = false }, &without);
        try testing.expectEqualStrings("", untraced.stack_trace orelse return error.TestExpectedFailure);
        try testing.expectEqual(@as(usize, 0), without.during);
        try testing.expectEqual(without.outside, with.outside);

        // Running out of memory while the flag is set fails the activation and clears the flag.
        if (text.len > 0) {
            var failing: SdkWorkSampler = .{ .backing = a, .sdk_work = undefined, .fail_during_sdk_work = true };
            const refused = try runFailing(case, .{}, &failing);
            try testing.expect(failing.during > 0);
            try testing.expect(refused.stack_trace == null);
        }
    }
}

// ---------------------------------------------------------------- __enhanced_stack_trace and __temporal_workflow_metadata

/// Sends query `Query` (a `workflow.QueryDef`) to `inst` and returns the answer decoded,
/// checking that the activation emits nothing else.
fn queryAnswer(comptime Query: type, inst: *Instance, a: Allocator) !Query.Result {
    var act: act_pb.WorkflowActivation = .{ .run_id = "run-1", .timestamp = .{ .seconds = 1_700_000_999 } };
    try act.jobs.append(a, .{ .variant = .{ .query_workflow = .{ .query_id = "q1", .query_type = Query.query_name } } });
    const cmds = commandsOf(try inst.activate(&act, a));
    try testing.expectEqual(@as(usize, 1), cmds.len);
    const payload = switch (cmds[0].variant.?.respond_to_query.variant.?) {
        .succeeded => |s| s.response.?,
        .failed => |f| {
            std.debug.print("{s} failed: {s}\n", .{ Query.query_name, f.message });
            return error.TestUnexpectedResult;
        },
    };
    return converter.fromPayload(Query.Result, a, payload);
}

const this_file = @embedFile("stack_trace_test.zig");

/// Line `n` (1-based) of this file.
fn sourceLine(n: u32) []const u8 {
    var lines = std.mem.splitScalar(u8, this_file, '\n');
    for (1..n) |_| _ = lines.next();
    return lines.next() orelse "";
}

/// Checks that `stack`'s innermost location is `line` of this file, and that `trace.sources`
/// holds that file with an excerpt that has the line and the five around it, as they are in
/// the file. An optimized build may have no locations for it (see `expectCallSite`).
fn expectLocation(trace: workflow.EnhancedStackTrace, stack: workflow.EnhancedStackTrace.Stack, line: u32) !void {
    if (stack.locations.len == 0 and builtin.mode != .debug) return;
    try testing.expect(stack.locations.len > 0);
    const loc = stack.locations[0];
    try testing.expect(std.mem.endsWith(u8, loc.file_path, "stack_trace_test.zig"));
    try testing.expectEqual(@as(i32, @intCast(line)), loc.line);
    try testing.expect(loc.column > 0);
    try testing.expect(loc.function_name.len > 0);
    try testing.expect(!loc.internal_code);
    const slices = trace.sources.map.get(loc.file_path) orelse return error.TestExpectedSource;
    for (slices) |slice| {
        const first = slice.line_offset + 1;
        const count: u32 = @intCast(std.mem.count(u8, slice.content, "\n"));
        if (line < first or line >= first + count) continue;
        try testing.expect(first + call_sites.excerpt_context_lines <= line);
        try testing.expect(line + call_sites.excerpt_context_lines < first + count);
        var lines = std.mem.splitScalar(u8, slice.content, '\n');
        for (first..first + count) |n| try testing.expectEqualStrings(sourceLine(@intCast(n)), lines.next().?);
        return;
    }
    std.debug.print("no excerpt holds line {d}\n", .{line});
    return error.TestExpectedSource;
}

test "__enhanced_stack_trace gives each coroutine's call sites and the source around them" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try Instance.create(testing.allocator, WaitsWf.registered, .{});
    defer inst.destroy();
    try startWaits(inst, a, false);

    const trace = try queryAnswer(workflow.EnhancedStackTraceQuery, inst, a);
    try testing.expectEqualStrings("temporal-zig", trace.sdk.name);
    try testing.expectEqualStrings(@import("build_options").version, trace.sdk.version);
    try testing.expectEqual(@as(usize, 3), trace.stacks.len);
    try expectLocation(trace, trace.stacks[0], wait_lines.activity);
    try expectLocation(trace, trace.stacks[1], wait_lines.timer);
    try expectLocation(trace, trace.stacks[2], wait_lines.condition);
    // Excerpts of one file are in ascending order and don't overlap or touch.
    for (trace.sources.map.values()) |slices| for (slices[0..slices.len -| 1], slices[@min(1, slices.len)..]) |x, y| {
        try testing.expect(x.line_offset + std.mem.count(u8, x.content, "\n") + 1 < y.line_offset + 1);
    };
}

test "__enhanced_stack_trace and __temporal_workflow_metadata change nothing, and a replay of the same history answers the same" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const first = try Instance.create(testing.allocator, WaitsWf.registered, .{});
    defer first.destroy();
    try startWaits(first, a, false);

    const allocated = first.allocated;
    const seqs = .{ first.next_activity_seq, first.next_timer_seq };
    const trace = try std.json.Stringify.valueAlloc(a, try queryAnswer(workflow.EnhancedStackTraceQuery, first, a), .{});
    const metadata = try std.json.Stringify.valueAlloc(a, try queryAnswer(workflow.WorkflowMetadataQuery, first, a), .{});
    try testing.expectEqual(allocated, first.allocated);
    try testing.expectEqual(seqs, .{ first.next_activity_seq, first.next_timer_seq });

    const replayed = try Instance.create(testing.allocator, WaitsWf.registered, .{});
    defer replayed.destroy();
    try startWaits(replayed, a, true);
    try testing.expectEqualStrings(trace, try std.json.Stringify.valueAlloc(a, try queryAnswer(workflow.EnhancedStackTraceQuery, replayed, a), .{}));
    try testing.expectEqualStrings(metadata, try std.json.Stringify.valueAlloc(a, try queryAnswer(workflow.WorkflowMetadataQuery, replayed, a), .{}));

    // The run goes on as if no query had come.
    try testing.expectEqual(@as(usize, 0), commandsOf(try first.activate(&try oneJob(a, .{ .fire_timer = .{ .seq = 1 } }), a)).len);
    try testing.expectEqual(@as(usize, 2), (try queryAnswer(workflow.EnhancedStackTraceQuery, first, a)).stacks.len);
}

/// Checks that `defs` holds exactly `want` (name and description pairs), in that order.
fn expectDefinitions(defs: std.ArrayList(proto.temporal.api.sdk.v1.WorkflowInteractionDefinition), want: []const [2][]const u8) !void {
    errdefer for (defs.items) |d| std.debug.print("  {s}: {s}\n", .{ d.name, d.description });
    try testing.expectEqual(want.len, defs.items.len);
    for (want, defs.items) |w, d| {
        try testing.expectEqualStrings(w[0], d.name);
        try testing.expectEqualStrings(w[1], d.description);
    }
}

const builtin_enhanced = [2][]const u8{ "__enhanced_stack_trace", "Where each coroutine of the workflow waits, as locations with the source around them." };
const builtin_stack_trace = [2][]const u8{ "__stack_trace", "Where each coroutine of the workflow waits, as text." };
const builtin_metadata = [2][]const u8{ "__temporal_workflow_metadata", "The workflow's type and its query, signal and update handlers with their descriptions." };

test "__temporal_workflow_metadata lists the workflow type and the built-in queries for a workflow with no handlers of its own" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try Instance.create(testing.allocator, WaitsWf.registered, .{});
    defer inst.destroy();
    try startWaits(inst, a, false);
    const metadata = try queryAnswer(workflow.WorkflowMetadataQuery, inst, a);
    try testing.expectEqualStrings("", metadata.current_details);
    const def = metadata.definition.?;
    try testing.expectEqualStrings("Waits", def.type);
    try expectDefinitions(def.query_definitions, &.{ builtin_enhanced, builtin_stack_trace, builtin_metadata });
    try expectDefinitions(def.signal_definitions, &.{});
    try expectDefinitions(def.update_definitions, &.{.{ "approve", "" }});
}

const Status = workflow.QueryDef("status", fn () []const u8);
const Count = workflow.QueryDef("count", fn () u32);
const Add = workflow.SignalDef("add", fn (u32) void);
const Reset = workflow.UpdateDef("reset", fn (u32) u32);
const Lower = workflow.UpdateDef("lower", fn () void);
const Note = workflow.SignalDef("note", fn ([]const u8) void);

/// Declares described handlers, and registers more in `run`: a `__stack_trace` handler, a
/// signal channel, a signal by name and a dynamic query handler.
const Described = struct {
    total: u32 = 0,

    pub const queries = .{
        .status = .{ Status, .{ .description = "How far the run got" } },
        .count = .{ Count, workflow.QueryHandlerOptions{ .description = "The total" } },
    };
    pub const signals = .{ .add = .{ Add, .{ .description = "Adds to the total", .unfinished_policy = .abandon } } };
    pub const updates = .{
        .reset = .{ Reset, .checkReset, .{ .description = "Sets the total" } },
        .lower = Lower,
    };

    pub fn run(d: *Described, ctx: *workflow.Context) !void {
        try ctx.setQueryHandlerWithOptions(OwnStackTrace, &own_context, ownTrace, .{ .description = "The workflow's own trace" });
        _ = try ctx.signalChannel(Note);
        const name = try std.fmt.allocPrint(ctx.allocator(), "by-name-{d}", .{d.total});
        try ctx.setSignalHandlerByNameWithOptions(name, @Tuple(&.{}), d, byName, .{ .description = "A signal named at run time \xff" });
        try ctx.setDynamicQueryHandler(&own_context, AnyQuery.answer);
        try ctx.io().sleep(.fromSeconds(60), .awake);
    }
    fn ownTrace(_: *const u8) []const u8 {
        return "own";
    }
    fn byName(_: *Described) void {}
    pub fn status(_: *Described) []const u8 {
        return "running";
    }
    pub fn count(d: *Described) u32 {
        return d.total;
    }
    pub fn add(d: *Described, n: u32) void {
        d.total += n;
    }
    pub fn reset(d: *Described, to: u32) u32 {
        d.total = to;
        return to;
    }
    pub fn checkReset(_: *Described, _: u32) void {}
    pub fn lower(_: *Described) void {}
};
const DescribedWf = workflow.Def("Described", Described);

test "__temporal_workflow_metadata lists every handler a struct workflow declares and registers, with its description, sorted by name" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try Instance.create(testing.allocator, DescribedWf.registered, .{});
    defer inst.destroy();
    _ = try inst.activate(&try initActivation(a, "Described"), a);

    const allocated = inst.allocated;
    const def = (try queryAnswer(workflow.WorkflowMetadataQuery, inst, a)).definition.?;
    try testing.expectEqual(allocated, inst.allocated);
    try testing.expectEqualStrings("Described", def.type);
    try expectDefinitions(def.query_definitions, &.{
        builtin_enhanced,
        .{ "__stack_trace", "The workflow's own trace" },
        builtin_metadata,
        .{ "count", "The total" },
        .{ "status", "How far the run got" },
    });
    try expectDefinitions(def.signal_definitions, &.{
        .{ "add", "Adds to the total" },
        .{ "by-name-0", "A signal named at run time \u{fffd}" },
        .{ "note", "" },
    });
    try expectDefinitions(def.update_definitions, &.{ .{ "lower", "" }, .{ "reset", "Sets the total" } });
    // The signal's other option still applies.
    try testing.expectEqual(workflow.HandlerUnfinishedPolicy.abandon, inst.signal_handlers.get("add").?.unfinished_policy);
    // The workflow's own `__stack_trace` handler answers it; the dynamic handler gets none of
    // the three.
    try testing.expectEqualStrings("own", try queryAnswer(OwnStackTrace, inst, a));
    try testing.expectEqual(@as(usize, 1), (try queryAnswer(workflow.EnhancedStackTraceQuery, inst, a)).stacks.len);
}

test "__enhanced_stack_trace and __temporal_workflow_metadata on a closed workflow" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try Instance.create(testing.allocator, Quick.registered, .{});
    defer inst.destroy();
    _ = commandsOf(try inst.activate(&try initActivation(a, "Quick"), a))[0].variant.?.complete_workflow_execution;
    const trace = try queryAnswer(workflow.EnhancedStackTraceQuery, inst, a);
    try testing.expectEqualStrings("temporal-zig", trace.sdk.name);
    try testing.expectEqual(@as(usize, 0), trace.stacks.len);
    try testing.expectEqual(@as(usize, 0), trace.sources.map.count());
    const def = (try queryAnswer(workflow.WorkflowMetadataQuery, inst, a)).definition.?;
    try testing.expectEqualStrings("Quick", def.type);
    try expectDefinitions(def.query_definitions, &.{ builtin_enhanced, builtin_stack_trace, builtin_metadata });
}

test "__enhanced_stack_trace's JSON has the field names Temporal's UI reads" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const inst = try Instance.create(testing.allocator, WaitsWf.registered, .{});
    defer inst.destroy();
    try startWaits(inst, a, false);
    var act: act_pb.WorkflowActivation = .{ .run_id = "run-1", .timestamp = .{ .seconds = 1_700_000_999 } };
    try act.jobs.append(a, .{ .variant = .{ .query_workflow = .{ .query_id = "q1", .query_type = "__enhanced_stack_trace" } } });
    const payload = commandsOf(try inst.activate(&act, a))[0].variant.?.respond_to_query.variant.?.succeeded.response.?;
    try testing.expectEqualStrings("json/plain", converter.encodingOf(payload).?);
    const parsed = try std.json.parseFromSliceLeaky(std.json.Value, a, payload.data, .{});
    const root = parsed.object;
    try testing.expectEqualStrings("temporal-zig", root.get("sdk").?.object.get("name").?.string);
    const stacks = root.get("stacks").?.array.items;
    try testing.expectEqual(@as(usize, 3), stacks.len);
    try testing.expect(root.get("sources").? == .object);
    for (stacks) |stack| for (stack.object.get("locations").?.array.items) |loc| {
        for ([_][]const u8{ "file_path", "line", "column", "function_name", "internal_code" }) |field| try testing.expect(loc.object.get(field) != null);
        const slices = root.get("sources").?.object.get(loc.object.get("file_path").?.string) orelse continue;
        for (slices.array.items) |slice| {
            try testing.expect(slice.object.get("line_offset").? == .integer);
            try testing.expect(slice.object.get("content").? == .string);
        }
    };
}

/// Asks for both answers through `next.handleQuery` from inside workflow code, before the
/// workflow function runs.
const SelfQueries = struct {
    arena: Allocator,
    trace: ?anyerror!converter.Payload = null,
    metadata: ?anyerror!converter.Payload = null,

    pub fn interceptWorkflow(s: *SelfQueries, _: *workflow.Context) error{OutOfMemory}!interceptor.Workflow {
        return .{ .inbound = interceptor.WorkflowInbound.from(SelfQueries, s) };
    }

    pub fn executeWorkflow(s: *SelfQueries, input: interceptor.ExecuteWorkflowInput, next: interceptor.WorkflowInbound.Next) anyerror!converter.Payload {
        s.trace = next.handleQuery(.{ .query_type = "__enhanced_stack_trace", .args = .{ .payloads = &.{} }, .header = .{}, .arena = s.arena });
        s.metadata = next.handleQuery(.{ .query_type = "__temporal_workflow_metadata", .args = .{ .payloads = &.{} }, .header = .{}, .arena = s.arena });
        return next.executeWorkflow(input);
    }
};

test "__enhanced_stack_trace and __temporal_workflow_metadata asked for from inside workflow code by an interceptor answer, and leave sdk_work clear" {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    var self_queries: SelfQueries = .{ .arena = a };
    const interceptors = [_]interceptor.Worker{.from(SelfQueries, &self_queries)};
    const inst = try Instance.create(testing.allocator, WaitsWf.registered, .{ .interceptors = &interceptors });
    defer inst.destroy();
    const started = commandsOf(try inst.activate(&try initActivation(a, "Waits"), a));
    try testing.expectEqual(@as(usize, 2), started.len);
    const trace = try converter.fromPayload(workflow.EnhancedStackTrace, a, try self_queries.trace.?);
    try testing.expectEqualStrings("temporal-zig", trace.sdk.name);
    // Only the calling coroutine is live, and it is running, not blocked.
    try testing.expectEqual(@as(usize, 1), trace.stacks.len);
    const metadata = try converter.fromPayload(proto.temporal.api.sdk.v1.WorkflowMetadata, a, try self_queries.metadata.?);
    try testing.expectEqualStrings("Waits", metadata.definition.?.type);
    try testing.expect(!inst.sched.sdk_work.load(.acquire));
}
