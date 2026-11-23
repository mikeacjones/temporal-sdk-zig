//! Converter contexts against a dev server: a type whose context hooks stamp each value with
//! the context they were given shows what the client, the workflow, its activities, local
//! activities, children, signals and updates convert with, and that replay converts the same.

const std = @import("std");
const activity = @import("activity.zig");
const workflow = @import("workflow.zig");
const converter = @import("converter.zig");
const testing_mod = @import("testing.zig");
const WorkflowEnvironment = testing_mod.WorkflowEnvironment;
const Allocator = std.mem.Allocator;

const testing = std.testing;

/// Every hook call, as "enc ..." or "dec ...", for checking what a replay converts. Hooks
/// run on several threads, so slots are claimed atomically; nothing reads them while a
/// worker runs.
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

/// Text that collects a stamp from every hook it passes through: encoding appends
/// "enc <context>", decoding "dec <context>".
const Traced = struct {
    text: []const u8,
    trail: []const []const u8 = &.{},

    const Wire = struct { text: []const u8, trail: []const []const u8 };

    fn stamp(arena: Allocator, what: []const u8, c: *const converter.Context) error{OutOfMemory}![]const u8 {
        const line = try std.fmt.allocPrint(arena, "{s} {s}|{s}|{s}|{s}|{s}", .{ what, c.namespace, c.workflow_id, c.workflow_type, c.activity_type, c.activity_id });
        Recorder.record(line);
        return line;
    }

    fn with(arena: Allocator, v: Traced, line: []const u8) error{OutOfMemory}!Traced {
        return .{ .text = v.text, .trail = try std.mem.concat(arena, []const u8, &.{ v.trail, &.{line} }) };
    }

    pub fn toPayloadWithContext(v: Traced, arena: Allocator, context: *const converter.Context) error{OutOfMemory}!converter.Payload {
        const stamped = try with(arena, v, try stamp(arena, "enc", context));
        return converter.toPayload(arena, Wire{ .text = stamped.text, .trail = stamped.trail });
    }

    pub fn fromPayloadWithContext(arena: Allocator, context: *const converter.Context, p: converter.Payload) converter.DecodeError!Traced {
        const wire = try converter.fromPayload(Wire, arena, p);
        return with(arena, .{ .text = wire.text, .trail = wire.trail }, try stamp(arena, "dec", context));
    }
};

const Fixtures = struct {
    /// Echoes its input with a stamp of the activity's own converter context.
    fn echo(ctx: *activity.Context, input: Traced) !Traced {
        return Traced.with(ctx.allocator(), input, try Traced.stamp(ctx.allocator(), "act", ctx.converterContext()));
    }
    const Echo = activity.Def("TracedEcho", echo);

    fn child(_: *workflow.Context, input: Traced) !Traced {
        return input;
    }
    const Child = workflow.Def("TracedChild", child);

    const More = workflow.SignalDef("more", fn (Traced) void);
    const Tag = workflow.UpdateDef("tag", fn (Traced) Traced);

    fn tag(_: *workflow.Context, input: Traced) Traced {
        return input;
    }

    /// Runs `input` through an activity, the signal's value through a local activity, and
    /// `input` again through a child, then returns the three trails joined, after a stamp
    /// of each future's converter context.
    fn flow(ctx: *workflow.Context, input: Traced) !Traced {
        const a = ctx.allocator();
        try ctx.setUpdateHandler(Tag, ctx, tag);
        const more = try (try ctx.signalChannel(More)).receive();
        const remote_future = ctx.executeActivity(Echo, .{ .start_to_close_timeout = .fromSeconds(10) }, .{input});
        const remote = try remote_future.get();
        const local_future = ctx.executeLocalActivity(Echo, .{ .start_to_close_timeout = .fromSeconds(10) }, .{more});
        const local = try local_future.get();
        const child_future = ctx.executeChildWorkflow(Child, .{ .id = "converter-context-child" }, .{input});
        const kid = try child_future.get();
        const futures: []const []const u8 = &.{
            try Traced.stamp(a, "future", remote_future.converterContext()),
            try Traced.stamp(a, "future", local_future.converterContext()),
            try Traced.stamp(a, "future", child_future.converterContext()),
            try Traced.stamp(a, "run", ctx.converterContext()),
        };
        return .{
            .text = try std.mem.join(a, "+", &.{ remote.text, local.text, kid.text }),
            .trail = try std.mem.concat(a, []const u8, &.{ remote.trail, &.{"--"}, local.trail, &.{"--"}, kid.trail, &.{"--"}, futures }),
        };
    }
    const Flow = workflow.Def("TracedFlow", flow);
};

const workflow_id = "converter-context-flow";
const run_ctx = "default|converter-context-flow|TracedFlow||";
const activity_ctx = "default|converter-context-flow|TracedFlow|TracedEcho|1";
const local_ctx = "default|converter-context-flow|TracedFlow|TracedEcho|2";
const child_ctx = "default|converter-context-child|TracedChild||";

test "context hooks see the client's, the workflow's, and each activity's and child's context, and replay converts the same" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(Fixtures.Flow);
    try env.registerWorkflow(Fixtures.Child);
    try env.registerActivity(Fixtures.Echo);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var run = try env.startWorkflow(Fixtures.Flow, .{ .id = workflow_id, .task_queue = "" }, .{.{ .text = "in" }});
    defer run.deinit();
    var update = try env.client.updateWorkflow(Fixtures.Tag, run.id, "", .{.{ .text = "up" }}, .{});
    defer update.deinit();
    const tagged = try update.get(a);
    try testing.expectEqualStrings("up", tagged.text);
    try expectTrail(&.{
        "enc default|converter-context-flow|||",
        "dec " ++ run_ctx,
        "enc " ++ run_ctx,
        "dec default|converter-context-flow|||",
    }, tagged.trail);
    try env.client.signalWorkflow(Fixtures.More, run.id, "", .{.{ .text = "more" }});

    Recorder.reset();
    const result = try env.getResult(Fixtures.Flow, &run, a, null);
    try testing.expectEqualStrings("in+more+in", result.text);
    try expectTrail(&.{
        // The start's argument, read by the workflow and sent to the activity.
        "enc default|converter-context-flow|TracedFlow||",
        "dec " ++ run_ctx,
        "enc " ++ activity_ctx,
        "dec " ++ activity_ctx,
        "act " ++ activity_ctx,
        "enc " ++ activity_ctx,
        "dec " ++ activity_ctx,
        "--",
        // The signal's argument, sent to the local activity.
        "enc default|converter-context-flow|||",
        "dec " ++ run_ctx,
        "enc " ++ local_ctx,
        "dec " ++ local_ctx,
        "act " ++ local_ctx,
        "enc " ++ local_ctx,
        "dec " ++ local_ctx,
        "--",
        // The start's argument again, through the child.
        "enc default|converter-context-flow|TracedFlow||",
        "dec " ++ run_ctx,
        "enc " ++ child_ctx,
        "dec " ++ child_ctx,
        "enc " ++ child_ctx,
        "dec " ++ child_ctx,
        "--",
        "future " ++ activity_ctx,
        "future " ++ local_ctx,
        "future " ++ child_ctx,
        "run " ++ run_ctx,
        // The result, sent by the workflow and read by the client.
        "enc " ++ run_ctx,
        "dec default|converter-context-flow|||",
    }, result.trail);

    // Replay converts the workflow's values with the same contexts it did live.
    Recorder.reset();
    try env.replayRun(run.id, run.run_id);
    const expected = [_][]const u8{
        "dec " ++ run_ctx, // the start's argument
        "dec " ++ run_ctx, // the update's argument
        "enc " ++ run_ctx, // the update's result
        "dec " ++ run_ctx, // the signal's argument
        "enc " ++ activity_ctx,
        "dec " ++ activity_ctx,
        "enc " ++ local_ctx,
        "dec " ++ local_ctx,
        "enc " ++ child_ctx,
        "dec " ++ child_ctx,
        "future " ++ activity_ctx,
        "future " ++ local_ctx,
        "future " ++ child_ctx,
        "run " ++ run_ctx,
        "enc " ++ run_ctx, // the workflow's result
    };
    const want = try a.dupe([]const u8, &expected);
    std.mem.sort([]const u8, want, {}, Recorder.lessThan);
    try expectTrail(want, try Recorder.sorted(a));
}

fn expectTrail(expected: []const []const u8, actual: []const []const u8) !void {
    const want = try std.mem.join(testing.allocator, "\n", expected);
    defer testing.allocator.free(want);
    const got = try std.mem.join(testing.allocator, "\n", actual);
    defer testing.allocator.free(got);
    try testing.expectEqualStrings(want, got);
}

const CronFixtures = struct {
    fn cron(_: *workflow.Context) !Traced {
        return .{ .text = "cron" };
    }
    const Cron = workflow.Def("TracedCron", cron);
};

test "the environment decodes a cron run's own result with the client's context, as WorkflowRun.get does" {
    const env = try WorkflowEnvironment.startTimeSkipping(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(CronFixtures.Cron);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var cron = try env.startWorkflow(CronFixtures.Cron, .{ .id = "converter-context-cron", .task_queue = "", .cron_schedule = "0 * * * *" }, .{});
    defer cron.deinit();
    defer env.client.terminateWorkflow(cron.id, "", "test cleanup") catch {};
    // Without following runs, the environment reads the run's own result from its replay.
    const result = try env.getResultWithOptions(CronFixtures.Cron, &cron, .{ .follow_runs = false }, a, null);
    try testing.expectEqualStrings("cron", result.text);
    try expectTrail(&.{
        "enc default|converter-context-cron|TracedCron||",
        "dec default|converter-context-cron|||",
    }, result.trail);
}

/// A pass-through codec that records, for each payload whose data ends in "-arg" or
/// "-arg-done", "codec enc|dec <context> <data>" with the context the worker or client
/// gave it.
const RecordingCodec = struct {
    fn codec() converter.PayloadCodec {
        return .{ .ptr = undefined, .vtable = &.{ .encode = encode, .decode = decode, .encodeWithContext = encodeWithContext, .decodeWithContext = decodeWithContext } };
    }

    fn encode(ptr: *anyopaque, arena: Allocator, io: std.Io, payloads: []const converter.Payload) converter.CodecError![]converter.Payload {
        return encodeWithContext(ptr, arena, io, &.{}, payloads);
    }

    fn decode(ptr: *anyopaque, arena: Allocator, io: std.Io, payloads: []const converter.Payload) converter.CodecError![]converter.Payload {
        return decodeWithContext(ptr, arena, io, &.{}, payloads);
    }

    fn encodeWithContext(_: *anyopaque, arena: Allocator, _: std.Io, context: *const converter.Context, payloads: []const converter.Payload) converter.CodecError![]converter.Payload {
        try note(arena, "enc", context, payloads);
        return arena.dupe(converter.Payload, payloads);
    }

    fn decodeWithContext(_: *anyopaque, arena: Allocator, _: std.Io, context: *const converter.Context, payloads: []const converter.Payload) converter.CodecError![]converter.Payload {
        try note(arena, "dec", context, payloads);
        return arena.dupe(converter.Payload, payloads);
    }

    fn note(arena: Allocator, what: []const u8, c: *const converter.Context, payloads: []const converter.Payload) error{OutOfMemory}!void {
        for (payloads) |p| {
            if (std.mem.indexOf(u8, p.data, "-arg") == null) continue;
            Recorder.record(try std.fmt.allocPrint(arena, "codec {s} {s}|{s}|{s}|{s}|{s} {s}", .{ what, c.namespace, c.workflow_id, c.workflow_type, c.activity_type, c.activity_id, p.data }));
        }
    }
};

const CodecFixtures = struct {
    fn echo(ctx: *activity.Context, input: []const u8) ![]const u8 {
        return std.fmt.allocPrint(ctx.allocator(), "{s}-done", .{input});
    }
    const Echo = activity.Def("CodecEcho", echo);

    fn child(ctx: *workflow.Context, input: []const u8) ![]const u8 {
        return std.fmt.allocPrint(ctx.allocator(), "{s}-done", .{input});
    }
    const Child = workflow.Def("CodecChild", child);

    fn flow(ctx: *workflow.Context) ![]const u8 {
        const a = ctx.allocator();
        _ = try ctx.executeActivity(Echo, .{ .start_to_close_timeout = .fromSeconds(10) }, .{"remote-arg"}).get();
        _ = try ctx.executeLocalActivity(Echo, .{ .start_to_close_timeout = .fromSeconds(10) }, .{"local-arg"}).get();
        _ = try ctx.executeChildWorkflow(Child, .{ .id = "converter-codec-child" }, .{"child-arg"}).get();
        return a.dupe(u8, "done");
    }
    const Flow = workflow.Def("CodecFlow", flow);
};

test "codecs see an activity's, local activity's and child's own context for its argument and result on both workers" {
    const codecs = [_]converter.PayloadCodec{RecordingCodec.codec()};
    const env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{ .codecs = &codecs });
    defer env.deinit();
    try env.registerWorkflow(CodecFixtures.Flow);
    try env.registerWorkflow(CodecFixtures.Child);
    try env.registerActivity(CodecFixtures.Echo);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    Recorder.reset();
    var run = try env.startWorkflow(CodecFixtures.Flow, .{ .id = "converter-codec-flow", .task_queue = "" }, .{});
    defer run.deinit();
    // Waiting for the result also replays the history with the codecs.
    try testing.expectEqualStrings("done", try env.getResult(CodecFixtures.Flow, &run, a, null));

    const flow_ctx = "default|converter-codec-flow|CodecFlow";
    const Expected = struct { data: []const u8, context: []const u8, enc: bool = false, dec: bool = false };
    var expected = [_]Expected{
        .{ .data = "\"remote-arg\"", .context = flow_ctx ++ "|CodecEcho|1" },
        .{ .data = "\"remote-arg-done\"", .context = flow_ctx ++ "|CodecEcho|1" },
        .{ .data = "\"local-arg\"", .context = flow_ctx ++ "|CodecEcho|2" },
        .{ .data = "\"local-arg-done\"", .context = flow_ctx ++ "|CodecEcho|2" },
        .{ .data = "\"child-arg\"", .context = "default|converter-codec-child|CodecChild||" },
        .{ .data = "\"child-arg-done\"", .context = "default|converter-codec-child|CodecChild||" },
    };
    const lines = try Recorder.sorted(a);
    try testing.expect(lines.len < Recorder.lines.len);
    for (lines) |line| {
        var parts = std.mem.splitScalar(u8, line, ' ');
        try testing.expectEqualStrings("codec", parts.next().?);
        const what = parts.next().?;
        const context = parts.next().?;
        const data = parts.rest();
        // A client call names no workflow type: here, reading the parent's history, which
        // holds every one of these values. Only the workers' coding is checked.
        var fields = std.mem.splitScalar(u8, context, '|');
        _ = fields.next();
        _ = fields.next();
        if (fields.next().?.len == 0) continue;
        const e = for (&expected) |*e| {
            if (std.mem.eql(u8, e.data, data)) break e;
        } else return error.TestUnexpectedResult;
        // Every encode and decode of the value by a worker, the workflow's, the activity's or
        // the child's, live or replaying, has the call's own context.
        testing.expectEqualStrings(e.context, context) catch |err| {
            std.debug.print("codec {s} of {s}\n", .{ what, data });
            return err;
        };
        if (std.mem.eql(u8, what, "enc")) e.enc = true else e.dec = true;
    }
    for (expected) |e| {
        testing.expect(e.enc and e.dec) catch |err| {
            std.debug.print("{s}: encoded {}, decoded {}\n", .{ e.data, e.enc, e.dec });
            return err;
        };
    }
}

const SideEffectFixtures = struct {
    fn value(_: void) []const u8 {
        return "side-effect-arg";
    }

    fn flow(ctx: *workflow.Context) ![]const u8 {
        return ctx.allocator().dupe(u8, try ctx.sideEffect([]const u8, {}, value));
    }
    const Flow = workflow.Def("CodecSideEffectFlow", flow);
};

test "codecs see the same context for a side effect's value on the workflow's worker as on the worker that records it" {
    const codecs = [_]converter.PayloadCodec{RecordingCodec.codec()};
    const env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{ .codecs = &codecs });
    defer env.deinit();
    try env.registerWorkflow(SideEffectFixtures.Flow);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    Recorder.reset();
    var run = try env.startWorkflow(SideEffectFixtures.Flow, .{ .id = "converter-codec-side-effect", .task_queue = "" }, .{});
    defer run.deinit();
    try testing.expectEqualStrings("side-effect-arg", try env.getResult(SideEffectFixtures.Flow, &run, a, null));

    // The value is encoded by the workflow's worker in the local activity's command, decoded
    // and encoded again by the worker that runs that local activity, and decoded by the
    // workflow's worker from its resolution: a codec keyed on its context must see one
    // context in all four.
    var seen: ?[]const u8 = null;
    var count: usize = 0;
    for (try Recorder.sorted(a)) |line| {
        var parts = std.mem.splitScalar(u8, line, ' ');
        _ = parts.next();
        const what = parts.next().?;
        const context = parts.next().?;
        const data = parts.rest();
        if (!std.mem.eql(u8, data, "\"side-effect-arg\"")) continue;
        // Client calls, which name no workflow type, are not the workers' coding.
        var fields = std.mem.splitScalar(u8, context, '|');
        _ = fields.next();
        _ = fields.next();
        if (fields.next().?.len == 0) continue;
        count += 1;
        if (seen) |s| testing.expectEqualStrings(s, context) catch |err| {
            std.debug.print("codec {s} of the side effect's value had context {s}, another had {s}\n", .{ what, context, s });
            return err;
        } else seen = context;
    }
    try testing.expect(count >= 4);
}

const CanceledChildFixtures = struct {
    const Finish = workflow.SignalDef("finish", fn () void);
    const Done = workflow.SignalDef("done", fn () void);

    /// Waits until canceled, then until `Finish`, and returns its input with "-done".
    fn child(ctx: *workflow.Context, input: []const u8) ![]const u8 {
        const finish = try ctx.signalChannel(Finish);
        ctx.io().sleep(.fromSeconds(24 * 60 * 60), .awake) catch |err| switch (err) {
            error.Canceled => {
                _ = try finish.receive();
                return std.fmt.allocPrint(ctx.allocator(), "{s}-done", .{input});
            },
        };
        return "not canceled";
    }
    const Child = workflow.Def("CodecCanceledChild", child);

    fn getChild(f: workflow.ChildWorkflowFuture([]const u8)) workflow.ChildWorkflowError![]const u8 {
        return f.get();
    }

    /// Starts `Child` under `wait_cancellation_requested`, cancels the wait for its result
    /// (which returns once the server recorded the request), then lets the child finish with
    /// a result and waits for `Done`.
    fn parent(ctx: *workflow.Context) ![]const u8 {
        const io = ctx.io();
        const f = ctx.executeChildWorkflow(Child, .{
            .id = "converter-codec-canceled-child",
            .cancellation_type = .wait_cancellation_requested,
            .parent_close_policy = .abandon,
        }, .{"canceled-child-arg"});
        _ = try f.started();
        var waiting = io.async(getChild, .{f});
        if (waiting.cancel(io)) |_| return error.ChildNotCanceled else |_| {}
        try (try f.signal(Finish, .{})).get();
        _ = try (try ctx.signalChannel(Done)).receive();
        return ctx.allocator().dupe(u8, "done");
    }
    const Parent = workflow.Def("CodecCanceledChildParent", parent);

    fn awaitEvent(c: anytype, id: []const u8, comptime tag: anytype) !void {
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

test "codecs see a child's own context for the result of a child whose cancellation the server recorded" {
    const codecs = [_]converter.PayloadCodec{RecordingCodec.codec()};
    const env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{ .codecs = &codecs });
    defer env.deinit();
    try env.registerWorkflow(CanceledChildFixtures.Parent);
    try env.registerWorkflow(CanceledChildFixtures.Child);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    Recorder.reset();
    var run = try env.startWorkflow(CanceledChildFixtures.Parent, .{ .id = "converter-codec-canceled-parent", .task_queue = "" }, .{});
    defer run.deinit();
    // The child's result reaches the parent after the parent saw its cancellation recorded.
    try CanceledChildFixtures.awaitEvent(&env.client, run.id, .child_workflow_execution_completed_event_attributes);
    try env.client.signalWorkflow(CanceledChildFixtures.Done, run.id, "", .{});
    try testing.expectEqualStrings("done", try env.getResult(CanceledChildFixtures.Parent, &run, a, null));

    // Every worker's coding of the child's result, by the child's worker and by the parent's
    // live and replaying, has the child's context.
    const child_ctx_ = "default|converter-codec-canceled-child|CodecCanceledChild||";
    var enc = false;
    var dec = false;
    for (try Recorder.sorted(a)) |line| {
        var parts = std.mem.splitScalar(u8, line, ' ');
        _ = parts.next();
        const what = parts.next().?;
        const context = parts.next().?;
        const data = parts.rest();
        if (!std.mem.eql(u8, data, "\"canceled-child-arg-done\"")) continue;
        // Client calls, which name no workflow type, are not the workers' coding.
        var fields = std.mem.splitScalar(u8, context, '|');
        _ = fields.next();
        _ = fields.next();
        if (fields.next().?.len == 0) continue;
        testing.expectEqualStrings(child_ctx_, context) catch |err| {
            std.debug.print("codec {s} of the canceled child's result\n", .{what});
            return err;
        };
        if (std.mem.eql(u8, what, "enc")) enc = true else dec = true;
    }
    try testing.expect(enc and dec);
}
