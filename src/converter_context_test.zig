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
