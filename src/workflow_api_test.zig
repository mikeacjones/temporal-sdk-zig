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
