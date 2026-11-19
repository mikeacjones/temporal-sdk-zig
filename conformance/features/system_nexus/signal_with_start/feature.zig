const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");
const workflow = temporal.workflow;

const Target = workflow.Def("TargetWorkflow", runTarget);
const Caller = workflow.Def("CallerWorkflow", runCaller);
const Add = workflow.SignalDef("add", fn ([]const u8) void);

pub const feature = harness.define(.{
    .workflows = .{ Target, Caller },
    .execute = execute,
});

const TargetState = struct {
    allocator: std.mem.Allocator,
    signals: std.ArrayList([]const u8) = .empty,

    fn add(state: *TargetState, value: []const u8) !void {
        try state.signals.append(state.allocator, try std.fmt.allocPrint(state.allocator, "signal: {s}", .{value}));
    }

    fn done(state: *TargetState) bool {
        return state.signals.items.len == 2;
    }
};

fn runTarget(ctx: *workflow.Context, value: []const u8) ![]const []const u8 {
    const a = ctx.allocator();
    const state = try a.create(TargetState);
    state.* = .{ .allocator = a };
    try ctx.setSignalHandler(Add, state, TargetState.add);
    try ctx.await(state, TargetState.done);
    const result = try a.alloc([]const u8, 3);
    result[0] = try std.fmt.allocPrint(a, "started: {s}", .{value});
    result[1] = state.signals.items[0];
    result[2] = state.signals.items[1];
    return result;
}

fn runCaller(ctx: *workflow.Context, target_id: []const u8, task_queue: []const u8) ![2][]const u8 {
    const first = try ctx.signalWithStartWorkflow(Target, .{
        .id = target_id,
        .task_queue = task_queue,
        .execution_timeout = .fromSeconds(60),
        .id_conflict_policy = .use_existing,
    }, .{"start-value"}, Add, .{"signal-one"}).get();
    const second = try ctx.signalWithStartWorkflow(Target, .{
        .id = target_id,
        .task_queue = task_queue,
        .id_conflict_policy = .use_existing,
    }, .{"unused-start-value"}, Add, .{"signal-two"}).get();

    if (first.run_id.len == 0) return ctx.fail(.{ .message = "first SignalWithStart returned no run ID", .non_retryable = true });
    if (second.run_id.len == 0) return ctx.fail(.{ .message = "second SignalWithStart returned no run ID", .non_retryable = true });
    if (!std.mem.eql(u8, first.run_id, second.run_id)) {
        const message = try std.fmt.allocPrint(ctx.allocator(), "SignalWithStart calls targeted different runs: first='{s}' second='{s}'", .{ first.run_id, second.run_id });
        return ctx.fail(.{ .message = message, .non_retryable = true });
    }
    return .{ target_id, first.run_id };
}

fn execute(ctx: *harness.Context) !void {
    const target_id = try std.fmt.allocPrint(ctx.arena, "system-nexus-signal-with-start-{s}", .{ctx.task_queue});
    var run = try ctx.client.executeWorkflow(Caller, ctx.startOptions(), .{ target_id, ctx.task_queue });
    defer run.deinit();
    const caller = run.get(ctx.arena) catch |err| {
        if (run.failure()) |failure| return ctx.fail("caller workflow failed: {s}", .{failure.message});
        return err;
    };
    try ctx.expectEqualStrings(target_id, caller[0]);

    var target = try ctx.client.getWorkflow(Target.Result, caller[0], caller[1]);
    defer target.deinit();
    const result = target.get(ctx.arena) catch |err| {
        if (target.failure()) |failure| return ctx.fail("target workflow failed: {s}", .{failure.message});
        return err;
    };
    const expected = [_][]const u8{ "started: start-value", "signal: signal-one", "signal: signal-two" };
    try ctx.expect(result.len == expected.len, "target returned {d} strings, want {d}", .{ result.len, expected.len });
    for (expected, result) |want, got| try ctx.expectEqualStrings(want, got);
}
