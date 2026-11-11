//! Port of samples-go/goroutine: process several sequences of activities in parallel. Go's
//! `workflow.Go` coroutines become an `std.Io.Group` on the workflow's Io, and the workflow
//! waits for them with `ctx.await` (Go `workflow.Await`).

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;
const activity = temporal.activity;

const State = struct {
    results: std.ArrayList([]const u8) = .empty,
    err: ?anyerror = null,
    parallelism: usize,

    fn done(s: *const State) bool {
        return s.err != null or s.results.items.len == s.parallelism;
    }
};

fn sequence(ctx: *workflow.Context, state: *State, input1: []const u8) void {
    const ao: workflow.ActivityOptions = .{ .start_to_close_timeout = .fromSeconds(10) };
    const result1 = ctx.executeActivity(Step1, ao, .{input1}).get() catch |err| {
        state.err = err;
        return;
    };
    const result2 = ctx.executeActivity(Step2, ao, .{result1}).get() catch |err| {
        state.err = err;
        return;
    };
    state.results.append(ctx.allocator(), result2) catch |err| {
        state.err = err;
    };
}

fn sampleGoroutineWorkflow(ctx: *workflow.Context, parallelism: i64) ![]const []const u8 {
    var state: State = .{ .parallelism = @intCast(@max(parallelism, 0)) };
    var group: std.Io.Group = .init;
    defer group.cancel(ctx.io());
    for (0..state.parallelism) |i| {
        const input1 = try std.fmt.allocPrint(ctx.allocator(), "{d}", .{i});
        group.async(ctx.io(), sequence, .{ ctx, &state, input1 });
    }

    try ctx.await(&state, State.done);
    if (state.err) |err| return err;
    return state.results.items;
}

pub const SampleGoroutineWorkflow = workflow.Def("SampleGoroutineWorkflow", sampleGoroutineWorkflow);

fn randomSleep(ctx: *activity.Context) !void {
    const source: std.Random.IoSource = .{ .io = ctx.io() };
    try ctx.io().sleep(.fromSeconds(source.interface().uintLessThan(u32, 10)), .awake);
}

fn step1(ctx: *activity.Context, input: []const u8) ![]const u8 {
    try randomSleep(ctx);
    return std.fmt.allocPrint(ctx.allocator(), "{s}, Step1", .{input});
}

fn step2(ctx: *activity.Context, input: []const u8) ![]const u8 {
    try randomSleep(ctx);
    return std.fmt.allocPrint(ctx.allocator(), "{s}, Step2", .{input});
}

pub const Step1 = activity.Def("Step1", step1);
pub const Step2 = activity.Def("Step2", step2);
