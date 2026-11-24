//! A start option pins a new run to a version other than the current one: with 2.0
//! current, a run started pinned to 1.0 returns version 1.0's result.
const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");
const workflow = temporal.workflow;

const deployment_name = "conformance-routing-with-override";

const WaitForSignal = workflow.DefWithOptions("WaitForSignal", waitForSignalV1, .{ .versioning_behavior = .pinned });
const WaitForSignalV2 = workflow.DefWithOptions("WaitForSignal", waitForSignalV2, .{ .versioning_behavior = .auto_upgrade });
const StartSignal = workflow.SignalDef("start-signal", fn ([]const u8) void);

pub const feature = harness.define(.{
    .workflows = .{WaitForSignal},
    .worker_options = temporal.worker.Options{ .deployment = .{ .deployment_name = deployment_name, .build_id = "1.0" } },
    .execute = execute,
});

const SignalState = struct {
    value: ?[]const u8 = null,

    fn receive(state: *SignalState, value: []const u8) void {
        state.value = value;
    }

    fn received(state: *SignalState) bool {
        return state.value != null;
    }
};

fn waitForSignal(ctx: *workflow.Context, comptime suffix: []const u8) ![]const u8 {
    const state = try ctx.allocator().create(SignalState);
    state.* = .{};
    try ctx.setSignalHandler(StartSignal, state, SignalState.receive);
    try ctx.await(state, SignalState.received);
    return std.fmt.allocPrint(ctx.allocator(), "{s}" ++ suffix, .{state.value.?});
}

fn waitForSignalV1(ctx: *workflow.Context) ![]const u8 {
    return waitForSignal(ctx, "_v1");
}

fn waitForSignalV2(ctx: *workflow.Context) ![]const u8 {
    return waitForSignal(ctx, "_v2");
}

fn execute(ctx: *harness.Context) !void {
    const v2 = try harness.SideWorker.start(ctx, .{ .deployment = .{
        .deployment_name = deployment_name,
        .build_id = "2.0",
        .default_versioning_behavior = .auto_upgrade,
    } }, .{ .workflows = .{WaitForSignalV2} });
    defer v2.stop();

    try harness.deployments.setCurrent(ctx, deployment_name, "2.0");
    var options = ctx.startOptions();
    options.versioning_override = .{ .pinned = .{ .deployment_name = deployment_name, .build_id = "1.0" } };
    var run = try ctx.client.executeWorkflow(WaitForSignal, options, .{});
    defer run.deinit();
    try harness.deployments.waitForWorkflowRunning(ctx, run.id, run.run_id);
    try ctx.client.signalWorkflow(StartSignal, run.id, run.run_id, .{"prefix"});
    try ctx.expectEqualStrings("prefix_v1", try run.get(ctx.arena));
}
