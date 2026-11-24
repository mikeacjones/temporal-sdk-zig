//! Zig has no panic a workflow recovers from, so Go's two panics become what they stand
//! for. The validator's panic is a rejection: it fails with `workflow.Context.fail` and
//! the panic's message. The update handler's panic fails the workflow task: it returns
//! `error.IntentionalPanic`, listed in the workflow's `task_failure_errors`, and the
//! server retries the task, where the handler then succeeds.
const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");
const workflow = temporal.workflow;

const PanickyUpdates = workflow.DefWithOptions("PanickyUpdates", runWorkflow, .{
    .task_failure_errors = &.{error.IntentionalPanic},
});
const PanicFromExec = workflow.UpdateDef("panic_from_exec", fn () void);
const PanicFromValidate = workflow.UpdateDef("panic_from_validate", fn () void);
const Shutdown = workflow.SignalDef("shutdown!", fn () void);
const panic_msg = "fear not, dear examiner of logs, for this panic was intentional";

pub const feature = harness.define(.{
    .workflows = .{PanickyUpdates},
    .execute = execute,
});

/// Whether the next `panic_from_exec` update fails its workflow task. Not part of the
/// workflow's state, which a real workflow must never do: `execute` arms it so the update
/// fails one task and succeeds when the task is retried. It stays false when
/// `zig build test` replays the stored history.
var fail_exec_once: std.atomic.Value(bool) = .init(false);

const State = struct {
    ctx: *workflow.Context,
    shutdown: bool = false,

    fn exec(_: *State) error{IntentionalPanic}!void {
        if (fail_exec_once.swap(false, .acq_rel)) return error.IntentionalPanic;
    }

    fn noop(_: *State) void {}

    fn validatePanics(state: *State) error{ApplicationFailure}!void {
        return state.ctx.fail(.{ .message = panic_msg });
    }

    fn stop(state: *State) void {
        state.shutdown = true;
    }

    fn stopped(state: *State) bool {
        return state.shutdown;
    }
};

fn runWorkflow(ctx: *workflow.Context) !void {
    const state = try ctx.allocator().create(State);
    state.* = .{ .ctx = ctx };
    try ctx.setUpdateHandler(PanicFromExec, state, State.exec);
    try ctx.setUpdateHandlerWithValidator(PanicFromValidate, state, State.noop, State.validatePanics);
    try ctx.setSignalHandler(Shutdown, state, State.stop);
    try ctx.await(state, State.stopped);
}

fn execute(ctx: *harness.Context) !void {
    var options = ctx.startOptions();
    // A shorter task timeout makes the failed task's retry come sooner.
    options.task_timeout = .fromSeconds(3);
    var run = try ctx.client.executeWorkflow(PanickyUpdates, options, .{});
    defer run.deinit();

    var rejected = try ctx.client.updateWorkflow(PanicFromValidate, run.id, run.run_id, .{}, .{});
    defer rejected.deinit();
    if (rejected.get(ctx.arena)) |_| {
        return ctx.fail("expected the failing validator to reject panic_from_validate, the update completed", .{});
    } else |err| switch (err) {
        error.UpdateFailed => {
            const failure = rejected.failure() orelse return ctx.fail("rejected update has no failure details", .{});
            try ctx.expect(std.mem.indexOf(u8, failure.message, panic_msg) != null, "expected the rejection to contain \"{s}\", got \"{s}\"", .{ panic_msg, failure.message });
        },
        else => return err,
    }

    fail_exec_once.store(true, .release);
    var failing = try ctx.client.updateWorkflow(PanicFromExec, run.id, run.run_id, .{}, .{});
    defer failing.deinit();
    failing.get(ctx.arena) catch |err| switch (err) {
        error.UpdateFailed => return ctx.fail("expected panic_from_exec to complete once its task was retried, it failed: {s}", .{if (failing.failure()) |f| f.message else "no failure details"}),
        else => return err,
    };
    try ctx.expect(!fail_exec_once.load(.acquire), "panic_from_exec completed without running its handler's failing path", .{});

    try ctx.client.signalWorkflow(Shutdown, run.id, run.run_id, .{});
    try run.get(ctx.arena);

    const recorded = try ctx.fetchHistory(run.id, run.run_id);
    try ctx.expectNoEvent(recorded, .EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_REJECTED);
    var task_failures: usize = 0;
    for (recorded.events.items) |event| {
        const attributes = event.attributes orelse continue;
        switch (attributes) {
            .workflow_task_failed_event_attributes => |failed| {
                const message = if (failed.failure) |f| f.message else "";
                if (failed.cause == .WORKFLOW_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE and
                    std.mem.indexOf(u8, message, "IntentionalPanic") != null) task_failures += 1;
            },
            else => {},
        }
    }
    try ctx.expect(task_failures == 1, "expected the update handler's error to fail 1 workflow task, found {d} such task failures", .{task_failures});
}
