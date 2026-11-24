//! A child workflow started with cancellation type and parent close policy `abandon` is left
//! alone when its parent is canceled. Three scenarios, as in the suite's PHP version:
//!
//! - main scope: the parent races the child's result against a 5 s timer; the client cancels
//!   the parent.
//! - inner scope: the parent starts and awaits the child in a task of its own, raced against
//!   the timer and a `close` signal; the client cancels the parent.
//! - closing inner scope: as before, but the `close` signal cancels the child's task instead.
//!
//! In each, waiting on the child ends at once as canceled and the parent returns "cancelled";
//! the child then receives no cancellation request, so the `exit` signal lets it complete
//! with its input. The PHP version's one-second timer before the parent closes, so that every
//! command of the canceling task reaches the server, is not needed here: the commands of the
//! last workflow task are sent with its completion, which the history checks then see.
const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");
const failure = temporal.failure;
const workflow = temporal.workflow;
const Io = std.Io;

const MainScopeWorkflow = workflow.Def("Harness_ChildWorkflow_CancelAbandon_MainScope", runMainScope);
const InnerScopeCancelWorkflow = workflow.Def("Harness_ChildWorkflow_CancelAbandon_InnerScopeCancel", runInnerScope);
const ChildWorkflow = workflow.Def("Harness_ChildWorkflow_CancelAbandon_Child", runChild);
const Close = workflow.SignalDef("close", fn () void);
const Exit = workflow.SignalDef("exit", fn () void);
const ChildFuture = workflow.ChildWorkflowFuture([]const u8);

const cancelled = "cancelled";
const child_cancelled = "child-cancelled";
const timer_fired = "timer";
const race_timeout: Io.Duration = .fromSeconds(5);

pub const feature = harness.define(.{
    .workflows = .{ MainScopeWorkflow, InnerScopeCancelWorkflow, ChildWorkflow },
    .execute = execute,
});

fn childOptions(ctx: *workflow.Context) error{OutOfMemory}!workflow.ChildWorkflowOptions {
    return .{
        .id = try std.fmt.allocPrint(ctx.allocator(), "{s}-child", .{ctx.info().workflow_id}),
        .run_timeout = .fromSeconds(20),
        .parent_close_policy = .abandon,
        .cancellation_type = .abandon,
    };
}

/// Whether the child's failure is its cancellation.
fn childWasCanceled(child: ChildFuture) bool {
    const top = child.failure() orelse return false;
    if (failure.kind(top) == .canceled) return true;
    const cause = top.cause orelse return false;
    return failure.kind(cause) == .canceled;
}

fn runMainScope(ctx: *workflow.Context, input: []const u8) ![]const u8 {
    const io = ctx.io();
    const child = ctx.executeChildWorkflow(ChildWorkflow, try childOptions(ctx), .{input});
    _ = child.started() catch |err| switch (err) {
        error.Canceled => return cancelled,
        error.ChildWorkflowFailed => return if (childWasCanceled(child)) cancelled else err,
        else => return err,
    };

    const Raced = union(enum) { child: workflow.ChildWorkflowError![]const u8, timer: Io.Cancelable!void };
    var buffer: [2]Raced = undefined;
    var select: Io.Select(Raced) = .init(io, &buffer);
    defer select.cancelDiscard();
    select.async(.child, ChildFuture.get, .{child});
    select.async(.timer, Io.sleep, .{ io, race_timeout, .awake });
    const raced = select.await() catch return cancelled;
    switch (raced) {
        .child => |result| _ = result catch |err| switch (err) {
            error.Canceled => return cancelled,
            error.ChildWorkflowFailed => return if (childWasCanceled(child)) cancelled else err,
            else => return err,
        },
        .timer => |result| result catch return cancelled,
    }
    return timer_fired;
}

const InnerState = struct {
    closed: bool = false,

    fn close(state: *InnerState) void {
        state.closed = true;
    }

    fn isClosed(state: *InnerState) bool {
        return state.closed;
    }
};

const ScopeOutcome = enum { completed, child_cancelled };

/// The inner scope: starts the child and waits for its result.
fn innerScope(ctx: *workflow.Context, input: []const u8) workflow.ChildWorkflowError!ScopeOutcome {
    const child = ctx.executeChildWorkflow(ChildWorkflow, try childOptions(ctx), .{input});
    _ = child.started() catch |err| return scopeFailed(child, err);
    _ = child.get() catch |err| return scopeFailed(child, err);
    return .completed;
}

fn scopeFailed(child: ChildFuture, err: workflow.ChildWorkflowError) workflow.ChildWorkflowError!ScopeOutcome {
    if (err == error.ChildWorkflowFailed and childWasCanceled(child)) return .child_cancelled;
    return err;
}

fn awaitClose(ctx: *workflow.Context, state: *InnerState) error{ Canceled, OutOfMemory }!void {
    return ctx.await(state, InnerState.isClosed);
}

fn runInnerScope(ctx: *workflow.Context, input: []const u8) ![]const u8 {
    const io = ctx.io();
    const state = try ctx.allocator().create(InnerState);
    state.* = .{};
    try ctx.setSignalHandler(Close, state, InnerState.close);

    const Raced = union(enum) {
        scope: workflow.ChildWorkflowError!ScopeOutcome,
        timer: Io.Cancelable!void,
        close: error{ Canceled, OutOfMemory }!void,
    };
    var buffer: [3]Raced = undefined;
    var select: Io.Select(Raced) = .init(io, &buffer);
    defer select.cancelDiscard();
    select.async(.scope, innerScope, .{ ctx, input });
    select.async(.timer, Io.sleep, .{ io, race_timeout, .awake });
    select.async(.close, awaitClose, .{ ctx, state });
    const raced = select.await() catch return cancelled;
    switch (raced) {
        .scope => |result| return scopeResult(result),
        .timer => |result| {
            result catch return cancelled;
            return timer_fired;
        },
        .close => |result| {
            result catch |err| switch (err) {
                error.Canceled => return cancelled,
                error.OutOfMemory => return err,
            };
            // The signal cancels the scope; report how the scope ended.
            var scope: ?workflow.ChildWorkflowError!ScopeOutcome = null;
            while (select.cancel()) |other| switch (other) {
                .scope => |r| scope = r,
                .timer, .close => {},
            };
            return scopeResult(scope orelse return error.ScopeNotFinished);
        },
    }
}

fn scopeResult(result: workflow.ChildWorkflowError!ScopeOutcome) ![]const u8 {
    const outcome = result catch |err| switch (err) {
        error.Canceled => return cancelled,
        else => return err,
    };
    return switch (outcome) {
        .completed => timer_fired,
        .child_cancelled => child_cancelled,
    };
}

const ExitState = struct {
    exit: bool = false,

    fn receive(state: *ExitState) void {
        state.exit = true;
    }

    fn received(state: *ExitState) bool {
        return state.exit;
    }
};

fn runChild(ctx: *workflow.Context, input: []const u8) ![]const u8 {
    const state = try ctx.allocator().create(ExitState);
    state.* = .{};
    try ctx.setSignalHandler(Exit, state, ExitState.receive);
    try ctx.await(state, ExitState.received);
    return input;
}

const How = enum { cancel_parent, close_signal };

fn execute(ctx: *harness.Context) !void {
    try scenario(ctx, MainScopeWorkflow, "main-scope", "test 42", .cancel_parent);
    try scenario(ctx, InnerScopeCancelWorkflow, "inner-scope", "baz", .cancel_parent);
    try scenario(ctx, InnerScopeCancelWorkflow, "closing-inner-scope", "foo bar", .close_signal);
}

fn scenario(ctx: *harness.Context, comptime Parent: type, comptime name: []const u8, input: []const u8, how: How) !void {
    var options = ctx.startOptions();
    options.id = try std.fmt.allocPrint(ctx.arena, "{s}-{s}", .{ ctx.task_queue, name });
    const child_id = try std.fmt.allocPrint(ctx.arena, "{s}-child", .{options.id});
    var parent = try ctx.client.executeWorkflow(Parent, options, .{input});
    defer parent.deinit();
    // Every run is closed before `execute` returns, also when a check fails.
    defer {
        ctx.client.terminateWorkflow(parent.id, "", "feature cleanup") catch {};
        ctx.client.terminateWorkflow(child_id, "", "feature cleanup") catch {};
    }

    try ctx.waitForEvent(parent.id, parent.run_id, .EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED, .fromSeconds(10));
    switch (how) {
        .cancel_parent => try ctx.client.cancelWorkflow(parent.id, parent.run_id, "feature cancels the parent"),
        .close_signal => try ctx.client.signalWorkflow(Close, parent.id, parent.run_id, .{}),
    }
    const result = parent.get(ctx.arena) catch |err| switch (err) {
        error.WorkflowFailed => return ctx.fail("{s}: expected the parent to return \"{s}\", it failed: {s}", .{ name, cancelled, if (parent.failure()) |f| f.message else "no failure details" }),
        else => return ctx.fail("{s}: expected the parent to return \"{s}\", got error.{t}", .{ name, cancelled, err }),
    };
    try ctx.expect(std.mem.eql(u8, cancelled, result), "{s}: expected the parent to return \"{s}\", got \"{s}\"", .{ name, cancelled, result });

    try ctx.client.signalWorkflow(Exit, child_id, "", .{});
    var child = try ctx.client.getWorkflow([]const u8, child_id, "");
    defer child.deinit();
    const child_result = child.get(ctx.arena) catch |err| switch (err) {
        error.WorkflowFailed => return ctx.fail("{s}: expected the abandoned child to return \"{s}\", it failed: {s}", .{ name, input, if (child.failure()) |f| f.message else "no failure details" }),
        else => return ctx.fail("{s}: expected the abandoned child to return \"{s}\", got error.{t}", .{ name, input, err }),
    };
    try ctx.expect(std.mem.eql(u8, input, child_result), "{s}: expected the abandoned child to return \"{s}\", got \"{s}\"", .{ name, input, child_result });

    const parent_history = try ctx.fetchHistory(parent.id, parent.run_id);
    try ctx.expect(
        !harness.history.contains(parent_history, .EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED),
        "{s}: the parent requested the abandoned child's cancellation",
        .{name},
    );
    const child_history = try ctx.fetchHistory(child_id, child.run_id);
    try ctx.expect(
        !harness.history.contains(child_history, .EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED),
        "{s}: the abandoned child received a cancellation request",
        .{name},
    );
}
