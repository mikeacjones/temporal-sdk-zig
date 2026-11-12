//! Handles three signals, Signal1, Signal2 and Signal3, that must be processed in that order
//! but can arrive in any order, and fails if one doesn't arrive in time. Two timeouts apply:
//! the time between signals, and the total time since the first signal arrived.
//!
//! The signal handlers only record which signals have arrived. The workflow function then waits
//! for each signal in turn with `ctx.awaitTimeout`, on a condition over that state, so the order
//! and timeout rules read as straight-line code rather than being spread across the handlers.

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;
const Duration = std.Io.Duration;

pub const Signal1 = workflow.SignalDef("Signal1", fn () void);
pub const Signal2 = workflow.SignalDef("Signal2", fn () void);
pub const Signal3 = workflow.SignalDef("Signal3", fn () void);

/// The longest the workflow waits for the next signal.
pub const signal_to_signal_timeout: Duration = .fromSeconds(30);
/// The longest the workflow waits for all signals, counted from the first one to arrive.
pub const from_first_signal_timeout: Duration = .fromSeconds(60);

/// Which signals have arrived. Allocated with `ctx.allocator()`, as the handlers outlive the
/// workflow function's stack frame.
const AwaitSignals = struct {
    ctx: *workflow.Context,
    /// When the first signal, whichever it was, arrived.
    first_signal_time: ?std.Io.Timestamp = null,
    signal1_received: bool = false,
    signal2_received: bool = false,
    signal3_received: bool = false,

    fn received(a: *AwaitSignals, comptime name: []const u8, flag: *bool) void {
        flag.* = true;
        if (a.first_signal_time == null) a.first_signal_time = a.ctx.now();
        a.ctx.logger().info(name ++ " Received", .{});
    }

    fn onSignal1(a: *AwaitSignals) void {
        a.received("Signal1", &a.signal1_received);
    }
    fn onSignal2(a: *AwaitSignals) void {
        a.received("Signal2", &a.signal2_received);
    }
    fn onSignal3(a: *AwaitSignals) void {
        a.received("Signal3", &a.signal3_received);
    }

    fn hasSignal1(a: *AwaitSignals) bool {
        return a.signal1_received;
    }
    fn hasSignal2(a: *AwaitSignals) bool {
        return a.signal2_received;
    }
    fn hasSignal3(a: *AwaitSignals) bool {
        return a.signal3_received;
    }

    /// The longest to wait for the next signal: the signal-to-signal timeout, or what is left
    /// of the total timeout if that is shorter. Fails the workflow once no time is left.
    fn nextTimeout(a: *AwaitSignals) error{ApplicationFailure}!Duration {
        // Only called once Signal1 has been received, so a first signal time is set.
        const total = a.first_signal_time.?.durationTo(a.ctx.now());
        const total_left = from_first_signal_timeout.nanoseconds - total.nanoseconds;
        if (total_left <= 0) return a.ctx.fail(.{ .message = "FromFirstSignalTimeout", .type = "timeout" });
        return .{ .nanoseconds = @min(signal_to_signal_timeout.nanoseconds, total_left) };
    }
};

fn awaitSignalsWorkflow(ctx: *workflow.Context) !void {
    const logger = ctx.logger();
    const a = try ctx.allocator().create(AwaitSignals);
    a.* = .{ .ctx = ctx };
    try ctx.setSignalHandler(Signal1, a, AwaitSignals.onSignal1);
    try ctx.setSignalHandler(Signal2, a, AwaitSignals.onSignal2);
    try ctx.setSignalHandler(Signal3, a, AwaitSignals.onSignal3);

    // Returns error.Canceled if the workflow is canceled, which ends it as canceled.
    try ctx.await(a, AwaitSignals.hasSignal1);
    logger.info("Signal1 Processed", .{});

    if (!try ctx.awaitTimeout(try a.nextTimeout(), a, AwaitSignals.hasSignal2))
        return ctx.fail(.{ .message = "Timed out waiting for signal2", .type = "timeout" });
    logger.info("Signal2 Processed", .{});

    if (!try ctx.awaitTimeout(try a.nextTimeout(), a, AwaitSignals.hasSignal3))
        return ctx.fail(.{ .message = "Timed out waiting for signal3", .type = "timeout" });
    logger.info("Signal3 Processed", .{});
}

pub const AwaitSignalsWorkflow = workflow.Def("AwaitSignalsWorkflow", awaitSignalsWorkflow);
