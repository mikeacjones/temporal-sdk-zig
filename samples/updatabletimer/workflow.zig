//! Updatable timer: a workflow that sleeps until a wake-up time, which the UpdateWakeUpTime
//! signal can move earlier or later while it sleeps. The GetWakeUpTime query reports the
//! current wake-up time.
//!
//! The sleep is `ctx.awaitTimeout` on "the wake-up time changed": the timeout is the time left
//! until the wake-up time, measured with `ctx.now()`. A signal ends the wait early (canceling
//! its durable timer) and the loop starts a new one for the new wake-up time. Canceling the
//! workflow cancels the wait, and the workflow ends canceled.
//!
//! Wake-up times are `temporal.converter.Time`, which encodes as an RFC 3339 string, so
//! clients in any language can send and read them as ordinary timestamps.

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;
const Time = temporal.converter.Time;

pub const task_queue = "updatable-timer";
pub const workflow_id = "updatable-timer";

pub const GetWakeUpTime = workflow.QueryDef("GetWakeUpTime", fn () Time);
pub const UpdateWakeUpTime = workflow.SignalDef("UpdateWakeUpTime", fn (wake_up_time: Time) void);

/// A sleep whose wake-up time can change while it waits. Allocated with `ctx.allocator()`:
/// the signal and query handlers use it for the whole run.
pub const UpdatableTimer = struct {
    wake_up_time: Time,
    changed: bool = false,

    fn update(t: *UpdatableTimer, wake_up_time: Time) void {
        t.wake_up_time = wake_up_time;
        t.changed = true;
    }

    fn wakeUpTimeChanged(t: *UpdatableTimer) bool {
        return t.changed;
    }

    fn current(t: *UpdatableTimer) Time {
        return t.wake_up_time;
    }

    /// Sleeps until `wake_up_time`, which `update` may change meanwhile. Returns
    /// `error.Canceled` if the workflow is canceled first.
    pub fn sleepUntil(t: *UpdatableTimer, ctx: *workflow.Context) !void {
        const log = ctx.logger();
        while (true) {
            const remaining = ctx.now().durationTo(t.wake_up_time.toTimestamp());
            if (remaining.nanoseconds <= 0) break;
            var buf: [Time.max_len]u8 = undefined;
            log.info("SleepUntil", .{ .wake_up_time = t.wake_up_time.format(&buf) });
            t.changed = false;
            const changed = ctx.awaitTimeout(remaining, t, wakeUpTimeChanged) catch |err| {
                if (err == error.Canceled) log.info("SleepUntil canceled", .{});
                return err;
            };
            if (!changed) break;
            log.info("Wake up time update requested", .{});
        }
        log.info("Timer fired", .{});
    }
};

fn updatableTimerWorkflow(ctx: *workflow.Context, initial_wake_up_time: Time) !void {
    const timer = try ctx.allocator().create(UpdatableTimer);
    timer.* = .{ .wake_up_time = initial_wake_up_time };
    try ctx.setQueryHandler(GetWakeUpTime, timer, UpdatableTimer.current);
    try ctx.setSignalHandler(UpdateWakeUpTime, timer, UpdatableTimer.update);
    return timer.sleepUntil(ctx);
}

/// Sleeps until its wake-up time, which the UpdateWakeUpTime signal can change.
pub const UpdatableTimerWorkflow = workflow.Def("Workflow", updatableTimerWorkflow);
