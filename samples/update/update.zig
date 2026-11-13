//! A counter workflow changed through the "fetch_and_add" update, which adds its argument to
//! the counter and returns the counter's previous value. The update's validator rejects
//! negative addends, so they never reach history. The "done" signal ends the workflow, which
//! returns the final count.

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;

/// Adds `addend` to the counter and returns the value before the addition.
pub const FetchAndAdd = workflow.UpdateDef("fetch_and_add", fn (addend: i64) i64);

/// Ends the workflow with the counter's current value.
pub const Done = workflow.SignalDef("done", fn () void);

/// The validator's error, the message of a rejected update's failure.
pub const ValidationError = error{NegativeAddend};

/// The counter workflow. Each run starts from `Counter{}`; the SDK registers the declared
/// update and signal handlers before the run's first workflow task delivers anything to them.
pub const Counter = struct {
    /// Set by `run` before it first blocks, which is before any handler runs.
    ctx: *workflow.Context = undefined,
    value: i64 = 0,
    done: bool = false,

    pub const updates = .{ .fetchAndAdd = .{ FetchAndAdd, .nonNegative } };
    pub const signals = .{ .finish = Done };

    pub fn run(c: *Counter, ctx: *workflow.Context) !i64 {
        c.ctx = ctx;
        try ctx.await(c, isDone);
        return c.value;
    }

    pub fn fetchAndAdd(c: *Counter, addend: i64) i64 {
        const previous = c.value;
        c.value += addend;
        c.ctx.logger().info("counter updated", .{ .addend = addend, .new_value = c.value });
        return previous;
    }

    /// Reads only its argument: an update sent with the start is validated before `run`
    /// has set `ctx`.
    pub fn nonNegative(_: *Counter, addend: i64) ValidationError!void {
        if (addend < 0) return error.NegativeAddend;
    }

    pub fn finish(c: *Counter) void {
        c.done = true;
    }

    fn isDone(c: *Counter) bool {
        return c.done;
    }
};

/// Holds a counter that starts at zero, and returns its value once it receives "done".
pub const CounterWorkflow = workflow.Def("Counter", Counter);
