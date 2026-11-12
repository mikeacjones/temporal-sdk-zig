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

/// The counter, shared by the workflow function and the handlers. It lives in
/// `ctx.allocator()` memory so the handlers can outlive the function's stack frame.
const Counter = struct {
    ctx: *workflow.Context,
    value: i64 = 0,
    done: bool = false,

    fn fetchAndAdd(c: *Counter, addend: i64) i64 {
        const previous = c.value;
        c.value += addend;
        c.ctx.logger().info("counter updated", .{ .addend = addend, .new_value = c.value });
        return previous;
    }

    fn nonNegative(c: *Counter, addend: i64) ValidationError!void {
        if (addend < 0) {
            c.ctx.logger().debug("Rejecting negative update", .{ .addend = addend });
            return error.NegativeAddend;
        }
        c.ctx.logger().debug("Accepting update", .{ .addend = addend });
    }

    fn finish(c: *Counter) void {
        c.done = true;
    }

    fn isDone(c: *Counter) bool {
        return c.done;
    }
};

fn counterWorkflow(ctx: *workflow.Context) !i64 {
    const c = try ctx.allocator().create(Counter);
    c.* = .{ .ctx = ctx };
    try ctx.setUpdateHandlerWithValidator(FetchAndAdd, c, Counter.fetchAndAdd, Counter.nonNegative);
    try ctx.setSignalHandler(Done, c, Counter.finish);

    try ctx.await(c, Counter.isDone);
    return c.value;
}

/// Holds a counter that starts at zero, and returns its value once it receives "done".
pub const CounterWorkflow = workflow.Def("Counter", counterWorkflow);
