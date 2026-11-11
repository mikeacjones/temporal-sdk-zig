//! Timer: start a long-running activity and a timer together. If the timer fires first, send
//! a notification email but keep waiting for the processing; if the processing finishes
//! first, cancel the timer. `std.Io.Select` waits for whichever comes first, and canceling the
//! select's remaining task cancels the durable timer.

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;
const activity = temporal.activity;

const Ready = union(enum) {
    processing: workflow.ActivityError!void,
    timer: std.Io.Cancelable!void,
};

fn timer(ctx: *workflow.Context, threshold_ms: i64) !void {
    const io = ctx.io();
    const ao: workflow.ActivityOptions = .{ .start_to_close_timeout = .fromSeconds(10) };

    var buffer: [2]Ready = undefined;
    var select: std.Io.Select(Ready) = .init(io, &buffer);
    defer select.cancelDiscard();

    const processing = ctx.executeActivity(ProcessOrder, ao, .{});
    select.async(.processing, workflow.ActivityFuture(void).get, .{processing});
    select.async(.timer, std.Io.sleep, .{ io, .fromMilliseconds(threshold_ms), .awake });

    switch (try select.await()) {
        // Processing is done: leaving cancels the timer.
        .processing => |r| try r,
        .timer => |fired| {
            try fired;
            // The processing is taking a long time: send a notification email, then wait for
            // the processing to finish.
            try ctx.executeActivity(SendEmail, ao, .{}).get();
            try (try select.await()).processing;
        },
    }

    ctx.logger().info("Workflow completed.", .{});
}

pub const Timer = workflow.Def("Timer", timer);

fn processOrder(ctx: *activity.Context) !void {
    const logger = ctx.logger();
    logger.info("Order processing started.", .{});
    const source: std.Random.IoSource = .{ .io = ctx.io() };
    const seconds = source.interface().uintLessThan(u32, 10);
    try ctx.io().sleep(.fromSeconds(seconds), .awake);
    logger.info("Order processing done.", .{ .duration_seconds = seconds });
}

fn sendEmail(ctx: *activity.Context) !void {
    ctx.logger().info("Sending a notification email: processing is taking a long time.", .{});
}

pub const ProcessOrder = activity.Def("ProcessOrder", processOrder);
pub const SendEmail = activity.Def("SendEmail", sendEmail);
