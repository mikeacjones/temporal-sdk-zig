//! Port of samples-go/timer: start a long-running activity and a timer together. If the
//! timer fires first, send a notification email but keep waiting for the processing; if the
//! processing finishes first, cancel the timer.
//!
//! Go's selector over the activity and timer futures becomes `std.Io.Select`, and canceling
//! the timer's context becomes canceling the select's remaining task (the timer).

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;
const activity = temporal.activity;

const Ready = union(enum) {
    processing: workflow.ActivityError!void,
    timer: std.Io.Cancelable!void,
};

fn sampleTimerWorkflow(ctx: *workflow.Context, processing_time_threshold: i64) !void {
    const io = ctx.io();
    const ao: workflow.ActivityOptions = .{ .start_to_close_timeout = .fromSeconds(10) };

    var buffer: [2]Ready = undefined;
    var selector: std.Io.Select(Ready) = .init(io, &buffer);
    defer selector.cancelDiscard();

    const processing = ctx.executeActivity(OrderProcessingActivity, ao, .{});
    selector.async(.processing, workflow.ActivityFuture(void).get, .{processing});
    // The threshold is a Go time.Duration: nanoseconds.
    selector.async(.timer, std.Io.sleep, .{ io, .fromNanoseconds(processing_time_threshold), .awake });

    switch (try selector.await()) {
        // Processing is done: leaving cancels the timer.
        .processing => |r| try r,
        .timer => |fired| {
            try fired;
            // The processing is taking a long time: send a notification email, then wait for
            // the processing to finish.
            try ctx.executeActivity(SendEmailActivity, ao, .{}).get();
            try (try selector.await()).processing;
        },
    }

    ctx.logger().info("Workflow completed.", .{});
}

pub const SampleTimerWorkflow = workflow.Def("SampleTimerWorkflow", sampleTimerWorkflow);

fn orderProcessingActivity(ctx: *activity.Context) !void {
    const logger = ctx.logger();
    logger.info("OrderProcessingActivity processing started.", .{});
    const source: std.Random.IoSource = .{ .io = ctx.io() };
    const seconds = source.interface().uintLessThan(u32, 10);
    try ctx.io().sleep(.fromSeconds(seconds), .awake);
    logger.info("OrderProcessingActivity done.", .{ .duration_seconds = seconds });
}

fn sendEmailActivity(ctx: *activity.Context) !void {
    ctx.logger().info("SendEmailActivity sending notification email as the process takes long time.", .{});
}

pub const OrderProcessingActivity = activity.Def("OrderProcessingActivity", orderProcessingActivity);
pub const SendEmailActivity = activity.Def("SendEmailActivity", sendEmailActivity);
