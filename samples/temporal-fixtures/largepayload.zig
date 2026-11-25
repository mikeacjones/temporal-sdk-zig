//! Large payload: a workflow whose first activity returns a payload of random bytes of the
//! size asked for, which the workflow passes to a second activity. The starter also gives
//! each run a memo of that size. The bytes travel as `temporal.converter.Bytes`
//! (`binary/plain`), since random bytes are not valid UTF-8 and so cannot be a JSON string.

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;
const activity = temporal.activity;
const Bytes = temporal.converter.Bytes;

pub const task_queue = "largepayload";

fn createLargeResultActivity(ctx: *activity.Context, size_bytes: u32) !Bytes {
    ctx.logger().info("Creating large result payload...", .{ .size_bytes = size_bytes });
    const token = try ctx.allocator().alloc(u8, size_bytes);
    ctx.io().random(token);
    return .{ .data = token };
}

pub const CreateLargeResultActivity = activity.Def("CreateLargeResultActivity", createLargeResultActivity);

fn processLargeInputActivity(ctx: *activity.Context, input: Bytes) !void {
    _ = ctx;
    _ = input;
}

pub const ProcessLargeInputActivity = activity.Def("ProcessLargeInputActivity", processLargeInputActivity);

fn largePayloadWorkflow(ctx: *workflow.Context, payload_size: u32) !void {
    const logger = ctx.logger();
    const ao: workflow.ActivityOptions = .{ .start_to_close_timeout = .fromSeconds(std.time.s_per_min) };

    const data = try ctx.executeActivity(CreateLargeResultActivity, ao, .{payload_size}).get();
    ctx.executeActivity(ProcessLargeInputActivity, ao, .{data}).get() catch |err| {
        logger.err("Workflow failed.", .{ .err = err });
        return err;
    };
    logger.info("Workflow completed.", .{});
}

pub const LargePayloadWorkflow = workflow.Def("LargePayloadWorkflow", largePayloadWorkflow);
