//! Port of samples-go/timer/workflow_test.go.

const std = @import("std");
const temporal = @import("temporal");
const timer = @import("workflow.zig");

const testing = std.testing;

const Fast = struct {
    fn processing(ctx: *temporal.activity.Context) !void {
        _ = ctx;
    }
    fn sendEmail(ctx: *temporal.activity.Context) !void {
        _ = ctx;
        // In the fast processing case, this activity should not get called.
        return error.SendEmailActivityShouldNotGetCalled;
    }
};

test "Workflow_FastProcessing" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(timer.SampleTimerWorkflow);
    try env.registerActivityAs(timer.OrderProcessingActivity, Fast.processing);
    try env.registerActivityAs(timer.SendEmailActivity, Fast.sendEmail);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    try env.executeWorkflow(timer.SampleTimerWorkflow, .{std.time.ns_per_min}, arena.allocator(), null);
}

// Processing completes only after the email has been sent.
const Slow = struct {
    var email_sent: std.atomic.Value(bool) = .init(false);
    fn processing(ctx: *temporal.activity.Context) !void {
        while (!email_sent.load(.acquire)) try ctx.io().sleep(.fromMilliseconds(10), .awake);
    }
    fn sendEmail(ctx: *temporal.activity.Context) !void {
        _ = ctx;
        email_sent.store(true, .release);
    }
};

test "Workflow_SlowProcessing" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(timer.SampleTimerWorkflow);
    try env.registerActivityAs(timer.OrderProcessingActivity, Slow.processing);
    try env.registerActivityAs(timer.SendEmailActivity, Slow.sendEmail);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    try env.executeWorkflow(timer.SampleTimerWorkflow, .{std.time.ns_per_us}, arena.allocator(), null);
    try testing.expect(Slow.email_sent.load(.acquire));
}
