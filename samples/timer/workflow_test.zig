//! Tests for the timer sample.

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
        return error.SendEmailShouldNotGetCalled;
    }
};

test "fast processing cancels the timer" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(timer.Timer);
    try env.registerActivityAs(timer.ProcessOrder, Fast.processing);
    try env.registerActivityAs(timer.SendEmail, Fast.sendEmail);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    try env.executeWorkflow(timer.Timer, .{60_000}, arena.allocator(), null);
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

test "slow processing sends the email" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(timer.Timer);
    try env.registerActivityAs(timer.ProcessOrder, Slow.processing);
    try env.registerActivityAs(timer.SendEmail, Slow.sendEmail);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    try env.executeWorkflow(timer.Timer, .{0}, arena.allocator(), null);
    try testing.expect(Slow.email_sent.load(.acquire));
}
