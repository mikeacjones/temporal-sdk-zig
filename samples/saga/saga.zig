//! Saga: a money transfer that undoes its completed steps when a later step fails. Each step
//! that succeeds registers its compensation with `errdefer`, so on the error path the
//! compensations run in reverse order and the workflow still fails with the original failure.

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;
const activity = temporal.activity;

pub const task_queue = "TRANSFER_MONEY_TASK_QUEUE";

pub const TransferDetails = struct {
    amount: f32,
    from_account: []const u8,
    to_account: []const u8,
    reference_id: []const u8,
};

fn transferMoney(ctx: *workflow.Context, transfer_details: TransferDetails) !void {
    const ao: workflow.ActivityOptions = .{
        .start_to_close_timeout = .fromSeconds(60),
        // Temporal retries failures by default; this policy is an example of customizing it.
        .retry_policy = .{
            .initial_interval = .fromSeconds(1),
            .backoff_coefficient = 2.0,
            .maximum_interval = .fromSeconds(60),
            .maximum_attempts = 3,
        },
    };

    try ctx.executeActivity(Withdraw, ao, .{transfer_details}).get();
    errdefer compensate(ctx, WithdrawCompensation, ao, transfer_details);

    try ctx.executeActivity(Deposit, ao, .{transfer_details}).get();
    errdefer compensate(ctx, DepositCompensation, ao, transfer_details);

    try ctx.executeActivity(StepWithError, ao, .{transfer_details}).get();
}

/// Runs a compensation on the error path. A compensation that fails is logged and the
/// remaining compensations still run; the workflow keeps failing with the step's error.
fn compensate(ctx: *workflow.Context, comptime Compensation: type, ao: workflow.ActivityOptions, transfer_details: TransferDetails) void {
    ctx.executeActivity(Compensation, ao, .{transfer_details}).get() catch |err| {
        // warn rather than err: the transfer's own failure is what fails the workflow.
        ctx.logger().warn("Compensation failed.", .{ .activity = Compensation.activity_name, .err = err });
    };
}

pub const TransferMoney = workflow.Def("TransferMoney", transferMoney);

fn withdraw(ctx: *activity.Context, transfer_details: TransferDetails) !void {
    ctx.logger().info("Withdrawing from account.", .{
        .amount = transfer_details.amount,
        .from_account = transfer_details.from_account,
        .reference_id = transfer_details.reference_id,
    });
}

fn withdrawCompensation(ctx: *activity.Context, transfer_details: TransferDetails) !void {
    ctx.logger().info("Withdrawing compensation from account.", .{
        .amount = transfer_details.amount,
        .from_account = transfer_details.from_account,
        .reference_id = transfer_details.reference_id,
    });
}

fn deposit(ctx: *activity.Context, transfer_details: TransferDetails) !void {
    ctx.logger().info("Depositing into account.", .{
        .amount = transfer_details.amount,
        .to_account = transfer_details.to_account,
        .reference_id = transfer_details.reference_id,
    });
}

fn depositCompensation(ctx: *activity.Context, transfer_details: TransferDetails) !void {
    ctx.logger().info("Depositing compensation into account.", .{
        .amount = transfer_details.amount,
        .to_account = transfer_details.to_account,
        .reference_id = transfer_details.reference_id,
    });
}

/// Always fails, to trigger the compensations.
fn stepWithError(ctx: *activity.Context, transfer_details: TransferDetails) !void {
    ctx.logger().info("Simulate failure to trigger compensation.", .{ .reference_id = transfer_details.reference_id });
    return ctx.fail(.{ .message = "some error" });
}

pub const Withdraw = activity.Def("Withdraw", withdraw);
pub const WithdrawCompensation = activity.Def("WithdrawCompensation", withdrawCompensation);
pub const Deposit = activity.Def("Deposit", deposit);
pub const DepositCompensation = activity.Def("DepositCompensation", depositCompensation);
pub const StepWithError = activity.Def("StepWithError", stepWithError);
