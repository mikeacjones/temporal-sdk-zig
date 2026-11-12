//! Expense: a workflow that creates an expense report in an expense system, waits for a person
//! to approve or reject it, and requests payment when it is approved.
//!
//! Waiting for the decision can take any amount of time, so `WaitForDecisionActivity` doesn't
//! wait inside its function: it registers its task token with the expense system and returns
//! `ctx.completeAsync()`. The activity stays running on the server until the expense system
//! completes it through the client with the decision (see expense_system.zig).

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;
const activity = temporal.activity;

pub const task_queue = "expense";

/// Where the worker's activities reach the expense system, and where the expense system
/// listens.
pub const expense_server_host = "127.0.0.1";
pub const expense_server_port = 8099;

/// The activities talk to the expense system over HTTP. They are methods so the worker can
/// point them at any expense system, such as one a test starts on a free port.
pub const Activities = struct {
    /// Base URL of the expense system, for example "http://127.0.0.1:8099". Must outlive
    /// the worker.
    server_url: []const u8,

    pub fn createExpense(a: *Activities, ctx: *activity.Context, expense_id: []const u8) !void {
        if (expense_id.len == 0) return ctx.fail(.{ .message = "expense id is empty" });
        const body = try a.call(ctx, try std.fmt.allocPrint(ctx.allocator(), "/create?is_api_call=true&id={s}", .{expense_id}), null);
        if (!std.mem.eql(u8, body, "SUCCEED")) return ctx.fail(.{ .message = body });
        ctx.logger().info("Expense created.", .{ .expense_id = expense_id });
    }

    /// Registers this attempt's task token with the expense system, which completes the
    /// activity with "APPROVED" or "REJECTED" once a person decides.
    pub fn waitForDecision(a: *Activities, ctx: *activity.Context, expense_id: []const u8) ![]const u8 {
        if (expense_id.len == 0) return ctx.fail(.{ .message = "expense id is empty" });
        const logger = ctx.logger();

        var form: std.Io.Writer.Allocating = .init(ctx.allocator());
        try form.writer.writeAll("task_token=");
        try (std.Uri.Component{ .raw = ctx.info().task_token }).formatEscaped(&form.writer);

        const path = try std.fmt.allocPrint(ctx.allocator(), "/registerCallback?id={s}", .{expense_id});
        const status = a.call(ctx, path, form.written()) catch |err| {
            logger.info("waitForDecisionActivity failed to register callback.", .{ .err = err });
            return err;
        };
        if (std.mem.eql(u8, status, "SUCCEED")) {
            logger.info("Successfully registered callback.", .{ .expense_id = expense_id });
            return ctx.completeAsync();
        }

        logger.warn("Register callback failed.", .{ .expense_status = status });
        return ctx.fail(.{ .message = try std.fmt.allocPrint(ctx.allocator(), "register callback failed status:{s}", .{status}) });
    }

    pub fn payment(a: *Activities, ctx: *activity.Context, expense_id: []const u8) !void {
        if (expense_id.len == 0) return ctx.fail(.{ .message = "expense id is empty" });
        const body = try a.call(ctx, try std.fmt.allocPrint(ctx.allocator(), "/action?is_api_call=true&type=payment&id={s}", .{expense_id}), null);
        if (!std.mem.eql(u8, body, "SUCCEED")) return ctx.fail(.{ .message = body });
        ctx.logger().info("paymentActivity succeed", .{ .expense_id = expense_id });
    }

    /// GETs `path` (or POSTs `form` to it as a form) and returns the response body, in the
    /// activity's memory.
    fn call(a: *Activities, ctx: *activity.Context, path: []const u8, form: ?[]const u8) ![]const u8 {
        const arena = ctx.allocator();
        var http: std.http.Client = .{ .allocator = arena, .io = ctx.io() };
        defer http.deinit();
        var body: std.Io.Writer.Allocating = .init(arena);
        const url = try std.fmt.allocPrint(arena, "{s}{s}", .{ a.server_url, path });
        const response = http.fetch(.{
            .location = .{ .url = url },
            .payload = form,
            .headers = .{ .content_type = if (form != null) .{ .override = "application/x-www-form-urlencoded" } else .default },
            .response_writer = &body.writer,
        }) catch |err| {
            ctx.logger().warn("Expense system request failed.", .{ .url = url, .err = err });
            return err;
        };
        if (response.status != .ok) return ctx.fail(.{ .message = try std.fmt.allocPrint(arena, "expense system answered {s} with HTTP {d}", .{ url, @backingInt(response.status) }) });
        return body.written();
    }
};

pub const CreateExpenseActivity = activity.Def("CreateExpenseActivity", Activities.createExpense);
pub const WaitForDecisionActivity = activity.Def("WaitForDecisionActivity", Activities.waitForDecision);
pub const PaymentActivity = activity.Def("PaymentActivity", Activities.payment);

/// Returns "COMPLETED" once an approved expense is paid, or "" when it was rejected.
fn sampleExpense(ctx: *workflow.Context, expense_id: []const u8) ![]const u8 {
    const logger = ctx.logger();

    // Step 1: create the expense report.
    ctx.executeActivity(CreateExpenseActivity, .{ .start_to_close_timeout = .fromSeconds(10) }, .{expense_id}).get() catch |err| {
        logger.err("Failed to create expense report", .{ .err = err });
        return err;
    };

    // Step 2: wait for the expense report to be approved or rejected. The start-to-close
    // timeout bounds how long a person has to decide: set it to the longest wait you expect,
    // or the activity times out first.
    const ao: workflow.ActivityOptions = .{ .start_to_close_timeout = .fromSeconds(10 * 60) };
    const status = try ctx.executeActivity(WaitForDecisionActivity, ao, .{expense_id}).get();
    if (!std.mem.eql(u8, status, "APPROVED")) {
        logger.info("Workflow completed.", .{ .expense_status = status });
        return "";
    }

    // Step 3: request payment for the expense.
    ctx.executeActivity(PaymentActivity, ao, .{expense_id}).get() catch |err| {
        logger.info("Workflow completed with payment failed.", .{ .err = err });
        return err;
    };

    logger.info("Workflow completed with expense payment completed.", .{});
    return "COMPLETED";
}

pub const SampleExpenseWorkflow = workflow.Def("SampleExpenseWorkflow", sampleExpense);
