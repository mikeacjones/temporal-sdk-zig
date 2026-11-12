//! Request/response with activity-based responses: a workflow that takes requests to uppercase
//! strings by signal and answers each one by calling a response activity that the requester
//! names in its request, on the requester's own task queue. So a requester runs a worker of
//! its own (see requester.zig), and the response is pushed to it rather than polled for.
//!
//! After a number of requests the workflow continues as new to keep its history small. It
//! waits until no request is in flight first: a request signal whose activities haven't
//! finished would otherwise be lost, as the new run never sees it. Requests that never stop
//! coming, or activities that keep retrying, can delay that idle moment indefinitely, which is
//! why both activities' timeouts and retries are bounded.

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;
const activity = temporal.activity;

/// A request to uppercase a string, the argument of the "request" signal.
pub const Request = struct {
    /// Set on the response too.
    id: []const u8,
    /// The string to uppercase.
    input: []const u8,
    /// The activity that receives the `Response`, and the task queue it runs on.
    response_activity: []const u8,
    response_task_queue: []const u8,
};

/// The response to a `Request`, the argument of the response activity.
pub const Response = struct {
    id: []const u8,
    output: []const u8 = "",
    /// Why the request failed; empty if it succeeded.
    error_message: []const u8 = "",
};

/// Sends a request to the workflow.
pub const RequestSignal = workflow.SignalDef("request", fn (request: Request) void);

/// How many requests a run takes before it continues as new.
pub const requests_before_continue_as_new = 500;

fn uppercaseActivity(ctx: *activity.Context, input: []const u8) ![]const u8 {
    return std.ascii.allocUpperString(ctx.allocator(), input);
}

pub const UppercaseActivity = activity.Def("UppercaseActivity", uppercaseActivity);

/// The workflow, continuing as new after `limit` requests. `UppercaseWorkflow` is the sample's
/// version; tests use a smaller limit to reach continue-as-new quickly. Every version
/// registers as "UppercaseWorkflow".
pub fn UppercaseWorkflowWithLimit(comptime limit: u32) type {
    return struct {
        /// The run's state, shared by the workflow function and its signal handler. Allocated
        /// with `ctx.allocator()` so handlers can outlive the function's stack frame.
        const Uppercaser = struct {
            ctx: *workflow.Context,
            request_count: u32 = 0,

            // The timeouts and retries of both activities bound how long a request keeps the
            // run from continuing as new.
            const uppercase_options: workflow.ActivityOptions = .{
                .schedule_to_close_timeout = .fromSeconds(5),
                .retry_policy = .{ .maximum_attempts = 2 },
            };
            // Schedule-to-start as well as schedule-to-close, because a requester whose worker
            // is gone would otherwise leave the response activity waiting in its task queue.
            const response_options: workflow.ActivityOptions = .{
                .schedule_to_start_timeout = .fromSeconds(5),
                .schedule_to_close_timeout = .fromSeconds(10),
                .retry_policy = .{ .maximum_attempts = 4 },
            };

            fn request(u: *Uppercaser, req: Request) void {
                u.request_count += 1;
                var response: Response = .{ .id = req.id };
                const uppercase = u.ctx.executeActivity(UppercaseActivity, uppercase_options, .{req.input});
                if (uppercase.get()) |output| {
                    response.output = output;
                } else |err| {
                    response.error_message = if (uppercase.failure()) |f| temporal.failure.rootCause(f).message else @errorName(err);
                }

                var options = response_options;
                options.task_queue = req.response_task_queue;
                const respond = u.ctx.executeActivityByName(void, req.response_activity, options, .{response});
                // A requester that stopped listening is not the workflow's failure: log it and
                // carry on taking requests.
                respond.get() catch |err| {
                    const reason = if (respond.failure()) |f| temporal.failure.rootCause(f).message else @errorName(err);
                    u.ctx.logger().warn("Failure sending response activity", .{ .request_id = req.id, .err = reason });
                };
            }

            fn readyToContinueAsNew(u: *Uppercaser) bool {
                return u.request_count >= limit and u.ctx.allHandlersFinished();
            }
        };

        fn run(ctx: *workflow.Context) !void {
            const u = try ctx.allocator().create(Uppercaser);
            u.* = .{ .ctx = ctx };
            try ctx.setSignalHandler(RequestSignal, u, Uppercaser.request);

            // Returns error.Canceled if the workflow is canceled, which ends it as canceled.
            try ctx.await(u, Uppercaser.readyToContinueAsNew);
            return ctx.continueAsNew(Def, .{}, .{});
        }

        pub const Def = workflow.Def("UppercaseWorkflow", run);
    };
}

/// Uppercases strings sent as "request" signals, answering each with the requester's response
/// activity, until it continues as new or is canceled.
pub const UppercaseWorkflow = UppercaseWorkflowWithLimit(requests_before_continue_as_new).Def;
