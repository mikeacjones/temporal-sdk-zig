//! Request/response through updates: a long-running workflow that uppercases strings sent to
//! it as "update" requests. Each request runs an activity and the update returns its result to
//! the caller.
//!
//! A workflow can't keep a history that grows forever, so after `requests_before_continue_as_new`
//! requests it continues as new. It waits until no update handler is running first, so no
//! request is lost. While it waits, requests that keep arriving could postpone that moment
//! indefinitely; with `reject_update_on_pending_continue_as_new` set, a validator rejects them
//! with `error.TryingToContinueAsNew` instead, which leaves no trace in history and tells the
//! caller to back off and retry.

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;
const activity = temporal.activity;

/// A request to uppercase `input`, the argument of the "update" update.
pub const Request = struct {
    input: []const u8,
};

/// The response to a `Request`, the result of the "update" update.
pub const Response = struct {
    output: []const u8,
};

/// Sends a request to the workflow and returns its response.
pub const Uppercase = workflow.UpdateDef("update", fn (request: Request) Response);

/// How many requests a run accepts before it continues as new. History grows with every
/// request and activity, so an endless stream of them must be split across runs.
pub const requests_before_continue_as_new = 500;

/// The validator's error, which a requester treats as a signal to back off.
pub const BackoffError = error{TryingToContinueAsNew};

fn uppercaseActivity(ctx: *activity.Context, input: []const u8) ![]const u8 {
    return std.ascii.allocUpperString(ctx.allocator(), input);
}

pub const UppercaseActivity = activity.Def("UppercaseActivity", uppercaseActivity);

/// The workflow, continuing as new after `limit` requests. `UppercaseWorkflow` is the sample's
/// version; tests use a smaller limit to reach continue-as-new quickly. Every version
/// registers as "UppercaseWorkflow".
pub fn UppercaseWorkflowWithLimit(comptime limit: u32) type {
    return struct {
        /// The workflow's state, shared by the workflow function, the update handler and its
        /// validator. It lives in `ctx.allocator()` memory so the handlers can outlive the
        /// function's stack frame.
        const Uppercaser = struct {
            ctx: *workflow.Context,
            request_count: u32 = 0,

            // Only one retry within five seconds. The timeout and retries decide how long a
            // request can keep the workflow from continuing as new.
            const activity_options: workflow.ActivityOptions = .{
                .schedule_to_close_timeout = .fromSeconds(5),
                .retry_policy = .{ .maximum_attempts = 2 },
            };

            fn uppercase(u: *Uppercaser, request: Request) !Response {
                u.request_count += 1;
                const output = try u.ctx.executeActivity(UppercaseActivity, activity_options, .{request.input}).get();
                return .{ .output = output };
            }

            /// Rejecting here keeps the update out of history, which matters when history is
            /// what the workflow is trying to shed.
            fn rejectWhileContinuingAsNew(u: *Uppercaser, request: Request) BackoffError!void {
                _ = request;
                if (u.request_count >= limit) return error.TryingToContinueAsNew;
            }

            fn readyToContinueAsNew(u: *Uppercaser) bool {
                return u.request_count >= limit and u.ctx.allHandlersFinished();
            }
        };

        fn run(ctx: *workflow.Context, reject_update_on_pending_continue_as_new: bool) !void {
            const u = try ctx.allocator().create(Uppercaser);
            u.* = .{ .ctx = ctx };
            if (reject_update_on_pending_continue_as_new) {
                try ctx.setUpdateHandlerWithValidator(Uppercase, u, Uppercaser.uppercase, Uppercaser.rejectWhileContinuingAsNew);
            } else {
                try ctx.setUpdateHandler(Uppercase, u, Uppercaser.uppercase);
            }

            // If requests arrive faster than they are handled, there is never a moment with no
            // handler running and the workflow never gets past this.
            try ctx.await(u, Uppercaser.readyToContinueAsNew);
            return ctx.continueAsNew(Def, .{}, .{reject_update_on_pending_continue_as_new});
        }

        pub const Def = workflow.Def("UppercaseWorkflow", run);
    };
}

/// Uppercases strings sent as "update" requests until it continues as new. Its argument is
/// whether to reject requests once it is waiting to continue as new.
pub const UppercaseWorkflow = UppercaseWorkflowWithLimit(requests_before_continue_as_new).Def;
