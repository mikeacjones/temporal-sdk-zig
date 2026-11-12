//! Request/response with query-based responses: a workflow that takes requests to uppercase
//! strings by signal and answers them by query, so a requester polls for its response.
//!
//! After a number of requests the workflow continues as new to keep its history small. It
//! waits until no request is in flight first: a request signal that hasn't run its activity
//! yet would otherwise be lost, as the new run never sees it. Requests that never stop
//! coming, or activities that keep retrying, can delay that idle moment indefinitely, which
//! is why the activity's retries are bounded.

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
};

/// The response to a `Request`, read with the "response" query.
pub const Response = struct {
    id: []const u8,
    output: []const u8 = "",
    /// Why the request failed; empty if it succeeded.
    error_message: []const u8 = "",
};

/// Sends a request to the workflow.
pub const RequestSignal = workflow.SignalDef("request", fn (request: Request) void);

/// Reads the response to the request with this ID, or null while it is in flight (or if the
/// workflow never got it, or it arrived before the current run continued as new).
pub const ResponseQuery = workflow.QueryDef("response", fn (request_id: []const u8) ?Response);

/// How many requests a run takes before it continues as new.
pub const requests_before_continue_as_new = 500;

fn uppercaseActivity(ctx: *activity.Context, input: []const u8) ![]const u8 {
    return std.ascii.allocUpperString(ctx.allocator(), input);
}

pub const UppercaseActivity = activity.Def("UppercaseActivity", uppercaseActivity);

/// The run's state, shared by the workflow function and its handlers. Allocated with
/// `ctx.allocator()` so queries can still read it after the workflow function has returned.
const Uppercaser = struct {
    ctx: *workflow.Context,
    request_count: u32 = 0,
    /// Every response of this run, by request ID. Kept for the whole run (queries can't
    /// remove them, and a requester may ask more than once), which is bounded by
    /// `requests_before_continue_as_new`.
    responses: std.StringHashMapUnmanaged(Response) = .empty,

    fn request(u: *Uppercaser, req: Request) !void {
        u.request_count += 1;
        // The timeout and retries bound how long this request keeps the run from continuing
        // as new.
        const options: workflow.ActivityOptions = .{
            .schedule_to_close_timeout = .fromSeconds(5),
            .retry_policy = .{ .maximum_attempts = 2 },
        };
        var response: Response = .{ .id = req.id };
        const future = u.ctx.executeActivity(UppercaseActivity, options, .{req.input});
        if (future.get()) |output| {
            response.output = output;
        } else |err| {
            response.error_message = if (future.failure()) |f| temporal.failure.rootCause(f).message else @errorName(err);
        }
        try u.responses.put(u.ctx.allocator(), req.id, response);
    }

    fn responseFor(u: *Uppercaser, request_id: []const u8) ?Response {
        return u.responses.get(request_id);
    }

    fn readyToContinueAsNew(u: *Uppercaser) bool {
        return u.request_count >= requests_before_continue_as_new and u.ctx.allHandlersFinished();
    }
};

fn uppercaseWorkflow(ctx: *workflow.Context) !void {
    const u = try ctx.allocator().create(Uppercaser);
    u.* = .{ .ctx = ctx };
    try ctx.setQueryHandler(ResponseQuery, u, Uppercaser.responseFor);
    try ctx.setSignalHandler(RequestSignal, u, Uppercaser.request);

    // Returns error.Canceled if the workflow is canceled, which ends it as canceled.
    try ctx.await(u, Uppercaser.readyToContinueAsNew);
    return ctx.continueAsNew(UppercaseWorkflow, .{}, .{});
}

pub const UppercaseWorkflow = workflow.Def("UppercaseWorkflow", uppercaseWorkflow);
