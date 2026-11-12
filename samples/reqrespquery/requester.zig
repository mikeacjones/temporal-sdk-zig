//! Requests uppercasing from a running uppercase workflow: sends each request by signal and
//! polls the "response" query until its answer appears.

const std = @import("std");
const temporal = @import("temporal");
const uppercase = @import("workflow.zig");

const Duration = std.Io.Duration;

pub const Requester = struct {
    client: *temporal.client.Client,
    io: std.Io,
    options: Options,

    pub const Options = struct {
        /// The ID of the workflow taking requests.
        target_workflow_id: []const u8,
        /// How often to query for the response.
        response_query_interval: Duration = .fromMilliseconds(300),
        /// How long to wait for the response.
        response_timeout: Duration = .fromSeconds(4),
    };

    pub const Error = error{
        OutOfMemory,
        Canceled,
        /// Sending the request signal failed; the client logs why.
        SignalFailed,
        /// The workflow answered with a failed response; its message is logged.
        RequestFailed,
        /// No response within `response_timeout`; the last query error, if any, is logged.
        Timeout,
    };

    /// `client` must outlive the requester. Requests may be made from one task at a time.
    pub fn init(client: *temporal.client.Client, io: std.Io, options: Options) error{MissingTargetWorkflowId}!Requester {
        if (options.target_workflow_id.len == 0) return error.MissingTargetWorkflowId;
        return .{ .client = client, .io = io, .options = options };
    }

    /// Asks the workflow to uppercase `input` and returns the result, allocated from `arena`.
    pub fn requestUppercase(r: *const Requester, arena: std.mem.Allocator, input: []const u8) Error![]const u8 {
        const request: uppercase.Request = .{ .id = try temporal.client.uuid(arena, r.io), .input = input };
        r.client.signalWorkflow(uppercase.RequestSignal, r.options.target_workflow_id, "", .{request}) catch |err| {
            std.log.err("Signaling request {s} to workflow {s} failed: {t}", .{ request.id, r.options.target_workflow_id, err });
            return switch (err) {
                error.OutOfMemory, error.Canceled => |e| e,
                else => error.SignalFailed,
            };
        };

        const deadline = std.Io.Timestamp.now(r.io, .awake).addDuration(r.options.response_timeout);
        var last_error: ?anyerror = null;
        while (true) {
            // A query can fail while the workflow continues as new, so a failed query is
            // retried until the timeout rather than ending the request.
            if (r.client.queryWorkflow(uppercase.ResponseQuery, arena, r.options.target_workflow_id, "", .{request.id})) |maybe_response| {
                if (maybe_response) |response| {
                    if (response.error_message.len > 0) {
                        std.log.err("Request {s} failed: {s}", .{ request.id, response.error_message });
                        return error.RequestFailed;
                    }
                    return response.output;
                }
                last_error = null;
            } else |err| {
                if (err == error.OutOfMemory) return error.OutOfMemory;
                std.log.warn("Query workflow failed: {t}", .{err});
                last_error = err;
            }

            const remaining = std.Io.Timestamp.now(r.io, .awake).durationTo(deadline);
            if (remaining.nanoseconds <= 0) {
                if (last_error) |err| {
                    std.log.err("No response to request {s} within {f}, last error: {t}", .{ request.id, r.options.response_timeout, err });
                } else {
                    std.log.err("No response to request {s} within {f}", .{ request.id, r.options.response_timeout });
                }
                return error.Timeout;
            }
            const wait: Duration = if (remaining.nanoseconds < r.options.response_query_interval.nanoseconds) remaining else r.options.response_query_interval;
            try r.io.sleep(wait, .awake);
        }
    }
};
