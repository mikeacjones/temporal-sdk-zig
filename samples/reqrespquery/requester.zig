//! Requests uppercasing from a running uppercase workflow: sends each request by signal to the
//! workflow's current run, named by its run ID, and polls that run's "response" query until its
//! answer appears. Responses are read from the run that received the request, even after it
//! continued as new: the next run doesn't have them.

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
        /// Finding the workflow's current run or signaling the request to it failed; why is
        /// logged.
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

    /// How many times `signalCurrentRun` looks up the current run and signals it.
    const signal_attempts = 3;

    /// Asks the workflow to uppercase `input` and returns the result, allocated from `arena`.
    /// The response is read from the run that received the request, even if that run has
    /// continued as new since.
    pub fn requestUppercase(r: *const Requester, arena: std.mem.Allocator, input: []const u8) Error![]const u8 {
        const request: uppercase.Request = .{ .id = try temporal.client.uuid(arena, r.io), .input = input };
        const run_id = try r.signalCurrentRun(arena, request);

        const deadline = std.Io.Timestamp.now(r.io, .awake).addDuration(r.options.response_timeout);
        var last_error: ?anyerror = null;
        while (true) {
            // A query can fail now and then (a closed run is answered by replaying it on a
            // worker), so a failed query is retried until the timeout rather than ending the
            // request.
            if (r.client.queryWorkflow(uppercase.ResponseQuery, arena, r.options.target_workflow_id, run_id, .{request.id})) |maybe_response| {
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

    /// Signals `request` to the workflow's current run, by its run ID, and returns that run ID
    /// (allocated in `arena`). If that run closed between the describe and the signal
    /// (error.NotFound from the signal, because it continued as new), it describes again and
    /// retries, `signal_attempts` times in all.
    ///
    /// Naming the run matters: a run that accepted the signal answers the request before it
    /// continues as new, so the requester knows which run holds the response.
    fn signalCurrentRun(r: *const Requester, arena: std.mem.Allocator, request: uppercase.Request) Error![]const u8 {
        const workflow_id = r.options.target_workflow_id;
        var attempt: u32 = 1;
        while (true) : (attempt += 1) {
            const description = r.client.describeWorkflow(arena, workflow_id, "") catch |err| {
                std.log.err("Describing workflow {s} for request {s} failed: {t}", .{ workflow_id, request.id, err });
                return switch (err) {
                    error.OutOfMemory, error.Canceled => |e| e,
                    else => error.SignalFailed,
                };
            };
            r.client.signalWorkflow(uppercase.RequestSignal, workflow_id, description.run_id, .{request}) catch |err| {
                if (err == error.NotFound and attempt < signal_attempts) {
                    // The run closed since the describe: most likely it continued as new.
                    std.log.info("Run {s} of workflow {s} closed before request {s}; trying its next run", .{ description.run_id, workflow_id, request.id });
                    continue;
                }
                std.log.err("Signaling request {s} to run {s} of workflow {s} failed: {t}", .{ request.id, description.run_id, workflow_id, err });
                return switch (err) {
                    error.OutOfMemory, error.Canceled => |e| e,
                    else => error.SignalFailed,
                };
            };
            return description.run_id;
        }
    }
};
