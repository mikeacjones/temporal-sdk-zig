//! A synchronous call between two workflows, built from signals. The calling workflow signals
//! a request to the target workflow, naming itself, and then blocks until the target signals a
//! response back to it. Each side receives through a `Channel`, a queue its signal handler
//! fills, so requests and responses that arrive before anyone waits for them are kept in order.

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;

pub const Request = struct {
    key: []const u8,
    value: []const u8,
    /// Where the response goes.
    calling_workflow_id: []const u8,
};

pub const Response = struct {
    key: []const u8 = "",
    value: []const u8 = "",
    /// Why the request failed; empty if it succeeded.
    error_message: []const u8 = "",
};

pub const RequestSignal = workflow.SignalDef("proxy-request-signal", fn (request: Request) void);
pub const ResponseSignal = workflow.SignalDef("proxy-response-signal", fn (response: Response) void);

/// The values `Signal` (a `SignalDef` with one argument of type `T`) delivers, in arrival
/// order. Allocated with `ctx.allocator()` by `init`, which registers the signal's handler:
/// create the channel before anything can need it, so no signal finds the handler missing.
pub fn Channel(comptime Signal: type, comptime T: type) type {
    return struct {
        const Self = @This();

        ctx: *workflow.Context,
        pending: std.ArrayList(T) = .empty,

        pub fn init(ctx: *workflow.Context) !*Self {
            const ch = try ctx.allocator().create(Self);
            ch.* = .{ .ctx = ctx };
            try ctx.setSignalHandler(Signal, ch, deliver);
            return ch;
        }

        fn deliver(ch: *Self, value: T) !void {
            try ch.pending.append(ch.ctx.allocator(), value);
        }

        fn hasPending(ch: *Self) bool {
            return ch.pending.items.len > 0;
        }

        /// Waits for the next value. Returns `error.Canceled` if the workflow is canceled.
        pub fn receive(ch: *Self) !T {
            try ch.ctx.await(ch, hasPending);
            return ch.pending.orderedRemove(0);
        }
    };
}

pub const RequestChannel = Channel(RequestSignal, Request);
pub const ResponseChannel = Channel(ResponseSignal, Response);

/// Signals a request to `target_workflow_id`, naming the calling workflow as the one to answer.
pub fn sendRequest(ctx: *workflow.Context, target_workflow_id: []const u8, key: []const u8, value: []const u8) !void {
    const workflow_id = ctx.info().workflow_id;
    ctx.logger().info("Sending request", .{ .target_workflow_id = target_workflow_id, .workflow_id = workflow_id });
    try ctx.signalExternalWorkflow(RequestSignal, target_workflow_id, "", .{.{
        .key = key,
        .value = value,
        .calling_workflow_id = workflow_id,
    }}).get();
}

/// Waits for a request.
pub fn receiveRequest(ctx: *workflow.Context, requests: *RequestChannel) !Request {
    ctx.logger().info("Waiting for request", .{});
    const request = try requests.receive();
    ctx.logger().info("Received request", .{});
    return request;
}

/// Answers the request from `calling_workflow_id`.
pub fn sendResponse(ctx: *workflow.Context, calling_workflow_id: []const u8, key: []const u8, value: []const u8) !void {
    ctx.logger().info("Sending response", .{ .workflow_id = calling_workflow_id });
    try ctx.signalExternalWorkflow(ResponseSignal, calling_workflow_id, "", .{.{ .key = key, .value = value }}).get();
}

/// Answers the request from `calling_workflow_id` with a failure.
pub fn sendErrorResponse(ctx: *workflow.Context, calling_workflow_id: []const u8, message: []const u8) !void {
    ctx.logger().info("Sending error response", .{ .workflow_id = calling_workflow_id });
    try ctx.signalExternalWorkflow(ResponseSignal, calling_workflow_id, "", .{.{ .error_message = message }}).get();
}

/// Waits for the response to this workflow's request. An error response fails the calling
/// workflow (`error.ApplicationFailure`) with the response's message.
pub fn receiveResponse(ctx: *workflow.Context, responses: *ResponseChannel) !Response {
    ctx.logger().info("Waiting for response", .{});
    const response = try responses.receive();
    ctx.logger().info("Received response", .{});
    if (response.error_message.len > 0) return ctx.fail(.{ .message = response.error_message });
    return response;
}
