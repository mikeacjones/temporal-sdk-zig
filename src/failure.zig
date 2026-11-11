//! Temporal failures and how they meet Zig errors.
//!
//! Zig errors carry no data, while Temporal failures carry a message, a type, details, and a
//! cause chain. The SDK uses both:
//!
//! - Operations return small error sets that say what went wrong at the level the caller
//!   handles: `error.ActivityFailed`, `error.WorkflowFailed`, `error.Canceled`, and so on.
//! - The full failure is read from the handle that failed, after the error:
//!   `future.failure()`, `run.failure()`. It is a `Failure` (the
//!   `temporal.api.failure.v1.Failure` protobuf), allocated with the handle's memory.
//! - Code that raises a failure returns a Zig error. A plain error becomes an application
//!   failure whose type and message are the error's name, so `error.InsufficientFunds`
//!   arrives as type "InsufficientFunds", which other SDKs can match on. To control the
//!   message, type, retryability, or details, set them with `ctx.fail(.{...})` and return
//!   the error it gives back.

const std = @import("std");
const proto = @import("proto.zig");
const converter = @import("converter.zig");
const Allocator = std.mem.Allocator;

pub const Failure = proto.temporal.api.failure.v1.Failure;

/// The source recorded on failures this SDK creates.
pub const source = "ZigSDK";

pub const Application = struct {
    message: []const u8,
    /// Matched by retry policies' non-retryable types and by callers. Empty for none.
    type: []const u8 = "",
    non_retryable: bool = false,
    /// Values attached to the failure, encoded with the default converter.
    details: []const converter.Payload = &.{},
};

/// An application failure. Memory comes from `arena`.
pub fn application(arena: Allocator, a: Application) error{OutOfMemory}!Failure {
    var details: ?converter.Payloads = null;
    if (a.details.len > 0) {
        details = .{};
        try details.?.payloads.appendSlice(arena, a.details);
    }
    return .{
        .message = a.message,
        .source = source,
        .failure_info = .{ .application_failure_info = .{
            .type = a.type,
            .non_retryable = a.non_retryable,
            .details = details,
        } },
    };
}

/// The application failure for a plain Zig error: type and message are the error name.
pub fn fromError(arena: Allocator, err: anyerror) error{OutOfMemory}!Failure {
    return application(arena, .{ .message = @errorName(err), .type = @errorName(err) });
}

/// The application type of `f`, or null if it is not an application failure.
pub fn applicationType(f: Failure) ?[]const u8 {
    const info = f.failure_info orelse return null;
    return switch (info) {
        .application_failure_info => |a| a.type,
        else => null,
    };
}

/// The innermost failure in the cause chain.
pub fn rootCause(f: *const Failure) *const Failure {
    var it = f;
    while (it.cause) |next| it = next;
    return it;
}

/// Writes `message` for each failure in the cause chain, joined by ": " ("activity error (type: Activity, ...): Hello failed").
pub fn format(f: Failure, w: *std.Io.Writer) std.Io.Writer.Error!void {
    var it: ?*const Failure = &f;
    var first = true;
    while (it) |cur| : (it = cur.cause) {
        if (!first) try w.writeAll(": ");
        first = false;
        try w.writeAll(if (cur.message.len > 0) cur.message else kindName(cur.*));
        if (applicationType(cur.*)) |t| if (t.len > 0 and !std.mem.eql(u8, t, cur.message)) {
            try w.print(" ({s})", .{t});
        };
    }
}

fn kindName(f: Failure) []const u8 {
    const info = f.failure_info orelse return "failure";
    return switch (info) {
        .application_failure_info => "application error",
        .timeout_failure_info => "timeout",
        .canceled_failure_info => "canceled",
        .terminated_failure_info => "terminated",
        .server_failure_info => "server error",
        .reset_workflow_failure_info => "reset",
        .activity_failure_info => "activity error",
        .child_workflow_execution_failure_info => "child workflow error",
        .nexus_operation_execution_failure_info => "nexus operation error",
        .nexus_handler_failure_info => "nexus handler error",
    };
}

// ---------------------------------------------------------------- tests

const testing = std.testing;

test "a plain Zig error becomes an application failure named after it" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const f = try fromError(arena.allocator(), error.InsufficientFunds);
    try testing.expectEqualStrings("InsufficientFunds", f.message);
    try testing.expectEqualStrings("InsufficientFunds", applicationType(f).?);
    try testing.expectEqualStrings(source, f.source);
}

test "application failures carry type, retryability and details, and format with causes" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const detail = try converter.toPayload(a, @as(u32, 42));
    var inner = try application(a, .{ .message = "card declined", .type = "PaymentError", .non_retryable = true, .details = &.{detail} });
    var outer: Failure = .{ .message = "activity error", .cause = &inner, .failure_info = .{ .activity_failure_info = .{} } };
    try testing.expect(inner.failure_info.?.application_failure_info.non_retryable);
    try testing.expectEqual(&inner, rootCause(&outer));

    var buf: [128]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try format(outer, &w);
    try testing.expectEqualStrings("activity error: card declined (PaymentError)", w.buffered());
}
