//! Reads what the server recorded: how each say-hello operation ended for the caller, from
//! the caller workflow's history, and how a canceled handler workflow ended, from its own.
//! The starter prints these, and the tests check them.

const std = @import("std");
const temporal = @import("temporal");
const service = @import("service.zig");

const History = temporal.client.History;

/// A say-hello operation, as the caller's history records it.
pub const Operation = struct {
    language: service.Language,
    /// The workflow the handler started, from the link on the operation's started event;
    /// null if it didn't start.
    handler: ?Workflow = null,
    outcome: Outcome = .pending,

    pub const Workflow = struct { namespace: []const u8, workflow_id: []const u8, run_id: []const u8 };

    /// The first event that resolved the operation for the caller.
    pub const Outcome = union(enum) {
        pending,
        /// The output's message.
        completed: []const u8,
        /// The handler received the cancel request (`wait_cancellation_requested` resolves
        /// here), or the operation ended canceled.
        canceled,
        failed,
        timed_out,
    };
};

/// The say-hello operations in `history` (a caller workflow's), in the order scheduled.
/// Allocated from `arena`, and borrows from `history`.
pub fn operations(arena: std.mem.Allocator, history: History) ![]Operation {
    var list: std.ArrayList(Operation) = .empty;
    var scheduled_ids: std.ArrayList(i64) = .empty;
    for (history.events.items) |event| {
        const attributes = event.attributes orelse continue;
        if (attributes == .nexus_operation_scheduled_event_attributes) {
            const a = attributes.nexus_operation_scheduled_event_attributes;
            if (!std.mem.eql(u8, a.operation, service.SayHello.operation_name)) continue;
            const input = try temporal.converter.fromPayload(service.HelloInput, arena, a.input orelse return error.OperationInputMissing);
            try list.append(arena, .{ .language = input.language });
            try scheduled_ids.append(arena, event.event_id);
            continue;
        }
        const scheduled_id, const outcome: ?Operation.Outcome = switch (attributes) {
            .nexus_operation_started_event_attributes => |a| .{ a.scheduled_event_id, null },
            .nexus_operation_completed_event_attributes => |a| .{ a.scheduled_event_id, .{
                .completed = (try temporal.converter.fromPayload(service.HelloOutput, arena, a.result orelse return error.OperationResultMissing)).message,
            } },
            .nexus_operation_cancel_request_completed_event_attributes => |a| .{ a.scheduled_event_id, .canceled },
            .nexus_operation_canceled_event_attributes => |a| .{ a.scheduled_event_id, .canceled },
            .nexus_operation_failed_event_attributes => |a| .{ a.scheduled_event_id, .failed },
            .nexus_operation_timed_out_event_attributes => |a| .{ a.scheduled_event_id, .timed_out },
            else => continue,
        };
        const i = std.mem.indexOfScalar(i64, scheduled_ids.items, scheduled_id) orelse continue;
        const op = &list.items[i];
        if (outcome) |o| {
            if (op.outcome == .pending) op.outcome = o;
            continue;
        }
        for (event.links.items) |link| switch (link.variant orelse continue) {
            .workflow_event => |w| op.handler = .{ .namespace = w.namespace, .workflow_id = w.workflow_id, .run_id = w.run_id },
            else => {},
        };
    }
    return list.items;
}

/// How long a canceled handler workflow cleaned up, from its `history`: the timers it started
/// after its cancellation was requested, which fired before it ended canceled. Errors:
/// `NotCancelRequested` if no cancellation was requested, `NotEndedCanceled` if it didn't end
/// canceled (or hasn't ended). `arena` holds scratch memory.
pub fn cleanupAfterCancel(arena: std.mem.Allocator, history: History) error{ NotCancelRequested, NotEndedCanceled, OutOfMemory }!std.Io.Duration {
    const Timer = struct { id: []const u8, ms: i64 };
    var cancel_requested = false;
    var timers: std.ArrayList(Timer) = .empty;
    var cleanup_ms: i64 = 0;
    for (history.events.items) |event| switch (event.attributes orelse continue) {
        .workflow_execution_cancel_requested_event_attributes => cancel_requested = true,
        .timer_started_event_attributes => |a| if (cancel_requested) {
            const timeout = a.start_to_fire_timeout orelse continue;
            try timers.append(arena, .{ .id = a.timer_id, .ms = timeout.seconds * std.time.ms_per_s + @divTrunc(timeout.nanos, std.time.ns_per_ms) });
        },
        .timer_fired_event_attributes => |a| for (timers.items) |t| {
            if (std.mem.eql(u8, t.id, a.timer_id)) cleanup_ms += t.ms;
        },
        .workflow_execution_canceled_event_attributes => return if (cancel_requested) .fromMilliseconds(cleanup_ms) else error.NotCancelRequested,
        else => {},
    };
    return if (cancel_requested) error.NotEndedCanceled else error.NotCancelRequested;
}
