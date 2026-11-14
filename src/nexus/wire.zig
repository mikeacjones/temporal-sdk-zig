//! How a worker's Nexus requests and responses look on the wire: links converted between
//! their Nexus form (a URL and a type) and their Temporal form, and handler results turned
//! into the task completions the core sends to the server.

const std = @import("std");
const nexus = @import("../nexus.zig");
const failure_mod = @import("../failure.zig");
const log_mod = @import("../log.zig");
const proto = @import("../proto.zig");
const Allocator = std.mem.Allocator;

const common = proto.temporal.api.common.v1;
const nexus_pb = proto.temporal.api.nexus.v1;
const EventType = proto.temporal.api.enums.v1.EventType;
pub const Completion = proto.coresdk.nexus.NexusTaskCompletion;
const Failure = failure_mod.Failure;

/// The Nexus link type of a link to a workflow event.
pub const workflow_event_link_type = "temporal.api.common.v1.Link.WorkflowEvent";

const scheme = "temporal://";
const event_type_prefix = "EVENT_TYPE_";

pub const LinkError = error{ OutOfMemory, InvalidLink };

/// The Temporal link a Nexus request's `link` names. Only workflow-event links convert:
/// `temporal:///namespaces/<ns>/workflows/<id>/<run>/history?<reference>`, with path segments
/// percent-encoded and the reference either `referenceType=EventReference` with `eventType`
/// and an optional `eventID`, or `referenceType=RequestIdReference` with `eventType` and an
/// optional `requestID`. Event types may be named in PascalCase (`NexusOperationScheduled`)
/// or as the enum constant (`EVENT_TYPE_NEXUS_OPERATION_SCHEDULED`). On
/// `error.InvalidLink`, `why` says what is wrong. Memory comes from `a`.
pub fn fromNexusLink(a: Allocator, link: nexus_pb.Link, why: *[]const u8) LinkError!common.Link {
    if (!std.mem.eql(u8, link.type, workflow_event_link_type))
        return invalid(why, "its type is not " ++ workflow_event_link_type);
    if (!std.mem.startsWith(u8, link.url, scheme)) return invalid(why, "its URL doesn't start with " ++ scheme);
    const rest = link.url[scheme.len..];
    const path_start = std.mem.indexOfScalar(u8, rest, '/') orelse return invalid(why, "its URL has no path");
    const after_host = rest[path_start..];
    const fragment_start = std.mem.indexOfScalar(u8, after_host, '#') orelse after_host.len;
    const no_fragment = after_host[0..fragment_start];
    const query_start = std.mem.indexOfScalar(u8, no_fragment, '?');
    const path = no_fragment[0 .. query_start orelse no_fragment.len];
    const query = if (query_start) |q| no_fragment[q + 1 ..] else "";

    var segments: [8][]const u8 = undefined;
    var count: usize = 0;
    var it = std.mem.splitScalar(u8, path, '/');
    while (it.next()) |segment| {
        if (count == segments.len) return invalid(why, "its path is not /namespaces/<namespace>/workflows/<workflow ID>/<run ID>/history");
        segments[count] = segment;
        count += 1;
    }
    if (count != 7 or segments[0].len != 0 or !std.mem.eql(u8, segments[1], "namespaces") or
        !std.mem.eql(u8, segments[3], "workflows") or !std.mem.eql(u8, segments[6], "history") or
        segments[2].len == 0 or segments[4].len == 0 or segments[5].len == 0)
        return invalid(why, "its path is not /namespaces/<namespace>/workflows/<workflow ID>/<run ID>/history");

    var reference_type: ?[]const u8 = null;
    var event_type: ?[]const u8 = null;
    var event_id: ?[]const u8 = null;
    var request_id: ?[]const u8 = null;
    var params = std.mem.splitScalar(u8, query, '&');
    while (params.next()) |param| {
        if (param.len == 0) continue;
        const eq = std.mem.indexOfScalar(u8, param, '=') orelse param.len;
        const key = try decodeComponent(a, param[0..eq], true, why);
        const value = try decodeComponent(a, if (eq < param.len) param[eq + 1 ..] else "", true, why);
        const slot: *?[]const u8 = if (std.mem.eql(u8, key, "referenceType"))
            &reference_type
        else if (std.mem.eql(u8, key, "eventType"))
            &event_type
        else if (std.mem.eql(u8, key, "eventID"))
            &event_id
        else if (std.mem.eql(u8, key, "requestID"))
            &request_id
        else
            continue;
        if (slot.* != null) {
            why.* = try std.fmt.allocPrint(a, "its query names {s} more than once", .{key});
            return error.InvalidLink;
        }
        slot.* = value;
    }

    var event: common.Link.WorkflowEvent = .{
        .namespace = try decodeComponent(a, segments[2], false, why),
        .workflow_id = try decodeComponent(a, segments[4], false, why),
        .run_id = try decodeComponent(a, segments[5], false, why),
    };
    const ref = reference_type orelse return invalid(why, "its query has no referenceType");
    const type_name = event_type orelse return invalid(why, "its query has no eventType");
    const parsed_type = try parseEventType(a, type_name, why);
    if (std.mem.eql(u8, ref, "EventReference")) {
        const id = if (event_id) |text| parseEventId(text) orelse {
            why.* = try std.fmt.allocPrint(a, "its eventID \"{s}\" is not an integer", .{text});
            return error.InvalidLink;
        } else 0;
        event.reference = .{ .event_ref = .{ .event_id = id, .event_type = parsed_type } };
    } else if (std.mem.eql(u8, ref, "RequestIdReference")) {
        event.reference = .{ .request_id_ref = .{ .request_id = request_id orelse "", .event_type = parsed_type } };
    } else {
        why.* = try std.fmt.allocPrint(a, "its referenceType \"{s}\" is neither EventReference nor RequestIdReference", .{ref});
        return error.InvalidLink;
    }
    return .{ .variant = .{ .workflow_event = event } };
}

/// The Nexus form of `link`, as `fromNexusLink` reads it: segments percent-encoded except for
/// unreserved characters; for an event reference the query `eventID` (when not 0),
/// `eventType`, `referenceType`; for a request-ID reference `referenceType`, `requestID` (when
/// not empty), `eventType`; event types in PascalCase. Only workflow-event links convert;
/// on `error.InvalidLink`, `why` says why. Memory comes from `a`.
pub fn toNexusLink(a: Allocator, link: common.Link, why: *[]const u8) LinkError!nexus_pb.Link {
    const variant = link.variant orelse return invalid(why, "it has no target");
    const event = switch (variant) {
        .workflow_event => |e| e,
        else => |v| {
            why.* = try std.fmt.allocPrint(a, "it is a {t} link; only workflow-event links are sent", .{v});
            return error.InvalidLink;
        },
    };
    var url: std.Io.Writer.Allocating = .init(a);
    const w = &url.writer;
    write: {
        w.writeAll(scheme ++ "/namespaces/") catch break :write;
        encodeComponent(w, event.namespace, false) catch break :write;
        w.writeAll("/workflows/") catch break :write;
        encodeComponent(w, event.workflow_id, false) catch break :write;
        w.writeByte('/') catch break :write;
        encodeComponent(w, event.run_id, false) catch break :write;
        w.writeAll("/history") catch break :write;
        if (event.reference) |ref| switch (ref) {
            .event_ref => |r| {
                const name = try eventTypeParam(a, r.event_type, why);
                w.writeByte('?') catch break :write;
                if (r.event_id != 0) w.print("eventID={d}&", .{r.event_id}) catch break :write;
                w.writeAll("eventType=") catch break :write;
                encodeComponent(w, name, true) catch break :write;
                w.writeAll("&referenceType=EventReference") catch break :write;
            },
            .request_id_ref => |r| {
                const name = try eventTypeParam(a, r.event_type, why);
                w.writeAll("?referenceType=RequestIdReference") catch break :write;
                if (r.request_id.len > 0) {
                    w.writeAll("&requestID=") catch break :write;
                    encodeComponent(w, r.request_id, true) catch break :write;
                }
                w.writeAll("&eventType=") catch break :write;
                encodeComponent(w, name, true) catch break :write;
            },
        };
        return .{ .url = url.written(), .type = workflow_event_link_type };
    }
    return error.OutOfMemory;
}

/// Digits only: `std.fmt.parseInt` alone also takes a sign and underscores.
fn parseEventId(text: []const u8) ?i64 {
    for (text) |c| if (!std.ascii.isDigit(c)) return null;
    return std.fmt.parseInt(i64, text, 10) catch null;
}

fn invalid(why: *[]const u8, comptime reason: []const u8) error{InvalidLink} {
    why.* = reason;
    return error.InvalidLink;
}

fn isUnreserved(c: u8) bool {
    return std.ascii.isAlphanumeric(c) or c == '-' or c == '.' or c == '_' or c == '~';
}

/// Percent-encodes `text`; in a query, a space is `+`.
fn encodeComponent(w: *std.Io.Writer, text: []const u8, query: bool) std.Io.Writer.Error!void {
    for (text) |c| {
        if (isUnreserved(c)) {
            try w.writeByte(c);
        } else if (query and c == ' ') {
            try w.writeByte('+');
        } else {
            try w.print("%{X:0>2}", .{c});
        }
    }
}

/// Percent-decodes `text`; in a query, `+` is a space.
fn decodeComponent(a: Allocator, text: []const u8, query: bool, why: *[]const u8) LinkError![]const u8 {
    if (std.mem.indexOfAny(u8, text, if (query) "%+" else "%") == null) return text;
    const out = try a.alloc(u8, text.len);
    var n: usize = 0;
    var i: usize = 0;
    while (i < text.len) : (i += 1) {
        const c = text[i];
        if (c == '%') {
            if (i + 2 >= text.len) return invalid(why, "it holds a truncated percent-encoding");
            const high = std.fmt.charToDigit(text[i + 1], 16) catch return invalid(why, "it holds an invalid percent-encoding");
            const low = std.fmt.charToDigit(text[i + 2], 16) catch return invalid(why, "it holds an invalid percent-encoding");
            out[n] = high << 4 | low;
            i += 2;
        } else {
            out[n] = if (query and c == '+') ' ' else c;
        }
        n += 1;
    }
    return out[0..n];
}

/// `t` as a link names it: `EVENT_TYPE_NEXUS_OPERATION_SCHEDULED` is `NexusOperationScheduled`.
fn eventTypeParam(a: Allocator, t: EventType, why: *[]const u8) LinkError![]const u8 {
    const name = std.enums.tagName(EventType, t) orelse {
        why.* = try std.fmt.allocPrint(a, "its event type {d} is not one this SDK knows", .{@backingInt(t)});
        return error.InvalidLink;
    };
    const words = if (std.mem.startsWith(u8, name, event_type_prefix)) name[event_type_prefix.len..] else name;
    var out = try a.alloc(u8, words.len);
    var n: usize = 0;
    var upper = true;
    for (words) |c| {
        if (c == '_') {
            upper = true;
            continue;
        }
        out[n] = if (upper) std.ascii.toUpper(c) else std.ascii.toLower(c);
        upper = false;
        n += 1;
    }
    return out[0..n];
}

/// The event type a link names, in PascalCase or as the enum constant.
fn parseEventType(a: Allocator, name: []const u8, why: *[]const u8) LinkError!EventType {
    const constant = if (std.mem.startsWith(u8, name, event_type_prefix)) name else constant: {
        if (name.len < 2 or !std.ascii.isUpper(name[0]) or !std.ascii.isLower(name[1])) break :constant null;
        var out: std.ArrayList(u8) = .empty;
        try out.appendSlice(a, event_type_prefix);
        for (name, 0..) |c, i| {
            if (std.ascii.isUpper(c) and i > 0) try out.append(a, '_');
            try out.append(a, std.ascii.toUpper(c));
        }
        break :constant out.items;
    };
    if (constant) |c| if (std.meta.stringToEnum(EventType, c)) |t| return t;
    why.* = try std.fmt.allocPrint(a, "its eventType \"{s}\" is not an event type", .{name});
    return error.InvalidLink;
}

// ---------------------------------------------------------------- completions

/// The failure a handler error is sent as: its message, a Nexus handler failure info with
/// its type and retry behavior, and its cause as an application failure. Memory comes from
/// `a`, or is borrowed from `e`.
pub fn handlerFailure(a: Allocator, e: nexus.HandlerError) error{OutOfMemory}!Failure {
    var f: Failure = .{
        .message = e.message,
        .source = failure_mod.source,
        .failure_info = .{ .nexus_handler_failure_info = .{
            .type = e.type.wireName(),
            .retry_behavior = switch (e.retry) {
                .default => .NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_UNSPECIFIED,
                .retryable => .NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_RETRYABLE,
                .non_retryable => .NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_NON_RETRYABLE,
            },
        } },
    };
    if (e.cause) |cause| {
        const c = try a.create(Failure);
        c.* = try failure_mod.application(a, cause);
        f.cause = c;
    }
    return f;
}

/// The failure an operation that ran and failed or was canceled is sent as: a non-retryable
/// application failure with the handler's message, type and details, or a canceled failure
/// with its message and details. The core requires one of these two kinds.
pub fn operationFailure(a: Allocator, e: nexus.OperationError) error{OutOfMemory}!Failure {
    switch (e.state) {
        .failed => {
            var app = e.failure;
            app.non_retryable = true;
            return failure_mod.application(a, app);
        },
        .canceled => {
            var details: ?common.Payloads = null;
            if (e.failure.details.len > 0) {
                details = .{};
                try details.?.payloads.appendSlice(a, e.failure.details);
            }
            return .{
                .message = e.failure.message,
                .source = failure_mod.source,
                .failure_info = .{ .canceled_failure_info = .{ .details = details } },
            };
        },
    }
}

/// The completion of a start request that came to `result`. A started operation's links are
/// converted to their Nexus form; one that doesn't convert is dropped and logged to `logger`.
/// Memory comes from `a`, or is borrowed from `token` and `result`.
pub fn startCompletion(a: Allocator, token: []const u8, result: nexus.StartResult, logger: log_mod.Logger) error{OutOfMemory}!Completion {
    var response: nexus_pb.StartOperationResponse = .{};
    switch (result) {
        .succeeded => |s| response.variant = .{ .sync_success = .{ .payload = s.result } },
        .started => |s| {
            var started: nexus_pb.StartOperationResponse.Async = .{ .operation_token = s.token };
            for (s.links) |link| {
                var why: []const u8 = "";
                const converted = toNexusLink(a, link, &why) catch |err| switch (err) {
                    error.OutOfMemory => return error.OutOfMemory,
                    error.InvalidLink => {
                        logger.warn("Dropping a link of the started Nexus operation that can't be sent", .{ .Reason = why });
                        continue;
                    },
                };
                try started.links.append(a, converted);
            }
            response.variant = .{ .async_success = started };
        },
        .operation_error => |e| response.variant = .{ .failure = try operationFailure(a, e) },
        .handler_error => |e| return failedCompletion(a, token, e),
    }
    return .{ .task_token = token, .status = .{ .completed = .{ .variant = .{ .start_operation = response } } } };
}

/// The completion of a cancel request that came to `result`. Memory as `startCompletion`.
pub fn cancelCompletion(a: Allocator, token: []const u8, result: nexus.CancelResult) error{OutOfMemory}!Completion {
    return switch (result) {
        .requested => .{ .task_token = token, .status = .{ .completed = .{ .variant = .{ .cancel_operation = .{} } } } },
        .handler_error => |e| failedCompletion(a, token, e),
    };
}

/// The completion of a request that failed with handler error `e`.
pub fn failedCompletion(a: Allocator, token: []const u8, e: nexus.HandlerError) error{OutOfMemory}!Completion {
    return .{ .task_token = token, .status = .{ .failure = try handlerFailure(a, e) } };
}

/// The completion acknowledging a cancel from the core: nothing is sent to the server.
pub fn ackCancel(token: []const u8) Completion {
    return .{ .task_token = token, .status = .{ .ack_cancel = true } };
}

/// The longest `encodeAckCancel` writes; task tokens are far shorter.
pub const max_ack_cancel_len = 4096;

/// `ackCancel(token)` encoded into `buffer` without allocating, for a worker out of memory;
/// null when it doesn't fit.
pub fn encodeAckCancel(buffer: *[max_ack_cancel_len]u8, token: []const u8) ?[]const u8 {
    var writer: std.Io.Writer = .fixed(buffer);
    ackCancel(token).encode(&writer, .failing) catch return null;
    return writer.buffered();
}

/// Whether a request that came to `e` was abandoned because of the cancel `ctx` records: the
/// handler returned `error.Canceled`, or an error whose cause it is, after the worker canceled it.
pub fn canceledByWorker(ctx: *const nexus.Context, e: nexus.HandlerError) bool {
    if (ctx.cancelReason() == null) return false;
    const cause = e.cause orelse return false;
    return std.mem.eql(u8, cause.type, @errorName(error.Canceled));
}

// ---------------------------------------------------------------- tests

const testing = std.testing;

fn eventLink(namespace: []const u8, workflow_id: []const u8, run_id: []const u8, reference: ?common.Link.WorkflowEvent.reference_union) common.Link {
    return .{ .variant = .{ .workflow_event = .{ .namespace = namespace, .workflow_id = workflow_id, .run_id = run_id, .reference = reference } } };
}

test "workflow-event links convert to the shared URL form and back" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const cases = [_]struct { link: common.Link, url: []const u8 }{
        .{
            .link = eventLink("ns", "wid", "rid", .{ .request_id_ref = .{ .request_id = "req-123", .event_type = .EVENT_TYPE_WORKFLOW_TASK_COMPLETED } }),
            .url = "temporal:///namespaces/ns/workflows/wid/rid/history?referenceType=RequestIdReference&requestID=req-123&eventType=WorkflowTaskCompleted",
        },
        .{
            .link = eventLink("ns2", "wid2", "rid2", .{ .event_ref = .{ .event_id = 42, .event_type = .EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED } }),
            .url = "temporal:///namespaces/ns2/workflows/wid2/rid2/history?eventID=42&eventType=WorkflowExecutionCompleted&referenceType=EventReference",
        },
        .{
            .link = eventLink("ns2", "wid/2", "rid2", .{ .event_ref = .{ .event_id = 42, .event_type = .EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED } }),
            .url = "temporal:///namespaces/ns2/workflows/wid%2F2/rid2/history?eventID=42&eventType=WorkflowExecutionCompleted&referenceType=EventReference",
        },
        // An unset event ID and request ID are left out.
        .{
            .link = eventLink("default", "a b+c&d", "r", .{ .event_ref = .{ .event_type = .EVENT_TYPE_NEXUS_OPERATION_SCHEDULED } }),
            .url = "temporal:///namespaces/default/workflows/a%20b%2Bc%26d/r/history?eventType=NexusOperationScheduled&referenceType=EventReference",
        },
        .{
            .link = eventLink("default", "w", "r", .{ .request_id_ref = .{ .event_type = .EVENT_TYPE_WORKFLOW_EXECUTION_STARTED } }),
            .url = "temporal:///namespaces/default/workflows/w/r/history?referenceType=RequestIdReference&eventType=WorkflowExecutionStarted",
        },
        .{
            .link = eventLink("d\u{e9}j\u{e0}", "~w.-_", "r", .{ .request_id_ref = .{ .request_id = "id with space", .event_type = .EVENT_TYPE_WORKFLOW_EXECUTION_STARTED } }),
            .url = "temporal:///namespaces/d%C3%A9j%C3%A0/workflows/~w.-_/r/history?referenceType=RequestIdReference&requestID=id+with+space&eventType=WorkflowExecutionStarted",
        },
    };
    for (cases) |case| {
        var why: []const u8 = "";
        const nexus_link = try toNexusLink(a, case.link, &why);
        try testing.expectEqualStrings(case.url, nexus_link.url);
        try testing.expectEqualStrings(workflow_event_link_type, nexus_link.type);
        const back = try fromNexusLink(a, nexus_link, &why);
        const want = case.link.variant.?.workflow_event;
        const got = back.variant.?.workflow_event;
        try testing.expectEqualStrings(want.namespace, got.namespace);
        try testing.expectEqualStrings(want.workflow_id, got.workflow_id);
        try testing.expectEqualStrings(want.run_id, got.run_id);
        switch (want.reference.?) {
            .event_ref => |r| {
                try testing.expectEqual(r.event_id, got.reference.?.event_ref.event_id);
                try testing.expectEqual(r.event_type, got.reference.?.event_ref.event_type);
            },
            .request_id_ref => |r| {
                try testing.expectEqualStrings(r.request_id, got.reference.?.request_id_ref.request_id);
                try testing.expectEqual(r.event_type, got.reference.?.request_id_ref.event_type);
            },
        }
    }
}

test "Nexus links in other spellings parse, and malformed ones are rejected with a reason" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var why: []const u8 = "";
    // The enum constant as the event type, a host, parameters in any order, and unknown ones.
    const constant = try fromNexusLink(a, .{
        .url = "temporal://host/namespaces/ns/workflows/w/r/history?x=1&eventID=7&referenceType=EventReference&eventType=EVENT_TYPE_NEXUS_OPERATION_SCHEDULED",
        .type = workflow_event_link_type,
    }, &why);
    try testing.expectEqual(EventType.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED, constant.variant.?.workflow_event.reference.?.event_ref.event_type);
    try testing.expectEqual(7, constant.variant.?.workflow_event.reference.?.event_ref.event_id);

    const bad = [_]nexus_pb.Link{
        .{ .url = "temporal:///namespaces/ns/workflows/w/r/history?referenceType=EventReference&eventType=WorkflowExecutionStarted", .type = "temporal.api.common.v1.Link.Workflow" },
        .{ .url = "http:///namespaces/ns/workflows/w/r/history?referenceType=EventReference&eventType=WorkflowExecutionStarted", .type = workflow_event_link_type },
        .{ .url = "temporal:///namespaces/ns/workflows/w/r?referenceType=EventReference&eventType=WorkflowExecutionStarted", .type = workflow_event_link_type },
        .{ .url = "temporal:///namespaces/ns/workflows/w//history?referenceType=EventReference&eventType=WorkflowExecutionStarted", .type = workflow_event_link_type },
        .{ .url = "temporal:///namespaces/ns/activities/w/r/history?referenceType=EventReference&eventType=WorkflowExecutionStarted", .type = workflow_event_link_type },
        .{ .url = "temporal:///namespaces/ns/workflows/w/r/history/extra?referenceType=EventReference&eventType=WorkflowExecutionStarted", .type = workflow_event_link_type },
        .{ .url = "temporal:///namespaces/ns/workflows/w/r/history", .type = workflow_event_link_type },
        .{ .url = "temporal:///namespaces/ns/workflows/w/r/history?referenceType=Other&eventType=WorkflowExecutionStarted", .type = workflow_event_link_type },
        .{ .url = "temporal:///namespaces/ns/workflows/w/r/history?referenceType=EventReference", .type = workflow_event_link_type },
        .{ .url = "temporal:///namespaces/ns/workflows/w/r/history?referenceType=EventReference&eventType=NoSuchEvent", .type = workflow_event_link_type },
        .{ .url = "temporal:///namespaces/ns/workflows/w/r/history?referenceType=EventReference&eventType=workflowExecutionStarted", .type = workflow_event_link_type },
        .{ .url = "temporal:///namespaces/ns/workflows/w/r/history?referenceType=EventReference&eventType=WorkflowExecutionStarted&eventID=x", .type = workflow_event_link_type },
        .{ .url = "temporal:///namespaces/ns/workflows/w/r/history?referenceType=EventReference&referenceType=EventReference&eventType=WorkflowExecutionStarted", .type = workflow_event_link_type },
        .{ .url = "temporal:///namespaces/n%2/workflows/w/r/history?referenceType=EventReference&eventType=WorkflowExecutionStarted", .type = workflow_event_link_type },
        .{ .url = "temporal:///namespaces/n%zz/workflows/w/r/history?referenceType=EventReference&eventType=WorkflowExecutionStarted", .type = workflow_event_link_type },
        .{ .url = "temporal:///namespaces/n%+4/workflows/w/r/history?referenceType=EventReference&eventType=WorkflowExecutionStarted", .type = workflow_event_link_type },
        .{ .url = "temporal:///namespaces/ns/workflows/w/r/history?referenceType=EventReference&eventType=WorkflowExecutionStarted&eventID=%2B1_0", .type = workflow_event_link_type },
        .{ .url = "temporal:", .type = workflow_event_link_type },
        .{ .url = "", .type = workflow_event_link_type },
    };
    for (bad) |link| {
        why = "";
        try testing.expectError(error.InvalidLink, fromNexusLink(a, link, &why));
        try testing.expect(why.len > 0);
    }

    // Only workflow-event links are sent.
    why = "";
    try testing.expectError(error.InvalidLink, toNexusLink(a, .{ .variant = .{ .workflow = .{ .namespace = "ns", .workflow_id = "w" } } }, &why));
    try testing.expect(std.mem.indexOf(u8, why, "workflow link") != null);
    try testing.expectError(error.InvalidLink, toNexusLink(a, .{}, &why));
    try testing.expectError(error.InvalidLink, toNexusLink(a, eventLink("ns", "w", "r", .{ .event_ref = .{ .event_type = @fromBackingInt(@intCast(9999)) } }), &why));
}

test "every start result maps to the completion the core expects" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const logger: log_mod.Logger = .{};

    const sync = try startCompletion(a, "tt", .{ .succeeded = .{ .result = .{ .data = "\"hi\"" } } }, logger);
    try testing.expectEqualStrings("tt", sync.task_token);
    try testing.expectEqualStrings("\"hi\"", sync.status.?.completed.variant.?.start_operation.variant.?.sync_success.payload.?.data);

    const links = [_]common.Link{
        eventLink("default", "handler", "run-1", .{ .request_id_ref = .{ .request_id = "req", .event_type = .EVENT_TYPE_WORKFLOW_EXECUTION_STARTED } }),
        .{ .variant = .{ .workflow = .{ .namespace = "default", .workflow_id = "dropped" } } },
    };
    const started = try startCompletion(a, "tt", .{ .started = .{ .token = "op-token", .links = &links } }, logger);
    const async_success = started.status.?.completed.variant.?.start_operation.variant.?.async_success;
    try testing.expectEqualStrings("op-token", async_success.operation_token);
    try testing.expectEqual(1, async_success.links.items.len);
    try testing.expectEqualStrings("temporal:///namespaces/default/workflows/handler/run-1/history?referenceType=RequestIdReference&requestID=req&eventType=WorkflowExecutionStarted", async_success.links.items[0].url);

    const failed = try startCompletion(a, "tt", .{ .operation_error = .{ .state = .failed, .failure = .{ .message = "out of stock", .type = "OutOfStock" } } }, logger);
    const failed_failure = failed.status.?.completed.variant.?.start_operation.variant.?.failure;
    try testing.expectEqualStrings("out of stock", failed_failure.message);
    try testing.expectEqualStrings("OutOfStock", failed_failure.failure_info.?.application_failure_info.type);
    try testing.expect(failed_failure.failure_info.?.application_failure_info.non_retryable);

    const canceled = try startCompletion(a, "tt", .{ .operation_error = .{ .state = .canceled, .failure = .{ .message = "stopped" } } }, logger);
    const canceled_failure = canceled.status.?.completed.variant.?.start_operation.variant.?.failure;
    try testing.expectEqualStrings("stopped", canceled_failure.message);
    try testing.expect(canceled_failure.failure_info.? == .canceled_failure_info);

    const handler = try startCompletion(a, "tt", .{ .handler_error = .{ .type = .bad_request, .message = "no", .retry = .non_retryable, .cause = .{ .message = "why", .type = "Why" } } }, logger);
    const handler_failure = handler.status.?.failure;
    try testing.expectEqualStrings("no", handler_failure.message);
    try testing.expectEqualStrings("BAD_REQUEST", handler_failure.failure_info.?.nexus_handler_failure_info.type);
    try testing.expectEqual(.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_NON_RETRYABLE, handler_failure.failure_info.?.nexus_handler_failure_info.retry_behavior);
    try testing.expectEqualStrings("Why", handler_failure.cause.?.failure_info.?.application_failure_info.type);

    const retried = try failedCompletion(a, "tt", .{ .type = .internal, .message = "later", .retry = .retryable });
    try testing.expectEqual(.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_RETRYABLE, retried.status.?.failure.failure_info.?.nexus_handler_failure_info.retry_behavior);
    try testing.expect(retried.status.?.failure.cause == null);
    const default = try failedCompletion(a, "tt", .{ .type = .unavailable, .message = "later" });
    try testing.expectEqual(.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_UNSPECIFIED, default.status.?.failure.failure_info.?.nexus_handler_failure_info.retry_behavior);

    try testing.expect(ackCancel("tt").status.?.ack_cancel);
}

test "every cancel result maps to the completion the core expects" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const requested = try cancelCompletion(a, "tt", .requested);
    try testing.expect(requested.status.?.completed.variant.? == .cancel_operation);
    const refused = try cancelCompletion(a, "tt", .{ .handler_error = .{ .type = .not_implemented, .message = "sync" } });
    try testing.expectEqualStrings("NOT_IMPLEMENTED", refused.status.?.failure.failure_info.?.nexus_handler_failure_info.type);
}

test "a request counts as abandoned only when the worker canceled it and it returned Canceled" {
    var ctx: nexus.Context = .{ .arena = testing.allocator, .io_ = testing.io, .info_ = .{ .service = "s", .operation = "o" } };
    const canceled: nexus.HandlerError = .{ .type = .internal, .message = "m", .cause = .{ .message = "Canceled", .type = "Canceled" } };
    const other: nexus.HandlerError = .{ .type = .internal, .message = "m", .cause = .{ .message = "Hoarse", .type = "Hoarse" } };
    try testing.expect(!canceledByWorker(&ctx, canceled));
    _ = ctx.setCancelReason(.timed_out);
    try testing.expect(canceledByWorker(&ctx, canceled));
    try testing.expect(!canceledByWorker(&ctx, other));
    try testing.expect(!canceledByWorker(&ctx, .{ .type = .internal, .message = "m" }));
}

test "an acknowledged cancel encodes without allocating, unless its token doesn't fit" {
    var buffer: [max_ack_cancel_len]u8 = undefined;
    const encoded = encodeAckCancel(&buffer, "task-token").?;
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var reader: std.Io.Reader = .fixed(encoded);
    const decoded = try Completion.decode(&reader, arena.allocator());
    try testing.expectEqualStrings("task-token", decoded.task_token);
    try testing.expect(decoded.status.?.ack_cancel);

    const long: [max_ack_cancel_len]u8 = @splat('t');
    try testing.expectEqual(null, encodeAckCancel(&buffer, &long));
}
