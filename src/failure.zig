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
//!   the error it gives back. `cause` puts another failure under it.
//! - When the SDK builds a failure from an error a workflow, handler or activity returned,
//!   `stack_trace` holds the error's return trace (`traceText`), unless
//!   `worker.Options.failure_stack_traces` is off.
//! - `client.Options.encode_failure_attributes` moves `message` and `stack_trace` into
//!   `encoded_attributes` on the way out (`encodeAttributes`), so payload codecs encode them;
//!   `decodeAttributes` restores them on the way in.
//! - `kind` reads a failure as a tagged union of the kinds the server defines, with details
//!   as `converter.EncodedValues`.

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
    category: Category = .unspecified,
    /// Overrides the retry policy's backoff before the next attempt of the activity this
    /// fails. Ignored when the failure isn't retried.
    next_retry_delay: ?std.Io.Duration = null,
    /// The failure that caused this one, such as a failure read from a future
    /// (`future.failure()`). Borrowed only until the call it is given to returns: every
    /// function that takes an `Application` (`ctx.fail`, `Client.failActivity`,
    /// `interceptor.rejectActivity`, ...) copies the chain, so it may live on the stack.
    cause: ?*const Failure = null,

    pub const Category = enum {
        unspecified,
        /// An expected failure, such as polling for something that isn't ready yet: workers
        /// log it at debug level instead of warn, and the server and metrics don't count it
        /// as an error.
        benign,
    };
};

/// An application failure. Memory comes from `arena`; the details list and the cause chain
/// are copied into it, so the failure doesn't borrow them from `a`.
pub fn application(arena: Allocator, a: Application) error{OutOfMemory}!Failure {
    var details: ?converter.Payloads = null;
    if (a.details.len > 0) {
        details = .{};
        try details.?.payloads.appendSlice(arena, a.details);
    }
    return .{
        .message = a.message,
        .source = source,
        .cause = @constCast(try copyChain(arena, a.cause)),
        .failure_info = .{ .application_failure_info = .{
            .type = a.type,
            .non_retryable = a.non_retryable,
            .details = details,
            .next_retry_delay = if (a.next_retry_delay) |d| .{
                .seconds = std.math.cast(i64, @divFloor(d.nanoseconds, std.time.ns_per_s)) orelse std.math.maxInt(i64),
                .nanos = @intCast(@mod(d.nanoseconds, std.time.ns_per_s)),
            } else null,
            .category = switch (a.category) {
                .unspecified => .APPLICATION_ERROR_CATEGORY_UNSPECIFIED,
                .benign => .APPLICATION_ERROR_CATEGORY_BENIGN,
            },
        } },
    };
}

/// The category of `f`, or null if it is not an application failure. A category added by a
/// newer server reads as `unspecified`.
pub fn applicationCategory(f: Failure) ?Application.Category {
    const info = f.failure_info orelse return null;
    return switch (info) {
        .application_failure_info => |a| if (a.category == .APPLICATION_ERROR_CATEGORY_BENIGN) .benign else .unspecified,
        else => null,
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

/// `a` with its details list and cause chain copied into `arena`, for a function that stores
/// `a` past its return (`ctx.fail`, `nexus.Context.handlerError`) before `application` turns
/// it into a failure: both are often built on the caller's stack. The chain is copied whole
/// (`copyChain`); `a`'s own strings and the details' payload data are not copied.
pub fn keep(arena: Allocator, a: Application) error{OutOfMemory}!Application {
    var kept = a;
    kept.details = try arena.dupe(converter.Payload, a.details);
    kept.cause = try copyChain(arena, a.cause);
    return kept;
}

/// A deep copy of the cause chain `cause` in `arena`, strings and payloads included. Null
/// stays null. Walks the chain in a loop, one failure at a time, so its stack use doesn't
/// grow with the chain's length: it runs on workflow coroutine stacks (`ctx.fail`).
pub fn copyChain(arena: Allocator, cause: ?*const Failure) error{OutOfMemory}!?*const Failure {
    const first = cause orelse return null;
    var head: ?*Failure = null;
    var tail: *?*Failure = &head;
    var it: ?*const Failure = first;
    while (it) |f| : (it = f.cause) {
        var node = f.*;
        // Copied below, iteratively: the generated dupe would recurse down the chain.
        node.cause = null;
        const copy = try arena.create(Failure);
        copy.* = try node.dupe(arena);
        tail.* = copy;
        tail = &copy.cause;
    }
    return head;
}

// ---------------------------------------------------------------- stack traces

/// The error return trace `trace` as text, one line per frame that debug info resolves to a
/// source location: "path/to/file.zig:12:5: in function_name". Memory comes from `arena`.
/// Empty when `trace` is null (a build without error return tracing), when the target has no
/// debug info to read, or when no frame resolves; frames without a source location are
/// left out, so the text never holds bare addresses. Reads the executable's debug info, which
/// takes file I/O and stack the first time: call it on a thread's own stack, not a
/// workflow coroutine's. A cancel pending on the calling thread doesn't interrupt it and stays
/// pending.
pub fn traceText(arena: Allocator, trace: ?*const std.builtin.StackTrace) error{OutOfMemory}![]const u8 {
    const t = trace orelse return "";
    return addressesText(arena, t.instruction_addresses[0..@min(t.instruction_addresses.len, t.index)]);
}

/// `traceText` for return addresses copied out of an error return trace.
pub fn addressesText(arena: Allocator, addresses: []const usize) error{OutOfMemory}![]const u8 {
    if (comptime !std.options.allow_stack_tracing or std.debug.SelfInfo == void) return "";
    if (addresses.len == 0) return "";
    const di = std.debug.getSelfDebugInfo() catch return "";
    const io = std.Options.debug_io;
    // As std's own trace printers do: a cancel pending on this thread (an activity's) would
    // otherwise fail the debug-info file open, and std caches that failure, losing every
    // later stack trace in the process.
    const prev = io.swapCancelProtection(.blocked);
    defer _ = io.swapCancelProtection(prev);
    // Not std's shared debug-info allocator: its arena frees only its latest allocation, so
    // scratch memory of concurrent callers would stay behind.
    var text: std.heap.ArenaAllocator = .init(std.heap.page_allocator);
    defer text.deinit();
    var out: std.Io.Writer.Allocating = .init(arena);
    errdefer out.deinit();
    for (addresses) |addr| {
        defer _ = text.reset(.retain_capacity);
        const ta = text.allocator();
        var symbols: std.ArrayList(std.debug.Symbol) = .empty;
        // A return address is just past the call: step back into the call for its line.
        di.getSymbols(io, ta, ta, addr -| 1, true, &symbols) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            else => continue,
        };
        for (symbols.items) |sym| {
            const loc = sym.source_location orelse continue;
            if (loc.file_name.len == 0) continue;
            out.writer.print("{s}:{d}:{d}: in {s}\n", .{ loc.file_name, loc.line, loc.column, sym.name orelse "???" }) catch return error.OutOfMemory;
        }
    }
    return out.toOwnedSlice() catch return error.OutOfMemory;
}

/// Starts an empty error return trace for one call into user code on a thread whose trace
/// may already hold frames: a pooled thread keeps its trace from one task to the next, so
/// without this an activity's trace would start with frames of whatever failed on the
/// thread before (or have no room left for its own, as frames past the trace's capacity are
/// dropped). `end` puts the thread's trace back as it was. A no-op without error return
/// tracing.
pub const TraceScope = struct {
    trace: ?*std.builtin.StackTrace,
    index: usize = 0,
    saved: [max_saved]usize = undefined,

    const max_saved = 64;

    pub fn begin(trace: ?*std.builtin.StackTrace) TraceScope {
        var s: TraceScope = .{ .trace = trace };
        const t = trace orelse return s;
        s.index = t.index;
        const n = @min(t.instruction_addresses.len, max_saved);
        @memcpy(s.saved[0..n], t.instruction_addresses[0..n]);
        t.index = 0;
        return s;
    }

    pub fn end(s: *const TraceScope) void {
        const t = s.trace orelse return;
        const n = @min(t.instruction_addresses.len, max_saved);
        @memcpy(t.instruction_addresses[0..n], s.saved[0..n]);
        t.index = s.index;
    }
};

/// The return addresses of the current error return trace, copied into `arena`, or empty
/// when the build has no error return tracing. Call it where the error is caught, before
/// anything else returns an error.
pub fn captureTrace(arena: Allocator, trace: ?*const std.builtin.StackTrace) error{OutOfMemory}![]const usize {
    const t = trace orelse return &.{};
    return arena.dupe(usize, t.instruction_addresses[0..@min(t.instruction_addresses.len, t.index)]);
}

// ---------------------------------------------------------------- encoded attributes

/// What `encodeAttributes` writes in `encoded_attributes`: a `json/plain` payload of this
/// object, the format other Temporal SDKs read and write.
pub const EncodedAttributes = struct {
    message: []const u8 = "",
    stack_trace: []const u8 = "",
};

/// The message a failure with encoded attributes carries in the clear.
pub const encoded_message = "Encoded failure";

/// Moves `message` and `stack_trace` of every failure in `msg` (a generated protobuf message,
/// or a `Failure`), and of every cause in each chain, into `encoded_attributes`
/// (`EncodedAttributes`), leaving the message "Encoded failure" and the stack trace empty.
/// Run before the payload codecs, so they encode the attributes. Copies into `arena` what it
/// changes and every message on the way to it; memory `msg` borrows is never written.
pub fn encodeAttributes(comptime T: type, msg: *T, arena: Allocator) error{OutOfMemory}!void {
    return visitFailures(T, msg, arena, true);
}

/// Undoes `encodeAttributes` for every failure in `msg`: one whose `encoded_attributes`
/// decodes as `EncodedAttributes` gets its message and stack trace back and loses the
/// payload; one that doesn't (no attributes, another encoding, a codec this side lacks) is
/// left as received. Run after the payload codecs. Memory as `encodeAttributes`.
pub fn decodeAttributes(comptime T: type, msg: *T, arena: Allocator) error{OutOfMemory}!void {
    return visitFailures(T, msg, arena, false);
}

fn visitFailures(comptime T: type, value: *T, arena: Allocator, encode: bool) error{OutOfMemory}!void {
    if (comptime !mayHoldFailures(T)) return;
    if (T == Failure) return visitChain(value, arena, encode);
    switch (@typeInfo(T)) {
        .optional => |o| if (value.*) |*inner| try visitFailures(o.child, inner, arena, encode),
        .pointer => |p| {
            const copy = try arena.create(p.child);
            copy.* = value.*.*;
            try visitFailures(p.child, copy, arena, encode);
            value.* = copy;
        },
        .@"union" => switch (value.*) {
            inline else => |*field| try visitFailures(@TypeOf(field.*), field, arena, encode),
        },
        .@"struct" => |s| if (comptime isArrayList(T)) {
            const E = @typeInfo(@FieldType(T, "items")).pointer.child;
            const items = try arena.dupe(E, value.items);
            for (items) |*item| try visitFailures(E, item, arena, encode);
            value.* = .fromOwnedSlice(items);
        } else {
            inline for (s.field_names) |name| try visitFailures(@FieldType(T, name), &@field(value, name), arena, encode);
        },
        else => {},
    }
}

/// Encodes or decodes the attributes of `f` and of each cause under it, in a loop, so stack
/// use doesn't grow with the chain's length. Each cause is copied into `arena` before it is
/// changed: the chain `f` points to may be borrowed.
fn visitChain(f: *Failure, arena: Allocator, encode: bool) error{OutOfMemory}!void {
    comptime for (@typeInfo(Failure).@"struct".field_names) |name| {
        // Only `cause` may lead to another failure; the loop below relies on it.
        if (!std.mem.eql(u8, name, "cause") and mayHoldFailures(@FieldType(Failure, name))) @compileError("Failure." ++ name ++ " can hold a failure; visitChain must visit it");
    };
    var cur = f;
    while (true) {
        if (encode) try encodeOne(cur, arena) else try decodeOne(cur, arena);
        const next = cur.cause orelse return;
        const copy = try arena.create(Failure);
        copy.* = next.*;
        cur.cause = copy;
        cur = copy;
    }
}

fn encodeOne(f: *Failure, arena: Allocator) error{OutOfMemory}!void {
    // Attributes that arrived encoded and didn't decode here (a codec this side lacks) are
    // passed on as received: overwriting them would lose the original message.
    if (f.encoded_attributes != null) return;
    // JSON strings must be UTF-8: bytes that aren't would be written as an array of numbers,
    // which other SDKs can't read.
    f.encoded_attributes = try converter.toPayload(arena, EncodedAttributes{
        .message = try validUtf8(arena, f.message),
        .stack_trace = try validUtf8(arena, f.stack_trace),
    });
    f.message = encoded_message;
    f.stack_trace = "";
}

/// What other SDKs may write: either field may be null or missing.
const ReceivedAttributes = struct {
    message: ?[]const u8 = null,
    stack_trace: ?[]const u8 = null,
};

fn decodeOne(f: *Failure, arena: Allocator) error{OutOfMemory}!void {
    const p = f.encoded_attributes orelse return;
    const enc = converter.encodingOf(p) orelse return;
    if (!std.mem.eql(u8, enc, converter.Encoding.json.text())) return;
    const attrs = parseAttributes(arena, p.data) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        // A JavaScript sender can write a lone UTF-16 surrogate escape, which std.json
        // rejects; read it as U+FFFD, as other SDKs' JSON readers do.
        else => parseAttributes(arena, try repairSurrogates(arena, p.data)) catch |e| switch (e) {
            error.OutOfMemory => return error.OutOfMemory,
            else => return,
        },
    };
    f.message = attrs.message orelse "";
    f.stack_trace = attrs.stack_trace orelse "";
    f.encoded_attributes = null;
}

fn parseAttributes(arena: Allocator, json: []const u8) std.json.ParseError(std.json.Scanner)!ReceivedAttributes {
    return std.json.parseFromSliceLeaky(ReceivedAttributes, arena, json, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
        .duplicate_field_behavior = .use_last,
    });
}

/// `s`, with each byte that doesn't start a valid UTF-8 sequence replaced by U+FFFD.
fn validUtf8(arena: Allocator, s: []const u8) error{OutOfMemory}![]const u8 {
    if (std.unicode.utf8ValidateSlice(s)) return s;
    var out: std.ArrayList(u8) = .empty;
    var i: usize = 0;
    while (i < s.len) {
        const len = std.unicode.utf8ByteSequenceLength(s[i]) catch 0;
        if (len > 0 and i + len <= s.len) if (std.unicode.utf8Decode(s[i..][0..len])) |_| {
            try out.appendSlice(arena, s[i..][0..len]);
            i += len;
            continue;
        } else |_| {};
        try out.appendSlice(arena, "\u{fffd}");
        i += 1;
    }
    return out.items;
}

/// The JSON text `json` with each `\u` escape of an unpaired UTF-16 surrogate replaced by
/// `\ufffd`. Escapes are read in order, so an escaped backslash before "u" isn't mistaken
/// for one.
fn repairSurrogates(arena: Allocator, json: []const u8) error{OutOfMemory}![]const u8 {
    var out: std.ArrayList(u8) = .empty;
    var i: usize = 0;
    while (i < json.len) {
        if (json[i] != '\\' or i + 1 >= json.len) {
            try out.append(arena, json[i]);
            i += 1;
            continue;
        }
        if (json[i + 1] != 'u') {
            try out.appendSlice(arena, json[i .. i + 2]);
            i += 2;
            continue;
        }
        const unit = escapeUnit(json, i) orelse {
            try out.appendSlice(arena, json[i .. i + 2]);
            i += 2;
            continue;
        };
        if (unit >= 0xd800 and unit <= 0xdbff) {
            if (escapeUnit(json, i + 6)) |low| if (low >= 0xdc00 and low <= 0xdfff) {
                try out.appendSlice(arena, json[i .. i + 12]);
                i += 12;
                continue;
            };
            try out.appendSlice(arena, "\\ufffd");
        } else if (unit >= 0xdc00 and unit <= 0xdfff) {
            try out.appendSlice(arena, "\\ufffd");
        } else {
            try out.appendSlice(arena, json[i .. i + 6]);
        }
        i += 6;
    }
    return out.items;
}

/// The code unit of the `\uXXXX` escape at `json[i..]`, or null if there isn't one.
fn escapeUnit(json: []const u8, i: usize) ?u16 {
    if (i + 6 > json.len or json[i] != '\\' or json[i + 1] != 'u') return null;
    return std.fmt.parseInt(u16, json[i + 2 .. i + 6], 16) catch null;
}

fn isArrayList(comptime T: type) bool {
    return @typeInfo(T) == .@"struct" and @hasField(T, "items") and @hasField(T, "capacity");
}

/// Whether a value of `T` can reach a `Failure`.
fn mayHoldFailures(comptime T: type) bool {
    @setEvalBranchQuota(1_000_000);
    if (T == Failure) return true;
    if (T == converter.Payload) return false;
    return switch (@typeInfo(T)) {
        .optional => |o| mayHoldFailures(o.child),
        // Generated messages refer to themselves only through single-item pointers (a
        // failure's `cause`); answering yes here keeps the check from recursing forever.
        .pointer => |p| p.size == .one,
        .@"struct" => |s| blk: {
            if (isArrayList(T)) break :blk mayHoldFailures(@typeInfo(@FieldType(T, "items")).pointer.child);
            for (s.field_types) |F| if (mayHoldFailures(F)) break :blk true;
            break :blk false;
        },
        .@"union" => |u| blk: {
            for (u.field_types) |F| if (mayHoldFailures(F)) break :blk true;
            break :blk false;
        },
        else => false,
    };
}

// ---------------------------------------------------------------- typed view

/// A failure's kind and the fields of its kind, as Zig values: `kind(f)`. One case per
/// failure kind the server defines, and `unknown` for a failure with no info or of a kind
/// newer than this SDK. A view of the failure it was read from: strings and payloads borrow
/// it, so it lives as long as that failure.
///
///     switch (failure.kind(failure.rootCause(f))) {
///         .timeout => |t| if (t.timeout_type == .start_to_close) ...,
///         .application => |app| const code = try app.details.get(u32, arena, 0),
///         else => ...,
///     }
pub const Kind = union(enum) {
    application: ApplicationKind,
    timeout: TimeoutKind,
    canceled: CanceledKind,
    terminated: TerminatedKind,
    server: ServerKind,
    reset_workflow: ResetWorkflowKind,
    activity: ActivityKind,
    child_workflow: ChildWorkflowKind,
    nexus_operation: NexusOperationKind,
    nexus_handler: NexusHandlerKind,
    unknown,

    pub const ApplicationKind = struct {
        type: []const u8,
        non_retryable: bool,
        details: converter.EncodedValues,
        /// Null when the failure sets none.
        next_retry_delay: ?std.Io.Duration,
        category: Application.Category,
    };

    pub const TimeoutKind = struct {
        timeout_type: TimeoutType,
        /// The details of the last heartbeat before a heartbeat timeout.
        last_heartbeat_details: converter.EncodedValues,
    };

    pub const CanceledKind = struct {
        details: converter.EncodedValues,
        identity: []const u8,
    };

    pub const TerminatedKind = struct {
        identity: []const u8,
    };

    pub const ServerKind = struct {
        non_retryable: bool,
    };

    pub const ResetWorkflowKind = struct {
        last_heartbeat_details: converter.EncodedValues,
    };

    /// An activity that failed; its cause says why.
    pub const ActivityKind = struct {
        activity_type: []const u8,
        activity_id: []const u8,
        identity: []const u8,
        retry_state: RetryState,
        scheduled_event_id: i64,
        started_event_id: i64,
    };

    /// A child workflow that failed; its cause says why.
    pub const ChildWorkflowKind = struct {
        namespace: []const u8,
        workflow_id: []const u8,
        run_id: []const u8,
        workflow_type: []const u8,
        retry_state: RetryState,
        initiated_event_id: i64,
        started_event_id: i64,
    };

    /// A Nexus operation that failed; its cause says why.
    pub const NexusOperationKind = struct {
        endpoint: []const u8,
        service: []const u8,
        operation: []const u8,
        operation_token: []const u8,
        scheduled_event_id: i64,
    };

    /// A Nexus handler that failed the request (`nexus.HandlerError`).
    pub const NexusHandlerKind = struct {
        /// The handler error's type as sent, such as "BAD_REQUEST".
        type: []const u8,
        retry_behavior: RetryBehavior,

        pub const RetryBehavior = enum { unspecified, retryable, non_retryable, unknown };
    };

    pub const TimeoutType = enum { unspecified, start_to_close, schedule_to_start, schedule_to_close, heartbeat, unknown };

    pub const RetryState = enum {
        unspecified,
        in_progress,
        non_retryable_failure,
        timeout,
        maximum_attempts_reached,
        retry_policy_not_set,
        internal_server_error,
        cancel_requested,
        unknown,
    };
};

/// The kind of `f` with its fields; see `Kind`. Borrows `f`.
pub fn kind(f: *const Failure) Kind {
    const info = if (f.failure_info) |*i| i else return .unknown;
    return switch (info.*) {
        .application_failure_info => |*a| .{ .application = .{
            .type = a.type,
            .non_retryable = a.non_retryable,
            .details = values(a.details),
            .next_retry_delay = if (a.next_retry_delay) |d| .fromNanoseconds(@as(i96, d.seconds) * std.time.ns_per_s + d.nanos) else null,
            .category = applicationCategory(f.*).?,
        } },
        .timeout_failure_info => |*t| .{ .timeout = .{
            .timeout_type = switch (t.timeout_type) {
                .TIMEOUT_TYPE_UNSPECIFIED => .unspecified,
                .TIMEOUT_TYPE_START_TO_CLOSE => .start_to_close,
                .TIMEOUT_TYPE_SCHEDULE_TO_START => .schedule_to_start,
                .TIMEOUT_TYPE_SCHEDULE_TO_CLOSE => .schedule_to_close,
                .TIMEOUT_TYPE_HEARTBEAT => .heartbeat,
                _ => .unknown,
            },
            .last_heartbeat_details = values(t.last_heartbeat_details),
        } },
        .canceled_failure_info => |*c| .{ .canceled = .{ .details = values(c.details), .identity = c.identity } },
        .terminated_failure_info => |*t| .{ .terminated = .{ .identity = t.identity } },
        .server_failure_info => |*s| .{ .server = .{ .non_retryable = s.non_retryable } },
        .reset_workflow_failure_info => |*r| .{ .reset_workflow = .{ .last_heartbeat_details = values(r.last_heartbeat_details) } },
        .activity_failure_info => |*a| .{ .activity = .{
            .activity_type = if (a.activity_type) |t| t.name else "",
            .activity_id = a.activity_id,
            .identity = a.identity,
            .retry_state = retryState(a.retry_state),
            .scheduled_event_id = a.scheduled_event_id,
            .started_event_id = a.started_event_id,
        } },
        .child_workflow_execution_failure_info => |*c| .{ .child_workflow = .{
            .namespace = c.namespace,
            .workflow_id = if (c.workflow_execution) |e| e.workflow_id else "",
            .run_id = if (c.workflow_execution) |e| e.run_id else "",
            .workflow_type = if (c.workflow_type) |t| t.name else "",
            .retry_state = retryState(c.retry_state),
            .initiated_event_id = c.initiated_event_id,
            .started_event_id = c.started_event_id,
        } },
        .nexus_operation_execution_failure_info => |*n| .{ .nexus_operation = .{
            .endpoint = n.endpoint,
            .service = n.service,
            .operation = n.operation,
            .operation_token = if (n.operation_token.len > 0) n.operation_token else n.operation_id,
            .scheduled_event_id = n.scheduled_event_id,
        } },
        .nexus_handler_failure_info => |*n| .{ .nexus_handler = .{
            .type = n.type,
            .retry_behavior = switch (n.retry_behavior) {
                .NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_UNSPECIFIED => .unspecified,
                .NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_RETRYABLE => .retryable,
                .NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_NON_RETRYABLE => .non_retryable,
                _ => .unknown,
            },
        } },
    };
}

fn values(p: ?converter.Payloads) converter.EncodedValues {
    return .{ .payloads = if (p) |ps| ps.payloads.items else &.{} };
}

fn retryState(s: proto.temporal.api.enums.v1.RetryState) Kind.RetryState {
    return switch (s) {
        .RETRY_STATE_UNSPECIFIED => .unspecified,
        .RETRY_STATE_IN_PROGRESS => .in_progress,
        .RETRY_STATE_NON_RETRYABLE_FAILURE => .non_retryable_failure,
        .RETRY_STATE_TIMEOUT => .timeout,
        .RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED => .maximum_attempts_reached,
        .RETRY_STATE_RETRY_POLICY_NOT_SET => .retry_policy_not_set,
        .RETRY_STATE_INTERNAL_SERVER_ERROR => .internal_server_error,
        .RETRY_STATE_CANCEL_REQUESTED => .cancel_requested,
        _ => .unknown,
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

test "application failures carry their category and next retry delay" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const f = try application(arena.allocator(), .{ .message = "not ready", .category = .benign, .next_retry_delay = .fromMilliseconds(2500) });
    const info = f.failure_info.?.application_failure_info;
    try testing.expectEqual(@as(i64, 2), info.next_retry_delay.?.seconds);
    try testing.expectEqual(@as(i32, 500_000_000), info.next_retry_delay.?.nanos);
    try testing.expectEqual(Application.Category.benign, applicationCategory(f).?);

    const plain = try application(arena.allocator(), .{ .message = "boom" });
    try testing.expectEqual(null, plain.failure_info.?.application_failure_info.next_retry_delay);
    try testing.expectEqual(Application.Category.unspecified, applicationCategory(plain).?);
    try testing.expectEqual(null, applicationCategory(.{ .failure_info = .{ .canceled_failure_info = .{} } }));
}

test "an application failure carries a copy of its cause chain, and so does ctx.fail's" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var root = try application(a, .{ .message = "card declined", .type = "Declined" });
    const middle = try application(a, .{ .message = "charge failed", .cause = &root });
    const top = try application(a, .{ .message = "order failed", .type = "OrderFailed", .cause = &middle });
    // application copies the chain: changing the original afterwards changes nothing.
    try testing.expect(top.cause.? != &middle);
    root.message = "changed before the copy was read";
    try testing.expectEqualStrings("charge failed", top.cause.?.message);
    try testing.expectEqualStrings("card declined", rootCause(&top).message);
    root.message = "card declined";

    const copy = (try copyChain(a, &middle)).?;
    root.message = "changed";
    try testing.expectEqualStrings("charge failed", copy.message);
    try testing.expectEqualStrings("card declined", copy.cause.?.message);
    try testing.expectEqual(null, try copyChain(a, null));
}

test "encoded attributes move message and stack trace of a cause chain into a payload and back" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var root = try application(a, .{ .message = "card declined", .type = "Declined" });
    root.stack_trace = "pay.zig:3:5: in charge\n";
    const top: Failure = .{ .message = "activity error", .cause = &root, .failure_info = .{ .activity_failure_info = .{} } };

    var sent = top;
    try encodeAttributes(Failure, &sent, a);
    // The original chain is left as it was.
    try testing.expectEqualStrings("card declined", root.message);
    try testing.expectEqual(null, top.encoded_attributes);
    for ([_]*const Failure{ &sent, sent.cause.? }) |f| {
        try testing.expectEqualStrings(encoded_message, f.message);
        try testing.expectEqualStrings("", f.stack_trace);
        try testing.expectEqualStrings("json/plain", converter.encodingOf(f.encoded_attributes.?).?);
    }
    try testing.expectEqualStrings("{\"message\":\"card declined\",\"stack_trace\":\"pay.zig:3:5: in charge\\n\"}", sent.cause.?.encoded_attributes.?.data);
    try testing.expectEqualStrings("Declined", applicationType(sent.cause.?.*).?);

    var received = sent;
    try decodeAttributes(Failure, &received, a);
    try testing.expectEqualStrings("activity error", received.message);
    try testing.expectEqual(null, received.encoded_attributes);
    try testing.expectEqualStrings("card declined", received.cause.?.message);
    try testing.expectEqualStrings("pay.zig:3:5: in charge\n", received.cause.?.stack_trace);
    try testing.expectEqual(null, received.cause.?.encoded_attributes);
}

test "encoded attributes reach failures nested in other messages, and ones that don't decode stay as received" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const Completion = proto.coresdk.workflow_completion.WorkflowActivationCompletion;
    var completion: Completion = .{ .run_id = "r", .status = .{ .failed = .{ .failure = try application(a, .{ .message = "boom" }) } } };
    try encodeAttributes(Completion, &completion, a);
    try testing.expectEqualStrings(encoded_message, completion.status.?.failed.failure.?.message);
    try decodeAttributes(Completion, &completion, a);
    try testing.expectEqualStrings("boom", completion.status.?.failed.failure.?.message);

    // A codec this side doesn't have, and JSON that isn't the object.
    const foreign = try converter.payloadWithEncoding(a, "binary/encrypted", "xyz");
    const not_object = try converter.payloadWithEncoding(a, "json/plain", "[1]");
    for ([_]converter.Payload{ foreign, not_object }) |p| {
        var f: Failure = .{ .message = encoded_message, .encoded_attributes = p };
        try decodeAttributes(Failure, &f, a);
        try testing.expectEqualStrings(encoded_message, f.message);
        try testing.expect(f.encoded_attributes != null);
    }
}

test "kind reads every failure kind with Zig values" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const common = proto.temporal.api.common.v1;
    var details: converter.Payloads = .{};
    try details.payloads.append(a, try converter.toPayload(a, @as(u32, 7)));

    const app = try application(a, .{ .message = "m", .type = "T", .non_retryable = true, .details = details.payloads.items, .next_retry_delay = .fromMilliseconds(1500), .category = .benign });
    const k = kind(&app).application;
    try testing.expectEqualStrings("T", k.type);
    try testing.expect(k.non_retryable);
    try testing.expectEqual(@as(u32, 7), try k.details.get(u32, a, 0));
    try testing.expectEqual(@as(i96, 1500 * std.time.ns_per_ms), k.next_retry_delay.?.nanoseconds);
    try testing.expectEqual(Application.Category.benign, k.category);

    const timeout: Failure = .{ .failure_info = .{ .timeout_failure_info = .{ .timeout_type = .TIMEOUT_TYPE_HEARTBEAT, .last_heartbeat_details = details } } };
    try testing.expectEqual(Kind.TimeoutType.heartbeat, kind(&timeout).timeout.timeout_type);
    try testing.expectEqual(@as(u32, 7), try kind(&timeout).timeout.last_heartbeat_details.get(u32, a, 0));
    const newer_timeout: Failure = .{ .failure_info = .{ .timeout_failure_info = .{ .timeout_type = @fromBackingInt(@intCast(99)) } } };
    try testing.expectEqual(Kind.TimeoutType.unknown, kind(&newer_timeout).timeout.timeout_type);

    const canceled: Failure = .{ .failure_info = .{ .canceled_failure_info = .{ .details = details, .identity = "me" } } };
    try testing.expectEqual(@as(usize, 1), kind(&canceled).canceled.details.len());
    try testing.expectEqualStrings("me", kind(&canceled).canceled.identity);

    const terminated: Failure = .{ .failure_info = .{ .terminated_failure_info = .{ .identity = "op" } } };
    try testing.expectEqualStrings("op", kind(&terminated).terminated.identity);

    const server: Failure = .{ .failure_info = .{ .server_failure_info = .{ .non_retryable = true } } };
    try testing.expect(kind(&server).server.non_retryable);

    const reset: Failure = .{ .failure_info = .{ .reset_workflow_failure_info = .{ .last_heartbeat_details = details } } };
    try testing.expectEqual(@as(usize, 1), kind(&reset).reset_workflow.last_heartbeat_details.len());

    const activity: Failure = .{ .failure_info = .{ .activity_failure_info = .{ .activity_type = .{ .name = "Charge" }, .activity_id = "1", .identity = "w", .retry_state = .RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED, .scheduled_event_id = 5, .started_event_id = 6 } } };
    const ak = kind(&activity).activity;
    try testing.expectEqualStrings("Charge", ak.activity_type);
    try testing.expectEqualStrings("1", ak.activity_id);
    try testing.expectEqual(Kind.RetryState.maximum_attempts_reached, ak.retry_state);
    try testing.expectEqual(@as(i64, 6), ak.started_event_id);
    const newer_retry: Failure = .{ .failure_info = .{ .activity_failure_info = .{ .retry_state = @fromBackingInt(@intCast(42)) } } };
    try testing.expectEqual(Kind.RetryState.unknown, kind(&newer_retry).activity.retry_state);

    const child: Failure = .{ .failure_info = .{ .child_workflow_execution_failure_info = .{
        .namespace = "ns",
        .workflow_execution = common.WorkflowExecution{ .workflow_id = "kid", .run_id = "run" },
        .workflow_type = .{ .name = "Kid" },
        .retry_state = .RETRY_STATE_NON_RETRYABLE_FAILURE,
        .initiated_event_id = 3,
    } } };
    const ck = kind(&child).child_workflow;
    try testing.expectEqualStrings("kid", ck.workflow_id);
    try testing.expectEqualStrings("run", ck.run_id);
    try testing.expectEqualStrings("Kid", ck.workflow_type);
    try testing.expectEqual(Kind.RetryState.non_retryable_failure, ck.retry_state);
    try testing.expectEqual(@as(i64, 3), ck.initiated_event_id);

    const nexus_op: Failure = .{ .failure_info = .{ .nexus_operation_execution_failure_info = .{ .endpoint = "e", .service = "s", .operation = "o", .operation_token = "tok", .scheduled_event_id = 9 } } };
    const nk = kind(&nexus_op).nexus_operation;
    try testing.expectEqualStrings("e", nk.endpoint);
    try testing.expectEqualStrings("tok", nk.operation_token);
    try testing.expectEqual(@as(i64, 9), nk.scheduled_event_id);

    const handler: Failure = .{ .failure_info = .{ .nexus_handler_failure_info = .{ .type = "BAD_REQUEST", .retry_behavior = .NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_NON_RETRYABLE } } };
    try testing.expectEqualStrings("BAD_REQUEST", kind(&handler).nexus_handler.type);
    try testing.expectEqual(Kind.NexusHandlerKind.RetryBehavior.non_retryable, kind(&handler).nexus_handler.retry_behavior);

    try testing.expectEqual(Kind.unknown, kind(&.{ .message = "no info" }));
}

fn traceLeaf() error{Leaf}!void {
    return error.Leaf;
}

fn traceMiddle() error{Leaf}!void {
    try traceLeaf();
}

test "traceText names the source of each frame of an error return trace, or is empty without one" {
    traceMiddle() catch {
        const text = try traceText(testing.allocator, @errorReturnTrace());
        defer testing.allocator.free(text);
        if (@errorReturnTrace() == null) {
            try testing.expectEqualStrings("", text);
        } else {
            try testing.expect(std.mem.indexOf(u8, text, "failure.zig:") != null);
            try testing.expect(std.mem.indexOf(u8, text, "traceLeaf") != null);
            try testing.expect(std.mem.indexOf(u8, text, "0x") == null);
        }
    };
    try testing.expectEqualStrings("", try traceText(testing.allocator, null));
}

test "encoding attributes keeps a failure whose attributes are already encoded as it was received" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    // Received from a worker with a codec this side lacks, so `decodeAttributes` left it.
    const foreign = try converter.payloadWithEncoding(a, "binary/encrypted", "secret message");
    var root = try application(a, .{ .message = encoded_message, .type = "Declined" });
    root.encoded_attributes = foreign;
    var sent: Failure = .{ .message = "activity error", .cause = &root, .failure_info = .{ .activity_failure_info = .{} } };
    try encodeAttributes(Failure, &sent, a);
    try testing.expectEqualStrings("binary/encrypted", converter.encodingOf(sent.cause.?.encoded_attributes.?).?);
    try testing.expectEqualStrings("secret message", sent.cause.?.encoded_attributes.?.data);
}

test "encoded attributes write invalid UTF-8 as a JSON string and read what other SDKs write" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var bad: Failure = .{ .message = "bad \xff", .stack_trace = "ok" };
    try encodeAttributes(Failure, &bad, a);
    try testing.expectEqualStrings("{\"message\":\"bad \u{fffd}\",\"stack_trace\":\"ok\"}", bad.encoded_attributes.?.data);
    try decodeAttributes(Failure, &bad, a);
    try testing.expectEqualStrings("bad \u{fffd}", bad.message);

    const cases = [_]struct { json: []const u8, message: []const u8 }{
        .{ .json = "{\"message\":\"\\ud800x\",\"stack_trace\":\"\"}", .message = "\u{fffd}x" },
        .{ .json = "{\"message\":\"\\udc00\"}", .message = "\u{fffd}" },
        .{ .json = "{\"message\":\"\\ud83d\\ude00 \\\\ud800\"}", .message = "\u{1f600} \\ud800" },
        .{ .json = "{\"message\":\"a\",\"message\":\"b\"}", .message = "b" },
        .{ .json = "{\"message\":null,\"stack_trace\":null}", .message = "" },
        .{ .json = "{}", .message = "" },
    };
    for (cases) |c| {
        var f: Failure = .{ .message = encoded_message, .encoded_attributes = try converter.payloadWithEncoding(a, "json/plain", c.json) };
        try decodeAttributes(Failure, &f, a);
        try testing.expectEqualStrings(c.message, f.message);
        try testing.expectEqual(null, f.encoded_attributes);
    }
}

const DeepChain = struct {
    const depth = 10_000;

    fn run(result: *anyerror!void) void {
        result.* = walk();
    }

    fn walk() !void {
        var arena: std.heap.ArenaAllocator = .init(std.heap.page_allocator);
        defer arena.deinit();
        const a = arena.allocator();
        const nodes = try a.alloc(Failure, depth);
        for (nodes, 0..) |*n, i| n.* = .{ .message = "link", .cause = if (i + 1 < depth) &nodes[i + 1] else null };
        nodes[depth - 1].message = "root";

        const app = try application(a, .{ .message = "top", .cause = &nodes[0] });
        const kept = try keep(a, .{ .message = "top", .cause = &nodes[0] });
        nodes[depth - 1].message = "changed";
        try testing.expectEqualStrings("root", rootCause(&app).message);
        try testing.expectEqualStrings("root", rootCause(kept.cause.?).message);

        var sent = app;
        try encodeAttributes(Failure, &sent, a);
        try testing.expectEqualStrings(encoded_message, rootCause(&sent).message);
        try decodeAttributes(Failure, &sent, a);
        try testing.expectEqualStrings("root", rootCause(&sent).message);
    }
};

test "copying, encoding and decoding a deep cause chain use the same stack whatever its length" {
    // The size of a workflow coroutine's default stack (worker.Options.workflow_stack_size):
    // recursion down a 10,000-link chain would overflow it. Not smaller: glibc refuses thread
    // stacks under PTHREAD_STACK_MIN, which is 128 KiB on aarch64 Linux.
    var result: anyerror!void = {};
    const thread = try std.Thread.spawn(.{ .stack_size = 256 * 1024 }, DeepChain.run, .{&result});
    thread.join();
    try result;
}

/// Its body is its own: in release builds the linker folds functions with identical code
/// into one, and the frame would then name another.
noinline fn scopeLeaf(n: u32) error{Scoped}!u32 {
    if (n > 3) return error.Scoped;
    return n * 7 + 1;
}

test "TraceScope gives a call an empty trace, even after the thread's trace filled up, and restores it" {
    const t = @errorReturnTrace() orelse return; // no error return tracing in this build
    const before_index = t.index;
    // As a pooled thread whose earlier tasks failed: the trace is full.
    t.index = t.instruction_addresses.len + 5;
    const marker = t.instruction_addresses[0];
    {
        const scope: TraceScope = .begin(t);
        defer scope.end();
        try testing.expectEqual(0, t.index);
        _ = scopeLeaf(5) catch {
            const text = try traceText(testing.allocator, t);
            defer testing.allocator.free(text);
            try testing.expect(std.mem.indexOf(u8, text, "scopeLeaf") != null);
            try testing.expect(t.index <= 2);
        };
    }
    try testing.expectEqual(t.instruction_addresses.len + 5, t.index);
    try testing.expectEqual(marker, t.instruction_addresses[0]);
    t.index = before_index;
}

noinline fn pendingCancelLeaf() error{Interrupted}!void {
    return error.Interrupted;
}

/// Symbolizes `addresses` on a task that ends with a cancel pending, as an activity's thread
/// does when the worker cancels its task while the activity returns an error of its own, or
/// when code it calls turns a cancel into another error and re-arms it with `Io.recancel`.
fn symbolizeWithCancelPending(io: std.Io, addresses: []const usize, started: *std.Io.Event, out: *[]const u8) void {
    started.set(io);
    var never: std.Io.Event = .unset;
    never.wait(io) catch io.recancel();
    out.* = addressesText(testing.allocator, addresses) catch "";
}

test "addressesText symbolizes a trace on a thread with a cancel pending, and debug info stays readable after" {
    var addresses: []const usize = &.{};
    pendingCancelLeaf() catch {
        addresses = try captureTrace(testing.allocator, @errorReturnTrace());
    };
    defer testing.allocator.free(addresses);
    if (addresses.len == 0) return; // no error return tracing in this build
    const io = testing.io;
    var started: std.Io.Event = .unset;
    var canceled_text: []const u8 = "";
    var task = try io.concurrent(symbolizeWithCancelPending, .{ io, addresses, &started, &canceled_text });
    try started.wait(io);
    task.cancel(io);
    defer if (canceled_text.len > 0) testing.allocator.free(canceled_text);
    // The cancel belongs to the task, not to reading debug info: std's own trace printers
    // block cancelation around it. Read with the cancel pending, the executable's debug info
    // fails to load, and std keeps that failure: no trace in the process symbolizes again.
    const later = try addressesText(testing.allocator, addresses);
    defer testing.allocator.free(later);
    try testing.expect(std.mem.indexOf(u8, later, "pendingCancelLeaf") != null);
    try testing.expect(std.mem.indexOf(u8, canceled_text, "pendingCancelLeaf") != null);
}
