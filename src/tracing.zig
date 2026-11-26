//! Tracing: spans around what clients send and what workers run, carried across calls so one
//! trace spans a client, its workflows, their activities, children and Nexus operations.
//!
//! The SDK depends on no tracing library. A backend implements `Tracer`, two functions that
//! start and finish a span, and `Interceptor` calls it:
//!
//!     var tracing: temporal.tracing.Interceptor = .init(my_tracer.tracer());
//!     const client_interceptors = [_]temporal.interceptor.Client{tracing.client()};
//!     const worker_interceptors = [_]temporal.interceptor.Worker{tracing.worker()};
//!     const propagators = [_]temporal.propagation.ContextPropagator{tracing.propagator()};
//!
//! given to `client.Options.interceptors`, `worker_interceptors` and `propagators`. A span's
//! context travels in the Temporal header field `header_key` and, for Nexus operations, as
//! one Nexus header per context field. Inside an activity or a Nexus handler,
//! `ctx.propagated().get(span_key)` is the running span's context.
//!
//! Workflow code (the workflow function, its signal and update handlers, update validators and
//! query handlers) opens spans of its own with `startSpan`; while one is open, the activities,
//! child workflows, signals and Nexus operations the same coroutine starts are its children:
//!
//!     const span = temporal.tracing.startSpan(ctx, "workflow-operation", .{});
//!     defer span.end();
//!
//! Workflow spans are replay-safe: neither the interceptor nor `startSpan` starts a span for
//! work replayed from history (queries and update validation, which never come from history,
//! excepted), and nothing a tracer returns is read by workflow code, so tracing never makes a
//! workflow nondeterministic, and a history recorded with or without `startSpan` calls or the
//! interceptor replays the same.

const std = @import("std");
const converter = @import("converter.zig");
const propagation = @import("propagation.zig");
const interceptor = @import("interceptor.zig");
const workflow = @import("workflow.zig");
const activity = @import("activity.zig");
const nexus = @import("nexus.zig");
const proto = @import("proto.zig");
const Allocator = std.mem.Allocator;
const Payload = converter.Payload;

/// The Temporal header field that carries a span context, as the other SDKs name it.
pub const header_key = "_tracer-data";

/// One name/value pair: a field of a span context, or a tag of a span.
pub const Field = struct { name: []const u8, value: []const u8 };

/// What identifies a span to its children and to other processes: the tracer's own fields,
/// for example `traceparent` and `tracestate`. Plain data; copy freely. Empty means no span.
pub const SpanContext = struct {
    fields: []const Field = &.{},

    /// The value of the field `name`, compared ignoring ASCII case, or null.
    pub fn get(c: SpanContext, name: []const u8) ?[]const u8 {
        for (c.fields) |f| if (std.ascii.eqlIgnoreCase(f.name, name)) return f.value;
        return null;
    }

    pub fn isEmpty(c: SpanContext) bool {
        return c.fields.len == 0;
    }
};

/// The key under which a span context lives in a `propagation.Values` store.
pub const span_key = propagation.key(SpanContext, "temporal.tracing.span");

pub const Kind = enum { client, server, internal };

pub const Operation = enum {
    start_workflow,
    signal_workflow,
    signal_with_start_workflow,
    query_workflow,
    update_workflow,
    run_workflow,
    handle_signal,
    handle_query,
    validate_update,
    handle_update,
    start_activity,
    start_child_workflow,
    start_nexus_operation,
    run_activity,
    run_start_nexus_operation_handler,
    run_cancel_nexus_operation_handler,
    /// Experimental: may change or be removed without notice. A span workflow code started
    /// with `startSpan`; its span name is the caller's name alone.
    application,
    /// Experimental: may change or be removed without notice. A signal the workflow sends
    /// with `ctx.signalExternalWorkflow` or `signalExternalWorkflowByName`.
    signal_external_workflow,
    /// Experimental: may change or be removed without notice. A signal the workflow sends
    /// to its child with `ChildWorkflowFuture.signal`.
    signal_child_workflow,

    /// The operation's text in span names, shared with the other SDKs, such as
    /// "StartWorkflow" or "RunActivity"; empty for `.application`.
    pub fn text(op: Operation) []const u8 {
        return switch (op) {
            .start_workflow => "StartWorkflow",
            .signal_workflow => "SignalWorkflow",
            .signal_with_start_workflow => "SignalWithStartWorkflow",
            .query_workflow => "QueryWorkflow",
            .update_workflow => "UpdateWorkflow",
            .run_workflow => "RunWorkflow",
            .handle_signal => "HandleSignal",
            .handle_query => "HandleQuery",
            .validate_update => "ValidateUpdate",
            .handle_update => "HandleUpdate",
            .start_activity => "StartActivity",
            .start_child_workflow => "StartChildWorkflow",
            .start_nexus_operation => "StartNexusOperation",
            .run_activity => "RunActivity",
            .run_start_nexus_operation_handler => "RunStartNexusOperationHandler",
            .run_cancel_nexus_operation_handler => "RunCancelNexusOperationHandler",
            .application => "",
            .signal_external_workflow => "SignalExternalWorkflow",
            .signal_child_workflow => "SignalChildWorkflow",
        };
    }

    /// `.client` for the operations that send (start_*, signal_*, query_workflow,
    /// update_workflow), `.server` for those that run or handle, `.internal` for
    /// `.application`.
    pub fn kind(op: Operation) Kind {
        return switch (op) {
            .start_workflow,
            .signal_workflow,
            .signal_with_start_workflow,
            .query_workflow,
            .update_workflow,
            .start_activity,
            .start_child_workflow,
            .start_nexus_operation,
            .signal_external_workflow,
            .signal_child_workflow,
            => .client,
            .run_workflow,
            .handle_signal,
            .handle_query,
            .validate_update,
            .handle_update,
            .run_activity,
            .run_start_nexus_operation_handler,
            .run_cancel_nexus_operation_handler,
            => .server,
            .application => .internal,
        };
    }
};

// The tag names the interceptor gives spans.
const tag_workflow_id = "temporalWorkflowID";
const tag_run_id = "temporalRunID";
const tag_activity_id = "temporalActivityID";
const tag_update_id = "temporalUpdateID";

pub const StartSpanOptions = struct {
    operation: Operation,
    /// The workflow type, activity type, signal name, query type, update name,
    /// "<service>/<operation>" for Nexus, or the name workflow code gave `startSpan`.
    name: []const u8,
    /// Null or empty: the span is a root.
    parent: ?SpanContext = null,
    /// Null: now, by the tracer's clock.
    start_time: ?std.Io.Timestamp = null,
    /// Tag names: "temporalWorkflowID", "temporalRunID", "temporalActivityID",
    /// "temporalUpdateID"; an `.application` span's attributes come first.
    tags: []const Field = &.{},

    /// "<operation text>:<name>", or `name` alone for `.application`, allocated from
    /// `arena`. Tracers use it as the span name.
    pub fn spanName(o: StartSpanOptions, arena: Allocator) error{OutOfMemory}![]const u8 {
        if (o.operation == .application) return arena.dupe(u8, o.name);
        return std.mem.concat(arena, u8, &.{ o.operation.text(), ":", o.name });
    }
};

pub const FinishSpanOptions = struct {
    /// Null on success; else a short description of what failed (the error's name, or a
    /// Nexus handler error's type and message). Borrowed for the call.
    failure: ?[]const u8 = null,
};

/// A started span. `handle` is the tracer's own and is given back to `finishSpan`;
/// `context` is what the interceptor sends on and uses as the parent of child spans.
pub const Span = struct {
    handle: ?*anyopaque = null,
    context: SpanContext = .{},
};

/// The interface a tracing backend implements: `ptr` is the tracer's state and `vtable` its
/// functions. Both are called from client threads, activity threads, Nexus request tasks and
/// workflow threads at once, and a span may be finished on another thread than it was
/// started on, so they must be thread-safe. They run on workflow threads too, so they should
/// not block: export spans in the background.
pub const Tracer = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        /// Starts a span. `Span.context.fields` must be allocated from `arena`; an empty
        /// context is allowed (not propagated). A parent that the tracer cannot read is
        /// treated as no parent. `options` (its name, tags and parent) is borrowed for the
        /// call only: a tracer that needs them later copies them.
        startSpan: *const fn (ptr: *anyopaque, arena: Allocator, options: StartSpanOptions) error{OutOfMemory}!Span,
        /// Ends the span; called exactly once for every span `startSpan` returned.
        finishSpan: *const fn (ptr: *anyopaque, span: Span, options: FinishSpanOptions) void,
    };

    pub fn startSpan(t: Tracer, arena: Allocator, options: StartSpanOptions) error{OutOfMemory}!Span {
        return t.vtable.startSpan(t.ptr, arena, options);
    }

    pub fn finishSpan(t: Tracer, span: Span, options: FinishSpanOptions) void {
        t.vtable.finishSpan(t.ptr, span, options);
    }

    /// The tracer of `impl`; `T` declares `startSpan(*T, Allocator, StartSpanOptions)` and
    /// `finishSpan(*T, Span, FinishSpanOptions)`.
    pub fn from(comptime T: type, impl: *T) Tracer {
        inline for (.{ "startSpan", "finishSpan" }) |name| {
            if (!@hasDecl(T, name)) @compileError("tracing.Tracer.from(" ++ @typeName(T) ++ "): the type declares no pub fn " ++ name);
        }
        const Erased = struct {
            fn start(ptr: *anyopaque, arena: Allocator, options: StartSpanOptions) error{OutOfMemory}!Span {
                return T.startSpan(@as(*T, @ptrCast(@alignCast(ptr))), arena, options);
            }
            fn finish(ptr: *anyopaque, span: Span, options: FinishSpanOptions) void {
                T.finishSpan(@as(*T, @ptrCast(@alignCast(ptr))), span, options);
            }
            const vtable: VTable = .{ .startSpan = start, .finishSpan = finish };
        };
        return .{ .ptr = impl, .vtable = &Erased.vtable };
    }
};

/// Reads `_tracer-data` from `header`: the span context, or null when the field is absent.
/// Members of the JSON object that are not strings are ignored. Memory comes from `arena`.
pub fn readHeader(arena: Allocator, header: converter.Header) converter.DecodeError!?SpanContext {
    const p = header.get(header_key) orelse return null;
    const map = try converter.fromPayload(std.json.ArrayHashMap(std.json.Value), arena, p);
    var n: usize = 0;
    for (map.map.values()) |v| if (v == .string) {
        n += 1;
    };
    const fields = try arena.alloc(Field, n);
    var i: usize = 0;
    for (map.map.keys(), map.map.values()) |k, v| if (v == .string) {
        fields[i] = .{ .name = k, .value = v.string };
        i += 1;
    };
    return .{ .fields = fields };
}

/// Writes `context` as the `_tracer-data` field, a `json/plain` object with one string member
/// per field, in the order given; an empty context writes nothing. Memory comes from `arena`;
/// the names and values are borrowed until the header is sent.
pub fn writeHeader(arena: Allocator, header: *converter.HeaderWriter, context: SpanContext) error{OutOfMemory}!void {
    if (context.isEmpty()) return;
    var map: std.json.ArrayHashMap([]const u8) = .{};
    try map.map.ensureTotalCapacity(arena, context.fields.len);
    for (context.fields) |f| map.map.putAssumeCapacity(f.name, f.value);
    try header.put(header_key, try converter.toPayload(arena, map));
}

/// Puts `_tracer-data` in `header` back to `before` (null: absent), with its codec mark.
fn restoreHeader(header: *converter.HeaderWriter, before: ?Payload, encoded: bool) void {
    const p = before orelse return header.remove(header_key);
    const mode = header.encode_fields;
    defer header.encode_fields = mode;
    header.encode_fields = encoded;
    // The field exists, and a mark it loses keeps its list's capacity, so this does not
    // allocate; were it to fail, sending no context beats sending the wrong parent.
    header.put(header_key, p) catch header.remove(header_key);
}

/// The span context in `_tracer-data` of `header`, or null when it is absent or does not
/// decode: the interceptors never fail a call over a header they cannot read.
fn headerParent(arena: Allocator, header: converter.Header) error{OutOfMemory}!?SpanContext {
    return readHeader(arena, header) catch |err| switch (err) {
        error.OutOfMemory => error.OutOfMemory,
        else => null,
    };
}

/// The failure text for `err`, or null for the errors that end a call without failing it.
fn failureOf(err: anyerror) ?[]const u8 {
    return switch (err) {
        error.ContinueAsNew, error.CompleteAsync => null,
        else => @errorName(err),
    };
}

/// Puts `context` under `span_key` in `values`, in a new store on `arena` holding the same
/// entries, so the store the worker built is not grown in place (it may have none and no
/// memory of its own).
fn setCurrent(arena: Allocator, values: *propagation.Values, context: SpanContext) error{OutOfMemory}!void {
    if (context.isEmpty()) return;
    var next: propagation.Values = .init(arena);
    try next.entries.appendSlice(arena, values.entries.items);
    try next.put(span_key, context);
    values.* = next;
}

/// Experimental: may change or be removed without notice. Options of `startSpan`.
pub const WorkflowSpanOptions = struct {
    /// The span's attributes, given to the tracer as tags ahead of temporalWorkflowID and
    /// temporalRunID. Borrowed for the call only.
    attributes: []const Field = &.{},
};

/// Experimental: may change or be removed without notice. Starts a span named `name` from
/// workflow code: the workflow function, a signal or update handler, an update validator or a
/// query handler. Its parent is the calling coroutine's newest open span (one it started
/// with `startSpan`, or the span of the handler it runs in), else the run's `RunWorkflow`
/// span. While it is open, the activities, local activities, child workflows, signals and
/// Nexus operations the same coroutine starts are its children. End it with `end` or
/// `endWithFailure`:
///
///     const span = temporal.tracing.startSpan(ctx, "workflow-operation", .{});
///     defer span.end();
///
/// The span is inert (`.{}`, nothing is traced) when the run has no tracing `Interceptor`,
/// while the run is evicted, once the workflow function has returned, and while the
/// workflow replays history, except in update validators and query handlers, which never
/// come from history. It never fails: without memory for it, it logs a warning and returns
/// an inert span. It sends no command and reads nothing a tracer returns, so calls may be
/// added or removed without breaking the replay of existing histories. A span still open
/// when the workflow function returns is ended then, without a failure.
///
/// A coroutine started with `io.async`, `io.concurrent`, a `Group` or a `Select` does not
/// see its starter's open span: its outbound calls are children of the `RunWorkflow` span.
/// A span started while replaying and ended in live code is not emitted. Workflow code on
/// the run's worker thread only; with two tracing interceptors on one worker, the last one
/// installed gets the spans.
pub fn startSpan(ctx: *workflow.Context, name: []const u8, options: WorkflowSpanOptions) WorkflowSpan {
    const run = run_var.get(ctx) orelse return .{};
    return run.startApplication(name, options.attributes);
}

/// Experimental: may change or be removed without notice. A span workflow code started with
/// `startSpan`. A small value; copy freely: copies are the same span. `.{}` is inert.
pub const WorkflowSpan = struct {
    record: ?*Record = null,

    /// Ends the span. A second call, or a call on an inert span, does nothing.
    pub fn end(s: WorkflowSpan) void {
        const r = s.record orelse return;
        r.run.endFrame(r, .{});
    }

    /// Ends the span as failed; `failure` is borrowed for the call (it becomes
    /// `FinishSpanOptions.failure`). While the run is evicted, the span ends without one.
    pub fn endWithFailure(s: WorkflowSpan, failure: []const u8) void {
        const r = s.record orelse return;
        r.run.endFrame(r, r.run.finishFailure(failure));
    }
};

/// An open span of a run's coroutine: an inbound call's, on that call's stack, or one
/// `startSpan` started, in run arena memory never freed on its own. Linked into
/// `WorkflowRun.newest` while open, so recording an inbound call's span allocates nothing:
/// a query leaves no memory in the run.
const Record = struct {
    run: *WorkflowRun,
    /// The `Scheduler.Fiber` ID of the coroutine that started it; 0 outside a coroutine.
    owner: u32,
    kind: enum { inbound, application },
    span: Span,
    ended: bool = false,
    /// The next older open span of the run.
    older: ?*Record = null,
};

/// The run `startSpan` traces into, set by `Interceptor.interceptWorkflow`.
const run_var: workflow.LocalVar(?*WorkflowRun) = .{ .name = "temporal.tracing.run", .default = null };

/// The tracing interceptor. Holds only the tracer; no allocation, no deinit. It is borrowed
/// by everything it hands out, so it must not move and must outlive the client and workers.
pub const Interceptor = struct {
    tracer: Tracer,

    pub fn init(tracer: Tracer) Interceptor {
        return .{ .tracer = tracer };
    }

    /// For `client.Options.interceptors`.
    pub fn client(t: *Interceptor) interceptor.Client {
        return .{ .ptr = t, .vtable = &client_vtable };
    }

    /// For `client.Options.worker_interceptors` or `worker.Options.interceptors`.
    pub fn worker(t: *Interceptor) interceptor.Worker {
        return .{ .ptr = t, .vtable = &worker_vtable };
    }

    /// For `client.Options.propagators`: carries `span_key` in the `_tracer-data` field
    /// (`encode_fields = false`).
    pub fn propagator(t: *Interceptor) propagation.ContextPropagator {
        return .{ .ptr = t, .vtable = &propagator_vtable, .encode_fields = false };
    }

    fn of(ptr: *anyopaque) *Interceptor {
        return @ptrCast(@alignCast(ptr));
    }

    // ------------------------------------------------------------ propagator

    const propagator_vtable: propagation.ContextPropagator.VTable = .{ .inject = inject, .extract = extract };

    fn inject(_: ?*anyopaque, arena: Allocator, values: *const propagation.Values, header: *converter.HeaderWriter) error{OutOfMemory}!void {
        const context = values.get(span_key) orelse return;
        try writeHeader(arena, header, context);
    }

    fn extract(_: ?*anyopaque, arena: Allocator, header: converter.Header, values: *propagation.Values) converter.DecodeError!void {
        const context = (try readHeader(arena, header)) orelse return;
        try values.put(span_key, context);
    }

    // ------------------------------------------------------------ client

    const client_vtable: interceptor.Client.VTable = .{ .call = clientCall };

    const wfs = proto.temporal.api.workflowservice.v1;

    const ClientSpan = struct { operation: Operation, name: []const u8, workflow_id: []const u8, update_id: []const u8 = "" };

    /// The span a client request opens, or null for the methods that are not traced.
    fn clientSpan(call: *const interceptor.ClientCall) ?ClientSpan {
        if (call.request(wfs.StartWorkflowExecutionRequest)) |r|
            return .{ .operation = .start_workflow, .name = if (r.workflow_type) |w| w.name else "", .workflow_id = r.workflow_id };
        if (call.request(wfs.SignalWorkflowExecutionRequest)) |r|
            return .{ .operation = .signal_workflow, .name = r.signal_name, .workflow_id = if (r.workflow_execution) |e| e.workflow_id else "" };
        if (call.request(wfs.SignalWithStartWorkflowExecutionRequest)) |r|
            return .{ .operation = .signal_with_start_workflow, .name = if (r.workflow_type) |w| w.name else "", .workflow_id = r.workflow_id };
        if (call.request(wfs.QueryWorkflowRequest)) |r|
            return .{ .operation = .query_workflow, .name = if (r.query) |q| q.query_type else "", .workflow_id = if (r.execution) |e| e.workflow_id else "" };
        if (call.request(wfs.UpdateWorkflowExecutionRequest)) |r| {
            const req = r.request orelse return .{ .operation = .update_workflow, .name = "", .workflow_id = "" };
            return .{
                .operation = .update_workflow,
                .name = if (req.input) |i| i.name else "",
                .workflow_id = if (r.workflow_execution) |e| e.workflow_id else "",
                .update_id = if (req.meta) |m| m.update_id else "",
            };
        }
        return null;
    }

    fn clientCall(ptr: *anyopaque, call: *interceptor.ClientCall, next: interceptor.Client.Next) interceptor.ClientError!void {
        const t = of(ptr);
        const what = clientSpan(call) orelse return next.call(call);
        const header = call.header orelse return next.call(call);
        const parent = try headerParent(call.arena, header.header());
        var tags_buf: [2]Field = .{ .{ .name = tag_workflow_id, .value = what.workflow_id }, .{ .name = tag_update_id, .value = what.update_id } };
        const tags: []const Field = if (what.operation == .update_workflow) &tags_buf else tags_buf[0..1];
        const span = try t.tracer.startSpan(call.arena, .{ .operation = what.operation, .name = what.name, .parent = parent, .tags = tags });
        var failure: ?[]const u8 = null;
        defer t.tracer.finishSpan(span, .{ .failure = failure });
        // The client sends some requests again with the same header (an update it polls until
        // its stage is reached): put back what the propagator wrote, so each request's span
        // has the caller's context as its parent, not the span of the request before it.
        const before = header.header().get(header_key);
        const before_encoded = header.encodes(header_key);
        writeHeader(call.arena, header, span.context) catch |err| {
            failure = @errorName(err);
            return err;
        };
        defer if (!span.context.isEmpty()) restoreHeader(header, before, before_encoded);
        next.call(call) catch |err| {
            failure = @errorName(err);
            return err;
        };
    }

    // ------------------------------------------------------------ worker

    const worker_vtable: interceptor.Worker.VTable = .{
        .interceptWorkflow = interceptWorkflow,
        .interceptActivity = interceptActivity,
        .interceptNexusOperation = interceptNexusOperation,
    };

    fn interceptWorkflow(ptr: *anyopaque, ctx: *workflow.Context) error{OutOfMemory}!interceptor.Workflow {
        const run = try ctx.allocator().create(WorkflowRun);
        run.* = .{ .t = of(ptr), .ctx = ctx, .run_context = (try headerParent(ctx.allocator(), ctx.headers())) orelse .{} };
        run_var.set(ctx, run) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            // Not reached: workflows are intercepted outside read-only code.
            error.ReadOnly => {},
        };
        return .{ .inbound = interceptor.WorkflowInbound.from(WorkflowRun, run), .outbound = interceptor.WorkflowOutbound.from(WorkflowRun, run) };
    }

    fn interceptActivity(ptr: *anyopaque, ctx: *activity.Context) error{OutOfMemory}!interceptor.Activity {
        const attempt = try ctx.allocator().create(ActivityAttempt);
        attempt.* = .{ .t = of(ptr), .ctx = ctx };
        return .{ .inbound = interceptor.ActivityInbound.from(ActivityAttempt, attempt) };
    }

    fn interceptNexusOperation(ptr: *anyopaque, ctx: *nexus.Context) error{OutOfMemory}!interceptor.NexusOperation {
        const request = try ctx.allocator().create(NexusRequest);
        request.* = .{ .t = of(ptr), .ctx = ctx };
        return .{ .inbound = interceptor.NexusOperationInbound.from(NexusRequest, request) };
    }
};

/// One workflow run's tracing state.
const WorkflowRun = struct {
    t: *Interceptor,
    ctx: *workflow.Context,
    /// The parent of the run's handler and outbound spans: the RunWorkflow span's context
    /// when that span was started in this process, else the start header's.
    run_context: SpanContext,
    /// The newest open span of the run's coroutines; the others follow by `Record.older`.
    newest: ?*Record = null,
    /// Set once `executeWorkflow` has returned: `startSpan` is inert from then on.
    closed: bool = false,

    fn tracer(r: *const WorkflowRun) Tracer {
        return r.t.tracer;
    }

    /// The failure to finish a workflow span with: none while the run is being evicted,
    /// which is not a failure of the workflow.
    fn finishFailure(r: *const WorkflowRun, failure: ?[]const u8) FinishSpanOptions {
        return .{ .failure = if (r.ctx.instance.evicting) null else failure };
    }

    /// Starts a span of an inbound call, parented to the call's header or the run's context.
    fn startInbound(r: *WorkflowRun, arena: Allocator, operation: Operation, name: []const u8, header: converter.Header, update_id: ?[]const u8, start_time: ?std.Io.Timestamp) error{OutOfMemory}!Span {
        const info = r.ctx.info();
        const parent = (try headerParent(arena, header)) orelse r.run_context;
        const tags = [_]Field{
            .{ .name = tag_workflow_id, .value = info.workflow_id },
            .{ .name = tag_run_id, .value = info.run_id },
            .{ .name = tag_update_id, .value = update_id orelse "" },
        };
        return r.tracer().startSpan(arena, .{
            .operation = operation,
            .name = name,
            .parent = parent,
            .start_time = start_time,
            .tags = if (update_id != null) &tags else tags[0..2],
        });
    }

    /// The `Scheduler.Fiber` ID of the calling coroutine, or 0 outside one.
    fn currentOwner(r: *const WorkflowRun) u32 {
        return if (r.ctx.instance.sched.currentFiber()) |f| f.id else 0;
    }

    /// The calling coroutine's newest open frame, or null.
    fn innermost(r: *const WorkflowRun) ?*Record {
        const owner = r.currentOwner();
        var at = r.newest;
        while (at) |f| : (at = f.older) if (f.owner == owner) return f;
        return null;
    }

    /// Links `f` as the newest open span, opened by the calling coroutine.
    fn pushFrame(r: *WorkflowRun, f: *Record) void {
        f.owner = r.currentOwner();
        f.older = r.newest;
        r.newest = f;
    }

    /// Records an inbound call's `span` as open in the calling coroutine, in `frame`, which
    /// lives on the call's stack until `removeFrame`.
    fn pushInbound(r: *WorkflowRun, frame: *Record, span: Span) *Record {
        frame.* = .{ .run = r, .owner = 0, .kind = .inbound, .span = span };
        r.pushFrame(frame);
        return frame;
    }

    /// Unlinks `f`, found by identity, and marks it ended.
    fn removeFrame(r: *WorkflowRun, f: *Record) void {
        f.ended = true;
        var link = &r.newest;
        while (link.*) |item| : (link = &item.older) if (item == f) {
            link.* = item.older;
            return;
        };
    }

    /// Ends an application span once: `WorkflowSpan.end` and `endWithFailure`.
    fn endFrame(r: *WorkflowRun, f: *Record, options: FinishSpanOptions) void {
        if (f.ended) return;
        r.removeFrame(f);
        r.tracer().finishSpan(f.span, options);
    }

    /// `startSpan` for this run; see there for when the span is inert.
    fn startApplication(r: *WorkflowRun, name: []const u8, attributes: []const Field) WorkflowSpan {
        const inst = r.ctx.instance;
        if (inst.evicting or r.closed) return .{};
        if (r.ctx.isReplaying() and inst.readOnly() == null) return .{};
        const a = inst.arena();
        const info = r.ctx.info();
        const tags = a.alloc(Field, attributes.len + 2) catch return r.applicationOutOfMemory(name);
        defer a.free(tags);
        @memcpy(tags[0..attributes.len], attributes);
        tags[attributes.len] = .{ .name = tag_workflow_id, .value = info.workflow_id };
        tags[attributes.len + 1] = .{ .name = tag_run_id, .value = info.run_id };
        const parent = if (r.innermost()) |f| f.span.context else r.run_context;
        const frame = a.create(Record) catch return r.applicationOutOfMemory(name);
        const span = r.tracer().startSpan(a, .{ .operation = .application, .name = name, .parent = parent, .tags = tags }) catch {
            a.destroy(frame);
            return r.applicationOutOfMemory(name);
        };
        frame.* = .{ .run = r, .owner = 0, .kind = .application, .span = span };
        r.pushFrame(frame);
        return .{ .record = frame };
    }

    fn applicationOutOfMemory(r: *WorkflowRun, name: []const u8) WorkflowSpan {
        r.ctx.logger().warn("out of memory starting the tracing span; the workflow goes on untraced", .{ .Name = name });
        return .{};
    }

    /// Ends every application span still open, without a failure, and makes `startSpan`
    /// inert: the workflow function has returned, or its run is being evicted.
    fn close(r: *WorkflowRun) void {
        r.closed = true;
        var link = &r.newest;
        while (link.*) |f| {
            if (f.kind != .application) {
                link = &f.older;
                continue;
            }
            link.* = f.older;
            f.ended = true;
            r.tracer().finishSpan(f.span, .{});
        }
    }

    pub fn executeWorkflow(r: *WorkflowRun, input: interceptor.ExecuteWorkflowInput, next: interceptor.WorkflowInbound.Next) anyerror!Payload {
        defer r.close();
        // A run rebuilt from history after a first workflow task that sent no commands runs
        // its function live as far as the core is concerned, but the function already ran
        // once: a task completed before this one.
        if (r.ctx.isReplaying() or r.ctx.instance.task_completed_before) return next.executeWorkflow(input);
        const span = try r.startInbound(r.ctx.allocator(), .run_workflow, input.workflow_type, input.header, null, r.ctx.info().start_time);
        var failure: ?[]const u8 = null;
        defer r.tracer().finishSpan(span, r.finishFailure(failure));
        if (!span.context.isEmpty()) r.run_context = span.context;
        var frame_memory: Record = undefined;
        const frame = r.pushInbound(&frame_memory, span);
        defer r.removeFrame(frame);
        return next.executeWorkflow(input) catch |err| {
            failure = failureOf(err);
            return err;
        };
    }

    pub fn handleSignal(r: *WorkflowRun, input: interceptor.HandleSignalInput, next: interceptor.WorkflowInbound.Next) anyerror!void {
        if (r.ctx.isReplaying()) return next.handleSignal(input);
        const span = try r.startInbound(r.ctx.allocator(), .handle_signal, input.signal_name, input.header, null, null);
        var failure: ?[]const u8 = null;
        defer r.tracer().finishSpan(span, r.finishFailure(failure));
        var frame_memory: Record = undefined;
        const frame = r.pushInbound(&frame_memory, span);
        defer r.removeFrame(frame);
        return next.handleSignal(input) catch |err| {
            failure = failureOf(err);
            return err;
        };
    }

    pub fn handleQuery(r: *WorkflowRun, input: interceptor.HandleQueryInput, next: interceptor.WorkflowInbound.Next) anyerror!Payload {
        const span = try r.startInbound(input.arena, .handle_query, input.query_type, input.header, null, null);
        var failure: ?[]const u8 = null;
        defer r.tracer().finishSpan(span, .{ .failure = failure });
        var frame_memory: Record = undefined;
        const frame = r.pushInbound(&frame_memory, span);
        defer r.removeFrame(frame);
        return next.handleQuery(input) catch |err| {
            failure = failureOf(err);
            return err;
        };
    }

    pub fn validateUpdate(r: *WorkflowRun, input: interceptor.UpdateInput, next: interceptor.WorkflowInbound.Next) anyerror!void {
        const span = try r.startInbound(r.ctx.allocator(), .validate_update, input.update_name, input.header, input.update_id, null);
        var failure: ?[]const u8 = null;
        defer r.tracer().finishSpan(span, .{ .failure = failure });
        var frame_memory: Record = undefined;
        const frame = r.pushInbound(&frame_memory, span);
        defer r.removeFrame(frame);
        return next.validateUpdate(input) catch |err| {
            failure = failureOf(err);
            return err;
        };
    }

    pub fn executeUpdate(r: *WorkflowRun, input: interceptor.UpdateInput, next: interceptor.WorkflowInbound.Next) anyerror!Payload {
        if (r.ctx.isReplaying()) return next.executeUpdate(input);
        const span = try r.startInbound(r.ctx.allocator(), .handle_update, input.update_name, input.header, input.update_id, null);
        var failure: ?[]const u8 = null;
        defer r.tracer().finishSpan(span, r.finishFailure(failure));
        var frame_memory: Record = undefined;
        const frame = r.pushInbound(&frame_memory, span);
        defer r.removeFrame(frame);
        return next.executeUpdate(input) catch |err| {
            failure = failureOf(err);
            return err;
        };
    }

    /// Starts the span of an outbound call, or returns null: while replaying, or when the
    /// span cannot be started for lack of memory (logged), in which case the call goes on
    /// untraced, since tracing must not change what a workflow does. Its parent is the
    /// calling coroutine's newest open span when `startSpan` started that one, else the
    /// run's context.
    fn startOutbound(r: *WorkflowRun, operation: Operation, name: []const u8) ?Span {
        if (r.ctx.isReplaying()) return null;
        const info = r.ctx.info();
        const tags = [_]Field{
            .{ .name = tag_workflow_id, .value = info.workflow_id },
            .{ .name = tag_run_id, .value = info.run_id },
        };
        const parent = if (r.innermost()) |f| (if (f.kind == .application) f.span.context else r.run_context) else r.run_context;
        return r.tracer().startSpan(r.ctx.allocator(), .{ .operation = operation, .name = name, .parent = parent, .tags = &tags }) catch {
            r.ctx.logger().warn("out of memory starting the tracing span; the call goes on untraced", .{ .Operation = operation.text(), .Name = name });
            return null;
        };
    }

    /// Writes `span`'s context into `header`; on failure the header stays as it was and the
    /// call goes on untraced (logged).
    fn writeOutbound(r: *WorkflowRun, header: *converter.HeaderWriter, span: Span) void {
        writeHeader(r.ctx.allocator(), header, span.context) catch
            r.ctx.logger().warn("out of memory writing the tracing header; the call goes on untraced", .{});
    }

    pub fn executeActivity(r: *WorkflowRun, input: interceptor.ActivityInput, next: interceptor.WorkflowOutbound.Next) workflow.ActivityFuture(Payload) {
        const span = r.startOutbound(.start_activity, input.activity_type) orelse return next.executeActivity(input);
        defer r.tracer().finishSpan(span, .{});
        r.writeOutbound(input.header, span);
        return next.executeActivity(input);
    }

    pub fn executeLocalActivity(r: *WorkflowRun, input: interceptor.LocalActivityInput, next: interceptor.WorkflowOutbound.Next) workflow.ActivityFuture(Payload) {
        const span = r.startOutbound(.start_activity, input.activity_type) orelse return next.executeLocalActivity(input);
        defer r.tracer().finishSpan(span, .{});
        r.writeOutbound(input.header, span);
        return next.executeLocalActivity(input);
    }

    pub fn executeChildWorkflow(r: *WorkflowRun, input: interceptor.ChildWorkflowInput, next: interceptor.WorkflowOutbound.Next) workflow.ChildWorkflowFuture(Payload) {
        const span = r.startOutbound(.start_child_workflow, input.workflow_type) orelse return next.executeChildWorkflow(input);
        defer r.tracer().finishSpan(span, .{});
        r.writeOutbound(input.header, span);
        return next.executeChildWorkflow(input);
    }

    pub fn signalExternalWorkflow(r: *WorkflowRun, input: interceptor.SignalExternalInput, next: interceptor.WorkflowOutbound.Next) workflow.SignalExternalFuture {
        const operation: Operation = if (input.child) .signal_child_workflow else .signal_external_workflow;
        const span = r.startOutbound(operation, input.signal_name) orelse return next.signalExternalWorkflow(input);
        defer r.tracer().finishSpan(span, .{});
        r.writeOutbound(input.header, span);
        return next.signalExternalWorkflow(input);
    }

    pub fn executeNexusOperation(r: *WorkflowRun, input: interceptor.NexusOperationInput, next: interceptor.WorkflowOutbound.Next) workflow.NexusOperationFuture(Payload) {
        const a = r.ctx.allocator();
        const name = std.mem.concat(a, u8, &.{ input.service, "/", input.operation }) catch {
            r.ctx.logger().warn("out of memory starting the tracing span; the call goes on untraced", .{ .Operation = Operation.start_nexus_operation.text() });
            return next.executeNexusOperation(input);
        };
        const span = r.startOutbound(.start_nexus_operation, name) orelse return next.executeNexusOperation(input);
        defer r.tracer().finishSpan(span, .{});
        var changed = input;
        changed.options.headers = withNexusHeaders(a, input.options.headers, span.context) catch blk: {
            r.ctx.logger().warn("out of memory writing the tracing Nexus headers; the call goes on untraced", .{});
            break :blk input.options.headers;
        };
        return next.executeNexusOperation(changed);
    }
};

/// `headers` with a header for each field of `context`, replacing those of the same name
/// (ignoring case). Allocated from `arena`; `headers` itself when `context` is empty.
fn withNexusHeaders(arena: Allocator, headers: []const nexus.Header, context: SpanContext) error{OutOfMemory}![]const nexus.Header {
    if (context.isEmpty()) return headers;
    var out: std.ArrayList(nexus.Header) = try .initCapacity(arena, headers.len + context.fields.len);
    for (headers) |h| {
        if (context.get(h.name) != null) continue;
        out.appendAssumeCapacity(h);
    }
    for (context.fields) |f| out.appendAssumeCapacity(.{ .name = f.name, .value = f.value });
    return out.items;
}

/// One activity attempt's tracing.
const ActivityAttempt = struct {
    t: *Interceptor,
    ctx: *activity.Context,

    pub fn execute(r: *ActivityAttempt, input: interceptor.ExecuteActivityInput, next: interceptor.ActivityInbound.Next) anyerror!Payload {
        const a = r.ctx.allocator();
        const info = r.ctx.info();
        const parent = try headerParent(a, input.header);
        var tags: [3]Field = undefined;
        var n: usize = 0;
        for ([_]Field{
            .{ .name = tag_workflow_id, .value = info.workflow_id },
            .{ .name = tag_run_id, .value = info.workflow_run_id },
            .{ .name = tag_activity_id, .value = info.activity_id },
        }) |tag| if (tag.value.len > 0) {
            tags[n] = tag;
            n += 1;
        };
        const span = try r.t.tracer.startSpan(a, .{ .operation = .run_activity, .name = input.activity_type, .parent = parent, .start_time = info.started_time, .tags = tags[0..n] });
        var failure: ?[]const u8 = null;
        defer r.t.tracer.finishSpan(span, .{ .failure = failure });
        setCurrent(a, &r.ctx.propagated_, span.context) catch |err| {
            failure = @errorName(err);
            return err;
        };
        return next.execute(input) catch |err| {
            failure = failureOf(err);
            return err;
        };
    }
};

/// One Nexus request's tracing.
const NexusRequest = struct {
    t: *Interceptor,
    ctx: *nexus.Context,

    const out_of_memory: nexus.HandlerError = .{ .type = .internal, .message = "out of memory starting the tracing span" };

    fn start(r: *NexusRequest, operation: Operation, service: []const u8, op_name: []const u8, parent: ?SpanContext) error{OutOfMemory}!Span {
        const a = r.ctx.allocator();
        const name = try std.mem.concat(a, u8, &.{ service, "/", op_name });
        const span = try r.t.tracer.startSpan(a, .{ .operation = operation, .name = name, .parent = parent });
        setCurrent(a, &r.ctx.propagated_, span.context) catch |err| {
            r.t.tracer.finishSpan(span, .{ .failure = @errorName(err) });
            return err;
        };
        return span;
    }

    /// The failure text of a handler error: its type and message.
    fn handlerFailure(r: *NexusRequest, e: nexus.HandlerError) []const u8 {
        return std.fmt.allocPrint(r.ctx.allocator(), "{t}: {s}", .{ e.type, e.message }) catch @tagName(e.type);
    }

    pub fn startOperation(r: *NexusRequest, input: interceptor.StartNexusOperationInput, next: interceptor.NexusOperationInbound.Next) nexus.StartResult {
        const a = r.ctx.allocator();
        const fields = a.alloc(Field, input.headers.len) catch return .{ .handler_error = out_of_memory };
        for (fields, input.headers) |*f, h| f.* = .{ .name = h.name, .value = h.value };
        const span = r.start(.run_start_nexus_operation_handler, input.service, input.operation, .{ .fields = fields }) catch
            return .{ .handler_error = out_of_memory };
        const result = next.startOperation(input);
        r.t.tracer.finishSpan(span, .{ .failure = switch (result) {
            .succeeded, .started => null,
            .handler_error => |e| r.handlerFailure(e),
            .operation_error => |e| std.fmt.allocPrint(a, "operation {t}: {s}", .{ e.state, e.failure.message }) catch @tagName(e.state),
        } });
        return result;
    }

    pub fn cancelOperation(r: *NexusRequest, input: interceptor.CancelNexusOperationInput, next: interceptor.NexusOperationInbound.Next) nexus.CancelResult {
        const span = r.start(.run_cancel_nexus_operation_handler, input.service, input.operation, r.ctx.propagated().get(span_key)) catch
            return .{ .handler_error = out_of_memory };
        const result = next.cancelOperation(input);
        r.t.tracer.finishSpan(span, .{ .failure = switch (result) {
            .requested => null,
            .handler_error => |e| r.handlerFailure(e),
        } });
        return result;
    }
};

// ---------------------------------------------------------------- tests

const testing = std.testing;

test "operations name themselves as the other SDKs do, and span names join operation and name" {
    try testing.expectEqualStrings("RunCancelNexusOperationHandler", Operation.run_cancel_nexus_operation_handler.text());
    try testing.expectEqualStrings("SignalWithStartWorkflow", Operation.signal_with_start_workflow.text());
    try testing.expectEqual(Kind.client, Operation.start_activity.kind());
    try testing.expectEqual(Kind.client, Operation.update_workflow.kind());
    try testing.expectEqual(Kind.server, Operation.validate_update.kind());
    try testing.expectEqualStrings("SignalExternalWorkflow", Operation.signal_external_workflow.text());
    try testing.expectEqualStrings("SignalChildWorkflow", Operation.signal_child_workflow.text());
    try testing.expectEqual(Kind.client, Operation.signal_external_workflow.kind());
    try testing.expectEqual(Kind.client, Operation.signal_child_workflow.kind());
    try testing.expectEqualStrings("", Operation.application.text());
    try testing.expectEqual(Kind.internal, Operation.application.kind());
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const o: StartSpanOptions = .{ .operation = .run_activity, .name = "Charge" };
    try testing.expectEqualStrings("RunActivity:Charge", try o.spanName(arena.allocator()));
    const app: StartSpanOptions = .{ .operation = .application, .name = "workflow-operation" };
    try testing.expectEqualStrings("workflow-operation", try app.spanName(arena.allocator()));
    const signal: StartSpanOptions = .{ .operation = .signal_child_workflow, .name = "go" };
    try testing.expectEqualStrings("SignalChildWorkflow:go", try signal.spanName(arena.allocator()));
}

test "a span context round-trips through _tracer-data as a JSON object of strings" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var w: converter.HeaderWriter = .init(a);
    try writeHeader(a, &w, .{});
    try testing.expectEqual(@as(usize, 0), w.fields.items.len);
    try testing.expectEqual(@as(?SpanContext, null), try readHeader(a, w.header()));

    const fields = [_]Field{ .{ .name = "traceparent", .value = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01" }, .{ .name = "tracestate", .value = "k=v" } };
    try writeHeader(a, &w, .{ .fields = &fields });
    const p = w.header().get(header_key).?;
    try testing.expectEqualStrings("json/plain", converter.encodingOf(p).?);
    try testing.expectEqualStrings("{\"traceparent\":\"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01\",\"tracestate\":\"k=v\"}", p.data);
    const back = (try readHeader(a, w.header())).?;
    try testing.expectEqual(@as(usize, 2), back.fields.len);
    try testing.expectEqualStrings("k=v", back.get("TraceState").?);
    try testing.expectEqualStrings(fields[0].value, back.get("traceparent").?);
    try testing.expectEqual(@as(?[]const u8, null), back.get("baggage"));

    // Non-string members are ignored; something other than an object is an error.
    try w.put(header_key, try converter.payloadWithEncoding(a, "json/plain", "{\"a\":1,\"b\":\"x\",\"c\":null}"));
    const mixed = (try readHeader(a, w.header())).?;
    try testing.expectEqual(@as(usize, 1), mixed.fields.len);
    try testing.expectEqualStrings("x", mixed.get("b").?);
    try w.put(header_key, try converter.payloadWithEncoding(a, "json/plain", "[\"a\"]"));
    try testing.expectError(error.InvalidPayload, readHeader(a, w.header()));
    try testing.expectEqual(@as(?SpanContext, null), try headerParent(a, w.header()));
}

test "the propagator carries span_key only when it holds a span, and a store gains the current span without changing the old one" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const Null = struct {
        pub fn startSpan(_: *@This(), _: Allocator, _: StartSpanOptions) error{OutOfMemory}!Span {
            return .{};
        }
        pub fn finishSpan(_: *@This(), _: Span, _: FinishSpanOptions) void {}
    };
    var n: Null = .{};
    var t: Interceptor = .init(Tracer.from(Null, &n));
    const props = [_]propagation.ContextPropagator{t.propagator()};

    var w: converter.HeaderWriter = .init(a);
    var values: propagation.Values = .init(a);
    try values.put(span_key, .{});
    try propagation.injectAll(&props, a, &values, &w);
    try testing.expectEqual(@as(usize, 0), w.fields.items.len);

    const fields = [_]Field{.{ .name = "span", .value = "7" }};
    try values.put(span_key, .{ .fields = &fields });
    try propagation.injectAll(&props, a, &values, &w);
    try testing.expect(!w.encodes(header_key));
    var got: propagation.Values = .init(a);
    try propagation.extractAll(&props, a, w.header(), &got, "a test call");
    try testing.expectEqualStrings("7", got.get(span_key).?.get("span").?);

    var empty: propagation.Values = .empty;
    try setCurrent(a, &empty, .{ .fields = &fields });
    try testing.expectEqualStrings("7", empty.get(span_key).?.get("span").?);
    const tenant = propagation.key([]const u8, "tenant");
    var old: propagation.Values = .init(a);
    try old.put(tenant, "acme");
    var current = old;
    try setCurrent(a, &current, .{ .fields = &fields });
    try testing.expectEqualStrings("acme", current.get(tenant).?);
    try testing.expect(current.get(span_key) != null);
    try testing.expect(old.get(span_key) == null);
}

test "Nexus headers gain the context's fields, replacing those of the same name ignoring case" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const given = [_]nexus.Header{ .{ .name = "TraceParent", .value = "old" }, .{ .name = "x-other", .value = "kept" } };
    try testing.expectEqual(@as([]const nexus.Header, &given), try withNexusHeaders(arena.allocator(), &given, .{}));
    const fields = [_]Field{.{ .name = "traceparent", .value = "new" }};
    const out = try withNexusHeaders(arena.allocator(), &given, .{ .fields = &fields });
    try testing.expectEqual(@as(usize, 2), out.len);
    try testing.expectEqualStrings("x-other", out[0].name);
    try testing.expectEqualStrings("new", out[1].value);
}
