//! Nexus end to end against a dev server: caller workflows call operations through an
//! endpoint, and a worker's Nexus task loop runs the handlers.

const std = @import("std");
const client_mod = @import("client.zig");
const converter = @import("converter.zig");
const failure_mod = @import("failure.zig");
const nexus = @import("nexus.zig");
const proto = @import("proto.zig");
const testing_mod = @import("testing.zig");
const worker_mod = @import("worker.zig");
const workflow = @import("workflow.zig");
const Io = std.Io;
const testing = std.testing;
const EventType = proto.temporal.api.enums.v1.EventType;
const WorkflowEnvironment = testing_mod.WorkflowEnvironment;

// ---------------------------------------------------------------- handler side

const Echo = nexus.OperationDef("echo", []const u8, []const u8);
const Greet = nexus.OperationDef("greet", []const u8, []const u8);
const Hold = nexus.OperationDef("hold", []const u8, []const u8);
const Fail = nexus.OperationDef("fail", []const u8, []const u8);
const Flaky = nexus.OperationDef("flaky", []const u8, []const u8);
const Refuse = nexus.OperationDef("refuse", []const u8, []const u8);
const LongId = nexus.OperationDef("long-id", []const u8, []const u8);
const Block = nexus.OperationDef("block", []const u8, []const u8);
const Service = nexus.ServiceDef("zig-test-service", .{ Echo, Greet, Hold, Fail, Flaky, Refuse, LongId, Block });

fn greetWorkflow(ctx: *workflow.Context, name: []const u8) ![]const u8 {
    return std.fmt.allocPrint(ctx.allocator(), "Hello, {s}", .{name});
}
const GreetWorkflow = workflow.Def("NexusTestGreet", greetWorkflow);

const Release = workflow.SignalDef("release", fn () void);

const Latch = struct {
    open: bool = false,
    fn set(l: *Latch) void {
        l.open = true;
    }
    fn isOpen(l: *Latch) bool {
        return l.open;
    }
};

/// Waits for `release`. Canceled first, it still waits for `release` and only then ends
/// canceled, so a test sees the operation outlive its cancellation.
fn holdWorkflow(ctx: *workflow.Context, name: []const u8) ![]const u8 {
    const latch = try ctx.allocator().create(Latch);
    latch.* = .{};
    try ctx.setSignalHandler(Release, latch, Latch.set);
    ctx.await(latch, Latch.isOpen) catch |err| switch (err) {
        error.Canceled => {
            try ctx.await(latch, Latch.isOpen);
            return error.Canceled;
        },
        else => return err,
    };
    return std.fmt.allocPrint(ctx.allocator(), "released {s}", .{name});
}
const HoldWorkflow = workflow.Def("NexusTestHold", holdWorkflow);

const no_reason = std.math.maxInt(u8);

/// The handlers' shared state: attempt counts, and what a blocking handler saw.
const Handlers = struct {
    io: Io,
    fail_calls: std.atomic.Value(u32) = .init(0),
    flaky_calls: std.atomic.Value(u32) = .init(0),
    refuse_calls: std.atomic.Value(u32) = .init(0),
    long_id_calls: std.atomic.Value(u32) = .init(0),
    /// Set when `block` starts, and when it is canceled with the reason in `reason`.
    entered: Io.Event = .unset,
    canceled: Io.Event = .unset,
    reason: std.atomic.Value(u8) = .init(no_reason),

    /// Returns the input with what the handler knows about its request.
    fn echo(_: *Handlers, ctx: *nexus.Context, input: []const u8) ![]const u8 {
        const i = ctx.info();
        const link_workflow = if (i.links.len > 0) i.links[0].variant.?.workflow_event.workflow_id else "none";
        return std.fmt.allocPrint(ctx.allocator(), "{s} op={s}/{s} ns={s} endpoint={s} queue={s} trace={s} request_id={} link={s} deadline={}", .{
            input,                i.service,     i.operation,                i.namespace, i.endpoint, i.task_queue, i.header("Trace-Id") orelse "none",
            i.request_id.len > 0, link_workflow, i.request_deadline != null,
        });
    }

    fn greet(ctx: *nexus.Context, name: []const u8) !nexus.WorkflowStart(GreetWorkflow) {
        return .{ .options = .{ .id = try std.fmt.allocPrint(ctx.allocator(), "greet-{s}", .{name}) }, .args = .{name} };
    }

    fn hold(_: *nexus.Context, id: []const u8) !nexus.WorkflowStart(HoldWorkflow) {
        return .{ .options = .{ .id = id }, .args = .{id} };
    }

    fn fail(h: *Handlers, ctx: *nexus.Context, input: []const u8) ![]const u8 {
        _ = h.fail_calls.fetchAdd(1, .monotonic);
        return ctx.fail(.{ .message = try std.fmt.allocPrint(ctx.allocator(), "out of stock: {s}", .{input}), .type = "OutOfStock" });
    }

    fn flaky(h: *Handlers, ctx: *nexus.Context, input: []const u8) ![]const u8 {
        const attempt = h.flaky_calls.fetchAdd(1, .monotonic) + 1;
        if (attempt == 1) return ctx.handlerError(.{ .type = .unavailable, .message = "not yet" });
        return std.fmt.allocPrint(ctx.allocator(), "{s} on attempt {d}", .{ input, attempt });
    }

    fn refuse(h: *Handlers, ctx: *nexus.Context, _: []const u8) ![]const u8 {
        _ = h.refuse_calls.fetchAdd(1, .monotonic);
        return ctx.handlerError(.{ .type = .internal, .message = "refused for good", .retry = .non_retryable });
    }

    /// Starts a workflow whose ID is longer than the server accepts.
    fn longId(h: *Handlers, ctx: *nexus.Context, name: []const u8) !nexus.WorkflowStart(GreetWorkflow) {
        _ = h.long_id_calls.fetchAdd(1, .monotonic);
        const id = try ctx.allocator().alloc(u8, 4000);
        @memset(id, 'x');
        return .{ .options = .{ .id = id }, .args = .{name} };
    }

    /// Blocks until canceled, and records why.
    fn block(h: *Handlers, ctx: *nexus.Context, _: []const u8) ![]const u8 {
        h.entered.set(h.io);
        ctx.io().sleep(.fromSeconds(3600), .awake) catch |err| {
            if (ctx.cancelReason()) |r| _ = h.reason.cmpxchgStrong(no_reason, @backingInt(r), .acq_rel, .acquire);
            h.canceled.set(h.io);
            return err;
        };
        return "woke";
    }

    fn cancelReason(h: *const Handlers) ?nexus.CancelReason {
        const r = h.reason.load(.acquire);
        return if (r == no_reason) null else @fromBackingInt(@intCast(r));
    }
};

const Handler = nexus.ServiceHandler(Service, .{
    nexus.OperationHandler(Echo, Handlers.echo),
    nexus.OperationHandler(Greet, Handlers.greet),
    nexus.OperationHandler(Hold, Handlers.hold),
    nexus.OperationHandler(Fail, Handlers.fail),
    nexus.OperationHandler(Flaky, Handlers.flaky),
    nexus.OperationHandler(Refuse, Handlers.refuse),
    nexus.OperationHandler(LongId, Handlers.longId),
    nexus.OperationHandler(Block, Handlers.block),
});

// ---------------------------------------------------------------- caller side

/// The failure's cause chain: each failure's message and kind, outermost first.
fn describe(ctx: *workflow.Context, f: ?*const failure_mod.Failure) ![]const u8 {
    var out: Io.Writer.Allocating = .init(ctx.allocator());
    const w = &out.writer;
    var it = f;
    while (it) |cur| : (it = cur.cause) {
        if (cur != f.?) w.writeAll(" <- ") catch return error.OutOfMemory;
        w.print("{s} [", .{cur.message}) catch return error.OutOfMemory;
        if (cur.failure_info) |info| switch (info) {
            .application_failure_info => |a| w.print("application {s}", .{a.type}) catch return error.OutOfMemory,
            .nexus_handler_failure_info => |h| w.print("handler {s}", .{h.type}) catch return error.OutOfMemory,
            else => |tag| w.print("{t}", .{tag}) catch return error.OutOfMemory,
        };
        w.writeAll("]") catch return error.OutOfMemory;
    }
    return out.written();
}

const call_options: workflow.NexusOperationOptions = .{
    .endpoint = "",
    .schedule_to_close_timeout = .fromSeconds(60),
    .headers = &.{.{ .name = "trace-id", .value = "abc" }},
};

/// Calls `operation` by name with `input`, and returns its output or "failed: " and the
/// failure's cause chain.
fn callByName(ctx: *workflow.Context, endpoint: []const u8, operation: []const u8, input: []const u8) ![]const u8 {
    var options = call_options;
    options.endpoint = endpoint;
    const f = ctx.executeNexusOperationByName([]const u8, Service.service_name, operation, options, input);
    return f.get() catch |err| switch (err) {
        error.NexusOperationFailed => std.fmt.allocPrint(ctx.allocator(), "failed: {s}", .{try describe(ctx, f.failure())}),
        else => err,
    };
}
const CallByName = workflow.Def("NexusTestCallByName", callByName);

/// Calls `Echo` through its contract.
fn callEcho(ctx: *workflow.Context, endpoint: []const u8, input: []const u8) ![]const u8 {
    var options = call_options;
    options.endpoint = endpoint;
    return ctx.executeNexusOperation(Service, Echo, options, input).get();
}
const CallEcho = workflow.Def("NexusTestCallEcho", callEcho);

/// Calls `Greet`, and returns its result and whether it started with a token.
fn callGreet(ctx: *workflow.Context, endpoint: []const u8, name: []const u8) ![]const u8 {
    var options = call_options;
    options.endpoint = endpoint;
    const f = ctx.executeNexusOperation(Service, Greet, options, name);
    const token = (try f.started()) orelse return error.StartedWithoutToken;
    return std.fmt.allocPrint(ctx.allocator(), "{s} (token of {d} bytes)", .{ try f.get(), token.len });
}
const CallGreet = workflow.Def("NexusTestCallGreet", callGreet);

/// Starts `Hold` as workflow `hold_id`, cancels it once started with `cancellation`, and
/// returns how the operation ended.
fn cancelHold(ctx: *workflow.Context, endpoint: []const u8, hold_id: []const u8, cancellation: []const u8) ![]const u8 {
    var options = call_options;
    options.endpoint = endpoint;
    options.cancellation_type = std.meta.stringToEnum(workflow.NexusOperationCancellationType, cancellation) orelse return error.UnknownCancellationType;
    const f = ctx.executeNexusOperation(Service, Hold, options, hold_id);
    _ = try f.started();
    f.cancel();
    const result = f.get() catch |err| switch (err) {
        error.NexusOperationFailed => return std.fmt.allocPrint(ctx.allocator(), "failed: {s}", .{try describe(ctx, f.failure())}),
        else => return err,
    };
    return std.fmt.allocPrint(ctx.allocator(), "completed: {s}", .{result});
}
const CancelHold = workflow.Def("NexusTestCancelHold", cancelHold);

/// Starts `Hold` as workflow `hold_id`, abandons it once started, and returns how the
/// operation resolved.
fn abandonHold(ctx: *workflow.Context, endpoint: []const u8, hold_id: []const u8) ![]const u8 {
    var options = call_options;
    options.endpoint = endpoint;
    options.cancellation_type = .abandon;
    const f = ctx.executeNexusOperation(Service, Hold, options, hold_id);
    _ = try f.started();
    f.cancel();
    const outcome = if (f.get()) |r| r else |err| switch (err) {
        error.NexusOperationFailed => try describe(ctx, f.failure()),
        else => return err,
    };
    return std.fmt.allocPrint(ctx.allocator(), "abandoned: {s}", .{outcome});
}
const AbandonHold = workflow.Def("NexusTestAbandonHold", abandonHold);

// ---------------------------------------------------------------- setup

/// An environment whose worker runs the callers, the handler workflows and `Handler`,
/// and an endpoint routed to it.
const Setup = struct {
    env: *WorkflowEnvironment,
    endpoint: []const u8,
    arena: std.heap.ArenaAllocator,

    fn start(handlers: *Handlers, options: WorkflowEnvironment.Options) !Setup {
        const env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, options);
        errdefer env.deinit();
        inline for (.{ CallByName, CallEcho, CallGreet, CancelHold, AbandonHold, GreetWorkflow, HoldWorkflow }) |W| try env.registerWorkflow(W);
        try env.registerNexusServiceWith(Handler, handlers);
        var s: Setup = .{ .env = env, .endpoint = undefined, .arena = .init(testing.allocator) };
        errdefer s.arena.deinit();
        s.endpoint = try uniqueName(s.arena.allocator(), "zig-endpoint");
        try env.createNexusEndpoint(s.endpoint, .{});
        return s;
    }

    fn deinit(s: *Setup) void {
        s.env.deinit();
        s.arena.deinit();
    }

    /// Runs `Caller` with `args` to its result, which `getResult` replays.
    fn run(s: *Setup, comptime Caller: type, args: Caller.Args) !Caller.Result {
        var r = try s.env.startWorkflow(Caller, .{}, args);
        defer r.deinit();
        return s.env.getResult(Caller, &r, s.arena.allocator(), null);
    }
};

fn uniqueName(a: std.mem.Allocator, prefix: []const u8) ![]const u8 {
    var suffix: [4]u8 = undefined;
    testing.io.random(&suffix);
    return std.fmt.allocPrint(a, "{s}-{x}", .{ prefix, suffix });
}

/// Follows the history of `workflow_id` until it records an event of type `event_type`.
fn waitForEvent(c: *client_mod.Client, workflow_id: []const u8, run_id: []const u8, event_type: EventType) !proto.temporal.api.history.v1.HistoryEvent {
    var it = try c.historyIterator(workflow_id, run_id, .{ .wait_new_event = true });
    defer it.deinit();
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    while (try it.next(scratch.allocator())) |event| {
        if (event.event_type == event_type) return .{ .event_id = event.event_id, .event_type = event.event_type };
    }
    std.debug.print("workflow {s} closed without a {t} event\n", .{ workflow_id, event_type });
    return error.TestEventNotRecorded;
}

fn waitFor(event: *Io.Event) !void {
    event.waitTimeout(testing.io, .{ .duration = .{ .raw = .fromSeconds(60), .clock = .awake } }) catch |err| {
        std.debug.print("waited 60s for a handler event: {t}\n", .{err});
        return err;
    };
}

fn expectContains(haystack: []const u8, needle: []const u8) !void {
    if (std.mem.indexOf(u8, haystack, needle) == null) {
        std.debug.print("\"{s}\"\n  does not contain \"{s}\"\n", .{ haystack, needle });
        return error.TestExpectedSubstring;
    }
}

// ---------------------------------------------------------------- tests

test "nexus: a synchronous operation returns its output to the calling workflow, with the request's info" {
    var handlers: Handlers = .{ .io = testing.io };
    var s = try Setup.start(&handlers, .{});
    defer s.deinit();
    var r = try s.env.startWorkflow(CallEcho, .{}, .{ s.endpoint, "hi" });
    defer r.deinit();
    const result = try s.env.getResult(CallEcho, &r, s.arena.allocator(), null);
    const expected = try std.fmt.allocPrint(s.arena.allocator(), "hi op=zig-test-service/echo ns=default endpoint={s} queue={s} trace=abc request_id=true link={s} deadline=true", .{ s.endpoint, s.env.task_queue, r.id });
    try testing.expectEqualStrings(expected, result);
}

test "nexus: a workflow-run operation completes with its workflow's result, and the caller's history links to the workflow" {
    var handlers: Handlers = .{ .io = testing.io };
    var s = try Setup.start(&handlers, .{});
    defer s.deinit();
    const a = s.arena.allocator();
    var r = try s.env.startWorkflow(CallGreet, .{}, .{ s.endpoint, "Zig" });
    defer r.deinit();
    const result = try s.env.getResult(CallGreet, &r, a, null);
    try expectContains(result, "Hello, Zig (token of ");

    var greeted = try s.env.client.getWorkflow([]const u8, "greet-Zig", "");
    defer greeted.deinit();
    try testing.expectEqualStrings("Hello, Zig", try greeted.get(a));
    const handler_run = (try s.env.client.describeWorkflow(a, "greet-Zig", "")).run_id;

    const history = try s.env.client.fetchHistory(a, r.id, r.run_id);
    var started = false;
    for (history.events.items) |event| {
        if (event.event_type != .EVENT_TYPE_NEXUS_OPERATION_STARTED) continue;
        started = true;
        try testing.expect(event.attributes.?.nexus_operation_started_event_attributes.operation_token.len > 0);
        try testing.expectEqual(1, event.links.items.len);
        const link = event.links.items[0].variant.?.workflow_event;
        try testing.expectEqualStrings("default", link.namespace);
        try testing.expectEqualStrings("greet-Zig", link.workflow_id);
        try testing.expectEqualStrings(handler_run, link.run_id);
        // The server names the start event by its ID; a link built from the request ID names
        // it by that.
        const event_type = switch (link.reference orelse return error.TestLinkWithoutReference) {
            .event_ref => |e| e.event_type,
            .request_id_ref => |e| e.event_type,
        };
        try testing.expectEqual(EventType.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, event_type);
    }
    try testing.expect(started);
}

test "nexus: a worker with only a Nexus service, in another namespace than the caller, serves its operations" {
    var handlers: Handlers = .{ .io = testing.io };
    const handler_namespace = "zig-nexus-handlers";
    const env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{ .dev_server_extra_args = &.{ "--namespace", handler_namespace } });
    defer env.deinit();
    try env.registerWorkflow(CallEcho);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var options = env.server.dev.clientOptions();
    options.namespace = handler_namespace;
    var handler_client = try client_mod.Client.connect(testing.allocator, testing.io, options);
    defer handler_client.close();
    const queue = try uniqueName(a, "zig-handlers");
    var w = try worker_mod.Worker.init(testing.allocator, testing.io, &handler_client, queue, .{});
    defer w.deinit();
    try w.registerNexusServiceWith(Handler, &handlers);
    var running = try testing.io.concurrent(worker_mod.Worker.run, .{&w});
    defer {
        w.stop();
        running.await(testing.io) catch {};
    }
    try w.waitUntilRunning();

    const endpoint = try uniqueName(a, "zig-endpoint");
    try env.createNexusEndpoint(endpoint, .{ .namespace = handler_namespace, .task_queue = queue });
    var r = try env.startWorkflow(CallEcho, .{}, .{ endpoint, "across" });
    defer r.deinit();
    const result = try env.getResult(CallEcho, &r, a, null);
    try expectContains(result, try std.fmt.allocPrint(a, "across op=zig-test-service/echo ns={s} endpoint={s} queue={s} trace=abc", .{ handler_namespace, endpoint, queue }));
    // The caller's link names its own namespace's workflow.
    try expectContains(result, try std.fmt.allocPrint(a, "link={s}", .{r.id}));
}

test "nexus: ctx.fail fails the operation once, with the handler's message and type in the cause chain" {
    var handlers: Handlers = .{ .io = testing.io };
    var s = try Setup.start(&handlers, .{});
    defer s.deinit();
    const result = try s.run(CallByName, .{ s.endpoint, "fail", "socks" });
    try testing.expect(std.mem.startsWith(u8, result, "failed: "));
    try expectContains(result, "out of stock: socks [application OutOfStock]");
    try testing.expectEqual(1, handlers.fail_calls.load(.monotonic));
}

test "nexus: a non-retryable handler error fails the operation once; a retryable one is retried until the handler succeeds" {
    var handlers: Handlers = .{ .io = testing.io };
    var s = try Setup.start(&handlers, .{});
    defer s.deinit();
    const refused = try s.run(CallByName, .{ s.endpoint, "refuse", "x" });
    try testing.expect(std.mem.startsWith(u8, refused, "failed: "));
    try expectContains(refused, "refused for good [handler INTERNAL]");
    try testing.expectEqual(1, handlers.refuse_calls.load(.monotonic));

    try testing.expectEqualStrings("x on attempt 2", try s.run(CallByName, .{ s.endpoint, "flaky", "x" }));
    try testing.expectEqual(2, handlers.flaky_calls.load(.monotonic));
}

test "nexus: an operation the service doesn't have fails as not found, naming the service and operation" {
    var handlers: Handlers = .{ .io = testing.io };
    var s = try Setup.start(&handlers, .{});
    defer s.deinit();
    const result = try s.run(CallByName, .{ s.endpoint, "no-such-operation", "x" });
    try testing.expect(std.mem.startsWith(u8, result, "failed: "));
    try expectContains(result, "Nexus service \"zig-test-service\" has no operation \"no-such-operation\"");
    try expectContains(result, "[handler NOT_FOUND]");
}

test "nexus: a canceled workflow-run operation cancels its workflow, and the caller resolves as each cancellation type says" {
    var handlers: Handlers = .{ .io = testing.io };
    var s = try Setup.start(&handlers, .{});
    defer s.deinit();
    const a = s.arena.allocator();
    const c = &s.env.client;

    // wait_cancellation_completed: the caller resolves only once the workflow has ended.
    {
        const hold_id = try uniqueName(a, "hold-wait");
        var r = try s.env.startWorkflow(CancelHold, .{}, .{ s.endpoint, hold_id, "wait_cancellation_completed" });
        defer r.deinit();
        _ = try waitForEvent(c, r.id, r.run_id, .EVENT_TYPE_NEXUS_OPERATION_STARTED);
        _ = try waitForEvent(c, hold_id, "", .EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED);
        // The handler workflow got the cancellation but holds on: the caller still waits.
        try testing.expectEqual(client_mod.WorkflowExecutionStatus.running, (try c.describeWorkflow(a, r.id, r.run_id)).status);
        try c.signalWorkflow(Release, hold_id, "", .{});
        const result = try s.env.getResult(CancelHold, &r, a, null);
        try testing.expect(std.mem.startsWith(u8, result, "failed: "));
        try expectContains(result, "[canceled_failure_info]");
        var held = try c.getWorkflow([]const u8, hold_id, "");
        defer held.deinit();
        try testing.expectError(error.WorkflowCanceled, held.get(a));
    }

    // wait_cancellation_requested: the caller resolves once the handler has requested the
    // workflow's cancellation, while the workflow still holds on.
    {
        const hold_id = try uniqueName(a, "hold-requested");
        var r = try s.env.startWorkflow(CancelHold, .{}, .{ s.endpoint, hold_id, "wait_cancellation_requested" });
        defer r.deinit();
        const result = try s.env.getResult(CancelHold, &r, a, null);
        try testing.expect(std.mem.startsWith(u8, result, "failed: "));
        try expectContains(result, "[canceled_failure_info]");
        var requested = false;
        for ((try c.fetchHistory(a, hold_id, "")).events.items) |event| {
            if (event.event_type == .EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED) requested = true;
        }
        try testing.expect(requested);
        try testing.expectEqual(client_mod.WorkflowExecutionStatus.running, (try c.describeWorkflow(a, hold_id, "")).status);
        try c.signalWorkflow(Release, hold_id, "", .{});
        var held = try c.getWorkflow([]const u8, hold_id, "");
        defer held.deinit();
        try testing.expectError(error.WorkflowCanceled, held.get(a));
    }

    // try_cancel: the caller resolves at once, without waiting for the handler. Its closing
    // may keep the cancel from reaching the handler, so the workflow ends either way.
    {
        const hold_id = try uniqueName(a, "hold-try");
        var r = try s.env.startWorkflow(CancelHold, .{}, .{ s.endpoint, hold_id, "try_cancel" });
        defer r.deinit();
        const result = try s.env.getResult(CancelHold, &r, a, null);
        try testing.expect(std.mem.startsWith(u8, result, "failed: "));
        try expectContains(result, "[canceled_failure_info]");
        try testing.expectEqual(client_mod.WorkflowExecutionStatus.running, (try c.describeWorkflow(a, hold_id, "")).status);
        try c.signalWorkflow(Release, hold_id, "", .{});
        var held = try c.getWorkflow([]const u8, hold_id, "");
        defer held.deinit();
        if (held.get(a)) |released| {
            try testing.expectEqualStrings(try std.fmt.allocPrint(a, "released {s}", .{hold_id}), released);
        } else |err| try testing.expectEqual(error.WorkflowCanceled, err);
    }
}

test "nexus: the client's codecs encode an operation's input and result on the wire" {
    var handlers: Handlers = .{ .io = testing.io };
    const zlib: converter.ZlibCodec = .{ .always_encode = true };
    const codecs = [_]converter.PayloadCodec{zlib.codec()};
    var s = try Setup.start(&handlers, .{ .codecs = &codecs });
    defer s.deinit();
    const a = s.arena.allocator();
    var r = try s.env.startWorkflow(CallEcho, .{}, .{ s.endpoint, "squeezed" });
    defer r.deinit();
    try expectContains(try s.env.getResult(CallEcho, &r, a, null), "squeezed op=zig-test-service/echo");

    // The environment's client decodes what it fetches; one without codecs sees the wire.
    var raw = try client_mod.Client.connect(testing.allocator, testing.io, s.env.server.dev.clientOptions());
    defer raw.close();
    const history = try raw.fetchHistory(a, r.id, r.run_id);
    var checked: u32 = 0;
    for (history.events.items) |event| switch (event.attributes.?) {
        .nexus_operation_scheduled_event_attributes => |attrs| {
            try testing.expectEqualStrings(converter.ZlibCodec.encoding, converter.encodingOf(attrs.input.?).?);
            checked += 1;
        },
        .nexus_operation_completed_event_attributes => |attrs| {
            try testing.expectEqualStrings(converter.ZlibCodec.encoding, converter.encodingOf(attrs.result.?).?);
            checked += 1;
        },
        else => {},
    };
    try testing.expectEqual(2, checked);
}

test "nexus: a workflow start the server rejects as an invalid argument fails the operation as a bad request, without retries" {
    var handlers: Handlers = .{ .io = testing.io };
    var s = try Setup.start(&handlers, .{});
    defer s.deinit();
    const result = try s.run(CallByName, .{ s.endpoint, "long-id", "x" });
    try testing.expect(std.mem.startsWith(u8, result, "failed: "));
    // The server's own words for what it rejected reach the caller.
    try expectContains(result, "gRPC status invalid_argument: ");
    try expectContains(result, "[handler BAD_REQUEST]");
    try testing.expectEqual(1, handlers.long_id_calls.load(.monotonic));
}

test "nexus: stopping the worker cancels a running handler with reason worker_shutdown, and run returns" {
    var handlers: Handlers = .{ .io = testing.io };
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(CallByName);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const queue = try uniqueName(a, "zig-handlers");
    var w = try worker_mod.Worker.init(testing.allocator, testing.io, &env.client, queue, .{});
    defer w.deinit();
    try w.registerNexusServiceWith(Handler, &handlers);
    var running = try testing.io.concurrent(worker_mod.Worker.run, .{&w});
    var stopped = false;
    defer if (!stopped) {
        w.stop();
        running.await(testing.io) catch {};
    };
    try w.waitUntilRunning();
    const endpoint = try uniqueName(a, "zig-endpoint");
    try env.createNexusEndpoint(endpoint, .{ .task_queue = queue });

    var r = try env.startWorkflow(CallByName, .{}, .{ endpoint, "block", "x" });
    defer r.deinit();
    try waitFor(&handlers.entered);
    w.stop();
    stopped = true;
    try running.await(testing.io);
    try testing.expect(handlers.canceled.isSet());
    try testing.expectEqual(nexus.CancelReason.worker_shutdown, handlers.cancelReason().?);
}

test "nexus: a handler still running at its request's deadline is canceled with reason timed_out" {
    var handlers: Handlers = .{ .io = testing.io };
    var s = try Setup.start(&handlers, .{});
    defer s.deinit();
    // The server gives each attempt a deadline (its request timeout) the core cancels at.
    var r = try s.env.startWorkflow(CallByName, .{}, .{ s.endpoint, "block", "x" });
    defer r.deinit();
    try waitFor(&handlers.canceled);
    try testing.expectEqual(nexus.CancelReason.timed_out, handlers.cancelReason().?);
}

// sdk-core 1.2.0 fails the caller's workflow task as nondeterministic when an abandoned
// operation ends while the caller still runs (`Cancelled::on_completed` in
// nexus_operation_state_machine.rs tests `== Abandon` where its message says the opposite), so
// this tests only the case that works: the caller closes before the operation ends.
test "nexus: a workflow that abandons a started operation resolves it as canceled and closes, while the operation runs on" {
    var handlers: Handlers = .{ .io = testing.io };
    var s = try Setup.start(&handlers, .{});
    defer s.deinit();
    const a = s.arena.allocator();
    const c = &s.env.client;
    const hold_id = try uniqueName(a, "hold-abandoned");
    var r = try s.env.startWorkflow(AbandonHold, .{}, .{ s.endpoint, hold_id });
    defer r.deinit();
    const result = try s.env.getResult(AbandonHold, &r, a, null);
    try expectContains(result, "abandoned: ");
    try expectContains(result, "[canceled_failure_info]");

    // No cancel was sent: the workflow runs on, and completes when released.
    try testing.expectEqual(client_mod.WorkflowExecutionStatus.running, (try c.describeWorkflow(a, hold_id, "")).status);
    for ((try c.fetchHistory(a, hold_id, "")).events.items) |event|
        try testing.expect(event.event_type != .EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED);
    try c.signalWorkflow(Release, hold_id, "", .{});
    var held = try c.getWorkflow([]const u8, hold_id, "");
    defer held.deinit();
    try testing.expectEqualStrings(try std.fmt.allocPrint(a, "released {s}", .{hold_id}), try held.get(a));
    try testing.expectEqual(client_mod.WorkflowExecutionStatus.completed, (try c.describeWorkflow(a, r.id, r.run_id)).status);
}
