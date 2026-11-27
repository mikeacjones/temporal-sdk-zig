//! Nexus end to end against a dev server: caller workflows call operations through an
//! endpoint, and a worker's Nexus task loop runs the handlers.

const std = @import("std");
const EphemeralServer = @import("bridge/EphemeralServer.zig");
const client_mod = @import("client.zig");
const converter = @import("converter.zig");
const failure_mod = @import("failure.zig");
const interceptor = @import("interceptor.zig");
const log = @import("log.zig");
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
const Late = nexus.OperationDef("late", []const u8, []const u8);
const FailWithDetails = nexus.OperationDef("fail-with-details", []const u8, []const u8);
const Service = nexus.ServiceDef("zig-test-service", .{ Echo, Greet, Hold, Fail, Flaky, Refuse, LongId, Block, Late, FailWithDetails });

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
    /// `late`'s attempts, and its first attempts that saw the worker's cancel and still
    /// returned, indexed by `lateMode`.
    late_calls: [2]std.atomic.Value(u32) = .{ .init(0), .init(0) },
    late_returned: [2]Io.Event = .{ .unset, .unset },

    /// Returns the input with what the handler knows about its request.
    fn echo(_: *Handlers, ctx: *nexus.Context, input: []const u8) ![]const u8 {
        ctx.logger().info("echo handled", .{});
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

    /// Its first attempt waits for the worker's cancel (at the request's deadline) and then
    /// returns anyway: a result for "result", an operation failure for "error". Later
    /// attempts return at once.
    fn late(h: *Handlers, ctx: *nexus.Context, mode: []const u8) ![]const u8 {
        const m = lateMode(mode);
        const attempt = h.late_calls[m].fetchAdd(1, .monotonic) + 1;
        if (attempt > 1) return std.fmt.allocPrint(ctx.allocator(), "on time {s} on attempt {d}", .{ mode, attempt });
        ctx.io().sleep(.fromSeconds(3600), .awake) catch {};
        defer if (ctx.cancelReason() != null) h.late_returned[m].set(h.io);
        if (m == 1) return ctx.fail(.{ .message = "late failure", .type = "Late", .non_retryable = true });
        return "late result";
    }

    fn lateMode(mode: []const u8) usize {
        return @intFromBool(std.mem.eql(u8, mode, "error"));
    }

    /// Fails the operation with `input` as the failure's detail.
    fn failWithDetails(_: *Handlers, ctx: *nexus.Context, input: []const u8) ![]const u8 {
        const detail = try converter.toPayload(ctx.allocator(), input);
        return ctx.fail(.{ .message = "failed with details", .type = "Detailed", .details = &.{detail} });
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
    nexus.OperationHandler(Late, Handlers.late),
    nexus.OperationHandler(FailWithDetails, Handlers.failWithDetails),
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

/// Calls `operation` of `service` by name, as `callByName` does.
fn callService(ctx: *workflow.Context, endpoint: []const u8, service: []const u8, operation: []const u8, input: []const u8) ![]const u8 {
    var options = call_options;
    options.endpoint = endpoint;
    const f = ctx.executeNexusOperationByName([]const u8, service, operation, options, input);
    return f.get() catch |err| switch (err) {
        error.NexusOperationFailed => std.fmt.allocPrint(ctx.allocator(), "failed: {s}", .{try describe(ctx, f.failure())}),
        else => err,
    };
}
const CallService = workflow.Def("NexusTestCallService", callService);

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
        inline for (.{ CallByName, CallService, CallEcho, CallGreet, CancelHold, AbandonHold, GreetWorkflow, HoldWorkflow }) |W| try env.registerWorkflow(W);
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

test "nexus: a service the worker hasn't registered fails as not found, naming the service and operation" {
    var handlers: Handlers = .{ .io = testing.io };
    var s = try Setup.start(&handlers, .{});
    defer s.deinit();
    const result = try s.run(CallService, .{ s.endpoint, "no-such-service", "echo", "x" });
    try testing.expect(std.mem.startsWith(u8, result, "failed: "));
    try expectContains(result, "Nexus service \"no-such-service\" is not registered on the worker");
    try expectContains(result, "its operation \"echo\"");
    try expectContains(result, "[handler NOT_FOUND]");
}

test "nexus: a handler that returns a result or a failure after the worker canceled it neither crashes the worker nor completes the operation" {
    var handlers: Handlers = .{ .io = testing.io };
    var s = try Setup.start(&handlers, .{});
    defer s.deinit();
    const a = s.arena.allocator();
    // The first attempt of each outlives its deadline; the server retries it.
    var runs: [2]client_mod.WorkflowRun([]const u8) = undefined;
    const modes = [2][]const u8{ "result", "error" };
    for (&runs, modes, 0..) |*r, mode, i| {
        errdefer for (runs[0..i]) |*done| done.deinit();
        r.* = try s.env.startWorkflow(CallByName, .{}, .{ s.endpoint, "late", mode });
    }
    defer for (&runs) |*r| r.deinit();
    for (&runs, modes) |*r, mode| {
        const result = try s.env.getResult(CallByName, r, a, null);
        try testing.expectEqualStrings(try std.fmt.allocPrint(a, "on time {s} on attempt 2", .{mode}), result);
        try waitFor(&handlers.late_returned[Handlers.lateMode(mode)]);
        try testing.expectEqual(2, handlers.late_calls[Handlers.lateMode(mode)].load(.monotonic));
        // The operation completed once, with the second attempt's result.
        var completed: u32 = 0;
        for ((try s.env.client.fetchHistory(a, r.id, r.run_id)).events.items) |event| switch (event.event_type) {
            .EVENT_TYPE_NEXUS_OPERATION_COMPLETED => completed += 1,
            .EVENT_TYPE_NEXUS_OPERATION_FAILED, .EVENT_TYPE_NEXUS_OPERATION_CANCELED, .EVENT_TYPE_NEXUS_OPERATION_TIMED_OUT => return error.TestUnexpectedResult,
            else => {},
        };
        try testing.expectEqual(1, completed);
    }
    // The worker still serves requests after sending the late responses.
    try expectContains(try s.run(CallEcho, .{ s.endpoint, "still here" }), "still here op=zig-test-service/echo");
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

/// The first detail of the first application failure in `f`'s cause chain.
fn applicationDetail(f: ?*const failure_mod.Failure) ?converter.Payload {
    var it = f;
    while (it) |cur| : (it = cur.cause) if (cur.failure_info) |info| switch (info) {
        .application_failure_info => |app| {
            const details = app.details orelse return null;
            return if (details.payloads.items.len > 0) details.payloads.items[0] else null;
        },
        else => {},
    };
    return null;
}

test "nexus: the client's codecs encode a handler failure's details on the wire" {
    var handlers: Handlers = .{ .io = testing.io };
    const zlib: converter.ZlibCodec = .{ .always_encode = true };
    const codecs = [_]converter.PayloadCodec{zlib.codec()};
    var s = try Setup.start(&handlers, .{ .codecs = &codecs });
    defer s.deinit();
    const a = s.arena.allocator();
    var r = try s.env.startWorkflow(CallByName, .{}, .{ s.endpoint, "fail-with-details", "the detail" });
    defer r.deinit();
    const result = try s.env.getResult(CallByName, &r, a, null);
    try expectContains(result, "failed with details [application Detailed]");

    // Only the handling worker wrote the failure the caller's history holds: a client
    // without codecs sees its detail as the codec encoded it, and the environment's client
    // decodes it back.
    var raw = try client_mod.Client.connect(testing.allocator, testing.io, s.env.server.dev.clientOptions());
    defer raw.close();
    for ([_]*client_mod.Client{ &raw, &s.env.client }, [_]bool{ true, false }) |c, on_wire| {
        const history = try c.fetchHistory(a, r.id, r.run_id);
        const failed = for (history.events.items) |event| switch (event.attributes.?) {
            .nexus_operation_failed_event_attributes => |attrs| break attrs,
            else => {},
        } else return error.TestExpectedFailedEvent;
        const detail = applicationDetail(if (failed.failure) |*f| f else null) orelse return error.TestExpectedDetails;
        if (on_wire) {
            try testing.expectEqualStrings(converter.ZlibCodec.encoding, converter.encodingOf(detail).?);
        } else {
            try testing.expectEqualStrings("the detail", try converter.fromPayload([]const u8, a, detail));
        }
    }
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

test "nexus: a worker with autoscaling Nexus pollers handles an operation" {
    var handlers: Handlers = .{ .io = testing.io };
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(CallEcho);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const queue = try uniqueName(a, "zig-autoscaling-handlers");
    var w = try worker_mod.Worker.init(testing.allocator, testing.io, &env.client, queue, .{
        .nexus_task_poller_behavior = .{ .autoscaling = .{} },
    });
    defer w.deinit();
    try w.registerNexusServiceWith(Handler, &handlers);
    var running = try testing.io.concurrent(worker_mod.Worker.run, .{&w});
    defer {
        w.stop();
        running.await(testing.io) catch {};
    }
    try w.waitUntilRunning();
    const endpoint = try uniqueName(a, "zig-autoscaling-endpoint");
    try env.createNexusEndpoint(endpoint, .{ .task_queue = queue });

    var r = try env.startWorkflow(CallEcho, .{}, .{ endpoint, "scaled" });
    defer r.deinit();
    const result = try env.getResult(CallEcho, &r, a, null);
    try expectContains(result, try std.fmt.allocPrint(a, "scaled op=zig-test-service/echo ns=default endpoint={s} queue={s}", .{ endpoint, queue }));
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

// ---------------------------------------------------------------- interceptors

/// What the Nexus interceptors saw, in order, from the handlers' tasks.
const Seen = struct {
    mutex: Io.Mutex = .init,
    arena: std.heap.ArenaAllocator = .init(testing.allocator),
    events: std.ArrayList([]const u8) = .empty,

    fn add(s: *Seen, comptime fmt: []const u8, args: anytype) void {
        s.mutex.lockUncancelable(testing.io);
        defer s.mutex.unlock(testing.io);
        const event = std.fmt.allocPrint(s.arena.allocator(), fmt, args) catch return;
        s.events.append(s.arena.allocator(), event) catch {};
    }

    fn expect(s: *Seen, event: []const u8) !void {
        s.mutex.lockUncancelable(testing.io);
        defer s.mutex.unlock(testing.io);
        for (s.events.items) |e| if (std.mem.eql(u8, e, event)) return;
        std.debug.print("the Nexus interceptors never saw \"{s}\"; they saw {any}\n", .{ event, s.events.items });
        return error.TestExpectedEvent;
    }
};

/// Keeps the message and the `NexusTagged` field of each log record that has one.
const TaggedRecords = struct {
    mutex: Io.Mutex = .init,
    arena: std.heap.ArenaAllocator = .init(testing.allocator),
    lines: std.ArrayList([]const u8) = .empty,

    fn sink(r: *TaggedRecords) log.Sink {
        return .{ .ptr = r, .vtable = &.{ .log = record } };
    }

    fn record(ptr: ?*anyopaque, _: log.Level, message: []const u8, fields: []const log.Field) void {
        const r: *TaggedRecords = @ptrCast(@alignCast(ptr.?));
        r.mutex.lockUncancelable(testing.io);
        defer r.mutex.unlock(testing.io);
        for (fields) |f| if (std.mem.eql(u8, f.key, "NexusTagged")) {
            const line = std.fmt.allocPrint(r.arena.allocator(), "{s} {s}", .{ message, f.value }) catch return;
            r.lines.append(r.arena.allocator(), line) catch {};
        };
    }

    fn has(r: *TaggedRecords, line: []const u8) bool {
        r.mutex.lockUncancelable(testing.io);
        defer r.mutex.unlock(testing.io);
        for (r.lines.items) |l| if (std.mem.eql(u8, l, line)) return true;
        return false;
    }
};

fn headerValue(headers: []const nexus.Header, name: []const u8) ?[]const u8 {
    for (headers) |h| if (std.ascii.eqlIgnoreCase(h.name, name)) return h.value;
    return null;
}

/// On the calling side, adds the header `x-tag: <tag>` to every Nexus call. On the handling
/// side, records each start and cancel with the `x-tag` header it carries, appends "+<tag>"
/// to the `trace-id` header the handler sees, refuses every start of `fail`, and adds the
/// field `NexusTagged=<tag>` to the handler's logger.
const NexusTagging = struct {
    tag: []const u8,
    seen: *Seen,

    pub fn interceptWorkflow(t: *NexusTagging, ctx: *workflow.Context) error{OutOfMemory}!interceptor.Workflow {
        const run = try ctx.allocator().create(Caller);
        run.* = .{ .t = t, .ctx = ctx };
        return .{ .outbound = interceptor.WorkflowOutbound.from(Caller, run) };
    }

    pub fn interceptNexusOperation(t: *NexusTagging, ctx: *nexus.Context) error{OutOfMemory}!interceptor.NexusOperation {
        const request = try ctx.allocator().create(Request);
        request.* = .{ .t = t, .ctx = ctx };
        return .{ .inbound = interceptor.NexusOperationInbound.from(Request, request), .outbound = interceptor.NexusOperationOutbound.from(Request, request) };
    }

    const Caller = struct {
        t: *NexusTagging,
        ctx: *workflow.Context,

        pub fn executeNexusOperation(c: *Caller, input: interceptor.NexusOperationInput, next: interceptor.WorkflowOutbound.Next) workflow.NexusOperationFuture(converter.Payload) {
            var changed = input;
            changed.options.headers = std.mem.concat(c.ctx.allocator(), nexus.Header, &.{ input.options.headers, &.{.{ .name = "x-tag", .value = c.t.tag }} }) catch
                return interceptor.rejectNexusOperation(c.ctx, input, .{ .message = "out of memory adding the tag" });
            return next.executeNexusOperation(changed);
        }
    };

    const Request = struct {
        t: *NexusTagging,
        ctx: *nexus.Context,

        pub fn startOperation(r: *Request, input: interceptor.StartNexusOperationInput, next: interceptor.NexusOperationInbound.Next) nexus.StartResult {
            const tag = headerValue(input.headers, "x-tag") orelse "none";
            r.t.seen.add("{s} saw start {s} x-tag={s}", .{ r.t.tag, input.operation, tag });
            if (std.mem.eql(u8, input.operation, "fail"))
                return .{ .handler_error = .{ .type = .unauthorized, .message = "refused by the tagging interceptor", .retry = .non_retryable } };
            const a = r.ctx.allocator();
            const headers = a.dupe(nexus.Header, input.headers) catch return .{ .handler_error = .{ .type = .internal, .message = "out of memory" } };
            for (headers) |*h| if (std.ascii.eqlIgnoreCase(h.name, "trace-id")) {
                h.value = std.fmt.allocPrint(a, "{s}+{s}", .{ h.value, r.t.tag }) catch return .{ .handler_error = .{ .type = .internal, .message = "out of memory" } };
            };
            var changed = input;
            changed.headers = headers;
            return next.startOperation(changed);
        }

        pub fn cancelOperation(r: *Request, input: interceptor.CancelNexusOperationInput, next: interceptor.NexusOperationInbound.Next) nexus.CancelResult {
            r.t.seen.add("{s} saw cancel {s} token={}", .{ r.t.tag, input.operation, input.token.len > 0 });
            return next.cancelOperation(input);
        }

        pub fn logger(r: *Request, input: interceptor.LoggerInput, next: interceptor.NexusOperationOutbound.Next) log.Logger {
            const base = next.logger(input);
            return base.with(r.ctx.allocator(), .{ .NexusTagged = r.t.tag }) catch base;
        }
    };
};

test "nexus: a workflow interceptor's header reaches the handling worker's interceptor, for a synchronous and a workflow-run operation" {
    var handlers: Handlers = .{ .io = testing.io };
    var seen: Seen = .{};
    defer seen.arena.deinit();
    var records: TaggedRecords = .{};
    defer records.arena.deinit();
    var tagging: NexusTagging = .{ .tag = "env", .seen = &seen };
    const interceptors = [_]interceptor.Worker{interceptor.Worker.from(NexusTagging, &tagging)};
    var s = try Setup.start(&handlers, .{ .worker_interceptors = &interceptors, .logger = records.sink() });
    defer s.deinit();

    // The handler sees the header as the interceptor changed it, and logs through its logger.
    try expectContains(try s.run(CallEcho, .{ s.endpoint, "hi" }), "hi op=zig-test-service/echo ns=default");
    try expectContains(try s.run(CallEcho, .{ s.endpoint, "hi" }), "trace=abc+env ");
    try seen.expect("env saw start echo x-tag=env");
    try testing.expect(records.has("echo handled env"));

    try expectContains(try s.run(CallGreet, .{ s.endpoint, "Tagged" }), "Hello, Tagged (token of ");
    try seen.expect("env saw start greet x-tag=env");
}

test "nexus: a start an interceptor refuses fails the caller's operation with its handler error, and the handler never runs" {
    var handlers: Handlers = .{ .io = testing.io };
    var seen: Seen = .{};
    defer seen.arena.deinit();
    var tagging: NexusTagging = .{ .tag = "env", .seen = &seen };
    const interceptors = [_]interceptor.Worker{interceptor.Worker.from(NexusTagging, &tagging)};
    var s = try Setup.start(&handlers, .{ .worker_interceptors = &interceptors });
    defer s.deinit();
    const result = try s.run(CallByName, .{ s.endpoint, "fail", "socks" });
    try testing.expect(std.mem.startsWith(u8, result, "failed: "));
    try expectContains(result, "refused by the tagging interceptor [handler UNAUTHORIZED]");
    try testing.expectEqual(0, handlers.fail_calls.load(.monotonic));
    try seen.expect("env saw start fail x-tag=env");
}

test "nexus: the handling worker's interceptor sees the cancel of a workflow-run operation" {
    var handlers: Handlers = .{ .io = testing.io };
    var seen: Seen = .{};
    defer seen.arena.deinit();
    var tagging: NexusTagging = .{ .tag = "env", .seen = &seen };
    const interceptors = [_]interceptor.Worker{interceptor.Worker.from(NexusTagging, &tagging)};
    var s = try Setup.start(&handlers, .{ .worker_interceptors = &interceptors });
    defer s.deinit();
    const a = s.arena.allocator();
    const hold_id = try uniqueName(a, "hold-intercepted");
    // wait_cancellation_requested resolves only after the handler's cancel returned.
    const result = try s.run(CancelHold, .{ s.endpoint, hold_id, "wait_cancellation_requested" });
    try expectContains(result, "[canceled_failure_info]");
    try seen.expect("env saw start hold x-tag=env");
    try seen.expect("env saw cancel hold token=true");
    try s.env.client.signalWorkflow(Release, hold_id, "", .{});
    var held = try s.env.client.getWorkflow([]const u8, hold_id, "");
    defer held.deinit();
    try testing.expectError(error.WorkflowCanceled, held.get(a));
}

test "nexus: the client's worker interceptors run outside the worker's own on a Nexus request" {
    var handlers: Handlers = .{ .io = testing.io };
    var seen: Seen = .{};
    defer seen.arena.deinit();
    var records: TaggedRecords = .{};
    defer records.arena.deinit();
    var client_tagging: NexusTagging = .{ .tag = "client", .seen = &seen };
    const client_interceptors = [_]interceptor.Worker{interceptor.Worker.from(NexusTagging, &client_tagging)};
    var worker_tagging: NexusTagging = .{ .tag = "worker", .seen = &seen };
    const worker_interceptors = [_]interceptor.Worker{interceptor.Worker.from(NexusTagging, &worker_tagging)};
    const env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{ .worker_interceptors = &client_interceptors, .logger = records.sink() });
    defer env.deinit();
    try env.registerWorkflow(CallEcho);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const queue = try uniqueName(a, "zig-intercepted");
    var w = try worker_mod.Worker.init(testing.allocator, testing.io, &env.client, queue, .{ .interceptors = &worker_interceptors });
    defer w.deinit();
    try w.registerNexusServiceWith(Handler, &handlers);
    var running = try testing.io.concurrent(worker_mod.Worker.run, .{&w});
    defer {
        w.stop();
        running.await(testing.io) catch {};
    }
    try w.waitUntilRunning();
    const endpoint = try uniqueName(a, "zig-endpoint");
    try env.createNexusEndpoint(endpoint, .{ .task_queue = queue });

    var r = try env.startWorkflow(CallEcho, .{}, .{ endpoint, "ordered" });
    defer r.deinit();
    // The client's interceptor changed the header first, then the worker's.
    try expectContains(try env.getResult(CallEcho, &r, a, null), "trace=abc+client+worker ");
    try seen.expect("client saw start echo x-tag=client");
    try seen.expect("worker saw start echo x-tag=client");
    // The innermost logger interceptor adds its field first, and each record lists both.
    try testing.expect(records.has("echo handled worker"));
    try testing.expect(records.has("echo handled client"));
}

/// Keeps the reason of each "Dropping a link of the Nexus request that doesn't parse" record.
const DroppedLinks = struct {
    mutex: Io.Mutex = .init,
    arena: std.heap.ArenaAllocator = .init(testing.allocator),
    reasons: std.ArrayList([]const u8) = .empty,

    fn sink(d: *DroppedLinks) log.Sink {
        return .{ .ptr = d, .vtable = &.{ .log = record } };
    }

    fn record(ptr: ?*anyopaque, _: log.Level, message: []const u8, fields: []const log.Field) void {
        if (!std.mem.eql(u8, message, "Dropping a link of the Nexus request that doesn't parse")) return;
        const d: *DroppedLinks = @ptrCast(@alignCast(ptr.?));
        d.mutex.lockUncancelable(testing.io);
        defer d.mutex.unlock(testing.io);
        for (fields) |f| if (std.mem.eql(u8, f.key, "Reason")) {
            const reason = d.arena.allocator().dupe(u8, f.value) catch return;
            d.reasons.append(d.arena.allocator(), reason) catch {};
        };
    }

    fn expectReasonContaining(d: *DroppedLinks, needle: []const u8) !void {
        d.mutex.lockUncancelable(testing.io);
        defer d.mutex.unlock(testing.io);
        for (d.reasons.items) |r| if (std.mem.indexOf(u8, r, needle) != null) return;
        std.debug.print("no dropped link's reason contains \"{s}\"; the reasons were {any}\n", .{ needle, d.reasons.items });
        return error.TestExpectedDroppedLink;
    }
};

test "nexus: a request whose link isn't UTF-8 once percent-decoded starts its workflow, and the link is logged and dropped" {
    // Reserved, not only bound and closed: under the parallel tests another socket took such a
    // port before the dev server bound it, and the server exited (see reserveLoopbackPort).
    const port = try EphemeralServer.reserveLoopbackPort();
    var port_text: [8]u8 = undefined;
    const extra_args = [_][]const u8{ "--http-port", try std.fmt.bufPrint(&port_text, "{d}", .{port}) };
    var dropped: DroppedLinks = .{};
    defer dropped.arena.deinit();
    var handlers: Handlers = .{ .io = testing.io };
    var s = try Setup.start(&handlers, .{ .dev_server_extra_args = &extra_args, .logger = dropped.sink() });
    defer s.deinit();
    const a = s.arena.allocator();
    // A call through the endpoint starts the environment's worker and shows it polling for
    // Nexus tasks, so the request below reaches it.
    try expectContains(try s.run(CallEcho, .{ s.endpoint, "hi" }), "hi op=zig-test-service/echo");

    // The server passes the request's links on to the worker as the caller sent them.
    const url = try std.fmt.allocPrint(a, "http://127.0.0.1:{d}/namespaces/default/task-queues/{s}/nexus-services/zig-test-service/greet", .{ port, s.env.task_queue });
    var http: std.http.Client = .{ .allocator = testing.allocator, .io = testing.io };
    defer http.deinit();
    var body: Io.Writer.Allocating = .init(a);
    const result = try http.fetch(.{
        .location = .{ .url = url },
        .method = .POST,
        .payload = "\"Link\"",
        .headers = .{ .content_type = .{ .override = "application/json" } },
        .extra_headers = &.{
            .{ .name = "Nexus-Request-Id", .value = try uniqueName(a, "request") },
            .{ .name = "Nexus-Link", .value = "<temporal:///namespaces/ns%FF/workflows/w/r/history?referenceType=EventReference&eventType=WorkflowExecutionStarted>; type=\"temporal.api.common.v1.Link.WorkflowEvent\"" },
        },
        .response_writer = &body.writer,
    });
    if (result.status.class() != .success) {
        std.debug.print("the start answered {d}: {s}\n", .{ @backingInt(result.status), body.written() });
        return error.TestStartFailed;
    }

    var greeted = try s.env.client.getWorkflow([]const u8, "greet-Link", "");
    defer greeted.deinit();
    try testing.expectEqualStrings("Hello, Link", try greeted.get(a));
    try dropped.expectReasonContaining("its namespace is not valid UTF-8");
}

// ---------------------------------------------------------------- activity-run operations and signals

const ActivityFixtures = struct {
    var beat_entered: Io.Event = .unset;
    var beat_canceled: Io.Event = .unset;

    fn reset() void {
        beat_entered = .unset;
        beat_canceled = .unset;
    }

    fn compose(ctx: *activity.Context, name: []const u8) ![]const u8 {
        return std.fmt.allocPrint(ctx.allocator(), "Hello, {s}!", .{name});
    }
    const Compose = activity.Def("NexusTestCompose", compose);

    fn refuse(ctx: *activity.Context, name: []const u8) ![]const u8 {
        return ctx.fail(.{ .message = try std.fmt.allocPrint(ctx.allocator(), "no greeting for {s}", .{name}), .type = "NoGreeting", .non_retryable = true });
    }
    const Refuse = activity.Def("NexusTestRefuse", refuse);

    /// Heartbeats until canceled, for at most 60 s.
    fn beat(ctx: *activity.Context, _: []const u8) ![]const u8 {
        beat_entered.set(testing.io);
        for (0..1200) |_| {
            ctx.heartbeat({});
            ctx.io().sleep(.fromMilliseconds(50), .awake) catch |err| {
                beat_canceled.set(testing.io);
                return err;
            };
        }
        return error.TestNeverCanceled;
    }
    const Beat = activity.Def("NexusTestBeat", beat);

    /// Heartbeats "beat <name>", then returns the Nexus context it was started with.
    fn contextual(ctx: *activity.Context, name: []const u8) ![]const u8 {
        const beat_details = try std.fmt.allocPrint(ctx.allocator(), "beat {s}", .{name});
        ctx.heartbeat(beat_details);
        const n = ctx.info().nexus_context orelse return "no context";
        return std.fmt.allocPrint(ctx.allocator(), "{s} {s} {s}/{s}", .{ name, n.endpoint, n.service, n.operation });
    }
    const Contextual = activity.Def("NexusTestContextual", contextual);
};
const activity = @import("activity.zig");

const ComposeOp = nexus.OperationDef("compose", []const u8, []const u8);
const RefuseOp = nexus.OperationDef("refuse", []const u8, []const u8);
const BeatOp = nexus.OperationDef("beat", []const u8, []const u8);
const SignalTwiceOp = nexus.OperationDef("signal-twice", []const u8, []const u8);
const SignalFlakyOp = nexus.OperationDef("signal-flaky", []const u8, []const u8);
const SignalWithStartOp = nexus.OperationDef("signal-with-start", []const u8, []const u8);
const ContextualOp = nexus.OperationDef("contextual", []const u8, []const u8);
const ActivityService = nexus.ServiceDef("zig-activity-service", .{ ComposeOp, RefuseOp, BeatOp, SignalTwiceOp, SignalFlakyOp, SignalWithStartOp, ContextualOp });

const Ping = workflow.SignalDef("ping", fn ([]const u8) void);

const Pings = struct {
    count: u32 = 0,
    released: bool = false,
    fn ping(p: *Pings, _: []const u8) void {
        p.count += 1;
    }
    fn release(p: *Pings) void {
        p.released = true;
    }
    fn isReleased(p: *Pings) bool {
        return p.released;
    }
};

/// Counts `ping` signals until `release`, and returns the count.
fn pingTarget(ctx: *workflow.Context) ![]const u8 {
    const pings = try ctx.allocator().create(Pings);
    pings.* = .{};
    try ctx.setSignalHandler(Ping, pings, Pings.ping);
    try ctx.setSignalHandler(Release, pings, Pings.release);
    try ctx.await(pings, Pings.isReleased);
    return std.fmt.allocPrint(ctx.allocator(), "pings={d}", .{pings.count});
}
const PingTarget = workflow.Def("NexusTestPingTarget", pingTarget);

const ActivityHandlers = struct {
    twice_calls: std.atomic.Value(u32) = .init(0),
    flaky_calls: std.atomic.Value(u32) = .init(0),
    sws_calls: std.atomic.Value(u32) = .init(0),

    fn compose(_: *ActivityHandlers, ctx: *nexus.Context, name: []const u8) !nexus.ActivityStart(ActivityFixtures.Compose) {
        return .{ .options = .{ .id = try std.fmt.allocPrint(ctx.allocator(), "compose-{s}", .{name}), .task_queue = "", .start_to_close_timeout = .fromSeconds(30) }, .args = .{name} };
    }

    fn refuse(_: *ActivityHandlers, _: *nexus.Context, name: []const u8) !nexus.ActivityStart(ActivityFixtures.Refuse) {
        return .{ .options = .{ .task_queue = "", .schedule_to_close_timeout = .fromSeconds(30) }, .args = .{name} };
    }

    fn contextual(_: *ActivityHandlers, ctx: *nexus.Context, name: []const u8) !nexus.ActivityStart(ActivityFixtures.Contextual) {
        return .{ .options = .{ .id = try std.fmt.allocPrint(ctx.allocator(), "contextual-{s}", .{name}), .task_queue = "", .start_to_close_timeout = .fromSeconds(30) }, .args = .{name} };
    }

    fn beat(_: *ActivityHandlers, _: *nexus.Context, name: []const u8) !nexus.ActivityStart(ActivityFixtures.Beat) {
        return .{ .options = .{ .task_queue = "", .start_to_close_timeout = .fromSeconds(120), .heartbeat_timeout = .fromSeconds(10) }, .args = .{name} };
    }

    /// Signals `target` twice; the second is refused.
    fn signalTwice(h: *ActivityHandlers, ctx: *nexus.Context, target: []const u8) ![]const u8 {
        _ = h.twice_calls.fetchAdd(1, .monotonic);
        try ctx.signalWorkflow(Ping, target, "", .{"once"});
        try ctx.signalWorkflow(Ping, target, "", .{"twice"});
        return "signaled twice";
    }

    /// Signals `target`, and fails its first attempt with a retryable handler error.
    fn signalFlaky(h: *ActivityHandlers, ctx: *nexus.Context, target: []const u8) ![]const u8 {
        const attempt = h.flaky_calls.fetchAdd(1, .monotonic) + 1;
        try ctx.signalWorkflow(Ping, target, "", .{"flaky"});
        if (attempt == 1) return ctx.handlerError(.{ .type = .unavailable, .message = "not yet" });
        return std.fmt.allocPrint(ctx.allocator(), "signaled on attempt {d}", .{attempt});
    }

    /// Signal-with-starts `PingTarget` as workflow `id`, fails its first attempt with a
    /// retryable handler error, and returns whether the second started the run.
    fn signalWithStart(h: *ActivityHandlers, ctx: *nexus.Context, id: []const u8) ![]const u8 {
        const r = try ctx.signalWithStartWorkflow(PingTarget, .{ .id = id }, .{}, Ping, .{"started"});
        const attempt = h.sws_calls.fetchAdd(1, .monotonic) + 1;
        if (attempt == 1) return ctx.handlerError(.{ .type = .unavailable, .message = "not yet" });
        return std.fmt.allocPrint(ctx.allocator(), "started={} on attempt {d}", .{ r.started, attempt });
    }
};

const ActivityHandler = nexus.ServiceHandler(ActivityService, .{
    nexus.OperationHandler(ComposeOp, ActivityHandlers.compose),
    nexus.OperationHandler(RefuseOp, ActivityHandlers.refuse),
    nexus.OperationHandler(BeatOp, ActivityHandlers.beat),
    nexus.OperationHandler(SignalTwiceOp, ActivityHandlers.signalTwice),
    nexus.OperationHandler(SignalFlakyOp, ActivityHandlers.signalFlaky),
    nexus.OperationHandler(SignalWithStartOp, ActivityHandlers.signalWithStart),
    nexus.OperationHandler(ContextualOp, ActivityHandlers.contextual),
});

/// Calls `operation` of `ActivityService` with `input`, and returns its output or "failed: "
/// and the failure's cause chain.
fn callActivityService(ctx: *workflow.Context, endpoint: []const u8, operation: []const u8, input: []const u8) ![]const u8 {
    var options = call_options;
    options.endpoint = endpoint;
    const f = ctx.executeNexusOperationByName([]const u8, ActivityService.service_name, operation, options, input);
    return f.get() catch |err| switch (err) {
        error.NexusOperationFailed => std.fmt.allocPrint(ctx.allocator(), "failed: {s}", .{try describe(ctx, f.failure())}),
        else => err,
    };
}
const CallActivityService = workflow.Def("NexusTestCallActivityService", callActivityService);

const CancelNow = workflow.SignalDef("cancel-now", fn () void);

/// Starts `beat`, cancels it once signaled `cancel-now`, and returns how it ended.
fn cancelBeat(ctx: *workflow.Context, endpoint: []const u8) ![]const u8 {
    var options = call_options;
    options.endpoint = endpoint;
    const latch = try ctx.allocator().create(Latch);
    latch.* = .{};
    try ctx.setSignalHandler(CancelNow, latch, Latch.set);
    const f = ctx.executeNexusOperation(ActivityService, BeatOp, options, "beat");
    _ = try f.started();
    try ctx.await(latch, Latch.isOpen);
    f.cancel();
    const result = f.get() catch |err| switch (err) {
        error.NexusOperationFailed => return std.fmt.allocPrint(ctx.allocator(), "failed: {s}", .{try describe(ctx, f.failure())}),
        else => return err,
    };
    return std.fmt.allocPrint(ctx.allocator(), "completed: {s}", .{result});
}
const CancelBeat = workflow.Def("NexusTestCancelBeat", cancelBeat);

/// `Setup` for the activity-run and signal tests: a dev server that lets standalone
/// activities complete Nexus operations and returns links for signals.
fn startActivitySetup(handlers: *ActivityHandlers) !Setup {
    return startActivitySetupWith(handlers, .{});
}

/// `startActivitySetup` with `options`, whose `dev_server_extra_args` it sets.
fn startActivitySetupWith(handlers: *ActivityHandlers, options: WorkflowEnvironment.Options) !Setup {
    ActivityFixtures.reset();
    var with_args = options;
    with_args.dev_server_extra_args = &(testing_mod.DevServer.standalone_activity_args ++ testing_mod.DevServer.activity_callback_args ++ testing_mod.DevServer.signal_link_args);
    const env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, with_args);
    errdefer env.deinit();
    inline for (.{ CallActivityService, CancelBeat, PingTarget }) |W| try env.registerWorkflow(W);
    inline for (.{ ActivityFixtures.Compose, ActivityFixtures.Refuse, ActivityFixtures.Beat, ActivityFixtures.Contextual }) |A| try env.registerActivity(A);
    try env.registerNexusServiceWith(ActivityHandler, handlers);
    var s: Setup = .{ .env = env, .endpoint = undefined, .arena = .init(testing.allocator) };
    errdefer s.arena.deinit();
    s.endpoint = try uniqueName(s.arena.allocator(), "zig-activity-endpoint");
    try env.createNexusEndpoint(s.endpoint, .{});
    return s;
}

/// Releases the `PingTarget` workflow `id` and returns its result.
fn releasePings(s: *Setup, id: []const u8) ![]const u8 {
    try s.env.client.signalWorkflow(Release, id, "", .{});
    var run = try s.env.client.getWorkflow([]const u8, id, "");
    defer run.deinit();
    return run.get(s.arena.allocator());
}

test "nexus: an activity-run operation completes with its activity's result, and the caller's history links to the activity" {
    var handlers: ActivityHandlers = .{};
    var s = try startActivitySetup(&handlers);
    defer s.deinit();
    const a = s.arena.allocator();
    const name = try uniqueName(a, "Zig");
    var r = try s.env.startWorkflow(CallActivityService, .{}, .{ s.endpoint, "compose", name });
    defer r.deinit();
    try testing.expectEqualStrings(try std.fmt.allocPrint(a, "Hello, {s}!", .{name}), try s.env.getResult(CallActivityService, &r, a, null));

    const activity_id = try std.fmt.allocPrint(a, "compose-{s}", .{name});
    var handle = try s.env.client.getActivityHandle([]const u8, activity_id, "");
    defer handle.deinit();
    const described = try handle.describe(a);
    try testing.expectEqualStrings(s.env.task_queue, described.task_queue);

    // The caller's started event links to the activity.
    const history = try s.env.client.fetchHistory(a, r.id, r.run_id);
    var started = false;
    for (history.events.items) |event| {
        if (event.event_type != .EVENT_TYPE_NEXUS_OPERATION_STARTED) continue;
        started = true;
        try testing.expectEqual(1, event.links.items.len);
        const link = event.links.items[0].variant.?.activity;
        try testing.expectEqualStrings("default", link.namespace);
        try testing.expectEqualStrings(activity_id, link.activity_id);
        try testing.expectEqualStrings(described.run_id, link.run_id);
    }
    try testing.expect(started);

    // The activity's completion callback carries the link back to the caller's scheduling event.
    const raw = try s.env.client.callService(.workflow, "DescribeActivityExecution", a, .{ .namespace = "default", .activity_id = activity_id }, null);
    try testing.expectEqual(1, raw.callbacks.items.len);
    const callback = raw.callbacks.items[0].info.?.callback.?;
    try testing.expectEqual(1, callback.links.items.len);
    const back = callback.links.items[0].variant.?.workflow_event;
    try testing.expectEqualStrings(r.id, back.workflow_id);
    try testing.expectEqual(EventType.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED, back.reference.?.event_ref.event_type);
}

test "nexus: an activity-run operation whose activity fails fails with the activity's failure" {
    var handlers: ActivityHandlers = .{};
    var s = try startActivitySetup(&handlers);
    defer s.deinit();
    const result = try s.run(CallActivityService, .{ s.endpoint, "refuse", "Zig" });
    try testing.expect(std.mem.startsWith(u8, result, "failed: "));
    try expectContains(result, "no greeting for Zig [application NoGreeting]");
}

test "nexus: canceling an activity-run operation cancels its heartbeating activity, and the caller sees the cancellation" {
    var handlers: ActivityHandlers = .{};
    var s = try startActivitySetup(&handlers);
    defer s.deinit();
    var r = try s.env.startWorkflow(CancelBeat, .{}, .{s.endpoint});
    defer r.deinit();
    try waitFor(&ActivityFixtures.beat_entered);
    try s.env.client.signalWorkflow(CancelNow, r.id, r.run_id, .{});
    const result = try s.env.getResult(CancelBeat, &r, s.arena.allocator(), null);
    try testing.expect(std.mem.startsWith(u8, result, "failed: "));
    try expectContains(result, "[canceled_failure_info]");
    try testing.expect(ActivityFixtures.beat_canceled.isSet());
}

test "nexus: a handler's second signal in one request is refused as a bad request, without retries, and the target gets one signal" {
    var handlers: ActivityHandlers = .{};
    var s = try startActivitySetup(&handlers);
    defer s.deinit();
    const a = s.arena.allocator();
    const target = try uniqueName(a, "ping-twice");
    var t = try s.env.startWorkflow(PingTarget, .{ .id = target }, .{});
    defer t.deinit();
    const result = try s.run(CallActivityService, .{ s.endpoint, "signal-twice", target });
    try testing.expect(std.mem.startsWith(u8, result, "failed: "));
    try expectContains(result, "SignalAlreadySent");
    try expectContains(result, "[handler BAD_REQUEST]");
    try testing.expectEqual(1, handlers.twice_calls.load(.monotonic));
    try testing.expectEqualStrings("pings=1", try releasePings(&s, target));
}

test "nexus: a handler that signals and is retried delivers its signal once, and the signal links back to the caller" {
    var handlers: ActivityHandlers = .{};
    var s = try startActivitySetup(&handlers);
    defer s.deinit();
    const a = s.arena.allocator();
    const target = try uniqueName(a, "ping-flaky");
    var t = try s.env.startWorkflow(PingTarget, .{ .id = target }, .{});
    defer t.deinit();
    var r = try s.env.startWorkflow(CallActivityService, .{}, .{ s.endpoint, "signal-flaky", target });
    defer r.deinit();
    try testing.expectEqualStrings("signaled on attempt 2", try s.env.getResult(CallActivityService, &r, a, null));
    try testing.expectEqual(2, handlers.flaky_calls.load(.monotonic));

    // The signal's event links to the caller's scheduling event.
    var signaled: ?proto.temporal.api.history.v1.HistoryEvent = null;
    for ((try s.env.client.fetchHistory(a, target, "")).events.items) |event| {
        if (event.event_type == .EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED) signaled = event;
    }
    const event = signaled orelse return error.TestSignalNotRecorded;
    try testing.expectEqual(1, event.links.items.len);
    const back = event.links.items[0].variant.?.workflow_event;
    try testing.expectEqualStrings(r.id, back.workflow_id);
    try testing.expectEqual(EventType.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED, back.reference.?.event_ref.event_type);
    try testing.expectEqualStrings("pings=1", try releasePings(&s, target));

    // The handler's response carried the server's link to the signal, which the caller's
    // completed event records.
    var completed_links: usize = 0;
    for ((try s.env.client.fetchHistory(a, r.id, r.run_id)).events.items) |e| {
        if (e.event_type != .EVENT_TYPE_NEXUS_OPERATION_COMPLETED) continue;
        for (e.links.items) |link| {
            const l = link.variant orelse continue;
            if (l == .workflow_event and std.mem.eql(u8, l.workflow_event.workflow_id, target)) completed_links += 1;
        }
    }
    try testing.expectEqual(1, completed_links);
}

test "nexus: a handler's signal-with-start needs a workflow ID, and with one starts and signals once across a retry" {
    var handlers: ActivityHandlers = .{};
    var s = try startActivitySetup(&handlers);
    defer s.deinit();
    const a = s.arena.allocator();
    const refused = try s.run(CallActivityService, .{ s.endpoint, "signal-with-start", "" });
    try testing.expect(std.mem.startsWith(u8, refused, "failed: "));
    try expectContains(refused, "InvalidOptions");
    try testing.expectEqual(0, handlers.sws_calls.load(.monotonic));

    const id = try uniqueName(a, "ping-started");
    // The retry finds the run the first attempt started, and its signal is dropped.
    try testing.expectEqualStrings("started=false on attempt 2", try s.run(CallActivityService, .{ s.endpoint, "signal-with-start", id }));
    try testing.expectEqualStrings("pings=1", try releasePings(&s, id));
}

// ---------------------------------------------------------------- serialization context

/// Records the context and the plain data of every payload it encodes, and passes payloads
/// through unchanged. Safe from the worker's and the client's threads.
const NexusRecordingCodec = struct {
    mutex: Io.Mutex = .init,
    arena: std.heap.ArenaAllocator = .init(testing.allocator),
    records: std.ArrayList(Record) = .empty,

    const Record = struct { encode: bool, nexus: ?converter.NexusContext, workflow_id: []const u8, data: []const u8 };

    fn codec(r: *NexusRecordingCodec) converter.PayloadCodec {
        return .{ .ptr = r, .vtable = &.{ .encode = encode, .decode = decode, .encodeWithContext = encodeWithContext, .decodeWithContext = decodeWithContext } };
    }

    fn deinit(r: *NexusRecordingCodec) void {
        r.arena.deinit();
    }

    fn encode(ptr: *anyopaque, arena: std.mem.Allocator, io: Io, payloads: []const converter.Payload) converter.CodecError![]converter.Payload {
        return encodeWithContext(ptr, arena, io, &.{}, payloads);
    }

    fn decode(ptr: *anyopaque, arena: std.mem.Allocator, io: Io, payloads: []const converter.Payload) converter.CodecError![]converter.Payload {
        return decodeWithContext(ptr, arena, io, &.{}, payloads);
    }

    fn encodeWithContext(ptr: *anyopaque, arena: std.mem.Allocator, io: Io, context: *const converter.Context, payloads: []const converter.Payload) converter.CodecError![]converter.Payload {
        try record(ptr, io, true, context, payloads);
        return arena.dupe(converter.Payload, payloads);
    }

    fn decodeWithContext(ptr: *anyopaque, arena: std.mem.Allocator, io: Io, context: *const converter.Context, payloads: []const converter.Payload) converter.CodecError![]converter.Payload {
        try record(ptr, io, false, context, payloads);
        return arena.dupe(converter.Payload, payloads);
    }

    fn record(ptr: *anyopaque, io: Io, encoding: bool, context: *const converter.Context, payloads: []const converter.Payload) error{OutOfMemory}!void {
        const r: *NexusRecordingCodec = @ptrCast(@alignCast(ptr));
        r.mutex.lockUncancelable(io);
        defer r.mutex.unlock(io);
        const a = r.arena.allocator();
        const nexus_copy: ?converter.NexusContext = if (context.nexus) |n| .{
            .endpoint = try a.dupe(u8, n.endpoint),
            .service = try a.dupe(u8, n.service),
            .operation = try a.dupe(u8, n.operation),
        } else null;
        for (payloads) |p| try r.records.append(a, .{ .encode = encoding, .nexus = nexus_copy, .workflow_id = try a.dupe(u8, context.workflow_id), .data = try a.dupe(u8, p.data) });
    }

    /// How many payloads holding `value`'s data were coded in the direction `encoding` with
    /// the Nexus context of `operation` through `endpoint`, and how many without a Nexus
    /// context, each of which must name one of `other_workflow_ids`.
    fn count(r: *NexusRecordingCodec, encoding: bool, value: []const u8, endpoint: []const u8, service: []const u8, operation: []const u8, other_workflow_ids: []const []const u8) !struct { nexus: usize, other: usize } {
        var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
        defer scratch.deinit();
        const want = (try converter.toPayload(scratch.allocator(), value)).data;
        r.mutex.lockUncancelable(testing.io);
        defer r.mutex.unlock(testing.io);
        var nexus_count: usize = 0;
        var other: usize = 0;
        for (r.records.items) |rec| {
            if (rec.encode != encoding or !std.mem.eql(u8, rec.data, want)) continue;
            const n = rec.nexus orelse {
                for (other_workflow_ids) |id| {
                    if (std.mem.eql(u8, id, rec.workflow_id)) break;
                } else {
                    std.debug.print("\"{s}\" was coded without a Nexus context for workflow \"{s}\"\n", .{ value, rec.workflow_id });
                    return error.TestUnexpectedContext;
                }
                other += 1;
                continue;
            };
            try testing.expectEqualStrings(endpoint, n.endpoint);
            try testing.expectEqualStrings(service, n.service);
            try testing.expectEqualStrings(operation, n.operation);
            try testing.expectEqualStrings("", rec.workflow_id);
            nexus_count += 1;
        }
        return .{ .nexus = nexus_count, .other = other };
    }
};

test "nexus: the caller's input and the backing workflow's result are coded with the endpoint's name, and both histories replay" {
    var handlers: Handlers = .{ .io = testing.io };
    var recording: NexusRecordingCodec = .{};
    defer recording.deinit();
    const codecs = [_]converter.PayloadCodec{recording.codec()};
    var s = try Setup.start(&handlers, .{ .codecs = &codecs, .dev_server_cli_version = testing_mod.DevServer.nexus_context_cli_version });
    defer s.deinit();
    const a = s.arena.allocator();
    const name = try uniqueName(a, "Zig");
    var r = try s.env.startWorkflow(CallGreet, .{}, .{ s.endpoint, name });
    defer r.deinit();
    try expectContains(try s.env.getResult(CallGreet, &r, a, null), try std.fmt.allocPrint(a, "Hello, {s} (token of ", .{name}));

    const greet_id = try std.fmt.allocPrint(a, "greet-{s}", .{name});
    // The caller's schedule command encodes the input, and the handler's worker decodes it,
    // with the operation's context. The handler's start encodes it again as the started
    // workflow's argument, with that workflow's own context; the client encoded it as the
    // caller's argument with the caller's.
    const own = [_][]const u8{ r.id, greet_id };
    const input_encoded = try recording.count(true, name, s.endpoint, Service.service_name, "greet", &own);
    // (`getResult` replays the caller, coding its commands again.)
    try testing.expect(input_encoded.nexus >= 1);
    try testing.expectEqual(2, input_encoded.other);
    try testing.expect((try recording.count(false, name, s.endpoint, Service.service_name, "greet", &own)).nexus >= 1);

    // The workflow the handler started encodes its result with the caller's context, which
    // the server propagated to it, and the caller decodes it with the same.
    const result = try std.fmt.allocPrint(a, "Hello, {s}", .{name});
    const result_encoded = try recording.count(true, result, s.endpoint, Service.service_name, "greet", &.{});
    try testing.expect(result_encoded.nexus >= 1);
    try testing.expectEqual(0, result_encoded.other);
    try testing.expect((try recording.count(false, result, s.endpoint, Service.service_name, "greet", &own)).nexus >= 1);

    try s.env.replayRun(greet_id, "");
    try s.env.replayRun(r.id, r.run_id);
}

test "nexus: an activity a handler started sees the caller's context, and codes its result and heartbeats with it" {
    var handlers: ActivityHandlers = .{};
    var recording: NexusRecordingCodec = .{};
    defer recording.deinit();
    const codecs = [_]converter.PayloadCodec{recording.codec()};
    var s = try startActivitySetupWith(&handlers, .{ .codecs = &codecs, .dev_server_cli_version = testing_mod.DevServer.nexus_context_cli_version });
    defer s.deinit();
    const a = s.arena.allocator();
    const name = try uniqueName(a, "Zig");
    var r = try s.env.startWorkflow(CallActivityService, .{}, .{ s.endpoint, "contextual", name });
    defer r.deinit();
    const result = try s.env.getResult(CallActivityService, &r, a, null);
    try testing.expectEqualStrings(try std.fmt.allocPrint(a, "{s} {s} {s}/contextual", .{ name, s.endpoint, ActivityService.service_name }), result);

    // The caller encodes the same value as its own result, with its own context.
    const result_encoded = try recording.count(true, result, s.endpoint, ActivityService.service_name, "contextual", &.{r.id});
    try testing.expect(result_encoded.nexus >= 1);
    try testing.expect(result_encoded.other >= 1);
    const beat = try recording.count(true, try std.fmt.allocPrint(a, "beat {s}", .{name}), s.endpoint, ActivityService.service_name, "contextual", &.{});
    try testing.expect(beat.nexus >= 1);
    try testing.expectEqual(0, beat.other);
}

/// Like a codec that encrypts with one key per Nexus endpoint: marks each payload with the
/// key of its context (the endpoint for a Nexus payload, "default" otherwise), and refuses
/// to decode a payload marked with another key, as decryption with the wrong key would.
const EndpointKeyCodec = struct {
    const key_metadata = "key-id";

    fn codec() converter.PayloadCodec {
        return .{ .ptr = undefined, .vtable = &.{ .encode = encode, .decode = decode, .encodeWithContext = encodeWithContext, .decodeWithContext = decodeWithContext } };
    }

    fn keyFor(context: *const converter.Context) []const u8 {
        return if (context.nexus) |n| n.endpoint else "default";
    }

    fn encode(ptr: *anyopaque, arena: std.mem.Allocator, io: Io, payloads: []const converter.Payload) converter.CodecError![]converter.Payload {
        return encodeWithContext(ptr, arena, io, &.{}, payloads);
    }

    fn decode(ptr: *anyopaque, arena: std.mem.Allocator, io: Io, payloads: []const converter.Payload) converter.CodecError![]converter.Payload {
        return decodeWithContext(ptr, arena, io, &.{}, payloads);
    }

    fn encodeWithContext(_: *anyopaque, arena: std.mem.Allocator, _: Io, context: *const converter.Context, payloads: []const converter.Payload) converter.CodecError![]converter.Payload {
        const out = try arena.alloc(converter.Payload, payloads.len);
        for (payloads, out) |p, *o| {
            o.* = try p.dupe(arena);
            try o.metadata.append(arena, .{ .key = key_metadata, .value = try arena.dupe(u8, keyFor(context)) });
        }
        return out;
    }

    fn decodeWithContext(_: *anyopaque, arena: std.mem.Allocator, _: Io, context: *const converter.Context, payloads: []const converter.Payload) converter.CodecError![]converter.Payload {
        const out = try arena.alloc(converter.Payload, payloads.len);
        for (payloads, out) |p, *o| {
            o.* = try p.dupe(arena);
            const index = for (o.metadata.items, 0..) |entry, i| {
                if (std.mem.eql(u8, entry.key, key_metadata)) break i;
            } else continue;
            if (!std.mem.eql(u8, o.metadata.items[index].value, keyFor(context))) {
                std.debug.print("EndpointKeyCodec: a payload encoded with key \"{s}\" was decoded with key \"{s}\" (workflow \"{s}\")\n", .{ o.metadata.items[index].value, keyFor(context), context.workflow_id });
                return error.CodecFailed;
            }
            _ = o.metadata.orderedRemove(index);
        }
        return out;
    }
};

test "nexus: with a codec keyed by Nexus endpoint, a caller's history still fetches and replays" {
    var handlers: Handlers = .{ .io = testing.io };
    const codecs = [_]converter.PayloadCodec{EndpointKeyCodec.codec()};
    var s = try Setup.start(&handlers, .{ .codecs = &codecs });
    defer s.deinit();
    const a = s.arena.allocator();
    var r = try s.env.startWorkflow(CallEcho, .{}, .{ s.endpoint, "hi" });
    defer r.deinit();
    // The run itself completes: its operation's input and result are coded with the
    // endpoint's key on both workers. `getResult` then fetches its history, whose Nexus
    // events hold those payloads, and replays it.
    try expectContains(try s.env.getResult(CallEcho, &r, a, null), "hi op=zig-test-service/echo");
    _ = try s.env.client.fetchHistory(a, r.id, r.run_id);
}

/// Reads every event of a run's history a page of one event at a time, so an operation's
/// scheduled event and its completion arrive on different pages.
fn readHistoryByEvent(c: *client_mod.Client, workflow_id: []const u8, run_id: []const u8) !usize {
    var it = try c.historyIterator(workflow_id, run_id, .{ .page_size = 1 });
    defer it.deinit();
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    var n: usize = 0;
    while (try it.next(scratch.allocator())) |_| n += 1;
    return n;
}

test "nexus: with a codec keyed by Nexus endpoint, the caller's and the backing workflow's histories read page by page, results read through handles, and both replay" {
    var handlers: Handlers = .{ .io = testing.io };
    const codecs = [_]converter.PayloadCodec{EndpointKeyCodec.codec()};
    var s = try Setup.start(&handlers, .{ .codecs = &codecs, .dev_server_cli_version = testing_mod.DevServer.nexus_context_cli_version });
    defer s.deinit();
    const a = s.arena.allocator();
    const name = try uniqueName(a, "Zig");
    var r = try s.env.startWorkflow(CallGreet, .{}, .{ s.endpoint, name });
    defer r.deinit();
    try expectContains(try s.env.getResult(CallGreet, &r, a, null), try std.fmt.allocPrint(a, "Hello, {s} (token of ", .{name}));
    try testing.expect(try readHistoryByEvent(&s.env.client, r.id, r.run_id) > 0);

    // The backing workflow encoded its result with the endpoint's key, as the server
    // propagated the caller's context to it.
    const greet_id = try std.fmt.allocPrint(a, "greet-{s}", .{name});
    try testing.expect(try readHistoryByEvent(&s.env.client, greet_id, "") > 0);
    var greeted = try s.env.client.getWorkflow([]const u8, greet_id, "");
    defer greeted.deinit();
    try testing.expectEqualStrings(try std.fmt.allocPrint(a, "Hello, {s}", .{name}), try greeted.get(a));
    try s.env.replayRun(greet_id, "");
    try s.env.replayRun(r.id, r.run_id);
}
