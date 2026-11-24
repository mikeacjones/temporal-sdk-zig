//! Borrowed for the task executing one feature. Activities must use their own context.
const Context = @This();

const std = @import("std");
const temporal = @import("temporal");
const history = @import("history.zig");
const Feature = @import("harness.zig").Feature;
const Io = std.Io;
const Worker = temporal.worker.Worker;

pub const Capability = struct { name: []const u8, value: bool };

/// Thread-safe allocator for values that outlive a call.
gpa: std.mem.Allocator,
io: std.Io,
/// Task-local allocator whose values live through teardown.
arena: std.mem.Allocator,
/// Shared with the worker and closed by the harness.
client: *temporal.client.Client,
feature: []const u8,
namespace: []const u8,
task_queue: []const u8,
variant: ?[]const u8 = null,
/// Runner-created endpoint targeting task_queue.
nexus_endpoint: ?[]const u8 = null,
capabilities: []const Capability = &.{},
failure_message_: ?[]const u8 = null,
worker_: ?*Worker = null,
running_: ?*Io.Future(Worker.RunError!void) = null,
/// The feature being run; set by the runner, for `startWorker`.
definition_: ?*const Feature = null,
/// The worker `startWorker` made, owned by the context and allocated from `gpa`.
restarted_: ?*Restarted = null,
/// The worker the last `stopWorker` stopped, for its `fatalMessage`.
stopped_: ?*Worker = null,

const Restarted = struct { worker: Worker, running: Io.Future(Worker.RunError!void) };

pub fn startOptions(ctx: *const Context) temporal.client.StartWorkflowOptions {
    return .{ .id = ctx.task_queue, .task_queue = ctx.task_queue, .execution_timeout = .fromSeconds(60) };
}

pub fn fail(ctx: *Context, comptime fmt: []const u8, args: anytype) error{FeatureFailed} {
    const message = std.fmt.allocPrint(ctx.arena, fmt, args) catch "out of memory formatting feature failure";
    ctx.failure_message_ = if (message.len == 0) "feature failed without a message" else message;
    return error.FeatureFailed;
}

pub fn expect(ctx: *Context, ok: bool, comptime fmt: []const u8, args: anytype) error{FeatureFailed}!void {
    if (!ok) return ctx.fail(fmt, args);
}

pub fn expectEqualStrings(ctx: *Context, expected: []const u8, actual: []const u8) error{FeatureFailed}!void {
    if (!std.mem.eql(u8, expected, actual))
        return ctx.fail("expected \"{f}\", got \"{f}\"", .{ std.zig.fmtString(expected), std.zig.fmtString(actual) });
}

/// Reads all recorded events, with payloads decoded by the client's codecs.
/// The result lives in the feature arena. An empty run ID selects the latest run.
pub fn fetchHistory(ctx: *Context, workflow_id: []const u8, run_id: []const u8) temporal.client.HistoryError!temporal.history.History {
    return ctx.client.fetchHistory(ctx.arena, workflow_id, run_id);
}

/// Looks up a capability declared by this variant, if any.
pub fn capability(ctx: *const Context, name: []const u8) ?bool {
    for (ctx.capabilities) |item| {
        if (std.mem.eql(u8, name, item.name)) return item.value;
    }
    return null;
}

/// Stops polling and waits for the worker's run task. The harness still owns the worker.
pub fn stopWorker(ctx: *Context) (Worker.RunError || error{NoWorker})!void {
    const worker = ctx.worker_ orelse return error.NoWorker;
    const running = ctx.running_ orelse return error.NoWorker;
    ctx.worker_ = null;
    ctx.running_ = null;
    ctx.stopped_ = worker;
    worker.stop();
    return running.await(ctx.io);
}

/// Starts a new worker for this feature on `task_queue`, with the feature's workflows,
/// activities, Nexus services and worker options, after `stopWorker` stopped the previous
/// one. The context owns it; `stopWorker` stops it like the first. Every failure but
/// cancellation is reported through `fail`, with a message starting "startWorker: ".
pub fn startWorker(ctx: *Context) error{ FeatureFailed, Canceled }!void {
    const definition = ctx.definition_ orelse return ctx.fail("startWorker: the feature declares no workflows, activities or Nexus services", .{});
    if (!definition.has_worker) return ctx.fail("startWorker: the feature declares no workflows, activities or Nexus services", .{});
    if (ctx.worker_ != null) return ctx.fail("startWorker: the feature's worker is still running; call stopWorker first", .{});
    // An earlier restarted worker has been stopped, since `worker_` is null.
    ctx.freeRestarted();

    const gpa = ctx.gpa;
    const io = ctx.io;
    const restarted = gpa.create(Restarted) catch |err| return ctx.fail("startWorker: worker init returned error.{t}", .{err});
    restarted.worker = Worker.init(gpa, io, ctx.client, ctx.task_queue, definition.workerOptions()) catch |err| {
        gpa.destroy(restarted);
        return ctx.fail("startWorker: worker init returned error.{t}", .{err});
    };
    const worker = &restarted.worker;
    var started = false;
    errdefer {
        if (started) {
            worker.stop();
            // The run task must end before the worker is freed, even if this task was canceled.
            const protection = io.swapCancelProtection(.blocked);
            defer _ = io.swapCancelProtection(protection);
            restarted.running.await(io) catch {};
        }
        worker.deinit();
        gpa.destroy(restarted);
    }
    definition.register(worker) catch |err| return ctx.fail("startWorker: registering feature definitions returned error.{t}", .{err});
    restarted.running = io.concurrent(Worker.run, .{worker}) catch |err|
        return ctx.fail("startWorker: starting worker task returned error.{t}", .{err});
    started = true;
    worker.waitUntilRunning() catch |start_err| switch (start_err) {
        error.Canceled => return error.Canceled,
        error.WorkerFailedToStart => {
            started = false;
            worker.stop();
            restarted.running.await(io) catch |err| {
                const message = worker.fatalMessage();
                return ctx.fail("startWorker: worker failed to start with error.{t}{s}{s}", .{ err, if (message == null) "" else ": ", message orelse "" });
            };
            return ctx.fail("startWorker: worker failed to start with error.{t}", .{start_err});
        },
    };
    ctx.restarted_ = restarted;
    ctx.worker_ = worker;
    ctx.running_ = &restarted.running;
}

/// Frees the worker `startWorker` made, if any. The runner calls it after the last
/// `stopWorker`; a feature never does.
pub fn deinit(ctx: *Context) void {
    std.debug.assert(ctx.worker_ == null);
    ctx.freeRestarted();
    ctx.stopped_ = null;
}

fn freeRestarted(ctx: *Context) void {
    const restarted = ctx.restarted_ orelse return;
    if (ctx.stopped_ == &restarted.worker) ctx.stopped_ = null;
    restarted.worker.deinit();
    ctx.gpa.destroy(restarted);
    ctx.restarted_ = null;
}

/// Polls the named run's history without retaining snapshots in the feature arena.
pub fn waitForEvent(
    ctx: *Context,
    workflow_id: []const u8,
    run_id: []const u8,
    event_type: history.EventType,
    timeout: Io.Duration,
) (temporal.client.HistoryError || error{FeatureFailed})!void {
    const io = ctx.io;
    const deadline = Io.Clock.Timestamp.now(io, .boot).addDuration(.{ .raw = timeout, .clock = .boot });
    if (timeout.nanoseconds > 0) {
        const Result = union(enum) { event: temporal.client.HistoryError!void, timeout: Io.Cancelable!void };
        var buffer: [2]Result = undefined;
        var race: Io.Select(Result) = .init(io, &buffer);
        defer race.cancelDiscard();
        race.concurrent(.timeout, Io.Clock.Timestamp.wait, .{ deadline, io }) catch
            return ctx.fail("cannot start the timeout waiting for {t} in workflow {s} run {s}", .{ event_type, workflow_id, run_id });
        race.concurrent(.event, pollHistory, .{ ctx.client, io, ctx.gpa, workflow_id, run_id, event_type }) catch
            return ctx.fail("cannot start polling for {t} in workflow {s} run {s}", .{ event_type, workflow_id, run_id });
        switch (try race.await()) {
            .event => |result| return result,
            .timeout => |result| try result,
        }
    }
    return ctx.fail("workflow {s} run {s} has no {t} event after {f}", .{ workflow_id, run_id, event_type, timeout });
}

fn pollHistory(
    client: *temporal.client.Client,
    io: Io,
    gpa: std.mem.Allocator,
    workflow_id: []const u8,
    run_id: []const u8,
    event_type: history.EventType,
) temporal.client.HistoryError!void {
    var scratch: std.heap.ArenaAllocator = .init(gpa);
    defer scratch.deinit();
    while (true) {
        const recorded = try client.fetchHistory(scratch.allocator(), workflow_id, run_id);
        if (history.contains(recorded, event_type)) return;
        _ = scratch.reset(.retain_capacity);
        try io.sleep(.fromMilliseconds(100), .boot);
    }
}

/// Reads all recorded events with their stored payloads. The result lives in the feature
/// arena. An empty run ID selects the latest run.
pub fn fetchRawHistory(ctx: *Context, workflow_id: []const u8, run_id: []const u8) (temporal.client.CallServiceError || error{OutOfMemory})!temporal.history.History {
    return readRawHistory(ctx.client, ctx.arena, ctx.namespace, workflow_id, run_id);
}

/// Reads every page with its stored payloads into the caller's arena.
pub fn readRawHistory(client: anytype, arena: std.mem.Allocator, namespace: []const u8, workflow_id: []const u8, run_id: []const u8) temporal.client.CallServiceError!temporal.history.History {
    var recorded: temporal.history.History = .{};
    var token: []const u8 = "";
    while (true) {
        const page = try client.callService(.workflow, "GetWorkflowExecutionHistory", arena, .{
            .namespace = namespace,
            .execution = .{ .workflow_id = workflow_id, .run_id = run_id },
            .next_page_token = token,
            .history_event_filter_type = .HISTORY_EVENT_FILTER_TYPE_ALL_EVENT,
            .skip_archival = true,
        }, null);
        if (page.history) |h| try recorded.events.appendSlice(arena, h.events.items);
        token = page.next_page_token;
        if (token.len == 0) return recorded;
    }
}

pub fn expectEvent(ctx: *Context, recorded: temporal.history.History, event_type: history.EventType) error{FeatureFailed}!void {
    try ctx.expect(history.contains(recorded, event_type), "history is missing event {f}", .{EventName{ .data = event_type }});
}

pub fn expectNoEvent(ctx: *Context, recorded: temporal.history.History, event_type: history.EventType) error{FeatureFailed}!void {
    try ctx.expect(!history.contains(recorded, event_type), "history contains unexpected event {f}", .{EventName{ .data = event_type }});
}

pub fn expectEventsInOrder(ctx: *Context, recorded: temporal.history.History, event_types: []const history.EventType) error{FeatureFailed}!void {
    const EventNames = std.fmt.Alt([]const history.EventType, formatEventTypes);
    try ctx.expect(history.containsInOrder(recorded, event_types), "history does not contain events in order {f}", .{EventNames{ .data = event_types }});
}

const EventName = std.fmt.Alt(history.EventType, formatEventType);

fn formatEventType(event_type: history.EventType, writer: *std.Io.Writer) std.Io.Writer.Error!void {
    if (std.enums.tagName(history.EventType, event_type)) |name| {
        try writer.writeAll(name);
    } else {
        try writer.print("unknown ({d})", .{@backingInt(event_type)});
    }
}

fn formatEventTypes(event_types: []const history.EventType, writer: *std.Io.Writer) std.Io.Writer.Error!void {
    try writer.writeAll("{ ");
    for (event_types, 0..) |event_type, i| {
        if (i != 0) try writer.writeAll(", ");
        try formatEventType(event_type, writer);
    }
    try writer.writeAll(" }");
}

test "conformance context defaults and assertions" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var ctx: Context = .{
        .gpa = std.testing.allocator,
        .io = std.testing.io,
        .arena = arena.allocator(),
        .client = undefined,
        .feature = "signal/basic",
        .namespace = "default",
        .task_queue = "conformance-signal/basic",
    };
    const options = ctx.startOptions();
    try std.testing.expectError(error.NoWorker, ctx.stopWorker());
    try std.testing.expectEqual(null, ctx.capability("workerHeartbeats"));
    ctx.capabilities = &.{
        .{ .name = "workerHeartbeats", .value = false },
        .{ .name = "pollerAutoscaling", .value = true },
    };
    try std.testing.expectEqual(@as(?bool, false), ctx.capability("workerHeartbeats"));
    try std.testing.expectEqual(@as(?bool, true), ctx.capability("pollerAutoscaling"));
    try std.testing.expectEqual(null, ctx.capability("unknown"));
    try std.testing.expectEqualStrings(ctx.task_queue, options.id);
    try std.testing.expectEqualStrings(ctx.task_queue, options.task_queue);
    try std.testing.expectEqual(@as(?std.Io.Duration, .fromSeconds(60)), options.execution_timeout);
    try ctx.expectEqualStrings("same", "same");
    try std.testing.expectEqual(null, ctx.failure_message_);
    try std.testing.expectError(error.FeatureFailed, ctx.expectEqualStrings("one", "two"));
    try std.testing.expectEqualStrings("expected \"one\", got \"two\"", ctx.failure_message_.?);
    try std.testing.expectError(error.FeatureFailed, ctx.expect(false, "expected {d}, got {d}", .{ 1, 2 }));
    try std.testing.expectEqualStrings("expected 1, got 2", ctx.failure_message_.?);
}

test "conformance history assertions report the expected event types" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var ctx: Context = .{
        .gpa = std.testing.allocator,
        .io = std.testing.io,
        .arena = arena.allocator(),
        .client = undefined,
        .feature = "update/basic",
        .namespace = "default",
        .task_queue = "conformance-update/basic",
    };
    const accepted: history.EventType = .EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED;
    const completed: history.EventType = .EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_COMPLETED;
    const rejected: history.EventType = .EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_REJECTED;
    var events = [_]temporal.history.HistoryEvent{
        .{ .event_type = accepted },
        .{ .event_type = completed },
    };
    var recorded: temporal.history.History = .{ .events = .initBuffer(&events) };
    recorded.events.items = &events;
    try ctx.expectEvent(recorded, accepted);
    try ctx.expectNoEvent(recorded, rejected);
    try ctx.expectEventsInOrder(recorded, &.{ accepted, completed });
    try std.testing.expectEqual(null, ctx.failure_message_);

    try std.testing.expectError(error.FeatureFailed, ctx.expectEvent(recorded, rejected));
    try std.testing.expectEqualStrings("history is missing event " ++ @tagName(rejected), ctx.failure_message_.?);
    try std.testing.expectError(error.FeatureFailed, ctx.expectNoEvent(recorded, accepted));
    try std.testing.expectEqualStrings("history contains unexpected event " ++ @tagName(accepted), ctx.failure_message_.?);
    try std.testing.expectError(error.FeatureFailed, ctx.expectEventsInOrder(recorded, &.{ completed, accepted }));
    try std.testing.expectEqualStrings(
        "history does not contain events in order { " ++ @tagName(completed) ++ ", " ++ @tagName(accepted) ++ " }",
        ctx.failure_message_.?,
    );
    try std.testing.expectError(error.FeatureFailed, ctx.expectEvent(recorded, @fromBackingInt(1234)));
    try std.testing.expectEqualStrings("history is missing event unknown (1234)", ctx.failure_message_.?);
}

test "conformance raw history preserves stored payloads across pages" {
    const Pager = struct {
        fn callService(
            _: @This(),
            comptime service: temporal.client.Service,
            comptime method: []const u8,
            arena: std.mem.Allocator,
            request: temporal.client.ServiceRequest(service, method),
            _: ?*?temporal.client.RpcFailure,
        ) temporal.client.CallServiceError!temporal.client.ServiceResponse(service, method) {
            const execution = request.execution orelse return error.InvalidArgument;
            if (!std.mem.eql(u8, request.namespace, "feature-namespace") or
                !std.mem.eql(u8, execution.workflow_id, "workflow") or
                !std.mem.eql(u8, execution.run_id, "run") or request.wait_new_event or
                request.history_event_filter_type != .HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
                return error.InvalidArgument;

            var page: temporal.history.History = .{};
            if (request.next_page_token.len == 0) {
                const payload = try temporal.converter.payloadWithEncoding(arena, "my-encoding", "opaque bytes");
                var input: temporal.converter.Payloads = .{};
                try input.payloads.append(arena, payload);
                try page.events.append(arena, .{
                    .event_id = 1,
                    .event_type = .EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
                    .attributes = .{ .workflow_execution_started_event_attributes = .{ .input = input } },
                });
                return .{ .history = page, .next_page_token = "empty" };
            }
            if (std.mem.eql(u8, request.next_page_token, "empty")) return .{ .next_page_token = "last" };
            if (!std.mem.eql(u8, request.next_page_token, "last")) return error.InvalidArgument;
            try page.events.append(arena, .{ .event_id = 2, .event_type = .EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED });
            return .{ .history = page };
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const recorded = try readRawHistory(Pager{}, arena.allocator(), "feature-namespace", "workflow", "run");
    try std.testing.expectEqual(2, recorded.events.items.len);
    try std.testing.expectEqual(1, recorded.events.items[0].event_id);
    try std.testing.expectEqual(2, recorded.events.items[1].event_id);
    const payloads = history.inputPayloads(recorded);
    try std.testing.expectEqual(1, payloads.len);
    try std.testing.expectEqualStrings("my-encoding", temporal.converter.encodingOf(payloads[0]).?);
    try std.testing.expectEqualStrings("opaque bytes", payloads[0].data);
}

test "conformance waitForEvent cancels a pending history read at the deadline" {
    const gpa = std.testing.allocator;
    const io = std.testing.io;
    const event_type: history.EventType = .EVENT_TYPE_ACTIVITY_TASK_SCHEDULED;
    const FakeHistory = struct {
        pub fn call(_: *@This(), request: *temporal.interceptor.ClientCall, _: temporal.interceptor.Client.Next) temporal.interceptor.ClientError!void {
            const service = temporal.proto.temporal.api.workflowservice.v1;
            const execution = (request.request(service.GetWorkflowExecutionHistoryRequest) orelse return error.InvalidResponse).execution orelse return error.InvalidResponse;
            if (!std.mem.eql(u8, execution.run_id, "run-1")) return error.NotFound;
            if (std.mem.eql(u8, execution.workflow_id, "pending")) {
                var never: Io.Event = .unset;
                never.waitTimeout(std.testing.io, .{ .duration = .{ .raw = .fromSeconds(2), .clock = .boot } }) catch |err|
                    return if (err == error.Canceled) error.Canceled else error.RpcFailed;
                return error.InvalidResponse;
            }
            var recorded: temporal.history.History = .{};
            try recorded.events.append(request.arena, .{ .event_type = event_type });
            const response = request.response(service.GetWorkflowExecutionHistoryResponse) orelse return error.InvalidResponse;
            response.* = .{ .history = recorded };
        }
    };
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    var fixture: FakeHistory = .{};
    var client = try temporal.client.Client.init(gpa, io, .{ .interceptors = &.{temporal.interceptor.Client.from(FakeHistory, &fixture)} });
    defer client.close();
    var ctx: Context = .{
        .gpa = gpa,
        .io = io,
        .arena = arena.allocator(),
        .client = &client,
        .feature = "test/history",
        .namespace = "default",
        .task_queue = "conformance-test/history",
    };
    try ctx.waitForEvent("ready", "run-1", event_type, .fromSeconds(1));
    try std.testing.expectError(error.NotFound, ctx.waitForEvent("ready", "unknown", event_type, .fromSeconds(1)));
    try std.testing.expectError(error.FeatureFailed, ctx.waitForEvent("pending", "run-1", event_type, .fromMilliseconds(10)));
    try std.testing.expectEqualStrings("workflow pending run run-1 has no EVENT_TYPE_ACTIVITY_TASK_SCHEDULED event after 10ms", ctx.failure_message_.?);
}

test "conformance stopWorker returns the run result and consumes the worker future" {
    const gpa = std.testing.allocator;
    const io = std.testing.io;
    var client = try temporal.client.Client.init(gpa, io, .{});
    defer client.close();
    var worker = try Worker.init(gpa, io, &client, "conformance-test/worker", .{});
    defer worker.deinit();
    var running = io.async(Worker.run, .{&worker});
    var ctx: Context = .{
        .gpa = gpa,
        .io = io,
        .arena = gpa,
        .client = &client,
        .feature = "test/worker",
        .namespace = "default",
        .task_queue = "conformance-test/worker",
        .worker_ = &worker,
        .running_ = &running,
    };
    try std.testing.expectError(error.NothingRegistered, ctx.stopWorker());
    try std.testing.expectError(error.NoWorker, ctx.stopWorker());
}

test "conformance startWorker reports misuse through fail" {
    const gpa = std.testing.allocator;
    const io = std.testing.io;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    var client = try temporal.client.Client.init(gpa, io, .{});
    defer client.close();
    var ctx: Context = .{
        .gpa = gpa,
        .io = io,
        .arena = arena.allocator(),
        .client = &client,
        .feature = "test/worker",
        .namespace = "default",
        .task_queue = "conformance-test/worker",
    };
    const no_worker_message = "startWorker: the feature declares no workflows, activities or Nexus services";
    try std.testing.expectError(error.FeatureFailed, ctx.startWorker());
    try std.testing.expectEqualStrings(no_worker_message, ctx.failure_message_.?);

    const Example = struct {
        fn run(_: *temporal.workflow.Context) !void {}
        fn execute(_: *Context) !void {}
    };
    const client_only = @import("harness.zig").define(.{ .execute = Example.execute });
    ctx.definition_ = &client_only;
    ctx.failure_message_ = null;
    try std.testing.expectError(error.FeatureFailed, ctx.startWorker());
    try std.testing.expectEqualStrings(no_worker_message, ctx.failure_message_.?);

    const with_workflow = @import("harness.zig").define(.{
        .workflows = .{temporal.workflow.Def("Workflow", Example.run)},
        .execute = Example.execute,
    });
    ctx.definition_ = &with_workflow;
    var worker = try Worker.init(gpa, io, &client, "conformance-test/worker", .{});
    defer worker.deinit();
    var running = io.async(Worker.run, .{&worker});
    ctx.worker_ = &worker;
    ctx.running_ = &running;
    try std.testing.expectError(error.FeatureFailed, ctx.startWorker());
    try std.testing.expectEqualStrings("startWorker: the feature's worker is still running; call stopWorker first", ctx.failure_message_.?);
    try std.testing.expectEqual(&worker, ctx.worker_.?);
    try std.testing.expectError(error.NothingRegistered, ctx.stopWorker());
    try std.testing.expectEqual(&worker, ctx.stopped_.?);
    ctx.deinit();
    try std.testing.expectEqual(null, ctx.stopped_);
    try std.testing.expectEqual(null, ctx.restarted_);
}

test "conformance startWorker frees the new worker when it fails to start" {
    const gpa = std.testing.allocator;
    const io = std.testing.io;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    var client = try temporal.client.Client.init(gpa, io, .{});
    defer client.close();
    const Example = struct {
        fn run(_: *temporal.workflow.Context) !void {}
        fn execute(_: *Context) !void {}
    };
    // Option validation fails when the worker runs, before the lazy client connects.
    const feature = @import("harness.zig").define(.{
        .workflows = .{temporal.workflow.Def("Workflow", Example.run)},
        .worker_options = .{ .max_activities_per_second = 0 },
        .execute = Example.execute,
    });
    var ctx: Context = .{
        .gpa = gpa,
        .io = io,
        .arena = arena.allocator(),
        .client = &client,
        .feature = "test/worker",
        .namespace = "default",
        .task_queue = "conformance-test/worker",
        .definition_ = &feature,
    };
    defer ctx.deinit();
    try std.testing.expectError(error.FeatureFailed, ctx.startWorker());
    try std.testing.expectEqualStrings("startWorker: worker failed to start with error.InvalidOptions", ctx.failure_message_.?);
    try std.testing.expectEqual(null, ctx.restarted_);
    try std.testing.expectEqual(null, ctx.worker_);
    try std.testing.expectEqual(null, ctx.running_);
}

test "conformance deinit without a restarted worker does nothing" {
    var ctx: Context = .{
        .gpa = std.testing.allocator,
        .io = std.testing.io,
        .arena = std.testing.allocator,
        .client = undefined,
        .feature = "test/worker",
        .namespace = "default",
        .task_queue = "conformance-test/worker",
    };
    ctx.deinit();
    ctx.deinit();
    try std.testing.expectEqual(null, ctx.restarted_);
    try std.testing.expectEqual(null, ctx.stopped_);
}
