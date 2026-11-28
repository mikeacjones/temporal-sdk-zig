//! README.md examples. The code between `// example: <id>` and `// end-example` is copied into
//! README.md by `zig build readme`; edit it here, never in README.md.

const std = @import("std");
const temporal = @import("temporal");

const workflow = temporal.workflow;
const activity = temporal.activity;
const testing = std.testing;
const WorkflowEnvironment = temporal.testing.WorkflowEnvironment;
const ActivityEnvironment = temporal.testing.ActivityEnvironment;
const WorkflowReplayer = temporal.testing.WorkflowReplayer;
const LogRecorder = temporal.testing.LogRecorder;
const MetricsRecorder = temporal.testing.MetricsRecorder;

fn greet(ctx: *activity.Context, name: []const u8) ![]const u8 {
    if (name.len == 0) return ctx.fail(.{ .message = "name is empty", .type = "InvalidName", .non_retryable = true });
    return std.fmt.allocPrint(ctx.allocator(), "Hello, {s}!", .{name});
}

const Greet = activity.Def("greet", greet);

fn greeting(ctx: *workflow.Context, name: []const u8) ![]const u8 {
    return ctx.executeActivity(Greet, .{ .start_to_close_timeout = .fromSeconds(10) }, .{name}).get();
}

const Greeting = workflow.Def("greeting", greeting);

// example: testing-workflow-environment
fn greetDouble(ctx: *activity.Context, name: []const u8) ![]const u8 {
    return std.fmt.allocPrint(ctx.allocator(), "Hi, {s}!", .{name});
}

test "testing: a workflow with a test double for its activity" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(Greeting);
    try env.registerActivityAs(Greet, greetDouble);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const result = try env.executeWorkflow(Greeting, .{"Ziggy"}, arena.allocator(), null);
    try testing.expectEqualStrings("Hi, Ziggy!", result);
}
// end-example

// example: testing-existing-client
test "testing: a workflow environment on the test's own server" {
    var server = try temporal.testing.DevServer.start(testing.allocator, testing.io, .{});
    defer server.stop(testing.io);
    var client = try temporal.client.Client.connect(testing.allocator, testing.io, server.clientOptions());
    defer client.close();

    const env = try WorkflowEnvironment.fromClient(testing.allocator, testing.io, &client);
    defer env.deinit(); // stops the environment's worker; the client and the server stay up
    try env.registerWorkflow(Greeting);
    try env.registerActivityAs(Greet, greetDouble);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const result = try env.executeWorkflow(Greeting, .{"Ziggy"}, arena.allocator(), null);
    try testing.expectEqualStrings("Hi, Ziggy!", result);
}
// end-example

// example: testing-time-skipping
fn reminder(ctx: *workflow.Context, name: []const u8) ![]const u8 {
    try ctx.io().sleep(.fromSeconds(std.time.s_per_day), .awake);
    return ctx.executeActivity(Greet, .{ .start_to_close_timeout = .fromSeconds(10) }, .{name}).get();
}

test "testing: a day-long timer fires at once" {
    const env = try WorkflowEnvironment.startTimeSkipping(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(Reminder);
    try env.registerActivity(Greet);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const start = try env.now();
    _ = try env.executeWorkflow(Reminder, .{"Ziggy"}, arena.allocator(), null);
    try testing.expect(start.durationTo(try env.now()).nanoseconds >= std.time.ns_per_day);
}
// end-example

const Reminder = workflow.Def("reminder", reminder);

// example: testing-time-skipping-options
/// Counts the workflow runs it sees start, on the worker and in replays.
const CountRuns = struct {
    runs: std.atomic.Value(u32) = .init(0),

    pub fn interceptWorkflow(c: *CountRuns, _: *workflow.Context) error{OutOfMemory}!temporal.interceptor.Workflow {
        _ = c.runs.fetchAdd(1, .monotonic);
        return .{};
    }
};

test "testing: time skipping with a codec and a worker interceptor" {
    const zlib: temporal.converter.ZlibCodec = .{};
    const codecs = [_]temporal.converter.PayloadCodec{zlib.codec()};
    var counter: CountRuns = .{};
    const interceptors = [_]temporal.interceptor.Worker{.from(CountRuns, &counter)};
    const env = try WorkflowEnvironment.startTimeSkippingWithOptions(testing.allocator, testing.io, .{
        .codecs = &codecs,
        .worker_interceptors = &interceptors,
    });
    defer env.deinit();
    try env.registerWorkflow(Reminder);
    try env.registerActivity(Greet);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const result = try env.executeWorkflow(Reminder, .{"Ziggy"}, arena.allocator(), null);
    try testing.expectEqualStrings("Hello, Ziggy!", result);
    // Once on the worker, once in the replay that checks the run's history.
    try testing.expectEqual(2, counter.runs.load(.monotonic));
}
// end-example

const ChainStep = workflow.Def("chain_step", chainStep);

// example: testing-get-result-options
fn chainStep(ctx: *workflow.Context, step: u32) !u32 {
    if (step < 2) return ctx.continueAsNew(ChainStep, .{}, .{step + 1});
    return step;
}

test "testing: stop at the first run of a continue-as-new chain" {
    const env = try WorkflowEnvironment.startTimeSkipping(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(ChainStep);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var run = try env.startWorkflow(ChainStep, .{}, .{0});
    defer run.deinit();
    // Waits for the first run alone, and replays it.
    try testing.expectError(error.WorkflowContinuedAsNew, env.getResultWithOptions(ChainStep, &run, .{ .follow_runs = false }, arena.allocator(), null));
    // Follows the chain to the last run's result, and replays the last run.
    try testing.expectEqual(2, try env.getResult(ChainStep, &run, arena.allocator(), null));
}
// end-example

// example: testing-activity-environment
test "testing: an activity on its own" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var env: ActivityEnvironment = .init(testing.io);

    const result = try env.executeActivity(Greet, .{"Ziggy"}, arena.allocator());
    try testing.expectEqualStrings("Hello, Ziggy!", result);
    // A failure the activity describes with `ctx.fail` returns its error.
    try testing.expectError(error.ApplicationFailure, env.executeActivity(Greet, .{""}, arena.allocator()));
}
// end-example

// example: testing-activity-cancel
var waiting: std.Io.Event = .unset;

fn waitForever(ctx: *activity.Context) ![]const u8 {
    waiting.set(ctx.io());
    ctx.io().sleep(.fromSeconds(3600), .awake) catch |err| {
        const reason = ctx.cancelReason() orelse return err;
        return @tagName(reason); // a cancel the activity reports as a result, for the test
    };
    return "slept";
}
const WaitForever = activity.Def("wait_forever", waitForever);

fn runWaitForever(env: *ActivityEnvironment, arena: std.mem.Allocator) ![]const u8 {
    return env.executeActivity(WaitForever, .{}, arena);
}

test "testing: an activity sees a cancel and the worker stopping" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var env: ActivityEnvironment = .init(testing.io);
    env.info.heartbeat_timeout = .fromSeconds(5); // the new Info fields start out null or zero

    var running = try testing.io.concurrent(runWaitForever, .{ &env, arena.allocator() });
    try waiting.wait(testing.io);
    env.cancel(.timed_out); // as the worker cancels it: the next wait returns error.Canceled
    try testing.expectEqualStrings("timed_out", try running.await(testing.io));

    env.stopWorker(); // this and later activities see ctx.isWorkerStopping()
    try testing.expect(try env.executeActivity(activity.Def("stopping", isStopping), .{}, arena.allocator()));
}

fn isStopping(ctx: *activity.Context) !bool {
    try ctx.waitForWorkerStop(); // returns at once: the worker is already stopping
    return ctx.isWorkerStopping();
}
// end-example

// example: testing-activity-logger
fn charge(ctx: *activity.Context, amount: u32) !void {
    ctx.logger().info("charging", .{ .amount = amount });
    ctx.logger().err("card declined", .{ .amount = amount }); // through std.log, this fails the test
}

test "testing: read what an activity logs" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var recorder: LogRecorder = .init(testing.allocator, testing.io);
    defer recorder.deinit();
    var env: ActivityEnvironment = .init(testing.io);
    env.logger = recorder.sink();
    env.info.activity_id = "charge-1";

    try env.executeActivity(activity.Def("charge", charge), .{@as(u32, 10)}, arena.allocator());
    const records = try recorder.records(arena.allocator());
    try testing.expectEqual(2, records.len);
    try testing.expectEqual(.info, records[0].level);
    try testing.expectEqual(1, recorder.count(.err, "card declined"));
    try testing.expectEqualStrings("charge-1", records[1].get("ActivityID").?);
}
// end-example

// example: testing-log-recorder
fn checkout(ctx: *workflow.Context, order: u32) ![]const u8 {
    ctx.logger().err("payment declined", .{ .order = order }); // through std.log, this fails the test
    return "declined";
}
const Checkout = workflow.Def("checkout", checkout);

test "testing: a workflow that logs an error" {
    // Declared before the environment, so it outlives it.
    var recorder: LogRecorder = .init(testing.allocator, testing.io);
    defer recorder.deinit();
    const env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{ .logger = recorder.sink() });
    defer env.deinit();
    try env.registerWorkflow(Checkout);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    try testing.expectEqualStrings("declined", try env.executeWorkflow(Checkout, .{7}, arena.allocator(), null));
    // Once, though getResult replayed the run: a workflow's logger is silent during replay.
    try testing.expectEqual(1, recorder.count(.err, "payment declined"));
    for (try recorder.records(arena.allocator())) |rec| {
        if (rec.level == .err) try testing.expectEqualStrings("checkout", rec.get("WorkflowType").?);
    }
}
// end-example

// example: testing-metrics-recorder
fn ship(ctx: *workflow.Context, items: u32) !u32 {
    const shipped = try ctx.metrics().counter("orders_shipped", .{ .description = "Orders shipped" });
    defer shipped.deinit();
    shipped.add(1, .{ .carrier = "zigpost" });
    return items;
}
const Ship = workflow.Def("ship", ship);

test "testing: check what a workflow records" {
    // Declared before the environment, so it outlives it.
    var recorder: MetricsRecorder = .init(testing.allocator);
    defer recorder.deinit();
    const env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{
        .metrics = .{ .exporter = .{ .custom = recorder.handler() } },
    });
    defer env.deinit();
    try env.registerWorkflow(Ship);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    try testing.expectEqual(3, try env.executeWorkflow(Ship, .{3}, arena.allocator(), null));
    // Once, though getResult replayed the run: a replaying workflow records nothing.
    const shipped = try recorder.recordsOf(arena.allocator(), "orders_shipped");
    try testing.expectEqual(1, shipped.len);
    try testing.expectEqual(temporal.metrics.Value{ .unsigned = 1 }, shipped[0].value);
    try testing.expectEqualStrings("zigpost", shipped[0].get("carrier").?.string);
    try testing.expectEqualStrings("ship", shipped[0].get("workflow_type").?.string);
    // sdk-core's own metrics arrive too, with the default `temporal_` prefix.
    try testing.expect(recorder.hasMetric("temporal_workflow_completed"));
}
// end-example

/// `greeting` after a change that drops its activity call.
fn greetingWithoutActivity(_: *workflow.Context, name: []const u8) ![]const u8 {
    _ = name;
    return "Hello!";
}

const ChangedGreeting = workflow.Def("greeting", greetingWithoutActivity);

test "testing: replay catches a change that breaks recorded histories" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(Greeting);
    try env.registerActivity(Greet);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var run = try env.startWorkflow(Greeting, .{}, .{"Ziggy"});
    defer run.deinit();
    _ = try env.getResult(Greeting, &run, arena.allocator(), null);

    // example: testing-replay
    const history = try env.client.fetchHistory(arena.allocator(), run.id, run.run_id);
    var replayer: WorkflowReplayer = .init(testing.allocator, testing.io);
    defer replayer.deinit();
    try replayer.registerWorkflow(Greeting);
    try replayer.replayHistory(history);

    // ChangedGreeting is registered as "greeting" but no longer calls its activity.
    var changed: WorkflowReplayer = .init(testing.allocator, testing.io);
    defer changed.deinit();
    try changed.registerWorkflow(ChangedGreeting);
    try testing.expectError(error.Nondeterminism, changed.replayHistory(history));
    // end-example
}
