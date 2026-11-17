//! README.md examples. The code between `// example: <id>` and `// end-example` is copied into
//! README.md by `zig build readme`; edit it here, never in README.md.

const std = @import("std");
const temporal = @import("temporal");

const testing = std.testing;
const activity = temporal.activity;
const workflow = temporal.workflow;
const Worker = temporal.worker.Worker;

fn greet(ctx: *activity.Context, name: []const u8) ![]const u8 {
    return std.fmt.allocPrint(ctx.allocator(), "Hello, {s}!", .{name});
}
const Greet = activity.Def("greet", greet);

fn greeting(ctx: *workflow.Context, name: []const u8) ![]const u8 {
    return ctx.executeActivity(Greet, .{ .start_to_close_timeout = .fromSeconds(10) }, .{name}).get();
}
const Greeting = workflow.Def("greeting", greeting);

test "workers: a worker runs a workflow until it is stopped" {
    var server = try temporal.testing.DevServer.start(testing.allocator, testing.io, .{});
    defer server.stop(testing.io);
    var client = try temporal.client.Client.connect(testing.allocator, testing.io, server.clientOptions());
    defer client.close();
    const gpa = testing.allocator;
    const io = testing.io;
    var arena_state: std.heap.ArenaAllocator = .init(gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    // example: worker-run
    var w = try Worker.init(gpa, io, &client, "greetings", .{});
    defer w.deinit();
    try w.registerWorkflow(Greeting);
    try w.registerActivity(Greet);

    // `run` returns once `stop` is called and in-flight work has finished.
    var running = try io.concurrent(Worker.run, .{&w});
    defer {
        w.stop();
        running.await(io) catch |err|
            std.log.err("greetings worker failed: {t} {s}", .{ err, w.fatalMessage() orelse "" });
    }
    try w.waitUntilRunning();

    var run = try client.executeWorkflow(Greeting, .{ .id = "greeting-ana", .task_queue = "greetings" }, .{"Ana"});
    defer run.deinit();
    const result = try run.get(arena); // "Hello, Ana!"
    // end-example
    try testing.expectEqualStrings("Hello, Ana!", result);
}

test "workers: a worker with its options set runs a workflow" {
    var server = try temporal.testing.DevServer.start(testing.allocator, testing.io, .{});
    defer server.stop(testing.io);
    var client = try temporal.client.Client.connect(testing.allocator, testing.io, server.clientOptions());
    defer client.close();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();

    var configured = try initGreetingWorker(testing.allocator, testing.io, &client);
    defer configured.deinit();
    try testing.expectEqual(@as(usize, 20), configured.options.max_concurrent_activity_executions);
    // The core accepts every option: the worker starts polling and runs a workflow.
    var running = try testing.io.concurrent(Worker.run, .{&configured});
    defer {
        configured.stop();
        running.await(testing.io) catch {};
    }
    try configured.waitUntilRunning();
    var run = try client.executeWorkflow(Greeting, .{ .id = "greeting-bo", .task_queue = "greetings" }, .{"Bo"});
    defer run.deinit();
    try testing.expectEqualStrings("Hello, Bo!", try run.get(arena.allocator()));
}

fn initGreetingWorker(gpa: std.mem.Allocator, io: std.Io, client: *temporal.client.Client) !Worker {
    // example: worker-options
    var w = try Worker.init(gpa, io, client, "greetings", .{
        .identity = "greetings-worker-1", // default: the client's identity
        .max_concurrent_activity_executions = 20,
        .max_concurrent_local_activity_executions = 50,
        .max_concurrent_activity_task_pollers = 4,
        .max_cached_workflows = 500,
        .sticky_schedule_to_start_timeout = .fromSeconds(5),
        .max_activities_per_second = 50, // this worker
        .max_task_queue_activities_per_second = 200, // all workers on "greetings"
        .default_heartbeat_throttle_interval = .fromSeconds(10),
        .max_heartbeat_throttle_interval = .fromSeconds(30),
        .max_eager_activity_reservations_per_workflow_task = 0, // no eager activities
        .graceful_shutdown_period = .fromSeconds(10),
        .workflow_memory_limit = 16 * 1024 * 1024,
    });
    // end-example
    errdefer w.deinit();
    try w.registerWorkflow(Greeting);
    try w.registerActivity(Greet);
    return w;
}

// example: worker-versioning
/// Runs stay on the build they started on; other workflows follow the worker's default.
const PinnedGreeting = workflow.DefWithOptions("pinned_greeting", greeting, .{ .versioning_behavior = .pinned });

fn runVersionedWorker(gpa: std.mem.Allocator, io: std.Io, client: *temporal.client.Client) !void {
    var w = try Worker.init(gpa, io, client, "greetings", .{
        .deployment = .{ .deployment_name = "greetings", .build_id = "2.0", .default_versioning_behavior = .auto_upgrade },
    });
    defer w.deinit();
    try w.registerWorkflow(Greeting);
    try w.registerWorkflow(PinnedGreeting);
    try w.registerActivity(Greet);
    try w.runUntilSignal();
}
// end-example

test "workers: deadlock detection takes a limit, or null to turn it off" {
    var client: temporal.client.Client = undefined; // never used: the workers don't run
    const gpa = testing.allocator;
    const io = testing.io;

    // example: worker-deadlock-detection
    // Workflow code may run up to 5 s without waiting before its task fails (default: 2 s).
    var patient = try Worker.init(gpa, io, &client, "reports", .{ .deadlock_detection_timeout = .fromSeconds(5) });
    defer patient.deinit();
    // No detection, for workflow code stopped at a debugger's breakpoints.
    var debugging = try Worker.init(gpa, io, &client, "reports-debug", .{ .deadlock_detection_timeout = null });
    defer debugging.deinit();
    // end-example
    try testing.expectEqual(std.Io.Duration.fromSeconds(5), patient.options.deadlock_detection_timeout.?);
    try testing.expect(debugging.options.deadlock_detection_timeout == null);
}

test "workers: poller behavior is a simple maximum or autoscaling, per kind of task" {
    var client: temporal.client.Client = undefined; // never used: the worker doesn't run
    const gpa = testing.allocator;
    const io = testing.io;

    // example: worker-poller-behavior
    var w = try Worker.init(gpa, io, &client, "orders", .{
        // Between 2 and 20 workflow task polls, starting from 4, as the server suggests.
        .workflow_task_poller_behavior = .{ .autoscaling = .{ .minimum = 2, .maximum = 20, .initial = 4 } },
        // The defaults: between 1 and 100, starting from 5.
        .activity_task_poller_behavior = .{ .autoscaling = .{} },
        // Up to 4 Nexus task polls at once.
        .nexus_task_poller_behavior = .{ .simple_maximum = 4 },
    });
    defer w.deinit();
    // end-example
    try testing.expectEqual(20, w.options.workflow_task_poller_behavior.?.autoscaling.maximum);
    try testing.expectEqual(4, w.options.nexus_task_poller_behavior.?.simple_maximum);
}

test "workers: a versioned worker compiles" {
    _ = &runVersionedWorker;
}

fn writeCard(ctx: *activity.Context, name: []const u8) ![]const u8 {
    return std.fmt.allocPrint(ctx.allocator(), "card-{s}.txt", .{name});
}
const WriteCard = activity.Def("write_card", writeCard);

fn printCard(ctx: *activity.Context, path: []const u8) ![]const u8 {
    return std.fmt.allocPrint(ctx.allocator(), "printed {s}", .{path});
}
const PrintCard = activity.Def("print_card", printCard);

// example: worker-sessions
/// Writes a card to the host's disk and prints it there: both activities run on one host.
fn greetingCard(ctx: *workflow.Context, name: []const u8) ![]const u8 {
    const session = try ctx.createSession(.{
        .creation_timeout = .fromSeconds(60),
        .execution_timeout = .fromSeconds(600),
    });
    defer ctx.completeSession(session) catch {};
    const options: workflow.ActivityOptions = .{ .start_to_close_timeout = .fromSeconds(30), .session = session };
    const path = try ctx.executeActivity(WriteCard, options, .{name}).get();
    return ctx.executeActivity(PrintCard, options, .{path}).get();
}
const GreetingCard = workflow.Def("greeting_card", greetingCard);

// end-example

fn initCardWorker(gpa: std.mem.Allocator, io: std.Io, client: *temporal.client.Client) !Worker {
    var w = try Worker.init(gpa, io, client, "greeting-cards", .{
        .sessions = .{ .max_concurrent_sessions = 4 },
    });
    errdefer w.deinit();
    try w.registerWorkflow(GreetingCard);
    try w.registerActivity(WriteCard);
    try w.registerActivity(PrintCard);
    return w;
}

test "workers: a session runs its activities on one host" {
    var server = try temporal.testing.DevServer.start(testing.allocator, testing.io, .{});
    defer server.stop(testing.io);
    var client = try temporal.client.Client.connect(testing.allocator, testing.io, server.clientOptions());
    defer client.close();

    var w = try initCardWorker(testing.allocator, testing.io, &client);
    defer w.deinit();
    var running = try testing.io.concurrent(Worker.run, .{&w});
    defer {
        w.stop();
        running.await(testing.io) catch {};
    }
    try w.waitUntilRunning();

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var run = try client.executeWorkflow(GreetingCard, .{ .id = "card-ana", .task_queue = "greeting-cards" }, .{"Ana"});
    defer run.deinit();
    try testing.expectEqualStrings("printed card-Ana.txt", try run.get(arena.allocator()));
}
