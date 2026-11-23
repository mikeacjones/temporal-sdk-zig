//! README.md examples. The code between `// example: <id>` and `// end-example` is copied into
//! README.md by `zig build readme`; edit it here, never in README.md.

const std = @import("std");
const temporal = @import("temporal");
const quickstart = @import("quickstart.zig");

const Allocator = std.mem.Allocator;
const Io = std.Io;
const activity = temporal.activity;
const workflow = temporal.workflow;
const Greet = quickstart.Greet;
const Greeting = quickstart.Greeting;

fn connectExamples(gpa: Allocator, io: Io, arena: Allocator, api_key: []const u8) !void {
    // example: client-connect
    var cloud = try temporal.client.Client.connect(gpa, io, .{
        .host_port = "my-namespace.a1b2c.tmprl.cloud:7233",
        .namespace = "my-namespace.a1b2c",
        .api_key = api_key,
        .tls = .{}, // mutual TLS: .{ .client_cert_path = "client.pem", .client_private_key_path = "client.key" }
    });
    defer cloud.close();
    // A profile of the Temporal config file, overridden by TEMPORAL_* environment variables.
    var staging = try temporal.client.Client.connect(gpa, io, try temporal.client.loadOptions(arena, .{ .profile = "staging" }));
    defer staging.close();
    // end-example
}

fn lazyConnectExample(gpa: Allocator, io: Io, options: temporal.client.Options, rotated_key: []const u8) !void {
    // example: client-lazy
    // Nothing goes over the network yet: the first call that needs the server connects.
    var client = try temporal.client.Client.init(gpa, io, options);
    defer client.close();
    // Optional: connect now, to see a failure here rather than on the first call.
    try client.ensureConnected();
    // Rotate the API key and headers: every later call, the workers' included, sends these.
    try client.updateApiKey(rotated_key);
    try client.updateHeaders(&.{.{ .key = "x-tenant", .value = "blue" }});
    // end-example
}

fn namespacesExample(gpa: Allocator, io: Io, options: temporal.client.Options) !u64 {
    // example: client-namespaces
    var client = try temporal.client.Client.connect(gpa, io, options);
    defer client.close();
    // Another namespace of the same server, over the same connection and runtime.
    var billing = try client.forNamespace("billing");
    defer billing.close();
    // Its calls, and workers on it, work in "billing".
    const open_invoices = try billing.countWorkflows("ExecutionStatus = 'Running'");
    // end-example
    return open_invoices;
}

fn rpcOptionsExample(client: *temporal.client.Client, request_id: []const u8) !u64 {
    // example: client-rpc-options
    // A view of the client whose calls get their own deadline and extra gRPC metadata.
    var quick = try client.withRpcOptions(.{
        .timeout = .fromSeconds(2),
        .metadata = &.{.{ .key = "x-request-id", .value = request_id }},
        .binary_metadata = &.{.{ .key = "x-trace-bin", .value = &.{ 0x01, 0x02 } }},
    });
    // Calls through it, and the handles they return, send those; `client`'s own calls don't.
    const running = try quick.countWorkflows("ExecutionStatus = 'Running'");
    // end-example
    return running;
}

fn serviceCallsExample(client: *temporal.client.Client, arena: Allocator) ![]const u8 {
    // example: client-service-calls
    const errordetails = temporal.proto.temporal.api.errordetails.v1;
    // Any method of the workflow, operator, cloud, test or health service, with its
    // generated request and response types.
    _ = try client.callService(.workflow, "RegisterNamespace", arena, .{
        .namespace = "orders",
        .description = "Order processing",
        .workflow_execution_retention_period = .{ .seconds = 7 * 24 * 60 * 60 },
    }, null);
    const described = try client.callService(.workflow, "DescribeNamespace", arena, .{ .namespace = "orders" }, null);
    const description = described.namespace_info.?.description; // "Order processing"

    // A refused call returns the error of its gRPC status; `failure` holds the status.
    var failure: ?temporal.client.RpcFailure = null;
    _ = client.callService(.workflow, "DescribeNamespace", arena, .{ .namespace = "returns" }, &failure) catch |err| switch (err) {
        error.NotFound => if (try failure.?.detail(errordetails.NamespaceNotFoundFailure, arena)) |missing|
            std.log.info("no namespace {s}", .{missing.namespace}),
        else => return err,
    };
    // Per-call deadline, retries and metadata come from a view.
    var patient = try client.withRpcOptions(.{ .timeout = .fromSeconds(10), .retry = true });
    _ = try patient.callService(.operator, "ListSearchAttributes", arena, .{ .namespace = "orders" }, null);
    // end-example
    return description;
}

fn transportExample(gpa: Allocator, io: Io, proxy_password: []const u8) !void {
    // example: client-transport
    var client = try temporal.client.Client.connect(gpa, io, .{
        .host_port = "temporal.internal:7233",
        // Tunnel the connection through an HTTP CONNECT proxy, with basic auth (both or neither).
        .http_connect_proxy = .{ .host_port = "proxy.corp:3128", .username = "svc", .password = proxy_password },
        // Ping every 10 s and drop the connection when a ping goes unanswered for 5 s.
        .keep_alive = .{ .interval = .fromSeconds(10), .timeout = .fromSeconds(5) },
        // Workers on this client heartbeat to the server every 30 s; `.zero` turns that off.
        .worker_heartbeat_interval = .fromSeconds(30),
    });
    defer client.close();
    // end-example
}

pub fn startWorkflows(client: *temporal.client.Client, arena: Allocator) ![]const u8 {
    // example: client-start
    var run = try client.executeWorkflow(Greeting, .{ .id = "greeting-ada", .task_queue = "greetings", .execution_timeout = .fromSeconds(60) }, .{"Ada"});
    defer run.deinit();
    const greeting = try run.get(arena);
    // By type name, giving the result type. No ID: the client generates a UUID.
    var by_name = try client.executeWorkflowByName([]const u8, "greeting", .{ .task_queue = "greetings" }, .{"Grace"});
    defer by_name.deinit();
    _ = try by_name.get(arena);
    // end-example
    return greeting;
}

pub fn controlStandaloneActivity(client: *temporal.client.Client, arena: Allocator) ![]const u8 {
    // example: client-standalone-activity-control
    var handle = try client.executeActivity(Greet, .{
        .id = "greet-later",
        .task_queue = "greetings",
        .start_to_close_timeout = .fromSeconds(10),
        .start_delay = .fromSeconds(1), // no worker gets it before then
        .priority = .{ .priority_key = 1 },
        .summary = "greet Grace once the delay passes",
        .id_conflict_policy = .use_existing,
    }, .{"Grace"});
    defer handle.deinit();
    std.log.info("started a new activity: {}", .{handle.started}); // false if it was running
    try handle.pause("hold on");
    try handle.unpause(.{ .reason = "go ahead" });
    const greeting = try handle.get(arena);
    const description = try handle.describe(arena);
    std.log.info("{s}: {s}", .{ description.summary, @tagName(description.status) });
    if (description.heartbeat_details.len() > 0) {
        const progress = try description.heartbeat_details.get(u32, arena, 0);
        std.log.info("last progress: {d}", .{progress});
    }
    try handle.delete(); // forget it: describe now returns error.NotFound
    // end-example
    return greeting;
}

/// Holds a greeting that updates change, until the `finish` signal ends it with
/// "<greeting>, <name>!".
const Greeter = struct {
    greeting: []const u8 = "Hello",
    done: bool = false,

    pub const queries = .{ .current = CurrentGreeting };
    pub const signals = .{ .finish = Finish };
    pub const updates = .{ .setGreeting = .{ SetGreeting, .checkGreeting } };

    pub fn run(g: *Greeter, ctx: *workflow.Context, name: []const u8) ![]const u8 {
        try ctx.await(g, isDone);
        return std.fmt.allocPrint(ctx.allocator(), "{s}, {s}!", .{ g.greeting, name });
    }

    pub fn current(g: *Greeter) []const u8 {
        return g.greeting;
    }

    pub fn finish(g: *Greeter) void {
        g.done = true;
    }

    pub fn setGreeting(g: *Greeter, greeting: []const u8) []const u8 {
        const previous = g.greeting;
        g.greeting = greeting;
        return previous;
    }

    pub fn checkGreeting(_: *Greeter, greeting: []const u8) error{EmptyGreeting}!void {
        if (greeting.len == 0) return error.EmptyGreeting;
    }

    fn isDone(g: *Greeter) bool {
        return g.done;
    }
};

// example: client-messages
pub const GreeterWorkflow = workflow.Def("greeter", Greeter);
pub const Finish = workflow.SignalDef("finish", fn () void);
pub const CurrentGreeting = workflow.QueryDef("current_greeting", fn () []const u8);
pub const SetGreeting = workflow.UpdateDef("set_greeting", fn (greeting: []const u8) []const u8);

pub const Replies = struct { previous: []const u8, current: []const u8, result: []const u8 };

pub fn sendMessages(client: *temporal.client.Client, arena: Allocator) !Replies {
    var run = try client.executeWorkflow(GreeterWorkflow, .{ .id = "greeter-1", .task_queue = "greetings" }, .{"World"});
    defer run.deinit();
    var update = try client.updateWorkflow(SetGreeting, run.id, "", .{"Hi"}, .{});
    defer update.deinit();
    const previous = try update.get(arena); // the previous greeting, "Hello"
    const current = try client.queryWorkflow(CurrentGreeting, arena, run.id, "", .{}); // "Hi"
    try client.signalWorkflow(Finish, run.id, "", .{});
    var signaled = try client.signalWithStartWorkflow(GreeterWorkflow, .{ .id = "greeter-2", .task_queue = "greetings" }, .{"Zig"}, Finish, .{});
    defer signaled.deinit();
    var updated = try client.updateWithStartWorkflow(GreeterWorkflow, .{ .id = "greeter-3", .task_queue = "greetings", .id_conflict_policy = .use_existing }, .{"Ziggy"}, SetGreeting, .{"Howdy"}, .{});
    defer updated.deinit();
    return .{ .previous = previous, .current = current, .result = try run.get(arena) }; // result: "Hi, World!"
}
// end-example

// example: client-messages-by-name
/// Message names known only at run time, for example read from configuration.
pub const MessageNames = struct { workflow_type: []const u8, signal: []const u8, query: []const u8, update: []const u8 };

pub fn sendMessagesByName(client: *temporal.client.Client, arena: Allocator, names: MessageNames) !Replies {
    const start: temporal.client.StartWorkflowOptions = .{ .id = "greeter-6", .task_queue = "greetings", .id_conflict_policy = .use_existing };
    var started = try client.updateWithStartWorkflowByName([]const u8, names.workflow_type, start, .{"Kai"}, names.update, .{"Hiya"}, .{});
    defer started.deinit();
    _ = try started.get(arena); // "Hello"
    var update = try client.updateWorkflowByName([]const u8, names.update, start.id, "", .{"Yo"}, .{});
    defer update.deinit();
    const previous = try update.get(arena); // "Hiya"
    const reply = try client.queryWorkflowByName(names.query, arena, start.id, "", .{});
    const current = try reply.get([]const u8, arena); // "Yo"
    try client.signalWorkflowByName(names.signal, start.id, "", .{});
    var signaled = try client.signalWithStartWorkflowByName([]const u8, names.workflow_type, .{ .id = "greeter-7", .task_queue = "greetings" }, .{"Ivy"}, names.signal, .{});
    defer signaled.deinit();
    var run = try client.getWorkflow([]const u8, start.id, started.run_id);
    defer run.deinit();
    return .{ .previous = previous, .current = current, .result = try run.get(arena) }; // result: "Yo, Kai!"
}
// end-example

// example: client-update-wait
const UpdateWaitError = temporal.client.UpdateError || temporal.client.UpdateResultError;
const Reply = union(enum) {
    greeting: UpdateWaitError![]const u8,
    timeout: Io.Cancelable!void,
};

fn setGreeting(client: *temporal.client.Client, arena: Allocator, workflow_id: []const u8, greeting: []const u8) UpdateWaitError![]const u8 {
    var update = try client.updateWorkflow(SetGreeting, workflow_id, "", .{greeting}, .{ .update_id = "set-greeting-1" });
    defer update.deinit();
    return update.get(arena);
}

/// The previous greeting, or null when no reply came within `timeout`. A null leaves
/// delivery unknown: the timeout can end the call before the server has the update. Calling
/// again is safe: it sends the same update ID, which the server runs once per workflow run,
/// so the call returns the result of the update already delivered, or delivers it now.
pub fn setGreetingWithin(client: *temporal.client.Client, io: Io, arena: Allocator, workflow_id: []const u8, timeout: Io.Duration) !?[]const u8 {
    var buffer: [2]Reply = undefined;
    var select: Io.Select(Reply) = .init(io, &buffer);
    defer select.cancelDiscard(); // cancels the task still waiting
    try select.concurrent(.greeting, setGreeting, .{ client, arena, workflow_id, "Hey" });
    try select.concurrent(.timeout, Io.sleep, .{ io, timeout, .awake });
    return switch (try select.await()) {
        .greeting => |previous| try previous,
        .timeout => null,
    };
}

/// Updates the workflow `greeter-4`, starting it first if it isn't running, then waits for
/// that workflow's result.
pub fn updateThenWait(client: *temporal.client.Client, arena: Allocator) ![]const u8 {
    var update = try client.updateWithStartWorkflow(GreeterWorkflow, .{ .id = "greeter-4", .task_queue = "greetings", .id_conflict_policy = .use_existing }, .{"Lin"}, SetGreeting, .{"Hey"}, .{});
    defer update.deinit();
    // The handle names the run that the call started or found.
    var run = try client.getWorkflow([]const u8, update.workflow_id, update.run_id);
    defer run.deinit();
    try client.signalWorkflow(Finish, run.id, run.run_id, .{});
    return run.get(arena); // "Hey, Lin!"
}
// end-example

pub fn manageWorkflows(client: *temporal.client.Client, arena: Allocator) !void {
    // example: client-manage
    var run = try client.executeWorkflow(GreeterWorkflow, .{ .task_queue = "greetings" }, .{"Ada"});
    defer run.deinit();
    const description = try client.describeWorkflow(arena, run.id, run.run_id);
    const history = try client.fetchHistory(arena, run.id, run.run_id);
    std.log.info("{t}, {d} events", .{ description.status, history.events.items.len });

    var it = try client.listWorkflows("WorkflowType = 'greeter' AND ExecutionStatus = 'Running'", .{});
    defer it.deinit();
    while (try it.next(arena)) |info| std.log.info("running: {s}", .{info.workflow_id});
    std.log.info("{d} in all", .{try client.countWorkflows("WorkflowType = 'greeter'")});

    try client.cancelWorkflow(run.id, run.run_id, "no longer needed");
    // end-example
    try std.testing.expectError(error.WorkflowCanceled, run.get(arena));
    var stuck = try client.executeWorkflow(GreeterWorkflow, .{ .task_queue = "greetings" }, .{"Bob"});
    defer stuck.deinit();
    try client.terminateWorkflow(stuck.id, stuck.run_id, "stuck");
    try std.testing.expectError(error.WorkflowTerminated, stuck.get(arena));

    var chain = try client.executeWorkflow(GreeterWorkflow, .{ .id = "greeter-chain", .task_queue = "greetings" }, .{"Cy"});
    defer chain.deinit();
    // Another chain's first run changes nothing.
    try std.testing.expectError(error.NotFound, stopChain(client, chain.id, run.run_id, run.id, run.run_id, false));
    try stopChain(client, chain.id, chain.run_id, run.id, run.run_id, false);
    try std.testing.expectError(error.WorkflowCanceled, chain.get(arena));
    var forced = try client.executeWorkflow(GreeterWorkflow, .{ .id = "greeter-forced", .task_queue = "greetings" }, .{"Di"});
    defer forced.deinit();
    try stopChain(client, forced.id, forced.run_id, run.id, run.run_id, true);
    try std.testing.expectError(error.WorkflowTerminated, forced.get(arena));
}

/// Cancels, or with `force` terminates, the workflow `workflow_id` if its chain of runs
/// began with the run `first_run_id`, linking the request to the run that asked for it.
pub fn stopChain(client: *temporal.client.Client, workflow_id: []const u8, first_run_id: []const u8, requester_id: []const u8, requester_run_id: []const u8, force: bool) !void {
    // example: client-cancel-options
    // Links record in the workflow's history what asked for the stop.
    const links = [_]temporal.proto.temporal.api.common.v1.Link{.{ .variant = .{ .workflow_event = .{
        .namespace = client.namespace,
        .workflow_id = requester_id,
        .run_id = requester_run_id,
    } } }};
    // Only the chain of runs that began with `first_run_id`: if the workflow ID has since
    // started a new chain, the call returns error.NotFound and changes nothing.
    if (force) {
        try client.terminateWorkflowWithOptions(workflow_id, "", "forced stop", {}, .{
            .first_execution_run_id = first_run_id,
            .links = &links,
        });
    } else {
        try client.cancelWorkflowWithOptions(workflow_id, "", "no longer needed", .{
            .first_execution_run_id = first_run_id,
            .links = &links,
        });
    }
    // end-example
}

pub fn resetGreeter(client: *temporal.client.Client, arena: Allocator, workflow_id: []const u8) ![]const u8 {
    // example: client-reset
    // Reset the latest run to the end of its first workflow task: the new run keeps the
    // history up to there. Reading the run ID once makes both calls name the same run.
    const run_id = (try client.describeWorkflow(arena, workflow_id, "")).run_id;
    const history = try client.fetchHistory(arena, workflow_id, run_id);
    const first_task = for (history.events.items) |event| {
        if (event.attributes) |attrs| if (attrs == .workflow_task_completed_event_attributes) break event.event_id;
    } else return error.NoCompletedWorkflowTask;
    const new_run_id = try client.resetWorkflow(arena, workflow_id, run_id, .{
        .workflow_task_finish_event_id = first_task,
        .reason = "greet again",
        .reapply_exclude = .{ .signals = true }, // the new run doesn't get the earlier `finish`
    });
    var new_run = try client.getWorkflow([]const u8, workflow_id, new_run_id);
    defer new_run.deinit();
    // end-example
    return new_run_id;
}

pub fn overrideVersioning(client: *temporal.client.Client, arena: Allocator, workflow_id: []const u8) !void {
    // example: client-execution-options
    // Auto-upgrade from the next workflow task on, whatever the workflow's definition says.
    const options = try client.updateWorkflowExecutionOptions(arena, workflow_id, "", .{
        .versioning_override = .{ .set = .auto_upgrade },
    });
    std.log.info("versioning override: {s}", .{if (options.versioning_override) |o| @tagName(o) else "none"});
    // Back to the definition's behavior.
    _ = try client.updateWorkflowExecutionOptions(arena, workflow_id, "", .{ .versioning_override = .remove });
    // end-example
}

pub fn scheduleGreetings(client: *temporal.client.Client, arena: Allocator) !void {
    // example: client-schedule
    var handle = try client.createSchedule("daily-greeting", .{ .schedule = .{
        .action = try temporal.schedule.startWorkflow(arena, Greeting, .{"World"}, .{ .id = "daily-greeting", .task_queue = "greetings" }),
        .spec = .{ .calendars = &.{.{ .hour = &.{.{ .start = 9 }} }}, .time_zone_name = "Europe/Paris" },
    } });
    defer handle.deinit();
    try handle.trigger(.unspecified); // once now, as well as daily at 9:00
    try handle.pause("maintenance");
    const description = try handle.describe(arena);
    std.log.info("paused: {}", .{description.schedule.state.paused});
    try handle.delete();
    // end-example
}

pub fn runStandaloneActivity(client: *temporal.client.Client, arena: Allocator) ![]const u8 {
    // example: client-standalone-activity
    var handle = try client.executeActivity(Greet, .{
        .task_queue = "greetings",
        .start_to_close_timeout = .fromSeconds(10),
    }, .{"Ada"});
    defer handle.deinit();
    const greeting = try handle.get(arena);
    const description = try handle.describe(arena);
    std.log.info("{s} took {d} attempt(s)", .{ description.activity_type, description.attempt });
    // end-example
    return greeting;
}

// example: client-async-completion
const Outbox = struct {
    gpa: Allocator,
    task_token: []u8 = &.{},
    ready: Io.Event = .unset,

    fn greetLater(o: *Outbox, ctx: *activity.Context, _: []const u8) ![]const u8 {
        o.task_token = try o.gpa.dupe(u8, ctx.info().task_token); // hand off a copy
        o.ready.set(ctx.io());
        return ctx.completeAsync(); // the activity stays open on the server
    }
};
pub const GreetLater = activity.Def("greet_later", Outbox.greetLater);
// Later, in any process that can reach the server:
pub fn finishGreeting(client: *temporal.client.Client, task_token: []const u8) !void {
    try client.completeActivity(task_token, "Hello, Lin!");
}
// end-example

fn greetLaterWorkflow(ctx: *workflow.Context, name: []const u8) ![]const u8 {
    return ctx.executeActivity(GreetLater, .{ .start_to_close_timeout = .fromSeconds(60) }, .{name}).get();
}
const GreetLaterWorkflow = workflow.Def("greet_later_workflow", greetLaterWorkflow);

fn runWorker(w: *temporal.worker.Worker) temporal.worker.Worker.RunError!void {
    return w.run();
}

test "client: connecting compiles" {
    _ = &connectExamples;
    _ = &transportExample;
}

/// A client interceptor that holds every UpdateWorkflowExecution request until the call is
/// canceled, so no update is ever sent.
const UpdateBlocker = struct {
    io: Io,
    never: Io.Event = .unset,
    /// How many requests it held.
    held: std.atomic.Value(u32) = .init(0),

    pub fn call(b: *UpdateBlocker, input: *temporal.interceptor.ClientCall, next: temporal.interceptor.Client.Next) temporal.interceptor.ClientError!void {
        if (std.mem.eql(u8, input.method, "UpdateWorkflowExecution")) {
            _ = b.held.fetchAdd(1, .monotonic);
            try b.never.wait(b.io);
        }
        return next.call(input);
    }
};

pub const Proceed = workflow.SignalDef("proceed", fn () void);
pub const UpdateCount = workflow.QueryDef("update_count", fn () u32);
/// Like `GreeterWorkflow`, but its `SetGreeting` handler sets the greeting, counts the
/// update, and then waits for `Proceed` before it returns.
pub const SlowGreeterWorkflow = workflow.Def("slow_greeter", slowGreeter);

const SlowGreeter = struct {
    ctx: *workflow.Context,
    greeting: []const u8 = "Hello",
    updates: u32 = 0,
    proceeded: bool = false,
    done: bool = false,

    fn current(s: *SlowGreeter) []const u8 {
        return s.greeting;
    }

    fn updateCount(s: *SlowGreeter) u32 {
        return s.updates;
    }

    fn proceed(s: *SlowGreeter) void {
        s.proceeded = true;
    }

    fn finish(s: *SlowGreeter) void {
        s.done = true;
    }

    fn setGreeting(s: *SlowGreeter, greeting: []const u8) ![]const u8 {
        const previous = s.greeting;
        s.greeting = greeting;
        s.updates += 1;
        try s.ctx.await(s, hasProceeded);
        return previous;
    }

    fn hasProceeded(s: *SlowGreeter) bool {
        return s.proceeded;
    }

    fn isDone(s: *SlowGreeter) bool {
        return s.done;
    }
};

fn slowGreeter(ctx: *workflow.Context, name: []const u8) ![]const u8 {
    const s = try ctx.allocator().create(SlowGreeter);
    s.* = .{ .ctx = ctx };
    try ctx.setQueryHandler(CurrentGreeting, s, SlowGreeter.current);
    try ctx.setQueryHandler(UpdateCount, s, SlowGreeter.updateCount);
    try ctx.setSignalHandler(Proceed, s, SlowGreeter.proceed);
    try ctx.setSignalHandler(Finish, s, SlowGreeter.finish);
    try ctx.setUpdateHandler(SetGreeting, s, SlowGreeter.setGreeting);
    try ctx.await(s, SlowGreeter.isDone);
    try ctx.await(ctx, workflow.Context.allHandlersFinished);
    return std.fmt.allocPrint(ctx.allocator(), "{s}, {s}!", .{ s.greeting, name });
}

test "client: examples against a dev server" {
    const gpa = std.testing.allocator;
    const io = std.testing.io;
    var server = try temporal.testing.DevServer.start(gpa, io, .{
        .extra_args = &(temporal.testing.DevServer.standalone_activity_args ++ [_][]const u8{ "--namespace", "billing" }),
    });
    defer server.stop(io);

    try lazyConnectExample(gpa, io, server.clientOptions(), "rotated-key");
    try std.testing.expectEqual(0, try namespacesExample(gpa, io, server.clientOptions()));

    var client = try temporal.client.Client.connect(gpa, io, server.clientOptions());
    defer client.close();
    try std.testing.expectEqual(0, try rpcOptionsExample(&client, "req-1"));
    {
        var arena_state: std.heap.ArenaAllocator = .init(gpa);
        defer arena_state.deinit();
        try std.testing.expectEqualStrings("Order processing", try serviceCallsExample(&client, arena_state.allocator()));
    }

    var outbox: Outbox = .{ .gpa = gpa };
    defer gpa.free(outbox.task_token);
    var w = try temporal.worker.Worker.init(gpa, io, &client, "greetings", .{});
    defer w.deinit();
    try w.registerWorkflow(Greeting);
    try w.registerWorkflow(GreeterWorkflow);
    try w.registerWorkflow(SlowGreeterWorkflow);
    try w.registerWorkflow(GreetLaterWorkflow);
    try w.registerActivity(Greet);
    try w.registerActivityWith(GreetLater, &outbox);
    var running = try io.concurrent(runWorker, .{&w});
    defer {
        w.stop();
        running.await(io) catch {};
    }

    var arena_state: std.heap.ArenaAllocator = .init(gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    try std.testing.expectEqualStrings("Hello, Ada!", try startWorkflows(&client, arena));

    const replies = try sendMessages(&client, arena);
    try std.testing.expectEqualStrings("Hello", replies.previous);
    try std.testing.expectEqualStrings("Hi", replies.current);
    try std.testing.expectEqualStrings("Hi, World!", replies.result);
    var signaled = try client.getWorkflow([]const u8, "greeter-2", "");
    defer signaled.deinit();
    try std.testing.expectEqualStrings("Hello, Zig!", try signaled.get(arena));
    try client.signalWorkflow(Finish, "greeter-3", "", .{});
    var updated = try client.getWorkflow([]const u8, "greeter-3", "");
    defer updated.deinit();
    try std.testing.expectEqualStrings("Howdy, Ziggy!", try updated.get(arena));
    try std.testing.expectEqualStrings("Hey, Lin!", try updateThenWait(&client, arena));
    const by_name = try sendMessagesByName(&client, arena, .{ .workflow_type = "greeter", .signal = "finish", .query = "current_greeting", .update = "set_greeting" });
    try std.testing.expectEqualStrings("Hiya", by_name.previous);
    try std.testing.expectEqualStrings("Yo", by_name.current);
    try std.testing.expectEqualStrings("Yo, Kai!", by_name.result);
    var signaled_by_name = try client.getWorkflow([]const u8, "greeter-7", "");
    defer signaled_by_name.deinit();
    try std.testing.expectEqualStrings("Hello, Ivy!", try signaled_by_name.get(arena));
    var bounded = try client.executeWorkflow(GreeterWorkflow, .{ .id = "greeter-5", .task_queue = "greetings" }, .{"Mo"});
    defer bounded.deinit();
    {
        // The timeout fires before the update is sent: the interceptor holds the request
        // until the call is canceled, so the workflow never sees it.
        var blocker: UpdateBlocker = .{ .io = io };
        const interceptors = [_]temporal.interceptor.Client{.from(UpdateBlocker, &blocker)};
        var options = server.clientOptions();
        options.interceptors = &interceptors;
        var blocking_client = try temporal.client.Client.connect(gpa, io, options);
        defer blocking_client.close();
        try std.testing.expectEqual(null, try setGreetingWithin(&blocking_client, io, arena, bounded.id, .fromSeconds(1)));
        try std.testing.expectEqual(1, blocker.held.load(.monotonic));
    }
    try std.testing.expectEqualStrings("Hello", try client.queryWorkflow(CurrentGreeting, arena, bounded.id, "", .{}));
    // Calling again delivers it.
    try std.testing.expectEqualStrings("Hello", (try setGreetingWithin(&client, io, arena, bounded.id, .fromSeconds(30))).?);
    try std.testing.expectEqualStrings("Hey", try client.queryWorkflow(CurrentGreeting, arena, bounded.id, "", .{}));
    try client.signalWorkflow(Finish, bounded.id, "", .{});
    try std.testing.expectEqualStrings("Hey, Mo!", try bounded.get(arena));

    // The timeout fires after the server accepted the update: its handler waits for
    // `Proceed`. The workflow saw the update, and calling again with the same update ID
    // returns its result without running it twice.
    var slow = try client.executeWorkflow(SlowGreeterWorkflow, .{ .id = "greeter-slow", .task_queue = "greetings" }, .{"Mo"});
    defer slow.deinit();
    try std.testing.expectEqual(null, try setGreetingWithin(&client, io, arena, slow.id, .fromSeconds(5)));
    try std.testing.expectEqualStrings("Hey", try client.queryWorkflow(CurrentGreeting, arena, slow.id, "", .{}));
    try client.signalWorkflow(Proceed, slow.id, "", .{});
    try std.testing.expectEqualStrings("Hello", (try setGreetingWithin(&client, io, arena, slow.id, .fromSeconds(30))).?);
    try std.testing.expectEqual(1, try client.queryWorkflow(UpdateCount, arena, slow.id, "", .{}));
    try client.signalWorkflow(Finish, slow.id, "", .{});
    try std.testing.expectEqualStrings("Hey, Mo!", try slow.get(arena));

    try manageWorkflows(&client, arena);

    var greeter = try client.executeWorkflow(GreeterWorkflow, .{ .id = "greeter-reset", .task_queue = "greetings" }, .{"Ada"});
    defer greeter.deinit();
    try std.testing.expectEqualStrings("Hello", try client.queryWorkflow(CurrentGreeting, arena, greeter.id, "", .{}));
    try client.signalWorkflow(Finish, greeter.id, "", .{});
    try std.testing.expectEqualStrings("Hello, Ada!", try greeter.get(arena));
    var reset = try client.getWorkflow([]const u8, greeter.id, try resetGreeter(&client, arena, "greeter-reset"));
    defer reset.deinit();
    try std.testing.expectEqual(temporal.client.WorkflowExecutionStatus.completed, (try client.describeWorkflow(arena, greeter.id, greeter.run_id)).status);
    // Without the `finish` signal, the new run waits for one.
    try std.testing.expectEqual(temporal.client.WorkflowExecutionStatus.running, (try client.describeWorkflow(arena, reset.id, reset.run_id)).status);
    // The greetings worker is unversioned: an auto-upgrade override needs no versioned worker.
    try overrideVersioning(&client, arena, "greeter-reset");
    // The server records a change once any workflow task in progress has ended, so follow
    // the history until both are there.
    var changes: std.ArrayList(temporal.proto.temporal.api.history.v1.WorkflowExecutionOptionsUpdatedEventAttributes) = .empty;
    var events = try client.historyIterator(reset.id, reset.run_id, .{ .wait_new_event = true });
    defer events.deinit();
    while (changes.items.len < 2) {
        const event = (try events.next(arena)) orelse return error.TestUnexpectedResult;
        const attrs = event.attributes orelse continue;
        if (attrs == .workflow_execution_options_updated_event_attributes) try changes.append(arena, attrs.workflow_execution_options_updated_event_attributes);
    }
    const set = changes.items[0].versioning_override orelse return error.TestUnexpectedResult;
    try std.testing.expect((set.override orelse return error.TestUnexpectedResult) == .auto_upgrade);
    try std.testing.expect(changes.items[1].unset_versioning_override);
    var update = try client.updateWorkflow(SetGreeting, reset.id, reset.run_id, .{"Hi"}, .{});
    defer update.deinit();
    try std.testing.expectEqualStrings("Hello", try update.get(arena));
    try client.signalWorkflow(Finish, reset.id, reset.run_id, .{});
    try std.testing.expectEqualStrings("Hi, Ada!", try reset.get(arena));
    try scheduleGreetings(&client, arena);
    try std.testing.expectEqualStrings("Hello, Ada!", try runStandaloneActivity(&client, arena));
    try std.testing.expectEqualStrings("Hello, Grace!", try controlStandaloneActivity(&client, arena));

    var later = try client.executeWorkflow(GreetLaterWorkflow, .{ .task_queue = "greetings" }, .{"Lin"});
    defer later.deinit();
    try outbox.ready.wait(io);
    try finishGreeting(&client, outbox.task_token);
    try std.testing.expectEqualStrings("Hello, Lin!", try later.get(arena));
}
