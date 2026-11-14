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
}

test "client: examples against a dev server" {
    const gpa = std.testing.allocator;
    const io = std.testing.io;
    var server = try temporal.testing.DevServer.start(gpa, io, .{
        .extra_args = &temporal.testing.DevServer.standalone_activity_args,
    });
    defer server.stop(io);

    var client = try temporal.client.Client.connect(gpa, io, server.clientOptions());
    defer client.close();

    var outbox: Outbox = .{ .gpa = gpa };
    defer gpa.free(outbox.task_token);
    var w = try temporal.worker.Worker.init(gpa, io, &client, "greetings", .{});
    defer w.deinit();
    try w.registerWorkflow(Greeting);
    try w.registerWorkflow(GreeterWorkflow);
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

    var later = try client.executeWorkflow(GreetLaterWorkflow, .{ .task_queue = "greetings" }, .{"Lin"});
    defer later.deinit();
    try outbox.ready.wait(io);
    try finishGreeting(&client, outbox.task_token);
    try std.testing.expectEqualStrings("Hello, Lin!", try later.get(arena));
}
