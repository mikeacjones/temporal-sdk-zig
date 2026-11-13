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

// example: testing-activity-environment
test "testing: an activity on its own" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const env: ActivityEnvironment = .init(testing.io);

    const result = try env.executeActivity(Greet, .{"Ziggy"}, arena.allocator());
    try testing.expectEqualStrings("Hello, Ziggy!", result);
    // A failure the activity describes with `ctx.fail` returns its error.
    try testing.expectError(error.ApplicationFailure, env.executeActivity(Greet, .{""}, arena.allocator()));
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
