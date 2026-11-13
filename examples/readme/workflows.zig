//! README.md examples. The code between `// example: <id>` and `// end-example` is copied into
//! README.md by `zig build readme`; edit it here, never in README.md.

const std = @import("std");
const temporal = @import("temporal");

const testing = std.testing;

// example: workflow-definition
const workflow = temporal.workflow;
const activity = temporal.activity;
fn greet(ctx: *activity.Context, name: []const u8) ![]const u8 {
    return std.fmt.allocPrint(ctx.allocator(), "Hello, {s}!", .{name});
}
pub const Greet = activity.Def("greet", greet);
const greet_options: workflow.ActivityOptions = .{ .start_to_close_timeout = .fromSeconds(10) };
fn greeting(ctx: *workflow.Context, name: []const u8) ![]const u8 {
    return ctx.executeActivity(Greet, greet_options, .{name}).get();
}
pub const Greeting = workflow.Def("greeting", greeting);

pub const Greeter = struct {
    greeted: usize = 0,
    pub fn run(g: *Greeter, ctx: *workflow.Context, name: []const u8) ![]const u8 {
        g.greeted += 1;
        return ctx.executeActivity(Greet, greet_options, .{name}).get();
    }
};
pub const GreeterWorkflow = workflow.DefWithOptions("greeter", Greeter, .{ .versioning_behavior = .auto_upgrade });
// end-example

test "workflows: function and struct definitions run" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(Greeting);
    try env.registerWorkflow(GreeterWorkflow);
    try env.registerActivity(Greet);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    try testing.expectEqualStrings("Hello, Ann!", try env.executeWorkflow(Greeting, .{"Ann"}, arena.allocator(), null));
    try testing.expectEqualStrings("Hello, Bo!", try env.executeWorkflow(GreeterWorkflow, .{"Bo"}, arena.allocator(), null));
}

// example: workflow-activities
fn greetTwice(ctx: *workflow.Context, name: []const u8) ![]const u8 {
    // Both start now; `get` waits for each result.
    const remote = ctx.executeActivity(Greet, .{
        .start_to_close_timeout = .fromSeconds(10),
        .retry_policy = .{ .maximum_attempts = 3 },
        .summary = "greet the caller", // shown on the scheduled event in the UI
        .priority = .{ .priority_key = 2, .fairness_key = "tenant-a" },
        .versioning_intent = .compatible, // with build-ID versioning: this workflow's build
    }, .{name});
    const local = ctx.executeLocalActivityByName([]const u8, "greet", .{
        .start_to_close_timeout = .fromSeconds(5),
        .summary = "greet a friend",
    }, .{"local friend"});
    return std.fmt.allocPrint(ctx.allocator(), "{s} {s}", .{ try remote.get(), try local.get() });
}
pub const GreetTwice = workflow.Def("greet_twice", greetTwice);
// end-example

test "workflows: an activity and a local activity" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(GreetTwice);
    try env.registerActivity(Greet);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const result = try env.executeWorkflow(GreetTwice, .{"Ann"}, arena.allocator(), null);
    try testing.expectEqualStrings("Hello, Ann! Hello, local friend!", result);
}

// example: workflow-timers
pub const Answer = workflow.SignalDef("answer", fn () void);
fn setTrue(b: *bool) void {
    b.* = true;
}
fn isTrue(b: *bool) bool {
    return b.*;
}
fn patientGreeting(ctx: *workflow.Context, name: []const u8) ![]const u8 {
    try ctx.io().sleep(.fromSeconds(60 * 60), .awake); // a durable timer
    const answered = try ctx.allocator().create(bool); // handler state outlives this call
    answered.* = false;
    try ctx.setSignalHandler(Answer, answered, setTrue);
    if (try ctx.awaitTimeout(.fromSeconds(24 * 60 * 60), answered, isTrue)) return name;
    return "no answer";
}
pub const PatientGreeting = workflow.Def("patient_greeting", patientGreeting);
// end-example

test "workflows: timers and awaitTimeout skip ahead on the time-skipping server" {
    const env = try temporal.testing.WorkflowEnvironment.startTimeSkipping(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(PatientGreeting);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const result = try env.executeWorkflow(PatientGreeting, .{"Ann"}, arena.allocator(), null);
    try testing.expectEqualStrings("no answer", result);
}

// example: workflow-select
const Outcome = union(enum) { greeting: workflow.ActivityError![]const u8, timeout: std.Io.Cancelable!void };

fn greetWithin(ctx: *workflow.Context, name: []const u8, patience: std.Io.Duration) ![]const u8 {
    var buffer: [2]Outcome = undefined;
    var select: std.Io.Select(Outcome) = .init(ctx.io(), &buffer);
    defer select.cancelDiscard(); // cancels the loser: the activity or the timer
    select.async(.greeting, workflow.ActivityFuture([]const u8).get, .{ctx.executeActivity(Greet, greet_options, .{name})});
    select.async(.timeout, std.Io.sleep, .{ ctx.io(), patience, .awake });
    return switch (try select.await()) {
        .greeting => |result| try result,
        .timeout => "Nobody answered.",
    };
}
pub const GreetWithin = workflow.Def("greet_within", greetWithin);
// end-example

test "workflows: Select returns whichever finishes first" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(GreetWithin);
    try env.registerActivity(Greet);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const result = try env.executeWorkflow(GreetWithin, .{ "Ann", .fromSeconds(60 * 60) }, arena.allocator(), null);
    try testing.expectEqualStrings("Hello, Ann!", result);
}

// example: workflow-group
fn greetInto(ctx: *workflow.Context, name: []const u8, out: *std.Io.Queue([]const u8)) void {
    const hello = ctx.executeActivity(Greet, greet_options, .{name}).get() catch "(no greeting)";
    out.putOne(ctx.io(), hello) catch {};
}
fn greetAll(ctx: *workflow.Context, names: []const []const u8) ![]const []const u8 {
    const io = ctx.io();
    const greetings = try ctx.allocator().alloc([]const u8, names.len);
    var buffer: [4][]const u8 = undefined;
    var queue: std.Io.Queue([]const u8) = .init(&buffer);
    var group: std.Io.Group = .init;
    defer group.cancel(io);
    for (names) |name| group.async(io, greetInto, .{ ctx, name, &queue });
    for (greetings) |*g| g.* = try queue.getOne(io); // in completion order
    return greetings;
}
pub const GreetAll = workflow.Def("greet_all", greetAll);
// end-example

test "workflows: a Group of coroutines feeds a Queue" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(GreetAll);
    try env.registerActivity(Greet);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const names: []const []const u8 = &.{ "Ann", "Bo", "Cy", "Di", "Ed", "Flo" };
    const result = try env.executeWorkflow(GreetAll, .{names}, arena.allocator(), null);
    try testing.expectEqual(names.len, result.len);
    for (names) |name| {
        const want = try std.fmt.allocPrint(arena.allocator(), "Hello, {s}!", .{name});
        for (result) |r| {
            if (std.mem.eql(u8, r, want)) break;
        } else return error.TestExpectedGreeting;
    }
}

// example: workflow-handlers
pub const AddGuest = workflow.SignalDef("add_guest", fn (name: []const u8) void);
pub const GuestCount = workflow.QueryDef("guest_count", fn () usize);
pub const Close = workflow.UpdateDef("close", fn (farewell: []const u8) usize);
pub const Party = struct {
    guests: usize = 0,
    closed: bool = false,
    pub const signals = .{ .addGuest = AddGuest };
    pub const queries = .{ .guestCount = GuestCount };
    pub const updates = .{ .close = .{ Close, .checkClose } };
    pub fn run(p: *Party, ctx: *workflow.Context) !usize {
        try ctx.await(p, isClosed);
        try ctx.await(ctx, workflow.Context.allHandlersFinished);
        return p.guests;
    }
    pub fn addGuest(p: *Party, _: []const u8) void {
        p.guests += 1;
    }
    pub fn guestCount(p: *Party) usize {
        return p.guests;
    }
    pub fn close(p: *Party, _: []const u8) usize {
        defer p.closed = true;
        return p.guests;
    }
    pub fn checkClose(_: *Party, farewell: []const u8) error{EmptyFarewell}!void {
        if (farewell.len == 0) return error.EmptyFarewell;
    }
    fn isClosed(p: *Party) bool {
        return p.closed;
    }
};
pub const PartyWorkflow = workflow.Def("party", Party);
// end-example

test "workflows: signal, query, and update handlers of a struct workflow" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(PartyWorkflow);
    try env.registerActivity(Greet);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var run = try env.startWorkflow(PartyWorkflow, .{}, .{});
    defer run.deinit();
    try env.signalWorkflow(AddGuest, run.id, .{"Ann"});
    try testing.expectError(error.UpdateFailed, env.updateWorkflow(Close, a, run.id, .{""}, null));
    try testing.expectEqual(1, try env.updateWorkflow(Close, a, run.id, .{"Goodbye"}, null));
    try testing.expectEqual(1, try env.getResult(PartyWorkflow, &run, a, null));
    try testing.expectEqual(1, try env.queryWorkflow(GuestCount, a, run.id, .{}));
}

// example: workflow-child
fn greetFamily(ctx: *workflow.Context, names: []const []const u8) ![]const []const u8 {
    const a = ctx.allocator();
    const children = try a.alloc(workflow.ChildWorkflowFuture([]const u8), names.len);
    for (names, children) |name, *child| {
        const id = try std.fmt.allocPrint(a, "{s}-{s}", .{ ctx.info().workflow_id, name });
        child.* = ctx.executeChildWorkflow(Greeting, .{ .id = id, .execution_timeout = .fromSeconds(60) }, .{name});
    }
    const greetings = try a.alloc([]const u8, names.len);
    for (children, greetings) |child, *out| out.* = try child.get();
    return greetings;
}
pub const GreetFamily = workflow.Def("greet_family", greetFamily);
// end-example

test "workflows: child workflows" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(GreetFamily);
    try env.registerWorkflow(Greeting);
    try env.registerActivity(Greet);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const result = try env.executeWorkflow(GreetFamily, .{&.{ "Ann", "Bo" }}, arena.allocator(), null);
    try testing.expectEqualStrings("Hello, Ann!", result[0]);
    try testing.expectEqualStrings("Hello, Bo!", result[1]);
}

// example: workflow-continue-as-new
fn greetInBatches(ctx: *workflow.Context, names: []const []const u8, greeted: u32) !u32 {
    const batch = names[0..@min(names.len, 2)];
    for (batch) |name| _ = try ctx.executeActivity(Greet, greet_options, .{name}).get();
    const total = greeted + @as(u32, @intCast(batch.len));
    if (batch.len < names.len) return ctx.continueAsNew(GreetInBatches, .{}, .{ names[batch.len..], total });
    return total;
}
pub const GreetInBatches = workflow.Def("greet_in_batches", greetInBatches);
// end-example

test "workflows: continue-as-new carries state to the next run" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(GreetInBatches);
    try env.registerActivity(Greet);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const total = try env.executeWorkflow(GreetInBatches, .{ &.{ "Ann", "Bo", "Cy", "Di", "Ed" }, 0 }, arena.allocator(), null);
    try testing.expectEqual(5, total);
}

// example: workflow-cancellation
fn greetUntilCanceled(ctx: *workflow.Context, name: []const u8) ![]const u8 {
    ctx.io().sleep(.fromSeconds(24 * 60 * 60), .awake) catch |err| switch (err) {
        error.Canceled => {
            _ = try ctx.executeActivity(Greet, greet_options, .{"cleanup"}).get();
            return error.Canceled; // the workflow ends canceled
        },
    };
    return ctx.executeActivity(Greet, greet_options, .{name}).get();
}
pub const GreetUntilCanceled = workflow.Def("greet_until_canceled", greetUntilCanceled);
// end-example

const CleanupRecorder = struct {
    var greeted: std.atomic.Value(bool) = .init(false);

    fn greet(ctx: *activity.Context, name: []const u8) ![]const u8 {
        if (std.mem.eql(u8, name, "cleanup")) greeted.store(true, .release);
        return Greet.call(null, ctx, .{name});
    }
};

test "workflows: a canceled workflow runs cleanup and ends canceled" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(GreetUntilCanceled);
    try env.registerActivityAs(Greet, CleanupRecorder.greet);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var run = try env.startWorkflow(GreetUntilCanceled, .{}, .{"Ann"});
    defer run.deinit();
    try env.client.cancelWorkflow(run.id, run.run_id, "party is over");
    try testing.expectError(error.WorkflowCanceled, env.getResult(GreetUntilCanceled, &run, arena.allocator(), null));
    try testing.expect(CleanupRecorder.greeted.load(.acquire));
}

// example: workflow-versioning
fn greetingV2(ctx: *workflow.Context, name: []const u8) ![]const u8 {
    const hello = if (ctx.patched("greet-with-activity"))
        try ctx.executeActivity(Greet, greet_options, .{name}).get()
    else
        try std.fmt.allocPrint(ctx.allocator(), "Hi, {s}.", .{name});
    const sign_off = switch (try ctx.getVersion("sign-off", workflow.default_version, 1)) {
        workflow.default_version => "",
        else => " Have a nice day.",
    };
    return std.fmt.allocPrint(ctx.allocator(), "{s}{s}", .{ hello, sign_off });
}
pub const GreetingV2 = workflow.Def("greeting_v2", greetingV2);
// end-example

test "workflows: a new run takes the patched path and the newest version" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(GreetingV2);
    try env.registerActivity(Greet);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const result = try env.executeWorkflow(GreetingV2, .{"Ann"}, arena.allocator(), null);
    try testing.expectEqualStrings("Hello, Ann! Have a nice day.", result);
}

// example: workflow-side-effects
const greeted_name = temporal.search_attributes.key(.keyword, "CustomKeywordField");
var tickets: std.atomic.Value(u64) = .init(0);
fn nextTicket(_: void) u64 {
    return tickets.fetchAdd(1, .monotonic) + 1;
}
fn trackedGreeting(ctx: *workflow.Context, name: []const u8) ![]const u8 {
    const ticket = try ctx.sideEffect(u64, {}, nextTicket); // replay reads it from history
    const hello = try ctx.executeActivity(Greet, greet_options, .{name}).get();
    try ctx.upsertMemo(.{ .last_greeting = hello });
    try ctx.upsertSearchAttributes(&.{greeted_name.set(name)});
    const language = try ctx.memoValue([]const u8, "language") orelse "en";
    return std.fmt.allocPrint(ctx.allocator(), "#{d} {s} ({s})", .{ ticket, hello, language });
}
pub const TrackedGreeting = workflow.Def("tracked_greeting", trackedGreeting);
// end-example

test "workflows: side effects, memo, and search attributes" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(TrackedGreeting);
    try env.registerActivity(Greet);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const memo = try temporal.converter.toFields(a, .{ .language = "fr" });
    const result = try env.executeWorkflowWithOptions(TrackedGreeting, .{ .memo = memo }, .{"Ann"}, a, null);
    const want = try std.fmt.allocPrint(a, "#{d} Hello, Ann! (fr)", .{tickets.load(.monotonic)});
    try testing.expectEqualStrings(want, result);
}

// example: workflow-session
fn greetOnOneHost(ctx: *workflow.Context, names: []const []const u8) ![]const u8 {
    const session = try ctx.createSession(.{
        .creation_timeout = .fromSeconds(60),
        .execution_timeout = .fromSeconds(10 * 60),
    });
    defer ctx.completeSession(session) catch {};
    const in_session: workflow.ActivityOptions = .{ .start_to_close_timeout = .fromSeconds(10), .session = session };
    for (names) |name| _ = try ctx.executeActivity(Greet, in_session, .{name}).get();
    return session.info().host_name;
}
pub const GreetOnOneHost = workflow.Def("greet_on_one_host", greetOnOneHost);
// end-example

test "workflows: a session runs its activities on one host" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    env.worker.options.sessions = .{};
    try env.registerWorkflow(GreetOnOneHost);
    try env.registerActivity(Greet);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const host = try env.executeWorkflow(GreetOnOneHost, .{&.{ "Ann", "Bo" }}, arena.allocator(), null);
    try testing.expect(host.len > 0);
}

// example: workflow-dynamic
fn anyGreeting(ctx: *workflow.Context, args: temporal.converter.EncodedValues) ![]const u8 {
    // Each workflow type may choose its own versioning behavior.
    const long_lived = std.mem.startsWith(u8, ctx.info().workflow_type, "long_");
    ctx.setVersioningBehavior(if (long_lived) .pinned else .auto_upgrade);
    const name = try args.get([]const u8, ctx.allocator(), 0);
    const hello = try ctx.executeActivity(Greet, greet_options, .{name}).get();
    return std.fmt.allocPrint(ctx.allocator(), "{s} ({s})", .{ hello, ctx.info().workflow_type });
}
pub const AnyGreeting = workflow.DynamicDef(anyGreeting);
// end-example

test "workflows: a dynamic workflow runs unregistered types" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerDynamicWorkflow(AnyGreeting);
    try env.registerActivity(Greet);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const result = try env.executeWorkflowByName([]const u8, "birthday_greeting", .{}, .{"Ann"}, arena.allocator(), null);
    try testing.expectEqualStrings("Hello, Ann! (birthday_greeting)", result);
}
