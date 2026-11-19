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

// example: workflow-uuid
fn greetChildById(ctx: *workflow.Context, name: []const u8) ![]const u8 {
    const id = try ctx.randomUuid(); // the same on replay
    const child = ctx.executeChildWorkflow(Greeting, .{ .id = id, .execution_timeout = .fromSeconds(60) }, .{name});
    return std.fmt.allocPrint(ctx.allocator(), "{s} ({s})", .{ try child.get(), id });
}
pub const GreetChildById = workflow.Def("greet_child_by_id", greetChildById);
// end-example

test "workflows: a UUID from the run's seeded randomness names a child" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(GreetChildById);
    try env.registerWorkflow(Greeting);
    try env.registerActivity(Greet);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var run = try env.startWorkflow(GreetChildById, .{}, .{"Ann"});
    defer run.deinit();
    const result = try env.getResult(GreetChildById, &run, arena.allocator(), null);
    try testing.expect(std.mem.startsWith(u8, result, "Hello, Ann! ("));
    try testing.expectEqual(@as(usize, "Hello, Ann! ()".len + 36), result.len);
    try env.replayRun(run.id, run.run_id);
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
    try ctx.sleep(.fromSeconds(60 * 60), .{ .summary = "give them an hour" }); // a durable timer
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

// example: workflow-event-value
const Reply = struct { value: []const u8 = "", ready: std.Io.Event = .unset };
fn fetchReply(ctx: *workflow.Context, name: []const u8, out: *Reply) void {
    ctx.setCoroutineName("fetch-reply") catch {}; // shown by stack-trace queries
    out.value = ctx.executeActivity(Greet, greet_options, .{name}).get() catch "(no greeting)";
    out.ready.set(ctx.io()); // after the value is written
}
fn greetWhenReady(ctx: *workflow.Context, name: []const u8) ![]const u8 {
    const io = ctx.io();
    var reply: Reply = .{};
    var group: std.Io.Group = .init;
    defer group.cancel(io);
    group.async(io, fetchReply, .{ ctx, name, &reply });
    try reply.ready.wait(io);
    return reply.value;
}
pub const GreetWhenReady = workflow.Def("greet_when_ready", greetWhenReady);
// end-example

test "workflows: one coroutine sets a value another waits for" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(GreetWhenReady);
    try env.registerActivity(Greet);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const result = try env.executeWorkflow(GreetWhenReady, .{"Ann"}, arena.allocator(), null);
    try testing.expectEqualStrings("Hello, Ann!", result);
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

// example: workflow-dynamic-query
const Lobby = struct {
    ctx: *workflow.Context,
    guests: usize = 0,
    fn addGuest(l: *Lobby, _: []const u8) void {
        l.guests += 1;
    }
    fn full(l: *Lobby) bool {
        return l.guests >= 2;
    }
    // Answers every query without a handler of its own; it may only read the lobby.
    fn anyQuery(l: *Lobby, name: []const u8, args: temporal.converter.EncodedValues) ![]const u8 {
        if (!std.mem.eql(u8, name, "welcome")) return error.UnknownQuery;
        const a = l.ctx.allocator();
        const guest = try args.get([]const u8, a, 0);
        return std.fmt.allocPrint(a, "Welcome, {s}! {d} here so far.", .{ guest, l.guests });
    }
};
fn lobby(ctx: *workflow.Context) !usize {
    const l = try ctx.allocator().create(Lobby); // outlives the workflow function
    l.* = .{ .ctx = ctx };
    try ctx.setSignalHandler(AddGuest, l, Lobby.addGuest);
    try ctx.setDynamicQueryHandler(l, Lobby.anyQuery);
    try ctx.await(l, Lobby.full);
    return l.guests;
}
pub const LobbyWorkflow = workflow.Def("lobby", lobby);
// end-example

test "workflows: a dynamic query handler answers queries without a handler of their own" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(LobbyWorkflow);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var run = try env.startWorkflow(LobbyWorkflow, .{}, .{});
    defer run.deinit();
    try env.signalWorkflow(AddGuest, run.id, .{"Ann"});
    const Welcome = workflow.QueryDef("welcome", fn (guest: []const u8) []const u8);
    try testing.expectEqualStrings("Welcome, Bob! 1 here so far.", try env.queryWorkflow(Welcome, a, run.id, .{"Bob"}));
    const Unknown = workflow.QueryDef("unknown", fn () []const u8);
    try testing.expectError(error.QueryFailed, env.queryWorkflow(Unknown, a, run.id, .{}));
    try env.signalWorkflow(AddGuest, run.id, .{"Bob"});
    try testing.expectEqual(2, try env.getResult(LobbyWorkflow, &run, a, null));
}

// example: workflow-dynamic-update
pub const CloseTally = workflow.SignalDef("close_tally", fn () void);
pub const Ping = workflow.SignalDef("ping", fn () void);
const Tally = struct {
    ctx: *workflow.Context,
    total: i64 = 0,
    closed: bool = false,
    // Takes every update without a handler of its own, in a coroutine of its own.
    fn anyUpdate(t: *Tally, name: []const u8, args: temporal.converter.EncodedValues) !i64 {
        const amount = try args.get(i64, t.ctx.allocator(), 0);
        t.total += if (std.mem.eql(u8, name, "subtract")) -amount else amount;
        return t.total;
    }
    // Runs before the update is accepted; its error rejects the update.
    fn checkUpdate(_: *Tally, name: []const u8, _: temporal.converter.EncodedValues) !void {
        if (!std.mem.eql(u8, name, "add") and !std.mem.eql(u8, name, "subtract")) return error.UnknownUpdate;
    }
    fn ping(t: *Tally) !void {
        try t.ctx.io().sleep(.fromSeconds(60), .awake);
    }
    fn close(t: *Tally) void {
        t.closed = true;
    }
    fn isClosed(t: *Tally) bool {
        return t.closed;
    }
};
fn tally(ctx: *workflow.Context) !i64 {
    const t = try ctx.allocator().create(Tally);
    t.* = .{ .ctx = ctx };
    try ctx.setDynamicUpdateHandlerWithValidator(t, Tally.anyUpdate, Tally.checkUpdate);
    try ctx.setSignalHandler(CloseTally, t, Tally.close);
    // A ping may still be sleeping when the tally closes; that is fine, so no warning.
    try ctx.setSignalHandlerWithOptions(Ping, t, Tally.ping, .{ .unfinished_policy = .abandon });
    try ctx.await(t, Tally.isClosed);
    return t.total;
}
pub const TallyWorkflow = workflow.Def("tally", tally);
// end-example

test "workflows: a dynamic update handler takes updates without a handler of their own" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(TallyWorkflow);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var run = try env.startWorkflow(TallyWorkflow, .{}, .{});
    defer run.deinit();
    try env.signalWorkflow(Ping, run.id, .{});
    const Add = workflow.UpdateDef("add", fn (amount: i64) i64);
    const Subtract = workflow.UpdateDef("subtract", fn (amount: i64) i64);
    const Multiply = workflow.UpdateDef("multiply", fn (factor: i64) i64);
    try testing.expectEqual(5, try env.updateWorkflow(Add, a, run.id, .{5}, null));
    try testing.expectEqual(3, try env.updateWorkflow(Subtract, a, run.id, .{2}, null));
    try testing.expectError(error.UpdateFailed, env.updateWorkflow(Multiply, a, run.id, .{4}, null));
    try env.signalWorkflow(CloseTally, run.id, .{});
    try testing.expectEqual(3, try env.getResult(TallyWorkflow, &run, a, null));
}

// example: workflow-signal-channel
const Arrival = union(enum) { guest: error{Canceled}![]const u8, quiet: std.Io.Cancelable!void };
fn greetArrivals(ctx: *workflow.Context) ![]const u8 {
    const io = ctx.io();
    const guests = try ctx.signalChannel(AddGuest); // takes the place of a handler
    var names: std.ArrayList([]const u8) = .empty;
    while (true) { // until a minute passes with no guest
        var buffer: [2]Arrival = undefined;
        var select: std.Io.Select(Arrival) = .init(io, &buffer);
        select.async(.guest, workflow.SignalChannel(AddGuest.Args).receive, .{guests});
        select.async(.quiet, std.Io.sleep, .{ io, .fromSeconds(60), .awake });
        const first = try select.await();
        select.cancelDiscard(); // one receive, awaited at once: no guest is dropped
        switch (first) {
            .guest => |name| try names.append(ctx.allocator(), try name),
            .quiet => break,
        }
    }
    ctx.removeSignalHandler(AddGuest); // later guests are buffered, unread
    const unread = try ctx.unhandledSignalNames(); // logged, and dropped, when the run ends
    if (unread.len > 0) ctx.logger().warn("signals left unread", .{ .first = unread[0] });
    return std.mem.join(ctx.allocator(), ", ", names.items);
}
pub const GreetArrivals = workflow.Def("greet_arrivals", greetArrivals);
// end-example

test "workflows: a signal channel receives guests until a quiet minute" {
    const env = try temporal.testing.WorkflowEnvironment.startTimeSkipping(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(GreetArrivals);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var run = try env.startWorkflow(GreetArrivals, .{}, .{});
    defer run.deinit();
    try env.signalWorkflow(AddGuest, run.id, .{"Ann"});
    try env.signalWorkflow(AddGuest, run.id, .{"Bob"});
    try testing.expectEqualStrings("Ann, Bob", try env.getResult(GreetArrivals, &run, a, null));
}

// example: workflow-child
fn greetFamily(ctx: *workflow.Context, names: []const []const u8) ![]const []const u8 {
    const a = ctx.allocator();
    const children = try a.alloc(workflow.ChildWorkflowFuture([]const u8), names.len);
    for (names, children) |name, *child| {
        const id = try std.fmt.allocPrint(a, "{s}-{s}", .{ ctx.info().workflow_id, name });
        child.* = ctx.executeChildWorkflow(Greeting, .{
            .id = id,
            .execution_timeout = .fromSeconds(60),
            // Shares dispatch fairly among family members; the priority key is the parent's.
            .priority = .{ .fairness_key = name },
            .cancellation_type = .wait_cancellation_requested,
        }, .{name});
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

// example: workflow-signal-with-start
/// Keeps a guest book open until `capacity` guests have signed it.
fn guestBook(ctx: *workflow.Context, capacity: u32) ![]const u8 {
    const guests = try ctx.signalChannel(AddGuest);
    var names: std.ArrayList([]const u8) = .empty;
    while (names.items.len < capacity) try names.append(ctx.allocator(), try guests.receive());
    return std.mem.join(ctx.allocator(), ", ", names.items);
}
pub const GuestBook = workflow.Def("guest_book", guestBook);

/// Signs `guest` into the book `book_id`, opening it first if it isn't open.
fn signIn(ctx: *workflow.Context, book_id: []const u8, guest: []const u8) !workflow.SignalWithStartResult {
    const sent = ctx.signalWithStartWorkflow(GuestBook, .{
        .id = book_id,
        .execution_timeout = .fromSeconds(3600),
        .id_conflict_policy = .use_existing, // only signal a book that is already open
    }, .{2}, AddGuest, .{guest});
    return sent.get() catch |err| {
        if (sent.failure()) |f| ctx.logger().warn("sign-in refused", .{ .reason = temporal.failure.rootCause(f).message });
        return err;
    };
}
pub const SignIn = workflow.Def("sign_in", signIn);
// end-example

test "workflows: signal-with-start opens a guest book once, then signs it" {
    const extra_args = [_][]const u8{ "--dynamic-config-value", "history.enableSignalWithStartFromWorkflow=true" };
    const env = try temporal.testing.WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{ .dev_server_extra_args = &extra_args });
    defer env.deinit();
    try env.registerWorkflow(GuestBook);
    try env.registerWorkflow(SignIn);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const book_id = try std.fmt.allocPrint(a, "guest-book-{s}", .{env.task_queue});
    const first = try env.executeWorkflow(SignIn, .{ book_id, "Ann" }, a, null);
    try testing.expect(first.started);
    try testing.expectEqualStrings(book_id, first.workflow_id);
    const second = try env.executeWorkflow(SignIn, .{ book_id, "Bob" }, a, null);
    try testing.expect(!second.started);
    try testing.expectEqualStrings(first.run_id, second.run_id);

    var book = try env.client.getWorkflow([]const u8, book_id, first.run_id);
    defer book.deinit();
    try testing.expectEqualStrings("Ann, Bob", try env.getResult(GuestBook, &book, a, null));
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

// example: workflow-continue-as-new-by-name
fn anyCountdown(ctx: *workflow.Context, args: temporal.converter.EncodedValues) ![]const u8 {
    const left = try args.get(u32, ctx.allocator(), 0);
    // Continues under whichever type this run was started as.
    if (left > 0) return ctx.continueAsNewByName(ctx.info().workflow_type, .{}, .{left - 1});
    return std.fmt.allocPrint(ctx.allocator(), "{s} done", .{ctx.info().workflow_type});
}
pub const AnyCountdown = workflow.DynamicDef(anyCountdown);
// end-example

test "workflows: a dynamic workflow continues as new under its own type" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerDynamicWorkflow(AnyCountdown);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const result = try env.executeWorkflowByName([]const u8, "launch_countdown", .{}, .{@as(u32, 2)}, arena.allocator(), null);
    try testing.expectEqualStrings("launch_countdown done", result);
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

// example: workflow-failure-policy
fn carrierRate(carrier: []const u8) ?u32 {
    if (std.mem.eql(u8, carrier, "post")) return 5;
    if (std.mem.eql(u8, carrier, "courier")) return 12;
    return null;
}
fn shipOrder(ctx: *workflow.Context, carrier: []const u8) ![]const u8 {
    // A carrier missing from the table is a bug to fix and deploy, not a reason to fail the
    // order: listed, the error fails the workflow task, which is retried until fixed code runs.
    const rate = carrierRate(carrier) orelse return error.UnknownCarrier;
    return std.fmt.allocPrint(ctx.allocator(), "shipped by {s} for {d}", .{ carrier, rate });
}
pub const ShipOrder = workflow.DefWithOptions("ship_order", shipOrder, .{
    .task_failure_errors = &.{error.UnknownCarrier},
    // Fail this workflow, not just its task, if its code stops matching its history.
    .nondeterminism_fails_workflow = true,
});

// For every workflow of a worker.
const strict_worker: temporal.worker.Options = .{
    .workflow_task_failure_errors = &.{error.InvariantBroken},
    .nondeterminism_fails_workflow = true,
};
// end-example

test "workflows: a failure policy lists errors that fail the workflow task" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    env.worker.options.workflow_task_failure_errors = strict_worker.workflow_task_failure_errors;
    env.worker.options.nondeterminism_fails_workflow = strict_worker.nondeterminism_fails_workflow;
    try env.registerWorkflow(ShipOrder);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var run = try env.startWorkflow(ShipOrder, .{}, .{"courier"});
    defer run.deinit();
    try testing.expectEqualStrings("shipped by courier for 12", try env.getResult(ShipOrder, &run, arena.allocator(), null));
}

// example: workflow-stack-trace
fn reminder(ctx: *workflow.Context, minutes: i64) ![]const u8 {
    try ctx.io().sleep(.fromSeconds(minutes * 60), .awake);
    return "reminded";
}
pub const Reminder = workflow.Def("reminder", reminder);

// From any client: the worker answers for every workflow, with no handler to register.
fn whereIsIt(client: *temporal.client.Client, arena: std.mem.Allocator, workflow_id: []const u8) ![]const u8 {
    return client.queryWorkflow(workflow.StackTraceQuery, arena, workflow_id, "", .{});
}
// end-example

test "workflows: the worker answers the stack trace query for every workflow" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(Reminder);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var run = try env.startWorkflow(Reminder, .{}, .{60});
    defer run.deinit();
    // The workflow's first task may not have run yet; ask until it waits.
    const answer = for (0..200) |_| {
        const text = try whereIsIt(&env.client, a, run.id);
        if (std.mem.find(u8, text, "waits on") != null) break text;
    } else return error.TestNeverWaited;
    try testing.expect(std.mem.startsWith(u8, answer, "workflow \"reminder\": 1 coroutine\n\ncoroutine 1 \"workflow\", the workflow function, waits on a timer (1h)\n  at "));
}
