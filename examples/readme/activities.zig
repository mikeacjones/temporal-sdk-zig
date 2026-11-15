//! README.md examples. The code between `// example: <id>` and `// end-example` is copied into
//! README.md by `zig build readme`; edit it here, never in README.md.

const std = @import("std");
const temporal = @import("temporal");

const testing = std.testing;

// example: activity-definition
const activity = temporal.activity;

fn greet(ctx: *activity.Context, name: []const u8) ![]const u8 {
    return std.fmt.allocPrint(ctx.allocator(), "Hello, {s}!", .{name});
}
pub const Greet = activity.Def("greet", greet);

pub const Greeter = struct {
    punctuation: []const u8,
    pub fn greetLoudly(g: *Greeter, ctx: *activity.Context, name: []const u8) ![]const u8 {
        return std.fmt.allocPrint(ctx.allocator(), "HELLO, {s}{s}", .{ name, g.punctuation });
    }
};
pub const greeter = activity.defs(Greeter, .{ .prefix = "Greeter." }); // "Greeter.greetLoudly"

fn registerGreetings(w: *temporal.worker.Worker, shared: *Greeter) !void {
    try w.registerActivity(Greet);
    try w.registerActivities(shared, greeter);
}
// end-example

test "activities: a function activity and a method activity run in the activity environment" {
    _ = &registerGreetings;
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var env: temporal.testing.ActivityEnvironment = .init(testing.io);
    try testing.expectEqualStrings("Hello, Ana!", try env.executeActivity(Greet, .{"Ana"}, arena.allocator()));

    var shared: Greeter = .{ .punctuation = "!!" };
    try testing.expectEqualStrings("Greeter.greetLoudly", greeter.greetLoudly.activity_name);
    try testing.expectEqualStrings("HELLO, Ana!!", try env.executeActivityWith(greeter.greetLoudly, &shared, .{"Ana"}, arena.allocator()));
}

// example: activity-heartbeat
fn sendGreetings(ctx: *activity.Context, names: []const []const u8) !u32 {
    var sent: u32 = if (ctx.hasHeartbeatDetails()) try ctx.heartbeatDetails(u32) else 0;
    while (sent < names.len) : (sent += 1) {
        try ctx.io().checkCancel(); // error.Canceled after a cancel; ctx.cancelReason() says why
        ctx.logger().info("Hello!", .{ .name = names[sent] });
        ctx.heartbeat(sent + 1);
    }
    return sent;
}
pub const SendGreetings = activity.Def("send_greetings", sendGreetings);
// end-example

const HeartbeatRecorder = struct {
    last: u32 = 0,
    count: usize = 0,

    fn record(context: *anyopaque, details: []const temporal.converter.Payload) void {
        const r: *HeartbeatRecorder = @ptrCast(@alignCast(context));
        r.count += 1;
        r.last = std.fmt.parseInt(u32, details[0].data, 10) catch 0;
    }
};

test "activities: a heartbeating activity resumes from the previous attempt's details" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var recorder: HeartbeatRecorder = .{};
    var env: temporal.testing.ActivityEnvironment = .init(testing.io);
    env.heartbeat_details = &.{try temporal.converter.toPayload(arena.allocator(), @as(u32, 1))};
    env.on_heartbeat = .{ .gpa = testing.allocator, .context = &recorder, .record = HeartbeatRecorder.record };

    const names: []const []const u8 = &.{ "Ana", "Ben", "Cy" };
    try testing.expectEqual(@as(u32, 3), try env.executeActivity(SendGreetings, .{names}, arena.allocator()));
    try testing.expectEqual(@as(usize, 2), recorder.count);
    try testing.expectEqual(@as(u32, 3), recorder.last);
}

// example: activity-info
/// How much of the attempt's time is left, from the timeouts the workflow set.
fn timeLeft(ctx: *activity.Context) !std.Io.Duration {
    const info = ctx.info();
    ctx.logger().info("attempt", .{ .attempt = info.attempt, .max = if (info.retry_policy) |p| p.maximum_attempts else 0 });
    const deadline = info.deadline() orelse return error.NoTimeout; // none in this attempt's info
    return std.Io.Timestamp.now(ctx.io(), .real).durationTo(deadline);
}
pub const TimeLeft = activity.Def("time_left", timeLeft);
// end-example

test "activities: the deadline comes from the timeouts in the info" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var env: temporal.testing.ActivityEnvironment = .init(testing.io);
    try testing.expectError(error.NoTimeout, env.executeActivity(TimeLeft, .{}, arena.allocator()));
    env.info.start_to_close_timeout = .fromSeconds(60);
    const left = try env.executeActivity(TimeLeft, .{}, arena.allocator());
    try testing.expect(left.nanoseconds > 0 and left.nanoseconds <= 60 * std.time.ns_per_s);
}

// example: activity-worker-stop
/// Waits for new mail, standing in for real I/O with a sleep, or for the worker to stop.
fn waitForMail(ctx: *activity.Context) ![]const u8 {
    const Woken = union(enum) { stopping: error{Canceled}!void, mail: error{Canceled}!void };
    var buffer: [2]Woken = undefined;
    var select: std.Io.Select(Woken) = .init(ctx.io(), &buffer);
    defer select.cancelDiscard();
    // `concurrent`, not `async`: an `async` the Io can't give a thread runs at once, here.
    try select.concurrent(.stopping, activity.Context.waitForWorkerStop, .{ctx});
    try select.concurrent(.mail, std.Io.sleep, .{ ctx.io(), .fromSeconds(30), .awake });
    switch (try select.await()) {
        // Within the worker's graceful_shutdown_period: return, and let a retry carry on.
        .stopping => |stopped| {
            try stopped; // error.Canceled when the activity was canceled first
            return "worker stopping";
        },
        .mail => |slept| {
            try slept;
            return "mail";
        },
    }
}
pub const WaitForMail = activity.Def("wait_for_mail", waitForMail);

/// A loop can check the same signal between steps instead.
fn copyFiles(ctx: *activity.Context, count: u32) !u32 {
    var copied: u32 = if (ctx.hasHeartbeatDetails()) try ctx.heartbeatDetails(u32) else 0;
    while (copied < count and !ctx.isWorkerStopping()) : (copied += 1) {
        ctx.heartbeat(copied + 1);
    }
    return copied;
}
pub const CopyFiles = activity.Def("copy_files", copyFiles);
// end-example

fn runWaitForMail(env: *temporal.testing.ActivityEnvironment, arena: std.mem.Allocator) ![]const u8 {
    return env.executeActivity(WaitForMail, .{}, arena);
}

test "activities: an activity returns when its worker begins stopping" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var env: temporal.testing.ActivityEnvironment = .init(testing.io);
    try testing.expectEqual(@as(u32, 3), try env.executeActivity(CopyFiles, .{3}, arena.allocator()));

    var waiting = try testing.io.concurrent(runWaitForMail, .{ &env, arena.allocator() });
    env.stopWorker(); // before or after the activity starts waiting: both see it
    try testing.expectEqualStrings("worker stopping", try waiting.await(testing.io));
    try testing.expectEqual(@as(u32, 0), try env.executeActivity(CopyFiles, .{3}, arena.allocator()));
}

fn runWaitForMailThenSignal(env: *temporal.testing.ActivityEnvironment, arena: std.mem.Allocator, done: *std.Io.Event) ![]const u8 {
    defer done.set(env.io);
    return env.executeActivity(WaitForMail, .{}, arena);
}

test "activities: waitForMail returns promptly after its worker stops on an Io sized for one CPU" {
    // `std.Io.Threaded` lets `async` run as many tasks on threads of their own as there are
    // CPUs less one; past that, and so always on a one-CPU host, `async` runs the function at
    // once on the caller. A worker with a few activities and its poll loops reaches the limit
    // on any host.
    var threaded: std.Io.Threaded = .init(testing.allocator, .{ .async_limit = .limited(0) });
    defer threaded.deinit();
    const io = threaded.io();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var env: temporal.testing.ActivityEnvironment = .init(io);
    var done: std.Io.Event = .unset;

    var waiting = try io.concurrent(runWaitForMailThenSignal, .{ &env, arena.allocator(), &done });
    env.stopWorker();
    // The activity returns at once once its worker is stopping; it must not first sit out the
    // 30 s it gives the mail. The bound only keeps a failure from taking that long.
    done.waitTimeout(io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } }) catch {
        env.cancel(.canceled);
        _ = waiting.await(io) catch {};
        return error.ActivityKeptRunningAfterWorkerStop;
    };
    try testing.expectEqualStrings("worker stopping", try waiting.await(io));
}

// example: activity-async
pub const Approvals = struct {
    gpa: std.mem.Allocator, // thread-safe: activities run concurrently
    mutex: std.Io.Mutex = .init,
    tokens: std.ArrayList([]u8) = .empty,
    pub fn requestGreeting(a: *Approvals, ctx: *activity.Context, name: []const u8) ![]const u8 {
        ctx.logger().info("awaiting approval", .{ .name = name });
        {
            const token = try a.gpa.dupe(u8, ctx.info().task_token); // freed after return
            errdefer a.gpa.free(token);
            try a.mutex.lock(ctx.io());
            defer a.mutex.unlock(ctx.io());
            try a.tokens.append(a.gpa, token);
        }
        return ctx.completeAsync(); // an error: errdefers in scope here would run
    }
};
pub const RequestGreeting = activity.Def("request_greeting", Approvals.requestGreeting);

fn approve(client: *temporal.client.Client, token: []const u8) !void {
    try client.completeActivity(token, "Hello, Ana!");
}
// end-example

test "activities: an asynchronous activity hands off its task token" {
    _ = &approve;
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var approvals: Approvals = .{ .gpa = testing.allocator };
    defer {
        for (approvals.tokens.items) |t| testing.allocator.free(t);
        approvals.tokens.deinit(testing.allocator);
    }
    var env: temporal.testing.ActivityEnvironment = .init(testing.io);
    try testing.expectError(error.CompleteAsync, env.executeActivityWith(RequestGreeting, &approvals, .{"Ana"}, arena.allocator()));
    try testing.expectEqual(@as(usize, 1), approvals.tokens.items.len);
    try testing.expectEqualStrings("test-task-token", approvals.tokens.items[0]);
}

fn directoryReady(io: std.Io) bool {
    _ = io;
    return true;
}

// example: activity-failures
fn greetMember(ctx: *activity.Context, name: []const u8) ![]const u8 {
    if (name.len == 0) return ctx.fail(.{
        .message = "greet_member: the name is empty",
        .type = "InvalidName",
        .non_retryable = true,
    });
    if (!directoryReady(ctx.io())) return ctx.fail(.{
        .message = "greet_member: the member directory is still loading",
        .category = .benign, // expected: logged at debug level, not counted as an error
        .next_retry_delay = .fromSeconds(30), // instead of the retry policy's backoff
    });
    return std.fmt.allocPrint(ctx.allocator(), "Hello, {s}!", .{name});
}
pub const GreetMember = activity.Def("greet_member", greetMember);
// end-example

test "activities: an activity fails with an application failure" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var env: temporal.testing.ActivityEnvironment = .init(testing.io);
    try testing.expectError(error.ApplicationFailure, env.executeActivity(GreetMember, .{""}, arena.allocator()));
    try testing.expectEqualStrings("Hello, Ana!", try env.executeActivity(GreetMember, .{"Ana"}, arena.allocator()));
}

/// Runs every activity type the worker has no Def registered for.
fn anyGreeting(ctx: *activity.Context, args: temporal.converter.EncodedValues) ![]const u8 {
    const name = try args.get([]const u8, ctx.allocator(), 0);
    return std.fmt.allocPrint(ctx.allocator(), "{s}: Hello, {s}!", .{ ctx.info().activity_type, name });
}
pub const AnyGreeting = activity.DynamicDef(anyGreeting);

fn greetingByName(ctx: *temporal.workflow.Context, activity_type: []const u8) ![]const u8 {
    const options: temporal.workflow.ActivityOptions = .{ .start_to_close_timeout = .fromSeconds(10) };
    return ctx.executeActivityByName([]const u8, activity_type, options, .{"Ana"}).get();
}
pub const GreetingByName = temporal.workflow.Def("greeting_by_name", greetingByName);

test "activities: a dynamic activity runs an activity type with no Def" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(GreetingByName);
    try env.registerDynamicActivity(AnyGreeting);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const result = try env.executeWorkflow(GreetingByName, .{"greet_in_french"}, arena.allocator(), null);
    try testing.expectEqualStrings("greet_in_french: Hello, Ana!", result);
}
