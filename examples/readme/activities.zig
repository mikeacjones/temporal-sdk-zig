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
    const env: temporal.testing.ActivityEnvironment = .init(testing.io);
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
    const env: temporal.testing.ActivityEnvironment = .init(testing.io);
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
    const env: temporal.testing.ActivityEnvironment = .init(testing.io);
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
