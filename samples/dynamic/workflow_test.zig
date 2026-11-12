//! Tests for the dynamic sample.

const std = @import("std");
const temporal = @import("temporal");
const dynamic = @import("dynamic.zig");

const testing = std.testing;

// Activities run on the worker's threads, so the call counts are atomic.
const Doubles = struct {
    var greeting_calls: std.atomic.Value(u32) = .init(0);
    var name_calls: std.atomic.Value(u32) = .init(0);
    var say_calls: std.atomic.Value(u32) = .init(0);

    fn getGreeting(ctx: *temporal.activity.Context) ![]const u8 {
        _ = ctx;
        _ = greeting_calls.fetchAdd(1, .monotonic);
        return "Greet";
    }
    fn getName(ctx: *temporal.activity.Context) ![]const u8 {
        _ = ctx;
        _ = name_calls.fetchAdd(1, .monotonic);
        return "Name";
    }
    fn sayGreeting(ctx: *temporal.activity.Context, greeting: []const u8, name: []const u8) ![]const u8 {
        _ = ctx;
        _ = say_calls.fetchAdd(1, .monotonic);
        try testing.expectEqualStrings("Greet", greeting);
        try testing.expectEqualStrings("Name", name);
        return "Greet Name";
    }
};

test "workflow calls each activity by name once" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(dynamic.SampleGreetingsWorkflow);
    try env.registerActivityAs(dynamic.GetGreeting, Doubles.getGreeting);
    try env.registerActivityAs(dynamic.GetName, Doubles.getName);
    try env.registerActivityAs(dynamic.SayGreeting, Doubles.sayGreeting);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    try env.executeWorkflow(dynamic.SampleGreetingsWorkflow, .{}, arena.allocator(), null);
    try testing.expectEqual(1, Doubles.greeting_calls.load(.monotonic));
    try testing.expectEqual(1, Doubles.name_calls.load(.monotonic));
    try testing.expectEqual(1, Doubles.say_calls.load(.monotonic));
}

test "activities return the greeting parts" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const env: temporal.testing.ActivityEnvironment = .init(testing.io);
    try testing.expectEqualStrings("Hello", try env.executeActivity(dynamic.GetGreeting, .{}, arena.allocator()));
    try testing.expectEqualStrings("Temporal", try env.executeActivity(dynamic.GetName, .{}, arena.allocator()));
    try testing.expectEqualStrings("Greeting: Hello Temporal!\n", try env.executeActivity(dynamic.SayGreeting, .{ "Hello", "Temporal" }, arena.allocator()));
}
