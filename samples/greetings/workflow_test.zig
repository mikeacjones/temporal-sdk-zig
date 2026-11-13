//! Tests for the greetings sample.

const std = @import("std");
const temporal = @import("temporal");
const greetings = @import("greetings.zig");

const testing = std.testing;

const Doubles = struct {
    fn getGreeting(ctx: *temporal.activity.Context) ![]const u8 {
        _ = ctx;
        return "Hello";
    }
    fn getName(ctx: *temporal.activity.Context) ![]const u8 {
        _ = ctx;
        return "World";
    }
    fn sayGreeting(ctx: *temporal.activity.Context, greeting: []const u8, name: []const u8) ![]const u8 {
        _ = ctx;
        try testing.expectEqualStrings("Hello", greeting);
        try testing.expectEqualStrings("World", name);
        return "Hello World!";
    }
};

test "workflow with test doubles for its activities" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(greetings.Greetings);
    try env.registerActivityAs(greetings.activities.getGreeting, Doubles.getGreeting);
    try env.registerActivityAs(greetings.activities.getName, Doubles.getName);
    try env.registerActivityAs(greetings.activities.sayGreeting, Doubles.sayGreeting);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const result = try env.executeWorkflow(greetings.Greetings, .{}, arena.allocator(), null);
    try testing.expectEqualStrings("Hello World!", result);
}

test "activities read their shared state" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var activities: greetings.Activities = .{ .name = "Temporal", .greeting = "Hello" };
    const env: temporal.testing.ActivityEnvironment = .init(testing.io);
    try testing.expectEqualStrings("Hello", try env.executeActivityWith(greetings.activities.getGreeting, &activities, .{}, arena.allocator()));
    try testing.expectEqualStrings("Greeting: Hello Temporal!\n", try env.executeActivityWith(greetings.activities.sayGreeting, &activities, .{ "Hello", "Temporal" }, arena.allocator()));
}
