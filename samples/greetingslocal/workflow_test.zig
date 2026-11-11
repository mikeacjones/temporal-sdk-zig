//! Tests for the greetingslocal sample.

const std = @import("std");
const temporal = @import("temporal");
const greetingslocal = @import("greetingslocal.zig");

const testing = std.testing;

const Doubles = struct {
    fn getGreeting(ctx: *temporal.activity.Context) ![]const u8 {
        try testing.expect(ctx.info().is_local);
        return "Hello";
    }
    fn getName(ctx: *temporal.activity.Context) ![]const u8 {
        try testing.expect(ctx.info().is_local);
        return "World";
    }
    fn sayGreeting(ctx: *temporal.activity.Context, greeting: []const u8, name: []const u8) ![]const u8 {
        try testing.expect(ctx.info().is_local);
        try testing.expectEqualStrings("Hello", greeting);
        try testing.expectEqualStrings("World", name);
        return "Hello World!";
    }
};

test "workflow runs its activities locally with test doubles" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(greetingslocal.GreetingSample);
    try env.registerActivityAs(greetingslocal.GetGreeting, Doubles.getGreeting);
    try env.registerActivityAs(greetingslocal.GetName, Doubles.getName);
    try env.registerActivityAs(greetingslocal.SayGreeting, Doubles.sayGreeting);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const result = try env.executeWorkflow(greetingslocal.GreetingSample, .{}, arena.allocator(), null);
    try testing.expectEqualStrings("Hello World!", result);
}

test "workflow with the real activities" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    var activities: greetingslocal.Activities = .{ .name = "Temporal", .greeting = "Hello" };
    try env.registerWorkflow(greetingslocal.GreetingSample);
    try env.registerActivitiesWith(&activities, .{ greetingslocal.GetGreeting, greetingslocal.GetName, greetingslocal.SayGreeting });

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const result = try env.executeWorkflow(greetingslocal.GreetingSample, .{}, arena.allocator(), null);
    try testing.expectEqualStrings("Greeting: Hello Temporal!\n", result);
}

test "activities read their shared state" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var activities: greetingslocal.Activities = .{ .name = "Temporal", .greeting = "Hello" };
    const env: temporal.testing.ActivityEnvironment = .init(testing.io);
    try testing.expectEqualStrings("Hello", try env.executeActivityWith(greetingslocal.GetGreeting, &activities, .{}, arena.allocator()));
    try testing.expectEqualStrings("Temporal", try env.executeActivityWith(greetingslocal.GetName, &activities, .{}, arena.allocator()));
    try testing.expectEqualStrings("Greeting: Hello Temporal!\n", try env.executeActivityWith(greetingslocal.SayGreeting, &activities, .{ "Hello", "Temporal" }, arena.allocator()));
}
