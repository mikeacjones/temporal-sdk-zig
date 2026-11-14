//! Tests of the handler workflow on its own, and of the ID the say-hello operation gives it.

const std = @import("std");
const temporal = @import("temporal");
const service = @import("service.zig");
const handler = @import("handler.zig");

const testing = std.testing;

test "the hello handler workflow greets in each language" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(handler.HelloHandlerWorkflow);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const cases = [_]struct { service.Language, []const u8 }{
        .{ .en, "Hello Temporal 👋" },
        .{ .fr, "Bonjour Temporal 👋" },
        .{ .de, "Hallo Temporal 👋" },
        .{ .es, "¡Hola! Temporal 👋" },
        .{ .tr, "Merhaba Temporal 👋" },
    };
    for (cases) |case| {
        const output = try env.executeWorkflow(handler.HelloHandlerWorkflow, .{.{ .name = "Temporal", .language = case[0] }}, arena.allocator(), null);
        try testing.expectEqualStrings(case[1], output.message);
    }
}

test "the say-hello workflow ID is the language and the trimmed, lowercased name with dashes for spaces" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    try testing.expectEqualStrings("hello-es-nexus", try service.helloWorkflowId(arena.allocator(), .{ .name = "Nexus", .language = .es }));
    try testing.expectEqualStrings("hello-tr-ada-lovelace", try service.helloWorkflowId(arena.allocator(), .{ .name = "  Ada Lovelace ", .language = .tr }));
}
