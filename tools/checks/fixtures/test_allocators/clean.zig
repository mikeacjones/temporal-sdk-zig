const std = @import("std");

test "testing-backed allocators" {
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    var failing: std.testing.FailingAllocator = .init(std.testing.allocator, .{});
    _ = failing.allocator();
    _ = std.testing.failing_allocator;
}
