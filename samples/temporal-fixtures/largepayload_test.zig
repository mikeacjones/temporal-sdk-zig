//! Tests for the large payload fixture.

const std = @import("std");
const temporal = @import("temporal");
const largepayload = @import("largepayload.zig");

const testing = std.testing;

test "a payload of 1024 bytes completes" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(largepayload.LargePayloadWorkflow);
    try env.registerActivity(largepayload.CreateLargeResultActivity);
    try env.registerActivity(largepayload.ProcessLargeInputActivity);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    try env.executeWorkflow(largepayload.LargePayloadWorkflow, .{1024}, arena.allocator(), null);
}

test "the large result is random bytes of the size asked for" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var env: temporal.testing.ActivityEnvironment = .init(testing.io);
    const result = try env.executeActivity(largepayload.CreateLargeResultActivity, .{1024}, arena.allocator());
    try testing.expectEqual(1024, result.data.len);
}
