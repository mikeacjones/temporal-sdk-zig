//! Workflow context APIs tested end to end against a dev server and the workflow replayer.

const std = @import("std");
const workflow = @import("workflow.zig");
const activity = @import("activity.zig");
const testing_ns = @import("testing.zig");

const testing = std.testing;

const Echo = activity.Def("uuid_echo", struct {
    fn echo(_: *activity.Context, id: []const u8) ![]const u8 {
        return id;
    }
}.echo);

const Child = workflow.Def("uuid_child", struct {
    fn run(ctx: *workflow.Context) ![]const u8 {
        return ctx.info().workflow_id;
    }
}.run);

/// Draws two UUIDs, starts a child with the first as its ID and an activity with the second as
/// its argument, and returns "<child id> <activity arg> <child's answer> <activity's answer>".
const UuidWorkflow = workflow.Def("uuid_workflow", struct {
    fn run(ctx: *workflow.Context) ![]const u8 {
        const a = ctx.allocator();
        const child_id = try ctx.randomUuid();
        const arg = try ctx.randomUuid();
        if (std.mem.eql(u8, child_id, arg)) return error.SameUuidTwice;
        const child = try ctx.executeChildWorkflow(Child, .{ .id = child_id, .execution_timeout = .fromSeconds(60) }, .{}).get();
        const echoed = try ctx.executeActivity(Echo, .{ .start_to_close_timeout = .fromSeconds(10) }, .{arg}).get();
        return std.fmt.allocPrint(a, "{s} {s} {s} {s}", .{ child_id, arg, child, echoed });
    }
}.run);

fn expectUuid4(text: []const u8) !void {
    try testing.expectEqual(@as(usize, 36), text.len);
    for (text, 0..) |c, i| switch (i) {
        8, 13, 18, 23 => try testing.expectEqual(@as(u8, '-'), c),
        else => try testing.expect(std.ascii.isDigit(c) or (c >= 'a' and c <= 'f')),
    };
    try testing.expectEqual(@as(u8, '4'), text[14]);
    try testing.expect(std.mem.indexOfScalar(u8, "89ab", text[19]) != null);
}

test "workflow: randomUuid gives distinct v4 UUIDs that commands carry and a replay reproduces" {
    const env = try testing_ns.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(UuidWorkflow);
    try env.registerWorkflow(Child);
    try env.registerActivity(Echo);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var run = try env.startWorkflow(UuidWorkflow, .{}, .{});
    defer run.deinit();
    const result = try env.getResult(UuidWorkflow, &run, a, null);

    var parts = std.mem.splitScalar(u8, result, ' ');
    const child_id = parts.next().?;
    const arg = parts.next().?;
    try expectUuid4(child_id);
    try expectUuid4(arg);
    try testing.expect(!std.mem.eql(u8, child_id, arg));
    try testing.expectEqualStrings(child_id, parts.next().?);
    try testing.expectEqualStrings(arg, parts.next().?);

    const history = try env.client.fetchHistory(a, run.id, run.run_id);
    var replayer: testing_ns.WorkflowReplayer = .init(testing.allocator, testing.io);
    defer replayer.deinit();
    try replayer.registerWorkflow(UuidWorkflow);
    try replayer.replayHistory(history);
}

test "workflow: two runs draw different UUIDs" {
    const env = try testing_ns.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(UuidWorkflow);
    try env.registerWorkflow(Child);
    try env.registerActivity(Echo);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const first = try env.executeWorkflow(UuidWorkflow, .{}, a, null);
    const second = try env.executeWorkflow(UuidWorkflow, .{}, a, null);
    try testing.expect(!std.mem.eql(u8, first[0..36], second[0..36]));
}
