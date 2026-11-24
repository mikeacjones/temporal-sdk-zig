//! A client executes an activity on the feature's task queue without a workflow, waits for
//! its result and describes it as completed. The suite's config says `noWorkflow`, so the
//! feature declares only the activity, and it runs on a dev server of its own with
//! standalone activities turned on.
const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");

const activity = temporal.activity;

const Echo = activity.Def("Echo", echo);

pub const feature = harness.define(.{
    .activities = .{Echo},
    .dev_server_args = &temporal.testing.DevServer.standalone_activity_args,
    .execute = execute,
});

fn echo(_: *activity.Context, value: []const u8) ![]const u8 {
    return value;
}

fn execute(ctx: *harness.Context) !void {
    var handle = try ctx.client.executeActivity(Echo, .{
        .id = try temporal.client.uuid(ctx.arena, ctx.io),
        .task_queue = ctx.task_queue,
        .start_to_close_timeout = .fromSeconds(60),
    }, .{"hello"});
    defer handle.deinit();
    const result = handle.get(ctx.arena) catch |err| {
        if (handle.failure()) |failure| return ctx.fail("activity failed: {s}", .{failure.message});
        return err;
    };
    try ctx.expectEqualStrings("hello", result);

    const description = try handle.describe(ctx.arena);
    try ctx.expect(description.status == .completed, "expected activity status completed, got {t}", .{description.status});
}
