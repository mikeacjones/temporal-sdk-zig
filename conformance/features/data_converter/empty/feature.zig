const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");
const activity = temporal.activity;
const workflow = temporal.workflow;
const converter = temporal.converter;
const Payload = converter.Payload;

const Workflow = workflow.Def("Workflow", runWorkflow);
const Activity = activity.Def("Activity", runActivity);

/// features/data_converter/empty/payload.json of temporalio/features at commit
/// a132d35815137c2e8fdc7767c52907c724f48c68, verbatim: neither the feature's process nor
/// `zig build test` can read the suite's directory.
const expected_payload_json =
    \\{ "metadata": { "encoding": "YmluYXJ5L251bGw=" } }
;

pub const feature = harness.define(.{
    .workflows = .{Workflow},
    .activities = .{Activity},
    .execute = execute,
});

fn runWorkflow(ctx: *workflow.Context) !void {
    return ctx.executeActivity(Activity, .{ .start_to_close_timeout = .fromSeconds(60) }, .{null}).get();
}

fn runActivity(ctx: *activity.Context, input: ?[]const u8) !void {
    if (input != null) return ctx.fail(.{ .message = "Activity input should be null", .type = "BadResult", .non_retryable = true });
}

fn execute(ctx: *harness.Context) !void {
    var run = try ctx.client.executeWorkflow(Workflow, ctx.startOptions(), .{});
    defer run.deinit();
    run.get(ctx.arena) catch |err| {
        if (run.failure()) |failure| return ctx.fail("workflow failed: {s}", .{failure.message});
        return err;
    };

    const recorded = try ctx.fetchRawHistory(run.id, run.run_id);
    const scheduled = harness.history.first(recorded, .EVENT_TYPE_ACTIVITY_TASK_SCHEDULED) orelse
        return ctx.fail("history has no EVENT_TYPE_ACTIVITY_TASK_SCHEDULED event", .{});
    const attributes = scheduled.attributes orelse return ctx.fail("ActivityTaskScheduled event has no attributes", .{});
    if (attributes != .activity_task_scheduled_event_attributes) return ctx.fail("ActivityTaskScheduled event has {t} attributes", .{attributes});
    const input = attributes.activity_task_scheduled_event_attributes.input orelse
        return ctx.fail("ActivityTaskScheduled event has no input", .{});
    try ctx.expect(input.payloads.items.len == 1, "ActivityTaskScheduled input has {d} payloads, expected 1", .{input.payloads.items.len});
    const actual = input.payloads.items[0];

    const expected = try temporal.protojson.decode(Payload, ctx.arena, expected_payload_json);
    try expectSamePayload(ctx, "activity input", expected, actual);
}

/// Payloads are equal when their data and their metadata entries are, in any order: the
/// server does not keep a payload's metadata in the order it was sent.
fn expectSamePayload(ctx: *harness.Context, what: []const u8, expected: converter.Payload, actual: converter.Payload) !void {
    try ctx.expect(std.mem.eql(u8, expected.data, actual.data), "{s}: data is {x}, expected {x}", .{ what, actual.data, expected.data });
    try ctx.expect(expected.metadata.items.len == actual.metadata.items.len, "{s}: {d} metadata entries, expected {d}", .{ what, actual.metadata.items.len, expected.metadata.items.len });
    for (expected.metadata.items) |entry| {
        const value = metadataValue(actual, entry.key) orelse return ctx.fail("{s}: no metadata entry \"{s}\"", .{ what, entry.key });
        try ctx.expect(std.mem.eql(u8, entry.value, value), "{s}: metadata \"{s}\" is \"{s}\", expected \"{s}\"", .{ what, entry.key, value, entry.value });
    }
}

fn metadataValue(payload: Payload, key: []const u8) ?[]const u8 {
    for (payload.metadata.items) |entry| {
        if (std.mem.eql(u8, entry.key, key)) return entry.value;
    }
    return null;
}
