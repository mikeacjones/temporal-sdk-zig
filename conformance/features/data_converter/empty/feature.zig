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
    try harness.payloads.expectSamePayload(ctx, "activity input", expected, actual);
}
