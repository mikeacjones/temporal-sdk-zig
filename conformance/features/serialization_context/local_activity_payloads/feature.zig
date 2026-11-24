const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");
const sercontext = harness.sercontext;
const activity = temporal.activity;
const workflow = temporal.workflow;

const Workflow = workflow.Def("Workflow", runWorkflow);
const LocalActivity = activity.Def("LocalActivity", runLocalActivity);

const workflow_input = "hello";

pub const feature = harness.define(.{
    .workflows = .{Workflow},
    .activities = .{LocalActivity},
    .configureClient = sercontext.configureClient,
    .execute = execute,
});

fn runWorkflow(ctx: *workflow.Context, input: []const u8) ![]const u8 {
    return ctx.executeLocalActivity(LocalActivity, .{ .start_to_close_timeout = .fromSeconds(10) }, .{input}).get();
}

fn runLocalActivity(ctx: *activity.Context, input: []const u8) ![]const u8 {
    return std.fmt.allocPrint(ctx.allocator(), "{s}|local", .{input});
}

/// The fields of core's local activity marker `data` payload this feature reads.
const MarkerData = struct { activity_id: []const u8, activity_type: []const u8 };

fn execute(ctx: *harness.Context) !void {
    var run = try ctx.client.executeWorkflow(Workflow, ctx.startOptions(), .{workflow_input});
    defer run.deinit();
    const result = run.get(ctx.arena) catch |err| {
        try sercontext.checkMismatch(ctx, "workflow result", null);
        if (run.failure()) |failure| return ctx.fail("workflow failed: {s}", .{failure.message});
        return err;
    };
    try ctx.expectEqualStrings(workflow_input ++ "|local", result);

    const recorded = try ctx.fetchRawHistory(run.id, run.run_id);
    const started = try sercontext.findAttributes(ctx, recorded, .workflow_execution_started_event_attributes);
    const marker = try sercontext.findAttributes(ctx, recorded, .marker_recorded_event_attributes);
    var data: ?temporal.converter.Payloads = null;
    var marker_result: ?temporal.converter.Payloads = null;
    for (marker.details.items) |detail| {
        if (std.mem.eql(u8, detail.key, "data")) data = detail.value;
        if (std.mem.eql(u8, detail.key, "result")) marker_result = detail.value;
    }
    const data_payloads = data orelse return ctx.fail("{s} marker has no data detail", .{marker.marker_name});
    if (data_payloads.payloads.items.len == 0) return ctx.fail("{s} marker data detail has no payload", .{marker.marker_name});
    const data_payload = data_payloads.payloads.items[0];

    // Core writes the marker's data payload itself, so no codec ever sees it.
    if (sercontext.signatureOf(data_payload)) |signed|
        return ctx.fail("{s} marker data: expected no context signature, got \"{s}\"", .{ marker.marker_name, signed });

    const parsed = std.json.parseFromSliceLeaky(MarkerData, ctx.arena, data_payload.data, .{ .ignore_unknown_fields = true }) catch |err|
        return ctx.fail("{s} marker data is not the JSON core writes ({t}): {s}", .{ marker.marker_name, err, data_payload.data });
    const workflow_type = if (started.workflow_type) |t| t.name else "";
    const expected = try sercontext.activitySignature(ctx.arena, ctx.namespace, run.id, workflow_type, parsed.activity_type, parsed.activity_id);
    try sercontext.expectSignature(ctx, "local activity marker result", expected, sercontext.firstSignature(marker_result));
}
