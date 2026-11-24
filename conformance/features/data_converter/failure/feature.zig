const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");
const activity = temporal.activity;
const workflow = temporal.workflow;
const converter = temporal.converter;
const Failure = temporal.failure.Failure;

const Workflow = workflow.Def("Workflow", runWorkflow);
const Activity = activity.Def("Activity", runActivity);

const main_message = "main error";
const cause_message = "cause error";

pub const feature = harness.define(.{
    .workflows = .{Workflow},
    .activities = .{Activity},
    .configureClient = configureClient,
    .execute = execute,
});

fn configureClient(options: *temporal.client.Options) void {
    options.encode_failure_attributes = true;
}

// The workflow returns the activity's failure, so the run fails with it.
fn runWorkflow(ctx: *workflow.Context) !void {
    return ctx.executeActivity(Activity, .{
        .start_to_close_timeout = .fromSeconds(60),
        .retry_policy = .{ .maximum_attempts = 1 },
    }, .{}).get();
}

fn runActivity(ctx: *activity.Context) !void {
    const cause = try temporal.failure.application(ctx.allocator(), .{ .message = cause_message });
    return ctx.fail(.{ .message = main_message, .type = "customType", .non_retryable = true, .cause = &cause });
}

fn execute(ctx: *harness.Context) !void {
    var run = try ctx.client.executeWorkflow(Workflow, ctx.startOptions(), .{});
    defer run.deinit();
    if (run.get(ctx.arena)) |_| {
        return ctx.fail("workflow completed, expected it to fail with the activity's failure", .{});
    } else |err| switch (err) {
        error.WorkflowFailed => {},
        else => return err,
    }

    const recorded = try ctx.fetchRawHistory(run.id, run.run_id);
    const failed = harness.history.first(recorded, .EVENT_TYPE_ACTIVITY_TASK_FAILED) orelse
        return ctx.fail("history has no EVENT_TYPE_ACTIVITY_TASK_FAILED event", .{});
    const attributes = failed.attributes orelse return ctx.fail("ActivityTaskFailed event has no attributes", .{});
    if (attributes != .activity_task_failed_event_attributes) return ctx.fail("ActivityTaskFailed event has {t} attributes", .{attributes});
    const main = attributes.activity_task_failed_event_attributes.failure orelse
        return ctx.fail("ActivityTaskFailed event has no failure", .{});
    try expectEncoded(ctx, "main error", main, main_message);
    const cause = main.cause orelse return ctx.fail("ActivityTaskFailed failure has no cause", .{});
    try expectEncoded(ctx, "cause error", cause.*, cause_message);
}

const Attributes = struct {
    message: ?[]const u8 = null,
    stack_trace: ?[]const u8 = null,
};

/// The failure's message and stack trace are hidden in `encoded_attributes`.
fn expectEncoded(ctx: *harness.Context, what: []const u8, f: Failure, message: []const u8) !void {
    try ctx.expect(std.mem.eql(u8, temporal.failure.encoded_message, f.message), "{s}: message is \"{s}\", expected \"{s}\"", .{ what, f.message, temporal.failure.encoded_message });
    try ctx.expect(f.stack_trace.len == 0, "{s}: stack trace is \"{s}\", expected it empty", .{ what, f.stack_trace });
    const encoded = f.encoded_attributes orelse return ctx.fail("{s}: no encoded attributes", .{what});
    const encoding = converter.encodingOf(encoded) orelse "";
    try ctx.expect(std.mem.eql(u8, "json/plain", encoding), "{s}: encoded attributes encoding is \"{s}\", expected \"json/plain\"", .{ what, encoding });
    const decoded = std.json.parseFromSliceLeaky(Attributes, ctx.arena, encoded.data, .{ .ignore_unknown_fields = true }) catch |err|
        return ctx.fail("{s}: encoded attributes are not a JSON object ({t}): {s}", .{ what, err, encoded.data });
    const actual = decoded.message orelse return ctx.fail("{s}: encoded attributes have no message: {s}", .{ what, encoded.data });
    try ctx.expect(std.mem.eql(u8, message, actual), "{s}: encoded message is \"{s}\", expected \"{s}\"", .{ what, actual, message });
    try ctx.expect(decoded.stack_trace != null, "{s}: encoded attributes have no stack_trace: {s}", .{ what, encoded.data });
}
