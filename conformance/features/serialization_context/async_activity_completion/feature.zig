const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");
const sercontext = harness.sercontext;
const activity = temporal.activity;
const workflow = temporal.workflow;

const Workflow = workflow.Def("Workflow", runWorkflow);
const Activity = activity.Def("Activity", runActivity);

const activity_result = "completed-out-of-band";
const heartbeat_data = "beat";
/// Bead of the SDK gap that breaks this feature: the client's by-ID activity calls give codecs
/// no workflow or activity type, while the workflow's worker decodes the result with both.
const gap = "temporal-sdk-zig-d51.4.31";

pub const feature = harness.define(.{
    .workflows = .{Workflow},
    .activities = .{Activity},
    .configureClient = sercontext.configureClient,
    .execute = execute,
});

fn runWorkflow(ctx: *workflow.Context) ![]const u8 {
    return ctx.executeActivity(Activity, .{
        .start_to_close_timeout = .fromSeconds(60),
        .heartbeat_timeout = .fromSeconds(30),
    }, .{}).get();
}

/// What the activity saw of itself, handed to the driver so the client can complete it. The
/// strings are copied into fixed buffers because the activity's memory dies with its task.
const Identity = struct {
    const Field = struct {
        buffer: [256]u8 = undefined,
        len: usize = 0,

        fn set(field: *Field, value: []const u8) bool {
            if (value.len > field.buffer.len) return false;
            @memcpy(field.buffer[0..value.len], value);
            field.len = value.len;
            return true;
        }

        fn copy(field: *const Field, arena: std.mem.Allocator) error{OutOfMemory}![]const u8 {
            return arena.dupe(u8, field.buffer[0..field.len]);
        }
    };

    mutex: std.atomic.Mutex = .unlocked,
    published: bool = false,
    workflow_id: Field = .{},
    run_id: Field = .{},
    workflow_type: Field = .{},
    activity_type: Field = .{},
    activity_id: Field = .{},

    fn lock(self: *Identity) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }
};

var identity: Identity = .{};

/// The published identity, copied into `arena`.
const Published = struct {
    workflow_id: []const u8,
    run_id: []const u8,
    workflow_type: []const u8,
    activity_type: []const u8,
    activity_id: []const u8,
};

fn runActivity(ctx: *activity.Context) ![]const u8 {
    const info = ctx.info();
    {
        identity.lock();
        defer identity.mutex.unlock();
        const fits = identity.workflow_id.set(info.workflow_id) and
            identity.run_id.set(info.workflow_run_id) and
            identity.workflow_type.set(info.workflow_type) and
            identity.activity_type.set(info.activity_type) and
            identity.activity_id.set(info.activity_id);
        if (!fits) return ctx.fail(.{ .message = "activity identity is longer than the driver's buffers", .type = "IdentityTooLong", .non_retryable = true });
        identity.published = true;
    }
    return ctx.completeAsync();
}

fn published(arena: std.mem.Allocator) error{OutOfMemory}!?Published {
    identity.lock();
    defer identity.mutex.unlock();
    if (!identity.published) return null;
    return .{
        .workflow_id = try identity.workflow_id.copy(arena),
        .run_id = try identity.run_id.copy(arena),
        .workflow_type = try identity.workflow_type.copy(arena),
        .activity_type = try identity.activity_type.copy(arena),
        .activity_id = try identity.activity_id.copy(arena),
    };
}

fn execute(ctx: *harness.Context) !void {
    var run = try ctx.client.executeWorkflow(Workflow, ctx.startOptions(), .{});
    defer run.deinit();

    const poll_interval: std.Io.Duration = .fromMilliseconds(100);
    const polls = 300; // 30 s
    const pending = for (0..polls) |_| {
        if (try published(ctx.arena)) |found| break found;
        try ctx.io.sleep(poll_interval, .boot);
    } else return ctx.fail("activity of workflow {s} did not publish its identity within 30s", .{run.id});

    const id: temporal.client.ActivityId = .{ .workflow_id = pending.workflow_id, .run_id = pending.run_id, .activity_id = pending.activity_id };
    _ = try ctx.client.recordActivityHeartbeatById(id, heartbeat_data);
    try ctx.client.completeActivityById(id, activity_result);

    // Checked before awaiting the result, which never comes when the workflow refuses it.
    try sercontext.waitForRawEvent(ctx, run.id, run.run_id, .EVENT_TYPE_ACTIVITY_TASK_COMPLETED, .fromSeconds(30));
    const recorded = try ctx.fetchRawHistory(run.id, run.run_id);
    const completed = try sercontext.findAttributes(ctx, recorded, .activity_task_completed_event_attributes);
    const expected = try sercontext.activitySignature(ctx.arena, ctx.namespace, pending.workflow_id, pending.workflow_type, pending.activity_type, pending.activity_id);
    const signature = sercontext.firstSignature(completed.result);
    if (signature == null or !std.mem.eql(u8, expected, signature.?)) return ctx.fail(
        "ActivityTaskCompleted result: expected context signature \"{s}\", got \"{s}\" (SDK gap: {s})",
        .{ expected, signature orelse "(none)", gap },
    );

    const result = run.get(ctx.arena) catch |err| {
        try sercontext.checkMismatch(ctx, "workflow result", gap);
        if (run.failure()) |failure| return ctx.fail("workflow failed: {s}", .{failure.message});
        return err;
    };
    try ctx.expectEqualStrings(activity_result, result);
}
