const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");

const Io = std.Io;
const activity = temporal.activity;
const workflow = temporal.workflow;
const NoopActivity = activity.Def("NoopActivity", noop);
const Workflow = workflow.Def("Workflow", runWorkflow);
const Run = temporal.client.WorkflowRun(void);
const workflow_count = 5;
const capability_name = "workerPollCompleteOnShutdown";

pub const feature = harness.define(.{
    .workflows = .{Workflow},
    .activities = .{NoopActivity},
    .worker_options = temporal.worker.Options{ .graceful_shutdown_period = .fromSeconds(10) },
    .execute = execute,
});

fn noop(_: *activity.Context) !void {}

fn runWorkflow(ctx: *workflow.Context) !void {
    while (true) {
        try ctx.sleep(.fromMilliseconds(20), .{});
        try ctx.executeActivity(NoopActivity, .{
            .schedule_to_close_timeout = .fromSeconds(10),
            .start_to_close_timeout = .fromSeconds(5),
            .retry_policy = .{ .maximum_attempts = 1 },
        }, .{}).get();
    }
}

fn execute(ctx: *harness.Context) !void {
    const complete_polls = ctx.capability(capability_name) orelse
        return ctx.fail("variant must declare {s}", .{capability_name});
    var run_buffer: [workflow_count]Run = undefined;
    var runs: std.ArrayList(Run) = .initBuffer(&run_buffer);
    defer for (runs.items) |*run| {
        ctx.client.terminateWorkflow(run.id, run.run_id, "feature cleanup") catch {};
        run.deinit();
    };
    for (0..workflow_count) |i| {
        var options = ctx.startOptions();
        options.id = try std.fmt.allocPrint(ctx.arena, "{s}-{d}", .{ ctx.task_queue, i });
        options.task_timeout = .fromSeconds(5);
        runs.appendAssumeCapacity(try ctx.client.executeWorkflow(Workflow, options, .{}));
    }
    for (runs.items) |*run| {
        try ctx.waitForEvent(run.id, run.run_id, .EVENT_TYPE_ACTIVITY_TASK_SCHEDULED, .fromSeconds(10));
    }

    const start = Io.Timestamp.now(ctx.io, .boot);
    try ctx.stopWorker();
    const elapsed = start.durationTo(Io.Timestamp.now(ctx.io, .boot));
    const shutdown_timeout: Io.Duration = .fromSeconds(5);
    try ctx.expect(elapsed.nanoseconds <= shutdown_timeout.nanoseconds, "worker shutdown took {f}, expected <= {f}", .{ elapsed, shutdown_timeout });

    if (complete_polls) {
        for (runs.items) |*run| {
            const recorded = try ctx.fetchHistory(run.id, run.run_id);
            try ctx.expect(!hasWorkflowTaskProblem(recorded), "unexpected workflow task failure or timeout in {s}", .{run.id});
        }
        return;
    }

    const history_timeout: Io.Duration = .fromSeconds(15);
    const deadline = Io.Timestamp.now(ctx.io, .boot).addDuration(history_timeout);
    var scratch: std.heap.ArenaAllocator = .init(ctx.gpa);
    defer scratch.deinit();
    while (true) {
        for (runs.items) |*run| {
            const recorded = try ctx.client.fetchHistory(scratch.allocator(), run.id, run.run_id);
            if (hasWorkflowTaskProblem(recorded)) return;
            _ = scratch.reset(.retain_capacity);
        }
        const remaining = Io.Timestamp.now(ctx.io, .boot).durationTo(deadline).nanoseconds;
        if (remaining <= 0) return ctx.fail("expected a workflow task failure or timeout within {f}", .{history_timeout});
        try ctx.io.sleep(.fromNanoseconds(@min(200 * std.time.ns_per_ms, remaining)), .boot);
    }
}

fn hasWorkflowTaskProblem(recorded: temporal.history.History) bool {
    return harness.history.contains(recorded, .EVENT_TYPE_WORKFLOW_TASK_FAILED) or
        harness.history.contains(recorded, .EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT);
}
