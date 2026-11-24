//! An activity or child workflow a versioned workflow starts on its own task queue runs, by
//! default, on the launching workflow's compatible set, and on the queue's default set when
//! it asks for the default version. The queue has {1.0}; a 1.0 worker (the feature's) and a
//! 2.0 worker run; the workflow starts on 1.0 and waits; {2.0} becomes the default; once
//! unblocked the workflow runs an activity and a child with default options (both must run
//! on 1.0), then an activity and a child asking for the default version (both on 2.0).
//!
//! `workflow.ChildWorkflowOptions` has no `versioning_intent` (only `ActivityOptions` has
//! one), so the last child cannot ask for the default version: it runs on 1.0 and the
//! feature fails, naming `gap_bead`.
//!
//! The 2.0 worker cannot start beside the 1.0 one today: sdk-core refuses a second worker on
//! the same client, namespace and task queue unless their Worker Deployment build IDs differ,
//! and a legacy build ID is not one, so `harness.SideWorker.start` fails with
//! error.WorkerInitFailed. A worker on a client of its own would not be refused, but the
//! harness has no server address to connect one to. That gap is `side_worker_bead`, and the
//! failure message names both beads.
const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");
const activity = temporal.activity;
const workflow = temporal.workflow;

/// The SDK gap behind the last child's check: no versioning intent on child workflow options.
const gap_bead = "temporal-sdk-zig-d51.7.14";
/// The SDK gap that stops the 2.0 side worker: sdk-core refuses a second worker with another
/// legacy build ID on the same client and task queue.
const side_worker_bead = "temporal-sdk-zig-ck4";

const Workflow = workflow.Def("Workflow", runWorkflow);
const RanByWf = workflow.Def("RanByWf", ranBy1Child);
const RanByWfV2 = workflow.Def("RanByWf", ranBy2Child);
const RanBy = activity.Def("RanBy", ranBy1);
const RanByV2 = activity.Def("RanBy", ranBy2);
const Unblocker = workflow.SignalDef("unblocker", fn () void);

/// What the workflow returns when every task ran on the version the README requires: the
/// worker that ran the default activity, the default child, the activity asking for the
/// default version, and the child asking for it.
const expected_result = "activity 1, child 1, default-version activity 2, default-version child 2";

pub const feature = harness.define(.{
    .workflows = .{ Workflow, RanByWf },
    .activities = .{RanBy},
    .worker_options = temporal.worker.Options{ .build_id = "1.0", .use_build_id_versioning = true },
    .dev_server_args = &harness.build_ids.dev_server_args,
    .execute = execute,
});

const State = struct {
    unblocked: bool = false,

    fn unblock(state: *State) void {
        state.unblocked = true;
    }

    fn isUnblocked(state: *State) bool {
        return state.unblocked;
    }
};

fn runWorkflow(ctx: *workflow.Context) ![]const u8 {
    const state = try ctx.allocator().create(State);
    state.* = .{};
    try ctx.setSignalHandler(Unblocker, state, State.unblock);
    try ctx.await(state, State.isUnblocked);

    const by_activity = try ctx.executeActivity(RanBy, .{ .schedule_to_close_timeout = .fromSeconds(5) }, .{}).get();
    const by_child = try ctx.executeChildWorkflow(RanByWf, .{ .execution_timeout = .fromSeconds(5) }, .{}).get();
    const by_default_activity = try ctx.executeActivity(RanBy, .{
        .schedule_to_close_timeout = .fromSeconds(5),
        .versioning_intent = .default,
    }, .{}).get();
    // The suite sets VersioningIntentDefault here; ChildWorkflowOptions has no field for it
    // (gap_bead), so the child gets the core's default, compatible on the same queue.
    const by_default_child = try ctx.executeChildWorkflow(RanByWf, .{ .execution_timeout = .fromSeconds(5) }, .{}).get();
    return std.fmt.allocPrint(ctx.allocator(), "activity {d}, child {d}, default-version activity {d}, default-version child {d}", .{
        by_activity, by_child, by_default_activity, by_default_child,
    });
}

fn ranBy1(_: *activity.Context) !i64 {
    return 1;
}

fn ranBy2(_: *activity.Context) !i64 {
    return 2;
}

fn ranBy1Child(_: *workflow.Context) !i64 {
    return 1;
}

fn ranBy2Child(_: *workflow.Context) !i64 {
    return 2;
}

fn execute(ctx: *harness.Context) !void {
    try harness.build_ids.addNewDefault(ctx, "1.0");
    const v2 = harness.SideWorker.start(ctx, .{ .build_id = "2.0", .use_build_id_versioning = true }, .{
        .workflows = .{ Workflow, RanByWfV2 },
        .activities = .{RanByV2},
    }) catch |err| switch (err) {
        // sdk-core's refusal is the worker's init failing; any other failure is reported as is.
        error.FeatureFailed => if (std.mem.indexOf(u8, ctx.failure_message_.?, "error.WorkerInitFailed") != null)
            return ctx.fail("{s}: sdk-core refuses a second worker with another legacy build ID on the same client and task queue (gap: " ++ side_worker_bead ++ "); the feature also needs a versioning intent on child workflow options (gap: " ++ gap_bead ++ ")", .{ctx.failure_message_.?})
        else
            return error.FeatureFailed,
        error.Canceled => return error.Canceled,
    };
    defer v2.stop();

    var run = try ctx.client.executeWorkflow(Workflow, ctx.startOptions(), .{});
    defer run.deinit();
    // Ends the run on a failed check before the workflow does; harmless after it completed.
    defer ctx.client.terminateWorkflow(run.id, run.run_id, "feature cleanup") catch {};
    // As the suite does, give the first workflow task time to be queued on 1.0.
    try ctx.io.sleep(.fromSeconds(1), .boot);
    try harness.build_ids.addNewDefault(ctx, "2.0");
    try ctx.client.signalWorkflow(Unblocker, run.id, run.run_id, .{});

    const result = run.get(ctx.arena) catch |err| {
        const message = if (run.failure()) |failure| failure.message else "no failure details";
        return ctx.fail("workflow returned error.{t}: {s}", .{ err, message });
    };
    if (std.mem.eql(u8, result, expected_result)) return;
    const gap_only = "activity 1, child 1, default-version activity 2, default-version child 1";
    if (std.mem.eql(u8, result, gap_only))
        return ctx.fail("expected \"{s}\", got \"{s}\": a child workflow cannot ask for the default version, as workflow.ChildWorkflowOptions has no versioning_intent (gap: " ++ gap_bead ++ ")", .{ expected_result, result });
    return ctx.fail("expected \"{s}\", got \"{s}\"", .{ expected_result, result });
}
