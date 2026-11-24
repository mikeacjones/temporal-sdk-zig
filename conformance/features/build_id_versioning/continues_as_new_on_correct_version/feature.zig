//! A versioned workflow that continues as new on its own task queue continues, by default, on
//! its compatible set, and on the queue's default set when it asks for the default version.
//! The queue has {1.0}; a 1.0 worker (the feature's) and a 2.0 worker run; the workflow
//! starts on 1.0; {2.0} becomes the default; signaled to continue on the same version, the
//! new run must run on 1.0; signaled to continue on the default version, the next run must
//! run on 2.0; a last signal ends it. Each run answers query "runningOn" with its worker's
//! build ID.
//!
//! `workflow.ContinueAsNewOptions` has no versioning intent, so the second continue cannot
//! ask for the default version: the new run stays on 1.0 and the feature fails, naming
//! `gap_bead`.
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
const Io = std.Io;
const workflow = temporal.workflow;

/// The SDK gap behind the second continue's check: no versioning intent on continue-as-new.
const gap_bead = "temporal-sdk-zig-d51.7.14";
/// The SDK gap that stops the 2.0 side worker: sdk-core refuses a second worker with another
/// legacy build ID on the same client and task queue.
const side_worker_bead = "temporal-sdk-zig-ck4";

const Workflow = workflow.Def("Workflow", runWorkflow1);
const WorkflowV2 = workflow.Def("Workflow", runWorkflow2);
const RunningOn = workflow.QueryDef("runningOn", fn () []const u8);
/// The suite's SignalDecision: 0 continues on the same version, 1 on the default version,
/// 2 ends the workflow.
const Unblocker = workflow.SignalDef("unblocker", fn (i64) void);
const continue_same = 0;
const continue_default = 1;
const end = 2;

pub const feature = harness.define(.{
    .workflows = .{Workflow},
    .worker_options = temporal.worker.Options{ .build_id = "1.0", .use_build_id_versioning = true },
    .dev_server_args = &harness.build_ids.dev_server_args,
    .execute = execute,
});

const State = struct {
    version: []const u8,
    decision: ?i64 = null,

    fn runningOn(state: *const State) []const u8 {
        return state.version;
    }

    fn unblock(state: *State, decision: i64) void {
        state.decision = decision;
    }

    fn decided(state: *State) bool {
        return state.decision != null;
    }
};

fn runWorkflow(ctx: *workflow.Context, version: []const u8) !void {
    const state = try ctx.allocator().create(State);
    state.* = .{ .version = version };
    try ctx.setQueryHandler(RunningOn, @as(*const State, state), State.runningOn);
    try ctx.setSignalHandler(Unblocker, state, State.unblock);
    try ctx.await(state, State.decided);
    if (state.decision.? == end) return;
    // The suite continues with VersioningIntentCompatible for continue_same and
    // VersioningIntentDefault for continue_default; ContinueAsNewOptions has no field for
    // either (gap_bead), so both get the core's default, compatible on the same queue.
    return ctx.continueAsNewByName("Workflow", .{}, .{});
}

fn runWorkflow1(ctx: *workflow.Context) !void {
    return runWorkflow(ctx, "1.0");
}

fn runWorkflow2(ctx: *workflow.Context) !void {
    return runWorkflow(ctx, "2.0");
}

fn execute(ctx: *harness.Context) !void {
    try harness.build_ids.addNewDefault(ctx, "1.0");
    const v2 = harness.SideWorker.start(ctx, .{ .build_id = "2.0", .use_build_id_versioning = true }, .{
        .workflows = .{WorkflowV2},
    }) catch |err| switch (err) {
        // sdk-core's refusal is the worker's init failing; any other failure is reported as is.
        error.FeatureFailed => if (std.mem.indexOf(u8, ctx.failure_message_.?, "error.WorkerInitFailed") != null)
            return ctx.fail("{s}: sdk-core refuses a second worker with another legacy build ID on the same client and task queue (gap: " ++ side_worker_bead ++ "); the feature also needs a versioning intent on continue-as-new options (gap: " ++ gap_bead ++ ")", .{ctx.failure_message_.?})
        else
            return error.FeatureFailed,
        error.Canceled => return error.Canceled,
    };
    defer v2.stop();

    var run = try ctx.client.executeWorkflow(Workflow, ctx.startOptions(), .{});
    defer run.deinit();
    // Ends the latest run on a failed check; harmless after the workflow ended.
    defer ctx.client.terminateWorkflow(run.id, "", "feature cleanup") catch {};
    // As the suite does, give the first workflow task time to be queued on 1.0.
    try ctx.io.sleep(.fromSeconds(1), .boot);
    try harness.build_ids.addNewDefault(ctx, "2.0");

    try waitForVersion(ctx, run.id, run.run_id, "1.0", "the first run");
    try ctx.client.signalWorkflow(Unblocker, run.id, run.run_id, .{continue_same});
    try waitForVersion(ctx, run.id, "", "1.0", "the run continued on the same version");
    try ctx.client.signalWorkflow(Unblocker, run.id, "", .{continue_default});
    waitForVersion(ctx, run.id, "", "2.0", "the run continued on the default version") catch |err| switch (err) {
        error.FeatureFailed => return ctx.fail("{s}: a workflow cannot continue as new on the default version, as workflow.ContinueAsNewOptions has no versioning intent (gap: " ++ gap_bead ++ ")", .{ctx.failure_message_.?}),
        else => return err,
    };
    try ctx.client.signalWorkflow(Unblocker, run.id, "", .{end});
    run.get(ctx.arena) catch |err| {
        const message = if (run.failure()) |failure| failure.message else "no failure details";
        return ctx.fail("workflow returned error.{t}: {s}", .{ err, message });
    };
}

/// Queries "runningOn" every 50 ms until it answers `expected`, for at most 5 s, as the
/// suite's QueryUntilEventually does. Each query has a 1 s deadline. An empty `run_id`
/// queries the latest run.
fn waitForVersion(ctx: *harness.Context, workflow_id: []const u8, run_id: []const u8, comptime expected: []const u8, comptime what: []const u8) !void {
    const io = ctx.io;
    const limit: Io.Duration = .fromSeconds(5);
    const deadline = Io.Clock.Timestamp.now(io, .boot).addDuration(.{ .raw = limit, .clock = .boot });
    var short = try ctx.client.withRpcOptions(.{ .timeout = .fromSeconds(1), .retry = false });
    var scratch: std.heap.ArenaAllocator = .init(ctx.gpa);
    defer scratch.deinit();
    var last: []const u8 = "no answer";
    var last_buf: [64]u8 = undefined;
    while (true) {
        _ = scratch.reset(.retain_capacity);
        if (short.queryWorkflow(RunningOn, scratch.allocator(), workflow_id, run_id, .{})) |version| {
            if (std.mem.eql(u8, version, expected)) return;
            last = std.fmt.bufPrint(&last_buf, "\"{s}\"", .{version[0..@min(version.len, 32)]}) catch "?";
        } else |err| switch (err) {
            error.Canceled => return error.Canceled,
            else => last = @errorName(err),
        }
        if (deadline.compare(.lte, Io.Clock.Timestamp.now(io, .boot)))
            return ctx.fail(what ++ " should run on version " ++ expected ++ ", but query \"runningOn\" did not answer \"" ++ expected ++ "\" within {f}; last: {s}", .{ limit, last });
        try io.sleep(.fromMilliseconds(50), .boot);
    }
}
