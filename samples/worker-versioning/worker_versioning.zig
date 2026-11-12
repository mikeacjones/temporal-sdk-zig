//! Worker versioning: three builds (1.0, 1.1 and 2.0) of one Worker Deployment's workers,
//! and how their workflows move between them as each build becomes the deployment's current
//! version.
//!
//! Every build registers its code under the same two workflow types:
//!
//! - "AutoUpgradingWorkflow" is auto-upgrade: each run moves to the current version at its
//!   next workflow task, so a new build's code must replay the history the old build wrote.
//!   Build 1.1 changes it compatibly, guarding its new activity with `ctx.getVersion`.
//! - "PinnedWorkflow" is pinned: each run finishes on the build it started on, so build 1.1
//!   changes it incompatibly (a new activity before its first signal) without any guard.
//!
//! The workflow functions carry version suffixes only to show the same workflow's code
//! changing over time; clients start the types by their unversioned names.

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;
const activity = temporal.activity;
const Io = std.Io;
const Allocator = std.mem.Allocator;
const Time = temporal.converter.Time;

pub const task_queue = "worker-versioning-sample";
pub const deployment_name = "my-worker-deployment";

/// Drives the workflows: each signal makes an auto-upgrading run do an activity
/// ("do-activity") or finish (anything else), and makes a pinned run finish ("conclude").
pub const DoNextSignal = workflow.SignalDef("do-next-signal", fn (signal: []const u8) void);

/// The signals a run has received, taken in arrival order with `receive`.
const Signals = struct {
    ctx: *workflow.Context,
    received: std.ArrayList([]const u8) = .empty,
    next: usize = 0,

    fn listen(ctx: *workflow.Context) !*Signals {
        const s = try ctx.allocator().create(Signals);
        s.* = .{ .ctx = ctx };
        try ctx.setSignalHandler(DoNextSignal, s, doNext);
        return s;
    }
    fn doNext(s: *Signals, signal: []const u8) !void {
        try s.received.append(s.ctx.allocator(), signal);
    }
    fn pending(s: *Signals) bool {
        return s.next < s.received.items.len;
    }
    fn receive(s: *Signals) ![]const u8 {
        try s.ctx.await(s, pending);
        defer s.next += 1;
        return s.received.items[s.next];
    }
};

const activity_options: workflow.ActivityOptions = .{ .start_to_close_timeout = .fromSeconds(10) };

fn logStarted(ctx: *workflow.Context, comptime message: []const u8) void {
    var buf: [Time.max_len]u8 = undefined;
    ctx.logger().info(message, .{ .start_time = Time.fromTimestamp(ctx.now()).format(&buf) });
}

/// Build 1.0 of the auto-upgrading workflow: runs `SomeActivity` for each "do-activity"
/// signal until any other signal arrives.
fn autoUpgradingWorkflowV1(ctx: *workflow.Context) !void {
    const logger = ctx.logger();
    logStarted(ctx, "Changing workflow v1 started.");
    const signals = try Signals.listen(ctx);
    while (true) {
        if (!std.mem.eql(u8, try signals.receive(), "do-activity")) {
            logger.info("Concluding workflow v1", .{});
            return;
        }
        logger.info("Changing workflow v1 running activity", .{});
        _ = try ctx.executeActivity(SomeActivity, activity_options, .{"v1"}).get();
    }
}

/// Build 1.1 of the auto-upgrading workflow, changed compatibly: new log lines, and a new
/// activity behind `getVersion`. A run that reached this point on build 1.0 (or replays a
/// history that did) gets `default_version` and keeps calling `SomeActivity`; changing that
/// activity's input is compatible. Every other run records version 1 and calls
/// `SomeIncompatibleActivity` instead. The first call decides for the whole run.
fn autoUpgradingWorkflowV1b(ctx: *workflow.Context) !void {
    const logger = ctx.logger();
    logStarted(ctx, "Changing workflow v1b started.");
    const signals = try Signals.listen(ctx);
    while (true) {
        if (!std.mem.eql(u8, try signals.receive(), "do-activity")) {
            logger.info("Concluding workflow v1b", .{});
            return;
        }
        logger.info("Changing workflow v1b running activity", .{});
        if (try ctx.getVersion("DifferentActivity", workflow.default_version, 1) == workflow.default_version) {
            _ = try ctx.executeActivity(SomeActivity, activity_options, .{"v1b"}).get();
        } else {
            try ctx.executeActivity(SomeIncompatibleActivity, activity_options, .{.{ .called_by = "v1b", .more_data = "hello!" }}).get();
        }
    }
}

/// Build 1.0 of a short-lived workflow that stays on the build it started on: waits for
/// "conclude", then runs `SomeActivity`.
fn pinnedWorkflowV1(ctx: *workflow.Context) !void {
    logStarted(ctx, "Pinned Workflow v1 started.");
    const signals = try Signals.listen(ctx);
    while (!std.mem.eql(u8, try signals.receive(), "conclude")) {}
    _ = try ctx.executeActivity(SomeActivity, activity_options, .{"Pinned-v1"}).get();
}

/// Build 1.1 (and 2.0) of the pinned workflow, changed incompatibly with no guard: an
/// activity before waiting for "conclude", and a different activity type after it. Runs
/// started on build 1.0 never run this code.
fn pinnedWorkflowV2(ctx: *workflow.Context) !void {
    logStarted(ctx, "Pinned Workflow v2 started.");
    const signals = try Signals.listen(ctx);
    _ = try ctx.executeActivity(SomeActivity, activity_options, .{"Pinned-v2"}).get();
    while (!std.mem.eql(u8, try signals.receive(), "conclude")) {}
    try ctx.executeActivity(SomeIncompatibleActivity, activity_options, .{.{ .called_by = "Pinned-v2", .more_data = "hi" }}).get();
}

pub const AutoUpgradingWorkflowV1 = workflow.DefWithOptions("AutoUpgradingWorkflow", autoUpgradingWorkflowV1, .{ .versioning_behavior = .auto_upgrade });
pub const AutoUpgradingWorkflowV1b = workflow.DefWithOptions("AutoUpgradingWorkflow", autoUpgradingWorkflowV1b, .{ .versioning_behavior = .auto_upgrade });
pub const PinnedWorkflowV1 = workflow.DefWithOptions("PinnedWorkflow", pinnedWorkflowV1, .{ .versioning_behavior = .pinned });
pub const PinnedWorkflowV2 = workflow.DefWithOptions("PinnedWorkflow", pinnedWorkflowV2, .{ .versioning_behavior = .pinned });

fn someActivity(ctx: *activity.Context, called_by: []const u8) ![]const u8 {
    ctx.logger().info("SomeActivity executing", .{ .called_by = called_by });
    return called_by;
}

/// What `SomeActivity`'s single string parameter had to grow into. A struct parameter from
/// the start would have let it gain fields compatibly.
pub const IncompatibleActivityInput = struct {
    called_by: []const u8,
    more_data: []const u8,
};

fn someIncompatibleActivity(ctx: *activity.Context, input: IncompatibleActivityInput) !void {
    ctx.logger().info("SomeIncompatibleActivity executing", .{ .called_by = input.called_by, .more_data = input.more_data });
}

pub const SomeActivity = activity.Def("SomeActivity", someActivity);
pub const SomeIncompatibleActivity = activity.Def("SomeIncompatibleActivity", someIncompatibleActivity);

/// A build of the deployment's workers.
pub const Build = enum {
    v1,
    v1_1,
    v2,

    pub fn id(b: Build) []const u8 {
        return switch (b) {
            .v1 => "1.0",
            .v1_1 => "1.1",
            .v2 => "2.0",
        };
    }

    /// Options of a worker of this build: its version of the deployment, with versioning
    /// on, so the server routes workflows by version.
    pub fn workerOptions(b: Build) temporal.worker.Options {
        return .{ .deployment = .{ .deployment_name = deployment_name, .build_id = b.id() } };
    }

    /// Registers this build's code. Every build registers its workflows under the same
    /// types, as one workflow's code changing over time would.
    pub fn register(b: Build, w: *temporal.worker.Worker) !void {
        switch (b) {
            .v1 => {
                try w.registerWorkflow(AutoUpgradingWorkflowV1);
                try w.registerWorkflow(PinnedWorkflowV1);
            },
            .v1_1, .v2 => {
                try w.registerWorkflow(AutoUpgradingWorkflowV1b);
                try w.registerWorkflow(PinnedWorkflowV2);
            },
        }
        try w.registerActivity(SomeActivity);
        try w.registerActivity(SomeIncompatibleActivity);
    }
};

/// The main of the worker programs: runs a worker of `build` until SIGINT or SIGTERM.
pub fn runWorkerUntilSignal(init: std.process.Init, build: Build) !void {
    const gpa = init.gpa;
    const io = init.io;

    // The client and worker are heavyweight objects that should be created once per process.
    const options = try temporal.client.loadDefaultOptions(init.arena.allocator());
    var client = temporal.client.Client.connect(gpa, io, options) catch |err| {
        std.log.err("Unable to create client: {t}", .{err});
        std.process.exit(1);
    };
    defer client.close();

    var w = try temporal.worker.Worker.init(gpa, io, &client, task_queue, build.workerOptions());
    defer w.deinit();
    try build.register(&w);

    w.runUntilSignal() catch |err| {
        std.log.err("Unable to start worker: {t}", .{err});
        std.process.exit(1);
    };
}

/// A worker of one build, running concurrently with its caller until `stop`. It must not
/// move between `start` and `stop`.
pub const BuildWorker = struct {
    w: temporal.worker.Worker,
    running: Io.Future(temporal.worker.Worker.RunError!void),

    /// Starts the worker on `client`, which must outlive it, and returns once it polls.
    pub fn start(bw: *BuildWorker, gpa: Allocator, io: Io, client: *temporal.client.Client, build: Build) !void {
        bw.w = try .init(gpa, io, client, task_queue, build.workerOptions());
        errdefer bw.w.deinit();
        try build.register(&bw.w);
        bw.running = try io.concurrent(run, .{&bw.w});
        bw.w.waitUntilRunning() catch {
            const err = if (bw.running.await(io)) error.WorkerFailedToStart else |e| e;
            std.log.err("Unable to start the worker of build {s}: {t}", .{ build.id(), err });
            return err;
        };
    }

    /// Stops the worker after its in-flight work, and frees it.
    pub fn stop(bw: *BuildWorker) void {
        bw.w.stop();
        bw.running.await(bw.w.io) catch {};
        bw.w.deinit();
    }

    fn run(w: *temporal.worker.Worker) temporal.worker.Worker.RunError!void {
        return w.run();
    }
};

const poll_interval: Io.Duration = .fromMilliseconds(100);

/// Waits until workers of `build` poll both of the task queue's kinds of tasks, makes `build`
/// the deployment's current version, and waits until the routing change has reached the
/// task queue, so the next workflow task already goes where it says.
pub fn waitForWorkerAndMakeCurrent(gpa: Allocator, io: Io, client: *temporal.client.Client, build: Build) !void {
    var deployment = try client.workerDeployment(deployment_name);
    defer deployment.deinit();
    var arena: std.heap.ArenaAllocator = .init(gpa);
    defer arena.deinit();

    // The server refuses a current version whose workers don't poll every task queue the
    // previous one's did.
    while (true) : (try io.sleep(poll_interval, .awake)) {
        _ = arena.reset(.retain_capacity);
        const version = deployment.describeVersion(arena.allocator(), build.id()) catch |err| switch (err) {
            error.NotFound => continue,
            else => return err,
        };
        var workflows = false;
        var activities = false;
        for (version.task_queues) |q| {
            if (!std.mem.eql(u8, q.name, task_queue)) continue;
            workflows = workflows or q.type == .workflow;
            activities = activities or q.type == .activity;
        }
        if (workflows and activities) break;
    }

    _ = try deployment.setCurrentVersion(arena.allocator(), build.id(), .{});
    while ((try deployment.describe(arena.allocator())).routing_update == .in_progress) {
        _ = arena.reset(.retain_capacity);
        try io.sleep(poll_interval, .awake);
    }
}

/// A run of either workflow, as clients start them by type name.
pub const Run = temporal.client.WorkflowRun(void);

/// Signals both runs three times: the auto-upgrading run does an activity each time, and the
/// pinned run keeps waiting.
pub fn advanceWorkflows(client: *temporal.client.Client, auto_upgrading: *const Run, pinned: *const Run) !void {
    for (0..3) |_| {
        try client.signalWorkflow(DoNextSignal, auto_upgrading.id, auto_upgrading.run_id, .{"do-activity"});
        try client.signalWorkflow(DoNextSignal, pinned.id, pinned.run_id, .{"some-signal"});
    }
}

/// The build that ran a run's latest workflow task: for a pinned run, the build it is pinned
/// to. Null before a versioned worker has run one. Memory from `arena`.
pub fn runBuildId(client: *temporal.client.Client, arena: Allocator, workflow_id: []const u8, run_id: []const u8) !?[]const u8 {
    const d = try client.describeWorkflow(arena, workflow_id, run_id);
    const info = d.raw.workflow_execution_info orelse return null;
    const versioning = info.versioning_info orelse return null;
    const version = versioning.deployment_version orelse return null;
    return version.build_id;
}
