//! Rolls the deployment out across builds 1.0, 1.1 and 2.0 while an auto-upgrading and a
//! pinned workflow run, then checks where each run finished: the pinned run on 1.0, where
//! it started, and the auto-upgrading run on 2.0, the last current version.
//!
//! This program also runs the worker of each build on its own client, starting each one
//! when the rollout reaches it; the workerv1, workerv1.1 and workerv2 programs run the same
//! workers by hand. Every worker keeps running until the end: the pinned run needs build 1.0
//! to finish.

const std = @import("std");
const temporal = @import("temporal");
const worker_versioning = @import("worker_versioning.zig");

const Build = worker_versioning.Build;

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;
    const arena = init.arena.allocator();

    // The client is a heavyweight object that should be created once per process.
    const options = try temporal.client.loadDefaultOptions(arena);
    var client = temporal.client.Client.connect(gpa, io, options) catch |err| {
        std.log.err("Unable to create client: {t}", .{err});
        std.process.exit(1);
    };
    defer client.close();

    // First, build 1.0 becomes the deployment's current version.
    var v1: worker_versioning.BuildWorker = undefined;
    try rollOut(&v1, gpa, io, &client, .v1);
    defer v1.stop();

    // Next, one auto-upgrading and one pinned workflow. The client starts them by their
    // unversioned type names: which code runs them is up to the deployment.
    var auto_upgrading = start(&client, arena, io, "AutoUpgradingWorkflow", "worker-versioning-versioning-autoupgrade");
    defer auto_upgrading.deinit();
    std.log.info("Started auto-upgrading workflow WorkflowID={s} RunID={s}", .{ auto_upgrading.id, auto_upgrading.run_id });
    var pinned = start(&client, arena, io, "PinnedWorkflow", "worker-versioning-versioning-pinned");
    defer pinned.deinit();
    std.log.info("Started pinned workflow WorkflowID={s} RunID={s}", .{ pinned.id, pinned.run_id });

    advance(&client, &auto_upgrading, &pinned);

    // Build 1.1 becomes current: the auto-upgrading run moves to it at its next workflow task,
    // while the pinned run stays on 1.0.
    var v1_1: worker_versioning.BuildWorker = undefined;
    try rollOut(&v1_1, gpa, io, &client, .v1_1);
    defer v1_1.stop();
    advance(&client, &auto_upgrading, &pinned);

    // Build 2.0 becomes current, and a new pinned workflow starts on it.
    var v2: worker_versioning.BuildWorker = undefined;
    try rollOut(&v2, gpa, io, &client, .v2);
    defer v2.stop();
    var pinned_2 = start(&client, arena, io, "PinnedWorkflow", "worker-versioning-versioning-pinned-2");
    defer pinned_2.deinit();
    std.log.info("Started pinned workflow v2 WorkflowID={s} RunID={s}", .{ pinned_2.id, pinned_2.run_id });

    const runs = [_]struct { name: []const u8, run: *worker_versioning.Run, want: Build }{
        .{ .name = "Auto-upgrading workflow", .run = &auto_upgrading, .want = .v2 },
        .{ .name = "Pinned workflow", .run = &pinned, .want = .v1 },
        .{ .name = "Second pinned workflow", .run = &pinned_2, .want = .v2 },
    };
    var moved_as_expected = true;
    for (runs) |r| {
        client.signalWorkflow(worker_versioning.DoNextSignal, r.run.id, r.run.run_id, .{"conclude"}) catch |err| {
            std.log.err("Unable to signal workflow {s}: {t}", .{ r.run.id, err });
            std.process.exit(1);
        };
        r.run.get(arena) catch |err| {
            std.log.err("Unable to get the result of workflow {s}: {t}", .{ r.run.id, err });
            std.process.exit(1);
        };
        const build_id = worker_versioning.runBuildId(&client, arena, r.run.id, r.run.run_id) catch |err| {
            std.log.err("Unable to describe workflow {s}: {t}", .{ r.run.id, err });
            std.process.exit(1);
        } orelse "none";
        std.log.info("{s} finished on build {s}", .{ r.name, build_id });
        if (!std.mem.eql(u8, build_id, r.want.id())) {
            std.log.err("{s} {s} should have finished on build {s}", .{ r.name, r.run.id, r.want.id() });
            moved_as_expected = false;
        }
    }
    if (!moved_as_expected) std.process.exit(1);
}

/// Starts the worker of `build` and makes `build` the deployment's current version.
fn rollOut(bw: *worker_versioning.BuildWorker, gpa: std.mem.Allocator, io: std.Io, client: *temporal.client.Client, build: Build) !void {
    std.log.info("Starting the worker of build {s}", .{build.id()});
    bw.start(gpa, io, client, build) catch std.process.exit(1);
    worker_versioning.waitForWorkerAndMakeCurrent(gpa, io, client, build) catch |err| {
        std.log.err("Unable to make build {s} the current version: {t}", .{ build.id(), err });
        std.process.exit(1);
    };
    std.log.info("Build {s} is the current version", .{build.id()});
}

fn start(client: *temporal.client.Client, arena: std.mem.Allocator, io: std.Io, workflow_type: []const u8, id_prefix: []const u8) worker_versioning.Run {
    const id = std.fmt.allocPrint(arena, "{s}_{s}", .{ id_prefix, temporal.client.uuid(arena, io) catch std.process.exit(1) }) catch std.process.exit(1);
    return client.executeWorkflowByName(void, workflow_type, .{ .id = id, .task_queue = worker_versioning.task_queue }, .{}) catch |err| {
        std.log.err("Unable to start workflow {s}: {t}", .{ workflow_type, err });
        std.process.exit(1);
    };
}

fn advance(client: *temporal.client.Client, auto_upgrading: *const worker_versioning.Run, pinned: *const worker_versioning.Run) void {
    worker_versioning.advanceWorkflows(client, auto_upgrading, pinned) catch |err| {
        std.log.err("Unable to signal workflow: {t}", .{err});
        std.process.exit(1);
    };
}
