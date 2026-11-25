//! Runs the four workers of the sample in one process: for each of the caller pattern and the
//! on-demand pattern, a handler worker in namespace
//! nexus-messaging-temporal-operation-target-namespace and a caller worker in namespace
//! nexus-messaging-temporal-operation-caller-namespace, on one client per namespace. One
//! SIGINT or SIGTERM stops all four.
//!
//! Before the workers run, it starts the caller pattern's greeting workflow for the user
//! "default-user", which the caller pattern's caller workflow then messages. The start uses
//! the running workflow if there is one, so the worker can start again.
//!
//! The namespaces and the endpoints must exist on the server first. Against a local dev
//! server, started so that signals sent by Nexus handlers link back to their callers:
//!
//!     temporal server start-dev \
//!         --dynamic-config-value history.enableCHASMSignalBacklinks=true
//!     temporal operator namespace create \
//!         --namespace nexus-messaging-temporal-operation-target-namespace
//!     temporal operator namespace create \
//!         --namespace nexus-messaging-temporal-operation-caller-namespace
//!     temporal operator nexus endpoint create \
//!         --name nexus-messaging-temporal-operation-callerpattern-endpoint \
//!         --target-namespace nexus-messaging-temporal-operation-target-namespace \
//!         --target-task-queue nexus-messaging-temporal-operation-callerpattern-handler
//!     temporal operator nexus endpoint create \
//!         --name nexus-messaging-temporal-operation-ondemandpattern-endpoint \
//!         --target-namespace nexus-messaging-temporal-operation-target-namespace \
//!         --target-task-queue nexus-messaging-temporal-operation-ondemandpattern-handler
//!
//! The sample runs without the dynamic config value too; the signals then carry no links.
//!
//! Then run this worker and the starter. The programs pick their namespaces themselves; the
//! rest of the connection comes from the environment (TEMPORAL_ADDRESS) and the Temporal
//! config file.

const std = @import("std");
const temporal = @import("temporal");
const caller_handler = @import("callerpattern/handler.zig");
const caller_caller = @import("callerpattern/caller.zig");
const ondemand_handler = @import("ondemandpattern/handler.zig");
const ondemand_caller = @import("ondemandpattern/caller.zig");

const Worker = temporal.worker.Worker;

/// The user whose greeting workflow this program starts for the caller pattern.
pub const starter_user_id = "default-user";

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;
    const arena = init.arena.allocator();

    // A client belongs to one namespace; both patterns' handlers share one namespace, and
    // both callers the other.
    var handler_client = caller_handler.connect(gpa, io, arena) catch |err| {
        std.log.err("Unable to create client for namespace {s}: {t}", .{ caller_handler.namespace, err });
        std.process.exit(1);
    };
    defer handler_client.close();
    var caller_client = caller_caller.connect(gpa, io, arena) catch |err| {
        std.log.err("Unable to create client for namespace {s}: {t}", .{ caller_caller.namespace, err });
        std.process.exit(1);
    };
    defer caller_client.close();

    // The caller pattern messages a workflow that is already running: start it for the
    // default user, or keep the one that runs, and don't wait for it.
    const workflow_id = try caller_handler.workflowId(arena, starter_user_id);
    var greeting = handler_client.executeWorkflow(caller_handler.GreetingWorkflow, .{
        .id = workflow_id,
        .task_queue = caller_handler.task_queue,
        .id_conflict_policy = .use_existing,
    }, .{starter_user_id}) catch |err| {
        std.log.err("Unable to start workflow {s} in namespace {s}: {t}", .{ workflow_id, caller_handler.namespace, err });
        std.process.exit(1);
    };
    std.log.info("GreetingWorkflow started or already running WorkflowID={s} RunID={s}", .{ greeting.id, greeting.run_id });
    greeting.deinit();

    var caller_handler_worker = try Worker.init(gpa, io, &handler_client, caller_handler.task_queue, .{});
    defer caller_handler_worker.deinit();
    try caller_handler.register(&caller_handler_worker);

    var caller_caller_worker = try Worker.init(gpa, io, &caller_client, caller_caller.task_queue, .{});
    defer caller_caller_worker.deinit();
    try caller_caller.register(&caller_caller_worker);

    var ondemand_handler_worker = try Worker.init(gpa, io, &handler_client, ondemand_handler.task_queue, .{});
    defer ondemand_handler_worker.deinit();
    try ondemand_handler.register(&ondemand_handler_worker);

    var ondemand_caller_worker = try Worker.init(gpa, io, &caller_client, ondemand_caller.task_queue, .{});
    defer ondemand_caller_worker.deinit();
    try ondemand_caller.register(&ondemand_caller_worker);

    // Three workers run on tasks of their own and the fourth here; the signal stops all of
    // them, and the `await`s wait for the three.
    var caller_handler_run = try io.concurrent(runWorker, .{&caller_handler_worker});
    var caller_caller_run = try io.concurrent(runWorker, .{&caller_caller_worker});
    var ondemand_handler_run = try io.concurrent(runWorker, .{&ondemand_handler_worker});
    runWorker(&ondemand_caller_worker);
    ondemand_handler_run.await(io);
    caller_caller_run.await(io);
    caller_handler_run.await(io);
}

fn runWorker(w: *Worker) void {
    w.runUntilSignal() catch |err| {
        std.log.err("Worker for namespace {s}, task queue {s} failed: {t} {s}", .{ w.client.namespace, w.task_queue, err, w.fatalMessage() orelse "" });
        std.process.exit(1);
    };
}
