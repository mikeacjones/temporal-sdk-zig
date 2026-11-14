//! Runs the handler worker (namespace my-target-namespace, task queue nexus-handler) and the
//! caller worker (namespace my-caller-namespace, task queue nexus-caller) in one process, each
//! on a client of its own namespace. One SIGINT or SIGTERM stops both. The programs
//! handler_worker and caller_worker run each on its own.
//!
//! The namespaces and the endpoint must exist on the server first. Against a local dev
//! server (`temporal server start-dev`):
//!
//!     temporal operator namespace create --namespace my-target-namespace
//!     temporal operator namespace create --namespace my-caller-namespace
//!     temporal operator nexus endpoint create --name nexus-endpoint \
//!         --target-namespace my-target-namespace --target-task-queue nexus-handler
//!
//! Then run this worker (or handler_worker and caller_worker), and the starter. The
//! programs pick their namespaces themselves; the rest of the connection comes from the
//! environment (TEMPORAL_ADDRESS) and the Temporal config file.

const std = @import("std");
const temporal = @import("temporal");
const handler = @import("handler.zig");
const caller = @import("caller.zig");

const Worker = temporal.worker.Worker;

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;
    const arena = init.arena.allocator();

    // A client belongs to one namespace, so each worker gets its own.
    var handler_client = handler.connect(gpa, io, arena) catch |err| {
        std.log.err("Unable to create client for namespace {s}: {t}", .{ handler.namespace, err });
        std.process.exit(1);
    };
    defer handler_client.close();
    var caller_client = caller.connect(gpa, io, arena) catch |err| {
        std.log.err("Unable to create client for namespace {s}: {t}", .{ caller.namespace, err });
        std.process.exit(1);
    };
    defer caller_client.close();

    var handler_worker = try Worker.init(gpa, io, &handler_client, handler.task_queue, .{});
    defer handler_worker.deinit();
    try handler.register(&handler_worker);

    var caller_worker = try Worker.init(gpa, io, &caller_client, caller.task_queue, .{});
    defer caller_worker.deinit();
    try caller.register(&caller_worker);

    // The handler worker runs on a task of its own and the caller worker here; the signal
    // stops both, and `await` waits for the handler worker.
    var handler_run = try io.concurrent(runWorker, .{&handler_worker});
    runWorker(&caller_worker);
    handler_run.await(io);
}

fn runWorker(w: *Worker) void {
    w.runUntilSignal() catch |err| {
        std.log.err("Worker for namespace {s}, task queue {s} failed: {t} {s}", .{ w.client.namespace, w.task_queue, err, w.fatalMessage() orelse "" });
        std.process.exit(1);
    };
}
