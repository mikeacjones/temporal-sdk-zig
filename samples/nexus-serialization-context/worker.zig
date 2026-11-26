//! Runs the caller worker and both handler workers in one process, each on a client of its
//! own namespace with its own codec. One SIGINT or SIGTERM stops all three. The programs
//! caller_worker, compressed_encrypted_worker and encrypted_worker run each on its own.
//!
//! The asynchronous operations' results are encrypted with their endpoint's key only on a
//! server that keeps a Nexus caller's context with the workflow a handler starts; stable
//! Temporal CLI releases don't yet. Against a local dev server of the Temporal CLI
//! v1.9.3-nexus-serialization-context:
//!
//!     temporal server start-dev \
//!         --namespace nexus-serialization-caller \
//!         --namespace nexus-serialization-compressed-encrypted-handler \
//!         --namespace nexus-serialization-encrypted-handler
//!     temporal operator nexus endpoint create --name nexus-serialization-compressed-encrypted-1 \
//!         --target-namespace nexus-serialization-compressed-encrypted-handler \
//!         --target-task-queue nexus-serialization-compressed-encrypted-handler
//!     temporal operator nexus endpoint create --name nexus-serialization-compressed-encrypted-2 \
//!         --target-namespace nexus-serialization-compressed-encrypted-handler \
//!         --target-task-queue nexus-serialization-compressed-encrypted-handler
//!     temporal operator nexus endpoint create --name nexus-serialization-encrypted-1 \
//!         --target-namespace nexus-serialization-encrypted-handler \
//!         --target-task-queue nexus-serialization-encrypted-handler
//!     temporal operator nexus endpoint create --name nexus-serialization-encrypted-2 \
//!         --target-namespace nexus-serialization-encrypted-handler \
//!         --target-task-queue nexus-serialization-encrypted-handler
//!
//! Then run this worker (or the three single workers), and the starter. The programs pick
//! their namespaces themselves; the rest of the connection comes from the environment
//! (TEMPORAL_ADDRESS) and the Temporal config file.

const std = @import("std");
const temporal = @import("temporal");
const service = @import("service.zig");
const handler = @import("handler.zig");
const caller = @import("caller.zig");

const Worker = temporal.worker.Worker;

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;
    const arena = init.arena.allocator();

    // A client belongs to one namespace, so each worker gets its own, with its own codec.
    var compressed_client = connectHandler(gpa, io, arena, .compressed_encrypted);
    defer compressed_client.close();
    var encrypted_client = connectHandler(gpa, io, arena, .encrypted);
    defer encrypted_client.close();
    var caller_client = caller.connect(gpa, io, arena) catch |err| {
        std.log.err("Unable to create client for namespace {s}: {t}", .{ service.caller_namespace, err });
        std.process.exit(1);
    };
    defer caller_client.close();

    var compressed_worker = try Worker.init(gpa, io, &compressed_client, handler.Handler.compressed_encrypted.namespace(), .{});
    defer compressed_worker.deinit();
    try handler.register(&compressed_worker);

    var encrypted_worker = try Worker.init(gpa, io, &encrypted_client, handler.Handler.encrypted.namespace(), .{});
    defer encrypted_worker.deinit();
    try handler.register(&encrypted_worker);

    var caller_worker = try Worker.init(gpa, io, &caller_client, caller.task_queue, .{});
    defer caller_worker.deinit();
    try caller.register(&caller_worker);

    // The handler workers run on tasks of their own and the caller worker here; the signal
    // stops all three, and `await` waits for the handler workers.
    var compressed_run = try io.concurrent(runWorker, .{&compressed_worker});
    var encrypted_run = try io.concurrent(runWorker, .{&encrypted_worker});
    runWorker(&caller_worker);
    compressed_run.await(io);
    encrypted_run.await(io);
}

fn connectHandler(gpa: std.mem.Allocator, io: std.Io, arena: std.mem.Allocator, which: handler.Handler) temporal.client.Client {
    return handler.connect(gpa, io, arena, which) catch |err| {
        std.log.err("Unable to create client for namespace {s}: {t}", .{ which.namespace(), err });
        std.process.exit(1);
    };
}

fn runWorker(w: *Worker) void {
    w.runUntilSignal() catch |err| {
        std.log.err("Worker for namespace {s}, task queue {s} failed: {t} {s}", .{ w.client.namespace, w.task_queue, err, w.fatalMessage() orelse "" });
        std.process.exit(1);
    };
}
