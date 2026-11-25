//! Runs the handler worker: serves `NexusGreetingService` and runs `CreateGreetingActivity`,
//! in namespace nexus-standalone-activity-handler-namespace on task queue
//! nexus-standalone-activity-handler.
//!
//! The server must let a standalone activity complete a Nexus operation, and the namespaces
//! and the endpoint must exist first. Against a local dev server:
//!
//!     temporal server start-dev \
//!         --dynamic-config-value activity.enableStandalone=true \
//!         --dynamic-config-value history.enableChasm=true \
//!         --dynamic-config-value history.enableTransitionHistory=true \
//!         --dynamic-config-value activity.enableCallbacks=true \
//!         --namespace nexus-standalone-activity-handler-namespace \
//!         --namespace nexus-standalone-activity-caller-namespace
//!     temporal operator nexus endpoint create --name nexus-standalone-activity-endpoint \
//!         --target-namespace nexus-standalone-activity-handler-namespace \
//!         --target-task-queue nexus-standalone-activity-handler
//!
//! Then run this worker, and the starter. The programs pick their namespaces themselves; the
//! rest of the connection comes from the environment (TEMPORAL_ADDRESS) and the Temporal
//! config file.

const std = @import("std");
const temporal = @import("temporal");
const handler = @import("handler.zig");

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;

    // The client and worker are heavyweight objects that should be created once per process.
    var client = handler.connect(gpa, io, init.arena.allocator()) catch |err| {
        std.log.err("Unable to create client for namespace {s}: {t}", .{ handler.namespace, err });
        std.process.exit(1);
    };
    defer client.close();

    var w = try temporal.worker.Worker.init(gpa, io, &client, handler.task_queue, .{});
    defer w.deinit();
    try handler.register(&w);

    w.runUntilSignal() catch |err| {
        std.log.err("Unable to start worker: {t}", .{err});
        std.process.exit(1);
    };
}
