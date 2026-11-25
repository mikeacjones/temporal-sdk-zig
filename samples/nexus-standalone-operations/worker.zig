//! The handler worker: serves the nexus sample's `my-hello-service` (its echo and say-hello
//! operations) and runs `HelloHandlerWorkflow` behind say-hello, in namespace
//! my-standalone-operations-handler-namespace on task queue
//! nexus-standalone-operations-handler-queue.
//!
//! The namespaces and the endpoint must exist on the server first. Against a local dev
//! server (`temporal server start-dev`, from Temporal CLI 1.10.0, which serves standalone
//! Nexus operations):
//!
//!     temporal operator namespace create --namespace my-standalone-operations-handler-namespace
//!     temporal operator namespace create --namespace my-standalone-operations-caller-namespace
//!     temporal operator nexus endpoint create --name my-standalone-operations-endpoint \
//!         --target-namespace my-standalone-operations-handler-namespace \
//!         --target-task-queue nexus-standalone-operations-handler-queue
//!
//! Then run this worker and the starter. The programs pick their namespaces themselves; the
//! rest of the connection comes from the environment (TEMPORAL_ADDRESS) and the Temporal
//! config file.

const std = @import("std");
const temporal = @import("temporal");
const nexus = @import("nexus");
const names = @import("names.zig");

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;

    // The client and worker are heavyweight objects that should be created once per process.
    var options = try temporal.client.loadDefaultOptions(init.arena.allocator());
    options.namespace = names.handler_namespace;
    var client = temporal.client.Client.connect(gpa, io, options) catch |err| {
        std.log.err("Unable to create client for namespace {s}: {t}", .{ names.handler_namespace, err });
        std.process.exit(1);
    };
    defer client.close();

    var w = try temporal.worker.Worker.init(gpa, io, &client, names.task_queue, .{});
    defer w.deinit();
    // The nexus sample's service handler and the workflow its say-hello operation starts.
    try nexus.handler.register(&w);

    w.runUntilSignal() catch |err| {
        std.log.err("Unable to start worker: {t} {s}", .{ err, w.fatalMessage() orelse "" });
        std.process.exit(1);
    };
}
