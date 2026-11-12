//! A worker of build 2.0 of the deployment, for running by hand next to the starter.

const std = @import("std");
const worker_versioning = @import("worker_versioning.zig");

pub fn main(init: std.process.Init) !void {
    return worker_versioning.runWorkerUntilSignal(init, .v2);
}
