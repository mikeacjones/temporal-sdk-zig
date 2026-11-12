//! A worker of build 1.0, the first version of the deployment, which samples-test runs
//! next to the starter. The starter runs workers of every build itself as well.

const std = @import("std");
const worker_versioning = @import("worker_versioning.zig");

pub fn main(init: std.process.Init) !void {
    return worker_versioning.runWorkerUntilSignal(init, .v1);
}
