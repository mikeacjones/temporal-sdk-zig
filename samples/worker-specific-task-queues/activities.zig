//! Activities of the worker-specific task queues sample. `GetWorkerSpecificTaskQueue` runs on
//! the shared task queue and names the task queue only its worker polls; the file activities
//! run on that queue, so they all see the same local disk.

const std = @import("std");
const temporal = @import("temporal");
const activity = temporal.activity;

/// A worker's unique task queue, read by `GetWorkerSpecificTaskQueue`.
pub const WorkerSpecificTaskQueue = struct {
    task_queue: []const u8,

    pub fn getWorkerSpecificTaskQueue(q: *WorkerSpecificTaskQueue, ctx: *activity.Context) ![]const u8 {
        _ = ctx;
        return q.task_queue;
    }
};

pub const GetWorkerSpecificTaskQueue = activity.Def("GetWorkerSpecificTaskQueue", WorkerSpecificTaskQueue.getWorkerSpecificTaskQueue);

/// Each file activity is slowed down to stand in for real work.
const work_time: std.Io.Duration = .fromSeconds(3);

/// Stands in for a download: writes some test data to `path` on this host.
fn downloadFile(ctx: *activity.Context, url: []const u8, path: []const u8) !void {
    ctx.logger().info("Downloading file", .{ .url = url, .path = path });
    try ctx.io().sleep(work_time, .awake);
    try std.Io.Dir.cwd().writeFile(ctx.io(), .{ .sub_path = path, .data = "Hello, Temporal!" });
}

/// Stands in for processing the file: reads it and logs its checksum.
fn processFile(ctx: *activity.Context, path: []const u8) !void {
    const data = try std.Io.Dir.cwd().readFileAlloc(ctx.io(), path, ctx.allocator(), .limited(1 << 20));
    var checksum: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(data, &checksum, .{});
    ctx.logger().info("Did some work", .{ .path = path, .checksum = &std.fmt.bytesToHex(checksum, .lower) });
    try ctx.io().sleep(work_time, .awake);
}

/// Removes the file from this host.
fn deleteFile(ctx: *activity.Context, path: []const u8) !void {
    try ctx.io().sleep(work_time, .awake);
    ctx.logger().info("Removing file", .{ .path = path });
    try std.Io.Dir.cwd().deleteFile(ctx.io(), path);
}

pub const DownloadFile = activity.Def("DownloadFile", downloadFile);
pub const ProcessFile = activity.Def("ProcessFile", processFile);
pub const DeleteFile = activity.Def("DeleteFile", deleteFile);
