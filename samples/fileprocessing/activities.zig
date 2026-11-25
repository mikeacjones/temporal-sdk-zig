//! Activities of the file processing sample. All three run in one session, so they run on one
//! host and share the files on its disk: the download saves a file, processing reads it and
//! saves an upper-cased copy, and the upload sends that copy on. Each removes its input file
//! when it is done with it. Failures are logged as warnings: the returned error reaches the
//! workflow, which decides what to do about it.

const std = @import("std");
const temporal = @import("temporal");
const activity = temporal.activity;

/// Stands in for remote file storage.
pub const BlobStore = struct {
    /// Returns some content for `file_id`, allocated from `arena`.
    pub fn downloadFile(b: *BlobStore, arena: std.mem.Allocator, file_id: []const u8) error{OutOfMemory}![]const u8 {
        _ = b;
        return std.fmt.allocPrint(arena, "dummy content for fileID:{s}", .{file_id});
    }

    /// Reads the file at `path` and takes five seconds to "upload" it, heartbeating each
    /// second with its progress.
    pub fn uploadFile(b: *BlobStore, ctx: *activity.Context, path: []const u8) !void {
        _ = b;
        _ = try std.Io.Dir.cwd().readFileAlloc(ctx.io(), path, ctx.allocator(), .limited(1 << 20));
        try slowWork(ctx);
    }
};

/// The activities' shared state: the blob store they download from and upload to. It must
/// outlive the worker.
pub const Activities = struct {
    blob_store: *BlobStore,

    /// Downloads `file_id` into a new file on this host and returns the file's path.
    pub fn downloadFileActivity(a: *Activities, ctx: *activity.Context, file_id: []const u8) ![]const u8 {
        const logger = ctx.logger();
        logger.info("Downloading file...", .{ .FileID = file_id });
        const data = try a.blob_store.downloadFile(ctx.allocator(), file_id);
        const path = saveToTmpFile(ctx, data) catch |err| {
            logger.warn("downloadFileActivity failed to save tmp file.", .{ .Error = err });
            return err;
        };
        logger.info("downloadFileActivity succeed.", .{ .SavedFilePath = path });
        return path;
    }

    /// Upper-cases the file at `file_name` into a new file, removes the input, and returns
    /// the new file's path.
    pub fn processFileActivity(a: *Activities, ctx: *activity.Context, file_name: []const u8) ![]const u8 {
        _ = a;
        const logger = ctx.logger();
        logger.info("processFileActivity started.", .{ .FileName = file_name });
        defer removeFile(ctx, file_name);

        const data = std.Io.Dir.cwd().readFileAlloc(ctx.io(), file_name, ctx.allocator(), .limited(1 << 20)) catch |err| {
            logger.warn("processFileActivity failed to read file.", .{ .FileName = file_name, .Error = err });
            return err;
        };
        const processed = try transcodeData(ctx, data);
        const path = saveToTmpFile(ctx, processed) catch |err| {
            logger.warn("processFileActivity failed to save tmp file.", .{ .Error = err });
            return err;
        };
        logger.info("processFileActivity succeed.", .{ .SavedFilePath = path });
        return path;
    }

    /// Uploads the file at `file_name` and removes it.
    pub fn uploadFileActivity(a: *Activities, ctx: *activity.Context, file_name: []const u8) !void {
        const logger = ctx.logger();
        logger.info("uploadFileActivity begin.", .{ .UploadedFileName = file_name });
        defer removeFile(ctx, file_name);

        a.blob_store.uploadFile(ctx, file_name) catch |err| {
            logger.warn("uploadFileActivity uploading failed.", .{ .Error = err });
            return err;
        };
        logger.info("uploadFileActivity succeed.", .{ .UploadedFileName = file_name });
    }
};

pub const DownloadFileActivity = activity.Def("DownloadFileActivity", Activities.downloadFileActivity);
pub const ProcessFileActivity = activity.Def("ProcessFileActivity", Activities.processFileActivity);
pub const UploadFileActivity = activity.Def("UploadFileActivity", Activities.uploadFileActivity);

/// The activities, for `Worker.registerActivitiesWith`.
pub const all = .{ DownloadFileActivity, ProcessFileActivity, UploadFileActivity };

/// Stands in for processing: takes five seconds, then upper-cases `data` into a new buffer.
/// A real processor would stream the file rather than hold all of it in memory.
fn transcodeData(ctx: *activity.Context, data: []const u8) ![]const u8 {
    try slowWork(ctx);
    return std.ascii.allocUpperString(ctx.allocator(), data);
}

/// Sleeps five times for one second, heartbeating after each with the step's number. The
/// short heartbeat timeout the workflow sets lets it notice a lost host within seconds;
/// the last details then come with the timeout's failure.
fn slowWork(ctx: *activity.Context) !void {
    for (0..5) |i| {
        try ctx.io().sleep(.fromSeconds(1), .awake);
        ctx.heartbeat(i);
    }
}

/// Writes `data` to a new file `/tmp/temporal_sample<uuid>` and returns its path. If the write
/// fails once the file exists, the file is removed.
fn saveToTmpFile(ctx: *activity.Context, data: []const u8) ![]const u8 {
    const a = ctx.allocator();
    const io = ctx.io();
    const path = try std.fmt.allocPrint(a, "/tmp/temporal_sample{s}", .{try temporal.client.uuid(a, io)});
    // Exclusive, so a file this activity didn't create is never written or removed.
    var file = try std.Io.Dir.cwd().createFile(io, path, .{ .exclusive = true });
    file.writeStreamingAll(io, data) catch |err| {
        file.close(io);
        removeFile(ctx, path);
        return err;
    };
    file.close(io);
    return path;
}

/// Removes the file at `path`, logging rather than failing when it can't. It runs on every
/// path out of an activity, cancellation included, so the activity's cancel waits until the
/// file is gone.
fn removeFile(ctx: *activity.Context, path: []const u8) void {
    const io = ctx.io();
    const protection = io.swapCancelProtection(.blocked);
    defer _ = io.swapCancelProtection(protection);
    std.Io.Dir.cwd().deleteFile(io, path) catch |err| switch (err) {
        error.FileNotFound => {},
        else => ctx.logger().warn("Unable to remove file", .{ .FileName = path, .Error = err }),
    };
}
