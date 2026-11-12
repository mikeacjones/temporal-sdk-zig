//! Tests for the worker-specific task queues sample. The file activities are test doubles
//! served by a second worker on their own task queue, as in the sample's worker.

const std = @import("std");
const temporal = @import("temporal");
const sample = @import("workflow.zig");
const activities = @import("activities.zig");

const testing = std.testing;
const Worker = temporal.worker.Worker;

const unique_task_queue = "unique-task-queue";

// Records the activities the workflow ran, in order. Activities run on the workers' threads.
const Recorder = struct {
    var mutex: std.Io.Mutex = .init;
    var calls: [32][]const u8 = undefined;
    var count: usize = 0;
    /// Calls of GetWorkerSpecificTaskQueue that fail before one succeeds.
    var failures_left: usize = 0;

    fn reset(failures: usize) void {
        count = 0;
        failures_left = failures;
    }

    fn recorded() []const []const u8 {
        return calls[0..count];
    }

    fn record(ctx: *temporal.activity.Context, name: []const u8) !void {
        try mutex.lock(ctx.io());
        defer mutex.unlock(ctx.io());
        if (count < calls.len) calls[count] = name;
        count += 1;
    }

    fn getWorkerSpecificTaskQueue(ctx: *temporal.activity.Context) ![]const u8 {
        try record(ctx, "GetWorkerSpecificTaskQueue");
        try mutex.lock(ctx.io());
        defer mutex.unlock(ctx.io());
        if (failures_left == 0) return unique_task_queue;
        failures_left -= 1;
        return ctx.fail(.{ .message = "temporary error" });
    }

    /// Checks that a file activity runs on the worker-specific task queue, with the path the
    /// workflow built for the attempt.
    fn fileActivity(ctx: *temporal.activity.Context, name: []const u8, path: []const u8) !void {
        try testing.expectEqualStrings(unique_task_queue, ctx.info().task_queue);
        try testing.expect(std.mem.startsWith(u8, path, "/tmp/"));
        try record(ctx, name);
    }

    fn downloadFile(ctx: *temporal.activity.Context, url: []const u8, path: []const u8) !void {
        try testing.expectEqualStrings("https://temporal.io", url);
        try fileActivity(ctx, "DownloadFile", path);
    }
    fn processFile(ctx: *temporal.activity.Context, path: []const u8) !void {
        try fileActivity(ctx, "ProcessFile", path);
    }
    fn processFileFails(ctx: *temporal.activity.Context, path: []const u8) !void {
        try fileActivity(ctx, "ProcessFile", path);
        return ctx.fail(.{ .message = "processing failed", .non_retryable = true });
    }
    fn deleteFile(ctx: *temporal.activity.Context, path: []const u8) !void {
        try fileActivity(ctx, "DeleteFile", path);
    }
};

/// The second worker of the sample's worker program, polling the worker-specific task queue.
const FileWorker = struct {
    worker: Worker,
    running: ?std.Io.Future(Worker.RunError!void) = null,

    fn init(env: *temporal.testing.WorkflowEnvironment, comptime process_file: anytype) !FileWorker {
        var w = try Worker.init(testing.allocator, testing.io, &env.client, unique_task_queue, .{});
        errdefer w.deinit();
        try w.registerActivityImpl(temporal.activity.Def(activities.DownloadFile.activity_name, Recorder.downloadFile).registered);
        try w.registerActivityImpl(temporal.activity.Def(activities.ProcessFile.activity_name, process_file).registered);
        try w.registerActivityImpl(temporal.activity.Def(activities.DeleteFile.activity_name, Recorder.deleteFile).registered);
        return .{ .worker = w };
    }

    fn start(fw: *FileWorker) !void {
        fw.running = try testing.io.concurrent(Worker.run, .{&fw.worker});
    }

    fn deinit(fw: *FileWorker) void {
        if (fw.running) |*r| {
            fw.worker.stop();
            r.await(testing.io) catch {};
        }
        fw.worker.deinit();
    }
};

fn startEnv() !*temporal.testing.WorkflowEnvironment {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    errdefer env.deinit();
    try env.registerWorkflow(sample.FileProcessingWorkflow);
    try env.registerActivityAs(activities.GetWorkerSpecificTaskQueue, Recorder.getWorkerSpecificTaskQueue);
    return env;
}

test "the file activities run on the worker-specific task queue" {
    Recorder.reset(0);
    const env = try startEnv();
    defer env.deinit();
    var files = try FileWorker.init(env, Recorder.processFile);
    defer files.deinit();
    try files.start();

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    try env.executeWorkflow(sample.FileProcessingWorkflow, .{}, arena.allocator(), null);
    try testing.expectEqualDeep(@as([]const []const u8, &.{
        "GetWorkerSpecificTaskQueue",
        "DownloadFile",
        "ProcessFile",
        "DeleteFile",
    }), Recorder.recorded());
}

test "a failed attempt is retried from the first activity" {
    Recorder.reset(2);
    const env = try startEnv();
    defer env.deinit();
    var files = try FileWorker.init(env, Recorder.processFile);
    defer files.deinit();
    try files.start();

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    try env.executeWorkflow(sample.FileProcessingWorkflow, .{}, arena.allocator(), null);
    try testing.expectEqualDeep(@as([]const []const u8, &.{
        "GetWorkerSpecificTaskQueue",
        "GetWorkerSpecificTaskQueue",
        "GetWorkerSpecificTaskQueue",
        "DownloadFile",
        "ProcessFile",
        "DeleteFile",
    }), Recorder.recorded());
}

test "the workflow fails after five failed attempts" {
    Recorder.reset(std.math.maxInt(usize));
    const env = try startEnv();
    defer env.deinit();
    var files = try FileWorker.init(env, Recorder.processFile);
    defer files.deinit();
    try files.start();

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var failure: ?temporal.failure.Failure = null;
    try testing.expectError(error.WorkflowFailed, env.executeWorkflow(sample.FileProcessingWorkflow, .{}, arena.allocator(), &failure));
    try testing.expectEqualStrings("temporary error", temporal.failure.rootCause(&failure.?).message);
    try testing.expectEqual(5, Recorder.recorded().len);
    for (Recorder.recorded()) |call| try testing.expectEqualStrings("GetWorkerSpecificTaskQueue", call);
}

test "the file is deleted when processing fails, and the attempt fails with the processing error" {
    Recorder.reset(0);
    const env = try startEnv();
    defer env.deinit();
    var files = try FileWorker.init(env, Recorder.processFileFails);
    defer files.deinit();
    try files.start();

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var failure: ?temporal.failure.Failure = null;
    try testing.expectError(error.WorkflowFailed, env.executeWorkflow(sample.FileProcessingWorkflow, .{}, arena.allocator(), &failure));
    try testing.expectEqualStrings("processing failed", temporal.failure.rootCause(&failure.?).message);
    const attempt: []const []const u8 = &.{ "GetWorkerSpecificTaskQueue", "DownloadFile", "ProcessFile", "DeleteFile" };
    try testing.expectEqual(5 * attempt.len, Recorder.recorded().len);
    for (Recorder.recorded(), 0..) |call, i| try testing.expectEqualStrings(attempt[i % attempt.len], call);
}

test "GetWorkerSpecificTaskQueue returns its worker's task queue" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var queue: activities.WorkerSpecificTaskQueue = .{ .task_queue = "worker-1" };
    const env: temporal.testing.ActivityEnvironment = .init(testing.io);
    try testing.expectEqualStrings("worker-1", try env.executeActivityWith(activities.GetWorkerSpecificTaskQueue, &queue, .{}, arena.allocator()));
}

test "ProcessFile fails when the file is not on this host" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const env: temporal.testing.ActivityEnvironment = .init(testing.io);
    try testing.expectError(error.FileNotFound, env.executeActivity(activities.ProcessFile, .{"/tmp/worker-specific-task-queues-missing-file"}, arena.allocator()));
}
