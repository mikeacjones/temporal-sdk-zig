//! Tests for the file processing sample. The three activities are test doubles that check
//! their inputs and record the session each ran in; the environment's worker is a session
//! host.

const std = @import("std");
const temporal = @import("temporal");
const sample = @import("workflow.zig");
const activities = @import("activities.zig");

const testing = std.testing;
const activity = temporal.activity;

// Records the activities the workflow ran, in order, with their session IDs. Activities run
// on the worker's threads.
const Recorder = struct {
    const Call = struct { name: []const u8, session_id: [64]u8 = undefined, session_id_len: usize = 0 };

    var mutex: std.Io.Mutex = .init;
    var calls: [32]Call = undefined;
    var count: usize = 0;
    /// Calls of ProcessFileActivity that fail before one succeeds.
    var process_failures_left: usize = 0;

    fn reset(process_failures: usize) void {
        count = 0;
        process_failures_left = process_failures;
    }

    fn recorded() []const Call {
        return calls[0..@min(count, calls.len)];
    }

    fn sessionId(call: *const Call) []const u8 {
        return call.session_id[0..call.session_id_len];
    }

    fn record(ctx: *activity.Context, name: []const u8) !void {
        const session_id = ctx.info().session_id;
        try testing.expect(session_id.len > 0 and session_id.len <= 64);
        try mutex.lock(ctx.io());
        defer mutex.unlock(ctx.io());
        if (count < calls.len) {
            calls[count] = .{ .name = name, .session_id_len = session_id.len };
            @memcpy(calls[count].session_id[0..session_id.len], session_id);
        }
        count += 1;
    }

    fn downloadFile(ctx: *activity.Context, file_id: []const u8) ![]const u8 {
        try testing.expectEqualStrings("file1", file_id);
        try record(ctx, "DownloadFileActivity");
        return "file2";
    }

    fn processFile(ctx: *activity.Context, file_name: []const u8) ![]const u8 {
        try testing.expectEqualStrings("file2", file_name);
        try record(ctx, "ProcessFileActivity");
        try mutex.lock(ctx.io());
        defer mutex.unlock(ctx.io());
        if (process_failures_left > 0) {
            process_failures_left -= 1;
            // Not retryable, so the activity itself isn't retried: the workflow retries the
            // whole sequence instead.
            return ctx.fail(.{ .message = "processing failed", .non_retryable = true });
        }
        return "file3";
    }

    fn uploadFile(ctx: *activity.Context, file_name: []const u8) !void {
        try testing.expectEqualStrings("file3", file_name);
        try record(ctx, "UploadFileActivity");
    }
};

fn startEnv() !*temporal.testing.WorkflowEnvironment {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    errdefer env.deinit();
    // Important for the worker to take part in sessions; set before the first workflow starts.
    env.worker.options.sessions = .{};
    try env.registerWorkflow(sample.SampleFileProcessingWorkflow);
    try env.registerActivityAs(activities.DownloadFileActivity, Recorder.downloadFile);
    try env.registerActivityAs(activities.ProcessFileActivity, Recorder.processFile);
    try env.registerActivityAs(activities.UploadFileActivity, Recorder.uploadFile);
    return env;
}

fn expectAttempt(calls: []const Recorder.Call) !void {
    try testing.expectEqual(3, calls.len);
    try testing.expectEqualStrings("DownloadFileActivity", calls[0].name);
    try testing.expectEqualStrings("ProcessFileActivity", calls[1].name);
    try testing.expectEqualStrings("UploadFileActivity", calls[2].name);
    for (calls[1..]) |*call| try testing.expectEqualStrings(Recorder.sessionId(&calls[0]), Recorder.sessionId(call));
}

test "the three activities run in one session and the workflow completes" {
    Recorder.reset(0);
    const env = try startEnv();
    defer env.deinit();

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    try env.executeWorkflow(sample.SampleFileProcessingWorkflow, .{"file1"}, arena.allocator(), null);
    try testing.expectEqual(3, Recorder.count);
    try expectAttempt(Recorder.recorded());
}

test "a failed attempt runs the whole sequence again in a new session" {
    Recorder.reset(1);
    const env = try startEnv();
    defer env.deinit();

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    try env.executeWorkflow(sample.SampleFileProcessingWorkflow, .{"file1"}, arena.allocator(), null);
    try testing.expectEqual(5, Recorder.count);
    const calls = Recorder.recorded();
    try testing.expectEqualStrings("DownloadFileActivity", calls[0].name);
    try testing.expectEqualStrings("ProcessFileActivity", calls[1].name);
    try testing.expectEqualStrings(Recorder.sessionId(&calls[0]), Recorder.sessionId(&calls[1]));
    try expectAttempt(calls[2..]);
    try testing.expect(!std.mem.eql(u8, Recorder.sessionId(&calls[0]), Recorder.sessionId(&calls[2])));
}

test "the workflow fails with the activity's failure after four failed attempts" {
    Recorder.reset(std.math.maxInt(usize));
    const env = try startEnv();
    defer env.deinit();

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var failure: ?temporal.failure.Failure = null;
    try testing.expectError(error.WorkflowFailed, env.executeWorkflow(sample.SampleFileProcessingWorkflow, .{"file1"}, arena.allocator(), &failure));
    try testing.expectEqualStrings("processing failed", temporal.failure.rootCause(&failure.?).message);
    try testing.expectEqual(8, Recorder.count);
}

test "the activities download, upper-case and upload a file, removing each input" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var blob_store: activities.BlobStore = .{};
    var acts: activities.Activities = .{ .blob_store = &blob_store };
    var env: temporal.testing.ActivityEnvironment = .init(testing.io);

    const downloaded = try env.executeActivityWith(activities.DownloadFileActivity, &acts, .{"abc"}, a);
    try testing.expect(std.mem.startsWith(u8, downloaded, "/tmp/temporal_sample"));
    const content = try std.Io.Dir.cwd().readFileAlloc(testing.io, downloaded, a, .limited(1 << 20));
    try testing.expectEqualStrings("dummy content for fileID:abc", content);

    const processed = try env.executeActivityWith(activities.ProcessFileActivity, &acts, .{downloaded}, a);
    try testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(testing.io, downloaded, .{}));
    const upper = try std.Io.Dir.cwd().readFileAlloc(testing.io, processed, a, .limited(1 << 20));
    try testing.expectEqualStrings("DUMMY CONTENT FOR FILEID:ABC", upper);

    try env.executeActivityWith(activities.UploadFileActivity, &acts, .{processed}, a);
    try testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(testing.io, processed, .{}));
}

test "processing fails when the file is not on this host" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var blob_store: activities.BlobStore = .{};
    var acts: activities.Activities = .{ .blob_store = &blob_store };
    var env: temporal.testing.ActivityEnvironment = .init(testing.io);
    try testing.expectError(error.FileNotFound, env.executeActivityWith(activities.ProcessFileActivity, &acts, .{"/tmp/fileprocessing-missing-file"}, arena.allocator()));
}
