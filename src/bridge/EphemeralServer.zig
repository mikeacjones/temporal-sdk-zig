//! Local Temporal servers started by sdk-core for tests and samples: the dev server (the
//! Temporal CLI's `server start-dev`) and the time-skipping test server.
//!
//! The core either runs an existing executable or downloads one into a cache directory and
//! reuses it on later starts.

const EphemeralServer = @This();

const std = @import("std");
const builtin = @import("builtin");
const c = @import("core_c");
const bytes = @import("bytes.zig");
const Runtime = @import("Runtime.zig");
const Completion = @import("completion.zig").Completion;
const handToCore = @import("tsan.zig").handToCore;
const takeFromCore = @import("tsan.zig").takeFromCore;
const Allocator = std.mem.Allocator;
const Io = std.Io;

ptr: *c.TemporalCoreEphemeralServer,
runtime: Runtime,
/// The server's gRPC address, such as "127.0.0.1:61234". Owned.
target: []u8,
gpa: Allocator,

pub const Executable = union(enum) {
    /// Run this binary (the Temporal CLI for the dev server, temporal-test-server otherwise).
    existing_path: []const u8,
    download: Download,

    pub const Download = struct {
        /// "default" picks the version sdk-core recommends; otherwise an exact version.
        version: []const u8 = "default",
        /// Where downloads are cached. Empty uses the system temp directory.
        dest_dir: []const u8 = "",
        /// Re-download after this many seconds. 0 keeps the cached binary forever.
        ttl_seconds: u64 = 0,
    };
};

pub const SearchAttribute = struct {
    name: []const u8,
    type: enum { Text, Keyword, Int, Double, Bool, Datetime, KeywordList },
};

pub const DevServerOptions = struct {
    executable: Executable = .{ .download = .{} },
    namespace: []const u8 = "default",
    ip: []const u8 = "127.0.0.1",
    /// 0 picks a free port.
    port: u16 = 0,
    /// Empty keeps the database in memory.
    database_filename: []const u8 = "",
    ui: bool = false,
    ui_port: u16 = 0,
    log_format: []const u8 = "pretty",
    log_level: []const u8 = "warn",
    /// Custom search attributes to register at startup.
    search_attributes: []const SearchAttribute = &.{},
    /// Extra command-line arguments for the server.
    extra_args: []const []const u8 = &.{},
    /// 0 uses the core default.
    startup_timeout_ms: u64 = 0,
    /// Where the server's stdout and stderr go. See `ServerOutput`.
    output: ServerOutput = .discard,
};

pub const TestServerOptions = struct {
    executable: Executable = .{ .download = .{} },
    /// 0 picks a free port.
    port: u16 = 0,
    extra_args: []const []const u8 = &.{},
    startup_timeout_ms: u64 = 0,
    /// Where the server's stdout and stderr go. See `ServerOutput`.
    output: ServerOutput = .discard,
};

pub const StartError = error{ OutOfMemory, InvalidOptions, StartFailed };

const sdk_name = "temporal-zig";
const sdk_version = @import("../temporal.zig").version;

/// Starts a dev server and waits for it to accept connections. The runtime must outlive the
/// server. A start that fails while downloading or unpacking the server executable is retried,
/// up to 3 attempts in all, 1 s and then 2 s apart, slept on `io`; any other failure returns
/// after one attempt. A cancel during that sleep stops the retries. On `error.StartFailed`,
/// `failure` (if given) receives the last attempt's message, allocated with `gpa`.
pub fn startDevServer(gpa: Allocator, io: Io, runtime: Runtime, options: DevServerOptions, failure: ?*?[]u8) StartError!EphemeralServer {
    var arena_state: std.heap.ArenaAllocator = .init(gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var args: std.ArrayList([]const u8) = .empty;
    for (options.search_attributes) |sa| {
        try args.append(arena, "--search-attribute");
        try args.append(arena, try std.fmt.allocPrint(arena, "{s}={t}", .{ sa.name, sa.type }));
    }
    try args.appendSlice(arena, options.extra_args);

    const test_server = try testServerOptions(arena, options.executable, options.port, args.items, options.startup_timeout_ms);
    const dev: c.TemporalCoreDevServerOptions = .{
        .test_server = test_server,
        .namespace_ = bytes.ref(options.namespace),
        .ip = bytes.ref(options.ip),
        .database_filename = bytes.ref(options.database_filename),
        .ui = options.ui,
        .ui_port = options.ui_port,
        .log_format = bytes.ref(options.log_format),
        .log_level = bytes.ref(options.log_level),
    };
    return startRetrying(gpa, io, runtime, options.output, .{ .dev = &dev }, failure);
}

/// Starts the time-skipping test server. See `startDevServer`.
pub fn startTestServer(gpa: Allocator, io: Io, runtime: Runtime, options: TestServerOptions, failure: ?*?[]u8) StartError!EphemeralServer {
    var arena_state: std.heap.ArenaAllocator = .init(gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const test_server = try testServerOptions(arena, options.executable, options.port, options.extra_args, options.startup_timeout_ms);
    return startRetrying(gpa, io, runtime, options.output, .{ .test_server = test_server }, failure);
}

/// Stops the server and frees it. On `error.ShutdownFailed`, `failure` (if given) receives the
/// core's message; the server is freed either way.
pub fn shutdown(s: *EphemeralServer, io: Io, failure: ?*?[]u8) error{ OutOfMemory, ShutdownFailed }!void {
    defer s.deinit();
    const done = try Completion(?bytes.Owned).create(s.gpa, io, freeOwned);
    handToCore(done);
    c.temporal_core_ephemeral_server_shutdown(s.ptr, done, onShutdown);
    const fail = done.waitUncancelable() orelse return;
    defer fail.deinit();
    if (failure) |f| f.* = fail.dupe(s.gpa) catch null;
    return error.ShutdownFailed;
}

/// Frees the server without stopping it first. Prefer `shutdown`.
pub fn deinit(s: *EphemeralServer) void {
    c.temporal_core_ephemeral_server_free(s.ptr);
    s.gpa.free(s.target);
    s.* = undefined;
}

/// Where a server's stdout and stderr go. The core starts server processes with this
/// process's stdout and stderr, and the C API has no option to change that, so the server's
/// output is chosen by pointing this process's fds 1 and 2 elsewhere while it starts (the
/// server inherits them) and restoring them once startup finishes. Other threads writing to
/// stdout or stderr during that window are redirected too.
pub const ServerOutput = enum {
    /// Drop it. A server that outlives this process (if the process crashes) then holds no
    /// pipe of its parent's open; `zig build` waits for a test's output pipes to close, so an
    /// inherited pipe turns a crashed test into a hung build.
    discard,
    /// Send the server's stdout (its startup banner and logs) to this process's stderr. Never
    /// leave it on stdout when stdout carries data: the Zig test runner uses it for its
    /// protocol with `zig build`.
    stderr,
    /// Leave both as they are.
    inherit,
};

const OutputRedirect = struct {
    saved_out: c_int = -1,
    saved_err: c_int = -1,

    fn begin(mode: ServerOutput) OutputRedirect {
        switch (mode) {
            .inherit => return .{},
            .stderr => {
                const saved = std.c.dup(1);
                if (saved < 0) return .{};
                if (std.c.dup2(2, 1) < 0) {
                    _ = std.c.close(saved);
                    return .{};
                }
                return .{ .saved_out = saved };
            },
            .discard => {
                const null_fd = std.c.open("/dev/null", .{ .ACCMODE = .WRONLY });
                if (null_fd < 0) return begin(.stderr);
                defer _ = std.c.close(null_fd);
                var r: OutputRedirect = .{ .saved_out = std.c.dup(1), .saved_err = std.c.dup(2) };
                if (r.saved_out < 0 or r.saved_err < 0 or std.c.dup2(null_fd, 1) < 0 or std.c.dup2(null_fd, 2) < 0) {
                    r.end();
                    return begin(.stderr);
                }
                return r;
            },
        }
    }

    fn end(r: OutputRedirect) void {
        if (r.saved_out >= 0) {
            _ = std.c.dup2(r.saved_out, 1);
            _ = std.c.close(r.saved_out);
        }
        if (r.saved_err >= 0) {
            _ = std.c.dup2(r.saved_err, 2);
            _ = std.c.close(r.saved_err);
        }
    }
};

const StartResult = struct {
    server: ?*c.TemporalCoreEphemeralServer,
    target: ?bytes.Owned,
    fail: ?bytes.Owned,

    fn free(r: StartResult) void {
        if (r.target) |t| t.deinit();
        if (r.fail) |f| f.deinit();
        if (r.server) |s| c.temporal_core_ephemeral_server_free(s);
    }
};

fn onStart(
    user_data: ?*anyopaque,
    server: ?*c.TemporalCoreEphemeralServer,
    target: ?*const c.TemporalCoreByteArray,
    fail: ?*const c.TemporalCoreByteArray,
) callconv(.c) void {
    takeFromCore(user_data.?);
    const done: *Completion(StartResult) = @ptrCast(@alignCast(user_data.?));
    // The arrays come from the runtime's pool; freeing without the runtime pointer is allowed
    // and only skips recycling the buffer.
    done.complete(.{ .server = server, .target = bytes.Owned.take(null, target), .fail = bytes.Owned.take(null, fail) });
}

fn onShutdown(user_data: ?*anyopaque, fail: ?*const c.TemporalCoreByteArray) callconv(.c) void {
    takeFromCore(user_data.?);
    const done: *Completion(?bytes.Owned) = @ptrCast(@alignCast(user_data.?));
    done.complete(bytes.Owned.take(null, fail));
}

fn freeOwned(o: ?bytes.Owned) void {
    if (o) |v| v.deinit();
}

fn finishStart(gpa: Allocator, runtime: Runtime, result: StartResult, failure: ?*?[]u8) StartError!EphemeralServer {
    defer if (result.fail) |f| f.deinit();
    defer if (result.target) |t| t.deinit();
    const server = result.server orelse {
        if (failure) |f| f.* = if (result.fail) |msg| msg.dupe(gpa) catch null else null;
        return error.StartFailed;
    };
    const target = gpa.dupe(u8, if (result.target) |t| t.bytes() else "") catch {
        c.temporal_core_ephemeral_server_free(server);
        return error.OutOfMemory;
    };
    return .{ .ptr = server, .runtime = runtime, .target = target, .gpa = gpa };
}

const max_start_attempts = 3;
/// Sleep before attempt 2 and before attempt 3.
const start_backoff = [_]Io.Duration{ .fromSeconds(1), .fromSeconds(2) };
comptime {
    std.debug.assert(start_backoff.len == max_start_attempts - 1);
}
const bridge_failure_prefix = "Connection failed: ";
const download_failure_prefixes = [_][]const u8{
    "failed to download ephemeral server executable",
    "invalid ephemeral server download",
};

/// True when the core's start failure came from downloading or unpacking the executable,
/// which a fresh start redoes (the core removes its `.downloading` temp file on failure).
/// The C bridge puts `bridge_failure_prefix` in front of every start error (sdk-core-c-bridge's
/// testing.rs), so the core's own message follows it.
fn isDownloadFailure(message: []const u8) bool {
    const core_message = if (std.mem.startsWith(u8, message, bridge_failure_prefix)) message[bridge_failure_prefix.len..] else message;
    for (download_failure_prefixes) |prefix| if (std.mem.startsWith(u8, core_message, prefix)) return true;
    return false;
}

/// What one attempt starts.
const Start = union(enum) {
    dev: *const c.TemporalCoreDevServerOptions,
    test_server: *const c.TemporalCoreTestServerOptions,
};

/// Makes one start through the core and waits for its callback.
fn startOnce(gpa: Allocator, io: Io, runtime: Runtime, output: ServerOutput, start: Start) error{OutOfMemory}!StartResult {
    const done = try Completion(StartResult).create(gpa, io, StartResult.free);
    const redirect: OutputRedirect = .begin(output);
    defer redirect.end();
    handToCore(done);
    switch (start) {
        .dev => |o| c.temporal_core_ephemeral_server_start_dev_server(runtime.ptr, o, done, onStart),
        .test_server => |o| c.temporal_core_ephemeral_server_start_test_server(runtime.ptr, o, done, onStart),
    }
    // Uncancelable: the core reads the options from `arena` until it calls back, and startup
    // is bounded by its timeout.
    return done.waitUncancelable();
}

// Test hook, read only when builtin.is_test. Tests that set it reset it.
/// The next N attempts fail with a fake download error and do not call the core.
var test_injected_download_failures: std.atomic.Value(u32) = .init(0);
/// Counts every attempt, fake ones included.
var test_start_attempts: std.atomic.Value(u32) = .init(0);

fn takeInjectedFailure() bool {
    var n = test_injected_download_failures.load(.monotonic);
    while (n > 0) {
        n = test_injected_download_failures.cmpxchgWeak(n, n - 1, .monotonic, .monotonic) orelse return true;
    }
    return false;
}

/// The retry loop that startDevServer and startTestServer both end in.
fn startRetrying(gpa: Allocator, io: Io, runtime: Runtime, output: ServerOutput, start: Start, failure: ?*?[]u8) StartError!EphemeralServer {
    var attempt: u32 = 1;
    while (true) : (attempt += 1) {
        const last = attempt == max_start_attempts;
        if (builtin.is_test) {
            _ = test_start_attempts.fetchAdd(1, .monotonic);
            if (takeInjectedFailure()) {
                const message = try std.fmt.allocPrint(gpa, "Connection failed: invalid ephemeral server download: injected by a test on attempt {d}", .{attempt});
                if (last or !isDownloadFailure(message) or !backoff(io, attempt)) {
                    if (failure) |f| f.* = message else gpa.free(message);
                    return error.StartFailed;
                }
                gpa.free(message);
                continue;
            }
        }
        const result = try startOnce(gpa, io, runtime, output, start);
        const retry = !last and result.server == null and
            if (result.fail) |f| isDownloadFailure(f.bytes()) else false;
        // A result with a server never retries, so freeing it below never drops a server.
        if (!retry or !backoff(io, attempt)) return finishStart(gpa, runtime, result, failure);
        result.free();
    }
}

/// Sleeps before the attempt after `attempt`. False when canceled: the cancel is re-armed so
/// the caller's next cancelation point still sees it, and no further attempt is made.
fn backoff(io: Io, attempt: u32) bool {
    io.sleep(start_backoff[attempt - 1], .awake) catch |err| switch (err) {
        error.Canceled => {
            io.recancel();
            return false;
        },
    };
    return true;
}

fn testServerOptions(
    arena: Allocator,
    executable: Executable,
    port: u16,
    extra_args: []const []const u8,
    startup_timeout_ms: u64,
) StartError!*c.TemporalCoreTestServerOptions {
    for (extra_args) |a| if (std.mem.indexOfScalar(u8, a, '\n') != null) return error.InvalidOptions;
    const joined = try std.mem.join(arena, "\n", extra_args);
    const opts = try arena.create(c.TemporalCoreTestServerOptions);
    opts.* = .{
        .existing_path = bytes.ref(""),
        .sdk_name = bytes.ref(sdk_name),
        .sdk_version = bytes.ref(sdk_version),
        .download_version = bytes.ref(""),
        .download_dest_dir = bytes.ref(""),
        .port = port,
        .extra_args = bytes.ref(joined),
        .download_ttl_seconds = 0,
        .startup_timeout_millis = startup_timeout_ms,
    };
    switch (executable) {
        .existing_path => |p| {
            if (p.len == 0) return error.InvalidOptions;
            opts.existing_path = bytes.ref(p);
        },
        .download => |d| {
            opts.download_version = bytes.ref(d.version);
            opts.download_dest_dir = bytes.ref(d.dest_dir);
            opts.download_ttl_seconds = d.ttl_seconds;
        },
    }
    return opts;
}

// ---------------------------------------------------------------- tests

const testing = std.testing;

test "a dev server starts, reports its address, and shuts down" {
    const rt = try Runtime.init(testing.allocator, .{}, null);
    defer rt.deinit();

    var failure: ?[]u8 = null;
    defer if (failure) |f| testing.allocator.free(f);
    var server = startDevServer(testing.allocator, testing.io, rt, .{
        .search_attributes = &.{.{ .name = "CustomerId", .type = .Keyword }},
    }, &failure) catch |err| {
        std.debug.print("dev server failed to start: {s}\n", .{failure orelse "(no message)"});
        return err;
    };
    try testing.expect(std.mem.startsWith(u8, server.target, "127.0.0.1:"));
    try server.shutdown(testing.io, null);
}

test "a missing executable fails with the core's message" {
    const rt = try Runtime.init(testing.allocator, .{}, null);
    defer rt.deinit();
    var failure: ?[]u8 = null;
    defer if (failure) |f| testing.allocator.free(f);
    try testing.expectError(error.StartFailed, startDevServer(testing.allocator, testing.io, rt, .{
        .executable = .{ .existing_path = "/nonexistent/temporal" },
    }, &failure));
    try testing.expect(failure != null and failure.?.len > 0);
}

test "arguments with newlines are rejected" {
    const rt = try Runtime.init(testing.allocator, .{}, null);
    defer rt.deinit();
    try testing.expectError(error.InvalidOptions, startDevServer(testing.allocator, testing.io, rt, .{
        .extra_args = &.{"--bad\narg"},
    }, null));
}

test "a time-skipping test server starts and shuts down" {
    const rt = try Runtime.init(testing.allocator, .{}, null);
    defer rt.deinit();
    var failure: ?[]u8 = null;
    defer if (failure) |f| testing.allocator.free(f);
    var server = startTestServer(testing.allocator, testing.io, rt, .{}, &failure) catch |err| {
        std.debug.print("test server failed to start: {s}\n", .{failure orelse "(no message)"});
        return err;
    };
    try testing.expect(server.target.len > 0);
    try server.shutdown(testing.io, null);
}

test "a download failure is retried and the dev server then starts" {
    test_start_attempts.store(0, .monotonic);
    test_injected_download_failures.store(1, .monotonic);
    defer test_injected_download_failures.store(0, .monotonic);
    const rt = try Runtime.init(testing.allocator, .{}, null);
    defer rt.deinit();
    var failure: ?[]u8 = null;
    defer if (failure) |f| testing.allocator.free(f);
    var server = startDevServer(testing.allocator, testing.io, rt, .{}, &failure) catch |err| {
        std.debug.print("dev server failed to start: {s}\n", .{failure orelse "(no message)"});
        return err;
    };
    defer server.shutdown(testing.io, null) catch {};
    try testing.expectEqual(2, test_start_attempts.load(.monotonic));
    try testing.expectEqual(null, failure);
}

test "a download failure is retried and the test server then starts" {
    test_start_attempts.store(0, .monotonic);
    test_injected_download_failures.store(1, .monotonic);
    defer test_injected_download_failures.store(0, .monotonic);
    const rt = try Runtime.init(testing.allocator, .{}, null);
    defer rt.deinit();
    var failure: ?[]u8 = null;
    defer if (failure) |f| testing.allocator.free(f);
    var server = startTestServer(testing.allocator, testing.io, rt, .{}, &failure) catch |err| {
        std.debug.print("test server failed to start: {s}\n", .{failure orelse "(no message)"});
        return err;
    };
    defer server.shutdown(testing.io, null) catch {};
    try testing.expectEqual(2, test_start_attempts.load(.monotonic));
    try testing.expectEqual(null, failure);
}

test "a failure that is not a download error makes one attempt" {
    test_start_attempts.store(0, .monotonic);
    const rt = try Runtime.init(testing.allocator, .{}, null);
    defer rt.deinit();
    var failure: ?[]u8 = null;
    defer if (failure) |f| testing.allocator.free(f);
    try testing.expectError(error.StartFailed, startDevServer(testing.allocator, testing.io, rt, .{
        .executable = .{ .existing_path = "/nonexistent/temporal" },
    }, &failure));
    try testing.expectEqual(1, test_start_attempts.load(.monotonic));
    try testing.expect(failure != null);
    try testing.expect(!isDownloadFailure(failure.?));
}

test "three download failures return the last attempt's message" {
    test_start_attempts.store(0, .monotonic);
    test_injected_download_failures.store(3, .monotonic);
    defer test_injected_download_failures.store(0, .monotonic);
    const rt = try Runtime.init(testing.allocator, .{}, null);
    defer rt.deinit();
    var failure: ?[]u8 = null;
    defer if (failure) |f| testing.allocator.free(f);
    try testing.expectError(error.StartFailed, startTestServer(testing.allocator, testing.io, rt, .{}, &failure));
    try testing.expectEqual(3, test_start_attempts.load(.monotonic));
    try testing.expectEqualStrings("Connection failed: invalid ephemeral server download: injected by a test on attempt 3", failure orelse "(no message)");
}

test "a real download failure from the core is retried" {
    // A download into a directory that does not exist fails in the core's lazy_download_exe
    // with EphemeralServerError::Download, the kind of failure the retry is for. The C bridge
    // hands it back as "Connection failed: failed to download ephemeral server executable: ...".
    test_start_attempts.store(0, .monotonic);
    const rt = try Runtime.init(testing.allocator, .{}, null);
    defer rt.deinit();
    var failure: ?[]u8 = null;
    defer if (failure) |f| testing.allocator.free(f);
    try testing.expectError(error.StartFailed, startDevServer(testing.allocator, testing.io, rt, .{
        .executable = .{ .download = .{ .dest_dir = "/nonexistent/temporal-sdk-zig-download-dir" } },
    }, &failure));
    try testing.expect(failure != null);
    try testing.expect(std.mem.indexOf(u8, failure.?, "failed to download ephemeral server executable") != null);
    try testing.expectEqual(max_start_attempts, test_start_attempts.load(.monotonic));
}

test "download failures are recognized behind the bridge's prefix" {
    try testing.expect(isDownloadFailure("Connection failed: invalid ephemeral server download: could not extract tar archive entry"));
    try testing.expect(isDownloadFailure("Connection failed: failed to download ephemeral server executable: timed out"));
    try testing.expect(isDownloadFailure("invalid ephemeral server download: bad"));
    try testing.expect(!isDownloadFailure("Connection failed: failed to start server: No such file or directory"));
    try testing.expect(!isDownloadFailure("server says: invalid ephemeral server download"));
    try testing.expect(!isDownloadFailure(""));
}
