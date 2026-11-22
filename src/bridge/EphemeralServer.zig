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
/// The server is freed through `Runtime.release`, which keeps it until the runtime is freed if
/// the runtime forwards logs.
pub fn deinit(s: *EphemeralServer) void {
    s.runtime.release(.{ .ephemeral_server = s.ptr });
    s.gpa.free(s.target);
    s.* = undefined;
}

/// Where a server's stdout and stderr go. The core starts server processes with this
/// process's stdout and stderr, and the C API has no option to change that, so the server's
/// output is chosen by pointing this process's fds 1 and 2 elsewhere while it starts (the
/// server inherits them) and restoring them once startup finishes. Other threads writing to
/// stdout or stderr during that window are redirected too.
///
/// Starts that overlap in one process share one redirect. The first start's mode applies until
/// the last overlapping start finishes, and then fds 1 and 2 are restored to what they were
/// before the first start (an fd that was closed then is closed again). An `.inherit` start
/// that overlaps a redirected one inherits the redirected fds. Other threads' output is
/// redirected for as long as any start is running.
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

/// Process-wide state of the fd 1/2 redirect shared by overlapping starts.
/// Guarded by `redirect_mutex`.
const RedirectState = struct {
    /// Starts between `begin` and `end` that take part (every mode but `.inherit`).
    active: u32 = 0,
    /// A copy of fd 1/2 (3 or above, close-on-exec), -1 when that fd was not redirected, or
    /// `fd_was_closed` when it was closed before the first start. The copies and the
    /// /dev/null fd are close-on-exec, so a server child inherits only fds 1 and 2.
    saved_out: c_int = -1,
    saved_err: c_int = -1,
};
/// A `RedirectState.saved_out` / `saved_err` value: that fd was closed before the start
/// redirected it, so `restore` closes it again instead of putting a copy back.
const fd_was_closed: c_int = -2;
/// Held only inside `OutputRedirect.begin` and `end`, never across the core call or the wait
/// for its callback, so it is safe with any `Io` and with several in one process.
var redirect_mutex: std.c.pthread_mutex_t = std.c.PTHREAD_MUTEX_INITIALIZER;
var redirect_state: RedirectState = .{};

const OutputRedirect = struct {
    /// True when this start holds a count in `redirect_state`. `end` then releases it.
    counted: bool = false,

    /// `.inherit` takes no part. Any other mode joins the redirect: the first start of an
    /// overlap points fds 1 and 2 by its mode, and later ones only count themselves in.
    fn begin(mode: ServerOutput) OutputRedirect {
        if (mode == .inherit) return .{};
        lock();
        defer unlock();
        if (redirect_state.active == 0) {
            const saved = redirectLocked(mode);
            redirect_state = .{ .active = 1, .saved_out = saved.out, .saved_err = saved.err };
        } else {
            redirect_state.active += 1;
        }
        return .{ .counted = true };
    }

    /// The last start of an overlap to end restores fds 1 and 2 to what they were before the
    /// first one began.
    fn end(r: OutputRedirect) void {
        if (!r.counted) return;
        lock();
        defer unlock();
        std.debug.assert(redirect_state.active > 0);
        redirect_state.active -= 1;
        if (redirect_state.active > 0) return;
        restore(redirect_state.saved_out, redirect_state.saved_err);
        redirect_state = .{};
    }

    fn lock() void {
        std.debug.assert(std.c.pthread_mutex_lock(&redirect_mutex) == .SUCCESS);
    }

    fn unlock() void {
        std.debug.assert(std.c.pthread_mutex_unlock(&redirect_mutex) == .SUCCESS);
    }

    /// Points fds 1 and 2 by `mode` and returns copies of the fds it replaced (-1 for one it
    /// left alone). On failure it falls back from `.discard` to `.stderr` to nothing, closing
    /// what it opened. Writes to no global; the caller holds `redirect_mutex`.
    fn redirectLocked(mode: ServerOutput) struct { out: c_int, err: c_int } {
        switch (mode) {
            .inherit => return .{ .out = -1, .err = -1 },
            .stderr => {
                const saved = dupCloexec(1);
                if (saved < 0) return .{ .out = -1, .err = -1 };
                if (std.c.dup2(2, 1) < 0) {
                    _ = std.c.close(saved);
                    return .{ .out = -1, .err = -1 };
                }
                return .{ .out = saved, .err = -1 };
            },
            .discard => {
                // Record fds 1 and 2 before opening /dev/null: with one of them closed, open
                // would take that number and the check would see it open.
                const out = saveFd(1);
                const err = saveFd(2);
                if (out == -1 or err == -1) {
                    dropSaved(out);
                    dropSaved(err);
                    return redirectLocked(.stderr);
                }
                const null_fd = nullFd: {
                    const low = std.c.open("/dev/null", .{ .ACCMODE = .WRONLY, .CLOEXEC = true });
                    if (low < 0 or low >= 3) break :nullFd low;
                    // Keep it off 0-2, so its close below can never close fd 1 or 2.
                    const high = dupCloexec(low);
                    _ = std.c.close(low);
                    break :nullFd high;
                };
                if (null_fd < 0) {
                    dropSaved(out);
                    dropSaved(err);
                    return redirectLocked(.stderr);
                }
                defer _ = std.c.close(null_fd);
                if (std.c.dup2(null_fd, 1) < 0) {
                    dropSaved(out);
                    dropSaved(err);
                    return redirectLocked(.stderr);
                }
                if (std.c.dup2(null_fd, 2) < 0) {
                    restore(out, -1);
                    dropSaved(err);
                    return redirectLocked(.stderr);
                }
                return .{ .out = out, .err = err };
            },
        }
    }

    /// A close-on-exec copy of `fd`, made in one call. A plain dup survives exec, so a server
    /// child would keep the test runner's stdout and stderr pipes open after the test process
    /// dies; a dup followed by a separate fcntl leaves a window for another thread's spawn.
    /// The copy is 3 or above: with fd 1 or 2 closed, a copy on that number would be taken
    /// for the caller's fd and closed under it.
    fn dupCloexec(fd: c_int) c_int {
        return std.c.fcntl(fd, std.c.F.DUPFD_CLOEXEC, @as(c_int, 3));
    }

    /// F_GETFD fails only with EBADF.
    fn isOpen(fd: c_int) bool {
        return std.c.fcntl(fd, std.c.F.GETFD) >= 0;
    }

    /// What `restore` needs to put `fd` back: a close-on-exec copy, `fd_was_closed` when it
    /// is closed, or -1 when the copy failed.
    fn saveFd(fd: c_int) c_int {
        if (!isOpen(fd)) return fd_was_closed;
        return dupCloexec(fd);
    }

    /// Discards a value `saveFd` returned without touching fd 1 or 2.
    fn dropSaved(saved: c_int) void {
        if (saved >= 0) _ = std.c.close(saved);
    }

    /// Puts saved copies back on fds 1 and 2 and closes them. `fd_was_closed` closes that fd
    /// again; -1 skips it.
    fn restore(saved_out: c_int, saved_err: c_int) void {
        restoreOne(1, saved_out);
        restoreOne(2, saved_err);
    }

    fn restoreOne(fd: c_int, saved: c_int) void {
        if (saved >= 0) {
            _ = std.c.dup2(saved, fd);
            _ = std.c.close(saved);
        } else if (saved == fd_was_closed) {
            _ = std.c.close(fd);
        }
    }
};

const StartResult = struct {
    server: ?*c.TemporalCoreEphemeralServer,
    target: ?bytes.Owned,
    fail: ?bytes.Owned,
    runtime: Runtime,

    fn free(r: StartResult) void {
        if (r.target) |t| t.deinit();
        if (r.fail) |f| f.deinit();
        if (r.server) |s| r.runtime.release(.{ .ephemeral_server = s });
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
    done.complete(.{ .server = server, .target = bytes.Owned.take(null, target), .fail = bytes.Owned.take(null, fail), .runtime = done.result.runtime });
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
        runtime.release(.{ .ephemeral_server = server });
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
    // `onStart` carries the runtime over into its result, for `StartResult.free`.
    done.result = .{ .server = null, .target = null, .fail = null, .runtime = runtime };
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

/// Its own function, not inline in startRetrying: Zig 0.17.1's self-hosted x86_64 backend
/// (Debug on x86_64 Linux) keeps a comparison's result in the CPU flags and does not save it
/// before the `lock xadd` of a `fetchAdd`, which overwrites the flags. Inline, `last` read
/// false on attempt 3 and the backoff table was indexed past its end. A call saves the flags.
fn countTestAttempt() void {
    _ = test_start_attempts.fetchAdd(1, .monotonic);
}

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
            countTestAttempt();
            if (takeInjectedFailure()) {
                const message = try std.fmt.allocPrint(gpa, "invalid ephemeral server download: injected by a test on attempt {d}", .{attempt});
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
    try testing.expectEqualStrings("invalid ephemeral server download: injected by a test on attempt 3", failure orelse "(no message)");
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

/// What an fd points at, by its device and inode. Zig 0.17.1's std.c has no `fstat` or `Stat`
/// on Linux (std goes through statx there), so Linux asks statx for the same two fields.
const FdId = struct { dev: u64, ino: u64 };

/// A device number as FdId's u64 field: its bits read as an unsigned integer of the same
/// width, then widened. macOS's dev_t is i32, and devfs device numbers (/dev/null's) often
/// have the high bit set, so a range-checked cast would panic on them; reading the bits
/// gives every device number one distinct value.
fn devBits(dev: anytype) u64 {
    const T = @TypeOf(dev);
    const U = @Int(.unsigned, @bitSizeOf(T));
    return @as(U, @bitCast(dev));
}

fn fdId(fd: c_int) ?FdId {
    if (builtin.os.tag == .linux) {
        const linux = std.os.linux;
        var stx: linux.Statx = undefined;
        if (linux.errno(linux.statx(fd, "", linux.AT.EMPTY_PATH, .{ .INO = true }, &stx)) != .SUCCESS) return null;
        if (!stx.mask.INO) return null;
        return .{ .dev = (@as(u64, stx.dev_major) << 32) | stx.dev_minor, .ino = stx.ino };
    }
    var st: std.c.Stat = undefined;
    if (std.c.fstat(fd, &st) != 0) return null;
    return .{ .dev = devBits(st.dev), .ino = @intCast(st.ino) };
}

fn fdIs(fd: c_int, want: FdId) bool {
    const got = fdId(fd) orelse return false;
    return std.meta.eql(got, want);
}

test "a device number with the high bit set maps to one distinct id without a range check" {
    // macOS's dev_t is i32 and devfs numbers can be negative (CI run 44890622640 panicked here).
    const min = devBits(@as(i32, std.math.minInt(i32)));
    const neg_one = devBits(@as(i32, -1));
    const max = devBits(@as(i32, std.math.maxInt(i32)));
    const zero = devBits(@as(i32, 0));
    try testing.expectEqual(@as(u64, 0x8000_0000), min);
    try testing.expectEqual(@as(u64, 0xFFFF_FFFF), neg_one);
    try testing.expectEqual(@as(u64, 0x7FFF_FFFF), max);
    try testing.expectEqual(@as(u64, 0), zero);
    try testing.expectEqual(@as(u64, 5), devBits(@as(i32, 5)));
    try testing.expectEqual(@as(u64, std.math.maxInt(u64)), devBits(@as(u64, std.math.maxInt(u64))));
    const ids = [_]u64{ min, neg_one, max, zero };
    for (ids, 0..) |a, i| for (ids[i + 1 ..]) |b| try testing.expect(a != b);
    if (builtin.os.tag == .macos) {
        // The values fdId actually sees: std.c.dev_t itself, with the high bit set.
        try testing.expectEqual(@as(u64, 0x8000_0000), devBits(@as(std.c.dev_t, std.math.minInt(std.c.dev_t))));
        try testing.expectEqual(@as(u64, 0xFFFF_FFFF), devBits(@as(std.c.dev_t, -1)));
    }
}

test "overlapping output redirects restore fds 1 and 2 in either order" {
    // Repair copies, so the runner's stdout protocol and stderr survive a failed check.
    const repair_out = std.c.dup(1);
    try testing.expect(repair_out >= 0);
    defer _ = std.c.close(repair_out);
    const repair_err = std.c.dup(2);
    try testing.expect(repair_err >= 0);
    defer _ = std.c.close(repair_err);
    const before_out = fdId(1) orelse return error.TestUnexpectedResult;
    const before_err = fdId(2) orelse return error.TestUnexpectedResult;

    const orders = [_]struct { first: ServerOutput, second: ServerOutput, first_ends_first: bool }{
        .{ .first = .discard, .second = .discard, .first_ends_first = true },
        .{ .first = .discard, .second = .discard, .first_ends_first = false },
        // The first start's mode wins: `.stderr` leaves fd 2 alone while `.discard` overlaps.
        .{ .first = .stderr, .second = .discard, .first_ends_first = true },
    };
    for (orders) |o| {
        const a: OutputRedirect = .begin(o.first);
        const b: OutputRedirect = .begin(o.second);
        const first_mode_held = o.first != .stderr or (fdIs(1, before_err) and fdIs(2, before_err));
        if (o.first_ends_first) {
            a.end();
            b.end();
        } else {
            b.end();
            a.end();
        }
        const ok = first_mode_held and fdIs(1, before_out) and fdIs(2, before_err) and redirect_state.active == 0 and
            redirect_state.saved_out == -1 and redirect_state.saved_err == -1;
        if (!ok) {
            _ = std.c.dup2(repair_out, 1);
            _ = std.c.dup2(repair_err, 2);
            return error.TestUnexpectedResult;
        }
    }
}

test "every fd an output redirect saves is close-on-exec" {
    // Repair copies, so the runner's stdout protocol and stderr survive a failed check.
    const repair_out = std.c.dup(1);
    try testing.expect(repair_out >= 0);
    defer _ = std.c.close(repair_out);
    const repair_err = std.c.dup(2);
    try testing.expect(repair_err >= 0);
    defer _ = std.c.close(repair_err);
    const before_out = fdId(1) orelse return error.TestUnexpectedResult;
    const before_err = fdId(2) orelse return error.TestUnexpectedResult;

    for ([_]ServerOutput{ .discard, .stderr }) |mode| {
        const r: OutputRedirect = .begin(mode);
        const saved_out = redirect_state.saved_out;
        const saved_err = redirect_state.saved_err;
        // A `.discard` start that fell back to `.stderr` saves no copy of fd 2 and fails here.
        var ok = saved_out >= 0 and (if (mode == .discard) saved_err >= 0 else saved_err == -1);
        for ([_]c_int{ saved_out, saved_err }) |fd| {
            if (fd < 0) continue;
            const flags = std.c.fcntl(fd, std.c.F.GETFD);
            ok = ok and flags >= 0 and flags & std.c.FD_CLOEXEC != 0;
        }
        r.end();
        ok = ok and fdIs(1, before_out) and fdIs(2, before_err);
        if (!ok) {
            _ = std.c.dup2(repair_out, 1);
            _ = std.c.dup2(repair_err, 2);
            return error.TestUnexpectedResult;
        }
    }
}

/// Closes `fd` (1 or 2), runs a `.discard` redirect and checks that /dev/null is on fds 1 and 2
/// meanwhile and that afterwards `fd` is closed again and the other fd is what it was.
fn checkDiscardWithClosed(fd: c_int) !void {
    // Repair copies: the runner's stdout protocol and stderr must come back on every path,
    // success included, since this closes one of them on purpose.
    const repair_out = std.c.dup(1);
    try testing.expect(repair_out >= 0);
    defer _ = std.c.close(repair_out);
    const repair_err = std.c.dup(2);
    try testing.expect(repair_err >= 0);
    defer _ = std.c.close(repair_err);
    const other: c_int = if (fd == 1) 2 else 1;
    const other_id = fdId(other) orelse return error.TestUnexpectedResult;
    const null_id = null_id: {
        const n = std.c.open("/dev/null", .{ .ACCMODE = .RDONLY, .CLOEXEC = true });
        try testing.expect(n >= 0);
        defer _ = std.c.close(n);
        break :null_id fdId(n) orelse return error.TestUnexpectedResult;
    };

    // No `try` from here until the repair.
    _ = std.c.close(fd);
    const r: OutputRedirect = .begin(.discard);
    var ok = fdIs(1, null_id) and fdIs(2, null_id);
    for ([_]c_int{ 1, 2 }) |f| {
        const flags = std.c.fcntl(f, std.c.F.GETFD);
        ok = ok and flags >= 0 and flags & std.c.FD_CLOEXEC == 0;
    }
    r.end();
    ok = ok and std.c.fcntl(fd, std.c.F.GETFD) < 0 and fdIs(other, other_id) and redirect_state.active == 0;
    _ = std.c.dup2(repair_out, 1);
    _ = std.c.dup2(repair_err, 2);
    if (!ok) return error.TestUnexpectedResult;
}

test "a discard redirect keeps a closed fd 1 closed and puts /dev/null on it meanwhile" {
    try checkDiscardWithClosed(1);
}

test "a discard redirect keeps a closed fd 2 closed and puts /dev/null on it meanwhile" {
    try checkDiscardWithClosed(2);
}
