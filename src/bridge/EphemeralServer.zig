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

/// Starts the time-skipping test server. See `startDevServer`. With `options.port` 0 on macOS,
/// the port is picked here rather than by sdk-core (see `pickTestServerPort`); when none can be
/// picked, `error.StartFailed` comes back with the reason in `failure`.
pub fn startTestServer(gpa: Allocator, io: Io, runtime: Runtime, options: TestServerOptions, failure: ?*?[]u8) StartError!EphemeralServer {
    var arena_state: std.heap.ArenaAllocator = .init(gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const port = if (options.port != 0) options.port else try pickTestServerPort(gpa, failure);
    const test_server = try testServerOptions(arena, options.executable, port, options.extra_args, options.startup_timeout_ms);
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
    /// Keep it out of this process's output. While the server starts, fds 1 and 2 point at an
    /// unnamed (already unlinked) temporary file, and on a failed start the last part of what
    /// it holds (at most 4 KiB) is appended to the failure message. The file is shared by every
    /// start that overlaps in this process, so the reported text can hold another server's
    /// lines and is not guaranteed to be this server's output alone. The server keeps the file
    /// as its stdout and stderr for its whole life: output it writes after startup stays in the
    /// unlinked file (emptied of the startup output once the start ends) until the server
    /// exits. If the file cannot be created, output goes to /dev/null instead. A server that
    /// outlives this process (if the process crashes) holds no pipe of its parent's open
    /// either way; `zig build` waits for a test's output pipes to close, so an inherited pipe
    /// turns a crashed test into a hung build.
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
    /// The `.discard` capture file of this overlap window (3 or above, close-on-exec,
    /// O_RDWR | O_APPEND, already unlinked), or -1.
    capture_fd: c_int = -1,
};
/// The most bytes of server output a failure message reports.
const capture_tail_max = 4096;
/// Makes capture file names unique within the process. Guarded by `redirect_mutex`.
var capture_seq: u32 = 0;
// Test hook, read only when builtin.is_test. Tests that set it reset it.
/// The directory the capture file is created in, instead of TMPDIR or /tmp.
var test_capture_dir: ?[*:0]const u8 = null;

/// What `OutputRedirect.readTail` found.
const CaptureTail = union(enum) {
    /// The start was not a `.discard` start: no output is reported.
    no_capture_file,
    /// A `.discard` start whose capture file could not be made or read.
    not_captured,
    /// The last part of the capture file (trailing whitespace removed, and cut forward to a
    /// line start when the file was longer than `capture_tail_max`) and the file's size.
    text: struct { bytes: []const u8, total: u64 },
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
    /// True for a `.discard` start, whose failure reports the capture file's tail.
    discard: bool = false,

    /// `.inherit` takes no part. Any other mode joins the redirect: the first start of an
    /// overlap points fds 1 and 2 by its mode, and later ones only count themselves in.
    fn begin(mode: ServerOutput) OutputRedirect {
        if (mode == .inherit) return .{};
        lock();
        defer unlock();
        if (redirect_state.active == 0) {
            const saved = redirectLocked(mode);
            redirect_state = .{ .active = 1, .saved_out = saved.out, .saved_err = saved.err, .capture_fd = saved.capture };
        } else {
            redirect_state.active += 1;
        }
        return .{ .counted = true, .discard = mode == .discard };
    }

    /// The last start of an overlap to end restores fds 1 and 2 to what they were before the
    /// first one began, and lets go of the capture file. Its truncate drops the startup output;
    /// a server still running keeps the file, and O_APPEND puts its later writes at offset 0.
    fn end(r: OutputRedirect) void {
        if (!r.counted) return;
        lock();
        defer unlock();
        std.debug.assert(redirect_state.active > 0);
        redirect_state.active -= 1;
        if (redirect_state.active > 0) return;
        const capture = redirect_state.capture_fd;
        if (capture >= 0) _ = std.c.ftruncate(capture, 0);
        restore(redirect_state.saved_out, redirect_state.saved_err);
        if (capture >= 0) _ = std.c.close(capture);
        redirect_state = .{};
    }

    /// The tail of the capture file, read into `buf`. Writes are O_APPEND, so the pread moves
    /// no offset another start relies on.
    fn readTail(r: *const OutputRedirect, buf: *[capture_tail_max]u8) CaptureTail {
        if (!r.counted or !r.discard) return .no_capture_file;
        lock();
        defer unlock();
        const fd = redirect_state.capture_fd;
        if (fd < 0) return .not_captured;
        const end_offset = std.c.lseek(fd, 0, std.c.SEEK.END);
        if (end_offset < 0) return .not_captured;
        const total: u64 = @intCast(end_offset);
        const want: usize = @intCast(@min(total, capture_tail_max));
        var got: usize = 0;
        while (got < want) {
            const offset: std.c.off_t = @intCast(total - want + got);
            const n = std.c.pread(fd, buf[got..].ptr, want - got, offset);
            if (n > 0) {
                got += @intCast(n);
            } else if (n == 0) {
                break;
            } else if (std.c.errno(n) != .INTR) {
                return .not_captured;
            }
        }
        var text: []const u8 = buf[0..got];
        if (total > capture_tail_max) {
            if (std.mem.indexOfScalar(u8, text, '\n')) |nl| text = text[nl + 1 ..];
        }
        text = std.mem.trimEnd(u8, text, &std.ascii.whitespace);
        return .{ .text = .{ .bytes = text, .total = total } };
    }

    fn lock() void {
        std.debug.assert(std.c.pthread_mutex_lock(&redirect_mutex) == .SUCCESS);
    }

    fn unlock() void {
        std.debug.assert(std.c.pthread_mutex_unlock(&redirect_mutex) == .SUCCESS);
    }

    /// Points fds 1 and 2 by `mode` and returns copies of the fds it replaced (-1 for one it
    /// left alone) and the capture file it opened (-1 for none). On failure it falls back from
    /// the capture file to /dev/null, from `.discard` to `.stderr`, and then to nothing,
    /// closing what it opened. Writes to no global but `capture_seq`; the caller holds
    /// `redirect_mutex`.
    fn redirectLocked(mode: ServerOutput) struct { out: c_int, err: c_int, capture: c_int = -1 } {
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
                // Record fds 1 and 2 before opening anything: with one of them closed, open
                // would take that number and the check would see it open.
                const out = saveFd(1);
                const err = saveFd(2);
                if (out == -1 or err == -1) {
                    dropSaved(out);
                    dropSaved(err);
                    return redirectLocked(.stderr);
                }
                const capture = openCaptureFile();
                const target = if (capture >= 0) capture else keepAbove2(std.c.open("/dev/null", .{ .ACCMODE = .WRONLY, .CLOEXEC = true }));
                if (target < 0) {
                    dropSaved(out);
                    dropSaved(err);
                    return redirectLocked(.stderr);
                }
                // The capture file stays open until `end`; /dev/null is not needed after.
                defer if (capture < 0) {
                    _ = std.c.close(target);
                };
                if (std.c.dup2(target, 1) < 0) {
                    if (capture >= 0) _ = std.c.close(capture);
                    dropSaved(out);
                    dropSaved(err);
                    return redirectLocked(.stderr);
                }
                if (std.c.dup2(target, 2) < 0) {
                    restore(out, -1);
                    if (capture >= 0) _ = std.c.close(capture);
                    dropSaved(err);
                    return redirectLocked(.stderr);
                }
                return .{ .out = out, .err = err, .capture = capture };
            },
        }
    }

    /// `fd` moved to a close-on-exec number of 3 or above, so that closing it can never close
    /// fd 1 or 2. Negative stays negative.
    fn keepAbove2(fd: c_int) c_int {
        if (fd < 0 or fd >= 3) return fd;
        const high = dupCloexec(fd);
        _ = std.c.close(fd);
        return high;
    }

    /// Creates and unlinks the `.discard` capture file, `<TMPDIR or /tmp>/temporal-zig-server-
    /// output-<pid>-<seq>`: O_EXCL and O_NOFOLLOW so that no file or link someone else put
    /// there is used, mode 0600, up to 8 names tried. -1 when none could be made. Allocates
    /// nothing: the caller holds `redirect_mutex`.
    fn openCaptureFile() c_int {
        const dir: []const u8 = dir: {
            if (builtin.is_test) if (test_capture_dir) |d| break :dir std.mem.span(d);
            if (std.c.getenv("TMPDIR")) |d| {
                const trimmed = std.mem.trimEnd(u8, std.mem.span(d), "/");
                if (trimmed.len > 0) break :dir trimmed;
            }
            break :dir "/tmp";
        };
        var path_buf: [1024]u8 = undefined;
        for (0..8) |_| {
            capture_seq +%= 1;
            const path = std.fmt.bufPrintSentinel(&path_buf, "{s}/temporal-zig-server-output-{d}-{d}", .{ dir, std.c.getpid(), capture_seq }, 0) catch return -1;
            const flags: std.c.O = .{ .ACCMODE = .RDWR, .APPEND = true, .CREAT = true, .EXCL = true, .NOFOLLOW = true, .CLOEXEC = true };
            const fd = keepAbove2(std.c.open(path.ptr, flags, @as(c_uint, 0o600)));
            if (fd < 0) {
                if (std.c.errno(fd) == .EXIST) continue;
                return -1;
            }
            if (std.c.unlink(path.ptr) != 0) {
                // A named file would outlive the start; do without it.
                _ = std.c.close(fd);
                return -1;
            }
            return fd;
        }
        return -1;
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

fn finishStart(gpa: Allocator, runtime: Runtime, result: StartResult, tail: CaptureTail, failure: ?*?[]u8) StartError!EphemeralServer {
    defer if (result.fail) |f| f.deinit();
    defer if (result.target) |t| t.deinit();
    const server = result.server orelse {
        if (failure) |f| f.* = if (result.fail) |msg|
            failureMessage(gpa, msg.bytes(), tail) catch msg.dupe(gpa) catch null
        else
            null;
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

/// The core's start failure followed by what `tail` holds, in one of the forms
/// `ServerOutput.discard` documents; a copy of `core_message` alone for `.no_capture_file`.
fn failureMessage(gpa: Allocator, core_message: []const u8, tail: CaptureTail) error{OutOfMemory}![]u8 {
    const heading = "\nserver output (stdout and stderr";
    return switch (tail) {
        .no_capture_file => gpa.dupe(u8, core_message),
        .not_captured => std.mem.concat(gpa, u8, &.{ core_message, heading ++ "): not captured" }),
        .text => |t| if (t.total == 0)
            std.mem.concat(gpa, u8, &.{ core_message, heading ++ "): none; nothing was written before the start failed" })
        else if (t.total <= capture_tail_max)
            std.fmt.allocPrint(gpa, "{s}" ++ heading ++ ", {d} bytes):\n{s}", .{ core_message, t.total, t.bytes })
        else
            std.fmt.allocPrint(gpa, "{s}" ++ heading ++ ", the last {d} of {d} bytes):\n{s}", .{ core_message, t.bytes.len, t.total, t.bytes }),
    };
}

/// One attempt's result and, for a failed `.discard` start that was not a download failure,
/// the tail of the server's output, which points into the caller's buffer.
const Attempt = struct { result: StartResult, tail: CaptureTail };

/// Makes one start through the core and waits for its callback.
fn startOnce(gpa: Allocator, io: Io, runtime: Runtime, output: ServerOutput, start: Start, tail_buf: *[capture_tail_max]u8) error{OutOfMemory}!Attempt {
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
    const result = done.waitUncancelable();
    // The core kills the server and waits for it before it reports a failed start, so its
    // output is complete here. The capture file goes in `redirect.end()`, after this.
    const failed = result.server == null and if (result.fail) |f| !isDownloadFailure(f.bytes()) else false;
    return .{ .result = result, .tail = if (failed) redirect.readTail(tail_buf) else .no_capture_file };
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
    var tail_buf: [capture_tail_max]u8 = undefined;
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
        const once = try startOnce(gpa, io, runtime, output, start, &tail_buf);
        const result = once.result;
        // Decided on the core's own message: the server's output never makes a start retry.
        const retry = !last and result.server == null and
            if (result.fail) |f| isDownloadFailure(f.bytes()) else false;
        // A result with a server never retries, so freeing it below never drops a server.
        if (!retry or !backoff(io, attempt)) return finishStart(gpa, runtime, result, once.tail, failure);
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

/// Ports `pickTestServerPort` tries before it gives up. It moves on only while a socket holds
/// the port it was handed, so running out takes that many held ports in a row.
const max_port_picks = 64;

/// How long a probe waits on its own loopback connection: for it to reach the listener's queue,
/// and for the probe side's FIN to reach the listener's side before that side closes. Both
/// arrive at once unless something is wrong; the second waits out the limit only when a server
/// spawned meanwhile kept a copy of the probe socket, which then sends no FIN.
const probe_wait_ms = 1000;

// Test hook, read only when builtin.is_test. Tests that set it reset it.
/// When not 0, the first try of `pickTestServerPort` takes this port instead of one from the
/// IPv4 allocator, and the hook goes back to 0.
var test_first_probe_port: std.atomic.Value(u16) = .init(0);

/// The port for a time-skipping server asked for a free one: picked here on macOS, 0 (sdk-core
/// picks) elsewhere.
///
/// sdk-core picks by binding 0.0.0.0:0, and temporal-test-server then binds [::]:<port>
/// dual-stack. macOS's IPv4 allocator does not look at IPv6 sockets, so it can hand out a port
/// an IPv6 socket holds, such as a system daemon's connection on a link-local address
/// (corespeechd's and remoted's on anri0 held 55561 and 55560). The server's bind then fails
/// with EADDRINUSE, it logs a BindException and goes on running without listening, and sdk-core,
/// which does not watch the process, waits out its startup timeout on "connection refused".
/// Each try takes a port from the IPv4 allocator, as sdk-core did, and then binds [::]:<port>
/// the way the server will; a port that bind refuses is skipped.
///
/// Every probe socket is connected before it is closed. macOS has no SOCK_CLOEXEC and Rust's
/// spawn passes every fd without close-on-exec to the new process, so a server another thread
/// spawns between a probe's socket() and its fcntl() keeps a copy of it for as long as it runs.
/// A copy left bound to the port took the IPv4 connections meant for the server given that port
/// and never answered them (16 servers started at once failed that way 4 times in 300 runs;
/// sdk-core's own probe has the same window). A connected copy has its own peer, so it neither
/// blocks the server's [::] bind nor takes its connections. The probes connect to two loopback
/// listeners of this function's own and are accepted at once. Forking a child to probe in would
/// avoid the copies too, but fork() deadlocks under the thread sanitizer on macOS when another
/// thread is inside the system's random number generator, as sdk-core's threads often are.
///
/// Linux keeps one port space for both families, so sdk-core's probe already sees IPv6 sockets
/// there, and it leaves the port in TIME_WAIT so the allocator does not hand it out again before
/// the server binds. The probe here leaves the same TIME_WAIT on the port: its side of the IPv4
/// connection closes first.
fn pickTestServerPort(gpa: Allocator, failure: ?*?[]u8) error{StartFailed}!u16 {
    if (comptime !builtin.os.tag.isDarwin()) return 0;
    const probe = probePorts();
    if (probe == .free) return probe.free;
    if (failure) |f| f.* = switch (probe) {
        .free => unreachable,
        .taken => |port| std.fmt.allocPrint(gpa, "no free port for the time-skipping test server: binding [::]:<port> failed with EADDRINUSE on each of the {d} ports tried, the last {d}", .{ max_port_picks, port }),
        .failed => |call| std.fmt.allocPrint(gpa, "picking a port for the time-skipping test server failed: {s}: {t}", .{ call.name, call.err }),
    } catch null;
    return error.StartFailed;
}

const PortProbe = union(enum) {
    free: u16,
    /// The server's bind would fail on this port.
    taken: u16,
    failed: struct { name: []const u8, err: std.c.E },
};

/// A loopback listener the probe sockets connect to.
const ProbePeer = struct {
    fd: c_int,
    addr: std.c.sockaddr.in6,
    len: std.c.socklen_t,

    /// Listens on 127.0.0.1:0 (`std.c.AF.INET`) or [::1]:0 (`std.c.AF.INET6`).
    fn listen(domain: c_uint) union(enum) { peer: ProbePeer, failed: PortProbe } {
        const fd = socketCloexec(domain) orelse return .{ .failed = failedCall("socket") };
        var peer: ProbePeer = .{ .fd = fd, .addr = undefined, .len = @sizeOf(std.c.sockaddr.in6) };
        const ok = ok: {
            if (domain == std.c.AF.INET) {
                const addr: std.c.sockaddr.in = .{ .port = 0, .addr = std.mem.nativeToBig(u32, 0x7f00_0001) };
                if (std.c.bind(fd, @ptrCast(&addr), @sizeOf(std.c.sockaddr.in)) != 0) break :ok false;
            } else {
                var loopback: [16]u8 = @splat(0);
                loopback[15] = 1;
                const addr: std.c.sockaddr.in6 = .{ .port = 0, .flowinfo = 0, .addr = loopback, .scope_id = 0 };
                if (std.c.bind(fd, @ptrCast(&addr), @sizeOf(std.c.sockaddr.in6)) != 0) break :ok false;
            }
            // Non-blocking, so an accept after poll cannot wait on a connection that went away.
            break :ok std.c.fcntl(fd, std.c.F.SETFL, @as(c_int, @bitCast(std.c.O{ .NONBLOCK = true }))) >= 0 and
                std.c.listen(fd, 8) == 0 and std.c.getsockname(fd, @ptrCast(&peer.addr), &peer.len) == 0;
        };
        if (!ok) {
            const probe = failedCall("listen on loopback");
            _ = std.c.close(fd);
            return .{ .failed = probe };
        }
        return .{ .peer = peer };
    }

    /// Connects `fd` to this listener and accepts the connection, which is closed when the
    /// returned fd is. A signal does not fail it (see `waitFor`).
    fn connect(peer: *const ProbePeer, fd: c_int) union(enum) { accepted: c_int, failed: PortProbe } {
        const deadline = probeDeadline();
        if (std.c.connect(fd, @ptrCast(&peer.addr), peer.len) != 0) {
            switch (std.c.errno(@as(c_int, -1))) {
                // macOS's connect returns EINTR on a signal even with SA_RESTART, and the
                // connection goes on; a repeated call says EALREADY or EISCONN. Wait for it and
                // read its outcome instead.
                .INTR, .ALREADY, .INPROGRESS => {
                    const err = waitFor(fd, std.c.POLL.OUT, deadline);
                    if (err != .SUCCESS) return .{ .failed = .{ .failed = .{ .name = "connect to the probe listener", .err = err } } };
                    var so_error: c_int = 0;
                    var len: std.c.socklen_t = @sizeOf(c_int);
                    if (std.c.getsockopt(fd, std.c.SOL.SOCKET, std.c.SO.ERROR, &so_error, &len) != 0) return .{ .failed = failedCall("getsockopt(SO_ERROR)") };
                    if (so_error != 0) return .{ .failed = .{ .failed = .{ .name = "connect to the probe listener", .err = @fromBackingInt(@intCast(@as(u16, @intCast(so_error)))) } } };
                },
                .ISCONN => {},
                else => |err| return .{ .failed = .{ .failed = .{ .name = "connect to the probe listener", .err = err } } },
            }
        }
        while (true) {
            const err = waitFor(peer.fd, std.c.POLL.IN, deadline);
            if (err != .SUCCESS) return .{ .failed = .{ .failed = .{ .name = "waiting for the probe connection", .err = err } } };
            const accepted = std.c.accept(peer.fd, null, null);
            if (accepted >= 0) {
                // A copy a spawned server keeps would hold the connection open for nothing.
                _ = std.c.fcntl(accepted, std.c.F.SETFD, @as(c_int, std.c.FD_CLOEXEC));
                return .{ .accepted = accepted };
            }
            switch (std.c.errno(@as(c_int, -1))) {
                .INTR, .AGAIN, .CONNABORTED => continue,
                else => |e| return .{ .failed = .{ .failed = .{ .name = "accept on the probe listener", .err = e } } },
            }
        }
    }
};

/// The end of a probe's waits on one of its connections: `probe_wait_ms` from now.
fn probeDeadline() i64 {
    return monotonicMs() + probe_wait_ms;
}

fn monotonicMs() i64 {
    var ts: std.c.timespec = undefined;
    // Fails only for an unknown clock.
    std.debug.assert(std.c.clock_gettime(.MONOTONIC, &ts) == 0);
    return @as(i64, ts.sec) * std.time.ms_per_s + @divTrunc(@as(i64, ts.nsec), std.time.ns_per_ms);
}

/// Waits until `fd` is ready for `events` or `deadline` passes (`.TIMEDOUT`). A signal resumes
/// the wait with the time left: poll is never restarted after a handler, and SIGCHLD, which
/// sdk-core's runtime handles, comes whenever any child of the process exits.
fn waitFor(fd: c_int, events: i16, deadline: i64) std.c.E {
    while (true) {
        const left = deadline - monotonicMs();
        if (left <= 0) return .TIMEDOUT;
        var pfd = [_]std.c.pollfd{.{ .fd = fd, .events = events, .revents = 0 }};
        const n = std.c.poll(&pfd, 1, @intCast(left));
        if (n > 0) return .SUCCESS;
        if (n == 0) return .TIMEDOUT;
        const err = std.c.errno(n);
        if (err != .INTR) return err;
    }
}

fn probePorts() PortProbe {
    const v4 = switch (ProbePeer.listen(std.c.AF.INET)) {
        .peer => |p| p,
        .failed => |probe| return probe,
    };
    defer _ = std.c.close(v4.fd);
    const v6 = switch (ProbePeer.listen(std.c.AF.INET6)) {
        .peer => |p| p,
        .failed => |probe| return probe,
    };
    defer _ = std.c.close(v6.fd);
    var probe: PortProbe = undefined;
    for (0..max_port_picks) |i| {
        const first: u16 = if (builtin.is_test and i == 0) test_first_probe_port.swap(0, .monotonic) else 0;
        probe = probePort(&v4, &v6, first);
        if (probe != .taken) break;
    }
    return probe;
}

/// One try of `probePorts`: a port from the IPv4 allocator (or `port` when not 0), then the
/// server's bind on it. Its sockets are connected to `v4` and `v6` and closed before it returns.
fn probePort(v4: *const ProbePeer, v6: *const ProbePeer, port_in: u16) PortProbe {
    // The IPv4 allocator never hands out a port an IPv4 socket holds. The IPv6 one does (it
    // does not look at IPv4 sockets), and under load it walked through runs of ports held by
    // loopback connections, so the port comes from the IPv4 side, as sdk-core's did: the
    // connect binds the socket to a free port.
    const s4 = socketCloexec(std.c.AF.INET) orelse return failedCall("socket(AF_INET)");
    const a4 = accepted: {
        if (port_in != 0) {
            const addr: std.c.sockaddr.in = .{ .port = std.mem.nativeToBig(u16, port_in), .addr = 0 };
            if (std.c.bind(s4, @ptrCast(&addr), @sizeOf(std.c.sockaddr.in)) != 0) return closeAfter(s4, failedCall("bind(0.0.0.0:<port>)"));
        }
        switch (v4.connect(s4)) {
            .accepted => |fd| break :accepted fd,
            .failed => |probe| return closeAfter(s4, probe),
        }
    };
    defer closePair(s4, a4);
    const port = port: {
        var addr: std.c.sockaddr.in = undefined;
        var len: std.c.socklen_t = @sizeOf(std.c.sockaddr.in);
        if (std.c.getsockname(s4, @ptrCast(&addr), &len) != 0) return failedCall("getsockname");
        break :port std.mem.bigToNative(u16, addr.port);
    };
    // Then the bind temporal-test-server makes: [::]:<port>, dual-stack, SO_REUSEADDR.
    const s6 = socketCloexec(std.c.AF.INET6) orelse return failedCall("socket(AF_INET6)");
    const off: c_int = 0;
    const on: c_int = 1;
    if (std.c.setsockopt(s6, std.c.IPPROTO.IPV6, std.c.IPV6.V6ONLY, &off, @sizeOf(c_int)) != 0) return closeAfter(s6, failedCall("setsockopt(IPV6_V6ONLY)"));
    if (std.c.setsockopt(s6, std.c.SOL.SOCKET, std.c.SO.REUSEADDR, &on, @sizeOf(c_int)) != 0) return closeAfter(s6, failedCall("setsockopt(SO_REUSEADDR)"));
    const addr: std.c.sockaddr.in6 = .{ .port = std.mem.nativeToBig(u16, port), .flowinfo = 0, .addr = @splat(0), .scope_id = 0 };
    if (std.c.bind(s6, @ptrCast(&addr), @sizeOf(std.c.sockaddr.in6)) != 0) {
        const err = std.c.errno(@as(c_int, -1));
        return closeAfter(s6, if (err == .ADDRINUSE) .{ .taken = port } else .{ .failed = .{ .name = "bind([::]:<port>)", .err = err } });
    }
    const a6 = switch (v6.connect(s6)) {
        .accepted => |fd| fd,
        .failed => |probe| return closeAfter(s6, probe),
    };
    closePair(s6, a6);
    return .{ .free = port };
}

/// Closes a probe socket and then, once its FIN has arrived, the listener's side of its
/// connection. Only the probe's side then goes through TIME_WAIT, on the picked port, which the
/// server's SO_REUSEADDR bind passes and which keeps the IPv4 allocator from handing the port
/// out again meanwhile, as sdk-core's reservation did. Closing both at once made both sides
/// close actively, and each try then also held a listener's port for 30 s; with many servers
/// starting, that ran the ephemeral range out (EADDRNOTAVAIL).
fn closePair(probe_fd: c_int, accepted_fd: c_int) void {
    _ = std.c.close(probe_fd);
    _ = waitFor(accepted_fd, std.c.POLL.IN, probeDeadline());
    _ = std.c.close(accepted_fd);
}

/// Closes `fd` and returns `probe`, whose errno was read before the close.
fn closeAfter(fd: c_int, probe: PortProbe) PortProbe {
    _ = std.c.close(fd);
    return probe;
}

/// A TCP socket with close-on-exec set right after it is made (macOS has no SOCK_CLOEXEC).
/// Null with errno set on failure.
fn socketCloexec(domain: c_uint) ?c_int {
    const fd = std.c.socket(domain, std.c.SOCK.STREAM, 0);
    if (fd < 0) return null;
    if (std.c.fcntl(fd, std.c.F.SETFD, @as(c_int, std.c.FD_CLOEXEC)) < 0) {
        const saved = std.c._errno().*;
        _ = std.c.close(fd);
        std.c._errno().* = saved;
        return null;
    }
    return fd;
}

fn failedCall(name: []const u8) PortProbe {
    return .{ .failed = .{ .name = name, .err = std.c.errno(@as(c_int, -1)) } };
}

pub const ReservePortError = error{ReservePortFailed};

/// A port on 127.0.0.1 for a server started next, such as the dev server's `--http-port` or the
/// core's Prometheus exporter, which bind it with SO_REUSEADDR (Go's and Rust's listeners
/// do). The port is left in TIME_WAIT, as sdk-core's `get_free_port` leaves the gRPC port it
/// hands the dev server: until the server binds it, the port allocator hands it to no other
/// bind(port 0) or connect, while the server's SO_REUSEADDR bind passes. A port only bound and
/// closed is free again at once, and under the parallel tests' load another socket took it
/// before the dev server bound it; the server then exited on EADDRINUSE and sdk-core waited out
/// its startup timeout on "connection refused". The reservation lasts the TIME_WAIT (60 s on
/// Linux, 30 s on macOS). On failure, logs the call that failed with its errno.
pub fn reserveLoopbackPort() ReservePortError!u16 {
    const probe = if (comptime builtin.os.tag.isDarwin()) reserveByConnect() else reserveByAccept();
    switch (probe) {
        .free => |port| return port,
        .taken => unreachable,
        .failed => |call| {
            std.log.scoped(.temporal).warn("reserving a port on 127.0.0.1 failed: {s}: {t}", .{ call.name, call.err });
            return error.ReservePortFailed;
        },
    }
}

/// Linux: sdk-core's reservation. A listener on 127.0.0.1:0 with SO_REUSEADDR, a connection to
/// it, and the accepted side closes first, so the TIME_WAIT is on the listener's port with
/// SO_REUSEADDR, inherited. Linux's connect skips a port a bind created; a TIME_WAIT left by a
/// connect would not stop another connect to a different peer from sharing its port. Every fd
/// is close-on-exec from the start, so a server spawned meanwhile holds no copy.
fn reserveByAccept() PortProbe {
    const listener = std.c.socket(std.c.AF.INET, std.c.SOCK.STREAM | std.c.SOCK.CLOEXEC, 0);
    if (listener < 0) return failedCall("socket(AF_INET)");
    defer _ = std.c.close(listener);
    const on: c_int = 1;
    if (std.c.setsockopt(listener, std.c.SOL.SOCKET, std.c.SO.REUSEADDR, &on, @sizeOf(c_int)) != 0) return failedCall("setsockopt(SO_REUSEADDR)");
    var addr: std.c.sockaddr.in = .{ .port = 0, .addr = std.mem.nativeToBig(u32, 0x7f00_0001) };
    if (std.c.bind(listener, @ptrCast(&addr), @sizeOf(std.c.sockaddr.in)) != 0) return failedCall("bind(127.0.0.1:0)");
    if (std.c.listen(listener, 1) != 0) return failedCall("listen");
    var len: std.c.socklen_t = @sizeOf(std.c.sockaddr.in);
    if (std.c.getsockname(listener, @ptrCast(&addr), &len) != 0) return failedCall("getsockname");

    const client = std.c.socket(std.c.AF.INET, std.c.SOCK.STREAM | std.c.SOCK.CLOEXEC, 0);
    if (client < 0) return failedCall("socket(AF_INET)");
    defer _ = std.c.close(client);
    // Blocking: a loopback connect to a listener with room in its queue completes at once.
    while (std.c.connect(client, @ptrCast(&addr), @sizeOf(std.c.sockaddr.in)) != 0) switch (std.c.errno(@as(c_int, -1))) {
        .INTR => continue,
        // An interrupted connect goes on; a repeated call reports it done.
        .ISCONN => break,
        else => return failedCall("connect to the reserving listener"),
    };
    const accepted = while (true) {
        const fd = std.c.accept4(listener, null, null, std.c.SOCK.CLOEXEC);
        if (fd >= 0) break fd;
        if (std.c.errno(fd) != .INTR) return failedCall("accept on the reserving listener");
    };
    defer _ = std.c.close(accepted);
    // The accepted side's FIN goes first; once the client's answering FIN is in, that side is
    // in TIME_WAIT before the port is returned.
    if (std.c.shutdown(accepted, std.c.SHUT.WR) != 0) return failedCall("shutdown(accepted)");
    if (waitFor(client, std.c.POLL.IN, probeDeadline()) != .SUCCESS) return failedCall("waiting for the accepted side's FIN");
    if (std.c.shutdown(client, std.c.SHUT.WR) != 0) return failedCall("shutdown(client)");
    if (waitFor(accepted, std.c.POLL.IN, probeDeadline()) != .SUCCESS) return failedCall("waiting for the client's FIN");
    return .{ .free = std.mem.bigToNative(u16, addr.port) };
}

/// macOS: the IPv4 probe of `pickTestServerPort`, whose socket closes first and so leaves its
/// TIME_WAIT on the port its connect was given. macOS has no SOCK_CLOEXEC, and a listener a
/// spawned server kept a copy of would take the connections meant for that server (see
/// `pickTestServerPort`); a connected copy does not.
fn reserveByConnect() PortProbe {
    const v4 = switch (ProbePeer.listen(std.c.AF.INET)) {
        .peer => |p| p,
        .failed => |probe| return probe,
    };
    defer _ = std.c.close(v4.fd);
    const s4 = socketCloexec(std.c.AF.INET) orelse return failedCall("socket(AF_INET)");
    const a4 = switch (v4.connect(s4)) {
        .accepted => |fd| fd,
        .failed => |probe| return closeAfter(s4, probe),
    };
    defer closePair(s4, a4);
    var addr: std.c.sockaddr.in = undefined;
    var len: std.c.socklen_t = @sizeOf(std.c.sockaddr.in);
    if (std.c.getsockname(s4, @ptrCast(&addr), &len) != 0) return failedCall("getsockname");
    return .{ .free = std.mem.bigToNative(u16, addr.port) };
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

/// Binds a fresh TCP socket to 127.0.0.1:`port`, with SO_REUSEADDR when `reuse`; the errno.
fn bindLoopback(port: u16, reuse: bool) std.c.E {
    const fd = std.c.socket(std.c.AF.INET, std.c.SOCK.STREAM, 0);
    std.debug.assert(fd >= 0);
    defer _ = std.c.close(fd);
    const on: c_int = 1;
    if (reuse) std.debug.assert(std.c.setsockopt(fd, std.c.SOL.SOCKET, std.c.SO.REUSEADDR, &on, @sizeOf(c_int)) == 0);
    const addr: std.c.sockaddr.in = .{ .port = std.mem.nativeToBig(u16, port), .addr = std.mem.nativeToBig(u32, 0x7f00_0001) };
    if (std.c.bind(fd, @ptrCast(&addr), @sizeOf(std.c.sockaddr.in)) != 0) return std.c.errno(@as(c_int, -1));
    if (std.c.listen(fd, 1) != 0) return std.c.errno(@as(c_int, -1));
    return .SUCCESS;
}

test "a reserved loopback port refuses other binds and takes the SO_REUSEADDR bind of the server it is for" {
    // The race the reservation closes: a port that was only bound and closed is free at once,
    // and a bind (or connect) elsewhere took the dev server's --http-port before the server
    // bound it. While reserved, a bind without SO_REUSEADDR is refused, as the allocator's own
    // choice of a port is; the server's bind, which sets it, succeeds.
    const port = try reserveLoopbackPort();
    try testing.expectEqual(std.c.E.ADDRINUSE, bindLoopback(port, false));
    try testing.expectEqual(std.c.E.SUCCESS, bindLoopback(port, true));
}

test "a dev server serves its HTTP API on a reserved port" {
    const rt = try Runtime.init(testing.allocator, .{}, null);
    defer rt.deinit();
    const port = try reserveLoopbackPort();
    var port_text: [8]u8 = undefined;
    var failure: ?[]u8 = null;
    defer if (failure) |f| testing.allocator.free(f);
    var server = startDevServer(testing.allocator, testing.io, rt, .{
        .extra_args = &.{ "--http-port", try std.fmt.bufPrint(&port_text, "{d}", .{port}) },
    }, &failure) catch |err| {
        std.debug.print("dev server failed to start: {s}\n", .{failure orelse "(no message)"});
        return err;
    };
    defer server.shutdown(testing.io, null) catch {};
    // The server holds the port now.
    try testing.expectEqual(std.c.E.ADDRINUSE, bindLoopback(port, true));
}

test "a missing executable fails with the core's message" {
    const rt = try Runtime.init(testing.allocator, .{}, null);
    defer rt.deinit();
    var failure: ?[]u8 = null;
    defer if (failure) |f| testing.allocator.free(f);
    try testing.expectError(error.StartFailed, startDevServer(testing.allocator, testing.io, rt, .{
        .executable = .{ .existing_path = "/nonexistent/temporal" },
    }, &failure));
    try testing.expect(failure != null);
    // The core's message, then the capture file's report: nothing was ever spawned.
    const none = "\nserver output (stdout and stderr): none; nothing was written before the start failed";
    try testing.expect(std.mem.endsWith(u8, failure.?, none));
    try testing.expect(failure.?.len > none.len);
}

test "a dev server start on a port another socket holds reports the server's own error" {
    // The CLI exits at once on the taken port and sdk-core, which does not watch it, reports
    // only its 5 s timeout; the server's own words come from the capture file.
    const listener = socketCloexec(std.c.AF.INET) orelse return error.TestUnexpectedResult;
    defer _ = std.c.close(listener);
    var addr: std.c.sockaddr.in = .{ .port = 0, .addr = std.mem.nativeToBig(u32, 0x7f00_0001) };
    var len: std.c.socklen_t = @sizeOf(std.c.sockaddr.in);
    try testing.expectEqual(0, std.c.bind(listener, @ptrCast(&addr), len));
    try testing.expectEqual(0, std.c.listen(listener, 1));
    try testing.expectEqual(0, std.c.getsockname(listener, @ptrCast(&addr), &len));

    const rt = try Runtime.init(testing.allocator, .{}, null);
    defer rt.deinit();
    var failure: ?[]u8 = null;
    defer if (failure) |f| testing.allocator.free(f);
    try testing.expectError(error.StartFailed, startDevServer(testing.allocator, testing.io, rt, .{
        .port = std.mem.bigToNative(u16, addr.port),
    }, &failure));
    const message = failure orelse return error.TestUnexpectedResult;
    errdefer std.debug.print("failure: {s}\n", .{message});
    try testing.expect(std.mem.indexOf(u8, message, "\nserver output (stdout and stderr") != null);
    try testing.expect(std.mem.indexOf(u8, message, "address already in use") != null or
        std.mem.indexOf(u8, message, "can't set frontend port") != null);
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

/// One of the concurrent starts of "time-skipping servers started at once all accept connections".
const ConcurrentStart = struct {
    rt: Runtime,
    server: ?EphemeralServer = null,
    failure: ?[]u8 = null,
    err: ?StartError = null,

    fn run(s: *ConcurrentStart) void {
        s.server = startTestServer(testing.allocator, testing.io, s.rt, .{}, &s.failure) catch |err| {
            s.err = err;
            return;
        };
    }
};

test "time-skipping servers started at once all accept connections" {
    const rt = try Runtime.init(testing.allocator, .{}, null);
    defer rt.deinit();
    var starts: [16]ConcurrentStart = @splat(.{ .rt = rt });
    var threads: [starts.len]?std.Thread = @splat(null);
    defer for (&starts) |*s| {
        if (s.server) |*server| server.shutdown(testing.io, null) catch {};
        if (s.failure) |f| testing.allocator.free(f);
    };
    for (&threads, &starts) |*t, *s| t.* = std.Thread.spawn(.{}, ConcurrentStart.run, .{s}) catch null;
    for (&threads, &starts) |t, *s| {
        if (t) |thread| thread.join() else s.run();
    }
    for (&starts, 0..) |*s, i| {
        const server = s.server orelse {
            std.debug.print("test server {d} failed to start: {s}\n", .{ i, s.failure orelse @errorName(s.err.?) });
            return error.TestUnexpectedResult;
        };
        const address = try Io.net.IpAddress.parseLiteral(server.target);
        const stream = try address.connect(testing.io, .{ .mode = .stream });
        stream.close(testing.io);
        // Two starts handed the same port would share one server.
        for (starts[0..i]) |other| try testing.expect(!std.mem.eql(u8, other.server.?.target, server.target));
    }
}

test "a time-skipping server starts while an IPv6-only socket holds the port the IPv4 probe picks" {
    // macOS's IPv4 allocator does not look at IPv6 sockets, so sdk-core's 0.0.0.0:0 probe could
    // hand out a port an IPv6 socket held, as a system daemon's did, and the server's [::] bind
    // then failed (see pickTestServerPort). The hook hands the probe such a port first, so the
    // test needs no guess at what the allocator gives next and takes no port another process
    // may have just been given. Linux keeps one port space for both families.
    if (!builtin.os.tag.isDarwin()) return error.SkipZigTest;
    // A port the IPv4 allocator gives, then held by an IPv6-only socket, as the daemon's was.
    var holder: c_int = -1;
    defer if (holder >= 0) {
        _ = std.c.close(holder);
    };
    var held: u16 = 0;
    for (0..16) |_| {
        const v4 = std.c.socket(std.c.AF.INET, std.c.SOCK.STREAM, 0);
        try testing.expect(v4 >= 0);
        var addr4: std.c.sockaddr.in = .{ .port = 0, .addr = 0 };
        var len4: std.c.socklen_t = @sizeOf(std.c.sockaddr.in);
        const got = std.c.bind(v4, @ptrCast(&addr4), len4) == 0 and std.c.getsockname(v4, @ptrCast(&addr4), &len4) == 0;
        _ = std.c.close(v4);
        try testing.expect(got);
        const port = std.mem.bigToNative(u16, addr4.port);
        const fd = std.c.socket(std.c.AF.INET6, std.c.SOCK.STREAM, 0);
        try testing.expect(fd >= 0);
        const on: c_int = 1;
        const addr6: std.c.sockaddr.in6 = .{ .port = std.mem.nativeToBig(u16, port), .flowinfo = 0, .addr = @splat(0), .scope_id = 0 };
        if (std.c.setsockopt(fd, std.c.IPPROTO.IPV6, std.c.IPV6.V6ONLY, &on, @sizeOf(c_int)) == 0 and
            std.c.bind(fd, @ptrCast(&addr6), @sizeOf(std.c.sockaddr.in6)) == 0)
        {
            holder = fd;
            held = port;
            break;
        }
        _ = std.c.close(fd);
    }
    try testing.expect(holder >= 0);
    test_first_probe_port.store(held, .monotonic);
    defer test_first_probe_port.store(0, .monotonic);

    const rt = try Runtime.init(testing.allocator, .{}, null);
    defer rt.deinit();
    var failure: ?[]u8 = null;
    defer if (failure) |f| testing.allocator.free(f);
    var server = startTestServer(testing.allocator, testing.io, rt, .{}, &failure) catch |err| {
        std.debug.print("test server failed to start: {s}\n", .{failure orelse "(no message)"});
        return err;
    };
    defer server.shutdown(testing.io, null) catch {};
    try testing.expectEqual(0, test_first_probe_port.load(.monotonic));
    const address = try Io.net.IpAddress.parseLiteral(server.target);
    try testing.expect(address.getPort() != held);
    const stream = try address.connect(testing.io, .{ .mode = .stream });
    stream.close(testing.io);
}

/// Draws random bytes until `stop` is set, as sdk-core's tokio workers do while servers start.
fn drawRandomUntil(stop: *std.atomic.Value(bool)) void {
    var buf: [16]u8 = undefined;
    while (!stop.load(.monotonic)) std.c.arc4random_buf(&buf, buf.len);
}

test "a time-skipping server starts while another thread draws random bytes" {
    // sdk-core's threads draw random bytes (through the system's locked RNG) while a server
    // starts. Starting must not depend on that RNG lock being free: under `zig build tsan` on
    // macOS, a fork() in the start path deadlocked with such a thread (fork's atfork handler
    // waits for the RNG lock, which the drawing thread holds while it waits for the sanitizer
    // lock fork took), and the start never returned.
    if (!builtin.os.tag.isDarwin()) return error.SkipZigTest;
    const rt = try Runtime.init(testing.allocator, .{}, null);
    defer rt.deinit();
    var stop: std.atomic.Value(bool) = .init(false);
    const drawer = try std.Thread.spawn(.{}, drawRandomUntil, .{&stop});
    defer {
        stop.store(true, .monotonic);
        drawer.join();
    }
    for (0..3) |_| {
        var failure: ?[]u8 = null;
        defer if (failure) |f| testing.allocator.free(f);
        var server = startTestServer(testing.allocator, testing.io, rt, .{}, &failure) catch |err| {
            std.debug.print("test server failed to start: {s}\n", .{failure orelse "(no message)"});
            return err;
        };
        try server.shutdown(testing.io, null);
    }
}

fn ignoreSignal(_: std.posix.SIG) callconv(.c) void {}

/// Sends SIGWINCH to `target` until `stop` is set.
fn signalUntil(target: std.c.pthread_t, stop: *std.atomic.Value(bool)) void {
    while (!stop.load(.monotonic)) _ = std.c.pthread_kill(target, .WINCH);
}

test "a time-skipping server starts while signals interrupt the starting thread" {
    // A signal with a handler can arrive while a server starts: sdk-core's tokio installs one
    // for SIGCHLD (with SA_RESTART) once it has spawned a server, so any child of the process
    // that exits sends one, and programs handle SIGWINCH, SIGALRM and others. SA_RESTART does
    // not restart an interrupted connect() or poll(), which then fail with EINTR; a start
    // must not fail because of that. The signals are sent in a loop so that one lands inside
    // the start's blocking calls every time.
    const act: std.posix.Sigaction = .{ .handler = .{ .handler = ignoreSignal }, .mask = std.posix.sigemptyset(), .flags = std.posix.SA.RESTART };
    var old: std.posix.Sigaction = undefined;
    std.posix.sigaction(.WINCH, &act, &old);
    defer std.posix.sigaction(.WINCH, &old, null);

    const rt = try Runtime.init(testing.allocator, .{}, null);
    defer rt.deinit();
    var stop: std.atomic.Value(bool) = .init(false);
    const sender = try std.Thread.spawn(.{}, signalUntil, .{ std.c.pthread_self(), &stop });
    var joined = false;
    defer if (!joined) {
        stop.store(true, .monotonic);
        sender.join();
    };
    var failure: ?[]u8 = null;
    defer if (failure) |f| testing.allocator.free(f);
    var server = startTestServer(testing.allocator, testing.io, rt, .{}, &failure) catch |err| {
        std.debug.print("test server failed to start: {s}\n", .{failure orelse "(no message)"});
        return err;
    };
    defer server.shutdown(testing.io, null) catch {};
    stop.store(true, .monotonic);
    sender.join();
    joined = true;
    const address = try Io.net.IpAddress.parseLiteral(server.target);
    const stream = try address.connect(testing.io, .{ .mode = .stream });
    stream.close(testing.io);
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
            redirect_state.saved_out == -1 and redirect_state.saved_err == -1 and redirect_state.capture_fd == -1;
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
        const capture = redirect_state.capture_fd;
        // A `.discard` start that fell back to `.stderr` saves no copy of fd 2 and fails here,
        // and one that fell back to /dev/null has no capture file.
        var ok = saved_out >= 0 and (if (mode == .discard) saved_err >= 0 and capture >= 3 else saved_err == -1 and capture == -1);
        for ([_]c_int{ saved_out, saved_err, capture }) |fd| {
            if (fd < 0) continue;
            const flags = std.c.fcntl(fd, std.c.F.GETFD);
            ok = ok and flags >= 0 and flags & std.c.FD_CLOEXEC != 0;
        }
        r.end();
        ok = ok and fdIs(1, before_out) and fdIs(2, before_err) and redirect_state.capture_fd == -1 and
            (capture < 0 or std.c.fcntl(capture, std.c.F.GETFD) < 0);
        if (!ok) {
            _ = std.c.dup2(repair_out, 1);
            _ = std.c.dup2(repair_err, 2);
            return error.TestUnexpectedResult;
        }
    }
}

/// Closes `fd` (1 or 2), runs a `.discard` redirect and checks that the capture file is on fds 1
/// and 2 meanwhile (not close-on-exec there, so a server inherits it) and that afterwards `fd`
/// is closed again and the other fd is what it was.
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

    // No `try` from here until the repair.
    _ = std.c.close(fd);
    const r: OutputRedirect = .begin(.discard);
    const capture = redirect_state.capture_fd;
    const capture_id = if (capture >= 3) fdId(capture) else null;
    var ok = capture_id != null and fdIs(1, capture_id.?) and fdIs(2, capture_id.?);
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

test "a discard redirect keeps a closed fd 1 closed and puts the capture file on it meanwhile" {
    try checkDiscardWithClosed(1);
}

test "a discard redirect keeps a closed fd 2 closed and puts the capture file on it meanwhile" {
    try checkDiscardWithClosed(2);
}

/// How many names `fd`'s file has: 0 for an unlinked one. Null when it cannot be read.
fn linkCount(fd: c_int) ?u64 {
    if (builtin.os.tag == .linux) {
        const linux = std.os.linux;
        var stx: linux.Statx = undefined;
        if (linux.errno(linux.statx(fd, "", linux.AT.EMPTY_PATH, .{ .NLINK = true }, &stx)) != .SUCCESS) return null;
        if (!stx.mask.NLINK) return null;
        return stx.nlink;
    }
    var st: std.c.Stat = undefined;
    if (std.c.fstat(fd, &st) != 0) return null;
    return @intCast(st.nlink);
}

/// Runs `body` inside a `.discard` redirect with repair copies of fds 1 and 2, which are put
/// back whatever `body` returns, so the runner's stdout protocol and stderr survive a failure.
/// `body` gets the redirect and must not `end` it.
fn withDiscardRedirect(body: anytype, args: anytype) !void {
    const repair_out = std.c.dup(1);
    try testing.expect(repair_out >= 0);
    defer _ = std.c.close(repair_out);
    const repair_err = std.c.dup(2);
    try testing.expect(repair_err >= 0);
    defer _ = std.c.close(repair_err);
    const r: OutputRedirect = .begin(.discard);
    const result = @call(.auto, body, .{&r} ++ args);
    r.end();
    _ = std.c.dup2(repair_out, 1);
    _ = std.c.dup2(repair_err, 2);
    return result;
}

fn writeAll(fd: c_int, data: []const u8) !void {
    var rest = data;
    while (rest.len > 0) {
        const n = std.c.write(fd, rest.ptr, rest.len);
        if (n <= 0) return error.TestUnexpectedResult;
        rest = rest[@intCast(n)..];
    }
}

fn checkCapturesInOrder(r: *const OutputRedirect) !void {
    const capture = redirect_state.capture_fd;
    if (capture < 3) return error.TestUnexpectedResult;
    // Unlinked from the start, so a crashed process leaves no file behind.
    if (linkCount(capture) != 0) return error.TestUnexpectedResult;
    try writeAll(1, "to stdout\n");
    try writeAll(2, "to stderr\n");
    try writeAll(1, "stdout again\n\n");
    var buf: [capture_tail_max]u8 = undefined;
    const tail = r.readTail(&buf);
    if (tail != .text) return error.TestUnexpectedResult;
    // Threads left over from earlier tests may log meanwhile, and their lines land in the
    // file too; this test's writes must be there in order, and the trailing blank line gone.
    if (tail.text.total < 34) return error.TestUnexpectedResult;
    var at: usize = 0;
    for ([_][]const u8{ "to stdout\n", "to stderr\n", "stdout again" }) |piece| {
        at = (std.mem.indexOfPos(u8, tail.text.bytes, at, piece) orelse return error.TestUnexpectedResult) + piece.len;
    }
    if (std.ascii.isWhitespace(tail.text.bytes[tail.text.bytes.len - 1])) return error.TestUnexpectedResult;
}

test "a discard redirect captures writes to fds 1 and 2 in order in an unlinked file" {
    try withDiscardRedirect(checkCapturesInOrder, .{});
    try testing.expectEqual(-1, redirect_state.capture_fd);
}

fn checkLongTail(r: *const OutputRedirect) !void {
    // 300 lines of 20 bytes: 6000 bytes, more than the tail holds.
    var line_buf: [32]u8 = undefined;
    for (0..300) |i| try writeAll(if (i % 2 == 0) 1 else 2, std.fmt.bufPrint(&line_buf, "line {d:0>4} of output\n", .{i}) catch unreachable);
    var buf: [capture_tail_max]u8 = undefined;
    const tail = r.readTail(&buf);
    if (tail != .text) return error.TestUnexpectedResult;
    const text = tail.text.bytes;
    if (tail.text.total < 6000 or text.len > capture_tail_max) return error.TestUnexpectedResult;
    if (std.mem.indexOf(u8, text, "line 0299 of output") == null) return error.TestUnexpectedResult;
    // Threads left over from earlier tests may log into the file too, so the expected cut is
    // worked out from the whole file: a suffix of it without trailing whitespace, starting
    // right after a newline.
    const file = try testing.allocator.alloc(u8, @intCast(tail.text.total));
    defer testing.allocator.free(file);
    if (std.c.pread(redirect_state.capture_fd, file.ptr, file.len, 0) != file.len) return error.TestUnexpectedResult;
    const whole = std.mem.trimEnd(u8, file, &std.ascii.whitespace);
    if (!std.mem.endsWith(u8, whole, text) or whole.len == text.len) return error.TestUnexpectedResult;
    if (whole[whole.len - text.len - 1] != '\n') return error.TestUnexpectedResult;
    // The cut drops at most one line's start: these lines are 20 bytes.
    if (file.len - (whole.len - text.len) > capture_tail_max + 20) return error.TestUnexpectedResult;
}

test "a capture tail over 4096 bytes starts at a line start" {
    try withDiscardRedirect(checkLongTail, .{});
}

test "failure messages give the server output in each form" {
    const gpa = testing.allocator;
    const core = "Connection failed: ephemeral server did not start";
    const cases = [_]struct { tail: CaptureTail, want: []const u8 }{
        .{ .tail = .no_capture_file, .want = core },
        .{ .tail = .not_captured, .want = core ++ "\nserver output (stdout and stderr): not captured" },
        .{ .tail = .{ .text = .{ .bytes = "", .total = 0 } }, .want = core ++ "\nserver output (stdout and stderr): none; nothing was written before the start failed" },
        .{ .tail = .{ .text = .{ .bytes = "Error: bind", .total = 12 } }, .want = core ++ "\nserver output (stdout and stderr, 12 bytes):\nError: bind" },
        .{ .tail = .{ .text = .{ .bytes = "last line", .total = 9000 } }, .want = core ++ "\nserver output (stdout and stderr, the last 9 of 9000 bytes):\nlast line" },
    };
    for (cases) |case| {
        const got = try failureMessage(gpa, core, case.tail);
        defer gpa.free(got);
        try testing.expectEqualStrings(case.want, got);
    }
}

fn checkFallbackToNull(r: *const OutputRedirect, null_id: FdId) !void {
    if (redirect_state.capture_fd != -1) return error.TestUnexpectedResult;
    if (!fdIs(1, null_id) or !fdIs(2, null_id)) return error.TestUnexpectedResult;
    var buf: [capture_tail_max]u8 = undefined;
    const tail = r.readTail(&buf);
    if (tail != .not_captured) return error.TestUnexpectedResult;
    const message = try failureMessage(testing.allocator, "core", tail);
    defer testing.allocator.free(message);
    if (!std.mem.eql(u8, message, "core\nserver output (stdout and stderr): not captured")) return error.TestUnexpectedResult;
}

test "a discard redirect whose capture file cannot be made falls back to /dev/null" {
    const null_id = null_id: {
        const n = std.c.open("/dev/null", .{ .ACCMODE = .RDONLY, .CLOEXEC = true });
        try testing.expect(n >= 0);
        defer _ = std.c.close(n);
        break :null_id fdId(n) orelse return error.TestUnexpectedResult;
    };
    test_capture_dir = "/nonexistent/temporal-sdk-zig-capture-dir";
    defer test_capture_dir = null;
    try withDiscardRedirect(checkFallbackToNull, .{null_id});
}

test "a stderr or inherit redirect reports no server output" {
    for ([_]ServerOutput{ .stderr, .inherit }) |mode| {
        const r: OutputRedirect = .begin(mode);
        var buf: [capture_tail_max]u8 = undefined;
        const tail = r.readTail(&buf);
        const capture = redirect_state.capture_fd;
        r.end();
        try testing.expect(tail == .no_capture_file);
        try testing.expectEqual(-1, capture);
    }
}
