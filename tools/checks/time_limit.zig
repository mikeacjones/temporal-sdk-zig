//! Runs a check case's program with a time limit, so that a program blocked on something
//! outside it fails its case instead of hanging `zig build test`. The Thread Sanitizer
//! cases need it: to print a report, the runtime on macOS starts `atos -p <pid>` and waits
//! for its answer, with no limit of its own, and a wedged atos never answers.
//!
//!   time-limit <seconds> <case> <program> [args...]
//!     Runs the program in a process group of its own, with this process's stdin, stdout,
//!     stderr and environment, and exits with its status (128 + the signal when a signal
//!     ended it). When it has not ended after <seconds>, kills the group, so the program
//!     and whatever it started (such as the symbolizer), prints
//!     `time-limit: <case>: <program> did not end within <seconds> s and was killed` to
//!     stderr and exits 124. SIGINT, SIGTERM and SIGHUP kill the group the same way.
//!     When a signal that neither of those kills sent ended the program (one from outside,
//!     such as the system's SIGKILL under memory pressure, or the program's own SIGABRT),
//!     prints `time-limit: <case>: ended by signal <N> (SIG<NAME>)` to stderr before
//!     exiting 128 + N, so that a case failing that way shows why even when the program
//!     printed nothing. A timeout prints only its own message.
//!     Whenever the program ends, by exiting or by any signal, kills what is left of its
//!     group before exiting, so that no process it started (such as a stuck symbolizer
//!     holding stderr open) outlives this one.
//!
//! The program may replace itself (see `tsan-suppressions exec`); it keeps the group.

const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;

const usage_text = "usage: time-limit <seconds> <case> <program> [args...]\n";

/// The exit status after a timeout, as timeout(1) uses.
pub const timeout_status = 124;

/// The program's process group, once it runs; 0 before. Read by the watchdog thread and the
/// signal handler.
var group: std.atomic.Value(posix.pid_t) = .init(0);

/// The status this process ends with because the watchdog or a signal killed the group; 0
/// while neither has. It is stored before the kill, so the main thread, which wakes from
/// waiting once the program dies, exits with it rather than with 128 + SIGKILL.
var forced_status: std.atomic.Value(u8) = .init(0);

pub fn main(init: std.process.Init) !void {
    if (builtin.os.tag == .windows) @compileError("time-limit kills a process group, which needs POSIX");
    const arena = init.arena.allocator();
    const args = try init.minimal.args.toSlice(arena);
    if (args.len < 4) usage();
    const seconds = parseSeconds(args[1]) orelse usage();
    const case = args[2];
    const argv = args[3..];

    var child = std.process.spawn(init.io, .{ .argv = argv, .pgid = 0 }) catch |err|
        fatal("{s}: cannot run {s}: {t}", .{ case, argv[0], err });
    group.store(child.id.?, .release);

    const act: posix.Sigaction = .{
        .handler = .{ .handler = onSignal },
        .mask = posix.sigemptyset(),
        .flags = 0,
    };
    inline for (.{ posix.SIG.INT, posix.SIG.TERM, posix.SIG.HUP }) |sig| posix.sigaction(sig, &act, null);

    const message = try std.fmt.allocPrint(arena, "time-limit: {s}: {s} did not end within {d} s and was killed\n", .{ case, argv[0], seconds });
    const watchdog = try std.Thread.spawn(.{}, watch, .{ init.io, seconds, message });
    watchdog.detach();

    const term = waitThenKillGroup(init.io, &child) catch |err| fatal("{s}: cannot wait for {s}: {t}", .{ case, argv[0], err });
    // One load decides both the line and the status, so they cannot disagree.
    const forced = forced_status.load(.acquire);
    if (try signalLine(arena, case, term, forced)) |line|
        std.Io.File.stderr().writeStreamingAll(init.io, line) catch {};
    std.process.exit(statusFor(term, forced));
}

/// Waits for the program to end, then kills what is left of its group, so that a process
/// it started cannot keep running, or keep this process's stderr open for a reader
/// waiting on its end, after this process exits. The group outlives the program while
/// any member does, and its ID is not reused while it exists; when no member is left the
/// kill fails with ESRCH, which `killGroup` ignores.
fn waitThenKillGroup(io: std.Io, child: *std.process.Child) std.process.Child.WaitError!std.process.Child.Term {
    const term = try child.wait(io);
    killGroup();
    return term;
}

/// The status to exit with once the program has ended: the one the watchdog or a signal
/// handler recorded before killing the group, or else the program's own.
pub fn statusAfterWait(term: std.process.Child.Term) u8 {
    return statusFor(term, forced_status.load(.acquire));
}

/// `statusAfterWait` given the forced status it read.
fn statusFor(term: std.process.Child.Term, forced: u8) u8 {
    return if (forced != 0) forced else exitStatus(term);
}

/// The line naming the signal that ended the program, or null when no signal did, or
/// when this process killed the group (`forced` is not 0): then the timeout message or
/// the signal sent to this process already explains the end.
fn signalLine(gpa: std.mem.Allocator, case: []const u8, term: std.process.Child.Term, forced: u8) error{OutOfMemory}!?[]u8 {
    if (forced != 0) return null;
    const sig = switch (term) {
        .signal => |sig| sig,
        else => return null,
    };
    const prefix: []const u8, const name: []const u8 = if (std.enums.tagName(posix.SIG, sig)) |n| .{ "SIG", n } else .{ "", "unnamed" };
    return try std.fmt.allocPrint(gpa, "time-limit: {s}: ended by signal {d} ({s}{s})\n", .{ case, @backingInt(sig), prefix, name });
}

/// Ends this process with `timeout_status` after `seconds`, having killed the group. The
/// status and the message come first: the kill wakes the main thread, which may exit
/// before this thread does, and must then exit with the same status after the message.
fn watch(io: std.Io, seconds: u32, message: []const u8) void {
    std.Io.sleep(io, .fromSeconds(seconds), .awake) catch {};
    forced_status.store(timeout_status, .release);
    std.Io.File.stderr().writeStreamingAll(io, message) catch {};
    killGroup();
    std.process.exit(timeout_status);
}

fn onSignal(sig: posix.SIG) callconv(.c) void {
    const status = 128 +| @as(u8, @truncate(@backingInt(sig)));
    // A first forced status wins, so a signal during a timeout still ends with 124.
    _ = forced_status.cmpxchgStrong(0, status, .acq_rel, .acquire);
    killGroup();
    exitNow(forced_status.load(.acquire));
}

/// Exits without running exit handlers, which is safe in a signal handler.
fn exitNow(status: u8) noreturn {
    if (builtin.link_libc or builtin.os.tag.isDarwin()) std.c._exit(status);
    std.process.exit(status);
}

fn killGroup() void {
    const pgid = group.load(.acquire);
    if (pgid > 0) posix.kill(-pgid, .KILL) catch {};
}

/// A whole number of seconds from 1 to a day, or null.
pub fn parseSeconds(text: []const u8) ?u32 {
    const n = std.fmt.parseInt(u32, text, 10) catch return null;
    return if (n >= 1 and n <= 24 * 60 * 60) n else null;
}

/// The status a shell gives a command that ended this way.
pub fn exitStatus(term: std.process.Child.Term) u8 {
    return switch (term) {
        .exited => |code| code,
        .signal, .stopped => |sig| 128 +| @as(u8, @truncate(@backingInt(sig))),
        .unknown => 1,
    };
}

fn usage() noreturn {
    std.debug.print(usage_text, .{});
    std.process.exit(2);
}

fn fatal(comptime format: []const u8, args: anytype) noreturn {
    std.debug.print("time-limit: " ++ format ++ "\n", args);
    std.process.exit(1);
}

const testing = std.testing;

test "seconds are a whole number from 1 to a day" {
    try testing.expectEqual(@as(?u32, 1), parseSeconds("1"));
    try testing.expectEqual(@as(?u32, 86400), parseSeconds("86400"));
    for ([_][]const u8{ "", "0", "-1", "1.5", "1s", "86401", "99999999999" }) |text|
        try testing.expectEqual(@as(?u32, null), parseSeconds(text));
}

test "after a timeout or a signal killed the group, the recorded status wins over SIGKILL's" {
    defer forced_status.store(0, .release);
    try testing.expectEqual(@as(u8, 128 + 9), statusAfterWait(.{ .signal = .KILL }));
    try testing.expectEqual(@as(u8, 3), statusAfterWait(.{ .exited = 3 }));
    forced_status.store(timeout_status, .release);
    try testing.expectEqual(@as(u8, timeout_status), statusAfterWait(.{ .signal = .KILL }));
    try testing.expectEqual(@as(u8, timeout_status), statusAfterWait(.{ .exited = 0 }));
}

test "a signal from outside is named, with 128 plus the signal as the status" {
    const line = (try signalLine(testing.allocator, "tsan race", .{ .signal = .KILL }, 0)).?;
    defer testing.allocator.free(line);
    try testing.expectEqualStrings("time-limit: tsan race: ended by signal 9 (SIGKILL)\n", line);
    try testing.expectEqual(@as(u8, 128 + 9), statusFor(.{ .signal = .KILL }, 0));

    const abort = (try signalLine(testing.allocator, "c", .{ .signal = .ABRT }, 0)).?;
    defer testing.allocator.free(abort);
    try testing.expectEqualStrings("time-limit: c: ended by signal 6 (SIGABRT)\n", abort);
}

test "after a timeout or a forwarded signal, and after an exit, no signal is named" {
    // The watchdog's kill: only its timeout message, and 124.
    try testing.expectEqual(@as(?[]u8, null), try signalLine(testing.allocator, "c", .{ .signal = .KILL }, timeout_status));
    try testing.expectEqual(@as(u8, timeout_status), statusFor(.{ .signal = .KILL }, timeout_status));
    // SIGTERM to this process, which killed the group.
    try testing.expectEqual(@as(?[]u8, null), try signalLine(testing.allocator, "c", .{ .signal = .KILL }, 128 + 15));
    // The program's own exit, clean or not.
    try testing.expectEqual(@as(?[]u8, null), try signalLine(testing.allocator, "c", .{ .exited = 0 }, 0));
    try testing.expectEqual(@as(u8, 0), statusFor(.{ .exited = 0 }, 0));
    try testing.expectEqual(@as(?[]u8, null), try signalLine(testing.allocator, "c", .{ .exited = 66 }, 0));
}

/// Runs `script` in `sh` in a process group of its own with stderr on a pipe, as `main`
/// runs a program, and checks that once `waitThenKillGroup` returns the pipe reaches EOF
/// within a few seconds, though the script left a `sleep` holding it open.
fn expectGroupKilledAfter(script: []const u8) !std.process.Child.Term {
    const io = testing.io;
    var child = try std.process.spawn(io, .{ .argv = &.{ "/bin/sh", "-c", script }, .pgid = 0, .stderr = .pipe });
    group.store(child.id.?, .release);
    const pgid = child.id.?;
    // Taken before the wait, which would close it.
    const stderr = child.stderr.?;
    child.stderr = null;
    defer stderr.close(io);
    // Without the fix, the sleep is still running here; end it so no process outlives the test.
    defer {
        posix.kill(-pgid, .KILL) catch {};
        group.store(0, .release);
    }
    const term = try waitThenKillGroup(io, &child);
    var buf: [256]u8 = undefined;
    while (true) {
        var fds = [_]posix.pollfd{.{ .fd = stderr.handle, .events = posix.POLL.IN, .revents = 0 }};
        if (try posix.poll(&fds, 5000) == 0) return error.StderrStillOpen;
        if (try posix.read(stderr.handle, &buf) == 0) return term;
    }
}

test "when the program is killed from outside, the rest of its group dies with it" {
    const term = try expectGroupKilledAfter("sleep 60 & kill -KILL $$");
    try testing.expectEqual(std.process.Child.Term{ .signal = .KILL }, term);
    try testing.expectEqual(@as(u8, 128 + 9), statusFor(term, 0));
    const line = (try signalLine(testing.allocator, "c", term, 0)).?;
    defer testing.allocator.free(line);
    try testing.expectEqualStrings("time-limit: c: ended by signal 9 (SIGKILL)\n", line);
}

test "when the program exits, the rest of its group dies with it" {
    const term = try expectGroupKilledAfter("sleep 60 & exit 3");
    try testing.expectEqual(std.process.Child.Term{ .exited = 3 }, term);
    try testing.expectEqual(@as(u8, 3), statusFor(term, 0));
    try testing.expectEqual(@as(?[]u8, null), try signalLine(testing.allocator, "c", term, 0));
}

test "the exit status is the program's, or 128 plus the signal that ended it" {
    try testing.expectEqual(@as(u8, 0), exitStatus(.{ .exited = 0 }));
    try testing.expectEqual(@as(u8, 66), exitStatus(.{ .exited = 66 }));
    try testing.expectEqual(@as(u8, 128 + 9), exitStatus(.{ .signal = .KILL }));
    try testing.expectEqual(@as(u8, 128 + 6), exitStatus(.{ .signal = .ABRT }));
}
