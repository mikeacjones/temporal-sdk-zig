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

    const term = child.wait(init.io) catch |err| fatal("{s}: cannot wait for {s}: {t}", .{ case, argv[0], err });
    std.process.exit(exitStatus(term));
}

/// Ends this process with `timeout_status` after `seconds`, having killed the group.
fn watch(io: std.Io, seconds: u32, message: []const u8) void {
    std.Io.sleep(io, .fromSeconds(seconds), .awake) catch {};
    killGroup();
    std.Io.File.stderr().writeStreamingAll(io, message) catch {};
    std.process.exit(timeout_status);
}

fn onSignal(sig: posix.SIG) callconv(.c) void {
    killGroup();
    exitNow(128 +| @as(u8, @truncate(@backingInt(sig))));
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

test "the exit status is the program's, or 128 plus the signal that ended it" {
    try testing.expectEqual(@as(u8, 0), exitStatus(.{ .exited = 0 }));
    try testing.expectEqual(@as(u8, 66), exitStatus(.{ .exited = 66 }));
    try testing.expectEqual(@as(u8, 128 + 9), exitStatus(.{ .signal = .KILL }));
    try testing.expectEqual(@as(u8, 128 + 6), exitStatus(.{ .signal = .ABRT }));
}
