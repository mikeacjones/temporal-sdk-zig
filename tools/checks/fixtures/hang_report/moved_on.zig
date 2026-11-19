//! A test whose thread the hang report freezes and then keeps waiting on for longer than the
//! report's hold limit, for the "check hang report thread that goes on before its unwind
//! passes" case. Another thread holds std.debug's SelfInfo lock (as one does while it loads
//! symbols for the first time), so the report's unwind of the frozen thread waits on it. The
//! frozen thread then goes on by itself and reuses the stack the report has not read yet. The
//! report must not read that stack as if the thread were still frozen: it must not crash or
//! hang the process, and the test must pass.

const std = @import("std");
const builtin = @import("builtin");

/// When the test ends, as a CLOCK_MONOTONIC reading; set before any thread starts.
var test_end: i64 = 0;

test "goes on before the report reads its stack and then finishes" {
    // A fixed run time is what this fixture tests, so AGENTS.md's rule against them does not apply.
    test_end = nowNanoseconds() + 14 * std.time.ns_per_s;
    // Spawned first, so that after the test's thread it is the next the report freezes.
    const holder = try std.Thread.spawn(.{}, holdSelfInfoLock, .{});
    // The bound of this fixture: a report that ends or stalls the process must fail the check,
    // not leave `zig build test` waiting.
    const bound = try std.Thread.spawn(.{}, exitAfterBound, .{});
    bound.detach();
    waitUntil(test_end, false);
    holder.join();
}

fn nowNanoseconds() i64 {
    var ts: std.posix.timespec = .{ .sec = 0, .nsec = 0 };
    _ = std.posix.system.clock_gettime(.MONOTONIC, &ts);
    return @as(i64, @intCast(ts.sec)) * std.time.ns_per_s + @as(i64, @intCast(ts.nsec));
}

/// Whether the hang report has installed its SIGURG handler, which it does once its own unwind
/// info is loaded and right before it freezes the first thread.
fn reportFreezing() bool {
    var old: std.posix.Sigaction = undefined;
    std.posix.sigaction(.URG, null, &old);
    return old.handler.handler != std.posix.SIG.DFL and old.handler.handler != std.posix.SIG.IGN;
}

fn lockSelfInfo(io: std.Io) void {
    const si = std.debug.getSelfDebugInfo() catch return;
    if (builtin.os.tag.isDarwin()) si.target.mutex.lockUncancelable(io) else si.target.rwlock.lockUncancelable(io);
}

fn unlockSelfInfo(io: std.Io) void {
    const si = std.debug.getSelfDebugInfo() catch return;
    if (builtin.os.tag.isDarwin()) si.target.mutex.unlock(io) else si.target.rwlock.unlock(io);
}

/// Takes the SelfInfo lock as soon as the report starts freezing, and keeps it for 8 s or until
/// this thread itself was frozen past the hold limit.
fn holdSelfInfoLock() void {
    const io = std.Io.Threaded.global_single_threaded.io();
    while (!reportFreezing()) {
        if (nowNanoseconds() >= test_end) return;
    }
    lockSelfInfo(io);
    waitUntil(nowNanoseconds() + 8 * std.time.ns_per_s, true);
}

/// Sleeps in short steps until `until`. A step that took more than 3 s means the thread was
/// frozen that long; it then reuses the stack it was frozen on for the rest of the wait.
/// `unlock`: the thread holds the SelfInfo lock and lets it go at the end of the wait.
noinline fn waitUntil(until: i64, unlock: bool) void {
    const io = std.Io.Threaded.global_single_threaded.io();
    const code = codeAddress();
    var last = nowNanoseconds();
    while (last < until) {
        io.sleep(.fromMilliseconds(10), .awake) catch {};
        const now = nowNanoseconds();
        if (now - last > 3 * std.time.ns_per_s) return overwriteStackAndWait(code, until, unlock);
        last = now;
    }
    if (unlock) unlockSelfInfo(io);
}

noinline fn codeAddress() usize {
    return @returnAddress();
}

/// Fills the stack below the caller, where the frames the thread was frozen in were, with
/// frame records whose saved frame pointer is unmapped and whose return address is code; lets
/// the SelfInfo lock go if `unlock`; then waits until `until`.
noinline fn overwriteStackAndWait(code: usize, until: i64, unlock: bool) void {
    var junk: [8192]usize align(16) = undefined;
    var i: usize = 0;
    while (i < junk.len) : (i += 2) {
        junk[i] = 0x6fff_0000_0000;
        junk[i + 1] = code;
    }
    std.mem.doNotOptimizeAway(&junk);
    const io = std.Io.Threaded.global_single_threaded.io();
    if (unlock) unlockSelfInfo(io);
    while (nowNanoseconds() < until) io.sleep(.fromMilliseconds(10), .awake) catch {};
    std.mem.doNotOptimizeAway(&junk);
}

/// Ends the process after 60 s. It writes without std.debug's stderr lock, which a crashing
/// watchdog may hold.
fn exitAfterBound() void {
    waitUntil(nowNanoseconds() + 60 * std.time.ns_per_s, false);
    const message = "moved_on.test.goes on before the report reads its stack and then finishes: still running after 60s\n";
    _ = std.posix.system.write(2, message, message.len);
    std.process.exit(1);
}
