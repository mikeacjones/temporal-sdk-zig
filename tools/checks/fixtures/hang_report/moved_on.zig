//! A test that never ends, whose threads go on and reuse their stacks as soon as the hang report
//! has frozen them and let them go, while another thread holds std.debug's SelfInfo lock, for
//! the "check hang report thread that goes on before its unwind passes" case. The report
//! freezes one thread at a time and, while it is frozen, only records the return addresses its
//! frame pointers lead to; it looks symbols up after every thread runs again. Here the holder
//! takes the SelfInfo lock before the report starts and never lets it go, and each thread, the
//! moment the report lets it go, fills the stack it was frozen on with frame records that point
//! at unmapped memory and says so on stderr. The report must use only the addresses it recorded
//! while each thread was frozen, must not wait on the lock without end (after 1 s it shows bare
//! addresses), must reach its end line and must not crash the process. The check kills the
//! process.
//!
//! Nothing here waits on a clock: a thread learns that the report has let it go because its
//! `sigsuspend` returns, which happens only once the report's SIGURG handler has run on it.
//!
//! Neither thread may miss that signal. A thread keeps SIGURG blocked from just before it parks
//! until its `sigsuspend`, so a SIGURG sent then stays pending; but a thread that is not in
//! its `sigsuspend` within the report's 1 s per-thread wait loses it when the report restores the
//! default action, and a holder that does not exist yet is never signalled. So the setup runs
//! in a first test, which returns only once the holder has the lock and has parked. The second
//! test, the one the report must name, only blocks SIGURG and parks, so both threads are parked,
//! or have the signal pending, before its limit can pass. However slow the setup, the first
//! test's own report, if it gets one, finds the holder running with SIGURG unblocked or parked;
//! a holder let go before the second test begins parks again.

const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;

/// Set by the second test before it parks, so that the holder goes on only after a report that
/// came during that test.
var second_test_began: std.atomic.Value(bool) = .init(false);

test "starts a thread that takes std.debug's symbol lock and parks" {
    var parked: std.Io.Event = .unset;
    // Never joined: the check kills the process.
    const holder = try std.Thread.spawn(.{}, holdsSymbolLock, .{&parked});
    _ = holder;
    try parked.wait(std.testing.io);
}

test "goes on after the report lets it go while another thread holds the symbol lock" {
    second_test_began.store(true, .release);
    blockUrg();
    goesOnWhenLetGo("the test's thread");
}

/// Takes std.debug's SelfInfo lock and keeps it; then parks and goes on like the test's thread.
noinline fn holdsSymbolLock(parked: *std.Io.Event) void {
    const io = std.Io.Threaded.global_single_threaded.io();
    if (std.debug.getSelfDebugInfo()) |si| {
        if (builtin.os.tag.isDarwin()) si.target.mutex.lockUncancelable(io) else si.target.rwlock.lockUncancelable(io);
    } else |_| {}
    blockUrg();
    parked.set(std.testing.io);
    goesOnWhenLetGo("the thread holding std.debug's symbol lock");
}

/// Blocks SIGURG, the report's freeze signal, on this thread until `waitUntilLetGo`: one sent
/// before then stays pending.
fn blockUrg() void {
    var urg = posix.sigemptyset();
    posix.sigaddset(&urg, .URG);
    threadSigmask(posix.SIG.BLOCK, &urg, null);
}

/// Changes this thread's signal mask only. On macOS, sigprocmask changes every thread's mask,
/// even that of a thread waiting in pselect or sigsuspend with SIGURG unblocked, which would
/// block the report's signal there.
fn threadSigmask(how: c_int, set: *const posix.sigset_t, old: ?*posix.sigset_t) void {
    if (builtin.link_libc) {
        var ignored: posix.sigset_t = undefined;
        _ = std.c.pthread_sigmask(how, set, old orelse &ignored);
    } else {
        posix.sigprocmask(@intCast(how), set, old);
    }
}

/// Waits until the report has frozen this thread and let it go, then reuses the stack it was
/// frozen on, says so, and waits forever.
noinline fn goesOnWhenLetGo(who: []const u8) void {
    waitUntilLetGo();
    while (!second_test_began.load(.acquire)) waitUntilLetGo();
    overwriteStack(codeAddress());
    std.debug.print("moved_on: {s} went on after the report let it go and reused its stack\n", .{who});
    var never: std.Io.Event = .unset;
    never.wait(std.testing.io) catch {};
}

/// Returns once a SIGURG handler has run on this thread and returned. Only the report installs
/// one, and its handler returns when the report lets the thread go. Every other signal stays
/// blocked meanwhile, so nothing else ends the wait.
noinline fn waitUntilLetGo() void {
    var only_urg = posix.sigfillset();
    posix.sigdelset(&only_urg, .URG);
    if (builtin.link_libc) {
        _ = sigsuspend(&only_urg);
    } else {
        _ = std.os.linux.syscall2(.rt_sigsuspend, @intFromPtr(&only_urg), std.os.linux.NSIG / 8);
    }
}

extern "c" fn sigsuspend(mask: *const posix.sigset_t) c_int;

noinline fn codeAddress() usize {
    return @returnAddress();
}

/// Fills the stack below the caller, where the frames the thread was frozen in were (the
/// signal handler's included), with frame records whose saved frame pointer is unmapped and
/// whose return address is code.
noinline fn overwriteStack(code: usize) void {
    var junk: [8192]usize align(16) = undefined;
    var i: usize = 0;
    while (i < junk.len) : (i += 2) {
        junk[i] = 0x6fff_0000_0000;
        junk[i + 1] = code;
    }
    std.mem.doNotOptimizeAway(&junk);
}
