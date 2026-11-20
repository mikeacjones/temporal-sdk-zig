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

const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;

test "goes on after the report lets it go while another thread holds the symbol lock" {
    // SIGURG, the report's freeze signal, stays blocked on this thread and on the holder, which
    // inherits the mask, except inside `waitUntilLetGo`; one sent before then stays pending.
    var urg = posix.sigemptyset();
    posix.sigaddset(&urg, .URG);
    posix.sigprocmask(posix.SIG.BLOCK, &urg, null);

    var locked: std.Io.Event = .unset;
    // Never joined: the check kills the process.
    const holder = try std.Thread.spawn(.{}, holdsSymbolLock, .{&locked});
    _ = holder;
    try locked.wait(std.testing.io);
    goesOnWhenLetGo("the test's thread");
}

/// Takes std.debug's SelfInfo lock and keeps it; then goes on like the test's thread.
noinline fn holdsSymbolLock(locked: *std.Io.Event) void {
    const io = std.Io.Threaded.global_single_threaded.io();
    if (std.debug.getSelfDebugInfo()) |si| {
        if (builtin.os.tag.isDarwin()) si.target.mutex.lockUncancelable(io) else si.target.rwlock.lockUncancelable(io);
    } else |_| {}
    locked.set(std.testing.io);
    goesOnWhenLetGo("the thread holding std.debug's symbol lock");
}

/// Waits until the report has frozen this thread and let it go, then reuses the stack it was
/// frozen on, says so, and waits forever.
noinline fn goesOnWhenLetGo(who: []const u8) void {
    waitUntilLetGo();
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
