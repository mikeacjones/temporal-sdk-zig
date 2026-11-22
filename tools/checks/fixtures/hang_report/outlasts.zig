//! A test that outlasts the hang report's limit and then passes, for the "check hang report
//! slow test passes" case. It returns only once the report has frozen its thread and let it go,
//! so the report came while it ran however late the report starts, and it then passes. Nothing
//! here waits on a clock: the wait ends when the report's SIGURG handler has run on this thread.
//! Only the bound, a report that never comes, ends it otherwise, and that fails the test.

const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;

test "passes once the report has let it go" {
    // SIGURG, the report's freeze signal, stays blocked except inside the wait, so one sent
    // before the wait begins stays pending and ends it at once.
    var urg = posix.sigemptyset();
    posix.sigaddset(&urg, .URG);
    var previous: posix.sigset_t = undefined;
    threadSigmask(posix.SIG.BLOCK, &urg, &previous);
    defer threadSigmask(posix.SIG.SETMASK, &previous, null);
    if (!waitUntilLetGo(60)) return error.NoHangReportWithin60s;
}

/// Returns true once a SIGURG handler has run on this thread and returned, false after `bound`
/// seconds without one. Only the report installs such a handler, and its handler returns when
/// the report lets the thread go. Every other signal stays blocked meanwhile. Neither pselect
/// nor ppoll is restarted after a handler, whatever its SA_RESTART.
fn waitUntilLetGo(bound: u32) bool {
    var only_urg = posix.sigfillset();
    posix.sigdelset(&only_urg, .URG);
    if (builtin.link_libc) {
        var timeout: std.c.timespec = .{ .sec = bound, .nsec = 0 };
        const rc = pselect(0, null, null, null, &timeout, &only_urg);
        return rc == -1 and posix.errno(rc) == .INTR;
    } else {
        const linux = std.os.linux;
        var timeout: linux.timespec = .{ .sec = bound, .nsec = 0 };
        var no_fds: [0]linux.pollfd = .{};
        return linux.errno(linux.ppoll(&no_fds, 0, &timeout, &only_urg)) == .INTR;
    }
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

extern "c" fn pselect(nfds: c_int, read: ?*anyopaque, write: ?*anyopaque, except: ?*anyopaque, timeout: *const std.c.timespec, mask: *const posix.sigset_t) c_int;
