//! A test that never ends while another thread keeps std.debug's symbol lock busy in short
//! turns, for the "check hang report finishes while another thread keeps taking and releasing
//! the symbol lock" case. To the report's symbol helper, a thread that keeps taking and
//! releasing the lock, and takes it back each time before the helper gets it, looks like this:
//! each of its futex waits ends after a while, it finds the lock taken again, and it waits
//! anew, so no single wait lasts long but they never end. The report must still finish: once
//! the helper's waits add up to 1 s it writes the rest with bare addresses. The check kills the
//! process.
//!
//! The churner gives the helper exactly that, and nothing else. It takes the lock before the
//! report and keeps it; every `hold` it wakes the helper's futex wait, which is what its
//! unlock would do, without letting the lock go. A real release would let the helper win the
//! race whenever the churner is off the CPU (as on a loaded host), and the helper would then
//! write every row with symbols within its 1 s of waiting, which is correct for the report but
//! leaves the check nothing to see. Waking without releasing keeps the helper from making
//! progress however threads are scheduled, so the lock line always comes.
//!
//! The churner also checks that the thread waiting on the lock has the name the report gives
//! its helper, "hang-report-sym", and says so once.
//!
//! The check needs no clock: it waits only for texts. `hold` is the length of the helper's
//! single waits, not an ordering. With any hold under 1 s a report that bounds only single
//! waits never ends, and a hold stretched past 1 s by a stalled host only makes the report give
//! up sooner.

const std = @import("std");
const builtin = @import("builtin");

/// How long each of the helper's futex waits lasts before the churner wakes it.
const hold: std.Io.Duration = .fromMilliseconds(100);

test "waits while another thread keeps taking and releasing the symbol lock" {
    // Never joined: the check kills the process.
    const churner = try std.Thread.spawn(.{}, churnsSymbolLock, .{});
    _ = churner;
    var never: std.Io.Event = .unset;
    try never.wait(std.testing.io);
}

noinline fn churnsSymbolLock() void {
    const io = std.Io.Threaded.global_single_threaded.io();
    const si = std.debug.getSelfDebugInfo() catch return;
    const target = &si.target;
    const has_mutex = comptime @hasField(@TypeOf(target.*), "mutex");
    const has_rwlock = comptime @hasField(@TypeOf(target.*), "rwlock");
    if (!has_mutex and !has_rwlock) return;
    // A reader or writer that the rwlock's writer keeps out waits on its mutex.
    const mutex: *std.Io.Mutex = if (has_mutex) &target.mutex else &target.rwlock.mutex;
    // Never a blocking lock: nothing else takes the lock before the report, and a churner
    // waiting on the futex could be the one the helper's wake goes to.
    while (!(if (has_mutex) mutex.tryLock() else target.rwlock.tryLock(io))) std.Thread.yield() catch {};
    while (mutex.state.load(.acquire) != .contended) std.Thread.yield() catch {};
    if (threadNamed("hang-report-sym")) {
        std.debug.print("symbol_lock_churn: a thread named hang-report-sym waits while this thread holds std.debug's symbol lock\n", .{});
    }
    while (true) {
        io.sleep(hold, .awake) catch {};
        // As the unlock would, but the woken waiter finds the lock still taken and waits again.
        io.futexWake(std.Io.Mutex.State, &mutex.state.raw, 1);
    }
}

/// Whether some thread of this process is named `name`. False on any failure.
fn threadNamed(name: []const u8) bool {
    if (builtin.os.tag == .linux) {
        const io = std.Io.Threaded.global_single_threaded.io();
        var dir = std.Io.Dir.openDirAbsolute(io, "/proc/self/task", .{ .iterate = true }) catch return false;
        defer dir.close(io);
        var it = dir.iterate();
        while (it.next(io) catch return false) |entry| {
            var path_buf: [64]u8 = undefined;
            const path = std.fmt.bufPrint(&path_buf, "/proc/self/task/{s}/comm", .{entry.name}) catch continue;
            var text_buf: [64]u8 = undefined;
            const text = std.Io.Dir.cwd().readFile(io, path, &text_buf) catch continue;
            if (std.mem.eql(u8, std.mem.trimEnd(u8, text, "\n"), name)) return true;
        }
        return false;
    } else if (builtin.os.tag.isDarwin()) {
        var ports: std.c.mach_port_array_t = undefined;
        var count: std.c.mach_msg_type_number_t = 0;
        if (std.c.task_threads(std.c.mach_task_self(), &ports, &count) != 0) return false;
        defer {
            for (ports[0..count]) |port| _ = std.c.mach_port_deallocate(std.c.mach_task_self(), port);
            if (count != 0) _ = std.c.vm_deallocate(
                std.c.mach_task_self(),
                @intFromPtr(ports),
                count * @sizeOf(std.c.mach_port_t),
            );
        }
        for (ports[0..count]) |port| {
            const pt = pthread_from_mach_thread_np(port) orelse continue;
            var buf: [64:0]u8 = @splat(0);
            if (std.c.pthread_getname_np(pt, &buf, buf.len) != 0) continue;
            if (std.mem.eql(u8, std.mem.sliceTo(&buf, 0), name)) return true;
        }
        return false;
    }
    return false;
}

// macOS: the pthread of a Mach thread, or null for a thread that has none; std lacks it.
extern "c" fn pthread_from_mach_thread_np(std.c.mach_port_t) ?std.c.pthread_t;
