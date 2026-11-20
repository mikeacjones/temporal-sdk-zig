//! A test that never ends while another thread takes std.debug's symbol lock only once the
//! report has begun to look up symbols, and then keeps it, for the "check hang report finishes
//! when the symbol lock is taken during its lookup" case. moved_on.zig meets this when its
//! setup is slow by about a second: the first test's report starts looking up symbols, and the
//! holder takes the lock in between. The report must still finish; the check kills the process.
//!
//! No clock: the taker waits until it sees the lock taken by someone else, which only the
//! report does in this process, then asks for it and keeps it.

const std = @import("std");
const builtin = @import("builtin");

test "waits while another thread takes the symbol lock during the report" {
    // Never joined: the check kills the process.
    const taker = try std.Thread.spawn(.{}, takesSymbolLockLate, .{});
    _ = taker;
    var never: std.Io.Event = .unset;
    try never.wait(std.testing.io);
}

noinline fn takesSymbolLockLate() void {
    const io = std.Io.Threaded.global_single_threaded.io();
    const si = std.debug.getSelfDebugInfo() catch return;
    if (comptime @hasField(@TypeOf(si.target), "mutex")) {
        const mutex = &si.target.mutex;
        while (mutex.state.load(.acquire) == .unlocked) std.atomic.spinLoopHint();
        mutex.lockUncancelable(io);
    } else if (comptime @hasField(@TypeOf(si.target), "rwlock")) {
        const rwlock = &si.target.rwlock;
        // A reader is the report looking up symbols; a waiting writer then keeps new readers out.
        const reader_mask: usize = ~@as(usize, 0) << (1 + @divFloor(@bitSizeOf(usize) - 1, 2));
        while (@atomicLoad(usize, &rwlock.state, .acquire) & reader_mask == 0) std.atomic.spinLoopHint();
        rwlock.lockUncancelable(io);
    } else return;
    std.debug.print("symbol_lock_late: took std.debug's symbol lock while the report was looking up symbols\n", .{});
    var never: std.Io.Event = .unset;
    never.wait(io) catch {};
}
