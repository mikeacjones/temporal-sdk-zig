//! A test that allocates through std.testing.allocator until the hang report has frozen it and
//! let it go, and then passes, for the "check hang report allocating test passes" case. In Debug
//! every allocation and free captures a stack trace, so the report freezes the test thread
//! inside std.debug's unwinder; the report must still let it go, and the test must finish.
//!
//! The test learns that the report has let it go without a clock: it gives its thread an
//! alternate signal stack filled with a pattern. The report's SIGURG handler runs on it
//! (SA_ONSTACK), so once the pattern has changed and the test runs again, the handler has run on
//! this thread and returned, which happens only when the report lets the thread go.

const std = @import("std");
const posix = std.posix;

const pattern: usize = @as(usize, 0xa5a5_a5a5_a5a5_a5a5 & std.math.maxInt(usize));
var signal_stack: [64 * 1024 / @sizeOf(usize)]usize align(16) = undefined;

test "allocates until the report has let it go" {
    // The bound of this fixture: a report that never comes or never lets the test go must fail
    // the check, not leave `zig build test` waiting.
    const bound = try std.Thread.spawn(.{}, exitAfterBound, .{});
    bound.detach();

    @memset(&signal_stack, pattern);
    var previous: posix.stack_t = undefined;
    try posix.sigaltstack(&.{ .sp = @ptrCast(&signal_stack), .flags = 0, .size = @sizeOf(@TypeOf(signal_stack)) }, &previous);
    defer posix.sigaltstack(&previous, null) catch {};

    while (true) {
        const bytes = try std.testing.allocator.alloc(u8, 32);
        std.testing.allocator.free(bytes);
        if (handlerRanOnThisThread()) break;
    }
}

/// Whether anything has written to the top of `signal_stack`, where the kernel puts a signal's
/// frame. Only the report's SIGURG handler runs on it while the test runs.
fn handlerRanOnThisThread() bool {
    const top = signal_stack[signal_stack.len - 2048 ..];
    for (@as(*volatile [2048]usize, @ptrCast(top))) |word| {
        if (word != pattern) return true;
    }
    return false;
}

fn exitAfterBound() void {
    const io = std.Io.Threaded.global_single_threaded.io();
    const deadline = std.Io.Clock.awake.now(io).addDuration(.fromSeconds(60));
    while (std.Io.Clock.awake.now(io).compare(.lt, deadline)) {
        io.sleep(.fromMilliseconds(250), .awake) catch {};
    }
    std.debug.print("busy.test.allocates until the report has let it go: still running after 60s\n", .{});
    std.process.exit(1);
}
