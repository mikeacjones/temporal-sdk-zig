//! A test that allocates through std.testing.allocator past the hang report's limit and then
//! passes, for the "check hang report allocating test passes" case. In Debug every allocation
//! and free captures a stack trace, so the report freezes the test thread inside std.debug's
//! unwinder; the report must still let it go, and the test must finish.

const std = @import("std");

test "allocates past the limit and then finishes" {
    // The bound of this fixture: a report that never lets the test go must fail the check, not
    // leave `zig build test` waiting.
    const bound = try std.Thread.spawn(.{}, exitAfterBound, .{});
    bound.detach();
    const io = std.testing.io;
    // A fixed run time is what this fixture tests, so AGENTS.md's rule against them does not apply.
    const until = std.Io.Clock.awake.now(io).addDuration(.fromSeconds(4));
    var count: usize = 0;
    while (std.Io.Clock.awake.now(io).compare(.lt, until)) : (count += 1) {
        const bytes = try std.testing.allocator.alloc(u8, 32);
        std.testing.allocator.free(bytes);
    }
    try std.testing.expect(count > 0);
}

fn exitAfterBound() void {
    const io = std.Io.Threaded.global_single_threaded.io();
    const deadline = std.Io.Clock.awake.now(io).addDuration(.fromSeconds(60));
    while (std.Io.Clock.awake.now(io).compare(.lt, deadline)) {
        io.sleep(.fromMilliseconds(250), .awake) catch {};
    }
    std.debug.print("busy.test.allocates past the limit and then finishes: still running after 60s\n", .{});
    std.process.exit(1);
}
