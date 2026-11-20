//! A test that never ends while another thread holds std's stderr lock and never lets it go,
//! for the "check hang report names a test while stderr is held" case: a hung test may be stuck
//! holding that lock, and the report must still name it and show the stacks. The check kills
//! the process.

const std = @import("std");

test "waits while another thread holds stderr" {
    var locked: std.Io.Event = .unset;
    // Never joined: the check kills the process.
    const holder = try std.Thread.spawn(.{}, holdsStderrForever, .{&locked});
    _ = holder;
    try locked.wait(std.testing.io);
    var never: std.Io.Event = .unset;
    try never.wait(std.testing.io);
}

noinline fn holdsStderrForever(locked: *std.Io.Event) void {
    _ = std.debug.lockStderr(&.{});
    locked.set(std.testing.io);
    var never: std.Io.Event = .unset;
    never.wait(std.testing.io) catch {};
}
