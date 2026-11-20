//! A test that never ends, for the "check hang report names a waiting test" case: the hang
//! report must name the test and show both waiting functions. The check kills the process.

const std = @import("std");

test "waits forever on an event nothing sets" {
    var helper_event: std.Io.Event = .unset;
    // Never joined: the check kills the process.
    const helper = try std.Thread.spawn(.{}, helperWaitsForever, .{&helper_event});
    _ = helper;
    try waitsForever();
}

noinline fn helperWaitsForever(event: *std.Io.Event) void {
    event.wait(std.testing.io) catch {};
}

noinline fn waitsForever() !void {
    var event: std.Io.Event = .unset;
    try event.wait(std.testing.io);
}
