//! Replays the history of every run of the sample's workflow ID against the current
//! workflow code. Run the starter first so the server has runs to replay.

const std = @import("std");
const temporal = @import("temporal");
const multi_history_replay = @import("multi_history_replay.zig");

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;
    const arena = init.arena.allocator();

    // The client is a heavyweight object that should be created once per process.
    const options = try temporal.client.loadDefaultOptions(arena);
    var client = temporal.client.Client.connect(gpa, io, options) catch |err| {
        std.log.err("Unable to create client: {t}", .{err});
        std.process.exit(1);
    };
    defer client.close();

    if (!multi_history_replay.replayAll(gpa, io, arena, &client)) std.process.exit(1);
}
