//! race.zig's writes under a mutex. Built with Thread Sanitizer, this must exit 0 with
//! nothing reported, so a report from race.zig is the race and not the setup.

const std = @import("std");

var shared: u64 = 0;
var mutex: std.Io.Mutex = .init;

noinline fn write(io: std.Io) void {
    for (0..1000) |_| {
        mutex.lockUncancelable(io);
        defer mutex.unlock(io);
        shared += 1;
    }
}

pub fn main(init: std.process.Init) !void {
    const a = try std.Thread.spawn(.{}, write, .{init.io});
    const b = try std.Thread.spawn(.{}, write, .{init.io});
    a.join();
    b.join();
}
