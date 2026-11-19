//! Two threads add to one variable with no synchronization. Built with Thread Sanitizer,
//! this must report a data race and exit non-zero, so `zig build tsan` fails on a report.

const std = @import("std");

var shared: u64 = 0;

noinline fn write() void {
    for (0..1000) |_| shared += 1;
}

pub fn main() !void {
    const a = try std.Thread.spawn(.{}, write, .{});
    const b = try std.Thread.spawn(.{}, write, .{});
    a.join();
    b.join();
}
