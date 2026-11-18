const std = @import("std");

test "fuzz crashes" {
    try std.testing.fuzz({}, crashes, .{});
}

fn crashes(_: void, smith: *std.testing.Smith) !void {
    var buffer: [16]u8 = undefined;
    if (smith.slice(&buffer) != 0) @panic("non-empty input");
}

test "fuzz clean" {
    try std.testing.fuzz({}, clean, .{});
}

fn clean(_: void, smith: *std.testing.Smith) !void {
    var buffer: [16]u8 = undefined;
    const data = buffer[0..smith.slice(&buffer)];
    const copy = try std.testing.allocator.dupe(u8, data);
    defer std.testing.allocator.free(copy);
    try std.testing.expectEqualSlices(u8, data, copy);
}
