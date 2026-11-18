//! Draws one slice into an 8192-byte buffer, the JSON text of a workflow history.

const std = @import("std");
const fuzz = @import("../fuzz.zig");
const history = @import("../history.zig");

pub fn testOne(_: void, smith: *std.testing.Smith) anyerror!void {
    var buffer: [fuzz.max_input_len]u8 = undefined;
    const json = buffer[0..smith.slice(&buffer)];
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const result: history.Error!history.History = history.fromJson(arena.allocator(), json);
    _ = result catch |err| switch (err) {
        error.OutOfMemory, error.InvalidHistoryJson => return,
    };
}
