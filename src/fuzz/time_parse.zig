//! Draws one slice into a 64-byte buffer, the text given to `converter.Time.parse`.
//!
//! A time that parses must format and parse back to the same instant.

const std = @import("std");
const converter = @import("../converter.zig");
const Time = converter.Time;

pub fn testOne(_: void, smith: *std.testing.Smith) anyerror!void {
    var buffer: [64]u8 = undefined;
    const text = buffer[0..smith.slice(&buffer)];
    const result: error{InvalidTime}!Time = Time.parse(text);
    const time = result catch |err| switch (err) {
        error.InvalidTime => return,
    };
    var formatted: [Time.max_len]u8 = undefined;
    const back = try Time.parse(time.format(&formatted));
    try std.testing.expectEqual(time.unix_ns, back.unix_ns);
}
