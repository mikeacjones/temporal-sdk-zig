//! Functions that return error.OutOfMemory for every failed allocation: one that allocates
//! into a caller's arena, and one that frees what it allocates.

const std = @import("std");
const alloc_failures = @import("alloc_failures");

fn threeAllocations(a: std.mem.Allocator, len: usize) error{OutOfMemory}!void {
    _ = try a.alloc(u8, len);
    _ = try a.alloc(u16, len);
    _ = try a.alloc(u32, len);
}

fn freesWhatItAllocates(a: std.mem.Allocator, len: usize) error{OutOfMemory}!void {
    const first = try a.alloc(u8, len);
    defer a.free(first);
    const second = try a.alloc(u8, len);
    defer a.free(second);
}

test "fixture.arena" {
    try alloc_failures.run("fixture.arena", .arena, threeAllocations, .{@as(usize, 16)});
}

test "fixture.owned" {
    try alloc_failures.run("fixture.owned", .owned, freesWhatItAllocates, .{@as(usize, 16)});
}

test "every known finding names a case" {
    try alloc_failures.expectFindingsHaveCases(@embedFile("correct.zig"));
}
