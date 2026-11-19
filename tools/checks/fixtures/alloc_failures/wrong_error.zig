//! A function that turns its second allocation's failure into error.InvalidInput, which
//! `alloc_failures.run` must report at fail index 1.

const std = @import("std");
const alloc_failures = @import("alloc_failures");

fn twoAllocations(a: std.mem.Allocator) error{ OutOfMemory, InvalidInput }!void {
    _ = try a.alloc(u8, 8);
    _ = a.alloc(u8, 8) catch return error.InvalidInput;
}

test "fixture.wrong_error" {
    try alloc_failures.run("fixture.wrong_error", .arena, twoAllocations, .{});
}

test "every known finding names a case" {
    try alloc_failures.expectFindingsHaveCases(@embedFile("wrong_error.zig"));
}
