//! Starts the SDK's own tasks on a caller's `std.Io`. Zig 0.17.1's `std.Io.Threaded` places a
//! future's result aligned above 8 up to 8 bytes past the future's allocation
//! (`Future.create`), so `concurrent` refuses such a result at compile time. Remove the check
//! when the minimum Zig version has the upstream fix (docs/upstream/threaded-future-alignment/).
//! A direct `io.concurrent` in SDK code bypasses the check and is caught only in review.
const std = @import("std");

/// The largest result alignment `std.Io.Threaded` (Zig 0.17.1) places inside a future's allocation.
pub const max_result_alignment = 8;

/// Whether a task started with `concurrent` may return `T`.
pub fn resultFits(comptime T: type) bool {
    return @alignOf(T) <= max_result_alignment;
}

fn Result(comptime function: anytype) type {
    return @typeInfo(@TypeOf(function)).@"fn".return_type.?;
}

/// `io.concurrent(function, args)`; a compile error when `function`'s result type is aligned
/// above `max_result_alignment`: hand such a result back through memory the caller owns.
pub fn concurrent(
    io: std.Io,
    comptime function: anytype,
    args: std.meta.ArgsTuple(@TypeOf(function)),
) std.Io.ConcurrentError!std.Io.Future(Result(function)) {
    comptime if (!resultFits(Result(function))) @compileError(std.fmt.comptimePrint(
        "io_tasks.concurrent: the result type {s} is aligned to {d} bytes, above the {d} that " ++
            "std.Io.Threaded keeps inside a future's allocation on Zig 0.17.1; have the task " ++
            "write its result through a pointer the caller owns and return void, as " ++
            "runNexusRequest in src/worker.zig does",
        .{ @typeName(Result(function)), @alignOf(Result(function)), max_result_alignment },
    ));
    return io.concurrent(function, args);
}

test "resultFits accepts results aligned to 8 or less and refuses wider ones" {
    try std.testing.expect(resultFits(void));
    try std.testing.expect(resultFits(u64));
    try std.testing.expect(resultFits(anyerror![]const u8));
    try std.testing.expect(!resultFits(std.Io.Duration));
    try std.testing.expect(!resultFits(u128));
    try std.testing.expect(!resultFits(@Vector(4, u32)));
}

test "concurrent runs the task and returns its result" {
    const S = struct {
        fn double(x: u64) u64 {
            return x * 2;
        }
    };
    var f = try concurrent(std.testing.io, S.double, .{21});
    try std.testing.expectEqual(@as(u64, 42), f.await(std.testing.io));
}
