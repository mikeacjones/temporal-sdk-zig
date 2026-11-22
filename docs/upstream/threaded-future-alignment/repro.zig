//! Reproduces a result written past the end of a `std.Io.Threaded` future's allocation when the
//! task's result type is aligned above `@alignOf(Future)` (8). Run with:
//!
//!     zig test docs/upstream/threaded-future-alignment/repro.zig
//!
//! `OffsetAllocator` hands out every allocation whose alignment is 8 or less at an address that
//! is 8 mod 16, which any general-purpose allocator may do, and writes canary bytes after the
//! requested length. `free` checks them. Two tasks return `std.Io.Duration` (alignment 16 on
//! aarch64 and x86_64); their argument tuples differ by one `usize`, so whatever
//! `@sizeOf(Future)` is modulo 16, one of the two futures has its result placed 8 bytes later
//! than `Future.create` budgeted for.
const std = @import("std");

const canary_len = 16;
const canary_byte = 0xa5;

const OffsetAllocator = struct {
    child: std.mem.Allocator,
    overruns: std.atomic.Value(u32) = .init(0),

    fn allocator(self: *OffsetAllocator) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }

    /// Bytes before the returned address: 8 when the request allows an address 8 mod 16.
    fn lead(alignment: std.mem.Alignment) usize {
        return if (alignment.toByteUnits() <= 8) 8 else 0;
    }

    fn outer(alignment: std.mem.Alignment) std.mem.Alignment {
        return alignment.max(.@"16");
    }

    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *OffsetAllocator = @ptrCast(@alignCast(ctx));
        const pad = lead(alignment);
        const base = self.child.rawAlloc(pad + len + canary_len, outer(alignment), ret_addr) orelse return null;
        @memset(base[pad + len ..][0..canary_len], canary_byte);
        return base + pad;
    }

    fn resize(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) bool {
        return false;
    }

    fn remap(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) ?[*]u8 {
        return null;
    }

    fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *OffsetAllocator = @ptrCast(@alignCast(ctx));
        const pad = lead(alignment);
        const base = memory.ptr - pad;
        for (base[pad + memory.len ..][0..canary_len]) |b| {
            if (b != canary_byte) {
                _ = self.overruns.fetchAdd(1, .monotonic);
                std.debug.print("write past the end of a {d}-byte allocation at 0x{x}\n", .{ memory.len, @intFromPtr(memory.ptr) });
                break;
            }
        }
        self.child.rawFree(base[0 .. pad + memory.len + canary_len], outer(alignment), ret_addr);
    }
};

fn noArgs() std.Io.Duration {
    return .fromNanoseconds(-1);
}

fn oneArg(x: usize) std.Io.Duration {
    return .fromNanoseconds(-@as(i96, @intCast(x)));
}

test "a concurrent task's 16-aligned result stays inside its future's allocation" {
    try std.testing.expect(@alignOf(std.Io.Duration) > 8);

    var checking: OffsetAllocator = .{ .child = std.testing.allocator };
    var threaded: std.Io.Threaded = .init(checking.allocator(), .{});
    defer threaded.deinit();
    const io = threaded.io();

    var a = try io.concurrent(noArgs, .{});
    var b = try io.concurrent(oneArg, .{2});
    const ra = a.await(io);
    const rb = b.await(io);

    try std.testing.expectEqual(@as(i96, -1), ra.toNanoseconds());
    try std.testing.expectEqual(@as(i96, -2), rb.toNanoseconds());
    // Fails on Zig 0.17.1: one of the two futures' results overran its allocation.
    try std.testing.expectEqual(@as(u32, 0), checking.overruns.load(.monotonic));
}
