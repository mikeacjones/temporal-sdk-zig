//! A `std.mem.Allocator` that wraps `backing` and blocks cancels on `io` around each alloc,
//! resize, remap and free. It backs the arena of an activity or a Nexus request (the
//! worker's, and `testing.ActivityEnvironment`'s). Such a task is canceled once, and in a
//! Debug build an allocation can wait on the task's `std.Io` without returning the error:
//! std's stack capture waits for its debug-info lock and drops `error.Canceled`. That would
//! lose the cancel and could leave the task running for good. With cancels blocked, the
//! cancel stays pending and reaches the task's next wait instead.
//!
//! Internal: not reachable from `@import("temporal")`.
const CancelBlockingAllocator = @This();
const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

/// The Io whose current task has cancels blocked during each call.
io: Io,
/// Where the memory comes from. Borrowed. Memory from the wrapper may be freed straight
/// through `backing`: pointers, alignments and lengths pass through unchanged.
backing: Allocator,

/// An allocator that points at `self`, valid while `self` stays at this address.
pub fn allocator(self: *CancelBlockingAllocator) Allocator {
    return .{ .ptr = self, .vtable = &vtable };
}

const vtable: Allocator.VTable = .{ .alloc = alloc, .resize = resize, .remap = remap, .free = free };

fn alloc(ptr: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
    const self: *CancelBlockingAllocator = @ptrCast(@alignCast(ptr));
    const prev = self.io.swapCancelProtection(.blocked);
    defer _ = self.io.swapCancelProtection(prev);
    return self.backing.rawAlloc(len, alignment, ret_addr);
}

fn resize(ptr: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
    const self: *CancelBlockingAllocator = @ptrCast(@alignCast(ptr));
    const prev = self.io.swapCancelProtection(.blocked);
    defer _ = self.io.swapCancelProtection(prev);
    return self.backing.rawResize(memory, alignment, new_len, ret_addr);
}

fn remap(ptr: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
    const self: *CancelBlockingAllocator = @ptrCast(@alignCast(ptr));
    const prev = self.io.swapCancelProtection(.blocked);
    defer _ = self.io.swapCancelProtection(prev);
    return self.backing.rawRemap(memory, alignment, new_len, ret_addr);
}

fn free(ptr: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
    const self: *CancelBlockingAllocator = @ptrCast(@alignCast(ptr));
    const prev = self.io.swapCancelProtection(.blocked);
    defer _ = self.io.swapCancelProtection(prev);
    self.backing.rawFree(memory, alignment, ret_addr);
}
