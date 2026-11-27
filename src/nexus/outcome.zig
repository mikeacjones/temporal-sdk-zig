//! Carries the failure behind a client wait's outcome error (such as `error.UpdateFailed`)
//! from the wait to the Nexus handler error the SDK builds when a handler returns that error.
//! A handler's `nexus.Context.allocator()` is a `Slot`'s allocator: a wait given it records
//! its failure in the slot, and the request's error mapping reads it back.

const std = @import("std");
const failure_mod = @import("../failure.zig");
const Allocator = std.mem.Allocator;
const Alignment = std.mem.Alignment;

/// The last outcome a wait recorded for one Nexus request. Not thread-safe, like the arena it
/// forwards to. Owns nothing: the failures it points to live in the backing allocator.
pub const Slot = struct {
    backing: Allocator = undefined,
    last: ?Recorded = null,

    pub const Recorded = struct { err: anyerror, failure: *const failure_mod.Failure };

    /// The allocator handed to handlers: allocations go to `backing`. The slot must not move
    /// while the allocator is in use.
    pub fn allocator(s: *Slot, backing: Allocator) Allocator {
        s.backing = backing;
        return .{ .ptr = s, .vtable = &vtable };
    }

    /// Records `f` for `err` when `a` is a Slot's allocator; otherwise does nothing. Copies
    /// the `Failure` struct (not what it points to, which must live as long as `a`'s memory)
    /// into the backing allocator; out of memory, it records nothing.
    pub fn note(a: Allocator, err: anyerror, f: *const failure_mod.Failure) void {
        if (a.vtable != &vtable) return;
        const s: *Slot = @ptrCast(@alignCast(a.ptr));
        const kept = s.backing.create(failure_mod.Failure) catch return;
        kept.* = f.*;
        s.last = .{ .err = err, .failure = kept };
    }

    /// The failure recorded for `err`, if the last recorded outcome was `err`.
    pub fn failureFor(s: *const Slot, err: anyerror) ?*const failure_mod.Failure {
        const r = s.last orelse return null;
        return if (r.err == err) r.failure else null;
    }

    const vtable: Allocator.VTable = .{ .alloc = alloc, .resize = resize, .remap = remap, .free = free };

    fn slot(ptr: *anyopaque) *Slot {
        return @ptrCast(@alignCast(ptr));
    }

    fn alloc(ptr: *anyopaque, len: usize, alignment: Alignment, ret_addr: usize) ?[*]u8 {
        return slot(ptr).backing.rawAlloc(len, alignment, ret_addr);
    }

    fn resize(ptr: *anyopaque, memory: []u8, alignment: Alignment, new_len: usize, ret_addr: usize) bool {
        return slot(ptr).backing.rawResize(memory, alignment, new_len, ret_addr);
    }

    fn remap(ptr: *anyopaque, memory: []u8, alignment: Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        return slot(ptr).backing.rawRemap(memory, alignment, new_len, ret_addr);
    }

    fn free(ptr: *anyopaque, memory: []u8, alignment: Alignment, ret_addr: usize) void {
        slot(ptr).backing.rawFree(memory, alignment, ret_addr);
    }
};

const testing = std.testing;

test "a slot's allocator forwards to its backing allocator" {
    var s: Slot = .{};
    const a = s.allocator(testing.allocator);
    const buf = try a.alloc(u8, 16);
    @memset(buf, 7);
    const grown = try a.realloc(buf, 64);
    try testing.expectEqual(@as(u8, 7), grown[15]);
    a.free(grown);
}

test "note records the failure only through a slot's allocator, and failureFor matches the error" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var s: Slot = .{};
    const a = s.allocator(arena.allocator());
    var f: failure_mod.Failure = .{ .message = "the update's activity failed" };

    Slot.note(arena.allocator(), error.UpdateFailed, &f);
    try testing.expectEqual(null, s.failureFor(error.UpdateFailed));

    Slot.note(a, error.UpdateFailed, &f);
    f.message = "changed after the note";
    const got = s.failureFor(error.UpdateFailed) orelse return error.TestExpectedFailure;
    try testing.expectEqualStrings("the update's activity failed", got.message);
    try testing.expectEqual(null, s.failureFor(error.WorkflowFailed));

    Slot.note(a, error.WorkflowFailed, &f);
    try testing.expectEqual(null, s.failureFor(error.UpdateFailed));
    try testing.expectEqualStrings("changed after the note", s.failureFor(error.WorkflowFailed).?.message);
}

test "note records nothing when copying the failure runs out of memory" {
    var failing: std.testing.FailingAllocator = .init(testing.allocator, .{ .fail_index = 0 });
    var s: Slot = .{};
    const a = s.allocator(failing.allocator());
    const f: failure_mod.Failure = .{ .message = "m" };
    Slot.note(a, error.UpdateFailed, &f);
    try testing.expectEqual(null, s.last);
}
