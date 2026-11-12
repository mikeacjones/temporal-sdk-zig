//! A small fixed-size cache that evicts its least recently used entry. The recovery
//! activities keep the executions they listed in one, so that later activities on the same
//! worker can recover them in batches.

const std = @import("std");
const Allocator = std.mem.Allocator;

/// A cache of at most `max_size` values of type `V`, keyed by string. `V` provides
/// `deinit(v: *V, gpa: Allocator) void`, which frees a value the cache owns, and
/// `clone(v: V, arena: Allocator) !V`, which copies one out of it. Safe to use from several
/// threads (activities run concurrently); the mutex is held only for the copy.
pub fn Lru(comptime V: type) type {
    return struct {
        const Self = @This();

        gpa: Allocator,
        io: std.Io,
        max_size: usize,
        mutex: std.Io.Mutex = .init,
        /// Least recently used first. Keys and values are owned with `gpa`.
        entries: std.ArrayList(Entry) = .empty,

        const Entry = struct { key: []u8, value: V };

        pub fn init(gpa: Allocator, io: std.Io, max_size: usize) Self {
            std.debug.assert(max_size > 0);
            return .{ .gpa = gpa, .io = io, .max_size = max_size };
        }

        pub fn deinit(c: *Self) void {
            for (c.entries.items) |*entry| c.free(entry);
            c.entries.deinit(c.gpa);
            c.* = undefined;
        }

        /// Stores `value` under `key`, taking ownership of `value` (allocated with the
        /// cache's allocator) once it returns without error. A value already stored under
        /// `key` is replaced and freed. When the cache is full, the least recently used entry
        /// is evicted. `key` is copied.
        pub fn put(c: *Self, key: []const u8, value: V) error{OutOfMemory}!void {
            c.mutex.lockUncancelable(c.io);
            defer c.mutex.unlock(c.io);
            if (c.find(key)) |i| {
                var old = c.entries.orderedRemove(i);
                old.value.deinit(c.gpa);
                old.value = value;
                c.entries.appendAssumeCapacity(old);
                return;
            }
            try c.entries.ensureUnusedCapacity(c.gpa, 1);
            const owned_key = try c.gpa.dupe(u8, key);
            if (c.entries.items.len == c.max_size) {
                var oldest = c.entries.orderedRemove(0);
                c.free(&oldest);
            }
            c.entries.appendAssumeCapacity(.{ .key = owned_key, .value = value });
        }

        /// A copy of the value stored under `key`, allocated from `arena`, or null if there is
        /// none. Marks the entry as the most recently used.
        pub fn get(c: *Self, arena: Allocator, key: []const u8) error{OutOfMemory}!?V {
            c.mutex.lockUncancelable(c.io);
            defer c.mutex.unlock(c.io);
            const i = c.find(key) orelse return null;
            const entry = c.entries.orderedRemove(i);
            c.entries.appendAssumeCapacity(entry);
            return try entry.value.clone(arena);
        }

        /// Removes and frees the value stored under `key`, if there is one.
        pub fn remove(c: *Self, key: []const u8) void {
            c.mutex.lockUncancelable(c.io);
            defer c.mutex.unlock(c.io);
            const i = c.find(key) orelse return;
            var entry = c.entries.orderedRemove(i);
            c.free(&entry);
        }

        /// The number of entries stored.
        pub fn len(c: *Self) usize {
            c.mutex.lockUncancelable(c.io);
            defer c.mutex.unlock(c.io);
            return c.entries.items.len;
        }

        fn find(c: *const Self, key: []const u8) ?usize {
            for (c.entries.items, 0..) |entry, i| {
                if (std.mem.eql(u8, entry.key, key)) return i;
            }
            return null;
        }

        fn free(c: *Self, entry: *Entry) void {
            c.gpa.free(entry.key);
            entry.value.deinit(c.gpa);
        }
    };
}

const testing = std.testing;

const Text = struct {
    bytes: []const u8,

    fn of(text: []const u8) !Text {
        return .{ .bytes = try testing.allocator.dupe(u8, text) };
    }

    pub fn deinit(t: *Text, gpa: Allocator) void {
        gpa.free(t.bytes);
    }

    pub fn clone(t: Text, arena: Allocator) !Text {
        return .{ .bytes = try arena.dupe(u8, t.bytes) };
    }
};

test "get returns a copy of what put stored" {
    var cache: Lru(Text) = .init(testing.allocator, testing.io, 3);
    defer cache.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();

    try cache.put("a", try .of("one"));
    const got = (try cache.get(arena.allocator(), "a")).?;
    try testing.expectEqualStrings("one", got.bytes);
    try testing.expectEqual(null, try cache.get(arena.allocator(), "missing"));
    try testing.expectEqual(1, cache.len());
}

test "put replaces the value under an existing key" {
    var cache: Lru(Text) = .init(testing.allocator, testing.io, 3);
    defer cache.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();

    try cache.put("a", try .of("one"));
    try cache.put("a", try .of("two"));
    try testing.expectEqualStrings("two", (try cache.get(arena.allocator(), "a")).?.bytes);
    try testing.expectEqual(1, cache.len());
}

test "a full cache evicts its least recently used entry" {
    var cache: Lru(Text) = .init(testing.allocator, testing.io, 2);
    defer cache.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();

    try cache.put("a", try .of("one"));
    try cache.put("b", try .of("two"));
    // Reading "a" makes "b" the least recently used.
    _ = try cache.get(arena.allocator(), "a");
    try cache.put("c", try .of("three"));

    try testing.expectEqual(2, cache.len());
    try testing.expectEqual(null, try cache.get(arena.allocator(), "b"));
    try testing.expectEqualStrings("one", (try cache.get(arena.allocator(), "a")).?.bytes);
    try testing.expectEqualStrings("three", (try cache.get(arena.allocator(), "c")).?.bytes);
}

test "remove frees an entry" {
    var cache: Lru(Text) = .init(testing.allocator, testing.io, 2);
    defer cache.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();

    try cache.put("a", try .of("one"));
    cache.remove("a");
    cache.remove("a");
    try testing.expectEqual(0, cache.len());
    try testing.expectEqual(null, try cache.get(arena.allocator(), "a"));
}
