//! Byte buffers crossing the sdk-core C boundary.
//!
//! Ownership rules:
//!
//! - Data passed into the core is borrowed (`TemporalCoreByteArrayRef`). The caller keeps it
//!   alive for the duration the core function documents: until it returns for synchronous
//!   calls, and until the callback fires for asynchronous ones. Build these refs from memory
//!   the caller owns (an arena for the call works well).
//! - Data the core returns (`TemporalCoreByteArray`) is owned by the caller and must be freed
//!   exactly once with `temporal_core_byte_array_free`. Wrap it in `Owned` and call `deinit`.
//!
//! The core turns refs into Rust slices even when they are empty, and a Rust slice may not
//! have a null pointer, so empty refs here always point at a static empty buffer.

const std = @import("std");
const c = @import("core_c");

pub const Ref = c.TemporalCoreByteArrayRef;
pub const RefArray = c.TemporalCoreByteArrayRefArray;
pub const MetadataRef = c.TemporalCoreMetadataRef;

const empty_bytes: [0]u8 = .{};
const empty_refs: [0]Ref = .{};

/// A borrowed view of `bytes` for passing into the core.
pub fn ref(bytes: []const u8) Ref {
    return .{ .data = if (bytes.len == 0) &empty_bytes else bytes.ptr, .size = bytes.len };
}

/// A borrowed view of `refs` for passing into the core.
pub fn refArray(refs: []const Ref) RefArray {
    return .{ .data = if (refs.len == 0) &empty_refs else refs.ptr, .size = refs.len };
}

/// The bytes a ref points to. Only valid while the memory behind the ref is.
pub fn slice(r: Ref) []const u8 {
    if (r.size == 0) return &.{};
    return r.data[0..r.size];
}

pub const MetadataEntry = struct {
    key: []const u8,
    value: []const u8,
};

pub const EncodeError = error{ OutOfMemory, NewlineInKey, NewlineInValue };

/// Encodes gRPC metadata as the core expects: one ref per entry, each `key\nvalue`. The core
/// splits at the first newline, so keys may not contain one; values may. All memory comes
/// from `arena` and must outlive the core call.
pub fn metadata(arena: std.mem.Allocator, entries: []const MetadataEntry) EncodeError!MetadataRef {
    const refs = try arena.alloc(Ref, entries.len);
    for (entries, refs) |e, *r| {
        if (std.mem.indexOfScalar(u8, e.key, '\n') != null) return error.NewlineInKey;
        const joined = try std.mem.concat(arena, u8, &.{ e.key, "\n", e.value });
        r.* = ref(joined);
    }
    return refArray(refs);
}

/// Encodes a string map as `k1\nv1\nk2\nv2`, the core's newline-delimited map (used for
/// metric tags, OTLP headers, and similar). Neither keys nor values may contain a newline.
/// Memory comes from `arena` and must outlive the core call.
pub fn newlineMap(arena: std.mem.Allocator, entries: []const MetadataEntry) EncodeError!Ref {
    var out: std.ArrayList(u8) = .empty;
    for (entries, 0..) |e, i| {
        if (std.mem.indexOfScalar(u8, e.key, '\n') != null) return error.NewlineInKey;
        if (std.mem.indexOfScalar(u8, e.value, '\n') != null) return error.NewlineInValue;
        if (i > 0) try out.append(arena, '\n');
        try out.appendSlice(arena, e.key);
        try out.append(arena, '\n');
        try out.appendSlice(arena, e.value);
    }
    return ref(out.items);
}

/// A byte array the core returned, owned by the caller until `deinit`.
pub const Owned = struct {
    array: *const c.TemporalCoreByteArray,
    /// The runtime that allocated the array. Null for calls made without one (for example
    /// env config loading); freeing works either way, the runtime only recycles the buffer.
    runtime: ?*c.TemporalCoreRuntime,

    /// Takes ownership of `array` if it is non-null.
    pub fn take(runtime: ?*c.TemporalCoreRuntime, array: ?*const c.TemporalCoreByteArray) ?Owned {
        return .{ .array = array orelse return null, .runtime = runtime };
    }

    /// The contents. Valid until `deinit`.
    pub fn bytes(o: Owned) []const u8 {
        if (o.array.size == 0) return &.{};
        return o.array.data[0..o.array.size];
    }

    /// Copies the contents into memory from `gpa`, so the array can be freed right away.
    pub fn dupe(o: Owned, gpa: std.mem.Allocator) error{OutOfMemory}![]u8 {
        return gpa.dupe(u8, o.bytes());
    }

    pub fn deinit(o: Owned) void {
        c.temporal_core_byte_array_free(o.runtime, o.array);
    }
};

// ---------------------------------------------------------------- tests

const testing = std.testing;

test "empty refs never carry a null pointer" {
    const r = ref("");
    try testing.expectEqual(@as(usize, 0), r.size);
    try testing.expect(@intFromPtr(r.data) != 0);
    const ra = refArray(&.{});
    try testing.expect(@intFromPtr(ra.data) != 0);
    try testing.expectEqualStrings("", slice(r));
    try testing.expectEqualStrings("abc", slice(ref("abc")));
}

test "metadata entries are key-newline-value, and keys may not contain a newline" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const md = try metadata(a, &.{
        .{ .key = "authorization", .value = "Bearer abc" },
        .{ .key = "x-multiline", .value = "line1\nline2" },
    });
    try testing.expectEqual(@as(usize, 2), md.size);
    try testing.expectEqualStrings("authorization\nBearer abc", slice(md.data[0]));
    try testing.expectEqualStrings("x-multiline\nline1\nline2", slice(md.data[1]));
    try testing.expectError(error.NewlineInKey, metadata(a, &.{.{ .key = "bad\nkey", .value = "v" }}));
}

test "newline maps join keys and values, and reject newlines in either" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const m = try newlineMap(a, &.{ .{ .key = "service", .value = "billing" }, .{ .key = "env", .value = "prod" } });
    try testing.expectEqualStrings("service\nbilling\nenv\nprod", slice(m));
    try testing.expectEqualStrings("", slice(try newlineMap(a, &.{})));
    try testing.expectError(error.NewlineInValue, newlineMap(a, &.{.{ .key = "k", .value = "a\nb" }}));
}

test "byte arrays returned by the core are read and freed" {
    // Env config loading needs no runtime and returns JSON on success or a message on failure.
    const good = c.temporal_core_client_env_config_load(&.{
        .path = ref(""),
        .data = ref(
            \\[profile.default]
            \\address = "localhost:7233"
            \\namespace = "samples"
        ),
        .config_file_strict = false,
        .env_vars = ref(""),
    });
    try testing.expect(good.fail == null);
    const json = Owned.take(null, good.success).?;
    defer json.deinit();
    try testing.expect(std.mem.indexOf(u8, json.bytes(), "\"samples\"") != null);

    const bad = c.temporal_core_client_env_config_load(&.{
        .path = ref(""),
        .data = ref("[profile.default\naddress ="),
        .config_file_strict = true,
        .env_vars = ref(""),
    });
    try testing.expect(bad.success == null);
    const message = Owned.take(null, bad.fail).?;
    defer message.deinit();
    try testing.expect(message.bytes().len > 0);
    const copy = try message.dupe(testing.allocator);
    defer testing.allocator.free(copy);
    try testing.expectEqualStrings(message.bytes(), copy);
}
