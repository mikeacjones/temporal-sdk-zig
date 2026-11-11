//! Internal wrappers over the sdk-core C bridge. Not part of the public API.

const std = @import("std");
const builtin = @import("builtin");

/// The sdk-core C bridge API, translated from temporal-sdk-core-c-bridge.h.
pub const c = @import("core_c");

test "core runtime can be created and freed" {
    var options = std.mem.zeroes(c.TemporalCoreRuntimeOptions);
    options.telemetry = null;
    const result = c.temporal_core_runtime_new(&options);
    try std.testing.expect(result.runtime != null);
    defer c.temporal_core_runtime_free(result.runtime);
    if (result.fail) |fail| {
        defer c.temporal_core_byte_array_free(result.runtime, fail);
        std.debug.print("runtime_new failed: {s}\n", .{fail.*.data[0..fail.*.size]});
        return error.RuntimeNewFailed;
    }
}

/// Writes one line per `TemporalCore*` struct and union in the C header: its size, alignment,
/// and each field's offset. The ABI test compares this against a committed snapshot.
fn writeAbiLayout(w: *std.Io.Writer) !void {
    @setEvalBranchQuota(1_000_000);
    const decls = comptime blk: {
        var names: []const []const u8 = &.{};
        for (std.meta.declarations(c)) |name| {
            if (!std.mem.startsWith(u8, name, "TemporalCore")) continue;
            const v = @field(c, name);
            if (@TypeOf(v) != type) continue;
            switch (@typeInfo(v)) {
                .@"struct" => |s| if (s.layout != .@"extern") continue,
                .@"union" => |u| if (u.layout != .@"extern") continue,
                else => continue,
            }
            names = names ++ .{name};
        }
        break :blk names;
    };
    inline for (decls) |name| {
        const T = @field(c, name);
        try w.print("{s} size={d} align={d}", .{ name, @sizeOf(T), @alignOf(T) });
        switch (@typeInfo(T)) {
            // Struct fields as name@offset.
            .@"struct" => |info| inline for (info.field_names) |f| {
                try w.print(" {s}@{d}", .{ f, @offsetOf(T, f) });
            },
            // Union fields all start at offset 0, so record name:size instead.
            .@"union" => |info| inline for (info.field_names, info.field_types) |f, F| {
                try w.print(" {s}:{d}", .{ f, @sizeOf(F) });
            },
            else => unreachable,
        }
        try w.writeByte('\n');
    }
}

// The snapshot is for 64-bit little-endian targets, the only ones the core supports.
test "C bridge struct layouts match the committed snapshot" {
    if (@sizeOf(usize) != 8 or builtin.cpu.arch.endian() != .little) return error.SkipZigTest;

    var aw: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer aw.deinit();
    try writeAbiLayout(&aw.writer);
    const actual = aw.written();
    const expected = @embedFile("bridge_abi.txt");

    if (!std.mem.eql(u8, actual, expected)) {
        std.debug.print(
            \\
            \\The sdk-core C bridge struct layouts changed. Review the header diff
            \\(scripts/core-header-diff.sh), update every bridge wrapper that builds or reads the
            \\changed structs, then replace src/bridge_abi.txt with:
            \\
            \\{s}
        , .{actual});
        return error.AbiLayoutChanged;
    }
}
