//! Internal wrappers over the sdk-core C bridge. Not part of the public API.

const std = @import("std");
const builtin = @import("builtin");

/// The sdk-core C bridge API, translated from temporal-sdk-core-c-bridge.h.
pub const c = @import("core_c");

/// Byte buffers and metadata crossing the C boundary, and their ownership rules.
pub const bytes = @import("bridge/bytes.zig");

/// Turns core callbacks into results std.Io code can wait for.
pub const Completion = @import("bridge/completion.zig").Completion;

/// The core runtime: threads, telemetry, and process-wide worker settings.
pub const Runtime = @import("bridge/Runtime.zig");

/// Local dev and time-skipping test servers started by the core.
pub const EphemeralServer = @import("bridge/EphemeralServer.zig");

/// A connection to a Temporal server and raw gRPC calls over it.
pub const Connection = @import("bridge/Connection.zig");

/// A core worker: polls tasks and takes completions as serialized protobuf.
pub const Worker = @import("bridge/Worker.zig");

test {
    _ = bytes;
    _ = @import("bridge/completion.zig");
    _ = Runtime;
    _ = EphemeralServer;
    _ = Connection;
    _ = Worker;
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
