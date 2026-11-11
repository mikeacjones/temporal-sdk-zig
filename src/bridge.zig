//! Internal wrappers over the sdk-core C bridge. Not part of the public API.

const std = @import("std");

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
