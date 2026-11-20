//! A test that runs past any short limit and then passes, for the "check hang report off" case:
//! with the report off, the runner's output must be Zig's, byte for byte.

const std = @import("std");

test "finishes after the limit" {
    // A fixed sleep is what this fixture tests, so AGENTS.md's rule against them does not apply.
    try std.testing.io.sleep(.fromSeconds(3), .awake);
}
