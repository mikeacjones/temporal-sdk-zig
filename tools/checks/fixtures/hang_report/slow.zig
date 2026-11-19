//! A test that runs past the hang report's limit and then passes, for the "check hang report
//! slow test passes" and "check hang report off" cases.

const std = @import("std");

test "finishes after the limit" {
    // A fixed sleep is what this fixture tests, so AGENTS.md's rule against them does not apply.
    try std.testing.io.sleep(.fromSeconds(3), .awake);
}
