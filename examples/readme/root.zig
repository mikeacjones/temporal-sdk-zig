//! The test program for README.md's examples. Each file holds the examples of one README
//! area; `zig build test` compiles and runs them, and `zig build readme` copies their regions
//! into README.md.

test {
    _ = @import("quickstart.zig");
    _ = @import("client.zig");
    _ = @import("workflows.zig");
    _ = @import("activities.zig");
    _ = @import("workers.zig");
    _ = @import("nexus.zig");
    _ = @import("converter.zig");
    _ = @import("testing.zig");
    _ = @import("observability.zig");
}
