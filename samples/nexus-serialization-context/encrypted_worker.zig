//! The encrypted handler worker on its own, in namespace nexus-serialization-encrypted-handler.
//! worker.zig says what to create on the server first.

const std = @import("std");
const handler = @import("handler.zig");

pub fn main(init: std.process.Init) !void {
    try handler.serve(init, .encrypted);
}
