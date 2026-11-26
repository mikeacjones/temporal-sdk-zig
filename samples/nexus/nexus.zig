//! The nexus sample's service contract and handler, for other samples to import
//! (`.imports = .{"nexus"}` in their sample.zon).

pub const service = @import("service.zig");
pub const handler = @import("handler.zig");
