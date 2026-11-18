const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.option([]const u8, "fuzz-target", "Select a fuzz target");
    const tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("targets.zig"),
            .target = b.graph.host,
            .optimize = .Debug,
        }),
        .filters = if (target) |name| &.{b.fmt("fuzz {s}", .{name})} else &.{},
    });
    b.step("fuzz", "Fuzz a fixture").dependOn(&b.addRunArtifact(tests).step);
}
