//! A project that depends on the SDK the way a user's does, through a fetched release
//! tarball. scripts/smoke-release.sh copies it, adds the tarball as the temporal dependency
//! and runs it.
const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});
    const core_bundle = b.option([]const u8, "core-bundle", "Passed to the SDK: an extracted core bundle to link instead of the one build.zig.zon pins");

    const temporal = if (core_bundle) |dir|
        b.dependency("temporal", .{ .target = target, .optimize = optimize, .@"core-bundle" = dir })
    else
        b.dependency("temporal", .{ .target = target, .optimize = optimize });

    const exe = b.addExecutable(.{
        .name = "release-smoke",
        .root_module = b.createModule(.{
            .root_source_file = b.path("main.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "temporal", .module = temporal.module("temporal") }},
        }),
    });
    b.installArtifact(exe);
    b.step("run", "Run the smoke test").dependOn(&b.addRunArtifact(exe).step);
}
