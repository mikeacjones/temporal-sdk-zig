const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const temporal = b.addModule("temporal", .{
        .root_source_file = b.path("src/temporal.zig"),
        .target = target,
        .optimize = optimize,
    });

    // zig build test
    const unit_tests = b.addTest(.{ .root_module = temporal });
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&b.addRunArtifact(unit_tests).step);

    // zig build fmt
    const fmt_paths: []const std.Build.LazyPath = &.{
        b.path("build.zig"),
        b.path("build.zig.zon"),
        b.path("src"),
        b.path("samples"),
    };
    const fmt_step = b.step("fmt", "Check formatting");
    fmt_step.dependOn(&b.addFmt(.{ .paths = fmt_paths, .check = true }).step);

    // zig build samples / zig build run-<sample>
    const samples_step = b.step("samples", "Build all samples");
    const samples = [_][]const u8{"hello"};
    for (samples) |name| {
        const exe = b.addExecutable(.{
            .name = name,
            .root_module = b.createModule(.{
                .root_source_file = b.path(b.fmt("samples/{s}/main.zig", .{name})),
                .target = target,
                .optimize = optimize,
                .imports = &.{.{ .name = "temporal", .module = temporal }},
            }),
        });
        samples_step.dependOn(&b.addInstallArtifact(exe, .{}).step);
        const run = b.addRunArtifact(exe);
        b.step(b.fmt("run-{s}", .{name}), b.fmt("Run the {s} sample", .{name})).dependOn(&run.step);
    }
}
