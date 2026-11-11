const std = @import("std");
const builtin = @import("builtin");

const core_header = "deps/sdk-core/crates/sdk-core-c-bridge/include/temporal-sdk-core-c-bridge.h";
const core_lib_name = "libtemporalio_sdk_core_c_bridge.a";

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const core_lib_opt = b.option(
        []const u8,
        "core-lib",
        "Path to a prebuilt sdk-core C bridge static library (skips the cargo build)",
    );
    const protoc_opt = b.option([]const u8, "protoc", "Path to protoc used by the cargo build (default: pinned download)");

    const core_c = b.addTranslateC(.{
        .root_source_file = b.path(core_header),
        .target = target,
        .optimize = optimize,
    }).createModule();

    const temporal = b.addModule("temporal", .{
        .root_source_file = b.path("src/temporal.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
        .imports = &.{.{ .name = "core_c", .module = core_c }},
    });

    const core_lib: std.Build.LazyPath = if (core_lib_opt) |p|
        .{ .cwd_relative = p }
    else
        buildCoreLib(b, target, protoc_opt);
    linkCoreLib(temporal, target, core_lib);

    // zig build core: build only the Rust core library
    const core_step = b.step("core", "Build the sdk-core C bridge static library");
    core_step.dependOn(&b.addInstallFile(core_lib, "lib/" ++ core_lib_name).step);

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

/// Runs cargo to build the C bridge as a static library for `target` and returns its path.
///
/// Cargo runs on every build and decides itself whether anything changed (about a second
/// when nothing did). Its target directory lives in this project's .zig-cache so it never
/// writes into a fetched package directory.
fn buildCoreLib(b: *std.Build, target: std.Build.ResolvedTarget, protoc_opt: ?[]const u8) std.Build.LazyPath {
    const triple = rustTriple(target.result) orelse std.debug.panic(
        "no Rust target for {s}; build the core yourself and pass -Dcore-lib=<path>",
        .{target.result.zigTriple(b.allocator) catch "this target"},
    );

    const cargo = b.addSystemCommand(&.{ "cargo", "rustc" });
    // rustup picks the toolchain from deps/rust-toolchain.toml based on the working directory.
    cargo.setCwd(b.path("deps"));
    cargo.addArgs(&.{ "--package", "temporalio-sdk-core-c-bridge" });
    cargo.addArgs(&.{ "--release", "--locked", "--crate-type", "staticlib" });
    cargo.addArgs(&.{ "--target", triple });
    cargo.addDirectoryArg2(.cache_root, .{ .prefix = "--target-dir=", .suffix = "/cargo-target", .make_absolute = true });

    // Cargo's [env] table passes PROTOC to sdk-core's build scripts. A literal TOML string
    // (single quotes) keeps Windows backslashes intact.
    const protoc: ?std.Build.LazyPath = if (protoc_opt) |p| .{ .cwd_relative = p } else pinnedProtoc(b);
    if (protoc) |p| {
        cargo.addFileArg2(p, .{ .prefix = "--config=env.PROTOC='", .suffix = "'", .make_absolute = true });
    }
    cargo.has_side_effects = true;

    // Copy the library into a tracked output so every artifact that links it waits for cargo.
    const out = b.addWriteFiles();
    out.step.dependOn(&cargo.step);
    const built: std.Build.LazyPath = .{ .relative = .{
        .base = .local_cache,
        .sub_path = b.fmt("cargo-target/{s}/release/{s}", .{ triple, core_lib_name }),
    } };
    return out.addCopyFile(built, core_lib_name);
}

/// The protoc release pinned in build.zig.zon for the machine running the build.
fn pinnedProtoc(b: *std.Build) ?std.Build.LazyPath {
    const name: []const u8 = switch (builtin.os.tag) {
        .macos => switch (builtin.cpu.arch) {
            .aarch64 => "protoc-osx-aarch_64",
            .x86_64 => "protoc-osx-x86_64",
            else => return null,
        },
        .linux => switch (builtin.cpu.arch) {
            .aarch64 => "protoc-linux-aarch_64",
            .x86_64 => "protoc-linux-x86_64",
            else => return null,
        },
        .windows => "protoc-win64",
        else => return null,
    };
    const dep = b.lazyDependency(name, .{}) orelse return null;
    return dep.path(if (builtin.os.tag == .windows) "bin/protoc.exe" else "bin/protoc");
}

fn rustTriple(t: std.Target) ?[]const u8 {
    return switch (t.os.tag) {
        .macos => switch (t.cpu.arch) {
            .aarch64 => "aarch64-apple-darwin",
            .x86_64 => "x86_64-apple-darwin",
            else => null,
        },
        .linux => switch (t.cpu.arch) {
            .aarch64 => if (t.abi.isMusl()) "aarch64-unknown-linux-musl" else "aarch64-unknown-linux-gnu",
            .x86_64 => if (t.abi.isMusl()) "x86_64-unknown-linux-musl" else "x86_64-unknown-linux-gnu",
            else => null,
        },
        .windows => switch (t.cpu.arch) {
            .x86_64 => if (t.abi == .msvc) "x86_64-pc-windows-msvc" else "x86_64-pc-windows-gnu",
            else => null,
        },
        else => null,
    };
}

/// Links the core library and the system libraries Rust's std and the core's dependencies
/// need. From `cargo rustc ... -- --print native-static-libs` per target.
fn linkCoreLib(m: *std.Build.Module, target: std.Build.ResolvedTarget, lib: std.Build.LazyPath) void {
    m.addObjectFile(lib);
    switch (target.result.os.tag) {
        .macos => {
            m.linkFramework("IOKit", .{});
            m.linkFramework("CoreFoundation", .{});
            m.linkFramework("Security", .{});
            m.linkSystemLibrary("iconv", .{});
        },
        .linux => {
            m.linkSystemLibrary("pthread", .{});
            m.linkSystemLibrary("dl", .{});
            m.linkSystemLibrary("m", .{});
        },
        .windows => {
            for ([_][]const u8{ "ws2_32", "userenv", "ntdll", "bcrypt", "advapi32", "kernel32", "secur32", "crypt32", "ncrypt" }) |name| {
                m.linkSystemLibrary(name, .{});
            }
        },
        else => {},
    }
}
