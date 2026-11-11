const std = @import("std");
const builtin = @import("builtin");
const protobuf = @import("protobuf");
const protos = @import("deps/protos.zig");

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

    const protobuf_dep = b.dependency("protobuf", .{ .target = target, .optimize = optimize });

    const temporal = b.addModule("temporal", .{
        .root_source_file = b.path("src/temporal.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
        .imports = &.{
            .{ .name = "core_c", .module = core_c },
            .{ .name = "protobuf", .module = protobuf_dep.module("protobuf") },
        },
    });

    // zig build gen-proto: regenerate src/proto from the sdk-core .proto files.
    const gen_proto = b.step("gen-proto", "Regenerate src/proto from deps/sdk-core protos");
    gen_proto.dependOn(genProto(b, protobuf_dep, protoc_opt));

    const core_lib: std.Build.LazyPath = if (core_lib_opt) |p|
        .{ .cwd_relative = p }
    else
        buildCoreLib(b, target, protoc_opt);
    linkCoreLib(temporal, target, core_lib);

    // zig build core: build only the Rust core library
    const core_step = b.step("core", "Build the sdk-core C bridge static library");
    core_step.dependOn(&b.addInstallFile(core_lib, "lib/" ++ core_lib_name).step);

    // zig build test
    const test_filters = b.option([]const []const u8, "test-filter", "Run only tests whose names contain this text (repeatable)") orelse &.{};
    const unit_tests = b.addTest(.{ .root_module = temporal, .filters = test_filters });
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&b.addRunArtifact(unit_tests).step);

    // zig build fmt
    const fmt_paths: []const std.Build.LazyPath = &.{
        b.path("build.zig"),
        b.path("build.zig.zon"),
        b.path("src"),
        b.path("samples"),
        b.path("tools"),
    };
    const fmt_step = b.step("fmt", "Check formatting");
    fmt_step.dependOn(&b.addFmt(.{ .paths = fmt_paths, .check = true }).step);

    // zig build samples / zig build samples-test
    //
    // Each sample in samples/<name> (a port of samples-go/<name>) builds a worker and a starter
    // program; its tests and its run against a dev server make up samples-test. The runner
    // runs natively (it starts processes), so samples-test is for the host target.
    const samples_step = b.step("samples", "Build all samples");
    const samples_test_step = b.step("samples-test", "Build every sample, run its tests, and validate it against a dev server");
    const runner = b.addExecutable(.{
        .name = "samples-runner",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tools/samples_runner.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "temporal", .module = temporal }},
        }),
    });
    const run_samples = b.addRunArtifact(runner);
    run_samples.addDirectoryArg(b.path("."));
    run_samples.addDirectoryArg(b.addWriteFiles().getDirectory());
    run_samples.has_side_effects = true;
    if (b.option([]const u8, "sample", "Only validate this sample in samples-test")) |only| run_samples.addArgs(&.{ "--only", only });
    samples_test_step.dependOn(&run_samples.step);

    for (samples) |sample| {
        // The temporal module, plus other samples' modules this sample shares code with.
        var imports: std.ArrayList(std.Build.Module.Import) = .empty;
        imports.append(b.allocator, .{ .name = "temporal", .module = temporal }) catch @panic("OOM");
        for (sample.imports) |other| imports.append(b.allocator, .{ .name = other, .module = b.createModule(.{
            .root_source_file = b.path(b.fmt("samples/{s}/{s}.zig", .{ other, other })),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "temporal", .module = temporal }},
        }) }) catch @panic("OOM");

        const programs = [_][]const u8{ "worker", "starter" };
        for (programs) |program| {
            const exe = b.addExecutable(.{
                .name = b.fmt("{s}-{s}", .{ sample.name, program }),
                .root_module = b.createModule(.{
                    .root_source_file = b.path(b.fmt("samples/{s}/{s}.zig", .{ sample.name, program })),
                    .target = target,
                    .optimize = optimize,
                    .imports = imports.items,
                }),
            });
            samples_step.dependOn(&b.addInstallArtifact(exe, .{}).step);
            run_samples.addArg(b.fmt("{s}:{s}", .{ sample.name, program }));
            run_samples.addArtifactArg(exe);
        }
        for (sample.tests) |test_file| {
            const t = b.addTest(.{
                .name = b.fmt("{s}-{s}", .{ sample.name, test_file }),
                .root_module = b.createModule(.{
                    .root_source_file = b.path(b.fmt("samples/{s}/{s}", .{ sample.name, test_file })),
                    .target = target,
                    .optimize = optimize,
                    .imports = imports.items,
                }),
            });
            samples_test_step.dependOn(&b.addRunArtifact(t).step);
        }
    }
}

/// Ported samples-go samples. Each has worker.zig, starter.zig and sample.zon in
/// samples/<name>; `tests` are its ported Go tests. `imports` names other samples whose
/// samples/<other>/<other>.zig this sample uses (Go samples import each other's packages).
const Sample = struct { name: []const u8, tests: []const []const u8 = &.{}, imports: []const []const u8 = &.{} };
const samples = [_]Sample{
    .{ .name = "helloworld", .tests = &.{ "helloworld_test.zig", "replay_test.zig" } },
    .{ .name = "greetings", .tests = &.{"workflow_test.zig"} },
    .{ .name = "branch", .tests = &.{"workflow_test.zig"} },
    .{ .name = "splitmerge-future", .tests = &.{"splitmerge_workflow_test.zig"} },
    .{ .name = "splitmerge-selector", .tests = &.{"splitmerge_workflow_test.zig"} },
    .{ .name = "choice-exclusive", .tests = &.{"workflow_test.zig"} },
    .{ .name = "choice-multi", .tests = &.{"workflow_test.zig"} },
    .{ .name = "goroutine", .tests = &.{"goroutine_workflow_test.zig"} },
    .{ .name = "timer", .tests = &.{"workflow_test.zig"} },
    .{ .name = "start-delay", .imports = &.{"helloworld"} },
    .{ .name = "retryactivity", .tests = &.{"retry_activity_workflow_test.zig"} },
    .{ .name = "cron" },
};

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

/// Compiles the files listed in deps/protos.zig into Zig sources under src/proto.
/// The generated code is committed, so normal builds never run protoc for Zig code.
fn genProto(b: *std.Build, protobuf_dep: *std.Build.Dependency, protoc_opt: ?[]const u8) *std.Build.Step {
    var sources: [protos.files.len]std.Build.LazyPath = undefined;
    for (protos.files, &sources) |f, *src| src.* = b.path(b.pathJoin(&.{ protos.root, f }));
    var includes: [protos.include_dirs.len]std.Build.LazyPath = undefined;
    for (protos.include_dirs, &includes) |d, *inc| inc.* = b.path(b.pathJoin(&.{ protos.root, d }));

    const step = protobuf.RunProtocStep.create(protobuf_dep.builder, b.graph.host, .{
        .destination_directory = b.path("src/proto"),
        .source_files = b.allocator.dupe(std.Build.LazyPath, &sources) catch @panic("OOM"),
        .include_directories = b.allocator.dupe(std.Build.LazyPath, &includes) catch @panic("OOM"),
        .protoc = if (protoc_opt) |p| .{ .cwd_relative = p } else pinnedProtoc(b),
        // Keep fields a newer server sends that this SDK doesn't know yet.
        .preserve_unknown_fields = true,
    });

    // Rebuild the src/proto.zig index over the generated packages.
    const index = b.addSystemCommand(&.{ "bash", "scripts/gen-proto-index.sh" });
    index.setCwd(b.path("."));
    index.has_side_effects = true;
    index.step.dependOn(step.step);
    return &index.step;
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
            // Rust's std needs the _Unwind_* functions (libgcc_s on glibc). link_libcpp makes
            // Zig build and link LLVM's libunwind, which provides them without a system lib.
            m.link_libcpp = true;
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
