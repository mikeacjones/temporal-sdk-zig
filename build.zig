const std = @import("std");
const builtin = @import("builtin");
const protobuf = @import("protobuf");
const protos = @import("deps/protos.zig");

const core_header_name = "temporal-sdk-core-c-bridge.h";
const core_header = "deps/sdk-core/crates/sdk-core-c-bridge/include/" ++ core_header_name;
const core_lib_name = "libtemporalio_sdk_core_c_bridge.a";

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const core_lib_opt = b.option(
        []const u8,
        "core-lib",
        "Path to a prebuilt sdk-core C bridge static library (skips the cargo build; the header comes from deps/sdk-core)",
    );
    const core_bundle_opt = b.option(
        []const u8,
        "core-bundle",
        "Path to an extracted core bundle (lib/ and include/, from scripts/package-core-bundle.sh) to link instead of building the core",
    );
    const protoc_opt = b.option([]const u8, "protoc", "Path to protoc used by the cargo build (default: pinned download)");
    const test_filters = b.option([]const []const u8, "test-filter", "Run only tests whose names contain this text (repeatable)") orelse &.{};

    // A fetched package has neither the submodule nor samples/, so the steps that need them
    // exist only in a checkout.
    const have_submodule = pathExists(b, core_header);
    const have_samples = pathExists(b, "samples");

    const protobuf_dep = b.dependency("protobuf", .{ .target = target, .optimize = optimize });

    const temporal = b.addModule("temporal", .{
        .root_source_file = b.path("src/temporal.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
        .imports = &.{
            .{ .name = "protobuf", .module = protobuf_dep.module("protobuf") },
        },
    });

    // zig build gen-proto: regenerate src/proto from the sdk-core .proto files.
    if (have_submodule) {
        const gen_proto = b.step("gen-proto", "Regenerate src/proto from deps/sdk-core protos");
        gen_proto.dependOn(genProto(b, protobuf_dep, protoc_opt));
    }

    // Null only while the core bundle dependency is still being fetched: the build runner
    // fetches it and configures again before building anything.
    const core = selectCore(b, target, .{
        .core_lib = core_lib_opt,
        .core_bundle = core_bundle_opt,
        .protoc = protoc_opt,
        .have_submodule = have_submodule,
    }) orelse return;
    temporal.addImport("core_c", b.addTranslateC(.{
        .root_source_file = core.header,
        .target = target,
        .optimize = optimize,
    }).createModule());
    linkCoreLib(temporal, target, core.lib);

    // zig build core: build only the Rust core library
    const core_step = b.step("core", "Build the sdk-core C bridge static library");
    core_step.dependOn(&b.addInstallFile(core.lib, "lib/" ++ core_lib_name).step);
    core_step.dependOn(&b.addInstallFile(core.header, "include/" ++ core_header_name).step);

    // zig build test
    const unit_tests = b.addTest(.{ .root_module = temporal, .filters = test_filters });
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&b.addRunArtifact(unit_tests).step);

    if (have_samples) addSampleSteps(b, target, optimize, temporal);
    if (pathExists(b, "examples/readme")) addReadmeSteps(b, target, optimize, temporal, test_step, test_filters);
}

/// The files under examples/readme whose regions README.md copies, in a fixed order.
const readme_example_files = [_][]const u8{
    "examples/readme/quickstart.zig",
    "examples/readme/client.zig",
    "examples/readme/workflows.zig",
    "examples/readme/activities.zig",
    "examples/readme/workers.zig",
    "examples/readme/converter.zig",
    "examples/readme/testing.zig",
    "examples/readme/observability.zig",
};

/// README.md's code blocks are copies of regions in examples/readme, which `test` compiles and
/// runs; `readme-check` fails when a copy differs, and `readme` rewrites the copies. A fetched
/// package has no examples/, so these steps exist only in a checkout.
fn addReadmeSteps(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    temporal: *std.Build.Module,
    test_step: *std.Build.Step,
    test_filters: []const []const u8,
) void {
    const examples = b.addTest(.{
        .name = "readme-examples",
        .root_module = b.createModule(.{
            .root_source_file = b.path("examples/readme/root.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "temporal", .module = temporal }},
        }),
        .filters = test_filters,
    });
    test_step.dependOn(&b.addRunArtifact(examples).step);

    const tool_module = b.createModule(.{
        .root_source_file = b.path("tools/readme_examples.zig"),
        .target = b.graph.host,
        .optimize = .Debug,
    });
    const tool = b.addExecutable(.{ .name = "readme-examples", .root_module = tool_module });
    test_step.dependOn(&b.addRunArtifact(b.addTest(.{ .name = "readme-examples-tool", .root_module = tool_module, .filters = test_filters })).step);

    inline for (.{ .{ "readme-check", "check", "Check that README.md's code blocks equal their regions in examples/readme" }, .{ "readme", "update", "Rewrite README.md's code blocks from their regions in examples/readme" } }) |s| {
        const run = b.addRunArtifact(tool);
        run.addArg(s[1]);
        run.addDirectoryArg(b.path("."));
        run.addArg("README.md");
        run.addArgs(&readme_example_files);
        // The tool reads the files itself, by path, so the run must not be cached.
        run.has_side_effects = true;
        const step = b.step(s[0], s[2]);
        step.dependOn(&run.step);
        if (std.mem.eql(u8, s[1], "check")) test_step.dependOn(&run.step);
    }
}

fn addSampleSteps(b: *std.Build, target: std.Build.ResolvedTarget, optimize: std.builtin.OptimizeMode, temporal: *std.Build.Module) void {
    // zig build fmt
    const fmt_paths: []const std.Build.LazyPath = &.{
        b.path("build.zig"),
        b.path("build.zig.zon"),
        b.path("src"),
        b.path("samples"),
        b.path("tools"),
        b.path("examples"),
    };
    const fmt_step = b.step("fmt", "Check formatting");
    fmt_step.dependOn(&b.addFmt(.{ .paths = fmt_paths, .check = true }).step);

    // zig build samples / zig build samples-test
    //
    // Each sample in samples/<name> builds a worker and a starter
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

    // samples-test runs most samples' workers and starters from one program,
    // samples/dispatch.zig, which picks the one named in SAMPLE_PROGRAM: compiling each
    // separately took most of the suite's time. Its module is rooted at samples/, so a sample
    // that imports another sample's module (a file can't be in two modules) keeps programs of
    // its own. `zig build samples-index` rewrites dispatch.zig from the samples found; CI checks
    // that it is current.
    const samples = findSamples(b);
    const dispatch_source = dispatchSource(b, samples);
    const samples_index = b.addUpdateSourceFiles();
    samples_index.addBytesToSource(dispatch_source, "samples/dispatch.zig");
    b.step("samples-index", "Rewrite samples/dispatch.zig from the samples under samples/").dependOn(&samples_index.step);
    // The samples/ listing is already a configure input, which covers dispatch.zig appearing.
    const committed: ?[]const u8 = b.root.root_dir.handle.readFileAlloc(b.graph.io, b.pathJoin(&.{ b.root.sub_path, "samples/dispatch.zig" }), b.allocator, .limited(1024 * 1024)) catch null;
    if (committed != null) b.dependOnFileContents(b.path("samples/dispatch.zig"));
    // While dispatch.zig is stale (a sample was just added), every sample runs its own programs.
    const dispatch: ?*std.Build.Step.Compile = if (committed != null and std.mem.eql(u8, committed.?, dispatch_source)) b.addExecutable(.{
        .name = "samples-dispatch",
        .root_module = b.createModule(.{
            .root_source_file = b.path("samples/dispatch.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "temporal", .module = temporal }},
        }),
    }) else null;

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

        // The runner runs the worker and the starter; other programs (`programs` in
        // sample.zon) are only built, for running by hand.
        const run_programs = [_][]const u8{ "worker", "starter" };
        var programs: std.ArrayList([]const u8) = .empty;
        programs.appendSlice(b.allocator, &run_programs) catch @panic("OOM");
        programs.appendSlice(b.allocator, sample.programs) catch @panic("OOM");
        for (programs.items, 0..) |program, i| {
            const module = b.createModule(.{
                .root_source_file = b.path(b.fmt("samples/{s}/{s}.zig", .{ sample.name, program })),
                .target = target,
                .optimize = optimize,
                .imports = imports.items,
            });
            const exe = b.addExecutable(.{ .name = b.fmt("{s}-{s}", .{ sample.name, program }), .root_module = module });
            samples_step.dependOn(&b.addInstallArtifact(exe, .{}).step);
            if (i >= run_programs.len) continue;
            run_samples.addArg(b.fmt("{s}:{s}", .{ sample.name, program }));
            run_samples.addArtifactArg(if (dispatch != null and sample.imports.len == 0) dispatch.? else exe);
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

/// A sample in samples/<name>: worker.zig, starter.zig, and sample.zon, whose `tests` are its
/// test files, whose `imports` name other samples whose samples/<other>/<other>.zig it
/// uses, and whose `programs` name extra programs (samples/<name>/<program>.zig) to build. The samples runner reads the rest of sample.zon.
const Sample = struct { name: []const u8, tests: []const []const u8 = &.{}, imports: []const []const u8 = &.{}, programs: []const []const u8 = &.{} };

/// Every directory under samples/ with a sample.zon, by name. The listing and the manifests
/// are configure inputs, so adding a sample or changing its manifest reconfigures the build.
fn findSamples(b: *std.Build) []const Sample {
    const io = b.graph.io;
    b.dependOnDirectoryContents(b.path("samples"));
    var dir = b.root.openDir(io, "samples", .{ .iterate = true }) catch |err| std.debug.panic("cannot open samples/: {t}", .{err});
    defer dir.close(io);
    var found: std.ArrayList(Sample) = .empty;
    var it = dir.iterate();
    while (it.next(io) catch |err| std.debug.panic("cannot list samples/: {t}", .{err})) |entry| {
        if (entry.kind != .directory) continue;
        const manifest_path = b.fmt("samples/{s}/sample.zon", .{entry.name});
        b.dependOnFileContents(b.path(manifest_path));
        const source = dir.readFileAllocOptions(io, manifest_path["samples/".len..], b.allocator, .limited(64 * 1024), .of(u8), 0) catch |err| switch (err) {
            error.FileNotFound => continue,
            else => std.debug.panic("cannot read {s}: {t}", .{ manifest_path, err }),
        };
        var diag: std.zon.parse.Diagnostics = undefined;
        const manifest = std.zon.parse.fromSlice(struct { tests: []const []const u8 = &.{}, imports: []const []const u8 = &.{}, programs: []const []const u8 = &.{} }, .{
            .gpa = b.allocator,
            .arena = b.allocator,
            .source = source,
            .diagnostics = &diag,
            .ignore_unknown_fields = true,
        }) catch std.debug.panic("invalid {s}: {f}", .{ manifest_path, diag.fmt(manifest_path) });
        found.append(b.allocator, .{ .name = b.dupe(entry.name), .tests = manifest.tests, .imports = manifest.imports, .programs = manifest.programs }) catch @panic("OOM");
    }
    std.mem.sort(Sample, found.items, {}, struct {
        fn lessThan(_: void, a: Sample, c: Sample) bool {
            return std.mem.lessThan(u8, a.name, c.name);
        }
    }.lessThan);
    return found.items;
}

const Core = struct { lib: std.Build.LazyPath, header: std.Build.LazyPath };

/// Picks the core library and the header that matches it: -Dcore-lib (header from the
/// submodule) or -Dcore-bundle, then cargo in a checkout with the submodule, then the bundle
/// that build.zig.zon pins for this target at CORE_VERSION. Returns null while that bundle
/// is still to be fetched.
fn selectCore(b: *std.Build, target: std.Build.ResolvedTarget, opts: struct {
    core_lib: ?[]const u8,
    core_bundle: ?[]const u8,
    protoc: ?[]const u8,
    have_submodule: bool,
}) ?Core {
    if (opts.core_lib != null and opts.core_bundle != null) {
        std.process.fatal("-Dcore-lib and -Dcore-bundle both name a core library; pass only one", .{});
    }
    if (opts.core_lib) |lib| {
        if (!opts.have_submodule) std.process.fatal(
            "-Dcore-lib needs the C header from the deps/sdk-core submodule, which is missing ({s}); pass -Dcore-bundle=<dir> with a bundle that has lib/ and include/ instead",
            .{core_header},
        );
        return .{ .lib = .{ .cwd_relative = lib }, .header = b.path(core_header) };
    }
    if (opts.core_bundle) |dir| {
        const lib = b.pathJoin(&.{ dir, "lib", core_lib_name });
        const header = b.pathJoin(&.{ dir, "include", core_header_name });
        for ([_][]const u8{ lib, header }) |p| {
            std.Io.Dir.cwd().access(b.graph.io, p, .{}) catch |err| std.process.fatal(
                "-Dcore-bundle={s}: cannot open {s} ({t}); a core bundle has lib/{s} and include/{s}",
                .{ dir, p, err, core_lib_name, core_header_name },
            );
        }
        return .{ .lib = .{ .cwd_relative = lib }, .header = .{ .cwd_relative = header } };
    }
    if (opts.have_submodule) {
        return .{ .lib = buildCoreLib(b, target, opts.protoc), .header = b.path(core_header) };
    }

    b.dependOnFileContents(b.path("CORE_VERSION"));
    const version_file = b.root.root_dir.handle.readFileAlloc(b.graph.io, b.pathJoin(&.{ b.root.sub_path, "CORE_VERSION" }), b.allocator, .limited(256)) catch |err|
        std.process.fatal("cannot read CORE_VERSION, which names the sdk-core release to link: {t}", .{err});
    const version = std.mem.trim(u8, version_file, " \t\r\n");
    const target_name = bundleTargetName(b, target.result);
    const dep_name = b.fmt("core-{s}-{s}", .{ version, target_name });
    // lazyDependency panics on a name build.zig.zon doesn't declare, so look first.
    for (b.available_deps) |dep| {
        if (!std.mem.eql(u8, dep[0], dep_name)) continue;
        const bundle = b.lazyDependency(dep_name, .{}) orelse return null;
        return .{
            .lib = bundle.path("lib/" ++ core_lib_name),
            .header = bundle.path("include/" ++ core_header_name),
        };
    }
    std.process.fatal(
        "no prebuilt core for {s} at sdk-core {s}: build.zig.zon has no dependency {s}. " ++
            "Build from a checkout with the deps/sdk-core submodule (cargo builds the core), " ++
            "or pass -Dcore-bundle=<dir> (an extracted bundle from scripts/package-core-bundle.sh)",
        .{ target_name, version, dep_name },
    );
}

/// The Zig target a core bundle is built for, as its name spells it: aarch64-macos,
/// x86_64-linux-gnu, x86_64-linux-musl.
fn bundleTargetName(b: *std.Build, t: std.Target) []const u8 {
    return switch (t.os.tag) {
        .linux => b.fmt("{t}-linux-{t}", .{ t.cpu.arch, t.abi }),
        else => b.fmt("{t}-{t}", .{ t.cpu.arch, t.os.tag }),
    };
}

/// Whether `sub_path` exists in this package, recorded so configuration runs again when that
/// changes. The configure cache fails on a dependency that doesn't exist, so a missing path
/// depends on its nearest existing parent directory instead.
fn pathExists(b: *std.Build, sub_path: []const u8) bool {
    var p = sub_path;
    while (true) {
        const stat = b.root.root_dir.handle.statFile(b.graph.io, b.pathJoin(&.{ b.root.sub_path, p }), .{}) catch {
            p = std.fs.path.dirname(p) orelse "";
            if (p.len == 0) {
                b.dependOnDirectoryMetadata(b.path(""));
                return false;
            }
            continue;
        };
        if (stat.kind == .directory) b.dependOnDirectoryMetadata(b.path(p)) else b.dependOnFileMetadata(b.path(p));
        return p.len == sub_path.len;
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
    // Whenever rustc compiles the bridge crate it prints the system libraries linkCoreLib must
    // link; CI records them per target. Passing it every time keeps cargo's fingerprint stable.
    cargo.addArgs(&.{ "--", "--print", "native-static-libs" });
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
            // `cargo rustc ... -- --print native-static-libs` (sdk-core c-bridge, Rust 1.95,
            // aarch64; CI's core step records each Linux target in its job summary):
            //   gnu:  -lgcc_s -lutil -lrt -lpthread -lm -ldl -lc
            //   musl: -lunwind -lc
            // gcc_s and unwind supply Rust's _Unwind_* functions. link_libcpp makes Zig build
            // and link LLVM's libunwind, which provides them without a system library.
            m.link_libcpp = true;
            if (target.result.abi.isGnu()) {
                for ([_][]const u8{ "util", "rt", "pthread", "m", "dl" }) |name| {
                    m.linkSystemLibrary(name, .{});
                }
            }
        },
        .windows => {
            for ([_][]const u8{ "ws2_32", "userenv", "ntdll", "bcrypt", "advapi32", "kernel32", "secur32", "crypt32", "ncrypt" }) |name| {
                m.linkSystemLibrary(name, .{});
            }
        },
        else => {},
    }
}

/// The source of samples/dispatch.zig: a program that runs the worker or starter of any sample
/// that imports no other sample, chosen by SAMPLE_PROGRAM ("<sample>:worker").
fn dispatchSource(b: *std.Build, samples: []const Sample) []const u8 {
    var out: std.Io.Writer.Allocating = .init(b.allocator);
    const w = &out.writer;
    w.writeAll(
        \\//! Generated by `zig build samples-index`; don't edit. Runs the sample program named in
        \\//! SAMPLE_PROGRAM ("<sample>:worker" or "<sample>:starter"), so samples-test compiles one
        \\//! program instead of two per sample.
        \\
        \\const std = @import("std");
        \\
        \\pub fn main(init: std.process.Init) !void {
        \\    const name = init.environ_map.get("SAMPLE_PROGRAM") orelse return error.SampleProgramNotSet;
        \\
    ) catch @panic("OOM");
    for (samples) |sample| {
        if (sample.imports.len > 0) continue;
        for ([_][]const u8{ "worker", "starter" }) |program| {
            w.print("    if (std.mem.eql(u8, name, \"{s}:{s}\")) return @import(\"{s}/{s}.zig\").main(init);\n", .{ sample.name, program, sample.name, program }) catch @panic("OOM");
        }
    }
    w.writeAll(
        \\    std.log.err("unknown SAMPLE_PROGRAM {s}", .{name});
        \\    return error.UnknownSampleProgram;
        \\}
        \\
    ) catch @panic("OOM");
    return out.written();
}
