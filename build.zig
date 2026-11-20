const std = @import("std");
const builtin = @import("builtin");
const protobuf = @import("protobuf");
const protos = @import("deps/protos.zig");
/// The package manifest: its `.version` is the SDK's version, which `temporal.version` reads.
const manifest = @import("build.zig.zon");
/// The fuzz corpus size limits, which src/fuzz.zig shares.
const fuzz_limits = @import("src/fuzz/limits.zig");

const core_header_name = "temporal-sdk-core-c-bridge.h";
const core_header = "deps/sdk-core/crates/sdk-core-c-bridge/include/" ++ core_header_name;
const core_lib_name = "libtemporalio_sdk_core_c_bridge.a";
/// The time-skipping test server release this repository's tests pass with.
const verified_test_server_version = "v1.40.0";

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
    const hang_report = b.option(u32, "test-hang-report", "Print the name, elapsed time and every thread's stack of a test still running after this many seconds, without ending it (default: 300; 0 turns the report off)") orelse 300;
    // A checkout tests against the test server release its tests were last verified with: a
    // new release reaches every machine without a cached copy at once, and one has broken
    // updates (sdk-java 1.41.0's never delivers a workflow task that carries one). A project
    // that depends on the SDK gets sdk-core's default unless it passes the option.
    const test_server_version = b.option(
        []const u8,
        "test-server-version",
        "Time-skipping test server release testing.TestServer downloads by default: \"default\" (sdk-core's choice) or a tag such as \"v1.40.0\" (default in a checkout: " ++ verified_test_server_version ++ ")",
    ) orelse if (b.isRoot()) verified_test_server_version else "default";
    const build_options = b.addOptions();
    build_options.addOption([]const u8, "test_server_version", test_server_version);
    build_options.addOption([]const u8, "version", manifest.version);

    // A fetched package has neither the submodule nor samples/, so the steps that need them
    // exist only in a checkout.
    const have_submodule = pathExists(b, core_header);
    const have_samples = pathExists(b, "samples");
    const have_conformance = pathExists(b, "conformance");

    const protobuf_dep = b.dependency("protobuf", .{ .target = target, .optimize = optimize });

    const temporal = b.addModule("temporal", .{
        .root_source_file = b.path("src/temporal.zig"),
        .target = target,
        .optimize = optimize,
        // No `.error_tracing`: the module a program's `main` (or test runner) is in decides
        // it. A dependency that turns it on while the root module doesn't breaks the program
        // in ReleaseSafe (decoding panics), so the SDK leaves it to the user's module.
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
    const core_c = b.addTranslateC(.{
        .root_source_file = core.header,
        .target = target,
        .optimize = optimize,
    }).createModule();
    temporal.addImport("core_c", core_c);
    temporal.addOptions("build_options", build_options);
    linkCoreLib(temporal, target, core.lib);

    // zig build core: build only the Rust core library
    const core_step = b.step("core", "Build the sdk-core C bridge static library");
    core_step.dependOn(&b.addInstallFile(core.lib, "lib/" ++ core_lib_name).step);
    core_step.dependOn(&b.addInstallFile(core.header, "include/" ++ core_header_name).step);

    // zig build test
    // The unit tests are their own root module, so they may turn error tracing on: in
    // ReleaseSafe too, for the failure stack trace tests. Programs using `temporal` choose
    // for themselves.
    const test_module = b.createModule(.{
        .root_source_file = b.path("src/temporal.zig"),
        .target = target,
        .optimize = optimize,
        .error_tracing = testErrorTracing(optimize),
        .link_libc = true,
        .imports = &.{
            .{ .name = "protobuf", .module = protobuf_dep.module("protobuf") },
            .{ .name = "core_c", .module = core_c },
        },
    });
    test_module.addOptions("build_options", build_options);
    linkCoreLib(test_module, target, core.lib);
    const unit_tests = b.addTest(.{ .root_module = test_module, .filters = test_filters, .test_runner = testRunner(b) });
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&runTests(b, unit_tests, hang_report).step);

    if (have_samples) addSampleSteps(b, target, optimize, temporal, test_step, test_filters, have_conformance);
    if (pathExists(b, "examples/readme")) addReadmeSteps(b, target, optimize, temporal, test_step, test_filters, hang_report);
    if (pathExists(b, "tools/checks")) addCheckSteps(b, .{
        .target = target,
        .optimize = optimize,
        .temporal = temporal,
        .protobuf = protobuf_dep.module("protobuf"),
        .core_c = core_c,
        .build_options = build_options,
        .core = core,
        .test_step = test_step,
        .test_filters = test_filters,
        .forwarded = .{ .core_lib = core_lib_opt, .core_bundle = core_bundle_opt, .protoc = protoc_opt },
    });
    // The test of the hang-dump script CI wraps samples-test in. It needs bash and ps, so a
    // checkout runs it on hosts other than Windows; it prints a skip where ps or job control is
    // missing.
    if (b.graph.host.result.os.tag != .windows and pathExists(b, "scripts/run-with-hang-dump-test.sh")) {
        const hang_dump = b.addSystemCommand(&.{ "bash", "scripts/run-with-hang-dump-test.sh" });
        hang_dump.setCwd(b.path("."));
        hang_dump.has_side_effects = true;
        test_step.dependOn(&hang_dump.step);
    }
    if (have_conformance) addConformanceSteps(b, target, optimize, temporal, test_step, test_filters, hang_report, .{
        .core_lib = core_lib_opt,
        .core_bundle = core_bundle_opt,
        .protoc = protoc_opt,
    });
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
    "examples/readme/nexus.zig",
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
    hang_report: u32,
) void {
    const examples = b.addTest(.{
        .name = "readme-examples",
        .root_module = b.createModule(.{
            .root_source_file = b.path("examples/readme/root.zig"),
            .target = target,
            .optimize = optimize,
            .error_tracing = testErrorTracing(optimize),
            .imports = &.{.{ .name = "temporal", .module = temporal }},
        }),
        .filters = test_filters,
        .test_runner = testRunner(b),
    });
    test_step.dependOn(&runTests(b, examples, hang_report).step);

    const tool_module = b.createModule(.{
        .root_source_file = b.path("tools/readme_examples.zig"),
        .target = b.graph.host,
        .optimize = .Debug,
    });
    const tool = b.addExecutable(.{ .name = "readme-examples", .root_module = tool_module });
    test_step.dependOn(&b.addRunArtifact(b.addTest(.{ .name = "readme-examples-tool", .root_module = tool_module, .filters = test_filters })).step);

    inline for (.{ .{ "readme-check", "check", "Check that README.md's code blocks equal their regions in examples/readme, and PARITY.md's feature table" }, .{ "readme", "update", "Rewrite README.md's code blocks from their regions in examples/readme" } }) |s| {
        const run = b.addRunArtifact(tool);
        run.addArg(s[1]);
        run.addDirectoryArg(b.path("."));
        run.addArg("README.md");
        run.addArgs(&readme_example_files);
        // The tool reads the files itself, by path, so the run must not be cached.
        run.has_side_effects = true;
        const step = b.step(s[0], s[2]);
        step.dependOn(&run.step);
        if (std.mem.eql(u8, s[1], "check")) {
            test_step.dependOn(&run.step);
            // PARITY.md's feature table links README.md's sections, so the same check covers it.
            // Only readme-check runs it (CI does), as it needs a POSIX shell.
            inline for (.{ "scripts/parity-check-test.sh", "scripts/parity-check.sh" }) |script| {
                const parity = b.addSystemCommand(&.{ "sh", script });
                parity.setCwd(b.path("."));
                parity.has_side_effects = true;
                step.dependOn(&parity.step);
            }
        }
    }
}

const CheckInputs = struct {
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    /// The module users import, with the core linked.
    temporal: *std.Build.Module,
    /// The protobuf module built for target.
    protobuf: *std.Build.Module,
    /// The core header translated for target.
    core_c: *std.Build.Module,
    build_options: *std.Build.Step.Options,
    core: Core,
    test_step: *std.Build.Step,
    test_filters: []const []const u8,
    /// The core options of this build, which a nested build repeats to link the same core.
    forwarded: struct { core_lib: ?[]const u8, core_bundle: ?[]const u8, protoc: ?[]const u8 },
};

/// Correctness commands (see AGENTS.md). A fetched package has no tools/checks.
fn addCheckSteps(b: *std.Build, in: CheckInputs) void {
    // zig build alloc-failures
    {
        const helper = struct {
            fn module(owner: *std.Build, target: std.Build.ResolvedTarget, optimize: std.builtin.OptimizeMode, known: []const u8) *std.Build.Module {
                const m = owner.createModule(.{
                    .root_source_file = owner.path("tools/checks/alloc_failures.zig"),
                    .target = target,
                    .optimize = optimize,
                });
                m.addAnonymousImport("alloc_failures_known", .{ .root_source_file = owner.path(known) });
                return m;
            }
        }.module;

        const module = b.createModule(.{
            .root_source_file = b.path("src/alloc_failure.zig"),
            .target = in.target,
            .optimize = in.optimize,
            .error_tracing = testErrorTracing(in.optimize),
            .link_libc = true,
            .imports = &.{
                .{ .name = "protobuf", .module = in.protobuf },
                .{ .name = "core_c", .module = in.core_c },
                .{ .name = "alloc_failures", .module = helper(b, in.target, in.optimize, "tools/checks/alloc_failures.zon") },
            },
        });
        module.addOptions("build_options", in.build_options);
        linkCoreLib(module, in.target, in.core.lib);
        // The SDK files the cases import bring their own tests, which the SDK's test binary
        // runs, so by default only the cases of src/alloc_failure.zig run here.
        const tests = b.addTest(.{
            .name = "alloc-failures",
            .root_module = module,
            .filters = if (in.test_filters.len > 0) in.test_filters else &.{"alloc_failure.test."},
        });
        b.step("alloc-failures", "Fail every allocation of the SDK's user-path functions once and expect error.OutOfMemory").dependOn(&b.addRunArtifact(tests).step);
        // Other goals' pipelines run `zig build test`, so an API change to a tested function
        // must break the compile there. The cases themselves run only under
        // `zig build alloc-failures`.
        in.test_step.dependOn(&tests.step);

        const fixtures = "tools/checks/fixtures/alloc_failures/";
        for ([_]struct { name: []const u8, root: []const u8, known: []const u8, exit_code: u8, stderr: []const u8 }{
            .{ .name = "wrong error", .root = "wrong_error.zig", .known = "empty.zon", .exit_code = 1, .stderr = "alloc-failures: fixture.wrong_error: fail index 1: returned error.InvalidInput instead of error.OutOfMemory" },
            .{ .name = "clean", .root = "correct.zig", .known = "empty.zon", .exit_code = 0, .stderr = "All 3 tests passed." },
            .{ .name = "known finding", .root = "wrong_error.zig", .known = "known.zon", .exit_code = 0, .stderr = "All 2 tests passed." },
            .{ .name = "stale entry", .root = "correct.zig", .known = "stale.zon", .exit_code = 1, .stderr = "alloc-failures: fixture.arena: listed in the known-findings file for temporal-sdk-zig-example but no longer fails; remove its entry" },
            .{ .name = "unknown case", .root = "correct.zig", .known = "gone.zon", .exit_code = 1, .stderr = "alloc-failures: fixture.gone: listed in the known-findings file for temporal-sdk-zig-example, but no case has that name" },
            .{ .name = "case only in a comment", .root = "commented.zig", .known = "commented.zon", .exit_code = 1, .stderr = "alloc-failures: fixture.commented: listed in the known-findings file for temporal-sdk-zig-example, but no case has that name" },
        }) |case| {
            const fixture = b.addTest(.{
                .name = b.fmt("alloc-failures-fixture-{s}", .{std.mem.sliceTo(case.root, '.')}),
                .root_module = b.createModule(.{
                    .root_source_file = b.path(b.fmt("{s}{s}", .{ fixtures, case.root })),
                    .target = b.graph.host,
                    .optimize = .Debug,
                    .imports = &.{.{ .name = "alloc_failures", .module = helper(b, b.graph.host, .Debug, b.fmt("{s}{s}", .{ fixtures, case.known })) }},
                }),
            });
            // addRunArtifact would turn on test-runner mode and hide the exit code.
            const run = std.Build.Step.Run.create(b, b.fmt("check alloc-failures {s}", .{case.name}));
            run.addArtifactArg(fixture);
            run.has_side_effects = true;
            run.expectExitCode(case.exit_code);
            run.expectStdErrMatch(case.stderr);
            in.test_step.dependOn(&run.step);
        }
    }

    // zig build api
    const api_lister, const api_options = api: {
        const source = "tools/checks/api_list.zig";
        const lister = checkTool(b, "api-list", source);
        lister.root_module.addImport("api_root", checkSdkModule(b, in, b.path("src/temporal.zig")));
        const options = b.addOptions();
        options.addOption([]const u8, "root_name", "temporal");
        options.addOption([]const []const u8, "skip", &.{"proto"});
        lister.root_module.addOptions("api_options", options);
        const run = b.addRunArtifact(lister);
        run.addPassthruArgs();
        b.step("api", "List the public names reachable from the package root").dependOn(&run.step);

        const fixture = checkTool(b, "api-list-fixture", source);
        fixture.root_module.addImport("api_root", b.createModule(.{
            .root_source_file = b.path("tools/checks/fixtures/api/root.zig"),
            .target = b.graph.host,
            .optimize = .Debug,
        }));
        const fixture_options = b.addOptions();
        fixture_options.addOption([]const u8, "root_name", "fixture");
        fixture_options.addOption([]const []const u8, "skip", &.{"generated"});
        fixture.root_module.addOptions("api_options", fixture_options);

        const list = addCheckCase(b, in.test_step, fixture, "api list", 0);
        list.addCheck(.{ .expect_stdout_snapshot = b.path("tools/checks/fixtures/api/list.expected.txt") });
        const bad_arg = addCheckCase(b, in.test_step, fixture, "api unexpected argument", 2);
        bad_arg.addArg("unexpected");
        bad_arg.expectStdErrMatch("api: takes no arguments");
        bad_arg.expectStdOutEqual("");

        // The "check api large root" case exercises four paths to a wide namespace,
        // producing a list about three times the size of the SDK's.
        const large = checkTool(b, "api-list-large-fixture", source);
        large.root_module.addImport("api_root", b.createModule(.{
            .root_source_file = b.path("tools/checks/fixtures/api/large.zig"),
            .target = b.graph.host,
            .optimize = .Debug,
        }));
        const large_options = b.addOptions();
        large_options.addOption([]const u8, "root_name", "large");
        large_options.addOption([]const []const u8, "skip", &.{});
        large.root_module.addOptions("api_options", large_options);
        const large_list = addCheckCase(b, in.test_step, large, "api large root", 0);
        large_list.expectStdOutMatch("large.c.S99.f29\n");

        break :api .{ lister, options };
    };

    // zig build api-diff
    {
        const base = b.option([]const u8, "base", "api-diff: the commit to compare from (required)");
        const head = b.option([]const u8, "head", "api-diff: the commit to compare to (default: the working tree)");
        const step = b.step("api-diff", "List the public names added and removed between -Dbase and -Dhead");
        const tool = checkTool(b, "api-diff", "tools/checks/api_diff.zig");

        if (base) |base_ref| {
            const compare = b.addRunArtifact(tool);
            compare.addArg("compare");
            for ([_][]const u8{ "base", "head" }, [_]?[]const u8{ base_ref, head }) |option, maybe_ref| {
                const lister = if (maybe_ref) |ref| lister: {
                    const export_tree = b.addRunArtifact(tool);
                    export_tree.setName(b.fmt("export -D{s}={s}", .{ option, ref }));
                    export_tree.has_side_effects = true;
                    export_tree.addArg("export");
                    export_tree.addDirectoryArg(b.path("."));
                    export_tree.addArgs(&.{ option, ref });
                    const tree = export_tree.addOutputDirectoryArg2("tree", .{});

                    const lister = checkTool(b, b.fmt("api-list-{s}", .{option}), "tools/checks/api_list.zig");
                    lister.step.name = b.fmt("compile the public names of -D{s}={s}", .{ option, ref });
                    lister.root_module.addImport("api_root", checkSdkModule(b, in, tree.path(b, "src/temporal.zig")));
                    lister.root_module.addOptions("api_options", api_options);
                    break :lister lister;
                } else api_lister;
                const list = b.addRunArtifact(lister);
                compare.addFileArg(list.captureStdOut(.{}));
            }
            step.dependOn(&compare.step);
        } else {
            step.dependOn(&b.addFail("api-diff: pass -Dbase=<ref>, the commit to compare from (-Dhead=<ref> is optional; the default is the working tree)").step);
        }

        const compare = addCheckCase(b, in.test_step, tool, "api-diff compare", 0);
        compare.addArg("compare");
        compare.addFileArg(b.path("tools/checks/fixtures/api_diff/base.txt"));
        compare.addFileArg(b.path("tools/checks/fixtures/api_diff/head.txt"));
        compare.addCheck(.{ .expect_stdout_snapshot = b.path("tools/checks/fixtures/api_diff/compare.expected.txt") });

        const unknown_ref = addCheckCase(b, in.test_step, tool, "api-diff unknown ref", 1);
        unknown_ref.addArg("export");
        unknown_ref.addDirectoryArg(b.path("."));
        unknown_ref.addArgs(&.{ "base", "refs/heads/api-diff-test-no-such-ref" });
        _ = unknown_ref.addOutputDirectoryArg2("tree", .{});
        unknown_ref.expectStdErrMatch("-Dbase=refs/heads/api-diff-test-no-such-ref");
    }

    // zig build dupes
    {
        const min_tokens = b.option(u32, "dupes-min-tokens", "dupes: smallest duplicated run in tokens (default: 100)") orelse 100;
        const tool = checkTool(b, "dupes", "tools/checks/dupes.zig");
        const run = b.addRunArtifact(tool);
        run.addDirectoryArg(b.path("."));
        run.addArgs(&.{ "--min-tokens", b.fmt("{d}", .{min_tokens}) });
        run.addPassthruArgs();
        run.has_side_effects = true;
        b.step("dupes", "Report duplicated code in hand-written Zig").dependOn(&run.step);
        const tests = b.addTest(.{ .name = "dupes-tool", .root_module = tool.root_module, .filters = in.test_filters });
        in.test_step.dependOn(&b.addRunArtifact(tests).step);

        const fixtures = b.path("tools/checks/fixtures/dupes");
        const report = addCheckCase(b, in.test_step, tool, "dupes report", 0);
        report.addDirectoryArg(fixtures);
        report.addArgs(&.{ "--min-tokens", "14", "c.zig", "b.zig", "a.zig" });
        report.addCheck(.{ .expect_stdout_snapshot = fixtures.path(b, "report.expected.txt") });
        report.expectStdErrEqual("");

        const missing = addCheckCase(b, in.test_step, tool, "dupes missing path", 1);
        missing.addDirectoryArg(fixtures);
        missing.addArg("dupes-test-no-such-path");
        missing.expectStdErrMatch("dupes: cannot read dupes-test-no-such-path: FileNotFound");
        missing.expectStdOutEqual("");

        const absent = addCheckCase(b, in.test_step, tool, "dupes default roots absent", 0);
        absent.addDirectoryArg(fixtures);
        absent.expectStdOutEqual("0 groups of 100 or more tokens in 0 files\n");
        absent.expectStdErrEqual("");

        const invalid = addCheckCase(b, in.test_step, tool, "dupes invalid minimum", 2);
        invalid.addDirectoryArg(fixtures);
        invalid.addArgs(&.{ "--min-tokens", "0" });
        invalid.expectStdErrMatch("dupes: --min-tokens must be a positive integer");
        invalid.expectStdOutEqual("");
    }

    // zig build fuzz
    {
        const seconds = b.option(u32, "fuzz-seconds", "fuzz: seconds to fuzz each target (default: 30)") orelse 30;
        const selected = b.option([]const u8, "fuzz-target", "Internal option set by the fuzz step on its nested build: fuzz only this target (with --fuzz)");
        const step = b.step("fuzz", "Fuzz each target for a time box and fail on a crash");
        const known = "tools/checks/fuzz.zon";
        const targets = findFuzzTargets(b);

        var source: std.Io.Writer.Allocating = .init(b.allocator);
        for (targets) |target| {
            source.writer.print("pub const @\"{s}\" = [_][]const u8{{\n", .{target.name}) catch @panic("OOM");
            for (target.corpus) |file| {
                source.writer.print("    @embedFile(\"{s}/{s}\"),\n", .{ target.name, file }) catch @panic("OOM");
            }
            source.writer.writeAll("};\n") catch @panic("OOM");
        }
        const corpus = b.createModule(.{ .root_source_file = b.addWriteFiles().add("fuzz_corpus.zig", source.written()) });
        for (targets) |target| for (target.corpus) |file| {
            const path = b.fmt("{s}/{s}", .{ target.name, file });
            corpus.addAnonymousImport(path, .{ .root_source_file = b.path(b.fmt("src/fuzz/{s}", .{path})) });
        };

        // The fuzz-targets module, and the same module over a fixture that lists one
        // finding, so the guard idiom `return fuzz.known(target, bead)` must compile.
        var modules: [2]*std.Build.Module = undefined;
        for (&modules, [_][]const u8{ known, "tools/checks/fixtures/fuzz/guard.zon" }) |*module, known_path| {
            module.* = b.createModule(.{
                .root_source_file = b.path("src/fuzz.zig"),
                .target = in.target,
                .optimize = in.optimize,
                .error_tracing = testErrorTracing(in.optimize),
                .link_libc = true,
                .imports = &.{
                    .{ .name = "protobuf", .module = in.protobuf },
                    .{ .name = "core_c", .module = in.core_c },
                    .{ .name = "fuzz_corpus", .module = corpus },
                },
            });
            module.*.addOptions("build_options", in.build_options);
            module.*.addAnonymousImport("fuzz_known", .{ .root_source_file = b.path(known_path) });
            linkCoreLib(module.*, in.target, in.core.lib);
        }
        const module, const guard_module = modules;
        // The SDK files a target imports bring their tests into this module; the SDK's own
        // test binary runs those (src/nexus/wire.zig alone brings over 700), so by default
        // only the tests of src/fuzz.zig run here.
        const filters: []const []const u8 = if (in.test_filters.len > 0) in.test_filters else &.{"fuzz.test."};
        const tests = b.addTest(.{ .name = "fuzz-targets", .root_module = module, .filters = filters });
        in.test_step.dependOn(&b.addRunArtifact(tests).step);
        const guard_tests = b.addTest(.{ .name = "fuzz-known-guard", .root_module = guard_module, .filters = &.{"known compiles as a guard"} });
        in.test_step.dependOn(&b.addRunArtifact(guard_tests).step);

        const tool = checkTool(b, "fuzz", "tools/checks/fuzz.zig");
        if (selected) |name| {
            for (targets) |target| {
                if (std.mem.eql(u8, target.name, name)) break;
            } else std.process.fatal("no fuzz target named {s}", .{name});
            const single = b.addTest(.{
                .name = "fuzz-targets",
                .root_module = module,
                .filters = &.{b.fmt("fuzz {s}", .{name})},
                // Only LLVM emits the coverage sections --fuzz reads. Zig 0.17.1's own x86_64
                // backend, its default for Debug on x86_64 Linux, emits none, and the fuzzer
                // then panics with "missing __start___sancov_cntrs symbol".
                .use_llvm = true,
            });
            step.dependOn(&b.addRunArtifact(single).step);
        } else {
            const run = b.addRunArtifact(tool);
            run.addDirectoryArg(b.path("."));
            run.addArg(known);
            run.addFileArg(.zig_exe);
            run.addDirectoryArg2(b.graph.path(.local_cache, ""), .{ .make_absolute = true });
            run.addDirectoryArg2(b.graph.path(.install_prefix, "fuzz"), .{ .make_absolute = true });
            run.addArg(b.fmt("{d}", .{seconds}));
            for (targets) |target| run.addArg(target.name);
            run.addArgs(&.{ "--", b.fmt("-Doptimize={t}", .{in.optimize}) });
            if (in.forwarded.core_lib) |path| {
                run.addArg(b.fmt("-Dcore-lib={s}", .{path}));
            } else if (in.forwarded.core_bundle == null and in.target.query.isNative() and pathExists(b, core_header)) {
                // Cargo built the core. A nested build would rerun cargo, get a fresh copy
                // of the library and relink the target inside its box, so it links this one.
                // Nested builds fuzz the host, so only a core built for the host is passed.
                run.addFileArg2(in.core.lib, .{ .prefix = "-Dcore-lib=", .make_absolute = true });
            }
            if (in.forwarded.core_bundle) |path| run.addArg(b.fmt("-Dcore-bundle={s}", .{path}));
            if (in.forwarded.protoc) |path| run.addArg(b.fmt("-Dprotoc={s}", .{path}));
            run.stdio = .inherit;
            run.has_side_effects = true;
            step.dependOn(&run.step);
        }

        const tool_tests = b.addTest(.{ .name = "fuzz-tool", .root_module = tool.root_module, .filters = in.test_filters });
        in.test_step.dependOn(&b.addRunArtifact(tool_tests).step);
        const fixtures = b.path("tools/checks/fixtures/fuzz");
        for ([_]struct { name: []const u8, target: []const u8, seconds: []const u8, exit_code: u8 }{
            .{ .name = "fuzz crash", .target = "crashes", .seconds = "5", .exit_code = 1 },
            .{ .name = "fuzz clean", .target = "clean", .seconds = "3", .exit_code = 0 },
        }) |case| {
            // A fresh directory per build: concurrent and successive builds share .zig-cache.
            const dir = b.tmpPath();
            const run = addCheckCase(b, in.test_step, tool, case.name, case.exit_code);
            run.addDirectoryArg(fixtures);
            run.addArg("known.zon");
            run.addFileArg(.zig_exe);
            run.addDirectoryArg2(dir.path(b, "cache"), .{ .make_absolute = true });
            run.addDirectoryArg2(dir.path(b, "crashes"), .{ .make_absolute = true });
            run.addArgs(&.{ case.seconds, case.target });
            removeAfterPass(b, in.test_step, run, dir);
            if (case.exit_code == 1) {
                run.expectStdOutMatch("crashes: crash (");
                run.expectStdOutMatch("input saved to");
                run.expectStdErrMatch("fuzz: crashes: building and replaying the corpus in process group ");
            } else {
                run.expectStdOutEqual("clean: no crash in 3 s\n");
                run.expectStdErrMatch("fuzz: clean: fuzzing for 3 s in process group ");
            }
        }

        const stale = addCheckCase(b, in.test_step, tool, "fuzz stale entry", 1);
        stale.addDirectoryArg(fixtures);
        stale.addArg("stale.zon");
        stale.addFileArg(.zig_exe);
        stale.addArgs(&.{ "stale-cache-not-created", "stale-crashes-not-created", "3", "clean" });
        stale.expectStdOutEqual("stale: gone temporal-sdk-zig-example (no such target; remove the entry)\n");
        stale.expectStdErrEqual("");

        const usage = addCheckCase(b, in.test_step, tool, "fuzz usage", 2);
        usage.addDirectoryArg(fixtures);
        usage.addArg("known.zon");
        usage.addFileArg(.zig_exe);
        usage.addArgs(&.{ "usage-cache-not-created", "usage-crashes-not-created", "3" });
        usage.expectStdErrMatch("fuzz: usage: ");
        usage.expectStdOutEqual("");

        // `zig build fuzz` in a terminal: the build runner gives the wrapper CLICOLOR_FORCE
        // (Run.color .auto with inherited stdio), so the nested build colors its crash line.
        // The crash must still be reported as one, with its input saved.
        // A fresh directory per build: concurrent and successive builds share .zig-cache.
        const colored_dir = b.tmpPath();
        const colored = addCheckCase(b, in.test_step, tool, "fuzz crash in color", 1);
        colored.color = .enable;
        colored.addDirectoryArg(fixtures);
        colored.addArg("known.zon");
        colored.addFileArg(.zig_exe);
        colored.addDirectoryArg2(colored_dir.path(b, "cache"), .{ .make_absolute = true });
        colored.addDirectoryArg2(colored_dir.path(b, "crashes"), .{ .make_absolute = true });
        colored.addArgs(&.{ "5", "crashes" });
        removeAfterPass(b, in.test_step, colored, colored_dir);
        colored.expectStdOutMatch("crashes: crash (");
        colored.expectStdOutMatch("input saved to");
        colored.expectStdErrMatch("fuzz: crashes: building and replaying the corpus in process group ");
    }

    // zig build lint
    if (b.isRoot()) if (b.lazyImport(@This(), "zlinter")) |zlinter| {
        var linter = zlinter.builder(b, .{});
        linter.addRule(.{ .builtin = .file_naming }, .{});
        linter.addRule(.{ .builtin = .no_comment_out_code }, .{ .severity = .@"error" });
        linter.addRule(.{ .builtin = .no_hidden_allocations }, .{ .severity = .@"error" });
        linter.addRule(.{ .builtin = .no_literal_only_bool_expression }, .{});
        linter.addRule(.{ .builtin = .no_todo }, .{ .severity = .@"error" });
        linter.addRule(.{ .builtin = .switch_case_ordering }, .{ .else_is_last = .@"error" });
        const include_dirs = [_]std.Build.LazyPath{
            b.path("src"),
            b.path("tools"),
            b.path("examples"),
            b.path("samples"),
            b.path("conformance"),
        };
        linter.addPaths(.{
            .include_dirs = if (pathExists(b, "conformance")) &include_dirs else include_dirs[0 .. include_dirs.len - 1],
            .exclude_dirs = &.{ b.path("src/proto"), b.path("tools/checks/fixtures") },
            .exclude_files = &.{ b.path("src/proto.zig"), b.path("samples/dispatch.zig"), b.path("tools/test_runner/main.zig") },
        });
        const scan = linter.build().cast(std.Build.Step.Run).?;
        // zlinter resolves the build configuration relative to its working directory.
        scan.setCwd(b.path("."));
        const lint = b.step("lint", "Lint hand-written Zig with zlinter");
        lint.dependOn(&scan.step);

        const fixture = b.addRunArtifact(scan.producer.?);
        fixture.setCwd(b.path("."));
        fixture.setName("check zlinter rejects a literal-only condition");
        fixture.addArg("--zig_exe");
        fixture.addFileArg(.zig_exe);
        fixture.addArg("--zig_lib_directory");
        fixture.addDirectoryArg(.zig_lib);
        fixture.addArg("--include");
        fixture.addFileArg(b.path("tools/checks/fixtures/lint/bad.zig"));
        fixture.expectExitCode(2);
        fixture.expectStdOutMatch("no_literal_only_bool_expression");
        lint.dependOn(&fixture.step);
    };

    // zig build test-allocators
    {
        const tool = checkTool(b, "test-allocators", "tools/checks/test_allocators.zig");
        const run = b.addRunArtifact(tool);
        run.addDirectoryArg(b.path("."));
        run.addArg("tools/checks/test_allocators.zon");
        run.has_side_effects = true;
        run.setName("check test-allocators tree");
        b.step("test-allocators", "Check that tests allocate through std.testing.allocator").dependOn(&run.step);
        in.test_step.dependOn(&run.step);

        const fixtures = b.path("tools/checks/fixtures/test_allocators");
        const page = addCheckCase(b, in.test_step, tool, "test-allocators page allocator", 1);
        page.addDirectoryArg(fixtures);
        page.addArgs(&.{ "empty.zon", "./page.zig", "page.zig" });
        page.addCheck(.{ .expect_stdout_snapshot = fixtures.path(b, "page.expected.txt") });
        page.expectStdErrEqual("");

        const stale = addCheckCase(b, in.test_step, tool, "test-allocators stale entry", 1);
        stale.addDirectoryArg(fixtures);
        stale.addArgs(&.{ "stale.zon", "page.zig" });
        stale.addCheck(.{ .expect_stdout_snapshot = fixtures.path(b, "stale.expected.txt") });
        stale.expectStdErrEqual("");

        const clean = addCheckCase(b, in.test_step, tool, "test-allocators clean file", 0);
        clean.addDirectoryArg(fixtures);
        clean.addArgs(&.{ "empty.zon", "clean.zig" });
        clean.expectStdOutEqual("");
        clean.expectStdErrEqual("");
    }

    // hang report (tools/test_runner)
    if (testRunner(b)) |runner| {
        const fixtures = "tools/checks/fixtures/hang_report/";
        const Fixture = struct {
            fn build(owner: *std.Build, name: []const u8, root: []const u8, test_runner: std.Build.Step.Compile.TestRunner) *std.Build.Step.Compile {
                return owner.addTest(.{
                    .name = owner.fmt("hang-report-fixture-{s}", .{name}),
                    .root_module = owner.createModule(.{
                        .root_source_file = owner.path(owner.fmt("{s}{s}", .{ fixtures, root })),
                        .target = owner.graph.host,
                        .optimize = .Debug,
                    }),
                    .test_runner = test_runner,
                });
            }
        };
        const hang = Fixture.build(b, "hang", "hang.zig", runner);
        const slow = Fixture.build(b, "slow", "slow.zig", runner);

        const tool = checkTool(b, "hang-report-check", "tools/checks/hang_report.zig");
        const names = addCheckCase(b, in.test_step, tool, "hang report names a waiting test", 0);
        // "in <function> (" is a frame's own line: the bare name also appears in its caller's source line.
        names.addArgs(&.{ "60", "still running after", "waits forever on an event nothing sets", "in waitsForever (", "in helperWaitsForever (", "--" });
        names.addArtifactArg(hang);
        names.addArg("--hang-report=2");

        // A hung test may be stuck holding std's stderr lock; the report must not wait for it.
        const stderr_held = Fixture.build(b, "stderr-held", "stderr_held.zig", runner);
        const held = addCheckCase(b, in.test_step, tool, "hang report names a test while stderr is held", 0);
        held.addArgs(&.{ "60", "still running after", "waits while another thread holds stderr", "in holdsStderrForever (", "--" });
        held.addArtifactArg(stderr_held);
        held.addArg("--hang-report=2");

        // A test may point fd 2 at /dev/null, as EphemeralServer's OutputRedirect does while a
        // dev server starts; the report must still reach the stderr the runner started with.
        const redirected = Fixture.build(b, "stderr-redirected", "stderr_redirected.zig", runner);
        const redirect = addCheckCase(b, in.test_step, tool, "hang report names a test whose stderr is redirected", 0);
        redirect.addArgs(&.{ "60", "still running after", "waits forever with stderr pointed at /dev/null", "in waitsWithStderrRedirected (", "end of the report for", "--" });
        redirect.addArtifactArg(redirected);
        redirect.addArg("--hang-report=2");

        // addRunArtifact would turn on test-runner mode and hide the output. The report's first
        // line is matched without its elapsed seconds, which a stall of the whole process makes
        // larger than the limit; each fixture passes only once the report has frozen its thread
        // and let it go, which proves the report came while the test ran. The test may pass and
        // the process end before the report has printed the stacks, so only its first line is
        // matched.
        const RunsToPass = struct {
            fn add(owner: *std.Build, test_step: *std.Build.Step, name: []const u8, fixture: *std.Build.Step.Compile, test_name: []const u8) void {
                const run = std.Build.Step.Run.create(owner, owner.fmt("check hang report {s}", .{name}));
                run.addArtifactArg(fixture);
                run.addArg("--hang-report=1");
                run.has_side_effects = true;
                run.expectExitCode(0);
                run.expectStdErrMatch(owner.fmt("test-hang-report: '{s}' is still running after ", .{test_name}));
                run.expectStdErrMatch("All 1 tests passed.");
                test_step.dependOn(&run.step);
            }
        };
        const outlasts = Fixture.build(b, "outlasts", "outlasts.zig", runner);
        RunsToPass.add(b, in.test_step, "slow test passes", outlasts, "outlasts.test.passes once the report has let it go");

        // With the report off, the output is Zig's runner's, byte for byte.
        const off = std.Build.Step.Run.create(b, "check hang report off");
        off.addArtifactArg(slow);
        off.addArg("--hang-report=0");
        off.has_side_effects = true;
        off.expectExitCode(0);
        off.expectStdErrEqual("1/1 slow.test.finishes after the limit...OK\nAll 1 tests passed.\n");
        in.test_step.dependOn(&off.step);

        in.test_step.dependOn(&b.addRunArtifact(b.addTest(.{ .name = "hang-report-check-tool", .root_module = tool.root_module, .filters = in.test_filters })).step);

        // A test frozen while it allocates through std.testing.allocator holds std.debug's
        // unwinder lock; the report must not wait on it forever. The fixture bounds itself.
        const busy = Fixture.build(b, "busy", "busy.zig", runner);
        RunsToPass.add(b, in.test_step, "allocating test passes", busy, "busy.test.allocates until the report has let it go");

        // Threads go on and reuse their stacks the moment the report lets them go, while another
        // holds std.debug's symbol lock; the report must use only what it read while each was
        // frozen, not wait on the lock without end, reach its end line and not crash.
        const moved_on = Fixture.build(b, "moved-on", "moved_on.zig", runner);
        const moved = addCheckCase(b, in.test_step, tool, "hang report thread that goes on before its unwind passes", 0);
        moved.addArgs(&.{
            "60",
            "'moved_on.test.goes on after the report lets it go while another thread holds the symbol lock' is still running after ",
            "moved_on: the test's thread went on after the report let it go",
            "moved_on: the thread holding std.debug's symbol lock went on after the report let it go",
            "another thread holds std.debug's symbol lock",
            "end of the report for 'moved_on.test.goes on after the report lets it go while another thread holds the symbol lock'",
            "--",
        });
        moved.addArtifactArg(moved_on);
        moved.addArg("--hang-report=1");

        // A thread may take std.debug's symbol lock after the report found it free and keep it,
        // as moved_on.zig's holder does when its setup is slow by about a second; the report
        // must still reach its end line.
        const symbol_lock_late = Fixture.build(b, "symbol-lock-late", "symbol_lock_late.zig", runner);
        const late = addCheckCase(b, in.test_step, tool, "hang report finishes when the symbol lock is taken during its lookup", 0);
        late.addArgs(&.{
            "60",
            "'symbol_lock_late.test.waits while another thread takes the symbol lock during the report' is still running after ",
            "end of the report for 'symbol_lock_late.test.waits while another thread takes the symbol lock during the report'",
            "--",
        });
        late.addArtifactArg(symbol_lock_late);
        late.addArg("--hang-report=1");

        // A thread may keep taking and releasing std.debug's symbol lock, so that no single wait
        // of the report's symbol helper lasts long; the report must still give up on symbols once
        // the helper's waits add up to 1 s and reach its end line. The churner also shows that
        // the waiting helper is named hang-report-sym, which on macOS only the thread itself can do.
        const symbol_lock_churn = Fixture.build(b, "symbol-lock-churn", "symbol_lock_churn.zig", runner);
        const churn = addCheckCase(b, in.test_step, tool, "hang report finishes while another thread keeps taking and releasing the symbol lock", 0);
        churn.addArgs(&.{
            "60",
            "'symbol_lock_churn.test.waits while another thread keeps taking and releasing the symbol lock' is still running after ",
            "symbol_lock_churn: a thread named hang-report-sym waits while this thread holds std.debug's symbol lock",
            "another thread holds std.debug's symbol lock; the stacks that follow are bare addresses",
            "end of the report for 'symbol_lock_churn.test.waits while another thread keeps taking and releasing the symbol lock'",
            "--",
        });
        churn.addArtifactArg(symbol_lock_churn);
        churn.addArg("--hang-report=1");
    }

    // zig build tsan
    {
        // Renders a known-findings file and adds to `run` the arguments that run `artifact`
        // with it through `exec`, which also makes a report end the process with exit 66
        // (see tools/checks/tsan_suppressions.zig), and symbolize with `symbolizer` when
        // given. `run` runs the suppressions tool.
        const Known = struct {
            fn addArgs(b2: *std.Build, tool: *std.Build.Step.Compile, zon: []const u8, run: *std.Build.Step.Run, artifact: *std.Build.Step.Compile, symbolizer: ?*std.Build.Step.Compile) void {
                const render = b2.addRunArtifact(tool);
                render.setName(b2.fmt("render {s}", .{zon}));
                render.addArg("render");
                render.addFileArg(b2.path(zon));
                run.addArg("exec");
                if (symbolizer) |s| {
                    run.addArg("--symbolizer");
                    run.addArtifactArg(s);
                }
                run.addFileArg(render.captureStdOut(.{ .basename = "tsan.supp" }));
                run.addArtifactArg(artifact);
            }
        };
        const suppressions_tool = checkTool(b, "tsan-suppressions", "tools/checks/tsan_suppressions.zig");
        const suppressions_tests = b.addTest(.{ .name = "tsan-suppressions", .root_module = suppressions_tool.root_module, .filters = in.test_filters });
        in.test_step.dependOn(&b.addRunArtifact(suppressions_tests).step);

        // Mirrors the unit test module of `build()`, with Thread Sanitizer. The core library
        // is not instrumented.
        const module = b.createModule(.{
            .root_source_file = b.path("src/temporal.zig"),
            .target = in.target,
            .optimize = in.optimize,
            .error_tracing = testErrorTracing(in.optimize),
            .link_libc = true,
            .sanitize_thread = true,
            .imports = &.{
                .{ .name = "protobuf", .module = in.protobuf },
                .{ .name = "core_c", .module = in.core_c },
            },
        });
        module.addOptions("build_options", in.build_options);
        linkCoreLib(module, in.target, in.core.lib);
        const tests = b.addTest(.{ .name = "tsan-unit-tests", .root_module = module, .filters = in.test_filters });
        // When the sanitizer reported anything, the runtime exits 66 at exit (see the
        // "tsan race" case), which fails the run even when every test passed. A report waits
        // for the symbolizer, which only `--test-timeout` bounds here: the tests need their
        // reports' function names, which the suppressions match.
        const run = b.addRunArtifact(suppressions_tool);
        Known.addArgs(b, suppressions_tool, "tools/checks/tsan.zon", run, tests, null);
        run.setName("run test tsan-unit-tests");
        run.enableTestRunnerMode();
        b.step("tsan", "Run the unit tests under Thread Sanitizer").dependOn(&run.step);

        // To print a report, the runtime symbolizes its stacks, which on macOS starts
        // `atos -p <pid>` and waits for its answer with no limit, so a wedged atos (as on
        // 2026-11-19) hung `zig build test` for good, and while the host's atos stays wedged
        // a case that uses it cannot pass. So each case below runs its program through
        // time-limit, which fails the case, naming it, when the program has not ended within
        // the given seconds; the cases that need no function names turn symbolization off;
        // and on macOS the one that needs them uses the build's own symbolizer instead of
        // the host's atos: tools/checks/symbolizer.zig, built under the file name
        // llvm-symbolizer, the name that makes the runtime speak llvm-symbolizer's protocol
        // to it, which names the binary's file and an address in it, not the live process.
        const limit_tool = checkTool(b, "time-limit", "tools/checks/time_limit.zig");
        in.test_step.dependOn(&b.addRunArtifact(b.addTest(.{ .name = "time-limit", .root_module = limit_tool.root_module, .filters = in.test_filters })).step);
        const symbolizer_tool = checkTool(b, "llvm-symbolizer", "tools/checks/symbolizer.zig");
        in.test_step.dependOn(&b.addRunArtifact(b.addTest(.{ .name = "symbolizer", .root_module = symbolizer_tool.root_module, .filters = in.test_filters })).step);
        // Linux keeps the runtime's own choice.
        const own_symbolizer: ?*std.Build.Step.Compile = if (b.graph.host.result.os.tag.isDarwin()) symbolizer_tool else null;
        const Limited = struct {
            /// Ample for a fixture and the symbolizer, even on a loaded machine.
            const seconds = 120;

            fn case(b2: *std.Build, test_step: *std.Build.Step, tool: *std.Build.Step.Compile, name: []const u8, limit: u32, exit_code: u8) *std.Build.Step.Run {
                const r = addCheckCase(b2, test_step, tool, name, exit_code);
                r.setCwd(b2.path("."));
                r.addArgs(&.{ b2.fmt("{d}", .{limit}), r.step.name });
                return r;
            }
        };

        // Libc, so that threads come from pthreads, which the sanitizer intercepts.
        const race_tool = checkTool(b, "tsan-race-fixture", "tools/checks/fixtures/tsan/race.zig");
        race_tool.root_module.sanitize_thread = true;
        race_tool.root_module.link_libc = true;
        const mutex_tool = checkTool(b, "tsan-mutex-fixture", "tools/checks/fixtures/tsan/mutex.zig");
        mutex_tool.root_module.sanitize_thread = true;
        mutex_tool.root_module.link_libc = true;

        // On macOS the cases run a second time with a symbolizer that never answers, to check
        // that the race and mutex cases do not wait for it and that the known finding case,
        // which does, fails within its limit.
        // The path is relative to the build root, where these cases run.
        const stuck_symbolizer: ?[]const u8 = if (b.graph.host.result.os.tag.isDarwin()) "tools/checks/fixtures/tsan/stuck-symbolizer/atos" else null;
        for ([_]?[]const u8{ null, stuck_symbolizer }, 0..) |symbolizer, i| {
            if (i > 0 and symbolizer == null) continue;
            const suffix = if (symbolizer != null) " with a stuck symbolizer" else "";
            const extra: []const u8 = if (symbolizer) |path| b.fmt(" external_symbolizer_path=\"{s}\"", .{path}) else "";

            // The check needs only the WARNING line, so the report is printed with addresses.
            // On Apple platforms `abort_on_error` defaults to true, which would end the
            // process with SIGABRT and leave a crash report.
            const race = Limited.case(b, in.test_step, limit_tool, b.fmt("tsan race{s}", .{suffix}), Limited.seconds, 66);
            race.addArtifactArg(race_tool);
            race.setEnvironmentVariable("TSAN_OPTIONS", b.fmt("abort_on_error=0 symbolize=0{s}", .{extra}));
            race.expectStdErrMatch("WARNING: ThreadSanitizer: data race");

            // The same program with its racing function listed: the suppression must take,
            // and `exec` keeps the caller's options (the runtime prints matches only when
            // asked). Suppressions match function names, so this case symbolizes, on macOS
            // with the build's symbolizer, so that it never starts the host's atos; given a
            // stuck symbolizer instead, it ends with time-limit's message.
            const known_name = b.fmt("tsan known finding{s}", .{suffix});
            const known = if (symbolizer == null)
                Limited.case(b, in.test_step, limit_tool, known_name, Limited.seconds, 0)
            else
                Limited.case(b, in.test_step, limit_tool, known_name, 5, 124);
            known.addArtifactArg(suppressions_tool);
            Known.addArgs(b, suppressions_tool, "tools/checks/fixtures/tsan/known.zon", known, race_tool, if (symbolizer == null) own_symbolizer else null);
            known.setEnvironmentVariable("TSAN_OPTIONS", b.fmt("print_suppressions=1{s}", .{extra}));
            if (symbolizer == null)
                known.expectStdErrMatch("ThreadSanitizer: Matched")
            else
                known.expectStdErrMatch(b.fmt("time-limit: check {s}: ", .{known_name}));

            // Reports nothing, so it never starts the symbolizer.
            const mutex = Limited.case(b, in.test_step, limit_tool, b.fmt("tsan mutex{s}", .{suffix}), Limited.seconds, 0);
            mutex.addArtifactArg(mutex_tool);
            if (symbolizer != null) mutex.setEnvironmentVariable("TSAN_OPTIONS", extra[1..]);
            mutex.expectStdErrEqual("");
        }
    }
}

/// Builds a check tool for the machine running the build.
fn checkTool(b: *std.Build, name: []const u8, source: []const u8) *std.Build.Step.Compile {
    return b.addExecutable(.{ .name = name, .root_module = b.createModule(.{
        .root_source_file = b.path(source),
        .target = b.graph.host,
        .optimize = .Debug,
    }) });
}

/// The project's test runner: Zig's, plus the hang report (tools/test_runner/main.zig).
/// Null in a fetched package, which has no tools/, so its tests use Zig's runner.
fn testRunner(b: *std.Build) ?std.Build.Step.Compile.TestRunner {
    if (!pathExists(b, "tools/test_runner")) return null;
    return .{ .path = b.path("tools/test_runner/main.zig"), .mode = .server };
}

/// addRunArtifact, plus the -Dtest-hang-report limit when `tests` uses the project's runner.
fn runTests(b: *std.Build, tests: *std.Build.Step.Compile, hang_report: u32) *std.Build.Step.Run {
    const run = b.addRunArtifact(tests);
    if (tests.test_runner != null) run.addArg(b.fmt("--hang-report={d}", .{hang_report}));
    return run;
}

/// Adds a tool run to test. Tools can read files by path, so their runs are never cached.
fn addCheckCase(b: *std.Build, test_step: *std.Build.Step, tool: *std.Build.Step.Compile, name: []const u8, exit_code: u8) *std.Build.Step.Run {
    const run = b.addRunArtifact(tool);
    run.setName(b.fmt("check {s}", .{name}));
    run.has_side_effects = true;
    run.expectExitCode(exit_code);
    test_step.dependOn(&run.step);
    return run;
}

/// Removes a check case's `b.tmpPath()` directory once the case passes. Zig 0.17.1 deletes
/// temporary directories after a failed build but leaves them after a successful one.
fn removeAfterPass(b: *std.Build, test_step: *std.Build.Step, case: *std.Build.Step.Run, dir: std.Build.LazyPath) void {
    const remove = b.addSystemCommand(&.{ "rm", "-rf" });
    remove.setName(b.fmt("clean up after {s}", .{case.step.name}));
    remove.addDirectoryArg(dir);
    remove.has_side_effects = true;
    remove.step.dependOn(&case.step);
    test_step.dependOn(&remove.step);
}

/// Gives the lister an SDK module and imports for the host. Reflection needs no core library.
fn checkSdkModule(b: *std.Build, in: CheckInputs, root: std.Build.LazyPath) *std.Build.Module {
    const host_protobuf = b.dependency("protobuf", .{ .target = b.graph.host, .optimize = .Debug }).module("protobuf");
    const host_core_c = b.addTranslateC(.{
        .root_source_file = in.core.header,
        .target = b.graph.host,
        .optimize = .Debug,
    }).createModule();
    const module = b.createModule(.{
        .root_source_file = root,
        .target = b.graph.host,
        .optimize = .Debug,
        .link_libc = true,
        .imports = &.{
            .{ .name = "protobuf", .module = host_protobuf },
            .{ .name = "core_c", .module = host_core_c },
        },
    });
    module.addOptions("build_options", in.build_options);
    return module;
}

/// A directory src/fuzz/<name>/ and the corpus files in it, both sorted by name.
const FuzzTarget = struct {
    name: []const u8,
    corpus: []const []const u8,

    fn lessThan(_: void, a: FuzzTarget, c: FuzzTarget) bool {
        return std.mem.lessThan(u8, a.name, c.name);
    }

    fn fileLessThan(_: void, a: []const u8, c: []const u8) bool {
        return std.mem.lessThan(u8, a, c);
    }
};

fn findFuzzTargets(b: *std.Build) []const FuzzTarget {
    const root = "src/fuzz";
    const target_chars = "abcdefghijklmnopqrstuvwxyz0123456789_";
    const file_chars = target_chars ++ "ABCDEFGHIJKLMNOPQRSTUVWXYZ.-";
    const io = b.graph.io;
    const dir = b.root.openDir(io, root, .{ .iterate = true }) catch |err|
        std.process.fatal("cannot open {s}: {t}", .{ root, err });
    defer dir.close(io);
    b.dependOnDirectoryContents(b.path(root));

    var targets: std.ArrayList(FuzzTarget) = .empty;
    var total: u64 = 0;
    var dirs = dir.iterate();
    while (dirs.next(io) catch |err| std.process.fatal("cannot scan {s}: {t}", .{ root, err })) |entry| {
        const target_path = b.fmt("{s}/{s}", .{ root, entry.name });
        const kind = if (entry.kind == .unknown)
            (dir.statFile(io, entry.name, .{ .follow_symlinks = false }) catch |err|
                std.process.fatal("cannot stat {s}: {t}", .{ target_path, err })).kind
        else
            entry.kind;
        if (kind != .directory) continue;
        if (std.mem.findNone(u8, entry.name, target_chars) != null)
            std.process.fatal("invalid fuzz target name in {s}: use [a-z0-9_]+", .{target_path});
        const name = b.dupe(entry.name);
        const target_dir = b.root.openDir(io, target_path, .{ .iterate = true }) catch |err|
            std.process.fatal("cannot open {s}: {t}", .{ target_path, err });
        defer target_dir.close(io);
        b.dependOnDirectoryContents(b.path(target_path));

        var corpus: std.ArrayList([]const u8) = .empty;
        var files = target_dir.iterate();
        while (files.next(io) catch |err| std.process.fatal("cannot scan {s}: {t}", .{ target_path, err })) |file| {
            if (file.name[0] == '.' or (file.kind != .file and file.kind != .unknown)) continue;
            const path = b.fmt("{s}/{s}", .{ target_path, file.name });
            const stat = target_dir.statFile(io, file.name, .{ .follow_symlinks = false }) catch |err|
                std.process.fatal("cannot stat {s}: {t}", .{ path, err });
            if (stat.kind != .file) continue;
            if (std.mem.findNone(u8, file.name, file_chars) != null)
                std.process.fatal("invalid fuzz corpus file name in {s}: use [A-Za-z0-9._-]+", .{path});
            if (stat.size > fuzz_limits.max_file_len)
                std.process.fatal("{s} is {d} bytes, exceeding the {d}-byte fuzz corpus file limit (src/fuzz/limits.zig)", .{ path, stat.size, fuzz_limits.max_file_len });
            b.dependOnFileMetadata(b.path(path));
            total += stat.size;
            corpus.append(b.allocator, b.dupe(file.name)) catch @panic("OOM");
        }
        std.mem.sort([]const u8, corpus.items, {}, FuzzTarget.fileLessThan);
        targets.append(b.allocator, .{ .name = name, .corpus = corpus.items }) catch @panic("OOM");
    }
    if (total > fuzz_limits.max_corpus_len)
        std.process.fatal("{s} contains {d} corpus bytes, exceeding the {d}-byte total limit", .{ root, total, fuzz_limits.max_corpus_len });
    std.mem.sort(FuzzTarget, targets.items, {}, FuzzTarget.lessThan);
    if (targets.items.len > 1) for (targets.items[0 .. targets.items.len - 1], targets.items[1..]) |a, c| {
        if (std.mem.startsWith(u8, c.name, a.name))
            std.process.fatal("fuzz target {s}/{s} is a prefix of {s}/{s}", .{ root, a.name, root, c.name });
    };
    return targets.items;
}

fn addSampleSteps(b: *std.Build, target: std.Build.ResolvedTarget, optimize: std.builtin.OptimizeMode, temporal: *std.Build.Module, test_step: *std.Build.Step, test_filters: []const []const u8, have_conformance: bool) void {
    // zig build fmt
    const fmt_paths = [_]std.Build.LazyPath{
        b.path("build.zig"),
        b.path("build.zig.zon"),
        b.path("src"),
        b.path("samples"),
        b.path("tools"),
        b.path("examples"),
        b.path("conformance"),
    };
    const fmt_step = b.step("fmt", "Check formatting");
    fmt_step.dependOn(&b.addFmt(.{
        .paths = if (have_conformance) &fmt_paths else fmt_paths[0 .. fmt_paths.len - 1],
        .check = true,
    }).step);

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
    // The runner's own logic (reading sample.zon, planning namespaces and endpoints).
    test_step.dependOn(&b.addRunArtifact(b.addTest(.{ .name = "samples-runner", .root_module = runner.root_module, .filters = test_filters })).step);
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

const ConformanceForwarded = struct { core_lib: ?[]const u8, core_bundle: ?[]const u8, protoc: ?[]const u8 };

const conformance_histories = "conformance/histories";

fn addConformanceSteps(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    temporal: *std.Build.Module,
    test_step: *std.Build.Step,
    test_filters: []const []const u8,
    hang_report: u32,
    forwarded: ConformanceForwarded,
) void {
    const patterns = b.option([]const []const u8, "feature", "Run conformance features matching this path pattern (repeatable)") orelse &.{};
    const record = b.option(bool, "record-histories", "Store the workflow histories of passing conformance features under conformance/histories") orelse false;
    const with_suite = b.option(bool, "conformance-suite", "Internal option set by the conformance step on its nested build") orelse false;
    const step = b.step("conformance", "Run the Temporal features suite and write conformance/report.json");
    const map_step = b.step("conformance-map", "Check conformance/parity_map.zon against the features suite");
    const check_step = b.step("conformance-check", "Run the features suite and fail when a supported PARITY.md row's feature is not passing");
    if (patterns.len != 0) {
        check_step.dependOn(&b.addFail("zig build conformance-check takes no -Dfeature: it compares the whole suite with PARITY.md (use zig build conformance -Dfeature=... to run part of the suite)").step);
    }
    const report_path = b.graph.path(.install_prefix, "conformance/report.json");

    const harness = b.createModule(.{
        .root_source_file = b.path("conformance/harness/harness.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{.{ .name = "temporal", .module = temporal }},
    });
    const runner = b.createModule(.{
        .root_source_file = b.path("conformance/runner/main.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{ .{ .name = "temporal", .module = temporal }, .{ .name = "harness", .module = harness } },
    });
    runner.addAnonymousImport("PARITY.md", .{ .root_source_file = b.path("PARITY.md") });
    runner.addAnonymousImport("parity_map.zon", .{ .root_source_file = b.path("conformance/parity_map.zon") });
    inline for (.{ .{ "conformance-harness", harness }, .{ "conformance-runner", runner } }) |artifact| {
        test_step.dependOn(&runTests(b, b.addTest(.{
            .name = artifact[0],
            .root_module = artifact[1],
            .filters = test_filters,
            .test_runner = testRunner(b),
        }), hang_report).step);
    }

    var source: std.Io.Writer.Allocating = .init(b.allocator);
    const writer = &source.writer;
    writer.writeAll(
        \\const harness = @import("harness");
        \\pub const Entry = struct { path: []const u8, feature: *const harness.Feature };
        \\pub const all = [_]Entry{
        \\
    ) catch @panic("OOM");
    const registry = b.createModule(.{ .imports = &.{.{ .name = "harness", .module = harness }} });
    for (findConformanceFeatures(b)) |path| {
        const feature = b.createModule(.{
            .root_source_file = b.path(b.fmt("conformance/features/{s}/feature.zig", .{path})),
            .target = target,
            .optimize = optimize,
            .imports = &.{ .{ .name = "temporal", .module = temporal }, .{ .name = "harness", .module = harness } },
        });
        registry.addImport(path, feature);
        writer.print("    .{{ .path = \"{f}\", .feature = &@import(\"{f}\").feature }},\n", .{ std.zig.fmtString(path), std.zig.fmtString(path) }) catch @panic("OOM");
    }
    writer.writeAll("};\n") catch @panic("OOM");
    registry.root_source_file = b.addWriteFiles().add("features.zig", source.written());
    runner.addImport("features", registry);

    const stored_histories = b.createModule(.{ .imports = &.{.{ .name = "harness", .module = harness }} });
    var stored_source: std.Io.Writer.Allocating = .init(b.allocator);
    const stored_writer = &stored_source.writer;
    stored_writer.writeAll(
        \\const harness = @import("harness");
        \\pub const files = [_]harness.stored.File{
        \\
    ) catch @panic("OOM");
    for (findStoredHistories(b), 0..) |path, i| {
        const name = b.fmt("stored-{d}", .{i});
        stored_histories.addAnonymousImport(name, .{ .root_source_file = b.path(b.fmt("{s}/{s}", .{ conformance_histories, path })) });
        stored_writer.print("    .{{ .path = \"{f}\", .bytes = @embedFile(\"{s}\") }},\n", .{ std.zig.fmtString(path), name }) catch @panic("OOM");
    }
    stored_writer.writeAll("};\n") catch @panic("OOM");
    stored_histories.root_source_file = b.addWriteFiles().add("stored_histories.zig", stored_source.written());
    runner.addImport("stored_histories", stored_histories);

    test_step.dependOn(&runTests(b, b.addTest(.{
        .name = "conformance-replay",
        .root_module = b.createModule(.{
            .root_source_file = b.path("conformance/replay/replay.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "temporal", .module = temporal },
                .{ .name = "harness", .module = harness },
                .{ .name = "features", .module = registry },
                .{ .name = "stored_histories", .module = stored_histories },
            },
        }),
        .filters = test_filters,
        .test_runner = testRunner(b),
    }), hang_report).step);

    if (!with_suite) {
        // Only the nested build requests the lazy dependency. Clear the report even if
        // fetching the suite or compiling the runner fails before the parent can clear it.
        const clear = b.addSystemCommand(&.{ "rm", "-f", "--" });
        clear.addFileArg2(report_path, .{ .make_absolute = true });
        clear.has_side_effects = true;
        const nested = nestedConformanceBuild(b, "conformance", optimize, forwarded);
        if (record) nested.addArg("-Drecord-histories=true");
        for (patterns) |pattern| nested.addArg(b.fmt("-Dfeature={s}", .{pattern}));
        nested.step.dependOn(&clear.step);
        step.dependOn(&nested.step);
        map_step.dependOn(&nestedConformanceBuild(b, "conformance-map", optimize, forwarded).step);
        if (patterns.len == 0) {
            const nested_check = nestedConformanceBuild(b, "conformance-check", optimize, forwarded);
            nested_check.step.dependOn(&clear.step);
            check_step.dependOn(&nested_check.step);
        }
        return;
    }

    const suite = b.lazyDependency("temporal_features", .{}) orelse return;
    const executable = b.addExecutable(.{ .name = "conformance-runner", .root_module = runner });
    const map = b.addRunArtifact(executable);
    map.addArgs(&.{ "map", "--suite" });
    map.addDirectoryArg(suite.path(""));
    map.addArg("--not-applicable");
    map.addFileArg(b.path("conformance/not_applicable.zon"));
    map.addArg("--parity-map");
    map.addFileArg(b.path("conformance/parity_map.zon"));
    map.stdio = .inherit;
    map.has_side_effects = true;
    map_step.dependOn(&map.step);

    const suite_url = @import("build.zig.zon").dependencies.temporal_features.url;
    const suite_commit = suite_url[std.mem.lastIndexOfScalar(u8, suite_url, '#').? + 1 ..];
    const work_dir = b.addWriteFiles().getDirectory();
    for ([_]*std.Build.Step{ step, check_step }) |suite_step| {
        const checking = suite_step == check_step;
        if (checking and patterns.len != 0) continue;

        const run = b.addRunArtifact(executable);
        run.addArgs(&.{ "run", "--suite" });
        run.addDirectoryArg(suite.path(""));
        run.addArgs(&.{ "--suite-commit", suite_commit, "--not-applicable" });
        run.addFileArg(b.path("conformance/not_applicable.zon"));
        run.addArg("--report");
        run.addFileArg2(report_path, .{ .make_absolute = true });
        run.addArg("--work-dir");
        run.addDirectoryArg(work_dir);
        run.stdio = .inherit;
        run.has_side_effects = true;
        if (!checking) {
            if (record) {
                const package_root = b.root.root_dir.handle.realPathFileAlloc(b.graph.io, b.root.subPathOrDot(), b.allocator) catch |err|
                    std.process.fatal("cannot resolve the conformance package root: {t}", .{err});
                run.addArgs(&.{ "--histories", b.pathJoin(&.{ package_root, conformance_histories }) });
            }
            for (patterns) |pattern| run.addArgs(&.{ "--feature", pattern });
            suite_step.dependOn(&run.step);
            continue;
        }

        run.addArgs(&.{ "--stdout-to", "stderr" });
        run.step.dependOn(&map.step);
        const check = b.addRunArtifact(executable);
        check.addArgs(&.{ "check", "--report" });
        check.addFileArg2(report_path, .{ .make_absolute = true });
        check.addArg("--parity-map");
        check.addFileArg(b.path("conformance/parity_map.zon"));
        check.addArg("--parity");
        check.addFileArg(b.path("PARITY.md"));
        check.stdio = .inherit;
        check.has_side_effects = true;
        check.step.dependOn(&run.step);
        suite_step.dependOn(&check.step);
    }
}

fn nestedConformanceBuild(b: *std.Build, step_name: []const u8, optimize: std.builtin.OptimizeMode, forwarded: ConformanceForwarded) *std.Build.Step.Run {
    const nested = b.addSystemCommand(&.{ b.graph.zig_exe, "build", step_name, "-Dconformance-suite=true", "--prefix" });
    nested.addFileArg2(b.graph.path(.install_prefix, ""), .{ .make_absolute = true });
    nested.removeEnvironmentVariable("DESTDIR");
    nested.setCwd(b.path(""));
    nested.stdio = .inherit;
    nested.has_side_effects = true;
    nested.addArg(b.fmt("-Doptimize={t}", .{optimize}));
    if (forwarded.core_lib) |path| nested.addArg(b.fmt("-Dcore-lib={s}", .{path}));
    if (forwarded.core_bundle) |path| nested.addArg(b.fmt("-Dcore-bundle={s}", .{path}));
    if (forwarded.protoc) |path| nested.addArg(b.fmt("-Dprotoc={s}", .{path}));
    return nested;
}

fn findStoredHistories(b: *std.Build) []const []const u8 {
    const io = b.graph.io;
    const dir = b.root.openDir(io, conformance_histories, .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound => {
            b.dependOnDirectoryContents(b.path("conformance"));
            return &.{};
        },
        else => std.process.fatal("cannot open {s}: {t}", .{ conformance_histories, err }),
    };
    defer dir.close(io);
    b.dependOnDirectoryContents(b.path(conformance_histories));
    var walker = dir.walkSelectively(b.allocator) catch @panic("OOM");
    defer walker.deinit();
    var paths: std.ArrayList([]const u8) = .empty;
    while (walker.next(io) catch |err| std.process.fatal("cannot scan {s}: {t}", .{ conformance_histories, err })) |entry| {
        if (entry.basename[0] == '.') continue;
        switch (entry.kind) {
            .directory => {
                b.dependOnDirectoryContents(b.path(b.fmt("{s}/{s}", .{ conformance_histories, entry.path })));
                walker.enter(io, entry) catch |err| std.process.fatal("cannot open {s}/{s}: {t}", .{ conformance_histories, entry.path, err });
            },
            .file => paths.append(b.allocator, b.dupe(entry.path)) catch @panic("OOM"),
            else => {},
        }
    }
    std.mem.sort([]const u8, paths.items, {}, struct {
        fn lessThan(_: void, a: []const u8, c: []const u8) bool {
            return std.mem.lessThan(u8, a, c);
        }
    }.lessThan);
    return paths.items;
}

fn findConformanceFeatures(b: *std.Build) []const []const u8 {
    const root = "conformance/features";
    const io = b.graph.io;
    const dir = b.root.openDir(io, root, .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound => {
            b.dependOnDirectoryContents(b.path("conformance"));
            return &.{};
        },
        else => std.process.fatal("cannot open {s}: {t}", .{ root, err }),
    };
    defer dir.close(io);
    b.dependOnDirectoryContents(b.path(root));
    var walker = dir.walk(b.allocator) catch @panic("OOM");
    defer walker.deinit();
    var paths: std.ArrayList([]const u8) = .empty;
    while (walker.next(io) catch |err| std.process.fatal("cannot scan {s}: {t}", .{ root, err })) |entry| {
        if (entry.kind == .directory) {
            b.dependOnDirectoryContents(b.path(b.fmt("{s}/{s}", .{ root, entry.path })));
        } else if (entry.kind == .file and std.mem.eql(u8, entry.basename, "feature.zig")) {
            const path = std.fs.path.dirname(entry.path) orelse std.process.fatal("{s}/feature.zig needs a feature directory", .{root});
            paths.append(b.allocator, b.dupe(path)) catch @panic("OOM");
        }
    }
    std.mem.sort([]const u8, paths.items, {}, struct {
        fn lessThan(_: void, a: []const u8, c: []const u8) bool {
            return std.mem.lessThan(u8, a, c);
        }
    }.lessThan);
    return paths.items;
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
        const sample_manifest = std.zon.parse.fromSlice(struct { tests: []const []const u8 = &.{}, imports: []const []const u8 = &.{}, programs: []const []const u8 = &.{} }, .{
            .gpa = b.allocator,
            .arena = b.allocator,
            .source = source,
            .diagnostics = &diag,
            .ignore_unknown_fields = true,
        }) catch std.debug.panic("invalid {s}: {f}", .{ manifest_path, diag.fmt(manifest_path) });
        found.append(b.allocator, .{ .name = b.dupe(entry.name), .tests = sample_manifest.tests, .imports = sample_manifest.imports, .programs = sample_manifest.programs }) catch @panic("OOM");
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

    const cargo = b.addSystemCommand(&.{ findCargo(b), "rustc" });
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

/// Error return tracing in ReleaseSafe too, for root modules whose tests check failure stack
/// traces; Zig's default (Debug only) otherwise. Only for root modules: see `temporal`.
fn testErrorTracing(optimize: std.builtin.OptimizeMode) ?bool {
    return if (optimize == .safe) true else null;
}

/// `cargo` from PATH, or else from rustup's install directory, $CARGO_HOME/bin (default
/// ~/.cargo/bin): rustup adds it to PATH from the shell's profile, which a non-login shell or
/// a service doesn't read. Plain "cargo" when neither has it, so the build fails naming it.
fn findCargo(b: *std.Build) []const u8 {
    if (b.findProgram(.{ .names = &.{"cargo"} })) |found| return found;
    const env = &b.graph.environ_map;
    const bin = if (env.get("CARGO_HOME")) |home| b.pathJoin(&.{ home, "bin", "cargo" }) else if (env.get("HOME")) |home| b.pathJoin(&.{ home, ".cargo", "bin", "cargo" }) else return "cargo";
    return b.findProgram(.{ .names = &.{bin} }) orelse "cargo";
}
