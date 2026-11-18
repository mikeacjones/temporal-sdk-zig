//! conformance-runner map --suite <dir> --not-applicable <file> --parity-map <file>
//! conformance-runner check --report <file> --parity-map <file> --parity <PARITY.md>
//! conformance-runner run --suite <dir> --suite-commit <sha> --not-applicable <file>
//!                        --report <file> --work-dir <dir> [--feature <pattern>]... [--stdout-to stderr]
//! conformance-runner feature <area>/<name> --address <host:port> --namespace <ns>
//!                            --task-queue <queue> --result <file> [--variant <name>]
//!                            [--nexus-endpoint <name>] [--capability <name>=<true|false>]...
const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");
const features = @import("features");
const dynamic_config = @import("dynamic_config.zig");
const feature_config = @import("feature_config.zig");
const inventory = @import("inventory.zig");
const parity_check = @import("parity_check.zig");
const parity_map = @import("parity_map.zig");
const parity_table = @import("parity_table.zig");
const report = @import("report.zig");

const Allocator = std.mem.Allocator;
const Io = std.Io;
const timeout_seconds = 240;
const namespace = "default";

const MapOption = enum { suite, @"not-applicable", @"parity-map" };
const CheckOption = enum { report, @"parity-map", parity };
const ParentOption = enum { suite, @"suite-commit", @"not-applicable", report, @"work-dir", @"stdout-to" };
const ChildOption = enum { address, namespace, @"task-queue", result, variant, @"nexus-endpoint" };
const ChildResult = struct {
    status: enum { pass, fail },
    message: ?[]const u8 = null,
};

pub fn main(init: std.process.Init) !void {
    const arena = init.arena.allocator();
    const args = try init.minimal.args.toSlice(arena);
    if (args.len < 2) return usage();
    if (std.mem.eql(u8, args[1], "map")) {
        runMap(init.gpa, arena, init.io, args[2..]) catch |err| {
            if (err == error.InvalidArguments) return usage();
            std.log.err("cannot validate the parity map: {t}", .{err});
            std.process.exit(1);
        };
    } else if (std.mem.eql(u8, args[1], "check")) {
        runCheck(init.gpa, arena, init.io, args[2..]) catch |err| {
            if (err == error.InvalidArguments) return usage();
            std.log.err("cannot check conformance: {t}", .{err});
            std.process.exit(1);
        };
    } else if (std.mem.eql(u8, args[1], "run")) {
        runParent(init.gpa, arena, init.io, args[2..], &features.all) catch |err| {
            if (err == error.InvalidArguments) return usage();
            return err;
        };
    } else if (std.mem.eql(u8, args[1], "feature")) {
        if (args.len < 3) return usage();
        runChild(init.gpa, init.io, args[2], args[3..]) catch |err| {
            if (err == error.InvalidArguments) return usage();
            return err;
        };
    } else return usage();
}

fn usage() error{InvalidArguments} {
    std.log.err(
        \\usage:
        \\  conformance-runner map --suite <dir> --not-applicable <file> --parity-map <file>
        \\  conformance-runner check --report <file> --parity-map <file> --parity <PARITY.md>
        \\  conformance-runner run --suite <dir> --suite-commit <sha> --not-applicable <file> --report <file> --work-dir <dir>
        \\                         [--feature <pattern>]... [--stdout-to stderr]
        \\  conformance-runner feature <area>/<name> --address <host:port> --namespace <ns> --task-queue <queue> --result <file>
        \\                             [--variant <name>] [--nexus-endpoint <name>] [--capability <name>=<true|false>]...
    , .{});
    return error.InvalidArguments;
}

fn parseOptions(
    comptime Option: type,
    args: []const [:0]const u8,
    arena: Allocator,
    repeated: ?struct { flag: []const u8, values: *std.ArrayList([]const u8) },
    optional: []const Option,
) !std.EnumArray(Option, ?[]const u8) {
    var values: std.EnumArray(Option, ?[]const u8) = .initFill(null);
    var i: usize = 0;
    while (i + 1 < args.len) : (i += 2) {
        const arg = args[i];
        if (repeated) |option| {
            if (std.mem.eql(u8, arg, option.flag)) {
                try option.values.append(arena, args[i + 1]);
                continue;
            }
        }
        if (!std.mem.startsWith(u8, arg, "--")) return error.InvalidArguments;
        const key = std.meta.stringToEnum(Option, arg[2..]) orelse return error.InvalidArguments;
        if (values.get(key) != null or args[i + 1].len == 0) return error.InvalidArguments;
        values.set(key, args[i + 1]);
    }
    if (i != args.len) return error.InvalidArguments;
    var keys = values.iterator();
    while (keys.next()) |entry| {
        if (entry.value.* == null and std.mem.indexOfScalar(Option, optional, entry.key) == null)
            return error.InvalidArguments;
    }
    return values;
}

fn runMap(gpa: Allocator, arena: Allocator, io: Io, args: []const [:0]const u8) !void {
    const options = try parseOptions(MapOption, args, arena, null, &.{});
    const suite_path = try std.fs.path.join(arena, &.{ options.get(.suite).?, "features" });
    const na_path = options.get(.@"not-applicable").?;
    const map_path = options.get(.@"parity-map").?;
    const cwd = Io.Dir.cwd();
    const features_dir = cwd.openDir(io, suite_path, .{ .iterate = true }) catch |err| {
        std.log.err("cannot open {s}: {t}", .{ suite_path, err });
        std.process.exit(1);
    };
    defer features_dir.close(io);
    const suite = inventory.scan(arena, io, features_dir) catch |err| {
        std.log.err("cannot scan {s}: {t}", .{ suite_path, err });
        std.process.exit(1);
    };
    const na_source = cwd.readFileAllocOptions(io, na_path, arena, .limited(64 * 1024), .of(u8), 0) catch |err| {
        std.log.err("cannot read {s}: {t}", .{ na_path, err });
        std.process.exit(1);
    };
    const not_applicable = inventory.parseNotApplicable(gpa, arena, na_source, na_path) orelse std.process.exit(1);
    const na_paths = try arena.alloc([]const u8, not_applicable.len);
    for (not_applicable, na_paths) |item, *path| path.* = item.feature;

    const map_source = cwd.readFileAllocOptions(io, map_path, arena, .limited(1024 * 1024), .of(u8), 0) catch |err| {
        std.log.err("cannot read {s}: {t}", .{ map_path, err });
        std.process.exit(1);
    };
    const entries = switch (try parity_map.parse(gpa, arena, map_source, map_path)) {
        .entries => |entries| entries,
        .problem => |message| {
            std.log.err("{s}", .{message});
            std.process.exit(1);
        },
    };
    const problems = try parity_map.validate(arena, entries, suite.features, na_paths);
    for (problems) |message| std.log.err("{s}", .{message});
    if (problems.len != 0) std.process.exit(1);

    const summary = try parity_map.summarize(arena, entries);
    var buffer: [1024]u8 = undefined;
    var stderr = Io.File.stderr().writerStreaming(io, &buffer);
    try stderr.interface.print("{s}: {d} entries, {d} mapped to {d} PARITY.md rows, {d} none\n", .{
        map_path, summary.entries, summary.mapped, summary.rows, summary.none,
    });
    try stderr.interface.flush();
}

fn runCheck(gpa: Allocator, arena: Allocator, io: Io, args: []const [:0]const u8) !void {
    const options = try parseOptions(CheckOption, args, arena, null, &.{});
    const report_path = options.get(.report).?;
    const map_path = options.get(.@"parity-map").?;
    const verdict = try parity_check.checkSources(gpa, arena, .{
        .report_path = report_path,
        .report_json = readCheckInput(arena, io, report_path),
        .map_path = map_path,
        .map_source = readCheckInput(arena, io, map_path),
        .parity_source = readCheckInput(arena, io, options.get(.parity).?),
    });
    switch (verdict) {
        .refused => |messages| for (messages) |message| std.log.err("{s}", .{message}),
        .findings => |findings| {
            var buffer: [4096]u8 = undefined;
            var stdout = Io.File.stdout().writerStreaming(io, &buffer);
            try parity_check.write(findings, &stdout.interface);
            try stdout.interface.flush();
        },
    }
    const code = parity_check.exitCode(verdict);
    if (code != 0) std.process.exit(code);
}

fn readCheckInput(arena: Allocator, io: Io, path: []const u8) [:0]const u8 {
    return Io.Dir.cwd().readFileAllocOptions(io, path, arena, .unlimited, .of(u8), 0) catch |err| {
        std.log.err("cannot read {s}: {t}", .{ path, err });
        std.process.exit(1);
    };
}

fn runParent(gpa: Allocator, arena: Allocator, io: Io, args: []const [:0]const u8, registry: []const features.Entry) !void {
    const cwd = Io.Dir.cwd();
    var patterns: std.ArrayList([]const u8) = .empty;
    var parsed = parseOptions(ParentOption, args, arena, .{ .flag = "--feature", .values = &patterns }, &.{.@"stdout-to"});
    // An invalid --stdout-to value fails like any other invalid option: only
    // after the previous report is cleared below, and without redirecting.
    if (parsed) |options| {
        if (options.get(.@"stdout-to")) |destination| redirect: {
            if (!std.mem.eql(u8, destination, "stderr")) {
                parsed = error.InvalidArguments;
                break :redirect;
            }
            const fd = std.c.dup2(2, 1);
            if (fd < 0) {
                std.log.err("cannot redirect conformance stdout to stderr: {t}", .{std.posix.errno(fd)});
                return error.StdoutRedirectFailed;
            }
        }
    } else |_| {}

    // Clear the previous report even when option parsing fails.
    var i: usize = 0;
    while (i + 1 < args.len) : (i += 2) {
        if (!std.mem.eql(u8, args[i], "--report")) continue;
        cwd.deleteFile(io, args[i + 1]) catch |err| switch (err) {
            error.FileNotFound => {},
            else => {
                std.log.err("cannot remove previous report {s}: {t}", .{ args[i + 1], err });
                return err;
            },
        };
    }

    const options = try parsed;
    const suite_path = options.get(.suite).?;
    const na_path = options.get(.@"not-applicable").?;
    const report_path = options.get(.report).?;
    const work_path = options.get(.@"work-dir").?;

    const suite_dir = cwd.openDir(io, suite_path, .{}) catch |err| {
        std.log.err("cannot open suite {s}: {t}", .{ suite_path, err });
        return err;
    };
    defer suite_dir.close(io);
    const features_dir = suite_dir.openDir(io, "features", .{ .iterate = true }) catch |err| {
        std.log.err("cannot open {s}/features: {t}", .{ suite_path, err });
        return err;
    };
    defer features_dir.close(io);
    const suite = inventory.scan(arena, io, features_dir) catch |err| {
        std.log.err("cannot scan {s}/features: {t}", .{ suite_path, err });
        return err;
    };
    const source = cwd.readFileAllocOptions(io, na_path, arena, .limited(64 * 1024), .of(u8), 0) catch |err| {
        std.log.err("cannot read not-applicable list {s}: {t}", .{ na_path, err });
        return err;
    };
    const not_applicable = inventory.parseNotApplicable(gpa, arena, source, na_path) orelse return error.InvalidNotApplicable;

    const written = try arena.alloc([]const u8, registry.len);
    for (registry, written) |entry, *path| path.* = entry.path;
    if (try inventory.check(arena, suite, not_applicable, written, patterns.items)) |problem| {
        std.log.err("{s}", .{problem});
        return error.InvalidInventory;
    }

    const config_path = "dockerfiles/dynamicconfig/docker.yaml";
    const yaml = suite_dir.readFileAlloc(io, config_path, arena, .limited(1024 * 1024)) catch |err| {
        std.log.warn("cannot read {s}/{s}: {t}", .{ suite_path, config_path, err });
        return switch (err) {
            error.FileNotFound, error.StreamTooLong => error.InvalidDynamicConfig,
            else => err,
        };
    };
    const entries = dynamic_config.parse(arena, yaml) catch |err| {
        std.log.warn("cannot parse {s}/{s}: {t}", .{ suite_path, config_path, err });
        return err;
    };

    var output = cwd.createFileAtomic(io, report_path, .{ .make_path = true, .replace = true }) catch |err| {
        std.log.err("cannot create report {s}: {t}", .{ report_path, err });
        return err;
    };
    defer output.deinit(io);
    const outcomes = try arena.alloc(?report.Outcome, written.len);
    @memset(outcomes, null);
    const selected = try selectConfigs(arena, io, features_dir, written, patterns.items, outcomes);
    for (selected) |config| {
        if (config != null) {
            try runSelected(gpa, arena, io, work_path, written, selected, outcomes, entries);
            break;
        }
    }

    const result = try report.assemble(arena, options.get(.@"suite-commit").?, temporal.version, suite, not_applicable, written, patterns.items, outcomes);
    writeReport(io, &output, result) catch |err| {
        std.log.err("cannot write report {s}: {t}", .{ report_path, err });
        return err;
    };
    showReport(io, result, report_path) catch {};
}

fn selectConfigs(
    arena: Allocator,
    io: Io,
    features_dir: Io.Dir,
    written: []const []const u8,
    patterns: []const []const u8,
    outcomes: []?report.Outcome,
) error{OutOfMemory}![]const ?feature_config.Config {
    const selected = try arena.alloc(?feature_config.Config, written.len);
    for (written, selected, outcomes) |path, *config, *outcome| {
        config.* = null;
        if (!inventory.selected(patterns, path)) continue;
        switch (try readFeatureConfig(arena, io, features_dir, path)) {
            .config => |value| config.* = value,
            .unsupported => |message| outcome.* = .{ .fail = message },
        }
    }
    return selected;
}

fn readFeatureConfig(arena: Allocator, io: Io, features_dir: Io.Dir, path: []const u8) error{OutOfMemory}!feature_config.Parsed {
    const config_path = try std.fmt.allocPrint(arena, "{s}/config.json", .{path});
    const source = features_dir.readFileAlloc(io, config_path, arena, .limited(1024 * 1024)) catch |err| switch (err) {
        error.FileNotFound => return .{ .config = .{} },
        error.OutOfMemory => return error.OutOfMemory,
        else => return .{ .unsupported = try std.fmt.allocPrint(arena, "cannot read {s}: {t}", .{ config_path, err }) },
    };
    return switch (try feature_config.parse(arena, source)) {
        .config => |config| .{ .config = config },
        .unsupported => |message| .{ .unsupported = try std.fmt.allocPrint(arena, "{s}: {s}", .{ config_path, message }) },
    };
}

fn runSelected(
    gpa: Allocator,
    arena: Allocator,
    io: Io,
    work_path: []const u8,
    written: []const []const u8,
    selected: []const ?feature_config.Config,
    outcomes: []?report.Outcome,
    defaults: []const dynamic_config.Entry,
) !void {
    const exe = try std.process.executablePathAlloc(io, arena);
    // The build's cache directory can be shared by simultaneous invocations.
    var random: u64 = undefined;
    io.random(std.mem.asBytes(&random));
    const run_path = try std.fmt.allocPrint(arena, "{s}/{x}", .{ work_path, random });
    const cwd = Io.Dir.cwd();
    cwd.createDir(io, run_path, .default_dir) catch |err| {
        std.log.err("cannot create conformance work directory {s}: {t}", .{ run_path, err });
        return err;
    };

    const last_shared = for (0..selected.len) |offset| {
        const i = selected.len - 1 - offset;
        const config = selected[i] orelse continue;
        if (config.run_variants.len == 0) break i;
    } else null;
    var shared: ?temporal.testing.DevServer = null;
    defer if (shared) |*server| server.stop(io);
    if (last_shared != null) {
        const server_args = try dynamic_config.serverArgs(arena, defaults);
        shared = try temporal.testing.DevServer.start(gpa, io, .{ .namespace = namespace, .port = 0, .extra_args = server_args });
        std.log.info("conformance dev server at {s}, child logs in {s}", .{ shared.?.address(), run_path });
    }

    const runner: Runner = .{ .gpa = gpa, .arena = arena, .io = io, .exe = exe, .work_dir = run_path };
    for (written, selected, outcomes, 0..) |path, maybe_config, *outcome, i| {
        const config = maybe_config orelse continue;
        const needs_endpoint = config.needs_nexus_endpoint or std.mem.startsWith(u8, path, "nexus/");
        if (config.run_variants.len == 0) {
            outcome.* = try runner.runFeature(shared.?.address(), path, i, null, needs_endpoint, null);
            if (last_shared == i) {
                shared.?.stop(io);
                shared = null;
            }
            continue;
        }
        const variants = try arena.alloc(report.VariantOutcome, config.run_variants.len);
        for (config.run_variants, variants, 0..) |variant, *variant_outcome, j| {
            variant_outcome.* = .{
                .name = variant.name,
                .outcome = try runner.runVariant(defaults, path, i, j, needs_endpoint, variant),
            };
        }
        outcome.* = try report.combineVariants(arena, path, variants);
    }
}

const Runner = struct {
    gpa: Allocator,
    arena: Allocator,
    io: Io,
    exe: []const u8,
    work_dir: []const u8,

    fn runVariant(
        runner: Runner,
        defaults: []const dynamic_config.Entry,
        path: []const u8,
        index: usize,
        variant_index: usize,
        needs_endpoint: bool,
        variant: feature_config.Variant,
    ) !report.Outcome {
        const merged = try dynamic_config.merge(runner.arena, defaults, variant.dynamic_config);
        const args = try dynamic_config.serverArgs(runner.arena, merged);
        var server = temporal.testing.DevServer.start(runner.gpa, runner.io, .{ .namespace = namespace, .port = 0, .extra_args = args }) catch |err|
            return failure(runner.arena, "cannot start variant dev server: {t}", .{err});
        defer server.stop(runner.io);
        std.log.info("conformance variant {s}#{s} dev server at {s}", .{ path, variant.name, server.address() });
        return runner.runFeature(server.address(), path, index, variant_index, needs_endpoint, variant);
    }

    fn runFeature(
        runner: Runner,
        address: []const u8,
        path: []const u8,
        index: usize,
        variant_index: ?usize,
        needs_endpoint: bool,
        variant: ?feature_config.Variant,
    ) !report.Outcome {
        const arena = runner.arena;
        const variant_name = if (variant) |v| v.name else null;
        const capabilities = if (variant) |v| v.expect_namespace_capabilities else &.{};
        const task_queue = if (variant_name) |name|
            try std.fmt.allocPrint(arena, "conformance-{s}-{s}", .{ path, name })
        else
            try std.fmt.allocPrint(arena, "conformance-{s}", .{path});
        const stem = if (variant_index) |j|
            try std.fmt.allocPrint(arena, "{s}/{d}-{d}", .{ runner.work_dir, index, j })
        else
            try std.fmt.allocPrint(arena, "{s}/{d}", .{ runner.work_dir, index });
        const log_path = try std.fmt.allocPrint(arena, "{s}.log", .{stem});
        const label = if (variant_name) |name| try std.fmt.allocPrint(arena, "{s}#{s}", .{ path, name }) else path;

        var client: ?temporal.client.Client = null;
        defer if (client) |*c| c.close();
        if (needs_endpoint or capabilities.len != 0) {
            client = temporal.client.Client.connect(runner.gpa, runner.io, .{
                .host_port = address,
                .namespace = namespace,
                .rpc_timeout = .fromSeconds(10),
            }) catch |err| return failure(arena, "cannot connect to {s} for feature setup: {t}", .{ address, err });
        }
        var rpc_failure: ?temporal.client.RpcFailure = null;
        if (capabilities.len != 0) {
            const response = client.?.callService(.workflow, "DescribeNamespace", arena, .{ .namespace = namespace }, &rpc_failure) catch |err|
                return serviceFailure(arena, "DescribeNamespace", namespace, err, rpc_failure);
            const actual: NamespaceCapabilities = if (response.namespace_info) |info| info.capabilities orelse .{} else .{};
            if (try checkCapabilities(arena, actual, capabilities)) |message| return .{ .fail = message };
        }

        const NexusEndpoint = temporal.proto.temporal.api.nexus.v1.Endpoint;
        var endpoint: ?NexusEndpoint = null;
        var endpoint_name: ?[]const u8 = null;
        defer if (endpoint) |ep| {
            // Cleanup must also run if waiting for the child was canceled.
            const protection = runner.io.swapCancelProtection(.blocked);
            defer _ = runner.io.swapCancelProtection(protection);
            if (client.?.callService(.operator, "DeleteNexusEndpoint", arena, .{ .id = ep.id, .version = ep.version }, &rpc_failure)) |_| {
                std.log.debug("deleted Nexus endpoint {s}", .{endpoint_name.?});
            } else |err| {
                const outcome = serviceFailure(arena, "DeleteNexusEndpoint", endpoint_name.?, err, rpc_failure);
                std.log.warn("{s}", .{outcome.fail});
            }
        };
        if (needs_endpoint) {
            const name = try arena.dupe(u8, task_queue);
            std.mem.replaceScalar(u8, name, '/', '-');
            std.mem.replaceScalar(u8, name, '_', '-');
            const response = client.?.callService(.operator, "CreateNexusEndpoint", arena, .{ .spec = .{
                .name = name,
                .target = .{ .variant = .{ .worker = .{ .namespace = namespace, .task_queue = task_queue } } },
            } }, &rpc_failure) catch |err| return serviceFailure(arena, "CreateNexusEndpoint", name, err, rpc_failure);
            endpoint = response.endpoint orelse return failure(arena, "CreateNexusEndpoint {s} returned no endpoint", .{name});
            endpoint_name = name;
            std.log.debug("created Nexus endpoint {s} for task queue {s}", .{ name, task_queue });
        }
        const result_path = try std.fmt.allocPrint(arena, "{s}.result.json", .{stem});
        var argv: std.ArrayList([]const u8) = .empty;
        try argv.appendSlice(arena, &.{ runner.exe, "feature", path, "--address", address, "--namespace", namespace, "--task-queue", task_queue, "--result", result_path });
        if (variant_name) |name| try argv.appendSlice(arena, &.{ "--variant", name });
        if (endpoint_name) |name| try argv.appendSlice(arena, &.{ "--nexus-endpoint", name });
        for (capabilities) |capability| {
            const arg = try std.fmt.allocPrint(arena, "{s}={}", .{ capability.name, capability.value });
            try argv.appendSlice(arena, &.{ "--capability", arg });
        }
        std.log.info("running {s} (log {s})", .{ label, log_path });
        return runOne(arena, runner.io, argv.items, result_path, log_path) catch |err|
            failure(arena, "cannot run child (log {s}, result {s}): {t}", .{ log_path, result_path, err });
    }
};

fn serviceFailure(
    arena: Allocator,
    method: []const u8,
    target: []const u8,
    err: temporal.client.CallServiceError,
    rpc_failure: ?temporal.client.RpcFailure,
) report.Outcome {
    if (rpc_failure) |detail| return failure(arena, "{s} {s} returned error.{t}: {s}", .{ method, target, err, detail.message });
    return failure(arena, "{s} {s} returned error.{t}", .{ method, target, err });
}

const NamespaceCapabilities = temporal.proto.temporal.api.namespace.v1.NamespaceInfo.Capabilities;

fn checkCapabilities(arena: Allocator, actual: NamespaceCapabilities, expected: []const harness.Context.Capability) error{OutOfMemory}!?[]const u8 {
    capabilities: for (expected) |capability| {
        inline for (@typeInfo(@TypeOf(NamespaceCapabilities._desc_table)).@"struct".field_names) |field| {
            if (std.mem.eql(u8, capability.name, comptime capabilityJsonName(field))) {
                const value = @field(actual, field);
                if (@TypeOf(value) == bool) {
                    if (value != capability.value) return try std.fmt.allocPrint(arena, "namespace capability \"{s}\" = {}, expected {}", .{ capability.name, value, capability.value });
                    continue :capabilities;
                } else return try std.fmt.allocPrint(arena, "namespace capability \"{s}\" is not boolean", .{capability.name});
            }
        }
        return try std.fmt.allocPrint(arena, "unknown namespace capability \"{s}\"", .{capability.name});
    }
    return null;
}

fn capabilityJsonName(comptime field: []const u8) []const u8 {
    comptime {
        var name: []const u8 = "";
        var upper = false;
        for (field) |c| {
            if (c == '_') {
                upper = true;
                continue;
            }
            name = name ++ &[_]u8{if (upper) std.ascii.toUpper(c) else c};
            upper = false;
        }
        return name;
    }
}

fn runOne(
    arena: Allocator,
    io: Io,
    argv: []const []const u8,
    result_path: []const u8,
    log_path: []const u8,
) !report.Outcome {
    const cwd = Io.Dir.cwd();
    const log_file = try cwd.createFile(io, log_path, .{ .read = true });
    defer log_file.close(io);
    var child = try std.process.spawn(io, .{
        .argv = argv,
        .stdin = .ignore,
        .stdout = .{ .file = log_file },
        .stderr = .{ .file = log_file },
    });
    const term = try waitExit(io, &child, .fromSeconds(timeout_seconds));
    return childOutcome(arena, io, term, result_path, log_file);
}

fn childOutcome(arena: Allocator, io: Io, term: ?std.process.Child.Term, result_path: []const u8, log_file: Io.File) !report.Outcome {
    const detail = if (term) |exit| blk: {
        const reason = if (exit.success()) result: {
            const source = Io.Dir.cwd().readFileAlloc(io, result_path, arena, .limited(1024 * 1024)) catch |err|
                break :result try std.fmt.allocPrint(arena, "cannot read child result: {t}", .{err});
            const result = std.json.parseFromSliceLeaky(ChildResult, arena, source, .{}) catch |err|
                break :result try std.fmt.allocPrint(arena, "invalid child result: {t}", .{err});
            switch (result.status) {
                .pass => if (result.message == null) return .pass,
                .fail => if (result.message) |msg| if (msg.len != 0) return .{ .fail = msg },
            }
            break :result "invalid child result status or message";
        } else "child did not finish successfully";
        break :blk try std.fmt.allocPrint(arena, "child {f}: {s}", .{ exit, reason });
    } else std.fmt.comptimePrint("did not finish within {d} s", .{timeout_seconds});

    var buf: [8000]u8 = undefined;
    const size = log_file.length(io) catch 0;
    const n = log_file.readPositionalAll(io, &buf, size -| buf.len) catch 0;
    const tail = if (std.unicode.utf8ValidateSlice(buf[0..n]))
        buf[0..n]
    else
        try std.fmt.allocPrint(arena, "{f}", .{std.zig.fmtString(buf[0..n])});
    return .{ .fail = if (tail.len == 0) detail else try std.fmt.allocPrint(arena, "{s}\n{s}", .{ detail, tail }) };
}

/// The child has no pipes. A successful waitpid reaps it and clears its PID before cleanup.
fn waitExit(io: Io, child: *std.process.Child, timeout: Io.Duration) !?std.process.Child.Term {
    const pid = child.id.?;
    defer if (child.id != null) {
        std.posix.kill(pid, .KILL) catch {};
        // Reaping after a kill must also work when the caller's Io task was canceled.
        while (std.posix.errno(std.c.waitpid(pid, null, 0)) == .INTR) {}
        child.id = null;
    };
    const start = Io.Timestamp.now(io, .boot);
    while (true) {
        var status: c_int = undefined;
        const waited = std.c.waitpid(pid, &status, std.posix.W.NOHANG);
        switch (std.posix.errno(waited)) {
            .SUCCESS, .INTR => {},
            .CHILD => {
                child.id = null;
                return error.ChildNotFound;
            },
            else => |err| return std.posix.unexpectedErrno(err),
        }
        if (waited == pid) {
            const s: u32 = @bitCast(status);
            if (std.posix.W.IFEXITED(s)) {
                child.id = null;
                return .{ .exited = std.posix.W.EXITSTATUS(s) };
            }
            if (std.posix.W.IFSIGNALED(s)) {
                child.id = null;
                return .{ .signal = std.posix.W.TERMSIG(s) };
            }
        }
        const remaining = timeout.nanoseconds - start.durationTo(Io.Timestamp.now(io, .boot)).nanoseconds;
        if (remaining <= 0) return null;
        try io.sleep(.{ .nanoseconds = @min(100 * std.time.ns_per_ms, remaining) }, .boot);
    }
}

fn writeReport(io: Io, output: *Io.File.Atomic, result: report.Report) !void {
    var buffer: [4096]u8 = undefined;
    var writer = output.file.writer(io, &buffer);
    try report.write(result, &writer.interface);
    try writer.interface.flush();
    try output.replace(io);
}

fn showReport(io: Io, result: report.Report, path: []const u8) !void {
    var buffer: [4096]u8 = undefined;
    var stdout = Io.File.stdout().writerStreaming(io, &buffer);
    const out = &stdout.interface;
    var counts: std.EnumArray(report.Status, usize) = .initFill(0);
    try out.print("\n{s:<48} {s:<15} detail\n", .{ "feature", "status" });
    for (result.features) |entry| {
        counts.getPtr(entry.status).* += 1;
        try out.print("{s:<48} {s:<15} {s}\n", .{ entry.path, @tagName(entry.status), entry.message orelse "" });
    }
    try out.print("\n{d} passed, {d} failed, {d} unwritten, {d} not applicable, {d} not run\nReport: {s}\n", .{
        counts.get(.pass), counts.get(.fail), counts.get(.unwritten), counts.get(.not_applicable), counts.get(.not_run), path,
    });
    try out.flush();
}

fn runChild(gpa: Allocator, io: Io, path: []const u8, args: []const [:0]const u8) !void {
    var state = std.heap.ArenaAllocator.init(gpa);
    defer state.deinit();
    const arena = state.allocator();
    var capability_args: std.ArrayList([]const u8) = .empty;
    const options = try parseOptions(ChildOption, args, arena, .{ .flag = "--capability", .values = &capability_args }, &.{ .variant, .@"nexus-endpoint" });
    const capabilities = try parseCapabilities(arena, capability_args.items);
    const feature = for (features.all) |entry| {
        if (std.mem.eql(u8, path, entry.path)) break entry.feature;
    } else {
        std.log.err("unknown Zig feature {s}", .{path});
        return error.UnknownFeature;
    };

    var ctx: harness.Context = .{
        .gpa = gpa,
        .io = io,
        .arena = arena,
        .client = undefined,
        .feature = path,
        .namespace = options.get(.namespace).?,
        .task_queue = options.get(.@"task-queue").?,
        .variant = options.get(.variant),
        .nexus_endpoint = options.get(.@"nexus-endpoint"),
        .capabilities = capabilities,
    };
    const outcome = executeFeature(feature, &ctx, options.get(.address).?);
    const result: ChildResult = switch (outcome) {
        .pass => .{ .status = .pass },
        .fail => |message| .{
            .status = .fail,
            .message = if (std.unicode.utf8ValidateSlice(message))
                message
            else
                try std.fmt.allocPrint(arena, "{f}", .{std.zig.fmtString(message)}),
        },
    };
    const data = try std.json.Stringify.valueAlloc(arena, result, .{ .emit_null_optional_fields = false });
    try Io.Dir.cwd().writeFile(io, .{ .sub_path = options.get(.result).?, .data = data });
}

fn parseCapabilities(arena: Allocator, args: []const []const u8) ![]const harness.Context.Capability {
    const capabilities = try arena.alloc(harness.Context.Capability, args.len);
    for (capabilities, args, 0..) |*capability, arg, i| {
        const separator = std.mem.indexOfScalar(u8, arg, '=') orelse return error.InvalidArguments;
        const name = arg[0..separator];
        if (name.len == 0) return error.InvalidArguments;
        for (capabilities[0..i]) |previous| {
            if (std.mem.eql(u8, previous.name, name)) return error.InvalidArguments;
        }
        const value = arg[separator + 1 ..];
        capability.* = .{ .name = name, .value = if (std.mem.eql(u8, value, "true"))
            true
        else if (std.mem.eql(u8, value, "false"))
            false
        else
            return error.InvalidArguments };
    }
    return capabilities;
}

fn executeFeature(feature: *const harness.Feature, ctx: *harness.Context, address: []const u8) report.Outcome {
    var client_options: temporal.client.Options = .{};
    if (feature.configureClient) |configure| configure(&client_options);
    client_options.host_port = address;
    client_options.namespace = ctx.namespace;
    var client = temporal.client.Client.connect(ctx.gpa, ctx.io, client_options) catch |err|
        return failure(ctx.arena, "connect to {s} returned error.{t}", .{ address, err });
    defer client.close();
    ctx.client = &client;

    if (!feature.has_worker) return execute(feature, ctx);
    return executeWithWorker(feature, ctx);
}

fn executeWithWorker(feature: *const harness.Feature, ctx: *harness.Context) report.Outcome {
    const io = ctx.io;
    const arena = ctx.arena;
    var worker = temporal.worker.Worker.init(ctx.gpa, io, ctx.client, ctx.task_queue, feature.worker_options) catch |err|
        return failure(arena, "worker init returned error.{t}", .{err});
    defer worker.deinit();
    feature.register(&worker) catch |err| return failure(arena, "registering feature definitions returned error.{t}", .{err});
    var running = io.concurrent(temporal.worker.Worker.run, .{&worker}) catch |err|
        return failure(arena, "starting worker task returned error.{t}", .{err});
    worker.waitUntilRunning() catch |start_err| {
        worker.stop();
        running.await(io) catch |err| {
            const message = worker.fatalMessage();
            return failure(arena, "worker failed to start with error.{t}{s}{s}", .{ err, if (message == null) "" else ": ", message orelse "" });
        };
        return failure(arena, "worker failed to start with error.{t}", .{start_err});
    };
    ctx.worker_ = &worker;
    ctx.running_ = &running;
    var worker_result: temporal.worker.Worker.RunError!void = {};
    const outcome = blk: {
        defer ctx.stopWorker() catch |err| switch (err) {
            error.NoWorker => {},
            else => |e| worker_result = e,
        };
        break :blk execute(feature, ctx);
    };
    worker_result catch |err| {
        const message = worker.fatalMessage();
        return switch (outcome) {
            .pass => failure(arena, "worker stopped with error.{t}{s}{s}", .{ err, if (message == null) "" else ": ", message orelse "" }),
            .fail => |detail| blk: {
                std.log.warn("worker stopped with error.{t}{s}{s}", .{ err, if (message == null) "" else ": ", message orelse "" });
                break :blk .{ .fail = detail };
            },
        };
    };
    return outcome;
}

fn execute(feature: *const harness.Feature, ctx: *harness.Context) report.Outcome {
    feature.execute(ctx) catch |err| {
        if (ctx.failure_message_) |message| {
            if (err == error.FeatureFailed) return .{ .fail = message };
            return failure(ctx.arena, "{s}: execute returned error.{t}", .{ message, err });
        }
        return failure(ctx.arena, "execute returned error.{t}", .{err});
    };
    return .pass;
}

fn failure(arena: Allocator, comptime fmt: []const u8, args: anytype) report.Outcome {
    return .{ .fail = std.fmt.allocPrint(arena, fmt, args) catch "out of memory formatting feature failure" };
}

test {
    _ = dynamic_config;
    _ = feature_config;
    _ = inventory;
    _ = parity_table;
    _ = parity_map;
    _ = parity_check;
    _ = report;
}

test "conformance map accepts only its three required options" {
    var state: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer state.deinit();
    const arena = state.allocator();
    const args = [_][:0]const u8{ "--parity-map", "map.zon", "--suite", "suite", "--not-applicable", "na.zon" };
    const options = try parseOptions(MapOption, &args, arena, null, &.{});
    try std.testing.expectEqualStrings("map.zon", options.get(.@"parity-map").?);
    try std.testing.expectEqualStrings("suite", options.get(.suite).?);
    try std.testing.expectEqualStrings("na.zon", options.get(.@"not-applicable").?);
    try std.testing.expectError(error.InvalidArguments, parseOptions(MapOption, args[0..4], arena, null, &.{}));
    try std.testing.expectError(error.InvalidArguments, parseOptions(MapOption, &(args ++ .{ "--suite", "again" }), arena, null, &.{}));
    try std.testing.expectError(error.InvalidArguments, parseOptions(MapOption, &(args ++ .{ "--feature", "*" }), arena, null, &.{}));
}

test "conformance plans only selected configs and reports unsupported keys without running them" {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var state: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer state.deinit();
    const arena = state.allocator();

    const paths = [_][]const u8{ "test/bad", "test/endpoint", "test/missing", "test/variants" };
    for (paths) |path| try tmp.dir.createDirPath(io, path);
    try tmp.dir.writeFile(io, .{ .sub_path = "test/bad/config.json", .data = "{\"extraSetting\": true}" });
    try tmp.dir.writeFile(io, .{ .sub_path = "test/endpoint/config.json", .data = "{\"needsNexusEndpoint\": true}" });
    try tmp.dir.writeFile(io, .{ .sub_path = "test/variants/config.json", .data = "{\"runVariants\": [{\"name\": \"one\"}]}" });

    var outcomes: [paths.len]?report.Outcome = @splat(null);
    const configs = try selectConfigs(arena, io, tmp.dir, &paths, &.{"test/bad"}, &outcomes);
    for (configs) |config| try std.testing.expectEqual(null, config);
    const result = try report.assemble(arena, "test", "0.0.0", .{ .features = &paths, .readme_only_directories = 0 }, &.{}, &paths, &.{"test/bad"}, &outcomes);
    try std.testing.expectEqual(report.Status.fail, result.features[0].status);
    try std.testing.expectEqualStrings("test/bad/config.json: unsupported key \"extraSetting\"", result.features[0].message.?);
    for (result.features[1..]) |entry| try std.testing.expectEqual(report.Status.not_run, entry.status);

    const all = try selectConfigs(arena, io, tmp.dir, &paths, &.{}, &outcomes);
    try std.testing.expect(all[1].?.needs_nexus_endpoint);
    try std.testing.expectEqualDeep(feature_config.Config{}, all[2].?);
    try std.testing.expectEqualStrings("one", all[3].?.run_variants[0].name);
}

test "conformance checks namespace capabilities by their JSON names" {
    var state: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer state.deinit();
    const arena = state.allocator();
    const actual: NamespaceCapabilities = .{ .worker_poll_complete_on_shutdown = true };
    try std.testing.expectEqual(null, try checkCapabilities(arena, actual, &.{
        .{ .name = "workerPollCompleteOnShutdown", .value = true },
        .{ .name = "workerHeartbeats", .value = false },
    }));
    try std.testing.expectEqualStrings("namespace capability \"workerPollCompleteOnShutdown\" = false, expected true", (try checkCapabilities(arena, .{}, &.{.{ .name = "workerPollCompleteOnShutdown", .value = true }})).?);
    try std.testing.expectEqualStrings("unknown namespace capability \"worker_heartbeats\"", (try checkCapabilities(arena, actual, &.{.{ .name = "worker_heartbeats", .value = false }})).?);
}

test "conformance child parses optional setup arguments and explicit false capabilities" {
    var state: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer state.deinit();
    const arena = state.allocator();
    var repeated: std.ArrayList([]const u8) = .empty;
    const options = try parseOptions(ChildOption, &.{
        "--address",        "localhost:1234",
        "--namespace",      "default",
        "--task-queue",     "conformance-a/b",
        "--result",         "result.json",
        "--variant",        "first",
        "--nexus-endpoint", "conformance-a-b-first",
        "--capability",     "workerHeartbeats=false",
        "--capability",     "eagerWorkflowStart=true",
    }, arena, .{ .flag = "--capability", .values = &repeated }, &.{ .variant, .@"nexus-endpoint" });
    try std.testing.expectEqualStrings("first", options.get(.variant).?);
    try std.testing.expectEqualStrings("conformance-a-b-first", options.get(.@"nexus-endpoint").?);
    try std.testing.expectEqualDeep(@as([]const harness.Context.Capability, &.{
        .{ .name = "workerHeartbeats", .value = false },
        .{ .name = "eagerWorkflowStart", .value = true },
    }), try parseCapabilities(arena, repeated.items));
    try std.testing.expectError(error.InvalidArguments, parseCapabilities(arena, &.{"workerHeartbeats=0"}));
    try std.testing.expectError(error.InvalidArguments, parseCapabilities(arena, &.{ "workerHeartbeats=true", "workerHeartbeats=false" }));
}

test "conformance parent removes a stale report before validating options" {
    const gpa = std.testing.allocator;
    const io = std.testing.io;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(io, .{ .sub_path = "report.json", .data = "stale" });
    const dir = try tmp.dir.realPathFileAlloc(io, ".", arena.allocator());
    const path = try std.fmt.allocPrintSentinel(arena.allocator(), "{s}/report.json", .{dir}, 0);
    try std.testing.expectError(error.InvalidArguments, runParent(gpa, arena.allocator(), io, &.{ "--bad-option", "value", "--report", path }, &.{}));
    try std.testing.expectError(error.FileNotFound, tmp.dir.access(io, "report.json", .{}));
}

test "conformance parent removes a stale report before rejecting a --stdout-to value other than stderr" {
    const gpa = std.testing.allocator;
    const io = std.testing.io;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try tmp.dir.realPathFileAlloc(io, ".", arena.allocator());
    const path = try std.fmt.allocPrintSentinel(arena.allocator(), "{s}/report.json", .{dir}, 0);
    for ([_][:0]const u8{ "stdout", "STDERR", "stderr2" }) |destination| {
        try tmp.dir.writeFile(io, .{ .sub_path = "report.json", .data = "stale" });
        const args: []const [:0]const u8 = &.{ "--suite", dir, "--suite-commit", "test", "--not-applicable", path, "--report", path, "--work-dir", dir, "--stdout-to", destination };
        try std.testing.expectError(error.InvalidArguments, runParent(gpa, arena.allocator(), io, args, &.{}));
        try std.testing.expectError(error.FileNotFound, tmp.dir.access(io, "report.json", .{}));
    }
}

test "conformance requires the suite dynamic config even with no features to run" {
    const level = std.testing.log_level;
    defer std.testing.log_level = level;
    std.testing.log_level = .err;

    const gpa = std.testing.allocator;
    const io = std.testing.io;
    var state: std.heap.ArenaAllocator = .init(gpa);
    defer state.deinit();
    const arena = state.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.createDir(io, "features", .default_dir);
    try tmp.dir.writeFile(io, .{ .sub_path = "not-applicable.zon", .data = ".{}" });
    const dir = try tmp.dir.realPathFileAlloc(io, ".", arena);
    const suite_path = try arena.dupeSentinel(u8, dir, 0);
    const na_path = try std.fmt.allocPrintSentinel(arena, "{s}/not-applicable.zon", .{dir}, 0);
    const report_path = try std.fmt.allocPrintSentinel(arena, "{s}/report.json", .{dir}, 0);
    const args: []const [:0]const u8 = &.{ "--suite", suite_path, "--suite-commit", "test", "--not-applicable", na_path, "--report", report_path, "--work-dir", suite_path };

    try std.testing.expectError(error.InvalidDynamicConfig, runParent(gpa, arena, io, args, &.{}));
    try std.testing.expectError(error.FileNotFound, tmp.dir.access(io, "report.json", .{}));

    try tmp.dir.createDirPath(io, "dockerfiles/dynamicconfig");
    try tmp.dir.writeFile(io, .{ .sub_path = "dockerfiles/dynamicconfig/docker.yaml", .data = "example.setting: []" });
    try std.testing.expectError(error.InvalidDynamicConfig, runParent(gpa, arena, io, args, &.{}));
    try std.testing.expectError(error.FileNotFound, tmp.dir.access(io, "report.json", .{}));
}

test "conformance child wait reports exits and signals and reaps timed-out children" {
    const io = std.testing.io;
    for ([_]struct { command: []const u8, term: ?std.process.Child.Term }{
        .{ .command = "exit 17", .term = .{ .exited = 17 } },
        .{ .command = "kill -TERM $$", .term = .{ .signal = .TERM } },
        .{ .command = "kill -STOP $$", .term = null },
    }) |case| {
        var child = try std.process.spawn(io, .{
            .argv = &.{ "/bin/sh", "-c", case.command },
            .stdin = .ignore,
            .stdout = .ignore,
            .stderr = .ignore,
        });
        const pid = child.id.?;
        const deadline: Io.Duration = if (case.term == null) .zero else .fromSeconds(5);
        try std.testing.expectEqualDeep(case.term, try waitExit(io, &child, deadline));
        try std.testing.expectEqual(null, child.id);
        var status: c_int = undefined;
        try std.testing.expectEqual(std.posix.E.CHILD, std.posix.errno(std.c.waitpid(pid, &status, std.posix.W.NOHANG)));
    }
}

test "conformance child timeout includes the end of its log" {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const log = "old line\n".* ++ @as([8000]u8, @splat('x')) ++ "\nworker error\n".*;
    try tmp.dir.writeFile(io, .{ .sub_path = "child.log", .data = &log });
    const file = try tmp.dir.openFile(io, "child.log", .{});
    defer file.close(io);
    const outcome = try childOutcome(arena.allocator(), io, null, "", file);
    const prefix = "did not finish within 240 s\n";
    try std.testing.expect(std.mem.startsWith(u8, outcome.fail, prefix));
    try std.testing.expectEqualStrings(log[log.len - 8000 ..], outcome.fail[prefix.len..]);
}

test "conformance worker startup report includes the underlying run error" {
    const gpa = std.testing.allocator;
    const io = std.testing.io;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    // Option validation runs before the lazy client connects.
    var client = try temporal.client.Client.init(gpa, io, .{});
    defer client.close();
    const Example = struct {
        fn run(_: *temporal.workflow.Context) !void {}
        fn drive(_: *harness.Context) !void {
            return error.ExecuteWasCalled;
        }
    };
    const feature = harness.define(.{
        .workflows = .{temporal.workflow.Def("test_workflow", Example.run)},
        .worker_options = .{ .max_activities_per_second = 0 },
        .execute = Example.drive,
    });
    var ctx: harness.Context = .{
        .gpa = gpa,
        .io = io,
        .arena = arena.allocator(),
        .client = &client,
        .feature = "test/workflow",
        .namespace = namespace,
        .task_queue = "conformance-test/workflow",
    };
    const outcome = executeWithWorker(&feature, &ctx);
    try std.testing.expectEqualStrings("worker failed to start with error.InvalidOptions", outcome.fail);
}
