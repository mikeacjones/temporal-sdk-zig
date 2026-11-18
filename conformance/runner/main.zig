//! conformance-runner run --suite <dir> --suite-commit <sha> --not-applicable <file>
//!                        --report <file> --work-dir <dir> [--feature <pattern>]...
//! conformance-runner feature <area>/<name> --address <host:port> --namespace <ns>
//!                            --task-queue <queue> --result <file>
const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");
const features = @import("features");
const dynamic_config = @import("dynamic_config.zig");
const inventory = @import("inventory.zig");
const report = @import("report.zig");

const Allocator = std.mem.Allocator;
const Io = std.Io;
const timeout_seconds = 240;
const namespace = "default";

const ParentOption = enum { suite, @"suite-commit", @"not-applicable", report, @"work-dir" };
const ChildOption = enum { address, namespace, @"task-queue", result };
const ChildResult = struct {
    status: enum { pass, fail },
    message: ?[]const u8 = null,
};

pub fn main(init: std.process.Init) !void {
    const arena = init.arena.allocator();
    const args = try init.minimal.args.toSlice(arena);
    if (args.len < 2) return usage();
    if (std.mem.eql(u8, args[1], "run")) {
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
        \\  conformance-runner run --suite <dir> --suite-commit <sha> --not-applicable <file> --report <file> --work-dir <dir> [--feature <pattern>]...
        \\  conformance-runner feature <area>/<name> --address <host:port> --namespace <ns> --task-queue <queue> --result <file>
    , .{});
    return error.InvalidArguments;
}

fn parseOptions(
    comptime Option: type,
    args: []const [:0]const u8,
    arena: Allocator,
    patterns: ?*std.ArrayList([]const u8),
) !std.EnumArray(Option, ?[]const u8) {
    var values: std.EnumArray(Option, ?[]const u8) = .initFill(null);
    var i: usize = 0;
    while (i + 1 < args.len) : (i += 2) {
        const arg = args[i];
        if (patterns) |filters| {
            if (std.mem.eql(u8, arg, "--feature")) {
                try filters.append(arena, args[i + 1]);
                continue;
            }
        }
        if (!std.mem.startsWith(u8, arg, "--")) return error.InvalidArguments;
        const key = std.meta.stringToEnum(Option, arg[2..]) orelse return error.InvalidArguments;
        if (values.get(key) != null or args[i + 1].len == 0) return error.InvalidArguments;
        values.set(key, args[i + 1]);
    }
    if (i != args.len) return error.InvalidArguments;
    for (values.values) |value| if (value == null) return error.InvalidArguments;
    return values;
}

fn runParent(gpa: Allocator, arena: Allocator, io: Io, args: []const [:0]const u8, registry: []const features.Entry) !void {
    const cwd = Io.Dir.cwd();
    // Clear the previous report even if another option is invalid.
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

    var patterns: std.ArrayList([]const u8) = .empty;
    const options = try parseOptions(ParentOption, args, arena, &patterns);
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
    const server_args = try dynamic_config.serverArgs(arena, entries);

    var output = cwd.createFileAtomic(io, report_path, .{ .make_path = true, .replace = true }) catch |err| {
        std.log.err("cannot create report {s}: {t}", .{ report_path, err });
        return err;
    };
    defer output.deinit(io);
    const outcomes = try arena.alloc(?report.Outcome, written.len);
    @memset(outcomes, null);
    for (written) |path| {
        if (inventory.selected(patterns.items, path)) {
            try runSelected(gpa, arena, io, work_path, written, patterns.items, outcomes, server_args);
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

fn runSelected(
    gpa: Allocator,
    arena: Allocator,
    io: Io,
    work_path: []const u8,
    written: []const []const u8,
    patterns: []const []const u8,
    outcomes: []?report.Outcome,
    server_args: []const []const u8,
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

    var server = try temporal.testing.DevServer.start(gpa, io, .{ .namespace = namespace, .port = 0, .extra_args = server_args });
    defer server.stop(io);
    std.log.info("conformance dev server at {s}, child logs in {s}", .{ server.address(), run_path });
    for (written, outcomes, 0..) |path, *outcome, i| {
        if (!inventory.selected(patterns, path)) continue;
        const task_queue = try std.fmt.allocPrint(arena, "conformance-{s}", .{path});
        const result_path = try std.fmt.allocPrint(arena, "{s}/{d}.result.json", .{ run_path, i });
        const log_path = try std.fmt.allocPrint(arena, "{s}/{d}.log", .{ run_path, i });
        std.log.info("running {s} (log {s})", .{ path, log_path });
        outcome.* = runOne(arena, io, exe, path, server.address(), task_queue, result_path, log_path) catch |err| {
            std.log.err("cannot run {s} (log {s}, result {s}): {t}", .{ path, log_path, result_path, err });
            return err;
        };
    }
}

fn runOne(
    arena: Allocator,
    io: Io,
    exe: []const u8,
    path: []const u8,
    address: []const u8,
    task_queue: []const u8,
    result_path: []const u8,
    log_path: []const u8,
) !report.Outcome {
    const cwd = Io.Dir.cwd();
    const log_file = try cwd.createFile(io, log_path, .{ .read = true });
    defer log_file.close(io);
    var child = try std.process.spawn(io, .{
        .argv = &.{ exe, "feature", path, "--address", address, "--namespace", namespace, "--task-queue", task_queue, "--result", result_path },
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
    const options = try parseOptions(ChildOption, args, gpa, null);
    const feature = for (features.all) |entry| {
        if (std.mem.eql(u8, path, entry.path)) break entry.feature;
    } else {
        std.log.err("unknown Zig feature {s}", .{path});
        return error.UnknownFeature;
    };

    var state = std.heap.ArenaAllocator.init(gpa);
    defer state.deinit();
    const arena = state.allocator();
    const outcome = executeFeature(gpa, arena, io, path, feature, options.get(.address).?, options.get(.namespace).?, options.get(.@"task-queue").?);
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

fn executeFeature(
    gpa: Allocator,
    arena: Allocator,
    io: Io,
    path: []const u8,
    feature: *const harness.Feature,
    address: []const u8,
    ns: []const u8,
    task_queue: []const u8,
) report.Outcome {
    var client_options: temporal.client.Options = .{};
    if (feature.configureClient) |configure| configure(&client_options);
    client_options.host_port = address;
    client_options.namespace = ns;
    var client = temporal.client.Client.connect(gpa, io, client_options) catch |err|
        return failure(arena, "connect to {s} returned error.{t}", .{ address, err });
    defer client.close();
    var ctx: harness.Context = .{ .gpa = gpa, .io = io, .arena = arena, .client = &client, .feature = path, .namespace = ns, .task_queue = task_queue };

    if (!feature.has_worker) return execute(feature, &ctx);
    return executeWithWorker(feature, &ctx);
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
    var worker_result: temporal.worker.Worker.RunError!void = {};
    const outcome = blk: {
        defer {
            worker.stop();
            worker_result = running.await(io);
        }
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
    _ = inventory;
    _ = report;
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
