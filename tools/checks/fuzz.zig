const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;
const Child = std.process.Child;

const Finding = struct { target: []const u8, bead: []const u8, reason: []const u8 };

const Run = struct {
    root: Io.Dir,
    zig: []const u8,
    cache_dir: []const u8,
    crash_dir: []const u8,
    seconds: u32,
    extra_args: []const []const u8,
    /// The environment of every nested build: this process's, without color or progress.
    environ: *const std.process.Environ.Map,
};

pub fn main(init: std.process.Init) void {
    const arena = init.arena.allocator();
    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.skip();
    const root = args.next() orelse usage();
    const known = args.next() orelse usage();
    const zig = args.next() orelse usage();
    const cache = args.next() orelse usage();
    const crashes = args.next() orelse usage();
    const seconds = std.fmt.parseInt(u32, args.next() orelse usage(), 10) catch usage();
    if (seconds == 0 or cache.len == 0 or crashes.len == 0) usage();

    var targets: std.ArrayList([]const u8) = .empty;
    while (args.next()) |target| {
        if (std.mem.eql(u8, target, "--")) break;
        if (target.len == 0 or std.mem.findNone(u8, target, "abcdefghijklmnopqrstuvwxyz0123456789_") != null) usage();
        targets.append(arena, target) catch fail("cannot read targets", error.OutOfMemory);
    }
    if (targets.items.len == 0) usage();
    var extra: std.ArrayList([]const u8) = .empty;
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--cache-dir")) usage();
        extra.append(arena, arg) catch fail("cannot read nested build arguments", error.OutOfMemory);
    }

    var buffer: [4096]u8 = undefined;
    var stdout = Io.File.stdout().writerStreaming(init.io, &buffer);
    // The verdict must not depend on how this wrapper was started. Zig colors its crash
    // line when the build runner hands a terminal's CLICOLOR_FORCE down, and ZIG_PROGRESS
    // names a pipe of the outer build, so neither reaches a nested build.
    const environ = nestedEnviron(arena, init.environ_map) catch |err| fail("cannot set up the nested environment", err);
    const ok = runAll(init.io, arena, root, known, zig, cache, crashes, seconds, targets.items, extra.items, environ, &stdout.interface) catch |err|
        fail("cannot run", err);
    if (!ok) std.process.exit(1);
}

fn runAll(
    io: Io,
    arena: Allocator,
    root_path: []const u8,
    known: []const u8,
    zig: []const u8,
    cache: []const u8,
    crashes: []const u8,
    seconds: u32,
    targets: []const []const u8,
    extra: []const []const u8,
    environ: *const std.process.Environ.Map,
    out: *Io.Writer,
) !bool {
    const cwd = Io.Dir.cwd();
    const root = cwd.openDir(io, root_path, .{}) catch |err|
        fail(try std.fmt.allocPrint(arena, "cannot open {s}", .{root_path}), err);
    defer root.close(io);
    const findings = try readFindings(io, arena, root, known);

    var stale = false;
    for (findings) |finding| {
        for (targets) |target| {
            if (std.mem.eql(u8, finding.target, target)) break;
        } else {
            try out.print("stale: {s} {s} (no such target; remove the entry)\n", .{ finding.target, finding.bead });
            stale = true;
        }
    }
    if (stale) {
        try out.flush();
        return false;
    }

    const cache_dir = if (std.fs.path.isAbsolute(cache)) cache else try std.fs.path.join(arena, &.{ try std.process.currentPathAlloc(io, arena), cache });
    const lock_path = try std.fs.path.join(arena, &.{ cache_dir, "fuzz.lock" });
    // The lock covers both warm-ups and timed runs, including the copy out of f/crash.
    const lock = takeLock(io, cache_dir, lock_path) catch |err|
        fail(try std.fmt.allocPrint(arena, "cannot lock {s}", .{lock_path}), err);
    defer lock.close(io);
    cwd.createDirPath(io, crashes) catch |err|
        fail(try std.fmt.allocPrint(arena, "cannot create {s}", .{crashes}), err);

    const run: Run = .{
        .root = root,
        .zig = zig,
        .cache_dir = cache_dir,
        .crash_dir = crashes,
        .seconds = seconds,
        .extra_args = extra,
        .environ = environ,
    };
    var ok = true;
    for (targets) |target| {
        if (!try runTarget(io, arena, run, target, out)) ok = false;
        try out.flush();
    }
    for (findings) |finding| try out.print("known: {s} {s}: {s}\n", .{ finding.target, finding.bead, finding.reason });
    try out.flush();
    return ok;
}

fn readFindings(io: Io, arena: Allocator, root: Io.Dir, path: []const u8) ![]const Finding {
    const source = root.readFileAllocOptions(io, path, arena, .limited(std.zig.max_src_size), .@"1", 0) catch |err|
        fail(try std.fmt.allocPrint(arena, "cannot read {s}", .{path}), err);
    var diagnostics: std.zon.parse.Diagnostics = undefined;
    const findings = std.zon.parse.fromSlice([]const Finding, .{
        .gpa = arena,
        .arena = arena,
        .source = source,
        .diagnostics = &diagnostics,
    }) catch |err| switch (err) {
        error.OutOfMemory => return err,
        error.ParseZon => {
            std.debug.print("fuzz: invalid known-findings file: {f}\n", .{diagnostics.fmt(path)});
            std.process.exit(1);
        },
    };
    for (findings, 0..) |finding, i| {
        if (std.mem.trim(u8, finding.reason, " \t\r\n").len == 0) {
            std.debug.print("fuzz: {s}: empty reason for {s} {s}\n", .{ path, finding.target, finding.bead });
            std.process.exit(1);
        }
        for (findings[0..i]) |prior| {
            if (!std.mem.eql(u8, finding.target, prior.target) or !std.mem.eql(u8, finding.bead, prior.bead)) continue;
            std.debug.print("fuzz: {s}: duplicate entry for {s} {s}\n", .{ path, finding.target, finding.bead });
            std.process.exit(1);
        }
    }
    return findings;
}

fn takeLock(io: Io, cache_dir: []const u8, lock_path: []const u8) !Io.File {
    const cwd = Io.Dir.cwd();
    try cwd.createDirPath(io, cache_dir);
    const file = try cwd.createFile(io, lock_path, .{ .truncate = false });
    errdefer file.close(io);
    if (!try file.tryLock(io, .exclusive)) {
        std.debug.print("fuzz: waiting for another fuzz run to release {s}\n", .{lock_path});
        try file.lock(io, .exclusive);
    }
    return file;
}

fn runTarget(io: Io, arena: Allocator, run: Run, target: []const u8, out: *Io.Writer) !bool {
    const args = try arena.alloc([]const u8, 7 + run.extra_args.len);
    const filter = try std.fmt.allocPrint(arena, "-Dfuzz-target={s}", .{target});
    args[0..7].* = .{ run.zig, "build", "fuzz", filter, "--fuzz=1", "--cache-dir", run.cache_dir };
    @memcpy(args[7..], run.extra_args);

    for ([_]bool{ false, true }) |timed| {
        if (timed) args[4] = "--fuzz=1000000G";
        const result = runBuild(io, arena, run, target, args, timed) catch |err| {
            try out.print("{s}: cannot run ({t})\n", .{ target, err });
            return false;
        };
        const plain: [2][]const u8 = .{ try stripEscapes(arena, result.output[0]), try stripEscapes(arena, result.output[1]) };
        if (findCrash(plain[1]) orelse findCrash(plain[0])) |crash| {
            const saved = saveCrash(io, arena, run, target, crash.path) catch |err| failed: {
                std.debug.print("fuzz: {s}: cannot copy {s} into {s}: {t}\n", .{ target, crash.path, run.crash_dir, err });
                break :failed crash.path;
            };
            try out.print("{s}: crash ({s}); input saved to {s}\n", .{ target, crash.how, saved });
            try out.print("{s}: to keep it as a regression test, copy it into src/fuzz/{s}/ and run zig build test\n", .{ target, target });
            showOutput(io, target, result.output);
            return false;
        }
        if (result.problem) |why| {
            try out.print("{s}: cannot run ({s})\n", .{ target, why });
            showOutput(io, target, result.output);
            return false;
        }
    }
    try out.print("{s}: no crash in {d} s\n", .{ target, run.seconds });
    return true;
}

const BuildResult = struct {
    output: [2][]const u8,
    problem: ?[]const u8,
};

fn runBuild(io: Io, arena: Allocator, run: Run, target: []const u8, args: []const []const u8, timed: bool) !BuildResult {
    // The write end belongs to this wrapper alone. Closing it, including on SIGKILL,
    // lets the watchdog kill every member of the group.
    var watchdog = std.process.spawn(io, .{
        .argv = &.{ "/bin/sh", "-c", "read line; kill -s KILL 0" },
        .pgid = 0,
        .stdin = .pipe,
        .stdout = .ignore,
        .stderr = .ignore,
    }) catch |err| {
        std.debug.print("fuzz: {s}: cannot start the watchdog: {t}\n", .{ target, err });
        return err;
    };
    defer watchdog.kill(io);
    const group = watchdog.id.?;
    var child = std.process.spawn(io, .{
        .argv = args,
        .cwd = .{ .dir = run.root },
        .environ_map = run.environ,
        .pgid = group,
        .stdin = .ignore,
        .stdout = .pipe,
        .stderr = .pipe,
    }) catch |err| {
        _ = stopWatchdog(io, target, &watchdog) catch {};
        std.debug.print("fuzz: {s}: cannot start {s}: {t}\n", .{ target, run.zig, err });
        return err;
    };
    defer child.kill(io);

    const timeout: Io.Timeout = if (timed)
        (Io.Timeout{ .duration = .{ .raw = .fromSeconds(run.seconds), .clock = .awake } }).toDeadline(io)
    else
        .none;
    if (timed) {
        std.debug.print("fuzz: {s}: fuzzing for {d} s in process group {d}\n", .{ target, run.seconds, group });
    } else {
        std.debug.print("fuzz: {s}: building and replaying the corpus in process group {d}\n", .{ target, group });
    }

    var buffer: Io.File.MultiReader.Buffer(2) = undefined;
    var reader: Io.File.MultiReader = undefined;
    reader.init(arena, io, buffer.toStreams(), &.{ child.stdout.?, child.stderr.? });
    defer reader.deinit();

    const read_result = readOutput(io, &reader, timeout);
    const watchdog_result = stopWatchdog(io, target, &watchdog);
    // Keep the nested build unreaped until after the fallback signal, so the group
    // cannot have been replaced. Darwin can return EPERM for an all-dead group.
    const stop_result = std.posix.kill(-group, .KILL);
    if (read_result == error.Timeout) {
        // A crash line could already be in the pipes when the deadline fired.
        reader.fillRemaining(.none) catch {};
    }
    const problem: ?[]const u8 = problem: {
        stop_result catch |err| switch (err) {
            error.ProcessNotFound, error.PermissionDenied => {},
            else => break :problem try std.fmt.allocPrint(arena, "cannot stop process group {d}: {t}", .{ group, err }),
        };
        const term = child.wait(io) catch |err|
            break :problem try std.fmt.allocPrint(arena, "cannot wait for nested build: {t}", .{err});
        watchdog_result catch |err| break :problem @errorName(err);
        read_result catch |err| switch (err) {
            error.Timeout => break :problem null,
            else => break :problem try std.fmt.allocPrint(arena, "cannot read nested build output: {t}", .{err}),
        };
        if (timed) break :problem try std.fmt.allocPrint(arena, "nested build ended before the deadline: {f}", .{term});
        if (!term.success()) break :problem try std.fmt.allocPrint(arena, "warm-up {f}", .{term});
        break :problem null;
    };
    return .{ .output = .{ try reader.toOwnedSlice(0), try reader.toOwnedSlice(1) }, .problem = problem };
}

fn readOutput(io: Io, reader: *Io.File.MultiReader, timeout: Io.Timeout) !void {
    while (reader.fill(4096, timeout)) |_| {
        // A pipe that is always readable can keep poll from returning Timeout.
        if (timeout == .deadline and timeout.deadline.durationFromNow(io).raw.nanoseconds <= 0)
            return error.Timeout;
    } else |err| switch (err) {
        error.EndOfStream => try reader.checkAnyError(),
        else => |e| return e,
    }
}

fn stopWatchdog(io: Io, target: []const u8, watchdog: *Child) !void {
    const group = watchdog.id.?;
    watchdog.stdin.?.close(io);
    watchdog.stdin = null;
    const term = watchdog.wait(io) catch |err| {
        std.debug.print("fuzz: {s}: the watchdog did not stop process group {d} ({t})\n", .{ target, group, err });
        return err;
    };
    if (term == .signal and term.signal == .KILL) return;
    std.debug.print("fuzz: {s}: the watchdog did not stop process group {d} ({f})\n", .{ target, group, term });
    return error.WatchdogFailed;
}

fn nestedEnviron(arena: Allocator, parent: *const std.process.Environ.Map) !*const std.process.Environ.Map {
    const map = try arena.create(std.process.Environ.Map);
    map.* = try parent.clone(arena);
    _ = map.swapRemove("CLICOLOR_FORCE");
    _ = map.swapRemove("ZIG_PROGRESS");
    try map.put("NO_COLOR", "1");
    return map;
}

/// Drops ANSI escape sequences (ESC '[' parameters, final byte), so a styled report still parses.
fn stripEscapes(arena: Allocator, text: []const u8) ![]const u8 {
    if (std.mem.findScalar(u8, text, 0x1b) == null) return text;
    var out: std.ArrayList(u8) = try .initCapacity(arena, text.len);
    var i: usize = 0;
    while (i < text.len) {
        if (text[i] == 0x1b and i + 1 < text.len and text[i + 1] == '[') {
            i += 2;
            while (i < text.len and (text[i] < 0x40 or text[i] > 0x7e)) i += 1;
            i += 1;
            continue;
        }
        out.appendAssumeCapacity(text[i]);
        i += 1;
    }
    return out.items;
}

const Crash = struct { how: []const u8, path: []const u8 };

fn findCrash(output: []const u8) ?Crash {
    const prefix = "error: test '";
    const marker = "; input saved to '";
    var lines = std.mem.splitScalar(u8, output, '\n');
    while (lines.next()) |raw| {
        const line = std.mem.trimEnd(u8, raw, "\r");
        if (!std.mem.startsWith(u8, line, prefix) or !std.mem.endsWith(u8, line, "'")) continue;
        const name_end = std.mem.find(u8, line[prefix.len..], "' ") orelse continue;
        const tail = line[prefix.len + name_end + 2 ..];
        const separator = std.mem.find(u8, tail, marker) orelse continue;
        const path_start = separator + marker.len;
        if (separator == 0 or path_start + 1 >= tail.len) continue;
        return .{ .how = tail[0..separator], .path = tail[path_start .. tail.len - 1] };
    }
    return null;
}

fn saveCrash(io: Io, arena: Allocator, run: Run, target: []const u8, path: []const u8) ![]const u8 {
    const input = try run.root.readFileAlloc(io, path, arena, .unlimited);
    const target_dir = try std.fs.path.join(arena, &.{ run.crash_dir, target });
    const cwd = Io.Dir.cwd();
    try cwd.createDirPath(io, target_dir);
    const dest = try std.fmt.allocPrint(arena, "{s}/crash-{x:0>8}", .{ target_dir, std.hash.Crc32.hash(input) });
    try cwd.writeFile(io, .{ .sub_path = dest, .data = input });
    return dest;
}

fn showOutput(io: Io, target: []const u8, output: [2][]const u8) void {
    std.debug.print("fuzz: {s}: output of the nested build follows\n", .{target});
    for (output) |stream| Io.File.stderr().writeStreamingAll(io, stream) catch {};
}

fn fail(message: []const u8, err: anyerror) noreturn {
    std.debug.print("fuzz: {s}: {t}\n", .{ message, err });
    std.process.exit(1);
}

fn usage() noreturn {
    std.debug.print("fuzz: usage: fuzz <root dir> <known-findings file> <zig exe> <cache dir> <crash dir> <seconds> <target>... [-- <zig build argument>...]\n", .{});
    std.process.exit(2);
}

test "findCrash reads Zig's signal report with a relative path" {
    const crash = findCrash(
        "thread panic: non-empty input\nerror: test 'targets.test.fuzz crashes' terminated with signal ABRT; input saved to '.zig-cache/f/crash'\n",
    ).?;
    try std.testing.expectEqualStrings("terminated with signal ABRT", crash.how);
    try std.testing.expectEqualStrings(".zig-cache/f/crash", crash.path);
}

test "findCrash reads Zig's exit report with an absolute path" {
    const crash = findCrash("error: test 'targets.test.fuzz errors' exited with code 1; input saved to '/abs/cache/f/crash'").?;
    try std.testing.expectEqualStrings("exited with code 1", crash.how);
    try std.testing.expectEqualStrings("/abs/cache/f/crash", crash.path);
}

test "findCrash ignores missing or incomplete reports" {
    try std.testing.expect(findCrash("error: expected type 'u8', found 'i8'\n") == null);
    try std.testing.expect(findCrash("error: test 'name' exited with code 1; input saved to '") == null);
}

test "a colored crash report parses once escapes are stripped" {
    const colored = "\x1b[1m\x1b[31merror:\x1b[0m test 'targets.test.fuzz crashes' terminated with signal ABRT; input saved to '/abs/cache/f/crash'\n";
    try std.testing.expect(findCrash(colored) == null);
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const crash = findCrash(try stripEscapes(arena.allocator(), colored)).?;
    try std.testing.expectEqualStrings("terminated with signal ABRT", crash.how);
    try std.testing.expectEqualStrings("/abs/cache/f/crash", crash.path);
}

test "nested builds get no color or progress pipe from the wrapper's environment" {
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    var parent: std.process.Environ.Map = .init(arena.allocator());
    try parent.put("CLICOLOR_FORCE", "1");
    try parent.put("ZIG_PROGRESS", "3");
    try parent.put("PATH", "/bin");
    const env = try nestedEnviron(arena.allocator(), &parent);
    try std.testing.expect(env.get("CLICOLOR_FORCE") == null);
    try std.testing.expect(env.get("ZIG_PROGRESS") == null);
    try std.testing.expectEqualStrings("1", env.get("NO_COLOR").?);
    try std.testing.expectEqualStrings("/bin", env.get("PATH").?);
}
