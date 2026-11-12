//! Validates the samples against a dev server: `zig build samples-test` builds every sample's
//! worker and starter and runs this.
//!
//! For each sample it starts the worker, runs the starter, checks the starter's output against
//! the sample's sample.zon, then sends the worker SIGTERM and requires a clean exit. Samples
//! run `--jobs` at a time (default 4) against one dev server; each uses its own task queue and
//! workflow IDs, and most of a run is spent waiting on timers. Workers and
//! starters get the server's address through TEMPORAL_ADDRESS, an empty Temporal config file,
//! and none of the caller's TEMPORAL_* settings, so a developer's default profile (which may
//! point at Temporal Cloud) is never used.
//!
//! Usage: samples_runner <build-root> <work-dir> [--only <sample>] [--jobs <n>] (<sample>:<program> <exe>)...

const std = @import("std");
const temporal = @import("temporal");
const Io = std.Io;
const Allocator = std.mem.Allocator;

const Manifest = struct {
    expect: []const []const u8,
    /// Arguments for the starter, for validating with shorter parameters than the sample's
    /// defaults.
    starter_args: []const []const u8 = &.{},
    timeout_seconds: u32 = 60,
};

const Sample = struct {
    name: []const u8,
    worker: ?[]const u8 = null,
    starter: ?[]const u8 = null,
};

const Result = struct {
    name: []const u8,
    ok: bool,
    detail: []const u8,
    seconds: f64,
};

const stop_grace_seconds = 20;

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;
    const arena = init.arena.allocator();

    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.skip();
    const root_path = args.next() orelse return usage();
    const work_path = args.next() orelse return usage();
    var only: ?[]const u8 = null;
    var jobs: usize = 4;
    var samples: std.ArrayList(Sample) = .empty;
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--only")) {
            only = args.next() orelse return usage();
            continue;
        }
        if (std.mem.eql(u8, arg, "--jobs")) {
            const text = args.next() orelse return usage();
            jobs = std.fmt.parseInt(usize, text, 10) catch return usage();
            if (jobs == 0) return usage();
            continue;
        }
        const exe = args.next() orelse return usage();
        const colon = std.mem.indexOfScalar(u8, arg, ':') orelse return usage();
        const name = arg[0..colon];
        const program = arg[colon + 1 ..];
        const sample = for (samples.items) |*s| {
            if (std.mem.eql(u8, s.name, name)) break s;
        } else blk: {
            try samples.append(arena, .{ .name = name });
            break :blk &samples.items[samples.items.len - 1];
        };
        if (std.mem.eql(u8, program, "worker")) sample.worker = exe else sample.starter = exe;
    }

    const root = try Io.Dir.cwd().openDir(io, root_path, .{});
    const work = try Io.Dir.cwd().openDir(io, work_path, .{});

    var server = try temporal.testing.DevServer.start(gpa, io, .{});
    errdefer server.stop(io);

    // An empty config file: the children read only TEMPORAL_ADDRESS and TEMPORAL_NAMESPACE.
    try work.writeFile(io, .{ .sub_path = "empty-temporal-config.toml", .data = "" });
    const config_path = try std.fs.path.join(arena, &.{ work_path, "empty-temporal-config.toml" });
    var env = try init.environ_map.clone(arena);
    var to_remove: std.ArrayList([]const u8) = .empty;
    for (env.keys()) |key| if (std.mem.startsWith(u8, key, "TEMPORAL_")) try to_remove.append(arena, key);
    for (to_remove.items) |key| _ = env.swapRemove(key);
    try env.put("TEMPORAL_ADDRESS", server.address());
    try env.put("TEMPORAL_NAMESPACE", "default");
    try env.put("TEMPORAL_CONFIG_FILE", config_path);

    var selected: std.ArrayList(Sample) = .empty;
    for (samples.items) |sample| {
        if (only) |o| if (!std.mem.eql(u8, o, sample.name)) continue;
        try selected.append(arena, sample);
    }
    const results = try arena.alloc(Result, selected.items.len);
    const arenas = try arena.alloc(std.heap.ArenaAllocator, selected.items.len);
    for (arenas) |*a| a.* = .init(gpa);
    defer for (arenas) |*a| a.deinit();
    var queue: Queue = .{ .samples = selected.items, .results = results, .arenas = arenas, .root = root, .work = work, .work_path = work_path, .env = &env };
    var lanes: Io.Group = .init;
    for (0..@min(jobs, selected.items.len)) |_| lanes.concurrent(io, runLane, .{ gpa, io, &queue }) catch {
        // No thread for another lane: this one and any started before it share the queue.
        runLane(gpa, io, &queue);
        break;
    };
    try lanes.await(io);

    var buf: [4096]u8 = undefined;
    var stdout = Io.File.stdout().writer(io, &buf);
    const out = &stdout.interface;
    var failures: usize = 0;
    try out.print("\n{s:<32} {s:<6} {s:>7}  detail\n", .{ "sample", "result", "seconds" });
    for (results) |r| {
        if (!r.ok) failures += 1;
        try out.print("{s:<32} {s:<6} {d:>7.1}  {s}\n", .{ r.name, if (r.ok) "pass" else "FAIL", r.seconds, r.detail });
    }
    try out.print("\n{d} samples, {d} passed, {d} failed\n", .{ results.len, results.len - failures, failures });
    try out.flush();
    // Stop the server before exiting: std.process.exit skips defers, and a dev server left
    // running would hold zig build's output pipe open.
    server.stop(io);
    if (failures > 0) std.process.exit(1);
}

/// The samples left to validate, shared by the lanes that validate them concurrently.
const Queue = struct {
    samples: []const Sample,
    /// Indexed like `samples`; each lane writes only the results of the samples it took.
    results: []Result,
    /// Indexed like `samples`: each sample's memory, results included. One per sample because
    /// `std.heap.ArenaAllocator` is not thread-safe.
    arenas: []std.heap.ArenaAllocator,
    next: std.atomic.Value(usize) = .init(0),
    root: Io.Dir,
    work: Io.Dir,
    work_path: []const u8,
    env: *const std.process.Environ.Map,
};

/// Validates samples from `queue` until none are left.
fn runLane(gpa: Allocator, io: Io, queue: *Queue) void {
    while (true) {
        const i = queue.next.fetchAdd(1, .monotonic);
        if (i >= queue.samples.len) return;
        const sample = queue.samples[i];
        const arena = queue.arenas[i].allocator();
        const start = Io.Timestamp.now(io, .awake);
        const outcome = validate(gpa, io, arena, queue.root, queue.work, queue.work_path, queue.env, sample);
        const seconds = @as(f64, @floatFromInt(start.durationTo(Io.Timestamp.now(io, .awake)).nanoseconds)) / std.time.ns_per_s;
        queue.results[i] = .{ .name = sample.name, .ok = outcome == null, .detail = outcome orelse "", .seconds = seconds };
    }
}

fn usage() noreturn {
    std.log.err("usage: samples_runner <build-root> <work-dir> [--only <sample>] [--jobs <n>] (<sample>:<program> <exe>)...", .{});
    std.process.exit(2);
}

/// Runs one sample. Returns null on success or a short reason on failure (details are logged).
fn validate(gpa: Allocator, io: Io, arena: Allocator, root: Io.Dir, work: Io.Dir, work_path: []const u8, env: *const std.process.Environ.Map, sample: Sample) ?[]const u8 {
    const worker_exe = sample.worker orelse return "no worker program";
    const starter_exe = sample.starter orelse return "no starter program";

    const manifest_path = std.fmt.allocPrint(arena, "samples/{s}/sample.zon", .{sample.name}) catch return "out of memory";
    const manifest_src = root.readFileAllocOptions(io, manifest_path, arena, .limited(64 * 1024), .of(u8), 0) catch return "missing sample.zon";
    var diag: std.zon.parse.Diagnostics = undefined;
    const manifest = std.zon.parse.fromSlice(Manifest, .{ .gpa = gpa, .arena = arena, .source = manifest_src, .diagnostics = &diag }) catch {
        diag.log(manifest_path);
        return "invalid sample.zon";
    };

    const log_name = std.fmt.allocPrint(arena, "{s}-worker.log", .{sample.name}) catch return "out of memory";
    const log_file = work.createFile(io, log_name, .{}) catch return "cannot create worker log";
    defer log_file.close(io);

    const worker = std.process.spawn(io, .{
        .argv = &.{worker_exe},
        .environ_map = env,
        .stdin = .ignore,
        .stdout = .{ .file = log_file },
        .stderr = .{ .file = log_file },
    }) catch return "worker failed to start";
    const worker_pid = worker.id.?;
    var worker_reaped = false;
    defer if (!worker_reaped) {
        std.posix.kill(worker_pid, .KILL) catch {};
        _ = waitExit(io, worker_pid, 5);
    };

    var argv: std.ArrayList([]const u8) = .empty;
    argv.append(arena, starter_exe) catch return "out of memory";
    argv.appendSlice(arena, manifest.starter_args) catch return "out of memory";
    const starter = std.process.run(gpa, io, .{
        .argv = argv.items,
        .environ_map = env,
        .timeout = .{ .duration = .{ .raw = .fromSeconds(manifest.timeout_seconds), .clock = .awake } },
    }) catch |err| {
        std.log.err("{s}: starter did not finish: {t}", .{ sample.name, err });
        showLog(io, arena, work, work_path, log_name);
        return "starter did not finish in time";
    };
    defer gpa.free(starter.stdout);
    defer gpa.free(starter.stderr);

    const output = std.mem.concat(arena, u8, &.{ starter.stdout, starter.stderr }) catch return "out of memory";
    const starter_ok = switch (starter.term) {
        .exited => |code| code == 0,
        else => false,
    };
    var missing: ?[]const u8 = null;
    for (manifest.expect) |line| if (std.mem.indexOf(u8, output, line) == null) {
        missing = line;
        break;
    };

    // Stop the worker the way an operator would, and require a clean exit.
    std.posix.kill(worker_pid, .TERM) catch {};
    const worker_code = waitExit(io, worker_pid, stop_grace_seconds);
    worker_reaped = worker_code != null;
    if (worker_code == null) dumpStacks(gpa, io, worker_pid);

    if (!starter_ok or missing != null or worker_code == null or worker_code.? != 0) {
        std.log.err("{s}: starter output:\n{s}", .{ sample.name, output });
        showLog(io, arena, work, work_path, log_name);
    }
    if (!starter_ok) return "starter exited with an error";
    if (missing) |line| return std.fmt.allocPrint(arena, "starter output lacks \"{s}\"", .{line}) catch "starter output lacks an expected line";
    if (worker_code == null) return "worker did not stop within 20s of SIGTERM";
    if (worker_code.? != 0) return std.fmt.allocPrint(arena, "worker exited with status {d} after SIGTERM", .{worker_code.?}) catch "worker exited with an error";
    return null;
}

/// Waits up to `seconds` for `pid` to exit. Returns its exit status (128 + signal number if
/// it was killed by a signal), or null if it is still running.
fn waitExit(io: Io, pid: std.posix.pid_t, seconds: u32) ?u8 {
    var waited_ms: u32 = 0;
    while (waited_ms <= seconds * 1000) : (waited_ms += 100) {
        var status: c_int = 0;
        const r = std.c.waitpid(pid, &status, 1); // WNOHANG
        if (r == pid) {
            const s: u32 = @bitCast(status);
            if (s & 0x7f == 0) return @intCast((s >> 8) & 0xff);
            return @intCast(128 + (s & 0x7f));
        }
        if (r < 0) return 255;
        io.sleep(.fromMilliseconds(100), .awake) catch return null;
    }
    return null;
}

fn showLog(io: Io, arena: Allocator, work: Io.Dir, work_path: []const u8, name: []const u8) void {
    const contents = work.readFileAlloc(io, name, arena, .limited(1024 * 1024)) catch return;
    const tail = contents[contents.len -| 8000..];
    std.log.err("worker log ({s}/{s}):\n{s}", .{ work_path, name, tail });
}

/// Logs the threads' stacks of a process that is stuck, with the platform's debugger, so a hang
/// in CI can be diagnosed from its log. Best effort: does nothing if no debugger is installed.
fn dumpStacks(gpa: Allocator, io: Io, pid: std.posix.pid_t) void {
    var pid_buf: [16]u8 = undefined;
    const pid_text = std.fmt.bufPrint(&pid_buf, "{d}", .{pid}) catch return;
    const argv: []const []const u8 = switch (@import("builtin").os.tag) {
        // ptrace is usually limited to descendants, and gdb is not the worker's parent.
        .linux => &.{ "sudo", "-n", "gdb", "-p", pid_text, "-batch", "-ex", "thread apply all bt" },
        .macos => &.{ "sample", pid_text, "1" },
        else => return,
    };
    const result = std.process.run(gpa, io, .{
        .argv = argv,
        .timeout = .{ .duration = .{ .raw = .fromSeconds(60), .clock = .awake } },
    }) catch |err| {
        std.log.err("could not dump the stuck worker's stacks with {s}: {t}", .{ argv[0], err });
        return;
    };
    defer gpa.free(result.stdout);
    defer gpa.free(result.stderr);
    std.log.err("stacks of the stuck worker (pid {d}):\n{s}\n{s}", .{ pid, result.stdout[0..@min(result.stdout.len, 64 * 1024)], result.stderr[0..@min(result.stderr.len, 4096)] });
}
