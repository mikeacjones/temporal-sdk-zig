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
//! Most samples' workers and starters come from one program, samples/dispatch.zig, which runs
//! the program named in the SAMPLE_PROGRAM environment variable ("<sample>:worker"); the runner
//! sets it for every program it starts.
//!
//! A sample whose sample.zon sets `.docker_server` runs against its own Temporal server in a
//! Docker container instead (see `DockerServer`), and is reported as skipped when Docker isn't
//! available.
//!
//! The runner reads the manifests before it starts the dev server, so that the server starts
//! with every namespace the selected samples declare, and then creates the Nexus endpoints
//! they declare (see `Manifest.namespaces` and `Manifest.nexus_endpoints`).
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
    /// Arguments for the worker, for example an address to serve on that won't clash with
    /// another program on the machine.
    worker_args: []const []const u8 = &.{},
    timeout_seconds: u32 = 60,
    /// The sample's test files, the other samples it imports, and its extra programs; read by
    /// build.zig.
    tests: []const []const u8 = &.{},
    imports: []const []const u8 = &.{},
    programs: []const []const u8 = &.{},
    /// Run against a server in Docker with TLS rather than the shared dev server.
    docker_server: ?DockerServer = null,
    /// Namespaces the sample's programs use besides "default". The shared dev server starts
    /// with them (`--namespace`), so every part of the server knows them before any program
    /// starts; a namespace registered on a running server may reach its services one by one.
    /// Several samples may declare the same namespace. A sample with `docker_server` can't
    /// declare any: its server is its own, and the runner registers only "default" there.
    namespaces: []const []const u8 = &.{},
    /// Nexus endpoints the runner creates on the shared dev server before the sample's
    /// programs start. Each routes to "default" or one of the sample's own `namespaces`.
    /// Several samples may declare an endpoint of the same name only with the same target. A
    /// sample with `docker_server` can't declare any.
    nexus_endpoints: []const NexusEndpoint = &.{},
};

/// A Nexus endpoint that routes operations to the worker polling `task_queue` in `namespace`,
/// as `temporal operator nexus endpoint create --name <name> --target-namespace <namespace>
/// --target-task-queue <task_queue>` creates it.
const NexusEndpoint = struct {
    name: []const u8,
    namespace: []const u8,
    task_queue: []const u8,
};

/// A temporalio/server container for one sample: in-memory SQLite and frontend TLS with the
/// test certificates in tools/fixtures/tls, configured by tools/fixtures/server/config.yaml.
/// The worker and starter get, besides TEMPORAL_ADDRESS and TEMPORAL_NAMESPACE ("default"):
/// TEMPORAL_TLS=true, TEMPORAL_TLS_SERVER_CA_CERT_PATH, TEMPORAL_TLS_SERVER_NAME, and
/// TEMPORAL_TLS_CLIENT_CERT_PATH and TEMPORAL_TLS_CLIENT_KEY_PATH (a client certificate the
/// server trusts). With `jwt` they also get TEMPORAL_API_KEY, a token with the "default:admin"
/// permission, and SAMPLE_JWT_KEY, the path of the ES256 private key (a JWK) the server trusts,
/// for samples that mint their own tokens.
const DockerServer = struct {
    /// Refuse connections without a client certificate signed by the test CA.
    require_client_cert: bool = false,
    /// Require a JWT (`authorization: Bearer <token>`) with the permissions for each call.
    jwt: bool = false,
};

const Sample = struct {
    name: []const u8,
    worker: ?[]const u8 = null,
    starter: ?[]const u8 = null,
    manifest: ?Manifest = null,
    /// Why the runner couldn't prepare the sample (its namespaces or endpoints), which fails
    /// it without running its programs.
    setup_failure: ?[]const u8 = null,
};

const Status = enum { pass, fail, skip };

const Result = struct {
    name: []const u8,
    status: Status,
    detail: []const u8,
    seconds: f64,
};

const Outcome = union(Status) {
    pass,
    fail: []const u8,
    skip: []const u8,
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
    // Docker mounts need absolute paths.
    const root_abs = try Io.Dir.cwd().realPathFileAlloc(io, root_path, arena);
    const work_abs = try Io.Dir.cwd().realPathFileAlloc(io, work_path, arena);

    var selected: std.ArrayList(Sample) = .empty;
    for (samples.items) |sample| {
        if (only) |o| if (!std.mem.eql(u8, o, sample.name)) continue;
        try selected.append(arena, sample);
    }
    const results = try arena.alloc(Result, selected.items.len);
    const arenas = try arena.alloc(std.heap.ArenaAllocator, selected.items.len);
    for (arenas) |*a| a.* = .init(gpa);
    defer for (arenas) |*a| a.deinit();
    var needs_docker = false;
    for (selected.items, arenas) |*sample, *a| {
        sample.manifest = readManifest(gpa, io, a.allocator(), root, sample.name);
        const m = sample.manifest orelse continue;
        if (m.docker_server != null) needs_docker = true;
        sample.setup_failure = checkManifest(a.allocator(), m);
    }
    for (selected.items, arenas, 0..) |*sample, *a, i| {
        if (sample.setup_failure == null) sample.setup_failure = endpointConflict(a.allocator(), selected.items, i);
    }

    var server = try startDevServer(gpa, io, arena, selected.items);
    errdefer server.stop(io);
    createNexusEndpoints(gpa, io, &server, selected.items, arenas);

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

    const docker: Docker = if (needs_docker) prepareDocker(gpa, io, arena) else .unused;
    var queue: Queue = .{ .samples = selected.items, .results = results, .arenas = arenas, .root = root, .work = work, .work_path = work_path, .env = &env, .docker = docker, .root_abs = root_abs, .work_abs = work_abs };
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
    var counts: std.EnumArray(Status, usize) = .initFill(0);
    try out.print("\n{s:<32} {s:<6} {s:>7}  detail\n", .{ "sample", "result", "seconds" });
    for (results) |r| {
        counts.getPtr(r.status).* += 1;
        const label = switch (r.status) {
            .pass => "pass",
            .fail => "FAIL",
            .skip => "skip",
        };
        try out.print("{s:<32} {s:<6} {d:>7.1}  {s}\n", .{ r.name, label, r.seconds, r.detail });
    }
    const failures = counts.get(.fail);
    try out.print("\n{d} samples, {d} passed, {d} failed, {d} skipped\n", .{ results.len, counts.get(.pass), failures, counts.get(.skip) });
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
    docker: Docker,
    root_abs: []const u8,
    work_abs: []const u8,
};

/// Validates samples from `queue` until none are left.
fn runLane(gpa: Allocator, io: Io, queue: *Queue) void {
    while (true) {
        const i = queue.next.fetchAdd(1, .monotonic);
        if (i >= queue.samples.len) return;
        const sample = queue.samples[i];
        const arena = queue.arenas[i].allocator();
        const start = Io.Timestamp.now(io, .awake);
        const outcome = validate(gpa, io, arena, queue, sample);
        const seconds = @as(f64, @floatFromInt(start.durationTo(Io.Timestamp.now(io, .awake)).nanoseconds)) / std.time.ns_per_s;
        queue.results[i] = .{ .name = sample.name, .status = outcome, .detail = switch (outcome) {
            .pass => "",
            .fail, .skip => |detail| detail,
        }, .seconds = seconds };
    }
}

fn usage() noreturn {
    std.log.err("usage: samples_runner <build-root> <work-dir> [--only <sample>] [--jobs <n>] (<sample>:<program> <exe>)...", .{});
    std.process.exit(2);
}

/// Reads samples/<name>/sample.zon; null (with the problem logged) if it is missing or invalid.
fn readManifest(gpa: Allocator, io: Io, arena: Allocator, root: Io.Dir, name: []const u8) ?Manifest {
    const manifest_path = std.fmt.allocPrint(arena, "samples/{s}/sample.zon", .{name}) catch return null;
    const manifest_src = root.readFileAllocOptions(io, manifest_path, arena, .limited(64 * 1024), .of(u8), 0) catch |err| {
        std.log.err("{s}: cannot read {s}: {t}", .{ name, manifest_path, err });
        return null;
    };
    return parseManifest(gpa, arena, manifest_src, manifest_path);
}

/// Parses a sample.zon read from `path`; null (with the problem logged) if it is invalid.
fn parseManifest(gpa: Allocator, arena: Allocator, source: [:0]const u8, path: []const u8) ?Manifest {
    var diag: std.zon.parse.Diagnostics = undefined;
    return std.zon.parse.fromSlice(Manifest, .{ .gpa = gpa, .arena = arena, .source = source, .diagnostics = &diag }) catch {
        diag.log(path);
        return null;
    };
}

/// Why `manifest`'s namespaces or endpoints can't be satisfied, or null if they can.
fn checkManifest(arena: Allocator, manifest: Manifest) ?[]const u8 {
    if (manifest.docker_server != null and (manifest.namespaces.len > 0 or manifest.nexus_endpoints.len > 0))
        return "sample.zon sets docker_server with namespaces or nexus_endpoints, which only the shared dev server supports";
    for (manifest.namespaces) |ns| if (ns.len == 0) return "sample.zon declares an empty namespace";
    for (manifest.nexus_endpoints, 0..) |e, i| {
        if (e.name.len == 0 or e.namespace.len == 0 or e.task_queue.len == 0)
            return std.fmt.allocPrint(arena, "sample.zon declares Nexus endpoint \"{s}\" with an empty name, namespace, or task queue", .{e.name}) catch "out of memory";
        for (manifest.nexus_endpoints[0..i]) |earlier| if (std.mem.eql(u8, earlier.name, e.name))
            return std.fmt.allocPrint(arena, "sample.zon declares Nexus endpoint {s} twice", .{e.name}) catch "out of memory";
        if (!std.mem.eql(u8, e.namespace, "default") and !contains(manifest.namespaces, e.namespace))
            return std.fmt.allocPrint(arena, "sample.zon's Nexus endpoint {s} routes to namespace {s}, which is neither \"default\" nor in its namespaces", .{ e.name, e.namespace }) catch "out of memory";
    }
    return null;
}

/// Why an endpoint of `samples[i]` conflicts with another sample's endpoint of the same name
/// and another target, or null if none does.
fn endpointConflict(arena: Allocator, samples: []const Sample, i: usize) ?[]const u8 {
    const mine = samples[i].manifest orelse return null;
    for (mine.nexus_endpoints) |e| for (samples, 0..) |other, j| {
        if (j == i) continue;
        const theirs = other.manifest orelse continue;
        for (theirs.nexus_endpoints) |o| {
            if (!std.mem.eql(u8, e.name, o.name)) continue;
            if (std.mem.eql(u8, e.namespace, o.namespace) and std.mem.eql(u8, e.task_queue, o.task_queue)) continue;
            return std.fmt.allocPrint(arena, "Nexus endpoint {s} routes to namespace {s}, task queue {s} here, but to namespace {s}, task queue {s} in sample {s}", .{ e.name, e.namespace, e.task_queue, o.namespace, o.task_queue, other.name }) catch "out of memory";
        }
    };
    return null;
}

/// The namespaces besides "default" that the samples not already failed declare, each once,
/// in the order declared.
fn serverNamespaces(arena: Allocator, samples: []const Sample) error{OutOfMemory}![]const []const u8 {
    var list: std.ArrayList([]const u8) = .empty;
    for (samples) |sample| {
        if (sample.setup_failure != null) continue;
        const m = sample.manifest orelse continue;
        for (m.namespaces) |ns| {
            if (std.mem.eql(u8, ns, "default") or contains(list.items, ns)) continue;
            try list.append(arena, ns);
        }
    }
    return list.items;
}

fn contains(list: []const []const u8, item: []const u8) bool {
    for (list) |x| if (std.mem.eql(u8, x, item)) return true;
    return false;
}

/// Starts the shared dev server with the custom search attributes tests get, standalone
/// activities, and the namespaces the samples declare.
fn startDevServer(gpa: Allocator, io: Io, arena: Allocator, samples: []const Sample) !temporal.testing.DevServer {
    var args: std.ArrayList([]const u8) = .empty;
    try args.appendSlice(arena, &temporal.testing.DevServer.standalone_activity_args);
    for (try serverNamespaces(arena, samples)) |ns| try args.appendSlice(arena, &.{ "--namespace", ns });
    return temporal.testing.DevServer.start(gpa, io, .{ .search_attributes = &temporal.testing.custom_search_attributes, .extra_args = args.items });
}

/// Creates the Nexus endpoints the samples not already failed declare, and fails a sample
/// whose endpoint can't be created. An endpoint that exists already was created for another
/// sample with the same target (`endpointConflict` failed the others).
fn createNexusEndpoints(gpa: Allocator, io: Io, server: *const temporal.testing.DevServer, samples: []Sample, arenas: []std.heap.ArenaAllocator) void {
    var client: ?temporal.client.Client = null;
    defer if (client) |*c| c.close();
    for (samples, arenas) |*sample, *a| {
        if (sample.setup_failure != null) continue;
        const m = sample.manifest orelse continue;
        for (m.nexus_endpoints) |e| {
            if (client == null) client = temporal.client.Client.connect(gpa, io, server.clientOptions()) catch |err| {
                std.log.err("connecting to the dev server to create Nexus endpoints failed: {t}", .{err});
                sample.setup_failure = std.fmt.allocPrint(a.allocator(), "cannot connect to the dev server to create Nexus endpoint {s}: {t}", .{ e.name, err }) catch "out of memory";
                break;
            };
            temporal.testing.createNexusEndpoint(&client.?, e.name, .{ .namespace = e.namespace, .task_queue = e.task_queue }) catch |err| switch (err) {
                error.AlreadyExists => {},
                else => {
                    sample.setup_failure = std.fmt.allocPrint(a.allocator(), "cannot create Nexus endpoint {s} (namespace {s}, task queue {s}): {t}; the server's reason is logged", .{ e.name, e.namespace, e.task_queue, err }) catch "out of memory";
                    break;
                },
            };
        }
    }
}

/// Runs one sample, against its own Docker server if its manifest asks for one.
fn validate(gpa: Allocator, io: Io, arena: Allocator, queue: *const Queue, sample: Sample) Outcome {
    const manifest = sample.manifest orelse return .{ .fail = "missing or invalid sample.zon" };
    if (sample.setup_failure) |reason| return .{ .fail = reason };
    const options = manifest.docker_server orelse
        return failure(runPrograms(gpa, io, arena, queue.work, queue.work_path, queue.env, sample, manifest));
    switch (queue.docker) {
        .unused => unreachable,
        .ready => {},
        .unavailable => |reason| return .{ .skip = reason },
        .failed => |reason| return .{ .fail = reason },
    }
    var env = queue.env.clone(arena) catch return .{ .fail = "out of memory" };
    var detail: []const u8 = "";
    const container = startServer(gpa, io, arena, queue, sample.name, options, &env, &detail) catch return .{ .fail = detail };
    defer stopServer(gpa, io, container);
    const result = runPrograms(gpa, io, arena, queue.work, queue.work_path, &env, sample, manifest);
    if (result != null) showServerLog(gpa, io, container);
    return failure(result);
}

fn failure(reason: ?[]const u8) Outcome {
    return if (reason) |r| .{ .fail = r } else .pass;
}

/// Runs a sample's worker and starter with `base_env`. Returns null on success or a short reason
/// on failure (details are logged).
fn runPrograms(gpa: Allocator, io: Io, arena: Allocator, work: Io.Dir, work_path: []const u8, base_env: *const std.process.Environ.Map, sample: Sample, manifest: Manifest) ?[]const u8 {
    const worker_exe = sample.worker orelse return "no worker program";
    const starter_exe = sample.starter orelse return "no starter program";
    var worker_env = base_env.clone(arena) catch return "out of memory";
    worker_env.put("SAMPLE_PROGRAM", std.fmt.allocPrint(arena, "{s}:worker", .{sample.name}) catch return "out of memory") catch return "out of memory";
    var starter_env = base_env.clone(arena) catch return "out of memory";
    starter_env.put("SAMPLE_PROGRAM", std.fmt.allocPrint(arena, "{s}:starter", .{sample.name}) catch return "out of memory") catch return "out of memory";

    const log_name = std.fmt.allocPrint(arena, "{s}-worker.log", .{sample.name}) catch return "out of memory";
    const log_file = work.createFile(io, log_name, .{}) catch return "cannot create worker log";
    defer log_file.close(io);

    var worker_argv_list: std.ArrayList([]const u8) = .empty;
    worker_argv_list.append(arena, worker_exe) catch return "out of memory";
    worker_argv_list.appendSlice(arena, manifest.worker_args) catch return "out of memory";
    const worker_argv = worker_argv_list.items;
    const worker = std.process.spawn(io, .{
        .argv = worker_argv,
        .environ_map = &worker_env,
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

    // A starter that finishes quickly (one that runs its own worker) would otherwise let the
    // runner send SIGTERM before the worker has installed its handler, killing it.
    if (!waitForWorkerStart(io, arena, work, log_name, worker_pid))
        std.log.warn("{s}: the worker never logged \"Started Worker\"; running the starter anyway", .{sample.name});

    var argv: std.ArrayList([]const u8) = .empty;
    argv.append(arena, starter_exe) catch return "out of memory";
    argv.appendSlice(arena, manifest.starter_args) catch return "out of memory";
    const starter = std.process.run(gpa, io, .{
        .argv = argv.items,
        .environ_map = &starter_env,
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

/// Waits up to 30s for the worker's log to contain "Started Worker" (logged once it polls,
/// after `runUntilSignal` has installed its signal handler). Returns false on timeout or if
/// the worker exits first.
fn waitForWorkerStart(io: Io, arena: Allocator, work: Io.Dir, log_name: []const u8, pid: std.posix.pid_t) bool {
    var waited_ms: u32 = 0;
    while (waited_ms < 30_000) : (waited_ms += 50) {
        const contents = work.readFileAlloc(io, log_name, arena, .limited(1024 * 1024)) catch "";
        if (std.mem.indexOf(u8, contents, "Started Worker") != null) return true;
        // Signal 0 checks that the worker is still alive.
        std.posix.kill(pid, @fromBackingInt(@intCast(0))) catch return false;
        io.sleep(.fromMilliseconds(50), .awake) catch return false;
    }
    return false;
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

const server_image = "temporalio/server:1.32.2";
const jwt_key_id = "samples-runner";
const Ecdsa = std.crypto.sign.ecdsa.EcdsaP256Sha256;

/// Whether samples with a `docker_server` can run: checked once, before any sample runs.
const Docker = union(enum) {
    unused,
    ready,
    /// Docker isn't installed or its daemon isn't running: the samples are skipped.
    unavailable: []const u8,
    /// Docker runs but the server image can't be pulled: the samples fail.
    failed: []const u8,
};

/// Checks that the Docker daemon answers and pulls the server image, so the samples' timeouts
/// don't include the download.
fn prepareDocker(gpa: Allocator, io: Io, arena: Allocator) Docker {
    const info = std.process.run(gpa, io, .{
        .argv = &.{ "docker", "info", "--format", "{{.ServerVersion}}" },
        .timeout = .{ .duration = .{ .raw = .fromSeconds(30), .clock = .awake } },
    }) catch |err| return .{ .unavailable = switch (err) {
        error.FileNotFound => "docker is not installed",
        else => std.fmt.allocPrint(arena, "docker info failed: {t}", .{err}) catch "docker info failed",
    } };
    defer gpa.free(info.stdout);
    defer gpa.free(info.stderr);
    if (!succeeded(info.term)) return .{ .unavailable = std.fmt.allocPrint(arena, "docker is not running: {s}", .{firstLine(info.stderr)}) catch "docker is not running" };

    const pull = std.process.run(gpa, io, .{
        .argv = &.{ "docker", "pull", "--quiet", server_image },
        .timeout = .{ .duration = .{ .raw = .fromSeconds(600), .clock = .awake } },
    }) catch |err| return .{ .failed = std.fmt.allocPrint(arena, "docker pull {s} failed: {t}", .{ server_image, err }) catch "docker pull failed" };
    defer gpa.free(pull.stdout);
    defer gpa.free(pull.stderr);
    if (!succeeded(pull.term)) return .{ .failed = std.fmt.allocPrint(arena, "docker pull {s} failed: {s}", .{ server_image, firstLine(pull.stderr) }) catch "docker pull failed" };
    return .ready;
}

/// Starts a server container for `sample`, waits until its frontend serves the "default"
/// namespace, and points `env` at it (see `DockerServer`). Returns the container's name; on
/// failure, `detail` says why.
fn startServer(gpa: Allocator, io: Io, arena: Allocator, queue: *const Queue, sample: []const u8, options: DockerServer, env: *std.process.Environ.Map, detail: *[]const u8) error{ServerFailed}![]const u8 {
    const fail = struct {
        fn f(d: *[]const u8, reason: []const u8) error{ServerFailed} {
            d.* = reason;
            return error.ServerFailed;
        }
    }.f;
    const oom = "out of memory";
    const tls_dir = std.fs.path.join(arena, &.{ queue.root_abs, "tools", "fixtures", "tls" }) catch return fail(detail, oom);
    const config = std.fs.path.join(arena, &.{ queue.root_abs, "tools", "fixtures", "server", "config.yaml" }) catch return fail(detail, oom);
    const name = std.fmt.allocPrint(arena, "samples-{s}-{d}", .{ sample, std.c.getpid() }) catch return fail(detail, oom);

    const config_mount = std.fmt.allocPrint(arena, "{s}:/etc/temporal/config.yaml:ro", .{config}) catch return fail(detail, oom);
    const tls_mount = std.fmt.allocPrint(arena, "{s}:/etc/temporal/tls:ro", .{tls_dir}) catch return fail(detail, oom);
    var argv: std.ArrayList([]const u8) = .empty;
    argv.appendSlice(arena, &.{ "docker", "run", "--detach", "--rm", "--name", name }) catch return fail(detail, oom);
    // The frontend's gRPC and HTTP ports, published on random loopback ports.
    argv.appendSlice(arena, &.{ "--publish", "127.0.0.1::7233", "--publish", "127.0.0.1::7243" }) catch return fail(detail, oom);
    argv.appendSlice(arena, &.{ "--volume", config_mount, "--volume", tls_mount }) catch return fail(detail, oom);
    argv.appendSlice(arena, &.{
        "--env", "TEMPORAL_SERVER_CONFIG_FILE_PATH=/etc/temporal/config.yaml",
        "--env", "TEMPORAL_SERVICES=frontend,internal-frontend,history,matching,worker",
        "--env", if (options.require_client_cert) "FIXTURE_REQUIRE_CLIENT_CERT=true" else "FIXTURE_REQUIRE_CLIENT_CERT=false",
    }) catch return fail(detail, oom);

    var admin_token: ?[]const u8 = null;
    if (options.jwt) {
        const key = Ecdsa.KeyPair.generate(io);
        const jwks_name = std.fmt.allocPrint(arena, "{s}-jwks.json", .{sample}) catch return fail(detail, oom);
        const key_name = std.fmt.allocPrint(arena, "{s}-jwt-key.json", .{sample}) catch return fail(detail, oom);
        const public_jwk = publicJwk(arena, key) catch return fail(detail, oom);
        const jwks = std.fmt.allocPrint(arena, "{{\"keys\":[{s}]}}", .{public_jwk}) catch return fail(detail, oom);
        const d = base64Url(arena, &key.secret_key.toBytes()) catch return fail(detail, oom);
        const private_jwk = std.fmt.allocPrint(arena, "{s},\"d\":\"{s}\"}}", .{ public_jwk[0 .. public_jwk.len - 1], d }) catch return fail(detail, oom);
        queue.work.writeFile(io, .{ .sub_path = jwks_name, .data = jwks }) catch return fail(detail, "cannot write the JWKS file");
        queue.work.writeFile(io, .{ .sub_path = key_name, .data = private_jwk }) catch return fail(detail, "cannot write the JWT key file");
        const jwks_path = std.fs.path.join(arena, &.{ queue.work_abs, jwks_name }) catch return fail(detail, oom);
        argv.appendSlice(arena, &.{
            "--volume", std.fmt.allocPrint(arena, "{s}:/etc/temporal/jwks.json:ro", .{jwks_path}) catch return fail(detail, oom),
            "--env",    "FIXTURE_JWKS=file:///etc/temporal/jwks.json",
        }) catch return fail(detail, oom);
        admin_token = mintJwt(arena, io, key, "temporal-system:admin") catch return fail(detail, "cannot sign a JWT");
        const token = mintJwt(arena, io, key, "default:admin") catch return fail(detail, "cannot sign a JWT");
        env.put("TEMPORAL_API_KEY", token) catch return fail(detail, oom);
        env.put("SAMPLE_JWT_KEY", std.fs.path.join(arena, &.{ queue.work_abs, key_name }) catch return fail(detail, oom)) catch return fail(detail, oom);
    } else {
        argv.appendSlice(arena, &.{ "--env", "TEMPORAL_ALLOW_NO_AUTH=true" }) catch return fail(detail, oom);
    }
    argv.append(arena, server_image) catch return fail(detail, oom);

    // Also after a failed or timed-out `docker run`, which may have started the container.
    errdefer stopServer(gpa, io, name);
    const run = std.process.run(gpa, io, .{
        .argv = argv.items,
        .timeout = .{ .duration = .{ .raw = .fromSeconds(60), .clock = .awake } },
    }) catch |err| return fail(detail, std.fmt.allocPrint(arena, "docker run failed: {t}", .{err}) catch oom);
    defer gpa.free(run.stdout);
    defer gpa.free(run.stderr);
    if (!succeeded(run.term)) return fail(detail, std.fmt.allocPrint(arena, "docker run {s} failed: {s}", .{ server_image, firstLine(run.stderr) }) catch oom);

    const grpc_port = publishedPort(gpa, io, arena, name, "7233/tcp") orelse return fail(detail, "cannot read the server container's published gRPC port");
    const http_port = publishedPort(gpa, io, arena, name, "7243/tcp") orelse return fail(detail, "cannot read the server container's published HTTP port");
    const ca = std.fs.path.join(arena, &.{ tls_dir, "ca.pem" }) catch return fail(detail, oom);
    const client_cert = std.fs.path.join(arena, &.{ tls_dir, "client.pem" }) catch return fail(detail, oom);
    const client_key = std.fs.path.join(arena, &.{ tls_dir, "client.key" }) catch return fail(detail, oom);

    // The server starts without namespaces. Registering one also tells that the frontend is
    // up; the new namespace is usable at once. curl does it because the HTTP API needs a client
    // certificate, which std.crypto.tls.Client can't send.
    var curl: std.ArrayList([]const u8) = .empty;
    const namespaces_url = std.fmt.allocPrint(arena, "https://localhost:{s}/api/v1/namespaces", .{http_port}) catch return fail(detail, oom);
    const register = "{\"namespace\":\"default\",\"workflowExecutionRetentionPeriod\":\"86400s\"}";
    curl.appendSlice(arena, &.{ "curl", "--silent", "--show-error", "--fail-with-body", "--max-time", "5" }) catch return fail(detail, oom);
    curl.appendSlice(arena, &.{ "--cacert", ca, "--cert", client_cert, "--key", client_key }) catch return fail(detail, oom);
    curl.appendSlice(arena, &.{ "--header", "Content-Type: application/json", "--data", register, namespaces_url }) catch return fail(detail, oom);
    if (admin_token) |t| curl.appendSlice(arena, &.{ "--header", std.fmt.allocPrint(arena, "Authorization: Bearer {s}", .{t}) catch return fail(detail, oom) }) catch return fail(detail, oom);
    const deadline = Io.Timestamp.now(io, .awake).addDuration(.fromSeconds(60));
    var last_error: []const u8 = "";
    while (true) {
        const r = std.process.run(gpa, io, .{
            .argv = curl.items,
            .timeout = .{ .duration = .{ .raw = .fromSeconds(10), .clock = .awake } },
        }) catch |err| return fail(detail, std.fmt.allocPrint(arena, "registering the default namespace with curl failed: {t}", .{err}) catch oom);
        defer gpa.free(r.stdout);
        defer gpa.free(r.stderr);
        if (succeeded(r.term)) break;
        // curl's error, then the server's response body.
        last_error = std.fmt.allocPrint(arena, "{s} {s}", .{ firstLine(r.stderr), firstLine(r.stdout) }) catch return fail(detail, oom);
        if (Io.Timestamp.now(io, .awake).nanoseconds > deadline.nanoseconds) {
            showServerLog(gpa, io, name);
            return fail(detail, std.fmt.allocPrint(arena, "the server container did not register the default namespace within 60s: {s}", .{last_error}) catch oom);
        }
        io.sleep(.fromMilliseconds(250), .awake) catch return fail(detail, "canceled");
    }

    const put = struct {
        fn f(e: *std.process.Environ.Map, key: []const u8, value: []const u8, d: *[]const u8) error{ServerFailed}!void {
            e.put(key, value) catch return fail(d, oom);
        }
    }.f;
    try put(env, "TEMPORAL_ADDRESS", std.fmt.allocPrint(arena, "127.0.0.1:{s}", .{grpc_port}) catch return fail(detail, oom), detail);
    try put(env, "TEMPORAL_NAMESPACE", "default", detail);
    try put(env, "TEMPORAL_TLS", "true", detail);
    try put(env, "TEMPORAL_TLS_SERVER_CA_CERT_PATH", ca, detail);
    try put(env, "TEMPORAL_TLS_SERVER_NAME", "localhost", detail);
    try put(env, "TEMPORAL_TLS_CLIENT_CERT_PATH", client_cert, detail);
    try put(env, "TEMPORAL_TLS_CLIENT_KEY_PATH", client_key, detail);
    return name;
}

fn stopServer(gpa: Allocator, io: Io, name: []const u8) void {
    const r = std.process.run(gpa, io, .{
        .argv = &.{ "docker", "rm", "--force", name },
        .timeout = .{ .duration = .{ .raw = .fromSeconds(60), .clock = .awake } },
    }) catch |err| {
        std.log.err("could not remove the server container {s}: {t}", .{ name, err });
        return;
    };
    gpa.free(r.stdout);
    gpa.free(r.stderr);
}

fn showServerLog(gpa: Allocator, io: Io, name: []const u8) void {
    const r = std.process.run(gpa, io, .{
        .argv = &.{ "docker", "logs", "--tail", "40", name },
        .timeout = .{ .duration = .{ .raw = .fromSeconds(30), .clock = .awake } },
    }) catch return;
    defer gpa.free(r.stdout);
    defer gpa.free(r.stderr);
    std.log.err("server container {s} log:\n{s}{s}", .{ name, r.stdout, r.stderr });
}

/// The loopback port Docker published for the container's `port` ("7233/tcp").
fn publishedPort(gpa: Allocator, io: Io, arena: Allocator, name: []const u8, port: []const u8) ?[]const u8 {
    const r = std.process.run(gpa, io, .{
        .argv = &.{ "docker", "port", name, port },
        .timeout = .{ .duration = .{ .raw = .fromSeconds(30), .clock = .awake } },
    }) catch return null;
    defer gpa.free(r.stdout);
    defer gpa.free(r.stderr);
    if (!succeeded(r.term)) return null;
    // "127.0.0.1:50685"
    const line = firstLine(r.stdout);
    const colon = std.mem.lastIndexOfScalar(u8, line, ':') orelse return null;
    _ = std.fmt.parseInt(u16, line[colon + 1 ..], 10) catch return null;
    return arena.dupe(u8, line[colon + 1 ..]) catch null;
}

/// The public half of `key` as a JWK object.
fn publicJwk(arena: Allocator, key: Ecdsa.KeyPair) error{OutOfMemory}![]const u8 {
    const point = key.public_key.toUncompressedSec1();
    return std.fmt.allocPrint(arena, "{{\"kty\":\"EC\",\"crv\":\"P-256\",\"alg\":\"ES256\",\"use\":\"sig\",\"kid\":\"{s}\",\"x\":\"{s}\",\"y\":\"{s}\"}}", .{
        jwt_key_id,
        try base64Url(arena, point[1..33]),
        try base64Url(arena, point[33..65]),
    });
}

/// An ES256 JWT for `key` granting `permission`: "<namespace>:<role>", or
/// "temporal-system:<role>" for every namespace and the cluster APIs. Valid for a day.
fn mintJwt(arena: Allocator, io: Io, key: Ecdsa.KeyPair, permission: []const u8) ![]const u8 {
    const now = Io.Clock.real.now(io).toSeconds();
    const header = try base64Url(arena, "{\"alg\":\"ES256\",\"typ\":\"JWT\",\"kid\":\"" ++ jwt_key_id ++ "\"}");
    const claims = try base64Url(arena, try std.fmt.allocPrint(arena, "{{\"sub\":\"samples-runner\",\"permissions\":[\"{s}\"],\"iat\":{d},\"exp\":{d}}}", .{ permission, now, now + std.time.s_per_day }));
    const signed = try std.fmt.allocPrint(arena, "{s}.{s}", .{ header, claims });
    const signature = try key.sign(signed, null);
    return std.fmt.allocPrint(arena, "{s}.{s}", .{ signed, try base64Url(arena, &signature.toBytes()) });
}

fn base64Url(arena: Allocator, bytes: []const u8) error{OutOfMemory}![]const u8 {
    const encoder = std.base64.url_safe_no_pad.Encoder;
    const out = try arena.alloc(u8, encoder.calcSize(bytes.len));
    return encoder.encode(out, bytes);
}

fn succeeded(term: std.process.Child.Term) bool {
    return switch (term) {
        .exited => |code| code == 0,
        else => false,
    };
}

fn firstLine(text: []const u8) []const u8 {
    const trimmed = std.mem.trim(u8, text, " \t\r\n");
    return trimmed[0 .. std.mem.indexOfScalar(u8, trimmed, '\n') orelse trimmed.len];
}

const testing = std.testing;

fn testManifest(arena: Allocator, source: [:0]const u8) !Manifest {
    return parseManifest(testing.allocator, arena, source, "sample.zon") orelse error.TestInvalidManifest;
}

test "a manifest without namespaces or endpoints declares none" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const m = try testManifest(arena.allocator(), ".{ .expect = .{\"done\"} }");
    try testing.expectEqual(0, m.namespaces.len);
    try testing.expectEqual(0, m.nexus_endpoints.len);
    try testing.expectEqual(null, checkManifest(arena.allocator(), m));
}

test "a manifest declares namespaces and Nexus endpoints" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const m = try testManifest(arena.allocator(),
        \\.{
        \\    .expect = .{"done"},
        \\    .namespaces = .{ "target", "caller" },
        \\    .nexus_endpoints = .{
        \\        .{ .name = "endpoint", .namespace = "target", .task_queue = "handlers" },
        \\        .{ .name = "local", .namespace = "default", .task_queue = "handlers" },
        \\    },
        \\}
    );
    try testing.expectEqualStrings("caller", m.namespaces[1]);
    try testing.expectEqualStrings("handlers", m.nexus_endpoints[0].task_queue);
    try testing.expectEqual(null, checkManifest(arena.allocator(), m));
}

test "a manifest whose namespaces or endpoints can't be satisfied is rejected with the reason" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const endpoint: NexusEndpoint = .{ .name = "endpoint", .namespace = "target", .task_queue = "handlers" };
    try testing.expectStringStartsWith(checkManifest(a, .{ .expect = &.{}, .docker_server = .{}, .namespaces = &.{"target"} }).?, "sample.zon sets docker_server with namespaces");
    try testing.expectStringStartsWith(checkManifest(a, .{ .expect = &.{}, .docker_server = .{}, .namespaces = &.{"target"}, .nexus_endpoints = &.{endpoint} }).?, "sample.zon sets docker_server with namespaces");
    try testing.expectEqualStrings("sample.zon declares an empty namespace", checkManifest(a, .{ .expect = &.{}, .namespaces = &.{""} }).?);
    try testing.expectEqualStrings(
        "sample.zon's Nexus endpoint endpoint routes to namespace target, which is neither \"default\" nor in its namespaces",
        checkManifest(a, .{ .expect = &.{}, .namespaces = &.{"caller"}, .nexus_endpoints = &.{endpoint} }).?,
    );
    try testing.expectEqualStrings(
        "sample.zon declares Nexus endpoint endpoint twice",
        checkManifest(a, .{ .expect = &.{}, .namespaces = &.{"target"}, .nexus_endpoints = &.{ endpoint, .{ .name = "endpoint", .namespace = "default", .task_queue = "handlers" } } }).?,
    );
    try testing.expectEqualStrings(
        "sample.zon declares Nexus endpoint \"endpoint\" with an empty name, namespace, or task queue",
        checkManifest(a, .{ .expect = &.{}, .nexus_endpoints = &.{.{ .name = "endpoint", .namespace = "default", .task_queue = "" }} }).?,
    );
}

test "samples may share an endpoint with one target, and conflict over one with two" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const endpoint: NexusEndpoint = .{ .name = "endpoint", .namespace = "target", .task_queue = "handlers" };
    const other_queue: NexusEndpoint = .{ .name = "endpoint", .namespace = "target", .task_queue = "elsewhere" };
    const samples = [_]Sample{
        .{ .name = "one", .manifest = .{ .expect = &.{}, .namespaces = &.{"target"}, .nexus_endpoints = &.{endpoint} } },
        .{ .name = "two", .manifest = .{ .expect = &.{}, .namespaces = &.{"target"}, .nexus_endpoints = &.{endpoint} } },
        .{ .name = "three", .manifest = .{ .expect = &.{}, .namespaces = &.{"target"}, .nexus_endpoints = &.{other_queue} } },
        .{ .name = "no-manifest" },
    };
    try testing.expectEqualStrings(
        "Nexus endpoint endpoint routes to namespace target, task queue handlers here, but to namespace target, task queue elsewhere in sample three",
        endpointConflict(a, &samples, 0).?,
    );
    try testing.expectEqualStrings(
        "Nexus endpoint endpoint routes to namespace target, task queue elsewhere here, but to namespace target, task queue handlers in sample one",
        endpointConflict(a, &samples, 2).?,
    );
    try testing.expectEqual(null, endpointConflict(a, samples[0..2], 0));
    try testing.expectEqual(null, endpointConflict(a, &samples, 3));
}

test "the dev server gets each declared namespace once, without default or those of failed samples" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const samples = [_]Sample{
        .{ .name = "plain", .manifest = .{ .expect = &.{} } },
        .{ .name = "one", .manifest = .{ .expect = &.{}, .namespaces = &.{ "target", "caller" } } },
        .{ .name = "two", .manifest = .{ .expect = &.{}, .namespaces = &.{ "default", "caller", "other" } } },
        .{ .name = "failed", .manifest = .{ .expect = &.{}, .namespaces = &.{"rejected"} }, .setup_failure = "rejected" },
        .{ .name = "no-manifest" },
    };
    const namespaces = try serverNamespaces(arena.allocator(), &samples);
    try testing.expectEqual(3, namespaces.len);
    try testing.expectEqualStrings("target", namespaces[0]);
    try testing.expectEqualStrings("caller", namespaces[1]);
    try testing.expectEqualStrings("other", namespaces[2]);
    try testing.expectEqual(0, (try serverNamespaces(arena.allocator(), samples[0..1])).len);
}
