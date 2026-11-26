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
//! available. A sample that sets `.dev_server` runs against a dev server of its own, at the
//! Temporal CLI version it names, started with its namespaces and given its Nexus endpoints
//! (see `OwnDevServer`).
//!
//! The runner reads the manifests before it starts the dev server, so that the server starts
//! with every namespace the selected samples declare, and then creates the Nexus endpoints
//! they declare (see `Manifest.namespaces` and `Manifest.nexus_endpoints`).
//!
//! A sample whose sample.zon declares `otlp_spans` gets an OTLP collector of its own,
//! tools/fixtures/otlp_collector.zig (passed as `--otlp-collector`), started before its worker;
//! the worker and the starter get its address in OTEL_EXPORTER_OTLP_ENDPOINT. Once both have
//! exited, the spans the collector received must include the declared ones, each under its
//! declared parent (see `checkSpans`). No OTEL_* setting of the caller reaches a sample.
//!
//! Usage: samples_runner <build-root> <work-dir> [--only <sample>] [--jobs <n>] [--otlp-collector <exe>] (<sample>:<program> <exe>)...

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
    /// Run against a dev server of the sample's own, at this Temporal CLI version, started
    /// with the sample's `namespaces` and given its `nexus_endpoints`, rather than the shared
    /// dev server. Not with `docker_server`.
    dev_server: ?OwnDevServer = null,
    /// Namespaces the sample's programs use besides "default". The shared dev server (or the
    /// sample's own, with `dev_server`) starts with them (`--namespace`), so every part of the
    /// server knows them before any program starts; a namespace registered on a running
    /// server may reach its services one by one. Several samples may declare the same
    /// namespace. A sample with `docker_server` can't declare any: its server is its own, and
    /// the runner registers only "default" there.
    namespaces: []const []const u8 = &.{},
    /// Nexus endpoints the runner creates on the shared dev server (or the sample's own, with
    /// `dev_server`) before the sample's programs start. Each routes to "default" or one of
    /// the sample's own `namespaces`. Several samples on the shared server may declare an
    /// endpoint of the same name only with the same target. A sample with `docker_server`
    /// can't declare any.
    nexus_endpoints: []const NexusEndpoint = &.{},
    /// Read by build.zig: the sample's programs and tests import `temporal_opentelemetry`
    /// (contrib/opentelemetry) and `opentelemetry-sdk`, the sample keeps programs of its own
    /// (it is not in samples/dispatch.zig), and it is built only in a root checkout.
    opentelemetry: bool = false,
    /// Spans an OTLP collector of the sample's own must have received by the time the starter
    /// and the worker have exited. Not empty: the runner starts the collector before the worker
    /// and gives both programs OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:<port>.
    otlp_spans: []const ExpectedSpan = &.{},
};

/// One span the collector must have received.
const ExpectedSpan = struct {
    name: []const u8,
    /// The name of the span that must be its parent, in the same trace. Null: a root span.
    parent: ?[]const u8 = null,
};

/// A Nexus endpoint that routes operations to the worker polling `task_queue` in `namespace`,
/// as `temporal operator nexus endpoint create --name <name> --target-namespace <namespace>
/// --target-task-queue <task_queue>` creates it.
const NexusEndpoint = struct {
    name: []const u8,
    namespace: []const u8,
    task_queue: []const u8,
};

/// A dev server for one sample, for a sample that needs a Temporal CLI version other than the
/// shared server's. The worker and starter get its address in TEMPORAL_ADDRESS and
/// TEMPORAL_NAMESPACE "default", as on the shared server. The server is started when the
/// sample's turn comes and stopped once its programs have exited, however they ended.
const OwnDevServer = struct {
    /// `temporal.testing.DevServer.Options.cli_version`, such as
    /// "v1.9.3-nexus-serialization-context".
    cli_version: []const u8,
    /// More arguments for `temporal server start-dev`, after the `--namespace` ones.
    extra_args: []const []const u8 = &.{},
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
    var otlp_collector: ?[]const u8 = null;
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
        if (std.mem.eql(u8, arg, "--otlp-collector")) {
            otlp_collector = args.next() orelse return usage();
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

    const selected = selectSamples(arena, samples.items, only) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        error.UnknownSample => {
            var names: std.ArrayList(u8) = .empty;
            for (samples.items, 0..) |sample, i| {
                if (i > 0) try names.appendSlice(arena, ", ");
                try names.appendSlice(arena, sample.name);
            }
            std.log.err("--only {s}: no sample has that name; the samples are: {s}", .{ only.?, names.items });
            std.process.exit(1);
        },
    };

    const root = try Io.Dir.cwd().openDir(io, root_path, .{});
    const work = try Io.Dir.cwd().openDir(io, work_path, .{});
    // Docker mounts need absolute paths.
    const root_abs = try Io.Dir.cwd().realPathFileAlloc(io, root_path, arena);
    const work_abs = try Io.Dir.cwd().realPathFileAlloc(io, work_path, arena);

    const results = try arena.alloc(Result, selected.len);
    const arenas = try arena.alloc(std.heap.ArenaAllocator, selected.len);
    for (arenas) |*a| a.* = .init(gpa);
    defer for (arenas) |*a| a.deinit();
    var needs_docker = false;
    for (selected, arenas) |*sample, *a| {
        sample.manifest = readManifest(gpa, io, a.allocator(), root, sample.name);
        const m = sample.manifest orelse continue;
        if (m.docker_server != null) needs_docker = true;
        sample.setup_failure = checkManifest(a.allocator(), m);
        if (sample.setup_failure == null and m.otlp_spans.len > 0 and otlp_collector == null)
            sample.setup_failure = "sample.zon declares otlp_spans, but the runner has no --otlp-collector (it is built only in a root checkout)";
    }
    for (selected, arenas, 0..) |*sample, *a, i| {
        if (sample.setup_failure == null) sample.setup_failure = endpointConflict(a.allocator(), selected, i);
    }

    var server = try startDevServer(gpa, io, arena, selected);
    errdefer server.stop(io);
    createNexusEndpoints(gpa, io, &server, selected, arenas);

    // An empty config file: the children read only TEMPORAL_ADDRESS and TEMPORAL_NAMESPACE.
    try work.writeFile(io, .{ .sub_path = "empty-temporal-config.toml", .data = "" });
    const config_path = try std.fs.path.join(arena, &.{ work_path, "empty-temporal-config.toml" });
    var env = try init.environ_map.clone(arena);
    var to_remove: std.ArrayList([]const u8) = .empty;
    for (env.keys()) |key| if (std.mem.startsWith(u8, key, "TEMPORAL_") or std.mem.startsWith(u8, key, "OTEL_")) try to_remove.append(arena, key);
    for (to_remove.items) |key| _ = env.swapRemove(key);
    try env.put("TEMPORAL_ADDRESS", server.address());
    try env.put("TEMPORAL_NAMESPACE", "default");
    try env.put("TEMPORAL_CONFIG_FILE", config_path);

    const docker: Docker = if (needs_docker) prepareDocker(gpa, io, arena) else .unused;
    var queue: Queue = .{ .samples = selected, .results = results, .arenas = arenas, .root = root, .work = work, .work_path = work_path, .env = &env, .docker = docker, .root_abs = root_abs, .work_abs = work_abs, .otlp_collector = otlp_collector };
    var lanes: Io.Group = .init;
    for (0..@min(jobs, selected.len)) |_| lanes.concurrent(io, runLane, .{ gpa, io, &queue }) catch {
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
    /// The OTLP collector program, for the samples that declare `otlp_spans`.
    otlp_collector: ?[]const u8,
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

/// The samples `only` names (all of them when it is null). `only` naming no sample is an
/// error rather than an empty selection, which would run nothing and pass.
fn selectSamples(arena: Allocator, samples: []const Sample, only: ?[]const u8) error{ OutOfMemory, UnknownSample }![]Sample {
    var selected: std.ArrayList(Sample) = .empty;
    for (samples) |sample| {
        if (only) |o| if (!std.mem.eql(u8, o, sample.name)) continue;
        try selected.append(arena, sample);
    }
    if (only != null and selected.items.len == 0) return error.UnknownSample;
    return selected.toOwnedSlice(arena);
}

fn usage() noreturn {
    std.log.err("usage: samples_runner <build-root> <work-dir> [--only <sample>] [--jobs <n>] [--otlp-collector <exe>] (<sample>:<program> <exe>)...", .{});
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

/// Why `manifest`'s namespaces, endpoints or spans can't be satisfied, or null if they can.
fn checkManifest(arena: Allocator, manifest: Manifest) ?[]const u8 {
    if (manifest.docker_server != null and manifest.dev_server != null)
        return "sample.zon sets both docker_server and dev_server; a sample runs against one server of its own";
    if (manifest.docker_server != null and (manifest.namespaces.len > 0 or manifest.nexus_endpoints.len > 0))
        return "sample.zon sets docker_server with namespaces or nexus_endpoints, which only a dev server supports";
    if (manifest.dev_server) |own| if (own.cli_version.len == 0)
        return "sample.zon sets dev_server with an empty cli_version; name a Temporal CLI version such as \"default\"";
    for (manifest.namespaces) |ns| if (ns.len == 0) return "sample.zon declares an empty namespace";
    for (manifest.nexus_endpoints, 0..) |e, i| {
        if (e.name.len == 0 or e.namespace.len == 0 or e.task_queue.len == 0)
            return std.fmt.allocPrint(arena, "sample.zon declares Nexus endpoint \"{s}\" with an empty name, namespace, or task queue", .{e.name}) catch "out of memory";
        for (manifest.nexus_endpoints[0..i]) |earlier| if (std.mem.eql(u8, earlier.name, e.name))
            return std.fmt.allocPrint(arena, "sample.zon declares Nexus endpoint {s} twice", .{e.name}) catch "out of memory";
        if (!std.mem.eql(u8, e.namespace, "default") and !contains(manifest.namespaces, e.namespace))
            return std.fmt.allocPrint(arena, "sample.zon's Nexus endpoint {s} routes to namespace {s}, which is neither \"default\" nor in its namespaces", .{ e.name, e.namespace }) catch "out of memory";
    }
    for (manifest.otlp_spans, 0..) |span, i| {
        if (span.name.len == 0) return "sample.zon declares an OTLP span with an empty name";
        for (manifest.otlp_spans[0..i]) |earlier| if (std.mem.eql(u8, earlier.name, span.name))
            return std.fmt.allocPrint(arena, "sample.zon declares OTLP span \"{s}\" twice", .{span.name}) catch "out of memory";
    }
    for (manifest.otlp_spans) |span| {
        const parent = span.parent orelse continue;
        // Names are unique by now, so another span is one of that name other than this one.
        const declared = !std.mem.eql(u8, parent, span.name) and for (manifest.otlp_spans) |other| {
            if (std.mem.eql(u8, other.name, parent)) break true;
        } else false;
        if (!declared)
            return std.fmt.allocPrint(arena, "sample.zon's OTLP span \"{s}\" has the parent \"{s}\", which is not another declared span", .{ span.name, parent }) catch "out of memory";
    }
    return null;
}

/// Whether `sample` runs on the shared dev server, so its namespaces and endpoints go there.
fn onSharedServer(sample: Sample) bool {
    const m = sample.manifest orelse return false;
    return m.dev_server == null and m.docker_server == null;
}

/// Why an endpoint of `samples[i]` conflicts with another sample's endpoint of the same name
/// and another target on the shared dev server, or null if none does.
fn endpointConflict(arena: Allocator, samples: []const Sample, i: usize) ?[]const u8 {
    if (!onSharedServer(samples[i])) return null;
    const mine = samples[i].manifest.?;
    for (mine.nexus_endpoints) |e| for (samples, 0..) |other, j| {
        if (j == i or !onSharedServer(other)) continue;
        const theirs = other.manifest.?;
        for (theirs.nexus_endpoints) |o| {
            if (!std.mem.eql(u8, e.name, o.name)) continue;
            if (std.mem.eql(u8, e.namespace, o.namespace) and std.mem.eql(u8, e.task_queue, o.task_queue)) continue;
            return std.fmt.allocPrint(arena, "Nexus endpoint {s} routes to namespace {s}, task queue {s} here, but to namespace {s}, task queue {s} in sample {s}", .{ e.name, e.namespace, e.task_queue, o.namespace, o.task_queue, other.name }) catch "out of memory";
        }
    };
    return null;
}

/// The namespaces besides "default" that the samples on the shared dev server not already
/// failed declare, each once, in the order declared.
fn serverNamespaces(arena: Allocator, samples: []const Sample) error{OutOfMemory}![]const []const u8 {
    var list: std.ArrayList([]const u8) = .empty;
    for (samples) |sample| {
        if (sample.setup_failure != null or !onSharedServer(sample)) continue;
        const m = sample.manifest.?;
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
/// activities, standalone activities completing Nexus operations
/// (`DevServer.activity_callback_args`), links for signals (`DevServer.signal_link_args`), and
/// the namespaces the samples declare.
fn startDevServer(gpa: Allocator, io: Io, arena: Allocator, samples: []const Sample) !temporal.testing.DevServer {
    var args: std.ArrayList([]const u8) = .empty;
    try args.appendSlice(arena, &temporal.testing.DevServer.standalone_activity_args);
    try args.appendSlice(arena, &temporal.testing.DevServer.activity_callback_args);
    try args.appendSlice(arena, &temporal.testing.DevServer.signal_link_args);
    for (try serverNamespaces(arena, samples)) |ns| try args.appendSlice(arena, &.{ "--namespace", ns });
    return temporal.testing.DevServer.start(gpa, io, .{ .search_attributes = &temporal.testing.custom_search_attributes, .extra_args = args.items });
}

/// Creates the Nexus endpoints the samples on the shared dev server not already failed
/// declare, and fails a sample whose endpoint can't be created. An endpoint that exists
/// already was created for another sample with the same target (`endpointConflict` failed the
/// others).
fn createNexusEndpoints(gpa: Allocator, io: Io, server: *const temporal.testing.DevServer, samples: []Sample, arenas: []std.heap.ArenaAllocator) void {
    var client: ?temporal.client.Client = null;
    defer if (client) |*c| c.close();
    for (samples, arenas) |*sample, *a| {
        if (sample.setup_failure != null or !onSharedServer(sample.*)) continue;
        sample.setup_failure = createEndpoints(gpa, io, a.allocator(), server, &client, sample.manifest.?.nexus_endpoints);
    }
}

/// Creates `endpoints` on `server` through `client`, which it connects first if it is null.
/// Returns null once every endpoint exists, or why one could not be created.
fn createEndpoints(gpa: Allocator, io: Io, arena: Allocator, server: *const temporal.testing.DevServer, client: *?temporal.client.Client, endpoints: []const NexusEndpoint) ?[]const u8 {
    for (endpoints) |e| {
        if (client.* == null) client.* = temporal.client.Client.connect(gpa, io, server.clientOptions()) catch |err| {
            std.log.err("connecting to the dev server to create Nexus endpoints failed: {t}", .{err});
            return std.fmt.allocPrint(arena, "cannot connect to the dev server to create Nexus endpoint {s}: {t}", .{ e.name, err }) catch "out of memory";
        };
        temporal.testing.createNexusEndpoint(&client.*.?, e.name, .{ .namespace = e.namespace, .task_queue = e.task_queue }) catch |err| switch (err) {
            error.AlreadyExists => {},
            else => return std.fmt.allocPrint(arena, "cannot create Nexus endpoint {s} (namespace {s}, task queue {s}): {t}; the server's reason is logged", .{ e.name, e.namespace, e.task_queue, err }) catch "out of memory",
        };
    }
    return null;
}

/// Runs one sample, against its own dev server or Docker server if its manifest asks for one.
fn validate(gpa: Allocator, io: Io, arena: Allocator, queue: *const Queue, sample: Sample) Outcome {
    const manifest = sample.manifest orelse return .{ .fail = "missing or invalid sample.zon" };
    if (sample.setup_failure) |reason| return .{ .fail = reason };
    if (manifest.dev_server) |own| return failure(runOnOwnDevServer(gpa, io, arena, queue, sample, manifest, own));
    const options = manifest.docker_server orelse
        return failure(runPrograms(gpa, io, arena, queue.work, queue.work_path, queue.env, queue.otlp_collector, sample, manifest));
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
    const result = runPrograms(gpa, io, arena, queue.work, queue.work_path, &env, queue.otlp_collector, sample, manifest);
    if (result != null) showServerLog(gpa, io, container);
    return failure(result);
}

/// Starts `sample`'s own dev server as `own` and `manifest` say, creates the sample's Nexus
/// endpoints there, runs its programs against it, and stops it on every path. Returns null on
/// success or a short reason on failure, as `runPrograms` does.
fn runOnOwnDevServer(gpa: Allocator, io: Io, arena: Allocator, queue: *const Queue, sample: Sample, manifest: Manifest, own: OwnDevServer) ?[]const u8 {
    const oom = "out of memory";
    const args = ownDevServerArgs(arena, manifest, own) catch return oom;
    var server = temporal.testing.DevServer.start(gpa, io, .{ .cli_version = own.cli_version, .extra_args = args }) catch |err| {
        std.log.err("{s}: starting the sample's own dev server (Temporal CLI {s}) failed: {t}", .{ sample.name, own.cli_version, err });
        return std.fmt.allocPrint(arena, "cannot start the sample's own dev server (Temporal CLI {s}): {t}; the server's reason is logged", .{ own.cli_version, err }) catch oom;
    };
    defer server.stop(io);
    {
        var client: ?temporal.client.Client = null;
        defer if (client) |*c| c.close();
        if (createEndpoints(gpa, io, arena, &server, &client, manifest.nexus_endpoints)) |reason| return reason;
    }
    var env = queue.env.clone(arena) catch return oom;
    env.put("TEMPORAL_ADDRESS", server.address()) catch return oom;
    return runPrograms(gpa, io, arena, queue.work, queue.work_path, &env, queue.otlp_collector, sample, manifest);
}

/// The `temporal server start-dev` arguments of a sample's own dev server: a `--namespace`
/// for each of its namespaces besides "default" (which the server has already), once each,
/// then `own.extra_args`.
fn ownDevServerArgs(arena: Allocator, manifest: Manifest, own: OwnDevServer) error{OutOfMemory}![]const []const u8 {
    var args: std.ArrayList([]const u8) = .empty;
    for (manifest.namespaces, 0..) |ns, i| {
        if (std.mem.eql(u8, ns, "default") or contains(manifest.namespaces[0..i], ns)) continue;
        try args.appendSlice(arena, &.{ "--namespace", ns });
    }
    try args.appendSlice(arena, own.extra_args);
    return args.items;
}

fn failure(reason: ?[]const u8) Outcome {
    return if (reason) |r| .{ .fail = r } else .pass;
}

/// Runs a sample's worker and starter with `base_env`, and for a sample with `otlp_spans` the
/// OTLP collector `otlp_collector` before them. Returns null on success or a short reason on
/// failure (details are logged).
fn runPrograms(gpa: Allocator, io: Io, arena: Allocator, work: Io.Dir, work_path: []const u8, base_env: *const std.process.Environ.Map, otlp_collector: ?[]const u8, sample: Sample, manifest: Manifest) ?[]const u8 {
    const worker_exe = sample.worker orelse return "no worker program";
    const starter_exe = sample.starter orelse return "no starter program";
    var worker_env = base_env.clone(arena) catch return "out of memory";
    worker_env.put("SAMPLE_PROGRAM", std.fmt.allocPrint(arena, "{s}:worker", .{sample.name}) catch return "out of memory") catch return "out of memory";
    var starter_env = base_env.clone(arena) catch return "out of memory";
    starter_env.put("SAMPLE_PROGRAM", std.fmt.allocPrint(arena, "{s}:starter", .{sample.name}) catch return "out of memory") catch return "out of memory";

    const log_name = std.fmt.allocPrint(arena, "{s}-worker.log", .{sample.name}) catch return "out of memory";
    const log_file = work.createFile(io, log_name, .{}) catch return "cannot create worker log";
    defer log_file.close(io);

    // Started first and stopped last (defers run in reverse), so it is serving for as long as
    // either program can export.
    var collector: ?Collector = null;
    defer if (collector) |*c| c.stop(io);
    if (manifest.otlp_spans.len > 0) {
        const exe = otlp_collector orelse return "sample.zon declares otlp_spans, but the runner has no --otlp-collector (it is built only in a root checkout)";
        collector = Collector.start(io, arena, work, work_path, exe, sample.name) catch |err| switch (err) {
            error.OutOfMemory => return "out of memory",
            error.CannotCreateLog => return "cannot create the OTLP collector log",
            error.StartFailed => {
                std.log.err("{s}: the OTLP collector failed to start", .{sample.name});
                if (collectorLogName(arena, sample.name)) |name| showLog(io, arena, work, work_path, "OTLP collector", name);
                return "the OTLP collector failed to start";
            },
        };
        const endpoint = std.fmt.allocPrint(arena, "http://{s}", .{collector.?.address}) catch return "out of memory";
        worker_env.put("OTEL_EXPORTER_OTLP_ENDPOINT", endpoint) catch return "out of memory";
        starter_env.put("OTEL_EXPORTER_OTLP_ENDPOINT", endpoint) catch return "out of memory";
    }
    const logs: Logs = .{ .io = io, .arena = arena, .work = work, .work_path = work_path, .worker = log_name, .collector = if (collector) |c| c.log_name else null };

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
    const starter = runBounded(gpa, io, argv.items, &starter_env, .fromSeconds(manifest.timeout_seconds)) catch |err| switch (err) {
        error.SpawnFailed => {
            std.log.err("{s}: starter failed to start", .{sample.name});
            return "starter failed to start";
        },
        error.OutOfMemory => return "out of memory",
        error.ReadFailed => {
            std.log.err("{s}: reading the starter's output failed", .{sample.name});
            logs.show();
            return "reading the starter's output failed";
        },
    };
    defer gpa.free(starter.stdout);
    defer gpa.free(starter.stderr);

    const output = std.mem.concat(arena, u8, &.{ starter.stdout, starter.stderr }) catch return "out of memory";
    const starter_term = starter.term orelse {
        std.log.err("{s}: starter did not finish within {d}s; killed it", .{ sample.name, manifest.timeout_seconds });
        std.log.err("{s}: starter output:\n{s}", .{ sample.name, output });
        logs.show();
        return "starter did not finish in time";
    };
    const starter_ok = switch (starter_term) {
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

    const reason: ?[]const u8 = if (!starter_ok)
        "starter exited with an error"
    else if (missing) |line|
        std.fmt.allocPrint(arena, "starter output lacks \"{s}\"", .{line}) catch "starter output lacks an expected line"
    else if (worker_code == null)
        "worker did not stop within 20s of SIGTERM"
    else if (worker_code.? != 0)
        std.fmt.allocPrint(arena, "worker exited with status {d} after SIGTERM", .{worker_code.?}) catch "worker exited with an error"
    else if (collector) |c|
        // Both programs have exited, and each export was answered only once the collector had
        // written its spans, so the file holds every span they exported.
        checkCollectedSpans(io, arena, work, c.spans_name, manifest.otlp_spans)
    else
        null;
    if (reason) |r| {
        std.log.err("{s}: {s}", .{ sample.name, r });
        std.log.err("{s}: starter output:\n{s}", .{ sample.name, output });
        logs.show();
    }
    return reason;
}

/// The sample's logs, shown when it fails.
const Logs = struct {
    io: Io,
    arena: Allocator,
    work: Io.Dir,
    work_path: []const u8,
    worker: []const u8,
    collector: ?[]const u8,

    fn show(l: Logs) void {
        showLog(l.io, l.arena, l.work, l.work_path, "worker", l.worker);
        if (l.collector) |name| showLog(l.io, l.arena, l.work, l.work_path, "OTLP collector", name);
    }
};

fn collectorLogName(arena: Allocator, sample: []const u8) ?[]const u8 {
    return std.fmt.allocPrint(arena, "{s}-otlp-collector.log", .{sample}) catch null;
}

/// A sample's OTLP collector (tools/fixtures/otlp_collector.zig), writing the spans it
/// receives to `<sample>-spans.jsonl` and its output to `<sample>-otlp-collector.log` in the
/// work directory.
const Collector = struct {
    pid: std.posix.pid_t,
    /// "127.0.0.1:<port>", as it printed it.
    address: []const u8,
    spans_name: []const u8,
    log_name: []const u8,

    /// Starts the collector and waits up to 30 s for it to print its address. On
    /// `error.StartFailed` the collector has been stopped and reaped.
    fn start(io: Io, arena: Allocator, work: Io.Dir, work_path: []const u8, exe: []const u8, sample: []const u8) error{ OutOfMemory, CannotCreateLog, StartFailed }!Collector {
        const spans_name = try std.fmt.allocPrint(arena, "{s}-spans.jsonl", .{sample});
        const log_name = collectorLogName(arena, sample) orelse return error.OutOfMemory;
        const spans_path = try std.fs.path.join(arena, &.{ work_path, spans_name });
        const log_file = work.createFile(io, log_name, .{}) catch return error.CannotCreateLog;
        defer log_file.close(io);
        // Its stdout goes to the log too: the address it prints first is read from there.
        const child = std.process.spawn(io, .{
            .argv = &.{ exe, spans_path },
            .stdin = .ignore,
            .stdout = .{ .file = log_file },
            .stderr = .{ .file = log_file },
        }) catch return error.StartFailed;
        var c: Collector = .{ .pid = child.id.?, .address = "", .spans_name = spans_name, .log_name = log_name };
        var waited_ms: u32 = 0;
        while (waited_ms < 30_000) : (waited_ms += 50) {
            const contents = work.readFileAlloc(io, log_name, arena, .limited(64 * 1024)) catch "";
            if (printedAddress(contents)) |a| {
                c.address = a;
                return c;
            }
            var status: c_int = 0;
            const r = std.c.waitpid(c.pid, &status, 1); // WNOHANG
            if (r == c.pid or r < 0) return error.StartFailed; // Exited and reaped: not to be signaled.
            io.sleep(.fromMilliseconds(50), .awake) catch break;
        }
        c.stop(io);
        return error.StartFailed;
    }

    /// The address the collector printed as the first line of `log`, once that line is whole.
    fn printedAddress(log: []const u8) ?[]const u8 {
        const end = std.mem.indexOfScalar(u8, log, '\n') orelse return null;
        const line = log[0..end];
        const prefix = "127.0.0.1:";
        if (!std.mem.startsWith(u8, line, prefix)) return null;
        _ = std.fmt.parseInt(u16, line[prefix.len..], 10) catch return null;
        return line;
    }

    /// SIGTERM, then SIGKILL if it has not exited within 5 s; reaps it either way.
    fn stop(c: *const Collector, io: Io) void {
        std.posix.kill(c.pid, .TERM) catch {};
        if (waitExit(io, c.pid, 5) != null) return;
        std.posix.kill(c.pid, .KILL) catch {};
        _ = waitExit(io, c.pid, 5);
    }
};

/// Why the spans in the work directory's `spans_name` don't satisfy `expected`, or null.
fn checkCollectedSpans(io: Io, arena: Allocator, work: Io.Dir, spans_name: []const u8, expected: []const ExpectedSpan) ?[]const u8 {
    const jsonl = work.readFileAlloc(io, spans_name, arena, .limited(64 * 1024 * 1024)) catch |err|
        return std.fmt.allocPrint(arena, "cannot read the collected spans: {s}: {t}", .{ spans_name, err }) catch "cannot read the collected spans";
    const received = parseSpans(arena, jsonl) catch |err| return switch (err) {
        error.OutOfMemory => "out of memory",
        error.InvalidSpans => std.fmt.allocPrint(arena, "cannot read the collected spans: {s} is not one span per line", .{spans_name}) catch "cannot read the collected spans",
    };
    return checkSpans(arena, received, expected);
}

/// A span the collector received, as one line of its spans file has it.
const ReceivedSpan = struct {
    name: []const u8,
    /// 32 lower-case hex digits.
    trace_id: []const u8,
    /// 16 lower-case hex digits.
    span_id: []const u8,
    /// 16 lower-case hex digits, or empty for a root span.
    parent_span_id: []const u8,
};

/// The spans of a spans file: one JSON object per line, blank lines skipped.
fn parseSpans(arena: Allocator, jsonl: []const u8) error{ OutOfMemory, InvalidSpans }![]ReceivedSpan {
    var spans: std.ArrayList(ReceivedSpan) = .empty;
    var lines = std.mem.splitScalar(u8, jsonl, '\n');
    while (lines.next()) |line| {
        if (std.mem.trim(u8, line, " \t\r").len == 0) continue;
        const span = std.json.parseFromSliceLeaky(ReceivedSpan, arena, line, .{}) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            else => return error.InvalidSpans,
        };
        if (!isHexId(span.trace_id, 32) or !isHexId(span.span_id, 16) or (span.parent_span_id.len > 0 and !isHexId(span.parent_span_id, 16)))
            return error.InvalidSpans;
        try spans.append(arena, span);
    }
    return spans.items;
}

fn isHexId(id: []const u8, len: usize) bool {
    if (id.len != len) return false;
    for (id) |c| switch (c) {
        '0'...'9', 'a'...'f' => {},
        else => return false,
    };
    return true;
}

/// Null when every span of `expected` is satisfied by `received`, else why the first one that
/// isn't is not. A received span S satisfies an expected span E when it has E's name and either
/// E has no parent and S is a root, or some received span P has E's parent's name, S's
/// trace ID, and S's parent span ID as its span ID.
fn checkSpans(arena: Allocator, received: []const ReceivedSpan, expected: []const ExpectedSpan) ?[]const u8 {
    for (expected) |e| {
        var found = false;
        var with_parent = false;
        const satisfied = for (received) |s| {
            if (!std.mem.eql(u8, s.name, e.name)) continue;
            found = true;
            if (s.parent_span_id.len > 0) with_parent = true;
            const parent = e.parent orelse {
                if (s.parent_span_id.len == 0) break true;
                continue;
            };
            if (s.parent_span_id.len == 0) continue;
            for (received) |p| {
                if (std.mem.eql(u8, p.name, parent) and std.mem.eql(u8, p.span_id, s.parent_span_id) and std.mem.eql(u8, p.trace_id, s.trace_id)) break;
            } else continue;
            break true;
        } else false;
        if (satisfied) continue;
        const oom = "out of memory";
        if (!found) return std.fmt.allocPrint(arena, "the OTLP collector received no span \"{s}\"", .{e.name}) catch oom;
        const parent = e.parent orelse return std.fmt.allocPrint(arena, "span \"{s}\" has a parent, expected a root span", .{e.name}) catch oom;
        if (!with_parent) return std.fmt.allocPrint(arena, "span \"{s}\" has no parent, expected \"{s}\"", .{ e.name, parent }) catch oom;
        return std.fmt.allocPrint(arena, "span \"{s}\" is not a child of \"{s}\"", .{ e.name, parent }) catch oom;
    }
    return null;
}

/// What `runBounded` saw of a program it ran.
const Bounded = struct {
    /// How it ended, or null when `limit` passed and it was killed.
    term: ?std.process.Child.Term,
    /// Everything it wrote before it ended or was killed; the caller frees both with `gpa`.
    stdout: []u8,
    stderr: []u8,
};

const BoundedError = error{ OutOfMemory, SpawnFailed, ReadFailed };

/// Runs `argv`, collecting its output, for at most `limit` counted from the start, however
/// much it prints; it also limits the wait for the exit after the output closes. At the
/// deadline it sends SIGKILL and reaps the process.
///
/// `std.process.run` can't do this: it passes its timeout to every read as a fresh duration,
/// so it limits only silence, and it waits for the exit with no limit at all.
fn runBounded(gpa: Allocator, io: Io, argv: []const []const u8, environ_map: ?*const std.process.Environ.Map, limit: Io.Duration) BoundedError!Bounded {
    const deadline = (Io.Timeout{ .duration = .{ .raw = limit, .clock = .awake } }).toDeadline(io);
    const deadline_ts = deadline.deadline;

    var child = std.process.spawn(io, .{
        .argv = argv,
        .environ_map = environ_map,
        .stdin = .ignore,
        .stdout = .pipe,
        .stderr = .pipe,
    }) catch return error.SpawnFailed;
    // Runs after the reader below is torn down, so no read is pending on the pipes it closes.
    // `Child.kill` sends SIGTERM and then blocks until the process exits, so SIGKILL goes
    // first: a program that handles SIGTERM must not hang the runner. Does nothing once the
    // child has been reaped (`id` is then null), so a reaped PID is never signaled.
    defer if (child.id) |pid| {
        std.posix.kill(pid, .KILL) catch {};
        child.kill(io);
    };

    var buffer: Io.File.MultiReader.Buffer(2) = undefined;
    var reader: Io.File.MultiReader = undefined;
    reader.init(gpa, io, buffer.toStreams(), &.{ child.stdout.?, child.stderr.? });
    defer reader.deinit();

    const timed_out = while (true) {
        // A program that writes without pause could keep `fill` returning data; the deadline
        // still ends the run.
        if (pastDeadline(io, deadline_ts)) break true;
        reader.fill(1024, deadline) catch |err| switch (err) {
            error.EndOfStream => break false,
            error.Timeout => break true,
            error.Canceled, error.ConcurrencyUnavailable => return error.ReadFailed,
        };
    };

    if (timed_out) {
        // The deferred kill stops and reaps the child. Reads may still be pending into the space after the buffered bytes, so copy what is
        // buffered rather than taking the buffers.
        const stdout = try gpa.dupe(u8, reader.reader(0).buffered());
        errdefer gpa.free(stdout);
        const stderr = try gpa.dupe(u8, reader.reader(1).buffered());
        return .{ .term = null, .stdout = stdout, .stderr = stderr };
    }

    reader.checkAnyError() catch return error.ReadFailed;
    const stdout = try reader.toOwnedSlice(0);
    errdefer gpa.free(stdout);
    const stderr = try reader.toOwnedSlice(1);
    errdefer gpa.free(stderr);

    // Both pipes are closed; wait for the exit until the same deadline.
    const pid = child.id.?;
    while (true) {
        var status: c_int = 0;
        const r = std.c.waitpid(pid, &status, 1); // WNOHANG
        if (r == pid) {
            reaped(io, &child);
            return .{ .term = std.Io.Threaded.statusToTerm(@bitCast(status)), .stdout = stdout, .stderr = stderr };
        }
        if (r < 0 and std.posix.errno(r) != .INTR) {
            // Only this function reaps the child, so this is not expected; the child can't be
            // waited for, and must not be signaled.
            reaped(io, &child);
            return .{ .term = .{ .unknown = 0 }, .stdout = stdout, .stderr = stderr };
        }
        const left = deadline_ts.durationFromNow(io).raw;
        if (left.nanoseconds <= 0) break;
        const step: Io.Duration = .fromNanoseconds(@min(left.nanoseconds, Io.Duration.fromMilliseconds(50).nanoseconds));
        io.sleep(step, .awake) catch return error.ReadFailed;
    }
    // The deferred kill sends SIGKILL and reaps it.
    return .{ .term = null, .stdout = stdout, .stderr = stderr };
}

fn pastDeadline(io: Io, deadline: Io.Clock.Timestamp) bool {
    return deadline.durationFromNow(io).raw.nanoseconds <= 0;
}

/// Leaves `child`, which `waitpid` has reaped, as `Child.wait` would: pipes closed, no ID.
fn reaped(io: Io, child: *std.process.Child) void {
    inline for (.{ "stdin", "stdout", "stderr" }) |name| if (@field(child, name)) |file| {
        file.close(io);
        @field(child, name) = null;
    };
    child.id = null;
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

fn showLog(io: Io, arena: Allocator, work: Io.Dir, work_path: []const u8, what: []const u8, name: []const u8) void {
    const contents = work.readFileAlloc(io, name, arena, .limited(1024 * 1024)) catch return;
    const tail = contents[contents.len -| 8000..];
    std.log.err("{s} log ({s}/{s}):\n{s}", .{ what, work_path, name, tail });
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

test "a program whose module has no error tracing decodes the SDK's protobuf messages" {
    // This test's module stands for a user's worker or client module: it doesn't set
    // `.error_tracing`, while the SDK's module may. Decoding must still end cleanly at the
    // end of the input in every build mode, ReleaseSafe included, without the user's module
    // having to turn error tracing on.
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const sent: temporal.Failure = .{ .message = "card declined", .source = "ZigSDK" };
    var encoded: std.Io.Writer.Allocating = .init(a);
    try sent.encode(&encoded.writer, a);
    var reader: std.Io.Reader = .fixed(encoded.written());
    const received = try temporal.Failure.decode(&reader, a);
    try std.testing.expectEqualStrings("card declined", received.message);
}

fn expectKilledWithin(io: Io, started: Io.Timestamp, bound: Io.Duration) !void {
    const elapsed = started.durationTo(Io.Timestamp.now(io, .awake));
    if (elapsed.nanoseconds >= bound.nanoseconds) {
        std.debug.print("killed after {d} ms\n", .{elapsed.toMilliseconds()});
        return error.TestKilledTooLate;
    }
}

test "a starter is killed at its deadline however much it prints" {
    const io = testing.io;
    const started = Io.Timestamp.now(io, .awake);
    const result = try runBounded(testing.allocator, io, &.{ "/bin/sh", "-c", "while :; do echo tick; sleep 0.05; done" }, null, .fromSeconds(1));
    defer testing.allocator.free(result.stdout);
    defer testing.allocator.free(result.stderr);
    try expectKilledWithin(io, started, .fromSeconds(5));
    try testing.expectEqual(null, result.term);
    try testing.expect(std.mem.indexOf(u8, result.stdout, "tick") != null);
}

test "a starter that closes its output and keeps running is killed at its deadline" {
    const io = testing.io;
    const started = Io.Timestamp.now(io, .awake);
    const result = try runBounded(testing.allocator, io, &.{ "/bin/sh", "-c", "exec >&- 2>&-; exec sleep 30" }, null, .fromSeconds(1));
    defer testing.allocator.free(result.stdout);
    defer testing.allocator.free(result.stderr);
    try expectKilledWithin(io, started, .fromSeconds(5));
    try testing.expectEqual(null, result.term);
}

test "a starter that ignores SIGTERM is still killed at its deadline" {
    const io = testing.io;
    const started = Io.Timestamp.now(io, .awake);
    const result = try runBounded(testing.allocator, io, &.{ "/bin/sh", "-c", "trap '' TERM; while :; do sleep 0.05; done" }, null, .fromSeconds(1));
    defer testing.allocator.free(result.stdout);
    defer testing.allocator.free(result.stderr);
    try expectKilledWithin(io, started, .fromSeconds(5));
    try testing.expectEqual(null, result.term);
}

test "a starter that exits reports its status and output" {
    const io = testing.io;
    const result = try runBounded(testing.allocator, io, &.{ "/bin/sh", "-c", "echo out; echo err >&2; exit 3" }, null, .fromSeconds(10));
    defer testing.allocator.free(result.stdout);
    defer testing.allocator.free(result.stderr);
    try testing.expectEqual(std.process.Child.Term{ .exited = 3 }, result.term.?);
    try testing.expectEqualStrings("out\n", result.stdout);
    try testing.expectEqualStrings("err\n", result.stderr);
}

test "a program that can't be started is reported as such" {
    try testing.expectError(error.SpawnFailed, runBounded(testing.allocator, testing.io, &.{"/nonexistent/samples-runner-test"}, null, .fromSeconds(1)));
}

test "--only selects a known sample and rejects an unknown one" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const samples = [_]Sample{ .{ .name = "helloworld" }, .{ .name = "timer" }, .{ .name = "mutex" } };
    try testing.expectError(error.UnknownSample, selectSamples(a, &samples, "helloworl"));
    try testing.expectError(error.UnknownSample, selectSamples(a, &samples, ""));
    const one = try selectSamples(a, &samples, "timer");
    try testing.expectEqual(1, one.len);
    try testing.expectEqualStrings("timer", one[0].name);
    const all = try selectSamples(a, &samples, null);
    try testing.expectEqual(3, all.len);
    try testing.expectEqualStrings("mutex", all[2].name);
}

test "a manifest declares the OTLP spans its collector must receive" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const m = try testManifest(arena.allocator(),
        \\.{
        \\    .expect = .{"done"},
        \\    .opentelemetry = true,
        \\    .otlp_spans = .{
        \\        .{ .name = "StartWorkflow:Workflow" },
        \\        .{ .name = "RunWorkflow:Workflow", .parent = "StartWorkflow:Workflow" },
        \\    },
        \\}
    );
    try testing.expect(m.opentelemetry);
    try testing.expectEqual(2, m.otlp_spans.len);
    try testing.expectEqual(null, m.otlp_spans[0].parent);
    try testing.expectEqualStrings("StartWorkflow:Workflow", m.otlp_spans[1].parent.?);
    try testing.expectEqual(null, checkManifest(arena.allocator(), m));
    const plain = try testManifest(arena.allocator(), ".{ .expect = .{\"done\"} }");
    try testing.expect(!plain.opentelemetry);
    try testing.expectEqual(0, plain.otlp_spans.len);
}

test "a manifest whose OTLP spans can't be checked is rejected with the reason" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    try testing.expectEqualStrings(
        "sample.zon declares an OTLP span with an empty name",
        checkManifest(a, .{ .expect = &.{}, .otlp_spans = &.{ .{ .name = "root" }, .{ .name = "", .parent = "root" } } }).?,
    );
    try testing.expectEqualStrings(
        "sample.zon declares OTLP span \"root\" twice",
        checkManifest(a, .{ .expect = &.{}, .otlp_spans = &.{ .{ .name = "root" }, .{ .name = "child", .parent = "root" }, .{ .name = "root", .parent = "child" } } }).?,
    );
    try testing.expectEqualStrings(
        "sample.zon's OTLP span \"child\" has the parent \"missing\", which is not another declared span",
        checkManifest(a, .{ .expect = &.{}, .otlp_spans = &.{ .{ .name = "root" }, .{ .name = "child", .parent = "missing" } } }).?,
    );
    try testing.expectEqualStrings(
        "sample.zon's OTLP span \"self\" has the parent \"self\", which is not another declared span",
        checkManifest(a, .{ .expect = &.{}, .otlp_spans = &.{.{ .name = "self", .parent = "self" }} }).?,
    );
}

const trace_a = "0af7651916cd43dd8448eb211c80319c";
const trace_b = "4bf92f3577b34da6a3ce929d0e0e4736";

fn receivedSpan(name: []const u8, trace_id: []const u8, span_id: []const u8, parent_span_id: []const u8) ReceivedSpan {
    return .{ .name = name, .trace_id = trace_id, .span_id = span_id, .parent_span_id = parent_span_id };
}

const chain = [_]ExpectedSpan{
    .{ .name = "StartWorkflow:Workflow" },
    .{ .name = "RunWorkflow:Workflow", .parent = "StartWorkflow:Workflow" },
    .{ .name = "custom-span", .parent = "RunWorkflow:Workflow" },
};

test "spans that form the declared chain satisfy it, whatever else was received" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const spans = [_]ReceivedSpan{
        // Children may arrive before their parents, and other spans may be mixed in.
        receivedSpan("custom-span", trace_a, "0000000000000003", "0000000000000002"),
        receivedSpan("RunWorkflow:Workflow", trace_a, "0000000000000002", "0000000000000001"),
        receivedSpan("unrelated", trace_b, "00000000000000ff", ""),
        receivedSpan("StartWorkflow:Workflow", trace_a, "0000000000000001", ""),
    };
    try testing.expectEqual(null, checkSpans(arena.allocator(), &spans, &chain));
    // A second, broken copy of a span doesn't matter while one copy satisfies it.
    const with_stray = spans ++ [_]ReceivedSpan{receivedSpan("custom-span", trace_b, "0000000000000004", "00000000000000ff")};
    try testing.expectEqual(null, checkSpans(arena.allocator(), &with_stray, &chain));
    try testing.expectEqual(null, checkSpans(arena.allocator(), &.{}, &.{}));
}

test "a span received several times satisfies its declaration when any copy has a parent of the declared name" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    // Two workflows of the same type, one running its activity inside an application span:
    // the first copies received of StartActivity and RunWorkflow don't satisfy the chain.
    const declared = [_]ExpectedSpan{
        .{ .name = "RunWorkflow:Workflow" },
        .{ .name = "workflow-operation", .parent = "RunWorkflow:Workflow" },
        .{ .name = "StartActivity:Activity", .parent = "workflow-operation" },
    };
    const spans = [_]ReceivedSpan{
        receivedSpan("RunWorkflow:Workflow", trace_a, "0000000000000001", ""),
        receivedSpan("StartActivity:Activity", trace_a, "0000000000000002", "0000000000000001"),
        receivedSpan("RunWorkflow:Workflow", trace_b, "0000000000000011", ""),
        receivedSpan("workflow-operation", trace_b, "0000000000000012", "0000000000000011"),
        receivedSpan("StartActivity:Activity", trace_b, "0000000000000013", "0000000000000012"),
    };
    try testing.expectEqual(null, checkSpans(arena.allocator(), &spans, &declared));
    // Without the second workflow's activity, no StartActivity copy is under workflow-operation.
    try testing.expectEqualStrings(
        "span \"StartActivity:Activity\" is not a child of \"workflow-operation\"",
        checkSpans(arena.allocator(), spans[0..4], &declared).?,
    );
}

test "a missing span, a wrong parent, a root with a parent and a parent in another trace are named" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const start = receivedSpan("StartWorkflow:Workflow", trace_a, "0000000000000001", "");
    const run = receivedSpan("RunWorkflow:Workflow", trace_a, "0000000000000002", "0000000000000001");

    try testing.expectEqualStrings(
        "the OTLP collector received no span \"custom-span\"",
        checkSpans(a, &.{ start, run }, &chain).?,
    );
    try testing.expectEqualStrings(
        "span \"custom-span\" has no parent, expected \"RunWorkflow:Workflow\"",
        checkSpans(a, &.{ start, run, receivedSpan("custom-span", trace_a, "0000000000000003", "") }, &chain).?,
    );
    // Its parent is the start span, not the run span.
    try testing.expectEqualStrings(
        "span \"custom-span\" is not a child of \"RunWorkflow:Workflow\"",
        checkSpans(a, &.{ start, run, receivedSpan("custom-span", trace_a, "0000000000000003", "0000000000000001") }, &chain).?,
    );
    try testing.expectEqualStrings(
        "span \"StartWorkflow:Workflow\" has a parent, expected a root span",
        checkSpans(a, &.{receivedSpan("StartWorkflow:Workflow", trace_a, "0000000000000001", "0000000000000009")}, &chain).?,
    );
    // The parent's span ID matches, but it is in another trace.
    try testing.expectEqualStrings(
        "span \"RunWorkflow:Workflow\" is not a child of \"StartWorkflow:Workflow\"",
        checkSpans(a, &.{ start, receivedSpan("RunWorkflow:Workflow", trace_b, "0000000000000002", "0000000000000001") }, &chain).?,
    );
}

test "a spans file is read one span per line, and a line that is not a span refuses it" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const spans = try parseSpans(a,
        \\{"name":"StartWorkflow:Workflow","trace_id":"0af7651916cd43dd8448eb211c80319c","span_id":"b7ad6b7169203331","parent_span_id":""}
        \\
        \\{"name":"say \"hi\"","trace_id":"0af7651916cd43dd8448eb211c80319c","span_id":"0001020304050607","parent_span_id":"b7ad6b7169203331"}
        \\
    );
    try testing.expectEqual(2, spans.len);
    try testing.expectEqualStrings("StartWorkflow:Workflow", spans[0].name);
    try testing.expectEqualStrings("", spans[0].parent_span_id);
    try testing.expectEqualStrings("say \"hi\"", spans[1].name);
    try testing.expectEqualStrings("b7ad6b7169203331", spans[1].parent_span_id);
    try testing.expectEqual(0, (try parseSpans(a, "")).len);

    const bad = [_][]const u8{
        "not json",
        "{\"name\":\"x\",\"trace_id\":\"0af7651916cd43dd8448eb211c80319c\",\"span_id\":\"b7ad6b7169203331\"}",
        "{\"name\":\"x\",\"trace_id\":\"0af7651916cd43dd8448eb211c80319c\",\"span_id\":\"b7ad6b7169203331\",\"parent_span_id\":\"\",\"extra\":1}",
        "{\"name\":\"x\",\"trace_id\":\"0AF7651916CD43DD8448EB211C80319C\",\"span_id\":\"b7ad6b7169203331\",\"parent_span_id\":\"\"}",
        "{\"name\":\"x\",\"trace_id\":\"0af7651916cd43dd\",\"span_id\":\"b7ad6b7169203331\",\"parent_span_id\":\"\"}",
        "{\"name\":\"x\",\"trace_id\":\"0af7651916cd43dd8448eb211c80319c\",\"span_id\":\"b7ad6b71\",\"parent_span_id\":\"\"}",
        "{\"name\":\"x\",\"trace_id\":\"0af7651916cd43dd8448eb211c80319c\",\"span_id\":\"b7ad6b7169203331\",\"parent_span_id\":\"zz\"}",
        "{\"name\":\"x\",\"trace_id\":\"0af7651916cd43dd8448eb211c80319c\",\"span_id\":\"b7ad6b7169203331\",\"parent_span_id\":\"\"}\n[1]",
    };
    for (bad) |text| try testing.expectError(error.InvalidSpans, parseSpans(a, text));
}

test "the collector's address is its first whole line" {
    try testing.expectEqualStrings("127.0.0.1:4318", Collector.printedAddress("127.0.0.1:4318\notlp-collector: refused").?);
    try testing.expectEqual(null, Collector.printedAddress("127.0.0.1:43"));
    try testing.expectEqual(null, Collector.printedAddress("error: cannot listen on 127.0.0.1: AddressInUse\n"));
    try testing.expectEqual(null, Collector.printedAddress("127.0.0.1:99999\n"));
}

test "a manifest declares a dev server of its own with its CLI version and arguments" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const m = try testManifest(a,
        \\.{
        \\    .expect = .{"done"},
        \\    .dev_server = .{ .cli_version = "v1.9.3-nexus-serialization-context", .extra_args = .{ "--log-level", "warn" } },
        \\    .namespaces = .{ "caller", "handler", "caller", "default" },
        \\    .nexus_endpoints = .{.{ .name = "endpoint", .namespace = "handler", .task_queue = "handler" }},
        \\}
    );
    try testing.expectEqualStrings("v1.9.3-nexus-serialization-context", m.dev_server.?.cli_version);
    try testing.expectEqual(null, checkManifest(a, m));
    const args = try ownDevServerArgs(a, m, m.dev_server.?);
    const want = [_][]const u8{ "--namespace", "caller", "--namespace", "handler", "--log-level", "warn" };
    try testing.expectEqual(want.len, args.len);
    for (want, args) |w, got| try testing.expectEqualStrings(w, got);

    const plain = try testManifest(a, ".{ .expect = .{\"done\"}, .dev_server = .{ .cli_version = \"default\" } }");
    try testing.expectEqual(0, plain.dev_server.?.extra_args.len);
    try testing.expectEqual(0, (try ownDevServerArgs(a, plain, plain.dev_server.?)).len);
    try testing.expectEqual(null, (try testManifest(a, ".{ .expect = .{\"done\"} }")).dev_server);
}

test "a manifest whose own dev server can't be started is rejected with the reason" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    try testing.expectEqualStrings(
        "sample.zon sets both docker_server and dev_server; a sample runs against one server of its own",
        checkManifest(a, .{ .expect = &.{}, .docker_server = .{}, .dev_server = .{ .cli_version = "default" } }).?,
    );
    try testing.expectStringStartsWith(checkManifest(a, .{ .expect = &.{}, .dev_server = .{ .cli_version = "" } }).?, "sample.zon sets dev_server with an empty cli_version");
    // Its endpoints must route to its own namespaces, as on the shared server.
    try testing.expectStringStartsWith(
        checkManifest(a, .{ .expect = &.{}, .dev_server = .{ .cli_version = "default" }, .nexus_endpoints = &.{.{ .name = "endpoint", .namespace = "elsewhere", .task_queue = "handlers" }} }).?,
        "sample.zon's Nexus endpoint endpoint routes to namespace elsewhere",
    );
}

test "a sample with its own dev server adds no namespaces to the shared one and conflicts with no endpoint there" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const endpoint: NexusEndpoint = .{ .name = "endpoint", .namespace = "target", .task_queue = "handlers" };
    const elsewhere: NexusEndpoint = .{ .name = "endpoint", .namespace = "own", .task_queue = "own" };
    const samples = [_]Sample{
        .{ .name = "shared", .manifest = .{ .expect = &.{}, .namespaces = &.{"target"}, .nexus_endpoints = &.{endpoint} } },
        .{ .name = "own", .manifest = .{ .expect = &.{}, .dev_server = .{ .cli_version = "default" }, .namespaces = &.{"own"}, .nexus_endpoints = &.{elsewhere} } },
    };
    const namespaces = try serverNamespaces(a, &samples);
    try testing.expectEqual(1, namespaces.len);
    try testing.expectEqualStrings("target", namespaces[0]);
    try testing.expectEqual(null, endpointConflict(a, &samples, 0));
    try testing.expectEqual(null, endpointConflict(a, &samples, 1));
    try testing.expect(onSharedServer(samples[0]));
    try testing.expect(!onSharedServer(samples[1]));
}
