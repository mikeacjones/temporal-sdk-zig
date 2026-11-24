//! Testing workflows and activities.
//!
//! Tests run against real local servers:
//!
//! - `DevServer`: a local Temporal dev server.
//! - `WorkflowEnvironment`: a dev server, client and worker on a fresh task queue. Register
//!   workflows and activities, replace activities with test doubles, and
//!   execute workflows to their result. `WorkflowEnvironment.fromClient` makes one on a
//!   server the test already runs.
//! - `ActivityEnvironment`: runs an activity function with a test context, and cancels it or
//!   stops its "worker" on request.
//! - `WorkflowReplayer`: replays recorded histories against current workflow code to catch
//!   nondeterministic changes.

const std = @import("std");
const builtin = @import("builtin");
const bridge = @import("bridge.zig");
const proto = @import("proto.zig");
const proto_time = @import("proto_time.zig");
const client_mod = @import("client.zig");
const worker_mod = @import("worker.zig");
const workflow = @import("workflow.zig");
const activity = @import("activity.zig");
const converter = @import("converter.zig");
const search_attributes = @import("search_attributes.zig");
const history_mod = @import("history.zig");
const schedule = @import("schedule.zig");
const deployment_mod = @import("deployment.zig");
const failure_mod = @import("failure.zig");
const log_mod = @import("log.zig");
const propagation = @import("propagation.zig");
const interceptor = @import("interceptor.zig");
const metrics_mod = @import("metrics.zig");
const nexus = @import("nexus.zig");
const CancelBlockingAllocator = @import("CancelBlockingAllocator.zig");
const io_tasks = @import("io_tasks.zig");
const build_options = @import("build_options");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const log = std.log.scoped(.temporal);

// Test hook, read only when builtin.is_test. Tests that set it reset it.
/// The next N re-locks of time skipping in `WorkflowEnvironment.getResultByNameWithOptions`
/// fail with `error.RpcFailed` without sending LockTimeSkipping, leaving the server unlocked.
var test_relock_failures: std.atomic.Value(u32) = .init(0);

fn takeRelockFailure() bool {
    var n = test_relock_failures.load(.monotonic);
    while (n > 0) {
        n = test_relock_failures.cmpxchgWeak(n, n - 1, .monotonic, .monotonic) orelse return true;
    }
    return false;
}

// Test hook, read only when builtin.is_test. Tests that set it reset it.
/// When set, `logTimeLockLost` writes its line here, with a newline, instead of logging it.
var test_time_lock_log: ?*Io.Writer = null;

/// Logs that `method` failed in `getResult`, losing the environment's time lock.
fn logTimeLockLost(comptime method: []const u8, err: anyerror) void {
    const format = "WorkflowEnvironment.getResult: " ++ method ++ " failed ({t}); the time-skipping server's time lock is unknown and time may keep skipping, so the environment can't be used again";
    if (builtin.is_test) if (test_time_lock_log) |w| {
        w.print(format ++ "\n", .{err}) catch {};
        return;
    };
    log.err(format, .{err});
}

/// A local dev server for tests.
pub const DevServer = struct {
    runtime: bridge.Runtime,
    server: bridge.EphemeralServer,
    namespace: []const u8,

    ///
    pub const Options = struct {
        namespace: []const u8 = "default",
        search_attributes: []const bridge.EphemeralServer.SearchAttribute = &.{},
        /// Path to a Temporal CLI binary; empty downloads one (cached).
        existing_path: []const u8 = "",
        /// CLI version to download when `existing_path` is empty: "default" (the version
        /// sdk-core recommends), "latest", or an exact version such as "1.10.0".
        cli_version: []const u8 = "default",
        ip: []const u8 = "127.0.0.1",
        /// 0 picks a free port.
        port: u16 = 0,
        ui: bool = false,
        /// Empty keeps the database in memory.
        database_filename: []const u8 = "",
        log_level: []const u8 = "error",
        /// Extra arguments for `temporal server start-dev`, such as
        /// `standalone_activity_args`.
        extra_args: []const []const u8 = &.{},
    };

    /// `extra_args` that enable standalone activities (`Client.executeActivity`), which the
    /// dev server leaves off by default. Needs Temporal CLI 1.10.0 or later.
    pub const standalone_activity_args = [_][]const u8{
        "--dynamic-config-value", "activity.enableStandalone=true",
        "--dynamic-config-value", "history.enableChasm=true",
        "--dynamic-config-value", "history.enableTransitionHistory=true",
    };

    pub fn start(gpa: Allocator, io: Io, options: Options) !DevServer {
        var message: ?[]u8 = null;
        defer if (message) |m| gpa.free(m);
        const runtime = try bridge.Runtime.init(gpa, .{}, &message);
        errdefer runtime.deinit();
        const server = bridge.EphemeralServer.startDevServer(gpa, io, runtime, .{
            .executable = executable(options.existing_path, options.cli_version),
            .namespace = options.namespace,
            .ip = options.ip,
            .port = options.port,
            .ui = options.ui,
            .database_filename = options.database_filename,
            .search_attributes = options.search_attributes,
            .log_level = options.log_level,
            .extra_args = options.extra_args,
        }, &message) catch |err| {
            log.err("starting the dev server failed: {s}", .{message orelse @errorName(err)});
            return err;
        };
        return .{ .runtime = runtime, .server = server, .namespace = options.namespace };
    }

    /// host:port of the server.
    pub fn address(s: *const DevServer) []const u8 {
        return s.server.target;
    }

    /// Options for a client of this server.
    pub fn clientOptions(s: *const DevServer) client_mod.Options {
        return .{ .host_port = s.server.target, .namespace = s.namespace };
    }

    pub fn stop(s: *DevServer, io: Io) void {
        s.server.shutdown(io, null) catch {};
        s.runtime.deinit();
        s.* = undefined;
    }
};

/// The time-skipping test server: time advances whenever every workflow is waiting on a
/// timer, so tests of long sleeps finish quickly.
pub const TestServer = struct {
    runtime: bridge.Runtime,
    server: bridge.EphemeralServer,

    pub const Options = struct {
        /// Path to a temporal-test-server binary; empty downloads one (cached).
        existing_path: []const u8 = "",
        /// Release to download when `existing_path` is empty: "default" (the one sdk-core
        /// picks, usually the newest) or a release tag with its `v`, such as "v1.41.1".
        /// Defaults to the build's `-Dtest-server-version`, which is the newest release the
        /// SDK's own tests pass with unless the build says otherwise; a project that depends
        /// on the SDK gets sdk-core's choice back with `.@"test-server-version" = "default"`
        /// in its `b.dependency` arguments.
        version: []const u8 = build_options.test_server_version,
        /// 0 picks a free port.
        port: u16 = 0,
        extra_args: []const []const u8 = &.{},
    };

    pub fn start(gpa: Allocator, io: Io, options: Options) !TestServer {
        var message: ?[]u8 = null;
        defer if (message) |m| gpa.free(m);
        const runtime = try bridge.Runtime.init(gpa, .{}, &message);
        errdefer runtime.deinit();
        const server = bridge.EphemeralServer.startTestServer(gpa, io, runtime, .{
            .executable = executable(options.existing_path, options.version),
            .port = options.port,
            .extra_args = options.extra_args,
        }, &message) catch |err| {
            log.err("starting the time-skipping test server failed: {s}", .{message orelse @errorName(err)});
            return err;
        };
        return .{ .runtime = runtime, .server = server };
    }

    /// host:port of the server.
    pub fn address(s: *const TestServer) []const u8 {
        return s.server.target;
    }

    /// Options for a client of this server (namespace "default").
    pub fn clientOptions(s: *const TestServer) client_mod.Options {
        return .{ .host_port = s.server.target };
    }

    pub fn stop(s: *TestServer, io: Io) void {
        s.server.shutdown(io, null) catch {};
        s.runtime.deinit();
        s.* = undefined;
    }
};

fn executable(existing_path: []const u8, version: []const u8) bridge.EphemeralServer.Executable {
    if (existing_path.len > 0) return .{ .existing_path = existing_path };
    return .{ .download = .{ .version = version } };
}

/// The custom search attributes a `WorkflowEnvironment` dev server registers, one of each
/// type. The dev server has none of its own.
pub const custom_search_attributes = [_]bridge.EphemeralServer.SearchAttribute{
    .{ .name = "CustomKeywordField", .type = .Keyword },
    .{ .name = "CustomTextField", .type = .Text },
    .{ .name = "CustomIntField", .type = .Int },
    .{ .name = "CustomDoubleField", .type = .Double },
    .{ .name = "CustomBoolField", .type = .Bool },
    .{ .name = "CustomDatetimeField", .type = .Datetime },
    .{ .name = "CustomKeywordListField", .type = .KeywordList },
};

const testing_ns = @This();

/// Where `createNexusEndpoint` routes an endpoint's operations: the namespace and task queue
/// of the worker that handles them.
pub const NexusWorkerTarget = struct {
    namespace: []const u8,
    task_queue: []const u8,
};

pub const CreateNexusEndpointError = error{
    OutOfMemory,
    Canceled,
    /// An endpoint called `name` already exists on the server.
    AlreadyExists,
    /// The server refused the endpoint, for example for a name it doesn't accept, or a
    /// namespace that doesn't exist; the reason is logged.
    RpcFailed,
    InvalidResponse,
};

/// Creates a Nexus endpoint called `name` on `client`'s server (the operator service's
/// CreateNexusEndpoint), routing to `target`, for a server that has no `WorkflowEnvironment`,
/// such as a `DevServer`. `WorkflowEnvironment.createNexusEndpoint` describes the endpoint
/// and the names the server accepts. The target namespace must exist on the server (a
/// `DevServer` registers more with `extra_args` `--namespace <name>`); `client`'s own
/// namespace doesn't matter. `name` and `target` are only read during the call, which may
/// run on any thread that may use `client`. Errors:
/// - `AlreadyExists`: an endpoint called `name` exists, whatever its target.
/// - `RpcFailed`: the server refused the endpoint, for example for a name it doesn't accept,
///   an empty task queue, or a namespace it doesn't have; the server's reason is logged.
/// - `InvalidResponse`: the server's response didn't decode.
/// - `OutOfMemory`, `Canceled`.
pub fn createNexusEndpoint(client: *client_mod.Client, name: []const u8, target: NexusWorkerTarget) CreateNexusEndpointError!void {
    var scratch: std.heap.ArenaAllocator = .init(client.gpa);
    defer scratch.deinit();
    const ops = proto.temporal.api.operatorservice.v1;
    const request: ops.CreateNexusEndpointRequest = .{ .spec = .{
        .name = name,
        .target = .{ .variant = .{ .worker = .{ .namespace = target.namespace, .task_queue = target.task_queue } } },
    } };
    var status: u32 = 0;
    _ = serviceCall(client, .operator, ops.CreateNexusEndpointResponse, "CreateNexusEndpoint", request, scratch.allocator(), &status) catch |err| return switch (err) {
        error.RpcFailed => if (status == 6) error.AlreadyExists else error.RpcFailed,
        else => |e| e,
    };
}

/// Calls `method` on `service` without retries; the response's memory comes from `arena`.
/// A failed call is logged, and its gRPC status code stored in `status` if given.
fn serviceCall(client: *client_mod.Client, service: bridge.Connection.Service, comptime Response: type, comptime method: []const u8, request: anytype, arena: Allocator, status: ?*u32) error{ OutOfMemory, Canceled, RpcFailed, InvalidResponse }!Response {
    var encoded: Io.Writer.Allocating = .init(arena);
    request.encode(&encoded.writer, arena) catch return error.OutOfMemory;
    var rpc_failure: ?bridge.Connection.RpcFailure = null;
    defer if (rpc_failure) |f| f.deinit(client.gpa);
    const connection = client.currentConnection() catch |err| return switch (err) {
        error.OutOfMemory => error.OutOfMemory,
        error.Canceled => error.Canceled,
        else => error.RpcFailed,
    };
    const bytes = connection.call(client.gpa, client.io, service, method, encoded.written(), .{}, &rpc_failure) catch |err| switch (err) {
        error.RpcFailed => {
            const f = rpc_failure.?;
            if (status) |out| {
                out.* = f.status_code;
                // ALREADY_EXISTS is the caller's to report.
                if (f.status_code == 6) return error.RpcFailed;
            }
            log.err("{s} {s} failed: {s} (gRPC status {d})", .{ @tagName(service), method, f.message, f.status_code });
            return error.RpcFailed;
        },
        error.InvalidOptions => return error.RpcFailed,
        else => |e| return e,
    };
    defer client.gpa.free(bytes);
    const owned = try arena.dupe(u8, bytes);
    var reader: Io.Reader = .fixed(owned);
    return Response.decode(&reader, arena) catch |err| switch (err) {
        error.OutOfMemory => error.OutOfMemory,
        else => error.InvalidResponse,
    };
}

/// A server, client and worker for running workflows in tests.
///
/// `start` runs a dev server with `custom_search_attributes` registered; `startTimeSkipping`
/// (or `startTimeSkippingWithOptions`) runs the time-skipping test server;
/// `fromClient` uses the server of a client the test already has. On the
/// test server time is locked (it moves at wall-clock speed) except while the test waits on
/// a workflow result through `getResult` or `executeWorkflow`, or calls `sleep`. Then it
/// jumps ahead whenever every workflow is waiting on a timer, so a 2h sleep finishes at
/// once, while a test can still look at a workflow it started with `startWorkflow` in the
/// middle of a timer.
///
/// Tests drive a running workflow by its ID with `signalWorkflow`, `queryWorkflow` and
/// `updateWorkflow`, or start one together with a signal or an update with
/// `signalWithStartWorkflow` and `updateWithStartWorkflow`. These leave time locked: a
/// workflow waiting for a signal or an update would otherwise skip straight to its execution
/// timeout. Let an update handler's sleep pass with `sleep` (see `updateWorkflow`).
///
/// When `getResult` can't lock time skipping again, the environment is unusable: its calls
/// that start, drive or wait on workflows, `sleep` and `now` return `error.TimeLockLost`.
///
/// Not thread-safe: use an environment from one task at a time.
pub const WorkflowEnvironment = struct {
    gpa: Allocator,
    io: Io,
    server: Server,
    client: client_mod.Client,
    worker: worker_mod.Worker,
    task_queue: []u8,
    running: ?Io.Future(worker_mod.Worker.RunError!void) = null,
    next_id: u32 = 0,
    /// Set when `getResult` on the time-skipping server failed to unlock or to lock time
    /// skipping again. Once set, the server's time lock count is unknown (time may be
    /// skipping), so the environment can't be used for time-dependent calls: they return
    /// `error.TimeLockLost`. Never cleared.
    time_lock_lost: bool = false,

    pub const Server = union(enum) {
        dev: DevServer,
        time_skipping: TestServer,
        /// A server the environment didn't start, reached through the client given to
        /// `fromClient`. The environment never stops it.
        existing,

        /// Options for another client of a server the environment started. Unit tests use it.
        fn clientOptions(s: *const Server) client_mod.Options {
            return switch (s.*) {
                .existing => unreachable, // Its client's options aren't kept.
                inline else => |*server| server.clientOptions(),
            };
        }

        fn stop(s: *Server, io: Io) void {
            switch (s.*) {
                .existing => {},
                inline else => |*server| server.stop(io),
            }
        }
    };

    pub const Options = struct {
        /// Extra arguments for the dev server's `temporal server start-dev`, such as
        /// `DevServer.standalone_activity_args`.
        dev_server_extra_args: []const []const u8 = &.{},
        /// The client's payload codecs (`client.Options.codecs`), which its worker and the
        /// replay of each finished workflow use too. Borrowed: they must outlive the
        /// environment.
        codecs: []const converter.PayloadCodec = &.{},
        /// Encode the message and stack trace of failures the client and its worker send
        /// (`client.Options.encode_failure_attributes`).
        encode_failure_attributes: bool = false,
        /// The client's failure converter (`client.Options.failure_converter`), which its
        /// worker and the replay of each finished workflow use too. Borrowed: it must outlive
        /// the environment.
        failure_converter: ?converter.FailureConverter = null,
        /// Where the client's sdk-core logs and the worker's workflow and activity logs go
        /// (`client.Options.logger`). Borrowed: it must outlive the environment.
        logger: ?log_mod.Sink = null,
        /// The client's context propagators (`client.Options.propagators`), which its worker
        /// and replays use too. Borrowed: they must outlive the environment.
        propagators: []const propagation.ContextPropagator = &.{},
        /// Interceptors of the worker's workflows, activities and Nexus requests (the requests
        /// the worker handles and the operations a workflow calls)
        /// (`client.Options.worker_interceptors`), which replays use too. Borrowed: they must
        /// outlive the environment.
        worker_interceptors: []const interceptor.Worker = &.{},
        /// Interceptors of the client's requests (`client.Options.interceptors`). Borrowed:
        /// they must outlive the environment.
        interceptors: []const interceptor.Client = &.{},
        /// Where the client's metrics go (`client.Options.metrics`).
        metrics: ?metrics_mod.Options = null,
    };

    /// Starts the environment on a dev server. Allocated so the worker's pointer to the
    /// client stays valid. Free with `deinit`.
    pub fn start(gpa: Allocator, io: Io) !*WorkflowEnvironment {
        return create(gpa, io, .dev, .{});
    }

    /// `start` with options.
    pub fn startWithOptions(gpa: Allocator, io: Io, options: Options) !*WorkflowEnvironment {
        return create(gpa, io, .dev, options);
    }

    /// Starts the environment on the time-skipping test server. Free with `deinit`.
    pub fn startTimeSkipping(gpa: Allocator, io: Io) !*WorkflowEnvironment {
        return create(gpa, io, .time_skipping, .{});
    }

    /// `startTimeSkipping` with the client's options: codecs, the failure attribute
    /// encoding, the failure converter, a logger, propagators, worker and client interceptors, and metrics, as
    /// `startWithOptions` takes them. `dev_server_extra_args` is for the dev server only: a
    /// non-empty one returns `error.InvalidOptions` before any server starts.
    pub fn startTimeSkippingWithOptions(gpa: Allocator, io: Io, options: Options) !*WorkflowEnvironment {
        if (options.dev_server_extra_args.len > 0) {
            log.warn("WorkflowEnvironment.startTimeSkippingWithOptions: dev_server_extra_args ({d} arguments) applies only to the dev server (startWithOptions); the time-skipping test server takes none", .{options.dev_server_extra_args.len});
            return error.InvalidOptions;
        }
        return create(gpa, io, .time_skipping, options);
    }

    /// Makes an environment on the server `client` is connected to, in `client`'s namespace,
    /// for a test that already runs a server (a shared dev server, one started with special
    /// settings, a staging namespace). It starts no server, and `deinit` stops none: it
    /// stops the environment's worker and leaves the server and `client` as they were.
    ///
    /// The environment works through its own client for the same namespace on `client`'s
    /// connection (`Client.forNamespace`), so it uses `client`'s codecs, logger,
    /// propagators, interceptors and metrics, and `client` and the environment may be closed
    /// in either order. A view from `Client.withRpcOptions` gives an environment without
    /// the view's RPC options. `client` is only read during the call.
    ///
    /// The helpers run workers, activity test doubles and workflows as on the dev server
    /// (`start`). Time is the server's own: the environment never skips time, even on a
    /// time-skipping test server, so `sleep` is a real sleep and `now` the wall clock, as on
    /// the dev server. Use `startTimeSkipping` for time skipping. The server needs whatever
    /// the test uses that `start` sets up on its own dev server, such as
    /// `custom_search_attributes`. Free with `deinit`.
    pub fn fromClient(gpa: Allocator, io: Io, client: *client_mod.Client) !*WorkflowEnvironment {
        const env = try gpa.create(WorkflowEnvironment);
        errdefer gpa.destroy(env);
        env.* = .{ .gpa = gpa, .io = io, .server = .existing, .client = try client.forNamespace(client.namespace), .worker = undefined, .task_queue = undefined };
        errdefer env.client.close();
        try env.initWorker();
        return env;
    }

    /// The servers an environment starts for itself.
    const OwnServer = enum { dev, time_skipping };

    fn create(gpa: Allocator, io: Io, kind: OwnServer, options: Options) !*WorkflowEnvironment {
        const env = try gpa.create(WorkflowEnvironment);
        errdefer gpa.destroy(env);
        env.gpa = gpa;
        env.io = io;
        env.running = null;
        env.next_id = 0;
        env.time_lock_lost = false;
        var client_options = switch (kind) {
            .dev => blk: {
                env.server = .{ .dev = try DevServer.start(gpa, io, .{ .search_attributes = &custom_search_attributes, .extra_args = options.dev_server_extra_args }) };
                break :blk env.server.clientOptions();
            },
            .time_skipping => blk: {
                env.server = .{ .time_skipping = try TestServer.start(gpa, io, .{}) };
                break :blk env.server.time_skipping.clientOptions();
            },
        };
        errdefer env.server.stop(io);
        client_options.codecs = options.codecs;
        client_options.encode_failure_attributes = options.encode_failure_attributes;
        client_options.failure_converter = options.failure_converter;
        client_options.logger = options.logger;
        client_options.propagators = options.propagators;
        client_options.worker_interceptors = options.worker_interceptors;
        client_options.interceptors = options.interceptors;
        client_options.metrics = options.metrics;
        // The time-skipping test server reports an old server version, which makes the core
        // warn about heartbeat details on every worker start.
        if (kind == .time_skipping) client_options.core_log_level = .err;
        env.client = try client_mod.Client.connect(gpa, io, client_options);
        errdefer env.client.close();
        try env.initWorker();
        return env;
    }

    /// Sets `task_queue` to a fresh one and `worker` to a worker on it.
    fn initWorker(env: *WorkflowEnvironment) !void {
        var suffix: [8]u8 = undefined;
        env.io.random(&suffix);
        env.task_queue = try std.fmt.allocPrint(env.gpa, "test-{x}", .{suffix});
        errdefer env.gpa.free(env.task_queue);
        env.worker = try worker_mod.Worker.init(env.gpa, env.io, &env.client, env.task_queue, .{});
    }

    pub fn deinit(env: *WorkflowEnvironment) void {
        if (env.running) |*r| {
            env.worker.stop();
            r.await(env.io) catch {};
        }
        env.worker.deinit();
        env.client.close();
        env.server.stop(env.io);
        env.gpa.free(env.task_queue);
        env.gpa.destroy(env);
    }

    pub fn registerWorkflow(env: *WorkflowEnvironment, comptime Workflow: type) !void {
        return env.worker.registerWorkflow(Workflow);
    }

    pub fn registerActivity(env: *WorkflowEnvironment, comptime Activity: type) !void {
        return env.worker.registerActivity(Activity);
    }

    /// Registers a method activity bound to `receiver`. See `Worker.registerActivityWith`.
    /// Naming rules: see `temporal.activity.Def`.
    pub fn registerActivityWith(env: *WorkflowEnvironment, comptime Activity: type, receiver: *Activity.Receiver) !void {
        return env.worker.registerActivityWith(Activity, receiver);
    }

    /// Registers several method activities bound to one receiver. See
    /// `Worker.registerActivitiesWith`. Naming rules: see `temporal.activity.Def`.
    pub fn registerActivitiesWith(env: *WorkflowEnvironment, receiver: anytype, comptime activities: anytype) !void {
        return env.worker.registerActivitiesWith(receiver, activities);
    }

    /// Registers every activity method of the struct `impl` points to, under the names of
    /// `acts`, the `temporal.activity.defs` value workflows call them through. See
    /// `Worker.registerActivities`.
    pub fn registerActivities(env: *WorkflowEnvironment, impl: anytype, comptime acts: anytype) !void {
        return env.worker.registerActivities(impl, acts);
    }

    /// Registers `impl` in place of `Activity`: workflows calling `Activity` run `impl`.
    /// `impl` takes the same parameters. It takes `Activity`'s name, so `Activity` itself
    /// must not be registered too (`error.AlreadyRegistered`).
    pub fn registerActivityAs(env: *WorkflowEnvironment, comptime Activity: type, comptime impl: anytype) !void {
        return env.worker.registerActivityImpl(activity.Def(Activity.activity_name, impl).registered);
    }

    /// Registers a dynamic workflow. See `Worker.registerDynamicWorkflow`.
    pub fn registerDynamicWorkflow(env: *WorkflowEnvironment, comptime Dynamic: type) !void {
        return env.worker.registerDynamicWorkflow(Dynamic);
    }

    /// Registers a dynamic activity. See `Worker.registerDynamicActivity`.
    pub fn registerDynamicActivity(env: *WorkflowEnvironment, comptime Dynamic: type) !void {
        return env.worker.registerDynamicActivity(Dynamic);
    }

    /// Registers a dynamic activity that is a method, bound to `receiver`. See
    /// `Worker.registerDynamicActivityWith`.
    pub fn registerDynamicActivityWith(env: *WorkflowEnvironment, comptime Dynamic: type, receiver: *Dynamic.Receiver) !void {
        return env.worker.registerDynamicActivityWith(Dynamic, receiver);
    }

    /// Registers a Nexus service handler (`temporal.nexus.ServiceHandler`) on the
    /// environment's worker, which then serves its operations to callers of an endpoint
    /// routed to the environment's task queue (`createNexusEndpoint`). See
    /// `Worker.registerNexusService`. Call it before the worker starts (the first workflow
    /// the environment starts); returns `error.AlreadyRegistered` for a service name
    /// registered already.
    pub fn registerNexusService(env: *WorkflowEnvironment, comptime Handler: type) !void {
        return env.worker.registerNexusService(Handler);
    }

    /// Registers a Nexus service handler whose handlers take a receiver, bound to `receiver`,
    /// which must outlive the environment and be safe to use from several requests at once.
    /// See `Worker.registerNexusServiceWith` and `registerNexusService`.
    pub fn registerNexusServiceWith(env: *WorkflowEnvironment, comptime Handler: type, receiver: *Handler.Receiver) !void {
        return env.worker.registerNexusServiceWith(Handler, receiver);
    }

    /// Runs a workflow to completion on the environment's worker and returns its result,
    /// allocated from `arena`. On `error.WorkflowFailed`, `failure` (if given) receives the
    /// workflow's failure.
    ///
    /// Once the workflow has closed, its history is replayed against the registered
    /// workflows, so a test also fails (`error.Nondeterminism`) if the workflow can't replay
    /// its own history.
    pub fn executeWorkflow(
        env: *WorkflowEnvironment,
        comptime Workflow: type,
        args: Workflow.Args,
        arena: Allocator,
        failure: ?*?failure_mod.Failure,
    ) !Workflow.Result {
        return env.executeWorkflowWithOptions(Workflow, .{}, args, arena, failure);
    }

    /// `executeWorkflow` with start options. `id` and `task_queue` default to a unique ID and
    /// the environment's task queue.
    pub fn executeWorkflowWithOptions(
        env: *WorkflowEnvironment,
        comptime Workflow: type,
        options: client_mod.StartWorkflowOptions,
        args: Workflow.Args,
        arena: Allocator,
        failure: ?*?failure_mod.Failure,
    ) !Workflow.Result {
        var run = try env.startWorkflow(Workflow, options, args);
        defer run.deinit();
        return env.getResult(Workflow, &run, arena, failure);
    }

    /// Starts a workflow on the environment's worker (starting the worker if needed) and
    /// returns its run without waiting for it. `id` and `task_queue` default to a unique ID
    /// and the environment's task queue. The caller owns the run (`run.deinit()`); wait for
    /// it with `getResult` so time skips and the history is replayed.
    pub fn startWorkflow(
        env: *WorkflowEnvironment,
        comptime Workflow: type,
        options: client_mod.StartWorkflowOptions,
        args: Workflow.Args,
    ) !client_mod.WorkflowRun(Workflow.Result) {
        try env.checkTimeLock();
        try env.ensureWorker();
        var generated_id: ?[]u8 = null;
        defer if (generated_id) |id| env.gpa.free(id);
        return env.client.executeWorkflow(Workflow, try env.startOptions(Workflow.workflow_name, options, &generated_id), args);
    }

    /// `executeWorkflowWithOptions` for a workflow started by its type name, such as one a
    /// dynamic workflow runs: `args` is a tuple of argument values, and the result is decoded
    /// as `Result`.
    pub fn executeWorkflowByName(
        env: *WorkflowEnvironment,
        comptime Result: type,
        workflow_type: []const u8,
        options: client_mod.StartWorkflowOptions,
        args: anytype,
        arena: Allocator,
        failure: ?*?failure_mod.Failure,
    ) !Result {
        var run = try env.startWorkflowByName(Result, workflow_type, options, args);
        defer run.deinit();
        return env.getResultByName(Result, &run, arena, failure);
    }

    /// `startWorkflow` for a workflow started by its type name; `args` is a tuple of argument
    /// values. Wait for the run with `getResultByName`.
    pub fn startWorkflowByName(
        env: *WorkflowEnvironment,
        comptime Result: type,
        workflow_type: []const u8,
        options: client_mod.StartWorkflowOptions,
        args: anytype,
    ) !client_mod.WorkflowRun(Result) {
        try env.checkTimeLock();
        try env.ensureWorker();
        var generated_id: ?[]u8 = null;
        defer if (generated_id) |id| env.gpa.free(id);
        return env.client.executeWorkflowByName(Result, workflow_type, try env.startOptions(workflow_type, options, &generated_id), args);
    }

    /// Sends `Signal` with `args` to the latest run of `workflow_id`. Returns once the server
    /// has recorded the signal; time stays locked on the time-skipping server.
    pub fn signalWorkflow(env: *WorkflowEnvironment, comptime Signal: type, workflow_id: []const u8, args: Signal.Args) !void {
        try env.checkTimeLock();
        return env.client.signalWorkflow(Signal, workflow_id, "", args);
    }

    /// Sends `Query` with `args` to the latest run of `workflow_id` (starting the worker if
    /// needed, so it can answer) and returns the result, allocated from `arena`. Time stays
    /// locked on the time-skipping server, so a workflow in the middle of a timer answers
    /// without the timer firing.
    pub fn queryWorkflow(env: *WorkflowEnvironment, comptime Query: type, arena: Allocator, workflow_id: []const u8, args: Query.Args) !Query.Result {
        try env.checkTimeLock();
        try env.ensureWorker();
        return env.client.queryWorkflow(Query, arena, workflow_id, "", args);
    }

    /// Sends `Update` with `args` to the latest run of `workflow_id` (starting the worker if
    /// needed), waits for it to complete, and returns the result, allocated from `arena`. On
    /// `error.UpdateFailed` (rejected by the validator, failed in the handler, or no handler
    /// for it), `failure` (if given) receives why; it borrows `arena`.
    ///
    /// Time stays locked on the time-skipping server. Unlocked, the server skips ahead to the
    /// next deadline as soon as the workflow is idle, and a workflow waiting for a signal or
    /// an update has only its execution timeout left, so it would time out before the update
    /// arrived or right after it completed. To let a handler that sleeps finish, send the
    /// update with `env.client.updateWorkflow` and `.wait_for_stage = .accepted`, call
    /// `env.sleep` for the handler's sleep, then `get` the handle.
    pub fn updateWorkflow(
        env: *WorkflowEnvironment,
        comptime Update: type,
        arena: Allocator,
        workflow_id: []const u8,
        args: Update.Args,
        failure: ?*?failure_mod.Failure,
    ) !Update.Result {
        try env.checkTimeLock();
        try env.ensureWorker();
        var handle = try env.client.updateWorkflow(Update, workflow_id, "", args, .{});
        defer handle.deinit();
        return handle.get(arena) catch |err| {
            if (err == error.UpdateFailed) if (failure) |f| {
                f.* = if (handle.failure()) |fl| fl.* else null;
            };
            return err;
        };
    }

    /// Starts `Workflow` with `args` and signals it with `Signal` and `signal_args` in one
    /// call, or only signals it if a workflow with that ID is running (see
    /// `Client.signalWithStartWorkflow`). Starts the worker if needed and defaults `id` and
    /// `task_queue` as `startWorkflow` does. The caller owns the run (`run.deinit()`).
    pub fn signalWithStartWorkflow(
        env: *WorkflowEnvironment,
        comptime Workflow: type,
        options: client_mod.StartWorkflowOptions,
        args: Workflow.Args,
        comptime Signal: type,
        signal_args: Signal.Args,
    ) !client_mod.WorkflowRun(Workflow.Result) {
        try env.checkTimeLock();
        try env.ensureWorker();
        var generated_id: ?[]u8 = null;
        defer if (generated_id) |id| env.gpa.free(id);
        return env.client.signalWithStartWorkflow(Workflow, try env.startOptions(Workflow.workflow_name, options, &generated_id), args, Signal, signal_args);
    }

    /// Starts `Workflow` with `args` and sends it `Update` with `update_args` in one call, or
    /// only updates it if a workflow with that ID is running (see
    /// `Client.updateWithStartWorkflow`; `options.id_conflict_policy` is required). Starts
    /// the worker if needed and defaults `id` and `task_queue` as `startWorkflow` does. Time
    /// stays locked on the time-skipping server, as in `updateWorkflow`. The caller owns the
    /// handle (`deinit`); wait for the workflow with `getResult` on
    /// `env.client.getWorkflow(handle.workflow_id, handle.run_id)`.
    pub fn updateWithStartWorkflow(
        env: *WorkflowEnvironment,
        comptime Workflow: type,
        options: client_mod.StartWorkflowOptions,
        args: Workflow.Args,
        comptime Update: type,
        update_args: Update.Args,
        update_options: client_mod.UpdateOptions,
    ) !client_mod.WorkflowUpdateHandle(Update.Result) {
        try env.checkTimeLock();
        try env.ensureWorker();
        var generated_id: ?[]u8 = null;
        defer if (generated_id) |id| env.gpa.free(id);
        return env.client.updateWithStartWorkflow(Workflow, try env.startOptions(Workflow.workflow_name, options, &generated_id), args, Update, update_args, update_options);
    }

    /// Starts the worker if it isn't running, and waits until it polls (so an eager start
    /// reaches it). A worker that fails to start returns its error here.
    fn ensureWorker(env: *WorkflowEnvironment) !void {
        if (env.running != null) return;
        const w = &env.worker;
        // Begin the run's generation before spawning it: the task may not have entered `run`
        // when `waitUntilRunning` below takes the mutex, and on a reused worker (a first
        // `ensureWorker` that returned NothingRegistered) it would then report the previous
        // run's failed start, and the `await` below would wait for a run that polls. `run`
        // raises the generation again and settles the one it ends at, which this waits for.
        {
            w.stop_mutex.lockUncancelable(env.io);
            defer w.stop_mutex.unlock(env.io);
            w.run_generation += 1;
            w.start_changed.broadcast(env.io);
        }
        env.running = io_tasks.concurrent(env.io, runWorker, .{w}) catch |err| {
            // No run will settle the generation begun above; settle it so no waiter blocks.
            w.stop_mutex.lockUncancelable(env.io);
            defer w.stop_mutex.unlock(env.io);
            w.started_generation = w.run_generation;
            w.start_failed = true;
            w.start_changed.broadcast(env.io);
            return err;
        };
        env.worker.waitUntilRunning() catch |err| switch (err) {
            error.WorkerFailedToStart => {
                const result = env.running.?.await(env.io);
                env.running = null;
                try result;
                return err;
            },
            else => |e| return e,
        };
    }

    /// `options` with `id` and `task_queue` defaulted to a unique ID and the environment's
    /// task queue. A generated ID is allocated with `env.gpa` and stored in `generated_id`,
    /// which the caller frees once the start call returns.
    fn startOptions(env: *WorkflowEnvironment, workflow_type: []const u8, options: client_mod.StartWorkflowOptions, generated_id: *?[]u8) !client_mod.StartWorkflowOptions {
        env.next_id += 1;
        var opts = options;
        if (opts.id.len == 0) {
            generated_id.* = try std.fmt.allocPrint(env.gpa, "{s}-{s}-{d}", .{ workflow_type, env.task_queue, env.next_id });
            opts.id = generated_id.*.?;
        }
        if (opts.task_queue.len == 0) opts.task_queue = env.task_queue;
        return opts;
    }

    /// Waits for a run from `startWorkflow` to close and returns its result, allocated from
    /// `arena`, then replays its history as `executeWorkflow` does. It follows the run's
    /// continue-as-new, cron and retry runs to the last one (`run.run_id` moves along), and
    /// replays that last run. On the time-skipping server, time skipping is unlocked for the
    /// wait. On `error.WorkflowFailed`, `failure` (if given) receives the workflow's failure.
    /// The run stays owned by the caller.
    ///
    /// If time skipping can't be unlocked, or locked again after the wait (the call failed,
    /// for example out of memory or on a lost connection), it returns `error.TimeLockLost` in
    /// place of the result and `failure` isn't written. The server's lock count is then
    /// unknown and time may keep skipping, so the environment is unusable: its later calls
    /// return `error.TimeLockLost` without reaching the server, and only `deinit` remains;
    /// start a new environment. A failed unlock counts as lost even if it never reached the
    /// server. The re-lock runs even when the task is canceled during the wait.
    ///
    /// On the time-skipping server, time may keep skipping between the run's close and the
    /// re-lock: once the run closes, nothing holds the server's time lock until the re-lock
    /// arrives, so the server skips ahead to its next pending timer, which may be years later.
    /// That includes timers the closed run itself left behind, such as an `awaitTimeout`
    /// timeout canceled when its condition came true, or a workflow task timeout, as well as
    /// the timers and default run timeouts of other open runs, which may then time out. The
    /// server has no call that locks together with the run's close. So `env.now()` afterwards
    /// is only guaranteed to be no earlier than the close. For the close time, use
    /// `close_time` from `env.client.describeWorkflow(arena, run.id, run.run_id)`, or
    /// `ctx.now()` inside the workflow.
    pub fn getResult(
        env: *WorkflowEnvironment,
        comptime Workflow: type,
        run: *client_mod.WorkflowRun(Workflow.Result),
        arena: Allocator,
        failure: ?*?failure_mod.Failure,
    ) !Workflow.Result {
        return env.getResultByNameWithOptions(Workflow.Result, run, .{}, arena, failure);
    }

    /// `getResult` with `client.WorkflowRun.GetOptions`. With `.follow_runs = false` it waits
    /// for the run `run.run_id` names alone and replays that run: a run that continued as
    /// new is replayed and then returns `error.WorkflowContinuedAsNew`, and a cron or retry
    /// run returns its own result or failure. What the workflow code did in the run decides
    /// which, as its replay shows, whatever its retry policy or cron schedule. A handle with
    /// an empty run ID (from `Client.getWorkflow(id, "")`) is first set to the run that is
    /// latest when the call starts, and that run is waited for and replayed. With
    /// `.follow_runs = true` it is `getResult`.
    /// Time skipping is unlocked for the wait and locked again, as in `getResult`, which also
    /// says when it returns `error.TimeLockLost`.
    pub fn getResultWithOptions(
        env: *WorkflowEnvironment,
        comptime Workflow: type,
        run: *client_mod.WorkflowRun(Workflow.Result),
        options: client_mod.WorkflowRun(Workflow.Result).GetOptions,
        arena: Allocator,
        failure: ?*?failure_mod.Failure,
    ) !Workflow.Result {
        return env.getResultByNameWithOptions(Workflow.Result, run, options, arena, failure);
    }

    /// `getResult` for a run from `startWorkflowByName`: `Result` is the type its result
    /// decodes as.
    pub fn getResultByName(
        env: *WorkflowEnvironment,
        comptime Result: type,
        run: *client_mod.WorkflowRun(Result),
        arena: Allocator,
        failure: ?*?failure_mod.Failure,
    ) !Result {
        return env.getResultByNameWithOptions(Result, run, .{}, arena, failure);
    }

    /// `getResultWithOptions` for a run from `startWorkflowByName`: `Result` is the type its
    /// result decodes as. It replays the same run `getResultWithOptions` does.
    pub fn getResultByNameWithOptions(
        env: *WorkflowEnvironment,
        comptime Result: type,
        run: *client_mod.WorkflowRun(Result),
        options: client_mod.WorkflowRun(Result).GetOptions,
        arena: Allocator,
        failure: ?*?failure_mod.Failure,
    ) !Result {
        try env.checkTimeLock();
        // The wait, the outcome and the replay must all be of one run. An empty run ID names
        // whichever run is latest at each call, which a continue-as-new, retry or cron run
        // changes while the wait goes on, so the handle is pinned to the latest run first.
        if (run.run_id.len == 0) {
            var scratch: std.heap.ArenaAllocator = .init(env.gpa);
            defer scratch.deinit();
            const latest_run_id = try env.client.latestRunId(scratch.allocator(), run.id);
            const pinned = try run.client.gpa.dupe(u8, latest_run_id);
            run.client.gpa.free(run.run_id);
            run.run_id = pinned;
        }
        var result = if (env.server == .time_skipping)
            try env.unlockedWait(Result, run, options, arena)
        else
            run.getWithOptions(arena, options);
        // The close event a wait sees can say continued-as-new for a run that a retry or its
        // cron schedule followed: the dev server's close-event poll reports a retried or cron
        // run that way, and the time-skipping server ends such a run's history with that
        // event. `ownOutcome` reads the run's own outcome and replays the run.
        var replayed = false;
        if (result) |_| {} else |err| if (err == error.WorkflowContinuedAsNew) {
            result = try env.ownOutcome(Result, run, arena);
            replayed = true;
        }
        if (result) |_| {} else |err| {
            if (failure) |f| f.* = if (run.failure()) |fl| fl.* else null;
            switch (err) {
                error.WorkflowFailed, error.WorkflowCanceled, error.WorkflowTimedOut, error.WorkflowContinuedAsNew => {},
                else => return err,
            }
        }
        if (!replayed) try env.replayRun(run.id, run.run_id);
        return result;
    }

    /// Waits for `run` with time skipping unlocked, and locks it again whatever the wait
    /// returned, so the server's lock count ends as it began. A failed unlock or re-lock sets
    /// `time_lock_lost` and returns `error.TimeLockLost` in place of the wait's outcome:
    /// neither call is retried, since the server counts them and a failed call may still have
    /// reached it. A failed unlock counts as lost even when it failed before sending, as its
    /// delivery can't be told apart. A cancel that the unlock or the wait consumed is re-armed
    /// before `error.TimeLockLost` is returned, so the caller's next cancellation point still
    /// returns `error.Canceled`.
    fn unlockedWait(
        env: *WorkflowEnvironment,
        comptime Result: type,
        run: *client_mod.WorkflowRun(Result),
        options: client_mod.WorkflowRun(Result).GetOptions,
        arena: Allocator,
    ) error{TimeLockLost}!(client_mod.ResultError!Result) {
        _ = env.testService(ts.UnlockTimeSkippingResponse, "UnlockTimeSkipping", ts.UnlockTimeSkippingRequest{}, arena) catch |err| {
            env.time_lock_lost = true;
            logTimeLockLost("UnlockTimeSkipping", err);
            // The cancel this consumed is the caller's: re-arm it, as TimeLockLost replaces it.
            if (err == error.Canceled) env.io.recancel();
            return error.TimeLockLost;
        };
        const result = run.getWithOptions(arena, options);
        // Blocked: a cancellation (one that also ended the wait above) must not stop the
        // re-lock, which would leave time skipping.
        const prev = env.io.swapCancelProtection(.blocked);
        defer _ = env.io.swapCancelProtection(prev);
        const locked = if (builtin.is_test and takeRelockFailure())
            error.RpcFailed
        else
            env.testService(ts.LockTimeSkippingResponse, "LockTimeSkipping", ts.LockTimeSkippingRequest{}, arena);
        _ = locked catch |err| {
            env.time_lock_lost = true;
            logTimeLockLost("LockTimeSkipping", err);
            // The re-lock can't be canceled (protection is blocked), but a canceled wait
            // consumed the caller's cancel: re-arm it, as TimeLockLost replaces it. The
            // deferred swap restores protection, so the caller's next cancellation point
            // sees it.
            if (result) |_| {} else |wait_err| if (wait_err == error.Canceled) env.io.recancel();
            return error.TimeLockLost;
        };
        return result;
    }

    /// `error.TimeLockLost` once `getResult` has lost the time lock (`time_lock_lost`).
    fn checkTimeLock(env: *const WorkflowEnvironment) error{TimeLockLost}!void {
        if (env.time_lock_lost) return error.TimeLockLost;
    }

    /// Replays the run `run` names, whose close event a wait read as continued-as-new, and
    /// returns its own outcome: the error is the replay's, the outcome is the inner value.
    ///
    /// A history that ends in a close event other than continued-as-new, or in a
    /// continued-as-new event the server says the workflow made, is that outcome. A
    /// continued-as-new event that a retry or a cron schedule may have made (the
    /// time-skipping server names no initiator on any of them) is dropped before the
    /// replay, since no workflow command made it, and the replay reports what the workflow
    /// code did: returned a result, failed, or continued as new itself. A run the workflow
    /// didn't finish (it timed out) takes the server's failure.
    fn ownOutcome(env: *WorkflowEnvironment, comptime Result: type, run: *client_mod.WorkflowRun(Result), arena: Allocator) !(client_mod.ResultError!Result) {
        var history = try env.client.fetchHistory(arena, run.id, run.run_id);
        const events = history.events.items;
        if (events.len == 0) return error.InvalidResponse;
        const attrs = events[events.len - 1].attributes orelse return error.InvalidResponse;
        const continued = switch (attrs) {
            .workflow_execution_continued_as_new_event_attributes => |c| c,
            else => {
                const outcome: client_mod.ResultError!Result = switch (attrs) {
                    .workflow_execution_completed_event_attributes => |done| decodeResult(Result, arena, &env.resultContext(run.id), done.result),
                    .workflow_execution_failed_event_attributes => |failed| blk: {
                        run.failure_ = try env.readFailure(arena, run.id, failed.failure);
                        break :blk error.WorkflowFailed;
                    },
                    .workflow_execution_timed_out_event_attributes => error.WorkflowTimedOut,
                    .workflow_execution_canceled_event_attributes => error.WorkflowCanceled,
                    .workflow_execution_terminated_event_attributes => error.WorkflowTerminated,
                    else => error.InvalidResponse,
                };
                try env.replayHistoryOf(run.id, history, null);
                return outcome;
            },
        };
        if (continued.initiator == .CONTINUE_AS_NEW_INITIATOR_WORKFLOW) {
            try env.replayHistoryOf(run.id, history, null);
            return @as(client_mod.ResultError!Result, error.WorkflowContinuedAsNew);
        }
        var watch: OutcomeWatch = .{ .arena = arena };
        history.events.items.len -= 1;
        try env.replayHistoryOf(run.id, history, &watch);
        return env.watchedOutcome(Result, run, arena, continued, watch.outcome);
    }

    /// The outcome of a run whose replay `outcome` watched, `continued` being its last event.
    fn watchedOutcome(
        env: *WorkflowEnvironment,
        comptime Result: type,
        run: *client_mod.WorkflowRun(Result),
        arena: Allocator,
        continued: proto.temporal.api.history.v1.WorkflowExecutionContinuedAsNewEventAttributes,
        outcome: OutcomeWatch.Outcome,
    ) client_mod.ResultError!Result {
        switch (outcome) {
            .continued_as_new => return error.WorkflowContinuedAsNew,
            .returned => |payload| {
                if (Result == void) return;
                return converter.fromPayloadWithContext(Result, arena, &env.resultContext(run.id), payload);
            },
            .failed, .unfinished => {
                // The time-skipping server records a retried run's failure only on the run
                // it starts.
                var why = continued.failure;
                if (why == null and continued.new_execution_run_id.len > 0)
                    why = (try env.startedEvent(arena, run.id, continued.new_execution_run_id)).continued_failure;
                run.failure_ = try env.readFailure(arena, run.id, why);
                if (why) |f| if (f.failure_info) |info| if (info == .timeout_failure_info) return error.WorkflowTimedOut;
                // A run the workflow didn't finish and the server didn't fail.
                if (outcome == .unfinished and why == null) return error.WorkflowContinuedAsNew;
                return error.WorkflowFailed;
            },
        }
    }

    /// Records how the workflow function of the run being replayed ended. It is the
    /// outermost inbound interceptor, so it sees what the workflow sends as its command.
    const OutcomeWatch = struct {
        arena: Allocator,
        outcome: Outcome = .unfinished,

        const Outcome = union(enum) { unfinished, returned: converter.Payload, failed, continued_as_new };

        pub fn interceptWorkflow(w: *OutcomeWatch, ctx: *workflow.Context) error{OutOfMemory}!interceptor.Workflow {
            const run = try ctx.allocator().create(Run);
            run.* = .{ .w = w };
            return .{ .inbound = interceptor.WorkflowInbound.from(Run, run) };
        }

        const Run = struct {
            w: *OutcomeWatch,

            pub fn executeWorkflow(r: *Run, input: interceptor.ExecuteWorkflowInput, next: interceptor.WorkflowInbound.Next) anyerror!converter.Payload {
                const result = next.executeWorkflow(input) catch |err| {
                    r.w.outcome = if (err == error.ContinueAsNew) .continued_as_new else .failed;
                    return err;
                };
                r.w.outcome = .{ .returned = try result.dupe(r.w.arena) };
                return result;
            }
        };
    };

    fn decodeResult(comptime Result: type, arena: Allocator, context: *const converter.Context, payloads: ?proto.temporal.api.common.v1.Payloads) client_mod.ResultError!Result {
        if (Result == void) return;
        const p = payloads orelse return error.InvalidPayload;
        if (p.payloads.items.len == 0) return error.InvalidPayload;
        return converter.fromPayloadWithContext(Result, arena, context, p.payloads.items[0]);
    }

    /// The run's failure `f` as `WorkflowRun.get` reads it: through the client's failure
    /// converter, with the context of the run's result. Memory from `arena`.
    fn readFailure(env: *const WorkflowEnvironment, arena: Allocator, workflow_id: []const u8, f: ?failure_mod.Failure) error{OutOfMemory}!?failure_mod.Failure {
        const v = f orelse return null;
        const c = env.client.failure_converter orelse return v;
        return try c.fromFailure(arena, &env.resultContext(workflow_id), v);
    }

    /// The converter context of the result of workflow `workflow_id`, as
    /// `client.WorkflowRun.get` decodes it with: the namespace and the workflow ID. Borrows
    /// `workflow_id`.
    fn resultContext(env: *const WorkflowEnvironment, workflow_id: []const u8) converter.Context {
        return .{ .namespace = env.client.namespace, .workflow_id = workflow_id };
    }

    /// The started event of run `run_id`, from `arena`.
    fn startedEvent(env: *WorkflowEnvironment, arena: Allocator, workflow_id: []const u8, run_id: []const u8) client_mod.ResultError!proto.temporal.api.history.v1.WorkflowExecutionStartedEventAttributes {
        var it = try env.client.historyIterator(workflow_id, run_id, .{});
        defer it.deinit();
        const event = try it.next(arena) orelse return error.InvalidResponse;
        return switch (event.attributes orelse return error.InvalidResponse) {
            .workflow_execution_started_event_attributes => |started| started,
            else => error.InvalidResponse,
        };
    }

    /// Lets `duration` of server time pass. On the time-skipping server this skips time
    /// (workflows see their timers fire) and returns as soon as server time has advanced; on
    /// the dev server it is a real sleep. On the time-skipping server a duration longer than
    /// protobuf can carry (about 10,000 years), such as `Duration.max`, skips the longest one
    /// it can.
    pub fn sleep(env: *WorkflowEnvironment, duration: Io.Duration) !void {
        try env.checkTimeLock();
        switch (env.server) {
            .dev, .existing => try env.io.sleep(duration, .awake),
            .time_skipping => {
                var scratch: std.heap.ArenaAllocator = .init(env.gpa);
                defer scratch.deinit();
                _ = try env.testService(ts.SleepResponse, "UnlockTimeSkippingWithSleep", ts.SleepRequest{ .duration = proto_time.durationToProto(duration).? }, scratch.allocator());
            },
        }
    }

    /// The server's current time: skipped time included on the time-skipping server, the
    /// wall clock on the dev server. After `getResult` on the time-skipping server it is only
    /// no earlier than the run's close, since time may skip on to a pending timer first, even
    /// a canceled one of the closed run itself; `getResult` says how to get the close time.
    pub fn now(env: *WorkflowEnvironment) !Io.Timestamp {
        try env.checkTimeLock();
        switch (env.server) {
            .dev, .existing => return Io.Timestamp.now(env.io, .real),
            .time_skipping => {
                var scratch: std.heap.ArenaAllocator = .init(env.gpa);
                defer scratch.deinit();
                const response = try env.testService(ts.GetCurrentTimeResponse, "GetCurrentTime", proto.google.protobuf.Empty{}, scratch.allocator());
                const t = response.time orelse {
                    log.err("GetCurrentTime returned no time", .{});
                    return error.InvalidResponse;
                };
                return .fromNanoseconds(@as(i96, t.seconds) * std.time.ns_per_s + t.nanos);
            },
        }
    }

    /// Where `createNexusEndpoint` routes an endpoint's operations: a namespace and a task
    /// queue on the environment's server, whose worker handles them.
    pub const NexusEndpointTarget = struct {
        /// Empty routes to the environment's namespace.
        namespace: []const u8 = "",
        /// Empty routes to the environment's task queue.
        task_queue: []const u8 = "",
    };

    /// Creates a Nexus endpoint called `name` on the environment's server (the operator
    /// service's CreateNexusEndpoint), routing to `target`, so workflows can call operations
    /// through it (`workflow.Context.executeNexusOperation` with `.endpoint = name`). The
    /// server accepts names of ASCII letters, digits, `-` and `_`, starting with a letter.
    /// The endpoint lasts as long as the server; nothing has to poll its task queue for it
    /// to exist. `name` and `target` are only read during the call. Tested against the dev
    /// server (`start`); a server without Nexus endpoints returns `error.RpcFailed`.
    pub fn createNexusEndpoint(env: *WorkflowEnvironment, name: []const u8, target: NexusEndpointTarget) CreateNexusEndpointError!void {
        return testing_ns.createNexusEndpoint(&env.client, name, .{
            .namespace = if (target.namespace.len > 0) target.namespace else env.client.namespace,
            .task_queue = if (target.task_queue.len > 0) target.task_queue else env.task_queue,
        });
    }

    const ts = proto.temporal.api.testservice.v1;

    /// Calls a TestService method; the response's memory comes from `arena`. Not retried:
    /// the server counts lock and unlock calls, so a retried call could unbalance them.
    fn testService(env: *WorkflowEnvironment, comptime Response: type, comptime method: []const u8, request: anytype, arena: Allocator) error{ OutOfMemory, Canceled, RpcFailed, InvalidResponse }!Response {
        return env.serviceCall(.test_service, Response, method, request, arena, null);
    }

    fn serviceCall(env: *WorkflowEnvironment, service: bridge.Connection.Service, comptime Response: type, comptime method: []const u8, request: anytype, arena: Allocator, status: ?*u32) error{ OutOfMemory, Canceled, RpcFailed, InvalidResponse }!Response {
        return testing_ns.serviceCall(&env.client, service, Response, method, request, arena, status);
    }

    /// Replays a run's history (fetched from the server) against the registered workflows.
    pub fn replayRun(env: *WorkflowEnvironment, workflow_id: []const u8, run_id: []const u8) !void {
        var scratch: std.heap.ArenaAllocator = .init(env.gpa);
        defer scratch.deinit();
        return env.replayHistoryOf(workflow_id, try env.client.fetchHistory(scratch.allocator(), workflow_id, run_id), null);
    }

    /// Replays `history` against the registered workflows, with `watch`, if given, as the
    /// outermost workflow interceptor.
    fn replayHistoryOf(env: *WorkflowEnvironment, workflow_id: []const u8, history: history_mod.History, watch: ?*OutcomeWatch) !void {
        var interceptors: std.ArrayList(interceptor.Worker) = .empty;
        defer interceptors.deinit(env.gpa);
        if (watch) |w| try interceptors.append(env.gpa, interceptor.Worker.from(OutcomeWatch, w));
        try interceptors.appendSlice(env.gpa, env.client.worker_interceptors);
        var replayer: WorkflowReplayer = .{
            .gpa = env.gpa,
            .io = env.io,
            .workflows = env.worker.workflows,
            .dynamic_workflow = env.worker.dynamic_workflow,
            .workflow_id = workflow_id,
            .options = .{
                .namespace = env.client.namespace,
                .task_queue = env.task_queue,
                .codecs = env.client.codecs,
                .failure_converter = env.client.failure_converter,
                .propagators = env.client.propagators,
                .interceptors = interceptors.items,
            },
        };
        return replayer.replayHistory(history);
    }

    fn runWorker(w: *worker_mod.Worker) worker_mod.Worker.RunError!void {
        return w.run();
    }
};

/// Runs activities without a server, each on a task of its own so `cancel` reaches it as a
/// worker's cancel would.
pub const ActivityEnvironment = struct {
    io: Io,
    info: activity.Info = .{
        .activity_id = "test-activity",
        .activity_type = "",
        .task_queue = "test-task-queue",
        .workflow_id = "test-workflow",
        .workflow_run_id = "test-run",
        .workflow_type = "TestWorkflow",
        .workflow_namespace = "default",
        .attempt = 1,
        .task_token = "test-task-token",
    },
    /// Heartbeat details the activity sees from a "previous attempt".
    heartbeat_details: []const converter.Payload = &.{},
    /// Called with each heartbeat's details.
    on_heartbeat: ?activity.Context.HeartbeatSink = null,
    /// What `ctx.client()` returns; null makes it return `error.NoClient`. Borrowed.
    client: ?*client_mod.Client = null,
    /// What `ctx.metrics()` returns, such as a custom handler that keeps what the activity
    /// records. Borrowed.
    metrics: metrics_mod.Handler = .noop,
    /// Where `ctx.logger()` writes, such as a sink that keeps the records for the test to read.
    /// Messages carry the context fields a worker adds, taken from `info`. Null writes to
    /// `std.log` with no context fields. Borrowed: it must outlive the activities it logs for,
    /// and be thread-safe, since activities run on tasks of their own.
    logger: ?log_mod.Sink = null,
    /// Guards `running` and `worker_stopped`, which `cancel` and `stopWorker` share with the
    /// activities running from other threads.
    mutex: Io.Mutex = .init,
    /// The activities running now, newest first.
    running: ?*Running = null,
    /// Set by `stopWorker`.
    worker_stopped: bool = false,

    const Running = struct {
        ctx: *activity.Context,
        /// Set when the activity returns or is canceled; wakes `executeActivityWith`.
        wake: Io.Event = .unset,
        next: ?*Running = null,
    };

    /// An environment whose `info` says the activity was scheduled and started now.
    pub fn init(io: Io) ActivityEnvironment {
        var env: ActivityEnvironment = .{ .io = io };
        const now: Io.Timestamp = .now(io, .real);
        env.info.scheduled_time = now;
        env.info.current_attempt_scheduled_time = now;
        env.info.started_time = now;
        return env;
    }

    /// Calls the activity's function with `args`. The result is allocated from `arena`.
    pub fn executeActivity(env: *ActivityEnvironment, comptime Activity: type, args: Activity.Args, arena: Allocator) !Activity.Result {
        return env.executeActivityWith(Activity, null, args, arena);
    }

    /// Calls a method activity on `receiver` (null for plain functions). The activity runs
    /// on a task of its own while this call waits for it; canceling the calling task cancels
    /// the activity, which sees `error.Canceled` with no `cancelReason`. Without a thread for
    /// that task it runs on the calling one, where `cancel` only sets the reason and wakes
    /// `waitForWorkerStop`. Allocations through `ctx.allocator()` reach `arena` with cancels
    /// blocked, as under a worker.
    pub fn executeActivityWith(env: *ActivityEnvironment, comptime Activity: type, receiver: ?*Activity.Receiver, args: Activity.Args, arena: Allocator) !Activity.Result {
        var info = env.info;
        info.activity_type = Activity.activity_name;
        // The fields a worker adds, kept on this frame, which outlives the activity: building
        // them allocates nothing, so the option adds no error to the activity's.
        var attempt_text: [10]u8 = undefined;
        const context: [6]log_mod.Field = .{
            .{ .key = "ActivityID", .value = info.activity_id },
            .{ .key = "ActivityType", .value = info.activity_type },
            .{ .key = "Attempt", .value = std.fmt.bufPrint(&attempt_text, "{d}", .{info.attempt}) catch unreachable },
            .{ .key = "WorkflowType", .value = info.workflow_type },
            .{ .key = "WorkflowID", .value = info.workflow_id },
            .{ .key = "RunID", .value = info.workflow_run_id },
        };
        const logger: log_mod.Logger = if (env.logger) |sink| .{
            .sink = sink,
            // As a worker does for a standalone activity, which has no workflow.
            .context = if (info.workflow_id.len > 0) &context else context[0..3],
        } else .{};
        // Blocks cancels around each of the activity's allocations, as a worker does, so an
        // allocation cannot consume a cancel meant for the activity's next wait. On this
        // frame, which outlives the activity; memory from it may be freed through `arena`.
        var task_gpa: CancelBlockingAllocator = .{ .io = env.io, .backing = arena };
        var ctx: activity.Context = .{
            .arena = task_gpa.allocator(),
            .io_ = env.io,
            .info_ = info,
            .logger_ = logger,
            .heartbeat_details = env.heartbeat_details,
            .heartbeat_sink = env.on_heartbeat,
            .client_ = env.client,
            .metrics_ = env.metrics,
            .converter_context_ = .{
                .namespace = info.workflow_namespace,
                .workflow_id = info.workflow_id,
                .workflow_type = info.workflow_type,
                .activity_type = info.activity_type,
                .activity_id = info.activity_id,
            },
        };
        ctx.converter_context_.values = &ctx.propagated_;
        var run: Running = .{ .ctx = &ctx };
        {
            env.mutex.lockUncancelable(env.io);
            defer env.mutex.unlock(env.io);
            if (env.worker_stopped) ctx.notifyWorkerStop();
            run.next = env.running;
            env.running = &run;
        }
        defer {
            env.mutex.lockUncancelable(env.io);
            defer env.mutex.unlock(env.io);
            var link = &env.running;
            while (link.*.? != &run) link = &link.*.?.next;
            link.* = run.next;
        }
        const Ret = @typeInfo(@TypeOf(Activity.call)).@"fn".return_type.?;
        // The task hands its result back through `ret`, on this frame, which awaits or
        // cancels the task before it returns: a result aligned above 8 (an error union holding
        // `std.Io.Duration`, say) would overrun its future's allocation (see io_tasks.zig).
        const S = struct {
            fn call(r: ?*Activity.Receiver, c: *activity.Context, a: Activity.Args, wake: *Io.Event, out: *Ret) void {
                defer wake.set(c.io_);
                out.* = Activity.call(r, c, a);
            }
        };
        var ret: Ret = undefined;
        var task = io_tasks.concurrent(env.io, S.call, .{ receiver, &ctx, args, &run.wake, &ret }) catch
            return Activity.call(receiver, &ctx, args);
        run.wake.wait(env.io) catch {
            task.cancel(env.io);
            return ret;
        };
        if (ctx.cancelReason() != null) task.cancel(env.io) else task.await(env.io);
        return ret;
    }

    /// Cancels the activities this environment is running, as a worker does: each sees
    /// `ctx.cancelReason()` return `reason`, and its next wait on `ctx.io()` returns
    /// `error.Canceled`. An activity that was already canceled keeps its first reason. Does
    /// nothing while no activity runs. Safe to call from any thread.
    pub fn cancel(env: *ActivityEnvironment, reason: activity.CancelReason) void {
        env.mutex.lockUncancelable(env.io);
        defer env.mutex.unlock(env.io);
        var it = env.running;
        while (it) |r| : (it = r.next) {
            if (r.ctx.setCancelReason(reason)) r.wake.set(env.io);
        }
    }

    /// Makes the worker-stop signal fire, as `worker.Worker.stop` does: the running
    /// activities, and those executed later, see `ctx.isWorkerStopping()` return true, and
    /// `ctx.waitForWorkerStop()` returns. Safe to call from any thread.
    pub fn stopWorker(env: *ActivityEnvironment) void {
        env.mutex.lockUncancelable(env.io);
        defer env.mutex.unlock(env.io);
        env.worker_stopped = true;
        var it = env.running;
        while (it) |r| : (it = r.next) r.ctx.notifyWorkerStop();
    }
};

/// Replays recorded histories against registered workflows.
pub const WorkflowReplayer = struct {
    gpa: Allocator,
    io: Io,
    workflows: std.StringHashMapUnmanaged(workflow.Registered) = .empty,
    /// Replays workflow types with no entry in `workflows`, if set.
    dynamic_workflow: ?workflow.Registered = null,
    /// The workflow ID replayed workflows see in `ctx.info().workflow_id` when a call doesn't
    /// name one. A history doesn't record its own workflow ID, so set this to the original
    /// run's ID when the workflow's commands depend on it (a child or activity ID derived
    /// from it, for example), or replay fails with `error.Nondeterminism`. Borrowed: it must
    /// outlive every replay call.
    workflow_id: []const u8 = "replay",
    options: Options = .{},

    /// What replayed workflows see in `ctx.info()`. Set them to the original worker's values
    /// when a workflow's commands depend on them. Borrowed: they must outlive every replay
    /// call.
    pub const Options = struct {
        namespace: []const u8 = "default",
        task_queue: []const u8 = "replay",
        /// Payload codecs that decode the history's payloads before the workflow sees them,
        /// as the original worker's client did (`client.Options.codecs`). A history fetched
        /// through a client with codecs is already decoded; codecs pass decoded payloads
        /// through.
        codecs: []const converter.PayloadCodec = &.{},
        /// The failure converter of the original worker's client
        /// (`client.Options.failure_converter`), which replayed workflows send and read
        /// failures through. It must be the one the recording worker used.
        failure_converter: ?converter.FailureConverter = null,
        /// Context propagators that extract the values workflows read with
        /// `ctx.propagated()`, as the original worker's client did
        /// (`client.Options.propagators`).
        propagators: []const propagation.ContextPropagator = &.{},
        /// Worker interceptors of the replayed workflows, as the original worker had them
        /// (`worker.Options.interceptors`): workflow interceptors run during replay, and
        /// those that change commands must be present for the history to match.
        interceptors: []const interceptor.Worker = &.{},
        /// The per-run memory limit to replay with (`worker.Options.workflow_memory_limit`).
        /// A history whose run goes over it fails with `error.ReplayFailed`: set it to the
        /// worker's value to check that its histories fit.
        workflow_memory_limit: ?usize = null,
        /// Deadlock detection while replaying (`worker.Options.deadlock_detection_timeout`).
        /// Null (the default) doesn't watch the replayed code. With a limit, a history whose
        /// workflow code runs longer than it without returning to the workflow scheduler
        /// fails with `error.ReplayFailed`, its message naming the deadlock. Must be positive:
        /// replay calls return `error.ReplayFailed` otherwise.
        deadlock_detection_timeout: ?Io.Duration = null,
        /// The worker's `worker.Options.workflow_task_failure_errors`, so that replayed code
        /// returning one of them fails with `error.ReplayFailed` as its task would, not by
        /// failing the workflow, which the history doesn't record.
        workflow_task_failure_errors: []const anyerror = &.{},
    };

    pub fn init(gpa: Allocator, io: Io) WorkflowReplayer {
        return .{ .gpa = gpa, .io = io };
    }

    pub fn initWithOptions(gpa: Allocator, io: Io, options: Options) WorkflowReplayer {
        return .{ .gpa = gpa, .io = io, .options = options };
    }

    pub fn deinit(r: *WorkflowReplayer) void {
        r.workflows.deinit(r.gpa);
    }

    pub fn registerWorkflow(r: *WorkflowReplayer, comptime Workflow: type) !void {
        try r.workflows.put(r.gpa, Workflow.workflow_name, Workflow.registered);
    }

    /// Registers a dynamic workflow for the histories of types with no registered `Def`.
    /// At most one; a second returns `error.AlreadyRegistered`.
    pub fn registerDynamicWorkflow(r: *WorkflowReplayer, comptime Dynamic: type) error{AlreadyRegistered}!void {
        if (r.dynamic_workflow != null) return error.AlreadyRegistered;
        r.dynamic_workflow = Dynamic.registered;
    }

    pub const ReplayError = error{
        OutOfMemory,
        InvalidHistoryJson,
        /// The history is not one the core can replay: it doesn't start with a
        /// WorkflowExecutionStarted event naming a workflow type and a task queue, a workflow
        /// task started event is followed by something other than its completion, failure or
        /// timeout, two workflow tasks share a started event ID, its first event ID is above
        /// 1, it ends before its last workflow task started event, or it isn't a serialized
        /// history.
        InvalidHistory,
        /// Replay couldn't run or finish the history, for example because its workflow type
        /// isn't registered.
        ReplayFailed,
        /// The workflow code doesn't produce the commands the history recorded.
        Nondeterminism,
    };

    /// How one history replayed, from `replayHistories`, `replayWorkflowExecution` or
    /// `replayListedWorkflows`. The slices borrow the arena passed to that call.
    pub const ReplayResult = struct {
        workflow_id: []const u8,
        /// The run ID in the history's started event; empty for an invalid history.
        run_id: []const u8,
        /// Null when the history replayed cleanly; otherwise `error.Nondeterminism`,
        /// `error.InvalidHistory`, or `error.ReplayFailed`.
        err: ?ReplayError = null,
        /// Why replay failed, as the core reports it (for nondeterminism, the command or event
        /// that didn't match). Empty on success.
        message: []const u8 = "",
    };

    /// Replays the history in a JSON file (a Temporal CLI or UI export).
    pub fn replayHistoryFromJsonFile(r: *WorkflowReplayer, path: []const u8) !void {
        const json = Io.Dir.cwd().readFileAlloc(r.io, path, r.gpa, .limited(256 * 1024 * 1024)) catch |err| {
            log.err("reading history file {s} failed: {t}", .{ path, err });
            return err;
        };
        defer r.gpa.free(json);
        return r.replayHistoryJson(json);
    }

    /// Replays a history given as JSON.
    pub fn replayHistoryJson(r: *WorkflowReplayer, json: []const u8) ReplayError!void {
        var arena_state: std.heap.ArenaAllocator = .init(r.gpa);
        defer arena_state.deinit();
        return r.replayHistory(try history_mod.fromJson(arena_state.allocator(), json));
    }

    /// Replays a serialized temporal.api.history.v1.History.
    pub fn replayHistoryBytes(r: *WorkflowReplayer, history_bytes: []const u8) ReplayError!void {
        var arena_state: std.heap.ArenaAllocator = .init(r.gpa);
        defer arena_state.deinit();
        const a = arena_state.allocator();
        var reader: Io.Reader = .fixed(history_bytes);
        const history = history_mod.History.decode(&reader, a) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            else => {
                log.warn("replay failed: the bytes are not a serialized history ({t})", .{err});
                return error.InvalidHistory;
            },
        };
        return r.replayHistory(history);
    }

    /// Replays a history, for example one from `Client.fetchHistory`, as `r.workflow_id`.
    /// The core's reason for a failure is logged; use `replayHistories` to read it instead.
    pub fn replayHistory(r: *WorkflowReplayer, history: history_mod.History) ReplayError!void {
        var arena_state: std.heap.ArenaAllocator = .init(r.gpa);
        defer arena_state.deinit();
        var session: Session = undefined;
        try session.init(r);
        defer session.deinit();
        const result = try session.replay(arena_state.allocator(), r.workflow_id, history);
        if (result.err) |err| {
            log.warn("replay failed: {s}", .{result.message});
            return err;
        }
    }

    /// Replays several histories through one replay worker, one after another, and returns
    /// a result for each in order: a history that fails to replay doesn't stop the rest.
    /// `histories` is a slice of `history.WorkflowHistory`, or a pointer to an iterator whose
    /// `next(arena)` returns `!?history.WorkflowHistory`; an error from the iterator ends the
    /// batch and is returned. Results are allocated from `arena`.
    pub fn replayHistories(r: *WorkflowReplayer, arena: Allocator, histories: anytype) ![]ReplayResult {
        var results: std.ArrayList(ReplayResult) = .empty;
        // Created on the first history, so an empty batch starts no worker.
        var session: Session = undefined;
        var started = false;
        defer if (started) session.deinit();
        const is_iterator = comptime switch (@typeInfo(@TypeOf(histories))) {
            .pointer => |p| p.size == .one and @typeInfo(p.child) == .@"struct",
            else => false,
        };
        if (is_iterator) {
            while (try histories.next(arena)) |h| {
                if (!started) try session.init(r);
                started = true;
                try results.append(arena, try session.replay(arena, h.workflow_id, h.history));
            }
        } else {
            const slice: []const history_mod.WorkflowHistory = histories;
            for (slice) |h| {
                if (!started) try session.init(r);
                started = true;
                try results.append(arena, try session.replay(arena, h.workflow_id, h.history));
            }
        }
        return results.toOwnedSlice(arena);
    }

    /// Fetches the history of a workflow run through `client` and replays it as
    /// `workflow_id`. An empty `run_id` reads the latest run. A replay failure is in the
    /// result's `err` and `message`; errors returned are fetch failures, or a replay worker
    /// that couldn't start (`error.ReplayFailed`, logged). The result is allocated from
    /// `arena`.
    pub fn replayWorkflowExecution(
        r: *WorkflowReplayer,
        arena: Allocator,
        client: *client_mod.Client,
        workflow_id: []const u8,
        run_id: []const u8,
    ) (client_mod.HistoryError || error{ReplayFailed})!ReplayResult {
        const history = try client.fetchHistory(arena, workflow_id, run_id);
        var session: Session = undefined;
        try session.init(r);
        defer session.deinit();
        return session.replay(arena, workflow_id, history);
    }

    pub const ReplayListedError = client_mod.ListError || client_mod.HistoryError || error{ReplayFailed};

    /// Replays every workflow run the visibility `query` matches (see `Client.listWorkflows`)
    /// through one replay worker, and returns a result for each run in listing order. It
    /// pages through the listing and fetches each run's history by its workflow ID and run
    /// ID as replay reaches it. A run that fails to replay doesn't stop the rest: its result
    /// carries its workflow ID, run ID, error and message. A listing or fetch failure ends
    /// the batch and is returned, logged with the query or the workflow and run IDs, and so
    /// is a replay worker that couldn't start (`error.ReplayFailed`). Visibility is
    /// eventually consistent: a run that closed a moment ago may not be listed yet.
    ///
    /// Only the results (their IDs and messages) are allocated from `arena`, and they grow
    /// with the number of runs. The current listing page is held with the client's
    /// allocator. One run's history at a time is held in scratch arenas on `r.gpa`, decoded
    /// and once more encoded for the replay worker, reused for each run and keeping the
    /// capacity of the largest one, so a batch holds at most the results, one page, and about
    /// twice the largest single history. The page and the histories are freed before the
    /// call returns. A listed run with no workflow task yet (a cron run waiting for its next
    /// fire, a delayed start) has run no workflow code and gets a clean result.
    pub fn replayListedWorkflows(
        r: *WorkflowReplayer,
        arena: Allocator,
        client: *client_mod.Client,
        query: []const u8,
    ) ReplayListedError![]ReplayResult {
        var executions = try client.listWorkflows(query, .{});
        defer executions.deinit();
        var results: std.ArrayList(ReplayResult) = .empty;
        var scratch: std.heap.ArenaAllocator = .init(r.gpa);
        defer scratch.deinit();
        // Created on the first run, so an empty listing starts no worker.
        var session: Session = undefined;
        var started = false;
        defer if (started) session.deinit();
        while (true) {
            _ = scratch.reset(.retain_capacity);
            const s = scratch.allocator();
            const info = executions.next(s) catch |err| {
                log.err("listing the workflows to replay for query \"{s}\" failed: {t}", .{ query, err });
                return err;
            } orelse break;
            // The listed run ID, so a run started since the listing isn't read instead.
            const history = client.fetchHistory(s, info.workflow_id, info.run_id) catch |err| {
                log.err("fetching the history of workflow {s} run {s} to replay (query \"{s}\") failed: {t}", .{ info.workflow_id, info.run_id, query, err });
                return err;
            };
            if (!started) try session.init(r);
            started = true;
            var result = try session.replay(arena, info.workflow_id, history);
            // The started event keeps the original run ID across a reset; report the listed run.
            if (!std.mem.eql(u8, result.run_id, info.run_id)) result.run_id = try arena.dupe(u8, info.run_id);
            try results.append(arena, result);
        }
        return results.toOwnedSlice(arena);
    }

    /// A replay worker with the workflow runner that answers its activations. Pinned in
    /// memory once `init` returns: the runner points at its `outcome`.
    const Session = struct {
        r: *const WorkflowReplayer,
        runtime: bridge.Runtime,
        replayer: bridge.Worker.Replayer,
        runner: worker_mod.WorkflowRunner,
        outcome: Outcome,
        /// The worker stopped polling; later histories can't replay.
        stopped: bool,

        fn init(s: *Session, r: *const WorkflowReplayer) error{ReplayFailed}!void {
            if (r.options.deadlock_detection_timeout) |d| if (d.nanoseconds <= 0) {
                log.warn("WorkflowReplayer.Options.deadlock_detection_timeout is {f}; it must be positive, or null to replay unwatched", .{d});
                return error.ReplayFailed;
            };
            var message: ?[]u8 = null;
            defer if (message) |m| r.gpa.free(m);
            const runtime = bridge.Runtime.init(r.gpa, .{}, &message) catch {
                log.err("creating the core runtime for replay failed: {s}", .{message orelse "unknown error"});
                return error.ReplayFailed;
            };
            errdefer runtime.deinit();
            const replayer = bridge.Worker.initReplayer(r.gpa, runtime, .{
                .namespace = r.options.namespace,
                .task_queue = r.options.task_queue,
                .task_types = .{ .local_activities = false, .remote_activities = false, .nexus = false },
                .max_cached_workflows = 1,
            }, &message) catch {
                log.err("creating the replay worker for namespace {s}, task queue {s} failed: {s}", .{ r.options.namespace, r.options.task_queue, message orelse "unknown error" });
                return error.ReplayFailed;
            };
            s.* = .{
                .r = r,
                .runtime = runtime,
                .replayer = replayer,
                .outcome = .{ .gpa = r.gpa },
                .stopped = false,
                .runner = .{
                    .gpa = r.gpa,
                    .workflows = &r.workflows,
                    .dynamic_workflow = r.dynamic_workflow,
                    .task_queue = r.options.task_queue,
                    .namespace = r.options.namespace,
                    .stack_size = 256 * 1024,
                    .memory_limit = r.options.workflow_memory_limit,
                    .task_failure_errors = r.options.workflow_task_failure_errors,
                    .deadlock_timeout = r.options.deadlock_detection_timeout,
                    .codecs = r.options.codecs,
                    .failure_converter = r.options.failure_converter,
                    .propagators = r.options.propagators,
                    .interceptors = r.options.interceptors,
                    .on_evict = Outcome.record,
                    .on_evict_ctx = undefined,
                },
            };
            s.runner.on_evict_ctx = &s.outcome;
        }

        fn deinit(s: *Session) void {
            const gpa = s.r.gpa;
            const io = s.r.io;
            var message: ?[]u8 = null;
            defer if (message) |m| gpa.free(m);
            s.replayer.worker.initiateShutdown();
            while (true) {
                const polled = s.replayer.worker.pollWorkflowActivation(gpa, io, &message) catch break orelse break;
                defer polled.deinit();
                s.runner.process(s.replayer.worker, io, polled.bytes());
            }
            s.replayer.worker.finalizeShutdown(gpa, io, null) catch {};
            // The core sends histories from a background task that uses the pusher, so the
            // pusher is freed only after the worker has shut down (as the .NET SDK does).
            s.replayer.finishPushing();
            s.runner.deinit();
            s.outcome.deinit();
            s.replayer.worker.deinit();
            s.runtime.deinit();
            s.* = undefined;
        }

        /// Replays one history as `workflow_id` and waits for the core to evict its run.
        /// Histories are pushed one at a time so each eviction belongs to the history just
        /// pushed.
        fn replay(s: *Session, arena: Allocator, workflow_id: []const u8, history: history_mod.History) error{OutOfMemory}!ReplayResult {
            const gpa = s.r.gpa;
            var result: ReplayResult = .{ .workflow_id = try arena.dupe(u8, workflow_id), .run_id = "" };
            // Checked before the session is touched, so a rejected history leaves it as it was.
            if (try replayProblem(arena, history)) |problem| {
                result.err = error.InvalidHistory;
                result.message = problem;
                return result;
            }
            const started = startedEvent(history) orelse unreachable; // replayProblem checked it.
            result.run_id = try arena.dupe(u8, started.original_execution_run_id);
            // A run with no workflow task scheduled yet (a cron run waiting for its next fire,
            // a delayed start) has run no workflow code, so it has nothing to check; the core
            // would never activate or evict it, and replay would wait forever.
            if (!hasWorkflowTask(history)) return result;
            if (s.stopped) {
                result.err = error.ReplayFailed;
                result.message = "the replay worker stopped while replaying an earlier history";
                return result;
            }

            var scratch: std.heap.ArenaAllocator = .init(gpa);
            defer scratch.deinit();
            var encoded: Io.Writer.Allocating = .init(scratch.allocator());
            history.encode(&encoded.writer, scratch.allocator()) catch return error.OutOfMemory;

            s.outcome.reset();
            var message: ?[]u8 = null;
            defer if (message) |m| gpa.free(m);
            s.replayer.push(gpa, workflow_id, encoded.written(), &message) catch {
                result.err = error.ReplayFailed;
                result.message = try std.fmt.allocPrint(arena, "pushing the history for replay failed: {s}", .{message orelse "unknown error"});
                return result;
            };
            // The core evicts the run when replay reaches the end of the history (or fails).
            while (!s.outcome.evicted) {
                const polled = s.replayer.worker.pollWorkflowActivation(gpa, s.r.io, &message) catch {
                    s.stopped = true;
                    result.err = error.ReplayFailed;
                    result.message = try std.fmt.allocPrint(arena, "polling the replay worker failed: {s}", .{message orelse "unknown error"});
                    return result;
                } orelse {
                    s.stopped = true;
                    result.err = error.ReplayFailed;
                    result.message = "replay ended without finishing the history";
                    return result;
                };
                defer polled.deinit();
                s.runner.process(s.replayer.worker, s.r.io, polled.bytes());
            }
            if (s.outcome.failed) {
                result.err = if (s.outcome.nondeterminism) error.Nondeterminism else error.ReplayFailed;
                result.message = try arena.dupe(u8, s.outcome.message orelse "unknown reason");
            }
            return result;
        }
    };

    fn hasWorkflowTask(history: history_mod.History) bool {
        for (history.events.items) |event| switch (event.attributes orelse continue) {
            .workflow_task_scheduled_event_attributes => return true,
            else => {},
        };
        return false;
    }

    fn startedEvent(history: history_mod.History) ?proto.temporal.api.history.v1.WorkflowExecutionStartedEventAttributes {
        if (history.events.items.len == 0) return null;
        const attrs = history.events.items[0].attributes orelse return null;
        return switch (attrs) {
            .workflow_execution_started_event_attributes => |started| started,
            else => null,
        };
    }

    /// Why the core can't replay `history`, or null when it can. The core doesn't report
    /// these: its replay poller panics or stalls on them, and the replay then never ends.
    /// This mirrors, at sdk-core c675c32 (recheck it when the submodule moves):
    /// - `HistoryInfo::new_from_history(h, None)` and `as_poll_wft_response` in
    ///   deps/sdk-core/crates/sdk-core/src/replay/history_info.rs. As there, the first event
    ///   is judged by its attributes and the others by their `event_type`, and an empty task
    ///   queue name is accepted.
    /// - `PreparedWFT::is_incremental` (worker/workflow/mod.rs): a first event ID above 1
    ///   makes the task a sticky-queue one, whose full history the core then fetches from the
    ///   replay worker's mocked client, which panics.
    /// - `HistoryPaginator::extract_next_update` (worker/workflow/history_update.rs): a last
    ///   event ID below the task's started event ID fails the poll, and the run is never
    ///   activated or evicted.
    fn replayProblem(arena: Allocator, history: history_mod.History) error{OutOfMemory}!?[]const u8 {
        const events = history.events.items;
        if (events.len == 0) return "the history has no events";
        const started = startedEvent(history) orelse
            return "the history's first event must be a WorkflowExecutionStarted event";
        if (started.workflow_type == null) return "the history's WorkflowExecutionStarted event names no workflow type";
        if (started.task_queue == null) return "the history's WorkflowExecutionStarted event names no task queue";
        if (events[0].event_id > 1) return try std.fmt.allocPrint(arena, "the history's first event has event ID {d}; a full history starts at event 1", .{events[0].event_id});

        var previous_started: i64 = 0;
        for (events, 0..) |event, i| {
            if (event.event_type != .EVENT_TYPE_WORKFLOW_TASK_STARTED) continue;
            const next: ?EventType = if (i + 1 < events.len) events[i + 1].event_type else null;
            if (next == null or next.? == .EVENT_TYPE_WORKFLOW_TASK_COMPLETED) {
                if (event.event_id == previous_started) {
                    if (previous_started == 0) return try std.fmt.allocPrint(arena, "the workflow task started event at position {d} has event ID 0", .{i + 1});
                    return try std.fmt.allocPrint(arena, "two workflow task started events have event ID {d}", .{event.event_id});
                }
                previous_started = event.event_id;
            } else switch (next.?) {
                .EVENT_TYPE_WORKFLOW_TASK_FAILED,
                .EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT,
                .EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
                .EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT,
                => {},
                else => {
                    const following = events[i + 1];
                    var unknown: [32]u8 = undefined;
                    const type_name = std.enums.tagName(EventType, following.event_type) orelse
                        std.fmt.bufPrint(&unknown, "event type {d}", .{@backingInt(following.event_type)}) catch unreachable;
                    return try std.fmt.allocPrint(arena, "workflow task started event {d} is followed by event {d} ({s}), not the task's completion, failure or timeout or the run's termination or timeout", .{ event.event_id, following.event_id, type_name });
                },
            }
        }
        // The core's task asks for events up to the last such started event.
        const last = events[events.len - 1].event_id;
        if (last < previous_started) return try std.fmt.allocPrint(arena, "the history ends at event ID {d}, before its last workflow task started event, {d}", .{ last, previous_started });
        return null;
    }

    const Outcome = struct {
        gpa: Allocator,
        evicted: bool = false,
        failed: bool = false,
        nondeterminism: bool = false,
        message: ?[]u8 = null,

        fn record(ctx: *anyopaque, reason: proto.coresdk.workflow_activation.RemoveFromCache.EvictionReason, message: []const u8) void {
            const o: *Outcome = @ptrCast(@alignCast(ctx));
            if (o.evicted) return;
            o.evicted = true;
            // As in the .NET SDK: only cache pressure and our own requests are clean endings.
            switch (reason) {
                .CACHE_FULL, .LANG_REQUESTED => {},
                else => {
                    o.failed = true;
                    o.nondeterminism = reason == .NONDETERMINISM;
                    o.message = std.fmt.allocPrint(o.gpa, "{t}: {s}", .{ reason, message }) catch null;
                },
            }
        }

        fn reset(o: *Outcome) void {
            o.deinit();
            o.* = .{ .gpa = o.gpa };
        }

        fn deinit(o: *Outcome) void {
            if (o.message) |m| o.gpa.free(m);
        }
    };
};

// ---------------------------------------------------------------- tests

const testing = std.testing;

const Fixtures = struct {
    fn greet(ctx: *activity.Context, name: []const u8) ![]const u8 {
        return std.fmt.allocPrint(ctx.allocator(), "Hello {s}!", .{name});
    }
    const Greet = activity.Def("Greet", greet);

    fn greetDouble(ctx: *activity.Context, name: []const u8) ![]const u8 {
        _ = name;
        return ctx.allocator().dupe(u8, "Hello from the test double!");
    }

    fn hello(ctx: *workflow.Context, name: []const u8) ![]const u8 {
        const greeting = ctx.executeActivity(Greet, .{ .start_to_close_timeout = .fromSeconds(10) }, .{name});
        return greeting.get();
    }
    const Hello = workflow.Def("HelloWorld", hello);

    fn other(ctx: *workflow.Context, name: []const u8) ![]const u8 {
        _ = ctx;
        _ = name;
        return "no activity here";
    }
    const Different = workflow.Def("HelloWorld", other);

    const Greeting = workflow.QueryDef("greeting", fn () []const u8);
    const GreetingState = struct {
        greeting: []const u8,
        fn current(s: *GreetingState) []const u8 {
            return s.greeting;
        }
    };
    /// `hello` with a query handler for its greeting.
    fn helloQueried(ctx: *workflow.Context, name: []const u8) ![]const u8 {
        const s = try ctx.allocator().create(GreetingState);
        s.* = .{ .greeting = "waiting" };
        try ctx.setQueryHandler(Greeting, s, GreetingState.current);
        s.greeting = try ctx.executeActivity(Greet, .{ .start_to_close_timeout = .fromSeconds(10) }, .{name}).get();
        return s.greeting;
    }
    const HelloQueried = workflow.Def("HelloWorld", helloQueried);

    const Status = workflow.QueryDef("status", fn () []const u8);
    const Add = workflow.QueryDef("add", fn (i64, i64) i64);
    const FailingQuery = workflow.QueryDef("fails", fn () i64);
    const QueryState = struct {
        phase: []const u8 = "starting",
        base: i64 = 100,

        fn status(s: *QueryState) []const u8 {
            return s.phase;
        }
        fn add(s: *QueryState, x: i64, y: i64) i64 {
            return s.base + x + y;
        }
        fn fails(_: *QueryState) error{NotReady}!i64 {
            return error.NotReady;
        }
    };
    /// Answers `Status`, `Add` and `FailingQuery` while it sleeps for `seconds`.
    fn queryable(ctx: *workflow.Context, seconds: i64) ![]const u8 {
        const s = try ctx.allocator().create(QueryState);
        s.* = .{};
        try ctx.setQueryHandler(Status, s, QueryState.status);
        try ctx.setQueryHandler(Add, s, QueryState.add);
        try ctx.setQueryHandler(FailingQuery, s, QueryState.fails);
        s.phase = "sleeping";
        try ctx.io().sleep(.fromSeconds(seconds), .awake);
        s.phase = "done";
        return "finished";
    }
    const Queryable = workflow.Def("Queryable", queryable);

    /// Waits until the run's history has a started timer (its first workflow task is done).
    fn waitForTimer(c: *client_mod.Client, id: []const u8, run_id: []const u8) !void {
        while (true) {
            var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
            defer scratch.deinit();
            const history = try c.fetchHistory(scratch.allocator(), id, run_id);
            for (history.events.items) |event| switch (event.attributes orelse continue) {
                .timer_started_event_attributes => return,
                else => {},
            };
            try testing.io.sleep(.fromMilliseconds(20), .awake);
        }
    }

    /// Sleeps, then returns how many seconds of workflow time passed.
    fn sleepFor(ctx: *workflow.Context, seconds: i64) !i64 {
        const before = ctx.now();
        try ctx.io().sleep(.fromSeconds(seconds), .awake);
        return @intCast(@divFloor(ctx.now().nanoseconds - before.nanoseconds, std.time.ns_per_s));
    }
    const Sleeper = workflow.Def("Sleeper", sleepFor);

    /// Sleeps; when canceled, runs a cleanup activity and then ends canceled.
    fn sleepThenCleanUp(ctx: *workflow.Context, seconds: i64) ![]const u8 {
        ctx.io().sleep(.fromSeconds(seconds), .awake) catch |err| switch (err) {
            error.Canceled => {
                _ = try ctx.executeActivity(Greet, .{ .start_to_close_timeout = .fromSeconds(10) }, .{"cleanup"}).get();
                return error.Canceled;
            },
        };
        return "slept";
    }
    const CleanUpOnCancel = workflow.Def("CleanUpOnCancel", sleepThenCleanUp);

    /// Upserts the memo before and after an activity; returns the start memo's status and
    /// the memo keys at the end.
    fn memoUpdates(ctx: *workflow.Context, status: []const u8) ![]const u8 {
        const a = ctx.allocator();
        const before = (try ctx.memoValue([]const u8, "status")) orelse "none";
        try ctx.upsertMemo(.{ .status = status, .tags = @as([]const []const u8, &.{ "nightly", "import" }) });
        _ = try ctx.executeActivity(Greet, .{ .start_to_close_timeout = .fromSeconds(10) }, .{status}).get();
        try ctx.upsertMemo(.{ .owner = @as(?[]const u8, null), .tags = @as([]const []const u8, &.{}), .done = true });
        var keys: std.ArrayList([]const u8) = .empty;
        for (ctx.memo()) |f| try keys.append(a, f.name);
        return std.fmt.allocPrint(a, "{s}: {s}", .{ before, try std.mem.join(a, ",", keys.items) });
    }
    const MemoUpdates = workflow.Def("MemoUpdates", memoUpdates);

    fn memoUpdatesWithoutUpserts(ctx: *workflow.Context, status: []const u8) ![]const u8 {
        _ = try ctx.executeActivity(Greet, .{ .start_to_close_timeout = .fromSeconds(10) }, .{status}).get();
        return "queued: done,note,status";
    }
    const MemoUpdatesChanged = workflow.Def("MemoUpdates", memoUpdatesWithoutUpserts);

    const order_id = search_attributes.key(.keyword, "CustomKeywordField");
    const priority = search_attributes.key(.int, "CustomIntField");
    const tags = search_attributes.key(.keyword_list, "CustomKeywordListField");
    const score = search_attributes.key(.double, "CustomDoubleField");
    const flagged = search_attributes.key(.bool, "CustomBoolField");
    const due = search_attributes.key(.datetime, "CustomDatetimeField");
    const notes = search_attributes.key(.text, "CustomTextField");
    const due_time = "2030-01-02T03:04:05.5Z";

    /// Reads its start search attributes, then upserts before and after an activity: sets
    /// every type, changes the priority, and unsets the order ID. Returns
    /// "<start order ID> <start priority>: <names set at the end>".
    fn searchAttributeUpdates(ctx: *workflow.Context, tag: []const u8) ![]const u8 {
        const a = ctx.allocator();
        const start = ctx.searchAttributes();
        const id = (try start.get(a, order_id)) orelse "none";
        const p = (try start.get(a, priority)) orelse 0;
        try ctx.upsertSearchAttributes(&.{
            priority.set(p + 1),
            tags.set(&.{ tag, "nightly" }),
            score.set(1.5),
            flagged.set(true),
            due.set(try converter.Time.parse(due_time)),
        });
        _ = try ctx.executeActivity(Greet, .{ .start_to_close_timeout = .fromSeconds(10) }, .{tag}).get();
        try ctx.upsertSearchAttributes(&.{ order_id.unset(), priority.set(p + 2), notes.set("all done") });
        var names: std.ArrayList([]const u8) = .empty;
        for (ctx.searchAttributes().fields) |f| try names.append(a, f.name);
        return std.fmt.allocPrint(a, "{s} {d}: {s}", .{ id, p, try std.mem.join(a, ",", names.items) });
    }
    const SearchAttributeUpdates = workflow.Def("SearchAttributeUpdates", searchAttributeUpdates);

    const SelfSignal = workflow.SignalDef("self", fn (value: []const u8) void);

    const SignalBox = struct {
        value: ?[]const u8 = null,
        fn set(b: *SignalBox, value: []const u8) void {
            b.value = value;
        }
        fn received(b: *SignalBox) bool {
            return b.value != null;
        }
    };

    /// Runs an activity that signals this workflow through the worker's client, then returns
    /// the signal's value.
    fn signalSelf(ctx: *workflow.Context, value: []const u8) ![]const u8 {
        const box = try ctx.allocator().create(SignalBox);
        box.* = .{};
        try ctx.setSignalHandler(SelfSignal, box, SignalBox.set);
        try ctx.executeActivity(SignalOwnWorkflow, .{ .start_to_close_timeout = .fromSeconds(10) }, .{value}).get();
        try ctx.await(box, SignalBox.received);
        return box.value.?;
    }
    const SignalSelf = workflow.Def("SignalSelf", signalSelf);

    fn signalOwnWorkflow(ctx: *activity.Context, value: []const u8) !void {
        const c = try ctx.client();
        const info = ctx.info();
        try c.signalWorkflow(SelfSignal, info.workflow_id, info.workflow_run_id, .{value});
    }
    const SignalOwnWorkflow = activity.Def("SignalOwnWorkflow", signalOwnWorkflow);

    fn searchAttributeUpdatesWithoutUpserts(ctx: *workflow.Context, tag: []const u8) ![]const u8 {
        _ = try ctx.executeActivity(Greet, .{ .start_to_close_timeout = .fromSeconds(10) }, .{tag}).get();
        return "unchanged";
    }
    const SearchAttributeUpdatesChanged = workflow.Def("SearchAttributeUpdates", searchAttributeUpdatesWithoutUpserts);

    /// Heartbeats until canceled, then returns error.Canceled.
    fn heartbeatUntilCanceled(ctx: *activity.Context) !void {
        while (true) {
            ctx.heartbeat({});
            try ctx.io().sleep(.fromMilliseconds(100), .awake);
        }
    }
    const HeartbeatUntilCanceled = activity.Def("HeartbeatUntilCanceled", heartbeatUntilCanceled);

    /// Waits on `HeartbeatUntilCanceled`; a workflow cancel ends the run after the activity's.
    fn waitCancellation(ctx: *workflow.Context) ![]const u8 {
        try ctx.executeActivity(HeartbeatUntilCanceled, .{
            .start_to_close_timeout = .fromSeconds(60),
            .heartbeat_timeout = .fromSeconds(5),
            .cancellation_type = .wait_cancellation_completed,
        }, .{}).get();
        return "not canceled";
    }
    const WaitCancellation = workflow.Def("WaitCancellation", waitCancellation);

    /// Fails its first two attempts, then greets.
    fn flakyGreet(ctx: *activity.Context, name: []const u8) ![]const u8 {
        if (ctx.info().attempt < 3) return error.NotYet;
        return std.fmt.allocPrint(ctx.allocator(), "Hello {s} on attempt {d}!", .{ name, ctx.info().attempt });
    }
    const FlakyGreet = activity.Def("FlakyGreet", flakyGreet);

    /// Runs `Greet` as a local activity, then `FlakyGreet` with a retry interval above its
    /// local retry threshold, so each retry waits on a workflow timer.
    fn localGreetings(ctx: *workflow.Context, name: []const u8) ![]const u8 {
        const first = try ctx.executeLocalActivity(Greet, .{ .start_to_close_timeout = .fromSeconds(10) }, .{name}).get();
        const second = try ctx.executeLocalActivity(FlakyGreet, .{
            .start_to_close_timeout = .fromSeconds(10),
            .retry_policy = .{ .initial_interval = .fromSeconds(1), .backoff_coefficient = 1 },
            .local_retry_threshold = .fromMilliseconds(500),
        }, .{name}).get();
        return std.fmt.allocPrint(ctx.allocator(), "{s} {s}", .{ first, second });
    }
    const LocalGreetings = workflow.Def("LocalGreetings", localGreetings);

    fn remoteGreetings(ctx: *workflow.Context, name: []const u8) ![]const u8 {
        const first = try ctx.executeLocalActivity(Greet, .{ .start_to_close_timeout = .fromSeconds(10) }, .{name}).get();
        const second = try ctx.executeActivity(FlakyGreet, .{ .start_to_close_timeout = .fromSeconds(10) }, .{name}).get();
        return std.fmt.allocPrint(ctx.allocator(), "{s} {s}", .{ first, second });
    }
    const LocalGreetingsChanged = workflow.Def("LocalGreetings", remoteGreetings);

    /// `waitCancellation` with a summary, a priority, and a versioning intent on its activity.
    fn waitCancellationDescribed(ctx: *workflow.Context) ![]const u8 {
        try ctx.executeActivity(HeartbeatUntilCanceled, .{
            .start_to_close_timeout = .fromSeconds(60),
            .heartbeat_timeout = .fromSeconds(5),
            .cancellation_type = .wait_cancellation_completed,
            .summary = "heartbeat until canceled",
            .priority = .{ .priority_key = 2, .fairness_key = "tenant-a" },
            .versioning_intent = .default,
        }, .{}).get();
        return "not canceled";
    }
    const WaitCancellationDescribed = workflow.Def("WaitCancellation", waitCancellationDescribed);

    /// `localGreetings` with a summary on each local activity.
    fn localGreetingsDescribed(ctx: *workflow.Context, name: []const u8) ![]const u8 {
        const first = try ctx.executeLocalActivity(Greet, .{ .start_to_close_timeout = .fromSeconds(10), .summary = "greet" }, .{name}).get();
        const second = try ctx.executeLocalActivity(FlakyGreet, .{
            .start_to_close_timeout = .fromSeconds(10),
            .retry_policy = .{ .initial_interval = .fromSeconds(1), .backoff_coefficient = 1 },
            .local_retry_threshold = .fromMilliseconds(500),
            .summary = "greet, eventually",
        }, .{name}).get();
        return std.fmt.allocPrint(ctx.allocator(), "{s} {s}", .{ first, second });
    }
    const LocalGreetingsDescribed = workflow.Def("LocalGreetings", localGreetingsDescribed);

    /// Runs `Greet` with a summary, a priority, and the default build's versioning intent, then
    /// as a local activity with a summary.
    fn describedGreetings(ctx: *workflow.Context, name: []const u8) ![]const u8 {
        const remote = try ctx.executeActivity(Greet, .{
            .start_to_close_timeout = .fromSeconds(10),
            .summary = "greet remotely",
            .priority = .{ .priority_key = 2, .fairness_key = "tenant-a", .fairness_weight = 1.5 },
            .versioning_intent = .default,
        }, .{name}).get();
        const local = try ctx.executeLocalActivity(Greet, .{ .start_to_close_timeout = .fromSeconds(10), .summary = "greet locally" }, .{name}).get();
        return std.fmt.allocPrint(ctx.allocator(), "{s} {s}", .{ remote, local });
    }
    const DescribedGreetings = workflow.Def("DescribedGreetings", describedGreetings);

    /// Waits on a timer only when run on an environment's task queue ("test-..."), so its
    /// history replays only with that task queue.
    fn queueDependent(ctx: *workflow.Context) ![]const u8 {
        if (!std.mem.startsWith(u8, ctx.info().task_queue, "test-")) return "no timer";
        try ctx.io().sleep(.fromMilliseconds(10), .awake);
        return "timer";
    }
    const QueueDependent = workflow.Def("QueueDependent", queueDependent);

    /// Sleeps on a timer with a summary, then races a long timer with a summary, run as a
    /// coroutine, against a short one without, and cancels the long one.
    fn summaryNaps(ctx: *workflow.Context) ![]const u8 {
        const io = ctx.io();
        try ctx.sleep(.fromMilliseconds(100), .{ .summary = "cool down" });
        var long = io.async(workflow.Context.sleep, .{ ctx, .fromSeconds(60 * 60), .{ .summary = "give up" } });
        try ctx.sleep(.fromMilliseconds(100), .{});
        long.cancel(io) catch |err| return @errorName(err);
        return "rested";
    }
    const SummaryNaps = workflow.Def("SummaryNaps", summaryNaps);

    /// `summaryNaps` without the long timer.
    fn summaryNapsChanged(ctx: *workflow.Context) ![]const u8 {
        try ctx.sleep(.fromMilliseconds(100), .{ .summary = "cool down" });
        try ctx.sleep(.fromMilliseconds(100), .{});
        return "Canceled";
    }
    const SummaryNapsChanged = workflow.Def("SummaryNaps", summaryNapsChanged);

    /// Holds a `std.Io.Mutex` across a timer while two coroutines wait to lock it, then
    /// cancels the first waiter and unlocks, so the second locks it and naps. The canceled
    /// branch naps too, so a waiter whose cancel was lost changes the commands.
    fn cancelMutexWaiter(ctx: *workflow.Context) ![]const u8 {
        const io = ctx.io();
        var mutex: std.Io.Mutex = .init;
        mutex.lockUncancelable(io);
        var canceled = io.async(lockMutex, .{ io, &mutex });
        var next = io.async(lockMutexAndNap, .{ io, &mutex });
        try io.sleep(.fromMilliseconds(100), .awake);
        const outcome = canceled.cancel(io);
        mutex.unlock(io);
        try next.await(io);
        if (outcome) |_| return "locked" else |err| {
            try io.sleep(.fromMilliseconds(100), .awake);
            return @errorName(err);
        }
    }
    const CancelMutexWaiter = workflow.Def("CancelMutexWaiter", cancelMutexWaiter);

    fn lockMutex(io: std.Io, mutex: *std.Io.Mutex) std.Io.Cancelable!void {
        try mutex.lock(io);
        mutex.unlock(io);
    }

    fn lockMutexAndNap(io: std.Io, mutex: *std.Io.Mutex) std.Io.Cancelable!void {
        mutex.lockUncancelable(io);
        defer mutex.unlock(io);
        try io.sleep(.fromMilliseconds(100), .awake);
    }

    /// `cancelMutexWaiter` without the canceled branch's nap.
    fn cancelMutexWaiterChanged(ctx: *workflow.Context) ![]const u8 {
        const io = ctx.io();
        var mutex: std.Io.Mutex = .init;
        mutex.lockUncancelable(io);
        var canceled = io.async(lockMutex, .{ io, &mutex });
        var next = io.async(lockMutexAndNap, .{ io, &mutex });
        try io.sleep(.fromMilliseconds(100), .awake);
        const outcome = canceled.cancel(io);
        mutex.unlock(io);
        try next.await(io);
        if (outcome) |_| return "locked" else |err| return @errorName(err);
    }
    const CancelMutexWaiterChanged = workflow.Def("CancelMutexWaiter", cancelMutexWaiterChanged);
};

const SideEffectFixtures = struct {
    /// How many times `issue` has run, in any workflow: replay must not move it.
    var issued: u32 = 0;

    const Ticket = struct { number: u32, label: []const u8 };

    fn issue(base: u32) Ticket {
        issued += 1;
        return .{ .number = base + issued, .label = "ticket" };
    }

    /// Records a ticket with a side effect, then waits on a timer so the run spans more than
    /// one workflow task.
    fn takeTicket(ctx: *workflow.Context, base: u32) ![]const u8 {
        const ticket = try ctx.sideEffect(Ticket, base, issue);
        try ctx.io().sleep(.fromMilliseconds(10), .awake);
        return std.fmt.allocPrint(ctx.allocator(), "{s} {d}", .{ ticket.label, ticket.number });
    }
    const TakeTicket = workflow.Def("TakeTicket", takeTicket);

    fn takeTicketDirectly(ctx: *workflow.Context, base: u32) ![]const u8 {
        const ticket = issue(base);
        try ctx.io().sleep(.fromMilliseconds(10), .awake);
        return std.fmt.allocPrint(ctx.allocator(), "{s} {d}", .{ ticket.label, ticket.number });
    }
    const TakeTicketChanged = workflow.Def("TakeTicket", takeTicketDirectly);
};

const SignalFixtures = struct {
    const Approve = workflow.SignalDef("approve", fn (approver: []const u8) void);

    const Approvals = struct {
        ctx: *workflow.Context,
        greetings: std.ArrayList([]const u8) = .empty,
        received: usize = 0,

        /// Greets the approver with an activity, so the handler's commands are in history.
        fn approve(s: *Approvals, approver: []const u8) !void {
            s.received += 1;
            const greeting = try s.ctx.executeActivity(Fixtures.Greet, .{ .start_to_close_timeout = .fromSeconds(10) }, .{approver}).get();
            try s.greetings.append(s.ctx.allocator(), greeting);
        }
        fn approveWithoutActivity(s: *Approvals, approver: []const u8) !void {
            s.received += 1;
            try s.greetings.append(s.ctx.allocator(), approver);
        }
        fn enough(s: *Approvals) bool {
            return s.received >= 2;
        }
    };

    /// Sleeps 2 s before registering its handler, so a signal sent at once is buffered; then
    /// waits for two approvals and for their handlers to finish.
    fn collect(ctx: *workflow.Context, comptime handler: anytype) ![]const u8 {
        const s = try ctx.allocator().create(Approvals);
        s.* = .{ .ctx = ctx };
        try ctx.io().sleep(.fromSeconds(2), .awake);
        try ctx.setSignalHandler(Approve, s, handler);
        try ctx.await(s, Approvals.enough);
        try ctx.await(ctx, workflow.Context.allHandlersFinished);
        return std.mem.join(ctx.allocator(), " ", s.greetings.items);
    }
    fn collectApprovals(ctx: *workflow.Context) ![]const u8 {
        return collect(ctx, Approvals.approve);
    }
    const CollectApprovals = workflow.Def("CollectApprovals", collectApprovals);
    fn collectApprovalsWithoutActivity(ctx: *workflow.Context) ![]const u8 {
        return collect(ctx, Approvals.approveWithoutActivity);
    }
    const CollectApprovalsChanged = workflow.Def("CollectApprovals", collectApprovalsWithoutActivity);

    const Approvers = struct {
        names: std.ArrayList([]const u8) = .empty,
        wanted: usize,
        ctx: *workflow.Context,

        fn approve(s: *Approvers, approver: []const u8) !void {
            try s.names.append(s.ctx.allocator(), approver);
        }
        fn enough(s: *Approvers) bool {
            return s.names.items.len >= s.wanted;
        }
    };
    /// Waits for `count` approvals and returns the approvers in the order they arrived.
    fn awaitApprovals(ctx: *workflow.Context, count: u32) ![]const u8 {
        const s = try ctx.allocator().create(Approvers);
        s.* = .{ .wanted = count, .ctx = ctx };
        try ctx.setSignalHandler(Approve, s, Approvers.approve);
        try ctx.await(s, Approvers.enough);
        return std.mem.join(ctx.allocator(), " ", s.names.items);
    }
    const AwaitApprovals = workflow.Def("AwaitApprovals", awaitApprovals);

    const Race = union(enum) { approval: error{Canceled}![]const u8, timeout: Io.Cancelable!void };
    /// Sleeps 2 s before opening a channel for approvals, so a signal sent at once is
    /// buffered; then receives two, each raced against a one-minute timer, and greets each
    /// approver with an activity (or, `greet` unset, keeps the name).
    fn receive(ctx: *workflow.Context, comptime greet: bool) ![]const u8 {
        const io = ctx.io();
        try io.sleep(.fromSeconds(2), .awake);
        const approvals = try ctx.signalChannel(Approve);
        var greetings: std.ArrayList([]const u8) = .empty;
        while (greetings.items.len < 2) {
            var buf: [2]Race = undefined;
            var select: Io.Select(Race) = .init(io, &buf);
            select.async(.approval, workflow.SignalChannel(Approve.Args).receive, .{approvals});
            select.async(.timeout, Io.sleep, .{ io, .fromSeconds(60), .awake });
            const first = try select.await();
            select.cancelDiscard();
            const greeting = switch (first) {
                .approval => |r| if (greet)
                    try ctx.executeActivity(Fixtures.Greet, .{ .start_to_close_timeout = .fromSeconds(10) }, .{try r}).get()
                else
                    try r,
                .timeout => |r| blk: {
                    try r;
                    break :blk "nobody";
                },
            };
            try greetings.append(ctx.allocator(), greeting);
        }
        return std.mem.join(ctx.allocator(), " ", greetings.items);
    }
    fn receiveApprovals(ctx: *workflow.Context) ![]const u8 {
        return receive(ctx, true);
    }
    const ReceiveApprovals = workflow.Def("ReceiveApprovals", receiveApprovals);
    fn receiveApprovalsWithoutActivity(ctx: *workflow.Context) ![]const u8 {
        return receive(ctx, false);
    }
    const ReceiveApprovalsChanged = workflow.Def("ReceiveApprovals", receiveApprovalsWithoutActivity);
};

const UpdateFixtures = struct {
    const Set = workflow.UpdateDef("set", fn (value: i64) i64);
    const Greet = workflow.UpdateDef("greet", fn (name: []const u8) []const u8);
    const Done = workflow.SignalDef("done", fn () void);

    const State = struct {
        ctx: *workflow.Context,
        value: i64 = 0,
        greetings: std.ArrayList([]const u8) = .empty,
        done: bool = false,

        /// Returns the previous value.
        fn set(s: *State, value: i64) i64 {
            defer s.value = value;
            return s.value;
        }
        fn positive(_: *State, value: i64) error{NotPositive}!void {
            if (value <= 0) return error.NotPositive;
        }
        /// Greets with an activity, so the handler's commands are in history.
        fn greet(s: *State, name: []const u8) ![]const u8 {
            const greeting = try s.ctx.executeActivity(Fixtures.Greet, .{ .start_to_close_timeout = .fromSeconds(10) }, .{name}).get();
            try s.greetings.append(s.ctx.allocator(), greeting);
            return greeting;
        }
        fn greetWithoutActivity(s: *State, name: []const u8) ![]const u8 {
            try s.greetings.append(s.ctx.allocator(), name);
            return name;
        }
        fn finish(s: *State) void {
            s.done = true;
        }
        fn finished(s: *State) bool {
            return s.done;
        }
    };

    /// Handles updates until a `done` signal and their handlers have finished; returns the
    /// value and the greetings.
    fn handle(ctx: *workflow.Context, comptime greet: anytype) ![]const u8 {
        const s = try ctx.allocator().create(State);
        s.* = .{ .ctx = ctx };
        try ctx.setUpdateHandlerWithValidator(Set, s, State.set, State.positive);
        try ctx.setUpdateHandler(Greet, s, greet);
        try ctx.setSignalHandler(Done, s, State.finish);
        try ctx.await(s, State.finished);
        try ctx.await(ctx, workflow.Context.allHandlersFinished);
        return std.fmt.allocPrint(ctx.allocator(), "{d} {s}", .{ s.value, try std.mem.join(ctx.allocator(), " ", s.greetings.items) });
    }
    fn handleUpdates(ctx: *workflow.Context) ![]const u8 {
        return handle(ctx, State.greet);
    }
    const HandleUpdates = workflow.Def("HandleUpdates", handleUpdates);
    fn handleUpdatesWithoutActivity(ctx: *workflow.Context) ![]const u8 {
        return handle(ctx, State.greetWithoutActivity);
    }
    const HandleUpdatesChanged = workflow.Def("HandleUpdates", handleUpdatesWithoutActivity);

    /// Fails unless `ctx.currentUpdate()` names update `name`.
    fn expectUpdate(s: *State, name: []const u8) error{WrongUpdate}!void {
        const u = s.ctx.currentUpdate() orelse return error.WrongUpdate;
        if (!std.mem.eql(u8, u.name, name) or u.id.len == 0) return error.WrongUpdate;
    }
    fn setChecked(s: *State, value: i64) !i64 {
        try expectUpdate(s, "set");
        return s.set(value);
    }
    fn positiveChecked(s: *State, value: i64) !void {
        try expectUpdate(s, "set");
        return s.positive(value);
    }
    /// Checks the update before and after the activity it waits for.
    fn greetChecked(s: *State, name: []const u8) ![]const u8 {
        try expectUpdate(s, "greet");
        const greeting = try s.greet(name);
        try expectUpdate(s, "greet");
        return greeting;
    }
    /// `handleUpdates` with handlers that fail unless they see their own update.
    fn handleUpdatesChecked(ctx: *workflow.Context) ![]const u8 {
        const s = try ctx.allocator().create(State);
        s.* = .{ .ctx = ctx };
        try ctx.setUpdateHandlerWithValidator(Set, s, setChecked, positiveChecked);
        try ctx.setUpdateHandler(Greet, s, greetChecked);
        try ctx.setSignalHandler(Done, s, State.finish);
        try ctx.await(s, State.finished);
        if (ctx.currentUpdate() != null) return error.WrongUpdate;
        try ctx.await(ctx, workflow.Context.allHandlersFinished);
        return std.fmt.allocPrint(ctx.allocator(), "{d} {s}", .{ s.value, try std.mem.join(ctx.allocator(), " ", s.greetings.items) });
    }
    const HandleUpdatesChecked = workflow.Def("HandleUpdates", handleUpdatesChecked);

    /// Handles `set` with its own handler and `greet` with the dynamic update handler, whose
    /// validator rejects every other update; otherwise like `handleUpdates`.
    fn handleDynamic(ctx: *workflow.Context, comptime greet: anytype) ![]const u8 {
        const s = try ctx.allocator().create(State);
        s.* = .{ .ctx = ctx };
        const Dynamic = struct {
            fn handle(st: *State, name: []const u8, args: converter.EncodedValues) ![]const u8 {
                try expectUpdate(st, name);
                return greet(st, try args.get([]const u8, st.ctx.allocator(), 0));
            }
            fn check(st: *State, name: []const u8, args: converter.EncodedValues) !void {
                try expectUpdate(st, name);
                if (!std.mem.eql(u8, name, "greet")) return error.UnknownUpdate;
                _ = try args.get([]const u8, st.ctx.allocator(), 0);
            }
        };
        try ctx.setUpdateHandlerWithValidator(Set, s, State.set, State.positive);
        try ctx.setDynamicUpdateHandlerWithValidator(s, Dynamic.handle, Dynamic.check);
        try ctx.setSignalHandler(Done, s, State.finish);
        try ctx.await(s, State.finished);
        try ctx.await(ctx, workflow.Context.allHandlersFinished);
        return std.fmt.allocPrint(ctx.allocator(), "{d} {s}", .{ s.value, try std.mem.join(ctx.allocator(), " ", s.greetings.items) });
    }
    fn handleDynamicUpdates(ctx: *workflow.Context) ![]const u8 {
        return handleDynamic(ctx, State.greet);
    }
    const HandleDynamicUpdates = workflow.Def("HandleDynamicUpdates", handleDynamicUpdates);
    fn handleDynamicUpdatesWithoutActivity(ctx: *workflow.Context) ![]const u8 {
        return handleDynamic(ctx, State.greetWithoutActivity);
    }
    const HandleDynamicUpdatesChanged = workflow.Def("HandleDynamicUpdates", handleDynamicUpdatesWithoutActivity);

    const Nap = workflow.UpdateDef("nap", fn (seconds: i64) i64);
    /// Sleeps in the handler; returns how many seconds of workflow time passed.
    fn nap(s: *State, seconds: i64) !i64 {
        const before = s.ctx.now();
        try s.ctx.io().sleep(.fromSeconds(seconds), .awake);
        return @intCast(@divFloor(s.ctx.now().nanoseconds - before.nanoseconds, std.time.ns_per_s));
    }
    /// Handles `Nap` updates until a `done` signal and their handlers have finished.
    fn napper(ctx: *workflow.Context) !void {
        const s = try ctx.allocator().create(State);
        s.* = .{ .ctx = ctx };
        try ctx.setUpdateHandler(Nap, s, nap);
        try ctx.setSignalHandler(Done, s, State.finish);
        try ctx.await(s, State.finished);
        try ctx.await(ctx, workflow.Context.allHandlersFinished);
    }
    const Napper = workflow.Def("Napper", napper);
};

/// A workflow whose `pass` update handler blocks until the `open` signal, so a client's
/// wait on the update outlasts any check the test makes.
const GateFixtures = struct {
    const Pass = workflow.UpdateDef("pass", fn (name: []const u8) []const u8);
    const Open = workflow.SignalDef("open", fn () void);
    const Waiting = workflow.QueryDef("waiting", fn () i64);

    const State = struct {
        ctx: *workflow.Context,
        open: bool = false,
        waiting: i64 = 0,

        fn pass(s: *State, name: []const u8) ![]const u8 {
            s.waiting += 1;
            try s.ctx.await(s, isOpen);
            return name;
        }
        fn isOpen(s: *State) bool {
            return s.open;
        }
        fn openGate(s: *State) void {
            s.open = true;
        }
        fn waitingCount(s: *State) i64 {
            return s.waiting;
        }
    };

    /// Ends once the gate is open and the updates waiting at it have passed; returns how
    /// many waited.
    fn gate(ctx: *workflow.Context) !i64 {
        const s = try ctx.allocator().create(State);
        s.* = .{ .ctx = ctx };
        try ctx.setUpdateHandler(Pass, s, State.pass);
        try ctx.setSignalHandler(Open, s, State.openGate);
        try ctx.setQueryHandler(Waiting, s, State.waitingCount);
        try ctx.await(s, State.isOpen);
        try ctx.await(ctx, workflow.Context.allHandlersFinished);
        return s.waiting;
    }
    const Gate = workflow.Def("Gate", gate);

    /// The calls whose wait the tests end early. Each owns what it allocates, so a canceled
    /// one leaks nothing.
    fn update(c: *client_mod.Client, id: []const u8, update_id: []const u8) client_mod.UpdateError!void {
        var handle = try c.updateWorkflow(Pass, id, "", .{"ann"}, .{ .update_id = update_id });
        handle.deinit();
    }
    fn updateWithStart(c: *client_mod.Client, task_queue: []const u8, id: []const u8, update_id: []const u8) client_mod.UpdateWithStartError!void {
        var handle = try c.updateWithStartWorkflow(Gate, .{ .id = id, .task_queue = task_queue, .id_conflict_policy = .use_existing }, .{}, Pass, .{"ann"}, .{ .update_id = update_id });
        handle.deinit();
    }
    fn get(c: *client_mod.Client, id: []const u8, update_id: []const u8) client_mod.UpdateResultError!void {
        var handle = try c.getWorkflowUpdateHandle([]const u8, id, "", update_id);
        defer handle.deinit();
        var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
        defer scratch.deinit();
        _ = try handle.get(scratch.allocator());
    }

    const Race = union(enum) { update: anyerror!void, timeout: Io.Cancelable!void };

    const Call = enum { update, update_with_start, get };

    /// Waits on update `pass` of a new gate workflow through `call`, ends the wait once the
    /// handler is running (by canceling the waiting task, or, `bounded`, by a 100 ms timeout
    /// that wins the race and then cancels it), then opens the gate and reads the update's
    /// result through a new handle with the same update ID.
    fn endWaitEarly(env: *WorkflowEnvironment, call: Call, bounded: bool) !void {
        const c = &env.client;
        var id_buf: [64]u8 = undefined;
        const id = try std.fmt.bufPrint(&id_buf, "gate-{t}-{s}-{s}", .{ call, if (bounded) "bounded" else "canceled", env.task_queue });
        const update_id = "pass-ann";
        try env.ensureWorker();
        if (call != .update_with_start) {
            var run = try env.startWorkflow(Gate, .{ .id = id, .task_queue = "" }, .{});
            run.deinit();
        }
        if (call == .get) {
            var accepted = try c.updateWorkflow(Pass, id, "", .{"ann"}, .{ .update_id = update_id, .wait_for_stage = .accepted });
            accepted.deinit();
        }

        var buffer: [2]Race = undefined;
        var select: Io.Select(Race) = .init(testing.io, &buffer);
        defer select.cancelDiscard();
        switch (call) {
            .update => try select.concurrent(.update, update, .{ c, id, update_id }),
            .update_with_start => try select.concurrent(.update, updateWithStart, .{ c, env.task_queue, id, update_id }),
            .get => try select.concurrent(.update, get, .{ c, id, update_id }),
        }
        // The update must reach the workflow before the wait ends, or there would be no
        // update left running to read.
        while (true) {
            var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
            defer scratch.deinit();
            const waiting = c.queryWorkflow(Waiting, scratch.allocator(), id, "", .{}) catch |err| switch (err) {
                error.NotFound => 0, // update-with-start hasn't started the workflow yet
                else => |e| return e,
            };
            if (waiting > 0) break;
            try testing.io.sleep(.fromMilliseconds(20), .awake);
        }

        const start = Io.Clock.awake.now(testing.io);
        if (bounded) {
            try select.concurrent(.timeout, Io.sleep, .{ testing.io, .fromMilliseconds(100), .awake });
            const first = try select.await();
            try testing.expect(first == .timeout);
            try first.timeout;
        }
        const ended = select.cancel() orelse return error.TestUnexpectedResult;
        try testing.expectError(error.Canceled, ended.update);
        const waited = start.durationTo(Io.Clock.awake.now(testing.io));
        try testing.expect(waited.nanoseconds < 5 * std.time.ns_per_s);

        // Ending the wait left the update running in the workflow.
        var arena: std.heap.ArenaAllocator = .init(testing.allocator);
        defer arena.deinit();
        try testing.expectEqual(@as(i64, 1), try c.queryWorkflow(Waiting, arena.allocator(), id, "", .{}));
        try c.signalWorkflow(Open, id, "", .{});
        var again = try c.getWorkflowUpdateHandle([]const u8, id, "", update_id);
        defer again.deinit();
        try testing.expectEqualStrings("ann", try again.get(arena.allocator()));
        var run = try c.getWorkflow(i64, id, "");
        defer run.deinit();
        try testing.expectEqual(@as(i64, 1), try env.getResult(Gate, &run, arena.allocator(), null));
    }
};

// Struct workflows: handlers declared on the workflow type.
const StructFixtures = struct {
    /// `UpdateFixtures.HandleUpdates` with its handlers declared instead of registered from
    /// the workflow function; it replays the same history.
    const HandleUpdates = struct {
        ctx: *workflow.Context = undefined,
        value: i64 = 0,
        greetings: std.ArrayList([]const u8) = .empty,
        done: bool = false,

        pub const updates = .{ .set = .{ UpdateFixtures.Set, .positive }, .greet = UpdateFixtures.Greet };
        pub const signals = .{ .finish = UpdateFixtures.Done };

        pub fn run(h: *HandleUpdates, ctx: *workflow.Context) ![]const u8 {
            h.ctx = ctx;
            try ctx.await(h, finished);
            try ctx.await(ctx, workflow.Context.allHandlersFinished);
            return std.fmt.allocPrint(ctx.allocator(), "{d} {s}", .{ h.value, try std.mem.join(ctx.allocator(), " ", h.greetings.items) });
        }
        /// Returns the previous value.
        pub fn set(h: *HandleUpdates, value: i64) i64 {
            defer h.value = value;
            return h.value;
        }
        pub fn positive(_: *HandleUpdates, value: i64) error{NotPositive}!void {
            if (value <= 0) return error.NotPositive;
        }
        pub fn greet(h: *HandleUpdates, name: []const u8) ![]const u8 {
            const greeting = try h.ctx.executeActivity(Fixtures.Greet, .{ .start_to_close_timeout = .fromSeconds(10) }, .{name}).get();
            try h.greetings.append(h.ctx.allocator(), greeting);
            return greeting;
        }
        pub fn finish(h: *HandleUpdates) void {
            h.done = true;
        }
        fn finished(h: *HandleUpdates) bool {
            return h.done;
        }
    };
    const HandleUpdatesWf = workflow.Def("HandleUpdates", HandleUpdates);

    const Record = workflow.SignalDef("record", fn (name: []const u8) void);
    const Close = workflow.SignalDef("close", fn () void);
    const Count = workflow.QueryDef("count", fn () usize);
    const SetLimit = workflow.UpdateDef("set_limit", fn (limit: i64) i64);

    /// Records greetings for the names it is signaled, each from an activity (or, changed,
    /// without one), until `close`; returns them and its limit. The validator of `set_limit`
    /// rejects a limit that isn't positive.
    fn LedgerOf(comptime greet_with_activity: bool) type {
        return struct {
            const State = @This();
            ctx: *workflow.Context = undefined,
            entries: std.ArrayList([]const u8) = .empty,
            limit: i64 = 10,
            closed: bool = false,

            pub const queries = .{ .count = Count };
            pub const signals = .{ .record = Record, .close = Close };
            pub const updates = .{ .setLimit = .{ SetLimit, .checkLimit } };

            pub fn run(l: *State, ctx: *workflow.Context, owner: []const u8) ![]const u8 {
                l.ctx = ctx;
                try ctx.await(l, isClosed);
                try ctx.await(ctx, workflow.Context.allHandlersFinished);
                const entries = try std.mem.join(ctx.allocator(), ", ", l.entries.items);
                return std.fmt.allocPrint(ctx.allocator(), "{s}: {s} (limit {d})", .{ owner, entries, l.limit });
            }
            pub fn record(l: *State, name: []const u8) !void {
                const entry = if (greet_with_activity)
                    try l.ctx.executeActivity(Fixtures.Greet, .{ .start_to_close_timeout = .fromSeconds(10) }, .{name}).get()
                else
                    name;
                try l.entries.append(l.ctx.allocator(), entry);
            }
            pub fn count(l: *State) usize {
                return l.entries.items.len;
            }
            /// Returns the previous limit.
            pub fn setLimit(l: *State, limit: i64) i64 {
                defer l.limit = limit;
                return l.limit;
            }
            pub fn checkLimit(_: *State, limit: i64) error{LimitNotPositive}!void {
                if (limit <= 0) return error.LimitNotPositive;
            }
            pub fn close(l: *State) void {
                l.closed = true;
            }
            fn isClosed(l: *State) bool {
                return l.closed;
            }
        };
    }
    const Ledger = workflow.Def("Ledger", LedgerOf(true));
    const LedgerChanged = workflow.Def("Ledger", LedgerOf(false));
};

// An update handler whose local activity fails, and one that calls ctx.fail.
const UpdateFailureFixtures = struct {
    fn chargeActivity(ctx: *activity.Context, amount: i64) !i64 {
        if (amount <= 0) return ctx.fail(.{ .message = "invalid Amount", .type = "InvalidAmount", .non_retryable = true });
        return amount;
    }
    const ChargeActivity = activity.Def("Charge", chargeActivity);

    const Charge = workflow.UpdateDef("charge", fn (amount: i64) i64);
    const Refuse = workflow.UpdateDef("refuse", fn () void);

    const State = struct {
        ctx: *workflow.Context,
        done: bool = false,

        fn charge(s: *State, amount: i64) !i64 {
            return s.ctx.executeLocalActivity(ChargeActivity, .{ .start_to_close_timeout = .fromSeconds(10) }, .{amount}).get();
        }
        fn refuse(s: *State) error{ApplicationFailure}!void {
            return s.ctx.fail(.{ .message = "refused", .type = "Refusal", .non_retryable = true });
        }
        fn finish(s: *State) void {
            s.done = true;
        }
        fn finished(s: *State) bool {
            return s.done;
        }
    };

    fn payments(ctx: *workflow.Context) !void {
        const s = try ctx.allocator().create(State);
        s.* = .{ .ctx = ctx };
        try ctx.setUpdateHandler(Charge, s, State.charge);
        try ctx.setUpdateHandler(Refuse, s, State.refuse);
        try ctx.setSignalHandler(UpdateFixtures.Done, s, State.finish);
        try ctx.await(s, State.finished);
        try ctx.await(ctx, workflow.Context.allHandlersFinished);
    }
    const Payments = workflow.Def("Payments", payments);
};

const FailWithFixtures = struct {
    fn charge(ctx: *activity.Context) !void {
        return ctx.fail(.{ .message = "card declined", .type = "Declined", .non_retryable = true });
    }
    const Charge = activity.Def("FailWithCharge", charge);

    fn release(ctx: *activity.Context) !void {
        return ctx.fail(.{ .message = "release failed", .type = "ReleaseFailed", .non_retryable = true });
    }
    const Release = activity.Def("FailWithRelease", release);

    const opts: workflow.ActivityOptions = .{ .start_to_close_timeout = .fromSeconds(10) };

    /// Charges, and when the charge fails releases the hold, which fails too; fails with the
    /// charge's failure.
    fn order(ctx: *workflow.Context) !void {
        const charged = ctx.executeActivity(Charge, opts, .{});
        charged.get() catch |err| {
            ctx.executeActivity(Release, opts, .{}).get() catch {};
            return switch (err) {
                error.ActivityFailed => ctx.failWith(charged.failure().?),
                else => err,
            };
        };
    }
    const Order = workflow.Def("FailWithOrder", order);

    /// `order` without the release.
    fn orderChanged(ctx: *workflow.Context) !void {
        const charged = ctx.executeActivity(Charge, opts, .{});
        charged.get() catch |err| return switch (err) {
            error.ActivityFailed => ctx.failWith(charged.failure().?),
            else => err,
        };
    }
    const OrderChanged = workflow.Def("FailWithOrder", orderChanged);
};

const ChildFixtures = struct {
    fn greetChild(ctx: *workflow.Context, name: []const u8) ![]const u8 {
        return std.fmt.allocPrint(ctx.allocator(), "Hello {s} from a child!", .{name});
    }
    const GreetChild = workflow.Def("GreetChild", greetChild);

    fn greetChildDouble(ctx: *workflow.Context, name: []const u8) ![]const u8 {
        _ = ctx;
        _ = name;
        return "Hello from the child's test double!";
    }

    /// Runs `GreetChild` under a generated ID and returns its greeting once it started.
    fn parent(ctx: *workflow.Context, name: []const u8) ![]const u8 {
        const child = ctx.executeChildWorkflow(GreetChild, .{}, .{name});
        const exec = try child.started();
        if (exec.workflow_id.len == 0 or exec.run_id.len == 0) return error.MissingChildExecution;
        return child.get();
    }
    const Parent = workflow.Def("Parent", parent);

    /// Runs `GreetChild` under an ID derived from its own, so replay needs the real ID.
    fn parentOfNamed(ctx: *workflow.Context, name: []const u8) ![]const u8 {
        const child_id = try std.fmt.allocPrint(ctx.allocator(), "{s}-child", .{ctx.info().workflow_id});
        return ctx.executeChildWorkflow(GreetChild, .{ .id = child_id }, .{name}).get();
    }
    const ParentOfNamed = workflow.Def("ParentOfNamed", parentOfNamed);

    /// Runs `GreetChild` as "described-child" with a static summary and details. The ID is
    /// fixed so the replayer, which runs the parent under its own ID, starts the same child.
    fn describedParent(ctx: *workflow.Context, name: []const u8) ![]const u8 {
        return ctx.executeChildWorkflow(GreetChild, .{
            .id = "described-child",
            .static_summary = "Greet the guest",
            .static_details = "Greets **one** guest by name.",
        }, .{name}).get();
    }
    const DescribedParent = workflow.Def("DescribedParent", describedParent);

    /// `describedParent` without the summary and details: they aren't replayed, so it replays
    /// `DescribedParent`'s history.
    fn describedParentPlain(ctx: *workflow.Context, name: []const u8) ![]const u8 {
        return ctx.executeChildWorkflow(GreetChild, .{ .id = "described-child" }, .{name}).get();
    }
    const DescribedParentPlain = workflow.Def("DescribedParent", describedParentPlain);

    /// `parent` starting a different child type, which can't replay `Parent`'s history.
    fn parentOfOther(ctx: *workflow.Context, name: []const u8) ![]const u8 {
        return ctx.executeChildWorkflowByName([]const u8, "OtherChild", .{}, .{name}).get();
    }
    const ParentChanged = workflow.Def("Parent", parentOfOther);

    fn failChild(ctx: *workflow.Context) ![]const u8 {
        return ctx.fail(.{ .message = "the child gave up", .type = "ChildGaveUp", .non_retryable = true });
    }
    const FailChild = workflow.Def("FailChild", failChild);

    /// Runs `FailChild`; with `handle`, describes its failure, otherwise fails with it.
    fn parentOfFailing(ctx: *workflow.Context, handle: bool) ![]const u8 {
        const child = ctx.executeChildWorkflow(FailChild, .{}, .{});
        const result = child.get() catch |err| switch (err) {
            error.ChildWorkflowFailed => if (handle) {
                const f = child.failure().?;
                const root = failure_mod.rootCause(f);
                return std.fmt.allocPrint(ctx.allocator(), "{t}: {s} ({s}) caused \"{s}\"", .{ err, f.message, failure_mod.applicationType(root.*).?, root.message });
            } else return err,
            else => return err,
        };
        return result;
    }
    const ParentOfFailing = workflow.Def("ParentOfFailing", parentOfFailing);

    fn cronChild(ctx: *workflow.Context) !void {
        _ = ctx;
    }
    const CronChild = workflow.Def("CronChild", cronChild);

    /// Starts an hourly cron child, sleeps two hours, and returns whether the child's future
    /// is still pending: a cron child resolves only when its schedule ends.
    fn cronParent(ctx: *workflow.Context) !bool {
        const child = ctx.executeChildWorkflow(CronChild, .{ .cron_schedule = "0 * * * *", .run_timeout = .fromSeconds(60) }, .{});
        try ctx.io().sleep(.fromSeconds(2 * std.time.s_per_hour), .awake);
        return !child.isReady();
    }
    const CronParent = workflow.Def("CronParent", cronParent);
};

const LineageFixtures = struct {
    /// Starts itself as a child `depth` times, each child under its parent's ID plus "-c".
    /// Each level reports "<parent ID>/<root ID>" ("-" for null), its own first, joined by
    /// ";" with its child's report.
    fn describe(ctx: *workflow.Context, depth: u32) ![]const u8 {
        const a = ctx.allocator();
        const info = ctx.info();
        for ([_]?workflow.WorkflowExecution{ info.parent, info.root }) |maybe| if (maybe) |e| {
            if (!std.mem.eql(u8, e.namespace, info.namespace) or e.run_id.len == 0) return error.IncompleteExecution;
        };
        const own = try std.fmt.allocPrint(a, "{s}/{s}", .{
            if (info.parent) |p| p.workflow_id else "-",
            if (info.root) |r| r.workflow_id else "-",
        });
        if (depth == 0) return own;
        const child_id = try std.fmt.allocPrint(a, "{s}-c", .{info.workflow_id});
        const child = try ctx.executeChildWorkflow(Describe, .{ .id = child_id }, .{depth - 1}).get();
        return std.fmt.allocPrint(a, "{s};{s}", .{ own, child });
    }
    const Describe = workflow.Def("DescribeLineage", describe);
};

const ExternalFixtures = struct {
    const Approve = workflow.SignalDef("approve", fn (approver: []const u8) void);

    const State = struct {
        approver: ?[]const u8 = null,
        fn approve(s: *State, approver: []const u8) void {
            s.approver = approver;
        }
        fn approved(s: *State) bool {
            return s.approver != null;
        }
    };

    /// Waits for an approval and returns the approver.
    fn waitForApproval(ctx: *workflow.Context) ![]const u8 {
        const s = try ctx.allocator().create(State);
        s.* = .{};
        try ctx.setSignalHandler(Approve, s, State.approve);
        try ctx.await(s, State.approved);
        return s.approver.?;
    }
    const WaitForApproval = workflow.Def("WaitForApproval", waitForApproval);

    /// Approves the latest run of `workflow_id` as "ann" and returns "delivered", or the
    /// error and the failure's message.
    fn approveOther(ctx: *workflow.Context, workflow_id: []const u8) ![]const u8 {
        const f = ctx.signalExternalWorkflow(Approve, workflow_id, "", .{"ann"});
        f.get() catch |err| switch (err) {
            error.ExternalWorkflowNotFound => return std.fmt.allocPrint(ctx.allocator(), "{t}: {s}", .{ err, f.failure().?.message }),
            else => return err,
        };
        return "delivered";
    }
    const ApproveOther = workflow.Def("ApproveOther", approveOther);

    /// Waits for its parent's approval, then sends it back to the parent's run.
    fn echoChild(ctx: *workflow.Context) !void {
        const approver = try waitForApproval(ctx);
        const parent = ctx.info().parent orelse return error.NoParent;
        const echoed = try std.fmt.allocPrint(ctx.allocator(), "{s} via the child", .{approver});
        try ctx.signalExternalWorkflow(Approve, parent.workflow_id, parent.run_id, .{echoed}).get();
    }
    const EchoChild = workflow.Def("EchoChild", echoChild);

    /// Approves `EchoChild` through its future and returns what it sends back.
    fn pingPong(ctx: *workflow.Context) ![]const u8 {
        const s = try ctx.allocator().create(State);
        s.* = .{};
        try ctx.setSignalHandler(Approve, s, State.approve);
        const child = ctx.executeChildWorkflow(EchoChild, .{ .id = "echo-child" }, .{});
        try (try child.signal(Approve, .{"ann"})).get();
        try ctx.await(s, State.approved);
        try child.get();
        return s.approver.?;
    }
    const PingPong = workflow.Def("PingPong", pingPong);

    /// `pingPong` without the signal to the child, which can't replay `PingPong`'s history.
    fn pingPongChanged(ctx: *workflow.Context) ![]const u8 {
        const s = try ctx.allocator().create(State);
        s.* = .{};
        try ctx.setSignalHandler(Approve, s, State.approve);
        const child = ctx.executeChildWorkflow(EchoChild, .{ .id = "echo-child" }, .{});
        _ = try child.started();
        try ctx.await(s, State.approved);
        try child.get();
        return s.approver.?;
    }
    const PingPongChanged = workflow.Def("PingPong", pingPongChanged);

    /// Requests cancellation of the latest run of `workflow_id` and returns "requested", or
    /// the error and the failure's message.
    fn cancelOther(ctx: *workflow.Context, workflow_id: []const u8) ![]const u8 {
        const f = ctx.cancelExternalWorkflow(workflow_id, "");
        f.get() catch |err| switch (err) {
            error.ExternalWorkflowNotFound => return std.fmt.allocPrint(ctx.allocator(), "{t}: {s}", .{ err, f.failure().?.message }),
            else => return err,
        };
        return "requested";
    }
    const CancelOther = workflow.Def("CancelOther", cancelOther);

    /// `cancelOther` without the request, which can't replay `CancelOther`'s history.
    fn cancelOtherChanged(ctx: *workflow.Context, workflow_id: []const u8) ![]const u8 {
        _ = ctx;
        _ = workflow_id;
        return "requested";
    }
    const CancelOtherChanged = workflow.Def("CancelOther", cancelOtherChanged);
};

const NexusFixtures = struct {
    const EchoInput = struct { message: []const u8 };
    const Echo = nexus.OperationDef("echo", EchoInput, []const u8);
    const Shout = nexus.OperationDef("shout", EchoInput, []const u8);
    const Service = nexus.ServiceDef("hello-service", .{ Echo, Shout });

    /// Calls `Operation` through `endpoint` with a 2s schedule-to-close timeout, and returns
    /// its output, or the error, the failure's message and its root cause's.
    fn call(comptime Operation: type, ctx: *workflow.Context, endpoint: []const u8) ![]const u8 {
        const f = ctx.executeNexusOperation(Service, Operation, .{
            .endpoint = endpoint,
            .schedule_to_close_timeout = .fromSeconds(2),
            .headers = &.{.{ .name = "trace-id", .value = "abc" }},
            .summary = "echo it",
        }, .{ .message = "hi" });
        return f.get() catch |err| switch (err) {
            error.NexusOperationFailed => {
                const fl = f.failure().?;
                const root = failure_mod.rootCause(fl);
                const timeout = switch (root.failure_info orelse return error.NoFailureInfo) {
                    .timeout_failure_info => |t| t.timeout_type,
                    else => return error.NotATimeout,
                };
                return std.fmt.allocPrint(ctx.allocator(), "{t}: {s} <- {s} ({t})", .{ err, fl.message, root.message, timeout });
            },
            else => return err,
        };
    }

    fn callEcho(ctx: *workflow.Context, endpoint: []const u8) ![]const u8 {
        return call(Echo, ctx, endpoint);
    }
    const CallEcho = workflow.Def("CallEcho", callEcho);

    /// `callEcho` calling another operation, which can't replay `CallEcho`'s history.
    fn callShout(ctx: *workflow.Context, endpoint: []const u8) ![]const u8 {
        return call(Shout, ctx, endpoint);
    }
    const CallEchoChanged = workflow.Def("CallEcho", callShout);
};

const ContinueFixtures = struct {
    /// Records `count` in the memo, then continues as new with `count + 1` until it reaches
    /// `until`. Each run checks that it continued from `previous_run_id`. The last run returns
    /// its count, the memo's `round` (carried from the run before), and its timeouts.
    fn countUp(ctx: *workflow.Context, count: u32, until: u32, previous_run_id: []const u8) ![]const u8 {
        if (!std.mem.eql(u8, ctx.info().continued_run_id, previous_run_id)) return error.WrongContinuedRunId;
        if (count < until) {
            try ctx.upsertMemo(.{ .round = count });
            return ctx.continueAsNew(CountUp, .{}, .{ count + 1, until, ctx.info().run_id });
        }
        const round = try ctx.memoValue(?u32, "round") orelse null;
        const seconds = struct {
            fn of(d: ?std.Io.Duration) i64 {
                return if (d) |v| @intCast(@divFloor(v.nanoseconds, std.time.ns_per_s)) else -1;
            }
        }.of;
        return std.fmt.allocPrint(ctx.allocator(), "{d} round={?d} run_timeout={d} task_timeout={d}", .{ count, round, seconds(ctx.info().run_timeout), seconds(ctx.info().task_timeout) });
    }
    const CountUp = workflow.Def("CountUp", countUp);

    /// `countUp` that completes where it continued as new.
    fn countUpCompleting(ctx: *workflow.Context, count: u32, until: u32, previous_run_id: []const u8) ![]const u8 {
        _ = until;
        _ = previous_run_id;
        try ctx.upsertMemo(.{ .round = count });
        return "done";
    }
    const CountUpCompleting = workflow.Def("CountUp", countUpCompleting);
};

/// One workflow at each stage of a change from greeting to saying goodbye: before it, behind a
/// patch, with the patch deprecated, and with it removed; and the same change made with
/// version numbers.
const PatchFixtures = struct {
    const opts: workflow.ActivityOptions = .{ .start_to_close_timeout = .fromSeconds(10) };

    fn farewell(ctx: *activity.Context, name: []const u8) ![]const u8 {
        return std.fmt.allocPrint(ctx.allocator(), "Goodbye {s}!", .{name});
    }
    const Farewell = activity.Def("Farewell", farewell);

    fn greet(ctx: *workflow.Context, name: []const u8) ![]const u8 {
        return ctx.executeActivity(Fixtures.Greet, opts, .{name}).get();
    }
    fn bid(ctx: *workflow.Context, name: []const u8) ![]const u8 {
        return ctx.executeActivity(Farewell, opts, .{name}).get();
    }

    const Original = workflow.Def("Patched", greet);

    fn patched(ctx: *workflow.Context, name: []const u8) ![]const u8 {
        if (ctx.patched("farewell")) return bid(ctx, name);
        return greet(ctx, name);
    }
    const Patched = workflow.Def("Patched", patched);

    fn deprecated(ctx: *workflow.Context, name: []const u8) ![]const u8 {
        ctx.deprecatePatch("farewell");
        return bid(ctx, name);
    }
    const Deprecated = workflow.Def("Patched", deprecated);

    const Removed = workflow.Def("Patched", bid);

    const VersionedOriginal = workflow.Def("Versioned", greet);

    fn versionedOne(ctx: *workflow.Context, name: []const u8) ![]const u8 {
        const v = try ctx.getVersion("farewell", workflow.default_version, 1);
        return if (v == workflow.default_version) greet(ctx, name) else bid(ctx, name);
    }
    const VersionedOne = workflow.Def("Versioned", versionedOne);

    /// Version 2 says goodbye twice.
    fn versionedTwo(ctx: *workflow.Context, name: []const u8) ![]const u8 {
        return switch (try ctx.getVersion("farewell", workflow.default_version, 2)) {
            workflow.default_version => greet(ctx, name),
            1 => bid(ctx, name),
            else => std.fmt.allocPrint(ctx.allocator(), "{s} {s}", .{ try bid(ctx, name), try bid(ctx, name) }),
        };
    }
    const VersionedTwo = workflow.Def("Versioned", versionedTwo);

    /// `versionedTwo` once no run uses an older version.
    fn versionedTwoOnly(ctx: *workflow.Context, name: []const u8) ![]const u8 {
        _ = try ctx.getVersion("farewell", 2, 2);
        return std.fmt.allocPrint(ctx.allocator(), "{s} {s}", .{ try bid(ctx, name), try bid(ctx, name) });
    }
    const VersionedTwoOnly = workflow.Def("Versioned", versionedTwoOnly);
};

const LocalFixtures = struct {
    var started: Io.Event = .unset;
    /// Set once `block` has seen a cancel; `seen_reason` is written before.
    var observed: Io.Event = .unset;
    var seen_reason: ?activity.CancelReason = null;

    fn reset() void {
        started = .unset;
        observed = .unset;
        seen_reason = null;
    }

    fn isLocal(ctx: *activity.Context) !bool {
        return ctx.info().is_local;
    }
    const IsLocal = activity.Def("IsLocal", isLocal);

    fn localAndRemote(ctx: *workflow.Context) ![]const u8 {
        const local = try ctx.executeLocalActivity(IsLocal, .{ .start_to_close_timeout = .fromSeconds(10) }, .{}).get();
        const remote = try ctx.executeActivity(IsLocal, .{ .start_to_close_timeout = .fromSeconds(10) }, .{}).get();
        return std.fmt.allocPrint(ctx.allocator(), "local={} remote={}", .{ local, remote });
    }
    const LocalAndRemote = workflow.Def("LocalAndRemote", localAndRemote);

    /// Blocks on its `std.Io` until canceled.
    fn block(ctx: *activity.Context) !void {
        started.set(ctx.io());
        ctx.io().sleep(.fromSeconds(600), .awake) catch |err| {
            seen_reason = ctx.cancelReason();
            observed.set(ctx.io());
            return err;
        };
    }
    const Block = activity.Def("Block", block);

    fn waitBlocked(ctx: *activity.Context) !void {
        try started.wait(ctx.io());
    }
    const WaitBlocked = activity.Def("WaitBlocked", waitBlocked);

    /// Describes the root cause of an activity future's failure.
    fn describe(ctx: *workflow.Context, f: *const failure_mod.Failure) ![]const u8 {
        const root = failure_mod.rootCause(f);
        return switch (root.failure_info orelse return "no failure info") {
            .canceled_failure_info => "canceled",
            .timeout_failure_info => |t| std.fmt.allocPrint(ctx.allocator(), "timeout {t}", .{t.timeout_type}),
            else => std.fmt.allocPrint(ctx.allocator(), "unexpected failure: {s}", .{root.message}),
        };
    }

    /// Cancels `Block` once it is running. The second local activity resolves inside the same
    /// workflow task, so the cancel needs no new events from the server.
    fn cancelLocal(ctx: *workflow.Context) ![]const u8 {
        const blocked = ctx.executeLocalActivity(Block, .{
            .start_to_close_timeout = .fromSeconds(60),
            .cancellation_type = .wait_cancellation_completed,
        }, .{});
        try ctx.executeLocalActivity(WaitBlocked, .{ .start_to_close_timeout = .fromSeconds(60) }, .{}).get();
        blocked.cancel();
        blocked.get() catch |err| switch (err) {
            error.ActivityFailed => return describe(ctx, blocked.failure().?),
            else => return err,
        };
        return "not canceled";
    }
    const CancelLocal = workflow.Def("CancelLocal", cancelLocal);

    /// Runs `Block` once with a half-second start-to-close (or, with `schedule_to_close`,
    /// schedule-to-close) timeout.
    fn timeOutLocal(ctx: *workflow.Context, schedule_to_close: bool) ![]const u8 {
        const timeout: Io.Duration = .fromMilliseconds(500);
        const blocked = ctx.executeLocalActivity(Block, .{
            .start_to_close_timeout = if (schedule_to_close) null else timeout,
            .schedule_to_close_timeout = if (schedule_to_close) timeout else null,
            .retry_policy = .{ .maximum_attempts = 1 },
        }, .{});
        blocked.get() catch |err| switch (err) {
            error.ActivityFailed => return describe(ctx, blocked.failure().?),
            else => return err,
        };
        return "not timed out";
    }
    const TimeOutLocal = workflow.Def("TimeOutLocal", timeOutLocal);

    fn start() !*WorkflowEnvironment {
        reset();
        const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
        errdefer env.deinit();
        try env.registerWorkflow(LocalAndRemote);
        try env.registerWorkflow(CancelLocal);
        try env.registerWorkflow(TimeOutLocal);
        try env.registerActivity(IsLocal);
        try env.registerActivity(Block);
        try env.registerActivity(WaitBlocked);
        return env;
    }
};

test "WorkflowEnvironment runs a workflow with a test double for its activity" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(Fixtures.Hello);
    try env.registerActivityAs(Fixtures.Greet, Fixtures.greetDouble);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const result = try env.executeWorkflow(Fixtures.Hello, .{"Temporal"}, arena.allocator(), null);
    try testing.expectEqualStrings("Hello from the test double!", result);
}

test "WorkflowEnvironment runs a workflow after its first start failed with NothingRegistered" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try testing.expectError(error.NothingRegistered, env.ensureWorker());
    try testing.expect(env.running == null);
    // Without the generation begun before the spawn, `ensureWorker` saw the first run's failed
    // start and then awaited the new run, which polls until stopped. The bound stops the
    // worker, so that shows as `error.WorkerFailedToStart` here rather than a hung suite.
    const Bound = struct {
        fn stopAfter(w: *worker_mod.Worker, after: Io.Duration) void {
            testing.io.sleep(after, .awake) catch return;
            w.stop();
        }
    };
    var bound = try io_tasks.concurrent(testing.io, Bound.stopAfter, .{ &env.worker, Io.Duration.fromSeconds(60) });
    defer bound.cancel(testing.io);
    try env.registerWorkflow(Fixtures.Different);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const result = try env.executeWorkflow(Fixtures.Different, .{"Temporal"}, arena.allocator(), null);
    try testing.expectEqualStrings("no activity here", result);
}

test "WorkflowEnvironment.fromClient runs a workflow on the test's own server and leaves it and the client running" {
    var server = try DevServer.start(testing.allocator, testing.io, .{});
    defer server.stop(testing.io);
    var client = try client_mod.Client.connect(testing.allocator, testing.io, server.clientOptions());
    defer client.close();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var first_id: []const u8 = undefined;
    {
        const env = try WorkflowEnvironment.fromClient(testing.allocator, testing.io, &client);
        defer env.deinit();
        try testing.expect(env.server == .existing);
        try testing.expectEqualStrings(client.namespace, env.client.namespace);
        try env.registerWorkflow(Fixtures.Hello);
        try env.registerActivityAs(Fixtures.Greet, Fixtures.greetDouble);
        var run = try env.startWorkflow(Fixtures.Hello, .{}, .{"Temporal"});
        defer run.deinit();
        first_id = try a.dupe(u8, run.id);
        try testing.expectEqualStrings("Hello from the test double!", try env.getResult(Fixtures.Hello, &run, a, null));

        // No time skipping: `now` is the wall clock and `sleep` a real sleep.
        const before = try env.now();
        try env.sleep(.fromMilliseconds(20));
        const after = try env.now();
        try testing.expect(before.durationTo(after).nanoseconds >= 20 * std.time.ns_per_ms);
        try testing.expect(before.durationTo(after).nanoseconds < std.time.ns_per_s * 60);
    }

    // The environment is gone; the client and the server still answer, with its run on record.
    const description = try client.describeWorkflow(a, first_id, "");
    try testing.expectEqual(.completed, description.status);

    // A second environment on the same client, closed after the client it came from.
    var other = try client.forNamespace(client.namespace);
    const env = try WorkflowEnvironment.fromClient(testing.allocator, testing.io, &other);
    defer env.deinit();
    other.close();
    try env.registerWorkflow(Fixtures.Hello);
    try env.registerActivity(Fixtures.Greet);
    _ = try env.executeWorkflow(Fixtures.Hello, .{"again"}, a, null);
}

const StructActivities = struct {
    const Shop = struct {
        fee: u32,

        pub fn charge(s: *Shop, ctx: *activity.Context, amount: u32) !u32 {
            _ = ctx;
            return amount + s.fee;
        }

        pub fn isLocal(s: *Shop, ctx: *activity.Context) !bool {
            _ = s;
            return ctx.info().is_local;
        }
    };
    const acts = activity.defs(Shop, .{ .prefix = "Shop." });

    fn order(ctx: *workflow.Context, amount: u32) ![]const u8 {
        const ao: workflow.ActivityOptions = .{ .start_to_close_timeout = .fromSeconds(10) };
        const lao: workflow.LocalActivityOptions = .{ .start_to_close_timeout = .fromSeconds(10) };
        const remote = try ctx.executeActivity(acts.charge, ao, .{amount}).get();
        const local = try ctx.executeLocalActivity(acts.charge, lao, .{remote}).get();
        const ran_local = try ctx.executeLocalActivity(acts.isLocal, lao, .{}).get();
        return std.fmt.allocPrint(ctx.allocator(), "{d} {d} local={}", .{ remote, local, ran_local });
    }
    const Order = workflow.Def("Order", order);
};

test "WorkflowEnvironment registers a struct's activities, and workflows call them through activity.defs" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    var shop: StructActivities.Shop = .{ .fee = 2 };
    try env.registerWorkflow(StructActivities.Order);
    try env.registerActivities(&shop, StructActivities.acts);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const result = try env.executeWorkflow(StructActivities.Order, .{@as(u32, 10)}, arena.allocator(), null);
    try testing.expectEqualStrings("12 14 local=true", result);
}

test "DevServer honors namespace, port and search attribute options" {
    var server = try DevServer.start(testing.allocator, testing.io, .{
        .namespace = "zig-tests",
        .port = 0,
        .search_attributes = &.{.{ .name = "OrderId", .type = .Keyword }},
    });
    defer server.stop(testing.io);
    try testing.expect(std.mem.startsWith(u8, server.address(), "127.0.0.1:"));
    var client = try client_mod.Client.connect(testing.allocator, testing.io, server.clientOptions());
    defer client.close();
    try testing.expectEqualStrings("zig-tests", client.namespace);
}

test "createNexusEndpoint creates an endpoint on a DevServer that routes to another namespace, and reports a duplicate" {
    var server = try DevServer.start(testing.allocator, testing.io, .{ .extra_args = &.{ "--namespace", "zig-endpoint-target" } });
    defer server.stop(testing.io);
    var client = try client_mod.Client.connect(testing.allocator, testing.io, server.clientOptions());
    defer client.close();

    try createNexusEndpoint(&client, "zig-endpoint", .{ .namespace = "zig-endpoint-target", .task_queue = "zig-handlers" });
    try testing.expectError(error.AlreadyExists, createNexusEndpoint(&client, "zig-endpoint", .{ .namespace = "default", .task_queue = "other" }));

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const ops = proto.temporal.api.operatorservice.v1;
    const listed = try serviceCall(&client, .operator, ops.ListNexusEndpointsResponse, "ListNexusEndpoints", ops.ListNexusEndpointsRequest{ .page_size = 10 }, arena.allocator(), null);
    try testing.expectEqual(1, listed.endpoints.items.len);
    const spec = listed.endpoints.items[0].spec.?;
    try testing.expectEqualStrings("zig-endpoint", spec.name);
    const worker_target = spec.target.?.variant.?.worker;
    try testing.expectEqualStrings("zig-endpoint-target", worker_target.namespace);
    try testing.expectEqualStrings("zig-handlers", worker_target.task_queue);
}

test "TestServer starts a time-skipping server a client can connect to" {
    var server = try TestServer.start(testing.allocator, testing.io, .{});
    defer server.stop(testing.io);
    var client = try client_mod.Client.connect(testing.allocator, testing.io, server.clientOptions());
    client.close();
}

test "ActivityEnvironment calls the activity directly" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const before: Io.Timestamp = .now(testing.io, .real);
    var env: ActivityEnvironment = .init(testing.io);
    try testing.expectEqualStrings("Hello World!", try env.executeActivity(Fixtures.Greet, .{"World"}, arena.allocator()));
    // The default info was scheduled and started when the environment was made.
    try testing.expect(env.info.started_time.?.nanoseconds >= before.nanoseconds);
    try testing.expectEqual(env.info.started_time.?, env.info.scheduled_time.?);
    try testing.expectEqual(env.info.started_time.?, env.info.current_attempt_scheduled_time.?);
}

const EnvironmentStopFixtures = struct {
    var started: Io.Event = .unset;
    var seen_reason: ?activity.CancelReason = null;

    /// Waits until canceled, recording the reason.
    fn hold(ctx: *activity.Context) !void {
        started.set(ctx.io());
        while (true) ctx.io().sleep(.fromSeconds(60), .awake) catch |err| {
            seen_reason = ctx.cancelReason();
            return err;
        };
    }
    const Hold = activity.Def("Hold", hold);

    /// Waits for the worker to stop on a coroutine of its own, beside a long sleep, and
    /// reports whether it saw the worker stopping before and after.
    fn drain(ctx: *activity.Context) ![2]bool {
        const before = ctx.isWorkerStopping();
        const Result = union(enum) { stop: error{Canceled}!void, slept: error{Canceled}!void };
        var buffer: [2]Result = undefined;
        var select: Io.Select(Result) = .init(ctx.io(), &buffer);
        defer select.cancelDiscard();
        try select.concurrent(.stop, activity.Context.waitForWorkerStop, .{ctx});
        try select.concurrent(.slept, Io.sleep, .{ ctx.io(), .fromSeconds(60), .awake });
        started.set(ctx.io());
        switch (try select.await()) {
            .stop => |r| try r,
            .slept => return error.WorkerNeverStopped,
        }
        return .{ before, ctx.isWorkerStopping() };
    }
    const Drain = activity.Def("Drain", drain);

    fn info(ctx: *activity.Context) ![]const u8 {
        const i = ctx.info();
        return std.fmt.allocPrint(ctx.allocator(), "{?f}|{?f}|{?f}|{d}|{d}|{s}|{d}|{}", .{
            i.schedule_to_close_timeout,
            i.start_to_close_timeout,
            i.heartbeat_timeout,
            if (i.retry_policy) |p| p.maximum_attempts else -1,
            i.priority.priority_key,
            i.priority.fairness_key,
            i.priority.fairness_weight,
            i.deadline() != null,
        });
    }
    const Info = activity.Def("Info", info);

    fn runHold(env: *ActivityEnvironment, arena: Allocator) !void {
        return env.executeActivity(Hold, .{}, arena);
    }

    fn runDrain(env: *ActivityEnvironment, arena: Allocator) ![2]bool {
        return env.executeActivity(Drain, .{}, arena);
    }
};

/// Forwards to `backing` and counts the calls made while cancels were not blocked on `io`'s
/// current task.
const CancelProbeAllocator = struct {
    io: Io,
    backing: Allocator,
    calls: std.atomic.Value(usize) = .init(0),
    unblocked: std.atomic.Value(usize) = .init(0),

    fn allocator(p: *CancelProbeAllocator) Allocator {
        return .{ .ptr = p, .vtable = &vtable };
    }

    const vtable: Allocator.VTable = .{ .alloc = alloc, .resize = resize, .remap = remap, .free = free };

    fn record(p: *CancelProbeAllocator) void {
        const prev = p.io.swapCancelProtection(.blocked);
        _ = p.io.swapCancelProtection(prev);
        _ = p.calls.fetchAdd(1, .monotonic);
        if (prev == .unblocked) _ = p.unblocked.fetchAdd(1, .monotonic);
    }

    fn alloc(ptr: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const p: *CancelProbeAllocator = @ptrCast(@alignCast(ptr));
        p.record();
        return p.backing.rawAlloc(len, alignment, ret_addr);
    }

    fn resize(ptr: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
        const p: *CancelProbeAllocator = @ptrCast(@alignCast(ptr));
        p.record();
        return p.backing.rawResize(memory, alignment, new_len, ret_addr);
    }

    fn remap(ptr: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        const p: *CancelProbeAllocator = @ptrCast(@alignCast(ptr));
        p.record();
        return p.backing.rawRemap(memory, alignment, new_len, ret_addr);
    }

    fn free(ptr: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const p: *CancelProbeAllocator = @ptrCast(@alignCast(ptr));
        p.record();
        p.backing.rawFree(memory, alignment, ret_addr);
    }
};

const CancelProbeFixtures = struct {
    /// Allocates, grows and frees a slice, then returns one allocated from the context.
    fn allocates(ctx: *activity.Context) ![]const u8 {
        const gpa = ctx.allocator();
        var scratch = try gpa.alloc(u8, 16);
        scratch = try gpa.realloc(scratch, 4096);
        gpa.free(scratch);
        return gpa.dupe(u8, "allocated");
    }
    const Allocates = activity.Def("ProbeAllocates", allocates);
};

test "ActivityEnvironment blocks cancels around each allocation the activity makes" {
    var probe: CancelProbeAllocator = .{ .io = testing.io, .backing = testing.allocator };
    var env: ActivityEnvironment = .init(testing.io);
    const result = try env.executeActivity(CancelProbeFixtures.Allocates, .{}, probe.allocator());
    // The wrapper passes memory through unchanged, so the result is freed straight through
    // the backing allocator, outside the probe: only the activity's calls are counted.
    defer testing.allocator.free(result);
    try testing.expectEqualStrings("allocated", result);
    // alloc, resize or remap (or an alloc and a free if neither grows in place), free, alloc.
    try testing.expect(probe.calls.load(.monotonic) >= 3);
    try testing.expectEqual(@as(usize, 0), probe.unblocked.load(.monotonic));
}

test "ActivityEnvironment.cancel cancels the running activity with the reason" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var env: ActivityEnvironment = .init(testing.io);
    // Without a running activity it does nothing.
    env.cancel(.canceled);
    for ([_]activity.CancelReason{ .timed_out, .paused }) |reason| {
        EnvironmentStopFixtures.started = .unset;
        EnvironmentStopFixtures.seen_reason = null;
        var running = try testing.io.concurrent(EnvironmentStopFixtures.runHold, .{ &env, arena.allocator() });
        try EnvironmentStopFixtures.started.wait(testing.io);
        env.cancel(reason);
        try testing.expectError(error.Canceled, running.await(testing.io));
        try testing.expectEqual(@as(?activity.CancelReason, reason), EnvironmentStopFixtures.seen_reason);
    }
    try testing.expectEqual(null, env.running);
}

test "ActivityEnvironment.stopWorker wakes waitForWorkerStop and flips isWorkerStopping" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var env: ActivityEnvironment = .init(testing.io);
    EnvironmentStopFixtures.started = .unset;
    var running = try testing.io.concurrent(EnvironmentStopFixtures.runDrain, .{ &env, arena.allocator() });
    try EnvironmentStopFixtures.started.wait(testing.io);
    env.stopWorker();
    try testing.expectEqual([2]bool{ false, true }, try running.await(testing.io));
    // An activity executed after the stop sees it at once.
    try testing.expectEqual([2]bool{ true, true }, try env.executeActivity(EnvironmentStopFixtures.Drain, .{}, arena.allocator()));
}

test "ActivityEnvironment's info has no timeouts, retry policy or priority unless set" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var env: ActivityEnvironment = .init(testing.io);
    try testing.expectEqualStrings("null|null|null|-1|0||0|false", try env.executeActivity(EnvironmentStopFixtures.Info, .{}, arena.allocator()));
    env.info.schedule_to_close_timeout = .fromSeconds(60);
    env.info.start_to_close_timeout = .fromSeconds(10);
    env.info.heartbeat_timeout = .fromSeconds(2);
    env.info.retry_policy = .{ .maximum_attempts = 3 };
    env.info.priority = .{ .priority_key = 1, .fairness_key = "tenant", .fairness_weight = 2 };
    try testing.expectEqualStrings("1m|10s|2s|3|1|tenant|2|true", try env.executeActivity(EnvironmentStopFixtures.Info, .{}, arena.allocator()));
}

const EnvironmentLogFixtures = struct {
    /// Keeps copies of the records it gets, for a test to read.
    const Recorder = struct {
        arena: std.heap.ArenaAllocator,
        mutex: Io.Mutex = .init,
        records: std.ArrayList(Record) = .empty,

        const Record = struct { level: log_mod.Level, message: []const u8, fields: []const log_mod.Field };

        fn sink(r: *Recorder) log_mod.Sink {
            return .{ .ptr = r, .vtable = &.{ .log = write } };
        }

        fn write(ptr: ?*anyopaque, level: log_mod.Level, message: []const u8, fields: []const log_mod.Field) void {
            const r: *Recorder = @ptrCast(@alignCast(ptr.?));
            r.mutex.lockUncancelable(testing.io);
            defer r.mutex.unlock(testing.io);
            const a = r.arena.allocator();
            const copy = a.alloc(log_mod.Field, fields.len) catch @panic("out of memory");
            for (fields, copy) |f, *c| c.* = .{ .key = a.dupe(u8, f.key) catch @panic("out of memory"), .value = a.dupe(u8, f.value) catch @panic("out of memory") };
            r.records.append(a, .{ .level = level, .message = a.dupe(u8, message) catch @panic("out of memory"), .fields = copy }) catch @panic("out of memory");
        }

        fn field(rec: Record, key: []const u8) ?[]const u8 {
            for (rec.fields) |f| if (std.mem.eql(u8, f.key, key)) return f.value;
            return null;
        }
    };

    fn chatty(ctx: *activity.Context, order: u32) !void {
        ctx.logger().info("charging", .{ .order = order });
        // Through std.log this would fail the test that runs it.
        ctx.logger().err("card declined", .{ .order = order });
    }
    const Chatty = activity.Def("Chatty", chatty);

    const Shop = struct {
        name: []const u8,

        pub fn open(s: *Shop, ctx: *activity.Context) !void {
            ctx.logger().err("shop closed", .{ .shop = s.name });
        }
    };
    const shop_acts = activity.defs(Shop, .{ .prefix = "Shop." });

    /// Reports whether the activity's logger is the default one: `std.log`, no context fields.
    fn defaultLogger(ctx: *activity.Context) !bool {
        const l = ctx.logger();
        return l.sink.vtable == log_mod.Sink.std_log.vtable and l.context.len == 0;
    }
    const DefaultLogger = activity.Def("DefaultLogger", defaultLogger);
};

test "ActivityEnvironment.logger receives an activity's info and error logs with the worker's context fields" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var recorder: EnvironmentLogFixtures.Recorder = .{ .arena = .init(testing.allocator) };
    defer recorder.arena.deinit();
    var env: ActivityEnvironment = .init(testing.io);
    env.logger = recorder.sink();
    env.info.activity_id = "act-7";
    env.info.attempt = 3;
    env.info.workflow_id = "wf-9";
    env.info.workflow_run_id = "run-9";
    env.info.workflow_type = "Checkout";

    try env.executeActivity(EnvironmentLogFixtures.Chatty, .{@as(u32, 42)}, arena.allocator());
    const records = recorder.records.items;
    try testing.expectEqual(2, records.len);
    try testing.expectEqual(log_mod.Level.info, records[0].level);
    try testing.expectEqualStrings("charging", records[0].message);
    try testing.expectEqual(log_mod.Level.err, records[1].level);
    try testing.expectEqualStrings("card declined", records[1].message);
    const Rec = EnvironmentLogFixtures.Recorder;
    for (records) |rec| {
        try testing.expectEqualStrings("42", Rec.field(rec, "order").?);
        try testing.expectEqualStrings("act-7", Rec.field(rec, "ActivityID").?);
        try testing.expectEqualStrings("Chatty", Rec.field(rec, "ActivityType").?);
        try testing.expectEqualStrings("3", Rec.field(rec, "Attempt").?);
        try testing.expectEqualStrings("Checkout", Rec.field(rec, "WorkflowType").?);
        try testing.expectEqualStrings("wf-9", Rec.field(rec, "WorkflowID").?);
        try testing.expectEqualStrings("run-9", Rec.field(rec, "RunID").?);
    }

    // A method activity, standalone (no workflow): no empty workflow fields, as on a worker.
    env.info.workflow_id = "";
    var shop: EnvironmentLogFixtures.Shop = .{ .name = "corner" };
    try env.executeActivityWith(EnvironmentLogFixtures.shop_acts.open, &shop, .{}, arena.allocator());
    try testing.expectEqual(3, recorder.records.items.len);
    const rec = recorder.records.items[2];
    try testing.expectEqual(log_mod.Level.err, rec.level);
    try testing.expectEqualStrings("shop closed", rec.message);
    try testing.expectEqualStrings("corner", Rec.field(rec, "shop").?);
    try testing.expectEqualStrings("act-7", Rec.field(rec, "ActivityID").?);
    try testing.expectEqualStrings("Shop.open", Rec.field(rec, "ActivityType").?);
    try testing.expectEqual(null, Rec.field(rec, "WorkflowID"));
    try testing.expectEqual(null, Rec.field(rec, "RunID"));
}

test "ActivityEnvironment without a logger gives the activity the default std.log logger" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var env: ActivityEnvironment = .init(testing.io);
    try testing.expect(try env.executeActivity(EnvironmentLogFixtures.DefaultLogger, .{}, arena.allocator()));
}

/// A history of samples/helloworld, whose workflow `Fixtures.Hello` matches.
const hello_history = @embedFile("testdata/helloworld_history.json");

test "WorkflowReplayer replays a matching history and catches a nondeterministic change" {
    var ok: WorkflowReplayer = .init(testing.allocator, testing.io);
    defer ok.deinit();
    try ok.registerWorkflow(Fixtures.Hello);
    try ok.replayHistoryJson(hello_history);

    var changed: WorkflowReplayer = .init(testing.allocator, testing.io);
    defer changed.deinit();
    try changed.registerWorkflow(Fixtures.Different);
    try testing.expectError(error.Nondeterminism, changed.replayHistoryJson(hello_history));
}

test "WorkflowReplayer applies workflow_memory_limit: a history fits a generous limit and fails a tiny one" {
    var ok: WorkflowReplayer = .init(testing.allocator, testing.io);
    defer ok.deinit();
    ok.options.workflow_memory_limit = 64 * 1024 * 1024;
    try ok.registerWorkflow(Fixtures.Hello);
    try ok.replayHistoryJson(hello_history);

    var tiny: WorkflowReplayer = .init(testing.allocator, testing.io);
    defer tiny.deinit();
    tiny.options.workflow_memory_limit = 1;
    try tiny.registerWorkflow(Fixtures.Hello);
    try testing.expectError(error.ReplayFailed, tiny.replayHistoryJson(hello_history));
}

// Detection adds no command and changes none: a history replays the same under a limit its
// code never reaches.
test "WorkflowReplayer replays a history under a deadlock detection limit it never reaches" {
    var r: WorkflowReplayer = .init(testing.allocator, testing.io);
    defer r.deinit();
    r.options.deadlock_detection_timeout = .fromSeconds(2);
    try r.registerWorkflow(Fixtures.Hello);
    try r.replayHistoryJson(hello_history);
}

test "WorkflowReplayer rejects a deadlock detection limit that isn't positive" {
    var r: WorkflowReplayer = .init(testing.allocator, testing.io);
    defer r.deinit();
    r.options.deadlock_detection_timeout = .zero;
    try r.registerWorkflow(Fixtures.Hello);
    try testing.expectError(error.ReplayFailed, r.replayHistoryJson(hello_history));
}

// Queries leave no trace in history, so registering query handlers must not change the
// commands a workflow produces: `Fixtures.HelloQueried` replays the hello history unchanged.
test "WorkflowReplayer replays a workflow with query handlers" {
    var r: WorkflowReplayer = .init(testing.allocator, testing.io);
    defer r.deinit();
    try r.registerWorkflow(Fixtures.HelloQueried);
    try r.replayHistoryJson(hello_history);
}

/// A dev-server run of `Fixtures.MemoUpdates` started with the memo
/// `{status: "queued", owner: "data", note: "keep"}`, exported with `temporal workflow show`.
const upsert_memo_history = @embedFile("testdata/upsert_memo_history.json");

test "WorkflowReplayer replays memo upserts and catches a dropped upsert" {
    var ok: WorkflowReplayer = .init(testing.allocator, testing.io);
    defer ok.deinit();
    try ok.registerWorkflow(Fixtures.MemoUpdates);
    try ok.replayHistoryJson(upsert_memo_history);

    var changed: WorkflowReplayer = .init(testing.allocator, testing.io);
    defer changed.deinit();
    try changed.registerWorkflow(Fixtures.MemoUpdatesChanged);
    try testing.expectError(error.Nondeterminism, changed.replayHistoryJson(upsert_memo_history));
}

/// A dev-server run of `Fixtures.SearchAttributeUpdates` with the argument "blue", started
/// with CustomKeywordField "order-7" and CustomIntField 1, exported with
/// `temporal workflow show`.
const upsert_search_attributes_history = @embedFile("testdata/upsert_search_attributes_history.json");

test "WorkflowReplayer replays search attribute upserts and catches a dropped upsert" {
    var ok: WorkflowReplayer = .init(testing.allocator, testing.io);
    defer ok.deinit();
    try ok.registerWorkflow(Fixtures.SearchAttributeUpdates);
    try ok.replayHistoryJson(upsert_search_attributes_history);

    var changed: WorkflowReplayer = .init(testing.allocator, testing.io);
    defer changed.deinit();
    try changed.registerWorkflow(Fixtures.SearchAttributeUpdatesChanged);
    try testing.expectError(error.Nondeterminism, changed.replayHistoryJson(upsert_search_attributes_history));
}

/// A dev-server run of `Fixtures.WaitCancellation` canceled with `Client.cancelWorkflow` while
/// the activity heartbeated, exported with `temporal workflow show`.
const wait_cancellation_history = @embedFile("testdata/wait_cancellation_history.json");

test "WorkflowReplayer replays a workflow that waits for its activity's cancellation" {
    var ok: WorkflowReplayer = .init(testing.allocator, testing.io);
    defer ok.deinit();
    try ok.registerWorkflow(Fixtures.WaitCancellation);
    try ok.replayHistoryJson(wait_cancellation_history);
}

test "WorkflowReplayer: an activity's summary, priority and versioning intent don't affect replay" {
    var remote: WorkflowReplayer = .init(testing.allocator, testing.io);
    defer remote.deinit();
    try remote.registerWorkflow(Fixtures.WaitCancellationDescribed);
    try remote.replayHistoryJson(wait_cancellation_history);

    var local: WorkflowReplayer = .init(testing.allocator, testing.io);
    defer local.deinit();
    try local.registerWorkflow(Fixtures.LocalGreetingsDescribed);
    try local.replayHistoryJson(local_activity_history);
}

test "activity summary, priority and versioning intent are recorded in history" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(Fixtures.DescribedGreetings);
    try env.registerActivity(Fixtures.Greet);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var run = try env.startWorkflow(Fixtures.DescribedGreetings, .{}, .{"Ann"});
    defer run.deinit();
    // Replays the run too.
    try testing.expectEqualStrings("Hello Ann! Hello Ann!", try env.getResult(Fixtures.DescribedGreetings, &run, a, null));

    const history = try env.client.fetchHistory(a, run.id, run.run_id);
    var scheduled_seen = false;
    var marker_seen = false;
    for (history.events.items) |event| switch (event.attributes.?) {
        .activity_task_scheduled_event_attributes => |attrs| {
            scheduled_seen = true;
            try StartOptionFixtures.expectMetadata(a, event, "greet remotely", "");
            try testing.expectEqual(@as(i32, 2), attrs.priority.?.priority_key);
            try testing.expectEqualStrings("tenant-a", attrs.priority.?.fairness_key);
            try testing.expectEqual(@as(f32, 1.5), attrs.priority.?.fairness_weight);
            try testing.expect(!attrs.use_workflow_build_id);
        },
        .marker_recorded_event_attributes => {
            marker_seen = true;
            try StartOptionFixtures.expectMetadata(a, event, "greet locally", "");
        },
        else => {},
    };
    try testing.expect(scheduled_seen and marker_seen);
}

/// A dev-server run of `SignalFixtures.CollectApprovals` signaled "ann" while it slept, before
/// it registered its handler, and "bob" after, exported with `temporal workflow show`.
const signal_history = @embedFile("testdata/signal_history.json");

test "WorkflowReplayer replays signal handlers and catches a handler that changed" {
    var ok: WorkflowReplayer = .init(testing.allocator, testing.io);
    defer ok.deinit();
    try ok.registerWorkflow(SignalFixtures.CollectApprovals);
    try ok.replayHistoryJson(signal_history);

    var changed: WorkflowReplayer = .init(testing.allocator, testing.io);
    defer changed.deinit();
    try changed.registerWorkflow(SignalFixtures.CollectApprovalsChanged);
    try testing.expectError(error.Nondeterminism, changed.replayHistoryJson(signal_history));
}

/// A dev-server run of `SignalFixtures.ReceiveApprovals` signaled "ann" while it slept, before
/// it opened its channel, and "bob" while its receive raced the timer (which bob's signal
/// canceled), recorded from a `WorkflowEnvironment` dev server with `Client.fetchHistory` and
/// encoded as ProtoJSON.
const signal_channel_history = @embedFile("testdata/signal_channel_history.json");

test "WorkflowReplayer replays a signal channel's receives and catches a receiver that changed" {
    var ok: WorkflowReplayer = .init(testing.allocator, testing.io);
    defer ok.deinit();
    try ok.registerWorkflow(SignalFixtures.ReceiveApprovals);
    try ok.replayHistoryJson(signal_channel_history);

    var changed: WorkflowReplayer = .init(testing.allocator, testing.io);
    defer changed.deinit();
    try changed.registerWorkflow(SignalFixtures.ReceiveApprovalsChanged);
    try testing.expectError(error.Nondeterminism, changed.replayHistoryJson(signal_channel_history));
}

/// A dev-server run of `UpdateFixtures.HandleUpdates` sent, with `temporal workflow update
/// execute`, `set 5` (accepted), `set -1` (rejected by the validator), an unregistered update
/// (rejected), and `greet "ann"` (whose handler waits for an activity), then signaled `done`;
/// exported with `temporal workflow show`. The rejected updates left no events.
const update_history = @embedFile("testdata/update_history.json");

test "WorkflowReplayer replays update handlers and catches a handler that changed" {
    var ok: WorkflowReplayer = .init(testing.allocator, testing.io);
    defer ok.deinit();
    try ok.registerWorkflow(UpdateFixtures.HandleUpdates);
    try ok.replayHistoryJson(update_history);

    var changed: WorkflowReplayer = .init(testing.allocator, testing.io);
    defer changed.deinit();
    try changed.registerWorkflow(UpdateFixtures.HandleUpdatesChanged);
    try testing.expectError(error.Nondeterminism, changed.replayHistoryJson(update_history));
}

test "WorkflowReplayer replays update_history.json with handlers that read the current update" {
    var r: WorkflowReplayer = .init(testing.allocator, testing.io);
    defer r.deinit();
    try r.registerWorkflow(UpdateFixtures.HandleUpdatesChecked);
    try r.replayHistoryJson(update_history);
}

test "WorkflowReplayer replays update_history.json with the handlers declared on a struct workflow" {
    var r: WorkflowReplayer = .init(testing.allocator, testing.io);
    defer r.deinit();
    try r.registerWorkflow(StructFixtures.HandleUpdatesWf);
    try r.replayHistoryJson(update_history);
}

/// A dev-server run of `UpdateFixtures.HandleDynamicUpdates` sent `set 5` (accepted by its
/// own handler), an update called `other` (rejected by the dynamic handler's validator, so it
/// left no events), and `greet "ann"` (taken by the dynamic handler, which waits for an
/// activity), then signaled `done`. Recorded from a `WorkflowEnvironment` dev server with
/// `Client.fetchHistory` and encoded as ProtoJSON.
const dynamic_update_history = @embedFile("testdata/dynamic_update_history.json");

test "WorkflowReplayer replays an update handled by the dynamic update handler and catches a handler that changed" {
    var ok: WorkflowReplayer = .init(testing.allocator, testing.io);
    defer ok.deinit();
    try ok.registerWorkflow(UpdateFixtures.HandleDynamicUpdates);
    try ok.replayHistoryJson(dynamic_update_history);

    var changed: WorkflowReplayer = .init(testing.allocator, testing.io);
    defer changed.deinit();
    try changed.registerWorkflow(UpdateFixtures.HandleDynamicUpdatesChanged);
    try testing.expectError(error.Nondeterminism, changed.replayHistoryJson(dynamic_update_history));
}

/// A dev-server run of the struct workflow `StructFixtures.Ledger` signal-with-started with
/// owner "ledger" and `record "ann"` (whose handler runs an activity), then sent `set_limit 0`
/// (rejected by the validator, so it left no events), `set_limit 25` (accepted), and `close`.
/// Recorded from a `WorkflowEnvironment` dev server and exported with `temporal workflow show
/// --output json`.
const struct_workflow_history = @embedFile("testdata/struct_workflow_history.json");

test "WorkflowReplayer replays a struct workflow's history and catches a handler that changed" {
    var ok: WorkflowReplayer = .init(testing.allocator, testing.io);
    defer ok.deinit();
    try ok.registerWorkflow(StructFixtures.Ledger);
    try ok.replayHistoryJson(struct_workflow_history);

    var changed: WorkflowReplayer = .init(testing.allocator, testing.io);
    defer changed.deinit();
    try changed.registerWorkflow(StructFixtures.LedgerChanged);
    try testing.expectError(error.Nondeterminism, changed.replayHistoryJson(struct_workflow_history));
}

/// The first run of a dev-server run of `ContinueFixtures.CountUp` started with
/// `.{ 0, 3, "" }`, which upserted its memo and continued as new; exported with `temporal
/// workflow show --run-id`.
const continue_as_new_history = @embedFile("testdata/continue_as_new_history.json");

test "WorkflowReplayer replays a run that continued as new and catches one that completes instead" {
    var ok: WorkflowReplayer = .init(testing.allocator, testing.io);
    defer ok.deinit();
    try ok.registerWorkflow(ContinueFixtures.CountUp);
    try ok.replayHistoryJson(continue_as_new_history);

    var changed: WorkflowReplayer = .init(testing.allocator, testing.io);
    defer changed.deinit();
    try changed.registerWorkflow(ContinueFixtures.CountUpCompleting);
    try testing.expectError(error.Nondeterminism, changed.replayHistoryJson(continue_as_new_history));
}

/// Dev-server runs of the `PatchFixtures` stages with the argument "Temporal", each on a
/// worker with only that stage registered, exported with `temporal workflow show`: before the
/// change (`Original`), behind its patch (`Patched`, which recorded the "farewell" marker),
/// and with the patch deprecated (`Deprecated`, a deprecated marker); and for the version
/// numbers, before the change (`VersionedOriginal`) and at version 1 (`VersionedOne`, a
/// "farewell@v1" marker).
const patch_before_history = @embedFile("testdata/patch_before_history.json");
const patch_marker_history = @embedFile("testdata/patch_marker_history.json");
const patch_deprecated_history = @embedFile("testdata/patch_deprecated_history.json");
const version_before_history = @embedFile("testdata/version_before_history.json");
const version_one_history = @embedFile("testdata/version_one_history.json");

/// Replays `json` with only `Workflow` registered.
fn replayWith(comptime Workflow: type, json: []const u8) WorkflowReplayer.ReplayError!void {
    var r: WorkflowReplayer = .init(testing.allocator, testing.io);
    defer r.deinit();
    try r.registerWorkflow(Workflow);
    return r.replayHistoryJson(json);
}

test "WorkflowReplayer: a patch replays runs from before it and after it" {
    try replayWith(PatchFixtures.Patched, patch_before_history);
    try replayWith(PatchFixtures.Patched, patch_marker_history);
    // Code from before the patch can't replay a run that took the new path.
    try testing.expectError(error.Nondeterminism, replayWith(PatchFixtures.Original, patch_marker_history));
}

test "WorkflowReplayer: deprecatePatch replays patched runs, and only then can the call go" {
    try replayWith(PatchFixtures.Deprecated, patch_marker_history);
    try replayWith(PatchFixtures.Deprecated, patch_deprecated_history);
    // The old path is gone.
    try testing.expectError(error.Nondeterminism, replayWith(PatchFixtures.Deprecated, patch_before_history));
    // Without the call, a deprecated marker replays but a plain one doesn't: removing the
    // patch before deprecating it breaks the runs that recorded it.
    try replayWith(PatchFixtures.Removed, patch_deprecated_history);
    try testing.expectError(error.Nondeterminism, replayWith(PatchFixtures.Removed, patch_marker_history));
}

test "WorkflowReplayer: getVersion replays each run on the version it recorded" {
    try replayWith(PatchFixtures.VersionedOne, version_before_history);
    try replayWith(PatchFixtures.VersionedOne, version_one_history);
    try replayWith(PatchFixtures.VersionedTwo, version_before_history);
    try replayWith(PatchFixtures.VersionedTwo, version_one_history);
    // Dropping support for the default version and version 1 fails the workflow task.
    try testing.expectError(error.ReplayFailed, replayWith(PatchFixtures.VersionedTwoOnly, version_before_history));
    try testing.expectError(error.ReplayFailed, replayWith(PatchFixtures.VersionedTwoOnly, version_one_history));
}

test "patches and versions take the new path on a first run, which then replays" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    try env.registerWorkflow(PatchFixtures.Patched);
    try env.registerWorkflow(PatchFixtures.VersionedTwo);
    try env.registerActivity(Fixtures.Greet);
    try env.registerActivity(PatchFixtures.Farewell);
    // executeWorkflow replays each finished run's history too.
    try testing.expectEqualStrings("Goodbye Temporal!", try env.executeWorkflow(PatchFixtures.Patched, .{"Temporal"}, arena.allocator(), null));
    try testing.expectEqualStrings("Goodbye Temporal! Goodbye Temporal!", try env.executeWorkflow(PatchFixtures.VersionedTwo, .{"Temporal"}, arena.allocator(), null));
}

const VersionFixtures = struct {
    /// Reports the deployment version running its first workflow task.
    fn whoRuns(ctx: *workflow.Context) ![]const u8 {
        const v = ctx.currentDeploymentVersion() orelse return "none";
        return std.fmt.allocPrint(ctx.allocator(), "{s}.{s}", .{ v.deployment_name, v.build_id });
    }
    const WhoRuns = workflow.DefWithOptions("WhoRuns", whoRuns, .{ .versioning_behavior = .pinned });

    const Release = workflow.SignalDef("release", fn () void);
    const StartedOn = workflow.QueryDef("started_on", fn () []const u8);
    const State = struct {
        released: bool = false,
        started_on: []const u8 = "",
        fn release(s: *State) void {
            s.released = true;
        }
        fn isReleased(s: *State) bool {
            return s.released;
        }
        fn startedOn(s: *State) []const u8 {
            return s.started_on;
        }
    };
    /// Waits for `Release`, then returns the build IDs that started it and that finished it.
    fn waitThenReport(ctx: *workflow.Context) ![]const u8 {
        const state = try ctx.allocator().create(State);
        state.* = .{ .started_on = if (ctx.currentDeploymentVersion()) |v| v.build_id else "none" };
        try ctx.setSignalHandler(Release, state, State.release);
        try ctx.setQueryHandler(StartedOn, state, State.startedOn);
        try ctx.await(state, State.isReleased);
        const finished_on = if (ctx.currentDeploymentVersion()) |v| v.build_id else "none";
        return std.fmt.allocPrint(ctx.allocator(), "{s}->{s}", .{ state.started_on, finished_on });
    }
    const PinnedRollout = workflow.DefWithOptions("PinnedRollout", waitThenReport, .{ .versioning_behavior = .pinned });
    const AutoRollout = workflow.DefWithOptions("AutoRollout", waitThenReport, .{ .versioning_behavior = .auto_upgrade });

    /// Starts a worker of version `build_id` of the "rollout" deployment on `task_queue`.
    const VersionWorker = struct {
        w: worker_mod.Worker,
        running: Io.Future(worker_mod.Worker.RunError!void) = undefined,

        fn start(vw: *VersionWorker, c: *client_mod.Client, task_queue: []const u8, build_id: []const u8) !void {
            vw.* = .{ .w = try .init(testing.allocator, testing.io, c, task_queue, .{
                .deployment = .{ .deployment_name = "rollout", .build_id = build_id },
            }) };
            errdefer vw.w.deinit();
            try vw.w.registerWorkflow(PinnedRollout);
            try vw.w.registerWorkflow(AutoRollout);
            vw.running = try testing.io.concurrent(WorkflowEnvironment.runWorker, .{&vw.w});
        }

        fn stop(vw: *VersionWorker) void {
            vw.w.stop();
            vw.running.await(testing.io) catch {};
            vw.w.deinit();
        }
    };

    /// Waits until `h`'s latest routing change has reached its task queues.
    fn awaitRouting(h: *const client_mod.WorkerDeploymentHandle) !void {
        while (true) {
            var arena: std.heap.ArenaAllocator = .init(testing.allocator);
            defer arena.deinit();
            if ((try h.describe(arena.allocator())).routing_update != .in_progress) return;
            try testing.io.sleep(.fromMilliseconds(100), .awake);
        }
    }

    /// Waits until the server knows version `build_id` of `h`'s deployment: its worker
    /// has polled.
    fn awaitVersion(h: *const client_mod.WorkerDeploymentHandle, build_id: []const u8) !void {
        while (true) {
            var arena: std.heap.ArenaAllocator = .init(testing.allocator);
            defer arena.deinit();
            const d = h.describe(arena.allocator()) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            };
            if (d) |desc| for (desc.versions) |v| if (std.mem.eql(u8, v.version.build_id, build_id)) return;
            try testing.io.sleep(.fromMilliseconds(100), .awake);
        }
    }

    /// Waits until version `build_id` of `h`'s deployment has polled `task_queue` for
    /// workflow tasks: the server pins a workflow only to a version that has.
    fn awaitPolled(h: *const client_mod.WorkerDeploymentHandle, build_id: []const u8, task_queue: []const u8) !void {
        while (true) {
            var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
            defer scratch.deinit();
            // The version can be missing for a moment after the deployment lists it.
            const version = h.describeVersion(scratch.allocator(), build_id) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            };
            if (version) |v| for (v.task_queues) |q| {
                if (q.type == .workflow and std.mem.eql(u8, q.name, task_queue)) return;
            };
            try testing.io.sleep(.fromMilliseconds(100), .awake);
        }
    }
};

test "a worker that only reports its deployment version runs workflows that see it" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var w = try worker_mod.Worker.init(testing.allocator, testing.io, &env.client, "reports-version", .{
        .deployment = .{ .deployment_name = "reporting", .build_id = "b1", .use_worker_versioning = false },
    });
    defer w.deinit();
    try w.registerWorkflow(VersionFixtures.WhoRuns);
    var running = try testing.io.concurrent(WorkflowEnvironment.runWorker, .{&w});
    defer {
        w.stop();
        running.await(testing.io) catch {};
    }
    var run = try env.client.executeWorkflow(VersionFixtures.WhoRuns, .{ .task_queue = "reports-version" }, .{});
    defer run.deinit();
    try testing.expectEqualStrings("reporting.b1", try run.get(arena.allocator()));
    try env.registerWorkflow(VersionFixtures.WhoRuns);
    try env.replayRun(run.id, run.run_id);
}

test "worker deployment: a pinned run stays on its version across a rollout, an auto-upgrade run moves" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const c = &env.client;
    var rollout = try c.workerDeployment("rollout");
    defer rollout.deinit();

    var v1: VersionFixtures.VersionWorker = undefined;
    try v1.start(c, "rollout", "v1");
    defer v1.stop();
    try VersionFixtures.awaitVersion(&rollout, "v1");
    const first = try rollout.setCurrentVersion(a, "v1", .{});
    try testing.expectEqual(@as(?deployment_mod.Version, null), first.previous);

    var pinned = try c.executeWorkflow(VersionFixtures.PinnedRollout, .{ .task_queue = "rollout" }, .{});
    defer pinned.deinit();
    var auto = try c.executeWorkflow(VersionFixtures.AutoRollout, .{ .task_queue = "rollout" }, .{});
    defer auto.deinit();
    // A query is answered once the first workflow task has run.
    try testing.expectEqualStrings("v1", try c.queryWorkflow(VersionFixtures.StartedOn, a, pinned.id, "", .{}));
    try testing.expectEqualStrings("v1", try c.queryWorkflow(VersionFixtures.StartedOn, a, auto.id, "", .{}));

    var v2: VersionFixtures.VersionWorker = undefined;
    try v2.start(c, "rollout", "v2");
    defer v2.stop();
    try VersionFixtures.awaitVersion(&rollout, "v2");
    const before = try rollout.describe(a);
    try testing.expectEqualStrings("v1", before.routing.current.?.build_id);
    const second = try rollout.setCurrentVersion(a, "v2", .{ .conflict_token = before.conflict_token });
    try testing.expectEqualStrings("v1", second.previous.?.build_id);
    // The token from before the change no longer applies.
    try testing.expectError(error.RpcFailed, rollout.setCurrentVersion(a, "v1", .{ .conflict_token = before.conflict_token }));
    try VersionFixtures.awaitRouting(&rollout);

    try c.signalWorkflow(VersionFixtures.Release, pinned.id, "", .{});
    try c.signalWorkflow(VersionFixtures.Release, auto.id, "", .{});
    try testing.expectEqualStrings("v1->v1", try pinned.get(a));
    try testing.expectEqualStrings("v1->v2", try auto.get(a));
    try env.registerWorkflow(VersionFixtures.PinnedRollout);
    try env.registerWorkflow(VersionFixtures.AutoRollout);
    try env.replayRun(pinned.id, pinned.run_id);
    try env.replayRun(auto.id, auto.run_id);

    const after = try rollout.describe(a);
    try testing.expectEqualStrings("rollout", after.name);
    try testing.expectEqualStrings("v2", after.routing.current.?.build_id);
    try testing.expectEqual(@as(usize, 2), after.versions.len);
    for (after.versions) |v| {
        const want: deployment_mod.VersionStatus = if (std.mem.eql(u8, v.version.build_id, "v2")) .current else .draining;
        // v1 drains while the server checks for open pinned runs, then is drained.
        if (want == .draining) {
            try testing.expect(v.status == .draining or v.status == .drained);
        } else try testing.expectEqual(want, v.status);
    }

    // Ramp a share back to v1, then stop ramping.
    const ramped = try rollout.setRampingVersion(a, "v1", 25, .{});
    try testing.expectEqual(@as(?deployment_mod.Version, null), ramped.previous);
    const ramping = try rollout.describe(a);
    try testing.expectEqualStrings("v1", ramping.routing.ramping.?.build_id);
    try testing.expectEqual(@as(f32, 25), ramping.routing.ramping_percentage);
    const unramped = try rollout.unsetRampingVersion(a, .{});
    try testing.expectEqualStrings("v1", unramped.previous.?.build_id);
    try testing.expectEqual(@as(f32, 25), unramped.previous_percentage);
    try testing.expectEqual(@as(?deployment_mod.Version, null), (try rollout.describe(a)).routing.ramping);

    try rollout.updateVersionMetadata("v2", .{ .release = "2026-11", .approved = true }, &.{});
    try rollout.updateVersionMetadata("v2", .{}, &.{"approved"});
    const described = try rollout.describeVersion(a, "v2");
    try testing.expectEqual(deployment_mod.VersionStatus.current, described.status);
    try testing.expectEqual(@as(usize, 1), described.metadata.len);
    try testing.expectEqualStrings("2026-11", (try converter.fieldValue([]const u8, a, described.metadata, "release")).?);
    var polled_workflows = false;
    for (described.task_queues) |q| {
        if (std.mem.eql(u8, q.name, "rollout") and q.type == .workflow) polled_workflows = true;
    }
    try testing.expect(polled_workflows);
    try testing.expectError(error.NotFound, rollout.describeVersion(a, "v9"));

    try testing.expectEqualStrings("", try rollout.setManagerIdentity(a, null, .{}));
    try testing.expectEqualStrings(c.identity, (try rollout.describe(a)).manager_identity);
    try testing.expectEqualStrings(c.identity, try rollout.setManagerIdentity(a, "", .{}));

    // v1's worker still polls, and the deployment still has versions.
    try testing.expectError(error.RpcFailed, rollout.deleteVersion("v1", .{ .skip_drainage = true }));
    try testing.expectError(error.RpcFailed, rollout.delete());

    var it = c.listWorkerDeployments(.{});
    defer it.deinit();
    var listed = false;
    while (try it.next(a)) |entry| {
        if (std.mem.eql(u8, entry.name, "rollout")) {
            listed = true;
            try testing.expectEqualStrings("v2", entry.routing.current.?.build_id);
        }
    }
    try testing.expect(listed);

    var missing = try c.workerDeployment("no-such-deployment");
    defer missing.deinit();
    try testing.expectError(error.NotFound, missing.describe(a));
}

test "WorkflowReplayer replays a batch of histories through one worker and reports each outcome" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var r: WorkflowReplayer = .init(testing.allocator, testing.io);
    defer r.deinit();
    try r.registerWorkflow(Fixtures.Hello);
    try r.registerWorkflow(Fixtures.MemoUpdatesChanged);

    const hello = try history_mod.fromJson(a, hello_history);
    const hello_run_id = hello.events.items[0].attributes.?.workflow_execution_started_event_attributes.original_execution_run_id;
    const histories = [_]history_mod.WorkflowHistory{
        .{ .workflow_id = "hello-1", .history = hello },
        .{ .workflow_id = "memo", .history = try history_mod.fromJson(a, upsert_memo_history) },
        .{ .workflow_id = "empty", .history = .{} },
        .{ .workflow_id = "hello-2", .history = hello },
    };
    const results = try r.replayHistories(a, &histories);
    try testing.expectEqual(@as(usize, 4), results.len);

    try testing.expectEqualStrings("hello-1", results[0].workflow_id);
    try testing.expectEqualStrings(hello_run_id, results[0].run_id);
    try testing.expectEqual(@as(?WorkflowReplayer.ReplayError, null), results[0].err);
    try testing.expectEqualStrings("", results[0].message);

    try testing.expectEqualStrings("memo", results[1].workflow_id);
    try testing.expectEqual(@as(?WorkflowReplayer.ReplayError, error.Nondeterminism), results[1].err);
    try testing.expect(std.mem.startsWith(u8, results[1].message, "NONDETERMINISM: "));

    try testing.expectEqual(@as(?WorkflowReplayer.ReplayError, error.InvalidHistory), results[2].err);
    try testing.expectEqualStrings("", results[2].run_id);

    // A failed history doesn't stop the ones after it.
    try testing.expectEqualStrings("hello-2", results[3].workflow_id);
    try testing.expectEqual(@as(?WorkflowReplayer.ReplayError, null), results[3].err);

    // The same, from an iterator.
    const Feed = struct {
        items: []const history_mod.WorkflowHistory,
        i: usize = 0,
        pub fn next(f: *@This(), _: Allocator) error{}!?history_mod.WorkflowHistory {
            if (f.i == f.items.len) return null;
            defer f.i += 1;
            return f.items[f.i];
        }
    };
    var feed: Feed = .{ .items = histories[0..2] };
    const fed = try r.replayHistories(a, &feed);
    try testing.expectEqual(@as(usize, 2), fed.len);
    try testing.expectEqual(@as(?WorkflowReplayer.ReplayError, null), fed[0].err);
    try testing.expectEqual(@as(?WorkflowReplayer.ReplayError, error.Nondeterminism), fed[1].err);

    try testing.expectEqual(@as(usize, 0), (try r.replayHistories(a, &[_]history_mod.WorkflowHistory{})).len);

    // A run waiting for its first workflow task (a cron or delayed start) has only its
    // started event; it replays cleanly instead of waiting for an activation that never comes.
    var started_only: history_mod.History = .{};
    try started_only.events.append(a, hello.events.items[0]);
    const waiting = try r.replayHistories(a, &[_]history_mod.WorkflowHistory{
        .{ .workflow_id = "waiting", .history = started_only },
        .{ .workflow_id = "hello-3", .history = hello },
    });
    try testing.expectEqual(@as(usize, 2), waiting.len);
    try testing.expectEqual(@as(?WorkflowReplayer.ReplayError, null), waiting[0].err);
    try testing.expectEqualStrings(hello_run_id, waiting[0].run_id);
    try testing.expectEqual(@as(?WorkflowReplayer.ReplayError, null), waiting[1].err);
    try r.replayHistory(started_only);
}

// The checks Session.replay makes before the core sees a history, which the core would panic
// on. tools/checks/replay_malformed.zig replays such histories through the core.
test "WorkflowReplayer rejects histories the core can't build a workflow task from, naming the problem" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const problem = WorkflowReplayer.replayProblem;

    try testing.expectEqual(@as(?[]const u8, null), try problem(a, try history_mod.fromJson(a, hello_history)));

    // (a) No events.
    try testing.expectEqualStrings("the history has no events", (try problem(a, .{})).?);

    // (b) The first event isn't a WorkflowExecutionStarted event.
    var no_start = try history_mod.fromJson(a, hello_history);
    _ = no_start.events.orderedRemove(0);
    try testing.expectEqualStrings("the history's first event must be a WorkflowExecutionStarted event", (try problem(a, no_start)).?);

    // (c) No workflow type.
    var no_type = try history_mod.fromJson(a, hello_history);
    no_type.events.items[0].attributes.?.workflow_execution_started_event_attributes.workflow_type = null;
    try testing.expectEqualStrings("the history's WorkflowExecutionStarted event names no workflow type", (try problem(a, no_type)).?);

    // (d) No task queue; an empty name is accepted, as the core accepts it.
    var no_queue = try history_mod.fromJson(a, hello_history);
    const queue = &no_queue.events.items[0].attributes.?.workflow_execution_started_event_attributes.task_queue;
    queue.* = null;
    try testing.expectEqualStrings("the history's WorkflowExecutionStarted event names no task queue", (try problem(a, no_queue)).?);
    queue.* = .{ .name = "" };
    try testing.expectEqual(@as(?[]const u8, null), try problem(a, no_queue));

    // (e) A workflow task started event (3) followed by something other than its
    // completion, failure or timeout, or the run's termination or timeout.
    var unfinished = try history_mod.fromJson(a, hello_history);
    _ = unfinished.events.orderedRemove(3);
    try testing.expectEqualStrings(
        "workflow task started event 3 is followed by event 5 (EVENT_TYPE_ACTIVITY_TASK_SCHEDULED), not the task's completion, failure or timeout or the run's termination or timeout",
        (try problem(a, unfinished)).?,
    );
    // Only event_type is read, as the core reads it, not the attributes the event carries.
    var relabeled = try history_mod.fromJson(a, hello_history);
    const completed = &relabeled.events.items[3];
    for ([_]EventType{
        .EVENT_TYPE_WORKFLOW_TASK_FAILED,
        .EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT,
        .EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
        .EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT,
    }) |accepted| {
        completed.event_type = accepted;
        try testing.expectEqual(@as(?[]const u8, null), try problem(a, relabeled));
    }
    completed.event_type = .EVENT_TYPE_TIMER_STARTED;
    try testing.expectEqualStrings(
        "workflow task started event 3 is followed by event 4 (EVENT_TYPE_TIMER_STARTED), not the task's completion, failure or timeout or the run's termination or timeout",
        (try problem(a, relabeled)).?,
    );
    completed.event_type = @fromBackingInt(-7);
    try testing.expectEqualStrings(
        "workflow task started event 3 is followed by event 4 (event type -7), not the task's completion, failure or timeout or the run's termination or timeout",
        (try problem(a, relabeled)).?,
    );
    // A history that ends on a workflow task started event is complete to the core.
    var open_task = try history_mod.fromJson(a, hello_history);
    open_task.events.shrinkRetainingCapacity(3);
    try testing.expectEqual(@as(?[]const u8, null), try problem(a, open_task));

    // (f) Completed (or last) workflow task started events with the same event ID as the
    // previous one, or 0 for the first.
    var repeated = try history_mod.fromJson(a, hello_history);
    repeated.events.items[8].event_id = 3;
    try testing.expectEqualStrings("two workflow task started events have event ID 3", (try problem(a, repeated)).?);
    var last_repeated = try history_mod.fromJson(a, hello_history);
    last_repeated.events.shrinkRetainingCapacity(9);
    last_repeated.events.items[8].event_id = 3;
    try testing.expectEqualStrings("two workflow task started events have event ID 3", (try problem(a, last_repeated)).?);
    var zero = try history_mod.fromJson(a, hello_history);
    zero.events.items[2].event_id = 0;
    try testing.expectEqualStrings("the workflow task started event at position 3 has event ID 0", (try problem(a, zero)).?);
    // A started event whose task failed doesn't count as the previous one.
    var failed_task = try history_mod.fromJson(a, hello_history);
    failed_task.events.items[3].event_type = .EVENT_TYPE_WORKFLOW_TASK_FAILED;
    failed_task.events.items[8].event_id = 3;
    try testing.expectEqual(@as(?[]const u8, null), try problem(a, failed_task));
    failed_task.events.items[8].event_id = 0;
    try testing.expectEqualStrings("the workflow task started event at position 9 has event ID 0", (try problem(a, failed_task)).?);

    // (g) A first event ID above 1.
    var shifted = try history_mod.fromJson(a, hello_history);
    shifted.events.items[0].event_id = 2;
    try testing.expectEqualStrings("the history's first event has event ID 2; a full history starts at event 1", (try problem(a, shifted)).?);

    // (h) A history ending before its last workflow task started event; ending on it is fine.
    var short = try history_mod.fromJson(a, hello_history);
    short.events.items[8].event_id = 12;
    try testing.expectEqualStrings("the history ends at event ID 11, before its last workflow task started event, 12", (try problem(a, short)).?);
    short.events.items[8].event_id = 11;
    try testing.expectEqual(@as(?[]const u8, null), try problem(a, short));
}

// A history whose first event ID is above 1 looks to the core like a sticky-queue task for a
// run it hasn't cached, so it fetches the full history from the replay worker's mocked client,
// which panics ("No matching expectation found" for get_workflow_execution_history), and
// replayHistory/replayHistories then never return, not even for the valid histories after it.
test "WorkflowReplayer rejects a history whose first event ID isn't 1" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var shifted = try history_mod.fromJson(a, hello_history);
    shifted.events.items[0].event_id = 2;
    try testing.expect(try WorkflowReplayer.replayProblem(a, shifted) != null);
}

// When the last event's ID is below the final workflow task started event's ID, the core's
// HistoryPaginator expects more history than the poll gave it and fails the poll with "We
// expected to be able to fetch more events but server says there are none"; the run is never
// activated or evicted, so replayHistory/replayHistories never return.
test "WorkflowReplayer rejects a history whose last workflow task started event ID is past its last event" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var ahead = try history_mod.fromJson(a, hello_history);
    ahead.events.items[8].event_id = 12; // The second workflow task's started event; the last event is 11.
    try testing.expect(try WorkflowReplayer.replayProblem(a, ahead) != null);
}

test "WorkflowReplayer replays a run fetched through a client, seeing the options' task queue" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(Fixtures.QueueDependent);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var run = try env.startWorkflow(Fixtures.QueueDependent, .{ .task_queue = "" }, .{});
    defer run.deinit();
    // getResult replays the run with the environment's task queue too.
    try testing.expectEqualStrings("timer", try env.getResult(Fixtures.QueueDependent, &run, a, null));

    var r: WorkflowReplayer = .initWithOptions(testing.allocator, testing.io, .{ .task_queue = env.task_queue });
    defer r.deinit();
    try r.registerWorkflow(Fixtures.QueueDependent);
    const ok = try r.replayWorkflowExecution(a, &env.client, run.id, "");
    try testing.expectEqual(@as(?WorkflowReplayer.ReplayError, null), ok.err);
    try testing.expectEqualStrings(run.id, ok.workflow_id);
    try testing.expectEqualStrings(run.run_id, ok.run_id);

    // On the default task queue the workflow skips the timer its history recorded.
    var defaults: WorkflowReplayer = .init(testing.allocator, testing.io);
    defer defaults.deinit();
    try defaults.registerWorkflow(Fixtures.QueueDependent);
    const changed = try defaults.replayWorkflowExecution(a, &env.client, run.id, run.run_id);
    try testing.expectEqual(@as(?WorkflowReplayer.ReplayError, error.Nondeterminism), changed.err);
    try testing.expect(changed.message.len > 0);

    try testing.expectError(error.NotFound, r.replayWorkflowExecution(a, &env.client, "no-such-workflow", ""));
}

test "WorkflowReplayer replays every run a visibility query lists and reports each outcome" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(Fixtures.Hello);
    try env.registerActivity(Fixtures.Greet);
    try env.registerWorkflow(Fixtures.QueueDependent);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const runs = 3;
    var run_ids: [runs][]const u8 = undefined;
    for (&run_ids, 0..) |*run_id, i| {
        const id = try std.fmt.allocPrint(a, "listed-hello-{d}", .{i});
        var run = try env.startWorkflow(Fixtures.Hello, .{ .id = id, .task_queue = "" }, .{"Temporal"});
        defer run.deinit();
        try testing.expectEqualStrings("Hello Temporal!", try env.getResult(Fixtures.Hello, &run, a, null));
        run_id.* = try a.dupe(u8, run.run_id);
    }
    for (0..2) |_| {
        var run = try env.startWorkflow(Fixtures.QueueDependent, .{ .task_queue = "" }, .{});
        defer run.deinit();
        try testing.expectEqualStrings("timer", try env.getResult(Fixtures.QueueDependent, &run, a, null));
    }
    const query = try std.fmt.allocPrint(a, "TaskQueue='{s}' AND WorkflowType='HelloWorld'", .{env.task_queue});
    try waitForCount(&env.client, try std.fmt.allocPrint(a, "{s} AND ExecutionStatus='Completed'", .{query}), runs);
    try waitForCount(&env.client, try std.fmt.allocPrint(a, "TaskQueue='{s}' AND WorkflowType='QueueDependent' AND ExecutionStatus='Completed'", .{env.task_queue}), 2);

    // Only Hello is registered: listing QueueDependent's runs too would fail them.
    var r: WorkflowReplayer = .initWithOptions(testing.allocator, testing.io, .{ .task_queue = env.task_queue });
    defer r.deinit();
    try r.registerWorkflow(Fixtures.Hello);
    const results = try r.replayListedWorkflows(a, &env.client, query);
    try testing.expectEqual(@as(usize, runs), results.len);
    var seen: [runs]bool = @splat(false);
    for (results) |result| {
        try testing.expectEqual(@as(?WorkflowReplayer.ReplayError, null), result.err);
        try testing.expectEqualStrings("", result.message);
        try testing.expect(std.mem.startsWith(u8, result.workflow_id, "listed-hello-"));
        for (run_ids, 0..) |run_id, i| {
            if (std.mem.eql(u8, run_id, result.run_id)) {
                try testing.expect(!seen[i]);
                seen[i] = true;
                break;
            }
        } else return error.TestUnexpectedResult;
    }

    var changed: WorkflowReplayer = .initWithOptions(testing.allocator, testing.io, .{ .task_queue = env.task_queue });
    defer changed.deinit();
    try changed.registerWorkflow(Fixtures.Different);
    const changed_results = try changed.replayListedWorkflows(a, &env.client, query);
    try testing.expectEqual(@as(usize, runs), changed_results.len);
    for (changed_results) |result| {
        try testing.expectEqual(@as(?WorkflowReplayer.ReplayError, error.Nondeterminism), result.err);
        try testing.expect(std.mem.startsWith(u8, result.message, "NONDETERMINISM: "));
        try testing.expect(std.mem.startsWith(u8, result.workflow_id, "listed-hello-"));
        try testing.expect(result.run_id.len > 0);
    }

    const none = try r.replayListedWorkflows(a, &env.client, "WorkflowType='NoSuchWorkflow'");
    try testing.expectEqual(@as(usize, 0), none.len);
}

const ListedReplayCall = struct {
    replayer: *WorkflowReplayer,
    arena: Allocator,
    client: *client_mod.Client,
    query: []const u8,
    results: WorkflowReplayer.ReplayListedError![]WorkflowReplayer.ReplayResult = &.{},
    done: Io.Event = .unset,

    fn run(call: *ListedReplayCall) void {
        call.results = call.replayer.replayListedWorkflows(call.arena, call.client, call.query);
        call.done.set(testing.io);
    }
};

test "WorkflowReplayer replayListedWorkflows returns when a listed run has no workflow task yet" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(Fixtures.Hello);
    try env.registerActivity(Fixtures.Greet);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var completed = try env.startWorkflow(Fixtures.Hello, .{ .id = "listed-no-task-completed", .task_queue = "" }, .{"Temporal"});
    defer completed.deinit();
    try testing.expectEqualStrings("Hello Temporal!", try env.getResult(Fixtures.Hello, &completed, a, null));
    // Both runs are open with only their started event: a cron run waiting for its next
    // fire and a delayed start. A query on the workflow type lists them.
    var cron = try env.startWorkflow(Fixtures.Hello, .{ .id = "listed-no-task-cron", .task_queue = "", .cron_schedule = "0 0 1 1 *" }, .{"Temporal"});
    defer cron.deinit();
    var delayed = try env.startWorkflow(Fixtures.Hello, .{ .id = "listed-no-task-delayed", .task_queue = "", .start_delay = .fromSeconds(3600) }, .{"Temporal"});
    defer delayed.deinit();
    const query = "WorkflowId STARTS_WITH 'listed-no-task-'";
    try waitForCount(&env.client, query, 3);
    try waitForCount(&env.client, "WorkflowId STARTS_WITH 'listed-no-task-' AND ExecutionStatus='Completed'", 1);

    var r: WorkflowReplayer = .initWithOptions(testing.allocator, testing.io, .{ .task_queue = env.task_queue });
    defer r.deinit();
    try r.registerWorkflow(Fixtures.Hello);
    var call: ListedReplayCall = .{ .replayer = &r, .arena = a, .client = &env.client, .query = query };
    const thread = try std.Thread.spawn(.{}, ListedReplayCall.run, .{&call});
    // A replay that never returns can't be cleaned up after: stop the dev server, so it
    // doesn't outlive the test process, and end the test run.
    call.done.waitTimeout(testing.io, .{ .duration = .{ .raw = .fromSeconds(60), .clock = .awake } }) catch |err| {
        env.deinit();
        std.debug.panic("replayListedWorkflows(\"{s}\") hasn't returned after 60s ({t}): it hangs on a listed run whose history has no workflow task", .{ query, err });
    };
    thread.join();
    const results = try call.results;
    try testing.expectEqual(@as(usize, 3), results.len);
    for (results) |result| {
        if (std.mem.eql(u8, result.workflow_id, "listed-no-task-completed")) {
            try testing.expectEqual(@as(?WorkflowReplayer.ReplayError, null), result.err);
        }
    }
}

test "client: a history iterator pages lazily, matches fetchHistory, and filters the close event" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(Fixtures.Hello);
    try env.registerActivity(Fixtures.Greet);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var run = try env.startWorkflow(Fixtures.Hello, .{ .task_queue = "" }, .{"pages"});
    defer run.deinit();
    _ = try env.getResult(Fixtures.Hello, &run, a, null);
    const whole = (try env.client.fetchHistory(a, run.id, run.run_id)).events.items;

    var it = try env.client.historyIterator(run.id, run.run_id, .{ .page_size = 2 });
    defer it.deinit();
    var count: usize = 0;
    while (try it.next(a)) |event| : (count += 1) {
        // Two events a page: after the first, more pages remain.
        if (count == 0) try testing.expect(it.next_page_token.len > 0);
        try testing.expect(count < whole.len);
        try testing.expectEqual(whole[count].event_id, event.event_id);
        try testing.expectEqual(whole[count].event_type, event.event_type);
    }
    try testing.expectEqual(whole.len, count);

    var close = try env.client.historyIterator(run.id, run.run_id, .{ .filter = .close_event });
    defer close.deinit();
    const last = (try close.next(a)) orelse return error.TestExpectedCloseEvent;
    try testing.expectEqual(EventType.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, last.event_type);
    try testing.expectEqual(whole[whole.len - 1].event_id, last.event_id);
    try testing.expectEqual(null, try close.next(a));

    var missing = try env.client.historyIterator("no-such-workflow", "", .{});
    defer missing.deinit();
    try testing.expectError(error.NotFound, missing.next(a));
}

const EventType = proto.temporal.api.enums.v1.EventType;

/// What `readHistoryToEnd` saw.
const HistoryTail = struct {
    events: usize = 0,
    signaled: bool = false,
    last: EventType = .EVENT_TYPE_UNSPECIFIED,
};

fn readHistoryToEnd(it: *client_mod.HistoryIterator) client_mod.HistoryError!HistoryTail {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var tail: HistoryTail = .{};
    while (try it.next(arena.allocator())) |event| {
        tail.events += 1;
        tail.signaled = tail.signaled or event.event_type == .EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED;
        tail.last = event.event_type;
    }
    return tail;
}

test "client: a waiting history iterator follows a running workflow until it closes" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(SignalFixtures.AwaitApprovals);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var run = try env.startWorkflow(SignalFixtures.AwaitApprovals, .{ .task_queue = "" }, .{1});
    defer run.deinit();

    var all = try env.client.historyIterator(run.id, run.run_id, .{ .wait_new_event = true });
    defer all.deinit();
    // Up to the first completed workflow task: then the workflow waits for a signal.
    while (try all.next(a)) |event| {
        if (event.event_type == .EVENT_TYPE_WORKFLOW_TASK_COMPLETED) break;
    } else return error.TestExpectedWorkflowTaskCompleted;

    // Without waiting, iteration stops at the events recorded so far.
    var now = try env.client.historyIterator(run.id, run.run_id, .{});
    defer now.deinit();
    const so_far = try readHistoryToEnd(&now);
    try testing.expectEqual(EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED, so_far.last);

    var close = try env.client.historyIterator(run.id, run.run_id, .{ .wait_new_event = true, .filter = .close_event });
    defer close.deinit();
    var tail_future = try env.io.concurrent(readHistoryToEnd, .{&all});
    var close_future = env.io.concurrent(readHistoryToEnd, .{&close}) catch |err| {
        _ = tail_future.cancel(env.io) catch {};
        return err;
    };
    env.signalWorkflow(SignalFixtures.Approve, run.id, .{"ann"}) catch |err| {
        _ = tail_future.cancel(env.io) catch {};
        _ = close_future.cancel(env.io) catch {};
        return err;
    };
    const tail_result = tail_future.await(env.io);
    const close_result = close_future.await(env.io);
    const tail = try tail_result;
    const closed = try close_result;

    try testing.expect(tail.signaled);
    try testing.expectEqual(EventType.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, tail.last);
    try testing.expectEqual(@as(usize, 1), closed.events);
    try testing.expectEqual(EventType.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, closed.last);
    try testing.expectEqualStrings("ann", try env.getResult(SignalFixtures.AwaitApprovals, &run, a, null));
}

test "continue-as-new: WorkflowRun.get follows the chain to the last run's result" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(ContinueFixtures.CountUp);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var run = try env.startWorkflow(ContinueFixtures.CountUp, .{
        .task_queue = "",
        .run_timeout = .fromSeconds(600),
        .task_timeout = .fromSeconds(7),
    }, .{ 0, 3, "" });
    defer run.deinit();
    const first_run_id = try a.dupe(u8, run.run_id);

    var first = try env.client.getWorkflow([]const u8, run.id, first_run_id);
    defer first.deinit();
    try testing.expectError(error.WorkflowContinuedAsNew, first.getWithOptions(a, .{ .follow_runs = false }));

    try testing.expectEqualStrings("3 round=2 run_timeout=600 task_timeout=7", try env.getResult(ContinueFixtures.CountUp, &run, a, null));
    try testing.expect(!std.mem.eql(u8, first_run_id, run.run_id));
    try env.replayRun(run.id, first_run_id);
}

/// A worker interceptor that records, for each run a workflow function starts in, whether it
/// started live on the worker or in a replay, and the run's ID.
const RunWatch = struct {
    gpa: Allocator,
    io: Io,
    mutex: Io.Mutex = .init,
    live: std.ArrayList([]u8) = .empty,
    replayed: std.ArrayList([]u8) = .empty,

    fn deinit(w: *RunWatch) void {
        for (w.live.items) |id| w.gpa.free(id);
        for (w.replayed.items) |id| w.gpa.free(id);
        w.live.deinit(w.gpa);
        w.replayed.deinit(w.gpa);
    }

    pub fn interceptWorkflow(w: *RunWatch, ctx: *workflow.Context) error{OutOfMemory}!interceptor.Workflow {
        const run = try ctx.allocator().create(Run);
        run.* = .{ .w = w, .ctx = ctx };
        return .{ .inbound = interceptor.WorkflowInbound.from(Run, run) };
    }

    const Run = struct {
        w: *RunWatch,
        ctx: *workflow.Context,

        pub fn executeWorkflow(r: *Run, input: interceptor.ExecuteWorkflowInput, next: interceptor.WorkflowInbound.Next) anyerror!converter.Payload {
            const w = r.w;
            const id = try w.gpa.dupe(u8, r.ctx.info().run_id);
            w.mutex.lockUncancelable(w.io);
            defer w.mutex.unlock(w.io);
            const list = if (r.ctx.isReplaying()) &w.replayed else &w.live;
            list.append(w.gpa, id) catch |err| {
                w.gpa.free(id);
                return err;
            };
            return next.executeWorkflow(input);
        }
    };

    fn count(w: *RunWatch, replayed: bool) usize {
        w.mutex.lockUncancelable(w.io);
        defer w.mutex.unlock(w.io);
        return (if (replayed) w.replayed else w.live).items.len;
    }

    fn lastReplayed(w: *RunWatch) ?[]const u8 {
        w.mutex.lockUncancelable(w.io);
        defer w.mutex.unlock(w.io);
        return if (w.replayed.items.len > 0) w.replayed.items[w.replayed.items.len - 1] else null;
    }
};

const ChainFixtures = struct {
    /// Sleeps `seconds`, then continues as new once; the second run returns at once.
    fn sleepThenContinue(ctx: *workflow.Context, seconds: u32, continued: bool) ![]const u8 {
        if (continued) return "second run";
        try ctx.io().sleep(.fromSeconds(seconds), .awake);
        return ctx.continueAsNew(SleepThenContinue, .{}, .{ seconds, true });
    }
    const SleepThenContinue = workflow.Def("SleepThenContinue", sleepThenContinue);

    /// Continues as new once; the second run returns what `HoldPastLongPoll` returns.
    fn continueThenHold(ctx: *workflow.Context, continued: bool) ![]const u8 {
        if (!continued) return ctx.continueAsNew(ContinueThenHold, .{}, .{true});
        return ctx.executeActivity(HoldPastLongPoll, .{ .start_to_close_timeout = .fromSeconds(120) }, .{}).get();
    }
    const ContinueThenHold = workflow.Def("ContinueThenHold", continueThenHold);

    /// Takes 21 s of wall-clock time, longer than the time-skipping server's 20 s history
    /// long poll. A scheduled activity locks time skipping, so the run stays open meanwhile.
    fn holdPastLongPoll(ctx: *activity.Context) ![]const u8 {
        try ctx.io().sleep(.fromSeconds(21), .awake);
        return "held";
    }
    const HoldPastLongPoll = activity.Def("HoldPastLongPoll", holdPastLongPoll);
};

test "getResult keeps waiting after the time-skipping server's history long poll ends with an empty page" {
    const env = try WorkflowEnvironment.startTimeSkipping(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(ChainFixtures.ContinueThenHold);
    try env.registerActivity(ChainFixtures.HoldPastLongPoll);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // The server ends a history long poll after 20 s of wall-clock time with a page that has
    // no events and no token; the second run is still open then.
    var run = try env.startWorkflow(ChainFixtures.ContinueThenHold, .{ .task_queue = "" }, .{false});
    defer run.deinit();
    const first_run_id = try a.dupe(u8, run.run_id);
    try testing.expectEqualStrings("held", try env.getResult(ChainFixtures.ContinueThenHold, &run, a, null));
    try testing.expect(!std.mem.eql(u8, first_run_id, run.run_id));
}

test "WorkflowEnvironment.getResultWithOptions: without following runs it stops at a run that continued as new and replays that run" {
    for (env_kinds) |kind| {
        var watch: RunWatch = .{ .gpa = testing.allocator, .io = testing.io };
        defer watch.deinit();
        const interceptors = [_]interceptor.Worker{interceptor.Worker.from(RunWatch, &watch)};
        const env = try WorkflowEnvironment.create(testing.allocator, testing.io, kind, .{ .worker_interceptors = &interceptors });
        defer env.deinit();
        try env.registerWorkflow(ChainFixtures.SleepThenContinue);
        try env.registerWorkflow(Fixtures.Queryable);
        var arena: std.heap.ArenaAllocator = .init(testing.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        // A day on the time-skipping server, which must be unlocked for the wait to end.
        const seconds: u32 = if (kind == .time_skipping) std.time.s_per_day else 1;

        // By type.
        var run = try env.startWorkflow(ChainFixtures.SleepThenContinue, .{ .task_queue = "" }, .{ seconds, false });
        defer run.deinit();
        const first_run_id = try a.dupe(u8, run.run_id);
        try testing.expectError(error.WorkflowContinuedAsNew, env.getResultWithOptions(ChainFixtures.SleepThenContinue, &run, .{ .follow_runs = false }, a, null));
        try testing.expectEqualStrings(first_run_id, run.run_id);
        try testing.expectEqualStrings(first_run_id, watch.lastReplayed() orelse return error.TestUnexpectedResult);
        // Following the chain from there gets the last run's result and replays that run.
        try testing.expectEqualStrings("second run", try env.getResultWithOptions(ChainFixtures.SleepThenContinue, &run, .{ .follow_runs = true }, a, null));
        try testing.expect(!std.mem.eql(u8, first_run_id, run.run_id));
        try testing.expectEqualStrings(run.run_id, watch.lastReplayed().?);

        // By name.
        var by_name = try env.startWorkflowByName([]const u8, ChainFixtures.SleepThenContinue.workflow_name, .{ .task_queue = "" }, .{ seconds, false });
        defer by_name.deinit();
        const first_by_name = try a.dupe(u8, by_name.run_id);
        try testing.expectError(error.WorkflowContinuedAsNew, env.getResultByNameWithOptions([]const u8, &by_name, .{ .follow_runs = false }, a, null));
        try testing.expectEqualStrings(first_by_name, by_name.run_id);
        try testing.expectEqualStrings(first_by_name, watch.lastReplayed().?);
        try testing.expectEqualStrings("second run", try env.getResultByName([]const u8, &by_name, a, null));
        try testing.expectEqualStrings(by_name.run_id, watch.lastReplayed().?);

        if (kind == .time_skipping) {
            // Time is locked again: a workflow in the middle of a 2h timer stays there.
            const start = try env.now();
            var sleeper = try env.startWorkflow(Fixtures.Queryable, .{ .task_queue = "" }, .{2 * std.time.s_per_hour});
            defer sleeper.deinit();
            try Fixtures.waitForTimer(&env.client, sleeper.id, sleeper.run_id);
            try testing.expectEqualStrings("sleeping", try env.client.queryWorkflow(Fixtures.Status, a, sleeper.id, sleeper.run_id, .{}));
            try testing.expect(start.durationTo(try env.now()).nanoseconds < std.time.ns_per_hour);
            try testing.expectEqualStrings("finished", try env.getResult(Fixtures.Queryable, &sleeper, a, null));
        }
    }
}

const RunEndFixtures = struct {
    fn cronRun(ctx: *workflow.Context) ![]const u8 {
        _ = ctx;
        return "cron run";
    }
    const CronRun = workflow.Def("CronRun", cronRun);
};

test "WorkflowEnvironment.getResultWithOptions: without following runs a retried run on the dev server returns its own failure" {
    var watch: RunWatch = .{ .gpa = testing.allocator, .io = testing.io };
    defer watch.deinit();
    const interceptors = [_]interceptor.Worker{interceptor.Worker.from(RunWatch, &watch)};
    const env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{ .worker_interceptors = &interceptors });
    defer env.deinit();
    try env.registerWorkflow(StartOptionFixtures.FailFirstAttempt);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var run = try env.startWorkflow(StartOptionFixtures.FailFirstAttempt, .{
        .task_queue = "",
        .retry_policy = .{ .initial_interval = .fromSeconds(1), .maximum_attempts = 2 },
    }, .{});
    defer run.deinit();
    const first_run_id = try a.dupe(u8, run.run_id);
    // The doc comment: "a cron or retry run returns its own result or failure".
    var why: ?failure_mod.Failure = null;
    try testing.expectError(error.WorkflowFailed, env.getResultWithOptions(StartOptionFixtures.FailFirstAttempt, &run, .{ .follow_runs = false }, a, &why));
    try testing.expect(why != null);
    try testing.expectEqualStrings(first_run_id, run.run_id);
    try testing.expectEqualStrings(first_run_id, watch.lastReplayed() orelse return error.TestUnexpectedResult);
    try testing.expectEqual(@as(u32, 2), try env.getResult(StartOptionFixtures.FailFirstAttempt, &run, a, null));
}

test "time-skipping WorkflowEnvironment.getResultWithOptions: a retried or cron run without following runs passes its replay" {
    const env = try WorkflowEnvironment.startTimeSkipping(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(StartOptionFixtures.FailFirstAttempt);
    try env.registerWorkflow(RunEndFixtures.CronRun);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // A retried run: the workflow is deterministic, so the replay of its first attempt must
    // not report nondeterminism.
    var retried = try env.startWorkflow(StartOptionFixtures.FailFirstAttempt, .{
        .task_queue = "",
        .retry_policy = .{ .initial_interval = .fromSeconds(std.time.s_per_hour), .maximum_attempts = 2 },
    }, .{});
    defer retried.deinit();
    const first_attempt = try a.dupe(u8, retried.run_id);
    if (env.getResultWithOptions(StartOptionFixtures.FailFirstAttempt, &retried, .{ .follow_runs = false }, a, null)) |_| {
        return error.TestUnexpectedResult;
    } else |err| switch (err) {
        error.WorkflowFailed, error.WorkflowContinuedAsNew => {},
        else => return err,
    }
    try testing.expectEqualStrings(first_attempt, retried.run_id);
    try testing.expectEqual(@as(u32, 2), try env.getResult(StartOptionFixtures.FailFirstAttempt, &retried, a, null));

    // A cron run: the same for its first run.
    var cron = try env.startWorkflow(RunEndFixtures.CronRun, .{ .task_queue = "", .cron_schedule = "0 * * * *" }, .{});
    defer cron.deinit();
    defer env.client.terminateWorkflow(cron.id, "", "test cleanup") catch {};
    if (env.getResultWithOptions(RunEndFixtures.CronRun, &cron, .{ .follow_runs = false }, a, null)) |result| {
        try testing.expectEqualStrings("cron run", result);
    } else |err| switch (err) {
        error.WorkflowContinuedAsNew => {},
        else => return err,
    }
}

test "time-skipping WorkflowEnvironment.getResultWithOptions: without following runs a retried run returns its failure and a cron run its result" {
    var watch: RunWatch = .{ .gpa = testing.allocator, .io = testing.io };
    defer watch.deinit();
    const interceptors = [_]interceptor.Worker{interceptor.Worker.from(RunWatch, &watch)};
    const env = try WorkflowEnvironment.startTimeSkippingWithOptions(testing.allocator, testing.io, .{ .worker_interceptors = &interceptors });
    defer env.deinit();
    try env.registerWorkflow(StartOptionFixtures.FailFirstAttempt);
    try env.registerWorkflow(RunEndFixtures.CronRun);
    try env.registerWorkflow(ChainFixtures.SleepThenContinue);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var retried = try env.startWorkflow(StartOptionFixtures.FailFirstAttempt, .{
        .task_queue = "",
        .retry_policy = .{ .initial_interval = .fromSeconds(std.time.s_per_hour), .maximum_attempts = 2 },
    }, .{});
    defer retried.deinit();
    const first_attempt = try a.dupe(u8, retried.run_id);
    var why: ?failure_mod.Failure = null;
    try testing.expectError(error.WorkflowFailed, env.getResultWithOptions(StartOptionFixtures.FailFirstAttempt, &retried, .{ .follow_runs = false }, a, &why));
    try testing.expectEqualStrings("FirstAttemptFails", why.?.message);
    try testing.expectEqualStrings(first_attempt, watch.lastReplayed().?);

    var cron = try env.startWorkflow(RunEndFixtures.CronRun, .{ .task_queue = "", .cron_schedule = "0 * * * *" }, .{});
    defer cron.deinit();
    defer env.client.terminateWorkflow(cron.id, "", "test cleanup") catch {};
    const first_cron = try a.dupe(u8, cron.run_id);
    try testing.expectEqualStrings("cron run", try env.getResultWithOptions(RunEndFixtures.CronRun, &cron, .{ .follow_runs = false }, a, null));
    try testing.expectEqualStrings(first_cron, cron.run_id);
    try testing.expectEqualStrings(first_cron, watch.lastReplayed().?);

    // A run the workflow continued as new still says so.
    var chain = try env.startWorkflow(ChainFixtures.SleepThenContinue, .{ .task_queue = "" }, .{ 60, false });
    defer chain.deinit();
    try testing.expectError(error.WorkflowContinuedAsNew, env.getResultWithOptions(ChainFixtures.SleepThenContinue, &chain, .{ .follow_runs = false }, a, null));
}

const OwnOutcomeFixtures = struct {
    /// Continues as new itself on its first run, whatever its retry policy.
    fn continueOnce(ctx: *workflow.Context, continued: bool) !u32 {
        if (continued) return ctx.info().attempt;
        return ctx.continueAsNew(ContinueOnce, .{}, .{true});
    }
    const ContinueOnce = workflow.Def("ContinueOnce", continueOnce);

    fn nothing(_: *workflow.Context) !void {}
    const Nothing = workflow.Def("Nothing", nothing);
};

test "time-skipping WorkflowEnvironment.getResultWithOptions: a run with a retry policy that continued as new itself, and a void cron run, return their own outcome" {
    const env = try WorkflowEnvironment.startTimeSkipping(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(OwnOutcomeFixtures.ContinueOnce);
    try env.registerWorkflow(OwnOutcomeFixtures.Nothing);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var retrying = try env.startWorkflow(OwnOutcomeFixtures.ContinueOnce, .{
        .task_queue = "",
        .retry_policy = .{ .initial_interval = .fromSeconds(std.time.s_per_hour), .maximum_attempts = 3 },
    }, .{false});
    defer retrying.deinit();
    try testing.expectError(error.WorkflowContinuedAsNew, env.getResultWithOptions(OwnOutcomeFixtures.ContinueOnce, &retrying, .{ .follow_runs = false }, a, null));
    try testing.expectEqual(@as(u32, 1), try env.getResult(OwnOutcomeFixtures.ContinueOnce, &retrying, a, null));

    var cron = try env.startWorkflow(OwnOutcomeFixtures.Nothing, .{ .task_queue = "", .cron_schedule = "0 * * * *" }, .{});
    defer cron.deinit();
    defer env.client.terminateWorkflow(cron.id, "", "test cleanup") catch {};
    try env.getResultWithOptions(OwnOutcomeFixtures.Nothing, &cron, .{ .follow_runs = false }, a, null);
}

test "WorkflowEnvironment.getResultWithOptions: without following runs a handle with no run ID gets the retried run's own failure and replays that run" {
    var watch: RunWatch = .{ .gpa = testing.allocator, .io = testing.io };
    defer watch.deinit();
    const interceptors = [_]interceptor.Worker{interceptor.Worker.from(RunWatch, &watch)};
    const env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{ .worker_interceptors = &interceptors });
    defer env.deinit();
    try env.registerWorkflow(StartOptionFixtures.FailFirstAttempt);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var started = try env.startWorkflow(StartOptionFixtures.FailFirstAttempt, .{
        .task_queue = "",
        .retry_policy = .{ .initial_interval = .fromSeconds(1), .maximum_attempts = 2 },
    }, .{});
    defer started.deinit();
    var latest = try env.client.getWorkflow(u32, started.id, "");
    defer latest.deinit();
    var why: ?failure_mod.Failure = null;
    try testing.expectError(error.WorkflowFailed, env.getResultWithOptions(StartOptionFixtures.FailFirstAttempt, &latest, .{ .follow_runs = false }, a, &why));
    try testing.expect(why != null);
    // The handle now names the run it waited for, which is the run replayed.
    try testing.expectEqualStrings(started.run_id, latest.run_id);
    try testing.expectEqualStrings(started.run_id, watch.lastReplayed().?);
    try testing.expectEqual(@as(u32, 2), try env.getResult(StartOptionFixtures.FailFirstAttempt, &latest, a, null));
}

test "WorkflowEnvironment runs local activities, retrying one after workflow timers" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(Fixtures.LocalGreetings);
    try env.registerActivity(Fixtures.Greet);
    try env.registerActivity(Fixtures.FlakyGreet);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var run = try env.startWorkflow(Fixtures.LocalGreetings, .{ .task_queue = "" }, .{"Temporal"});
    defer run.deinit();
    const result = try env.getResult(Fixtures.LocalGreetings, &run, arena.allocator(), null);
    try testing.expectEqualStrings("Hello Temporal! Hello Temporal on attempt 3!", result);
    // Both retries backed off past the local retry threshold, so each waited on a timer.
    const history = try env.client.fetchHistory(arena.allocator(), run.id, run.run_id);
    var timers: usize = 0;
    for (history.events.items) |e| timers += @intFromBool(e.event_type == .EVENT_TYPE_TIMER_FIRED);
    try testing.expectEqual(2, timers);
}

test "activity.Info.is_local is true for a local activity and false for a remote one" {
    const env = try LocalFixtures.start();
    defer env.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const result = try env.executeWorkflow(LocalFixtures.LocalAndRemote, .{}, arena.allocator(), null);
    try testing.expectEqualStrings("local=true remote=false", result);
}

test "a canceled local activity sees the cancel on its Io and reports canceled" {
    const env = try LocalFixtures.start();
    defer env.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const result = try env.executeWorkflow(LocalFixtures.CancelLocal, .{}, arena.allocator(), null);
    try testing.expectEqualStrings("canceled", result);
    // wait_cancellation_completed resolved the future only after the activity returned.
    try testing.expect(LocalFixtures.observed.isSet());
    try testing.expectEqual(@as(?activity.CancelReason, .canceled), LocalFixtures.seen_reason);
}

test "a local activity past its start-to-close or schedule-to-close timeout is canceled and times out" {
    const env = try LocalFixtures.start();
    defer env.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    for ([_]bool{ false, true }) |schedule_to_close| {
        LocalFixtures.reset();
        const result = try env.executeWorkflow(LocalFixtures.TimeOutLocal, .{schedule_to_close}, arena.allocator(), null);
        if (schedule_to_close) {
            // Without a start-to-close timeout the attempt's timer also runs for the
            // schedule-to-close time, so either may fire first.
            try testing.expect(std.mem.startsWith(u8, result, "timeout TIMEOUT_TYPE_"));
        } else {
            try testing.expectEqualStrings("timeout TIMEOUT_TYPE_START_TO_CLOSE", result);
        }
        // The core times the activity out without waiting for it, then cancels it.
        try LocalFixtures.observed.wait(testing.io);
        try testing.expectEqual(@as(?activity.CancelReason, .timed_out), LocalFixtures.seen_reason);
    }
}

test "client: generated IDs, start delay, history, and terminate" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(Fixtures.Hello);
    try env.registerActivity(Fixtures.Greet);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // A delayed start with a generated ID.
    _ = try env.executeWorkflow(Fixtures.Hello, .{"warm-up"}, a, null); // starts the worker
    var run = try env.client.executeWorkflow(Fixtures.Hello, .{ .task_queue = env.task_queue, .start_delay = .fromSeconds(1) }, .{"later"});
    defer run.deinit();
    try testing.expectEqual(@as(usize, 36), run.id.len);
    try testing.expectEqualStrings("Hello later!", try run.get(a));
    const history = try env.client.fetchHistory(a, run.id, run.run_id);
    const started = history.events.items[0].attributes.?.workflow_execution_started_event_attributes;
    try testing.expectEqual(@as(i64, 1), started.first_workflow_task_backoff.?.seconds);

    // Terminate a workflow nobody runs (it waits on a task queue without a worker).
    var stuck = try env.client.executeWorkflow(Fixtures.Hello, .{ .task_queue = "nobody-polls-this" }, .{"never"});
    defer stuck.deinit();
    try env.client.terminateWorkflow(stuck.id, "", "test cleanup");
    try testing.expectError(error.WorkflowTerminated, stuck.get(a));
}

test "client: start a workflow by its type name" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(Fixtures.Hello);
    try env.registerActivity(Fixtures.Greet);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    _ = try env.executeWorkflow(Fixtures.Hello, .{"warm-up"}, a, null); // starts the worker
    var run = try env.client.executeWorkflowByName([]const u8, Fixtures.Hello.workflow_name, .{ .task_queue = env.task_queue }, .{"by name"});
    defer run.deinit();
    try testing.expectEqualStrings("Hello by name!", try run.get(a));

    // An unregistered type fails its workflow task until the run is ended.
    var unknown = try env.client.executeWorkflowByName(void, "NoSuchWorkflow", .{ .task_queue = env.task_queue }, .{});
    defer unknown.deinit();
    try env.client.terminateWorkflow(unknown.id, "", "test cleanup");
    try testing.expectError(error.WorkflowTerminated, unknown.get(a));
}

test "client: cancel ends a run canceled, after any cleanup the workflow does" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(Fixtures.Sleeper);
    try env.registerWorkflow(Fixtures.CleanUpOnCancel);
    try env.registerActivity(Fixtures.Greet);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const timerStarted = struct {
        fn wait(c: *client_mod.Client, id: []const u8, run_id: []const u8) !void {
            while (true) {
                var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
                defer scratch.deinit();
                const history = try c.fetchHistory(scratch.allocator(), id, run_id);
                for (history.events.items) |event| switch (event.attributes orelse continue) {
                    .timer_started_event_attributes => return,
                    else => {},
                };
                try testing.io.sleep(.fromMilliseconds(20), .awake);
            }
        }
    }.wait;

    // A workflow that doesn't catch the cancellation ends canceled at once.
    var sleeper = try env.startWorkflow(Fixtures.Sleeper, .{ .task_queue = "" }, .{std.time.s_per_hour});
    defer sleeper.deinit();
    try timerStarted(&env.client, sleeper.id, sleeper.run_id);
    try env.client.cancelWorkflow(sleeper.id, "", "no longer needed");
    try testing.expectError(error.WorkflowCanceled, env.getResult(Fixtures.Sleeper, &sleeper, a, null));
    const history = try env.client.fetchHistory(a, sleeper.id, sleeper.run_id);
    var requested: ?proto.temporal.api.history.v1.WorkflowExecutionCancelRequestedEventAttributes = null;
    for (history.events.items) |event| switch (event.attributes orelse continue) {
        .workflow_execution_cancel_requested_event_attributes => |attrs| requested = attrs,
        else => {},
    };
    try testing.expectEqualStrings("no longer needed", requested.?.cause);
    try testing.expectEqualStrings(env.client.identity, requested.?.identity);
    const last = history.events.items[history.events.items.len - 1].attributes.?;
    try testing.expect(last == .workflow_execution_canceled_event_attributes);

    // A workflow that catches it runs its cleanup activity, then ends canceled.
    var cleaner = try env.startWorkflow(Fixtures.CleanUpOnCancel, .{ .task_queue = "" }, .{std.time.s_per_hour});
    defer cleaner.deinit();
    try timerStarted(&env.client, cleaner.id, cleaner.run_id);
    try env.client.cancelWorkflow(cleaner.id, cleaner.run_id, "");
    try testing.expectError(error.WorkflowCanceled, env.getResult(Fixtures.CleanUpOnCancel, &cleaner, a, null));
    const cleaned = try env.client.fetchHistory(a, cleaner.id, cleaner.run_id);
    var cleanup_completed = false;
    for (cleaned.events.items) |event| switch (event.attributes orelse continue) {
        .activity_task_completed_event_attributes => cleanup_completed = true,
        else => {},
    };
    try testing.expect(cleanup_completed);
    const cleaned_last = cleaned.events.items[cleaned.events.items.len - 1].attributes.?;
    try testing.expect(cleaned_last == .workflow_execution_canceled_event_attributes);

    try testing.expectError(error.NotFound, env.client.cancelWorkflow("no-such-workflow", "", "nothing to cancel"));
}

test "client: a memo set at start is recorded in the started event" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const Owner = struct { team: []const u8, oncall: bool };
    const memo = try converter.toFields(a, .{
        .description = "nightly import",
        .owner = Owner{ .team = "data", .oncall = true },
    });
    // No worker polls this task queue: the started event is all the test needs.
    var run = try env.client.executeWorkflow(Fixtures.Hello, .{ .task_queue = "nobody-polls-this", .memo = memo }, .{"memo"});
    defer run.deinit();
    defer env.client.terminateWorkflow(run.id, run.run_id, "test cleanup") catch {};

    const history = try env.client.fetchHistory(a, run.id, run.run_id);
    const started = history.events.items[0].attributes.?.workflow_execution_started_event_attributes;
    const entries = started.memo.?.fields.items;
    try testing.expectEqual(@as(usize, 2), entries.len);
    var fields: [2]converter.Field = undefined;
    for (entries, &fields) |entry, *field| field.* = .{ .name = entry.key, .value = entry.value.? };
    try testing.expectEqualStrings("nightly import", (try converter.fieldValue([]const u8, a, &fields, "description")).?);
    const owner = (try converter.fieldValue(Owner, a, &fields, "owner")).?;
    try testing.expectEqualStrings("data", owner.team);
    try testing.expect(owner.oncall);
}

const StartOptionFixtures = struct {
    fn failFirstAttempt(ctx: *workflow.Context) !u32 {
        if (ctx.info().attempt == 1) return error.FirstAttemptFails;
        return ctx.info().attempt;
    }
    const FailFirstAttempt = workflow.Def("FailFirstAttempt", failFirstAttempt);

    /// Fails its first attempt; the second returns
    /// "<previous failure>|<maximum attempts>|<priority key>".
    fn retryInfo(ctx: *workflow.Context) ![]const u8 {
        const info = ctx.info();
        if (info.attempt == 1) return ctx.fail(.{ .message = "first attempt", .type = "Flaky" });
        const previous = ctx.previousRunFailure() orelse return error.NoPreviousFailure;
        return std.fmt.allocPrint(ctx.allocator(), "{s}/{s}|{d}|{d}", .{
            previous.message,
            failure_mod.applicationType(previous.*) orelse "",
            info.retry_policy.?.maximum_attempts,
            info.priority.priority_key,
        });
    }
    const RetryInfo = workflow.Def("RetryInfo", retryInfo);

    fn startedEvent(c: *client_mod.Client, a: Allocator, id: []const u8, run_id: []const u8) !proto.temporal.api.history.v1.HistoryEvent {
        const history = try c.fetchHistory(a, id, run_id);
        try testing.expect(history.events.items[0].attributes.? == .workflow_execution_started_event_attributes);
        return history.events.items[0];
    }

    fn expectMetadata(a: Allocator, event: proto.temporal.api.history.v1.HistoryEvent, summary: []const u8, details: []const u8) !void {
        const metadata = event.user_metadata.?;
        try testing.expectEqualStrings(summary, try converter.fromPayload([]const u8, a, metadata.summary.?));
        if (details.len > 0) {
            try testing.expectEqualStrings(details, try converter.fromPayload([]const u8, a, metadata.details.?));
        } else try testing.expect(metadata.details == null);
    }
};

test "client: a retry policy set at start retries a failed run" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(StartOptionFixtures.FailFirstAttempt);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var run = try env.startWorkflow(StartOptionFixtures.FailFirstAttempt, .{
        .task_queue = "",
        .retry_policy = .{ .initial_interval = .fromSeconds(1), .backoff_coefficient = 2, .maximum_attempts = 2, .non_retryable_error_types = &.{"Fatal"} },
    }, .{});
    defer run.deinit();
    try testing.expectEqual(@as(u32, 2), try run.get(a));

    const started = (try StartOptionFixtures.startedEvent(&env.client, a, run.id, run.run_id)).attributes.?.workflow_execution_started_event_attributes;
    const policy = started.retry_policy.?;
    try testing.expectEqual(@as(i32, 2), policy.maximum_attempts);
    try testing.expectEqual(@as(f64, 2), policy.backoff_coefficient);
    try testing.expectEqual(@as(i64, 1), policy.initial_interval.?.seconds);
    try testing.expectEqualStrings("Fatal", policy.non_retryable_error_types.items[0]);
}

test "workflow: a retried run reads its retry policy, priority, and the previous attempt's failure" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(StartOptionFixtures.RetryInfo);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // Replaying the last run's history covers replay of these reads.
    try testing.expectEqualStrings("first attempt/Flaky|2|2", try env.executeWorkflowWithOptions(StartOptionFixtures.RetryInfo, .{
        .retry_policy = .{ .initial_interval = .fromSeconds(1), .maximum_attempts = 2 },
        .priority = .{ .priority_key = 2 },
    }, .{}, a, null));
}

test "client: summary, details, priority and a versioning override set at start are recorded in the started event" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // The server only pins to a version that has polled the task queue.
    var rollout = try env.client.workerDeployment("rollout");
    defer rollout.deinit();
    var v1: VersionFixtures.VersionWorker = undefined;
    try v1.start(&env.client, "start-override", "v1");
    defer v1.stop();
    try VersionFixtures.awaitVersion(&rollout, "v1");
    while (true) {
        var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
        defer scratch.deinit();
        // The version can be missing here for a moment after the deployment lists it.
        const version = rollout.describeVersion(scratch.allocator(), "v1") catch |err| switch (err) {
            error.NotFound => {
                try testing.io.sleep(.fromMilliseconds(100), .awake);
                continue;
            },
            else => return err,
        };
        const polled = for (version.task_queues) |q| {
            if (q.type == .workflow and std.mem.eql(u8, q.name, "start-override")) break true;
        } else false;
        if (polled) break;
        try testing.io.sleep(.fromMilliseconds(100), .awake);
    }

    // The auto-upgrade workflow is pinned, the pinned one auto-upgrades. Both wait for a
    // signal that never comes; the started event is all the test needs.
    var pinned = try env.client.executeWorkflow(VersionFixtures.AutoRollout, .{
        .task_queue = "start-override",
        .static_summary = "Nightly import",
        .static_details = "Imports **yesterday's** orders.",
        .priority = .{ .priority_key = 2, .fairness_key = "tenant-a", .fairness_weight = 1.5 },
        .versioning_override = .{ .pinned = .{ .deployment_name = "rollout", .build_id = "v1" } },
    }, .{});
    defer pinned.deinit();
    defer env.client.terminateWorkflow(pinned.id, pinned.run_id, "test cleanup") catch {};
    const event = try StartOptionFixtures.startedEvent(&env.client, a, pinned.id, pinned.run_id);
    try StartOptionFixtures.expectMetadata(a, event, "Nightly import", "Imports **yesterday's** orders.");
    const started = event.attributes.?.workflow_execution_started_event_attributes;
    const priority = started.priority.?;
    try testing.expectEqual(@as(i32, 2), priority.priority_key);
    try testing.expectEqualStrings("tenant-a", priority.fairness_key);
    try testing.expectEqual(@as(f32, 1.5), priority.fairness_weight);
    const override = started.versioning_override.?.override.?.pinned;
    try testing.expectEqual(.PINNED_OVERRIDE_BEHAVIOR_PINNED, override.behavior);
    try testing.expectEqualStrings("rollout", override.version.?.deployment_name);
    try testing.expectEqualStrings("v1", override.version.?.build_id);

    var auto = try env.client.executeWorkflow(VersionFixtures.PinnedRollout, .{
        .task_queue = "start-override",
        .static_summary = "Summary only",
        .versioning_override = .auto_upgrade,
    }, .{});
    defer auto.deinit();
    defer env.client.terminateWorkflow(auto.id, auto.run_id, "test cleanup") catch {};
    const auto_event = try StartOptionFixtures.startedEvent(&env.client, a, auto.id, auto.run_id);
    try StartOptionFixtures.expectMetadata(a, auto_event, "Summary only", "");
    try testing.expect(auto_event.attributes.?.workflow_execution_started_event_attributes.versioning_override.?.override.?.auto_upgrade);

    // The defaults send none of them. No worker polls this task queue.
    var plain = try env.client.executeWorkflow(Fixtures.Hello, .{ .task_queue = "nobody-polls-this" }, .{"plain"});
    defer plain.deinit();
    defer env.client.terminateWorkflow(plain.id, plain.run_id, "test cleanup") catch {};
    const plain_event = try StartOptionFixtures.startedEvent(&env.client, a, plain.id, plain.run_id);
    try testing.expect(plain_event.user_metadata == null);
    const plain_started = plain_event.attributes.?.workflow_execution_started_event_attributes;
    try testing.expect(plain_started.versioning_override == null);
    try testing.expect(plain_started.retry_policy == null);
    try testing.expect(plain_started.priority == null);
}

/// A client interceptor that keeps a copy of every terminate, cancel, reset and execution-options
/// request, recorded before the call goes on so a refused call is recorded too. It can play
/// the server for those calls: `options_answer` (or `reset_answer`) answers
/// UpdateWorkflowExecutionOptions (or ResetWorkflowExecution), `fail_with` fails them, and
/// `block` holds them until the calling task is canceled, setting `blocked` first. Copies
/// live in `arena`, which the test frees after the environment.
const RequestCapture = struct {
    const wfs = proto.temporal.api.workflowservice.v1;

    io: Io,
    mutex: Io.Mutex = .init,
    arena: std.heap.ArenaAllocator,
    terminates: std.ArrayList(wfs.TerminateWorkflowExecutionRequest) = .empty,
    cancels: std.ArrayList(wfs.RequestCancelWorkflowExecutionRequest) = .empty,
    resets: std.ArrayList(wfs.ResetWorkflowExecutionRequest) = .empty,
    option_updates: std.ArrayList(wfs.UpdateWorkflowExecutionOptionsRequest) = .empty,
    options_answer: ?wfs.UpdateWorkflowExecutionOptionsResponse = null,
    reset_answer: ?wfs.ResetWorkflowExecutionResponse = null,
    fail_with: ?interceptor.ClientError = null,
    block: bool = false,
    blocked: Io.Event = .unset,

    pub fn call(r: *RequestCapture, input: *interceptor.ClientCall, next: interceptor.Client.Next) interceptor.ClientError!void {
        const fail_with, const block = played: {
            r.mutex.lockUncancelable(r.io);
            defer r.mutex.unlock(r.io);
            const a = r.arena.allocator();
            // The request's memory is the call's arena, freed when the call returns.
            if (input.request(wfs.TerminateWorkflowExecutionRequest)) |req| {
                try r.terminates.append(a, try req.dupe(a));
            } else if (input.request(wfs.RequestCancelWorkflowExecutionRequest)) |req| {
                try r.cancels.append(a, try req.dupe(a));
            } else if (input.request(wfs.ResetWorkflowExecutionRequest)) |req| {
                try r.resets.append(a, try req.dupe(a));
            } else if (input.request(wfs.UpdateWorkflowExecutionOptionsRequest)) |req| {
                try r.option_updates.append(a, try req.dupe(a));
            } else break :played .{ null, false };
            if (r.fail_with == null and !r.block) {
                if (input.response(wfs.UpdateWorkflowExecutionOptionsResponse)) |response| if (r.options_answer) |answer| {
                    response.* = try answer.dupe(input.arena);
                    return;
                };
                if (input.response(wfs.ResetWorkflowExecutionResponse)) |response| if (r.reset_answer) |answer| {
                    response.* = try answer.dupe(input.arena);
                    return;
                };
            }
            break :played .{ r.fail_with, r.block };
        };
        if (fail_with) |err| return err;
        if (block) {
            r.blocked.set(r.io);
            var never: Io.Event = .unset;
            try never.wait(r.io);
            return error.InvalidResponse;
        }
        try next.call(input);
    }

    fn interceptors(r: *RequestCapture) [1]interceptor.Client {
        return .{interceptor.Client.from(RequestCapture, r)};
    }
};

const ResetFixtures = struct {
    /// Continues as new `hops` times, then waits for one approval.
    fn hop(ctx: *workflow.Context, hops: u32) ![]const u8 {
        if (hops > 0) return ctx.continueAsNew(Hop, .{}, .{hops - 1});
        return SignalFixtures.awaitApprovals(ctx, 1);
    }
    const Hop = workflow.Def("ResetHop", hop);

    /// Schedules an activity on a task queue nobody polls, so it stays pending.
    fn pendingActivity(ctx: *workflow.Context) ![]const u8 {
        return ctx.executeActivity(Fixtures.Greet, .{ .task_queue = "reset-pending-activity-nobody-polls", .start_to_close_timeout = .fromSeconds(60) }, .{"never"}).get();
    }
    const PendingActivity = workflow.Def("ResetPendingActivity", pendingActivity);

    const EventTag = std.meta.Tag(proto.temporal.api.history.v1.HistoryEvent.attributes_union);

    /// Waits, long-polling the server, until the run records its `n`th event of kind `tag`,
    /// and returns that event's ID. Fails if the run closes first.
    fn awaitEvent(c: *client_mod.Client, id: []const u8, run_id: []const u8, comptime tag: EventTag, n: usize) !i64 {
        var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
        defer scratch.deinit();
        var it = try c.historyIterator(id, run_id, .{ .wait_new_event = true });
        defer it.deinit();
        var seen: usize = 0;
        while (try it.next(scratch.allocator())) |event| {
            const attrs = event.attributes orelse continue;
            if (attrs != tag) continue;
            seen += 1;
            if (seen == n) return event.event_id;
        }
        return error.TestUnexpectedResult;
    }

    /// Waits until the run's first workflow task has completed, and returns the ID of its
    /// completed event: a point the run can be reset to.
    fn firstTaskCompleted(c: *client_mod.Client, id: []const u8, run_id: []const u8) !i64 {
        return awaitEvent(c, id, run_id, .workflow_task_completed_event_attributes, 1);
    }

    /// The number of events of kind `tag` in the run's history so far.
    fn countEvents(c: *client_mod.Client, a: Allocator, id: []const u8, run_id: []const u8, comptime tag: EventTag) !usize {
        var n: usize = 0;
        for ((try c.fetchHistory(a, id, run_id)).events.items) |event| {
            const attrs = event.attributes orelse continue;
            if (attrs == tag) n += 1;
        }
        return n;
    }

    /// The approvers of the run's signaled events, in history order.
    fn signalers(c: *client_mod.Client, a: Allocator, id: []const u8, run_id: []const u8) ![]const []const u8 {
        const history = try c.fetchHistory(a, id, run_id);
        var names: std.ArrayList([]const u8) = .empty;
        for (history.events.items) |event| switch (event.attributes orelse continue) {
            .workflow_execution_signaled_event_attributes => |attrs| {
                if (!std.mem.eql(u8, attrs.signal_name, "approve")) return error.TestUnexpectedResult;
                const input = attrs.input orelse return error.TestUnexpectedResult;
                if (input.payloads.items.len != 1) return error.TestUnexpectedResult;
                try names.append(a, try converter.fromPayload([]const u8, a, input.payloads.items[0]));
            },
            else => {},
        };
        return names.items;
    }

    /// The reset's failed workflow task, which the new run's history records at the reset
    /// point.
    fn resetEvent(c: *client_mod.Client, a: Allocator, id: []const u8, new_run_id: []const u8, point: i64) !proto.temporal.api.history.v1.WorkflowTaskFailedEventAttributes {
        const history = try c.fetchHistory(a, id, new_run_id);
        for (history.events.items) |event| if (event.event_id == point) switch (event.attributes orelse return error.TestUnexpectedResult) {
            .workflow_task_failed_event_attributes => |attrs| return attrs,
            else => return error.TestUnexpectedResult,
        };
        return error.TestUnexpectedResult;
    }

    fn lastEvent(c: *client_mod.Client, a: Allocator, id: []const u8, run_id: []const u8) !proto.temporal.api.history.v1.HistoryEvent {
        const history = try c.fetchHistory(a, id, run_id);
        if (history.events.items.len == 0) return error.TestUnexpectedResult;
        return history.events.items[history.events.items.len - 1];
    }

    /// The terminated event that closed the run.
    fn terminated(c: *client_mod.Client, a: Allocator, id: []const u8, run_id: []const u8) !proto.temporal.api.history.v1.WorkflowExecutionTerminatedEventAttributes {
        return switch ((try lastEvent(c, a, id, run_id)).attributes orelse return error.TestUnexpectedResult) {
            .workflow_execution_terminated_event_attributes => |attrs| attrs,
            else => error.TestUnexpectedResult,
        };
    }

    /// The single payload of a terminated event's details.
    fn detail(event: proto.temporal.api.history.v1.WorkflowExecutionTerminatedEventAttributes) !converter.Payload {
        const details = event.details orelse return error.TestUnexpectedResult;
        if (details.payloads.items.len != 1) return error.TestUnexpectedResult;
        return details.payloads.items[0];
    }
};

test "client: terminate, reset and execution-options API shapes" {
    // terminateWorkflow keeps its signature and error set.
    try testing.expect(@TypeOf(client_mod.Client.terminateWorkflow) == fn (*client_mod.Client, []const u8, []const u8, []const u8) error{ OutOfMemory, Canceled, RpcFailed, NotFound, InvalidResponse, CodecFailed }!void);
    const terminate_options: client_mod.TerminateOptions = .{};
    try testing.expectEqualStrings("", terminate_options.first_execution_run_id);

    // The reset point is an event ID or a target: the event ID defaults to 0 (no event ID)
    // and the target to null, and resetWorkflow refuses options that name neither or both.
    try testing.expect(@FieldType(client_mod.ResetOptions, "workflow_task_finish_event_id") == i64);
    try testing.expect(@FieldType(client_mod.ResetOptions, "target") == ?client_mod.ResetTarget);
    const default_reset: client_mod.ResetOptions = .{};
    try testing.expectEqual(0, default_reset.workflow_task_finish_event_id);
    try testing.expect(default_reset.target == null);
    try testing.expectEqual(0, default_reset.post_reset_operations.len);
    const reset_options: client_mod.ResetOptions = .{ .workflow_task_finish_event_id = 4 };
    try testing.expectEqualStrings("", reset_options.reason);
    try testing.expectEqualStrings("", reset_options.request_id);

    // Every exclusion is off by default.
    const exclude: client_mod.ResetReapplyExclude = .{};
    try testing.expect(!exclude.signals and !exclude.updates and !exclude.nexus);
    try testing.expect(@TypeOf(reset_options.reapply_exclude) == client_mod.ResetReapplyExclude);

    // Changes default to changing nothing, so fields can be added without breaking callers.
    const changes: client_mod.WorkflowExecutionOptionsChanges = .{};
    try testing.expect(changes.versioning_override == null);
    try testing.expect(changes.priority == null);
    try testing.expect(@FieldType(client_mod.VersioningOverrideChange, "set") == deployment_mod.VersioningOverride);
    try testing.expect(@FieldType(client_mod.WorkflowExecutionOptions, "versioning_override") == ?deployment_mod.VersioningOverride);
    try testing.expect(@FieldType(client_mod.WorkflowExecutionOptions, "raw") == proto.temporal.api.workflowservice.v1.UpdateWorkflowExecutionOptionsResponse);
}

test "client: terminate with details and a first-execution run ID; unknown, closed and other-chain runs are NotFound" {
    var capture: RequestCapture = .{ .io = testing.io, .arena = .init(testing.allocator) };
    defer capture.arena.deinit();
    const interceptors = capture.interceptors();
    const env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{ .interceptors = &interceptors });
    defer env.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const c = &env.client;
    const Detail = struct { ticket: []const u8, attempts: u32 };

    // Nobody polls the task queue: the runs wait until they are terminated.
    var guarded = try c.executeWorkflow(Fixtures.Hello, .{ .id = "terminate-guarded", .task_queue = "nobody-polls-this" }, .{"guarded"});
    defer guarded.deinit();
    defer c.terminateWorkflow(guarded.id, "", "test cleanup") catch {};

    // Another chain's first run ID: refused, and the run keeps running.
    try testing.expectError(error.NotFound, c.terminateWorkflowWithOptions(guarded.id, "", "wrong chain", Detail{ .ticket = "OPS-1", .attempts = 1 }, .{
        .first_execution_run_id = try client_mod.uuid(a, testing.io),
    }));
    try testing.expectEqual(client_mod.WorkflowExecutionStatus.running, (try c.describeWorkflow(a, guarded.id, guarded.run_id)).status);
    try testing.expectEqual(0, try ResetFixtures.countEvents(c, a, guarded.id, guarded.run_id, .workflow_execution_terminated_event_attributes));

    // Its own first run ID terminates it, with the details in the terminated event.
    try c.terminateWorkflowWithOptions(guarded.id, guarded.run_id, "operator request", Detail{ .ticket = "OPS-42", .attempts = 3 }, .{
        .first_execution_run_id = guarded.run_id,
    });
    try testing.expectError(error.WorkflowTerminated, guarded.get(a));
    const terminated = try ResetFixtures.terminated(c, a, guarded.id, guarded.run_id);
    try testing.expectEqualStrings("operator request", terminated.reason);
    try testing.expectEqualStrings(c.identity, terminated.identity);
    const detail = try converter.fromPayload(Detail, a, try ResetFixtures.detail(terminated));
    try testing.expectEqualStrings("OPS-42", detail.ticket);
    try testing.expectEqual(3, detail.attempts);
    const guarded_request = capture.terminates.items[capture.terminates.items.len - 1];
    try testing.expectEqualStrings(guarded.run_id, guarded_request.workflow_execution.?.run_id);
    try testing.expectEqualStrings(guarded.run_id, guarded_request.first_execution_run_id);
    try testing.expect(guarded_request.details != null);

    // No details sends none: a plain terminate and a `{}` one send the same request.
    var plain = try c.executeWorkflow(Fixtures.Hello, .{ .id = "terminate-plain", .task_queue = "nobody-polls-this" }, .{"plain"});
    defer plain.deinit();
    defer c.terminateWorkflow(plain.id, "", "test cleanup") catch {};
    var empty = try c.executeWorkflow(Fixtures.Hello, .{ .id = "terminate-empty", .task_queue = "nobody-polls-this" }, .{"empty"});
    defer empty.deinit();
    defer c.terminateWorkflow(empty.id, "", "test cleanup") catch {};
    try c.terminateWorkflow(plain.id, "", "plain");
    try c.terminateWorkflowWithOptions(empty.id, "", "empty", {}, .{});
    for ([_][]const u8{ plain.id, empty.id }, [_][]const u8{ plain.run_id, empty.run_id }) |id, run_id| {
        try testing.expect((try ResetFixtures.terminated(c, a, id, run_id)).details == null);
    }
    try testing.expectEqual(4, capture.terminates.items.len);
    for (capture.terminates.items[2..], [_][]const u8{ "terminate-plain", "terminate-empty" }, [_][]const u8{ "plain", "empty" }) |request, id, reason| {
        try testing.expect(request.details == null);
        try testing.expectEqual(0, request.first_execution_run_id.len);
        try testing.expectEqual(0, request.links.items.len);
        try testing.expectEqualStrings(c.namespace, request.namespace);
        try testing.expectEqualStrings(c.identity, request.identity);
        try testing.expectEqualStrings(id, request.workflow_execution.?.workflow_id);
        try testing.expectEqual(0, request.workflow_execution.?.run_id.len);
        try testing.expectEqualStrings(reason, request.reason);
    }

    // A closed chain and an unknown workflow.
    try testing.expectError(error.NotFound, c.terminateWorkflowWithOptions(guarded.id, "", "again", {}, .{}));
    try testing.expectError(error.NotFound, c.terminateWorkflowWithOptions("no-such-workflow", "", "x", {}, .{}));
}

test "client: cancel with a first-execution run ID after continue-as-new, and links on cancel and terminate requests" {
    var capture: RequestCapture = .{ .io = testing.io, .arena = .init(testing.allocator) };
    defer capture.arena.deinit();
    const interceptors = capture.interceptors();
    const env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{ .interceptors = &interceptors });
    defer env.deinit();
    try env.registerWorkflow(SignalFixtures.AwaitApprovals);
    try env.registerWorkflow(ResetFixtures.Hop);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const c = &env.client;
    const common = proto.temporal.api.common.v1;

    // cancelWorkflow keeps its signature and error set.
    try testing.expect(@TypeOf(client_mod.Client.cancelWorkflow) == fn (*client_mod.Client, []const u8, []const u8, []const u8) error{ OutOfMemory, Canceled, RpcFailed, NotFound, InvalidResponse, CodecFailed }!void);
    const cancel_defaults: client_mod.CancelOptions = .{};
    try testing.expectEqualStrings("", cancel_defaults.first_execution_run_id);
    try testing.expectEqual(0, cancel_defaults.links.len);
    try testing.expectEqual(0, (client_mod.TerminateOptions{}).links.len);

    // A chain that continued as new once: its second run waits for an approval.
    var chain = try env.startWorkflow(ResetFixtures.Hop, .{ .id = "cancel-chain", .task_queue = "" }, .{1});
    defer chain.deinit();
    defer c.terminateWorkflow(chain.id, "", "test cleanup") catch {};
    var close = try c.historyIterator(chain.id, chain.run_id, .{ .wait_new_event = true, .filter = .close_event });
    defer close.deinit();
    const hopped = switch (((try close.next(a)) orelse return error.TestUnexpectedResult).attributes orelse return error.TestUnexpectedResult) {
        .workflow_execution_continued_as_new_event_attributes => |attrs| attrs.new_execution_run_id,
        else => return error.TestUnexpectedResult,
    };
    _ = try ResetFixtures.firstTaskCompleted(c, chain.id, hopped);

    // Another run's ID, or an unrelated one, isn't the chain's first: nothing is canceled.
    try testing.expectError(error.NotFound, c.cancelWorkflowWithOptions(chain.id, "", "later run", .{ .first_execution_run_id = hopped }));
    try testing.expectError(error.NotFound, c.cancelWorkflowWithOptions(chain.id, "", "other chain", .{ .first_execution_run_id = try client_mod.uuid(a, testing.io) }));
    const still = try c.describeWorkflow(a, chain.id, "");
    try testing.expectEqualStrings(hopped, still.run_id);
    try testing.expectEqual(client_mod.WorkflowExecutionStatus.running, still.status);
    try testing.expectEqual(0, try ResetFixtures.countEvents(c, a, chain.id, hopped, .workflow_execution_cancel_requested_event_attributes));

    // The chain's first run ID cancels the current run, with the links in the request.
    const cancel_links = [_]common.Link{.{ .variant = .{ .workflow_event = .{ .namespace = c.namespace, .workflow_id = "cancel-cause", .run_id = "cause-run" } } }};
    try c.cancelWorkflowWithOptions(chain.id, "", "operator request", .{ .first_execution_run_id = chain.run_id, .links = &cancel_links });
    _ = try ResetFixtures.awaitEvent(c, chain.id, hopped, .workflow_execution_canceled_event_attributes, 1);
    try testing.expectEqual(client_mod.WorkflowExecutionStatus.canceled, (try c.describeWorkflow(a, chain.id, hopped)).status);
    for ((try c.fetchHistory(a, chain.id, hopped)).events.items) |event| switch (event.attributes orelse continue) {
        .workflow_execution_cancel_requested_event_attributes => |attrs| {
            try testing.expectEqualStrings("operator request", attrs.cause);
            // The server records the links on the event.
            try testing.expectEqual(1, event.links.items.len);
            try testing.expectEqualStrings("cancel-cause", event.links.items[0].variant.?.workflow_event.workflow_id);
        },
        else => {},
    };

    try testing.expectEqual(3, capture.cancels.items.len);
    for (capture.cancels.items, [_][]const u8{ hopped, "", chain.run_id }) |request, first| {
        try testing.expectEqualStrings(chain.id, request.workflow_execution.?.workflow_id);
        if (first.len > 0) try testing.expectEqualStrings(first, request.first_execution_run_id);
    }
    try testing.expectEqual(0, capture.cancels.items[0].links.items.len);
    const sent_cancel = capture.cancels.items[2];
    try testing.expectEqualStrings("operator request", sent_cancel.reason);
    try testing.expectEqual(1, sent_cancel.links.items.len);
    try testing.expectEqualStrings("cancel-cause", sent_cancel.links.items[0].variant.?.workflow_event.workflow_id);
    try testing.expectEqualStrings("cause-run", sent_cancel.links.items[0].variant.?.workflow_event.run_id);

    // Links on a terminate reach its request.
    var run = try c.executeWorkflow(Fixtures.Hello, .{ .id = "terminate-links", .task_queue = "nobody-polls-this" }, .{"linked"});
    defer run.deinit();
    defer c.terminateWorkflow(run.id, "", "test cleanup") catch {};
    const terminate_links = [_]common.Link{
        .{ .variant = .{ .workflow_event = .{ .namespace = c.namespace, .workflow_id = "terminate-cause", .run_id = "cause-run" } } },
        .{ .variant = .{ .batch_job = .{ .job_id = "batch-7" } } },
    };
    try c.terminateWorkflowWithOptions(run.id, "", "linked", {}, .{ .links = &terminate_links });
    try testing.expectError(error.WorkflowTerminated, run.get(a));
    const terminated_event = try ResetFixtures.lastEvent(c, a, run.id, run.run_id);
    try testing.expectEqual(2, terminated_event.links.items.len);
    try testing.expectEqualStrings("batch-7", terminated_event.links.items[1].variant.?.batch_job.job_id);
    const sent_terminate = capture.terminates.items[capture.terminates.items.len - 1];
    try testing.expectEqualStrings("terminate-links", sent_terminate.workflow_execution.?.workflow_id);
    try testing.expectEqual(2, sent_terminate.links.items.len);
    try testing.expectEqualStrings("terminate-cause", sent_terminate.links.items[0].variant.?.workflow_event.workflow_id);
    try testing.expectEqualStrings("batch-7", sent_terminate.links.items[1].variant.?.batch_job.job_id);
}

test "client: terminate details pass through the client's codecs" {
    var reversing: ReversingCodec = .{};
    const codecs = [_]converter.PayloadCodec{reversing.codec()};
    const env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{ .codecs = &codecs });
    defer env.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const c = &env.client;

    var run = try c.executeWorkflow(Fixtures.Hello, .{ .id = "terminate-coded", .task_queue = "nobody-polls-this" }, .{"coded"});
    defer run.deinit();
    defer c.terminateWorkflow(run.id, "", "test cleanup") catch {};
    const before = reversing.encoded.load(.monotonic);
    try c.terminateWorkflowWithOptions(run.id, "", "coded", "secret-detail", .{});
    try testing.expectEqual(before + 1, reversing.encoded.load(.monotonic));

    const decoded = try ResetFixtures.terminated(c, a, run.id, run.run_id);
    try testing.expectEqualStrings("secret-detail", try converter.fromPayload([]const u8, a, try ResetFixtures.detail(decoded)));

    // A client without codecs sees what the server stored.
    var plain = try client_mod.Client.connect(testing.allocator, testing.io, env.server.clientOptions());
    defer plain.close();
    const stored = try ResetFixtures.detail(try ResetFixtures.terminated(&plain, a, run.id, run.run_id));
    try testing.expectEqualStrings(ReversingCodec.encoding, converter.encodingOf(stored) orelse return error.TestUnexpectedResult);
    const event = try ResetFixtures.lastEvent(&plain, a, run.id, run.run_id);
    var bytes: Io.Writer.Allocating = .init(a);
    try event.encode(&bytes.writer, a);
    try testing.expect(std.mem.indexOf(u8, bytes.written(), "secret-detail") == null);
}

/// Fails every payload it is asked to encode, and decodes nothing.
const FailingCodec = struct {
    fn codec(f: *FailingCodec) converter.PayloadCodec {
        return .{ .ptr = f, .vtable = &.{ .encode = encode, .decode = decode } };
    }

    fn encode(_: *anyopaque, _: Allocator, _: Io, _: []const converter.Payload) converter.CodecError![]converter.Payload {
        return error.CodecFailed;
    }

    fn decode(_: *anyopaque, arena: Allocator, _: Io, payloads: []const converter.Payload) converter.CodecError![]converter.Payload {
        return arena.dupe(converter.Payload, payloads);
    }
};

test "client: terminate details a codec fails to encode are CodecFailed, and the workflow keeps running" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var run = try env.client.executeWorkflow(Fixtures.Hello, .{ .id = "terminate-codec-fails", .task_queue = "nobody-polls-this" }, .{"coded"});
    defer run.deinit();
    defer env.client.terminateWorkflow(run.id, "", "test cleanup") catch {};

    var failing_codec: FailingCodec = .{};
    const codecs = [_]converter.PayloadCodec{failing_codec.codec()};
    var options = env.server.clientOptions();
    options.codecs = &codecs;
    var failing = try client_mod.Client.connect(testing.allocator, testing.io, options);
    defer failing.close();
    try testing.expectError(error.CodecFailed, failing.terminateWorkflowWithOptions(run.id, "", "coded", "secret-detail", .{}));
    try testing.expectEqual(client_mod.WorkflowExecutionStatus.running, (try env.client.describeWorkflow(a, run.id, run.run_id)).status);
    // No details, nothing to encode: the same client terminates.
    try failing.terminateWorkflowWithOptions(run.id, "", "plain", {}, .{});
    try testing.expectError(error.WorkflowTerminated, run.get(a));
}

test "client: reset, terminate and execution options take the interceptor chain's answers, errors and cancellation as the server's" {
    var capture: RequestCapture = .{ .io = testing.io, .arena = .init(testing.allocator) };
    defer capture.arena.deinit();
    const interceptors = capture.interceptors();
    const env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{ .interceptors = &interceptors });
    defer env.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const c = &env.client;
    const wf = proto.temporal.api.workflow.v1;

    // A reset response that names no new run.
    capture.reset_answer = .{};
    try testing.expectError(error.InvalidResponse, c.resetWorkflow(a, "chain-reset", "", .{ .workflow_task_finish_event_id = 4 }));
    try testing.expectEqual(1, capture.resets.items.len);

    // A huge run ID and a non-UTF-8 one come back as given, copied into the caller's arena.
    const huge = try testing.allocator.alloc(u8, 1024 * 1024);
    defer testing.allocator.free(huge);
    for (huge, 0..) |*b, i| b.* = @truncate(i *% 31 +% 7);
    for ([_][]const u8{ huge, "\xff\x00\xfe" }) |run_id| {
        capture.reset_answer = .{ .run_id = run_id };
        const got = try c.resetWorkflow(a, "chain-reset", "", .{ .workflow_task_finish_event_id = 4 });
        try testing.expect(got.ptr != run_id.ptr);
        for (0..64) |_| @memset(try a.alloc(u8, 64 * 1024), 0xaa);
        try testing.expectEqualSlices(u8, run_id, got);
    }

    // Each exclusion maps to its own exclude type.
    capture.reset_answer = .{ .run_id = "r" };
    const ExcludeType = proto.temporal.api.enums.v1.ResetReapplyExcludeType;
    for ([_]client_mod.ResetReapplyExclude{ .{ .signals = true }, .{ .updates = true }, .{ .nexus = true } }, [_]ExcludeType{
        .RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL,
        .RESET_REAPPLY_EXCLUDE_TYPE_UPDATE,
        .RESET_REAPPLY_EXCLUDE_TYPE_NEXUS,
    }) |exclude, want| {
        _ = try c.resetWorkflow(a, "chain-reset", "", .{ .workflow_task_finish_event_id = 4, .reapply_exclude = exclude });
        try testing.expectEqualSlices(ExcludeType, &.{want}, capture.resets.items[capture.resets.items.len - 1].reset_reapply_exclude_types.items);
    }
    capture.reset_answer = null;

    // The options the response reports are lifted into the result, and `raw` keeps the rest.
    capture.options_answer = .{ .workflow_execution_options = .{ .versioning_override = .{ .override = .{ .pinned = .{
        .behavior = .PINNED_OVERRIDE_BEHAVIOR_PINNED,
        .version = .{ .deployment_name = "rollout", .build_id = "v1" },
    } } } } };
    const pinned = try c.updateWorkflowExecutionOptions(a, "chain-options", "", .{ .versioning_override = .remove });
    try testing.expectEqualDeep(@as(?deployment_mod.VersioningOverride, .{ .pinned = .{ .deployment_name = "rollout", .build_id = "v1" } }), pinned.versioning_override);
    capture.options_answer = .{ .workflow_execution_options = .{ .versioning_override = wf.VersioningOverride{
        .override = .{ .one_time = .{ .target_deployment_version = .{ .deployment_name = "rollout", .build_id = "v1" } } },
    } } };
    const one_time = try c.updateWorkflowExecutionOptions(a, "chain-options", "", .{ .versioning_override = .remove });
    try testing.expect(one_time.versioning_override == null);
    const raw_override = (one_time.raw.workflow_execution_options orelse return error.TestUnexpectedResult).versioning_override orelse return error.TestUnexpectedResult;
    try testing.expect((raw_override.override orelse return error.TestUnexpectedResult) == .one_time);
    capture.options_answer = .{};
    const empty = try c.updateWorkflowExecutionOptions(a, "chain-options", "", .{ .versioning_override = .remove });
    try testing.expect(empty.versioning_override == null);
    try testing.expect(empty.raw.workflow_execution_options == null);
    capture.options_answer = null;

    // Each error the chain can return comes back as returned, except AlreadyExists, which
    // none of these calls can return.
    inline for (@typeInfo(interceptor.ClientError).error_set.error_names.?) |name| {
        const err = @field(anyerror, name);
        capture.fail_with = @errorCast(err);
        const want: anyerror = if (err == error.AlreadyExists) error.InvalidResponse else err;
        try testing.expectError(want, c.resetWorkflow(a, "chain-errors", "", .{ .workflow_task_finish_event_id = 4 }));
        try testing.expectError(want, c.updateWorkflowExecutionOptions(a, "chain-errors", "", .{ .versioning_override = .remove }));
        try testing.expectError(want, c.terminateWorkflowWithOptions("chain-errors", "", "x", "detail", .{}));
    }
    capture.fail_with = null;

    // Canceling the calling task mid-call returns Canceled, and leaks nothing.
    const Calls = struct {
        fn reset(cl: *client_mod.Client) client_mod.ResetError!void {
            var call_arena: std.heap.ArenaAllocator = .init(testing.allocator);
            defer call_arena.deinit();
            _ = try cl.resetWorkflow(call_arena.allocator(), "chain-cancel", "", .{ .workflow_task_finish_event_id = 4 });
        }
        fn options(cl: *client_mod.Client) client_mod.UpdateExecutionOptionsError!void {
            var call_arena: std.heap.ArenaAllocator = .init(testing.allocator);
            defer call_arena.deinit();
            _ = try cl.updateWorkflowExecutionOptions(call_arena.allocator(), "chain-cancel", "", .{ .versioning_override = .{ .set = .auto_upgrade } });
        }
        fn terminate(cl: *client_mod.Client) client_mod.TerminateError!void {
            return cl.terminateWorkflowWithOptions("chain-cancel", "", "x", .{ .big = "detail" }, .{ .first_execution_run_id = "abc" });
        }
    };
    capture.block = true;
    var reset = try testing.io.concurrent(Calls.reset, .{c});
    try capture.blocked.wait(testing.io);
    capture.blocked.reset();
    try testing.expectError(error.Canceled, reset.cancel(testing.io));
    var options = try testing.io.concurrent(Calls.options, .{c});
    try capture.blocked.wait(testing.io);
    capture.blocked.reset();
    try testing.expectError(error.Canceled, options.cancel(testing.io));
    var terminate = try testing.io.concurrent(Calls.terminate, .{c});
    try capture.blocked.wait(testing.io);
    capture.blocked.reset();
    try testing.expectError(error.Canceled, terminate.cancel(testing.io));
}

test "client: reset a running workflow: later signals are reapplied, the old run ends terminated, and the new run replays and completes" {
    var capture: RequestCapture = .{ .io = testing.io, .arena = .init(testing.allocator) };
    defer capture.arena.deinit();
    const interceptors = capture.interceptors();
    const env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{ .interceptors = &interceptors });
    defer env.deinit();
    try env.registerWorkflow(SignalFixtures.AwaitApprovals);
    try env.registerWorkflow(ResetFixtures.PendingActivity);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const c = &env.client;

    var run = try env.startWorkflow(SignalFixtures.AwaitApprovals, .{ .id = "reset-running", .task_queue = "" }, .{3});
    defer run.deinit();
    defer c.terminateWorkflow(run.id, "", "test cleanup") catch {};
    // A handle with an empty run ID, made before the reset.
    var latest_handle = try c.getWorkflow([]const u8, run.id, "");
    defer latest_handle.deinit();
    const point = try ResetFixtures.firstTaskCompleted(c, run.id, run.run_id);
    try c.signalWorkflow(SignalFixtures.Approve, run.id, run.run_id, .{"ann"});
    try c.signalWorkflow(SignalFixtures.Approve, run.id, run.run_id, .{"bob"});

    // An empty run ID resets the latest run.
    const new_run_id = try c.resetWorkflow(a, run.id, "", .{ .workflow_task_finish_event_id = point, .reason = "replay the approvals" });
    try testing.expect(new_run_id.len > 0);
    try testing.expect(!std.mem.eql(u8, new_run_id, run.run_id));

    try testing.expectEqual(client_mod.WorkflowExecutionStatus.terminated, (try c.describeWorkflow(a, run.id, run.run_id)).status);
    try testing.expectEqualStrings("replay the approvals", (try ResetFixtures.terminated(c, a, run.id, run.run_id)).reason);
    // The base run names the run that replaced it: how a caller tells whether a reset happened.
    const base_info = (try c.describeWorkflow(a, run.id, run.run_id)).raw.workflow_extended_info orelse return error.TestUnexpectedResult;
    try testing.expectEqualStrings(new_run_id, base_info.reset_run_id);
    const latest = try c.describeWorkflow(a, run.id, "");
    try testing.expectEqualStrings(new_run_id, latest.run_id);
    try testing.expectEqual(client_mod.WorkflowExecutionStatus.running, latest.status);

    const reset = try ResetFixtures.resetEvent(c, a, run.id, new_run_id, point);
    try testing.expectEqual(.WORKFLOW_TASK_FAILED_CAUSE_RESET_WORKFLOW, reset.cause);
    try testing.expectEqualStrings("replay the approvals", (reset.failure orelse return error.TestUnexpectedResult).message);
    try testing.expectEqualStrings(run.run_id, reset.base_run_id);
    try testing.expectEqualStrings(new_run_id, reset.new_run_id);
    // The signals after the reset point are reapplied by default.
    const signalers = try ResetFixtures.signalers(c, a, run.id, new_run_id);
    try testing.expectEqual(2, signalers.len);
    try testing.expectEqualStrings("ann", signalers[0]);
    try testing.expectEqualStrings("bob", signalers[1]);

    // The handle that names the old run doesn't follow the reset.
    try testing.expectError(error.WorkflowTerminated, run.get(a));

    // Waiting for the result replays the new run's history, reset event included.
    try c.signalWorkflow(SignalFixtures.Approve, run.id, new_run_id, .{"cy"});
    var next = try c.getWorkflow([]const u8, run.id, new_run_id);
    defer next.deinit();
    try testing.expectEqualStrings("ann bob cy", try env.getResult(SignalFixtures.AwaitApprovals, &next, a, null));
    // The handle with an empty run ID reads the latest run, so it follows.
    try testing.expectEqualStrings("ann bob cy", try latest_handle.get(a));

    try testing.expectEqual(1, capture.resets.items.len);
    const request = capture.resets.items[0];
    try testing.expectEqualStrings(c.namespace, request.namespace);
    try testing.expectEqualStrings(c.identity, request.identity);
    try testing.expectEqualStrings("reset-running", request.workflow_execution.?.workflow_id);
    try testing.expectEqual(0, request.workflow_execution.?.run_id.len);
    try testing.expectEqual(point, request.workflow_task_finish_event_id);
    try testing.expectEqualStrings("replay the approvals", request.reason);
    try testing.expectEqual(36, request.request_id.len);
    try testing.expectEqual(0, @backingInt(request.reset_reapply_type));
    try testing.expectEqual(0, request.reset_reapply_exclude_types.items.len);
    try testing.expectEqual(0, request.post_reset_operations.items.len);

    // A run with a pending activity: the new run replays up to the reset point on the
    // worker and schedules the activity again. A nondeterminism failure would fail that
    // workflow task instead.
    var pending = try env.startWorkflow(ResetFixtures.PendingActivity, .{ .id = "reset-pending-activity", .task_queue = "" }, .{});
    defer pending.deinit();
    defer c.terminateWorkflow(pending.id, "", "test cleanup") catch {};
    const pending_point = try ResetFixtures.firstTaskCompleted(c, pending.id, pending.run_id);
    try testing.expectEqual(1, try ResetFixtures.countEvents(c, a, pending.id, pending.run_id, .activity_task_scheduled_event_attributes));
    const pending_new = try c.resetWorkflow(a, pending.id, "", .{ .workflow_task_finish_event_id = pending_point });
    _ = try ResetFixtures.awaitEvent(c, pending.id, pending_new, .activity_task_scheduled_event_attributes, 1);
    try testing.expectEqual(1, try ResetFixtures.countEvents(c, a, pending.id, pending_new, .workflow_task_failed_event_attributes));
    try testing.expectEqual(client_mod.WorkflowExecutionStatus.running, (try c.describeWorkflow(a, pending.id, pending_new)).status);
}

test "client: reset with reapply exclusions: a reset completed workflow gets no signals and runs again, and an update after the reset point is reapplied unless excluded" {
    var capture: RequestCapture = .{ .io = testing.io, .arena = .init(testing.allocator) };
    defer capture.arena.deinit();
    const interceptors = capture.interceptors();
    const env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{ .interceptors = &interceptors });
    defer env.deinit();
    try env.registerWorkflow(SignalFixtures.AwaitApprovals);
    try env.registerWorkflow(UpdateFixtures.HandleUpdatesChanged);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const c = &env.client;

    var run = try env.startWorkflow(SignalFixtures.AwaitApprovals, .{ .id = "reset-closed", .task_queue = "" }, .{1});
    defer run.deinit();
    defer c.terminateWorkflow(run.id, "", "test cleanup") catch {};
    const point = try ResetFixtures.firstTaskCompleted(c, run.id, run.run_id);
    try c.signalWorkflow(SignalFixtures.Approve, run.id, run.run_id, .{"ann"});
    try testing.expectEqualStrings("ann", try env.getResult(SignalFixtures.AwaitApprovals, &run, a, null));

    const new_run_id = try c.resetWorkflow(a, run.id, run.run_id, .{
        .workflow_task_finish_event_id = point,
        .reason = "rerun",
        .reapply_exclude = .{ .signals = true, .updates = true, .nexus = true },
    });
    // A closed base run stays as it was.
    try testing.expectEqual(client_mod.WorkflowExecutionStatus.completed, (try c.describeWorkflow(a, run.id, run.run_id)).status);
    // Without the signal, the new run waits for one.
    try testing.expectEqual(client_mod.WorkflowExecutionStatus.running, (try c.describeWorkflow(a, run.id, new_run_id)).status);
    try testing.expectEqual(0, (try ResetFixtures.signalers(c, a, run.id, new_run_id)).len);
    _ = try ResetFixtures.resetEvent(c, a, run.id, new_run_id, point);

    try c.signalWorkflow(SignalFixtures.Approve, run.id, new_run_id, .{"bob"});
    var next = try c.getWorkflow([]const u8, run.id, new_run_id);
    defer next.deinit();
    try testing.expectEqualStrings("bob", try env.getResult(SignalFixtures.AwaitApprovals, &next, a, null));

    try testing.expectEqual(1, capture.resets.items.len);
    const request = capture.resets.items[0];
    try testing.expectEqualStrings(run.run_id, request.workflow_execution.?.run_id);
    try testing.expectEqual(36, request.request_id.len);
    try testing.expectEqual(0, @backingInt(request.reset_reapply_type));
    const excluded = request.reset_reapply_exclude_types.items;
    try testing.expectEqual(3, excluded.len);
    for ([_]proto.temporal.api.enums.v1.ResetReapplyExcludeType{
        .RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL,
        .RESET_REAPPLY_EXCLUDE_TYPE_UPDATE,
        .RESET_REAPPLY_EXCLUDE_TYPE_NEXUS,
    }) |want| {
        try testing.expect(std.mem.indexOfScalar(proto.temporal.api.enums.v1.ResetReapplyExcludeType, excluded, want) != null);
    }

    // Updates accepted after the reset point are reapplied to the new run and run again
    // there, unless excluded.
    for ([_]bool{ false, true }) |exclude| {
        const id = if (exclude) "reset-updates-excluded" else "reset-updates-reapplied";
        var updated = try env.startWorkflow(UpdateFixtures.HandleUpdatesChanged, .{ .id = id, .task_queue = "" }, .{});
        defer updated.deinit();
        defer c.terminateWorkflow(updated.id, "", "test cleanup") catch {};
        const update_point = try ResetFixtures.firstTaskCompleted(c, updated.id, updated.run_id);
        var set = try c.updateWorkflow(UpdateFixtures.Set, updated.id, updated.run_id, .{5}, .{});
        defer set.deinit();
        try testing.expectEqual(0, try set.get(a));
        var greet = try c.updateWorkflow(UpdateFixtures.Greet, updated.id, updated.run_id, .{"ann"}, .{});
        defer greet.deinit();
        try testing.expectEqualStrings("ann", try greet.get(a));

        const reset_id = try c.resetWorkflow(a, updated.id, "", .{ .workflow_task_finish_event_id = update_point, .reapply_exclude = .{ .updates = exclude } });
        try c.signalWorkflow(UpdateFixtures.Done, updated.id, reset_id, .{});
        var reset_run = try c.getWorkflow([]const u8, updated.id, reset_id);
        defer reset_run.deinit();
        try testing.expectEqualStrings(if (exclude) "0 " else "5 ann", try env.getResult(UpdateFixtures.HandleUpdatesChanged, &reset_run, a, null));
        const reapplied: usize = if (exclude) 0 else 2;
        try testing.expectEqual(reapplied, try ResetFixtures.countEvents(c, a, updated.id, reset_id, .workflow_execution_update_admitted_event_attributes));
        try testing.expectEqual(reapplied, try ResetFixtures.countEvents(c, a, updated.id, reset_id, .workflow_execution_update_accepted_event_attributes));
    }
}

test "client: a reset's request ID is sent as given, and repeating it resets again; first-execution and closed run IDs across continue-as-new and a reset" {
    var capture: RequestCapture = .{ .io = testing.io, .arena = .init(testing.allocator) };
    defer capture.arena.deinit();
    const interceptors = capture.interceptors();
    const env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{ .interceptors = &interceptors });
    defer env.deinit();
    try env.registerWorkflow(SignalFixtures.AwaitApprovals);
    try env.registerWorkflow(ResetFixtures.Hop);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const c = &env.client;

    var run = try env.startWorkflow(SignalFixtures.AwaitApprovals, .{ .id = "reset-request-id", .task_queue = "" }, .{1});
    defer run.deinit();
    defer c.terminateWorkflow(run.id, "", "test cleanup") catch {};
    const point = try ResetFixtures.firstTaskCompleted(c, run.id, run.run_id);

    const options: client_mod.ResetOptions = .{ .workflow_task_finish_event_id = point, .request_id = "reset-once" };
    const first = try c.resetWorkflow(a, run.id, run.run_id, options);
    const second = try c.resetWorkflow(a, run.id, run.run_id, options);
    // Observed on dev server 1.32.2: the server doesn't deduplicate resets by request ID. If
    // this fails, the server started to: update the README's note on request_id.
    try testing.expect(!std.mem.eql(u8, first, second));
    try testing.expectEqual(client_mod.WorkflowExecutionStatus.terminated, (try c.describeWorkflow(a, run.id, first)).status);
    const latest = try c.describeWorkflow(a, run.id, "");
    try testing.expectEqualStrings(second, latest.run_id);
    try testing.expectEqual(client_mod.WorkflowExecutionStatus.running, latest.status);

    try testing.expectEqual(2, capture.resets.items.len);
    for (capture.resets.items) |request| try testing.expectEqualStrings("reset-once", request.request_id);

    // The chain's first run is still the original one: a reset run's ID doesn't match it.
    try testing.expectError(error.NotFound, c.terminateWorkflowWithOptions(run.id, "", "x", {}, .{ .first_execution_run_id = first }));
    try c.terminateWorkflowWithOptions(run.id, "", "done", {}, .{ .first_execution_run_id = run.run_id });
    try testing.expectEqual(client_mod.WorkflowExecutionStatus.terminated, (try c.describeWorkflow(a, run.id, second)).status);

    // A chain that continued as new once: its second run waits for an approval.
    var chain = try env.startWorkflow(ResetFixtures.Hop, .{ .id = "reset-chain", .task_queue = "" }, .{1});
    defer chain.deinit();
    defer c.terminateWorkflow(chain.id, "", "test cleanup") catch {};
    var close = try c.historyIterator(chain.id, chain.run_id, .{ .wait_new_event = true, .filter = .close_event });
    defer close.deinit();
    const hopped = switch (((try close.next(a)) orelse return error.TestUnexpectedResult).attributes orelse return error.TestUnexpectedResult) {
        .workflow_execution_continued_as_new_event_attributes => |attrs| attrs.new_execution_run_id,
        else => return error.TestUnexpectedResult,
    };
    const chain_point = try ResetFixtures.firstTaskCompleted(c, chain.id, hopped);

    // A later run of the chain is not its first.
    try testing.expectError(error.NotFound, c.terminateWorkflowWithOptions(chain.id, "", "later run", {}, .{ .first_execution_run_id = hopped }));
    // The first run closed by continuing as new: naming it is NotFound.
    try testing.expectError(error.NotFound, c.terminateWorkflowWithOptions(chain.id, chain.run_id, "closed run", {}, .{}));
    try testing.expectError(error.NotFound, c.updateWorkflowExecutionOptions(a, chain.id, chain.run_id, .{ .versioning_override = .{ .set = .auto_upgrade } }));

    // The reset closes the second run, and naming it is NotFound too.
    const third = try c.resetWorkflow(a, chain.id, "", .{ .workflow_task_finish_event_id = chain_point });
    try testing.expectError(error.NotFound, c.updateWorkflowExecutionOptions(a, chain.id, hopped, .{ .versioning_override = .{ .set = .auto_upgrade } }));
    try testing.expectError(error.NotFound, c.terminateWorkflowWithOptions(chain.id, hopped, "reset run", {}, .{}));
    const changed = try c.updateWorkflowExecutionOptions(a, chain.id, third, .{ .versioning_override = .{ .set = .auto_upgrade } });
    try testing.expect(changed.versioning_override.? == .auto_upgrade);
    // The server holds the event back while a workflow task is in progress, as the reset
    // run's first one may be: wait for it rather than read the history once.
    _ = try ResetFixtures.awaitEvent(c, chain.id, third, .workflow_execution_options_updated_event_attributes, 1);
    try testing.expectEqual(1, try ResetFixtures.countEvents(c, a, chain.id, third, .workflow_execution_options_updated_event_attributes));
    // The chain's first run survives continue-as-new and the reset.
    try testing.expectError(error.NotFound, c.terminateWorkflowWithOptions(chain.id, "", "reset run's id", {}, .{ .first_execution_run_id = third }));
    try c.terminateWorkflowWithOptions(chain.id, third, "original", {}, .{ .first_execution_run_id = chain.run_id });
    try testing.expectEqual(client_mod.WorkflowExecutionStatus.terminated, (try c.describeWorkflow(a, chain.id, third)).status);
}

test "client: reset event IDs: unknown workflow or run is NotFound, a non-positive one is InvalidOptions before any call, one the server refuses is RpcFailed, and a workflow task's started or failed event is accepted" {
    var capture: RequestCapture = .{ .io = testing.io, .arena = .init(testing.allocator) };
    defer capture.arena.deinit();
    const interceptors = capture.interceptors();
    const env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{ .interceptors = &interceptors });
    defer env.deinit();
    try env.registerWorkflow(SignalFixtures.AwaitApprovals);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const c = &env.client;

    var run = try env.startWorkflow(SignalFixtures.AwaitApprovals, .{ .id = "reset-errors", .task_queue = "" }, .{1});
    defer run.deinit();
    defer c.terminateWorkflow(run.id, "", "test cleanup") catch {};
    const point = try ResetFixtures.firstTaskCompleted(c, run.id, run.run_id);

    for ([_]i64{ 0, -5, std.math.minInt(i64) }) |event_id| {
        try testing.expectError(error.InvalidOptions, c.resetWorkflow(a, run.id, "", .{ .workflow_task_finish_event_id = event_id }));
    }
    try testing.expectEqual(0, capture.resets.items.len);

    // Event 1 (the workflow's start), beyond the history, and an event that isn't at a
    // workflow task's boundary.
    const last = (try ResetFixtures.lastEvent(c, a, run.id, run.run_id)).event_id;
    for ([_]i64{ 1, last + 1, 999, std.math.maxInt(i64) }) |event_id| {
        try testing.expectError(error.RpcFailed, c.resetWorkflow(a, run.id, "", .{ .workflow_task_finish_event_id = event_id }));
    }
    const history = try c.fetchHistory(a, run.id, run.run_id);
    const scheduled = for (history.events.items) |event| {
        if (event.event_id == point - 2) break event;
    } else return error.TestUnexpectedResult;
    try testing.expect((scheduled.attributes orelse return error.TestUnexpectedResult) == .workflow_task_scheduled_event_attributes);
    try testing.expectError(error.RpcFailed, c.resetWorkflow(a, run.id, "", .{ .workflow_task_finish_event_id = point - 2 }));

    try testing.expectError(error.NotFound, c.resetWorkflow(a, "no-such-workflow", "", .{ .workflow_task_finish_event_id = 4 }));
    try testing.expectError(error.NotFound, c.resetWorkflow(a, run.id, try client_mod.uuid(a, testing.io), .{ .workflow_task_finish_event_id = point }));

    // Nothing was reset.
    const latest = try c.describeWorkflow(a, run.id, "");
    try testing.expectEqualStrings(run.run_id, latest.run_id);
    try testing.expectEqual(client_mod.WorkflowExecutionStatus.running, latest.status);
    try testing.expectEqual(7, capture.resets.items.len);

    // The server also accepts a workflow task's started event.
    const started_point = for (history.events.items) |event| {
        const attrs = event.attributes orelse continue;
        if (attrs == .workflow_task_started_event_attributes and event.event_id < point) break event.event_id;
    } else return error.TestUnexpectedResult;
    const started = try c.resetWorkflow(a, run.id, "", .{ .workflow_task_finish_event_id = started_point, .reason = "started" });

    // A reset run reset to the reset's own failed workflow task.
    const failed_point = try ResetFixtures.awaitEvent(c, run.id, started, .workflow_task_failed_event_attributes, 1);
    _ = try ResetFixtures.awaitEvent(c, run.id, started, .workflow_task_completed_event_attributes, 1);
    const again = try c.resetWorkflow(a, run.id, started, .{ .workflow_task_finish_event_id = failed_point, .reason = "reset of a reset" });
    try testing.expectEqual(client_mod.WorkflowExecutionStatus.terminated, (try c.describeWorkflow(a, run.id, started)).status);
    try c.signalWorkflow(SignalFixtures.Approve, run.id, again, .{"ann"});
    var next = try c.getWorkflow([]const u8, run.id, again);
    defer next.deinit();
    try testing.expectEqualStrings("ann", try env.getResult(SignalFixtures.AwaitApprovals, &next, a, null));
}

test "client: execution options: set a pinned, then an auto-upgrade override, then remove it; history and describe follow" {
    var capture: RequestCapture = .{ .io = testing.io, .arena = .init(testing.allocator) };
    defer capture.arena.deinit();
    const interceptors = capture.interceptors();
    const env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{ .interceptors = &interceptors });
    defer env.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const c = &env.client;

    // The server pins only to a version that has polled the workflow's task queue.
    var rollout = try c.workerDeployment("rollout");
    defer rollout.deinit();
    var v1: VersionFixtures.VersionWorker = undefined;
    try v1.start(c, "options-override", "v1");
    defer v1.stop();
    try VersionFixtures.awaitVersion(&rollout, "v1");
    try VersionFixtures.awaitPolled(&rollout, "v1", "options-override");

    // v1 isn't current and no unversioned worker polls, so the first workflow task waits
    // until the pin sends it to v1.
    var run = try c.executeWorkflow(VersionFixtures.AutoRollout, .{ .id = "options-pinned", .task_queue = "options-override" }, .{});
    defer run.deinit();
    defer c.terminateWorkflow(run.id, "", "test cleanup") catch {};

    const pinned = try c.updateWorkflowExecutionOptions(a, run.id, "", .{ .versioning_override = .{ .set = .{ .pinned = .{ .deployment_name = "rollout", .build_id = "v1" } } } });
    try testing.expectEqualStrings("rollout", pinned.versioning_override.?.pinned.deployment_name);
    try testing.expectEqualStrings("v1", pinned.versioning_override.?.pinned.build_id);
    try testing.expectEqualStrings("v1", pinned.raw.workflow_execution_options.?.versioning_override.?.override.?.pinned.version.?.build_id);
    try testing.expectEqualStrings("v1", try c.queryWorkflow(VersionFixtures.StartedOn, a, run.id, "", .{}));
    const pinned_info = (try c.describeWorkflow(a, run.id, "")).raw.workflow_execution_info.?.versioning_info.?;
    try testing.expectEqualStrings("v1", pinned_info.versioning_override.?.override.?.pinned.version.?.build_id);

    const auto = try c.updateWorkflowExecutionOptions(a, run.id, "", .{ .versioning_override = .{ .set = .auto_upgrade } });
    try testing.expect(auto.versioning_override.? == .auto_upgrade);
    const auto_info = (try c.describeWorkflow(a, run.id, "")).raw.workflow_execution_info.?.versioning_info.?;
    try testing.expect(auto_info.versioning_override.?.override.?.auto_upgrade);

    const removed = try c.updateWorkflowExecutionOptions(a, run.id, "", .{ .versioning_override = .remove });
    try testing.expect(removed.versioning_override == null);
    // The run's own versioning info stays (it ran on v1); only the override is gone.
    if ((try c.describeWorkflow(a, run.id, "")).raw.workflow_execution_info.?.versioning_info) |info| {
        try testing.expect(info.versioning_override == null);
    }

    _ = try ResetFixtures.awaitEvent(c, run.id, run.run_id, .workflow_execution_options_updated_event_attributes, 3);
    const history = try c.fetchHistory(a, run.id, run.run_id);
    var updates: std.ArrayList(proto.temporal.api.history.v1.WorkflowExecutionOptionsUpdatedEventAttributes) = .empty;
    for (history.events.items) |event| switch (event.attributes orelse continue) {
        .workflow_execution_options_updated_event_attributes => |attrs| try updates.append(a, attrs),
        else => {},
    };
    try testing.expectEqual(3, updates.items.len);
    const pin = updates.items[0].versioning_override.?.override.?.pinned;
    try testing.expectEqual(.PINNED_OVERRIDE_BEHAVIOR_PINNED, pin.behavior);
    try testing.expectEqualStrings("rollout", pin.version.?.deployment_name);
    try testing.expectEqualStrings("v1", pin.version.?.build_id);
    try testing.expect(!updates.items[0].unset_versioning_override);
    try testing.expectEqualStrings(c.identity, updates.items[0].identity);
    try testing.expect(updates.items[1].versioning_override.?.override.?.auto_upgrade);
    try testing.expect(updates.items[2].unset_versioning_override);
    try testing.expect(updates.items[2].versioning_override == null);

    try testing.expectEqual(3, capture.option_updates.items.len);
    for (capture.option_updates.items) |request| {
        const paths = request.update_mask.?.paths.items;
        try testing.expectEqual(1, paths.len);
        try testing.expectEqualStrings("versioning_override", paths[0]);
        try testing.expect(request.workflow_execution_options != null);
        try testing.expectEqualStrings(c.namespace, request.namespace);
        try testing.expectEqualStrings(c.identity, request.identity);
        try testing.expectEqualStrings("options-pinned", request.workflow_execution.?.workflow_id);
        try testing.expectEqual(0, request.workflow_execution.?.run_id.len);
    }
    try testing.expect(capture.option_updates.items[2].workflow_execution_options.?.versioning_override == null);
}

test "client: execution options errors and no-ops: no change is InvalidOptions, a pin the server refuses is RpcFailed, a change to the options as they are records nothing, unknown and closed workflows are NotFound" {
    var capture: RequestCapture = .{ .io = testing.io, .arena = .init(testing.allocator) };
    defer capture.arena.deinit();
    const interceptors = capture.interceptors();
    const env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{ .interceptors = &interceptors });
    defer env.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const c = &env.client;

    // v1 polls a task queue other than the workflow's.
    var rollout = try c.workerDeployment("rollout");
    defer rollout.deinit();
    var v1: VersionFixtures.VersionWorker = undefined;
    try v1.start(c, "options-errors-other-queue", "v1");
    defer v1.stop();
    try VersionFixtures.awaitPolled(&rollout, "v1", "options-errors-other-queue");

    var run = try c.executeWorkflow(Fixtures.Hello, .{ .id = "options-errors", .task_queue = "nobody-polls-this" }, .{"options"});
    defer run.deinit();
    defer c.terminateWorkflow(run.id, "", "test cleanup") catch {};

    try testing.expectError(error.InvalidOptions, c.updateWorkflowExecutionOptions(a, run.id, "", .{}));
    try testing.expectEqual(0, capture.option_updates.items.len);

    // Pins to versions that never polled, have empty or huge names, or polled another queue.
    const long = try a.alloc(u8, 1024);
    @memset(long, 'v');
    for ([_]deployment_mod.Version{
        .{ .deployment_name = "rollout", .build_id = "never-polled" },
        .{ .deployment_name = "", .build_id = "" },
        .{ .deployment_name = "rollout", .build_id = "" },
        .{ .deployment_name = "", .build_id = "v1" },
        .{ .deployment_name = long, .build_id = long },
        .{ .deployment_name = "rollout", .build_id = "v1" },
    }) |version| {
        try testing.expectError(error.RpcFailed, c.updateWorkflowExecutionOptions(a, run.id, "", .{ .versioning_override = .{ .set = .{ .pinned = version } } }));
    }
    try testing.expectEqual(0, try ResetFixtures.countEvents(c, a, run.id, run.run_id, .workflow_execution_options_updated_event_attributes));

    // Each step names the run; one that leaves the options as they were records nothing.
    const Step = struct { change: client_mod.VersioningOverrideChange, events: usize };
    for ([_]Step{
        .{ .change = .remove, .events = 0 },
        .{ .change = .{ .set = .auto_upgrade }, .events = 1 },
        .{ .change = .{ .set = .auto_upgrade }, .events = 1 },
        .{ .change = .remove, .events = 2 },
        .{ .change = .remove, .events = 2 },
    }) |step| {
        const options = try c.updateWorkflowExecutionOptions(a, run.id, run.run_id, .{ .versioning_override = step.change });
        switch (step.change) {
            .remove => try testing.expect(options.versioning_override == null),
            .set => try testing.expect((options.versioning_override orelse return error.TestUnexpectedResult) == .auto_upgrade),
        }
        try testing.expectEqualStrings(run.run_id, capture.option_updates.items[capture.option_updates.items.len - 1].workflow_execution.?.run_id);
        try testing.expectEqual(step.events, try ResetFixtures.countEvents(c, a, run.id, run.run_id, .workflow_execution_options_updated_event_attributes));
    }

    try testing.expectError(error.NotFound, c.updateWorkflowExecutionOptions(a, "no-such-workflow", "", .{ .versioning_override = .{ .set = .auto_upgrade } }));
    try c.terminateWorkflow(run.id, "", "close it");
    try testing.expectError(error.NotFound, c.updateWorkflowExecutionOptions(a, run.id, "", .{ .versioning_override = .{ .set = .auto_upgrade } }));
    try testing.expectEqual(13, capture.option_updates.items.len);
}

test "client: an empty task queue fails every client start before any call; the environment fills in its own" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(Fixtures.Hello);
    try env.registerActivity(Fixtures.Greet);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    try testing.expectEqualStrings("Hello default!", try env.executeWorkflowWithOptions(Fixtures.Hello, .{}, .{"default"}, a, null));

    const options: client_mod.StartWorkflowOptions = .{ .id = "no-task-queue", .id_conflict_policy = .use_existing };
    try testing.expectError(error.InvalidOptions, env.client.executeWorkflow(Fixtures.Hello, options, .{"typed"}));
    try testing.expectError(error.InvalidOptions, env.client.executeWorkflowByName([]const u8, Fixtures.Hello.workflow_name, options, .{"by name"}));
    try testing.expectError(error.InvalidOptions, env.client.signalWithStartWorkflow(SignalFixtures.AwaitApprovals, options, .{1}, SignalFixtures.Approve, .{"ann"}));
    try testing.expectError(error.InvalidOptions, env.client.updateWithStartWorkflow(UpdateFixtures.HandleUpdates, options, .{}, UpdateFixtures.Set, .{3}, .{}));
    // None of them reached the server.
    try testing.expectError(error.NotFound, env.client.describeWorkflow(a, "no-task-queue", ""));
}

test "client: signal-with-start sends the retry policy, summary and priority" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // No worker polls this task queue; the started event is all the test needs.
    var run = try env.client.signalWithStartWorkflow(SignalFixtures.AwaitApprovals, .{
        .task_queue = "nobody-polls-this",
        .retry_policy = .{ .maximum_attempts = 5 },
        .static_summary = "Approval",
        .priority = .{ .priority_key = 3 },
    }, .{1}, SignalFixtures.Approve, .{"ann"});
    defer run.deinit();
    defer env.client.terminateWorkflow(run.id, run.run_id, "test cleanup") catch {};
    const event = try StartOptionFixtures.startedEvent(&env.client, a, run.id, run.run_id);
    try StartOptionFixtures.expectMetadata(a, event, "Approval", "");
    const started = event.attributes.?.workflow_execution_started_event_attributes;
    try testing.expectEqual(@as(i32, 5), started.retry_policy.?.maximum_attempts);
    try testing.expectEqual(@as(i32, 3), started.priority.?.priority_key);
}

test "client: update-with-start sends the retry policy, summary and priority" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(UpdateFixtures.HandleUpdates);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var handle = try env.updateWithStartWorkflow(UpdateFixtures.HandleUpdates, .{
        .id_conflict_policy = .use_existing,
        .retry_policy = .{ .maximum_attempts = 5 },
        .static_summary = "Updates",
        .priority = .{ .priority_key = 3 },
    }, .{}, UpdateFixtures.Set, .{3}, .{});
    defer handle.deinit();
    try testing.expectEqual(@as(i64, 0), try handle.get(a));
    defer env.client.terminateWorkflow(handle.workflow_id, handle.run_id, "test cleanup") catch {};
    const event = try StartOptionFixtures.startedEvent(&env.client, a, handle.workflow_id, handle.run_id);
    try StartOptionFixtures.expectMetadata(a, event, "Updates", "");
    const started = event.attributes.?.workflow_execution_started_event_attributes;
    try testing.expectEqual(@as(i32, 5), started.retry_policy.?.maximum_attempts);
    try testing.expectEqual(@as(i32, 3), started.priority.?.priority_key);
}

test "client: start options reach by-name, eager, and environment starts" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(Fixtures.Hello);
    try env.registerActivity(Fixtures.Greet);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const options: client_mod.StartWorkflowOptions = .{
        .task_queue = "",
        .retry_policy = .{ .maximum_attempts = 3 },
        .static_summary = "Greets someone",
        .priority = .{ .priority_key = 4 },
    };
    // The environment's worker is unversioned, so no versioning override here.
    var eager_options = options;
    eager_options.enable_eager_start = true;
    var eager = try env.startWorkflow(Fixtures.Hello, eager_options, .{"eager"});
    defer eager.deinit();
    try testing.expect(eager.eagerly_started);
    try testing.expectEqualStrings("Hello eager!", try env.getResult(Fixtures.Hello, &eager, a, null));

    var by_name = try env.startWorkflowByName([]const u8, Fixtures.Hello.workflow_name, options, .{"by name"});
    defer by_name.deinit();
    try testing.expectEqualStrings("Hello by name!", try env.getResultByName([]const u8, &by_name, a, null));

    for ([_][2][]const u8{ .{ eager.id, eager.run_id }, .{ by_name.id, by_name.run_id } }) |run| {
        const event = try StartOptionFixtures.startedEvent(&env.client, a, run[0], run[1]);
        try StartOptionFixtures.expectMetadata(a, event, "Greets someone", "");
        const started = event.attributes.?.workflow_execution_started_event_attributes;
        try testing.expectEqual(@as(i32, 3), started.retry_policy.?.maximum_attempts);
        try testing.expectEqual(@as(i32, 4), started.priority.?.priority_key);
    }
}

test "client: error_when_already_started = false returns the run holding the workflow ID" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const id = try std.fmt.allocPrint(a, "already-started-{s}", .{env.task_queue});
    const queue = "nobody-polls-this";

    var first = try env.client.executeWorkflow(Fixtures.Hello, .{ .id = id, .task_queue = queue }, .{"first"});
    defer first.deinit();

    // A running run.
    try testing.expectError(error.WorkflowExecutionAlreadyStarted, env.client.executeWorkflow(Fixtures.Hello, .{ .id = id, .task_queue = queue }, .{"again"}));
    try testing.expectError(error.WorkflowExecutionAlreadyStarted, env.client.executeWorkflow(Fixtures.Hello, .{ .id = id, .task_queue = queue, .id_conflict_policy = .fail }, .{"again"}));
    var running = try env.client.executeWorkflow(Fixtures.Hello, .{ .id = id, .task_queue = queue, .error_when_already_started = false }, .{"again"});
    defer running.deinit();
    try testing.expectEqualStrings(first.run_id, running.run_id);
    try testing.expect(!running.eagerly_started);
    var running_by_name = try env.client.executeWorkflowByName([]const u8, Fixtures.Hello.workflow_name, .{ .id = id, .task_queue = queue, .id_conflict_policy = .fail, .error_when_already_started = false }, .{"again"});
    defer running_by_name.deinit();
    try testing.expectEqualStrings(first.run_id, running_by_name.run_id);
    var existing = try env.client.executeWorkflow(Fixtures.Hello, .{ .id = id, .task_queue = queue, .id_conflict_policy = .use_existing }, .{"again"});
    defer existing.deinit();
    try testing.expectEqualStrings(first.run_id, existing.run_id);

    // A closed run whose ID the reuse policy keeps.
    try env.client.terminateWorkflow(id, first.run_id, "test");
    const closed: client_mod.StartWorkflowOptions = .{ .id = id, .task_queue = queue, .id_reuse_policy = .reject_duplicate };
    try testing.expectError(error.WorkflowExecutionAlreadyStarted, env.client.executeWorkflow(Fixtures.Hello, closed, .{"again"}));
    var use_existing = closed;
    use_existing.id_conflict_policy = .use_existing;
    try testing.expectError(error.WorkflowExecutionAlreadyStarted, env.client.executeWorkflow(Fixtures.Hello, use_existing, .{"again"}));
    use_existing.error_when_already_started = false;
    var closed_existing = try env.client.executeWorkflow(Fixtures.Hello, use_existing, .{"again"});
    defer closed_existing.deinit();
    try testing.expectEqualStrings(first.run_id, closed_existing.run_id);
    var lenient = closed;
    lenient.error_when_already_started = false;
    var closed_by_name = try env.client.executeWorkflowByName([]const u8, Fixtures.Hello.workflow_name, lenient, .{"again"});
    defer closed_by_name.deinit();
    try testing.expectEqualStrings(first.run_id, closed_by_name.run_id);
    try testing.expectError(error.WorkflowTerminated, closed_by_name.get(a));
}

test "client: describe reads status and the memo set at start, while running and after" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(Fixtures.Hello);
    try env.registerActivity(Fixtures.Greet);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const Owner = struct { team: []const u8, oncall: bool };
    const memo = try converter.toFields(a, .{
        .description = "nightly import",
        .owner = Owner{ .team = "data", .oncall = true },
    });
    // Started before the environment's worker runs, so it stays running until the worker starts.
    var run = try env.client.executeWorkflow(Fixtures.Hello, .{ .id = "describe-memo", .task_queue = env.task_queue, .memo = memo }, .{"memo"});
    defer run.deinit();

    const running = try env.client.describeWorkflow(a, run.id, "");
    try testing.expectEqual(client_mod.WorkflowExecutionStatus.running, running.status);
    try testing.expectEqualStrings("describe-memo", running.workflow_id);
    try testing.expectEqualStrings(run.run_id, running.run_id);
    try testing.expectEqualStrings("HelloWorld", running.workflow_type);
    try testing.expectEqualStrings(env.task_queue, running.task_queue);
    try testing.expect(running.start_time != null);
    try testing.expect(running.execution_time != null);
    try testing.expectEqual(@as(?Io.Timestamp, null), running.close_time);
    try testing.expectEqual(@as(usize, 2), running.memo.len);
    try testing.expectEqualStrings("nightly import", (try running.memoValue([]const u8, a, "description")).?);
    const owner = (try running.memoValue(Owner, a, "owner")).?;
    try testing.expectEqualStrings("data", owner.team);
    try testing.expect(owner.oncall);
    try testing.expectEqual(@as(?[]const u8, null), try running.memoValue([]const u8, a, "missing"));

    _ = try env.executeWorkflow(Fixtures.Hello, .{"warm-up"}, a, null); // starts the worker
    try testing.expectEqualStrings("Hello memo!", try env.getResult(Fixtures.Hello, &run, a, null));

    const done = try env.client.describeWorkflow(a, run.id, run.run_id);
    try testing.expectEqual(client_mod.WorkflowExecutionStatus.completed, done.status);
    try testing.expect(done.close_time.?.nanoseconds >= done.start_time.?.nanoseconds);
    try testing.expect(done.history_length > running.history_length);
    try testing.expectEqualStrings("nightly import", (try done.memoValue([]const u8, a, "description")).?);
    try testing.expectEqualStrings("data", (try done.memoValue(Owner, a, "owner")).?.team);
    try testing.expect(done.raw.workflow_execution_info != null);

    try testing.expectError(error.NotFound, env.client.describeWorkflow(a, "no-such-workflow", ""));
}

test "an upserted memo reads back through describe; null and empty slices remove keys" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(Fixtures.MemoUpdates);
    try env.registerActivity(Fixtures.Greet);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const memo = try converter.toFields(a, .{ .status = "queued", .owner = "data", .note = "keep" });
    var run = try env.startWorkflow(Fixtures.MemoUpdates, .{ .task_queue = "", .memo = memo }, .{"shipped"});
    defer run.deinit();
    // getResult also replays the run's history.
    try testing.expectEqualStrings("queued: done,note,status", try env.getResult(Fixtures.MemoUpdates, &run, a, null));

    const d = try env.client.describeWorkflow(a, run.id, run.run_id);
    var names: std.ArrayList([]const u8) = .empty;
    for (d.memo) |f| try names.append(a, f.name);
    std.mem.sort([]const u8, names.items, {}, struct {
        fn lt(_: void, x: []const u8, y: []const u8) bool {
            return std.mem.lessThan(u8, x, y);
        }
    }.lt);
    try testing.expectEqualStrings("done,note,status", try std.mem.join(a, ",", names.items));
    try testing.expectEqualStrings("shipped", (try d.memoValue([]const u8, a, "status")).?);
    try testing.expectEqualStrings("keep", (try d.memoValue([]const u8, a, "note")).?);
    try testing.expect((try d.memoValue(bool, a, "done")).?);
}

test "client: query a running, a canceled and a completed workflow" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(Fixtures.Queryable);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var run = try env.startWorkflow(Fixtures.Queryable, .{ .task_queue = "" }, .{std.time.s_per_hour});
    defer run.deinit();
    // A query sent before the first workflow task completes is held by the server until it
    // does (or until the call times out with no worker polling), so the handlers are set.
    try testing.expectEqualStrings("sleeping", try env.client.queryWorkflow(Fixtures.Status, a, run.id, run.run_id, .{}));
    try Fixtures.waitForTimer(&env.client, run.id, run.run_id);
    try testing.expectEqual(@as(i64, 103), try env.client.queryWorkflow(Fixtures.Add, a, run.id, "", .{ 1, 2 }));
    try testing.expectEqual(@as(i64, 103), try env.client.queryWorkflowWithOptions(Fixtures.Add, a, run.id, run.run_id, .{ 1, 2 }, .{ .reject_condition = .not_open }));

    // An unknown query type and a failing handler fail the query with a message.
    var message: []const u8 = "";
    const Missing = workflow.QueryDef("missing", fn () u32);
    try testing.expectError(error.QueryFailed, env.client.queryWorkflowWithOptions(Missing, a, run.id, "", .{}, .{ .failure_message = &message }));
    try testing.expect(std.mem.indexOf(u8, message, "query type \"missing\" is not registered on workflow \"Queryable\"; registered query types: [status, add, fails]") != null);
    message = "";
    try testing.expectError(error.QueryFailed, env.client.queryWorkflowWithOptions(Fixtures.FailingQuery, a, run.id, "", .{}, .{ .failure_message = &message }));
    try testing.expect(std.mem.indexOf(u8, message, "NotReady") != null);
    try testing.expectError(error.QueryFailed, env.client.queryWorkflow(Fixtures.FailingQuery, a, run.id, "", .{}));

    // A canceled run still answers unless the reject condition excludes it.
    try env.client.cancelWorkflow(run.id, run.run_id, "");
    try testing.expectError(error.WorkflowCanceled, env.getResult(Fixtures.Queryable, &run, a, null));
    try testing.expectEqualStrings("sleeping", try env.client.queryWorkflow(Fixtures.Status, a, run.id, run.run_id, .{}));
    var status: client_mod.WorkflowExecutionStatus = .unspecified;
    try testing.expectError(error.QueryRejected, env.client.queryWorkflowWithOptions(Fixtures.Status, a, run.id, run.run_id, .{}, .{ .reject_condition = .not_completed_cleanly, .rejected_status = &status }));
    try testing.expectEqual(client_mod.WorkflowExecutionStatus.canceled, status);

    // A completed run answers from its final state: the server sends the query to a worker,
    // which replays the history.
    var done = try env.startWorkflow(Fixtures.Queryable, .{ .task_queue = "" }, .{1});
    defer done.deinit();
    try testing.expectEqualStrings("finished", try env.getResult(Fixtures.Queryable, &done, a, null));
    try testing.expectEqualStrings("done", try env.client.queryWorkflow(Fixtures.Status, a, done.id, "", .{}));
    try testing.expectEqualStrings("done", try env.client.queryWorkflowWithOptions(Fixtures.Status, a, done.id, "", .{}, .{ .reject_condition = .not_completed_cleanly }));
    status = .unspecified;
    try testing.expectError(error.QueryRejected, env.client.queryWorkflowWithOptions(Fixtures.Status, a, done.id, "", .{}, .{ .reject_condition = .not_open, .rejected_status = &status }));
    try testing.expectEqual(client_mod.WorkflowExecutionStatus.completed, status);

    try testing.expectError(error.NotFound, env.client.queryWorkflow(Fixtures.Status, a, "no-such-workflow", "", .{}));
}

test "client: every connection option at once, a health check, and a stopped server" {
    var server = try DevServer.start(testing.allocator, testing.io, .{});
    var running = true;
    defer if (running) server.stop(testing.io);
    var options = server.clientOptions();
    options.keep_alive = .{ .interval = .fromSeconds(5), .timeout = .fromSeconds(2) };
    options.retry = .{ .max_retries = 2, .max_elapsed_time = .fromSeconds(3) };
    options.client_name = "temporal-zig-test-wrapper";
    options.client_version = "0.0.1";
    options.worker_heartbeat_interval = .fromSeconds(5);
    options.rpc_timeout = .fromSeconds(10);
    var c = try client_mod.Client.connect(testing.allocator, testing.io, options);
    defer c.close();

    try c.checkHealth();
    _ = try c.countWorkflows("");

    // A server that is gone fails the check at once (it isn't retried).
    server.stop(testing.io);
    running = false;
    try testing.expectError(error.RpcFailed, c.checkHealth());
}

test "client: rpc_timeout doesn't cut short waiting for a workflow's result" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(Fixtures.Queryable);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();

    var run = try env.startWorkflow(Fixtures.Queryable, .{ .task_queue = "" }, .{1});
    defer run.deinit();
    var options = env.server.clientOptions();
    options.rpc_timeout = .fromMilliseconds(200);
    var other = try client_mod.Client.connect(testing.allocator, testing.io, options);
    defer other.close();
    // The workflow sleeps for 1 s, so the long poll for its result outlasts the timeout.
    var waiting = try other.getWorkflow([]const u8, run.id, run.run_id);
    defer waiting.deinit();
    try testing.expectEqualStrings("finished", try waiting.get(arena.allocator()));
}

test "client: query a workflow by a name known only at run time" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(Fixtures.Queryable);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var run = try env.startWorkflow(Fixtures.Queryable, .{ .task_queue = "" }, .{std.time.s_per_hour});
    defer run.deinit();
    const status = try env.client.queryWorkflowByName(Fixtures.Status.query_name, a, run.id, run.run_id, .{});
    try testing.expect(status.hasValue());
    try testing.expectEqualStrings(try env.client.queryWorkflow(Fixtures.Status, a, run.id, run.run_id, .{}), try status.get([]const u8, a));
    try testing.expectError(error.InvalidPayload, status.get(i64, a));
    try Fixtures.waitForTimer(&env.client, run.id, run.run_id);
    const sum = try env.client.queryWorkflowByNameWithOptions(Fixtures.Add.query_name, a, run.id, "", .{ @as(i64, 1), @as(i64, 2) }, .{ .reject_condition = .not_open });
    try testing.expectEqual(try env.client.queryWorkflow(Fixtures.Add, a, run.id, "", .{ 1, 2 }), try sum.get(i64, a));

    var message: []const u8 = "";
    try testing.expectError(error.QueryFailed, env.client.queryWorkflowByNameWithOptions("missing", a, run.id, "", .{}, .{ .failure_message = &message }));
    try testing.expect(std.mem.indexOf(u8, message, "query type \"missing\" is not registered") != null);
    try testing.expectError(error.QueryFailed, env.client.queryWorkflowByName("missing", a, run.id, "", .{}));
    try env.client.cancelWorkflow(run.id, run.run_id, "");
}

test "ActivityEnvironment hands the activity every payload of the previous attempt's heartbeat" {
    const S = struct {
        fn resumeWork(ctx: *activity.Context) !void {
            const details = ctx.encodedHeartbeatDetails();
            if (!ctx.hasHeartbeatDetails()) return testing.expectEqual(@as(usize, 0), details.len());
            try testing.expectEqual(@as(usize, 3), details.len());
            try testing.expectEqual(@as(i64, 5), try details.get(i64, ctx.allocator(), 0));
            try testing.expectEqualStrings("cursor-9", try details.get([]const u8, ctx.allocator(), 1));
            try testing.expect(try details.get(bool, ctx.allocator(), 2));
            try testing.expectEqual(@as(i64, 5), try ctx.heartbeatDetails(i64));
        }
        const Resume = activity.Def("Resume", resumeWork);
    };
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var env: ActivityEnvironment = .init(testing.io);
    try env.executeActivity(S.Resume, .{}, a);
    const previous = try converter.toPayloads(a, .{ @as(i64, 5), @as([]const u8, "cursor-9"), true });
    env.heartbeat_details = previous.items;
    try env.executeActivity(S.Resume, .{}, a);
}

test "client: signal a running workflow; signaling an unknown or closed one is NotFound" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(SignalFixtures.AwaitApprovals);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var run = try env.startWorkflow(SignalFixtures.AwaitApprovals, .{ .task_queue = "" }, .{2});
    defer run.deinit();
    try env.client.signalWorkflow(SignalFixtures.Approve, run.id, run.run_id, .{"ann"});
    try env.client.signalWorkflow(SignalFixtures.Approve, run.id, "", .{"bob"});
    try testing.expectEqualStrings("ann bob", try env.getResult(SignalFixtures.AwaitApprovals, &run, a, null));

    const history = try env.client.fetchHistory(a, run.id, run.run_id);
    var signaled: usize = 0;
    for (history.events.items) |event| switch (event.attributes orelse continue) {
        .workflow_execution_signaled_event_attributes => |attrs| {
            signaled += 1;
            try testing.expectEqualStrings("approve", attrs.signal_name);
            try testing.expectEqualStrings(env.client.identity, attrs.identity);
        },
        else => {},
    };
    try testing.expectEqual(@as(usize, 2), signaled);

    try testing.expectError(error.NotFound, env.client.signalWorkflow(SignalFixtures.Approve, run.id, run.run_id, .{"late"}));
    try testing.expectError(error.NotFound, env.client.signalWorkflow(SignalFixtures.Approve, "no-such-workflow", "", .{"nobody"}));
}

test "client: signal-with-start starts a workflow with the signal, then only signals it while it runs" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(SignalFixtures.AwaitApprovals);
    try env.registerWorkflow(Fixtures.Hello);
    try env.registerActivity(Fixtures.Greet);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    _ = try env.executeWorkflow(Fixtures.Hello, .{"warm-up"}, a, null); // starts the worker

    const memo = try converter.toFields(a, .{ .source = "signal-with-start" });
    const options: client_mod.StartWorkflowOptions = .{ .id = "signal-with-start", .task_queue = env.task_queue, .memo = memo };
    var run = try env.client.signalWithStartWorkflow(SignalFixtures.AwaitApprovals, options, .{2}, SignalFixtures.Approve, .{"ann"});
    defer run.deinit();
    try testing.expectEqualStrings("signal-with-start", run.id);

    // The workflow is running (it waits for a second approval), so this only signals it;
    // the different start argument is ignored.
    var again = try env.client.signalWithStartWorkflow(SignalFixtures.AwaitApprovals, options, .{99}, SignalFixtures.Approve, .{"bob"});
    defer again.deinit();
    try testing.expectEqualStrings(run.run_id, again.run_id);
    try testing.expectEqualStrings("ann bob", try env.getResult(SignalFixtures.AwaitApprovals, &run, a, null));

    // The start options reached the server along with the signal.
    const d = try env.client.describeWorkflow(a, run.id, run.run_id);
    try testing.expectEqualStrings("AwaitApprovals", d.workflow_type);
    try testing.expectEqualStrings(env.task_queue, d.task_queue);
    try testing.expectEqualStrings("signal-with-start", (try d.memoValue([]const u8, a, "source")).?);

    // A generated ID always starts a new workflow.
    var fresh = try env.client.signalWithStartWorkflow(SignalFixtures.AwaitApprovals, .{ .task_queue = env.task_queue }, .{1}, SignalFixtures.Approve, .{"cy"});
    defer fresh.deinit();
    try testing.expectEqual(@as(usize, 36), fresh.id.len);
    try testing.expectEqualStrings("cy", try env.getResult(SignalFixtures.AwaitApprovals, &fresh, a, null));
}

test "client: update a running workflow; rejected and unregistered updates are UpdateFailed" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(UpdateFixtures.HandleUpdates);
    try env.registerActivity(Fixtures.Greet);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var run = try env.startWorkflow(UpdateFixtures.HandleUpdates, .{ .task_queue = "" }, .{});
    defer run.deinit();

    var set = try env.client.updateWorkflow(UpdateFixtures.Set, run.id, run.run_id, .{5}, .{});
    defer set.deinit();
    try testing.expectEqual(@as(i64, 0), try set.get(a));
    try testing.expectEqualStrings(run.run_id, set.run_id);

    var rejected = try env.client.updateWorkflow(UpdateFixtures.Set, run.id, "", .{-1}, .{});
    defer rejected.deinit();
    try testing.expectError(error.UpdateFailed, rejected.get(a));
    try testing.expectEqualStrings("NotPositive", rejected.failure().?.message);

    const Unregistered = workflow.UpdateDef("unregistered", fn () void);
    var unknown = try env.client.updateWorkflow(Unregistered, run.id, run.run_id, .{}, .{});
    defer unknown.deinit();
    try testing.expectError(error.UpdateFailed, unknown.get(a));
    try testing.expectEqualStrings("UpdateNotRegistered", failure_mod.applicationType(unknown.failure().?.*).?);

    // Accepted returns once the handler started; `get` waits for its activity.
    var greet = try env.client.updateWorkflow(UpdateFixtures.Greet, run.id, run.run_id, .{"ann"}, .{ .update_id = "greet-ann", .wait_for_stage = .accepted });
    defer greet.deinit();
    try testing.expectEqualStrings("greet-ann", greet.update_id);
    try testing.expectEqualStrings("Hello ann!", try greet.get(a));

    var again = try env.client.getWorkflowUpdateHandle([]const u8, run.id, run.run_id, "greet-ann");
    defer again.deinit();
    try testing.expectEqualStrings("Hello ann!", try again.get(a));

    try env.client.signalWorkflow(UpdateFixtures.Done, run.id, run.run_id, .{});
    try testing.expectEqualStrings("5 Hello ann!", try env.getResult(UpdateFixtures.HandleUpdates, &run, a, null));
    try testing.expectError(error.NotFound, env.client.updateWorkflow(UpdateFixtures.Set, "no-such-workflow", "", .{1}, .{}));
}

test "client: an update handler's failed activity or ctx.fail is the update's failure" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(UpdateFailureFixtures.Payments);
    try env.registerActivity(UpdateFailureFixtures.ChargeActivity);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var run = try env.startWorkflow(UpdateFailureFixtures.Payments, .{ .task_queue = "" }, .{});
    defer run.deinit();

    var charged = try env.client.updateWorkflow(UpdateFailureFixtures.Charge, run.id, run.run_id, .{-5}, .{});
    defer charged.deinit();
    try testing.expectError(error.UpdateFailed, charged.get(a));
    const cause = failure_mod.rootCause(charged.failure().?);
    try testing.expectEqualStrings("invalid Amount", cause.message);
    try testing.expectEqualStrings("InvalidAmount", failure_mod.applicationType(cause.*).?);

    var refused = try env.client.updateWorkflow(UpdateFailureFixtures.Refuse, run.id, run.run_id, .{}, .{});
    defer refused.deinit();
    try testing.expectError(error.UpdateFailed, refused.get(a));
    try testing.expectEqualStrings("refused", refused.failure().?.message);
    try testing.expectEqualStrings("Refusal", failure_mod.applicationType(refused.failure().?.*).?);

    // Neither failure became the workflow's: it completes.
    try env.client.signalWorkflow(UpdateFixtures.Done, run.id, run.run_id, .{});
    try env.getResult(UpdateFailureFixtures.Payments, &run, a, null);
}

test "client: update-with-start starts a workflow, then updates the running one" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(UpdateFixtures.HandleUpdates);
    try env.registerWorkflow(Fixtures.Hello);
    try env.registerActivity(Fixtures.Greet);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    _ = try env.executeWorkflow(Fixtures.Hello, .{"warm-up"}, a, null); // starts the worker

    var options: client_mod.StartWorkflowOptions = .{ .id = "update-with-start", .task_queue = env.task_queue };
    try testing.expectError(error.InvalidOptions, env.client.updateWithStartWorkflow(UpdateFixtures.HandleUpdates, options, .{}, UpdateFixtures.Set, .{3}, .{}));

    options.id_conflict_policy = .use_existing;
    var first = try env.client.updateWithStartWorkflow(UpdateFixtures.HandleUpdates, options, .{}, UpdateFixtures.Set, .{3}, .{});
    defer first.deinit();
    try testing.expectEqual(@as(i64, 0), try first.get(a));
    try testing.expect(first.run_id.len > 0);

    var second = try env.client.updateWithStartWorkflow(UpdateFixtures.HandleUpdates, options, .{}, UpdateFixtures.Set, .{7}, .{ .wait_for_stage = .accepted });
    defer second.deinit();
    try testing.expectEqual(@as(i64, 3), try second.get(a));
    try testing.expectEqualStrings(first.run_id, second.run_id);

    var failing = options;
    failing.id_conflict_policy = .fail;
    try testing.expectError(error.WorkflowExecutionAlreadyStarted, env.client.updateWithStartWorkflow(UpdateFixtures.HandleUpdates, failing, .{}, UpdateFixtures.Set, .{9}, .{}));

    try env.client.signalWorkflow(UpdateFixtures.Done, options.id, "", .{});
    var run = try env.client.getWorkflow([]const u8, options.id, first.run_id);
    defer run.deinit();
    try testing.expectEqualStrings("7 ", try env.getResult(UpdateFixtures.HandleUpdates, &run, a, null));
}

test "client: canceling the task waiting on an update, or a timeout bounding it, returns Canceled and leaves the update running" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(GateFixtures.Gate);
    for ([_]bool{ false, true }) |bounded| {
        for (std.enums.values(GateFixtures.Call)) |call| {
            GateFixtures.endWaitEarly(env, call, bounded) catch |err| {
                std.debug.print("ending the wait of {t} early ({s}) failed\n", .{ call, if (bounded) "timeout" else "cancel" });
                return err;
            };
        }
    }
}

test "time-skipping WorkflowEnvironment: a workflow mid-timer answers a query while time is locked" {
    const env = try WorkflowEnvironment.startTimeSkipping(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(Fixtures.Queryable);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const start = try env.now();
    var run = try env.startWorkflow(Fixtures.Queryable, .{ .task_queue = "" }, .{2 * std.time.s_per_hour});
    defer run.deinit();
    try Fixtures.waitForTimer(&env.client, run.id, run.run_id);
    try testing.expectEqualStrings("sleeping", try env.client.queryWorkflow(Fixtures.Status, a, run.id, run.run_id, .{}));
    try testing.expectEqual(@as(i64, 105), try env.client.queryWorkflow(Fixtures.Add, a, run.id, run.run_id, .{ 2, 3 }));
    try testing.expect(start.durationTo(try env.now()).nanoseconds < std.time.ns_per_hour);

    try testing.expectEqualStrings("finished", try env.getResult(Fixtures.Queryable, &run, a, null));
    try testing.expectEqualStrings("done", try env.client.queryWorkflow(Fixtures.Status, a, run.id, run.run_id, .{}));
}

test "WorkflowEnvironment: a struct workflow's declared handlers take signal-with-start, update-with-start, and queries" {
    for (env_kinds) |kind| {
        const env = try startEnv(kind);
        defer env.deinit();
        try env.registerWorkflow(StructFixtures.Ledger);
        try env.registerActivity(Fixtures.Greet);
        var arena: std.heap.ArenaAllocator = .init(testing.allocator);
        defer arena.deinit();
        const a = arena.allocator();

        // The signal sent with the start reaches its declared handler, which runs an activity.
        var signaled = try env.signalWithStartWorkflow(StructFixtures.Ledger, .{ .task_queue = "" }, .{"ann"}, StructFixtures.Record, .{"bob"});
        defer signaled.deinit();
        try env.client.signalWorkflow(StructFixtures.Close, signaled.id, signaled.run_id, .{});
        try testing.expectEqualStrings("ann: Hello bob! (limit 10)", try env.getResult(StructFixtures.Ledger, &signaled, a, null));
        try testing.expectEqual(@as(usize, 1), try env.client.queryWorkflow(StructFixtures.Count, a, signaled.id, signaled.run_id, .{}));

        // The validator rejects the update sent with the start; the workflow starts anyway,
        // and the next update-with-start reaches the running workflow and is accepted.
        const options: client_mod.StartWorkflowOptions = .{ .id = "struct-ledger", .task_queue = "", .id_conflict_policy = .use_existing };
        var rejected = try env.updateWithStartWorkflow(StructFixtures.Ledger, options, .{"cy"}, StructFixtures.SetLimit, .{0}, .{});
        defer rejected.deinit();
        try testing.expectError(error.UpdateFailed, rejected.get(a));
        try testing.expectEqualStrings("LimitNotPositive", rejected.failure().?.message);
        var accepted = try env.updateWithStartWorkflow(StructFixtures.Ledger, options, .{"cy"}, StructFixtures.SetLimit, .{25}, .{});
        defer accepted.deinit();
        try testing.expectEqual(@as(i64, 10), try accepted.get(a));
        try testing.expectEqualStrings(rejected.run_id, accepted.run_id);
        try testing.expectEqual(@as(usize, 0), try env.client.queryWorkflow(StructFixtures.Count, a, options.id, accepted.run_id, .{}));

        try env.client.signalWorkflow(StructFixtures.Record, options.id, accepted.run_id, .{"dee"});
        try env.client.signalWorkflow(StructFixtures.Close, options.id, accepted.run_id, .{});
        var run = try env.client.getWorkflow([]const u8, options.id, accepted.run_id);
        defer run.deinit();
        try testing.expectEqualStrings("cy: Hello dee! (limit 25)", try env.getResult(StructFixtures.Ledger, &run, a, null));
    }
}

/// Both kinds of `WorkflowEnvironment`, for tests that run on each.
const env_kinds = [_]WorkflowEnvironment.OwnServer{ .dev, .time_skipping };

fn startEnv(kind: WorkflowEnvironment.OwnServer) !*WorkflowEnvironment {
    return WorkflowEnvironment.create(testing.allocator, testing.io, kind, .{});
}

/// Whether the run's history has a fired timer.
fn timerFired(c: *client_mod.Client, id: []const u8, run_id: []const u8) !bool {
    var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
    defer scratch.deinit();
    const history = try c.fetchHistory(scratch.allocator(), id, run_id);
    for (history.events.items) |event| switch (event.attributes orelse continue) {
        .timer_fired_event_attributes => return true,
        else => {},
    };
    return false;
}

test "WorkflowEnvironment signals, queries and updates a running workflow by its ID" {
    for (env_kinds) |kind| {
        const env = try startEnv(kind);
        defer env.deinit();
        try env.registerWorkflow(SignalFixtures.AwaitApprovals);
        try env.registerWorkflow(Fixtures.Queryable);
        try env.registerWorkflow(UpdateFixtures.HandleUpdates);
        try env.registerActivity(Fixtures.Greet);
        var arena: std.heap.ArenaAllocator = .init(testing.allocator);
        defer arena.deinit();
        const a = arena.allocator();

        var approvals = try env.startWorkflow(SignalFixtures.AwaitApprovals, .{ .task_queue = "" }, .{2});
        defer approvals.deinit();
        try env.signalWorkflow(SignalFixtures.Approve, approvals.id, .{"ann"});
        try env.signalWorkflow(SignalFixtures.Approve, approvals.id, .{"bob"});
        try testing.expectEqualStrings("ann bob", try env.getResult(SignalFixtures.AwaitApprovals, &approvals, a, null));

        // A query to a workflow in the middle of a 2h timer answers at once; with time
        // locked, the timer hasn't fired.
        const start = try env.now();
        var sleeping = try env.startWorkflow(Fixtures.Queryable, .{ .task_queue = "" }, .{2 * std.time.s_per_hour});
        defer sleeping.deinit();
        try Fixtures.waitForTimer(&env.client, sleeping.id, sleeping.run_id);
        try testing.expectEqualStrings("sleeping", try env.queryWorkflow(Fixtures.Status, a, sleeping.id, .{}));
        try testing.expectEqual(@as(i64, 105), try env.queryWorkflow(Fixtures.Add, a, sleeping.id, .{ 2, 3 }));
        try testing.expect(!try timerFired(&env.client, sleeping.id, sleeping.run_id));
        try testing.expect(start.durationTo(try env.now()).nanoseconds < std.time.ns_per_hour);
        try env.client.terminateWorkflow(sleeping.id, "", "test done");

        var updates = try env.startWorkflow(UpdateFixtures.HandleUpdates, .{ .task_queue = "" }, .{});
        defer updates.deinit();
        try testing.expectEqual(@as(i64, 0), try env.updateWorkflow(UpdateFixtures.Set, a, updates.id, .{5}, null));
        var failure: ?failure_mod.Failure = null;
        try testing.expectError(error.UpdateFailed, env.updateWorkflow(UpdateFixtures.Set, a, updates.id, .{-1}, &failure));
        try testing.expectEqualStrings("NotPositive", failure.?.message);
        try testing.expectEqualStrings("Hello ann!", try env.updateWorkflow(UpdateFixtures.Greet, a, updates.id, .{"ann"}, null));
        try env.signalWorkflow(UpdateFixtures.Done, updates.id, .{});
        try testing.expectEqualStrings("5 Hello ann!", try env.getResult(UpdateFixtures.HandleUpdates, &updates, a, null));
    }
}

test "time-skipping WorkflowEnvironment: an update handler's hour-long sleep passes with env.sleep" {
    const env = try WorkflowEnvironment.startTimeSkipping(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(UpdateFixtures.Napper);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var run = try env.startWorkflow(UpdateFixtures.Napper, .{ .task_queue = "" }, .{});
    defer run.deinit();
    const wall_start = Io.Timestamp.now(testing.io, .awake);
    var nap = try env.client.updateWorkflow(UpdateFixtures.Nap, run.id, "", .{std.time.s_per_hour}, .{ .wait_for_stage = .accepted });
    defer nap.deinit();
    try env.sleep(.fromSeconds(std.time.s_per_hour));
    try testing.expect(try nap.get(a) >= std.time.s_per_hour);
    try testing.expect(wall_start.durationTo(Io.Timestamp.now(testing.io, .awake)).nanoseconds < 60 * std.time.ns_per_s);

    // The workflow outlived the skip: it still handles updates and finishes on its signal.
    try testing.expectEqual(@as(i64, 0), try env.updateWorkflow(UpdateFixtures.Nap, a, run.id, .{0}, null));
    try env.signalWorkflow(UpdateFixtures.Done, run.id, .{});
    try env.getResult(UpdateFixtures.Napper, &run, a, null);
}

test "WorkflowEnvironment signal-with-start starts the worker and defaults the ID and task queue" {
    for (env_kinds) |kind| {
        const env = try startEnv(kind);
        defer env.deinit();
        try env.registerWorkflow(SignalFixtures.AwaitApprovals);
        var arena: std.heap.ArenaAllocator = .init(testing.allocator);
        defer arena.deinit();
        const a = arena.allocator();

        var run = try env.signalWithStartWorkflow(SignalFixtures.AwaitApprovals, .{ .task_queue = "" }, .{2}, SignalFixtures.Approve, .{"ann"});
        defer run.deinit();
        try testing.expect(std.mem.startsWith(u8, run.id, "AwaitApprovals-"));
        var again = try env.signalWithStartWorkflow(SignalFixtures.AwaitApprovals, .{ .id = run.id, .task_queue = "" }, .{99}, SignalFixtures.Approve, .{"bob"});
        defer again.deinit();
        try testing.expectEqualStrings(run.run_id, again.run_id);
        try testing.expectEqualStrings("ann bob", try env.getResult(SignalFixtures.AwaitApprovals, &run, a, null));
        try testing.expectEqualStrings(env.task_queue, (try env.client.describeWorkflow(a, run.id, run.run_id)).task_queue);
    }
}

test "WorkflowEnvironment update-with-start starts the worker and defaults the ID and task queue" {
    for (env_kinds) |kind| {
        const env = try startEnv(kind);
        defer env.deinit();
        try env.registerWorkflow(UpdateFixtures.HandleUpdates);
        var arena: std.heap.ArenaAllocator = .init(testing.allocator);
        defer arena.deinit();
        const a = arena.allocator();

        var first = try env.updateWithStartWorkflow(UpdateFixtures.HandleUpdates, .{ .task_queue = "", .id_conflict_policy = .use_existing }, .{}, UpdateFixtures.Set, .{3}, .{});
        defer first.deinit();
        try testing.expectEqual(@as(i64, 0), try first.get(a));
        try testing.expect(std.mem.startsWith(u8, first.workflow_id, "HandleUpdates-"));

        var second = try env.updateWithStartWorkflow(UpdateFixtures.HandleUpdates, .{ .id = first.workflow_id, .task_queue = "", .id_conflict_policy = .use_existing }, .{}, UpdateFixtures.Set, .{7}, .{});
        defer second.deinit();
        try testing.expectEqual(@as(i64, 3), try second.get(a));
        try testing.expectEqualStrings(first.run_id, second.run_id);

        try env.signalWorkflow(UpdateFixtures.Done, first.workflow_id, .{});
        var run = try env.client.getWorkflow([]const u8, first.workflow_id, first.run_id);
        defer run.deinit();
        try testing.expectEqualStrings("7 ", try env.getResult(UpdateFixtures.HandleUpdates, &run, a, null));
        try testing.expectEqualStrings(env.task_queue, (try env.client.describeWorkflow(a, run.id, run.run_id)).task_queue);
    }
}

test "search attributes set at start are read and upserted by the workflow; describe shows the final values" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(Fixtures.SearchAttributeUpdates);
    try env.registerActivity(Fixtures.Greet);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var run = try env.startWorkflow(Fixtures.SearchAttributeUpdates, .{
        .task_queue = "",
        // An unset at start has nothing to remove and is left out.
        .search_attributes = &.{ Fixtures.order_id.set("order-7"), Fixtures.priority.set(1), Fixtures.notes.unset() },
    }, .{"blue"});
    defer run.deinit();
    // getResult also replays the run's history.
    try testing.expectEqualStrings(
        "order-7 1: CustomBoolField,CustomDatetimeField,CustomDoubleField,CustomIntField,CustomKeywordListField,CustomTextField",
        try env.getResult(Fixtures.SearchAttributeUpdates, &run, a, null),
    );

    const d = try env.client.describeWorkflow(a, run.id, run.run_id);
    const sa = d.search_attributes;
    try testing.expectEqual(@as(?[]const u8, null), try sa.get(a, Fixtures.order_id));
    try testing.expectEqual(@as(?i64, 3), try sa.get(a, Fixtures.priority));
    const tags = (try sa.get(a, Fixtures.tags)).?;
    try testing.expectEqual(@as(usize, 2), tags.len);
    try testing.expectEqualStrings("blue", tags[0]);
    try testing.expectEqualStrings("nightly", tags[1]);
    try testing.expectEqual(@as(?f64, 1.5), try sa.get(a, Fixtures.score));
    try testing.expectEqual(@as(?bool, true), try sa.get(a, Fixtures.flagged));
    try testing.expectEqual(try converter.Time.parse(Fixtures.due_time), (try sa.get(a, Fixtures.due)).?);
    try testing.expectEqualStrings("all done", (try sa.get(a, Fixtures.notes)).?);
    // Reading through a key of another type is an error, not a wrong value.
    try testing.expectError(error.InvalidPayload, sa.get(a, search_attributes.key(.keyword, Fixtures.priority.name)));
}

/// Polls `countWorkflows(query)` until it reports `want`, for at most 30 s: visibility
/// catches up with starts and upserts a moment later.
fn waitForCount(c: *client_mod.Client, query: []const u8, want: u64) !void {
    var tries: usize = 0;
    while (try c.countWorkflows(query) != want) : (tries += 1) {
        if (tries == 300) {
            log.err("countWorkflows(\"{s}\") never reached {d}", .{ query, want });
            return error.TestUnexpectedResult;
        }
        try testing.io.sleep(.fromMilliseconds(100), .awake);
    }
}

test "client: list and count workflows by a visibility query on search attributes, a page at a time" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(Fixtures.SearchAttributeUpdates);
    try env.registerActivity(Fixtures.Greet);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // Two runs nobody works on, tagged at start, and one that upserts its own tag.
    for ([_][]const u8{ "idle-1", "idle-2" }) |id| {
        var idle = try env.client.executeWorkflow(Fixtures.Hello, .{
            .id = id,
            .task_queue = "nobody-polls-this",
            .search_attributes = &.{Fixtures.tags.set(&.{env.task_queue})},
            .memo = try converter.toFields(a, .{ .owner = "lister" }),
        }, .{"x"});
        idle.deinit();
    }
    var run = try env.startWorkflow(Fixtures.SearchAttributeUpdates, .{ .id = "upserts-tag", .task_queue = "" }, .{env.task_queue});
    defer run.deinit();
    _ = try env.getResult(Fixtures.SearchAttributeUpdates, &run, a, null);

    const tagged = try std.fmt.allocPrint(a, "CustomKeywordListField = '{s}'", .{env.task_queue});
    try waitForCount(&env.client, tagged, 3);
    const upserted = try std.fmt.allocPrint(a, "{s} AND CustomIntField = 2 AND ExecutionStatus = 'Completed'", .{tagged});
    try waitForCount(&env.client, upserted, 1);

    var it = try env.client.listWorkflows(tagged, .{ .page_size = 2 });
    defer it.deinit();
    var ids: std.ArrayList([]const u8) = .empty;
    while (try it.next(a)) |info| {
        try ids.append(a, info.workflow_id);
        try testing.expectEqualStrings(env.task_queue, (try info.search_attributes.get(a, Fixtures.tags)).?[0]);
        if (std.mem.eql(u8, info.workflow_id, "upserts-tag")) {
            try testing.expectEqual(client_mod.WorkflowExecutionStatus.completed, info.status);
            try testing.expectEqualStrings("SearchAttributeUpdates", info.workflow_type);
            try testing.expectEqualStrings(env.task_queue, info.task_queue);
            try testing.expectEqualStrings(run.run_id, info.run_id);
            try testing.expect(info.close_time != null and info.start_time != null);
            try testing.expect(info.history_length > 0);
            try testing.expectEqual(@as(?i64, 2), try info.search_attributes.get(a, Fixtures.priority));
        } else {
            try testing.expectEqual(client_mod.WorkflowExecutionStatus.running, info.status);
            try testing.expectEqualStrings("lister", (try info.memoValue([]const u8, a, "owner")).?);
        }
    }
    std.mem.sort([]const u8, ids.items, {}, struct {
        fn lt(_: void, x: []const u8, y: []const u8) bool {
            return std.mem.lessThan(u8, x, y);
        }
    }.lt);
    try testing.expectEqualStrings("idle-1,idle-2,upserts-tag", try std.mem.join(a, ",", ids.items));
    try testing.expectEqual(@as(?client_mod.WorkflowExecutionInfo, null), try it.next(a));

    var none = try env.client.listWorkflows("CustomKeywordListField = 'no-such-tag'", .{});
    defer none.deinit();
    try testing.expect(try none.next(a) == null);
}

const ScheduleFixtures = struct {
    /// Reports the search attributes the server sets on a workflow a schedule starts.
    fn scheduledBy(ctx: *workflow.Context, greeting: []const u8) ![]const u8 {
        const sa = ctx.searchAttributes();
        const by = try sa.get(ctx.allocator(), schedule.scheduled_by_id) orelse "nobody";
        const at = try sa.get(ctx.allocator(), schedule.scheduled_start_time);
        return std.fmt.allocPrint(ctx.allocator(), "{s} from {s}, scheduled time known: {}", .{ greeting, by, at != null });
    }
    const ScheduledBy = workflow.Def("ScheduledBy", scheduledBy);

    /// Replaces the intervals with one of `every`, built in the update's arena.
    fn newSpec(every: Io.Duration, arena: Allocator, d: *schedule.Description) !?schedule.Update {
        const intervals = try arena.alloc(schedule.Interval, 1);
        intervals[0] = .{ .every = every };
        d.schedule.spec.intervals = intervals;
        d.schedule.state.note = "slowed down";
        const updates = try arena.alloc(search_attributes.Update, 1);
        updates[0] = Fixtures.priority.set(2);
        return .{ .schedule = d.schedule, .search_attributes = updates };
    }

    fn noChange(_: void, _: Allocator, _: *schedule.Description) error{}!?schedule.Update {
        return null;
    }

    /// Polls `describe` until the schedule has taken `want` actions, for at most 30 s, and
    /// returns that description.
    fn waitForActions(handle: *const client_mod.ScheduleHandle, arena: Allocator, want: i64) !schedule.Description {
        var tries: usize = 0;
        while (true) : (tries += 1) {
            const d = try handle.describe(arena);
            if (d.info.action_count >= want) return d;
            if (tries == 300) {
                log.err("schedule {s} took {d} actions, want {d}", .{ handle.id, d.info.action_count, want });
                return error.TestUnexpectedResult;
            }
            try testing.io.sleep(.fromMilliseconds(100), .awake);
        }
    }
};

test "client: create, describe, update, pause, trigger, backfill, list and delete a schedule" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(ScheduleFixtures.ScheduledBy);
    try env.ensureWorker();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const id = try std.fmt.allocPrint(a, "schedule-{s}", .{env.task_queue});

    var handle = try env.client.createSchedule(id, .{
        .schedule = .{
            .action = try schedule.startWorkflow(a, ScheduleFixtures.ScheduledBy, .{"hello"}, .{
                .id = "scheduled",
                .task_queue = env.task_queue,
                .run_timeout = .fromSeconds(30),
            }),
            .spec = .{ .intervals = &.{.{ .every = .fromSeconds(1) }} },
            .policies = .{ .overlap = .allow_all },
        },
        .memo = try converter.toFields(a, .{ .owner = "scheduler" }),
        .search_attributes = &.{Fixtures.order_id.set(env.task_queue)},
    });
    defer handle.deinit();
    try testing.expectError(error.ScheduleAlreadyExists, env.client.createSchedule(id, .{ .schedule = .{
        .action = try schedule.startWorkflowByName(a, "ScheduledBy", .{"again"}, .{ .id = "x", .task_queue = env.task_queue }),
        .spec = .{},
    } }));

    // The interval fires within a second; the workflow sees which schedule started it.
    var d = try ScheduleFixtures.waitForActions(&handle, a, 1);
    try testing.expectEqualStrings(id, d.id);
    const first = d.info.recent_actions[0];
    try testing.expect(std.mem.startsWith(u8, first.workflow.workflow_id, "scheduled-"));
    try testing.expect(first.scheduled_at != null and first.taken_at != null);
    var run = try env.client.getWorkflow([]const u8, first.workflow.workflow_id, first.workflow.run_id);
    defer run.deinit();
    const want = try std.fmt.allocPrint(a, "hello from {s}, scheduled time known: true", .{id});
    try testing.expectEqualStrings(want, try env.getResult(ScheduleFixtures.ScheduledBy, &run, a, null));

    const action = d.schedule.action.start_workflow;
    try testing.expectEqualStrings("ScheduledBy", action.workflow_type);
    try testing.expectEqualStrings(env.task_queue, action.task_queue);
    try testing.expectEqual(@as(?Io.Duration, .fromSeconds(30)), action.run_timeout);
    try testing.expectEqualStrings("hello", (try converter.fromPayloads(struct { []const u8 }, a, action.args))[0]);
    try testing.expectEqual(Io.Duration.fromSeconds(1), d.schedule.spec.intervals[0].every);
    try testing.expectEqual(schedule.OverlapPolicy.allow_all, d.schedule.policies.overlap);
    try testing.expect(d.info.next_action_times.len > 0);
    try testing.expect(d.info.create_time != null);
    try testing.expectEqualStrings("scheduler", (try d.memoValue([]const u8, a, "owner")).?);
    try testing.expectEqualStrings(env.task_queue, (try d.search_attributes.get(a, Fixtures.order_id)).?);

    // Update by read-modify-write: an hourly interval leaves only triggers and backfills.
    try handle.update({}, ScheduleFixtures.noChange);
    try handle.update(Io.Duration.fromSeconds(3600), ScheduleFixtures.newSpec);
    d = try handle.describe(a);
    try testing.expectEqual(@as(usize, 1), d.schedule.spec.intervals.len);
    try testing.expectEqual(Io.Duration.fromSeconds(3600), d.schedule.spec.intervals[0].every);
    try testing.expectEqualStrings("slowed down", d.schedule.state.note);
    try testing.expectEqualStrings("ScheduledBy", d.schedule.action.start_workflow.workflow_type);
    // The update replaces the schedule's search attributes.
    // The update replaces the schedule's search attributes once the server has applied it,
    // a moment after it returns.
    var polls: usize = 0;
    while (try d.search_attributes.get(a, Fixtures.priority) == null) : (polls += 1) {
        if (polls == 300) return error.TestUnexpectedResult;
        try testing.io.sleep(.fromMilliseconds(100), .awake);
        d = try handle.describe(a);
    }
    try testing.expectEqual(@as(?i64, 2), try d.search_attributes.get(a, Fixtures.priority));
    try testing.expectEqual(@as(?[]const u8, null), try d.search_attributes.get(a, Fixtures.order_id));
    const before_trigger = d.info.action_count;

    try handle.pause("maintenance");
    d = try handle.describe(a);
    try testing.expect(d.schedule.state.paused);
    try testing.expectEqualStrings("maintenance", d.schedule.state.note);
    try handle.unpause("");
    d = try handle.describe(a);
    try testing.expect(!d.schedule.state.paused);
    try testing.expectEqualStrings("Unpaused", d.schedule.state.note);

    try handle.trigger(.unspecified);
    _ = try ScheduleFixtures.waitForActions(&handle, a, before_trigger + 1);

    // Any two-hour period holds two hourly actions.
    const now = Io.Timestamp.now(testing.io, .real);
    try handle.backfill(&.{.{
        .start = now.subDuration(.fromSeconds(3 * 3600 - 1)),
        .end = now.subDuration(.fromSeconds(3600 - 1)),
    }});
    d = try ScheduleFixtures.waitForActions(&handle, a, before_trigger + 3);
    var backfilled: usize = 0;
    for (d.info.recent_actions) |r| {
        if (r.scheduled_at.?.nanoseconds < now.subDuration(.fromSeconds(1800)).nanoseconds) backfilled += 1;
    }
    try testing.expectEqual(@as(usize, 2), backfilled);

    // Listing is eventually consistent: wait until the schedule shows up.
    var tries: usize = 0;
    const listed = while (true) : (tries += 1) {
        var it = try env.client.listSchedules(.{ .page_size = 1 });
        defer it.deinit();
        const found = while (try it.next(a)) |entry| {
            if (std.mem.eql(u8, entry.id, id)) break entry;
        } else null;
        if (found) |entry| if (std.mem.eql(u8, entry.note, "Unpaused")) break entry;
        if (tries == 300) return error.TestUnexpectedResult;
        try testing.io.sleep(.fromMilliseconds(100), .awake);
    };
    try testing.expectEqualStrings("ScheduledBy", listed.workflow_type);
    try testing.expect(!listed.paused);
    try testing.expectEqual(Io.Duration.fromSeconds(3600), listed.spec.intervals[0].every);
    try testing.expectEqualStrings("scheduler", (try converter.fieldValue([]const u8, a, listed.memo, "owner")).?);

    try handle.delete();
    try testing.expectError(error.NotFound, handle.describe(a));
    try testing.expectError(error.NotFound, handle.trigger(.unspecified));
}

test "an activity signals its own workflow through ctx.client()" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(Fixtures.SignalSelf);
    try env.registerActivity(Fixtures.SignalOwnWorkflow);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    try testing.expectEqualStrings("from the activity", try env.executeWorkflow(Fixtures.SignalSelf, .{"from the activity"}, arena.allocator(), null));
}

test "ActivityEnvironment hands the activity its client, or error.NoClient without one" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var env: ActivityEnvironment = .init(testing.io);
    try testing.expectError(error.NoClient, env.executeActivity(Fixtures.SignalOwnWorkflow, .{"x"}, arena.allocator()));

    const Probe = struct {
        fn same(ctx: *activity.Context) !bool {
            return (try ctx.client()).namespace.len > 0;
        }
    };
    var c: client_mod.Client = undefined; // the activity only reads its namespace
    c.namespace = @constCast("default");
    env.client = &c;
    try testing.expect(try env.executeActivity(activity.Def("Probe", Probe.same), .{}, arena.allocator()));
}

test "client: a search attribute value the server can't index fails the start" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    const bad = [_]search_attributes.Update{Fixtures.score.set(std.math.inf(f64))};
    try testing.expectError(error.InvalidSearchAttribute, env.client.executeWorkflow(Fixtures.Hello, .{ .task_queue = env.task_queue, .search_attributes = &bad }, .{"x"}));
}

test "time-skipping WorkflowEnvironment completes a 2h sleep in seconds" {
    const env = try WorkflowEnvironment.startTimeSkipping(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(Fixtures.Sleeper);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const wall_start = Io.Timestamp.now(testing.io, .awake);
    const slept = try env.executeWorkflow(Fixtures.Sleeper, .{2 * std.time.s_per_hour}, arena.allocator(), null);
    try testing.expect(slept >= 2 * std.time.s_per_hour);
    const wall = wall_start.durationTo(Io.Timestamp.now(testing.io, .awake));
    try testing.expect(wall.nanoseconds < 60 * std.time.ns_per_s);
}

test "time-skipping WorkflowEnvironment keeps time locked until a result is awaited" {
    const env = try WorkflowEnvironment.startTimeSkipping(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(Fixtures.Sleeper);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const start = try env.now();
    var run = try env.startWorkflow(Fixtures.Sleeper, .{ .task_queue = "" }, .{2 * std.time.s_per_hour});
    defer run.deinit();

    // Wait for the timer to start; with time locked it must not have fired.
    while (true) {
        var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
        defer scratch.deinit();
        const history = try env.client.fetchHistory(scratch.allocator(), run.id, run.run_id);
        var started = false;
        for (history.events.items) |event| switch (event.attributes orelse continue) {
            .timer_started_event_attributes => started = true,
            .timer_fired_event_attributes => return error.TestUnexpectedResult,
            else => {},
        };
        if (started) break;
        try testing.io.sleep(.fromMilliseconds(20), .awake);
    }
    try testing.expect(start.durationTo(try env.now()).nanoseconds < std.time.ns_per_hour);

    try testing.expectEqual(@as(i64, 2 * std.time.s_per_hour), try env.getResult(Fixtures.Sleeper, &run, a, null));
    try testing.expect(start.durationTo(try env.now()).nanoseconds >= 2 * std.time.ns_per_hour);
}

test "time-skipping WorkflowEnvironment sleep advances server time" {
    const env = try WorkflowEnvironment.startTimeSkipping(testing.allocator, testing.io);
    defer env.deinit();
    const before = try env.now();
    try env.sleep(.fromSeconds(std.time.s_per_hour));
    try testing.expect(before.durationTo(try env.now()).nanoseconds >= std.time.ns_per_hour);
}

/// A dev-server run of `Fixtures.SummaryNaps`, whose timers carry summaries, exported with
/// `temporal workflow show`.
const summary_timer_history = @embedFile("testdata/summary_timer_history.json");

test "WorkflowReplayer replays timers started by ctx.sleep with a summary and catches a dropped timer" {
    var ok: WorkflowReplayer = .init(testing.allocator, testing.io);
    defer ok.deinit();
    try ok.registerWorkflow(Fixtures.SummaryNaps);
    try ok.replayHistoryJson(summary_timer_history);

    var changed: WorkflowReplayer = .init(testing.allocator, testing.io);
    defer changed.deinit();
    try changed.registerWorkflow(Fixtures.SummaryNapsChanged);
    try testing.expectError(error.Nondeterminism, changed.replayHistoryJson(summary_timer_history));
}

/// A dev-server run of `Fixtures.CancelMutexWaiter`, which cancels a coroutine blocked on a
/// `std.Io.Mutex`, exported as ProtoJSON from `Client.fetchHistory`.
const mutex_cancel_history = @embedFile("testdata/mutex_cancel_history.json");

test "WorkflowReplayer replays a workflow that cancels a coroutine blocked on a mutex and catches a changed one" {
    var ok: WorkflowReplayer = .init(testing.allocator, testing.io);
    defer ok.deinit();
    try ok.registerWorkflow(Fixtures.CancelMutexWaiter);
    try ok.replayHistoryJson(mutex_cancel_history);

    var changed: WorkflowReplayer = .init(testing.allocator, testing.io);
    defer changed.deinit();
    try changed.registerWorkflow(Fixtures.CancelMutexWaiterChanged);
    try testing.expectError(error.Nondeterminism, changed.replayHistoryJson(mutex_cancel_history));
}

test "ctx.sleep summaries are recorded on the timer started events" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(Fixtures.SummaryNaps);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var run = try env.startWorkflow(Fixtures.SummaryNaps, .{}, .{});
    defer run.deinit();
    // Replays the run too.
    try testing.expectEqualStrings("Canceled", try env.getResult(Fixtures.SummaryNaps, &run, a, null));

    const history = try env.client.fetchHistory(a, run.id, run.run_id);
    var summaries: std.ArrayList([]const u8) = .empty;
    for (history.events.items) |event| if (event.attributes.? == .timer_started_event_attributes) {
        const summary = if (event.user_metadata) |m| try converter.fromPayload([]const u8, a, m.summary.?) else "";
        try summaries.append(a, summary);
    };
    try testing.expectEqual(@as(usize, 3), summaries.items.len);
    try testing.expectEqualStrings("cool down", summaries.items[0]);
    // The short timer starts before the coroutine that starts the long one runs.
    try testing.expectEqualStrings("", summaries.items[1]);
    try testing.expectEqualStrings("give up", summaries.items[2]);
}

/// A dev-server run of `Fixtures.LocalGreetings`, whose second local activity waited on two
/// backoff timers, exported with `temporal workflow show`.
const local_activity_history = @embedFile("testdata/local_activity_history.json");

test "WorkflowReplayer replays local activities and their backoff timers" {
    var ok: WorkflowReplayer = .init(testing.allocator, testing.io);
    defer ok.deinit();
    try ok.registerWorkflow(Fixtures.LocalGreetings);
    try ok.replayHistoryJson(local_activity_history);

    var changed: WorkflowReplayer = .init(testing.allocator, testing.io);
    defer changed.deinit();
    try changed.registerWorkflow(Fixtures.LocalGreetingsChanged);
    try testing.expectError(error.Nondeterminism, changed.replayHistoryJson(local_activity_history));
}

test "sideEffect records its value once, on a worker with no activities, and replay reads it back" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(SideEffectFixtures.TakeTicket);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const before = SideEffectFixtures.issued;
    var run = try env.startWorkflow(SideEffectFixtures.TakeTicket, .{ .task_queue = "" }, .{100});
    defer run.deinit();
    // getResult also replays the run, which must not issue another ticket.
    const result = try env.getResult(SideEffectFixtures.TakeTicket, &run, a, null);
    try testing.expectEqual(before + 1, SideEffectFixtures.issued);
    try testing.expectEqualStrings(try std.fmt.allocPrint(a, "ticket {d}", .{100 + before + 1}), result);

    const history = try env.client.fetchHistory(a, run.id, run.run_id);
    var markers: usize = 0;
    for (history.events.items) |e| markers += @intFromBool(e.event_type == .EVENT_TYPE_MARKER_RECORDED);
    try testing.expectEqual(1, markers);
}

/// A dev-server run of `SideEffectFixtures.TakeTicket` with base 100, exported with
/// `temporal workflow show`.
const side_effect_history = @embedFile("testdata/side_effect_history.json");

test "WorkflowReplayer replays a side effect without calling its function and catches a dropped one" {
    var ok: WorkflowReplayer = .init(testing.allocator, testing.io);
    defer ok.deinit();
    try ok.registerWorkflow(SideEffectFixtures.TakeTicket);
    const before = SideEffectFixtures.issued;
    try ok.replayHistoryJson(side_effect_history);
    try testing.expectEqual(before, SideEffectFixtures.issued);

    var changed: WorkflowReplayer = .init(testing.allocator, testing.io);
    defer changed.deinit();
    try changed.registerWorkflow(SideEffectFixtures.TakeTicketChanged);
    try testing.expectError(error.Nondeterminism, changed.replayHistoryJson(side_effect_history));
}

test "WorkflowReplayer replays a workflow that fails through failWith after a failed cleanup activity" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(FailWithFixtures.Order);
    try env.registerActivity(FailWithFixtures.Charge);
    try env.registerActivity(FailWithFixtures.Release);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var run = try env.startWorkflow(FailWithFixtures.Order, .{ .task_queue = "" }, .{});
    defer run.deinit();
    var failure: ?failure_mod.Failure = null;
    // getResult replays the run too.
    try testing.expectError(error.WorkflowFailed, env.getResult(FailWithFixtures.Order, &run, a, &failure));
    try testing.expect(failure.?.failure_info.? == .activity_failure_info);
    try testing.expectEqualStrings("FailWithCharge", failure.?.failure_info.?.activity_failure_info.activity_type.?.name);
    try testing.expectEqualStrings("card declined", failure_mod.rootCause(&failure.?).message);

    const history = try env.client.fetchHistory(a, run.id, run.run_id);
    var ok: WorkflowReplayer = .init(testing.allocator, testing.io);
    defer ok.deinit();
    try ok.registerWorkflow(FailWithFixtures.Order);
    try ok.replayHistory(history);

    var changed: WorkflowReplayer = .init(testing.allocator, testing.io);
    defer changed.deinit();
    try changed.registerWorkflow(FailWithFixtures.OrderChanged);
    try testing.expectError(error.Nondeterminism, changed.replayHistory(history));
}

test "dev-server WorkflowEnvironment starts a workflow and awaits its result" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(Fixtures.Hello);
    try env.registerActivity(Fixtures.Greet);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const before = try env.now();
    var run = try env.startWorkflow(Fixtures.Hello, .{ .task_queue = "" }, .{"later"});
    defer run.deinit();
    try testing.expect(std.mem.startsWith(u8, run.id, "HelloWorld-test-"));
    try testing.expectEqualStrings("Hello later!", try env.getResult(Fixtures.Hello, &run, arena.allocator(), null));
    try env.sleep(.fromMilliseconds(1));
    try testing.expect(before.durationTo(try env.now()).nanoseconds > 0);
}

test "client: an eager start hands the first workflow task to the worker on the same client" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(Fixtures.Hello);
    try env.registerActivity(Fixtures.Greet);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    // startWorkflow starts the worker and waits until it is running, which is when it
    // registers with the client.
    var eager = try env.startWorkflow(Fixtures.Hello, .{ .task_queue = "", .enable_eager_start = true }, .{"eager"});
    defer eager.deinit();
    try testing.expect(eager.eagerly_started);
    try testing.expectEqualStrings("Hello eager!", try env.getResult(Fixtures.Hello, &eager, a, null));
    // The worker took the eager task: had it rejected it, the task would have timed out.
    const history = try env.client.fetchHistory(a, eager.id, eager.run_id);
    for (history.events.items) |event| try testing.expect(event.attributes.? != .workflow_task_timed_out_event_attributes);

    // Another client has no worker to take the task, so the workflow starts normally and the
    // worker polls for it.
    var other = try client_mod.Client.connect(testing.allocator, testing.io, env.server.clientOptions());
    defer other.close();
    var polled = try other.executeWorkflow(Fixtures.Hello, .{ .task_queue = env.task_queue, .enable_eager_start = true }, .{"polled"});
    defer polled.deinit();
    try testing.expect(!polled.eagerly_started);
    try testing.expectEqualStrings("Hello polled!", try env.getResult(Fixtures.Hello, &polled, a, null));
}

const StandaloneFixtures = struct {
    fn charge(ctx: *activity.Context, customer: []const u8, cents: u32) ![]const u8 {
        if (cents == 0) return ctx.fail(.{ .message = "nothing to charge", .type = "EmptyCharge", .non_retryable = true });
        return std.fmt.allocPrint(ctx.allocator(), "charged {s} {d}", .{ customer, cents });
    }
    const Charge = activity.Def("Charge", charge);

    /// Bounds one named wait of a test.
    const wait_limit: Io.Duration = .fromSeconds(30);
    /// Set by `stop`: the looping fixtures return, whatever happened to their cancel.
    var abandon: std.atomic.Value(bool) = .init(false);

    /// A named wait that gives up `wait_limit` after it began.
    const Wait = struct {
        deadline: Io.Clock.Timestamp,
        what: []const u8,

        fn begin(what: []const u8) Wait {
            return .{ .deadline = .fromNow(testing.io, .{ .raw = wait_limit, .clock = .awake }), .what = what };
        }

        /// Sleeps before the next try; once the limit has passed, logs `what` and returns
        /// `error.Timeout` instead.
        fn again(w: Wait) error{ Timeout, Canceled }!void {
            try testing.io.sleep(.fromMilliseconds(100), .awake);
            if (w.deadline.durationFromNow(testing.io).raw.nanoseconds > 0) return;
            log.err("timed out after {d} s waiting for {s}", .{ wait_limit.toSeconds(), w.what });
            return error.Timeout;
        }
    };

    /// Heartbeats until it is canceled.
    fn hold(ctx: *activity.Context) !void {
        while (!abandon.load(.acquire)) {
            ctx.heartbeat({});
            try ctx.io().sleep(.fromMilliseconds(50), .awake);
        }
        return error.TestAbandoned;
    }
    const Hold = activity.Def("Hold", hold);

    /// Waits until a worker has started the activity, for at most `wait_limit`.
    fn waitUntilStarted(handle: anytype, arena: Allocator) !void {
        const wait: Wait = .begin("the activity to start");
        while ((try handle.describe(arena)).last_started_time == null) try wait.again();
    }

    /// The first line of a test that runs a looping heartbeating fixture.
    fn reset() void {
        abandon.store(false, .release);
        PauseThenCancelFixtures.learned = .unset;
        PauseThenCancelFixtures.release = .unset;
        testing.log_level = .debug;
    }

    /// What such a test defers: tells the fixtures to end, so the worker can stop whatever
    /// happened to their cancel, then tears the environment down.
    fn stop(env: *WorkflowEnvironment) void {
        abandon.store(true, .release);
        PauseThenCancelFixtures.release.set(testing.io);
        env.deinit();
        testing.log_level = .warn;
    }

    fn waitForCount(c: *client_mod.Client, query: []const u8, want: u64) !void {
        var tries: usize = 0;
        while (try c.countActivities(query) != want) : (tries += 1) {
            if (tries == 300) {
                log.err("countActivities(\"{s}\") never reached {d}", .{ query, want });
                return error.TestUnexpectedResult;
            }
            try testing.io.sleep(.fromMilliseconds(100), .awake);
        }
    }
};

test "client: standalone activities run without a workflow; get, describe, cancel, terminate, list and count them" {
    StandaloneFixtures.reset();
    const env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{ .dev_server_extra_args = &DevServer.standalone_activity_args });
    defer StandaloneFixtures.stop(env);
    try env.registerActivity(StandaloneFixtures.Charge);
    try env.registerActivity(StandaloneFixtures.Hold);
    try env.ensureWorker();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const c = &env.client;

    var charged = try c.executeActivity(StandaloneFixtures.Charge, .{ .id = "charge-1", .task_queue = env.task_queue, .start_to_close_timeout = .fromSeconds(10) }, .{ "c-42", 1250 });
    defer charged.deinit();
    try testing.expect(charged.run_id.len > 0);
    try testing.expectEqualStrings("charged c-42 1250", try charged.get(a));
    const done = try charged.describe(a);
    try testing.expectEqual(client_mod.ActivityExecutionStatus.completed, done.status);
    try testing.expectEqualStrings("charge-1", done.activity_id);
    try testing.expectEqualStrings(charged.run_id, done.run_id);
    try testing.expectEqualStrings("Charge", done.activity_type);
    try testing.expectEqualStrings(env.task_queue, done.task_queue);
    try testing.expectEqual(@as(i32, 1), done.attempt);
    try testing.expect(done.schedule_time != null and done.close_time != null);
    // A handle made from the ID alone follows the latest run.
    var by_id = try c.getActivityHandle([]const u8, "charge-1", "");
    defer by_id.deinit();
    try testing.expectEqualStrings("charged c-42 1250", try by_id.get(a));

    var empty = try c.executeActivity(StandaloneFixtures.Charge, .{ .id = "charge-0", .task_queue = env.task_queue, .start_to_close_timeout = .fromSeconds(10) }, .{ "c-42", 0 });
    defer empty.deinit();
    try testing.expectError(error.ActivityFailed, empty.get(a));
    try testing.expectEqualStrings("nothing to charge", empty.failure().?.message);
    try testing.expectEqualStrings("EmptyCharge", failure_mod.applicationType(empty.failure().?.*).?);

    const hold: client_mod.StartActivityOptions = .{ .task_queue = env.task_queue, .start_to_close_timeout = .fromSeconds(60), .heartbeat_timeout = .fromSeconds(2), .id_conflict_policy = .fail };
    var canceled = try c.executeActivity(StandaloneFixtures.Hold, blk: {
        var o = hold;
        o.id = "hold-cancel";
        break :blk o;
    }, .{});
    defer canceled.deinit();
    errdefer canceled.terminate("test failed") catch {};
    try StandaloneFixtures.waitUntilStarted(&canceled, a);
    try canceled.cancel("no longer needed");
    _ = try StandaloneFollowUpFixtures.waitForStatus(&canceled, a, .canceled, "hold-cancel to end canceled");
    try testing.expectError(error.ActivityCanceled, canceled.get(a));
    try testing.expectEqual(client_mod.ActivityExecutionStatus.canceled, (try canceled.describe(a)).status);

    var terminated = try c.executeActivity(StandaloneFixtures.Hold, blk: {
        var o = hold;
        o.id = "hold-terminate";
        break :blk o;
    }, .{});
    defer terminated.deinit();
    errdefer terminated.terminate("test failed") catch {};
    try testing.expectError(error.ActivityExecutionAlreadyStarted, c.executeActivity(StandaloneFixtures.Hold, blk: {
        var o = hold;
        o.id = "hold-terminate";
        break :blk o;
    }, .{}));
    try StandaloneFixtures.waitUntilStarted(&terminated, a);
    try terminated.terminate("stopping it");
    try testing.expectError(error.ActivityTerminated, terminated.get(a));
    try testing.expectEqual(client_mod.ActivityExecutionStatus.terminated, (try terminated.describe(a)).status);
    try testing.expectError(error.ActivityClosed, terminated.cancel("too late"));
    var missing = try c.getActivityHandle(void, "no-such-activity", "");
    defer missing.deinit();
    try testing.expectError(error.NotFound, missing.describe(a));

    const query = try std.fmt.allocPrint(a, "TaskQueue = '{s}'", .{env.task_queue});
    try StandaloneFixtures.waitForCount(c, query, 4);
    const charges = try std.fmt.allocPrint(a, "{s} AND ActivityType = 'Charge' AND ExecutionStatus = 'Completed'", .{query});
    try testing.expectEqual(@as(u64, 1), try c.countActivities(charges));
    var it = try c.listActivities(query, .{ .page_size = 3 });
    defer it.deinit();
    var ids: std.ArrayList([]const u8) = .empty;
    while (try it.next(a)) |info| {
        try ids.append(a, info.activity_id);
        try testing.expectEqualStrings(env.task_queue, info.task_queue);
        if (std.mem.eql(u8, info.activity_id, "charge-1")) {
            try testing.expectEqual(client_mod.ActivityExecutionStatus.completed, info.status);
            try testing.expectEqualStrings("Charge", info.activity_type);
            try testing.expectEqualStrings(charged.run_id, info.run_id);
        }
    }
    std.mem.sort([]const u8, ids.items, {}, struct {
        fn lt(_: void, x: []const u8, y: []const u8) bool {
            return std.mem.lessThan(u8, x, y);
        }
    }.lt);
    try testing.expectEqualStrings("charge-0,charge-1,hold-cancel,hold-terminate", try std.mem.join(a, ",", ids.items));
}

const StandaloneFollowUpFixtures = struct {
    const Progress = struct { step: u32, note: []const u8 };

    /// Heartbeats its progress until it is canceled or paused.
    fn beat(ctx: *activity.Context) ![]const u8 {
        while (!StandaloneFixtures.abandon.load(.acquire)) {
            ctx.heartbeat(Progress{ .step = 3, .note = "halfway" });
            try ctx.io().sleep(.fromMilliseconds(50), .awake);
        }
        return error.TestAbandoned;
    }
    const Beat = activity.Def("Beat", beat);

    /// Waits up to `StandaloneFixtures.wait_limit` for the activity's status to be `want`;
    /// `what` names the wait in the log when it isn't.
    fn waitForStatus(handle: anytype, arena: Allocator, want: client_mod.ActivityExecutionStatus, what: []const u8) !client_mod.ActivityDescription {
        const wait: StandaloneFixtures.Wait = .begin(what);
        while (true) {
            const d = try handle.describe(arena);
            if (d.status == want) return d;
            wait.again() catch |err| {
                log.err("{s}: activity {s} stayed {t}, never {t}", .{ what, d.activity_id, d.status, want });
                return err;
            };
        }
    }
};

/// A standalone activity's value and failure, each stamped with the converter context it
/// was decoded with as "namespace|workflow ID|workflow type|activity type|activity ID".
const StandaloneContextFixtures = struct {
    fn contextText(arena: Allocator, c: *const converter.Context) error{OutOfMemory}![]const u8 {
        return std.fmt.allocPrint(arena, "{s}|{s}|{s}|{s}|{s}", .{ c.namespace, c.workflow_id, c.workflow_type, c.activity_type, c.activity_id });
    }

    /// Its encoding is plain text; decoding records the context it was decoded with.
    const Stamped = struct {
        text: []const u8,
        decoded_with: []const u8 = "",

        pub fn toPayloadWithContext(v: Stamped, arena: Allocator, _: *const converter.Context) error{OutOfMemory}!converter.Payload {
            return converter.toPayload(arena, v.text);
        }

        pub fn fromPayloadWithContext(arena: Allocator, context: *const converter.Context, p: converter.Payload) converter.DecodeError!Stamped {
            return .{ .text = try converter.fromPayload([]const u8, arena, p), .decoded_with = try contextText(arena, context) };
        }
    };

    fn echo(_: *activity.Context, v: Stamped) !Stamped {
        return .{ .text = v.text };
    }
    const Echo = activity.Def("StampedEcho", echo);

    fn refuse(_: *activity.Context) !void {
        return error.Refused;
    }
    const Refuse = activity.Def("StampedRefuse", refuse);

    /// Prefixes a received failure's message with "<ctx>"; sends failures as built.
    const converter_value: converter.FailureConverter = .{ .ptr = undefined, .vtable = &.{ .toFailure = toFailure, .fromFailure = fromFailure } };

    fn toFailure(_: *anyopaque, _: Allocator, _: *const converter.Context, _: ?anyerror, f: converter.Failure) error{OutOfMemory}!converter.Failure {
        return f;
    }

    fn fromFailure(_: *anyopaque, arena: Allocator, context: *const converter.Context, f: converter.Failure) error{OutOfMemory}!converter.Failure {
        var out = f;
        out.message = try std.fmt.allocPrint(arena, "<{s}> {s}", .{ try contextText(arena, context), f.message });
        return out;
    }
};

test "client: a standalone activity's handle decodes its result and failure with the activity type it was started with" {
    const env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{
        .dev_server_extra_args = &DevServer.standalone_activity_args,
        .failure_converter = StandaloneContextFixtures.converter_value,
    });
    defer env.deinit();
    try env.registerActivity(StandaloneContextFixtures.Echo);
    try env.registerActivity(StandaloneContextFixtures.Refuse);
    try env.ensureWorker();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const c = &env.client;

    var echoed = try c.executeActivity(StandaloneContextFixtures.Echo, .{ .id = "stamped-echo", .task_queue = env.task_queue, .start_to_close_timeout = .fromSeconds(10) }, .{.{ .text = "solo" }});
    defer echoed.deinit();
    const result = try echoed.get(a);
    try testing.expectEqualStrings("solo", result.text);
    try testing.expectEqualStrings("default|||StampedEcho|stamped-echo", result.decoded_with);

    // A handle from the ID alone doesn't know the type.
    var by_id = try c.getActivityHandle(StandaloneContextFixtures.Stamped, "stamped-echo", "");
    defer by_id.deinit();
    try testing.expectEqualStrings("default||||stamped-echo", (try by_id.get(a)).decoded_with);

    var refused = try c.executeActivityByName(void, "StampedRefuse", .{ .id = "stamped-refuse", .task_queue = env.task_queue, .start_to_close_timeout = .fromSeconds(10), .retry_policy = .{ .maximum_attempts = 1 } }, .{});
    defer refused.deinit();
    try testing.expectError(error.ActivityFailed, refused.get(a));
    try testing.expectEqualStrings("<default|||StampedRefuse|stamped-refuse> Refused", refused.failure().?.message);
}

test "client: a standalone activity starts with a delay, a priority and a summary, and reports whether it started" {
    StandaloneFixtures.reset();
    const zlib: converter.ZlibCodec = .{ .always_encode = true };
    const codecs = [_]converter.PayloadCodec{zlib.codec()};
    const env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{ .dev_server_extra_args = &DevServer.standalone_activity_args, .codecs = &codecs });
    defer StandaloneFixtures.stop(env);
    try env.registerActivity(StandaloneFixtures.Charge);
    try env.registerActivity(StandaloneFixtures.Hold);
    try env.ensureWorker();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const c = &env.client;

    var delayed = try c.executeActivity(StandaloneFixtures.Charge, .{
        .id = "delayed",
        .task_queue = env.task_queue,
        .start_to_close_timeout = .fromSeconds(10),
        .start_delay = .fromSeconds(2),
        .priority = .{ .priority_key = 2, .fairness_key = "tenant-a", .fairness_weight = 1.5 },
        .summary = "charge c-7 later",
    }, .{ "c-7", 300 });
    defer delayed.deinit();
    try testing.expect(delayed.started);
    const pending = try delayed.describe(a);
    try testing.expectEqual(client_mod.ActivityExecutionStatus.running, pending.status);
    try testing.expectEqual(Io.Duration.fromSeconds(2), pending.start_delay.?);
    try testing.expectEqual(@as(i32, 2), pending.priority.?.priority_key);
    try testing.expectEqualStrings("tenant-a", pending.priority.?.fairness_key);
    try testing.expectEqual(@as(f32, 1.5), pending.priority.?.fairness_weight);
    try testing.expectEqualStrings("charge c-7 later", pending.summary);
    try testing.expectEqualStrings("charged c-7 300", try delayed.get(a));
    // The server's own clock: the first attempt started no sooner than the delay after the
    // activity was scheduled.
    const done = try delayed.describe(a);
    const waited = done.schedule_time.?.durationTo(done.last_started_time.?);
    try testing.expect(waited.nanoseconds >= Io.Duration.fromSeconds(2).nanoseconds);

    var plain = try c.executeActivity(StandaloneFixtures.Charge, .{ .id = "undelayed", .task_queue = env.task_queue, .start_to_close_timeout = .fromSeconds(10) }, .{ "c-8", 1 });
    defer plain.deinit();
    _ = try plain.get(a);
    const plain_d = try plain.describe(a);
    try testing.expectEqual(@as(?Io.Duration, null), plain_d.start_delay);
    try testing.expect(plain_d.priority == null);
    try testing.expectEqualStrings("", plain_d.summary);
    // A zero delay and an all-default priority read back as none.
    var zeroed = try c.executeActivity(StandaloneFixtures.Charge, .{ .id = "zeroed", .task_queue = env.task_queue, .start_to_close_timeout = .fromSeconds(10), .start_delay = .fromMilliseconds(0), .priority = .{} }, .{ "c-8", 2 });
    defer zeroed.deinit();
    _ = try zeroed.get(a);
    const zeroed_d = try zeroed.describe(a);
    try testing.expectEqual(@as(?Io.Duration, null), zeroed_d.start_delay);
    try testing.expect(zeroed_d.priority == null);

    const hold: client_mod.StartActivityOptions = .{ .id = "attach", .task_queue = env.task_queue, .start_to_close_timeout = .fromSeconds(60), .heartbeat_timeout = .fromSeconds(2), .id_conflict_policy = .use_existing };
    var first = try c.executeActivity(StandaloneFixtures.Hold, hold, .{});
    defer first.deinit();
    errdefer first.terminate("test failed") catch {};
    try testing.expect(first.started);
    var again = try c.executeActivity(StandaloneFixtures.Hold, hold, .{});
    defer again.deinit();
    try testing.expect(!again.started);
    try testing.expectEqualStrings(first.run_id, again.run_id);
    var by_id = try c.getActivityHandle(void, "attach", "");
    defer by_id.deinit();
    try testing.expect(!by_id.started);
    try first.terminate("done");
}

test "client: a standalone activity can be paused, unpaused and deleted, and reports its heartbeat details" {
    StandaloneFixtures.reset();
    const zlib: converter.ZlibCodec = .{ .always_encode = true };
    const codecs = [_]converter.PayloadCodec{zlib.codec()};
    const env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{ .dev_server_extra_args = &DevServer.standalone_activity_args, .codecs = &codecs });
    defer StandaloneFixtures.stop(env);
    try env.registerActivity(StandaloneFollowUpFixtures.Beat);
    try env.registerActivity(StandaloneFixtures.Charge);
    try env.ensureWorker();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const c = &env.client;
    const Progress = StandaloneFollowUpFixtures.Progress;

    var beat = try c.executeActivity(StandaloneFollowUpFixtures.Beat, .{ .id = "beat", .task_queue = env.task_queue, .start_to_close_timeout = .fromSeconds(60), .heartbeat_timeout = .fromSeconds(2) }, .{});
    defer beat.deinit();
    errdefer beat.terminate("test failed") catch {};
    const beating = blk: {
        const wait: StandaloneFixtures.Wait = .begin("beat's first heartbeat details to show in describe");
        while (true) {
            const d = try beat.describe(a);
            if (d.heartbeat_details.len() > 0) break :blk d;
            try wait.again();
        }
    };
    const progress = try beating.heartbeat_details.get(Progress, a, 0);
    try testing.expectEqual(@as(u32, 3), progress.step);
    try testing.expectEqualStrings("halfway", progress.note);
    try testing.expect(beating.last_heartbeat_time != null);
    try testing.expectEqual(@as(?Progress, null), try beating.heartbeat_details.get(?Progress, a, 1));

    try beat.pause("maintenance");
    const paused = try StandaloneFollowUpFixtures.waitForStatus(&beat, a, .paused, "beat to be paused after the first pause");
    // The worker ended the running attempt as a pause, not a cancellation.
    const ended = blk: {
        const wait: StandaloneFixtures.Wait = .begin("the paused attempt's failure to show in describe");
        while (true) {
            const d = try beat.describe(a);
            if (d.raw.last_failure) |f| break :blk f;
            try wait.again();
        }
    };
    const ended_type = failure_mod.applicationType(ended) orelse {
        log.err("the paused attempt ended with a {t} failure, not an application failure: {s}", .{ ended.failure_info orelse return error.TestUnexpectedResult, ended.message });
        return error.TestUnexpectedResult;
    };
    try testing.expectEqualStrings("ActivityPause", ended_type);
    // Ending the paused attempt kept its last heartbeat's details for the next one.
    try testing.expectEqualStrings("halfway", (try (try beat.describe(a)).heartbeat_details.get(Progress, a, 0)).note);
    try testing.expectEqual(client_mod.ActivityExecutionStatus.paused, (try beat.describe(a)).status);
    try beat.unpause(.{ .reason = "maintenance over" });
    {
        const wait: StandaloneFixtures.Wait = .begin("beat to run a second attempt after the first unpause");
        while (true) {
            const d = try beat.describe(a);
            if (d.status == .running and d.attempt > 1 and d.last_started_time != null and
                d.last_started_time.?.nanoseconds > paused.last_started_time.?.nanoseconds) break;
            try wait.again();
        }
    }
    // Unpausing an activity that isn't paused does nothing.
    // The server refuses to unpause a running activity, or to pause one twice.
    try testing.expectError(error.RpcFailed, beat.unpause(.{ .jitter = .fromSeconds(1) }));
    try beat.pause("again");
    try testing.expectError(error.RpcFailed, beat.pause("again"));
    _ = try StandaloneFollowUpFixtures.waitForStatus(&beat, a, .paused, "beat to be paused after the second pause");
    try beat.unpause(.{ .reason = "jittered", .jitter = .fromMilliseconds(500) });
    _ = try StandaloneFollowUpFixtures.waitForStatus(&beat, a, .running, "beat to run again after the second unpause");

    try beat.delete();
    try testing.expectError(error.NotFound, beat.describe(a));

    var done = try c.executeActivity(StandaloneFixtures.Charge, .{ .id = "charged", .task_queue = env.task_queue, .start_to_close_timeout = .fromSeconds(10) }, .{ "c-9", 5 });
    defer done.deinit();
    _ = try done.get(a);
    try testing.expectError(error.ActivityClosed, done.pause("too late"));
    try testing.expectError(error.ActivityClosed, done.unpause(.{}));
    try done.delete();
    try testing.expectError(error.NotFound, done.describe(a));

    var missing = try c.getActivityHandle(void, "no-such-activity", "");
    defer missing.deinit();
    try testing.expectError(error.NotFound, missing.pause("nothing"));
    try testing.expectError(error.NotFound, missing.unpause(.{}));
    try testing.expectError(error.NotFound, missing.delete());
}

test "client: ActivityHandle.delete returns once the server has removed the record, for a running and a closed activity" {
    // The server removes a deleted activity's record in a transfer-queue task after the
    // DeleteActivityExecution call returns. Polling that queue once a second makes the task
    // run about 1 s later, so a describe right after a delete that doesn't wait for the
    // record to go reads it every time, not only when the task happens to be slow.
    const args = DevServer.standalone_activity_args ++ [_][]const u8{
        "--dynamic-config-value", "history.transferProcessorMaxPollRPS=1",
        "--dynamic-config-value", "history.transferProcessorMaxPollHostRPS=1",
    };
    const env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{ .dev_server_extra_args = &args });
    defer env.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const c = &env.client;

    // No worker polls the task queue, so the activity stays running until deleted.
    var running = try c.executeActivity(StandaloneFixtures.Charge, .{ .id = "delete-running", .task_queue = env.task_queue, .start_to_close_timeout = .fromSeconds(60) }, .{ "c-1", 1 });
    defer running.deinit();
    try running.delete();
    try testing.expectError(error.NotFound, running.describe(a));
    try testing.expectError(error.NotFound, running.get(a));

    var closed = try c.executeActivity(StandaloneFixtures.Charge, .{ .id = "delete-closed", .task_queue = env.task_queue, .start_to_close_timeout = .fromSeconds(60) }, .{ "c-1", 1 });
    defer closed.deinit();
    try closed.terminate("done");
    try closed.delete();
    try testing.expectError(error.NotFound, closed.describe(a));

    var started = try c.executeActivity(StandaloneFixtures.Charge, .{ .id = "delete-by-id", .task_queue = env.task_queue, .start_to_close_timeout = .fromSeconds(60) }, .{ "c-1", 1 });
    defer started.deinit();
    var by_id = try c.getActivityHandle(void, "delete-by-id", "");
    defer by_id.deinit();
    try by_id.delete();
    try testing.expectError(error.NotFound, by_id.describe(a));
}

const PauseThenCancelFixtures = struct {
    /// Set when the running attempt has learned of the pause.
    var learned: Io.Event = .unset;
    /// Set by the test once it has canceled the activity.
    var release: Io.Event = .unset;

    /// Heartbeats until canceled, then holds the attempt open until `release`, and ends
    /// it with the cancellation.
    fn hold(ctx: *activity.Context) !void {
        while (!StandaloneFixtures.abandon.load(.acquire)) {
            ctx.heartbeat({});
            ctx.io().sleep(.fromMilliseconds(50), .awake) catch |err| {
                learned.set(ctx.io());
                release.waitUncancelable(ctx.io());
                return err;
            };
        }
        return error.TestAbandoned;
    }
    const Hold = activity.Def("PauseThenCancelHold", hold);
};

test "client: cancelling a standalone activity while its pause reaches the running attempt cancels it" {
    StandaloneFixtures.reset();
    const env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{ .dev_server_extra_args = &DevServer.standalone_activity_args });
    defer StandaloneFixtures.stop(env);
    try env.registerActivity(PauseThenCancelFixtures.Hold);
    try env.ensureWorker();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const c = &env.client;

    var held = try c.executeActivity(PauseThenCancelFixtures.Hold, .{ .id = "pause-then-cancel", .task_queue = env.task_queue, .start_to_close_timeout = .fromSeconds(60), .heartbeat_timeout = .fromSeconds(5) }, .{});
    defer held.deinit();
    errdefer held.terminate("test failed") catch {};
    try StandaloneFixtures.waitUntilStarted(&held, a);
    // An operator pauses the activity, then cancels it before the running attempt it
    // paused has ended: the caller asked for a cancellation, so the activity must end
    // canceled, not as a failure of the attempt the pause ended.
    try held.pause("hold on");
    const learning: StandaloneFixtures.Wait = .begin("the running attempt to learn of the pause");
    while (!PauseThenCancelFixtures.learned.isSet()) try learning.again();
    // A failure from here on releases the attempt in `StandaloneFixtures.stop`.
    try held.cancel("no longer needed");
    PauseThenCancelFixtures.release.set(testing.io);
    _ = try StandaloneFollowUpFixtures.waitForStatus(&held, a, .canceled, "pause-then-cancel to end canceled");
    held.get(a) catch |err| {
        if (err != error.ActivityCanceled) {
            const message = if (held.failure()) |f| f.message else "";
            std.debug.print("activity ended {t}: {s}\n", .{ err, message });
        }
        try testing.expectEqual(error.ActivityCanceled, err);
        return;
    };
    return error.TestUnexpectedResult;
}

test "WorkflowEnvironment runs a child workflow on the same worker, or a test double in its place" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    {
        const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
        defer env.deinit();
        try env.registerWorkflow(ChildFixtures.Parent);
        try env.registerWorkflow(ChildFixtures.GreetChild);
        const result = try env.executeWorkflow(ChildFixtures.Parent, .{"Temporal"}, arena.allocator(), null);
        try testing.expectEqualStrings("Hello Temporal from a child!", result);
    }
    {
        const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
        defer env.deinit();
        try env.registerWorkflow(ChildFixtures.Parent);
        try env.registerWorkflow(workflow.Def(ChildFixtures.GreetChild.workflow_name, ChildFixtures.greetChildDouble));
        const result = try env.executeWorkflow(ChildFixtures.Parent, .{"Temporal"}, arena.allocator(), null);
        try testing.expectEqualStrings("Hello from the child's test double!", result);
    }
}

test "a failed child workflow surfaces error.ChildWorkflowFailed with its failure" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(ChildFixtures.ParentOfFailing);
    try env.registerWorkflow(ChildFixtures.FailChild);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const handled = try env.executeWorkflow(ChildFixtures.ParentOfFailing, .{true}, a, null);
    try testing.expectEqualStrings("ChildWorkflowFailed: Child Workflow execution failed (ChildGaveUp) caused \"the child gave up\"", handled);

    // Returned unhandled, the error fails the parent with the child's failure.
    var failure: ?failure_mod.Failure = null;
    try testing.expectError(error.WorkflowFailed, env.executeWorkflow(ChildFixtures.ParentOfFailing, .{false}, a, &failure));
    try testing.expect(failure.?.failure_info.? == .child_workflow_execution_failure_info);
    try testing.expectEqualStrings("the child gave up", failure_mod.rootCause(&failure.?).message);
}

test "time-skipping WorkflowEnvironment: a cron child is still pending when its parent returns" {
    const env = try WorkflowEnvironment.startTimeSkipping(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(ChildFixtures.CronParent);
    try env.registerWorkflow(ChildFixtures.CronChild);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    try testing.expect(try env.executeWorkflow(ChildFixtures.CronParent, .{}, arena.allocator(), null));
}

test "replay runs a workflow under its real workflow ID" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(ChildFixtures.ParentOfNamed);
    try env.registerWorkflow(ChildFixtures.GreetChild);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // getResult replays the run under its own ID.
    var run = try env.startWorkflow(ChildFixtures.ParentOfNamed, .{ .task_queue = "" }, .{"Temporal"});
    defer run.deinit();
    try testing.expectEqualStrings("Hello Temporal from a child!", try env.getResult(ChildFixtures.ParentOfNamed, &run, a, null));

    const history = try env.client.fetchHistory(a, run.id, run.run_id);
    var replayer: WorkflowReplayer = .init(testing.allocator, testing.io);
    defer replayer.deinit();
    try replayer.registerWorkflow(ChildFixtures.ParentOfNamed);
    replayer.workflow_id = run.id;
    try replayer.replayHistory(history);

    // Under another ID the derived child ID differs from the recorded one.
    replayer.workflow_id = "another-id";
    try testing.expectError(error.Nondeterminism, replayer.replayHistory(history));
}

test "workflow.Info: a child sees its parent, and a grandchild its parent and the root" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(LineageFixtures.Describe);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const result = try env.executeWorkflowWithOptions(LineageFixtures.Describe, .{ .id = "lineage", .task_queue = "" }, .{2}, a, null);
    try testing.expectEqualStrings("-/-;lineage/lineage;lineage-c/lineage", result);
}

test "signalExternalWorkflow: one workflow signals another by ID and waits for the delivery" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(ExternalFixtures.WaitForApproval);
    try env.registerWorkflow(ExternalFixtures.ApproveOther);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var receiver = try env.startWorkflow(ExternalFixtures.WaitForApproval, .{ .task_queue = "" }, .{});
    defer receiver.deinit();
    try testing.expectEqualStrings("delivered", try env.executeWorkflow(ExternalFixtures.ApproveOther, .{receiver.id}, a, null));
    try testing.expectEqualStrings("ann", try env.getResult(ExternalFixtures.WaitForApproval, &receiver, a, null));

    // The receiver has closed now, and a workflow that never ran isn't found either.
    for ([_][]const u8{ receiver.id, "no-such-workflow" }) |id| {
        const result = try env.executeWorkflow(ExternalFixtures.ApproveOther, .{id}, a, null);
        try testing.expect(std.mem.startsWith(u8, result, "ExternalWorkflowNotFound: Unable to signal external workflow because it was not found"));
        try testing.expect(std.mem.indexOf(u8, result, id) != null);
    }
}

test "signalExternalWorkflow: a parent signals its child, which signals the parent back through Info.parent" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(ExternalFixtures.PingPong);
    try env.registerWorkflow(ExternalFixtures.EchoChild);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    try testing.expectEqualStrings("ann via the child", try env.executeWorkflow(ExternalFixtures.PingPong, .{}, arena.allocator(), null));
}

/// A dev-server run of `ExternalFixtures.PingPong`, whose child ran on the same worker,
/// exported with `temporal workflow show`.
const signal_external_history = @embedFile("testdata/signal_external_history.json");

test "WorkflowReplayer replays signals to other workflows and catches a dropped signal" {
    var ok: WorkflowReplayer = .init(testing.allocator, testing.io);
    defer ok.deinit();
    try ok.registerWorkflow(ExternalFixtures.PingPong);
    try ok.replayHistoryJson(signal_external_history);

    var changed: WorkflowReplayer = .init(testing.allocator, testing.io);
    defer changed.deinit();
    try changed.registerWorkflow(ExternalFixtures.PingPongChanged);
    try testing.expectError(error.Nondeterminism, changed.replayHistoryJson(signal_external_history));
}

test "cancelExternalWorkflow: one workflow cancels another, which ends canceled" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(ExternalFixtures.WaitForApproval);
    try env.registerWorkflow(ExternalFixtures.CancelOther);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var target = try env.startWorkflow(ExternalFixtures.WaitForApproval, .{ .task_queue = "" }, .{});
    defer target.deinit();
    try testing.expectEqualStrings("requested", try env.executeWorkflow(ExternalFixtures.CancelOther, .{target.id}, a, null));
    try testing.expectError(error.WorkflowCanceled, env.getResult(ExternalFixtures.WaitForApproval, &target, a, null));

    const missing = try env.executeWorkflow(ExternalFixtures.CancelOther, .{"no-such-workflow"}, a, null);
    try testing.expect(std.mem.startsWith(u8, missing, "ExternalWorkflowNotFound: Unable to cancel external workflow because not found"));
}

/// A dev-server run of `ExternalFixtures.CancelOther` canceling a running
/// `ExternalFixtures.WaitForApproval`, exported with `temporal workflow show`.
const cancel_external_history = @embedFile("testdata/cancel_external_history.json");

test "nexus: a workflow schedules an operation through an endpoint, and it times out on a task queue nobody polls" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(NexusFixtures.CallEcho);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var suffix: [4]u8 = undefined;
    testing.io.random(&suffix);
    const endpoint = try std.fmt.allocPrint(a, "test-endpoint-{x}", .{suffix});
    const unpolled = try std.fmt.allocPrint(a, "{s}-unpolled", .{env.task_queue});
    try env.createNexusEndpoint(endpoint, .{ .task_queue = unpolled });
    try testing.expectError(error.AlreadyExists, env.createNexusEndpoint(endpoint, .{}));

    var run = try env.startWorkflow(NexusFixtures.CallEcho, .{}, .{endpoint});
    defer run.deinit();
    // Replays the run too.
    const result = try env.getResult(NexusFixtures.CallEcho, &run, a, null);
    try testing.expectEqualStrings("NexusOperationFailed: nexus operation completed unsuccessfully <- operation timed out (TIMEOUT_TYPE_SCHEDULE_TO_CLOSE)", result);

    const history = try env.client.fetchHistory(a, run.id, run.run_id);
    var scheduled_seen = false;
    var timed_out_seen = false;
    for (history.events.items) |event| switch (event.attributes.?) {
        .nexus_operation_scheduled_event_attributes => |attrs| {
            scheduled_seen = true;
            try testing.expectEqualStrings(endpoint, attrs.endpoint);
            try testing.expectEqualStrings("hello-service", attrs.service);
            try testing.expectEqualStrings("echo", attrs.operation);
            try testing.expectEqualStrings("{\"message\":\"hi\"}", attrs.input.?.data);
            try testing.expectEqual(@as(i64, 2), attrs.schedule_to_close_timeout.?.seconds);
            try testing.expectEqual(@as(usize, 1), attrs.nexus_header.items.len);
            try testing.expectEqualStrings("trace-id", attrs.nexus_header.items[0].key);
            try testing.expectEqualStrings("abc", attrs.nexus_header.items[0].value);
            try StartOptionFixtures.expectMetadata(a, event, "echo it", "");
        },
        .nexus_operation_timed_out_event_attributes => timed_out_seen = true,
        else => {},
    };
    try testing.expect(scheduled_seen and timed_out_seen);
}

/// A dev-server run of `NexusFixtures.CallEcho` whose operation timed out on a task queue
/// nobody polled, from the test above, exported as JSON.
const nexus_history = @embedFile("testdata/nexus_history.json");

test "WorkflowReplayer replays a Nexus operation and catches a workflow that schedules another" {
    var ok: WorkflowReplayer = .init(testing.allocator, testing.io);
    defer ok.deinit();
    try ok.registerWorkflow(NexusFixtures.CallEcho);
    try ok.replayHistoryJson(nexus_history);

    var changed: WorkflowReplayer = .init(testing.allocator, testing.io);
    defer changed.deinit();
    try changed.registerWorkflow(NexusFixtures.CallEchoChanged);
    try testing.expectError(error.Nondeterminism, changed.replayHistoryJson(nexus_history));
}

test "WorkflowReplayer replays a cancellation request to another workflow and catches a dropped one" {
    var ok: WorkflowReplayer = .init(testing.allocator, testing.io);
    defer ok.deinit();
    try ok.registerWorkflow(ExternalFixtures.CancelOther);
    try ok.replayHistoryJson(cancel_external_history);

    var changed: WorkflowReplayer = .init(testing.allocator, testing.io);
    defer changed.deinit();
    try changed.registerWorkflow(ExternalFixtures.CancelOtherChanged);
    try testing.expectError(error.Nondeterminism, changed.replayHistoryJson(cancel_external_history));
}

/// A dev-server run of `ChildFixtures.Parent`, whose child ran on the same worker, exported
/// with `temporal workflow show`.
const child_workflow_history = @embedFile("testdata/child_workflow_history.json");

test "WorkflowReplayer replays a parent's child workflow and catches a changed child type" {
    var ok: WorkflowReplayer = .init(testing.allocator, testing.io);
    defer ok.deinit();
    try ok.registerWorkflow(ChildFixtures.Parent);
    try ok.replayHistoryJson(child_workflow_history);

    var changed: WorkflowReplayer = .init(testing.allocator, testing.io);
    defer changed.deinit();
    try changed.registerWorkflow(ChildFixtures.ParentChanged);
    try testing.expectError(error.Nondeterminism, changed.replayHistoryJson(child_workflow_history));
}

test "a child workflow's static summary and details are recorded on its started event" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(ChildFixtures.DescribedParent);
    try env.registerWorkflow(ChildFixtures.GreetChild);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var run = try env.startWorkflow(ChildFixtures.DescribedParent, .{}, .{"Ada"});
    defer run.deinit();
    // Replays the run too.
    try testing.expectEqualStrings("Hello Ada from a child!", try env.getResult(ChildFixtures.DescribedParent, &run, a, null));

    const event = try StartOptionFixtures.startedEvent(&env.client, a, "described-child", "");
    try StartOptionFixtures.expectMetadata(a, event, "Greet the guest", "Greets **one** guest by name.");
    try testing.expectEqualStrings(run.id, event.attributes.?.workflow_execution_started_event_attributes.parent_workflow_execution.?.workflow_id);
}

/// A dev-server run of `ChildFixtures.DescribedParent`, whose child was started with a static
/// summary and details, exported as JSON.
const described_child_history = @embedFile("testdata/described_child_history.json");

test "WorkflowReplayer replays a child started with a static summary and details, with or without them" {
    var ok: WorkflowReplayer = .init(testing.allocator, testing.io);
    defer ok.deinit();
    try ok.registerWorkflow(ChildFixtures.DescribedParent);
    try ok.replayHistoryJson(described_child_history);

    var plain: WorkflowReplayer = .init(testing.allocator, testing.io);
    defer plain.deinit();
    try plain.registerWorkflow(ChildFixtures.DescribedParentPlain);
    try plain.replayHistoryJson(described_child_history);
}

const RetryDelayFixtures = struct {
    /// Fails its first attempt as an expected, benign failure that asks for a 2 s retry delay.
    fn pollReady(ctx: *activity.Context) ![]const u8 {
        if (ctx.info().attempt == 1) return ctx.fail(.{
            .message = "not ready yet",
            .type = "NotReady",
            .category = .benign,
            .next_retry_delay = .fromSeconds(2),
        });
        return "ready";
    }
    const PollReady = activity.Def("PollReady", pollReady);

    fn waitReady(ctx: *workflow.Context) ![]const u8 {
        return ctx.executeActivity(PollReady, .{
            .start_to_close_timeout = .fromSeconds(10),
            .retry_policy = .{ .initial_interval = .fromMilliseconds(100), .backoff_coefficient = 1 },
        }, .{}).get();
    }
    const WaitReady = workflow.Def("WaitReady", waitReady);

    fn nanos(t: ?proto.google.protobuf.Timestamp) i96 {
        const v = t orelse return 0;
        return @as(i96, v.seconds) * std.time.ns_per_s + v.nanos;
    }
};

test "a benign failure's next retry delay overrides the retry policy, and its category reaches history" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(RetryDelayFixtures.WaitReady);
    try env.registerActivity(RetryDelayFixtures.PollReady);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var run = try env.startWorkflow(RetryDelayFixtures.WaitReady, .{ .task_queue = "" }, .{});
    defer run.deinit();
    try testing.expectEqualStrings("ready", try env.getResult(RetryDelayFixtures.WaitReady, &run, a, null));

    const history = try env.client.fetchHistory(a, run.id, run.run_id);
    var scheduled_at: ?i96 = null;
    var started: ?proto.temporal.api.history.v1.HistoryEvent = null;
    for (history.events.items) |e| switch (e.attributes orelse continue) {
        .activity_task_scheduled_event_attributes => scheduled_at = RetryDelayFixtures.nanos(e.event_time),
        .activity_task_started_event_attributes => started = e,
        else => {},
    };
    const attempt = started.?.attributes.?.activity_task_started_event_attributes;
    try testing.expectEqual(@as(i32, 2), attempt.attempt);
    // The second attempt waited for the failure's 2 s delay, not the policy's 100 ms.
    try testing.expect(RetryDelayFixtures.nanos(started.?.event_time) - scheduled_at.? >= 2 * std.time.ns_per_s);
    const last = attempt.last_failure.?;
    try testing.expectEqual(failure_mod.Application.Category.benign, failure_mod.applicationCategory(last).?);
    try testing.expectEqual(@as(i64, 2), last.failure_info.?.application_failure_info.next_retry_delay.?.seconds);
}

const AsyncFixtures = struct {
    /// Set once `awaitApproval` has handed off its task token and activity ID.
    var started: Io.Event = .unset;
    var token_buf: [1024]u8 = undefined;
    var token: []const u8 = "";
    var activity_id_buf: [64]u8 = undefined;
    var activity_id: []const u8 = "";

    fn reset() void {
        started = .unset;
        token = "";
        activity_id = "";
    }

    fn keep(buf: []u8, value: []const u8) error{ValueTooLong}![]const u8 {
        if (value.len > buf.len) return error.ValueTooLong;
        @memcpy(buf[0..value.len], value);
        return buf[0..value.len];
    }

    /// Hands its task token to the test and leaves the activity running for the client to
    /// complete.
    fn awaitApproval(ctx: *activity.Context) ![]const u8 {
        token = try keep(&token_buf, ctx.info().task_token);
        activity_id = try keep(&activity_id_buf, ctx.info().activity_id);
        started.set(ctx.io());
        return ctx.completeAsync();
    }
    const AwaitApproval = activity.Def("AwaitApproval", awaitApproval);

    /// Returns the approval, "failed: <type> <message>" when it fails, or ends canceled when
    /// the run is canceled (after the activity reports its cancellation).
    fn approve(ctx: *workflow.Context) ![]const u8 {
        const approval = ctx.executeActivity(AwaitApproval, .{
            .start_to_close_timeout = .fromSeconds(60),
            .retry_policy = .{ .maximum_attempts = 1 },
            .cancellation_type = .wait_cancellation_completed,
        }, .{});
        return approval.get() catch |err| switch (err) {
            error.ActivityFailed => {
                const root = failure_mod.rootCause(approval.failure().?);
                if (root.failure_info != null and root.failure_info.? == .canceled_failure_info) return error.Canceled;
                return std.fmt.allocPrint(ctx.allocator(), "failed: {s} {s}", .{ failure_mod.applicationType(root.*) orelse "", root.message });
            },
            else => err,
        };
    }
    const Approve = workflow.Def("Approve", approve);

    fn approveLocally(ctx: *workflow.Context) ![]const u8 {
        const approval = ctx.executeLocalActivity(AwaitApproval, .{ .start_to_close_timeout = .fromSeconds(60) }, .{});
        return approval.get() catch |err| switch (err) {
            error.ActivityFailed => failure_mod.applicationType(failure_mod.rootCause(approval.failure().?).*) orelse "no type",
            else => err,
        };
    }
    const ApproveLocally = workflow.Def("ApproveLocally", approveLocally);

    fn start() !*WorkflowEnvironment {
        reset();
        const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
        errdefer env.deinit();
        try env.registerWorkflow(Approve);
        try env.registerWorkflow(ApproveLocally);
        try env.registerActivity(AwaitApproval);
        return env;
    }
};

test "an activity that completes async stays started on the server; a local one fails" {
    const env = try AsyncFixtures.start();
    defer env.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var run = try env.startWorkflow(AsyncFixtures.Approve, .{ .task_queue = "" }, .{});
    defer run.deinit();
    try AsyncFixtures.started.wait(testing.io);
    try testing.expect(AsyncFixtures.token.len > 0);
    const description = try env.client.describeWorkflow(a, run.id, run.run_id);
    const pending = description.raw.pending_activities.items;
    try testing.expectEqual(1, pending.len);
    try testing.expectEqual(.PENDING_ACTIVITY_STATE_STARTED, pending[0].state);
    try env.client.terminateWorkflow(run.id, run.run_id, "test cleanup");

    try testing.expectEqualStrings("LocalActivityCompleteAsync", try env.executeWorkflow(AsyncFixtures.ApproveLocally, .{}, a, null));
}

test "client: complete an async activity by task token or by ID; completing it again is NotFound" {
    const env = try AsyncFixtures.start();
    defer env.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var by_token = try env.startWorkflow(AsyncFixtures.Approve, .{ .task_queue = "" }, .{});
    defer by_token.deinit();
    try AsyncFixtures.started.wait(testing.io);
    try env.client.completeActivity(AsyncFixtures.token, @as([]const u8, "approved by token"));
    try testing.expectEqualStrings("approved by token", try env.getResult(AsyncFixtures.Approve, &by_token, a, null));
    try testing.expectError(error.NotFound, env.client.completeActivity(AsyncFixtures.token, @as([]const u8, "again")));

    AsyncFixtures.reset();
    var by_id = try env.startWorkflow(AsyncFixtures.Approve, .{ .task_queue = "" }, .{});
    defer by_id.deinit();
    try AsyncFixtures.started.wait(testing.io);
    const id: client_mod.ActivityId = .{ .workflow_id = by_id.id, .run_id = by_id.run_id, .activity_id = AsyncFixtures.activity_id };
    try env.client.completeActivityById(id, @as([]const u8, "approved by ID"));
    try testing.expectEqualStrings("approved by ID", try env.getResult(AsyncFixtures.Approve, &by_id, a, null));
    try testing.expectError(error.NotFound, env.client.completeActivityById(id, @as([]const u8, "again")));
}

test "client: fail an async activity by ID or by task token" {
    const env = try AsyncFixtures.start();
    defer env.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var by_id = try env.startWorkflow(AsyncFixtures.Approve, .{ .task_queue = "" }, .{});
    defer by_id.deinit();
    try AsyncFixtures.started.wait(testing.io);
    const id: client_mod.ActivityId = .{ .workflow_id = by_id.id, .activity_id = AsyncFixtures.activity_id };
    try env.client.failActivityById(id, .{ .message = "over budget", .type = "Declined" });
    try testing.expectEqualStrings("failed: Declined over budget", try env.getResult(AsyncFixtures.Approve, &by_id, a, null));
    try testing.expectError(error.NotFound, env.client.failActivityById(id, .{ .message = "again" }));

    AsyncFixtures.reset();
    var by_token = try env.startWorkflow(AsyncFixtures.Approve, .{ .task_queue = "" }, .{});
    defer by_token.deinit();
    try AsyncFixtures.started.wait(testing.io);
    try env.client.failActivity(AsyncFixtures.token, .{ .message = "missing receipt", .type = "Declined", .non_retryable = true });
    try testing.expectEqualStrings("failed: Declined missing receipt", try env.getResult(AsyncFixtures.Approve, &by_token, a, null));
}

test "client: heartbeating an async activity reports its cancellation once the workflow cancels it" {
    const env = try AsyncFixtures.start();
    defer env.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var run = try env.startWorkflow(AsyncFixtures.Approve, .{ .task_queue = "" }, .{});
    defer run.deinit();
    try AsyncFixtures.started.wait(testing.io);
    const token = AsyncFixtures.token;
    const id: client_mod.ActivityId = .{ .workflow_id = run.id, .run_id = run.run_id, .activity_id = AsyncFixtures.activity_id };
    try testing.expect(!try env.client.recordActivityHeartbeat(token, @as(u32, 1)));
    try testing.expect(!try env.client.recordActivityHeartbeatById(id, {}));

    // Canceling the run cancels the activity, and the workflow waits for it to report.
    try env.client.cancelWorkflow(run.id, run.run_id, "testing async activity cancellation");
    while (!try env.client.recordActivityHeartbeat(token, @as(u32, 2))) try testing.io.sleep(.fromMilliseconds(20), .awake);
    try testing.expect(try env.client.recordActivityHeartbeatById(id, {}));
    try env.client.reportActivityCancellationById(id, @as([]const u8, "stopped at step 2"));
    try testing.expectError(error.WorkflowCanceled, env.getResult(AsyncFixtures.Approve, &run, a, null));
    try testing.expectError(error.NotFound, env.client.reportActivityCancellation(token, {}));
    try testing.expectError(error.NotFound, env.client.recordActivityHeartbeat(token, {}));
}

const DynamicFixtures = struct {
    /// Runs any workflow type: reads a name and a count, calls the activity "<type>.greet" by
    /// name with them, and names the workflow type in its result.
    fn anyWorkflow(ctx: *workflow.Context, args: converter.EncodedValues) ![]const u8 {
        const a = ctx.allocator();
        const name = try args.get([]const u8, a, 0);
        const count = try args.get(u32, a, 1);
        const activity_type = try std.fmt.allocPrint(a, "{s}.greet", .{ctx.info().workflow_type});
        const greeting = try ctx.executeActivityByName([]const u8, activity_type, .{ .start_to_close_timeout = .fromSeconds(10) }, .{ name, count }).get();
        return std.fmt.allocPrint(a, "workflow {s} ran {s}", .{ ctx.info().workflow_type, greeting });
    }
    const AnyWorkflow = workflow.DynamicDef(anyWorkflow);

    fn anyWorkflowWithoutActivity(ctx: *workflow.Context, args: converter.EncodedValues) ![]const u8 {
        _ = args;
        return ctx.info().workflow_type;
    }
    const AnyWorkflowChanged = workflow.DynamicDef(anyWorkflowWithoutActivity);

    const Greeter = struct {
        punctuation: []const u8,
        /// Runs any activity type with a name and a count, and names the type it ran as.
        fn anyActivity(g: *Greeter, ctx: *activity.Context, args: converter.EncodedValues) ![]const u8 {
            const name, const count = try args.decode(struct { []const u8, u32 }, ctx.allocator());
            return std.fmt.allocPrint(ctx.allocator(), "activity {s} greeting {s} x{d}{s}", .{ ctx.info().activity_type, name, count, g.punctuation });
        }
    };
    const AnyActivity = activity.DynamicDef(Greeter.anyActivity);

    fn plainActivity(ctx: *activity.Context, args: converter.EncodedValues) !usize {
        _ = ctx;
        return args.len();
    }
    const PlainActivity = activity.DynamicDef(plainActivity);
};

test "a dynamic workflow started by name reads its arguments and calls a dynamic activity by name" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    var greeter: DynamicFixtures.Greeter = .{ .punctuation = "!" };
    try env.registerDynamicWorkflow(DynamicFixtures.AnyWorkflow);
    try env.registerDynamicActivityWith(DynamicFixtures.AnyActivity, &greeter);
    try testing.expectError(error.AlreadyRegistered, env.registerDynamicWorkflow(DynamicFixtures.AnyWorkflowChanged));
    try testing.expectError(error.AlreadyRegistered, env.registerDynamicActivity(DynamicFixtures.PlainActivity));
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // Started (and replayed) through the environment, which starts its worker.
    try testing.expectEqualStrings(
        "workflow Onboarding ran activity Onboarding.greet greeting ann x2!",
        try env.executeWorkflowByName([]const u8, "Onboarding", .{ .task_queue = "" }, .{ "ann", @as(u32, 2) }, a, null),
    );
    // Any other type runs the same code; once the worker runs, the client can start it too.
    var run = try env.client.executeWorkflowByName([]const u8, "Offboarding", .{ .id = "offboarding", .task_queue = env.task_queue }, .{ "bob", @as(u32, 1) });
    defer run.deinit();
    try testing.expectEqualStrings("workflow Offboarding ran activity Offboarding.greet greeting bob x1!", try env.getResultByName([]const u8, &run, a, null));

    // An argument of the wrong type is an error the workflow returns, failing it.
    var failure: ?failure_mod.Failure = null;
    try testing.expectError(error.WorkflowFailed, env.executeWorkflowByName([]const u8, "Onboarding", .{ .task_queue = "" }, .{ "ann", "two" }, a, &failure));
    try testing.expectEqualStrings("InvalidPayload", failure.?.message);
}

test "registered workflows and activities win over the dynamic ones" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(Fixtures.Hello);
    try env.registerActivity(Fixtures.Greet);
    try env.registerDynamicWorkflow(DynamicFixtures.AnyWorkflowChanged);
    try env.registerDynamicActivity(DynamicFixtures.PlainActivity);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    try testing.expectEqualStrings("Hello Temporal!", try env.executeWorkflow(Fixtures.Hello, .{"Temporal"}, a, null));
    try testing.expectEqualStrings("Other", try env.executeWorkflowByName([]const u8, "Other", .{ .task_queue = "" }, .{}, a, null));
}

const dynamic_history = @embedFile("testdata/dynamic_history.json");

test "WorkflowReplayer replays a dynamic workflow and catches a changed one" {
    var ok: WorkflowReplayer = .init(testing.allocator, testing.io);
    defer ok.deinit();
    try ok.registerDynamicWorkflow(DynamicFixtures.AnyWorkflow);
    try testing.expectError(error.AlreadyRegistered, ok.registerDynamicWorkflow(DynamicFixtures.AnyWorkflow));
    try ok.replayHistoryJson(dynamic_history);

    var changed: WorkflowReplayer = .init(testing.allocator, testing.io);
    defer changed.deinit();
    try changed.registerDynamicWorkflow(DynamicFixtures.AnyWorkflowChanged);
    try testing.expectError(error.Nondeterminism, changed.replayHistoryJson(dynamic_history));
}

const RuntimeSignalFixtures = struct {
    /// Holds a lock until a release signal "unlock-event-<its workflow ID>" arrives, named at
    /// run time, and returns who released it.
    fn holdLock(ctx: *workflow.Context) ![]const u8 {
        const Lock = struct {
            releaser: ?[]const u8 = null,
            fn release(l: *@This(), releaser: []const u8) void {
                l.releaser = releaser;
            }
            fn released(l: *@This()) bool {
                return l.releaser != null;
            }
        };
        const lock = try ctx.allocator().create(Lock);
        lock.* = .{};
        const name = try std.fmt.allocPrint(ctx.allocator(), "unlock-event-{s}", .{ctx.info().workflow_id});
        try ctx.setSignalHandlerByName(name, struct { []const u8 }, lock, Lock.release);
        try ctx.await(lock, Lock.released);
        return std.fmt.allocPrint(ctx.allocator(), "released by {s}", .{lock.releaser.?});
    }
    const HoldLock = workflow.Def("HoldLock", holdLock);

    /// Releases the lock `holder_id` holds with a signal named after it.
    fn releaseLock(ctx: *workflow.Context, holder_id: []const u8) !void {
        const name = try std.fmt.allocPrint(ctx.allocator(), "unlock-event-{s}", .{holder_id});
        try ctx.signalExternalWorkflowByName(name, holder_id, "", .{"releaser"}).get();
    }
    const ReleaseLock = workflow.Def("ReleaseLock", releaseLock);

    const UnlockEarly = workflow.SignalDef("unlock-event-early-holder", fn (releaser: []const u8) void);

    const Inbox = struct {
        ctx: *workflow.Context,
        entries: std.ArrayList([]const u8) = .empty,

        fn approve(s: *Inbox, approver: []const u8) !void {
            try s.entries.append(s.ctx.allocator(), try std.fmt.allocPrint(s.ctx.allocator(), "approved by {s}", .{approver}));
        }
        fn other(s: *Inbox, name: []const u8, args: converter.EncodedValues) !void {
            const value = try args.get([]const u8, s.ctx.allocator(), 0);
            try s.entries.append(s.ctx.allocator(), try std.fmt.allocPrint(s.ctx.allocator(), "{s}={s}", .{ name, value }));
        }
        fn full(s: *Inbox) bool {
            return s.entries.items.len >= 3;
        }
    };
    /// Handles "approve" with its own handler and every other signal with the dynamic one,
    /// until three have arrived.
    fn inbox(ctx: *workflow.Context) ![]const u8 {
        const s = try ctx.allocator().create(Inbox);
        s.* = .{ .ctx = ctx };
        try ctx.setSignalHandler(SignalFixtures.Approve, s, Inbox.approve);
        try ctx.setDynamicSignalHandler(s, Inbox.other);
        try ctx.await(s, Inbox.full);
        return std.mem.join(ctx.allocator(), ", ", s.entries.items);
    }
    const InboxWf = workflow.Def("Inbox", inbox);
    const Color = workflow.SignalDef("color", fn (value: []const u8) void);
    const Size = workflow.SignalDef("size", fn (value: []const u8) void);
};

test "signals named at run time: an early one waits for its handler, and a workflow sends one" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(RuntimeSignalFixtures.HoldLock);
    try env.registerWorkflow(RuntimeSignalFixtures.ReleaseLock);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // Signal-with-start delivers the signal with the start, before the handler registers.
    var early = try env.signalWithStartWorkflow(RuntimeSignalFixtures.HoldLock, .{ .id = "early-holder", .task_queue = "" }, .{}, RuntimeSignalFixtures.UnlockEarly, .{"starter"});
    defer early.deinit();
    try testing.expectEqualStrings("released by starter", try env.getResult(RuntimeSignalFixtures.HoldLock, &early, a, null));

    var holder = try env.startWorkflow(RuntimeSignalFixtures.HoldLock, .{ .id = "holder", .task_queue = "" }, .{});
    defer holder.deinit();
    try env.executeWorkflow(RuntimeSignalFixtures.ReleaseLock, .{"holder"}, a, null);
    try testing.expectEqualStrings("released by releaser", try env.getResult(RuntimeSignalFixtures.HoldLock, &holder, a, null));
}

test "a dynamic signal handler receives the signals no handler is registered for" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(RuntimeSignalFixtures.InboxWf);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var run = try env.startWorkflow(RuntimeSignalFixtures.InboxWf, .{ .task_queue = "" }, .{});
    defer run.deinit();
    // Signals that arrive before the handlers register start in registration order, ones
    // that arrive after in arrival order; sending "approve" first makes both orders agree.
    try env.signalWorkflow(SignalFixtures.Approve, run.id, .{"ann"});
    try env.signalWorkflow(RuntimeSignalFixtures.Color, run.id, .{"red"});
    try env.signalWorkflow(RuntimeSignalFixtures.Size, run.id, .{"large"});
    try testing.expectEqualStrings("approved by ann, color=red, size=large", try env.getResult(RuntimeSignalFixtures.InboxWf, &run, a, null));
}

test "client: signals sent by name reach a typed handler and the dynamic handler" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(RuntimeSignalFixtures.InboxWf);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var run = try env.startWorkflow(RuntimeSignalFixtures.InboxWf, .{ .task_queue = "" }, .{});
    defer run.deinit();
    // "approve" first, so registration order and arrival order agree (see the test above).
    try env.client.signalWorkflowByName("approve", run.id, run.run_id, .{"ann"});
    try env.client.signalWorkflowByNameWithOptions("color", run.id, "", .{"red"}, .{});
    const size: []const u8 = "large";
    try env.client.signalWorkflowByName("size", run.id, "", .{size});
    try testing.expectEqualStrings("approved by ann, color=red, size=large", try env.getResult(RuntimeSignalFixtures.InboxWf, &run, a, null));

    try testing.expectError(error.NotFound, env.client.signalWorkflowByName("color", run.id, run.run_id, .{"late"}));
    try testing.expectError(error.NotFound, env.client.signalWorkflowByName("color", "no-such-workflow", "", .{"nobody"}));
}

test "client: updates sent by name reach a typed handler and the dynamic handler; a rejected one is UpdateFailed" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(UpdateFixtures.HandleDynamicUpdates);
    try env.registerActivity(Fixtures.Greet);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var run = try env.startWorkflow(UpdateFixtures.HandleDynamicUpdates, .{ .task_queue = "" }, .{});
    defer run.deinit();

    // "set" has a handler of its own, with a validator.
    var set = try env.client.updateWorkflowByName(i64, "set", run.id, run.run_id, .{@as(i64, 5)}, .{});
    defer set.deinit();
    try testing.expectEqual(@as(i64, 0), try set.get(a));
    try testing.expectEqualStrings(run.run_id, set.run_id);

    var rejected = try env.client.updateWorkflowByName(i64, "set", run.id, "", .{@as(i64, -1)}, .{});
    defer rejected.deinit();
    try testing.expectError(error.UpdateFailed, rejected.get(a));
    try testing.expectEqualStrings("NotPositive", rejected.failure().?.message);

    // "greet" goes to the dynamic handler; accepted returns once it started.
    var greet = try env.client.updateWorkflowByName([]const u8, "greet", run.id, run.run_id, .{"ann"}, .{ .update_id = "greet-ann", .wait_for_stage = .accepted });
    defer greet.deinit();
    try testing.expectEqualStrings("greet-ann", greet.update_id);
    try testing.expectEqualStrings("Hello ann!", try greet.get(a));

    // The dynamic handler's validator rejects every other name.
    var unknown = try env.client.updateWorkflowByName(void, "unknown", run.id, run.run_id, .{"x"}, .{});
    defer unknown.deinit();
    try testing.expectError(error.UpdateFailed, unknown.get(a));
    try testing.expectEqualStrings("UnknownUpdate", unknown.failure().?.message);

    try env.client.signalWorkflowByName("done", run.id, run.run_id, .{});
    try testing.expectEqualStrings("5 Hello ann!", try env.getResult(UpdateFixtures.HandleDynamicUpdates, &run, a, null));
    try testing.expectError(error.NotFound, env.client.updateWorkflowByName(i64, "set", "no-such-workflow", "", .{@as(i64, 1)}, .{}));
}

test "client: signal-with-start and update-with-start by name start a workflow and deliver to typed and dynamic handlers" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(SignalFixtures.AwaitApprovals);
    try env.registerWorkflow(RuntimeSignalFixtures.InboxWf);
    try env.registerWorkflow(UpdateFixtures.HandleDynamicUpdates);
    try env.registerWorkflow(Fixtures.Hello);
    try env.registerActivity(Fixtures.Greet);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    _ = try env.executeWorkflow(Fixtures.Hello, .{"warm-up"}, a, null); // starts the worker

    // A typed signal handler; the second call only signals the running workflow.
    const approvals: client_mod.StartWorkflowOptions = .{ .id = "signal-with-start-by-name", .task_queue = env.task_queue };
    var run = try env.client.signalWithStartWorkflowByName([]const u8, "AwaitApprovals", approvals, .{@as(u32, 2)}, "approve", .{"ann"});
    defer run.deinit();
    try testing.expectEqualStrings("signal-with-start-by-name", run.id);
    var again = try env.client.signalWithStartWorkflowByName([]const u8, "AwaitApprovals", approvals, .{@as(u32, 99)}, "approve", .{"bob"});
    defer again.deinit();
    try testing.expectEqualStrings(run.run_id, again.run_id);
    try testing.expectEqualStrings("ann bob", try run.get(a));

    // The dynamic signal handler, with a workflow definition's name and a signal named at run time.
    var inbox = try env.client.signalWithStartWorkflowByName([]const u8, RuntimeSignalFixtures.InboxWf.workflow_name, .{ .task_queue = env.task_queue }, .{}, "color", .{"red"});
    defer inbox.deinit();
    try testing.expectEqual(@as(usize, 36), inbox.id.len);
    try env.client.signalWorkflowByName("approve", inbox.id, "", .{"ann"});
    try env.client.signalWorkflowByName("size", inbox.id, "", .{"large"});
    const entries = try inbox.get(a);
    for ([_][]const u8{ "color=red", "approved by ann", "size=large" }) |entry| {
        if (std.mem.indexOf(u8, entries, entry) == null) {
            std.debug.print("inbox result {s} lacks {s}\n", .{ entries, entry });
            return error.TestUnexpectedResult;
        }
    }

    // Update-with-start: the typed "set" handler starts the workflow, then the dynamic
    // handler takes "greet" on the running one.
    var options: client_mod.StartWorkflowOptions = .{ .id = "update-with-start-by-name", .task_queue = env.task_queue };
    try testing.expectError(error.InvalidOptions, env.client.updateWithStartWorkflowByName(i64, "HandleDynamicUpdates", options, .{}, "set", .{@as(i64, 3)}, .{}));
    options.id_conflict_policy = .use_existing;
    var first = try env.client.updateWithStartWorkflowByName(i64, "HandleDynamicUpdates", options, .{}, "set", .{@as(i64, 3)}, .{});
    defer first.deinit();
    try testing.expectEqual(@as(i64, 0), try first.get(a));
    try testing.expect(first.run_id.len > 0);
    var greeted = try env.client.updateWithStartWorkflowByName([]const u8, "HandleDynamicUpdates", options, .{}, "greet", .{"cy"}, .{ .wait_for_stage = .accepted });
    defer greeted.deinit();
    try testing.expectEqualStrings("Hello cy!", try greeted.get(a));
    try testing.expectEqualStrings(first.run_id, greeted.run_id);

    // A rejected update-with-start surfaces as the typed call's does.
    var rejected = try env.client.updateWithStartWorkflowByName(i64, "HandleDynamicUpdates", options, .{}, "set", .{@as(i64, -1)}, .{});
    defer rejected.deinit();
    try testing.expectError(error.UpdateFailed, rejected.get(a));
    try testing.expectEqualStrings("NotPositive", rejected.failure().?.message);

    var failing = options;
    failing.id_conflict_policy = .fail;
    try testing.expectError(error.WorkflowExecutionAlreadyStarted, env.client.updateWithStartWorkflowByName(i64, "HandleDynamicUpdates", failing, .{}, "set", .{@as(i64, 9)}, .{}));

    try env.client.signalWorkflowByName("done", options.id, "", .{});
    var updated = try env.client.getWorkflow([]const u8, options.id, first.run_id);
    defer updated.deinit();
    try testing.expectEqualStrings("3 Hello cy!", try updated.get(a));
}

/// Replaces each payload with one of encoding "test/reversed" holding the original,
/// serialized and reversed, so no payload text reaches the server in the clear; decoding
/// undoes "test/reversed" payloads and passes others through.
const ReversingCodec = struct {
    const encoding = "test/reversed";
    encoded: std.atomic.Value(u32) = .init(0),

    fn codec(r: *ReversingCodec) converter.PayloadCodec {
        return .{ .ptr = r, .vtable = &.{ .encode = encode, .decode = decode } };
    }

    fn encode(ptr: *anyopaque, arena: Allocator, _: Io, payloads: []const converter.Payload) converter.CodecError![]converter.Payload {
        const r: *ReversingCodec = @ptrCast(@alignCast(ptr));
        _ = r.encoded.fetchAdd(@intCast(payloads.len), .monotonic);
        const out = try arena.alloc(converter.Payload, payloads.len);
        for (payloads, out) |p, *o| {
            var serialized: Io.Writer.Allocating = .init(arena);
            p.encode(&serialized.writer, arena) catch return error.OutOfMemory;
            std.mem.reverse(u8, serialized.written());
            o.* = try converter.payloadWithEncoding(arena, encoding, serialized.written());
        }
        return out;
    }

    fn decode(_: *anyopaque, arena: Allocator, _: Io, payloads: []const converter.Payload) converter.CodecError![]converter.Payload {
        const out = try arena.dupe(converter.Payload, payloads);
        for (out) |*p| {
            if (!std.mem.eql(u8, converter.encodingOf(p.*) orelse "", encoding)) continue;
            const data = try arena.dupe(u8, p.data);
            std.mem.reverse(u8, data);
            var reader: Io.Reader = .fixed(data);
            p.* = converter.Payload.decode(&reader, arena) catch return error.CodecFailed;
        }
        return out;
    }
};

/// A codec whose decode fails with `error.CodecFailed` on a payload of encoding
/// `binary/poison` and passes every other payload through, as does its encode.
const PoisonCodec = struct {
    const encoding = "binary/poison";
    const vtable: converter.PayloadCodec.VTable = .{ .encode = pass, .decode = decode };

    fn codec() converter.PayloadCodec {
        return .{ .ptr = @ptrCast(@constCast(&vtable)), .vtable = &vtable };
    }

    fn pass(_: *anyopaque, _: Allocator, _: Io, payloads: []const converter.Payload) converter.CodecError![]converter.Payload {
        return @constCast(payloads);
    }

    fn decode(_: *anyopaque, _: Allocator, _: Io, payloads: []const converter.Payload) converter.CodecError![]converter.Payload {
        for (payloads) |p| if (std.mem.eql(u8, converter.encodingOf(p) orelse "", encoding)) return error.CodecFailed;
        return @constCast(payloads);
    }
};

const PoisonFixtures = struct {
    /// Set once getResult has described the run, which lets `Hold` return.
    var release: Io.Event = .unset;

    /// Heartbeats a payload the codec can't decode, which only the run's description holds
    /// (no history event or activation carries a successful attempt's heartbeat details),
    /// then waits for `release`.
    fn hold(ctx: *activity.Context) ![]const u8 {
        ctx.heartbeat(try converter.payloadWithEncoding(ctx.allocator(), PoisonCodec.encoding, "written by another codec"));
        try release.wait(ctx.io());
        return "done";
    }
    const Hold = activity.Def("PoisonHold", hold);

    fn holding(ctx: *workflow.Context) ![]const u8 {
        return ctx.executeActivity(Hold, .{ .start_to_close_timeout = .fromSeconds(60) }, .{}).get();
    }
    const Holding = workflow.Def("PoisonHolding", holding);

    /// Sets `release` once a DescribeWorkflowExecution call returns after `armed` is set.
    const ReleaseOnDescribe = struct {
        armed: std.atomic.Value(bool) = .init(false),

        pub fn call(_: *ReleaseOnDescribe, input: *interceptor.ClientCall, next: interceptor.Client.Next) interceptor.ClientError!void {
            defer if (input.request(proto.temporal.api.workflowservice.v1.DescribeWorkflowExecutionRequest) != null and release_on_describe.armed.load(.acquire)) release.set(testing.io);
            return next.call(input);
        }
    };
    var release_on_describe: ReleaseOnDescribe = .{};

    /// Waits until `Hold`'s heartbeat reaches the server: until then the run's description
    /// decodes, and after it the public describe fails with `error.CodecFailed`, as it must.
    fn waitForPoisonedDescription(env: *WorkflowEnvironment, id: []const u8) !void {
        for (0..500) |_| {
            var scratch: std.heap.ArenaAllocator = .init(testing.allocator);
            defer scratch.deinit();
            _ = env.client.describeWorkflow(scratch.allocator(), id, "") catch |err| switch (err) {
                error.CodecFailed => return,
                else => |e| return e,
            };
            try testing.io.sleep(.fromMilliseconds(20), .awake);
        }
        return error.HeartbeatNeverDescribed;
    }
};

test "WorkflowEnvironment.getResult pins a handle with no run ID when the run's description holds a payload the codecs can't decode" {
    testing.log_level = .err; // describeWorkflow's codec failure is logged as a warning.
    PoisonFixtures.release.reset();
    PoisonFixtures.release_on_describe.armed.store(false, .release);
    const codecs = [_]converter.PayloadCodec{PoisonCodec.codec()};
    const interceptors = [_]interceptor.Client{interceptor.Client.from(PoisonFixtures.ReleaseOnDescribe, &PoisonFixtures.release_on_describe)};
    const env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{ .codecs = &codecs, .interceptors = &interceptors });
    defer env.deinit();
    // Whatever happens, the activity must not keep the worker from stopping.
    defer PoisonFixtures.release.set(testing.io);
    try env.registerWorkflow(PoisonFixtures.Holding);
    try env.registerActivity(PoisonFixtures.Hold);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var started = try env.startWorkflow(PoisonFixtures.Holding, .{ .task_queue = "" }, .{});
    defer started.deinit();
    try PoisonFixtures.waitForPoisonedDescription(env, started.id);

    // The pin's describe is the first after arming, and the activity returns once it is done.
    PoisonFixtures.release_on_describe.armed.store(true, .release);
    var latest = try env.client.getWorkflow([]const u8, started.id, "");
    defer latest.deinit();
    try testing.expectEqualStrings("done", try env.getResult(PoisonFixtures.Holding, &latest, a, null));
    try testing.expectEqualStrings(started.run_id, latest.run_id);
}

test "WorkflowEnvironment.getResultByNameWithOptions pins a handle with no run ID without decoding the run's description" {
    testing.log_level = .err; // describeWorkflow's codec failure is logged as a warning.
    PoisonFixtures.release.reset();
    PoisonFixtures.release_on_describe.armed.store(false, .release);
    const codecs = [_]converter.PayloadCodec{PoisonCodec.codec()};
    const interceptors = [_]interceptor.Client{interceptor.Client.from(PoisonFixtures.ReleaseOnDescribe, &PoisonFixtures.release_on_describe)};
    const env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{ .codecs = &codecs, .interceptors = &interceptors });
    defer env.deinit();
    defer PoisonFixtures.release.set(testing.io);
    try env.registerWorkflow(PoisonFixtures.Holding);
    try env.registerActivity(PoisonFixtures.Hold);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var started = try env.startWorkflowByName([]const u8, "PoisonHolding", .{ .task_queue = "" }, .{});
    defer started.deinit();
    try PoisonFixtures.waitForPoisonedDescription(env, started.id);

    PoisonFixtures.release_on_describe.armed.store(true, .release);
    var latest = try env.client.getWorkflow([]const u8, started.id, "");
    defer latest.deinit();
    try testing.expectEqualStrings("done", try env.getResultByNameWithOptions([]const u8, &latest, .{ .follow_runs = false }, a, null));
    try testing.expectEqualStrings(started.run_id, latest.run_id);
    // The run is closed and its activity gone, so the description decodes again.
    _ = try env.client.describeWorkflow(a, started.id, "");
}

const CodecFixtures = struct {
    const Note = workflow.SignalDef("note", fn (text: []const u8) void);
    const Notes = workflow.QueryDef("notes", fn () []const u8);
    const Shout = workflow.UpdateDef("shout", fn (text: []const u8) []const u8);

    const State = struct {
        ctx: *workflow.Context,
        notes: std.ArrayList([]const u8) = .empty,
        shouted: ?[]const u8 = null,

        fn note(s: *State, text: []const u8) void {
            s.notes.append(s.ctx.allocator(), text) catch {};
        }
        fn list(s: *State) []const u8 {
            return std.mem.join(s.ctx.allocator(), ",", s.notes.items) catch "";
        }
        fn shout(s: *State, text: []const u8) ![]const u8 {
            s.shouted = try std.ascii.allocUpperString(s.ctx.allocator(), text);
            return s.shouted.?;
        }
        fn ready(s: *State) bool {
            return s.notes.items.len > 0 and s.shouted != null;
        }
    };

    fn refuse(ctx: *activity.Context, text: []const u8) ![]const u8 {
        const details = try ctx.allocator().alloc(converter.Payload, 1);
        details[0] = try converter.toPayload(ctx.allocator(), text);
        return ctx.fail(.{ .message = "refused", .type = "Refused", .non_retryable = true, .details = details });
    }
    const Refuse = activity.Def("CodecRefuse", refuse);

    fn child(ctx: *workflow.Context, text: []const u8) ![]const u8 {
        return std.fmt.allocPrint(ctx.allocator(), "child got {s}", .{text});
    }
    const Child = workflow.Def("CodecChild", child);

    /// Runs an activity, a child, and a failing activity, waits for a signal and an update,
    /// then fails with every value it saw (the memo's included) as the failure's details.
    fn coded(ctx: *workflow.Context, input: []const u8) ![]const u8 {
        const s = try ctx.allocator().create(State);
        s.* = .{ .ctx = ctx };
        try ctx.setSignalHandler(Note, s, State.note);
        try ctx.setQueryHandler(Notes, s, State.list);
        try ctx.setUpdateHandler(Shout, s, State.shout);
        const options: workflow.ActivityOptions = .{ .start_to_close_timeout = .fromSeconds(10) };
        const greeting = try ctx.executeActivity(Fixtures.Greet, options, .{input}).get();
        const from_child = try ctx.executeChildWorkflow(Child, .{}, .{greeting}).get();
        const refused = ctx.executeActivity(Refuse, options, .{input});
        _ = refused.get() catch {};
        const info = (failure_mod.rootCause(refused.failure() orelse return error.ActivityDidNotFail).failure_info) orelse return error.NoFailureInfo;
        const details = info.application_failure_info.details orelse return error.NoFailureDetails;
        const detail = try converter.fromPayload([]const u8, ctx.allocator(), details.payloads.items[0]);
        try ctx.await(s, State.ready);
        try ctx.await(ctx, workflow.Context.allHandlersFinished);
        const memo = (try ctx.memoValue([]const u8, "owner")) orelse "no memo";
        const summary = try std.mem.join(ctx.allocator(), "|", &.{ greeting, from_child, detail, s.notes.items[0], s.shouted.?, memo });
        const out = try ctx.allocator().alloc(converter.Payload, 1);
        out[0] = try converter.toPayload(ctx.allocator(), summary);
        return ctx.fail(.{ .message = "finished with a summary", .type = "Summary", .non_retryable = true, .details = out });
    }
    const Coded = workflow.Def("Coded", coded);
};

test "codecs encode every payload the client and worker send and decode what they receive" {
    var reversing: ReversingCodec = .{};
    const zlib: converter.ZlibCodec = .{};
    const codecs = [_]converter.PayloadCodec{ reversing.codec(), zlib.codec() };
    const env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{ .codecs = &codecs });
    defer env.deinit();
    try env.registerWorkflow(CodecFixtures.Coded);
    try env.registerWorkflow(CodecFixtures.Child);
    try env.registerActivity(Fixtures.Greet);
    try env.registerActivity(CodecFixtures.Refuse);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var run = try env.startWorkflow(CodecFixtures.Coded, .{
        .task_queue = "",
        .memo = try converter.toFields(a, .{ .owner = "secret-memo" }),
    }, .{"secret-input"});
    defer run.deinit();
    try env.signalWorkflow(CodecFixtures.Note, run.id, .{"secret-note"});
    var shout = try env.client.updateWorkflow(CodecFixtures.Shout, run.id, run.run_id, .{"secret-update"}, .{});
    defer shout.deinit();
    try testing.expectEqualStrings("SECRET-UPDATE", try shout.get(a));
    try testing.expectEqualStrings("secret-note", try env.queryWorkflow(CodecFixtures.Notes, a, run.id, .{}));

    // Waiting for the result also replays the (decoded) history with the codecs.
    var failure: ?failure_mod.Failure = null;
    try testing.expectError(error.WorkflowFailed, env.getResult(CodecFixtures.Coded, &run, a, &failure));
    const details = failure.?.failure_info.?.application_failure_info.details.?.payloads.items;
    try testing.expectEqualStrings(
        "Hello secret-input!|child got Hello secret-input!|secret-input|secret-note|SECRET-UPDATE|secret-memo",
        try converter.fromPayload([]const u8, a, details[0]),
    );
    const description = try env.client.describeWorkflow(a, run.id, run.run_id);
    try testing.expectEqualStrings("secret-memo", (try description.memoValue([]const u8, a, "owner")).?);
    try testing.expect(reversing.encoded.load(.monotonic) > 0);

    // A client without codecs sees what the server stored: no payload in the clear.
    var plain = try client_mod.Client.connect(testing.allocator, testing.io, env.server.clientOptions());
    defer plain.close();
    const raw = try plain.fetchHistory(a, run.id, run.run_id);
    var raw_bytes: Io.Writer.Allocating = .init(a);
    try raw.encode(&raw_bytes.writer, a);
    try testing.expect(std.mem.indexOf(u8, raw_bytes.written(), "secret") == null);
    try testing.expect(std.mem.indexOf(u8, raw_bytes.written(), ReversingCodec.encoding) != null);
    try testing.expectEqualStrings(ReversingCodec.encoding, converter.encodingOf(raw.events.items[0].attributes.?.workflow_execution_started_event_attributes.input.?.payloads.items[0]).?);

    // The encoded history replays with the codecs.
    var replayer: WorkflowReplayer = .initWithOptions(testing.allocator, testing.io, .{ .namespace = env.client.namespace, .task_queue = env.task_queue, .codecs = &codecs });
    defer replayer.deinit();
    replayer.workflow_id = run.id;
    try replayer.registerWorkflow(CodecFixtures.Coded);
    try replayer.replayHistory(raw);
}

const TimeSkippingFixtures = struct {
    fn dayLater(ctx: *workflow.Context, name: []const u8) ![]const u8 {
        try ctx.io().sleep(.fromSeconds(std.time.s_per_day), .awake);
        return std.fmt.allocPrint(ctx.allocator(), "a day later, {s}", .{name});
    }
    const DayLater = workflow.Def("DayLater", dayLater);
};

test "time-skipping WorkflowEnvironment with options: a codec and a worker interceptor see a day-long workflow and its replay" {
    var reversing: ReversingCodec = .{};
    const codecs = [_]converter.PayloadCodec{reversing.codec()};
    var watch: RunWatch = .{ .gpa = testing.allocator, .io = testing.io };
    defer watch.deinit();
    const interceptors = [_]interceptor.Worker{interceptor.Worker.from(RunWatch, &watch)};
    const env = try WorkflowEnvironment.startTimeSkippingWithOptions(testing.allocator, testing.io, .{ .codecs = &codecs, .worker_interceptors = &interceptors });
    defer env.deinit();
    try testing.expect(env.server == .time_skipping);
    try env.registerWorkflow(TimeSkippingFixtures.DayLater);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const wall_start = Io.Timestamp.now(testing.io, .awake);
    const server_start = try env.now();
    var run = try env.startWorkflow(TimeSkippingFixtures.DayLater, .{ .task_queue = "" }, .{"secret-name"});
    defer run.deinit();
    try testing.expectEqualStrings("a day later, secret-name", try env.getResult(TimeSkippingFixtures.DayLater, &run, a, null));
    try testing.expect(server_start.durationTo(try env.now()).nanoseconds >= std.time.ns_per_day);
    try testing.expect(wall_start.durationTo(Io.Timestamp.now(testing.io, .awake)).nanoseconds < 60 * std.time.ns_per_s);

    // The interceptor ran once on the worker and once in the replay `getResult` ran.
    try testing.expectEqual(@as(usize, 1), watch.count(false));
    try testing.expectEqual(@as(usize, 1), watch.count(true));
    try testing.expectEqualStrings(run.run_id, watch.lastReplayed().?);

    // The codec encoded the workflow's input and result: the server stored neither in the clear.
    try testing.expect(reversing.encoded.load(.monotonic) >= 2);
    var plain = try client_mod.Client.connect(testing.allocator, testing.io, env.server.clientOptions());
    defer plain.close();
    const raw = try plain.fetchHistory(a, run.id, run.run_id);
    var raw_bytes: Io.Writer.Allocating = .init(a);
    try raw.encode(&raw_bytes.writer, a);
    try testing.expect(std.mem.indexOf(u8, raw_bytes.written(), "secret") == null);
    const input = raw.events.items[0].attributes.?.workflow_execution_started_event_attributes.input.?.payloads.items[0];
    try testing.expectEqualStrings(ReversingCodec.encoding, converter.encodingOf(input).?);

    // The replay decoded that history with the codec: without it, the same history can't
    // replay, because the workflow can't read its encoded input.
    var uncoded: WorkflowReplayer = .initWithOptions(testing.allocator, testing.io, .{ .namespace = env.client.namespace, .task_queue = env.task_queue });
    defer uncoded.deinit();
    uncoded.workflow_id = run.id;
    try uncoded.registerWorkflow(TimeSkippingFixtures.DayLater);
    testing.log_level = .err; // The failed replay is logged as a warning.
    try testing.expectError(error.Nondeterminism, uncoded.replayHistory(raw));
}

test "WorkflowEnvironment marks itself unusable when getResult cannot re-lock time skipping" {
    const env = try WorkflowEnvironment.startTimeSkipping(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(TimeSkippingFixtures.DayLater);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // A working environment first, so the failure below is the hook's.
    try testing.expectEqualStrings("a day later, before", try env.executeWorkflow(TimeSkippingFixtures.DayLater, .{"before"}, a, null));

    test_relock_failures.store(1, .monotonic);
    defer test_relock_failures.store(0, .monotonic);
    // The lost lock is logged as an error, which would fail the test: capture it.
    var logged: Io.Writer.Allocating = .init(testing.allocator);
    defer logged.deinit();
    test_time_lock_log = &logged.writer;
    defer test_time_lock_log = null;
    var why: ?failure_mod.Failure = null;
    try testing.expectError(error.TimeLockLost, env.executeWorkflow(TimeSkippingFixtures.DayLater, .{"lost"}, a, &why));
    try testing.expectEqual(@as(u32, 0), test_relock_failures.load(.monotonic));
    try testing.expect(env.time_lock_lost);
    try testing.expect(why == null);
    try testing.expect(std.mem.indexOf(u8, logged.written(), "LockTimeSkipping failed (RpcFailed)") != null);

    // Every time-dependent call now fails before reaching the server, though the hook is
    // spent and the server would answer.
    try testing.expectError(error.TimeLockLost, env.startWorkflow(TimeSkippingFixtures.DayLater, .{ .task_queue = "" }, .{"after"}));
    try testing.expectError(error.TimeLockLost, env.executeWorkflow(TimeSkippingFixtures.DayLater, .{"after"}, a, null));
    try testing.expectError(error.TimeLockLost, env.now());
    try testing.expectError(error.TimeLockLost, env.sleep(.fromSeconds(1)));
    const Poke = workflow.SignalDef("poke", fn () void);
    try testing.expectError(error.TimeLockLost, env.signalWorkflow(Poke, "any-id", .{}));
    var run = try env.client.getWorkflow([]const u8, "any-id", "");
    defer run.deinit();
    try testing.expectError(error.TimeLockLost, env.getResultByName([]const u8, &run, a, null));
}

const CanceledRelockFixtures = struct {
    var started: Io.Event = .unset;
    var release: Io.Event = .unset;

    /// Sleeps a day, which passes only while time skipping is unlocked, then runs `Hold`.
    fn dayThenHold(ctx: *workflow.Context) !void {
        try ctx.io().sleep(.fromSeconds(std.time.s_per_day), .awake);
        try ctx.executeLocalActivity(Hold, .{ .start_to_close_timeout = .fromSeconds(600) }, .{}).get();
    }
    const DayThenHold = workflow.Def("DayThenHold", dayThenHold);

    /// Sets `started`, then waits for `release`. The workflow task stays open while it runs,
    /// which keeps the server from skipping time to the run's timeout, so the run stays open.
    fn hold(ctx: *activity.Context) !void {
        started.set(ctx.io());
        release.wait(ctx.io()) catch {};
    }
    const Hold = activity.Def("Hold", hold);

    const Seen = struct {
        result: anyerror!void = {},
        after: Io.Cancelable!void = {},
    };

    /// Waits for `run` through `getResult` and records what it returned and what the next
    /// cancellation point returned after it.
    fn wait(env: *WorkflowEnvironment, run: *client_mod.WorkflowRun(void), arena: Allocator) Seen {
        var seen: Seen = .{};
        seen.result = env.getResult(DayThenHold, run, arena, null);
        seen.after = testing.io.checkCancel();
        return seen;
    }
};

test "WorkflowEnvironment.getResult re-arms a cancel it consumed when it loses the time lock" {
    const env = try WorkflowEnvironment.startTimeSkipping(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(CanceledRelockFixtures.DayThenHold);
    try env.registerActivity(CanceledRelockFixtures.Hold);
    CanceledRelockFixtures.started = .unset;
    CanceledRelockFixtures.release = .unset;
    // Before the worker stops, so the activity ends.
    defer CanceledRelockFixtures.release.set(testing.io);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();

    var run = try env.startWorkflow(CanceledRelockFixtures.DayThenHold, .{ .task_queue = "" }, .{});
    defer run.deinit();

    test_relock_failures.store(1, .monotonic);
    defer test_relock_failures.store(0, .monotonic);
    // The lost lock is logged as an error, which would fail the test: capture it.
    var logged: Io.Writer.Allocating = .init(testing.allocator);
    defer logged.deinit();
    test_time_lock_log = &logged.writer;
    defer test_time_lock_log = null;

    var waiting = try testing.io.concurrent(CanceledRelockFixtures.wait, .{ env, &run, arena.allocator() });
    // The day passes only once getResult has unlocked time skipping, so the activity starting
    // means getResult is in its wait, which the open run keeps going until the cancel.
    // Bounded, so a getResult that ends before its wait fails the test rather than hanging it.
    const began = CanceledRelockFixtures.started.waitTimeout(testing.io, .{ .duration = .{ .raw = .fromSeconds(60), .clock = .awake } });
    const seen = waiting.cancel(testing.io);
    try began;

    try testing.expectError(error.TimeLockLost, seen.result);
    try testing.expect(env.time_lock_lost);
    try testing.expectEqual(@as(u32, 0), test_relock_failures.load(.monotonic));
    try testing.expect(std.mem.indexOf(u8, logged.written(), "LockTimeSkipping failed (RpcFailed)") != null);
    // The wait consumed the cancel and TimeLockLost replaced it, so it was re-armed.
    try testing.expectError(error.Canceled, seen.after);
}

const CloseTimeFixtures = struct {
    const timeout_s = 5 * 365 * std.time.s_per_day;
    const Go = workflow.SignalDef("go", fn () void);

    const State = struct {
        go: bool = false,

        fn onGo(st: *State) void {
            st.go = true;
        }
        fn received(st: *State) bool {
            return st.go;
        }
    };

    /// Waits up to five years for a signal, as samples/await-signals waits for its signals,
    /// then sleeps an hour.
    fn signaledThenHour(ctx: *workflow.Context) !void {
        const st = try ctx.allocator().create(State);
        st.* = .{};
        try ctx.setSignalHandler(Go, st, State.onGo);
        if (!try ctx.awaitTimeout(.fromSeconds(timeout_s), st, State.received)) return error.NoSignal;
        try ctx.io().sleep(.fromSeconds(std.time.s_per_hour), .awake);
    }
    const SignaledThenHour = workflow.Def("SignaledThenHour", signaledThenHour);
};

test "time-skipping env.now() after getResult is no earlier than the run's close time" {
    const env = try WorkflowEnvironment.startTimeSkipping(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(CloseTimeFixtures.SignaledThenHour);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // One run, no other run and no cron schedule. The signal arrives while time is locked,
    // so the run's five-year await timeout is canceled before any time skips; the run then
    // sleeps an hour and closes. The server still holds the canceled timer, and once the run
    // closes nothing holds its time lock until `getResult` takes it back, so it skips to that
    // timer in between.
    const start = try env.now();
    var run = try env.startWorkflow(CloseTimeFixtures.SignaledThenHour, .{ .task_queue = "" }, .{});
    defer run.deinit();
    try env.signalWorkflow(CloseTimeFixtures.Go, run.id, .{});
    try env.getResult(CloseTimeFixtures.SignaledThenHour, &run, a, null);
    const after = try env.now();

    // The documented alternative: the run's close time is an hour after its start.
    const described = try env.client.describeWorkflow(a, run.id, run.run_id);
    const close = described.close_time.?;
    const to_close = start.durationTo(close).nanoseconds;
    try testing.expect(to_close >= std.time.ns_per_hour);
    try testing.expect(to_close < std.time.ns_per_hour + std.time.ns_per_min);
    // The documented limit: env.now() is no earlier than the close, and may be much later.
    // When it is (as on nearly every run with test server v1.41.1), the server skipped to a
    // pending timer at least five years past the start: the closed run's own canceled await
    // timeout, the only timer that far ahead and nearer than the server's default run timeout.
    const gap = close.durationTo(after).nanoseconds;
    try testing.expect(gap >= 0);
    if (gap > std.time.ns_per_min) {
        try testing.expect(start.durationTo(after).nanoseconds >= CloseTimeFixtures.timeout_s * std.time.ns_per_s);
    }
}

test "startTimeSkippingWithOptions refuses the dev server's extra arguments" {
    // The refusal is logged as a warning.
    testing.log_level = .err;
    try testing.expectError(error.InvalidOptions, WorkflowEnvironment.startTimeSkippingWithOptions(testing.allocator, testing.io, .{ .dev_server_extra_args = &DevServer.standalone_activity_args }));
}

/// Tags every payload it encodes with the propagated key ID in its context, as an encryption
/// codec picking its key per tenant would, and on decode accepts only payloads tagged with a
/// known key ID. Counts encodes that had no key ID, and those for the Greet activity.
const KeyIdCodec = struct {
    const key_id = propagation.key([]const u8, "key-id");
    const metadata_key = "test-key-id";
    const known_key = "key-1";
    without_key: std.atomic.Value(u32) = .init(0),
    greet_encodes: std.atomic.Value(u32) = .init(0),

    fn codec(k: *KeyIdCodec) converter.PayloadCodec {
        return .{ .ptr = k, .vtable = &.{ .encode = encode, .decode = decode, .encodeWithContext = encodeWithContext, .decodeWithContext = decodeWithContext } };
    }

    fn encode(ptr: *anyopaque, arena: Allocator, io: Io, payloads: []const converter.Payload) converter.CodecError![]converter.Payload {
        return encodeWithContext(ptr, arena, io, &.{}, payloads);
    }

    fn decode(ptr: *anyopaque, arena: Allocator, io: Io, payloads: []const converter.Payload) converter.CodecError![]converter.Payload {
        return decodeWithContext(ptr, arena, io, &.{}, payloads);
    }

    fn encodeWithContext(ptr: *anyopaque, arena: Allocator, _: Io, context: *const converter.Context, payloads: []const converter.Payload) converter.CodecError![]converter.Payload {
        const k: *KeyIdCodec = @ptrCast(@alignCast(ptr));
        const id = context.values.get(key_id) orelse {
            _ = k.without_key.fetchAdd(1, .monotonic);
            return arena.dupe(converter.Payload, payloads);
        };
        if (std.mem.eql(u8, context.activity_type, Fixtures.Greet.activity_name)) _ = k.greet_encodes.fetchAdd(1, .monotonic);
        const out = try arena.dupe(converter.Payload, payloads);
        for (out) |*p| {
            var metadata = try p.metadata.clone(arena);
            try metadata.append(arena, .{ .key = metadata_key, .value = id });
            p.metadata = metadata;
        }
        return out;
    }

    fn decodeWithContext(_: *anyopaque, arena: Allocator, _: Io, _: *const converter.Context, payloads: []const converter.Payload) converter.CodecError![]converter.Payload {
        const out = try arena.dupe(converter.Payload, payloads);
        for (out) |*p| {
            const id = keyIdOf(p.*) orelse continue;
            if (!std.mem.eql(u8, id, known_key)) return error.CodecFailed;
            var metadata: std.ArrayList(converter.Payload.MetadataEntry) = .empty;
            for (p.metadata.items) |m| if (!std.mem.eql(u8, m.key, metadata_key)) try metadata.append(arena, m);
            p.metadata = metadata;
        }
        return out;
    }

    fn keyIdOf(p: converter.Payload) ?[]const u8 {
        for (p.metadata.items) |m| if (std.mem.eql(u8, m.key, metadata_key)) return m.value;
        return null;
    }
};

test "codecs see the propagated key ID of the client call, the workflow run, and the activity" {
    var key_ids: KeyIdCodec = .{};
    const codecs = [_]converter.PayloadCodec{key_ids.codec()};
    const propagators = [_]propagation.ContextPropagator{propagation.HeaderPropagator(KeyIdCodec.key_id).propagator};
    const env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{ .codecs = &codecs, .propagators = &propagators });
    defer env.deinit();
    try env.registerWorkflow(Fixtures.Hello);
    try env.registerActivity(Fixtures.Greet);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var values: propagation.Values = .init(a);
    try values.put(KeyIdCodec.key_id, KeyIdCodec.known_key);
    var run = try env.startWorkflow(Fixtures.Hello, .{ .task_queue = "", .context = &values }, .{"keyed"});
    defer run.deinit();
    // Waiting for the result also replays the history with the codecs.
    try testing.expectEqualStrings("Hello keyed!", try env.getResult(Fixtures.Hello, &run, a, null));
    try testing.expectEqual(@as(u32, 0), key_ids.without_key.load(.monotonic));
    try testing.expect(key_ids.greet_encodes.load(.monotonic) > 0);

    // The server stored each payload tagged with the key ID.
    var plain = try client_mod.Client.connect(testing.allocator, testing.io, env.server.clientOptions());
    defer plain.close();
    const raw = try plain.fetchHistory(a, run.id, run.run_id);
    var tagged: usize = 0;
    for (raw.events.items) |event| {
        const payload = switch (event.attributes orelse continue) {
            .workflow_execution_started_event_attributes => |e| e.input.?.payloads.items[0],
            .activity_task_scheduled_event_attributes => |e| e.input.?.payloads.items[0],
            .activity_task_completed_event_attributes => |e| e.result.?.payloads.items[0],
            .workflow_execution_completed_event_attributes => |e| e.result.?.payloads.items[0],
            else => continue,
        };
        try testing.expectEqualStrings(KeyIdCodec.known_key, KeyIdCodec.keyIdOf(payload).?);
        tagged += 1;
    }
    try testing.expectEqual(@as(usize, 4), tagged);
}

test "codecs and propagators apply to signals and updates sent by name" {
    var key_ids: KeyIdCodec = .{};
    const codecs = [_]converter.PayloadCodec{key_ids.codec()};
    const propagators = [_]propagation.ContextPropagator{propagation.HeaderPropagator(KeyIdCodec.key_id).propagator};
    const env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{ .codecs = &codecs, .propagators = &propagators });
    defer env.deinit();
    try env.registerWorkflow(UpdateFixtures.HandleDynamicUpdates);
    try env.registerWorkflow(RuntimeSignalFixtures.InboxWf);
    try env.registerWorkflow(Fixtures.Hello);
    try env.registerActivity(Fixtures.Greet);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var values: propagation.Values = .init(a);
    try values.put(KeyIdCodec.key_id, KeyIdCodec.known_key);
    _ = try env.executeWorkflowWithOptions(Fixtures.Hello, .{ .task_queue = "", .context = &values }, .{"warm-up"}, a, null); // starts the worker

    const updates: client_mod.StartWorkflowOptions = .{ .id = "coded-by-name", .task_queue = env.task_queue, .id_conflict_policy = .use_existing, .context = &values };
    var set = try env.client.updateWithStartWorkflowByName(i64, "HandleDynamicUpdates", updates, .{}, "set", .{@as(i64, 4)}, .{ .context = &values });
    defer set.deinit();
    try testing.expectEqual(@as(i64, 0), try set.get(a));
    var greet = try env.client.updateWorkflowByName([]const u8, "greet", updates.id, "", .{"ann"}, .{ .context = &values });
    defer greet.deinit();
    try testing.expectEqualStrings("Hello ann!", try greet.get(a));
    try env.client.signalWorkflowByNameWithOptions("done", updates.id, "", .{}, .{ .context = &values });
    var run = try env.client.getWorkflow([]const u8, updates.id, set.run_id);
    defer run.deinit();
    try testing.expectEqualStrings("4 Hello ann!", try run.get(a));

    var inbox = try env.client.signalWithStartWorkflowByName([]const u8, "Inbox", .{ .id = "coded-inbox-by-name", .task_queue = env.task_queue, .context = &values }, .{}, "approve", .{"ann"});
    defer inbox.deinit();
    try env.client.signalWorkflowByNameWithOptions("color", inbox.id, "", .{"red"}, .{ .context = &values });
    try env.client.signalWorkflowByNameWithOptions("size", inbox.id, "", .{"large"}, .{ .context = &values });
    try testing.expectEqualStrings("approved by ann, color=red, size=large", try inbox.get(a));
    try testing.expectEqual(@as(u32, 0), key_ids.without_key.load(.monotonic));

    // The server stored each message's arguments tagged with the key ID, and each message's
    // header carries the propagated value.
    var plain = try client_mod.Client.connect(testing.allocator, testing.io, env.server.clientOptions());
    defer plain.close();
    const Message = struct { input: ?proto.temporal.api.common.v1.Payloads, header: ?proto.temporal.api.common.v1.Header };
    var messages: usize = 0;
    for ([_][]const u8{ updates.id, inbox.id }) |id| {
        const raw = try plain.fetchHistory(a, id, "");
        for (raw.events.items) |event| {
            const m: Message = switch (event.attributes orelse continue) {
                .workflow_execution_signaled_event_attributes => |e| .{ .input = e.input, .header = e.header },
                .workflow_execution_update_accepted_event_attributes => |e| .{ .input = e.accepted_request.?.input.?.args, .header = e.accepted_request.?.input.?.header },
                else => continue,
            };
            for (m.input.?.payloads.items) |p| try testing.expectEqualStrings(KeyIdCodec.known_key, KeyIdCodec.keyIdOf(p).?);
            const header = m.header orelse return error.TestUnexpectedResult;
            for (header.fields.items) |f| {
                if (std.mem.eql(u8, f.key, KeyIdCodec.key_id.name)) break;
            } else return error.TestUnexpectedResult;
            messages += 1;
        }
    }
    // Two updates and a signal, then three signals.
    try testing.expectEqual(@as(usize, 6), messages);
}

const InfoTimeFixtures = struct {
    const Times = struct { workflow_start: i64, activity_scheduled: i64, activity_attempt_scheduled: i64, activity_started: i64 };

    fn startedAt(ctx: *activity.Context) ![3]i64 {
        const info = ctx.info();
        return .{ nanos(info.scheduled_time), nanos(info.current_attempt_scheduled_time), nanos(info.started_time) };
    }
    const StartedAt = activity.Def("StartedAt", startedAt);

    fn times(ctx: *workflow.Context) !Times {
        const activity_times = try ctx.executeActivity(StartedAt, .{ .start_to_close_timeout = .fromSeconds(10) }, .{}).get();
        return .{
            .workflow_start = nanos(ctx.info().start_time),
            .activity_scheduled = activity_times[0],
            .activity_attempt_scheduled = activity_times[1],
            .activity_started = activity_times[2],
        };
    }
    const TimesWf = workflow.Def("InfoTimes", times);

    /// Nanoseconds since the epoch, or -1 for null.
    fn nanos(t: ?Io.Timestamp) i64 {
        return if (t) |v| @intCast(v.nanoseconds) else -1;
    }
};

test "workflow and activity info carry the start and schedule times the server recorded" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(InfoTimeFixtures.TimesWf);
    try env.registerActivity(InfoTimeFixtures.StartedAt);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var run = try env.startWorkflow(InfoTimeFixtures.TimesWf, .{ .task_queue = "" }, .{});
    defer run.deinit();
    const times = try env.getResult(InfoTimeFixtures.TimesWf, &run, a, null);
    const history = try env.client.fetchHistory(a, run.id, run.run_id);
    var started: ?i64 = null;
    var scheduled: ?i64 = null;
    var activity_started: ?i64 = null;
    for (history.events.items) |event| {
        const t = event.event_time orelse continue;
        const ns: i64 = t.seconds * std.time.ns_per_s + t.nanos;
        switch (event.attributes orelse continue) {
            .workflow_execution_started_event_attributes => started = ns,
            .activity_task_scheduled_event_attributes => scheduled = ns,
            .activity_task_started_event_attributes => activity_started = ns,
            else => {},
        }
    }
    // Close, not equal: the server and the core may each stamp a time of their own.
    const tolerance = std.time.ns_per_s;
    try testing.expect(@abs(times.workflow_start - started.?) < tolerance);
    try testing.expect(@abs(times.activity_scheduled - scheduled.?) < tolerance);
    try testing.expect(@abs(times.activity_attempt_scheduled - scheduled.?) < tolerance);
    try testing.expect(@abs(times.activity_started - activity_started.?) < tolerance);
    try testing.expect(times.activity_started >= times.activity_scheduled);
}

/// A history of `Fixtures.Hello` run with `ReversingCodec`: every payload in it is encoded.
const codec_history = @embedFile("testdata/codec_history.json");

test "WorkflowReplayer decodes an encoded history with its codecs" {
    var reversing: ReversingCodec = .{};
    const codecs = [_]converter.PayloadCodec{reversing.codec()};
    var with_codecs: WorkflowReplayer = .initWithOptions(testing.allocator, testing.io, .{ .codecs = &codecs });
    defer with_codecs.deinit();
    try with_codecs.registerWorkflow(Fixtures.Hello);
    try with_codecs.replayHistoryJson(codec_history);
    // Replay encodes the workflow's commands again, as the original worker did.
    try testing.expect(reversing.encoded.load(.monotonic) > 0);

    // Without them the workflow can't read its argument and does something else.
    var without: WorkflowReplayer = .init(testing.allocator, testing.io);
    defer without.deinit();
    try without.registerWorkflow(Fixtures.Hello);
    try testing.expectError(error.Nondeterminism, without.replayHistoryJson(codec_history));
}

const SessionFixtures = struct {
    var hold_started: Io.Event = .unset;

    /// The session and task queue the activity ran in.
    fn whereAmI(ctx: *activity.Context) ![]const u8 {
        return std.fmt.allocPrint(ctx.allocator(), "{s}|{s}", .{ ctx.info().session_id, ctx.info().task_queue });
    }
    const WhereAmI = activity.Def("WhereAmI", whereAmI);

    /// Runs until canceled.
    fn hold(ctx: *activity.Context) !void {
        hold_started.set(ctx.io());
        try ctx.io().sleep(.fromSeconds(600), .awake);
    }
    const Hold = activity.Def("Hold", hold);

    const options: workflow.SessionOptions = .{ .creation_timeout = .fromSeconds(10), .execution_timeout = .fromSeconds(60) };

    fn in(s: *const workflow.Session) workflow.ActivityOptions {
        return .{ .start_to_close_timeout = .fromSeconds(10), .session = s };
    }

    fn whereSessionIs(ctx: *workflow.Context, s: *const workflow.Session) ![]const u8 {
        return std.fmt.allocPrint(ctx.allocator(), "{s}|{s}", .{ s.id, s.task_queue });
    }

    /// What the last failed call's failure was, by its root cause.
    fn lastFailure(ctx: *workflow.Context) ![]const u8 {
        const root = failure_mod.rootCause(ctx.last_failure orelse return "no failure");
        if (failure_mod.applicationType(root.*)) |t| return t;
        return switch (root.failure_info orelse return root.message) {
            .timeout_failure_info => |t| std.fmt.allocPrint(ctx.allocator(), "timeout {t}", .{t.timeout_type}),
            else => root.message,
        };
    }

    const Report = struct {
        session_id: []const u8,
        task_queue: []const u8,
        host_name: []const u8,
        opened: []const u8,
        activities_in_session: bool,
        closed: []const u8,
        after_close: []const u8,
    };

    fn happy(ctx: *workflow.Context) !Report {
        const s = try ctx.createSession(options);
        const opened = s.info();
        const expected = try whereSessionIs(ctx, s);
        const first = try ctx.executeActivity(WhereAmI, in(s), .{}).get();
        const second = try ctx.executeActivity(WhereAmI, in(s), .{}).get();
        try ctx.completeSession(s);
        try ctx.completeSession(s); // closed: a no-op
        const after: []const u8 = if (ctx.executeActivity(WhereAmI, in(s), .{}).get()) |_| "ran" else |err| @errorName(err);
        return .{
            .session_id = opened.session_id,
            .task_queue = opened.task_queue,
            .host_name = opened.host_name,
            .opened = @tagName(opened.state),
            .activities_in_session = std.mem.eql(u8, first, expected) and std.mem.eql(u8, second, expected),
            .closed = @tagName(s.info().state),
            .after_close = after,
        };
    }
    const Happy = workflow.Def("SessionHappyPath", happy);

    /// With one session slot: a second session can't open while the first is, neither from
    /// the shared queue nor recreated on the host; once the first completes, a third opens
    /// on the same host.
    fn cap(ctx: *workflow.Context) ![]const u8 {
        const a = try ctx.createSession(options);
        const queued: []const u8 = if (ctx.createSession(.{ .creation_timeout = .fromSeconds(2), .execution_timeout = .fromSeconds(60) })) |_| "opened" else |err| @errorName(err);
        const queued_failure = try lastFailure(ctx);
        const recreated: []const u8 = if (ctx.recreateSession(a.task_queue, options)) |_| "opened" else |err| @errorName(err);
        const recreated_failure = try lastFailure(ctx);
        try ctx.completeSession(a);
        const c = try ctx.createSession(options);
        const same_host = std.mem.eql(u8, a.task_queue, c.task_queue);
        try ctx.completeSession(c);
        return std.fmt.allocPrint(ctx.allocator(), "{s} ({s}); {s} ({s}); after complete same_host={}", .{ queued, queued_failure, recreated, recreated_failure, same_host });
    }
    const Cap = workflow.Def("SessionCap", cap);

    fn recreate(ctx: *workflow.Context) ![]const u8 {
        const first = try ctx.createSession(options);
        const host = first.info().task_queue;
        try ctx.completeSession(first);
        const second = try ctx.recreateSession(host, options);
        const seen = try ctx.executeActivity(WhereAmI, in(second), .{}).get();
        const expected = try whereSessionIs(ctx, second);
        try ctx.completeSession(second);
        return std.fmt.allocPrint(ctx.allocator(), "new_id={} same_host={} seen={}", .{ !std.mem.eql(u8, first.id, second.id), std.mem.eql(u8, host, second.task_queue), std.mem.eql(u8, seen, expected) });
    }
    const Recreate = workflow.Def("SessionRecreate", recreate);

    fn isFailed(s: *const workflow.Session) bool {
        return s.state() == .failed;
    }

    /// Waits for its session's host to go away while an activity runs in it.
    fn lostHost(ctx: *workflow.Context) ![]const u8 {
        const s = try ctx.createSession(options);
        _ = ctx.executeActivity(Hold, .{ .start_to_close_timeout = .fromSeconds(600), .session = s }, .{});
        try ctx.await(@as(*const workflow.Session, s), isFailed);
        const after: []const u8 = if (ctx.executeActivity(WhereAmI, in(s), .{}).get()) |_| "ran" else |err| @errorName(err);
        const f = s.failure() orelse return "no failure";
        return std.fmt.allocPrint(ctx.allocator(), "{t} {s} {s}", .{ s.state(), after, failure_mod.applicationType(failure_mod.rootCause(f).*) orelse "?" });
    }
    const LostHost = workflow.Def("SessionLostHost", lostHost);

    fn start(max_concurrent_sessions: usize) !*WorkflowEnvironment {
        hold_started = .unset;
        const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
        errdefer env.deinit();
        env.worker.options.sessions = .{ .max_concurrent_sessions = max_concurrent_sessions };
        try env.registerWorkflow(Happy);
        try env.registerWorkflow(Cap);
        try env.registerWorkflow(Recreate);
        try env.registerActivity(WhereAmI);
        return env;
    }
};

test "sessions: activities run on the session's host and see its ID, until the session completes" {
    const env = try SessionFixtures.start(1000);
    defer env.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var run = try env.startWorkflow(SessionFixtures.Happy, .{ .task_queue = "" }, .{});
    defer run.deinit();
    const report = try env.getResult(SessionFixtures.Happy, &run, a, null);
    try testing.expectEqual(36, report.session_id.len);
    try testing.expect(std.mem.startsWith(u8, report.task_queue, try std.fmt.allocPrint(a, "{s}@{s}@", .{ env.task_queue, report.host_name })));
    try testing.expect(report.host_name.len > 0);
    try testing.expectEqualStrings("open", report.opened);
    try testing.expect(report.activities_in_session);
    try testing.expectEqualStrings("closed", report.closed);
    try testing.expectEqualStrings("InvalidOptions", report.after_close);

    // The creation activity waited on the shared session queue, with the session's timeouts
    // and one attempt.
    const history = try env.client.fetchHistory(a, run.id, run.run_id);
    var found = false;
    for (history.events.items) |event| switch (event.attributes orelse continue) {
        .activity_task_scheduled_event_attributes => |s| if (std.mem.eql(u8, s.activity_type.?.name, "__temporal_session_create")) {
            found = true;
            try testing.expectEqualStrings(try std.fmt.allocPrint(a, "{s}__temporal_sessions", .{env.task_queue}), s.task_queue.?.name);
            try testing.expectEqual(10, s.schedule_to_start_timeout.?.seconds);
            try testing.expectEqual(60, s.start_to_close_timeout.?.seconds);
            try testing.expectEqual(20, s.heartbeat_timeout.?.seconds);
            try testing.expectEqual(1, s.retry_policy.?.maximum_attempts);
        },
        else => {},
    };
    try testing.expect(found);
}

test "sessions: unlimited session and activity slots don't overflow, and sessions still run" {
    const env = try SessionFixtures.start(std.math.maxInt(usize));
    defer env.deinit();
    env.worker.options.max_concurrent_activity_executions = std.math.maxInt(usize);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const report = try env.executeWorkflow(SessionFixtures.Happy, .{}, arena.allocator(), null);
    try testing.expect(report.activities_in_session);
    try testing.expectEqualStrings("closed", report.closed);
}

test "sessions: a full host takes no more sessions until one completes" {
    const env = try SessionFixtures.start(1);
    defer env.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const result = try env.executeWorkflow(SessionFixtures.Cap, .{}, arena.allocator(), null);
    try testing.expectEqualStrings("SessionCreationFailed (timeout TIMEOUT_TYPE_SCHEDULE_TO_START); SessionCreationFailed (SessionWorkerFull); after complete same_host=true", result);
}

test "sessions: recreating a session opens a new one on the same host" {
    const env = try SessionFixtures.start(1);
    defer env.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const result = try env.executeWorkflow(SessionFixtures.Recreate, .{}, arena.allocator(), null);
    try testing.expectEqualStrings("new_id=true same_host=true seen=true", result);
}

test "sessions: a session fails when its host stops, and then its activities aren't scheduled" {
    SessionFixtures.hold_started = .unset;
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(SessionFixtures.LostHost);
    var host = try worker_mod.Worker.init(testing.allocator, testing.io, &env.client, env.task_queue, .{ .sessions = .{} });
    defer host.deinit();
    try host.registerActivity(SessionFixtures.Hold);
    try host.registerActivity(SessionFixtures.WhereAmI);
    var host_running = try testing.io.concurrent(WorkflowEnvironment.runWorker, .{&host});
    defer {
        host.stop();
        host_running.await(testing.io) catch {};
    }
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var run = try env.startWorkflow(SessionFixtures.LostHost, .{ .task_queue = "" }, .{});
    defer run.deinit();
    try SessionFixtures.hold_started.wait(testing.io);
    host.stop();
    const result = try env.getResult(SessionFixtures.LostHost, &run, arena.allocator(), null);
    try testing.expectEqualStrings("failed SessionFailed SessionWorkerStopped", result);
}

// Failure policy: a listed error fails the workflow task; the run stays open and completes
// once the code no longer returns it.
const PolicyFixtures = struct {
    const Poke = workflow.UpdateDef("poke", fn () u32);
    /// How many times the code below ran; each returns error.ConfigMissing the first time.
    /// Test-only global state.
    var calls: std.atomic.Value(u32) = .init(0);

    fn flaky(_: *workflow.Context) ![]const u8 {
        if (calls.fetchAdd(1, .acq_rel) == 0) return error.ConfigMissing;
        return "configured";
    }
    fn alwaysMissing(_: *workflow.Context) ![]const u8 {
        return error.ConfigMissing;
    }

    const State = struct {
        poked: bool = false,
        fn poke(s: *State) !u32 {
            if (calls.fetchAdd(1, .acq_rel) == 0) return error.ConfigMissing;
            s.poked = true;
            return 7;
        }
        fn isPoked(s: *State) bool {
            return s.poked;
        }
    };
    fn waitForPoke(ctx: *workflow.Context) ![]const u8 {
        const s = try ctx.allocator().create(State);
        s.* = .{};
        try ctx.setUpdateHandler(Poke, s, State.poke);
        try ctx.await(s, State.isPoked);
        return "poked";
    }

    const Flaky = workflow.Def("PolicyFlaky", flaky);
    const AlwaysMissing = workflow.Def("PolicyFlaky", alwaysMissing);
    const WaitForPoke = workflow.DefWithOptions("PolicyWaitForPoke", waitForPoke, .{ .task_failure_errors = &.{error.ConfigMissing} });

    /// The message of the first workflow task failure in the run's history, or null.
    fn taskFailureMessage(env: *WorkflowEnvironment, a: Allocator, id: []const u8, run_id: []const u8) !?[]const u8 {
        const history = try env.client.fetchHistory(a, id, run_id);
        for (history.events.items) |event| switch (event.attributes orelse continue) {
            .workflow_task_failed_event_attributes => |f| return if (f.failure) |failure| failure.message else "",
            else => {},
        };
        return null;
    }
};

test "failure policy: a listed error fails the workflow task, the workflow completes on retry, and its history replays" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    env.worker.options.workflow_task_failure_errors = &.{error.ConfigMissing};
    try env.registerWorkflow(PolicyFixtures.Flaky);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    PolicyFixtures.calls.store(0, .release);

    var run = try env.startWorkflow(PolicyFixtures.Flaky, .{ .task_queue = "" }, .{});
    defer run.deinit();
    try testing.expectEqualStrings("configured", try env.getResult(PolicyFixtures.Flaky, &run, a, null));
    const message = (try PolicyFixtures.taskFailureMessage(env, a, run.id, run.run_id)) orelse return error.TestExpectedTaskFailure;
    try testing.expect(std.mem.indexOf(u8, message, "the workflow function of workflow \"PolicyFlaky\" returned error.ConfigMissing") != null);

    // The failed task isn't replayed: code that no longer returns the error matches.
    var r: WorkflowReplayer = .initWithOptions(testing.allocator, testing.io, .{ .task_queue = env.task_queue, .workflow_task_failure_errors = &.{error.ConfigMissing} });
    defer r.deinit();
    try r.registerWorkflow(PolicyFixtures.Flaky);
    const ok = try r.replayWorkflowExecution(a, &env.client, run.id, run.run_id);
    try testing.expectEqual(@as(?WorkflowReplayer.ReplayError, null), ok.err);

    // Code that still returns it fails its task in replay too.
    var still: WorkflowReplayer = .initWithOptions(testing.allocator, testing.io, .{ .task_queue = env.task_queue, .workflow_task_failure_errors = &.{error.ConfigMissing} });
    defer still.deinit();
    try still.registerWorkflow(PolicyFixtures.AlwaysMissing);
    const failed = try still.replayWorkflowExecution(a, &env.client, run.id, run.run_id);
    try testing.expectEqual(@as(?WorkflowReplayer.ReplayError, error.ReplayFailed), failed.err);
}

test "failure policy: a listed error from an update handler fails the task, and the retried task completes the update" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(PolicyFixtures.WaitForPoke);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    PolicyFixtures.calls.store(0, .release);

    var run = try env.startWorkflow(PolicyFixtures.WaitForPoke, .{ .task_queue = "" }, .{});
    defer run.deinit();
    var update = try env.client.updateWorkflow(PolicyFixtures.Poke, run.id, run.run_id, .{}, .{});
    defer update.deinit();
    try testing.expectEqual(@as(u32, 7), try update.get(a));
    try testing.expectEqualStrings("poked", try env.getResult(PolicyFixtures.WaitForPoke, &run, a, null));
    const message = (try PolicyFixtures.taskFailureMessage(env, a, run.id, run.run_id)) orelse return error.TestExpectedTaskFailure;
    try testing.expect(std.mem.indexOf(u8, message, "update handler \"poke\"") != null);
    try testing.expect(std.mem.indexOf(u8, message, "returned error.ConfigMissing") != null);
}

const CronContinueFixtures = struct {
    /// A cron workflow whose run continues as new itself; the run it starts returns.
    fn cronContinue(ctx: *workflow.Context, continued: bool) ![]const u8 {
        if (continued) return "continued run";
        return ctx.continueAsNew(CronContinue, .{}, .{true});
    }
    const CronContinue = workflow.Def("CronContinue", cronContinue);
};

test "time-skipping WorkflowEnvironment.getResultWithOptions: a cron run that continued as new itself returns error.WorkflowContinuedAsNew" {
    var watch: RunWatch = .{ .gpa = testing.allocator, .io = testing.io };
    defer watch.deinit();
    const interceptors = [_]interceptor.Worker{interceptor.Worker.from(RunWatch, &watch)};
    const env = try WorkflowEnvironment.startTimeSkippingWithOptions(testing.allocator, testing.io, .{ .worker_interceptors = &interceptors });
    defer env.deinit();
    try env.registerWorkflow(CronContinueFixtures.CronContinue);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var cron = try env.startWorkflow(CronContinueFixtures.CronContinue, .{ .task_queue = "", .cron_schedule = "0 * * * *" }, .{false});
    defer cron.deinit();
    defer env.client.terminateWorkflow(cron.id, "", "test cleanup") catch {};
    const first_run = try a.dupe(u8, cron.run_id);
    // The workflow continued as new itself: the doc comment says that run "is replayed and
    // then returns error.WorkflowContinuedAsNew", whatever its cron schedule.
    try testing.expectError(error.WorkflowContinuedAsNew, env.getResultWithOptions(CronContinueFixtures.CronContinue, &cron, .{ .follow_runs = false }, a, null));
    try testing.expectEqualStrings(first_run, cron.run_id);
    try testing.expectEqualStrings(first_run, watch.lastReplayed() orelse return error.TestUnexpectedResult);
}

test "WorkflowEnvironment.getResultWithOptions: without following runs a run handle with no run ID stops at the run that continued as new" {
    for (env_kinds) |kind| {
        var watch: RunWatch = .{ .gpa = testing.allocator, .io = testing.io };
        defer watch.deinit();
        const interceptors = [_]interceptor.Worker{interceptor.Worker.from(RunWatch, &watch)};
        const env = try WorkflowEnvironment.create(testing.allocator, testing.io, kind, .{ .worker_interceptors = &interceptors });
        defer env.deinit();
        try env.registerWorkflow(ChainFixtures.SleepThenContinue);
        var arena: std.heap.ArenaAllocator = .init(testing.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        const seconds: u32 = if (kind == .time_skipping) std.time.s_per_day else 1;

        var started = try env.startWorkflow(ChainFixtures.SleepThenContinue, .{ .task_queue = "" }, .{ seconds, false });
        defer started.deinit();
        // `client.getWorkflow` with an empty run ID follows the latest run, and the client
        // itself stops at that run's continue-as-new with `.follow_runs = false`.
        var latest = try env.client.getWorkflow([]const u8, started.id, "");
        defer latest.deinit();
        try testing.expectError(error.WorkflowContinuedAsNew, env.getResultWithOptions(ChainFixtures.SleepThenContinue, &latest, .{ .follow_runs = false }, a, null));
        // The run replayed is the one that continued as new, not the run it started.
        try testing.expectEqualStrings(started.run_id, watch.lastReplayed() orelse return error.TestUnexpectedResult);
    }
}

test "ActivityEnvironment.executeActivity adds only error.Canceled to the activity's own errors" {
    // An activity that can only fail with error.Declined: the environment's call may add
    // error.Canceled (a canceled task), and with no logger set it has nothing to allocate,
    // so callers that switch exhaustively over its errors still compile as before.
    const S = struct {
        fn declined(_: *activity.Context, fail: bool) error{Declined}!void {
            if (fail) return error.Declined;
        }
        const Declined = activity.Def("Declined", declined);
    };
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var env: ActivityEnvironment = .init(testing.io);
    const E = @typeInfo(@TypeOf(env.executeActivity(S.Declined, .{false}, arena.allocator()))).error_union.error_set;
    const names = comptime @typeInfo(E).error_set.error_names.?;
    for (names) |name| {
        if (!std.mem.eql(u8, name, "Declined") and !std.mem.eql(u8, name, "Canceled")) {
            std.debug.print("executeActivity added error.{s} to the activity's errors\n", .{name});
            return error.TestUnexpectedError;
        }
    }
    try env.executeActivity(S.Declined, .{false}, arena.allocator());

    // Setting a logger builds its context fields without allocating, so it adds nothing either.
    var recorder: EnvironmentLogFixtures.Recorder = .{ .arena = .init(testing.allocator) };
    defer recorder.arena.deinit();
    env.logger = recorder.sink();
    const Logged = @typeInfo(@TypeOf(env.executeActivity(S.Declined, .{false}, arena.allocator()))).error_union.error_set;
    try testing.expect(Logged == E);
    try testing.expectError(error.Declined, env.executeActivity(S.Declined, .{true}, arena.allocator()));
}
