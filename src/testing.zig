//! Testing workflows and activities.
//!
//! Tests run against real local servers:
//!
//! - `DevServer`: a local Temporal dev server.
//! - `WorkflowEnvironment`: a dev server, client and worker on a fresh task queue. Register
//!   workflows and activities, replace activities with test doubles, and
//!   execute workflows to their result.
//! - `ActivityEnvironment`: calls an activity function directly with a test context.
//! - `WorkflowReplayer`: replays recorded histories against current workflow code to catch
//!   nondeterministic changes.

const std = @import("std");
const bridge = @import("bridge.zig");
const proto = @import("proto.zig");
const client_mod = @import("client.zig");
const worker_mod = @import("worker.zig");
const workflow = @import("workflow.zig");
const activity = @import("activity.zig");
const converter = @import("converter.zig");
const history_mod = @import("history.zig");
const failure_mod = @import("failure.zig");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const log = std.log.scoped(.temporal);

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
        /// Extra arguments for `temporal server start-dev`.
        extra_args: []const []const u8 = &.{},
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
        /// Version to download when `existing_path` is empty: "default" or an exact version.
        version: []const u8 = "default",
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

/// A server, client and worker for running workflows in tests.
///
/// `start` runs a dev server; `startTimeSkipping` runs the time-skipping test server. On the
/// test server time is locked (it moves at wall-clock speed) except while the test waits on
/// a workflow result through `getResult` or `executeWorkflow`, or calls `sleep`. Then it
/// jumps ahead whenever every workflow is waiting on a timer, so a 2h sleep finishes at
/// once, while a test can still look at a workflow it started with `startWorkflow` in the
/// middle of a timer.
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

    pub const Server = union(enum) {
        dev: DevServer,
        time_skipping: TestServer,

        fn clientOptions(s: *const Server) client_mod.Options {
            return switch (s.*) {
                inline else => |*server| server.clientOptions(),
            };
        }

        fn stop(s: *Server, io: Io) void {
            switch (s.*) {
                inline else => |*server| server.stop(io),
            }
        }
    };

    /// Starts the environment on a dev server. Allocated so the worker's pointer to the
    /// client stays valid. Free with `deinit`.
    pub fn start(gpa: Allocator, io: Io) !*WorkflowEnvironment {
        return create(gpa, io, .dev);
    }

    /// Starts the environment on the time-skipping test server. Free with `deinit`.
    pub fn startTimeSkipping(gpa: Allocator, io: Io) !*WorkflowEnvironment {
        return create(gpa, io, .time_skipping);
    }

    fn create(gpa: Allocator, io: Io, kind: std.meta.Tag(Server)) !*WorkflowEnvironment {
        const env = try gpa.create(WorkflowEnvironment);
        errdefer gpa.destroy(env);
        env.gpa = gpa;
        env.io = io;
        env.running = null;
        env.next_id = 0;
        env.server = switch (kind) {
            .dev => .{ .dev = try DevServer.start(gpa, io, .{}) },
            .time_skipping => .{ .time_skipping = try TestServer.start(gpa, io, .{}) },
        };
        errdefer env.server.stop(io);
        env.client = try client_mod.Client.connect(gpa, io, env.server.clientOptions());
        errdefer env.client.close();
        var suffix: [8]u8 = undefined;
        io.random(&suffix);
        env.task_queue = try std.fmt.allocPrint(gpa, "test-{x}", .{suffix});
        errdefer gpa.free(env.task_queue);
        env.worker = try worker_mod.Worker.init(gpa, io, &env.client, env.task_queue, .{});
        return env;
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
    pub fn registerActivityWith(env: *WorkflowEnvironment, comptime Activity: type, receiver: *Activity.Receiver) !void {
        return env.worker.registerActivityWith(Activity, receiver);
    }

    /// Registers several method activities bound to one receiver. See
    /// `Worker.registerActivitiesWith`.
    pub fn registerActivitiesWith(env: *WorkflowEnvironment, receiver: anytype, comptime activities: anytype) !void {
        return env.worker.registerActivitiesWith(receiver, activities);
    }

    /// Registers `impl` in place of `Activity`: workflows calling `Activity` run `impl`.
    /// `impl` takes the same parameters.
    pub fn registerActivityAs(env: *WorkflowEnvironment, comptime Activity: type, comptime impl: anytype) !void {
        return env.worker.registerActivityImpl(activity.Def(Activity.activity_name, impl).registered);
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
        return env.executeWorkflowWithOptions(Workflow, .{ .task_queue = "" }, args, arena, failure);
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
        if (env.running == null) env.running = try env.io.concurrent(runWorker, .{&env.worker});
        env.next_id += 1;
        var opts = options;
        var generated_id: ?[]u8 = null;
        defer if (generated_id) |id| env.gpa.free(id);
        if (opts.id.len == 0) {
            generated_id = try std.fmt.allocPrint(env.gpa, "{s}-{s}-{d}", .{ Workflow.workflow_name, env.task_queue, env.next_id });
            opts.id = generated_id.?;
        }
        if (opts.task_queue.len == 0) opts.task_queue = env.task_queue;
        return env.client.executeWorkflow(Workflow, opts, args);
    }

    /// Waits for a run from `startWorkflow` to close and returns its result, allocated from
    /// `arena`, then replays its history as `executeWorkflow` does. On the time-skipping
    /// server, time skipping is unlocked for the wait. On `error.WorkflowFailed`, `failure`
    /// (if given) receives the workflow's failure. The run stays owned by the caller.
    pub fn getResult(
        env: *WorkflowEnvironment,
        comptime Workflow: type,
        run: *client_mod.WorkflowRun(Workflow.Result),
        arena: Allocator,
        failure: ?*?failure_mod.Failure,
    ) !Workflow.Result {
        const skipping = env.server == .time_skipping;
        if (skipping) _ = try env.testService(ts.UnlockTimeSkippingResponse, "UnlockTimeSkipping", ts.UnlockTimeSkippingRequest{}, arena);
        const result = run.get(arena);
        if (skipping) _ = try env.testService(ts.LockTimeSkippingResponse, "LockTimeSkipping", ts.LockTimeSkippingRequest{}, arena);
        if (result) |_| {} else |err| {
            if (failure) |f| f.* = if (run.failure()) |fl| fl.* else null;
            switch (err) {
                error.WorkflowFailed, error.WorkflowCanceled, error.WorkflowTimedOut => {},
                else => return err,
            }
        }
        try env.replayRun(run.id, run.run_id);
        return result;
    }

    /// Lets `duration` of server time pass. On the time-skipping server this skips time
    /// (workflows see their timers fire) and returns as soon as server time has advanced; on
    /// the dev server it is a real sleep.
    pub fn sleep(env: *WorkflowEnvironment, duration: Io.Duration) !void {
        switch (env.server) {
            .dev => try env.io.sleep(duration, .awake),
            .time_skipping => {
                var scratch: std.heap.ArenaAllocator = .init(env.gpa);
                defer scratch.deinit();
                _ = try env.testService(ts.SleepResponse, "UnlockTimeSkippingWithSleep", ts.SleepRequest{ .duration = .{
                    .seconds = @intCast(@divFloor(duration.nanoseconds, std.time.ns_per_s)),
                    .nanos = @intCast(@mod(duration.nanoseconds, std.time.ns_per_s)),
                } }, scratch.allocator());
            },
        }
    }

    /// The server's current time: skipped time included on the time-skipping server, the
    /// wall clock on the dev server.
    pub fn now(env: *WorkflowEnvironment) !Io.Timestamp {
        switch (env.server) {
            .dev => return Io.Timestamp.now(env.io, .real),
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

    const ts = proto.temporal.api.testservice.v1;

    /// Calls a TestService method; the response's memory comes from `arena`. Not retried:
    /// the server counts lock and unlock calls, so a retried call could unbalance them.
    fn testService(env: *WorkflowEnvironment, comptime Response: type, comptime method: []const u8, request: anytype, arena: Allocator) error{ OutOfMemory, Canceled, RpcFailed, InvalidResponse }!Response {
        var encoded: Io.Writer.Allocating = .init(arena);
        request.encode(&encoded.writer, arena) catch return error.OutOfMemory;
        var rpc_failure: ?bridge.Connection.RpcFailure = null;
        defer if (rpc_failure) |f| f.deinit(env.gpa);
        const bytes = env.client.connection.call(env.gpa, env.io, .test_service, method, encoded.written(), .{}, &rpc_failure) catch |err| switch (err) {
            error.RpcFailed => {
                const f = rpc_failure.?;
                log.err("test server {s} failed: {s} (gRPC status {d})", .{ method, f.message, f.status_code });
                return error.RpcFailed;
            },
            error.InvalidOptions => return error.RpcFailed,
            else => |e| return e,
        };
        defer env.gpa.free(bytes);
        const owned = try arena.dupe(u8, bytes);
        var reader: Io.Reader = .fixed(owned);
        return Response.decode(&reader, arena) catch |err| switch (err) {
            error.OutOfMemory => error.OutOfMemory,
            else => error.InvalidResponse,
        };
    }

    /// Replays a run's history (fetched from the server) against the registered workflows.
    pub fn replayRun(env: *WorkflowEnvironment, workflow_id: []const u8, run_id: []const u8) !void {
        var scratch: std.heap.ArenaAllocator = .init(env.gpa);
        defer scratch.deinit();
        const history = try env.client.fetchHistory(scratch.allocator(), workflow_id, run_id);
        var replayer: WorkflowReplayer = .{ .gpa = env.gpa, .io = env.io, .workflows = env.worker.workflows };
        return replayer.replayHistory(history);
    }

    fn runWorker(w: *worker_mod.Worker) worker_mod.Worker.RunError!void {
        return w.run();
    }
};

/// Runs activities directly, without a server.
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
    },
    /// Heartbeat details the activity sees from a "previous attempt".
    heartbeat_details: []const converter.Payload = &.{},
    /// Called with each heartbeat's details.
    on_heartbeat: ?activity.Context.HeartbeatSink = null,

    pub fn init(io: Io) ActivityEnvironment {
        return .{ .io = io };
    }

    /// Calls the activity's function with `args`. The result is allocated from `arena`.
    pub fn executeActivity(env: *const ActivityEnvironment, comptime Activity: type, args: Activity.Args, arena: Allocator) !Activity.Result {
        return env.executeActivityWith(Activity, null, args, arena);
    }

    /// Calls a method activity on `receiver` (null for plain functions).
    pub fn executeActivityWith(env: *const ActivityEnvironment, comptime Activity: type, receiver: ?*Activity.Receiver, args: Activity.Args, arena: Allocator) !Activity.Result {
        var info = env.info;
        info.activity_type = Activity.activity_name;
        var ctx: activity.Context = .{
            .arena = arena,
            .io_ = env.io,
            .info_ = info,
            .logger_ = .{},
            .heartbeat_details = env.heartbeat_details,
            .heartbeat_sink = env.on_heartbeat,
        };
        return Activity.call(receiver, &ctx, args);
    }
};

/// Replays recorded histories against registered workflows.
pub const WorkflowReplayer = struct {
    gpa: Allocator,
    io: Io,
    workflows: std.StringHashMapUnmanaged(workflow.Registered) = .empty,

    pub fn init(gpa: Allocator, io: Io) WorkflowReplayer {
        return .{ .gpa = gpa, .io = io };
    }

    pub fn deinit(r: *WorkflowReplayer) void {
        r.workflows.deinit(r.gpa);
    }

    pub fn registerWorkflow(r: *WorkflowReplayer, comptime Workflow: type) !void {
        try r.workflows.put(r.gpa, Workflow.workflow_name, Workflow.registered);
    }

    pub const ReplayError = error{ OutOfMemory, InvalidHistoryJson, ReplayFailed, Nondeterminism };

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
        const a = arena_state.allocator();
        const h = try history_mod.fromJson(a, json);
        var encoded: Io.Writer.Allocating = .init(a);
        h.encode(&encoded.writer, a) catch return error.OutOfMemory;
        return r.replayHistoryBytes(encoded.written());
    }

    /// Replays a history, for example one from `Client.fetchHistory`.
    pub fn replayHistory(r: *WorkflowReplayer, history: history_mod.History) ReplayError!void {
        var scratch: std.heap.ArenaAllocator = .init(r.gpa);
        defer scratch.deinit();
        var encoded: Io.Writer.Allocating = .init(scratch.allocator());
        history.encode(&encoded.writer, scratch.allocator()) catch return error.OutOfMemory;
        return r.replayHistoryBytes(encoded.written());
    }

    /// Replays a serialized temporal.api.history.v1.History.
    pub fn replayHistoryBytes(r: *WorkflowReplayer, history_bytes: []const u8) ReplayError!void {
        var message: ?[]u8 = null;
        defer if (message) |m| r.gpa.free(m);
        const runtime = bridge.Runtime.init(r.gpa, .{}, &message) catch return error.ReplayFailed;
        defer runtime.deinit();
        var replayer = bridge.Worker.initReplayer(r.gpa, runtime, .{
            .namespace = "default",
            .task_queue = "replay",
            .task_types = .{ .local_activities = false, .remote_activities = false, .nexus = false },
            .max_cached_workflows = 1,
        }, &message) catch {
            log.err("creating the replay worker failed: {s}", .{message orelse "unknown error"});
            return error.ReplayFailed;
        };
        defer replayer.worker.deinit();

        var outcome: Outcome = .{ .gpa = r.gpa };
        defer outcome.deinit();
        var runner: worker_mod.WorkflowRunner = .{
            .gpa = r.gpa,
            .workflows = &r.workflows,
            .task_queue = "replay",
            .namespace = "default",
            .stack_size = 256 * 1024,
            .on_evict = Outcome.record,
            .on_evict_ctx = &outcome,
        };
        defer runner.deinit();

        // The core sends the history from a background task that uses the pusher, so the
        // pusher is freed only after the worker has shut down (as the .NET SDK does).
        defer replayer.finishPushing();
        replayer.push(r.gpa, "replay", history_bytes, &message) catch {
            log.err("pushing a history for replay failed: {s}", .{message orelse "unknown error"});
            return error.ReplayFailed;
        };

        // The core evicts the run when replay reaches the end of the history (or fails).
        while (!outcome.evicted) {
            const polled = replayer.worker.pollWorkflowActivation(r.gpa, r.io, &message) catch {
                log.err("replay polling failed: {s}", .{message orelse "unknown error"});
                return error.ReplayFailed;
            } orelse break;
            defer polled.deinit();
            runner.process(replayer.worker, r.io, polled.bytes());
        }
        replayer.worker.initiateShutdown();
        while (true) {
            const polled = replayer.worker.pollWorkflowActivation(r.gpa, r.io, &message) catch break orelse break;
            defer polled.deinit();
            runner.process(replayer.worker, r.io, polled.bytes());
        }
        replayer.worker.finalizeShutdown(r.gpa, r.io, null) catch {};

        if (!outcome.evicted) {
            log.err("replay ended without finishing the history", .{});
            return error.ReplayFailed;
        }
        if (outcome.failed) {
            log.warn("replay failed: {s}", .{outcome.message orelse "unknown reason"});
            return if (outcome.nondeterminism) error.Nondeterminism else error.ReplayFailed;
        }
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

    /// Sleeps, then returns how many seconds of workflow time passed.
    fn sleepFor(ctx: *workflow.Context, seconds: i64) !i64 {
        const before = ctx.now();
        try ctx.io().sleep(.fromSeconds(seconds), .awake);
        return @intCast(@divFloor(ctx.now().nanoseconds - before.nanoseconds, std.time.ns_per_s));
    }
    const Sleeper = workflow.Def("Sleeper", sleepFor);
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

test "TestServer starts a time-skipping server a client can connect to" {
    var server = try TestServer.start(testing.allocator, testing.io, .{});
    defer server.stop(testing.io);
    var client = try client_mod.Client.connect(testing.allocator, testing.io, server.clientOptions());
    client.close();
}

test "ActivityEnvironment calls the activity directly" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const env: ActivityEnvironment = .init(testing.io);
    try testing.expectEqualStrings("Hello World!", try env.executeActivity(Fixtures.Greet, .{"World"}, arena.allocator()));
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
