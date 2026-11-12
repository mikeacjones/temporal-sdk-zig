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
/// Tests drive a running workflow by its ID with `signalWorkflow`, `queryWorkflow` and
/// `updateWorkflow`, or start one together with a signal or an update with
/// `signalWithStartWorkflow` and `updateWithStartWorkflow`. These leave time locked: a
/// workflow waiting for a signal or an update would otherwise skip straight to its execution
/// timeout. Let an update handler's sleep pass with `sleep` (see `updateWorkflow`).
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
        try env.ensureWorker();
        var generated_id: ?[]u8 = null;
        defer if (generated_id) |id| env.gpa.free(id);
        return env.client.executeWorkflow(Workflow, try env.startOptions(Workflow, options, &generated_id), args);
    }

    /// Sends `Signal` with `args` to the latest run of `workflow_id`. Returns once the server
    /// has recorded the signal; time stays locked on the time-skipping server.
    pub fn signalWorkflow(env: *WorkflowEnvironment, comptime Signal: type, workflow_id: []const u8, args: Signal.Args) !void {
        return env.client.signalWorkflow(Signal, workflow_id, "", args);
    }

    /// Sends `Query` with `args` to the latest run of `workflow_id` (starting the worker if
    /// needed, so it can answer) and returns the result, allocated from `arena`. Time stays
    /// locked on the time-skipping server, so a workflow in the middle of a timer answers
    /// without the timer firing.
    pub fn queryWorkflow(env: *WorkflowEnvironment, comptime Query: type, arena: Allocator, workflow_id: []const u8, args: Query.Args) !Query.Result {
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
        try env.ensureWorker();
        var generated_id: ?[]u8 = null;
        defer if (generated_id) |id| env.gpa.free(id);
        return env.client.signalWithStartWorkflow(Workflow, try env.startOptions(Workflow, options, &generated_id), args, Signal, signal_args);
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
        try env.ensureWorker();
        var generated_id: ?[]u8 = null;
        defer if (generated_id) |id| env.gpa.free(id);
        return env.client.updateWithStartWorkflow(Workflow, try env.startOptions(Workflow, options, &generated_id), args, Update, update_args, update_options);
    }

    fn ensureWorker(env: *WorkflowEnvironment) !void {
        if (env.running == null) env.running = try env.io.concurrent(runWorker, .{&env.worker});
    }

    /// `options` with `id` and `task_queue` defaulted to a unique ID and the environment's
    /// task queue. A generated ID is allocated with `env.gpa` and stored in `generated_id`,
    /// which the caller frees once the start call returns.
    fn startOptions(env: *WorkflowEnvironment, comptime Workflow: type, options: client_mod.StartWorkflowOptions, generated_id: *?[]u8) !client_mod.StartWorkflowOptions {
        env.next_id += 1;
        var opts = options;
        if (opts.id.len == 0) {
            generated_id.* = try std.fmt.allocPrint(env.gpa, "{s}-{s}-{d}", .{ Workflow.workflow_name, env.task_queue, env.next_id });
            opts.id = generated_id.*.?;
        }
        if (opts.task_queue.len == 0) opts.task_queue = env.task_queue;
        return opts;
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
        var replayer: WorkflowReplayer = .{ .gpa = env.gpa, .io = env.io, .workflows = env.worker.workflows, .workflow_id = workflow_id };
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
    /// The workflow ID replayed workflows see in `ctx.info().workflow_id`. A history doesn't
    /// record its own workflow ID, so set this to the original run's ID when the workflow's
    /// commands depend on it (a child or activity ID derived from it, for example), or
    /// replay fails with `error.Nondeterminism`. Borrowed: it must outlive every replay call.
    workflow_id: []const u8 = "replay",

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
        replayer.push(r.gpa, r.workflow_id, history_bytes, &message) catch {
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

/// A dev-server run of `Fixtures.WaitCancellation` canceled with `Client.cancelWorkflow` while
/// the activity heartbeated, exported with `temporal workflow show`.
const wait_cancellation_history = @embedFile("testdata/wait_cancellation_history.json");

test "WorkflowReplayer replays a workflow that waits for its activity's cancellation" {
    var ok: WorkflowReplayer = .init(testing.allocator, testing.io);
    defer ok.deinit();
    try ok.registerWorkflow(Fixtures.WaitCancellation);
    try ok.replayHistoryJson(wait_cancellation_history);
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

/// Both kinds of `WorkflowEnvironment`, for tests that run on each.
const env_kinds = [_]std.meta.Tag(WorkflowEnvironment.Server){ .dev, .time_skipping };

fn startEnv(kind: std.meta.Tag(WorkflowEnvironment.Server)) !*WorkflowEnvironment {
    return WorkflowEnvironment.create(testing.allocator, testing.io, kind);
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
