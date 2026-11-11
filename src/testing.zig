//! Testing workflows and activities (Go `testsuite` parity).
//!
//! The Go SDK tests workflows in an in-memory mock environment. This SDK, like the other
//! SDKs built on sdk-core, tests against real local servers instead:
//!
//! - `DevServer`: a local Temporal dev server (Go `testsuite.StartDevServer`).
//! - `WorkflowEnvironment`: a dev server, client and worker on a fresh task queue. Register
//!   workflows and activities, replace activities with test doubles (Go's `OnActivity`), and
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
const history_mod = @import("history.zig");
const failure_mod = @import("failure.zig");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const log = std.log.scoped(.temporal);

/// A local dev server for tests. Go `testsuite.StartDevServer`.
pub const DevServer = struct {
    runtime: bridge.Runtime,
    server: bridge.EphemeralServer,
    namespace: []const u8,

    pub const Options = struct {
        namespace: []const u8 = "default",
        search_attributes: []const bridge.EphemeralServer.SearchAttribute = &.{},
        /// Path to a Temporal CLI binary; empty downloads one (cached).
        existing_path: []const u8 = "",
        log_level: []const u8 = "error",
    };

    pub fn start(gpa: Allocator, io: Io, options: Options) !DevServer {
        var message: ?[]u8 = null;
        defer if (message) |m| gpa.free(m);
        const runtime = try bridge.Runtime.init(gpa, .{}, &message);
        errdefer runtime.deinit();
        const server = bridge.EphemeralServer.startDevServer(gpa, io, runtime, .{
            .executable = if (options.existing_path.len > 0) .{ .existing_path = options.existing_path } else .{ .download = .{} },
            .namespace = options.namespace,
            .search_attributes = options.search_attributes,
            .log_level = options.log_level,
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

/// A dev server, client and worker for running workflows in tests.
pub const WorkflowEnvironment = struct {
    gpa: Allocator,
    io: Io,
    server: DevServer,
    client: client_mod.Client,
    worker: worker_mod.Worker,
    task_queue: []u8,
    running: ?Io.Future(worker_mod.Worker.RunError!void) = null,
    next_id: u32 = 0,

    /// Starts the environment. Allocated so the worker's pointer to the client stays valid.
    pub fn start(gpa: Allocator, io: Io) !*WorkflowEnvironment {
        const env = try gpa.create(WorkflowEnvironment);
        errdefer gpa.destroy(env);
        env.gpa = gpa;
        env.io = io;
        env.running = null;
        env.next_id = 0;
        env.server = try DevServer.start(gpa, io, .{});
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

    /// Registers `impl` in place of `Activity`: workflows calling `Activity` run `impl`.
    /// `impl` takes the same parameters. Go `env.OnActivity(Activity, ...).Return(...)`.
    pub fn registerActivityAs(env: *WorkflowEnvironment, comptime Activity: type, comptime impl: anytype) !void {
        return env.worker.registerActivityImpl(activity.Def(Activity.activity_name, impl).registered);
    }

    /// Runs a workflow to completion on the environment's worker and returns its result,
    /// allocated from `arena`. On `error.WorkflowFailed`, `failure` (if given) receives the
    /// workflow's failure.
    pub fn executeWorkflow(
        env: *WorkflowEnvironment,
        comptime Workflow: type,
        args: Workflow.Args,
        arena: Allocator,
        failure: ?*?failure_mod.Failure,
    ) !Workflow.Result {
        if (env.running == null) env.running = try env.io.concurrent(runWorker, .{&env.worker});
        env.next_id += 1;
        const id = try std.fmt.allocPrint(arena, "{s}-{s}-{d}", .{ Workflow.workflow_name, env.task_queue, env.next_id });
        var run = try env.client.executeWorkflow(Workflow, .{ .id = id, .task_queue = env.task_queue }, args);
        defer run.deinit();
        return run.get(arena) catch |err| {
            if (failure) |f| f.* = if (run.failure()) |fl| fl.* else null;
            return err;
        };
    }

    fn runWorker(w: *worker_mod.Worker) worker_mod.Worker.RunError!void {
        return w.run();
    }
};

/// Runs activities directly, without a server. Go `TestActivityEnvironment`.
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

    pub fn init(io: Io) ActivityEnvironment {
        return .{ .io = io };
    }

    /// Calls the activity's function with `args`. The result is allocated from `arena`.
    pub fn executeActivity(env: *const ActivityEnvironment, comptime Activity: type, args: Activity.Args, arena: Allocator) !Activity.Result {
        var info = env.info;
        info.activity_type = Activity.activity_name;
        var ctx: activity.Context = .{ .arena = arena, .io_ = env.io, .info_ = info, .logger_ = .{} };
        return @call(.auto, Activity.function, .{&ctx} ++ args);
    }
};

/// Replays recorded histories against registered workflows. Go `worker.NewWorkflowReplayer`.
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
    const Greet = activity.Def("Activity", greet);

    fn greetDouble(ctx: *activity.Context, name: []const u8) ![]const u8 {
        _ = name;
        return ctx.allocator().dupe(u8, "Hello from the test double!");
    }

    fn hello(ctx: *workflow.Context, name: []const u8) ![]const u8 {
        const greeting = ctx.executeActivity(Greet, .{ .start_to_close_timeout = .fromSeconds(10) }, .{name});
        return greeting.get();
    }
    const Hello = workflow.Def("Workflow", hello);

    fn other(ctx: *workflow.Context, name: []const u8) ![]const u8 {
        _ = ctx;
        _ = name;
        return "no activity here";
    }
    const Different = workflow.Def("Workflow", other);
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

test "ActivityEnvironment calls the activity directly" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const env: ActivityEnvironment = .init(testing.io);
    try testing.expectEqualStrings("Hello World!", try env.executeActivity(Fixtures.Greet, .{"World"}, arena.allocator()));
}

/// samples-go/helloworld/helloworld.json (MIT), a history recorded from the Go SDK.
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
