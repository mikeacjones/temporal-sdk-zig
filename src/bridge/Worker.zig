//! An sdk-core worker: it polls a task queue and hands out workflow activations, activity
//! tasks, and Nexus tasks as serialized protobuf, and takes back serialized completions.
//!
//! Lifecycle:
//!
//! 1. `init`, then optionally `validate` (checks the namespace and task queue with the server).
//! 2. Poll and complete. Every activation and task that a poll returns must be completed,
//!    including cache-eviction activations, or shutdown will not finish.
//! 3. `initiateShutdown`. Keep polling each enabled task type until its poll returns null
//!    (shut down), completing whatever arrives meanwhile.
//! 4. `finalizeShutdown` once no poll or completion is in flight, then `deinit`.
//!
//! Polls wait without cancellation: abandoning one could drop an activation the core then
//! waits on forever. Stop polling with `initiateShutdown` instead.

const Worker = @This();

const std = @import("std");
const c = @import("core_c");
const bytes = @import("bytes.zig");
const Runtime = @import("Runtime.zig");
const Connection = @import("Connection.zig");
const Completion = @import("completion.zig").Completion;
const Allocator = std.mem.Allocator;
const Io = std.Io;

ptr: *c.TemporalCoreWorker,
runtime: Runtime,

/// Core-level worker options. The public worker API maps its options onto these.
pub const Options = struct {
    namespace: []const u8,
    task_queue: []const u8,
    versioning: Versioning = .{ .none = .{} },
    /// Empty uses the connection's identity.
    identity_override: []const u8 = "",
    max_cached_workflows: u32 = 1000,
    tuner: Tuner = .{},
    task_types: TaskTypes = .{},
    sticky_queue_schedule_to_start_timeout_ms: u64 = 10_000,
    max_heartbeat_throttle_interval_ms: u64 = 60_000,
    default_heartbeat_throttle_interval_ms: u64 = 30_000,
    /// 0 means unlimited.
    max_activities_per_second: f64 = 0,
    /// 0 means unlimited.
    max_task_queue_activities_per_second: f64 = 0,
    /// How long running activities get after shutdown starts before they are canceled.
    graceful_shutdown_period_ms: u64 = 0,
    workflow_task_pollers: PollerBehavior = .{ .simple_maximum = 5 },
    nonsticky_to_sticky_poll_ratio: f32 = 0.2,
    activity_task_pollers: PollerBehavior = .{ .simple_maximum = 5 },
    nexus_task_pollers: PollerBehavior = .{ .simple_maximum = 5 },
    /// Fail the workflow, not just the task, on nondeterminism.
    nondeterminism_as_workflow_fail: bool = false,
    nondeterminism_as_workflow_fail_for_types: []const []const u8 = &.{},
    plugins: []const Plugin = &.{},
    storage_drivers: []const []const u8 = &.{},
    disable_payload_error_limit: bool = false,
    /// 0 disables eager activity execution.
    max_eager_activity_reservations_per_workflow_task: u32 = 3,

    pub const Versioning = union(enum) {
        none: struct { build_id: []const u8 = "" },
        deployment: struct {
            deployment_name: []const u8,
            build_id: []const u8,
            use_worker_versioning: bool = true,
            /// temporal.api.enums.v1.VersioningBehavior; 0 is unspecified.
            default_versioning_behavior: i32 = 0,
        },
        legacy_build_id: struct { build_id: []const u8 },
    };

    pub const Tuner = struct {
        workflow: SlotSupplier = .{ .fixed = 100 },
        activity: SlotSupplier = .{ .fixed = 100 },
        local_activity: SlotSupplier = .{ .fixed = 100 },
        nexus: SlotSupplier = .{ .fixed = 100 },
    };

    pub const SlotSupplier = union(enum) {
        fixed: usize,
        resource_based: struct {
            minimum_slots: usize,
            maximum_slots: usize,
            ramp_throttle_ms: u64,
            target_memory_usage: f64,
            target_cpu_usage: f64,
        },
        custom: *const c.TemporalCoreCustomSlotSupplierCallbacks,
    };

    pub const TaskTypes = struct {
        workflows: bool = true,
        local_activities: bool = true,
        remote_activities: bool = true,
        nexus: bool = true,
    };

    pub const PollerBehavior = union(enum) {
        simple_maximum: usize,
        autoscaling: struct { minimum: usize, maximum: usize, initial: usize },
    };

    pub const Plugin = struct { name: []const u8, version: []const u8 = "" };
};

pub const InitError = error{ OutOfMemory, InvalidOptions, WorkerInitFailed };

/// Creates a worker on `connection`. The core copies the options. On
/// `error.WorkerInitFailed`, `failure` (if given) receives the core's message.
pub fn init(gpa: Allocator, connection: Connection, options: Options, failure: ?*?[]u8) InitError!Worker {
    var arena_state: std.heap.ArenaAllocator = .init(gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var core: c.TemporalCoreWorkerOptions = std.mem.zeroes(c.TemporalCoreWorkerOptions);
    core.namespace_ = bytes.ref(options.namespace);
    core.task_queue = bytes.ref(options.task_queue);
    switch (options.versioning) {
        .none => |v| {
            core.versioning_strategy.tag = c.TemporalCoreWorkerVersioningStrategy_None;
            core.versioning_strategy.unnamed_0.unnamed_0.none = .{ .build_id = bytes.ref(v.build_id) };
        },
        .deployment => |v| {
            core.versioning_strategy.tag = c.TemporalCoreWorkerVersioningStrategy_DeploymentBased;
            core.versioning_strategy.unnamed_0.unnamed_1.deployment_based = .{
                .version = .{ .deployment_name = bytes.ref(v.deployment_name), .build_id = bytes.ref(v.build_id) },
                .use_worker_versioning = v.use_worker_versioning,
                .default_versioning_behavior = v.default_versioning_behavior,
            };
        },
        .legacy_build_id => |v| {
            core.versioning_strategy.tag = c.TemporalCoreWorkerVersioningStrategy_LegacyBuildIdBased;
            core.versioning_strategy.unnamed_0.unnamed_2.legacy_build_id_based = .{ .build_id = bytes.ref(v.build_id) };
        },
    }
    core.identity_override = bytes.ref(options.identity_override);
    core.max_cached_workflows = options.max_cached_workflows;
    core.tuner = .{
        .workflow_slot_supplier = slotSupplier(options.tuner.workflow),
        .activity_slot_supplier = slotSupplier(options.tuner.activity),
        .local_activity_slot_supplier = slotSupplier(options.tuner.local_activity),
        .nexus_task_slot_supplier = slotSupplier(options.tuner.nexus),
    };
    core.task_types = .{
        .enable_workflows = options.task_types.workflows,
        .enable_local_activities = options.task_types.local_activities,
        .enable_remote_activities = options.task_types.remote_activities,
        .enable_nexus = options.task_types.nexus,
    };
    core.sticky_queue_schedule_to_start_timeout_millis = options.sticky_queue_schedule_to_start_timeout_ms;
    core.max_heartbeat_throttle_interval_millis = options.max_heartbeat_throttle_interval_ms;
    core.default_heartbeat_throttle_interval_millis = options.default_heartbeat_throttle_interval_ms;
    core.max_activities_per_second = options.max_activities_per_second;
    core.max_task_queue_activities_per_second = options.max_task_queue_activities_per_second;
    core.graceful_shutdown_period_millis = options.graceful_shutdown_period_ms;
    core.workflow_task_poller_behavior = try pollerBehavior(arena, options.workflow_task_pollers);
    core.nonsticky_to_sticky_poll_ratio = options.nonsticky_to_sticky_poll_ratio;
    core.activity_task_poller_behavior = try pollerBehavior(arena, options.activity_task_pollers);
    core.nexus_task_poller_behavior = try pollerBehavior(arena, options.nexus_task_pollers);
    core.nondeterminism_as_workflow_fail = options.nondeterminism_as_workflow_fail;
    core.nondeterminism_as_workflow_fail_for_types = try refArray(arena, options.nondeterminism_as_workflow_fail_for_types);
    const plugins = try arena.alloc(c.TemporalCorePluginInfo, options.plugins.len);
    for (options.plugins, plugins) |p, *out| out.* = .{ .name = bytes.ref(p.name), .version = bytes.ref(p.version) };
    core.plugins = .{ .data = if (plugins.len == 0) null else plugins.ptr, .size = plugins.len };
    core.storage_drivers = try refArray(arena, options.storage_drivers);
    core.disable_payload_error_limit = options.disable_payload_error_limit;
    core.max_eager_activity_reservations_per_workflow_task = options.max_eager_activity_reservations_per_workflow_task;

    const result = c.temporal_core_worker_new(connection.ptr, &core);
    if (bytes.Owned.take(connection.runtime.ptr, result.fail)) |fail| {
        defer fail.deinit();
        if (result.worker) |w| c.temporal_core_worker_free(w);
        if (failure) |f| f.* = fail.dupe(gpa) catch null;
        return error.WorkerInitFailed;
    }
    return .{ .ptr = result.worker orelse return error.WorkerInitFailed, .runtime = connection.runtime };
}

/// Frees the worker. Call `finalizeShutdown` first if it was used.
pub fn deinit(w: Worker) void {
    c.temporal_core_worker_free(w.ptr);
}

/// Checks the namespace and task queue with the server. On `error.ValidationFailed`,
/// `failure` (if given) receives the reason.
pub fn validate(w: Worker, gpa: Allocator, io: Io, failure: ?*?[]u8) error{ OutOfMemory, ValidationFailed }!void {
    const done = try Completion(?bytes.Owned).create(gpa, io, freeOwned);
    c.temporal_core_worker_validate(w.ptr, done, onDone);
    return finishDone(gpa, done.waitUncancelable(), failure, error.ValidationFailed);
}

/// Serialized poll result, owned by the caller (free with `deinit`).
pub const Polled = bytes.Owned;

pub const PollError = error{ OutOfMemory, PollFailed };

/// Waits for the next coresdk.workflow_activation.WorkflowActivation. Returns null once the
/// worker has shut down.
pub fn pollWorkflowActivation(w: Worker, gpa: Allocator, io: Io, failure: ?*?[]u8) PollError!?Polled {
    return w.poll(gpa, io, c.temporal_core_worker_poll_workflow_activation, failure);
}

/// Waits for the next coresdk.activity_task.ActivityTask. Returns null once shut down.
pub fn pollActivityTask(w: Worker, gpa: Allocator, io: Io, failure: ?*?[]u8) PollError!?Polled {
    return w.poll(gpa, io, c.temporal_core_worker_poll_activity_task, failure);
}

/// Waits for the next coresdk.nexus.NexusTask. Returns null once shut down.
pub fn pollNexusTask(w: Worker, gpa: Allocator, io: Io, failure: ?*?[]u8) PollError!?Polled {
    return w.poll(gpa, io, c.temporal_core_worker_poll_nexus_task, failure);
}

const PollFn = *const fn (?*c.TemporalCoreWorker, ?*anyopaque, c.TemporalCoreWorkerPollCallback) callconv(.c) void;

fn poll(w: Worker, gpa: Allocator, io: Io, poll_fn: PollFn, failure: ?*?[]u8) PollError!?Polled {
    const done = try Completion(PollResult).create(gpa, io, PollResult.free);
    poll_fn(w.ptr, done, onPoll);
    const result = done.waitUncancelable();
    if (result.fail) |fail| {
        defer fail.deinit();
        if (result.success) |s| s.deinit();
        if (failure) |f| f.* = fail.dupe(gpa) catch null;
        return error.PollFailed;
    }
    return result.success;
}

pub const CompleteError = error{ OutOfMemory, CompletionFailed };

/// Sends a serialized coresdk.workflow_completion.WorkflowActivationCompletion. The bytes are
/// only read during the call.
pub fn completeWorkflowActivation(w: Worker, gpa: Allocator, io: Io, completion: []const u8, failure: ?*?[]u8) CompleteError!void {
    return w.complete(gpa, io, c.temporal_core_worker_complete_workflow_activation, completion, failure);
}

/// Sends a serialized coresdk.ActivityTaskCompletion.
pub fn completeActivityTask(w: Worker, gpa: Allocator, io: Io, completion: []const u8, failure: ?*?[]u8) CompleteError!void {
    return w.complete(gpa, io, c.temporal_core_worker_complete_activity_task, completion, failure);
}

/// Sends a serialized coresdk.nexus.NexusTaskCompletion.
pub fn completeNexusTask(w: Worker, gpa: Allocator, io: Io, completion: []const u8, failure: ?*?[]u8) CompleteError!void {
    return w.complete(gpa, io, c.temporal_core_worker_complete_nexus_task, completion, failure);
}

const CompleteFn = *const fn (?*c.TemporalCoreWorker, c.TemporalCoreByteArrayRef, ?*anyopaque, c.TemporalCoreWorkerCallback) callconv(.c) void;

fn complete(w: Worker, gpa: Allocator, io: Io, complete_fn: CompleteFn, completion: []const u8, failure: ?*?[]u8) CompleteError!void {
    const done = try Completion(?bytes.Owned).create(gpa, io, freeOwned);
    complete_fn(w.ptr, bytes.ref(completion), done, onDone);
    // Completions are always awaited: the core must learn the outcome of every task.
    return finishDone(gpa, done.waitUncancelable(), failure, error.CompletionFailed);
}

/// Records a serialized coresdk.ActivityHeartbeat. The core batches and throttles heartbeats.
pub fn recordActivityHeartbeat(w: Worker, gpa: Allocator, heartbeat: []const u8, failure: ?*?[]u8) error{HeartbeatFailed}!void {
    const fail = bytes.Owned.take(w.runtime.ptr, c.temporal_core_worker_record_activity_heartbeat(w.ptr, bytes.ref(heartbeat))) orelse return;
    defer fail.deinit();
    if (failure) |f| f.* = fail.dupe(gpa) catch null;
    return error.HeartbeatFailed;
}

/// Asks the core to evict a workflow run from its cache; an eviction activation follows.
pub fn requestWorkflowEviction(w: Worker, run_id: []const u8) void {
    c.temporal_core_worker_request_workflow_eviction(w.ptr, bytes.ref(run_id));
}

/// Points the worker at a different connection (for example after rotating credentials).
pub fn replaceClient(w: Worker, gpa: Allocator, connection: Connection, failure: ?*?[]u8) error{ReplaceFailed}!void {
    const fail = bytes.Owned.take(w.runtime.ptr, c.temporal_core_worker_replace_client(w.ptr, connection.ptr)) orelse return;
    defer fail.deinit();
    if (failure) |f| f.* = fail.dupe(gpa) catch null;
    return error.ReplaceFailed;
}

/// Starts shutdown: polls drain and then return null.
pub fn initiateShutdown(w: Worker) void {
    c.temporal_core_worker_initiate_shutdown(w.ptr);
}

/// Waits for shutdown to finish. Every poll must have returned null and every completion must
/// have finished first; otherwise this fails.
pub fn finalizeShutdown(w: Worker, gpa: Allocator, io: Io, failure: ?*?[]u8) error{ OutOfMemory, ShutdownFailed }!void {
    const done = try Completion(?bytes.Owned).create(gpa, io, freeOwned);
    c.temporal_core_worker_finalize_shutdown(w.ptr, done, onDone);
    return finishDone(gpa, done.waitUncancelable(), failure, error.ShutdownFailed);
}

const PollResult = struct {
    success: ?bytes.Owned,
    fail: ?bytes.Owned,

    fn free(r: PollResult) void {
        if (r.success) |s| s.deinit();
        if (r.fail) |f| f.deinit();
    }
};

fn onPoll(user_data: ?*anyopaque, success: ?*const c.TemporalCoreByteArray, fail: ?*const c.TemporalCoreByteArray) callconv(.c) void {
    const done: *Completion(PollResult) = @ptrCast(@alignCast(user_data.?));
    done.complete(.{ .success = bytes.Owned.take(null, success), .fail = bytes.Owned.take(null, fail) });
}

fn onDone(user_data: ?*anyopaque, fail: ?*const c.TemporalCoreByteArray) callconv(.c) void {
    const done: *Completion(?bytes.Owned) = @ptrCast(@alignCast(user_data.?));
    done.complete(bytes.Owned.take(null, fail));
}

fn freeOwned(o: ?bytes.Owned) void {
    if (o) |v| v.deinit();
}

fn finishDone(gpa: Allocator, fail: ?bytes.Owned, failure: ?*?[]u8, comptime err: anytype) @TypeOf(err)!void {
    const f = fail orelse return;
    defer f.deinit();
    if (failure) |out| out.* = f.dupe(gpa) catch null;
    return err;
}

fn slotSupplier(s: Options.SlotSupplier) c.TemporalCoreSlotSupplier {
    var out = std.mem.zeroes(c.TemporalCoreSlotSupplier);
    switch (s) {
        .fixed => |n| {
            out.tag = c.TemporalCoreSlotSupplier_FixedSize;
            out.unnamed_0.unnamed_0.fixed_size = .{ .num_slots = n };
        },
        .resource_based => |r| {
            out.tag = c.TemporalCoreSlotSupplier_ResourceBased;
            out.unnamed_0.unnamed_1.resource_based = .{
                .minimum_slots = r.minimum_slots,
                .maximum_slots = r.maximum_slots,
                .ramp_throttle_ms = r.ramp_throttle_ms,
                .tuner_options = .{ .target_memory_usage = r.target_memory_usage, .target_cpu_usage = r.target_cpu_usage },
            };
        },
        .custom => |callbacks| {
            out.tag = c.TemporalCoreSlotSupplier_Custom;
            out.unnamed_0.unnamed_2.custom = .{ ._0 = callbacks };
        },
    }
    return out;
}

fn pollerBehavior(arena: Allocator, p: Options.PollerBehavior) error{OutOfMemory}!c.TemporalCorePollerBehavior {
    switch (p) {
        .simple_maximum => |n| {
            const v = try arena.create(c.TemporalCorePollerBehaviorSimpleMaximum);
            v.* = .{ .simple_maximum = n };
            return .{ .simple_maximum = v, .autoscaling = null };
        },
        .autoscaling => |a| {
            const v = try arena.create(c.TemporalCorePollerBehaviorAutoscaling);
            v.* = .{ .minimum = a.minimum, .maximum = a.maximum, .initial = a.initial };
            return .{ .simple_maximum = null, .autoscaling = v };
        },
    }
}

fn refArray(arena: Allocator, items: []const []const u8) error{OutOfMemory}!bytes.RefArray {
    const refs = try arena.alloc(bytes.Ref, items.len);
    for (items, refs) |s, *r| r.* = bytes.ref(s);
    return bytes.refArray(refs);
}

// ---------------------------------------------------------------- tests

const testing = std.testing;
const EphemeralServer = @import("EphemeralServer.zig");
const proto = @import("../proto.zig");
const wfs = proto.temporal.api.workflowservice.v1;
const activation = proto.coresdk.workflow_activation;
const completion_pb = proto.coresdk.workflow_completion;

fn encode(msg: anytype, a: Allocator) ![]const u8 {
    var aw: std.Io.Writer.Allocating = .init(a);
    try msg.encode(&aw.writer, a);
    return aw.written();
}

fn decode(comptime T: type, data: []const u8, a: Allocator) !T {
    var r: std.Io.Reader = .fixed(data);
    return T.decode(&r, a);
}

test "a workflow started by RPC is polled, completed, and shut down through the core" {
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    const rt = try Runtime.init(testing.allocator, .{}, null);
    defer rt.deinit();
    var server = try EphemeralServer.startDevServer(testing.allocator, testing.io, rt, .{}, null);
    defer server.shutdown(testing.io, null) catch {};
    const url = try std.fmt.allocPrint(a, "http://{s}", .{server.target});
    const conn = try Connection.connect(testing.allocator, testing.io, rt, .{ .target_url = url, .identity = "worker-test@zig" }, null);
    defer conn.deinit();

    const task_queue = "bridge-worker-test";
    var failure: ?[]u8 = null;
    defer if (failure) |f| testing.allocator.free(f);
    const worker = Worker.init(testing.allocator, conn, .{
        .namespace = "default",
        .task_queue = task_queue,
        .task_types = .{ .local_activities = false, .remote_activities = false, .nexus = false },
        .workflow_task_pollers = .{ .simple_maximum = 2 },
    }, &failure) catch |err| {
        std.debug.print("worker init failed: {s}\n", .{failure orelse "(no message)"});
        return err;
    };
    defer worker.deinit();
    try worker.validate(testing.allocator, testing.io, &failure);

    // Start a workflow directly through the WorkflowService.
    const start = try conn.call(testing.allocator, testing.io, .workflow, "StartWorkflowExecution", try encode(wfs.StartWorkflowExecutionRequest{
        .namespace = "default",
        .workflow_id = "bridge-worker-wf",
        .workflow_type = .{ .name = "BridgeTest" },
        .task_queue = .{ .name = task_queue, .kind = .TASK_QUEUE_KIND_NORMAL },
        .identity = "worker-test@zig",
        .request_id = "6f1c2a0e-4b5d-4c3e-9f1a-0d2b3c4e5f60",
    }, a), .{}, null);
    testing.allocator.free(start);

    // The first activation initializes the workflow; complete it right away.
    const first = (try worker.pollWorkflowActivation(testing.allocator, testing.io, &failure)).?;
    defer first.deinit();
    const act = try decode(activation.WorkflowActivation, first.bytes(), a);
    try testing.expectEqualStrings("BridgeTest", act.jobs.items[0].variant.?.initialize_workflow.workflow_type);

    var result: proto.temporal.api.common.v1.Payload = .{ .data = "\"done\"" };
    try result.metadata.append(a, .{ .key = "encoding", .value = "json/plain" });
    var success: completion_pb.Success = .{};
    try success.commands.append(a, .{ .variant = .{ .complete_workflow_execution = .{ .result = result } } });
    try worker.completeWorkflowActivation(testing.allocator, testing.io, try encode(completion_pb.WorkflowActivationCompletion{
        .run_id = act.run_id,
        .status = .{ .successful = success },
    }, a), &failure);

    // The server now reports the workflow as completed.
    const described = try conn.call(testing.allocator, testing.io, .workflow, "DescribeWorkflowExecution", try encode(wfs.DescribeWorkflowExecutionRequest{
        .namespace = "default",
        .execution = .{ .workflow_id = "bridge-worker-wf" },
    }, a), .{}, null);
    defer testing.allocator.free(described);
    const info = (try decode(wfs.DescribeWorkflowExecutionResponse, described, a)).workflow_execution_info.?;
    try testing.expectEqual(proto.temporal.api.enums.v1.WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_COMPLETED, info.status);

    // Shut down: keep polling, completing evictions, until the poll reports shutdown.
    worker.initiateShutdown();
    while (try worker.pollWorkflowActivation(testing.allocator, testing.io, &failure)) |polled| {
        defer polled.deinit();
        const next = try decode(activation.WorkflowActivation, polled.bytes(), a);
        try worker.completeWorkflowActivation(testing.allocator, testing.io, try encode(completion_pb.WorkflowActivationCompletion{
            .run_id = next.run_id,
            .status = .{ .successful = .{} },
        }, a), &failure);
    }
    try worker.finalizeShutdown(testing.allocator, testing.io, &failure);
}

test "invalid worker options fail with the core's message" {
    const rt = try Runtime.init(testing.allocator, .{}, null);
    defer rt.deinit();
    var server = try EphemeralServer.startDevServer(testing.allocator, testing.io, rt, .{}, null);
    defer server.shutdown(testing.io, null) catch {};
    const url = try std.fmt.allocPrint(testing.allocator, "http://{s}", .{server.target});
    defer testing.allocator.free(url);
    const conn = try Connection.connect(testing.allocator, testing.io, rt, .{ .target_url = url, .identity = "test" }, null);
    defer conn.deinit();

    var failure: ?[]u8 = null;
    defer if (failure) |f| testing.allocator.free(f);
    try testing.expectError(error.WorkerInitFailed, Worker.init(testing.allocator, conn, .{
        .namespace = "default",
        .task_queue = "q",
        .versioning = .{ .deployment = .{ .deployment_name = "d", .build_id = "1", .default_versioning_behavior = 99 } },
    }, &failure));
    try testing.expect(failure != null and failure.?.len > 0);
}
