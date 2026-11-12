//! Worker Deployments: named sets of workers that roll out new workflow code a version at a
//! time. A worker joins one with `worker.Options.deployment`, naming the deployment and its
//! own build ID; together they are its Worker Deployment Version. The server routes a task
//! queue's workflows to the deployment's current version (and a share of them to a ramping
//! version), and each workflow's versioning behavior (`workflow.VersioningBehavior`) says
//! whether a run stays on the version it started on or moves to the current one.
//!
//! A deployment appears on the server when its first versioned worker polls. Manage it with
//! `client.Client.workerDeployment`, whose handle describes it and sets its current and
//! ramping versions:
//!
//!     var shop = try client.workerDeployment("shop");
//!     defer shop.deinit();
//!     _ = try shop.setCurrentVersion(arena, "b2", .{});
//!     const d = try shop.describe(arena);
//!     for (d.versions) |v| std.log.info("{s}: {t}", .{ v.version.build_id, v.status });

const std = @import("std");
const proto = @import("proto.zig");
const converter = @import("converter.zig");
const Allocator = std.mem.Allocator;
const Timestamp = std.Io.Timestamp;

const pb = proto.temporal.api.deployment.v1;
const enums = proto.temporal.api.enums.v1;

/// A Worker Deployment Version: a deployment and one build of its workers' code. The slices
/// are borrowed from wherever the version came from.
pub const Version = struct {
    deployment_name: []const u8,
    build_id: []const u8,

    fn fromProto(v: ?pb.WorkerDeploymentVersion) ?Version {
        const p = v orelse return null;
        if (p.build_id.len == 0) return null;
        return .{ .deployment_name = p.deployment_name, .build_id = p.build_id };
    }
};

/// Where a version stands in its deployment's rollout.
pub const VersionStatus = enum {
    unspecified,
    /// Never current or ramping.
    inactive,
    current,
    ramping,
    /// No longer current or ramping, with pinned runs still open on it.
    draining,
    /// No longer current or ramping, with no open pinned runs left: safe to shut down.
    drained,
    /// Seen by the server (a worker polled) and not yet current or ramping.
    created,

    fn fromProto(s: enums.WorkerDeploymentVersionStatus) VersionStatus {
        return switch (s) {
            .WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE => .inactive,
            .WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT => .current,
            .WORKER_DEPLOYMENT_VERSION_STATUS_RAMPING => .ramping,
            .WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING => .draining,
            .WORKER_DEPLOYMENT_VERSION_STATUS_DRAINED => .drained,
            .WORKER_DEPLOYMENT_VERSION_STATUS_CREATED => .created,
            else => .unspecified,
        };
    }
};

/// Whether pinned runs still use a version that is no longer current or ramping.
pub const DrainageStatus = enum {
    unspecified,
    draining,
    drained,

    fn fromProto(s: enums.VersionDrainageStatus) DrainageStatus {
        return switch (s) {
            .VERSION_DRAINAGE_STATUS_DRAINING => .draining,
            .VERSION_DRAINAGE_STATUS_DRAINED => .drained,
            else => .unspecified,
        };
    }
};

/// Which versions the server routes a deployment's task queues to.
pub const Routing = struct {
    /// Null routes new workflows (and auto-upgrade runs) to unversioned workers.
    current: ?Version = null,
    /// Gets `ramping_percentage` percent of what would go to `current`. Null with a
    /// non-zero percentage ramps unversioned workers; with zero, nothing ramps.
    ramping: ?Version = null,
    ramping_percentage: f32 = 0,
    current_changed_time: ?Timestamp = null,
    ramping_changed_time: ?Timestamp = null,

    fn fromProto(r: ?pb.RoutingConfig) Routing {
        const p = r orelse return .{};
        return .{
            .current = Version.fromProto(p.current_deployment_version),
            .ramping = Version.fromProto(p.ramping_deployment_version),
            .ramping_percentage = p.ramping_version_percentage,
            .current_changed_time = timestamp(p.current_version_changed_time),
            .ramping_changed_time = timestamp(p.ramping_version_changed_time),
        };
    }
};

/// One version of a deployment, as `Description.versions` lists it.
pub const VersionSummary = struct {
    version: Version,
    status: VersionStatus,
    drainage_status: DrainageStatus,
    create_time: ?Timestamp,
};

/// A deployment, from `WorkerDeploymentHandle.describe`. Memory from the call's arena.
pub const Description = struct {
    name: []const u8,
    create_time: ?Timestamp,
    routing: Routing,
    /// The versions whose workers have polled, oldest first.
    versions: []const VersionSummary,
    last_modifier_identity: []const u8,
    /// Only this identity may change the deployment's routing, when set (see
    /// `WorkerDeploymentHandle.setManagerIdentity`).
    manager_identity: []const u8,
    /// Pass to a routing change's options to apply it only if nothing changed the
    /// deployment since this description.
    conflict_token: []const u8,
    /// Whether the latest routing change has reached all of the deployment's task queues;
    /// until it has, some tasks still go where the previous routing sent them.
    routing_update: RoutingUpdateState,

    pub const RoutingUpdateState = enum { unspecified, in_progress, completed };

    pub fn fromProto(arena: Allocator, conflict_token: []const u8, info: pb.WorkerDeploymentInfo) error{OutOfMemory}!Description {
        const versions = try arena.alloc(VersionSummary, info.version_summaries.items.len);
        var n: usize = 0;
        for (info.version_summaries.items) |s| {
            const v = Version.fromProto(s.deployment_version) orelse continue;
            versions[n] = .{
                .version = v,
                .status = .fromProto(s.status),
                .drainage_status = .fromProto(s.drainage_status),
                .create_time = timestamp(s.create_time),
            };
            n += 1;
        }
        return .{
            .name = info.name,
            .create_time = timestamp(info.create_time),
            .routing = .fromProto(info.routing_config),
            .versions = versions[0..n],
            .last_modifier_identity = info.last_modifier_identity,
            .manager_identity = info.manager_identity,
            .conflict_token = conflict_token,
            .routing_update = switch (info.routing_config_update_state) {
                .ROUTING_CONFIG_UPDATE_STATE_IN_PROGRESS => .in_progress,
                .ROUTING_CONFIG_UPDATE_STATE_COMPLETED => .completed,
                else => .unspecified,
            },
        };
    }
};

/// A deployment as `Client.listWorkerDeployments` lists it. Memory from the arena passed to
/// the iterator's `next`.
pub const ListEntry = struct {
    name: []const u8,
    create_time: ?Timestamp,
    routing: Routing,

    pub fn fromProto(s: proto.temporal.api.workflowservice.v1.ListWorkerDeploymentsResponse.WorkerDeploymentSummary) ListEntry {
        return .{ .name = s.name, .create_time = timestamp(s.create_time), .routing = .fromProto(s.routing_config) };
    }
};

/// The kind of task a task queue serves.
pub const TaskQueueType = enum {
    unspecified,
    workflow,
    activity,
    nexus,

    fn fromProto(t: enums.TaskQueueType) TaskQueueType {
        return switch (t) {
            .TASK_QUEUE_TYPE_WORKFLOW => .workflow,
            .TASK_QUEUE_TYPE_ACTIVITY => .activity,
            .TASK_QUEUE_TYPE_NEXUS => .nexus,
            else => .unspecified,
        };
    }
};

/// A version, from `WorkerDeploymentHandle.describeVersion`. Memory from the call's arena.
pub const VersionDescription = struct {
    version: Version,
    status: VersionStatus,
    create_time: ?Timestamp,
    current_since_time: ?Timestamp,
    ramping_since_time: ?Timestamp,
    /// While ramping.
    ramp_percentage: f32,
    /// The task queues the version's workers poll.
    task_queues: []const TaskQueue,
    drainage_status: DrainageStatus,
    /// Values set with `WorkerDeploymentHandle.updateVersionMetadata`, sorted by name. Read
    /// one with `converter.fieldValue`.
    metadata: []const converter.Field,

    pub const TaskQueue = struct { name: []const u8, type: TaskQueueType };

    pub fn fromProto(arena: Allocator, info: pb.WorkerDeploymentVersionInfo) error{ OutOfMemory, InvalidResponse }!VersionDescription {
        const queues = try arena.alloc(TaskQueue, info.task_queue_infos.items.len);
        for (info.task_queue_infos.items, queues) |q, *out| out.* = .{ .name = q.name, .type = .fromProto(q.type) };
        var metadata: []converter.Field = &.{};
        if (info.metadata) |m| {
            metadata = try arena.alloc(converter.Field, m.entries.items.len);
            for (m.entries.items, metadata) |e, *f| f.* = .{ .name = e.key, .value = e.value orelse .{} };
            std.mem.sort(converter.Field, metadata, {}, struct {
                fn lessThan(_: void, x: converter.Field, y: converter.Field) bool {
                    return std.mem.lessThan(u8, x.name, y.name);
                }
            }.lessThan);
        }
        return .{
            .version = Version.fromProto(info.deployment_version) orelse return error.InvalidResponse,
            .status = .fromProto(info.status),
            .create_time = timestamp(info.create_time),
            .current_since_time = timestamp(info.current_since_time),
            .ramping_since_time = timestamp(info.ramping_since_time),
            .ramp_percentage = info.ramp_percentage,
            .task_queues = queues,
            .drainage_status = if (info.drainage_info) |d| .fromProto(d.status) else .unspecified,
            .metadata = metadata,
        };
    }
};

/// Options of a routing change (`setCurrentVersion`, `setRampingVersion`).
pub const RoutingOptions = struct {
    /// From `Description.conflict_token` (or an earlier `RoutingChange`): the server then
    /// refuses the change (`error.RpcFailed`, logged) if the deployment changed since.
    /// Empty applies it anyway.
    conflict_token: []const u8 = "",
    /// Change the routing even if the new version's workers don't poll every task queue the
    /// current version's did. The server otherwise refuses, to catch a worker that dropped a
    /// task queue or hasn't started yet.
    ignore_missing_task_queues: bool = false,
    /// Change the routing even if no worker of the new version has polled yet.
    allow_no_pollers: bool = false,
};

/// Options of `WorkerDeploymentHandle.deleteVersion`.
pub const DeleteVersionOptions = struct {
    /// Delete the version even while pinned runs still use it.
    skip_drainage: bool = false,
};

/// Options of `WorkerDeploymentHandle.setManagerIdentity`.
pub const ManagerOptions = struct {
    /// As `RoutingOptions.conflict_token`.
    conflict_token: []const u8 = "",
};

/// What a routing change replaced. Memory from the call's arena.
pub const RoutingChange = struct {
    /// The version that was current (or ramping); null for unversioned workers.
    previous: ?Version,
    /// The previous ramp percentage, for `setRampingVersion`.
    previous_percentage: f32 = 0,
    /// For a further change that must not race another client's.
    conflict_token: []const u8,

    pub fn fromVersion(conflict_token: []const u8, previous: ?pb.WorkerDeploymentVersion, previous_percentage: f32) RoutingChange {
        return .{ .previous = Version.fromProto(previous), .previous_percentage = previous_percentage, .conflict_token = conflict_token };
    }
};

fn timestamp(t: ?proto.google.protobuf.Timestamp) ?Timestamp {
    const p = t orelse return null;
    // The server reports an unset time as zero or as 0001-01-01T00:00:00Z.
    if ((p.seconds == 0 and p.nanos == 0) or p.seconds <= -62_135_596_800) return null;
    return .{ .nanoseconds = @as(i96, p.seconds) * std.time.ns_per_s + p.nanos };
}

test "statuses map from the server's values" {
    try std.testing.expectEqual(VersionStatus.drained, VersionStatus.fromProto(.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINED));
    try std.testing.expectEqual(VersionStatus.unspecified, VersionStatus.fromProto(@fromBackingInt(@intCast(99))));
    try std.testing.expectEqual(DrainageStatus.draining, DrainageStatus.fromProto(.VERSION_DRAINAGE_STATUS_DRAINING));
    // An empty routing version means unversioned workers.
    try std.testing.expectEqual(@as(?Version, null), Version.fromProto(.{ .deployment_name = "shop" }));
}
