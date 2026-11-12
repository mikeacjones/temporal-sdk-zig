//! Schedules: the server starts a workflow on a calendar, at intervals, or by cron
//! expression, until the schedule is paused, runs out of actions, or is deleted.
//!
//!     const schedule = temporal.schedule;
//!     var handle = try client.createSchedule("nightly-report", .{ .schedule = .{
//!         .action = try schedule.startWorkflow(arena, Report, .{"all"}, .{ .id = "report", .task_queue = "reports" }),
//!         .spec = .{ .calendars = &.{.{ .hour = &.{.{ .start = 2 }} }}, .time_zone_name = "Europe/Paris" },
//!     } });
//!     defer handle.deinit();
//!     try handle.trigger(.unspecified); // run it once now as well
//!
//! Manage an existing schedule through `Client.getScheduleHandle`, and find schedules with
//! `Client.listSchedules`. A workflow a schedule started reads when it was scheduled, and
//! by which schedule, from its search attributes with the keys `scheduled_start_time` and
//! `scheduled_by_id`.

const std = @import("std");
const proto = @import("proto.zig");
const converter = @import("converter.zig");
const search_attributes = @import("search_attributes.zig");
const client_mod = @import("client.zig");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const Duration = Io.Duration;

const pb = proto.temporal.api.schedule.v1;
const wfs = proto.temporal.api.workflowservice.v1;
const common = proto.temporal.api.common.v1;
const log = std.log.scoped(.temporal);

/// The search attribute the server sets on each workflow a schedule starts: the time the
/// action was scheduled for (before jitter).
pub const scheduled_start_time = search_attributes.key(.datetime, "TemporalScheduledStartTime");
/// The search attribute the server sets on each workflow a schedule starts: the schedule's
/// ID.
pub const scheduled_by_id = search_attributes.key(.keyword, "TemporalScheduledById");

/// A schedule: what it does, when, and how. Slices are borrowed; the client copies what it
/// sends.
pub const Schedule = struct {
    action: Action,
    spec: Spec,
    policies: Policies = .{},
    state: State = .{},

    /// The schedule as the proto the server reads, allocated from `arena`. Fields of `base`
    /// (the schedule as described) that `Schedule` doesn't carry, such as a workflow's retry
    /// policy or header, are kept, so a read-modify-write doesn't drop them.
    pub fn toProto(s: Schedule, arena: Allocator, base: ?pb.Schedule) error{OutOfMemory}!pb.Schedule {
        const b = base orelse pb.Schedule{};
        var out = b;
        out.spec = try s.spec.toProto(arena, b.spec orelse .{});
        out.action = switch (s.action) {
            .start_workflow => |sw| .{ .start_workflow = try sw.toProto(arena, if (b.action) |ba| ba.start_workflow else null) },
        };
        var policies = b.policies orelse pb.SchedulePolicies{};
        policies.overlap_policy = @fromBackingInt(@backingInt(s.policies.overlap));
        policies.catchup_window = protoDuration(s.policies.catchup_window);
        policies.pause_on_failure = s.policies.pause_on_failure;
        policies.keep_original_workflow_id = s.policies.keep_original_workflow_id;
        out.policies = policies;
        var state = b.state orelse pb.ScheduleState{};
        state.notes = s.state.note;
        state.paused = s.state.paused;
        state.limited_actions = s.state.remaining_actions != null;
        state.remaining_actions = if (s.state.remaining_actions) |n| std.math.cast(i64, n) orelse std.math.maxInt(i64) else 0;
        out.state = state;
        return out;
    }

    /// Lifts a schedule the server returned. Slices borrow `s` or come from `arena`. Returns
    /// `error.InvalidResponse` (logged) for an action this SDK doesn't know.
    pub fn fromProto(arena: Allocator, s: pb.Schedule) error{ OutOfMemory, InvalidResponse }!Schedule {
        const action = s.action orelse pb.ScheduleAction{};
        const sw = action.start_workflow orelse {
            log.err("the schedule's action is not a workflow start, the only action this SDK supports", .{});
            return error.InvalidResponse;
        };
        const policies = s.policies orelse pb.SchedulePolicies{};
        const state = s.state orelse pb.ScheduleState{};
        return .{
            .action = .{ .start_workflow = try .fromProto(arena, sw) },
            .spec = try .fromProto(arena, s.spec orelse .{}),
            .policies = .{
                .overlap = @fromBackingInt(@backingInt(policies.overlap_policy)),
                .catchup_window = duration(policies.catchup_window),
                .pause_on_failure = policies.pause_on_failure,
                .keep_original_workflow_id = policies.keep_original_workflow_id,
            },
            .state = .{
                .note = state.notes,
                .paused = state.paused,
                .remaining_actions = if (state.limited_actions) std.math.cast(u64, state.remaining_actions) orelse 0 else null,
            },
        };
    }
};

/// When a schedule takes its action: the union of every calendar, interval and cron
/// expression, minus the times `skip` matches, between `start_at` and `end_at`.
pub const Spec = struct {
    calendars: []const Calendar = &.{},
    intervals: []const Interval = &.{},
    /// Cron expressions, such as "0 12 * * MON-FRI" or "@hourly", optionally preceded by
    /// "CRON_TZ=<zone>". The server turns them into calendars (and intervals), so a
    /// described schedule lists them there instead.
    cron_expressions: []const []const u8 = &.{},
    /// Times to leave out, even when another field matches them.
    skip: []const Calendar = &.{},
    /// No action before this time.
    start_at: ?Io.Timestamp = null,
    /// No action after this time.
    end_at: ?Io.Timestamp = null,
    /// Each action is delayed by a random amount up to this, to spread out the load of
    /// many schedules that match the same time.
    jitter: ?Duration = null,
    /// The IANA time zone calendars and cron expressions are read in, such as
    /// "America/New_York". Empty is UTC.
    time_zone_name: []const u8 = "",

    fn toProto(s: Spec, arena: Allocator, base: pb.ScheduleSpec) error{OutOfMemory}!pb.ScheduleSpec {
        var out = base;
        out.structured_calendar = try calendarsToProto(arena, s.calendars);
        out.exclude_structured_calendar = try calendarsToProto(arena, s.skip);
        // Unstructured calendars only exist on input; the server compiles them into the
        // structured ones a description returns.
        out.calendar = .empty;
        out.exclude_calendar = .empty;
        out.cron_string = .fromOwnedSlice(try arena.dupe([]const u8, s.cron_expressions));
        const intervals = try arena.alloc(pb.IntervalSpec, s.intervals.len);
        for (s.intervals, intervals) |i, *o| o.* = .{ .interval = protoDuration(i.every), .phase = protoDuration(i.offset) };
        out.interval = .fromOwnedSlice(intervals);
        out.start_time = protoTimestamp(s.start_at);
        out.end_time = protoTimestamp(s.end_at);
        out.jitter = protoDuration(s.jitter);
        out.timezone_name = s.time_zone_name;
        return out;
    }

    fn fromProto(arena: Allocator, s: pb.ScheduleSpec) error{OutOfMemory}!Spec {
        const intervals = try arena.alloc(Interval, s.interval.items.len);
        for (s.interval.items, intervals) |i, *o| o.* = .{ .every = duration(i.interval) orelse .zero, .offset = duration(i.phase) orelse .zero };
        return .{
            .calendars = try calendarsFromProto(arena, s.structured_calendar.items),
            .intervals = intervals,
            .cron_expressions = s.cron_string.items,
            .skip = try calendarsFromProto(arena, s.exclude_structured_calendar.items),
            .start_at = timestamp(s.start_time),
            .end_at = timestamp(s.end_time),
            .jitter = duration(s.jitter),
            .time_zone_name = s.timezone_name,
        };
    }
};

/// Matches the times whose every field is in one of that field's ranges. The defaults match
/// midnight of every day, so `.{ .hour = &.{.{ .start = 9 }} }` is 09:00:00 daily.
pub const Calendar = struct {
    second: []const Range = &.{.{ .start = 0 }},
    minute: []const Range = &.{.{ .start = 0 }},
    hour: []const Range = &.{.{ .start = 0 }},
    day_of_month: []const Range = &.{.{ .start = 1, .end = 31 }},
    month: []const Range = &.{.{ .start = 1, .end = 12 }},
    /// Empty matches every year.
    year: []const Range = &.{},
    /// 0 (or 7) is Sunday.
    day_of_week: []const Range = &.{.{ .start = 0, .end = 6 }},
    comment: []const u8 = "",
};

/// The values `start`, `start + step`, ... up to `end` inclusive.
pub const Range = struct {
    start: i32,
    /// Below `start` (the default) means `start`: a single value.
    end: i32 = 0,
    /// 0 means 1.
    step: i32 = 0,
};

/// Matches the times `epoch + n * every + offset`, so `.{ .every = .fromSeconds(3600),
/// .offset = .fromSeconds(15 * 60) }` is every hour at a quarter past. Both are truncated
/// to whole seconds.
pub const Interval = struct {
    every: Duration,
    offset: Duration = .zero,
};

/// What a schedule does each time it fires.
pub const Action = union(enum) {
    start_workflow: StartWorkflowAction,
};

/// Starts a workflow. Build it with `startWorkflow` or `startWorkflowByName`, which encode
/// the arguments and search attributes.
pub const StartWorkflowAction = struct {
    workflow_type: []const u8,
    /// The encoded arguments.
    args: []const converter.Payload = &.{},
    /// The workflow ID. Each start appends the scheduled time ("<id>-<RFC 3339 time>") so
    /// runs don't collide, unless `Policies.keep_original_workflow_id` is set.
    id: []const u8,
    task_queue: []const u8,
    execution_timeout: ?Duration = null,
    run_timeout: ?Duration = null,
    task_timeout: ?Duration = null,
    /// Decode a value with `converter.fieldValue`.
    memo: []const converter.Field = &.{},
    /// Read a value with `search_attributes.get(arena, key)`.
    search_attributes: search_attributes.View = .{},

    fn toProto(sw: StartWorkflowAction, arena: Allocator, base: ?proto.temporal.api.workflow.v1.NewWorkflowExecutionInfo) error{OutOfMemory}!proto.temporal.api.workflow.v1.NewWorkflowExecutionInfo {
        var out = base orelse proto.temporal.api.workflow.v1.NewWorkflowExecutionInfo{};
        out.workflow_id = sw.id;
        out.workflow_type = .{ .name = sw.workflow_type };
        out.task_queue = .{ .name = sw.task_queue, .kind = .TASK_QUEUE_KIND_NORMAL };
        out.input = .{ .payloads = .fromOwnedSlice(try arena.dupe(converter.Payload, sw.args)) };
        out.workflow_execution_timeout = protoDuration(sw.execution_timeout);
        out.workflow_run_timeout = protoDuration(sw.run_timeout);
        out.workflow_task_timeout = protoDuration(sw.task_timeout);
        out.memo = null;
        if (sw.memo.len > 0) {
            var memo: common.Memo = .{};
            try memo.fields.ensureTotalCapacity(arena, sw.memo.len);
            for (sw.memo) |field| memo.fields.appendAssumeCapacity(.{ .key = field.name, .value = field.value });
            out.memo = memo;
        }
        out.search_attributes = if (sw.search_attributes.len() > 0) try sw.search_attributes.toProto(arena) else null;
        return out;
    }

    fn fromProto(arena: Allocator, sw: proto.temporal.api.workflow.v1.NewWorkflowExecutionInfo) error{OutOfMemory}!StartWorkflowAction {
        return .{
            .workflow_type = if (sw.workflow_type) |t| t.name else "",
            .args = if (sw.input) |in| in.payloads.items else &.{},
            .id = sw.workflow_id,
            .task_queue = if (sw.task_queue) |q| q.name else "",
            .execution_timeout = duration(sw.workflow_execution_timeout),
            .run_timeout = duration(sw.workflow_run_timeout),
            .task_timeout = duration(sw.workflow_task_timeout),
            .memo = if (sw.memo) |m| try converter.fieldsFromMap(arena, m.fields.items) else &.{},
            .search_attributes = try .fromProto(arena, sw.search_attributes),
        };
    }
};

/// How each workflow a schedule starts is started.
pub const StartWorkflowOptions = struct {
    /// The workflow ID; see `StartWorkflowAction.id`.
    id: []const u8,
    task_queue: []const u8,
    execution_timeout: ?Duration = null,
    run_timeout: ?Duration = null,
    task_timeout: ?Duration = null,
    /// Build with `converter.toFields`.
    memo: []const converter.Field = &.{},
    /// Built with `search_attributes.Key.set`; unsets are ignored. The server adds
    /// `scheduled_start_time` and `scheduled_by_id` to each start.
    search_attributes: []const search_attributes.Update = &.{},
};

/// An action that starts `Workflow` (a `workflow.Def`) with `args`. Arguments and search
/// attributes are encoded into `arena`; the action borrows `options`' other slices.
pub fn startWorkflow(arena: Allocator, comptime Workflow: type, args: Workflow.Args, options: StartWorkflowOptions) search_attributes.EncodeError!Action {
    return startWorkflowByName(arena, Workflow.workflow_name, args, options);
}

/// An action that starts the workflow type `workflow_type` with `args`, a tuple of its
/// arguments, for a workflow this program has no definition of.
pub fn startWorkflowByName(arena: Allocator, workflow_type: []const u8, args: anytype, options: StartWorkflowOptions) search_attributes.EncodeError!Action {
    const attributes = try search_attributes.toProto(arena, options.search_attributes, .start);
    return .{ .start_workflow = .{
        .workflow_type = workflow_type,
        .args = (try converter.toPayloads(arena, args)).items,
        .id = options.id,
        .task_queue = options.task_queue,
        .execution_timeout = options.execution_timeout,
        .run_timeout = options.run_timeout,
        .task_timeout = options.task_timeout,
        .memo = options.memo,
        .search_attributes = try .fromProto(arena, attributes),
    } };
}

/// What happens when an action is due while a workflow the schedule started is still
/// running. Values match the server's; the enum is open so a policy added by a newer server
/// is kept rather than rejected.
pub const OverlapPolicy = enum(i32) {
    /// For a schedule, the server's default (`skip`); for a trigger or backfill, the
    /// schedule's policy.
    unspecified = 0,
    /// Don't start the workflow.
    skip = 1,
    /// Start it once the running one closes; later actions due meanwhile are skipped.
    buffer_one = 2,
    /// Start every action due, one after another, as the running ones close.
    buffer_all = 3,
    /// Cancel the running workflow, then start the new one once it closes.
    cancel_other = 4,
    /// Terminate the running workflow and start the new one.
    terminate_other = 5,
    /// Start the new workflow alongside the running one.
    allow_all = 6,
    _,
};

pub const Policies = struct {
    overlap: OverlapPolicy = .unspecified,
    /// How late an action may still be taken after the server was unavailable. Null uses
    /// the server's default (a year).
    catchup_window: ?Duration = null,
    /// Pause the schedule when a workflow it started fails or times out.
    pause_on_failure: bool = false,
    /// Start every workflow with `StartWorkflowAction.id` as it is, with no time appended.
    /// Overlapping starts then conflict.
    keep_original_workflow_id: bool = false,
};

pub const State = struct {
    /// Why the schedule is in this state, for people reading it.
    note: []const u8 = "",
    paused: bool = false,
    /// How many more actions the schedule takes before it stops; null is no limit.
    remaining_actions: ?u64 = null,
};

/// Takes the actions a schedule would have taken between `start` and `end`, as if it had
/// been running then.
pub const Backfill = struct {
    start: Io.Timestamp,
    end: Io.Timestamp,
    overlap: OverlapPolicy = .unspecified,

    pub fn toProto(b: Backfill) pb.BackfillRequest {
        return .{
            .start_time = protoTimestamp(b.start),
            .end_time = protoTimestamp(b.end),
            .overlap_policy = @fromBackingInt(@backingInt(b.overlap)),
        };
    }
};

pub const CreateOptions = struct {
    schedule: Schedule,
    /// Take the action once right away, as `ScheduleHandle.trigger` does.
    trigger_immediately: bool = false,
    /// Take the actions of these past periods right away.
    backfill: []const Backfill = &.{},
    /// Build with `converter.toFields`.
    memo: []const converter.Field = &.{},
    /// The schedule's own search attributes, for `Client.listSchedules` queries; not given
    /// to the workflows it starts.
    search_attributes: []const search_attributes.Update = &.{},
};

/// What `ScheduleHandle.update`'s callback returns to change a schedule.
pub const Update = struct {
    /// Replaces the whole schedule. Start from the described one and change what you need.
    schedule: Schedule,
    /// Replaces the schedule's search attributes; null keeps them.
    search_attributes: ?[]const search_attributes.Update = null,
};

pub const ListOptions = struct {
    /// Schedules per page; 0 uses the server's default.
    page_size: u31 = 0,
    /// A visibility query on the schedules' search attributes; empty lists them all.
    query: []const u8 = "",
};

/// A workflow run a schedule started.
pub const Execution = struct {
    workflow_id: []const u8,
    run_id: []const u8,
};

/// One action a schedule took.
pub const ActionResult = struct {
    /// When the action was due, before jitter.
    scheduled_at: ?Io.Timestamp,
    /// When the server took it.
    taken_at: ?Io.Timestamp,
    /// The workflow it started.
    workflow: Execution,
    /// The workflow's status when the schedule last saw it; the server updates it only
    /// for workflows it is tracking, so it may lag.
    status: client_mod.WorkflowExecutionStatus,
};

/// What a schedule has done and will do.
pub const Info = struct {
    action_count: i64,
    /// Actions skipped because the server was unavailable past the catchup window.
    missed_catchup_window: i64,
    /// Actions skipped by the overlap policy.
    overlap_skipped: i64,
    /// Buffered actions dropped because the buffer was full.
    buffer_dropped: i64,
    /// Actions waiting in the buffer.
    buffer_size: i64,
    /// The workflows the schedule started that are still running.
    running_workflows: []const Execution,
    /// The most recent actions, oldest first.
    recent_actions: []const ActionResult,
    /// The next times the schedule will take its action.
    next_action_times: []const Io.Timestamp,
    create_time: ?Io.Timestamp,
    update_time: ?Io.Timestamp,
};

/// A schedule as `ScheduleHandle.describe` reports it. Every slice borrows the arena passed
/// to `describe`.
pub const Description = struct {
    id: []const u8,
    schedule: Schedule,
    info: Info,
    /// Decode a value with `memoValue` or `converter.fieldValue`.
    memo: []const converter.Field,
    /// The schedule's own search attributes. Read a value with `search_attributes.get(arena, key)`.
    search_attributes: search_attributes.View,
    /// The full server response, for anything not lifted into the fields above.
    raw: wfs.DescribeScheduleResponse,

    /// Lifts a describe response into a description that borrows it.
    pub fn fromProto(arena: Allocator, id: []const u8, response: wfs.DescribeScheduleResponse) error{ OutOfMemory, InvalidResponse }!Description {
        const info = response.info orelse pb.ScheduleInfo{};
        const running = try arena.alloc(Execution, info.running_workflows.items.len);
        for (info.running_workflows.items, running) |e, *o| o.* = .{ .workflow_id = e.workflow_id, .run_id = e.run_id };
        return .{
            .id = id,
            .schedule = try .fromProto(arena, response.schedule orelse .{}),
            .info = .{
                .action_count = info.action_count,
                .missed_catchup_window = info.missed_catchup_window,
                .overlap_skipped = info.overlap_skipped,
                .buffer_dropped = info.buffer_dropped,
                .buffer_size = info.buffer_size,
                .running_workflows = running,
                .recent_actions = try actionResults(arena, info.recent_actions.items),
                .next_action_times = try timestamps(arena, info.future_action_times.items),
                .create_time = timestamp(info.create_time),
                .update_time = timestamp(info.update_time),
            },
            .memo = if (response.memo) |m| try converter.fieldsFromMap(arena, m.fields.items) else &.{},
            .search_attributes = try .fromProto(arena, response.search_attributes),
            .raw = response,
        };
    }

    /// Decodes the memo value called `name` as `T`, or returns null if the memo has no such
    /// value. Memory comes from `arena`.
    pub fn memoValue(d: *const Description, comptime T: type, arena: Allocator, name: []const u8) converter.DecodeError!?T {
        return converter.fieldValue(T, arena, d.memo, name);
    }
};

/// A schedule as `Client.listSchedules` reports it: a summary, eventually consistent with
/// the schedule itself. Describe it through `Client.getScheduleHandle` for everything. Every
/// slice borrows the arena passed to `ScheduleIterator.next`.
pub const ListEntry = struct {
    id: []const u8,
    workflow_type: []const u8,
    spec: Spec,
    note: []const u8,
    paused: bool,
    recent_actions: []const ActionResult,
    next_action_times: []const Io.Timestamp,
    /// Decode a value with `converter.fieldValue`.
    memo: []const converter.Field,
    search_attributes: search_attributes.View,
    raw: pb.ScheduleListEntry,

    pub fn fromProto(arena: Allocator, entry: pb.ScheduleListEntry) error{OutOfMemory}!ListEntry {
        const info = entry.info orelse pb.ScheduleListInfo{};
        return .{
            .id = entry.schedule_id,
            .workflow_type = if (info.workflow_type) |t| t.name else "",
            .spec = try .fromProto(arena, info.spec orelse .{}),
            .note = info.notes,
            .paused = info.paused,
            .recent_actions = try actionResults(arena, info.recent_actions.items),
            .next_action_times = try timestamps(arena, info.future_action_times.items),
            .memo = if (entry.memo) |m| try converter.fieldsFromMap(arena, m.fields.items) else &.{},
            .search_attributes = try .fromProto(arena, entry.search_attributes),
            .raw = entry,
        };
    }
};

fn actionResults(arena: Allocator, results: []const pb.ScheduleActionResult) error{OutOfMemory}![]ActionResult {
    const out = try arena.alloc(ActionResult, results.len);
    for (results, out) |r, *o| {
        const e = r.start_workflow_result orelse common.WorkflowExecution{};
        o.* = .{
            .scheduled_at = timestamp(r.schedule_time),
            .taken_at = timestamp(r.actual_time),
            .workflow = .{ .workflow_id = e.workflow_id, .run_id = e.run_id },
            .status = @fromBackingInt(@backingInt(r.start_workflow_status)),
        };
    }
    return out;
}

fn timestamps(arena: Allocator, ts: []const proto.google.protobuf.Timestamp) error{OutOfMemory}![]Io.Timestamp {
    const out = try arena.alloc(Io.Timestamp, ts.len);
    for (ts, out) |t, *o| o.* = timestamp(t).?;
    return out;
}

fn calendarsToProto(arena: Allocator, calendars: []const Calendar) error{OutOfMemory}!std.ArrayList(pb.StructuredCalendarSpec) {
    const out = try arena.alloc(pb.StructuredCalendarSpec, calendars.len);
    for (calendars, out) |c, *o| o.* = .{
        .second = try rangesToProto(arena, c.second),
        .minute = try rangesToProto(arena, c.minute),
        .hour = try rangesToProto(arena, c.hour),
        .day_of_month = try rangesToProto(arena, c.day_of_month),
        .month = try rangesToProto(arena, c.month),
        .year = try rangesToProto(arena, c.year),
        .day_of_week = try rangesToProto(arena, c.day_of_week),
        .comment = c.comment,
    };
    return .fromOwnedSlice(out);
}

fn calendarsFromProto(arena: Allocator, calendars: []const pb.StructuredCalendarSpec) error{OutOfMemory}![]Calendar {
    const out = try arena.alloc(Calendar, calendars.len);
    for (calendars, out) |c, *o| o.* = .{
        .second = try rangesFromProto(arena, c.second.items),
        .minute = try rangesFromProto(arena, c.minute.items),
        .hour = try rangesFromProto(arena, c.hour.items),
        .day_of_month = try rangesFromProto(arena, c.day_of_month.items),
        .month = try rangesFromProto(arena, c.month.items),
        .year = try rangesFromProto(arena, c.year.items),
        .day_of_week = try rangesFromProto(arena, c.day_of_week.items),
        .comment = c.comment,
    };
    return out;
}

fn rangesToProto(arena: Allocator, ranges: []const Range) error{OutOfMemory}!std.ArrayList(pb.Range) {
    const out = try arena.alloc(pb.Range, ranges.len);
    for (ranges, out) |r, *o| o.* = .{ .start = r.start, .end = r.end, .step = r.step };
    return .fromOwnedSlice(out);
}

fn rangesFromProto(arena: Allocator, ranges: []const pb.Range) error{OutOfMemory}![]Range {
    const out = try arena.alloc(Range, ranges.len);
    for (ranges, out) |r, *o| o.* = .{ .start = r.start, .end = r.end, .step = r.step };
    return out;
}

fn timestamp(t: ?proto.google.protobuf.Timestamp) ?Io.Timestamp {
    const v = t orelse return null;
    return .fromNanoseconds(@as(i96, v.seconds) * std.time.ns_per_s + v.nanos);
}

fn protoTimestamp(t: ?Io.Timestamp) ?proto.google.protobuf.Timestamp {
    const v = t orelse return null;
    return .{
        .seconds = @intCast(@divFloor(v.nanoseconds, std.time.ns_per_s)),
        .nanos = @intCast(@mod(v.nanoseconds, std.time.ns_per_s)),
    };
}

fn duration(d: ?proto.google.protobuf.Duration) ?Duration {
    const v = d orelse return null;
    return .fromNanoseconds(@as(i96, v.seconds) * std.time.ns_per_s + v.nanos);
}

fn protoDuration(d: ?Duration) ?proto.google.protobuf.Duration {
    const v = d orelse return null;
    return .{
        .seconds = @intCast(@divFloor(v.nanoseconds, std.time.ns_per_s)),
        .nanos = @intCast(@mod(v.nanoseconds, std.time.ns_per_s)),
    };
}

// ---------------------------------------------------------------- tests

const testing = std.testing;

test "overlap policies match the server's values" {
    const info = @typeInfo(OverlapPolicy).@"enum";
    inline for (info.field_names, info.field_values) |name, value| {
        const upper = comptime blk: {
            var buf: [name.len]u8 = undefined;
            _ = std.ascii.upperString(&buf, name);
            break :blk buf;
        };
        const server = @field(proto.temporal.api.enums.v1.ScheduleOverlapPolicy, "SCHEDULE_OVERLAP_POLICY_" ++ upper);
        try testing.expectEqual(value, @backingInt(server));
    }
}

test "a schedule survives a round trip through its proto, keeping fields it doesn't carry" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const Wf = struct {
        fn run(ctx: *@import("workflow.zig").Context, name: []const u8, n: i64) ![]const u8 {
            _ = ctx;
            _ = n;
            return name;
        }
    };
    const Def = @import("workflow.zig").Def("Report", Wf.run);
    const tag = search_attributes.key(.keyword, "CustomKeywordField");

    const s: Schedule = .{
        .action = try startWorkflow(a, Def, .{ "all", 3 }, .{
            .id = "report",
            .task_queue = "reports",
            .run_timeout = .fromSeconds(60),
            .memo = try converter.toFields(a, .{ .owner = "ops" }),
            .search_attributes = &.{ tag.set("nightly"), search_attributes.key(.int, "Gone").unset() },
        }),
        .spec = .{
            .calendars = &.{.{ .hour = &.{.{ .start = 2 }}, .day_of_week = &.{.{ .start = 1, .end = 5 }}, .comment = "weekdays" }},
            .intervals = &.{.{ .every = .fromSeconds(3600), .offset = .fromSeconds(900) }},
            .cron_expressions = &.{"@daily"},
            .skip = &.{.{ .month = &.{.{ .start = 12 }}, .day_of_month = &.{.{ .start = 25 }} }},
            .start_at = .fromNanoseconds(1_700_000_000 * std.time.ns_per_s),
            .jitter = .fromMilliseconds(1500),
            .time_zone_name = "Europe/Paris",
        },
        .policies = .{ .overlap = .buffer_one, .catchup_window = .fromSeconds(600), .pause_on_failure = true },
        .state = .{ .note = "new", .paused = true, .remaining_actions = 4 },
    };

    var base: pb.Schedule = .{ .action = .{ .start_workflow = .{ .cron_schedule = "kept", .retry_policy = .{ .maximum_attempts = 3 } } } };
    base.spec = .{ .timezone_data = "tzdata" };
    const encoded = try s.toProto(a, base);
    const sw = encoded.action.?.start_workflow.?;
    try testing.expectEqualStrings("kept", sw.cron_schedule);
    try testing.expectEqual(@as(i32, 3), sw.retry_policy.?.maximum_attempts);
    try testing.expectEqualStrings("tzdata", encoded.spec.?.timezone_data);
    try testing.expectEqual(proto.temporal.api.enums.v1.ScheduleOverlapPolicy.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE, encoded.policies.?.overlap_policy);
    try testing.expect(encoded.state.?.limited_actions);

    const back = try Schedule.fromProto(a, encoded);
    const action = back.action.start_workflow;
    try testing.expectEqualStrings("Report", action.workflow_type);
    try testing.expectEqualStrings("report", action.id);
    try testing.expectEqualStrings("reports", action.task_queue);
    try testing.expectEqual(@as(?Duration, .fromSeconds(60)), action.run_timeout);
    const args = try converter.fromPayloads(struct { []const u8, i64 }, a, action.args);
    try testing.expectEqualStrings("all", args[0]);
    try testing.expectEqual(@as(i64, 3), args[1]);
    try testing.expectEqualStrings("ops", (try converter.fieldValue([]const u8, a, action.memo, "owner")).?);
    try testing.expectEqual(@as(usize, 1), action.search_attributes.len());
    try testing.expectEqualStrings("nightly", (try action.search_attributes.get(a, tag)).?);

    const spec = back.spec;
    try testing.expectEqual(@as(usize, 1), spec.calendars.len);
    try testing.expectEqual(@as(i32, 2), spec.calendars[0].hour[0].start);
    try testing.expectEqual(@as(i32, 5), spec.calendars[0].day_of_week[0].end);
    try testing.expectEqual(@as(usize, 0), spec.calendars[0].year.len);
    try testing.expectEqualStrings("weekdays", spec.calendars[0].comment);
    try testing.expectEqual(Duration.fromSeconds(900), spec.intervals[0].offset);
    try testing.expectEqualStrings("@daily", spec.cron_expressions[0]);
    try testing.expectEqual(@as(i32, 25), spec.skip[0].day_of_month[0].start);
    try testing.expectEqual(s.spec.start_at, spec.start_at);
    try testing.expectEqual(@as(?Io.Timestamp, null), spec.end_at);
    try testing.expectEqual(@as(?Duration, .fromMilliseconds(1500)), spec.jitter);
    try testing.expectEqualStrings("Europe/Paris", spec.time_zone_name);
    try testing.expectEqual(s.policies, back.policies);
    try testing.expectEqualStrings("new", back.state.note);
    try testing.expect(back.state.paused);
    try testing.expectEqual(@as(?u64, 4), back.state.remaining_actions);

    const unlimited = try Schedule.fromProto(a, try (Schedule{ .action = s.action, .spec = .{} }).toProto(a, null));
    try testing.expectEqual(@as(?u64, null), unlimited.state.remaining_actions);
    try testing.expectEqual(OverlapPolicy.unspecified, unlimited.policies.overlap);
}
