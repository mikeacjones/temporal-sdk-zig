//! Transfer type conversion: a type's `toPayload`/`fromPayload` hooks turn it into another
//! value (here a `WorkflowExecution` message) that the payload converter then encodes, and
//! back. Only top-level values are converted by their hooks; a hooked type nested in a plain
//! struct is encoded by the JSON converter like any field.
//!
//! The suite's standalone Nexus step is left out until the SDK can start a Nexus operation
//! from a client (temporal-sdk-zig-d51.5.12); the suite skips it where that is unsupported.
const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");
const activity = temporal.activity;
const workflow = temporal.workflow;
const converter = temporal.converter;
const Allocator = std.mem.Allocator;
const Payload = converter.Payload;
const WorkflowExecution = temporal.proto.temporal.api.common.v1.WorkflowExecution;

const transferred_marker = "created-from-transfer-type";
const unserialized_marker = "must-not-be-serialized";

/// Encodes `value` as the transfer value `WorkflowExecution{ value, tag }`.
fn toTransfer(arena: Allocator, value: []const u8, tag: []const u8) error{OutOfMemory}!Payload {
    return converter.toPayload(arena, WorkflowExecution{ .workflow_id = value, .run_id = tag });
}

/// Decodes the transfer value and returns its `workflow_id`, or `error.InvalidPayload` when
/// the value was not encoded with `tag`.
fn fromTransfer(arena: Allocator, p: Payload, tag: []const u8) converter.DecodeError![]const u8 {
    const transfer = try converter.fromPayload(WorkflowExecution, arena, p);
    if (!std.mem.eql(u8, transfer.run_id, tag)) return error.InvalidPayload;
    return transfer.workflow_id;
}

const NonGenericValue = struct {
    value: []const u8,
    extra: []const u8,

    pub fn toPayload(self: NonGenericValue, arena: Allocator) error{OutOfMemory}!Payload {
        return toTransfer(arena, self.value, "non-generic");
    }

    pub fn fromPayload(arena: Allocator, p: Payload) converter.DecodeError!NonGenericValue {
        return .{ .value = try fromTransfer(arena, p, "non-generic"), .extra = transferred_marker };
    }

    fn eql(a: NonGenericValue, b: NonGenericValue) bool {
        return std.mem.eql(u8, a.value, b.value) and std.mem.eql(u8, a.extra, b.extra);
    }
};

fn Box(comptime T: type) type {
    return struct {
        value: T,
        extra: []const u8,

        const Self = @This();

        pub fn toPayload(self: Self, arena: Allocator) error{OutOfMemory}!Payload {
            return toTransfer(arena, try std.fmt.allocPrint(arena, "{d}", .{self.value}), "box");
        }

        pub fn fromPayload(arena: Allocator, p: Payload) converter.DecodeError!Self {
            const text = try fromTransfer(arena, p, "box");
            const value = std.fmt.parseInt(T, text, 10) catch return error.InvalidPayload;
            return .{ .value = value, .extra = transferred_marker };
        }
    };
}

const ConvertedDerived = struct {
    value: []const u8,
    extra: []const u8,
    derived_extra: []const u8,

    pub fn toPayload(self: ConvertedDerived, arena: Allocator) error{OutOfMemory}!Payload {
        return toTransfer(arena, self.value, "converted-derived");
    }

    pub fn fromPayload(arena: Allocator, p: Payload) converter.DecodeError!ConvertedDerived {
        return .{
            .value = try fromTransfer(arena, p, "converted-derived"),
            .extra = transferred_marker,
            .derived_extra = transferred_marker,
        };
    }
};

/// No hooks: encoded as JSON, `nested` included, whose own hooks are not used.
const PlainValue = struct {
    value: []const u8,
    extra: []const u8,
    nested: NonGenericValue,
};

/// A type whose conversion always fails. `error.OutOfMemory` is the only error a
/// `toPayload` hook may return.
const ThrowingValue = struct {
    value: []const u8,

    pub fn toPayload(_: ThrowingValue, _: Allocator) error{OutOfMemory}!Payload {
        return error.OutOfMemory;
    }

    pub fn fromPayload(_: Allocator, _: Payload) converter.DecodeError!ThrowingValue {
        return error.InvalidPayload;
    }
};

const Workflow = workflow.Def("Workflow", runWorkflow);
const ThrowingWorkflow = workflow.Def("ThrowingWorkflow", runThrowingWorkflow);
const FailWithTransferDetail = activity.Def("fail_with_transfer_detail", failWithTransferDetail);
const StandaloneTransferActivity = activity.Def("standalone_transfer_activity", standaloneTransferActivity);

pub const feature = harness.define(.{
    .workflows = .{ Workflow, ThrowingWorkflow },
    .activities = .{ FailWithTransferDetail, StandaloneTransferActivity },
    .dev_server_args = &temporal.testing.DevServer.standalone_activity_args,
    .execute = execute,
});

fn failWithTransferDetail(ctx: *activity.Context) !void {
    const detail = try converter.toPayload(ctx.allocator(), NonGenericValue{ .value = "failure-detail", .extra = unserialized_marker });
    return ctx.fail(.{
        .message = "intentional transfer detail failure",
        .details = &.{detail},
        .non_retryable = true,
    });
}

fn standaloneTransferActivity(ctx: *activity.Context, value: NonGenericValue) !NonGenericValue {
    if (!value.eql(.{ .value = "activity-input", .extra = transferred_marker })) {
        const message = try std.fmt.allocPrint(ctx.allocator(), "unexpected activity input {{ \"{s}\", \"{s}\" }}", .{ value.value, value.extra });
        return ctx.fail(.{ .message = message, .type = "UnexpectedInput", .non_retryable = true });
    }
    return .{ .value = "activity-result", .extra = unserialized_marker };
}

fn runWorkflow(
    ctx: *workflow.Context,
    non_generic: NonGenericValue,
    box: Box(i32),
    plain: PlainValue,
    converted_derived: ConvertedDerived,
) !NonGenericValue {
    var failures: std.ArrayList([]const u8) = .empty;
    const a = ctx.allocator();

    if (!non_generic.eql(.{ .value = "non-generic", .extra = transferred_marker })) try failures.append(a, "non-generic");
    if (!(box.value == 123 and std.mem.eql(u8, box.extra, transferred_marker))) try failures.append(a, "generic");
    if (!(std.mem.eql(u8, converted_derived.value, "converted-derived") and
        std.mem.eql(u8, converted_derived.extra, transferred_marker) and
        std.mem.eql(u8, converted_derived.derived_extra, transferred_marker))) try failures.append(a, "converted-derived");
    if (!(std.mem.eql(u8, plain.value, "plain") and std.mem.eql(u8, plain.extra, "plain-extra") and
        plain.nested.eql(.{ .value = "nested", .extra = "nested-extra" }))) try failures.append(a, "top-level-only");

    const future = ctx.executeActivity(FailWithTransferDetail, .{ .start_to_close_timeout = .fromSeconds(10) }, .{});
    if (future.get()) |_| {
        try failures.append(a, "failure-detail-missing");
    } else |err| switch (err) {
        error.ActivityFailed => try checkFailureDetail(a, future.failure(), &failures),
        else => |e| return e,
    }

    const value = if (failures.items.len == 0)
        "workflow-result"
    else
        try std.mem.concat(a, u8, &.{ "failed:", try std.mem.join(a, ",", failures.items) });
    return .{ .value = value, .extra = unserialized_marker };
}

/// The activity's application failure carries one detail, its transfer value.
fn checkFailureDetail(a: Allocator, activity_failure: ?*const temporal.failure.Failure, failures: *std.ArrayList([]const u8)) !void {
    const cause = if (activity_failure) |f| f.cause else null;
    const details: converter.EncodedValues = if (cause) |c| switch (temporal.failure.kind(c)) {
        .application => |app| app.details,
        else => .{ .payloads = &.{} },
    } else .{ .payloads = &.{} };
    if (cause == null or temporal.failure.kind(cause.?) != .application) try failures.append(a, "failure-detail-error");
    if (details.len() != 1) try failures.append(a, "failure-detail-count");
    if (details.len() == 0) return;
    const detail = details.get(WorkflowExecution, a, 0) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return failures.append(a, "failure-detail-type"),
    };
    if (!(std.mem.eql(u8, detail.workflow_id, "failure-detail") and std.mem.eql(u8, detail.run_id, "non-generic")))
        try failures.append(a, "failure-detail-value");
}

fn runThrowingWorkflow(ctx: *workflow.Context, _: ThrowingValue) !void {
    return ctx.fail(.{ .message = "a converter failure must prevent workflow start" });
}

fn execute(ctx: *harness.Context) !void {
    try expectThrowingStart(ctx);
    try exerciseStandaloneActivity(ctx);

    var run = try ctx.client.executeWorkflow(Workflow, ctx.startOptions(), .{
        NonGenericValue{ .value = "non-generic", .extra = "client-extra" },
        Box(i32){ .value = 123, .extra = "client-extra" },
        PlainValue{ .value = "plain", .extra = "plain-extra", .nested = .{ .value = "nested", .extra = "nested-extra" } },
        ConvertedDerived{ .value = "converted-derived", .extra = "client-extra", .derived_extra = "client-derived-extra" },
    });
    defer run.deinit();
    const result = run.get(ctx.arena) catch |err| {
        if (run.failure()) |failure| return ctx.fail("workflow failed: {s}", .{failure.message});
        return err;
    };
    try ctx.expect(
        result.eql(.{ .value = "workflow-result", .extra = transferred_marker }),
        "workflow result is {{ \"{s}\", \"{s}\" }}, expected {{ \"workflow-result\", \"{s}\" }}",
        .{ result.value, result.extra, transferred_marker },
    );

    const recorded = try ctx.fetchRawHistory(run.id, run.run_id);
    const inputs = harness.history.inputPayloads(recorded);
    try ctx.expect(inputs.len == 4, "workflow start has {d} input payloads, expected 4", .{inputs.len});
    try expectTransferPayload(ctx, "input 0", inputs[0], "non-generic");
    try expectTransferPayload(ctx, "input 1", inputs[1], "box");
    try ctx.expectEqualStrings("json/plain", converter.encodingOf(inputs[2]) orelse "");
    try expectTransferPayload(ctx, "input 3", inputs[3], "converted-derived");

    const failed = harness.history.first(recorded, .EVENT_TYPE_ACTIVITY_TASK_FAILED) orelse
        return ctx.fail("history has no EVENT_TYPE_ACTIVITY_TASK_FAILED event", .{});
    const detail = failureDetail(failed) orelse return ctx.fail("ActivityTaskFailed has no application failure detail", .{});
    try expectTransferPayload(ctx, "activity failure detail", detail, "non-generic");

    const outputs = harness.history.resultPayloads(recorded);
    try ctx.expect(outputs.len == 1, "workflow completion has {d} result payloads, expected 1", .{outputs.len});
    try expectTransferPayload(ctx, "workflow result", outputs[0], "non-generic");
}

/// A conversion failure stops the start before any request reaches the server.
fn expectThrowingStart(ctx: *harness.Context) !void {
    const id = try std.fmt.allocPrint(ctx.arena, "{s}-failing", .{ctx.task_queue});
    var options = ctx.startOptions();
    options.id = id;
    if (ctx.client.executeWorkflow(ThrowingWorkflow, options, .{ThrowingValue{ .value = "expected transfer conversion failure" }})) |started| {
        var run = started;
        run.deinit();
        return ctx.fail("ThrowingWorkflow started, expected its argument's conversion to fail", .{});
    } else |err| switch (err) {
        error.OutOfMemory => {},
        else => return ctx.fail("starting ThrowingWorkflow failed with {t}, expected the conversion's error.OutOfMemory", .{err}),
    }
    if (ctx.client.describeWorkflow(ctx.arena, id, "")) |_| {
        return ctx.fail("workflow {s} exists, expected the conversion failure to prevent its start", .{id});
    } else |err| switch (err) {
        error.NotFound => {},
        else => return ctx.fail("describing workflow {s} failed with {t}, expected error.NotFound", .{ id, err }),
    }
}

fn exerciseStandaloneActivity(ctx: *harness.Context) !void {
    const id = try std.fmt.allocPrint(ctx.arena, "{s}-activity", .{ctx.task_queue});
    var handle = try ctx.client.executeActivity(StandaloneTransferActivity, .{
        .id = id,
        .task_queue = ctx.task_queue,
        .start_to_close_timeout = .fromSeconds(10),
        .retry_policy = .{ .maximum_attempts = 1 },
    }, .{NonGenericValue{ .value = "activity-input", .extra = "client-extra" }});
    defer handle.deinit();
    const result = handle.get(ctx.arena) catch |err| {
        if (handle.failure()) |failure| return ctx.fail("standalone activity failed: {s}", .{failure.message});
        return err;
    };
    try ctx.expect(
        result.eql(.{ .value = "activity-result", .extra = transferred_marker }),
        "standalone activity result is {{ \"{s}\", \"{s}\" }}, expected {{ \"activity-result\", \"{s}\" }}",
        .{ result.value, result.extra, transferred_marker },
    );
}

fn failureDetail(event: temporal.history.HistoryEvent) ?Payload {
    const attributes = event.attributes orelse return null;
    if (attributes != .activity_task_failed_event_attributes) return null;
    const f = attributes.activity_task_failed_event_attributes.failure orelse return null;
    const info = f.failure_info orelse return null;
    if (info != .application_failure_info) return null;
    const details = info.application_failure_info.details orelse return null;
    if (details.payloads.items.len == 0) return null;
    return details.payloads.items[0];
}

fn expectTransferPayload(ctx: *harness.Context, what: []const u8, payload: Payload, run_id: []const u8) !void {
    const encoding = converter.encodingOf(payload) orelse "";
    try ctx.expect(std.mem.eql(u8, "json/protobuf", encoding), "{s}: encoding is \"{s}\", expected \"json/protobuf\"", .{ what, encoding });
    const message_type = for (payload.metadata.items) |entry| {
        if (std.mem.eql(u8, entry.key, converter.message_type_key)) break entry.value;
    } else "";
    try ctx.expect(
        std.mem.eql(u8, "temporal.api.common.v1.WorkflowExecution", message_type),
        "{s}: messageType is \"{s}\", expected \"temporal.api.common.v1.WorkflowExecution\"",
        .{ what, message_type },
    );
    const value = converter.fromPayload(WorkflowExecution, ctx.arena, payload) catch |err|
        return ctx.fail("{s}: not a WorkflowExecution ({t}): {s}", .{ what, err, payload.data });
    try ctx.expect(std.mem.eql(u8, run_id, value.run_id), "{s}: run_id is \"{s}\", expected \"{s}\"", .{ what, value.run_id, run_id });
}
