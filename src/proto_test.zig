//! Round-trip tests for the generated protobuf code on the messages the SDK exchanges with
//! sdk-core and the server.

const std = @import("std");
const testing = std.testing;
const proto = @import("proto.zig");

const act = proto.coresdk.workflow_activation;
const cmds = proto.coresdk.workflow_commands;
const completion = proto.coresdk.workflow_completion;
const common = proto.temporal.api.common.v1;
const history = proto.temporal.api.history.v1;
const enums = proto.temporal.api.enums.v1;
const wfs = proto.temporal.api.workflowservice.v1;

fn encode(msg: anytype, a: std.mem.Allocator) ![]const u8 {
    var aw: std.Io.Writer.Allocating = .init(a);
    try msg.encode(&aw.writer, a);
    return aw.written();
}

fn decode(comptime T: type, bytes: []const u8, a: std.mem.Allocator) !T {
    var r: std.Io.Reader = .fixed(bytes);
    return T.decode(&r, a);
}

fn jsonPayload(a: std.mem.Allocator, json: []const u8) !common.Payload {
    var p: common.Payload = .{ .data = json };
    try p.metadata.append(a, .{ .key = "encoding", .value = "json/plain" });
    return p;
}

test "WorkflowActivation round-trips byte for byte" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var init_job: act.InitializeWorkflow = .{
        .workflow_type = "Greeting",
        .workflow_id = "wf-1",
        .randomness_seed = 0xDEADBEEF,
        .workflow_run_timeout = .{ .seconds = 30 },
    };
    try init_job.arguments.append(a, try jsonPayload(a, "\"bob\""));
    try init_job.headers.append(a, .{ .key = "trace", .value = try jsonPayload(a, "\"abc\"") });

    var signal: act.SignalWorkflow = .{ .signal_name = "approve", .identity = "cli" };
    try signal.input.append(a, try jsonPayload(a, "{\"ok\":true}"));

    var msg: act.WorkflowActivation = .{
        .run_id = "run-123",
        .timestamp = .{ .seconds = 1_700_000_000, .nanos = 5 },
        .is_replaying = true,
        .history_length = 7,
    };
    try msg.jobs.append(a, .{ .variant = .{ .initialize_workflow = init_job } });
    try msg.jobs.append(a, .{ .variant = .{ .fire_timer = .{ .seq = 3 } } });
    try msg.jobs.append(a, .{ .variant = .{ .signal_workflow = signal } });

    const bytes = try encode(msg, a);
    const back = try decode(act.WorkflowActivation, bytes, a);

    try testing.expectEqualStrings("run-123", back.run_id);
    try testing.expect(back.is_replaying);
    try testing.expectEqual(@as(usize, 3), back.jobs.items.len);
    const ij = back.jobs.items[0].variant.?.initialize_workflow;
    try testing.expectEqualStrings("Greeting", ij.workflow_type);
    try testing.expectEqual(@as(u64, 0xDEADBEEF), ij.randomness_seed);
    try testing.expectEqualStrings("\"bob\"", ij.arguments.items[0].data);
    try testing.expectEqualStrings("json/plain", ij.arguments.items[0].metadata.items[0].value);
    try testing.expectEqual(@as(u32, 3), back.jobs.items[1].variant.?.fire_timer.seq);
    try testing.expectEqualStrings("approve", back.jobs.items[2].variant.?.signal_workflow.signal_name);
    try testing.expectEqualSlices(u8, bytes, try encode(back, a));
}

test "WorkflowActivationCompletion with commands round-trips" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var success: completion.Success = .{};
    try success.commands.append(a, .{ .variant = .{ .start_timer = .{ .seq = 1, .start_to_fire_timeout = .{ .seconds = 5 } } } });
    var sched: cmds.ScheduleActivity = .{
        .seq = 2,
        .activity_id = "1",
        .activity_type = "greet",
        .task_queue = "q",
        .start_to_close_timeout = .{ .seconds = 10 },
    };
    try sched.arguments.append(a, try jsonPayload(a, "\"bob\""));
    try success.commands.append(a, .{ .variant = .{ .schedule_activity = sched } });

    const msg: completion.WorkflowActivationCompletion = .{ .run_id = "run-123", .status = .{ .successful = success } };
    const bytes = try encode(msg, a);
    const back = try decode(completion.WorkflowActivationCompletion, bytes, a);
    const cs = back.status.?.successful.commands.items;
    try testing.expectEqual(@as(usize, 2), cs.len);
    try testing.expectEqualStrings("greet", cs[1].variant.?.schedule_activity.activity_type);
    try testing.expectEqualSlices(u8, bytes, try encode(back, a));
}

fn samplePoll(a: std.mem.Allocator) !wfs.PollWorkflowTaskQueueResponse {
    var started: history.WorkflowExecutionStartedEventAttributes = .{
        .workflow_type = .{ .name = "Greeting" },
        .attempt = 1,
        .input = .{},
    };
    try started.input.?.payloads.append(a, try jsonPayload(a, "\"bob\""));

    var hist: history.History = .{};
    try hist.events.append(a, .{
        .event_id = 1,
        .event_time = .{ .seconds = 1_700_000_000 },
        .event_type = .EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
        .attributes = .{ .workflow_execution_started_event_attributes = started },
    });
    try hist.events.append(a, .{
        .event_id = 2,
        .event_type = .EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
        .attributes = .{ .workflow_task_scheduled_event_attributes = .{ .attempt = 1 } },
    });

    var resp: wfs.PollWorkflowTaskQueueResponse = .{
        .task_token = &.{ 0x00, 0xFF, 0x10, 0x80 },
        .workflow_execution = .{ .workflow_id = "wf-1", .run_id = "run-123" },
        .workflow_type = .{ .name = "Greeting" },
        .started_event_id = 3,
        .attempt = 1,
        .history = hist,
    };
    try resp.queries.append(a, .{ .key = "q1", .value = .{ .query_type = "__stack_trace" } });
    return resp;
}

test "PollWorkflowTaskQueueResponse with history and query map round-trips" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const resp = try samplePoll(a);
    const bytes = try encode(resp, a);
    const back = try decode(wfs.PollWorkflowTaskQueueResponse, bytes, a);

    try testing.expectEqualSlices(u8, resp.task_token, back.task_token);
    const evs = back.history.?.events.items;
    try testing.expectEqual(@as(usize, 2), evs.len);
    try testing.expectEqual(enums.EventType.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, evs[0].event_type);
    const st = evs[0].attributes.?.workflow_execution_started_event_attributes;
    try testing.expectEqualStrings("Greeting", st.workflow_type.?.name);
    try testing.expectEqualStrings("\"bob\"", st.input.?.payloads.items[0].data);
    try testing.expectEqualStrings("__stack_trace", back.queries.items[0].value.?.query_type);
    try testing.expectEqualSlices(u8, bytes, try encode(back, a));
}

test "unknown fields from a newer server survive decode and encode" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const known = try encode(act.FireTimer{ .seq = 9 }, a);
    // Field 999 as a varint: tag (999 << 3) = 7992 = 0xB8 0x3E, value 42.
    const with_unknown = try std.mem.concat(a, u8, &.{ known, &.{ 0xB8, 0x3E, 42 } });
    const back = try decode(act.FireTimer, with_unknown, a);
    try testing.expectEqual(@as(u32, 9), back.seq);
    try testing.expectEqualSlices(u8, with_unknown, try encode(back, a));
}

test "History ProtoJSON uses flat oneofs and parses back" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const hist = (try samplePoll(a)).history.?;
    const json = try hist.jsonEncode(.{}, .{ .emit_oneof_field_name = false }, a);
    try testing.expect(std.mem.indexOf(u8, json, "\"workflowExecutionStartedEventAttributes\"") != null);
    try testing.expect(std.mem.indexOf(u8, json, "\"attributes\"") == null);

    const parsed = try history.History.jsonDecode(json, .{}, a);
    const evs = parsed.value.events.items;
    try testing.expectEqual(@as(usize, 2), evs.len);
    try testing.expectEqual(enums.EventType.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, evs[0].event_type);
    const st = evs[0].attributes.?.workflow_execution_started_event_attributes;
    try testing.expectEqualStrings("\"bob\"", st.input.?.payloads.items[0].data);
    // Payload.metadata (map<string, bytes>) is not checked: the pinned zig-protobuf does not
    // base64-decode bytes map values from JSON yet (beads temporal-sdk-zig-d51.2.6).
}

test "decode and deinit without an arena does not leak" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const bytes = try encode(try samplePoll(arena.allocator()), arena.allocator());

    var r: std.Io.Reader = .fixed(bytes);
    var back = try wfs.PollWorkflowTaskQueueResponse.decode(&r, testing.allocator);
    back.deinit(testing.allocator);
}
