//! The cases of `zig build alloc-failures` (see AGENTS.md). Each case calls one SDK function
//! with each of its allocations failing in turn, through `alloc_failures.run` in
//! tools/checks/alloc_failures.zig, and expects error.OutOfMemory from every call that saw a
//! failure.
//!
//! Covered:
//! - converter.toPayload, converter.fromPayload, converter.toPayloads,
//!   converter.fromPayloads, converter.toFields, converter.fieldValue,
//!   converter.Header.fromEntries, converter.Header.toEntries
//! - failure.fromError, failure.application, failure.copyChain, failure.encodeAttributes,
//!   failure.decodeAttributes
//! - history.fromJson
//! - protojson.encode, protojson.decode
//! - search_attributes.toProto, search_attributes.encodeValue,
//!   search_attributes.View.fromProto, search_attributes.View.get,
//!   search_attributes.View.dupe, search_attributes.View.toProto
//! - nexus.wire.fromNexusLink, nexus.wire.toNexusLink, nexus.wire.handlerFailure,
//!   nexus.wire.operationFailure
//! - schedule.Schedule.toProto, schedule.Schedule.fromProto, schedule.Description.fromProto
//! - propagation.injectAll, propagation.extractAll
//! - client.loadOptions, from `LoadOptions.config_data` and `LoadOptions.env` only
//!
//! Left out: every function that needs a core runtime, a worker or a server, such as the
//! `Client` calls, a worker's polling and task completions (including the Nexus worker's
//! task decoding), and the workflow and activity context calls. A case here is
//! single-threaded and starts nothing, and those functions can't run without what they
//! connect to; worker.zig tests their allocation failures where it can. client.loadOptions
//! is in because sdk-core reads its profile without a runtime.
//!
//! Adding a case: write a `test "<case>"`, where the case is the function's path from src
//! with dots ("converter.Header.fromEntries"). Build the inputs before the call, with
//! std.testing.allocator, and free them with `defer`. Then call
//! `try alloc_failures.run("<case>", .arena, <wrapper>, .{inputs...})`, where the wrapper is
//! a file-level fn that takes the allocator first and fixes the comptime arguments. Pick
//! inputs that make the function allocate at least twice. Use `.owned` for a function that
//! frees what it allocates. A function that mutates its input gets a wrapper that copies it
//! first. A finding that is not fixed with its case goes in tools/checks/alloc_failures.zon.

const std = @import("std");
const Allocator = std.mem.Allocator;
const alloc_failures = @import("alloc_failures");
const converter = @import("converter.zig");
const proto = @import("proto.zig");
const failure = @import("failure.zig");
const history = @import("history.zig");
const protojson = @import("protojson.zig");
const search_attributes = @import("search_attributes.zig");
const nexus = @import("nexus.zig");
const wire = @import("nexus/wire.zig");
const schedule = @import("schedule.zig");
const propagation = @import("propagation.zig");
const client = @import("client.zig");

const Color = enum { red, green, blue };

const Item = struct {
    name: []const u8,
    tags: []const []const u8 = &.{},
    count: ?u32 = null,
    color: Color = .red,
};

const Address = struct {
    street: []const u8,
    city: []const u8,
    floor: ?u8 = null,
};

const Order = struct {
    id: u64,
    items: []const Item,
    note: ?[]const u8 = null,
    ship_to: Address,
};

const order: Order = .{
    .id = 42,
    .items = &.{
        .{ .name = "widget", .tags = &.{ "blue", "small" }, .count = 3, .color = .blue },
        .{ .name = "gadget", .tags = &.{"large"}, .color = .green },
    },
    .note = "leave at the door",
    .ship_to = .{ .street = "1 Main Street", .city = "Springfield", .floor = 2 },
};

/// A protobuf map entry of a header, the shape `Header.fromEntries` reads.
const ProtoEntry = proto.temporal.api.common.v1.Header.FieldsEntry;

/// A second entry shape for `Header.toEntries`, which allocates once per call.
const LocalEntry = struct { key: []const u8, value: converter.Payload };

fn toPayloadOrder(a: Allocator, value: Order) !void {
    _ = try converter.toPayload(a, value);
}

fn fromPayloadOrder(a: Allocator, p: converter.Payload) !void {
    _ = try converter.fromPayload(Order, a, p);
}

fn toPayloadsArgs(a: Allocator, value: Order, label: []const u8) !void {
    _ = try converter.toPayloads(a, .{ value, label, @as(?u32, 7) });
}

const Args = struct { Order, []const u8, ?u32 };

fn fromPayloadsArgs(a: Allocator, payloads: []const converter.Payload) !void {
    _ = try converter.fromPayloads(Args, a, payloads);
}

fn toFieldsMemo(a: Allocator, value: Order, label: []const u8) !void {
    _ = try converter.toFields(a, .{ .order = value, .label = label, .missing = @as(?u32, null) });
}

fn fieldValueOrder(a: Allocator, fields: []const converter.Field) !void {
    _ = try converter.fieldValue(Order, a, fields, "order");
}

fn headerFromEntries(a: Allocator, entries: []const ProtoEntry) !void {
    _ = try converter.Header.fromEntries(a, entries);
}

fn headerToEntries(a: Allocator, header: converter.Header) !void {
    _ = try header.toEntries(ProtoEntry, a);
    _ = try header.toEntries(LocalEntry, a);
}

test "converter.toPayload" {
    try alloc_failures.run("converter.toPayload", .arena, toPayloadOrder, .{order});
}

test "converter.fromPayload" {
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const p = try converter.toPayload(arena.allocator(), order);
    try alloc_failures.run("converter.fromPayload", .arena, fromPayloadOrder, .{p});
}

test "converter.toPayloads" {
    try alloc_failures.run("converter.toPayloads", .arena, toPayloadsArgs, .{ order, @as([]const u8, "nightly") });
}

test "converter.fromPayloads" {
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const payloads = try converter.toPayloads(arena.allocator(), .{ order, "nightly", @as(?u32, 7) });
    try alloc_failures.run("converter.fromPayloads", .arena, fromPayloadsArgs, .{@as([]const converter.Payload, payloads.items)});
}

test "converter.toFields" {
    try alloc_failures.run("converter.toFields", .arena, toFieldsMemo, .{ order, @as([]const u8, "nightly") });
}

test "converter.fieldValue" {
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const fields = try converter.toFields(arena.allocator(), .{ .label = "nightly", .order = order });
    try alloc_failures.run("converter.fieldValue", .arena, fieldValueOrder, .{@as([]const converter.Field, fields)});
}

test "converter.Header.fromEntries" {
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const entries = [_]ProtoEntry{
        .{ .key = "trace", .value = try converter.toPayload(a, "00-4bf92f3577b34da6-01") },
        .{ .key = "order", .value = try converter.toPayload(a, order) },
        .{ .key = "empty" },
        .{ .key = "trace", .value = try converter.toPayload(a, @as(u32, 9)) },
    };
    try alloc_failures.run("converter.Header.fromEntries", .arena, headerFromEntries, .{@as([]const ProtoEntry, &entries)});
}

test "converter.Header.toEntries" {
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var w: converter.HeaderWriter = .init(a);
    try w.set("trace", "00-4bf92f3577b34da6-01");
    try w.set("order", order);
    try alloc_failures.run("converter.Header.toEntries", .arena, headerToEntries, .{w.header()});
}

// ---------------------------------------------------------------- failure

const Failure = failure.Failure;

/// A request that holds a failure, so `encodeAttributes` and `decodeAttributes` walk an
/// optional field down to the chain as they do on the way to the server.
const FailedRequest = proto.temporal.api.workflowservice.v1.RespondActivityTaskFailedRequest;

/// A chain of three failures, each with a message and a stack trace, built in `a`.
fn failureChain(a: Allocator) !*const Failure {
    const root = try a.create(Failure);
    root.* = try failure.application(a, .{ .message = "card declined", .type = "CardDeclined", .details = &.{try converter.toPayload(a, order)} });
    root.stack_trace = "payments.zig:12:5: in charge";
    const middle = try a.create(Failure);
    middle.* = try failure.application(a, .{ .message = "charge failed", .type = "ChargeFailed", .cause = root });
    middle.stack_trace = "checkout.zig:40:9: in pay";
    const top = try a.create(Failure);
    top.* = try failure.application(a, .{ .message = "order failed", .type = "OrderFailed", .non_retryable = true, .cause = middle });
    top.stack_trace = "order.zig:7:3: in place";
    return top;
}

fn failureFromError(a: Allocator) !void {
    // fromError allocates nothing itself (no details, no cause), so the case follows its
    // result to where the SDK sends it with encode_failure_attributes on: its allocation
    // failures can only surface there.
    var f = try failure.fromError(a, error.InsufficientFunds);
    try failure.encodeAttributes(Failure, &f, a);
}

fn failureApplication(a: Allocator, cause: *const Failure) !void {
    _ = try failure.application(a, .{
        .message = "shipment lost",
        .type = "ShipmentLost",
        .details = &.{ .{ .data = "\"parcel-7\"" }, .{ .data = "3" } },
        .next_retry_delay = .fromSeconds(5),
        .category = .benign,
        .cause = cause,
    });
}

fn failureCopyChain(a: Allocator, cause: *const Failure) !void {
    _ = try failure.copyChain(a, cause);
}

fn failureEncodeAttributes(a: Allocator, request: FailedRequest) !void {
    var copy = request;
    try failure.encodeAttributes(FailedRequest, &copy, a);
}

fn failureDecodeAttributes(a: Allocator, request: FailedRequest) !void {
    var copy = request;
    try failure.decodeAttributes(FailedRequest, &copy, a);
}

test "failure.fromError" {
    try alloc_failures.run("failure.fromError", .arena, failureFromError, .{});
}

test "failure.application" {
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const chain = try failureChain(arena.allocator());
    try alloc_failures.run("failure.application", .arena, failureApplication, .{chain});
}

test "failure.copyChain" {
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const chain = try failureChain(arena.allocator());
    try alloc_failures.run("failure.copyChain", .arena, failureCopyChain, .{chain});
}

test "failure.encodeAttributes" {
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const chain = try failureChain(a);
    var top = chain.*;
    // Bytes that are not UTF-8 take the replacement path.
    top.message = "order \xff failed";
    const request: FailedRequest = .{ .task_token = "token", .failure = top, .identity = "worker-1" };
    try alloc_failures.run("failure.encodeAttributes", .arena, failureEncodeAttributes, .{request});
}

test "failure.decodeAttributes" {
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var request: FailedRequest = .{ .task_token = "token", .failure = (try failureChain(a)).*, .identity = "worker-1" };
    try failure.encodeAttributes(FailedRequest, &request, a);
    // The middle failure's attributes hold a lone UTF-16 surrogate, as a JavaScript sender
    // may write, which takes the repair path.
    const middle = request.failure.?.cause.?;
    middle.encoded_attributes = try converter.toPayload(a, failure.EncodedAttributes{ .message = "placeholder" });
    middle.encoded_attributes.?.data = "{\"message\":\"charge \\ud800 failed\",\"stack_trace\":\"checkout.zig:40:9\"}";
    try alloc_failures.run("failure.decodeAttributes", .arena, failureDecodeAttributes, .{request});
}

// ---------------------------------------------------------------- history

fn historyFromJson(a: Allocator, json: []const u8) !void {
    _ = try history.fromJson(a, json);
}

test "history.fromJson" {
    try alloc_failures.run("history.fromJson", .arena, historyFromJson, .{@as([]const u8, @embedFile("testdata/continue_as_new_by_name_history.json"))});
}

// ---------------------------------------------------------------- protojson

/// A history export with a repeated field (`events`), a map (`metadata`), a oneof (the
/// event attributes), nested messages, and enums by short name as older exports write them.
const history_json =
    \\{"events":[
    \\ {"eventId":"1","eventType":"WorkflowExecutionStarted","eventTime":"2026-11-16T14:47:40.965611Z",
    \\  "workflowExecutionStartedEventAttributes":{"workflowType":{"name":"Checkout"},"taskQueue":{"name":"orders","kind":"Normal"},
    \\   "input":{"payloads":[{"metadata":{"encoding":"anNvbi9wbGFpbg==","messageType":"T3JkZXI="},"data":"eyJpZCI6NDJ9"}]},
    \\   "header":{"fields":{"trace":{"metadata":{"encoding":"anNvbi9wbGFpbg=="},"data":"IjAwLTRiZjki"}}},
    \\   "workflowRunTimeout":"60s","attempt":1}},
    \\ {"eventId":"2","eventType":"WorkflowTaskScheduled","workflowTaskScheduledEventAttributes":{"taskQueue":{"name":"orders"},"startToCloseTimeout":"10s","attempt":1}},
    \\ {"eventId":"3","eventType":"WorkflowExecutionFailed","workflowExecutionFailedEventAttributes":{"failure":{"message":"order failed","source":"ZigSDK",
    \\   "cause":{"message":"card declined","applicationFailureInfo":{"type":"CardDeclined"}},"applicationFailureInfo":{"type":"OrderFailed","nonRetryable":true}},
    \\   "retryState":"NonRetryableFailure"}}
    \\]}
;

fn protojsonEncode(a: Allocator, message: history.History) !void {
    _ = try protojson.encode(a, message);
}

fn protojsonDecode(a: Allocator, json: []const u8) !void {
    _ = try protojson.decode(history.History, a, json);
}

test "protojson.encode" {
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const message = try protojson.decode(history.History, arena.allocator(), history_json);
    try alloc_failures.run("protojson.encode", .arena, protojsonEncode, .{message});
}

test "protojson.decode" {
    try alloc_failures.run("protojson.decode", .arena, protojsonDecode, .{@as([]const u8, history_json)});
}

// ---------------------------------------------------------------- search attributes

const sa = search_attributes;

const customer_id = sa.key(.keyword, "CustomerId");
const notes = sa.key(.text, "Notes");
const priority = sa.key(.int, "Priority");
const score = sa.key(.double, "Score");
const vip = sa.key(.bool, "Vip");
const due = sa.key(.datetime, "Due");
const regions = sa.key(.keyword_list, "Regions");

/// Values of every type, a keyword list among them, and one unset.
const updates = [_]sa.Update{
    customer_id.set("c-42"),
    notes.set("leave at the door"),
    priority.set(3),
    score.set(0.75),
    vip.set(true),
    due.set(.{ .unix_ns = 1_795_000_000_000_000_000 }),
    regions.set(&.{ "eu-west", "us-east", "ap-south" }),
    sa.key(.keyword, "Retired").unset(),
};

fn saToProto(a: Allocator, purpose: sa.Purpose) !void {
    _ = try sa.toProto(a, &updates, purpose);
}

fn saEncodeValue(a: Allocator) !void {
    for (updates) |u| _ = try sa.encodeValue(a, u.name, u.value);
}

fn saViewFromProto(a: Allocator, attributes: proto.temporal.api.common.v1.SearchAttributes) !void {
    // fromProto allocates once per call, so the case reads two sets: all the attributes,
    // and the first of them.
    _ = try sa.View.fromProto(a, attributes);
    _ = try sa.View.fromProto(a, .{ .indexed_fields = .fromOwnedSlice(attributes.indexed_fields.items[0..1]) });
}

fn saViewGet(a: Allocator, view: sa.View) !void {
    _ = try view.get(a, customer_id);
    _ = try view.get(a, notes);
    _ = try view.get(a, priority);
    _ = try view.get(a, score);
    _ = try view.get(a, vip);
    _ = try view.get(a, due);
    _ = try view.get(a, regions);
}

fn saViewDupe(a: Allocator, view: sa.View) !void {
    _ = try view.dupe(a);
}

fn saViewToProto(a: Allocator, view: sa.View) !void {
    // toProto allocates once per call, so the case writes two views: the whole view and
    // its first attribute.
    _ = try view.toProto(a);
    _ = try (sa.View{ .fields = view.fields[0..1] }).toProto(a);
}

/// The encoded attributes of `updates` and the view of them, built in `a`.
fn saView(a: Allocator) !sa.View {
    return sa.View.fromProto(a, try sa.toProto(a, &updates, .start));
}

test "search_attributes.toProto" {
    try alloc_failures.run("search_attributes.toProto", .arena, saToProto, .{sa.Purpose.upsert});
}

test "search_attributes.encodeValue" {
    try alloc_failures.run("search_attributes.encodeValue", .arena, saEncodeValue, .{});
}

test "search_attributes.View.fromProto" {
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const attributes = try sa.toProto(arena.allocator(), &updates, .upsert);
    try alloc_failures.run("search_attributes.View.fromProto", .arena, saViewFromProto, .{attributes});
}

test "search_attributes.View.get" {
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    try alloc_failures.run("search_attributes.View.get", .arena, saViewGet, .{try saView(arena.allocator())});
}

test "search_attributes.View.dupe" {
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    try alloc_failures.run("search_attributes.View.dupe", .arena, saViewDupe, .{try saView(arena.allocator())});
}

test "search_attributes.View.toProto" {
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    try alloc_failures.run("search_attributes.View.toProto", .arena, saViewToProto, .{try saView(arena.allocator())});
}

// ---------------------------------------------------------------- bytes fields of every size

/// Appends `count` payloads whose base64 `data` is `step`, 2 * `step`, ... characters long,
/// so that the allocation of some decoded `bytes` value is the one that needs a new chunk
/// of the generated decoder's arena.
fn appendSizedPayloads(gpa: Allocator, out: *std.ArrayList(u8), count: usize, step: usize) !void {
    try out.appendSlice(gpa, "\"payloads\":[");
    for (1..count + 1) |p| {
        if (p > 1) try out.append(gpa, ',');
        try out.appendSlice(gpa, "{\"data\":\"");
        for (0..p * step) |k| try out.append(gpa, "ABCDxy09"[k % 8]);
        try out.appendSlice(gpa, "\"}");
    }
    try out.append(gpa, ']');
}

fn protojsonDecodePayloads(a: Allocator, json: []const u8) !void {
    _ = try protojson.decode(proto.temporal.api.common.v1.Payloads, a, json);
}

test "protojson.decode reports OutOfMemory on bytes fields of every size" {
    const gpa = std.testing.allocator;
    var json: std.ArrayList(u8) = .empty;
    defer json.deinit(gpa);
    try json.append(gpa, '{');
    try appendSizedPayloads(gpa, &json, 32, 32);
    try json.append(gpa, '}');
    try alloc_failures.run("protojson.decode", .arena, protojsonDecodePayloads, .{@as([]const u8, json.items)});
}

test "history.fromJson reports OutOfMemory on payloads of every size" {
    const gpa = std.testing.allocator;
    var json: std.ArrayList(u8) = .empty;
    defer json.deinit(gpa);
    try json.appendSlice(gpa,
        \\{"events":[{"eventId":"1","eventType":"WorkflowExecutionStarted","workflowExecutionStartedEventAttributes":{"workflowType":{"name":"Checkout"},"taskQueue":{"name":"orders","kind":"Normal"},"input":{
    );
    try appendSizedPayloads(gpa, &json, 300, 3);
    try json.appendSlice(gpa, "}}}]}");
    try alloc_failures.run("history.fromJson", .arena, historyFromJson, .{@as([]const u8, json.items)});
}

// ---------------------------------------------------------------- Nexus wire

const common = proto.temporal.api.common.v1;

/// A Nexus link to a workflow event with percent-encoded segments and query values, and the
/// event type in PascalCase, so the reading decodes each part and spells the type again.
const nexus_link: proto.temporal.api.nexus.v1.Link = .{
    .url = "temporal:///namespaces/d%C3%A9j%C3%A0/workflows/order%2F42/run%201/history?referenceType=RequestIdReference&requestID=req+7%2B8&eventType=NexusOperationScheduled",
    .type = wire.workflow_event_link_type,
};

/// The Temporal form of a link to a workflow event, with segments that need encoding.
const temporal_link: common.Link = .{ .variant = .{ .workflow_event = .{
    .namespace = "d\u{e9}j\u{e0}",
    .workflow_id = "order/42",
    .run_id = "run 1",
    .reference = .{ .event_ref = .{ .event_id = 7, .event_type = .EVENT_TYPE_NEXUS_OPERATION_SCHEDULED } },
} } };

fn nexusFromLink(a: Allocator, link: proto.temporal.api.nexus.v1.Link) !void {
    var why: []const u8 = "";
    _ = try wire.fromNexusLink(a, link, &why);
}

fn nexusToLink(a: Allocator, link: common.Link) !void {
    var why: []const u8 = "";
    _ = try wire.toNexusLink(a, link, &why);
}

fn nexusHandlerFailure(a: Allocator, cause: *const Failure) !void {
    _ = try wire.handlerFailure(a, .{
        .type = .internal,
        .message = "inventory unavailable",
        .retry = .non_retryable,
        .cause = .{ .message = "warehouse down", .type = "WarehouseDown", .details = &.{.{ .data = "\"east\"" }}, .cause = cause },
    });
}

fn nexusOperationFailure(a: Allocator, cause: *const Failure) !void {
    // A failed operation copies its cause chain; a canceled one copies its details.
    _ = try wire.operationFailure(a, .{ .state = .failed, .failure = .{
        .message = "reservation failed",
        .type = "ReservationFailed",
        .details = &.{.{ .data = "\"r-9\"" }},
        .cause = cause,
    } });
    _ = try wire.operationFailure(a, .{ .state = .canceled, .failure = .{
        .message = "reservation canceled",
        .details = &.{ .{ .data = "\"r-9\"" }, .{ .data = "2" } },
    } });
}

test "nexus.wire.fromNexusLink" {
    var why: []const u8 = "";
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    // The input is valid: with no failure it converts.
    _ = try wire.fromNexusLink(arena.allocator(), nexus_link, &why);
    try alloc_failures.run("nexus.wire.fromNexusLink", .arena, nexusFromLink, .{nexus_link});
}

test "nexus.wire.toNexusLink" {
    try alloc_failures.run("nexus.wire.toNexusLink", .arena, nexusToLink, .{temporal_link});
}

test "nexus.wire.handlerFailure" {
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const chain = try failureChain(arena.allocator());
    try alloc_failures.run("nexus.wire.handlerFailure", .arena, nexusHandlerFailure, .{chain});
}

test "nexus.wire.operationFailure" {
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const chain = try failureChain(arena.allocator());
    try alloc_failures.run("nexus.wire.operationFailure", .arena, nexusOperationFailure, .{chain});
}

// ---------------------------------------------------------------- schedule

const schedule_pb = proto.temporal.api.schedule.v1;
const DescribeScheduleResponse = proto.temporal.api.workflowservice.v1.DescribeScheduleResponse;

/// A schedule with calendars, a skip calendar, intervals and cron expressions, a workflow
/// start with arguments, a memo and search attributes, and every policy set, built in `a`.
fn scheduleValue(a: Allocator) !schedule.Schedule {
    const action = try schedule.startWorkflowByName(a, "Checkout", .{ order, @as([]const u8, "nightly") }, .{
        .id = "checkout-nightly",
        .task_queue = "orders",
        .run_timeout = .fromSeconds(600),
        .memo = try converter.toFields(a, .{ .owner = "billing", .order = order }),
        .search_attributes = &.{ customer_id.set("c-42"), priority.set(3), regions.set(&.{ "eu-west", "us-east" }) },
    });
    return .{
        .action = action,
        .spec = .{
            .calendars = &.{
                .{ .hour = &.{.{ .start = 9, .end = 17, .step = 2 }}, .day_of_week = &.{.{ .start = 1, .end = 5 }}, .comment = "weekdays" },
                .{ .minute = &.{ .{ .start = 0 }, .{ .start = 30 } }, .year = &.{.{ .start = 2027 }} },
            },
            // Duration.fromSeconds runs at run time, so a literal here would be an array in
            // this function's frame; the intervals are copied into `a` to outlive it.
            .intervals = try a.dupe(schedule.Interval, &.{
                .{ .every = .fromSeconds(3600), .offset = .fromSeconds(15 * 60) },
                .{ .every = .fromSeconds(86400) },
            }),
            .cron_expressions = &.{ "0 12 * * MON-FRI", "@hourly" },
            .skip = &.{.{ .month = &.{.{ .start = 12 }}, .day_of_month = &.{.{ .start = 25 }} }},
            .start_at = .fromNanoseconds(1_795_000_000_000_000_000),
            .jitter = .fromSeconds(30),
            .time_zone_name = "America/New_York",
        },
        .policies = .{ .overlap = .buffer_one, .catchup_window = .fromSeconds(3600), .pause_on_failure = true, .keep_original_workflow_id = true },
        .state = .{ .note = "nightly checkout", .paused = true, .remaining_actions = 10 },
    };
}

fn scheduleToProto(a: Allocator, s: schedule.Schedule, base: ?schedule_pb.Schedule) !void {
    // Once with no base and once over the schedule as described, as an update does.
    _ = try s.toProto(a, null);
    _ = try s.toProto(a, base);
}

fn scheduleFromProto(a: Allocator, s: schedule_pb.Schedule) !void {
    _ = try schedule.Schedule.fromProto(a, s);
}

fn scheduleDescriptionFromProto(a: Allocator, response: DescribeScheduleResponse) !void {
    _ = try schedule.Description.fromProto(a, "checkout-nightly", response);
}

/// A describe response for `scheduleValue`, with running workflows, recent actions, future
/// action times, a memo and search attributes, built in `a`.
fn describeResponse(a: Allocator) !DescribeScheduleResponse {
    const s = try (try scheduleValue(a)).toProto(a, null);
    const running = try a.dupe(common.WorkflowExecution, &.{
        .{ .workflow_id = "checkout-nightly-1", .run_id = "r1" },
        .{ .workflow_id = "checkout-nightly-2", .run_id = "r2" },
    });
    const recent = try a.dupe(schedule_pb.ScheduleActionResult, &.{
        .{ .schedule_time = .{ .seconds = 1_795_000_000 }, .actual_time = .{ .seconds = 1_795_000_001 }, .start_workflow_result = .{ .workflow_id = "checkout-nightly-1", .run_id = "r1" } },
        .{ .schedule_time = .{ .seconds = 1_795_003_600 }, .actual_time = .{ .seconds = 1_795_003_602 }, .start_workflow_result = .{ .workflow_id = "checkout-nightly-2", .run_id = "r2" } },
    });
    const future = try a.dupe(proto.google.protobuf.Timestamp, &.{ .{ .seconds = 1_795_007_200 }, .{ .seconds = 1_795_010_800 } });
    const memo_fields = try converter.toFields(a, .{ .owner = "billing", .order = order });
    var memo: common.Memo = .{};
    for (memo_fields) |f| try memo.fields.append(a, .{ .key = f.name, .value = f.value });
    return .{
        .schedule = s,
        .info = .{
            .action_count = 2,
            .running_workflows = .fromOwnedSlice(running),
            .recent_actions = .fromOwnedSlice(recent),
            .future_action_times = .fromOwnedSlice(future),
            .create_time = .{ .seconds = 1_794_990_000 },
        },
        .memo = memo,
        .search_attributes = try sa.toProto(a, &updates, .upsert),
    };
}

test "schedule.Schedule.toProto" {
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const s = try scheduleValue(arena.allocator());
    const base = try s.toProto(arena.allocator(), null);
    try alloc_failures.run("schedule.Schedule.toProto", .arena, scheduleToProto, .{ s, @as(?schedule_pb.Schedule, base) });
}

test "schedule.Schedule.fromProto" {
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const s = try (try scheduleValue(arena.allocator())).toProto(arena.allocator(), null);
    try alloc_failures.run("schedule.Schedule.fromProto", .arena, scheduleFromProto, .{s});
}

test "schedule.Description.fromProto" {
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    try alloc_failures.run("schedule.Description.fromProto", .arena, scheduleDescriptionFromProto, .{try describeResponse(arena.allocator())});
}

// ---------------------------------------------------------------- propagation

const tenant = propagation.key([]const u8, "tenant");
const Caller = struct { user: []const u8, roles: []const []const u8, level: u8 };
const caller = propagation.key(Caller, "caller");

const propagators = [_]propagation.ContextPropagator{
    propagation.HeaderPropagator(tenant).propagator,
    propagation.HeaderPropagator(caller).propagator,
};

fn propagationInjectAll(a: Allocator, values: *const propagation.Values) !void {
    var header: converter.HeaderWriter = .init(a);
    try propagation.injectAll(&propagators, a, values, &header);
}

fn propagationExtractAll(a: Allocator, header: converter.Header) !void {
    var values: propagation.Values = .init(a);
    try propagation.extractAll(&propagators, a, header, &values, "a test call");
}

/// A store with a value for each of `propagators`, built in `a`.
fn propagatedValues(a: Allocator) !propagation.Values {
    var values: propagation.Values = .init(a);
    try values.put(tenant, "acme");
    try values.put(caller, .{ .user = "ada", .roles = &.{ "admin", "billing" }, .level = 3 });
    return values;
}

test "propagation.injectAll" {
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const values = try propagatedValues(arena.allocator());
    try alloc_failures.run("propagation.injectAll", .arena, propagationInjectAll, .{&values});
}

test "propagation.extractAll" {
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const values = try propagatedValues(a);
    var header: converter.HeaderWriter = .init(a);
    try propagation.injectAll(&propagators, a, &values, &header);
    // The input is valid: with no failure both propagators read their value back.
    var back: propagation.Values = .init(a);
    try propagation.extractAll(&propagators, a, header.header(), &back, "a test call");
    try std.testing.expectEqualStrings("acme", back.get(tenant).?);
    try std.testing.expectEqualStrings("billing", back.get(caller).?.roles[1]);
    try alloc_failures.run("propagation.extractAll", .arena, propagationExtractAll, .{header.header()});
}

// ---------------------------------------------------------------- client

/// A profile with every key `loadOptions` lifts: address, namespace, API key, TLS settings
/// and gRPC metadata.
const client_config =
    \\[profile.prod]
    \\address = "prod.example.com:7233"
    \\namespace = "orders"
    \\api_key = "secret-key"
    \\[profile.prod.tls]
    \\server_name = "prod.example.com"
    \\server_ca_cert_path = "/etc/temporal/ca.pem"
    \\client_cert_data = "-----BEGIN CERTIFICATE-----"
    \\client_key_path = "/etc/temporal/client.key"
    \\[profile.prod.grpc_meta]
    \\region = "eu"
    \\team = "billing"
;

fn clientLoadOptions(a: Allocator, env: *const std.process.Environ.Map) !void {
    _ = try client.loadOptions(a, .{ .config_data = client_config, .env = env });
}

test "client.loadOptions" {
    // The environment is this map, not the process's, and the config is `client_config`, not
    // a file: TEMPORAL_PROFILE picks the profile and the other variables override it.
    var env: std.process.Environ.Map = .init(std.testing.allocator);
    defer env.deinit();
    try env.put("TEMPORAL_PROFILE", "prod");
    try env.put("TEMPORAL_NAMESPACE", "orders-eu");
    try env.put("TEMPORAL_GRPC_META_TRACE", "on");
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const options = try client.loadOptions(arena.allocator(), .{ .config_data = client_config, .env = &env });
    try std.testing.expectEqualStrings("prod.example.com:7233", options.host_port);
    try std.testing.expectEqualStrings("orders-eu", options.namespace);
    try std.testing.expectEqualStrings("secret-key", options.api_key);
    try std.testing.expectEqualStrings("/etc/temporal/ca.pem", options.tls.?.server_root_ca_cert_path);
    try std.testing.expectEqual(3, options.headers.len);
    try alloc_failures.run("client.loadOptions", .arena, clientLoadOptions, .{@as(*const std.process.Environ.Map, &env)});
}

test "every known finding names a case" {
    try alloc_failures.expectFindingsHaveCases(@embedFile("alloc_failure.zig"));
}

/// Writes over the stack below the caller's frame, where a function the caller has returned
/// from kept its locals, so a slice that still points there reads something else.
noinline fn overwriteDeadStack() void {
    var scratch: [64 * 1024]u8 = undefined;
    @memset(&scratch, 0xaa);
    std.mem.doNotOptimizeAway(&scratch);
}

test "the schedule cases' input outlives the function that builds it" {
    // scheduleValue's intervals must not point into its own returned-from stack frame: the
    // schedule cases read them after it returns (in ReleaseSafe, toProto panics on them).
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const s = try scheduleValue(arena.allocator());
    overwriteDeadStack();
    try std.testing.expectEqual(2, s.spec.intervals.len);
    try std.testing.expectEqual(std.Io.Duration.fromSeconds(3600), s.spec.intervals[0].every);
    try std.testing.expectEqual(std.Io.Duration.fromSeconds(15 * 60), s.spec.intervals[0].offset);
    try std.testing.expectEqual(std.Io.Duration.fromSeconds(86400), s.spec.intervals[1].every);
}
