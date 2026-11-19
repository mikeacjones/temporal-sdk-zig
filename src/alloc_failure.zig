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

test "every known finding names a case" {
    try alloc_failures.expectFindingsHaveCases(@embedFile("alloc_failure.zig"));
}
