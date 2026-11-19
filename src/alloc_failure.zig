//! The cases of `zig build alloc-failures` (see AGENTS.md). Each case calls one SDK function
//! with each of its allocations failing in turn, through `alloc_failures.run` in
//! tools/checks/alloc_failures.zig, and expects error.OutOfMemory from every call that saw a
//! failure.
//!
//! Covered:
//! - converter.toPayload, converter.fromPayload, converter.toPayloads,
//!   converter.fromPayloads, converter.toFields, converter.fieldValue,
//!   converter.Header.fromEntries, converter.Header.toEntries
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

test "every known finding names a case" {
    try alloc_failures.expectFindingsHaveCases(@embedFile("alloc_failure.zig"));
}
