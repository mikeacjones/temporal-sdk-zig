//! Payload comparisons the data_converter features share.
const std = @import("std");
const temporal = @import("temporal");
const Context = @import("Context.zig");

const converter = temporal.converter;

/// Payloads are equal when their data and their metadata entries are, in any order: the
/// server does not keep a payload's metadata in the order it was sent.
pub fn expectSamePayload(ctx: *Context, what: []const u8, expected: converter.Payload, actual: converter.Payload) error{FeatureFailed}!void {
    try ctx.expect(std.mem.eql(u8, expected.data, actual.data), "{s}: data is {x}, expected {x}", .{ what, actual.data, expected.data });
    try ctx.expect(expected.metadata.items.len == actual.metadata.items.len, "{s}: {d} metadata entries, expected {d}", .{ what, actual.metadata.items.len, expected.metadata.items.len });
    for (expected.metadata.items) |entry| {
        const value = metadataValue(actual, entry.key) orelse return ctx.fail("{s}: no metadata entry \"{s}\"", .{ what, entry.key });
        try ctx.expect(std.mem.eql(u8, entry.value, value), "{s}: metadata \"{s}\" is \"{s}\", expected \"{s}\"", .{ what, entry.key, value, entry.value });
    }
}

/// The value of metadata entry `key`, or null.
pub fn metadataValue(payload: converter.Payload, key: []const u8) ?[]const u8 {
    for (payload.metadata.items) |entry| {
        if (std.mem.eql(u8, entry.key, key)) return entry.value;
    }
    return null;
}

const Entry = converter.Payload.MetadataEntry;

fn testPayload(data: []const u8, entries: []Entry) converter.Payload {
    return .{ .data = data, .metadata = .fromOwnedSlice(entries) };
}

test "payloads metadataValue finds an entry by key" {
    var entries = [_]Entry{
        .{ .key = "encoding", .value = "binary/plain" },
        .{ .key = "messageType", .value = "x.Y" },
    };
    const payload = testPayload("", &entries);
    try std.testing.expectEqualStrings("binary/plain", metadataValue(payload, "encoding").?);
    try std.testing.expectEqualStrings("x.Y", metadataValue(payload, "messageType").?);
    try std.testing.expectEqual(null, metadataValue(payload, "missing"));
}

test "payloads expectSamePayload ignores metadata order and reports differences" {
    var arena_state: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena_state.deinit();
    var ctx: Context = .{
        .gpa = std.testing.allocator,
        .io = std.testing.io,
        .arena = arena_state.allocator(),
        .client = undefined,
        .feature = "data_converter/test",
        .namespace = "default",
        .task_queue = "conformance-data_converter/test",
    };

    var expected_entries = [_]Entry{
        .{ .key = "encoding", .value = "binary/plain" },
        .{ .key = "messageType", .value = "x.Y" },
    };
    var reordered = [_]Entry{
        .{ .key = "messageType", .value = "x.Y" },
        .{ .key = "encoding", .value = "binary/plain" },
    };
    const expected = testPayload("\xde\xad", &expected_entries);
    try expectSamePayload(&ctx, "result", expected, testPayload("\xde\xad", &reordered));
    try std.testing.expectEqual(null, ctx.failure_message_);

    try std.testing.expectError(error.FeatureFailed, expectSamePayload(&ctx, "result", expected, testPayload("\xbe\xef", &reordered)));
    try std.testing.expectEqualStrings("result: data is beef, expected dead", ctx.failure_message_.?);

    var fewer = [_]Entry{.{ .key = "encoding", .value = "binary/plain" }};
    try std.testing.expectError(error.FeatureFailed, expectSamePayload(&ctx, "input", expected, testPayload("\xde\xad", &fewer)));
    try std.testing.expectEqualStrings("input: 1 metadata entries, expected 2", ctx.failure_message_.?);

    var other_key = [_]Entry{
        .{ .key = "encoding", .value = "binary/plain" },
        .{ .key = "other", .value = "x.Y" },
    };
    try std.testing.expectError(error.FeatureFailed, expectSamePayload(&ctx, "input", expected, testPayload("\xde\xad", &other_key)));
    try std.testing.expectEqualStrings("input: no metadata entry \"messageType\"", ctx.failure_message_.?);

    var other_value = [_]Entry{
        .{ .key = "encoding", .value = "json/plain" },
        .{ .key = "messageType", .value = "x.Y" },
    };
    try std.testing.expectError(error.FeatureFailed, expectSamePayload(&ctx, "input", expected, testPayload("\xde\xad", &other_value)));
    try std.testing.expectEqualStrings("input: metadata \"encoding\" is \"json/plain\", expected \"binary/plain\"", ctx.failure_message_.?);
}
