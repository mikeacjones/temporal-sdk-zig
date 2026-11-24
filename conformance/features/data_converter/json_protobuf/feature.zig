const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");
const converter = temporal.converter;
const DataBlob = temporal.proto.temporal.api.common.v1.DataBlob;

const Workflow = temporal.workflow.Def("Workflow", echo);

const expected_bytes = "\xde\xad\xbe\xef";

pub const feature = harness.define(.{ .workflows = .{Workflow}, .execute = execute });

// A generated message converts as `json/protobuf` by default.
fn echo(_: *temporal.workflow.Context, value: DataBlob) !DataBlob {
    return value;
}

fn execute(ctx: *harness.Context) !void {
    var run = try ctx.client.executeWorkflow(Workflow, ctx.startOptions(), .{DataBlob{ .data = expected_bytes }});
    defer run.deinit();
    const result = try run.get(ctx.arena);
    try ctx.expect(std.mem.eql(u8, expected_bytes, result.data), "client result data is {x}, expected {x}", .{ result.data, expected_bytes });

    const recorded = try ctx.fetchRawHistory(run.id, run.run_id);
    const input = harness.history.inputPayloads(recorded);
    const output = harness.history.resultPayloads(recorded);
    try ctx.expect(input.len == 1 and output.len == 1, "expected one input and output payload, got {d} and {d}", .{ input.len, output.len });
    try ctx.expectEqualStrings("json/protobuf", converter.encodingOf(output[0]) orelse "");
    try ctx.expectEqualStrings("temporal.api.common.v1.DataBlob", metadataValue(output[0], converter.message_type_key) orelse "");

    const in_history = temporal.protojson.decode(DataBlob, ctx.arena, output[0].data) catch |err|
        return ctx.fail("result payload is not a ProtoJSON DataBlob ({t}): {s}", .{ err, output[0].data });
    try ctx.expect(in_history.encoding_type == result.encoding_type, "history result encoding_type is {t}, client result's is {t}", .{ in_history.encoding_type, result.encoding_type });
    try ctx.expect(std.mem.eql(u8, result.data, in_history.data), "history result data is {x}, client result data is {x}", .{ in_history.data, result.data });

    try expectSamePayload(ctx, "input payload", output[0], input[0]);
}

fn metadataValue(payload: converter.Payload, key: []const u8) ?[]const u8 {
    for (payload.metadata.items) |entry| {
        if (std.mem.eql(u8, entry.key, key)) return entry.value;
    }
    return null;
}

/// Payloads are equal when their data and their metadata entries are, in any order: the
/// server does not keep a payload's metadata in the order it was sent.
fn expectSamePayload(ctx: *harness.Context, what: []const u8, expected: converter.Payload, actual: converter.Payload) !void {
    try ctx.expect(std.mem.eql(u8, expected.data, actual.data), "{s}: data is {x}, expected {x}", .{ what, actual.data, expected.data });
    try ctx.expect(expected.metadata.items.len == actual.metadata.items.len, "{s}: {d} metadata entries, expected {d}", .{ what, actual.metadata.items.len, expected.metadata.items.len });
    for (expected.metadata.items) |entry| {
        const value = metadataValue(actual, entry.key) orelse return ctx.fail("{s}: no metadata entry \"{s}\"", .{ what, entry.key });
        try ctx.expect(std.mem.eql(u8, entry.value, value), "{s}: metadata \"{s}\" is \"{s}\", expected \"{s}\"", .{ what, entry.key, value, entry.value });
    }
}
