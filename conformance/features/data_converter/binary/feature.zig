const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");
const converter = temporal.converter;
const Payload = converter.Payload;

const Workflow = temporal.workflow.Def("Workflow", echo);

const expected_bytes = "\xde\xad\xbe\xef";

/// features/data_converter/binary/payload.json of temporalio/features at commit
/// a132d35815137c2e8fdc7767c52907c724f48c68, verbatim: neither the feature's process nor
/// `zig build test` can read the suite's directory.
const expected_payload_json =
    \\{ "metadata": { "encoding": "YmluYXJ5L3BsYWlu" }, "data": "3q2+7w==" }
;

pub const feature = harness.define(.{ .workflows = .{Workflow}, .execute = execute });

fn echo(_: *temporal.workflow.Context, value: converter.Bytes) !converter.Bytes {
    return value;
}

fn execute(ctx: *harness.Context) !void {
    var run = try ctx.client.executeWorkflow(Workflow, ctx.startOptions(), .{converter.Bytes{ .data = expected_bytes }});
    defer run.deinit();
    const result = try run.get(ctx.arena);
    try ctx.expect(std.mem.eql(u8, expected_bytes, result.data), "client result is {x}, expected {x}", .{ result.data, expected_bytes });

    const recorded = try ctx.fetchRawHistory(run.id, run.run_id);
    const input = harness.history.inputPayloads(recorded);
    const output = harness.history.resultPayloads(recorded);
    try ctx.expect(input.len == 1 and output.len == 1, "expected one input and output payload, got {d} and {d}", .{ input.len, output.len });

    const expected = try temporal.protojson.decode(Payload, ctx.arena, expected_payload_json);
    try expectSamePayload(ctx, "result payload", expected, output[0]);
    try expectSamePayload(ctx, "input payload", output[0], input[0]);
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

fn metadataValue(payload: Payload, key: []const u8) ?[]const u8 {
    for (payload.metadata.items) |entry| {
        if (std.mem.eql(u8, entry.key, key)) return entry.value;
    }
    return null;
}
