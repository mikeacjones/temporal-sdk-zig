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
    try harness.payloads.expectSamePayload(ctx, "result payload", expected, output[0]);
    try harness.payloads.expectSamePayload(ctx, "input payload", output[0], input[0]);
}
