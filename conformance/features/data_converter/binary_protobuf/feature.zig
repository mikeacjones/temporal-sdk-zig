const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");
const converter = temporal.converter;
const DataBlob = temporal.proto.temporal.api.common.v1.DataBlob;
const Blob = converter.ProtoBinary(DataBlob);

const Workflow = temporal.workflow.Def("Workflow", echo);

const expected_bytes = "\xde\xad\xbe\xef";

pub const feature = harness.define(.{ .workflows = .{Workflow}, .execute = execute });

fn echo(_: *temporal.workflow.Context, value: Blob) !Blob {
    return value;
}

fn execute(ctx: *harness.Context) !void {
    var run = try ctx.client.executeWorkflow(Workflow, ctx.startOptions(), .{Blob{ .message = .{ .data = expected_bytes } }});
    defer run.deinit();
    const result = try run.get(ctx.arena);
    try ctx.expect(std.mem.eql(u8, expected_bytes, result.message.data), "client result data is {x}, expected {x}", .{ result.message.data, expected_bytes });

    const recorded = try ctx.fetchRawHistory(run.id, run.run_id);
    const input = harness.history.inputPayloads(recorded);
    const output = harness.history.resultPayloads(recorded);
    try ctx.expect(input.len == 1 and output.len == 1, "expected one input and output payload, got {d} and {d}", .{ input.len, output.len });
    try ctx.expectEqualStrings("binary/protobuf", converter.encodingOf(output[0]) orelse "");
    try ctx.expectEqualStrings("temporal.api.common.v1.DataBlob", harness.payloads.metadataValue(output[0], converter.message_type_key) orelse "");

    var reader: std.Io.Reader = .fixed(output[0].data);
    const in_history = DataBlob.decode(&reader, ctx.arena) catch |err|
        return ctx.fail("result payload is not a binary DataBlob: {t}", .{err});
    try ctx.expect(in_history.encoding_type == result.message.encoding_type, "history result encoding_type is {t}, client result's is {t}", .{ in_history.encoding_type, result.message.encoding_type });
    try ctx.expect(std.mem.eql(u8, result.message.data, in_history.data), "history result data is {x}, client result data is {x}", .{ in_history.data, result.message.data });

    try harness.payloads.expectSamePayload(ctx, "input payload", output[0], input[0]);
}
