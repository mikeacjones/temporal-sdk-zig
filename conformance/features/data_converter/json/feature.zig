const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");
const converter = temporal.converter;

const Message = struct { spec: bool };
const Workflow = temporal.workflow.Def("Workflow", echo);

pub const feature = harness.define(.{ .workflows = .{Workflow}, .execute = execute });

fn echo(_: *temporal.workflow.Context, value: Message) !Message {
    return value;
}

fn execute(ctx: *harness.Context) !void {
    var run = try ctx.client.executeWorkflow(Workflow, ctx.startOptions(), .{Message{ .spec = true }});
    defer run.deinit();
    const result = try run.get(ctx.arena);
    try ctx.expect(result.spec, "client result spec is false, expected true", .{});

    const recorded = try ctx.fetchRawHistory(run.id, run.run_id);
    const input = harness.history.inputPayloads(recorded);
    const output = harness.history.resultPayloads(recorded);
    try ctx.expect(input.len == 1 and output.len == 1, "expected one input and output payload, got {d} and {d}", .{ input.len, output.len });
    try ctx.expectEqualStrings("json/plain", converter.encodingOf(output[0]) orelse "");
    const value = try std.json.parseFromSliceLeaky(Message, ctx.arena, output[0].data, .{});
    try ctx.expect(value.spec == result.spec, "history result spec is {}, client result spec is {}", .{ value.spec, result.spec });

    var encoded_input: std.Io.Writer.Allocating = .init(ctx.arena);
    var encoded_output: std.Io.Writer.Allocating = .init(ctx.arena);
    try input[0].encode(&encoded_input.writer, ctx.arena);
    try output[0].encode(&encoded_output.writer, ctx.arena);
    try ctx.expectEqualStrings(encoded_input.written(), encoded_output.written());
}
