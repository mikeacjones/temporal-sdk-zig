const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");
const converter = temporal.converter;
const Payload = converter.Payload;
const base64 = std.base64.standard;

const Message = struct { spec: bool };
const Workflow = temporal.workflow.Def("Workflow", echo);

const encoding = "my-encoding";
var codec_context: u8 = 0;
const codecs = [_]converter.PayloadCodec{.{
    .ptr = &codec_context,
    .vtable = &.{ .encode = encode, .decode = decode },
}};

pub const feature = harness.define(.{
    .workflows = .{Workflow},
    .configureClient = configureClient,
    .execute = execute,
});

fn configureClient(options: *temporal.client.Options) void {
    options.codecs = &codecs;
}

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
    try ctx.expectEqualStrings(encoding, converter.encodingOf(output[0]) orelse "");
    const inner = (try codecs[0].decode(ctx.arena, ctx.io, output))[0];
    try ctx.expectEqualStrings("json/plain", converter.encodingOf(inner) orelse "");
    const value = try std.json.parseFromSliceLeaky(Message, ctx.arena, inner.data, .{});
    try ctx.expect(value.spec == result.spec, "decoded history result spec is {}, client result spec is {}", .{ value.spec, result.spec });

    var encoded_input: std.Io.Writer.Allocating = .init(ctx.arena);
    var encoded_output: std.Io.Writer.Allocating = .init(ctx.arena);
    try input[0].encode(&encoded_input.writer, ctx.arena);
    try output[0].encode(&encoded_output.writer, ctx.arena);
    try ctx.expectEqualStrings(encoded_input.written(), encoded_output.written());
}

fn encode(_: *anyopaque, arena: std.mem.Allocator, _: std.Io, payloads: []const Payload) converter.CodecError![]Payload {
    const output = try arena.alloc(Payload, payloads.len);
    for (payloads, output) |payload, *outer| {
        var bytes: std.Io.Writer.Allocating = .init(arena);
        payload.encode(&bytes.writer, arena) catch return error.OutOfMemory;
        const plain = bytes.written();
        const encoded = try arena.alloc(u8, base64.Encoder.calcSize(plain.len));
        outer.* = try converter.payloadWithEncoding(arena, encoding, base64.Encoder.encode(encoded, plain));
    }
    return output;
}

fn decode(_: *anyopaque, arena: std.mem.Allocator, _: std.Io, payloads: []const Payload) converter.CodecError![]Payload {
    const output = try arena.dupe(Payload, payloads);
    for (output) |*payload| {
        const tag = converter.encodingOf(payload.*) orelse continue;
        if (!std.mem.eql(u8, tag, encoding)) continue;
        const len = base64.Decoder.calcSizeForSlice(payload.data) catch {
            std.log.warn("decoding {s} payload failed: invalid Base64 size", .{encoding});
            return error.CodecFailed;
        };
        const plain = try arena.alloc(u8, len);
        base64.Decoder.decode(plain, payload.data) catch {
            std.log.warn("decoding {s} payload failed: invalid Base64 data", .{encoding});
            return error.CodecFailed;
        };
        var reader: std.Io.Reader = .fixed(plain);
        payload.* = Payload.decode(&reader, arena) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            else => {
                std.log.warn("decoding {s} payload failed: invalid protobuf payload ({t})", .{ encoding, err });
                return error.CodecFailed;
            },
        };
    }
    return output;
}
