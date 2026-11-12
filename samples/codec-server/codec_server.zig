//! Codec server: a workflow and an activity whose payloads a codec compresses with Snappy
//! before they leave the worker or the client. The server only ever stores the compressed
//! payloads, so the Temporal CLI and Web UI can't show them on their own; the codec server
//! program (server.zig) runs the same codec over HTTP so they can.

const std = @import("std");
const temporal = @import("temporal");
const snappy = @import("snappy.zig");
const workflow = temporal.workflow;
const activity = temporal.activity;
const converter = temporal.converter;
const Allocator = std.mem.Allocator;
const Io = std.Io;

const log = std.log.scoped(.codec_server);

pub const task_queue = "codecserver";
pub const workflow_id = "codecserver_workflowID";

/// Compresses every payload with Snappy: the payload is serialized whole and replaced by one
/// with encoding `binary/snappy` holding the compressed bytes. Decoding decompresses
/// `binary/snappy` payloads and passes others through. Set it with
/// `client.Options.codecs = &.{snappy_codec.codec()}`; it must outlive the client.
pub const SnappyCodec = struct {
    /// Decoding fails for a payload that declares more bytes than this, so corrupt or
    /// hostile data can't exhaust memory.
    max_decoded_len: usize = 256 * 1024 * 1024,

    pub const encoding = "binary/snappy";

    pub fn codec(s: *const SnappyCodec) converter.PayloadCodec {
        return .{ .ptr = @constCast(s), .vtable = &.{ .encode = encode, .decode = decode } };
    }

    fn encode(_: *anyopaque, arena: Allocator, _: Io, payloads: []const converter.Payload) converter.CodecError![]converter.Payload {
        const out = try arena.alloc(converter.Payload, payloads.len);
        for (payloads, out) |p, *o| {
            var serialized: Io.Writer.Allocating = .init(arena);
            p.encode(&serialized.writer, arena) catch return error.OutOfMemory;
            o.* = try converter.payloadWithEncoding(arena, encoding, try snappy.compress(arena, serialized.written()));
        }
        return out;
    }

    fn decode(ptr: *anyopaque, arena: Allocator, _: Io, payloads: []const converter.Payload) converter.CodecError![]converter.Payload {
        const s: *const SnappyCodec = @ptrCast(@alignCast(ptr));
        const out = try arena.dupe(converter.Payload, payloads);
        for (out, 0..) |*p, i| {
            const enc = converter.encodingOf(p.*) orelse continue;
            if (!std.mem.eql(u8, enc, encoding)) continue;
            const plain = snappy.decompress(arena, p.data, s.max_decoded_len) catch |err| switch (err) {
                error.OutOfMemory => return error.OutOfMemory,
                error.TooLarge => {
                    log.warn("decoding payload {d} with the snappy codec failed: it declares more than max_decoded_len ({d} bytes)", .{ i, s.max_decoded_len });
                    return error.CodecFailed;
                },
                error.Corrupt => {
                    log.warn("decoding payload {d} with the snappy codec failed: its data is not a Snappy block", .{i});
                    return error.CodecFailed;
                },
            };
            var reader: Io.Reader = .fixed(plain);
            p.* = converter.Payload.decode(&reader, arena) catch |err| switch (err) {
                error.OutOfMemory => return error.OutOfMemory,
                else => {
                    log.warn("decoding payload {d} with the snappy codec failed: the decompressed data is not a serialized payload ({t})", .{ i, err });
                    return error.CodecFailed;
                },
            };
        }
        return out;
    }
};

fn activityFn(ctx: *activity.Context, input: []const u8) ![]const u8 {
    ctx.logger().info("Activity", .{ .input = input });
    return std.fmt.allocPrint(ctx.allocator(), "Received {s}", .{input});
}

pub const Activity = activity.Def("Activity", activityFn);

/// The workflow and activity don't know their inputs and results are compressed: the
/// codec runs on the payloads after conversion.
fn codecServerWorkflow(ctx: *workflow.Context, input: []const u8) ![]const u8 {
    const ao: workflow.ActivityOptions = .{ .start_to_close_timeout = .fromSeconds(10) };
    const lao: workflow.LocalActivityOptions = .{ .start_to_close_timeout = .fromSeconds(10) };
    const logger = ctx.logger();
    logger.info("Codec Server workflow started", .{ .input = input });

    var result = (ctx.sideEffect(?[]const u8, ctx.allocator(), newUuid) catch |err| {
        logger.err("SideEffect failed.", .{ .err = err });
        return err;
    }) orelse return error.OutOfMemory;

    result = ctx.executeLocalActivity(Activity, lao, .{input}).get() catch |err| {
        logger.err("Local Activity failed.", .{ .err = err });
        return err;
    };

    result = ctx.executeActivity(Activity, ao, .{input}).get() catch |err| {
        logger.err("Activity failed.", .{ .err = err });
        return err;
    };

    logger.info("Codec Server workflow completed.", .{ .result = result });
    return result;
}

/// Random on purpose: `ctx.sideEffect` records the value, so a replay reads it from history
/// instead of calling this. The workflow's own `io` draws seeded, replayable randomness, so
/// this takes the process-wide one. Null when out of memory: a side effect returns failures
/// as values.
fn newUuid(arena: Allocator) ?[]const u8 {
    return temporal.client.uuid(arena, Io.Threaded.global_single_threaded.io()) catch null;
}

pub const Workflow = workflow.Def("Workflow", codecServerWorkflow);
