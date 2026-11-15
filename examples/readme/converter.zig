//! README.md examples. The code between `// example: <id>` and `// end-example` is copied into
//! README.md by `zig build readme`; edit it here, never in README.md.

const std = @import("std");
const temporal = @import("temporal");

const converter = temporal.converter;
const workflow = temporal.workflow;
const activity = temporal.activity;
const testing = std.testing;
const Allocator = std.mem.Allocator;

/// Sent as decimal text with its own encoding instead of as JSON.
const Cents = struct {
    value: u64,

    pub fn toPayload(c: Cents, arena: Allocator) error{OutOfMemory}!converter.Payload {
        return converter.payloadWithEncoding(arena, "text/cents", try std.fmt.allocPrint(arena, "{d}", .{c.value}));
    }

    pub fn fromPayload(_: Allocator, p: converter.Payload) converter.DecodeError!Cents {
        const enc = converter.encodingOf(p) orelse return error.UnknownEncoding;
        if (!std.mem.eql(u8, enc, "text/cents")) return error.UnknownEncoding;
        return .{ .value = std.fmt.parseInt(u64, p.data, 10) catch return error.InvalidPayload };
    }
};

test "converter: values round-trip through payloads" {
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    // example: converter-payloads
    const Request = struct { name: []const u8, language: ?[]const u8 = null };
    const json = try converter.toPayload(arena, Request{ .name = "Ziggy" }); // "json/plain"
    try testing.expectEqualStrings("Ziggy", (try converter.fromPayload(Request, arena, json)).name);

    // Cents declares toPayload and fromPayload, so it encodes itself.
    const tip = try converter.toPayload(arena, Cents{ .value = 250 });
    try testing.expectEqual(250, (try converter.fromPayload(Cents, arena, tip)).value);

    const Execution = temporal.proto.temporal.api.common.v1.WorkflowExecution;
    const execution: Execution = .{ .workflow_id = "greeting-1" };
    const proto_json = try converter.toPayload(arena, execution); // "json/protobuf"
    const proto_binary = try converter.toPayload(arena, converter.ProtoBinary(Execution){ .message = execution });
    try testing.expectEqualStrings("binary/protobuf", converter.encodingOf(proto_binary).?);

    // A value whose type the caller learns later stays encoded until read.
    const later: converter.EncodedValue = .{ .payload = proto_json };
    try testing.expectEqualStrings("greeting-1", (try later.get(Execution, arena)).workflow_id);
    // end-example
    try testing.expectEqualStrings("json/plain", converter.encodingOf(json).?);
    try testing.expectEqualStrings("json/protobuf", converter.encodingOf(proto_json).?);
}

test "converter: compress payloads with the zlib codec" {
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const io = testing.io;
    const name = try arena.alloc(u8, 400);
    @memset(name, 'z');

    // example: converter-codecs
    const zlib: converter.ZlibCodec = .{};
    const codecs = [_]converter.PayloadCodec{zlib.codec()};

    // What a client with `.codecs = &codecs` does to every payload it sends and receives.
    const plain = try converter.toPayload(arena, name);
    const encoded = try converter.encodePayloads(&codecs, arena, io, &.{plain});
    try testing.expectEqualStrings("binary/zlib", converter.encodingOf(encoded[0]).?);
    const decoded = try converter.decodePayloads(&codecs, arena, io, encoded);
    try testing.expectEqualStrings(name, try converter.fromPayload([]const u8, arena, decoded[0]));

    const env = try temporal.testing.WorkflowEnvironment.startWithOptions(testing.allocator, io, .{ .codecs = &codecs });
    defer env.deinit();
    // end-example
    try testing.expect(encoded[0].data.len < plain.data.len);
    try env.registerWorkflow(Greeting);
    try env.registerActivity(Greet);
    const result = try env.executeWorkflow(Greeting, .{name}, arena, null);
    try testing.expectEqualStrings(try std.fmt.allocPrint(arena, "Hello, {s}!", .{name}), result);
}

// example: converter-failures
fn greet(ctx: *activity.Context, name: []const u8) ![]const u8 {
    if (name.len == 0) return ctx.fail(.{ .message = "name is empty", .type = "InvalidName", .non_retryable = true });
    return std.fmt.allocPrint(ctx.allocator(), "Hello, {s}!", .{name});
}

fn greeting(ctx: *workflow.Context, name: []const u8) ![]const u8 {
    const future = ctx.executeActivity(Greet, .{ .start_to_close_timeout = .fromSeconds(10) }, .{name});
    return future.get() catch |err| {
        if (future.failure()) |f| {
            // For an empty name: "name is empty", type "InvalidName".
            ctx.logger().warn("greet failed", .{ .reason = temporal.failure.rootCause(f).message });
        }
        return err;
    };
}
// end-example

const Greet = activity.Def("greet", greet);
const Greeting = workflow.Def("greeting", greeting);

test "converter: a failure's cause chain reaches the caller" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(Greeting);
    try env.registerActivity(Greet);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var run = try env.startWorkflow(Greeting, .{}, .{""});
    defer run.deinit();
    try testing.expectError(error.WorkflowFailed, env.getResult(Greeting, &run, arena.allocator(), null));

    // The workflow failed with the activity's failure, caused by greet's application failure.
    const root = temporal.failure.rootCause(run.failure().?);
    try testing.expectEqualStrings("name is empty", root.message);
    try testing.expectEqualStrings("InvalidName", temporal.failure.applicationType(root.*).?);
}

// example: converter-fail-with
fn checkout(ctx: *workflow.Context, order: []const u8) !void {
    const opts: workflow.ActivityOptions = .{ .start_to_close_timeout = .fromSeconds(10) };
    try ctx.executeActivity(Hold, opts, .{order}).get();
    // Releases the hold if anything below fails; the release can fail too.
    errdefer ctx.executeActivity(Release, opts, .{order}).get() catch {};

    const charge = ctx.executeActivity(Charge, opts, .{order});
    // Keeps the charge's failure: without failWith, a failed release would replace it.
    // Other errors, such as error.Canceled, are returned as they are.
    charge.get() catch |err| return switch (err) {
        error.ActivityFailed => ctx.failWith(charge.failure().?),
        else => err,
    };
}
// end-example

fn holdFunds(_: *activity.Context, _: []const u8) !void {}
fn chargeCard(ctx: *activity.Context, _: []const u8) !void {
    return ctx.fail(.{ .message = "card declined", .type = "Declined", .non_retryable = true });
}
fn releaseFunds(ctx: *activity.Context, _: []const u8) !void {
    return ctx.fail(.{ .message = "release failed", .non_retryable = true });
}

const Hold = activity.Def("hold", holdFunds);
const Charge = activity.Def("charge", chargeCard);
const Release = activity.Def("release", releaseFunds);
const Checkout = workflow.Def("checkout", checkout);

test "converter: failWith keeps a failure that a failed cleanup would replace" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(Checkout);
    try env.registerActivity(Hold);
    try env.registerActivity(Charge);
    try env.registerActivity(Release);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var failure: ?temporal.failure.Failure = null;
    try testing.expectError(error.WorkflowFailed, env.executeWorkflow(Checkout, .{"order-1"}, arena.allocator(), &failure));
    try testing.expectEqualStrings("card declined", temporal.failure.rootCause(&failure.?).message);
    try testing.expectEqualStrings("charge", failure.?.failure_info.?.activity_failure_info.activity_type.?.name);
}

// example: converter-failure-kinds
fn reserve(ctx: *activity.Context, sku: []const u8) !void {
    const why = try temporal.failure.application(ctx.allocator(), .{ .message = "warehouse offline", .type = "WarehouseOffline" });
    return ctx.fail(.{
        .message = "reservation failed",
        .type = "ReserveFailed",
        .non_retryable = true,
        .details = &.{try converter.toPayload(ctx.allocator(), sku)},
        // Copied by ctx.fail, so it may live on the stack.
        .cause = &why,
    });
}

fn fulfill(ctx: *workflow.Context, sku: []const u8) ![]const u8 {
    const future = ctx.executeActivity(Reserve, .{ .start_to_close_timeout = .fromSeconds(5) }, .{sku});
    future.get() catch |err| {
        const f = future.failure() orelse return err;
        // f is the activity failure; its cause is the one the activity raised.
        const reason = switch (temporal.failure.kind(f.cause orelse f)) {
            .timeout => |t| try std.fmt.allocPrint(ctx.allocator(), "timed out ({t})", .{t.timeout_type}),
            .application => |app| try std.fmt.allocPrint(ctx.allocator(), "{s} for {s}: {s}", .{
                app.type,
                try app.details.get([]const u8, ctx.allocator(), 0),
                temporal.failure.rootCause(f).message,
            }),
            else => return err,
        };
        // The workflow's failure, with the activity's chain under it.
        return ctx.fail(.{ .message = reason, .type = "FulfillFailed", .cause = f });
    };
    return "reserved";
}
// end-example

const Reserve = activity.Def("reserve", reserve);
const Fulfill = workflow.Def("fulfill", fulfill);

test "converter: causes, kinds and stack traces of failures" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(Fulfill);
    try env.registerActivity(Reserve);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var failure: ?temporal.failure.Failure = null;
    try testing.expectError(error.WorkflowFailed, env.executeWorkflow(Fulfill, .{"sku-1"}, arena.allocator(), &failure));
    try testing.expectEqualStrings("ReserveFailed for sku-1: warehouse offline", failure.?.message);
    try testing.expectEqualStrings("warehouse offline", temporal.failure.rootCause(&failure.?).message);
    // Built from ctx.fail and the error the workflow returned, so it has a stack trace.
    try testing.expect(failure.?.stack_trace.len > 0);
}

test "converter: encoded failure attributes" {
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const io = testing.io;

    // example: converter-encoded-attributes
    const zlib: converter.ZlibCodec = .{};
    const codecs = [_]converter.PayloadCodec{zlib.codec()};
    const env = try temporal.testing.WorkflowEnvironment.startWithOptions(testing.allocator, io, .{
        .codecs = &codecs,
        // client.Options.encode_failure_attributes, for the client and its workers.
        .encode_failure_attributes = true,
    });
    defer env.deinit();

    // What the client and its workers do to each failure they send, before the codecs run,
    // and to each failure they receive, after them.
    var f = try temporal.failure.application(arena, .{ .message = "card 4242 declined" });
    try temporal.failure.encodeAttributes(temporal.Failure, &f, arena);
    try testing.expectEqualStrings("Encoded failure", f.message);
    try temporal.failure.decodeAttributes(temporal.Failure, &f, arena);
    try testing.expectEqualStrings("card 4242 declined", f.message);
    // end-example
}
