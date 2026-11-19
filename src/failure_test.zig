//! Failures against a dev server: causes set with `ctx.fail` reach the server and come back
//! whole, failures built from returned errors carry their stack traces, encoded attributes
//! hide messages behind the codecs, and `failure.kind` reads what arrives.

const std = @import("std");
const activity = @import("activity.zig");
const workflow = @import("workflow.zig");
const converter = @import("converter.zig");
const client_mod = @import("client.zig");
const failure_mod = @import("failure.zig");
const testing_mod = @import("testing.zig");
const WorkflowEnvironment = testing_mod.WorkflowEnvironment;
const WorkflowReplayer = testing_mod.WorkflowReplayer;
const Failure = failure_mod.Failure;
const Allocator = std.mem.Allocator;
const Io = std.Io;

const testing = std.testing;

/// This file's name, which stack traces of failures raised here must contain.
const this_file = "failure_test.zig";

const opts: workflow.ActivityOptions = .{
    .start_to_close_timeout = .fromSeconds(10),
    .retry_policy = .{ .maximum_attempts = 1 },
};

/// Writes each failure of the chain as "message (type)" or "message [kind]", joined by " <- ".
fn chain(a: Allocator, f: *const Failure) ![]const u8 {
    var out: Io.Writer.Allocating = .init(a);
    var it: ?*const Failure = f;
    while (it) |cur| : (it = cur.cause) {
        if (cur != f) try out.writer.writeAll(" <- ");
        if (failure_mod.applicationType(cur.*)) |t|
            try out.writer.print("{s} ({s})", .{ cur.message, t })
        else
            try out.writer.print("{s} [{t}]", .{ cur.message, failure_mod.kind(cur) });
    }
    return out.written();
}

const Causes = struct {
    /// Fails with a cause chain built on its own stack: `ctx.fail` copies it.
    fn charge(ctx: *activity.Context) !void {
        var bank: Failure = try failure_mod.application(ctx.allocator(), .{ .message = "bank offline", .type = "BankOffline" });
        const declined = try failure_mod.application(ctx.allocator(), .{ .message = "card declined", .type = "Declined", .cause = &bank });
        return ctx.fail(.{ .message = "charge failed", .type = "ChargeFailed", .non_retryable = true, .cause = &declined });
    }
    const Charge = activity.Def("CausesCharge", charge);

    /// Reads the activity's whole chain, then fails with it as the cause of its own failure.
    fn order(ctx: *workflow.Context) ![]const u8 {
        const charged = ctx.executeActivity(Charge, opts, .{});
        charged.get() catch |err| switch (err) {
            error.ActivityFailed => {
                const seen = try chain(ctx.allocator(), charged.failure().?);
                return ctx.fail(.{ .message = seen, .type = "OrderFailed", .cause = charged.failure().? });
            },
            else => return err,
        };
        return "charged";
    }
    const Order = workflow.Def("CausesOrder", order);

    const Reject = workflow.UpdateDef("reject", fn () void);
    const Done = workflow.SignalDef("done", fn () void);

    const State = struct {
        ctx: *workflow.Context,
        done: bool = false,

        fn reject(s: *State) !void {
            const why: Failure = try failure_mod.application(s.ctx.allocator(), .{ .message = "out of stock", .type = "OutOfStock" });
            return s.ctx.fail(.{ .message = "rejected", .type = "Rejected", .cause = &why });
        }
        fn finish(s: *State) void {
            s.done = true;
        }
        fn finished(s: *State) bool {
            return s.done;
        }
    };

    fn shop(ctx: *workflow.Context) !void {
        const s = try ctx.allocator().create(State);
        s.* = .{ .ctx = ctx };
        try ctx.setUpdateHandler(Reject, s, State.reject);
        try ctx.setSignalHandler(Done, s, State.finish);
        try ctx.await(s, State.finished);
    }
    const Shop = workflow.Def("CausesShop", shop);

    var token_buf: [512]u8 = undefined;
    var token_len: std.atomic.Value(usize) = .init(0);

    fn later(ctx: *activity.Context) !void {
        const token = ctx.info().task_token;
        @memcpy(token_buf[0..token.len], token);
        token_len.store(token.len, .release);
        return ctx.completeAsync();
    }
    const Later = activity.Def("CausesLater", later);

    fn waitLater(ctx: *workflow.Context) ![]const u8 {
        const f = ctx.executeActivity(Later, opts, .{});
        f.get() catch |err| switch (err) {
            error.ActivityFailed => return chain(ctx.allocator(), f.failure().?),
            else => return err,
        };
        return "completed";
    }
    const WaitLater = workflow.Def("CausesWaitLater", waitLater);
};

test "an activity's ctx.fail cause chain reaches the workflow, and the workflow's and an update's ctx.fail send causes" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(Causes.Order);
    try env.registerWorkflow(Causes.Shop);
    try env.registerActivity(Causes.Charge);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var failure: ?Failure = null;
    try testing.expectError(error.WorkflowFailed, env.executeWorkflow(Causes.Order, .{}, a, &failure));
    const activity_chain = "Activity task failed [activity] <- charge failed (ChargeFailed) <- card declined (Declined) <- bank offline (BankOffline)";
    try testing.expectEqualStrings(activity_chain, failure.?.message);
    try testing.expectEqualStrings(activity_chain, (try chain(a, &failure.?))[activity_chain.len + " (OrderFailed) <- ".len ..]);

    var run = try env.startWorkflow(Causes.Shop, .{ .task_queue = "" }, .{});
    defer run.deinit();
    var rejected = try env.client.updateWorkflow(Causes.Reject, run.id, run.run_id, .{}, .{});
    defer rejected.deinit();
    try testing.expectError(error.UpdateFailed, rejected.get(a));
    try testing.expectEqualStrings("rejected (Rejected) <- out of stock (OutOfStock)", try chain(a, rejected.failure().?));
    try env.client.signalWorkflow(Causes.Done, run.id, run.run_id, .{});
    try env.getResult(Causes.Shop, &run, a, null);
}

test "Client.failActivity sends the failure's cause, by task token and by ID" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(Causes.WaitLater);
    try env.registerActivity(Causes.Later);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    for ([_]bool{ false, true }) |by_id| {
        Causes.token_len.store(0, .release);
        var run = try env.startWorkflow(Causes.WaitLater, .{ .task_queue = "" }, .{});
        defer run.deinit();
        while (Causes.token_len.load(.acquire) == 0) try testing.io.sleep(.fromMilliseconds(10), .awake);
        const why = try failure_mod.application(a, .{ .message = "receipt lost", .type = "Lost" });
        const f: failure_mod.Application = .{ .message = "gave up", .type = "GaveUp", .non_retryable = true, .cause = &why };
        if (by_id)
            try env.client.failActivityById(.{ .workflow_id = run.id, .run_id = run.run_id, .activity_id = "1" }, f)
        else
            try env.client.failActivity(Causes.token_buf[0..Causes.token_len.load(.acquire)], f);
        try testing.expectEqualStrings("Activity task failed [activity] <- gave up (GaveUp) <- receipt lost (Lost)", try env.getResult(Causes.WaitLater, &run, a, null));
    }
}

const Traces = struct {
    noinline fn leaf() error{OutOfStock}!void {
        return error.OutOfStock;
    }

    noinline fn helper() error{OutOfStock}!void {
        try leaf();
    }

    fn pick(_: *activity.Context) !void {
        try helper();
    }
    const Pick = activity.Def("TracesPick", pick);

    /// Fails from `helper`, two calls deep.
    fn pack(_: *workflow.Context) !void {
        try helper();
    }
    const Pack = workflow.Def("TracesPack", pack);

    /// Returns the stack trace of the activity's failure.
    fn ship(ctx: *workflow.Context) ![]const u8 {
        const picked = ctx.executeActivity(Pick, opts, .{});
        picked.get() catch |err| switch (err) {
            error.ActivityFailed => return failure_mod.rootCause(picked.failure().?).stack_trace,
            else => return err,
        };
        return "picked";
    }
    const Ship = workflow.Def("TracesShip", ship);

    const Check = workflow.UpdateDef("check", fn () void);
    const Done = workflow.SignalDef("done", fn () void);

    const State = struct {
        done: bool = false,

        fn check(_: *State) !void {
            try helper();
        }
        fn finish(s: *State) void {
            s.done = true;
        }
        fn finished(s: *State) bool {
            return s.done;
        }
    };

    fn store(ctx: *workflow.Context) !void {
        const s = try ctx.allocator().create(State);
        s.* = .{};
        try ctx.setUpdateHandler(Check, s, State.check);
        try ctx.setSignalHandler(Done, s, State.finish);
        try ctx.await(s, State.finished);
    }
    const Store = workflow.Def("TracesStore", store);
};

fn expectTrace(trace: []const u8) !void {
    if (std.mem.indexOf(u8, trace, this_file) == null or std.mem.indexOf(u8, trace, "helper") == null) {
        std.debug.print("stack trace without {s}'s helper:\n{s}\n", .{ this_file, trace });
        return error.TestUnexpectedResult;
    }
}

test "failures built from errors a workflow, an update handler and an activity returned carry their stack traces, unless turned off" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(Traces.Pack);
    try env.registerWorkflow(Traces.Ship);
    try env.registerWorkflow(Traces.Store);
    try env.registerActivity(Traces.Pick);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var failure: ?Failure = null;
    try testing.expectError(error.WorkflowFailed, env.executeWorkflow(Traces.Pack, .{}, a, &failure));
    try testing.expectEqualStrings("OutOfStock", failure.?.message);
    try expectTrace(failure.?.stack_trace);
    try expectTrace(try env.executeWorkflow(Traces.Ship, .{}, a, null));

    var run = try env.startWorkflow(Traces.Store, .{ .task_queue = "" }, .{});
    defer run.deinit();
    var checked = try env.client.updateWorkflow(Traces.Check, run.id, run.run_id, .{}, .{});
    defer checked.deinit();
    try testing.expectError(error.UpdateFailed, checked.get(a));
    try expectTrace(checked.failure().?.stack_trace);
    try env.client.signalWorkflow(Traces.Done, run.id, run.run_id, .{});
    try env.getResult(Traces.Store, &run, a, null);

    // The replayer builds the same failure, traces on, and the history replays.
    var pack = try env.startWorkflow(Traces.Pack, .{ .task_queue = "" }, .{});
    defer pack.deinit();
    try testing.expectError(error.WorkflowFailed, env.getResult(Traces.Pack, &pack, a, null));
    var replayer: WorkflowReplayer = .init(testing.allocator, testing.io);
    defer replayer.deinit();
    try replayer.registerWorkflow(Traces.Pack);
    try replayer.replayHistory(try env.client.fetchHistory(a, pack.id, pack.run_id));
}

test "worker.Options.failure_stack_traces off leaves the stack traces of built failures empty" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    env.worker.options.failure_stack_traces = false;
    try env.registerWorkflow(Traces.Pack);
    try env.registerWorkflow(Traces.Ship);
    try env.registerActivity(Traces.Pick);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var failure: ?Failure = null;
    try testing.expectError(error.WorkflowFailed, env.executeWorkflow(Traces.Pack, .{}, a, &failure));
    try testing.expectEqualStrings("OutOfStock", failure.?.message);
    try testing.expectEqualStrings("", failure.?.stack_trace);
    try testing.expectEqualStrings("", try env.executeWorkflow(Traces.Ship, .{}, a, null));
}

/// Encodes every payload as "test/xor" with its serialized bytes XORed, so nothing of a
/// payload is readable without it.
const XorCodec = struct {
    const encoding = "test/xor";

    fn codec(x: *XorCodec) converter.PayloadCodec {
        return .{ .ptr = x, .vtable = &.{ .encode = encode, .decode = decode } };
    }

    fn encode(_: *anyopaque, arena: Allocator, _: Io, payloads: []const converter.Payload) converter.CodecError![]converter.Payload {
        const out = try arena.alloc(converter.Payload, payloads.len);
        for (payloads, out) |p, *o| {
            var serialized: Io.Writer.Allocating = .init(arena);
            p.encode(&serialized.writer, arena) catch return error.OutOfMemory;
            for (serialized.written()) |*b| b.* ^= 0x5a;
            o.* = try converter.payloadWithEncoding(arena, encoding, serialized.written());
        }
        return out;
    }

    fn decode(_: *anyopaque, arena: Allocator, _: Io, payloads: []const converter.Payload) converter.CodecError![]converter.Payload {
        const out = try arena.dupe(converter.Payload, payloads);
        for (out) |*p| {
            if (!std.mem.eql(u8, converter.encodingOf(p.*) orelse "", encoding)) continue;
            const data = try arena.dupe(u8, p.data);
            for (data) |*b| b.* ^= 0x5a;
            var reader: Io.Reader = .fixed(data);
            p.* = converter.Payload.decode(&reader, arena) catch return error.CodecFailed;
        }
        return out;
    }
};

const Encoded = struct {
    fn charge(ctx: *activity.Context) !void {
        return ctx.fail(.{ .message = "card 4242 declined", .type = "Declined", .non_retryable = true });
    }
    const Charge = activity.Def("EncodedCharge", charge);

    /// Fails with the activity's failure, passed on unchanged.
    fn order(ctx: *workflow.Context) !void {
        try ctx.executeActivity(Charge, opts, .{}).get();
    }
    const Order = workflow.Def("EncodedOrder", order);
};

test "encode_failure_attributes hides failure messages behind the codecs, and a client with them reads them back" {
    var xor: XorCodec = .{};
    const codecs = [_]converter.PayloadCodec{xor.codec()};
    const env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{ .codecs = &codecs, .encode_failure_attributes = true });
    defer env.deinit();
    try env.registerWorkflow(Encoded.Order);
    try env.registerActivity(Encoded.Charge);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var run = try env.startWorkflow(Encoded.Order, .{ .task_queue = "" }, .{});
    defer run.deinit();
    try testing.expectError(error.WorkflowFailed, env.getResult(Encoded.Order, &run, a, null));
    try testing.expectEqualStrings("Activity task failed [activity] <- card 4242 declined (Declined)", try chain(a, run.failure().?));
    try expectActivityTrace(failure_mod.rootCause(run.failure().?).stack_trace);

    // A client without the codec sees only what the server stored.
    var plain = try client_mod.Client.connect(testing.allocator, testing.io, env.server.dev.clientOptions());
    defer plain.close();
    var seen = try plain.getWorkflow(void, run.id, run.run_id);
    defer seen.deinit();
    try testing.expectError(error.WorkflowFailed, seen.get(a));
    var it: ?*const Failure = seen.failure().?;
    while (it) |f| : (it = f.cause) {
        try testing.expectEqualStrings(failure_mod.encoded_message, f.message);
        try testing.expectEqualStrings("", f.stack_trace);
        try testing.expectEqualStrings(XorCodec.encoding, converter.encodingOf(f.encoded_attributes.?).?);
    }
    try testing.expectEqualStrings("Declined", failure_mod.applicationType(failure_mod.rootCause(seen.failure().?).*).?);
}

fn expectActivityTrace(trace: []const u8) !void {
    // The activity's failure came from ctx.fail in `Encoded.charge`.
    if (std.mem.indexOf(u8, trace, this_file) == null) {
        std.debug.print("stack trace without {s}:\n{s}\n", .{ this_file, trace });
        return error.TestUnexpectedResult;
    }
}

const Kinds = struct {
    fn slow(ctx: *activity.Context) !void {
        try ctx.io().sleep(.fromSeconds(3), .awake);
    }
    const Slow = activity.Def("KindsSlow", slow);

    fn refuse(ctx: *activity.Context) !void {
        const code = try converter.toPayload(ctx.allocator(), @as(u32, 402));
        const reason = try converter.toPayload(ctx.allocator(), @as([]const u8, "payment required"));
        return ctx.fail(.{ .message = "refused", .type = "Refused", .non_retryable = true, .details = &.{ code, reason } });
    }
    const Refuse = activity.Def("KindsRefuse", refuse);

    fn describe(ctx: *workflow.Context, f: *const Failure) ![]const u8 {
        const outer = failure_mod.kind(f).activity;
        return switch (failure_mod.kind(failure_mod.rootCause(f))) {
            .timeout => |t| std.fmt.allocPrint(ctx.allocator(), "{s} timed out: {t} ({t})", .{ outer.activity_type, t.timeout_type, outer.retry_state }),
            .application => |app| std.fmt.allocPrint(ctx.allocator(), "{s} failed: {s} {d} {s}", .{
                outer.activity_type,
                app.type,
                try app.details.get(u32, ctx.allocator(), 0),
                try app.details.get([]const u8, ctx.allocator(), 1),
            }),
            else => |k| std.fmt.allocPrint(ctx.allocator(), "unexpected {t}", .{k}),
        };
    }

    fn run(ctx: *workflow.Context) ![]const u8 {
        const slow_f = ctx.executeActivity(Slow, .{ .start_to_close_timeout = .fromSeconds(1), .retry_policy = .{ .maximum_attempts = 1 } }, .{});
        const refuse_f = ctx.executeActivity(Refuse, opts, .{});
        var out: std.ArrayList(u8) = .empty;
        slow_f.get() catch |err| switch (err) {
            error.ActivityFailed => try out.appendSlice(ctx.allocator(), try describe(ctx, slow_f.failure().?)),
            else => return err,
        };
        try out.appendSlice(ctx.allocator(), "; ");
        refuse_f.get() catch |err| switch (err) {
            error.ActivityFailed => try out.appendSlice(ctx.allocator(), try describe(ctx, refuse_f.failure().?)),
            else => return err,
        };
        return out.items;
    }
    const Run = workflow.Def("KindsRun", run);
};

test "a workflow switches on failure.kind for an activity timeout and decodes an application failure's details" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(Kinds.Run);
    try env.registerActivity(Kinds.Slow);
    try env.registerActivity(Kinds.Refuse);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    try testing.expectEqualStrings(
        "KindsSlow timed out: start_to_close (maximum_attempts_reached); KindsRefuse failed: Refused 402 payment required",
        try env.executeWorkflow(Kinds.Run, .{}, arena.allocator(), null),
    );
}

const worker_mod = @import("worker.zig");

const PassOn = struct {
    /// The task queue of the worker that has the codec and runs `Encoded.Charge`.
    var activity_queue: []const u8 = "";

    /// Fails with the activity's failure, passed on unchanged.
    fn order(ctx: *workflow.Context) !void {
        var o = opts;
        o.task_queue = activity_queue;
        try ctx.executeActivity(Encoded.Charge, o, .{}).get();
    }
    const Order = workflow.Def("PassOnOrder", order);
};

test "a worker without the codec that passes on an encoded failure keeps its encoded attributes" {
    var xor: XorCodec = .{};
    const codecs = [_]converter.PayloadCodec{xor.codec()};
    const env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{ .codecs = &codecs, .encode_failure_attributes = true });
    defer env.deinit();
    try env.registerActivity(Encoded.Charge);
    PassOn.activity_queue = env.task_queue;
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // A workflow worker that encodes failure attributes but has no codec: it can't decode
    // the activity failure's attributes, so it must send them on as it received them.
    var plain_options = env.server.dev.clientOptions();
    plain_options.encode_failure_attributes = true;
    var plain = try client_mod.Client.connect(testing.allocator, testing.io, plain_options);
    defer plain.close();
    const queue = try std.fmt.allocPrint(a, "{s}-no-codec", .{env.task_queue});
    var w = try worker_mod.Worker.init(testing.allocator, testing.io, &plain, queue, .{});
    defer w.deinit();
    try w.registerWorkflow(PassOn.Order);
    var running = try testing.io.concurrent(worker_mod.Worker.run, .{&w});
    defer {
        w.stop();
        running.await(testing.io) catch {};
    }
    try w.waitUntilRunning();

    var run = try env.startWorkflow(PassOn.Order, .{ .task_queue = queue }, .{});
    defer run.deinit();
    var seen = try env.client.getWorkflow(void, run.id, run.run_id);
    defer seen.deinit();
    try testing.expectError(error.WorkflowFailed, seen.get(a));
    // The client with the codec reads the activity's own message, not "Encoded failure".
    try testing.expectEqualStrings("Activity task failed [activity] <- card 4242 declined (Declined)", try chain(a, seen.failure().?));
}

const Queries = struct {
    const Secret = workflow.QueryDef("secret", fn () []const u8);
    const Done = workflow.SignalDef("done", fn () void);

    const State = struct {
        done: bool = false,

        fn secret(_: *State) error{SecretQueryFailure}![]const u8 {
            return error.SecretQueryFailure;
        }
        fn finish(s: *State) void {
            s.done = true;
        }
        fn finished(s: *State) bool {
            return s.done;
        }
    };

    fn wait(ctx: *workflow.Context) !void {
        const s = try ctx.allocator().create(State);
        s.* = .{};
        try ctx.setQueryHandler(Secret, s, State.secret);
        try ctx.setSignalHandler(Done, s, State.finish);
        try ctx.await(s, State.finished);
    }
    const Wait = workflow.Def("QueriesWait", wait);
};

test "a failed query's failure_message names the handler's error with encode_failure_attributes on" {
    var xor: XorCodec = .{};
    const codecs = [_]converter.PayloadCodec{xor.codec()};
    const env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{ .codecs = &codecs, .encode_failure_attributes = true });
    defer env.deinit();
    try env.registerWorkflow(Queries.Wait);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var run = try env.startWorkflow(Queries.Wait, .{ .task_queue = "" }, .{});
    defer run.deinit();
    var message: []const u8 = "";
    try testing.expectError(error.QueryFailed, env.client.queryWorkflowWithOptions(Queries.Secret, a, run.id, run.run_id, .{}, .{ .failure_message = &message }));
    try testing.expect(std.mem.indexOf(u8, message, "SecretQueryFailure") != null);
    try env.client.signalWorkflow(Queries.Done, run.id, run.run_id, .{});
    try env.getResult(Queries.Wait, &run, a, null);
}

/// Fails on a thread of the pool's own, after every thread of the pool has started one: an
/// error a task returns stays in its thread's error return trace.
const StaleTrace = struct {
    const threads = 64;

    noinline fn earlierLeaf() error{EarlierTaskFailed}!void {
        return error.EarlierTaskFailed;
    }

    noinline fn earlierTask(started: *std.atomic.Value(usize)) error{EarlierTaskFailed}!void {
        _ = started.fetchAdd(1, .acq_rel);
        while (started.load(.acquire) < threads) std.atomic.spinLoopHint();
        try earlierLeaf();
    }
};

test "an activity's stack trace holds only its own error's frames, not those of earlier tasks on its thread" {
    if (@errorReturnTrace() == null) return error.SkipZigTest;
    // 64 threads spin on a barrier, which took more than 10 minutes under Thread Sanitizer
    // (measured 2026-11-16, aarch64-macos).
    if (@import("builtin").sanitize_thread) return error.SkipZigTest;
    var threaded: Io.Threaded = .init(testing.allocator, .{
        .environ = testing.environ,
        .async_limit = .limited(StaleTrace.threads),
        .concurrent_limit = .limited(StaleTrace.threads),
    });
    defer threaded.deinit();
    const io = threaded.io();
    // Every thread of the pool runs a task that fails, as an activity's own concurrent work
    // that fails or is canceled does: the worker's activities then run on those threads.
    var started: std.atomic.Value(usize) = .init(0);
    var earlier: [StaleTrace.threads]Io.Future(error{EarlierTaskFailed}!void) = undefined;
    for (&earlier) |*f| f.* = try io.concurrent(StaleTrace.earlierTask, .{&started});
    for (&earlier) |*f| try testing.expectError(error.EarlierTaskFailed, f.await(io));

    const env = try WorkflowEnvironment.start(testing.allocator, io);
    defer env.deinit();
    try env.registerWorkflow(Traces.Ship);
    try env.registerActivity(Traces.Pick);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const trace = try env.executeWorkflow(Traces.Ship, .{}, arena.allocator(), null);
    try expectTrace(trace);
    if (std.mem.indexOf(u8, trace, "in earlierLeaf") != null or std.mem.indexOf(u8, trace, "in earlierTask") != null) {
        std.debug.print("the activity's stack trace holds frames of an earlier task on its thread:\n{s}\n", .{trace});
        return error.TestUnexpectedResult;
    }
}
