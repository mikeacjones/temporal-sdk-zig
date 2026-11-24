//! Context propagation against a dev server: values set on a client call reach the workflow,
//! its activities, local activities, children, the run it continues as, and the Nexus
//! operations it calls; header fields go through the payload codecs only when their
//! propagator asks.

const std = @import("std");
const activity = @import("activity.zig");
const client_mod = @import("client.zig");
const converter = @import("converter.zig");
const interceptor = @import("interceptor.zig");
const nexus = @import("nexus.zig");
const workflow = @import("workflow.zig");
const propagation = @import("propagation.zig");
const proto = @import("proto.zig");
const schedule = @import("schedule.zig");
const WorkflowEnvironment = @import("testing.zig").WorkflowEnvironment;
const Allocator = std.mem.Allocator;

const testing = std.testing;

const tenant = propagation.key([]const u8, "tenant");
const propagators = [_]propagation.ContextPropagator{propagation.HeaderPropagator(tenant).propagator};

const Fixtures = struct {
    const opts: workflow.ActivityOptions = .{ .start_to_close_timeout = .fromSeconds(10) };

    fn whoAmI(ctx: *activity.Context) ![]const u8 {
        const header = if (ctx.headers().get("tenant") != null) "header" else "no header";
        return std.fmt.allocPrint(ctx.allocator(), "{s} ({s})", .{ ctx.propagated().get(tenant) orelse "none", header });
    }
    const WhoAmI = activity.Def("WhoAmI", whoAmI);

    fn child(ctx: *workflow.Context) ![]const u8 {
        return ctx.propagated().get(tenant) orelse "none";
    }
    const Child = workflow.Def("PropagationChild", child);

    /// Continues as new `hops` times, then reports the tenant each callee saw.
    fn relay(ctx: *workflow.Context, hops: u32) ![]const u8 {
        if (hops > 0) return ctx.continueAsNew(Relay, .{}, .{hops - 1});
        const own = ctx.propagated().get(tenant) orelse "none";
        const remote = try ctx.executeActivity(WhoAmI, opts, .{}).get();
        const local = try ctx.executeLocalActivity(WhoAmI, .{ .start_to_close_timeout = .fromSeconds(10) }, .{}).get();
        const from_child = try ctx.executeChildWorkflow(Child, .{}, .{}).get();
        return std.fmt.allocPrint(ctx.allocator(), "workflow={s} header={s} activity={s} local={s} child={s}", .{
            own, if (ctx.headers().get("tenant") != null) "yes" else "no", remote, local, from_child,
        });
    }
    const Relay = workflow.Def("PropagationRelay", relay);
};

test "a propagated value travels from a client start through the workflow, its activities, child and continue-as-new" {
    const env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{ .propagators = &propagators });
    defer env.deinit();
    try env.registerWorkflow(Fixtures.Relay);
    try env.registerWorkflow(Fixtures.Child);
    try env.registerActivity(Fixtures.WhoAmI);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();

    var values: propagation.Values = .init(arena.allocator());
    try values.put(tenant, "acme");
    const result = try env.executeWorkflowWithOptions(Fixtures.Relay, .{ .task_queue = "", .context = &values }, .{1}, arena.allocator(), null);
    try testing.expectEqualStrings("workflow=acme header=yes activity=acme (header) local=acme (header) child=acme", result);

    // Without values, nothing is propagated.
    const bare = try env.executeWorkflow(Fixtures.Relay, .{0}, arena.allocator(), null);
    try testing.expectEqualStrings("workflow=none header=no activity=none (no header) local=none (no header) child=none", bare);
}

const NexusFixtures = struct {
    const Who = nexus.OperationDef("who", void, []const u8);
    const WhoRun = nexus.OperationDef("who-run", void, []const u8);
    const Service = nexus.ServiceDef("zig-propagation-service", .{ Who, WhoRun });

    /// The tenant the handler's context reports, and the raw propagation header it received.
    fn who(ctx: *nexus.Context) ![]const u8 {
        const raw = ctx.info().header(propagation.nexus_header_name) orelse "none";
        return std.fmt.allocPrint(ctx.allocator(), "{s} (header {s})", .{
            ctx.propagated().get(tenant) orelse "none",
            if (std.mem.eql(u8, raw, "stale")) "stale" else if (std.mem.eql(u8, raw, "none")) "none" else "propagated",
        });
    }

    fn started(ctx: *workflow.Context) ![]const u8 {
        return ctx.propagated().get(tenant) orelse "none";
    }
    const Started = workflow.Def("PropagationNexusStarted", started);

    fn whoRun(ctx: *nexus.Context) !nexus.WorkflowStart(Started) {
        return .{ .options = .{ .id = try std.fmt.allocPrint(ctx.allocator(), "propagation-{s}", .{ctx.info().request_id}) }, .args = .{} };
    }

    const Handler = nexus.ServiceHandler(Service, .{
        nexus.OperationHandler(Who, who),
        nexus.OperationHandler(WhoRun, whoRun),
    });

    /// Calls both operations, the synchronous one with a caller header of the reserved name.
    fn call(ctx: *workflow.Context, endpoint: []const u8) ![]const u8 {
        const options: workflow.NexusOperationOptions = .{
            .endpoint = endpoint,
            .schedule_to_close_timeout = .fromSeconds(60),
            .headers = &.{.{ .name = "Temporal-Zig-Context", .value = "stale" }},
        };
        const sync = try ctx.executeNexusOperation(Service, Who, options, {}).get();
        const run = try ctx.executeNexusOperation(Service, WhoRun, .{ .endpoint = endpoint, .schedule_to_close_timeout = .fromSeconds(60) }, {}).get();
        return std.fmt.allocPrint(ctx.allocator(), "sync={s} workflow={s}", .{ sync, run });
    }
    const Call = workflow.Def("PropagationNexusCaller", call);
};

test "a propagated value reaches a synchronous Nexus handler and the workflow a workflow-run operation starts" {
    const env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{ .propagators = &propagators });
    defer env.deinit();
    try env.registerWorkflow(NexusFixtures.Call);
    try env.registerWorkflow(NexusFixtures.Started);
    try env.registerNexusService(NexusFixtures.Handler);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var suffix: [4]u8 = undefined;
    testing.io.random(&suffix);
    const endpoint = try std.fmt.allocPrint(a, "zig-propagation-{x}", .{suffix});
    try env.createNexusEndpoint(endpoint, .{});

    // The propagated header replaces the caller's header of the same name.
    var values: propagation.Values = .init(a);
    try values.put(tenant, "acme");
    const result = try env.executeWorkflowWithOptions(NexusFixtures.Call, .{ .task_queue = "", .context = &values }, .{endpoint}, a, null);
    try testing.expectEqualStrings("sync=acme (header propagated) workflow=acme", result);

    // Nothing to propagate: the caller's header arrives as it was sent, and doesn't extract.
    const level = testing.log_level;
    defer testing.log_level = level;
    testing.log_level = .err;
    const bare = try env.executeWorkflow(NexusFixtures.Call, .{endpoint}, a, null);
    try testing.expectEqualStrings("sync=none (header stale) workflow=none", bare);
}

/// Wraps every payload it encodes in one of encoding `encoding` (the payload serialized and
/// reversed) and unwraps only those on decode, passing others through. Counts the encodes
/// of the values "s3cr3t" and "acme".
const WrappingCodec = struct {
    const encoding = "test/wrapped";
    var secret_encodes: std.atomic.Value(u32) = .init(0);
    var tenant_encodes: std.atomic.Value(u32) = .init(0);

    fn codec() converter.PayloadCodec {
        return .{ .ptr = undefined, .vtable = &.{ .encode = encode, .decode = decode } };
    }

    fn encode(_: *anyopaque, arena: Allocator, _: std.Io, payloads: []const converter.Payload) converter.CodecError![]converter.Payload {
        const out = try arena.alloc(converter.Payload, payloads.len);
        for (payloads, out) |p, *o| {
            if (std.mem.eql(u8, p.data, "\"s3cr3t\"")) _ = secret_encodes.fetchAdd(1, .monotonic);
            if (std.mem.eql(u8, p.data, "\"acme\"")) _ = tenant_encodes.fetchAdd(1, .monotonic);
            var serialized: std.Io.Writer.Allocating = .init(arena);
            p.encode(&serialized.writer, arena) catch return error.OutOfMemory;
            std.mem.reverse(u8, serialized.written());
            o.* = try converter.payloadWithEncoding(arena, encoding, serialized.written());
        }
        return out;
    }

    fn decode(_: *anyopaque, arena: Allocator, _: std.Io, payloads: []const converter.Payload) converter.CodecError![]converter.Payload {
        const out = try arena.dupe(converter.Payload, payloads);
        for (out) |*p| {
            if (!std.mem.eql(u8, converter.encodingOf(p.*) orelse "", encoding)) continue;
            const data = try arena.dupe(u8, p.data);
            std.mem.reverse(u8, data);
            var reader: std.Io.Reader = .fixed(data);
            p.* = converter.Payload.decode(&reader, arena) catch |err| switch (err) {
                error.OutOfMemory => return error.OutOfMemory,
                else => return error.CodecFailed,
            };
        }
        return out;
    }
};

/// Adds the header field `added` to every workflow start, after the propagators wrote theirs.
const AddingInterceptor = struct {
    pub fn call(_: *AddingInterceptor, input: *interceptor.ClientCall, next: interceptor.Client.Next) interceptor.ClientError!void {
        if (input.request(proto.temporal.api.workflowservice.v1.StartWorkflowExecutionRequest) != null) try input.header.?.set("added", @as([]const u8, "by interceptor"));
        return next.call(input);
    }
};

const CodecFixtures = struct {
    const secret = propagation.key([]const u8, "secret");
    /// "secret" through the codecs, "tenant" as written.
    const propagators = [_]propagation.ContextPropagator{
        propagation.HeaderPropagator(secret).encoded_propagator,
        propagation.HeaderPropagator(tenant).propagator,
    };

    /// The propagated values and the encodings of the header fields, as decoded for the caller.
    fn describe(a: Allocator, values: *const propagation.Values, header: converter.Header) ![]const u8 {
        const encodingOf = struct {
            fn f(h: converter.Header, name: []const u8) []const u8 {
                const p = h.get(name) orelse return "none";
                return converter.encodingOf(p) orelse "unset";
            }
        }.f;
        return std.fmt.allocPrint(a, "secret={s} ({s}) tenant={s} ({s})", .{
            values.get(secret) orelse "none", encodingOf(header, "secret"),
            values.get(tenant) orelse "none", encodingOf(header, "tenant"),
        });
    }

    fn report(ctx: *activity.Context) ![]const u8 {
        return describe(ctx.allocator(), ctx.propagated(), ctx.headers());
    }
    const Report = activity.Def("HeaderCodecReport", report);

    fn flow(ctx: *workflow.Context) ![]const u8 {
        const own = try describe(ctx.allocator(), ctx.propagated(), ctx.headers());
        const from_activity = try ctx.executeActivity(Report, .{ .start_to_close_timeout = .fromSeconds(10) }, .{}).get();
        return std.fmt.allocPrint(ctx.allocator(), "workflow: {s}; activity: {s}", .{ own, from_activity });
    }
    const Flow = workflow.Def("HeaderCodecFlow", flow);

    /// The encoding of each named field of `header` as stored, "none" for a missing field.
    fn storedEncodings(a: Allocator, header: ?proto.temporal.api.common.v1.Header) ![]const u8 {
        const h = try converter.Header.fromEntries(a, if (header) |x| x.fields.items else &.{});
        var out: std.Io.Writer.Allocating = .init(a);
        for (h.fields) |f| try out.writer.print("{s}={s} ", .{ f.name, converter.encodingOf(f.value) orelse "unset" });
        return out.written();
    }
};

test "a field of a propagator that sets encode_fields is encoded in history and decoded for the workflow and its activity; the others are sent as written" {
    const codecs = [_]converter.PayloadCodec{WrappingCodec.codec()};
    var adding: AddingInterceptor = .{};
    const interceptors = [_]interceptor.Client{interceptor.Client.from(AddingInterceptor, &adding)};
    const env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{
        .codecs = &codecs,
        .propagators = &CodecFixtures.propagators,
        .interceptors = &interceptors,
    });
    defer env.deinit();
    try env.registerWorkflow(CodecFixtures.Flow);
    try env.registerActivity(CodecFixtures.Report);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    WrappingCodec.secret_encodes.store(0, .monotonic);
    WrappingCodec.tenant_encodes.store(0, .monotonic);
    var values: propagation.Values = .init(a);
    try values.put(CodecFixtures.secret, "s3cr3t");
    try values.put(tenant, "acme");
    var run = try env.startWorkflow(CodecFixtures.Flow, .{ .id = "header-codec-flow", .task_queue = "", .context = &values }, .{});
    defer run.deinit();
    // Both fields reach the workflow and its activity decoded, so `headers()` shows them as
    // the propagators wrote them.
    try testing.expectEqualStrings(
        "workflow: secret=s3cr3t (json/plain) tenant=acme (json/plain); activity: secret=s3cr3t (json/plain) tenant=acme (json/plain)",
        try env.getResult(CodecFixtures.Flow, &run, a, null),
    );

    // A client without the codecs reads the history as stored: the opted-in field is
    // encoded in the start and in the activity's schedule; the propagator's other field and
    // the interceptor's are as written.
    var plain = try client_mod.Client.connect(testing.allocator, testing.io, env.server.dev.clientOptions());
    defer plain.close();
    const history = try plain.fetchHistory(a, run.id, run.run_id);
    var scheduled: ?[]const u8 = null;
    for (history.events.items) |event| {
        const attributes = event.attributes orelse continue;
        switch (attributes) {
            .workflow_execution_started_event_attributes => |started| {
                try testing.expectEqualStrings("added=json/plain secret=test/wrapped tenant=json/plain ", try CodecFixtures.storedEncodings(a, started.header));
                const h = try converter.Header.fromEntries(a, started.header.?.fields.items);
                try testing.expectEqualStrings("\"acme\"", h.get("tenant").?.data);
                try testing.expectEqualStrings("\"by interceptor\"", h.get("added").?.data);
            },
            .activity_task_scheduled_event_attributes => |s| scheduled = try CodecFixtures.storedEncodings(a, s.header),
            // The rest of the payloads still go through the codecs.
            .workflow_execution_completed_event_attributes => |c| {
                try testing.expectEqualStrings(WrappingCodec.encoding, converter.encodingOf(c.result.?.payloads.items[0]).?);
            },
            else => {},
        }
    }
    try testing.expectEqualStrings("secret=test/wrapped tenant=json/plain ", scheduled.?);
    try testing.expect(WrappingCodec.secret_encodes.load(.monotonic) >= 2);
    try testing.expectEqual(@as(u32, 0), WrappingCodec.tenant_encodes.load(.monotonic));
}

test "a schedule update sends its start action's header fields back as stored, the encoded ones still encoded" {
    const codecs = [_]converter.PayloadCodec{WrappingCodec.codec()};
    const env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{ .codecs = &codecs });
    defer env.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // The schedule as a client whose propagator opted in creates it: its field encoded by the
    // codecs, another propagator's field plain.
    var plain = try client_mod.Client.connect(testing.allocator, testing.io, env.server.dev.clientOptions());
    defer plain.close();
    const secret = (try WrappingCodec.encode(undefined, a, testing.io, &.{try converter.toPayload(a, @as([]const u8, "s3cr3t"))}))[0];
    var header: proto.temporal.api.common.v1.Header = .{};
    try header.fields.append(a, .{ .key = "secret", .value = secret });
    try header.fields.append(a, .{ .key = "tenant", .value = try converter.toPayload(a, @as([]const u8, "acme")) });
    _ = try plain.callService(.workflow, "CreateSchedule", a, .{
        .namespace = env.client.namespace,
        .schedule_id = "header-schedule",
        .request_id = "header-schedule-create",
        .schedule = .{
            .spec = .{},
            .action = .{ .start_workflow = .{
                .workflow_id = "scheduled",
                .workflow_type = .{ .name = "Scheduled" },
                .task_queue = .{ .name = "header-schedule-queue" },
                .header = header,
            } },
            .state = .{ .paused = true },
        },
    }, null);

    // A read-modify-write through the client with the codecs, changing nothing.
    var handle = try env.client.getScheduleHandle("header-schedule");
    defer handle.deinit();
    try handle.update({}, struct {
        fn f(_: void, _: Allocator, d: *schedule.Description) !?schedule.Update {
            return .{ .schedule = d.schedule };
        }
    }.f);

    // The stored header is as it was: the encoded field was not left decoded on the server.
    const described = try plain.callService(.workflow, "DescribeSchedule", a, .{ .namespace = env.client.namespace, .schedule_id = "header-schedule" }, null);
    const stored = described.schedule.?.action.?.start_workflow.?.header;
    try testing.expectEqualStrings("secret=test/wrapped tenant=json/plain ", try CodecFixtures.storedEncodings(a, stored));
}

test "a schedule update whose callback changes a start action's header field in place sends the new value" {
    const codecs = [_]converter.PayloadCodec{WrappingCodec.codec()};
    const env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{ .codecs = &codecs });
    defer env.deinit();
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var plain = try client_mod.Client.connect(testing.allocator, testing.io, env.server.dev.clientOptions());
    defer plain.close();
    var header: proto.temporal.api.common.v1.Header = .{};
    try header.fields.append(a, .{ .key = "tenant", .value = try converter.toPayload(a, @as([]const u8, "acme")) });
    _ = try plain.callService(.workflow, "CreateSchedule", a, .{
        .namespace = env.client.namespace,
        .schedule_id = "header-schedule-edit",
        .request_id = "header-schedule-edit-create",
        .schedule = .{
            .spec = .{},
            .action = .{ .start_workflow = .{
                .workflow_id = "scheduled-edit",
                .workflow_type = .{ .name = "Scheduled" },
                .task_queue = .{ .name = "header-schedule-edit-queue" },
                .header = header,
            } },
            .state = .{ .paused = true },
        },
    }, null);

    // The callback moves the schedule to another tenant by setting the header field's value
    // where it is, as code that changes one entry of a list does.
    var handle = try env.client.getScheduleHandle("header-schedule-edit");
    defer handle.deinit();
    try handle.update({}, struct {
        fn f(_: void, arena_: Allocator, d: *schedule.Description) !?schedule.Update {
            const fields = d.raw.schedule.?.action.?.start_workflow.?.header.?.fields.items;
            for (fields) |*field| {
                if (std.mem.eql(u8, field.key, "tenant")) field.value = try converter.toPayload(arena_, @as([]const u8, "globex"));
            }
            return .{ .schedule = d.schedule };
        }
    }.f);

    // The server holds the value the callback wrote.
    const described = try plain.callService(.workflow, "DescribeSchedule", a, .{ .namespace = env.client.namespace, .schedule_id = "header-schedule-edit" }, null);
    const stored = try converter.Header.fromEntries(a, described.schedule.?.action.?.start_workflow.?.header.?.fields.items);
    try testing.expectEqualStrings("\"globex\"", stored.get("tenant").?.data);
}
