//! Tests of the on-demand pattern: the caller workflow starts two greeting workflows through
//! the service's handlers, attaches approval context to them, messages them, and waits for
//! their results. The handler runs on a worker of its own on `handler.task_queue`, where the
//! endpoint routes and where attachApprovalContext starts the greeting workflow, as in the
//! sample's worker program; the test environment's worker runs the caller.

const std = @import("std");
const temporal = @import("temporal");
const service = @import("ondemandpattern/service.zig");
const handler = @import("ondemandpattern/handler.zig");
const caller = @import("ondemandpattern/caller.zig");

const testing = std.testing;
const workflow = temporal.workflow;
const Worker = temporal.worker.Worker;
const WorkflowEnvironment = temporal.testing.WorkflowEnvironment;
const EventType = temporal.proto.temporal.api.enums.v1.EventType;
const HistoryEvent = temporal.proto.temporal.api.history.v1.HistoryEvent;

/// A dev server that returns links for signals, with the handler on a worker of its own and
/// the caller workflows on the environment's worker.
const Setup = struct {
    env: *WorkflowEnvironment,
    handler_worker: Worker,
    running: ?std.Io.Future(Worker.RunError!void) = null,

    /// Starts the environment and the handler worker. `s` must not move afterwards.
    fn start(s: *Setup) !void {
        s.running = null;
        s.env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{
            .dev_server = .{ .extra_args = &temporal.testing.DevServer.signal_link_args },
        });
        errdefer s.env.deinit();
        try caller.register(&s.env.worker);
        try s.env.registerWorkflow(AttachTwice);
        try s.env.createNexusEndpoint(caller.endpoint, .{ .task_queue = handler.task_queue });
        s.handler_worker = try Worker.init(testing.allocator, testing.io, &s.env.client, handler.task_queue, .{});
        errdefer s.handler_worker.deinit();
        try handler.register(&s.handler_worker);
        s.running = try testing.io.concurrent(Worker.run, .{&s.handler_worker});
    }

    fn deinit(s: *Setup) void {
        if (s.running) |*r| {
            s.handler_worker.stop();
            r.await(testing.io) catch {};
        }
        s.handler_worker.deinit();
        s.env.deinit();
    }
};

/// The signals `workflow_id`'s run received, in order, each checked to carry a request ID
/// and a link back to the scheduling of an operation of `caller_id`.
fn signalsOf(s: *Setup, a: std.mem.Allocator, workflow_id: []const u8, caller_id: []const u8) ![]const HistoryEvent {
    var signals: std.ArrayList(HistoryEvent) = .empty;
    for ((try s.env.client.fetchHistory(a, workflow_id, "")).events.items) |event| {
        const attrs = switch (event.attributes.?) {
            .workflow_execution_signaled_event_attributes => |attrs| attrs,
            else => continue,
        };
        try testing.expect(attrs.request_id.len > 0);
        try testing.expectEqual(1, event.links.items.len);
        const back = event.links.items[0].variant.?.workflow_event;
        try testing.expectEqualStrings(caller_id, back.workflow_id);
        try testing.expectEqual(EventType.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED, back.reference.?.event_ref.event_type);
        try signals.append(a, event);
    }
    return signals.items;
}

fn signalName(event: HistoryEvent) []const u8 {
    return event.attributes.?.workflow_execution_signaled_event_attributes.signal_name;
}

/// Whether the signal's argument contains `text`.
fn signalCarries(event: HistoryEvent, text: []const u8) bool {
    const input = event.attributes.?.workflow_execution_signaled_event_attributes.input orelse return false;
    return input.payloads.items.len == 1 and std.mem.indexOf(u8, input.payloads.items[0].data, text) != null;
}

test "the caller remote workflow attaches approval context, starts, messages and approves two greeting workflows" {
    var s: Setup = undefined;
    try s.start();
    defer s.deinit();

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var run = try s.env.startWorkflow(caller.CallerRemoteWorkflow, .{}, .{});
    defer run.deinit();
    const entries = try s.env.getResult(caller.CallerRemoteWorkflow, &run, a, null);
    const expected = [_][]const u8{
        "attached approval context before the workflow existed: nexus-messaging-greeting-one",
        "started remote workflow one: nexus-messaging-greeting-one",
        "started remote workflow two: nexus-messaging-greeting-two",
        "attached approval context to the running workflow: nexus-messaging-greeting-two",
        "getLanguages (one) returned 2 languages",
        "getLanguages (two) with unsupported returned 7 languages",
        "setLanguage(French) on one returned previous: English",
        "setLanguage(Spanish) on two returned previous: English",
        "getLanguage (one) = French",
        "getLanguage (two) = Spanish",
        "approved workflow one",
        "approved workflow two",
        "remote workflow one result: Bonjour, monde (approved by CallerRemoteWorkflow)",
        "remote workflow two result: Hola, mundo (approved by CallerRemoteWorkflow)",
    };
    try testing.expectEqual(expected.len, entries.len);
    for (expected, entries) |want, got| try testing.expectEqualStrings(want, got);

    // Workflow one was started by the signal-with-start, whose signal is its first event after
    // the start; runFromRemote attached to that run. Each signal arrived once.
    const one_id = try handler.workflowId(a, caller.user_one);
    const one_history = try s.env.client.fetchHistory(a, one_id, "");
    try testing.expectEqual(EventType.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, one_history.events.items[0].event_type);
    try testing.expectEqual(EventType.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED, one_history.events.items[1].event_type);
    const one = try signalsOf(&s, a, one_id, run.id);
    try testing.expectEqual(2, one.len);
    try testing.expectEqualStrings(handler.AttachApprovalContextSignal.signal_name, signalName(one[0]));
    try testing.expect(signalCarries(one[0], "nightly batch"));
    try testing.expectEqualStrings(handler.ApproveSignal.signal_name, signalName(one[1]));

    // Workflow two was started by runFromRemote; the signal-with-start only signaled it.
    const two_id = try handler.workflowId(a, caller.user_two);
    const two = try signalsOf(&s, a, two_id, run.id);
    try testing.expectEqual(2, two.len);
    try testing.expectEqualStrings(handler.AttachApprovalContextSignal.signal_name, signalName(two[0]));
    try testing.expect(signalCarries(two[0], "localization team"));
    try testing.expectEqualStrings(handler.ApproveSignal.signal_name, signalName(two[1]));

    var result = try s.env.client.getWorkflow([]const u8, one_id, "");
    defer result.deinit();
    try testing.expectEqualStrings("Bonjour, monde (approved by CallerRemoteWorkflow)", try result.get(a));
}

/// Attaches approval context twice to the greeting workflow of `user_id`, then approves it.
fn attachTwice(ctx: *workflow.Context, user_id: []const u8) !void {
    const Service = service.NexusRemoteGreetingService;
    const options: workflow.NexusOperationOptions = .{ .endpoint = caller.endpoint };
    _ = try ctx.executeNexusOperation(Service, service.AttachApprovalContext, options, .{ .note = "first note", .user_id = user_id }).get();
    _ = try ctx.executeNexusOperation(Service, service.AttachApprovalContext, options, .{ .note = "second note", .user_id = user_id }).get();
    _ = try ctx.executeNexusOperation(Service, service.Approve, options, .{ .name = "AttachTwice", .user_id = user_id }).get();
}

const AttachTwice = workflow.Def("AttachTwice", attachTwice);

test "attachApprovalContext starts the greeting workflow once and signals the running one afterwards" {
    var s: Setup = undefined;
    try s.start();
    defer s.deinit();

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var run = try s.env.startWorkflow(AttachTwice, .{}, .{"attach-twice"});
    defer run.deinit();
    try s.env.getResult(AttachTwice, &run, a, null);

    // One run, started by the first operation, received both notes and then the approval.
    const id = try handler.workflowId(a, "attach-twice");
    var greeting = try s.env.client.getWorkflow([]const u8, id, "");
    defer greeting.deinit();
    try testing.expectEqualStrings("Hello, world (approved by AttachTwice)", try greeting.get(a));
    const signals = try signalsOf(&s, a, id, run.id);
    try testing.expectEqual(3, signals.len);
    try testing.expect(signalCarries(signals[0], "first note"));
    try testing.expect(signalCarries(signals[1], "second note"));
    try testing.expectEqualStrings(handler.ApproveSignal.signal_name, signalName(signals[2]));
}
