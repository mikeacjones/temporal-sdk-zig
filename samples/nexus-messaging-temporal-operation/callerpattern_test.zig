//! Tests of the caller pattern: the greeting workflow's messages, and the caller workflow
//! messaging a running greeting workflow through the service's handlers, served by the test
//! environment's worker through an endpoint that routes to it.

const std = @import("std");
const temporal = @import("temporal");
const service = @import("callerpattern/service.zig");
const handler = @import("callerpattern/handler.zig");
const caller = @import("callerpattern/caller.zig");

const testing = std.testing;
const WorkflowEnvironment = temporal.testing.WorkflowEnvironment;
const EventType = temporal.proto.temporal.api.enums.v1.EventType;

/// An environment whose worker runs the handler's workflow, activity and service and the
/// caller workflow, with `caller.endpoint` routed to its own namespace and task queue. Its
/// dev server returns links for signals, so a handler's signal and the caller's operation
/// link to each other.
fn startEnvironment() !*WorkflowEnvironment {
    const env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{
        .dev_server = .{ .extra_args = &temporal.testing.DevServer.signal_link_args },
    });
    errdefer env.deinit();
    try handler.register(&env.worker);
    try caller.register(&env.worker);
    try env.createNexusEndpoint(caller.endpoint, .{});
    return env;
}

test "the greeting workflow answers its queries, takes a new language, and completes once approved" {
    const env = try startEnvironment();
    defer env.deinit();

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const id = try handler.workflowId(a, "tester");
    try testing.expectEqualStrings("GreetingWorkflow_for_tester", id);

    var run = try env.startWorkflow(handler.GreetingWorkflow, .{ .id = id }, .{"tester"});
    defer run.deinit();

    const supported = try env.queryWorkflow(handler.GetLanguagesQuery, a, id, .{false});
    try testing.expectEqualSlices(service.Language, &.{ .chinese, .english }, supported.languages);
    const all = try env.queryWorkflow(handler.GetLanguagesQuery, a, id, .{true});
    try testing.expectEqual(7, all.languages.len);
    try testing.expectEqual(.english, try env.queryWorkflow(handler.GetLanguageQuery, a, id, .{}));

    // Hindi has no greeting yet: the update fetches them all and returns the previous language.
    try testing.expectEqual(.english, try env.updateWorkflow(handler.SetLanguageUpdate, a, id, .{.hindi}, null));
    try testing.expectEqual(.hindi, try env.queryWorkflow(handler.GetLanguageQuery, a, id, .{}));
    try testing.expectEqual(7, (try env.queryWorkflow(handler.GetLanguagesQuery, a, id, .{false})).languages.len);
    try testing.expectEqual(.hindi, try env.updateWorkflow(handler.SetLanguageUpdate, a, id, .{.chinese}, null));

    try env.signalWorkflow(handler.ApproveSignal, id, .{"tester"});
    try testing.expectEqualStrings("你好，世界 (approved by tester)", try env.getResult(handler.GreetingWorkflow, &run, a, null));
}

test "the caller workflow messages the user's running greeting workflow through Nexus, and the approve signal links back to it" {
    const env = try startEnvironment();
    defer env.deinit();

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const greeting_id = try handler.workflowId(a, "user-1");
    var greeting = try env.startWorkflow(handler.GreetingWorkflow, .{ .id = greeting_id }, .{"user-1"});
    defer greeting.deinit();

    var run = try env.startWorkflow(caller.CallerWorkflow, .{}, .{"user-1"});
    defer run.deinit();
    const entries = try env.getResult(caller.CallerWorkflow, &run, a, null);
    const expected = [_][]const u8{
        "getLanguages returned 2 languages",
        "setLanguage(Arabic) returned previous language: English",
        "getLanguage returned: Arabic (confirmed Arabic)",
        "approve sent successfully",
    };
    try testing.expectEqual(expected.len, entries.len);
    for (expected, entries) |want, got| try testing.expectEqualStrings(want, got);

    // Approved, the greeting workflow completed in Arabic.
    try testing.expectEqualStrings("مرحبا بالعالم (approved by CallerWorkflow)", try env.getResult(handler.GreetingWorkflow, &greeting, a, null));

    // The approve signal arrived once, carrying the Nexus request's ID and a link to the
    // caller's scheduling of the operation.
    var signals: usize = 0;
    for ((try env.client.fetchHistory(a, greeting.id, greeting.run_id)).events.items) |event| {
        const attrs = switch (event.attributes.?) {
            .workflow_execution_signaled_event_attributes => |s| s,
            else => continue,
        };
        try testing.expectEqualStrings(handler.ApproveSignal.signal_name, attrs.signal_name);
        try testing.expect(attrs.request_id.len > 0);
        try testing.expectEqual(1, event.links.items.len);
        const back = event.links.items[0].variant.?.workflow_event;
        try testing.expectEqualStrings(run.id, back.workflow_id);
        try testing.expectEqual(EventType.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED, back.reference.?.event_ref.event_type);
        signals += 1;
    }
    try testing.expectEqual(1, signals);

    // The approve operation's completion in the caller's history links to the signal.
    var completed_links: usize = 0;
    for ((try env.client.fetchHistory(a, run.id, run.run_id)).events.items) |event| {
        if (event.event_type != .EVENT_TYPE_NEXUS_OPERATION_COMPLETED) continue;
        for (event.links.items) |link| {
            const l = link.variant orelse continue;
            if (l == .workflow_event and std.mem.eql(u8, l.workflow_event.workflow_id, greeting_id)) completed_links += 1;
        }
    }
    try testing.expectEqual(1, completed_links);
}

test "the caller workflow fails when the user has no greeting workflow" {
    const env = try startEnvironment();
    defer env.deinit();

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    // The query finds no workflow, so the operation fails as not found and isn't retried.
    try testing.expectError(error.WorkflowFailed, env.executeWorkflow(caller.CallerWorkflow, .{"nobody"}, arena.allocator(), null));
}
