//! Tests of the on-demand pattern: the caller workflow starts two greeting workflows through
//! the service's handlers, messages them, and waits for their results, all served by the test
//! environment's worker through an endpoint that routes to it.

const std = @import("std");
const temporal = @import("temporal");
const handler = @import("ondemandpattern/handler.zig");
const caller = @import("ondemandpattern/caller.zig");

const testing = std.testing;
const WorkflowEnvironment = temporal.testing.WorkflowEnvironment;

test "the caller remote workflow starts, messages and approves two greeting workflows" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try handler.register(&env.worker);
    try caller.register(&env.worker);
    try env.createNexusEndpoint(caller.endpoint, .{});

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const entries = try env.executeWorkflow(caller.CallerRemoteWorkflow, .{}, a, null);
    const expected = [_][]const u8{
        "started remote workflow one: nexus-messaging-greeting-one",
        "started remote workflow two: nexus-messaging-greeting-two",
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

    // The operations started the greeting workflows under the users' workflow IDs.
    var one = try env.client.getWorkflow([]const u8, try handler.workflowId(a, caller.user_one), "");
    defer one.deinit();
    try testing.expectEqualStrings("Bonjour, monde (approved by CallerRemoteWorkflow)", try one.get(a));
}
