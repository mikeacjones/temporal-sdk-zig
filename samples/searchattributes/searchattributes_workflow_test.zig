//! Tests for the search attributes sample.

const std = @import("std");
const temporal = @import("temporal");
const searchattributes = @import("searchattributes.zig");

const testing = std.testing;

const Doubles = struct {
    fn listExecutions(ctx: *temporal.activity.Context, query: []const u8) ![]const searchattributes.Execution {
        _ = ctx;
        try testing.expectEqualStrings("CustomIntField=2 AND CustomKeywordField STARTS_WITH 'Keyword fields'", query);
        return &.{.{ .workflow_id = "", .run_id = "" }};
    }
};

test "workflow upserts its search attributes, with a test double for the list activity" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(searchattributes.SearchAttributesWorkflow);
    try env.registerActivityAs(searchattributes.ListExecutions, Doubles.listExecutions);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var run = try env.startWorkflow(searchattributes.SearchAttributesWorkflow, .{
        .task_queue = "",
        .search_attributes = &.{searchattributes.custom_int_field.set(1)},
    }, .{});
    defer run.deinit();
    try env.getResult(searchattributes.SearchAttributesWorkflow, &run, a, null);

    // The server stores the attributes as both upserts left them.
    const sa = (try env.client.describeWorkflow(a, run.id, run.run_id)).search_attributes;
    try testing.expectEqual(@as(?i64, 2), try sa.get(a, searchattributes.custom_int_field));
    try testing.expectEqualStrings("Keyword fields supports prefix search", (try sa.get(a, searchattributes.custom_keyword_field)).?);
    try testing.expectEqual(@as(?bool, true), try sa.get(a, searchattributes.custom_bool_field));
    try testing.expectEqual(@as(?f64, null), try sa.get(a, searchattributes.custom_double_field));
    try testing.expect(try sa.get(a, searchattributes.custom_datetime_field) != null);
    const list = (try sa.get(a, searchattributes.custom_keyword_list_field)).?;
    try testing.expectEqual(2, list.len);
    try testing.expectEqualStrings("value1", list[0]);
    try testing.expectEqualStrings("value2", list[1]);
}

test "workflow fails when started without CustomIntField" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(searchattributes.SearchAttributesWorkflow);
    try env.registerActivityAs(searchattributes.ListExecutions, Doubles.listExecutions);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var failure: ?temporal.failure.Failure = null;
    try testing.expectError(error.WorkflowFailed, env.executeWorkflow(searchattributes.SearchAttributesWorkflow, .{}, arena.allocator(), &failure));
    try testing.expectEqualStrings("expected CustomIntField to be set", failure.?.message);
}

test "list activity finds the workflow through a visibility query on its upserted values" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(searchattributes.SearchAttributesWorkflow);
    try env.registerActivity(searchattributes.ListExecutions);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    // The activity lists until it sees this run, so completing means the query found it.
    var run = try env.startWorkflow(searchattributes.SearchAttributesWorkflow, .{
        .task_queue = "",
        .search_attributes = &.{searchattributes.custom_int_field.set(1)},
    }, .{});
    defer run.deinit();
    try env.getResult(searchattributes.SearchAttributesWorkflow, &run, a, null);

    // The same query from a client, once visibility has the completed run.
    const query = "CustomIntField=2 AND CustomKeywordField STARTS_WITH 'Keyword fields' AND ExecutionStatus = 'Completed'";
    var tries: usize = 0;
    while (try env.client.countWorkflows(query) != 1) : (tries += 1) {
        if (tries == 300) return error.TestUnexpectedResult;
        try testing.io.sleep(.fromMilliseconds(100), .awake);
    }
    var it = try env.client.listWorkflows(query, .{});
    defer it.deinit();
    const info = (try it.next(a)).?;
    try testing.expectEqualStrings(run.id, info.workflow_id);
    try testing.expectEqualStrings(run.run_id, info.run_id);
    try testing.expectEqual(@as(?bool, true), try info.search_attributes.get(a, searchattributes.custom_bool_field));
    try testing.expectEqual(@as(?f64, null), try info.search_attributes.get(a, searchattributes.custom_double_field));
    try testing.expect(try it.next(a) == null);
}
