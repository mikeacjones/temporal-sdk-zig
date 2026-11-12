//! Search attributes: a workflow that reads the typed search attribute it was started with,
//! upserts new values (and removes one), and then finds itself with a visibility query on
//! those values from an activity that lists workflows through the worker's client.

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;
const activity = temporal.activity;
const sa = temporal.search_attributes;

pub const custom_int_field = sa.key(.int, "CustomIntField");
pub const custom_keyword_field = sa.key(.keyword, "CustomKeywordField");
pub const custom_bool_field = sa.key(.bool, "CustomBoolField");
pub const custom_double_field = sa.key(.double, "CustomDoubleField");
pub const custom_datetime_field = sa.key(.datetime, "CustomDatetimeField");
pub const custom_keyword_list_field = sa.key(.keyword_list, "CustomKeywordListField");

/// A workflow run as the visibility query reports it.
pub const Execution = struct {
    workflow_id: []const u8,
    run_id: []const u8,
};

/// Lists the workflow runs matching `query`. Visibility is eventually consistent, so it lists
/// again until the run that scheduled this activity shows up, heartbeating as it goes; the
/// activity's timeouts bound the wait.
fn listExecutions(ctx: *activity.Context, query: []const u8) ![]const Execution {
    const logger = ctx.logger();
    const c = try ctx.client();
    const current_run_id = ctx.info().workflow_run_id;
    const a = ctx.allocator();

    logger.info("List executions.", .{ .query = query });

    while (true) {
        var it = try c.listWorkflows(query, .{ .page_size = 10 });
        defer it.deinit();
        var executions: std.ArrayList(Execution) = .empty;
        var seen_current_execution = false;
        while (try it.next(a)) |info| {
            seen_current_execution = seen_current_execution or std.mem.eql(u8, info.run_id, current_run_id);
            try executions.append(a, .{ .workflow_id = info.workflow_id, .run_id = info.run_id });
            ctx.heartbeat({});
        }
        if (seen_current_execution) return executions.items;
        ctx.heartbeat({});
        try ctx.io().sleep(.fromMilliseconds(100), .awake);
    }
}

pub const ListExecutions = activity.Def("ListExecutions", listExecutions);

/// Logs every search attribute the workflow currently has, in name order, with its value as
/// JSON.
fn printSearchAttributes(ctx: *workflow.Context) void {
    const logger = ctx.logger();
    for (ctx.searchAttributes().fields) |field| {
        logger.info("Current search attribute value", .{ .name = field.name, .value = field.value.data });
    }
}

fn searchAttributesWorkflow(ctx: *workflow.Context) !void {
    const logger = ctx.logger();
    const a = ctx.allocator();
    logger.info("SearchAttributes workflow started", .{});

    const current_int_value = try ctx.searchAttributes().get(a, custom_int_field) orelse
        return ctx.fail(.{ .message = "expected CustomIntField to be set" });
    logger.info("Current search attribute value.", .{ .CustomIntField = current_int_value });

    // `ctx.searchAttributes()` sees the upsert right away; the server records it when this
    // workflow task completes.
    try ctx.upsertSearchAttributes(&.{
        custom_int_field.set(2), // update CustomIntField from 1 to 2, then insert other fields
        custom_keyword_field.set("Keyword fields supports prefix search"),
        custom_bool_field.set(true),
        custom_double_field.set(3.14),
        custom_datetime_field.set(.fromTimestamp(ctx.now())),
        custom_keyword_list_field.set(&.{ "value1", "value2" }),
    });
    printSearchAttributes(ctx);

    // Remove a value with `unset`.
    try ctx.upsertSearchAttributes(&.{custom_double_field.unset()});
    printSearchAttributes(ctx);

    // Sleeping completes the workflow task, so the server has the upserts and the visibility
    // store can index them.
    try ctx.io().sleep(.fromSeconds(1), .awake);

    const query = try std.fmt.allocPrint(a, "{s}=2 AND {s} STARTS_WITH 'Keyword fields'", .{
        custom_int_field.name,
        custom_keyword_field.name,
    });
    const ao: workflow.ActivityOptions = .{
        .start_to_close_timeout = .fromSeconds(10),
        .heartbeat_timeout = .fromSeconds(1),
    };
    const list_results = ctx.executeActivity(ListExecutions, ao, .{query}).get() catch |err| {
        logger.err("Failed to list workflow executions.", .{ .err = err });
        return err;
    };
    if (list_results.len == 0) {
        logger.err("The visibility query matched no workflow executions.", .{ .query = query });
        return error.NoExecutionsListed;
    }

    // Visibility lists open runs before closed ones, newest first, so the first one is this
    // run even when earlier runs match the query too.
    const last_execution = list_results[0];
    const info = ctx.info();
    logger.info("WorkflowID must be the same", .{ .@"info.WorkflowID" = info.workflow_id, .@"lastExecution.WorkflowID" = last_execution.workflow_id });
    logger.info("RunID must be the same", .{ .@"info.RunID" = info.run_id, .@"lastExecution.RunID" = last_execution.run_id });

    // No change since the last print.
    printSearchAttributes(ctx);

    logger.info("Workflow completed.", .{});
}

pub const SearchAttributesWorkflow = workflow.Def("SearchAttributesWorkflow", searchAttributesWorkflow);
