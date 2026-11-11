//! Memo: a workflow that reads the memo it was started with, upserts a new value, and then
//! reads the memo back from the server with a describe activity. The activity is a method of
//! a struct holding a client, bound to it when the worker registers it.

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;
const activity = temporal.activity;
const converter = temporal.converter;

/// One memo entry with its value decoded from JSON.
pub const MemoEntry = struct {
    name: []const u8,
    value: std.json.Value,
};

pub const Activities = struct {
    client: *temporal.client.Client,

    /// Returns the memo of the latest run of `workflow_id`, as the server stores it.
    pub fn describeWorkflow(a: *Activities, ctx: *activity.Context, workflow_id: []const u8) ![]const MemoEntry {
        ctx.logger().info("Describe workflow.", .{ .WorkflowId = workflow_id });
        const description = try a.client.describeWorkflow(ctx.allocator(), workflow_id, "");
        return memoEntries(ctx.allocator(), description.memo);
    }
};

pub const DescribeWorkflow = activity.Def("DescribeWorkflow", Activities.describeWorkflow);

/// Decodes every memo field into `arena`, keeping the fields' order.
pub fn memoEntries(arena: std.mem.Allocator, fields: []const converter.Field) ![]const MemoEntry {
    const entries = try arena.alloc(MemoEntry, fields.len);
    for (fields, entries) |field, *entry| {
        entry.* = .{ .name = field.name, .value = try converter.fromPayload(std.json.Value, arena, field.value) };
    }
    return entries;
}

fn printMemo(logger: temporal.log.Logger, arena: std.mem.Allocator, entries: []const MemoEntry) !void {
    if (entries.len == 0) {
        logger.info("Current memo is empty.", .{});
        return;
    }
    for (entries) |entry| {
        const value = try std.json.Stringify.valueAlloc(arena, entry.value, .{});
        logger.info("Current memo value.", .{ .name = entry.name, .value = value });
    }
}

/// Returns the memo the describe activity read from the server after the upsert.
fn memoWorkflow(ctx: *workflow.Context) ![]const MemoEntry {
    const logger = ctx.logger();
    logger.info("Memo workflow started", .{});

    // Get the memo provided when the workflow was started.
    const current_description = ctx.memoValue([]const u8, "description") catch |err| {
        logger.err("Get memo failed.", .{ .err = err });
        return err;
    } orelse {
        logger.err("Get memo failed: the workflow was started without a description memo.", .{});
        return error.MissingDescriptionMemo;
    };
    logger.info("Current memo value.", .{ .description = current_description });

    // Update the memo. `ctx.memo()` sees the change right away; the server records it when
    // this workflow task completes.
    try ctx.upsertMemo(.{ .description = "Test upsert memo workflow" });
    try printMemo(logger, ctx.allocator(), try memoEntries(ctx.allocator(), ctx.memo()));

    // Sleeping completes the workflow task, so the server has stored the upsert before the
    // activity describes the workflow.
    try ctx.io().sleep(.fromSeconds(1), .awake);

    const ao: workflow.ActivityOptions = .{ .start_to_close_timeout = .fromSeconds(10) };
    const described = ctx.executeActivity(DescribeWorkflow, ao, .{ctx.info().workflow_id}).get() catch |err| {
        logger.err("Failed to describe workflow execution.", .{ .err = err });
        return err;
    };

    // Print the memo as the server stores it.
    try printMemo(logger, ctx.allocator(), described);

    logger.info("Workflow completed.", .{});
    return described;
}

pub const MemoWorkflow = workflow.Def("MemoWorkflow", memoWorkflow);
