//! Starts the memo workflow with a description memo, waits for it, and prints the memo the
//! workflow read back from the server. It then describes the workflow itself to show the
//! upserted memo is what the server stores.

const std = @import("std");
const temporal = @import("temporal");
const memo = @import("memo.zig");

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;
    const arena = init.arena.allocator();

    // The client is a heavyweight object that should be created once per process.
    const options = try temporal.client.loadDefaultOptions(arena);
    var client = temporal.client.Client.connect(gpa, io, options) catch |err| {
        std.log.err("Unable to create client: {t}", .{err});
        std.process.exit(1);
    };
    defer client.close();

    var run = client.executeWorkflow(memo.MemoWorkflow, .{
        .id = try std.fmt.allocPrint(arena, "memo_{s}", .{try temporal.client.uuid(arena, io)}),
        .task_queue = "memo",
        .memo = try temporal.converter.toFields(arena, .{ .description = "Test memo workflow" }),
    }, .{}) catch |err| {
        std.log.err("Unable to execute workflow: {t}", .{err});
        std.process.exit(1);
    };
    defer run.deinit();
    std.log.info("Started workflow WorkflowID={s} RunID={s}", .{ run.id, run.run_id });

    const entries = run.get(arena) catch |err| {
        std.log.err("Unable get workflow result: {t}", .{err});
        std.process.exit(1);
    };
    for (entries) |entry| {
        std.log.info("Workflow result memo: {s}={f}", .{ entry.name, std.json.fmt(entry.value, .{}) });
    }

    const description = client.describeWorkflow(arena, run.id, run.run_id) catch |err| {
        std.log.err("Unable to describe workflow {s}: {t}", .{ run.id, err });
        std.process.exit(1);
    };
    const described = description.memoValue([]const u8, arena, "description") catch |err| {
        std.log.err("Unable to decode the description memo of workflow {s}: {t}", .{ run.id, err });
        std.process.exit(1);
    } orelse {
        std.log.err("Workflow {s} has no description memo", .{run.id});
        std.process.exit(1);
    };
    std.log.info("Described memo: description={s}", .{described});
}
