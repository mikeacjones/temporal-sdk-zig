const std = @import("std");
const temporal = @import("temporal");
const splitmerge = @import("splitmerge_workflow.zig");

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

    var run = client.executeWorkflow(splitmerge.SampleSplitMergeSelectorWorkflow, .{
        .id = try std.fmt.allocPrint(arena, "split_merge_selector_{s}", .{try temporal.client.uuid(arena, io)}),
        .task_queue = "split-merge-selector",
    }, .{5}) catch |err| {
        std.log.err("Unable to execute workflow: {t}", .{err});
        std.process.exit(1);
    };
    defer run.deinit();
    std.log.info("Started workflow WorkflowID={s} RunID={s}", .{ run.id, run.run_id });

    // The Go sample only starts the workflow; waiting for its result lets samples-test check it.
    const result = run.get(arena) catch |err| {
        std.log.err("Unable get workflow result: {t}", .{err});
        std.process.exit(1);
    };
    std.log.info("Workflow result: NumberOfItemsInChunk={d} SumInChunk={d}", .{ result.NumberOfItemsInChunk, result.SumInChunk });
}
