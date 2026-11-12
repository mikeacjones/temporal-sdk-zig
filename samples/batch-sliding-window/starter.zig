//! Starts a batch and waits for it to finish.
//!
//! Usage: starter [page-size sliding-window-size partitions]. The defaults are 5, 10 and 3;
//! samples-test passes a wider window so the batch finishes sooner.

const std = @import("std");
const temporal = @import("temporal");
const sample = @import("batch_sliding_window.zig");

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;
    const arena = init.arena.allocator();

    var input: sample.ProcessBatchWorkflowInput = .{ .page_size = 5, .sliding_window_size = 10, .partitions = 3 };
    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.skip();
    inline for (.{ "page_size", "sliding_window_size", "partitions" }) |field| {
        if (args.next()) |a| @field(input, field) = std.fmt.parseInt(u32, a, 10) catch {
            std.log.err("{s} must be a whole number, got \"{s}\"", .{ field, a });
            std.process.exit(2);
        };
    }

    // The client is a heavyweight object that should be created once per process.
    const options = try temporal.client.loadDefaultOptions(arena);
    var client = temporal.client.Client.connect(gpa, io, options) catch |err| {
        std.log.err("Unable to create client: {t}", .{err});
        std.process.exit(1);
    };
    defer client.close();

    var run = client.executeWorkflow(sample.ProcessBatchWorkflow, .{
        .id = try temporal.client.uuid(arena, io),
        .task_queue = sample.task_queue,
    }, .{input}) catch |err| {
        std.log.err("Failure starting workflow: {t}", .{err});
        std.process.exit(1);
    };
    defer run.deinit();
    std.log.info("Started Workflow Execution WorkflowID={s} RunID={s}", .{ run.id, run.run_id });

    // Waiting for a batch is rarely needed in practice: batch workflows usually run long.
    const result = run.get(arena) catch |err| {
        std.log.err("Failure getting workflow result: {t}", .{err});
        std.process.exit(1);
    };
    std.log.info("Completed workflow WorkflowID={s} RunID={s} Result={d}", .{ run.id, run.run_id, result });
}
