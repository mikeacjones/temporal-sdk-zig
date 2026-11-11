//! Starts the cron workflow, as the Go sample does. The Go starter leaves it running; this one
//! waits for the first run's result and then terminates the workflow, so samples-test can
//! check a run and leave nothing behind. Pass --leave-running to behave like the Go starter.

const std = @import("std");
const temporal = @import("temporal");
const cron = @import("workflow.zig");

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;
    const arena = init.arena.allocator();

    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.skip();
    const leave_running = if (args.next()) |a| std.mem.eql(u8, a, "--leave-running") else false;

    // The client is a heavyweight object that should be created once per process.
    const options = try temporal.client.loadDefaultOptions(arena);
    var client = temporal.client.Client.connect(gpa, io, options) catch |err| {
        std.log.err("Unable to create client: {t}", .{err});
        std.process.exit(1);
    };
    defer client.close();

    const workflow_id = try std.fmt.allocPrint(arena, "cron_{s}", .{try temporal.client.uuid(arena, io)});
    var run = client.executeWorkflow(cron.SampleCronWorkflow, .{
        .id = workflow_id,
        .task_queue = "cron",
        .cron_schedule = "* * * * *",
    }, .{}) catch |err| {
        std.log.err("Unable to execute workflow: {t}", .{err});
        std.process.exit(1);
    };
    defer run.deinit();
    std.log.info("Started workflow WorkflowID={s} RunID={s}", .{ run.id, run.run_id });
    if (leave_running) return;

    // The first run starts at the next minute boundary. A finished cron run starts the next
    // scheduled run; its close event (completed, or continued-as-new) carries its result.
    const first_run_id = try arena.dupe(u8, run.run_id);
    const result = run.getWithOptions(arena, .{ .follow_runs = false }) catch |err| switch (err) {
        error.WorkflowContinuedAsNew => firstRunResult(&client, arena, workflow_id, first_run_id) catch |e| {
            std.log.err("Unable to read the first run's result: {t}", .{e});
            std.process.exit(1);
        },
        else => {
            std.log.err("Unable to wait for the first run: {t}", .{err});
            std.process.exit(1);
        },
    };
    var buf: [temporal.converter.Time.max_len]u8 = undefined;
    std.log.info("First cron run completed: RunTime={s}", .{result.RunTime.format(&buf)});

    client.terminateWorkflow(workflow_id, "", "cron sample finished") catch |err| {
        std.log.err("Unable to terminate the cron workflow: {t}", .{err});
        std.process.exit(1);
    };
    std.log.info("Terminated the cron workflow", .{});
}

/// The result of a finished cron run, from its history's close event. The server may present
/// the close as continued-as-new (as `get` saw it) while the history records it as completed;
/// both carry the result.
fn firstRunResult(client: *temporal.client.Client, arena: std.mem.Allocator, workflow_id: []const u8, run_id: []const u8) !cron.CronResult {
    const history = try client.fetchHistory(arena, workflow_id, run_id);
    const events = history.events.items;
    if (events.len == 0) return error.EmptyHistory;
    const payloads = switch (events[events.len - 1].attributes orelse return error.UnexpectedCloseEvent) {
        .workflow_execution_completed_event_attributes => |e| e.result,
        .workflow_execution_continued_as_new_event_attributes => |e| e.last_completion_result,
        else => return error.UnexpectedCloseEvent,
    } orelse return error.NoResult;
    if (payloads.payloads.items.len == 0) return error.NoResult;
    return temporal.converter.fromPayload(cron.CronResult, arena, payloads.payloads.items[0]);
}
