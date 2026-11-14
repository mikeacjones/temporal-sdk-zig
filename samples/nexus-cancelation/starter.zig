//! Runs the caller workflow in namespace my-cancelation-caller-namespace and prints its
//! result. Then it shows from the server's records how each say-hello operation ended: from
//! the caller's history, which one completed and which were canceled, and for each canceled
//! one, from its handler workflow's history (in the namespace the operation's link names),
//! that the workflow received the cancellation, cleaned up, and ended canceled.
//!
//! Usage: starter [--work-ms <language>=<ms>,...] [--cleanup-ms <ms>]
//!
//! By default the handler workflows draw their work and cleanup times at random, so which
//! operation completes first changes from run to run, and now and then every operation
//! completes before the cancellation. The options fix the times: `--work-ms` how long the
//! handler workflow of each listed language works, and `--cleanup-ms` how long a canceled one
//! cleans up. samples-test lets one language finish at once and the others work for ten
//! minutes, so that one completes and the others are canceled.

const std = @import("std");
const temporal = @import("temporal");
const service = @import("service.zig");
const caller = @import("caller.zig");
const history = @import("history.zig");

const Client = temporal.client.Client;

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;
    const arena = init.arena.allocator();

    var bad_arg: []const u8 = "";
    const timing = parseArgs(init.minimal.args, &bad_arg) catch |err| {
        std.log.err("Invalid argument \"{s}\": {t}. Usage: starter [--work-ms <language>=<ms>,...] [--cleanup-ms <ms>], with languages {s}", .{ bad_arg, err, language_names });
        std.process.exit(2);
    };

    var client = caller.connect(gpa, io, arena) catch |err| {
        std.log.err("Unable to create client for namespace {s}: {t}", .{ caller.namespace, err });
        std.process.exit(1);
    };
    defer client.close();

    var run = client.executeWorkflow(caller.HelloCallerWorkflow, .{
        .id = try std.fmt.allocPrint(arena, "nexus_cancelation_caller_workflow_{s}", .{try temporal.client.uuid(arena, io)}),
        .task_queue = caller.task_queue,
    }, .{ "Nexus", timing }) catch |err| {
        std.log.err("Unable to execute workflow: {t}", .{err});
        std.process.exit(1);
    };
    defer run.deinit();
    std.log.info("Started workflow WorkflowID={s} RunID={s}", .{ run.id, run.run_id });

    // Synchronously wait for the workflow completion.
    const result = run.get(arena) catch |err| {
        std.log.err("Unable to get workflow result: {t}", .{err});
        std.process.exit(1);
    };
    std.log.info("Workflow result: {s}", .{result});

    const caller_history = client.fetchHistory(arena, run.id, run.run_id) catch |err| {
        std.log.err("Unable to fetch the history of workflow {s}: {t}", .{ run.id, err });
        std.process.exit(1);
    };
    const operations = history.operations(arena, caller_history) catch |err| {
        std.log.err("Unable to read the operations in the history of workflow {s}: {t}", .{ run.id, err });
        std.process.exit(1);
    };
    // A client for the handlers' namespace, connected when the first canceled operation needs it.
    var handler_client: ?Client = null;
    defer if (handler_client) |*c| c.close();
    var returned_completed = false;
    for (operations) |op| switch (op.outcome) {
        .completed => |message| {
            std.log.info("Operation for {t} completed: {s}", .{ op.language, message });
            if (std.mem.eql(u8, message, result)) returned_completed = true;
        },
        .canceled => showCanceled(gpa, io, arena, &handler_client, op) catch |err| {
            std.log.err("Operation for {t} was canceled, but its handler workflow didn't end as expected: {t}", .{ op.language, err });
            std.process.exit(1);
        },
        else => |outcome| std.log.info("Operation for {t} ended {t}", .{ op.language, outcome }),
    };
    if (!returned_completed) {
        std.log.err("Workflow {s} returned \"{s}\", which no completed operation's history records", .{ run.id, result });
        std.process.exit(1);
    }
}

/// Waits for the handler workflow of the canceled operation `op` to end, and prints how it
/// handled the cancellation, from its history. Connects `handler_client` to the workflow's
/// namespace if it isn't; the caller closes it.
fn showCanceled(gpa: std.mem.Allocator, io: std.Io, arena: std.mem.Allocator, handler_client: *?Client, op: history.Operation) !void {
    const handler = op.handler orelse return error.OperationNeverStarted;
    if (handler_client.*) |*c| if (!std.mem.eql(u8, c.namespace, handler.namespace)) {
        c.close();
        handler_client.* = null;
    };
    if (handler_client.* == null) {
        var options = try temporal.client.loadDefaultOptions(arena);
        options.namespace = handler.namespace;
        handler_client.* = try Client.connect(gpa, io, options);
    }
    const client = &handler_client.*.?;

    var handler_run = try client.getWorkflow(service.HelloOutput, handler.workflow_id, handler.run_id);
    defer handler_run.deinit();
    if (handler_run.get(arena)) |_| return error.HandlerWorkflowCompleted else |err| switch (err) {
        error.WorkflowCanceled => {},
        else => return err,
    }
    const cleanup = try history.cleanupAfterCancel(arena, try client.fetchHistory(arena, handler.workflow_id, handler.run_id));
    std.log.info("Operation for {t} canceled: handler workflow {s} in namespace {s} received the cancellation, cleaned up for {d} ms, and ended canceled", .{
        op.language, handler.workflow_id, handler.namespace, cleanup.toMilliseconds(),
    });
}

const language_names = blk: {
    var names: []const u8 = "";
    for (std.meta.fieldNames(service.Language), 0..) |name, i| names = names ++ (if (i > 0) ", " else "") ++ name;
    break :blk names;
};

/// The timing the arguments ask for. On error, `bad_arg` is the argument at fault.
fn parseArgs(argv: std.process.Args, bad_arg: *[]const u8) !caller.Timing {
    var timing: caller.Timing = .{};
    var args = std.process.Args.Iterator.init(argv);
    _ = args.skip();
    while (args.next()) |arg| {
        bad_arg.* = arg;
        if (std.mem.eql(u8, arg, "--cleanup-ms")) {
            bad_arg.* = args.next() orelse return error.MissingValue;
            timing.cleanup_ms = try std.fmt.parseInt(u32, bad_arg.*, 10);
        } else if (std.mem.eql(u8, arg, "--work-ms")) {
            var entries = std.mem.splitScalar(u8, args.next() orelse return error.MissingValue, ',');
            while (entries.next()) |entry| {
                bad_arg.* = entry;
                const eq = std.mem.indexOfScalar(u8, entry, '=') orelse return error.ExpectedLanguageEqualsMilliseconds;
                const language = std.meta.stringToEnum(service.Language, entry[0..eq]) orelse return error.UnknownLanguage;
                const ms = try std.fmt.parseInt(u32, entry[eq + 1 ..], 10);
                switch (language) {
                    inline else => |l| @field(timing.work_ms, @tagName(l)) = ms,
                }
            }
        } else return error.UnknownOption;
    }
    return timing;
}
