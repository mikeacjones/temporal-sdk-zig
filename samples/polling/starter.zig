//! Starts one workflow for each polling pattern and waits for their results.
//!
//! Usage: starter [infrequent-poll-interval-ms] [periodic-poll-interval-ms]. The intervals
//! default to 60 seconds and 1 second; samples-test passes shorter ones.

const std = @import("std");
const temporal = @import("temporal");
const frequent = @import("frequent.zig");
const infrequent = @import("infrequent.zig");
const periodic_sequence = @import("periodic_sequence.zig");

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;
    const arena = init.arena.allocator();

    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.skip();
    const infrequent_interval = intervalArg(args.next(), "infrequent-poll-interval-ms", infrequent.default_poll_interval);
    const periodic_interval = intervalArg(args.next(), "periodic-poll-interval-ms", .fromSeconds(1));

    // The client is a heavyweight object that should be created once per process.
    const options = try temporal.client.loadDefaultOptions(arena);
    var client = temporal.client.Client.connect(gpa, io, options) catch |err| {
        std.log.err("Unable to create client: {t}", .{err});
        std.process.exit(1);
    };
    defer client.close();

    // Start all three first so they poll at the same time, then wait for each.
    var frequent_run = client.executeWorkflow(frequent.FrequentPolling, .{
        .id = try std.fmt.allocPrint(arena, "FrequentPollingSampleWorkflow{s}", .{try temporal.client.uuid(arena, io)}),
        .task_queue = frequent.task_queue,
    }, .{}) catch |err| fatal("Unable to execute workflow FrequentPolling", err);
    defer frequent_run.deinit();
    std.log.info("Started workflow WorkflowID={s} RunID={s}", .{ frequent_run.id, frequent_run.run_id });

    var infrequent_run = client.executeWorkflow(infrequent.InfrequentPolling, .{
        .id = try std.fmt.allocPrint(arena, "InfrequentPollingSampleWorkflow{s}", .{try temporal.client.uuid(arena, io)}),
        .task_queue = infrequent.task_queue,
    }, .{infrequent_interval}) catch |err| fatal("Unable to execute workflow InfrequentPolling", err);
    defer infrequent_run.deinit();
    std.log.info("Started workflow WorkflowID={s} RunID={s}", .{ infrequent_run.id, infrequent_run.run_id });

    var periodic_run = client.executeWorkflow(periodic_sequence.PeriodicSequencePolling, .{
        .id = try std.fmt.allocPrint(arena, "pollingSampleQueue_{s}", .{try temporal.client.uuid(arena, io)}),
        .task_queue = periodic_sequence.task_queue,
    }, .{periodic_interval}) catch |err| fatal("Unable to execute workflow PeriodicSequencePolling", err);
    defer periodic_run.deinit();
    std.log.info("Started workflow WorkflowID={s} RunID={s}", .{ periodic_run.id, periodic_run.run_id });

    const frequent_result = frequent_run.get(arena) catch |err| fatal("Unable to get the FrequentPolling result", err);
    std.log.info("FrequentPolling result: {s}", .{frequent_result});
    const infrequent_result = infrequent_run.get(arena) catch |err| fatal("Unable to get the InfrequentPolling result", err);
    std.log.info("InfrequentPolling result: {s}", .{infrequent_result});
    const periodic_result = periodic_run.get(arena) catch |err| fatal("Unable to get the PeriodicSequencePolling result", err);
    std.log.info("PeriodicSequencePolling result: {s}", .{periodic_result});
}

fn intervalArg(arg: ?[]const u8, name: []const u8, default: std.Io.Duration) std.Io.Duration {
    const text = arg orelse return default;
    const ms = std.fmt.parseInt(i64, text, 10) catch -1;
    if (ms <= 0) {
        std.log.err("{s} must be a positive whole number of milliseconds, got \"{s}\"", .{ name, text });
        std.process.exit(2);
    }
    return .fromMilliseconds(ms);
}

fn fatal(comptime what: []const u8, err: anyerror) noreturn {
    std.log.err(what ++ ": {t}", .{err});
    std.process.exit(1);
}
