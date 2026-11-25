//! Populates a server with fixtures for trying out a UI: runs in every status, a long
//! history, large payloads, open and closed runs, stuck runs, and many namespaces.
//!
//! Usage: `starter [--wait] [<fixture>...]`, where a fixture is one of `rainbow-statuses`,
//! `large-event-history`, `largepayload`, `openNclosed`, `stuck-workflows` and `namespaces`.
//! With none named, it starts them all, in that order. It returns once everything is started;
//! with `--wait` it then waits until each workflow has the status its fixture is for, and
//! prints it.

const std = @import("std");
const temporal = @import("temporal");
const rainbow_statuses = @import("rainbow_statuses.zig");
const large_event_history = @import("large_event_history.zig");
const largepayload = @import("largepayload.zig");
const open_n_closed = @import("open_n_closed.zig");
const stuck_workflows = @import("stuck_workflows.zig");

const Client = temporal.client.Client;
const Status = temporal.client.WorkflowExecutionStatus;
const sa = temporal.search_attributes;

/// How many sets of rainbow statuses to start.
const number_of_sets = 1;
/// About how many events the large event history workflow's history has.
const length_of_history = 1000;
/// Whether the large event history workflow fails after its activities.
const will_fail = true;
/// How many large payload workflows to start.
const largepayload_workflows = 5;
/// The size of each large payload, and of each large payload workflow's memo.
const payload_size = 1024 * 1024;
/// How many open and closed workflows to start.
const open_n_closed_workflows = 5;
/// Whether the open and closed workflows stay open.
const keep_open = true;
/// How many namespaces to register.
const number_of_namespaces = 20;

const custom_keyword_field = sa.key(.keyword, "CustomKeywordField");
const custom_int_field = sa.key(.int, "CustomIntField");
const custom_double_field = sa.key(.double, "CustomDoubleField");
const custom_bool_field = sa.key(.bool, "CustomBoolField");
const custom_datetime_field = sa.key(.datetime, "CustomDatetimeField");
const custom_text_field = sa.key(.text, "CustomTextField");

const Fixture = enum {
    @"rainbow-statuses",
    @"large-event-history",
    largepayload,
    openNclosed,
    @"stuck-workflows",
    namespaces,
};

/// A started workflow `--wait` waits for, and the line it prints once the workflow has
/// `status`: `<label> status=<tag> WorkflowID=<id>`.
const Started = struct {
    label: []const u8,
    id: []const u8,
    run_id: []const u8,
    status: Status,
};

/// How long `--wait` waits for each workflow to reach its status.
const wait_limit: std.Io.Duration = .fromSeconds(60);
const poll_interval: std.Io.Duration = .fromMilliseconds(200);

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;
    const arena = init.arena.allocator();

    var wait = false;
    var chosen: std.EnumSet(Fixture) = .empty;
    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.skip();
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--wait")) {
            wait = true;
        } else if (std.meta.stringToEnum(Fixture, arg)) |f| {
            chosen.insert(f);
        } else {
            std.log.err("Unknown argument {s}. Usage: starter [--wait] [<fixture>...], where a fixture is one of: {s}", .{ arg, fixture_names });
            std.process.exit(2);
        }
    }
    if (chosen.count() == 0) chosen = .full;

    // The client is a heavyweight object that should be created once per process.
    const options = try temporal.client.loadDefaultOptions(arena);
    var client = Client.connect(gpa, io, options) catch |err| {
        std.log.err("Unable to create client: {t}", .{err});
        std.process.exit(1);
    };
    defer client.close();

    var started: std.ArrayList(Started) = .empty;
    for (std.enums.values(Fixture)) |f| {
        if (!chosen.contains(f)) continue;
        switch (f) {
            .@"rainbow-statuses" => try startRainbowStatuses(&client, arena, io, &started),
            .@"large-event-history" => try startLargeEventHistory(&client, arena, io, &started),
            .largepayload => try startLargePayload(&client, arena, io, &started),
            .openNclosed => try startOpenNClosed(&client, arena, io, &started),
            .@"stuck-workflows" => try startStuckWorkflows(&client, arena, io, &started),
            .namespaces => try registerNamespaces(&client, arena, io),
        }
    }

    if (!wait) return;
    var scratch: std.heap.ArenaAllocator = .init(gpa);
    defer scratch.deinit();
    for (started.items) |s| {
        const status = waitForStatus(&client, &scratch, io, s) catch |err| {
            std.log.err("Unable to describe workflow {s}: {t}", .{ s.id, err });
            std.process.exit(1);
        };
        if (status != s.status) {
            std.log.err("Workflow {s} has status {s}, not {s}", .{ s.id, statusName(status), statusName(s.status) });
            std.process.exit(1);
        }
        std.log.info("{s} status={s} WorkflowID={s}", .{ s.label, statusName(status), s.id });
    }
}

const fixture_names = blk: {
    var text: []const u8 = "";
    for (std.meta.fieldNames(Fixture), 0..) |name, i| text = text ++ (if (i == 0) "" else ", ") ++ name;
    break :blk text;
};

/// The tag of `s`, or "unknown" for a status this SDK does not name.
fn statusName(s: Status) []const u8 {
    return std.enums.tagName(Status, s) orelse "unknown";
}

/// Describes the run every `poll_interval` until it has the status `s` is for, for up to
/// `wait_limit`, and returns the status it last had. A run that has closed with another
/// status will not reach it, so that status is returned at once.
fn waitForStatus(client: *Client, scratch: *std.heap.ArenaAllocator, io: std.Io, s: Started) !Status {
    const deadline = std.Io.Clock.awake.now(io).addDuration(wait_limit);
    while (true) {
        _ = scratch.reset(.retain_capacity);
        const description = try client.describeWorkflow(scratch.allocator(), s.id, s.run_id);
        if (description.status == s.status) return description.status;
        if (description.status != .running) return description.status;
        if (std.Io.Clock.awake.now(io).compare(.gte, deadline)) return description.status;
        try io.sleep(poll_interval, .awake);
    }
}

/// The first `n` hex digits of a fresh UUID, as the fixtures' IDs use.
fn shortId(arena: std.mem.Allocator, io: std.Io, comptime n: usize) ![]const u8 {
    return (try temporal.client.uuid(arena, io))[0..n];
}

fn logStarted(run: anytype) void {
    std.log.info("Started workflow WorkflowID={s} RunID={s}", .{ run.id, run.run_id });
}

fn fail(comptime action: []const u8, err: anyerror) noreturn {
    std.log.err("Unable to " ++ action ++ ": {t}", .{err});
    std.process.exit(1);
}

fn startRainbowStatuses(client: *Client, arena: std.mem.Allocator, io: std.Io, started: *std.ArrayList(Started)) !void {
    for (0..number_of_sets) |_| {
        const prefix = try shortId(arena, io, 6);
        for (std.enums.values(rainbow_statuses.Status), 0..) |status, i| {
            const label = rainbow_statuses.label(status);
            const now = std.Io.Clock.real.now(io);
            var run = client.executeWorkflow(rainbow_statuses.RainbowStatusesWorkflow, .{
                .id = try std.fmt.allocPrint(arena, "{s}_{s}", .{ prefix, label }),
                .task_queue = rainbow_statuses.task_queue,
                .execution_timeout = if (status == .timed_out) .fromSeconds(1) else null,
                .search_attributes = &.{
                    custom_keyword_field.set(try std.fmt.allocPrint(arena, "rainbow-statuses-{s}", .{prefix})),
                    custom_int_field.set(@intCast(i)),
                    custom_double_field.set(@floatFromInt(i)),
                    custom_bool_field.set(i % 2 == 0),
                    custom_datetime_field.set(.fromTimestamp(now)),
                    custom_text_field.set(try std.fmt.allocPrint(arena, "rainbow statuses {s} {s}", .{ prefix, label })),
                },
            }, .{status}) catch |err| fail("execute workflow", err);
            defer run.deinit();
            logStarted(run);

            switch (status) {
                .canceled => client.cancelWorkflow(run.id, run.run_id, "sample cancellation") catch |err| fail("cancel workflow", err),
                .terminated => client.terminateWorkflow(run.id, run.run_id, "sample termination") catch |err| fail("terminate workflow", err),
                .running => client.signalWorkflowByName("customSignal", run.id, run.run_id, .{.{
                    .Hey = "from Mars",
                    .At = temporal.converter.Time.fromTimestamp(now),
                }}) catch |err| fail("signal workflow", err),
                else => {},
            }

            try started.append(arena, .{
                .label = try std.fmt.allocPrint(arena, "rainbow-statuses {s}", .{label}),
                .id = try arena.dupe(u8, run.id),
                .run_id = try arena.dupe(u8, run.run_id),
                .status = switch (status) {
                    .running => .running,
                    .completed => .completed,
                    .failed => .failed,
                    .canceled => .canceled,
                    .terminated => .terminated,
                    .continued_as_new => .continued_as_new,
                    .timed_out => .timed_out,
                },
            });
        }
    }
}

fn startLargeEventHistory(client: *Client, arena: std.mem.Allocator, io: std.Io, started: *std.ArrayList(Started)) !void {
    var run = client.executeWorkflow(large_event_history.LargeEventHistoryWorkflow, .{
        .id = try std.fmt.allocPrint(arena, "largeeventhistory_{s}", .{try shortId(arena, io, 4)}),
        .task_queue = large_event_history.task_queue,
    }, .{ length_of_history, will_fail }) catch |err| fail("execute workflow", err);
    defer run.deinit();
    logStarted(run);
    try started.append(arena, .{
        .label = "large-event-history",
        .id = try arena.dupe(u8, run.id),
        .run_id = try arena.dupe(u8, run.run_id),
        .status = if (will_fail) .failed else .completed,
    });
}

fn startLargePayload(client: *Client, arena: std.mem.Allocator, io: std.Io, started: *std.ArrayList(Started)) !void {
    const id = try shortId(arena, io, 4);
    // Random bytes are not valid UTF-8, so the memo holds them as `binary/plain`.
    const memo_token = try arena.alloc(u8, payload_size);
    io.random(memo_token);
    const memo = try temporal.converter.toFields(arena, .{ .attr1 = temporal.converter.Bytes{ .data = memo_token } });
    for (1..largepayload_workflows + 1) |i| {
        var run = client.executeWorkflow(largepayload.LargePayloadWorkflow, .{
            .id = try std.fmt.allocPrint(arena, "largepayload_{s}_{d}", .{ id, i }),
            .task_queue = largepayload.task_queue,
            .memo = memo,
        }, .{payload_size}) catch |err| fail("execute workflow", err);
        defer run.deinit();
        logStarted(run);
        try started.append(arena, .{
            .label = try std.fmt.allocPrint(arena, "largepayload {d}", .{i}),
            .id = try arena.dupe(u8, run.id),
            .run_id = try arena.dupe(u8, run.run_id),
            .status = .completed,
        });
    }
}

fn startOpenNClosed(client: *Client, arena: std.mem.Allocator, io: std.Io, started: *std.ArrayList(Started)) !void {
    const id = try shortId(arena, io, 8);
    for (1..open_n_closed_workflows + 1) |i| {
        var run = client.executeWorkflow(open_n_closed.OpenNClosedWorkflow, .{
            .id = try std.fmt.allocPrint(arena, "{s}###___{d}", .{ id, i }),
            .task_queue = open_n_closed.task_queue,
        }, .{keep_open}) catch |err| fail("execute workflow", err);
        defer run.deinit();
        logStarted(run);
        try started.append(arena, .{
            .label = try std.fmt.allocPrint(arena, "openNclosed {d}", .{i}),
            .id = try arena.dupe(u8, run.id),
            .run_id = try arena.dupe(u8, run.run_id),
            .status = if (keep_open) .running else .completed,
        });
    }
}

fn startStuckWorkflows(client: *Client, arena: std.mem.Allocator, io: std.Io, started: *std.ArrayList(Started)) !void {
    const id = try shortId(arena, io, 6);
    const runs = [_]struct { suffix: []const u8, task_queue: []const u8 }{
        .{ .suffix = "stuck_activity", .task_queue = stuck_workflows.task_queue },
        .{ .suffix = "no_worker", .task_queue = stuck_workflows.no_worker_task_queue },
    };
    for (runs) |r| {
        var run = client.executeWorkflow(stuck_workflows.StuckWorkflow, .{
            .id = try std.fmt.allocPrint(arena, "{s}_{s}", .{ id, r.suffix }),
            .task_queue = r.task_queue,
        }, .{}) catch |err| fail("execute workflow", err);
        defer run.deinit();
        logStarted(run);
        try started.append(arena, .{
            .label = try std.fmt.allocPrint(arena, "stuck-workflows {s}", .{r.suffix}),
            .id = try arena.dupe(u8, run.id),
            .run_id = try arena.dupe(u8, run.run_id),
            .status = .running,
        });
    }
}

fn registerNamespaces(client: *Client, arena: std.mem.Allocator, io: std.Io) !void {
    const id = try shortId(arena, io, 6);
    for (1..number_of_namespaces + 1) |i| {
        _ = client.callService(.workflow, "RegisterNamespace", arena, .{
            .namespace = try std.fmt.allocPrint(arena, "{s}_{d}", .{ id, i }),
            .description = try std.fmt.allocPrint(arena, "Namespace Description {d}", .{i}),
            .owner_email = "owner@mail.com",
            .workflow_execution_retention_period = .{ .seconds = 24 * std.time.s_per_hour },
        }, null) catch |err| fail("register namespace", err);
    }
    std.log.info("namespaces registered={d}", .{number_of_namespaces});
}
