//! Walks through a recovery. It starts the trip workflow, signals it three trips and queries
//! its count (the work of the signal and query programs), then runs the recovery workflow,
//! which restarts the trip workflow in a new run. Querying the new run shows the count
//! rebuilt from the old run's history. The starter terminates the trip workflow at the end,
//! so nothing is left running.

const std = @import("std");
const temporal = @import("temporal");
const trip_workflow = @import("trip_workflow.zig");
const recovery_workflow = @import("recovery_workflow.zig");
const Client = temporal.client.Client;

const trip_workflow_id = "trip_workflow";
const trips = [_]trip_workflow.TripEvent{
    .{ .id = "Trip1", .total = 10 },
    .{ .id = "Trip2", .total = 20 },
    .{ .id = "Trip3", .total = 30 },
};
/// How many times to check a condition that becomes true shortly, 100ms apart.
const max_tries = 100;

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;
    const arena = init.arena.allocator();

    // The client is a heavyweight object that should be created once per process.
    const options = try temporal.client.loadDefaultOptions(arena);
    var client = Client.connect(gpa, io, options) catch |err| {
        std.log.err("Unable to create client: {t}", .{err});
        std.process.exit(1);
    };
    defer client.close();

    var trip_run = client.executeWorkflow(trip_workflow.TripWorkflow, .{
        .id = trip_workflow_id,
        .task_queue = "recovery",
    }, .{.{}}) catch |err| {
        std.log.err("Unable to execute workflow: {t}", .{err});
        std.process.exit(1);
    };
    defer trip_run.deinit();
    std.log.info("Started workflow WorkflowID={s} RunID={s}", .{ trip_run.id, trip_run.run_id });

    for (trips) |trip| {
        client.signalWorkflow(trip_workflow.TripSignal, trip_workflow_id, "", .{trip}) catch |err| {
            std.log.err("Unable to signal trip {s} to workflow {s}: {t}", .{ trip.id, trip_workflow_id, err });
            std.process.exit(1);
        };
    }
    waitForCounter(&client, io, arena, trips.len);
    std.log.info("Trip counter before recovery: {d}", .{trips.len});

    // Visibility lists a workflow a moment after it starts; the recovery workflow finds the
    // trip workflow through it.
    const query = "WorkflowId = '" ++ trip_workflow_id ++ "' AND WorkflowType = 'TripWorkflow' AND ExecutionStatus = 'Running'";
    var tries: usize = 0;
    while (true) : (tries += 1) {
        const count = client.countWorkflows(query) catch |err| {
            std.log.err("Unable to count workflows matching \"{s}\": {t}", .{ query, err });
            std.process.exit(1);
        };
        if (count == 1) break;
        if (tries == max_tries) {
            std.log.err("No workflow matched \"{s}\" after {d} tries", .{ query, tries });
            std.process.exit(1);
        }
        try io.sleep(.fromMilliseconds(100), .awake);
    }

    var recovery_run = client.executeWorkflow(recovery_workflow.RecoverWorkflow, .{
        .id = try std.fmt.allocPrint(arena, "recovery_workflow_{s}", .{try temporal.client.uuid(arena, io)}),
        .task_queue = "recovery",
        .execution_timeout = .fromSeconds(60),
    }, .{.{ .workflow_type = "TripWorkflow", .concurrency = 2 }}) catch |err| {
        std.log.err("Unable to execute workflow: {t}", .{err});
        std.process.exit(1);
    };
    defer recovery_run.deinit();
    std.log.info("Started workflow WorkflowID={s} RunID={s}", .{ recovery_run.id, recovery_run.run_id });
    recovery_run.get(arena) catch |err| {
        std.log.err("Unable to get the recovery workflow's result: {t}", .{err});
        std.process.exit(1);
    };
    std.log.info("Recovery workflow completed", .{});

    const description = client.describeWorkflow(arena, trip_workflow_id, "") catch |err| {
        std.log.err("Unable to describe workflow {s}: {t}", .{ trip_workflow_id, err });
        std.process.exit(1);
    };
    if (std.mem.eql(u8, description.run_id, trip_run.run_id) or description.status != .running) {
        std.log.err("Workflow {s} wasn't restarted: its latest run is {s} ({t})", .{ trip_workflow_id, description.run_id, description.status });
        std.process.exit(1);
    }
    std.log.info("Trip workflow restarted in a new run RunID={s}", .{description.run_id});

    waitForCounter(&client, io, arena, trips.len);
    std.log.info("Trip counter after recovery: {d}", .{trips.len});

    client.terminateWorkflow(trip_workflow_id, "", "recovery sample finished") catch |err| {
        std.log.err("Unable to terminate workflow {s}: {t}", .{ trip_workflow_id, err });
        std.process.exit(1);
    };
    std.log.info("Terminated the trip workflow", .{});
}

/// Queries the trip workflow's latest run until its counter reaches `expected`: signals are
/// recorded before the workflow has counted them.
fn waitForCounter(client: *Client, io: std.Io, arena: std.mem.Allocator, expected: i64) void {
    var counter: i64 = 0;
    for (0..max_tries) |_| {
        counter = client.queryWorkflow(trip_workflow.Counter, arena, trip_workflow_id, "", .{}) catch |err| {
            std.log.err("Unable to query workflow {s}: {t}", .{ trip_workflow_id, err });
            std.process.exit(1);
        };
        std.log.info("Received query result Result={d}", .{counter});
        if (counter == expected) return;
        io.sleep(.fromMilliseconds(100), .awake) catch std.process.exit(1);
    }
    std.log.err("The trip counter of workflow {s} is {d} after {d} tries, not {d}", .{ trip_workflow_id, counter, max_tries, expected });
    std.process.exit(1);
}
