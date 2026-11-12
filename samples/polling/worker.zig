//! Runs a worker for each polling pattern, each on its own task queue and with its own test
//! service, sharing one client. One SIGINT or SIGTERM stops all three.

const std = @import("std");
const temporal = @import("temporal");
const TestService = @import("test_service.zig").TestService;
const frequent = @import("frequent.zig");
const infrequent = @import("infrequent.zig");
const periodic_sequence = @import("periodic_sequence.zig");

const Worker = temporal.worker.Worker;

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;

    // The client and workers are heavyweight objects that should be created once per process.
    const options = try temporal.client.loadDefaultOptions(init.arena.allocator());
    var client = temporal.client.Client.connect(gpa, io, options) catch |err| {
        std.log.err("Unable to create client: {t}", .{err});
        std.process.exit(1);
    };
    defer client.close();

    var frequent_service: TestService = .init(5);
    var frequent_activities: frequent.PollingActivities = .{ .test_service = &frequent_service, .poll_interval = .fromSeconds(1) };
    var frequent_worker = try Worker.init(gpa, io, &client, frequent.task_queue, .{});
    defer frequent_worker.deinit();
    try frequent_worker.registerWorkflow(frequent.FrequentPolling);
    try frequent_worker.registerActivityWith(frequent.DoPoll, &frequent_activities);

    var infrequent_service: TestService = .init(5);
    var infrequent_activities: infrequent.PollingActivities = .{ .test_service = &infrequent_service };
    var infrequent_worker = try Worker.init(gpa, io, &client, infrequent.task_queue, .{});
    defer infrequent_worker.deinit();
    try infrequent_worker.registerWorkflow(infrequent.InfrequentPolling);
    try infrequent_worker.registerActivityWith(infrequent.DoPoll, &infrequent_activities);

    var periodic_service: TestService = .init(50);
    var periodic_activities: periodic_sequence.PollingActivities = .{ .test_service = &periodic_service };
    var periodic_worker = try Worker.init(gpa, io, &client, periodic_sequence.task_queue, .{});
    defer periodic_worker.deinit();
    try periodic_worker.registerWorkflow(periodic_sequence.PeriodicSequencePolling);
    try periodic_worker.registerWorkflow(periodic_sequence.PollingChildWorkflow);
    try periodic_worker.registerActivityWith(periodic_sequence.DoPoll, &periodic_activities);

    var group: std.Io.Group = .init;
    try group.concurrent(io, runWorker, .{&frequent_worker});
    try group.concurrent(io, runWorker, .{&infrequent_worker});
    try group.concurrent(io, runWorker, .{&periodic_worker});
    // Wait for all three workers to stop.
    try group.await(io);
}

fn runWorker(w: *Worker) void {
    w.runUntilSignal() catch |err| {
        std.log.err("Unable to start worker for task queue {s}: {t}", .{ w.task_queue, err });
        std.process.exit(1);
    };
}
