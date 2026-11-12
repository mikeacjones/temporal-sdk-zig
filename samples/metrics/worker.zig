//! Runs the metrics workflow and activity on a client that serves its metrics for Prometheus
//! to scrape.
//!
//! Usage: worker [bind-address]. The address defaults to 0.0.0.0:9090; the endpoint is
//! http://<bind-address>/metrics.

const std = @import("std");
const temporal = @import("temporal");
const sample = @import("metrics.zig");

const default_bind_address = "0.0.0.0:9090";

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;

    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.skip();
    const bind_address = args.next() orelse default_bind_address;

    // The client and worker are heavyweight objects that should be created once per process.
    var options = try temporal.client.loadDefaultOptions(init.arena.allocator());
    options.metrics = .{ .exporter = .{ .prometheus = .{ .bind_address = bind_address } } };
    var client = temporal.client.Client.connect(gpa, io, options) catch |err| {
        std.log.err("Unable to create client with a Prometheus exporter on {s}: {t}", .{ bind_address, err });
        std.process.exit(1);
    };
    defer client.close();
    std.log.info("Serving metrics on http://{s}/metrics", .{bind_address});

    var w = try temporal.worker.Worker.init(gpa, io, &client, sample.task_queue, .{});
    defer w.deinit();

    try w.registerWorkflow(sample.Workflow);
    try w.registerActivity(sample.Activity);

    w.runUntilSignal() catch |err| {
        std.log.err("Unable to start worker: {t}", .{err});
        std.process.exit(1);
    };
}
