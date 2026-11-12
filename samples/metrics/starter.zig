//! Runs the metrics workflow, then scrapes the worker's metrics endpoint the way Prometheus
//! would and prints the activity's metrics.
//!
//! Usage: starter [metrics-address]. The address defaults to 127.0.0.1:9090, where the worker
//! serves by default.

const std = @import("std");
const temporal = @import("temporal");
const sample = @import("metrics.zig");

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;
    const arena = init.arena.allocator();

    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.skip();
    const metrics_address = args.next() orelse "127.0.0.1:9090";

    // The client is a heavyweight object that should be created once per process.
    const options = try temporal.client.loadDefaultOptions(arena);
    var client = temporal.client.Client.connect(gpa, io, options) catch |err| {
        std.log.err("Unable to create client: {t}", .{err});
        std.process.exit(1);
    };
    defer client.close();

    var run = client.executeWorkflow(sample.Workflow, .{
        .id = "metrics_workflowID",
        .task_queue = sample.task_queue,
    }, .{}) catch |err| {
        std.log.err("Unable to execute workflow: {t}", .{err});
        std.process.exit(1);
    };
    defer run.deinit();
    std.log.info("Started workflow WorkflowID={s} RunID={s}", .{ run.id, run.run_id });

    // Synchronously wait for the workflow completion.
    run.get(arena) catch |err| {
        std.log.err("Unable to wait for workflow completion: {t}", .{err});
        std.process.exit(1);
    };

    const url = try std.fmt.allocPrint(arena, "http://{s}/metrics", .{metrics_address});
    var http: std.http.Client = .{ .allocator = gpa, .io = io };
    defer http.deinit();
    var body: std.Io.Writer.Allocating = .init(arena);
    const response = http.fetch(.{ .location = .{ .url = url }, .response_writer = &body.writer }) catch |err| {
        std.log.err("Unable to scrape the worker's metrics at {s}: {t}", .{ url, err });
        std.process.exit(1);
    };
    if (response.status != .ok) {
        std.log.err("Scraping the worker's metrics at {s} returned HTTP {d}", .{ url, @backingInt(response.status) });
        std.process.exit(1);
    }

    const names = [_][]const u8{
        sample.activity_started_count,
        sample.activity_success_count,
        sample.schedule_to_start_latency ++ "_count",
        sample.activity_latency ++ "_count",
    };
    for (names) |name| {
        const line = findSample(body.written(), name) orelse {
            std.log.err("The worker's metrics at {s} lack {s}", .{ url, name });
            std.process.exit(1);
        };
        std.log.info("Scraped {s}", .{line});
    }
    std.log.info("Check metrics at {s}", .{url});
}

/// The first line of the Prometheus text `body` that is a sample of the metric `name`.
fn findSample(body: []const u8, name: []const u8) ?[]const u8 {
    var lines = std.mem.splitScalar(u8, body, '\n');
    while (lines.next()) |line| {
        if (std.mem.startsWith(u8, line, name) and line.len > name.len and (line[name.len] == '{' or line[name.len] == ' ')) return line;
    }
    return null;
}
