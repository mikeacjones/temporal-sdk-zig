//! Queries a trip workflow's trip count: `recovery-query [workflow id]`, by default
//! trip_workflow.

const std = @import("std");
const temporal = @import("temporal");
const trip_workflow = @import("trip_workflow.zig");

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;
    const arena = init.arena.allocator();

    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.skip();
    const workflow_id = args.next() orelse "trip_workflow";

    // The client is a heavyweight object that should be created once per process.
    const options = try temporal.client.loadDefaultOptions(arena);
    var client = temporal.client.Client.connect(gpa, io, options) catch |err| {
        std.log.err("Unable to create client: {t}", .{err});
        std.process.exit(1);
    };
    defer client.close();

    const counter = client.queryWorkflow(trip_workflow.Counter, arena, workflow_id, "", .{}) catch |err| {
        std.log.err("Unable to query workflow {s}: {t}", .{ workflow_id, err });
        std.process.exit(1);
    };
    std.log.info("Received query result Result={d}", .{counter});
}
