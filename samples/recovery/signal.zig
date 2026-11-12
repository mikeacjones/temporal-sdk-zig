//! Signals a completed trip to a trip workflow: `recovery-signal <trip id> <total>
//! [workflow id]`, by default to trip_workflow.

const std = @import("std");
const temporal = @import("temporal");
const trip_workflow = @import("trip_workflow.zig");

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;
    const arena = init.arena.allocator();

    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.skip();
    const id = args.next();
    const total_arg = args.next();
    if (id == null or total_arg == null) {
        std.log.err("Usage: recovery-signal <trip id> <total> [workflow id]", .{});
        std.process.exit(1);
    }
    const total = std.fmt.parseInt(i64, total_arg.?, 10) catch {
        std.log.err("Unable to parse the trip total \"{s}\": expected a whole number", .{total_arg.?});
        std.process.exit(1);
    };
    const workflow_id = args.next() orelse "trip_workflow";

    // The client is a heavyweight object that should be created once per process.
    const options = try temporal.client.loadDefaultOptions(arena);
    var client = temporal.client.Client.connect(gpa, io, options) catch |err| {
        std.log.err("Unable to create client: {t}", .{err});
        std.process.exit(1);
    };
    defer client.close();

    client.signalWorkflow(trip_workflow.TripSignal, workflow_id, "", .{.{ .id = id.?, .total = total }}) catch |err| {
        std.log.err("Unable to signal workflow {s}: {t}", .{ workflow_id, err });
        std.process.exit(1);
    };
    std.log.info("Signaled trip {s} to workflow {s}", .{ id.?, workflow_id });
}
