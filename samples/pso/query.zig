//! Queries a running or closed optimization: `pso-query <workflow id> [query type]`. The
//! query type is `child` (the default), which a PSOWorkflow answers with its child workflow's
//! ID, or `iteration`, which a PSOChildWorkflow answers with its latest progress message.

const std = @import("std");
const temporal = @import("temporal");
const pso = @import("pso.zig");

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;
    const arena = init.arena.allocator();

    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.skip();
    const workflow_id = args.next() orelse {
        std.log.err("Usage: pso-query <workflow id> [child|iteration]", .{});
        std.process.exit(1);
    };
    const query_type = args.next() orelse "child";

    // The client is a heavyweight object that should be created once per process.
    const options = try temporal.client.loadDefaultOptions(arena);
    var client = temporal.client.Client.connect(gpa, io, options) catch |err| {
        std.log.err("Unable to create client: {t}", .{err});
        std.process.exit(1);
    };
    defer client.close();

    const result = if (std.mem.eql(u8, query_type, "child"))
        client.queryWorkflow(pso.ChildQuery, arena, workflow_id, "", .{})
    else if (std.mem.eql(u8, query_type, "iteration"))
        client.queryWorkflow(pso.IterationQuery, arena, workflow_id, "", .{})
    else {
        std.log.err("Unknown query type \"{s}\": use child or iteration", .{query_type});
        std.process.exit(1);
    };
    const state = result catch |err| {
        std.log.err("Unable to query workflow {s} with {s}: {t}", .{ workflow_id, query_type, err });
        std.process.exit(1);
    };
    std.log.info("Received query result Result={s}", .{state});
}
