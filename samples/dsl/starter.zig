//! Starts the DSL workflow with a definition read from a ZON file and waits for it to finish.
//!
//! Usage: dsl-starter [definition.zon]
//!
//! Without an argument it runs workflow1.zon, built into the program; workflow2.zon adds a
//! parallel block. Write your own definition to call the activities in another shape.

const std = @import("std");
const temporal = @import("temporal");
const dsl = @import("dsl.zig");

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;
    const arena = init.arena.allocator();

    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.skip();
    const path = args.next();
    const source: [:0]const u8 = if (path) |p|
        std.Io.Dir.cwd().readFileAllocOptions(io, p, arena, .limited(1024 * 1024), .of(u8), 0) catch |err| {
            std.log.err("Failed to load DSL definition file {s}: {t}", .{ p, err });
            std.process.exit(1);
        }
    else
        @embedFile("workflow1.zon");

    var diag: std.zon.parse.Diagnostics = undefined;
    const definition = dsl.parseDefinition(arena, source, &diag) catch |err| {
        std.log.err("Failed to parse DSL definition {s}: {t}\n{f}", .{ path orelse "workflow1.zon", err, diag.fmt(path orelse "workflow1.zon") });
        std.process.exit(1);
    };

    // The client is a heavyweight object that should be created once per process.
    const options = try temporal.client.loadDefaultOptions(arena);
    var client = temporal.client.Client.connect(gpa, io, options) catch |err| {
        std.log.err("Unable to create client: {t}", .{err});
        std.process.exit(1);
    };
    defer client.close();

    var run = client.executeWorkflow(dsl.SimpleDslWorkflow, .{
        .id = try std.fmt.allocPrint(arena, "dsl_{s}", .{try temporal.client.uuid(arena, io)}),
        .task_queue = "dsl",
    }, .{definition}) catch |err| {
        std.log.err("Unable to execute workflow: {t}", .{err});
        std.process.exit(1);
    };
    defer run.deinit();
    std.log.info("Started workflow WorkflowID={s} RunID={s}", .{ run.id, run.run_id });

    run.get(arena) catch |err| {
        std.log.err("DSL workflow failed: {t}", .{err});
        std.process.exit(1);
    };
    std.log.info("DSL workflow completed", .{});
}
