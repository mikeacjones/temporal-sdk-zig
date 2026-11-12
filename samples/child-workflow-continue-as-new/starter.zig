//! Placeholder starter for the child-workflow-continue-as-new sample, replaced by its implementation. It runs the
//! placeholder workflow so the worker is known to be running when samples-test stops it.

const std = @import("std");
const temporal = @import("temporal");

fn placeholder(ctx: *temporal.workflow.Context) !void {
    _ = ctx;
}
const Placeholder = temporal.workflow.Def("Placeholder", placeholder);

pub fn main(init: std.process.Init) !void {
    const arena = init.arena.allocator();
    var client = try temporal.client.Client.connect(init.gpa, init.io, try temporal.client.loadDefaultOptions(arena));
    defer client.close();
    var run = try client.executeWorkflow(Placeholder, .{ .task_queue = "child-workflow-continue-as-new" }, .{});
    defer run.deinit();
    try run.get(arena);
    std.log.info("child-workflow-continue-as-new sample is a placeholder", .{});
}
