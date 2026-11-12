//! Placeholder worker for the reqrespquery sample, replaced by its implementation.

const std = @import("std");
const temporal = @import("temporal");

fn placeholder(ctx: *temporal.workflow.Context) !void {
    _ = ctx;
}
const Placeholder = temporal.workflow.Def("Placeholder", placeholder);

pub fn main(init: std.process.Init) !void {
    const options = try temporal.client.loadDefaultOptions(init.arena.allocator());
    var client = try temporal.client.Client.connect(init.gpa, init.io, options);
    defer client.close();
    var w = try temporal.worker.Worker.init(init.gpa, init.io, &client, "reqrespquery", .{});
    defer w.deinit();
    try w.registerWorkflow(Placeholder);
    try w.runUntilSignal();
}
