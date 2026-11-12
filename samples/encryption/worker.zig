const std = @import("std");
const temporal = @import("temporal");
const encryption = @import("encryption.zig");
const codec = @import("codec.zig");

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;

    // The client and worker are heavyweight objects that should be created once per process.
    // The codecs on the client make its workers encrypt activity inputs and workflow and
    // activity results. The propagator gives each workflow and activity the key ID the
    // workflow was started with, which the codecs encrypt with.
    //
    // To encrypt every workflow with one key instead, give the encryption codec a default
    // `key_id`; the propagator is then not needed.
    const propagators = [_]temporal.propagation.ContextPropagator{encryption.propagator};
    var options = try temporal.client.loadDefaultOptions(init.arena.allocator());
    options.codecs = &codec.codecs;
    options.propagators = &propagators;
    var client = temporal.client.Client.connect(gpa, io, options) catch |err| {
        std.log.err("Unable to create client: {t}", .{err});
        std.process.exit(1);
    };
    defer client.close();

    var w = try temporal.worker.Worker.init(gpa, io, &client, encryption.task_queue, .{});
    defer w.deinit();

    try w.registerWorkflow(encryption.Workflow);
    try w.registerActivity(encryption.Activity);

    w.runUntilSignal() catch |err| {
        std.log.err("Unable to start worker: {t}", .{err});
        std.process.exit(1);
    };
}
