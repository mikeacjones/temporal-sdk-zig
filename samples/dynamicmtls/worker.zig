const std = @import("std");
const temporal = @import("temporal");
const helloworld = @import("helloworld");
const dynamicmtls = @import("dynamicmtls.zig");

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;
    const arena = init.arena.allocator();

    const args = try dynamicmtls.programArgs(arena, init);
    // parseClientOptions and reloadInterval log what is wrong with the arguments.
    const options = dynamicmtls.parseClientOptions(arena, args, init.environ_map) catch std.process.exit(1);
    const interval = dynamicmtls.reloadInterval(args) catch std.process.exit(1);

    // Recorded before connecting, so a certificate replaced while the client connects is seen
    // as a change rather than missed.
    var watcher: dynamicmtls.CertificateWatcher = .init(gpa, io, options.tls.?.client_cert_path, options.tls.?.client_private_key_path);

    // The client and worker are heavyweight objects that should be created once per process.
    var client = temporal.client.Client.connect(gpa, io, options) catch |err| {
        std.log.err("Unable to create client: {t}", .{err});
        std.process.exit(1);
    };
    defer client.close();

    var w = try temporal.worker.Worker.init(gpa, io, &client, dynamicmtls.task_queue, .{});
    defer w.deinit();

    try w.registerWorkflow(helloworld.HelloWorld);
    try w.registerActivity(helloworld.Greet);

    // Declared after the client and the worker, so it is canceled before they are freed.
    var watching = io.concurrent(dynamicmtls.CertificateWatcher.run, .{ &watcher, &client, options.connectionOptions(), interval }) catch |err| {
        std.log.err("Unable to start watching the client certificate: {t}", .{err});
        std.process.exit(1);
    };
    defer watching.cancel(io) catch {};

    w.runUntilSignal() catch |err| {
        std.log.err("Unable to start worker: {t}", .{err});
        std.process.exit(1);
    };
}
