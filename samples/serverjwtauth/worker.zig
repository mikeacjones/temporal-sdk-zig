const std = @import("std");
const temporal = @import("temporal");
const helloworld = @import("helloworld");
const serverjwtauth = @import("serverjwtauth.zig");

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;
    const arena = init.arena.allocator();

    var options = temporal.client.loadOptions(arena, .{ .env = init.environ_map }) catch |err| {
        std.log.err("Unable to load the client options: {t}", .{err});
        std.process.exit(1);
    };
    // Only the token below authorizes the calls, never an API key from the environment.
    options.api_key = "";

    // readKey logs what is wrong with the key file.
    const key = serverjwtauth.readKey(arena, io, serverjwtauth.keyPath(init.environ_map)) catch std.process.exit(1);
    const refresher: serverjwtauth.HeaderRefresher = .{
        .config = .{ .key = key, .permissions = try serverjwtauth.namespacePermissions(arena, options.namespace) },
        .base_headers = options.headers,
    };
    options.headers = refresher.headers(arena, io) catch |err| {
        std.log.err("Unable to mint a JWT: {t}", .{err});
        std.process.exit(1);
    };

    // The client and worker are heavyweight objects that should be created once per process.
    var client = temporal.client.Client.connect(gpa, io, options) catch |err| {
        std.log.err("Unable to create client: {t}", .{err});
        std.process.exit(1);
    };
    defer client.close();

    var w = try temporal.worker.Worker.init(gpa, io, &client, serverjwtauth.task_queue, .{});
    defer w.deinit();

    try w.registerWorkflow(helloworld.HelloWorld);
    try w.registerActivity(helloworld.Greet);

    // Declared after the client and the worker, so it is canceled before they are freed.
    var refreshing = io.concurrent(serverjwtauth.HeaderRefresher.run, .{ &refresher, gpa, io, &client }) catch |err| {
        std.log.err("Unable to start refreshing the JWT: {t}", .{err});
        std.process.exit(1);
    };
    defer refreshing.cancel(io) catch {};

    w.runUntilSignal() catch |err| {
        std.log.err("Unable to start worker: {t}", .{err});
        std.process.exit(1);
    };
}
