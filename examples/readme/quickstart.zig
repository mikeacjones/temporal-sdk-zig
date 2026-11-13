//! README.md examples. The code between `// example: <id>` and `// end-example` is copied into
//! README.md by `zig build readme`; edit it here, never in README.md.

const std = @import("std");
const temporal = @import("temporal");

// example: quickstart-workflow
const activity = temporal.activity;
const workflow = temporal.workflow;

fn greet(ctx: *activity.Context, name: []const u8) ![]const u8 {
    return std.fmt.allocPrint(ctx.allocator(), "Hello, {s}!", .{name});
}

pub const Greet = activity.Def("greet", greet);

fn greeting(ctx: *workflow.Context, name: []const u8) ![]const u8 {
    const future = ctx.executeActivity(Greet, .{ .start_to_close_timeout = .fromSeconds(10) }, .{name});
    return future.get();
}

pub const Greeting = workflow.Def("greeting", greeting);
// end-example

// example: quickstart-worker
pub fn runWorker(gpa: std.mem.Allocator, io: std.Io, options: temporal.client.Options) !void {
    var client = try temporal.client.Client.connect(gpa, io, options);
    defer client.close();

    var worker = try temporal.worker.Worker.init(gpa, io, &client, "greetings", .{});
    defer worker.deinit();
    try worker.registerWorkflow(Greeting);
    try worker.registerActivity(Greet);

    // Polls the task queue until the process gets SIGINT or SIGTERM.
    try worker.runUntilSignal();
}
// end-example

// example: quickstart-starter
pub fn startGreeting(gpa: std.mem.Allocator, io: std.Io, arena: std.mem.Allocator, options: temporal.client.Options) ![]const u8 {
    var client = try temporal.client.Client.connect(gpa, io, options);
    defer client.close();

    var run = try client.executeWorkflow(Greeting, .{
        .id = "greeting-world",
        .task_queue = "greetings",
    }, .{"World"});
    defer run.deinit();

    // Waits for the workflow to complete; the result is allocated from `arena`.
    const result = try run.get(arena);
    std.log.info("workflow result: {s}", .{result});
    return result;
}
// end-example

fn ignoreSignal(_: std.posix.SIG) callconv(.c) void {}

test "quickstart: a worker and a starter against a dev server" {
    const gpa = std.testing.allocator;
    const io = std.testing.io;
    var server = try temporal.testing.DevServer.start(gpa, io, .{});
    defer server.stop(io);

    // `runUntilSignal` replaces this handler while it runs. Installing a harmless one first
    // keeps the SIGTERM below from killing the test if the worker has already returned.
    const ignore: std.posix.Sigaction = .{ .handler = .{ .handler = ignoreSignal }, .mask = std.posix.sigemptyset(), .flags = 0 };
    var previous: std.posix.Sigaction = undefined;
    std.posix.sigaction(.TERM, &ignore, &previous);
    defer std.posix.sigaction(.TERM, &previous, null);

    var worker = try io.concurrent(runWorker, .{ gpa, io, server.clientOptions() });
    var worker_done = false;
    defer if (!worker_done) {
        std.posix.kill(std.c.getpid(), .TERM) catch {};
        worker.await(io) catch {};
    };

    var arena: std.heap.ArenaAllocator = .init(gpa);
    defer arena.deinit();
    const result = try startGreeting(gpa, io, arena.allocator(), server.clientOptions());
    try std.testing.expectEqualStrings("Hello, World!", result);

    // The workflow completed, so the worker is in `runUntilSignal` with its handler installed.
    worker_done = true;
    try std.posix.kill(std.c.getpid(), .TERM);
    try worker.await(io);
}
